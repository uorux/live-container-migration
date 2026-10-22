// Package gateway implements lcm-gateway: it accepts player TCP connections,
// holds them across relay outages, and multiplexes them as streams over a
// resumable tunnel that the relay (inside the migrating container) dials in.
package gateway

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/proxyproto"
	"lcm/internal/tunnel"
	"lcm/internal/wire"
)

// Config configures a Gateway.
type Config struct {
	Tunnel tunnel.Config
	// Backend is the name relays must present in HELLO, and where players go
	// when no route matches.
	Backend string
	// Backends are further servers behind this gateway (Minecraft mode routes
	// players to them by the hostname in their handshake; see Route).
	Backends []string
	// Routes maps a hostname (lower case, no trailing dot) to a backend.
	// Hostnames without a route go to the backend named by their first DNS
	// label if there is one (atm10.mc.example.com -> atm10), else to Backend.
	Routes map[string]string
	// Minecraft, if set, makes the player listener Minecraft-aware for the
	// first packets of each connection (local status while the backend is
	// down, login hold). Nil keeps the gateway pure L4.
	Minecraft *MinecraftConfig
	// AcceptProxy lists the sources (e.g. a local HAProxy with send-proxy-v2)
	// whose connections start with a PROXY v2 header carrying the player's
	// real address. Connections from other sources are taken as direct.
	AcceptProxy []netip.Prefix
	// Wake, if set, is called (at most every WakeEvery per backend) when a
	// player arrives for a backend whose relay isn't connected: an on-demand
	// server that is asleep. The player is held meanwhile.
	Wake      func(ctx context.Context, backend string) error
	WakeEvery time.Duration
	Logger    *slog.Logger
	// Version is reported by lcm_gateway_build_info and /status ("dev" if empty).
	Version string
}

// Backend is one Minecraft server reachable through a relay session.
type Backend struct {
	Name    string
	Session *tunnel.Session
	held    atomic.Int64 // login streams currently held (Minecraft mode)
	// lastWake is when Wake was last called for this backend (unix nanos).
	lastWake atomic.Int64
	// statusAnswered counts status pings the gateway answered itself.
	statusAnswered atomic.Uint64
}

// Gateway serves player and tunnel listeners.
type Gateway struct {
	cfg      Config
	log      *slog.Logger
	mu       sync.Mutex
	backends map[string]*Backend
	started  time.Time
	metrics  *gwMetrics
}

// New creates a gateway with a single backend.
func New(cfg Config) *Gateway {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Backend == "" {
		cfg.Backend = "default"
	}
	cfg.Tunnel.Logger = cfg.Logger
	cfg.Tunnel.Defaults()
	if cfg.Minecraft != nil {
		mc := *cfg.Minecraft
		mc.defaults()
		cfg.Minecraft = &mc
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	g := &Gateway{cfg: cfg, log: cfg.Logger, backends: map[string]*Backend{}, started: time.Now()}
	g.metrics = newGWMetrics(g, cfg.Version)
	g.addBackend(cfg.Backend)
	for _, name := range cfg.Backends {
		if g.Backend(name) == nil {
			g.addBackend(name)
		}
	}
	return g
}

// Route picks the backend for a player who asked for host (as sent in the
// handshake; Forge markers and a trailing dot are ignored).
func (g *Gateway) Route(host string) *Backend {
	host = strings.ToLower(mcproto.CleanHost(host))
	if name, ok := g.cfg.Routes[host]; ok {
		if b := g.Backend(name); b != nil {
			return b
		}
	}
	if label, _, ok := strings.Cut(host, "."); ok {
		if b := g.Backend(label); b != nil {
			return b
		}
	}
	return g.DefaultBackend()
}

func (g *Gateway) addBackend(name string) {
	tc := g.cfg.Tunnel
	tc.Observer = backendObserver{m: g.metrics, name: name}
	g.metrics.initBackend(name)
	g.mu.Lock()
	g.backends[name] = &Backend{
		Name:    name,
		Session: tunnel.NewSession(tunnel.RoleGateway, tc, [wire.SessionIDLen]byte{}, nil),
	}
	g.mu.Unlock()
}

// Backend returns a backend by name (nil if unknown).
func (g *Gateway) Backend(name string) *Backend {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.backends[name]
}

// DefaultBackend returns the configured backend.
func (g *Gateway) DefaultBackend() *Backend { return g.Backend(g.cfg.Backend) }

func (g *Gateway) lookup(name string) *tunnel.Session {
	if b := g.Backend(name); b != nil {
		return b.Session
	}
	return nil
}

// Listen opens a plain-TCP (never MPTCP) listener.
func Listen(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	lc.SetMultipathTCP(false)
	return lc.Listen(ctx, "tcp", addr)
}

// ServePlayers accepts player connections on ln for backend b until ln is closed.
func (g *Gateway) ServePlayers(ln net.Listener, b *Backend) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			g.log.Error("player accept", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go g.handlePlayer(c, b)
	}
}

func (g *Gateway) handlePlayer(c net.Conn, b *Backend) {
	if g.trustedProxy(c.RemoteAddr()) {
		pc, err := readProxyHeader(c)
		if err != nil {
			g.log.Warn("PROXY header from a trusted source", "src", c.RemoteAddr().String(), "err", err)
			c.Close()
			return
		}
		c = pc
	}
	if g.cfg.Minecraft != nil {
		g.handleMinecraft(c, b)
		return
	}
	g.addStream(b, c, "l4", g.log.With("src", c.RemoteAddr().String()))
}

func (g *Gateway) trustedProxy(a net.Addr) bool {
	ap := addrPort(a)
	for _, p := range g.cfg.AcceptProxy {
		if p.Contains(ap.Addr()) {
			return true
		}
	}
	return false
}

// proxiedConn is a player connection that arrived through a PROXY-protocol
// sender: it reports the addresses from the header and replays bytes the
// header read buffered.
type proxiedConn struct {
	*net.TCPConn
	r        *bufio.Reader
	src, dst net.Addr
}

func (p *proxiedConn) Read(b []byte) (int, error) { return p.r.Read(b) }
func (p *proxiedConn) RemoteAddr() net.Addr       { return p.src }
func (p *proxiedConn) LocalAddr() net.Addr        { return p.dst }

var _ tunnel.TCPLike = (*proxiedConn)(nil)

func readProxyHeader(c net.Conn) (net.Conn, error) {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("not a TCP connection")
	}
	tc.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(tc)
	info, err := proxyproto.Read(br)
	tc.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, err
	}
	pc := &proxiedConn{TCPConn: tc, r: br, src: tc.RemoteAddr(), dst: tc.LocalAddr()}
	if !info.Local {
		pc.src, pc.dst = net.TCPAddrFromAddrPort(info.Src), net.TCPAddrFromAddrPort(info.Dst)
	}
	return pc, nil
}

// addStream hands a player connection to the backend's session.
func (g *Gateway) addStream(b *Backend, c net.Conn, kind string, log *slog.Logger) *tunnel.Stream {
	open := wire.Open{Src: addrPort(c.RemoteAddr()), Dst: addrPort(c.LocalAddr())}
	st := b.Session.AddStream(c, open)
	g.metrics.conns.Inc(b.Name, resultOpened)
	if kind == "login" || kind == "l4" {
		g.maybeWake(b)
	}
	log.Info("player connected", "stream", st.ID(), "kind", kind, "tunnel_up", b.Session.Connected())
	return st
}

// maybeWake asks for an asleep backend to be started (see Config.Wake).
func (g *Gateway) maybeWake(b *Backend) {
	if g.cfg.Wake == nil || b.Session.Connected() {
		return
	}
	every := g.cfg.WakeEvery
	if every <= 0 {
		every = 15 * time.Second
	}
	now := time.Now().UnixNano()
	last := b.lastWake.Load()
	if now-last < int64(every) || !b.lastWake.CompareAndSwap(last, now) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := g.cfg.Wake(ctx, b.Name); err != nil {
			g.metrics.wakes.Inc(b.Name, "error")
			g.log.Warn("waking backend", "backend", b.Name, "err", err)
			return
		}
		g.metrics.wakes.Inc(b.Name, "ok")
		g.log.Info("woke backend", "backend", b.Name)
	}()
}

func addrPort(a net.Addr) netip.AddrPort {
	if ta, ok := a.(*net.TCPAddr); ok {
		ap := ta.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	return netip.AddrPort{}
}

// ServeTunnel accepts relay connections on ln until it is closed.
func (g *Gateway) ServeTunnel(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			g.log.Error("tunnel accept", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go g.handleTunnel(c)
	}
}

func (g *Gateway) handleTunnel(nc net.Conn) {
	conn, err := tunnel.ServerHandshake(nc, g.cfg.Tunnel.Token, g.cfg.Tunnel.HandshakeTimeout, g.lookup)
	if err != nil {
		g.log.Warn("tunnel handshake failed", "remote", nc.RemoteAddr().String(), "err", err)
		return
	}
	err = conn.Run()
	g.log.Info("tunnel connection ended", "remote", nc.RemoteAddr().String(), "err", err)
}

// Status is the JSON status document.
type Status struct {
	Version  string          `json:"version"`
	Uptime   string          `json:"uptime"`
	Backends []BackendStatus `json:"backends"`
}

// BackendStatus describes one backend. Durations are given both as Go
// duration strings (for people) and in seconds (for scripts).
type BackendStatus struct {
	Name      string `json:"name"`
	Session   string `json:"session"`
	Connected bool   `json:"connected"`
	// UpSince is set while connected; DownSince (and DownFor) while not.
	UpSince        time.Time `json:"up_since,omitzero"`
	DownSince      time.Time `json:"down_since,omitzero"`
	DownFor        string    `json:"down_for,omitempty"`
	DownForSeconds float64   `json:"down_for_seconds,omitempty"`
	// LastOutage is the most recent tunnel outage that ended in a resume
	// (what players perceived as a freeze).
	LastOutage        string  `json:"last_outage,omitempty"`
	LastOutageSeconds float64 `json:"last_outage_seconds,omitempty"`
	Streams           int     `json:"streams"` // = players
	// HeldPlayers: all streams while the tunnel is down, else held logins.
	HeldPlayers    int64               `json:"held_players"`
	Held           int64               `json:"held,omitempty"` // logins held by -minecraft
	Connects       uint64              `json:"connects"`
	Resumes        uint64              `json:"resumes"`
	NewSessions    uint64              `json:"new_sessions"`
	Disconnects    uint64              `json:"disconnects"`
	LastConnect    time.Time           `json:"last_connect,omitzero"`
	LastDisconnect time.Time           `json:"last_disconnect,omitzero"`
	BytesToServer  uint64              `json:"bytes_to_server"`
	BytesToPlayer  uint64              `json:"bytes_to_player"`
	BufferedBytes  int                 `json:"buffered_bytes"`
	MaxTxBuffered  int                 `json:"max_tx_buffered"`
	StatusAnswered uint64              `json:"status_answered,omitempty"`
	StreamInfo     []tunnel.StreamInfo `json:"stream_info,omitempty"`
}

// Status returns a snapshot.
func (g *Gateway) Status(detail bool) Status {
	st := Status{Version: g.cfg.Version, Uptime: time.Since(g.started).Round(time.Second).String()}
	for _, b := range g.backendList() {
		s := b.Session.Stats()
		id := b.Session.ID()
		bst := BackendStatus{Name: b.Name, Session: hex.EncodeToString(id[:]), Connected: s.Connected,
			DownSince: s.DownSince, Streams: s.Streams, HeldPlayers: heldPlayers(b, s), Held: b.held.Load(),
			Connects: s.Connects, Resumes: s.Resumes, NewSessions: s.NewSessions, Disconnects: s.Disconnects,
			LastConnect: s.LastConnect, LastDisconnect: s.LastDisconnect,
			BytesToServer: s.BytesFromLocal, BytesToPlayer: s.BytesToLocal, BufferedBytes: s.TxBuffered + s.RxQueued,
			MaxTxBuffered: s.MaxTxBuffered, StatusAnswered: b.statusAnswered.Load()}
		if s.Connected {
			bst.UpSince = s.LastConnect
		} else {
			d := time.Since(s.DownSince)
			bst.DownFor, bst.DownForSeconds = d.Round(time.Millisecond).String(), d.Seconds()
		}
		if s.LastOutage > 0 {
			bst.LastOutage, bst.LastOutageSeconds = s.LastOutage.Round(time.Millisecond).String(), s.LastOutage.Seconds()
		}
		if detail {
			bst.StreamInfo = b.Session.StreamInfos()
		}
		st.Backends = append(st.Backends, bst)
	}
	return st
}

// AdminHandler serves /status (JSON; ?detail=1 for per-stream info),
// /metrics (Prometheus text format) and /healthz.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", g.metricsHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(g.Status(r.URL.Query().Get("detail") != ""))
	})
	return mux
}
