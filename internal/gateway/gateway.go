// Package gateway implements lcm-gateway: it accepts player TCP connections,
// holds them across relay outages, and multiplexes them as streams over a
// resumable tunnel that the relay (inside the migrating container) dials in.
package gateway

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/tunnel"
	"lcm/internal/wire"
)

// Config configures a Gateway.
type Config struct {
	Tunnel tunnel.Config
	// Backend is the name relays must present in HELLO. One backend per
	// gateway for now; the structure allows several (one player listener each).
	Backend string
	// Minecraft, if set, makes the player listener Minecraft-aware for the
	// first packets of each connection (local status while the backend is
	// down, login hold). Nil keeps the gateway pure L4.
	Minecraft *MinecraftConfig
	Logger    *slog.Logger
}

// Backend is one Minecraft server reachable through a relay session.
type Backend struct {
	Name    string
	Session *tunnel.Session
	held    atomic.Int64 // login streams currently held (Minecraft mode)
}

// Gateway serves player and tunnel listeners.
type Gateway struct {
	cfg      Config
	log      *slog.Logger
	mu       sync.Mutex
	backends map[string]*Backend
	started  time.Time
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
	g := &Gateway{cfg: cfg, log: cfg.Logger, backends: map[string]*Backend{}, started: time.Now()}
	g.backends[cfg.Backend] = &Backend{
		Name:    cfg.Backend,
		Session: tunnel.NewSession(tunnel.RoleGateway, cfg.Tunnel, [wire.SessionIDLen]byte{}, nil),
	}
	return g
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
		if g.cfg.Minecraft != nil {
			go g.handleMinecraft(c, b)
			continue
		}
		open := wire.Open{Src: addrPort(c.RemoteAddr()), Dst: addrPort(c.LocalAddr())}
		st := b.Session.AddStream(c, open)
		g.log.Info("player connected", "stream", st.ID(), "src", open.Src.String(),
			"tunnel_up", b.Session.Connected())
	}
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
	Uptime   string          `json:"uptime"`
	Backends []BackendStatus `json:"backends"`
}

// BackendStatus describes one backend.
type BackendStatus struct {
	Name           string              `json:"name"`
	Session        string              `json:"session"`
	Connected      bool                `json:"connected"`
	Streams        int                 `json:"streams"`
	Connects       uint64              `json:"connects"`
	Disconnects    uint64              `json:"disconnects"`
	LastConnect    time.Time           `json:"last_connect,omitzero"`
	LastDisconnect time.Time           `json:"last_disconnect,omitzero"`
	MaxTxBuffered  int                 `json:"max_tx_buffered"`
	Held           int64               `json:"held,omitempty"`
	StreamInfo     []tunnel.StreamInfo `json:"stream_info,omitempty"`
}

// Status returns a snapshot.
func (g *Gateway) Status(detail bool) Status {
	g.mu.Lock()
	bs := make([]*Backend, 0, len(g.backends))
	for _, b := range g.backends {
		bs = append(bs, b)
	}
	g.mu.Unlock()
	st := Status{Uptime: time.Since(g.started).Round(time.Second).String()}
	for _, b := range bs {
		s := b.Session.Stats()
		id := b.Session.ID()
		bst := BackendStatus{Name: b.Name, Session: hex.EncodeToString(id[:]), Connected: s.Connected,
			Streams: s.Streams, Connects: s.Connects, Disconnects: s.Disconnects,
			LastConnect: s.LastConnect, LastDisconnect: s.LastDisconnect, MaxTxBuffered: s.MaxTxBuffered, Held: b.held.Load()}
		if detail {
			bst.StreamInfo = b.Session.StreamInfos()
		}
		st.Backends = append(st.Backends, bst)
	}
	return st
}

// AdminHandler serves /status (JSON; ?detail=1 for per-stream info) and /healthz.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(g.Status(r.URL.Query().Get("detail") != ""))
	})
	return mux
}
