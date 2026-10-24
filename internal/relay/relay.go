// Package relay implements lcm-relay's tunnel side: it dials out to the
// gateway, keeps a resumable session (whose state lives only in memory and so
// survives a CRIU checkpoint), opens a loopback connection to the server for
// each stream with a PROXY v2 header, and obeys quiesce/resume commands.
package relay

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/proxyproto"
	"lcm/internal/tunnel"
	"lcm/internal/wire"
)

// Config configures a Relay.
type Config struct {
	Gateway  string // host:port of the gateway's tunnel listener
	Backend  string // backend name presented in HELLO
	Upstream string // server address, normally 127.0.0.1:25565
	// ProxyProtocol sends a PROXY v2 header on each upstream connection.
	ProxyProtocol bool
	Tunnel        tunnel.Config
	// QuiesceTimeout: auto-resume if no resume arrives within this time.
	QuiesceTimeout time.Duration
	BackoffMin     time.Duration
	BackoffMax     time.Duration
	// CheckSockets verifies via /proc that no non-loopback socket remains
	// before acknowledging quiesce.
	CheckSockets bool
	// Ready selects the check that must pass before the first tunnel
	// connection of a fresh relay process: none (zero value), a TCP connect
	// to Upstream, or a Minecraft status ping answered by Upstream. Once
	// passed it is never repeated, so a CRIU-restored relay does not re-wait.
	Ready ReadyCheck
	// ReadyInterval is the probe period (default 1 s).
	ReadyInterval time.Duration
	// DialTunnel overrides how the gateway is dialled (tests). The default is a
	// plain-TCP (MPTCP disabled) dialer.
	DialTunnel func(ctx context.Context, addr string) (net.Conn, error)
	Logger     *slog.Logger
}

// ReadyCheck is a readiness probe kind; see Config.Ready.
type ReadyCheck int

const (
	ReadyNone ReadyCheck = iota
	ReadyTCP
	ReadyMinecraft
)

func (c ReadyCheck) String() string {
	switch c {
	case ReadyTCP:
		return "tcp"
	case ReadyMinecraft:
		return "minecraft"
	}
	return "none"
}

type stopReason int

const (
	stopAbort stopReason = iota
	stopQuiesce
	stopFinal
)

// Relay is the tunnel client.
type Relay struct {
	cfg  Config
	log  *slog.Logger
	sess *tunnel.Session
	up   net.Dialer

	mu            sync.Mutex
	quiesced      bool
	quiescedAt    time.Time
	final         bool
	stop          stopReason
	attemptCancel context.CancelFunc
	attemptDone   chan struct{}
	autoTimer     *time.Timer
	autoGen       uint64
	state         string
	wake          chan struct{}
	lastErr       string
	dials         sync.WaitGroup // in-flight upstream dials

	ready  atomic.Bool
	probes atomic.Int64 // readiness probes run (tests, status)

	ChildPID atomic.Int64
}

// New creates a relay with a fresh random session id.
func New(cfg Config) *Relay {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Backend == "" {
		cfg.Backend = "default"
	}
	if cfg.Upstream == "" {
		cfg.Upstream = "127.0.0.1:25565"
	}
	if cfg.QuiesceTimeout <= 0 {
		cfg.QuiesceTimeout = 120 * time.Second
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = 100 * time.Millisecond
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 2 * time.Second
	}
	if cfg.ReadyInterval <= 0 {
		cfg.ReadyInterval = time.Second
	}
	cfg.Tunnel.Logger = cfg.Logger
	cfg.Tunnel.Defaults()
	r := &Relay{cfg: cfg, log: cfg.Logger, wake: make(chan struct{}, 1), state: "connecting"}
	// Never MPTCP: CRIU cannot checkpoint IPPROTO_MPTCP sockets.
	r.up.SetMultipathTCP(false)
	r.ready.Store(cfg.Ready == ReadyNone)
	var id [wire.SessionIDLen]byte
	crand.Read(id[:])
	r.sess = tunnel.NewSession(tunnel.RoleRelay, cfg.Tunnel, id, func(st *tunnel.Stream) {
		// Called synchronously from the tunnel reader, so every Add happens
		// before the attempt ends and Quiesce's Wait cannot miss a dial.
		r.dials.Add(1)
		go func() { defer r.dials.Done(); r.openUpstream(st) }()
	})
	if ap, err := netip.ParseAddrPort(cfg.Upstream); err != nil || !ap.Addr().IsLoopback() {
		r.log.Warn("upstream is not a loopback address; its sockets will block quiesce", "upstream", cfg.Upstream)
	}
	return r
}

// Session exposes the tunnel session (tests, status).
func (r *Relay) Session() *tunnel.Session { return r.sess }

// SessionID returns the hex session id.
func (r *Relay) SessionID() string {
	id := r.sess.ID()
	return hex.EncodeToString(id[:])
}

func (r *Relay) dialTunnel(ctx context.Context) (net.Conn, error) {
	if r.cfg.DialTunnel != nil {
		return r.cfg.DialTunnel(ctx, r.cfg.Gateway)
	}
	var d net.Dialer
	d.SetMultipathTCP(false)
	return d.DialContext(ctx, "tcp", r.cfg.Gateway)
}

func (r *Relay) openUpstream(st *tunnel.Stream) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := r.up.DialContext(ctx, "tcp", r.cfg.Upstream)
	if err != nil {
		r.log.Warn("upstream dial failed", "stream", st.ID(), "err", err)
		st.Reset(wire.ResetDialFailed, err.Error())
		return
	}
	if r.cfg.ProxyProtocol {
		a := st.Addrs()
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write(proxyproto.Header(a.Src, a.Dst)); err != nil {
			c.Close()
			st.Reset(wire.ResetDialFailed, "proxy header: "+err.Error())
			return
		}
		c.SetWriteDeadline(time.Time{})
	}
	st.AttachLocal(c)
}

func (r *Relay) setState(s string) {
	r.mu.Lock()
	r.state = s
	r.mu.Unlock()
}

func (r *Relay) signalWake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run maintains the tunnel until ctx is cancelled. Cancelling ctx aborts the
// tunnel and closes all streams (use Shutdown first for a clean exit).
func (r *Relay) Run(ctx context.Context) {
	backoff := r.cfg.BackoffMin
	for {
		r.mu.Lock()
		if ctx.Err() != nil {
			r.mu.Unlock()
			break
		}
		if r.quiesced {
			r.state = "quiesced"
			if r.final {
				r.state = "stopped"
			}
			r.mu.Unlock()
			select {
			case <-r.wake:
				continue
			case <-ctx.Done():
				continue
			}
		}
		actx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		r.attemptCancel, r.attemptDone = cancel, done
		r.stop = stopAbort
		r.state = "connecting"
		r.mu.Unlock()

		connected, err := r.attempt(actx)

		r.mu.Lock()
		r.attemptCancel, r.attemptDone = nil, nil
		if err != nil {
			r.lastErr = err.Error()
		}
		quiesced := r.quiesced
		r.mu.Unlock()
		cancel()
		close(done)

		if quiesced || ctx.Err() != nil {
			continue
		}
		var delay time.Duration
		switch {
		case connected:
			backoff = r.cfg.BackoffMin
		case errors.Is(err, tunnel.ErrNewSession):
			// The gateway didn't know the session: reconnect at once with the new id.
		default:
			delay = backoff
			if errors.Is(err, tunnel.ErrRejected) || errors.Is(err, wire.ErrAuth) {
				delay = r.cfg.BackoffMax
			}
			backoff = min(backoff*2, r.cfg.BackoffMax)
			delay = time.Duration(float64(delay) * (0.8 + 0.4*rand.Float64()))
			r.log.Warn("tunnel connect failed", "gateway", r.cfg.Gateway, "err", err, "retry_in", delay.Round(time.Millisecond))
		}
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-r.wake:
			case <-ctx.Done():
			}
			t.Stop()
		}
	}
	r.sess.CloseAll()
}

// attempt dials, handshakes and runs one tunnel connection. On a fresh
// process it first waits for the server to be ready.
func (r *Relay) attempt(ctx context.Context) (connected bool, err error) {
	if !r.ready.Load() {
		r.setState("waiting-for-server")
		if err := r.waitReady(ctx); err != nil {
			return false, err
		}
		r.setState("connecting")
	}
	dctx, dcancel := context.WithTimeout(ctx, r.cfg.Tunnel.HandshakeTimeout)
	nc, err := r.dialTunnel(dctx)
	dcancel()
	if err != nil {
		return false, err
	}
	stopAbortHandshake := context.AfterFunc(ctx, func() { nc.Close() })
	c, err := r.sess.ClientHandshake(nc, r.cfg.Backend)
	stopAbortHandshake()
	if err != nil {
		return false, err
	}
	r.setState("connected")
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run() }()
	select {
	case err = <-runDone:
		return true, err
	case <-ctx.Done():
	}
	r.mu.Lock()
	reason := r.stop
	r.mu.Unlock()
	switch reason {
	case stopQuiesce:
		c.GoAway(0, "quiesce", time.Second)
	case stopFinal:
		c.GoAway(wire.GoAwayFinal, "server exited", 2*time.Second)
	default:
		c.Close()
	}
	return true, <-runDone
}

// waitReady polls the upstream until the readiness check passes or ctx ends.
// It runs inside an attempt, so Quiesce cancels it and waits for the probe's
// loopback socket to be closed before acknowledging.
func (r *Relay) waitReady(ctx context.Context) error {
	start := time.Now()
	lastLog := start
	r.log.Info("waiting for the server to become ready", "check", r.cfg.Ready.String(), "upstream", r.cfg.Upstream)
	for {
		err := r.probe(ctx)
		r.probes.Add(1)
		if err == nil {
			r.ready.Store(true)
			r.log.Info("server ready", "check", r.cfg.Ready.String(), "waited", time.Since(start).Round(time.Millisecond))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			r.log.Info("server not ready yet", "waited", time.Since(start).Round(time.Second), "err", err)
		} else {
			r.log.Debug("server not ready", "err", err)
		}
		t := time.NewTimer(r.cfg.ReadyInterval)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// probe runs one readiness check against the upstream.
func (r *Relay) probe(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := r.up.DialContext(pctx, "tcp", r.cfg.Upstream)
	if err != nil {
		return err
	}
	defer c.Close()
	if r.cfg.Ready != ReadyMinecraft {
		return nil
	}
	stop := context.AfterFunc(pctx, func() { c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if r.cfg.ProxyProtocol {
		// LOCAL: the server uses the socket's own addresses.
		if _, err := c.Write(proxyproto.Header(netip.AddrPort{}, netip.AddrPort{})); err != nil {
			return err
		}
	}
	host, port := "127.0.0.1", uint16(25565)
	if ap, err := netip.ParseAddrPort(r.cfg.Upstream); err == nil {
		host, port = ap.Addr().String(), ap.Port()
	}
	_, err = mcproto.QueryStatus(c, -1, host, port)
	return err
}

// Ready reports whether the readiness check has passed.
func (r *Relay) Ready() bool { return r.ready.Load() }

// Quiesce closes the tunnel, stops reconnecting and waits until no
// non-loopback socket of this process remains (if CheckSockets).
func (r *Relay) Quiesce(ctx context.Context) error {
	r.mu.Lock()
	if !r.quiesced {
		r.quiesced = true
		r.quiescedAt = time.Now()
		r.log.Info("quiesce requested", "auto_resume_after", r.cfg.QuiesceTimeout)
	}
	r.stop = stopQuiesce
	cancel, done := r.attemptCancel, r.attemptDone
	r.autoGen++
	gen := r.autoGen
	if r.autoTimer != nil {
		r.autoTimer.Stop()
	}
	r.autoTimer = time.AfterFunc(r.cfg.QuiesceTimeout, func() {
		r.mu.Lock()
		stale := gen != r.autoGen || !r.quiesced || r.final
		r.mu.Unlock()
		if !stale {
			r.log.Warn("no resume within quiesce timeout; resuming on our own")
			r.Resume()
		}
	})
	r.mu.Unlock()
	r.signalWake()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("waiting for tunnel to close: %w", ctx.Err())
		}
	}
	// No half-open (SYN_SENT) upstream connection may be in the dump.
	dialsDone := make(chan struct{})
	go func() { r.dials.Wait(); close(dialsDone) }()
	select {
	case <-dialsDone:
	case <-ctx.Done():
		return fmt.Errorf("waiting for upstream dials: %w", ctx.Err())
	}
	if r.cfg.CheckSockets {
		if err := waitNoSockets(ctx, r.log); err != nil {
			return err
		}
	}
	r.setState("quiesced")
	r.log.Info("quiesced", "streams", r.sess.NumStreams())
	return nil
}

// Resume re-enables the tunnel; the manager reconnects immediately.
func (r *Relay) Resume() {
	r.mu.Lock()
	if r.final {
		r.mu.Unlock()
		return
	}
	was := r.quiesced
	r.quiesced = false
	r.autoGen++
	if r.autoTimer != nil {
		r.autoTimer.Stop()
		r.autoTimer = nil
	}
	if was {
		r.log.Info("resume", "quiesced_for", time.Since(r.quiescedAt).Round(time.Millisecond))
	}
	r.mu.Unlock()
	r.signalWake()
}

// Shutdown is used when the server process has exited: it waits briefly for
// stream closes to be sent, sends a final GOAWAY and stops reconnecting.
func (r *Relay) Shutdown(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && r.sess.Connected() && !r.sess.Drained() {
		time.Sleep(20 * time.Millisecond)
	}
	r.mu.Lock()
	r.final = true
	r.quiesced = true
	r.stop = stopFinal
	cancel, done := r.attemptCancel, r.attemptDone
	r.mu.Unlock()
	r.signalWake()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Until(deadline) + 2*time.Second):
		}
	}
}

// Status is the control socket's status document.
type Status struct {
	Session        string    `json:"session"`
	Gateway        string    `json:"gateway"`
	State          string    `json:"state"`
	Connected      bool      `json:"connected"`
	Streams        int       `json:"streams"`
	Connects       uint64    `json:"connects"`
	Disconnects    uint64    `json:"disconnects"`
	LastConnect    time.Time `json:"last_connect,omitzero"`
	LastDisconnect time.Time `json:"last_disconnect,omitzero"`
	QuiescedForMS  int64     `json:"quiesced_for_ms,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	ChildPID       int64     `json:"child_pid,omitempty"`
	Ready          bool      `json:"ready"`
}

// Status returns a snapshot.
func (r *Relay) Status() Status {
	s := r.sess.Stats()
	r.mu.Lock()
	defer r.mu.Unlock()
	st := Status{Gateway: r.cfg.Gateway, Session: r.SessionID(), State: r.state, Connected: s.Connected, Streams: s.Streams,
		Connects: s.Connects, Disconnects: s.Disconnects, LastConnect: s.LastConnect,
		LastDisconnect: s.LastDisconnect, LastError: r.lastErr, ChildPID: r.ChildPID.Load(), Ready: r.ready.Load()}
	if r.quiesced {
		st.QuiescedForMS = time.Since(r.quiescedAt).Milliseconds()
	}
	return st
}
