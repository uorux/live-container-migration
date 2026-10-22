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
	// DialTunnel overrides how the gateway is dialled (tests). The default is a
	// plain-TCP (MPTCP disabled) dialer.
	DialTunnel func(ctx context.Context, addr string) (net.Conn, error)
	Logger     *slog.Logger
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
	cfg.Tunnel.Logger = cfg.Logger
	cfg.Tunnel.Defaults()
	r := &Relay{cfg: cfg, log: cfg.Logger, wake: make(chan struct{}, 1), state: "connecting"}
	// Never MPTCP: CRIU cannot checkpoint IPPROTO_MPTCP sockets.
	r.up.SetMultipathTCP(false)
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
		if connected {
			backoff = r.cfg.BackoffMin
		} else {
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

// attempt dials, handshakes and runs one tunnel connection.
func (r *Relay) attempt(ctx context.Context) (connected bool, err error) {
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
		for {
			socks, err := NonLoopbackSockets()
			if err != nil {
				r.log.Warn("socket check unavailable", "err", err)
				break
			}
			if len(socks) == 0 {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("non-loopback sockets remain: %v", socks)
			case <-time.After(20 * time.Millisecond):
			}
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
}

// Status returns a snapshot.
func (r *Relay) Status() Status {
	s := r.sess.Stats()
	r.mu.Lock()
	defer r.mu.Unlock()
	st := Status{Session: r.SessionID(), State: r.state, Connected: s.Connected, Streams: s.Streams,
		Connects: s.Connects, Disconnects: s.Disconnects, LastConnect: s.LastConnect,
		LastDisconnect: s.LastDisconnect, LastError: r.lastErr, ChildPID: r.ChildPID.Load()}
	if r.quiesced {
		st.QuiescedForMS = time.Since(r.quiescedAt).Milliseconds()
	}
	return st
}
