package tunnel

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/wire"
)

var (
	errDetached = errors.New("connection detached from session")
	errReplaced = errors.New("replaced by a newer tunnel connection")
	errDead     = errors.New("tunnel dead: no frames received within timeout")
	errShutdown = errors.New("shutdown timeout")
)

// Conn is one tunnel TCP connection attached to a Session.
type Conn struct {
	s      *Session
	nc     net.Conn
	r      *wire.Reader
	notify chan struct{}
	done   chan struct{}

	// guarded by s.mu
	ctrl       []byte // encoded control frames waiting for the writer
	goaway     []byte // GOAWAY payload once requested
	goawaySent bool

	// guarded by s.mu: frames received, and (gateway) whether the handshake
	// answered NEW
	frames      int
	answeredNew bool

	once     sync.Once
	errMu    sync.Mutex
	err      error
	lastRecv atomic.Int64
	pingSeq  uint64
}

func newConn(s *Session, nc net.Conn, r *wire.Reader) *Conn {
	c := &Conn{s: s, nc: nc, r: r, notify: make(chan struct{}, 1), done: make(chan struct{})}
	c.lastRecv.Store(time.Now().UnixNano())
	return c
}

// fail closes the connection once. It never takes s.mu.
func (c *Conn) fail(err error) {
	c.once.Do(func() {
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		close(c.done)
		c.nc.Close()
	})
}

// Err returns the error that ended the connection (nil while running).
func (c *Conn) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

// Done is closed when the connection has failed or been closed.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Close aborts the connection immediately.
func (c *Conn) Close() { c.fail(net.ErrClosed) }

// Run serves the connection until it ends; the session is detached before Run returns
// and all of the connection's goroutines have exited.
func (c *Conn) Run() error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.writer() }()
	go func() { defer wg.Done(); c.watchdog() }()
	for {
		f, err := c.r.ReadFrame()
		if err != nil {
			c.fail(err)
			break
		}
		c.lastRecv.Store(time.Now().UnixNano())
		if err := c.s.handleFrame(c, f); err != nil {
			c.fail(err)
			break
		}
	}
	wg.Wait()
	c.s.detach(c)
	return c.Err()
}

func (c *Conn) writer() {
	var buf []byte
	for {
		var final bool
		buf, final = c.s.collect(c, buf[:0])
		if len(buf) > 0 {
			if _, err := c.nc.Write(buf); err != nil {
				c.fail(err)
				return
			}
		}
		if final {
			if cw, ok := c.nc.(closeWriter); ok {
				cw.CloseWrite()
			}
			return
		}
		if cap(buf) > 1<<20 {
			buf = nil
		}
		if len(buf) == 0 {
			select {
			case <-c.notify:
			case <-c.done:
				return
			}
		}
	}
}

func (c *Conn) watchdog() {
	cfg := &c.s.cfg
	tick := min(cfg.PingInterval, cfg.DeadTimeout/4)
	t := time.NewTicker(tick)
	defer t.Stop()
	lastPing := time.Now()
	for {
		select {
		case <-c.done:
			return
		case now := <-t.C:
			if now.Sub(time.Unix(0, c.lastRecv.Load())) > cfg.DeadTimeout {
				c.fail(errDead)
				return
			}
			if now.Sub(lastPing) >= cfg.PingInterval {
				lastPing = now
				c.s.mu.Lock()
				if c.s.conn == c {
					c.pingSeq++
					c.ctrl = wire.AppendFrame(c.ctrl, wire.TPing, 0, wire.PingPayload(c.pingSeq))
					c.s.kick()
				}
				c.s.mu.Unlock()
			}
		}
	}
}

// GoAway sends a GOAWAY after any pending frames, half-closes the socket and
// lets the peer close; if that takes longer than timeout the connection is
// aborted. It returns immediately; wait for Run to return.
func (c *Conn) GoAway(flags uint8, msg string, timeout time.Duration) {
	c.s.mu.Lock()
	if c.s.conn == c && c.goaway == nil {
		c.goaway = wire.GoAwayPayload(flags, msg)
		c.s.kick()
	} else if c.s.conn != c {
		c.s.mu.Unlock()
		c.fail(errDetached)
		return
	}
	c.s.mu.Unlock()
	t := time.AfterFunc(timeout, func() { c.fail(errShutdown) })
	go func() { <-c.done; t.Stop() }()
}

// ---- handshakes ----

// SessionLookup maps a backend name from HELLO to its session (gateway side).
type SessionLookup func(backend string) *Session

// ServerHandshake runs the gateway side of the handshake on nc and returns an
// attached Conn ready to Run. On error nc is closed.
func ServerHandshake(nc net.Conn, token []byte, timeout time.Duration, lookup SessionLookup) (*Conn, error) {
	nc.SetDeadline(time.Now().Add(timeout))
	ch := wire.Challenge{Version: wire.Version}
	rand.Read(ch.Nonce[:])
	if _, err := nc.Write(wire.AppendFrame(nil, wire.TChallenge, 0, ch.Append(nil))); err != nil {
		nc.Close()
		return nil, err
	}
	r := wire.NewReader(nc)
	f, err := r.ReadFrame()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if f.Type != wire.THello {
		nc.Close()
		return nil, fmt.Errorf("%w: expected HELLO, got %v", wire.ErrProtocol, f.Type)
	}
	h, err := wire.ParseHello(f.Payload, token, ch.Nonce)
	if err != nil {
		nc.Close()
		return nil, err
	}
	reject := func(msg string) (*Conn, error) {
		a := wire.HelloAck{Status: wire.AckRejected, Message: msg}
		nc.Write(wire.AppendFrame(nil, wire.THelloAck, 0, a.Marshal(token, ch.Nonce, h.Session)))
		nc.Close()
		return nil, errors.New("rejected relay: " + msg)
	}
	if h.Version != wire.Version {
		return reject(fmt.Sprintf("unsupported version %d", h.Version))
	}
	if h.Window < 4096 {
		return reject("window too small")
	}
	s := lookup(h.Backend)
	if s == nil {
		return reject(fmt.Sprintf("unknown backend %q", h.Backend))
	}
	if s.isFrozen() {
		// Being handed off: look like a gateway that went away, so the
		// relay keeps retrying until it reaches the one taking over.
		nc.Close()
		return nil, errFrozen
	}
	c := newConn(s, nc, r)
	ack := s.gatewayAccept(c, &h)
	if _, err := nc.Write(wire.AppendFrame(nil, wire.THelloAck, 0, ack.Marshal(token, ch.Nonce, h.Session))); err != nil {
		c.fail(err)
		s.detach(c)
		return nil, err
	}
	nc.SetDeadline(time.Time{})
	s.log.Info("relay attached", "remote", nc.RemoteAddr().String(), "resumed", ack.Status == wire.AckResumed,
		"relay_streams", len(h.Streams), "resumed_streams", len(ack.Streams))
	return c, nil
}

// ErrRejected is returned by ClientHandshake when the gateway refuses the relay.
var ErrRejected = errors.New("gateway rejected relay")

// ErrNewSession is returned by ClientHandshake when the gateway did not know
// the session although a gateway had accepted it before: the session has a
// new id now (see Session.renewID), and the caller should reconnect at once.
var ErrNewSession = errors.New("gateway does not know this session; new session id")

// ClientHandshake runs the relay side of the handshake on nc and returns an
// attached Conn ready to Run. On error nc is closed.
func (s *Session) ClientHandshake(nc net.Conn, backend string) (*Conn, error) {
	nc.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
	r := wire.NewReader(nc)
	fail := func(err error) (*Conn, error) { nc.Close(); return nil, err }
	f, err := r.ReadFrame()
	if err != nil {
		return fail(err)
	}
	if f.Type != wire.TChallenge {
		return fail(fmt.Errorf("%w: expected CHALLENGE, got %v", wire.ErrProtocol, f.Type))
	}
	ch, err := wire.ParseChallenge(f.Payload)
	if err != nil {
		return fail(err)
	}
	if ch.Version != wire.Version {
		return fail(fmt.Errorf("%w: gateway speaks version %d", wire.ErrProtocol, ch.Version))
	}
	s.mu.Lock()
	h := wire.Hello{Version: wire.Version, Session: s.id, Backend: backend,
		Window: uint32(s.cfg.Window), MaxSeen: s.maxSeen, Streams: s.relayView()}
	s.mu.Unlock()
	if _, err := nc.Write(wire.AppendFrame(nil, wire.THello, 0, h.Marshal(s.cfg.Token, ch.Nonce))); err != nil {
		return fail(err)
	}
	f, err = r.ReadFrame()
	if err != nil {
		return fail(err)
	}
	if f.Type != wire.THelloAck {
		return fail(fmt.Errorf("%w: expected HELLO_ACK, got %v", wire.ErrProtocol, f.Type))
	}
	a, err := wire.ParseHelloAck(f.Payload, s.cfg.Token, ch.Nonce, h.Session)
	if err != nil {
		return fail(err)
	}
	if a.Status == wire.AckRejected {
		return fail(fmt.Errorf("%w: %s", ErrRejected, a.Message))
	}
	if a.Status != wire.AckResumed && a.Status != wire.AckNew {
		return fail(fmt.Errorf("%w: bad hello-ack status %d", wire.ErrProtocol, a.Status))
	}
	if a.Window < 4096 {
		return fail(fmt.Errorf("%w: window too small", wire.ErrProtocol))
	}
	if a.Status == wire.AckNew && s.renewID() {
		// Hang up before processing anything, saying so: the gateway then
		// opens the streams it sent OPENs for on this connection again on
		// the next one, which presents the new id.
		nc.Write(wire.AppendFrame(nil, wire.TGoAway, 0, wire.GoAwayPayload(wire.GoAwayRebind, "new session id")))
		if cw, ok := nc.(closeWriter); ok {
			cw.CloseWrite()
			nc.SetReadDeadline(time.Now().Add(time.Second))
			io.Copy(io.Discard, nc)
		}
		return fail(ErrNewSession)
	}
	nc.SetDeadline(time.Time{})
	c := newConn(s, nc, r)
	s.relayApply(c, &a)
	return c, nil
}
