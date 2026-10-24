// Package tunnel implements the resumable, multiplexed stream session shared
// by lcm-gateway and lcm-relay (protocol: docs/tunnel-protocol.md).
//
// A Session outlives tunnel connections: streams, their send buffers and
// receive offsets live in the Session, and a Conn (one tunnel TCP connection)
// is attached to it while it is up. When a Conn dies, streams keep their state
// and local sockets; the next Conn reconciles offsets and retransmits.
package tunnel

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"lcm/internal/wire"
)

// Role says which end of the tunnel a Session is.
type Role int

const (
	RoleGateway Role = iota
	RoleRelay
)

func (r Role) String() string {
	if r == RoleGateway {
		return "gateway"
	}
	return "relay"
}

// Config holds tunables common to both ends.
type Config struct {
	Token []byte
	// Window is both the per-stream send-buffer cap (bytes read from the local
	// socket but not yet acknowledged by the peer) and the receive window
	// advertised to the peer.
	Window int
	// PingInterval is how often a PING is sent on an idle or busy tunnel.
	PingInterval time.Duration
	// DeadTimeout: a tunnel connection that delivered no frame for this long is closed.
	DeadTimeout time.Duration
	// AckDelay bounds how long a small acknowledgement may be held back.
	AckDelay time.Duration
	// HandshakeTimeout bounds CHALLENGE/HELLO/HELLO_ACK.
	HandshakeTimeout time.Duration
	Logger           *slog.Logger
	// Observer, if set, is told about tunnel and stream events (gateway metrics).
	Observer Observer
}

// Observer receives session events. Its methods are called with the session
// lock held: they must be quick and must not call back into the session.
type Observer interface {
	// TunnelAttached is called after each successful handshake. resumed is
	// false for a NEW session. outage is how long no working tunnel was
	// attached before this one: since the previous detach, or, if this
	// connection replaced one that was still attached, since that one last
	// received a frame. It is 0 if no tunnel was ever attached.
	TunnelAttached(resumed bool, outage time.Duration)
	// StreamClosed is called once for every stream removed from the session.
	StreamClosed(why CloseReason)
}

// CloseReason says why a stream was removed. "Local" is the player on the
// gateway and the server's loopback connection on the relay.
type CloseReason uint8

const (
	// CloseLocalFin: graceful close, the local side sent its FIN first.
	CloseLocalFin CloseReason = iota
	// ClosePeerFin: graceful close initiated by the peer, or the peer forgot
	// the stream (closed it while the two sides were apart, FINAL GOAWAY).
	ClosePeerFin
	// CloseLocalError: the local socket failed (RST, read/write error).
	CloseLocalError
	// ClosePeerReset: the peer sent RESET.
	ClosePeerReset
	// CloseAborted: Stream.Reset was called; the caller knows why.
	CloseAborted
	// CloseSessionLost: the other side presented a different session
	// (restarted), so streams it had seen are gone.
	CloseSessionLost
	// CloseProtocol: window overrun or inconsistent offsets on reconnect.
	CloseProtocol
	// CloseShutdown: CloseAll.
	CloseShutdown
	// CloseHandedOff: Freeze moved the stream to another gateway.
	CloseHandedOff
)

func (r CloseReason) String() string {
	switch r {
	case CloseLocalFin:
		return "local_fin"
	case ClosePeerFin:
		return "peer_fin"
	case CloseLocalError:
		return "local_error"
	case ClosePeerReset:
		return "peer_reset"
	case CloseAborted:
		return "aborted"
	case CloseSessionLost:
		return "session_lost"
	case CloseProtocol:
		return "protocol"
	case CloseShutdown:
		return "shutdown"
	case CloseHandedOff:
		return "handed_off"
	}
	return fmt.Sprintf("reason(%d)", uint8(r))
}

// Defaults fills unset fields.
func (c *Config) Defaults() {
	if c.Window <= 0 {
		c.Window = 1 << 20
	}
	if c.PingInterval <= 0 {
		c.PingInterval = 2 * time.Second
	}
	if c.DeadTimeout <= 0 {
		c.DeadTimeout = 10 * time.Second
	}
	if c.AckDelay <= 0 {
		c.AckDelay = 5 * time.Millisecond
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c *Config) ackThreshold() int {
	return min(c.Window/4, 64<<10)
}

// Stream is one player connection: a pair of byte streams between a local
// socket (player on the gateway, loopback to the server on the relay) and the peer.
type Stream struct {
	s     *Session
	id    uint32
	open  wire.Open
	local net.Conn // nil on the relay until the upstream dial completes

	// gateway: OPEN not yet written on the current tunnel connection.
	openPending bool
	// gateway: an OPEN was written on some connection of the current session id.
	everOpened bool

	// local -> peer
	tx       byteQueue // bytes [txBase, txEnd)
	txBase   uint64    // first unacknowledged offset
	txNext   uint64    // next offset to transmit on the current connection
	txFin    bool      // local EOF seen; FIN offset is txEnd()
	finSent  bool
	finAcked bool

	// peer -> local
	rx           byteQueue // undelivered bytes [rxDelivered, rxNext)
	rxNext       uint64
	rxDelivered  uint64
	rxFin        bool
	finDelivered bool // CloseWrite applied to local
	peerGone     bool // peer forgot or reset the stream: deliver what is queued, then close
	goneReason   CloseReason
	// localFinFirst: the local EOF was seen before the peer's CLOSE.
	localFinFirst bool
	// localErr is set when the local socket failed: bytes already read from it
	// are still sent, followed by a RESET instead of a CLOSE.
	localErr string

	ackSent    uint64
	ackFinSent bool
	ackDirty   bool

	closed bool
	txCond sync.Cond // space in tx, or closed
	rxCond sync.Cond // data/fin in rx, or closed

	// pre is written to local before rx and outside the offsets: bytes an
	// earlier gateway had already acknowledged but its socket never sent
	// (a handed-off connection's unsent tail, see Import).
	pre []byte

	maxTxBuffered int
	created       time.Time
}

func (st *Stream) txEnd() uint64 { return st.txBase + uint64(st.tx.Len()) }

// ID returns the stream id.
func (st *Stream) ID() uint32 { return st.id }

// Addrs returns the player addresses carried by OPEN.
func (st *Stream) Addrs() wire.Open { return st.open }

// Session is the long-lived state of one gateway<->relay pairing.
type Session struct {
	role Role
	cfg  Config
	log  *slog.Logger

	mu sync.Mutex
	id [wire.SessionIDLen]byte
	// bound: a relay has presented id (gateway); a gateway has accepted id
	// (relay).
	bound   bool
	streams map[uint32]*Stream
	order   []uint32 // ascending stream ids
	rr      int
	conn    *Conn
	last    *Conn // the connection attached most recently (may be gone)
	// peerWindow is the per-stream window the peer advertised.
	peerWindow uint64
	nextID     uint32 // gateway: next stream id to allocate
	maxSeen    uint32 // relay: highest OPEN processed

	ackTimer *time.Timer
	ackFlush bool

	// relay: called for each OPEN with s.mu held, so it must not block or call
	// back into the session synchronously; it should start a goroutine that
	// eventually calls AttachLocal or Reset.
	onOpen func(*Stream)

	stats   Stats
	created time.Time

	// frozen: the session is being handed off (Freeze): no tunnel
	// connection, and the local pumps stop at once.
	frozen bool
	pumps  sync.WaitGroup // running readPump/writePump goroutines
}

// Stats are counters for status reporting and tests.
type Stats struct {
	Connected      bool
	Streams        int
	Connects       uint64 // successful handshakes
	NewSessions    uint64 // handshakes that started a NEW session
	Resumes        uint64 // handshakes that RESUMED the session
	Disconnects    uint64
	MaxTxBuffered  int // high-water mark of any stream's unacked send buffer
	TxBuffered     int // bytes read from local sockets, not yet acknowledged by the peer (all streams, now)
	RxQueued       int // bytes received from the peer, not yet written locally (all streams, now)
	BytesFromLocal uint64
	BytesToLocal   uint64
	LastConnect    time.Time
	LastDisconnect time.Time
	// DownSince is when the tunnel went down (session creation if it was
	// never up); zero while connected.
	DownSince time.Time
	// LastOutage is the length of the most recent outage that ended in a
	// resume (see Observer.TunnelAttached).
	LastOutage time.Duration
	// PeerExited (gateway): the last tunnel connection ended with the relay's
	// FINAL GOAWAY (its server process exited). Cleared when a relay attaches.
	PeerExited bool
	// Bound (gateway): a relay has presented a session id; it may be away now.
	Bound bool
}

// NewSession creates a session. For the relay, id is the session id it will
// present; onOpen handles OPENs. For the gateway, id is ignored until a relay binds.
func NewSession(role Role, cfg Config, id [wire.SessionIDLen]byte, onOpen func(*Stream)) *Session {
	cfg.Defaults()
	s := &Session{
		role:       role,
		cfg:        cfg,
		log:        cfg.Logger.With("side", role.String()),
		id:         id,
		streams:    make(map[uint32]*Stream),
		nextID:     1,
		onOpen:     onOpen,
		peerWindow: uint64(cfg.Window),
		created:    time.Now(),
	}
	return s
}

// ID returns the current session id.
func (s *Session) ID() [wire.SessionIDLen]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// Stats returns a snapshot.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.Connected = s.conn != nil
	st.Bound = s.bound
	st.Streams = len(s.streams)
	for _, x := range s.streams {
		st.TxBuffered += x.tx.Len()
		st.RxQueued += x.rx.Len()
	}
	if !st.Connected {
		st.DownSince = s.stats.LastDisconnect
		if st.DownSince.IsZero() {
			st.DownSince = s.created
		}
	}
	return st
}

// Connected reports whether a tunnel connection is attached.
func (s *Session) Connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn != nil
}

// NumStreams returns the number of live streams.
func (s *Session) NumStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) newStream(id uint32, open wire.Open, local net.Conn) *Stream {
	st := &Stream{s: s, id: id, open: open, local: local, created: time.Now()}
	st.txCond.L = &s.mu
	st.rxCond.L = &s.mu
	s.streams[id] = st
	// ids are allocated/received in increasing order, so append keeps order sorted.
	if n := len(s.order); n > 0 && s.order[n-1] > id {
		i, _ := slices.BinarySearch(s.order, id)
		s.order = slices.Insert(s.order, i, id)
	} else {
		s.order = append(s.order, id)
	}
	return st
}

// kick wakes the current connection's writer. Caller holds s.mu.
func (s *Session) kick() {
	if c := s.conn; c != nil {
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}
}

// ---- gateway: new player ----

// AddStream registers a new player connection (gateway only). The stream is
// opened on the tunnel now if connected, otherwise after the next resume.
func (s *Session) AddStream(local net.Conn, open wire.Open) *Stream {
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	st := s.newStream(id, open, local)
	st.openPending = true
	s.kick()
	s.mu.Unlock()
	st.startPumps()
	return st
}

func (st *Stream) startPumps() {
	st.s.pumps.Add(2)
	go st.readPump()
	go st.writePump()
}

// ---- relay: upstream connected ----

// AttachLocal gives a relay stream its upstream socket and starts its pumps.
// If the stream was closed meanwhile, local is closed and false is returned.
func (st *Stream) AttachLocal(local net.Conn) bool {
	s := st.s
	s.mu.Lock()
	if st.closed {
		s.mu.Unlock()
		local.Close()
		return false
	}
	st.local = local
	s.mu.Unlock()
	st.startPumps()
	return true
}

// Reset aborts the stream and tells the peer. It reports whether this call
// closed the stream (false if it was already closed).
func (st *Stream) Reset(code uint8, msg string) bool {
	s := st.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.closed {
		return false
	}
	s.resetStream(st, code, msg, CloseAborted)
	return true
}

// ---- stream teardown (caller holds s.mu) ----

func (s *Session) removeStream(st *Stream, abort bool, why CloseReason) {
	if st.closed {
		return
	}
	st.closed = true
	if o := s.cfg.Observer; o != nil {
		o.StreamClosed(why)
	}
	delete(s.streams, st.id)
	if i, ok := slices.BinarySearch(s.order, st.id); ok {
		s.order = slices.Delete(s.order, i, i+1)
	}
	st.txCond.Broadcast()
	st.rxCond.Broadcast()
	if l := st.local; l != nil {
		go closeLocal(l, abort)
	}
	s.log.Debug("stream closed", "stream", st.id, "abort", abort, "why", why.String(),
		"tx", st.txEnd(), "rx", st.rxNext, "age", time.Since(st.created).Round(time.Millisecond))
}

// lingerTimeout bounds how long a gracefully closed local socket is drained.
const lingerTimeout = 10 * time.Second

// closeLocal closes a local socket. abort sends RST. Otherwise the close is
// graceful: closing a socket that still has unread input makes the kernel
// send RST and discard data still in its send buffer (e.g. the server's final
// kick message to a player who is still sending). So: half-close (queued data,
// then FIN, still go out), drain input until the peer closes, then close.
// TCPLike is what closeLocal needs for graceful and abortive closes;
// *net.TCPConn implements it, and so may wrappers (e.g. the gateway's
// Minecraft hold wrapper) that want the same treatment.
type TCPLike interface {
	net.Conn
	CloseWrite() error
	SetLinger(sec int) error
}

func closeLocal(c net.Conn, abort bool) {
	tc, ok := c.(TCPLike)
	if abort || !ok {
		if ok {
			tc.SetLinger(0)
		}
		c.Close()
		return
	}
	tc.CloseWrite()
	tc.SetReadDeadline(time.Now().Add(lingerTimeout))
	io.Copy(io.Discard, tc)
	tc.Close()
}

// resetStream aborts st locally and queues a RESET for the peer if the peer
// knows the stream.
func (s *Session) resetStream(st *Stream, code uint8, msg string, why CloseReason) {
	if st.closed {
		return
	}
	peerKnows := s.role == RoleRelay || st.everOpened
	if peerKnows && s.conn != nil && !st.openPending {
		s.conn.ctrl = wire.AppendFrame(s.conn.ctrl, wire.TReset, st.id, wire.ResetPayload(code, msg))
		s.kick()
	}
	s.log.Info("stream reset", "stream", st.id, "code", code, "reason", msg)
	s.removeStream(st, true, why)
}

// failLocal handles a failed local socket (RST, write error): the socket is
// closed now, data already read from it is still sent to the peer (e.g. the
// server's final kick message), then a RESET. Caller holds s.mu.
func (s *Session) failLocal(st *Stream, msg string) {
	if st.closed || st.localErr != "" {
		return
	}
	if s.role == RoleGateway && (st.openPending || !st.everOpened) {
		s.removeStream(st, true, CloseLocalError) // the relay does not know this stream
		return
	}
	s.log.Info("local socket failed", "stream", st.id, "err", msg)
	st.localErr = msg
	st.rx.Discard(st.rx.Len())
	st.txCond.Broadcast()
	st.rxCond.Broadcast()
	if l := st.local; l != nil {
		go closeLocal(l, true)
	}
	s.kick()
}

// peerForgot handles a stream the peer no longer knows: deliver whatever is
// already queued for the local side, then close it.
func (s *Session) peerForgot(st *Stream) {
	if st.closed {
		return
	}
	why := ClosePeerFin
	if st.localErr != "" {
		why = CloseLocalError
	}
	if st.rx.Len() == 0 || st.local == nil {
		s.removeStream(st, false, why)
		return
	}
	st.peerGone = true
	st.goneReason = why
	st.rxCond.Broadcast()
}

func (s *Session) maybeFinish(st *Stream) {
	if !st.closed && st.txFin && st.finAcked && st.finDelivered && st.ackFinSent {
		why := ClosePeerFin
		if st.localFinFirst {
			why = CloseLocalFin
		}
		s.removeStream(st, false, why)
	}
}

// ---- local pumps ----

func (st *Stream) readPump() {
	s := st.s
	defer s.pumps.Done()
	buf := make([]byte, 32<<10)
	for {
		s.mu.Lock()
		for !st.closed && !s.frozen && st.localErr == "" && st.tx.Len() >= s.cfg.Window {
			st.txCond.Wait()
		}
		if st.closed || st.txFin || st.localErr != "" || s.frozen {
			s.mu.Unlock()
			return
		}
		room := s.cfg.Window - st.tx.Len()
		s.mu.Unlock()

		n, err := st.local.Read(buf[:min(room, len(buf))])

		s.mu.Lock()
		if st.closed || st.localErr != "" {
			s.mu.Unlock()
			return
		}
		if n > 0 {
			st.tx.Append(buf[:n])
			s.stats.BytesFromLocal += uint64(n)
			if l := st.tx.Len(); l > st.maxTxBuffered {
				st.maxTxBuffered = l
				if l > s.stats.MaxTxBuffered {
					s.stats.MaxTxBuffered = l
				}
			}
			s.kick()
		}
		if s.frozen {
			// Freeze expired the socket's deadline: not a failure. What was
			// read is kept; the socket itself is handed off.
			s.mu.Unlock()
			return
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				st.txFin = true
				st.localFinFirst = !st.rxFin
				s.kick()
			} else {
				s.failLocal(st, "local read: "+err.Error())
			}
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

type closeWriter interface{ CloseWrite() error }

func (st *Stream) writePump() {
	s := st.s
	defer s.pumps.Done()
	buf := make([]byte, 32<<10)
	for {
		s.mu.Lock()
		for !st.closed && !s.frozen && st.localErr == "" && len(st.pre) == 0 && st.rx.Len() == 0 && !st.peerGone && !(st.rxFin && !st.finDelivered) {
			st.rxCond.Wait()
		}
		if st.closed || st.localErr != "" || s.frozen {
			s.mu.Unlock()
			return
		}
		if len(st.pre) > 0 {
			p := st.pre
			s.mu.Unlock()
			w, err := st.local.Write(p)
			s.mu.Lock()
			st.pre = st.pre[w:]
			if st.closed || st.localErr != "" || s.frozen {
				s.mu.Unlock()
				return
			}
			if err != nil {
				s.failLocal(st, "local write: "+err.Error())
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			continue
		}
		if st.rx.Len() == 0 {
			if st.peerGone {
				s.removeStream(st, false, st.goneReason)
				s.mu.Unlock()
				return
			}
			// FIN: everything delivered, half-close the local socket.
			s.mu.Unlock()
			var err error
			if cw, ok := st.local.(closeWriter); ok {
				err = cw.CloseWrite()
			}
			s.mu.Lock()
			if !st.closed {
				if err != nil {
					s.failLocal(st, "local close-write: "+err.Error())
				} else {
					st.finDelivered = true
					st.ackDirty = true
					s.kick()
				}
			}
			s.mu.Unlock()
			return
		}
		n := st.rx.CopyAt(buf, 0)
		s.mu.Unlock()

		w, err := st.local.Write(buf[:n])

		s.mu.Lock()
		if st.closed || st.localErr != "" {
			s.mu.Unlock()
			return
		}
		if w > 0 {
			st.rx.Discard(w)
			st.rxDelivered += uint64(w)
			s.stats.BytesToLocal += uint64(w)
			s.noteDelivered(st)
		}
		if s.frozen {
			s.mu.Unlock()
			return
		}
		if err != nil {
			s.failLocal(st, "local write: "+err.Error())
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// noteDelivered schedules an ACK. Caller holds s.mu.
func (s *Session) noteDelivered(st *Stream) {
	st.ackDirty = true
	if st.rxDelivered-st.ackSent >= uint64(s.cfg.ackThreshold()) {
		s.kick()
		return
	}
	if s.ackTimer == nil {
		s.ackTimer = time.AfterFunc(s.cfg.AckDelay, func() {
			s.mu.Lock()
			s.ackTimer = nil
			s.ackFlush = true
			s.kick()
			s.mu.Unlock()
		})
	}
}

// ---- frame handling ----

var errGoAway = errors.New("peer sent GOAWAY")

func (s *Session) protoErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", wire.ErrProtocol, fmt.Sprintf(format, a...))
}

// handleFrame applies one frame received on c. Errors close the connection.
func (s *Session) handleFrame(c *Conn, f wire.Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return errDetached
	}
	if f.Type != wire.TGoAway {
		c.used = true
	}
	switch f.Type {
	case wire.TPing:
		c.ctrl = wire.AppendFrame(c.ctrl, wire.TPong, 0, f.Payload)
		s.kick()
	case wire.TPong:
	case wire.TData:
		off, data, err := wire.ParseData(f.Payload)
		if err != nil {
			return err
		}
		st := s.streams[f.Stream]
		if st == nil {
			return nil // closed locally; the peer has been or will be told
		}
		if off > st.rxNext {
			return s.protoErr("stream %d: data at %d, expected %d", st.id, off, st.rxNext)
		}
		skip := st.rxNext - off
		if skip >= uint64(len(data)) {
			return nil // retransmitted duplicate
		}
		if st.rxFin {
			return s.protoErr("stream %d: data after close", st.id)
		}
		data = data[skip:]
		if st.rx.Len()+len(data) > s.cfg.Window {
			s.resetStream(st, wire.ResetProtocol, "peer exceeded window", CloseProtocol)
			return nil
		}
		st.rx.Append(data)
		st.rxNext += uint64(len(data))
		st.rxCond.Signal()
	case wire.TAck:
		off, flags, err := wire.ParseAck(f.Payload)
		if err != nil {
			return err
		}
		st := s.streams[f.Stream]
		if st == nil {
			return nil
		}
		if off > st.txNext {
			return s.protoErr("stream %d: ack %d beyond sent %d", st.id, off, st.txNext)
		}
		if off > st.txBase {
			st.tx.Discard(int(off - st.txBase))
			st.txBase = off
			st.txCond.Signal()
			s.kick()
		}
		if flags&wire.AckFin != 0 && st.txFin && st.finSent && off == st.txEnd() {
			st.finAcked = true
			s.maybeFinish(st)
		}
	case wire.TClose:
		fin, err := wire.ParseClose(f.Payload)
		if err != nil {
			return err
		}
		st := s.streams[f.Stream]
		if st == nil || st.rxFin {
			return nil
		}
		if fin != st.rxNext {
			return s.protoErr("stream %d: close at %d, received %d", st.id, fin, st.rxNext)
		}
		st.rxFin = true
		st.rxCond.Signal()
	case wire.TReset:
		code, msg, err := wire.ParseReset(f.Payload)
		if err != nil {
			return err
		}
		if st := s.streams[f.Stream]; st != nil {
			s.log.Info("stream reset by peer", "stream", st.id, "code", code, "reason", msg)
			if st.rx.Len() > 0 && st.local != nil && st.localErr == "" {
				st.peerGone = true // deliver what we have first
				st.goneReason = ClosePeerReset
				st.rxCond.Broadcast()
			} else {
				s.removeStream(st, true, ClosePeerReset)
			}
		}
	case wire.TOpen:
		if s.role != RoleRelay {
			return s.protoErr("unexpected OPEN")
		}
		o, err := wire.ParseOpen(f.Payload)
		if err != nil {
			return err
		}
		if f.Stream <= s.maxSeen {
			return s.protoErr("OPEN %d not above %d", f.Stream, s.maxSeen)
		}
		s.maxSeen = f.Stream
		st := s.newStream(f.Stream, o, nil)
		s.log.Debug("stream open", "stream", st.id, "src", o.Src)
		if s.onOpen != nil {
			s.onOpen(st)
		}
	case wire.TGoAway:
		flags, msg, err := wire.ParseGoAway(f.Payload)
		if err != nil {
			return err
		}
		s.log.Info("peer going away", "final", flags&wire.GoAwayFinal != 0, "reason", msg)
		if flags&wire.GoAwayRebind == 0 {
			c.used = true
		}
		if flags&wire.GoAwayFinal != 0 && s.role == RoleGateway {
			s.stats.PeerExited = true
			for _, id := range slices.Clone(s.order) {
				if st := s.streams[id]; st.everOpened {
					s.peerForgot(st)
				}
			}
		}
		return errGoAway
	default:
		return s.protoErr("unexpected frame %v", f.Type)
	}
	return nil
}

// collect builds the next batch of frames for c into buf. final reports that
// a GOAWAY was appended and the writer must stop afterwards.
func (s *Session) collect(c *Conn, buf []byte) (out []byte, final bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return buf, false
	}
	buf = append(buf, c.ctrl...)
	c.ctrl = c.ctrl[:0]

	// OPENs strictly in id order, before any data of those streams.
	if s.role == RoleGateway {
		for _, id := range s.order {
			st := s.streams[id]
			if st.openPending {
				buf = wire.AppendFrame(buf, wire.TOpen, id, st.open.Append(nil))
				st.openPending = false
				st.everOpened = true
			}
		}
	}

	// ACKs.
	flushAll := s.ackFlush
	s.ackFlush = false
	thr := uint64(s.cfg.ackThreshold())
	var finished []*Stream
	for _, id := range s.order {
		st := s.streams[id]
		if !st.ackDirty || st.peerGone || st.localErr != "" {
			continue
		}
		finNew := st.finDelivered && !st.ackFinSent
		if flushAll || finNew || st.rxDelivered-st.ackSent >= thr {
			var flags uint8
			if st.finDelivered {
				flags = wire.AckFin
			}
			buf = wire.AppendFrame(buf, wire.TAck, id, wire.AckPayload(st.rxDelivered, flags))
			st.ackSent = st.rxDelivered
			st.ackFinSent = st.finDelivered
			st.ackDirty = false
			if finNew {
				finished = append(finished, st)
			}
		} else if s.ackTimer == nil {
			s.noteDelivered(st)
		}
	}

	// DATA, round-robin, one chunk per stream per pass; then CLOSE or RESET.
	var reset []*Stream
	budget := 256 << 10
	n := len(s.order)
	for pass := 0; budget > 0 && n > 0; pass++ {
		progress := false
		for i := 0; i < n && budget > 0; i++ {
			st := s.streams[s.order[(s.rr+i)%n]]
			if st.openPending || st.peerGone || st.closed {
				continue
			}
			inflight := st.txNext - st.txBase
			if end := st.txEnd(); st.txNext < end && inflight < s.peerWindow {
				k := min(end-st.txNext, s.peerWindow-inflight, wire.MaxData)
				buf = append(buf, byte(wire.TData))
				buf = be32(buf, st.id)
				buf = be32(buf, uint32(8+k))
				buf = be64(buf, st.txNext)
				buf = st.tx.AppendAt(buf, int(st.txNext-st.txBase), int(k))
				st.txNext += k
				budget -= int(k)
				progress = true
			}
			if st.txNext == st.txEnd() {
				if st.localErr != "" {
					buf = wire.AppendFrame(buf, wire.TReset, st.id, wire.ResetPayload(wire.ResetLocal, st.localErr))
					reset = append(reset, st)
					st.closed = true // no more frames for it in this batch
				} else if st.txFin && !st.finSent {
					buf = wire.AppendFrame(buf, wire.TClose, st.id, wire.ClosePayload(st.txNext))
					st.finSent = true
				}
			}
		}
		if !progress {
			break
		}
	}
	if n > 0 {
		s.rr = (s.rr + 1) % n
	}
	for _, st := range finished {
		s.maybeFinish(st)
	}
	for _, st := range reset {
		st.closed = false
		s.removeStream(st, true, CloseLocalError)
	}

	if c.goaway != nil && !c.goawaySent {
		buf = wire.AppendFrame(buf, wire.TGoAway, 0, c.goaway)
		c.goawaySent = true
		return buf, true
	}
	return buf, false
}

func be32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func be64(b []byte, v uint64) []byte {
	return be32(be32(b, uint32(v>>32)), uint32(v))
}

// ---- attach / detach / reconcile ----

func (s *Session) detach(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == c {
		s.conn = nil
		s.stats.Disconnects++
		s.stats.LastDisconnect = time.Now()
		s.log.Info("tunnel down", "streams", len(s.streams), "err", c.Err())
	}
}

// relayView builds the HELLO stream list. Caller holds s.mu.
func (s *Session) relayView() []wire.StreamState {
	es := make([]wire.StreamState, 0, len(s.order))
	for _, id := range s.order {
		st := s.streams[id]
		var fl uint8
		if st.rxFin {
			fl = wire.EntryFin
		}
		es = append(es, wire.StreamState{ID: id, RxOffset: st.rxNext, Flags: fl})
	}
	return es
}

// gatewayAccept reconciles the session with a relay's HELLO and attaches c.
// It returns the HELLO_ACK to send. Caller must not hold s.mu.
func (s *Session) gatewayAccept(c *Conn, h *wire.Hello) wire.HelloAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	outage := s.outageLocked()
	if old := s.conn; old != nil {
		s.conn = nil
		old.fail(errReplaced)
		s.stats.Disconnects++
	}
	status := uint8(wire.AckResumed)
	if !s.bound || h.Session != s.id {
		status = wire.AckNew
		// The previous connection was answered NEW too, and the relay never
		// used it (no frame but a REBIND GOAWAY): it never saw the OPENs sent
		// there, which are all of the opened streams (that NEW reset the
		// rest). Whether it then renewed its id (Session.renewID) or
		// restarted, offer those streams again instead of resetting them.
		// Decided here, under the lock, so it doesn't matter whether that
		// connection's GOAWAY was read before this connection arrived.
		unused := s.last != nil && s.last.answeredNew && !s.last.used
		reset, reopened := 0, 0
		for _, id := range slices.Clone(s.order) {
			st := s.streams[id]
			if !st.everOpened {
				continue
			}
			if unused && st.txBase == 0 && st.rxNext == 0 && st.localErr == "" && !st.peerGone {
				st.everOpened = false // opened again below, from offset 0
				reopened++
				continue
			}
			s.removeStream(st, true, CloseSessionLost)
			reset++
		}
		if s.bound {
			s.log.Warn("relay presented a new session; old streams reset",
				"old", fmt.Sprintf("%x", s.id), "new", fmt.Sprintf("%x", h.Session), "reset", reset, "reopened", reopened)
		}
		s.id = h.Session
		s.bound = true
	}
	s.peerWindow = uint64(h.Window)
	known := make(map[uint32]wire.StreamState, len(h.Streams))
	for _, e := range h.Streams {
		known[e.ID] = e
	}
	var entries []wire.StreamState
	for _, id := range slices.Clone(s.order) {
		st := s.streams[id]
		st.ackDirty = true
		if status == wire.AckNew || id > h.MaxSeen || !st.everOpened {
			// The relay never processed this stream's OPEN.
			if st.txBase != 0 || st.rxNext != 0 {
				s.removeStream(st, true, CloseProtocol)
				continue
			}
			st.openPending = true
			st.txNext = 0
			st.finSent = false
			continue
		}
		e, ok := known[id]
		if !ok {
			s.peerForgot(st) // relay closed it while we were apart
			continue
		}
		if e.RxOffset < st.txBase || e.RxOffset > st.txEnd() {
			s.log.Warn("stream offsets inconsistent; resetting", "stream", id,
				"peer_rx", e.RxOffset, "tx_base", st.txBase, "tx_end", st.txEnd())
			s.removeStream(st, true, CloseProtocol) // omitted from entries -> relay resets too
			continue
		}
		st.openPending = false
		st.txNext = e.RxOffset
		st.finSent = e.Flags&wire.EntryFin != 0
		var fl uint8
		if st.rxFin {
			fl = wire.EntryFin
		}
		entries = append(entries, wire.StreamState{ID: id, RxOffset: st.rxNext, Flags: fl})
	}
	c.answeredNew = status == wire.AckNew
	s.attachLocked(c, status == wire.AckResumed, outage)
	return wire.HelloAck{Status: status, Window: uint32(s.cfg.Window), Streams: entries}
}

// relayApply reconciles the session with the gateway's HELLO_ACK and attaches c.
func (s *Session) relayApply(c *Conn, a *wire.HelloAck) {
	s.mu.Lock()
	defer s.mu.Unlock()
	outage := s.outageLocked()
	s.peerWindow = uint64(a.Window)
	s.bound = true
	if a.Status == wire.AckNew {
		if len(s.streams) > 0 || s.maxSeen > 0 {
			s.log.Warn("gateway does not know this session; dropping all streams", "streams", len(s.streams))
		}
		for _, id := range slices.Clone(s.order) {
			s.removeStream(s.streams[id], true, CloseSessionLost)
		}
		s.maxSeen = 0
		s.attachLocked(c, false, outage)
		return
	}
	known := make(map[uint32]wire.StreamState, len(a.Streams))
	for _, e := range a.Streams {
		known[e.ID] = e
		if s.streams[e.ID] == nil {
			// Closed here between HELLO and HELLO_ACK: tell the gateway.
			c.ctrl = wire.AppendFrame(c.ctrl, wire.TReset, e.ID, wire.ResetPayload(wire.ResetLocal, "closed during reconnect"))
		}
	}
	for _, id := range slices.Clone(s.order) {
		st := s.streams[id]
		e, ok := known[id]
		if !ok {
			s.removeStream(st, true, ClosePeerFin) // gateway no longer has the player
			continue
		}
		if e.RxOffset < st.txBase || e.RxOffset > st.txEnd() {
			s.log.Warn("stream offsets inconsistent; resetting", "stream", id,
				"peer_rx", e.RxOffset, "tx_base", st.txBase, "tx_end", st.txEnd())
			c.ctrl = wire.AppendFrame(c.ctrl, wire.TReset, id, wire.ResetPayload(wire.ResetProtocol, "offset mismatch"))
			s.removeStream(st, true, CloseProtocol)
			continue
		}
		st.txNext = e.RxOffset
		st.finSent = e.Flags&wire.EntryFin != 0
		st.ackDirty = true
	}
	s.attachLocked(c, true, outage)
}

// renewID is the relay's answer to a gateway that does not know its session
// (HELLO_ACK NEW) although some gateway accepted the id before: that one may
// still hold the id with streams of its own (a gateway that lost the VIP
// while stalled, say), and a later reconnect there must not resume them. So
// the id is never presented again: all streams are dropped (the gateway does
// not have them), and the session gets a new id. It reports false, changing
// nothing, if no gateway has accepted the current id yet.
func (s *Session) renewID() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != RoleRelay || !s.bound {
		return false
	}
	old := s.id
	crand.Read(s.id[:])
	s.bound = false
	s.log.Warn("gateway does not know this session; dropping all streams and reconnecting with a new session id",
		"streams", len(s.streams), "old", fmt.Sprintf("%x", old), "new", fmt.Sprintf("%x", s.id))
	for _, id := range slices.Clone(s.order) {
		s.removeStream(s.streams[id], true, CloseSessionLost)
	}
	s.maxSeen = 0
	return true
}

// outageLocked measures the outage a new connection ends (see
// Observer.TunnelAttached). Caller holds s.mu.
func (s *Session) outageLocked() time.Duration {
	if old := s.conn; old != nil {
		return time.Since(time.Unix(0, old.lastRecv.Load()))
	}
	if !s.stats.LastDisconnect.IsZero() {
		return time.Since(s.stats.LastDisconnect)
	}
	return 0
}

func (s *Session) attachLocked(c *Conn, resumed bool, outage time.Duration) {
	s.conn = c
	s.last = c
	s.stats.PeerExited = false
	s.ackFlush = true
	s.stats.Connects++
	if resumed {
		s.stats.Resumes++
		s.stats.LastOutage = outage
	} else {
		s.stats.NewSessions++
	}
	if o := s.cfg.Observer; o != nil {
		o.TunnelAttached(resumed, outage)
	}
	s.stats.LastConnect = time.Now()
	s.kick()
	s.log.Info("tunnel up", "session", fmt.Sprintf("%x", s.id), "streams", len(s.streams),
		"resumed", resumed, "outage", outage.Round(time.Millisecond))
}

// CloseAll aborts every stream (used on shutdown).
func (s *Session) CloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range slices.Clone(s.order) {
		s.removeStream(s.streams[id], false, CloseShutdown)
	}
}

// Drained reports whether every stream has sent its FIN (or there are none);
// used by the relay before a final GOAWAY.
func (s *Session) Drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.streams {
		if !(st.txFin && st.finSent && st.txNext == st.txEnd()) {
			return false
		}
	}
	return true
}

// StreamInfo is a per-stream snapshot for status output.
type StreamInfo struct {
	ID         uint32 `json:"id"`
	Src        string `json:"src"`
	TxBuffered int    `json:"tx_buffered"`
	RxQueued   int    `json:"rx_queued"`
	TxOffset   uint64 `json:"tx_offset"`
	RxOffset   uint64 `json:"rx_offset"`
}

// StreamInfos returns a snapshot of all streams.
func (s *Session) StreamInfos() []StreamInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]StreamInfo, 0, len(s.order))
	for _, id := range s.order {
		st := s.streams[id]
		out = append(out, StreamInfo{ID: id, Src: st.open.Src.String(), TxBuffered: st.tx.Len(),
			RxQueued: st.rx.Len(), TxOffset: st.txEnd(), RxOffset: st.rxNext})
	}
	return out
}
