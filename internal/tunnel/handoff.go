package tunnel

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"lcm/internal/wire"
)

// Handing a gateway session to another gateway: Freeze stops it and returns
// its state plus the player sockets, which the caller moves (tcprepair) to
// the gateway taking over; Import re-creates the session there. The relay
// then reconnects to the new gateway and resumes as after any tunnel drop:
// the offsets carried in the state make the byte streams continue exactly.

// Handoffer is implemented by local sockets that wrap a TCP connection
// (e.g. the gateway's Minecraft wrappers). HandoffConn returns the TCP
// connection if it can be moved as-is now: no bytes buffered in the wrapper,
// no gateway-side protocol state left.
type Handoffer interface {
	HandoffConn() (*net.TCPConn, bool)
}

// handoffConn returns the TCP connection under a stream's local socket, if
// it can be handed off.
func handoffConn(c net.Conn) (*net.TCPConn, bool) {
	switch x := c.(type) {
	case *net.TCPConn:
		return x, true
	case Handoffer:
		return x.HandoffConn()
	}
	return nil, false
}

// StreamState is one stream's share of a SessionState.
type StreamState struct {
	ID         uint32
	Open       wire.Open
	EverOpened bool
	// Tx holds the bytes read from the player and not yet acknowledged by
	// the relay; TxBase is the offset of Tx[0].
	Tx     []byte
	TxBase uint64
	// Rx holds the bytes from the relay not yet written to the player; RxNext
	// is the offset just past them. AckSent is what was last acknowledged.
	Rx      []byte
	RxNext  uint64
	AckSent uint64
	// Pre is written to the player before Rx (see Stream.pre).
	Pre []byte
}

// SessionState is a frozen gateway session.
type SessionState struct {
	ID         [wire.SessionIDLen]byte
	Bound      bool
	NextID     uint32
	PeerWindow uint64
	Streams    []StreamState
}

// Frozen is what Freeze returns: the session's state and, for each stream in
// it, the player's TCP connection, untouched since the freeze (no pump reads
// or writes it any more; its deadlines are in the past).
type Frozen struct {
	State  SessionState
	Locals map[uint32]*net.TCPConn
	// Dropped counts streams that could not be handed off (half-closed,
	// failed, or with gateway-side state such as a login still on hold);
	// they were reset.
	Dropped int
}

var errFrozen = errors.New("session frozen for handoff")

func (s *Session) isFrozen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frozen
}

// Freeze stops a gateway session for a handoff: the tunnel connection is
// closed (the relay will reconnect, to the new gateway), every local pump
// stops, and the streams' state is returned. Afterwards the session is empty
// and must not be used again. timeout bounds the wait for the pumps.
func (s *Session) Freeze(timeout time.Duration) (*Frozen, error) {
	s.mu.Lock()
	if s.role != RoleGateway {
		s.mu.Unlock()
		return nil, errors.New("only a gateway session can be frozen")
	}
	if s.frozen {
		s.mu.Unlock()
		return nil, errors.New("session already frozen")
	}
	s.frozen = true
	c := s.conn
	s.conn = nil
	if s.ackTimer != nil {
		s.ackTimer.Stop()
		s.ackTimer = nil
	}
	var locals []net.Conn
	for _, id := range s.order {
		st := s.streams[id]
		st.txCond.Broadcast()
		st.rxCond.Broadcast()
		if st.local != nil {
			locals = append(locals, st.local)
		}
	}
	s.mu.Unlock()
	if c != nil {
		c.fail(errFrozen)
	}
	// Wake pumps blocked in Read or Write. A Write may still have put part
	// of its buffer into the socket; the pump accounts for exactly that.
	past := time.Unix(1, 0)
	for _, l := range locals {
		l.SetDeadline(past)
	}
	done := make(chan struct{})
	go func() { s.pumps.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		return nil, fmt.Errorf("local pumps did not stop within %v", timeout)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	f := &Frozen{Locals: make(map[uint32]*net.TCPConn)}
	f.State = SessionState{ID: s.id, Bound: s.bound, NextID: s.nextID, PeerWindow: s.peerWindow}
	for _, id := range slices.Clone(s.order) {
		st := s.streams[id]
		tc, ok := handoffConn(st.local)
		if st.closed || st.txFin || st.rxFin || st.localErr != "" || st.peerGone || !ok {
			s.removeStream(st, true, CloseShutdown)
			f.Dropped++
			continue
		}
		f.State.Streams = append(f.State.Streams, StreamState{
			ID: id, Open: st.open, EverOpened: st.everOpened,
			Tx: st.tx.AppendAt(nil, 0, st.tx.Len()), TxBase: st.txBase,
			Rx: st.rx.AppendAt(nil, 0, st.rx.Len()), RxNext: st.rxNext, AckSent: st.ackSent,
			Pre: slices.Clone(st.pre),
		})
		f.Locals[id] = tc
		st.local = nil // handed over: removeStream must not close it
		s.removeStream(st, false, CloseHandedOff)
	}
	return f, nil
}

// Import re-creates a frozen gateway session with each stream's player
// socket from locals (moved there by the caller), and starts its pumps.
// pending[id], if any, is written to that player before the stream's own
// queued bytes: the moved socket's never-transmitted tail (tcprepair). A
// stream without a socket in locals is left out; the relay will forget it.
func Import(cfg Config, st SessionState, locals map[uint32]net.Conn, pending map[uint32][]byte) *Session {
	s := NewSession(RoleGateway, cfg, st.ID, nil)
	s.mu.Lock()
	s.bound = st.Bound
	s.nextID = st.NextID
	s.peerWindow = st.PeerWindow
	var started []*Stream
	for _, e := range st.Streams {
		local := locals[e.ID]
		if local == nil {
			continue
		}
		local.SetDeadline(time.Time{}) // Freeze expired the old socket's
		x := s.newStream(e.ID, e.Open, local)
		x.everOpened = e.EverOpened
		x.openPending = !e.EverOpened
		x.tx.Append(e.Tx)
		x.txBase = e.TxBase
		x.txNext = e.TxBase // reconciled with the relay's offsets on resume
		x.rx.Append(e.Rx)
		x.rxNext = e.RxNext
		x.rxDelivered = e.RxNext - uint64(len(e.Rx))
		x.ackSent = e.AckSent
		x.ackDirty = true
		x.pre = append(append([]byte(nil), pending[e.ID]...), e.Pre...)
		started = append(started, x)
	}
	s.mu.Unlock()
	for _, x := range started {
		x.startPumps()
	}
	return s
}
