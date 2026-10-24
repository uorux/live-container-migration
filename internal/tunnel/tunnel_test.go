package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"lcm/internal/wire"
)

func TestByteQueue(t *testing.T) {
	var q byteQueue
	var ref []byte
	rng := rand.New(rand.NewPCG(1, 2))
	next := byte(0)
	for i := 0; i < 20000; i++ {
		if rng.IntN(2) == 0 {
			n := rng.IntN(5000)
			p := make([]byte, n)
			for j := range p {
				p[j] = next
				next++
			}
			q.Append(p)
			ref = append(ref, p...)
		} else if len(ref) > 0 {
			n := rng.IntN(len(ref) + 1)
			q.Discard(n)
			ref = ref[n:]
		}
		if q.Len() != len(ref) {
			t.Fatalf("len %d want %d", q.Len(), len(ref))
		}
		if len(ref) > 0 {
			pos := rng.IntN(len(ref))
			n := min(len(ref)-pos, 100)
			got := q.AppendAt(nil, pos, n)
			if !bytes.Equal(got, ref[pos:pos+n]) {
				t.Fatalf("AppendAt mismatch at iteration %d", i)
			}
			buf := make([]byte, 64)
			k := q.CopyAt(buf, pos)
			if !bytes.Equal(buf[:k], ref[pos:pos+k]) {
				t.Fatalf("CopyAt mismatch at iteration %d", i)
			}
		}
	}
	if cap(q.buf) > 4*max(len(ref), 1<<16) {
		t.Fatalf("queue did not compact: cap %d for len %d", cap(q.buf), len(ref))
	}
}

func testCfg() Config {
	return Config{Token: []byte("t"), Window: 64 << 10, PingInterval: 50 * time.Millisecond,
		DeadTimeout: time.Second, AckDelay: time.Millisecond, HandshakeTimeout: time.Second}
}

// fakeConn returns a Conn on one end of a pipe (never Run).
func fakeConn(s *Session) *Conn {
	a, _ := net.Pipe()
	return newConn(s, a, wire.NewReader(a))
}

func addFake(s *Session, id uint32, tx []byte, txBase uint64, rxNext uint64) *Stream {
	st := s.newStream(id, wire.Open{}, nil)
	st.tx.Append(tx)
	st.txBase = txBase
	st.txNext = txBase + uint64(len(tx))
	st.rxNext = rxNext
	st.rxDelivered = rxNext
	return st
}

func TestGatewayReconcile(t *testing.T) {
	sid := [16]byte{7}
	s := NewSession(RoleGateway, testCfg(), [16]byte{}, nil)
	s.bound, s.id = true, sid
	// 1: relay knows it; relay received 100 of our 150 bytes (we have 50..150 unacked).
	a := addFake(s, 1, make([]byte, 100), 50, 40)
	a.everOpened = true
	// 2: opened, relay processed the OPEN but has forgotten it (closed meanwhile).
	b := addFake(s, 2, nil, 0, 0)
	b.everOpened = true
	// 3: like 2 (relay saw OPEN 3, no longer has it), with unacked data.
	c := addFake(s, 3, []byte("abc"), 0, 0)
	c.everOpened = true
	// 4: accepted during the outage, never opened.
	d := addFake(s, 4, []byte("xyz"), 0, 0)
	d.openPending = true
	// 5: relay claims an offset we cannot serve (beyond what we have).
	e := addFake(s, 5, []byte("12345"), 0, 0)
	e.everOpened = true

	// MaxSeen=5: the relay processed OPENs 1..5; it still has 1 and 5 (and a
	// stream 9 we never opened, which must simply be ignored).
	h := wire.Hello{Session: sid, Window: 32 << 10, MaxSeen: 5,
		Streams: []wire.StreamState{{ID: 1, RxOffset: 100}, {ID: 5, RxOffset: 50}, {ID: 9, RxOffset: 1}}}
	ack := s.gatewayAccept(fakeConn(s), &h)
	if ack.Status != wire.AckResumed {
		t.Fatal("status", ack.Status)
	}
	if len(ack.Streams) != 1 || ack.Streams[0] != (wire.StreamState{ID: 1, RxOffset: 40}) {
		t.Fatalf("ack streams %+v", ack.Streams)
	}
	if a.txNext != 100 || a.txBase != 50 || a.closed {
		t.Fatalf("stream 1: next %d base %d closed %v", a.txNext, a.txBase, a.closed)
	}
	if !b.closed || !c.closed {
		t.Fatal("streams the relay forgot must be closed")
	}
	if !e.closed {
		t.Fatal("stream with impossible offset must be reset")
	}
	if d.closed || !d.openPending || d.txNext != 0 {
		t.Fatal("never-opened stream must be (re)opened", d.closed, d.openPending)
	}
	if s.peerWindow != 32<<10 {
		t.Fatal("peer window", s.peerWindow)
	}

	// Second reconnect: the relay never processed OPEN 4 (MaxSeen 3) -> reopen from 0.
	d.openPending = false
	d.everOpened = true
	d.txNext = 3
	ack = s.gatewayAccept(fakeConn(s), &wire.Hello{Session: sid, Window: 1 << 20, MaxSeen: 3,
		Streams: []wire.StreamState{{ID: 1, RxOffset: 150, Flags: 0}}})
	if !d.openPending || d.txNext != 0 || d.closed {
		t.Fatal("stream 4 must be reopened from offset 0")
	}
	if a.txNext != 150 {
		t.Fatal("stream 1 next", a.txNext)
	}
}

func TestGatewayNewSession(t *testing.T) {
	s := NewSession(RoleGateway, testCfg(), [16]byte{}, nil)
	// Before any relay: players queue up.
	p1 := addFake(s, 1, []byte("early"), 0, 0)
	p1.openPending = true
	c := fakeConn(s)
	ack := s.gatewayAccept(c, &wire.Hello{Session: [16]byte{1}, Window: 1 << 20})
	if ack.Status != wire.AckNew || p1.closed || !p1.openPending {
		t.Fatal("first relay: status", ack.Status, p1.closed, p1.openPending)
	}
	// Simulate that OPEN 1 went out and the relay ran the connection (it
	// sent something); player 2 arrives during an outage.
	p1.openPending, p1.everOpened = false, true
	s.handleFrame(c, wire.Frame{Type: wire.TPing, Payload: wire.PingPayload(1)})
	p2 := addFake(s, 2, nil, 0, 0)
	p2.openPending = true
	// Relay restarted with a new session id.
	ack = s.gatewayAccept(fakeConn(s), &wire.Hello{Session: [16]byte{2}, Window: 1 << 20, MaxSeen: 0})
	if ack.Status != wire.AckNew {
		t.Fatal("status", ack.Status)
	}
	if !p1.closed {
		t.Fatal("stream opened on the old session must be reset")
	}
	if p2.closed || !p2.openPending {
		t.Fatal("never-opened stream must carry over to the new session")
	}
	if s.ID() != [16]byte{2} {
		t.Fatal("session id not adopted")
	}
}

// TestGatewayRebind: a relay that presents an unknown id after a connection
// answered NEW that it never used (no frame but a REBIND GOAWAY) never saw the
// OPENs sent there: those streams are opened again on the new session rather
// than reset, however the old connection's GOAWAY and the new HELLO are
// ordered, and only once. A used connection, or one that resumed, keeps the
// old rule: opened streams are reset.
func TestGatewayRebind(t *testing.T) {
	rebind := wire.Frame{Type: wire.TGoAway, Payload: wire.GoAwayPayload(wire.GoAwayRebind, "new session id")}
	ping := wire.Frame{Type: wire.TPing, Payload: wire.PingPayload(1)}
	for _, tc := range []struct {
		name    string
		resumed bool // the old connection resumed rather than answered NEW
		ping    bool // the relay sent a PING on it (it ran the connection)
		goaway  int  // REBIND GOAWAY: 0 none, 1 read before the new HELLO, 2 after it
		detach  bool // the old connection was detached before the new HELLO
		reopen  bool
	}{
		{name: "GOAWAY first", goaway: 1, detach: true, reopen: true},
		{name: "new HELLO before the GOAWAY is read", goaway: 2, reopen: true},
		{name: "no GOAWAY (relay restarted)", detach: true, reopen: true},
		{name: "relay used the connection", ping: true, goaway: 1, detach: true},
		{name: "old connection resumed", resumed: true, goaway: 1, detach: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSession(RoleGateway, testCfg(), [16]byte{}, nil)
			if tc.resumed {
				s.gatewayAccept(fakeConn(s), &wire.Hello{Session: [16]byte{1}, Window: 1 << 20})
			}
			p := addFake(s, 1, []byte("login"), 0, 0)
			p.openPending = true
			c := fakeConn(s)
			s.gatewayAccept(c, &wire.Hello{Session: [16]byte{1}, Window: 1 << 20})
			s.mu.Lock()
			p.openPending, p.everOpened, p.txNext = false, true, 5 // OPEN 1 and its data went out on c
			s.mu.Unlock()
			if tc.ping {
				s.handleFrame(c, ping)
			}
			if tc.goaway == 1 {
				if err := s.handleFrame(c, rebind); err != errGoAway {
					t.Fatal("GOAWAY must end the connection:", err)
				}
			}
			if tc.detach {
				s.detach(c)
			}
			s.gatewayAccept(fakeConn(s), &wire.Hello{Session: [16]byte{2}, Window: 1 << 20})
			if tc.goaway == 2 {
				if err := s.handleFrame(c, rebind); err != errDetached {
					t.Fatal("late GOAWAY on a replaced connection:", err)
				}
			}
			if tc.reopen {
				if p.closed || p.everOpened || !p.openPending || p.txNext != 0 {
					t.Fatalf("want reopened from 0: closed %v everOpened %v openPending %v txNext %d",
						p.closed, p.everOpened, p.openPending, p.txNext)
				}
				if s.NumStreams() != 1 {
					t.Fatalf("%d streams, want 1", s.NumStreams())
				}
			} else if !p.closed {
				t.Fatal("stream opened on the old session must be reset")
			}
		})
	}
}

func TestRelayReconcile(t *testing.T) {
	s := NewSession(RoleRelay, testCfg(), [16]byte{3}, nil)
	s.maxSeen = 9
	a := addFake(s, 1, make([]byte, 10), 20, 500) // tx [20,30)
	b := addFake(s, 2, nil, 0, 0)
	c := addFake(s, 3, make([]byte, 10), 0, 0)
	c.rxFin = true
	view := s.relayView()
	if len(view) != 3 || view[0].RxOffset != 500 || view[2].Flags != wire.EntryFin {
		t.Fatalf("view %+v", view)
	}
	conn := fakeConn(s)
	s.relayApply(conn, &wire.HelloAck{Status: wire.AckResumed, Window: 8 << 10,
		Streams: []wire.StreamState{{ID: 1, RxOffset: 25}, {ID: 3, RxOffset: 99}, {ID: 7, RxOffset: 0}}})
	if a.closed || a.txNext != 25 || a.txBase != 20 {
		t.Fatalf("stream 1: closed %v next %d base %d", a.closed, a.txNext, a.txBase)
	}
	if !b.closed {
		t.Fatal("stream unknown to gateway must be closed")
	}
	if !c.closed {
		t.Fatal("stream with impossible offset must be reset")
	}
	// RESETs queued: one for stream 7 (gateway knows, we don't), one for stream 3.
	r := wire.NewReader(bytes.NewReader(conn.ctrl))
	var resets []uint32
	for {
		f, err := r.ReadFrame()
		if err != nil {
			break
		}
		if f.Type == wire.TReset {
			resets = append(resets, f.Stream)
		}
	}
	if len(resets) != 2 {
		t.Fatalf("resets %v", resets)
	}
	if s.peerWindow != 8<<10 {
		t.Fatal("window")
	}

	// Gateway lost the session: drop everything, maxSeen back to 0.
	s.relayApply(fakeConn(s), &wire.HelloAck{Status: wire.AckNew, Window: 1 << 20})
	if !a.closed || s.maxSeen != 0 || len(s.streams) != 0 {
		t.Fatal("AckNew must drop all streams")
	}
}

// tcpPair returns two connected loopback TCP conns.
func tcpPair(t testing.TB) (*net.TCPConn, *net.TCPConn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ch := make(chan net.Conn)
	go func() { c, _ := ln.Accept(); ch <- c }()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return a.(*net.TCPConn), (<-ch).(*net.TCPConn)
}

// sessionPair connects a gateway and a relay session over a TCP tunnel; the
// relay's upstream for each stream is handed to upstream().
type sessionPair struct {
	gw, rl    *Session
	gwc, rlc  *Conn
	upstreams chan net.Conn
	wg        sync.WaitGroup
	cfg       Config
	relayCfg  Config
	t         testing.TB
}

func newSessionPair(t testing.TB, cfg Config) *sessionPair {
	p := &sessionPair{t: t, cfg: cfg, upstreams: make(chan net.Conn, 64)}
	p.gw = NewSession(RoleGateway, cfg, [16]byte{}, nil)
	p.rl = NewSession(RoleRelay, cfg, [16]byte{0xaa}, func(st *Stream) {
		go func() {
			mine, theirs := tcpPair(t)
			st.AttachLocal(mine)
			p.upstreams <- theirs
		}()
	})
	p.connect()
	return p
}

func (p *sessionPair) connect() {
	a, b := tcpPair(p.t)
	errc := make(chan error, 1)
	go func() {
		c, err := ServerHandshake(b, p.cfg.Token, time.Second, func(string) *Session { return p.gw })
		p.gwc = c
		errc <- err
	}()
	c, err := p.rl.ClientHandshake(a, "default")
	if err != nil {
		p.t.Fatal(err)
	}
	if err := <-errc; err != nil {
		p.t.Fatal(err)
	}
	p.rlc = c
	p.wg.Add(2)
	go func() { defer p.wg.Done(); p.gwc.Run() }()
	go func() { defer p.wg.Done(); p.rlc.Run() }()
}

func (p *sessionPair) drop() {
	p.rlc.Close()
	p.gwc.Close()
	p.wg.Wait()
}

// TestFlowControlBounded: the server side never reads; the gateway must stop
// reading from the player once Window bytes are buffered, and the relay must
// never queue more than Window bytes.
func TestFlowControlBounded(t *testing.T) {
	cfg := testCfg()
	p := newSessionPair(t, cfg)
	defer p.drop()
	player, gwLocal := tcpPair(t)
	defer player.Close()
	p.gw.AddStream(gwLocal, wire.Open{})
	up := <-p.upstreams // the "server", which does not read for now
	defer up.Close()

	// Push far more than window + socket buffers can hold.
	total := 64 << 20
	written := make(chan int, 1)
	go func() {
		buf := make([]byte, 64<<10)
		n := 0
		player.SetWriteDeadline(time.Now().Add(1500 * time.Millisecond))
		for n < total {
			k, err := player.Write(buf)
			n += k
			if err != nil {
				break
			}
		}
		written <- n
	}()
	n := <-written
	if n >= total {
		t.Fatal("writer was never blocked: no backpressure")
	}
	gs, rs := p.gw.Stats(), p.rl.StreamInfos()
	if gs.MaxTxBuffered > cfg.Window {
		t.Fatalf("gateway buffered %d > window %d", gs.MaxTxBuffered, cfg.Window)
	}
	for _, si := range rs {
		if si.RxQueued > cfg.Window {
			t.Fatalf("relay queued %d > window", si.RxQueued)
		}
	}
	// Now drain at the server: every byte the player wrote must arrive.
	up.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.CopyN(io.Discard, up, int64(n))
	if err != nil || got != int64(n) {
		t.Fatalf("server got %d/%d: %v", got, n, err)
	}
}

// TestResumeRandomDrops runs bidirectional traffic through the session pair
// and drops the tunnel at random moments; the byte streams must be exact.
func TestResumeRandomDrops(t *testing.T) {
	cfg := testCfg()
	p := newSessionPair(t, cfg)
	player, gwLocal := tcpPair(t)
	p.gw.AddStream(gwLocal, wire.Open{})
	up := <-p.upstreams

	const n = 8 << 20
	src := make([]byte, n)
	for i := range src {
		src[i] = byte(i*7 + i>>9)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	// player -> server, and server echoes back.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for off := 0; off < n; off += 32 << 10 {
			player.Write(src[off : off+32<<10])
			time.Sleep(time.Millisecond)
		}
		player.CloseWrite()
	}()
	go func() { defer wg.Done(); io.Copy(up, up); up.(*net.TCPConn).CloseWrite() }()
	gotc := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(player); gotc <- b }()

	rng := rand.New(rand.NewPCG(5, 6))
	drops := 0
	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(10+rng.IntN(50)) * time.Millisecond)
		if p.gw.NumStreams() == 1 {
			drops++ // dropped while the stream was live
		}
		p.drop()
		p.connect()
	}
	if drops < 5 {
		t.Fatalf("only %d drops happened mid-stream; test is not exercising resume", drops)
	}
	select {
	case got := <-gotc:
		if !bytes.Equal(got, src) {
			t.Fatalf("echo mismatch: got %d bytes", len(got))
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
	wg.Wait()
	// Stream torn down on both sides after the close handshake.
	deadline := time.Now().Add(5 * time.Second)
	for p.gw.NumStreams()+p.rl.NumStreams() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.gw.NumStreams()+p.rl.NumStreams() > 0 {
		t.Fatalf("streams left: gw %d relay %d", p.gw.NumStreams(), p.rl.NumStreams())
	}
	p.drop()
}

// TestRelayNewIDAfterUnknownSession: a relay whose session a gateway accepted
// before and that reaches a gateway which doesn't know it (one that restarted,
// or took the VIP over without a handoff) never presents that id again, so a
// gateway still holding it (the one it left, stalled) can't resume stale
// streams. A player held at the new gateway, whose OPEN went out on the
// connection the relay hung up, is opened again on the new session.
func TestRelayNewIDAfterUnknownSession(t *testing.T) {
	cfg := testCfg()
	p := newSessionPair(t, cfg)
	playerA, gwLocalA := tcpPair(t)
	defer playerA.Close()
	p.gw.AddStream(gwLocalA, wire.Open{})
	upA := <-p.upstreams
	defer upA.Close()
	// The relay uses the connection: data goes through and is acknowledged.
	playerA.Write([]byte("hi"))
	upA.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(upA, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.gw.Stats().TxBuffered != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.drop()
	oldID := p.rl.ID()

	// handshake runs one connection to gw, the gateway side served until it ends.
	handshake := func(gw *Session) error {
		a, b := tcpPair(t)
		go func() {
			if c, err := ServerHandshake(b, cfg.Token, time.Second, func(string) *Session { return gw }); err == nil {
				c.Run()
			}
		}()
		c, err := p.rl.ClientHandshake(a, "default")
		if err == nil {
			c.Close()
		}
		return err
	}

	gwA := p.gw
	p.gw = NewSession(RoleGateway, cfg, [16]byte{}, nil)
	playerB, gwLocalB := tcpPair(t)
	defer playerB.Close()
	p.gw.AddStream(gwLocalB, wire.Open{})
	playerB.Write([]byte("login"))
	if err := handshake(p.gw); !errors.Is(err, ErrNewSession) {
		t.Fatalf("gateway B didn't know the session: got %v, want ErrNewSession", err)
	}
	if p.rl.ID() == oldID || p.rl.NumStreams() != 0 {
		t.Fatalf("relay kept its id (%v) or its streams (%d)", p.rl.ID() == oldID, p.rl.NumStreams())
	}
	p.connect()
	var up net.Conn
	select {
	case up = <-p.upstreams:
	case <-time.After(5 * time.Second):
		t.Fatal("the held player was not opened on the new session")
	}
	defer up.Close()
	got := make([]byte, 5)
	up.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(up, got); err != nil || string(got) != "login" {
		t.Fatalf("server got %q, %v", got, err)
	}
	if st := p.gw.Stats(); st.NewSessions != 2 || st.Streams != 1 {
		t.Fatalf("gateway B: %d new sessions, %d streams; want 2, 1", st.NewSessions, st.Streams)
	}
	p.drop()

	// Back at A, which still holds the old id and a stream under it: nothing is resumed.
	if err := handshake(gwA); !errors.Is(err, ErrNewSession) {
		t.Fatalf("gateway A: got %v, want ErrNewSession", err)
	}
	if st := gwA.Stats(); st.Resumes != 0 || st.Streams != 0 {
		t.Fatalf("gateway A: %d resumes, %d streams left; want 0, 0", st.Resumes, st.Streams)
	}
	playerA.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.Copy(io.Discard, playerA); err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("stale player at A: %v", err)
	}
}
