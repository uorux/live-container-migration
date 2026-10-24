package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"lcm/internal/relay"
	"lcm/internal/tcprepair"
)

// canRepair reports whether TCP_REPAIR works here (CAP_NET_ADMIN in this
// network namespace: run the test binary under `unshare -rn`).
func canRepair() bool {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	return unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 1) == nil
}

// switcher forwards TCP connections to a target that can be changed, like the
// Service that points relays at whichever gateway holds the VIP.
type switcher struct {
	ln     net.Listener
	target atomic.Value // string
	mu     sync.Mutex
	conns  []net.Conn
}

func newSwitcher(t *testing.T, target string) *switcher {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &switcher{ln: ln}
	s.target.Store(target)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", s.target.Load().(string))
			if err != nil {
				c.Close()
				continue
			}
			s.mu.Lock()
			s.conns = append(s.conns, c, u)
			s.mu.Unlock()
			go func() { io.Copy(u, c); u.Close() }()
			go func() { io.Copy(c, u); c.Close() }()
		}
	}()
	t.Cleanup(func() { ln.Close(); s.switchTo("") })
	return s
}

// switchTo points new connections at target and cuts the current ones.
func (s *switcher) switchTo(target string) {
	if target != "" {
		s.target.Store(target)
	}
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
}

func echoServer(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String()
}

type gwUnderTest struct {
	gw                 *Gateway
	player, tunnelAddr string
}

func startGW(t *testing.T) *gwUnderTest {
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger()})
	pl, err := Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tl, err := Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go gw.ServePlayers(pl, gw.DefaultBackend())
	go gw.ServeTunnel(tl)
	t.Cleanup(func() { pl.Close(); tl.Close(); gw.DefaultBackend().Session().CloseAll() })
	return &gwUnderTest{gw: gw, player: pl.Addr().String(), tunnelAddr: tl.Addr().String()}
}

// TestGatewayHandoff: a player connected through gateway A keeps its
// connection when A hands everything to gateway B: the player socket moves
// (tcprepair, same namespace here), the relay reconnects to B and resumes.
func TestGatewayHandoff(t *testing.T) {
	if !canRepair() {
		t.Skip("TCP_REPAIR not permitted: run under unshare -rn")
	}
	a, b := startGW(t), startGW(t)
	svc := newSwitcher(t, a.tunnelAddr)
	r := relay.New(relay.Config{Gateway: svc.ln.Addr().String(), Upstream: echoServer(t),
		Tunnel: testTunnelCfg(), BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	waitUntil(t, "relay attached to A", func() bool { return a.gw.DefaultBackend().Session().Connected() })
	player, err := net.Dial("tcp", a.player)
	if err != nil {
		t.Fatal(err)
	}
	defer player.Close()
	echo := func(msg string) {
		t.Helper()
		if _, err := player.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(msg))
		player.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(player, got); err != nil || !bytes.Equal(got, []byte(msg)) {
			t.Fatalf("echo %q: got %q, %v", msg, got, err)
		}
	}
	echo("before the handoff")

	h, err := a.gw.Freeze(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h.Players() != 1 {
		t.Fatalf("handoff carries %d players, want 1", h.Players())
	}
	svc.switchTo(b.tunnelAddr)
	n, err := b.gw.Import(h, func(st *tcprepair.State) (net.Conn, []byte, error) {
		fd, pending, err := tcprepair.Restore(st)
		if err != nil {
			return nil, nil, err
		}
		c, err := tcprepair.Conn(fd, "player")
		return c, pending, err
	})
	if err != nil || n != 1 {
		t.Fatalf("import: %d players, %v", n, err)
	}
	waitUntil(t, "relay resumed on B", func() bool {
		s := b.gw.DefaultBackend().Session().Stats()
		return s.Connected && s.Resumes > 0
	})
	echo("after the handoff")
	big := bytes.Repeat([]byte("0123456789"), 100000)
	echo(string(big))
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A gateway that stops serving without a handoff drops its relays, which
// reconnect, and forgets their sessions and players: the relay comes back
// with a new session (nothing from before the drop is resumed), and the
// players connected then are disconnected.
func TestDropTunnels(t *testing.T) {
	e := newMCEnv(t, nil)
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Backend: "default", Upstream: echoServer(t), Tunnel: testTunnelCfg(),
		BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	e.waitFor("relay connected", e.session().Connected)
	p := e.dial()
	p.c.Write([]byte("before"))
	p.expect([]byte("before"))
	oldID := r.SessionID()

	if n := e.gw.DropTunnels(); n != 1 {
		t.Fatalf("dropped %d relay connections, want 1", n)
	}
	p.expectEOF()
	e.waitFor("relay reconnected", e.session().Connected)
	id := e.session().ID()
	if r.SessionID() == oldID || r.SessionID() != hex.EncodeToString(id[:]) {
		t.Fatalf("relay session %s (was %s), gateway has %x: want a new id on both", r.SessionID(), oldID, id)
	}
	if n := scrape(t, e.gw)[`lcm_gateway_tunnel_connects_total{backend="default",kind="resume"}`]; n != 0 {
		t.Fatalf("%v resumes after forgetting the sessions", n)
	}
	p2 := e.dial()
	p2.c.Write([]byte("after"))
	p2.expect([]byte("after"))
}

// Logins held when the gateway forgets its sessions are disconnected, not
// left waiting for a relay that will never come back here.
func TestForgetDropsHeldPlayers(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval})
	p := e.login(774, "Steve")
	p.keepalives(1, testInterval)
	if n := e.gw.Forget(); n != 1 {
		t.Fatalf("Forget closed %d players, want 1", n)
	}
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.Copy(io.Discard, p.br); err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("held player not disconnected: %v", err)
	}
	e.waitFor("hold over", func() bool { return e.gw.Status(false).Backends[0].Held == 0 })
	if n := e.session().NumStreams(); n != 0 {
		t.Fatalf("%d streams in the new session", n)
	}
}

// While the gate says no, neither players nor relays are served.
func TestGate(t *testing.T) {
	e := newMCEnv(t, nil)
	var open atomic.Bool
	e.gw.SetGate(open.Load)
	srv := echoServer(t)
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Backend: "default", Upstream: srv, Tunnel: testTunnelCfg(),
		BackoffMin: 20 * time.Millisecond, BackoffMax: 50 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	p := e.dial()
	p.c.Write([]byte("hello"))
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ne net.Error
	if n, err := p.br.Read(make([]byte, 1)); n != 0 || errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("a closed gate must refuse the player: read %d, %v", n, err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := e.session().Stats(); st.Connects != 0 || st.Streams != 0 {
		t.Fatalf("closed gate: %d relay connects, %d streams", st.Connects, st.Streams)
	}

	open.Store(true)
	e.waitFor("relay connected", e.session().Connected)
	p = e.dial()
	p.c.Write([]byte("hello"))
	p.expect([]byte("hello"))
}
