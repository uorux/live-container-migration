package relay

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lcm/internal/mcproto/mctest"
	"lcm/internal/tunnel"
)

// fakeGateway accepts tunnel connections and counts them; it never answers,
// so a test can close them to force a reconnect.
type fakeGateway struct {
	ln      net.Listener
	accepts atomic.Int64
	mu      sync.Mutex
	conns   []net.Conn
}

func newFakeGateway(t *testing.T) *fakeGateway {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			g.conns = append(g.conns, c)
			g.mu.Unlock()
			g.accepts.Add(1)
		}
	}()
	t.Cleanup(func() { ln.Close(); g.dropAll() })
	return g
}

func (g *fakeGateway) dropAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.conns {
		c.Close()
	}
	g.conns = nil
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func runRelay(t *testing.T, cfg Config) *Relay {
	cfg.Tunnel = tunnel.Config{Token: []byte("x"), HandshakeTimeout: 5 * time.Second}
	cfg.BackoffMin, cfg.BackoffMax = 10*time.Millisecond, 50*time.Millisecond
	cfg.ReadyInterval = 20 * time.Millisecond
	r := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return r
}

// The relay must not dial the gateway until the server answers a status
// ping (a TCP accept is not enough), and once ready it never probes again:
// reconnects (as after a CRIU restore, whose memory says "ready") go
// straight to the gateway.
func TestMinecraftReadinessGate(t *testing.T) {
	gw := newFakeGateway(t)
	srv, err := mctest.Start(true)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	r := runRelay(t, Config{Gateway: gw.ln.Addr().String(), Upstream: srv.Addr(), ProxyProtocol: true, Ready: ReadyMinecraft})

	waitUntil(t, "several refused probes", func() bool { return srv.StatusRefused.Load() >= 3 })
	if gw.accepts.Load() != 0 || r.Ready() {
		t.Fatal("relay dialed the gateway before the server was ready")
	}
	if st := r.Status(); st.State != "waiting-for-server" || st.Ready {
		t.Fatalf("status %+v", st)
	}

	srv.SetReady(true)
	waitUntil(t, "tunnel dial", func() bool { return gw.accepts.Load() == 1 })
	if !r.Ready() || !r.Status().Ready {
		t.Fatal("not marked ready")
	}

	srv.SetReady(false)
	answered, refused, probes := srv.StatusAnswered.Load(), srv.StatusRefused.Load(), r.probes.Load()
	gw.dropAll()
	waitUntil(t, "reconnect", func() bool { return gw.accepts.Load() >= 2 })
	if r.probes.Load() != probes || srv.StatusAnswered.Load() != answered || srv.StatusRefused.Load() != refused {
		t.Fatal("relay re-ran the readiness check after it had passed")
	}
}

func TestTCPReadinessGate(t *testing.T) {
	gw := newFakeGateway(t)
	ln0, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln0.Addr().String()
	ln0.Close() // nothing listens yet
	r := runRelay(t, Config{Gateway: gw.ln.Addr().String(), Upstream: addr, Ready: ReadyTCP})
	waitUntil(t, "a few probes", func() bool { return r.probes.Load() >= 3 })
	if gw.accepts.Load() != 0 {
		t.Fatal("dialed the gateway while the server port was closed")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skip("port reuse failed:", err)
	}
	defer ln.Close()
	waitUntil(t, "tunnel dial", func() bool { return gw.accepts.Load() == 1 })
}

// Quiesce while still waiting for the server must succeed promptly, and the
// wait continues after resume.
func TestQuiesceWhileWaitingForServer(t *testing.T) {
	gw := newFakeGateway(t)
	srv, err := mctest.Start(false)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	r := runRelay(t, Config{Gateway: gw.ln.Addr().String(), Upstream: srv.Addr(), Ready: ReadyMinecraft})
	waitUntil(t, "probing", func() bool { return srv.StatusRefused.Load() >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	n := srv.StatusRefused.Load()
	time.Sleep(100 * time.Millisecond)
	if srv.StatusRefused.Load() != n {
		t.Fatal("still probing while quiesced")
	}
	r.Resume()
	srv.SetReady(true)
	waitUntil(t, "tunnel dial", func() bool { return gw.accepts.Load() == 1 })
}
