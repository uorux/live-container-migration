// Package e2e runs gateway, relay, an echo "server" and fake players in one
// process, with a chaos proxy on the tunnel link.
package e2e

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lcm/internal/gateway"
	"lcm/internal/loadtest"
	"lcm/internal/relay"
	"lcm/internal/tunnel"
)

var token = []byte("test-token")

func logger() *slog.Logger {
	lvl := slog.LevelWarn
	if os.Getenv("LCM_TEST_LOG") != "" {
		lvl = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func tunnelCfg(window int) tunnel.Config {
	return tunnel.Config{Token: token, Window: window, PingInterval: 100 * time.Millisecond,
		DeadTimeout: time.Second, AckDelay: 2 * time.Millisecond, HandshakeTimeout: 2 * time.Second}
}

// chaosProxy forwards TCP connections and can sever or blackhole them.
type chaosProxy struct {
	ln     net.Listener
	target string
	paused atomic.Bool
	mu     sync.Mutex
	conns  map[net.Conn]bool
}

func newChaosProxy(t *testing.T, target string) *chaosProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &chaosProxy{ln: ln, target: target, conns: map[net.Conn]bool{}}
	go p.serve()
	t.Cleanup(func() { ln.Close(); p.sever() })
	return p
}

func (p *chaosProxy) addr() string { return p.ln.Addr().String() }

func (p *chaosProxy) serve() {
	for {
		a, err := p.ln.Accept()
		if err != nil {
			return
		}
		b, err := net.Dial("tcp", p.target)
		if err != nil {
			a.Close()
			continue
		}
		p.mu.Lock()
		p.conns[a], p.conns[b] = true, true
		p.mu.Unlock()
		go p.pipe(a, b)
		go p.pipe(b, a)
	}
}

func (p *chaosProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	defer func() { dst.Close(); src.Close() }()
	for {
		for p.paused.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		n, err := src.Read(buf)
		if n > 0 {
			for p.paused.Load() { // blackhole: hold bytes, deliver nothing
				time.Sleep(5 * time.Millisecond)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// sever abruptly closes every proxied connection (RST where possible).
func (p *chaosProxy) sever() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		c.Close()
		delete(p.conns, c)
	}
}

type harness struct {
	t          *testing.T
	gw         *gateway.Gateway
	playerAddr string
	tunnelAddr string
	echo       *loadtest.EchoServer
	proxy      *chaosProxy
	window     int
}

type relayHandle struct {
	r      *relay.Relay
	cancel context.CancelFunc
	done   chan struct{}
	ctl    string
}

func newHarness(t *testing.T, window int) *harness {
	return newHarnessCfg(t, window, gateway.Config{})
}

func newHarnessCfg(t *testing.T, window int, cfg gateway.Config) *harness {
	h := &harness{t: t, window: window}
	cfg.Tunnel, cfg.Logger = tunnelCfg(window), logger()
	h.gw = gateway.New(cfg)
	pl, err := gateway.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tl, err := gateway.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go h.gw.ServePlayers(pl, h.gw.DefaultBackend())
	go h.gw.ServeTunnel(tl)
	h.playerAddr, h.tunnelAddr = pl.Addr().String(), tl.Addr().String()
	h.echo, err = loadtest.NewEchoServer("127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	go h.echo.Serve()
	h.proxy = newChaosProxy(t, h.tunnelAddr)
	t.Cleanup(func() {
		pl.Close()
		tl.Close()
		h.echo.Close()
		h.gw.DefaultBackend().Session.CloseAll()
	})
	return h
}

func (h *harness) startRelay(mod func(*relay.Config)) *relayHandle {
	ctl := filepath.Join(h.t.TempDir(), "relay.sock")
	cfg := relay.Config{
		Gateway: h.proxy.addr(), Upstream: h.echo.Addr(), ProxyProtocol: true,
		Tunnel: tunnelCfg(h.window), QuiesceTimeout: time.Minute, CheckSockets: true,
		BackoffMin: 20 * time.Millisecond, BackoffMax: 200 * time.Millisecond, Logger: logger(),
	}
	if mod != nil {
		mod(&cfg)
	}
	r := relay.New(cfg)
	ln, err := relay.ListenControl(ctl)
	if err != nil {
		h.t.Fatal(err)
	}
	go r.ServeControl(ln)
	ctx, cancel := context.WithCancel(context.Background())
	rh := &relayHandle{r: r, cancel: cancel, done: make(chan struct{}), ctl: ctl}
	go func() { r.Run(ctx); close(rh.done) }()
	h.t.Cleanup(func() { rh.kill(); ln.Close() })
	return rh
}

// kill simulates the container dying: tunnel aborted, server conns closed.
func (rh *relayHandle) kill() {
	rh.cancel()
	<-rh.done
}

func (h *harness) gwStats() tunnel.Stats { return h.gw.DefaultBackend().Session.Stats() }

func (h *harness) waitFor(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// control sends one control command and returns the reply line; for quiesce
// it also waits for the relay to close the connection.
func control(t *testing.T, path, cmd string) string {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(c, cmd+"\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	if cmd == "quiesce" {
		if n, err := br.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Fatalf("relay did not close control conn after quiesce: n=%d err=%v", n, err)
		}
	}
	return strings.TrimSpace(line)
}

// players runs n fake players concurrently; results arrive on the returned channel.
type playerRun struct {
	verified atomic.Int64
	errs     chan error
	results  chan loadtest.PlayerResult
	n        int
}

func (h *harness) players(ctx context.Context, n int, seed uint64, mod func(i int, c *loadtest.PlayerConfig)) *playerRun {
	pr := &playerRun{errs: make(chan error, n), results: make(chan loadtest.PlayerResult, n), n: n}
	for i := 0; i < n; i++ {
		cfg := loadtest.PlayerConfig{Addr: h.playerAddr, Seed: seed + uint64(i), Bytes: 2 << 20,
			HalfClose: true, ExpectEOFAfter: -1, OnVerified: func(k int) { pr.verified.Add(int64(k)) }}
		if mod != nil {
			mod(i, &cfg)
		}
		go func() {
			res, err := loadtest.RunPlayer(ctx, cfg)
			pr.results <- res
			pr.errs <- err
		}()
	}
	return pr
}

func (pr *playerRun) wait(t *testing.T) []loadtest.PlayerResult {
	t.Helper()
	var out []loadtest.PlayerResult
	for i := 0; i < pr.n; i++ {
		if err := <-pr.errs; err != nil {
			t.Errorf("player failed: %v", err)
		}
		out = append(out, <-pr.results)
	}
	return out
}
