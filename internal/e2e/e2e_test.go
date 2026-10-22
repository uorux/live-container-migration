package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"lcm/internal/loadtest"
	"lcm/internal/relay"
)

// (a) Tunnel TCP dropped abruptly (RST) several times mid-transfer.
func TestTunnelDropMidTransfer(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	sid := rh.r.SessionID()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pr := h.players(ctx, 8, 100, func(i int, c *loadtest.PlayerConfig) {
		c.Bytes = 3 << 20
		c.Rate = 2 << 20 // ~1.5 s per player, so the drops land mid-stream
	})
	for i := 0; i < 4; i++ {
		time.Sleep(250 * time.Millisecond)
		before := h.gwStats().Connects
		h.proxy.sever()
		h.waitFor("reconnect", 5*time.Second, func() bool { s := h.gwStats(); return s.Connected && s.Connects > before })
	}
	pr.wait(t)
	if got := pr.verified.Load(); got != 8*3<<20 {
		t.Fatalf("verified %d bytes", got)
	}
	if rh.r.SessionID() != sid || h.gwStats().Connects < 5 {
		t.Fatalf("session changed or too few reconnects: %+v", h.gwStats())
	}
	// Every drop ended in a resume and was observed as an outage; bytes were counted.
	var mb strings.Builder
	h.gw.WriteMetrics(&mb)
	metricsText := mb.String()
	st := h.gwStats()
	for _, want := range []string{
		fmt.Sprintf(`lcm_gateway_tunnel_outage_seconds_count{backend="default"} %d`, st.Resumes),
		fmt.Sprintf(`lcm_gateway_tunnel_connects_total{backend="default",kind="resume"} %d`, st.Resumes),
		`lcm_gateway_tunnel_connects_total{backend="default",kind="new_session"} 1`,
		`lcm_gateway_bytes_total{backend="default",direction="to_player"} ` + strconv.FormatFloat(float64(st.BytesToLocal), 'g', -1, 64),
		`lcm_gateway_player_connections_total{backend="default",result="opened"} 8`,
	} {
		if !strings.Contains(metricsText, want+"\n") {
			t.Errorf("metrics lack %q:\n%s", want, metricsText)
		}
	}
	if st.Resumes < 4 || st.BytesToLocal < 8*3<<20 {
		t.Fatalf("resumes %d, bytes to players %d", st.Resumes, st.BytesToLocal)
	}
	// PROXY headers carried the players' real (source) addresses.
	hdrs := h.echo.Headers()
	if len(hdrs) != 8 {
		t.Fatalf("%d proxy headers", len(hdrs))
	}
	for _, hd := range hdrs {
		if hd.Local || !hd.Src.Addr().IsLoopback() || hd.Dst.String() != h.playerAddr {
			t.Fatalf("bad proxy header %+v (player addr %s)", hd, h.playerAddr)
		}
	}
	h.waitFor("streams cleaned up", 5*time.Second, func() bool {
		return h.gwStats().Streams == 0 && rh.r.Session().NumStreams() == 0 && h.echo.NumConns() == 0
	})
}

// (b) quiesce -> frozen period -> resume via the control socket, players keep sending.
func TestQuiesceResume(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pr := h.players(ctx, 6, 200, func(i int, c *loadtest.PlayerConfig) {
		c.Bytes = 3 << 20
		c.Rate = 1 << 20
	})
	time.Sleep(500 * time.Millisecond)
	if r := control(t, rh.ctl, "quiesce"); r != "ok" {
		t.Fatalf("quiesce: %q", r)
	}
	if h.gwStats().Connected && !waitDisconnected(h) {
		t.Fatal("gateway still sees the tunnel after quiesce")
	}
	if st := rh.r.Status(); st.State != "quiesced" || st.Connected {
		t.Fatalf("relay status %+v", st)
	}
	if socks, err := relay.NonLoopbackSockets(); err == nil && len(socks) > 0 {
		t.Fatalf("non-loopback sockets after quiesce: %v", socks)
	}
	v0 := pr.verified.Load()
	time.Sleep(2 * time.Second) // "CRIU dump + transfer + restore"
	if v := pr.verified.Load(); v != v0 {
		t.Fatalf("bytes flowed while quiesced: %d -> %d", v0, v)
	}
	if h.gwStats().Connects != 1 {
		t.Fatal("relay reconnected while quiesced")
	}
	if r := control(t, rh.ctl, "resume"); r != "ok" {
		t.Fatalf("resume: %q", r)
	}
	pr.wait(t)
	if h.gwStats().Connects != 2 {
		t.Fatalf("connects %d", h.gwStats().Connects)
	}
	if r := control(t, rh.ctl, "status"); r == "" || r[0] != '{' {
		t.Fatalf("status: %q", r)
	}
}

func waitDisconnected(h *harness) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !h.gwStats().Connected {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// (c) New players arrive during the outage (and before any relay ever connected).
func TestNewPlayersDuringOutage(t *testing.T) {
	h := newHarness(t, 256<<10)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	early := h.players(ctx, 3, 300, nil) // gateway has no relay yet
	time.Sleep(300 * time.Millisecond)
	rh := h.startRelay(nil)
	early.wait(t)

	if r := control(t, rh.ctl, "quiesce"); r != "ok" {
		t.Fatalf("quiesce: %q", r)
	}
	late := h.players(ctx, 5, 400, nil)
	h.waitFor("players accepted", 5*time.Second, func() bool { return h.gwStats().Streams == 5 })
	time.Sleep(time.Second)
	if h.echo.NumConns() != 0 {
		t.Fatal("server saw connections while the relay was quiesced")
	}
	if r := control(t, rh.ctl, "resume"); r != "ok" {
		t.Fatalf("resume: %q", r)
	}
	late.wait(t)
	if n := len(h.echo.Headers()); n != 8 {
		t.Fatalf("server saw %d connections, want 8", n)
	}
}

// (d) The relay comes back with a new session id (server restarted, not
// migrated): old streams must be reset, new ones work.
func TestNewSessionResetsStreams(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	old := h.players(ctx, 4, 500, func(i int, c *loadtest.PlayerConfig) {
		c.Bytes = 64 << 20
		c.Rate = 256 << 10 // would take minutes
	})
	h.waitFor("old players flowing", 5*time.Second, func() bool { return old.verified.Load() > 4*64<<10 })
	oldID := rh.r.SessionID()
	rh.kill() // container died; tunnel aborted without GOAWAY

	start := time.Now()
	rh2 := h.startRelay(nil)
	if rh2.r.SessionID() == oldID {
		t.Fatal("session ids collide")
	}
	for i := 0; i < 4; i++ {
		if err := <-old.errs; err == nil {
			t.Fatal("old player survived a session change")
		}
		<-old.results
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("old players were reset only after %v", d)
	}
	fresh := h.players(ctx, 3, 600, nil)
	fresh.wait(t)
}

// (e) The server closes a stream: the player gets everything up to that
// point and then EOF; stream state is cleaned up on both sides.
func TestServerClosesStream(t *testing.T) {
	h := newHarness(t, 256<<10)
	h.echo.CloseAfter = 100_000
	rh := h.startRelay(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pr := h.players(ctx, 4, 700, func(i int, c *loadtest.PlayerConfig) {
		c.Bytes = 1 << 20
		c.HalfClose = false
		c.ExpectEOFAfter = 100_000
	})
	pr.wait(t)
	h.waitFor("streams cleaned up", 5*time.Second, func() bool {
		return h.gwStats().Streams == 0 && rh.r.Session().NumStreams() == 0
	})
}

// The server aborts connections (RST) mid-stream: players are disconnected,
// never see corrupted data, and stream state is cleaned up. (Bytes the
// server's own kernel discards on RST are lost, as with a direct connection.)
func TestServerResetsStream(t *testing.T) {
	h := newHarness(t, 256<<10)
	h.echo.AbortAfter = 100_000
	rh := h.startRelay(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pr := h.players(ctx, 4, 750, func(i int, c *loadtest.PlayerConfig) { c.Bytes = 1 << 20 })
	for i := 0; i < 4; i++ {
		err := <-pr.errs
		res := <-pr.results
		if err == nil || strings.Contains(err.Error(), "mismatch") || res.Verified > 100_000 {
			t.Fatalf("player: verified %d, err %v", res.Verified, err)
		}
	}
	h.waitFor("streams cleaned up", 5*time.Second, func() bool {
		return h.gwStats().Streams == 0 && rh.r.Session().NumStreams() == 0
	})
}

// Player half-close and full close propagate to the server.
func TestPlayerCloses(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.players(ctx, 5, 800, func(i int, c *loadtest.PlayerConfig) { c.Bytes = 300_000 }).wait(t)
	h.waitFor("server conns closed", 5*time.Second, func() bool {
		return h.echo.NumConns() == 0 && h.gwStats().Streams == 0 && rh.r.Session().NumStreams() == 0
	})
}

// (f) Outage longer than the per-stream buffer: the gateway stops reading
// from the player (backpressure) and loses nothing.
func TestBackpressureDuringOutage(t *testing.T) {
	const window = 64 << 10
	h := newHarness(t, window)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if r := control(t, rh.ctl, "quiesce"); r != "ok" {
		t.Fatalf("quiesce: %q", r)
	}
	pr := h.players(ctx, 3, 900, func(i int, c *loadtest.PlayerConfig) { c.Bytes = 32 << 20 })
	time.Sleep(1500 * time.Millisecond)
	st := h.gwStats()
	if st.MaxTxBuffered > window {
		t.Fatalf("gateway buffered %d bytes for one stream, cap %d", st.MaxTxBuffered, window)
	}
	if st.MaxTxBuffered < window/2 {
		t.Fatalf("gateway buffered only %d bytes; expected to fill the window", st.MaxTxBuffered)
	}
	if r := control(t, rh.ctl, "resume"); r != "ok" {
		t.Fatalf("resume: %q", r)
	}
	pr.wait(t)
	if h.gwStats().MaxTxBuffered > window {
		t.Fatal("window exceeded after resume")
	}
}

// Heartbeat: a blackholed tunnel (no FIN/RST) is detected on both sides
// within DeadTimeout and resumed.
func TestHeartbeatBlackhole(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pr := h.players(ctx, 3, 1000, func(i int, c *loadtest.PlayerConfig) { c.Bytes = 2 << 20; c.Rate = 1 << 20 })
	time.Sleep(300 * time.Millisecond)
	h.proxy.paused.Store(true)
	start := time.Now()
	h.waitFor("gateway detects dead tunnel", 3*time.Second, func() bool { return !h.gwStats().Connected })
	h.waitFor("relay detects dead tunnel", 3*time.Second, func() bool { return !rh.r.Status().Connected })
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("declared dead after %v, before DeadTimeout", d)
	}
	h.proxy.sever()
	h.proxy.paused.Store(false)
	pr.wait(t)
}

// Aborted migration: no resume arrives; the relay resumes on its own.
func TestAutoResume(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(func(c *relay.Config) { c.QuiesceTimeout = 500 * time.Millisecond })
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pr := h.players(ctx, 2, 1100, func(i int, c *loadtest.PlayerConfig) { c.Rate = 2 << 20 })
	if r := control(t, rh.ctl, "quiesce"); r != "ok" {
		t.Fatalf("quiesce: %q", r)
	}
	h.waitFor("auto resume", 3*time.Second, func() bool { return h.gwStats().Connects == 2 && h.gwStats().Connected })
	pr.wait(t)
}

// Wrong token: the gateway refuses the relay.
func TestBadToken(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(func(c *relay.Config) { c.Tunnel.Token = []byte("wrong") })
	time.Sleep(500 * time.Millisecond)
	if h.gwStats().Connects != 0 || rh.r.Status().Connected {
		t.Fatal("relay with a bad token was accepted")
	}
}

// Relay exits (server process ended): final GOAWAY closes the players.
func TestFinalGoAway(t *testing.T) {
	h := newHarness(t, 256<<10)
	rh := h.startRelay(nil)
	h.waitFor("tunnel", 5*time.Second, func() bool { return h.gwStats().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pr := h.players(ctx, 3, 1200, func(i int, c *loadtest.PlayerConfig) { c.Bytes = 64 << 20; c.Rate = 256 << 10 })
	h.waitFor("flowing", 5*time.Second, func() bool { return pr.verified.Load() > 3*32<<10 })
	h.echo.Close() // the "server" dies: its connections close
	rh.r.Shutdown(2 * time.Second)
	for i := 0; i < 3; i++ {
		if err := <-pr.errs; err == nil {
			t.Fatal("player should have been disconnected")
		}
	}
	h.waitFor("gateway streams closed", 5*time.Second, func() bool { return h.gwStats().Streams == 0 })
}
