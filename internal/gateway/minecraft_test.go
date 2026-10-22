package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/mcproto/mctest"
	"lcm/internal/relay"
	"lcm/internal/tunnel"
)

const testInterval = 100 * time.Millisecond

func testLogger() *slog.Logger {
	lvl := slog.LevelWarn
	if os.Getenv("LCM_TEST_LOG") != "" {
		lvl = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func testTunnelCfg() tunnel.Config {
	return tunnel.Config{Token: []byte("t"), PingInterval: 100 * time.Millisecond,
		DeadTimeout: time.Second, AckDelay: 2 * time.Millisecond, HandshakeTimeout: 2 * time.Second}
}

type mcEnv struct {
	t          *testing.T
	gw         *Gateway
	playerAddr string
	tunnelAddr string
}

func newMCEnv(t *testing.T, mc *MinecraftConfig) *mcEnv {
	t.Helper()
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger(), Minecraft: mc})
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
	t.Cleanup(func() { pl.Close(); tl.Close(); gw.DefaultBackend().Session.CloseAll() })
	return &mcEnv{t: t, gw: gw, playerAddr: pl.Addr().String(), tunnelAddr: tl.Addr().String()}
}

func (e *mcEnv) session() *tunnel.Session { return e.gw.DefaultBackend().Session }

func (e *mcEnv) startServer(ready bool) *mctest.Server {
	s, err := mctest.Start(true)
	if err != nil {
		e.t.Fatal(err)
	}
	s.SetReady(ready)
	e.t.Cleanup(s.Close)
	return s
}

func (e *mcEnv) startRelay(upstream string) *relay.Relay {
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Upstream: upstream, ProxyProtocol: true,
		Tunnel: testTunnelCfg(), Ready: relay.ReadyMinecraft, ReadyInterval: 30 * time.Millisecond,
		BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	e.t.Cleanup(func() { cancel(); <-done })
	return r
}

func (e *mcEnv) waitFor(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// player is a fake client.
type player struct {
	t     *testing.T
	c     net.Conn
	br    *bufio.Reader
	sent  []byte
	start time.Time
	rd    *mcproto.Reader // packet reader over br, kept across calls (menu tests)
}

func (e *mcEnv) dial() *player {
	e.t.Helper()
	c, err := net.Dial("tcp", e.playerAddr)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { c.Close() })
	return &player{t: e.t, c: c, br: bufio.NewReader(c), start: time.Now()}
}

func (e *mcEnv) login(protocol int32, name string) *player {
	p := e.dial()
	p.sent = mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: protocol, Host: "mc.test\x00FML3\x00", Port: 25565, Intent: mcproto.IntentLogin})
	p.sent = mcproto.AppendLoginStart(p.sent, name)
	// Send in two pieces to exercise the sniffer's reassembly.
	p.c.Write(p.sent[:5])
	time.Sleep(10 * time.Millisecond)
	p.c.Write(p.sent[5:])
	return p
}

// keepalives reads n Set Compression -1 packets and checks their spacing.
func (p *player) keepalives(n int, interval time.Duration) {
	p.t.Helper()
	last := time.Now()
	buf := make([]byte, len(mcproto.SetCompressionNoop))
	for i := 0; i < n; i++ {
		p.c.SetReadDeadline(time.Now().Add(interval + time.Second))
		if _, err := io.ReadFull(p.br, buf); err != nil {
			p.t.Fatalf("keepalive %d: %v", i, err)
		}
		if !bytes.Equal(buf, mcproto.SetCompressionNoop) {
			p.t.Fatalf("keepalive %d: got %x", i, buf)
		}
		now := time.Now()
		if gap := now.Sub(last); gap > interval+300*time.Millisecond {
			p.t.Fatalf("keepalive %d arrived %v after the previous one (interval %v)", i, gap, interval)
		}
		last = now
	}
}

// skipKeepalives consumes any keepalives, then returns.
func (p *player) skipKeepalives() int {
	p.t.Helper()
	n := 0
	for {
		p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, err := p.br.Peek(1)
		if err != nil {
			p.t.Fatalf("peek: %v", err)
		}
		if b[0] != mcproto.SetCompressionNoop[0] {
			return n
		}
		buf := make([]byte, len(mcproto.SetCompressionNoop))
		io.ReadFull(p.br, buf)
		if !bytes.Equal(buf, mcproto.SetCompressionNoop) {
			p.t.Fatalf("expected keepalive, got %x", buf)
		}
		n++
	}
}

func (p *player) expect(want []byte) {
	p.t.Helper()
	got := make([]byte, len(want))
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(p.br, got); err != nil {
		p.t.Fatalf("reading %d bytes: %v", len(want), err)
	}
	if !bytes.Equal(got, want) {
		p.t.Fatalf("got %x, want %x", got, want)
	}
}

func (p *player) expectSilence(d time.Duration) {
	p.t.Helper()
	p.c.SetReadDeadline(time.Now().Add(d))
	b, err := p.br.Peek(1)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		p.t.Fatalf("expected silence for %v, got %x err=%v", d, b, err)
	}
}

func (p *player) expectEOF() {
	p.t.Helper()
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err := p.br.ReadByte()
	if err == nil {
		p.t.Fatalf("expected EOF, got byte %#x", b)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		p.t.Fatalf("expected EOF, timed out")
	}
}

func (e *mcEnv) nextLogin(s *mctest.Server) *mctest.Login {
	e.t.Helper()
	select {
	case l := <-s.Logins:
		e.t.Cleanup(func() { l.Conn.Close() })
		return l
	case <-time.After(5 * time.Second):
		e.t.Fatal("server got no login")
		return nil
	}
}

// A login arrives while the server is still booting (its port accepts but
// status is not answered). The client is kept alive with Set Compression -1
// at the configured interval, the relay only connects once the server
// answers status, and then the client gets the server's bytes byte-exact,
// with no keepalive ever after the first server byte.
func TestLoginHeldUntilServerReady(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, MaxHold: time.Minute})
	srv := e.startServer(false)
	r := e.startRelay(srv.Addr())

	p := e.login(774, "Steve")
	p.keepalives(6, testInterval)
	if e.session().Connected() || r.Ready() {
		t.Fatal("relay connected before the server was ready")
	}
	if srv.StatusRefused.Load() == 0 {
		t.Fatal("relay did not probe the server")
	}
	if st := e.gw.Status(false); st.Backends[0].Held != 1 {
		t.Fatalf("held = %d, want 1", st.Backends[0].Held)
	}

	srv.SetReady(true)
	l := e.nextLogin(srv)
	if !bytes.Equal(l.Raw, p.sent) || l.Name != "Steve" {
		t.Fatalf("server got %x, client sent %x", l.Raw, p.sent)
	}
	if !r.Ready() {
		t.Fatal("relay not marked ready")
	}

	first := []byte("\x02\x01SERVER-FIRST") // not 0x06, so distinguishable from a keepalive
	l.Conn.Write(first)
	p.skipKeepalives()
	p.expect(first)

	// Long server silence after the first byte: no injection. The next
	// bytes even start like a keepalive; they must arrive exactly once.
	time.Sleep(5 * testInterval)
	second := append(append([]byte{}, mcproto.SetCompressionNoop...), []byte("compressed-or-encrypted")...)
	l.Conn.Write(second)
	p.expect(second)
	p.expectSilence(3 * testInterval)

	// Client to server still works.
	p.c.Write([]byte("hello server"))
	got := make([]byte, 12)
	l.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(l.R, got); err != nil || string(got) != "hello server" {
		t.Fatalf("server read %q %v", got, err)
	}
	e.waitFor("held count back to 0", func() bool { return e.gw.Status(false).Backends[0].Held == 0 })
}

func statusExchange(t *testing.T, p *player, protocol int32) mcproto.StatusDoc {
	t.Helper()
	req := mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: protocol, Host: "mc.test", Port: 25565, Intent: mcproto.IntentStatus})
	req = append(req, mcproto.StatusRequest...)
	p.c.Write(req)
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	rd := mcproto.NewReader(p.br, 1<<20)
	id, body, err := rd.ReadPacket()
	if err != nil || id != 0 {
		t.Fatalf("status response: id=%d err=%v", id, err)
	}
	js, err := mcproto.ParseStatusResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	var doc mcproto.StatusDoc
	if err := json.Unmarshal([]byte(js), &doc); err != nil {
		t.Fatalf("%s: %v", js, err)
	}
	ping := mcproto.AppendPacket(nil, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	p.c.Write(ping)
	id, body, err = rd.ReadPacket()
	if err != nil || id != 1 || !bytes.Equal(body, ping[2:]) {
		t.Fatalf("pong: id=%d body=%x err=%v", id, body, err)
	}
	return doc
}

func TestStatusLocalWhileDownThenPassthrough(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{MOTD: "Starting... ~2 min", VersionName: "lcm"})
	doc := statusExchange(t, e.dial(), 765)
	if doc.Description.Text != "Starting... ~2 min" || doc.Version.Protocol != 765 || doc.Version.Name != "lcm" {
		t.Fatalf("local status: %+v", doc)
	}
	if e.session().NumStreams() != 0 {
		t.Fatal("locally answered status created a stream")
	}

	srv := e.startServer(true)
	e.startRelay(srv.Addr())
	e.waitFor("relay connected", e.session().Connected)
	doc = statusExchange(t, e.dial(), 765)
	if doc.Description.Text != "REAL SERVER" || doc.Version.Name != "Fake 1.21" {
		t.Fatalf("status not passed through: %+v", doc)
	}
	if srv.StatusAnswered.Load() == 0 {
		t.Fatal("server answered no status")
	}
}

// A migration-like outage in the middle of an established stream (the relay
// quiesces, the tunnel is down) must not inject anything into that stream;
// a login arriving during the outage is held with keepalives.
func TestNoInjectionDuringOutage(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, MaxHold: time.Minute})
	srv := e.startServer(true)
	r := e.startRelay(srv.Addr())
	e.waitFor("relay connected", e.session().Connected)

	p := e.login(774, "Alex")
	l := e.nextLogin(srv)
	first := []byte("\x05\x02play")
	l.Conn.Write(first)
	p.skipKeepalives()
	p.expect(first)

	if err := r.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.waitFor("tunnel down", func() bool { return !e.session().Connected() })
	during := []byte("written by the server during the blackout")
	l.Conn.Write(during)
	p2 := e.login(774, "Late")
	p2.keepalives(3, testInterval)
	p.expectSilence(8 * testInterval)

	r.Resume()
	p.expect(during)
	l2 := e.nextLogin(srv)
	if !bytes.Equal(l2.Raw, p2.sent) {
		t.Fatalf("late login replay mismatch: %x vs %x", l2.Raw, p2.sent)
	}
	l2.Conn.Write([]byte("\x01\x00"))
	p2.skipKeepalives()
	p2.expect([]byte("\x01\x00"))
	p.expectSilence(3 * testInterval)
}

func TestHoldLimitDisconnect(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, MaxHold: 450 * time.Millisecond, HoldMessage: "too slow"})
	p := e.login(774, "Steve")
	if n := p.skipKeepalives(); n < 2 {
		t.Fatalf("only %d keepalives before the kick", n)
	}
	p.expect(mcproto.LoginDisconnect("too slow"))
	p.expectEOF()
	e.waitFor("stream dropped", func() bool { return e.session().NumStreams() == 0 })
	e.waitFor("held count back to 0", func() bool { return e.gw.Status(false).Backends[0].Held == 0 })
}

// 1.7.x has no Set Compression: no keepalive, and the hold is capped.
func TestLegacyProtocolNoKeepalive(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, MaxHold: time.Minute,
		LegacyMaxHold: 300 * time.Millisecond, HoldMessage: "rejoin"})
	p := e.login(5, "Old")
	p.expect(mcproto.LoginDisconnect("rejoin"))
	p.expectEOF()
}

func TestGarbageAndLegacyPing(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{MOTD: "Down"})
	p := e.dial()
	p.c.Write([]byte("GET / HTTP/1.1\r\nHost: example\r\n\r\n" + string(make([]byte, 64))))
	p.expectEOF()

	p = e.dial()
	p.c.Write([]byte{0x00, 0x00}) // zero-length packet
	p.expectEOF()

	for _, legacy := range [][]byte{{0xfe}, {0xfe, 0x01}, {0xfe, 0x01, 0xfa, 0x00, 0x0b}} {
		p = e.dial()
		p.c.Write(legacy)
		p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, err := p.br.ReadByte()
		if err != nil || b != 0xff {
			t.Fatalf("legacy %x: first byte %#x err=%v", legacy, b, err)
		}
		io.Copy(io.Discard, p.br)
	}
	if n := e.session().NumStreams(); n != 0 {
		t.Fatalf("%d streams left", n)
	}
}

// With Minecraft mode off the gateway stays pure L4: arbitrary bytes pass
// through untouched (the e2e tests cover this in depth).
func TestMinecraftOffIsPureL4(t *testing.T) {
	e := newMCEnv(t, nil)
	p := e.dial()
	p.c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	time.Sleep(50 * time.Millisecond)
	if e.session().NumStreams() != 1 {
		t.Fatal("non-Minecraft connection was not streamed")
	}
	p.expectSilence(3 * testInterval) // nothing injected while the relay is absent
}

// One gateway, two servers: players are routed by the hostname they connect to
// (first label = backend name, explicit routes, default for the rest).
func TestRouteByHostname(t *testing.T) {
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger(), Minecraft: &MinecraftConfig{HoldInterval: testInterval},
		Backend: "lobby", Backends: []string{"atm10"}, Routes: map[string]string{"modded.example.org": "atm10"}})
	pl, _ := Listen(context.Background(), "127.0.0.1:0")
	tl, _ := Listen(context.Background(), "127.0.0.1:0")
	go gw.ServePlayers(pl, gw.DefaultBackend())
	go gw.ServeTunnel(tl)
	defer pl.Close()
	defer tl.Close()
	e := &mcEnv{t: t, gw: gw, playerAddr: pl.Addr().String(), tunnelAddr: tl.Addr().String()}
	servers := map[string]*mctest.Server{}
	for _, name := range []string{"lobby", "atm10"} {
		s := e.startServer(true)
		servers[name] = s
		r := relay.New(relay.Config{Gateway: e.tunnelAddr, Backend: name, Upstream: s.Addr(), ProxyProtocol: true,
			Tunnel: testTunnelCfg(), BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { r.Run(ctx); close(done) }()
		defer func() { cancel(); <-done }()
		e.waitFor("relay "+name, func() bool { return gw.Backend(name).Session.Connected() })
	}
	for host, want := range map[string]string{
		"atm10.mc.example.com\x00FML3\x00": "atm10",
		"ATM10.mc.example.com.":            "atm10",
		"modded.example.org":               "atm10",
		"mc.example.com":                   "lobby",
		"nosuchpack.mc.example.com":        "lobby",
	} {
		p := e.dial()
		p.c.Write(mcproto.AppendLoginStart(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: host, Port: 25565, Intent: mcproto.IntentLogin}), "steve"))
		select {
		case l := <-servers[want].Logins:
			if l.Handshake.Host != host {
				t.Errorf("%q: server got host %q", host, l.Handshake.Host)
			}
			l.Conn.Close()
		case <-time.After(5 * time.Second):
			t.Fatalf("%q: no login at %s", host, want)
		}
		p.c.Close()
	}
}

func TestWakeOnLogin(t *testing.T) {
	woke := make(chan string, 8)
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger(), Minecraft: &MinecraftConfig{HoldInterval: testInterval},
		Backend: "lobby", Backends: []string{"atm10"}, WakeEvery: time.Minute,
		Wake: func(_ context.Context, b string) error { woke <- b; return nil }})
	pl, _ := Listen(context.Background(), "127.0.0.1:0")
	tl, _ := Listen(context.Background(), "127.0.0.1:0")
	go gw.ServePlayers(pl, gw.DefaultBackend())
	go gw.ServeTunnel(tl)
	defer pl.Close()
	defer tl.Close()
	e := &mcEnv{t: t, gw: gw, playerAddr: pl.Addr().String(), tunnelAddr: tl.Addr().String()}

	// A server-list ping doesn't wake anything.
	sp := e.dial()
	sp.c.Write(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "atm10.mc.test", Port: 25565, Intent: mcproto.IntentStatus}))
	sp.c.Write([]byte{1, 0}) // status request
	sp.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	sp.br.ReadByte()
	select {
	case b := <-woke:
		t.Fatalf("status ping woke %s", b)
	case <-time.After(100 * time.Millisecond):
	}

	// Two logins: one wake (debounced), and both are held.
	login := func() *player {
		p := e.dial()
		p.sent = mcproto.AppendLoginStart(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "atm10.mc.test", Port: 25565, Intent: mcproto.IntentLogin}), "alex")
		p.c.Write(p.sent)
		return p
	}
	p1, p2 := login(), login()
	select {
	case b := <-woke:
		if b != "atm10" {
			t.Fatalf("woke %s", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no wake")
	}
	p1.keepalives(1, testInterval)
	p2.keepalives(1, testInterval)
	select {
	case b := <-woke:
		t.Fatalf("second wake for %s within WakeEvery", b)
	default:
	}

	// The server comes up: the held login reaches it.
	s := e.startServer(true)
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Backend: "atm10", Upstream: s.Addr(), ProxyProtocol: true,
		Tunnel: testTunnelCfg(), BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	select {
	case l := <-s.Logins:
		if l.Name != "alex" {
			t.Fatalf("login %q", l.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("held login never reached the woken server")
	}
}
