package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/mcproto/mctest"
	"lcm/internal/relay"
)

// menuEnv is a gateway with -menu, backends "survival" (running) and
// "creative" (asleep until woken).
type menuEnv struct {
	*mcEnv
	survival *mctest.Server
	woke     chan string
	wakeErr  atomic.Pointer[error] // what Wake returns
}

func newMenuEnv(t *testing.T) *menuEnv {
	t.Helper()
	woke := make(chan string, 8)
	e := &menuEnv{woke: woke}
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger(),
		Minecraft: &MinecraftConfig{HoldInterval: testInterval, Menu: true, MenuTitle: "Test MC",
			Labels: map[string]string{"creative": "Creative World"}},
		Backend: "survival", Backends: []string{"creative"}, WakeEvery: time.Minute,
		Wake: func(_ context.Context, b string) error {
			woke <- b
			if err := e.wakeErr.Load(); err != nil {
				return *err
			}
			return nil
		}})
	pl, _ := Listen(context.Background(), "127.0.0.1:0")
	tl, _ := Listen(context.Background(), "127.0.0.1:0")
	go gw.ServePlayers(pl, gw.DefaultBackend())
	go gw.ServeTunnel(tl)
	t.Cleanup(func() { pl.Close(); tl.Close() })
	e.mcEnv = &mcEnv{t: t, gw: gw, playerAddr: pl.Addr().String(), tunnelAddr: tl.Addr().String()}
	e.survival = e.startBackend("survival")
	return e
}

func (e *menuEnv) startBackend(name string) *mctest.Server {
	s := e.startServer(true)
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Backend: name, Upstream: s.Addr(), ProxyProtocol: true,
		Tunnel: testTunnelCfg(), BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	e.t.Cleanup(func() { cancel(); <-done })
	e.waitFor("relay "+name, func() bool { return e.gw.Backend(name).Session().Connected() })
	return s
}

var testUUID = [16]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

func loginStart(name string) []byte {
	return append(mcproto.AppendString(nil, name), testUUID[:]...)
}

// menuLogin connects to host as a 1.21.10 client (intent 2 or 3).
func (e *menuEnv) menuLogin(host string, intent int32) *player {
	p := e.dial()
	p.sent = mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 773, Host: host, Port: 25565, Intent: intent})
	p.sent = mcproto.AppendPacket(p.sent, 0x00, loginStart("Alex"))
	p.c.Write(p.sent)
	return p
}

// packet reads the next packet, skipping configuration keepalives if asked.
func (p *player) packet(skipKeepalive bool) (int32, []byte) {
	p.t.Helper()
	if p.rd == nil {
		p.rd = mcproto.NewReader(p.br, 1<<20)
	}
	for {
		p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		p.rd.Discard() // the previous packet, which the caller is done with
		id, body, err := p.rd.ReadPacket()
		if err != nil {
			p.t.Fatalf("reading packet: %v", err)
		}
		if skipKeepalive && id == mcproto.ConfigKeepAliveID {
			continue
		}
		return id, body
	}
}

// enterMenu completes the login and returns the first dialog.
func (p *player) enterMenu() map[string]any {
	p.t.Helper()
	id, body := p.packet(false)
	if id != mcproto.LoginSuccessID || !bytes.HasPrefix(body, testUUID[:]) {
		p.t.Fatalf("expected Login Success, got %#x %x", id, body)
	}
	// Login Acknowledged, then Client Information in the same segment.
	p.c.Write(append(mcproto.AppendPacket(nil, mcproto.LoginAcknowledgedID, nil), mcproto.AppendPacket(nil, 0x00, []byte("client-info"))...))
	return p.dialog()
}

func (p *player) dialog() map[string]any {
	p.t.Helper()
	id, body := p.packet(true)
	if id != 0x12 {
		p.t.Fatalf("expected Show Dialog, got %#x %x", id, body)
	}
	d, n, err := mctest.DecodeNBT(body)
	if err != nil || n != len(body) {
		p.t.Fatalf("dialog NBT: %v (%d of %d bytes)", err, n, len(body))
	}
	return d.(map[string]any)
}

func (p *player) click(action string) {
	body := append(mcproto.AppendString(nil, action), 0) // no payload
	p.c.Write(mcproto.AppendPacket(nil, mcproto.ConfigCustomClickID, body))
}

// transferred reads Store Cookie + Transfer and returns the cookie payload.
func (p *player) transferred() []byte {
	p.t.Helper()
	id, body := p.packet(true)
	if id != 0x0a {
		p.t.Fatalf("expected Store Cookie, got %#x %x", id, body)
	}
	key := mcproto.AppendString(nil, "lcm:route")
	if !bytes.HasPrefix(body, key) {
		p.t.Fatalf("cookie key: %x", body)
	}
	cookie := body[len(key)+1:]
	id, body = p.packet(true)
	if want := mcproto.AppendVarInt(mcproto.AppendString(nil, "mc.test"), 25565); id != 0x0b || !bytes.Equal(body, want) {
		p.t.Fatalf("expected Transfer to mc.test:25565, got %#x %x", id, body)
	}
	if _, _, err := p.rd.ReadPacket(); err == nil {
		p.t.Fatal("expected EOF after Transfer")
	}
	return cookie
}

func text(c any) string {
	switch v := c.(type) {
	case string:
		return v
	case map[string]any:
		s, _ := v["text"].(string)
		if ex, ok := v["extra"].([]any); ok {
			for _, x := range ex {
				s += text(x)
			}
		}
		return s
	}
	return ""
}

// A player on the main address gets the menu, picks a running server, is
// transferred back to the same address and, with the cookie, lands on that
// server with an ordinary login handshake. The cookie works once.
func TestMenuPickRunningServer(t *testing.T) {
	e := newMenuEnv(t)
	p := e.menuLogin("mc.test", mcproto.IntentLogin)
	d := p.enterMenu()
	if d["type"] != "minecraft:multi_action" || text(d["title"]) != "Test MC" || d["can_close_with_escape"] != int8(0) {
		t.Fatalf("dialog: %v", d)
	}
	actions := d["actions"].([]any)
	if len(actions) != 2 {
		t.Fatalf("actions: %v", actions)
	}
	first, second := actions[0].(map[string]any), actions[1].(map[string]any)
	if first["action"].(map[string]any)["id"] != "lcm:join/survival" || !strings.Contains(text(first["label"]), "1 playing") && !strings.Contains(text(first["label"]), "playing") {
		t.Fatalf("first button: %v", first)
	}
	if l := text(second["label"]); !strings.Contains(l, "Creative World") || !strings.Contains(l, "asleep") {
		t.Fatalf("second button label %q", l)
	}
	if !strings.Contains(text(first["tooltip"]), "Fake 1.21") {
		t.Fatalf("tooltip %q lacks the server's version", text(first["tooltip"]))
	}

	p.click("lcm:join/survival")
	cookie := p.transferred()

	// The transferred connection: intent 3 to the same address.
	p2 := e.menuLogin("mc.test", mcproto.IntentTransfer)
	id, body := p2.packet(false)
	if id != mcproto.LoginCookieRequestID || !bytes.Equal(body, mcproto.AppendString(nil, "lcm:route")) {
		t.Fatalf("expected Cookie Request, got %#x %x", id, body)
	}
	resp := append(mcproto.AppendString(nil, "lcm:route"), 1)
	resp = append(mcproto.AppendVarInt(resp, int32(len(cookie))), cookie...)
	p2.c.Write(mcproto.AppendPacket(nil, mcproto.LoginCookieResponseID, resp))
	l := e.nextLogin(e.survival)
	want := mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 773, Host: "mc.test", Port: 25565, Intent: mcproto.IntentLogin})
	want = mcproto.AppendPacket(want, 0x00, loginStart("Alex"))
	if !bytes.Equal(l.Raw, want) || l.Handshake.Intent != mcproto.IntentLogin {
		t.Fatalf("server got %x, want %x", l.Raw, want)
	}

	// Replaying the cookie gets the menu instead.
	p3 := e.menuLogin("mc.test", mcproto.IntentTransfer)
	p3.packet(false)
	p3.c.Write(mcproto.AppendPacket(nil, mcproto.LoginCookieResponseID, resp))
	if d := p3.enterMenu(); d["type"] != "minecraft:multi_action" {
		t.Fatalf("replayed cookie: %v", d)
	}

	m := scrape(t, e.gw)
	if m[`lcm_gateway_menu_total{result="arrived"}`] != 1 || m[`lcm_gateway_menu_transfers_total{backend="survival"}`] != 1 {
		t.Fatalf("metrics: arrived=%v transfers=%v", m[`lcm_gateway_menu_total{result="arrived"}`], m[`lcm_gateway_menu_transfers_total{backend="survival"}`])
	}
}

// Picking an asleep server wakes it; the player sees a notice until the
// relay connects, then is transferred.
func TestMenuWakesAsleepServer(t *testing.T) {
	e := newMenuEnv(t)
	p := e.menuLogin("mc.test", mcproto.IntentLogin)
	p.enterMenu()
	p.click("lcm:join/creative")
	select {
	case b := <-e.woke:
		if b != "creative" {
			t.Fatalf("woke %s", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no wake")
	}
	n := p.dialog()
	if n["type"] != "minecraft:notice" || !strings.Contains(text(n["title"]), "Starting Creative World") {
		t.Fatalf("notice: %v", n)
	}
	if a := n["action"].(map[string]any)["action"].(map[string]any)["id"]; a != "lcm:menu" {
		t.Fatalf("notice button: %v", a)
	}
	// Back to the list and pick it again: no second wake (debounced), still waiting.
	p.click("lcm:menu")
	if d := p.dialog(); !strings.Contains(text(d["actions"].([]any)[1].(map[string]any)["label"]), "starting") {
		t.Fatalf("list after waking: %v", d["actions"])
	}
	p.click("lcm:join/creative")
	p.dialog()
	e.startBackend("creative")
	p.transferred()
}

// A failed wake shows an error and doesn't count toward WakeEvery, so
// picking the server again retries at once.
func TestMenuWakeFailure(t *testing.T) {
	e := newMenuEnv(t)
	err := errors.New("no LivePod")
	e.wakeErr.Store(&err)
	p := e.menuLogin("mc.test", mcproto.IntentLogin)
	p.enterMenu()
	for i := 0; i < 2; i++ {
		p.click("lcm:join/creative")
		select {
		case <-e.woke:
		case <-time.After(2 * time.Second):
			t.Fatalf("attempt %d: no wake", i)
		}
		p.dialog() // "Starting ..."
		if n := p.dialog(); !strings.Contains(text(n["title"]), "Couldn't start Creative World") {
			t.Fatalf("attempt %d: %v", i, n)
		}
	}
	p.click("lcm:menu")
	if d := p.dialog(); !strings.Contains(text(d["actions"].([]any)[1].(map[string]any)["label"]), "asleep") {
		t.Fatalf("after a failed wake the server should show as asleep: %v", d["actions"])
	}
}

func TestMenuQuitAndOldClients(t *testing.T) {
	e := newMenuEnv(t)
	p := e.menuLogin("mc.test", mcproto.IntentLogin)
	p.enterMenu()
	p.click("lcm:quit")
	if id, body := p.packet(true); id != mcproto.ConfigDisconnectID {
		t.Fatalf("expected Disconnect, got %#x %x", id, body)
	}
	p.expectEOF()

	// 1.21.1 can't show dialogs: it gets the addresses to use instead.
	old := e.dial()
	old.c.Write(mcproto.AppendPacket(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "mc.test", Port: 25566, Intent: mcproto.IntentLogin}), 0x00, loginStart("Old")))
	id, body := old.packet(false)
	msg, _ := mcproto.ParseStatusResponse(body) // a JSON string, like the status document
	if id != 0x00 || !strings.Contains(msg, "1.21.6") || !strings.Contains(msg, "survival.mc.test:25566") || !strings.Contains(msg, "Creative World: creative.mc.test:25566") {
		t.Fatalf("old client got %#x %s", id, msg)
	}
	// A named server still works for it.
	direct := e.dial()
	direct.c.Write(mcproto.AppendLoginStart(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "survival.mc.test", Port: 25565, Intent: mcproto.IntentLogin}), "Old"))
	if l := e.nextLogin(e.survival); l.Name != "Old" {
		t.Fatalf("direct login %q", l.Name)
	}
}

func TestMenuStatus(t *testing.T) {
	e := newMenuEnv(t)
	statusExchange(t, e.dial(), 773) // warms the server info cache
	doc := statusExchange(t, e.dial(), 773)
	if !strings.HasPrefix(doc.Description.Text, "Test MC\n1 of 2 servers up") || doc.Version.Protocol != 773 {
		t.Fatalf("menu status: %+v", doc)
	}
	if doc := statusExchange(t, e.dial(), 767); doc.Version.Protocol == 767 {
		t.Fatalf("an old client is shown as compatible: %+v", doc)
	}
}
