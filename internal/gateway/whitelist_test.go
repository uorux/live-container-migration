package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"lcm/internal/mcproto"
)

const testUUIDString = "deadbeef-0102-0304-0506-0708090a0b0c" // testUUID

func writeList(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWhitelistMatch(t *testing.T) {
	dir := t.TempDir()
	backend := writeList(t, dir, "backend.json", `[{"uuid":"`+testUUIDString+`","name":"Renamed"}]`)
	ops := writeList(t, dir, "ops.json", `[{"uuid":"00000000-0000-0000-0000-000000000001","name":"Admin","level":4,"bypassesPlayerLimit":false}]`)
	global := writeList(t, dir, "global.json", `[{"uuid":"00000000-0000-0000-0000-000000000002","name":"Friend"}]`)
	w, err := ParseWhitelist("*="+backend+"+"+ops+", sdfs="+global, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	u := testUUID
	other := [16]byte{9}
	for _, c := range []struct {
		backend, name string
		uuid          *[16]byte
		want          bool
	}{
		{"atm10", "Renamed", nil, true},
		{"atm10", "rEnAmEd", nil, true},  // names are case-insensitive
		{"atm10", "NewName", &u, true},   // a rename: the UUID still matches
		{"atm10", "admin", &other, true}, // ops count, like on a vanilla server
		{"atm10", "Friend", nil, false},  // global list is sdfs only
		{"sdfs", "Friend", nil, true},    // ... which doesn't fall back to "*"
		{"sdfs", "Renamed", &u, false},
		{"atm10", "Mallory", &other, false},
	} {
		if got := w.Allowed(c.backend, c.name, c.uuid); got != c.want {
			t.Errorf("Allowed(%s, %s, %v) = %v, want %v", c.backend, c.name, c.uuid != nil, got, c.want)
		}
	}

	// No "*": unlisted backends are open; no whitelist at all allows everyone.
	w2, _ := ParseWhitelist("sdfs="+global, testLogger())
	if !w2.Allowed("atm10", "Anyone", nil) || w2.Allowed("sdfs", "Anyone", nil) {
		t.Error("backend without a list should be open, sdfs closed")
	}
	if w3, err := ParseWhitelist("", nil); w3 != nil || err != nil || !w3.Allowed("x", "y", nil) {
		t.Errorf("empty spec: %v %v", w3, err)
	}

	// Bad specs and unreadable files fail at startup.
	for _, spec := range []string{"atm10", "=x", "atm10=" + filepath.Join(dir, "missing.json"), "atm10=" + writeList(t, dir, "bad.json", "{")} {
		if _, err := ParseWhitelist(spec, testLogger()); err == nil {
			t.Errorf("ParseWhitelist(%q) succeeded", spec)
		}
	}
}

// An edited list takes effect without a restart; a list that becomes
// unreadable allows nobody.
func TestWhitelistReload(t *testing.T) {
	old := listRecheck
	listRecheck = 0
	defer func() { listRecheck = old }()
	dir := t.TempDir()
	p := writeList(t, dir, "wl.json", `[{"name":"A"}]`)
	w, err := ParseWhitelist("*="+p, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if !w.Allowed("x", "a", nil) || w.Allowed("x", "b", nil) {
		t.Fatal("initial list")
	}
	// ConfigMap-style update: a new file renamed over the old one.
	tmp := writeList(t, dir, "wl.json.new", `[{"name":"A"},{"name":"Bee"}]`)
	future := time.Now().Add(time.Minute)
	os.Chtimes(tmp, future, future)
	if err := os.Rename(tmp, p); err != nil {
		t.Fatal(err)
	}
	if !w.Allowed("x", "bee", nil) {
		t.Fatal("update not picked up")
	}
	os.Remove(p)
	if w.Allowed("x", "a", nil) {
		t.Fatal("unreadable list still allows")
	}
}

func TestLoginUUID(t *testing.T) {
	name := mcproto.AppendString(nil, "Alex")
	if u := loginUUID(append(append([]byte{}, name...), testUUID[:]...), len(name)); u == nil || *u != testUUID {
		t.Errorf("1.20.2+: %v", u)
	}
	if u := loginUUID(append(append(append([]byte{}, name...), 1), testUUID[:]...), len(name)); u == nil || *u != testUUID {
		t.Errorf("1.19.3-1.20.1 with UUID: %v", u)
	}
	for _, rest := range [][]byte{nil, {0}, {1, 2, 3}} {
		if u := loginUUID(append(append([]byte{}, name...), rest...), len(name)); u != nil {
			t.Errorf("rest %x: got a UUID", rest)
		}
	}
}

// A login from someone not on the list neither wakes the server nor is held;
// a listed player still wakes it.
func TestWhitelistGatesWake(t *testing.T) {
	dir := t.TempDir()
	wl, err := ParseWhitelist("atm10="+writeList(t, dir, "wl.json", `[{"name":"Alex"}]`), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	woke := make(chan string, 8)
	gw := New(Config{Tunnel: testTunnelCfg(), Logger: testLogger(), Minecraft: &MinecraftConfig{HoldInterval: testInterval, Whitelist: wl},
		Backend: "lobby", Backends: []string{"atm10"}, WakeEvery: time.Millisecond,
		Wake: func(_ context.Context, b string) error { woke <- b; return nil }})
	pl, _ := Listen(context.Background(), "127.0.0.1:0")
	go gw.ServePlayers(pl, gw.DefaultBackend())
	defer pl.Close()
	e := &mcEnv{t: t, gw: gw, playerAddr: pl.Addr().String()}
	login := func(name string) *player {
		p := e.dial()
		p.sent = mcproto.AppendLoginStart(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "atm10.mc.test", Port: 25565, Intent: mcproto.IntentLogin}), name)
		p.c.Write(p.sent)
		return p
	}

	p := login("Mallory")
	p.expect(mcproto.LoginDisconnect(notWhitelisted))
	p.expectEOF()
	select {
	case b := <-woke:
		t.Fatalf("non-whitelisted login woke %s", b)
	case <-time.After(100 * time.Millisecond):
	}
	if n := scrape(t, gw)[`lcm_gateway_player_connections_total{backend="atm10",result="not_whitelisted"}`]; n != 1 {
		t.Errorf("not_whitelisted = %v", n)
	}

	// The lobby has no list: open.
	pLobby := e.dial()
	pLobby.c.Write(mcproto.AppendLoginStart(mcproto.AppendHandshake(nil, mcproto.Handshake{Protocol: 767, Host: "mc.test", Port: 25565, Intent: mcproto.IntentLogin}), "Mallory"))
	ok := login("alex")
	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case b := <-woke:
			got[b] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("wakes: %v", got)
		}
	}
	ok.keepalives(1, testInterval)
}

// In the menu, picking an asleep server you aren't listed for shows a notice
// and wakes nothing; running servers stay reachable.
func TestMenuWhitelist(t *testing.T) {
	e := newMenuEnv(t)
	wl, err := ParseWhitelist("*="+writeList(t, t.TempDir(), "wl.json", `[{"name":"Someone"}]`), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	e.gw.cfg.Minecraft.Whitelist = wl
	p := e.menuLogin("mc.test", mcproto.IntentLogin) // as Alex, not listed
	p.enterMenu()
	p.click("lcm:join/creative")
	if n := p.dialog(); n["type"] != "minecraft:notice" || text(n["title"]) != "Not whitelisted" {
		t.Fatalf("notice: %v", n)
	}
	select {
	case b := <-e.woke:
		t.Fatalf("woke %s", b)
	case <-time.After(100 * time.Millisecond):
	}
	p.click("lcm:menu")
	p.dialog()
	p.click("lcm:join/survival") // running: the server decides
	p.transferred()
}

// The whitelist only stops unlisted players from waking an asleep server. A
// server that is up but whose relay is briefly away (a migration's quiesce,
// a reconnect) holds them like anyone else, without a wake; once the relay
// says its server exited, they are refused again.
func TestWhitelistHoldsWhileAway(t *testing.T) {
	wl, err := ParseWhitelist("default="+writeList(t, t.TempDir(), "wl.json", `[{"name":"Alex"}]`), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	woke := make(chan string, 8)
	e := newEnv(t, Config{Tunnel: testTunnelCfg(), Logger: testLogger(), WakeEvery: time.Millisecond,
		Minecraft: &MinecraftConfig{HoldInterval: testInterval, Whitelist: wl},
		Wake:      func(_ context.Context, b string) error { woke <- b; return nil }})
	noWake := func() {
		t.Helper()
		select {
		case b := <-woke:
			t.Fatalf("an unlisted player woke %s", b)
		default:
		}
	}
	srv := e.startServer(true)
	r := e.startRelay(srv.Addr())
	e.waitFor("relay connected", e.session().Connected)

	e.login(774, "Mallory")
	if l := e.nextLogin(srv); l.Name != "Mallory" {
		t.Fatalf("server got %q", l.Name)
	}

	// A migration quiesces the relay: the server is up, just unreachable for a moment.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	e.waitFor("tunnel down", func() bool { return !e.session().Connected() })
	p := e.login(774, "Mallory")
	p.keepalives(2, testInterval)
	noWake()
	r.Resume()
	if l := e.nextLogin(srv); l.Name != "Mallory" {
		t.Fatalf("server got %q", l.Name)
	}

	// The server exits (idle stop): asleep now.
	r.Shutdown(time.Second)
	e.waitFor("tunnel down", func() bool { return !e.session().Connected() })
	p = e.login(774, "Mallory")
	p.expect(mcproto.LoginDisconnect(notWhitelisted))
	p.expectEOF()
	noWake()
	p = e.login(774, "Alex")
	p.keepalives(1, testInterval)
	select {
	case <-woke:
	case <-time.After(2 * time.Second):
		t.Fatal("a listed player didn't wake the server")
	}
}

// A relay away longer than AwakeGrace without saying goodbye: asleep.
func TestWhitelistAwakeGrace(t *testing.T) {
	wl, err := ParseWhitelist("default="+writeList(t, t.TempDir(), "wl.json", `[{"name":"Alex"}]`), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, Whitelist: wl, AwakeGrace: 50 * time.Millisecond})
	srv := e.startServer(true)
	r := e.startRelay(srv.Addr())
	e.waitFor("relay connected", e.session().Connected)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if e.gw.asleep(e.gw.DefaultBackend()) {
		t.Fatal("asleep right after the relay went away")
	}
	time.Sleep(100 * time.Millisecond)
	p := e.login(774, "Mallory")
	p.expect(mcproto.LoginDisconnect(notWhitelisted))
}
