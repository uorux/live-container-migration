package mcproto_test

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"lcm/internal/mcproto"
	"lcm/internal/mcproto/mctest"
)

func TestNBTEncoding(t *testing.T) {
	// {"a": 1b, "s": "hé\x00😀", "l": [1, 2], "e": []}
	v := mcproto.Compound{
		mcproto.F("a", mcproto.Byte(1)),
		mcproto.F("s", mcproto.String("hé\x00😀")),
		mcproto.F("l", mcproto.List{mcproto.Int(1), mcproto.Int(2)}),
		mcproto.F("e", mcproto.List{}),
	}
	got := hex.EncodeToString(mcproto.AppendNBT(nil, v))
	want := "0a" + // compound, no root name
		"01" + "0001" + "61" + "01" +
		"08" + "0001" + "73" + "000b" + "68" + "c3a9" + "c080" + "eda0bd" + "edb880" + // modified UTF-8: NUL and a surrogate pair
		"09" + "0001" + "6c" + "03" + "00000002" + "00000001" + "00000002" +
		"09" + "0001" + "65" + "00" + "00000000" +
		"00"
	if got != want {
		t.Fatalf("encoding\n got %s\nwant %s", got, want)
	}
	dec, n, err := mctest.DecodeNBT(mcproto.AppendNBT(nil, v))
	if err != nil || n != len(want)/2 {
		t.Fatalf("decode: %v (%d bytes)", err, n)
	}
	wantDec := map[string]any{"a": int8(1), "s": "hé\x00😀", "l": []any{int32(1), int32(2)}, "e": []any{}}
	if !reflect.DeepEqual(dec, wantDec) {
		t.Fatalf("round trip: %#v", dec)
	}
}

func TestMenuProtocols(t *testing.T) {
	for _, c := range []struct {
		proto                  int32
		ok                     bool
		cookie, transfer, show int32
		sessionID              bool
	}{
		{770, false, 0, 0, 0, false},         // 1.21.5: no dialogs
		{771, true, 0x0a, 0x0b, 0x12, false}, // 1.21.6
		{773, true, 0x0a, 0x0b, 0x12, false}, // 1.21.9/10
		{776, true, 0x0a, 0x0b, 0x12, false}, // 26.2
		{777, true, 0x0b, 0x0c, 0x13, true},  // 26.3
		{778, false, 0, 0, 0, false},
	} {
		p, ok := mcproto.MenuProtocolFor(c.proto)
		if ok != c.ok || p.StoreCookieID != c.cookie || p.TransferID != c.transfer || p.ShowDialogID != c.show || p.LoginSessionID != c.sessionID {
			t.Errorf("protocol %d: got %+v %v", c.proto, p, ok)
		}
	}
}

func TestLoginPackets(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ls := mcproto.AppendString(nil, "Steve")
	ls = append(ls, uuid[:]...)
	got, err := mcproto.ParseLoginStart(ls)
	if err != nil || got.Name != "Steve" || got.UUID != uuid {
		t.Fatalf("ParseLoginStart: %+v %v", got, err)
	}
	if _, err := mcproto.ParseLoginStart(mcproto.AppendString(nil, "Steve")); err == nil {
		t.Fatal("Login Start without UUID accepted")
	}

	p773, _ := mcproto.MenuProtocolFor(773)
	id, body, _, err := mcproto.ParsePacket(p773.LoginSuccess(uuid, "Steve"), 1<<10)
	if err != nil || id != 0x02 || !bytes.Equal(body, append(append(uuid[:], 5, 'S', 't', 'e', 'v', 'e'), 0)) {
		t.Fatalf("Login Success (773): %x %x %v", id, body, err)
	}
	p777, _ := mcproto.MenuProtocolFor(777)
	_, body, _, _ = mcproto.ParsePacket(p777.LoginSuccess(uuid, "Steve"), 1<<10)
	if len(body) != 16+6+1+16 {
		t.Fatalf("Login Success (777) is %d bytes, want a trailing session UUID", len(body))
	}

	id, body, _, _ = mcproto.ParsePacket(mcproto.CookieRequest("lcm:route"), 1<<10)
	if id != 0x05 || !bytes.Equal(body, mcproto.AppendString(nil, "lcm:route")) {
		t.Fatalf("Cookie Request: %x %x", id, body)
	}
	resp := append(mcproto.AppendString(nil, "lcm:route"), 1, 3, 'a', 'b', 'c')
	key, payload, err := mcproto.ParseCookieResponse(resp)
	if err != nil || key != "lcm:route" || string(payload) != "abc" {
		t.Fatalf("Cookie Response: %q %q %v", key, payload, err)
	}
	key, payload, err = mcproto.ParseCookieResponse(append(mcproto.AppendString(nil, "lcm:route"), 0))
	if err != nil || key != "lcm:route" || payload != nil {
		t.Fatalf("absent cookie: %q %q %v", key, payload, err)
	}
}

func TestConfigPackets(t *testing.T) {
	p, _ := mcproto.MenuProtocolFor(773)
	id, body, _, _ := mcproto.ParsePacket(p.StoreCookie("lcm:route", []byte("xy")), 1<<10)
	if id != 0x0a || !bytes.Equal(body, append(mcproto.AppendString(nil, "lcm:route"), 2, 'x', 'y')) {
		t.Fatalf("Store Cookie: %x %x", id, body)
	}
	id, body, _, _ = mcproto.ParsePacket(p.Transfer("mc.example.com", 25565), 1<<10)
	if id != 0x0b || !bytes.Equal(body, mcproto.AppendVarInt(mcproto.AppendString(nil, "mc.example.com"), 25565)) {
		t.Fatalf("Transfer: %x %x", id, body)
	}
	id, body, _, _ = mcproto.ParsePacket(p.ShowDialog(mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:notice"))}), 1<<10)
	if dec, _, err := mctest.DecodeNBT(body); id != 0x12 || err != nil || dec.(map[string]any)["type"] != "minecraft:notice" {
		t.Fatalf("Show Dialog: %x %v %v", id, dec, err)
	}
	id, body, _, _ = mcproto.ParsePacket(mcproto.ConfigDisconnect(mcproto.Text("bye")), 1<<10)
	if dec, _, err := mctest.DecodeNBT(body); id != 0x02 || err != nil || dec != "bye" {
		t.Fatalf("Disconnect: %x %v %v", id, dec, err)
	}
	id, body, _, _ = mcproto.ParsePacket(mcproto.ConfigKeepAlive(42), 1<<10)
	if id != 0x04 || !bytes.Equal(body, []byte{0, 0, 0, 0, 0, 0, 0, 42}) {
		t.Fatalf("Keep Alive: %x %x", id, body)
	}
	// Custom Click Action: the id, then a length-prefixed optional tag.
	click := append(mcproto.AppendString(nil, "lcm:join/atm9"), 1, 0)
	if a, err := mcproto.ParseCustomClick(click); err != nil || a != "lcm:join/atm9" {
		t.Fatalf("Custom Click: %q %v", a, err)
	}
}
