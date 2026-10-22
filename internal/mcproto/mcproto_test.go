package mcproto

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf16"
)

func TestVarInt(t *testing.T) {
	// Vectors from the protocol documentation.
	cases := []struct {
		v   int32
		hex string
	}{
		{0, "00"}, {1, "01"}, {2, "02"}, {127, "7f"}, {128, "8001"}, {255, "ff01"},
		{25565, "ddc701"}, {2097151, "ffff7f"}, {2147483647, "ffffffff07"},
		{-1, "ffffffff0f"}, {-2147483648, "8080808008"},
	}
	for _, c := range cases {
		got := hex.EncodeToString(AppendVarInt(nil, c.v))
		if got != c.hex {
			t.Errorf("AppendVarInt(%d) = %s, want %s", c.v, got, c.hex)
		}
		b, _ := hex.DecodeString(c.hex)
		v, n, err := ReadVarInt(append(b, 0xaa))
		if err != nil || v != c.v || n != len(b) {
			t.Errorf("ReadVarInt(%s) = %d,%d,%v", c.hex, v, n, err)
		}
		if VarIntLen(c.v) != len(b) {
			t.Errorf("VarIntLen(%d) = %d", c.v, VarIntLen(c.v))
		}
		for i := 0; i < len(b); i++ {
			if _, _, err := ReadVarInt(b[:i]); !errors.Is(err, ErrShort) {
				t.Errorf("ReadVarInt(%x) err = %v, want ErrShort", b[:i], err)
			}
		}
	}
	for _, bad := range []string{"ffffffff1f", "ffffffff8f", "8080808080"} {
		b, _ := hex.DecodeString(bad)
		if _, _, err := ReadVarInt(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("ReadVarInt(%s) err = %v, want ErrMalformed", bad, err)
		}
	}
}

func TestSetCompressionNoopFraming(t *testing.T) {
	want := AppendPacket(nil, 0x03, AppendVarInt(nil, -1))
	if !bytes.Equal(SetCompressionNoop, want) {
		t.Fatalf("SetCompressionNoop = %x, framed = %x", SetCompressionNoop, want)
	}
	if hex.EncodeToString(SetCompressionNoop) != "0603ffffffff0f" {
		t.Fatalf("got %x", SetCompressionNoop)
	}
	id, body, total, err := ParsePacket(SetCompressionNoop, 64)
	if err != nil || id != 3 || total != 7 {
		t.Fatalf("parse: id=%d total=%d err=%v", id, total, err)
	}
	if v, n, _ := ReadVarInt(body); v != -1 || n != len(body) {
		t.Fatalf("threshold %d", v)
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	h := Handshake{Protocol: 774, Host: "mc.example.org\x00FML3\x00", Port: 25565, Intent: IntentLogin}
	pkt := AppendHandshake(nil, h)
	id, body, total, err := ParsePacket(pkt, 1024)
	if err != nil || id != 0 || total != len(pkt) {
		t.Fatalf("parse packet: %v", err)
	}
	got, err := ParseHandshake(body)
	if err != nil || got != h {
		t.Fatalf("got %+v %v", got, err)
	}
	if CleanHost(got.Host) != "mc.example.org" {
		t.Fatalf("clean host %q", CleanHost(got.Host))
	}
	// A known capture: 1.21.11 to localhost:25565, status.
	capture, _ := hex.DecodeString("1000860609" + hex.EncodeToString([]byte("localhost")) + "63dd01")
	id, body, _, err = ParsePacket(capture, 1024)
	if err != nil || id != 0 {
		t.Fatal(err)
	}
	got, err = ParseHandshake(body)
	want := Handshake{Protocol: 774, Host: "localhost", Port: 25565, Intent: IntentStatus}
	if err != nil || got != want {
		t.Fatalf("capture: %+v %v", got, err)
	}
	// Truncated or trailing bytes are malformed.
	if _, err := ParseHandshake(body[:len(body)-1]); err == nil {
		t.Fatal("truncated handshake parsed")
	}
	if _, err := ParseHandshake(append(body, 0)); err == nil {
		t.Fatal("handshake with trailing byte parsed")
	}
}

func TestParsePacketLimits(t *testing.T) {
	if _, _, _, err := ParsePacket([]byte{0x00}, 100); !errors.Is(err, ErrMalformed) {
		t.Fatalf("zero length: %v", err)
	}
	if _, _, _, err := ParsePacket([]byte{0x65, 0x00}, 100); !errors.Is(err, ErrMalformed) {
		t.Fatalf("over max: %v", err)
	}
	if _, _, _, err := ParsePacket([]byte{0x05, 0x00, 0x01}, 100); !errors.Is(err, ErrShort) {
		t.Fatalf("short: %v", err)
	}
	if _, _, _, err := ParsePacket([]byte{0xff, 0xff, 0xff}, 1<<20); !errors.Is(err, ErrMalformed) {
		t.Fatalf("huge length: %v", err)
	}
	// "GET / HTTP/1.1": length 'G'=71, id 'E'=0x45.
	id, _, _, err := ParsePacket([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"+string(make([]byte, 64))), 1024)
	if err != nil || id == 0 {
		t.Fatalf("http: id=%#x err=%v (must not look like a handshake)", id, err)
	}
}

func TestLoginStartAndDisconnect(t *testing.T) {
	pkt := AppendLoginStart(nil, "Steve")
	id, body, _, err := ParsePacket(pkt, 100)
	if err != nil || id != 0 {
		t.Fatal(err)
	}
	// 1.20.2+ appends a UUID; only the name is read.
	name, err := ParseLoginStartName(append(body, make([]byte, 16)...))
	if err != nil || name != "Steve" {
		t.Fatalf("name %q %v", name, err)
	}

	d := LoginDisconnect(`Bye "now"`)
	id, body, total, err := ParsePacket(d, 1024)
	if err != nil || id != 0 || total != len(d) {
		t.Fatal(err)
	}
	s, err := ParseStatusResponse(body) // same layout: one string
	if err != nil {
		t.Fatal(err)
	}
	var tc struct{ Text string }
	if err := json.Unmarshal([]byte(s), &tc); err != nil || tc.Text != `Bye "now"` {
		t.Fatalf("%s %v", s, err)
	}
}

func TestLegacy(t *testing.T) {
	for _, c := range []struct {
		in   []byte
		want bool
	}{
		{[]byte{0xfe}, true}, {[]byte{0xfe, 0x01}, true}, {[]byte{0xfe, 0x01, 0xfa, 0x00}, true},
		{[]byte{0xfe, 0x01, 0x00}, false}, // modern packet of length 254
		{[]byte{0xfe, 0x02}, false}, {[]byte{0x10, 0x00}, false}, {nil, false},
	} {
		if got := IsLegacyPing(c.in); got != c.want {
			t.Errorf("IsLegacyPing(%x) = %v", c.in, got)
		}
	}
	k := LegacyKick(127, "1.21", "Starting", 0, 20)
	if k[0] != 0xff {
		t.Fatalf("kick id %#x", k[0])
	}
	n := int(k[1])<<8 | int(k[2])
	if len(k) != 3+2*n {
		t.Fatalf("length %d vs %d", n, len(k))
	}
	u := make([]uint16, n)
	for i := range u {
		u[i] = uint16(k[3+2*i])<<8 | uint16(k[4+2*i])
	}
	if s := string(utf16.Decode(u)); s != "§1\x00127\x001.21\x00Starting\x000\x0020" {
		t.Fatalf("kick %q", s)
	}
}

func TestReaderFragmentedAndRaw(t *testing.T) {
	stream := AppendHandshake(nil, Handshake{Protocol: 47, Host: "a", Port: 1, Intent: 2})
	stream = AppendLoginStart(stream, "Alex")
	stream = append(stream, 0xde, 0xad)
	r := NewReader(iotest.OneByteReader(bytes.NewReader(stream)), 1024)
	if _, _, err := r.ReadPacket(); err != nil {
		t.Fatal(err)
	}
	id, body, err := r.ReadPacket()
	if err != nil || id != 0 {
		t.Fatal(err)
	}
	if name, _ := ParseLoginStartName(body); name != "Alex" {
		t.Fatalf("name %q", name)
	}
	if !bytes.Equal(r.Raw(), stream[:len(stream)-2]) {
		t.Fatalf("raw %x", r.Raw())
	}
	if _, _, err := r.ReadPacket(); err != io.ErrUnexpectedEOF {
		t.Fatalf("partial tail: %v", err)
	}
}

func TestQueryStatus(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	a.SetDeadline(time.Now().Add(5 * time.Second))
	b.SetDeadline(time.Now().Add(5 * time.Second))
	go func() {
		rd := NewReader(b, 1024)
		_, body, err := rd.ReadPacket()
		if err != nil {
			return
		}
		h, _ := ParseHandshake(body)
		if id, _, err := rd.ReadPacket(); err != nil || id != 0 || h.Intent != IntentStatus {
			return
		}
		b.Write(StatusResponse(NewStatus("hello", "x", h.Protocol)))
	}()
	j, err := QueryStatus(a, -1, "127.0.0.1", 25565)
	if err != nil {
		t.Fatal(err)
	}
	var d StatusDoc
	if err := json.Unmarshal([]byte(j), &d); err != nil || d.Description.Text != "hello" || d.Version.Protocol != -1 {
		t.Fatalf("%s %v", j, err)
	}
}
