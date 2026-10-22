package wire

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf []byte
	buf = AppendFrame(buf, TPing, 0, PingPayload(42))
	buf = AppendDataFrame(buf, 7, 1<<40, []byte("hello"))
	buf = AppendFrame(buf, TAck, 7, AckPayload(123, AckFin))
	buf = AppendFrame(buf, TClose, 7, ClosePayload(99))
	buf = AppendFrame(buf, TReset, 9, ResetPayload(ResetDialFailed, "nope"))
	buf = AppendFrame(buf, TGoAway, 0, GoAwayPayload(GoAwayFinal, "bye"))
	r := NewReader(bytes.NewReader(buf))

	f, err := r.ReadFrame()
	if err != nil || f.Type != TPing {
		t.Fatal(f, err)
	}
	if v, _ := ParsePing(f.Payload); v != 42 {
		t.Fatal(v)
	}
	f, _ = r.ReadFrame()
	off, data, err := ParseData(f.Payload)
	if f.Type != TData || f.Stream != 7 || off != 1<<40 || string(data) != "hello" || err != nil {
		t.Fatal(f, off, data, err)
	}
	f, _ = r.ReadFrame()
	if a, fl, err := ParseAck(f.Payload); a != 123 || fl != AckFin || err != nil {
		t.Fatal(a, fl, err)
	}
	f, _ = r.ReadFrame()
	if c, err := ParseClose(f.Payload); c != 99 || err != nil {
		t.Fatal(c, err)
	}
	f, _ = r.ReadFrame()
	if code, msg, err := ParseReset(f.Payload); f.Stream != 9 || code != ResetDialFailed || msg != "nope" || err != nil {
		t.Fatal(code, msg, err)
	}
	f, _ = r.ReadFrame()
	if fl, msg, err := ParseGoAway(f.Payload); fl != GoAwayFinal || msg != "bye" || err != nil {
		t.Fatal(fl, msg, err)
	}
	if _, err := r.ReadFrame(); err != io.EOF {
		t.Fatal("want EOF, got", err)
	}
}

func TestReadFrameErrors(t *testing.T) {
	// Oversized length.
	hdr := []byte{byte(TData), 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff}
	if _, err := NewReader(bytes.NewReader(hdr)).ReadFrame(); !errors.Is(err, ErrProtocol) {
		t.Fatal("want protocol error, got", err)
	}
	// Truncated payload.
	b := AppendFrame(nil, TPing, 0, PingPayload(1))
	if _, err := NewReader(bytes.NewReader(b[:len(b)-1])).ReadFrame(); err != io.ErrUnexpectedEOF {
		t.Fatal("want unexpected EOF, got", err)
	}
	// Trailing junk in a fixed-size payload.
	if _, err := ParseClose(append(ClosePayload(1), 0)); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if _, _, err := ParseData([]byte{1, 2}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestHelloAuth(t *testing.T) {
	tok := []byte("secret")
	var nonce [NonceLen]byte
	nonce[0] = 1
	h := Hello{Version: Version, Session: [16]byte{1, 2, 3}, Backend: "mc", Window: 1 << 20, MaxSeen: 17,
		Streams: []StreamState{{ID: 3, RxOffset: 1000, Flags: EntryFin}, {ID: 17, RxOffset: 5}}}
	p := h.Marshal(tok, nonce)
	got, err := ParseHello(p, tok, nonce)
	if err != nil || !reflect.DeepEqual(got, h) {
		t.Fatalf("got %+v err %v", got, err)
	}
	if _, err := ParseHello(p, []byte("wrong"), nonce); !errors.Is(err, ErrAuth) {
		t.Fatal("wrong token accepted", err)
	}
	var other [NonceLen]byte
	if _, err := ParseHello(p, tok, other); !errors.Is(err, ErrAuth) {
		t.Fatal("replay with another nonce accepted", err)
	}
	p[5] ^= 1
	if _, err := ParseHello(p, tok, nonce); !errors.Is(err, ErrAuth) {
		t.Fatal("tampered hello accepted", err)
	}

	a := HelloAck{Status: AckResumed, Window: 65536, Message: "hi", Streams: []StreamState{{ID: 3, RxOffset: 77}}}
	ap := a.Marshal(tok, nonce, h.Session)
	gotA, err := ParseHelloAck(ap, tok, nonce, h.Session)
	if err != nil || !reflect.DeepEqual(gotA, a) {
		t.Fatalf("got %+v err %v", gotA, err)
	}
	if _, err := ParseHelloAck(ap, tok, nonce, [16]byte{9}); !errors.Is(err, ErrAuth) {
		t.Fatal("ack for another session accepted", err)
	}
}

func TestOpenAndChallenge(t *testing.T) {
	for _, o := range []Open{
		{Src: netip.MustParseAddrPort("192.0.2.1:5555"), Dst: netip.MustParseAddrPort("10.0.0.1:25565")},
		{Src: netip.MustParseAddrPort("[2001:db8::1]:5555"), Dst: netip.MustParseAddrPort("[2001:db8::2]:25565")},
		{},
	} {
		got, err := ParseOpen(o.Append(nil))
		if err != nil || got != o {
			t.Fatalf("open %+v -> %+v %v", o, got, err)
		}
	}
	c := Challenge{Version: Version}
	c.Nonce[31] = 9
	got, err := ParseChallenge(c.Append(nil))
	if err != nil || got != c {
		t.Fatal(got, err)
	}
	bad := c.Append(nil)
	bad[0] = 'X'
	if _, err := ParseChallenge(bad); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestEntriesCountBounded(t *testing.T) {
	// A count that claims more entries than bytes present must not allocate.
	b := []byte{0xff, 0xff, 0xff, 0xff}
	d := dec{b: b}
	if d.entries(); !errors.Is(d.err, ErrProtocol) {
		t.Fatal(d.err)
	}
}
