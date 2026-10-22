package proxyproto

import (
	"bufio"
	"bytes"
	"net/netip"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		src, dst string
		wantLen  int
	}{
		{"203.0.113.7:51234", "10.42.1.5:25565", 16 + 12},
		{"[2001:db8::7]:51234", "[2001:db8::1]:25565", 16 + 36},
		{"203.0.113.7:51234", "[2001:db8::1]:25565", 16 + 36}, // mixed -> v6
	}
	for _, c := range cases {
		src, dst := netip.MustParseAddrPort(c.src), netip.MustParseAddrPort(c.dst)
		h := Header(src, dst)
		if len(h) != c.wantLen {
			t.Fatalf("%s: len %d want %d", c.src, len(h), c.wantLen)
		}
		info, err := Read(bufio.NewReader(bytes.NewReader(append(h, 'x'))))
		if err != nil {
			t.Fatal(err)
		}
		if info.Src.Addr().Unmap() != src.Addr().Unmap() || info.Src.Port() != src.Port() ||
			info.Dst.Addr().Unmap() != dst.Addr().Unmap() || info.Dst.Port() != dst.Port() {
			t.Fatalf("got %+v", info)
		}
	}
	// Exact bytes for IPv4 (spec example layout).
	h := Header(netip.MustParseAddrPort("1.2.3.4:256"), netip.MustParseAddrPort("5.6.7.8:25565"))
	want := append(append([]byte{}, Signature...), 0x21, 0x11, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8, 1, 0, 0x63, 0xdd)
	if !bytes.Equal(h, want) {
		t.Fatalf("got % x\nwant % x", h, want)
	}
	// LOCAL when addresses unknown.
	info, err := Read(bufio.NewReader(bytes.NewReader(Header(netip.AddrPort{}, netip.AddrPort{}))))
	if err != nil || !info.Local {
		t.Fatal(info, err)
	}
	if _, err := Read(bufio.NewReader(bytes.NewReader(make([]byte, 16)))); err != ErrInvalid {
		t.Fatal(err)
	}
}
