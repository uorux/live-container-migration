package relay

import (
	"bufio"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"lcm/internal/tunnel"
)

func TestParseProcAddr(t *testing.T) {
	cases := map[string]string{
		"0100007F:1F90":                         "127.0.0.1:8080",
		"0A2A0105:6DDD":                         "5.1.42.10:28125",
		"00000000000000000000000001000000:0050": "[::1]:80",
		"0000000000000000FFFF00000100007F:0050": "[::ffff:127.0.0.1]:80",
	}
	for in, want := range cases {
		got, err := parseProcAddr(in)
		if err != nil || got != netip.MustParseAddrPort(want) {
			t.Errorf("%s -> %v %v, want %s", in, got, err, want)
		}
	}
}

func TestNonLoopbackSockets(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	socks, err := NonLoopbackSockets()
	if err != nil {
		t.Skip("no /proc:", err)
	}
	for _, s := range socks {
		if strings.Contains(s, ln.Addr().String()) {
			t.Fatalf("loopback socket reported: %v", socks)
		}
	}
	// A UDP socket "connected" to a non-loopback address must be reported.
	u, err := net.Dial("udp", "192.0.2.1:9")
	if err != nil {
		t.Skip("no route for test address:", err)
	}
	defer u.Close()
	socks, _ = NonLoopbackSockets()
	found := false
	for _, s := range socks {
		found = found || strings.Contains(s, "192.0.2.1:9")
	}
	if !found {
		t.Fatalf("non-loopback socket not reported: %v", socks)
	}
}

// Quiesce/resume while the gateway is unreachable (no tunnel ever): the
// control protocol must still answer and the relay must not be dialing.
func TestControlWithoutGateway(t *testing.T) {
	ln0, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln0.Addr().String()
	ln0.Close() // nothing listens here
	r := New(Config{Gateway: addr, Tunnel: tunnel.Config{Token: []byte("x")},
		BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, CheckSockets: true})
	path := filepath.Join(t.TempDir(), "c.sock")
	ln, err := ListenControl(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go r.ServeControl(ln)
	ctx := t.Context()
	go r.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	c.Write([]byte("status\nbogus\nquiesce\n"))
	for _, want := range []string{`{"session":`, "error unknown command", "ok"} {
		line, err := br.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, want) {
			t.Fatalf("got %q %v, want prefix %q", line, err, want)
		}
	}
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("control connection not closed after quiesce")
	}
	c.Close()
	if st := r.Status(); st.State != "quiesced" {
		t.Fatalf("state %q", st.State)
	}
	c, _ = net.Dial("unix", path)
	defer c.Close()
	c.Write([]byte("resume\n"))
	if line, _ := bufio.NewReader(c).ReadString('\n'); line != "ok\n" {
		t.Fatalf("resume: %q", line)
	}
	time.Sleep(50 * time.Millisecond)
	if st := r.Status(); st.State != "connecting" || st.LastError == "" {
		t.Fatalf("status after resume %+v", st)
	}
}
