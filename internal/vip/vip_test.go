package vip

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"lcm/internal/tcprepair"
)

// These tests need CAP_NET_ADMIN in their own network namespace: run the
// test binary under `unshare -rn` with LCM_NETNS_TEST=1 (they skip otherwise).
func setup(t *testing.T) {
	t.Helper()
	if os.Getenv("LCM_NETNS_TEST") == "" {
		t.Skip("set LCM_NETNS_TEST=1 and run under unshare -rn")
	}
	if _, err := net.InterfaceByName("lan0"); err == nil {
		return
	}
	// A dummy "LAN" NIC for the ipvlans to sit on.
	if err := createLink("lan0", "dummy"); err != nil {
		t.Skipf("can't create a dummy NIC (%v): run under unshare -rn", err)
	}
	if err := setLinkUp("lan0", true); err != nil {
		t.Fatal(err)
	}
	if err := setLinkUp("lo", true); err != nil {
		t.Fatal(err)
	}
}

var (
	vipAddr    = netip.MustParsePrefix("10.9.0.5/24")
	clientAddr = netip.MustParsePrefix("10.9.0.6/24")
	gw         = netip.MustParseAddr("10.9.0.1")
)

func dialFrom(t *testing.T, n *NS, to netip.AddrPort, timeout time.Duration) (net.Conn, error) {
	var c net.Conn
	err := n.Do(func() error {
		var err error
		c, err = net.DialTimeout("tcp4", to.String(), timeout)
		return err
	})
	return c, err
}

func TestVIPLifecycle(t *testing.T) {
	setup(t)
	a, err := Create("lan0", vipAddr, gw)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := net.InterfaceByName(IfName); err == nil {
		t.Fatal("vip0 is visible in the node's namespace")
	}
	ln, err := a.Listen(8080)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := a.Up(); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	client, err := Create("lan0", clientAddr, netip.Addr{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Up(); err != nil {
		t.Fatal(err)
	}
	c, err := dialFrom(t, client, netip.AddrPortFrom(vipAddr.Addr(), 8080), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Write([]byte("ping"))
	buf := make([]byte, 4)
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo: %q %v", buf, err)
	}
	c.Close()

	// Down: the VIP goes silent (a timeout, not a refusal).
	if err := a.Down(); err != nil {
		t.Fatal(err)
	}
	_, err = dialFrom(t, client, netip.AddrPortFrom(vipAddr.Addr(), 8080), time.Second)
	var ne net.Error
	if err == nil || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("dial to a down VIP: want a timeout, got %v", err)
	}
	if err := a.Up(); err != nil {
		t.Fatal(err)
	}
	if c, err := dialFrom(t, client, netip.AddrPortFrom(vipAddr.Addr(), 8080), 3*time.Second); err != nil {
		t.Fatalf("after Up again: %v", err)
	} else {
		c.Close()
	}
}

// TestHandoffAcrossNamespaces moves a live connection from one VIP namespace
// to another that takes the VIP over: what two gateways on two nodes do,
// here on one dummy NIC (so A must be gone before B can claim the address).
func TestHandoffAcrossNamespaces(t *testing.T) {
	setup(t)
	// Addresses of its own: TestVIPLifecycle's namespaces die asynchronously.
	vipAddr := netip.MustParsePrefix("10.9.0.15/24")
	clientAddr := netip.MustParsePrefix("10.9.0.16/24")
	a, err := Create("lan0", vipAddr, gw)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := a.Listen(25565)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Up(); err != nil {
		t.Fatal(err)
	}
	client, err := Create("lan0", clientAddr, netip.Addr{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Up()

	accepted := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accepted <- c }()
	player, err := dialFrom(t, client, netip.AddrPortFrom(vipAddr.Addr(), 25565), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer player.Close()
	srv := <-accepted
	srv.Write([]byte("hello from A"))
	buf := make([]byte, 12)
	player.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(player, buf); err != nil {
		t.Fatal(err)
	}

	// The player keeps sending through the move.
	sent := make(chan []byte, 1)
	go func() {
		var all []byte
		for i := 0; i < 50; i++ {
			p := bytes.Repeat([]byte{byte('a' + i%26)}, 1000)
			if _, err := player.Write(p); err != nil {
				break
			}
			all = append(all, p...)
			time.Sleep(10 * time.Millisecond)
		}
		sent <- all
	}()
	time.Sleep(100 * time.Millisecond)

	// A: silence the VIP, dump, close quietly, go away.
	start := time.Now()
	if err := a.Down(); err != nil {
		t.Fatal(err)
	}
	st, err := tcprepair.DumpConn(srv.(*net.TCPConn))
	if err != nil {
		t.Fatal(err)
	}
	srv.Close()
	ln.Close()
	a.Close()

	// B: claim the VIP (the dying namespace may hold it for a moment),
	// restore, come up.
	var b *NS
	for i := 0; ; i++ {
		b, err = Create("lan0", vipAddr, gw)
		if err == nil {
			break
		}
		if i > 50 {
			t.Fatalf("B can't claim the VIP: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer b.Close()
	var moved net.Conn
	var pending []byte
	err = b.Do(func() error {
		fd, p, err := tcprepair.Restore(st)
		if err != nil {
			return err
		}
		pending = p
		moved, err = tcprepair.Conn(fd, "moved")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer moved.Close()
	if err := b.Up(); err != nil {
		t.Fatal(err)
	}
	t.Logf("handoff took %v (%d bytes queued toward A, %d pending)", time.Since(start), len(st.RecvQueue), len(pending))
	if len(pending) > 0 {
		moved.Write(pending)
	}

	// Everything the player sent arrives, in order, then B answers.
	want := <-sent
	got := make([]byte, len(want))
	moved.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(moved, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("player bytes after the move: %v (equal=%v)", err, bytes.Equal(got, want))
	}
	moved.Write([]byte("hello from B"))
	if _, err := io.ReadFull(player, buf); err != nil || string(buf) != "hello from B" {
		t.Fatalf("B's reply: %q %v", buf, err)
	}
}
