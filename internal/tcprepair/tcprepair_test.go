package tcprepair

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// needRepair skips unless this process may use TCP_REPAIR (CAP_NET_ADMIN in
// its network namespace: run the tests under `unshare -rn`, see repairtest).
func needRepair(t *testing.T) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 1); err != nil {
		t.Skipf("TCP_REPAIR not permitted (%v); run under unshare -rn", err)
	}
	// A fresh network namespace has lo down.
	if l, err := net.Listen("tcp", "127.0.0.1:0"); err != nil {
		t.Skipf("no loopback: %v", err)
	} else {
		l.Close()
	}
}

func TestMain(m *testing.M) {
	// Under `unshare -rn` the loopback interface starts down.
	if os.Getenv("LCM_REPAIRTEST_LO") != "" {
		if fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0); err == nil {
			ifr, _ := unix.NewIfreq("lo")
			if unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, ifr) == nil {
				ifr.SetUint16(ifr.Uint16() | unix.IFF_UP)
				unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, ifr)
			}
			unix.Close(fd)
		}
	}
	os.Exit(m.Run())
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

// TestMoveConnection moves the server side of a loopback connection to a new
// socket while data is queued both ways: the client's bytes the server hasn't
// read, and the server's bytes the client's small window hasn't let through.
func TestMoveConnection(t *testing.T) {
	needRepair(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	d := net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		return rc.Control(func(fd uintptr) { unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, 4096) })
	}}
	client, err := d.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	srv, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// A round trip, then client bytes the server leaves unread.
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(srv, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("hello: %q %v", buf, err)
	}
	unread := pattern(3000, 1)
	if _, err := client.Write(unread); err != nil {
		t.Fatal(err)
	}
	// Server bytes stuck behind the client's window.
	big := pattern(32<<20, 2)
	wrote := 0
	srv.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	for wrote < len(big) {
		n, err := srv.Write(big[wrote:min(wrote+64<<10, len(big))])
		wrote += n
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if wrote == len(big) {
		t.Fatal("the window never filled")
	}
	time.Sleep(50 * time.Millisecond)

	st, err := DumpConn(srv.(*net.TCPConn))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dumped %v", st)
	if len(st.RecvQueue) != len(unread) || len(st.SendQueue) == 0 {
		t.Fatalf("queues: recv %d (want %d), send %d", len(st.RecvQueue), len(unread), len(st.SendQueue))
	}
	srv.Close() // in repair mode: no FIN, no RST

	// The client reads all along, as a player's client does.
	type result struct {
		b   []byte
		err error
	}
	readDone := make(chan result, 1)
	go func() {
		client.SetReadDeadline(time.Now().Add(20 * time.Second))
		b := make([]byte, wrote)
		_, err := io.ReadFull(client, b)
		readDone <- result{b, err}
	}()

	fd, pending, err := Restore(st)
	if err != nil {
		t.Fatal(err)
	}
	moved, err := Conn(fd, "restored")
	if err != nil {
		t.Fatal(err)
	}
	defer moved.Close()
	if len(pending) != st.Unsent {
		t.Fatalf("pending %d, unsent %d", len(pending), st.Unsent)
	}
	moved.SetWriteDeadline(time.Now().Add(20 * time.Second))
	if _, err := moved.Write(pending); err != nil {
		t.Fatalf("writing the unsent tail: %v", err)
	}
	moved.SetWriteDeadline(time.Time{})

	// Everything the old socket accepted reaches the client, in order.
	r := <-readDone
	if r.err != nil || !bytes.Equal(r.b, big[:wrote]) {
		t.Fatalf("client read of %d bytes: %v (equal=%v)", wrote, r.err, bytes.Equal(r.b, big[:wrote]))
	}
	// The unread client bytes are there to read.
	got := make([]byte, len(unread))
	moved.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(moved, got); err != nil || !bytes.Equal(got, unread) {
		t.Fatalf("unread bytes after restore: %v (equal=%v)", err, bytes.Equal(got, unread))
	}
	// And the connection keeps working both ways.
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := moved.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(client, buf); err != nil || string(buf) != "after" {
		t.Fatalf("after: %q %v", buf, err)
	}
	if _, err := client.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(moved, buf); err != nil || string(buf) != "again" {
		t.Fatalf("again: %q %v", buf, err)
	}
}

// TestDumpRefusesNonEstablished: a socket that is closing isn't moved.
func TestDumpRefusesNonEstablished(t *testing.T) {
	needRepair(t)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	s, _ := ln.Accept()
	c.Close()
	time.Sleep(50 * time.Millisecond) // server side is now CLOSE_WAIT
	if _, err := DumpConn(s.(*net.TCPConn)); err == nil {
		t.Fatal("dumped a CLOSE_WAIT socket")
	}
	// Still usable as a normal socket (repair mode was never left on).
	if _, err := s.Write([]byte("x")); err != nil {
		t.Logf("write after refused dump: %v (fine: peer is gone)", err)
	}
	s.Close()
}
