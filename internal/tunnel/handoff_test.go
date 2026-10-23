package tunnel

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"lcm/internal/wire"
)

// TestHandoffResume runs echo traffic through a player, the gateway session
// and the relay, and hands the gateway session to a new Session several
// times mid-stream (Freeze, Import, the relay reconnects): both byte streams
// must arrive exactly. The player socket itself stays (moving it is
// tcprepair's job); what moves is every byte of session state around it.
func TestHandoffResume(t *testing.T) {
	cfg := testCfg()
	p := newSessionPair(t, cfg)
	player, gwLocal := tcpPair(t)
	p.gw.AddStream(gwLocal, wire.Open{})
	up := <-p.upstreams

	const n = 8 << 20
	src := make([]byte, n)
	for i := range src {
		src[i] = byte(i*13 + i>>11)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for off := 0; off < n; off += 32 << 10 {
			player.Write(src[off : off+32<<10])
			time.Sleep(500 * time.Microsecond)
		}
		player.CloseWrite()
	}()
	go func() { defer wg.Done(); io.Copy(up, up); up.(*net.TCPConn).CloseWrite() }()
	gotc := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(player); gotc <- b }()

	rng := rand.New(rand.NewPCG(7, 8))
	handoffs := 0
	for i := 0; i < 5; i++ {
		time.Sleep(time.Duration(20+rng.IntN(60)) * time.Millisecond)
		fz, err := p.gw.Freeze(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		p.wg.Wait() // both tunnel ends noticed and detached
		if len(fz.State.Streams) == 1 {
			handoffs++
		} else if fz.Dropped > 0 {
			t.Fatalf("stream dropped at handoff %d", i)
		}
		locals := map[uint32]net.Conn{}
		for id, c := range fz.Locals {
			locals[id] = c
		}
		p.gw = Import(cfg, fz.State, locals, nil)
		p.connect()
	}
	if handoffs < 4 {
		t.Fatalf("only %d handoffs happened mid-stream", handoffs)
	}
	select {
	case got := <-gotc:
		if !bytes.Equal(got, src) {
			t.Fatalf("echo mismatch: got %d of %d bytes", len(got), n)
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
	wg.Wait()
	p.drop()
}

// TestHandoffPending: bytes passed as pending reach the player before the
// stream's own queued bytes, and don't disturb the offsets.
func TestHandoffPending(t *testing.T) {
	cfg := testCfg()
	p := newSessionPair(t, cfg)
	player, gwLocal := tcpPair(t)
	p.gw.AddStream(gwLocal, wire.Open{})
	up := <-p.upstreams
	if _, err := up.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	player.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(player, buf); err != nil || string(buf) != "one" {
		t.Fatalf("one: %q %v", buf, err)
	}
	fz, err := p.gw.Freeze(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p.wg.Wait()
	locals := map[uint32]net.Conn{}
	pending := map[uint32][]byte{}
	for id, c := range fz.Locals {
		locals[id] = c
		pending[id] = []byte("[tail]")
	}
	p.gw = Import(cfg, fz.State, locals, pending)
	p.connect()
	if _, err := up.Write([]byte("two")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("[tail]two"))
	if _, err := io.ReadFull(player, got); err != nil || string(got) != "[tail]two" {
		t.Fatalf("after import: %q %v", got, err)
	}
	if _, err := player.Write([]byte("back")); err != nil {
		t.Fatal(err)
	}
	up.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(up, buf[:3]); err != nil || string(buf[:3]) != "bac" {
		t.Fatalf("player -> server after import: %q %v", buf, err)
	}
	player.Close()
	up.Close()
	p.drop()
}
