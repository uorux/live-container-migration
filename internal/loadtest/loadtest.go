// Package loadtest provides fake players that push deterministic random data
// and verify the echo, and an echo server that understands PROXY v2. Used by
// the integration tests and by cmd/lcm-loadtest for lab runs.
package loadtest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"lcm/internal/proxyproto"
)

// Stream returns a deterministic pseudo-random byte source for seed.
func Stream(seed uint64) io.Reader {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	return rand.NewChaCha8(key)
}

// PlayerConfig configures one fake player.
type PlayerConfig struct {
	Addr string
	Seed uint64
	// Bytes to send; the echo must return exactly these bytes.
	Bytes int64
	// MaxChunk bounds each write (random size 1..MaxChunk). Default 16 KiB.
	MaxChunk int
	// Rate limits sending in bytes/second (0 = unlimited).
	Rate int64
	// HalfClose: after sending, CloseWrite and expect EOF after the full echo.
	HalfClose bool
	// ExpectEOFAfter: if >= 0, expect the server to close after echoing exactly
	// this many bytes (the player stops checking at that point).
	ExpectEOFAfter int64
	// Progress is incremented (atomically by the caller's choice) as bytes are verified.
	OnVerified func(n int)
}

// PlayerResult reports a player run.
type PlayerResult struct {
	Sent, Verified int64
	SHA256         [32]byte
	Duration       time.Duration
	LocalAddr      netip.AddrPort
}

// RunPlayer connects, sends, and verifies the echo. ctx cancels it.
func RunPlayer(ctx context.Context, cfg PlayerConfig) (PlayerResult, error) {
	var d net.Dialer
	d.SetMultipathTCP(false)
	c, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return PlayerResult{}, err
	}
	return RunPlayerConn(ctx, c.(*net.TCPConn), cfg)
}

// RunPlayerConn is RunPlayer on an existing connection.
func RunPlayerConn(ctx context.Context, c *net.TCPConn, cfg PlayerConfig) (PlayerResult, error) {
	defer c.Close()
	if cfg.MaxChunk <= 0 {
		cfg.MaxChunk = 16 << 10
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	start := time.Now()
	res := PlayerResult{LocalAddr: c.LocalAddr().(*net.TCPAddr).AddrPort()}

	var wg sync.WaitGroup
	var sendErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		src := Stream(cfg.Seed)
		rng := rand.New(rand.NewPCG(cfg.Seed, 99))
		buf := make([]byte, cfg.MaxChunk)
		var sent int64
		for sent < cfg.Bytes {
			n := min(int64(1+rng.IntN(cfg.MaxChunk)), cfg.Bytes-sent)
			io.ReadFull(src, buf[:n])
			if _, err := c.Write(buf[:n]); err != nil {
				if cfg.ExpectEOFAfter < 0 {
					sendErr = fmt.Errorf("write after %d bytes: %w", sent, err)
				}
				break
			}
			sent += n
			if cfg.Rate > 0 {
				want := time.Duration(float64(sent) / float64(cfg.Rate) * float64(time.Second))
				if el := time.Since(start); el < want {
					time.Sleep(want - el)
				}
			}
		}
		res.Sent = sent
		if cfg.HalfClose && sendErr == nil && cfg.ExpectEOFAfter < 0 {
			c.CloseWrite()
		}
	}()

	expect := cfg.Bytes
	if cfg.ExpectEOFAfter >= 0 {
		expect = cfg.ExpectEOFAfter
	}
	ref := Stream(cfg.Seed)
	h := sha256.New()
	buf := make([]byte, 32<<10)
	want := make([]byte, 32<<10)
	var verr error
	for res.Verified < expect {
		n, err := c.Read(buf[:min(int64(len(buf)), expect-res.Verified)])
		if n > 0 {
			io.ReadFull(ref, want[:n])
			if !bytes.Equal(buf[:n], want[:n]) {
				i := 0
				for buf[i] == want[i] {
					i++
				}
				verr = fmt.Errorf("echo mismatch at offset %d", res.Verified+int64(i))
				break
			}
			h.Write(buf[:n])
			res.Verified += int64(n)
			if cfg.OnVerified != nil {
				cfg.OnVerified(n)
			}
		}
		if err != nil {
			verr = fmt.Errorf("read after %d/%d verified bytes: %w", res.Verified, expect, err)
			break
		}
	}
	if verr == nil && (cfg.HalfClose || cfg.ExpectEOFAfter >= 0) {
		// The far side must close now.
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := c.Read(buf[:1])
		// A server that closes while the player is still sending makes its
		// kernel answer with RST, so a reset is as good as EOF here.
		if n > 0 || !(errors.Is(err, io.EOF) || (cfg.ExpectEOFAfter >= 0 && errors.Is(err, syscall.ECONNRESET))) {
			verr = fmt.Errorf("expected EOF after %d bytes, got n=%d err=%v", expect, n, err)
		}
	}
	if verr != nil {
		c.Close() // unblock the sender
	}
	wg.Wait()
	copy(res.SHA256[:], h.Sum(nil))
	res.Duration = time.Since(start)
	if verr != nil {
		return res, verr
	}
	return res, sendErr
}

// EchoServer echoes every connection, optionally consuming a PROXY v2 header first.
type EchoServer struct {
	Listener      net.Listener
	ProxyProtocol bool
	// CloseAfter > 0 ends each connection after echoing that many bytes, the
	// way a well-behaved server does: half-close, drain input, close.
	CloseAfter int64
	// AbortAfter > 0 resets each connection (RST) after echoing that many bytes.
	AbortAfter int64

	mu      sync.Mutex
	headers []proxyproto.Info
	conns   map[net.Conn]bool
	errs    []error
}

// NewEchoServer listens on addr (plain TCP).
func NewEchoServer(addr string, proxyProtocol bool) (*EchoServer, error) {
	var lc net.ListenConfig
	lc.SetMultipathTCP(false)
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &EchoServer{Listener: ln, ProxyProtocol: proxyProtocol, conns: map[net.Conn]bool{}}, nil
}

// Addr returns the listen address.
func (e *EchoServer) Addr() string { return e.Listener.Addr().String() }

// Serve accepts until the listener is closed.
func (e *EchoServer) Serve() {
	for {
		c, err := e.Listener.Accept()
		if err != nil {
			return
		}
		go e.handle(c)
	}
}

// Close stops listening and closes all connections.
func (e *EchoServer) Close() {
	e.Listener.Close()
	e.mu.Lock()
	for c := range e.conns {
		c.Close()
	}
	e.mu.Unlock()
}

// Headers returns the PROXY headers received so far.
func (e *EchoServer) Headers() []proxyproto.Info {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]proxyproto.Info{}, e.headers...)
}

// Errors returns connection-level errors observed.
func (e *EchoServer) Errors() []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]error{}, e.errs...)
}

// NumConns returns the number of open connections.
func (e *EchoServer) NumConns() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.conns)
}

func (e *EchoServer) handle(c net.Conn) {
	e.mu.Lock()
	e.conns[c] = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.conns, c)
		e.mu.Unlock()
		c.Close()
	}()
	br := bufio.NewReader(c)
	if e.ProxyProtocol {
		info, err := proxyproto.Read(br)
		if err != nil {
			e.mu.Lock()
			e.errs = append(e.errs, fmt.Errorf("proxy header: %w", err))
			e.mu.Unlock()
			return
		}
		e.mu.Lock()
		e.headers = append(e.headers, info)
		e.mu.Unlock()
	}
	var r io.Reader = br
	if e.CloseAfter > 0 {
		r = io.LimitReader(br, e.CloseAfter)
	} else if e.AbortAfter > 0 {
		r = io.LimitReader(br, e.AbortAfter)
	}
	if _, err := io.Copy(c, r); err != nil {
		return
	}
	if e.AbortAfter > 0 {
		c.(*net.TCPConn).SetLinger(0)
		return
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
	// Wait for the peer to finish closing so the close is orderly.
	c.SetReadDeadline(time.Now().Add(time.Minute))
	io.Copy(io.Discard, br)
}
