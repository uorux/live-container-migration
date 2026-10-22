//go:debug multipathtcp=0

// lcm-loadtest drives fake players through lcm-gateway and can stand in for
// the Minecraft server with a PROXY-v2-aware echo server.
//
//	lcm-loadtest echo    [-listen 127.0.0.1:25565] [-proxy-protocol]
//	lcm-loadtest players [-addr host:25565] [-n 20] [-bytes 8M] [-rate 64K] [-duration 0]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"lcm/internal/cliutil"
	"lcm/internal/loadtest"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lcm-loadtest echo|players [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "echo":
		echo(os.Args[2:])
	case "players":
		os.Exit(players(os.Args[2:]))
	default:
		fmt.Fprintln(os.Stderr, "usage: lcm-loadtest echo|players [flags]")
		os.Exit(2)
	}
}

func echo(args []string) {
	fs := flag.NewFlagSet("echo", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:25565", "listen address")
	pp := fs.Bool("proxy-protocol", true, "expect a PROXY v2 header on each connection")
	fs.Parse(args)
	srv, err := loadtest.NewEchoServer(*listen, *pp)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "echo server on", srv.Addr())
	go srv.Serve()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	seen := 0
	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			srv.Close()
			return
		case <-t.C:
			hs := srv.Headers()
			for _, h := range hs[seen:] {
				fmt.Fprintf(os.Stderr, "proxy header: src=%v dst=%v local=%v\n", h.Src, h.Dst, h.Local)
			}
			seen = len(hs)
			fmt.Fprintf(os.Stderr, "open connections: %d, errors: %v\n", srv.NumConns(), srv.Errors())
		}
	}
}

func players(args []string) int {
	fs := flag.NewFlagSet("players", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:25565", "gateway player address")
	n := fs.Int("n", 20, "number of concurrent players")
	size := cliutil.Size(8 << 20)
	fs.Var(&size, "bytes", "bytes each player sends (and verifies echoed)")
	rate := cliutil.Size(64 << 10)
	fs.Var(&rate, "rate", "per-player send rate in bytes/s (0 = unlimited)")
	seed := fs.Uint64("seed", uint64(time.Now().UnixNano()), "base seed")
	halfClose := fs.Bool("half-close", true, "half-close after sending and expect the server's EOF")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var verified atomic.Int64
	var wg sync.WaitGroup
	var failed atomic.Int32
	start := time.Now()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-done:
				return
			case <-t.C:
				v := verified.Load()
				fmt.Fprintf(os.Stderr, "%6.1fs verified %d bytes (+%d/s)\n", time.Since(start).Seconds(), v, v-last)
				last = v
			}
		}
	}()
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := loadtest.RunPlayer(ctx, loadtest.PlayerConfig{
				Addr: *addr, Seed: *seed + uint64(i), Bytes: int64(size), Rate: int64(rate),
				HalfClose: *halfClose, ExpectEOFAfter: -1,
				OnVerified: func(k int) { verified.Add(int64(k)) },
			})
			if err != nil {
				failed.Add(1)
				fmt.Fprintf(os.Stderr, "player %d (%v): FAIL after %d/%d bytes: %v\n", i, res.LocalAddr, res.Verified, int64(size), err)
				return
			}
			fmt.Fprintf(os.Stderr, "player %d (%v): ok %d bytes in %v sha256=%x\n", i, res.LocalAddr, res.Verified, res.Duration.Round(time.Millisecond), res.SHA256[:8])
		}(i)
	}
	wg.Wait()
	close(done)
	fmt.Printf("players=%d failed=%d verified=%d bytes elapsed=%v\n", *n, failed.Load(), verified.Load(), time.Since(start).Round(time.Millisecond))
	if failed.Load() > 0 {
		return 1
	}
	return 0
}
