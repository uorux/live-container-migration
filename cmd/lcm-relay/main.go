//go:debug multipathtcp=0

// lcm-relay is the entrypoint of the migrating Minecraft container. It
// supervises the server process (as PID 1), dials out to lcm-gateway, and
// bridges each player stream to the server over loopback with a PROXY v2
// header. See docs/tunnel-protocol.md.
//
//	lcm-relay run [flags] [-- command args...]
//	lcm-relay ctl [-socket path] quiesce|resume|status
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"lcm/internal/cliutil"
	"lcm/internal/relay"
	"lcm/internal/supervise"
	"lcm/internal/tunnel"
)

const defaultSocket = "/run/lcm/relay.sock"

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  lcm-relay run [flags] [-- command args...]
  lcm-relay ctl [-socket path] quiesce|resume|status
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		os.Exit(run(os.Args[2:]))
	case "ctl":
		os.Exit(ctl(os.Args[2:]))
	default:
		usage()
	}
}

func run(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	gw := fs.String("gateway", cliutil.EnvDefault("LCM_GATEWAY", ""), "gateway tunnel address host:port (env LCM_GATEWAY)")
	backend := fs.String("backend", cliutil.EnvDefault("LCM_BACKEND", "default"), "backend name (env LCM_BACKEND)")
	upstream := fs.String("upstream", cliutil.EnvDefault("LCM_UPSTREAM", "127.0.0.1:25565"), "server address, loopback (env LCM_UPSTREAM)")
	proxyProto := fs.Bool("proxy-protocol", true, "send a PROXY v2 header on each upstream connection")
	control := fs.String("control", cliutil.EnvDefault("LCM_CONTROL", defaultSocket), "control unix socket path, empty to disable (env LCM_CONTROL)")
	quiesceTimeout := fs.Duration("quiesce-timeout", 120*time.Second, "auto-resume after this long quiesced")
	token := fs.String("token", "", "shared tunnel secret (prefer -token-file / LCM_TOKEN / LCM_TOKEN_FILE)")
	tokenFile := fs.String("token-file", "", "file containing the shared tunnel secret")
	window := cliutil.Size(1 << 20)
	fs.Var(&window, "window", "per-stream buffer / flow-control window (e.g. 1M)")
	ping := fs.Duration("ping-interval", 2*time.Second, "tunnel heartbeat interval")
	dead := fs.Duration("dead-timeout", 10*time.Second, "declare the tunnel dead after this long without frames")
	checkSockets := fs.Bool("check-sockets", true, "verify via /proc that no non-loopback socket remains before acking quiesce")
	logLevel := fs.String("log-level", cliutil.EnvDefault("LCM_LOG_LEVEL", "info"), "debug|info|warn|error")
	fs.Parse(args)
	cmd := fs.Args()

	log := cliutil.Logger(*logLevel, "lcm-relay")
	if *gw == "" {
		fmt.Fprintln(os.Stderr, "lcm-relay: -gateway (or LCM_GATEWAY) is required")
		return 2
	}
	tok, err := cliutil.Token(*token, *tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-relay:", err)
		return 2
	}

	r := relay.New(relay.Config{
		Gateway: *gw, Backend: *backend, Upstream: *upstream, ProxyProtocol: *proxyProto,
		QuiesceTimeout: *quiesceTimeout, CheckSockets: *checkSockets, Logger: log,
		Tunnel: tunnel.Config{Token: tok, Window: int(window), PingInterval: *ping, DeadTimeout: *dead},
	})

	// Signals: forward to the child; without a child, terminate on TERM/INT.
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)

	var child *supervise.Child
	if len(cmd) > 0 {
		child, err = supervise.Start(cmd)
		if err != nil {
			log.Error("cannot start server", "cmd", cmd, "err", err)
			return 127
		}
		r.ChildPID.Store(int64(child.Pid))
		log.Info("server started", "pid", child.Pid, "cmd", strings.Join(cmd, " "))
	}

	var ctlLn net.Listener
	if *control != "" {
		ctlLn, err = relay.ListenControl(*control)
		if err != nil {
			log.Error("control socket", "path", *control, "err", err)
		} else {
			go r.ServeControl(ctlLn)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { r.Run(ctx); close(runDone) }()
	log.Info("relay started", "session", r.SessionID(), "gateway", *gw, "upstream", *upstream)

	var childDone <-chan struct{}
	if child != nil {
		childDone = child.Done()
	}
	code := 0
loop:
	for {
		select {
		case sig := <-sigs:
			s := sig.(syscall.Signal)
			if child != nil {
				log.Info("forwarding signal", "signal", s.String())
				child.Signal(s)
			} else if s == syscall.SIGTERM || s == syscall.SIGINT {
				break loop
			}
		case <-childDone:
			ws := child.Wait()
			code = supervise.ExitCode(ws)
			log.Info("server exited", "code", code)
			break loop
		}
	}
	r.Shutdown(3 * time.Second)
	cancel()
	<-runDone
	if ctlLn != nil {
		ctlLn.Close()
	}
	return code
}

func ctl(args []string) int {
	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	sock := fs.String("socket", cliutil.EnvDefault("LCM_CONTROL", defaultSocket), "control socket path")
	timeout := fs.Duration("timeout", 60*time.Second, "overall timeout")
	fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	cmd := fs.Arg(0)
	c, err := net.DialTimeout("unix", *sock, 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-relay ctl:", err)
		return 1
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(*timeout))
	if _, err := fmt.Fprintf(c, "%s\n", cmd); err != nil {
		fmt.Fprintln(os.Stderr, "lcm-relay ctl:", err)
		return 1
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-relay ctl:", err)
		return 1
	}
	fmt.Print(line)
	if cmd == "quiesce" {
		// The relay closes its end after answering quiesce; EOF means its fd is gone.
		if _, err := io.Copy(io.Discard, br); err != nil && !errors.Is(err, io.EOF) {
			fmt.Fprintln(os.Stderr, "lcm-relay ctl: waiting for close:", err)
			return 1
		}
	}
	if strings.HasPrefix(line, "error") {
		return 1
	}
	return 0
}
