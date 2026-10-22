//go:debug multipathtcp=0

// lcm-gateway holds player TCP connections and multiplexes them over a
// resumable tunnel to lcm-relay inside the (migrating) Minecraft container.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lcm/internal/cliutil"
	"lcm/internal/gateway"
	"lcm/internal/tunnel"
)

func main() {
	listen := flag.String("listen", cliutil.EnvDefault("LCM_LISTEN", ":25565"), "player listen address (env LCM_LISTEN)")
	tunnelListen := flag.String("tunnel-listen", cliutil.EnvDefault("LCM_TUNNEL_LISTEN", ":7000"), "relay tunnel listen address (env LCM_TUNNEL_LISTEN)")
	admin := flag.String("admin-listen", cliutil.EnvDefault("LCM_ADMIN_LISTEN", ""), "HTTP address for /status and /healthz (empty = off; env LCM_ADMIN_LISTEN)")
	backend := flag.String("backend", "default", "backend name relays must present")
	token := flag.String("token", "", "shared tunnel secret (prefer -token-file / LCM_TOKEN / LCM_TOKEN_FILE)")
	tokenFile := flag.String("token-file", "", "file containing the shared tunnel secret")
	window := cliutil.Size(1 << 20)
	flag.Var(&window, "window", "per-stream buffer / flow-control window (e.g. 1M)")
	ping := flag.Duration("ping-interval", 2*time.Second, "tunnel heartbeat interval")
	dead := flag.Duration("dead-timeout", 10*time.Second, "declare the tunnel dead after this long without frames")
	logLevel := flag.String("log-level", cliutil.EnvDefault("LCM_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()

	log := cliutil.Logger(*logLevel, "lcm-gateway")
	tok, err := cliutil.Token(*token, *tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-gateway:", err)
		os.Exit(2)
	}
	g := gateway.New(gateway.Config{
		Backend: *backend,
		Logger:  log,
		Tunnel:  tunnel.Config{Token: tok, Window: int(window), PingInterval: *ping, DeadTimeout: *dead},
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pl, err := gateway.Listen(ctx, *listen)
	if err != nil {
		log.Error("listen players", "err", err)
		os.Exit(1)
	}
	tl, err := gateway.Listen(ctx, *tunnelListen)
	if err != nil {
		log.Error("listen tunnel", "err", err)
		os.Exit(1)
	}
	go g.ServePlayers(pl, g.DefaultBackend())
	go g.ServeTunnel(tl)
	if *admin != "" {
		al, err := gateway.Listen(ctx, *admin)
		if err != nil {
			log.Error("listen admin", "err", err)
			os.Exit(1)
		}
		go http.Serve(al, g.AdminHandler())
	}
	log.Info("serving", "players", pl.Addr().String(), "tunnel", tl.Addr().String(), "backend", *backend, "window", int(window))
	<-ctx.Done()
	log.Info("shutting down")
	pl.Close()
	tl.Close()
	g.DefaultBackend().Session.CloseAll()
	time.Sleep(200 * time.Millisecond)
}
