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
	"strconv"
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
	minecraft := flag.Bool("minecraft", envBool("LCM_MINECRAFT"), "Minecraft-aware first packets: answer status and hold logins while no relay is connected (env LCM_MINECRAFT=true)")
	motd := flag.String("motd", cliutil.EnvDefault("LCM_MOTD", "Server is starting, please wait..."), "server list MOTD while the backend is down (-minecraft; env LCM_MOTD)")
	statusVersion := flag.String("status-version", "starting", "version name in the local status reply (-minecraft)")
	holdInterval := flag.Duration("hold-interval", 10*time.Second, "keepalive interval for held logins; must stay well under the client's 30 s timeout (-minecraft)")
	holdMax := flag.Duration("hold-max", 5*time.Minute, "kick a held login with -hold-message after this long (-minecraft)")
	holdMessage := flag.String("hold-message", "The server did not start in time. Please try again later.", "Login Disconnect text when -hold-max passes (-minecraft)")
	logLevel := flag.String("log-level", cliutil.EnvDefault("LCM_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()

	log := cliutil.Logger(*logLevel, "lcm-gateway")
	tok, err := cliutil.Token(*token, *tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-gateway:", err)
		os.Exit(2)
	}
	var mc *gateway.MinecraftConfig
	if *minecraft {
		mc = &gateway.MinecraftConfig{MOTD: *motd, VersionName: *statusVersion,
			HoldInterval: *holdInterval, MaxHold: *holdMax, HoldMessage: *holdMessage}
	}
	g := gateway.New(gateway.Config{
		Backend:   *backend,
		Logger:    log,
		Minecraft: mc,
		Tunnel:    tunnel.Config{Token: tok, Window: int(window), PingInterval: *ping, DeadTimeout: *dead},
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
	log.Info("serving", "players", pl.Addr().String(), "tunnel", tl.Addr().String(), "backend", *backend, "window", int(window), "minecraft", *minecraft)
	<-ctx.Done()
	log.Info("shutting down")
	pl.Close()
	tl.Close()
	g.DefaultBackend().Session.CloseAll()
	time.Sleep(200 * time.Millisecond)
}

func envBool(name string) bool {
	b, _ := strconv.ParseBool(cliutil.EnvDefault(name, "false"))
	return b
}
