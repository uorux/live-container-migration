//go:debug multipathtcp=0

// lcm-gateway holds player TCP connections and multiplexes them over a
// resumable tunnel to lcm-relay inside the (migrating) Minecraft container.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"lcm/internal/cliutil"
	"lcm/internal/gateway"
	"lcm/internal/tunnel"
	"lcm/internal/vip"
	"lcm/internal/vipgw"
)

// version is set at build time: -ldflags "-X main.version=...".
var version = "dev"

func main() {
	listen := flag.String("listen", cliutil.EnvDefault("LCM_LISTEN", ":25565"), "player listen address (env LCM_LISTEN)")
	tunnelListen := flag.String("tunnel-listen", cliutil.EnvDefault("LCM_TUNNEL_LISTEN", ":7000"), "relay tunnel listen address (env LCM_TUNNEL_LISTEN)")
	admin := flag.String("admin-listen", cliutil.EnvDefault("LCM_ADMIN_LISTEN", ""), "HTTP address for /status, /metrics and /healthz (empty = off; env LCM_ADMIN_LISTEN)")
	backend := flag.String("backend", cliutil.EnvDefault("LCM_BACKEND", "default"), "backend name relays must present; with several, where players go when no route matches (env LCM_BACKEND)")
	backends := flag.String("backends", cliutil.EnvDefault("LCM_BACKENDS", ""), "more backends behind this gateway, comma-separated; -minecraft routes players by hostname: <backend>.<any domain>, or -route (env LCM_BACKENDS)")
	routes := flag.String("route", cliutil.EnvDefault("LCM_ROUTES", ""), "explicit host=backend routes, comma-separated (env LCM_ROUTES)")
	token := flag.String("token", "", "shared tunnel secret (prefer -token-file / LCM_TOKEN / LCM_TOKEN_FILE)")
	tokenFile := flag.String("token-file", "", "file containing the shared tunnel secret")
	window := cliutil.Size(1 << 20)
	flag.Var(&window, "window", "per-stream buffer / flow-control window (e.g. 1M)")
	ping := flag.Duration("ping-interval", cliutil.EnvDuration("LCM_PING_INTERVAL", 2*time.Second), "tunnel heartbeat interval (env LCM_PING_INTERVAL)")
	dead := flag.Duration("dead-timeout", cliutil.EnvDuration("LCM_DEAD_TIMEOUT", 10*time.Second), "declare the tunnel dead after this long without frames (env LCM_DEAD_TIMEOUT)")
	minecraft := flag.Bool("minecraft", envBool("LCM_MINECRAFT"), "Minecraft-aware first packets: answer status and hold logins while no relay is connected (env LCM_MINECRAFT=true)")
	motd := flag.String("motd", cliutil.EnvDefault("LCM_MOTD", "Server is starting, please wait..."), "server list MOTD while the backend is down; {backend} is replaced by its name (-minecraft; env LCM_MOTD)")
	statusVersion := flag.String("status-version", cliutil.EnvDefault("LCM_STATUS_VERSION", "starting"), "version name in the local status reply (-minecraft; env LCM_STATUS_VERSION)")
	holdInterval := flag.Duration("hold-interval", cliutil.EnvDuration("LCM_HOLD_INTERVAL", 10*time.Second), "keepalive interval for held logins; must stay well under the client's 30 s timeout (-minecraft; env LCM_HOLD_INTERVAL)")
	holdMax := flag.Duration("hold-max", cliutil.EnvDuration("LCM_HOLD_MAX", 5*time.Minute), "kick a held login with -hold-message after this long (-minecraft; env LCM_HOLD_MAX)")
	holdMessage := flag.String("hold-message", cliutil.EnvDefault("LCM_HOLD_MESSAGE", "The server did not start in time. Please try again later."), "Login Disconnect text when -hold-max passes (-minecraft; env LCM_HOLD_MESSAGE)")
	menu := flag.Bool("menu", envBool("LCM_MENU"), "players whose hostname names no backend get a server menu that starts the server they pick and transfers them to it (1.21.6+ clients; older ones get a list of addresses) instead of the default backend (-minecraft; env LCM_MENU=true)")
	menuTitle := flag.String("menu-title", cliutil.EnvDefault("LCM_MENU_TITLE", "Minecraft servers"), "title of the server menu and its server list entry (env LCM_MENU_TITLE)")
	whitelist := flag.String("whitelist", cliutil.EnvDefault("LCM_WHITELIST", ""), "who may wake an asleep backend: backend=file[+file],... with * for every other backend; files are whitelist.json/ops.json lists (-minecraft; env LCM_WHITELIST)")
	labels := flag.String("label", cliutil.EnvDefault("LCM_LABELS", ""), "display names in the menu, backend=name pairs separated by commas (env LCM_LABELS)")
	acceptProxy := flag.String("accept-proxy", cliutil.EnvDefault("LCM_ACCEPT_PROXY", ""), "comma-separated CIDRs (e.g. 127.0.0.1/32 for a local HAProxy with send-proxy-v2) whose player connections start with a PROXY v2 header (env LCM_ACCEPT_PROXY)")
	wakeURLs := flag.String("wake-url", cliutil.EnvDefault("LCM_WAKE_URL", ""), "lcm-agent base URLs (comma-separated, e.g. http://node:7790) to wake an on-demand backend when a player arrives and its relay isn't connected (env LCM_WAKE_URL)")
	wakeTokenFile := flag.String("wake-token-file", cliutil.EnvDefault("LCM_WAKE_TOKEN_FILE", ""), "file with the agents' wake token (env LCM_WAKE_TOKEN_FILE)")
	vipCIDR := flag.String("vip", cliutil.EnvDefault("LCM_VIP", ""), "serve players on this VIP (e.g. 10.0.0.250/24), in a private network namespace on -vip-parent; one gateway per node, the active one chosen by a Lease, handing its players off on SIGTERM (env LCM_VIP)")
	vipParent := flag.String("vip-parent", cliutil.EnvDefault("LCM_VIP_PARENT", ""), "the node's LAN NIC the VIP's ipvlan sits on (-vip; env LCM_VIP_PARENT)")
	vipGateway := flag.String("vip-gateway", cliutil.EnvDefault("LCM_VIP_GATEWAY", ""), "the LAN's default gateway, for replies to players beyond the LAN (-vip; env LCM_VIP_GATEWAY)")
	vipLease := flag.String("vip-lease", cliutil.EnvDefault("LCM_VIP_LEASE", "lcm-gateway-vip"), "the Lease that says which gateway serves the VIP (-vip; env LCM_VIP_LEASE)")
	vipPeers := flag.String("vip-peers", cliutil.EnvDefault("LCM_VIP_PEERS", "app=lcm-gateway-vip"), "label selector of the gateway pods that can take a handoff (-vip; env LCM_VIP_PEERS)")
	vipLabel := flag.String("vip-active-label", cliutil.EnvDefault("LCM_VIP_ACTIVE_LABEL", "lcm.io/vip-active"), "pod label set to true on the gateway serving the VIP, for the relays' Service (-vip)")
	handoffPort := flag.Int("handoff-port", cliutil.EnvInt("LCM_HANDOFF_PORT", 7792), "TCP port for handoffs between gateways (-vip; env LCM_HANDOFF_PORT)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	logLevel := flag.String("log-level", cliutil.EnvDefault("LCM_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	log := cliutil.Logger(*logLevel, "lcm-gateway")
	tok, err := cliutil.Token(*token, *tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-gateway:", err)
		os.Exit(2)
	}
	labelMap := map[string]string{}
	for _, l := range strings.Split(*labels, ",") {
		if l = strings.TrimSpace(l); l == "" {
			continue
		}
		be, name, ok := strings.Cut(l, "=")
		if !ok {
			fmt.Fprintln(os.Stderr, "lcm-gateway: -label wants backend=name, got", l)
			os.Exit(2)
		}
		labelMap[strings.TrimSpace(be)] = strings.TrimSpace(name)
	}
	if *menu && !*minecraft {
		fmt.Fprintln(os.Stderr, "lcm-gateway: -menu needs -minecraft")
		os.Exit(2)
	}
	if *whitelist != "" && !*minecraft {
		fmt.Fprintln(os.Stderr, "lcm-gateway: -whitelist needs -minecraft")
		os.Exit(2)
	}
	var mc *gateway.MinecraftConfig
	if *minecraft {
		wl, err := gateway.ParseWhitelist(*whitelist, log)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lcm-gateway:", err)
			os.Exit(2)
		}
		mc = &gateway.MinecraftConfig{MOTD: *motd, VersionName: *statusVersion,
			HoldInterval: *holdInterval, MaxHold: *holdMax, HoldMessage: *holdMessage,
			Menu: *menu, MenuTitle: *menuTitle, Labels: labelMap, Whitelist: wl}
	}
	var trusted []netip.Prefix
	for _, c := range strings.Split(*acceptProxy, ",") {
		if c = strings.TrimSpace(c); c == "" {
			continue
		}
		p, err := netip.ParsePrefix(c)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lcm-gateway: -accept-proxy:", err)
			os.Exit(2)
		}
		trusted = append(trusted, p)
	}
	var extra []string
	for _, b := range strings.Split(*backends, ",") {
		if b = strings.TrimSpace(b); b != "" {
			extra = append(extra, b)
		}
	}
	routeMap := map[string]string{}
	for _, r := range strings.Split(*routes, ",") {
		if r = strings.TrimSpace(r); r == "" {
			continue
		}
		host, be, ok := strings.Cut(r, "=")
		if !ok {
			fmt.Fprintln(os.Stderr, "lcm-gateway: -route wants host=backend, got", r)
			os.Exit(2)
		}
		routeMap[strings.ToLower(strings.TrimSuffix(host, "."))] = be
		extra = append(extra, be)
	}
	if len(extra) > 0 && !*minecraft {
		fmt.Fprintln(os.Stderr, "lcm-gateway: routing to several backends needs -minecraft (it reads the handshake's hostname)")
		os.Exit(2)
	}
	var wake func(context.Context, string) error
	if *wakeURLs != "" {
		var urls []string
		for _, u := range strings.Split(*wakeURLs, ",") {
			if u = strings.TrimSpace(u); u != "" {
				urls = append(urls, u)
			}
		}
		b, err := os.ReadFile(*wakeTokenFile)
		wt := strings.TrimSpace(string(b))
		if err != nil || wt == "" {
			fmt.Fprintln(os.Stderr, "lcm-gateway: -wake-url needs a readable, non-empty -wake-token-file")
			os.Exit(2)
		}
		wake = gateway.HTTPWaker(urls, wt)
	}
	g := gateway.New(gateway.Config{
		Wake:        wake,
		AcceptProxy: trusted,
		Backends:    extra,
		Routes:      routeMap,
		Backend:     *backend,
		Logger:      log,
		Minecraft:   mc,
		Version:     version,
		Tunnel:      tunnel.Config{Token: tok, Window: int(window), PingInterval: *ping, DeadTimeout: *dead},
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var ctrl *vipgw.Controller
	var pl net.Listener
	if *vipCIDR != "" {
		ctrl, pl, err = startVIP(g, log.With("component", "vip"), tok, *listen, *tunnelListen, *vipCIDR, *vipParent, *vipGateway, *vipLease, *vipPeers, *vipLabel, *handoffPort)
	} else {
		pl, err = gateway.Listen(ctx, *listen)
	}
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
		h := g.AdminHandler()
		if ctrl != nil {
			mux := http.NewServeMux()
			mux.Handle("/", h)
			mux.HandleFunc("GET /vip", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(ctrl.Stats())
			})
			mux.HandleFunc("POST /vip/handoff", func(w http.ResponseWriter, r *http.Request) {
				if err := ctrl.HandOff(r.Context()); err != nil {
					http.Error(w, err.Error(), http.StatusConflict)
					return
				}
				w.Write([]byte("handed off\n"))
			})
			h = mux
		}
		go http.Serve(al, h)
	}
	ctrlDone := make(chan error, 1)
	if ctrl != nil {
		go func() { ctrlDone <- ctrl.Run(ctx) }()
	}
	log.Info("serving", "players", pl.Addr().String(), "tunnel", tl.Addr().String(), "backend", *backend, "window", int(window), "minecraft", *minecraft, "menu", *menu, "version", version)
	<-ctx.Done()
	if ctrl != nil {
		if err := <-ctrlDone; err != nil {
			log.Error("VIP handoff at shutdown", "err", err)
		} else if st := ctrl.Stats(); st.HandoffsOut > 0 && !st.Active {
			// Relays reconnect here until their Service follows the new
			// gateway's label: keep forwarding them for a moment.
			log.Info("handed off; forwarding relays to the new gateway before exiting")
			time.Sleep(5 * time.Second)
		}
	}
	log.Info("shutting down")
	pl.Close()
	tl.Close()
	g.DefaultBackend().Session().CloseAll()
	time.Sleep(200 * time.Millisecond)
}

func envBool(name string) bool {
	b, _ := strconv.ParseBool(cliutil.EnvDefault(name, "false"))
	return b
}

// startVIP sets up the VIP namespace and its player listener, and the
// controller that decides when this gateway serves the VIP.
func startVIP(g *gateway.Gateway, log *slog.Logger, tok []byte, listen, tunnelListen, cidr, parent, gw, lease, peers, label string, handoffPort int) (*vipgw.Controller, net.Listener, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return nil, nil, fmt.Errorf("-vip: %w", err)
	}
	var gwAddr netip.Addr
	if gw != "" {
		if gwAddr, err = netip.ParseAddr(gw); err != nil {
			return nil, nil, fmt.Errorf("-vip-gateway: %w", err)
		}
	}
	if parent == "" {
		return nil, nil, fmt.Errorf("-vip needs -vip-parent")
	}
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, nil, fmt.Errorf("-listen: %w", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, nil, fmt.Errorf("-listen port: %w", err)
	}
	tport := 0
	if _, tp, err := net.SplitHostPort(tunnelListen); err == nil {
		tport, _ = strconv.Atoi(tp)
	}
	pod, podNS := os.Getenv("POD_NAME"), os.Getenv("POD_NAMESPACE")
	if pod == "" || podNS == "" {
		return nil, nil, fmt.Errorf("-vip needs POD_NAME and POD_NAMESPACE (downward API)")
	}
	rc, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("kubernetes client: %w", err)
	}
	kc, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, nil, err
	}
	ns, err := vip.Create(parent, prefix, gwAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("VIP namespace: %w", err)
	}
	pl, err := ns.Listen(uint16(port))
	if err != nil {
		return nil, nil, err
	}
	ctrl := vipgw.New(vipgw.Config{Gateway: g, NS: ns, Token: tok, Kube: kc, Namespace: podNS, Identity: pod,
		Lease: lease, Selector: peers, HandoffPort: handoffPort, TunnelPort: tport, ActiveLabel: label, Logger: log})
	return ctrl, pl, nil
}
