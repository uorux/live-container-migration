// lcm is the live-migration CLI.
//
//	lcm migrate <pod> --to <node> [-n namespace] [--container c] [--token-file f]
//	lcm status                      every node's agent: health, staging RAM, fences, in-flight work
//	lcm history [-limit 20]            recent migrations across all nodes (outcome, blackout, errors)
//	lcm recover <template-pod> --to <node> --images-on <node> --key <ns_pod_container>
//	    restore images left staged by a migration that failed after the source stopped
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"lcm/internal/api"
	"lcm/internal/controller"
)

func main() {
	usage := "usage: lcm migrate <pod>|livepod/<name> --to <node>|auto [flags]\n       lcm drain <node> [flags]   (live-migrate every lcm pod off a node)\n       lcm status | lcm history [-limit N]\n       lcm recover <template-pod> --to <node> --images-on <node> --key <key>"
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, pod, rest := os.Args[1], "", os.Args[2:]
	switch cmd {
	case "status", "history":
	case "migrate", "recover", "drain":
		if len(rest) == 0 {
			fmt.Fprintln(os.Stderr, usage)
			os.Exit(2)
		}
		pod, rest = rest[0], rest[1:]
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	imagesOn := fs.String("images-on", "", "recover: node holding the staged images")
	key := fs.String("key", "", "recover: staging key (namespace_pod_container)")
	ns := fs.String("n", "default", "namespace")
	to := fs.String("to", "", "target node (required)")
	ctr := fs.String("container", "", "container to migrate (default: the only one)")
	tokenFile := fs.String("token-file", envOr("LCM_TOKEN_FILE", "/run/lcm/token"), "agent bearer token file")
	kubeconfig := fs.String("kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig (default: in-cluster or ~/.kube/config)")
	preMax := fs.Int("predump-max", 8, "max pre-copy rounds (they stop early once a round shrinks by less than 10%, or the destination's memory couldn't hold another)")
	preTarget := fs.Int64("predump-target", 64<<20, "stop pre-copy once a round is smaller than this many bytes")
	relay := fs.String("relay", "auto", "lcm-relay handshake: auto, required, off")
	waitImage := fs.String("wait-image", "", "image for the destination's wait init container (default busybox)")
	asJSON := fs.Bool("json", false, "print the result as JSON")
	histN := fs.Int("limit", 20, "history: how many migrations")
	skipMem := fs.Bool("skip-memory-check", false, "migrate even if the nodes look short of RAM for the staged images")
	allowSkew := fs.Bool("allow-version-skew", false, "migrate even if an agent is too old to negotiate with (see docs/operations.md)")
	lazy := fs.Bool("lazy-restore", false, "experimental: restore with CRIU lazy pages (resume before memory is loaded)")
	transferNet := fs.String("transfer-network", os.Getenv("LCM_TRANSFER_NETWORK"), "ship checkpoint data over agent addresses in this CIDR (e.g. a LAN) instead of InternalIPs, encrypted (agents' -tls-listen)")
	transferPlain := fs.Bool("transfer-plaintext", os.Getenv("LCM_TRANSFER_PLAINTEXT") == "1", "allow unencrypted HTTP on --transfer-network to agents without -tls-listen (otherwise those ship over InternalIPs)")
	allowManaged := fs.Bool("allow-managed", false, "migrate a pod with GitOps (Flux/Helm) labels; suspend reconciliation first")
	fs.Parse(rest)
	if *to == "" && (cmd == "migrate" || cmd == "recover") {
		fmt.Fprintln(os.Stderr, "--to is required")
		os.Exit(2)
	}
	tok, err := os.ReadFile(*tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reading token:", err)
		os.Exit(1)
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = *kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	kc := kubernetes.NewForConfigOrDie(cfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	start := time.Now()
	opts := controller.Options{
		Namespace: *ns, Pod: pod, Container: *ctr, TargetNode: *to,
		Token: strings.TrimSpace(string(tok)), WaitImage: *waitImage, Relay: *relay, AllowManaged: *allowManaged, TransferNetwork: *transferNet, TransferPlaintext: *transferPlain, LazyRestore: *lazy, SkipMemoryCheck: *skipMem, AllowVersionSkew: *allowSkew,
		PreDump: api.PreDumpPolicy{Max: *preMax, TargetBytes: *preTarget},
		Log: func(f string, a ...any) {
			fmt.Fprintf(os.Stderr, "%7.3fs "+f+"\n", append([]any{time.Since(start).Seconds()}, a...)...)
		},
	}
	switch cmd {
	case "status":
		sts, err := controller.Status(ctx, kc, opts)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		bad := false
		for _, st := range sts {
			if !st.Healthy {
				bad = true
				fmt.Printf("%-12s DOWN  %s\n", st.Node, st.Error)
				continue
			}
			ver := "?"
			for k := range st.Metrics {
				if v, ok := strings.CutPrefix(k, `lcm_agent_build_info{version="`); ok {
					ver = strings.TrimSuffix(v, `"}`)
				}
			}
			fmt.Printf("%-12s up    version=%s inflight=%.0f staged=%.0f fences=%.0f staging ckpt=%s restore=%s\n", st.Node, ver,
				st.Metrics["lcm_agent_inflight_checkpoints"], st.Metrics["lcm_staged_restores"], st.Metrics["lcm_fences"],
				humanBytes(st.Metrics[`lcm_staging_bytes{dir="ckpt"}`]), humanBytes(st.Metrics[`lcm_staging_bytes{dir="restore"}`]))
		}
		if bad {
			os.Exit(1)
		}
		return
	case "history":
		reps, err := controller.History(ctx, kc, opts, *histN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if *asJSON {
			json.NewEncoder(os.Stdout).Encode(reps)
			return
		}
		for _, r := range reps {
			to := r.DestNode + "/" + r.DestPod
			if r.Outcome == "rolled_back" && r.TargetNode != "" {
				to = r.TargetNode + ", back on " + to
			}
			line := fmt.Sprintf("%s %-11s %s/%s %s -> %s blackout=%s total=%s", r.Time.Local().Format("01-02 15:04:05"), r.Outcome,
				r.Namespace, r.SourcePod, r.SourceNode, to,
				time.Duration(r.BlackoutMS)*time.Millisecond, time.Duration(r.TotalMS)*time.Millisecond)
			if r.Error != "" {
				line += "\n    " + r.Error
			}
			fmt.Println(line)
		}
		return
	}
	if cmd == "drain" {
		os.Exit(drain(ctx, kc, opts, pod, *asJSON))
	}
	if p, ok := strings.CutPrefix(opts.Pod, "livepod/"); ok && cmd == "migrate" {
		name, err := controller.LivePodPod(ctx, kc, *ns, p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		opts.Pod = name
	}
	if cmd == "recover" {
		res, err := controller.Recover(ctx, kc, opts, *imagesOn, *key)
		if err != nil {
			fmt.Fprintln(os.Stderr, "recover failed:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "recovered: running on %s as %s\n", res.DestNode, res.DestPod)
		return
	}
	if opts.TargetNode == "auto" {
		src, err := kc.CoreV1().Pods(opts.Namespace).Get(ctx, opts.Pod, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if opts.TargetNode, err = controller.PickTarget(ctx, kc, opts, src.Spec.NodeName); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		opts.Log("target: %s (most available memory)", opts.TargetNode)
	}
	res, err := controller.Migrate(ctx, kc, opts)
	if res != nil { // nil: refused before anything started, nothing to record
		rep := controller.NewReport(opts, res, err, time.Since(start))
		if rerr := controller.Report(context.WithoutCancel(ctx), kc, opts, rep); rerr != nil {
			fmt.Fprintln(os.Stderr, "warning: reporting the outcome to the agents:", rerr)
		}
	}
	if *asJSON && res != nil {
		json.NewEncoder(os.Stdout).Encode(res)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "migration failed:", err)
		os.Exit(1)
	}
	ck := res.Checkpoint
	bo := res.Blackout.Round(time.Millisecond).String()
	if res.Restore == nil {
		bo = "unknown (the destination agent didn't return the restore result)"
	}
	fmt.Fprintf(os.Stderr, "done: %s -> %s/%s in %s; blackout %s (final dump %dms, ship %dms, restore %dms)\n",
		res.SourcePod, res.DestNode, res.DestPod, time.Since(start).Round(time.Millisecond), bo,
		ck.FinalDumpMS, ck.ShipMS, restoreMS(res))
}

// drain live-migrates every lcm pod off node, one at a time, each to the node with the most
// available memory. It carries on past failures and returns the exit code.
func drain(ctx context.Context, kc kubernetes.Interface, base controller.Options, node string, asJSON bool) int {
	pods, err := controller.DrainCandidates(ctx, kc, node)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if len(pods) == 0 {
		fmt.Fprintf(os.Stderr, "no running lcm pods on %s\n", node)
		return 0
	}
	fmt.Fprintf(os.Stderr, "draining %d pod(s) from %s (cordon it first so nothing new lands there)\n", len(pods), node)
	failed := 0
	for _, p := range pods {
		if ctx.Err() != nil {
			break
		}
		o := base
		o.Namespace, o.Pod = p.Namespace, p.Name
		if o.Container == "" && len(p.Spec.Containers) != 1 {
			fmt.Fprintf(os.Stderr, "skipping %s/%s: %d containers (migrate it with --container)\n", p.Namespace, p.Name, len(p.Spec.Containers))
			failed++
			continue
		}
		start := time.Now()
		to, err := controller.PickTarget(ctx, kc, o, node)
		if err == nil {
			o.TargetNode = to
			o.Log("%s/%s -> %s", p.Namespace, p.Name, to)
			var res *controller.Result
			res, err = controller.Migrate(ctx, kc, o)
			rep := controller.NewReport(o, res, err, time.Since(start))
			controller.Report(context.WithoutCancel(ctx), kc, o, rep)
			if asJSON && res != nil {
				json.NewEncoder(os.Stdout).Encode(res)
			}
			if err == nil {
				fmt.Fprintf(os.Stderr, "done: %s/%s -> %s/%s; blackout %s\n", p.Namespace, p.Name, res.DestNode, res.DestPod, res.Blackout.Round(time.Millisecond))
				continue
			}
		}
		fmt.Fprintf(os.Stderr, "FAILED: %s/%s: %v\n", p.Namespace, p.Name, err)
		failed++
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d of %d pod(s) not drained\n", failed, len(pods))
		return 1
	}
	return 0
}

func humanBytes(v float64) string {
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.1fG", v/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.0fM", v/(1<<20))
	}
	return fmt.Sprintf("%.0f", v)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func restoreMS(r *controller.Result) int64 {
	if r.Restore == nil {
		return -1
	}
	return r.Restore.RestoreMS
}
