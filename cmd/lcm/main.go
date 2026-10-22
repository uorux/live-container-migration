// lcm is the live-migration CLI.
//
//	lcm migrate <pod> --to <node> [-n namespace] [--container c] [--token-file f]
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

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"lcm/internal/api"
	"lcm/internal/controller"
)

func main() {
	if len(os.Args) < 3 || (os.Args[1] != "migrate" && os.Args[1] != "recover") {
		fmt.Fprintln(os.Stderr, "usage: lcm migrate <pod> --to <node> [flags]\n       lcm recover <template-pod> --to <node> --images-on <node> --key <key>")
		os.Exit(2)
	}
	cmd, pod := os.Args[1], os.Args[2]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	imagesOn := fs.String("images-on", "", "recover: node holding the staged images")
	key := fs.String("key", "", "recover: staging key (namespace_pod_container)")
	ns := fs.String("n", "default", "namespace")
	to := fs.String("to", "", "target node (required)")
	ctr := fs.String("container", "", "container to migrate (default: the only one)")
	tokenFile := fs.String("token-file", os.Getenv("LCM_TOKEN_FILE"), "agent bearer token file")
	kubeconfig := fs.String("kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig (default: in-cluster or ~/.kube/config)")
	preMax := fs.Int("predump-max", 4, "max pre-copy rounds")
	preTarget := fs.Int64("predump-target", 64<<20, "stop pre-copy once a round is smaller than this many bytes")
	relay := fs.String("relay", "auto", "lcm-relay handshake: auto, required, off")
	waitImage := fs.String("wait-image", "", "image for the destination's wait init container (default busybox)")
	asJSON := fs.Bool("json", false, "print the result as JSON")
	allowManaged := fs.Bool("allow-managed", false, "migrate a pod with GitOps (Flux/Helm) labels; suspend reconciliation first")
	fs.Parse(os.Args[3:])
	if *to == "" {
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
		Token: strings.TrimSpace(string(tok)), WaitImage: *waitImage, Relay: *relay, AllowManaged: *allowManaged,
		PreDump: api.PreDumpPolicy{Max: *preMax, TargetBytes: *preTarget},
		Log: func(f string, a ...any) {
			fmt.Fprintf(os.Stderr, "%7.3fs "+f+"\n", append([]any{time.Since(start).Seconds()}, a...)...)
		},
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
	res, err := controller.Migrate(ctx, kc, opts)
	if *asJSON && res != nil {
		json.NewEncoder(os.Stdout).Encode(res)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "migration failed:", err)
		os.Exit(1)
	}
	ck := res.Checkpoint
	fmt.Fprintf(os.Stderr, "done: %s -> %s/%s in %s; blackout %s (final dump %dms, ship %dms, restore %dms)\n",
		res.SourcePod, res.DestNode, res.DestPod, time.Since(start).Round(time.Millisecond), res.Blackout.Round(time.Millisecond),
		ck.FinalDumpMS, ck.ShipMS, restoreMS(res))
}

func restoreMS(r *controller.Result) int64 {
	if r.Restore == nil {
		return -1
	}
	return r.Restore.RestoreMS
}
