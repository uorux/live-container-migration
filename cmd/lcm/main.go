// lcm is the live-migration CLI.
//
//	lcm migrate <pod> --to <node> [-n namespace] [--container c] [--token-file f]
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
	if len(os.Args) < 3 || os.Args[1] != "migrate" {
		fmt.Fprintln(os.Stderr, "usage: lcm migrate <pod> --to <node> [flags]")
		os.Exit(2)
	}
	pod := os.Args[2]
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
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
	res, err := controller.Migrate(ctx, kc, controller.Options{
		Namespace: *ns, Pod: pod, Container: *ctr, TargetNode: *to,
		Token: strings.TrimSpace(string(tok)), WaitImage: *waitImage, Relay: *relay,
		PreDump: api.PreDumpPolicy{Max: *preMax, TargetBytes: *preTarget},
		Log: func(f string, a ...any) {
			fmt.Fprintf(os.Stderr, "%7.3fs "+f+"\n", append([]any{time.Since(start).Seconds()}, a...)...)
		},
	})
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
