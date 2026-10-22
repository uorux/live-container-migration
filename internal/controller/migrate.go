// Package controller orchestrates one live migration of a pod to another node:
//
//  1. create the destination pod (same spec, pinned to the target node, same hostname) with a
//     final init container that blocks until the checkpoint is staged on that node;
//  2. once that init container runs (sandbox, IP, volumes all ready), ask the source node's agent
//     to pre-copy, quiesce the relay, fence, final-dump and ship;
//  3. the init container exits, kubelet creates the main container, lcm-runc restores it;
//  4. delete the source pod, lift its fence, clean up staging.
package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"lcm/internal/api"
	"lcm/internal/stage"
)

const (
	RuntimeClass  = "lcm"
	WaitInitName  = "lcm-wait"
	LabelFrom     = "lcm.rooty.dev/migrated-from"
	AnnMigration  = "lcm.rooty.dev/migration-id"
	defaultWaitIm = "busybox:1.37"
)

type Options struct {
	Namespace, Pod, Container, TargetNode string
	Token                                 string
	AgentPort                             int
	WaitImage                             string
	PreDump                               api.PreDumpPolicy
	Relay                                 string
	AllowManaged                          bool // migrate pods carrying GitOps labels (reconciliation suspended)
	// TransferNetwork (CIDR): ship checkpoint data to an agent address in this network instead
	// of the node's InternalIP, e.g. a LAN faster than the overlay the InternalIPs are on.
	TransferNetwork string
	LazyRestore     bool // experimental: CRIU lazy pages on the destination
	// SkipMemoryCheck skips the pre-flight check that both nodes have the RAM for the staged
	// images (see checkMemory).
	SkipMemoryCheck bool
	Log             func(format string, a ...any)
}

type Result struct {
	MigrationID, SourcePod, SourceNode, DestPod, DestNode string
	Checkpoint                                            *api.CheckpointResult
	Restore                                               *api.RestoreResult
	// Blackout is how long the workload was not running: final dump start to restore finished.
	Blackout time.Duration
	// RolledBack: the destination restore failed and the workload was restored on the source node.
	RolledBack bool
}

func Migrate(ctx context.Context, kc kubernetes.Interface, o Options) (*Result, error) {
	if o.AgentPort == 0 {
		o.AgentPort = api.DefaultPort
	}
	if o.WaitImage == "" {
		o.WaitImage = defaultWaitIm
	}
	logf := o.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	pods := kc.CoreV1().Pods(o.Namespace)
	src, err := pods.Get(ctx, o.Pod, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := checkMigratable(src); err != nil && !(o.AllowManaged && strings.Contains(err.Error(), "GitOps")) {
		return nil, err
	}
	ctrName := o.Container
	if ctrName == "" {
		if len(src.Spec.Containers) != 1 {
			return nil, fmt.Errorf("pod has %d containers; pick one with --container", len(src.Spec.Containers))
		}
		ctrName = src.Spec.Containers[0].Name
	}
	ctrID := ""
	for _, cs := range src.Status.ContainerStatuses {
		if cs.Name == ctrName && cs.State.Running != nil {
			ctrID = strings.TrimPrefix(cs.ContainerID, "containerd://")
		}
	}
	if ctrID == "" {
		return nil, fmt.Errorf("container %q is not running", ctrName)
	}
	if src.Spec.NodeName == o.TargetNode {
		return nil, fmt.Errorf("pod is already on %s", o.TargetNode)
	}
	srcURL, err := agentURL(ctx, kc, src.Spec.NodeName, o.AgentPort)
	if err != nil {
		return nil, err
	}
	dstURL, err := agentURL(ctx, kc, o.TargetNode, o.AgentPort)
	if err != nil {
		return nil, err
	}

	if !o.SkipMemoryCheck {
		if err := checkMemory(ctx, o.Token, srcURL, dstURL, ctrID, logf); err != nil {
			return nil, err
		}
	}

	res := &Result{MigrationID: time.Now().UTC().Format("20060102-150405") + "-" + rand.String(5),
		SourcePod: src.Name, SourceNode: src.Spec.NodeName, DestPod: nextName(src.Name), DestNode: o.TargetNode}
	key := stage.Key{Namespace: o.Namespace, Pod: res.DestPod, Container: ctrName}.String()
	logf("migration %s: %s/%s (%s) -> %s on %s", res.MigrationID, o.Namespace, src.Name, src.Spec.NodeName, res.DestPod, o.TargetNode)
	defer holdLivePod(ctx, kc, src, res.MigrationID, logf)()

	// 1. Destination pod, parked in its wait init container. Any leftover staging under this key
	// (an earlier run that died before cleanup) goes first, or the wait would pass on stale images.
	if _, err := agentDo(ctx, dstURL, o.Token, http.MethodDelete, "/v1/stage/"+key, nil); err != nil {
		return nil, fmt.Errorf("resetting staging on %s: %w", o.TargetNode, err)
	}
	dst := destinationPod(src, res, o.WaitImage, key, o.AgentPort)
	created, err := pods.Create(ctx, dst, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating destination pod: %w", err)
	}
	destUID := string(created.UID)
	cleanupDest := func() {
		// Fence before deleting: whatever happens to staging, this pod must never restore.
		bg := context.WithoutCancel(ctx)
		agentDo(bg, dstURL, o.Token, http.MethodPost, "/v1/fence/"+destUID+"?key="+key, nil)
		pods.Delete(bg, res.DestPod, metav1.DeleteOptions{})
		agentDo(bg, dstURL, o.Token, http.MethodDelete, "/v1/stage/"+key, nil)
	}
	t0 := time.Now()
	if err := waitPod(ctx, kc, o.Namespace, res.DestPod, 3*time.Minute, func(p *corev1.Pod) (bool, error) {
		for _, s := range p.Status.InitContainerStatuses {
			if s.Name == WaitInitName && s.State.Running != nil {
				return true, nil
			}
			if s.State.Terminated != nil && s.State.Terminated.ExitCode != 0 {
				return false, fmt.Errorf("init container %s failed: %s", s.Name, s.State.Terminated.Message)
			}
		}
		return false, nil
	}); err != nil {
		cleanupDest()
		return nil, fmt.Errorf("destination pod never became ready to receive: %w", err)
	}
	logf("destination pod parked in %s after %s", WaitInitName, time.Since(t0).Round(time.Millisecond))

	// Pull the pod's images on the destination now: kubelet would otherwise pull the main
	// container's image only after the wait, inside the blackout.
	var images []string
	for _, c := range src.Spec.Containers {
		images = append(images, c.Image)
	}
	t1 := time.Now()
	body, _ := json.Marshal(map[string]any{"images": images})
	if _, err := agentDo(ctx, dstURL, o.Token, http.MethodPost, "/v1/pull", bytes.NewReader(body)); err != nil {
		logf("warning: pre-pulling images on %s: %v (kubelet will pull them during the blackout)", o.TargetNode, err)
	} else {
		logf("images present on %s after %s", o.TargetNode, time.Since(t1).Round(time.Millisecond))
	}
	shipURL := dstURL
	if o.TransferNetwork != "" {
		if u, err := transferURL(ctx, dstURL, o.Token, o.TransferNetwork, port(o)); err != nil {
			logf("warning: no %s address on %s (%v); shipping over %s", o.TransferNetwork, o.TargetNode, err, dstURL)
		} else {
			shipURL = u
			logf("shipping checkpoint data via %s", shipURL)
		}
	}

	// 2. Checkpoint on the source, streamed progress.
	req := api.CheckpointRequest{MigrationID: res.MigrationID, Namespace: o.Namespace, Pod: src.Name,
		PodUID: string(src.UID), Container: ctrName, ContainerID: ctrID, DestKey: key, DestURL: shipURL, DestPodUID: destUID, PreDump: o.PreDump, Relay: o.Relay, LazyRestore: o.LazyRestore}
	ck, err := runCheckpoint(ctx, srcURL, o.Token, req, logf)
	res.Checkpoint = ck
	if err != nil {
		if ck == nil || !ck.SourceKilled {
			cleanupDest()
			return res, fmt.Errorf("checkpoint failed, source left running: %w", err)
		}
		return res, fmt.Errorf("checkpoint failed AFTER the source was stopped (the agent retried shipping): %w; "+
			"the final dump is on %s under /var/lib/lcm/ckpt/%s and the pre-dumps on %s under /var/lib/lcm/restore/%s: "+
			"copy the former's images/, rootfs-diff.tar and source-addrs into the latter, touch READY and run lcm recover",
			err, res.SourceNode, res.MigrationID, o.TargetNode, key)
	}

	// 3. Wait for the restored container. If the restore fails, the source is already gone:
	// roll back by restoring the same images in a new pod on the source node.
	rolledBack := false
	if err := waitRestored(ctx, kc, o.Namespace, res.DestPod, ctrName); err != nil {
		logf("restore on %s failed: %v", o.TargetNode, err)
		// kubelet retries failed containers on its own, and a retry may restore fine. Fence the
		// destination pod so no further attempt can start, then make sure none already has:
		// two live copies of one workload must never exist.
		// The fence call waits for any restore in flight (they share a lock) and stops new ones.
		if _, ferr := agentDo(ctx, dstURL, o.Token, http.MethodPost, "/v1/fence/"+destUID+"?key="+key, nil); ferr != nil {
			return res, fmt.Errorf("restore failed and fencing the destination failed too (%v): %w", ferr, err)
		}
		if restoredOK(ctx, dstURL, o.Token, key) {
			// A retry won the race and restored fine: keep it, and lift the fence we just set
			// (it would block this healthy pod's future restarts).
			logf("a kubelet retry restored the container after all; keeping it")
			if _, ferr := agentDo(ctx, dstURL, o.Token, http.MethodDelete, "/v1/fence/"+destUID, nil); ferr != nil {
				logf("warning: lifting the destination fence: %v", ferr)
			}
			goto restored
		}
		rb, rerr := rollback(ctx, kc, o, src, res, ctrName, key, srcURL, dstURL, logf)
		if rerr != nil {
			return res, fmt.Errorf("restore on %s failed (%v) and rollback failed: %w; images are still staged on %s under /var/lib/lcm/restore/%s",
				o.TargetNode, err, rerr, o.TargetNode, key)
		}
		logf("rolled back: workload restored on %s as %s", rb.DestNode, rb.DestPod)
		res.DestPod, res.DestNode, key, dstURL, rolledBack = rb.DestPod, rb.DestNode, rb.key, srcURL, true
		res.RolledBack = true
	}
restored:
	var rr api.RestoreResult
	// The destination agent may be restarting; the result is only needed for the report.
	for attempt := 0; attempt < 15 && res.Restore == nil; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Second)
		}
		if b, err := agentDo(ctx, dstURL, o.Token, http.MethodGet, "/v1/stage/"+key+"/result", nil); err == nil && json.Unmarshal(b, &rr) == nil {
			res.Restore = &rr
			res.Blackout = blackout(ck, &rr)
		}
	}
	logf("restored on %s: restore %dms, relay resume %q, blackout %s", res.DestNode, rr.RestoreMS, rr.RelayResume, res.Blackout.Round(time.Millisecond))

	// 4. Retire the source. Its fence stays (keyed by its UID, garbage-collected by the agent);
	// a graceful delete lets kubelet confirm the sandbox is gone.
	if !rolledBack {
		if err := deletePodAndWait(ctx, kc, o.Namespace, src.Name); err != nil {
			logf("warning: deleting source pod: %v", err)
		}
	}
	for _, call := range []struct{ url, method, path string }{
		{srcURL, http.MethodDelete, "/v1/ckpt/" + res.MigrationID},
		{dstURL, http.MethodDelete, "/v1/stage/" + key},
	} {
		if _, err := agentDo(ctx, call.url, o.Token, call.method, call.path, nil); err != nil {
			logf("warning: cleanup %s %s: %v", call.method, call.path, err)
		}
	}
	if rolledBack {
		return res, fmt.Errorf("restore on %s failed; rolled back, workload is running on %s as %s", o.TargetNode, res.DestNode, res.DestPod)
	}
	return res, nil
}

// blackout is final dump start to restore finished. Node clocks may disagree, so it is summed
// from per-node durations: freeze to READY acknowledged (source clock) plus READY to restored
// (destination clock). Older lcm-runc versions don't report ReadyAt; fall back to the clocks.
func blackout(ck *api.CheckpointResult, rr *api.RestoreResult) time.Duration {
	if ck == nil || ck.FreezeStart.IsZero() {
		return 0
	}
	if !rr.ReadyAt.IsZero() && ck.FrozenMS > 0 {
		return time.Duration(ck.FrozenMS)*time.Millisecond + rr.Finished.Sub(rr.ReadyAt)
	}
	return rr.Finished.Sub(ck.FreezeStart)
}

func deletePodAndWait(ctx context.Context, kc kubernetes.Interface, ns, name string) error {
	pods := kc.CoreV1().Pods(ns)
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := pods.Get(ctx, name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
}

// Recover restores a workload whose migration failed after the source was stopped: the images
// staged on imagesNode under key are restored into a new pod on targetNode, using templatePod
// (normally the fenced source pod, which is deleted) as the spec.
func Recover(ctx context.Context, kc kubernetes.Interface, o Options, imagesNode, key string) (*Result, error) {
	logf := o.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if o.AgentPort == 0 {
		o.AgentPort = api.DefaultPort
	}
	if o.WaitImage == "" {
		o.WaitImage = defaultWaitIm
	}
	src, err := kc.CoreV1().Pods(o.Namespace).Get(ctx, o.Pod, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("template pod: %w", err)
	}
	ctr := o.Container
	if ctr == "" {
		ctr = src.Spec.Containers[0].Name
	}
	imgURL, err := agentURL(ctx, kc, imagesNode, o.AgentPort)
	if err != nil {
		return nil, err
	}
	tgtURL, err := agentURL(ctx, kc, o.TargetNode, o.AgentPort)
	if err != nil {
		return nil, err
	}
	res := &Result{MigrationID: "recover-" + rand.String(5), SourcePod: src.Name, SourceNode: o.TargetNode,
		DestPod: strings.TrimSuffix(key, "_"+ctr)}
	defer holdLivePod(ctx, kc, src, res.MigrationID, logf)()
	res.DestPod = strings.TrimPrefix(res.DestPod, o.Namespace+"_")
	rb, err := rollback(ctx, kc, o, src, res, ctr, key, tgtURL, imgURL, logf)
	if err != nil {
		return res, err
	}
	agentDo(ctx, tgtURL, o.Token, http.MethodDelete, "/v1/stage/"+rb.key, nil)
	res.DestPod, res.DestNode = rb.DestPod, rb.DestNode
	return res, nil
}

type agentInfo struct {
	Addresses     []string `json:"addresses"`
	MemAvailable  int64    `json:"memAvailable"`
	StagingFree   int64    `json:"stagingFree"`
	ContainerAnon int64    `json:"containerAnon"`
}

const memHeadroom = 512 << 20

// checkMemory refuses a migration the nodes can't hold in RAM: checkpoint images are staged on
// tmpfs, outside Kubernetes' accounting. The source holds up to one full pre-dump before it is
// shipped; the destination holds the images and, while restoring, the process itself (about
// twice the workload's memory at peak). Agents too old to report memory are not checked.
func checkMemory(ctx context.Context, token, srcURL, dstURL, ctrID string, logf func(string, ...any)) error {
	get := func(base, q string) (*agentInfo, error) {
		b, err := agentDo(ctx, base, token, http.MethodGet, "/v1/info"+q, nil)
		if err != nil {
			return nil, err
		}
		var i agentInfo
		return &i, json.Unmarshal(b, &i)
	}
	src, err := get(srcURL, "?container="+url.QueryEscape(ctrID))
	if err != nil || src.ContainerAnon <= 0 {
		logf("warning: skipping the memory check (source agent: %v)", err)
		return nil
	}
	dst, err := get(dstURL, "")
	if err != nil || dst.MemAvailable <= 0 {
		logf("warning: skipping the memory check (destination agent: %v)", err)
		return nil
	}
	need := src.ContainerAnon
	var problems []string
	if src.MemAvailable > 0 && src.MemAvailable < need+memHeadroom {
		problems = append(problems, fmt.Sprintf("the source node has %s available, needs ~%s for the first pre-dump", human(src.MemAvailable), human(need+memHeadroom)))
	}
	if dst.MemAvailable < 2*need+memHeadroom {
		problems = append(problems, fmt.Sprintf("the destination node has %s available, needs ~%s (staged images + the restored process)", human(dst.MemAvailable), human(2*need+memHeadroom)))
	}
	if dst.StagingFree > 0 && dst.StagingFree < need+need/10 {
		problems = append(problems, fmt.Sprintf("the destination's staging tmpfs has %s free, needs ~%s (services.lcm.stagingSize)", human(dst.StagingFree), human(need+need/10)))
	}
	if len(problems) > 0 {
		return fmt.Errorf("not enough memory for a %s workload: %s (override with --skip-memory-check)", human(need), strings.Join(problems, "; "))
	}
	logf("memory check: workload %s; source %s available, destination %s available", human(need), human(src.MemAvailable), human(dst.MemAvailable))
	return nil
}

// transferURL asks the agent at agentURL for its addresses and returns one inside cidr.
func transferURL(ctx context.Context, agentURL, token, cidr string, port int) (string, error) {
	pfx, err := netip.ParsePrefix(cidr)
	if err != nil {
		return "", err
	}
	b, err := agentDo(ctx, agentURL, token, http.MethodGet, "/v1/info", nil)
	if err != nil {
		return "", err
	}
	var info struct {
		Addresses []string `json:"addresses"`
	}
	if err := json.Unmarshal(b, &info); err != nil {
		return "", err
	}
	for _, a := range info.Addresses {
		if ip, err := netip.ParseAddr(a); err == nil && pfx.Contains(ip) {
			return fmt.Sprintf("http://%s", netip.AddrPortFrom(ip, uint16(port))), nil
		}
	}
	return "", fmt.Errorf("agent has no address in %s (has %v)", cidr, info.Addresses)
}

// restoredOK reports whether lcm-runc's last restore attempt for key succeeded. Call it after
// fencing the key: the fence waits out a restore in flight, so the answer is final.
func restoredOK(ctx context.Context, url, token, key string) bool {
	b, err := agentDo(ctx, url, token, http.MethodGet, "/v1/stage/"+key+"/result", nil)
	if err != nil {
		return false
	}
	var rr api.RestoreResult
	return json.Unmarshal(b, &rr) == nil && rr.ExitCode == 0 && rr.Error == ""
}

func waitRestored(ctx context.Context, kc kubernetes.Interface, ns, pod, ctr string) error {
	return waitPod(ctx, kc, ns, pod, 2*time.Minute, func(p *corev1.Pod) (bool, error) {
		for _, s := range p.Status.ContainerStatuses {
			if s.Name != ctr {
				continue
			}
			if s.State.Running != nil {
				return true, nil
			}
			if w := s.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
				return false, fmt.Errorf("restored container %s: %s: %s", ctr, w.Reason, w.Message)
			}
			if t := s.LastTerminationState.Terminated; t != nil {
				return false, fmt.Errorf("restored container %s exited: %s (code %d)", ctr, t.Reason, t.ExitCode)
			}
		}
		return false, nil
	})
}

type rollbackResult struct{ DestPod, DestNode, key string }

// rollback restores the images staged on the failed destination into a new pod on the source
// node. The destination holds the only complete copy (the source freed shipped pre-dump pages).
func rollback(ctx context.Context, kc kubernetes.Interface, o Options, src *corev1.Pod, res *Result, ctr, key, srcURL, dstURL string, logf func(string, ...any)) (*rollbackResult, error) {
	pods := kc.CoreV1().Pods(o.Namespace)
	// Both the failed destination and the (dead, fenced) source pod go first: the source still
	// holds its resource requests on the node we are about to restore onto.
	for _, name := range []string{res.DestPod, src.Name} {
		if err := deletePodAndWait(ctx, kc, o.Namespace, name); err != nil {
			return nil, err
		}
	}
	rb := *res
	rb.DestPod, rb.DestNode = nextName(res.DestPod), res.SourceNode
	rbKey := stage.Key{Namespace: o.Namespace, Pod: rb.DestPod, Container: ctr}.String()
	if _, err := agentDo(ctx, srcURL, o.Token, http.MethodDelete, "/v1/stage/"+rbKey, nil); err != nil {
		return nil, fmt.Errorf("resetting staging on %s: %w", res.SourceNode, err)
	}
	if _, err := pods.Create(ctx, destinationPod(src, &rb, o.WaitImage, rbKey, o.AgentPort), metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("creating rollback pod: %w", err)
	}
	if err := waitPod(ctx, kc, o.Namespace, rb.DestPod, 3*time.Minute, func(p *corev1.Pod) (bool, error) {
		for _, s := range p.Status.InitContainerStatuses {
			if s.Name == WaitInitName && s.State.Running != nil {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return nil, fmt.Errorf("rollback pod never parked: %w", err)
	}
	fwdURL := srcURL
	if srcURL == dstURL {
		fwdURL = "" // same node: the agent moves the staging instead of copying it
	}
	body, _ := json.Marshal(map[string]string{"destURL": fwdURL, "destKey": rbKey})
	t0 := time.Now()
	if _, err := agentDo(ctx, dstURL, o.Token, http.MethodPost, "/v1/stage/"+key+"/forward", bytes.NewReader(body)); err != nil {
		return nil, fmt.Errorf("forwarding images back to %s: %w", res.SourceNode, err)
	}
	logf("images forwarded back to %s in %s", res.SourceNode, time.Since(t0).Round(time.Millisecond))
	if err := waitRestored(ctx, kc, o.Namespace, rb.DestPod, ctr); err != nil {
		return nil, err
	}
	agentDo(context.WithoutCancel(ctx), dstURL, o.Token, http.MethodDelete, "/v1/stage/"+key, nil)
	return &rollbackResult{DestPod: rb.DestPod, DestNode: rb.DestNode, key: rbKey}, nil
}

// managedLabels mark pods a GitOps controller would recreate after the migration deletes them.
var managedLabels = []string{"kustomize.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/name", "app.kubernetes.io/managed-by"}

func checkMigratable(p *corev1.Pod) error {
	for _, l := range managedLabels {
		if v, ok := p.Labels[l]; ok && (l != "app.kubernetes.io/managed-by" || v == "Helm") {
			return fmt.Errorf("pod carries %s=%s: its GitOps controller would recreate it after the migration (two servers on one world); manage it with something migration-aware, or suspend reconciliation and pass --allow-managed", l, v)
		}
	}
	if p.Spec.RuntimeClassName == nil || *p.Spec.RuntimeClassName != RuntimeClass {
		return fmt.Errorf("pod must use runtimeClassName %q (it fences the source and restores the destination)", RuntimeClass)
	}
	if p.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("pod is %s, not Running", p.Status.Phase)
	}
	for _, o := range p.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			return fmt.Errorf("pod is controlled by %s %s; its controller would fight the migration", o.Kind, o.Name)
		}
	}
	for _, v := range p.Spec.Volumes {
		if v.HostPath != nil {
			return fmt.Errorf("volume %q is a hostPath: its contents don't exist on other nodes", v.Name)
		}
	}
	return nil
}

var genSuffix = regexp.MustCompile(`^(.*)-m(\d+)$`)

// nextName gives the destination pod a generation suffix: mc -> mc-m1 -> mc-m2 ...
func nextName(name string) string {
	if m := genSuffix.FindStringSubmatch(name); m != nil {
		n, _ := strconv.Atoi(m[2])
		return fmt.Sprintf("%s-m%d", m[1], n+1)
	}
	return name + "-m1"
}

func destinationPod(src *corev1.Pod, res *Result, waitImage, key string, port int) *corev1.Pod {
	spec := *src.Spec.DeepCopy()
	spec.NodeName = res.DestNode
	// The restored processes keep their UTS namespace view; give the new pod the same hostname.
	if spec.Hostname == "" {
		spec.Hostname = src.Name
	}
	// Drop the previous hop's wait container; it gets a fresh one with this migration's key.
	inits := spec.InitContainers[:0]
	for _, c := range spec.InitContainers {
		if c.Name != WaitInitName {
			inits = append(inits, c)
		}
	}
	spec.InitContainers = inits
	waitURL := fmt.Sprintf("http://$(HOST_IP):%d/v1/stage/%s/wait?timeout=50s", port, key)
	spec.InitContainers = append(spec.InitContainers, corev1.Container{
		Name:    WaitInitName,
		Image:   waitImage,
		Command: []string{"sh", "-c", `until wget -q -O /dev/null "$WAIT_URL"; do sleep 0.2; done`},
		Env: []corev1.EnvVar{
			{Name: "HOST_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
			{Name: "WAIT_URL", Value: waitURL},
		},
		Resources: corev1.ResourceRequirements{},
	})
	labels := map[string]string{}
	for k, v := range src.Labels {
		labels[k] = v
	}
	labels[LabelFrom] = src.Name
	ann := map[string]string{}
	for k, v := range src.Annotations {
		if k != "kubectl.kubernetes.io/last-applied-configuration" {
			ann[k] = v
		}
	}
	ann[AnnMigration] = res.MigrationID
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: res.DestPod, Namespace: src.Namespace, Labels: labels, Annotations: ann},
		Spec:       spec,
	}
}

func agentURL(ctx context.Context, kc kubernetes.Interface, node string, port int) (string, error) {
	n, err := kc.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return fmt.Sprintf("http://%s:%d", a.Address, port), nil
		}
	}
	return "", fmt.Errorf("node %s has no InternalIP", node)
}

func runCheckpoint(ctx context.Context, url, token string, req api.CheckpointRequest, logf func(string, ...any)) (*api.CheckpointResult, error) {
	body, _ := json.Marshal(req)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/checkpoint", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("source agent: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var last *api.CheckpointResult
	for sc.Scan() {
		var ev api.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}
		switch ev.Phase {
		case "done":
			return ev.Result, nil
		case "error":
			return ev.Result, fmt.Errorf("%s", ev.Message)
		case "dump":
			last = &api.CheckpointResult{SourceKilled: true}
			fallthrough
		default:
			logf("  %-8s round=%d bytes=%s %dms %s", ev.Phase, ev.Round, human(ev.Bytes), ev.Millis, ev.Message)
		}
	}
	// The stream broke (network, agent restart, ...). After the fence the agent carries on
	// regardless, so ask it how things ended instead of guessing.
	logf("  progress stream lost (%v); polling the source agent for the outcome", sc.Err())
	return pollCheckpoint(ctx, url, token, req.MigrationID, last)
}

func pollCheckpoint(ctx context.Context, url, token, id string, last *api.CheckpointResult) (*api.CheckpointResult, error) {
	var st struct {
		Done   bool                  `json:"done"`
		Error  string                `json:"error"`
		Result *api.CheckpointResult `json:"result"`
	}
	var unknownSince time.Time
	err := wait.PollUntilContextTimeout(context.WithoutCancel(ctx), time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
		b, err := agentDo(ctx, url, token, http.MethodGet, "/v1/checkpoint/"+id, nil)
		if err != nil {
			// Unreachable: keep trying. Reachable but "unknown migration" for a while: a restarted
			// agent found nothing to recover, so there is nothing left to wait for.
			if strings.Contains(err.Error(), "404") {
				if unknownSince.IsZero() {
					unknownSince = time.Now()
				} else if time.Since(unknownSince) > 30*time.Second {
					return false, fmt.Errorf("the source agent no longer knows migration %s (it restarted and had nothing to resume)", id)
				}
			}
			return false, nil
		}
		unknownSince = time.Time{}
		return json.Unmarshal(b, &st) == nil && st.Done, nil
	})
	if err != nil {
		if !wait.Interrupted(err) {
			return last, err
		}
		return last, fmt.Errorf("lost track of the checkpoint (source agent unreachable): its state is unknown; check `journalctl -u lcm-agent` on the source node")
	}
	if st.Result == nil {
		st.Result = last
	}
	if st.Error != "" {
		return st.Result, fmt.Errorf("%s", st.Error)
	}
	return st.Result, nil
}

func agentDo(ctx context.Context, url, token, method, path string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func waitPod(ctx context.Context, kc kubernetes.Interface, ns, name string, timeout time.Duration, cond func(*corev1.Pod) (bool, error)) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		p, err := kc.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return cond(p)
	})
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0fK", float64(n)/(1<<10))
	}
	return strconv.FormatInt(n, 10)
}

func ptr[T any](v T) *T { return &v }
