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
	"errors"
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
	// of the node's InternalIP, e.g. a LAN faster than the overlay the InternalIPs are on. Only
	// over the agents' encrypted transport (lcm-agent -tls-listen) unless TransferPlaintext;
	// without it, data goes over the InternalIPs.
	TransferNetwork   string
	TransferPlaintext bool
	LazyRestore       bool // experimental: CRIU lazy pages on the destination
	// SkipMemoryCheck skips the pre-flight check that both nodes have the RAM for the staged
	// images (see checkMemory).
	SkipMemoryCheck bool
	// AllowVersionSkew migrates even when an agent predates compression negotiation (see checkVersions).
	AllowVersionSkew bool
	Log              func(format string, a ...any)
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

	if err := checkVersions(ctx, o.Token, srcURL, dstURL, logf); err != nil && !o.AllowVersionSkew {
		return nil, err
	} else if err != nil {
		logf("warning: %v", err)
	}
	if !o.SkipMemoryCheck {
		if err := checkMemory(ctx, o.Token, srcURL, dstURL, ctrID, logf); err != nil {
			return nil, err
		}
	}
	if short, err := checkRequests(ctx, kc, src, o.TargetNode); err != nil {
		logf("warning: skipping the resource-request check: %v", err)
	} else if short != "" {
		return nil, errors.New(short)
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
		if err := refused(p); err != nil {
			return false, err
		}
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
	// Private images: send the pod's pull credentials along (kubelet would use them, but only
	// once the blackout has started).
	auths, warns := pullAuths(ctx, kc, src, images)
	for _, w := range warns {
		logf("warning: %s (pre-pulling without it)", w)
	}
	body, _ := json.Marshal(map[string]any{"images": images, "auth": auths})
	if _, err := agentDo(ctx, dstURL, o.Token, http.MethodPost, "/v1/pull", bytes.NewReader(body)); err != nil {
		logf("warning: pre-pulling images on %s: %v (kubelet will pull them during the blackout)", o.TargetNode, err)
	} else {
		logf("images present on %s after %s", o.TargetNode, time.Since(t1).Round(time.Millisecond))
	}
	shipTo := shipURL(ctx, o, srcURL, dstURL, o.TargetNode, logf)

	runHook(ctx, srcURL, o.Token, src, PreMigrateAnnotation, ctrID,
		map[string]string{"LCM_SOURCE_NODE": res.SourceNode, "LCM_TARGET_NODE": o.TargetNode}, logf)

	// 2. Checkpoint on the source, streamed progress.
	req := api.CheckpointRequest{MigrationID: res.MigrationID, Namespace: o.Namespace, Pod: src.Name,
		PodUID: string(src.UID), Container: ctrName, ContainerID: ctrID, DestKey: key, DestURL: shipTo, DestPodUID: destUID, PreDump: o.PreDump, Relay: o.Relay, LazyRestore: o.LazyRestore}
	if shipTo != dstURL {
		req.DestFallbackURL = dstURL // if the source can't reach the transfer address
	}
	ck, err := runCheckpoint(ctx, srcURL, o.Token, req, logf)
	res.Checkpoint = ck
	if err != nil {
		if ck == nil || !ck.SourceKilled {
			cleanupDest()
			runHook(ctx, srcURL, o.Token, src, PostMigrateAnnotation, ctrID,
				map[string]string{"LCM_SOURCE_NODE": res.SourceNode, "LCM_TARGET_NODE": res.SourceNode,
					"LCM_BLACKOUT": "0s", "LCM_RESULT": "aborted"}, logf)
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
	staged := time.Now() // the checkpoint stream ends once the destination acknowledged READY
	var rollbackWait time.Duration
	if err := waitRestored(ctx, kc, o.Namespace, res.DestPod, ctrName, func(ctx context.Context) *api.RestoreResult {
		return restoreResult(ctx, dstURL, o.Token, key)
	}, logf); err != nil {
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
		rollbackWait = time.Since(staged)
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
	if rolledBack && ck != nil && ck.FrozenMS > 0 {
		// The rollback's own READY-to-restored misses the failed attempt, the fencing, the new pod
		// and the forward: freeze to READY (source clock) plus READY to rolled back (this clock).
		res.Blackout = time.Duration(ck.FrozenMS)*time.Millisecond + rollbackWait
	}
	logf("restored on %s: restore %dms, relay resume %q, blackout %s", res.DestNode, rr.RestoreMS, rr.RelayResume, res.Blackout.Round(time.Millisecond))
	result := "done"
	if rolledBack {
		result = "rolled_back"
	}
	runHook(ctx, dstURL, o.Token, src, PostMigrateAnnotation, containerID(ctx, kc, o.Namespace, res.DestPod, ctrName),
		map[string]string{"LCM_SOURCE_NODE": res.SourceNode, "LCM_TARGET_NODE": res.DestNode,
			"LCM_BLACKOUT": res.Blackout.Round(100 * time.Millisecond).String(), "LCM_RESULT": result}, logf)

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

// checkVersions refuses a migration involving an agent from before compression negotiation and
// the source's staging-space check (its /v1/info lists no "encodings"): with one, shipping fails
// in either direction, or a final dump can overflow the destination's staging after the workload
// has stopped. Agent versions that merely differ are fine and only logged.
func checkVersions(ctx context.Context, token, srcURL, dstURL string, logf func(string, ...any)) error {
	get := func(base string) (version string, current bool, err error) {
		b, err := agentDo(ctx, base, token, http.MethodGet, "/v1/info", nil)
		if err != nil {
			if strings.HasPrefix(err.Error(), "404") {
				return "old", false, nil
			}
			return "", false, err
		}
		var i struct {
			Version   string   `json:"version"`
			Encodings []string `json:"encodings"`
		}
		json.Unmarshal(b, &i)
		return i.Version, i.Encodings != nil, nil
	}
	sv, scur, err := get(srcURL)
	if err != nil {
		return fmt.Errorf("source agent: %w", err)
	}
	dv, dcur, err := get(dstURL)
	if err != nil {
		return fmt.Errorf("destination agent: %w", err)
	}
	if !scur || !dcur {
		return fmt.Errorf("agent version skew (source %s, destination %s): an agent this old can strand the workload mid-migration; upgrade every node first (or pass --allow-version-skew)", sv, dv)
	}
	if sv != dv {
		logf("note: agent versions differ (source %s, destination %s)", sv, dv)
	}
	return nil
}

type agentInfo struct {
	Addresses     []string `json:"addresses"`
	MemAvailable  int64    `json:"memAvailable"`
	StagingFree   int64    `json:"stagingFree"`
	ContainerAnon int64    `json:"containerAnon"`
	MemTotal      int64    `json:"memTotal"`
}

// srcHeadroom is what the source keeps free while it holds a pre-dump until it's shipped.
const srcHeadroom = 512 << 20

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
	if src.MemAvailable > 0 && src.MemAvailable < need+srcHeadroom {
		problems = append(problems, fmt.Sprintf("the source node has %s available, needs ~%s for the first pre-dump", human(src.MemAvailable), human(need+srcHeadroom)))
	}
	// The same headroom the source agent insists on before it freezes the workload: checking
	// less here only wastes a whole pre-copy before the agent refuses.
	if dstNeed := 2*need + api.MemHeadroom(dst.MemTotal); dst.MemAvailable < dstNeed {
		problems = append(problems, fmt.Sprintf("the destination node has %s available, needs ~%s (staged images + the restored process + headroom)", human(dst.MemAvailable), human(dstNeed)))
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

// shipURL is where the agent at fromURL sends checkpoint data for the agent at toURL (on node):
// toURL's address in o.TransferNetwork when there is a usable one (see transferURL), otherwise
// toURL itself.
func shipURL(ctx context.Context, o Options, fromURL, toURL, node string, logf func(string, ...any)) string {
	if o.TransferNetwork == "" {
		return toURL
	}
	u, err := transferURL(ctx, fromURL, toURL, o.Token, o.TransferNetwork, port(o), o.TransferPlaintext)
	if err != nil {
		logf("warning: not shipping over %s to %s (%v); shipping over %s", o.TransferNetwork, node, err, toURL)
		return toURL
	}
	logf("shipping checkpoint data to %s via %s", node, u)
	return u
}

// transferURL asks the agent at toURL for its addresses and returns one inside cidr: https on its
// encrypted transport port, or, if either agent lacks the transport and plaintext is allowed,
// plain http on port. The answers come over the InternalIPs, so someone on the transfer network
// can't strip the TLS port from them to force plaintext.
func transferURL(ctx context.Context, fromURL, toURL, token, cidr string, port int, plaintext bool) (string, error) {
	pfx, err := netip.ParsePrefix(cidr)
	if err != nil {
		return "", err
	}
	type info struct {
		Addresses []string `json:"addresses"`
		TLSPort   int      `json:"tlsPort"`
	}
	get := func(url string) (info, error) {
		var in info
		b, err := agentDo(ctx, url, token, http.MethodGet, "/v1/info", nil)
		if err == nil {
			err = json.Unmarshal(b, &in)
		}
		return in, err
	}
	to, err := get(toURL)
	if err != nil {
		return "", err
	}
	tlsOK := to.TLSPort > 0 && to.TLSPort < 1<<16
	if tlsOK {
		// The sender must speak the transport too: an agent serving it also dials it, one from
		// before it would fail every handshake.
		from, err := get(fromURL)
		if err != nil {
			return "", err
		}
		tlsOK = from.TLSPort > 0
	}
	for _, a := range to.Addresses {
		ip, err := netip.ParseAddr(a)
		if err != nil || !pfx.Contains(ip) {
			continue
		}
		switch {
		case tlsOK:
			return fmt.Sprintf("https://%s", netip.AddrPortFrom(ip, uint16(to.TLSPort))), nil
		case plaintext:
			return fmt.Sprintf("http://%s", netip.AddrPortFrom(ip, uint16(port))), nil
		default:
			return "", fmt.Errorf("the agents don't both serve the encrypted transport (lcm-agent -tls-listen) and --transfer-plaintext is not set")
		}
	}
	return "", fmt.Errorf("agent has no address in %s (has %v)", cidr, to.Addresses)
}

// restoredOK reports whether lcm-runc's last restore attempt for key succeeded. Call it after
// fencing the key: the fence waits out a restore in flight, so the answer is final.
func restoredOK(ctx context.Context, url, token, key string) bool {
	rr := restoreResult(ctx, url, token, key)
	return rr != nil && rr.ExitCode == 0 && rr.Error == ""
}

// restoreResult is lcm-runc's last restore result for key: nil if none was attempted since the
// staging was reset (or the agent doesn't answer).
func restoreResult(ctx context.Context, url, token, key string) *api.RestoreResult {
	b, err := agentDo(ctx, url, token, http.MethodGet, "/v1/stage/"+key+"/result", nil)
	if err != nil {
		return nil
	}
	var rr api.RestoreResult
	if json.Unmarshal(b, &rr) != nil {
		return nil
	}
	return &rr
}

const (
	// restoreWait bounds the wait for the restored container once the images are staged.
	restoreWait = 2 * time.Minute
	// restoreRetryWait bounds it when an earlier attempt failed before they were (see
	// checkRestore): kubelet's back-off between attempts grows to 5 min.
	restoreRetryWait = 6 * time.Minute
)

// waitRestored waits for the restored container ctr of pod to run, once its images are staged
// (READY); result fetches lcm-runc's last restore result for it (see restoreResult).
func waitRestored(ctx context.Context, kc kubernetes.Interface, ns, pod, ctr string, result func(context.Context) *api.RestoreResult, logf func(string, ...any)) error {
	t0 := time.Now()
	deadline := t0.Add(restoreWait)
	var why error // a start failure kubelet's retry may make good
	return wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		if p, err := kc.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{}); err == nil {
			state, err := checkRestore(p, ctr, func() *api.RestoreResult {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // a dead agent mustn't stall the wait
				defer cancel()
				return result(ctx)
			})
			switch state {
			case restoreRunning:
				return true, nil
			case restoreFailed:
				return false, err
			}
			if err != nil {
				if why == nil {
					logf("%v; no restore attempted yet (an attempt gave up before the images were staged?): waiting up to %s for kubelet's retry", err, restoreRetryWait)
				}
				why, deadline = err, t0.Add(restoreRetryWait) // wait out kubelet's back-off
			}
		}
		if time.Now().After(deadline) {
			if why != nil {
				return false, fmt.Errorf("%w; no restore attempted within %s of the images being staged", why, restoreRetryWait)
			}
			return false, fmt.Errorf("restored container %s not running %s after the images were staged", ctr, restoreWait)
		}
		return false, nil
	})
}

type restoreState int

const (
	restorePending restoreState = iota // being created or restored, or waiting for kubelet's retry
	restoreRunning
	restoreFailed
)

// checkRestore reads one look at the restored container ctr. A container that failed to start is
// a failed restore only if a restore was attempted: lcm-runc waits for READY only so long (it runs
// inside one CRI call, see cmd/lcm-runc), and an attempt that gave up before the images were
// staged writes no result and leaves the container failed to start (containerd's StartError) and
// kubelet backing off. Kubelet's next attempt restores the images, so until there is a result such
// a failure is pending (and returned as why). A failed restore (a CRIU error) writes a result:
// that, or any other failure, is final at once. result fetches lcm-runc's last result.
func checkRestore(p *corev1.Pod, ctr string, result func() *api.RestoreResult) (restoreState, error) {
	for _, s := range p.Status.ContainerStatuses {
		if s.Name != ctr {
			continue
		}
		if s.State.Running != nil {
			return restoreRunning, nil
		}
		var why error
		retried := p.Spec.RestartPolicy != corev1.RestartPolicyNever
		if w := s.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
			why = fmt.Errorf("restored container %s: %s: %s", ctr, w.Reason, w.Message)
			retried = retried && (w.Reason == "CrashLoopBackOff" || w.Reason == "RunContainerError")
		}
		if t := s.LastTerminationState.Terminated; t != nil {
			if why == nil {
				why = fmt.Errorf("restored container %s exited: %s (code %d)", ctr, t.Reason, t.ExitCode)
			}
			retried = retried && t.Reason == "StartError"
		}
		switch {
		case why == nil:
			return restorePending, nil
		case !retried:
			return restoreFailed, why
		}
		if rr := result(); rr != nil && (rr.ExitCode != 0 || rr.Error != "") {
			return restoreFailed, fmt.Errorf("%w; restore: %s", why, rr.Error)
		}
		// No attempt since READY yet, one in flight, or one that succeeded and kubelet hasn't
		// reported it running yet.
		return restorePending, why
	}
	return restorePending, nil
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
		if err := refused(p); err != nil {
			return false, err
		}
		for _, s := range p.Status.InitContainerStatuses {
			if s.Name == WaitInitName && s.State.Running != nil {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return nil, fmt.Errorf("rollback pod never parked: %w", err)
	}
	fwdURL := "" // same node: the agent moves the staging instead of copying it
	if srcURL != dstURL {
		fwdURL = shipURL(ctx, o, dstURL, srcURL, res.SourceNode, logf)
	}
	forward := func(to string) error {
		body, _ := json.Marshal(map[string]string{"destURL": to, "destKey": rbKey})
		_, err := agentDo(ctx, dstURL, o.Token, http.MethodPost, "/v1/stage/"+key+"/forward", bytes.NewReader(body))
		return err
	}
	t0 := time.Now()
	err := forward(fwdURL)
	if err != nil && fwdURL != "" && fwdURL != srcURL {
		// The workload is down: don't let the transfer network be what keeps it down.
		logf("warning: forwarding via %s failed (%v); retrying over %s", fwdURL, err, srcURL)
		err = forward(srcURL)
	}
	if err != nil {
		return nil, fmt.Errorf("forwarding images back to %s: %w", res.SourceNode, err)
	}
	logf("images forwarded back to %s in %s", res.SourceNode, time.Since(t0).Round(time.Millisecond))
	if err := waitRestored(ctx, kc, o.Namespace, rb.DestPod, ctr, func(ctx context.Context) *api.RestoreResult {
		return restoreResult(ctx, srcURL, o.Token, rbKey)
	}, logf); err != nil {
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

// AgentURL is the lcm-agent address on node (its InternalIP).
func AgentURL(ctx context.Context, kc kubernetes.Interface, node string, port int) (string, error) {
	return agentURL(ctx, kc, node, port)
}
