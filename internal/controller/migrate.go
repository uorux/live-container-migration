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
	Log                                   func(format string, a ...any)
}

type Result struct {
	MigrationID, SourcePod, SourceNode, DestPod, DestNode string
	Checkpoint                                            *api.CheckpointResult
	Restore                                               *api.RestoreResult
	// Blackout is how long the workload was not running: final dump start to restore finished.
	Blackout time.Duration
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
	if err := checkMigratable(src); err != nil {
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

	res := &Result{MigrationID: time.Now().UTC().Format("20060102-150405") + "-" + rand.String(5),
		SourcePod: src.Name, SourceNode: src.Spec.NodeName, DestPod: nextName(src.Name), DestNode: o.TargetNode}
	key := stage.Key{Namespace: o.Namespace, Pod: res.DestPod, Container: ctrName}.String()
	logf("migration %s: %s/%s (%s) -> %s on %s", res.MigrationID, o.Namespace, src.Name, src.Spec.NodeName, res.DestPod, o.TargetNode)

	// 1. Destination pod, parked in its wait init container.
	dst := destinationPod(src, res, o.WaitImage, key, o.AgentPort)
	if _, err := pods.Create(ctx, dst, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("creating destination pod: %w", err)
	}
	cleanupDest := func() {
		pods.Delete(context.WithoutCancel(ctx), res.DestPod, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))})
		agentDo(context.WithoutCancel(ctx), dstURL, o.Token, http.MethodDelete, "/v1/stage/"+key, nil)
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

	// 2. Checkpoint on the source, streamed progress.
	req := api.CheckpointRequest{MigrationID: res.MigrationID, Namespace: o.Namespace, Pod: src.Name,
		Container: ctrName, ContainerID: ctrID, DestKey: key, DestURL: dstURL, PreDump: o.PreDump, Relay: o.Relay}
	ck, err := runCheckpoint(ctx, srcURL, o.Token, req, logf)
	res.Checkpoint = ck
	if err != nil {
		if ck == nil || !ck.SourceKilled {
			cleanupDest()
			return res, fmt.Errorf("checkpoint failed, source left running: %w", err)
		}
		return res, fmt.Errorf("checkpoint failed AFTER the source was stopped; its images are on %s under "+
			"/var/lib/lcm/ckpt/%s and can be restored there: %w", res.SourceNode, res.MigrationID, err)
	}

	// 3. Wait for the restored container.
	if err := waitPod(ctx, kc, o.Namespace, res.DestPod, 2*time.Minute, func(p *corev1.Pod) (bool, error) {
		for _, s := range p.Status.ContainerStatuses {
			if s.Name != ctrName {
				continue
			}
			if s.State.Running != nil {
				return true, nil
			}
			if w := s.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
				return false, fmt.Errorf("restored container %s: %s: %s", ctrName, w.Reason, w.Message)
			}
		}
		return false, nil
	}); err != nil {
		return res, fmt.Errorf("restore on %s failed (source images kept on %s under /var/lib/lcm/ckpt/%s): %w",
			o.TargetNode, res.SourceNode, res.MigrationID, err)
	}
	var rr api.RestoreResult
	if b, err := agentDo(ctx, dstURL, o.Token, http.MethodGet, "/v1/stage/"+key+"/result", nil); err == nil {
		if json.Unmarshal(b, &rr) == nil {
			res.Restore = &rr
			res.Blackout = rr.Finished.Sub(ck.FreezeStart)
		}
	}
	logf("restored on %s: restore %dms, relay resume %q, blackout %s", o.TargetNode, rr.RestoreMS, rr.RelayResume, res.Blackout.Round(time.Millisecond))

	// 4. Retire the source.
	if err := pods.Delete(ctx, src.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))}); err != nil && !apierrors.IsNotFound(err) {
		logf("warning: deleting source pod: %v", err)
	}
	wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := pods.Get(ctx, src.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	for _, call := range []struct{ url, method, path string }{
		{srcURL, http.MethodDelete, "/v1/fence/" + o.Namespace + "/" + src.Name},
		{srcURL, http.MethodDelete, "/v1/ckpt/" + res.MigrationID},
		{dstURL, http.MethodDelete, "/v1/stage/" + key},
	} {
		if _, err := agentDo(ctx, call.url, o.Token, call.method, call.path, nil); err != nil {
			logf("warning: cleanup %s %s: %v", call.method, call.path, err)
		}
	}
	return res, nil
}

func checkMigratable(p *corev1.Pod) error {
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
	return last, fmt.Errorf("source agent stream ended without a result: %v", sc.Err())
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
