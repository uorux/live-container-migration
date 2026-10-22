package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

// PickTarget chooses a destination node for a workload leaving `from`: a Ready, schedulable node
// other than `from` whose agent answers, with the most available memory.
func PickTarget(ctx context.Context, kc kubernetes.Interface, o Options, from string) (string, error) {
	nodes, err := kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	port := o.AgentPort
	if port == 0 {
		port = 7790
	}
	type cand struct {
		name  string
		avail int64
	}
	var cands []cand
	var skipped []string
	for _, n := range nodes.Items {
		if n.Name == from || n.Spec.Unschedulable || !nodeReady(&n) {
			continue
		}
		url, err := agentURL(ctx, kc, n.Name, port)
		if err != nil {
			skipped = append(skipped, n.Name+": "+err.Error())
			continue
		}
		b, err := agentDo(ctx, url, o.Token, http.MethodGet, "/v1/info", nil)
		if err != nil {
			skipped = append(skipped, n.Name+": agent: "+err.Error())
			continue
		}
		var info agentInfo
		json.Unmarshal(b, &info)
		cands = append(cands, cand{n.Name, info.MemAvailable})
	}
	if len(cands) == 0 {
		return "", fmt.Errorf("no ready, schedulable node with a reachable agent besides %s (%v)", from, skipped)
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].avail > cands[j].avail })
	return cands[0].name, nil
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// DrainCandidates lists the migratable pods on node: runtimeClassName lcm, Running and Ready
// (a destination parked mid-migration isn't Ready), not being deleted.
func DrainCandidates(ctx context.Context, kc kubernetes.Interface, node string) ([]corev1.Pod, error) {
	pods, err := kc.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(),
	})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range pods.Items {
		if p.Spec.RuntimeClassName == nil || *p.Spec.RuntimeClassName != "lcm" {
			continue
		}
		if p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || !podReady(&p) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}
