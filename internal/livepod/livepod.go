// Package livepod implements the LivePod controller: a LivePod (lcm.rooty.dev/v1alpha1) holds a
// pod template, and the controller keeps exactly one pod for it in existence. It is the
// migration-friendly replacement for a StatefulSet of one: migrations create and delete the pods
// themselves (the controller only acts when there is no pod at all), and GitOps tools manage the
// LivePod object instead of the pod.
//
// Safety rule (as for StatefulSets): a new pod is created only when no pod for the LivePod exists
// at all, including Terminating ones and ones on unreachable nodes, so a partitioned node's copy
// can't overlap a fresh one.
package livepod

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"lcm/internal/controller"
)

// Label put on every pod of a LivePod (migrations copy it to the destination pod).
const Label = controller.LivePodLabel

// PausedAnnotation on a LivePod ("true") stops the controller from creating pods for it.
const PausedAnnotation = "lcm.rooty.dev/paused"

var GVR = schema.GroupVersionResource{Group: "lcm.rooty.dev", Version: "v1alpha1", Resource: "livepods"}

type Controller struct {
	Kube     kubernetes.Interface
	Dyn      dynamic.Interface
	Log      *slog.Logger
	Interval time.Duration

	mu      sync.Mutex
	leading bool
	state   map[string]Status // namespace/name -> last reconciled status (leader only)
}

// Status is what the controller last saw for one LivePod.
type Status struct {
	Namespace, Name, Pod, Node, Phase string
}

// Snapshot returns whether this agent leads and, if so, the LivePods' last statuses (for metrics).
func (c *Controller) Snapshot() (bool, []Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Status, 0, len(c.state))
	for _, s := range c.state {
		out = append(out, s)
	}
	return c.leading, out
}

// Run competes for leadership (Lease kube-system/lcm-livepod-controller) and reconciles while
// leader. Every node's agent runs it; one acts.
func (c *Controller) Run(ctx context.Context, identity string) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "lcm-livepod-controller", Namespace: "kube-system"},
		Client:     c.Kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   30 * time.Second,
		RenewDeadline:   20 * time.Second,
		RetryPeriod:     5 * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.Log.Info("livepod controller: leading")
				c.mu.Lock()
				c.leading = true
				c.mu.Unlock()
				c.loop(ctx)
			},
			OnStoppedLeading: func() {
				c.Log.Info("livepod controller: lost leadership")
				c.mu.Lock()
				c.leading, c.state = false, nil
				c.mu.Unlock()
			},
		},
	})
}

func (c *Controller) loop(ctx context.Context) {
	interval := c.Interval
	if interval == 0 {
		interval = 5 * time.Second
	}
	for {
		if err := c.reconcileAll(ctx); err != nil {
			c.Log.Warn("livepod reconcile", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (c *Controller) reconcileAll(ctx context.Context) error {
	list, err := c.Dyn.Resource(GVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // CRD not installed
		}
		return err
	}
	present := map[string]bool{}
	for i := range list.Items {
		present[list.Items[i].GetNamespace()+"/"+list.Items[i].GetName()] = true
		if err := c.reconcile(ctx, &list.Items[i]); err != nil {
			c.Log.Warn("livepod", "namespace", list.Items[i].GetNamespace(), "name", list.Items[i].GetName(), "err", err)
		}
	}
	c.mu.Lock()
	for k := range c.state {
		if !present[k] {
			delete(c.state, k) // LivePod deleted
		}
	}
	c.mu.Unlock()
	return nil
}

// Reconcile one LivePod: if no pod exists for it (in any state short of fully deleted), create one
// from the template. Report the current pod in status.
func (c *Controller) reconcile(ctx context.Context, lp *unstructured.Unstructured) error {
	ns, name := lp.GetNamespace(), lp.GetName()
	pods, err := c.Kube.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: Label + "=" + name})
	if err != nil {
		return err
	}
	c.retireStaleSources(ctx, pods.Items)
	var current *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		// Any pod still present blocks creation, even a Terminating one or one on a dead node.
		if current == nil || (p.Status.Phase == corev1.PodRunning && current.Status.Phase != corev1.PodRunning) {
			current = p
		}
	}
	if current != nil {
		return c.setStatus(ctx, lp, current.Name, current.Spec.NodeName, string(current.Status.Phase), len(pods.Items))
	}
	if lp.GetAnnotations()[PausedAnnotation] == "true" {
		return c.setStatus(ctx, lp, "", "", "Paused", 0)
	}
	// A migration's rollback deletes the dead source before creating the restore pod; it holds
	// a Lease meanwhile so no cold copy is started next to the one being restored.
	if held, err := controller.LivePodHeld(ctx, c.Kube, ns, name); held || err != nil {
		return c.setStatus(ctx, lp, "", "", "Migrating", 0)
	}
	pod, err := podFromTemplate(lp)
	if err != nil {
		return err
	}
	created, err := c.Kube.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating pod: %w", err)
	}
	c.Log.Info("livepod: created pod (no pod existed)", "namespace", ns, "livepod", name, "pod", created.Name)
	return c.setStatus(ctx, lp, created.Name, "", "Created", 1)
}

// staleAfter is how long a migrated-away source may linger after its successor is ready before
// the controller deletes it (normally the migration deletes it within seconds).
const staleAfter = 2 * time.Minute

// retireStaleSources deletes source pods a migration left behind (its CLI died before cleanup):
// a pod whose successor (a pod labelled lcm.rooty.dev/migrated-from=<it>) is Ready, while it
// itself has not been Ready for staleAfter. Its container is fenced and can never run again; it
// only blocks `lcm migrate livepod/...` and holds resources.
func (c *Controller) retireStaleSources(ctx context.Context, pods []corev1.Pod) {
	byName := map[string]*corev1.Pod{}
	for i := range pods {
		byName[pods[i].Name] = &pods[i]
	}
	for i := range pods {
		succ := &pods[i]
		src := byName[succ.Labels[controller.LabelFrom]]
		if src == nil || src.DeletionTimestamp != nil || !ready(succ) || ready(src) || time.Since(notReadySince(src)) < staleAfter {
			continue
		}
		c.Log.Info("livepod: deleting a migrated-away source pod left behind", "namespace", src.Namespace, "pod", src.Name, "successor", succ.Name)
		if err := c.Kube.CoreV1().Pods(src.Namespace).Delete(ctx, src.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			c.Log.Warn("livepod: deleting stale source", "pod", src.Name, "err", err)
		}
	}
}

func ready(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func notReadySince(p *corev1.Pod) time.Time {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status != corev1.ConditionTrue {
			return c.LastTransitionTime.Time
		}
	}
	return time.Now() // no Ready condition yet: treat as just changed
}

func podFromTemplate(lp *unstructured.Unstructured) (*corev1.Pod, error) {
	tmpl, found, err := unstructured.NestedMap(lp.Object, "spec", "template")
	if err != nil || !found {
		return nil, fmt.Errorf("spec.template missing")
	}
	b, err := json.Marshal(tmpl)
	if err != nil {
		return nil, err
	}
	var t corev1.PodTemplateSpec
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("spec.template: %w", err)
	}
	labels := map[string]string{}
	for k, v := range t.Labels {
		labels[k] = v
	}
	labels[Label] = lp.GetName()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: lp.GetName() + "-",
			Namespace:    lp.GetNamespace(),
			Labels:       labels,
			Annotations:  t.Annotations,
		},
		Spec: t.Spec,
	}
	// Pods are deliberately not owned by the LivePod: an owner reference would make the
	// migration CLI refuse them, and garbage collection must never delete a running server.
	return pod, nil
}

func (c *Controller) setStatus(ctx context.Context, lp *unstructured.Unstructured, pod, node, phase string, pods int) error {
	c.mu.Lock()
	if c.state == nil {
		c.state = map[string]Status{}
	}
	c.state[lp.GetNamespace()+"/"+lp.GetName()] = Status{lp.GetNamespace(), lp.GetName(), pod, node, phase}
	c.mu.Unlock()
	cur, _, _ := unstructured.NestedMap(lp.Object, "status")
	want := map[string]any{"pod": pod, "node": node, "phase": phase, "pods": int64(pods)}
	same := cur != nil
	for k, v := range want {
		if cur[k] != v {
			same = false
		}
	}
	if same {
		return nil
	}
	cp := lp.DeepCopy()
	if err := unstructured.SetNestedMap(cp.Object, want, "status"); err != nil {
		return err
	}
	_, err := c.Dyn.Resource(GVR).Namespace(lp.GetNamespace()).UpdateStatus(ctx, cp, metav1.UpdateOptions{})
	return err
}

// Identity for leader election: the hostname.
func Identity() string {
	h, _ := os.Hostname()
	return h
}
