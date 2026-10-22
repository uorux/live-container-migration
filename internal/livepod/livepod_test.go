package livepod

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"lcm/internal/controller"
)

func newLivePod() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "lcm.rooty.dev/v1alpha1", "kind": "LivePod",
		"metadata": map[string]any{"name": "fab", "namespace": "ns"},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"labels": map[string]any{"app": "fabric"}},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": "mc", "image": "x"}}},
		}},
	}}
}

func setup(objs ...runtime.Object) (*Controller, *fake.Clientset) {
	kc := fake.NewSimpleClientset(objs...)
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{GVR: "LivePodList"}, newLivePod())
	return &Controller{Kube: kc, Dyn: dyn, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, kc
}

func podCount(t *testing.T, kc *fake.Clientset) int {
	l, err := kc.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{LabelSelector: Label + "=fab"})
	if err != nil {
		t.Fatal(err)
	}
	return len(l.Items)
}

func TestCreatesPodWhenNone(t *testing.T) {
	c, kc := setup()
	for i := 0; i < 3; i++ { // idempotent: one pod, not three
		if err := c.reconcileAll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := podCount(t, kc); n != 1 {
		t.Fatalf("want 1 pod, got %d", n)
	}
	p, _ := kc.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	if p.Items[0].Labels["app"] != "fabric" || len(p.Items[0].OwnerReferences) != 0 {
		t.Fatalf("bad pod: %+v", p.Items[0].ObjectMeta)
	}
}

func TestExistingPodBlocksCreation(t *testing.T) {
	now := metav1.Now()
	terminating := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "fab-old", Namespace: "ns", DeletionTimestamp: &now,
		Finalizers: []string{"x"}, Labels: map[string]string{Label: "fab"}}}
	c, kc := setup(terminating)
	if err := c.reconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := podCount(t, kc); n != 1 {
		t.Fatalf("a Terminating pod must block creation; got %d pods", n)
	}
}

func TestMigrationHoldBlocksCreation(t *testing.T) {
	c, kc := setup()
	secs := int32(600)
	now := metav1.NewMicroTime(metav1.Now().Time)
	_, err := kc.CoordinationV1().Leases("ns").Create(context.Background(), &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: controller.HoldLeaseName("fab"), Namespace: "ns"},
		Spec:       coordv1.LeaseSpec{LeaseDurationSeconds: &secs, RenewTime: &now},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := podCount(t, kc); n != 0 {
		t.Fatalf("held LivePod got a pod")
	}
}

func podWithReady(name, from string, readyNow bool, since time.Time) *corev1.Pod {
	st := corev1.ConditionFalse
	if readyNow {
		st = corev1.ConditionTrue
	}
	labels := map[string]string{Label: "fab"}
	if from != "" {
		labels[controller.LabelFrom] = from
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: labels},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: st, LastTransitionTime: metav1.NewTime(since)},
		}},
	}
}

func TestRetiresStaleSource(t *testing.T) {
	old := time.Now().Add(-10 * time.Minute)
	c, kc := setup(podWithReady("fab-a", "", false, old), podWithReady("fab-a-m1", "fab-a", true, old))
	c.reconcileAll(context.Background())
	if _, err := kc.CoreV1().Pods("ns").Get(context.Background(), "fab-a", metav1.GetOptions{}); err == nil {
		t.Fatal("stale source not deleted")
	}
	if n := podCount(t, kc); n != 1 {
		t.Fatalf("want only the successor left, got %d pods", n)
	}
}

func TestKeepsSourceDuringMigration(t *testing.T) {
	old := time.Now().Add(-10 * time.Minute)
	for name, pods := range map[string][]runtime.Object{
		// destination parked, source serving
		"parked": {podWithReady("fab-a", "", true, old), podWithReady("fab-a-m1", "fab-a", false, old)},
		// restore just finished; the CLI is about to delete the source
		"fresh": {podWithReady("fab-a", "", false, time.Now()), podWithReady("fab-a-m1", "fab-a", true, time.Now())},
	} {
		c, kc := setup(pods...)
		c.reconcileAll(context.Background())
		if n := podCount(t, kc); n != 2 {
			t.Fatalf("%s: source deleted too early (%d pods left)", name, n)
		}
	}
}

func setupOnDemand(players *int, objs ...runtime.Object) (*Controller, *fake.Clientset) {
	lp := newLivePod()
	unstructured.SetNestedMap(lp.Object, map[string]any{"idleAfter": "20ms", "backend": "fabric"}, "spec", "onDemand")
	kc := fake.NewSimpleClientset(objs...)
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{GVR: "LivePodList"}, lp)
	c := &Controller{Kube: kc, Dyn: dyn, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Players: func(context.Context, *corev1.Pod) (int, error) { return *players, nil }}
	return c, kc
}

func livePodStatus(t *testing.T, c *Controller) map[string]any {
	lp, err := c.Dyn.Resource(GVR).Namespace("ns").Get(context.Background(), "fab", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st, _, _ := unstructured.NestedMap(lp.Object, "status")
	return st
}

func TestOnDemandSleepsWhenIdleAndWakes(t *testing.T) {
	ctx := context.Background()
	players := 1
	c, kc := setupOnDemand(&players, podWithReady("fab-a", "", true, time.Now()))
	c.reconcileAll(ctx)
	time.Sleep(30 * time.Millisecond)
	c.reconcileAll(ctx)
	if podCount(t, kc) != 1 {
		t.Fatal("stopped while players were on")
	}
	players = 0
	c.reconcileAll(ctx) // idle timer starts
	time.Sleep(30 * time.Millisecond)
	c.reconcileAll(ctx) // idle long enough: stop
	if n := podCount(t, kc); n != 0 {
		t.Fatalf("idle pod not stopped (%d pods)", n)
	}
	c.reconcileAll(ctx)
	if n := podCount(t, kc); n != 0 {
		t.Fatalf("a pod was created while asleep (%d pods)", n)
	}
	if st := livePodStatus(t, c); st["asleep"] != true || st["phase"] != "Asleep" {
		t.Fatalf("status %v", st)
	}
	if names, err := c.Wake(ctx, "fabric"); err != nil || len(names) != 1 {
		t.Fatalf("wake: %v %v", names, err)
	}
	if names, _ := c.Wake(ctx, "other"); len(names) != 0 {
		t.Fatalf("woke an unrelated backend: %v", names)
	}
	c.reconcileAll(ctx)
	if n := podCount(t, kc); n != 1 {
		t.Fatalf("not woken (%d pods)", n)
	}
}

func TestOnDemandNotStoppedDuringMigration(t *testing.T) {
	ctx := context.Background()
	players := 0
	lease := &coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: controller.HoldLeaseName("fab"), Namespace: "ns"},
		Spec: coordv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: time.Now()}, LeaseDurationSeconds: ptr(int32(600))}}
	c, kc := setupOnDemand(&players, podWithReady("fab-a", "", true, time.Now()), lease)
	c.reconcileAll(ctx)
	time.Sleep(30 * time.Millisecond)
	c.reconcileAll(ctx)
	if podCount(t, kc) != 1 {
		t.Fatal("stopped while a migration held the LivePod")
	}
}

func ptr[T any](v T) *T { return &v }
