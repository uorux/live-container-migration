package livepod

import (
	"context"
	"io"
	"log/slog"
	"testing"

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
