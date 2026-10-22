package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLivePodHold(t *testing.T) {
	ctx := context.Background()
	kc := fake.NewSimpleClientset()
	logf := func(f string, a ...any) { t.Logf(f, a...) }
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "fab-x", Namespace: "ns", Labels: map[string]string{LivePodLabel: "fab"}}}

	if held, _ := LivePodHeld(ctx, kc, "ns", "fab"); held {
		t.Fatal("held before any migration")
	}
	release := holdLivePod(ctx, kc, p, "m1", logf)
	if held, _ := LivePodHeld(ctx, kc, "ns", "fab"); !held {
		t.Fatal("not held during migration")
	}
	// A second hold (a stale lease from a dead CLI) takes over rather than failing.
	release2 := holdLivePod(ctx, kc, p, "m2", logf)
	release2()
	release()
	if held, _ := LivePodHeld(ctx, kc, "ns", "fab"); held {
		t.Fatal("still held after release")
	}
	// Pods outside a LivePod are untouched.
	holdLivePod(ctx, kc, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns"}}, "m3", logf)()
}
