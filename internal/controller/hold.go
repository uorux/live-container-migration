package controller

import (
	"context"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// LivePodLabel marks pods kept alive by the LivePod controller (internal/livepod).
const LivePodLabel = "lcm.rooty.dev/livepod"

// HoldLeaseName is the Lease a migration holds on a LivePod while it runs: a rollback briefly
// leaves the LivePod with no pod at all, and the controller must not cold-start a second copy then.
func HoldLeaseName(livePod string) string { return "lcm-livepod-" + livePod }

// HoldDuration bounds a hold left behind by a CLI that died: after it the controller acts again.
const HoldDuration = 30 * time.Minute

// holdLivePod takes the hold for p's LivePod (no-op for other pods) and returns its release.
func holdLivePod(ctx context.Context, kc kubernetes.Interface, p *corev1.Pod, id string, logf func(string, ...any)) func() {
	name := p.Labels[LivePodLabel]
	if name == "" {
		return func() {}
	}
	leases := kc.CoordinationV1().Leases(p.Namespace)
	now := metav1.NewMicroTime(time.Now())
	secs := int32(HoldDuration.Seconds())
	l := &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: HoldLeaseName(name), Namespace: p.Namespace},
		Spec:       coordv1.LeaseSpec{HolderIdentity: &id, LeaseDurationSeconds: &secs, AcquireTime: &now, RenewTime: &now},
	}
	if _, err := leases.Create(ctx, l, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
		// A stale hold from an earlier run: take it over.
		if cur, gerr := leases.Get(ctx, l.Name, metav1.GetOptions{}); gerr == nil {
			cur.Spec = l.Spec
			_, err = leases.Update(ctx, cur, metav1.UpdateOptions{})
		}
		if err != nil {
			logf("warning: holding livepod %s: %v", name, err)
		}
	} else if err != nil {
		logf("warning: holding livepod %s: %v", name, err)
	}
	return func() {
		if err := leases.Delete(context.WithoutCancel(ctx), l.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			logf("warning: releasing livepod hold %s: %v", name, err)
		}
	}
}

// LivePodHeld reports whether a migration currently holds LivePod name.
func LivePodHeld(ctx context.Context, kc kubernetes.Interface, ns, name string) (bool, error) {
	l, err := kc.CoordinationV1().Leases(ns).Get(ctx, HoldLeaseName(name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return true, err // unsure: err on the side of not creating
	}
	if l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return false, nil
	}
	return time.Since(l.Spec.RenewTime.Time) < time.Duration(*l.Spec.LeaseDurationSeconds)*time.Second, nil
}
