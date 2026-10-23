package livepod

import (
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSyncWakeSecrets(t *testing.T) {
	ctx := context.Background()
	kc := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "lcm-wake", Namespace: "stale"}, Data: map[string][]byte{"token": []byte("old"), "other": []byte("kept")}},
	)
	c := &Controller{Kube: kc, Log: slog.Default(), WakeToken: "w4ke",
		WakeSecrets: []string{"games/lcm-wake", "stale/lcm-wake", "bogus"}}
	c.syncWakeSecrets(ctx)
	for _, ns := range []string{"games", "stale"} {
		s, err := kc.CoreV1().Secrets(ns).Get(ctx, "lcm-wake", metav1.GetOptions{})
		if err != nil || string(s.Data["token"]) != "w4ke" {
			t.Fatalf("%s: %v %q", ns, err, s.Data["token"])
		}
	}
	if s, _ := kc.CoreV1().Secrets("stale").Get(ctx, "lcm-wake", metav1.GetOptions{}); string(s.Data["other"]) != "kept" {
		t.Fatal("other keys must be kept")
	}
	c.WakeToken = ""
	c.syncWakeSecrets(ctx) // no token: nothing to do, no panic
}
