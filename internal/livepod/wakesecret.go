package livepod

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// wakeSecretEvery is how often the leader re-checks the wake token Secrets.
const wakeSecretEvery = time.Minute

// syncWakeSecrets keeps every Secret in WakeSecrets ("namespace/name")
// holding the wake token under key "token", so gateways in the cluster can
// mount it (lcm-gateway -wake-token-file) without anyone copying a secret by
// hand. A namespace that doesn't exist yet is skipped until it does.
func (c *Controller) syncWakeSecrets(ctx context.Context) {
	if c.WakeToken == "" {
		return
	}
	for _, ref := range c.WakeSecrets {
		ns, name, ok := strings.Cut(ref, "/")
		if !ok || ns == "" || name == "" {
			continue
		}
		secrets := c.Kube.CoreV1().Secrets(ns)
		s, err := secrets.Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			s = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app.kubernetes.io/managed-by": "lcm-agent"}},
				Data:       map[string][]byte{"token": []byte(c.WakeToken)},
			}
			if _, err := secrets.Create(ctx, s, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsNotFound(err) { // NotFound: the namespace isn't there yet
					c.Log.Warn("wake token secret", "secret", ref, "err", err)
				}
				continue
			}
			c.Log.Info("wake token secret created", "secret", ref)
		case err != nil:
			c.Log.Warn("wake token secret", "secret", ref, "err", err)
		case string(s.Data["token"]) != c.WakeToken:
			if s.Data == nil {
				s.Data = map[string][]byte{}
			}
			s.Data["token"] = []byte(c.WakeToken)
			if _, err := secrets.Update(ctx, s, metav1.UpdateOptions{}); err != nil {
				c.Log.Warn("wake token secret", "secret", ref, "err", err)
				continue
			}
			c.Log.Info("wake token secret updated", "secret", ref)
		}
	}
}
