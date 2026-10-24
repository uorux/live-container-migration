package controller

import (
	"context"
	"encoding/base64"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRegistryHost(t *testing.T) {
	for img, want := range map[string]string{
		"git.rooty.dev/minecraft/atm9@sha256:abc": "git.rooty.dev",
		"busybox:1.37":                    "docker.io",
		"itzg/minecraft-server:java21":    "docker.io",
		"docker.io/library/busybox":       "docker.io",
		"index.docker.io/library/busybox": "docker.io",
		"localhost/foo:1":                 "localhost",
		"registry.local:5000/team/app:v1": "registry.local:5000",
		"ghcr.io/owner/img":               "ghcr.io",
	} {
		if got := registryHost(img); got != want {
			t.Errorf("registryHost(%q) = %q, want %q", img, got, want)
		}
	}
}

func TestPullAuths(t *testing.T) {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	kc := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "gitea", Namespace: "mc"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(
				`{"auths":{"https://git.rooty.dev":{"auth":"` + b64("bot:tok") + `"}}}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "hub", Namespace: "mc"},
			Type:       corev1.SecretTypeDockercfg,
			Data: map[string][]byte{corev1.DockerConfigKey: []byte(
				`{"https://index.docker.io/v1/":{"username":"u","password":"p"}}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "shadow", Namespace: "mc"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(
				`{"auths":{"git.rooty.dev":{"username":"other","password":"x"}}}`)},
		},
	)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "atm9", Namespace: "mc"},
		Spec: corev1.PodSpec{ImagePullSecrets: []corev1.LocalObjectReference{
			{Name: "missing"}, {Name: "gitea"}, {Name: "hub"}, {Name: "shadow"},
		}},
	}
	imgs := []string{"git.rooty.dev/minecraft/atm9@sha256:abc", "itzg/minecraft-server:java21", "ghcr.io/x/y:1"}
	auths, warns := pullAuths(context.Background(), kc, pod, imgs)
	if auths[imgs[0]] != b64("bot:tok") {
		t.Errorf("gitea auth = %q (first secret with the registry must win)", auths[imgs[0]])
	}
	if auths[imgs[1]] != b64("u:p") {
		t.Errorf("docker hub auth = %q", auths[imgs[1]])
	}
	if _, ok := auths[imgs[2]]; ok {
		t.Errorf("ghcr.io got an auth with no secret for it")
	}
	if len(warns) != 1 {
		t.Errorf("warnings %v, want one for the missing secret", warns)
	}

	// No pull secrets: nothing to send, nothing to warn about.
	if a, w := pullAuths(context.Background(), kc, &corev1.Pod{}, imgs); len(a) != 0 || len(w) != 0 {
		t.Errorf("no secrets: %v %v", a, w)
	}
}
