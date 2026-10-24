package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// pullAuths finds, for each image, the registry credential kubelet would use
// from the pod's imagePullSecrets: base64("user:password"), as crictl's
// --auth takes it. Images with no matching secret are left out (pulled
// anonymously). The destination agent pre-pulls with these before the
// freeze; without them a private image would be pulled by kubelet inside the
// blackout.
func pullAuths(ctx context.Context, kc kubernetes.Interface, pod *corev1.Pod, images []string) (map[string]string, []string) {
	var warns []string
	var configs []map[string]dockerAuth
	for _, ref := range pod.Spec.ImagePullSecrets {
		s, err := kc.CoreV1().Secrets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			warns = append(warns, "imagePullSecret "+ref.Name+": "+err.Error())
			continue
		}
		cfg, err := parseDockerSecret(s)
		if err != nil {
			warns = append(warns, "imagePullSecret "+ref.Name+": "+err.Error())
			continue
		}
		configs = append(configs, cfg)
	}
	auths := map[string]string{}
	for _, img := range images {
		host := registryHost(img)
		for _, cfg := range configs { // first secret with the registry wins, like kubelet
			if a, ok := cfg[host]; ok {
				auths[img] = a.encoded()
				break
			}
		}
	}
	return auths, warns
}

type dockerAuth struct {
	Auth     string `json:"auth"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (a dockerAuth) encoded() string {
	if a.Auth != "" {
		return a.Auth
	}
	return base64.StdEncoding.EncodeToString([]byte(a.Username + ":" + a.Password))
}

// parseDockerSecret reads a kubernetes.io/dockerconfigjson or the legacy
// kubernetes.io/dockercfg Secret into registry host -> credential.
func parseDockerSecret(s *corev1.Secret) (map[string]dockerAuth, error) {
	raw := map[string]dockerAuth{}
	switch {
	case s.Data[corev1.DockerConfigJsonKey] != nil:
		var cfg struct {
			Auths map[string]dockerAuth `json:"auths"`
		}
		if err := json.Unmarshal(s.Data[corev1.DockerConfigJsonKey], &cfg); err != nil {
			return nil, err
		}
		raw = cfg.Auths
	case s.Data[corev1.DockerConfigKey] != nil:
		if err := json.Unmarshal(s.Data[corev1.DockerConfigKey], &raw); err != nil {
			return nil, err
		}
	}
	out := map[string]dockerAuth{}
	for k, v := range raw {
		out[normalizeRegistry(k)] = v
	}
	return out, nil
}

// normalizeRegistry turns a docker config key ("https://index.docker.io/v1/",
// "git.rooty.dev") into the host an image reference names.
func normalizeRegistry(k string) string {
	k = strings.TrimPrefix(strings.TrimPrefix(k, "https://"), "http://")
	if i := strings.IndexByte(k, '/'); i >= 0 {
		k = k[:i]
	}
	switch k {
	case "index.docker.io", "registry-1.docker.io", "registry.hub.docker.com":
		return "docker.io"
	}
	return k
}

// registryHost is the registry an image reference pulls from.
func registryHost(image string) string {
	first, _, ok := strings.Cut(image, "/")
	if !ok || !(strings.ContainsAny(first, ".:") || first == "localhost") {
		return "docker.io"
	}
	return normalizeRegistry(first)
}
