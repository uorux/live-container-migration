package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"lcm/internal/api"
)

// Migration hooks: shell commands (sh -c) from pod annotations, run inside the workload's
// container through the node's agent. A Minecraft server uses them to tell players in chat that
// it's moving and that it's back. They never block or fail a migration: errors and timeouts are
// only logged.
const (
	// PreMigrateAnnotation runs on the source just before checkpointing starts (the workload is
	// still running; the freeze comes seconds later). Env: LCM_SOURCE_NODE, LCM_TARGET_NODE.
	PreMigrateAnnotation = "lcm.rooty.dev/pre-migrate"
	// PostMigrateAnnotation runs in the restored container once it's back. Env: LCM_SOURCE_NODE,
	// LCM_TARGET_NODE (where it runs now), LCM_BLACKOUT (e.g. "2.8s"), LCM_RESULT ("done";
	// "rolled_back" when the restore failed and it was restored on the source again; "aborted"
	// when the migration stopped before the freeze and it never stopped running).
	PostMigrateAnnotation = "lcm.rooty.dev/post-migrate"

	hookTimeout = 5 * time.Second
)

// runHook runs pod's hook (annotation key) in its container ctrID through the agent at agentURL.
func runHook(ctx context.Context, agentURL, token string, pod *corev1.Pod, key, ctrID string, env map[string]string, logf func(string, ...any)) {
	cmd := pod.Annotations[key]
	if cmd == "" || ctrID == "" {
		return
	}
	body, _ := json.Marshal(api.ExecRequest{Container: ctrID, Command: cmd, Env: env, TimeoutSec: int(hookTimeout / time.Second)})
	hctx, cancel := context.WithTimeout(ctx, hookTimeout+5*time.Second)
	defer cancel()
	if _, err := agentDo(hctx, agentURL, token, http.MethodPost, "/v1/exec", bytes.NewReader(body)); err != nil {
		logf("warning: %s hook: %v", key, err)
		return
	}
	logf("ran the %s hook", key)
}

// containerID returns the containerd ID of pod's container ctrName, or "".
func containerID(ctx context.Context, kc kubernetes.Interface, ns, pod, ctrName string) string {
	p, err := kc.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name == ctrName && cs.State.Running != nil {
			return trimRuntime(cs.ContainerID)
		}
	}
	return ""
}

func trimRuntime(id string) string {
	if _, after, ok := strings.Cut(id, "://"); ok {
		return after
	}
	return id
}
