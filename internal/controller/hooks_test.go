package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"lcm/internal/api"
)

func TestRunHook(t *testing.T) {
	var got []api.ExecRequest
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/exec" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		auth = r.Header.Get("Authorization")
		var req api.ExecRequest
		json.NewDecoder(r.Body).Decode(&req)
		got = append(got, req)
		if req.Command == "fail" {
			http.Error(w, "exit status 1", http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	var logs []string
	logf := func(f string, a ...any) { logs = append(logs, f) }
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{PreMigrateAnnotation: `echo "say moving" > /tmp/mc-console`}}}
	env := map[string]string{"LCM_TARGET_NODE": "carrack"}

	runHook(context.Background(), srv.URL, "tok", pod, PreMigrateAnnotation, "ctr1", env, logf)
	if len(got) != 1 || got[0].Container != "ctr1" || got[0].Command != pod.Annotations[PreMigrateAnnotation] ||
		got[0].Env["LCM_TARGET_NODE"] != "carrack" || got[0].TimeoutSec <= 0 || auth != "Bearer tok" {
		t.Fatalf("request %+v auth %q", got, auth)
	}
	// No annotation or no container: nothing is sent.
	runHook(context.Background(), srv.URL, "tok", pod, PostMigrateAnnotation, "ctr1", env, logf)
	runHook(context.Background(), srv.URL, "tok", pod, PreMigrateAnnotation, "", env, logf)
	if len(got) != 1 {
		t.Fatalf("%d requests, want 1", len(got))
	}
	// A failing hook is a warning, not an error (runHook returns nothing to fail with).
	pod.Annotations[PostMigrateAnnotation] = "fail"
	runHook(context.Background(), srv.URL, "tok", pod, PostMigrateAnnotation, "ctr2", nil, logf)
	if len(logs) == 0 || logs[len(logs)-1] != "warning: %s hook: %v" {
		t.Errorf("logs %v", logs)
	}
	if trimRuntime("containerd://abc") != "abc" || trimRuntime("abc") != "abc" {
		t.Error("trimRuntime")
	}
}
