package agent

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

func TestHookScript(t *testing.T) {
	s, err := hookScript(map[string]string{"LCM_TARGET_NODE": "carrack", "LCM_X": "it's $(id)"}, `echo "$LCM_TARGET_NODE"`)
	if err != nil {
		t.Fatal(err)
	}
	// The values reach the command verbatim: no expansion, no breaking out of the quotes.
	out, err := exec.CommandContext(context.Background(), "sh", "-c", s+`; printf '%s' "$LCM_X"`).Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(out); got != "carrack\nit's $(id)" {
		t.Errorf("output %q", got)
	}
	for _, bad := range []string{"lower", "1X", "A-B", "A B", ""} {
		if _, err := hookScript(map[string]string{bad: "v"}, "true"); err == nil {
			t.Errorf("env name %q accepted", bad)
		}
	}
}

// The wake token, which gateways outside the cluster hold, must not run commands in containers.
func TestExecRefusesWakeToken(t *testing.T) {
	a := New(testToken, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.SetWake("wake-only-token", func(context.Context, string) ([]string, error) { return nil, nil })
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	body := `{"container":"abc","command":"true"}`
	if resp := do(t, "POST", srv.URL+"/v1/exec", "wake-only-token", strings.NewReader(body)); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wake token: %d, want 401", resp.StatusCode)
	}
	for _, b := range []string{`{"container":"../x","command":"true"}`, `{"container":"abc","command":""}`, `{"container":"abc","command":"true","env":{"a":"b"}}`} {
		if resp := do(t, "POST", srv.URL+"/v1/exec", testToken, strings.NewReader(b)); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", b, resp.StatusCode)
		}
	}
}

func TestStagingClientInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Write([]byte(`{"addresses":["10.0.0.1"],"memAvailable":26658975744,"stagingFree":0,"version":"x"}`))
	}))
	defer srv.Close()
	c := &stagingClient{base: srv.URL, token: "tok"}
	if got := c.memAvailable(context.Background()); got != 26658975744 {
		t.Errorf("memAvailable %d", got)
	}
	if got := c.stagingFree(context.Background()); got != 0 { // a full staging tmpfs, not "unknown"
		t.Errorf("stagingFree %d", got)
	}
	if got := (&stagingClient{base: srv.URL, token: "bad"}).memAvailable(context.Background()); got != -1 {
		t.Errorf("unauthorized: %d", got)
	}
}
