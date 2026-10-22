package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeAgent(info agentInfo) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(info)
	}))
}

func TestCheckMemory(t *testing.T) {
	const gi = int64(1) << 30
	logf := func(f string, a ...any) { t.Logf(f, a...) }
	src := fakeAgent(agentInfo{MemAvailable: 20 * gi, ContainerAnon: 4 * gi})
	defer src.Close()
	for name, tc := range map[string]struct {
		dst     agentInfo
		wantErr string
	}{
		"plenty":       {agentInfo{MemAvailable: 20 * gi, StagingFree: 20 * gi}, ""},
		"short of RAM": {agentInfo{MemAvailable: 6 * gi, StagingFree: 20 * gi}, "destination node has"},
		"small tmpfs":  {agentInfo{MemAvailable: 20 * gi, StagingFree: 2 * gi}, "staging tmpfs"},
		"old agent":    {agentInfo{}, ""}, // reports no memory: not checked
	} {
		dst := fakeAgent(tc.dst)
		err := checkMemory(context.Background(), "t", src.URL, dst.URL, "abc", logf)
		dst.Close()
		if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.wantErr)
		}
	}
}

func TestCheckVersions(t *testing.T) {
	info := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if body == "" {
				http.NotFound(w, r)
				return
			}
			io.WriteString(w, body)
		}))
	}
	cur := info(`{"version":"0.1.0-a","encodings":["zstd"]}`)
	cur2 := info(`{"version":"0.1.0-b","encodings":["zstd"]}`)
	legacy := info("")
	noEnc := info(`{"version":"0.1.0-c"}`)
	for _, s := range []*httptest.Server{cur, cur2, legacy, noEnc} {
		defer s.Close()
	}
	nolog := func(string, ...any) {}
	ctx := context.Background()
	if err := checkVersions(ctx, "t", cur.URL, cur2.URL, nolog); err != nil {
		t.Errorf("differing current versions: %v", err)
	}
	if err := checkVersions(ctx, "t", cur.URL, legacy.URL, nolog); err == nil {
		t.Error("legacy destination accepted")
	}
	if err := checkVersions(ctx, "t", noEnc.URL, cur.URL, nolog); err == nil {
		t.Error("source without encodings accepted")
	}
}
