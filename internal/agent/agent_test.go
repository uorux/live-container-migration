package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"lcm/internal/stage"
)

const testToken = "s3cret"

type tarEntry struct {
	name, link, body string
	typ              byte
}

func mkTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Linkname: e.link, Typeflag: e.typ, Mode: 0o644}
		if e.typ == tar.TypeReg {
			hdr.Size = int64(len(e.body))
		}
		if e.typ == tar.TypeDir {
			hdr.Mode = 0o755
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		io.WriteString(tw, e.body)
	}
	tw.Close()
	return buf.Bytes()
}

func TestUntarImageDir(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "images")
	data := mkTar(t,
		tarEntry{name: "inventory.img", typ: tar.TypeReg, body: "inv"},
		tarEntry{name: "sub/", typ: tar.TypeDir},
		tarEntry{name: "sub/pages-1.img", typ: tar.TypeReg, body: "pages"},
		tarEntry{name: "parent", typ: tar.TypeSymlink, link: "../pre-2"},
	)
	if err := untar(bytes.NewReader(data), dir); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "sub/pages-1.img")); string(b) != "pages" {
		t.Errorf("pages = %q", b)
	}
	if l, err := os.Readlink(filepath.Join(dir, "parent")); err != nil || l != "../pre-2" {
		t.Errorf("parent -> %q (%v)", l, err)
	}
}

func TestUntarRefusesEscapes(t *testing.T) {
	cases := map[string][]tarEntry{
		"dotdot":      {{name: "../escaped", typ: tar.TypeReg, body: "x"}},
		"dotdot-deep": {{name: "a/../../escaped", typ: tar.TypeReg, body: "x"}},
		"absolute":    {{name: "/escaped", typ: tar.TypeReg, body: "x"}},
		"abs-symlink": {{name: "l", typ: tar.TypeSymlink, link: "/etc"}},
		"deep-symlink": {
			{name: "l", typ: tar.TypeSymlink, link: "../../escaped-dir"},
		},
		"hardlink": {{name: "h", typ: tar.TypeLink, link: "../escaped"}},
		"device":   {{name: "d", typ: tar.TypeChar}},
		// Each link only climbs one level, but they compose.
		"chained-symlinks": {
			{name: "a", typ: tar.TypeSymlink, link: ".."},
			{name: "a/b", typ: tar.TypeSymlink, link: ".."},
			{name: "a/b/escaped", typ: tar.TypeReg, body: "x"},
		},
		// A regular file entry written through a symlink planted earlier in the same tar.
		"write-through-symlink": {
			{name: "victim", typ: tar.TypeSymlink, link: "../escaped"},
			{name: "victim", typ: tar.TypeReg, body: "x"},
		},
		"dir-through-symlink": {
			{name: "s", typ: tar.TypeSymlink, link: "../sibling"},
			{name: "s/inner/", typ: tar.TypeDir},
			{name: "s/inner/escaped", typ: tar.TypeReg, body: "x"},
		},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			// base/key/images is the target; anything appearing in base or base/key (other
			// than the target itself) escaped.
			base := t.TempDir()
			keyDir := filepath.Join(base, "key")
			target := filepath.Join(keyDir, "images")
			os.MkdirAll(filepath.Join(keyDir, "sibling"), 0o755)
			err := untar(bytes.NewReader(mkTar(t, entries...)), target)
			t.Logf("untar: %v", err)
			for _, p := range []string{
				filepath.Join(base, "escaped"), filepath.Join(keyDir, "escaped"),
				filepath.Join(keyDir, "sibling", "inner"), "/escaped",
			} {
				if _, serr := os.Lstat(p); serr == nil {
					t.Fatalf("%s created outside the target dir", p)
				}
			}
			if l, lerr := os.Readlink(filepath.Join(target, "l")); lerr == nil {
				t.Fatalf("escaping symlink created: l -> %s", l)
			}
		})
	}
}

func newTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("LCM_STATE_DIR", root)
	a := New(testToken, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	return srv, root
}

func do(t *testing.T, method, url, token string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func TestMutatingEndpointsRequireAuth(t *testing.T) {
	srv, root := newTestServer(t)
	key := "games_mc-m1_paper"
	os.MkdirAll(filepath.Join(root, "restore", key, "images"), 0o700)
	os.MkdirAll(filepath.Join(root, "ckpt", "mig"), 0o700)
	endpoints := []struct{ method, path string }{
		{"POST", "/v1/checkpoint"},
		{"PUT", "/v1/stage/" + key + "/dir/images"},
		{"PUT", "/v1/stage/" + key + "/file/rootfs-diff.tar"},
		{"POST", "/v1/stage/" + key + "/ready"},
		{"DELETE", "/v1/stage/" + key},
		{"POST", "/v1/stage/" + key + "/forward"},
		{"GET", "/v1/stage/" + key + "/result"},
		{"DELETE", "/v1/ckpt/mig"},
		{"POST", "/v1/fence/uid-1"},
		{"DELETE", "/v1/fence/uid-1"},
	}
	for _, e := range endpoints {
		for _, tok := range []string{"", "wrong", testToken + "x", strings.ToUpper(testToken)} {
			resp := do(t, e.method, srv.URL+e.path, tok, strings.NewReader("{}"))
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s %s with token %q: %d, want 401", e.method, e.path, tok, resp.StatusCode)
			}
		}
		// Other schemes / malformed headers.
		for _, h := range []string{testToken, "Basic " + testToken, "Bearer", "bearer " + testToken} {
			req, _ := http.NewRequest(e.method, srv.URL+e.path, strings.NewReader("{}"))
			req.Header.Set("Authorization", h)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s %s with Authorization %q: %d, want 401", e.method, e.path, h, resp.StatusCode)
			}
		}
	}
	// Nothing was touched by the rejected calls.
	if _, err := os.Stat(filepath.Join(root, "restore", key, "images")); err != nil {
		t.Errorf("staged images removed by unauthenticated call: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "ckpt", "mig")); err != nil {
		t.Errorf("checkpoint removed by unauthenticated call: %v", err)
	}
	if stage.IsFenced("uid-1") {
		t.Errorf("unauthenticated call fenced a pod")
	}
	// Unauthenticated read-only endpoints stay open.
	if resp := do(t, "GET", srv.URL+"/healthz", "", nil); resp.StatusCode != 200 {
		t.Errorf("healthz: %d", resp.StatusCode)
	}
}

func TestReadyAndWait(t *testing.T) {
	srv, root := newTestServer(t)
	key := "games_mc-m1_paper"
	dir := filepath.Join(root, "restore", key)

	// Not ready: wait times out with 408.
	if resp := do(t, "GET", srv.URL+"/v1/stage/"+key+"/wait?timeout=100ms", "", nil); resp.StatusCode != http.StatusRequestTimeout {
		t.Errorf("wait before ready: %d, want 408", resp.StatusCode)
	}
	// Ready without final images is refused.
	if resp := do(t, "POST", srv.URL+"/v1/stage/"+key+"/ready", testToken, nil); resp.StatusCode != http.StatusConflict {
		t.Errorf("ready without images: %d, want 409", resp.StatusCode)
	}
	if stage.IsReady(stage.Key{Namespace: "games", Pod: "mc-m1", Container: "paper"}) {
		t.Fatalf("READY written without images")
	}

	// A waiter blocked before READY is released once it lands.
	done := make(chan int, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/v1/stage/" + key + "/wait?timeout=5s")
		if err != nil {
			done <- -1
			return
		}
		resp.Body.Close()
		done <- resp.StatusCode
	}()
	time.Sleep(100 * time.Millisecond)

	tarball := mkTar(t, tarEntry{name: "inventory.img", typ: tar.TypeReg, body: "inv"})
	if resp := do(t, "PUT", srv.URL+"/v1/stage/"+key+"/dir/images", testToken, bytes.NewReader(tarball)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put images: %d", resp.StatusCode)
	}
	select {
	case code := <-done:
		t.Fatalf("waiter released before READY (%d)", code)
	default:
	}
	if resp := do(t, "POST", srv.URL+"/v1/stage/"+key+"/ready", testToken, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ready: %d", resp.StatusCode)
	}
	if !stage.IsReady(stage.Key{Namespace: "games", Pod: "mc-m1", Container: "paper"}) {
		t.Errorf("stage.IsReady false after ready (agent and lcm-runc disagree on the layout)")
	}
	select {
	case code := <-done:
		if code != http.StatusNoContent {
			t.Errorf("waiter: %d, want 204", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("waiter not released after READY")
	}

	// lcm-runc renames READY and writes a persistent tombstone when it consumes the restore; wait
	// still reports ready (a recreated sandbox after a reboot must not hang in its wait container).
	if err := os.Rename(filepath.Join(dir, "READY"), filepath.Join(dir, "READY.consumed")); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(root, "consumed"), 0o755)
	if err := os.WriteFile(stage.ConsumedFile(key), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(filepath.Join(root, "restore")) // tmpfs wiped by the reboot
	if resp := do(t, "GET", srv.URL+"/v1/stage/"+key+"/wait?timeout=100ms", "", nil); resp.StatusCode != http.StatusNoContent {
		t.Errorf("wait after consume: %d, want 204", resp.StatusCode)
	}

	// DELETE drops the stage; wait goes back to not ready.
	if resp := do(t, "DELETE", srv.URL+"/v1/stage/"+key, testToken, nil); resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete: %d", resp.StatusCode)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("stage dir still there: %v", err)
	}
	if _, err := os.Stat(stage.ConsumedFile(key)); !os.IsNotExist(err) {
		t.Errorf("stage reset kept the old tombstone: %v", err)
	}
	if resp := do(t, "GET", srv.URL+"/v1/stage/"+key+"/wait?timeout=50ms", "", nil); resp.StatusCode != http.StatusRequestTimeout {
		t.Errorf("wait after delete: %d, want 408", resp.StatusCode)
	}
}

func TestStageKeyAndNameValidation(t *testing.T) {
	srv, root := newTestServer(t)
	outside := filepath.Join(root, "outside")
	for _, path := range []string{
		"/v1/stage/..%2F..%2Foutside/file/x",
		"/v1/stage/../file/x",
		"/v1/stage/%2E%2E/file/x",
		"/v1/stage/k/file/..%2F..%2F..%2Foutside",
		"/v1/stage/k/file/%2E%2E",
		"/v1/stage/k/dir/%2E%2E",
		"/v1/stage/k/dir/..%2Foutside",
	} {
		resp := do(t, "PUT", srv.URL+path, testToken, strings.NewReader("x"))
		if resp.StatusCode/100 == 2 {
			t.Errorf("PUT %s accepted (%d)", path, resp.StatusCode)
		}
	}
	for _, path := range []string{"/v1/fence/..%2F..%2Foutside", "/v1/fence/%2E%2E", "/v1/fence/ok?key=..%2Fx", "/v1/ckpt/%2E%2E"} {
		for _, m := range []string{"POST", "DELETE"} {
			resp := do(t, m, srv.URL+path, testToken, nil)
			if resp.StatusCode/100 == 2 {
				t.Errorf("%s %s accepted (%d)", m, path, resp.StatusCode)
			}
		}
	}
	if _, err := os.Stat(outside); err == nil {
		t.Errorf("file written outside the state dir")
	}
	if _, err := os.Stat(root); err != nil {
		t.Errorf("state dir removed: %v", err)
	}
	// The wait endpoint is unauthenticated: it must validate the key too.
	if resp := do(t, "GET", srv.URL+"/v1/stage/..%2F..%2Fetc/wait?timeout=10ms", "", nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("wait with traversal key: %d, want 400", resp.StatusCode)
	}
}

func TestFenceRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	if resp := do(t, "POST", srv.URL+"/v1/fence/uid-1", testToken, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("fence: %d", resp.StatusCode)
	}
	if !stage.IsFenced("uid-1") {
		t.Fatalf("not fenced")
	}
	if resp := do(t, "DELETE", srv.URL+"/v1/fence/uid-1", testToken, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unfence: %d", resp.StatusCode)
	}
	if stage.IsFenced("uid-1") {
		t.Fatalf("still fenced")
	}
	// Lifting a fence that isn't there is not an error (cleanup is idempotent).
	if resp := do(t, "DELETE", srv.URL+"/v1/fence/uid-1", testToken, nil); resp.StatusCode != http.StatusNoContent {
		t.Errorf("unfence twice: %d", resp.StatusCode)
	}
}

func TestFenceWithKeyStopsStagedRestore(t *testing.T) {
	srv, root := newTestServer(t)
	key := "games_mc-m1_paper"
	dir := filepath.Join(root, "restore", key)
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, "READY"), nil, 0o644)

	// While lcm-runc holds the key's lock (a restore in flight), the fence call must wait.
	unlock, err := stage.Lock(key)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { done <- do(t, "POST", srv.URL+"/v1/fence/uid-2?key="+key, testToken, nil).StatusCode }()
	select {
	case <-done:
		t.Fatal("fence returned while a restore held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	unlock()
	if code := <-done; code != http.StatusNoContent {
		t.Fatalf("fence: %d", code)
	}
	if !stage.IsFenced("uid-2") {
		t.Errorf("not fenced")
	}
	if stage.IsReady(stage.Key{Namespace: "games", Pod: "mc-m1", Container: "paper"}) {
		t.Errorf("staged restore still READY after fencing")
	}
}

func TestBinEndpointOnlyServesKnownNames(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, name := range []string{"lcm-agent", "..%2F..%2Fetc%2Fpasswd", "%2E%2E", "lcm-runc"} {
		if resp := do(t, "GET", srv.URL+"/v1/bin/"+name, "", nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /v1/bin/%s: %d, want 404", name, resp.StatusCode)
		}
	}
}

// putDir/forward ship image dirs with writeTar; the receiving untar must reproduce them,
// including CRIU's relative parent link to a sibling pre-dump dir.
func TestWriteTarUntarRoundTrip(t *testing.T) {
	src := filepath.Join(t.TempDir(), "images")
	os.MkdirAll(filepath.Join(src, "nested"), 0o700)
	os.WriteFile(filepath.Join(src, "pages-1.img"), bytes.Repeat([]byte{7}, 1<<16), 0o600)
	os.WriteFile(filepath.Join(src, "nested", "x.img"), []byte("x"), 0o600)
	os.Symlink("../pre-1", filepath.Join(src, "parent"))
	var buf bytes.Buffer
	if err := writeTar(&buf, src); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "key", "images")
	if err := untar(&buf, dst); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "pages-1.img")); len(b) != 1<<16 {
		t.Errorf("pages-1.img: %d bytes", len(b))
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "nested", "x.img")); string(b) != "x" {
		t.Errorf("nested/x.img = %q", b)
	}
	if l, err := os.Readlink(filepath.Join(dst, "parent")); err != nil || l != "../pre-1" {
		t.Errorf("parent -> %q (%v)", l, err)
	}
}

func TestCompressedStagingRoundTrip(t *testing.T) {
	srv, root := newTestServer(t)
	src := t.TempDir()
	pages := bytes.Repeat([]byte("jvm heap page "), 1<<16) // compressible, ~900 KiB
	os.WriteFile(filepath.Join(src, "pages-1.img"), pages, 0o644)
	os.WriteFile(filepath.Join(src, "inventory.img"), []byte("inv"), 0o644)
	os.Symlink("../pre-1", filepath.Join(src, "parent"))
	c := &stagingClient{base: srv.URL, key: "ns_pod-m1_c", token: testToken}
	ctx := context.Background()
	if err := c.putDir(ctx, src, "images"); err != nil {
		t.Fatal(err)
	}
	if err := c.putFile(ctx, filepath.Join(src, "pages-1.img"), "rootfs-diff.tar"); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "restore", "ns_pod-m1_c")
	got, err := os.ReadFile(filepath.Join(dst, "images", "pages-1.img"))
	if err != nil || !bytes.Equal(got, pages) {
		t.Fatalf("pages mismatch (err %v, %d bytes)", err, len(got))
	}
	if l, _ := os.Readlink(filepath.Join(dst, "images", "parent")); l != "../pre-1" {
		t.Errorf("parent link = %q", l)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "rootfs-diff.tar")); !bytes.Equal(got, pages) {
		t.Errorf("file mismatch")
	}
}

// An agent from before compressed transfers (no GET /v1/info, ignores Content-Encoding) must get
// plain tar bodies: nodes are upgraded one at a time.
func TestStagingPlainForLegacyReceiver(t *testing.T) {
	var got []string
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/stage/{key}/dir/{name}", func(w http.ResponseWriter, r *http.Request) {
		got = append(got, "enc="+r.Header.Get("Content-Encoding"))
		tr := tar.NewReader(r.Body)
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			got = append(got, h.Name)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	legacy := httptest.NewServer(mux)
	defer legacy.Close()
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "pages-1.img"), []byte("pages"), 0o644)
	c := &stagingClient{base: legacy.URL, key: "ns_pod-m1_c", token: testToken}
	if err := c.putDir(context.Background(), src, "images"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "enc=,pages-1.img" {
		t.Errorf("legacy receiver saw %v", got)
	}

	// A current agent advertises zstd.
	srv, _ := newTestServer(t)
	c = &stagingClient{base: srv.URL, key: "ns_pod-m1_c", token: testToken}
	if enc := c.encoding(context.Background()); enc != "zstd" {
		t.Errorf("current agent: encoding %q, want zstd", enc)
	}
}

func TestRelayPlayers(t *testing.T) {
	for in, want := range map[string]int{
		`{"session":"x","streams":3}`:                             3,
		`{"ready":true,"gateways":[{"streams":2},{"streams":5}]}`: 7,
		`{"session":"x","streams":0}`:                             0,
	} {
		if n, err := relayPlayers(in); err != nil || n != want {
			t.Errorf("%s: %d %v, want %d", in, n, err, want)
		}
	}
}

// The wake-only token wakes backends and can do nothing else.
func TestWakeToken(t *testing.T) {
	t.Setenv("LCM_STATE_DIR", t.TempDir())
	a := New(testToken, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	var woke []string
	a.SetWake("wakeonly", func(_ context.Context, b string) ([]string, error) {
		woke = append(woke, b)
		if b == "atm10" {
			return []string{"mc/atm10"}, nil
		}
		return nil, nil
	})
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	for _, c := range []struct {
		method, path, token string
		want                int
	}{
		{"POST", "/v1/wake/atm10", "wakeonly", 200},
		{"POST", "/v1/wake/atm10", testToken, 200},
		{"POST", "/v1/wake/nope", "wakeonly", 404},
		{"POST", "/v1/wake/atm10", "wrong", 401},
		{"GET", "/v1/info", "wakeonly", 401},
		{"DELETE", "/v1/stage/ns_p_c", "wakeonly", 401},
	} {
		resp := do(t, c.method, srv.URL+c.path, c.token, nil)
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %s with %s: %d, want %d", c.method, c.path, c.token, resp.StatusCode, c.want)
		}
	}
	if len(woke) != 3 {
		t.Errorf("wake calls %v", woke)
	}
}
