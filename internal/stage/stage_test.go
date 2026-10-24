package stage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProgress(t *testing.T) {
	t.Setenv("LCM_STATE_DIR", t.TempDir())
	k := Key{"ns", "pod-m1", "c"}
	if b, last := Progress(k); b != 0 || !last.IsZero() {
		t.Fatalf("nothing staged: %d bytes, last written %v", b, last)
	}
	pages := filepath.Join(RestoreDir(k), "pre-1", "pages-1.img")
	os.MkdirAll(filepath.Dir(pages), 0o755)
	os.WriteFile(pages, make([]byte, 3000), 0o644)
	os.WriteFile(FinalFile(k), nil, 0o644)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, p := range []string{pages, filepath.Dir(pages), FinalFile(k), RestoreDir(k)} {
		os.Chtimes(p, old, old)
	}
	if b, last := Progress(k); b != 3000 || !last.Equal(old) {
		t.Fatalf("got %d bytes, last written %v; want 3000, %v", b, last, old)
	}
	os.MkdirAll(filepath.Join(RestoreDir(k), "images"), 0o755)
	os.WriteFile(filepath.Join(RestoreDir(k), "images", "pages-2.img"), make([]byte, 500), 0o644)
	if b, last := Progress(k); b != 3500 || time.Since(last) > time.Minute {
		t.Fatalf("after the final images began arriving: %d bytes, last written %v", b, last)
	}
}

func TestRestorePending(t *testing.T) {
	t.Setenv("LCM_STATE_DIR", t.TempDir())
	k := Key{"ns", "pod-m1", "c"}
	os.MkdirAll(RestoreDir(k), 0o755)
	touch := func(p string) { os.WriteFile(p, nil, 0o644) }

	if RestorePending(k) {
		t.Fatal("pending without FINAL")
	}
	touch(FinalFile(k))
	if !RestorePending(k) {
		t.Fatal("FINAL without READY must be pending (lcm-runc waits)")
	}
	touch(ReadyFile(k))
	if RestorePending(k) {
		t.Fatal("READY: not pending, restore now")
	}
	os.Rename(ReadyFile(k), ReadyFile(k)+".consumed")
	if RestorePending(k) {
		t.Fatal("consumed: a later container restart must cold-start, not wait")
	}
	os.Rename(ReadyFile(k)+".consumed", ReadyFile(k)+".fenced")
	if RestorePending(k) {
		t.Fatal("fenced: never wait")
	}
}
