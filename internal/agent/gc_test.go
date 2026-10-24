package agent

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReclaimStaging(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LCM_STATE_DIR", root)
	a := New(testToken, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	old := time.Now().Add(-time.Hour)
	mk := func(dir string, files map[string]bool) {
		for f, aged := range files {
			p := filepath.Join(root, dir, f)
			os.MkdirAll(filepath.Dir(p), 0o755)
			os.WriteFile(p, []byte("x"), 0o644)
			if aged {
				os.Chtimes(p, old, old)
			}
		}
	}
	mk("restore/done", map[string]bool{"images/pages-1.img": false, "pre-1/pages-1.img": false, "READY.consumed": true, "result.json": false})
	mk("restore/fresh", map[string]bool{"images/pages-1.img": false, "READY.consumed": false})
	mk("restore/failed", map[string]bool{"images/pages-1.img": false, "READY.fenced": true})
	mk("ckpt/shipped", map[string]bool{"images/pages-1.img": false, "SHIPPED": true})
	mk("ckpt/unshipped", map[string]bool{"images/pages-1.img": false, "DUMPED": true})

	a.reclaimStaging()

	for p, want := range map[string]bool{
		"restore/done/images":      false,
		"restore/done/pre-1":       false,
		"restore/done/result.json": true, // the CLI may still read it
		"restore/fresh/images":     true, // within retention
		"restore/failed/images":    true, // rollback / recover material
		"ckpt/shipped":             false,
		"ckpt/unshipped/images":    true, // failed after the dump: lcm recover needs it
	} {
		_, err := os.Stat(filepath.Join(root, p))
		if (err == nil) != want {
			t.Errorf("%s: exists=%v, want %v", p, err == nil, want)
		}
	}
}

func TestPruneLogs(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	mk := func(name string, age time.Duration, size int) {
		d := filepath.Join(root, name)
		os.MkdirAll(d, 0o755)
		os.WriteFile(filepath.Join(d, "dump.log"), make([]byte, size), 0o644)
		os.Chtimes(d, now.Add(-age), now.Add(-age))
	}
	mk("m1", 1*time.Hour, 100)    // newest
	mk("m2", 2*time.Hour, 100)    // kept: within keep
	mk("m3", 3*time.Hour, 100)    // kept: total 300 <= 350
	mk("m4", 4*time.Hour, 100)    // total 400 > 350: removed
	mk("m5", 40*24*time.Hour, 10) // too old: removed
	if n := pruneLogs(root, 350, 2, 30*24*time.Hour); n != 2 {
		t.Fatalf("removed %d, want 2", n)
	}
	for name, want := range map[string]bool{"m1": true, "m2": true, "m3": true, "m4": false, "m5": false} {
		if _, err := os.Stat(filepath.Join(root, name)); (err == nil) != want {
			t.Errorf("%s exists=%v, want %v", name, err == nil, want)
		}
	}
}
