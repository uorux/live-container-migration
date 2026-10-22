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
