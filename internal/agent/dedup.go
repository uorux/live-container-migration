package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// dedup frees, in the earlier pre-copy layers staged here, the pages a newly arrived layer (dir)
// supersedes: `criu dedup` punches them out of the parents' page files. Without it every layer
// stays whole until the restore, so a workload that rewrites memory quickly stages a multiple of
// its size (13 GB for a 5.6 GB server took a node down). Staging is a tmpfs, so the holes are RAM
// back. Restores read the newest layer first, so dropping superseded pages is safe. Failures only
// cost memory: they're logged, never fatal.
func (a *Agent) dedup(ctx context.Context, dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, "parent")); err != nil {
		return nil // the first layer has nothing to dedup
	}
	before := stagingFree()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	t0 := time.Now()
	unhide, err := hideGone(dir, a.log)
	if err != nil {
		return err
	}
	out, err := exec.CommandContext(ctx, "criu", "dedup", "-D", dir).CombinedOutput()
	unhide()
	if err != nil {
		a.log.Warn("criu dedup failed; earlier layers stay whole", "dir", dir, "err", err, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("%v: %s", err, lastLine(string(out)))
	}
	a.log.Info("deduplicated staged pre-dumps", "dir", dir, "freed", humanBytes(stagingFree()-before), "ms", time.Since(t0).Milliseconds())
	return nil
}

// hideGone renames away, for the length of a dedup, the parent layer's pagemaps of processes
// (and shared memory) that aren't in dir: criu dedup walks the parent's pagemaps and gives up at
// the first one with no counterpart in the new layer, leaving every later process's pages whole.
// A process that exited between rounds (a hook's child, a worker) does that. Nothing reads those
// pagemaps again: a restore follows the final dump's processes down the layers, so a renamed one
// left behind by a crash is harmless.
func hideGone(dir string, log *slog.Logger) (unhide func(), err error) {
	parent := filepath.Join(dir, "parent")
	ents, err := os.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	var hidden []string
	unhide = func() {
		for _, name := range hidden {
			if err := os.Rename(filepath.Join(parent, name+hiddenSuffix), filepath.Join(parent, name)); err != nil {
				log.Warn("restoring a pagemap hidden from criu dedup", "err", err)
			}
		}
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "pagemap-") || !strings.HasSuffix(name, ".img") {
			continue
		}
		if _, err := os.Lstat(filepath.Join(dir, name)); err == nil || !errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err := os.Rename(filepath.Join(parent, name), filepath.Join(parent, name+hiddenSuffix)); err != nil {
			unhide()
			return nil, err
		}
		hidden = append(hidden, name)
	}
	return unhide, nil
}

const hiddenSuffix = ".lcm-gone"

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return s
}
