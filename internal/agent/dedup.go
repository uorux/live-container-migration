package agent

import (
	"context"
	"fmt"
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
	out, err := exec.CommandContext(ctx, "criu", "dedup", "-D", dir).CombinedOutput()
	if err != nil {
		a.log.Warn("criu dedup failed; earlier layers stay whole", "dir", dir, "err", err, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("%v: %s", err, lastLine(string(out)))
	}
	a.log.Info("deduplicated staged pre-dumps", "dir", dir, "freed", humanBytes(stagingFree()-before), "ms", time.Since(t0).Milliseconds())
	return nil
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return s
}
