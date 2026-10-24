package agent

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"lcm/internal/stage"
)

// Staging lives in RAM (tmpfs). The migration CLI deletes it when a migration finishes, but a
// CLI that dies (or loses its connection) leaves it behind, and a few leftover multi-GB images
// can push the node into the OOM killer. The agent reclaims what can no longer be needed:
//
//   - destination: the images of a restore that succeeded (READY.consumed) once it is a minute
//     old and no lazy-pages daemon still serves from them; result.json stays for the CLI;
//   - source: a checkpoint shipped (SHIPPED) more than shippedRetention ago: by then its
//     destination restored or rolled back from its own copy.
//
// Anything belonging to a failed migration is kept: `lcm recover` may need it
// (LcmStagingNotCleaned reports it).
const (
	consumedRetention = time.Minute
	shippedRetention  = 10 * time.Minute
)

func (a *Agent) reclaimStaging() {
	restores, _ := filepath.Glob(filepath.Join(stage.Root(), "restore", "*"))
	for _, dir := range restores {
		fi, err := os.Stat(filepath.Join(dir, "READY.consumed"))
		if err != nil || time.Since(fi.ModTime()) < consumedRetention || lazyDaemonAlive(filepath.Join(dir, "work", "lazy-pages.pid")) {
			continue
		}
		ents, _ := os.ReadDir(dir)
		freed := false
		for _, e := range ents {
			switch n := e.Name(); {
			case n == "images" || strings.HasPrefix(n, "pre-") || n == "rootfs-diff.tar" || n == stage.RelayExeName:
				os.RemoveAll(filepath.Join(dir, n))
				freed = true
			}
		}
		if freed {
			a.log.Info("reclaimed images of a completed restore the migration didn't clean up", "dir", dir)
		}
	}
	ckpts, _ := filepath.Glob(filepath.Join(stage.Root(), "ckpt", "*"))
	for _, dir := range ckpts {
		fi, err := os.Stat(filepath.Join(dir, "SHIPPED"))
		if err != nil || time.Since(fi.ModTime()) < shippedRetention {
			continue
		}
		keepLogs(dir, filepath.Base(dir))
		os.RemoveAll(dir)
		a.log.Info("reclaimed a shipped checkpoint the migration didn't clean up", "dir", dir)
	}
}

func lazyDaemonAlive(pidFile string) bool {
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return err == nil && pid > 0 && syscall.Kill(pid, 0) == nil
}

// CRIU's logs of each migration (kept for debugging, see keepLogs) run to tens of MB per GB of
// workload, on the node's disk: keep the newest logsKeep migrations' always, and older ones
// only while all of them fit in logsMaxBytes and are younger than logsMaxAge.
const (
	logsMaxBytes = 512 << 20
	logsKeep     = 5
	logsMaxAge   = 30 * 24 * time.Hour
)

// pruneLogs deletes the oldest per-migration directories under root beyond those limits and
// returns how many it deleted.
func pruneLogs(root string, maxBytes int64, keep int, maxAge time.Duration) int {
	ents, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	type logDir struct {
		path string
		mod  time.Time
		size int64
	}
	var dirs []logDir
	for _, e := range ents {
		fi, err := e.Info()
		if err != nil || !e.IsDir() {
			continue
		}
		d := logDir{path: filepath.Join(root, e.Name()), mod: fi.ModTime()}
		filepath.WalkDir(d.path, func(_ string, f fs.DirEntry, err error) error {
			if err == nil && f.Type().IsRegular() {
				if i, err := f.Info(); err == nil {
					d.size += i.Size()
				}
			}
			return nil
		})
		dirs = append(dirs, d)
	}
	slices.SortFunc(dirs, func(a, b logDir) int { return b.mod.Compare(a.mod) }) // newest first
	var total int64
	removed := 0
	for i, d := range dirs {
		total += d.size
		if i < keep || (total <= maxBytes && time.Since(d.mod) < maxAge) {
			continue
		}
		if os.RemoveAll(d.path) == nil {
			removed++
		}
	}
	return removed
}
