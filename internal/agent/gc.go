package agent

import (
	"os"
	"path/filepath"
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
