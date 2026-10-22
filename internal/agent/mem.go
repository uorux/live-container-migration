package agent

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"lcm/internal/runc"
	"lcm/internal/stage"
)

// Memory facts for the migration pre-flight check: checkpoint images are staged in RAM (tmpfs),
// outside Kubernetes' accounting, so a node short on memory would push the OOM killer onto its
// workloads mid-migration.

// memAvailable is MemAvailable from /proc/meminfo, in bytes.
func memAvailable() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return -1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "MemAvailable:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			if err == nil {
				return kb * 1024
			}
		}
	}
	return -1
}

// stagingFree is the free space of the restore staging filesystem (its tmpfs size limit).
func stagingFree() int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(filepath.Join(stage.Root(), "restore"), &st); err != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

// containerAnon is the anonymous memory of a container's cgroup (cgroup v2 memory.stat "anon"):
// roughly what a checkpoint of it dumps.
func containerAnon(ctx context.Context, id string) (int64, error) {
	st, err := runc.GetState(ctx, id)
	if err != nil {
		return -1, err
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", st.Pid))
	if err != nil {
		return -1, err
	}
	var cg string
	for _, l := range strings.Split(string(b), "\n") {
		if p, ok := strings.CutPrefix(l, "0::"); ok {
			cg = p
		}
	}
	if cg == "" {
		return -1, fmt.Errorf("no cgroup v2 path for pid %d", st.Pid)
	}
	f, err := os.Open(filepath.Join("/sys/fs/cgroup", cg, "memory.stat"))
	if err != nil {
		return -1, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "anon "); ok {
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		}
	}
	return -1, fmt.Errorf("no anon in memory.stat")
}
