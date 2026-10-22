package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Shared-volume warm-up. On NFSv4 the server hands out delegations: the source node's client
// keeps them for files the workload had open, and the first open of each such file from another
// node waits for a recall (the client retries after 100 ms). CRIU opens files one by one during
// the restore, so a server with a hundred open world files spent over a second of the blackout
// on recalls. Instead, while the source is still running, the destination agent opens (and
// immediately closes, without reading or writing) the same files through the destination pod's
// own volume mounts, so the recalls happen before the freeze.

// KubeletPodsDir is where kubelet mounts pod volumes (pods/<uid>/volumes/<plugin>/<name>).
func KubeletPodsDir() string {
	if d := os.Getenv("LCM_KUBELET_PODS_DIR"); d != "" {
		return d
	}
	return "/var/lib/kubelet/pods"
}

const maxWarmFiles = 20000

// sharedFiles lists the regular files process pid and its descendants hold open on the
// pod's kubelet-managed volumes, as paths relative to the pod's kubelet dir
// ("volumes/kubernetes.io~nfs/data/world/region/r.0.0.mca"). Only network-backed volumes matter,
// but the list is cheap and opening a local file is harmless, so it isn't filtered by type.
func sharedFiles(pid int, bundle, podUID string) ([]string, error) {
	var spec struct {
		Mounts []struct{ Destination, Source string } `json:"mounts"`
	}
	b, err := os.ReadFile(filepath.Join(bundle, "config.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	podDir := filepath.Join(KubeletPodsDir(), podUID) + "/"
	var mounts []volMount
	for _, m := range spec.Mounts {
		if rel, ok := strings.CutPrefix(m.Source, podDir); ok && strings.HasPrefix(rel, "volumes/") {
			mounts = append(mounts, volMount{filepath.Clean(m.Destination), rel})
		}
	}
	if len(mounts) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range descendants(pid) {
		out = appendOpenFiles(out, seen, p, mounts)
		if len(out) >= maxWarmFiles {
			break
		}
	}
	return out, nil
}

type volMount struct{ dst, rel string }

// descendants returns pid and all its descendants (the workload is usually a child of lcm-relay).
func descendants(pid int) []int {
	out := []int{pid}
	for i := 0; i < len(out) && len(out) < 10000; i++ {
		tasks, _ := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", out[i]))
		for _, t := range tasks {
			b, _ := os.ReadFile(t)
			for _, f := range strings.Fields(string(b)) {
				var c int
				if _, err := fmt.Sscan(f, &c); err == nil {
					out = append(out, c)
				}
			}
		}
	}
	return out
}

func appendOpenFiles(out []string, seen map[string]bool, pid int, mounts []volMount) []string {
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	ents, err := os.ReadDir(fdDir)
	if err != nil {
		return out
	}
	for _, e := range ents {
		target, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil || !strings.HasPrefix(target, "/") || strings.HasSuffix(target, " (deleted)") {
			continue
		}
		var best *volMount
		for i := range mounts {
			m := &mounts[i]
			if (target == m.dst || strings.HasPrefix(target, m.dst+"/")) && (best == nil || len(m.dst) > len(best.dst)) {
				best = m
			}
		}
		if best == nil {
			continue
		}
		rel := filepath.Join(best.rel, strings.TrimPrefix(target, best.dst))
		if fi, err := os.Stat(filepath.Join(fdDir, e.Name())); err != nil || !fi.Mode().IsRegular() || seen[rel] {
			continue
		}
		seen[rel] = true
		out = append(out, rel)
		if len(out) >= maxWarmFiles {
			break
		}
	}
	return out
}

type warmRequest struct {
	PodUID string   `json:"podUID"`
	Paths  []string `json:"paths"`
}

type warmResult struct {
	Opened int   `json:"opened"`
	Failed int   `json:"failed"`
	Millis int64 `json:"millis"`
}

func (c *stagingClient) warm(ctx context.Context, req warmRequest) error {
	b, _ := json.Marshal(req)
	return c.do(ctx, http.MethodPost, "/warm", strings.NewReader(string(b)))
}

// handleWarm opens and closes the listed files under the (destination) pod's kubelet dir.
func (a *Agent) handleWarm(w http.ResponseWriter, r *http.Request) {
	var req warmRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !validUID(req.PodUID) || len(req.Paths) > maxWarmFiles {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	root := filepath.Join(KubeletPodsDir(), req.PodUID)
	res := warmFiles(root, req.Paths, 32)
	a.log.Info("warmed shared files", "key", r.PathValue("key"), "opened", res.Opened, "failed", res.Failed, "ms", res.Millis)
	json.NewEncoder(w).Encode(res)
}

func validUID(s string) bool {
	return s != "" && len(s) <= 64 && !strings.ContainsAny(s, "/\x00") && s != "." && s != ".."
}

func warmFiles(root string, paths []string, workers int) warmResult {
	t0 := time.Now()
	var opened, failed atomic.Int64
	ch := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range ch {
				if warmOne(root, rel) {
					opened.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for _, p := range paths {
		ch <- p
	}
	close(ch)
	wg.Wait()
	return warmResult{Opened: int(opened.Load()), Failed: int(failed.Load()), Millis: time.Since(t0).Milliseconds()}
}

// warmOne opens root/rel the way the restore will (read-write when allowed), without reading or
// writing. It only ever opens regular files inside root/volumes: symlinks are resolved first and
// must stay inside, and FIFOs/devices are never opened.
func warmOne(root, rel string) bool {
	rel = filepath.Clean("/" + rel)[1:]
	if !strings.HasPrefix(rel, "volumes/") {
		return false
	}
	base := filepath.Join(root, "volumes")
	p, err := filepath.EvalSymlinks(filepath.Join(root, rel))
	if err != nil || !strings.HasPrefix(p, base+"/") {
		return false
	}
	fi, err := os.Lstat(p)
	if err != nil || !fi.Mode().IsRegular() {
		return false
	}
	flags := syscall.O_NOFOLLOW | syscall.O_NONBLOCK | syscall.O_CLOEXEC | syscall.O_NOATIME
	fd, err := syscall.Open(p, flags|syscall.O_RDWR, 0)
	if err == syscall.EPERM { // O_NOATIME needs ownership
		fd, err = syscall.Open(p, (flags&^syscall.O_NOATIME)|syscall.O_RDWR, 0)
	}
	if err != nil {
		fd, err = syscall.Open(p, (flags&^syscall.O_NOATIME)|syscall.O_RDONLY, 0)
	}
	if err != nil {
		return false
	}
	syscall.Close(fd)
	return true
}
