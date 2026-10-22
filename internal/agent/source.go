package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"lcm/internal/api"
	"lcm/internal/netaddr"
	"lcm/internal/relayctl"
	"lcm/internal/rootfsdiff"
	"lcm/internal/runc"
	"lcm/internal/stage"
)

// checkpoint runs a whole source-side migration: iterative pre-dumps shipped to the destination
// while the workload runs, then relay quiesce, fence, final dump (which stops the workload), and
// shipping of the last delta and the writable layer. emit receives progress events.
func (a *Agent) checkpoint(ctx context.Context, req api.CheckpointRequest, emit func(api.Event)) (*api.CheckpointResult, error) {
	st, err := runc.GetState(ctx, req.ContainerID)
	if err != nil {
		return nil, err
	}
	if st.Status != "running" {
		return nil, fmt.Errorf("container %s is %s, not running", req.ContainerID, st.Status)
	}
	upper, err := runc.Upperdir(st.Rootfs)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(stage.Root(), "ckpt", stage.Sanitize(req.MigrationID))
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// Images live in RAM (tmpfs). Unless the source was stopped (then they are the only copy of
	// the workload), a failed migration must not leave them behind.
	res := &api.CheckpointResult{}
	defer func() {
		keepLogs(dir, req.MigrationID)
		if !res.SourceKilled {
			os.RemoveAll(dir)
		}
	}()
	dst := &stagingClient{base: req.DestURL, key: req.DestKey, token: a.token}
	if err := dst.reset(ctx); err != nil {
		return nil, fmt.Errorf("destination agent: %w", err)
	}

	// Pre-copy rounds.
	parent := ""
	var prevBytes int64 = -1
	for round := 1; round <= req.PreDump.Max; round++ {
		name := fmt.Sprintf("pre-%d", round)
		t0 := time.Now()
		err := runc.Checkpoint(ctx, req.ContainerID, runc.CheckpointOpts{
			ImagePath: filepath.Join(dir, name), WorkPath: filepath.Join(dir, "work-"+name),
			ParentPath: rel(parent), PreDump: true,
		})
		if err != nil {
			return nil, fmt.Errorf("pre-dump %d: %w", round, err)
		}
		dumpMS := since(t0)
		size := dirSize(filepath.Join(dir, name))
		t1 := time.Now()
		if err := dst.putDir(ctx, filepath.Join(dir, name), name); err != nil {
			return nil, fmt.Errorf("shipping %s: %w", name, err)
		}
		// The destination has the pages now. Later dumps only consult this round's pagemaps (to
		// skip pages that haven't changed), so free the page data: it's RAM on a tmpfs.
		dropPages(filepath.Join(dir, name))
		r := api.Round{DumpMS: dumpMS, ShipMS: since(t1), Bytes: size}
		res.PreDumps = append(res.PreDumps, r)
		emit(api.Event{Phase: "predump", Round: round, Bytes: size, Millis: r.DumpMS + r.ShipMS,
			Message: fmt.Sprintf("dump %dms ship %dms", r.DumpMS, r.ShipMS)})
		parent = name
		if size < req.PreDump.TargetBytes || (prevBytes >= 0 && size >= prevBytes) {
			break // converged, or the dirty rate outpaces us: more rounds won't help
		}
		prevBytes = size
	}

	// Quiesce the in-container relay so the final dump holds no TCP connections to the outside.
	relayed := false
	if req.Relay != "off" {
		switch err := relayctl.Command(st.Pid, "quiesce", 10*time.Second); {
		case err == nil:
			relayed = true
			emit(api.Event{Phase: "quiesce", Message: "relay quiesced"})
		case req.Relay == "required" || !os.IsNotExist(err):
			return nil, fmt.Errorf("relay quiesce: %w", err)
		}
	}
	abort := func(err error) (*api.CheckpointResult, error) {
		os.Remove(stage.FenceFile(req.Namespace, req.Pod))
		if relayed {
			if rerr := relayctl.Command(st.Pid, "resume", 5*time.Second); rerr != nil {
				err = fmt.Errorf("%w (and relay resume failed: %v)", err, rerr)
			}
		}
		return nil, err
	}

	// Fence first: once the final dump kills the container, kubelet will try to restart it.
	if err := os.MkdirAll(filepath.Dir(stage.FenceFile(req.Namespace, req.Pod)), 0o755); err != nil {
		return abort(err)
	}
	if err := os.WriteFile(stage.FenceFile(req.Namespace, req.Pod), []byte(req.MigrationID), 0o644); err != nil {
		return abort(err)
	}
	emit(api.Event{Phase: "fence"})

	// The pod IP changes on the destination; lcm-runc needs the old addresses to restore (and then
	// reset) any non-loopback TCP sockets the workload holds.
	srcAddrs, err := netaddr.ContainerAddrs(st.Pid)
	if err != nil {
		return abort(err)
	}

	res.FreezeStart = time.Now()
	err = runc.Checkpoint(ctx, req.ContainerID, runc.CheckpointOpts{
		ImagePath: filepath.Join(dir, "images"), WorkPath: filepath.Join(dir, "work"), ParentPath: rel(parent),
	})
	if err != nil {
		// CRIU unfreezes the tree when a dump fails: the source keeps running untouched.
		return abort(fmt.Errorf("final dump: %w (log: %s/logs/%s/work-dump.log)", err, stage.Root(), req.MigrationID))
	}
	res.SourceKilled = true
	res.FinalDumpMS = since(res.FreezeStart)
	res.FinalBytes = dirSize(filepath.Join(dir, "images"))
	emit(api.Event{Phase: "dump", Bytes: res.FinalBytes, Millis: res.FinalDumpMS})

	// From here on the source is gone; failures must be recovered by restoring from `dir`.
	t0 := time.Now()
	diffPath := filepath.Join(dir, "rootfs-diff.tar")
	if err := captureRootfs(upper, diffPath); err != nil {
		return res, fmt.Errorf("rootfs diff: %w", err)
	}
	res.RootfsBytes = fileSize(diffPath)
	emit(api.Event{Phase: "rootfs", Bytes: res.RootfsBytes, Millis: since(t0)})

	t1 := time.Now()
	if err := dst.putDir(ctx, filepath.Join(dir, "images"), "images"); err != nil {
		return res, fmt.Errorf("shipping final images: %w", err)
	}
	if err := dst.putFile(ctx, diffPath, "rootfs-diff.tar"); err != nil {
		return res, fmt.Errorf("shipping rootfs diff: %w", err)
	}
	if len(srcAddrs) > 0 {
		p := filepath.Join(dir, "source-addrs")
		if err := netaddr.Write(p, srcAddrs); err != nil {
			return res, err
		}
		if err := dst.putFile(ctx, p, "source-addrs"); err != nil {
			return res, fmt.Errorf("shipping source addresses: %w", err)
		}
	}
	if err := dst.ready(ctx); err != nil {
		return res, err
	}
	res.ShipMS = since(t1)
	res.ReadyAt = time.Now()
	res.FrozenMS = res.ReadyAt.Sub(res.FreezeStart).Milliseconds()
	emit(api.Event{Phase: "ready", Millis: res.FrozenMS, Message: "destination staged"})
	return res, nil
}

// keepLogs copies CRIU's logs out of the (RAM-backed, soon deleted) image dir to
// /var/lib/lcm/logs/<migration>/.
func keepLogs(dir, id string) {
	logs, _ := filepath.Glob(filepath.Join(dir, "work*", "*.log"))
	out := filepath.Join(stage.Root(), "logs", stage.Sanitize(id))
	for _, l := range logs {
		b, err := os.ReadFile(l)
		if err != nil {
			continue
		}
		os.MkdirAll(out, 0o755)
		os.WriteFile(filepath.Join(out, filepath.Base(filepath.Dir(l))+"-"+filepath.Base(l)), b, 0o644)
	}
}

func dropPages(dir string) {
	pages, _ := filepath.Glob(filepath.Join(dir, "pages-*.img"))
	for _, p := range pages {
		os.Truncate(p, 0)
	}
}

func captureRootfs(upper, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := rootfsdiff.Capture(upper, f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func rel(parent string) string {
	if parent == "" {
		return ""
	}
	return "../" + parent
}

func since(t time.Time) int64 { return time.Since(t).Milliseconds() }

func dirSize(dir string) int64 {
	var n int64
	filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			n += fi.Size()
		}
		return nil
	})
	return n
}

func fileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
}
