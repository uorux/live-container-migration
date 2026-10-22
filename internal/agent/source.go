package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
//
// ctx (the client's request) can abort the migration only up to the fence: after that the
// workload is being stopped and its state must reach the destination regardless. runc/CRIU
// subprocesses are never killed mid-flight (a killed dump can leave the tree frozen); ctx is
// checked between steps instead.
func (a *Agent) checkpoint(ctx context.Context, req api.CheckpointRequest, emit func(api.Event)) (*api.CheckpointResult, error) {
	bg := context.WithoutCancel(ctx)
	st, err := runc.GetState(bg, req.ContainerID)
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
	dir := filepath.Join(stage.Root(), "ckpt", req.MigrationID)
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
	if err := writeJSON(filepath.Join(dir, "request.json"), persisted{Request: req, Upper: upper, Pid: st.Pid}); err != nil {
		return nil, err
	}
	dst := &stagingClient{base: req.DestURL, key: req.DestKey, token: a.token}
	if err := dst.reset(ctx); err != nil {
		return nil, fmt.Errorf("destination agent: %w", err)
	}

	// Pre-copy rounds.
	parent := ""
	var prevBytes int64 = -1
	for round := 1; round <= req.PreDump.Max; round++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("cancelled before pre-dump %d: %w", round, err)
		}
		name := fmt.Sprintf("pre-%d", round)
		t0 := time.Now()
		err := runc.Checkpoint(bg, req.ContainerID, runc.CheckpointOpts{
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
	// Tell the destination the final dump is next: its pod leaves the wait init container now and
	// kubelet starts the container (lcm-runc then waits for READY) while we dump and ship.
	if err := os.WriteFile(filepath.Join(dir, "FINAL"), nil, 0o644); err == nil {
		if err := dst.putFile(ctx, filepath.Join(dir, "FINAL"), "FINAL"); err != nil {
			return nil, fmt.Errorf("signalling the destination: %w", err)
		}
	}
	if req.Relay != "off" {
		switch err := relayctl.Command(st.Pid, "quiesce", 10*time.Second); {
		case err == nil:
			relayed = true
			emit(api.Event{Phase: "quiesce", Message: "relay quiesced"})
		case req.Relay == "required" || !os.IsNotExist(err):
			return nil, fmt.Errorf("relay quiesce: %w", err)
		}
	}
	fence := stage.FenceFile(req.PodUID)
	abort := func(err error) (*api.CheckpointResult, error) {
		os.Remove(fence)
		if relayed {
			if rerr := relayctl.Command(st.Pid, "resume", 5*time.Second); rerr != nil && !os.IsNotExist(rerr) {
				err = fmt.Errorf("%w (and relay resume failed: %v)", err, rerr)
			}
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return abort(fmt.Errorf("cancelled before the final dump: %w", err))
	}

	// Fence first: once the final dump kills the container, kubelet will try to restart it.
	// The fence is keyed by pod UID and stays forever (the agent garbage-collects old fences).
	if err := os.MkdirAll(filepath.Dir(fence), 0o755); err != nil {
		return abort(err)
	}
	if err := os.WriteFile(fence, []byte(req.MigrationID), 0o644); err != nil {
		return abort(err)
	}
	os.WriteFile(filepath.Join(dir, "FENCED"), nil, 0o644)
	emit(api.Event{Phase: "fence"})

	// The pod IP changes on the destination; lcm-runc needs the old addresses to restore (and then
	// reset) any non-loopback TCP sockets the workload holds.
	srcAddrs, err := netaddr.ContainerAddrs(st.Pid)
	if err != nil {
		return abort(err)
	}
	// The destination injects lcm-relay from its own node's lcm version, which may differ from
	// the binary this process runs (an upgrade in between): CRIU would refuse the mismatched
	// executable. Ship the exact running binary; lcm-runc puts it in place before restoring.
	if err := captureRelay(st.Pid, dir); err != nil {
		return abort(fmt.Errorf("capturing the running relay binary: %w", err))
	}
	if len(srcAddrs) > 0 {
		if err := netaddr.Write(filepath.Join(dir, "source-addrs"), srcAddrs); err != nil {
			return abort(err)
		}
	}
	if req.LazyRestore {
		os.WriteFile(filepath.Join(dir, "LAZY"), nil, 0o644)
	}
	// A fallback copy of the writable layer in case the authoritative post-dump capture becomes
	// impossible (e.g. the agent restarts and kubelet removes the dead container meanwhile).
	if err := captureRootfs(upper, filepath.Join(dir, "rootfs-diff.pre.tar")); err != nil {
		return abort(fmt.Errorf("rootfs diff: %w", err))
	}

	res.FreezeStart = time.Now()
	err = runc.Checkpoint(bg, req.ContainerID, runc.CheckpointOpts{
		ImagePath: filepath.Join(dir, "images"), WorkPath: filepath.Join(dir, "work"), ParentPath: rel(parent),
	})
	if err != nil {
		cerr := fmt.Errorf("final dump: %w (log: %s/logs/%s/work-dump.log)", err, stage.Root(), req.MigrationID)
		if alive(bg, req.ContainerID) {
			// CRIU unfreezes the tree when a dump fails: the source keeps running untouched.
			return abort(cerr)
		}
		if !imagesComplete(filepath.Join(dir, "images")) {
			// The tree is gone and there is no usable image: the state is lost. Lift the fence so
			// kubelet cold-starts the workload where it was (players are disconnected).
			os.Remove(fence)
			return nil, fmt.Errorf("%w; the container died without a complete image, so it will cold-start on %s", cerr, "the source node")
		}
		// runc reported an error after CRIU dumped and stopped the tree: carry on with the image.
		emit(api.Event{Phase: "dump", Message: "runc reported an error after a complete dump; continuing: " + err.Error()})
	}
	res.SourceKilled = true
	res.FinalDumpMS = since(res.FreezeStart)
	res.FinalBytes = dirSize(filepath.Join(dir, "images"))
	os.WriteFile(filepath.Join(dir, "DUMPED"), nil, 0o644)
	emit(api.Event{Phase: "dump", Bytes: res.FinalBytes, Millis: res.FinalDumpMS})
	return a.finish(bg, req, dir, upper, res, emit)
}

// finish ships everything after a successful final dump: from here on the source is gone and its
// state exists only in dir plus what the destination already has. It is also how an agent that
// restarted mid-migration completes it (see Recover). Transient failures are retried.
func (a *Agent) finish(ctx context.Context, req api.CheckpointRequest, dir, upper string, res *api.CheckpointResult, emit func(api.Event)) (*api.CheckpointResult, error) {
	dst := &stagingClient{base: req.DestURL, key: req.DestKey, token: a.token}
	t0 := time.Now()
	diffPath := filepath.Join(dir, "rootfs-diff.tar")
	if err := captureRootfs(upper, diffPath); err != nil {
		// The dead container's layer may already be gone; fall back to the pre-dump copy.
		if _, perr := os.Stat(filepath.Join(dir, "rootfs-diff.pre.tar")); perr != nil {
			return res, fmt.Errorf("rootfs diff: %w", err)
		}
		os.Rename(filepath.Join(dir, "rootfs-diff.pre.tar"), diffPath)
		emit(api.Event{Phase: "rootfs", Message: "post-dump capture failed (" + err.Error() + "); using the pre-dump copy"})
	}
	res.RootfsBytes = fileSize(diffPath)
	emit(api.Event{Phase: "rootfs", Bytes: res.RootfsBytes, Millis: since(t0)})

	t1 := time.Now()
	ship := func() error {
		if err := dst.putDir(ctx, filepath.Join(dir, "images"), "images"); err != nil {
			return fmt.Errorf("shipping final images: %w", err)
		}
		for _, f := range []string{"rootfs-diff.tar", "source-addrs", "LAZY", stage.RelayExeName, stage.RelayPathName} {
			if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
				if err := dst.putFile(ctx, filepath.Join(dir, f), f); err != nil {
					return fmt.Errorf("shipping %s: %w", f, err)
				}
			}
		}
		return dst.ready(ctx)
	}
	for attempt := 1; ; attempt++ {
		err := ship()
		if err == nil {
			break
		}
		if attempt == 4 {
			return res, err
		}
		emit(api.Event{Phase: "ship", Message: fmt.Sprintf("attempt %d failed, retrying: %v", attempt, err)})
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	os.WriteFile(filepath.Join(dir, "SHIPPED"), nil, 0o644)
	res.ShipMS = since(t1)
	res.ReadyAt = time.Now()
	if !res.FreezeStart.IsZero() {
		res.FrozenMS = res.ReadyAt.Sub(res.FreezeStart).Milliseconds()
	}
	emit(api.Event{Phase: "ready", Millis: res.FrozenMS, Message: "destination staged"})
	return res, nil
}

// persisted is what the agent keeps on disk about an in-flight checkpoint, so a restarted agent
// can tell what state it was in.
type persisted struct {
	Request api.CheckpointRequest `json:"request"`
	Upper   string                `json:"upper"`
	Pid     int                   `json:"pid"`
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// captureRelay saves the container init's executable if it is lcm-relay: its bytes (read via
// /proc/<pid>/exe, valid even if the file was replaced) and its path inside the container.
func captureRelay(pid int, dir string) error {
	exe := fmt.Sprintf("/proc/%d/exe", pid)
	target, err := os.Readlink(exe)
	if err != nil {
		return err
	}
	if filepath.Base(strings.TrimSuffix(target, " (deleted)")) != "lcm-relay" {
		return nil // no relay in this container
	}
	src, err := os.Open(exe)
	if err != nil {
		return err
	}
	defer src.Close()
	out, err := os.OpenFile(filepath.Join(dir, stage.RelayExeName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, src); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, stage.RelayPathName), []byte(strings.TrimSuffix(target, " (deleted)")), 0o644)
}

// alive reports whether the container still has running (or frozen) processes.
func alive(ctx context.Context, id string) bool {
	st, err := runc.GetState(ctx, id)
	return err == nil && (st.Status == "running" || st.Status == "paused")
}

// imagesComplete is a sanity check that a final dump wrote a restorable image set.
func imagesComplete(dir string) bool {
	for _, f := range []string{"inventory.img", "pstree.img"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return false
		}
	}
	return true
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
