package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"lcm/internal/api"
	"lcm/internal/relayctl"
	"lcm/internal/runc"
	"lcm/internal/stage"
)

// Recover deals with checkpoints a previous agent process left in flight (it restarted or
// crashed mid-migration). runc/CRIU keep running across an agent restart (KillMode=process), so
// a dump may still be under way when the new agent starts: for each leftover checkpoint dir it
// first waits for the runc/CRIU working on that dir to exit (see dumpProcs), then looks at what
// they left behind (see decideRecovery):
//
//   - final dump complete, not yet shipped: finish it (ship to the destination, mark READY), so
//     the parked destination pod restores and the workload comes back;
//   - no complete final dump, source still running: abort (lift the fence, resume the relay);
//   - no complete final dump, source gone: lift the fence so kubelet cold-starts it;
//   - runc/CRIU still running after dumpExitWait: touch nothing (fence, images) and report the
//     migration failed with the source in an unknown state, for an operator.
//
// Recover registers every leftover checkpoint's status before it returns and decides in the
// background. Outcomes are recorded as checkpoint status, so a CLI polling
// GET /v1/checkpoint/{id} learns what happened.
func (a *Agent) Recover(ctx context.Context) {
	dirs, _ := filepath.Glob(filepath.Join(stage.Root(), "ckpt", "*"))
	for _, dir := range dirs {
		id := filepath.Base(dir)
		var p persisted
		b, err := os.ReadFile(filepath.Join(dir, "request.json"))
		if err != nil || json.Unmarshal(b, &p) != nil {
			continue
		}
		if exists(filepath.Join(dir, "SHIPPED")) {
			continue // done; the controller cleans it up
		}
		st := &checkpointSt{Last: api.Event{Phase: "recover", Time: time.Now(), Message: "agent restarted; checking on the migration"}}
		a.mu.Lock()
		a.status[id] = st
		// No new migration of this container while the old one's runc/CRIU may still run.
		a.migrations[p.Request.ContainerID] = true
		a.mu.Unlock()
		emit := func(ev api.Event) {
			ev.Time = time.Now()
			a.log.Info("recovered checkpoint", "migration", id, "phase", ev.Phase, "msg", ev.Message)
			a.mu.Lock()
			st.Last = ev
			if ev.Result != nil {
				st.Result = ev.Result
			}
			switch ev.Phase {
			case "error":
				st.Done, st.Error = true, ev.Message
			case "done":
				st.Done = true
			}
			a.mu.Unlock()
		}
		go a.recoverOne(ctx, dir, p, emit)
	}
}

func (a *Agent) recoverOne(ctx context.Context, dir string, p persisted, emit func(api.Event)) {
	id, ctr := filepath.Base(dir), p.Request.ContainerID
	release := func() { a.mu.Lock(); delete(a.migrations, ctr); a.mu.Unlock() }

	if pids := dumpProcs(dir); len(pids) > 0 {
		emit(api.Event{Phase: "recover", Message: fmt.Sprintf("waiting for the previous agent's runc/CRIU (%s) to exit", procNames(pids))})
	}
	left := waitDumpProcs(ctx, dir, dumpExitWait)
	o := dumpOutcome{
		stillRunning:   len(left) > 0,
		dumped:         exists(filepath.Join(dir, "DUMPED")),
		imagesComplete: imagesComplete(filepath.Join(dir, "images")),
		logSucceeded:   dumpSucceeded(filepath.Join(dir, "work", "dump.log")),
	}
	var cst *runc.State
	if !o.stillRunning {
		cst, _ = runc.GetState(ctx, ctr)
		o.alive = cst != nil && (cst.Status == "running" || cst.Status == "paused")
	}
	fence := stage.FenceFile(p.Request.PodUID)
	switch decideRecovery(o) {
	case recoverLeave:
		// Can't tell how the dump ends, and either guess destroys something: deleting the images
		// may delete the workload's only copy, finishing may ship a half-written one. Leave it all
		// in place; SourceKilled tells the CLI not to clean up the destination's pre-dumps.
		msg := fmt.Sprintf("the agent restarted mid-migration and the final dump's runc/CRIU (%s) were still running after %s; "+
			"nothing was touched (fence %s, images %s): the source may be frozen, dead or running. "+
			"Once they have exited, restart lcm-agent to finish or abort the migration, or recover by hand",
			procNames(left), dumpExitWait, fence, dir)
		a.log.Error("recovered checkpoint left for an operator", "migration", id, "pids", left, "msg", msg)
		emit(api.Event{Phase: "error", Message: msg, Result: &api.CheckpointResult{SourceKilled: true}})
		a.tm.checkpoints.Inc("failed_after_stop")
		// a.migrations keeps the container: no new migration of it until the agent restarts.
	case recoverFinish:
		emit(api.Event{Phase: "recover", Message: "agent restarted after the final dump; finishing the migration"})
		res := &api.CheckpointResult{SourceKilled: true, FinalBytes: dirSize(filepath.Join(dir, "images"))}
		// The freeze began right after the fence was written.
		if fi, err := os.Stat(filepath.Join(dir, "FENCED")); err == nil {
			res.FreezeStart = fi.ModTime()
		}
		res, err := a.finish(context.WithoutCancel(ctx), p.Request, dir, p.Upper, res, emit)
		keepLogs(dir, id)
		a.tm.recordCheckpoint(res, err)
		release()
		if err != nil {
			emit(api.Event{Phase: "error", Message: "after agent restart: " + err.Error(), Result: res})
			return
		}
		emit(api.Event{Phase: "done", Result: res})
	case recoverAbort:
		if cst.Status == "paused" {
			// CRIU died without thawing the tree it froze.
			if _, err := runc.Run(ctx, "resume", ctr); err != nil {
				a.log.Warn("thawing the container after an aborted checkpoint", "migration", id, "err", err)
			}
		}
		os.Remove(fence)
		if err := relayctl.Command(p.Pid, "resume", 5*time.Second); err != nil && !os.IsNotExist(err) {
			a.log.Warn("relay resume after aborted checkpoint", "migration", id, "err", err)
		}
		keepLogs(dir, id)
		os.RemoveAll(dir)
		release()
		emit(api.Event{Phase: "error", Message: "aborted: the agent restarted and there was no complete final dump; the source kept running"})
		a.tm.checkpoints.Inc("aborted")
	default:
		os.Remove(fence)
		keepLogs(dir, id)
		os.RemoveAll(dir)
		release()
		emit(api.Event{Phase: "error", Message: "the agent restarted mid-dump and the container died without a complete image; it will cold-start on this node"})
		a.tm.checkpoints.Inc("failed_after_stop")
	}
}

// dumpOutcome is what a restarted agent finds of a checkpoint the previous process left behind.
type dumpOutcome struct {
	stillRunning   bool // runc/CRIU working on the checkpoint dir outlived the wait
	dumped         bool // DUMPED: the previous agent saw the final dump through
	imagesComplete bool // the final image set has inventory.img and pstree.img
	logSucceeded   bool // CRIU's dump.log ends in "Dumping finished successfully"
	alive          bool // the container is running (or frozen)
}

type recoveryAction int

const (
	recoverColdStart recoveryAction = iota // the source died without a usable image: lift the fence
	recoverFinish                          // ship the final dump to the destination
	recoverAbort                           // the source runs: lift the fence, resume the relay, drop the images
	recoverLeave                           // the outcome is unknowable for now: touch nothing
)

// decideRecovery picks what a restarted agent does with a leftover checkpoint. Only once no
// runc/CRIU works on it any more do the files say how the dump ended: CRIU writes
// inventory.img after every task's images, logs success only after it killed the tree, and on
// failure before that thaws the tree (so the container is alive).
func decideRecovery(o dumpOutcome) recoveryAction {
	switch {
	case o.stillRunning:
		return recoverLeave
	case o.dumped, o.imagesComplete && o.logSucceeded:
		return recoverFinish
	case o.alive:
		return recoverAbort
	case o.imagesComplete:
		// The tree is gone and the images are complete: as checkpoint does when runc reports an
		// error after CRIU stopped the tree, carry on with them rather than cold-start.
		return recoverFinish
	}
	return recoverColdStart
}

// dumpExitWait bounds how long Recover waits for a previous agent's runc/CRIU. A final dump
// writes only what changed since the last pre-dump; minutes means something is wedged.
var dumpExitWait = 5 * time.Minute

// procRoot is /proc (tests point it elsewhere).
var procRoot = "/proc"

// waitDumpProcs waits up to max for dumpProcs(dir) to come back empty, and returns what still
// runs then (nil once they all exited).
func waitDumpProcs(ctx context.Context, dir string, max time.Duration) []int {
	deadline := time.Now().Add(max)
	pids := dumpProcs(dir)
	for len(pids) > 0 {
		if time.Now().After(deadline) {
			return pids
		}
		select {
		case <-ctx.Done():
			return pids
		case <-time.After(250 * time.Millisecond):
		}
		// Re-check the known ones (cheap); once they're gone, a full scan makes sure no CRIU that
		// runc started meanwhile outlives it.
		pids = slices.DeleteFunc(pids, func(pid int) bool { return !procUses(pid, dir) })
		if len(pids) == 0 {
			pids = dumpProcs(dir)
		}
	}
	return nil
}

// dumpProcs lists the runc and CRIU processes working on checkpoint dir: `runc checkpoint`
// names its image and work paths in its command line, and CRIU ("criu swrk 3", runc's child,
// possibly orphaned) holds the image dir and its log open. Other processes using the dir (an
// operator's `tail -f work/dump.log`) don't count.
func dumpProcs(dir string) []int {
	ents, _ := os.ReadDir(procRoot)
	var pids []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err == nil && pid != os.Getpid() && procUses(pid, dir) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// procUses reports whether pid is a runc or CRIU whose command line, working directory or open
// files reference dir.
func procUses(pid int, dir string) bool {
	p := filepath.Join(procRoot, strconv.Itoa(pid))
	comm, err := os.ReadFile(filepath.Join(p, "comm"))
	// Also matches wrapped binaries (NixOS: .criu-wrapped).
	if err != nil || !strings.Contains(string(comm), "runc") && !strings.Contains(string(comm), "criu") {
		return false
	}
	dirs := []string{dir}
	if r, err := filepath.EvalSymlinks(dir); err == nil && r != dir {
		dirs = append(dirs, r)
	}
	in := func(path string) bool {
		for _, d := range dirs {
			if path == d || strings.HasPrefix(path, d+"/") {
				return true
			}
		}
		return false
	}
	if b, err := os.ReadFile(filepath.Join(p, "cmdline")); err == nil && slices.ContainsFunc(strings.Split(string(b), "\x00"), in) {
		return true
	}
	if l, err := os.Readlink(filepath.Join(p, "cwd")); err == nil && in(l) {
		return true
	}
	fds, _ := os.ReadDir(filepath.Join(p, "fd"))
	for _, fd := range fds {
		if l, err := os.Readlink(filepath.Join(p, "fd", fd.Name())); err == nil && in(l) {
			return true
		}
	}
	return false
}

// procNames renders pids as "runc[123] criu[124]" for messages.
func procNames(pids []int) string {
	var s []string
	for _, pid := range pids {
		comm, _ := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "comm"))
		s = append(s, fmt.Sprintf("%s[%d]", strings.TrimSpace(string(comm)), pid))
	}
	return strings.Join(s, " ")
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func dumpSucceeded(log string) bool {
	b, err := os.ReadFile(log)
	return err == nil && strings.Contains(string(b), "Dumping finished successfully")
}
