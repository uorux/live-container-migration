package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lcm/internal/api"
	"lcm/internal/relayctl"
	"lcm/internal/stage"
)

// Recover deals with checkpoints a previous agent process left in flight (it restarted or
// crashed mid-migration). runc/CRIU keep running across an agent restart (KillMode=process), so a
// final dump may even have completed after the agent died. For each leftover checkpoint dir:
//
//   - final dump complete, not yet shipped: finish it (ship to the destination, mark READY), so
//     the parked destination pod restores and the workload comes back;
//   - no complete final dump, source still running: abort (lift the fence, resume the relay);
//   - no complete final dump, source gone: lift the fence so kubelet cold-starts it.
//
// Outcomes are recorded as checkpoint status, so a CLI polling GET /v1/checkpoint/{id} learns
// what happened.
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
		st := &checkpointSt{}
		a.mu.Lock()
		a.status[id] = st
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
		dumped := exists(filepath.Join(dir, "DUMPED")) ||
			(imagesComplete(filepath.Join(dir, "images")) && dumpSucceeded(filepath.Join(dir, "work", "dump.log")))
		fence := stage.FenceFile(p.Request.PodUID)
		switch {
		case dumped:
			emit(api.Event{Phase: "recover", Message: "agent restarted after the final dump; finishing the migration"})
			res := &api.CheckpointResult{SourceKilled: true, FinalBytes: dirSize(filepath.Join(dir, "images"))}
			// The freeze began right after the fence was written.
			if fi, err := os.Stat(filepath.Join(dir, "FENCED")); err == nil {
				res.FreezeStart = fi.ModTime()
			}
			go func(req api.CheckpointRequest, dir, upper string) {
				res, err := a.finish(context.WithoutCancel(ctx), req, dir, upper, res, emit)
				keepLogs(dir, req.MigrationID)
				a.tm.recordCheckpoint(res, err)
				if err != nil {
					emit(api.Event{Phase: "error", Message: "after agent restart: " + err.Error(), Result: res})
					return
				}
				emit(api.Event{Phase: "done", Result: res})
			}(p.Request, dir, p.Upper)
		case alive(ctx, p.Request.ContainerID):
			os.Remove(fence)
			if err := relayctl.Command(p.Pid, "resume", 5*time.Second); err != nil && !os.IsNotExist(err) {
				a.log.Warn("relay resume after aborted checkpoint", "migration", id, "err", err)
			}
			keepLogs(dir, id)
			os.RemoveAll(dir)
			emit(api.Event{Phase: "error", Message: "aborted: the agent restarted before the final dump; the source kept running"})
			a.tm.checkpoints.Inc("aborted")
		default:
			os.Remove(fence)
			keepLogs(dir, id)
			os.RemoveAll(dir)
			emit(api.Event{Phase: "error", Message: "the agent restarted mid-dump and the container died without a complete image; it will cold-start on this node"})
			a.tm.checkpoints.Inc("failed_after_stop")
		}
	}
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func dumpSucceeded(log string) bool {
	b, err := os.ReadFile(log)
	return err == nil && strings.Contains(string(b), "Dumping finished successfully")
}
