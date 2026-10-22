package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"lcm/internal/api"
	"lcm/internal/metrics"
	"lcm/internal/stage"
)

// Version is set at build time (-ldflags "-X lcm/internal/agent.Version=...").
var Version = "dev"

// telemetry is everything lcm-agent exports at GET /metrics. Counters reset when the agent
// restarts, as Prometheus expects; the durable record is the history file.
type telemetry struct {
	reg *metrics.Registry

	checkpoints    *metrics.Counter   // result
	phaseSeconds   *metrics.Histogram // phase
	bytes          *metrics.Counter   // kind
	predumpRounds  *metrics.Histogram
	restores       *metrics.Counter   // result
	restoreSeconds *metrics.Histogram // part
	migrations     *metrics.Counter   // outcome
	blackout       *metrics.Histogram
	lastMigration  *metrics.Gauge // outcome
}

func newTelemetry(a *Agent) *telemetry {
	r := metrics.NewRegistry()
	t := &telemetry{
		reg:            r,
		checkpoints:    r.Counter("lcm_checkpoints_total", "Source-side migrations run by this agent, by result (success, aborted: source kept running, failed_after_stop: source stopped but the destination wasn't staged).", "result"),
		phaseSeconds:   r.Histogram("lcm_checkpoint_phase_seconds", "Duration of source-side phases: predump_dump, predump_ship (workload running), final_dump, final_ship (workload frozen), frozen (final dump start to destination staged).", metrics.DurationBuckets, "phase"),
		bytes:          r.Counter("lcm_checkpoint_bytes_total", "Checkpoint bytes produced and shipped, by kind (predump, final, rootfs).", "kind"),
		predumpRounds:  r.Histogram("lcm_checkpoint_predump_rounds", "Pre-copy rounds per migration.", []float64{0, 1, 2, 3, 4, 6, 8}),
		restores:       r.Counter("lcm_restores_total", "Restore attempts by lcm-runc on this node, by result (success, failure).", "result"),
		restoreSeconds: r.Histogram("lcm_restore_seconds", "Duration of restore steps on this node: rootfs (writable layer replay), criu (runc restore).", metrics.DurationBuckets, "part"),
		migrations:     r.Counter("lcm_migrations_total", "Completed migrations reported by the lcm CLI to this agent, by outcome (success, rolled_back, aborted, failed).", "outcome"),
		blackout:       r.Histogram("lcm_migration_blackout_seconds", "Time the workload was not running during a migration (final dump start to restore finished): what connected clients perceive.", metrics.DurationBuckets),
		lastMigration:  r.Gauge("lcm_migration_last_timestamp_seconds", "Unix time of the last reported migration, by outcome.", "outcome"),
	}
	r.Gauge("lcm_agent_build_info", "lcm-agent build.", "version").Set(1, Version)
	r.GaugeFunc("lcm_agent_inflight_checkpoints", "Checkpoints currently running on this agent.", nil, func() []metrics.Sample {
		a.mu.Lock()
		defer a.mu.Unlock()
		return []metrics.Sample{{Value: float64(len(a.migrations))}}
	})
	r.GaugeFunc("lcm_staging_bytes", "Bytes of checkpoint images held in RAM (tmpfs) by directory; should drop to 0 after each migration.", []string{"dir"}, func() []metrics.Sample {
		var out []metrics.Sample
		for _, d := range []string{"ckpt", "restore"} {
			out = append(out, metrics.Sample{Labels: []string{d}, Value: float64(dirSize(filepath.Join(stage.Root(), d)))})
		}
		return out
	})
	r.GaugeFunc("lcm_fences", "Pods fenced on this node (never to start here again); GC'd after 14 days.", nil, func() []metrics.Sample {
		n, _ := filepath.Glob(filepath.Join(stage.Root(), "fence", "*"))
		return []metrics.Sample{{Value: float64(len(n))}}
	})
	r.GaugeFunc("lcm_staged_restores", "Restores staged on this node and not yet consumed (READY present).", nil, func() []metrics.Sample {
		n, _ := filepath.Glob(filepath.Join(stage.Root(), "restore", "*", "READY"))
		return []metrics.Sample{{Value: float64(len(n))}}
	})
	return t
}

// ExportLogShipping publishes the log shipper's counters (lines sent to / dropped for VictoriaLogs).
func (a *Agent) ExportLogShipping(sent, dropped func() int64) {
	a.tm.reg.GaugeFunc("lcm_logship_lines", "Log lines shipped to VictoriaLogs by lcm-agent since start, by state (sent, dropped).", []string{"state"}, func() []metrics.Sample {
		return []metrics.Sample{{Labels: []string{"sent"}, Value: float64(sent())}, {Labels: []string{"dropped"}, Value: float64(dropped())}}
	})
}

func (t *telemetry) recordCheckpoint(res *api.CheckpointResult, err error) {
	switch {
	case err == nil:
		t.checkpoints.Inc("success")
	case res != nil && res.SourceKilled:
		t.checkpoints.Inc("failed_after_stop")
	default:
		t.checkpoints.Inc("aborted")
	}
	if res == nil {
		return
	}
	t.predumpRounds.Observe(float64(len(res.PreDumps)))
	for _, r := range res.PreDumps {
		t.phaseSeconds.Observe(float64(r.DumpMS)/1000, "predump_dump")
		t.phaseSeconds.Observe(float64(r.ShipMS)/1000, "predump_ship")
		t.bytes.Add(float64(r.Bytes), "predump")
	}
	if res.SourceKilled {
		t.phaseSeconds.Observe(float64(res.FinalDumpMS)/1000, "final_dump")
		t.bytes.Add(float64(res.FinalBytes), "final")
		t.bytes.Add(float64(res.RootfsBytes), "rootfs")
	}
	if err == nil {
		t.phaseSeconds.Observe(float64(res.ShipMS)/1000, "final_ship")
		t.phaseSeconds.Observe(float64(res.FrozenMS)/1000, "frozen")
	}
}

// RestoreEventsFile is where lcm-runc appends api.RestoreEvent lines.
func RestoreEventsFile() string { return filepath.Join(stage.Root(), "events", "restores.jsonl") }

// followRestores turns lcm-runc's restore events into metrics. It starts at the end of the file:
// counters cover events since the agent started.
func (t *telemetry) followRestores(ctx context.Context, log func(msg string, args ...any)) {
	var off int64 = -1
	for {
		if f, err := os.Open(RestoreEventsFile()); err == nil {
			if off < 0 {
				off, _ = f.Seek(0, io.SeekEnd)
			} else if fi, err := f.Stat(); err == nil && fi.Size() < off {
				off = 0 // rotated/truncated
			}
			f.Seek(off, io.SeekStart)
			r := bufio.NewReader(f)
			for {
				line, err := r.ReadBytes('\n')
				if err != nil {
					break // partial line: re-read next time
				}
				off += int64(len(line))
				var ev api.RestoreEvent
				if json.Unmarshal(line, &ev) != nil {
					continue
				}
				result := "success"
				if ev.ExitCode != 0 || ev.Error != "" {
					result = "failure"
				}
				t.restores.Inc(result)
				t.restoreSeconds.Observe(float64(ev.RootfsMS)/1000, "rootfs")
				if ev.RestoreMS > 0 {
					t.restoreSeconds.Observe(float64(ev.RestoreMS)/1000, "criu")
				}
				log("restore", "key", ev.Key, "result", result, "restoreMs", ev.RestoreMS, "error", ev.Error)
			}
			f.Close()
		} else if off < 0 {
			off = 0 // file appears later: read it from the start
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// HistoryFile keeps every migration report this agent received (one JSON object per line).
func HistoryFile() string { return filepath.Join(stage.Root(), "history.jsonl") }

func (a *Agent) handleReport(w http.ResponseWriter, r *http.Request) {
	var rep api.MigrationReport
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&rep); err != nil || !stage.ValidName(rep.ID) {
		http.Error(w, "bad report", http.StatusBadRequest)
		return
	}
	if rep.Time.IsZero() {
		rep.Time = time.Now()
	}
	a.tm.migrations.Inc(rep.Outcome)
	a.tm.lastMigration.Set(float64(rep.Time.Unix()), rep.Outcome)
	if rep.BlackoutMS > 0 {
		a.tm.blackout.Observe(float64(rep.BlackoutMS) / 1000)
	}
	b, _ := json.Marshal(rep)
	if f, err := os.OpenFile(HistoryFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		f.Write(append(b, '\n'))
		f.Close()
	}
	a.log.Info("migration report", "id", rep.ID, "outcome", rep.Outcome, "from", rep.SourceNode, "to", rep.DestNode,
		"blackoutMs", rep.BlackoutMS, "error", rep.Error)
	w.WriteHeader(http.StatusNoContent)
}

// handleHistory returns the last ?n (default 50) migration reports, oldest first.
func (a *Agent) handleHistory(w http.ResponseWriter, r *http.Request) {
	n := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("n")); err == nil && v > 0 {
		n = v
	}
	var reps []json.RawMessage
	if f, err := os.Open(HistoryFile()); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			reps = append(reps, append(json.RawMessage(nil), sc.Bytes()...))
			if len(reps) > n {
				reps = reps[1:]
			}
		}
		f.Close()
	}
	w.Header().Set("Content-Type", "application/json")
	if reps == nil {
		reps = []json.RawMessage{}
	}
	json.NewEncoder(w).Encode(reps)
}
