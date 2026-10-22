// Package agent implements lcm-agent, the per-node daemon (a host systemd service on NixOS) that
// checkpoints containers on the source node and stages their images on the destination node.
//
// HTTP API (all JSON; mutating calls need "Authorization: Bearer <token>"):
//
//	POST   /v1/checkpoint                 source: run a migration, streams NDJSON api.Event lines
//	PUT    /v1/stage/{key}/dir/{name}     destination: body is a tar of an image dir, unpacked as {name}/
//	PUT    /v1/stage/{key}/file/{name}    destination: body is written to {name}
//	POST   /v1/stage/{key}/ready          destination: mark the staged restore complete
//	DELETE /v1/stage/{key}                destination: drop a staged restore
//	POST   /v1/stage/{key}/forward        ship a staged restore on to another agent (rollback:
//	                                      the destination holds the only complete image set)
//	GET    /v1/stage/{key}/wait           destination: long-poll until ready (used by the pod's init
//	                                      container; unauthenticated, reveals only readiness)
//	GET    /v1/stage/{key}/result         destination: lcm-runc's restore result, once there
//	DELETE /v1/ckpt/{migrationID}         source: drop checkpoint images after success
//	POST   /v1/fence/{ns}/{pod}           source: (un)fence by hand: POST fences, DELETE lifts
//	GET    /v1/bin/{name}                 lcm-relay / lcm-gateway / lcm-loadtest binaries, so pods
//	                                      can inject them from a stock image (no registry needed)
//	GET    /healthz
package agent

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"lcm/internal/api"
	"lcm/internal/stage"
)

type Agent struct {
	token  string
	binDir string // where lcm-relay etc. live, served at /v1/bin/{name}
	log    *slog.Logger

	mu         sync.Mutex
	migrations map[string]bool // in-flight checkpoints, by container id
}

func New(token, binDir string, log *slog.Logger) *Agent {
	return &Agent{token: token, binDir: binDir, log: log, migrations: map[string]bool{}}
}

func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("POST /v1/checkpoint", a.auth(a.handleCheckpoint))
	mux.HandleFunc("PUT /v1/stage/{key}/dir/{name}", a.auth(a.handlePutDir))
	mux.HandleFunc("PUT /v1/stage/{key}/file/{name}", a.auth(a.handlePutFile))
	mux.HandleFunc("POST /v1/stage/{key}/ready", a.auth(a.handleReady))
	mux.HandleFunc("DELETE /v1/stage/{key}", a.auth(a.handleDeleteStage))
	mux.HandleFunc("POST /v1/stage/{key}/forward", a.auth(a.handleForward))
	mux.HandleFunc("GET /v1/stage/{key}/wait", a.handleWait)
	mux.HandleFunc("GET /v1/stage/{key}/result", a.auth(a.handleResult))
	mux.HandleFunc("DELETE /v1/ckpt/{id}", a.auth(a.handleDeleteCkpt))
	mux.HandleFunc("POST /v1/fence/{ns}/{pod}", a.auth(a.handleFence))
	mux.HandleFunc("DELETE /v1/fence/{ns}/{pod}", a.auth(a.handleFence))
	mux.HandleFunc("GET /v1/bin/{name}", func(w http.ResponseWriter, r *http.Request) {
		switch name := r.PathValue("name"); name {
		case "lcm-relay", "lcm-gateway", "lcm-loadtest":
			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeFile(w, r, filepath.Join(a.binDir, name))
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

func (a *Agent) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

func (a *Agent) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req api.CheckpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ContainerID == "" || req.DestURL == "" || req.DestKey == "" || req.MigrationID == "" {
		http.Error(w, "containerID, destURL, destKey and migrationID are required", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	if a.migrations[req.ContainerID] {
		a.mu.Unlock()
		http.Error(w, "a migration of this container is already running", http.StatusConflict)
		return
	}
	a.migrations[req.ContainerID] = true
	a.mu.Unlock()
	defer func() { a.mu.Lock(); delete(a.migrations, req.ContainerID); a.mu.Unlock() }()

	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	emit := func(ev api.Event) {
		ev.Time = time.Now()
		a.log.Info("checkpoint", "migration", req.MigrationID, "phase", ev.Phase, "round", ev.Round, "bytes", ev.Bytes, "ms", ev.Millis, "msg", ev.Message)
		enc.Encode(ev)
		if flusher != nil {
			flusher.Flush()
		}
	}
	// Deliberately not r.Context(): once the final dump starts, a dropped client connection must
	// not abort shipping the only copy of the workload's state.
	res, err := a.checkpoint(context.WithoutCancel(r.Context()), req, emit)
	if err != nil {
		ev := api.Event{Phase: "error", Message: err.Error(), Result: res}
		emit(ev)
		return
	}
	emit(api.Event{Phase: "done", Result: res})
}

func (a *Agent) stageKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" || strings.ContainsAny(key, "/\x00") || key == "." || key == ".." {
		http.Error(w, "bad key", http.StatusBadRequest)
		return "", false
	}
	return filepath.Join(stage.Root(), "restore", key), true
}

func validName(n string) bool {
	return n != "" && n != "." && n != ".." && !strings.ContainsAny(n, "/\x00")
}

func (a *Agent) handlePutDir(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	name := r.PathValue("name")
	if !ok || !validName(name) {
		http.Error(w, "bad name", http.StatusBadRequest)
		return
	}
	target := filepath.Join(dir, name)
	if err := os.RemoveAll(target); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if err := untar(bufio.NewReaderSize(r.Body, 1<<20), target); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handlePutFile(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	name := r.PathValue("name")
	if !ok || !validName(name) {
		http.Error(w, "bad name", http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	f, err := os.Create(filepath.Join(dir, name))
	if err == nil {
		_, err = io.Copy(f, r.Body)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleReady(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	if !ok {
		return
	}
	if _, err := os.Stat(filepath.Join(dir, "images")); err != nil {
		http.Error(w, "no final images staged", http.StatusConflict)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "READY"), []byte(time.Now().Format(time.RFC3339Nano)), 0o644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleDeleteStage(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	if !ok {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type ForwardRequest struct {
	DestURL string `json:"destURL"`
	DestKey string `json:"destKey"`
}

// handleForward re-ships a staged restore (images, pre-dumps, rootfs diff, source addresses) to
// another agent under a new key and marks it ready there.
func (a *Agent) handleForward(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	if !ok {
		return
	}
	var req ForwardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DestURL == "" || !validName(req.DestKey) {
		http.Error(w, "destURL and destKey required", http.StatusBadRequest)
		return
	}
	ctx := context.WithoutCancel(r.Context())
	dst := &stagingClient{base: req.DestURL, key: req.DestKey, token: a.token}
	if err := dst.reset(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case name == "work" || name == "result.json" || strings.HasPrefix(name, "READY"):
			continue
		case e.IsDir():
			err = dst.putDir(ctx, filepath.Join(dir, name), name)
		default:
			err = dst.putFile(ctx, filepath.Join(dir, name), name)
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("forwarding %s: %v", name, err), http.StatusBadGateway)
			return
		}
	}
	if err := dst.ready(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	a.log.Info("forwarded staged restore", "key", r.PathValue("key"), "to", req.DestURL, "as", req.DestKey)
	w.WriteHeader(http.StatusNoContent)
}

// handleWait blocks until the staged restore is READY (204), or up to ?timeout (default 60s,
// then 408 so the caller loops). A consumed restore also counts as ready.
func (a *Agent) handleWait(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	if !ok {
		return
	}
	timeout := 60 * time.Second
	if t, err := time.ParseDuration(r.URL.Query().Get("timeout")); err == nil && t > 0 && t < 10*time.Minute {
		timeout = t
	}
	deadline := time.Now().Add(timeout)
	for {
		for _, f := range []string{"READY", "READY.consumed"} {
			if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if time.Now().After(deadline) {
			http.Error(w, "not ready", http.StatusRequestTimeout)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (a *Agent) handleResult(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.stageKey(w, r)
	if !ok {
		return
	}
	b, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "no result yet", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func (a *Agent) handleDeleteCkpt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	if err := os.RemoveAll(filepath.Join(stage.Root(), "ckpt", id)); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleFence(w http.ResponseWriter, r *http.Request) {
	ns, pod := r.PathValue("ns"), r.PathValue("pod")
	if !validName(ns) || !validName(pod) {
		http.Error(w, "bad pod", http.StatusBadRequest)
		return
	}
	f := stage.FenceFile(ns, pod)
	var err error
	if r.Method == http.MethodDelete {
		err = os.Remove(f)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	} else if err = os.MkdirAll(filepath.Dir(f), 0o755); err == nil {
		err = os.WriteFile(f, []byte("manual"), 0o644)
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// untar unpacks a tar stream of regular files and directories (what CRIU image dirs contain, plus
// the relative `parent` symlink) into dir, refusing anything that would escape it. All writes go
// through an os.Root, so a symlink created by an earlier entry is never followed out of dir.
func untar(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		if name == "." {
			continue
		}
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return fmt.Errorf("tar entry %q escapes target", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if d := filepath.Dir(name); d != "." {
				if err := root.MkdirAll(d, 0o700); err != nil {
					return err
				}
			}
			f, err := root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode).Perm())
			if err != nil {
				return err
			}
			_, err = io.Copy(f, tr)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		case tar.TypeSymlink:
			// CRIU's `parent` link points at a sibling pre-dump dir: ../pre-N.
			if filepath.IsAbs(hdr.Linkname) || strings.Count(filepath.Clean(hdr.Linkname), "..") > 1 {
				return fmt.Errorf("refusing symlink %q -> %q", hdr.Name, hdr.Linkname)
			}
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported tar entry %q (type %c)", hdr.Name, hdr.Typeflag)
		}
	}
}
