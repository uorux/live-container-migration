package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"lcm/internal/api"
	"lcm/internal/stage"
)

var envName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// hookScript prefixes command with exports of env, single-quoted.
func hookScript(env map[string]string, command string) (string, error) {
	keys := make([]string, 0, len(env))
	for k := range env {
		if !envName.MatchString(k) {
			return "", fmt.Errorf("bad env name %q", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "export %s='%s'\n", k, strings.ReplaceAll(env[k], "'", `'\''`))
	}
	b.WriteString(command)
	return b.String(), nil
}

// handleExec runs a migration hook (sh -c) in a container on this node, as the container's own
// user: crictl exec. Agent token only (never the wake token); bounded in time.
func (a *Agent) handleExec(w http.ResponseWriter, r *http.Request) {
	var req api.ExecRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !stage.ValidName(req.Container) || req.Command == "" {
		http.Error(w, "bad container id or empty command", http.StatusBadRequest)
		return
	}
	script, err := hookScript(req.Env, req.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	timeout := min(max(req.TimeoutSec, 1), 60)
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout+5)*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "k3s", "crictl", "exec", "--sync", "--timeout", strconv.Itoa(timeout),
		req.Container, "sh", "-c", script).CombinedOutput()
	if len(out) > 4096 {
		out = out[:4096]
	}
	if err != nil {
		a.log.Warn("exec hook failed", "container", req.Container, "err", err, "output", strings.TrimSpace(string(out)))
		http.Error(w, fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out))), http.StatusBadGateway)
		return
	}
	a.log.Info("exec hook ran", "container", req.Container)
	w.Header().Set("Content-Type", "text/plain")
	w.Write(out)
}
