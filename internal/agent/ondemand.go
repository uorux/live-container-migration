package agent

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"lcm/internal/relayctl"
	"lcm/internal/runc"
	"lcm/internal/stage"
)

// WakeFunc wakes the on-demand LivePods serving backend and returns their names.
type WakeFunc func(ctx context.Context, backend string) ([]string, error)

// SetWake enables POST /v1/wake/{backend}. Besides the agent token it accepts wakeToken
// (if set), which allows nothing else: gateways outside the cluster hold that one.
func (a *Agent) SetWake(wakeToken string, fn WakeFunc) {
	a.mu.Lock()
	a.wakeToken, a.wake = wakeToken, fn
	a.mu.Unlock()
}

func (a *Agent) handleWake(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	tok, fn := a.wakeToken, a.wake
	a.mu.Unlock()
	got, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	okAgent := subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) == 1
	okWake := tok != "" && subtle.ConstantTimeCompare([]byte(got), []byte(tok)) == 1
	if !okAgent && !okWake {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if fn == nil {
		http.Error(w, "this agent doesn't run the LivePod controller (services.lcm.livePods.enable)", http.StatusServiceUnavailable)
		return
	}
	backend := r.PathValue("backend")
	if !stage.ValidName(backend) {
		http.Error(w, "bad backend name", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	woke, err := fn(ctx, backend)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if len(woke) == 0 {
		http.Error(w, "no on-demand LivePod serves backend "+backend, http.StatusNotFound)
		return
	}
	a.log.Info("wake", "backend", backend, "livepods", woke, "from", r.RemoteAddr)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"woke": woke})
}

// RelayPlayers is GET /v1/relay?container=ID: how many player connections the container's
// lcm-relay carries, over all its gateways.
type RelayPlayers struct {
	Players int `json:"players"`
}

func (a *Agent) handleRelay(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("container")
	if !stage.ValidName(id) {
		http.Error(w, "bad container id", http.StatusBadRequest)
		return
	}
	st, err := runc.GetState(r.Context(), id)
	if err != nil || st.Pid <= 0 {
		http.Error(w, "container not found", http.StatusNotFound)
		return
	}
	line, err := relayctl.Query(st.Pid, "status", 3*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	n, err := relayPlayers(line)
	if err != nil {
		http.Error(w, "relay status: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(RelayPlayers{Players: n})
}

// relayPlayers counts streams in a relay status document (one gateway, or several).
func relayPlayers(status string) (int, error) {
	var st struct {
		Streams  *int `json:"streams"`
		Gateways []struct {
			Streams int `json:"streams"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal([]byte(status), &st); err != nil {
		return 0, err
	}
	if st.Streams != nil {
		return *st.Streams, nil
	}
	n := 0
	for _, g := range st.Gateways {
		n += g.Streams
	}
	return n, nil
}
