package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"lcm/internal/api"
)

// NewReport summarises a finished Migrate call for metrics and `lcm history`.
func NewReport(o Options, res *Result, err error, took time.Duration) api.MigrationReport {
	rep := api.MigrationReport{Time: time.Now(), Namespace: o.Namespace, SourcePod: o.Pod, DestNode: o.TargetNode, TotalMS: took.Milliseconds()}
	if res != nil {
		rep.ID, rep.SourcePod, rep.SourceNode, rep.DestPod, rep.DestNode = res.MigrationID, res.SourcePod, res.SourceNode, res.DestPod, res.DestNode
		rep.BlackoutMS = res.Blackout.Milliseconds()
		if ck := res.Checkpoint; ck != nil {
			rep.FrozenMS, rep.PreDumps = ck.FrozenMS, ck.PreDumps
		}
		if res.Restore != nil {
			rep.RestoreMS = res.Restore.RestoreMS
		}
	}
	switch {
	case err == nil:
		rep.Outcome = "success"
	case res != nil && res.RolledBack:
		rep.Outcome = "rolled_back"
	case res == nil || res.Checkpoint == nil || !res.Checkpoint.SourceKilled:
		rep.Outcome = "aborted"
	default:
		rep.Outcome = "failed"
	}
	if err != nil {
		rep.Error = err.Error()
	}
	if rep.ID == "" {
		rep.ID = "unstarted-" + time.Now().UTC().Format("20060102-150405")
	}
	return rep
}

// Report sends rep to the agent on the node where the workload ended up (the source node if it
// never left), which turns it into metrics and keeps it in its history.
func Report(ctx context.Context, kc kubernetes.Interface, o Options, rep api.MigrationReport) error {
	node := rep.DestNode
	if rep.Outcome == "aborted" || node == "" {
		node = rep.SourceNode
	}
	if node == "" {
		return fmt.Errorf("no node to report to")
	}
	url, err := agentURL(ctx, kc, node, port(o))
	if err != nil {
		return err
	}
	b, _ := json.Marshal(rep)
	_, err = agentDo(ctx, url, o.Token, http.MethodPost, "/v1/report", bytes.NewReader(b))
	return err
}

func port(o Options) int {
	if o.AgentPort == 0 {
		return api.DefaultPort
	}
	return o.AgentPort
}

// NodeStatus is one node's view for `lcm status`.
type NodeStatus struct {
	Node, URL, Error string
	Healthy          bool
	Metrics          map[string]float64 // selected lcm_* gauges
}

// Status asks every node's agent for health and a few gauges.
func Status(ctx context.Context, kc kubernetes.Interface, o Options) ([]NodeStatus, error) {
	nodes, err := kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []NodeStatus
	for _, n := range nodes.Items {
		st := NodeStatus{Node: n.Name, Metrics: map[string]float64{}}
		for _, a := range n.Status.Addresses {
			if a.Type == corev1.NodeInternalIP {
				st.URL = fmt.Sprintf("http://%s:%d", a.Address, port(o))
			}
		}
		if st.URL == "" {
			st.Error = "no InternalIP"
			out = append(out, st)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if _, err := agentDo(cctx, st.URL, o.Token, http.MethodGet, "/healthz", nil); err != nil {
			st.Error = err.Error()
		} else {
			st.Healthy = true
			if b, err := agentDo(cctx, st.URL, o.Token, http.MethodGet, "/metrics", nil); err == nil {
				for _, line := range strings.Split(string(b), "\n") {
					f := strings.Fields(line)
					if len(f) != 2 || strings.HasPrefix(line, "#") {
						continue
					}
					for _, want := range []string{"lcm_agent_build_info", "lcm_agent_inflight_checkpoints", "lcm_staging_bytes", "lcm_fences", "lcm_staged_restores"} {
						if strings.HasPrefix(f[0], want) {
							var v float64
							fmt.Sscan(f[1], &v)
							st.Metrics[f[0]] = v
						}
					}
				}
			}
		}
		cancel()
		out = append(out, st)
	}
	return out, nil
}

// History merges every agent's migration history, newest last.
func History(ctx context.Context, kc kubernetes.Interface, o Options, n int) ([]api.MigrationReport, error) {
	sts, err := Status(ctx, kc, o)
	if err != nil {
		return nil, err
	}
	var all []api.MigrationReport
	for _, st := range sts {
		if !st.Healthy {
			continue
		}
		b, err := agentDo(ctx, st.URL, o.Token, http.MethodGet, fmt.Sprintf("/v1/migrations?n=%d", n), nil)
		if err != nil {
			continue
		}
		var reps []api.MigrationReport
		if json.Unmarshal(b, &reps) == nil {
			all = append(all, reps...)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Time.Before(all[j].Time) })
	if len(all) > n {
		all = all[len(all)-n:]
	}
	return all, nil
}
