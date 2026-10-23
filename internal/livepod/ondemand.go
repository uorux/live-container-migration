package livepod

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"lcm/internal/api"
	"lcm/internal/controller"
)

// On-demand LivePods (spec.onDemand) sleep when nobody uses them and wake when a player
// arrives:
//
//   - The controller asks the pod's lcm-relay (through its node's agent) how many player
//     connections it carries, over all gateways. After idleAfter with none, it marks the
//     LivePod asleep (status.asleep) and deletes the pod; the server gets SIGTERM and its
//     terminationGracePeriodSeconds to save the world.
//   - While asleep no pod is created. A gateway holding a player for the backend calls an
//     agent's POST /v1/wake/{backend}, which clears status.asleep (Wake); the controller then
//     creates the pod as usual, once the old one is fully gone. The gateway holds the player
//     through the boot.
//
// Migrations hold the LivePod's Lease; an idle LivePod isn't put to sleep while held.

type onDemand struct {
	backend   string
	idleAfter time.Duration
	// startAsleep: a new LivePod starts asleep (no pod until the first wake),
	// so applying many on-demand servers at once doesn't boot them all.
	startAsleep bool
}

func onDemandSpec(lp *unstructured.Unstructured) (onDemand, bool) {
	m, found, _ := unstructured.NestedMap(lp.Object, "spec", "onDemand")
	if !found {
		return onDemand{}, false
	}
	od := onDemand{backend: lp.GetName(), idleAfter: 10 * time.Minute}
	if b, ok := m["backend"].(string); ok && b != "" {
		od.backend = b
	}
	if s, ok := m["idleAfter"].(string); ok && s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			od.idleAfter = d
		}
	}
	od.startAsleep, _ = m["startAsleep"].(bool)
	return od, true
}

// statusNew reports whether the controller has never written this LivePod's status.
func statusNew(lp *unstructured.Unstructured) bool {
	p, _, _ := unstructured.NestedString(lp.Object, "status", "phase")
	return p == ""
}

func statusAsleep(lp *unstructured.Unstructured) bool {
	b, _, _ := unstructured.NestedBool(lp.Object, "status", "asleep")
	return b
}

// idle records whether pod has players and reports whether it has had none for idleAfter.
// Unknown counts as busy: never stop a server on a failed query.
func (c *Controller) idle(ctx context.Context, key string, p *corev1.Pod, od onDemand) bool {
	n, err := c.players(ctx, p)
	if err != nil {
		c.Log.Debug("livepod: player count unavailable", "pod", p.Name, "err", err)
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		c.active = map[string]time.Time{}
	}
	last, seen := c.active[key]
	if !seen || err != nil || n > 0 {
		c.active[key] = now
		return false
	}
	return now.Sub(last) >= od.idleAfter
}

func (c *Controller) touch(key string) {
	c.mu.Lock()
	if c.active == nil {
		c.active = map[string]time.Time{}
	}
	c.active[key] = time.Now()
	c.mu.Unlock()
}

func (c *Controller) players(ctx context.Context, p *corev1.Pod) (int, error) {
	if c.Players != nil {
		return c.Players(ctx, p)
	}
	id := ""
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Running != nil {
			id = strings.TrimPrefix(cs.ContainerID, "containerd://")
			break
		}
	}
	if id == "" {
		return 0, fmt.Errorf("no running container")
	}
	port := c.AgentPort
	if port == 0 {
		port = api.DefaultPort
	}
	base, err := controller.AgentURL(ctx, c.Kube, p.Spec.NodeName, port)
	if err != nil {
		return 0, err
	}
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(qctx, http.MethodGet, base+"/v1/relay?container="+id, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var rp struct {
		Players int `json:"players"`
	}
	return rp.Players, json.NewDecoder(resp.Body).Decode(&rp)
}

// Wake clears status.asleep on every on-demand LivePod serving backend and returns their
// names (awake ones included). Any agent can call it; the leading controller acts on it.
func (c *Controller) Wake(ctx context.Context, backend string) ([]string, error) {
	list, err := c.Dyn.Resource(GVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var names []string
	for i := range list.Items {
		lp := &list.Items[i]
		od, ok := onDemandSpec(lp)
		if !ok || od.backend != backend {
			continue
		}
		names = append(names, lp.GetNamespace()+"/"+lp.GetName())
		if !statusAsleep(lp) {
			continue
		}
		cp := lp.DeepCopy()
		unstructured.SetNestedField(cp.Object, false, "status", "asleep")
		unstructured.SetNestedField(cp.Object, "Waking", "status", "phase")
		if _, err := c.Dyn.Resource(GVR).Namespace(lp.GetNamespace()).UpdateStatus(ctx, cp, metav1.UpdateOptions{}); err != nil {
			return names, fmt.Errorf("waking %s/%s: %w", lp.GetNamespace(), lp.GetName(), err)
		}
		c.Log.Info("livepod: woken", "namespace", lp.GetNamespace(), "livepod", lp.GetName(), "backend", backend)
	}
	return names, nil
}
