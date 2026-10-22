package gateway

import (
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"lcm/internal/metrics"
	"lcm/internal/tunnel"
)

// OutageBuckets are the bucket bounds of lcm_gateway_tunnel_outage_seconds:
// the freeze players perceive while a migration is in its blackout.
var OutageBuckets = []float64{0.25, 0.5, 1, 2, 3, 5, 8, 12, 20, 30, 60, 120}

// Values of the result label of lcm_gateway_player_connections_total.
const (
	resultOpened         = "opened"
	resultClosedByPlayer = "closed_by_player" // graceful, the player's FIN came first
	resultClosedByServer = "closed_by_server" // graceful from the server side, or the server forgot the stream
	resultResetByPlayer  = "reset_by_player"  // player socket RST/error, or a held login's keepalive failed
	resultResetByServer  = "reset_by_server"  // the relay sent RESET (server RST, upstream dial failure)
	resultSessionLost    = "session_lost"     // the relay came back with a NEW session (server restarted)
	resultHoldTimeout    = "hold_timeout"     // -minecraft login hold passed -hold-max
	resultError          = "error"            // protocol error (window overrun, offset mismatch)
	resultShutdown       = "shutdown"         // gateway shutting down
)

var closeResults = map[tunnel.CloseReason]string{
	tunnel.CloseLocalFin:    resultClosedByPlayer,
	tunnel.ClosePeerFin:     resultClosedByServer,
	tunnel.CloseLocalError:  resultResetByPlayer,
	tunnel.ClosePeerReset:   resultResetByServer,
	tunnel.CloseSessionLost: resultSessionLost,
	tunnel.CloseProtocol:    resultError,
	tunnel.CloseShutdown:    resultShutdown,
	// tunnel.CloseAborted is counted by whoever called Stream.Reset.
}

// gwMetrics is the gateway's Prometheus registry. Labels are bounded: the
// backend name and fixed enums, never a player or stream id.
type gwMetrics struct {
	reg        *metrics.Registry
	connects   *metrics.Counter
	outage     *metrics.Histogram
	conns      *metrics.Counter
	bytes      *metrics.Counter
	statusResp *metrics.Counter
	wakes      *metrics.Counter

	mu        sync.Mutex // serialises syncCounters
	lastBytes map[string][2]uint64
}

func newGWMetrics(g *Gateway, version string) *gwMetrics {
	r := metrics.NewRegistry()
	m := &gwMetrics{reg: r, lastBytes: map[string][2]uint64{}}
	r.Gauge("lcm_gateway_build_info", "Build information; always 1.", "version").Set(1, version)
	r.GaugeFunc("lcm_gateway_tunnel_up", "1 while a relay tunnel is attached to the backend's session.",
		[]string{"backend"}, g.perBackend(func(b *Backend, s tunnel.Stats) float64 { return b01(s.Connected) }))
	m.connects = r.Counter("lcm_gateway_tunnel_connects_total",
		"Successful relay handshakes, by whether the session was new or resumed.", "backend", "kind")
	m.outage = r.Histogram("lcm_gateway_tunnel_outage_seconds",
		"Length of each tunnel outage that ended in a resume: the freeze players perceive during a migration.",
		OutageBuckets, "backend")
	r.GaugeFunc("lcm_gateway_tunnel_down_seconds", "Seconds since the tunnel went down (since start if it never came up); 0 while up.",
		[]string{"backend"}, g.perBackend(func(b *Backend, s tunnel.Stats) float64 {
			if s.Connected {
				return 0
			}
			return time.Since(s.DownSince).Seconds()
		}))
	r.GaugeFunc("lcm_gateway_players", "Open player connections (streams).",
		[]string{"backend"}, g.perBackend(func(b *Backend, s tunnel.Stats) float64 { return float64(s.Streams) }))
	r.GaugeFunc("lcm_gateway_held_players", "Player connections waiting: all of them while the tunnel is down, else logins held by -minecraft.",
		[]string{"backend"}, g.perBackend(func(b *Backend, s tunnel.Stats) float64 { return float64(heldPlayers(b, s)) }))
	m.conns = r.Counter("lcm_gateway_player_connections_total",
		"Player connections opened, and closed by reason.", "backend", "result")
	m.bytes = r.Counter("lcm_gateway_bytes_total", "Player payload bytes relayed.", "backend", "direction")
	r.GaugeFunc("lcm_gateway_buffered_bytes", "Bytes buffered in the gateway: player bytes not yet acknowledged by the relay plus server bytes not yet written to players.",
		[]string{"backend"}, g.perBackend(func(b *Backend, s tunnel.Stats) float64 { return float64(s.TxBuffered + s.RxQueued) }))
	m.statusResp = r.Counter("lcm_gateway_status_answered_total",
		"Server list pings answered by the gateway itself (-minecraft, tunnel down).", "backend")
	m.wakes = r.Counter("lcm_gateway_wakes_total",
		"Wake requests for a backend whose relay wasn't connected when a player arrived (-wake-url), by result.", "backend", "result")
	return m
}

// initBackend creates the backend's counter series at 0 so they exist before the first event.
func (m *gwMetrics) initBackend(name string) {
	for _, k := range []string{"new_session", "resume"} {
		m.connects.Add(0, name, k)
	}
	for _, r := range []string{resultOpened, resultClosedByPlayer, resultClosedByServer, resultResetByPlayer,
		resultResetByServer, resultSessionLost, resultHoldTimeout, resultError, resultShutdown} {
		m.conns.Add(0, name, r)
	}
	m.bytes.Add(0, name, "to_server")
	m.bytes.Add(0, name, "to_player")
	m.statusResp.Add(0, name)
	m.wakes.Add(0, name, "ok")
	m.wakes.Add(0, name, "error")
}

// syncCounters copies the sessions' byte counters into the registry.
func (m *gwMetrics) syncCounters(g *Gateway) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range g.backendList() {
		s := b.Session.Stats()
		last := m.lastBytes[b.Name]
		m.bytes.Add(float64(s.BytesFromLocal-last[0]), b.Name, "to_server")
		m.bytes.Add(float64(s.BytesToLocal-last[1]), b.Name, "to_player")
		m.lastBytes[b.Name] = [2]uint64{s.BytesFromLocal, s.BytesToLocal}
	}
}

// backendObserver feeds one backend's session events into the registry.
type backendObserver struct {
	m    *gwMetrics
	name string
}

func (o backendObserver) TunnelAttached(resumed bool, outage time.Duration) {
	if !resumed {
		o.m.connects.Inc(o.name, "new_session")
		return
	}
	o.m.connects.Inc(o.name, "resume")
	o.m.outage.Observe(outage.Seconds(), o.name)
}

func (o backendObserver) StreamClosed(why tunnel.CloseReason) {
	if r, ok := closeResults[why]; ok {
		o.m.conns.Inc(o.name, r)
	}
}

func heldPlayers(b *Backend, s tunnel.Stats) int64 {
	if !s.Connected {
		return int64(s.Streams)
	}
	return b.held.Load()
}

func b01(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// backendList returns the backends sorted by name.
func (g *Gateway) backendList() []*Backend {
	g.mu.Lock()
	bs := make([]*Backend, 0, len(g.backends))
	for _, b := range g.backends {
		bs = append(bs, b)
	}
	g.mu.Unlock()
	sort.Slice(bs, func(i, j int) bool { return bs[i].Name < bs[j].Name })
	return bs
}

func (g *Gateway) perBackend(f func(*Backend, tunnel.Stats) float64) func() []metrics.Sample {
	return func() []metrics.Sample {
		var out []metrics.Sample
		for _, b := range g.backendList() {
			out = append(out, metrics.Sample{Labels: []string{b.Name}, Value: f(b, b.Session.Stats())})
		}
		return out
	}
}

// WriteMetrics writes the gateway's metrics in the Prometheus text format.
func (g *Gateway) WriteMetrics(w io.Writer) {
	g.metrics.syncCounters(g)
	g.metrics.reg.WriteText(w)
}

func (g *Gateway) metricsHandler() http.Handler {
	h := g.metrics.reg.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.metrics.syncCounters(g)
		h.ServeHTTP(w, r)
	})
}
