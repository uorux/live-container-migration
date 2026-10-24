package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"lcm/internal/loadtest"
	"lcm/internal/relay"
)

var (
	metricName = `[a-zA-Z_:][a-zA-Z0-9_:]*`
	labelPair  = `[a-zA-Z_][a-zA-Z0-9_]*="(?:\\.|[^"\\])*"`
	sampleRE   = regexp.MustCompile(`^(` + metricName + `(?:\{` + labelPair + `(?:,` + labelPair + `)*\})?) (\S+)$`)
	helpRE     = regexp.MustCompile(`^# HELP (` + metricName + `) .*$`)
	typeRE     = regexp.MustCompile(`^# TYPE (` + metricName + `) (counter|gauge|histogram|summary|untyped)$`)
)

// parseExposition checks every line against the Prometheus text format and
// returns the samples keyed by `name{labels}` exactly as written.
func parseExposition(t *testing.T, text string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	typed := map[string]string{}
	for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if m := typeRE.FindStringSubmatch(line); m != nil {
			typed[m[1]] = m[2]
			continue
		}
		if helpRE.MatchString(line) {
			continue
		}
		m := sampleRE.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("line %d does not match the text format: %q", i+1, line)
		}
		v, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			t.Fatalf("line %d: bad value: %q", i+1, line)
		}
		name := m[1]
		if j := strings.IndexByte(name, '{'); j >= 0 {
			name = name[:j]
		}
		base := name
		for _, suf := range []string{"_bucket", "_sum", "_count"} {
			if b, ok := strings.CutSuffix(name, suf); ok && typed[b] == "histogram" {
				base = b
			}
		}
		if typed[base] == "" {
			t.Fatalf("line %d: sample %s before its # TYPE", i+1, name)
		}
		if _, dup := out[m[1]]; dup {
			t.Fatalf("line %d: duplicate series %s", i+1, m[1])
		}
		out[m[1]] = v
	}
	return out
}

func scrape(t *testing.T, gw *Gateway) map[string]float64 {
	t.Helper()
	var b bytes.Buffer
	gw.WriteMetrics(&b)
	return parseExposition(t, b.String())
}

const be = `{backend="default"}`

// A player's bytes cross a quiesce/resume cycle: the outage histogram gets
// one observation of about the blackout length, the gauges follow the tunnel
// and the player, and /status tells the same story.
func TestMetricsOutageAndResume(t *testing.T) {
	e := newMCEnv(t, nil)
	echo, err := loadtest.NewEchoServer("127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	go echo.Serve()
	t.Cleanup(echo.Close)
	r := relay.New(relay.Config{Gateway: e.tunnelAddr, Upstream: echo.Addr(), ProxyProtocol: true,
		Tunnel: testTunnelCfg(), BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		QuiesceTimeout: time.Minute, Logger: testLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	e.waitFor("relay connected", e.session().Connected)

	m := scrape(t, e.gw)
	for k, want := range map[string]float64{
		`lcm_gateway_build_info{version="dev"}`:                                   1,
		"lcm_gateway_tunnel_up" + be:                                              1,
		"lcm_gateway_tunnel_down_seconds" + be:                                    0,
		`lcm_gateway_tunnel_connects_total{backend="default",kind="new_session"}`: 1,
		`lcm_gateway_tunnel_connects_total{backend="default",kind="resume"}`:      0,
		"lcm_gateway_players" + be:                                                0,
	} {
		if m[k] != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}

	p := e.dial()
	msg := []byte("hello through the tunnel")
	p.c.Write(msg)
	p.expect(msg)
	m = scrape(t, e.gw)
	if m["lcm_gateway_players"+be] != 1 || m[`lcm_gateway_player_connections_total{backend="default",result="opened"}`] != 1 {
		t.Fatalf("player not counted: %v", m)
	}
	if got := m[`lcm_gateway_bytes_total{backend="default",direction="to_server"}`]; got != float64(len(msg)) {
		t.Fatalf("to_server bytes %v", got)
	}
	// The gateway counts bytes to the player after writing them, so the
	// player may read them first.
	e.waitFor("to_player bytes counted", func() bool {
		return scrape(t, e.gw)[`lcm_gateway_bytes_total{backend="default",direction="to_player"}`] == float64(len(msg))
	})

	if err := r.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.waitFor("tunnel down", func() bool { return !e.session().Connected() })
	during := []byte("typed during the blackout")
	p.c.Write(during)
	e.waitFor("player bytes buffered", func() bool { return e.session().Stats().TxBuffered >= len(during) })
	buffered := float64(e.session().Stats().TxBuffered)
	const blackout = 400 * time.Millisecond
	time.Sleep(blackout)
	m = scrape(t, e.gw)
	if m["lcm_gateway_tunnel_up"+be] != 0 || m["lcm_gateway_held_players"+be] != 1 ||
		m["lcm_gateway_buffered_bytes"+be] != buffered || m["lcm_gateway_tunnel_down_seconds"+be] < blackout.Seconds() {
		t.Fatalf("during outage: up=%v held=%v buffered=%v down=%v", m["lcm_gateway_tunnel_up"+be],
			m["lcm_gateway_held_players"+be], m["lcm_gateway_buffered_bytes"+be], m["lcm_gateway_tunnel_down_seconds"+be])
	}
	if _, ok := m["lcm_gateway_tunnel_outage_seconds_count"+be]; ok {
		t.Fatal("outage observed before the resume")
	}
	st := e.gw.Status(false).Backends[0]
	if st.Connected || st.DownSince.IsZero() || st.DownForSeconds < blackout.Seconds() || st.HeldPlayers != 1 {
		t.Fatalf("status during outage: %+v", st)
	}

	r.Resume()
	p.expect(during)
	e.waitFor("buffer acked", func() bool { return e.session().Stats().TxBuffered == 0 })
	m = scrape(t, e.gw)
	if got := m["lcm_gateway_tunnel_outage_seconds_count"+be]; got != 1 {
		t.Fatalf("outage count %v", got)
	}
	if sum := m["lcm_gateway_tunnel_outage_seconds_sum"+be]; sum < blackout.Seconds() || sum > 5 {
		t.Fatalf("outage sum %v", sum)
	}
	if m[`lcm_gateway_tunnel_outage_seconds_bucket{backend="default",le="0.25"}`] != 0 ||
		m[`lcm_gateway_tunnel_outage_seconds_bucket{backend="default",le="+Inf"}`] != 1 {
		t.Fatal("outage buckets wrong")
	}
	if m[`lcm_gateway_tunnel_connects_total{backend="default",kind="resume"}`] != 1 ||
		m["lcm_gateway_tunnel_up"+be] != 1 || m["lcm_gateway_held_players"+be] != 0 || m["lcm_gateway_players"+be] != 1 {
		t.Fatalf("after resume: %v", m)
	}
	if got := m[`lcm_gateway_bytes_total{backend="default",direction="to_server"}`]; got != float64(len(msg)+len(during)) {
		t.Fatalf("to_server bytes %v", got)
	}

	// /status over HTTP carries the same facts.
	srv := httptest.NewServer(e.gw.AdminHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var doc Status
	err = json.NewDecoder(resp.Body).Decode(&doc)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	st = doc.Backends[0]
	if !st.Connected || st.UpSince.IsZero() || st.LastOutageSeconds < blackout.Seconds() || st.LastOutage == "" ||
		st.Streams != 1 || st.Resumes != 1 || st.NewSessions != 1 || st.BytesToPlayer != uint64(len(msg)+len(during)) || doc.Version != "dev" {
		t.Fatalf("status after resume: %+v", doc)
	}

	// The player half-closes first: a graceful close by the player.
	p.c.(*net.TCPConn).CloseWrite()
	p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.Copy(io.Discard, p.br); err != nil {
		t.Fatal(err)
	}
	p.c.Close()
	e.waitFor("stream gone", func() bool { return e.session().NumStreams() == 0 })
	m = scrape(t, e.gw)
	if m[`lcm_gateway_player_connections_total{backend="default",result="closed_by_player"}`] != 1 || m["lcm_gateway_players"+be] != 0 {
		t.Fatalf("close not counted: %v", m)
	}
}

// GET /metrics on the admin handler serves the text format.
func TestMetricsEndpoint(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{MOTD: "Down"})
	statusExchange(t, e.dial(), 774) // answered by the gateway: no relay yet
	srv := httptest.NewServer(e.gw.AdminHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var text strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		text.WriteString(sc.Text() + "\n")
	}
	m := parseExposition(t, text.String())
	if m["lcm_gateway_status_answered_total"+be] != 1 {
		t.Fatalf("status_answered_total = %v", m["lcm_gateway_status_answered_total"+be])
	}
	if m["lcm_gateway_tunnel_up"+be] != 0 || m["lcm_gateway_tunnel_down_seconds"+be] <= 0 {
		t.Fatal("never-connected tunnel should be down since start")
	}
	if e.gw.Status(false).Backends[0].StatusAnswered != 1 {
		t.Fatal("status_answered missing from /status")
	}
	if r, err := http.Post(srv.URL+"/metrics", "text/plain", nil); err == nil {
		r.Body.Close()
		if r.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST /metrics: %d", r.StatusCode)
		}
	}
}

func TestMetricsHoldTimeout(t *testing.T) {
	e := newMCEnv(t, &MinecraftConfig{HoldInterval: testInterval, MaxHold: 300 * time.Millisecond, HoldMessage: "bye"})
	p := e.login(774, "Steve")
	p.skipKeepalives()
	e.waitFor("stream dropped", func() bool { return e.session().NumStreams() == 0 })
	m := scrape(t, e.gw)
	if m[`lcm_gateway_player_connections_total{backend="default",result="hold_timeout"}`] != 1 ||
		m[`lcm_gateway_player_connections_total{backend="default",result="opened"}`] != 1 {
		t.Fatalf("hold timeout not counted: %v", m)
	}
}
