package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("lcm_things_total", "Things.", "result")
	c.Inc("ok")
	c.Add(2, "ok")
	c.Inc(`we"ird`)
	g := r.Gauge("lcm_up", "Up.")
	g.Set(1)
	h := r.Histogram("lcm_dur_seconds", "Durations.", []float64{1, 5}, "phase")
	h.Observe(0.5, "dump")
	h.Observe(3, "dump")
	h.Observe(9, "dump")
	r.GaugeFunc("lcm_files", "Files.", []string{"dir"}, func() []Sample {
		return []Sample{{Labels: []string{"fence"}, Value: 4}}
	})
	var b bytes.Buffer
	r.WriteText(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE lcm_things_total counter",
		`lcm_things_total{result="ok"} 3`,
		`lcm_things_total{result="we\"ird"} 1`,
		"lcm_up 1",
		`lcm_dur_seconds_bucket{phase="dump",le="1"} 1`,
		`lcm_dur_seconds_bucket{phase="dump",le="5"} 2`,
		`lcm_dur_seconds_bucket{phase="dump",le="+Inf"} 3`,
		`lcm_dur_seconds_sum{phase="dump"} 12.5`,
		`lcm_dur_seconds_count{phase="dump"} 3`,
		`lcm_files{dir="fence"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
