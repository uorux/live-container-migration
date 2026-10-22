// Package metrics is a minimal, dependency-free Prometheus text-format registry: counters,
// gauges and histograms with labels, plus gauge callbacks evaluated at scrape time.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Registry struct {
	mu       sync.Mutex
	families []*family
	byName   map[string]*family
}

func NewRegistry() *Registry { return &Registry{byName: map[string]*family{}} }

type kind int

const (
	counterKind kind = iota
	gaugeKind
	histogramKind
)

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64
	series     map[string]*series // by joined label values
	collect    func() []Sample    // gauge callback families
}

type series struct {
	values []string
	value  float64  // counter/gauge
	counts []uint64 // histogram buckets (cumulative at write time)
	sum    float64
	count  uint64
}

// Sample is one labelled value returned by a gauge callback.
type Sample struct {
	Labels []string
	Value  float64
}

func (r *Registry) add(f *family) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[f.name]; dup {
		panic("metrics: duplicate " + f.name)
	}
	f.series = map[string]*series{}
	r.byName[f.name] = f
	r.families = append(r.families, f)
	return f
}

type Counter struct {
	r *Registry
	f *family
}
type Gauge struct {
	r *Registry
	f *family
}
type Histogram struct {
	r *Registry
	f *family
}

func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	return &Counter{r, r.add(&family{name: name, help: help, kind: counterKind, labels: labels})}
}

func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r, r.add(&family{name: name, help: help, kind: gaugeKind, labels: labels})}
}

// GaugeFunc registers a gauge family whose samples are produced by fn at every scrape.
func (r *Registry) GaugeFunc(name, help string, labels []string, fn func() []Sample) {
	r.add(&family{name: name, help: help, kind: gaugeKind, labels: labels, collect: fn})
}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return &Histogram{r, r.add(&family{name: name, help: help, kind: histogramKind, labels: labels, buckets: b})}
}

func (r *Registry) get(f *family, values []string) *series {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s wants %d label values, got %d", f.name, len(f.labels), len(values)))
	}
	key := strings.Join(values, "\x00")
	s := f.series[key]
	if s == nil {
		s = &series{values: append([]string(nil), values...)}
		if f.kind == histogramKind {
			s.counts = make([]uint64, len(f.buckets))
		}
		f.series[key] = s
	}
	return s
}

func (c *Counter) Add(v float64, labels ...string) {
	c.r.mu.Lock()
	c.r.get(c.f, labels).value += v
	c.r.mu.Unlock()
}
func (c *Counter) Inc(labels ...string) { c.Add(1, labels...) }

func (g *Gauge) Set(v float64, labels ...string) {
	g.r.mu.Lock()
	g.r.get(g.f, labels).value = v
	g.r.mu.Unlock()
}
func (g *Gauge) Add(v float64, labels ...string) {
	g.r.mu.Lock()
	g.r.get(g.f, labels).value += v
	g.r.mu.Unlock()
}

func (h *Histogram) Observe(v float64, labels ...string) {
	h.r.mu.Lock()
	s := h.r.get(h.f, labels)
	for i, ub := range h.f.buckets {
		if v <= ub {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
	h.r.mu.Unlock()
}

// Handler serves the registry in the Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteText(w)
	})
}

func (r *Registry) WriteText(w io.Writer) {
	r.mu.Lock()
	fams := append([]*family(nil), r.families...)
	r.mu.Unlock()
	for _, f := range fams {
		var samples []Sample
		if f.collect != nil {
			samples = f.collect() // outside the lock: callbacks may be slow (du, file reads)
		}
		r.mu.Lock()
		typ := map[kind]string{counterKind: "counter", gaugeKind: "gauge", histogramKind: "histogram"}[f.kind]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, escapeHelp(f.help), f.name, typ)
		if f.collect != nil {
			for _, s := range samples {
				fmt.Fprintf(w, "%s%s %s\n", f.name, labelStr(f.labels, s.Labels, "", ""), num(s.Value))
			}
		}
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := f.series[k]
			if f.kind != histogramKind {
				fmt.Fprintf(w, "%s%s %s\n", f.name, labelStr(f.labels, s.values, "", ""), num(s.value))
				continue
			}
			for i, ub := range f.buckets {
				fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelStr(f.labels, s.values, "le", num(ub)), s.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelStr(f.labels, s.values, "le", "+Inf"), s.count)
			fmt.Fprintf(w, "%s_sum%s %s\n", f.name, labelStr(f.labels, s.values, "", ""), num(s.sum))
			fmt.Fprintf(w, "%s_count%s %d\n", f.name, labelStr(f.labels, s.values, "", ""), s.count)
		}
		r.mu.Unlock()
	}
}

func labelStr(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v := ""
		if i < len(values) {
			v = values[i]
		}
		b.WriteString(n + `="` + escapeLabel(v) + `"`)
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName + `="` + extraValue + `"`)
	}
	b.WriteByte('}')
	return b.String()
}

func num(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

// DurationBuckets suits migration phases: 50 ms .. 2 min.
var DurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 12, 20, 30, 60, 120}
