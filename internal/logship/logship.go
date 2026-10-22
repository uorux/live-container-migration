// Package logship sends log lines to VictoriaLogs' jsonline ingest endpoint: lcm-agent's own
// slog records (via Handler) and lines tailed from files (lcm-runc's log). Best effort: a bounded
// queue that drops (and counts) when VictoriaLogs is unreachable for long, so logging can never
// block a migration.
package logship

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Shipper struct {
	url     string
	host    string
	q       chan map[string]any
	Dropped atomic.Int64
	Sent    atomic.Int64
}

// New ships to baseURL (e.g. http://10.0.0.11:30428), stream fields host and unit.
func New(baseURL string) *Shipper {
	host, _ := os.Hostname()
	return &Shipper{
		url:  strings.TrimRight(baseURL, "/") + "/insert/jsonline?_stream_fields=host,unit&_msg_field=_msg&_time_field=_time",
		host: host,
		q:    make(chan map[string]any, 20000),
	}
}

func (s *Shipper) enqueue(rec map[string]any) {
	select {
	case s.q <- rec:
	default:
		s.Dropped.Add(1)
	}
}

// Run batches and posts queued records until ctx ends.
func (s *Shipper) Run(ctx context.Context) {
	client := &http.Client{Timeout: 10 * time.Second}
	var batch []map[string]any
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, r := range batch {
			enc.Encode(r)
		}
		for attempt := 0; attempt < 3; attempt++ {
			resp, err := client.Post(s.url, "application/stream+json", bytes.NewReader(buf.Bytes()))
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					s.Sent.Add(int64(len(batch)))
					batch = batch[:0]
					return
				}
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		// Keep the most recent records if VictoriaLogs stays down.
		if len(batch) > 5000 {
			s.Dropped.Add(int64(len(batch) - 5000))
			batch = append(batch[:0], batch[len(batch)-5000:]...)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case r := <-s.q:
			batch = append(batch, r)
			if len(batch) >= 500 {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

// Handler returns a slog.Handler that ships records (as unit) and also passes them to next.
func (s *Shipper) Handler(unit string, next slog.Handler) slog.Handler {
	return &handler{s: s, unit: unit, next: next}
}

type handler struct {
	s     *Shipper
	unit  string
	next  slog.Handler
	attrs []slog.Attr
	group string
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool { return h.next.Enabled(ctx, l) }

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	rec := map[string]any{"_time": r.Time.Format(time.RFC3339Nano), "_msg": r.Message, "host": h.s.host, "unit": h.unit, "level": r.Level.String()}
	add := func(a slog.Attr) {
		k := a.Key
		if h.group != "" {
			k = h.group + "." + k
		}
		rec[k] = a.Value.Resolve().String()
	}
	for _, a := range h.attrs {
		add(a)
	}
	r.Attrs(func(a slog.Attr) bool { add(a); return true })
	h.s.enqueue(rec)
	return h.next.Handle(ctx, r)
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	return &handler{s: h.s, unit: h.unit, next: h.next.WithAttrs(as), attrs: append(append([]slog.Attr{}, h.attrs...), as...), group: h.group}
}

func (h *handler) WithGroup(g string) slog.Handler {
	return &handler{s: h.s, unit: h.unit, next: h.next.WithGroup(g), attrs: h.attrs, group: g}
}

// Tail ships lines appended to path (starting at its current end) as unit, until ctx ends.
func (s *Shipper) Tail(ctx context.Context, path, unit string) {
	var off int64 = -1
	for {
		if f, err := os.Open(path); err == nil {
			fi, _ := f.Stat()
			if off < 0 || (fi != nil && fi.Size() < off) {
				if off < 0 && fi != nil {
					off = fi.Size()
				} else {
					off = 0
				}
			}
			f.Seek(off, io.SeekStart)
			br := bufio.NewReader(f)
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					break
				}
				off += int64(len(line))
				s.enqueue(map[string]any{"_time": time.Now().Format(time.RFC3339Nano), "_msg": strings.TrimRight(line, "\n"), "host": s.host, "unit": unit})
			}
			f.Close()
		} else if off < 0 {
			off = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
