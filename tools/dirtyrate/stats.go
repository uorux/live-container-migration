//go:build linux

package main

import (
	"sort"

	"lcm/internal/api"
)

type pageKey struct {
	pid int
	vpn uint64
}

// tracker remembers, for every page that was soft-dirty in any interval, in which intervals it
// was: enough to answer "how many distinct pages are dirtied in any window of k intervals".
type tracker struct {
	n, words int
	idx      map[pageKey]uint32
	bits     []uint64 // words per page
	label    []int    // label id per page
	labels   []string
	labelID  map[string]int
	uniq     []uint64 // distinct pages per label so far
}

func newTracker(intervals int) *tracker {
	return &tracker{n: intervals, words: (intervals + 63) / 64, idx: map[pageKey]uint32{}, labelID: map[string]int{}}
}

func (t *tracker) lid(label string) int {
	id, ok := t.labelID[label]
	if !ok {
		id = len(t.labels)
		t.labelID[label] = id
		t.labels = append(t.labels, label)
		t.uniq = append(t.uniq, 0)
	}
	return id
}

// record notes that pages were dirty in interval i.
func (t *tracker) record(i, pid int, pages []pageRec) {
	for _, p := range pages {
		k := pageKey{pid, p.vpn}
		id, ok := t.idx[k]
		if !ok {
			id = uint32(len(t.label))
			t.idx[k] = id
			l := t.lid(p.label)
			t.label = append(t.label, l)
			t.uniq[l]++
			for w := 0; w < t.words; w++ {
				t.bits = append(t.bits, 0)
			}
		}
		t.bits[int(id)*t.words+i/64] |= 1 << (i % 64)
	}
}

// windows returns, per label id and in total (last row), the median over all window positions
// of the number of distinct pages dirtied in a window of k consecutive intervals, k = 0..done.
func (t *tracker) windows(done int) [][]uint64 {
	nl := len(t.labels)
	// hist[l][i][d]: pages of label l whose first dirty interval at or after i is i+d-1.
	hist := make([][][]uint64, nl)
	for l := range hist {
		hist[l] = make([][]uint64, done)
		for i := range hist[l] {
			hist[l][i] = make([]uint64, done+1)
		}
	}
	for id, l := range t.label {
		b := t.bits[id*t.words : (id+1)*t.words]
		next := done
		for i := done - 1; i >= 0; i-- {
			if b[i/64]&(1<<(i%64)) != 0 {
				next = i
			}
			if next < done {
				hist[l][i][next-i+1]++
			}
		}
	}
	out := make([][]uint64, nl+1)
	for l := range out {
		out[l] = make([]uint64, done+1)
	}
	for k := 1; k <= done; k++ {
		total := make([]uint64, done-k+1)
		for l := 0; l < nl; l++ {
			vals := make([]uint64, done-k+1)
			for i := 0; i+k <= done; i++ {
				var s uint64
				for d := 1; d <= k; d++ {
					s += hist[l][i][d]
				}
				vals[i] = s
				total[i] += s
			}
			out[l][k] = median(vals)
		}
		out[nl][k] = median(total)
	}
	return out
}

func median(v []uint64) uint64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]uint64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

// dirtyAfter interpolates D (distinct dirty bytes per window of k intervals of dt seconds) at
// t seconds. capped is true when t is longer than the measurement (D's last value is a floor).
func dirtyAfter(D []float64, dt, t float64) (v float64, capped bool) {
	x := t / dt
	n := len(D) - 1
	if x >= float64(n) {
		return D[n], x > float64(n)
	}
	k := int(x)
	return D[k] + (D[k+1]-D[k])*(x-float64(k)), false
}

// predictRounds mirrors lcm-agent's pre-copy loop (internal/agent/source.go): round 1 ships all
// resident memory; each later round ships what was dirtied while the previous one was dumped and
// shipped (its size / bw + overhead seconds); stop after max rounds or by lcm's own rule
// (api.PreCopyProgress). The final dump carries what was dirtied during the last round.
func predictRounds(resident float64, D []float64, dt, bw, overhead float64, max int, target float64) (rounds []float64, final float64, capped bool) {
	progress := api.PreCopyProgress{TargetBytes: int64(target)}
	for r := 1; r <= max; r++ {
		size := resident
		if r > 1 {
			var c bool
			size, c = dirtyAfter(D, dt, rounds[len(rounds)-1]/bw+overhead)
			capped = capped || c
		}
		rounds = append(rounds, size)
		if progress.Done(int64(size)) {
			break
		}
	}
	final, c := dirtyAfter(D, dt, rounds[len(rounds)-1]/bw+overhead)
	return rounds, final, capped || c
}
