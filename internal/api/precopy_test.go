package api

import "testing"

func TestPreCopyProgress(t *testing.T) {
	const G, M = 1 << 30, 1 << 20
	cases := []struct {
		name   string
		rounds []int64 // sizes in order
		stopAt int     // 1-based round after which Done is true; 0 = never
	}{
		{"converges under the target", []int64{1400 * M, 10 * M}, 2},
		{"first round already small", []int64{20 * M}, 1},
		// The two bettermc runs of 2026-10-24.
		{"one GC-noisy round doesn't stop it", []int64{5140 * M, 2800 * M, 2680 * M, 1500 * M, 1200 * M}, 0},
		{"keeps going while shrinking 10%+", []int64{5140 * M, 2850 * M, 2320 * M, 1390 * M, 1150 * M, 1005 * M, 862 * M, 769 * M}, 0},
		{"two stalls in a row stop it", []int64{4 * G, 2 * G, 2 * G, 2 * G}, 4},
		// A stall is measured against the best round, not the previous one: a rebound after a
		// good round and a small dip back don't count as progress.
		{"progress is against the best so far", []int64{4 * G, 1 * G, 1500 * M, 950 * M}, 4},
		{"a real improvement resets the count", []int64{4 * G, 2 * G, 2 * G, 1 * G, 1 * G, 1 * G}, 6},
	}
	for _, c := range cases {
		p := PreCopyProgress{TargetBytes: 64 * M}
		got := 0
		for i, s := range c.rounds {
			if p.Done(s) {
				got = i + 1
				break
			}
		}
		if got != c.stopAt {
			t.Errorf("%s: stopped after round %d, want %d", c.name, got, c.stopAt)
		}
	}
}
