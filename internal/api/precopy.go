package api

// DefaultPreDumpMax is the default cap on pre-copy rounds. Rounds stop earlier once they converge
// or stall (PreCopyProgress); the cap only bounds a workload that keeps shrinking slowly. On a
// LAN a late round of a heavy JVM server takes ~5 s, and every one it gets still makes the final
// dump smaller (bettermc 2026-10-24 was still shrinking ~11% a round when it hit the old cap of 8).
const DefaultPreDumpMax = 12

// PreCopyProgress applies a PreDumpPolicy's stopping rule round by round. lcm-agent's pre-copy
// loop and tools/dirtyrate's prediction of it share this, so the two can't drift apart.
//
// A round that doesn't beat the smallest one so far by 10% is a stall. Pre-copy stops at the
// first round under TargetBytes, or after preCopyPatience stalls in a row: the dirty rate keeps
// up with us. One stall isn't enough, because a garbage collection landing in one round can
// make it as big as the last. bettermc (2026-10-24) went 5.14, 2.80, 2.68 GB and stopped there
// under a one-stall rule, leaving a 2.36 GB final dump; another run went on through a similar
// plateau (2.85, 2.32) to 1.39 GB and further, ending at 0.77 GB.
type PreCopyProgress struct {
	TargetBytes int64
	best        int64 // smallest round so far; 0 before the first
	stalls      int   // rounds in a row that didn't beat best by 10%
}

const preCopyPatience = 2

// Done records the size of the round that just finished and reports whether pre-copy should
// stop (the caller also stops at the policy's Max).
func (p *PreCopyProgress) Done(size int64) bool {
	if size < p.TargetBytes {
		return true
	}
	if p.best == 0 || size*10 < p.best*9 {
		p.best, p.stalls = size, 0
		return false
	}
	p.stalls++
	return p.stalls >= preCopyPatience
}
