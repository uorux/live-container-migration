// lcm-memcheck verifies that live migration preserves memory exactly. It fills a buffer with a
// pattern derived from each page's index and version, keeps rewriting random pages (bumping their
// version), and keeps verifying every page against its expected version. A page lost or mixed up by
// a migration (a missing pre-dump page, a bad dedup, a stale parent) fails verification, and the
// process exits 2 so the pod shows it. It also reports pauses it observes (a migration's freeze).
//
// Run it in a pod with runtimeClassName lcm and migrate the pod while it runs; tune -dirty to set
// how hard pre-copy has to work.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"
)

const page = 4096

func main() {
	sizeMiB := flag.Int("size", 512, "buffer size in MiB")
	dirty := flag.Int("dirty", 20000, "pages rewritten per second (4 KiB each)")
	report := flag.Duration("report", 10*time.Second, "status line interval")
	flag.Parse()

	pages := *sizeMiB << 20 / page
	buf := make([]byte, pages*page)
	ver := make([]uint32, pages)
	for p := range pages {
		fill(buf[p*page:(p+1)*page], p, 0)
	}
	fmt.Printf("memcheck: %d MiB (%d pages) filled; dirtying %d pages/s\n", *sizeMiB, pages, *dirty)

	var rewritten, verified, passes int
	next := 0 // next page to verify
	start, last, lastReport := time.Now(), time.Now(), time.Now()
	tick := 10 * time.Millisecond
	perTick := max(1, *dirty*int(tick/time.Millisecond)/1000)
	for {
		now := time.Now()
		if gap := now.Sub(last); gap > time.Second {
			fmt.Printf("memcheck: paused for %s (a migration's freeze?)\n", gap.Round(time.Millisecond))
		}
		last = now
		// Rewrite some pages.
		for range perTick {
			p := rand.IntN(pages)
			ver[p]++
			fill(buf[p*page:(p+1)*page], p, ver[p])
			rewritten++
		}
		// Verify a slice of the buffer every tick, so a whole pass takes a few seconds.
		for range max(1, pages/300) {
			if err := check(buf[next*page:(next+1)*page], next, ver[next]); err != nil {
				fmt.Printf("MEMCHECK FAIL after %s: %v\n", time.Since(start).Round(time.Second), err)
				os.Exit(2)
			}
			verified++
			if next++; next == pages {
				next, passes = 0, passes+1
			}
		}
		if now.Sub(lastReport) >= *report {
			fmt.Printf("memcheck: ok, %d full passes, %d pages rewritten, %d verified, up %s\n",
				passes, rewritten, verified, time.Since(start).Round(time.Second))
			lastReport = now
		}
		time.Sleep(tick)
	}
}

// word is the expected content of 8-byte word w of page p at version v.
func word(p int, v uint32, w int) uint64 {
	return (uint64(p)<<32 | uint64(v)) ^ (uint64(w) * 0x9e3779b97f4a7c15)
}

func fill(b []byte, p int, v uint32) {
	for w := range page / 8 {
		binary.LittleEndian.PutUint64(b[w*8:], word(p, v, w))
	}
}

func check(b []byte, p int, v uint32) error {
	for w := range page / 8 {
		if got := binary.LittleEndian.Uint64(b[w*8:]); got != word(p, v, w) {
			return fmt.Errorf("page %d word %d: got %#x, want %#x (version %d)", p, w, got, word(p, v, w), v)
		}
	}
	return nil
}
