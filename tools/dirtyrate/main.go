//go:build linux

// Command dirtyrate measures how fast a process (or every process of a cgroup) dirties the
// memory CRIU would dump, using the kernel's soft-dirty bits, without migrating anything.
// The usage text below is its documentation.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"lcm/internal/stage"
)

const usage = `dirtyrate: measure a process's memory dirty rate the way CRIU pre-copy sees it.

  dirtyrate -pid 1234 [-interval 2s] [-duration 60s]
  dirtyrate -cgroup /sys/fs/cgroup/kubepods.slice/.../cri-containerd-<id>.scope
  dirtyrate -pid 1234 -labels map.txt -by-label -bw 60M

!!! DO NOT RUN THIS ON A PROCESS THAT IS BEING MIGRATED, OR MAY START MIGRATING WHILE IT RUNS !!!
It resets the process's soft-dirty bits (writes "4" to /proc/PID/clear_refs) every interval.
CRIU pre-copy relies on the same bits: a reset between two pre-dumps makes CRIU skip pages that
changed, and the restored process silently runs with STALE MEMORY. Pause automatic migrations of
the target (and don't run lcm migrate) for the duration. As a safety net dirtyrate refuses to
start, and stops before its next reset, if a criu process is running or an lcm checkpoint
directory (` + "$LCM_STATE_DIR or /var/lib/lcm" + `/ckpt/*) is in progress; -force skips that check.

Otherwise it is read-only and safe on production processes: it reads /proc/PID/smaps and
/proc/PID/pagemap. Costs to the target: each reset write-protects its pages, so the next write
to each page takes a minor fault (the same cost every CRIU pre-dump round imposes), and the page
table walks hold the target's mmap lock for read for a few ms per GiB.

Privileges: soft-dirty bits need ptrace-read access to the target (same user, or root);
clear_refs needs write access to it (the process owner, or root). Page frame numbers (used only
to skip the shared zero page, as CRIU does) need CAP_SYS_ADMIN; without them the zero page counts.

What is counted (as CRIU dumps it): resident or swapped pages of private mappings, except pages
of private file mappings that were never written (still the file's). Shared memfd / shared
anonymous / SysV mappings (ZGC's heap is a memfd!) are reported separately: CRIU dumps those
WHOLE in the final dump, never incrementally.

Per interval it prints the pages dirtied since the previous reset, the dumpable resident set, the
distinct pages dirtied since the start, and (new-vma) how many dirty pages sit in VMAs created or
expanded during the interval: the kernel reports EVERY page of such a VMA as soft-dirty, so a JVM
that commits more heap (mmap over its reservation, merging with the committed heap mapping) makes
CRIU re-dump the whole heap (seen as smaps VmFlags "sd", or as a >= 64 MiB VMA dirty in full).
At the end: distinct pages per window of -window (a pre-copy round's duration: what each round
re-ships, and the final dump's size), and with -bw a prediction of lcm's pre-copy rounds.

Labels: -labels takes 'jcmd <pid> System.map' output (JDK 21.0.x+; start the JVM with
-XX:NativeMemoryTracking=summary so its "vm info" column names javaheap, meta, class, code,
stack, gc, ...) or lines "0xSTART-0xEND name". Unlabelled mappings are anon, brk-heap,
main-stack, file-private or shared:<name>.

Flags:
`

type flags struct {
	pids               []int
	cgroup             string
	interval, duration time.Duration
	window, overhead   time.Duration
	labels             string
	byLabel            bool
	jsonOut            string
	bw, target         float64
	rounds             int
	force              bool
}

func main() {
	var f flags
	var pidList, bw, target string
	flag.StringVar(&pidList, "pid", "", "process ID(s), comma-separated")
	flag.StringVar(&f.cgroup, "cgroup", "", "cgroup v2 directory (absolute, or relative to /sys/fs/cgroup): measure all its processes, re-reading cgroup.procs each interval")
	flag.DurationVar(&f.interval, "interval", 2*time.Second, "sampling interval")
	flag.DurationVar(&f.duration, "duration", 60*time.Second, "how long to measure")
	flag.DurationVar(&f.window, "window", 10*time.Second, "window for the distinct-dirty summary (about one pre-copy round)")
	flag.StringVar(&f.labels, "labels", "", "file with 'jcmd PID System.map' output or 'START-END name' lines (single-process use)")
	flag.BoolVar(&f.byLabel, "by-label", false, "print the per-label breakdown every interval")
	flag.StringVar(&f.jsonOut, "json", "", "also write intervals and summary as JSON to this file")
	flag.StringVar(&bw, "bw", "", "predict lcm pre-copy rounds at this dump+ship rate in bytes/s (e.g. 60M, 180M)")
	flag.DurationVar(&f.overhead, "overhead", time.Second, "fixed per-round cost for -bw (CRIU start, freeze, http)")
	flag.IntVar(&f.rounds, "rounds", 4, "max pre-dump rounds for -bw (lcm migrate -predump-max)")
	flag.StringVar(&target, "target", "64M", "pre-copy stop size for -bw (lcm migrate -predump-target)")
	flag.BoolVar(&f.force, "force", false, "skip the in-progress-migration check (see the warning above)")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	for _, p := range strings.Split(pidList, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			fatal("bad -pid %q", p)
		}
		f.pids = append(f.pids, n)
	}
	var err error
	if f.bw, err = parseSize(bw); err != nil {
		fatal("-bw: %v", err)
	}
	if f.target, err = parseSize(target); err != nil {
		fatal("-target: %v", err)
	}
	if (len(f.pids) == 0) == (f.cgroup == "") {
		flag.Usage()
		os.Exit(2)
	}
	if f.cgroup != "" && !filepath.IsAbs(f.cgroup) {
		f.cgroup = filepath.Join("/sys/fs/cgroup", f.cgroup)
	}
	if f.interval <= 0 || f.duration < f.interval {
		fatal("need 0 < -interval <= -duration")
	}
	if err := run(f); err != nil {
		fatal("%v", err)
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "dirtyrate: "+format+"\n", a...)
	os.Exit(1)
}

type intervalOut struct {
	T          float64           `json:"t"`
	Dirty      uint64            `json:"dirtyBytes"`
	Resident   uint64            `json:"residentBytes"`
	Unique     uint64            `json:"uniqueBytes"`
	NewVMA     uint64            `json:"newVMADirtyBytes"`
	Shared     uint64            `json:"sharedResidentBytes,omitempty"`
	SharedDirt uint64            `json:"sharedDirtyBytes,omitempty"`
	AnonHuge   uint64            `json:"anonHugeBytes"`
	ScanMS     int64             `json:"scanMs"`
	ByLabel    map[string]uint64 `json:"dirtyByLabel,omitempty"`
	ResByLabel map[string]uint64 `json:"residentByLabel,omitempty"`
}

func run(f flags) error {
	var labels labelSet
	if f.labels != "" {
		b, err := os.Open(f.labels)
		if err != nil {
			return err
		}
		labels, err = parseLabels(b)
		b.Close()
		if err != nil {
			return err
		}
		if len(labels) == 0 {
			return fmt.Errorf("%s: no labelled ranges found", f.labels)
		}
	}
	if !f.force {
		if why := migrationInProgress(); why != "" {
			return fmt.Errorf("refusing to run: %s (a soft-dirty reset now would corrupt that migration; -force overrides)", why)
		}
	}
	zeroPFN := findZeroPFN()
	n := int(f.duration / f.interval)
	tr := newTracker(n)
	ps := pageSize

	pids := map[int]bool{}
	refresh := func() error {
		cur := f.pids
		if f.cgroup != "" {
			var err error
			if cur, err = cgroupProcs(f.cgroup); err != nil {
				return err
			}
		}
		for _, p := range cur {
			if !pids[p] {
				if err := clearSoftDirty(p); err != nil {
					if len(pids) == 0 && f.cgroup == "" {
						return fmt.Errorf("pid %d: %w", p, err)
					}
					continue
				}
				pids[p] = true
			}
		}
		if len(pids) == 0 {
			return errors.New("no processes to measure")
		}
		return nil
	}
	if err := refresh(); err != nil {
		return err
	}
	if mode, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/enabled"); err == nil {
		fmt.Printf("# THP enabled: %s", mode)
	}
	fmt.Printf("# %d process(es), interval %s, duration %s, page %d B, zero-page detection %v\n",
		len(pids), f.interval, f.duration, ps, zeroPFN != 0)
	fmt.Printf("# %7s %10s %12s %10s %10s %10s %9s %6s\n", "t", "dirty", "rate", "new-vma", "resident", "distinct", "thp", "scan")

	var outs []intervalOut
	var lastRes map[string]uint64
	var lastShared uint64
	start := time.Now()
	stopped := ""
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	done := 0
loop:
	for i := 0; i < n; i++ {
		select {
		case <-time.After(time.Until(start.Add(time.Duration(i+1) * f.interval))):
		case <-sig:
			stopped = "interrupted"
			break loop
		}
		if !f.force {
			if why := migrationInProgress(); why != "" {
				stopped = "stopped early: " + why
				break
			}
		}
		t0 := time.Now()
		o := intervalOut{T: time.Since(start).Seconds(), ByLabel: map[string]uint64{}, ResByLabel: map[string]uint64{}}
		for p := range pids {
			r, err := scanProcess(p, labels, zeroPFN)
			if err == nil {
				err = clearSoftDirty(p)
			}
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
					delete(pids, p) // exited
					continue
				}
				return fmt.Errorf("pid %d: %w", p, err)
			}
			tr.record(i, p, r.dirty)
			for _, d := range r.dirty {
				o.ByLabel[d.label] += ps
			}
			for l, v := range r.resident {
				o.ResByLabel[l] += v * ps
				o.Resident += v * ps
			}
			o.NewVMA += r.wholeVMA * ps
			o.Dirty += uint64(len(r.dirty)) * ps
			o.Shared += r.sharedRes * ps
			o.SharedDirt += r.sharedDirty * ps
			o.AnonHuge += r.anonHuge
		}
		o.ScanMS = time.Since(t0).Milliseconds()
		o.Unique = uint64(len(tr.label)) * ps
		outs = append(outs, o)
		lastRes, lastShared = o.ResByLabel, o.Shared
		done = i + 1
		fmt.Printf("  %6.1fs %10s %10s/s %10s %10s %10s %9s %4dms\n", o.T, hb(o.Dirty), hb(uint64(float64(o.Dirty)/f.interval.Seconds())),
			hb(o.NewVMA), hb(o.Resident), hb(o.Unique), hb(o.AnonHuge), o.ScanMS)
		if o.Shared > 0 {
			fmt.Printf("  %8s shared memfd/anon (not pre-dumped): %s resident, %s dirty\n", "", hb(o.Shared), hb(o.SharedDirt))
		}
		if f.byLabel {
			for _, l := range sortedKeys(o.ResByLabel) {
				if o.ByLabel[l] > 0 || o.ResByLabel[l] >= 1<<20 {
					fmt.Printf("  %8s %10s %12s %10s %10s  %s\n", "", hb(o.ByLabel[l]), "", "", hb(o.ResByLabel[l]), l)
				}
			}
		}
		if f.cgroup != "" {
			if err := refresh(); err != nil {
				stopped = err.Error()
				break
			}
		}
	}
	if stopped != "" {
		fmt.Printf("# %s\n", stopped)
	}
	if done == 0 {
		return errors.New("no complete interval")
	}
	return summarize(f, tr, outs, done, lastRes, lastShared)
}

func summarize(f flags, tr *tracker, outs []intervalOut, done int, lastRes map[string]uint64, lastShared uint64) error {
	ps := float64(pageSize)
	dt := f.interval.Seconds()
	W := tr.windows(done)
	total := W[len(W)-1]
	k := int(math.Round(f.window.Seconds() / dt))
	k = max(1, min(k, done))

	var dirty []float64
	var res uint64
	for _, o := range outs {
		dirty = append(dirty, float64(o.Dirty))
	}
	res = outs[len(outs)-1].Resident
	sort.Float64s(dirty)
	fmt.Printf("\n# summary over %d intervals of %s\n", done, f.interval)
	fmt.Printf("#   dirty per interval: median %s, max %s (%s/s median)\n",
		hb(uint64(dirty[len(dirty)/2])), hb(uint64(dirty[len(dirty)-1])), hb(uint64(dirty[len(dirty)/2]/dt)))
	fmt.Printf("#   distinct dirty in a %s window (median): %s = %.0f%% of %s resident\n",
		time.Duration(float64(k)*dt*float64(time.Second)), hb(total[k]*pageSize), pct(total[k]*pageSize, res), hb(res))
	fmt.Printf("#   distinct dirty over the whole run: %s = %.0f%% of resident\n", hb(total[done]*pageSize), pct(total[done]*pageSize, res))
	fmt.Printf("#   distinct dirty vs window: ")
	for _, kk := range []int{1, 2, 5, 10, 15, 30} {
		if kk <= done {
			fmt.Printf(" %gs=%s", float64(kk)*dt, hb(total[kk]*pageSize))
		}
	}
	fmt.Println()
	if lastShared > 0 {
		fmt.Printf("#   WARNING: %s resident in shared memfd/anonymous mappings: CRIU dumps these whole in the final dump (no pre-copy)\n", hb(lastShared))
	}
	if len(tr.labels) > 1 || len(lastRes) > 1 {
		fmt.Printf("#\n#   %-22s %10s %12s %12s %12s\n", "label", "resident", "dirty/intv", "distinct/win", "distinct/all")
		per := map[string][3]uint64{}
		for id, l := range tr.labels {
			per[l] = [3]uint64{0, W[id][k] * pageSize, W[id][done] * pageSize}
		}
		meds := map[string]uint64{}
		for _, l := range sortedKeys(lastRes) {
			var v []uint64
			for _, o := range outs {
				v = append(v, o.ByLabel[l])
			}
			meds[l] = median(v)
		}
		for _, l := range sortedKeys(lastRes) {
			fmt.Printf("#   %-22s %10s %12s %12s %12s\n", l, hb(lastRes[l]), hb(meds[l]), hb(per[l][1]), hb(per[l][2]))
		}
	}
	D := make([]float64, done+1)
	for i := range D {
		D[i] = float64(total[i]) * ps
	}
	type prediction struct {
		BW      float64   `json:"bwBytesPerSec"`
		Rounds  []float64 `json:"roundBytes"`
		Final   float64   `json:"finalBytes"`
		Capped  bool      `json:"lowerBound"`
		ShipSec float64   `json:"finalShipSec"`
	}
	var pred *prediction
	if f.bw > 0 {
		rounds, final, capped := predictRounds(float64(res), D, dt, f.bw, f.overhead.Seconds(), f.rounds, f.target)
		final += float64(lastShared) // shared memfd/anon memory goes into every final dump whole
		pred = &prediction{f.bw, rounds, final, capped, final / f.bw}
		fmt.Printf("#\n#   predicted lcm pre-copy at %s/s (+%s/round): rounds", hb(uint64(f.bw)), f.overhead)
		for _, r := range rounds {
			fmt.Printf(" %s", hb(uint64(r)))
		}
		ge := ""
		if capped {
			ge = ">="
		}
		fmt.Printf(", final dump %s%s (ships in ~%.1fs, before restore)\n", ge, hb(uint64(final)), final/f.bw)
		if capped {
			fmt.Printf("#   (a round outlasted the measurement; run longer for an exact figure)\n")
		}
	}
	if f.jsonOut != "" {
		b, _ := json.MarshalIndent(map[string]any{
			"intervalSec": dt, "intervals": outs, "distinctByWindow": D, "residentBytes": res,
			"labels": tr.labels, "prediction": pred,
		}, "", "  ")
		return os.WriteFile(f.jsonOut, b, 0o644)
	}
	return nil
}

func cgroupProcs(dir string) ([]int, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	var out []int
	for _, l := range strings.Fields(string(b)) {
		if n, err := strconv.Atoi(l); err == nil {
			out = append(out, n)
		}
	}
	// Child cgroups (e.g. a container's processes under a pod slice).
	subs, _ := filepath.Glob(filepath.Join(dir, "*", "cgroup.procs"))
	for _, s := range subs {
		more, err := cgroupProcs(filepath.Dir(s))
		if err == nil {
			out = append(out, more...)
		}
	}
	return out, nil
}

// migrationInProgress reports a running criu, or an lcm source checkpoint that hasn't reached its
// final dump (pre-copy runs criu only intermittently, so the process check alone isn't enough).
func migrationInProgress() string {
	comms, _ := filepath.Glob("/proc/[0-9]*/comm")
	for _, c := range comms {
		if b, err := os.ReadFile(c); err == nil && strings.TrimSpace(string(b)) == "criu" {
			return "criu is running (" + filepath.Dir(c) + ")"
		}
	}
	dirs, _ := filepath.Glob(filepath.Join(stage.Root(), "ckpt", "*"))
	for _, d := range dirs {
		st, err := os.Stat(d)
		if err != nil || !st.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(d, "DUMPED")); err == nil {
			continue
		}
		if time.Since(st.ModTime()) < 30*time.Minute {
			return "lcm checkpoint in progress: " + d
		}
	}
	return ""
}

func parseSize(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	mult := 1.0
	switch s[len(s)-1] {
	case 'k', 'K':
		mult = 1 << 10
	case 'm', 'M':
		mult = 1 << 20
	case 'g', 'G':
		mult = 1 << 30
	}
	if mult != 1 {
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(s, 64)
	return v * mult, err
}

func hb(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0fKiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%dB", b)
}

func pct(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

func sortedKeys(m map[string]uint64) []string {
	var k []string
	for l := range m {
		k = append(k, l)
	}
	sort.Slice(k, func(i, j int) bool { return m[k[i]] > m[k[j]] })
	return k
}
