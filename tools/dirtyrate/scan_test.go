//go:build linux

package main

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

func TestParseMapsLine(t *testing.T) {
	for _, c := range []struct {
		line  string
		kind  kind
		label string
		path  string
	}{
		{"7f0000000000-7f0000100000 rw-p 00000000 00:00 0 ", kindPrivate, "anon", ""},
		{"7f0000000000-7f0000100000 rw-p 00000000 00:00 0", kindPrivate, "anon", ""},
		{"7f0000000000-7f0000100000 ---p 00000000 00:00 0", kindSkip, "", ""},
		{"55d0c000-55d0e000 rw-p 00000000 00:00 0                          [heap]", kindPrivate, "brk-heap", "[heap]"},
		{"7ffd0000-7ffd2000 rw-p 00000000 00:00 0                          [stack]", kindPrivate, "main-stack", "[stack]"},
		{"7f01000-7f02000 rw-p 0001c000 fd:01 1234     /usr/lib/libc.so.6", kindPrivateFile, "file-private", "/usr/lib/libc.so.6"},
		{"7f01000-7f02000 rw-s 00000000 00:01 77       /memfd:java_heap (deleted)", kindShared, "shared:memfd:java_heap", "/memfd:java_heap (deleted)"},
		{"7f01000-7f02000 rw-s 00000000 00:01 77       /dev/zero (deleted)", kindShared, "shared:dev/zero", "/dev/zero (deleted)"},
		{"7f01000-7f02000 rw-s 00000000 fd:01 99       /tmp/hsperfdata_mc/7", kindSkip, "", "/tmp/hsperfdata_mc/7"},
		{"7f01000-7f02000 rw-p 00000000 00:00 0        [anon:java heap]", kindPrivate, "anon:java heap", "[anon:java heap]"},
		{"ffffffffff600000-ffffffffff601000 --xp 00000000 00:00 0  [vsyscall]", kindSkip, "", "[vsyscall]"},
		{"7f01000-7f02000 rw-p 00000000 00:00 0        /path with spaces/x (deleted)", kindPrivate, "anon", "/path with spaces/x (deleted)"},
	} {
		m, ok := parseMapsLine(c.line)
		if !ok {
			t.Errorf("%q: not parsed", c.line)
			continue
		}
		if m.kind != c.kind || m.label != c.label || m.path != c.path {
			t.Errorf("%q: got kind %d label %q path %q, want %d %q %q", c.line, m.kind, m.label, m.path, c.kind, c.label, c.path)
		}
	}
	for _, bad := range []string{"", "Rss:  4 kB", "VmFlags: rd wr mr mw me ac sd", "zz-yy rw-p 0 00:00 0"} {
		if _, ok := parseMapsLine(bad); ok {
			t.Errorf("%q parsed as a mapping", bad)
		}
	}
}

func TestParseSmaps(t *testing.T) {
	in := `7f0000000000-7f0000400000 rw-p 00000000 00:00 0
Size:               4096 kB
AnonHugePages:      2048 kB
VmFlags: rd wr mr mw me ac sd
7f0000400000-7f0000800000 rw-p 00000000 00:00 0
AnonHugePages:         0 kB
VmFlags: rd wr mr mw me ac
`
	ms, err := parseSmaps(strings.NewReader(in))
	if err != nil || len(ms) != 2 {
		t.Fatalf("got %d mappings, err %v", len(ms), err)
	}
	if !ms[0].softDirty || ms[0].anonHuge != 2<<20 || ms[1].softDirty || ms[1].anonHuge != 0 {
		t.Fatalf("got %+v", ms)
	}
}

func TestParseLabels(t *testing.T) {
	in := `from               to                        vsize prot          rss      hugetlb pgsz notes            info                                  file
0x0000000080000000-0x00000000f3000000   1929379840 rw-p   1736581120            0 4K   com              JAVAHEAP                              -
0x00007791d4000000-0x00007791d4021000       135168 rw-p        49152            0 4K   com              -                                     -
0x000077923a4f6000-0x000077923a9fb000      5263360 rw-p        81920            0 4K   com,nothp        STACK-24312-GC-Thread                 -
0x00007f0000000000-0x00007f0000010000        65536 rw-p        65536            0 4K   com              CODE                                  /some/file
                      META: Metaspace nodes (non-class)
0x1000-0x2000 mine
`
	ls, err := parseLabels(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	for addr, want := range map[uint64]string{
		0x80000000: "javaheap", 0xf2fff000: "javaheap", 0xf3000000: "",
		0x7791d4000000: "", 0x77923a4f6000: "stack", 0x7f0000000000: "code",
		0x1000: "mine", 0x2000: "",
	} {
		if got := ls.find(addr); got != want {
			t.Errorf("%#x: got %q want %q", addr, got, want)
		}
	}
}

// mapAnon maps n bytes of private anonymous memory (at addr with MAP_FIXED when addr != 0).
func mapAnon(t *testing.T, addr, n uintptr, prot int) uintptr {
	t.Helper()
	flags := syscall.MAP_PRIVATE | syscall.MAP_ANONYMOUS
	if addr != 0 {
		flags |= syscall.MAP_FIXED
	}
	if prot == syscall.PROT_NONE {
		flags |= syscall.MAP_NORESERVE
	}
	p, _, e := syscall.Syscall6(syscall.SYS_MMAP, addr, n, uintptr(prot), uintptr(flags), ^uintptr(0), 0)
	if e != 0 {
		t.Fatalf("mmap: %v", e)
	}
	return p
}

// mapIsolated maps n bytes read-write, aligned to align, between PROT_NONE guards: a read-write
// neighbour (the Go runtime maps memory too) would merge with it, and a VMA that grows is
// reported soft-dirty as a whole.
func mapIsolated(t *testing.T, n, align uintptr) (addr uintptr, unmap func()) {
	t.Helper()
	guard := max(align, uintptr(pageSize))
	base := mapAnon(t, 0, n+3*guard, syscall.PROT_NONE)
	addr = (base + 2*guard - 1) &^ (guard - 1)
	mapAnon(t, addr, n, syscall.PROT_READ|syscall.PROT_WRITE)
	return addr, func() { syscall.Syscall(syscall.SYS_MUNMAP, base, n+3*guard, 0) }
}

// at turns an address from mmap (memory outside the Go heap) into a pointer.
func at(addr uintptr) *byte { return *(**byte)(unsafe.Pointer(&addr)) }

func touch(addr, n, stride uintptr) {
	for off := uintptr(0); off < n; off += stride {
		*at(addr + off) = 1
	}
}

// inRange counts a scan's dirty and resident pages within [addr, addr+n).
func inRange(t *testing.T, addr, n uintptr) (dirty, resident uint64, m mapping) {
	t.Helper()
	r, err := scanProcess(os.Getpid(), nil, findZeroPFN())
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := uint64(addr)/pageSize, uint64(addr+n)/pageSize
	for _, p := range r.dirty {
		if p.vpn >= lo && p.vpn < hi {
			dirty++
		}
	}
	smaps, _ := os.ReadFile("/proc/self/smaps")
	ms, _ := parseSmaps(strings.NewReader(string(smaps)))
	for _, mm := range ms {
		if mm.start <= uint64(addr) && uint64(addr) < mm.end {
			m = mm
		}
	}
	pm, err := os.Open("/proc/self/pagemap")
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()
	err = readPagemap(pm, uint64(addr), uint64(addr+n), make([]byte, 4096), func(_, e uint64) {
		if e&pmPresent != 0 {
			resident++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestSoftDirtyOnSelf(t *testing.T) {
	const size = 64 << 20
	addr, unmap := mapIsolated(t, size, 0)
	defer unmap()
	// 4 KiB granularity: keep THP (2 MiB soft-dirty granularity) out of this test.
	syscall.Syscall(syscall.SYS_MADVISE, addr, size, 15 /* MADV_NOHUGEPAGE */)
	ps := uintptr(pageSize)
	touch(addr, size, ps)
	if err := clearSoftDirty(os.Getpid()); err != nil {
		t.Skipf("clear_refs: %v (kernel without CONFIG_MEM_SOFT_DIRTY?)", err)
	}
	if d, res, _ := inRange(t, addr, size); d != 0 || res != size/uint64(ps) {
		t.Fatalf("after clear: %d dirty, %d resident; want 0, %d", d, res, size/ps)
	}
	touch(addr, 16<<20, 4*ps) // every 4th page of the first 16 MiB: 1024 pages
	d, res, m := inRange(t, addr, size)
	if want := uint64(16<<20) / uint64(4*ps); d != want {
		t.Errorf("dirty pages: got %d want %d", d, want)
	}
	if res != size/uint64(ps) {
		t.Errorf("resident pages: got %d want %d", res, size/ps)
	}
	if m.softDirty {
		t.Errorf("VMA flagged sd after clear_refs")
	}
	// Reading doesn't dirty.
	clearSoftDirty(os.Getpid())
	for off := uintptr(0); off < size; off += ps {
		sink += *at(addr + off)
	}
	if d, _, _ := inRange(t, addr, size); d != 0 {
		t.Errorf("reads dirtied %d pages", d)
	}
}

// HotSpot commits heap by mmap(MAP_FIXED) over its PROT_NONE reservation. The kernel marks a new
// or expanded VMA VM_SOFTDIRTY and pagemap then reports every page in it soft-dirty, so committing
// a little more heap next to a big committed range makes CRIU re-dump the whole range.
func TestVMAExpansionMarksAllSoftDirty(t *testing.T) {
	const reserve, committed = 4 << 20, 1 << 20 // < 2 MiB VMAs: no THP
	ps := uintptr(pageSize)
	rsv := mapAnon(t, 0, reserve, syscall.PROT_NONE)
	defer syscall.Syscall(syscall.SYS_MUNMAP, rsv, reserve, 0)
	base := rsv + 16*ps // PROT_NONE below too: nothing else can merge with the committed range
	mapAnon(t, base, committed, syscall.PROT_READ|syscall.PROT_WRITE)
	touch(base, committed, ps)
	if err := clearSoftDirty(os.Getpid()); err != nil {
		t.Skipf("clear_refs: %v", err)
	}
	if d, _, _ := inRange(t, base, committed); d != 0 {
		t.Fatalf("after clear: %d dirty", d)
	}
	mapAnon(t, base+committed, ps, syscall.PROT_READ|syscall.PROT_WRITE) // "commit" one more page, untouched
	d, res, m := inRange(t, base, committed+ps)
	t.Logf("after committing 1 adjacent page: %d of %d resident pages report soft-dirty; VMA %x-%x sd=%v", d, res, m.start, m.end, m.softDirty)
	if m.end != uint64(base+committed+ps) {
		t.Skipf("the kernel didn't merge the VMAs (%x-%x)", m.start, m.end)
	}
	if !m.softDirty || d != uint64(committed/ps) {
		t.Errorf("expected the merged VMA to be sd and all %d pages soft-dirty", committed/ps)
	}
}

// With THP, soft-dirty is tracked per 2 MiB PMD: one written byte makes 512 pages dirty for CRIU.
func TestTHPDirtyGranularity(t *testing.T) {
	const huge = 2 << 20
	ps := uintptr(pageSize)
	const n = 4 * huge
	addr, unmap := mapIsolated(t, n, huge)
	defer unmap()
	syscall.Syscall(syscall.SYS_MADVISE, addr, n, 14 /* MADV_HUGEPAGE */)
	touch(addr, n, ps)
	_, _, m := inRange(t, addr, n)
	if m.anonHuge == 0 {
		t.Skip("no transparent huge pages here")
	}
	clearSoftDirty(os.Getpid())
	touch(addr, n, huge) // one byte per 2 MiB
	d, _, m := inRange(t, addr, n)
	t.Logf("%d bytes written, %d pages soft-dirty (AnonHugePages %d KiB)", n/huge, d, m.anonHuge>>10)
	if want := m.anonHuge / uint64(ps); d < want {
		t.Errorf("got %d dirty pages, want >= %d (whole huge pages)", d, want)
	}
}

func TestTrackerWindows(t *testing.T) {
	tr := newTracker(4)
	// Page 1 dirty in every interval, page 2 in 0 and 2, page 3 only in 3 (label b).
	tr.record(0, 1, []pageRec{{1, "a"}, {2, "a"}})
	tr.record(1, 1, []pageRec{{1, "a"}})
	tr.record(2, 1, []pageRec{{1, "a"}, {2, "a"}})
	tr.record(3, 1, []pageRec{{1, "a"}, {3, "b"}})
	tr.record(3, 2, []pageRec{{1, "a"}}) // another process: a different page
	w := tr.windows(4)
	total := w[len(w)-1]
	// k=1 windows: 2,1,2,3 -> median (sorted 1,2,2,3)[2] = 2
	// k=2: {1,2}=2,{1,2}=2,{1,2,3,p2}=4 -> 2
	// k=4: all = 4
	if total[1] != 2 || total[2] != 2 || total[4] != 4 {
		t.Fatalf("got %v", total)
	}
	if w[tr.labelID["b"]][4] != 1 || tr.uniq[tr.labelID["a"]] != 3 {
		t.Fatalf("per label: %v uniq %v", w, tr.uniq)
	}
}

func TestPredictRounds(t *testing.T) {
	// 100 MB/s dirtied up to a 1000 MB working set, measured over 20 one-second intervals.
	D := make([]float64, 21)
	for k := range D {
		D[k] = min(float64(k)*100, 1000)
	}
	rounds, final, capped := predictRounds(5000, D, 1, 500, 0, 4, 64)
	// 5000/500 = 10 s -> 1000; 1000/500 = 2 s -> 200; 0.4 s -> 40 < target: stop.
	want := []float64{5000, 1000, 200, 40}
	if len(rounds) != len(want) || capped {
		t.Fatalf("rounds %v capped %v", rounds, capped)
	}
	for i := range want {
		if rounds[i] < want[i]-1e-9 || rounds[i] > want[i]+1e-9 {
			t.Fatalf("rounds %v want %v", rounds, want)
		}
	}
	if final < 8-1e-9 || final > 8+1e-9 {
		t.Fatalf("final %v want 8", final)
	}
	// A dirty rate above the link rate doesn't converge: stops after two rounds in a row that
	// don't shrink (api.PreCopyProgress).
	rounds, _, _ = predictRounds(5000, D, 1, 50, 0, 8, 64)
	if len(rounds) != 4 || rounds[1] != 1000 || rounds[3] != 1000 {
		t.Fatalf("rounds %v", rounds)
	}
}
