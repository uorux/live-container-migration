//go:build linux

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

// /proc/PID/pagemap entry bits (Documentation/admin-guide/mm/pagemap.rst).
const (
	pmPresent   = 1 << 63
	pmSwap      = 1 << 62
	pmFile      = 1 << 61 // file page, or shared anonymous
	pmGuard     = 1 << 58 // MADV_GUARD_INSTALL guard region (6.15+)
	pmSoftDirty = 1 << 55
	pmPFNMask   = 1<<55 - 1
)

var pageSize = uint64(os.Getpagesize())

// kind says how CRIU treats a mapping's pages.
type kind int

const (
	kindSkip        kind = iota // not dumped page by page (shared file mappings, vdso, PROT_NONE)
	kindPrivate                 // private anonymous: every resident page is dumped, soft-dirty ones incrementally
	kindPrivateFile             // private file mapping: only pages COW'd away from the file are dumped
	kindShared                  // memfd / shared anonymous / SysV: dumped WHOLE in the final dump, never incrementally
)

type mapping struct {
	start, end uint64
	perms      string
	inode      uint64
	path       string
	softDirty  bool   // VmFlags "sd": the VMA was created or expanded since the last clear_refs
	anonHuge   uint64 // AnonHugePages bytes (THP)
	kind       kind
	label      string
}

// parseSmaps reads /proc/PID/smaps (the maps lines plus VmFlags and AnonHugePages per VMA).
// It also accepts plain /proc/PID/maps content.
func parseSmaps(r io.Reader) ([]mapping, error) {
	var out []mapping
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if m, ok := parseMapsLine(line); ok {
			out = append(out, m)
			continue
		}
		if len(out) == 0 {
			continue
		}
		cur := &out[len(out)-1]
		switch {
		case strings.HasPrefix(line, "VmFlags:"):
			for _, f := range strings.Fields(line[len("VmFlags:"):]) {
				if f == "sd" {
					cur.softDirty = true
				}
			}
		case strings.HasPrefix(line, "AnonHugePages:"):
			f := strings.Fields(line[len("AnonHugePages:"):])
			if len(f) >= 1 {
				kb, _ := strconv.ParseUint(f[0], 10, 64)
				cur.anonHuge = kb << 10
			}
		}
	}
	return out, sc.Err()
}

// parseMapsLine parses "start-end perms offset dev inode [path]".
func parseMapsLine(line string) (mapping, bool) {
	f := strings.Fields(line)
	if len(f) < 5 {
		return mapping{}, false
	}
	lo, hi, ok := strings.Cut(f[0], "-")
	if !ok || len(f[1]) != 4 || !strings.Contains(f[3], ":") {
		return mapping{}, false
	}
	start, err1 := strconv.ParseUint(lo, 16, 64)
	end, err2 := strconv.ParseUint(hi, 16, 64)
	inode, err3 := strconv.ParseUint(f[4], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || end <= start {
		return mapping{}, false
	}
	m := mapping{start: start, end: end, perms: f[1], inode: inode}
	if len(f) > 5 {
		// The path is everything after the inode column (it may contain spaces).
		rest := line
		for i := 0; i < 5; i++ {
			rest = strings.TrimLeft(rest, " \t")
			rest = rest[strings.IndexAny(rest+" ", " \t"):]
		}
		m.path = strings.TrimSpace(rest)
	}
	m.kind, m.label = classify(m)
	return m, true
}

// classify decides how CRIU dumps a mapping and gives it a default label.
func classify(m mapping) (kind, string) {
	if m.perms[:3] == "---" {
		return kindSkip, "" // reserved, not committed (the JVM's heap/metaspace reservations)
	}
	switch m.path {
	case "[vdso]", "[vvar]", "[vvar_vclock]", "[vsyscall]":
		return kindSkip, ""
	}
	shared := m.perms[3] == 's'
	switch {
	case shared && (strings.HasPrefix(m.path, "/memfd:") || strings.HasPrefix(m.path, "/dev/zero") ||
		strings.HasPrefix(m.path, "/SYSV") || m.path == "" || strings.HasPrefix(m.path, "[anon_shmem")):
		name := strings.TrimSuffix(strings.TrimPrefix(m.path, "/"), " (deleted)")
		if name == "" {
			name = "anon"
		}
		return kindShared, "shared:" + name
	case shared:
		return kindSkip, "" // a regular shared file mapping: its data is in the file
	case m.path == "[heap]":
		return kindPrivate, "brk-heap"
	case m.path == "[stack]":
		return kindPrivate, "main-stack"
	case strings.HasPrefix(m.path, "[anon:"):
		return kindPrivate, strings.TrimSuffix(m.path[1:], "]")
	case m.path == "" || m.inode == 0:
		return kindPrivate, "anon"
	default:
		return kindPrivateFile, "file-private"
	}
}

// labelRange names an address range, e.g. from `jcmd PID System.map` (JDK 21.0.x+; the "vm info"
// column needs -XX:NativeMemoryTracking=summary) or lines like "0x7f00000000-0x7f80000000 heap".
type labelRange struct {
	start, end uint64
	label      string
}

type labelSet []labelRange // sorted by start

func (ls labelSet) find(addr uint64) string {
	l, _, _ := ls.span(addr, "")
	return l
}

// span returns addr's label (def outside any labelled range) and the range [lo, hi) around addr
// that has that same answer.
func (ls labelSet) span(addr uint64, def string) (label string, lo, hi uint64) {
	i := sort.Search(len(ls), func(i int) bool { return ls[i].end > addr })
	if i < len(ls) && ls[i].start <= addr {
		return ls[i].label, ls[i].start, ls[i].end
	}
	lo, hi = 0, ^uint64(0)
	if i > 0 {
		lo = ls[i-1].end
	}
	if i < len(ls) {
		hi = ls[i].start
	}
	return def, lo, hi
}

// parseLabels reads System.map output or "LO-HI label" lines; other lines are ignored.
func parseLabels(r io.Reader) (labelSet, error) {
	var ls labelSet
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 2 {
			continue
		}
		lo, hi, ok := strings.Cut(f[0], "-")
		if !ok {
			continue
		}
		start, err1 := strconv.ParseUint(strings.TrimPrefix(lo, "0x"), 16, 64)
		end, err2 := strconv.ParseUint(strings.TrimPrefix(hi, "0x"), 16, 64)
		if err1 != nil || err2 != nil || end <= start {
			continue
		}
		label := ""
		if len(f) == 2 {
			label = f[1]
		} else {
			// System.map: from-to vsize prot rss hugetlb pgsz notes INFO file. The NMT info
			// column is the only token starting with a capitalised tag (JAVAHEAP, META, CLASS,
			// CODE, GC, STACK-<tid>-<thread name>, ...).
			for _, t := range f[2:] {
				if tag := leadingTag(t); tag != "" {
					label = tag
					break
				}
			}
		}
		if label == "" {
			continue
		}
		ls = append(ls, labelRange{start, end, strings.ToLower(label)})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].start < ls[j].start })
	return ls, sc.Err()
}

// leadingTag returns t's leading run of capitals if it is at least 2 long and is all of t or is
// followed by '-' or ','.
func leadingTag(t string) string {
	i := 0
	for i < len(t) && (t[i] >= 'A' && t[i] <= 'Z' || t[i] == '_') {
		i++
	}
	if i < 2 || (i < len(t) && t[i] != '-' && t[i] != ',') {
		return ""
	}
	return t[:i]
}

// pageRec is one page CRIU would dump, as seen by one scan.
type pageRec struct {
	vpn   uint64 // virtual page number
	label string
}

// wholeVMAMin (pages, 64 MiB) is the size above which a VMA with every resident page dirty is
// taken to be a grown VMA rather than genuinely rewritten.
var wholeVMAMin = 64 << 20 / pageSize

// scanResult is one scan of one process.
type scanResult struct {
	dirty       []pageRec         // soft-dirty dumpable pages (address order)
	resident    map[string]uint64 // dumpable resident pages per label
	wholeVMA    uint64            // dirty pages in VMAs that are dirty as a whole (see wholeVMAMin)
	anonHuge    uint64            // AnonHugePages bytes over dumpable mappings
	sharedRes   uint64            // resident pages of kindShared mappings (CRIU dumps those whole)
	sharedDirty uint64            // ... of which soft-dirty
}

// scanProcess reads PID's mappings and pagemap. zeroPFN (0 = unknown) is the shared zero
// page's frame, which CRIU skips; PFNs are only visible with CAP_SYS_ADMIN.
func scanProcess(pid int, labels labelSet, zeroPFN uint64) (*scanResult, error) {
	smaps, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps", pid))
	if err != nil {
		return nil, err
	}
	maps, err := parseSmaps(bytes.NewReader(smaps))
	if err != nil {
		return nil, err
	}
	pm, err := os.Open(fmt.Sprintf("/proc/%d/pagemap", pid))
	if err != nil {
		return nil, err
	}
	defer pm.Close()
	res := &scanResult{resident: map[string]uint64{}}
	buf := make([]byte, 8*65536)
	for _, m := range maps {
		if m.kind == kindSkip {
			continue
		}
		if m.kind != kindShared {
			res.anonHuge += m.anonHuge
		}
		// Labels go by page: one VMA can hold several labelled ranges (adjacent anonymous
		// mappings merge, e.g. thread stacks next to malloc arenas).
		label, lo, hi := m.label, uint64(1), uint64(0)
		var vmaRes, vmaDirty uint64
		err := readPagemap(pm, m.start, m.end, buf, func(vpn, e uint64) {
			if addr := vpn * pageSize; len(labels) > 0 && m.kind != kindShared && (addr < lo || addr >= hi) {
				label, lo, hi = labels.span(addr, m.label)
			}
			if e&(pmPresent|pmSwap) == 0 || e&pmGuard != 0 {
				return
			}
			if m.kind == kindPrivateFile && e&pmFile != 0 {
				return // still the file's page: CRIU doesn't dump it
			}
			if zeroPFN != 0 && e&pmPresent != 0 && e&pmPFNMask == zeroPFN {
				return
			}
			if m.kind == kindShared {
				// Not pre-dumped at all: kept out of the dirty/resident figures.
				res.sharedRes++
				if e&pmSoftDirty != 0 {
					res.sharedDirty++
				}
				return
			}
			res.resident[label]++
			vmaRes++
			if e&pmSoftDirty != 0 {
				res.dirty = append(res.dirty, pageRec{vpn, label})
				vmaDirty++
			}
		})
		if err != nil {
			return nil, fmt.Errorf("pagemap %x-%x: %w", m.start, m.end, err)
		}
		// The kernel reports every page of a VMA created or expanded since the last clear_refs as
		// soft-dirty. smaps (read first) shows that as VmFlags "sd"; a VMA that grew between the
		// two reads shows up as a big VMA dirty in full.
		if m.softDirty || (vmaRes >= wholeVMAMin && vmaDirty == vmaRes) {
			res.wholeVMA += vmaDirty
		}
	}
	return res, nil
}

// readPagemap calls fn for each page of [start, end) with its pagemap entry.
func readPagemap(pm *os.File, start, end uint64, buf []byte, fn func(vpn, entry uint64)) error {
	for vpn := start / pageSize; vpn < end/pageSize; {
		n := min(uint64(len(buf)/8), end/pageSize-vpn)
		got, err := pm.ReadAt(buf[:n*8], int64(vpn*8))
		if got == 0 && err != nil {
			return err
		}
		for i := 0; i < got/8; i++ {
			fn(vpn+uint64(i), binary.LittleEndian.Uint64(buf[i*8:]))
		}
		vpn += uint64(got / 8)
	}
	return nil
}

// clearSoftDirty resets PID's soft-dirty bits (and its VMAs' VM_SOFTDIRTY) and write-protects
// its pages, exactly what CRIU does before/between pre-dumps.
func clearSoftDirty(pid int) error {
	return os.WriteFile(fmt.Sprintf("/proc/%d/clear_refs", pid), []byte("4"), 0)
}

var sink byte

// findZeroPFN returns the zero page's frame number (as CRIU's kerndat does), or 0 when PFNs
// are hidden (no CAP_SYS_ADMIN).
func findZeroPFN() uint64 {
	b, err := syscall.Mmap(-1, 0, int(pageSize), syscall.PROT_READ, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	if err != nil {
		return 0
	}
	defer syscall.Munmap(b)
	sink += b[0] // read fault: maps the zero page
	f, err := os.Open("/proc/self/pagemap")
	if err != nil {
		return 0
	}
	defer f.Close()
	var e [8]byte
	if _, err := f.ReadAt(e[:], int64(uint64(uintptr(unsafe.Pointer(&b[0])))/pageSize*8)); err != nil {
		return 0
	}
	v := binary.LittleEndian.Uint64(e[:])
	if v&pmPresent == 0 {
		return 0
	}
	return v & pmPFNMask
}
