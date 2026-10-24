package agent

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// A restore recreates the container's procfs and sysfs fresh on the destination and mounts
// everything that sat on them (runc's masked and read-only paths, cgroupfs at /sys/fs/cgroup,
// ...) back onto the same paths. CRIU can't create a mountpoint inside procfs or sysfs, so a path
// the destination's kernel doesn't provide fails the restore after the workload has stopped.
// Such paths come and go with kernel modules: /proc/scsi exists once scsi_mod is loaded (on the
// homelab's nodes that happened whenever fwupd first ran, up to an hour after boot), /proc/asound
// with sound drivers, /proc/sys/net/netfilter with nf_conntrack. runc masks /proc/scsi only where
// it exists, so a container started on a node that had it can't be restored on one that doesn't
// (carrack, 2026-10-24: "Can't stat mountpoint .../proc/scsi"). The source asks the destination
// about these paths before pre-copy starts (handleInfo's ?path=) and refuses instead.

// procSysMounts returns the paths, as /proc/... and /sys/..., of the mounts that sit on a procfs
// or sysfs in the mount namespace of pid (a container's init).
func procSysMounts(pid int) ([]string, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/mountinfo", pid))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseProcSysMounts(f)
}

func parseProcSysMounts(r io.Reader) ([]string, error) {
	type mount struct {
		parent     string
		mountpoint string
		fstype     string
	}
	mounts := map[string]mount{}
	var order []string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		// id parent major:minor root mountpoint options [optional...] - fstype source superoptions
		fields := strings.Fields(sc.Text())
		sep := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				sep = i
				break
			}
		}
		if len(fields) < 5 || sep < 0 || sep+1 >= len(fields) {
			continue
		}
		mounts[fields[0]] = mount{parent: fields[1], mountpoint: unescapeMountinfo(fields[4]), fstype: fields[sep+1]}
		order = append(order, fields[0])
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var paths []string
	for _, id := range order {
		m := mounts[id]
		p, ok := mounts[m.parent]
		if !ok || (p.fstype != "proc" && p.fstype != "sysfs") {
			continue
		}
		rel, ok := strings.CutPrefix(m.mountpoint, strings.TrimSuffix(p.mountpoint, "/")+"/")
		if !ok || rel == "" {
			continue
		}
		base := "/proc/"
		if p.fstype == "sysfs" {
			base = "/sys/"
		}
		if path := base + rel; !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// unescapeMountinfo undoes mountinfo's octal escapes (\040 for a space and so on).
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// validProcSysPath accepts the paths a source may ask about: clean, absolute, under /proc or /sys.
func validProcSysPath(p string) bool {
	return filepath.Clean(p) == p && (strings.HasPrefix(p, "/proc/") || strings.HasPrefix(p, "/sys/"))
}

// missingPaths returns those of paths that don't exist under root ("/" on a node).
func missingPaths(root string, paths []string) []string {
	missing := []string{}
	for _, p := range paths {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			missing = append(missing, p)
		}
	}
	return missing
}
