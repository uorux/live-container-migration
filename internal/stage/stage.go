// Package stage defines the node-local on-disk contract between lcm-agent (which stages a
// checkpoint for restore, or fences a migrated-away pod) and lcm-runc (which acts on it when
// containerd asks runc to create the container).
package stage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Root is the node-local state directory. Overridable for tests via LCM_STATE_DIR.
func Root() string {
	if d := os.Getenv("LCM_STATE_DIR"); d != "" {
		return d
	}
	return "/var/lib/lcm"
}

// Key identifies one container of one pod.
type Key struct{ Namespace, Pod, Container string }

func (k Key) String() string { return k.Namespace + "_" + k.Pod + "_" + k.Container }

// RestoreDir holds a staged restore for k (on tmpfs):
//
//	images/          CRIU image dir of the final dump (may have a `parent` symlink to ../pre-N)
//	pre-N/           pre-dump image dirs
//	rootfs-diff.tar  the source container's writable layer (OCI whiteouts)
//	source-addrs     the source pod's IPs (see internal/netaddr)
//	relay-exe        the exact lcm-relay binary the source process ran, and
//	relay-path       its path inside the container (lcm-runc installs it before restoring)
//	FINAL            the source has begun its final dump: a restore follows within seconds, so
//	                 the wait init container may exit and lcm-runc waits for READY (overlapping
//	                 kubelet's start-up latency with the dump and transfer)
//	READY            written last; lcm-runc only restores when it exists
//	READY.fenced     READY after the pod was fenced: never restored
//	work/            CRIU work dir for the restore (logs)
//	result.json      written by lcm-runc after the restore attempt
const (
	RelayExeName  = "relay-exe"
	RelayPathName = "relay-path"
)

func RestoreDir(k Key) string { return filepath.Join(Root(), "restore", k.String()) }

// LazyFile marks a staged restore to use CRIU lazy pages (userfaultfd): the process resumes
// before its memory is loaded; a criu lazy-pages daemon serves pages on fault and pushes the rest.
func LazyFile(k Key) string { return filepath.Join(RestoreDir(k), "LAZY") }

// LazyPidFile holds the pid of a restore's lazy-pages daemon while it may still serve pages.
func LazyPidFile(k Key) string { return filepath.Join(WorkDir(k), "lazy-pages.pid") }

func ImagesDir(k Key) string  { return filepath.Join(RestoreDir(k), "images") }
func RootfsDiff(k Key) string { return filepath.Join(RestoreDir(k), "rootfs-diff.tar") }
func ReadyFile(k Key) string  { return filepath.Join(RestoreDir(k), "READY") }
func WorkDir(k Key) string    { return filepath.Join(RestoreDir(k), "work") }
func ResultFile(k Key) string { return filepath.Join(RestoreDir(k), "result.json") }
func IsReady(k Key) bool      { _, err := os.Stat(ReadyFile(k)); return err == nil }
func FinalFile(k Key) string  { return filepath.Join(RestoreDir(k), "FINAL") }

// RestorePending reports whether a restore for k is about to be staged: the final dump began
// (FINAL) but READY isn't there yet, and the staging was neither consumed nor fenced.
func RestorePending(k Key) bool {
	for _, f := range []string{ReadyFile(k), ReadyFile(k) + ".consumed", ReadyFile(k) + ".fenced"} {
		if _, err := os.Stat(f); err == nil {
			return false
		}
	}
	_, err := os.Stat(FinalFile(k))
	return err == nil
}

// Progress sums the sizes of what is staged for k so far and finds when it was last written to,
// for reporting a staging that is still arriving.
func Progress(k Key) (bytes int64, last time.Time) {
	filepath.Walk(RestoreDir(k), func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if fi.Mode().IsRegular() {
			bytes += fi.Size()
		}
		if fi.ModTime().After(last) {
			last = fi.ModTime()
		}
		return nil
	})
	return bytes, last
}

// ConsumedFile is a persistent (on-disk, survives reboots) tombstone that the restore for key
// happened. The destination pod keeps its wait init container; if its sandbox is ever recreated
// (node reboot), the wait must pass so the container cold-starts instead of hanging.
func ConsumedFile(key string) string { return filepath.Join(Root(), "consumed", key) }

// FenceFile marks a pod (by UID) whose containers must never start on this node again: a source
// after its final dump, or a destination abandoned by a rollback. UIDs are never reused, so
// fences are never lifted except to abort a migration that didn't stop the source; the agent
// garbage-collects old ones.
func FenceFile(podUID string) string { return filepath.Join(Root(), "fence", podUID) }

func IsFenced(podUID string) bool {
	if podUID == "" {
		return false
	}
	_, err := os.Stat(FenceFile(podUID))
	return err == nil
}

// Lock takes an exclusive lock serialising restore and fencing for one staging key: lcm-runc
// holds it from its fence check until the restore is done (or refused); the agent holds it while
// fencing. Once a fence call returns, no restore for that key is running or can start.
func Lock(key string) (unlock func(), err error) {
	dir := filepath.Join(Root(), "locks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, key+".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("locking %s: %w", key, err)
	}
	return func() { f.Close() }, nil
}

// RestoredMarker records that runc container id was created by restore (so `start` is a no-op).
func RestoredMarker(id string) string { return filepath.Join("/run/lcm/restored", id) }

// ValidName reports whether s is safe as a single path component (Kubernetes names, UIDs,
// migration IDs and staging keys all are).
func ValidName(s string) bool {
	return s != "" && s != "." && s != ".." && len(s) <= 253 && !strings.ContainsAny(s, "/\x00")
}

// Sanitize keeps keys path-safe; Kubernetes names already are, this is defence in depth.
func Sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == 0 {
			return '-'
		}
		return r
	}, s)
}
