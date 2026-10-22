// Package stage defines the node-local on-disk contract between lcm-agent (which stages a
// checkpoint for restore, or fences a migrated-away pod) and lcm-runc (which acts on it when
// containerd asks runc to create the container).
package stage

import (
	"os"
	"path/filepath"
	"strings"
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

// RestoreDir holds a staged restore for k:
//
//	images/          CRIU image dir of the final dump (may have a `parent` symlink to ../pre-N)
//	pre-N/           pre-dump image dirs
//	rootfs-diff.tar  the source container's writable layer (OCI whiteouts)
//	READY            written last; lcm-runc only restores when it exists
//	work/            CRIU work dir for the restore (logs)
//	result.json      written by lcm-runc after the restore attempt
func RestoreDir(k Key) string { return filepath.Join(Root(), "restore", k.String()) }

func ImagesDir(k Key) string     { return filepath.Join(RestoreDir(k), "images") }
func RootfsDiff(k Key) string    { return filepath.Join(RestoreDir(k), "rootfs-diff.tar") }
func ReadyFile(k Key) string     { return filepath.Join(RestoreDir(k), "READY") }
func WorkDir(k Key) string       { return filepath.Join(RestoreDir(k), "work") }
func ResultFile(k Key) string    { return filepath.Join(RestoreDir(k), "result.json") }
func IsReady(k Key) bool         { _, err := os.Stat(ReadyFile(k)); return err == nil }
func FenceFile(ns, pod string) string { return filepath.Join(Root(), "fence", ns+"_"+pod) }

// IsFenced reports whether the pod has been migrated away from this node: its containers must
// never be (re)started here again.
func IsFenced(ns, pod string) bool { _, err := os.Stat(FenceFile(ns, pod)); return err == nil }

// RestoredMarker records that runc container id was created by restore (so `start` is a no-op).
func RestoredMarker(id string) string { return filepath.Join("/run/lcm/restored", id) }

// Sanitize keeps keys path-safe; Kubernetes names already are, this is defence in depth.
func Sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == 0 {
			return '-'
		}
		return r
	}, s)
}
