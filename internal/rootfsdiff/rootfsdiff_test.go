package rootfsdiff

import (
	"archive/tar"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func write(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil { // umask
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exists(path string) bool { _, err := os.Lstat(path); return err == nil }

// setOpaque marks dir as an overlay opaque dir. trusted.* needs CAP_SYS_ADMIN; user.* (what
// overlayfs uses with the userxattr mount option) works unprivileged on most filesystems.
func setOpaque(t *testing.T, dir string) {
	t.Helper()
	if err := syscall.Setxattr(dir, "trusted.overlay.opaque", []byte("y"), 0); err == nil {
		return
	}
	if err := syscall.Setxattr(dir, "user.overlay.opaque", []byte("y"), 0); err != nil {
		t.Skipf("cannot set an overlay opaque xattr here: %v", err)
	}
}

type entry struct {
	name, link string
	typ        byte
	body       string
	mode       int64
}

func buildTar(t *testing.T, entries []entry) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o644
			if e.typ == tar.TypeDir {
				mode = 0o755
			}
		}
		hdr := &tar.Header{Name: e.name, Linkname: e.link, Typeflag: e.typ, Mode: mode,
			Size: int64(len(e.body)), Uid: os.Getuid(), Gid: os.Getgid()}
		if e.typ != tar.TypeReg {
			hdr.Size = 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

// lowerRoot builds the destination rootfs as the image would provide it.
func lowerRoot(t *testing.T) string {
	root := t.TempDir()
	write(t, filepath.Join(root, "etc/hostname"), "image\n", 0o644)
	write(t, filepath.Join(root, "etc/deleted.conf"), "lower\n", 0o644)
	write(t, filepath.Join(root, "opaque/lower-only"), "lower\n", 0o644)
	write(t, filepath.Join(root, "opaque/sub/deep"), "lower\n", 0o644)
	write(t, filepath.Join(root, "keep/untouched"), "lower\n", 0o644)
	// Image-provided symlinked dir, like /lib -> usr/lib on merged-/usr distros.
	write(t, filepath.Join(root, "usr/lib/libold.so"), "lower\n", 0o644)
	if err := os.Symlink("usr/lib", filepath.Join(root, "lib")); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestCaptureApplyRoundTrip(t *testing.T) {
	upper := t.TempDir()
	write(t, filepath.Join(upper, "etc/hostname"), "changed\n", 0o644)
	write(t, filepath.Join(upper, "data/world.dat"), "world bytes", 0o600)
	write(t, filepath.Join(upper, "data/run.sh"), "#!/bin/sh\n", 0o755)
	write(t, filepath.Join(upper, "data/empty"), "", 0o644)
	if err := os.MkdirAll(filepath.Join(upper, "data/emptydir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(upper, "data/emptydir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("world.dat", filepath.Join(upper, "data/rel-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/hostname", filepath.Join(upper, "data/abs-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/nonexistent/target", filepath.Join(upper, "data/dangling")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(upper, "data/world.dat"), filepath.Join(upper, "data/world.hard")); err != nil {
		t.Fatal(err)
	}
	// Written through the image's /lib symlink: lands in usr/lib in the upperdir.
	write(t, filepath.Join(upper, "usr/lib/libnew.so"), "new\n", 0o644)
	write(t, filepath.Join(upper, "opaque/fresh"), "upper\n", 0o644)
	setOpaque(t, filepath.Join(upper, "opaque"))

	// A real overlayfs whiteout: a 0:0 character device. Root can always make one; since Linux
	// 5.8 unprivileged users may too (vfs: allow unprivileged whiteout creation).
	haveWhiteout := true
	if err := syscall.Mknod(filepath.Join(upper, "etc/deleted.conf"), syscall.S_IFCHR|0o600, 0); err != nil {
		t.Logf("cannot create a whiteout device (%v); whiteouts covered by TestApplyWhiteouts only", err)
		haveWhiteout = false
	}

	var buf bytes.Buffer
	if err := Capture(upper, &buf); err != nil {
		t.Fatal(err)
	}

	// The tar must use OCI conventions, not leak overlay internals.
	names := map[string]byte{}
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		names[hdr.Name] = hdr.Typeflag
	}
	if _, ok := names["opaque/"+opaqueMarker]; !ok {
		t.Errorf("no opaque marker in capture; entries: %v", names)
	}
	if names["data/world.hard"] != tar.TypeLink && names["data/world.dat"] != tar.TypeLink {
		t.Errorf("hardlink not captured as TypeLink: %v", names)
	}
	if haveWhiteout {
		if typ, ok := names["etc/.wh.deleted.conf"]; !ok || typ != tar.TypeReg {
			t.Errorf("whiteout not captured as .wh. entry: %v", names)
		}
	}

	dst := lowerRoot(t)
	if err := Apply(bytes.NewReader(buf.Bytes()), dst); err != nil {
		t.Fatal(err)
	}

	if got := readFile(t, filepath.Join(dst, "etc/hostname")); got != "changed\n" {
		t.Errorf("etc/hostname = %q", got)
	}
	if got := readFile(t, filepath.Join(dst, "data/world.dat")); got != "world bytes" {
		t.Errorf("world.dat = %q", got)
	}
	for p, want := range map[string]os.FileMode{"data/world.dat": 0o600, "data/run.sh": 0o755, "data/emptydir": 0o750} {
		fi, err := os.Lstat(filepath.Join(dst, p))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != want {
			t.Errorf("%s mode %v, want %v", p, fi.Mode().Perm(), want)
		}
	}
	if fi, err := os.Stat(filepath.Join(dst, "data/emptydir")); err != nil || !fi.IsDir() {
		t.Errorf("empty dir not recreated: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "data/empty")); err != nil || fi.Size() != 0 {
		t.Errorf("empty file: %v", err)
	}
	for p, want := range map[string]string{"data/rel-link": "world.dat", "data/abs-link": "/etc/hostname", "data/dangling": "/nonexistent/target"} {
		got, err := os.Readlink(filepath.Join(dst, p))
		if err != nil || got != want {
			t.Errorf("symlink %s -> %q (%v), want %q", p, got, err, want)
		}
	}
	a, _ := os.Stat(filepath.Join(dst, "data/world.dat"))
	b, _ := os.Stat(filepath.Join(dst, "data/world.hard"))
	if a == nil || b == nil || !os.SameFile(a, b) {
		t.Errorf("hardlink not preserved")
	}
	if got := readFile(t, filepath.Join(dst, "lib/libnew.so")); got != "new\n" {
		t.Errorf("file under image symlinked dir = %q", got)
	}
	if !exists(filepath.Join(dst, "usr/lib/libold.so")) || !exists(filepath.Join(dst, "keep/untouched")) {
		t.Errorf("unrelated lower files were removed")
	}
	if fi, err := os.Lstat(filepath.Join(dst, "lib")); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("image symlink /lib was replaced")
	}
	// Opaque dir: only what the upper layer had.
	if exists(filepath.Join(dst, "opaque/lower-only")) || exists(filepath.Join(dst, "opaque/sub")) {
		t.Errorf("opaque dir still shows lower entries")
	}
	if got := readFile(t, filepath.Join(dst, "opaque/fresh")); got != "upper\n" {
		t.Errorf("opaque/fresh = %q", got)
	}
	if exists(filepath.Join(dst, "opaque", opaqueMarker)) {
		t.Errorf("opaque marker materialised as a file")
	}
	if haveWhiteout {
		if exists(filepath.Join(dst, "etc/deleted.conf")) {
			t.Errorf("whiteout did not delete etc/deleted.conf")
		}
		if exists(filepath.Join(dst, "etc/.wh.deleted.conf")) {
			t.Errorf("whiteout materialised as a file")
		}
	}
}

func TestApplyWhiteouts(t *testing.T) {
	dst := lowerRoot(t)
	write(t, filepath.Join(dst, "var/cache/a"), "x", 0o644)
	write(t, filepath.Join(dst, "var/cache/b/c"), "x", 0o644)
	tarball := buildTar(t, []entry{
		{name: "etc/.wh.deleted.conf", typ: tar.TypeReg},
		{name: "var/.wh.cache", typ: tar.TypeReg},     // whole directory
		{name: "lib/.wh.libold.so", typ: tar.TypeReg}, // through the image's symlink
		{name: ".wh.does-not-exist", typ: tar.TypeReg},
	})
	if err := Apply(tarball, dst); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"etc/deleted.conf", "var/cache", "usr/lib/libold.so"} {
		if exists(filepath.Join(dst, p)) {
			t.Errorf("%s survived its whiteout", p)
		}
	}
	for _, p := range []string{"etc/.wh.deleted.conf", "var/.wh.cache", ".wh.does-not-exist"} {
		if exists(filepath.Join(dst, p)) {
			t.Errorf("whiteout %s materialised", p)
		}
	}
	if !exists(filepath.Join(dst, "etc/hostname")) || !exists(filepath.Join(dst, "lib")) {
		t.Errorf("whiteout removed too much")
	}
}

func TestApplyWhiteoutOfSymlinkRemovesLinkOnly(t *testing.T) {
	dst := lowerRoot(t)
	outside := t.TempDir()
	write(t, filepath.Join(outside, "precious"), "keep", 0o644)
	if err := os.Symlink(outside, filepath.Join(dst, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := Apply(buildTar(t, []entry{{name: ".wh.escape", typ: tar.TypeReg}}), dst); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(dst, "escape")) {
		t.Errorf("symlink survived whiteout")
	}
	if !exists(filepath.Join(outside, "precious")) {
		t.Fatalf("whiteout followed a symlink out of root")
	}
}

func TestApplyOpaqueMarker(t *testing.T) {
	dst := lowerRoot(t)
	tarball := buildTar(t, []entry{
		{name: "opaque/", typ: tar.TypeDir},
		{name: "opaque/" + opaqueMarker, typ: tar.TypeReg},
		{name: "opaque/new", typ: tar.TypeReg, body: "n"},
	})
	if err := Apply(tarball, dst); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(dst, "opaque"))
	if len(entries) != 1 || entries[0].Name() != "new" {
		var n []string
		for _, e := range entries {
			n = append(n, e.Name())
		}
		t.Errorf("opaque dir contents %v, want [new]", n)
	}
}

// Every escape attempt must either fail or land inside root; nothing may appear outside.
func TestApplyConfinesPaths(t *testing.T) {
	cases := map[string][]entry{
		"dotdot":      {{name: "../escaped", typ: tar.TypeReg, body: "x"}},
		"dotdot-deep": {{name: "a/../../../escaped", typ: tar.TypeReg, body: "x"}},
		"absolute":    {{name: "/escaped", typ: tar.TypeReg, body: "x"}},
		"dotdot-dir":  {{name: "../escaped/", typ: tar.TypeDir}},
		"abs-symlink-parent": {
			{name: "evil", typ: tar.TypeSymlink, link: "OUTSIDE"},
			{name: "evil/escaped", typ: tar.TypeReg, body: "x"},
		},
		"rel-symlink-parent": {
			{name: "evil", typ: tar.TypeSymlink, link: "../../../../../../../../OUTSIDE_REL"},
			{name: "evil/escaped", typ: tar.TypeReg, body: "x"},
		},
		"chained-symlinks": {
			{name: "a", typ: tar.TypeSymlink, link: ".."},
			{name: "a/b", typ: tar.TypeSymlink, link: ".."},
			{name: "a/b/c", typ: tar.TypeSymlink, link: "OUTSIDE"},
			{name: "a/b/c/escaped", typ: tar.TypeReg, body: "x"},
		},
		"symlink-then-overwrite": { // a regular file entry must not write through a symlink
			{name: "victim", typ: tar.TypeSymlink, link: "OUTSIDE/escaped"},
			{name: "victim", typ: tar.TypeReg, body: "x"},
		},
		"symlink-dir-entry": {
			{name: "d", typ: tar.TypeSymlink, link: "OUTSIDE"},
			{name: "d/", typ: tar.TypeDir},
			{name: "d/escaped", typ: tar.TypeReg, body: "x"},
		},
		"hardlink-out": {{name: "h", typ: tar.TypeLink, link: "../../../../OUTSIDE/target"}},
		"hardlink-via-symlink": {
			{name: "s", typ: tar.TypeSymlink, link: "OUTSIDE"},
			{name: "h", typ: tar.TypeLink, link: "s/target"},
		},
		"whiteout-dotdot":      {{name: "../.wh.target", typ: tar.TypeReg}},
		"whiteout-via-symlink": {{name: "s", typ: tar.TypeSymlink, link: "OUTSIDE"}, {name: "s/.wh.target", typ: tar.TypeReg}},
		"opaque-via-symlink":   {{name: "s", typ: tar.TypeSymlink, link: "OUTSIDE"}, {name: "s/" + opaqueMarker, typ: tar.TypeReg}},
		"whiteout-abs-symlink": {{name: "s", typ: tar.TypeSymlink, link: "/../../OUTSIDE"}, {name: "s/.wh.target", typ: tar.TypeReg}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "root")
			outside := filepath.Join(base, "outside")
			for _, d := range []string{root, outside} {
				if err := os.MkdirAll(d, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			write(t, filepath.Join(outside, "target"), "precious", 0o644)
			// Make absolute "OUTSIDE" links point at the real outside dir, as an attacker
			// knowing the host layout would.
			for i := range entries {
				entries[i].link = strings.ReplaceAll(entries[i].link, "OUTSIDE_REL", strings.TrimPrefix(filepath.Join(outside), "/"))
				entries[i].link = strings.ReplaceAll(entries[i].link, "OUTSIDE", outside)
			}
			err := Apply(buildTar(t, entries), root)
			t.Logf("Apply: %v", err)

			if exists(filepath.Join(base, "escaped")) || exists(filepath.Join(outside, "escaped")) || exists("/escaped") {
				t.Fatalf("entry escaped root")
			}
			if got, err := os.ReadFile(filepath.Join(outside, "target")); err != nil || string(got) != "precious" {
				t.Fatalf("file outside root modified or removed: %q %v", got, err)
			}
			fi, _ := os.Stat(filepath.Join(outside, "target"))
			if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Nlink != 1 {
				t.Fatalf("file outside root gained a hardlink inside root (nlink %d)", st.Nlink)
			}
			entries, _ := os.ReadDir(outside)
			if len(entries) != 1 {
				t.Fatalf("outside dir changed: %d entries", len(entries))
			}
		})
	}
}

// A symlink in the image pointing at an absolute host-looking path resolves inside root.
func TestApplyAbsoluteSymlinkResolvedInsideRoot(t *testing.T) {
	dst := lowerRoot(t)
	if err := os.Symlink("/srv/data", filepath.Join(dst, "data")); err != nil {
		t.Fatal(err)
	}
	if err := Apply(buildTar(t, []entry{{name: "data/file", typ: tar.TypeReg, body: "in"}}), dst); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(dst, "srv/data/file")); got != "in" {
		t.Errorf("srv/data/file = %q", got)
	}
}

func TestApplyReplacesTypes(t *testing.T) {
	dst := lowerRoot(t)
	// dir in the image, file in the diff; file in the image, dir in the diff.
	err := Apply(buildTar(t, []entry{
		{name: "keep", typ: tar.TypeReg, body: "now a file"},
		{name: "etc/hostname/", typ: tar.TypeDir},
		{name: "etc/hostname/inner", typ: tar.TypeReg, body: "i"},
	}), dst)
	if err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(dst, "keep")); got != "now a file" {
		t.Errorf("keep = %q", got)
	}
	if got := readFile(t, filepath.Join(dst, "etc/hostname/inner")); got != "i" {
		t.Errorf("etc/hostname/inner = %q", got)
	}
}

func TestApplyRejectsUnsupported(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "x", Typeflag: tar.TypeGNUSparse, Mode: 0o644})
	tw.Close()
	if err := Apply(&buf, t.TempDir()); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Errorf("unsupported entry type accepted: %v", err)
	}
}
