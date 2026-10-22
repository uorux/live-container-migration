// Package rootfsdiff captures a container's writable overlay layer as an OCI-style layer tar
// (whiteouts as .wh.<name>, opaque dirs as .wh..wh..opq) and applies it onto another
// container's mounted rootfs. CRIU restores processes but not the rootfs, so the destination
// container must see the same files the source had written before it was frozen.
package rootfsdiff

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	whiteoutPrefix = ".wh."
	opaqueMarker   = ".wh..wh..opq"
)

// Capture writes the overlay upperdir as an OCI layer tar to w.
func Capture(upperdir string, w io.Writer) error {
	tw := tar.NewWriter(w)
	hardlinks := map[uint64]string{}
	err := filepath.WalkDir(upperdir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(upperdir, path)
		if err != nil || rel == "." {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)

		// overlayfs whiteout: a 0:0 character device hides the lower file.
		if info.Mode()&os.ModeCharDevice != 0 && st.Rdev == 0 {
			return tw.WriteHeader(&tar.Header{
				Name:     filepath.Join(filepath.Dir(rel), whiteoutPrefix+filepath.Base(rel)),
				Typeflag: tar.TypeReg,
				Mode:     0o600,
			})
		}

		// Unix sockets can't be archived; CRIU re-binds any listening socket on restore.
		if info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		hdr.Uid, hdr.Gid = int(st.Uid), int(st.Gid)
		hdr.Uname, hdr.Gname = "", ""
		if info.Mode().IsRegular() && st.Nlink > 1 {
			if first, ok := hardlinks[st.Ino]; ok {
				hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeLink, first, 0
			} else {
				hardlinks[st.Ino] = rel
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.IsDir() && isOpaque(path) {
			if err := tw.WriteHeader(&tar.Header{Name: filepath.Join(rel, opaqueMarker), Typeflag: tar.TypeReg, Mode: 0o600}); err != nil {
				return err
			}
		}
		if hdr.Typeflag == tar.TypeReg && hdr.Size > 0 {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			f.Close()
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

func isOpaque(dir string) bool {
	buf := make([]byte, 1)
	for _, attr := range []string{"trusted.overlay.opaque", "user.overlay.opaque"} {
		if n, err := syscall.Getxattr(dir, attr, buf); err == nil && n == 1 && buf[0] == 'y' {
			return true
		}
	}
	return false
}

// Apply extracts an OCI layer tar onto the mounted rootfs at root, honouring whiteouts.
// Paths are confined to root; symlinks inside the tar are created as-is but never followed
// when resolving where an entry goes.
func Apply(r io.Reader, root string) error {
	tr := tar.NewReader(r)
	var dirs []*tar.Header
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Clean("/" + hdr.Name)
		if name == "/" {
			continue
		}
		dir, base := filepath.Split(name)
		target, err := securePath(root, name)
		if err != nil {
			return err
		}

		switch {
		case base == opaqueMarker:
			// target's parent components are resolved inside root; securePath(root, dir) would
			// leave dir's last component unresolved and ReadDir/RemoveAll would follow a symlink
			// there out of root.
			parent := filepath.Dir(target)
			entries, _ := os.ReadDir(parent)
			for _, e := range entries {
				// Entries from this tar are written after their parent dir header and the
				// marker follows the dir header immediately, so everything here is from the image.
				if err := os.RemoveAll(filepath.Join(parent, e.Name())); err != nil {
					return err
				}
			}
			continue
		case strings.HasPrefix(base, whiteoutPrefix):
			victim, err := securePath(root, filepath.Join(dir, strings.TrimPrefix(base, whiteoutPrefix)))
			if err != nil {
				return err
			}
			if err := os.RemoveAll(victim); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeDir {
			if err := os.RemoveAll(target); err != nil {
				return err
			}
		}
		mode := os.FileMode(hdr.Mode).Perm() | os.FileMode(hdr.Mode)&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if fi, err := os.Lstat(target); err == nil && !fi.IsDir() {
				if err := os.Remove(target); err != nil {
					return err
				}
			}
			if err := os.MkdirAll(target, mode); err != nil {
				return err
			}
			dirs = append(dirs, hdr)
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, mode)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			src, err := securePath(root, filepath.Clean("/"+hdr.Linkname))
			if err != nil {
				return err
			}
			if err := os.Link(src, target); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			m := uint32(mode)
			switch hdr.Typeflag {
			case tar.TypeChar:
				m |= syscall.S_IFCHR
			case tar.TypeBlock:
				m |= syscall.S_IFBLK
			default:
				m |= syscall.S_IFIFO
			}
			if err := syscall.Mknod(target, m, mkdev(hdr.Devmajor, hdr.Devminor)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("rootfsdiff: unsupported tar entry %q type %c", hdr.Name, hdr.Typeflag)
		}
		if hdr.Typeflag != tar.TypeLink {
			if err := os.Lchown(target, hdr.Uid, hdr.Gid); err != nil {
				return err
			}
			if hdr.Typeflag != tar.TypeSymlink {
				if err := os.Chmod(target, mode); err != nil {
					return err
				}
				_ = os.Chtimes(target, hdr.AccessTime, hdr.ModTime)
			}
		}
	}
	// Directory mtimes last, since writing entries into them bumps them.
	for _, hdr := range dirs {
		target, _ := securePath(root, filepath.Clean("/"+hdr.Name))
		_ = os.Chtimes(target, hdr.AccessTime, hdr.ModTime)
	}
	return nil
}

// securePath resolves name (absolute, cleaned) under root without following symlinks in any
// parent component that would escape root.
func securePath(root, name string) (string, error) { return securePathDepth(root, name, 0) }

func mkdev(major, minor int64) int {
	return int((major&0xfff)<<8 | (minor & 0xff) | (minor&^0xff)<<12 | (major&^0xfff)<<32)
}

func securePathDepth(root, name string, depth int) (string, error) {
	if depth > 40 {
		return "", fmt.Errorf("rootfsdiff: too many symlinks resolving %q", name)
	}
	cur := root
	parts := strings.Split(strings.TrimPrefix(filepath.Clean(name), "/"), "/")
	for i, p := range parts {
		if p == "" || p == "." {
			continue
		}
		if p == ".." {
			return "", fmt.Errorf("rootfsdiff: path %q escapes root", name)
		}
		next := filepath.Join(cur, p)
		if i < len(parts)-1 {
			if fi, err := os.Lstat(next); err == nil && fi.Mode()&os.ModeSymlink != 0 {
				// Resolve the symlink relative to root, as the container would see it.
				link, err := os.Readlink(next)
				if err != nil {
					return "", err
				}
				var resolved string
				if filepath.IsAbs(link) {
					resolved = filepath.Clean(link)
				} else {
					rel, _ := filepath.Rel(root, cur)
					resolved = filepath.Clean("/" + filepath.Join(rel, link))
				}
				return securePathDepth(root, filepath.Join(append([]string{resolved}, parts[i+1:]...)...), depth+1)
			}
		}
		cur = next
	}
	return cur, nil
}
