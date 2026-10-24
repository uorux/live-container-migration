package agent

import (
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestHideGone(t *testing.T) {
	root := t.TempDir()
	pre1, pre2 := filepath.Join(root, "pre-1"), filepath.Join(root, "pre-2")
	for d, files := range map[string][]string{
		pre1: {"pagemap-1.img", "pagemap-212.img", "pagemap-shmem-7.img", "pages-1.img", "pstree.img"},
		pre2: {"pagemap-1.img", "pagemap-300.img", "pages-1.img", "pstree.img"},
	} {
		os.MkdirAll(d, 0o755)
		for _, f := range files {
			os.WriteFile(filepath.Join(d, f), nil, 0o644)
		}
	}
	if err := os.Symlink("../pre-1", filepath.Join(pre2, "parent")); err != nil {
		t.Fatal(err)
	}
	ls := func() []string {
		ents, _ := os.ReadDir(pre1)
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		return names
	}
	unhide, err := hideGone(pre2, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"lcm-gone-pagemap-212.img", "lcm-gone-pagemap-shmem-7.img", "pagemap-1.img", "pages-1.img", "pstree.img"}
	if got := ls(); !slices.Equal(got, want) {
		t.Fatalf("while hidden: %v, want %v", got, want)
	}
	unhide()
	want = []string{"pagemap-1.img", "pagemap-212.img", "pagemap-shmem-7.img", "pages-1.img", "pstree.img"}
	if got := ls(); !slices.Equal(got, want) {
		t.Fatalf("after unhide: %v, want %v", got, want)
	}
}
