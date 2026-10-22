package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestSharedFilesAndWarm(t *testing.T) {
	tmp := t.TempDir()
	pods := filepath.Join(tmp, "pods")
	t.Setenv("LCM_KUBELET_PODS_DIR", pods)
	vol := filepath.Join(pods, "src-uid", "volumes", "kubernetes.io~nfs", "data")
	if err := os.MkdirAll(filepath.Join(vol, "world"), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(vol, "world", "r.0.0.mca"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Not in a container: the "container path" is the host path, so mount it onto itself.
	bundle := filepath.Join(tmp, "bundle")
	os.MkdirAll(bundle, 0o755)
	cfg, _ := json.Marshal(map[string]any{"mounts": []map[string]string{{"destination": vol, "source": vol}}})
	os.WriteFile(filepath.Join(bundle, "config.json"), cfg, 0o644)

	paths, err := sharedFiles(os.Getpid(), bundle, "src-uid")
	if err != nil {
		t.Fatal(err)
	}
	want := "volumes/kubernetes.io~nfs/data/world/r.0.0.mca"
	if len(paths) != 1 || paths[0] != want {
		t.Fatalf("sharedFiles = %v, want [%s]", paths, want)
	}

	// The destination pod's view of the same volume.
	dvol := filepath.Join(pods, "dst-uid", "volumes", "kubernetes.io~nfs", "data")
	os.MkdirAll(filepath.Join(dvol, "world"), 0o755)
	os.WriteFile(filepath.Join(dvol, "world", "r.0.0.mca"), nil, 0o644)
	os.WriteFile(filepath.Join(tmp, "outside"), nil, 0o644)
	os.Symlink(filepath.Join(tmp, "outside"), filepath.Join(dvol, "escape"))
	syscall.Mkfifo(filepath.Join(dvol, "fifo"), 0o644)

	res := warmFiles(filepath.Join(pods, "dst-uid"), []string{
		want,
		"volumes/kubernetes.io~nfs/data/escape", // symlink out of the volume
		"volumes/kubernetes.io~nfs/data/fifo",   // never opened: would block
		"volumes/kubernetes.io~nfs/data/../../../../../outside",
		"etc/passwd", // not under volumes/
		"volumes/kubernetes.io~nfs/data/missing",
	}, 4)
	if res.Opened != 1 || res.Failed != 5 {
		t.Fatalf("warm = %+v, want 1 opened, 5 refused", res)
	}
}
