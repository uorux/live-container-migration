package stage

import (
	"os"
	"testing"
)

func TestRestorePending(t *testing.T) {
	t.Setenv("LCM_STATE_DIR", t.TempDir())
	k := Key{"ns", "pod-m1", "c"}
	os.MkdirAll(RestoreDir(k), 0o755)
	touch := func(p string) { os.WriteFile(p, nil, 0o644) }

	if RestorePending(k) {
		t.Fatal("pending without FINAL")
	}
	touch(FinalFile(k))
	if !RestorePending(k) {
		t.Fatal("FINAL without READY must be pending (lcm-runc waits)")
	}
	touch(ReadyFile(k))
	if RestorePending(k) {
		t.Fatal("READY: not pending, restore now")
	}
	os.Rename(ReadyFile(k), ReadyFile(k)+".consumed")
	if RestorePending(k) {
		t.Fatal("consumed: a later container restart must cold-start, not wait")
	}
	os.Rename(ReadyFile(k)+".consumed", ReadyFile(k)+".fenced")
	if RestorePending(k) {
		t.Fatal("fenced: never wait")
	}
}
