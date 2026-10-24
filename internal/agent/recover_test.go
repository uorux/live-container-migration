package agent

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"lcm/internal/api"
	"lcm/internal/stage"
)

func TestDecideRecovery(t *testing.T) {
	for _, c := range []struct {
		name string
		o    dumpOutcome
		want recoveryAction
	}{
		{"dump never started, source runs", dumpOutcome{alive: true}, recoverAbort},
		{"dump failed, CRIU thawed the source", dumpOutcome{alive: true, imagesComplete: true}, recoverAbort},
		{"dump never started, source died", dumpOutcome{}, recoverColdStart},
		{"dump died half-way with the source", dumpOutcome{logSucceeded: false, imagesComplete: false}, recoverColdStart},
		{"dump completed after the agent died", dumpOutcome{imagesComplete: true, logSucceeded: true}, recoverFinish},
		{"dump completed before the agent died", dumpOutcome{dumped: true, imagesComplete: true}, recoverFinish},
		{"DUMPED wins over a container runc still lists", dumpOutcome{dumped: true, alive: true}, recoverFinish},
		{"complete images, tree gone, no success logged", dumpOutcome{imagesComplete: true}, recoverFinish},
		{"success logged but images incomplete, source died", dumpOutcome{logSucceeded: true}, recoverColdStart},
		{"dump still running", dumpOutcome{stillRunning: true}, recoverLeave},
		{"dump still running, partial images", dumpOutcome{stillRunning: true, imagesComplete: true, logSucceeded: true}, recoverLeave},
		{"dump still running, DUMPED from an earlier try", dumpOutcome{stillRunning: true, dumped: true}, recoverLeave},
	} {
		if got := decideRecovery(c.o); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

// fakeProc adds a process to a fake /proc: its comm, command line, working directory and fds.
func fakeProc(t *testing.T, root string, pid int, comm string, cmdline []string, cwd string, fds ...string) {
	t.Helper()
	p := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(p, "fd"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(p, "comm"), []byte(comm+"\n"), 0o644)
	os.WriteFile(filepath.Join(p, "cmdline"), []byte(strings.Join(cmdline, "\x00")+"\x00"), 0o644)
	if cwd != "" {
		os.Symlink(cwd, filepath.Join(p, "cwd"))
	}
	for i, fd := range fds {
		os.Symlink(fd, filepath.Join(p, "fd", strconv.Itoa(i+3)))
	}
}

func useProcRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	old := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = old })
	return root
}

func TestDumpProcs(t *testing.T) {
	proc := useProcRoot(t)
	dir := "/var/lib/lcm/ckpt/m1"
	fakeProc(t, proc, 100, "runc", []string{"runc", "--root", "/run/containerd/runc/k8s.io", "checkpoint",
		"--image-path", dir + "/images", "--work-path", dir + "/work", "--parent-path", "../pre-2", "ctr"}, "/")
	fakeProc(t, proc, 101, ".criu-wrapped", []string{"criu", "swrk", "3"}, "/", "/dev/null", dir+"/images", dir+"/work/dump.log")
	fakeProc(t, proc, 102, "criu", []string{"criu", "swrk", "3"}, dir+"/work")
	fakeProc(t, proc, 103, "runc", []string{"runc", "checkpoint", "--pre-dump", "--image-path", dir + "/pre-1", "ctr"}, "/")
	// Not this migration's:
	fakeProc(t, proc, 200, "tail", []string{"tail", "-f", dir + "/work/dump.log"}, dir, dir+"/work/dump.log")
	fakeProc(t, proc, 201, "runc", []string{"runc", "checkpoint", "--image-path", dir + "0/images", "ctr"}, "/")
	fakeProc(t, proc, 202, "criu", []string{"criu", "swrk", "3"}, "/", "/var/lib/lcm/ckpt/m2/work/dump.log")
	fakeProc(t, proc, 203, "java", []string{"java", "-jar", "server.jar"}, "/", "/data/world/level.dat")
	os.MkdirAll(filepath.Join(proc, "self"), 0o755)

	got := dumpProcs(dir)
	slices.Sort(got)
	if want := []int{100, 101, 102, 103}; !slices.Equal(got, want) {
		t.Fatalf("dumpProcs = %v, want %v", got, want)
	}
	if s := procNames([]int{100, 101}); s != "runc[100] .criu-wrapped[101]" {
		t.Errorf("procNames = %q", s)
	}
}

// recoverFixture is a source node's state after its agent restarted mid-migration: a checkpoint
// dir with request.json, the fence, and an agent that is also the destination (its own staging
// server).
type recoverFixture struct {
	a          *Agent
	dir, fence string
	key        string
	proc       string
}

func newRecoverFixture(t *testing.T) *recoverFixture {
	t.Helper()
	root := t.TempDir()
	t.Setenv("LCM_STATE_DIR", root)
	t.Setenv("LCM_RUNC", filepath.Join(t.TempDir(), "no-runc")) // runc state fails: the container is gone
	f := &recoverFixture{a: New(testToken, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil))),
		dir: filepath.Join(root, "ckpt", "m1"), key: "ns_pod-m1_c", proc: useProcRoot(t)}
	srv := httptest.NewServer(f.a.Handler())
	t.Cleanup(srv.Close)
	upper := t.TempDir()
	os.WriteFile(filepath.Join(upper, "changed"), []byte("x"), 0o644)
	req := api.CheckpointRequest{MigrationID: "m1", Namespace: "ns", Pod: "pod", PodUID: "uid-1", Container: "c",
		ContainerID: "ctr1", DestKey: f.key, DestURL: srv.URL}
	os.MkdirAll(filepath.Join(f.dir, "work"), 0o755)
	if err := writeJSON(filepath.Join(f.dir, "request.json"), persisted{Request: req, Upper: upper}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(f.dir, "FENCED"), nil, 0o644)
	f.fence = stage.FenceFile("uid-1")
	os.MkdirAll(filepath.Dir(f.fence), 0o755)
	os.WriteFile(f.fence, []byte("m1"), 0o644)
	return f
}

// runcRunning makes the final dump's runc (pid 100) show up in the fake /proc.
func (f *recoverFixture) runcRunning(t *testing.T) {
	fakeProc(t, f.proc, 100, "runc", []string{"runc", "checkpoint", "--image-path", f.dir + "/images", "--work-path", f.dir + "/work", "ctr1"}, "/")
}

func (f *recoverFixture) runcExits() { os.RemoveAll(filepath.Join(f.proc, "100")) }

func (f *recoverFixture) status() checkpointSt {
	f.a.mu.Lock()
	defer f.a.mu.Unlock()
	if st := f.a.status["m1"]; st != nil {
		return *st
	}
	return checkpointSt{}
}

func (f *recoverFixture) busy() bool {
	f.a.mu.Lock()
	defer f.a.mu.Unlock()
	return f.a.migrations["ctr1"]
}

func (f *recoverFixture) waitDone(t *testing.T) checkpointSt {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if st := f.status(); st.Done {
			return st
		}
	}
	t.Fatalf("recovery never finished; last status %+v", f.status())
	return checkpointSt{}
}

// The lab bug: the agent restarted while the final dump ran, and recovery deleted the images
// under it. Now it waits for runc, and ships the dump runc completed.
func TestRecoverWaitsForRunningDumpThenFinishes(t *testing.T) {
	f := newRecoverFixture(t)
	f.runcRunning(t)
	f.a.Recover(context.Background())
	if st := f.status(); st.Done || st.Last.Phase != "recover" {
		t.Fatalf("status right after Recover = %+v, want an undecided recovery", st)
	}
	time.Sleep(600 * time.Millisecond)
	if st := f.status(); st.Done {
		t.Fatalf("decided while runc still ran: %+v", st)
	}
	if !exists(f.dir) || !exists(f.fence) || !f.busy() {
		t.Fatal("touched the checkpoint while runc still ran")
	}
	// CRIU finishes: the tree is killed, the images are complete, success is logged; then runc exits.
	os.MkdirAll(filepath.Join(f.dir, "images"), 0o755)
	for _, img := range []string{"inventory.img", "pstree.img", "pages-1.img"} {
		os.WriteFile(filepath.Join(f.dir, "images", img), []byte(img), 0o644)
	}
	os.WriteFile(filepath.Join(f.dir, "work", "dump.log"), []byte("(00.9) Dumping finished successfully\n"), 0o644)
	f.runcExits()

	st := f.waitDone(t)
	if st.Error != "" || st.Result == nil || !st.Result.SourceKilled {
		t.Fatalf("status = %+v, want a shipped migration", st)
	}
	if !exists(filepath.Join(f.dir, "SHIPPED")) || !exists(filepath.Join(stage.Root(), "restore", f.key, "READY")) ||
		!exists(filepath.Join(stage.Root(), "restore", f.key, "images", "inventory.img")) {
		t.Fatal("the final dump wasn't shipped and marked READY")
	}
	if !exists(f.fence) {
		t.Fatal("the source's fence was lifted after shipping")
	}
	if f.busy() {
		t.Fatal("the container stays marked as migrating")
	}
}

func TestRecoverWaitsForRunningDumpThatFailed(t *testing.T) {
	f := newRecoverFixture(t)
	f.runcRunning(t)
	f.a.Recover(context.Background())
	time.Sleep(400 * time.Millisecond)
	if f.status().Done || !exists(f.dir) {
		t.Fatal("decided while runc still ran")
	}
	// The dump died with the container and left nothing usable.
	os.WriteFile(filepath.Join(f.dir, "work", "dump.log"), []byte("Error (criu/cr-dump.c:2093): Dumping FAILED.\n"), 0o644)
	f.runcExits()
	st := f.waitDone(t)
	if !strings.Contains(st.Error, "cold-start") {
		t.Fatalf("status = %+v, want a cold start", st)
	}
	if exists(f.dir) || exists(f.fence) {
		t.Fatal("the images or the fence were left behind")
	}
	if b, err := os.ReadFile(filepath.Join(stage.Root(), "logs", "m1", "work-dump.log")); err != nil || !strings.Contains(string(b), "FAILED") {
		t.Fatalf("CRIU's log wasn't kept: %v", err)
	}
}

func TestRecoverLeavesWedgedDumpAlone(t *testing.T) {
	old := dumpExitWait
	dumpExitWait = 300 * time.Millisecond
	t.Cleanup(func() { dumpExitWait = old })
	f := newRecoverFixture(t)
	f.runcRunning(t)
	f.a.Recover(context.Background())
	st := f.waitDone(t)
	if !strings.Contains(st.Error, "still running") || !strings.Contains(st.Error, "runc[100]") ||
		st.Result == nil || !st.Result.SourceKilled {
		t.Fatalf("status = %+v, want a failure the CLI treats as after the source stopped", st)
	}
	if !exists(f.dir) || !exists(f.fence) || !exists(filepath.Join(f.dir, "request.json")) {
		t.Fatal("touched the checkpoint of a dump still running")
	}
	if !f.busy() {
		t.Fatal("a new migration of the container would be accepted")
	}
}

func TestRecoverAbortsWhenSourceRuns(t *testing.T) {
	for _, status := range []string{"running", "paused"} {
		t.Run(status, func(t *testing.T) {
			f := newRecoverFixture(t)
			calls := filepath.Join(t.TempDir(), "calls")
			fakeRunc := filepath.Join(t.TempDir(), "runc")
			os.WriteFile(fakeRunc, []byte("#!/bin/sh\necho \"$*\" >> "+calls+"\n"+
				"case \"$3\" in state) echo '{\"id\":\"ctr1\",\"pid\":1,\"status\":\""+status+"\"}';; esac\n"), 0o755)
			t.Setenv("LCM_RUNC", fakeRunc)
			// Images half-written by a pre-dump the restart interrupted (it thawed the tree).
			os.MkdirAll(filepath.Join(f.dir, "pre-1"), 0o755)
			f.a.Recover(context.Background())
			st := f.waitDone(t)
			if !strings.Contains(st.Error, "the source kept running") {
				t.Fatalf("status = %+v, want an abort", st)
			}
			if exists(f.dir) || exists(f.fence) || f.busy() {
				t.Fatal("abort left the images, the fence or the migration mark")
			}
			b, _ := os.ReadFile(calls)
			if resumed := strings.Contains(string(b), "resume ctr1"); resumed != (status == "paused") {
				t.Fatalf("runc calls %q: resume expected only for a frozen container", b)
			}
		})
	}
}

// dumpProcs on the real /proc: a "runc" naming the dir on its command line and a "criu" holding
// a file in it open (copies of sh under those names).
func TestDumpProcsLive(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	b, err := os.ReadFile(sh)
	if err != nil {
		t.Skip(err)
	}
	bin, dir := t.TempDir(), t.TempDir()
	os.MkdirAll(filepath.Join(dir, "work"), 0o755)
	os.WriteFile(filepath.Join(dir, "work", "dump.log"), nil, 0o644)
	start := func(name string, args ...string) *exec.Cmd {
		os.WriteFile(filepath.Join(bin, name), b, 0o755)
		cmd := exec.Command(filepath.Join(bin, name), append([]string{"-c", "sleep 30; true"}, args...)...)
		if name == "criu" || name == "tail" {
			log, err := os.Open(filepath.Join(dir, "work", "dump.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			cmd.Stdin = log
		}
		if err := cmd.Start(); err != nil {
			t.Skip(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		return cmd
	}
	r := start("runc", "runc", dir+"/images")
	c := start("criu")
	start("tail", "tail", dir+"/work/dump.log")
	want := []int{r.Process.Pid, c.Process.Pid}
	slices.Sort(want)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got := dumpProcs(dir)
		slices.Sort(got)
		if slices.Equal(got, want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dumpProcs = %v, want %v", got, want)
		}
	}
	r.Process.Kill()
	r.Wait()
	if left := waitDumpProcs(context.Background(), dir, 300*time.Millisecond); !slices.Equal(left, []int{c.Process.Pid}) {
		t.Fatalf("waitDumpProcs = %v, want only the criu left", left)
	}
	c.Process.Kill()
	c.Wait()
	if left := waitDumpProcs(context.Background(), dir, 5*time.Second); left != nil {
		t.Fatalf("waitDumpProcs = %v after they all exited", left)
	}
}
