//go:build linux

package supervise

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary re-executes itself as the child.
func TestMain(m *testing.M) {
	switch os.Getenv("LCM_SUPERVISE_CHILD") {
	case "":
		os.Exit(m.Run())
	case "exit":
		code, _ := strconv.Atoi(os.Getenv("LCM_EXIT"))
		os.Exit(code)
	case "wait-term":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		<-ch
		os.Exit(42) // graceful shutdown path
	case "sleep":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "orphan":
		// Spawn a grandchild that outlives us; it gets re-parented (to us in a
		// container where the relay is PID 1; to a subreaper or init otherwise).
		env := []string{"LCM_SUPERVISE_CHILD=exit", "LCM_EXIT=0"}
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "LCM_") {
				env = append(env, kv)
			}
		}
		exe, _ := os.Executable()
		syscall.ForkExec(exe, []string{exe, "-test.run=^$"}, &syscall.ProcAttr{Env: env})
		os.Exit(7)
	}
}

func child(t *testing.T, mode string, extra ...string) *Child {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LCM_SUPERVISE_CHILD", mode)
	for i := 0; i+1 < len(extra); i += 2 {
		t.Setenv(extra[i], extra[i+1])
	}
	c, err := Start([]string{exe, "-test.run=^$"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func waitTimeout(t *testing.T, c *Child) syscall.WaitStatus {
	t.Helper()
	select {
	case <-c.Done():
		return c.Wait()
	case <-time.After(10 * time.Second):
		t.Fatal("child not reaped")
		return 0
	}
}

func TestExitStatus(t *testing.T) {
	for _, code := range []int{0, 3, 255} {
		c := child(t, "exit", "LCM_EXIT", strconv.Itoa(code))
		if got := ExitCode(waitTimeout(t, c)); got != code {
			t.Fatalf("exit code %d, want %d", got, code)
		}
	}
}

func TestSignalForwarding(t *testing.T) {
	c := child(t, "wait-term")
	time.Sleep(200 * time.Millisecond) // let it install its handler
	if err := c.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if got := ExitCode(waitTimeout(t, c)); got != 42 {
		t.Fatalf("exit code %d, want 42", got)
	}
	c = child(t, "sleep")
	c.Signal(syscall.SIGKILL)
	if got := ExitCode(waitTimeout(t, c)); got != 128+9 {
		t.Fatalf("exit code %d, want 137", got)
	}
	// Signalling a reaped child is a no-op, not an error or a stray kill.
	if err := c.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}

func TestOrphanReaping(t *testing.T) {
	// Become a subreaper so the grandchild is re-parented to us, as it would be
	// to the relay running as PID 1.
	if err := prctlSubreaper(); err != nil {
		t.Skip("PR_SET_CHILD_SUBREAPER unavailable:", err)
	}
	c := child(t, "orphan")
	if got := ExitCode(waitTimeout(t, c)); got != 7 {
		t.Fatalf("exit code %d", got)
	}
	// The grandchild must not stay a zombie: wait4(-1) in the reaper gets it.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var ws syscall.WaitStatus
		_, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if err == syscall.ECHILD {
			return // no children left at all: reaped
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("grandchild not reaped")
}

func prctlSubreaper() error {
	const prSetChildSubreaper = 36
	_, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
