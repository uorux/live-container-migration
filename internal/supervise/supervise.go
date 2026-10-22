//go:build linux

// Package supervise starts and reaps a child process without pidfds
// (os/exec uses pidfd_open/CLONE_PIDFD on recent kernels, which CRIU handles
// poorly). It is meant for a PID-1 relay: it reaps every child (including
// re-parented orphans) via wait4(-1) on SIGCHLD.
package supervise

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Child is a supervised process.
type Child struct {
	Pid    int
	done   chan struct{}
	status syscall.WaitStatus
}

var (
	reaperOnce sync.Once
	mu         sync.Mutex
	children   = map[int]*Child{}
	// exited records statuses of watched pids reaped before registration.
	early = map[int]syscall.WaitStatus{}
)

func startReaper() {
	reaperOnce.Do(func() {
		ch := make(chan os.Signal, 16)
		signal.Notify(ch, syscall.SIGCHLD)
		go func() {
			t := time.NewTicker(time.Second) // safety net for coalesced/missed signals
			defer t.Stop()
			for {
				select {
				case <-ch:
				case <-t.C:
				}
				reapAll()
			}
		}()
	})
}

func reapAll() {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if err == syscall.EINTR {
			continue
		}
		if pid <= 0 || err != nil {
			return
		}
		mu.Lock()
		if c := children[pid]; c != nil {
			delete(children, pid)
			c.status = ws
			close(c.done)
		} else {
			early[pid] = ws
			if len(early) > 1024 { // orphans we never asked about; don't grow forever
				clear(early)
			}
		}
		mu.Unlock()
	}
}

// Start runs argv with the current environment and stdio. argv[0] is looked
// up in PATH.
func Start(argv []string) (*Child, error) {
	if len(argv) == 0 {
		return nil, errors.New("no command")
	}
	path, err := exec.LookPath(argv[0]) // LookPath only stats files; no pidfd involved
	if err != nil {
		return nil, err
	}
	startReaper()
	mu.Lock() // hold across fork so the reaper can't miss the registration
	defer mu.Unlock()
	pid, err := syscall.ForkExec(path, argv, &syscall.ProcAttr{
		Env:   os.Environ(),
		Files: []uintptr{0, 1, 2},
		Sys:   &syscall.SysProcAttr{}, // PidFD nil: plain clone, no CLONE_PIDFD
	})
	if err != nil {
		return nil, err
	}
	c := &Child{Pid: pid, done: make(chan struct{})}
	if ws, ok := early[pid]; ok {
		delete(early, pid)
		c.status = ws
		close(c.done)
	} else {
		children[pid] = c
	}
	return c, nil
}

// Signal sends sig to the child (no-op once it has been reaped).
func (c *Child) Signal(sig syscall.Signal) error {
	select {
	case <-c.done:
		return nil
	default:
	}
	return syscall.Kill(c.Pid, sig)
}

// Done is closed once the child has been reaped.
func (c *Child) Done() <-chan struct{} { return c.done }

// Wait blocks until the child exits and returns its status.
func (c *Child) Wait() syscall.WaitStatus {
	<-c.done
	return c.status
}

// ExitCode maps a wait status to a shell-style exit code (128+signal if killed).
func ExitCode(ws syscall.WaitStatus) int {
	switch {
	case ws.Exited():
		return ws.ExitStatus()
	case ws.Signaled():
		return 128 + int(ws.Signal())
	}
	return 1
}
