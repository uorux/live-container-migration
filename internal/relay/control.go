package relay

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ListenControl creates the unix control socket at path (removing a stale one).
func ListenControl(path string) (net.Listener, error) {
	if len(path) > 107 {
		return nil, fmt.Errorf("control socket path too long for sun_path (%d > 107 bytes): %s", len(path), path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		os.Remove(path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(true)
	return ln, nil
}

// ServeControl serves the line protocol on ln until it is closed:
//
//	quiesce -> "ok" once the tunnel is closed and no non-loopback sockets remain,
//	           then the relay closes the control connection (wait for EOF before dumping)
//	resume  -> "ok"
//	status  -> one line of JSON
//
// Errors are reported as "error <message>".
func (r *Relay) ServeControl(ln net.Listener) error {
	var mu sync.Mutex // serialise commands across connections
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			r.log.Error("control accept", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go r.handleControl(c, &mu)
	}
}

func (r *Relay) handleControl(c net.Conn, mu *sync.Mutex) {
	defer c.Close()
	sc := bufio.NewScanner(c)
	for sc.Scan() {
		cmd := strings.TrimSpace(sc.Text())
		if cmd == "" {
			continue
		}
		mu.Lock()
		reply, closeAfter := r.command(cmd)
		mu.Unlock()
		if _, err := c.Write([]byte(reply + "\n")); err != nil {
			return
		}
		if closeAfter {
			// The agent dumps right after quiesce: this unix socket must be gone
			// by then. Closing (not just shutting down) means the agent's EOF
			// implies our fd is released.
			return
		}
	}
}

func (r *Relay) command(cmd string) (reply string, closeAfter bool) {
	switch cmd {
	case "quiesce":
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := r.Quiesce(ctx); err != nil {
			return "error " + oneLine(err.Error()), true
		}
		return "ok", true
	case "resume":
		r.Resume()
		return "ok", false
	case "status":
		b, _ := json.Marshal(r.Status())
		return string(b), false
	default:
		return "error unknown command " + oneLine(cmd), false
	}
}

func oneLine(s string) string { return strings.ReplaceAll(s, "\n", " ") }
