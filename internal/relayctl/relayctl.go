// Package relayctl talks to the lcm-relay control socket inside a workload container.
package relayctl

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// RelaySocket is where lcm-relay listens for control commands inside the workload container.
const RelaySocket = "/run/lcm/relay.sock"

// Command sends one control command ("quiesce" or "resume") to the lcm-relay inside the
// container whose init has host pid `pid`, reaching its socket through /proc/<pid>/root. It
// returns an error satisfying os.IsNotExist if the container has no relay.
//
// The connection is closed before returning: a connected unix socket into the container would be
// an external resource CRIU refuses to dump.
func Command(pid int, cmd string, timeout time.Duration) error {
	path := "/proc/" + strconv.Itoa(pid) + "/root" + RelaySocket
	if _, err := os.Stat(path); err != nil {
		return err
	}
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := fmt.Fprintf(conn, "%s\n", cmd); err != nil {
		return err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("relay %s: %w", cmd, err)
	}
	if line = strings.TrimSpace(line); line != "ok" {
		return fmt.Errorf("relay %s: %s", cmd, line)
	}
	if cmd == "quiesce" {
		// The relay closes its end after quiescing; EOF means its fd is gone too, so the dump
		// won't see a connected unix socket pointing outside the container.
		if _, err := br.ReadByte(); err == nil {
			return fmt.Errorf("relay quiesce: unexpected data after ok")
		}
	}
	return nil
}
