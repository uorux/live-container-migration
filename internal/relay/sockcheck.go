package relay

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NonLoopbackSockets lists this process's TCP/UDP sockets (from /proc/self)
// whose local or remote address is not loopback/unspecified. These are what
// would make a CRIU restore on another node fail. Orphaned kernel sockets
// (TIME_WAIT etc., no fd) are not counted: CRIU only dumps sockets held by fds.
func NonLoopbackSockets() ([]string, error) {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, err
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		l, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(l, "socket:[") {
			inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
		}
	}
	var out []string
	for _, proto := range []string{"tcp", "tcp6", "udp", "udp6"} {
		f, err := os.Open("/proc/self/net/" + proto)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Scan() // header
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) < 10 || !inodes[fields[9]] {
				continue
			}
			local, err1 := parseProcAddr(fields[1])
			remote, err2 := parseProcAddr(fields[2])
			if err1 != nil || err2 != nil {
				continue
			}
			if !okAddr(local.Addr()) || !okAddr(remote.Addr()) {
				out = append(out, fmt.Sprintf("%s %s->%s", proto, local, remote))
			}
		}
		f.Close()
	}
	return out, nil
}

func okAddr(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsLoopback() || a.IsUnspecified()
}

// parseProcAddr parses "0100007F:1F90" (IPv4) or a 32-hex-digit IPv6 form;
// each 32-bit word is in host (little-endian on all supported arches) order.
func parseProcAddr(s string) (netip.AddrPort, error) {
	host, port, ok := strings.Cut(s, ":")
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("bad addr %q", s)
	}
	b, err := hex.DecodeString(host)
	if err != nil || (len(b) != 4 && len(b) != 16) {
		return netip.AddrPort{}, fmt.Errorf("bad addr %q", s)
	}
	for i := 0; i < len(b); i += 4 {
		b[i], b[i+1], b[i+2], b[i+3] = b[i+3], b[i+2], b[i+1], b[i]
	}
	p, err := strconv.ParseUint(port, 16, 16)
	if err != nil {
		return netip.AddrPort{}, err
	}
	var a netip.Addr
	if len(b) == 4 {
		a = netip.AddrFrom4([4]byte(b))
	} else {
		a = netip.AddrFrom16([16]byte(b))
	}
	return netip.AddrPortFrom(a, uint16(p)), nil
}
