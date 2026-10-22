// Package netaddr deals with the pod IP changing across a migration: it lists a container's
// addresses on the source, and on the destination lets CRIU rebuild TCP sockets bound to the old
// address (as a temporary alias on lo) before destroying them, so the application sees an
// ordinary connection reset on anything that wasn't loopback and reconnects from its new IP.
package netaddr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// Tool resolves ip/ss/nsenter: /var/lib/lcm/bin (set up by the NixOS module) first, then PATH.
func Tool(name string) string {
	p := filepath.Join("/var/lib/lcm/bin", name)
	if _, err := os.Stat(p); err == nil {
		return p
	}
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	return name
}

// ContainerAddrs returns the global unicast addresses in the network namespace of host pid.
func ContainerAddrs(pid int) ([]netip.Addr, error) {
	out, err := exec.Command(Tool("nsenter"), "--net=/proc/"+strconv.Itoa(pid)+"/ns/net", Tool("ip"), "-j", "addr", "show").Output()
	if err != nil {
		return nil, fmt.Errorf("listing container addresses: %w", err)
	}
	var ifaces []struct {
		AddrInfo []struct {
			Local string `json:"local"`
		} `json:"addr_info"`
	}
	if err := json.Unmarshal(out, &ifaces); err != nil {
		return nil, err
	}
	var addrs []netip.Addr
	for _, i := range ifaces {
		for _, a := range i.AddrInfo {
			if ip, err := netip.ParseAddr(a.Local); err == nil && ip.IsGlobalUnicast() {
				addrs = append(addrs, ip)
			}
		}
	}
	return addrs, nil
}

func Write(path string, addrs []netip.Addr) error {
	var b strings.Builder
	for _, a := range addrs {
		b.WriteString(a.String() + "\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func Read(path string) ([]netip.Addr, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var addrs []netip.Addr
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if a, err := netip.ParseAddr(strings.TrimSpace(sc.Text())); err == nil {
			addrs = append(addrs, a)
		}
	}
	return addrs, sc.Err()
}

func inNetns(netns string, args ...string) error {
	out, err := exec.Command(Tool("nsenter"), append([]string{"--net=" + netns}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func prefix(a netip.Addr) string {
	if a.Is4() {
		return a.String() + "/32"
	}
	return a.String() + "/128"
}

// AddAliases puts the old addresses on lo in netns (skipping ones the new pod already has).
// It returns the ones it added, for KillAndRemove.
func AddAliases(netns string, addrs []netip.Addr) ([]netip.Addr, error) {
	var added []netip.Addr
	for _, a := range addrs {
		err := inNetns(netns, Tool("ip"), "addr", "add", prefix(a), "dev", "lo")
		if err != nil {
			if strings.Contains(err.Error(), "File exists") {
				continue
			}
			return added, err
		}
		added = append(added, a)
	}
	return added, nil
}

// KillAndRemove destroys every TCP socket bound to one of addrs (the peers of those connections
// live behind the old IP, which no longer routes here) and drops the aliases.
func KillAndRemove(netns string, addrs []netip.Addr) error {
	var errs []string
	for _, a := range addrs {
		if err := inNetns(netns, Tool("ss"), "-K", "-t", "-n", "src", a.String()); err != nil {
			errs = append(errs, err.Error())
		}
		if err := inNetns(netns, Tool("ip"), "addr", "del", prefix(a), "dev", "lo"); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}
