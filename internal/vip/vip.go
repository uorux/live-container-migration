// Package vip gives a process a virtual IP on the node's LAN without touching
// the node's own network namespace: a private namespace holding one ipvlan
// (L2 mode) on the LAN NIC, created directly inside it. Nothing the node's
// network stack, Cilium or anything else watches changes when the VIP comes,
// goes or moves; on 2026-10-22 an address added to a node's NIC made Cilium
// reload its datapath, and on carrack that reload took the node off the
// network.
//
// The ipvlan shares the NIC's MAC, so whatever delivers the node's frames
// (a macvtap for crosvm guests) delivers the VIP's. The node itself can't
// reach the VIP (ipvlan doesn't hairpin to its parent); other hosts can.
//
// Sockets are created in the namespace with Do (Listen, tcprepair.Restore)
// and stay there; the rest of the process keeps using the node's namespace.
package vip

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// IfName is the VIP interface's name inside the namespace.
const IfName = "vip0"

// holdName is a dummy interface in the namespace carrying a last-resort
// default route, so sockets can be created and restored (connect() needs a
// route) while the VIP is down, and what they send meanwhile is discarded
// instead of failing with "network unreachable". Once the VIP is up its
// routes win, and the kernel re-routes those sockets.
const holdName = "hold0"

// NS is a VIP namespace. The VIP's address is configured from the start but
// the link is down, so nothing answers for it until Up.
type NS struct {
	fd      int // the namespace (open /proc/thread-self/ns/net)
	prefix  netip.Prefix
	gateway netip.Addr
	mu      sync.Mutex
	up      bool
}

// Create makes the namespace and its ipvlan on parent (the node's LAN NIC),
// with prefix (the VIP and its LAN's length, e.g. 10.0.0.250/24) and the
// LAN's default gateway (for replies to clients beyond the LAN).
func Create(parent string, prefix netip.Prefix, gateway netip.Addr) (*NS, error) {
	if !prefix.Addr().Is4() {
		return nil, errors.New("only IPv4 VIPs are supported")
	}
	pif, err := net.InterfaceByName(parent)
	if err != nil {
		return nil, fmt.Errorf("parent %q: %w", parent, err)
	}
	fd, err := newNetns()
	if err != nil {
		return nil, fmt.Errorf("new network namespace: %w", err)
	}
	n := &NS{fd: fd, prefix: prefix, gateway: gateway}
	if err := createIpvlan(pif.Index, IfName, fd); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("ipvlan on %s: %w", parent, err)
	}
	err = n.Do(func() error {
		if err := setLinkUp("lo", true); err != nil {
			return err
		}
		if err := createLink(holdName, "dummy"); err != nil {
			return fmt.Errorf("%s: %w", holdName, err)
		}
		if err := setLinkUp(holdName, true); err != nil {
			return err
		}
		if err := addDefaultRoute(holdName, netip.Addr{}, ^uint32(0)); err != nil {
			return fmt.Errorf("%s route: %w", holdName, err)
		}
		return addAddr(IfName, prefix)
	})
	if err != nil {
		n.Close()
		return nil, err
	}
	return n, nil
}

// Close destroys the namespace once nothing else holds it (sockets created
// in it keep it alive until they're closed); its ipvlan goes with it.
func (n *NS) Close() error { return unix.Close(n.fd) }

// Prefix returns the VIP and its prefix length.
func (n *NS) Prefix() netip.Prefix { return n.prefix }

// Do runs fn on an OS thread switched into the namespace. Sockets fn creates
// belong to the namespace for good.
func (n *NS) Do(fn func() error) error {
	errc := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		orig, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()
			errc <- err
			return
		}
		defer orig.Close()
		if err := unix.Setns(n.fd, unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()
			errc <- fmt.Errorf("setns: %w", err)
			return
		}
		err = fn()
		if unix.Setns(int(orig.Fd()), unix.CLONE_NEWNET) == nil {
			runtime.UnlockOSThread()
		} // else: the thread stays locked and exits with this goroutine
		errc <- err
	}()
	return <-errc
}

// Listen creates a TCP listener on the VIP in the namespace.
func (n *NS) Listen(port uint16) (net.Listener, error) {
	var ln net.Listener
	err := n.Do(func() error {
		var err error
		ln, err = net.Listen("tcp4", netip.AddrPortFrom(n.prefix.Addr(), port).String())
		return err
	})
	return ln, err
}

// Up brings the VIP's link up, adds the default route and announces the VIP
// with gratuitous ARP, so the LAN (the router's port-forward included) sends
// its traffic here at once.
func (n *NS) Up() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	err := n.Do(func() error {
		if err := setLinkUp(IfName, true); err != nil {
			return err
		}
		if n.gateway.IsValid() {
			if err := addDefaultRoute(IfName, n.gateway, 0); err != nil && !errors.Is(err, unix.EEXIST) {
				return fmt.Errorf("default route: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	n.up = true
	return n.announceLocked()
}

// Down takes the VIP's link down. Frames for the VIP that still arrive are
// dropped by ipvlan: no RST or ICMP goes back, so peers just retransmit
// (to whoever announces the VIP next).
func (n *NS) Down() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up = false
	return n.Do(func() error { return setLinkUp(IfName, false) })
}

// Announce repeats the gratuitous ARP (request and reply forms).
func (n *NS) Announce() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.up {
		return errors.New("VIP is down")
	}
	return n.announceLocked()
}

func (n *NS) announceLocked() error {
	return n.Do(func() error {
		ifi, err := net.InterfaceByName(IfName)
		if err != nil {
			return err
		}
		return gratuitousARP(ifi, n.prefix.Addr())
	})
}

// newNetns creates a network namespace and returns a descriptor for it,
// leaving the calling goroutine's thread where it was.
func newNetns() (int, error) {
	type res struct {
		fd  int
		err error
	}
	c := make(chan res, 1)
	go func() {
		runtime.LockOSThread()
		orig, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()
			c <- res{-1, err}
			return
		}
		defer orig.Close()
		if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()
			c <- res{-1, err}
			return
		}
		fd, err := unix.Open("/proc/thread-self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if unix.Setns(int(orig.Fd()), unix.CLONE_NEWNET) == nil {
			runtime.UnlockOSThread()
		}
		c <- res{fd, err}
	}()
	r := <-c
	return r.fd, r.err
}

// ---- rtnetlink ----

var native = binary.NativeEndian

func nlAttr(b []byte, typ uint16, data []byte) []byte {
	l := 4 + len(data)
	b = native.AppendUint16(b, uint16(l))
	b = native.AppendUint16(b, typ)
	b = append(b, data...)
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	return b
}

func u32(v uint32) []byte { return native.AppendUint32(nil, v) }
func u16(v uint16) []byte { return native.AppendUint16(nil, v) }

// nlDo sends one rtnetlink request in the calling thread's namespace and
// waits for its acknowledgement.
func nlDo(typ, flags uint16, body []byte) error {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	msg := native.AppendUint32(nil, uint32(unix.NLMSG_HDRLEN+len(body)))
	msg = native.AppendUint16(msg, typ)
	msg = native.AppendUint16(msg, flags|unix.NLM_F_REQUEST|unix.NLM_F_ACK)
	msg = native.AppendUint32(msg, 1) // seq
	msg = native.AppendUint32(msg, 0) // pid
	msg = append(msg, body...)
	if err := unix.Sendto(fd, msg, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	buf := make([]byte, 8192)
	for {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			return err
		}
		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if m.Header.Type == unix.NLMSG_ERROR {
				if errno := int32(native.Uint32(m.Data[:4])); errno != 0 {
					return unix.Errno(-errno)
				}
				return nil
			}
		}
	}
}

func ifinfo(index int32, flags, change uint32) []byte {
	b := []byte{unix.AF_UNSPEC, 0}
	b = native.AppendUint16(b, 0) // type
	b = native.AppendUint32(b, uint32(index))
	b = native.AppendUint32(b, flags)
	b = native.AppendUint32(b, change)
	return b
}

// createIpvlan creates an L2 ipvlan on parent directly in the namespace nsfd.
func createIpvlan(parent int, name string, nsfd int) error {
	const iflaIpvlanMode = 1 // IFLA_IPVLAN_MODE; IPVLAN_MODE_L2 is 0
	data := nlAttr(nil, iflaIpvlanMode, u16(0))
	info := nlAttr(nil, unix.IFLA_INFO_KIND, []byte("ipvlan\x00"))
	info = nlAttr(info, unix.IFLA_INFO_DATA|unix.NLA_F_NESTED, data)
	b := ifinfo(0, 0, 0)
	b = nlAttr(b, unix.IFLA_IFNAME, append([]byte(name), 0))
	b = nlAttr(b, unix.IFLA_LINK, u32(uint32(parent)))
	b = nlAttr(b, unix.IFLA_NET_NS_FD, u32(uint32(nsfd)))
	b = nlAttr(b, unix.IFLA_LINKINFO|unix.NLA_F_NESTED, info)
	return nlDo(unix.RTM_NEWLINK, unix.NLM_F_CREATE|unix.NLM_F_EXCL, b)
}

// createLink creates a link of kind in the calling thread's namespace.
func createLink(name, kind string) error {
	info := nlAttr(nil, unix.IFLA_INFO_KIND, append([]byte(kind), 0))
	b := ifinfo(0, 0, 0)
	b = nlAttr(b, unix.IFLA_IFNAME, append([]byte(name), 0))
	b = nlAttr(b, unix.IFLA_LINKINFO|unix.NLA_F_NESTED, info)
	return nlDo(unix.RTM_NEWLINK, unix.NLM_F_CREATE|unix.NLM_F_EXCL, b)
}

func setLinkUp(name string, up bool) error {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return err
	}
	var flags uint32
	if up {
		flags = unix.IFF_UP
	}
	return nlDo(unix.RTM_NEWLINK, 0, ifinfo(int32(ifi.Index), flags, unix.IFF_UP))
}

func addAddr(name string, p netip.Prefix) error {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return err
	}
	a := p.Addr().As4()
	b := []byte{unix.AF_INET, byte(p.Bits()), 0, unix.RT_SCOPE_UNIVERSE}
	b = native.AppendUint32(b, uint32(ifi.Index))
	b = nlAttr(b, unix.IFA_LOCAL, a[:])
	b = nlAttr(b, unix.IFA_ADDRESS, a[:])
	return nlDo(unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_REPLACE, b)
}

// addDefaultRoute adds a default route out of name, via gw if valid (else
// directly on the link), with the given metric.
func addDefaultRoute(name string, gw netip.Addr, metric uint32) error {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return err
	}
	scope := byte(unix.RT_SCOPE_UNIVERSE)
	if !gw.IsValid() {
		scope = unix.RT_SCOPE_LINK
	}
	// struct rtmsg: family, dst_len, src_len, tos, table, protocol, scope, type, flags.
	b := []byte{unix.AF_INET, 0, 0, 0, unix.RT_TABLE_MAIN, unix.RTPROT_STATIC, scope, unix.RTN_UNICAST}
	b = native.AppendUint32(b, 0)
	if gw.IsValid() {
		g := gw.As4()
		b = nlAttr(b, unix.RTA_GATEWAY, g[:])
	}
	b = nlAttr(b, unix.RTA_OIF, u32(uint32(ifi.Index)))
	b = nlAttr(b, unix.RTA_PRIORITY, u32(metric))
	return nlDo(unix.RTM_NEWROUTE, unix.NLM_F_CREATE|unix.NLM_F_EXCL, b)
}

// gratuitousARP broadcasts an ARP request and an ARP reply that both claim ip
// for ifi's MAC (what `arping -U` and `arping -A` send).
func gratuitousARP(ifi *net.Interface, ip netip.Addr) error {
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, int(htons(unix.ETH_P_ARP)))
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	bcast := [8]uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	to := &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ARP), Ifindex: ifi.Index, Halen: 6, Addr: bcast}
	a := ip.As4()
	for _, op := range []uint16{1, 2} {
		pkt := []byte{0, 1, 8, 0, 6, 4} // Ethernet, IPv4, hlen 6, plen 4
		pkt = binary.BigEndian.AppendUint16(pkt, op)
		pkt = append(pkt, ifi.HardwareAddr...)
		pkt = append(pkt, a[:]...)
		if op == 1 {
			pkt = append(pkt, 0, 0, 0, 0, 0, 0)
		} else {
			pkt = append(pkt, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		}
		pkt = append(pkt, a[:]...)
		if err := unix.Sendto(fd, pkt, 0, to); err != nil {
			return fmt.Errorf("gratuitous ARP: %w", err)
		}
	}
	return nil
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }
