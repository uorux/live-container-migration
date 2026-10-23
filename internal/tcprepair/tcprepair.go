// Package tcprepair moves an established TCP connection's kernel state out of
// one socket and into a new one, possibly in another network namespace or on
// another host that has taken over the local address (TCP_REPAIR, as CRIU's
// libsoccr does it). The peer sees nothing but a pause: no handshake, no FIN,
// no RST, and every byte in flight in either direction survives.
//
// The caller must keep the connection quiet while it is dumped and until the
// copy is restored: no reads or writes on the socket, and no packets for it
// reaching any host (the gateway takes its VIP's interface down first).
// Packets the peer sends meanwhile are dropped and retransmitted later.
package tcprepair

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Linux UAPI values (include/uapi/linux/tcp.h) that x/sys doesn't name.
const (
	tcpNoQueue   = 0
	tcpRecvQueue = 1
	tcpSendQueue = 2

	tcpiOptTimestamps = 1
	tcpiOptSack       = 2
	tcpiOptWscale     = 4

	tcpEstablished = 1
)

// window is struct tcp_repair_window.
type window struct {
	SndWl1    uint32
	SndWnd    uint32
	MaxWindow uint32
	RcvWnd    uint32
	RcvWup    uint32
}

// State is everything needed to re-create one established connection.
type State struct {
	Local, Remote netip.AddrPort

	// SendSeq is the sequence number of SendQueue[0] (the oldest unacknowledged
	// byte); RecvSeq is that of RecvQueue[0] (the oldest byte not yet read by
	// the application), so RecvSeq+len(RecvQueue) is rcv_nxt.
	SendSeq, RecvSeq uint32
	// SendQueue holds the bytes written but not acknowledged by the peer; its
	// last Unsent bytes were never transmitted.
	SendQueue []byte
	Unsent    int
	RecvQueue []byte

	MSS                  uint32
	Timestamps, SACK     bool
	Wscale               bool
	SndWscale, RcvWscale uint8
	// TSVal is the connection's current timestamp clock (TCP_TIMESTAMP), so
	// the peer's PAWS check accepts the restored socket's segments.
	TSVal  uint32
	Window window
}

func (s *State) String() string {
	return fmt.Sprintf("%v->%v snd=%d+%d(%d unsent) rcv=%d+%d mss=%d", s.Local, s.Remote,
		s.SendSeq, len(s.SendQueue), s.Unsent, s.RecvSeq, len(s.RecvQueue), s.MSS)
}

func getsockopt(fd, level, opt int, p unsafe.Pointer, n uintptr) (uintptr, error) {
	l := uint32(n)
	_, _, e := unix.Syscall6(unix.SYS_GETSOCKOPT, uintptr(fd), uintptr(level), uintptr(opt), uintptr(p), uintptr(unsafe.Pointer(&l)), 0)
	if e != 0 {
		return 0, e
	}
	return uintptr(l), nil
}

func setsockopt(fd, level, opt int, p unsafe.Pointer, n uintptr) error {
	_, _, e := unix.Syscall6(unix.SYS_SETSOCKOPT, uintptr(fd), uintptr(level), uintptr(opt), uintptr(p), n, 0)
	if e != 0 {
		return e
	}
	return nil
}

func ioctlInt(fd int, req uint) (int, error) {
	return unix.IoctlGetInt(fd, req)
}

// Dump puts the socket into repair mode and reads its state. On success the
// socket stays in repair mode: closing it then sends nothing to the peer. On
// failure it is taken out of repair mode again, untouched.
func Dump(fd int) (st *State, err error) {
	// tcp_info: state at 0, options at 5, wscales at 6 (snd low nibble).
	var info [104]byte
	if _, err := getsockopt(fd, unix.SOL_TCP, unix.TCP_INFO, unsafe.Pointer(&info[0]), uintptr(len(info))); err != nil {
		return nil, fmt.Errorf("TCP_INFO: %w", err)
	}
	if info[0] != tcpEstablished {
		return nil, fmt.Errorf("tcp state %d, not established", info[0])
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 1); err != nil {
		return nil, fmt.Errorf("TCP_REPAIR on: %w", err)
	}
	defer func() {
		if err != nil {
			unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, tcpNoQueue)
			unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 0)
		}
	}()
	st = &State{
		Timestamps: info[5]&tcpiOptTimestamps != 0,
		SACK:       info[5]&tcpiOptSack != 0,
		Wscale:     info[5]&tcpiOptWscale != 0,
		SndWscale:  info[6] & 0x0f,
		RcvWscale:  info[6] >> 4,
	}
	if st.Local, err = sockAddr(unix.Getsockname(fd)); err != nil {
		return nil, err
	}
	if st.Remote, err = sockAddr(unix.Getpeername(fd)); err != nil {
		return nil, err
	}
	mss, err := unix.GetsockoptInt(fd, unix.SOL_TCP, unix.TCP_MAXSEG)
	if err != nil {
		return nil, fmt.Errorf("TCP_MAXSEG: %w", err)
	}
	st.MSS = uint32(mss)
	if st.Timestamps {
		ts, err := unix.GetsockoptInt(fd, unix.SOL_TCP, unix.TCP_TIMESTAMP)
		if err != nil {
			return nil, fmt.Errorf("TCP_TIMESTAMP: %w", err)
		}
		st.TSVal = uint32(ts)
	}
	if _, err := getsockopt(fd, unix.SOL_TCP, unix.TCP_REPAIR_WINDOW, unsafe.Pointer(&st.Window), unsafe.Sizeof(st.Window)); err != nil {
		return nil, fmt.Errorf("TCP_REPAIR_WINDOW: %w", err)
	}
	outq, err := ioctlInt(fd, unix.SIOCOUTQ)
	if err != nil {
		return nil, fmt.Errorf("SIOCOUTQ: %w", err)
	}
	unsent, err := ioctlInt(fd, unix.SIOCOUTQNSD)
	if err != nil {
		return nil, fmt.Errorf("SIOCOUTQNSD: %w", err)
	}
	inq, err := ioctlInt(fd, unix.SIOCINQ)
	if err != nil {
		return nil, fmt.Errorf("SIOCINQ: %w", err)
	}
	st.Unsent = unsent
	if st.SendSeq, st.SendQueue, err = dumpQueue(fd, tcpSendQueue, outq); err != nil {
		return nil, fmt.Errorf("send queue: %w", err)
	}
	if st.RecvSeq, st.RecvQueue, err = dumpQueue(fd, tcpRecvQueue, inq); err != nil {
		return nil, fmt.Errorf("receive queue: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, tcpNoQueue); err != nil {
		return nil, err
	}
	return st, nil
}

func dumpQueue(fd, q, n int) (uint32, []byte, error) {
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, q); err != nil {
		return 0, nil, fmt.Errorf("TCP_REPAIR_QUEUE: %w", err)
	}
	// TCP_QUEUE_SEQ is the sequence number just past the queue (write_seq,
	// rcv_nxt); the queue starts n bytes earlier.
	end, err := unix.GetsockoptInt(fd, unix.SOL_TCP, unix.TCP_QUEUE_SEQ)
	if err != nil {
		return 0, nil, fmt.Errorf("TCP_QUEUE_SEQ: %w", err)
	}
	seq := uint32(end) - uint32(n)
	if n == 0 {
		return seq, nil, nil
	}
	// In repair mode a peek returns the selected queue's whole content.
	buf := make([]byte, n+1)
	got, _, err := unix.Recvfrom(fd, buf, unix.MSG_PEEK|unix.MSG_DONTWAIT)
	if err != nil {
		return 0, nil, fmt.Errorf("peek: %w", err)
	}
	if got != n {
		return 0, nil, fmt.Errorf("peeked %d bytes, queue holds %d", got, n)
	}
	return seq, buf[:n], nil
}

func sockAddr(sa unix.Sockaddr, err error) (netip.AddrPort, error) {
	if err != nil {
		return netip.AddrPort{}, err
	}
	switch a := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)), nil
	case *unix.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr).Unmap(), uint16(a.Port)), nil
	}
	return netip.AddrPort{}, fmt.Errorf("unexpected address family %T", sa)
}

func toSockaddr(ap netip.AddrPort) unix.Sockaddr {
	if ap.Addr().Is4() {
		return &unix.SockaddrInet4{Port: int(ap.Port()), Addr: ap.Addr().As4()}
	}
	return &unix.SockaddrInet6{Port: int(ap.Port()), Addr: ap.Addr().As16()}
}

// Restore creates a socket in the calling thread's network namespace that
// continues the dumped connection, and returns its descriptor (blocking mode,
// close-on-exec). The local address must exist in that namespace.
//
// pending is the tail of the send queue the old socket never transmitted. It
// can't go back in repair mode (the kernel would count it as sent), and
// writing it may block on the peer's window, so the caller writes it as
// ordinary data before anything else it sends.
func Restore(st *State) (fd int, pending []byte, err error) {
	family := unix.AF_INET
	if st.Local.Addr().Is6() {
		family = unix.AF_INET6
	}
	fd, err = unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP)
	if err != nil {
		return -1, nil, err
	}
	defer func() {
		if err != nil {
			unix.Close(fd)
			fd = -1
		}
	}()
	// Repair mode also forces SO_REUSEADDR-style binding, so the restored
	// socket can share the listener's port.
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 1); err != nil {
		return -1, nil, fmt.Errorf("TCP_REPAIR on: %w", err)
	}
	if err := unix.Bind(fd, toSockaddr(st.Local)); err != nil {
		return -1, nil, fmt.Errorf("bind %v: %w", st.Local, err)
	}
	for _, q := range []struct {
		q   int
		seq uint32
	}{{tcpSendQueue, st.SendSeq}, {tcpRecvQueue, st.RecvSeq}} {
		if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, q.q); err != nil {
			return -1, nil, fmt.Errorf("TCP_REPAIR_QUEUE: %w", err)
		}
		if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_QUEUE_SEQ, int(int32(q.seq))); err != nil {
			return -1, nil, fmt.Errorf("TCP_QUEUE_SEQ: %w", err)
		}
	}
	// In repair mode connect() moves straight to ESTABLISHED: no SYN.
	if err := unix.Connect(fd, toSockaddr(st.Remote)); err != nil {
		return -1, nil, fmt.Errorf("connect %v: %w", st.Remote, err)
	}
	opts := []unix.TCPRepairOpt{{Code: unix.TCPOPT_MAXSEG, Val: st.MSS}}
	if st.Wscale {
		opts = append(opts, unix.TCPRepairOpt{Code: unix.TCPOPT_WINDOW, Val: uint32(st.SndWscale) | uint32(st.RcvWscale)<<16})
	}
	if st.SACK {
		opts = append(opts, unix.TCPRepairOpt{Code: unix.TCPOPT_SACK_PERMITTED})
	}
	if st.Timestamps {
		opts = append(opts, unix.TCPRepairOpt{Code: unix.TCPOPT_TIMESTAMP})
	}
	if err := setsockopt(fd, unix.SOL_TCP, unix.TCP_REPAIR_OPTIONS, unsafe.Pointer(&opts[0]), uintptr(len(opts))*unsafe.Sizeof(opts[0])); err != nil {
		return -1, nil, fmt.Errorf("TCP_REPAIR_OPTIONS: %w", err)
	}
	if st.Timestamps {
		if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_TIMESTAMP, int(int32(st.TSVal))); err != nil {
			return -1, nil, fmt.Errorf("TCP_TIMESTAMP: %w", err)
		}
	}
	sent := st.SendQueue[:len(st.SendQueue)-st.Unsent]
	if err := restoreQueue(fd, tcpRecvQueue, st.RecvQueue); err != nil {
		return -1, nil, fmt.Errorf("receive queue: %w", err)
	}
	if err := restoreQueue(fd, tcpSendQueue, sent); err != nil {
		return -1, nil, fmt.Errorf("send queue: %w", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, tcpNoQueue); err != nil {
		return -1, nil, err
	}
	w := st.Window
	if err := setsockopt(fd, unix.SOL_TCP, unix.TCP_REPAIR_WINDOW, unsafe.Pointer(&w), unsafe.Sizeof(w)); err != nil {
		return -1, nil, fmt.Errorf("TCP_REPAIR_WINDOW: %w", err)
	}
	// Leaving repair mode sends a window probe, which gets the peer to ACK
	// and so resumes the flow at once instead of at its next retransmit.
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, unix.TCP_REPAIR_OFF); err != nil {
		return -1, nil, fmt.Errorf("TCP_REPAIR off: %w", err)
	}
	return fd, st.SendQueue[len(sent):], nil
}

func restoreQueue(fd, q int, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR_QUEUE, q); err != nil {
		return err
	}
	return writeAll(fd, data)
}

func writeAll(fd int, b []byte) error {
	for len(b) > 0 {
		n, err := unix.Write(fd, b)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		b = b[n:]
	}
	return nil
}

// DumpConn dumps a *net.TCPConn (see Dump). Afterwards, closing c sends
// nothing to the peer.
func DumpConn(c *net.TCPConn) (*State, error) {
	rc, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	var st *State
	var derr error
	if err := rc.Control(func(fd uintptr) { st, derr = Dump(int(fd)) }); err != nil {
		return nil, err
	}
	return st, derr
}

// Conn wraps a restored descriptor as a net.Conn (taking ownership of fd).
func Conn(fd int, name string) (net.Conn, error) {
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	return net.FileConn(f)
}
