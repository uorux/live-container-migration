package gateway

import (
	"fmt"
	"net"
	"time"

	"lcm/internal/tcprepair"
	"lcm/internal/tunnel"
)

// HandoffVersion is bumped whenever Handoff's meaning changes; a gateway
// refuses a handoff of another version (the sender then keeps its players).
const HandoffVersion = 1

// Handoff is everything a gateway passes to the one taking over its VIP:
// each backend's frozen relay session and its players' TCP connections.
type Handoff struct {
	Version  int
	Backends []BackendHandoff
}

// BackendHandoff is one backend's part of a Handoff.
type BackendHandoff struct {
	Name    string
	Session tunnel.SessionState
	Conns   map[uint32]*tcprepair.State
}

// Players returns the number of player connections in h.
func (h *Handoff) Players() int {
	n := 0
	for _, b := range h.Backends {
		n += len(b.Conns)
	}
	return n
}

// Freeze stops every backend's session and takes its players' connections
// out of the kernel (tcprepair): afterwards those sockets are closed without
// a word to the players, who just see a pause. Call it with the VIP down, so
// nothing arrives for those connections meanwhile. Players the gateway still
// owns (status pings, the menu, logins on hold) aren't in it: their sockets
// go when this gateway does, and those players reconnect.
func (g *Gateway) Freeze(timeout time.Duration) (*Handoff, error) {
	h := &Handoff{Version: HandoffVersion}
	g.mu.Lock()
	names := append([]string(nil), g.order...)
	g.mu.Unlock()
	for _, name := range names {
		b := g.Backend(name)
		fz, err := b.Session().Freeze(timeout)
		if err != nil {
			return nil, fmt.Errorf("backend %s: %w", name, err)
		}
		bh := BackendHandoff{Name: name, Session: fz.State, Conns: map[uint32]*tcprepair.State{}}
		for id, tc := range fz.Locals {
			st, err := tcprepair.DumpConn(tc)
			if err != nil {
				g.log.Warn("player connection can't be moved; dropping it", "backend", name, "stream", id, "err", err)
				tc.SetLinger(0)
				tc.Close()
				continue
			}
			tc.Close() // in repair mode: nothing is sent
			bh.Conns[id] = st
		}
		if fz.Dropped > 0 {
			g.log.Info("streams not handed off", "backend", name, "dropped", fz.Dropped)
		}
		h.Backends = append(h.Backends, bh)
	}
	return h, nil
}

// Import takes a Handoff over: restore re-creates each player connection
// (in the VIP's namespace, before the VIP comes up here) and returns it with
// its unsent tail; each backend's session is replaced by the imported one,
// to which the relay then reconnects and resumes. It returns the number of
// players restored.
func (g *Gateway) Import(h *Handoff, restore func(*tcprepair.State) (net.Conn, []byte, error)) (int, error) {
	if h.Version != HandoffVersion {
		return 0, fmt.Errorf("handoff version %d, this gateway speaks %d", h.Version, HandoffVersion)
	}
	restored := 0
	for _, bh := range h.Backends {
		b := g.Backend(bh.Name)
		if b == nil {
			g.log.Warn("handoff for a backend this gateway doesn't serve; its players are lost", "backend", bh.Name, "players", len(bh.Conns))
			continue
		}
		locals := map[uint32]net.Conn{}
		pending := map[uint32][]byte{}
		for id, st := range bh.Conns {
			c, p, err := restore(st)
			if err != nil {
				g.log.Warn("player connection not restored", "backend", bh.Name, "stream", id, "conn", st.String(), "err", err)
				continue
			}
			locals[id], pending[id] = c, p
			restored++
		}
		old := b.sess.Swap(tunnel.Import(b.tc, bh.Session, locals, pending))
		old.CloseAll()
	}
	return restored, nil
}
