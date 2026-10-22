package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Group runs one Relay per gateway for a single server, so players can come in
// through several gateways (one per ingress, say), each over its own resumable
// session. The control socket's quiesce and resume apply to all of them.
type Group struct {
	members      []*Relay
	checkSockets bool
	log          *slog.Logger
}

// NewGroup creates a relay per gateway address, all with cfg otherwise.
func NewGroup(cfg Config, gateways []string) *Group {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	g := &Group{checkSockets: cfg.CheckSockets, log: cfg.Logger}
	for _, gw := range gateways {
		c := cfg
		c.Gateway = gw
		c.CheckSockets = false // checked once for the whole group, after all closed
		if len(gateways) > 1 {
			c.Logger = cfg.Logger.With("gateway", gw)
		}
		g.members = append(g.members, New(c))
	}
	return g
}

// Members returns the per-gateway relays.
func (g *Group) Members() []*Relay { return g.members }

// SetChildPID records the server's pid for status.
func (g *Group) SetChildPID(pid int) {
	for _, r := range g.members {
		r.ChildPID.Store(int64(pid))
	}
}

// Run maintains every member's tunnel until ctx is cancelled.
func (g *Group) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, r := range g.members {
		wg.Add(1)
		go func() { defer wg.Done(); r.Run(ctx) }()
	}
	wg.Wait()
}

// Quiesce quiesces every member and then waits until no non-loopback socket of
// the process remains. If any of it fails, everything is resumed: the agent
// treats a failed quiesce as "nothing stopped" and won't send resume itself.
func (g *Group) Quiesce(ctx context.Context) error {
	errs := make([]error, len(g.members))
	var wg sync.WaitGroup
	for i, r := range g.members {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = r.Quiesce(ctx) }()
	}
	wg.Wait()
	err := errors.Join(errs...)
	if err == nil && g.checkSockets {
		err = waitNoSockets(ctx, g.log)
	}
	if err != nil {
		g.Resume()
		return err
	}
	return nil
}

// Resume re-enables every member's tunnel.
func (g *Group) Resume() {
	for _, r := range g.members {
		r.Resume()
	}
}

// Shutdown stops every member (see Relay.Shutdown).
func (g *Group) Shutdown(timeout time.Duration) {
	var wg sync.WaitGroup
	for _, r := range g.members {
		wg.Add(1)
		go func() { defer wg.Done(); r.Shutdown(timeout) }()
	}
	wg.Wait()
}

// GroupStatus is the control socket's status document for several gateways.
type GroupStatus struct {
	Ready    bool     `json:"ready"`
	ChildPID int64    `json:"child_pid,omitempty"`
	Gateways []Status `json:"gateways"`
}

// Status returns the single member's status (unchanged format) or, with
// several gateways, one entry per gateway.
func (g *Group) Status() any {
	if len(g.members) == 1 {
		return g.members[0].Status()
	}
	st := GroupStatus{Ready: true}
	for _, r := range g.members {
		s := r.Status()
		st.Ready = st.Ready && s.Ready
		st.ChildPID = s.ChildPID
		st.Gateways = append(st.Gateways, s)
	}
	return st
}

// ServeControl serves the control socket for the whole group.
func (g *Group) ServeControl(ln net.Listener) error {
	return serveControl(ln, g.log, g)
}

func waitNoSockets(ctx context.Context, log *slog.Logger) error {
	for {
		socks, err := NonLoopbackSockets()
		if err != nil {
			log.Warn("socket check unavailable", "err", err)
			return nil
		}
		if len(socks) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("non-loopback sockets remain: %v", socks)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
