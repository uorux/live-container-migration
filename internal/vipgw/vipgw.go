// Package vipgw runs a set of lcm-gateways (one per node) behind one VIP, so
// players on a path that can only point at a fixed address (the router's
// port-forward) keep their connections when the active gateway's node is
// drained or the gateway is upgraded: the active gateway hands its players'
// connections and its relay sessions to a standby, which takes the VIP over.
//
// Who is active is a Kubernetes Lease. The active gateway labels its pod
// (ActiveLabel=true) so a Service can point relays at it. On SIGTERM it
// hands off to a ready standby (HandOff); if that fails it takes everything
// back. If the active node dies instead, a standby takes the VIP when the
// lease expires; the players connected through it are lost then.
package vipgw

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"lcm/internal/gateway"
	"lcm/internal/tcprepair"
	"lcm/internal/vip"
)

// Config configures a Controller.
type Config struct {
	Gateway *gateway.Gateway
	NS      *vip.NS
	// Token authenticates handoffs between gateways (the tunnel token).
	Token []byte

	Kube      kubernetes.Interface
	Namespace string
	Identity  string // this gateway's pod name
	Lease     string // the Lease's name
	// Selector picks the gateway pods that can take a handoff.
	Selector    string
	HandoffPort int
	// TunnelPort is where gateways accept relays (the same on every node):
	// after a handoff this gateway forwards relays to the new one's.
	TunnelPort  int
	ActiveLabel string

	LeaseDuration time.Duration // default 10s
	RenewInterval time.Duration // default 2s
	Logger        *slog.Logger
}

// Controller decides when this gateway serves the VIP.
type Controller struct {
	cfg Config
	log *slog.Logger

	mu         sync.Mutex // serialises activate, stepDown, HandOff and handoffs in
	active     atomic.Bool
	lastRenew  time.Time
	handingOff bool
	stats      Stats
}

// Stats are reported on the admin endpoint.
type Stats struct {
	Active       bool      `json:"active"`
	Since        time.Time `json:"since"`
	HandoffsOut  int       `json:"handoffs_out"`
	HandoffsIn   int       `json:"handoffs_in"`
	LastHandoff  string    `json:"last_handoff,omitempty"`
	LastPlayers  int       `json:"last_players"`
	LastDuration string    `json:"last_duration,omitempty"`
}

// New returns a Controller (Run starts it).
func New(cfg Config) *Controller {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 10 * time.Second
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Controller{cfg: cfg, log: cfg.Logger.With("vip", cfg.NS.Prefix().Addr().String())}
}

// Active reports whether this gateway serves the VIP now.
func (c *Controller) Active() bool { return c.active.Load() }

// Stats returns a snapshot.
func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Active = c.active.Load()
	return s
}

// Run serves handoffs and keeps the lease until ctx is done; then, if this
// gateway is active, it hands off to a standby before returning.
func (c *Controller) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(c.cfg.HandoffPort))
	if err != nil {
		return fmt.Errorf("handoff listener: %w", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go c.serveHandoff(conn)
		}
	}()
	c.setLabel(context.Background(), false)
	t := time.NewTicker(c.cfg.RenewInterval)
	defer t.Stop()
	c.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			hctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := c.HandOff(hctx)
			cancel()
			return err
		case <-t.C:
			c.tick(ctx)
		}
	}
}

func (c *Controller) tick(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handingOff || ctx.Err() != nil {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, c.cfg.RenewInterval)
	defer cancel()
	if c.active.Load() {
		err := c.renew(rctx)
		switch {
		case err == nil:
			c.lastRenew = time.Now()
		case errors.Is(err, errLost):
			c.stepDownLocked("lease taken by another gateway")
		case time.Since(c.lastRenew) > c.cfg.LeaseDuration*2/3:
			// Another gateway may take the VIP once the lease runs out:
			// let go first, so the VIP is never answered twice.
			c.stepDownLocked("can't renew the lease: " + err.Error())
		default:
			c.log.Warn("lease renewal failed; retrying", "err", err)
		}
		return
	}
	if ok, err := c.tryAcquire(rctx); err != nil {
		c.log.Debug("lease not acquired", "err", err)
	} else if ok {
		c.activateLocked("lease acquired")
	}
}

// ---- lease ----

var errLost = errors.New("lease held by another gateway")

func (c *Controller) leases() interface {
	Get(context.Context, string, metav1.GetOptions) (*coordv1.Lease, error)
	Create(context.Context, *coordv1.Lease, metav1.CreateOptions) (*coordv1.Lease, error)
	Update(context.Context, *coordv1.Lease, metav1.UpdateOptions) (*coordv1.Lease, error)
} {
	return c.cfg.Kube.CoordinationV1().Leases(c.cfg.Namespace)
}

func (c *Controller) expired(l *coordv1.Lease, now time.Time) bool {
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" || l.Spec.RenewTime == nil {
		return true
	}
	d := c.cfg.LeaseDuration
	if l.Spec.LeaseDurationSeconds != nil {
		d = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
	}
	return now.After(l.Spec.RenewTime.Add(d))
}

func holder(l *coordv1.Lease) string {
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func (c *Controller) claim(l *coordv1.Lease, now time.Time) {
	t := metav1.NewMicroTime(now)
	secs := int32(c.cfg.LeaseDuration / time.Second)
	id := c.cfg.Identity
	if holder(l) != id {
		l.Spec.AcquireTime = &t
		n := int32(1)
		if l.Spec.LeaseTransitions != nil {
			n = *l.Spec.LeaseTransitions + 1
		}
		l.Spec.LeaseTransitions = &n
	}
	l.Spec.HolderIdentity = &id
	l.Spec.RenewTime = &t
	l.Spec.LeaseDurationSeconds = &secs
}

func (c *Controller) renew(ctx context.Context) error {
	l, err := c.leases().Get(ctx, c.cfg.Lease, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if holder(l) != c.cfg.Identity {
		return errLost
	}
	c.claim(l, time.Now())
	_, err = c.leases().Update(ctx, l, metav1.UpdateOptions{})
	return err
}

// tryAcquire takes the lease if nobody holds it, it has expired, or it
// already names this gateway (a handoff in, or a restart).
func (c *Controller) tryAcquire(ctx context.Context) (bool, error) {
	now := time.Now()
	l, err := c.leases().Get(ctx, c.cfg.Lease, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		l = &coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: c.cfg.Lease, Namespace: c.cfg.Namespace}}
		c.claim(l, now)
		_, err = c.leases().Create(ctx, l, metav1.CreateOptions{})
		return err == nil, err
	}
	if err != nil {
		return false, err
	}
	if holder(l) != c.cfg.Identity && !c.expired(l, now) {
		return false, nil
	}
	c.claim(l, now)
	_, err = c.leases().Update(ctx, l, metav1.UpdateOptions{})
	return err == nil, err
}

// takeOver points the lease at this gateway after a handoff from `from`.
func (c *Controller) takeOver(from string) {
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		l, err := c.leases().Get(ctx, c.cfg.Lease, metav1.GetOptions{})
		if err == nil {
			h := holder(l)
			if h != c.cfg.Identity && h != from && h != "" && !c.expired(l, time.Now()) {
				cancel()
				c.log.Error("lease held by a third gateway after a handoff; stepping down", "holder", h)
				c.mu.Lock()
				c.stepDownLocked("lease held by " + h)
				c.mu.Unlock()
				return
			}
			c.claim(l, time.Now())
			_, err = c.leases().Update(ctx, l, metav1.UpdateOptions{})
		}
		cancel()
		if err == nil {
			c.mu.Lock()
			c.lastRenew = time.Now()
			c.mu.Unlock()
			return
		}
		c.log.Warn("taking the lease over failed; retrying", "err", err)
		time.Sleep(500 * time.Millisecond)
	}
}

// ---- becoming active, stepping down ----

func (c *Controller) activateLocked(why string) {
	if err := c.cfg.NS.Up(); err != nil {
		c.log.Error("bringing the VIP up failed", "err", err)
		return
	}
	c.active.Store(true)
	c.lastRenew = time.Now()
	c.stats.Since = time.Now()
	c.log.Info("serving the VIP", "why", why)
	go c.setLabel(context.Background(), true)
	go c.reannounce()
}

// reannounce repeats the gratuitous ARP, in case the first was lost.
func (c *Controller) reannounce() {
	for _, d := range []time.Duration{time.Second, 3 * time.Second} {
		time.Sleep(d)
		if c.active.Load() {
			c.cfg.NS.Announce()
		}
	}
}

func (c *Controller) stepDownLocked(why string) {
	if !c.active.Load() {
		return
	}
	c.active.Store(false)
	if err := c.cfg.NS.Down(); err != nil {
		c.log.Error("taking the VIP down failed", "err", err)
	}
	c.stats.Since = time.Now()
	c.log.Warn("stopped serving the VIP", "why", why)
	go c.setLabel(context.Background(), false)
}

func (c *Controller) setLabel(ctx context.Context, active bool) {
	if c.cfg.ActiveLabel == "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, c.cfg.ActiveLabel, strconv.FormatBool(active))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.cfg.Kube.CoreV1().Pods(c.cfg.Namespace).Patch(ctx, c.cfg.Identity, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		c.log.Warn("labelling this pod failed", "active", active, "err", err)
	}
}

// ---- handoff out ----

type peer struct{ name, ip, addr string }

func (c *Controller) peers(ctx context.Context) ([]peer, error) {
	pods, err := c.cfg.Kube.CoreV1().Pods(c.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: c.cfg.Selector})
	if err != nil {
		return nil, err
	}
	var out []peer
	for _, p := range pods.Items {
		if p.Name == c.cfg.Identity || p.DeletionTimestamp != nil || p.Status.PodIP == "" || !podReady(&p) {
			continue
		}
		out = append(out, peer{p.Name, p.Status.PodIP, net.JoinHostPort(p.Status.PodIP, strconv.Itoa(c.cfg.HandoffPort))})
	}
	return out, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// HandOff moves the VIP, every player connection and every relay session
// to a ready standby. If none takes it, everything is restored here.
func (c *Controller) HandOff(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active.Load() {
		return nil
	}
	peers, err := c.peers(ctx)
	if err != nil || len(peers) == 0 {
		return fmt.Errorf("no standby to hand off to (players stay here): %v", err)
	}
	c.handingOff = true
	defer func() { c.handingOff = false }()

	start := time.Now()
	if err := c.cfg.NS.Down(); err != nil {
		return fmt.Errorf("VIP down: %w", err)
	}
	h, err := c.cfg.Gateway.Freeze(5 * time.Second)
	if err != nil {
		// Partly frozen: what can't be handed off is lost either way.
		c.log.Error("freezing the gateway failed; staying active", "err", err)
		c.cfg.NS.Up()
		return err
	}
	var blob bytes.Buffer
	if err := gob.NewEncoder(&blob).Encode(h); err != nil {
		return c.rollback(h, start, fmt.Errorf("encode: %w", err))
	}
	for _, p := range peers {
		msg, err := c.send(ctx, p, blob.Bytes())
		if err == nil {
			c.forwardRelays(p)
			c.active.Store(false)
			c.stats.HandoffsOut++
			c.stats.LastHandoff = "out to " + p.name
			c.stats.LastPlayers = h.Players()
			c.stats.LastDuration = time.Since(start).Round(time.Millisecond).String()
			c.stats.Since = time.Now()
			c.log.Info("handed off", "to", p.name, "players", h.Players(), "took", time.Since(start).Round(time.Millisecond), "peer", msg)
			go c.setLabel(context.Background(), false)
			return nil
		}
		var delivered *deliveredErr
		if c.tookOver(err, p) {
			c.forwardRelays(p)
			c.active.Store(false)
			c.log.Warn("handoff reply lost but the standby took the lease; treating it as done", "to", p.name, "err", err)
			go c.setLabel(context.Background(), false)
			return nil
		}
		c.log.Warn("handoff to standby failed", "to", p.name, "err", err)
		if errors.As(err, &delivered) {
			break // the standby may hold some of it: don't offer it to a second one
		}
	}
	return c.rollback(h, start, errors.New("no standby took the handoff"))
}

// rollback restores a frozen handoff here and brings the VIP back.
func (c *Controller) rollback(h *gateway.Handoff, start time.Time, cause error) error {
	conns, restored := c.restoreAll(h)
	if err := c.cfg.NS.Up(); err != nil {
		c.log.Error("VIP up after a failed handoff", "err", err)
	}
	probeAll(conns)
	c.log.Error("handoff failed; players restored here", "players", restored, "of", h.Players(), "took", time.Since(start).Round(time.Millisecond), "err", cause)
	return cause
}

// tookOver: the standby got the handoff and holds the lease, though its
// answer was lost.
func (c *Controller) tookOver(err error, p peer) bool {
	var delivered *deliveredErr
	return errors.As(err, &delivered) && c.leaseHeldBy(p.name, 3*time.Second)
}

func (c *Controller) forwardRelays(p peer) {
	if c.cfg.TunnelPort > 0 {
		c.cfg.Gateway.ForwardTunnels(net.JoinHostPort(p.ip, strconv.Itoa(c.cfg.TunnelPort)))
	}
}

func (c *Controller) leaseHeldBy(name string, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		l, err := c.leases().Get(ctx, c.cfg.Lease, metav1.GetOptions{})
		cancel()
		if err == nil && holder(l) == name {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// deliveredErr: the standby got the whole handoff but no answer came back.
type deliveredErr struct{ err error }

func (e *deliveredErr) Error() string { return "handoff delivered, no answer: " + e.err.Error() }
func (e *deliveredErr) Unwrap() error { return e.err }

const authTag = "lcm-vip-handoff\x00"

func mac(token, nonce []byte) []byte {
	m := hmac.New(sha256.New, token)
	m.Write([]byte(authTag))
	m.Write(nonce)
	return m.Sum(nil)
}

func (c *Controller) send(ctx context.Context, p peer, blob []byte) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	nonce := make([]byte, 32)
	if _, err := io.ReadFull(conn, nonce); err != nil {
		return "", err
	}
	msg := mac(c.cfg.Token, nonce)
	msg = binary.BigEndian.AppendUint16(msg, uint16(len(c.cfg.Identity)))
	msg = append(msg, c.cfg.Identity...)
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(blob)))
	if _, err := conn.Write(msg); err != nil {
		return "", err
	}
	if _, err := conn.Write(blob); err != nil {
		return "", &deliveredErr{err} // possibly partly delivered: treat as delivered
	}
	var hdr [3]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return "", &deliveredErr{err}
	}
	text := make([]byte, binary.BigEndian.Uint16(hdr[1:]))
	if _, err := io.ReadFull(conn, text); err != nil {
		return "", &deliveredErr{err}
	}
	if hdr[0] != 0 {
		return "", errors.New("standby refused: " + string(text))
	}
	return string(text), nil
}

// ---- handoff in ----

func reply(conn net.Conn, ok bool, text string) {
	status := byte(1)
	if ok {
		status = 0
	}
	b := []byte{status}
	b = binary.BigEndian.AppendUint16(b, uint16(len(text)))
	conn.Write(append(b, text...))
}

func (c *Controller) serveHandoff(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	nonce := make([]byte, 32)
	rand.Read(nonce)
	if _, err := conn.Write(nonce); err != nil {
		return
	}
	got := make([]byte, sha256.Size+2)
	if _, err := io.ReadFull(conn, got); err != nil {
		return
	}
	if !hmac.Equal(got[:sha256.Size], mac(c.cfg.Token, nonce)) {
		c.log.Warn("handoff with a bad token refused", "remote", conn.RemoteAddr().String())
		return
	}
	from := make([]byte, binary.BigEndian.Uint16(got[sha256.Size:]))
	var n [4]byte
	if _, err := io.ReadFull(conn, from); err != nil {
		return
	}
	if _, err := io.ReadFull(conn, n[:]); err != nil {
		return
	}
	blob := make([]byte, binary.BigEndian.Uint32(n[:]))
	if _, err := io.ReadFull(conn, blob); err != nil {
		return
	}
	var h gateway.Handoff
	if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&h); err != nil {
		reply(conn, false, "decode: "+err.Error())
		return
	}

	c.mu.Lock()
	if c.active.Load() {
		c.mu.Unlock()
		reply(conn, false, "this gateway already serves the VIP")
		return
	}
	start := time.Now()
	conns, restored := c.restoreAll(&h)
	if err := c.cfg.NS.Up(); err != nil {
		c.mu.Unlock()
		c.log.Error("VIP up after a handoff in failed", "err", err)
		reply(conn, false, "VIP up: "+err.Error())
		return
	}
	probeAll(conns)
	c.active.Store(true)
	c.lastRenew = time.Now()
	c.stats.HandoffsIn++
	c.stats.LastHandoff = "in from " + string(from)
	c.stats.LastPlayers = restored
	c.stats.LastDuration = time.Since(start).Round(time.Millisecond).String()
	c.stats.Since = time.Now()
	c.mu.Unlock()
	reply(conn, true, fmt.Sprintf("%d of %d players restored in %v", restored, h.Players(), time.Since(start).Round(time.Millisecond)))
	c.log.Info("took a handoff", "from", string(from), "players", restored, "of", h.Players(), "took", time.Since(start).Round(time.Millisecond))
	go c.takeOver(string(from))
	go c.setLabel(context.Background(), true)
	go c.reannounce()
}

// restoreAll imports a handoff into the gateway, restoring each player
// connection in the VIP namespace (before the VIP is up here).
func (c *Controller) restoreAll(h *gateway.Handoff) ([]net.Conn, int) {
	var conns []net.Conn
	n, err := c.cfg.Gateway.Import(h, func(st *tcprepair.State) (net.Conn, []byte, error) {
		var conn net.Conn
		var pending []byte
		err := c.cfg.NS.Do(func() error {
			fd, p, err := tcprepair.Restore(st)
			if err != nil {
				return err
			}
			pending = p
			conn, err = tcprepair.Conn(fd, "player")
			return err
		})
		if err == nil {
			conns = append(conns, conn)
		}
		return conn, pending, err
	})
	if err != nil {
		c.log.Error("handoff import failed", "err", err)
	}
	return conns, n
}

func probeAll(conns []net.Conn) {
	for _, conn := range conns {
		tcprepair.Probe(conn)
	}
}
