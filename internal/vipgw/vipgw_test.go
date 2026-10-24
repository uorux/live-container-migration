package vipgw

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"lcm/internal/gateway"
	"lcm/internal/wire"
)

type fakeNS struct{ ups, downs atomic.Int32 }

func (n *fakeNS) Prefix() netip.Prefix     { return netip.MustParsePrefix("10.0.0.250/24") }
func (n *fakeNS) Up() error                { n.ups.Add(1); return nil }
func (n *fakeNS) Down() error              { n.downs.Add(1); return nil }
func (n *fakeNS) Announce() error          { return nil }
func (n *fakeNS) Do(fn func() error) error { return fn() }

const testNS, testLease = "lcm", "vip"

// newActive returns a controller serving the VIP, with the lease held by
// holder and last renewed by this gateway renewedAgo ago.
func newActive(t *testing.T, holder string, renewedAgo time.Duration) (*Controller, *fakeNS, *fake.Clientset) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := metav1.NewMicroTime(time.Now())
	secs := int32(10)
	kc := fake.NewSimpleClientset(&coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: testLease, Namespace: testNS},
		Spec: coordv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &now, LeaseDurationSeconds: &secs}})
	ns := &fakeNS{}
	g := gateway.New(gateway.Config{Logger: log})
	c := New(Config{Gateway: g, NS: ns, Kube: kc, Namespace: testNS, Identity: "me", Lease: testLease,
		LeaseDuration: 10 * time.Second, RenewInterval: 2 * time.Second, Logger: log})
	c.mu.Lock()
	c.activateLocked("test")
	c.mu.Unlock()
	if !c.Active() {
		t.Fatal("not active")
	}
	c.setRenewed(time.Now().Add(-renewedAgo))
	kc.ClearActions()
	return c, ns, kc
}

// A freshly renewed lease: serving costs no API call.
func TestServingFresh(t *testing.T) {
	c, _, kc := newActive(t, "me", time.Second)
	if !c.Serving() {
		t.Fatal("not serving with a fresh lease")
	}
	if n := len(kc.Actions()); n != 0 {
		t.Fatalf("%d API calls for a fresh lease", n)
	}
}

// After a stall the lease belongs to another gateway: the first connection
// accepted makes this one step down, and is refused, before anything is
// served; its relay sessions and players are forgotten.
func TestServingAfterStallLeaseLost(t *testing.T) {
	c, ns, _ := newActive(t, "other", 2*time.Minute)
	b := c.cfg.Gateway.DefaultBackend()
	sess := b.Session()
	player, local := net.Pipe()
	defer player.Close()
	sess.AddStream(local, wire.Open{})

	if c.Serving() {
		t.Fatal("serving with a lease another gateway took")
	}
	if c.Active() || ns.downs.Load() != 1 {
		t.Fatalf("active %v, VIP downs %d: want stepped down", c.Active(), ns.downs.Load())
	}
	if b.Session() == sess || sess.NumStreams() != 0 {
		t.Fatal("relay sessions and players not forgotten on step-down")
	}
	if c.Serving() {
		t.Fatal("serving after stepping down")
	}
}

// After a shorter stall nobody took the lease: it is renewed on the spot and
// the connection is served.
func TestServingAfterStallLeaseKept(t *testing.T) {
	c, ns, kc := newActive(t, "me", 8*time.Second)
	if !c.Serving() {
		t.Fatal("not serving although the lease is still ours")
	}
	if !c.Active() || ns.downs.Load() != 0 {
		t.Fatal("stepped down")
	}
	l, err := kc.CoordinationV1().Leases(testNS).Get(context.Background(), testLease, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(l.Spec.RenewTime.Time) > time.Second {
		t.Fatalf("lease not renewed: renew time %v", l.Spec.RenewTime.Time)
	}
	if !c.leaseFresh() {
		t.Fatal("renewal not recorded")
	}
}

// A standby never serves.
func TestStandbyNotServing(t *testing.T) {
	c, _, _ := newActive(t, "me", 0)
	c.mu.Lock()
	c.stepDownLocked("test")
	c.mu.Unlock()
	if c.Serving() {
		t.Fatal("a standby serves")
	}
}
