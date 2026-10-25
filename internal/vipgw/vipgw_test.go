package vipgw

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

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

// failGets makes the first n lease reads fail the way a request on a
// connection to a vanished API server does: no answer before the deadline.
func failGets(kc *fake.Clientset, n int) *atomic.Int32 {
	var calls atomic.Int32
	kc.PrependReactor("get", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) <= int32(n) {
			return true, nil, fmt.Errorf("Get \"https://10.43.0.1:443/...\": %w", context.DeadlineExceeded)
		}
		return false, nil, nil
	})
	return &calls
}

func countResets(c *Controller) *atomic.Int32 {
	var n atomic.Int32
	c.cfg.ResetConns = func() { n.Add(1) }
	c.cfg.APITimeout = 100 * time.Millisecond
	return &n
}

func leaseHolder(t *testing.T, kc *fake.Clientset) string {
	t.Helper()
	l, err := kc.CoordinationV1().Leases(testNS).Get(context.Background(), testLease, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return holder(l)
}

// Renewals that get no answer (an API server's node went down under the
// connection) are retried on fresh connections within the tick: the lease is
// renewed and the VIP kept, as long as one API server answers.
func TestRenewReconnectsWithinTick(t *testing.T) {
	c, ns, kc := newActive(t, "me", 6*time.Second) // a tick away from letting go
	resets := countResets(c)
	failGets(kc, 2)
	c.tick(context.Background())
	if !c.Active() || ns.downs.Load() != 0 {
		t.Fatal("stepped down although a retry could renew")
	}
	if !c.leaseFresh() {
		t.Fatal("lease not renewed")
	}
	if resets.Load() != 2 {
		t.Fatalf("%d connection resets, want 2 (one per unanswered attempt)", resets.Load())
	}
}

// With no API server answering at all, the gateway still lets go of the VIP
// before another may take it.
func TestRenewNoAnswerStepsDown(t *testing.T) {
	c, ns, kc := newActive(t, "me", 7*time.Second)
	countResets(c)
	failGets(kc, 1<<20)
	c.tick(context.Background())
	if c.Active() || ns.downs.Load() != 1 {
		t.Fatal("kept the VIP without renewing the lease past 2/3 of its duration")
	}
}

// An attempt that hangs is cut off at APITimeout, so the retry fits the tick.
func TestAPIAttemptsBoundsAHang(t *testing.T) {
	c, _, _ := newActive(t, "me", 0)
	resets := countResets(c)
	var calls int
	start := time.Now()
	err := c.apiAttempts(context.Background(), func(ctx context.Context) error {
		if calls++; calls == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil || calls != 2 || resets.Load() != 1 {
		t.Fatalf("err %v, %d calls, %d resets: want a second attempt after a reset", err, calls, resets.Load())
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v: the hanging attempt wasn't cut off", d)
	}
}

// An answer from the API server (a conflict, say) is the caller's to handle:
// not retried, connections kept.
func TestAPIAttemptsAnswerNotRetried(t *testing.T) {
	c, _, _ := newActive(t, "me", 0)
	resets := countResets(c)
	var calls int
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, testLease, errors.New("modified"))
	err := c.apiAttempts(context.Background(), func(context.Context) error { calls++; return conflict })
	if !apierrors.IsConflict(err) || calls != 1 || resets.Load() != 0 {
		t.Fatalf("err %v, %d calls, %d resets: want the conflict back at once", err, calls, resets.Load())
	}
}

// Right after a handoff in, the lease still names the gateway it came from
// until takeOver rewrites it. A tick meanwhile takes it over rather than
// reading it as lost; without a pending handoff it is lost.
func TestRenewDuringTakeOver(t *testing.T) {
	c, ns, kc := newActive(t, "old", time.Second)
	c.mu.Lock()
	c.takingOverFrom = "old"
	c.mu.Unlock()
	c.tick(context.Background())
	if !c.Active() || ns.downs.Load() != 0 {
		t.Fatal("stepped down over the lease it is taking over")
	}
	if h := leaseHolder(t, kc); h != "me" {
		t.Fatalf("lease holder %q, want me", h)
	}

	c2, ns2, _ := newActive(t, "old", time.Second)
	c2.tick(context.Background())
	if c2.Active() || ns2.downs.Load() != 1 {
		t.Fatal("kept the VIP with the lease held by another gateway and no handoff pending")
	}
}
