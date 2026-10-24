package controller

import (
	"strings"
	"testing"

	"lcm/internal/api"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNextName(t *testing.T) {
	for in, want := range map[string]string{
		"mc":       "mc-m1",
		"mc-m1":    "mc-m2",
		"mc-m9":    "mc-m10",
		"mc-m41":   "mc-m42",
		"mc-m1-m2": "mc-m1-m3",
		"mc-mx":    "mc-mx-m1",
		"m1":       "m1-m1",
		"a-m":      "a-m-m1",
	} {
		if got := nextName(in); got != want {
			t.Errorf("nextName(%q) = %q, want %q", in, got, want)
		}
	}
}

func basePod() *corev1.Pod {
	rc := RuntimeClass
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mc", Namespace: "games",
			Labels: map[string]string{"app": "mc"},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
				"team": "jacob",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "arquitens",
			RuntimeClassName: &rc,
			InitContainers: []corev1.Container{
				{Name: "fetch-relay", Image: "busybox"},
			},
			Containers: []corev1.Container{{Name: "paper", Image: "paper:1"}},
			Volumes: []corev1.Volume{{Name: "world", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "world"}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestDestinationPodFirstHop(t *testing.T) {
	src := basePod()
	res := &Result{MigrationID: "mig-1", DestPod: nextName(src.Name), DestNode: "carrack"}
	key := "games_mc-m1_paper"
	dst := destinationPod(src, res, "busybox:1.37", key, 7790)

	if dst.Name != "mc-m1" || dst.Namespace != "games" {
		t.Errorf("name %s/%s", dst.Namespace, dst.Name)
	}
	if dst.Spec.NodeName != "carrack" {
		t.Errorf("node %q", dst.Spec.NodeName)
	}
	if dst.Spec.Hostname != "mc" {
		t.Errorf("hostname %q, want the source pod's name", dst.Spec.Hostname)
	}
	if dst.Labels["app"] != "mc" || dst.Labels[LabelFrom] != "mc" {
		t.Errorf("labels %v", dst.Labels)
	}
	if _, ok := dst.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Errorf("last-applied-configuration copied")
	}
	if dst.Annotations["team"] != "jacob" || dst.Annotations[AnnMigration] != "mig-1" {
		t.Errorf("annotations %v", dst.Annotations)
	}
	if dst.ResourceVersion != "" || dst.UID != "" {
		t.Errorf("server-set metadata copied: rv=%q uid=%q", dst.ResourceVersion, dst.UID)
	}
	if len(dst.Spec.InitContainers) != 2 || dst.Spec.InitContainers[0].Name != "fetch-relay" ||
		dst.Spec.InitContainers[1].Name != WaitInitName {
		t.Fatalf("init containers %v", names(dst.Spec.InitContainers))
	}
	w := dst.Spec.InitContainers[1]
	var waitURL string
	for _, e := range w.Env {
		if e.Name == "WAIT_URL" {
			waitURL = e.Value
		}
	}
	if !strings.Contains(waitURL, ":7790/v1/stage/"+key+"/wait") || !strings.HasPrefix(waitURL, "http://$(HOST_IP):") {
		t.Errorf("wait URL %q", waitURL)
	}
	// Source must be untouched (destinationPod deep-copies).
	if src.Spec.NodeName != "arquitens" || src.Spec.Hostname != "" || len(src.Spec.InitContainers) != 1 ||
		src.Labels[LabelFrom] != "" || src.Annotations[AnnMigration] != "" {
		t.Errorf("source pod mutated: %+v", src)
	}
}

func TestDestinationPodSecondHop(t *testing.T) {
	src := basePod()
	hop1 := destinationPod(src, &Result{MigrationID: "mig-1", DestPod: "mc-m1", DestNode: "carrack"}, "busybox", "games_mc-m1_paper", 7790)
	// What the API server would hand back for the running mc-m1.
	hop1.Spec.NodeName = "carrack"
	hop1.Status.Phase = corev1.PodRunning
	initBefore := append([]corev1.Container(nil), hop1.Spec.InitContainers...)

	hop2 := destinationPod(hop1, &Result{MigrationID: "mig-2", DestPod: nextName(hop1.Name), DestNode: "munificent"}, "busybox", "games_mc-m2_paper", 7790)
	if hop2.Name != "mc-m2" {
		t.Errorf("name %q", hop2.Name)
	}
	if hop2.Spec.Hostname != "mc" {
		t.Errorf("hostname %q, want the original pod's (mc)", hop2.Spec.Hostname)
	}
	var waits int
	for _, c := range hop2.Spec.InitContainers {
		if c.Name == WaitInitName {
			waits++
			for _, e := range c.Env {
				if e.Name == "WAIT_URL" && !strings.Contains(e.Value, "games_mc-m2_paper") {
					t.Errorf("wait container still points at the previous hop: %s", e.Value)
				}
			}
		}
	}
	if waits != 1 || len(hop2.Spec.InitContainers) != 2 || hop2.Spec.InitContainers[1].Name != WaitInitName {
		t.Errorf("init containers %v, want [fetch-relay %s]", names(hop2.Spec.InitContainers), WaitInitName)
	}
	if hop2.Labels[LabelFrom] != "mc-m1" || hop2.Annotations[AnnMigration] != "mig-2" {
		t.Errorf("labels %v annotations %v", hop2.Labels, hop2.Annotations)
	}
	// The previous hop's spec must not be modified (the in-place filter of InitContainers runs
	// on a deep copy).
	if len(hop1.Spec.InitContainers) != len(initBefore) {
		t.Fatalf("previous hop's init containers changed length")
	}
	for i := range initBefore {
		if hop1.Spec.InitContainers[i].Name != initBefore[i].Name {
			t.Errorf("previous hop's init containers mutated: %v", names(hop1.Spec.InitContainers))
		}
	}
}

func TestDestinationPodKeepsExplicitHostname(t *testing.T) {
	src := basePod()
	src.Spec.Hostname = "survival"
	dst := destinationPod(src, &Result{DestPod: "mc-m1", DestNode: "carrack"}, "busybox", "k", 7790)
	if dst.Spec.Hostname != "survival" {
		t.Errorf("hostname %q", dst.Spec.Hostname)
	}
}

func names(cs []corev1.Container) []string {
	var n []string
	for _, c := range cs {
		n = append(n, c.Name)
	}
	return n
}

func TestCheckMigratable(t *testing.T) {
	yes := true
	other := "runc"
	cases := []struct {
		name   string
		mutate func(*corev1.Pod)
		errSub string
	}{
		{"ok", func(*corev1.Pod) {}, ""},
		{"no runtime class", func(p *corev1.Pod) { p.Spec.RuntimeClassName = nil }, "runtimeClassName"},
		{"other runtime class", func(p *corev1.Pod) { p.Spec.RuntimeClassName = &other }, "runtimeClassName"},
		{"pending", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }, "not Running"},
		{"controlled", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "rs", Controller: &yes}}
		}, "controlled by ReplicaSet"},
		{"non-controller owner is fine", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "ConfigMap", Name: "cm"}}
		}, ""},
		{"hostPath", func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "host", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/srv"}}})
		}, "hostPath"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := basePod()
			c.mutate(p)
			err := checkMigratable(p)
			switch {
			case c.errSub == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case c.errSub != "" && (err == nil || !strings.Contains(err.Error(), c.errSub)):
				t.Errorf("error %v, want one mentioning %q", err, c.errSub)
			}
		})
	}
}

func TestCheckRestore(t *testing.T) {
	startError := &corev1.ContainerStateTerminated{Reason: "StartError", ExitCode: 128,
		Message: "failed to create containerd task: ... lcm: the final checkpoint for games_mc-m1_paper was not staged within 1m40s"}
	waiting := func(reason string) corev1.ContainerState {
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
	}
	var (
		none    *api.RestoreResult
		failed  = &api.RestoreResult{ExitCode: 1, Error: "runc restore: exit status 1"}
		success = &api.RestoreResult{}
	)
	cases := []struct {
		name   string
		state  corev1.ContainerState
		last   *corev1.ContainerStateTerminated
		policy corev1.RestartPolicy
		result *api.RestoreResult
		want   restoreState
		why    bool // pending with a reason (waiting out kubelet's back-off)
	}{
		{name: "creating", state: waiting("ContainerCreating"), result: none, want: restorePending},
		{name: "running", state: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, last: startError, want: restoreRunning},
		// An attempt gave up before READY; kubelet is backing off, or its retry is under way.
		{name: "back-off after not staged", state: waiting("CrashLoopBackOff"), last: startError, result: none, want: restorePending, why: true},
		{name: "retry under way", state: waiting("ContainerCreating"), last: startError, result: none, want: restorePending, why: true},
		{name: "first start failed", state: waiting("RunContainerError"), result: none, want: restorePending, why: true},
		{name: "retry restored, not reported yet", state: waiting("ContainerCreating"), last: startError, result: success, want: restorePending, why: true},
		// A restore was attempted and failed: roll back now.
		{name: "restore failed", state: waiting("CrashLoopBackOff"), last: startError, result: failed, want: restoreFailed},
		{name: "first restore failed", state: waiting("RunContainerError"), result: failed, want: restoreFailed},
		// Failures no retry of lcm-runc makes good stay final at once.
		{name: "restored process exited", state: waiting("CrashLoopBackOff"),
			last: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}, result: success, want: restoreFailed},
		{name: "config error", state: waiting("CreateContainerConfigError"), result: none, want: restoreFailed},
		{name: "image", state: waiting("ImagePullBackOff"), result: none, want: restoreFailed},
		{name: "never restarted", state: corev1.ContainerState{Terminated: startError}, last: startError,
			policy: corev1.RestartPolicyNever, result: none, want: restoreFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := corev1.ContainerStatus{Name: "paper", State: c.state}
			if c.last != nil {
				s.LastTerminationState.Terminated = c.last
			}
			p := &corev1.Pod{
				Spec: corev1.PodSpec{RestartPolicy: c.policy},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
					{Name: "relay-sidecar", State: waiting("CrashLoopBackOff")}, s}},
			}
			got, why := checkRestore(p, "paper", func() *api.RestoreResult { return c.result })
			if got != c.want {
				t.Fatalf("state %d (%v), want %d", got, why, c.want)
			}
			if c.want == restoreFailed && why == nil {
				t.Fatal("failed without a reason")
			}
			if c.want == restorePending && (why != nil) != c.why {
				t.Fatalf("pending with reason %v, want one: %v", why, c.why)
			}
		})
	}
}
