package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func reqPod(name, node, cpu, mem string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)}}}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func TestCheckRequests(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "munificent"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("12"), corev1.ResourceMemory: resource.MustParse("46Gi")}}}
	kc := fake.NewSimpleClientset(node,
		reqPod("ceph", "munificent", "10235m", "20Gi", corev1.PodRunning),
		reqPod("done", "munificent", "4", "10Gi", corev1.PodSucceeded), // finished pods don't count
		reqPod("elsewhere", "carrack", "8", "8Gi", corev1.PodRunning))
	ctx := context.Background()

	// The case that bit us: 2 CPUs requested, 1765m unrequested.
	short, err := checkRequests(ctx, kc, reqPod("bettermc", "carrack", "2", "12Gi", corev1.PodRunning), "munificent")
	if err != nil || !strings.Contains(short, "CPU: the pod requests 2, munificent has 1765m unrequested") {
		t.Errorf("shortfall %q, err %v", short, err)
	}
	if strings.Contains(short, "memory") {
		t.Errorf("memory fits: %q", short)
	}
	short, _ = checkRequests(ctx, kc, reqPod("big", "carrack", "500m", "30Gi", corev1.PodRunning), "munificent")
	if !strings.Contains(short, "memory: the pod requests 30.00G") {
		t.Errorf("memory shortfall %q", short)
	}
	if short, err := checkRequests(ctx, kc, reqPod("bettermc", "carrack", "500m", "12Gi", corev1.PodRunning), "munificent"); short != "" || err != nil {
		t.Errorf("fits, got %q %v", short, err)
	}
	if _, err := checkRequests(ctx, kc, reqPod("x", "carrack", "1", "1Gi", corev1.PodRunning), "nosuchnode"); err == nil {
		t.Error("missing node: no error")
	}
}

func TestPodRequestsInitAndOverhead(t *testing.T) {
	p := reqPod("p", "n", "500m", "1Gi", corev1.PodRunning)
	p.Spec.InitContainers = []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("512Mi")}}}}
	p.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
	cpu, mem := podRequests(p)
	if cpu.MilliValue() != 2100 || mem.Value() != 1<<30 {
		t.Errorf("cpu %s mem %s", cpu.String(), mem.String())
	}
}

func TestRefused(t *testing.T) {
	p := reqPod("p", "n", "1", "1Gi", corev1.PodFailed)
	p.Status.Reason, p.Status.Message = "OutOfcpu", "Node didn't have enough resource: cpu"
	if err := refused(p); err == nil || !strings.Contains(err.Error(), "OutOfcpu") {
		t.Errorf("refused: %v", err)
	}
	if err := refused(reqPod("p", "n", "1", "1Gi", corev1.PodPending)); err != nil {
		t.Errorf("pending: %v", err)
	}
}
