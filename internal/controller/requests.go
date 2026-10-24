package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// checkRequests refuses a migration kubelet would refuse. The destination pod is bound with
// spec.nodeName, so the scheduler never sees it, and kubelet's admission rejects it (OutOfcpu,
// OutOfmemory) when the node's allocatable minus its pods' requests can't fit the pod's.
// Requests are bookkeeping, not usage: checkMemory covers the real RAM.
// It returns the shortfall ("" when the pod fits) and an error when it couldn't look.
func checkRequests(ctx context.Context, kc kubernetes.Interface, pod *corev1.Pod, node string) (string, error) {
	n, err := kc.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("reading node %s: %w", node, err)
	}
	pods, err := kc.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return "", fmt.Errorf("listing pods on %s: %w", node, err)
	}
	var usedCPU, usedMem resource.Quantity
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != node || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		c, m := podRequests(p)
		usedCPU.Add(c)
		usedMem.Add(m)
	}
	needCPU, needMem := podRequests(pod)
	freeCPU, freeMem := n.Status.Allocatable[corev1.ResourceCPU], n.Status.Allocatable[corev1.ResourceMemory]
	freeCPU.Sub(usedCPU)
	freeMem.Sub(usedMem)
	var short []string
	if needCPU.Cmp(freeCPU) > 0 {
		short = append(short, fmt.Sprintf("CPU: the pod requests %s, %s has %s unrequested", needCPU.String(), node, freeCPU.String()))
	}
	if needMem.Cmp(freeMem) > 0 {
		short = append(short, fmt.Sprintf("memory: the pod requests %s, %s has %s unrequested", human(needMem.Value()), node, human(freeMem.Value())))
	}
	if len(short) > 0 {
		return fmt.Sprintf("kubelet on %s would refuse the destination pod (%s); lower the pod's requests or free some on the node",
			node, strings.Join(short, "; ")), nil
	}
	return "", nil
}

// podRequests is what kubelet's admission charges a pod: the larger of its containers' summed
// requests and its biggest init container's, plus the runtime class overhead.
func podRequests(p *corev1.Pod) (cpu, mem resource.Quantity) {
	for _, c := range p.Spec.Containers {
		cpu.Add(c.Resources.Requests[corev1.ResourceCPU])
		mem.Add(c.Resources.Requests[corev1.ResourceMemory])
	}
	for _, c := range p.Spec.InitContainers {
		if q := c.Resources.Requests[corev1.ResourceCPU]; q.Cmp(cpu) > 0 {
			cpu = q.DeepCopy()
		}
		if q := c.Resources.Requests[corev1.ResourceMemory]; q.Cmp(mem) > 0 {
			mem = q.DeepCopy()
		}
	}
	cpu.Add(p.Spec.Overhead[corev1.ResourceCPU])
	mem.Add(p.Spec.Overhead[corev1.ResourceMemory])
	return cpu, mem
}

// refused reports a pod kubelet's admission rejected (it never starts; waiting is pointless).
func refused(p *corev1.Pod) error {
	if p.Status.Phase == corev1.PodFailed {
		return fmt.Errorf("kubelet refused the pod: %s: %s", p.Status.Reason, p.Status.Message)
	}
	return nil
}
