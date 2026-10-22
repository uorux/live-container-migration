// Package runc drives the host's runc for containers managed by containerd's k8s.io namespace.
package runc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Root is containerd's runc state root for the CRI (k8s.io) namespace.
const Root = "/run/containerd/runc/k8s.io"

// Binary returns the runc containerd's shims use. $LCM_RUNC wins; otherwise the shim's own PATH
// is consulted (on NixOS it is k3s's service PATH, which our PATH may not share).
func Binary() string {
	if p := os.Getenv("LCM_RUNC"); p != "" {
		return p
	}
	if path := shimPATH(); path != "" {
		for _, dir := range filepath.SplitList(path) {
			p := filepath.Join(dir, "runc")
			if fi, err := os.Stat(p); err == nil && fi.Mode()&0o111 != 0 {
				return p
			}
		}
	}
	if p, err := exec.LookPath("runc"); err == nil {
		return p
	}
	return "runc"
}

func shimPATH() string {
	procs, _ := filepath.Glob("/proc/[0-9]*/cmdline")
	for _, c := range procs {
		b, err := os.ReadFile(c)
		if err != nil || !bytes.Contains(b, []byte("containerd-shim-runc-v2")) {
			continue
		}
		env, err := os.ReadFile(filepath.Join(filepath.Dir(c), "environ"))
		if err != nil {
			continue
		}
		for _, kv := range bytes.Split(env, []byte{0}) {
			if v, ok := bytes.CutPrefix(kv, []byte("PATH=")); ok {
				return string(v)
			}
		}
	}
	return ""
}

// State is the subset of `runc state` we use.
type State struct {
	ID     string `json:"id"`
	Pid    int    `json:"pid"`
	Status string `json:"status"`
	Bundle string `json:"bundle"`
	Rootfs string `json:"rootfs"`
}

func Run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, Binary(), append([]string{"--root", Root}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("runc %s: %w: %s", strings.Join(args, " "), err, lastLines(stderr.String(), 6))
	}
	return out, nil
}

func GetState(ctx context.Context, id string) (*State, error) {
	out, err := Run(ctx, "state", id)
	if err != nil {
		return nil, err
	}
	var s State
	return &s, json.Unmarshal(out, &s)
}

// CheckpointOpts maps to `runc checkpoint` flags.
type CheckpointOpts struct {
	ImagePath, WorkPath, ParentPath string
	PreDump, LeaveRunning           bool
}

func Checkpoint(ctx context.Context, id string, o CheckpointOpts) error {
	args := []string{"checkpoint", "--image-path", o.ImagePath, "--work-path", o.WorkPath}
	if o.ParentPath != "" {
		args = append(args, "--parent-path", o.ParentPath)
	}
	if o.PreDump {
		args = append(args, "--pre-dump")
	} else {
		args = append(args, "--tcp-established", "--file-locks")
	}
	if o.LeaveRunning {
		args = append(args, "--leave-running")
	}
	_, err := Run(ctx, append(args, id)...)
	return err
}

// Upperdir returns the overlayfs upper directory backing a container's rootfs mount.
func Upperdir(rootfs string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 || fields[4] != rootfs {
			continue
		}
		for _, opt := range strings.Split(fields[len(fields)-1], ",") {
			if v, ok := strings.CutPrefix(opt, "upperdir="); ok {
				return v, nil
			}
		}
	}
	return "", fmt.Errorf("no overlay mount with upperdir at %s", rootfs)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}
