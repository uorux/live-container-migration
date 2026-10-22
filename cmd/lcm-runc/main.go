// lcm-runc is the runc binary for the `lcm` containerd runtime handler. It passes every call
// through to the real runc, except:
//
//   - create of a workload container whose pod has a staged checkpoint (see internal/stage):
//     the writable layer is replayed onto the new rootfs and the container is brought up with
//     `runc restore --detach` instead, so containerd/kubelet see an ordinary created container
//     that happens to already be running the migrated process tree;
//   - start of such a restored container: a no-op, it is already running;
//   - create of any container in a pod fenced on this node (migrated away): refused, so kubelet
//     can never restart the source after its final dump.
package main

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"lcm/internal/api"
	"lcm/internal/netaddr"
	"lcm/internal/relayctl"
	"lcm/internal/rootfsdiff"
	"lcm/internal/stage"
)

// runc global flags that take a value.
var globalValueFlags = map[string]bool{"--log": true, "--log-format": true, "--root": true, "--rootless": true, "--criu": true}

// `runc create` flags that take a value.
var createValueFlags = map[string]bool{"--bundle": true, "-b": true, "--console-socket": true, "--pid-file": true, "--preserve-fds": true, "--pidfd-socket": true}

type invocation struct {
	global []string // global flags, verbatim
	cmd    string
	args   []string // everything after the subcommand, verbatim
}

func parse(argv []string) invocation {
	var inv invocation
	i := 0
	for ; i < len(argv); i++ {
		a := argv[i]
		if !strings.HasPrefix(a, "-") {
			break
		}
		inv.global = append(inv.global, a)
		if globalValueFlags[a] && i+1 < len(argv) {
			i++
			inv.global = append(inv.global, argv[i])
		}
	}
	if i < len(argv) {
		inv.cmd = argv[i]
		inv.args = argv[i+1:]
	}
	return inv
}

type createArgs struct {
	bundle, pidFile, consoleSocket, pidfdSocket, id string
	noPivot, noNewKeyring                           bool
	preserveFDs                                     string
}

func parseCreate(args []string) createArgs {
	c := createArgs{bundle: "."}
	for i := 0; i < len(args); i++ {
		a := args[i]
		name, val, hasVal := strings.Cut(a, "=")
		if createValueFlags[name] {
			if !hasVal && i+1 < len(args) {
				i++
				val = args[i]
			}
			switch name {
			case "--bundle", "-b":
				c.bundle = val
			case "--pid-file":
				c.pidFile = val
			case "--console-socket":
				c.consoleSocket = val
			case "--pidfd-socket":
				c.pidfdSocket = val
			case "--preserve-fds":
				c.preserveFDs = val
			}
			continue
		}
		switch a {
		case "--no-pivot":
			c.noPivot = true
		case "--no-new-keyring":
			c.noNewKeyring = true
		default:
			if !strings.HasPrefix(a, "-") {
				c.id = a
			}
		}
	}
	return c
}

func main() {
	inv := parse(os.Args[1:])
	logf("argv=%q", os.Args[1:])
	switch inv.cmd {
	case "create":
		create(inv)
	case "start":
		if id := lastPositional(inv.args); id != "" {
			if _, err := os.Stat(stage.RestoredMarker(id)); err == nil {
				logf("start %s: restored container already running, no-op", id)
				os.Exit(0)
			}
		}
	case "delete":
		if id := lastPositional(inv.args); id != "" {
			os.Remove(stage.RestoredMarker(id))
		}
	}
	execRunc(os.Args[1:])
}

func create(inv invocation) {
	c := parseCreate(inv.args)
	spec, err := readSpec(c.bundle)
	if err != nil {
		logf("create %s: reading spec: %v (passing through)", c.id, err)
		execRunc(os.Args[1:])
	}
	ann := spec.Annotations
	if ann["io.kubernetes.cri.container-type"] != "container" {
		execRunc(os.Args[1:]) // sandbox (pause) containers are never touched
	}
	ns, pod, ctr := ann["io.kubernetes.cri.sandbox-namespace"], ann["io.kubernetes.cri.sandbox-name"], ann["io.kubernetes.cri.container-name"]
	if stage.IsFenced(ns, pod) {
		msg := fmt.Sprintf("lcm: pod %s/%s was live-migrated away from this node; refusing to start container %s", ns, pod, ctr)
		logf("%s", msg)
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(1)
	}
	key := stage.Key{Namespace: stage.Sanitize(ns), Pod: stage.Sanitize(pod), Container: stage.Sanitize(ctr)}
	if !stage.IsReady(key) {
		execRunc(os.Args[1:])
	}
	os.Exit(restore(inv, c, spec, key))
}

func restore(inv invocation, c createArgs, spec *ociSpec, key stage.Key) int {
	res := api.RestoreResult{ContainerID: c.id, Started: time.Now()}
	defer func() {
		res.Finished = time.Now()
		// Keep CRIU's log: the staging dir lives in RAM and is dropped after the migration.
		if b, err := os.ReadFile(filepath.Join(stage.WorkDir(key), "restore.log")); err == nil {
			dir := filepath.Join(stage.Root(), "logs", key.String())
			os.MkdirAll(dir, 0o755)
			os.WriteFile(filepath.Join(dir, "restore.log"), b, 0o644)
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		os.WriteFile(stage.ResultFile(key), b, 0o644)
	}()
	fail := func(err error) int {
		res.Error, res.ExitCode = err.Error(), 1
		logf("restore %s (%s): %v", c.id, key, err)
		fmt.Fprintf(os.Stderr, "lcm: restore %s: %v\n", key, err)
		return 1
	}

	// Fault injection for testing rollback: the next N restores on this node fail (N is the
	// file's content, default 1).
	if injectFailure() {
		return fail(fmt.Errorf("injected failure (FAIL_NEXT_RESTORE)"))
	}

	rootfs := spec.Root.Path
	if !filepath.IsAbs(rootfs) {
		rootfs = filepath.Join(c.bundle, rootfs)
	}
	t0 := time.Now()
	if f, err := os.Open(stage.RootfsDiff(key)); err == nil {
		err = rootfsdiff.Apply(f, rootfs)
		f.Close()
		if err != nil {
			return fail(fmt.Errorf("applying rootfs diff: %w", err))
		}
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	res.RootfsMS = time.Since(t0).Milliseconds()

	if err := os.MkdirAll(stage.WorkDir(key), 0o700); err != nil {
		return fail(err)
	}
	args := append([]string{}, inv.global...)
	args = append(args, "restore", "--detach",
		"--bundle", c.bundle,
		"--image-path", stage.ImagesDir(key),
		"--work-path", stage.WorkDir(key),
		"--tcp-established", "--file-locks",
		"--manage-cgroups-mode", "soft",
	)
	if c.pidFile != "" {
		args = append(args, "--pid-file", c.pidFile)
	}
	if c.consoleSocket != "" {
		args = append(args, "--console-socket", c.consoleSocket)
	}
	if c.noPivot {
		args = append(args, "--no-pivot")
	}
	args = append(args, c.id)

	// Old pod addresses as aliases on lo, so CRIU can rebuild sockets bound to them.
	netns := spec.netns()
	var aliases []netip.Addr
	if addrs, err := netaddr.Read(filepath.Join(stage.RestoreDir(key), "source-addrs")); err == nil && netns != "" {
		if aliases, err = netaddr.AddAliases(netns, addrs); err != nil {
			return fail(fmt.Errorf("adding source address aliases: %w", err))
		}
		logf("restore %s: source addrs %v, aliased %v on lo in %s", c.id, addrs, aliases, netns)
	} else {
		logf("restore %s: no source addrs (err=%v netns=%q)", c.id, err, netns)
	}

	t1 := time.Now()
	cmd := exec.Command(realRunc(), args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	err := cmd.Run()
	res.RestoreMS = time.Since(t1).Milliseconds()
	if len(aliases) > 0 {
		// Connections to the outside were bound to the old pod IP: their peers can't reach us
		// any more. Reset them now so the application reconnects instead of hanging.
		if kerr := netaddr.KillAndRemove(netns, aliases); kerr != nil {
			logf("restore %s: cleaning up old addresses: %v", c.id, kerr)
		}
	}
	if err != nil {
		return fail(fmt.Errorf("runc restore: %w (CRIU log: %s/restore.log)", err, stage.WorkDir(key)))
	}
	// The relay was quiesced before the dump; let it reconnect to the gateway now. (It also
	// auto-resumes after a timeout, so a failure here only costs time.)
	if pid := readPid(c.pidFile); pid > 0 {
		if err := relayctl.Command(pid, "resume", 3*time.Second); err == nil {
			res.RelayResume = "ok"
		} else if !os.IsNotExist(err) {
			res.RelayResume = err.Error()
		}
	}
	if err := os.MkdirAll(filepath.Dir(stage.RestoredMarker(c.id)), 0o755); err == nil {
		os.WriteFile(stage.RestoredMarker(c.id), []byte(key.String()), 0o644)
	}
	// Consume the staged restore so a later container restart starts fresh instead of re-restoring
	// a stale image; keep the images around for debugging until the agent cleans up.
	os.Rename(stage.ReadyFile(key), stage.ReadyFile(key)+".consumed")
	logf("restore %s (%s): ok rootfs=%dms restore=%dms", c.id, key, res.RootfsMS, res.RestoreMS)
	return 0
}

func injectFailure() bool {
	f := filepath.Join(stage.Root(), "FAIL_NEXT_RESTORE")
	b, err := os.ReadFile(f)
	if err != nil {
		return false
	}
	n := 1
	fmt.Sscan(strings.TrimSpace(string(b)), &n)
	if n <= 1 {
		os.Remove(f)
	} else {
		os.WriteFile(f, []byte(fmt.Sprint(n-1)), 0o644)
	}
	return true
}

func readPid(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var pid int
	fmt.Sscan(strings.TrimSpace(string(b)), &pid)
	return pid
}

type ociSpec struct {
	Root struct {
		Path string `json:"path"`
	} `json:"root"`
	Annotations map[string]string `json:"annotations"`
	Linux       struct {
		Namespaces []struct {
			Type string `json:"type"`
			Path string `json:"path"`
		} `json:"namespaces"`
	} `json:"linux"`
}

// netns is the path of the (pod sandbox's) network namespace the container joins.
func (s *ociSpec) netns() string {
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == "network" {
			return ns.Path
		}
	}
	return ""
}

func readSpec(bundle string) (*ociSpec, error) {
	b, err := os.ReadFile(filepath.Join(bundle, "config.json"))
	if err != nil {
		return nil, err
	}
	var s ociSpec
	return &s, json.Unmarshal(b, &s)
}

func lastPositional(args []string) string {
	for i := len(args) - 1; i >= 0; i-- {
		if !strings.HasPrefix(args[i], "-") {
			return args[i]
		}
	}
	return ""
}

// realRunc finds the real runc: $LCM_REAL_RUNC, else the first `runc` on PATH that isn't us.
func realRunc() string {
	if p := os.Getenv("LCM_REAL_RUNC"); p != "" {
		return p
	}
	self, _ := os.Executable()
	self, _ = filepath.EvalSymlinks(self)
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		p := filepath.Join(dir, "runc")
		if r, err := filepath.EvalSymlinks(p); err == nil && r != self {
			if fi, err := os.Stat(r); err == nil && fi.Mode()&0o111 != 0 {
				return p
			}
		}
	}
	return "runc"
}

func execRunc(args []string) {
	bin := realRunc()
	err := syscall.Exec(bin, append([]string{"runc"}, args...), os.Environ())
	fmt.Fprintf(os.Stderr, "lcm-runc: exec %s: %v\n", bin, err)
	os.Exit(127)
}

func logf(format string, a ...any) {
	f, err := os.OpenFile(filepath.Join(stage.Root(), "lcm-runc.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s [%d] %s\n", time.Now().Format(time.RFC3339Nano), os.Getpid(), fmt.Sprintf(format, a...))
}
