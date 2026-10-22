package main

import (
	"os"
	"path/filepath"

	"lcm/internal/stage"

	"reflect"
	"strings"
	"testing"
)

// Global flags as go-runc (used by containerd-shim-runc-v2) renders them: --root R, --log L,
// --log-format json, --systemd-cgroup, --rootless=bool, optionally --debug.
var shimGlobal = []string{
	"--root", "/run/containerd/runc/k8s.io",
	"--log", "/run/containerd/io.containerd.runtime.v2.task/k8s.io/abc123/log.json",
	"--log-format", "json",
	"--systemd-cgroup",
}

func TestParseSubcommands(t *testing.T) {
	const id = "abc123"
	cases := []struct {
		name   string
		argv   []string
		global []string
		cmd    string
		args   []string
	}{
		{"create", append(append([]string{}, shimGlobal...), "create", "--bundle", "/b", "--pid-file", "/b/init.pid", id),
			shimGlobal, "create", []string{"--bundle", "/b", "--pid-file", "/b/init.pid", id}},
		{"start", append(append([]string{}, shimGlobal...), "start", id), shimGlobal, "start", []string{id}},
		{"delete-force", append(append([]string{}, shimGlobal...), "delete", "--force", id), shimGlobal, "delete", []string{"--force", id}},
		{"kill-all", append(append([]string{}, shimGlobal...), "kill", "--all", id, "9"), shimGlobal, "kill", []string{"--all", id, "9"}},
		{"state", append(append([]string{}, shimGlobal...), "state", id), shimGlobal, "state", []string{id}},
		{"exec", append(append([]string{}, shimGlobal...), "exec", "--process", "/tmp/p.json", "--detach", "--pid-file", "/tmp/e.pid", id),
			shimGlobal, "exec", []string{"--process", "/tmp/p.json", "--detach", "--pid-file", "/tmp/e.pid", id}},
		{"update", append(append([]string{}, shimGlobal...), "update", "--resources", "-", id), shimGlobal, "update", []string{"--resources", "-", id}},
		{"pause", append(append([]string{}, shimGlobal...), "pause", id), shimGlobal, "pause", []string{id}},
		{"resume", append(append([]string{}, shimGlobal...), "resume", id), shimGlobal, "resume", []string{id}},
		{"ps", append(append([]string{}, shimGlobal...), "ps", "--format", "json", id), shimGlobal, "ps", []string{"--format", "json", id}},
		{"features", []string{"features"}, nil, "features", []string{}},
		{"rootless-equals", []string{"--root", "/r", "--rootless=false", "--debug", "state", id},
			[]string{"--root", "/r", "--rootless=false", "--debug"}, "state", []string{id}},
		{"rootless-space", []string{"--rootless", "false", "state", id}, []string{"--rootless", "false"}, "state", []string{id}},
		{"equals-form", []string{"--root=/r", "--log=/l", "--log-format=json", "create", "--bundle=/b", id},
			[]string{"--root=/r", "--log=/l", "--log-format=json"}, "create", []string{"--bundle=/b", id}},
		{"empty", nil, nil, "", nil},
		{"only-flags", []string{"--debug"}, []string{"--debug"}, "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inv := parse(c.argv)
			if !reflect.DeepEqual(inv.global, c.global) {
				t.Errorf("global = %q, want %q", inv.global, c.global)
			}
			if inv.cmd != c.cmd {
				t.Errorf("cmd = %q, want %q", inv.cmd, c.cmd)
			}
			if len(inv.args) != len(c.args) || (len(c.args) > 0 && !reflect.DeepEqual(inv.args, c.args)) {
				t.Errorf("args = %q, want %q", inv.args, c.args)
			}
		})
	}
}

func TestParseCreate(t *testing.T) {
	cases := []struct {
		name string
		args string
		want createArgs
	}{
		{"go-runc", "--bundle /run/b --pid-file /run/b/init.pid abc",
			createArgs{bundle: "/run/b", pidFile: "/run/b/init.pid", id: "abc"}},
		{"tty", "--bundle /run/b --pid-file /run/b/init.pid --console-socket /tmp/pty.sock abc",
			createArgs{bundle: "/run/b", pidFile: "/run/b/init.pid", consoleSocket: "/tmp/pty.sock", id: "abc"}},
		{"all-flags", "--bundle /b --pid-file /p --console-socket /c --no-pivot --no-new-keyring --preserve-fds 2 --pidfd-socket /s abc",
			createArgs{bundle: "/b", pidFile: "/p", consoleSocket: "/c", pidfdSocket: "/s", noPivot: true, noNewKeyring: true, preserveFDs: "2", id: "abc"}},
		{"equals", "--bundle=/b --pid-file=/p --console-socket=/c --preserve-fds=1 --pidfd-socket=/s abc",
			createArgs{bundle: "/b", pidFile: "/p", consoleSocket: "/c", preserveFDs: "1", pidfdSocket: "/s", id: "abc"}},
		{"short-bundle", "-b /b abc", createArgs{bundle: "/b", id: "abc"}},
		{"id-first", "abc --bundle /b", createArgs{bundle: "/b", id: "abc"}},
		{"default-bundle", "abc", createArgs{bundle: ".", id: "abc"}},
		{"value-looks-like-flag-free", "--pid-file /p --bundle /b abc", createArgs{bundle: "/b", pidFile: "/p", id: "abc"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseCreate(strings.Fields(c.args))
			if got != c.want {
				t.Errorf("parseCreate(%q)\n got %+v\nwant %+v", c.args, got, c.want)
			}
		})
	}
}

// The full path containerd takes: global flags, then create's own.
func TestParseThenCreate(t *testing.T) {
	argv := append(append([]string{}, shimGlobal...), "create", "--bundle", "/run/containerd/io.containerd.runtime.v2.task/k8s.io/abc",
		"--pid-file", "/run/containerd/io.containerd.runtime.v2.task/k8s.io/abc/init.pid", "abc")
	inv := parse(argv)
	c := parseCreate(inv.args)
	if inv.cmd != "create" || c.id != "abc" || c.bundle != "/run/containerd/io.containerd.runtime.v2.task/k8s.io/abc" ||
		!strings.HasSuffix(c.pidFile, "/init.pid") {
		t.Errorf("inv=%+v create=%+v", inv, c)
	}
}

func TestLastPositional(t *testing.T) {
	for args, want := range map[string]string{
		"abc":         "abc",
		"--force abc": "abc",
		"--force":     "",
		"":            "",
	} {
		if got := lastPositional(strings.Fields(args)); got != want {
			t.Errorf("lastPositional(%q) = %q, want %q", args, got, want)
		}
	}
}

func TestOCISpecNetns(t *testing.T) {
	var s ociSpec
	s.Linux.Namespaces = append(s.Linux.Namespaces,
		struct {
			Type string `json:"type"`
			Path string `json:"path"`
		}{"pid", ""},
		struct {
			Type string `json:"type"`
			Path string `json:"path"`
		}{"network", "/var/run/netns/cni-1"})
	if got := s.netns(); got != "/var/run/netns/cni-1" {
		t.Errorf("netns = %q", got)
	}
}

func TestInstallRelay(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LCM_STATE_DIR", root)
	key := stage.Key{Namespace: "ns", Pod: "mc-m1", Container: "mc"}
	if err := os.MkdirAll(stage.RestoreDir(key), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(stage.RestoreDir(key), stage.RelayExeName), []byte("old-relay-bytes"), 0o755)
	os.WriteFile(filepath.Join(stage.RestoreDir(key), stage.RelayPathName), []byte("/lcm/lcm-relay"), 0o644)
	emptyDir := t.TempDir()
	os.WriteFile(filepath.Join(emptyDir, "lcm-relay"), []byte("new-relay-from-this-node"), 0o755)
	spec := &ociSpec{}
	spec.Mounts = append(spec.Mounts,
		struct {
			Destination string `json:"destination"`
			Source      string `json:"source"`
		}{"/data", t.TempDir()},
		struct {
			Destination string `json:"destination"`
			Source      string `json:"source"`
		}{"/lcm", emptyDir})
	if err := installRelay(spec, key); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(emptyDir, "lcm-relay"))
	if string(got) != "old-relay-bytes" {
		t.Fatalf("relay = %q, want the source's binary", got)
	}
	// Nothing staged: no-op.
	os.Remove(filepath.Join(stage.RestoreDir(key), stage.RelayPathName))
	if err := installRelay(spec, key); err != nil {
		t.Fatal(err)
	}
}
