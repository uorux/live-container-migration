package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A runc container's mounts (trimmed): its masked and read-only /proc and /sys paths sit on the
// container's own procfs and sysfs.
const containerMountinfo = `2167 2071 0:285 / / rw,relatime master:1 - overlay overlay rw,lowerdir=/l,upperdir=/u,workdir=/w
2168 2167 0:288 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
2169 2167 0:289 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
2175 2167 0:291 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
2176 2175 0:30 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
2180 2168 0:288 /bus /proc/bus ro,relatime - proc proc rw
2185 2168 0:293 / /proc/acpi ro,relatime - tmpfs tmpfs ro
2186 2168 0:5 /null /proc/kcore rw,nosuid - devtmpfs devtmpfs rw,size=4096k
2190 2168 0:295 / /proc/scsi ro,relatime - tmpfs tmpfs ro
2191 2175 0:296 / /sys/firmware ro,relatime - tmpfs tmpfs ro
2192 2190 0:297 / /proc/scsi/nested rw - tmpfs tmpfs rw
2200 2167 0:300 / /data rw shared:5 - ceph 10.0.0.10:3300:/volumes/csi rw
2201 2169 0:301 / /dev/shm rw - tmpfs shm rw
2202 2167 0:302 / /my\040dir rw - tmpfs tmpfs rw
2203 2168 0:303 / /proc/odd\040name ro - tmpfs tmpfs ro
2204 2168 0:304 / /proc/kcore rw - tmpfs tmpfs rw
`

func TestParseProcSysMounts(t *testing.T) {
	got, err := parseProcSysMounts(strings.NewReader(containerMountinfo))
	if err != nil {
		t.Fatal(err)
	}
	// Not /data, /dev/shm or /my dir (not on procfs/sysfs), nor /proc/scsi/nested (on a tmpfs,
	// where CRIU can create mountpoints); /proc/kcore once although mounted twice.
	want := []string{"/proc/acpi", "/proc/bus", "/proc/kcore", "/proc/odd name", "/proc/scsi", "/sys/firmware", "/sys/fs/cgroup"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestProcSysMountsOfSelf(t *testing.T) {
	// The test process's own namespace: whatever it holds, every path must be a valid query.
	paths, err := procSysMounts(os.Getpid())
	if err != nil {
		t.Skip("no /proc/self/mountinfo:", err)
	}
	for _, p := range paths {
		if !validProcSysPath(p) {
			t.Errorf("%q isn't a valid query path", p)
		}
	}
}

func TestValidProcSysPath(t *testing.T) {
	for p, ok := range map[string]bool{
		"/proc/scsi": true, "/sys/firmware": true, "/proc/sys/net/netfilter": true,
		"/proc": false, "/sys/": false, "/etc/passwd": false, "/proc/../etc": false,
		"proc/scsi": false, "/proc/scsi/": false, "/procfoo": false,
	} {
		if validProcSysPath(p) != ok {
			t.Errorf("validProcSysPath(%q) = %v", p, !ok)
		}
	}
}

func TestMissingPaths(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "proc", "acpi"), 0o755)
	os.MkdirAll(filepath.Join(root, "sys", "firmware"), 0o755)
	got := missingPaths(root, []string{"/proc/acpi", "/proc/scsi", "/sys/firmware", "/proc/asound"})
	if want := []string{"/proc/scsi", "/proc/asound"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := missingPaths(root, []string{"/proc/acpi"}); got == nil || len(got) != 0 {
		t.Errorf("nothing missing should be an empty list, not %v", got)
	}
}

func TestInfoMissingPaths(t *testing.T) {
	srv, _ := newTestServer(t)
	c := &stagingClient{base: srv.URL, key: "ns_pod-m1_c", token: testToken}
	ctx := context.Background()
	// The node running the test has /proc/self and no /proc/lcm-test-absent.
	missing, ok := c.missingPaths(ctx, []string{"/proc/self", "/proc/lcm-test-absent"})
	if !ok || !reflect.DeepEqual(missing, []string{"/proc/lcm-test-absent"}) {
		t.Errorf("missing %q ok %v", missing, ok)
	}
	if missing, ok := c.missingPaths(ctx, []string{"/proc/self"}); !ok || len(missing) != 0 {
		t.Errorf("nothing missing: got %q ok %v", missing, ok)
	}
	// Only /proc and /sys may be asked about.
	req, _ := http.NewRequest("GET", srv.URL+"/v1/info?path=/etc/shadow", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a path outside /proc and /sys: %v %v", resp.StatusCode, err)
	}
}

// An agent from before the check answers /v1/info without missingPaths: the source must not take
// that for "nothing missing".
func TestMissingPathsLegacyReceiver(t *testing.T) {
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"addresses":["10.0.0.2"],"version":"old"}`))
	}))
	defer legacy.Close()
	c := &stagingClient{base: legacy.URL, key: "ns_pod-m1_c", token: testToken}
	if _, ok := c.missingPaths(context.Background(), []string{"/proc/scsi"}); ok {
		t.Error("a receiver that doesn't report missingPaths must not count as checked")
	}
}
