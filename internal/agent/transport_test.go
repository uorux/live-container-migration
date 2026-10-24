package agent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func testAgent(t testing.TB, token string) *Agent {
	return New(token, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// serveTLS serves a over its encrypted transport on loopback and returns the https base URL.
func serveTLS(t testing.TB, a *Agent) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: a.Handler()}
	go srv.Serve(a.TLSListener(ln))
	t.Cleanup(func() { srv.Close() })
	return "https://" + ln.Addr().String()
}

func get(c *http.Client, url, token string) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return resp, err
}

func TestTransportKeyFromToken(t *testing.T) {
	a, b, c := newTransport("tok"), newTransport("tok"), newTransport("other")
	if !a.pub.Equal(b.pub) || !bytes.Equal(a.cert.Certificate[0], b.cert.Certificate[0]) {
		t.Error("same token, different key or certificate")
	}
	if a.pub.Equal(c.pub) {
		t.Error("different tokens, same key")
	}
	if _, err := x509.ParseCertificate(a.cert.Certificate[0]); err != nil {
		t.Error(err)
	}
}

func TestTransportHandshake(t *testing.T) {
	t.Setenv("LCM_STATE_DIR", t.TempDir())
	base := serveTLS(t, testAgent(t, testToken))

	// Another agent with the same token: TLS 1.3, and the server presented the shared key.
	resp, err := get(testAgent(t, testToken).ship, base+"/v1/info", testToken)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.TLS == nil || resp.TLS.Version != tls.VersionTLS13 || resp.ProtoMajor != 1 {
		t.Fatalf("status %d, TLS %+v, proto %s", resp.StatusCode, resp.TLS, resp.Proto)
	}
	t.Logf("negotiated %s, key exchange %s", tls.CipherSuiteName(resp.TLS.CipherSuite), resp.TLS.CurveID)
	if !resp.TLS.PeerCertificates[0].PublicKey.(ed25519.PublicKey).Equal(newTransport(testToken).pub) {
		t.Error("server presented another key")
	}
	// The bearer token is still checked inside the tunnel.
	if resp, err := get(testAgent(t, testToken).ship, base+"/v1/info", "wrong"); err != nil || resp.StatusCode != 401 {
		t.Errorf("wrong bearer token over TLS: %v %v", resp, err)
	}

	// A client with another token: it rejects the server's key (and the server would reject its).
	if _, err := get(testAgent(t, "other").ship, base+"/healthz", testToken); err == nil || !strings.Contains(err.Error(), "doesn't match") {
		t.Errorf("client with another token: %v", err)
	}
	// A client that trusts the server but presents no certificate, or a different key.
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	for name, certs := range map[string][]tls.Certificate{"no certificate": nil, "other key": {selfSigned(t, other)}} {
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, Certificates: certs, MinVersion: tls.VersionTLS13}}}
		if resp, err := get(c, base+"/healthz", testToken); err == nil {
			t.Errorf("%s: server accepted the client (%d)", name, resp.StatusCode)
		}
	}
	// Plain HTTP to the TLS port gets nowhere.
	if resp, err := get(http.DefaultClient, strings.Replace(base, "https", "http", 1)+"/healthz", testToken); err == nil && resp.StatusCode == 200 {
		t.Error("plain HTTP served on the TLS port")
	}
}

// The pin is on the exact key: a server with a valid-looking certificate for another key is refused.
func TestTransportPinsServerKey(t *testing.T) {
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impostor := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("impostor got a request: %s %s (token %q)", r.Method, r.URL, r.Header.Get("Authorization"))
	})}
	go impostor.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{selfSigned(t, other)}, ClientAuth: tls.RequestClientCert}))
	defer impostor.Close()
	_, err = get(testAgent(t, testToken).ship, "https://"+ln.Addr().String()+"/v1/info", testToken)
	if err == nil || !strings.Contains(err.Error(), "doesn't match") {
		t.Errorf("impostor server: %v", err)
	}
}

func selfSigned(t testing.TB, priv ed25519.PrivateKey) tls.Certificate {
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, priv.Public(), priv)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

// A source agent ships an image dir and a file to a destination agent over the transport.
func TestStagingOverTransport(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LCM_STATE_DIR", root)
	dstAgent := testAgent(t, testToken)
	base := serveTLS(t, dstAgent)

	// The destination advertises its TLS port where the CLI looks.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/info", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	dstAgent.Handler().ServeHTTP(rec, req)
	var info struct {
		TLSPort int `json:"tlsPort"`
	}
	json.Unmarshal(rec.Body.Bytes(), &info)
	if !strings.HasSuffix(base, ":"+strconv.Itoa(info.TLSPort)) {
		t.Errorf("info tlsPort %d, serving %s", info.TLSPort, base)
	}

	src := t.TempDir()
	pages := bytes.Repeat([]byte("jvm heap page "), 1<<16)
	os.WriteFile(filepath.Join(src, "pages-1.img"), pages, 0o644)
	os.Symlink("../pre-1", filepath.Join(src, "parent"))
	c := testAgent(t, testToken).stagingClient(base, "ns_pod-m1_c")
	ctx := context.Background()
	if enc := c.encoding(ctx); enc != "zstd" {
		t.Errorf("encoding over TLS: %q", enc)
	}
	if err := c.putDir(ctx, src, "images"); err != nil {
		t.Fatal(err)
	}
	if err := c.putFile(ctx, filepath.Join(src, "pages-1.img"), "rootfs-diff.tar"); err != nil {
		t.Fatal(err)
	}
	if err := c.ready(ctx); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "restore", "ns_pod-m1_c")
	if got, _ := os.ReadFile(filepath.Join(dst, "images", "pages-1.img")); !bytes.Equal(got, pages) {
		t.Errorf("pages mismatch (%d bytes)", len(got))
	}
	if l, _ := os.Readlink(filepath.Join(dst, "images", "parent")); l != "../pre-1" {
		t.Errorf("parent link = %q", l)
	}
	if _, err := os.Stat(filepath.Join(dst, "READY")); err != nil {
		t.Error(err)
	}

	// An agent with another token can't ship here.
	bad := testAgent(t, "other").stagingClient(base, "ns_pod-m2_c")
	if err := bad.putFile(ctx, filepath.Join(src, "pages-1.img"), "x"); err == nil {
		t.Error("agent with another token shipped over the transport")
	}
}

// Benchmarks: what encryption costs next to the compression already in the path.
//
//	go test ./internal/agent -run '^$' -bench 'Transport|Zstd' -benchtime 4s
//
// cpu-s/GB is process CPU time (both ends, plus the kernel's loopback copies) per GB moved.

func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func benchLoopback(b *testing.B, wrapServer, wrapClient func(net.Conn) net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(io.Discard, wrapServer(c))
		done <- err
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	c := wrapClient(raw)
	buf := make([]byte, 1<<20)
	rand.Read(buf)
	c.Write(buf[:1]) // handshake outside the timing
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		if _, err := c.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	c.Close()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric((cpuTime()-cpu0).Seconds()/(float64(b.N)*float64(len(buf))/1e9), "cpu-s/GB")
}

func BenchmarkTransportTCP(b *testing.B) {
	id := func(c net.Conn) net.Conn { return c }
	benchLoopback(b, id, id)
}

func BenchmarkTransportTLS(b *testing.B) {
	tr := newTransport(testToken)
	benchLoopback(b,
		func(c net.Conn) net.Conn { return tls.Server(c, tr.serverConfig()) },
		func(c net.Conn) net.Conn { return tls.Client(c, tr.clientConfig()) })
}

// heapLike is synthetic page data that compresses roughly like a JVM heap: zero pages, pages of
// small integers and repeated references, and some incompressible pages.
func heapLike(n int) []byte {
	r := mrand.New(mrand.NewPCG(1, 2))
	out := make([]byte, n)
	for p := 0; p+4096 <= n; p += 4096 {
		page := out[p : p+4096]
		switch k := r.IntN(10); {
		case k < 3: // zero
		case k < 9:
			refs := []uint64{0x7f00_1000_0000 + uint64(r.IntN(1<<20))*8, uint64(r.IntN(256)), 0}
			for i := 0; i < len(page); i += 8 {
				v := refs[r.IntN(len(refs))]
				if r.IntN(4) == 0 {
					v = 0x7f00_1000_0000 + uint64(r.IntN(1<<16))*8
				}
				binary.LittleEndian.PutUint64(page[i:], v)
			}
		default:
			for i := range page {
				page[i] = byte(r.Uint32())
			}
		}
	}
	return out
}

func BenchmarkZstdCompress(b *testing.B) {
	data := heapLike(64 << 20)
	zw, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	var out []byte
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		out = zw.EncodeAll(data, out[:0])
	}
	b.StopTimer()
	b.ReportMetric((cpuTime()-cpu0).Seconds()/(float64(b.N)*float64(len(data))/1e9), "cpu-s/GB")
	b.ReportMetric(float64(len(data))/float64(len(out)), "ratio")
}

func BenchmarkZstdDecompress(b *testing.B) {
	data := heapLike(64 << 20)
	zw, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	comp := zw.EncodeAll(data, nil)
	zr, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	out := make([]byte, 0, len(data))
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		out, _ = zr.DecodeAll(comp, out[:0])
	}
	b.StopTimer()
	b.ReportMetric((cpuTime()-cpu0).Seconds()/(float64(b.N)*float64(len(data))/1e9), "cpu-s/GB")
}

// The whole shipping path (zstd at compressConcurrency, HTTP, untar on the receiver) on loopback,
// plain and over the transport.
func benchShip(b *testing.B, overTLS bool) {
	b.Setenv("LCM_STATE_DIR", b.TempDir())
	dst := testAgent(b, testToken)
	base := serveTLS(b, dst)
	if !overTLS {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		srv := &http.Server{Handler: dst.Handler()}
		go srv.Serve(ln)
		b.Cleanup(func() { srv.Close() })
		base = "http://" + ln.Addr().String()
	}
	src := b.TempDir()
	data := heapLike(256 << 20)
	if err := os.WriteFile(filepath.Join(src, "pages-1.img"), data, 0o644); err != nil {
		b.Fatal(err)
	}
	c := testAgent(b, testToken).stagingClient(base, "ns_pod-m1_c")
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		if err := c.putDir(context.Background(), src, "images"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric((cpuTime()-cpu0).Seconds()/(float64(b.N)*float64(len(data))/1e9), "cpu-s/GB")
}

func BenchmarkShipPlain(b *testing.B) { benchShip(b, false) }
func BenchmarkShipTLS(b *testing.B)   { benchShip(b, true) }
