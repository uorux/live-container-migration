package agent

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// stagingClient ships image dirs from the source agent to the destination agent. Bodies are
// zstd-compressed: page images compress 6-8x (measured on a Minecraft JVM) and node-to-node links
// (WireGuard) are far slower than the compressor, so this is the biggest lever on both pre-copy
// convergence and the final ship inside the blackout. Receivers too old to decode zstd get plain
// bodies (see encoding), so nodes can be upgraded one at a time. An https:// base goes over the
// encrypted agent transport (transport.go).
type stagingClient struct {
	base, key, token string
	hc               *http.Client // nil: shipClient (plain HTTP only)

	mu    sync.Mutex
	known bool
	enc   string // Content-Encoding the receiver decodes: "zstd" or "" (plain)
}

// encoding returns the Content-Encoding to send, asking the receiving agent once. Agents from
// before compressed transfers have no GET /v1/info (404) and get plain bodies; later ones decode
// zstd, and list what they decode in "encodings". If the receiver can't be asked (unreachable,
// restarting) the answer is zstd for now and it is asked again next time.
func (c *stagingClient) encoding(ctx context.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.known {
		return c.enc
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.base, "/")+"/v1/info", nil)
	if err != nil {
		return "zstd"
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client().Do(req)
	if err != nil {
		return "zstd"
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		c.known, c.enc = true, ""
	case resp.StatusCode/100 == 2:
		var info struct {
			Encodings []string `json:"encodings"`
		}
		json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&info)
		c.known, c.enc = true, "zstd"
		if info.Encodings != nil && !slices.Contains(info.Encodings, "zstd") {
			c.enc = ""
		}
	default:
		return "zstd"
	}
	return c.enc
}

// compressConcurrency bounds the cores compression takes on the source, where the workload is
// still running during pre-copy.
const compressConcurrency = 4

var shipClient = newShipClient(nil)

// newShipClient returns a client for agent-to-agent transfers; tlsConf (the transport's client
// side) is used for https:// URLs.
func newShipClient(tlsConf *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}).DialContext,
		TLSClientConfig:     tlsConf,
		TLSHandshakeTimeout: 5 * time.Second,
		MaxIdleConnsPerHost: 4,
		WriteBufferSize:     1 << 20,
		ReadBufferSize:      64 << 10,
	}}
}

// stagingClient returns a client shipping to the agent at base under key.
func (a *Agent) stagingClient(base, key string) *stagingClient {
	return &stagingClient{base: base, key: key, token: a.token, hc: a.ship}
}

func (c *stagingClient) client() *http.Client {
	if c.hc != nil {
		return c.hc
	}
	return shipClient
}

func (c *stagingClient) do(ctx context.Context, method, path string, body io.Reader) error {
	return c.doEnc(ctx, method, path, body, "")
}

func (c *stagingClient) doEnc(ctx context.Context, method, path string, body io.Reader, encoding string) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.base, "/")+"/v1/stage/"+url.PathEscape(c.key)+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

// stagingFree returns the receiver's free staging space, or -1 if it doesn't say (an older agent,
// or it can't be asked right now).
func (c *stagingClient) stagingFree(ctx context.Context) int64 {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.base, "/")+"/v1/info", nil)
	if err != nil {
		return -1
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client().Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var info struct {
		StagingFree *int64 `json:"stagingFree"`
	}
	if resp.StatusCode/100 != 2 || json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&info) != nil || info.StagingFree == nil {
		return -1
	}
	return *info.StagingFree
}

func (c *stagingClient) reset(ctx context.Context) error {
	return c.do(ctx, http.MethodDelete, "", nil)
}
func (c *stagingClient) ready(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/ready", nil)
}

// putDir streams dir as a (zstd-compressed) tar (regular files, plus CRIU's relative `parent`
// symlink).
func (c *stagingClient) putDir(ctx context.Context, dir, name string) error {
	return c.putStream(ctx, "/dir/"+url.PathEscape(name), func(w io.Writer) error { return writeTar(w, dir) })
}

func (c *stagingClient) putFile(ctx context.Context, path, name string) error {
	return c.putStream(ctx, "/file/"+url.PathEscape(name), func(w io.Writer) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	})
}

func (c *stagingClient) putStream(ctx context.Context, path string, write func(io.Writer) error) error {
	pr, pw := io.Pipe()
	enc := c.encoding(ctx)
	if enc == "" {
		go func() {
			bw := bufio.NewWriterSize(pw, 1<<20)
			err := write(bw)
			if err == nil {
				err = bw.Flush()
			}
			pw.CloseWithError(err)
		}()
		return c.do(ctx, http.MethodPut, path, pr)
	}
	go func() {
		zw, err := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(compressConcurrency))
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		bw := bufio.NewWriterSize(zw, 1<<20)
		err = write(bw)
		if err == nil {
			err = bw.Flush()
		}
		if cerr := zw.Close(); err == nil {
			err = cerr
		}
		pw.CloseWithError(err)
	}()
	return c.doEnc(ctx, http.MethodPut, path, pr, "zstd")
}

func writeTar(w io.Writer, dir string) error {
	tw := tar.NewWriter(w)
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		link := ""
		if fi.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			f.Close()
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return tw.Close()
}
