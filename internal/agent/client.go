package agent

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// stagingClient ships image dirs from the source agent to the destination agent.
type stagingClient struct {
	base, key, token string
}

var shipClient = &http.Client{Transport: &http.Transport{
	DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}).DialContext,
	MaxIdleConnsPerHost: 4,
	WriteBufferSize:     1 << 20,
	ReadBufferSize:      64 << 10,
}}

func (c *stagingClient) do(ctx context.Context, method, path string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.base, "/")+"/v1/stage/"+url.PathEscape(c.key)+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := shipClient.Do(req)
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

func (c *stagingClient) reset(ctx context.Context) error {
	return c.do(ctx, http.MethodDelete, "", nil)
}
func (c *stagingClient) ready(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/ready", nil)
}

// putDir streams dir as a tar (regular files, plus CRIU's relative `parent` symlink).
func (c *stagingClient) putDir(ctx context.Context, dir, name string) error {
	pr, pw := io.Pipe()
	go func() {
		bw := bufio.NewWriterSize(pw, 1<<20)
		err := writeTar(bw, dir)
		if err == nil {
			err = bw.Flush()
		}
		pw.CloseWithError(err)
	}()
	return c.do(ctx, http.MethodPut, "/dir/"+url.PathEscape(name), pr)
}

func (c *stagingClient) putFile(ctx context.Context, path, name string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return c.do(ctx, http.MethodPut, "/file/"+url.PathEscape(name), bufio.NewReaderSize(f, 1<<20))
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
