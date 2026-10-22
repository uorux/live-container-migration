package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPWaker returns a Config.Wake that asks lcm-agents (base URLs, e.g.
// http://node:7790) to wake a backend: POST /v1/wake/{backend}, trying each
// agent in turn until one answers. token is the agents' wake-only token.
func HTTPWaker(agents []string, token string) func(ctx context.Context, backend string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	return func(ctx context.Context, backend string) error {
		var errs []error
		for _, base := range agents {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/v1/wake/"+url.PathEscape(backend), nil)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := client.Do(req)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			switch {
			case resp.StatusCode/100 == 2:
				return nil
			case resp.StatusCode == http.StatusNotFound:
				// Same answer from every agent: there is no on-demand LivePod for it.
				return fmt.Errorf("%s: %s", base, strings.TrimSpace(string(msg)))
			default:
				errs = append(errs, fmt.Errorf("%s: %s: %s", base, resp.Status, strings.TrimSpace(string(msg))))
			}
		}
		return errors.Join(errs...)
	}
}
