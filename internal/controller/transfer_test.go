package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeInfo serves GET /v1/info with whatever *info holds.
func fakeInfo(t *testing.T, info *string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/info" || r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		w.Write([]byte(*info))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestTransferURL(t *testing.T) {
	var fromInfo, toInfo string
	from, to := fakeInfo(t, &fromInfo), fakeInfo(t, &toInfo)
	const withTLS = `{"addresses":["100.64.0.2","10.0.0.2"],"tlsPort":7791}`
	const plain = `{"addresses":["100.64.0.2","10.0.0.2"]}`
	for _, c := range []struct {
		from, to  string
		plaintext bool
		want, err string
	}{
		{withTLS, withTLS, false, "https://10.0.0.2:7791", ""},
		{withTLS, withTLS, true, "https://10.0.0.2:7791", ""}, // TLS whenever both can
		{withTLS, plain, false, "", "encrypted transport"},
		{withTLS, plain, true, "http://10.0.0.2:7790", ""},
		{plain, withTLS, false, "", "encrypted transport"}, // a sender without the transport
		{plain, withTLS, true, "http://10.0.0.2:7790", ""},
		{withTLS, `{"addresses":["100.64.0.2"],"tlsPort":7791}`, false, "", "no address in 10.0.0.0/24"},
	} {
		fromInfo, toInfo = c.from, c.to
		got, err := transferURL(context.Background(), from, to, "tok", "10.0.0.0/24", 7790, c.plaintext)
		if got != c.want || (c.err == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.err)) {
			t.Errorf("from %s to %s plaintext=%v: %q, %v; want %q, %q", c.from, c.to, c.plaintext, got, err, c.want, c.err)
		}
	}

	// Without a usable transfer address the data goes over the agent URL (InternalIP).
	fromInfo, toInfo = withTLS, plain
	var logs []string
	o := Options{Token: "tok", TransferNetwork: "10.0.0.0/24"}
	if u := shipURL(context.Background(), o, from, to, "carrack", func(f string, a ...any) { logs = append(logs, f) }); u != to {
		t.Errorf("fallback: %q", u)
	}
	if len(logs) != 1 || !strings.HasPrefix(logs[0], "warning") {
		t.Errorf("logs %q", logs)
	}
}
