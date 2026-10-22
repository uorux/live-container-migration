// lcm-agent is the per-node live-migration daemon. See internal/agent for the HTTP API.
//
//	lcm-agent serve [-listen :7790] [-token-file F] [-bin-dir DIR]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"lcm/internal/agent"
	"lcm/internal/api"
	"lcm/internal/stage"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: lcm-agent serve [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", fmt.Sprintf(":%d", api.DefaultPort), "listen address")
	tokenFile := fs.String("token-file", os.Getenv("LCM_TOKEN_FILE"), "file holding the shared bearer token")
	binDir := fs.String("bin-dir", os.Getenv("LCM_BIN_DIR"), "directory with lcm-relay/lcm-gateway to serve to init containers")
	fs.Parse(os.Args[2:])

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	b, err := os.ReadFile(*tokenFile)
	token := strings.TrimSpace(string(b))
	if err != nil || token == "" {
		log.Error("a non-empty -token-file is required", "err", err)
		os.Exit(1)
	}
	for _, d := range []string{"ckpt", "restore", "fence"} {
		os.MkdirAll(stage.Root()+"/"+d, 0o700)
	}
	a := agent.New(token, *binDir, log)
	go a.GC(context.Background(), 14*24*time.Hour)
	srv := &http.Server{Addr: *listen, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Info("lcm-agent listening", "addr", *listen, "state", stage.Root())
	if err := srv.ListenAndServe(); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
