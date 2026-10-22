// Package cliutil holds small helpers shared by the lcm commands.
package cliutil

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Token resolves the shared tunnel secret: -token, then -token-file, then
// $LCM_TOKEN, then $LCM_TOKEN_FILE. Surrounding whitespace is trimmed.
func Token(flagVal, flagFile string) ([]byte, error) {
	val, file := flagVal, flagFile
	if val == "" && file == "" {
		val, file = os.Getenv("LCM_TOKEN"), os.Getenv("LCM_TOKEN_FILE")
	}
	if val == "" && file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		val = string(b)
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return nil, errors.New("no tunnel token: set -token, -token-file, LCM_TOKEN or LCM_TOKEN_FILE")
	}
	return []byte(val), nil
}

// Logger builds a text logger on stderr at the named level.
func Logger(level, prefix string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})).With("app", prefix)
}

// Size is a flag.Value accepting byte sizes like 1048576, 512K, 1M, 1MiB.
type Size int

func (s *Size) String() string { return strconv.Itoa(int(*s)) }
func (s *Size) Set(v string) error {
	v = strings.TrimSpace(strings.ToUpper(v))
	v = strings.TrimSuffix(strings.TrimSuffix(v, "IB"), "B")
	mult := 1
	switch {
	case strings.HasSuffix(v, "K"):
		mult, v = 1<<10, v[:len(v)-1]
	case strings.HasSuffix(v, "M"):
		mult, v = 1<<20, v[:len(v)-1]
	case strings.HasSuffix(v, "G"):
		mult, v = 1<<30, v[:len(v)-1]
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fmt.Errorf("bad size %q", v)
	}
	*s = Size(n * mult)
	return nil
}

// EnvDefault returns $name if set, else def (for flag defaults).
func EnvDefault(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

var _ flag.Value = (*Size)(nil)
