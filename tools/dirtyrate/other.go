//go:build !linux

// Command dirtyrate measures memory dirty rates with Linux soft-dirty bits; Linux only.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "dirtyrate: Linux only (soft-dirty page tracking)")
	os.Exit(1)
}
