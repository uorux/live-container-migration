// lcm-rootfsdiff captures an overlay upperdir as an OCI layer tar, or applies one to a rootfs.
//
//	lcm-rootfsdiff capture <upperdir> > diff.tar
//	lcm-rootfsdiff apply <rootfs> < diff.tar
package main

import (
	"bufio"
	"fmt"
	"os"

	"lcm/internal/rootfsdiff"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: lcm-rootfsdiff capture <upperdir> | apply <rootfs>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "capture":
		w := bufio.NewWriterSize(os.Stdout, 1<<20)
		if err = rootfsdiff.Capture(os.Args[2], w); err == nil {
			err = w.Flush()
		}
	case "apply":
		err = rootfsdiff.Apply(bufio.NewReaderSize(os.Stdin, 1<<20), os.Args[2])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcm-rootfsdiff:", err)
		os.Exit(1)
	}
}
