// Command result-check validates scheduler simulation result bundles.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
)

const version = "dev"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--version" || args[0] == "version") {
		fmt.Fprintf(stdout, "result-check %s\n", version)
		return 0
	}
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: result-check BUNDLE_DIR")
		return 2
	}
	if err := results.ValidateBundle(args[0]); err != nil {
		fmt.Fprintf(stderr, "invalid result bundle: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "valid result bundle: %s\n", args[0])
	return 0
}
