package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/experiment"
)

func main() { os.Exit(runCLI(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("experiment-run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scheduler := flags.String("scheduler", "", "path to scheduler-sim binary")
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: experiment-run [--scheduler PATH] EXPERIMENT.yaml")
		return 2
	}
	spec, err := experiment.Load(flags.Arg(0))
	if err != nil {
		writeError(stderr, err)
		return 1
	}
	binary := *scheduler
	if binary == "" {
		executable, executableErr := os.Executable()
		if executableErr != nil {
			writeError(stderr, executableErr)
			return 1
		}
		binary = filepath.Join(filepath.Dir(executable), "scheduler-sim")
	}
	matrix, err := experiment.Run(ctx, spec, experiment.CommandRunner{Binary: binary})
	if err != nil {
		writeError(stderr, err)
		return 1
	}
	_ = json.NewEncoder(stdout).Encode(map[string]any{"output_dir": spec.OutputDir, "runs": len(matrix.Runs)})
	return 0
}

func writeError(writer io.Writer, err error) {
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": err.Error()})
}
