package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/config"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

const usage = `Usage:
  scheduler-sim run --config PATH [--output PATH] [--policy first-fit]
  scheduler-sim --version
`

type commandError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	err     error
}

func (e *commandError) Error() string { return e.Message }
func (e *commandError) Unwrap() error { return e.err }

type errorEnvelope struct {
	Error *commandError `json:"error"`
}

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--version" || args[0] == "version") {
		fmt.Fprintf(stdout, "scheduler-sim %s\n", version)
		return 0
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if len(args) == 0 || args[0] != "run" {
		writeCommandError(stderr, newCommandError("usage", errors.New("expected run command")))
		return 2
	}

	if err := runCommand(ctx, args[1:], stdout); err != nil {
		writeCommandError(stderr, err)
		if err.Code == "usage" {
			return 2
		}
		return 1
	}
	return 0
}

func runCommand(ctx context.Context, args []string, stdout io.Writer) *commandError {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "simulation configuration path")
	outputPath := flags.String("output", "-", "summary output path, or - for stdout")
	policy := flags.String("policy", "first-fit", "scheduling policy")
	if err := flags.Parse(args); err != nil {
		return newCommandError("usage", err)
	}
	if *configPath == "" {
		return newCommandError("usage", errors.New("--config is required"))
	}
	if flags.NArg() != 0 {
		return newCommandError("usage", fmt.Errorf("unexpected arguments: %v", flags.Args()))
	}
	if *policy != "first-fit" {
		return newCommandError("usage", fmt.Errorf("unsupported policy %q", *policy))
	}

	configuration, err := config.Load(*configPath)
	if err != nil {
		return newCommandError("config_load_failed", err)
	}
	nodes, jobs, err := configuration.DomainValues()
	if err != nil {
		return newCommandError("config_invalid", err)
	}
	simulation, err := simulator.NewSimulation(nodes, jobs)
	if err != nil {
		return newCommandError("simulation_init_failed", err)
	}
	if err := runFirstFit(ctx, simulation); err != nil {
		return newCommandError("simulation_failed", err)
	}
	encoded, err := json.MarshalIndent(buildSummary(configuration.Seed, simulation), "", "  ")
	if err != nil {
		return newCommandError("result_encode_failed", err)
	}
	encoded = append(encoded, '\n')
	if *outputPath == "-" {
		if _, err := stdout.Write(encoded); err != nil {
			return newCommandError("result_write_failed", err)
		}
		return nil
	}
	if err := os.WriteFile(*outputPath, encoded, 0o644); err != nil {
		return newCommandError("result_write_failed", err)
	}
	return nil
}

func runFirstFit(ctx context.Context, simulation *simulator.Simulation) error {
	for {
		_, ok, err := simulation.RunNext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if next, exists := simulation.NextEvent(); exists && next.Time == simulation.Now() {
			continue
		}
		if _, err := scheduleFirstFit(simulation); err != nil {
			return err
		}
	}
}

func scheduleFirstFit(simulation *simulator.Simulation) (bool, error) {
	jobs := simulation.Jobs()
	sort.SliceStable(jobs, func(left, right int) bool {
		if jobs[left].Priority == jobs[right].Priority {
			if jobs[left].SubmitTime == jobs[right].SubmitTime {
				return jobs[left].ID < jobs[right].ID
			}
			return jobs[left].SubmitTime < jobs[right].SubmitTime
		}
		return jobs[left].Priority > jobs[right].Priority
	})
	for _, job := range jobs {
		if job.State != domain.JobStateQueued {
			continue
		}
		for _, node := range simulation.Nodes() {
			if !node.CanFit(job.Request) {
				continue
			}
			_, err := simulation.SchedulePlacement(simulation.Now(), domain.Placement{JobID: job.ID, NodeID: node.ID})
			return err == nil, err
		}
	}
	return false, nil
}

func newCommandError(code string, err error) *commandError {
	return &commandError{Code: code, Message: err.Error(), err: err}
}

func writeCommandError(writer io.Writer, commandErr *commandError) {
	_ = json.NewEncoder(writer).Encode(errorEnvelope{Error: commandErr})
}
