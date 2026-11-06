package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/config"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policies"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

const usage = `Usage:
  scheduler-sim run --config PATH [--output PATH] [--results-dir DIR] [--policy POLICY]
  scheduler-sim --version

Policies: first-fit, round-robin, random-fit, least-loaded, bin-pack
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
	resultsDir := flags.String("results-dir", "", "write a versioned detailed result bundle")
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
	startedAt := time.Now()
	configData, err := os.ReadFile(*configPath)
	if err != nil {
		return newCommandError("config_load_failed", err)
	}
	configuration, err := config.Decode(bytes.NewReader(configData))
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
	selectedPolicy, err := newPolicy(*policy, configuration.Seed)
	if err != nil {
		return newCommandError("usage", err)
	}
	var recorder *detailRecorder
	if *resultsDir != "" {
		recorder = &detailRecorder{}
	}
	report, err := runSimulation(ctx, simulation, selectedPolicy, metrics.NewCollector(), recorder)
	if err != nil {
		return newCommandError("simulation_failed", err)
	}
	summary := buildSummary(selectedPolicy.Name(), configuration.Seed, simulation, report)
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return newCommandError("result_encode_failed", err)
	}
	encoded = append(encoded, '\n')
	if *resultsDir != "" {
		manifest := results.NewManifest(*configPath, configData, selectedPolicy.Name(), configuration.Seed, version, startedAt, time.Now())
		if err := results.WriteBundle(*resultsDir, manifest, summary, buildJobRecords(simulation.Jobs()), recorder.series); err != nil {
			return newCommandError("result_write_failed", err)
		}
	}
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
	_, err := runSimulation(ctx, simulation, firstFitPolicy{}, metrics.NewCollector(), nil)
	return err
}

func runSimulation(ctx context.Context, simulation *simulator.Simulation, policy scheduler.Policy, collector *metrics.Collector, recorder *detailRecorder) (metrics.Report, error) {
	if err := observeMetrics(collector, simulation); err != nil {
		return metrics.Report{}, err
	}
	if recorder != nil {
		if err := recorder.record(simulator.Event{Type: "initial"}, simulation); err != nil {
			return metrics.Report{}, err
		}
	}
	for {
		event, ok, err := simulation.RunNext(ctx)
		if err != nil {
			return metrics.Report{}, err
		}
		if !ok {
			return collector.Finalize(simulation.Now(), simulation.Jobs())
		}
		if err := observeMetrics(collector, simulation); err != nil {
			return metrics.Report{}, err
		}
		if recorder != nil {
			if err := recorder.record(event, simulation); err != nil {
				return metrics.Report{}, err
			}
		}
		if next, exists := simulation.NextEvent(); exists && next.Time == simulation.Now() {
			continue
		}
		if _, err := schedule(ctx, simulation, policy, collector); err != nil {
			return metrics.Report{}, err
		}
	}
}

type firstFitPolicy struct{}

func (firstFitPolicy) Name() string { return "first-fit" }

func (firstFitPolicy) Decide(_ context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	for _, job := range snapshot.Jobs() {
		if job.State != domain.JobStateQueued {
			continue
		}
		for _, node := range snapshot.Nodes() {
			if !node.CanFit(job.Request) {
				continue
			}
			return scheduler.Decision{Placements: []domain.Placement{{JobID: job.ID, NodeID: node.ID}}}, nil
		}
	}
	return scheduler.Decision{Defer: true}, nil
}

func schedule(ctx context.Context, simulation *simulator.Simulation, policy scheduler.Policy, collector *metrics.Collector) (bool, error) {
	snapshot, err := scheduler.NewSnapshot(simulation.Now(), simulation.Nodes(), simulation.Jobs())
	if err != nil {
		return false, err
	}
	evaluation, err := scheduler.Evaluate(ctx, policy, snapshot)
	collector.RecordDecision(evaluation.Telemetry)
	if err != nil {
		return false, err
	}
	for _, placement := range evaluation.Decision.Placements {
		if _, err := simulation.SchedulePlacement(simulation.Now(), placement); err != nil {
			return false, err
		}
	}
	return len(evaluation.Decision.Placements) != 0, nil
}

func observeMetrics(collector *metrics.Collector, simulation *simulator.Simulation) error {
	queued := 0
	for _, job := range simulation.Jobs() {
		if job.State == domain.JobStateQueued {
			queued++
		}
	}
	return collector.ObserveCluster(simulation.Now(), simulation.Nodes(), queued)
}

func newPolicy(name string, seed int64) (scheduler.Policy, error) {
	switch name {
	case "first-fit":
		return firstFitPolicy{}, nil
	case "round-robin":
		return policies.NewRoundRobin(), nil
	case "random-fit":
		return policies.NewRandomFit(seed), nil
	case "least-loaded":
		return policies.NewLeastLoaded(), nil
	case "bin-pack", "binpack":
		return policies.NewBinPack(), nil
	default:
		return nil, fmt.Errorf("unsupported policy %q", name)
	}
}

func newCommandError(code string, err error) *commandError {
	return &commandError{Code: code, Message: err.Error(), err: err}
}

func writeCommandError(writer io.Writer, commandErr *commandError) {
	_ = json.NewEncoder(writer).Encode(errorEnvelope{Error: commandErr})
}
