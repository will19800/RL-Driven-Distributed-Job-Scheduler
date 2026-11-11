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
	"runtime/pprof"
	"sort"
	"strconv"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/config"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/inference"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policies"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

const usage = `Usage:
  scheduler-sim run --config PATH [--output PATH] [--results-dir DIR] [--policy POLICY] [--seed SEED]
  scheduler-sim --version

Policies: first-fit, round-robin, random-fit, least-loaded, bin-pack, rl
RL: --model DIR --python PATH [--inference-timeout 250ms] [--fallback bin-pack]
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
	modelDir := flags.String("model", "", "versioned exported model directory")
	python := flags.String("python", "python3", "Python interpreter with training extra installed")
	inferenceTimeout := flags.Duration("inference-timeout", 250*time.Millisecond, "per-decision inference budget")
	fallbackName := flags.String("fallback", "bin-pack", "fallback baseline policy")
	cpuProfile := flags.String("cpu-profile", "", "write a CPU profile (profiling runs only)")
	seedOverride := ""
	flags.StringVar(&seedOverride, "seed", "", "override the configuration seed")
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
	if *cpuProfile != "" {
		file, err := os.OpenFile(*cpuProfile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return newCommandError("profile_failed", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			file.Close()
			return newCommandError("profile_failed", err)
		}
		defer func() { pprof.StopCPUProfile(); _ = file.Close() }()
	}
	configData, err := os.ReadFile(*configPath)
	if err != nil {
		return newCommandError("config_load_failed", err)
	}
	configuration, err := config.DecodeRelativeTo(bytes.NewReader(configData), *configPath)
	if err != nil {
		return newCommandError("config_load_failed", err)
	}
	if seedOverride != "" {
		seed, parseErr := strconv.ParseInt(seedOverride, 10, 64)
		if parseErr != nil {
			return newCommandError("usage", fmt.Errorf("invalid --seed %q: %w", seedOverride, parseErr))
		}
		configuration.Seed = seed
	}
	nodes, jobs, err := configuration.DomainValues()
	if err != nil {
		return newCommandError("config_invalid", err)
	}
	simulation, err := simulator.NewSimulation(nodes, jobs)
	if err != nil {
		return newCommandError("simulation_init_failed", err)
	}
	outages := append([]config.OutageConfig(nil), configuration.Outages...)
	sort.SliceStable(outages, func(i, j int) bool { return outages[i].FailAt.Duration < outages[j].FailAt.Duration })
	for _, outage := range outages {
		if _, err := simulation.ScheduleAvailability(outage.FailAt.Duration, domain.NodeID(outage.NodeID), false); err != nil {
			return newCommandError("simulation_init_failed", err)
		}
		if _, err := simulation.ScheduleAvailability(outage.RecoverAt.Duration, domain.NodeID(outage.NodeID), true); err != nil {
			return newCommandError("simulation_init_failed", err)
		}
	}
	var selectedPolicy scheduler.Policy
	var rlPolicy *inference.Policy
	var process *inference.Process
	if *policy == "rl" {
		if *modelDir == "" || *inferenceTimeout <= 0 {
			return newCommandError("usage", errors.New("rl requires --model and a positive --inference-timeout"))
		}
		fallback, fallbackErr := newPolicy(*fallbackName, configuration.Seed)
		if fallbackErr != nil {
			return newCommandError("usage", fallbackErr)
		}
		startup, cancel := context.WithTimeout(ctx, inference.StartupTimeout)
		process, err = inference.Start(startup, *python, *modelDir)
		cancel()
		if err != nil {
			return newCommandError("model_load_failed", err)
		}
		defer process.Close()
		rlPolicy, err = inference.NewPolicy(process.Config, process, fallback, *inferenceTimeout)
		if err != nil {
			return newCommandError("model_load_failed", err)
		}
		selectedPolicy = rlPolicy
	} else {
		if *modelDir != "" {
			return newCommandError("usage", errors.New("--model requires --policy rl"))
		}
		selectedPolicy, err = newPolicy(*policy, configuration.Seed)
		if err != nil {
			return newCommandError("usage", err)
		}
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
	if rlPolicy != nil {
		diagnostics := rlPolicy.Diagnostics()
		summary.Inference = &diagnostics
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return newCommandError("result_encode_failed", err)
	}
	encoded = append(encoded, '\n')
	if *resultsDir != "" {
		manifest := results.NewManifest(*configPath, configData, selectedPolicy.Name(), configuration.Seed, version, startedAt, time.Now())
		if process != nil {
			manifest.Model = &process.Metadata
			manifest.InferenceTimeout = inferenceTimeout.String()
			manifest.FallbackPolicy = *fallbackName
		}
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
