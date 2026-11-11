package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/config"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/inference"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

func TestOutOfOrderAdjacentOutagesRunChronologically(t *testing.T) {
	input := `schema_version: v1
seed: 1
cluster:
  nodes:
    - id: node
      capacity: {cpu_millis: 1000}
workload:
  jobs:
    - id: job
      submit_time: 0s
      resources: {cpu_millis: 1000}
      duration: 10s
outages:
  - {node_id: node, fail_at: 3s, recover_at: 4s}
  - {node_id: node, fail_at: 1s, recover_at: 3s}
`
	path := filepath.Join(t.TempDir(), "outages.yaml")
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runCLI(context.Background(), []string{"run", "--config", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	var summary resultSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Jobs.Rejected != 1 {
		t.Fatalf("jobs %+v", summary.Jobs)
	}
}

func smokeConfigPath() string {
	return filepath.Join("..", "..", "configs", "experiments", "smoke.yaml")
}

func TestRunCLIMatchesGoldenSmokeSummary(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath()}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
	}
	want, err := os.ReadFile(filepath.Join("testdata", "smoke_summary.golden.json"))
	if err != nil {
		t.Fatalf("ReadFile(golden) unexpected error: %v", err)
	}
	var normalized resultSummary
	if err := json.Unmarshal(stdout.Bytes(), &normalized); err != nil {
		t.Fatalf("summary is not valid JSON: %v", err)
	}
	normalized.Metrics.Scheduler.TotalDuration = "0s"
	normalized.Metrics.Scheduler.MeanDuration = "0s"
	normalized.Metrics.Scheduler.MaxDuration = "0s"
	got, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent(normalized) unexpected error: %v", err)
	}
	got = append(got, '\n')
	if !bytes.Equal(got, want) {
		t.Fatalf("summary mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunCLIWritesOutputFile(t *testing.T) {
	t.Parallel()
	output := filepath.Join(t.TempDir(), "summary.json")
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath(), "--output", output}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("ReadFile(output) unexpected error: %v", err)
	}
	var summary resultSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if summary.Jobs.Completed != 3 {
		t.Fatalf("completed jobs = %d, want 3", summary.Jobs.Completed)
	}
}

func TestRunCLIWritesValidDetailedBundle(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "run")
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath(), "--results-dir", directory}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
	}
	if err := results.ValidateBundle(directory); err != nil {
		t.Fatalf("ValidateBundle() unexpected error: %v", err)
	}
	for _, name := range []string{"manifest.json", "summary.json", "jobs.csv", "timeseries.csv"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Fatalf("missing bundle file %q: %v", name, err)
		}
	}
	if !bytes.Equal(stdout.Bytes(), mustReadFile(t, filepath.Join(directory, "summary.json"))) {
		t.Fatal("stdout summary differs from bundle summary")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return data
}

func TestRunCLISupportsBaselinePolicies(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"round-robin", "random-fit", "least-loaded", "bin-pack"} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath(), "--policy", policy}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
			}
			var summary resultSummary
			if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
				t.Fatalf("summary is not valid JSON: %v", err)
			}
			if summary.Policy != policy || summary.Jobs.Completed != 3 {
				t.Fatalf("summary = %+v, want policy %q and 3 completed jobs", summary, policy)
			}
		})
	}
}

func TestRunCLIAcceptsBinPackAlias(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath(), "--policy", "binpack"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
	}
	var summary resultSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("summary is not valid JSON: %v", err)
	}
	if summary.Policy != "bin-pack" {
		t.Fatalf("summary policy = %q, want bin-pack", summary.Policy)
	}
}

func TestRunCLIOverridesSeed(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", filepath.Join("..", "..", "configs", "experiments", "synthetic-smoke.yaml"), "--seed", "99"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runCLI() code = %d, stderr = %s", code, stderr.String())
	}
	var summary resultSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Seed != 99 {
		t.Fatalf("summary seed = %d, want 99", summary.Seed)
	}
}

func TestRunCLIReturnsStructuredErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		code int
		want string
	}{
		{"missing command", nil, 2, "usage"},
		{"missing config flag", []string{"run"}, 2, "usage"},
		{"unknown policy", []string{"run", "--config", smokeConfigPath(), "--policy", "unknown"}, 2, "usage"},
		{"missing model", []string{"run", "--config", smokeConfigPath(), "--policy", "rl"}, 2, "usage"},
		{"model with baseline", []string{"run", "--config", smokeConfigPath(), "--model", "unused"}, 2, "usage"},
		{"invalid seed", []string{"run", "--config", smokeConfigPath(), "--seed", "nope"}, 2, "usage"},
		{"missing config file", []string{"run", "--config", filepath.Join(t.TempDir(), "missing.yaml")}, 1, "config_load_failed"},
		{"bad output path", []string{"run", "--config", smokeConfigPath(), "--output", filepath.Join(t.TempDir(), "missing", "summary.json")}, 1, "result_write_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if code := runCLI(context.Background(), test.args, &stdout, &stderr); code != test.code {
				t.Fatalf("runCLI() code = %d, want %d; stderr=%s", code, test.code, stderr.String())
			}
			var envelope errorEnvelope
			if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil {
				t.Fatalf("stderr is not structured JSON: %v; stderr=%q", err, stderr.String())
			}
			if envelope.Error == nil || envelope.Error.Code != test.want {
				t.Fatalf("error = %+v, want code %q", envelope.Error, test.want)
			}
		})
	}
}

type brokenRuntime struct{}

func (brokenRuntime) Predict(context.Context, policyv1.Observation) ([]float64, error) {
	return nil, errors.New("injected runtime failure")
}

func TestRLFallbackCompletesSimulationWithoutInvariantFailures(t *testing.T) {
	configuration, err := config.Load(smokeConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	nodes, jobs, err := configuration.DomainValues()
	if err != nil {
		t.Fatal(err)
	}
	simulation, err := simulator.NewSimulation(nodes, jobs)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := inference.NewPolicy(policyv1.DefaultConfig(), brokenRuntime{}, firstFitPolicy{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	report, err := runSimulation(context.Background(), simulation, policy, metrics.NewCollector(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Completed != 3 || report.Decisions.InvalidDecisions != 0 || policy.Diagnostics().Fallbacks != 3 {
		t.Fatalf("report %+v, diagnostics %+v", report, policy.Diagnostics())
	}
	if err := simulation.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRLCLIWithRealModel(t *testing.T) {
	model := os.Getenv("SCHEDULER_TEST_MODEL")
	python := os.Getenv("SCHEDULER_TEST_PYTHON")
	if model == "" || python == "" {
		t.Skip("set real model and interpreter paths")
	}
	for _, budget := range []string{"1s", "1ns"} {
		t.Run(budget, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "bundle")
			var stdout, stderr bytes.Buffer
			code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath(), "--policy", "rl", "--model", model, "--python", python, "--inference-timeout", budget, "--results-dir", directory}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit %d: %s", code, stderr.String())
			}
			if err := results.ValidateBundle(directory); err != nil {
				t.Fatal(err)
			}
			var summary resultSummary
			if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Inference == nil {
				t.Fatal("missing inference diagnostics")
			}
			if budget == "1ns" && summary.Inference.Fallbacks == 0 {
				t.Fatal("deadline did not trigger fallback")
			}
		})
	}
}

func TestRunCLIHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	code := runCLI(ctx, []string{"run", "--config", smokeConfigPath()}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("runCLI() code = %d, want 1", code)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil {
		t.Fatalf("stderr is not structured JSON: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "simulation_failed" {
		t.Fatalf("error = %+v, want simulation_failed", envelope.Error)
	}
}

func TestRunCLIVersionAndHelp(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--version"}, {"help"}} {
		var stdout, stderr bytes.Buffer
		if code := runCLI(context.Background(), args, &stdout, &stderr); code != 0 {
			t.Fatalf("runCLI(%v) code = %d", args, code)
		}
		if stdout.Len() == 0 || stderr.Len() != 0 {
			t.Fatalf("runCLI(%v) stdout=%q stderr=%q", args, stdout.String(), stderr.String())
		}
	}
}

func TestFirstFitSerializesSameTimeFullNodeJobs(t *testing.T) {
	t.Parallel()
	node := domain.Node{
		ID:       "node-1",
		Capacity: domain.ResourceVector{CPUMillis: 1_000},
		State:    domain.NodeStateAvailable,
	}
	jobs := []domain.Job{
		{ID: "job-1", Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: time.Second, State: domain.JobStateQueued},
		{ID: "job-2", Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: time.Second, State: domain.JobStateQueued},
	}
	simulation, err := simulator.NewSimulation([]domain.Node{node}, jobs)
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	if err := runFirstFit(context.Background(), simulation); err != nil {
		t.Fatalf("runFirstFit() unexpected error: %v", err)
	}
	for _, job := range simulation.Jobs() {
		if job.State != domain.JobStateCompleted {
			t.Fatalf("job %q state = %q, want completed", job.ID, job.State)
		}
	}
	if simulation.Now() != 2*time.Second {
		t.Fatalf("simulation time = %s, want 2s", simulation.Now())
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestRunCLIReportsStdoutFailure(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"run", "--config", smokeConfigPath()}, failingWriter{}, &stderr)
	if code != 1 {
		t.Fatalf("runCLI() code = %d, want 1", code)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil {
		t.Fatalf("stderr is not structured JSON: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "result_write_failed" {
		t.Fatalf("error = %+v, want result_write_failed", envelope.Error)
	}
}
