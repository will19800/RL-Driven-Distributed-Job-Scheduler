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

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

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
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("summary mismatch\n--- got ---\n%s--- want ---\n%s", stdout.Bytes(), want)
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
