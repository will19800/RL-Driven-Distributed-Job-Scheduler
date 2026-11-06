package main

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
)

func TestRunUsageVersionAndInvalidBundle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"usage", nil, 2},
		{"version", []string{"--version"}, 0},
		{"invalid", []string{filepath.Join(t.TempDir(), "missing")}, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if got := run(test.args, &stdout, &stderr); got != test.want {
				t.Fatalf("run() = %d, want %d; stdout=%q stderr=%q", got, test.want, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunValidatesBundle(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "bundle")
	started := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	manifest := results.NewManifest("test.yaml", []byte("test"), "first-fit", 1, "test", started, started)
	zero := results.DurationDistribution{Min: "0s", Max: "0s", Mean: "0s", P50: "0s", P95: "0s", P99: "0s"}
	summary := results.Summary{
		SchemaVersion: results.SchemaVersion, Policy: "first-fit", Seed: 1, SimulationTime: "0s",
		Metrics: results.MetricSummary{
			Latency: zero, QueueWait: zero,
			Scheduler: results.SchedulerMetrics{TotalDuration: "0s", MeanDuration: "0s", MaxDuration: "0s"},
		},
	}
	if err := results.WriteBundle(directory, manifest, summary, nil, nil); err != nil {
		t.Fatalf("WriteBundle() unexpected error: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{directory}, &stdout, &stderr); code != 0 {
		t.Fatalf("run() = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}
