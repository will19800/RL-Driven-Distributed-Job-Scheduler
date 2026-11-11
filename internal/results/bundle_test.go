package results

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/inference"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
)

func validSummary() Summary {
	zeroDistribution := DurationDistribution{Min: "0s", Max: "0s", Mean: "0s", P50: "0s", P95: "0s", P99: "0s"}
	return Summary{
		SchemaVersion: SchemaVersion, Policy: "first-fit", Seed: 42, SimulationTime: "1s", NodeCount: 1,
		Jobs: JobSummary{Total: 1, Completed: 1},
		Metrics: MetricSummary{
			SuccessRate: 1, Latency: zeroDistribution, QueueWait: zeroDistribution,
			MeanBoundedSlowdown: 1, SlowdownFairness: 1,
			Scheduler: SchedulerMetrics{TotalDuration: "0s", MeanDuration: "0s", MaxDuration: "0s"},
		},
	}
}

func TestRLBundleProvenanceAndDiagnosticValidation(t *testing.T) {
	manifest, summary, jobs, series := validBundleInputs()
	manifest.Policy = "rl"
	summary.Policy = "rl"
	contract, _ := policyv1.DefaultConfig().Schema()
	hash := strings.Repeat("a", 64)
	manifest.Model = &inference.Metadata{Format: "inference/v1", Runtime: "torchscript", Schema: contract, Torch: "test", ModelSHA256: hash, CheckpointSHA256: hash, ParitySHA256: hash}
	manifest.InferenceTimeout = "250ms"
	manifest.FallbackPolicy = "bin-pack"
	summary.Inference = &inference.Diagnostics{Decisions: 2, ModelCalls: 1, Fallbacks: 1, Reasons: map[string]uint64{"runtime": 1}}
	directory := filepath.Join(t.TempDir(), "rl")
	if err := WriteBundle(directory, manifest, summary, jobs, series); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBundle(directory); err != nil {
		t.Fatal(err)
	}
	summary.Inference.Reasons["runtime"] = 2
	if err := validateSummary(summary); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("counter mismatch accepted: %v", err)
	}
	manifest.Model.Format = "bad"
	if err := validateManifest(manifest); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("invalid model provenance accepted: %v", err)
	}
}

func validBundleInputs() (Manifest, Summary, []JobRecord, []TimeSeriesRecord) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	manifest := NewManifest("configs/test.yaml", []byte("schema_version: v1\n"), "first-fit", 42, "test", started, started.Add(time.Second))
	jobs := []JobRecord{{
		ID: "job-1", State: "completed", AssignedNode: "node-1", SubmitTime: "0s", StartTime: "0s",
		EndTime: "1s", Duration: "1s", QueueWait: "0s", Latency: "1s", CPUMillis: 1,
	}}
	series := []TimeSeriesRecord{{SimulationTime: "0s", EventType: "initial"}}
	return manifest, validSummary(), jobs, series
}

func TestWriteAndValidateBundle(t *testing.T) {
	t.Parallel()
	manifest, summary, jobs, series := validBundleInputs()
	directory := filepath.Join(t.TempDir(), "nested", "bundle")
	if err := WriteBundle(directory, manifest, summary, jobs, series); err != nil {
		t.Fatalf("WriteBundle() unexpected error: %v", err)
	}
	if err := ValidateBundle(directory); err != nil {
		t.Fatalf("ValidateBundle() unexpected error: %v", err)
	}
	var stored Manifest
	if err := decodeStrictFile(filepath.Join(directory, manifestName), &stored); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if stored.RunID == "" || !validHash(stored.Configuration.SHA256) || len(stored.Outputs) != 3 {
		t.Fatalf("manifest provenance = %+v", stored)
	}
	if err := WriteBundle(directory, manifest, summary, jobs, series); !errors.Is(err, ErrBundleExists) {
		t.Fatalf("second WriteBundle() error = %v, want ErrBundleExists", err)
	}
}

func TestValidateBundleDetectsTampering(t *testing.T) {
	t.Parallel()
	manifest, summary, jobs, series := validBundleInputs()
	directory := filepath.Join(t.TempDir(), "bundle")
	if err := WriteBundle(directory, manifest, summary, jobs, series); err != nil {
		t.Fatalf("WriteBundle() unexpected error: %v", err)
	}
	path := filepath.Join(directory, summaryName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, ' '), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBundle(directory); !errors.Is(err, ErrInvalidBundle) || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("ValidateBundle() error = %v, want hash mismatch", err)
	}
}

func TestValidateBundleRejectsUnknownManifestFields(t *testing.T) {
	t.Parallel()
	manifest, summary, jobs, series := validBundleInputs()
	directory := filepath.Join(t.TempDir(), "bundle")
	if err := WriteBundle(directory, manifest, summary, jobs, series); err != nil {
		t.Fatalf("WriteBundle() unexpected error: %v", err)
	}
	path := filepath.Join(directory, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	modified := strings.TrimSpace(string(data))
	modified = strings.TrimSuffix(modified, "}") + ",\n  \"unknown\": true\n}\n"
	if err := os.WriteFile(path, []byte(modified), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBundle(directory); !errors.Is(err, ErrInvalidBundle) || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ValidateBundle() error = %v, want unknown field", err)
	}
}

func TestWriteBundleRejectsInvalidSummaryBeforeCreatingDirectory(t *testing.T) {
	t.Parallel()
	manifest, summary, jobs, series := validBundleInputs()
	summary.SchemaVersion = "v2"
	directory := filepath.Join(t.TempDir(), "bundle")
	if err := WriteBundle(directory, manifest, summary, jobs, series); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("WriteBundle() error = %v, want ErrInvalidBundle", err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid bundle directory exists: %v", err)
	}
}

func TestCSVSemanticValidation(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	jobsPath := filepath.Join(directory, jobsName)
	if err := os.WriteFile(jobsPath, []byte(strings.Join(jobHeader, ",")+"\njob,bad,,,,,,,,,0,0,0,0,0,\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := validateCSV(jobsPath, jobHeader, validateJobRow); err == nil {
		t.Fatal("validateCSV(invalid job) error = nil")
	}
}
