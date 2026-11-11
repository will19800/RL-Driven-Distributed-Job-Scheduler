package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingRunner struct {
	mu, release     sync.Mutex
	cells           []RunRecord
	active, maximum int
	failPolicy      string
}

func (r *recordingRunner) Run(ctx context.Context, cell RunRecord, output string) error {
	r.mu.Lock()
	r.cells = append(r.cells, cell)
	r.active++
	if r.active > r.maximum {
		r.maximum = r.active
	}
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.active--; r.mu.Unlock() }()
	if cell.Policy == r.failPolicy {
		return errors.New("injected failure")
	}
	select {
	case <-time.After(5 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := os.Mkdir(output, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(output, "marker"), []byte(cell.Workload), 0o600)
}

func validSpec(t *testing.T) Spec {
	t.Helper()
	directory := t.TempDir()
	config := filepath.Join(directory, "workload.yaml")
	if err := os.WriteFile(config, []byte("schema_version: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return Spec{SchemaVersion: SchemaVersion, OutputDir: filepath.Join(directory, "results"), Workers: 2, Seeds: []int64{2, 1}, Policies: []string{"round-robin", "first-fit"}, Workloads: []Workload{{Name: "steady", Config: config}, {Name: "burst", Config: config}}}
}

func TestRunExpandsDeterministicMatrixWithBoundedWorkers(t *testing.T) {
	t.Parallel()
	spec := validSpec(t)
	runner := &recordingRunner{}
	matrix, err := Run(context.Background(), spec, runner)
	if err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if len(matrix.Runs) != 8 {
		t.Fatalf("runs = %d, want 8", len(matrix.Runs))
	}
	if runner.maximum > spec.Workers || runner.maximum < 2 {
		t.Fatalf("maximum concurrency = %d, want 2", runner.maximum)
	}
	wantFirst := RunRecord{Workload: "burst", Config: spec.Workloads[1].Config, Policy: "first-fit", Seed: 1, Bundle: "burst/seed-1/first-fit"}
	if !reflect.DeepEqual(matrix.Runs[0], wantFirst) {
		t.Fatalf("first run = %+v, want %+v", matrix.Runs[0], wantFirst)
	}
	data, err := os.ReadFile(filepath.Join(spec.OutputDir, "matrix.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Matrix
	if err := json.Unmarshal(data, &persisted); err != nil || !reflect.DeepEqual(matrix, persisted) {
		t.Fatalf("persisted matrix = %+v, %v", persisted, err)
	}
	for _, cell := range matrix.Runs {
		if _, err := os.Stat(filepath.Join(spec.OutputDir, filepath.FromSlash(cell.Bundle), "marker")); err != nil {
			t.Fatalf("bundle %q: %v", cell.Bundle, err)
		}
	}
}

func TestRunRejectsExistingOutput(t *testing.T) {
	t.Parallel()
	spec := validSpec(t)
	if err := os.Mkdir(spec.OutputDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), spec, &recordingRunner{})
	if !errors.Is(err, ErrOutputExists) {
		t.Fatalf("Run() error = %v, want ErrOutputExists", err)
	}
}

func TestRunReturnsCellFailureWithoutIndex(t *testing.T) {
	t.Parallel()
	spec := validSpec(t)
	runner := &recordingRunner{failPolicy: "first-fit"}
	_, err := Run(context.Background(), spec, runner)
	if err == nil || !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("Run() error = %v, want injected failure", err)
	}
	if _, err := os.Stat(filepath.Join(spec.OutputDir, "matrix.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("matrix index error = %v, want not exist", err)
	}
}

func TestLoadResolvesPathsAndRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workload := filepath.Join(directory, "workload.yaml")
	if err := os.WriteFile(workload, []byte("schema_version: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	input := "schema_version: v1\noutput_dir: output\nworkers: 2\nseeds: [1, 2]\npolicies: [first-fit]\nworkloads:\n  - name: smoke\n    config: workload.yaml\n"
	path := filepath.Join(directory, "matrix.yaml")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err := Load(path)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if spec.OutputDir != filepath.Join(directory, "output") || spec.Workloads[0].Config != workload {
		t.Fatalf("resolved paths = %q, %q", spec.OutputDir, spec.Workloads[0].Config)
	}
	if err := os.WriteFile(path, []byte(input+"mystery: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("Load(unknown) error = %v, want ErrInvalidSpec", err)
	}
}

func TestValidateRejectsInvalidMatrices(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Spec){
		"version": func(s *Spec) { s.SchemaVersion = "v2" }, "workers": func(s *Spec) { s.Workers = 0 }, "empty seeds": func(s *Spec) { s.Seeds = nil },
		"duplicate seed": func(s *Spec) { s.Seeds = []int64{1, 1} }, "duplicate policy": func(s *Spec) { s.Policies = []string{"first-fit", "first-fit"} },
		"unsafe policy": func(s *Spec) { s.Policies = []string{"../escape"} }, "unsafe workload": func(s *Spec) { s.Workloads[0].Name = "../escape" },
		"duplicate workload": func(s *Spec) { s.Workloads = append(s.Workloads, s.Workloads[0]) }, "missing config": func(s *Spec) { s.Workloads[0].Config = filepath.Join(t.TempDir(), "missing") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			spec := validSpec(t)
			mutate(&spec)
			if err := spec.Validate(); !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("Validate() error = %v, want ErrInvalidSpec", err)
			}
		})
	}
}

func TestRLRequiresValidModelAndBudget(t *testing.T) {
	spec := validSpec(t)
	spec.Policies = []string{"rl", "bin-pack"}
	if err := spec.Validate(); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected missing model error, got %v", err)
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	spec.Inference = &InferenceConfig{Model: directory, Python: "python3", Timeout: "250ms", Fallback: "bin-pack"}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Inference.Timeout = "-1s"
	if err := spec.Validate(); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected timeout validation, got %v", err)
	}
}
