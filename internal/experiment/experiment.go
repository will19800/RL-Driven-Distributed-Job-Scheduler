// Package experiment coordinates reproducible experiment matrices.
package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	SchemaVersion = "v1"
	maxSpecSize   = 1 << 20
	maxRuns       = 10_000
)

var (
	ErrInvalidSpec  = errors.New("invalid experiment specification")
	ErrOutputExists = errors.New("experiment output already exists")
)

type Spec struct {
	Inference     *InferenceConfig `yaml:"inference,omitempty"`
	SchemaVersion string           `yaml:"schema_version"`
	OutputDir     string           `yaml:"output_dir"`
	Workers       int              `yaml:"workers"`
	Seeds         []int64          `yaml:"seeds"`
	Policies      []string         `yaml:"policies"`
	Workloads     []Workload       `yaml:"workloads"`
}

type InferenceConfig struct {
	Model    string `yaml:"model"`
	Python   string `yaml:"python"`
	Timeout  string `yaml:"timeout"`
	Fallback string `yaml:"fallback"`
}

type Workload struct {
	Name   string `yaml:"name"`
	Config string `yaml:"config"`
}

type Matrix struct {
	SchemaVersion string      `json:"schema_version"`
	Runs          []RunRecord `json:"runs"`
}

type RunRecord struct {
	Workload string `json:"workload"`
	Config   string `json:"config"`
	Policy   string `json:"policy"`
	Seed     int64  `json:"seed"`
	Bundle   string `json:"bundle"`
}

type CellRunner interface {
	Run(context.Context, RunRecord, string) error
}

type CommandRunner struct {
	Binary    string
	Inference *InferenceConfig
}

func (r CommandRunner) Run(ctx context.Context, cell RunRecord, outputDir string) error {
	args := []string{"run", "--config", cell.Config, "--policy", cell.Policy, "--seed", strconv.FormatInt(cell.Seed, 10), "--results-dir", outputDir}
	if cell.Policy == "rl" {
		if r.Inference == nil {
			return errors.New("rl requires inference configuration")
		}
		args = append(args, "--model", r.Inference.Model, "--python", r.Inference.Python, "--inference-timeout", r.Inference.Timeout, "--fallback", r.Inference.Fallback)
	}
	command := exec.CommandContext(ctx, r.Binary, args...)
	// Give scheduler-sim a chance to cancel inference and reap its worker before
	// escalating to Kill. Interrupt is handled by the simulator's main context.
	command.Cancel = func() error { return command.Process.Signal(os.Interrupt) }
	command.WaitDelay = 5 * time.Second
	command.Stdout = io.Discard
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("scheduler-sim: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// Load reads a strict experiment YAML file and resolves its paths relative to
// the experiment file.
func Load(path string) (Spec, error) {
	file, err := os.Open(path)
	if err != nil {
		return Spec{}, fmt.Errorf("open experiment %q: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSpecSize+1))
	if err != nil {
		return Spec{}, fmt.Errorf("read experiment %q: %w", path, err)
	}
	if len(data) > maxSpecSize {
		return Spec{}, fmt.Errorf("%w: file exceeds 1 MiB", ErrInvalidSpec)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var spec Spec
	if err := decoder.Decode(&spec); err != nil {
		return Spec{}, fmt.Errorf("%w: decode YAML: %v", ErrInvalidSpec, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Spec{}, fmt.Errorf("%w: multiple YAML documents are not allowed", ErrInvalidSpec)
	}
	base := filepath.Dir(path)
	if spec.Inference != nil {
		if !filepath.IsAbs(spec.Inference.Model) {
			spec.Inference.Model = filepath.Join(base, spec.Inference.Model)
		}
		if spec.Inference.Python == "" {
			spec.Inference.Python = "python3"
		} else if strings.Contains(spec.Inference.Python, string(filepath.Separator)) && !filepath.IsAbs(spec.Inference.Python) {
			spec.Inference.Python = filepath.Join(base, spec.Inference.Python)
		}
		if spec.Inference.Timeout == "" {
			spec.Inference.Timeout = "250ms"
		}
		if spec.Inference.Fallback == "" {
			spec.Inference.Fallback = "bin-pack"
		}
	}
	if !filepath.IsAbs(spec.OutputDir) {
		spec.OutputDir = filepath.Clean(filepath.Join(base, spec.OutputDir))
	}
	for index := range spec.Workloads {
		if !filepath.IsAbs(spec.Workloads[index].Config) {
			spec.Workloads[index].Config = filepath.Clean(filepath.Join(base, spec.Workloads[index].Config))
		}
	}
	if err := spec.Validate(); err != nil {
		return Spec{}, err
	}
	return spec, nil
}

func (s Spec) Validate() error {
	if s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: schema_version must be %q", ErrInvalidSpec, SchemaVersion)
	}
	if s.OutputDir == "" {
		return fmt.Errorf("%w: output_dir is required", ErrInvalidSpec)
	}
	if s.Workers <= 0 || s.Workers > 256 {
		return fmt.Errorf("%w: workers must be between 1 and 256", ErrInvalidSpec)
	}
	if len(s.Seeds) == 0 || len(s.Policies) == 0 || len(s.Workloads) == 0 {
		return fmt.Errorf("%w: seeds, policies, and workloads must not be empty", ErrInvalidSpec)
	}
	if len(s.Seeds) > maxRuns/len(s.Policies) || len(s.Seeds)*len(s.Policies) > maxRuns/len(s.Workloads) {
		return fmt.Errorf("%w: matrix exceeds %d runs", ErrInvalidSpec, maxRuns)
	}
	if err := uniqueSeeds(s.Seeds); err != nil {
		return err
	}
	if err := uniqueStrings("policy", s.Policies); err != nil {
		return err
	}
	allowedPolicies := map[string]bool{"first-fit": true, "round-robin": true, "random-fit": true, "least-loaded": true, "bin-pack": true}
	for _, policy := range s.Policies {
		if policy == "rl" {
			if s.Inference == nil {
				return fmt.Errorf("%w: rl requires inference configuration", ErrInvalidSpec)
			}
			budget, err := time.ParseDuration(s.Inference.Timeout)
			if err != nil || budget <= 0 || !allowedPolicies[s.Inference.Fallback] || s.Inference.Python == "" {
				return fmt.Errorf("%w: invalid inference settings", ErrInvalidSpec)
			}
			if _, err := os.Stat(filepath.Join(s.Inference.Model, "metadata.json")); err != nil {
				return fmt.Errorf("%w: model metadata: %v", ErrInvalidSpec, err)
			}
			continue
		}
		if !allowedPolicies[policy] {
			return fmt.Errorf("%w: unsupported policy %q", ErrInvalidSpec, policy)
		}
	}
	names := make([]string, len(s.Workloads))
	for index, workload := range s.Workloads {
		names[index] = workload.Name
		if workload.Name == "" || workload.Config == "" {
			return fmt.Errorf("%w: workload %d requires name and config", ErrInvalidSpec, index)
		}
		if filepath.Base(workload.Name) != workload.Name || workload.Name == "." || workload.Name == ".." {
			return fmt.Errorf("%w: workload name %q is not path-safe", ErrInvalidSpec, workload.Name)
		}
		if _, err := os.Stat(workload.Config); err != nil {
			return fmt.Errorf("%w: workload %q config: %v", ErrInvalidSpec, workload.Name, err)
		}
	}
	return uniqueStrings("workload name", names)
}

// Run executes every paired workload/policy/seed cell with at most Workers
// active calls, then writes a deterministically ordered matrix.json index.
func Run(ctx context.Context, spec Spec, runner CellRunner) (Matrix, error) {
	if err := spec.Validate(); err != nil {
		return Matrix{}, err
	}
	if _, err := os.Stat(spec.OutputDir); err == nil {
		return Matrix{}, fmt.Errorf("%w: %s", ErrOutputExists, spec.OutputDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Matrix{}, err
	}
	if err := os.MkdirAll(spec.OutputDir, 0o755); err != nil {
		return Matrix{}, fmt.Errorf("create output: %w", err)
	}
	cells := expand(spec)
	jobs := make(chan RunRecord)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wait sync.WaitGroup
	var firstErr error
	var errorOnce sync.Once
	for range min(spec.Workers, len(cells)) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for cell := range jobs {
				output := filepath.Join(spec.OutputDir, filepath.FromSlash(cell.Bundle))
				if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
					errorOnce.Do(func() { firstErr = err; cancel() })
					return
				}
				if err := runner.Run(ctx, cell, output); err != nil {
					errorOnce.Do(func() {
						firstErr = fmt.Errorf("run %s seed=%d policy=%s: %w", cell.Workload, cell.Seed, cell.Policy, err)
						cancel()
					})
					return
				}
			}
		}()
	}
sendLoop:
	for _, cell := range cells {
		select {
		case jobs <- cell:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wait.Wait()
	if firstErr != nil {
		return Matrix{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return Matrix{}, err
	}
	matrix := Matrix{SchemaVersion: SchemaVersion, Runs: cells}
	data, err := json.MarshalIndent(matrix, "", "  ")
	if err != nil {
		return Matrix{}, err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(spec.OutputDir, "matrix.json"), data, 0o644); err != nil {
		return Matrix{}, fmt.Errorf("write matrix index: %w", err)
	}
	return matrix, nil
}

func expand(spec Spec) []RunRecord {
	var cells []RunRecord
	for _, workload := range spec.Workloads {
		for _, seed := range spec.Seeds {
			for _, policy := range spec.Policies {
				bundle := filepath.ToSlash(filepath.Join(workload.Name, "seed-"+strconv.FormatInt(seed, 10), policy))
				cells = append(cells, RunRecord{Workload: workload.Name, Config: workload.Config, Policy: policy, Seed: seed, Bundle: bundle})
			}
		}
	}
	sort.Slice(cells, func(i, j int) bool {
		a, b := cells[i], cells[j]
		if a.Workload != b.Workload {
			return a.Workload < b.Workload
		}
		if a.Seed != b.Seed {
			return a.Seed < b.Seed
		}
		return a.Policy < b.Policy
	})
	return cells
}

func uniqueStrings(kind string, values []string) error {
	seen := map[string]struct{}{}
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("%w: %s must not be empty", ErrInvalidSpec, kind)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%w: duplicate %s %q", ErrInvalidSpec, kind, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}
func uniqueSeeds(values []int64) error {
	seen := map[int64]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%w: duplicate seed %d", ErrInvalidSpec, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}
