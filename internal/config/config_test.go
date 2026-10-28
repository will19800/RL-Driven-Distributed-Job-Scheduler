package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

func validYAML() string {
	return `schema_version: v1
seed: 42
cluster:
  nodes:
    - id: node-1
      type: general
      labels:
        zone: test-a
      capacity:
        cpu_millis: 4000
        memory_mib: 8192
        gpus: 1
        ephemeral_disk_mib: 10240
workload:
  jobs:
    - id: job-1
      submit_time: 250ms
      resources:
        cpu_millis: 500
        memory_mib: 1024
        gpus: 0
        ephemeral_disk_mib: 100
      duration: 5s
      priority: 2
      deadline: 10s
`
}

func TestDecodeAndConvertValidConfiguration(t *testing.T) {
	t.Parallel()
	configuration, err := Decode(strings.NewReader(validYAML()))
	if err != nil {
		t.Fatalf("Decode() unexpected error: %v", err)
	}
	if configuration.Seed != 42 {
		t.Fatalf("Seed = %d, want 42", configuration.Seed)
	}
	nodes, jobs, err := configuration.DomainValues()
	if err != nil {
		t.Fatalf("DomainValues() unexpected error: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != "node-1" || nodes[0].Labels["zone"] != "test-a" {
		t.Fatalf("nodes = %+v, want configured node", nodes)
	}
	if len(jobs) != 1 || jobs[0].SubmitTime != 250*time.Millisecond || jobs[0].Duration != 5*time.Second {
		t.Fatalf("jobs = %+v, want configured job", jobs)
	}
	if _, err := simulator.NewSimulation(nodes, jobs); err != nil {
		t.Fatalf("NewSimulation() from config: %v", err)
	}
}

func TestLoadSmokeConfiguration(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "configs", "experiments", "smoke.yaml")
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) unexpected error: %v", path, err)
	}
	nodes, jobs, err := configuration.DomainValues()
	if err != nil {
		t.Fatalf("DomainValues() unexpected error: %v", err)
	}
	if len(nodes) != 2 || len(jobs) != 3 {
		t.Fatalf("smoke sizes = %d nodes, %d jobs; want 2, 3", len(nodes), len(jobs))
	}
}

func TestLoadSyntheticConfigurationDeterministically(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "configs", "experiments", "synthetic-smoke.yaml")
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) unexpected error: %v", path, err)
	}
	_, first, err := configuration.DomainValues()
	if err != nil {
		t.Fatalf("DomainValues(first) unexpected error: %v", err)
	}
	_, second, err := configuration.DomainValues()
	if err != nil {
		t.Fatalf("DomainValues(second) unexpected error: %v", err)
	}
	if len(first) != 100 || !reflect.DeepEqual(first, second) {
		t.Fatalf("synthetic workload was not a deterministic 100-job sequence")
	}
}

func TestDecodeRejectsUnknownFieldsAtEveryLevel(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"root":     strings.Replace(validYAML(), "seed: 42", "seed: 42\nunknown: true", 1),
		"node":     strings.Replace(validYAML(), "      type: general", "      type: general\n      mystery: true", 1),
		"resource": strings.Replace(validYAML(), "        cpu_millis: 4000", "        cpu_millis: 4000\n        mystery: 1", 1),
		"job":      strings.Replace(validYAML(), "      duration: 5s", "      duration: 5s\n      mystery: true", 1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(strings.NewReader(input)); !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "field mystery") && name != "root" {
				t.Fatalf("Decode() error = %v, want unknown-field ErrInvalidConfig", err)
			}
		})
	}
}

func TestDecodeRejectsInvalidDocuments(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"empty":              "",
		"multiple documents": validYAML() + "---\n" + validYAML(),
		"wrong version":      strings.Replace(validYAML(), "schema_version: v1", "schema_version: v2", 1),
		"numeric duration":   strings.Replace(validYAML(), "duration: 5s", "duration: 5", 1),
		"bad duration":       strings.Replace(validYAML(), "duration: 5s", "duration: forever", 1),
		"zero duration":      strings.Replace(validYAML(), "duration: 5s", "duration: 0s", 1),
		"negative resource":  strings.Replace(validYAML(), "cpu_millis: 500", "cpu_millis: -1", 1),
		"early deadline":     strings.Replace(validYAML(), "deadline: 10s", "deadline: 1ms", 1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(strings.NewReader(input)); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Decode() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestValidateRejectsDuplicateIDsAndEmptyCollections(t *testing.T) {
	t.Parallel()
	configuration, err := Decode(strings.NewReader(validYAML()))
	if err != nil {
		t.Fatalf("Decode() unexpected error: %v", err)
	}
	tests := map[string]func(*Config){
		"duplicate node": func(c *Config) { c.Cluster.Nodes = append(c.Cluster.Nodes, c.Cluster.Nodes[0]) },
		"duplicate job":  func(c *Config) { c.Workload.Jobs = append(c.Workload.Jobs, c.Workload.Jobs[0]) },
		"no nodes":       func(c *Config) { c.Cluster.Nodes = nil },
		"no jobs":        func(c *Config) { c.Workload.Jobs = nil },
		"both workload sources": func(c *Config) {
			c.Workload.Synthetic = &SyntheticWorkloadConfig{}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := configuration
			candidate.Cluster.Nodes = append([]NodeConfig(nil), configuration.Cluster.Nodes...)
			candidate.Workload.Jobs = append([]JobConfig(nil), configuration.Workload.Jobs...)
			mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestValidateRejectsInvalidSyntheticWorkload(t *testing.T) {
	t.Parallel()
	configuration, err := Decode(strings.NewReader(validYAML()))
	if err != nil {
		t.Fatalf("Decode() unexpected error: %v", err)
	}
	configuration.Workload.Jobs = nil
	configuration.Workload.Synthetic = &SyntheticWorkloadConfig{
		Count:                1,
		ArrivalRatePerSecond: 1,
		Duration: SyntheticDurationConfig{
			Median: Duration{Duration: time.Second},
			Min:    Duration{Duration: time.Second},
			Max:    Duration{Duration: time.Second},
		},
	}
	if err := configuration.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
	}
}

func TestDecodeEnforcesSizeLimit(t *testing.T) {
	t.Parallel()
	input := strings.NewReader(strings.Repeat("#", maxConfigSize+1))
	if _, err := Decode(input); !errors.Is(err, ErrConfigTooLarge) {
		t.Fatalf("Decode() error = %v, want ErrConfigTooLarge", err)
	}
}

func TestLoadReportsOpenError(t *testing.T) {
	t.Parallel()
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() error = %v, want os.ErrNotExist", err)
	}
}

func TestDomainValuesReturnDefensiveLabels(t *testing.T) {
	t.Parallel()
	configuration, err := Decode(strings.NewReader(validYAML()))
	if err != nil {
		t.Fatalf("Decode() unexpected error: %v", err)
	}
	nodes, _, err := configuration.DomainValues()
	if err != nil {
		t.Fatalf("DomainValues() unexpected error: %v", err)
	}
	nodes[0].Labels["zone"] = "changed"
	if configuration.Cluster.Nodes[0].Labels["zone"] != "test-a" {
		t.Fatal("DomainValues() exposed configuration labels")
	}
}
