package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"gopkg.in/yaml.v3"
)

const (
	// SchemaVersion is the only configuration schema currently supported.
	SchemaVersion = "v1"
	maxConfigSize = 1 << 20
)

var (
	// ErrInvalidConfig indicates malformed or semantically invalid configuration.
	ErrInvalidConfig = errors.New("invalid configuration")
	// ErrConfigTooLarge indicates input exceeded the bounded decoder size.
	ErrConfigTooLarge = errors.New("configuration exceeds 1 MiB limit")
)

// Duration is a time.Duration with strict YAML string decoding such as "250ms"
// or "2m". Numeric durations are intentionally rejected to avoid unit mistakes.
type Duration struct {
	time.Duration
}

// UnmarshalYAML parses a scalar duration string.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("duration must be a string such as \"5s\"")
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	d.Duration = parsed
	return nil
}

// Config is the root of the v1 simulation configuration schema.
type Config struct {
	SchemaVersion string         `yaml:"schema_version"`
	Seed          int64          `yaml:"seed"`
	Cluster       ClusterConfig  `yaml:"cluster"`
	Workload      WorkloadConfig `yaml:"workload"`
}

// ClusterConfig defines the initial cluster.
type ClusterConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

// NodeConfig defines one initially empty, available node.
type NodeConfig struct {
	ID       string                `yaml:"id"`
	Type     string                `yaml:"type,omitempty"`
	Labels   map[string]string     `yaml:"labels,omitempty"`
	Capacity domain.ResourceVector `yaml:"capacity"`
}

// WorkloadConfig defines explicit jobs. Generated workloads are added in the
// workload milestone without changing the v1 root contract.
type WorkloadConfig struct {
	Jobs []JobConfig `yaml:"jobs"`
}

// JobConfig defines one queued, non-preemptive job.
type JobConfig struct {
	ID         string                `yaml:"id"`
	SubmitTime Duration              `yaml:"submit_time"`
	Resources  domain.ResourceVector `yaml:"resources"`
	Duration   Duration              `yaml:"duration"`
	Priority   int32                 `yaml:"priority,omitempty"`
	Deadline   *Duration             `yaml:"deadline,omitempty"`
}

// Load reads and decodes one configuration file.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open configuration %q: %w", path, err)
	}
	defer file.Close()

	configuration, err := Decode(file)
	if err != nil {
		return Config{}, fmt.Errorf("decode configuration %q: %w", path, err)
	}
	return configuration, nil
}

// Decode reads one bounded YAML document, rejects unknown fields, and validates
// all schema and domain invariants.
func Decode(reader io.Reader) (Config, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxConfigSize+1))
	if err != nil {
		return Config{}, fmt.Errorf("%w: read YAML: %v", ErrInvalidConfig, err)
	}
	if len(data) > maxConfigSize {
		return Config{}, ErrConfigTooLarge
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var configuration Config
	if err := decoder.Decode(&configuration); err != nil {
		return Config{}, fmt.Errorf("%w: decode YAML: %v", ErrInvalidConfig, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, fmt.Errorf("%w: multiple YAML documents are not allowed", ErrInvalidConfig)
		}
		return Config{}, fmt.Errorf("%w: trailing YAML: %v", ErrInvalidConfig, err)
	}
	if err := configuration.Validate(); err != nil {
		return Config{}, err
	}
	return configuration, nil
}

// Validate checks schema and domain invariants without modifying the config.
func (c Config) Validate() error {
	_, _, err := c.DomainValues()
	return err
}

// DomainValues converts a validated configuration into defensive domain values.
func (c Config) DomainValues() ([]domain.Node, []domain.Job, error) {
	if c.SchemaVersion != SchemaVersion {
		return nil, nil, fmt.Errorf("%w: schema_version must be %q", ErrInvalidConfig, SchemaVersion)
	}
	if len(c.Cluster.Nodes) == 0 {
		return nil, nil, fmt.Errorf("%w: cluster.nodes must not be empty", ErrInvalidConfig)
	}
	if len(c.Workload.Jobs) == 0 {
		return nil, nil, fmt.Errorf("%w: workload.jobs must not be empty", ErrInvalidConfig)
	}

	nodes := make([]domain.Node, 0, len(c.Cluster.Nodes))
	nodeIDs := make(map[domain.NodeID]struct{}, len(c.Cluster.Nodes))
	for index, input := range c.Cluster.Nodes {
		node := domain.Node{
			ID:       domain.NodeID(input.ID),
			Type:     input.Type,
			Labels:   cloneLabels(input.Labels),
			Capacity: input.Capacity,
			State:    domain.NodeStateAvailable,
		}
		if err := node.Validate(); err != nil {
			return nil, nil, fmt.Errorf("%w: cluster.nodes[%d]: %v", ErrInvalidConfig, index, err)
		}
		if _, exists := nodeIDs[node.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate node id %q", ErrInvalidConfig, node.ID)
		}
		nodeIDs[node.ID] = struct{}{}
		nodes = append(nodes, node)
	}

	jobs := make([]domain.Job, 0, len(c.Workload.Jobs))
	jobIDs := make(map[domain.JobID]struct{}, len(c.Workload.Jobs))
	for index, input := range c.Workload.Jobs {
		job := domain.Job{
			ID:         domain.JobID(input.ID),
			SubmitTime: input.SubmitTime.Duration,
			Request:    input.Resources,
			Duration:   input.Duration.Duration,
			Priority:   input.Priority,
			State:      domain.JobStateQueued,
		}
		if input.Deadline != nil {
			deadline := input.Deadline.Duration
			job.Deadline = &deadline
		}
		if err := job.Validate(); err != nil {
			return nil, nil, fmt.Errorf("%w: workload.jobs[%d]: %v", ErrInvalidConfig, index, err)
		}
		if _, exists := jobIDs[job.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate job id %q", ErrInvalidConfig, job.ID)
		}
		jobIDs[job.ID] = struct{}{}
		jobs = append(jobs, job)
	}
	return nodes, jobs, nil
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	copy := make(map[string]string, len(labels))
	for key, value := range labels {
		copy[key] = value
	}
	return copy
}
