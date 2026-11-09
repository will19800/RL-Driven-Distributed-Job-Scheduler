// Package policyv1 defines the language-neutral reinforcement-learning policy contract.
package policyv1

import (
	"errors"
	"fmt"
	"math"
)

const Version = "policy/v1"

var ErrInvalidContract = errors.New("invalid policy/v1 contract")

var nodeFeatureOrder = []string{"free_cpu", "free_memory", "free_gpu", "free_disk", "total_cpu", "total_memory", "total_gpu", "total_disk", "available", "utilization_cpu", "utilization_memory", "utilization_gpu", "utilization_disk"}
var jobFeatureOrder = []string{"request_cpu", "request_memory", "request_gpu", "request_disk", "age", "duration", "priority", "deadline_slack", "has_deadline"}
var globalFeatureOrder = []string{"queue_length", "utilization_cpu", "utilization_memory", "utilization_gpu", "utilization_disk", "arrival_rate", "simulation_time"}

type Normalization struct {
	CPUMillis             float64 `json:"cpu_millis"`
	MemoryMiB             float64 `json:"memory_mib"`
	GPUs                  float64 `json:"gpus"`
	EphemeralDiskMiB      float64 `json:"ephemeral_disk_mib"`
	DurationSeconds       float64 `json:"duration_seconds"`
	Priority              float64 `json:"priority"`
	QueueLength           float64 `json:"queue_length"`
	ArrivalRate           float64 `json:"arrival_rate"`
	SimulationTimeSeconds float64 `json:"simulation_time_seconds"`
}

type Config struct {
	MaxJobs       int           `json:"max_jobs"`
	MaxNodes      int           `json:"max_nodes"`
	Normalization Normalization `json:"normalization"`
}

func DefaultConfig() Config {
	return Config{MaxJobs: 32, MaxNodes: 64, Normalization: Normalization{CPUMillis: 64_000, MemoryMiB: 262_144, GPUs: 8, EphemeralDiskMiB: 1_048_576, DurationSeconds: 3_600, Priority: 100, QueueLength: 1_000, ArrivalRate: 100, SimulationTimeSeconds: 86_400}}
}

func (c Config) Validate() error {
	if c.MaxJobs <= 0 || c.MaxJobs > 10_000 || c.MaxNodes <= 0 || c.MaxNodes > 10_000 || c.MaxJobs > 1_000_000/c.MaxNodes {
		return fmt.Errorf("%w: candidate dimensions must be positive and action count at most 1000000", ErrInvalidContract)
	}
	n := c.Normalization
	values := []float64{n.CPUMillis, n.MemoryMiB, n.GPUs, n.EphemeralDiskMiB, n.DurationSeconds, n.Priority, n.QueueLength, n.ArrivalRate, n.SimulationTimeSeconds}
	for _, value := range values {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%w: normalization constants must be positive and finite", ErrInvalidContract)
		}
	}
	return nil
}

type Schema struct {
	Version              string        `json:"version"`
	MaxJobs              int           `json:"max_jobs"`
	MaxNodes             int           `json:"max_nodes"`
	NodeFeatures         []string      `json:"node_features"`
	JobFeatures          []string      `json:"job_features"`
	GlobalFeatures       []string      `json:"global_features"`
	Normalization        Normalization `json:"normalization"`
	PlacementActionCount int           `json:"placement_action_count"`
	DeferAction          int           `json:"defer_action"`
}

func (c Config) Schema() (Schema, error) {
	if err := c.Validate(); err != nil {
		return Schema{}, err
	}
	count := c.MaxJobs * c.MaxNodes
	return Schema{Version: Version, MaxJobs: c.MaxJobs, MaxNodes: c.MaxNodes, NodeFeatures: append([]string(nil), nodeFeatureOrder...), JobFeatures: append([]string(nil), jobFeatureOrder...), GlobalFeatures: append([]string(nil), globalFeatureOrder...), Normalization: c.Normalization, PlacementActionCount: count, DeferAction: count}, nil
}
