// Package results defines and validates versioned simulation result bundles.
package results

import (
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/inference"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
)

// SchemaVersion identifies the result bundle contract.
const SchemaVersion = "v1"

// Summary is the aggregate result schema.
type Summary struct {
	Inference      *inference.Diagnostics `json:"inference,omitempty"`
	SchemaVersion  string                 `json:"schema_version"`
	Policy         string                 `json:"policy"`
	Seed           int64                  `json:"seed"`
	SimulationTime string                 `json:"simulation_time"`
	NodeCount      int                    `json:"node_count"`
	Jobs           JobSummary             `json:"jobs"`
	Metrics        MetricSummary          `json:"metrics"`
}

type JobSummary struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Rejected  int `json:"rejected"`
}

// MetricSummary is the public aggregate metrics schema.
type MetricSummary struct {
	ThroughputPerSecond   float64                     `json:"throughput_jobs_per_second"`
	SuccessRate           float64                     `json:"success_rate"`
	RejectionRate         float64                     `json:"rejection_rate"`
	DeadlineJobs          uint64                      `json:"deadline_jobs"`
	DeadlineViolations    uint64                      `json:"deadline_violations"`
	DeadlineViolationRate float64                     `json:"deadline_violation_rate"`
	Latency               DurationDistribution        `json:"latency"`
	QueueWait             DurationDistribution        `json:"queue_wait"`
	MeanBoundedSlowdown   float64                     `json:"mean_bounded_slowdown"`
	SlowdownFairness      float64                     `json:"slowdown_fairness_jain"`
	Utilization           metrics.ResourceUtilization `json:"utilization"`
	MeanQueueDepth        float64                     `json:"mean_queue_depth"`
	ResourceFragmentation float64                     `json:"resource_fragmentation"`
	Scheduler             SchedulerMetrics            `json:"scheduler"`
}

type DurationDistribution struct {
	Count uint64 `json:"count"`
	Min   string `json:"min"`
	Max   string `json:"max"`
	Mean  string `json:"mean"`
	P50   string `json:"p50"`
	P95   string `json:"p95"`
	P99   string `json:"p99"`
}

type SchedulerMetrics struct {
	Decisions        uint64 `json:"decisions"`
	Placements       uint64 `json:"placements"`
	Deferred         uint64 `json:"deferred"`
	PolicyErrors     uint64 `json:"policy_errors"`
	InvalidDecisions uint64 `json:"invalid_decisions"`
	TotalDuration    string `json:"total_duration"`
	MeanDuration     string `json:"mean_duration"`
	MaxDuration      string `json:"max_duration"`
}

// Manifest records run identity, inputs, provenance, and output hashes.
type Manifest struct {
	Model            *inference.Metadata `json:"model,omitempty"`
	InferenceTimeout string              `json:"inference_timeout,omitempty"`
	FallbackPolicy   string              `json:"fallback_policy,omitempty"`
	SchemaVersion    string              `json:"schema_version"`
	RunID            string              `json:"run_id"`
	StartedAt        time.Time           `json:"started_at"`
	FinishedAt       time.Time           `json:"finished_at"`
	Policy           string              `json:"policy"`
	Seed             int64               `json:"seed"`
	CommandVersion   string              `json:"command_version"`
	Configuration    InputArtifact       `json:"configuration"`
	Git              GitProvenance       `json:"git"`
	Runtime          RuntimeProvenance   `json:"runtime"`
	Dependencies     []Dependency        `json:"dependencies"`
	Outputs          []OutputArtifact    `json:"outputs"`
}

type InputArtifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type OutputArtifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Rows   int    `json:"rows,omitempty"`
}

type GitProvenance struct {
	Revision  string `json:"revision,omitempty"`
	Modified  bool   `json:"modified"`
	Available bool   `json:"available"`
}

type RuntimeProvenance struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

type Dependency struct {
	Path    string `json:"path"`
	Version string `json:"version"`
}

// JobRecord is one jobs.csv row.
type JobRecord struct {
	ID, State, AssignedNode, SubmitTime, StartTime, EndTime, Duration, QueueWait, Latency, Deadline, RejectionReason string
	Priority                                                                                                         int32
	CPUMillis, MemoryMiB, GPUs, EphemeralDiskMiB                                                                     int64
}

// TimeSeriesRecord is one event-aligned timeseries.csv row.
type TimeSeriesRecord struct {
	SimulationTime                                                                    string
	EventSequence                                                                     uint64
	EventType                                                                         string
	Queued, Running, Completed, Rejected                                              int
	CPUUtilization, MemoryUtilization, GPUUtilization, DiskUtilization, Fragmentation float64
}
