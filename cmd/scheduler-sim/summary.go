package main

import (
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

type resultSummary struct {
	SchemaVersion  string        `json:"schema_version"`
	Policy         string        `json:"policy"`
	Seed           int64         `json:"seed"`
	SimulationTime string        `json:"simulation_time"`
	NodeCount      int           `json:"node_count"`
	Jobs           jobSummary    `json:"jobs"`
	Metrics        metricSummary `json:"metrics"`
}

type jobSummary struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Rejected  int `json:"rejected"`
}

type metricSummary struct {
	ThroughputPerSecond   float64                     `json:"throughput_jobs_per_second"`
	SuccessRate           float64                     `json:"success_rate"`
	RejectionRate         float64                     `json:"rejection_rate"`
	DeadlineJobs          uint64                      `json:"deadline_jobs"`
	DeadlineViolations    uint64                      `json:"deadline_violations"`
	DeadlineViolationRate float64                     `json:"deadline_violation_rate"`
	Latency               durationDistributionSummary `json:"latency"`
	QueueWait             durationDistributionSummary `json:"queue_wait"`
	MeanBoundedSlowdown   float64                     `json:"mean_bounded_slowdown"`
	SlowdownFairness      float64                     `json:"slowdown_fairness_jain"`
	Utilization           metrics.ResourceUtilization `json:"utilization"`
	MeanQueueDepth        float64                     `json:"mean_queue_depth"`
	ResourceFragmentation float64                     `json:"resource_fragmentation"`
	Scheduler             schedulerMetricSummary      `json:"scheduler"`
}

type durationDistributionSummary struct {
	Count uint64 `json:"count"`
	Min   string `json:"min"`
	Max   string `json:"max"`
	Mean  string `json:"mean"`
	P50   string `json:"p50"`
	P95   string `json:"p95"`
	P99   string `json:"p99"`
}

type schedulerMetricSummary struct {
	Decisions        uint64 `json:"decisions"`
	Placements       uint64 `json:"placements"`
	Deferred         uint64 `json:"deferred"`
	PolicyErrors     uint64 `json:"policy_errors"`
	InvalidDecisions uint64 `json:"invalid_decisions"`
	TotalDuration    string `json:"total_duration"`
	MeanDuration     string `json:"mean_duration"`
	MaxDuration      string `json:"max_duration"`
}

func buildSummary(policy string, seed int64, simulation *simulator.Simulation, report metrics.Report) resultSummary {
	jobs := simulation.Jobs()
	summary := resultSummary{
		SchemaVersion:  "v1",
		Policy:         policy,
		Seed:           seed,
		SimulationTime: simulation.Now().String(),
		NodeCount:      len(simulation.Nodes()),
		Jobs:           jobSummary{Total: len(jobs)},
		Metrics: metricSummary{
			ThroughputPerSecond:   report.ThroughputPerSecond,
			SuccessRate:           report.SuccessRate,
			RejectionRate:         report.RejectionRate,
			DeadlineJobs:          report.DeadlineJobs,
			DeadlineViolations:    report.DeadlineViolations,
			DeadlineViolationRate: report.DeadlineViolationRate,
			Latency:               distributionSummary(report.Latency),
			QueueWait:             distributionSummary(report.QueueWait),
			MeanBoundedSlowdown:   report.MeanBoundedSlowdown,
			SlowdownFairness:      report.SlowdownFairness,
			Utilization:           report.Utilization,
			MeanQueueDepth:        report.MeanQueueDepth,
			ResourceFragmentation: report.ResourceFragmentation,
			Scheduler: schedulerMetricSummary{
				Decisions:        report.Decisions.Count,
				Placements:       report.Decisions.Placements,
				Deferred:         report.Decisions.Deferred,
				PolicyErrors:     report.Decisions.PolicyErrors,
				InvalidDecisions: report.Decisions.InvalidDecisions,
				TotalDuration:    report.Decisions.TotalDuration.String(),
				MeanDuration:     report.Decisions.MeanDuration.String(),
				MaxDuration:      report.Decisions.MaxDuration.String(),
			},
		},
	}
	for _, job := range jobs {
		switch job.State {
		case domain.JobStateQueued:
			summary.Jobs.Queued++
		case domain.JobStateRunning:
			summary.Jobs.Running++
		case domain.JobStateCompleted:
			summary.Jobs.Completed++
		case domain.JobStateRejected:
			summary.Jobs.Rejected++
		}
	}
	return summary
}

func distributionSummary(distribution metrics.DurationDistribution) durationDistributionSummary {
	return durationDistributionSummary{
		Count: distribution.Count,
		Min:   distribution.Min.String(),
		Max:   distribution.Max.String(),
		Mean:  distribution.Mean.String(),
		P50:   distribution.P50.String(),
		P95:   distribution.P95.String(),
		P99:   distribution.P99.String(),
	}
}
