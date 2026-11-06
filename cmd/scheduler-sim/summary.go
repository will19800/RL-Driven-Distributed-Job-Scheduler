package main

import (
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

type resultSummary = results.Summary

func buildSummary(policy string, seed int64, simulation *simulator.Simulation, report metrics.Report) resultSummary {
	jobs := simulation.Jobs()
	summary := results.Summary{
		SchemaVersion:  results.SchemaVersion,
		Policy:         policy,
		Seed:           seed,
		SimulationTime: simulation.Now().String(),
		NodeCount:      len(simulation.Nodes()),
		Jobs:           results.JobSummary{Total: len(jobs)},
		Metrics: results.MetricSummary{
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
			Scheduler: results.SchedulerMetrics{
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

func distributionSummary(distribution metrics.DurationDistribution) results.DurationDistribution {
	return results.DurationDistribution{
		Count: distribution.Count,
		Min:   distribution.Min.String(),
		Max:   distribution.Max.String(),
		Mean:  distribution.Mean.String(),
		P50:   distribution.P50.String(),
		P95:   distribution.P95.String(),
		P99:   distribution.P99.String(),
	}
}
