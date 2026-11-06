package main

import (
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/metrics"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/results"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

type detailRecorder struct {
	series []results.TimeSeriesRecord
}

func (r *detailRecorder) record(event simulator.Event, simulation *simulator.Simulation) error {
	measurement, err := metrics.MeasureCluster(simulation.Nodes())
	if err != nil {
		return err
	}
	row := results.TimeSeriesRecord{
		SimulationTime: simulation.Now().String(), EventSequence: event.Sequence, EventType: string(event.Type),
		CPUUtilization: measurement.Utilization.CPU, MemoryUtilization: measurement.Utilization.Memory,
		GPUUtilization: measurement.Utilization.GPU, DiskUtilization: measurement.Utilization.Ephemeral,
		Fragmentation: measurement.Fragmentation,
	}
	for _, job := range simulation.Jobs() {
		switch job.State {
		case domain.JobStateQueued:
			row.Queued++
		case domain.JobStateRunning:
			row.Running++
		case domain.JobStateCompleted:
			row.Completed++
		case domain.JobStateRejected:
			row.Rejected++
		}
	}
	r.series = append(r.series, row)
	return nil
}

func buildJobRecords(jobs []domain.Job) []results.JobRecord {
	records := make([]results.JobRecord, 0, len(jobs))
	for _, job := range jobs {
		record := results.JobRecord{
			ID: string(job.ID), State: string(job.State), AssignedNode: string(job.AssignedNode),
			SubmitTime: job.SubmitTime.String(), Duration: job.Duration.String(), Priority: job.Priority,
			CPUMillis: job.Request.CPUMillis, MemoryMiB: job.Request.MemoryMiB,
			GPUs: job.Request.GPUs, EphemeralDiskMiB: job.Request.EphemeralDiskMiB,
			RejectionReason: job.RejectionReason,
		}
		if job.StartTime != nil {
			record.StartTime = job.StartTime.String()
			record.QueueWait = (*job.StartTime - job.SubmitTime).String()
		}
		if job.EndTime != nil {
			record.EndTime = job.EndTime.String()
			record.Latency = (*job.EndTime - job.SubmitTime).String()
		}
		if job.Deadline != nil {
			record.Deadline = job.Deadline.String()
		}
		records = append(records, record)
	}
	return records
}
