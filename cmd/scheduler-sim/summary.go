package main

import (
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/simulator"
)

type resultSummary struct {
	SchemaVersion  string     `json:"schema_version"`
	Policy         string     `json:"policy"`
	Seed           int64      `json:"seed"`
	SimulationTime string     `json:"simulation_time"`
	NodeCount      int        `json:"node_count"`
	Jobs           jobSummary `json:"jobs"`
}

type jobSummary struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Rejected  int `json:"rejected"`
}

func buildSummary(policy string, seed int64, simulation *simulator.Simulation) resultSummary {
	jobs := simulation.Jobs()
	summary := resultSummary{
		SchemaVersion:  "v1",
		Policy:         policy,
		Seed:           seed,
		SimulationTime: simulation.Now().String(),
		NodeCount:      len(simulation.Nodes()),
		Jobs:           jobSummary{Total: len(jobs)},
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
