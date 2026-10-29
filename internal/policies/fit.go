package policies

import (
	"context"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

func fitCandidates(ctx context.Context, snapshot scheduler.Snapshot) (domain.Job, []domain.Node, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Job{}, nil, false, err
	}
	for _, job := range snapshot.Jobs() {
		if job.State != domain.JobStateQueued {
			continue
		}
		candidates := make([]domain.Node, 0)
		for _, node := range snapshot.Nodes() {
			if node.CanFit(job.Request) {
				candidates = append(candidates, node)
			}
		}
		if len(candidates) != 0 {
			return job, candidates, true, nil
		}
	}
	return domain.Job{}, nil, false, nil
}

func placement(job domain.Job, node domain.Node) scheduler.Decision {
	return scheduler.Decision{Placements: []domain.Placement{{JobID: job.ID, NodeID: node.ID}}}
}
