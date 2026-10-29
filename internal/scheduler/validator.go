package scheduler

import (
	"fmt"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

// ValidateDecision verifies every placement against the snapshot and reserves
// capacity between placements. This makes a successful multi-placement
// decision safe to enqueue atomically from the scheduler's point of view.
func ValidateDecision(snapshot Snapshot, decision Decision) error {
	if decision.Defer && len(decision.Placements) != 0 {
		return invalidDecision("defer cannot include placements")
	}
	if !decision.Defer && len(decision.Placements) == 0 {
		return invalidDecision("decision must place work or explicitly defer")
	}

	jobs := make(map[domain.JobID]domain.Job)
	for _, job := range snapshot.jobs {
		jobs[job.ID] = job
	}
	nodes := make(map[domain.NodeID]domain.Node)
	for _, node := range snapshot.nodes {
		nodes[node.ID] = node
	}
	placed := make(map[domain.JobID]struct{}, len(decision.Placements))
	for index, placement := range decision.Placements {
		if err := placement.Validate(); err != nil {
			return invalidDecision("placement %d: %v", index, err)
		}
		if _, duplicate := placed[placement.JobID]; duplicate {
			return invalidDecision("placement %d: job %q appears more than once", index, placement.JobID)
		}
		job, exists := jobs[placement.JobID]
		if !exists {
			return invalidDecision("placement %d: job %q not found", index, placement.JobID)
		}
		if job.State != domain.JobStateQueued {
			return invalidDecision("placement %d: job %q is %s", index, job.ID, job.State)
		}
		if job.SubmitTime > snapshot.now {
			return invalidDecision("placement %d: job %q is not submitted", index, job.ID)
		}
		node, exists := nodes[placement.NodeID]
		if !exists {
			return invalidDecision("placement %d: node %q not found", index, placement.NodeID)
		}
		if !node.CanFit(job.Request) {
			return invalidDecision("placement %d: job %q does not fit node %q", index, job.ID, node.ID)
		}
		allocated, err := node.Allocated.Add(job.Request)
		if err != nil {
			return invalidDecision("placement %d: reserve node %q: %v", index, node.ID, err)
		}
		node.Allocated = allocated
		nodes[node.ID] = node
		placed[job.ID] = struct{}{}
	}
	return nil
}

func invalidDecision(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidDecision, fmt.Sprintf(format, args...))
}
