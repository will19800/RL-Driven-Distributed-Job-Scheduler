package scheduler

import (
	"fmt"
	"sort"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

// Snapshot is an immutable view of the schedulable cluster state. Its fields
// are private and every accessor returns a defensive copy so policies cannot
// mutate simulator-owned state or state observed by another policy.
type Snapshot struct {
	now   time.Duration
	nodes []domain.Node
	jobs  []domain.Job
}

// NewSnapshot validates and copies the current cluster state. Nodes are sorted
// by ID; jobs are sorted by priority, submission time, and ID to provide stable
// policy inputs regardless of map iteration order.
func NewSnapshot(now time.Duration, nodes []domain.Node, jobs []domain.Job) (Snapshot, error) {
	if now < 0 {
		return Snapshot{}, fmt.Errorf("snapshot time must be non-negative: %s", now)
	}
	snapshot := Snapshot{now: now, nodes: cloneNodes(nodes), jobs: cloneJobs(jobs)}
	nodeIDs := make(map[domain.NodeID]struct{}, len(snapshot.nodes))
	for _, node := range snapshot.nodes {
		if err := node.Validate(); err != nil {
			return Snapshot{}, fmt.Errorf("node %q: %w", node.ID, err)
		}
		if _, exists := nodeIDs[node.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate node %q", node.ID)
		}
		nodeIDs[node.ID] = struct{}{}
	}
	jobIDs := make(map[domain.JobID]struct{}, len(snapshot.jobs))
	for _, job := range snapshot.jobs {
		if err := job.Validate(); err != nil {
			return Snapshot{}, fmt.Errorf("job %q: %w", job.ID, err)
		}
		if _, exists := jobIDs[job.ID]; exists {
			return Snapshot{}, fmt.Errorf("duplicate job %q", job.ID)
		}
		jobIDs[job.ID] = struct{}{}
	}
	sort.Slice(snapshot.nodes, func(i, j int) bool { return snapshot.nodes[i].ID < snapshot.nodes[j].ID })
	sort.SliceStable(snapshot.jobs, func(i, j int) bool {
		if snapshot.jobs[i].Priority != snapshot.jobs[j].Priority {
			return snapshot.jobs[i].Priority > snapshot.jobs[j].Priority
		}
		if snapshot.jobs[i].SubmitTime != snapshot.jobs[j].SubmitTime {
			return snapshot.jobs[i].SubmitTime < snapshot.jobs[j].SubmitTime
		}
		return snapshot.jobs[i].ID < snapshot.jobs[j].ID
	})
	return snapshot, nil
}

// Now returns the current simulated time.
func (s Snapshot) Now() time.Duration { return s.now }

// Nodes returns defensive copies in stable node-ID order.
func (s Snapshot) Nodes() []domain.Node { return cloneNodes(s.nodes) }

// Jobs returns defensive copies in stable scheduling order.
func (s Snapshot) Jobs() []domain.Job { return cloneJobs(s.jobs) }

func cloneNodes(nodes []domain.Node) []domain.Node {
	clones := make([]domain.Node, len(nodes))
	for i, node := range nodes {
		clones[i] = node
		if node.Labels != nil {
			clones[i].Labels = make(map[string]string, len(node.Labels))
			for key, value := range node.Labels {
				clones[i].Labels[key] = value
			}
		}
	}
	return clones
}

func cloneJobs(jobs []domain.Job) []domain.Job {
	clones := make([]domain.Job, len(jobs))
	for i, job := range jobs {
		clones[i] = job
		if job.Deadline != nil {
			value := *job.Deadline
			clones[i].Deadline = &value
		}
		if job.StartTime != nil {
			value := *job.StartTime
			clones[i].StartTime = &value
		}
		if job.EndTime != nil {
			value := *job.EndTime
			clones[i].EndTime = &value
		}
	}
	return clones
}
