package policies

import (
	"context"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

// LeastLoaded selects the feasible node with the lowest mean normalized usage
// across the resource dimensions requested by the chosen job. Equal scores are
// resolved by the snapshot's stable node-ID order.
type LeastLoaded struct{}

// NewLeastLoaded constructs a least-loaded policy.
func NewLeastLoaded() *LeastLoaded { return &LeastLoaded{} }

// Name implements scheduler.Policy.
func (*LeastLoaded) Name() string { return "least-loaded" }

// Decide implements scheduler.Policy.
func (*LeastLoaded) Decide(ctx context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	job, candidates, ok, err := fitCandidates(ctx, snapshot)
	if err != nil {
		return scheduler.Decision{}, err
	}
	if !ok {
		return scheduler.Decision{Defer: true}, nil
	}
	best := candidates[0]
	bestScore, err := normalizedUsage(best, job.Request, false)
	if err != nil {
		return scheduler.Decision{}, err
	}
	for _, candidate := range candidates[1:] {
		score, err := normalizedUsage(candidate, job.Request, false)
		if err != nil {
			return scheduler.Decision{}, err
		}
		if score < bestScore {
			best, bestScore = candidate, score
		}
	}
	return placement(job, best), nil
}
