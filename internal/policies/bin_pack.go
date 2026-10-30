package policies

import (
	"context"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

// BinPack selects the feasible node with the highest mean normalized usage
// after placement, equivalently leaving the least normalized residual capacity
// across dimensions requested by the job. Equal scores use stable node-ID order.
type BinPack struct{}

// NewBinPack constructs a bin-pack policy.
func NewBinPack() *BinPack { return &BinPack{} }

// Name implements scheduler.Policy.
func (*BinPack) Name() string { return "bin-pack" }

// Decide implements scheduler.Policy.
func (*BinPack) Decide(ctx context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	job, candidates, ok, err := fitCandidates(ctx, snapshot)
	if err != nil {
		return scheduler.Decision{}, err
	}
	if !ok {
		return scheduler.Decision{Defer: true}, nil
	}
	best := candidates[0]
	bestScore, err := normalizedUsage(best, job.Request, true)
	if err != nil {
		return scheduler.Decision{}, err
	}
	for _, candidate := range candidates[1:] {
		score, err := normalizedUsage(candidate, job.Request, true)
		if err != nil {
			return scheduler.Decision{}, err
		}
		if score > bestScore {
			best, bestScore = candidate, score
		}
	}
	return placement(job, best), nil
}
