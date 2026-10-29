package policies

import (
	"context"
	"math/rand"
	"sync"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

// RandomFit chooses uniformly among feasible nodes using a policy-local random
// stream. Candidate ordering remains stable, so the same experiment seed and
// decision sequence produce the same placements.
type RandomFit struct {
	mu  sync.Mutex
	rng *rand.Rand
}

// NewRandomFit constructs a deterministic random-fit policy. The supplied
// experiment seed is mixed into a stream separate from workload generation.
func NewRandomFit(experimentSeed int64) *RandomFit {
	return &RandomFit{rng: rand.New(rand.NewSource(derivePolicySeed(experimentSeed)))}
}

// Name implements scheduler.Policy.
func (*RandomFit) Name() string { return "random-fit" }

// Decide implements scheduler.Policy.
func (p *RandomFit) Decide(ctx context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	job, candidates, ok, err := fitCandidates(ctx, snapshot)
	if err != nil {
		return scheduler.Decision{}, err
	}
	if !ok {
		return scheduler.Decision{Defer: true}, nil
	}
	p.mu.Lock()
	node := candidates[p.rng.Intn(len(candidates))]
	p.mu.Unlock()
	return placement(job, node), nil
}

func derivePolicySeed(seed int64) int64 {
	value := uint64(seed) + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}
