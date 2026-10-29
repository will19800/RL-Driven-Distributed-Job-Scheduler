package policies

import (
	"context"
	"sync"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

// RoundRobin cycles through the feasible nodes in stable node-ID order. The
// cursor advances only after a successful placement; deferred decisions do not
// consume a turn.
type RoundRobin struct {
	mu     sync.Mutex
	cursor uint64
}

// NewRoundRobin constructs a round-robin policy with its cursor at the first
// eligible node.
func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

// Name implements scheduler.Policy.
func (*RoundRobin) Name() string { return "round-robin" }

// Decide implements scheduler.Policy.
func (p *RoundRobin) Decide(ctx context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	job, candidates, ok, err := fitCandidates(ctx, snapshot)
	if err != nil {
		return scheduler.Decision{}, err
	}
	if !ok {
		return scheduler.Decision{Defer: true}, nil
	}
	p.mu.Lock()
	node := candidates[p.cursor%uint64(len(candidates))]
	p.cursor++
	p.mu.Unlock()
	return placement(job, node), nil
}
