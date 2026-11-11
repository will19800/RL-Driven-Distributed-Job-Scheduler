package inference

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

type Runtime interface {
	// Predict must return when ctx is canceled. Process enforces this by killing
	// and reaping a timed-out worker, including blocked pipe writes.
	Predict(context.Context, policyv1.Observation) ([]float64, error)
}
type Diagnostics struct {
	Decisions  uint64            `json:"decisions"`
	Fallbacks  uint64            `json:"fallbacks"`
	ModelCalls uint64            `json:"model_calls"`
	Reasons    map[string]uint64 `json:"reasons"`
}
type Policy struct {
	config      policyv1.Config
	runtime     Runtime
	fallback    scheduler.Policy
	timeout     time.Duration
	mu          sync.Mutex
	diagnostics Diagnostics
}

func NewPolicy(config policyv1.Config, runtime Runtime, fallback scheduler.Policy, timeout time.Duration) (*Policy, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if runtime == nil || fallback == nil || timeout <= 0 {
		return nil, errors.New("runtime, fallback, and positive timeout are required")
	}
	return &Policy{config: config, runtime: runtime, fallback: fallback, timeout: timeout, diagnostics: Diagnostics{Reasons: map[string]uint64{}}}, nil
}
func (p *Policy) Name() string { return "rl" }
func (p *Policy) Diagnostics() Diagnostics {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.diagnostics
	result.Reasons = map[string]uint64{}
	for key, value := range p.diagnostics.Reasons {
		result.Reasons[key] = value
	}
	return result
}
func (p *Policy) Decide(ctx context.Context, snapshot scheduler.Snapshot) (scheduler.Decision, error) {
	if err := ctx.Err(); err != nil {
		return scheduler.Decision{}, err
	}
	p.mu.Lock()
	p.diagnostics.Decisions++
	p.mu.Unlock()
	// Same trailing 60s arrivals estimator as the Python environment, using only
	// submitted jobs (including completed/rejected jobs) from the snapshot.
	arrivals := 0
	for _, job := range snapshot.Jobs() {
		if job.SubmitTime <= snapshot.Now() && job.SubmitTime > snapshot.Now()-60*time.Second {
			arrivals++
		}
	}
	observation, err := policyv1.Encode(p.config, snapshot, float64(arrivals)/60)
	if err != nil {
		return p.recover(ctx, snapshot, "encoding")
	}
	// No placement is needed; skip model overhead on empty or blocked queues.
	feasible := false
	for _, allowed := range observation.ActionMask[:len(observation.ActionMask)-1] {
		feasible = feasible || allowed
	}
	if !feasible {
		nodes := snapshot.Nodes()
		for _, job := range snapshot.Jobs() {
			if job.State != domain.JobStateQueued || job.SubmitTime > snapshot.Now() {
				continue
			}
			for _, node := range nodes {
				if node.CanFit(job.Request) {
					return p.recover(ctx, snapshot, "candidates")
				}
			}
		}
		return scheduler.Decision{Defer: true}, nil
	}
	budget, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	p.mu.Lock()
	p.diagnostics.ModelCalls++
	p.mu.Unlock()
	logits, err := p.runtime.Predict(budget, observation)
	if ctx.Err() != nil {
		return scheduler.Decision{}, ctx.Err()
	}
	if err != nil {
		reason := "runtime"
		if errors.Is(err, context.DeadlineExceeded) || budget.Err() != nil {
			reason = "timeout"
		}
		return p.recover(ctx, snapshot, reason)
	}
	if budget.Err() != nil {
		return p.recover(ctx, snapshot, "timeout")
	}
	if len(logits) != len(observation.ActionMask) {
		return p.recover(ctx, snapshot, "shape")
	}
	best := -1
	for index, value := range logits {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return p.recover(ctx, snapshot, "nonfinite")
		}
		if observation.ActionMask[index] && (best < 0 || value > logits[best]) {
			best = index
		}
	}
	action, err := policyv1.DecodeAction(p.config, observation, best)
	if err != nil {
		return p.recover(ctx, snapshot, "action")
	}
	decision := scheduler.Decision{Defer: action.Defer}
	if !action.Defer {
		placement, ok := action.Placement(observation)
		if !ok {
			return p.recover(ctx, snapshot, "action")
		}
		decision.Placements = []domain.Placement{placement}
	}
	if err := scheduler.ValidateDecision(snapshot, decision); err != nil {
		return p.recover(ctx, snapshot, "validation")
	}
	return decision, nil
}
func (p *Policy) recover(ctx context.Context, snapshot scheduler.Snapshot, reason string) (scheduler.Decision, error) {
	p.mu.Lock()
	p.diagnostics.Fallbacks++
	p.diagnostics.Reasons[reason]++
	p.mu.Unlock()
	decision, err := p.fallback.Decide(ctx, snapshot)
	if err != nil {
		return scheduler.Decision{}, fmt.Errorf("fallback: %w", err)
	}
	if err := scheduler.ValidateDecision(snapshot, decision); err != nil {
		return scheduler.Decision{}, fmt.Errorf("invalid fallback: %w", err)
	}
	return decision, nil
}
