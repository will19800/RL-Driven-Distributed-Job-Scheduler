package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

var (
	// ErrPolicy indicates that a policy could not produce a decision.
	ErrPolicy = errors.New("policy decision failed")
	// ErrInvalidDecision indicates that a decision is unsafe to apply.
	ErrInvalidDecision = errors.New("invalid scheduling decision")
)

// Policy chooses zero or more placements from an immutable snapshot.
type Policy interface {
	Name() string
	Decide(context.Context, Snapshot) (Decision, error)
}

// Decision either places queued jobs or deliberately defers scheduling.
type Decision struct {
	Placements []domain.Placement `json:"placements,omitempty"`
	Defer      bool               `json:"defer,omitempty"`
}

// Telemetry describes policy execution independently from workload results.
type Telemetry struct {
	Policy           string        `json:"policy"`
	Duration         time.Duration `json:"duration"`
	PlacementCount   int           `json:"placement_count"`
	Deferred         bool          `json:"deferred"`
	PolicyErrors     uint64        `json:"policy_errors"`
	InvalidDecisions uint64        `json:"invalid_decisions"`
}

// Evaluation contains a validated decision and its execution telemetry.
type Evaluation struct {
	Decision  Decision
	Telemetry Telemetry
}

// Evaluate invokes a policy and validates its decision before returning it to
// the simulator. An error always accompanies policy or validation telemetry.
func Evaluate(ctx context.Context, policy Policy, snapshot Snapshot) (Evaluation, error) {
	if policy == nil {
		return Evaluation{}, fmt.Errorf("%w: policy is nil", ErrPolicy)
	}
	telemetry := Telemetry{Policy: policy.Name()}
	if telemetry.Policy == "" {
		telemetry.PolicyErrors = 1
		return Evaluation{Telemetry: telemetry}, fmt.Errorf("%w: name is empty", ErrPolicy)
	}
	started := time.Now()
	decision, err := policy.Decide(ctx, snapshot)
	telemetry.Duration = time.Since(started)
	if err != nil {
		telemetry.PolicyErrors = 1
		return Evaluation{Telemetry: telemetry}, fmt.Errorf("%w: %s: %v", ErrPolicy, telemetry.Policy, err)
	}
	telemetry.PlacementCount = len(decision.Placements)
	telemetry.Deferred = decision.Defer
	if err := ValidateDecision(snapshot, decision); err != nil {
		telemetry.InvalidDecisions = 1
		return Evaluation{Telemetry: telemetry}, err
	}
	return Evaluation{Decision: decision, Telemetry: telemetry}, nil
}
