package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

func testSnapshot(t *testing.T) Snapshot {
	t.Helper()
	nodes := []domain.Node{
		{ID: "node-b", Labels: map[string]string{"zone": "west"}, Capacity: domain.ResourceVector{CPUMillis: 1_000}, State: domain.NodeStateAvailable},
		{ID: "node-a", Capacity: domain.ResourceVector{CPUMillis: 2_000}, State: domain.NodeStateAvailable},
	}
	jobs := []domain.Job{
		{ID: "job-low", SubmitTime: time.Second, Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: time.Second, State: domain.JobStateQueued},
		{ID: "job-high", SubmitTime: time.Second, Request: domain.ResourceVector{CPUMillis: 1_500}, Duration: time.Second, Priority: 2, State: domain.JobStateQueued},
	}
	snapshot, err := NewSnapshot(time.Second, nodes, jobs)
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	return snapshot
}

func TestSnapshotIsStableAndDefensive(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	if got := snapshot.Nodes(); got[0].ID != "node-a" {
		t.Fatalf("first node = %q, want node-a", got[0].ID)
	}
	if got := snapshot.Jobs(); got[0].ID != "job-high" {
		t.Fatalf("first job = %q, want job-high", got[0].ID)
	}
	nodes := snapshot.Nodes()
	nodes[1].Labels["zone"] = "changed"
	nodes[0].Allocated.CPUMillis = 2_000
	jobs := snapshot.Jobs()
	jobs[0].ID = "changed"
	if got := snapshot.Nodes()[1].Labels["zone"]; got != "west" {
		t.Fatalf("snapshot label = %q after caller mutation, want west", got)
	}
	if got := snapshot.Nodes()[0].Allocated.CPUMillis; got != 0 {
		t.Fatalf("snapshot allocation = %d after caller mutation, want 0", got)
	}
	if got := snapshot.Jobs()[0].ID; got != "job-high" {
		t.Fatalf("snapshot job = %q after caller mutation, want job-high", got)
	}
}

func TestNewSnapshotRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	validNode := domain.Node{ID: "node", Capacity: domain.ResourceVector{CPUMillis: 1}, State: domain.NodeStateAvailable}
	validJob := domain.Job{ID: "job", Request: domain.ResourceVector{CPUMillis: 1}, Duration: time.Second, State: domain.JobStateQueued}
	tests := []struct {
		name  string
		now   time.Duration
		nodes []domain.Node
		jobs  []domain.Job
	}{
		{"negative time", -1, nil, nil},
		{"invalid node", 0, []domain.Node{{}}, nil},
		{"duplicate node", 0, []domain.Node{validNode, validNode}, nil},
		{"invalid job", 0, nil, []domain.Job{{}}},
		{"duplicate job", 0, nil, []domain.Job{validJob, validJob}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewSnapshot(test.now, test.nodes, test.jobs); err == nil {
				t.Fatal("NewSnapshot() error = nil, want error")
			}
		})
	}
}

func TestValidateDecisionAcceptsDeferAndCapacityAwarePlacements(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	if err := ValidateDecision(snapshot, Decision{Defer: true}); err != nil {
		t.Fatalf("ValidateDecision(defer) unexpected error: %v", err)
	}
	decision := Decision{Placements: []domain.Placement{
		{JobID: "job-high", NodeID: "node-a"},
		{JobID: "job-low", NodeID: "node-b"},
	}}
	if err := ValidateDecision(snapshot, decision); err != nil {
		t.Fatalf("ValidateDecision(placements) unexpected error: %v", err)
	}
}

func TestValidateDecisionRejectsUnsafeActions(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	running := snapshot.Jobs()[1]
	running.State = domain.JobStateRunning
	running.AssignedNode = "node-b"
	started := time.Second
	running.StartTime = &started
	runningSnapshot, err := NewSnapshot(time.Second, snapshot.Nodes(), []domain.Job{running})
	if err != nil {
		t.Fatalf("NewSnapshot(running) unexpected error: %v", err)
	}
	future := snapshot.Jobs()[1]
	future.SubmitTime = 2 * time.Second
	futureSnapshot, err := NewSnapshot(time.Second, snapshot.Nodes(), []domain.Job{future})
	if err != nil {
		t.Fatalf("NewSnapshot(future) unexpected error: %v", err)
	}
	tests := []struct {
		name     string
		snapshot Snapshot
		decision Decision
	}{
		{"implicit empty", snapshot, Decision{}},
		{"defer with placement", snapshot, Decision{Defer: true, Placements: []domain.Placement{{JobID: "job-low", NodeID: "node-a"}}}},
		{"malformed placement", snapshot, Decision{Placements: []domain.Placement{{JobID: "job-low"}}}},
		{"missing job", snapshot, Decision{Placements: []domain.Placement{{JobID: "missing", NodeID: "node-a"}}}},
		{"missing node", snapshot, Decision{Placements: []domain.Placement{{JobID: "job-low", NodeID: "missing"}}}},
		{"running job", runningSnapshot, Decision{Placements: []domain.Placement{{JobID: running.ID, NodeID: "node-a"}}}},
		{"future job", futureSnapshot, Decision{Placements: []domain.Placement{{JobID: future.ID, NodeID: "node-a"}}}},
		{"infeasible", snapshot, Decision{Placements: []domain.Placement{{JobID: "job-high", NodeID: "node-b"}}}},
		{"duplicate job", snapshot, Decision{Placements: []domain.Placement{{JobID: "job-low", NodeID: "node-a"}, {JobID: "job-low", NodeID: "node-b"}}}},
		{"aggregate overcommit", snapshot, Decision{Placements: []domain.Placement{{JobID: "job-high", NodeID: "node-a"}, {JobID: "job-low", NodeID: "node-a"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateDecision(test.snapshot, test.decision); !errors.Is(err, ErrInvalidDecision) {
				t.Fatalf("ValidateDecision() error = %v, want ErrInvalidDecision", err)
			}
		})
	}
}

type stubPolicy struct {
	name     string
	decision Decision
	err      error
}

func (p stubPolicy) Name() string { return p.name }
func (p stubPolicy) Decide(context.Context, Snapshot) (Decision, error) {
	return p.decision, p.err
}

func TestEvaluateRecordsDecisionTelemetry(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	policy := stubPolicy{name: "test", decision: Decision{Placements: []domain.Placement{{JobID: "job-low", NodeID: "node-a"}}}}
	evaluation, err := Evaluate(context.Background(), policy, snapshot)
	if err != nil {
		t.Fatalf("Evaluate() unexpected error: %v", err)
	}
	if evaluation.Telemetry.Policy != "test" || evaluation.Telemetry.PlacementCount != 1 || evaluation.Telemetry.PolicyErrors != 0 || evaluation.Telemetry.InvalidDecisions != 0 {
		t.Fatalf("telemetry = %+v", evaluation.Telemetry)
	}
}

func TestEvaluateDistinguishesPolicyAndValidationErrors(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	tests := []struct {
		name       string
		policy     Policy
		wantError  error
		policyErrs uint64
		invalid    uint64
	}{
		{"nil policy", nil, ErrPolicy, 0, 0},
		{"empty name", stubPolicy{}, ErrPolicy, 1, 0},
		{"policy error", stubPolicy{name: "broken", err: errors.New("boom")}, ErrPolicy, 1, 0},
		{"invalid decision", stubPolicy{name: "unsafe", decision: Decision{}}, ErrInvalidDecision, 0, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			evaluation, err := Evaluate(context.Background(), test.policy, snapshot)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("Evaluate() error = %v, want %v", err, test.wantError)
			}
			if evaluation.Telemetry.PolicyErrors != test.policyErrs || evaluation.Telemetry.InvalidDecisions != test.invalid {
				t.Fatalf("telemetry = %+v", evaluation.Telemetry)
			}
		})
	}
}
