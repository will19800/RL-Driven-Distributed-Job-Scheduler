package policies

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

func policyFixture(t *testing.T) scheduler.Snapshot {
	t.Helper()
	nodes := []domain.Node{
		{ID: "node-c", Capacity: domain.ResourceVector{CPUMillis: 4_000}, State: domain.NodeStateAvailable},
		{ID: "node-a", Capacity: domain.ResourceVector{CPUMillis: 4_000}, State: domain.NodeStateAvailable},
		{ID: "node-b", Capacity: domain.ResourceVector{CPUMillis: 4_000}, State: domain.NodeStateAvailable},
	}
	jobs := []domain.Job{{ID: "job-1", Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: time.Second, State: domain.JobStateQueued}}
	snapshot, err := scheduler.NewSnapshot(0, nodes, jobs)
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	return snapshot
}

func selectedNodes(t *testing.T, policy scheduler.Policy, snapshot scheduler.Snapshot, count int) []domain.NodeID {
	t.Helper()
	selected := make([]domain.NodeID, 0, count)
	for range count {
		evaluation, err := scheduler.Evaluate(context.Background(), policy, snapshot)
		if err != nil {
			t.Fatalf("Evaluate() unexpected error: %v", err)
		}
		selected = append(selected, evaluation.Decision.Placements[0].NodeID)
	}
	return selected
}

func TestRoundRobinCyclesInStableOrder(t *testing.T) {
	t.Parallel()
	got := selectedNodes(t, NewRoundRobin(), policyFixture(t), 7)
	want := []domain.NodeID{"node-a", "node-b", "node-c", "node-a", "node-b", "node-c", "node-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected nodes = %v, want %v", got, want)
	}
}

func TestRoundRobinSkipsIneligibleNodesWithoutConsumingDeferredTurns(t *testing.T) {
	t.Parallel()
	policy := NewRoundRobin()
	snapshot := policyFixture(t)
	first := selectedNodes(t, policy, snapshot, 1)
	if first[0] != "node-a" {
		t.Fatalf("first node = %q, want node-a", first[0])
	}
	job := snapshot.Jobs()[0]
	job.Request.CPUMillis = 5_000
	blocked, err := scheduler.NewSnapshot(0, snapshot.Nodes(), []domain.Job{job})
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	decision, err := policy.Decide(context.Background(), blocked)
	if err != nil || !decision.Defer {
		t.Fatalf("Decide(blocked) = %+v, %v; want defer", decision, err)
	}
	nodes := snapshot.Nodes()
	nodes[1].State = domain.NodeStateUnavailable
	partial, err := scheduler.NewSnapshot(0, nodes, snapshot.Jobs())
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	if got := selectedNodes(t, policy, partial, 1)[0]; got != "node-c" {
		t.Fatalf("node after defer = %q, want node-c", got)
	}
}

func TestRandomFitIsSeededAndStable(t *testing.T) {
	t.Parallel()
	snapshot := policyFixture(t)
	first := selectedNodes(t, NewRandomFit(42), snapshot, 12)
	second := selectedNodes(t, NewRandomFit(42), snapshot, 12)
	different := selectedNodes(t, NewRandomFit(43), snapshot, 12)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different sequences: %v and %v", first, second)
	}
	if reflect.DeepEqual(first, different) {
		t.Fatalf("different seeds produced identical sequence: %v", first)
	}
}

func heterogeneousFixture(t *testing.T) scheduler.Snapshot {
	t.Helper()
	nodes := []domain.Node{
		{
			ID:        "node-a",
			Capacity:  domain.ResourceVector{CPUMillis: 4_000, MemoryMiB: 8_000},
			Allocated: domain.ResourceVector{CPUMillis: 1_000, MemoryMiB: 6_000},
			State:     domain.NodeStateAvailable,
		},
		{
			ID:        "node-b",
			Capacity:  domain.ResourceVector{CPUMillis: 8_000, MemoryMiB: 4_000},
			Allocated: domain.ResourceVector{CPUMillis: 2_000, MemoryMiB: 500},
			State:     domain.NodeStateAvailable,
		},
	}
	jobs := []domain.Job{{
		ID:       "job-mixed",
		Request:  domain.ResourceVector{CPUMillis: 1_000, MemoryMiB: 2_000},
		Duration: time.Second,
		State:    domain.JobStateQueued,
	}}
	snapshot, err := scheduler.NewSnapshot(0, nodes, jobs)
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	return snapshot
}

func TestNormalizedPoliciesChooseOppositePackingGoals(t *testing.T) {
	t.Parallel()
	snapshot := heterogeneousFixture(t)
	tests := []struct {
		policy scheduler.Policy
		want   domain.NodeID
	}{
		{NewLeastLoaded(), "node-b"},
		{NewBinPack(), "node-a"},
	}
	for _, test := range tests {
		t.Run(test.policy.Name(), func(t *testing.T) {
			t.Parallel()
			got := selectedNodes(t, test.policy, snapshot, 1)[0]
			if got != test.want {
				t.Fatalf("selected node = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNormalizedPoliciesBreakTiesByNodeID(t *testing.T) {
	t.Parallel()
	for _, policy := range []scheduler.Policy{NewLeastLoaded(), NewBinPack()} {
		if got := selectedNodes(t, policy, policyFixture(t), 1)[0]; got != "node-a" {
			t.Fatalf("%s selected node = %q, want node-a", policy.Name(), got)
		}
	}
}

func TestNormalizedUsageIgnoresUnrequestedDimensions(t *testing.T) {
	t.Parallel()
	request := domain.ResourceVector{CPUMillis: 1_000}
	withoutGPU := domain.Node{
		Capacity:  domain.ResourceVector{CPUMillis: 4_000},
		Allocated: domain.ResourceVector{CPUMillis: 1_000},
	}
	withGPU := domain.Node{
		Capacity:  domain.ResourceVector{CPUMillis: 4_000, GPUs: 8},
		Allocated: domain.ResourceVector{CPUMillis: 1_000},
	}
	left, leftErr := normalizedUsage(withoutGPU, request, false)
	right, rightErr := normalizedUsage(withGPU, request, false)
	if leftErr != nil || rightErr != nil {
		t.Fatalf("normalizedUsage() errors = %v and %v", leftErr, rightErr)
	}
	if left != right {
		t.Fatalf("scores = %f and %f, want equal", left, right)
	}
}

func TestNormalizedUsageRejectsZeroRequest(t *testing.T) {
	t.Parallel()
	if _, err := normalizedUsage(domain.Node{}, domain.ResourceVector{}, false); err == nil {
		t.Fatal("normalizedUsage() error = nil, want error")
	}
}

func TestPoliciesDeferWithoutFeasibleWork(t *testing.T) {
	t.Parallel()
	snapshot, err := scheduler.NewSnapshot(0, policyFixture(t).Nodes(), nil)
	if err != nil {
		t.Fatalf("NewSnapshot() unexpected error: %v", err)
	}
	for _, policy := range []scheduler.Policy{NewRoundRobin(), NewRandomFit(1), NewLeastLoaded(), NewBinPack()} {
		evaluation, err := scheduler.Evaluate(context.Background(), policy, snapshot)
		if err != nil {
			t.Fatalf("Evaluate(%s) unexpected error: %v", policy.Name(), err)
		}
		if !evaluation.Decision.Defer {
			t.Fatalf("Evaluate(%s) did not defer", policy.Name())
		}
	}
}

func TestPoliciesHonorCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, policy := range []scheduler.Policy{NewRoundRobin(), NewRandomFit(1), NewLeastLoaded(), NewBinPack()} {
		_, err := policy.Decide(ctx, policyFixture(t))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Decide(%s) error = %v, want context.Canceled", policy.Name(), err)
		}
	}
}
