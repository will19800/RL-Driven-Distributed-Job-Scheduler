package inference

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policies"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

type runtimeFunc func(context.Context, policyv1.Observation) ([]float64, error)

func (f runtimeFunc) Predict(ctx context.Context, observation policyv1.Observation) ([]float64, error) {
	return f(ctx, observation)
}

func testSnapshot(t *testing.T) scheduler.Snapshot {
	t.Helper()
	nodes := []domain.Node{
		{ID: "a-full", State: domain.NodeStateAvailable, Capacity: domain.ResourceVector{CPUMillis: 1000}, Allocated: domain.ResourceVector{CPUMillis: 1000}},
		{ID: "b-free", State: domain.NodeStateAvailable, Capacity: domain.ResourceVector{CPUMillis: 1000}},
	}
	jobs := []domain.Job{{ID: "job", State: domain.JobStateQueued, Duration: time.Second, Request: domain.ResourceVector{CPUMillis: 1000}}}
	snapshot, err := scheduler.NewSnapshot(time.Second, nodes, jobs)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func testConfig() policyv1.Config {
	config := policyv1.DefaultConfig()
	config.MaxJobs = 1
	config.MaxNodes = 2
	return config
}

func TestMaskedArgmaxAndDefer(t *testing.T) {
	for _, test := range []struct {
		name        string
		logits      []float64
		deferAction bool
	}{
		{"masked largest", []float64{100, 2, 1}, false}, {"defer", []float64{100, 1, 2}, true}, {"stable tie", []float64{100, 2, 2}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := runtimeFunc(func(context.Context, policyv1.Observation) ([]float64, error) { return test.logits, nil })
			policy, err := NewPolicy(testConfig(), runtime, policies.NewBinPack(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := testSnapshot(t)
			decision, err := policy.Decide(context.Background(), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Defer != test.deferAction {
				t.Fatalf("decision %+v", decision)
			}
			if !decision.Defer && decision.Placements[0].NodeID != "b-free" {
				t.Fatalf("unsafe selection %+v", decision)
			}
			if err := scheduler.ValidateDecision(snapshot, decision); err != nil {
				t.Fatal(err)
			}
			if policy.Diagnostics().Fallbacks != 0 {
				t.Fatal("unexpected fallback")
			}
		})
	}
}

func TestFaultsFallbackAndRemainFeasible(t *testing.T) {
	for _, test := range []struct {
		name    string
		runtime runtimeFunc
		reason  string
	}{
		{"error", func(context.Context, policyv1.Observation) ([]float64, error) { return nil, errors.New("injected") }, "runtime"},
		{"shape", func(context.Context, policyv1.Observation) ([]float64, error) { return []float64{1}, nil }, "shape"},
		{"nan", func(context.Context, policyv1.Observation) ([]float64, error) {
			return []float64{0, math.NaN(), 0}, nil
		}, "nonfinite"},
		{"infinity", func(context.Context, policyv1.Observation) ([]float64, error) {
			return []float64{0, math.Inf(1), 0}, nil
		}, "nonfinite"},
		{"timeout", func(ctx context.Context, _ policyv1.Observation) ([]float64, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, "timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := NewPolicy(testConfig(), test.runtime, policies.NewBinPack(), 5*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := testSnapshot(t)
			decision, err := policy.Decide(context.Background(), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := scheduler.ValidateDecision(snapshot, decision); err != nil {
				t.Fatal(err)
			}
			if len(decision.Placements) != 1 {
				t.Fatalf("fallback did not place: %+v", decision)
			}
			diagnostics := policy.Diagnostics()
			if diagnostics.Fallbacks != 1 || diagnostics.Reasons[test.reason] != 1 {
				t.Fatalf("diagnostics %+v", diagnostics)
			}
			diagnostics.Reasons[test.reason] = 100
			if policy.Diagnostics().Reasons[test.reason] != 1 {
				t.Fatal("diagnostics alias")
			}
		})
	}
}

func TestCancellationAndBlockedQueueDoNotInvokeRuntime(t *testing.T) {
	runtime := runtimeFunc(func(context.Context, policyv1.Observation) ([]float64, error) {
		t.Fatal("unexpected inference")
		return nil, nil
	})
	policy, _ := NewPolicy(testConfig(), runtime, policies.NewBinPack(), time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := policy.Decide(ctx, testSnapshot(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	empty, _ := scheduler.NewSnapshot(0, nil, nil)
	decision, err := policy.Decide(context.Background(), empty)
	if err != nil || !decision.Defer {
		t.Fatalf("decision %+v, %v", decision, err)
	}
}

type unsafeFallback struct{}

func (unsafeFallback) Name() string { return "unsafe" }
func (unsafeFallback) Decide(context.Context, scheduler.Snapshot) (scheduler.Decision, error) {
	return scheduler.Decision{Placements: []domain.Placement{{JobID: "job", NodeID: "a-full"}}}, nil
}
func TestUnsafeFallbackIsRejected(t *testing.T) {
	runtime := runtimeFunc(func(context.Context, policyv1.Observation) ([]float64, error) { return nil, errors.New("fail") })
	policy, _ := NewPolicy(testConfig(), runtime, unsafeFallback{}, time.Second)
	if _, err := policy.Decide(context.Background(), testSnapshot(t)); err == nil {
		t.Fatal("unsafe fallback accepted")
	}
}

func TestCandidateExhaustionUsesFullClusterFallback(t *testing.T) {
	config := testConfig()
	config.MaxNodes = 1
	runtime := runtimeFunc(func(context.Context, policyv1.Observation) ([]float64, error) {
		t.Fatal("candidate set has no feasible action")
		return nil, nil
	})
	policy, _ := NewPolicy(config, runtime, policies.NewBinPack(), time.Second)
	decision, err := policy.Decide(context.Background(), testSnapshot(t))
	if err != nil || len(decision.Placements) != 1 || decision.Placements[0].NodeID != "b-free" {
		t.Fatalf("decision %+v, %v", decision, err)
	}
	if policy.Diagnostics().Reasons["candidates"] != 1 {
		t.Fatal("missing candidate exhaustion diagnostic")
	}
}

func TestCandidateIDsAreRevalidatedAfterRuntime(t *testing.T) {
	runtime := runtimeFunc(func(_ context.Context, observation policyv1.Observation) ([]float64, error) {
		observation.NodeIDs[1] = "missing"
		return []float64{0, 1, 0}, nil
	})
	policy, _ := NewPolicy(testConfig(), runtime, policies.NewBinPack(), time.Second)
	snapshot := testSnapshot(t)
	decision, err := policy.Decide(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.ValidateDecision(snapshot, decision); err != nil {
		t.Fatal(err)
	}
	if policy.Diagnostics().Reasons["validation"] != 1 {
		t.Fatal("unsafe candidate ID was not rejected")
	}
}
