package metrics

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

func metricsNode(allocation domain.ResourceVector) domain.Node {
	return domain.Node{
		ID: "node-1", Capacity: domain.ResourceVector{CPUMillis: 100, MemoryMiB: 100},
		Allocated: allocation, State: domain.NodeStateAvailable,
	}
}

func completedJob(id domain.JobID, submit, start, end, duration time.Duration) domain.Job {
	return domain.Job{
		ID: id, SubmitTime: submit, Request: domain.ResourceVector{CPUMillis: 1}, Duration: duration,
		State: domain.JobStateCompleted, AssignedNode: "node-1", StartTime: &start, EndTime: &end,
	}
}

func TestCollectorAggregatesWorkloadClusterAndDecisionMetrics(t *testing.T) {
	t.Parallel()
	collector := NewCollector()
	observations := []struct {
		at        time.Duration
		allocated domain.ResourceVector
		queued    int
	}{
		{0, domain.ResourceVector{}, 2},
		{2 * time.Second, domain.ResourceVector{CPUMillis: 50, MemoryMiB: 25}, 1},
		{4 * time.Second, domain.ResourceVector{}, 0},
	}
	for _, observation := range observations {
		if err := collector.ObserveCluster(observation.at, []domain.Node{metricsNode(observation.allocated)}, observation.queued); err != nil {
			t.Fatalf("ObserveCluster() unexpected error: %v", err)
		}
	}
	collector.RecordDecision(scheduler.Telemetry{Duration: 2 * time.Millisecond, PlacementCount: 1})
	collector.RecordDecision(scheduler.Telemetry{Duration: 4 * time.Millisecond, Deferred: true, PolicyErrors: 1, InvalidDecisions: 1})
	deadline := time.Second
	first := completedJob("job-1", 0, time.Second, 3*time.Second, time.Second)
	second := completedJob("job-2", 0, 0, 2*time.Second, 2*time.Second)
	second.Deadline = &deadline
	rejectedAt := 2 * time.Second
	rejected := domain.Job{
		ID: "job-3", Request: domain.ResourceVector{CPUMillis: 1}, Duration: time.Second,
		State: domain.JobStateRejected, EndTime: &rejectedAt, RejectionReason: "test rejection",
	}
	report, err := collector.Finalize(6*time.Second, []domain.Job{first, second, rejected})
	if err != nil {
		t.Fatalf("Finalize() unexpected error: %v", err)
	}
	assertClose(t, report.ThroughputPerSecond, 1.0/3.0)
	assertClose(t, report.SuccessRate, 2.0/3.0)
	assertClose(t, report.RejectionRate, 1.0/3.0)
	assertClose(t, report.Utilization.CPU, 1.0/6.0)
	assertClose(t, report.Utilization.Memory, 1.0/12.0)
	assertClose(t, report.MeanQueueDepth, 1)
	assertClose(t, report.ResourceFragmentation, 1.0/12.0)
	assertClose(t, report.MeanBoundedSlowdown, 2)
	assertClose(t, report.SlowdownFairness, 0.8)
	if report.Completed != 2 || report.Rejected != 1 || report.DeadlineJobs != 1 || report.DeadlineViolations != 1 || report.DeadlineViolationRate != 1 {
		t.Fatalf("job counters = completed %d rejected %d deadlines %d", report.Completed, report.Rejected, report.DeadlineViolations)
	}
	if report.Latency.Count != 2 || report.Latency.Min != 2*time.Second || report.Latency.Max != 3*time.Second || report.Latency.Mean != 2500*time.Millisecond {
		t.Fatalf("latency = %+v", report.Latency)
	}
	if report.QueueWait.Count != 2 || report.QueueWait.Mean != 500*time.Millisecond {
		t.Fatalf("queue wait = %+v", report.QueueWait)
	}
	if report.Decisions.Count != 2 || report.Decisions.Placements != 1 || report.Decisions.Deferred != 1 || report.Decisions.PolicyErrors != 1 || report.Decisions.InvalidDecisions != 1 || report.Decisions.TotalDuration != 6*time.Millisecond || report.Decisions.MeanDuration != 3*time.Millisecond || report.Decisions.MaxDuration != 4*time.Millisecond {
		t.Fatalf("decisions = %+v", report.Decisions)
	}
}

func TestDurationHistogramUsesBoundedBuckets(t *testing.T) {
	t.Parallel()
	histogram := newDurationHistogram()
	for _, value := range []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 20 * time.Second} {
		histogram.observe(value)
	}
	got := histogram.snapshot()
	if got.Count != 3 || got.Min != 500*time.Millisecond || got.Max != 20*time.Second || got.P50 != 2*time.Second || got.P95 != 30*time.Second || got.P99 != 30*time.Second {
		t.Fatalf("snapshot() = %+v", got)
	}
	if len(histogram.counts) != len(defaultDurationBuckets)+1 {
		t.Fatalf("bucket count grew to %d", len(histogram.counts))
	}
}

func TestCollectorRejectsInvalidObservations(t *testing.T) {
	t.Parallel()
	collector := NewCollector()
	if err := collector.ObserveCluster(-1, nil, 0); !errors.Is(err, ErrInvalidObservation) {
		t.Fatalf("negative observation error = %v", err)
	}
	if err := collector.ObserveCluster(time.Second, []domain.Node{metricsNode(domain.ResourceVector{})}, 0); err != nil {
		t.Fatalf("ObserveCluster() unexpected error: %v", err)
	}
	if err := collector.ObserveCluster(0, nil, 0); !errors.Is(err, ErrInvalidObservation) {
		t.Fatalf("backward observation error = %v", err)
	}
	invalidNode := metricsNode(domain.ResourceVector{})
	invalidNode.Capacity = domain.ResourceVector{}
	if err := collector.ObserveCluster(time.Second, []domain.Node{invalidNode}, 0); !errors.Is(err, ErrInvalidObservation) {
		t.Fatalf("invalid node observation error = %v", err)
	}
	if _, err := collector.Finalize(0, nil); !errors.Is(err, ErrInvalidObservation) {
		t.Fatalf("backward Finalize() error = %v", err)
	}
}

func TestCollectorHandlesEmptyAndZeroMakespan(t *testing.T) {
	t.Parallel()
	collector := NewCollector()
	if err := collector.ObserveCluster(0, nil, 0); err != nil {
		t.Fatalf("ObserveCluster() unexpected error: %v", err)
	}
	report, err := collector.Finalize(0, nil)
	if err != nil {
		t.Fatalf("Finalize() unexpected error: %v", err)
	}
	if report != (Report{}) {
		t.Fatalf("report = %+v, want zero report", report)
	}
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got %.15f, want %.15f", got, want)
	}
}
