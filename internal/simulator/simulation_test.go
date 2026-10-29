package simulator

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

func simulationNode() domain.Node {
	return domain.Node{
		ID:       "node-1",
		Labels:   map[string]string{"pool": "default"},
		Capacity: domain.ResourceVector{CPUMillis: 2_000, MemoryMiB: 4_096},
		State:    domain.NodeStateAvailable,
	}
}

func simulationJob() domain.Job {
	deadline := 20 * time.Second
	return domain.Job{
		ID:         "job-1",
		SubmitTime: 2 * time.Second,
		Request:    domain.ResourceVector{CPUMillis: 500, MemoryMiB: 1_024},
		Duration:   5 * time.Second,
		Deadline:   &deadline,
		State:      domain.JobStateQueued,
	}
}

func TestSimulationProcessesJobLifecycleAndAccounting(t *testing.T) {
	t.Parallel()
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{simulationJob()})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	next, exists := simulation.NextEvent()
	if !exists || next.Type != EventJobSubmitted || next.Time != 2*time.Second || simulation.PendingEvents() != 1 {
		t.Fatalf("NextEvent() = %+v, %t; want submission at 2s", next, exists)
	}
	if _, err := simulation.SchedulePlacement(3*time.Second, domain.Placement{JobID: "job-1", NodeID: "node-1"}); err != nil {
		t.Fatalf("SchedulePlacement() unexpected error: %v", err)
	}

	event, ok, err := simulation.RunNext(context.Background())
	if err != nil || !ok || event.Type != EventJobSubmitted || simulation.Now() != 2*time.Second {
		t.Fatalf("submission = %+v, %t, %v, now=%s", event, ok, err, simulation.Now())
	}
	assertJobState(t, simulation, domain.JobStateQueued)

	event, ok, err = simulation.RunNext(context.Background())
	if err != nil || !ok || event.Type != EventJobStarted || simulation.Now() != 3*time.Second {
		t.Fatalf("start = %+v, %t, %v, now=%s", event, ok, err, simulation.Now())
	}
	assertJobState(t, simulation, domain.JobStateRunning)
	node, _ := simulation.Node("node-1")
	if node.Allocated != simulationJob().Request {
		t.Fatalf("running allocation = %+v, want %+v", node.Allocated, simulationJob().Request)
	}

	event, ok, err = simulation.RunNext(context.Background())
	if err != nil || !ok || event.Type != EventJobCompleted || simulation.Now() != 8*time.Second {
		t.Fatalf("completion = %+v, %t, %v, now=%s", event, ok, err, simulation.Now())
	}
	assertJobState(t, simulation, domain.JobStateCompleted)
	node, _ = simulation.Node("node-1")
	if !node.Allocated.IsZero() {
		t.Fatalf("completed allocation = %+v, want zero", node.Allocated)
	}
	if simulation.PendingEvents() != 0 {
		t.Fatalf("PendingEvents() = %d, want 0", simulation.PendingEvents())
	}
	if err := simulation.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
}

func TestSimulationFailedPlacementIsAtomic(t *testing.T) {
	t.Parallel()
	job := simulationJob()
	job.Request.CPUMillis = 3_000
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{job})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	if _, err := simulation.SchedulePlacement(3*time.Second, domain.Placement{JobID: job.ID, NodeID: "node-1"}); err != nil {
		t.Fatalf("SchedulePlacement() unexpected error: %v", err)
	}
	if _, _, err := simulation.RunNext(context.Background()); err != nil {
		t.Fatalf("submission unexpected error: %v", err)
	}
	_, ok, err := simulation.RunNext(context.Background())
	if !ok || !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("failed start ok=%t error=%v, want true, ErrInvalidTransition", ok, err)
	}
	if simulation.Now() != job.SubmitTime {
		t.Fatalf("failed start advanced clock to %s", simulation.Now())
	}
	assertJobState(t, simulation, domain.JobStateQueued)
	node, _ := simulation.Node("node-1")
	if !node.Allocated.IsZero() {
		t.Fatalf("failed start allocated resources: %+v", node.Allocated)
	}
	if err := simulation.Validate(); err != nil {
		t.Fatalf("Validate() after failed start: %v", err)
	}
}

func TestSimulationCompletionSchedulingFailureIsAtomic(t *testing.T) {
	t.Parallel()
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{simulationJob()})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	if _, err := simulation.SchedulePlacement(3*time.Second, domain.Placement{JobID: "job-1", NodeID: "node-1"}); err != nil {
		t.Fatalf("SchedulePlacement() unexpected error: %v", err)
	}
	if _, _, err := simulation.RunNext(context.Background()); err != nil {
		t.Fatalf("submission unexpected error: %v", err)
	}
	simulation.events.nextSequence = math.MaxUint64
	if _, ok, err := simulation.RunNext(context.Background()); !ok || !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("start ok=%t error=%v, want ErrSequenceExhausted", ok, err)
	}
	if simulation.Now() != 2*time.Second {
		t.Fatalf("failed completion scheduling advanced clock to %s", simulation.Now())
	}
	assertJobState(t, simulation, domain.JobStateQueued)
	node, _ := simulation.Node("node-1")
	if !node.Allocated.IsZero() {
		t.Fatalf("failed completion scheduling allocated resources: %+v", node.Allocated)
	}
}

func TestSimulationRunCompletesMultipleJobs(t *testing.T) {
	t.Parallel()
	second := simulationJob()
	second.ID = "job-2"
	second.SubmitTime = 4 * time.Second
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{simulationJob(), second})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	for _, placement := range []struct {
		at  time.Duration
		job domain.JobID
	}{{3 * time.Second, "job-1"}, {4 * time.Second, "job-2"}} {
		if _, err := simulation.SchedulePlacement(placement.at, domain.Placement{JobID: placement.job, NodeID: "node-1"}); err != nil {
			t.Fatalf("SchedulePlacement(%s) unexpected error: %v", placement.job, err)
		}
	}
	if err := simulation.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	assertJobStateByID(t, simulation, "job-1", domain.JobStateCompleted)
	assertJobStateByID(t, simulation, "job-2", domain.JobStateCompleted)
	if simulation.Now() != 9*time.Second {
		t.Fatalf("Now() = %s, want 9s", simulation.Now())
	}
}

func TestSimulationCancellationLeavesQueueAndStateUntouched(t *testing.T) {
	t.Parallel()
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{simulationJob()})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := simulation.RunNext(ctx); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunNext(canceled) ok=%t error=%v", ok, err)
	}
	if simulation.PendingEvents() != 1 || simulation.Now() != 0 {
		t.Fatalf("canceled run changed state: events=%d now=%s", simulation.PendingEvents(), simulation.Now())
	}
}

func TestSimulationRejectsInvalidConstructionAndPlacement(t *testing.T) {
	t.Parallel()
	node := simulationNode()
	job := simulationJob()
	tests := []struct {
		name  string
		nodes []domain.Node
		jobs  []domain.Job
		want  error
	}{
		{"duplicate node", []domain.Node{node, node}, nil, ErrDuplicateNode},
		{"duplicate job", []domain.Node{node}, []domain.Job{job, job}, ErrDuplicateJob},
		{"allocated node", []domain.Node{func() domain.Node { copy := node; copy.Allocated.CPUMillis = 1; return copy }()}, nil, ErrInvalidSimulation},
		{"running initial job", []domain.Node{node}, []domain.Job{func() domain.Job {
			copy := job
			copy.State = domain.JobStateRunning
			copy.AssignedNode = node.ID
			start := copy.SubmitTime
			copy.StartTime = &start
			return copy
		}()}, ErrInvalidSimulation},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSimulation(test.nodes, test.jobs)
			if !errors.Is(err, test.want) {
				t.Fatalf("NewSimulation() error = %v, want %v", err, test.want)
			}
		})
	}

	simulation, err := NewSimulation([]domain.Node{node}, []domain.Job{job})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	placements := []struct {
		name      string
		at        time.Duration
		placement domain.Placement
		want      error
	}{
		{"missing job", job.SubmitTime, domain.Placement{JobID: "missing", NodeID: node.ID}, ErrJobNotFound},
		{"missing node", job.SubmitTime, domain.Placement{JobID: job.ID, NodeID: "missing"}, ErrNodeNotFound},
		{"before submit", job.SubmitTime - time.Second, domain.Placement{JobID: job.ID, NodeID: node.ID}, ErrInvalidTransition},
		{"invalid placement", job.SubmitTime, domain.Placement{JobID: job.ID}, domain.ErrInvalidPlacement},
	}
	for _, test := range placements {
		t.Run(test.name, func(t *testing.T) {
			_, err := simulation.SchedulePlacement(test.at, test.placement)
			if !errors.Is(err, test.want) {
				t.Fatalf("SchedulePlacement() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSimulationDetectsAllocationInvariantViolation(t *testing.T) {
	t.Parallel()
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, nil)
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	node := simulation.nodes["node-1"]
	node.Allocated.CPUMillis = 1
	simulation.nodes[node.ID] = node
	if err := simulation.Validate(); !errors.Is(err, ErrInvalidSimulation) {
		t.Fatalf("Validate() error = %v, want ErrInvalidSimulation", err)
	}
}

func TestSimulationCompletionTimeOverflowIsAtomic(t *testing.T) {
	t.Parallel()
	job := simulationJob()
	job.SubmitTime = 0
	job.Duration = 2
	job.Deadline = nil
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{job})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	start := time.Duration(math.MaxInt64 - 1)
	if _, err := simulation.SchedulePlacement(start, domain.Placement{JobID: job.ID, NodeID: "node-1"}); err != nil {
		t.Fatalf("SchedulePlacement() unexpected error: %v", err)
	}
	if _, _, err := simulation.RunNext(context.Background()); err != nil {
		t.Fatalf("submission unexpected error: %v", err)
	}
	if _, ok, err := simulation.RunNext(context.Background()); !ok || !errors.Is(err, ErrSimulationOverflow) {
		t.Fatalf("overflow start ok=%t error=%v", ok, err)
	}
	assertJobState(t, simulation, domain.JobStateQueued)
	node, _ := simulation.Node("node-1")
	if !node.Allocated.IsZero() {
		t.Fatalf("overflow start allocated resources: %+v", node.Allocated)
	}
}

func TestSimulationAccessorsReturnDefensiveCopies(t *testing.T) {
	t.Parallel()
	simulation, err := NewSimulation([]domain.Node{simulationNode()}, []domain.Job{simulationJob()})
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	node, _ := simulation.Node("node-1")
	node.Labels["pool"] = "changed"
	storedNode, _ := simulation.Node("node-1")
	if storedNode.Labels["pool"] != "default" {
		t.Fatal("Node() exposed mutable labels")
	}
	job, _ := simulation.Job("job-1")
	*job.Deadline = time.Second
	storedJob, _ := simulation.Job("job-1")
	if *storedJob.Deadline != 20*time.Second {
		t.Fatal("Job() exposed mutable time pointer")
	}
}

func TestSimulationCollectionAccessorsAreSortedAndDefensive(t *testing.T) {
	t.Parallel()
	secondNode := simulationNode()
	secondNode.ID = "node-0"
	secondJob := simulationJob()
	secondJob.ID = "job-0"
	secondJob.SubmitTime = time.Second
	simulation, err := NewSimulation(
		[]domain.Node{simulationNode(), secondNode},
		[]domain.Job{simulationJob(), secondJob},
	)
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	if err := simulation.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	nodes := simulation.Nodes()
	if len(nodes) != 2 || nodes[0].ID != "node-0" || nodes[1].ID != "node-1" {
		t.Fatalf("Nodes() = %+v, want node ID order", nodes)
	}
	jobs := simulation.Jobs()
	if len(jobs) != 2 || jobs[0].ID != "job-0" || jobs[1].ID != "job-1" {
		t.Fatalf("Jobs() = %+v, want submission order", jobs)
	}
	nodes[0].Labels["pool"] = "changed"
	jobs[0].State = domain.JobStateRejected
	storedNode, _ := simulation.Node("node-0")
	storedJob, _ := simulation.Job("job-0")
	if storedNode.Labels["pool"] != "default" || storedJob.State != domain.JobStateQueued {
		t.Fatal("collection accessor exposed mutable state")
	}
}

func assertJobState(t *testing.T, simulation *Simulation, want domain.JobState) {
	t.Helper()
	assertJobStateByID(t, simulation, "job-1", want)
}

func assertJobStateByID(t *testing.T, simulation *Simulation, id domain.JobID, want domain.JobState) {
	t.Helper()
	job, ok := simulation.Job(id)
	if !ok {
		t.Fatalf("Job(%q) not found", id)
	}
	if job.State != want {
		t.Fatalf("Job(%q).State = %q, want %q", id, job.State, want)
	}
}
