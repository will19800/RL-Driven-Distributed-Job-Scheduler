package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

type goldenScenario struct {
	SchemaVersion string        `json:"schema_version"`
	Events        []goldenEvent `json:"events"`
	Now           string        `json:"now"`
	Nodes         []goldenNode  `json:"nodes"`
	Jobs          []goldenJob   `json:"jobs"`
}

type goldenEvent struct {
	Time     string        `json:"time"`
	Sequence uint64        `json:"sequence"`
	Type     EventType     `json:"type"`
	JobID    domain.JobID  `json:"job_id,omitempty"`
	NodeID   domain.NodeID `json:"node_id,omitempty"`
}

type goldenNode struct {
	ID        domain.NodeID         `json:"id"`
	Allocated domain.ResourceVector `json:"allocated"`
}

type goldenJob struct {
	ID           domain.JobID    `json:"id"`
	State        domain.JobState `json:"state"`
	AssignedNode domain.NodeID   `json:"assigned_node"`
	StartTime    string          `json:"start_time"`
	EndTime      string          `json:"end_time"`
}

func TestSimulationGoldenInvariantScenario(t *testing.T) {
	t.Parallel()
	nodes := []domain.Node{
		{ID: "node-a", Capacity: domain.ResourceVector{CPUMillis: 2_000}, State: domain.NodeStateAvailable},
		{ID: "node-b", Capacity: domain.ResourceVector{CPUMillis: 1_000}, State: domain.NodeStateAvailable},
	}
	jobs := []domain.Job{
		{ID: "job-a", Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: 3 * time.Second, State: domain.JobStateQueued},
		{ID: "job-b", Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: time.Second, State: domain.JobStateQueued},
		{ID: "job-c", SubmitTime: time.Second, Request: domain.ResourceVector{CPUMillis: 1_000}, Duration: 2 * time.Second, State: domain.JobStateQueued},
	}
	simulation, err := NewSimulation(nodes, jobs)
	if err != nil {
		t.Fatalf("NewSimulation() unexpected error: %v", err)
	}
	placements := []struct {
		at     time.Duration
		jobID  domain.JobID
		nodeID domain.NodeID
	}{
		{0, "job-a", "node-a"},
		{0, "job-b", "node-a"},
		{time.Second, "job-c", "node-b"},
	}
	for _, input := range placements {
		if _, err := simulation.SchedulePlacement(input.at, domain.Placement{JobID: input.jobID, NodeID: input.nodeID}); err != nil {
			t.Fatalf("SchedulePlacement(%q) unexpected error: %v", input.jobID, err)
		}
	}

	result := goldenScenario{SchemaVersion: "v1"}
	for {
		event, ok, err := simulation.RunNext(context.Background())
		if err != nil {
			t.Fatalf("RunNext() unexpected error: %v", err)
		}
		if !ok {
			break
		}
		if err := simulation.Validate(); err != nil {
			t.Fatalf("Validate() after %s unexpected error: %v", event.Type, err)
		}
		result.Events = append(result.Events, goldenEvent{
			Time: event.Time.String(), Sequence: event.Sequence, Type: event.Type,
			JobID: event.JobID, NodeID: event.NodeID,
		})
	}
	result.Now = simulation.Now().String()
	for _, node := range simulation.Nodes() {
		result.Nodes = append(result.Nodes, goldenNode{ID: node.ID, Allocated: node.Allocated})
	}
	for _, job := range simulation.Jobs() {
		result.Jobs = append(result.Jobs, goldenJob{
			ID: job.ID, State: job.State, AssignedNode: job.AssignedNode,
			StartTime: goldenDuration(job.StartTime), EndTime: goldenDuration(job.EndTime),
		})
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent() unexpected error: %v", err)
	}
	encoded = append(encoded, '\n')
	want, err := os.ReadFile(filepath.Join("testdata", "invariant_scenario.golden.json"))
	if err != nil {
		t.Fatalf("ReadFile(golden) unexpected error: %v", err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("golden scenario mismatch\n--- got ---\n%s--- want ---\n%s", encoded, want)
	}
}

func goldenDuration(value *time.Duration) string {
	if value == nil {
		return ""
	}
	return value.String()
}
