package policyv1

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

type fixture struct {
	Config Config `json:"config"`
	State  struct {
		NowSeconds  float64       `json:"now_seconds"`
		ArrivalRate float64       `json:"arrival_rate"`
		Nodes       []fixtureNode `json:"nodes"`
		Jobs        []fixtureJob  `json:"jobs"`
	} `json:"state"`
	ExpectedSchema Schema      `json:"expected_schema"`
	Expected       Observation `json:"expected"`
}
type fixtureNode struct {
	ID                  string `json:"id"`
	Capacity, Allocated domain.ResourceVector
	State               domain.NodeState `json:"state"`
}
type fixtureJob struct {
	ID                string                `json:"id"`
	SubmitTimeSeconds float64               `json:"submit_time_seconds"`
	Request           domain.ResourceVector `json:"request"`
	DurationSeconds   float64               `json:"duration_seconds"`
	Priority          int32                 `json:"priority"`
	DeadlineSeconds   *float64              `json:"deadline_seconds"`
	State             domain.JobState       `json:"state"`
}

func TestEncodeMatchesSharedParityFixture(t *testing.T) {
	t.Parallel()
	value := loadFixture(t)
	gotSchema, err := value.Config.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSchema, value.ExpectedSchema) {
		t.Fatalf("schema = %+v, want %+v", gotSchema, value.ExpectedSchema)
	}
	nodes := make([]domain.Node, 0, len(value.State.Nodes))
	for _, item := range value.State.Nodes {
		nodes = append(nodes, domain.Node{ID: domain.NodeID(item.ID), Capacity: item.Capacity, Allocated: item.Allocated, State: item.State})
	}
	jobs := make([]domain.Job, 0, len(value.State.Jobs))
	for _, item := range value.State.Jobs {
		job := domain.Job{ID: domain.JobID(item.ID), SubmitTime: seconds(item.SubmitTimeSeconds), Request: item.Request, Duration: seconds(item.DurationSeconds), Priority: item.Priority, State: item.State}
		if item.DeadlineSeconds != nil {
			deadline := seconds(*item.DeadlineSeconds)
			job.Deadline = &deadline
		}
		if item.State == domain.JobStateRunning {
			started := job.SubmitTime
			job.StartTime = &started
			job.AssignedNode = "node-b"
		}
		jobs = append(jobs, job)
	}
	snapshot, err := scheduler.NewSnapshot(seconds(value.State.NowSeconds), nodes, jobs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Encode(value.Config, snapshot, value.State.ArrivalRate)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, value.Expected) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(value.Expected, "", "  ")
		t.Fatalf("observation mismatch\ngot=%s\nwant=%s", gotJSON, wantJSON)
	}
}

func TestSchemaAndActions(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.MaxJobs, config.MaxNodes = 1, 1
	schema, err := config.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if schema.Version != Version || schema.DeferAction != 1 || schema.PlacementActionCount != 1 {
		t.Fatalf("schema = %+v", schema)
	}
	observation := Observation{Version: Version, ActionMask: []bool{true, true}, JobIDs: []string{"job"}, NodeIDs: []string{"node"}}
	action, err := DecodeAction(config, observation, 0)
	if err != nil {
		t.Fatal(err)
	}
	placement, ok := action.Placement(observation)
	if !ok || placement.JobID != "job" || placement.NodeID != "node" {
		t.Fatalf("placement = %+v, %t", placement, ok)
	}
	deferAction, err := DecodeAction(config, observation, 1)
	if err != nil || !deferAction.Defer {
		t.Fatalf("defer = %+v, %v", deferAction, err)
	}
	observation.ActionMask[0] = false
	if _, err := DecodeAction(config, observation, 0); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("masked error = %v", err)
	}
}

func TestRejectsInvalidContractInputs(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.MaxJobs = 0
	if err := config.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate() = %v", err)
	}
	config = DefaultConfig()
	config.Normalization.ArrivalRate = 0
	if err := config.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate() = %v", err)
	}
	config = DefaultConfig()
	snapshot, err := scheduler.NewSnapshot(0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Encode(config, snapshot, -1); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Encode() = %v", err)
	}
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile("testdata/parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var value fixture
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func seconds(value float64) time.Duration { return time.Duration(value * float64(time.Second)) }
