package simulator

import (
	"errors"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

func TestEventValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		event     Event
		wantError bool
	}{
		{"job submitted", Event{Type: EventJobSubmitted, JobID: "job-1"}, false},
		{"scheduling tick", Event{Type: EventSchedulingTick}, false},
		{"job started", Event{Type: EventJobStarted, JobID: "job-1", NodeID: "node-1"}, false},
		{"job completed", Event{Type: EventJobCompleted, JobID: "job-1", NodeID: "node-1"}, false},
		{"node failed", Event{Type: EventNodeFailed, NodeID: "node-1"}, false},
		{"node recovered", Event{Type: EventNodeRecovered, NodeID: "node-1"}, false},
		{"negative time", Event{Time: -time.Nanosecond, Type: EventSchedulingTick}, true},
		{"unknown type", Event{Type: "unknown"}, true},
		{"submitted missing job", Event{Type: EventJobSubmitted}, true},
		{"started missing node", Event{Type: EventJobStarted, JobID: "job-1"}, true},
		{"completed missing job", Event{Type: EventJobCompleted, NodeID: "node-1"}, true},
		{"failure missing node", Event{Type: EventNodeFailed}, true},
		{"recovery missing node", Event{Type: EventNodeRecovered}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.event.Validate()
			if got := errors.Is(err, ErrInvalidEvent); got != test.wantError {
				t.Fatalf("Validate() error = %v, invalid = %t, want %t", err, got, test.wantError)
			}
		})
	}
}

func FuzzEventValidationBoundary(f *testing.F) {
	f.Add(int64(0), "scheduling_tick", "", "")
	f.Add(int64(-1), "job_started", "job-1", "node-1")
	f.Add(int64(1), "unknown", "job-1", "node-1")
	f.Fuzz(func(t *testing.T, timestamp int64, eventType, jobID, nodeID string) {
		if len(eventType) > 64 || len(jobID) > 64 || len(nodeID) > 64 {
			t.Skip()
		}
		event := Event{
			Time:   time.Duration(timestamp),
			Type:   EventType(eventType),
			JobID:  domain.JobID(jobID),
			NodeID: domain.NodeID(nodeID),
		}
		err := event.Validate()
		if err != nil {
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Validate() error = %v, want ErrInvalidEvent", err)
			}
			return
		}
		queue := NewEventQueue()
		scheduled, err := queue.Schedule(event)
		if err != nil {
			t.Fatalf("Schedule(valid event) unexpected error: %v", err)
		}
		if scheduled.Time != event.Time || scheduled.Type != event.Type || scheduled.JobID != event.JobID || scheduled.NodeID != event.NodeID {
			t.Fatalf("scheduled event = %+v, want input fields %+v", scheduled, event)
		}
	})
}
