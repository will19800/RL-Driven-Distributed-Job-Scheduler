package simulator

import (
	"errors"
	"testing"
	"time"
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
