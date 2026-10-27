package simulator

import (
	"errors"
	"fmt"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

var ErrInvalidEvent = errors.New("invalid simulation event")

// EventType identifies a state transition handled by the simulation kernel.
type EventType string

const (
	EventJobSubmitted   EventType = "job_submitted"
	EventSchedulingTick EventType = "scheduling_tick"
	EventJobStarted     EventType = "job_started"
	EventJobCompleted   EventType = "job_completed"
	EventNodeFailed     EventType = "node_failed"
	EventNodeRecovered  EventType = "node_recovered"
)

// Event is an immutable scheduled transition. Sequence is assigned by the
// queue and provides deterministic ordering when timestamps are equal.
type Event struct {
	Time     time.Duration `json:"time"`
	Sequence uint64        `json:"sequence"`
	Type     EventType     `json:"type"`
	JobID    domain.JobID  `json:"job_id,omitempty"`
	NodeID   domain.NodeID `json:"node_id,omitempty"`
}

// Validate checks timestamp, event type, and required entity identifiers.
func (e Event) Validate() error {
	if e.Time < 0 {
		return fmt.Errorf("%w: negative timestamp %s", ErrInvalidEvent, e.Time)
	}
	switch e.Type {
	case EventJobSubmitted:
		if e.JobID == "" {
			return fmt.Errorf("%w: %s requires a job id", ErrInvalidEvent, e.Type)
		}
	case EventJobStarted, EventJobCompleted:
		if e.JobID == "" || e.NodeID == "" {
			return fmt.Errorf("%w: %s requires job and node ids", ErrInvalidEvent, e.Type)
		}
	case EventNodeFailed, EventNodeRecovered:
		if e.NodeID == "" {
			return fmt.Errorf("%w: %s requires a node id", ErrInvalidEvent, e.Type)
		}
	case EventSchedulingTick:
		// Scheduling ticks are global and require no entity identifier.
	default:
		return fmt.Errorf("%w: unknown type %q", ErrInvalidEvent, e.Type)
	}
	return nil
}
