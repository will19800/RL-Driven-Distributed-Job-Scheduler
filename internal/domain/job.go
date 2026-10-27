package domain

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidJob indicates that a job violates a domain invariant.
var ErrInvalidJob = errors.New("invalid job")

// JobID uniquely identifies a job within one simulation.
type JobID string

// JobState describes a job's non-preemptive lifecycle state.
type JobState string

const (
	// JobStateQueued is waiting for placement.
	JobStateQueued JobState = "queued"
	// JobStateRunning has a node assignment and start time.
	JobStateRunning JobState = "running"
	// JobStateCompleted finished after running.
	JobStateCompleted JobState = "completed"
	// JobStateRejected ended without receiving a node assignment.
	JobStateRejected JobState = "rejected"
)

// Job describes one single-node, non-preemptive unit of work. Times are offsets
// from simulation start, not wall-clock timestamps.
type Job struct {
	ID              JobID          `json:"id" yaml:"id"`
	SubmitTime      time.Duration  `json:"submit_time" yaml:"submit_time"`
	Request         ResourceVector `json:"request" yaml:"request"`
	Duration        time.Duration  `json:"duration" yaml:"duration"`
	Priority        int32          `json:"priority" yaml:"priority"`
	Deadline        *time.Duration `json:"deadline,omitempty" yaml:"deadline,omitempty"`
	State           JobState       `json:"state" yaml:"state"`
	AssignedNode    NodeID         `json:"assigned_node,omitempty" yaml:"assigned_node,omitempty"`
	StartTime       *time.Duration `json:"start_time,omitempty" yaml:"start_time,omitempty"`
	EndTime         *time.Duration `json:"end_time,omitempty" yaml:"end_time,omitempty"`
	RejectionReason string         `json:"rejection_reason,omitempty" yaml:"rejection_reason,omitempty"`
}

// Validate checks the job definition and state-specific lifecycle invariants.
func (j Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidJob)
	}
	if j.SubmitTime < 0 {
		return fmt.Errorf("%w: submit time must be non-negative", ErrInvalidJob)
	}
	if err := j.Request.Validate(); err != nil || j.Request.IsZero() {
		return fmt.Errorf("%w: request must contain valid non-zero resources", ErrInvalidJob)
	}
	if j.Duration <= 0 {
		return fmt.Errorf("%w: duration must be positive", ErrInvalidJob)
	}
	if j.Priority < 0 {
		return fmt.Errorf("%w: priority must be non-negative", ErrInvalidJob)
	}
	if j.Deadline != nil && *j.Deadline < j.SubmitTime {
		return fmt.Errorf("%w: deadline precedes submit time", ErrInvalidJob)
	}
	switch j.State {
	case JobStateQueued:
		return j.validateQueued()
	case JobStateRunning:
		return j.validateRunning()
	case JobStateCompleted:
		return j.validateCompleted()
	case JobStateRejected:
		return j.validateRejected()
	default:
		return fmt.Errorf("%w: unknown state %q", ErrInvalidJob, j.State)
	}
}

func (j Job) validateQueued() error {
	if j.AssignedNode != "" || j.StartTime != nil || j.EndTime != nil || j.RejectionReason != "" {
		return fmt.Errorf("%w: queued job has terminal or assignment data", ErrInvalidJob)
	}
	return nil
}

func (j Job) validateRunning() error {
	if j.AssignedNode == "" || j.StartTime == nil {
		return fmt.Errorf("%w: running job requires node and start time", ErrInvalidJob)
	}
	if *j.StartTime < j.SubmitTime {
		return fmt.Errorf("%w: start time precedes submit time", ErrInvalidJob)
	}
	if j.EndTime != nil || j.RejectionReason != "" {
		return fmt.Errorf("%w: running job has terminal data", ErrInvalidJob)
	}
	return nil
}

func (j Job) validateCompleted() error {
	if j.AssignedNode == "" || j.StartTime == nil || j.EndTime == nil {
		return fmt.Errorf("%w: completed job requires node, start, and end times", ErrInvalidJob)
	}
	if *j.StartTime < j.SubmitTime || *j.EndTime < *j.StartTime {
		return fmt.Errorf("%w: completed job times are out of order", ErrInvalidJob)
	}
	if j.RejectionReason != "" {
		return fmt.Errorf("%w: completed job has rejection reason", ErrInvalidJob)
	}
	return nil
}

func (j Job) validateRejected() error {
	if j.EndTime == nil || j.RejectionReason == "" {
		return fmt.Errorf("%w: rejected job requires end time and reason", ErrInvalidJob)
	}
	if *j.EndTime < j.SubmitTime {
		return fmt.Errorf("%w: rejection time precedes submit time", ErrInvalidJob)
	}
	if j.AssignedNode != "" || j.StartTime != nil {
		return fmt.Errorf("%w: rejected job must not have an assignment", ErrInvalidJob)
	}
	return nil
}
