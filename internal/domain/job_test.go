package domain

import (
	"errors"
	"testing"
	"time"
)

func durationPointer(value time.Duration) *time.Duration { return &value }

func validQueuedJob() Job {
	deadline := 20 * time.Second
	return Job{ID: "job-1", SubmitTime: 2 * time.Second, Request: ResourceVector{CPUMillis: 500, MemoryMiB: 256}, Duration: 5 * time.Second, Priority: 1, Deadline: &deadline, State: JobStateQueued}
}

func TestJobValidateAcceptsLifecycleStates(t *testing.T) {
	t.Parallel()
	queued := validQueuedJob()
	running := validQueuedJob()
	running.State, running.AssignedNode, running.StartTime = JobStateRunning, "node-1", durationPointer(3*time.Second)
	completed := running
	completed.State, completed.EndTime = JobStateCompleted, durationPointer(8*time.Second)
	rejected := validQueuedJob()
	rejected.State, rejected.EndTime, rejected.RejectionReason = JobStateRejected, durationPointer(4*time.Second), "queue limit"
	for _, job := range []Job{queued, running, completed, rejected} {
		if err := job.Validate(); err != nil {
			t.Errorf("Validate() for %q returned %v", job.State, err)
		}
	}
}

func TestJobValidateRejectsInvalidBaseFields(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Job){
		"missing id": func(j *Job) { j.ID = "" }, "negative submit": func(j *Job) { j.SubmitTime = -1 },
		"empty request": func(j *Job) { j.Request = ResourceVector{} }, "negative request": func(j *Job) { j.Request.CPUMillis = -1 },
		"zero duration": func(j *Job) { j.Duration = 0 }, "negative priority": func(j *Job) { j.Priority = -1 },
		"early deadline": func(j *Job) { j.Deadline = durationPointer(time.Second) }, "unknown state": func(j *Job) { j.State = "paused" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			job := validQueuedJob()
			mutate(&job)
			if err := job.Validate(); !errors.Is(err, ErrInvalidJob) {
				t.Fatalf("Validate() error = %v, want ErrInvalidJob", err)
			}
		})
	}
}

func TestJobValidateRejectsInconsistentLifecycleData(t *testing.T) {
	t.Parallel()
	tests := map[string]Job{}
	queuedAssigned := validQueuedJob()
	queuedAssigned.AssignedNode = "node-1"
	tests["queued assigned"] = queuedAssigned
	runningNoNode := validQueuedJob()
	runningNoNode.State = JobStateRunning
	runningNoNode.StartTime = durationPointer(3 * time.Second)
	tests["running without node"] = runningNoNode
	runningEarly := validQueuedJob()
	runningEarly.State = JobStateRunning
	runningEarly.AssignedNode = "node-1"
	runningEarly.StartTime = durationPointer(time.Second)
	tests["running early"] = runningEarly
	completedEarly := validQueuedJob()
	completedEarly.State = JobStateCompleted
	completedEarly.AssignedNode = "node-1"
	completedEarly.StartTime = durationPointer(5 * time.Second)
	completedEarly.EndTime = durationPointer(4 * time.Second)
	tests["completed early"] = completedEarly
	rejectedNoReason := validQueuedJob()
	rejectedNoReason.State = JobStateRejected
	rejectedNoReason.EndTime = durationPointer(3 * time.Second)
	tests["rejected without reason"] = rejectedNoReason
	for name, job := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := job.Validate(); !errors.Is(err, ErrInvalidJob) {
				t.Fatalf("Validate() error = %v, want ErrInvalidJob", err)
			}
		})
	}
}
