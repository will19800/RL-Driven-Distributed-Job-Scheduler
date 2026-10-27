package simulator

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestEventQueueOrdersByTimeThenInsertion(t *testing.T) {
	t.Parallel()
	queue := NewEventQueue()
	inputs := []Event{
		{Time: 5 * time.Second, Type: EventSchedulingTick},
		{Time: time.Second, Type: EventJobSubmitted, JobID: "first-at-one"},
		{Time: time.Second, Type: EventJobSubmitted, JobID: "second-at-one"},
		{Time: 3 * time.Second, Type: EventNodeFailed, NodeID: "node-1"},
	}
	for _, event := range inputs {
		if _, err := queue.Schedule(event); err != nil {
			t.Fatalf("Schedule() unexpected error: %v", err)
		}
	}
	wantJobs := []string{"first-at-one", "second-at-one", "", ""}
	wantTimes := []time.Duration{time.Second, time.Second, 3 * time.Second, 5 * time.Second}
	for index := range wantTimes {
		event, ok, err := queue.Pop(context.Background())
		if err != nil || !ok {
			t.Fatalf("Pop() = %+v, %t, %v; want event", event, ok, err)
		}
		if event.Time != wantTimes[index] || string(event.JobID) != wantJobs[index] {
			t.Fatalf("event %d = %+v, want time=%s job=%q", index, event, wantTimes[index], wantJobs[index])
		}
	}
	if queue.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", queue.Len())
	}
}

func TestEventQueueAssignsSequencesAndIgnoresCallerValue(t *testing.T) {
	t.Parallel()
	queue := NewEventQueue()
	first, err := queue.Schedule(Event{Type: EventSchedulingTick, Sequence: 99})
	if err != nil {
		t.Fatalf("Schedule(first) unexpected error: %v", err)
	}
	second, err := queue.Schedule(Event{Type: EventSchedulingTick})
	if err != nil {
		t.Fatalf("Schedule(second) unexpected error: %v", err)
	}
	if first.Sequence != 0 || second.Sequence != 1 {
		t.Fatalf("sequences = %d, %d; want 0, 1", first.Sequence, second.Sequence)
	}
}

func TestEventQueuePeekAndEmptyPop(t *testing.T) {
	t.Parallel()
	queue := NewEventQueue()
	if _, ok := queue.Peek(); ok {
		t.Fatal("Peek() returned an event for an empty queue")
	}
	want, err := queue.Schedule(Event{Time: time.Second, Type: EventSchedulingTick})
	if err != nil {
		t.Fatalf("Schedule() unexpected error: %v", err)
	}
	got, ok := queue.Peek()
	if !ok || got != want || queue.Len() != 1 {
		t.Fatalf("Peek() = %+v, %t, len=%d; want %+v, true, 1", got, ok, queue.Len(), want)
	}
	if _, ok, err := queue.Pop(context.Background()); err != nil || !ok {
		t.Fatalf("Pop() ok=%t error=%v; want true, nil", ok, err)
	}
	if _, ok, err := queue.Pop(context.Background()); err != nil || ok {
		t.Fatalf("empty Pop() ok=%t error=%v; want false, nil", ok, err)
	}
}

func TestEventQueueCancellationDoesNotRemoveEvent(t *testing.T) {
	t.Parallel()
	queue := NewEventQueue()
	if _, err := queue.Schedule(Event{Type: EventSchedulingTick}); err != nil {
		t.Fatalf("Schedule() unexpected error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := queue.Pop(ctx); !errors.Is(err, context.Canceled) || ok {
		t.Fatalf("Pop(canceled) ok=%t error=%v; want false, context.Canceled", ok, err)
	}
	if queue.Len() != 1 {
		t.Fatalf("canceled Pop() changed queue length to %d", queue.Len())
	}
}

func TestEventQueueRejectsInvalidEventAndSequenceExhaustion(t *testing.T) {
	t.Parallel()
	queue := NewEventQueue()
	if _, err := queue.Schedule(Event{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("Schedule(invalid) error = %v, want ErrInvalidEvent", err)
	}
	queue.nextSequence = math.MaxUint64
	if _, err := queue.Schedule(Event{Type: EventSchedulingTick}); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("Schedule(exhausted) error = %v, want ErrSequenceExhausted", err)
	}
}

func FuzzEventQueueChronologicalOrder(f *testing.F) {
	f.Add([]byte{5, 1, 1, 3, 0, 255})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, timestamps []byte) {
		queue := NewEventQueue()
		for _, timestamp := range timestamps {
			if _, err := queue.Schedule(Event{Time: time.Duration(timestamp), Type: EventSchedulingTick}); err != nil {
				t.Fatalf("Schedule() unexpected error: %v", err)
			}
		}
		var previous Event
		for index := 0; queue.Len() > 0; index++ {
			event, ok, err := queue.Pop(context.Background())
			if err != nil || !ok {
				t.Fatalf("Pop() = %+v, %t, %v; want event", event, ok, err)
			}
			if index > 0 && (event.Time < previous.Time || (event.Time == previous.Time && event.Sequence < previous.Sequence)) {
				t.Fatalf("queue order regressed from %+v to %+v", previous, event)
			}
			previous = event
		}
	})
}
