package simulator

import (
	"container/heap"
	"context"
	"errors"
	"math"
)

var ErrSequenceExhausted = errors.New("event sequence exhausted")

// EventQueue is a deterministic min-heap ordered by event time and then by
// insertion sequence. It is not safe for concurrent use by design.
type EventQueue struct {
	events       eventHeap
	nextSequence uint64
}

// NewEventQueue constructs an empty event queue.
func NewEventQueue() *EventQueue {
	queue := &EventQueue{}
	heap.Init(&queue.events)
	return queue
}

// Len returns the number of pending events.
func (q *EventQueue) Len() int {
	return q.events.Len()
}

// Schedule validates and inserts event. Any caller-provided sequence is
// ignored so ordering depends only on insertion order.
func (q *EventQueue) Schedule(event Event) (Event, error) {
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	if q.nextSequence == math.MaxUint64 {
		return Event{}, ErrSequenceExhausted
	}
	event.Sequence = q.nextSequence
	q.nextSequence++
	heap.Push(&q.events, event)
	return event, nil
}

// Peek returns the next event without removing it.
func (q *EventQueue) Peek() (Event, bool) {
	if q.Len() == 0 {
		return Event{}, false
	}
	return q.events[0], true
}

// Pop returns and removes the next event. A canceled context leaves the queue
// unchanged, allowing a caller to resume or inspect the simulation safely.
func (q *EventQueue) Pop(ctx context.Context) (Event, bool, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, false, err
	}
	if q.Len() == 0 {
		return Event{}, false, nil
	}
	return heap.Pop(&q.events).(Event), true, nil
}

type eventHeap []Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(left, right int) bool {
	if h[left].Time == h[right].Time {
		return h[left].Sequence < h[right].Sequence
	}
	return h[left].Time < h[right].Time
}

func (h eventHeap) Swap(left, right int) { h[left], h[right] = h[right], h[left] }

func (h *eventHeap) Push(value any) {
	*h = append(*h, value.(Event))
}

func (h *eventHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = Event{}
	*h = old[:last]
	return value
}
