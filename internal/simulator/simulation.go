package simulator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

var (
	ErrInvalidSimulation  = errors.New("invalid simulation state")
	ErrDuplicateNode      = errors.New("duplicate node")
	ErrDuplicateJob       = errors.New("duplicate job")
	ErrNodeNotFound       = errors.New("node not found")
	ErrJobNotFound        = errors.New("job not found")
	ErrInvalidTransition  = errors.New("invalid job transition")
	ErrUnsupportedEvent   = errors.New("unsupported event")
	ErrSimulationOverflow = errors.New("simulation time overflow")
)

// Simulation owns all mutable cluster state and applies queued events in a
// deterministic order. State is copied and validated before each commit so a
// rejected transition cannot partially update jobs or node allocations.
type Simulation struct {
	clock   *Clock
	events  *EventQueue
	nodes   map[domain.NodeID]domain.Node
	jobs    map[domain.JobID]domain.Job
	pending map[domain.JobID]domain.Job
}

// NewSimulation validates its inputs and schedules each job submission.
// Initial nodes must have no allocation because no jobs are running yet.
func NewSimulation(nodes []domain.Node, jobs []domain.Job) (*Simulation, error) {
	clock, err := NewClock(0)
	if err != nil {
		return nil, err
	}
	simulation := &Simulation{
		clock:   clock,
		events:  NewEventQueue(),
		nodes:   make(map[domain.NodeID]domain.Node, len(nodes)),
		jobs:    make(map[domain.JobID]domain.Job, len(jobs)),
		pending: make(map[domain.JobID]domain.Job, len(jobs)),
	}

	for _, input := range nodes {
		node := cloneNode(input)
		if err := node.Validate(); err != nil {
			return nil, fmt.Errorf("%w: node %q: %v", ErrInvalidSimulation, node.ID, err)
		}
		if _, exists := simulation.nodes[node.ID]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateNode, node.ID)
		}
		simulation.nodes[node.ID] = node
	}

	for _, input := range jobs {
		job := cloneJob(input)
		if err := job.Validate(); err != nil {
			return nil, fmt.Errorf("%w: job %q: %v", ErrInvalidSimulation, job.ID, err)
		}
		if job.State != domain.JobStateQueued {
			return nil, fmt.Errorf("%w: initial job %q must be queued", ErrInvalidSimulation, job.ID)
		}
		if _, exists := simulation.pending[job.ID]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateJob, job.ID)
		}
		simulation.pending[job.ID] = job
		if _, err := simulation.events.Schedule(Event{
			Time:  job.SubmitTime,
			Type:  EventJobSubmitted,
			JobID: job.ID,
		}); err != nil {
			return nil, fmt.Errorf("schedule job %q submission: %w", job.ID, err)
		}
	}

	if err := simulation.Validate(); err != nil {
		return nil, err
	}
	return simulation, nil
}

// Now returns the current simulated time.
func (s *Simulation) Now() time.Duration { return s.clock.Now() }

// PendingEvents returns the number of events waiting to be processed.
func (s *Simulation) PendingEvents() int { return s.events.Len() }

// NextEvent returns the next event without consuming it.
func (s *Simulation) NextEvent() (Event, bool) { return s.events.Peek() }

// Node returns a defensive copy of a node.
func (s *Simulation) Node(id domain.NodeID) (domain.Node, bool) {
	node, ok := s.nodes[id]
	return cloneNode(node), ok
}

// Nodes returns defensive copies ordered by node ID.
func (s *Simulation) Nodes() []domain.Node {
	nodes := make([]domain.Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, cloneNode(node))
	}
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
	return nodes
}

// Job returns a defensive copy of an active or not-yet-submitted job.
func (s *Simulation) Job(id domain.JobID) (domain.Job, bool) {
	if job, ok := s.jobs[id]; ok {
		return cloneJob(job), true
	}
	job, ok := s.pending[id]
	return cloneJob(job), ok
}

// Jobs returns defensive copies of submitted jobs ordered by submission time
// and then job ID. Future jobs are omitted until their submission event runs.
func (s *Simulation) Jobs() []domain.Job {
	jobs := make([]domain.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	sort.Slice(jobs, func(left, right int) bool {
		if jobs[left].SubmitTime == jobs[right].SubmitTime {
			return jobs[left].ID < jobs[right].ID
		}
		return jobs[left].SubmitTime < jobs[right].SubmitTime
	})
	return jobs
}

// SchedulePlacement schedules a start transition. Feasibility is checked when
// the event executes because earlier events may change available resources.
func (s *Simulation) SchedulePlacement(at time.Duration, placement domain.Placement) (Event, error) {
	if err := placement.Validate(); err != nil {
		return Event{}, err
	}
	if at < s.Now() {
		return Event{}, fmt.Errorf("%w: current=%s target=%s", ErrTimeReversal, s.Now(), at)
	}
	job, exists := s.jobs[placement.JobID]
	if !exists {
		job, exists = s.pending[placement.JobID]
	}
	if !exists {
		return Event{}, fmt.Errorf("%w: %q", ErrJobNotFound, placement.JobID)
	}
	if _, exists := s.nodes[placement.NodeID]; !exists {
		return Event{}, fmt.Errorf("%w: %q", ErrNodeNotFound, placement.NodeID)
	}
	if at < job.SubmitTime {
		return Event{}, fmt.Errorf("%w: job %q starts before submission", ErrInvalidTransition, job.ID)
	}
	return s.events.Schedule(Event{
		Time:   at,
		Type:   EventJobStarted,
		JobID:  placement.JobID,
		NodeID: placement.NodeID,
	})
}

// RunNext processes one event. Failed events are consumed, but clock and state
// remain unchanged. The caller may correct the cause and schedule another event.
func (s *Simulation) RunNext(ctx context.Context) (Event, bool, error) {
	event, ok, err := s.events.Pop(ctx)
	if err != nil || !ok {
		return event, ok, err
	}
	if event.Time < s.Now() {
		return event, true, fmt.Errorf("%w: current=%s event=%s", ErrTimeReversal, s.Now(), event.Time)
	}
	nextClock, err := NewClock(s.Now())
	if err != nil {
		return event, true, err
	}
	if err := nextClock.AdvanceTo(event.Time); err != nil {
		return event, true, err
	}

	nodes, jobs, pending := s.cloneState()
	completion, err := applyEvent(event, nodes, jobs, pending)
	if err != nil {
		return event, true, err
	}
	if err := validateState(nodes, jobs, pending); err != nil {
		return event, true, err
	}
	if completion != nil {
		if _, err := s.events.Schedule(*completion); err != nil {
			return event, true, fmt.Errorf("schedule completion: %w", err)
		}
	}

	s.nodes, s.jobs, s.pending, s.clock = nodes, jobs, pending, nextClock
	return event, true, nil
}

// Run processes events until the queue is empty, the context is canceled, or a
// transition fails.
func (s *Simulation) Run(ctx context.Context) error {
	for {
		_, ok, err := s.RunNext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
}

// Validate checks all job, node, identity, and allocation invariants.
func (s *Simulation) Validate() error {
	return validateState(s.nodes, s.jobs, s.pending)
}

func applyEvent(event Event, nodes map[domain.NodeID]domain.Node, jobs, pending map[domain.JobID]domain.Job) (*Event, error) {
	switch event.Type {
	case EventJobSubmitted:
		job, exists := pending[event.JobID]
		if !exists {
			return nil, fmt.Errorf("%w: pending job %q", ErrJobNotFound, event.JobID)
		}
		if event.Time != job.SubmitTime {
			return nil, fmt.Errorf("%w: submission time mismatch for job %q", ErrInvalidTransition, job.ID)
		}
		delete(pending, job.ID)
		jobs[job.ID] = job
		return nil, nil

	case EventJobStarted:
		job, exists := jobs[event.JobID]
		if !exists {
			return nil, fmt.Errorf("%w: active job %q", ErrJobNotFound, event.JobID)
		}
		if job.State != domain.JobStateQueued {
			return nil, fmt.Errorf("%w: job %q is %s, not queued", ErrInvalidTransition, job.ID, job.State)
		}
		node, exists := nodes[event.NodeID]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, event.NodeID)
		}
		if !node.CanFit(job.Request) {
			return nil, fmt.Errorf("%w: job %q does not fit node %q", ErrInvalidTransition, job.ID, node.ID)
		}
		if event.Time > time.Duration(math.MaxInt64)-job.Duration {
			return nil, fmt.Errorf("%w: completion time for job %q", ErrSimulationOverflow, job.ID)
		}
		allocated, err := node.Allocated.Add(job.Request)
		if err != nil {
			return nil, fmt.Errorf("allocate job %q: %w", job.ID, err)
		}
		startTime := event.Time
		job.State = domain.JobStateRunning
		job.AssignedNode = node.ID
		job.StartTime = &startTime
		node.Allocated = allocated
		jobs[job.ID], nodes[node.ID] = job, node
		completion := Event{
			Time:   event.Time + job.Duration,
			Type:   EventJobCompleted,
			JobID:  job.ID,
			NodeID: node.ID,
		}
		return &completion, nil

	case EventJobCompleted:
		job, exists := jobs[event.JobID]
		if !exists {
			return nil, fmt.Errorf("%w: active job %q", ErrJobNotFound, event.JobID)
		}
		if job.State != domain.JobStateRunning || job.AssignedNode != event.NodeID {
			return nil, fmt.Errorf("%w: completion does not match running job %q", ErrInvalidTransition, job.ID)
		}
		node, exists := nodes[event.NodeID]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, event.NodeID)
		}
		allocated, err := node.Allocated.Subtract(job.Request)
		if err != nil {
			return nil, fmt.Errorf("release job %q: %w", job.ID, err)
		}
		endTime := event.Time
		job.State = domain.JobStateCompleted
		job.EndTime = &endTime
		node.Allocated = allocated
		jobs[job.ID], nodes[node.ID] = job, node
		return nil, nil

	case EventSchedulingTick:
		return nil, nil
	case EventNodeFailed, EventNodeRecovered:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedEvent, event.Type)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedEvent, event.Type)
	}
}

func validateState(nodes map[domain.NodeID]domain.Node, jobs, pending map[domain.JobID]domain.Job) error {
	expected := make(map[domain.NodeID]domain.ResourceVector, len(nodes))
	for id, node := range nodes {
		if id != node.ID {
			return fmt.Errorf("%w: node map key %q does not match id %q", ErrInvalidSimulation, id, node.ID)
		}
		if err := node.Validate(); err != nil {
			return fmt.Errorf("%w: node %q: %v", ErrInvalidSimulation, id, err)
		}
		expected[id] = domain.ResourceVector{}
	}
	for id, job := range pending {
		if id != job.ID || job.State != domain.JobStateQueued {
			return fmt.Errorf("%w: malformed pending job %q", ErrInvalidSimulation, id)
		}
		if _, exists := jobs[id]; exists {
			return fmt.Errorf("%w: job %q is pending and active", ErrInvalidSimulation, id)
		}
		if err := job.Validate(); err != nil {
			return fmt.Errorf("%w: pending job %q: %v", ErrInvalidSimulation, id, err)
		}
	}
	for id, job := range jobs {
		if id != job.ID {
			return fmt.Errorf("%w: job map key %q does not match id %q", ErrInvalidSimulation, id, job.ID)
		}
		if err := job.Validate(); err != nil {
			return fmt.Errorf("%w: job %q: %v", ErrInvalidSimulation, id, err)
		}
		if job.AssignedNode != "" {
			if _, exists := nodes[job.AssignedNode]; !exists {
				return fmt.Errorf("%w: job %q references missing node %q", ErrInvalidSimulation, id, job.AssignedNode)
			}
		}
		if job.State == domain.JobStateRunning {
			allocation, err := expected[job.AssignedNode].Add(job.Request)
			if err != nil {
				return fmt.Errorf("%w: total allocation on node %q: %v", ErrInvalidSimulation, job.AssignedNode, err)
			}
			expected[job.AssignedNode] = allocation
		}
	}
	for id, node := range nodes {
		if node.Allocated != expected[id] {
			return fmt.Errorf("%w: node %q allocation %+v does not match running jobs %+v", ErrInvalidSimulation, id, node.Allocated, expected[id])
		}
	}
	return nil
}

func (s *Simulation) cloneState() (map[domain.NodeID]domain.Node, map[domain.JobID]domain.Job, map[domain.JobID]domain.Job) {
	nodes := make(map[domain.NodeID]domain.Node, len(s.nodes))
	for id, node := range s.nodes {
		nodes[id] = cloneNode(node)
	}
	jobs := make(map[domain.JobID]domain.Job, len(s.jobs))
	for id, job := range s.jobs {
		jobs[id] = cloneJob(job)
	}
	pending := make(map[domain.JobID]domain.Job, len(s.pending))
	for id, job := range s.pending {
		pending[id] = cloneJob(job)
	}
	return nodes, jobs, pending
}

func cloneNode(node domain.Node) domain.Node {
	if node.Labels != nil {
		labels := make(map[string]string, len(node.Labels))
		for key, value := range node.Labels {
			labels[key] = value
		}
		node.Labels = labels
	}
	return node
}

func cloneJob(job domain.Job) domain.Job {
	job.Deadline = cloneDuration(job.Deadline)
	job.StartTime = cloneDuration(job.StartTime)
	job.EndTime = cloneDuration(job.EndTime)
	return job
}

func cloneDuration(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
