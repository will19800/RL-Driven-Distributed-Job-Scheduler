package metrics

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

// ErrInvalidObservation indicates inconsistent metrics input.
var ErrInvalidObservation = errors.New("invalid metrics observation")

// DurationDistribution is a bounded-memory histogram summary. Quantiles are
// upper bounds of fixed buckets; min, max, and mean use exact observed values.
type DurationDistribution struct {
	Count uint64
	Min   time.Duration
	Max   time.Duration
	Mean  time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
}

// ResourceUtilization contains time-weighted cluster utilization in [0, 1].
type ResourceUtilization struct {
	CPU       float64 `json:"cpu"`
	Memory    float64 `json:"memory"`
	GPU       float64 `json:"gpu"`
	Ephemeral float64 `json:"ephemeral_disk"`
}

// ClusterMeasurement is an instantaneous aggregate cluster measurement.
type ClusterMeasurement struct {
	Utilization   ResourceUtilization
	Fragmentation float64
}

// DecisionMetrics aggregates scheduler overhead independently from workload metrics.
type DecisionMetrics struct {
	Count            uint64
	Placements       uint64
	Deferred         uint64
	PolicyErrors     uint64
	InvalidDecisions uint64
	TotalDuration    time.Duration
	MeanDuration     time.Duration
	MaxDuration      time.Duration
}

// Report is the aggregate measurement result for one simulation.
type Report struct {
	Makespan              time.Duration
	Completed             uint64
	Rejected              uint64
	DeadlineJobs          uint64
	DeadlineViolations    uint64
	DeadlineViolationRate float64
	SuccessRate           float64
	RejectionRate         float64
	ThroughputPerSecond   float64
	Latency               DurationDistribution
	QueueWait             DurationDistribution
	MeanBoundedSlowdown   float64
	SlowdownFairness      float64
	Utilization           ResourceUtilization
	MeanQueueDepth        float64
	ResourceFragmentation float64
	Decisions             DecisionMetrics
}

// Collector incrementally accumulates bounded-memory cluster and decision data.
type Collector struct {
	initialized bool
	lastAt      time.Duration
	lastUtil    ResourceUtilization
	lastQueue   int
	lastFrag    float64
	cpuArea     float64
	memoryArea  float64
	gpuArea     float64
	diskArea    float64
	queueArea   float64
	fragArea    float64
	decisions   DecisionMetrics
}

// NewCollector constructs an empty collector.
func NewCollector() *Collector { return &Collector{} }

// ObserveCluster records the state effective from at until the next sample.
func (c *Collector) ObserveCluster(at time.Duration, nodes []domain.Node, queued int) error {
	if at < 0 || queued < 0 {
		return fmt.Errorf("%w: negative time or queue depth", ErrInvalidObservation)
	}
	if c.initialized && at < c.lastAt {
		return fmt.Errorf("%w: time moved backward from %s to %s", ErrInvalidObservation, c.lastAt, at)
	}
	measurement, err := MeasureCluster(nodes)
	if err != nil {
		return err
	}
	if c.initialized {
		c.integrate(at - c.lastAt)
	}
	c.initialized = true
	c.lastAt, c.lastUtil, c.lastQueue, c.lastFrag = at, measurement.Utilization, queued, measurement.Fragmentation
	return nil
}

// RecordDecision adds one scheduler evaluation to the decision counters.
func (c *Collector) RecordDecision(telemetry scheduler.Telemetry) {
	c.decisions.Count++
	c.decisions.Placements += uint64(telemetry.PlacementCount)
	if telemetry.Deferred {
		c.decisions.Deferred++
	}
	c.decisions.PolicyErrors += telemetry.PolicyErrors
	c.decisions.InvalidDecisions += telemetry.InvalidDecisions
	c.decisions.TotalDuration += telemetry.Duration
	if telemetry.Duration > c.decisions.MaxDuration {
		c.decisions.MaxDuration = telemetry.Duration
	}
}

// Finalize derives job metrics and returns a report without retaining samples.
func (c *Collector) Finalize(makespan time.Duration, jobs []domain.Job) (Report, error) {
	if makespan < 0 || c.initialized && makespan < c.lastAt {
		return Report{}, fmt.Errorf("%w: invalid makespan %s", ErrInvalidObservation, makespan)
	}
	copy := *c
	if copy.initialized {
		copy.integrate(makespan - copy.lastAt)
	}
	report := Report{Makespan: makespan, Decisions: copy.decisions}
	if report.Decisions.Count > 0 {
		report.Decisions.MeanDuration = time.Duration(int64(report.Decisions.TotalDuration) / int64(report.Decisions.Count))
	}
	latency := newDurationHistogram()
	wait := newDurationHistogram()
	var slowdownSum, inverseSum, inverseSquares float64
	var slowdownCount uint64
	for _, job := range jobs {
		if err := job.Validate(); err != nil {
			return Report{}, fmt.Errorf("%w: job %q: %v", ErrInvalidObservation, job.ID, err)
		}
		switch job.State {
		case domain.JobStateCompleted:
			report.Completed++
			endToEnd := *job.EndTime - job.SubmitTime
			queueWait := *job.StartTime - job.SubmitTime
			latency.observe(endToEnd)
			wait.observe(queueWait)
			slowdown := math.Max(1, float64(endToEnd)/float64(job.Duration))
			slowdownSum += slowdown
			inverse := 1 / slowdown
			inverseSum += inverse
			inverseSquares += inverse * inverse
			slowdownCount++
			if job.Deadline != nil {
				report.DeadlineJobs++
				if *job.EndTime > *job.Deadline {
					report.DeadlineViolations++
				}
			}
		case domain.JobStateRejected:
			report.Rejected++
		}
	}
	report.Latency, report.QueueWait = latency.snapshot(), wait.snapshot()
	terminal := report.Completed + report.Rejected
	if terminal > 0 {
		report.SuccessRate = float64(report.Completed) / float64(terminal)
		report.RejectionRate = float64(report.Rejected) / float64(terminal)
	}
	if report.DeadlineJobs > 0 {
		report.DeadlineViolationRate = float64(report.DeadlineViolations) / float64(report.DeadlineJobs)
	}
	if makespan > 0 {
		seconds := makespan.Seconds()
		report.ThroughputPerSecond = float64(report.Completed) / seconds
		report.Utilization = ResourceUtilization{
			CPU: copy.cpuArea / seconds, Memory: copy.memoryArea / seconds,
			GPU: copy.gpuArea / seconds, Ephemeral: copy.diskArea / seconds,
		}
		report.MeanQueueDepth = copy.queueArea / seconds
		report.ResourceFragmentation = copy.fragArea / seconds
	}
	if slowdownCount > 0 {
		report.MeanBoundedSlowdown = slowdownSum / float64(slowdownCount)
		report.SlowdownFairness = inverseSum * inverseSum / (float64(slowdownCount) * inverseSquares)
	}
	return report, nil
}

func (c *Collector) integrate(delta time.Duration) {
	seconds := delta.Seconds()
	c.cpuArea += c.lastUtil.CPU * seconds
	c.memoryArea += c.lastUtil.Memory * seconds
	c.gpuArea += c.lastUtil.GPU * seconds
	c.diskArea += c.lastUtil.Ephemeral * seconds
	c.queueArea += float64(c.lastQueue) * seconds
	c.fragArea += c.lastFrag * seconds
}

// MeasureCluster validates nodes and calculates instantaneous utilization and fragmentation.
func MeasureCluster(nodes []domain.Node) (ClusterMeasurement, error) {
	var allocated, capacity domain.ResourceVector
	var fragmentation float64
	var available int
	for _, node := range nodes {
		if err := node.Validate(); err != nil {
			return ClusterMeasurement{}, fmt.Errorf("%w: node %q: %v", ErrInvalidObservation, node.ID, err)
		}
		var err error
		allocated, err = allocated.Add(node.Allocated)
		if err != nil {
			return ClusterMeasurement{}, fmt.Errorf("%w: allocation total: %v", ErrInvalidObservation, err)
		}
		capacity, err = capacity.Add(node.Capacity)
		if err != nil {
			return ClusterMeasurement{}, fmt.Errorf("%w: capacity total: %v", ErrInvalidObservation, err)
		}
		if node.State == domain.NodeStateAvailable {
			fragmentation += nodeFragmentation(node)
			available++
		}
	}
	utilization := ResourceUtilization{
		CPU:       ratio(allocated.CPUMillis, capacity.CPUMillis),
		Memory:    ratio(allocated.MemoryMiB, capacity.MemoryMiB),
		GPU:       ratio(allocated.GPUs, capacity.GPUs),
		Ephemeral: ratio(allocated.EphemeralDiskMiB, capacity.EphemeralDiskMiB),
	}
	if available > 0 {
		fragmentation /= float64(available)
	}
	return ClusterMeasurement{Utilization: utilization, Fragmentation: fragmentation}, nil
}

func nodeFragmentation(node domain.Node) float64 {
	values := make([]float64, 0, 4)
	add := func(allocated, capacity int64) {
		if capacity > 0 {
			values = append(values, 1-ratio(allocated, capacity))
		}
	}
	add(node.Allocated.CPUMillis, node.Capacity.CPUMillis)
	add(node.Allocated.MemoryMiB, node.Capacity.MemoryMiB)
	add(node.Allocated.GPUs, node.Capacity.GPUs)
	add(node.Allocated.EphemeralDiskMiB, node.Capacity.EphemeralDiskMiB)
	if len(values) < 2 {
		return 0
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum = math.Min(minimum, value)
		maximum = math.Max(maximum, value)
	}
	return maximum - minimum
}

func ratio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}
