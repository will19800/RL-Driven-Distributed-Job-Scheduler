package policyv1

import (
	"fmt"
	"math"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/scheduler"
)

type Observation struct {
	Version        string      `json:"version"`
	NodeFeatures   [][]float64 `json:"node_features"`
	JobFeatures    [][]float64 `json:"job_features"`
	GlobalFeatures []float64   `json:"global_features"`
	NodeMask       []bool      `json:"node_mask"`
	JobMask        []bool      `json:"job_mask"`
	ActionMask     []bool      `json:"action_mask"`
	NodeIDs        []string    `json:"node_ids"`
	JobIDs         []string    `json:"job_ids"`
}

// Encode produces fixed-size tensors from the scheduler's canonical snapshot
// ordering. ArrivalRate is an externally estimated jobs-per-second value.
func Encode(config Config, snapshot scheduler.Snapshot, arrivalRate float64) (Observation, error) {
	if err := config.Validate(); err != nil {
		return Observation{}, err
	}
	if arrivalRate < 0 || math.IsNaN(arrivalRate) || math.IsInf(arrivalRate, 0) {
		return Observation{}, fmt.Errorf("%w: arrival rate must be non-negative and finite", ErrInvalidContract)
	}
	nodes := snapshot.Nodes()
	if len(nodes) > config.MaxNodes {
		nodes = nodes[:config.MaxNodes]
	}
	queued := make([]domain.Job, 0)
	for _, job := range snapshot.Jobs() {
		if job.State == domain.JobStateQueued && job.SubmitTime <= snapshot.Now() {
			queued = append(queued, job)
		}
	}
	jobs := queued
	if len(jobs) > config.MaxJobs {
		jobs = jobs[:config.MaxJobs]
	}
	observation := Observation{Version: Version, NodeFeatures: matrix(config.MaxNodes, len(nodeFeatureOrder)), JobFeatures: matrix(config.MaxJobs, len(jobFeatureOrder)), GlobalFeatures: make([]float64, len(globalFeatureOrder)), NodeMask: make([]bool, config.MaxNodes), JobMask: make([]bool, config.MaxJobs), ActionMask: make([]bool, config.MaxJobs*config.MaxNodes+1), NodeIDs: make([]string, config.MaxNodes), JobIDs: make([]string, config.MaxJobs)}
	for index, node := range nodes {
		features, err := nodeFeatures(config.Normalization, node)
		if err != nil {
			return Observation{}, err
		}
		observation.NodeFeatures[index] = features
		observation.NodeMask[index] = true
		observation.NodeIDs[index] = string(node.ID)
	}
	for index, job := range jobs {
		observation.JobFeatures[index] = jobFeatures(config.Normalization, snapshot.Now(), job)
		observation.JobMask[index] = true
		observation.JobIDs[index] = string(job.ID)
	}
	for jobIndex, job := range jobs {
		for nodeIndex, node := range nodes {
			observation.ActionMask[jobIndex*config.MaxNodes+nodeIndex] = node.CanFit(job.Request)
		}
	}
	observation.ActionMask[len(observation.ActionMask)-1] = true
	observation.GlobalFeatures = globalFeatures(config.Normalization, snapshot.Now(), queued, snapshot.Nodes(), arrivalRate)
	return observation, nil
}

func nodeFeatures(n Normalization, node domain.Node) ([]float64, error) {
	free, err := node.Free()
	if err != nil {
		return nil, fmt.Errorf("%w: node %q: %v", ErrInvalidContract, node.ID, err)
	}
	return []float64{ratio(float64(free.CPUMillis), n.CPUMillis), ratio(float64(free.MemoryMiB), n.MemoryMiB), ratio(float64(free.GPUs), n.GPUs), ratio(float64(free.EphemeralDiskMiB), n.EphemeralDiskMiB), ratio(float64(node.Capacity.CPUMillis), n.CPUMillis), ratio(float64(node.Capacity.MemoryMiB), n.MemoryMiB), ratio(float64(node.Capacity.GPUs), n.GPUs), ratio(float64(node.Capacity.EphemeralDiskMiB), n.EphemeralDiskMiB), boolValue(node.State == domain.NodeStateAvailable), resourceRatio(node.Allocated.CPUMillis, node.Capacity.CPUMillis), resourceRatio(node.Allocated.MemoryMiB, node.Capacity.MemoryMiB), resourceRatio(node.Allocated.GPUs, node.Capacity.GPUs), resourceRatio(node.Allocated.EphemeralDiskMiB, node.Capacity.EphemeralDiskMiB)}, nil
}

func jobFeatures(n Normalization, now time.Duration, job domain.Job) []float64 {
	age := now - job.SubmitTime
	if age < 0 {
		age = 0
	}
	slack, hasDeadline := 0.0, 0.0
	if job.Deadline != nil {
		slack = signedRatio((*job.Deadline - now).Seconds(), n.DurationSeconds)
		hasDeadline = 1
	}
	return []float64{ratio(float64(job.Request.CPUMillis), n.CPUMillis), ratio(float64(job.Request.MemoryMiB), n.MemoryMiB), ratio(float64(job.Request.GPUs), n.GPUs), ratio(float64(job.Request.EphemeralDiskMiB), n.EphemeralDiskMiB), ratio(age.Seconds(), n.DurationSeconds), ratio(job.Duration.Seconds(), n.DurationSeconds), ratio(float64(job.Priority), n.Priority), slack, hasDeadline}
}

func globalFeatures(n Normalization, now time.Duration, queued []domain.Job, nodes []domain.Node, arrivalRate float64) []float64 {
	var allocated, capacity [4]float64
	for _, node := range nodes {
		allocated[0] += float64(node.Allocated.CPUMillis)
		allocated[1] += float64(node.Allocated.MemoryMiB)
		allocated[2] += float64(node.Allocated.GPUs)
		allocated[3] += float64(node.Allocated.EphemeralDiskMiB)
		capacity[0] += float64(node.Capacity.CPUMillis)
		capacity[1] += float64(node.Capacity.MemoryMiB)
		capacity[2] += float64(node.Capacity.GPUs)
		capacity[3] += float64(node.Capacity.EphemeralDiskMiB)
	}
	return []float64{ratio(float64(len(queued)), n.QueueLength), aggregateRatio(allocated[0], capacity[0]), aggregateRatio(allocated[1], capacity[1]), aggregateRatio(allocated[2], capacity[2]), aggregateRatio(allocated[3], capacity[3]), ratio(arrivalRate, n.ArrivalRate), ratio(now.Seconds(), n.SimulationTimeSeconds)}
}

func matrix(rows, columns int) [][]float64 {
	output := make([][]float64, rows)
	for index := range output {
		output[index] = make([]float64, columns)
	}
	return output
}
func ratio(value, maximum float64) float64       { return math.Max(0, math.Min(1, value/maximum)) }
func signedRatio(value, maximum float64) float64 { return math.Max(-1, math.Min(1, value/maximum)) }
func resourceRatio(value, maximum int64) float64 {
	if maximum == 0 {
		return 0
	}
	return ratio(float64(value), float64(maximum))
}
func aggregateRatio(value, maximum float64) float64 {
	if maximum == 0 {
		return 0
	}
	return ratio(value, maximum)
}
func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
