package policies

import (
	"fmt"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

// normalizedUsage is the arithmetic mean of allocation/capacity for the
// resource dimensions requested by the job. Ignoring unrequested dimensions
// avoids sending CPU-only work to scarce GPU nodes merely because their unused
// GPU capacity lowers an all-dimension average.
func normalizedUsage(node domain.Node, request domain.ResourceVector, includeRequest bool) (float64, error) {
	allocated := node.Allocated
	if includeRequest {
		var err error
		allocated, err = allocated.Add(request)
		if err != nil {
			return 0, fmt.Errorf("calculate post-placement allocation: %w", err)
		}
	}
	var total float64
	var dimensions int
	add := func(requested, used, capacity int64) {
		if requested == 0 {
			return
		}
		total += float64(used) / float64(capacity)
		dimensions++
	}
	add(request.CPUMillis, allocated.CPUMillis, node.Capacity.CPUMillis)
	add(request.MemoryMiB, allocated.MemoryMiB, node.Capacity.MemoryMiB)
	add(request.GPUs, allocated.GPUs, node.Capacity.GPUs)
	add(request.EphemeralDiskMiB, allocated.EphemeralDiskMiB, node.Capacity.EphemeralDiskMiB)
	if dimensions == 0 {
		return 0, fmt.Errorf("score requires a non-zero request")
	}
	return total / float64(dimensions), nil
}
