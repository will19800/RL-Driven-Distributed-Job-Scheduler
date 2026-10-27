package domain

import (
	"errors"
	"fmt"
	"math"
)

var (
	// ErrInvalidResource indicates that a resource dimension is negative.
	ErrInvalidResource = errors.New("resource values must be non-negative")
	// ErrResourceOverflow indicates that resource addition exceeded int64.
	ErrResourceOverflow = errors.New("resource arithmetic overflow")
	// ErrInsufficientResource indicates that a subtraction would underflow.
	ErrInsufficientResource = errors.New("insufficient resources")
)

// ResourceVector uses integer units to avoid floating-point drift. CPU is in
// millicores; memory and ephemeral storage are in MiB; GPUs are whole devices.
type ResourceVector struct {
	CPUMillis        int64 `json:"cpu_millis" yaml:"cpu_millis"`
	MemoryMiB        int64 `json:"memory_mib" yaml:"memory_mib"`
	GPUs             int64 `json:"gpus" yaml:"gpus"`
	EphemeralDiskMiB int64 `json:"ephemeral_disk_mib" yaml:"ephemeral_disk_mib"`
}

// Validate checks that every resource dimension is non-negative.
func (r ResourceVector) Validate() error {
	dimensions := []struct {
		name  string
		value int64
	}{
		{"cpu_millis", r.CPUMillis},
		{"memory_mib", r.MemoryMiB},
		{"gpus", r.GPUs},
		{"ephemeral_disk_mib", r.EphemeralDiskMiB},
	}
	for _, dimension := range dimensions {
		if dimension.value < 0 {
			return fmt.Errorf("%w: %s is %d", ErrInvalidResource, dimension.name, dimension.value)
		}
	}
	return nil
}

// IsZero reports whether all resource dimensions are zero.
func (r ResourceVector) IsZero() bool { return r == ResourceVector{} }

// FitsWithin reports whether r is valid and no larger than capacity in any
// dimension. An invalid capacity never admits a request.
func (r ResourceVector) FitsWithin(capacity ResourceVector) bool {
	if r.Validate() != nil || capacity.Validate() != nil {
		return false
	}
	return r.CPUMillis <= capacity.CPUMillis &&
		r.MemoryMiB <= capacity.MemoryMiB &&
		r.GPUs <= capacity.GPUs &&
		r.EphemeralDiskMiB <= capacity.EphemeralDiskMiB
}

// Add returns the component-wise sum, rejecting invalid vectors and overflow.
func (r ResourceVector) Add(other ResourceVector) (ResourceVector, error) {
	if err := r.Validate(); err != nil {
		return ResourceVector{}, err
	}
	if err := other.Validate(); err != nil {
		return ResourceVector{}, err
	}
	left := [...]int64{r.CPUMillis, r.MemoryMiB, r.GPUs, r.EphemeralDiskMiB}
	right := [...]int64{other.CPUMillis, other.MemoryMiB, other.GPUs, other.EphemeralDiskMiB}
	values := [4]int64{}
	for index := range left {
		if left[index] > math.MaxInt64-right[index] {
			return ResourceVector{}, ErrResourceOverflow
		}
		values[index] = left[index] + right[index]
	}
	return ResourceVector{
		CPUMillis:        values[0],
		MemoryMiB:        values[1],
		GPUs:             values[2],
		EphemeralDiskMiB: values[3],
	}, nil
}

// Subtract returns the component-wise difference, rejecting underflow.
func (r ResourceVector) Subtract(other ResourceVector) (ResourceVector, error) {
	if err := r.Validate(); err != nil {
		return ResourceVector{}, err
	}
	if err := other.Validate(); err != nil {
		return ResourceVector{}, err
	}
	if !other.FitsWithin(r) {
		return ResourceVector{}, ErrInsufficientResource
	}
	return ResourceVector{
		CPUMillis:        r.CPUMillis - other.CPUMillis,
		MemoryMiB:        r.MemoryMiB - other.MemoryMiB,
		GPUs:             r.GPUs - other.GPUs,
		EphemeralDiskMiB: r.EphemeralDiskMiB - other.EphemeralDiskMiB,
	}, nil
}
