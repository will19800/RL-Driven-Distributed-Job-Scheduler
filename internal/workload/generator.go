package workload

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

// MaxJobs bounds memory and CPU consumption from one generated configuration.
const MaxJobs = 100_000

var (
	// ErrInvalidSpec indicates invalid distribution or resource parameters.
	ErrInvalidSpec = errors.New("invalid workload specification")
	// ErrGenerationOverflow indicates generated simulation time exceeded int64.
	ErrGenerationOverflow = errors.New("workload generation overflow")
)

// DurationSpec defines a bounded log-normal duration distribution. Median,
// minimum, and maximum use simulation durations; Sigma is log-space spread.
type DurationSpec struct {
	Median time.Duration
	Sigma  float64
	Min    time.Duration
	Max    time.Duration
}

// ResourceProfile is one weighted job shape.
type ResourceProfile struct {
	Name      string
	Weight    float64
	Resources domain.ResourceVector
	Priority  int32
}

// Spec defines one deterministic synthetic workload.
type Spec struct {
	Count                int
	StartTime            time.Duration
	ArrivalRatePerSecond float64
	Duration             DurationSpec
	Profiles             []ResourceProfile
}

// Validate checks distribution bounds, finite weights, and domain resources.
func (s Spec) Validate() error {
	if s.Count <= 0 || s.Count > MaxJobs {
		return fmt.Errorf("%w: count must be between 1 and %d", ErrInvalidSpec, MaxJobs)
	}
	if s.StartTime < 0 {
		return fmt.Errorf("%w: start time must be non-negative", ErrInvalidSpec)
	}
	if !positiveFinite(s.ArrivalRatePerSecond) {
		return fmt.Errorf("%w: arrival rate must be positive and finite", ErrInvalidSpec)
	}
	if s.Duration.Min <= 0 || s.Duration.Median <= 0 || s.Duration.Max < s.Duration.Min {
		return fmt.Errorf("%w: duration bounds and median must be positive and ordered", ErrInvalidSpec)
	}
	if s.Duration.Median < s.Duration.Min || s.Duration.Median > s.Duration.Max {
		return fmt.Errorf("%w: duration median must be within min and max", ErrInvalidSpec)
	}
	if s.Duration.Sigma < 0 || math.IsNaN(s.Duration.Sigma) || math.IsInf(s.Duration.Sigma, 0) {
		return fmt.Errorf("%w: duration sigma must be non-negative and finite", ErrInvalidSpec)
	}
	if len(s.Profiles) == 0 {
		return fmt.Errorf("%w: at least one resource profile is required", ErrInvalidSpec)
	}
	totalWeight := 0.0
	for index, profile := range s.Profiles {
		if profile.Name == "" {
			return fmt.Errorf("%w: profile %d name is required", ErrInvalidSpec, index)
		}
		if !positiveFinite(profile.Weight) {
			return fmt.Errorf("%w: profile %q weight must be positive and finite", ErrInvalidSpec, profile.Name)
		}
		if err := profile.Resources.Validate(); err != nil || profile.Resources.IsZero() {
			return fmt.Errorf("%w: profile %q resources must be valid and non-zero", ErrInvalidSpec, profile.Name)
		}
		if profile.Priority < 0 {
			return fmt.Errorf("%w: profile %q priority must be non-negative", ErrInvalidSpec, profile.Name)
		}
		totalWeight += profile.Weight
		if math.IsInf(totalWeight, 0) {
			return fmt.Errorf("%w: total profile weight must be finite", ErrInvalidSpec)
		}
	}
	return nil
}

// Generate returns jobs in nondecreasing submission order. Separate PCG
// streams are deterministically derived from seed for arrivals, durations, and
// resource profiles.
func Generate(seed int64, spec Spec) ([]domain.Job, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	arrivalRNG := newStream(seed, 1)
	durationRNG := newStream(seed, 2)
	resourceRNG := newStream(seed, 3)
	totalWeight := profileWeight(spec.Profiles)

	jobs := make([]domain.Job, 0, spec.Count)
	submitTime := spec.StartTime
	for index := 0; index < spec.Count; index++ {
		interarrival := arrivalRNG.ExpFloat64() / spec.ArrivalRatePerSecond
		delta, err := secondsToDuration(interarrival)
		if err != nil || delta > time.Duration(math.MaxInt64)-submitTime {
			return nil, fmt.Errorf("%w: submission %d", ErrGenerationOverflow, index)
		}
		submitTime += delta

		profile := chooseProfile(resourceRNG.Float64()*totalWeight, spec.Profiles)
		duration := sampleDuration(durationRNG, spec.Duration)
		job := domain.Job{
			ID:         domain.JobID(fmt.Sprintf("job-%06d", index+1)),
			SubmitTime: submitTime,
			Request:    profile.Resources,
			Duration:   duration,
			Priority:   profile.Priority,
			State:      domain.JobStateQueued,
		}
		if err := job.Validate(); err != nil {
			return nil, fmt.Errorf("generated job %d: %w", index, err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func newStream(seed int64, stream uint64) *rand.Rand {
	first := splitMix64(uint64(seed) ^ stream*0x9e3779b97f4a7c15)
	second := splitMix64(first ^ 0xd1b54a32d192ed03)
	return rand.New(rand.NewPCG(first, second))
}

func splitMix64(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func sampleDuration(rng *rand.Rand, spec DurationSpec) time.Duration {
	sample := float64(spec.Median) * math.Exp(spec.Sigma*rng.NormFloat64())
	if math.IsNaN(sample) || sample <= float64(spec.Min) {
		return spec.Min
	}
	if math.IsInf(sample, 1) || sample >= float64(spec.Max) {
		return spec.Max
	}
	return time.Duration(math.Round(sample))
}

func chooseProfile(sample float64, profiles []ResourceProfile) ResourceProfile {
	cumulative := 0.0
	for _, profile := range profiles {
		cumulative += profile.Weight
		if sample < cumulative {
			return profile
		}
	}
	return profiles[len(profiles)-1]
}

func profileWeight(profiles []ResourceProfile) float64 {
	total := 0.0
	for _, profile := range profiles {
		total += profile.Weight
	}
	return total
}

func secondsToDuration(seconds float64) (time.Duration, error) {
	nanoseconds := seconds * float64(time.Second)
	if math.IsNaN(nanoseconds) || math.IsInf(nanoseconds, 0) || nanoseconds >= float64(math.MaxInt64) {
		return 0, ErrGenerationOverflow
	}
	return time.Duration(math.Round(nanoseconds)), nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
