package workload

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

func validSpec() Spec {
	return Spec{
		Count:                100,
		StartTime:            time.Second,
		ArrivalRatePerSecond: 4,
		Duration: DurationSpec{
			Median: 2 * time.Second,
			Sigma:  0.5,
			Min:    500 * time.Millisecond,
			Max:    10 * time.Second,
		},
		Profiles: []ResourceProfile{
			{Name: "cpu", Weight: 3, Resources: domain.ResourceVector{CPUMillis: 1_000, MemoryMiB: 512}},
			{Name: "memory", Weight: 1, Resources: domain.ResourceVector{CPUMillis: 250, MemoryMiB: 4_096}, Priority: 1},
		},
	}
}

func TestGenerateIsDeterministicForSeed(t *testing.T) {
	t.Parallel()
	first, err := Generate(42, validSpec())
	if err != nil {
		t.Fatalf("Generate(first) unexpected error: %v", err)
	}
	second, err := Generate(42, validSpec())
	if err != nil {
		t.Fatalf("Generate(second) unexpected error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed and spec produced different jobs")
	}
	different, err := Generate(43, validSpec())
	if err != nil {
		t.Fatalf("Generate(different seed) unexpected error: %v", err)
	}
	if reflect.DeepEqual(first, different) {
		t.Fatal("different seeds produced identical jobs")
	}
}

func TestGenerateUsesIndependentRandomStreams(t *testing.T) {
	t.Parallel()
	base, err := Generate(9, validSpec())
	if err != nil {
		t.Fatalf("Generate(base) unexpected error: %v", err)
	}
	changedSpec := validSpec()
	changedSpec.Profiles = []ResourceProfile{{
		Name: "gpu", Weight: 1, Resources: domain.ResourceVector{CPUMillis: 2_000, MemoryMiB: 8_192, GPUs: 1},
	}}
	changed, err := Generate(9, changedSpec)
	if err != nil {
		t.Fatalf("Generate(changed profiles) unexpected error: %v", err)
	}
	for index := range base {
		if base[index].SubmitTime != changed[index].SubmitTime || base[index].Duration != changed[index].Duration {
			t.Fatalf("profile change perturbed arrival or duration at job %d", index)
		}
	}
}

func TestGenerateProducesValidBoundedJobs(t *testing.T) {
	t.Parallel()
	spec := validSpec()
	jobs, err := Generate(123, spec)
	if err != nil {
		t.Fatalf("Generate() unexpected error: %v", err)
	}
	if len(jobs) != spec.Count {
		t.Fatalf("len(jobs) = %d, want %d", len(jobs), spec.Count)
	}
	previous := spec.StartTime
	for index, job := range jobs {
		if err := job.Validate(); err != nil {
			t.Fatalf("job %d invalid: %v", index, err)
		}
		if job.SubmitTime < previous {
			t.Fatalf("submission time regressed at job %d", index)
		}
		if job.Duration < spec.Duration.Min || job.Duration > spec.Duration.Max {
			t.Fatalf("job %d duration %s outside bounds", index, job.Duration)
		}
		previous = job.SubmitTime
	}
}

func TestPoissonArrivalMean(t *testing.T) {
	t.Parallel()
	spec := validSpec()
	spec.Count = 50_000
	spec.StartTime = 0
	spec.ArrivalRatePerSecond = 10
	jobs, err := Generate(77, spec)
	if err != nil {
		t.Fatalf("Generate() unexpected error: %v", err)
	}
	meanSeconds := jobs[len(jobs)-1].SubmitTime.Seconds() / float64(len(jobs))
	want := 1 / spec.ArrivalRatePerSecond
	if math.Abs(meanSeconds-want) > want*0.03 {
		t.Fatalf("mean interarrival = %f, want within 3%% of %f", meanSeconds, want)
	}
}

func TestGenerateArrivalPhases(t *testing.T) {
	t.Parallel()
	spec := validSpec()
	spec.Count = 40_000
	spec.ArrivalRatePerSecond = 0
	spec.ArrivalPhases = []ArrivalPhase{
		{Name: "quiet", Count: 20_000, RatePerSecond: 2},
		{Name: "burst", Count: 20_000, RatePerSecond: 20},
	}
	jobs, err := Generate(88, spec)
	if err != nil {
		t.Fatalf("Generate() unexpected error: %v", err)
	}
	quietMean := (jobs[19_999].SubmitTime - spec.StartTime).Seconds() / 20_000
	burstMean := (jobs[39_999].SubmitTime - jobs[19_999].SubmitTime).Seconds() / 20_000
	if math.Abs(quietMean-0.5) > 0.015 || math.Abs(burstMean-0.05) > 0.002 {
		t.Fatalf("phase means = %f, %f; want approximately 0.5, 0.05", quietMean, burstMean)
	}
}

func TestSpecValidateRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Spec){
		"zero count":     func(s *Spec) { s.Count = 0 },
		"too many jobs":  func(s *Spec) { s.Count = MaxJobs + 1 },
		"negative start": func(s *Spec) { s.StartTime = -1 },
		"zero arrival":   func(s *Spec) { s.ArrivalRatePerSecond = 0 },
		"nan arrival":    func(s *Spec) { s.ArrivalRatePerSecond = math.NaN() },
		"rate and phases": func(s *Spec) {
			s.ArrivalPhases = []ArrivalPhase{{Name: "phase", Count: s.Count, RatePerSecond: 1}}
		},
		"phase total mismatch": func(s *Spec) {
			s.ArrivalRatePerSecond = 0
			s.ArrivalPhases = []ArrivalPhase{{Name: "phase", Count: 1, RatePerSecond: 1}}
		},
		"invalid phase rate": func(s *Spec) {
			s.ArrivalRatePerSecond = 0
			s.ArrivalPhases = []ArrivalPhase{{Name: "phase", Count: s.Count, RatePerSecond: 0}}
		},
		"zero minimum":       func(s *Spec) { s.Duration.Min = 0 },
		"median below min":   func(s *Spec) { s.Duration.Median = time.Millisecond },
		"max below min":      func(s *Spec) { s.Duration.Max = time.Millisecond },
		"negative sigma":     func(s *Spec) { s.Duration.Sigma = -1 },
		"infinite sigma":     func(s *Spec) { s.Duration.Sigma = math.Inf(1) },
		"no profiles":        func(s *Spec) { s.Profiles = nil },
		"empty profile name": func(s *Spec) { s.Profiles[0].Name = "" },
		"zero weight":        func(s *Spec) { s.Profiles[0].Weight = 0 },
		"infinite weights": func(s *Spec) {
			s.Profiles[0].Weight, s.Profiles[1].Weight = math.MaxFloat64, math.MaxFloat64
		},
		"zero resources":    func(s *Spec) { s.Profiles[0].Resources = domain.ResourceVector{} },
		"negative priority": func(s *Spec) { s.Profiles[0].Priority = -1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			spec := validSpec()
			mutate(&spec)
			if err := spec.Validate(); !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("Validate() error = %v, want ErrInvalidSpec", err)
			}
		})
	}
}

func TestGenerateRejectsOverflow(t *testing.T) {
	t.Parallel()
	spec := validSpec()
	spec.Count = 1
	spec.StartTime = time.Duration(math.MaxInt64)
	if _, err := Generate(1, spec); !errors.Is(err, ErrGenerationOverflow) {
		t.Fatalf("Generate() error = %v, want ErrGenerationOverflow", err)
	}
}
