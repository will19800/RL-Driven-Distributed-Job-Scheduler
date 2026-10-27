package domain

import (
	"errors"
	"math"
	"testing"
)

func TestResourceVectorValidateRejectsNegativeDimensions(t *testing.T) {
	t.Parallel()
	tests := map[string]ResourceVector{
		"cpu": {CPUMillis: -1}, "memory": {MemoryMiB: -1},
		"gpu": {GPUs: -1}, "storage": {EphemeralDiskMiB: -1},
	}
	for name, resource := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := resource.Validate(); !errors.Is(err, ErrInvalidResource) {
				t.Fatalf("Validate() error = %v, want ErrInvalidResource", err)
			}
		})
	}
}

func TestResourceVectorArithmetic(t *testing.T) {
	t.Parallel()
	left := ResourceVector{2_000, 4_096, 1, 200}
	right := ResourceVector{500, 1_024, 0, 50}
	wantSum := ResourceVector{2_500, 5_120, 1, 250}
	sum, err := left.Add(right)
	if err != nil || sum != wantSum {
		t.Fatalf("Add() = %+v, %v; want %+v, nil", sum, err, wantSum)
	}
	difference, err := sum.Subtract(right)
	if err != nil || difference != left {
		t.Fatalf("Subtract() = %+v, %v; want %+v, nil", difference, err, left)
	}
}

func TestResourceVectorArithmeticErrors(t *testing.T) {
	t.Parallel()
	if _, err := (ResourceVector{CPUMillis: math.MaxInt64}).Add(ResourceVector{CPUMillis: 1}); !errors.Is(err, ErrResourceOverflow) {
		t.Fatalf("Add() error = %v, want ErrResourceOverflow", err)
	}
	if _, err := (ResourceVector{CPUMillis: 1_000}).Subtract(ResourceVector{CPUMillis: 1_001}); !errors.Is(err, ErrInsufficientResource) {
		t.Fatalf("Subtract() error = %v, want ErrInsufficientResource", err)
	}
}

func TestResourceVectorFitsWithin(t *testing.T) {
	t.Parallel()
	capacity := ResourceVector{CPUMillis: 1_000, MemoryMiB: 2_048, GPUs: 1}
	tests := []struct {
		name    string
		request ResourceVector
		want    bool
	}{
		{"equal", capacity, true}, {"smaller", ResourceVector{CPUMillis: 500}, true},
		{"cpu exceeds", ResourceVector{CPUMillis: 1_001}, false}, {"negative", ResourceVector{CPUMillis: -1}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := test.request.FitsWithin(capacity); got != test.want {
				t.Fatalf("FitsWithin() = %t, want %t", got, test.want)
			}
		})
	}
}
