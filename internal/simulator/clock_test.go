package simulator

import (
	"errors"
	"testing"
	"time"
)

func TestClockAdvancesMonotonically(t *testing.T) {
	t.Parallel()
	clock, err := NewClock(2 * time.Second)
	if err != nil {
		t.Fatalf("NewClock() unexpected error: %v", err)
	}
	if err := clock.AdvanceTo(2 * time.Second); err != nil {
		t.Fatalf("AdvanceTo(same time) unexpected error: %v", err)
	}
	if err := clock.AdvanceTo(5 * time.Second); err != nil {
		t.Fatalf("AdvanceTo(later time) unexpected error: %v", err)
	}
	if got := clock.Now(); got != 5*time.Second {
		t.Fatalf("Now() = %s, want 5s", got)
	}
	if err := clock.AdvanceTo(4 * time.Second); !errors.Is(err, ErrTimeReversal) {
		t.Fatalf("AdvanceTo(earlier time) error = %v, want ErrTimeReversal", err)
	}
	if got := clock.Now(); got != 5*time.Second {
		t.Fatalf("failed advance changed time to %s", got)
	}
}

func TestClockRejectsNegativeTime(t *testing.T) {
	t.Parallel()
	if _, err := NewClock(-time.Nanosecond); !errors.Is(err, ErrInvalidSimulationTime) {
		t.Fatalf("NewClock() error = %v, want ErrInvalidSimulationTime", err)
	}
	clock, err := NewClock(0)
	if err != nil {
		t.Fatalf("NewClock() unexpected error: %v", err)
	}
	if err := clock.AdvanceTo(-time.Nanosecond); !errors.Is(err, ErrInvalidSimulationTime) {
		t.Fatalf("AdvanceTo() error = %v, want ErrInvalidSimulationTime", err)
	}
}
