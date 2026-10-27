package simulator

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidSimulationTime indicates a negative simulation timestamp.
	ErrInvalidSimulationTime = errors.New("simulation time must be non-negative")
	// ErrTimeReversal indicates an attempt to move the simulation clock backward.
	ErrTimeReversal = errors.New("simulation time cannot move backward")
)

// Clock tracks elapsed simulated time independently of wall-clock time.
type Clock struct {
	now time.Duration
}

// NewClock returns a clock at initialTime.
func NewClock(initialTime time.Duration) (*Clock, error) {
	if initialTime < 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidSimulationTime, initialTime)
	}
	return &Clock{now: initialTime}, nil
}

// Now returns the current simulated time.
func (c *Clock) Now() time.Duration {
	return c.now
}

// AdvanceTo moves the clock to target. Advancing to the current time is valid
// because multiple events may share a timestamp.
func (c *Clock) AdvanceTo(target time.Duration) error {
	if target < 0 {
		return fmt.Errorf("%w: %s", ErrInvalidSimulationTime, target)
	}
	if target < c.now {
		return fmt.Errorf("%w: current=%s target=%s", ErrTimeReversal, c.now, target)
	}
	c.now = target
	return nil
}
