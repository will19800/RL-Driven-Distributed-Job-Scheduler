package domain

import (
	"errors"
	"testing"
)

func TestPlacementValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		placement Placement
		wantError bool
	}{
		{"valid", Placement{JobID: "job-1", NodeID: "node-1"}, false},
		{"missing job", Placement{NodeID: "node-1"}, true},
		{"missing node", Placement{JobID: "job-1"}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.placement.Validate()
			if got := errors.Is(err, ErrInvalidPlacement); got != test.wantError {
				t.Fatalf("Validate() error = %v, invalid = %t, want %t", err, got, test.wantError)
			}
		})
	}
}
