package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidPlacement indicates that a placement lacks an identity.
var ErrInvalidPlacement = errors.New("invalid placement")

// Placement maps one queued job to one node.
type Placement struct {
	JobID  JobID  `json:"job_id" yaml:"job_id"`
	NodeID NodeID `json:"node_id" yaml:"node_id"`
}

// Validate checks that both sides of a placement are identified.
func (p Placement) Validate() error {
	if p.JobID == "" {
		return fmt.Errorf("%w: job id is required", ErrInvalidPlacement)
	}
	if p.NodeID == "" {
		return fmt.Errorf("%w: node id is required", ErrInvalidPlacement)
	}
	return nil
}
