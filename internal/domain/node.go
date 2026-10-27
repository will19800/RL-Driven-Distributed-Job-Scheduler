package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidNode indicates that a node violates a domain invariant.
var ErrInvalidNode = errors.New("invalid node")

// NodeID uniquely identifies a node within one simulation.
type NodeID string

// NodeState describes whether a node can accept work.
type NodeState string

const (
	// NodeStateAvailable allows new placements.
	NodeStateAvailable NodeState = "available"
	// NodeStateUnavailable prevents new placements.
	NodeStateUnavailable NodeState = "unavailable"
)

// Node describes a schedulable machine and its current allocation.
type Node struct {
	ID        NodeID            `json:"id" yaml:"id"`
	Type      string            `json:"type,omitempty" yaml:"type,omitempty"`
	Labels    map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Capacity  ResourceVector    `json:"capacity" yaml:"capacity"`
	Allocated ResourceVector    `json:"allocated" yaml:"allocated"`
	State     NodeState         `json:"state" yaml:"state"`
}

// Validate checks identity, state, labels, and resource accounting.
func (n Node) Validate() error {
	if n.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidNode)
	}
	if n.State != NodeStateAvailable && n.State != NodeStateUnavailable {
		return fmt.Errorf("%w: unknown state %q", ErrInvalidNode, n.State)
	}
	if err := n.Capacity.Validate(); err != nil || n.Capacity.IsZero() {
		return fmt.Errorf("%w: capacity must contain valid non-zero resources", ErrInvalidNode)
	}
	if err := n.Allocated.Validate(); err != nil {
		return fmt.Errorf("%w: allocated: %v", ErrInvalidNode, err)
	}
	if !n.Allocated.FitsWithin(n.Capacity) {
		return fmt.Errorf("%w: allocation exceeds capacity", ErrInvalidNode)
	}
	for key := range n.Labels {
		if key == "" {
			return fmt.Errorf("%w: label keys must not be empty", ErrInvalidNode)
		}
	}
	return nil
}

// Free returns the node's unallocated resources after validating it.
func (n Node) Free() (ResourceVector, error) {
	if err := n.Validate(); err != nil {
		return ResourceVector{}, err
	}
	return n.Capacity.Subtract(n.Allocated)
}

// CanFit reports whether an available, valid node can host request.
func (n Node) CanFit(request ResourceVector) bool {
	if n.State != NodeStateAvailable || request.IsZero() || request.Validate() != nil {
		return false
	}
	free, err := n.Free()
	return err == nil && request.FitsWithin(free)
}
