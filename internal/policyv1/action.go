package policyv1

import (
	"fmt"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

type Action struct {
	Defer     bool
	JobIndex  int
	NodeIndex int
}

// DecodeAction maps the row-major discrete action to candidate indices and
// rejects padded or infeasible actions using the encoded mask.
func DecodeAction(config Config, observation Observation, index int) (Action, error) {
	if err := config.Validate(); err != nil {
		return Action{}, err
	}
	want := config.MaxJobs*config.MaxNodes + 1
	if observation.Version != Version || len(observation.ActionMask) != want {
		return Action{}, fmt.Errorf("%w: observation is incompatible", ErrInvalidContract)
	}
	if index < 0 || index >= want {
		return Action{}, fmt.Errorf("%w: action %d is out of range", ErrInvalidContract, index)
	}
	if !observation.ActionMask[index] {
		return Action{}, fmt.Errorf("%w: action %d is masked", ErrInvalidContract, index)
	}
	if index == want-1 {
		return Action{Defer: true, JobIndex: -1, NodeIndex: -1}, nil
	}
	jobIndex, nodeIndex := index/config.MaxNodes, index%config.MaxNodes
	if jobIndex >= len(observation.JobIDs) || nodeIndex >= len(observation.NodeIDs) || observation.JobIDs[jobIndex] == "" || observation.NodeIDs[nodeIndex] == "" {
		return Action{}, fmt.Errorf("%w: action %d selects padding", ErrInvalidContract, index)
	}
	return Action{JobIndex: jobIndex, NodeIndex: nodeIndex}, nil
}

func (a Action) Placement(observation Observation) (domain.Placement, bool) {
	if a.Defer || a.JobIndex < 0 || a.NodeIndex < 0 || a.JobIndex >= len(observation.JobIDs) || a.NodeIndex >= len(observation.NodeIDs) {
		return domain.Placement{}, false
	}
	return domain.Placement{JobID: domain.JobID(observation.JobIDs[a.JobIndex]), NodeID: domain.NodeID(observation.NodeIDs[a.NodeIndex])}, true
}
