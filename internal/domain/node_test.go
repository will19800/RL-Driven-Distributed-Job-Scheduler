package domain

import (
	"errors"
	"testing"
)

func validNode() Node {
	return Node{ID: "node-1", Capacity: ResourceVector{4_000, 8_192, 1, 0}, Allocated: ResourceVector{1_000, 2_048, 0, 0}, State: NodeStateAvailable}
}

func TestNodeFreeAndCanFit(t *testing.T) {
	t.Parallel()
	node := validNode()
	want := ResourceVector{3_000, 6_144, 1, 0}
	free, err := node.Free()
	if err != nil || free != want {
		t.Fatalf("Free() = %+v, %v; want %+v, nil", free, err, want)
	}
	if !node.CanFit(ResourceVector{CPUMillis: 3_000, GPUs: 1}) {
		t.Fatal("CanFit() = false for feasible request")
	}
	if node.CanFit(ResourceVector{CPUMillis: 3_001}) {
		t.Fatal("CanFit() = true for infeasible request")
	}
}

func TestUnavailableNodeCannotFit(t *testing.T) {
	t.Parallel()
	node := validNode()
	node.State = NodeStateUnavailable
	if node.CanFit(ResourceVector{CPUMillis: 1}) {
		t.Fatal("CanFit() = true for unavailable node")
	}
}

func TestNodeValidateRejectsInvalidNodes(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Node){
		"missing id": func(n *Node) { n.ID = "" }, "unknown state": func(n *Node) { n.State = "draining" },
		"zero capacity": func(n *Node) { n.Capacity = ResourceVector{} }, "negative allocation": func(n *Node) { n.Allocated.CPUMillis = -1 },
		"over capacity": func(n *Node) { n.Allocated.GPUs = 2 }, "empty label": func(n *Node) { n.Labels = map[string]string{"": "bad"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			node := validNode()
			mutate(&node)
			if err := node.Validate(); !errors.Is(err, ErrInvalidNode) {
				t.Fatalf("Validate() error = %v, want ErrInvalidNode", err)
			}
		})
	}
}
