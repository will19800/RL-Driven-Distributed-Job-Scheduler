// Package simulator implements a deterministic discrete-event simulation. Its
// clock and event queue are intentionally single-threaded: callers may prepare
// workloads concurrently, but all state transitions occur in queue order.
package simulator
