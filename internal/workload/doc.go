// Package workload creates reproducible synthetic job workloads. Independent
// random streams isolate arrival, duration, and resource-profile sampling so a
// change to one distribution does not silently perturb the others.
package workload
