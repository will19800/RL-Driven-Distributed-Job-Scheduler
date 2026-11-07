# Configuration schema v1

Simulation inputs use one strict YAML document. Unknown fields, duplicate IDs,
invalid resources, numeric duration values, multiple documents, and files over
1 MiB are rejected before a simulation is created.

The root fields are:

- `schema_version`: required and currently must be `v1`.
- `seed`: signed 64-bit seed reserved for deterministic workload streams.
- `cluster.nodes`: one or more initially empty and available nodes.
- `workload`: exactly one of `jobs`, `synthetic`, or `trace`.

Resources use integer units:

- `cpu_millis`: CPU millicores.
- `memory_mib`: memory in MiB.
- `gpus`: whole GPU devices.
- `ephemeral_disk_mib`: ephemeral storage in MiB.

Durations and simulation timestamps must be quoted or plain YAML strings
accepted by Go duration syntax, such as `250ms`, `5s`, or `2m30s`. Bare numeric
values are rejected because their unit would be ambiguous.

See [`configs/experiments/smoke.yaml`](../configs/experiments/smoke.yaml) for a
complete configuration that is validated in the test suite.

## Synthetic workloads

`workload.synthetic` generates deterministic jobs from the root `seed`. It
requires a job `count`, `start_time`, either a positive
`arrival_rate_per_second` or `arrival_phases`, a bounded log-normal `duration`,
and one or more weighted resource `profiles`.
The generator currently limits a configuration to 100,000 jobs.

Arrival, duration, and profile choices use independently derived random streams.
Changing a resource-profile mix therefore does not change arrival timestamps or
durations for the same seed. See
[`configs/experiments/synthetic-smoke.yaml`](../configs/experiments/synthetic-smoke.yaml)
for a complete example.

### Arrival phases and workload families

For a stationary workload, set `arrival_rate_per_second`. For time-varying
load, replace that field with `arrival_phases`. Each phase has a descriptive
`name`, positive `count`, and positive `rate_per_second`; phase counts must add
up exactly to the synthetic workload `count`. Jobs are produced consecutively
by phase, with an independent exponential interarrival sample at the phase's
rate. Arrival, duration, and profile random streams remain isolated, so changing
a profile mixture cannot perturb timestamps.

The checked-in suites demonstrate the intended families:

- [`bursty.yaml`](../configs/experiments/bursty.yaml): low/high/low phases.
- [`heterogeneous.yaml`](../configs/experiments/heterogeneous.yaml): CPU-heavy,
  memory-heavy, GPU, and balanced shapes on dissimilar node pools.
- [`saturation.yaml`](../configs/experiments/saturation.yaml): offered load well
  above cluster capacity, useful for exposing queueing behavior.

Synthetic output is ordinary domain jobs and does not embed policy or node
availability assumptions. The same workloads can therefore be reused when
seeded failure and recovery events are added.

## Trace workloads

`workload.trace.path` selects a CSV or JSON trace. Relative paths resolve from
the configuration file, not the process working directory. `format` may be
`csv` or `json`; when omitted, it is inferred from the filename extension.
Trace files are limited to 32 MiB and 100,000 jobs. IDs must be unique,
submission times must be nondecreasing, and every job must satisfy the same
resource, duration, priority, and deadline invariants as configured jobs.

CSV input uses this exact header and permits an empty final `deadline` value:

```text
id,submit_time,cpu_millis,memory_mib,gpus,ephemeral_disk_mib,duration,priority,deadline
```

JSON input is an array of objects with the same snake-case fields. Duration
values are strings such as `250ms`; unknown fields and trailing JSON values are
rejected. See [`trace-smoke.yaml`](../configs/experiments/trace-smoke.yaml) and
[`smoke.csv`](../configs/traces/smoke.csv) for a runnable trace example.
