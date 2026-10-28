# Configuration schema v1

Simulation inputs use one strict YAML document. Unknown fields, duplicate IDs,
invalid resources, numeric duration values, multiple documents, and files over
1 MiB are rejected before a simulation is created.

The root fields are:

- `schema_version`: required and currently must be `v1`.
- `seed`: signed 64-bit seed reserved for deterministic workload streams.
- `cluster.nodes`: one or more initially empty and available nodes.
- `workload.jobs`: one or more explicit queued jobs.

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
