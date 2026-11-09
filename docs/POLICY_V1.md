# Policy contract v1

`policy/v1` is the shared observation and action contract between the Go
simulator/inference path and the Python training environment. Any change to
feature order, normalization semantics, candidate selection, tensor shape, or
action indexing requires a new version.

The canonical implementations are
[`internal/policyv1`](../internal/policyv1) and
[`policy_v1.py`](../python/scheduler_rl/policy_v1.py). Both are checked against
the same [parity fixture](../internal/policyv1/testdata/parity.json).

## Candidate construction

The configured `max_jobs` and `max_nodes` fix all tensor and action dimensions.

1. Nodes use ascending node-ID order from the immutable scheduler snapshot.
2. Only submitted jobs in `queued` state are eligible.
3. Jobs use descending priority, ascending submission time, then ascending ID.
4. Take the first `max_nodes` and `max_jobs` candidates.
5. Zero-pad unused rows and mark them false in `node_mask` or `job_mask`.

Candidate construction does not depend on map iteration, policy randomness, or
feasibility. This makes the same state byte-for-byte reproducible in both
languages. Feasibility is represented separately by the action mask.

## Feature tensors

All ordinary normalized values are clipped to `[0, 1]`. Resource requests,
free resources, and total resources divide by the declared per-dimension
normalization constants. A resource utilization is allocated divided by
capacity, or zero when the capacity is zero. Cluster utilization uses aggregate
allocation divided by aggregate capacity.

Node feature order (13 values):

```text
free_cpu, free_memory, free_gpu, free_disk,
total_cpu, total_memory, total_gpu, total_disk,
available,
utilization_cpu, utilization_memory, utilization_gpu, utilization_disk
```

Job feature order (9 values):

```text
request_cpu, request_memory, request_gpu, request_disk,
age, duration, priority, deadline_slack, has_deadline
```

Age is `max(now - submit_time, 0)`. Deadline slack is
`(deadline - now) / duration_seconds`, clipped to `[-1, 1]`; it is zero when no
deadline exists, while `has_deadline` distinguishes that case from zero slack.

Global feature order (7 values):

```text
queue_length,
utilization_cpu, utilization_memory, utilization_gpu, utilization_disk,
arrival_rate, simulation_time
```

Queue length includes every eligible queued job, including jobs truncated from
the candidate tensor. Cluster utilization includes every node, including nodes
truncated from the candidate tensor. Arrival rate is an external non-negative
jobs-per-second estimate supplied by the environment.

## Actions and masks

Placement actions are row-major:

```text
action = job_index * max_nodes + node_index
```

There are `max_jobs * max_nodes` placement actions. A placement is true in
`action_mask` only when both candidates are real and the available node has
enough free resources for the job. The final action at index
`max_jobs * max_nodes` is the explicit defer action and is always valid.

Model logits must be masked before sampling or argmax. Go decodes through
`DecodeAction`, which rejects out-of-range, padded, or infeasible selections;
later inference integration will additionally pass the resulting placement
through the scheduler's live-state decision validator.

## Normalization metadata

The schema records positive finite constants for CPU millicores, memory MiB,
GPU count, ephemeral-disk MiB, duration seconds, priority, queue length,
arrival rate, and simulation time seconds. It also records the exact feature
orders, candidate dimensions, placement count, and defer index. Models and
checkpoints must retain this schema and inference must reject mismatches.
