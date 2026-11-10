# Seeded training environment

The Python [`TrainingEnvironment`](../python/scheduler_rl/environment.py)
provides the event-driven environment used by reinforcement-learning code. It
uses `policy/v1` observations and actions and exposes Gymnasium-style method
signatures without introducing Gymnasium or a specific learning algorithm yet:

```python
observation, info = environment.reset(seed=42)
observation, reward, terminated, truncated, info = environment.step(action)
```

Rewards now expose throughput, integrated queue wait, completed-job slowdown,
integrated fragmentation, deadline misses, and invalid-action diagnostics. See
the [training guide](TRAINING.md) for weights, normalization, and PPO commands.

## Event and decision semantics

An episode begins with empty nodes and queued job definitions. Jobs are hidden
until their submission events execute.

1. Events are ordered by simulation time and then insertion sequence.
2. Every event at the next timestamp is applied before an observation is
   returned, matching the Go simulation runner's scheduling boundary.
3. A feasible placement allocates the selected node immediately and schedules
   a completion event at `now + duration`.
4. A completion releases all job resources before the next decision.
5. If no queued job currently fits, the environment automatically advances
   through events until a placement becomes feasible or the episode finishes.
6. The explicit defer action always advances to the next event; it never
   returns the same state in a busy loop.

An episode `terminated` when every submitted job completed and no pending event
or future job remains. It `truncated` with reason `unschedulable_queue` when
queued work remains but no event can change capacity. Calls to `step` before
`reset`, after completion, or with masked/out-of-range actions raise
`EnvironmentError`.

## Determinism and seeds

`reset(seed=...)` passes the integer seed to an episode factory. The included
`SyntheticEpisodeFactory` derives independent SHA-256-based streams for Poisson
arrivals and uniform durations, so changing one distribution does not consume
the other stream. Environment state is defensively copied at reset.

The synthetic factory intentionally has a narrow smoke-test configuration:
identical nodes, a fixed resource request, Poisson arrivals, and bounded uniform
durations. Broader workload randomization can supply another episode factory
without changing environment semantics.

The `info` mapping reports the episode seed, simulated time, action-step count,
completed job count, and pending-event count. Arrival-rate observations use a
deterministic rolling window configured in seconds.

## Current compatibility boundary

The reset and five-value step signatures, seed behavior, termination split, and
dictionary observations follow the Gymnasium API shape. The package does not
yet inherit from `gymnasium.Env` or declare Gymnasium `Space` objects because
the repository remains dependency-free at this milestone. The PPO integration
will provide the thin adapter when Gymnasium becomes a declared dependency.
