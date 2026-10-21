# RL-Driven Distributed Job Scheduler: Design and Delivery Plan

Status: proposed design
Last updated: 2026-10-21
Repository: https://github.com/will19800/RL-Driven-Distributed-Job-Scheduler

## 1. Executive summary

This project will be a reproducible research simulator for comparing traditional and reinforcement-learning scheduling policies on heterogeneous compute clusters. The simulation control plane will be written in Go. Policy training and model export will be implemented in Python with PyTorch. A stable, versioned observation/action contract will connect the two systems.

The intended result is not merely an RL demo. It is an experiment platform that can answer, with repeatable evidence:

- When does an RL scheduler outperform round-robin and bin-packing?
- What does it cost in decision latency, fairness, and operational complexity?
- Does a trained policy generalize to unseen workloads and cluster sizes?
- Does it remain safe when inputs are invalid, nodes fail, or inference times out?

The repository description currently claims a 27% reduction in p99 latency and an 18% utilization improvement. Those numbers are not yet supported by code, experiment artifacts, or benchmark results in the repository. They must be treated as hypotheses until the benchmark pipeline reproduces them.

## 2. Current-state audit

As of 2026-10-21, the remote repository has one commit (`719a45e`, "Initial commit") and three files:

- `.gitignore`: standard Go-oriented exclusions plus `.env`.
- `LICENSE`: MIT license.
- `README.md`: project title and the performance claim.

There is currently no application code, Go module, Python package, model, simulator, configuration, test, CI workflow, benchmark output, architecture record, or user documentation. Therefore, the project is at the concept/scaffold stage. No scheduling behavior or reported performance is presently verifiable.

## 3. Goals and non-goals

### Goals

1. Simulate at least 200 heterogeneous nodes and configurable job-arrival patterns deterministically.
2. Implement correct reference schedulers: round-robin, random, least-loaded, and bin-packing.
3. Define an RL environment whose observations, actions, rewards, and invalid-action behavior are explicit and versioned.
4. Train a policy in PyTorch, export it, and run inference from the Go simulator.
5. Benchmark all policies on identical held-out workloads with multiple seeds and confidence intervals.
6. Measure scheduling quality, inference overhead, fairness, and failure behavior.
7. Produce a one-command local workflow and CI checks that prevent result drift.

### Non-goals for v1

- Operating a production Kubernetes scheduler.
- Live migration, preemption, gang scheduling, or topology-aware networking.
- Multi-cluster federation.
- Online learning in a production control loop.
- Claiming universal superiority of RL over heuristic schedulers.
- A web dashboard; machine-readable results and static plots are sufficient initially.

## 4. Success criteria

The first credible release is complete when:

- A seeded run is deterministic and produces a versioned JSON/CSV result bundle.
- At least 200 nodes and 100,000 jobs can be simulated without correctness failures.
- Every policy is evaluated on the same held-out traces and seeds.
- Reported comparisons include median, p95, p99, throughput, utilization, queue time, drop/failure rate, fairness, decision time, and confidence intervals.
- Invalid RL actions can never allocate beyond capacity.
- The system falls back to a configured heuristic on model load failure, invalid output, or inference timeout.
- `make test`, `make lint`, `make train-smoke`, and `make benchmark-smoke` work from a clean checkout.
- Any headline performance statement links to a committed experiment manifest and generated report.

The 27% p99 and 18% utilization figures are validation thresholds only if an experiment later reproduces them. Otherwise, the README must report the measured values without cherry-picking.

## 5. Users and primary workflows

### Researcher

Creates workload and cluster configurations, trains a policy, runs a benchmark matrix, and inspects aggregate metrics with statistical uncertainty.

### Scheduler engineer

Implements a policy against a small interface, checks invariants with unit/property tests, and compares it against reference policies.

### Reviewer or recruiter

Runs a smoke experiment, follows the architecture documentation, and verifies that published claims can be reproduced.

## 6. Proposed repository layout

```text
.
├── cmd/
│   ├── scheduler-sim/        # Simulator CLI
│   └── result-check/         # Result/schema validation CLI
├── internal/
│   ├── config/               # Strict config parsing and defaults
│   ├── domain/               # Job, node, resource, placement types
│   ├── simulator/            # Clock, event queue, lifecycle, accounting
│   ├── workload/             # Generators and trace readers
│   ├── scheduler/            # Policy interface and shared validation
│   ├── policies/             # Heuristic implementations
│   ├── inference/            # RL policy adapter and fallback logic
│   ├── metrics/              # Online aggregation and result schemas
│   └── experiment/           # Matrix expansion and seeded execution
├── api/
│   └── policy/v1/            # Versioned observation/action schemas
├── python/
│   ├── pyproject.toml
│   ├── scheduler_rl/         # Environment, PPO, export, evaluation
│   └── tests/
├── configs/
│   ├── clusters/
│   ├── workloads/
│   ├── training/
│   └── experiments/
├── testdata/                 # Small stable fixtures only
├── scripts/                  # Reproducible orchestration and plots
├── results/                  # Manifests/reports; large raw data ignored
├── docs/                     # Design, ADRs, experiment protocol
├── .github/workflows/
├── Makefile
├── go.mod
└── README.md
```

## 7. System architecture

### 7.1 Simulation kernel

Use a discrete-event simulation rather than wall-clock goroutines. This makes runs fast, reproducible, and testable.

The event queue is a min-heap ordered by `(simulation_time, event_sequence)`. The sequence number provides deterministic ordering for events at the same timestamp. Core event types:

- `JobSubmitted`
- `SchedulingTick`
- `JobStarted`
- `JobCompleted`
- `NodeFailed`
- `NodeRecovered`

The simulator owns all mutable cluster state. Policies receive immutable snapshots and return decisions; they must not mutate nodes or jobs. Each accepted placement produces future completion events and updates resource accounting atomically.

### 7.2 Domain model

`ResourceVector` contains integer-scaled CPU, memory, GPU, and optionally ephemeral storage. Integer units avoid floating-point capacity drift.

`Node` contains ID, capacity, current allocation, type/labels, availability state, and failure metadata.

`Job` contains ID, submit time, requested resources, expected/actual duration, priority, optional deadline, and lifecycle state.

`Placement` maps a queued job to one node. In v1, a job fits on exactly one node and is non-preemptive.

Required invariants:

- Allocated resources are never negative and never exceed node capacity.
- A job occupies at most one node.
- A completed or rejected job cannot be scheduled again.
- Simulation time never moves backward.
- The same seed and config produce the same ordered event/result stream.

### 7.3 Scheduler interface

Conceptual Go contract:

```go
type Policy interface {
    Name() string
    Decide(ctx context.Context, snapshot Snapshot) (Decision, error)
}
```

A `Decision` may place one or more queued jobs or deliberately defer. A shared validator checks every decision before state mutation. Policy decision duration, error counts, invalid actions, and fallbacks are recorded independently of workload metrics.

Reference policies:

- Round-robin: cycles through eligible nodes; establishes a simple fairness baseline.
- Random-fit: seeded random selection among eligible nodes; checks sensitivity to policy structure.
- Least-loaded: chooses the lowest normalized resource usage.
- Bin-pack: chooses the feasible node with the least residual normalized capacity after placement.

Tie-breaking must always be stable by node ID unless a seeded policy explicitly randomizes.

### 7.4 Workload generation

Workload specs should support:

- Poisson and bursty arrivals.
- Log-normal or trace-provided duration distributions.
- Configurable resource-shape mixtures: CPU-heavy, memory-heavy, GPU, balanced.
- Time-varying load phases.
- Optional deadlines and priorities reserved for later evaluation.
- CSV/JSON trace ingestion with strict validation.

Generation and scheduling use independent derived random seeds so changing a policy does not change the workload.

### 7.5 RL environment contract

The Python environment and Go inference adapter share `policy/v1` semantics.

Observation, bounded to a configured candidate set:

- Per-node normalized free and total resources, availability, and recent utilization.
- Per-job normalized request, age, expected duration, priority, and deadline slack.
- Global queue length, cluster utilization, arrival-rate estimate, and simulation time features.
- Masks for padded entries and infeasible actions.

Action:

- Select `(job_index, node_index)` from the candidate tensors.
- Include an explicit no-op/defer action.
- Mask all infeasible placements before sampling or argmax.

Candidate construction must be deterministic. A configurable top-K queue and node selection keeps the action space stable when the cluster contains 200+ nodes. The observation schema stores dimensions, normalization constants, feature order, and version.

Reward should be decomposed and logged rather than emitted as an opaque scalar:

```text
reward =
  + w_throughput   * completed_jobs
  - w_queue        * normalized_queue_time
  - w_slowdown     * bounded_slowdown
  - w_fragment     * resource_fragmentation
  - w_sla          * deadline_violations
  - w_invalid      * invalid_actions
```

Weights live in configuration. Reward terms must be normalized, clipped where justified, and covered by tests. Utilization alone is insufficient because a scheduler could improve it while starving jobs.

### 7.6 Training

Start with PPO because masked discrete actions, stable libraries, and parallel rollout support make it a practical baseline. The design must not couple the environment to PPO; alternate algorithms should remain possible.

Training flow:

1. Load and validate cluster/workload/training configs.
2. Create vectorized seeded environments.
3. Train on randomized workload families, not benchmark traces.
4. Periodically evaluate on a fixed validation suite.
5. Save checkpoints, optimizer state, feature schema, normalization values, config hash, Git SHA, dependency versions, and seed.
6. Select a model using a declared validation score, never test results.
7. Export the inference artifact (prefer ONNX if operator coverage is verified; otherwise TorchScript) and a metadata sidecar.

### 7.7 Go inference and safety boundary

The inference adapter loads the model and metadata once, validates schema compatibility, builds feature tensors, applies masks, and returns a normal `Decision`.

Safety behavior:

- Reject incompatible schema/model versions at startup.
- Revalidate the selected action against current cluster state.
- Apply a per-decision timeout.
- On timeout, inference error, NaN/Inf, invalid shape, or infeasible action, record the reason and invoke the configured fallback policy.
- Never silently coerce a bad action into a placement.

For the first vertical slice, a subprocess or local service boundary is acceptable because it is easy to debug. Before performance claims, use in-process ONNX inference or rigorously include IPC overhead in policy decision latency.

### 7.8 Metrics and output

Every run emits:

- `manifest.json`: Git SHA, dirty flag, configs and hashes, policy/model identity, seeds, platform, dependency versions, start/end timestamps.
- `summary.json`: aggregate metrics and counters.
- `jobs.csv` or Parquet: per-job lifecycle and latency fields.
- `timeseries.csv` or Parquet: interval utilization and queue depth.
- Structured logs with run ID, simulated time, and event type.

Core metrics:

- End-to-end latency: median, p95, p99.
- Queue waiting time and bounded slowdown.
- Throughput and makespan.
- CPU, memory, and GPU utilization over time.
- Resource fragmentation.
- Job success, rejection, and deadline violation rates.
- Fairness across job classes/tenants (Jain's fairness index where applicable).
- Scheduler decision latency and fallback/error rates.

Avoid unbounded in-memory samples for large runs. Use streaming counters/histograms and optional per-job output.

## 8. Configuration and interfaces

Use YAML for human-authored configs with strict decoding: unknown fields are errors. Each config has a `schema_version`.

Example command surface:

```bash
scheduler-sim run --config configs/experiments/smoke.yaml --policy binpack
scheduler-sim run --config configs/experiments/smoke.yaml --policy rl --model artifacts/policy.onnx
python -m scheduler_rl.train --config configs/training/ppo.yaml
python -m scheduler_rl.export --checkpoint artifacts/checkpoint.pt
make benchmark
```

Config precedence is explicit: file, then supported CLI overrides. Environment variables are reserved for operational paths, not experiment parameters, to keep manifests complete.

## 9. Benchmark and validation protocol

### Workload suites

- `smoke`: tiny, fast, deterministic CI workload.
- `steady`: stationary moderate load.
- `bursty`: alternating low/high arrival rates.
- `heterogeneous`: mixed resource shapes on heterogeneous nodes.
- `saturation`: load above capacity to reveal queueing behavior.
- `failure`: seeded node failure/recovery events.
- `generalization`: cluster sizes and distributions unseen during training.

### Experimental rules

1. Freeze training, validation, and test workload families separately.
2. Evaluate each policy using paired workload seeds (minimum 20 for publishable claims; increase after power analysis).
3. Warm up consistently and declare whether warm-up jobs are excluded.
4. Report distributions and 95% bootstrap confidence intervals, not only averages.
5. Compare paired per-seed differences for the main claims.
6. Include scheduler decision time and fallback rate.
7. Run RL ablations: reward terms, masking, candidate size, and fallback behavior.
8. Generate tables/plots from raw results; do not manually transcribe figures.

### Claim gate

A headline claim can enter the README only when a committed report identifies:

- exact baseline and RL policy versions;
- test configurations and all seeds;
- hardware/software environment;
- sample counts and confidence intervals;
- raw or regenerable result locations;
- failures and exclusions;
- the script that generated the table/figure.

## 10. Testing strategy

### Go

- Unit tests for resource arithmetic, event ordering, job transitions, policy tie-breaks, and metrics.
- Table-driven tests for each baseline.
- Property/fuzz tests for invariants and malformed configs/traces.
- Golden tests for deterministic small simulations and result schemas.
- Integration tests for CLI, config-to-result flow, inference failure, and fallback.
- Race detector in CI for code that uses concurrency outside the deterministic kernel.

### Python

- Unit tests for observation encoding, masks, reward components, seeding, and export metadata.
- Environment API compliance tests.
- Short training smoke test that checks learning code executes, not that it achieves a performance target.
- Export/parity test comparing Python inference with the Go adapter on fixed observations.

### Performance

- Go benchmarks for event queue, snapshot construction, policies, and metrics.
- Scale test at 200/500/1,000 nodes.
- Memory and decision-latency budgets recorded in benchmark reports.

## 11. Reliability, security, and operational concerns

- Treat workload/config/model files as untrusted inputs; enforce size limits and strict schemas.
- Do not deserialize arbitrary Python objects from untrusted checkpoints. Export a constrained inference format.
- Pin Go and Python dependency versions and scan them in CI.
- Never store secrets or large model/checkpoint files in Git.
- Bound queue, candidate, output, and log sizes.
- Propagate cancellation through the CLI, experiment runner, and inference path.
- Save partial run metadata on cancellation or failure so failed experiments remain diagnosable.

## 12. Key design decisions and tradeoffs

1. **Discrete-event over real-time simulation:** stronger determinism and faster experiments; less fidelity for network/control-plane timing.
2. **Go core plus Python training:** efficient simulator and mature ML tooling; requires a carefully versioned boundary.
3. **Masked bounded action space:** tractable and safe; may omit globally optimal nodes/jobs outside the candidate set.
4. **Offline training for v1:** reproducible and operationally safe; cannot adapt online.
5. **Heuristic fallback:** guarantees availability and feasibility; fallback rates must be reported so they cannot inflate RL results invisibly.
6. **Configuration-first experiments:** reproducible matrices; adds schema/versioning work early.

## 13. Risks and mitigations

| Risk | Consequence | Mitigation |
|---|---|---|
| Reward hacking | Good reward, bad user-facing latency/fairness | Decomposed rewards, held-out metrics, ablations |
| Train/test leakage | Inflated results | Separate generators/seeds and hashed manifests |
| Baseline bugs | Misleading RL advantage | Reference tests, invariants, peer-readable implementations |
| Nondeterminism | Irreproducible claims | Discrete events, stable tie-breaks, explicit seeds |
| Simulator/model skew | Invalid inference | Versioned schema and parity tests |
| Action-space explosion | Slow/unstable learning | Deterministic top-K candidates and masks |
| Inference overhead | Quality win erased in practice | Decision-time metric, in-process runtime, fallback |
| Starvation | High utilization but poor service | Age features, slowdown/fairness metrics, saturation tests |
| Overfitting | Weak generalization | Domain randomization and unseen test workloads |
| Unsubstantiated README metrics | Loss of credibility | Claim gate and generated reports |

## 14. Milestones

### M0 — Truthful project foundation

Deliver repository structure, architecture docs, development commands, pinned toolchains, and CI. Rewrite the README so current capabilities and target claims are distinct.

Exit criteria: clean checkout passes formatting, linting, and placeholder test commands; documentation states that no benchmark has yet validated the headline numbers.

### M1 — Deterministic simulator vertical slice

Deliver domain types, event queue, job lifecycle, resource accounting, strict config, a basic workload, and JSON summary output.

Exit criteria: a golden seeded scenario produces byte-stable normalized results and all invariants pass.

### M2 — Baseline scheduler suite

Deliver the policy interface, shared decision validation, round-robin, random-fit, least-loaded, and bin-pack policies.

Exit criteria: each baseline passes feasibility and deterministic tie-break tests and can run the same workload through the CLI.

### M3 — Experiment and measurement platform

Deliver workload families, trace input, metric aggregation, manifests, result schemas, multi-seed experiment matrices, and plots/statistics.

Exit criteria: one command compares all baselines across seeds and creates a reproducible report with confidence intervals.

### M4 — RL environment and training

Deliver the versioned policy schema, Python environment, masking, decomposed reward, PPO training, evaluation, checkpoints, and export.

Exit criteria: smoke training completes deterministically enough for diagnostics; exported model and metadata pass schema and fixed-vector inference tests.

### M5 — Safe Go inference integration

Deliver Go observation encoding, model execution, output validation, timeout handling, fallback, and cross-language parity tests.

Exit criteria: the RL policy runs end to end; injected model failures cannot violate placement invariants and are visible in metrics.

### M6 — Credible scale benchmark

Deliver 200+ node suites, held-out multi-seed evaluation, generalization/failure tests, ablations, and an automatically generated report.

Exit criteria: results are reproducible from manifests, uncertainty is reported, and the README uses measured—not aspirational—numbers.

### M7 — Release hardening

Deliver profiling, scale fixes, dependency/security automation, examples, release documentation, and v0.1.0 artifacts.

Exit criteria: a new user can reproduce the smoke benchmark from the README and the release records known limitations.

## 15. Iterative 28-commit plan

Each commit should be independently reviewable and leave the branch buildable. Tests belong in the same commit as behavior. Commit messages below are proposed conventional-commit subjects.

### M0: foundation

1. `docs: record current-state audit and target architecture`
   Add this design, ADR index, terminology, and explicit status of the unverified claims.

2. `chore: initialize Go and Python project skeletons`
   Add `go.mod`, `pyproject.toml`, package directories, toolchain versions, and minimal entry points.

3. `build: add repeatable developer commands`
   Add Make targets for format, lint, test, smoke training, smoke benchmark, and cleanup without hiding failures.

4. `ci: validate Go Python and documentation`
   Add GitHub Actions with dependency caching, Go tests/race checks, Python tests, linting, and config/schema validation.

### M1: simulator

5. `feat(domain): define resources nodes jobs and placements`
   Implement integer resource arithmetic, lifecycle enums, validation, and exhaustive unit tests.

6. `feat(sim): add deterministic clock and event queue`
   Implement stable event ordering, cancellation context, and queue property tests.

7. `feat(sim): implement job lifecycle and resource accounting`
   Process submit/start/complete events atomically and assert core invariants.

8. `feat(config): load versioned strict simulation configs`
   Add YAML schemas/defaults, reject unknown/invalid fields, and create a smoke config.

9. `feat(workload): generate seeded synthetic jobs`
   Add independent RNG streams, Poisson arrivals, resource mixtures, durations, and determinism tests.

10. `feat(cli): run a simulation and emit a result summary`
    Wire config, simulator, a temporary first-fit path, structured errors, and golden CLI integration test.

### M2: policies

11. `feat(scheduler): introduce policy contract and decision validator`
    Add immutable snapshots, feasible action checks, decision telemetry, and negative-path tests.

12. `feat(policies): add round-robin and seeded random-fit`
    Establish simple baselines with stable behavior and shared fixtures.

13. `feat(policies): add least-loaded and bin-pack schedulers`
    Define normalized scores/tie-breaks and test heterogeneous multidimensional resources.

14. `test(sim): add fuzz and golden invariant coverage`
    Fuzz event/config/decision boundaries and save deterministic reference scenarios.

### M3: experiments and metrics

15. `feat(metrics): collect latency throughput utilization and fairness`
    Add streaming aggregation, histogram policy, fragmentation, decision time, and correctness tests.

16. `feat(results): write versioned manifests and detailed outputs`
    Record provenance/config hashes and validate summary/per-job/time-series schemas.

17. `feat(workload): add bursty heterogeneous and trace workloads`
    Add strict CSV/JSON readers plus steady, saturation, and failure-ready generators.

18. `feat(experiment): run paired multi-policy multi-seed matrices`
    Expand experiment configs, isolate outputs, support bounded parallel workers, and preserve determinism.

19. `feat(analysis): generate statistical comparison reports`
    Add paired bootstrap confidence intervals, plots/tables, and a small golden report fixture.

### M4: RL training

20. `feat(rl): define policy v1 observations actions and masks`
    Add canonical feature order, normalization, candidate selection, schema fixtures, and validation tests in both languages.

21. `feat(rl): implement the seeded training environment`
    Reuse simulator semantics, expose Gymnasium-compatible steps, and test reset/step/termination determinism.

22. `feat(rl): add decomposed reward and diagnostics`
    Implement normalized reward terms, log each component, and test anti-starvation/invalid-action behavior.

23. `feat(rl): train and evaluate a PPO baseline`
    Add vectorized rollouts, checkpoint/resume, validation selection, smoke configuration, and reproducibility metadata.

24. `feat(rl): export versioned inference artifacts`
    Export ONNX or TorchScript plus metadata and fixed observation/output vectors; block unsafe/incompatible formats.

### M5: inference

25. `feat(inference): execute exported policies from Go`
    Implement feature encoding, runtime loading, action decoding, and fixed-vector parity with Python.

26. `feat(inference): enforce timeouts validation and fallback`
    Add model-error taxonomy, fallback policy, counters, integration/fault-injection tests, and invariant checks.

### M6–M7: evidence and release

27. `perf: benchmark held-out workloads at 200-plus nodes`
    Run paired seeds across baselines/RL, add generalization and failure suites, profile bottlenecks, and commit manifests plus generated report (not bulky raw artifacts).

28. `docs: publish reproducible results and v0.1 release guide`
    Update README from measured evidence, document one-command reproduction and limitations, add examples/changelog, and prepare the release checklist.

## 16. How to work through the plan

Implement one numbered commit at a time. Before each commit:

1. Reconfirm its acceptance scope and dependencies.
2. Add behavior and its tests together.
3. Run the narrow tests, then the milestone-level suite.
4. Inspect the diff for unrelated or generated files.
5. Record any changed design decision in an ADR.
6. Commit only after the branch is in a reviewable state.

At each milestone boundary, run the smoke experiment from a clean checkout and update a short status table in the README. Do not combine result-changing code with benchmark-result publication in the same commit; this keeps evidence reviewable.

## 17. Open decisions to resolve during implementation

These do not block M0–M2, but must be decided before the named milestone:

- **Before M3:** CSV versus Parquet for detailed large-run outputs; histogram precision and retention limits.
- **Before M4:** PPO library versus a small in-repo implementation; exact top-K candidate construction; reward weights and validation score.
- **Before M5:** ONNX Runtime binding versus a process/service adapter, based on operator compatibility and benchmarked overhead.
- **Before M6:** publishable seed count after variance/power analysis; hardware reference environment; whether public raw results live in GitHub Releases or object storage.

The default recommendations are Parquet for large detailed outputs with JSON summaries, a mature PPO implementation behind an in-repo environment abstraction, ONNX Runtime after parity validation, and at least 20 paired held-out seeds for initial claims.
