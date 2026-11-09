# RL-Driven Distributed Job Scheduler

A research-oriented distributed cluster scheduler simulator being built with a
Go simulation core and Python reinforcement-learning tooling.

The project currently has a deterministic simulator and four baseline
schedulers, and is moving into its experiment and measurement platform. The RL
training pipeline and performance benchmarks remain planned. Earlier references
to a 27% p99 latency reduction and an 18% utilization improvement are target
hypotheses; no repository evidence currently validates those figures.

See [the design and delivery plan](docs/PROJECT_DESIGN.md) for the architecture,
milestones, validation rules, and ordered commit roadmap. The
[configuration reference](docs/CONFIGURATION.md) documents the strict v1 YAML
schema, burst and heterogeneous suites, and CSV/JSON trace ingestion. The
[result bundle reference](docs/RESULTS.md) defines output schemas and integrity
checks. The [experiment matrix guide](docs/EXPERIMENTS.md) covers paired,
bounded-parallel policy comparisons across workloads and seeds. The
[`policy/v1` contract](docs/POLICY_V1.md) fixes cross-language RL features,
normalization, candidate ordering, masks, and action indexing.
The [seeded training environment](docs/TRAINING_ENVIRONMENT.md) defines the
event, reset, step, termination, and reproducibility semantics used by RL code.

## Prerequisites

- Go 1.26.4 or newer compatible release
- Python 3.12 or newer
- Make

Go dependencies are pinned in `go.mod` and `go.sum`. The Python package does
not yet require third-party dependencies.

## Project status

| Milestone | Status |
|---|---|
| M0 — Truthful project foundation | Complete |
| M1 — Deterministic simulator vertical slice | Complete |
| M2 — Baseline scheduler suite | Complete |
| M3 — Experiment and measurement platform | In progress (aggregate metrics available) |
| M4–M7 — RL, evidence, and release | Planned |

## Run the smoke simulation

```bash
go run ./cmd/scheduler-sim run --config configs/experiments/smoke.yaml
```

The command defaults to the deterministic `first-fit` policy and writes a
versioned JSON summary to standard output. The baseline choices are
`round-robin`, `random-fit`, `least-loaded`, and `bin-pack`. Use
`--output summary.json` to write the result to a file. Random-fit uses a
policy-local stream derived from the configuration seed, so repeated runs
remain reproducible.

The summary includes streaming latency and queue-wait histograms, throughput,
time-weighted resource utilization and queue depth, resource fragmentation,
bounded-slowdown fairness, deadline outcomes, and scheduler decision telemetry.

Write detailed, provenance-linked results and validate them with:

```bash
go run ./cmd/scheduler-sim run \
  --config configs/experiments/smoke.yaml \
  --results-dir results/smoke
go run ./cmd/result-check results/smoke
```

Result directories are never overwritten. Each bundle contains a versioned
manifest, summary, per-job CSV, and event-aligned time-series CSV with SHA-256
integrity checks.

Run the paired smoke matrix after building the binaries:

```bash
./bin/experiment-run configs/matrices/smoke.yaml
```

This runs five baseline policies on identical steady and bursty workloads for
two seeds, with at most two simulations active at once. Outputs are isolated by
workload, seed, and policy beneath `results/paired-smoke`.

Generate a deterministic paired comparison report with:

```bash
python3 -m scheduler_rl analyze results/paired-smoke/matrix.json \
  --baseline first-fit \
  --output results/paired-smoke-analysis
```

The report includes machine-readable JSON/CSV, a Markdown table, and SVG plots
with paired bootstrap confidence intervals. The smoke matrix verifies the
pipeline; it is too small to support performance claims.

## Development commands

Run `make help` to list all supported commands. The main workflow is:

```bash
make fmt
make lint
make test
make build
make train-smoke
make benchmark-smoke
```

Generated binaries and local caches are written beneath `bin/` and `.cache/`
and are excluded from version control. Override tool commands when needed, for
example `make test PYTHON=python3.12`.
