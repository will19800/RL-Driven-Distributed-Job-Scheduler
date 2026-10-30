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
schema and links to the tested smoke configuration.

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
| M3 — Experiment and measurement platform | Next |
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
