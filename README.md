# RL-Driven Distributed Job Scheduler

A research-oriented distributed cluster scheduler simulator being built with a
Go simulation core and Python reinforcement-learning tooling.

The project is currently in its foundation phase. The simulator, scheduling
policies, RL training pipeline, and performance benchmarks are planned but not
yet implemented. Earlier references to a 27% p99 latency reduction and an 18%
utilization improvement are target hypotheses; no repository evidence currently
validates those figures.

See [the design and delivery plan](docs/PROJECT_DESIGN.md) for the architecture,
milestones, validation rules, and ordered commit roadmap. The
[configuration reference](docs/CONFIGURATION.md) documents the strict v1 YAML
schema and links to the tested smoke configuration.

## Prerequisites

- Go 1.26.4 or newer compatible release
- Python 3.12 or newer
- Make

The current project skeleton has no third-party runtime dependencies.

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
