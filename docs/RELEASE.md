# v0.1 reproducibility and release guide

The repository contains a v0.1 candidate simulator and RL research pipeline.
This guide prepares a release; it does not create a Git tag or GitHub Release.

## Installation and checks

Use Go from `go.mod`, Python 3.12 or later, and Make. For CPU training and export:

```bash
python3 -m venv .venv
.venv/bin/pip install -r configs/training/requirements-tested.txt
.venv/bin/pip install -e 'python[training]'
make lint
make test PYTHON=.venv/bin/python
make build
```

The pinned training snapshot was tested on macOS arm64/Python 3.12; confirm
wheel availability before using it on another platform. Base simulator and
analysis commands do not require PyTorch.

## One-command benchmark reproduction

After installing the training extra, run:

```bash
make benchmark-release PYTHON=.venv/bin/python
```

This builds the Go programs, trains and freezes the declared smoke policy,
generates three scale configurations, executes 20 paired seeds for six policies
on each workload, validates all 360 bundles, computes paired bootstrap
intervals, and profiles a separate baseline run. The result is written beneath
`results/release-reproduction`. Override `BENCHMARK_OUTPUT` with a new path to
repeat; existing result directories are never overwritten.

The compact evidence is in `evidence/`; raw per-job and time-series CSVs stay
under `matrix/`. `provenance.json` records the exact production-source hashes,
binary hash, platform, experiment settings, and elapsed wall time. Every run's
manifest records configuration/output hashes, Git provenance, and model hashes.

The checked-in [v0.1 evidence](../benchmarks/v0.1/README.md) documents the
measured run. Equivalent seeded workload measurements are expected under the
same code and dependency versions; wall-clock timing, CPU profiles, artifact
container hashes, and timestamps can differ across runs and hardware.

## Limits of this candidate

- The RL model is a smoke-trained PPO baseline with four job and two node
  candidates. Its scale results include candidate-exhaustion fallback and do
  not establish learned generalization or an optimized scheduling policy.
- Held-out workloads are synthetic, with 600 jobs per run. A 100,000-job scale
  target, real trace validation, reward ablations, and a training sweep remain
  unverified research work.
- Outages reject running work without retries. The Python training environment
  does not yet model outages; the failure suite therefore tests out-of-training
  behavior and simulator safety.
- The simulated horizon includes scheduled recovery and stale completion events.
  Those events can extend the horizon after useful job processing has ended.
- Latency percentiles use histogram bucket upper bounds and exclude rejected
  jobs. Rejection, completion, and queued-job counts must be read alongside them.
- The TorchScript process bridge adds IPC overhead. Startup is outside decision
  latency but inside run wall time. Tracing is deprecated in current PyTorch.
- Training is deterministic on the tested CPU configuration; identical training
  across platforms/PyTorch versions is not guaranteed.

## Release checklist

- [x] Simulator, baseline suite, metrics, paired analysis, and detailed bundles.
- [x] Seeded environment, reward diagnostics, PPO checkpoint/resume, export.
- [x] Go inference parity, timeout enforcement, validated fallback, fault tests.
- [x] Measured 200-plus-node comparison and compact reproducibility evidence.
- [ ] Resolve the GitHub account billing lock and obtain a passing hosted CI run.
- [ ] Confirm a clean checkout reproduces checks on the supported release host.
- [ ] Choose and consistently set the final Go/Python release version.
- [ ] Review dependency/security status and platform support for release binaries.
- [ ] Create the authorized `v0.1.0` tag, checksums, and GitHub Release artifacts.

The final checklist items are publication gates, not implied actions of running
the benchmark or committing this guide.
