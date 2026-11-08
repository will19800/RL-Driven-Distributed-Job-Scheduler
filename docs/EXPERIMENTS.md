# Experiment matrices

`experiment-run` executes paired workload, policy, and seed matrices through the
same `scheduler-sim` command used for individual simulations. Each matrix cell
gets an isolated, validated result bundle and every policy receives the same
generated workload for a given workload/seed pair.

Build the commands and run the checked-in smoke matrix with:

```bash
make build
./bin/experiment-run configs/matrices/smoke.yaml
```

The command locates `scheduler-sim` beside its own binary by default. Use
`--scheduler PATH` when the programs are installed separately.

## Schema v1

An experiment YAML document has these fields:

- `schema_version`: must be `v1`.
- `output_dir`: a new directory; existing outputs are never overwritten.
- `workers`: maximum concurrent simulations, from 1 through 256.
- `seeds`: unique signed 64-bit seeds.
- `policies`: unique canonical baseline names: `first-fit`, `round-robin`,
  `random-fit`, `least-loaded`, or `bin-pack`.
- `workloads`: unique path-safe names paired with simulation configuration
  paths.

Paths resolve relative to the experiment file. Unknown fields, duplicate
dimensions, missing workload files, unsafe names, matrices over 10,000 runs,
and multiple YAML documents are rejected before execution.

## Output layout

Bundles use a deterministic hierarchy:

```text
OUTPUT_DIR/
├── matrix.json
└── WORKLOAD/
    └── seed-SEED/
        └── POLICY/
            ├── manifest.json
            ├── summary.json
            ├── jobs.csv
            └── timeseries.csv
```

`matrix.json` is written only after every cell succeeds. Its run records are
sorted by workload, seed, and policy regardless of worker completion order.
Each record identifies the source configuration and relative bundle path.

On failure, the command cancels outstanding work and returns a nonzero status.
Completed bundles remain for diagnosis, but the absent `matrix.json` makes the
incomplete experiment unambiguous. Choose a new output directory to rerun.

## Statistical comparison report

Analyze a completed matrix against a baseline policy with:

```bash
python3 -m scheduler_rl analyze results/paired-smoke/matrix.json \
  --baseline first-fit \
  --output results/paired-smoke-analysis \
  --bootstrap-samples 10000 \
  --seed 2026
```

The analyzer requires an identical seed set for the baseline and every compared
policy within each workload. It computes paired per-seed improvements for:

- throughput;
- p99 latency;
- mean queue depth;
- CPU utilization;
- bounded-slowdown Jain fairness; and
- deadline violation rate.

Improvement is direction-normalized: positive values always favor the compared
policy. For latency, queue depth, and deadline violations this means baseline
minus candidate; for the other metrics it means candidate minus baseline.

Each mean improvement receives a two-sided 95% percentile interval from paired
bootstrap resampling. Random streams are deterministically derived from the
requested seed plus the workload, policy, and metric, so results do not depend
on iteration order. For credible claims, use at least 20 paired seeds and
increase the resample count after power analysis; the two-seed smoke matrix only
tests the pipeline.

The output directory is never overwritten and contains:

- `analysis.json`: versioned machine-readable comparisons;
- `comparisons.csv`: flat table for downstream tools;
- `report.md`: a human-readable comparison table; and
- `plots/*.svg`: dependency-free confidence-interval plots for every metric.
