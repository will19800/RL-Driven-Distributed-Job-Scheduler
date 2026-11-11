# Result bundle format

`scheduler-sim run --results-dir DIR` writes a non-overwriting, versioned result
bundle. The command fails if `DIR` already exists so an earlier experiment
cannot be replaced accidentally.

## v1 files

- `manifest.json` records the run ID, wall-clock bounds, policy and seed,
  simulator version, exact configuration path and SHA-256, Git revision and
  dirty state when available, Go/OS/architecture details, pinned module
  versions, and the SHA-256 plus row count of every result artifact.
- `summary.json` contains aggregate workload, latency, utilization, fairness,
  fragmentation, and scheduler metrics.
- `jobs.csv` contains one lifecycle row per submitted job, ordered by submission
  time and job ID.
- `timeseries.csv` contains the initial cluster state and one state sample after
  every processed simulation event. Its event sequence preserves deterministic
  ordering at equal timestamps.

JSON decoding is strict: unknown fields and trailing documents are rejected.
CSV validation checks exact headers, field counts, numeric and duration types,
normalized ranges, and lifecycle-specific fields. Artifact hashes are checked
before schema validation.

Validate a bundle with:

```bash
go run ./cmd/result-check path/to/bundle
```

Detailed rows are opt-in because retaining event-aligned time-series records in
memory is inappropriate for unrestricted large runs. Aggregate metrics remain
streaming and bounded when `--results-dir` is omitted.

RL runs additionally include `summary.inference` (decisions, model calls,
fallbacks, and reason counts). Their manifests include `model` metadata,
`inference_timeout`, and `fallback_policy`. These optional v1 fields are absent
for baseline runs. The current validator checks model metadata and diagnostic
counter consistency as well as the existing bundle hashes and output schemas.
