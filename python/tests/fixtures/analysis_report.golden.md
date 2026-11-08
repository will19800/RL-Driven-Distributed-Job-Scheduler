# Paired scheduler comparison

Baseline: `first-fit`. Bootstrap samples: 200. Bootstrap seed: 7.

Positive improvement favors the compared policy; latency, queue depth, and deadline violations are direction-adjusted.

| Workload | Policy | Metric | Pairs | Baseline | Policy | Improvement | 95% CI |
|---|---|---|---:|---:|---:|---:|---:|
| smoke | bin-pack | Throughput (jobs/s) | 3 | 11 | 12 | 1 | [-1, 2.33333] |
| smoke | bin-pack | P99 latency (seconds) | 3 | 5 | 4.66667 | 0.333333 | [-1, 1] |
| smoke | bin-pack | Mean queue depth | 3 | 2 | 1.66667 | 0.333333 | [-1, 1] |
| smoke | bin-pack | CPU utilization | 3 | 0.55 | 0.6 | 0.05 | [-0.05, 0.1] |
| smoke | bin-pack | Slowdown fairness (Jain) | 3 | 0.85 | 0.866667 | 0.0166667 | [-0.05, 0.05] |
| smoke | bin-pack | Deadline violation rate | 3 | 0.1 | 0.0666667 | 0.0333333 | [-0.1, 0.1] |

Intervals are percentile confidence intervals from paired seed resampling. They describe uncertainty across the supplied seeds and should not be treated as evidence when the matrix is only a smoke fixture.
