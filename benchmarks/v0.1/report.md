# Paired scheduler comparison

Baseline: `bin-pack`. Bootstrap samples: 10000. Bootstrap seed: 2026.

Positive improvement favors the compared policy; latency, queue depth, and deadline violations are direction-adjusted.

| Workload | Policy | Metric | Pairs | Baseline | Policy | Improvement | 95% CI |
|---|---|---|---:|---:|---:|---:|---:|
| failure-200 | first-fit | Throughput (jobs/s) | 20 | 9.85774 | 9.85774 | 0 | [0, 0] |
| failure-200 | first-fit | P99 latency (seconds) | 20 | 57 | 57 | 0 | [0, 0] |
| failure-200 | first-fit | Mean queue depth | 20 | 0.0494561 | 0.0494561 | 0 | [0, 0] |
| failure-200 | first-fit | CPU utilization | 20 | 0.283628 | 0.283628 | 0 | [0, 0] |
| failure-200 | first-fit | Slowdown fairness (Jain) | 20 | 0.999982 | 0.999982 | 0 | [0, 0] |
| failure-200 | first-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| failure-200 | least-loaded | Throughput (jobs/s) | 20 | 9.85774 | 9.75217 | -0.105574 | [-0.115486, -0.0955171] |
| failure-200 | least-loaded | P99 latency (seconds) | 20 | 57 | 58.5 | -1.5 | [-4.5, 0] |
| failure-200 | least-loaded | Mean queue depth | 20 | 0.0494561 | 0.0329173 | 0.0165388 | [0.00442639, 0.0318825] |
| failure-200 | least-loaded | CPU utilization | 20 | 0.283628 | 0.281004 | -0.00262412 | [-0.00346888, -0.00178042] |
| failure-200 | least-loaded | Slowdown fairness (Jain) | 20 | 0.999982 | 0.999987 | 4.80893e-06 | [1.42428e-06, 8.95644e-06] |
| failure-200 | least-loaded | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| failure-200 | random-fit | Throughput (jobs/s) | 20 | 9.85774 | 9.78547 | -0.0722785 | [-0.113122, -0.0330404] |
| failure-200 | random-fit | P99 latency (seconds) | 20 | 57 | 57 | 0 | [-4.5, 4.5] |
| failure-200 | random-fit | Mean queue depth | 20 | 0.0494561 | 0.0478292 | 0.00162691 | [-0.0291137, 0.0287452] |
| failure-200 | random-fit | CPU utilization | 20 | 0.283628 | 0.282313 | -0.00131534 | [-0.0026103, -1.25429e-05] |
| failure-200 | random-fit | Slowdown fairness (Jain) | 20 | 0.999982 | 0.999981 | -8.19689e-07 | [-1.75761e-05, 1.45684e-05] |
| failure-200 | random-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| failure-200 | rl | Throughput (jobs/s) | 20 | 9.85774 | 9.85653 | -0.00121116 | [-0.00312793, 0] |
| failure-200 | rl | P99 latency (seconds) | 20 | 57 | 57 | 0 | [0, 0] |
| failure-200 | rl | Mean queue depth | 20 | 0.0494561 | 0.0536827 | -0.00422661 | [-0.00759588, -0.000377024] |
| failure-200 | rl | CPU utilization | 20 | 0.283628 | 0.283682 | 5.38266e-05 | [-0.000162643, 0.000245898] |
| failure-200 | rl | Slowdown fairness (Jain) | 20 | 0.999982 | 0.999982 | -2.71945e-07 | [-1.54159e-06, 1.07603e-06] |
| failure-200 | rl | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| failure-200 | round-robin | Throughput (jobs/s) | 20 | 9.85774 | 9.7439 | -0.113841 | [-0.128163, -0.100392] |
| failure-200 | round-robin | P99 latency (seconds) | 20 | 57 | 58.5 | -1.5 | [-4.5, 0] |
| failure-200 | round-robin | Mean queue depth | 20 | 0.0494561 | 0.0297272 | 0.0197288 | [0.00482985, 0.0366236] |
| failure-200 | round-robin | CPU utilization | 20 | 0.283628 | 0.280811 | -0.00281678 | [-0.00357842, -0.00201965] |
| failure-200 | round-robin | Slowdown fairness (Jain) | 20 | 0.999982 | 0.99999 | 7.65028e-06 | [1.58639e-06, 1.50001e-05] |
| failure-200 | round-robin | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| generalization-240 | first-fit | Throughput (jobs/s) | 20 | 11.4635 | 11.5625 | 0.0990886 | [-0.0189854, 0.255541] |
| generalization-240 | first-fit | P99 latency (seconds) | 20 | 57 | 52.5 | 4.5 | [0, 9] |
| generalization-240 | first-fit | Mean queue depth | 20 | 9.39977 | 1.90033 | 7.49944 | [6.47404, 8.66932] |
| generalization-240 | first-fit | CPU utilization | 20 | 0.249878 | 0.252019 | 0.00214123 | [-0.000417073, 0.00546676] |
| generalization-240 | first-fit | Slowdown fairness (Jain) | 20 | 0.983657 | 0.997811 | 0.0141536 | [0.0119017, 0.0167634] |
| generalization-240 | first-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| generalization-240 | least-loaded | Throughput (jobs/s) | 20 | 11.4635 | 11.5467 | 0.0832337 | [-0.0409406, 0.246409] |
| generalization-240 | least-loaded | P99 latency (seconds) | 20 | 57 | 52.5 | 4.5 | [0, 9] |
| generalization-240 | least-loaded | Mean queue depth | 20 | 9.39977 | 1.98875 | 7.41102 | [6.42596, 8.53477] |
| generalization-240 | least-loaded | CPU utilization | 20 | 0.249878 | 0.251682 | 0.0018043 | [-0.000887954, 0.00521256] |
| generalization-240 | least-loaded | Slowdown fairness (Jain) | 20 | 0.983657 | 0.997638 | 0.0139805 | [0.0117835, 0.0165744] |
| generalization-240 | least-loaded | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| generalization-240 | random-fit | Throughput (jobs/s) | 20 | 11.4635 | 11.5569 | 0.0934625 | [-0.0159208, 0.23598] |
| generalization-240 | random-fit | P99 latency (seconds) | 20 | 57 | 54 | 3 | [0, 7.5] |
| generalization-240 | random-fit | Mean queue depth | 20 | 9.39977 | 3.33306 | 6.06671 | [5.34748, 6.846] |
| generalization-240 | random-fit | CPU utilization | 20 | 0.249878 | 0.251902 | 0.00202409 | [-0.000347282, 0.00506476] |
| generalization-240 | random-fit | Slowdown fairness (Jain) | 20 | 0.983657 | 0.995762 | 0.0121049 | [0.0104755, 0.0140481] |
| generalization-240 | random-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| generalization-240 | rl | Throughput (jobs/s) | 20 | 11.4635 | 10.8347 | -0.628728 | [-1.90617, 0.0170121] |
| generalization-240 | rl | P99 latency (seconds) | 20 | 57 | 55.5 | 1.5 | [0, 4.5] |
| generalization-240 | rl | Mean queue depth | 20 | 9.39977 | 26.8246 | -17.4248 | [-52.5211, 0.213767] |
| generalization-240 | rl | CPU utilization | 20 | 0.249878 | 0.23586 | -0.0140184 | [-0.0425184, 0.000393995] |
| generalization-240 | rl | Slowdown fairness (Jain) | 20 | 0.983657 | 0.98447 | 0.000812747 | [-0.000176953, 0.00232986] |
| generalization-240 | rl | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| generalization-240 | round-robin | Throughput (jobs/s) | 20 | 11.4635 | 11.5123 | 0.0488422 | [-0.0179716, 0.127612] |
| generalization-240 | round-robin | P99 latency (seconds) | 20 | 57 | 54 | 3 | [0, 7.5] |
| generalization-240 | round-robin | Mean queue depth | 20 | 9.39977 | 4.60917 | 4.7906 | [4.25541, 5.33777] |
| generalization-240 | round-robin | CPU utilization | 20 | 0.249878 | 0.250952 | 0.0010735 | [-0.000310489, 0.0028068] |
| generalization-240 | round-robin | Slowdown fairness (Jain) | 20 | 0.983657 | 0.993675 | 0.0100178 | [0.00891421, 0.0112638] |
| generalization-240 | round-robin | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | first-fit | Throughput (jobs/s) | 20 | 10.4274 | 10.4274 | 0 | [0, 0] |
| steady-200 | first-fit | P99 latency (seconds) | 20 | 52.5 | 52.5 | 0 | [0, 0] |
| steady-200 | first-fit | Mean queue depth | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | first-fit | CPU utilization | 20 | 0.296232 | 0.296232 | 0 | [0, 0] |
| steady-200 | first-fit | Slowdown fairness (Jain) | 20 | 1 | 1 | 0 | [0, 0] |
| steady-200 | first-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | least-loaded | Throughput (jobs/s) | 20 | 10.4274 | 10.4274 | 0 | [0, 0] |
| steady-200 | least-loaded | P99 latency (seconds) | 20 | 52.5 | 52.5 | 0 | [0, 0] |
| steady-200 | least-loaded | Mean queue depth | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | least-loaded | CPU utilization | 20 | 0.296232 | 0.296232 | 0 | [0, 0] |
| steady-200 | least-loaded | Slowdown fairness (Jain) | 20 | 1 | 1 | 0 | [0, 0] |
| steady-200 | least-loaded | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | random-fit | Throughput (jobs/s) | 20 | 10.4274 | 10.4274 | 0 | [0, 0] |
| steady-200 | random-fit | P99 latency (seconds) | 20 | 52.5 | 52.5 | 0 | [0, 0] |
| steady-200 | random-fit | Mean queue depth | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | random-fit | CPU utilization | 20 | 0.296232 | 0.296232 | 0 | [0, 0] |
| steady-200 | random-fit | Slowdown fairness (Jain) | 20 | 1 | 1 | 0 | [0, 0] |
| steady-200 | random-fit | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | rl | Throughput (jobs/s) | 20 | 10.4274 | 10.4274 | 0 | [0, 0] |
| steady-200 | rl | P99 latency (seconds) | 20 | 52.5 | 52.5 | 0 | [0, 0] |
| steady-200 | rl | Mean queue depth | 20 | 0 | 0.00603362 | -0.00603362 | [-0.00720898, -0.00495383] |
| steady-200 | rl | CPU utilization | 20 | 0.296232 | 0.296232 | 2.77556e-18 | [-1.94289e-17, 2.77556e-17] |
| steady-200 | rl | Slowdown fairness (Jain) | 20 | 1 | 0.999999 | -6.84501e-07 | [-8.98826e-07, -4.93158e-07] |
| steady-200 | rl | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | round-robin | Throughput (jobs/s) | 20 | 10.4274 | 10.4274 | 0 | [0, 0] |
| steady-200 | round-robin | P99 latency (seconds) | 20 | 52.5 | 52.5 | 0 | [0, 0] |
| steady-200 | round-robin | Mean queue depth | 20 | 0 | 0 | 0 | [0, 0] |
| steady-200 | round-robin | CPU utilization | 20 | 0.296232 | 0.296232 | 0 | [0, 0] |
| steady-200 | round-robin | Slowdown fairness (Jain) | 20 | 1 | 1 | 0 | [0, 0] |
| steady-200 | round-robin | Deadline violation rate | 20 | 0 | 0 | 0 | [0, 0] |

Intervals are percentile confidence intervals from paired seed resampling. They describe uncertainty across the supplied seeds and should not be treated as evidence when the matrix is only a smoke fixture.
