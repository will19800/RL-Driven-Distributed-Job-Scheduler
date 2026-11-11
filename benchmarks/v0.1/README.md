# v0.1 measured scale evidence

Measured on November 11, 2026: 360 runs, 216,000 submitted jobs, three synthetic
workloads, six policies, and 20 paired held-out seeds (2001–2020). Every bundle
passed integrity and schema checks. No invalid scheduling decisions or policy
errors were recorded. This evidence does not establish an RL performance win.

## Completion comes before latency interpretation

Counts below total the 20 seeds for each workload/policy (12,000 jobs per row).
Throughput is the mean across per-seed runs, in completed jobs per simulated
second. Bin-pack is the comparison baseline.

| Workload | Policy | Completed | Rejected | Still queued | Mean throughput |
|---|---|---:|---:|---:|---:|
| failure-200 | bin-pack | 11,341 | 659 | 0 | 9.8577 |
| failure-200 | RL | 11,341 | 659 | 0 | 9.8565 |
| generalization-240 | bin-pack | 12,000 | 0 | 0 | 11.4635 |
| generalization-240 | RL | 11,401 | 0 | 599 | 10.8347 |
| steady-200 | bin-pack | 12,000 | 0 | 0 | 10.4274 |
| steady-200 | RL | 12,000 | 0 | 0 | 10.4274 |

In generalization seed **2011**, the RL model completed one job and stranded 599
queued jobs by selecting valid defer actions. Inference safety validation cannot
turn a valid but unproductive action into a useful policy. This is a known
progress failure, not an excluded run. Completed-job latency therefore has
selection bias for this policy. The apparently better mean histogram p99
(55.5 seconds versus 57.0) must not be read as an improvement in delivered
service.

Across the 60 RL runs, the adapter recorded 106,842 scheduler decisions, 1,558
actual model calls, and 34,967 fallbacks (32.73% of decisions). All fallbacks
were candidate-exhaustion recoveries; there were no runtime or timeout
fallbacks in this measurement. The frozen smoke model sees only four queued
jobs and two nodes. Most of its scale behavior therefore relies on baseline
fallback or ordinary deferral, rather than demonstrating learned scale behavior.

The unweighted mean of per-run mean decision time was 167.185 microseconds for
bin-pack and 248.074 microseconds for RL. These are observed wall-clock values
under two-worker execution on a shared machine, not isolated microbenchmarks.
RL timing includes encoding, IPC, validation, and fallback; interpreter startup
is excluded from decision time and included in run wall time.

## Experimental protocol

- Workloads: 200 homogeneous nodes at steady load; 240 heterogeneous CPU,
  memory, and GPU nodes with shifted job shapes; and 200 nodes with 50
  deterministically selected nodes failing at 5 seconds and recovering at 15.
- Each workload has 600 jobs, Poisson submissions, and bounded log-normal
  durations (median 10 seconds, sigma 0.5, bounds 1–60 seconds).
- Policies: first-fit, round-robin, random-fit, least-loaded, bin-pack, and RL.
- Training: the declared three-update smoke PPO configuration with training
  seed 42 and separate validation seeds 1000001–1000002. The selected model was
  frozen before evaluation; held-out results did not choose it.
- No warm-up exclusion or failed-run exclusion. Outage-rejected work and
  unfinished queues are retained. Latency histograms cover completed jobs.
- Intervals: 10,000 paired-seed bootstrap resamples, seed 2026, two-sided 95%
  percentile intervals. Six metrics and five policy comparisons per workload
  produce 90 comparison rows. Intervals are descriptive and not corrected for
  multiple comparisons.
- Host: macOS 15.5, arm64, eight logical CPUs, Python 3.12.1, PyTorch 2.15.0;
  two parallel workers. The complete pipeline took approximately 226.7 seconds.

For example, RL minus bin-pack throughput on the heterogeneous suite averaged
−0.6287 jobs/s, with a paired 95% interval of [−1.9062, 0.0170]. This interval
and the unfinished workload do not support a positive performance claim.

## Artifacts and provenance

- [Full comparison table](report.md) and [machine-readable comparisons](analysis.json).
- [Per-run completion, timing, and fallback counters](runs.csv).
- [Every run's manifest](manifests.json), including input/output and model hashes.
- [Exact generated configurations](configs) and [training provenance](training.json).
- [Model metadata](model-metadata.json) and [source/binary/environment hashes](provenance.json).
- [Separate CPU profile summary](profile.txt) and [metric confidence-interval plots](plots).

The manifests identify base revision `b39df0d` with a dirty worktree because
the outage implementation and publication were delivered in this final
consolidated commit. `provenance.json` freezes the exact production-source hashes
used during measurement; the runner refuses publication if those sources
change during execution. No post-measurement simulator optimization is included.
The generated package is under 1 MiB; bulky raw CSV bundles and model binaries
remain in the ignored local results directory and can be regenerated.

The short separate CPU profile has 890 ms of samples, dominated by runtime
wait/OS frames; simulation job sorting is visible at 2.25% flat samples. That
window is insufficient to justify a specific optimization or a scalable
throughput claim. Full-state copying, validation, and snapshot sorting remain
profiling targets before a 100,000-job experiment.

See the [release guide](../../docs/RELEASE.md) for one-command reproduction,
the complete limitations, and the publication checklist. In particular, this
candidate has not validated the 100,000-job target, real-trace generalization,
reward ablations, or cross-platform training reproducibility.
