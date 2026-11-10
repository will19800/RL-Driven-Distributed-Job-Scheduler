# Reward, PPO training, and export

Install optional training dependencies in an isolated environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -e 'python[training]'
PYTHONPATH=python .venv/bin/python -m scheduler_rl train \
  configs/training/smoke.json --output results/training-smoke
PYTHONPATH=python .venv/bin/python -m scheduler_rl export \
  results/training-smoke/best.pt --output results/policy-smoke
```

Output directories must be new. The base package remains dependency-free;
training tests run when PyTorch is installed and otherwise skip explicitly.
Run all training tests with `make test-python PYTHON=.venv/bin/python`.
The exact packages used for the local CPU smoke run are recorded in
[`requirements-tested.txt`](../configs/training/requirements-tested.txt).
Install that file before the editable package to reproduce the tested versions.

## Reward accounting

`RewardConfig` supplies finite nonnegative weights and positive scales. Every
successful step reports raw signed `reward_terms` and episode `reward_totals`.
The scalar reward is the weighted sum:

- throughput: number of completed jobs;
- queue: negative queued-job-seconds divided by `time_scale`;
- slowdown: negative completed-job bounded slowdown, capped at
  `slowdown_clip` and divided by that cap;
- fragmentation: negative time integral of the mean per-node spread in free
  resource fractions, divided by `time_scale`;
- SLA: negative count of completed jobs past their deadlines;
- invalid: negative count of rejected actions, exposed in diagnostics.

Queue and fragmentation integrals are accumulated before every event batch,
using the state that held during that interval. Queue waiting is deliberately
uncapped so prolonged starvation continues to incur cost. Truncation additionally
charges one queue unit per unfinished queued job. Invalid actions raise
`EnvironmentError`, add an invalid diagnostic, and preserve simulation state;
the masked trainer never intentionally samples them.

## PPO baseline

The synchronous trainer batches independent environments on CPU. A shared
64-unit tanh body feeds a discrete actor and scalar critic. It masks placement
logits, uses discounted bootstrapped returns, normalized advantages, a clipped
PPO surrogate (epsilon 0.2), squared value loss (weight 0.5), entropy bonus
(weight 0.01), Adam, and gradient clipping at 0.5. JSON config controls rollout
length, environment count, update epochs, learning rate, discount, and reward.

Each update resets environments with independently assigned training seeds.
This defines the checkpoint boundary and permits exact resume without serializing
mutable environments. Checkpoints include model, optimizer, RNG, complete
configuration, schema, completed-update count, and best validation score.
Validation uses greedy actions on separate declared seeds; mean episode reward
selects `best.pt`. A step-limit breach receives an additional selection penalty.
Test workloads never select checkpoints.

```bash
PYTHONPATH=python .venv/bin/python -m scheduler_rl train \
  configs/training/smoke.json --output results/resumed \
  --resume results/training-smoke/update-1.pt
```

Resume requires the original configuration and a checkpoint with remaining
updates. The resumed directory contains `best.pt` only when a subsequent update
improves the saved best score; retain the original best checkpoint otherwise.
`training.json` records configuration/hash, Python and PyTorch versions, Git
revision/dirty state, selection rule, loss, and validation scores per update.

## Inference artifact

Exports contain `policy.ts`, `metadata.json`, and `parity.json`. Metadata fixes
`inference/v1`, the TorchScript runtime, exact `policy/v1` schema, PyTorch
version, checkpoint hash, model hash, and parity hash. The fixture records an
actual observation, flattened features, mask, expected logits, and value.
`load_export` rejects incompatible metadata or hashes before loading and checks
the fixed outputs afterward. Checkpoints use restricted `weights_only=True`
loading. Load artifacts only from trusted sources: hashes establish integrity,
not publisher authenticity.

TorchScript is a compatibility bridge for the next process-based Go adapter.
Current PyTorch marks tracing deprecated; a future runtime migration must retain
the same schema and parity checks. This smoke workload proves the pipeline,
not scheduling performance or generalization.
