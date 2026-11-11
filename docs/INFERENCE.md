# Go inference and fallback

Build the Go commands and export a model using the [training guide](TRAINING.md).
Run that model through the simulator with:

```bash
make build
PYTHONPATH=python ./bin/scheduler-sim run \
  --config configs/experiments/synthetic-smoke.yaml \
  --policy rl --model results/policy-smoke \
  --python .venv/bin/python --fallback bin-pack \
  --inference-timeout 250ms --results-dir results/rl-smoke
./bin/result-check results/rl-smoke
```

The selected Python interpreter must have the training extra installed and be
able to import `scheduler_rl`. The example sets `PYTHONPATH` for a repository
checkout; an editable install also works.

## Startup and process lifetime

Go strictly reads the model metadata, validates `inference/v1`, the TorchScript
runtime, the complete `policy/v1` schema, and artifact hash syntax. It verifies
the model and parity fixture bytes before launching one persistent Python
worker. The worker loads the model once, verifies its fixed output fixture,
and sends a protocol handshake. Go then independently flattens the fixture
observation and requests its logits to check cross-language parity.

The schema fixes tensor dimensions and feature ordering. The parity check
accounts for the model's float32 inputs and permits relative output error of
`1e-5`. Startup has a separate 30-second deadline; metadata, hash, startup, and
parity failures fail the run with `model_load_failed`. They do not silently
turn an incorrectly configured RL experiment into a baseline run.

Only locally trusted exports should be loaded. Hashes establish integrity, not
the identity of the publisher or safety of arbitrary TorchScript code.

## Decision safety

Go builds every observation from an immutable scheduler snapshot. It estimates
arrivals using the same trailing 60-second window as the Python environment.
The model receives flattened node rows, job rows, then global features, along
with the boolean action mask. Responses are strictly decoded JSON lines.

For each decision, Go:

1. Applies one deadline to both writing the request and reading the response.
2. Requires exactly one finite logit per action, including defer.
3. Selects the greatest logit among feasible actions only; ties use the lowest
   action index.
4. Decodes candidate IDs and validates the resulting decision against the
   snapshot before returning it to the normal scheduler validator.

If no placement fits the candidates but another submitted queued job fits the
full cluster, a recorded `candidates` fallback prevents prefix truncation from
stranding feasible work. If the entire queue is empty or blocked, inference is
skipped and scheduling defers normally. A model-selected valid defer remains
an explicit policy choice; it is not classified as an inference failure.

Decision failures use a configured baseline fallback (`bin-pack` by default).
Failure reasons include `timeout`, `runtime`, `shape`, `nonfinite`, `encoding`,
`action`, `validation`, and `candidates`. The fallback decision is also fully
validated. Parent context cancellation stops the simulation instead of invoking
fallback on an already-canceled run.

In-flight timeouts, pipe failures, and malformed protocol output kill and reap the worker.
After such a failure the worker stays closed; later decisions use fallback with
a `runtime` reason. It is not restarted in a way that adds hidden startup cost.
The interpreter is also killed and reaped when the run finishes.

## Diagnostics and timing

RL summaries contain an `inference` object with total decision count, actual
model-call count, fallback count, and per-reason counts. Manifests retain the
complete model metadata (including model/checkpoint/parity hashes), configured
deadline, and fallback policy. Bundle validation checks diagnostic totals and
model metadata. Baseline summaries keep their existing output shape.

Scheduler decision timing includes encoding, JSON IPC, model execution,
validation, and fallback when used. Worker startup and startup parity are outside
per-decision timing but inside the manifest's run wall-clock bounds. This bridge
is a debugging and compatibility implementation; scale comparisons must report
its overhead and fallback rate.

## Tests with a real export

Transport and injected fault tests run without Python dependencies. To include
the real model startup, parity, CLI, and forced-timeout tests:

```bash
PYTHONPATH="$PWD/python" \
SCHEDULER_TEST_MODEL="$PWD/results/policy-smoke" \
SCHEDULER_TEST_PYTHON="$PWD/.venv/bin/python" \
make test-go
```
