# ADR 002: initial PPO and inference runtime

Status: accepted for the first training vertical slice.

The initial trainer uses a small in-repository PyTorch PPO implementation with
synchronous batched rollouts. This keeps the existing dictionary environment
and avoids a Gymnasium adapter solely to satisfy a third-party library. The
implementation declares all algorithm constants and tests actual updates and
exact checkpoint-boundary resume.

Use TorchScript for the initial export and a process bridge in the following
commit. This supports a direct parity fixture without adding a native Go model
runtime. PyTorch tracing is deprecated, so this is a versioned compatibility
choice; no throughput claim may omit interpreter startup or IPC costs. A future
ONNX/runtime change requires measured operator coverage and parity validation.
