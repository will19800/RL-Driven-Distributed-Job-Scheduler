# Scheduler RL

Python training and model-export tools for the distributed job scheduler.

The package starts without runtime dependencies. It currently provides paired
experiment analysis with deterministic bootstrap confidence intervals and SVG
plots. It also contains the Python implementation of the shared `policy/v1`
observation/action contract and a dependency-free, event-driven seeded training
environment. Gymnasium, PyTorch, and learning-algorithm dependencies will be
introduced with the PPO milestone.
