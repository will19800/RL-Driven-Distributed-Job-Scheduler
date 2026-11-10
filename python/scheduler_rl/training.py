"""Small synchronous batched PPO baseline and versioned TorchScript export.

Training extras are imported only by this module; base tooling stays lightweight.
"""

import hashlib
import json
import platform
import subprocess
from pathlib import Path

import torch
from torch import nn
from torch.distributions import Categorical

from scheduler_rl.environment import SyntheticEpisodeConfig, SyntheticEpisodeFactory, TrainingEnvironment
from scheduler_rl.policy_v1 import schema
from scheduler_rl.reward import RewardConfig, empty_terms


def flatten(observation):
    return [value for row in observation["node_features"] for value in row] + [value for row in observation["job_features"] for value in row] + observation["global_features"]


class ActorCritic(nn.Module):
    def __init__(self, inputs, actions):
        super().__init__()
        self.body = nn.Sequential(nn.Linear(inputs, 64), nn.Tanh())
        self.actor = nn.Linear(64, actions)
        self.critic = nn.Linear(64, 1)

    def forward(self, features, mask):
        hidden = self.body(features)
        return self.actor(hidden).masked_fill(~mask, -1e9), self.critic(hidden).squeeze(-1)


def load_config(path):
    config = json.loads(Path(path).read_text())
    required = {"policy", "episode", "reward", "seed", "updates", "workers", "rollout_steps", "epochs", "learning_rate", "gamma", "validation_seeds", "max_episode_steps"}
    if set(config) != required:
        raise ValueError("training config fields do not match training/v1")
    if type(config["seed"]) is not int or not 0 <= config["seed"] < 2**63:
        raise ValueError("seed must be a nonnegative int64")
    schema(config["policy"])
    RewardConfig(**config["reward"]).validate()
    SyntheticEpisodeFactory(SyntheticEpisodeConfig(**config["episode"]))
    for name in ("updates", "workers", "rollout_steps", "epochs", "max_episode_steps"):
        if type(config[name]) is not int or config[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if not 0 < config["gamma"] <= 1 or not 0 < config["learning_rate"] < 1:
        raise ValueError("invalid optimizer parameters")
    if not config["validation_seeds"] or any(type(seed) is not int for seed in config["validation_seeds"]):
        raise ValueError("validation_seeds must be nonempty integers")
    if any(config["seed"] <= seed < config["seed"] + config["updates"] * config["workers"] * (config["rollout_steps"] + 1) + config["workers"] for seed in config["validation_seeds"]):
        raise ValueError("validation seeds overlap training episode seeds")
    return config


def make_environment(config):
    return TrainingEnvironment(config["policy"], SyntheticEpisodeFactory(SyntheticEpisodeConfig(**config["episode"])), reward_config=RewardConfig(**config["reward"]))


def tensors(observations):
    return torch.tensor([flatten(item) for item in observations], dtype=torch.float32), torch.tensor([item["action_mask"] for item in observations], dtype=torch.bool)


def evaluate(model, config):
    returns = []
    for seed in config["validation_seeds"]:
        env = make_environment(config)
        observation, _ = env.reset(seed=seed)
        score = 0.0
        for _ in range(config["max_episode_steps"]):
            with torch.no_grad():
                logits, _ = model(*tensors([observation]))
            observation, reward, terminated, truncated, _ = env.step(int(logits.argmax(-1)[0]))
            score += reward
            if terminated or truncated:
                break
        else:
            score -= 100.0  # selection penalizes policies that fail to finish
        returns.append(score)
    return sum(returns) / len(returns)


def train(config_path, output, resume=None):
    config = load_config(config_path)
    output = Path(output)
    if output.exists():
        raise ValueError("training output already exists")
    torch.set_num_threads(1)
    torch.manual_seed(config["seed"])
    torch.use_deterministic_algorithms(True)
    envs = [make_environment(config) for _ in range(config["workers"])]
    observations = [env.reset(seed=config["seed"] + i)[0] for i, env in enumerate(envs)]
    inputs, actions = len(flatten(observations[0])), len(observations[0]["action_mask"])
    model = ActorCritic(inputs, actions)
    optimizer = torch.optim.Adam(model.parameters(), lr=config["learning_rate"])
    start, best = 0, float("-inf")
    if resume:
        checkpoint = torch.load(resume, map_location="cpu", weights_only=True)
        if checkpoint.get("format") != "training/v1":
            raise ValueError("unsupported checkpoint format")
        if checkpoint["config"] != config or checkpoint["schema"] != schema(config["policy"]):
            raise ValueError("resume configuration/schema mismatch")
        model.load_state_dict(checkpoint["model"])
        optimizer.load_state_dict(checkpoint["optimizer"])
        torch.set_rng_state(checkpoint["rng"])
        start, best = checkpoint["update"], checkpoint["best_score"]
    if start >= config["updates"]:
        raise ValueError("checkpoint has no remaining updates")
    output.mkdir(parents=True)
    history = []
    for update in range(start, config["updates"]):
        # Reset at update boundaries so checkpoint/resume needs no mutable env state.
        episode_counter = config["seed"] + update * config["workers"] * (config["rollout_steps"] + 1) + config["workers"]
        observations = [env.reset(seed=episode_counter + i)[0] for i, env in enumerate(envs)]
        episode_counter += len(envs)
        batch = []
        reward_diagnostics = empty_terms()
        for _ in range(config["rollout_steps"]):
            features, masks = tensors(observations)
            with torch.no_grad():
                logits, values = model(features, masks)
                distribution = Categorical(logits=logits)
                selected = distribution.sample()
                log_probs = distribution.log_prob(selected)
            next_observations, rewards, dones = [], [], []
            for i, env in enumerate(envs):
                obs, reward, terminated, truncated, info = env.step(int(selected[i]))
                for key, value in info["reward_terms"].items():
                    reward_diagnostics[key] += value
                done = terminated or truncated
                if done:
                    obs, _ = env.reset(seed=episode_counter)
                    episode_counter += 1
                next_observations.append(obs); rewards.append(reward); dones.append(done)
            batch.append((features, masks, selected, log_probs, values, torch.tensor(rewards), torch.tensor(dones)))
            observations = next_observations
        with torch.no_grad():
            _, returns = model(*tensors(observations))
        targets = []
        for item in reversed(batch):
            returns = item[5] + config["gamma"] * returns * (~item[6])
            targets.append(returns)
        targets = torch.stack(list(reversed(targets))).flatten()
        features = torch.cat([item[0] for item in batch]); masks = torch.cat([item[1] for item in batch])
        selected = torch.cat([item[2] for item in batch]); old_logs = torch.cat([item[3] for item in batch])
        advantages = targets - torch.cat([item[4] for item in batch])
        advantages = (advantages - advantages.mean()) / (advantages.std(unbiased=False) + 1e-8)
        for _ in range(config["epochs"]):
            logits, values = model(features, masks)
            distribution = Categorical(logits=logits)
            ratio = (distribution.log_prob(selected) - old_logs).exp()
            loss = -torch.minimum(ratio * advantages, ratio.clamp(0.8, 1.2) * advantages).mean() + 0.5 * (values - targets).square().mean() - 0.01 * distribution.entropy().mean()
            if not torch.isfinite(loss):
                raise ValueError("nonfinite PPO loss")
            optimizer.zero_grad(); loss.backward(); nn.utils.clip_grad_norm_(model.parameters(), 0.5); optimizer.step()
        score = evaluate(model, config)
        history.append({"update": update + 1, "loss": float(loss.detach()), "validation_score": score, "reward_terms": reward_diagnostics})
        improved = score > best
        best = max(best, score)
        checkpoint = {"format": "training/v1", "schema": schema(config["policy"]), "config": config, "model": model.state_dict(), "optimizer": optimizer.state_dict(), "rng": torch.get_rng_state(), "update": update + 1, "best_score": best}
        torch.save(checkpoint, output / "last.pt")
        if improved:
            torch.save(checkpoint, output / "best.pt")
        torch.save(checkpoint, output / f"update-{update + 1}.pt")
    metadata = {"format": "training/v1", "config": config, "config_sha256": hashlib.sha256(Path(config_path).read_bytes()).hexdigest(), "torch": str(torch.__version__), "python": platform.python_version(), "git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "git_modified": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()), "selection": "mean validation episode reward", "history": history}
    (output / "training.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def load_export(directory, expected_schema):
    """Validate metadata and hashes before loading a locally trusted artifact."""
    directory = Path(directory)
    metadata = json.loads((directory / "metadata.json").read_text())
    if metadata.get("format") != "inference/v1" or metadata.get("runtime") != "torchscript" or metadata.get("schema") != expected_schema:
        raise ValueError("incompatible inference metadata")
    for name, key in (("policy.ts", "model_sha256"), ("parity.json", "parity_sha256")):
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != metadata[key]:
            raise ValueError(f"artifact hash mismatch: {name}")
    model = torch.jit.load(str(directory / "policy.ts"), map_location="cpu")
    fixture = json.loads((directory / "parity.json").read_text())
    with torch.no_grad():
        logits, values = model(torch.tensor(fixture["features"]), torch.tensor(fixture["mask"], dtype=torch.bool))
    if not torch.allclose(logits, torch.tensor(fixture["logits"]), atol=1e-5) or not torch.allclose(values, torch.tensor(fixture["values"]), atol=1e-5):
        raise ValueError("artifact parity check failed")
    return model


def export(checkpoint_path, output):
    checkpoint = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
    if checkpoint.get("format") != "training/v1":
        raise ValueError("unsupported checkpoint format")
    config = checkpoint["config"]
    contract = schema(config["policy"])
    if checkpoint["schema"] != contract:
        raise ValueError("checkpoint policy schema mismatch")
    observation, _ = make_environment(config).reset(seed=config["validation_seeds"][0])
    features, masks = tensors([observation])
    model = ActorCritic(features.shape[1], masks.shape[1])
    model.load_state_dict(checkpoint["model"]); model.eval()
    if not all(torch.isfinite(item).all() for item in model.parameters()):
        raise ValueError("nonfinite model weights")
    output = Path(output)
    if output.exists():
        raise ValueError("export output already exists")
    output.mkdir(parents=True)
    artifact = torch.jit.trace(model, (features, masks))
    artifact.save(str(output / "policy.ts"))
    with torch.no_grad():
        logits, values = artifact(features, masks)
    fixture = {"observation": observation, "features": features.tolist(), "mask": masks.tolist(), "logits": logits.tolist(), "values": values.tolist()}
    (output / "parity.json").write_text(json.dumps(fixture, indent=2) + "\n")
    metadata = {"format": "inference/v1", "runtime": "torchscript", "schema": contract, "torch": str(torch.__version__), "model_sha256": hashlib.sha256((output / "policy.ts").read_bytes()).hexdigest(), "checkpoint_sha256": hashlib.sha256(Path(checkpoint_path).read_bytes()).hexdigest(), "parity_sha256": hashlib.sha256((output / "parity.json").read_bytes()).hexdigest()}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata
