"""Deterministic event-driven training environment for policy/v1."""

from __future__ import annotations

import copy
import hashlib
import heapq
import math
import random
from dataclasses import dataclass
from typing import Any, Callable

from scheduler_rl.policy_v1 import ContractError, RESOURCE_KEYS, decode_action, encode
from scheduler_rl.reward import RewardConfig, empty_terms


class EnvironmentError(ValueError):
    """Raised when an episode or environment transition is invalid."""


@dataclass(frozen=True)
class Episode:
    """Initial nodes and queued jobs for one deterministic episode."""

    nodes: list[dict[str, Any]]
    jobs: list[dict[str, Any]]


@dataclass(frozen=True)
class SyntheticEpisodeConfig:
    """Small dependency-free seeded workload family for training smoke runs."""

    node_count: int
    node_capacity: dict[str, int]
    job_count: int
    arrival_rate: float
    duration_min: float
    duration_max: float
    request: dict[str, int]


class SyntheticEpisodeFactory:
    """Generate reproducible Poisson arrivals and uniform durations by seed."""

    def __init__(self, config: SyntheticEpisodeConfig):
        self.config = config
        _validate_synthetic_config(config)

    def __call__(self, seed: int) -> Episode:
        arrivals = random.Random(_stream_seed(seed, "arrivals"))
        durations = random.Random(_stream_seed(seed, "durations"))
        nodes = [{"id": f"node-{index + 1:04d}", "capacity": dict(self.config.node_capacity), "allocated": _zero_resources(), "state": "available"} for index in range(self.config.node_count)]
        jobs, submit = [], 0.0
        for index in range(self.config.job_count):
            submit += arrivals.expovariate(self.config.arrival_rate)
            jobs.append({"id": f"job-{index + 1:06d}", "submit_time_seconds": submit, "request": dict(self.config.request), "duration_seconds": durations.uniform(self.config.duration_min, self.config.duration_max), "priority": 0, "deadline_seconds": None, "state": "queued"})
        return Episode(nodes, jobs)


class TrainingEnvironment:
    """Gymnasium-style event environment with deterministic reset and step.

    Step returns ``(observation, reward, terminated, truncated,
    info)`` and reset returns ``(observation, info)``.
    """

    def __init__(self, policy_config: dict[str, Any], episode_factory: Callable[[int], Episode], arrival_window_seconds: float = 60.0, reward_config: RewardConfig | None = None):
        if not callable(episode_factory):
            raise EnvironmentError("episode_factory must be callable")
        if isinstance(arrival_window_seconds, bool) or not isinstance(arrival_window_seconds, (int, float)) or not math.isfinite(arrival_window_seconds) or arrival_window_seconds <= 0:
            raise EnvironmentError("arrival_window_seconds must be positive and finite")
        self.policy_config = copy.deepcopy(policy_config)
        self.episode_factory = episode_factory
        self.arrival_window_seconds = float(arrival_window_seconds)
        self._initialized = False
        self._done = False
        self.reward_config = reward_config or RewardConfig()
        self.reward_config.validate()

    def reset(self, *, seed: int | None = None, options: dict[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        if options not in (None, {}):
            raise EnvironmentError("reset options are not supported")
        episode_seed = 0 if seed is None else seed
        if isinstance(episode_seed, bool) or not isinstance(episode_seed, int):
            raise EnvironmentError("seed must be an integer")
        episode = self.episode_factory(episode_seed)
        if not isinstance(episode, Episode):
            raise EnvironmentError("episode_factory must return Episode")
        self.seed, self.now, self.steps, self.completed = episode_seed, 0.0, 0, 0
        self.nodes = copy.deepcopy(episode.nodes)
        self.pending: dict[str, dict[str, Any]] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.events: list[tuple[float, int, str, str]] = []
        self.sequence = 0
        self.arrivals: list[float] = []
        self.reward_terms = empty_terms()
        self.reward_totals = empty_terms()
        self._validate_episode(episode)
        for job in copy.deepcopy(episode.jobs):
            self.pending[job["id"]] = job
            self._schedule(job["submit_time_seconds"], "submit", job["id"])
        self._initialized, self._done = True, False
        self._advance(force_event=True)
        self.reward_terms = empty_terms()
        observation = self._observation()
        return observation, self._info()

    def step(self, action_index: int) -> tuple[dict[str, Any], float, bool, bool, dict[str, Any]]:
        if not self._initialized:
            raise EnvironmentError("reset must be called before step")
        if self._done:
            raise EnvironmentError("step called after episode finished")
        observation = self._observation()
        self.reward_terms = empty_terms()
        try:
            action = decode_action(self.policy_config, observation, action_index)
        except ContractError as error:
            self.reward_terms["invalid"] = -1.0
            self.reward_totals["invalid"] -= 1.0
            raise EnvironmentError(str(error)) from error
        self.steps += 1
        if action["defer"]:
            progressed = self._advance(force_event=True)
        else:
            job_id = observation["job_ids"][action["job_index"]]
            node_id = observation["node_ids"][action["node_index"]]
            self._start(job_id, node_id)
            progressed = self._advance(force_event=False)
        terminated = self._terminated()
        truncated = not terminated and not progressed and not self.events
        self._done = terminated or truncated
        info = self._info()
        if truncated:
            info["truncation_reason"] = "unschedulable_queue"
            self.reward_terms["queue"] -= sum(job["state"] == "queued" for job in self.jobs.values())
        for key, value in self.reward_terms.items():
            self.reward_totals[key] += value
        info["reward_terms"] = dict(self.reward_terms)
        info["reward_totals"] = dict(self.reward_totals)
        return self._observation(), self.reward_config.weighted(self.reward_terms), terminated, truncated, info

    def _validate_episode(self, episode: Episode) -> None:
        if not episode.nodes or not episode.jobs:
            raise EnvironmentError("episode requires at least one node and job")
        node_ids, job_ids = set(), set()
        for node in episode.nodes:
            if node.get("allocated") != _zero_resources():
                raise EnvironmentError("initial nodes must have zero allocation")
            node_id = node.get("id")
            if node_id in node_ids:
                raise EnvironmentError(f"duplicate node ID {node_id!r}")
            node_ids.add(node_id)
        for job in episode.jobs:
            job_id = job.get("id")
            if job.get("state") != "queued":
                raise EnvironmentError("initial jobs must be queued")
            if job_id in job_ids:
                raise EnvironmentError(f"duplicate job ID {job_id!r}")
            job_ids.add(job_id)
        try:
            encode(self.policy_config, {"now_seconds": 0.0, "arrival_rate": 0.0, "nodes": episode.nodes, "jobs": []})
            encode(self.policy_config, {"now_seconds": max(job["submit_time_seconds"] for job in episode.jobs), "arrival_rate": 0.0, "nodes": episode.nodes, "jobs": episode.jobs})
        except (ContractError, KeyError, TypeError) as error:
            raise EnvironmentError(f"invalid episode: {error}") from error

    def _schedule(self, when: float, kind: str, job_id: str) -> None:
        heapq.heappush(self.events, (when, self.sequence, kind, job_id)); self.sequence += 1

    def _advance(self, force_event: bool) -> bool:
        progressed = False
        if force_event:
            if not self.events:
                return False
            self._process_next_time(); progressed = True
        while not self._has_feasible_placement() and self.events:
            self._process_next_time(); progressed = True
        return progressed or self._has_feasible_placement() or self._terminated()

    def _process_next_time(self) -> None:
        when = self.events[0][0]
        elapsed = when - self.now
        self.reward_terms["queue"] -= elapsed * sum(job["state"] == "queued" for job in self.jobs.values()) / self.reward_config.time_scale
        spreads = []
        for node in self.nodes:
            free = [1 - node["allocated"][key] / node["capacity"][key] for key in RESOURCE_KEYS if node["capacity"][key]]
            spreads.append(max(free) - min(free))
        self.reward_terms["fragmentation"] -= elapsed * sum(spreads) / len(spreads) / self.reward_config.time_scale
        self.now = when
        while self.events and self.events[0][0] == when:
            _, _, kind, job_id = heapq.heappop(self.events)
            if kind == "submit":
                job = self.pending.pop(job_id); self.jobs[job_id] = job; self.arrivals.append(when)
            elif kind == "complete":
                job = self.jobs[job_id]; node = self._node(job["assigned_node"])
                for key in RESOURCE_KEYS: node["allocated"][key] -= job["request"][key]
                job["state"] = "completed"; job["end_time_seconds"] = when; self.completed += 1
                self.reward_terms["throughput"] += 1
                slowdown = (when - job["submit_time_seconds"]) / max(job["duration_seconds"], 1.0)
                self.reward_terms["slowdown"] -= min(self.reward_config.slowdown_clip, max(1.0, slowdown)) / self.reward_config.slowdown_clip
                if job["deadline_seconds"] is not None and when > job["deadline_seconds"]:
                    self.reward_terms["sla"] -= 1
            else:
                raise EnvironmentError(f"unknown event {kind!r}")

    def _start(self, job_id: str, node_id: str) -> None:
        job, node = self.jobs[job_id], self._node(node_id)
        if job["state"] != "queued" or not _can_fit(job, node):
            raise EnvironmentError("selected placement is no longer feasible")
        for key in RESOURCE_KEYS: node["allocated"][key] += job["request"][key]
        job["state"] = "running"; job["assigned_node"] = node_id; job["start_time_seconds"] = self.now
        self._schedule(self.now + job["duration_seconds"], "complete", job_id)

    def _has_feasible_placement(self) -> bool:
        return any(_can_fit(job, node) for job in self.jobs.values() if job["state"] == "queued" for node in self.nodes)

    def _terminated(self) -> bool:
        return not self.pending and not self.events and all(job["state"] == "completed" for job in self.jobs.values())

    def _observation(self) -> dict[str, Any]:
        cutoff = self.now - self.arrival_window_seconds
        rate = sum(value > cutoff for value in self.arrivals) / self.arrival_window_seconds
        jobs = [{key: job[key] for key in ("id", "submit_time_seconds", "request", "duration_seconds", "priority", "deadline_seconds", "state")} for job in self.jobs.values()]
        return encode(self.policy_config, {"now_seconds": self.now, "arrival_rate": rate, "nodes": self.nodes, "jobs": jobs})

    def _node(self, node_id: str) -> dict[str, Any]:
        return next(node for node in self.nodes if node["id"] == node_id)

    def _info(self) -> dict[str, Any]:
        return {"seed": self.seed, "simulation_time_seconds": self.now, "steps": self.steps, "completed_jobs": self.completed, "pending_events": len(self.events)}


def _validate_synthetic_config(config: SyntheticEpisodeConfig) -> None:
    valid_count = lambda value: not isinstance(value, bool) and isinstance(value, int) and value > 0
    valid_positive = lambda value: not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value > 0
    if not valid_count(config.node_count) or not valid_count(config.job_count) or not valid_positive(config.arrival_rate) or not valid_positive(config.duration_min) or not valid_positive(config.duration_max) or config.duration_max < config.duration_min:
        raise EnvironmentError("invalid synthetic episode configuration")
    for resources in (config.node_capacity, config.request):
        if set(resources) != set(RESOURCE_KEYS) or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in resources.values()):
            raise EnvironmentError("synthetic resources must be non-negative integers")
    if not any(config.node_capacity.values()) or not any(config.request.values()):
        raise EnvironmentError("synthetic resources must be non-zero")


def _stream_seed(seed: int, stream: str) -> int:
    return int.from_bytes(hashlib.sha256(f"{seed}\0{stream}".encode()).digest()[:8], "big")


def _zero_resources() -> dict[str, int]: return {key: 0 for key in RESOURCE_KEYS}
def _can_fit(job: dict[str, Any], node: dict[str, Any]) -> bool: return node["state"] == "available" and all(job["request"][key] <= node["capacity"][key] - node["allocated"][key] for key in RESOURCE_KEYS)
