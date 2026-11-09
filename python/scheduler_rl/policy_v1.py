"""Pure-Python implementation of the language-neutral policy/v1 contract."""

from __future__ import annotations

import math
from typing import Any


VERSION = "policy/v1"
NODE_FEATURES = ["free_cpu", "free_memory", "free_gpu", "free_disk", "total_cpu", "total_memory", "total_gpu", "total_disk", "available", "utilization_cpu", "utilization_memory", "utilization_gpu", "utilization_disk"]
JOB_FEATURES = ["request_cpu", "request_memory", "request_gpu", "request_disk", "age", "duration", "priority", "deadline_slack", "has_deadline"]
GLOBAL_FEATURES = ["queue_length", "utilization_cpu", "utilization_memory", "utilization_gpu", "utilization_disk", "arrival_rate", "simulation_time"]
RESOURCE_KEYS = ("cpu_millis", "memory_mib", "gpus", "ephemeral_disk_mib")
NORMALIZATION_KEYS = (*RESOURCE_KEYS, "duration_seconds", "priority", "queue_length", "arrival_rate", "simulation_time_seconds")


class ContractError(ValueError):
    """Raised for an invalid policy/v1 schema, input, or action."""


def schema(config: dict[str, Any]) -> dict[str, Any]:
    """Return the canonical schema metadata for a validated config."""
    max_jobs, max_nodes, normalization = _validate_config(config)
    count = max_jobs * max_nodes
    return {"version": VERSION, "max_jobs": max_jobs, "max_nodes": max_nodes, "node_features": NODE_FEATURES, "job_features": JOB_FEATURES, "global_features": GLOBAL_FEATURES, "normalization": normalization, "placement_action_count": count, "defer_action": count}


def encode(config: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    """Encode a state dictionary into fixed-size policy/v1 tensors."""
    max_jobs, max_nodes, norm = _validate_config(config)
    now = _finite_nonnegative(state.get("now_seconds"), "now_seconds")
    arrival_rate = _finite_nonnegative(state.get("arrival_rate"), "arrival_rate")
    raw_nodes, raw_jobs = state.get("nodes"), state.get("jobs")
    if not isinstance(raw_nodes, list) or not isinstance(raw_jobs, list):
        raise ContractError("nodes and jobs must be lists")
    nodes = sorted((_validate_node(item) for item in raw_nodes), key=lambda item: item["id"])
    ordered_jobs = sorted((_validate_job(item) for item in raw_jobs), key=lambda item: (-item["priority"], item["submit_time_seconds"], item["id"]))
    if len({item["id"] for item in nodes}) != len(nodes) or len({item["id"] for item in ordered_jobs}) != len(ordered_jobs):
        raise ContractError("node and job IDs must be unique")
    queued = [item for item in ordered_jobs if item["state"] == "queued" and item["submit_time_seconds"] <= now]
    candidates_nodes, candidates_jobs = nodes[:max_nodes], queued[:max_jobs]
    node_features = [[0.0] * len(NODE_FEATURES) for _ in range(max_nodes)]
    job_features = [[0.0] * len(JOB_FEATURES) for _ in range(max_jobs)]
    node_mask, job_mask = [False] * max_nodes, [False] * max_jobs
    node_ids, job_ids = [""] * max_nodes, [""] * max_jobs
    for index, node in enumerate(candidates_nodes):
        node_features[index] = _node_features(norm, node); node_mask[index] = True; node_ids[index] = node["id"]
    for index, job in enumerate(candidates_jobs):
        job_features[index] = _job_features(norm, now, job); job_mask[index] = True; job_ids[index] = job["id"]
    action_mask = [False] * (max_jobs * max_nodes + 1)
    for job_index, job in enumerate(candidates_jobs):
        for node_index, node in enumerate(candidates_nodes):
            action_mask[job_index * max_nodes + node_index] = _can_fit(job, node)
    action_mask[-1] = True
    return {"version": VERSION, "node_features": node_features, "job_features": job_features, "global_features": _global_features(norm, now, arrival_rate, queued, nodes), "node_mask": node_mask, "job_mask": job_mask, "action_mask": action_mask, "node_ids": node_ids, "job_ids": job_ids}


def decode_action(config: dict[str, Any], observation: dict[str, Any], index: int) -> dict[str, Any]:
    """Decode a feasible row-major action or the explicit defer action."""
    max_jobs, max_nodes, _ = _validate_config(config)
    count = max_jobs * max_nodes
    mask = observation.get("action_mask")
    if observation.get("version") != VERSION or not isinstance(mask, list) or len(mask) != count + 1:
        raise ContractError("observation is incompatible")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0 or index > count:
        raise ContractError(f"action {index!r} is out of range")
    if not mask[index]:
        raise ContractError(f"action {index} is masked")
    if index == count:
        return {"defer": True, "job_index": -1, "node_index": -1}
    job_index, node_index = divmod(index, max_nodes)
    return {"defer": False, "job_index": job_index, "node_index": node_index}


def _validate_config(config: Any) -> tuple[int, int, dict[str, float]]:
    if not isinstance(config, dict) or set(config) != {"max_jobs", "max_nodes", "normalization"}:
        raise ContractError("config fields must be max_jobs, max_nodes, and normalization")
    max_jobs, max_nodes, normalization = config["max_jobs"], config["max_nodes"], config["normalization"]
    if isinstance(max_jobs, bool) or isinstance(max_nodes, bool) or not isinstance(max_jobs, int) or not isinstance(max_nodes, int) or max_jobs <= 0 or max_nodes <= 0 or max_jobs > 10_000 or max_nodes > 10_000 or max_jobs * max_nodes > 1_000_000:
        raise ContractError("candidate dimensions are invalid")
    if not isinstance(normalization, dict) or set(normalization) != set(NORMALIZATION_KEYS):
        raise ContractError("normalization fields are invalid")
    normalized = {key: _finite_positive(normalization[key], f"normalization.{key}") for key in NORMALIZATION_KEYS}
    return max_jobs, max_nodes, normalized


def _validate_node(value: Any) -> dict[str, Any]:
    required = {"id", "capacity", "allocated", "state"}
    if not isinstance(value, dict) or set(value) != required or not isinstance(value["id"], str) or not value["id"] or value["state"] not in ("available", "unavailable"):
        raise ContractError("invalid node")
    capacity, allocated = _resources(value["capacity"]), _resources(value["allocated"])
    if not any(capacity.values()) or any(allocated[key] > capacity[key] for key in RESOURCE_KEYS):
        raise ContractError(f"invalid node resources for {value['id']!r}")
    return {"id": value["id"], "capacity": capacity, "allocated": allocated, "state": value["state"]}


def _validate_job(value: Any) -> dict[str, Any]:
    required = {"id", "submit_time_seconds", "request", "duration_seconds", "priority", "deadline_seconds", "state"}
    if not isinstance(value, dict) or set(value) != required or not isinstance(value["id"], str) or not value["id"] or value["state"] not in ("queued", "running", "completed", "rejected"):
        raise ContractError("invalid job")
    submit = _finite_nonnegative(value["submit_time_seconds"], "submit_time_seconds")
    duration, priority = _finite_positive(value["duration_seconds"], "duration_seconds"), _integer_nonnegative(value["priority"], "priority")
    if priority > 2_147_483_647: raise ContractError("priority exceeds int32")
    request = _resources(value["request"])
    if not any(request.values()): raise ContractError("job request must be non-zero")
    deadline = value["deadline_seconds"]
    if deadline is not None:
        deadline = _finite_nonnegative(deadline, "deadline_seconds")
        if deadline < submit: raise ContractError("deadline precedes submission")
    return {"id": value["id"], "submit_time_seconds": submit, "request": request, "duration_seconds": duration, "priority": priority, "deadline_seconds": deadline, "state": value["state"]}


def _resources(value: Any) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(RESOURCE_KEYS): raise ContractError("invalid resource fields")
    return {key: _integer_nonnegative(value[key], key) for key in RESOURCE_KEYS}


def _node_features(norm: dict[str, float], node: dict[str, Any]) -> list[float]:
    capacity, allocated = node["capacity"], node["allocated"]
    free = {key: capacity[key] - allocated[key] for key in RESOURCE_KEYS}
    return [_ratio(free[key], norm[key]) for key in RESOURCE_KEYS] + [_ratio(capacity[key], norm[key]) for key in RESOURCE_KEYS] + [1.0 if node["state"] == "available" else 0.0] + [_resource_ratio(allocated[key], capacity[key]) for key in RESOURCE_KEYS]


def _job_features(norm: dict[str, float], now: float, job: dict[str, Any]) -> list[float]:
    deadline = job["deadline_seconds"]
    return [_ratio(job["request"][key], norm[key]) for key in RESOURCE_KEYS] + [_ratio(max(0.0, now - job["submit_time_seconds"]), norm["duration_seconds"]), _ratio(job["duration_seconds"], norm["duration_seconds"]), _ratio(job["priority"], norm["priority"]), 0.0 if deadline is None else _signed_ratio(deadline - now, norm["duration_seconds"]), 0.0 if deadline is None else 1.0]


def _global_features(norm: dict[str, float], now: float, arrival_rate: float, queued: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> list[float]:
    allocated = {key: math.fsum(node["allocated"][key] for node in nodes) for key in RESOURCE_KEYS}
    capacity = {key: math.fsum(node["capacity"][key] for node in nodes) for key in RESOURCE_KEYS}
    return [_ratio(len(queued), norm["queue_length"])] + [_resource_ratio(allocated[key], capacity[key]) for key in RESOURCE_KEYS] + [_ratio(arrival_rate, norm["arrival_rate"]), _ratio(now, norm["simulation_time_seconds"])]


def _can_fit(job: dict[str, Any], node: dict[str, Any]) -> bool:
    return node["state"] == "available" and all(job["request"][key] <= node["capacity"][key] - node["allocated"][key] for key in RESOURCE_KEYS)


def _finite_nonnegative(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0: raise ContractError(f"{name} must be non-negative and finite")
    return float(value)


def _finite_positive(value: Any, name: str) -> float:
    result = _finite_nonnegative(value, name)
    if result == 0: raise ContractError(f"{name} must be positive")
    return result


def _integer_nonnegative(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0: raise ContractError(f"{name} must be a non-negative integer")
    return value


def _ratio(value: float, maximum: float) -> float: return max(0.0, min(1.0, value / maximum))
def _signed_ratio(value: float, maximum: float) -> float: return max(-1.0, min(1.0, value / maximum))
def _resource_ratio(value: float, maximum: float) -> float: return 0.0 if maximum == 0 else _ratio(value, maximum)
