"""Deterministic paired statistical analysis for experiment matrices."""

from __future__ import annotations

import csv
import hashlib
import html
import json
import math
import random
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "v1"
_DURATION_TOKEN = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
_DURATION_SCALE = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


class AnalysisError(ValueError):
    """Raised when matrix data cannot support a valid paired analysis."""


@dataclass(frozen=True)
class Metric:
    key: str
    label: str
    higher_is_better: bool


METRICS = (
    Metric("throughput_jobs_per_second", "Throughput (jobs/s)", True),
    Metric("p99_latency_seconds", "P99 latency (seconds)", False),
    Metric("mean_queue_depth", "Mean queue depth", False),
    Metric("cpu_utilization", "CPU utilization", True),
    Metric("slowdown_fairness_jain", "Slowdown fairness (Jain)", True),
    Metric("deadline_violation_rate", "Deadline violation rate", False),
)


@dataclass(frozen=True)
class Comparison:
    workload: str
    baseline: str
    policy: str
    metric: str
    metric_label: str
    higher_is_better: bool
    pairs: int
    baseline_mean: float
    policy_mean: float
    mean_difference: float
    mean_improvement: float
    ci95_low: float
    ci95_high: float


def analyze(matrix_path: Path, output_dir: Path, baseline: str, bootstrap_samples: int = 10_000, seed: int = 0) -> list[Comparison]:
    """Validate a matrix, compute paired comparisons, and write a report bundle."""
    if bootstrap_samples < 100 or bootstrap_samples > 1_000_000:
        raise AnalysisError("bootstrap_samples must be between 100 and 1000000")
    if output_dir.exists():
        raise AnalysisError(f"output directory already exists: {output_dir}")
    matrix = _read_json(matrix_path)
    if set(matrix) != {"schema_version", "runs"} or matrix["schema_version"] != SCHEMA_VERSION or not isinstance(matrix["runs"], list):
        raise AnalysisError("matrix.json must contain schema_version v1 and runs")
    values, policies = _load_values(matrix_path.parent, matrix["runs"])
    if baseline not in policies:
        raise AnalysisError(f"baseline policy {baseline!r} is not present")
    comparisons = _compare(values, policies, baseline, bootstrap_samples, seed)
    if not comparisons:
        raise AnalysisError("matrix needs at least one non-baseline policy")
    output_dir.mkdir(parents=True)
    (output_dir / "plots").mkdir()
    _write_json(output_dir / "analysis.json", {"schema_version": SCHEMA_VERSION, "baseline": baseline, "bootstrap_samples": bootstrap_samples, "bootstrap_seed": seed, "comparisons": [asdict(item) for item in comparisons]})
    _write_csv(output_dir / "comparisons.csv", comparisons)
    (output_dir / "report.md").write_text(_markdown(comparisons, baseline, bootstrap_samples, seed), encoding="utf-8")
    for metric in METRICS:
        selected = [item for item in comparisons if item.metric == metric.key]
        (output_dir / "plots" / f"{metric.key}.svg").write_text(_svg(metric, selected), encoding="utf-8")
    return comparisons


def _load_values(root: Path, runs: list[Any]) -> tuple[dict[tuple[str, str, int], dict[str, float]], set[str]]:
    values: dict[tuple[str, str, int], dict[str, float]] = {}
    policies: set[str] = set()
    required = {"workload", "config", "policy", "seed", "bundle"}
    for index, run in enumerate(runs):
        if not isinstance(run, dict) or set(run) != required:
            raise AnalysisError(f"run {index} has an invalid schema")
        workload, policy, seed, bundle = run["workload"], run["policy"], run["seed"], run["bundle"]
        if not isinstance(workload, str) or not isinstance(policy, str) or not isinstance(seed, int) or not isinstance(bundle, str):
            raise AnalysisError(f"run {index} has invalid field types")
        bundle_path = (root / bundle).resolve()
        try:
            bundle_path.relative_to(root.resolve())
        except ValueError as error:
            raise AnalysisError(f"run {index} bundle escapes the matrix directory") from error
        summary = _read_json(bundle_path / "summary.json")
        if summary.get("schema_version") != SCHEMA_VERSION or summary.get("policy") != policy or summary.get("seed") != seed:
            raise AnalysisError(f"run {index} summary identity does not match matrix")
        key = (workload, policy, seed)
        if key in values:
            raise AnalysisError(f"duplicate matrix cell {key}")
        values[key] = _extract_metrics(summary, index)
        policies.add(policy)
    return values, policies


def _extract_metrics(summary: dict[str, Any], index: int) -> dict[str, float]:
    try:
        metrics = summary["metrics"]
        raw = {
            "throughput_jobs_per_second": metrics["throughput_jobs_per_second"],
            "p99_latency_seconds": parse_duration(metrics["latency"]["p99"]),
            "mean_queue_depth": metrics["mean_queue_depth"],
            "cpu_utilization": metrics["utilization"]["cpu"],
            "slowdown_fairness_jain": metrics["slowdown_fairness_jain"],
            "deadline_violation_rate": metrics["deadline_violation_rate"],
        }
    except (KeyError, TypeError) as error:
        raise AnalysisError(f"run {index} summary is missing metrics") from error
    output: dict[str, float] = {}
    for key, value in raw.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise AnalysisError(f"run {index} metric {key} must be finite")
        output[key] = float(value)
    return output


def parse_duration(value: Any) -> float:
    """Parse the non-negative Go duration forms emitted in summaries."""
    if not isinstance(value, str) or not value or value.startswith("-"):
        raise AnalysisError(f"invalid duration {value!r}")
    position, total = 0, 0.0
    for match in _DURATION_TOKEN.finditer(value):
        if match.start() != position:
            raise AnalysisError(f"invalid duration {value!r}")
        total += float(match.group(1)) * _DURATION_SCALE[match.group(2)]
        position = match.end()
    if position != len(value):
        raise AnalysisError(f"invalid duration {value!r}")
    return total


def _compare(values: dict[tuple[str, str, int], dict[str, float]], policies: set[str], baseline: str, samples: int, seed: int) -> list[Comparison]:
    workloads = sorted({key[0] for key in values})
    output: list[Comparison] = []
    for workload in workloads:
        baseline_seeds = {key[2] for key in values if key[0] == workload and key[1] == baseline}
        if not baseline_seeds:
            raise AnalysisError(f"workload {workload!r} has no baseline runs")
        for policy in sorted(policies - {baseline}):
            policy_seeds = {key[2] for key in values if key[0] == workload and key[1] == policy}
            if policy_seeds != baseline_seeds:
                raise AnalysisError(f"workload {workload!r} policy {policy!r} does not have the baseline seed set")
            seeds = sorted(baseline_seeds)
            for metric in METRICS:
                base = [values[(workload, baseline, item)][metric.key] for item in seeds]
                candidate = [values[(workload, policy, item)][metric.key] for item in seeds]
                differences = [right - left for left, right in zip(base, candidate, strict=True)]
                direction = 1.0 if metric.higher_is_better else -1.0
                improvements = [direction * item for item in differences]
                low, high = _bootstrap_ci(improvements, samples, _derived_seed(seed, workload, policy, metric.key))
                output.append(Comparison(workload, baseline, policy, metric.key, metric.label, metric.higher_is_better, len(seeds), _mean(base), _mean(candidate), _mean(differences), _mean(improvements), low, high))
    return output


def _bootstrap_ci(values: list[float], samples: int, seed: int) -> tuple[float, float]:
    rng = random.Random(seed)
    estimates = sorted(_mean([values[rng.randrange(len(values))] for _ in values]) for _ in range(samples))
    return _quantile(estimates, 0.025), _quantile(estimates, 0.975)


def _derived_seed(seed: int, *parts: str) -> int:
    digest = hashlib.sha256("\0".join((str(seed), *parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _mean(values: list[float]) -> float:
    return math.fsum(values) / len(values)


def _quantile(sorted_values: list[float], probability: float) -> float:
    position = (len(sorted_values) - 1) * probability
    lower = math.floor(position); upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    return sorted_values[lower] * (upper - position) + sorted_values[upper] * (position - lower)


def _markdown(comparisons: list[Comparison], baseline: str, samples: int, seed: int) -> str:
    lines = ["# Paired scheduler comparison", "", f"Baseline: `{baseline}`. Bootstrap samples: {samples}. Bootstrap seed: {seed}.", "", "Positive improvement favors the compared policy; latency, queue depth, and deadline violations are direction-adjusted.", "", "| Workload | Policy | Metric | Pairs | Baseline | Policy | Improvement | 95% CI |", "|---|---|---|---:|---:|---:|---:|---:|"]
    for item in comparisons:
        lines.append(f"| {item.workload} | {item.policy} | {item.metric_label} | {item.pairs} | {_number(item.baseline_mean)} | {_number(item.policy_mean)} | {_number(item.mean_improvement)} | [{_number(item.ci95_low)}, {_number(item.ci95_high)}] |")
    lines.extend(("", "Intervals are percentile confidence intervals from paired seed resampling. They describe uncertainty across the supplied seeds and should not be treated as evidence when the matrix is only a smoke fixture.", ""))
    return "\n".join(lines)


def _write_csv(path: Path, comparisons: list[Comparison]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(asdict(comparisons[0])))
        writer.writeheader(); writer.writerows(asdict(item) for item in comparisons)


def _svg(metric: Metric, comparisons: list[Comparison]) -> str:
    width, left, right, row = 900, 270, 30, 34
    height = 85 + row * len(comparisons)
    extent = max([abs(value) for item in comparisons for value in (item.ci95_low, item.ci95_high)] + [1e-12])
    scale = (width - left - right) / (2 * extent); zero = left + (width - left - right) / 2
    elements = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">', '<style>text{font:13px sans-serif;fill:#222}.title{font-size:17px;font-weight:600}.axis{stroke:#888}.ci{stroke:#2463a6;stroke-width:3}.point{fill:#2463a6}</style>', f'<text class="title" x="12" y="24">{html.escape(metric.label)}: paired improvement with 95% CI</text>', f'<line class="axis" x1="{zero:.2f}" y1="42" x2="{zero:.2f}" y2="{height-25}"/>']
    for index, item in enumerate(comparisons):
        y = 62 + index * row; label = html.escape(f"{item.workload} / {item.policy}")
        low, high, point = zero + item.ci95_low * scale, zero + item.ci95_high * scale, zero + item.mean_improvement * scale
        elements.extend((f'<text x="12" y="{y+4}">{label}</text>', f'<line class="ci" x1="{low:.2f}" y1="{y}" x2="{high:.2f}" y2="{y}"/>', f'<circle class="point" cx="{point:.2f}" cy="{y}" r="5"/>'))
    elements.extend((f'<text x="{left}" y="{height-7}">{_number(-extent)}</text>', f'<text x="{width-right-50}" y="{height-7}">{_number(extent)}</text>', "</svg>", ""))
    return "\n".join(elements)


def _number(value: float) -> str:
    return f"{value:.6g}"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AnalysisError(f"read {path}: {error}") from error
    if not isinstance(value, dict):
        raise AnalysisError(f"{path} must contain a JSON object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
