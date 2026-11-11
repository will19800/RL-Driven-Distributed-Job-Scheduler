"""Reproduce the v0.1 held-out scale experiment and compact evidence package."""

import argparse
import csv
import hashlib
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

from scheduler_rl.analysis import analyze


ROOT = Path(__file__).resolve().parents[1]
POLICIES = ["first-fit", "round-robin", "random-fit", "least-loaded", "bin-pack", "rl"]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def run(args, **kwargs):
    return subprocess.run([str(item) for item in args], cwd=ROOT, check=True, text=True, **kwargs)


def resources(cpu, memory, gpus=0, disk=16384):
    return dict(cpu_millis=cpu, memory_mib=memory, gpus=gpus, ephemeral_disk_mib=disk)


def workload(name, count):
    size = 240 if name == "generalization-240" else 200
    nodes = []
    for index in range(size):
        capacity = resources(4000, 8192)
        if name == "generalization-240":
            capacity = (resources(8000, 8192), resources(2000, 32768), resources(4000, 16384, 2))[index % 3]
        nodes.append({"id": f"node-{index:04d}", "capacity": capacity})
    profiles = [{"name": "balanced", "weight": 1, "resources": resources(2000, 2048, disk=64)}]
    if name == "generalization-240":
        profiles = [
            {"name": "cpu", "weight": 3, "resources": resources(4000, 1024, disk=64)},
            {"name": "memory", "weight": 3, "resources": resources(1000, 16384, disk=64)},
            {"name": "gpu", "weight": 2, "resources": resources(1000, 4096, 1, disk=64)},
        ]
    specification = {"schema_version": "v1", "seed": 2001, "cluster": {"nodes": nodes}, "workload": {"synthetic": {
        "count": count, "start_time": "0s", "arrival_rate_per_second": 60 if name == "generalization-240" else 30,
        "duration": {"median": "10s", "sigma": 0.5, "min": "1s", "max": "1m"}, "profiles": profiles,
    }}}
    if name == "failure-200":
        affected = random.Random(2026).sample(range(size), 50)
        specification["outages"] = [{"node_id": f"node-{index:04d}", "fail_at": "5s", "recover_at": "15s"} for index in sorted(affected)]
    return specification


def source_hashes():
    paths = [path for directory in ("cmd", "internal", "python/scheduler_rl") for path in (ROOT / directory).rglob("*") if path.suffix in (".go", ".py") and not path.name.endswith("_test.go")]
    paths += [ROOT / "go.mod", ROOT / "go.sum", ROOT / "configs/training/smoke.json", Path(__file__).resolve()]
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--python", type=Path, default=Path(".venv/bin/python"))
    parser.add_argument("--seeds", type=int, default=20)
    parser.add_argument("--jobs", type=int, default=600)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--publish", type=Path, help="copy compact evidence to a new repository directory")
    args = parser.parse_args()
    if not 2 <= args.seeds <= 100 or not 100 <= args.jobs <= 10000 or not 1 <= args.workers <= 16:
        parser.error("use 2..100 seeds, 100..10000 jobs, and 1..16 workers")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    # Preserve the virtual-environment launcher path rather than following its
    # symlink to the base interpreter (which would lose the training packages).
    interpreter = Path(os.path.abspath(args.python))
    os.environ["PYTHONPATH"] = str(ROOT / "python")
    started = time.monotonic()
    frozen_sources = source_hashes()
    print("Training and freezing the smoke policy before held-out evaluation", flush=True)
    run([interpreter, "-m", "scheduler_rl", "train", "configs/training/smoke.json", "--output", output / "training"])
    run([interpreter, "-m", "scheduler_rl", "export", output / "training/best.pt", "--output", output / "model"])
    names = ["steady-200", "generalization-240", "failure-200"]
    configs = output / "configs"
    configs.mkdir()
    for name in names:
        write_json(configs / f"{name}.json", workload(name, args.jobs))
    specification = {"schema_version": "v1", "output_dir": str(output / "matrix"), "workers": args.workers, "seeds": list(range(2001, 2001 + args.seeds)), "policies": POLICIES,
                     "inference": {"model": str(output / "model"), "python": str(interpreter), "timeout": "250ms", "fallback": "bin-pack"},
                     "workloads": [{"name": name, "config": str(configs / f"{name}.json")} for name in names]}
    write_json(output / "matrix.json", specification)
    print(f"Evaluating {len(names) * args.seeds * len(POLICIES)} held-out runs", flush=True)
    run([ROOT / "bin/experiment-run", output / "matrix.json"])
    matrix = json.loads((output / "matrix/matrix.json").read_text())
    manifests, rows = [], []
    for cell in matrix["runs"]:
        bundle = output / "matrix" / cell["bundle"]
        run([ROOT / "bin/result-check", bundle], stdout=subprocess.DEVNULL)
        manifest = json.loads((bundle / "manifest.json").read_text())
        summary = json.loads((bundle / "summary.json").read_text())
        manifests.append({"workload": cell["workload"], "manifest": manifest})
        scheduler = summary["metrics"]["scheduler"]
        inference = summary.get("inference", {})
        rows.append({"workload": cell["workload"], "policy": cell["policy"], "seed": cell["seed"], "nodes": summary["node_count"], **summary["jobs"],
                     "throughput": summary["metrics"]["throughput_jobs_per_second"], "p99": summary["metrics"]["latency"]["p99"],
                     "cpu_utilization": summary["metrics"]["utilization"]["cpu"], "mean_decision_time": scheduler["mean_duration"],
                     "invalid_decisions": scheduler["invalid_decisions"], "policy_errors": scheduler["policy_errors"],
                     "fallbacks": inference.get("fallbacks", 0), "model_calls": inference.get("model_calls", 0), "decisions": scheduler["decisions"],
                     "fallback_reasons": json.dumps(inference.get("reasons", {}), sort_keys=True)})
    evidence = output / "evidence"
    analyze(output / "matrix/matrix.json", evidence, "bin-pack", 10000, 2026)
    write_json(evidence / "manifests.json", manifests)
    with (evidence / "runs.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    shutil.copytree(configs, evidence / "configs")
    shutil.copy2(output / "training/training.json", evidence / "training.json")
    shutil.copy2(output / "model/metadata.json", evidence / "model-metadata.json")
    print("Profiling one separate baseline run", flush=True)
    run([ROOT / "bin/scheduler-sim", "run", "--config", configs / "steady-200.json", "--policy", "bin-pack", "--cpu-profile", output / "baseline.cpu"], stdout=subprocess.DEVNULL)
    profile = run(["go", "tool", "pprof", "-top", "-nodecount=12", ROOT / "bin/scheduler-sim", output / "baseline.cpu"], capture_output=True)
    (evidence / "profile.txt").write_text(profile.stdout)
    identity = {"format": "benchmark/v1", "platform": platform.platform(), "machine": platform.machine(), "python": platform.python_version(),
                "processor": platform.processor(), "cpu_count": os.cpu_count(), "seeds": specification["seeds"], "jobs_per_run": args.jobs, "workers": args.workers,
                "policies": POLICIES, "workloads": names, "base_git_revision": run(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
                "source_sha256": frozen_sources, "binary_sha256": hashlib.sha256((ROOT / "bin/scheduler-sim").read_bytes()).hexdigest(),
                "elapsed_seconds": time.monotonic() - started, "run_count": len(rows), "bootstrap_samples": 10000, "bootstrap_seed": 2026}
    write_json(evidence / "provenance.json", identity)
    if source_hashes() != frozen_sources:
        raise RuntimeError("production sources changed during the benchmark; evidence will not be published")
    print(json.dumps({"evidence": str(evidence), "runs": len(rows), "invalid_decisions": sum(row["invalid_decisions"] for row in rows), "fallbacks": sum(row["fallbacks"] for row in rows)}, indent=2), flush=True)
    if args.publish:
        shutil.copytree(evidence, args.publish)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
