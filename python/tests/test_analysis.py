"""Tests for deterministic paired experiment analysis."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scheduler_rl.analysis import AnalysisError, analyze, parse_duration


FIXTURES = Path(__file__).with_name("fixtures")


class AnalysisTest(unittest.TestCase):
    def test_writes_deterministic_golden_report_and_plots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = self._write_matrix(root)
            output = root / "report"

            comparisons = analyze(matrix, output, "first-fit", 200, 7)

            self.assertEqual(len(comparisons), 6)
            self.assertEqual(
                (output / "report.md").read_text(encoding="utf-8"),
                (FIXTURES / "analysis_report.golden.md").read_text(encoding="utf-8"),
            )
            self.assertEqual(len(list((output / "plots").glob("*.svg"))), 6)
            self.assertTrue((output / "comparisons.csv").is_file())
            analysis = json.loads((output / "analysis.json").read_text(encoding="utf-8"))
            self.assertEqual(analysis["bootstrap_samples"], 200)
            self.assertEqual(len(analysis["comparisons"]), 6)

    def test_requires_complete_pairing_and_new_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = self._write_matrix(root)
            document = json.loads(matrix.read_text(encoding="utf-8"))
            removed = document["runs"].pop()
            matrix.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(AnalysisError, "seed set"):
                analyze(matrix, root / "report", "first-fit", 100, 0)

            document["runs"].append(removed)
            matrix.write_text(json.dumps(document), encoding="utf-8")
            (root / "existing").mkdir()
            with self.assertRaisesRegex(AnalysisError, "already exists"):
                analyze(matrix, root / "existing", "first-fit", 100, 0)

    def test_parse_go_durations(self) -> None:
        self.assertAlmostEqual(parse_duration("1h2m3.5s4ms5us6ns"), 3723.504005006)
        self.assertEqual(parse_duration("0s"), 0)
        for invalid in ("", "1", "-1s", "1fortnight"):
            with self.subTest(invalid=invalid), self.assertRaises(AnalysisError):
                parse_duration(invalid)

    def _write_matrix(self, root: Path) -> Path:
        runs = []
        baseline = [(10, "5s", 2, 0.5, 0.8, 0.1), (12, "6s", 3, 0.6, 0.9, 0.2), (11, "4s", 1, 0.55, 0.85, 0.0)]
        candidate = [(11, "4s", 1, 0.6, 0.85, 0.0), (15, "5s", 2, 0.7, 0.95, 0.1), (10, "5s", 2, 0.5, 0.8, 0.1)]
        for policy, rows in (("first-fit", baseline), ("bin-pack", candidate)):
            for seed, row in enumerate(rows, 1):
                bundle = f"smoke/seed-{seed}/{policy}"
                path = root / bundle
                path.mkdir(parents=True, exist_ok=True)
                summary = {
                    "schema_version": "v1", "policy": policy, "seed": seed,
                    "metrics": {
                        "throughput_jobs_per_second": row[0], "latency": {"p99": row[1]},
                        "mean_queue_depth": row[2], "utilization": {"cpu": row[3]},
                        "slowdown_fairness_jain": row[4], "deadline_violation_rate": row[5],
                    },
                }
                (path / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
                runs.append({"workload": "smoke", "config": "smoke.yaml", "policy": policy, "seed": seed, "bundle": bundle})
        matrix = root / "matrix.json"
        matrix.write_text(json.dumps({"schema_version": "v1", "runs": runs}), encoding="utf-8")
        return matrix


if __name__ == "__main__":
    unittest.main()
