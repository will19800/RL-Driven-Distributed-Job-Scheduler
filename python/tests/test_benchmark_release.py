"""The scale suite must remain feasible and deterministic before publication."""
import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/benchmark_release.py"
SPEC = importlib.util.spec_from_file_location("benchmark_release", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class BenchmarkSuiteTest(unittest.TestCase):
    def test_scale_profiles_and_seeded_outages(self):
        for name, size in (("steady-200", 200), ("generalization-240", 240), ("failure-200", 200)):
            with self.subTest(name=name):
                config = MODULE.workload(name, 600)
                self.assertEqual(config, MODULE.workload(name, 600))
                nodes = config["cluster"]["nodes"]
                self.assertEqual(len(nodes), size)
                for profile in config["workload"]["synthetic"]["profiles"]:
                    self.assertTrue(any(all(profile["resources"][key] <= node["capacity"][key] for key in profile["resources"]) for node in nodes))
                if name == "failure-200":
                    self.assertEqual(len(config["outages"]), 50)
                    self.assertEqual(len({item["node_id"] for item in config["outages"]}), 50)
