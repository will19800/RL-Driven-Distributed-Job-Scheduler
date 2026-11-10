"""Optional training tests: actual PPO updates, resume parity, and export."""
import importlib.util
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("torch"), "install training extra")
class TrainingTest(unittest.TestCase):
    def test_train_resume_export_and_tamper_detection(self):
        import torch
        from scheduler_rl.training import train, export, load_export
        config = Path(__file__).resolve().parents[2] / "configs/training/smoke.json"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            train(config, root / "first")
            train(config, root / "resumed", root / "first/update-1.pt")
            first = torch.load(root / "first/last.pt", weights_only=True)
            resumed = torch.load(root / "resumed/last.pt", weights_only=True)
            earlier = torch.load(root / "first/update-1.pt", weights_only=True)
            self.assertTrue(any(not torch.equal(weights, earlier["model"][name]) for name, weights in first["model"].items()))
            for name, weights in first["model"].items():
                self.assertTrue(torch.equal(weights, resumed["model"][name]), name)
            metadata = export(root / "first/best.pt", root / "export")
            load_export(root / "export", metadata["schema"])
            with self.assertRaisesRegex(ValueError, "incompatible"):
                load_export(root / "export", {})
            with self.assertRaisesRegex(ValueError, "already exists"):
                export(root / "first/best.pt", root / "export")
            with (root / "export/policy.ts").open("ab") as artifact:
                artifact.write(b"tampered")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                load_export(root / "export", metadata["schema"])
