"""Persistent JSON-lines TorchScript worker. Stdout is protocol-only."""

import json
import sys
from pathlib import Path

import torch

from scheduler_rl.training import load_export


def main():
    directory = Path(sys.argv[1])
    metadata = json.loads((directory / "metadata.json").read_text())
    torch.set_num_threads(1)
    model = load_export(directory, metadata["schema"])
    print(json.dumps({"ready": "inference/v1"}), flush=True)
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if set(request) != {"features", "mask"}:
                raise ValueError("invalid request fields")
            features = torch.tensor([request["features"]], dtype=torch.float32)
            mask = torch.tensor([request["mask"]], dtype=torch.bool)
            if not torch.isfinite(features).all():
                raise ValueError("nonfinite features")
            with torch.no_grad():
                logits, _ = model(features, mask)
            print(json.dumps({"logits": logits[0].tolist()}, allow_nan=False), flush=True)
        except (ValueError, RuntimeError, KeyError, TypeError) as error:
            print(json.dumps({"error": str(error)}), flush=True)


if __name__ == "__main__":
    main()
