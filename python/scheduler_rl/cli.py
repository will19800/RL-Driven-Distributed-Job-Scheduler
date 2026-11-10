"""Command-line entry point for scheduler and analysis tooling."""

import argparse
import sys
from pathlib import Path

from scheduler_rl import __version__
from scheduler_rl.analysis import AnalysisError, analyze


def main(argv: list[str] | None = None) -> int:
    """Run the requested scheduler tooling command."""
    parser = argparse.ArgumentParser(prog="scheduler-rl")
    parser.add_argument("--version", action="version", version=f"scheduler-rl {__version__}")
    subcommands = parser.add_subparsers(dest="command")
    analysis = subcommands.add_parser("analyze", help="analyze a completed experiment matrix")
    analysis.add_argument("matrix", type=Path)
    analysis.add_argument("--output", type=Path, required=True)
    analysis.add_argument("--baseline", default="first-fit")
    analysis.add_argument("--bootstrap-samples", type=int, default=10_000)
    analysis.add_argument("--seed", type=int, default=0)
    training = subcommands.add_parser("train", help="train the masked PPO baseline")
    training.add_argument("config", type=Path)
    training.add_argument("--output", type=Path, required=True)
    training.add_argument("--resume", type=Path)
    exporting = subcommands.add_parser("export", help="export a versioned TorchScript policy")
    exporting.add_argument("checkpoint", type=Path)
    exporting.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args(argv)
    if arguments.command is None:
        print(f"scheduler-rl {__version__}")
        return 0
    if arguments.command in ("train", "export"):
        try:
            from scheduler_rl.training import train, export
            if arguments.command == "train":
                train(arguments.config, arguments.output, arguments.resume)
            else:
                export(arguments.checkpoint, arguments.output)
        except (ValueError, OSError, ImportError) as error:
            print(f"{arguments.command} failed: {error}", file=sys.stderr)
            return 1
        print(f"wrote {arguments.command} artifacts to {arguments.output}")
        return 0
    try:
        comparisons = analyze(arguments.matrix, arguments.output, arguments.baseline, arguments.bootstrap_samples, arguments.seed)
    except AnalysisError as error:
        print(f"analysis failed: {error}", file=sys.stderr)
        return 1
    print(f"wrote {len(comparisons)} paired comparisons to {arguments.output}")
    return 0
