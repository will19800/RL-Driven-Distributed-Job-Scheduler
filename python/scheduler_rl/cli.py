"""Command-line entry point for scheduler RL tooling."""

from scheduler_rl import __version__


def main() -> int:
    """Print the package version until training commands are introduced."""
    print(f"scheduler-rl {__version__}")
    return 0
