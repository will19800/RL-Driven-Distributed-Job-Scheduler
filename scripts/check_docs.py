"""Validate repository Markdown without third-party dependencies."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
REMOTE_SCHEMES = ("http://", "https://", "mailto:")


def markdown_files() -> list[Path]:
    """Return tracked-source Markdown paths in stable order."""
    return sorted(
        path
        for path in ROOT.rglob("*.md")
        if ".git" not in path.parts and ".cache" not in path.parts
    )


def validate(path: Path) -> list[str]:
    """Return human-readable validation errors for one Markdown file."""
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    try:
        display_path = path.relative_to(ROOT)
    except ValueError:
        display_path = path

    for number, line in enumerate(text.splitlines(), start=1):
        if line.endswith((" ", "\t")):
            errors.append(f"{display_path}:{number}: trailing whitespace")

    if text.count("```") % 2:
        errors.append(f"{display_path}: unclosed fenced code block")

    for match in LINK.finditer(text):
        target = match.group(1).strip().strip("<>")
        if not target or target.startswith(("#", *REMOTE_SCHEMES)):
            continue

        relative_target = unquote(target.split("#", maxsplit=1)[0])
        resolved = (path.parent / relative_target).resolve()
        if not resolved.exists():
            line = text.count("\n", 0, match.start()) + 1
            errors.append(
                f"{display_path}:{line}: local link does not exist: {relative_target}"
            )

    return errors


def main() -> int:
    """Validate all Markdown files and report every error in one run."""
    errors = [error for path in markdown_files() for error in validate(path)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1

    print(f"validated {len(markdown_files())} Markdown files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
