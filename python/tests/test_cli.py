"""Smoke tests for the initial Python package entry point."""

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scheduler_rl.cli import main


class CLITest(unittest.TestCase):
    def test_main_prints_development_version(self) -> None:
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            result = main([])

        self.assertEqual(result, 0)
        self.assertEqual(output.getvalue(), "scheduler-rl 0.0.0\n")

    def test_analyze_reports_structured_failure(self) -> None:
        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(error):
            result = main(["analyze", str(Path(directory) / "missing.json"), "--output", str(Path(directory) / "report")])
        self.assertEqual(result, 1)
        self.assertIn("analysis failed:", error.getvalue())


if __name__ == "__main__":
    unittest.main()
