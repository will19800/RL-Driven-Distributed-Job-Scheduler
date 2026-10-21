"""Smoke tests for the initial Python package entry point."""

import contextlib
import io
import unittest

from scheduler_rl.cli import main


class CLITest(unittest.TestCase):
    def test_main_prints_development_version(self) -> None:
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            result = main()

        self.assertEqual(result, 0)
        self.assertEqual(output.getvalue(), "scheduler-rl 0.0.0\n")


if __name__ == "__main__":
    unittest.main()
