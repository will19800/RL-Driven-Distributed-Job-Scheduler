"""Tests for the dependency-free documentation validator."""

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_docs.py"
SPEC = importlib.util.spec_from_file_location("check_docs", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CHECK_DOCS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK_DOCS)


class DocumentationValidationTest(unittest.TestCase):
    def test_valid_local_link(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.md"
            source = root / "source.md"
            target.write_text("# Target\n", encoding="utf-8")
            source.write_text("[target](target.md)\n", encoding="utf-8")

            self.assertEqual(CHECK_DOCS.validate(source), [])

    def test_reports_broken_link_and_trailing_whitespace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.md"
            source.write_text("[missing](nope.md)  \n", encoding="utf-8")

            errors = CHECK_DOCS.validate(source)

            self.assertEqual(len(errors), 2)
            self.assertTrue(any("trailing whitespace" in error for error in errors))
            self.assertTrue(any("local link does not exist" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
