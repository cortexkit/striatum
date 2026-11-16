import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATE = runpy.run_path(str(ROOT / "scripts/source-size.py"))


class SourceSizeGateTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix="source-size-", dir=scratch)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "crates").mkdir()
        (self.root / "scripts").mkdir()
        self.allow("")

    def allow(self, content):
        (self.root / "scripts/source-size.allow").write_text(content)

    def source(self, name, count, prefix=""):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(prefix + "line\n" * (count - len(prefix.splitlines())))

    def check(self):
        return GATE["violations"](self.root)

    def test_new_oversize_source_fails_and_boundary_passes(self):
        self.source("crates/a/src/okay.rs", 5000)
        self.source("crates/a/src/new.rs", 5001)
        errors, notices = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("crates/a/src/new.rs: 5001 lines, limit 5000", errors[0])
        self.assertEqual(notices, [])

    def test_listed_file_cannot_grow(self):
        self.source("crates/a/src/main.rs", 5002)
        self.allow("crates/a/src/main.rs 5001\n")
        errors, notices = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("crates/a/src/main.rs: 5002 lines, limit 5000", errors[0])
        self.assertIn("recorded count 5001", errors[0])
        self.assertEqual(notices, [])

    def test_missing_entry_fails(self):
        self.allow("crates/a/src/missing.rs 5001\n")
        errors, _ = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("crates/a/src/missing.rs: 0 lines, limit 5000", errors[0])
        self.assertIn("missing", errors[0])

    def test_entry_at_or_below_limit_fails(self):
        for count in (5000, 4999):
            with self.subTest(count=count):
                self.source("crates/a/src/small.rs", count)
                self.allow("crates/a/src/small.rs 5001\n")
                errors, _ = self.check()
                self.assertEqual(len(errors), 1)
                self.assertIn(f"crates/a/src/small.rs: {count} lines, limit 5000", errors[0])
                self.assertIn("remove stale", errors[0])

    def test_shrink_above_limit_is_a_notice(self):
        self.source("crates/a/src/main.rs", 5001)
        self.allow("crates/a/src/main.rs 5002\n")
        errors, notices = self.check()
        self.assertEqual(errors, [])
        self.assertEqual(len(notices), 1)
        self.assertIn("crates/a/src/main.rs: 5001 lines, limit 5000", notices[0])
        self.assertIn("lower recorded count from 5002", notices[0])

    def test_exempt_files_and_non_crate_experiment(self):
        for name in ("crates/a/tests/integration.rs", "crates/a/src/tests.rs",
                     "crates/a/src/thing_tests.rs", "crates/a/src/tests/nested.rs",
                     "crates/a/fixtures/sample.rs", "crates/a/src/thing_fixture.rs",
                     "scripts/experiments/large.rs"):
            self.source(name, 5001)
        self.source("crates/a/src/generated.rs", 5001, "// @generated\n")
        self.assertEqual(self.check(), ([], []))

    def test_unterminated_last_line_counts(self):
        path = self.root / "crates/a.rs"
        path.write_text("line\n" * 5000 + "last")
        errors, _ = self.check()
        self.assertIn("crates/a.rs: 5001 lines, limit 5000", errors[0])


if __name__ == "__main__":
    unittest.main()
