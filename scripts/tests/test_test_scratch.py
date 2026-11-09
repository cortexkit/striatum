import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATE = runpy.run_path(str(ROOT / "scripts/test-scratch.py"))


class TestScratchGate(unittest.TestCase):
    def check_source(self, source, minimum_files=1):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="scratch-gate-", dir=scratch) as directory:
            root = Path(directory)
            (root / "crates").mkdir()
            (root / "crates/probe.rs").write_text(source)
            return GATE["violations"](root, minimum_files=minimum_files)

    def test_refuses_raw_constructors(self):
        for call in ["tempfile::Builder::new().tempdir()", "TempDir::new()", "tempfile::tempdir()", "std::env::temp_dir()", "env::temp_dir()", 'std::env::var_os("TMPDIR")', 'var_os("TMPDIR")', 'std::env::var("TEMP")', "temp_dir()", "tempdir()", "x.\n tempdir ()"]:
            with self.subTest(call=call):
                errors = self.check_source(f"fn scratch() {{\n {call};\n}}")
                self.assertEqual(len(errors), 1)
                self.assertIn("crates/probe.rs:2: scratch:", errors[0])

    def test_accepts_relocated_constructor_and_ignores_comments_and_literals(self):
        self.assertEqual(self.check_source('''
fn scratch() {
    // TempDir::new()
    let example = r#"std::env::temp_dir()"#;
    /* .tempdir() */
    tempfile::Builder::new().prefix("suite-").tempdir_in(scratch_root());
}
'''), [])

    def test_does_not_exempt_same_symbol_in_another_file(self):
        self.assertEqual(len(self.check_source("fn scratch_root() { std::env::temp_dir(); }")), 1)

    def test_production_default_refuses_one_file_tree(self):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="scratch-gate-", dir=scratch) as directory:
            root = Path(directory)
            (root / "crates").mkdir()
            (root / "crates/probe.rs").write_text("fn scratch() {}")
            errors = GATE["violations"](root)

        self.assertIn("tree scan read 1 Rust files; expected at least 180", errors)


if __name__ == "__main__":
    unittest.main()
