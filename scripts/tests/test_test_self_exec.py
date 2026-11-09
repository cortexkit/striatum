"""Fixtures for the raw current_exe source guard."""

import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD = runpy.run_path(str(ROOT / "scripts/test-self-exec.py"))


class TestSelfExecTests(unittest.TestCase):
    def test_rust_gate_always_checks_working_tree_sources(self):
        gate = (ROOT / "scripts/rust-test-gate.sh").read_text()
        self.assertIn('python3 -m unittest scripts/tests/test_test_self_exec.py', gate)
        self.assertIn('python3 scripts/test-self-exec.py', gate)

    def scan(self, source, *, relative="crates/probe/src/lib.rs", allowed=None):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="self-exec-", dir=scratch) as directory:
            root = Path(directory)
            path = root / relative
            path.parent.mkdir(parents=True)
            path.write_text(source)
            return GUARD["scan"](root, {} if allowed is None else allowed)

    def test_direct_self_spawn_bypassing_helper_is_rejected(self):
        count, errors = self.scan('''
#[cfg(test)]
mod tests {
    #[test]
    fn isolated_process() {
        std::process::Command::new(std::env::current_exe().unwrap()).status().unwrap();
    }
}
''')
        self.assertEqual(count, 1)
        self.assertEqual(len(errors), 1)
        self.assertIn("isolated_process", errors[0])
        self.assertIn("crate::test_exe::current_test_exe()", errors[0])

    def test_stored_multiline_executable_is_rejected_in_integration_tests(self):
        _, errors = self.scan('''
fn isolated_process() {
    let exe = std::env::
        current_exe().unwrap();
    std::process::Command::new(exe).spawn().unwrap();
}
''', relative="crates/probe/tests/it/probe.rs")
        self.assertEqual(len(errors), 1)

    def test_import_alias_and_function_pointer_cannot_bypass_guard(self):
        for source in (
            "use std::env::current_exe as executable;",
            "fn isolated_process() { let executable = std::env::current_exe; }",
        ):
            with self.subTest(source=source):
                self.assertEqual(len(self.scan(source)[1]), 1)

    def test_helper_and_non_code_references_are_allowed(self):
        _, errors = self.scan('''
// std::env::current_exe()
/* nested /* current_exe */ comment */
const EXAMPLE: &str = r#"std::env::current_exe()"#;
fn isolated_process() {
    std::process::Command::new(crate::test_exe::current_test_exe().unwrap());
}
''')
        self.assertEqual(errors, [])

    def test_reviewed_production_lookup_is_allowed_but_not_an_extra_spawn(self):
        source = "fn locate_module() { let exe = std::env::current_exe().unwrap(); }"
        allowed = {("crates/probe/src/lib.rs", "locate_module"): 1}
        self.assertEqual(self.scan(source, allowed=allowed)[1], [])
        extra = source.replace(" }", " std::process::Command::new(std::env::current_exe().unwrap()); }")
        self.assertEqual(len(self.scan(extra, allowed=allowed)[1]), 1)

    def test_allowance_does_not_cover_other_functions_or_files(self):
        allowed = {("crates/probe/src/lib.rs", "locate_module"): 1}
        _, errors = self.scan("fn self_spawn() { std::env::current_exe().unwrap(); }", allowed=allowed)
        self.assertEqual(len(errors), 2)
        self.assertIn("self_spawn", errors[0])
        self.assertIn("stale", errors[1])

    def test_allowlist_requires_unique_reasoned_positive_counts(self):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="self-exec-allow-", dir=scratch) as directory:
            path = Path(directory) / "guard.allow"
            for invalid in ("p\tf\n", "p\tf\t0\treason\n", "p\tf\t1\t\n", "p\tf\t1\treason\np\tf\t1\treason\n"):
                with self.subTest(invalid=invalid):
                    path.write_text(invalid)
                    with self.assertRaises(ValueError):
                        GUARD["read_allowlist"](path)
            path.write_text("# reviewed uses\np\tf\t1\tLocates a sibling executable\n")
            self.assertEqual(GUARD["read_allowlist"](path), {("p", "f"): 1})


if __name__ == "__main__":
    unittest.main()
