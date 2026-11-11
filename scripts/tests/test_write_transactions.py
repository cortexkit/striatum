import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD = runpy.run_path(str(ROOT / "scripts/write-transactions.py"))


class WriteTransactionsTests(unittest.TestCase):
    def sites(self, source):
        return GUARD["transaction_sites"](source)

    def test_deferred_methods_and_explicit_constructors_are_found(self):
        for opening in (
            "c.unchecked_transaction()",
            "c.transaction()",
            "c.transaction_with_behavior(TransactionBehavior::Deferred)",
            "Transaction::new(c, TransactionBehavior::Deferred)",
            "rusqlite::Transaction::new_unchecked(c, TransactionBehavior::Immediate)",
            "c.\n unchecked_transaction \n ()",
        ):
            with self.subTest(opening=opening):
                self.assertEqual(self.sites(f"fn write() {{ {opening}; }}"), [("write", 1 if "\n" not in opening else 2)])

    def test_comments_literals_and_inline_tests_are_not_production(self):
        self.assertEqual(self.sites('''
// c.unchecked_transaction()
const EXAMPLE: &str = r#"c.transaction()"#;
#[cfg(test)]
mod tests { fn probe() { c.unchecked_transaction(); } }
#[test]
fn probe() { c.transaction(); }
'''), [])

    def test_test_attributes_do_not_hide_the_next_production_function(self):
        self.assertEqual(self.sites('''
#[cfg(test)]
fn probe() { c.transaction(); }
fn production() { c.unchecked_transaction(); }
'''), [("production", 4)])

    def test_nested_function_is_the_owner(self):
        self.assertEqual(self.sites("fn outer() { fn inner() { c.transaction(); } }"), [("inner", 1)])

    def test_only_helper_and_audited_read_only_sites_are_allowed(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/striatum-store/src"
            source.mkdir(parents=True)
            (source / "transactions.rs").write_text(
                "fn write_transaction(c: &Connection) { Transaction::new_unchecked(c, Immediate); }"
            )
            (source / "lib.rs").write_text("fn read_snapshot() { c.unchecked_transaction(); }")
            site = "crates/striatum-store/src/lib.rs::read_snapshot"
            self.assertEqual(len(GUARD["scan"](root, {})[1]), 1)
            self.assertEqual(GUARD["scan"](root, {site: "Snapshot contains SELECTs only."})[1], [])
            (source / "lib.rs").write_text("fn read_snapshot() {}")
            self.assertEqual(GUARD["scan"](root, {site: "SELECTs only."})[1], [f"stale read-only transaction allowance: {site}"])

    def test_an_allowance_cannot_cover_a_second_opening(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/probe/src/lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text("fn read_snapshot() { c.transaction(); c.transaction(); }")
            _, errors = GUARD["scan"](root, {"crates/probe/src/lib.rs::read_snapshot": "SELECTs only."})
            self.assertEqual(len(errors), 1)

    def test_repository_transaction_policy(self):
        _, errors = GUARD["scan"](ROOT, GUARD["read_allowlist"](GUARD["ALLOWLIST"]))
        self.assertEqual(errors, [])

    def test_a_test_filename_does_not_exempt_production_code(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/probe/src/production_tests.rs"
            source.parent.mkdir(parents=True)
            source.write_text("fn write() { c.unchecked_transaction(); }")
            _, errors = GUARD["scan"](root, {})
            self.assertEqual(len(errors), 1)


if __name__ == "__main__":
    unittest.main()
