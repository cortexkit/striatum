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

    def test_account_entry_points_require_the_adapter(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/striatum-module/src"
            source.mkdir(parents=True)
            main = source / "main.rs"
            main.write_text("mod account_writer; fn handler() { account_writer::record(store); }")
            (source / "account_writer.rs").write_text("fn record() { store.record_account_eligibility(fact); }")
            entries = {"record_account_eligibility"}
            self.assertEqual(GUARD["account_writer_violations"](root, entries), [])
            main.write_text("fn handler() { arbitrary_name.\nrecord_account_eligibility(fact); }")
            errors = GUARD["account_writer_violations"](root, entries)
            self.assertEqual(len(errors), 1)
            self.assertIn("record_account_eligibility must be submitted", errors[0])

    def test_transaction_methods_and_test_modules_are_not_live_store_calls(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/striatum-module/src"
            source.mkdir(parents=True)
            main = source / "main.rs"
            main.write_text('''
#[cfg(all(test, target_os="linux"))] mod seeds;
fn operation(tx: &AccountTx<'_>) { tx.clear_account_route_fences_before(p,c,now); }
fn handler() { crate::account_writer::repair(store, move |tx| {
    tx.clear_account_route_fences_before(p,c,now)
}); }
''')
            (source / "seeds.rs").write_text("fn seed() { store.account_transaction(seed); }")
            entries = {"account_transaction", "clear_account_route_fences_before"}
            self.assertEqual(GUARD["account_writer_violations"](root, entries), [])
            main.write_text("#[cfg(test)] mod seeds;\nfn handler() { tx.account_transaction(seed); }")
            self.assertEqual(len(GUARD["account_writer_violations"](root, entries)), 1)

    def test_repository_account_writers_use_the_adapter(self):
        entries = GUARD["read_account_entries"](GUARD["ACCOUNT_ENTRIES"])
        self.assertEqual(GUARD["account_writer_violations"](ROOT, entries), [])

    def test_cfg_test_attribute_does_not_hide_the_following_live_module(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        for attribute in (
            '#[cfg(all(test, target_os="linux"))]',
            '#[cfg(all(\n    test,\n    target_os="linux",\n))]',
        ):
            with tempfile.TemporaryDirectory(dir=scratch) as directory:
                root = Path(directory)
                source = root / "crates/striatum-module/src"
                source.mkdir(parents=True)
                (source / "main.rs").write_text(
                    attribute + '\n#[path="seeds.rs"]\nmod seeds;\nmod live;\n'
                )
                (source / "seeds.rs").write_text("fn seed() { store.account_transaction(seed); }")
                (source / "live.rs").write_text("fn handler() { store.account_transaction(write); }")
                errors = GUARD["account_writer_violations"](root, {"account_transaction"})
                self.assertEqual(len(errors), 1, errors)
                self.assertIn("/live.rs:", errors[0])

    def test_projection_allowance_is_rejected(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            entries = Path(directory) / "entries.txt"
            entries.write_text("account_transaction\n")
            self.assertEqual(GUARD["read_account_entries"](entries), {"account_transaction"})
            entries.write_text("@projection path::function\naccount_transaction\n")
            with self.assertRaisesRegex(ValueError, "invalid account writer entry"):
                GUARD["read_account_entries"](entries)

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
