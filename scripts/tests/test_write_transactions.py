import runpy
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD = runpy.run_path(str(ROOT / "scripts/write-transactions.py"))


class WriteTransactionsTests(unittest.TestCase):
    def test_retained_view_and_removed_trigger_both_remain_frozen_names(self):
        rules = GUARD["read_frozen_names"](GUARD["FROZEN_NAMES"])
        self.assertIn("fence_origin", rules[0])
        self.assertIn("model_route_state_inherit_fence_scope", rules[0])
        for sql in (
            "SELECT * FROM fence_origin",
            "CREATE TRIGGER model_route_state_inherit_fence_scope AFTER INSERT ON model_route_state BEGIN SELECT 1; END",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(GUARD["frozen_sql_names"](sql, rules))

    def frozen(self, source):
        rules = GUARD["read_frozen_names"](GUARD["FROZEN_NAMES"])
        return GUARD["frozen_sql_sites"](source, rules)[1]

    def test_frozen_relations_are_detected_in_reads_writes_and_quoted_sql(self):
        for sql in (
            "SELECT * FROM account_fence",
            "UPDATE credential_entitlement_fences SET code=?1",
            "DELETE FROM fence_recheck WHERE kind=?1",
            "INSERT INTO account_size_refusal VALUES (?1)",
            'SELECT * FROM main."fence_origin"',
            "SELECT * FROM [account_principal_exclusion]",
            "SELECT * FROM 'account_transient_backoff'",
            "DROP TRIGGER fence_retire_enforcement",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(self.frozen(f'fn write() {{ let sql = r###"{sql}"###; }}'))

    def test_frozen_columns_follow_aliases_but_not_dto_fields_or_joined_block_columns(self):
        for sql in (
            "SELECT m.cooldown_until FROM model_route_state AS m",
            "SELECT p.`wall_reason` FROM pool_key_state p",
            "SELECT [p].[wall_class] FROM provider_cooldown_state [p]",
            "UPDATE model_route_state SET quota_exhausted_until=?1",
            "SELECT cooldown_until FROM model_route_state",
            "SELECT x.cooldown_until FROM block b, model_route_state x",
            "SELECT m.* FROM model_route_state m",
            "SELECT * FROM pool_key_state",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(self.frozen(f'const SQL: &str = r#"{sql}"#;'))
        for source in (
            "fn write(update: Update) { let until = update.cooldown_until; }",
            'const FIELD: &str = "cooldown_until";',
            'const SQL: &str = "SELECT b.cooldown_until FROM model_route_state m JOIN block b ON b.origin_key=json_array(m.route_key)";',
            'const SQL: &str = "UPDATE block SET cooldown_until=?1 WHERE kind=\'route\'";',
            'const SQL: &str = "SELECT COUNT(*) FROM model_route_state";',
            'const SQL: &str = "SELECT \'cooldown_until\',m.route_key FROM model_route_state m";',
            'const SQL: &str = "SELECT b.cooldown_until FROM block b -- FROM model_route_state\\n";',
        ):
            with self.subTest(source=source):
                self.assertEqual(self.frozen(source), [])

    def test_comments_attributes_and_test_only_oracle_items_are_not_serving_sql(self):
        self.assertEqual(self.frozen('''
// const SQL: &str = "SELECT * FROM account_fence";
/* outer /* nested */ "SELECT * FROM fence_origin" */
#[cfg(test)]
fn old() { conn.prepare("SELECT * FROM fence_origin"); }
#[cfg(any(test, feature = "test-support"))]
const OLD: &str = r#"SELECT * FROM account_fence"#;
#[cfg(feature = "test-support")]
fn oracle() { conn.prepare("SELECT * FROM account_route_fence"); }
#[test]
fn invariant() { conn.prepare("SELECT * FROM fence_recheck"); }
const LIVE: &str = "SELECT provider FROM block";
'''), [])
        errors = self.frozen('''
#[cfg(any(test, feature = "test-support"))]
fn old() { conn.prepare("SELECT * FROM fence_origin"); }
fn serving() { conn.prepare("SELECT * FROM account_fence"); }
''')
        self.assertEqual(errors, [(4, "frozen relation account_fence")])

    def test_rust_literal_decoding_cannot_hide_a_frozen_name(self):
        for spelling in (
            '"SELECT * FROM account_\\x66ence"',
            '"SELECT * FROM fence_\\u{6f}rigin"',
            'r"SELECT * FROM account_fence"',
            'r####"SELECT * FROM account_fence"####',
            '"SELECT * \\\n            FROM account_fence"',
            '"SELECT * FROM \\"account_fence\\""',
        ):
            with self.subTest(spelling=spelling):
                self.assertTrue(self.frozen(f"const SQL: &str = {spelling};"))

    def test_literal_concat_and_format_cannot_split_a_frozen_column_from_its_table(self):
        for expression in (
            'concat!("SELECT ", "cooldown_until", " FROM model_route_state")',
            'concat!("SELECT * FROM ", "account_", "fence")',
            'format!("SELECT {} FROM model_route_state", "cooldown_until")',
            'format!("SELECT {0} FROM {1}", "wall_reason", "pool_key_state")',
        ):
            with self.subTest(expression=expression):
                self.assertTrue(self.frozen(f"fn serving() {{ let sql = {expression}; }}"))
                self.assertEqual(self.frozen(f"#[cfg(test)] fn oracle() {{ let sql = {expression}; }}"), [])

    def test_test_only_module_descendants_are_excluded_but_filenames_do_not_exempt(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        rules = GUARD["read_frozen_names"](GUARD["FROZEN_NAMES"])
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/probe/src"
            (source / "old").mkdir(parents=True)
            (source / "lib.rs").write_text('#[cfg(any(test, feature="test-support"))]\nmod old;\n')
            (source / "old.rs").write_text('mod child;\nconst SQL: &str = "SELECT * FROM account_fence";')
            (source / "old/child.rs").write_text('const SQL: &str = "SELECT * FROM fence_origin";')
            self.assertEqual(GUARD["frozen_sql_violations"](root, rules)[1], [])
            live = source / "production_tests.rs"
            live.write_text('const SQL: &str = "SELECT * FROM fence_origin";')
            errors = GUARD["frozen_sql_violations"](root, rules)[1]
            self.assertEqual(len(errors), 1)
            self.assertIn("production_tests.rs:1: frozen relation fence_origin", errors[0])
            (source / "lib.rs").write_text('mod old;\n')
            errors = GUARD["frozen_sql_violations"](root, rules)[1]
            self.assertEqual(len(errors), 3)

    def test_cli_runs_frozen_checks_from_the_committed_catalogue(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            source = root / "crates/probe/src/lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text('const SQL: &str = "SELECT * FROM fence_origin";')
            empty = root / "empty.txt"
            empty.write_text("")
            globals_ = GUARD["main"].__globals__
            previous = {name: globals_[name] for name in ("ROOT", "ALLOWLIST", "ACCOUNT_ENTRIES")}
            try:
                globals_.update(ROOT=root, ALLOWLIST=empty, ACCOUNT_ENTRIES=empty)
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    result = GUARD["main"]()
                self.assertTrue(result)
                self.assertIn("crates/probe/src/lib.rs:1: frozen relation fence_origin", output.getvalue())
                self.assertIn("1 violations", output.getvalue())
            finally:
                globals_.update(previous)

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
