"""Offline rehearsal: every ck response and every store lives under a scratch root."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "script/move-store-to-striatum.sh"


class MoveStoreTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="striatum-store-move-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.base = self.home / ".local/share/cortexkit"
        self.old = self.base / "prefrontal-routing"
        self.new = self.base / "striatum"
        self.old.mkdir(parents=True)
        self.ck = self.base / "bin/ck"
        self.ck.parent.mkdir()
        self.statuses = self.home / "statuses.json"
        self.statuses.write_text("{}")
        self.ck.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
assert sys.argv[1:3] == ["module", "status"] and sys.argv[4:] == ["--json"]
module_id = sys.argv[3]
overrides = json.loads((Path(os.environ["HOME"]) / "statuses.json").read_text())
status = overrides.get(module_id, {"module": {"module_id": module_id, "state": "disabled", "live": False}, "catalog": None})
if status == "error":
    sys.exit(1)
if status == "invalid_json":
    print("not json")
else:
    print(json.dumps(status))
''')
        self.ck.chmod(0o700)
        with sqlite3.connect(self.old / "store.db") as database:
            database.executescript("CREATE TABLE models(id TEXT); INSERT INTO models VALUES ('first'), ('second');"
                                   "CREATE TABLE account_state(id INTEGER); INSERT INTO account_state VALUES (17);")
        (self.old / "boot-strikes.json").write_text('{"strikes": []}\n')
        (self.old / "engram-catalog.json").write_text('{"module_id":"prefrontal-routing"}\n')
        (self.old / "writer.lease").write_text("leave this lease alone\n")

    def run_script(self, *args):
        environment = dict(os.environ, HOME=str(self.home))
        environment.pop("XDG_DATA_HOME", None)
        return subprocess.run([str(SCRIPT), *args], env=environment, text=True, capture_output=True)

    def tree(self):
        return {
            str(path.relative_to(self.home)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in self.home.rglob("*") if path.is_file()
        }

    def test_dry_run_prints_all_steps_without_changing_any_files(self):
        before = self.tree()
        result = self.run_script("--dry-run", "--base-dir", str(self.base))
        self.assertEqual(result.returncode, 0, result.stderr)
        for step in ("wal_checkpoint(TRUNCATE)", "WHOLE", "SHA-256", "row", "integrity_check",
                     "boot-strikes.json", "delete", ".lease", "no files changed"):
            self.assertIn(step, result.stdout)
        self.assertEqual(self.tree(), before)
        self.assertFalse(self.new.exists())

    def test_moves_store_and_ledger_retires_catalog_and_validates_backup(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Before row counts: {"account_state": 1, "models": 2}', result.stdout)
        self.assertIn('After row counts: {"account_state": 1, "models": 2}', result.stdout)
        self.assertIn("integrity_check: ok", result.stdout)
        self.assertFalse((self.old / "store.db").exists())
        self.assertFalse((self.old / "engram-catalog.json").exists())
        self.assertFalse((self.new / "engram-catalog.json").exists())
        self.assertEqual((self.old / "writer.lease").read_text(), "leave this lease alone\n")
        self.assertEqual((self.new / "boot-strikes.json").read_text(), '{"strikes": []}\n')
        backups = list(self.base.glob("*.tar.gz"))
        self.assertEqual(len(backups), 1)
        with tarfile.open(backups[0], "r:gz") as archive:
            self.assertIn("prefrontal-routing/engram-catalog.json", archive.getnames())
            self.assertIn("prefrontal-routing/writer.lease", archive.getnames())
            self.assertGreater(len(archive.extractfile("prefrontal-routing/store.db").read()), 0)
        self.assertEqual(json.loads(Path(str(backups[0]) + ".rows.json").read_text()),
                         {"account_state": 1, "models": 2})

    def test_checkpoints_unmerged_wal_without_losing_committed_rows(self):
        # Abrupt process exit preserves committed WAL pages without a last-close checkpoint.
        subprocess.run(["python3", "-c", '''
import os, sqlite3, sys
c = sqlite3.connect(sys.argv[1])
c.execute("PRAGMA journal_mode=WAL")
c.execute("PRAGMA wal_autocheckpoint=0")
c.execute("INSERT INTO models VALUES ('wal-only')")
c.commit()
os._exit(0)
''', str(self.old / "store.db")], check=True)
        self.assertGreater((self.old / "store.db-wal").stat().st_size, 0)
        before = self.tree()
        preview = self.run_script("--dry-run")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        self.assertEqual(self.tree(), before)
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        with sqlite3.connect(self.new / "store.db") as database:
            self.assertEqual(database.execute("SELECT count(*) FROM models").fetchone()[0], 3)
        self.assertIn('"models": 3', result.stdout)

    def test_refuses_running_missing_unknown_or_unparseable_status_for_either_id(self):
        for module_id in ("prefrontal-routing", "striatum"):
            stopped = {"module": {"module_id": module_id, "state": "stopped", "live": False}, "catalog": None}
            bad_statuses = [
                "error", "invalid_json", {},
                {"module": {"module_id": module_id, "state": "running", "live": True}, "catalog": {}},
                {**stopped, "module": {**stopped["module"], "state": "starting"}},
                {**stopped, "module": {**stopped["module"], "live": 0}},
                {**stopped, "module": {**stopped["module"], "module_id": "other"}},
                {"module": stopped["module"]},
                {**stopped, "catalog": {"module_id": module_id}},
            ]
            for status in bad_statuses:
                with self.subTest(module_id=module_id, status=status):
                    self.statuses.write_text(json.dumps({module_id: status}))
                    before = self.tree()
                    for args in ((), ("--dry-run",)):
                        result = self.run_script(*args)
                        self.assertNotEqual(result.returncode, 0, result.stdout)
                        self.assertIn("REFUSED", result.stderr)
                        self.assertEqual(self.tree(), before)

    def test_accepts_stopped_as_well_as_disabled(self):
        self.statuses.write_text(json.dumps({name: {"module": {"module_id": name, "state": "stopped", "live": False},
                                                       "catalog": None}
                                            for name in ("prefrontal-routing", "striatum")}))
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_refuses_destination_store_or_sidecar_or_ledger_without_overwriting(self):
        self.new.mkdir()
        for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
            with self.subTest(name=name):
                target = self.new / name
                target.write_text("do not overwrite")
                before = self.tree()
                result = self.run_script()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("destination already holds", result.stderr)
                self.assertEqual(self.tree(), before)
                target.unlink()

    def test_refuses_busy_wal_and_leaves_source_catalog_in_place(self):
        with sqlite3.connect(self.old / "store.db") as database:
            database.execute("PRAGMA journal_mode=WAL")
            database.execute("BEGIN IMMEDIATE")
            result = self.run_script()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("checkpoint incomplete", result.stderr)
        self.assertTrue((self.old / "engram-catalog.json").exists())
        self.assertFalse(self.new.exists())


if __name__ == "__main__":
    unittest.main()
