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
        self.sql_log = self.home / "sql.log"
        self.clone_log = self.home / "clone.log"
        self.python_shim = self.home / "python-shim"
        self.python_shim.mkdir()
        (self.python_shim / "sitecustomize.py").write_text('''
import ctypes, errno, os, shutil, sqlite3
from pathlib import Path
original_connect = sqlite3.connect
def traced_connect(*args, **kwargs):
    connection = original_connect(*args, **kwargs)
    database = str(args[0])
    def record(statement):
        with open(os.environ["MOVE_SQL_LOG"], "a") as output:
            output.write(database + "\\t" + statement + "\\n")
    connection.set_trace_callback(record)
    return connection
sqlite3.connect = traced_connect
original_cdll = ctypes.CDLL
def clonefile(source, target, flags):
    assert flags == 0x0001, flags
    with open(os.environ["MOVE_CLONE_LOG"], "a") as output:
        output.write(os.fsdecode(source) + " -> " + os.fsdecode(target) + "\\n")
    mode = os.environ["CLONE_STUB_MODE"]
    if mode == "unsupported":
        ctypes.set_errno(errno.ENOTSUP)
        return -1
    source, target = Path(os.fsdecode(source)), Path(os.fsdecode(target))
    try:
        if mode == "existing":
            target.mkdir()
            (target / "do-not-overwrite").write_text("existing backup")
        shutil.copytree(source, target, symlinks=True,
                        copy_function=os.link if mode == "hardlink" else shutil.copy2)
        if mode == "incomplete":
            (target / "boot-strikes.json").unlink()
        if mode == "wrong-size":
            (target / "boot-strikes.json").write_text("truncated")
        return 0
    except FileExistsError:
        ctypes.set_errno(errno.EEXIST)
        return -1
def stubbed_cdll(*args, **kwargs):
    library = original_cdll(*args, **kwargs)
    if os.environ.get("MOVE_CLONE_STUB") == "1":
        library.clonefile = clonefile
    return library
ctypes.CDLL = stubbed_cdll
''')
        with sqlite3.connect(self.old / "store.db") as database:
            database.executescript("CREATE TABLE models(id TEXT); INSERT INTO models VALUES ('first'), ('second');"
                                   "CREATE TABLE account_state(id INTEGER); INSERT INTO account_state VALUES (17);")
        (self.old / "boot-strikes.json").write_text('{"strikes": []}\n')
        (self.old / "engram-catalog.json").write_text('{"module_id":"prefrontal-routing"}\n')
        (self.old / "writer.lease").write_text("leave this lease alone\n")
        (self.old / "nested").mkdir()
        (self.old / "nested/metadata.json").write_text('{"keep":"whole directory"}\n')
        (self.old / "ledger-link").symlink_to("boot-strikes.json")

    def run_script(self, *args, clone_mode=None):
        environment = dict(os.environ, HOME=str(self.home),
                           PYTHONPATH=str(self.python_shim), MOVE_SQL_LOG=str(self.sql_log),
                           MOVE_CLONE_LOG=str(self.clone_log), MOVE_CLONE_STUB="1",
                           CLONE_STUB_MODE=clone_mode or "ok", PYTHONDONTWRITEBYTECODE="1")
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
        for step in ("wal_checkpoint(TRUNCATE)", "WHOLE", "clonefile", "inode", "file listings/sizes",
                     "boot-strikes.json", "delete", ".lease", "no files changed", "TIMING", "--verify-full"):
            self.assertIn(step, result.stdout)
        self.assertEqual(self.tree(), before)
        self.assertFalse(self.new.exists())
        self.assertFalse(self.sql_log.exists())

    def test_moves_store_and_ledger_retires_catalog_and_validates_backup(self):
        source_stat = (self.old / "store.db").stat()
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        moved_stat = (self.new / "store.db").stat()
        self.assertEqual((moved_stat.st_dev, moved_stat.st_ino, moved_stat.st_size),
                         (source_stat.st_dev, source_stat.st_ino, source_stat.st_size))
        self.assertIn("Fast verification passed", result.stdout)
        self.assertIn("TIMING verify moved store inode and size:", result.stdout)
        self.assertIn("TIMING total:", result.stdout)
        statements = self.sql_log.read_text().splitlines()
        self.assertEqual(len(statements), 1, statements)
        self.assertIn("PRAGMA wal_checkpoint(TRUNCATE)", statements[0])
        self.assertIn(str(self.old / "store.db"), statements[0])
        self.assertFalse((self.old / "store.db").exists())
        self.assertFalse((self.old / "engram-catalog.json").exists())
        self.assertFalse((self.new / "engram-catalog.json").exists())
        self.assertEqual((self.old / "writer.lease").read_text(), "leave this lease alone\n")
        self.assertEqual((self.new / "boot-strikes.json").read_text(), '{"strikes": []}\n')
        backups = [path for path in self.base.glob("prefrontal-routing-backup-*") if path.is_dir()]
        self.assertEqual(len(backups), 1)
        self.assertTrue((backups[0] / "engram-catalog.json").is_file())
        self.assertTrue((backups[0] / "writer.lease").is_file())
        self.assertEqual((backups[0] / "nested/metadata.json").read_text(), '{"keep":"whole directory"}\n')
        self.assertTrue((backups[0] / "ledger-link").is_symlink())
        record = json.loads(Path(str(backups[0]) + ".manifest.json").read_text())
        self.assertEqual(record["files"]["store.db"]["size"], source_stat.st_size)
        self.assertEqual(record["source_store"]["inode"], source_stat.st_ino)
        self.assertEqual(record["backup_mode"], "clone")
        self.assertNotEqual((backups[0] / "store.db").stat().st_ino, moved_stat.st_ino)
        self.assertEqual(len(self.clone_log.read_text().splitlines()), 1)

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
        self.assertIn("Fast verification passed", result.stdout)

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

    def test_clone_failure_never_falls_back_to_a_full_copy_or_moves_source(self):
        for mode, message in (("unsupported", "clonefile refused"), ("incomplete", "listing/size mismatch"),
                              ("wrong-size", "listing/size mismatch"), ("hardlink", "hard link"),
                              ("existing", "clonefile refused")):
            with self.subTest(mode=mode):
                result = self.run_script(clone_mode=mode)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(message, result.stderr)
                self.assertTrue((self.old / "store.db").is_file())
                self.assertTrue((self.old / "engram-catalog.json").is_file())
                self.assertFalse(self.new.exists())
                self.assertFalse(list(self.base.glob("*.tar")))
        existing = list(self.base.glob("prefrontal-routing-backup-*/do-not-overwrite"))
        self.assertEqual(len(existing), 1)
        self.assertEqual(existing[0].read_text(), "existing backup")

    def test_tar_is_an_explicit_backup_opt_in_and_never_calls_clonefile(self):
        result = self.run_script("--backup-mode=tar", clone_mode="unsupported")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.clone_log.exists())
        backups = list(self.base.glob("*.tar"))
        self.assertEqual(len(backups), 1)
        with tarfile.open(backups[0], "r:") as archive:
            self.assertIn("prefrontal-routing/store.db", archive.getnames())
            self.assertIn("prefrontal-routing/engram-catalog.json", archive.getnames())
            self.assertIn("prefrontal-routing/nested/metadata.json", archive.getnames())
        self.assertIn("tar -xf", result.stderr)
        self.assertIn("Fast verification passed", result.stdout)

    def moved_clone(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        backups = [path for path in self.base.glob("prefrontal-routing-backup-*") if path.is_dir()]
        self.assertEqual(len(backups), 1)
        self.sql_log.unlink()
        # Offline confirmation must not consult subc, even when neither
        # module can report stopped because striatum is already serving.
        self.ck.unlink()
        return backups[0]

    def test_full_verification_runs_only_on_private_clone_without_module_guards(self):
        backup = self.moved_clone()
        with sqlite3.connect(self.new / "store.db") as live:
            live.execute("INSERT INTO models VALUES ('live-only')")
        live_before = (self.new / "store.db").read_bytes()
        result = self.run_script("--verify-full", str(backup))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Clone row counts: {"account_state": 1, "models": 2}', result.stdout)
        statements = self.sql_log.read_text().splitlines()
        self.assertTrue(any("count(*)" in statement for statement in statements), statements)
        self.assertTrue(any("PRAGMA integrity_check" in statement for statement in statements), statements)
        self.assertTrue(all(str(backup / "store.db") in statement for statement in statements), statements)
        self.assertEqual((self.new / "store.db").read_bytes(), live_before)
        reports = list(self.base.glob(backup.name + ".verification-*.json"))
        self.assertEqual(len(reports), 1)
        report = json.loads(reports[0].read_text())
        self.assertTrue(report["ok"])
        self.assertEqual(report["integrity_check"], ["ok"])
        self.assertEqual(report["row_counts"], {"account_state": 1, "models": 2})
        self.assertIn("read-write", report["sqlite_open_mode"])
        self.assertIn("private", report["scope"])

    def test_full_verification_dry_run_does_not_open_sqlite_or_write_report(self):
        backup = self.moved_clone()
        before = self.tree()
        result = self.run_script("--dry-run", "--verify-full", str(backup))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("only on private clone", result.stdout)
        self.assertEqual(self.tree(), before)
        self.assertFalse(self.sql_log.exists())

    def test_full_verification_rejects_live_directory_and_source_hardlink(self):
        backup = self.moved_clone()
        result = self.run_script("--verify-full", str(self.new))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must never target", result.stderr)
        self.assertFalse(self.sql_log.exists())
        (backup / "store.db").unlink()
        os.link(self.new / "store.db", backup / "store.db")
        result = self.run_script("--verify-full", str(backup))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("shares the source inode", result.stderr)
        self.assertFalse(self.sql_log.exists())

    def test_full_verification_saves_failure_report_without_touching_live_store(self):
        backup = self.moved_clone()
        (backup / "store.db").write_bytes(b"not a SQLite database")
        live_before = (self.new / "store.db").read_bytes()
        result = self.run_script("--verify-full", str(backup))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("live store was not opened", result.stderr)
        reports = list(self.base.glob(backup.name + ".verification-*.json"))
        self.assertEqual(len(reports), 1)
        report = json.loads(reports[0].read_text())
        self.assertFalse(report["ok"])
        self.assertIn("not a database", report["error"])
        self.assertEqual((self.new / "store.db").read_bytes(), live_before)


if __name__ == "__main__":
    unittest.main()
