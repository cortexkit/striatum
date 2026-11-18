import hashlib
from pathlib import Path
import sqlite3
import stat
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'script/split-state-history.sh'
MIGRATIONS = ROOT / 'crates/striatum-store/history-migrations'
MOVED = set((MIGRATIONS / 'tables.txt').read_text().splitlines())
WHOLE = {'account_admission', 'account_observation', 'model_route_decisions',
         'routing_v1_decision', 'routing_v1_report', 'routing_v1_report_delivery'}
STATE = WHOLE | {'model_route_usage', 'model_route_speed_samples', 'routing_engine_speed'}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SplitStateHistoryTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix='split-state-history-')
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.state = self.root / 'state.db'
        self.state.touch(mode=0o600)
        self.archive = self.root / 'history.db'
        self.backup = self.root / 'state.db.pre-split'
        self.verifier = self.root / 'verify'
        self.verifier.write_text('#!/bin/sh\nexit 0\n')
        self.verifier.chmod(0o700)
        db = sqlite3.connect(self.state)
        with db:
            db.executescript((MIGRATIONS / '001_split_baseline.sql').read_text())
            db.execute('CREATE TABLE cortexkit_schema_version(namespace TEXT, version INTEGER)')
            db.execute("INSERT INTO cortexkit_schema_version VALUES ('route_state', 97)")
            for table in sorted(STATE):
                db.execute(f'CREATE TABLE {table}(sample TEXT)')
                db.execute(f'INSERT INTO {table} VALUES (?)', (table,))
            db.execute("INSERT INTO quota_reading_log VALUES ('acct','window',10,4,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL)")
            db.execute("INSERT INTO quota_reading_log VALUES ('acct','window',11,5,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL)")
        db.close()

    def run_script(self):
        return subprocess.run([str(SCRIPT), str(self.state), '--now-ms', '20', '--verifier',
                               str(self.verifier)], text=True, capture_output=True, check=False)

    def test_places_whole_tables_and_seeds_the_watermark(self):
        before = digest(self.state)
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('row-count parity', result.stdout)
        self.assertIn('split complete:', result.stdout)
        self.assertEqual(digest(self.backup), before)
        for file in (self.state, self.archive, self.backup):
            self.assertEqual(stat.S_IMODE(file.stat().st_mode), 0o600)
        with sqlite3.connect(self.state) as state, sqlite3.connect(self.archive) as history:
            census = 'SELECT name FROM sqlite_schema WHERE type=\'table\''
            self.assertEqual({name for name, in history.execute(census)}, MOVED)
            self.assertEqual({name for name, in state.execute(census)}, STATE | {
                'cortexkit_schema_version', 'pending_account_hold', 'pending_quota_reading_log',
                'quota_reading_moved'})
            for table in sorted(STATE):
                self.assertEqual(state.execute(f'SELECT * FROM {table}').fetchall(), [(table,)])
            self.assertEqual(history.execute('SELECT observed_at,used_percent FROM quota_reading_log').fetchall(),
                             [(10, 4.0), (11, 5.0)])
            self.assertEqual(state.execute('SELECT * FROM quota_reading_moved').fetchall(),
                             [('acct', 'window', 11)])

    def test_verifier_failure_keeps_inode_and_bytes_and_publishes_nothing(self):
        self.verifier.write_text('#!/bin/sh\necho planted-mismatch >&2\nexit 3\n')
        before, inode = digest(self.state), self.state.stat().st_ino
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('planted-mismatch', result.stderr)
        self.assertEqual(digest(self.state), before)
        self.assertEqual(self.state.stat().st_ino, inode)
        self.assertFalse(self.archive.exists())
        self.assertFalse(self.backup.exists())

    def test_preflight_refuses_history_and_each_unclean_sidecar(self):
        before = digest(self.state)
        for name in ('history.db', 'state.db-wal', 'state.db-shm', 'state.db-journal'):
            with self.subTest(name=name):
                obstruction = self.root / name
                obstruction.write_bytes(b'planted')
                result = self.run_script()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f'refusing existing file:', result.stderr)
                self.assertIn(name, result.stderr)
                self.assertEqual(obstruction.read_bytes(), b'planted')
                self.assertEqual(digest(self.state), before)
                self.assertFalse(self.backup.exists())
                obstruction.unlink()