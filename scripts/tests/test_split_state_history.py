from contextlib import closing
import hashlib
import os
import shlex
import shutil
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
        self.archive = self.root / 'history.db'
        self.backup = self.root / 'state.db.pre-split'
        self.verifier = Path(os.environ['STRIATUM_SPLIT_VERIFIER']).resolve(strict=True)
        completed = subprocess.run([self.verifier, '--seed-test-fixture', self.state],
                                   text=True, capture_output=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def run_script(self, verifier=None, env=None):
        return subprocess.run([str(SCRIPT), str(self.state), '--now-ms', '1800000000000', '--verifier',
                               str(verifier or self.verifier)], text=True, capture_output=True, check=False, env=env)

    def fake_verifier(self, body):
        path = self.root / 'verify-wrapper'
        path.write_text('#!/bin/sh\nset -eu\n' + body + '\n')
        path.chmod(0o700)
        return path

    def test_places_whole_tables_and_seeds_the_watermark(self):
        before, inode = digest(self.state), self.state.stat().st_ino
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('row-count parity', result.stdout)
        self.assertIn('split complete:', result.stdout)
        self.assertIn('preflight: disk budget', result.stdout)
        self.assertIn('staging_peak_bytes=', result.stdout)
        self.assertRegex(result.stdout, r'evaluation parity: [1-9][0-9]* comparisons over [1-9][0-9]* identities; 0 mismatches')
        self.assertIn('explained 1 decisions referenced by moved holds', result.stdout)
        self.assertEqual(self.backup.stat().st_ino, inode)
        self.assertEqual(digest(self.backup), before)
        for file in (self.state, self.archive, self.backup):
            self.assertEqual(stat.S_IMODE(file.stat().st_mode), 0o600)
        with closing(sqlite3.connect(self.state)) as state, closing(sqlite3.connect(self.archive)) as history, closing(sqlite3.connect(self.backup)) as original:
            census = "SELECT name FROM sqlite_schema WHERE type='table'"
            self.assertEqual({name for name, in history.execute(census)}, MOVED | {'cortexkit_schema_version'})
            original_tables = {name for name, in original.execute(census)}
            self.assertTrue(STATE <= original_tables)
            self.assertEqual({name for name, in state.execute(census)}, original_tables - MOVED | {
                'history_layout', 'pending_account_hold', 'pending_quota_reading_log', 'quota_reading_moved'})
            for table in sorted(STATE):
                self.assertEqual(state.execute(f'SELECT * FROM {table}').fetchall(),
                                 original.execute(f'SELECT * FROM {table}').fetchall())
            for db, namespace, version in ((state, 'route_state', 98), (history, 'route_history', 1)):
                self.assertEqual(db.execute('SELECT max(version) FROM cortexkit_schema_version WHERE namespace=?', (namespace,)).fetchone(), (version,))
            self.assertEqual(history.execute('SELECT observed_at,used_percent FROM quota_reading_log').fetchall(),
                             [(10, 4.0), (11, 5.0)])
            self.assertEqual(state.execute('SELECT * FROM quota_reading_moved').fetchall(),
                             [('acct', 'window', 11)])

    def test_compaction_creates_private_output_without_precreating_it(self):
        # Some SQLite versions accept an empty destination. Model the stricter
        # behavior so this regression also runs on those versions.
        (self.root / 'sitecustomize.py').write_text('''\
import os
import sqlite3

os.umask(0o022)
real_connect = sqlite3.connect

class StrictVacuumConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        if sql == 'VACUUM INTO ?':
            if os.path.lexists(parameters[0]):
                raise sqlite3.OperationalError('VACUUM INTO output file already exists')
            print('tested VACUUM INTO with an absent destination', flush=True)
        return super().execute(sql, parameters)

def connect(*args, **kwargs):
    return real_connect(*args, factory=StrictVacuumConnection, **kwargs)

sqlite3.connect = connect
''')
        result = self.run_script(env={**os.environ, 'PYTHONPATH': str(self.root)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('tested VACUUM INTO with an absent destination', result.stdout)
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o600)

    def test_verifier_failure_keeps_inode_and_bytes_and_publishes_nothing(self):
        verifier = self.fake_verifier('echo planted-mismatch >&2\nexit 3')
        before, inode = digest(self.state), self.state.stat().st_ino
        result = self.run_script(verifier)
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
    def test_refuses_low_space_before_staging(self):
        (self.root / 'sitecustomize.py').write_text(
            'import shutil\nshutil.disk_usage = lambda _p: shutil._ntuple_diskusage(0, 0, 1)\n')
        result = self.run_script(env={**os.environ, 'PYTHONPATH': str(self.root)})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('insufficient free space for split', result.stderr)
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.archive.exists())

    def test_refuses_stale_stage_and_explains_half_publish_recovery(self):
        stale = self.root / '.split-state-history-seeded'
        stale.mkdir()
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(stale), result.stderr)
        stale.rmdir()
        os.link(self.state, self.backup)
        self.archive.write_bytes(b'archive linked before interruption')
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('half-published split: original is intact and unsplit.', result.stderr)
        self.assertIn('cmp --', result.stderr)
        self.assertIn('rm --', result.stderr)
        self.assertEqual(self.state.stat().st_ino, self.backup.stat().st_ino)

    def test_refuses_uncheckpointed_verifier_output_before_publish(self):
        verifier = self.fake_verifier(f'{shlex.quote(str(self.verifier))} "$@"\n' + 'touch "$2-wal"')
        result = self.run_script(verifier)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('state.db-wal', result.stderr)
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.archive.exists())

    def test_explain_verifier_detects_changed_account_decision(self):
        verifier = self.fake_verifier(
            'python3 -c ' + shlex.quote('import sqlite3, sys; c=sqlite3.connect(sys.argv[1]); c.execute("UPDATE account_decision SET policy_version=99"); c.commit()') + ' "$2"\n' +
            shlex.quote(str(self.verifier)) + ' "$@"')
        result = self.run_script(verifier)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('decision explain changed for moved hold test-hold', result.stderr)
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.archive.exists())
