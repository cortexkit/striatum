#!/bin/sh
# Run on a cleanly closed offline copy, never on the live store.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec python3 - "$root" "$@" <<'PY'
import argparse
from contextlib import closing
import shlex
import secrets
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time

ROOT = Path(sys.argv[1])
MIGRATIONS = ROOT / 'crates/striatum-store'
HISTORY_TABLES = (MIGRATIONS / 'history-migrations/tables.txt').read_text().splitlines()
STATE_VERSION = 97


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def absent(path):
    require(not os.path.lexists(path), f'refusing existing file: {path}')


def identity(path):
    st = path.lstat()
    require(stat.S_ISREG(st.st_mode), f'not a regular file: {path}')
    require(stat.S_IMODE(st.st_mode) == 0o600, f'offline source must be mode 0600: {path}')
    for suffix in ('-wal', '-shm', '-journal'):
        absent(Path(str(path) + suffix))
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def connect(path, read_only=False):
    uri = path.as_uri() + ('?mode=ro&immutable=1' if read_only else '?mode=rw')
    return sqlite3.connect(uri, uri=True, timeout=0)


def counts(connection):
    tables = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {name: connection.execute(f'SELECT count(*) FROM {quote(name)}').fetchone()[0]
            for (name,) in tables}


def check_integrity(path):
    with connect(path, read_only=True) as db:
        require(db.execute('PRAGMA integrity_check').fetchall() == [('ok',)],
                f'integrity_check failed: {path.name}')
        require(not db.execute('PRAGMA foreign_key_check').fetchone(),
                f'foreign_key_check failed: {path.name}')
        return counts(db)


def copy_history(original, history, split_identity):
    history.touch(mode=0o600, exist_ok=False)
    with connect(history) as db:
        db.executescript((MIGRATIONS / 'history-migrations/001_split_baseline.sql').read_text())
        db.execute('INSERT INTO history_binding VALUES (1, ?, 0)', (split_identity,))
        db.execute('ATTACH DATABASE ? AS source', (original.as_uri() + '?mode=ro&immutable=1',))
        for name in HISTORY_TABLES:
            table = quote(name)
            kind = db.execute(
                'SELECT wr FROM pragma_table_list WHERE schema=? AND name=?', ('main', name),
            ).fetchone()
            names = db.execute(f'PRAGMA main.table_xinfo({table})')
            columns = ','.join(quote(row[1]) for row in names if not row[6])
            if not kind[0]:
                columns = 'rowid,' + columns
            db.execute(f'INSERT INTO main.{table} ({columns}) SELECT {columns} FROM source.{table}')


def stage_state(original, state, history, compact, split_identity):
    state.touch(mode=0o600, exist_ok=False)
    shutil.copyfile(original, state)
    with closing(connect(state)) as db, db:
        db.execute('PRAGMA journal_mode=DELETE')
        db.create_function('history_split_identity', 0, lambda: split_identity, deterministic=True)
        db.executescript((MIGRATIONS / 'migrations/098_history_outbox.sql').read_text())
        db.execute('ATTACH DATABASE ? AS archive', (history.as_uri() + '?mode=ro&immutable=1',))
        db.execute('''INSERT INTO quota_reading_moved
                      SELECT account_key, window_key, max(observed_at)
                      FROM archive.quota_reading_log GROUP BY account_key, window_key''')
    # Some SQLite builds reject a pre-created destination, even if empty.
    # Let SQLite create the file privately instead.
    previous_umask = os.umask(0o077)
    try:
        with closing(connect(state)) as db:
            db.execute('VACUUM INTO ?', (str(compact),))
    finally:
        os.umask(previous_umask)


def sync(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def history_bytes(db, state_bytes):
    names = tuple(HISTORY_TABLES)
    marks = ','.join('?' for _ in names)
    try:
        used = db.execute(f"""SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name IN (
            SELECT name FROM sqlite_schema WHERE tbl_name IN ({marks})
        )""", names).fetchone()[0]
        return used + used // 10 + 1024 * 1024
    except sqlite3.OperationalError:
        # Without per-table page accounting, budget another full source copy.
        return state_bytes + 1024 * 1024


def check_partial_publish(original, history, backup):
    if not backup.is_file() or backup.is_symlink():
        return
    if os.path.lexists(history) and (history.is_symlink() or not history.is_file()):
        return
    with connect(original, read_only=True) as db:
        version = db.execute("SELECT max(version) FROM cortexkit_schema_version WHERE namespace='route_state'").fetchone()[0]
        names = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
    if version == STATE_VERSION and set(HISTORY_TABLES) <= names:
        require(identity(original)[:3] == identity(backup)[:3],
                'partial publish: original and rollback link are not the same inode and size; preserve all files and investigate')
        if not history.exists():
            rm_cmd = shlex.join(['rm', '--', str(backup)])
            raise RuntimeError(f'backup-only interruption: original is intact and unsplit. Keep routing stopped, run `{rm_cmd}`, then rerun the split. Do not replace the original.')
        identity(history)
        paths = [str(path) for path in (original, backup, history)]
        cmp_cmd = shlex.join(['cmp', '--', paths[0], paths[1]])
        rm_cmd = shlex.join(['rm', '--', paths[1], paths[2]])
        raise RuntimeError(f'half-published split: original is intact and unsplit. Keep routing stopped, run `{cmp_cmd} && {rm_cmd}`, then rerun the split. Do not replace the original.')


def main(args):
    started = time.monotonic()
    original = Path(os.path.abspath(args.state_db))
    stem = 'history.db' if original.name in ('state.db', 'store.db') else original.name + '.history.db'
    history = original.with_name(stem)
    backup = original.with_name(original.name + '.pre-split')
    before = identity(original)
    check_partial_publish(original, history, backup)
    for stale in sorted(original.parent.glob('.split-state-history-*')):
        raise RuntimeError(f'stale split staging path: {stale}; confirm no split is running, inspect or remove it, then rerun')
    for target in (history, backup):
        for suffix in ('', '-wal', '-shm', '-journal'):
            absent(Path(str(target) + suffix))
    with connect(original, read_only=True) as db:
        version = db.execute("SELECT max(version) FROM cortexkit_schema_version WHERE namespace='route_state'").fetchone()[0]
        require(version == STATE_VERSION, f'expected route_state version {STATE_VERSION}, found {version}')
        required_bytes = 2 * before[2] + history_bytes(db, before[2])
        free_bytes = shutil.disk_usage(original.parent).free
        print(f'preflight: disk budget {required_bytes} bytes (scratch state, compact state, history and slack); free {free_bytes} bytes', flush=True)
        require(free_bytes >= required_bytes,
                f'insufficient free space for split: need about {required_bytes} bytes (scratch state, compact state, and history), have {free_bytes}')
        expected = counts(db)
    require(set(HISTORY_TABLES) <= expected.keys(), 'missing history-assigned table in original')

    with tempfile.TemporaryDirectory(prefix='.split-state-history-', dir=original.parent) as work:
        work = Path(work)
        state_stage, history_stage = work / original.name, work / stem
        scratch = work / 'build'
        scratch.mkdir()
        state_work = scratch / original.name
        split_identity = secrets.token_hex(16)
        copy_history(original, history_stage, split_identity)
        stage_state(original, state_work, history_stage, state_stage, split_identity)
        peak_bytes = sum(path.stat().st_size for path in (state_work, state_stage, history_stage))
        state_work.unlink()
        state_counts = check_integrity(state_stage)
        history_counts = check_integrity(history_stage)
        state_expected = {name: n for name, n in expected.items() if name not in HISTORY_TABLES}
        state_expected.update(pending_account_hold=0, pending_quota_reading_log=0, history_layout=1)
        # This is new state, derived from the shipped history rows rather than copied.
        state_counts.pop('quota_reading_moved')
        require(state_counts == state_expected, 'state table inventory or row-count parity failed')
        require(history_counts == {**{name: expected[name] for name in HISTORY_TABLES}, 'history_binding': 1},
                'history table inventory or row-count parity failed')
        print(f'row-count parity: {len(expected)} tables; {sum(expected.values())} rows', flush=True)
        subprocess.run([args.verifier, str(original), str(state_stage), str(args.now_ms)], check=True)

        for path, namespace, expected_version, binding_table in (
            (state_stage, 'route_state', 101, 'history_layout'),
            (history_stage, 'route_history', 1, 'history_binding'),
        ):
            identity(path)
            check_integrity(path)
            with closing(connect(path, read_only=True)) as conn:
                version = conn.execute('SELECT max(version) FROM cortexkit_schema_version WHERE namespace=?', (namespace,)).fetchone()[0]
                binding = conn.execute(f'SELECT split_identity, move_generation FROM {binding_table} WHERE singleton=1').fetchone()
            require(binding == (split_identity, 0), f'verifier did not preserve initial split identity and generation in {path.name}')
            require(version == expected_version, f'verifier did not persist {namespace} version {expected_version}: {version}')
            sync(path)
        require(identity(original) == before, 'source changed during checks; nothing published')
        for out in (history, backup):
            absent(out)
        # No path is renamed before the checks above. If publication fails before
        # the final atomic replace, the old state is still the authoritative copy.
        published = []
        try:
            os.link(original, backup)
            published.append(backup)
            os.link(history_stage, history)
            published.append(history)
            sync(original.parent)
            os.replace(state_stage, original)
            sync(original.parent)
        except BaseException as error:
            # A signal can land immediately after the atomic replace. Keep the
            # matching archive and rollback link if state has already switched.
            if original.stat().st_ino == before[1]:
                for path in reversed(published):
                    path.unlink()
                raise
            if not isinstance(error, OSError):
                raise
            print(f'split-state-history: published; directory sync failed, run `sync` and verify both files before starting routing: {error}', file=sys.stderr)
    print(f'split complete: state_bytes={original.stat().st_size} history_bytes={history.stat().st_size} '
          f'staging_peak_bytes={peak_bytes} duration_s={time.monotonic() - started:.3f}; original backup: {backup}')
    print('To roll back or restore, stop routing and remove the replaced database -wal and -shm sidecars before replacing either database file.')


parser = argparse.ArgumentParser(description='Split a clean, offline route_state v97 copy; never run on the live store.')
parser.add_argument('state_db')
parser.add_argument('--now-ms', type=int, required=True, help='fixed evaluation clock, in Unix milliseconds')
parser.add_argument('--verifier', required=True, help='path to the built verify_history_split example from this release')
try:
    main(parser.parse_args(sys.argv[2:]))
except (OSError, RuntimeError, sqlite3.Error, subprocess.CalledProcessError) as error:
    print(f'split-state-history: {error}', file=sys.stderr)
    sys.exit(1)
PY
