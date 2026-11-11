#!/bin/sh
# Offline compaction of the routing store: VACUUM INTO a new file, verify it,
# then swap it in. Run it only with the striatum module stopped, and outside
# Engram's nightly capture (10:45-12:00 UTC). This script never starts,
# stops or reconfigures a module.
#
# Why: the retention pass deletes old rows and compacts old decision detail in
# place, which frees pages inside store.db but never shrinks the file. Only a
# rebuild returns that space to the disk.
set -eu
export STRIATUM_COMPACT_SCRIPT="$0"
exec python3 - "$@" <<'PY'
import argparse
from contextlib import contextmanager
import datetime
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time

CAPTURE_START = datetime.time(10, 45)
CAPTURE_END = datetime.time(12, 0)
MODULE_ID = "striatum"


@contextmanager
def timed(label):
    started = time.perf_counter()
    print(f"STEP: {label}", flush=True)
    try:
        yield
    finally:
        print(f"TIMING {label}: {time.perf_counter() - started:.3f}s", flush=True)


def stopped(ck):
    command = [str(ck), "module", "status", MODULE_ID, "--json"]
    print("Check stopped: " + shlex.join(command), flush=True)
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=15)
    status = json.loads(result.stdout)
    module = status.get("module")
    if not (
        isinstance(module, dict)
        and module.get("module_id") == MODULE_ID
        and module.get("live") is False
        and module.get("state") in {"disabled", "stopped"}
    ):
        raise RuntimeError(f"{MODULE_ID} did not affirmatively report stopped/disabled and not live")


def outside_capture_window(now):
    clock = now.astimezone(datetime.timezone.utc).time()
    if CAPTURE_START <= clock < CAPTURE_END:
        raise RuntimeError(
            f"it is {clock.strftime('%H:%M')}Z, inside Engram's nightly capture "
            f"({CAPTURE_START.strftime('%H:%M')}-{CAPTURE_END.strftime('%H:%M')}Z); run it later"
        )


def regular(path):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"refusing missing or non-regular file: {path}")


def connect(path, mode):
    return sqlite3.connect(path.as_uri() + f"?mode={mode}", uri=True, timeout=0)


def pragma(connection, name):
    return connection.execute(f"PRAGMA {name}").fetchone()[0]


def inventory(connection):
    tables = [
        name
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    if not tables:
        raise RuntimeError("store has no application tables")
    counts = {
        name: connection.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
        for name in tables
    }
    schema = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    return {
        "page_size": pragma(connection, "page_size"),
        "page_count": pragma(connection, "page_count"),
        "freelist_count": pragma(connection, "freelist_count"),
        "user_version": pragma(connection, "user_version"),
        "row_counts": counts,
        "schema": schema,
    }


def foreign_key_violations(connection):
    return sorted(tuple(row) for row in connection.execute("PRAGMA foreign_key_check"))


def fsync(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def summary(label, facts, size):
    print(
        f"{label}: {size / 2**30:.2f} GiB on disk, page_size {facts['page_size']}, "
        f"page_count {facts['page_count']}, freelist_count {facts['freelist_count']}, "
        f"user_version {facts['user_version']}",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compact the stopped routing store with VACUUM INTO, verify, then swap; never restarts it."
    )
    data_home = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local/share")
    parser.add_argument("--store-dir", type=Path, default=data_home / "cortexkit/striatum",
                        help="directory holding store.db (override only for rehearsal on a copy)")
    parser.add_argument("--ck", type=Path, default=Path.home() / ".local/share/cortexkit/bin/ck",
                        help="ck executable (override with a stub for rehearsal)")
    parser.add_argument("--dry-run", action="store_true",
                        help="check preconditions and print the steps; open no database")
    parser.add_argument("--allow-existing-fk-violations", action="store_true",
                        help="accept foreign_key_check rows in the compacted copy when the source has exactly the same rows")
    args = parser.parse_args()
    store_dir = args.store_dir.absolute()
    args.ck = args.ck.absolute()
    source = store_dir / "store.db"
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    compacted = store_dir / f"store.db.compact-{stamp}"
    kept = store_dir / f"store.db.pre-compact-{stamp}"
    started = time.perf_counter()
    swapped = False
    try:
        with timed("check module stopped and capture window"):
            outside_capture_window(datetime.datetime.now(datetime.timezone.utc))
            stopped(args.ck)
        with timed("no-overwrite preflight"):
            if store_dir.is_symlink() or not store_dir.is_dir():
                raise RuntimeError(f"refusing missing or symlinked store directory: {store_dir}")
            regular(source)
            for path in (compacted, kept):
                if path.exists() or path.is_symlink():
                    raise RuntimeError(f"{path} already exists; nothing will be overwritten")
        if args.dry_run:
            for step in (
                f"checkpoint {source} with PRAGMA wal_checkpoint(TRUNCATE); refuse if busy",
                "record page_size, page_count, freelist_count, user_version, schema and every table's row count",
                f"VACUUM INTO {compacted} (same directory, so the swap is a rename)",
                "verify the copy: quick_check ok, foreign_key_check empty, same schema, user_version "
                "and row counts, fewer pages, journal_mode set to WAL",
                f"rename {source} to {kept}, move any -wal/-shm beside it, rename the copy to {source}",
                "reopen store.db and check page_count and journal_mode",
            ):
                print(f"DRY RUN: {step}")
            print("DRY RUN: no files changed")
            return 0

        with timed("checkpoint WAL (TRUNCATE)"):
            connection = connect(source, "rw")
            try:
                busy, log, checkpointed = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if busy != 0 or log != checkpointed:
                    raise RuntimeError(
                        f"WAL checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}"
                    )
                with timed("record source facts and row counts"):
                    before = inventory(connection)
                    summary("Source", before, source.stat().st_size)
                    print("Source row counts: " + json.dumps(before["row_counts"], sort_keys=True), flush=True)
                with timed("VACUUM INTO compacted copy"):
                    connection.execute("VACUUM INTO ?", (str(compacted),))
            finally:
                connection.close()

        with timed("verify compacted copy"):
            fsync(compacted)
            connection = connect(compacted, "rw")
            try:
                check = [row[0] for row in connection.execute("PRAGMA quick_check")]
                if check != ["ok"]:
                    raise RuntimeError(f"quick_check failed on the copy: {check[:20]}")
                violations = foreign_key_violations(connection)
                if violations:
                    if not args.allow_existing_fk_violations:
                        raise RuntimeError(
                            f"foreign_key_check reported {len(violations)} rows in the copy, e.g. {violations[:5]}; "
                            "rerun with --allow-existing-fk-violations to accept them if the source has the same rows"
                        )
                    source_connection = connect(source, "ro")
                    try:
                        if foreign_key_violations(source_connection) != violations:
                            raise RuntimeError("foreign_key_check differs between the source and the copy")
                    finally:
                        source_connection.close()
                    print(f"WARNING: {len(violations)} foreign_key_check rows, identical in the source", flush=True)
                after = inventory(connection)
                summary("Compacted", after, compacted.stat().st_size)
                for key in ("row_counts", "schema", "user_version", "page_size"):
                    if after[key] != before[key]:
                        raise RuntimeError(f"the copy's {key} differs from the source")
                if after["page_count"] >= before["page_count"]:
                    raise RuntimeError("the copy is not smaller than the source; nothing to reclaim")
                mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode != "wal":
                    raise RuntimeError(f"could not put the copy in WAL mode (journal_mode={mode})")
            finally:
                connection.close()
            fsync(compacted)

        with timed("swap"):
            # Recheck right before touching store.db: nothing may have started it.
            stopped(args.ck)
            os.rename(source, kept)
            swapped = True
            for suffix in ("-wal", "-shm"):
                side = store_dir / f"store.db{suffix}"
                if side.exists():
                    os.rename(side, store_dir / f"{kept.name}{suffix}")
            os.rename(compacted, source)
            fsync(store_dir)

        with timed("verify swapped store"):
            # Read-write: a read-only open of a WAL database cannot create the
            # -shm file it needs. The module is stopped, so nothing else opens it.
            connection = connect(source, "rw")
            try:
                if pragma(connection, "page_count") != after["page_count"]:
                    raise RuntimeError("store.db after the swap is not the verified copy")
                if pragma(connection, "journal_mode") != "wal":
                    raise RuntimeError("store.db after the swap is not in WAL mode")
            finally:
                connection.close()
        print(f"Compaction complete: {before['page_count']} -> {after['page_count']} pages.")
        print(f"The previous store is kept at {kept}; delete it once striatum has run cleanly.")
        print("Restore, with striatum stopped: " + shlex.join(["mv", str(kept), str(source)]))
        print("The module was not started; enable it when ready.")
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"REFUSED / COMPACTION FAILED: {error}", file=sys.stderr)
        if swapped:
            print("The swap began. Keep striatum stopped; restore the previous store with: "
                  + shlex.join(["mv", str(kept), str(source)]), file=sys.stderr)
        else:
            print("store.db was not changed.", file=sys.stderr)
            if compacted.exists():
                print(f"Remove the unverified copy: {shlex.join(['rm', str(compacted)])}", file=sys.stderr)
        return 1
    finally:
        print(f"TIMING total: {time.perf_counter() - started:.3f}s", flush=True)


sys.exit(main())
PY
