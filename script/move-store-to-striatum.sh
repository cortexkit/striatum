#!/bin/sh
# Offline store move. This script never changes daemon configuration or starts modules.
set -eu
exec python3 - "$@" <<'PY'
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import tarfile
import uuid


def stopped(ck, module_id):
    command = [str(ck), "module", "status", module_id, "--json"]
    print("Check stopped: " + shlex.join(command), flush=True)
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=15)
    status = json.loads(result.stdout)
    module = status.get("module")
    if not (
        isinstance(module, dict)
        and module.get("module_id") == module_id
        and module.get("live") is False
        and "catalog" in status
        and status["catalog"] is None
        and module.get("state") in {"disabled", "stopped"}
    ):
        raise RuntimeError(f"{module_id} did not affirmatively report stopped/disabled and not live")


def regular_if_present(path):
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise RuntimeError(f"refusing non-regular file: {path}")


def preflight(old, new):
    for directory in (old.parent, old, new):
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise RuntimeError(f"refusing non-directory or symlink: {directory}")
    if not (old / "store.db").is_file():
        raise RuntimeError(f"source store is missing: {old / 'store.db'}")
    for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
        regular_if_present(old / name)
        target = new / name
        if target.exists() or target.is_symlink():
            raise RuntimeError(f"destination already holds {target}; nothing will be overwritten")
    regular_if_present(old / "engram-catalog.json")


def counts(connection):
    tables = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    if not tables:
        raise RuntimeError("store has no application tables")
    return {
        name: connection.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
        for (name,) in tables
    }


def connect(path, mode):
    return sqlite3.connect(path.as_uri() + f"?mode={mode}", uri=True, timeout=0)


def digest(stream):
    value = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        value.update(block)
    return value.digest()


def backup_directory(old, backup):
    # Exclusive creation protects earlier backups, including two runs in one second.
    with backup.open("xb") as output:
        os.chmod(backup, 0o600)
        with tarfile.open(fileobj=output, mode="w:gz") as archive:
            archive.add(old, arcname=old.name)
        output.flush()
        os.fsync(output.fileno())
    with tarfile.open(backup, "r:gz") as archive:
        archive.getmember(f"{old.name}/store.db")
        for member in archive.getmembers():
            if member.isfile():
                with archive.extractfile(member) as saved:
                    saved_hash = digest(saved)
                with (old.parent / member.name).open("rb") as original:
                    original_hash = digest(original)
                if saved_hash != original_hash:
                    raise RuntimeError(f"backup validation mismatch: {member.name}")


def restore_instructions(base, old, new, backup):
    print("RESTORE: keep BOTH modules stopped and disabled. Do not start either on a partial move.", file=sys.stderr)
    print(f"Preserve/quarantine {old} and {new} before restoring (do not overwrite evidence).", file=sys.stderr)
    print("Restore the old directory with: " + shlex.join(
        ["tar", "-xzf", str(backup), "-C", str(base)]
    ), file=sys.stderr)
    print("Verify the restored store, then re-enable only the old module with its old binary.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Move the stopped routing store to striatum; never deploys or restarts.")
    parser.add_argument("--dry-run", action="store_true")
    data_home = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local/share")
    parser.add_argument("--base-dir", type=Path, default=data_home / "cortexkit",
                        help="CortexKit data directory (override only for rehearsal/custom data homes)")
    parser.add_argument("--ck", type=Path, default=Path.home() / ".local/share/cortexkit/bin/ck",
                        help="ck executable (override with a stub for rehearsal)")
    args = parser.parse_args()
    base = args.base_dir.absolute()
    old, new = base / "prefrontal-routing", base / "striatum"
    backup = None
    validated_backup = False
    try:
        for module_id in ("prefrontal-routing", "striatum"):
            stopped(args.ck, module_id)
        preflight(old, new)
        print(f"Source: {old}\nDestination: {new}")
        print("Keep both modules stopped and disabled until the entire move and verification finish.")
        if args.dry_run:
            # Even a read-only SQLite WAL connection can create a -shm file. Do not
            # open SQLite during preview: the dry run must leave every byte alone.
            print(f"DRY RUN: checkpoint {old / 'store.db'} with PRAGMA wal_checkpoint(TRUNCATE); refuse if busy")
            print("DRY RUN: count rows in every application table in sqlite_schema; print and record counts beside the backup")
            print(f"DRY RUN: tar the WHOLE {old} directory to an exclusive timestamped backup in {base}")
            print("DRY RUN: read every archived file and compare SHA-256 with its source")
            print("DRY RUN: recheck both module statuses and all destination conflicts")
            print(f"DRY RUN: create {new} with mode 0700 if absent; require the same filesystem")
            for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
                print(f"DRY RUN: move {old / name} -> {new / name} if present, without replacing anything")
            print(f"DRY RUN: delete {old / 'engram-catalog.json'} in the move step, if present; leave .lease files alone")
            print("DRY RUN: recount every table; compare with recorded counts; run PRAGMA integrity_check (must be ok)")
            print("DRY RUN: on any failure keep modules stopped, quarantine partial directories and restore the tar backup")
            print("DRY RUN: no files changed; the module, not this script, writes the new engram catalog at startup")
            return 0

        print("Checkpoint SQLite WAL before backing up or moving", flush=True)
        with connect(old / "store.db", "rw") as connection:
            busy, log, checkpointed = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if busy != 0 or log != checkpointed:
                raise RuntimeError(f"WAL checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}")
            before = counts(connection)
        connection.close()
        print("Before row counts: " + json.dumps(before, sort_keys=True), flush=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = base / f"prefrontal-routing-backup-{stamp}-{uuid.uuid4().hex}.tar.gz"
        print(f"Back up whole directory and validate every archived file: {backup}", flush=True)
        backup_directory(old, backup)
        validated_backup = True
        with Path(str(backup) + ".rows.json").open("x") as record:
            os.chmod(record.name, 0o600)
            json.dump(before, record, indent=2, sort_keys=True)
            record.write("\n")
            record.flush()
            os.fsync(record.fileno())
        restore_instructions(base, old, new, backup)
        for module_id in ("prefrontal-routing", "striatum"):
            stopped(args.ck, module_id)
        preflight(old, new)
        new.mkdir(mode=0o700, exist_ok=True)
        if old.stat().st_dev != new.stat().st_dev:
            raise RuntimeError("source and destination must be on the same filesystem for no-replace moves")
        # Link/unlink is a no-replace move on one filesystem. A partial failure
        # leaves the validated backup available; never auto-start or auto-restore.
        for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
            source, target = old / name, new / name
            if source.exists():
                print(f"Move {source} -> {target}", flush=True)
                os.link(source, target)
                source.unlink()
        # A catalog pointing at the removed store makes Engram permanently
        # degraded. Retire it in the move step; the new module writes its own.
        print(f"Delete old catalog: {old / 'engram-catalog.json'}; leave .lease files alone", flush=True)
        (old / "engram-catalog.json").unlink(missing_ok=True)
        with connect(new / "store.db", "ro") as connection:
            after = counts(connection)
            integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        connection.close()
        print("After row counts: " + json.dumps(after, sort_keys=True))
        if before != after:
            raise RuntimeError(f"ROW COUNT MISMATCH: before={before}, after={after}")
        if integrity != ["ok"]:
            raise RuntimeError(f"INTEGRITY CHECK FAILED: {integrity}")
        print(f"PRAGMA integrity_check: ok; all row counts match. Backup: {backup}")
        print("Move complete. Neither module was started; enable only striatum in the coordinated switch-over window.")
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"REFUSED / MOVE FAILED: {error}", file=sys.stderr)
        if validated_backup:
            restore_instructions(base, old, new, backup)
        else:
            print("No validated backup was completed; no store files were moved. Keep both modules stopped and investigate.", file=sys.stderr)
        return 1


sys.exit(main())
PY
