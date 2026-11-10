#!/bin/sh
# Offline store move. This script never changes daemon configuration or starts modules.
set -eu
export STRIATUM_MOVE_SCRIPT="$0"
exec python3 - "$@" <<'PY'
import argparse
from contextlib import contextmanager
import ctypes
import datetime
import json
import os
from pathlib import Path
import shlex
import sqlite3
import stat
import subprocess
import sys
import tarfile
import time
import uuid


@contextmanager
def timed(label):
    started = time.perf_counter()
    print(f"STEP: {label}", flush=True)
    try:
        yield
    finally:
        print(f"TIMING {label}: {time.perf_counter() - started:.3f}s", flush=True)


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


def file_inventory(directory):
    result = {}
    def refuse_unreadable(error):
        raise error
    for root, directories, files in os.walk(directory, followlinks=False, onerror=refuse_unreadable):
        for name in sorted(directories + files):
            path = Path(root) / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                entry = {"kind": "symlink", "size": metadata.st_size, "target": os.readlink(path)}
            elif stat.S_ISREG(metadata.st_mode):
                entry = {"kind": "file", "size": metadata.st_size}
            elif stat.S_ISDIR(metadata.st_mode):
                entry = {"kind": "directory"}
            else:
                raise RuntimeError(f"unsupported backup member: {path}")
            result[str(path.relative_to(directory))] = entry
    return result


def print_inventory(label, inventory):
    for name, entry in sorted(inventory.items()):
        print(f"{label}: {name}: {json.dumps(entry, sort_keys=True)}", flush=True)


def store_identity(path):
    metadata = path.stat()
    return {"device": metadata.st_dev, "inode": metadata.st_ino, "size": metadata.st_size}


def clone_backup(old, backup, expected):
    # cp -c silently falls back to a full copy on unsupported filesystems.
    # clonefile clones the directory hierarchy in one call and refuses an
    # existing destination; never pre-create it or retry with a copying API.
    try:
        clonefile = ctypes.CDLL(None, use_errno=True).clonefile
    except AttributeError as error:
        raise RuntimeError("clonefile unavailable; use --backup-mode=tar only if a slow copy is acceptable") from error
    clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint32]
    clonefile.restype = ctypes.c_int
    clone_nofollow = 0x0001
    if clonefile(os.fsencode(old), os.fsencode(backup), clone_nofollow) != 0:
        error = ctypes.get_errno()
        raise RuntimeError(f"clonefile refused: {os.strerror(error)}; no copy fallback was attempted")
    if backup.is_symlink() or not backup.is_dir():
        raise RuntimeError("clonefile did not create a real backup directory")
    actual = file_inventory(backup)
    print_inventory("Backup", actual)
    if actual != expected:
        raise RuntimeError("clone backup file listing/size mismatch")
    source, cloned = store_identity(old / "store.db"), store_identity(backup / "store.db")
    if (source["device"], source["inode"]) == (cloned["device"], cloned["inode"]):
        raise RuntimeError("backup store is a hard link, not an independent clone")


def tar_backup(old, backup, expected):
    # Tar is an explicit slow fallback for non-APFS hosts, never an automatic one.
    with backup.open("xb") as output:
        os.chmod(backup, 0o600)
        with tarfile.open(fileobj=output, mode="w") as archive:
            archive.add(old, arcname=old.name)
        output.flush()
        os.fsync(output.fileno())
    actual = {}
    with tarfile.open(backup, "r:") as archive:
        for member in archive.getmembers():
            if member.name == old.name:
                continue
            name = str(Path(member.name).relative_to(old.name))
            if member.issym():
                actual[name] = {"kind": "symlink", "size": len(os.fsencode(member.linkname)), "target": member.linkname}
            elif member.isdir():
                actual[name] = {"kind": "directory"}
            elif member.isfile() or member.islnk():
                actual[name] = {"kind": "file", "size": archive.getmember(member.linkname).size if member.islnk() else member.size}
            else:
                raise RuntimeError(f"unsupported archive member: {member.name}")
    print_inventory("Backup", actual)
    if actual != expected:
        raise RuntimeError("tar backup file listing/size mismatch")


def write_json_exclusive(path, value):
    with path.open("x") as output:
        os.chmod(path, 0o600)
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def verify_full(backup, base, dry_run):
    backup = backup.absolute()
    old, new = base / "prefrontal-routing", base / "striatum"
    with timed("validate offline clone target"):
        if backup.is_symlink() or not backup.is_dir():
            raise RuntimeError("--verify-full requires a recorded backup clone directory, not an archive")
        if backup.resolve() in {old.resolve(), new.resolve()}:
            raise RuntimeError("--verify-full must never target the live or retired store directory")
        record = json.loads(Path(str(backup) + ".manifest.json").read_text())
        if record["backup_mode"] != "clone" or record["backup_path"] != str(backup.resolve()):
            raise RuntimeError("backup manifest does not identify this directory as a clone")
        database = backup / "store.db"
        regular_if_present(database)
        identity = store_identity(database)
        source = record["source_store"]
        if (identity["device"], identity["inode"]) == (source["device"], source["inode"]):
            raise RuntimeError("backup shares the source inode; refusing to open the live store")
    if dry_run:
        print(f"DRY RUN: scan rows and run PRAGMA integrity_check only on private clone {database}; save report beside it")
        print("TIMING offline full verification: 0.000s (dry-run; not executed)")
        return
    report = {"backup_path": str(backup), "scope": "offline private clone only; live store not opened",
              "sqlite_open_mode": "read-write; SQLite may modify WAL/SHM files in this private copy"}
    started = time.perf_counter()
    try:
        # A private clone may need SQLite to recover/checkpoint its own WAL.
        # Neither the running store nor the retired source is opened here.
        connection = connect(database, "rw")
        try:
            with timed("count rows in offline clone"):
                report["row_counts"] = counts(connection)
                print("Clone row counts: " + json.dumps(report["row_counts"], sort_keys=True))
            with timed("integrity_check offline clone"):
                report["integrity_check"] = [row[0] for row in connection.execute("PRAGMA integrity_check")]
                if report["integrity_check"] != ["ok"]:
                    raise RuntimeError(f"INTEGRITY CHECK FAILED: {report['integrity_check']}")
        finally:
            connection.close()
        report["ok"] = True
    except (Exception, KeyboardInterrupt) as error:
        report.update(ok=False, error=str(error))
        raise
    finally:
        report["elapsed_seconds"] = time.perf_counter() - started
        destination = Path(str(backup) + f".verification-{uuid.uuid4().hex}.json")
        with timed("save offline verification report"):
            write_json_exclusive(destination, report)
            print(f"Offline verification report: {destination}", flush=True)


def restore_instructions(base, old, new, backup, mode):
    print("RESTORE: keep BOTH modules stopped and disabled. Do not start either on a partial move.", file=sys.stderr)
    print(f"Preserve/quarantine {old} and {new} before restoring (do not overwrite evidence).", file=sys.stderr)
    if mode == "clone":
        print("After quarantining the partial directories, restore the old directory with: " + shlex.join(
            ["mv", str(backup), str(old)]
        ), file=sys.stderr)
    else:
        print("Restore the old directory with: " + shlex.join(
            ["tar", "-xf", str(backup), "-C", str(base)]
        ), file=sys.stderr)
    print("Verify the restored store, then re-enable only the old module with its old binary.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Move the stopped routing store to striatum; never deploys or restarts.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--backup-mode", choices=("clone", "tar"), default="clone",
                        help="clone (default) requires APFS clonefile support; tar is an explicit slow fallback")
    parser.add_argument("--verify-full", type=Path, metavar="BACKUP_DIRECTORY",
                        help="standalone offline row counts and integrity_check on a recorded private clone only")
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
    started = time.perf_counter()
    try:
        if args.verify_full is not None:
            verify_full(args.verify_full, base, args.dry_run)
            return 0
        with timed("check both modules stopped"):
            for module_id in ("prefrontal-routing", "striatum"):
                stopped(args.ck, module_id)
        with timed("no-overwrite preflight"):
            preflight(old, new)
        print(f"Source: {old}\nDestination: {new}")
        print("Keep both modules stopped and disabled until the fast move and inode/size verification finish.")
        if args.dry_run:
            # Even a read-only SQLite WAL connection can create a -shm file. Do not
            # open SQLite during preview: the dry run must leave every byte alone.
            steps = [
                f"checkpoint {old / 'store.db'} with PRAGMA wal_checkpoint(TRUNCATE); refuse if busy",
                "record source store.db device, inode and size; list every source file with its size",
                f"back up WHOLE {old} to an exclusively created timestamped sibling using "
                + ("forced APFS clonefile; refuse unsupported cloning, never fall back to a copy" if args.backup_mode == "clone"
                   else "tar (explicit slow opt-in)"),
                "compare source and backup file listings/sizes and save a manifest beside the backup",
                "recheck both module statuses, destination conflicts and source store identity",
                f"create {new} with mode 0700 if absent; require the same filesystem",
            ]
            for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
                steps.append(f"link/unlink {old / name} -> {new / name} if present, without replacing anything")
            steps += [
                f"delete {old / 'engram-catalog.json'} in the move step, if present; leave .lease files alone",
                "verify moved store.db has the original device, inode and size; no database scan",
                "on failure keep modules stopped, quarantine partial directories and restore the backup",
            ]
            for step in steps:
                print(f"DRY RUN: {step}")
                print(f"TIMING {step}: 0.000s (dry-run; not executed)")
            print("DRY RUN: optional --verify-full BACKUP_DIRECTORY scans only the clone after service starts")
            print("DRY RUN: no files changed; the module, not this script, writes the new engram catalog at startup")
            return 0

        with timed("checkpoint WAL (TRUNCATE)"):
            connection = connect(old / "store.db", "rw")
            try:
                busy, log, checkpointed = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if busy != 0 or log != checkpointed:
                    raise RuntimeError(f"WAL checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}")
            finally:
                connection.close()
        with timed("record source file sizes and store identity"):
            source_identity = store_identity(old / "store.db")
            inventory = file_inventory(old)
            print_inventory("Source", inventory)
            print("Source store identity: " + json.dumps(source_identity, sort_keys=True))
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = ".tar" if args.backup_mode == "tar" else ""
        backup = base / f"prefrontal-routing-backup-{stamp}-{uuid.uuid4().hex}{suffix}"
        with timed(f"{args.backup_mode} whole-directory backup and listing validation"):
            print(f"Backup: {backup}", flush=True)
            if args.backup_mode == "clone":
                clone_backup(old, backup, inventory)
            else:
                tar_backup(old, backup, inventory)
        validated_backup = True
        with timed("record backup manifest"):
            write_json_exclusive(Path(str(backup) + ".manifest.json"), {
                "backup_mode": args.backup_mode, "backup_path": str(backup.resolve()),
                "source_store": source_identity, "files": inventory,
            })
        restore_instructions(base, old, new, backup, args.backup_mode)
        with timed("recheck stopped modules, conflicts and source identity"):
            for module_id in ("prefrontal-routing", "striatum"):
                stopped(args.ck, module_id)
            preflight(old, new)
            if store_identity(old / "store.db") != source_identity:
                raise RuntimeError("source store identity/size changed after backup")
            new.mkdir(mode=0o700, exist_ok=True)
            if old.stat().st_dev != new.stat().st_dev:
                raise RuntimeError("source and destination must be on the same filesystem for no-replace moves")
        # Link/unlink is a no-replace move on one filesystem. A partial failure
        # leaves the validated backup available; never auto-start or auto-restore.
        with timed("no-replace link/unlink move"):
            for name in ("store.db", "store.db-wal", "store.db-shm", "boot-strikes.json"):
                source, target = old / name, new / name
                if source.exists():
                    print(f"Move {source} -> {target}", flush=True)
                    os.link(source, target)
                    source.unlink()
        # A catalog pointing at the removed store makes Engram permanently
        # degraded. Retire it in the move step; the new module writes its own.
        with timed("retire old Engram catalog"):
            print(f"Delete old catalog: {old / 'engram-catalog.json'}; leave .lease files alone", flush=True)
            (old / "engram-catalog.json").unlink(missing_ok=True)
        with timed("verify moved store inode and size"):
            moved_identity = store_identity(new / "store.db")
            if moved_identity != source_identity:
                raise RuntimeError(f"MOVED STORE IDENTITY/SIZE MISMATCH: before={source_identity}, after={moved_identity}")
            print("Moved store identity: " + json.dumps(moved_identity, sort_keys=True))
        print("Fast verification passed: same device/inode/size; no quick_check, row counts or integrity_check run.")
        if args.backup_mode == "clone":
            print("Optional after service starts: " + shlex.join([os.environ["STRIATUM_MOVE_SCRIPT"], "--verify-full", str(backup)]))
        print("Move complete. Neither module was started; enable only striatum in the coordinated switch-over window.")
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"REFUSED / MOVE FAILED: {error}", file=sys.stderr)
        if args.verify_full is not None:
            print("Offline clone verification failed; live store was not opened. See the saved report if scanning began.", file=sys.stderr)
        elif validated_backup:
            restore_instructions(base, old, new, backup, args.backup_mode)
        else:
            print("No validated backup was completed; no store files were moved. Keep both modules stopped and investigate.", file=sys.stderr)
        return 1
    finally:
        print(f"TIMING total: {time.perf_counter() - started:.3f}s", flush=True)


sys.exit(main())
PY
