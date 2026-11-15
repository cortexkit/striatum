# Striatum

Striatum picks the model and the account for every model call in the CortexKit fleet.

It runs as the `striatum` module under subc, with binary `ck-striatum` and store at `~/.local/share/cortexkit/striatum/store.db` (or `$XDG_DATA_HOME/cortexkit/striatum/store.db`). Its crates are `striatum-core`, `striatum-store` and `striatum-module`.

Settings live at `$XDG_CONFIG_HOME/cortexkit/striatum.jsonc` (default `~/.config/cortexkit/striatum.jsonc`); project and workspace tier settings use `.cortexkit/striatum.jsonc` under each root. Routing reads only `striatum.jsonc`, with no fallback to older filenames. `--check-config` validates and names the same global file. A file absent on its first read means defaults (or no tier settings). Once a settings file has loaded successfully, moving or deleting it keeps the last good configuration and reports the missing file once until it is restored. Malformed global edits also keep the last good configuration; replace a file with an empty JSON object (`{}`) to intentionally clear its settings.

Tracing warnings and errors go to stderr, one event per line, as `LEVEL message key=value`.
The daemon adds timestamps and the module prefix. The default threshold is `warn`.
`STRIATUM_LOG` overrides `RUST_LOG`; either accepts comma-separated global levels
(`off`, `error`, `warn`, `info`, `debug`, `trace`) and `target=level` directives, such
as `warn,striatum_store=debug,ck_striatum=info`. The longest target prefix wins;
invalid directives are ignored. Span lifecycle events and span fields are not printed.
Existing operator messages, including the `--check-config` success report, are unchanged.

## Build and test

```sh
cargo build --locked --release -p striatum-module
bash scripts/rust-test-gate.sh
```

The `rust-test-gate.sh` script needs Rust 1.100.0, Python 3.10 or newer, and cargo-nextest. It runs the source checks and the whole test suite, with no sibling checkout.

Six real-daemon tests are ignored by default because they build `../subconscious` and `../insula`, with `../commons` beside them. Run them on a machine that has those checkouts:

```sh
cargo nextest run --locked -p striatum-module --test it --run-ignored only \
  -E 'test(/^(both_modules_e2e|real_daemon_e2e)::/)'
```

## Deploy

The identity switch is a coordinated window: stop and disable `prefrontal-routing`
before enabling `striatum`; never run both routing providers together. While both
are stopped, preview `script/move-store-to-striatum.sh --dry-run`, then run it
without the flag. It backs up the old data directory, moves the store and boot
ledger, and removes the old engram catalog. The new module writes its own catalog
at startup. The script does not stop, start, enable or deploy either module. Keep
them stopped throughout the move; on failure follow its printed restore instructions.

The default backup is an APFS copy-on-write clone of the **whole** old directory
to an exclusively created timestamped sibling. It uses `clonefile(2)` directly:
`cp -cR` can silently fall back to a full copy when cloning is unsupported, but
the script refuses instead. It compares every backup file's path/type/size with
the source and records the listing beside the clone. `--backup-mode=tar` opts
into a slow, uncompressed tar backup for non-APFS hosts; it is never an automatic
fallback. `--base-dir` overrides the CortexKit data directory for rehearsal;
never rehearse against the live data directory.

The outage path checkpoints the WAL with `TRUNCATE` (refusing if busy), performs
same-filesystem no-overwrite link/unlink moves, and verifies that the moved
`store.db` retains its source device, inode and size. It does **not** scan rows or
run `quick_check`/`integrity_check` by default. Every step prints elapsed seconds.
After `striatum` is serving, optional full confirmation runs separately:

```sh
script/move-store-to-striatum.sh --verify-full /path/to/prefrontal-routing-backup-TIMESTAMP-ID
```

This mode only opens the recorded private clone, never the live store, and does
not require either module to be stopped. It counts every application table and
runs `PRAGMA integrity_check`, saving a report beside the clone. SQLite opens the
private copy read-write so it can recover its own WAL/SHM files if needed; the
report states that fact. Counts describe the frozen backup, not the now-changing
live store. A tar archive must be restored separately; `--verify-full` accepts
only a recorded clone directory. `--dry-run` previews either mode without
opening SQLite or writing files (the move preview still checks module status).

### Compacting the store

The retention pass deletes old rows and compacts old decisions' scoring detail
in place, which frees pages inside `store.db` but never shrinks the file.
`script/compact-store.sh` rebuilds it offline: with `striatum` stopped and
outside the nightly backup capture by Engram, CortexKit's backup module, which
reads `store.db` live between 10:45 and 12:00 UTC (the script refuses that
window, so a capture never sees a half-swapped store),
it checkpoints the WAL, runs `VACUUM INTO` a sibling file, checks the copy
(`quick_check`, an empty `foreign_key_check`, the same schema, `user_version`
and row counts, a smaller `page_count`), puts it in WAL mode, and swaps it in by
rename. The previous file stays beside it as `store.db.pre-compact-TIMESTAMP`
with the restore command printed. `--dry-run` previews the steps without opening
SQLite; `--store-dir` and `--ck` point it at a copy and a stub for rehearsal.
Let the retention pass finish its backlog first: on the first deployment it
compacts about 500 decisions a minute.

```sh
sh script/deploy-module.sh routing
```

This builds, signs and places the binary, checks the settings file, and restarts the module. It stops if the running process doesn't serve the placed file or the module doesn't report healthy. It needs the installed `ck` and `subc-probe` tools.

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md) and the [routing specification](docs/specs/model-routing.md).
