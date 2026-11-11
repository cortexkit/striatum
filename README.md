# Striatum

Striatum picks the model and the account for every model call in the CortexKit fleet.

It runs as the `striatum` module under subc, with binary `ck-striatum` and store at `~/.local/share/cortexkit/striatum/store.db` (or `$XDG_DATA_HOME/cortexkit/striatum/store.db`). Its crates are `striatum-core`, `striatum-store` and `striatum-module`.

Settings live at `$XDG_CONFIG_HOME/cortexkit/striatum.jsonc` (default `~/.config/cortexkit/striatum.jsonc`); project and workspace tier settings use `.cortexkit/striatum.jsonc` under each root. Routing reads only `striatum.jsonc`, with no fallback to older filenames. `--check-config` validates and names the same global file. A file absent on its first read means defaults (or no tier settings). Once a settings file has loaded successfully, moving or deleting it keeps the last good configuration and reports the missing file once until it is restored. Malformed global edits also keep the last good configuration; replace a file with an empty JSON object (`{}`) to intentionally clear its settings.

## Build and test

```sh
cargo build --locked --release -p striatum-module
bash scripts/rust-test-gate.sh
```

The gate needs Rust 1.99.0, Python 3.10 or newer, and cargo-nextest. It runs the source checks and the whole test suite, with no sibling checkout.

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

```sh
sh script/deploy-module.sh routing
```

This builds, signs and places the binary, checks the settings file, and restarts the module. It stops if the running process doesn't serve the placed file or the module doesn't report healthy. It needs the installed `ck` and `subc-probe` tools.

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md) and the [routing specification](docs/specs/model-routing.md).
