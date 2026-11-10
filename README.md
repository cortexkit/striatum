# Striatum

Striatum picks the model and the account for every model call in the CortexKit fleet.

It runs as the `striatum` module under subc, with binary `ck-striatum` and store at `~/.local/share/cortexkit/striatum/store.db` (or `$XDG_DATA_HOME/cortexkit/striatum/store.db`). The crates keep their `prefrontal-routing-*` names until a later change.

Settings live at `$XDG_CONFIG_HOME/cortexkit/striatum.jsonc` (default `~/.config/cortexkit/striatum.jsonc`); project and workspace tier settings use `.cortexkit/striatum.jsonc` under each root. In each directory, routing falls back to `prefrontal-routing.jsonc` if the new file is absent and logs a rename reminder. If both exist, only `striatum.jsonc` is read and the old file is logged as ignored. Global config readers and live reload keep the startup choice until restart, so restart after moving the fallback file to the new name. `--check-config` validates and names the same selected global file.

## Build and test

```sh
cargo build --locked --release -p prefrontal-routing-module
bash scripts/rust-test-gate.sh
```

The gate needs Rust 1.99.0, Python 3.10 or newer, and cargo-nextest. It runs the source checks and the whole test suite, with no sibling checkout.

Six real-daemon tests are ignored by default because they build `../subconscious` and `../insula`, with `../commons` beside them. Run them on a machine that has those checkouts:

```sh
cargo nextest run --locked -p prefrontal-routing-module --test it --run-ignored only \
  -E 'test(/^(both_modules_e2e|real_daemon_e2e)::/)'
```

## Deploy

The identity switch is a coordinated window: stop and disable `prefrontal-routing`
before enabling `striatum`; never run both routing providers together. While both
are stopped, preview `script/move-store-to-striatum.sh --dry-run`, then run it
without the flag. It backs up the old data directory, moves the store and boot
ledger, removes the old engram catalog, and checks row counts and integrity. The
new module writes its own catalog at startup. `--base-dir` overrides the CortexKit
data directory for rehearsal; never rehearse against the live data directory.
The script does not stop, start, enable or deploy either module. Keep them stopped
throughout the move; on failure follow its printed restore instructions.

```sh
sh script/deploy-module.sh routing
```

This builds, signs and places the binary, checks the settings file, and restarts the module. It stops if the running process doesn't serve the placed file or the module doesn't report healthy. It needs the installed `ck` and `subc-probe` tools.

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md) and the [routing specification](docs/specs/model-routing.md).
