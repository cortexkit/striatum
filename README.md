# Striatum

Striatum picks the model and the account for every model call in the CortexKit fleet.

It runs as the `prefrontal-routing` module under subc. The crates, the `ck-prefrontal-routing` binary, the `prefrontal-routing.jsonc` settings file and the store at `~/.local/share/cortexkit/prefrontal-routing/store.db` keep their old names until a later rename.

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

```sh
sh script/deploy-module.sh routing
```

This builds, signs and places the binary, checks the settings file, and restarts the module. It stops if the running process doesn't serve the placed file or the module doesn't report healthy. It needs the installed `ck` and `subc-probe` tools.

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md) and the [routing specification](docs/specs/model-routing.md).
