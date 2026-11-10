# Structure

A Rust workspace of three crates. They keep their `prefrontal-routing-*` names for now, while the module registers as `striatum` and runs as `ck-striatum`. This file is a map: search the code for a specific symbol.

```text
crates/
  prefrontal-routing-core/     selection policy and domain types (no SQL, no wire)
  prefrontal-routing-store/    SQLite store and numbered migrations
  prefrontal-routing-module/   ck-striatum binary and wire handlers
docs/specs/model-routing.md    routing policy specification
test-vectors/route-*/          reply samples routing publishes for its consumers
scripts/                       source checks and rust-test-gate.sh
script/                        deploy, signing and offline store-move scripts
.cargo/                        SQLite build flag and the macOS test-binary signer
.config/nextest.toml           test timeouts and real-daemon test limits
```

## Entry point

`crates/prefrontal-routing-module/src/main.rs` registers the operations, loads settings and starts serving. It is very large; search it.

## Where to add new code

- **Wire operation:** a handler in `prefrontal-routing-module` and its entry in the module manifest in `main.rs`.
- **Policy change:** `prefrontal-routing-core`, with a test beside it.
- **Migration:** the next free number in `crates/prefrontal-routing-store/migrations`, rehearsed on a copy of the live store before deploy.
- **Integration test:** a file in the crate's shared `tests/it` binary with a `mod` line in its `main.rs`. The model-router suite keeps its own process because it changes process-wide state. `scripts/test-binary-fold.py` refuses other standalone test binaries.

## Conventions

- Tests take scratch paths from `cortexkit-test-support` and never touch live stores or `$HOME`.
- Fixtures copied from other repositories record their source in `crates/prefrontal-routing-module/fixtures/IMPORTED.md`.
- Real-daemon tests are ignored by default; they build sibling checkouts by relative path.
