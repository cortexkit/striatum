# Structure

A Rust workspace of three `striatum-*` crates. The module registers as `striatum` and runs as `ck-striatum`. This file is a map: search the code for a specific symbol.

```text
crates/
  striatum-core/               selection policy and domain types (no SQL, no wire)
  striatum-store/              SQLite store and numbered migrations
  striatum-module/             ck-striatum binary and wire handlers
docs/specs/model-routing.md    routing policy specification
test-vectors/route-*/          reply samples routing publishes for its consumers
scripts/                       source checks and rust-test-gate.sh
scripts/experiments/           offline studies over a read-only store (calibration replay)
script/                        optional deploy, signing, store-move and compaction tooling
.cargo/                        SQLite build flag and the macOS test-binary signer
.config/nextest.toml           test timeouts and real-daemon test limits
```

## Entry point

`crates/striatum-module/src/main.rs` registers the operations, loads settings and starts serving. It is very large; search it.

## Where to add new code

- **Wire operation:** a handler in `striatum-module` and its entry in the module manifest in `main.rs`.
- **Policy change:** `striatum-core`, with a test beside it.
- **Migration:** the next free number in `crates/striatum-store/migrations`, rehearsed on a disposable copy of the store before deploy.
- **Integration test:** a file in the crate's shared `tests/it` binary with a `mod` line in its `main.rs`. The model-router suite keeps its own process because it changes process-wide state. `scripts/test-binary-fold.py` refuses other standalone test binaries.

## Conventions

- Hand-written source files stay under 5,000 lines; tests and generated files are exempt, and larger existing files must not grow.
- Tests take temporary paths from `cortexkit-test-support` and never touch live stores or `$HOME`.
- Fixtures copied from other repositories record their source in `crates/striatum-module/fixtures/IMPORTED.md`.
- Real-daemon tests are optional and ignored by default; they build sibling checkouts by relative path.
