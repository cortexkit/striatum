# Structure (draft)

```text
crates/
  prefrontal-routing-core/     pure model/account policy and domain types
  prefrontal-routing-store/    SQLite persistence and numbered migrations
  prefrontal-routing-module/   ck-prefrontal-routing binary and wire handlers
docs/specs/model-routing.md    routing policy specification
test-vectors/route-*/          routing-produced account/catalog/head samples
scripts/                      source checks and rust-test-gate.sh
script/                       macOS deployment and signing gates
.cargo/                       SQLite build setting and macOS test runner
.config/nextest.toml           test timeouts and daemon concurrency limits
```

Start at `crates/prefrontal-routing-module/src/main.rs` for registration,
configuration and serving. The crate manifests retain edition 2021, and the
root manifest supplies their shared dependencies and build profiles.

Integration suites are folded into `tests/it/main.rs`; the model-router suite
keeps its own process because it changes process-global state. Test scratch
and daemon staging use the published `cortexkit-test-support` helpers. The
source gates reject unreviewed standalone suites, unreachable production
modules, raw scratch constructors and unsafe process-helper bypasses.

`crates/prefrontal-routing-module/fixtures/IMPORTED.md` records the provenance of
frozen consumer fixtures copied from the former workspace. Tests never need
to read another prefrontal crate. Real-daemon integration tests are ignored by
default and retain sibling-checkout resolution for subconscious and Insula;
the standard standalone gate does not build those siblings.

[TODO(user): Review the navigation and testing conventions before treating
these draft pages as contributor guidance.]
