# Architecture (draft)

Striatum owns model and account selection for CortexKit. Its supervised
`ck-prefrontal-routing` process serves `route.*` operations and the `routing/v1`
role under the unchanged `prefrontal-routing` module id.

- **Pure policy:** `prefrontal-routing-core` implements model scoring,
  eligibility, quota and account allocation without transport or SQL.
- **Persistence:** `prefrontal-routing-store` implements the route-state store
  over the shared CortexKit storage layer and SQLite. Routing owns
  `~/.local/share/cortexkit/prefrontal-routing/store.db` and its migrations.
- **Composition:** `prefrontal-routing-module` serves the wire contract and
  refreshes the inputs policy needs: the model catalog from Fusiform,
  credentials from Claustrum, quota from Insula and project settings through
  Entorhinal. The subc daemon supervises the process.

Consumers ask routing for models and accounts; they do not duplicate its
selection policy. The three crates build without any prefrontal crate.
Published wire, storage and test-support dependencies remain pinned by the
imported lockfile. Names and persistent paths are deliberately unchanged;
renaming is a separate change.

Consumer wire captures live in crate-owned fixtures, while routing's published
sample replies live under `test-vectors/`. Prefrontal's host health monitor
relays routing notices using the prefix pinned in
`crates/prefrontal-routing-module/fixtures/sentinel-notice-prefix.json`. Tests
check this captured consumer contract, not another repository's live source.

[TODO(user): Confirm how much detail about upstream modules and consumer
ownership belongs in this overview.]
