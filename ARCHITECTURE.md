# Architecture

Striatum decides which model and which account serve each model call in a CortexKit install. It registers as `striatum`, running `ck-striatum` under the `ck` daemon, which supervises modules and routes calls between them. It serves the `routing/v1` role and its `route.*` operations. Its default store is `$XDG_DATA_HOME/cortexkit/striatum/store.db`, with data home defaulting to `~/.local/share`.

## Terms

- **Consumer:** a module asking for a model or an account, such as a runner starting a session or the agent runtime launching a worker. Each request names its consumer kind and its demands (reasoning and judgment levels, tools, context size).
- **Decision:** routing's answer to one selection: the chosen route (provider and model), the reasons, and an id the consumer cites in later calls.
- **Admission:** routing's grant of one account for one run under a decision. The runner asks for it through `route.account_for` when it starts sending.
- **Report:** what the consumer or runner tells routing after a run: outcome, token use, timings, and limits the provider returned.

## Inputs

Routing owns no model facts and no credentials. It reads them from the modules that do:

- **Fusiform (required for normal routing):** which models exist, their prices, limits, capabilities and published grades.
- **Claustrum (required for normal routing):** which credentials exist and which providers each serves (metadata only, never the secret).
- **Insula (optional):** quota readings for each account.
- **Astrocyte (optional):** measured values for subscription plans.
- **The daemon:** the caller's attested project identity and managed workspace/project settings. Routing does not use a separate project-settings module or read project files itself.
- **Runners:** which models and service tiers each runner can serve, from its `role.describe`.

The CortexKit bus is an optional transport for quota notices. Catalog and credential inputs are necessary for operations that depend on them, but companion startup ordering does not gate `ck-striatum` readiness.

## Crates

The three crates share the `striatum-*` prefix with the runtime module.

- `striatum-core`: the selection policy (scoring, eligibility, quota pressure, account allocation), with no SQL and no transport.
- `striatum-store`: routing's SQLite store and its migrations. Routing alone opens it.
- `striatum-module`: the binary. It serves the wire contract, refreshes the inputs above, and writes decisions, admissions and reports to the store.

## Rules

- Consumers never repeat routing's policy. They ask, and they act on the answer.
- A refusal names its reason and, when known, when to retry (`reset_at_ms` or `retry_after`), so the consumer can wait instead of failing.
- Account walls (an exhausted account, a refused credential) live in routing's store and outlast restarts.
- Wire samples from consumers sit in crate fixtures with their source recorded in `crates/striatum-module/fixtures/IMPORTED.md`. Routing's own reply samples sit in `test-vectors/`.
