# Architecture

Striatum decides which model and which account serve each model call in the CortexKit fleet. It runs as one supervised module, `ck-prefrontal-routing`, under subc, the daemon that supervises modules and routes calls between them. It serves the `routing/v1` role and its `route.*` operations.

## Terms

- **Consumer:** a module asking for a model or an account, such as a runner starting a session or prefrontal launching a worker. Each request names its consumer kind (mason, sidekick, head, panel seat and so on) and its demands (reasoning and judgment levels, tools, context size).
- **Decision:** routing's answer to one selection: the chosen route (provider and model), the reasons, and an id the consumer cites in later calls.
- **Admission:** routing's grant of one account for one run under a decision. The runner asks for it through `route.account_for` when it starts sending.
- **Report:** what the consumer or runner tells routing after a run: outcome, token use, timings, and limits the provider returned.

## Inputs

Routing owns no model facts and no credentials. It reads them from the modules that do:

- **Fusiform:** which models exist, their prices, limits and capabilities.
- **Claustrum:** which credentials exist and which providers each serves (metadata only, never the secret).
- **Insula:** quota readings for each account.
- **Entorhinal:** which project a request belongs to, so per-project settings apply.
- **Runners:** which models and service tiers each runner can serve, from its `role.describe`.

## Crates

- `prefrontal-routing-core`: the selection policy (scoring, eligibility, quota pressure, account allocation), with no SQL and no transport.
- `prefrontal-routing-store`: routing's SQLite store and its migrations. Routing alone opens it.
- `prefrontal-routing-module`: the binary. It serves the wire contract, refreshes the inputs above, and writes decisions, admissions and reports to the store.

## Rules

- Consumers never repeat routing's policy. They ask, and they act on the answer.
- A refusal names its reason and, when known, when to retry (`reset_at_ms` or `retry_after`), so the consumer can wait instead of failing.
- Account walls (an exhausted account, a refused credential) live in routing's store and outlast restarts.
- Wire samples from consumers sit in crate fixtures with their source recorded in `crates/prefrontal-routing-module/fixtures/IMPORTED.md`. Routing's own reply samples sit in `test-vectors/`.
