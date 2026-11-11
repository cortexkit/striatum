# Routing performance and structure: plan

Date: 2026-11-11. Base: `main` at `f292133c`. Audience: Ufuk, the owner, who decides, and the engineers who implement the slices.

Inputs: two independent audits (called **Opus** and **Astra** below), the brief they answered, and the two investigations shipped on 2026-11-11 (request CPU, and route-handler store contention). Those are the maintainers' internal notes and are not in this repository; this plan restates what it relies on from them. Both audits cite line numbers at `6b32ab3`; where this plan cites current code, the line numbers were re-read at `f292133c`. Where it reuses an audit's citation, it says so.

The plan has six parts: where the time goes now (§1), what the audits agree and disagree on (§2), the target architecture for the four structural items (§3), a design for selection CPU (§4), ordered slices (§5) and open questions (§6).

## 0. Summary

- **Done today, not re-proposed:** credential snapshots are copy-on-write, resolved facts are cached by catalog/quality version, unchanged quota-hold reconciliation no longer takes the writer, and reading-log polls prune and append in one transaction (`49ee2a3`). `update_route_state` takes the writer up front (`f292133c`), and a worker is converting the remaining write transactions. Account admissions, observations and reports now cost about half the CPU they did on the production-sized fixture.
- **What is left, by measured size:** selection is now 85% of fixture CPU, and most of it is matching settings rules once per candidate and per credential row. Next come the per-request rebuild of the account context, the observation de-duplication scan (11–15 ms per observation on the live store), three commits per quota event on an async worker thread, and reads that queue on the writer mutex.
- **Selection fix (§4):** compile the subscription, exclusion and runner route rules once, when a config snapshot is published, into per-provider lookup tables. Then cache the structural "which credential rows serve this fact and are they subscription-covered" index per (config, catalog version, credential listing version). Per-candidate work becomes hash lookups plus the dynamic block check.
- **Structure (§3):** account state lives in memory as one immutable snapshot, owned by a single writer thread that commits every change to SQLite before publishing it. Thirteen block sources collapse into one `block` table with typed scope and cause, and the lifetime rules from today's fence work become properties of the cause. Live state moves into a small `state.db`; history moves into a separate file, fed by an outbox in the same transaction.
- **Order (§5):** measurement and four low-risk, independent slices first (rule compilation, the observation index, the quota-event transaction, checkpoints off the request path). Then reads off the writer. Then the typed block evaluator, the writer thread, the in-memory snapshot, the block-storage collapse and the live/history split, in that order. Each slice ships alone and has its own gate and measurement.

## 1. Where the time goes now

### 1.1 What is measured

| Source | What it measured | Key numbers | Status after today |
|---|---|---|---|
| Live 60 s `sample` at 13:20Z, before today's fixes (Opus §1; Astra "Evidence and limits") | Wall-busy ticks per thread at ~1 ms | 5,335 busy ticks in ~50 s ≈ 10.7% of a core wall-busy; ~2,850 on CPU ≈ 5.7%; 1,007 mutex wait; 904 `fsync`; 496 `pread`; 73 `pwrite`. Both audits reconcile to the same totals. | Pre-fix. **There is no post-fix live sample yet.** |
| Same sample, per handler (Opus §1 table) | Inclusive ticks | `account_observe` 1,642; `quota_events::run` 961 (579 `fsync`); `account_for` 836; legacy `record_usage` 605 (354 mutex wait); `select_at_tier` 473; `account_quota_loop` 237; retention 224; v1 `report` 178 | Pre-fix. Per-call figures (~65 ms observe, ~10 ms admission, ~19 ms select) are Opus's estimates: ticks divided by the brief's request rates. |
| Release bench `production_sized_request_cpu`, macOS ARM64 (`request-cpu.md` "CPU per operation") | Process CPU per operation, base vs `49ee2a3` | `account_for` 75.9 → 37.6 ms; `account_observe` 76.2 → 37.2 ms; `report` 9.9 → 5.7 ms; `select` 994.7 → 965.4 ms (−2.9%); 190-operation batch 40.0 → 34.0 s. Select is 85.2% of the batch. | Current. |
| Same bench, phases (`request-cpu.md` "What bought the improvement") | Per-phase CPU | Unchanged `ClassContext` load 42.5 → 4.9 ms, and writer transactions per 100 loads 100 → 0; `get_facts` 3.84 → 0.07 ms; reading-log poll 8.0 → 3.0 ms | Current. |
| Select-only `sample` of the bench (`request-cpu.md` "Remaining select CPU") | Inclusive share of 5,322 selection-worker samples (overlapping) | `ClassContext::class` 53.5%, `resolve_spec` 43.5%, `spec_matches_key` 42.3%, `prepare_candidates` 24.2%, `ClassContext::probe` 23.0%, `parse_spec` 15.7%, `roster_and_fences_for` 12.7%, `apply_eligibility` 11.2%, `origin_applies` 9.9%, `allocation_context` 9.5% | Current. |
| Observation lookup on the live store (Astra P1, measurements supplied by Alfonso) | Warm lookup of a new event id | 278,884 rows; principal-only plan on both arms; 14.5 ms (Broca), 11.0 ms (Thalamus) | Current: no migration since `083` adds an index. |
| Synthetic observation lookup (Opus P2; Astra P1) | Same query with a covering `(principal, event_id, runner)` index | 33–46 ms → 0.005 ms at 300k rows (Opus); 13.8 ms → 0.002 ms at 302k rows (Astra) | Current. |
| Route-handler contention (`route-handler-store-contention.md`) | Deferred read-to-write upgrade | Returned `database is locked` without waiting for the busy timeout | Fixed in `f292133c` for `update_route_state`; the other transactions are being converted. |

### 1.2 How to read these numbers

- **The bench overstates selection relative to live.** The fixture's catalogue installer generates 800 per-model subscription entries (`request-cpu.md`, "Important workload limitation"). Selection cost grows with the rule count times the candidate count (§4.1), so 965 ms per select is a stress figure. The live sample put select at about 19 ms per call. The live rule count is not known; §6 Q4 asks for it, and slice S0 builds a fixture that matches it.
- **The bench understates SQLite waits.** It measures CPU, not `fsync` or mutex queueing. Those were about 40% of live busy time before today (1,007 + 904 of 5,335 ticks), and today's fixes removed only part of them.
- **The 1% target is 600 ms of CPU per minute.** At the stated load (about 190 foreground calls a minute) that leaves about 3.2 ms of CPU per call, before quota events and background loops (Astra, "Evidence and limits").

### 1.3 What remains, after today's fixes

Estimates are marked. "Read" means the code was re-read at `f292133c`.

| Remaining cost | Evidence | Size |
|---|---|---|
| Settings-rule matching per candidate and per credential row in selection | Measured (bench profile); read: §4.1 | Dominant on the fixture; on live, it scales with the unknown rule count |
| Account context rebuilt per admission, observation and selection: per-provider `pool_key_states`, which runs `fence_rechecks::sync` inside a write transaction for pool-target providers (`crates/striatum-store/src/key_pool.rs:110`, `460-466`); `sync` can insert into `fence_retired_origin` (`crates/striatum-store/src/fence_rechecks.rs:114-144`); the 13-arm view is re-materialized into a temp table on every call (`fence_rechecks.rs:157-164`) | Read. Opus P1/P6 measured `sync` at 231 ticks before the fixes. The bench's unchanged-context phase now costs 4.9 ms per load without a writer, but its fixture has no pool targets. | Estimate: 4–6 ms per account call on the fixture, plus writer exposure for pool providers |
| Observation de-duplication scan inside the observation's write transaction | Measured live: 11–15 ms | ~30 per minute, all holding the writer |
| Quota events: three commits per event (reading log, account poll, full snapshot), on a Tokio worker (`crates/striatum-module/src/quota_events.rs:99-107`) | Read. Before the fix: 961 ticks, 579 of them `fsync` | Estimate: about half the pre-fix `fsync` remains, because per-account appends are now one transaction but the snapshot is still written per event |
| Pure reads on the writer mutex: `account_read` is a deferred transaction on `with_conn` (`crates/striatum-store/src/account_selection.rs:2364-2378`); `get_catalog_facts` on every usage report (`crates/striatum-store/src/lib.rs:1538-1543`, from `crates/striatum-module/src/model_resolution.rs:355-357`), which today's fact cache does not cover | Read; 1,007 mutex-wait ticks before the fix | Latency rather than CPU; it sits in Broca's 1.5 s admission budget |
| WAL checkpoints inside request commits; `synchronous=FULL` by default | Measured: 191 ticks of inline checkpoints (Opus P3). Read: no pragma setter in `crates/striatum-store` | Latency, plus `fsync` under the writer |
| Admission authorization builds the whole runner registry (Opus P5.2, 386 ticks) | Measured before the fix; the registry code is unchanged | Estimate: about 100–250 ticks per 50 s now that fact reads are cached |
| Tables that nothing prunes; 13 GB store | Read (Opus P11; Astra P5) | Storage and page-cache pressure. Handled by the retention track. |

**Where the audits are now stale.**

- Opus P1 lists the `BrocaCredentials` clone and the holds-refresh write transaction as per-request costs. Both are done (`49ee2a3`).
- Opus P3's "2N+2 commits per quota event" is now three commits per event (`quota_events.rs:99-107`).
- Opus P7's second Insula-id join at `credential_fences.rs:216` was replaced by a credential-to-account map computed once per context (`request-cpu.md`, "Delivery and correctness"). Treat P7 as resolved apart from re-reading `active_pool_fences`.
- Opus's combined estimate of about 1.2% CPU assumed selection was 9% of busy time. The fixture profile shows selection can dominate when rule counts are high, so that estimate is unreliable until S0 measures a representative config.
- Neither audit noticed the deferred read-to-write upgrade that `f292133c` fixed.

## 2. Findings

### 2.1 Where both audits independently agree

Ranked by expected benefit, then risk. "Done" marks today's work.

| # | Finding | Opus | Astra | Benefit | Risk | Status |
|---|---|---|---|---|---|---|
| A1 | Observation de-duplication walks every row of the principal. Add covering `(principal, event_id, runner)` indexes to `account_observation` and `account_observation_event`, with a query-plan test. | P2 (`crates/striatum-store/src/account_selection.rs:2237-2251`; `migrations/044_account_selection.sql:159`; `056_restorable_history.sql:37`) | P1 (`account_selection.rs:2189-2251,2380-2398`) | 11–15 ms off every observation, measured live, all of it writer hold time | Low; one rehearsed index build | Open → S2 |
| A2 | Account operations rebuild a fleet-wide `ClassContext` and repair it while reading. Split immutable structural inputs from dynamic state, key them by versions, and move the repairs to the events that cause them. | P1, P7 (`crates/striatum-module/src/account_selection.rs:625-930` at `6b32ab3`) | P2 (`account_selection.rs:173-197,625-887`) | Largest remaining account-path cost; removes writer exposure from reads | Medium: probe ownership and evidence ordering | Partly done (`49ee2a3`: credential COW, holds read-only, fact cache). Rest → S4, S8 |
| A3 | A quota event is a synchronous multi-commit pipeline on an async worker. Use one transaction per event, prune elsewhere, write the snapshot periodically, and move the work off the Tokio worker. | P3, P4 (`quota_events.rs:74-116`; `main.rs:633-700`) | P3 (`quota_events.rs:72-116`; `main.rs:630-700`) | Most of the remaining `fsync` and the mutex waits it causes | Medium: ACK only after durable state | Partly done (per-account appends batched). Rest → S3 |
| A4 | Pure reads queue on the writer mutex although a four-reader pool exists. Move genuine reads to `with_read`; keep admission read-modify-write on the writer. | P4 (`cortexkit-store` `lib.rs:302,346-352,373-393`) | P4 (`account_selection.rs:2361-2378`; `lib.rs:2855-2899`) | Tail latency within Broca's 1.5 s budget | Low to medium: snapshot staleness | Open → S4 |
| A5 | Selection rebuilds its inputs per tier attempt, panel seat and `what_if` point, and re-parses route specs per lookup. | P5.3–4, P9 (`routing_v1/select.rs:294-407,593-619`; `runners.rs:420-470`) | P6 (`select.rs:294-407,474-731`; `runners.rs:204-402`) | Now the measured top cost on the fixture (§1) | Medium: policy regressions | Open → S1, S6 |
| A6 | Several newer tables are never pruned, and deleting rows does not shrink the file. | P11 (`062_routing_v1.sql`; `078_report_runs_and_call_counts.sql`; `retention.rs:7-12`) | P5 (same tables; `lib.rs:4544-4567`) | Store size, page-cache pressure | Medium: replay contracts | Retention track |
| A7 | The decision-retention prune re-walks referenced old rows on every pass; the row limit does not bound the work. | P8 (`crates/striatum-store/src/retention.rs:82-101`) | P4, second half (`retention.rs:48-57,82-101`) | ~180 `pread` ticks per pass | Low | Retention track (cursor plus covering index) |
| A8 | `runners::effective` turns store failures into an empty registry, so a store outage reaches Broca as `account_not_authorized`. | S5 (`runners.rs:474-487`; `accounts.rs:149-154`) | C3 (`runners.rs:473-488`) | Correctness and diagnosis | Low | Open → S4 (with the membership check) |
| A9 | v1 account operations reach the legacy handler through a JSON round trip and keep a second replay record. | S3 (`routing_v1/accounts.rs:287-364`) | C2 (`accounts.rs:177-416`) | Small CPU gain; large maintenance gain | Medium | Inside S8a (the typed command is the writer's input) |
| A10 | Account state should be an in-memory read model, with SQLite as its durable journal and restart source. | D1 | D1 (more cautious: not as the first patch, no write-behind for admissions) | Structural; removes A2 entirely | High | Owner agreed → S8 |
| A11 | Fences, cooldowns, refusals and backoffs are modelled many times over; use one typed blocking-evidence model. | D2 | D2 (keep evidence, probe permission and hold answers as distinct types) | Structural; removes `sync` and the 13-arm view | High | Owner agreed → S7 |
| A12 | One request can see two config snapshots; derived policy is rebuilt on every call. | S6, D6 (`accounts.rs:331-333`; `config_overrides.rs:962-1050`) | D4 (`config_overrides.rs:268-294,403-479`) | Correctness, plus some CPU | Low | → S1 (snapshot-time compilation) |

### 2.2 Disputed or single-source findings

| Finding | Source | Position in this plan |
|---|---|---|
| `synchronous=NORMAL` on the writer | Opus P3 recommends it. Astra P3 says not to buy speed by "silently turning off synchronous durability". **Disputed.** | Not in the first slices. S3 cuts commits per quota event from three to one, and S5 moves checkpoints off requests. Re-measure `fsync` afterwards and then decide (§6 Q1). |
| Turn off auto-checkpoint and checkpoint from a background tick | Opus P3 only; measured 191 ticks inline | Adopted as S5. Low risk. |
| Replace the three-column observation uniqueness with `(principal, event_id)` | Opus P2, "better still". Astra P1 warns about the historical meaning of duplicates. **Disputed.** | Keep the uniqueness; add the index only. |
| `ANALYZE` as the fix for the observation scan | Astra P1 shows it gives a skip-scan plan (0.007 ms) but treats it as a control, not the fix | Index it. Running `ANALYZE` stays optional maintenance. |
| `fence_rechecks::sync` runs on reads, many times per request | Opus P6 (measured 231 ticks). Astra mentions repeated fence enumeration only generally. Single-source detail. | Still true (§1.3). S4 moves it to writers and a tick; S7 deletes it. |
| Runner-registry build used for a membership check | Opus P5.2, single-source, measured 386 ticks | S4. Answer it from `operator_config.v1_runners()`. |
| `get_catalog_facts` per usage report | Opus P5.1, single-source, measured 454 ticks | S4: serve it from the catalogue cache. Still uncached (`lib.rs:1538-1543`). |
| Vault roster rewritten every minute even when unchanged | Opus P12, single-source | S3 (compare a digest before writing). |
| `what_if` sweeps up to 101 IQ points (values of the requested capability level), each re-preparing from the live store, so the sweep is not one coherent generation | Astra P6, single-source | S8c makes the sweep a pure function of one snapshot. |
| `select_engine` prepares full LLM inputs before looking at local engines; `head_choices` rebuilds joins per call | Astra P6, single-source; frequency unknown | Deferred. Revisit after S0 measures traffic per operation. |
| Project settings may make a cross-module census call on the selection path | Astra D4, single-source | Out of scope here; noted as a latency risk. |
| Config stamp `(mtime, length)` can miss a same-size edit | Both. Opus calls it theoretical on APFS; Astra calls it bad design. | Use `policy_revision` as the publish key in S1. It is cheap. |
| Two database files, history partitioned by month | Opus D5, single-source. Owner: "definitely" separate live from history. | §3.3, with an outbox instead of cross-file transactions. Partitioning is left to the retention track. |
| Moving `main.rs` tests out of the file | Opus S1. Astra C1 says this does not fix the misplaced responsibility. | Not a performance slice. The writer and evaluator split in S7/S8 is the real responsibility fix. |
| Legacy wire retirement | Both, with different emphasis | Out of scope. A census comes first (`routing_call_count`). |

## 3. Target architecture

### 3.1 Account state in memory, with SQLite as its journal

**What exists today.** `ClassContext::load`/`read` (`crates/striatum-module/src/account_selection.rs:637-666` and after) rebuilds the account picture on every admission, observation and selection, and repairs it while doing so. The picture covers continuations, eligibility, size and route fences, pool key state, recheck clocks, credential refusals and quota holds. The struct it builds (`account_selection.rs:173-198`) is thrown away after the request. Admissions and affinities are read later, inside the serving transaction (`account_selection.rs:5170-5190`). The live state is small: about 22K unexpired affinity rows (4.5K used in 24 h), about 90 blocks, 22 latest quota readings, about 40 logins and a handful of open admissions.

**Target.**

```
                 commands (typed)                      publish after commit
 request ──► AccountWriter thread ──► one IMMEDIATE txn ──► ArcSwap<AccountState>
 handlers         (single owner)        state rows +             │
                                         history outbox          ▼
 readers (select, panels, head_choices, what_if) ──── load() ── snapshot (no lock, no SQL)
```

- **`AccountState`** is an immutable value behind an `ArcSwap`. It holds affinities, open admissions with demand aggregated per account, open continuation offers, eligibility facts, pool key state and leases, the latest quota reading per account (decoded once, not per candidate), completeness, the vault roster, and the block set from §3.2 with its probe clocks and claims. It carries a `generation: u64`. Structural inputs (catalogue, credential listing, compiled config) stay in their own versioned `Arc`s (§4) and are referenced, not copied.
- **`AccountWriter`** is one OS thread (not a Tokio task) that owns the SQLite writer connection for account tables. Every account change becomes a typed command: `Admit`, `Observe`, `Report`, `QuotaDelta`, `ListingChanged`, `ConfigPublished`, `GrantProbes`, `ReleaseProbe` and `Tick`. These replace the JSON tunnel between v1 and legacy (A9).

  The writer handles a batch of commands in four steps:
  1. **Compute.** For each command in arrival order, a pure function computes `(next_state, effects, reply)` from the current state. Effects are row upserts and deletes plus history records.
  2. **Commit.** All effects in the batch are written in one `BEGIN IMMEDIATE` transaction. A batch is whatever is queued when the previous commit finishes, capped at 32 commands and at a few milliseconds of waiting.
  3. **Publish.** The new state is published only after `COMMIT` returns.
  4. **Reply.** Each command's reply is sent only after publication.

  The writer computes a batch's commands one after another against the same in-memory state, so two admissions in one batch see each other's effects.
- **Readers** take `state.load()` and never touch SQLite for account state. A reader's snapshot is a view, not permission. Selection may *prefer* an account from a snapshot, but only `Admit` in the writer grants, revalidating against the latest state. This keeps the rule from `f9853c6f`: an established-affinity Hold is built from the admission transaction's own applicable origins, not from a pre-claim snapshot.
- **Time-based transitions need no writes.** A block becoming due for a probe, an affinity or continuation expiring, a lease lapsing: all are evaluated against `now` when read. That is what `read_effective` already does for clocks (`crates/striatum-store/src/fence_rechecks.rs:267-356`). The `Tick` command (every 5–10 s, and driveable from tests) only compacts the state: it releases expired continuations, retires expired non-quota evidence, prunes expired affinities from memory, and reconciles eligibility against a new listing. These are the repairs `ClassContext::read` does on every request today.

**Crash recovery.** State is published after commit and replies are sent after publication, so a crash loses only commands whose callers never got an answer. Those callers retry, and the idempotency keys (admission id, event id, delivery id) live in the same committed state, so a retry replays rather than repeats. There is no write-behind: Astra's condition for D1 holds. A commit that fails rolls the whole batch back, because the batch is one transaction. The previous state stays published, and the writer answers every command in the batch with a retryable `store_error`. If the writer cannot tell why a commit failed, it reloads the state from the store before taking the next batch.

**Consistency across requests.** One writer serializes every account transition, so the lock-upgrade race fixed in `f292133c` cannot recur for account tables, and `BEGIN IMMEDIATE` is the writer's only transaction kind. Probe claims stay atomic: `GrantProbes` and `Admit` run the same claim predicate that `claim_owned` runs today (`fence_rechecks.rs:375-385`, `436-468`), against in-memory clocks, and commit the clock change in the batch. A losing concurrent claim consumes nothing, as today.

**External writes.** Today the fact cache detects commits from other connections through `PRAGMA data_version` (`request-cpu.md`). The writer checks the writer connection's `data_version` at the start of each batch and on each `Tick`. If another connection has committed, it reloads the whole state. The load is small, estimated at tens of milliseconds for 22K affinity rows. Tests that seed state with raw SQL either call `reload()` or go through commands. §6 Q3 asks whether anything outside the daemon writes these tables in production.

**Restart.** `AccountState::load(conn, now)` reads the state tables once, inside one read transaction: unexpired affinities, active and tombstoned blocks with clocks and claims, open admissions, open continuations, eligibility, pool keys and live leases, latest readings and completeness, and the roster. It builds the derived indexes from §4.3 and publishes generation 1. Until then, account operations answer with a retryable `account_selection_unavailable`; §6 Q9 asks whether to hold the module's ready signal instead. A load-equivalence test checks that load(store after N commands) equals the in-memory state after the same N commands, for randomized command sequences.

**`route.what_if` afterwards.** Today `what_if` copies about 20 tables into a private in-memory SQLite database (`crates/striatum-store/src/evaluation_snapshot.rs:46-87`, `116-160`). (`what_if` evaluates a selection at up to 101 values of the request's IQ requirement, the capability level the caller asks for, without recording anything.) It does that because loading account state writes, and those writes must not reach the live store. The selector still reads facts and route state from the live store (`crates/striatum-module/src/routing_v1/what_if.rs:156-162`; Astra P6). After S8 there are no read-path writes. `what_if` takes one `AccountState` snapshot and one catalogue/config snapshot, then runs the pure evaluator for every IQ point and panel seat with probe grants disabled. The sweep becomes coherent by construction, nothing is copied, `evaluation_snapshot.rs` is deleted, and the existing guarantee that `what_if` changes no rows holds trivially. The test `what_if_changes_no_rows_even_when_account_loading_reconciles_old_evidence` (Opus S8: `routing_v1/what_if_tests.rs:337`) stays as the guard.

### 3.2 One block model

**What exists today.** The `fence_origin` view unions 13 arms over 11 source tables and yields 14 kind strings (`crates/striatum-store/migrations/082_fence_rechecks.sql:119-184`). Five more tables carry the bookkeeping: `fence_recheck` (clocks, `:5-27`), `fence_probe_authorization` (`:28-36`), `fence_recheck_route_clear` (`:37-42`), `fence_success_pool` (`:43-49`) and `fence_retired_origin` (`:186-193`). A trigger that clears the eight kinds of source rows keeps them consistent (`:194-225`). Rust code re-materializes the view into a temp table on every `sync` (`fence_rechecks.rs:157-164`).

**Target: one `block` row per piece of blocking evidence.**

| Field | Type | Meaning |
|---|---|---|
| `block_id` | integer | Identity |
| `scope` | enum plus typed keys | `Provider{provider}`, `Route{route_key}`, `Account{provider, account, applicability}`, `Credential{provider, credential}`, `AccountRoute{provider, credential, model}`, `PoolKey{provider, credential}`, `Size{provider, credential, model, size_class}`, `Principal{principal, admission, credential}`, `AccountBackoff{provider, account}` |
| `cause` | enum | `Quota`, `Billing`, `Entitlement`, `RateLimit`, `Transient`, `Failure`, `SizeRefusal`, `PrincipalExclusion` |
| `evidence`, `evidence_at`, `evidence_generation` | JSON, ms, integer | What was observed and when. A changed evidence value starts a new generation. |
| `reported_until`, `reset_at` | ms, nullable | Deadlines the provider reported. Quota blocks keep these as *checks*, never as expiry. |
| `next_probe_at`, `reset_check_at` | ms | The probe clock |
| `claim_owner`, `claim_generation` | text, integer | The current probe claim. The generation is separate from the authorization launch time. |
| `cleared_models` | set | Partial route clears (replaces `fence_recheck_route_clear`) |
| `cleared_at`, `cleared_by` | ms, enum | Lifecycle. A cleared row is a tombstone (replaces `fence_retired_origin`). |

Two small tables stay separate, because they are many-to-many or a different concept, as Astra D2 asks:

- `probe_authorization(owner, block_id, evidence_generation, claim_generation, launched_at)`. One launch can authorize blocks from two providers (alias and target pool), per `3a087b9f`.
- `success_proof(block_id, pool, healed_at)`, replacing `fence_success_pool`.

Hold answers (`account_hold`) are history, not blocks. Probe permission is the clock and claim on a block, not a separate kind of evidence.

**How the 13 arms map.**

| Today (kind string, source table) | Scope | Cause |
|---|---|---|
| `account`, `account_fence` | Account (with pool/family/model applicability) | Quota or Billing, from the fence row |
| `credential`, `credential_entitlement_fences` | Credential | Quota when classified as a quota hold, else Entitlement or Billing |
| `account_route`, `account_route_fence` (`wall_class='quota'`) | AccountRoute | Quota |
| `non_quota_route` / `transient_route`, `account_non_quota_route_fence` | AccountRoute | Billing or Entitlement / Transient |
| `pool_billing`, `pool_key_state.walled_until` | PoolKey | Billing |
| `pool_rate`, `pool_key_state.cooldown_until` | PoolKey | RateLimit |
| `provider`, `provider_cooldown_state` | Provider | RateLimit, or Quota for `quota_exhausted_until` |
| `route`, `model_route_state` (cooldown/quota) | Route | RateLimit or Quota |
| `failure`, `model_route_state.failure_backoff_until` | Route | Failure |
| `size`, `account_size_refusal` | Size | SizeRefusal (quota) |
| `non_quota_size`, `account_non_quota_size_refusal` | Size | SizeRefusal (non-quota) |
| `principal`, `account_principal_exclusion` | Principal | PrincipalExclusion |
| `observation`, `account_transient_backoff` | AccountBackoff | Transient |

`model_route_state` and `provider_cooldown_state` also hold route data that is not a block, such as the last error. Those columns stay where they are, and only the deadline columns move. Quarantine, meaning account-local errors stored on model or provider rows, becomes an `Account`-scoped block when it is written. Today `sync` works this out on every call by parsing `last_error` JSON (`fence_rechecks.rs:134-142`).

**Lifetime rules as properties of the cause.**

- **Quota** (`cause = Quota`): there is no expiry. `next_probe_at = evidence_at + 15 min` (`QUOTA_FENCE_RECHECK_MS`, `crates/striatum-core/src/fence_lifetimes.rs:2`), and a reported reset adds `reset_check_at`, a check, not an end. The block clears on fresh Insula headroom: a reading fetched after the evidence, with headroom in every reported window (`crates/striatum-module/src/credential_fences.rs:154-203`). It also clears on a successful authorized probe. An inconclusive probe leaves the quota interval alone.
- **Non-quota** (every other cause): `next_probe_at = min(reported_until, evidence_at + 1 h)` (`NON_QUOTA_FENCE_MAX_MS`, `fence_lifetimes.rs:3`). After that, the block admits one probe. Success clears it. A neutral outcome releases that claim generation and makes the block due again immediately. A failure is new evidence and starts a new generation. Expired, unclaimed non-quota evidence is retired by the writer's `Tick` into a tombstone. Today `sync` does this on reads (`fence_rechecks.rs:119-128`).
- **Probe admission:** a candidate identity is probe-eligible when every block that applies to it is due and unclaimed by another owner. The claim covers all those blocks atomically. The applicability and self-exemption rules are ported unchanged from `eligible`/`probe_eligible` (`account_selection.rs:1188-1252`) and `decide.rs` (`crates/striatum-core/src/accounts/decide.rs:335-359`, per the code lookup), with string kinds replaced by the enum.
- **One write rule:** `put_block` accepts evidence only if it is newer than the live block's evidence and newer than any tombstone with the same scope and cause. It is the single place that enforces "newer evidence wins". Today each table re-implements it (Opus D2: `key_pool.rs:490-498`, `account_selection.rs:747-748` at `6b32ab3`).

### 3.3 Invariants the new design must keep

These are the behaviours today's fence-lifetime work fixed (merge `d285865d` and the commits in it, plus `bef5ebbf`, `6a914f97`, `743d7e93` and `f292133c`). Every slice that touches blocks or account state must keep the listed tests green **without changing their assertions**. The table names the pinning test where one exists.

| # | Invariant | Source | Pinning test |
|---|---|---|---|
| I1 | Quota evidence has no fixed end. Reset dates and Retry-After are checks, not expiry. | `ff19c58f`, `3231687b` | `quota_rechecks_every_fifteen_minutes_and_reset_only_adds_a_check` (`fence_rechecks.rs:2018-2067`) |
| I2 | A quota block becomes probe-due every 15 minutes, including when readings disappear. | `fence_lifetimes.rs:2` | `fence_policy_intervals_are_fifteen_minutes_and_one_hour` (`fence_lifetimes.rs:25-28`); `a_billing_refusal_converted_without_a_reset_keeps_its_fifteen_minute_check_when_readings_disappear` (`routing_v1/account_classes_tests.rs:2919-3001`) |
| I3 | Fresh headroom clears quota holds: a reading after the refusal, with headroom in every window. Healing route evidence also requires `block_end` over all active pool fences of the account, newer than every route report, checked in one writer transaction. | `bef5ebbf`, `f9853c6f` | `subscription_quota_hold_lifts_on_fresh_headroom_reading` (`account_classes_tests.rs:2440-2483`); `run_and_reading_resets_do_not_postpone_checks_and_headroom_lifts_both` (`:3004-3052`) |
| I4 | Readings that only report a spend balance do not suppress timed probes. Contradictory fresh headroom creates an independent non-quota refusal. Consistent exhaustion stays quota and never rescales IQ demand. | `bef5ebbf`, `ff19c58f`, `3231687b` | xAI 403 spending-limit sequence tests named in `ff19c58f` |
| I5 | Non-quota blocks last at most one hour, and a shorter reported retry is kept. | `3231687b` | `every_non_quota_origin_caps_at_one_hour_preserves_shorter_retry_and_claims_once` (`fence_rechecks.rs:1755-1892`); `pay_per_token_credential_without_reading_keeps_the_one_hour_probe` (`account_classes_tests.rs:2581-2619`) |
| I6 | Only one probe is admitted per due interval, atomically across connections and requests. A losing claim consumes nothing. | `claim_owned` (`fence_rechecks.rs:375-385`) | `strict_probe_claim_is_atomic_across_distinct_connections` (`fence_rechecks.rs:2821-2890`); `direct_pool_acquire_admits_exactly_one_concurrent_probe` (`:1896-1964`) |
| I7 | No block blocks its own probe, including an independently stored route fence on the same identity. The probe is still admitted only once. | `bef5ebbf`, `account_selection.rs:1188-1252` | `credential_probe_admission_is_single_and_completion_heals_its_credential` (`account_classes_tests.rs:271-389`); `stale_api_billing_probe_passes_its_route_fence_once_even_with_spend_reading` (`:4172-4232`) |
| I8 | Probe claims are pinned to the actual serving identity. Legacy hosts that cannot be pinned may claim only anonymous route or provider trials. | `3231687b` | Tests named in that commit |
| I9 | A selection-owned probe transfers its owner to the decision, is consumed once at admission, and cannot be reused by another admission. Decision-owned transient clocks stay out of the Hold and its retry deadline. | `8ec93dc2`, `bind_fence_probe_owner` (`fence_rechecks.rs:474-485`) | Double-send and competing-decision regressions in `8ec93dc2` |
| I10 | A neutral outcome releases only the matching non-quota claim generations, in every origin provider of an alias or target launch, and leaves quota intervals alone. | `62bb4633`, `3a087b9f`; `release_inconclusive_probe` (`fence_rechecks.rs:535-562`) | Regressions in `3a087b9f` |
| I11 | Success heals exactly the authorized scope: the known pool or family. A launch of unknown size neither claims nor heals size blocks. A size success needs a matching account admission. | `75bb7431`, `3231687b`; `heal_authorized_probe` (`fence_rechecks.rs:565-611`) | Tests named in those commits |
| I12 | The clock claim generation is separate from the authorization launch time, so consuming an admission cannot burn an inconclusive recovery. | `e3a0ed14` | Tests named in that commit |
| I13 | Credential refusal evidence is kept. Its former six-hour horizon was probe eligibility, never expiry. | `e3a0ed14` | Migration and classification controls in that commit |
| I14 | Newer evidence wins. A changed evidence value is new evidence, not a probe. Leasing a key does not extend its deadline. A refusal reported concurrently is never erased by an older recovery snapshot. | `fence_rechecks.rs:166-169`; Opus D2 | Existing key-pool and recovery ordering tests |
| I15 | Retired evidence is archived and must not resurrect. Backups include proofs, authorizations and retired origins. `what_if` evaluations include them too. | `81b30478`, `743d7e93` | Backup-exclusion rehearsal; `what_if` snapshot tests |
| I16 | Account-local errors written on model or provider rows are quarantined: they never wall healthy sibling accounts. | `3231687b`; `fence_rechecks.rs:134-142` | Quarantine controls |
| I17 | A concurrent probe claim that loses keeps the fence and deadline diagnostics rather than turning into `not_in_runner_candidates`. | `498ac44a` | Deterministic regression in that commit |
| I18 | Scoring, selection and `what_if` are read-only. Only the admission transaction materializes and claims retry permissions. | `3231687b` | `what_if_changes_no_rows_even_when_account_loading_reconciles_old_evidence`; `CONTEXT_LOADS` and write-count guards (`account_classes_tests.rs:1814-1839`, per Opus S8) |
| I19 | An established-affinity Hold is built from the admission transaction's applicable origins, not from the snapshot taken before the claim. | `f9853c6f` | Started-pin / due-snapshot / other-winner interleaving test |
| I20 | Read-modify-write takes the writer before it reads, and contention waits for the busy handler instead of failing. | `f292133c` | `route_handler_isolation::a_route_handler_waiting_on_the_store_does_not_stall_other_tasks` |

### 3.4 Live state and history in separate files

**Problem.** One 13 GB file holds both the small hot state and append-only history. Most of it is decision scoring detail: `score_json` is about 134 KB and `quota_signal_json` about 16 KB per decision, 10.9 GB in total. Hot queries share a page cache (the default 2 MiB), a WAL and the writer with history inserts, and deleting rows never shrinks the file (Opus P11, D5; Astra P5).

**Target.**

- **`state.db`** (small; the account writer is its only writer) holds blocks with their claims, authorizations and tombstones, affinities, open admissions, continuations, eligibility, pool keys and leases, latest readings and completeness, the roster, idempotency keys for their replay window, catalogue facts and route state. It is what the restart loader reads and what backups must always include.
- **`history.db`** (append-only) holds decisions (the compact record and the full-detail record), observations, reports and deliveries (bodies and answers), usage, the quota reading log, speed samples, call counts, hold answers, closed admissions, and tombstones past their window. `route.explain`, `route.decisions_list` and the task-profile refresh read it through the reader pool, using `ATTACH`.
- **No transaction spans the two files.** In WAL mode a transaction across attached databases is atomic per file but not across files. So the writer appends history records to an `outbox(seq, table, row_json)` table in `state.db`, in the same transaction as the state change. A background mover copies batches into `history.db`, using `seq` to stay idempotent, commits, then deletes the moved outbox rows. A crash between those steps only re-copies rows. Readers that need the newest history, such as `route.explain` immediately after a decision, look in the outbox first.
- **What must stay atomic with state stays in state.** Report delivery claims its delivery key and applies effects atomically today (`crates/striatum-store/src/report_delivery.rs:26-126`, per Astra §5). The key and its outcome stay in `state.db` for the replay window. Only the verbose body and answer go through the outbox.
- **Retention becomes per file.** The retention track sets the windows (full detail about 2 days, compact record 30 days, 1-in-20 calibration sample). If it adopts daily files for full detail and monthly files for compact history, expiring data means detaching and deleting a file, with no row deletes and no `VACUUM`. This plan only requires that history writes go through the outbox, so that partitioning can change without touching request code.
- **Durability per file.** `history.db` can run `synchronous=NORMAL` with large checkpoints without affecting probe-claim safety. `state.db` decides on its own (§6 Q1).

**Migration.** The old file becomes `history.db` and a new, small `state.db` is created, so the 13 GB file is renamed rather than copied. The steps run offline, in a deploy window, by script:

1. Stop the daemon.
2. Back up the store.
3. Create `state.db` from the state tables, copied with `INSERT … SELECT` through `ATTACH`.
4. Verify row counts and an evaluation-parity check (§5, S9).
5. Rename the old file to `history.db` and drop the state tables from it.
6. Start the daemon.

The new layout reclaims no space by itself. A one-time `VACUUM INTO` of `history.db`, after the retention track has pruned it, does, and it runs offline.

## 4. Selection CPU: compile settings rules once

### 4.1 Why selection is expensive now (read at `f292133c`)

A single selection calls `ClassContext::class` at least three times per catalogue fact:

- through `apply_eligibility` → `refusal_mark` (`account_selection.rs:1915-1937`, `1894`);
- in `prepare_candidates` (`crates/striatum-module/src/select_policy.rs:340`), which also computes a fallback `resolve_class` unconditionally on the next line (`:341`), even when the context has already answered;
- through `probe` → `roster_and_fences_for` for each scored candidate (`account_selection.rs:1756-1774`, `1621`).

Each `class` call does the following:

1. It clones and cleans the whole subscription list (`operator_policy()`, `crates/striatum-module/src/config_overrides.rs:664-671`) and re-parses it into owned strings (`account_selection.rs:1054-1056`). It also clones the fact (`:1057`).
2. It filters every listed credential through `credential_serves_fact` (`crates/striatum-module/src/broca_credentials.rs:141-159`). Then, for each serving row, it calls `resolve_spec` up to four times (`account_selection.rs:240-265`). `resolve_spec` walks the whole rule list twice (`crates/striatum-core/src/provider_model_spec.rs:93-106`), and `spec_matches_key` allocates two strings for every model-level rule under the same provider (`provider_model_spec.rs:79-80`).
3. For each row, `usable` → `usable_candidate` parses the rule list *again* (`account_selection.rs:1519-1521`). It then loops over every listed credential, calling `resolve_spec` twice, `eligible` and `credential_serves_fact` for each (`:1531-1540`).
4. `eligible` scans every recheck origin with string kind comparisons (`account_selection.rs:1188-1242`). For account origins, `origin_applies` rebuilds and re-validates the provider's whole allocator policy per origin (`:1272-1274` → `config_overrides.rs:1111-1125`, `962-1050`), including a fresh `quota_model_pools` vector (`:1084-1095`).
5. `view` scans every quota reading and decodes the chosen reading's JSON into an `AccountEntry` per (row, fact) (`account_selection.rs:1099-1123`).

With R subscription rules, C listed credentials and F facts, one `class` call costs on the order of C² × R string comparisons and allocations, and a selection pays that about 3F times. The fixture has R = 800, C = 40 and F = 800, which is why `resolve_spec`, `spec_matches_key` and `parse_spec` dominate its profile. Runner reachability repeats the pattern: `resolve_spec(&parse_spec_list(specs), …)` runs inside the per-fact filter (`crates/striatum-module/src/routing_v1/runners.rs:361`) and in each `Registry` lookup (`runners.rs:428-468`).

None of these inputs changes per request. The rules change when the config is reloaded, the facts when the catalogue changes, and the rows when the credential listing changes.

### 4.2 Design

Four layers. Each has one invalidation key, and the slower-changing layers never depend on the faster ones.

**Layer 1: `CompiledSpecs`** (new, in `crates/striatum-core/src/provider_model_spec.rs`)

```rust
pub struct CompiledSpecs { by_provider: HashMap<Box<str>, ProviderSpecs> }
struct ProviderSpecs {
    provider_level: Option<bool>,                 // last provider-level entry: !negated
    models: HashMap<Box<str>, (u32, bool)>,       // model path -> (list index, !negated), last entry kept
}
impl CompiledSpecs {
    pub fn compile(entries: &[String]) -> Self;
    /// None means "no rule names this key": the caller's base value stands.
    pub fn override_for(&self, key: &str) -> Option<bool>;
    pub fn resolve(&self, key: &str, base: bool) -> bool { self.override_for(key).unwrap_or(base) }
}
```

`override_for` splits off the key's provider and looks it up. It then enumerates the segment prefixes of the rest of the key: for `p/a/b/c` those are `a`, `a/b` and `a/b/c`. Each prefix is a hash lookup in `models`, and among the hits the entry with the highest list index wins. With no hit, `provider_level` decides.

This reproduces `resolve_spec` exactly. There, model-level rules beat provider-level rules, the last matching entry wins within each level, and a model rule matches the bare key or any key under it at a `/` boundary. The cost becomes O(segments) lookups with no allocation. `resolve_spec` stays as the reference implementation, used by a property test over random lists (negations, duplicates, nested model paths, variant keys, empty and whitespace entries).

**Layer 2: `CompiledPolicy`**, built once per published config snapshot (in `crates/striatum-module/src/config_overrides.rs`)

It is built where the config is validated, outside the publication lock (Astra D4), and stored inside the `Arc<LiveConfig>`. It holds:

- the operator exclusion and subscription rules as `CompiledSpecs`, with the cleaned string lists kept for wire output and explanations;
- the allocator policy per provider, plus the default, each as an `Arc<AllocatorPolicy>`, with `quota_model_pools` built once;
- each runner's route rules as `CompiledSpecs`;
- the per-provider flags such as `metered_fallback`.

`operator_policy()`, `allocator_policy()` and `quota_model_pools()` return references instead of fresh values. A request takes one `Arc<LiveConfig>` at entry and passes it down, which also closes the "two snapshots in one request" gap (A12). The publish key becomes `policy_revision` (`config_overrides.rs:657-662`) instead of `(mtime, length)` alone.

**Layer 3: `ServingIndex`**, cached across requests. Key: (`Arc<LiveConfig>` identity, catalogue generation, credential listing version).

The catalogue generation is the key that `49ee2a3`'s fact cache already uses: the TEMP catalogue/quality write counter plus `PRAGMA data_version`. For each fact the index stores:

- the indices of the serving credential rows (`credential_serves_fact`, evaluated once);
- for each serving row, `covered = !borrowed && subscription.resolve(route_key, row is oauth)`;
- the sibling sets that `usable_candidate` needs: rows of the same coverage class that serve the fact;
- the fact's subscription key and capacity key;
- per runner, whether the runner reaches the route. These are the static parts of `Registry`. `listing_is_fresh(now)` (`runners.rs:339`) stays a read-time check, as Opus P5 requires.

The index is built lazily on first use after a key changes, at a cost of O(F × C) once per catalogue refresh (every 10 minutes) or listing change, and published through an `ArcSwap`. The continuation restriction, which today clones the fact to narrow `auth_methods` (`account_selection.rs:1057-1072`), becomes a filter of the indexed serving rows by the continuation credential's method.

**Layer 4: per-request dynamic indexes**, built from the account snapshot (`ClassContext` until S8, then `AccountState`)

- Blocks and recheck origins indexed by `(provider, credential)`, `(provider, account)` and route, with kinds as an enum and the "due at `now`" flag computed once per request.
- Latest quota reading per account, decoded once per quota generation rather than per (row, fact).
- A per-request memo of `class(fact_index, priced)`, so the three call sites share one result.
- The fallback `resolve_class` in `prepare_candidates` computed only when there is no context.

After this, a `class` call is a few hash lookups, a walk over that fact's serving rows (usually one to five), and an indexed block check per row.

**Not cached:** the probe owner, tier, size class, consumer policy, the caller's own exclusion list (short; compiled per request) and the continuation credential. These stay per request, as today.

### 4.3 Correctness checks and expected effect

**Checks.**

- The property test comparing `CompiledSpecs` with `resolve_spec`.
- A differential test on the production-sized fixture and on the S0 representative fixture. For every fact, both payment classes, with and without a continuation credential, and with and without due probes, the old `class`/`eligible`/`view` must equal the new.
- The existing selection, quality-frontier, service-tier and fence tests, unchanged.
- I7 and I17 in particular, because `eligible` is where a block's own probe is exempted.

**Expected effect (estimate).** On the 800-rule fixture, the `parse_spec` (15.7%) and `resolve_spec`/`spec_matches_key` (about 43%) shares should mostly disappear, and the C² sibling loop becomes a lookup. I expect select CPU on the fixture to fall by 60–80%. The S0 fixture with live rule counts decides whether selection then meets the roughly 3 ms per-call budget.

## 5. Slice plan

### 5.1 Rules for every slice

- **Gate.** Remote `cargo check --locked` for the touched crates with `--tests`; a focused nextest filter covering the touched area, including every I-test from §3.3 that the slice can affect; then `bash scripts/rust-test-gate.sh` on 8 vCPUs on the committed tree. No assertion in an I-test may change. If a slice must change one, the change goes to the owner first.
- **Measurement.** Run the release bench `production_sized_request_cpu` on macOS before and after, on both fixtures from S0, and report CPU per operation, p50/p99 wall latency, writer-queue wait and commits per operation. After deploy, record process CPU time over one hour at normal traffic, compared with the 1% target of 36 s per hour, plus a 60 s `sample`.
- **Migrations.** Each store change gets the next free migration number, assigned at merge time because slices run in parallel (`083` is the latest today). Each gets a rehearsal example under `crates/striatum-store/examples/`, run by the owner on a copy of the 13 GB live store before deploy, never on the live store (`STRUCTURE.md:26`). A rehearsal reports duration, file-size change, row-count parity and the query plans the slice relies on.

### 5.2 Slices

| Slice | Content | Files touched (the slice changes nothing outside them) | Depends on | Store change / migration | Gate beyond §5.1 | Proof |
|---|---|---|---|---|---|---|
| **S0** Measurement | A second bench fixture with live-representative rule, credential and fact counts (§6 Q4). Per-op wall latency percentiles; stage timers for writer-queue wait, SQL, commit and serialization (Astra §6 Q1); a quota-event burst scenario; a tier-fallback scenario; pool-target providers in the fixture. Capture a post-`49ee2a3` live baseline (1 h CPU plus a `sample`). | `crates/striatum-module/src/routing_v1/request_cpu_bench.rs`; a small timing module | none | none | bench runs on both fixtures | The baseline table every later slice compares against |
| **S1a** Compiled rules | Layers 1–2 and the per-request memo from Layer 4; lazy fallback in `prepare_candidates`; compiled runner route specs | `crates/striatum-core/src/provider_model_spec.rs`, `launch_policy.rs`; `crates/striatum-module/src/config_overrides.rs` (+`config_overrides/new_shape.rs`); `select_policy.rs`; `routing_v1/runners.rs`; `head_choices.rs`; in `account_selection.rs` only `resolve_class`, `class`, `usable_candidate`, `origin_applies`, `probe` | none | none | `CompiledSpecs` property test; differential `class` test | Select CPU on both fixtures. Estimated ≥50% lower on the 800-rule fixture. |
| **S2** Observation index | Covering `(principal, event_id, runner)` index on `account_observation` and `account_observation_event`; query-plan test in the style of `tests/it/observed_speed_query_plan.rs`. Keep the three-column uniqueness. | `crates/striatum-store/migrations/0NN_observation_principal_event.sql`; `crates/striatum-store/tests/it/`; rehearsal example | none | Two `CREATE INDEX`. Rehearse: build time and size on the copy (Opus estimates ~520 MB of observations), plan on the copy, and a lookup of a new event id for Broca and Thalamus (today 14.5 / 11.0 ms). | The plan test asserts `principal=? AND event_id=?` on both arms | Observe p99 on a fixture with 300k rows; lookup ticks in the live `sample` near zero |
| **S3** Quota events | One `apply_quota_delta` transaction per event (reading log plus account poll); the full snapshot at most once a minute and at shutdown; store work on `spawn_blocking`; ACK after commit; roster written only when its digest changes | `crates/striatum-module/src/quota_events.rs`, `main.rs` (`write_quota_readings`, `write_quota_snapshot`, `:632-700`), `account_quota.rs` (roster write and loop); one new function in `crates/striatum-store/src/lib.rs` | The writer-conversion worker's merge (shared store transaction code) | none (snapshot format unchanged) | Epoch ordering, snapshot coverage, tombstones, failure-before-ACK, restart from snapshot (Astra P3) | Commits per event 3 → 1 in the burst scenario; quota-path `fsync` and mutex ticks in the live `sample` |
| **S5** Checkpoints | `wal_autocheckpoint=0` on the writer after open; passive checkpoint every 15 s from a background tick, `TRUNCATE` above a WAL size cap; reader `cache_size` raised | `crates/striatum-store/src/lib.rs` (open path); `crates/striatum-module/src/background.rs` | none | none (connection pragmas only) | A test that no checkpoint runs inside a request commit and that the WAL stays bounded | `sqlite3WalDefaultHook` absent under request frames in the live `sample` |
| **S1b** Serving index | Layer 3 (`ServingIndex`) and the Layer 4 block, reading and recheck indexes | `account_selection.rs` (`class`, `eligible`, `probe_eligible`, `view`, `allocation_context`, `roster_and_fences_for`); `routing_v1/runners.rs` (static `Registry`); a new `serving_index.rs` | S1a | none | Differential test including due probes and the continuation restriction; I7, I17 | Select and admission CPU on both fixtures |
| **S4** Reads off the writer | `account_read` and other pure reads via `with_read`; `pool_key_states` read-only without `sync`; `sync` only from writers (`register` at receipt) and a maintenance tick; `enabled()` cached; `get_catalog_facts` from the catalogue cache; the runner membership check from `v1_runners()`; store errors returned as retryable errors instead of empty registries (A8); repairs moved out of `ClassContext::read` into the tick | `crates/striatum-store/src/account_selection.rs`, `key_pool.rs`, `fence_rechecks.rs`, `lib.rs`; `crates/striatum-module/src/account_selection.rs` (`ClassContext::read` only); `routing_v1/accounts.rs`; `background.rs` | Writer-conversion worker; S1b merged first (same file, different functions) | none | I1–I20; a tick hook for tests; tighten the `CONTEXT_LOADS` and write-count guards to zero writes per read | Writer transactions per admission → 0 on the fixture with pool targets; live mutex-wait ticks; admission p99 |
| **S6** Shared selection inputs | Build tier-independent inputs once per request; vary only tier, IQ and allowed routes; a `RefusalCode` enum for fallback (Opus P9; Astra P6) | `routing_v1/select.rs` (`select_with_tier_fallback`, `select_at_tier`), `routing_v1/panels.rs` | S1b | none | Service-tier affinity and fallback tests | Tier-fallback scenario CPU |
| **S7a** Typed block evaluator | `Block`/`Scope`/`Cause` and a pure evaluator in core; an adapter that loads the 11 source tables plus clocks into `Vec<Block>`; `eligible`/`probe_eligible`/`origin_applies` call the evaluator; old and new evaluations compared in tests and on the rehearsal copy | new `crates/striatum-core/src/blocks.rs`; `crates/striatum-store/src/fence_rechecks.rs` (adapter); `account_selection.rs` (the evaluator call sites) | S4 | none | Evaluation parity for every (provider, credential, model) at fixed `now` on the fixtures and the copy; I1–I20 | Neutral on CPU; proves equivalence |
| **S8a** Account writer | `AccountWriter` thread with typed commands and group commit; all account writes go through it, keeping the existing SQL; the v1→legacy JSON tunnel replaced by typed commands (A9) | new `crates/striatum-module/src/account_writer.rs`; `account_selection.rs` handlers; `routing_v1/accounts.rs`; report delivery in `main.rs`; `quota_events.rs` | S3, S4 | none | Race tests for I6, I9, I19, I20; crash-before-commit test (no reply, retry replays) | Commits per second and admission p99 under the burst scenario |
| **S8b** In-memory state | `AccountState` behind `ArcSwap`; loader; reload on a `data_version` change; `ClassContext` built from the snapshot; time transitions evaluated on read | new `crates/striatum-module/src/account_state.rs`; `account_selection.rs`; `probe_reservations` | S7a, S8a | none | Load-equivalence property test; I1–I20 | Store reads per account operation → 0; admission and observation CPU (estimate ≤3 ms on the representative fixture) |
| **S8c** `what_if` on snapshots | Pure evaluation over one account snapshot plus one catalogue/config snapshot; delete `evaluation_snapshot.rs` | `routing_v1/what_if.rs`; `crates/striatum-store/src/evaluation_snapshot.rs` (deleted); `select.rs` | S8b | none | The `what_if` no-mutation test; panel dry-run tests | `what_if` sweep CPU |
| **S7b** Block storage | `block`, `probe_authorization` and `success_proof` tables; backfill; writer and loader switched; old tables, view and triggers kept read-only for one release, then dropped in a later migration | migration; `crates/striatum-store/src/fence_rechecks.rs` (rewritten around `block`); backup table lists; the writer and loader | S7a, S8b (only the writer and loader touch storage by then) | Backfill migration. Rehearse: duration, per-kind count parity, evaluation parity at fixed `now`, `backup_exclusion_rehearsal` covering the new tables, tombstones preserved (I15). | I1–I20; `sync` and the 13-arm view gone | `fence_rechecks` frames gone from the `sample`; store size of the block tables |
| **S9** Live/history split | `state.db` plus `history.db`; outbox and mover; explain and list readers over `ATTACH`; retention per file | `crates/striatum-store` (open, outbox, mover); `crates/striatum-module/src/retention.rs`; explain and list handlers; `script/` (offline split); backups | S8a; retention-track decisions | Offline split script (§3.4). Rehearse the whole procedure on a copy: duration, `state.db` size, parity, explain over moved rows, restart. | Outbox crash test (re-copy is idempotent); report-delivery replay test | Page-cache hit rate and request latency under history writes; checkpoint time |

### 5.3 What can run in parallel

```
wave 1 (parallel):  S0   S1a   S2   S5   S3 (after the writer-conversion worker merges)
wave 2:             S1b ──► S4          S6 (after S1b, parallel with S4)
wave 3:             S7a ──► S8a ──► S8b ──► S8c
                                      └──► S7b ──► S9 (also needs the retention track)
```

- S1a, S2, S5 and S0 share no files. S3 shares store transaction code with the writer-conversion worker, so it starts after that merges.
- S1b and S4 both edit `account_selection.rs`, but different functions (the query methods versus `ClassContext::read`). They can be developed in parallel; merge S1b first.
- The retention track runs alongside everything. It changes `retention.rs` and adds prune targets. S9 needs its decisions on windows and partitioning.
- **Checkpoint for the owner after waves 1 and 2** (S0, S1a, S2, S3, S5, S1b, S4, S6). Deploy them and measure live CPU. If it is at or below 1%, the structural waves are judged on latency, correctness and storage, not CPU, and can be paced accordingly.

### 5.4 Expected trajectory (estimates)

| After | Expected effect | Basis |
|---|---|---|
| Wave 1 | Observation lookup 11–15 ms → <0.1 ms; quota-event commits 3 → 1 per event; no inline checkpoints; select CPU on the 800-rule fixture roughly halved | Measured lookup timings; read; §4.3 |
| Wave 2 | No writer transactions on read paths; mutex waits mostly gone; select dominated by scoring, not rule matching | Read; Opus P4 and P6 tick counts before today's fixes |
| Wave 3 | Account operations do no SQL reads; one group commit per batch; `what_if` costs one snapshot per sweep instead of one table copy per call | Design |
| S9 | Hot state isolated from 13 GB of history; retention becomes deleting files | Design; the retention track |

Whether wave 1 and 2 alone reach 1% depends on the live rule count and the post-fix live baseline. S0 measures both first.

## 6. Open questions for the owner

The owner answers Q1, Q2, Q5, Q6 and Q8 in the approval form that accompanies this document. STRIATUM settled the others, as noted in each row.

| # | Question | Recommendation |
|---|---|---|
| Q1 | Should the writer move to `synchronous=NORMAL`? Opus recommends it; Astra objects. On macOS a plain `fsync` hands data to the drive but does not force the drive to flush its cache; only `fcntl(F_FULLFSYNC)` does. So FULL does not guarantee durability across power loss today anyway (Opus §6 Q1). | Not yet. Ship S3 and S5, then re-measure `fsync`. Use NORMAL for `history.db` at S9. Decide `state.db` from the measurement; group commit in S8a makes FULL cheap there. |
| Q2 | What form should the journal take: write-through state tables plus a history outbox, or an append-only event log that is replayed on start? | Write-through. Restart loads tables in well under a second, and replaying an event log adds a second source of truth to keep in step. |
| Q3 | Does anything outside the daemon write the account or block tables in production (operator scripts, manual SQL)? | Treat external writes as unsupported while the daemon runs. Detect them via `data_version` and reload, so an accident costs a reload, not wrong answers. **Settled:** no production writer exists outside the daemon. The fleet inventory of 11 Nov found only read-only openers (the health sentinel and offline research scripts), and the old operator scripts that wrote were deleted from the prefrontal repository that day. |
| Q4 | How many subscription, exclusion and runner route rules, listed credentials and catalogue facts does the live config have? (Counts only. Workers do not read `$HOME`.) | Provide the counts for the S0 fixture. Without them, the bench's 800-rule fixture overstates selection cost and no slice can claim the 1% target. **Settled:** S0 adds a counts-only summary to `ck-striatum --check-config` (rule, credential and fact counts, no names), and STRIATUM runs it against the live settings and store and hands the numbers to S0. |
| Q5 | Should block storage collapse (S7b) come after the in-memory state (S8b) rather than before it? | Yes. After S8b only the writer and the loader touch block storage, so the migration changes two code sites instead of every writer of 11 tables. The typed evaluator (S7a) comes first in either order. |
| Q6 | Do S4 as an interim step, or go straight to S8? | Do S4. It is cheaper, takes writes off reads within weeks, and its maintenance tick and register-at-receipt writes carry over into S8 unchanged. |
| Q7 | While state loads at startup, should account operations answer `account_selection_unavailable`, or should the module's ready signal wait? | Hold the ready signal. The load is estimated at well under a second, and callers already handle a module that is starting. **Settled as recommended.** Broca retries `account_selection_unavailable` forever, so routing must never answer it for a startup load. |
| Q8 | If `state.db` runs NORMAL, a power loss can lose the last committed probe claims, so one extra probe may be sent after the reboot. Is that acceptable? | Yes. It is bounded to one request per block, and the evidence itself is not lost. |
| Q9 | Has migration `082` been rehearsed on the 13 GB copy and deployed? Commit `3231687b` recorded that the owner's rehearsal and deployment were still pending. | Confirm before S2's rehearsal. Each new migration's rehearsal assumes `082` and `083` are applied, and the S7a parity runs need a live copy that includes `082`. **Settled:** both are live. `082` was rehearsed on a fresh copy of the live store and deployed with `743d7e9` on 11 Nov. `083` was rehearsed (29 ms, clean integrity check) and deployed with `5a4f00e` the same day. |
| Q10 | Should history be partitioned (daily files for full detail, monthly for compact records) or kept in one `history.db`? | Decide in the retention track. The outbox makes either choice local to the store crate. Recommend daily files for the 2-day full-detail record. **Settled by the retention ruling of 11 Nov:** full detail is kept 2 days, the compact record 30 days, a 1-in-20 full-detail sample 30 days, and every unpruned table is pruned. The file layout stays the retention track's choice. |
| Q11 | How large should a group-commit batch be? | Cap it at 32 commands or 2 ms of waiting, whichever comes first, and tune it from S0's burst scenario. Admissions must never wait more than a few milliseconds for a batch. **Settled as recommended,** tuned in S8a from S0's measurements. |
