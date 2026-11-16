# Wave 3 inventory

## Baseline and use

This inventory describes `6f2485bb7c9460734af07d3b014785f636d478c1`
(the W3-INV base), not the eventual schemas of the later slices. The reviewed
campaign specification is
`.cortexkit/prefrontal/specs/ct_00000000-0000-421c-98e7-d6378bfe8288.md`;
its chair rulings override the older plan. Source references below are to this
base checkout. The supplied manifest at
`.cortexkit/prefrontal/evidence/slice/manifest.json` records frozen source copies
and references that could not be hydrated; both lists are empty in this slice.
No foreign checkout was consulted.

The baseline command was `cargo nextest list --locked --lib --bins --tests
--message-format json`, using cargo-nextest 0.9.138 on Linux. It listed **2,177
tests in seven binaries**. Test names below are executable names from that list,
not names inferred from commit messages. A missing coverage obligation is marked
**missing**, rather than represented by a proposed test that does not yet exist.

This is a worklist, not permission to change test assertions. In particular,
store-driven races cannot be made into writer races by adding a reload. The
reviewed specification's **Gate** constraint allows only its five setup edits.
Replacement tests require owner sign-off before S8b and a control run in which
breaking the guarded behaviour makes the replacement fail; the old store tests
remain until S7b. Missing coverage must be filled by the slice that first changes
the behaviour, with the same sign-off where it replaces a store test.

Paths in the tables use these prefixes:

| Prefix | Repository path |
|---|---|
| `C/` | `crates/striatum-core/src/` |
| `S/` | `crates/striatum-store/src/` |
| `M/` | `crates/striatum-module/src/` |
| `ST/` | `crates/striatum-store/tests/it/` |
| `MT/` | `crates/striatum-module/tests/it/` |

Expand a prefix before using a `path::name` as a source location. A symbol group
in the production census names every member explicitly; it is not a wildcard
allowance for future entry points. Internal SQL helpers inherit their owning
entry point's disposition, including trigger effects.

## Invariants and executable pins

Seed notation: **H** = request/report handler (including the module's internal
handler helpers); **S** = typed store call; **Q** = raw SQL; **P** = process-wide
quota/listing fixture or injected clock; **N** = no persisted seed. Each entry
states its own seeds, not just the common harness's catalogue setup.

Gate dispositions: **native** keeps the module/pure test's assertions, with only
permitted setup edits. **replace** requires a signed-off writer-driven module
replacement before S8b; demonstrate the guarded failure, retain the old test
through S8c, then delete it at S7b. This is a decision to replace, not a claim
that the replacement already exists. **keep-prune** retains the store entry
point over the new batch implementation: production `M/background.rs` catalogue
refresh still calls `prune_unserved_unpicked_facts`; the reader-plan/watermark
semantics do not change. **schema** is a schema/backup check, not a store-driven
account behaviour test.

Affected-slice shorthand **A** expands to S7a, S8a-2, S8a-3, S8b-1, S8b-2,
S8c, S7b, S9a, S9b-2 and S9b-3. Bench-only S8a-1 and inventory-only S9b-1
change no behaviour but run the same conservative pins. All 27 invariants are
included in each later slice's filter below; this intentionally over-approximates
the blast radius rather than dropping a guard during a structural transition.

| Invariant | Pinning tests (`path::executable name`) | Seeds and Gate disposition (in test order) | Slices |
|---|---|---|---|
| I1 — quota has no fixed expiry; reset is a check | `S/fence_rechecks.rs::fence_rechecks::tests::quota_rechecks_every_fifteen_minutes_and_reset_only_adds_a_check` | S route fence; store reads and transactional claims are under test: **replace**. | A |
| I2 — quota probe every 15 minutes even without readings | `C/fence_lifetimes.rs::fence_lifetimes::tests::fence_policy_intervals_are_fifteen_minutes_and_one_hour`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::a_billing_refusal_converted_without_a_reset_keeps_its_fifteen_minute_check_when_readings_disappear` | N constants: **native**; S credential refusal + P quota replacement/clock, H account requests: **native**, reload after seed only. | A |
| I3 — fresh headroom heals every required pool, newer than reports | `M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::subscription_quota_hold_lifts_on_fresh_headroom_reading`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::run_and_reading_resets_do_not_postpone_checks_and_headroom_lifts_both` | Both P readings/clock and H select/report/account-for: **native**. The all-pools writer recheck also has `M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::s4b_route_recovery_rechecks_newer_sibling_report_under_writer` (S fences, P quota, interleaved store report: **replace**). | A |
| I4 — spend balance does not suppress probes; contradictory headroom is non-quota; exhaustion does not rescale demand | `M/main.rs::tests::xai_spending_limit_with_fresh_zero_usage_is_a_non_quota_refusal` | S decision/identity, P readings; direct reset-fence helper and store claims under test: **replace** with report-driven sequence. **Missing:** one pin covering the complete spend-only → contradictory → consistently exhausted sequence and unchanged IQ demand. | A |
| I5 — non-quota cap one hour, preserve shorter retry | `S/fence_rechecks.rs::fence_rechecks::tests::every_non_quota_origin_caps_at_one_hour_preserves_shorter_retry_and_claims_once`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::pay_per_token_credential_without_reading_keeps_the_one_hour_probe` | Q credential/failure + S other origins; store claims under test: **replace**; P reading + H select/report/account-for: **native**. | A |
| I6 — atomic single claim; loser consumes nothing | `S/fence_rechecks.rs::fence_rechecks::tests::strict_probe_claim_is_atomic_across_distinct_connections`;<br>`S/fence_rechecks.rs::fence_rechecks::tests::direct_pool_acquire_admits_exactly_one_concurrent_probe` | S+Q origins, eight raw connections claim: **replace**, concurrent writer commands required; S pool wall, concurrent pool-acquire calls: **replace**, writer queue is the future arbiter. | A |
| I7 — self-exempt probe, including its route fence, once only | `M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::credential_probe_admission_is_single_and_completion_heals_its_credential`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::stale_api_billing_probe_passes_its_route_fence_once_even_with_spend_reading` | S refusal then H select/account/report/internal stale-roster helper: **native**; S route fence + P spend reading then H: **native**. | A |
| I8 — actual serving identity; anonymous legacy trials only | `M/main.rs::tests::legacy_completed_cannot_heal_a_credential_that_did_not_serve`;<br>`M/main.rs::tests::anonymous_legacy_reported_credential_heals_only_its_claimed_authorization`;<br>`M/main.rs::tests::anonymous_legacy_probe_success_heals_its_actual_launch_authorization` | Each S identity/origin/authorization and direct legacy report helpers: **replace** with writer-driven legacy report coverage, not only seed reloads. | A |
| I9 — selection owner transfers to decision, consumed once | `M/account_selection.rs::account_selection::tests::established_affinity_consumes_its_decisions_transient_probe_once`;<br>`M/account_selection.rs::account_selection::tests::established_affinity_prepared_before_a_transient_claim_cannot_double_send` | Both S decisions/fences/owner claims, internal `serve`; second interleaves another winner: **replace** both with queued-command races. | A |
| I10 — neutral releases matching non-quota generation across alias/target, not quota | `S/fence_rechecks.rs::fence_rechecks::tests::inconclusive_release_preserves_quota_interval_and_a_newer_claim`;<br>`S/fence_rechecks.rs::fence_rechecks::tests::linked_pool_neutral_outcome_releases_alias_and_target_provider_origins`;<br>`S/report_delivery/tests.rs::report_delivery::tests::inconclusive_v1_reports_release_only_their_non_quota_probe_claim` | S fences/claims; S alias/target walls and lease; S decision/fence/auth. Store grant/release/report are under test in each: **replace** all three. | A |
| I11 — authorized scope only; unknown size neither claims nor heals | `S/fence_rechecks.rs::fence_rechecks::tests::success_heals_only_the_known_pool_and_family_and_retains_other_evidence`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::size_shaped_429_restricts_only_its_band_and_only_an_admitted_size_probe_clears_it`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::size_quota_refusal_contradicted_by_headroom_remains_non_quota_until_its_size_probe` | Q account fences, direct heal: **replace**; both P quota + H select/report/account requests: **native**. **Missing:** explicit unknown-size negative pin in this selected set. | A |
| I12 — clock generation distinct from authorization launch time | `S/fence_rechecks.rs::fence_rechecks::tests::consumed_authorization_neutral_release_reopens_the_claim_generation` | S fence, grant/consume/release under test: **replace**. | A |
| I13 — old credential refusals retained, never six-hour expiry | `S/fence_rechecks.rs::fence_rechecks::tests::migration_retires_real_deadlines_but_preserves_quota_and_credential_evidence`;<br>`S/fence_rechecks.rs::fence_rechecks::tests::classification_preserves_old_credentials_and_tolerates_removed_credentials` | Q version-81 migration fixture + store reads/claims; Q standing refusal + store classification/removal: **replace** both for live behaviour. Preserve historical migration coverage separately when retiring the live-store tests. | A |
| I14 — newer evidence wins; lease touch is not evidence; stale recovery cannot clear a newer refusal | `S/key_pool.rs::key_pool::wall_generation_tests::first_pool_refusal_survives_a_later_lease_touch`;<br>`S/key_pool.rs::key_pool::wall_generation_tests::newer_pool_refusal_replaces_a_long_migrated_deadline_with_its_short_retry`;<br>`S/fence_rechecks.rs::fence_rechecks::tests::provider_repeat_keeps_an_independent_model_origin_and_its_generation`;<br>`S/fence_rechecks.rs::fence_rechecks::tests::an_older_completed_decision_cannot_reset_a_newer_failure_backoff` | S pool API; Q migrated pool + S refusal; S route/provider updates; S decision/outcome + Q timestamps. Store operations under test: **replace** all four. I3's sibling-report race pins the concurrent clear side. | A |
| I15 — retirement cannot resurrect; backups/projections carry proofs and authorizations | `ST/backup_exclusion.rs::backup_exclusion::every_routing_table_is_classed_exactly_once_with_a_reason`;<br>`ST/backup_exclusion.rs::backup_exclusion::no_backed_up_table_holds_a_foreign_key_into_a_skippable_one`;<br>`S/evaluation_snapshot.rs::connection_store::tests::snapshot_carries_fence_archive_authorizations_and_triggers` | N schema inspection: **schema** for first two; Q cooldown/auth, snapshot + sync under test: **replace**, with owner approval before S8b. At S8c retain the old store test in an oracle harness when deleting the production projection; delete the store test at S7b, not earlier. **Missing:** an end-to-end backup/restore pin seeded with all proof/authorization/retirement rows. | A |
| I16 — quarantine account-local model/provider errors, preserve siblings | `S/lib.rs::tests::legacy_account_wall_is_quarantined_without_erasing_evidence_or_real_walls` | Q legacy errors and independent real wall; store snapshot under test: **replace** with module snapshot/status coverage. | A |
| I17 — losing concurrent probe retains fence diagnostics | `M/account_selection.rs::account_selection::tests::a_probe_claimed_after_roster_preparation_returns_its_fence_not_missing_candidates` | S fence and competing claim then internal `serve`: **replace** with writer-queued interleaving. | A |
| I18 — scoring/what-if read-only; admission owns materialization | `M/routing_v1/what_if_tests.rs::routing_v1::what_if_tests::what_if_changes_no_rows_even_when_account_loading_reconciles_old_evidence`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::selection_with_unchanged_context_opens_no_account_write_transactions` | Q old evidence then H what-if; S/P prepared context then H selection, counter inspection: **native** both (flush before counters when needed). Decision-seat grants in I23 remain the specified selection exception. | A |
| I19 — established-affinity Hold uses transaction origins, not stale roster | `M/account_selection.rs::account_selection::tests::established_affinity_prepared_before_a_transient_claim_cannot_double_send` | S started pin + due fence; competing claim before internal `serve`: **replace**, same replacement as I9. | A |
| I20 — acquire writer before RMW; busy handler waits | `M/main.rs::tests::route_handler_isolation::a_route_handler_waiting_on_the_store_does_not_stall_other_tasks` | Q second connection holds lock, H cooldown frame runs on real handler: **native**; the lock controls contention, not account claim ownership. | A |
| I21 — shared up-front `write_transaction`, source gate rejects deferred RMW | **Missing nextest pin** for the source-policy gate. `scripts/write-transactions.py` and its Python unit suite are the authoritative source check, run separately by the Rust gate. | Synthetic-source/allowlist and repository scan; not a nextest test, so not presented as one. Do not substitute a transaction-counter test for a source-policy proof. | S8a-2, S8a-3, S8b-1, S7b, S9a, S9b-2, S9b-3 |
| I22 — 60-second unstarted expiry releases exact generation | `S/fence_rechecks.rs::fence_rechecks::tests::expired_admission_probe_cannot_reopen_newer_evidence_or_claim_generation`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_unstarted_admission_returns_its_exact_generation` | S fence + Q admission, store expiry under test: **replace**; S fence + H select/account/report, P clock: **native**. | A |
| I23 — decision probe durable interval/report semantics across selections/panels | `M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_failure_without_account_admission_advances_once`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_ignores_rejected_unknown_and_other_credential_reports`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_late_failure_cannot_advance_a_newer_generation`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_reported_admission_cannot_expire_back_to_due`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_reset_check_cannot_bypass_a_held_slot`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_success_without_account_admission_heals_its_fence`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_unstarted_admission_returns_its_exact_generation`;<br>`M/routing_v1/metered_tests.rs::routing_v1::metered_tests::decision_probe_without_account_admission_holds_for_fifteen_minutes`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::a_decision_owned_probe_answers_other_admissions_with_a_timed_wait` | Each metered test: S typed fence seed + H select/account/report and P clock: **native**. Last: H refusal/claim and after-roster internal handler hook, P readings/clock: **native**. **Missing:** a specific panel-seat decision-probe pin in this selected set. | A |
| I24 — roster probe hold survives later empty roster as timed exhaustion | `M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::a_decision_owned_probe_answers_other_admissions_with_a_timed_wait` | H refusal/decision probe, P clock, deterministic after-roster hook before losing account request: **native**. This pins I24's previously unidentified roster-hold race: the losing request reports timed exhaustion rather than lost authorization. | A |
| I25 — pinned refusal retains availability cause and recovery time | `M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::pin_availability_reserve_names_the_subscription_accounts_weekly_reset`;<br>`M/routing_v1/account_classes_tests.rs::routing_v1::account_classes_tests::pin_availability_timed_account_block_names_its_recovery` | P subscription reading + H what-if/select; H select/report billing refusal then pin request: **native** both. | A |
| I26 — what-if and panel dry runs consume no live probes/writes | `M/routing_v1/what_if_tests.rs::routing_v1::what_if_tests::what_if_changes_no_rows_even_when_account_loading_reconciles_old_evidence`;<br>`M/routing_v1/what_if_tests.rs::routing_v1::what_if_tests::what_if_panel_seats_what_select_panel_seats_for_the_same_seed` | Q old evidence + H sweep; S catalogue fixture + H dry/live panel comparison and write counters: **native** both. | A |
| I27 — reader-plan prune, no-op takes no writer, rowid watermark recheck | `S/prune_facts.rs::prune_facts::tests::no_op_prune_never_acquires_or_commits_on_writer`;<br>`S/prune_facts.rs::prune_facts::tests::prune_rechecks_selections_and_catalog_changes_after_reader_snapshot`;<br>`S/prune_facts.rs::prune_facts::tests::prune_keeps_selection_timestamped_before_snapshot_but_inserted_after_it` | Each Q facts/controlled selection interleavings, store prune under test: **keep-prune** all three. Later history-aware selection-tail lookup must preserve the watermark race. | S8a-3, S8b-1, S7b, S9a, S9b-2, S9b-3 |

## Later-slice nextest filters

Use `cargo nextest run --locked --lib --bins --tests -E "$FILTER"`. The following
is a nextest expression, not a shell glob. It covers every named Rust pin above,
including the store oracles until their approved replacements land. The family
filters deliberately include nearby regressions. Do not use `--run-ignored all`:
the ignored CPU/hold measurements have their own prescribed runs.

```sh
PINS='test(/^(fence_lifetimes::tests::|fence_rechecks::tests::|key_pool::wall_generation_tests::|report_delivery::tests::|routing_v1::(account_classes_tests|metered_tests|what_if_tests)::|account_selection::tests::|connection_store::tests::|prune_facts::tests::|backup_exclusion::)/) | test(/^tests::(xai_spending_limit|legacy_completed|anonymous_legacy|legacy_account_wall|route_handler_isolation::)/)'
```

Each row gives one executable filter assignment. `PINS` expands to the expression
above; thus every slice conservatively protects I1–I27 (I21's source check is
separate). New writer/state/migration tests must be added to the applicable row
when those modules exist; this base inventory does not pretend to list future
tests. Update filters and test paths when an approved replacement moves a pin.

| Later slice | Filter assignment | Additional obligation not yet present at this base |
|---|---|---|
| S7a | `FILTER="$PINS"` | Adapter/evaluator parity on all 14 kinds, both fixtures and boundary times. |
| S8a-1 | `FILTER="$PINS"` | Ignored admission-burst measurement, once per burst, separately. |
| S8a-2 | `FILTER="$PINS | test(account_writer::)"` | Group commit, savepoint/batch errors, crash/reply replay, writer death. |
| S8a-3 | `FILTER="$PINS | test(account_writer::) | test(transaction_contention_tests::)"` | Full writer-handler coverage, source negative and per-table bypass counter. |
| S8b-1 | `FILTER="$PINS | test(account_writer::) | test(account_state::)"` | Startup/load/reload, round trip and command-answer properties; replacement sign-offs already obtained. |
| S8b-2 | `FILTER="$PINS | test(account_state::) | test(reads_off_writer::)"` | Reader-answer property, counted reader negatives, panel/key-pool mix, route-state differential. |
| S8c | `FILTER="$PINS | test(account_state::) | test(panels_estimate_tests::) | test(media_samples_tests::) | test(key_pool_tests::s8c_pool_wall_and_open_offer_match_the_sql_reference) | test(account_writer::tests::) | test(block_state::tests::)"` | Coherent sweep, private quota/listing repairs, pool walls/open offers and moved connection hooks. |
| S7b | `FILTER="$PINS | test(account_state::) | test(backup_exclusion::)"` | Block migration, frozen data, tombstones, stored-deadline provider status cases. |
| S9a | `FILTER="$PINS | test(backup_exclusion::) | test(task_usage::) | test(observed_speed)"` | Split script Python check, open/refusal/history-chain, outbox freshness and idempotent mover. |
| S9b-1 | `FILTER="$PINS"` | Derive windows/predicates/triggers/decision columns, no executable change. |
| S9b-2 | `FILTER="$PINS | test(task_usage::) | test(decision_detail::) | test(retention::)"` | Aging between replay/probe/continuation steps; merged readers and lifetime guards. |
| S9b-3 | `FILTER="$PINS | test(task_usage::) | test(decision_detail::) | test(retention::)"` | Conversion script Python check and copy-only rehearsal/compaction. |

The source-policy check and its Python unit suite still run separately. The
owner runs `bash scripts/rust-test-gate.sh` on the committed merged batch; that
full gate is not replaced by these slice filters. CPU/rehearsal obligations in
the reviewed spec are likewise additional to a nextest expression.

### Placement ruling for mutable history-shaped tables

The owner clarified W1 for this inventory: **`model_route_usage` and
`account_decision` remain whole in `state.db` at S9a**, because live paths enrich
existing usage rows and read/update an account decision's service tier. They
join the S9b aging list beside the six tables already named by the reviewed
specification. S9b-1 must derive and cite each additional live window from how
late those updates can arrive; this inventory does not invent a window or an
outbox-upsert protocol. After aging, their history readers use history, outbox,
then state, as the decision readers do. Thus the specification's explicit
usage-in-history goal is met at S9b for rows outside the live window, not by
moving a still-mutable whole table at S9a.

Evidence: `S/lib.rs::record_usage` (3644–3782),
`S/routing_v1.rs::record_usage_service_tier` (122–154),
`S/account_selection.rs::account_decision_service_tier` (610–635) and
`record_account_service_tier` (790–800). This is the owner's disposition under W1,
not an assumption that usage is append-only.

The owner applied the same ruling to **`routing_call_window` and
`routing_call_count`**: both remain whole in `state.db` at S9a and join S9b's
aging list. S9b-1 derives the closed-bucket window from the point after which no
increment can land in a bucket, including buffered increments. Only buckets
past that derived window may age. Plan §3.4 assigns call-count history to
`history.db`; aging closed buckets at S9b fulfills that placement goal without
introducing cross-file updates to a live counter.

## T membership and enforcement boundaries

T initially contains the 21 account tables copied by
`S/evaluation_snapshot.rs:256–294` other than `model_facts`, plus the five tables
the reviewed specification adds explicitly:

```text
credential_entitlement_fences
account_eligibility
account_route_fence
account_non_quota_route_fence
account_size_refusal
account_non_quota_size_refusal
account_principal_exclusion
account_transient_backoff
account_admission
account_observation
account_fence
account_continue
pool_key_state
pool_key_lease
model_route_state
provider_cooldown_state
fence_recheck
fence_probe_authorization
fence_recheck_route_clear
fence_success_pool
fence_retired_origin
account_observation_event
account_affinity
account_quota_reading
account_quota_completeness
account_vault_roster
```

The membership decisions below extend this list. Membership means **all writes
to the table**, not just writes of block columns. Reading route data, ordering
columns, or an exempt keyed lookup does not authorize an outside-writer write.
Migrations and offline examples are exempt; tests that seed directly are not
production writers. Non-account writes keep the same store writer connection.

| Additional table read/written by an `AccountTx` method or T-writing entry point | In T? | Reason and disposition |
|---|---|---|
| `account_pin` | Yes | Admission/continuation pin ownership is allocator state; commands mutate it and the snapshot supplies current pins. |
| `pool_session_affinity` | Yes | Pool acquire/release uses session→credential affinity to choose the serving identity; commands own it and reads move with pool state. |
| `routing_v1_affinity` | Yes | Selection/report/continuation affinity participates in account identity decisions and is mutated with them; command writes and snapshot reads. |
| `account_selection_salt` | Yes | Stable allocator tie-breaking must be loaded consistently with admissions; initialization is a command, not a lazy outside-writer insert. |
| `account_decision` | No | Account answer ledger and keyed service-tier read/update, not allocator state; admission replay uses `account_admission`. Remains in state at S9a for `account_decision_service_tier`/`record_account_service_tier`; joins S9b aging under owner ruling W1's live-window rule. |
| `model_route_decisions` | No | Whole decision row stays in state at S9a; commands use keyed decision/probe identity/outcome lookups, explicitly exempt from snapshot reads. It is also route history; it is not loaded wholesale into `AccountState`. A probe-binding update still executes inside its owning command. |
| `routing_v1_decision` | No | Decision binding is a keyed report/clear/replay lookup, not account allocation state. Keep whole in state until S9b. |
| `routing_v1_report`, `routing_v1_report_delivery` | No | Keyed report replay/body/answer transaction; effects on T run in the report command, but replay rows themselves are outside the loaded account state. Both remain whole in state at S9a. |
| `model_route_usage` | No | Usage history has keyed delivery enrichment (`record_usage`) and service-tier updates (`record_usage_service_tier`). Remains in state at S9a; joins S9b aging under the same live-window rule, not a new outbox-upsert protocol. |
| `model_route_parent_failures` | No | Route-health/head-withholding index maintained by decision outcome triggers, not account blocks. Keep in state for its live head-failure lookup; the firing report/clear statement owns the trigger write. |
| `model_facts` | No | Explicit catalogue exemption. An insert that fires route-state creation must nevertheless be a writer command; ordinary route catalogue reads stay live. |
| `account_hold` | No | Diagnostic answer append, not the allocator's replay authority. Goes through the same batch's history outbox. |
| `routing_v1_admission`, `routing_v1_panel` | No | Keyed routing reply/panel lifecycle lookups, not the loaded allocator state; state placement does not imply T membership. |
| `fusiform_catalog_cursor`, `quota_snapshot` | No | Catalogue refresh cursor and process-wide quota restart snapshot, respectively; not per-account allocation/block state. Their state placement is independent of T. |
| `quota_reading_log`, `model_route_speed_samples`, `routing_engine_speed` | No | Historical time series/samples, not allocator state. Where coupled to a T change their append goes into that batch's outbox. |
| `routing_call_window`, `routing_call_count`, `routing_engine_eval`, `routing_operator_quality_state`, `routing_operator_quality`, `routing_unscored_admissible_models`, `route_winner_baseline`, `model_route_decision_detail_cursor` | No | Mutable routing metrics/configuration/cursors outside account allocation; keyed updates keep them in state. The detail cursor follows the decision record to history at S9b. |
| `cortexkit_schema_version`; `sqlite_schema`/`sqlite_master`; connection-local catalogue tracking and `current_fence_origin` | No | Runner metadata/schema checks or temporary connection state, not additional durable allocator tables. Startup migrations are exempt; a temp helper is not an excuse for a persistent T write outside a batch. |

From S7b, `block`, `probe_authorization` and `success_proof` enter T. The eight
legacy fence tables and five bookkeeping tables listed as frozen in the
reviewed specification leave T and become read-only audit copies. The three
mixed tables remain in T for their live route/ordering writes, with their block
columns frozen. `fence_origin` is a view, not an additional table; its reads count
as account-state reads until it freezes. Its retirement trigger writes are
charged to the statements that fire it.

## Request-path repairs

These are the nine repairs in `M/account_selection.rs::ClassContext::read_with_catalog`
at lines 1019–1284. `credential_fences::live` is also called by probe reservation.
At S8a-3 a live request checks, sends a command only if work exists, and awaits
it; repairs on the private SQL projection remain direct until S8c. At S8b the
following split is mandatory. A check involving quota or listings must not be
turned into a time-only overlay: those inputs can change back after a repair.
At S8c both kinds operate on sweep-private state, never the published snapshot.

| Repair | What its check reads | S8b disposition |
|---|---|---|
| `has_expired_admission_probes` → `release_expired_admission_probes` (1019–1025) | Request clock; admission state/expiry and the exact bound authorization and clock generation in the snapshot | Evaluate expiry at read time; no request command. Durable release/compaction in a later batch or `Tick`. |
| `has_expired_continuations` → `release_expired_continuations` (1027–1033) | Request clock; open continuation expiry/ownership in the snapshot | Evaluate expiry at read time; no request command; persist on a later batch or `Tick`. |
| `credential_fences::live` (1035, 1315; `M/credential_fences.rs:485–538`) | Snapshot refusal and recorded fingerprint; **current credential listing** (`Unknown`, absent, or listed fingerprint) | Await command for fingerprint adoption or quota-hold clear plus refusal deletion. A later restored listing must not resurrect the deleted refusal. |
| `reconcile_account_eligibility` (1041–1055) | Stored eligibility/fingerprint and **current credential listing**, including listing version | Await listing-reconcile command when changed. |
| `clear_account_size_refusal` (1109–1143) | Matching current size origin/evidence in snapshot; **request quota state**, credential→account mapping, policy and clock; headroom must postdate refusal | Await evidence-guarded clear command. |
| `repair_route_fences` (1159; 3539–3638) | Snapshot route reports/latest report per credential and all account pool fences; **request quota reading**, mapping, policy and clock; `block_end` for every retained pool | Await command; recheck reports and pool requirements in the batch so a concurrent refusal survives. |
| `reconcile_account_route_fence` (1163–1197) | Snapshot route horizon/latest report; **request quota state** reset and fetched time, mapping, policy and clock | Await guarded horizon-reconcile command; retain the current newer-evidence rule. |
| `refresh_credential_quota_holds` (1200; 3797–4035) | Snapshot credential refusals, stored class/`classified`, prior holds and route reports; **request quota state**, mapping, policy and clock | Await command for classification, hold creation/clear and refusal lift. Same-millisecond refusal resets classification without inventing new evidence. |
| Fresh-headroom `clear_fence` (1253–1283; `origin_recovered`:1793–1830) | Snapshot account origin and all matching pool fences; **request quota state** via `block_end`, policy and clock | Await guarded clear command; do not heal only a subset of the account's required pools. |

## Production T entry-point census

Disposition key:

* **W**: every live write runs in an `AccountWriter` batch by S8a-3, including
  wrapper calls, autocommit statements, lazy repairs and trigger effects. An
  enclosing command owns the named SQL helpers; helpers must not open nested
  writer transactions. The source-entry-point list must cover the wrappers,
  not merely SQL leaf functions.
* **R**: account-state reads become `AccountState` reads at S8b; SQL is retained
  only for load/reload, the temporary projection/oracle, and the separately
  counted exceptions below. Read-first checks still have to be read-first.
* **K**: exempt keyed lookup for a row the state does not hold (closed admission,
  absent/expired/released lease, observation event, decision tier/identity or
  report delivery). Repetition/application checks happen inside the batch.
* **O/D**: exempt SQL ordering/route-data read, per the column census. Mixed
  entry points must split their block read from their route-data read.
* **Q**: persisted quota-state restore/inspection; requests instead clone the
  current process-wide quota state. These three T tables are write-only for
  `AccountState`, never loader fields.

All paths below are under `S/` unless prefixed `M/`. **W+R** does not allow a
command to compute from stale SQL after S8b; its account-state computation uses
the current writer-owned state, and its durable statements run in the batch.
The property generator and handler-coverage test must drive every W row.
The reader counter and negative tests must cover every R row, including direct
`with_read` paths. Outside-writer counting is per changed T row, so a trigger or
previously unknown wrapper cannot evade it.

### Admission and evidence (`account_selection.rs:610–2540`)

| Production functions (store wrappers and `AccountTx` methods) | Tables / delegated effects | Disposition |
|---|---|---|
| `account_read`; `account_transaction`; `account_repair`; `begin_account_write` | Callback-defined T footprint; common read/write acquisition boundary | R for account reads; W for writes/repairs; shared batch transaction uses `write_transaction`. |
| `record_account_eligibility` → `record_eligibility`; `reconcile_account_eligibility`; `account_eligibility`; `account_fact_table_exists` | `account_eligibility`; last helper reads schema metadata only | W+R; listing-dependent reconcile is an awaited command. Metadata checks are not account-state reads. |
| `record_account_size_refusal`; `clear_account_size_refusal`; `account_size_refusals` | `account_size_refusal`, `account_non_quota_size_refusal`; registration reads origin sources and writes clocks | W+R; evidence-guarded clear. |
| `record_account_route_fence`; `record_classified_account_route_fence`; `record_transient_account_route_fence`; `record_billing_account_route_fence`; `AccountTx::record_account_route_backoff`; `reconcile_account_route_fence`; store and `AccountTx::clear_account_route_fences_before`; `clear_all_account_route_fences_for_credential` | `account_route_fence`, `account_non_quota_route_fence`, `fence_recheck` via register/sync | W+R; no autocommit clear or recovery transaction outside writer. |
| Store and `AccountTx::route_fence_latest_reports` (store wrapper `account_route_fence_latest_reports`); `account_route_fences`; `non_quota_account_route_fences`; `transient_account_route_fences`; `classified_account_route_fences` | Both route-fence tables | R. |
| `AccountTx::admission`; `demand` (`ACCOUNT_DEMAND_SQL`); `parent_used_account`; `pin_has_started`; `admission_reported_failure`; `decision_context` | `account_admission`, `account_affinity`, `account_observation`; `decision_context` also reads non-T `account_decision` | R; `admission` is K only for a no-longer-open admission. Demand includes **every charged unreleased admission**, finished/expired included, before applying the request's reading floor. Decision context/tier is K. |
| `insert_admission`; `mark_admission_expired`; `mark_admission_started`; `mark_admission_released`; `mark_admission_finished` | `account_admission`; later writer-owned demand entries track charge/finish/release/prune | W; keep durable replay reply. |
| `salt`; `existing_salt` | `account_selection_salt` | W+R for initialize/read; no lazy outside-writer insert. |
| `affinity`; `put_affinity`; `touch_affinity`; `end_lease` | `account_affinity` | R/W as named; allocator state, not SQL on account request. |
| `insert_pin`; `insert_hold` | `account_pin`; non-T `account_hold` | Pin W; hold append belongs to the admission's batch/outbox, not an independently committed answer. |
| `active_fences`; `active_pool_fences`; `active_fence_providers`; `insert_fence`; `insert_pool_fence`; `clear_fence`; `set_fence_recovery_set` | `account_fence`, plus `fence_success_pool` when decoding recovery sets and clocks when registering | R/W; S7b successors operate on `block`/`success_proof`, retaining fence IDs. |
| `admission_exclusions`; `exclusion_count`; `insert_exclusion`; `insert_exclusion_with_recheck` | `account_principal_exclusion`, clocks on timed exclusion registration | R/W; principal applicability is required even though the selection-side origin filter skips it. |
| `observation_by_event`; `observation_by_principal_event`; `observation_backoffs` (`OBSERVATION_BACKOFFS_SQL`) | `account_observation`, fallback `account_observation_event`, transient backoff | Backoffs R; event de-duplication and closed-admission association K. |
| `insert_observation`; `set_observation_effects`; `record_transient_observation_effects` | `account_observation`, `account_transient_backoff`, clocks; observation insert/update triggers write `account_observation_event` | W+R; event trigger is charged to insert/update, not a separate waiver. |
| `credential_entitlement_fence`; `claim_credential_probe`; `bind_model_account` | Credential refusal + probe state; non-T `model_route_decisions` identity binding | R/W; decision binding K, claims through writer. |
| `account_decision_service_tier`; `record_account_service_tier`; `insert_decision`; `account_decision`; `account_decisions`; `record_routing_outcome` | Non-T `account_decision`, `model_route_decisions` | K/ledger readers; writes stay in their owning admission/report batch when coupled to T. They do not become T merely because `AccountTx` exposes them. |

### Continuations (`account_selection/continuation.rs:1–261`)

| Production functions | Tables | Disposition |
|---|---|---|
| `has_expired_continuations`; `release_expired_continuations`; `continuation`; `continued_reservation`; `continuation_has_used_successor`; `record_continuation`; `use_continuation`; `use_observed_continuation`; `latest_used_continuation`; `release_continued_reservation`; `finish_continuations` | `account_continue`, `account_admission`, `account_affinity`; schema-presence checks are metadata | R/W; time-only expiry is interpreted at read, compacted by command/`Tick`; command paths own continuation use/reservation release. Replay lookup remains keyed where the snapshot no longer holds the row. |
| `link_continued_decision` | Non-T `model_route_decisions`, `routing_v1_decision` | Batch-owned K read/copy. S9b explicitly copies only live columns into a slim state row, and copies the predecessor's record from history/outbox/full state into the new history record. |

Module entry points `M/account_selection/continuation.rs` must enqueue the same
commands, not call these writers directly. The continuation expiry/replay
semantics are not replaced by a timer race.

### Key pool (`key_pool.rs:104–601`)

| Production functions | Tables / effects | Disposition |
|---|---|---|
| `pool_acquire` → `pool_acquire_for_route`; `pool_renew`; `pool_release`; `pool_transaction`; helper `touch` | `pool_key_lease`, `pool_key_state`, `pool_session_affinity`; fence claim/auth and sync | W+R; lease identity missing from state is K. Renewal must reinstate expired unreleased leases, return nothing for a released one, and not invent a lease except under today's credential-ID rule. |
| `pool_key_states`; `read_first_key_states`; `key_states`; `read_key_states` | Pool state/live lease counts plus effective fence clocks | R for account blocks/leases; D for identity/last-use. `read_first_key_states` currently can repair: its write branch is W at S8a, then time-only repair is a read overlay at S8b. |
| Store and `AccountTx::pool_wall` | `pool_key_state`, `fence_recheck` and origin sources through register | W+O/R. Preserve independent per-kind observed clocks; do not use lease `updated_at` as refusal generation. |

### Clocks, probes and healing (`fence_rechecks.rs:111–1058`)

The **origin sources** below mean exactly `account_fence`,
`credential_entitlement_fences`, `account_route_fence`,
`account_non_quota_route_fence`, `pool_key_state`, `provider_cooldown_state`,
`model_route_state`, `account_size_refusal`, `account_non_quota_size_refusal`,
`account_principal_exclusion` and `account_transient_backoff`, the eleven tables
under `fence_origin` (migration 082:119–184).

| Production functions | Tables / effects | Disposition |
|---|---|---|
| `enabled`; `snapshot_origins`; `upsert_clocks`; `register`; `sync`; `needs_sync`; `quarantined_origins` | Metadata (`enabled`), origin sources/view, connection-local `current_fence_origin`, `fence_recheck`; `sync` also inserts `fence_retired_origin` | W+R for persistent state. `sync` becomes `Tick`/batch compaction; S7b removes production sync/view access. Temp snapshot construction is owned by these entries, not an extra durable T table. |
| `read`; `read_effective`; store and `AccountTx::fence_rechecks` | Origins, `fence_recheck`, `fence_recheck_route_clear`; quarantine reads mixed rows | R; SQL remains oracle/projection until their specified removal, not a request fallback. |
| `claim_owned`; `claim_with_authorization`; `claim_fence_rechecks`; `claim_fence_rechecks_for_admission`; `claim_fence_rechecks_for_authorized_admission`; `grant_fence_rechecks`; `bind_fence_probe_owner` | `fence_recheck`, `fence_probe_authorization`, origin sources through sync; owner transfer | W+R; all applicable origins claimed atomically by the writer. |
| `record_admission_probe_scope`; `size_admission_for_model_decision`; `admission_size_class`; `bind_decision_probe`; `probe_report_matches`; `fail_authorized_probe`; `probe_authorization_names_credential`; `transfer_probe_authorization` | Admission/probe authorization/clocks; non-T `model_route_decisions` probe identity | W+R for binding/advance/transfer; R for matching/size helpers; decision identity K. |
| `has_expired_admission_probes`; `release_expired_admission_probes` (`EXPIRED_ADMISSION_PROBES`) | `account_admission`, `fence_probe_authorization`, `fence_recheck` | R/W, exact generation release. Time-only check moves to snapshot; writer/`Tick` persists. |
| `release_inconclusive_probe`; `heal_authorized_probe`; `heal_serving_fences`; `heal_serving_fences_in_scope`; `heal_origins`; `successful_quota_pools` | Origin sources, `fence_recheck`, `fence_probe_authorization`, `fence_success_pool`; scoped account clears, credential/route/size deletes, pool/route/provider deadline clears | W+R except proof-only reader `successful_quota_pools` is R. Post-S7b write `block`, `probe_authorization`, `success_proof`, not frozen evidence. |
| `credential_quota_classification`; `classify_credential_quota` | `fence_origin`, `fence_recheck`; stored quota/classified state | R / W+R. Only production classifier caller is module quota-hold reconcile; resets on reported refusal even if evidence text/time stays equal. |

`sync`'s insert into `fence_retired_origin` fires `fence_retire_enforcement`:
deletes credential, non-quota route/size, principal and transient-backoff origins;
clears route/provider/pool deadlines and failure reasons as specified by kind.
These writes belong to **`sync`**, including trigger fan-out onto mixed tables.
`model_route_state_inherit_fence_scope` belongs to any mixed-row INSERT, and
`model_facts_inherit_provider_cooldown` belongs to catalogue fact INSERTs.
Neither trigger is an independent outside-writer path.

### Report delivery (`report_delivery.rs:29–379`)

| Production functions | Tables / effects | Disposition |
|---|---|---|
| `v1_commit_report` | Non-T `routing_v1_report_delivery`, `routing_v1_report`, `routing_v1_decision`, `model_route_decisions`, `model_route_usage`; T `routing_v1_affinity`, continuation/probe/healing/evidence effects below | W+R; report delivery key/body/answer and decision identity are K. All effects and replay answer commit together, reply only after commit. |
| `apply_account_effects`; `record_credential_refusal`; `record_non_quota_route_fence` → `record_account_route_backoff`; `record_non_quota_size_refusal`; `clear_credential_entitlement_fence`; `report_credential_refusals`; `report_route_fences` | `account_route_fence`, `account_non_quota_route_fence`, `account_eligibility`, `credential_entitlement_fences`, both size-refusal tables, clocks | W/R as appropriate; first report owns effects. Helpers may not bypass the report command or directly commit. |
| `insert_usage`; `report_outcome` | Non-T usage/decision; outcome can invoke route-state writer and decision-parent-failure trigger | Batch-owned ledger effects; inherited T effects are W. |

### Other store and module entry points

| Path / production functions | Tables / effects | Disposition |
|---|---|---|
| `lib.rs:2496–2678`: `account_quota_readings`; `account_quota_completeness`; `put_account_quota_poll`; `apply_quota_delta`; `account_vault_roster`; `put_account_vault_roster` | `account_quota_reading`, `account_quota_completeness`, `account_vault_roster` | Writes W (`QuotaDelta`/listing commands), reads Q; do not put any of these values in `AccountState`. |
| `lib.rs:2769–2939`: `record_credential_entitlement_fence`; `credential_entitlement_fences`; `touch_credential_entitlement_fence`; `adopt_credential_entitlement_fingerprint`; `clear_credential_entitlement_fence`; `clear_credential_entitlement_fences`; `clear_credential_entitlement_fences_for_credential` | `credential_entitlement_fences`, clock registration where applicable | W+R, including today's standalone touch transaction and autocommit clears. Module owners `M/credential_fences.rs::{record,live,lift_after_success,clear_quota_holds}` enqueue commands. |
| `lib.rs:687–735`: `clear_route_cooldowns` | `model_route_state`, `pool_key_state`, route-clear bookkeeping; non-T decision bindings/failure copy | W+R/O/D; operator clear must not unconditionally delete other routes' inherited provider evidence. |
| `lib.rs:4208–4862`: `record_provider_cooldown`; `update_route_state`; `set_decision_outcome`; `clear_failure_backoff` | Mixed route/provider tables; outcome also reads `account_admission` timing and non-T decision; parent-failure trigger on decision update | W+R/O/D/K; split block and route telemetry reads. `M/main.rs` legacy report/reset helpers and report delivery enqueue writer commands rather than nested store transactions. |
| `lib.rs:4170–4206,5049–5163`: `get_provider_cooldown`; `build_snapshot` | Mixed provider/route state plus effective clocks | O/D only for retained telemetry; block reads R move to snapshot overlay for `M/routing_v1/select.rs`, `M/select.rs`, `M/main.rs::handle_cooldown_status`. Raw provider status read remains until S7b and is outside the account-operation counter domain (status is not an account operation); do not use that exemption from an account request. Trait SQL `build_snapshot` stays as test oracle over blocks thereafter. |
| `lib.rs:2176–2417,3847–3917`: `apply_fusiform_catalog_for_providers_inner`; `apply_fusiform_catalog`; `apply_fusiform_catalog_for_providers`; `apply_fusiform_catalog_snapshot_for_providers`; `upsert_fact` | Non-T `model_facts`, cursor; fact INSERT trigger creates `model_route_state` with provider inheritance | W for insertion-trigger effect (catalogue writes creating no T row remain normal store writes). Catalogue readers are D/exempt. S7b replaces inheritance trigger; new mixed rows have frozen defaults. |
| `routing_v1.rs`: `v1_record_decision`; `v1_record_selection`; `v1_affinity_decision`; `v1_record_report` | `routing_v1_affinity`; non-T decision/selection binding | Affinity W+R; decisions K. Panel routing/legacy select readers join the same snapshot, not a second live affinity query. |
| `prune_facts.rs`: `prune_unserved_unpicked_facts`; `prune_unserved_after_plan` | Non-T facts/decisions/watermarks; deletes `model_route_state` | W only when plan finds deletions; preserve reader scan, rowid watermark and writer recheck. `M/background.rs` catalogue task owns the command. |
| `retention.rs`: `prune_account_admissions`; `prune_account_affinities`; `prune_account_observations`; `prune_observation_events`; `prune_account_continuations`; `prune_retired_fence_origins`; `prune_v1_decisions`; `prune_v1_admissions`; `prune_v1_affinities` | Deletes T admissions/affinity/pin/observation/event/continuation/retirement/route-affinity rows; guard reads fences/exclusions/pins/admissions/continuations; non-T replay rows | T deletes W; guard reads use writer-consistent state/keyed lookups as applicable. Non-T replay deletes retain SQL guards. At S7b retirement prune becomes `block` retirement-tombstone prune; observation guards read block tombstones too. `M/retention.rs::run_retention_pass` owns scheduling, not its own account connection. |
| `retention.rs:55–117`: `purge_shadow_decisions`; `prune_serve_decisions`; `delete_unreferenced_decisions` (`DECISION_UNREFERENCED`); helper `retention_write` | Non-T account-decision delete, guard reads `account_pin`, `account_admission`, `account_hold` | R reference guards; move hold guard to history/outbox at S9a and preserve atomic live-reference recheck. No T write is implied by the ledger delete alone. `retention_write` inherits each prune's W classification when it does delete T. |
| `diagnostics.rs`: `diagnostic_route_walls`; `diagnostic_account_blocks`; `diagnostic_key_walls`; `diagnostic_account_holds` | Mixed route/pool state, `account_fence`; non-T holds | Diagnostics are outside the account-operation counter domain, but frozen table/column reads still move to blocks at S7b. Live unfrozen metadata/route-data reads remain D; holds move to history+outbox. |
| `evaluation_snapshot.rs::account_evaluation_snapshot` | All 22 copied tables, `fence_origin`, two triggers | Read-only source copy; direct repairs only on private projection. Sole source-gate projection allowance until S8c; delete it then. |

The module-facing writers also include
`M/account_selection.rs::{serve,commit_admission,record_observation,apply_credential_refused,apply_bound_observation,header_fences,apply_confirmation,release,refresh_all_blocks,repair_route_fences,refresh_credential_quota_holds,refresh_credential_quota_holds_in}`,
`ClassContext::reserve_selection_probes` (`GrantProbes`), the nine repairs above,
`M/account_quota.rs::persist_roster_if_changed` and
`M/quota_events.rs::LiveQuota::commit_body`. Their durable T effects all enter
the same writer; process-wide listing/config publication must reconcile through
that writer rather than introduce a new SQL path. Pure prepare/parse helpers do not become
independent writers. `M/background.rs` starts `Tick` at ≤10 seconds. Startup
loads state before ready/hello and lets an early account handler wait; no
`account_selection_unavailable` fallback is allowed for a load in progress.

Module read owners include `ClassContext::read_with_catalog`,
`load_blocks`, `load_fences`, `load_exclusions`, `load_demand`, `account_status`,
`prepare_roster`, `preflight_model_for_class_with_reset`, `account_for`, `serve`,
`replay` and `record_observation` in `M/account_selection.rs`; the read branches
use R snapshots, P quota/listings or K replay lookups as recorded above. Quota
restore readers are `M/account_quota.rs::restore_account_state` and the initial
roster read in `account_quota_loop` (Q). The route-state snapshot consumers are
the three named in the mixed-reader row. `M/head_choices.rs` catalogue and
reasoning-catalogue reads are exempt catalogue reads, not a T-read escape hatch
for an account operation. `read_counts_offline` in `S/reasoning_catalog.rs` reads
the persisted roster only offline and is not a daemon reader.

## S8b-2 recorded exceptions to "no block-table SQL reads on request paths"

After S8b-2 every request-path decision about block rows reads the account
state, and the block read trace (`S/group_reads.rs`, authorizer-based) fails on
any other read. Three classes of SQL read remain, each counted apart by the
trace and each statement pinned in `S/block_state.rs::reviewed_exempt_reads`
(proven keyed by `every_exempt_read_is_keyed`, which rejects any table scan in
its query plan):

* **Decision checks (`STATE_CHECK`).** A decision taken on the state reads, by
  key, the rows it decided on and raises `STATE_DIVERGED` if SQL differs
  (claims, probe answers, healing, sync, pool leases, admissions).
* **Route-state writers' own-row reads (`ROUTE_WRITE_READ`), owner-approved
  bounded exception.** These four writers read the row they are about to write,
  inside the account command that writes it, by primary key or the indexed
  `provider_id`. The merge is decided in SQL, which is the single decider for
  these writes, and a read's result decides only that statement's own write:
  * `record_provider_cooldown`: prior provider observation time, inherited
    route rows, the existing provider row;
  * `update_route_state`: prior route observation time, the existing route row,
    and its provider's `provider_cooldown_state` row;
  * the route-outcome recorder in `set_decision_outcome`: the route's failure
    streak and backoff;
  * `clear_failure_backoff`: the route's failure streak and backoff, for its
    prior-state reply.
  Eight statements in all (three, three, one and one), each a `ROUTE_READ_*`
  constant in `S/lib.rs`. Pinned by
  `the_route_state_writers_read_only_their_own_rows_through_reviewed_statements`,
  which drives each writer, requires all eight to run and checks the key each
  bound.
* **Reviewed reading triggers.** Schema triggers that read block tables when a
  write fires them, each keyed (`S/group_reads.rs::REVIEWED_TRIGGERS`):
  * `model_route_state_inherit_fence_scope`: `provider_cooldown_state` by
    `provider_key = NEW.provider_id`;
  * `model_facts_inherit_provider_cooldown`: `provider_cooldown_state` by
    `provider_key = NEW.provider_id`;
  * `fence_retire_enforcement`: each source table by the primary key in
    `NEW.origin_key`; `model_route_state` by the indexed `provider_id`, with
    `provider_cooldown_state` by `provider_key`.
  A read from any other trigger counts as an ordinary read and fails the trace.

The state's own load and read-backs (`MAINTENANCE_READ`) keep it current and
are not request reads.

## Mixed-table columns

Here **ordering** means a writer's **report-ordering guard**, not a selector's
ranking input. For example `quota_pressure` ranks routes but is route data;
`last_used_at` ranks pool keys but is route data. A healed wall's ordering guard
must remain even when no block/clock remains. Column lists in each cell are
exhaustive and disjoint: 23 route-state columns, ten provider columns and ten
pool-state columns at this base (migrations 001, 015, 032, 039, 048 and 082).

| Table | Class | Every column in class | Production readers / required post-S7b reader |
|---|---|---|---|
| `model_route_state` | Block | `cooldown_until`, `quota_exhausted_until`, `failure_backoff_until`, `failure_backoff_reason`, `wall_class`, `wall_unscoped` | Today `build_snapshot`, `diagnostic_route_walls`, `clear_route_cooldowns`, `clear_failure_backoff`, route writers, `fence_origin`, `quarantined_origins` and `heal_origins`. Move enforcement/diagnostics/clear replies to blocks; **no production SQL read after S7b**. |
| `model_route_state` | Ordering | `wall_observed_at`, `failure_observed_at`, `consecutive_terminal_failures` | `record_provider_cooldown` reads inherited route wall time for fan-out ordering; `update_route_state` reads wall time; `set_decision_outcome` reads wall time, failure time and streak (stale failures and late completion reset); `clear_failure_backoff` reads the streak for its prior-state reply. Baseline `build_snapshot` reads wall time/streak, `fence_origin` reads wall/failure times, and `model_route_state_inherit_fence_scope` reads/copies wall time; their account-state reads move to blocks and the scope trigger is dropped. Only owning writer commands read ordering values inside the batch after S7b. |
| `model_route_state` | Route data | `route_key`, `capacity_key`, `provider_id`, `model_id`, `variant`, `in_flight_limit`, `quota_pressure`, `latency_ema_ms`, `success_ema`, `failure_count`, `quota_error_count`, `last_error`, `last_error_at`, `updated_at` | S8b route-data-only snapshot method (the successor to `build_snapshot`) reads identity, limits, pressure, EMAs, error and counters; `diagnostic_route_walls` retains identity/error telemetry; route RMW commands (`record_provider_cooldown`, `update_route_state`, `set_decision_outcome`) retain prior telemetry; prune uses route identity. `variant` has no independent SQL consumer in today's snapshot (identity is reconstructed from route key); it remains route identity, not a frozen enforcement field. `updated_at` is bookkeeping in updates/prunes, not a report guard. |
| `provider_cooldown_state` | Block | `cooldown_until`, `quota_exhausted_until`, `wall_class`, `wall_unscoped` | Today raw `get_provider_cooldown`, route writers, catalogue inheritance, `fence_origin`, quarantine and healing. After S7b raw status becomes the provider block's stored-deadline term, not its probe-clock term; quarantined rows have no block and answer false. No production read of these columns remains. |
| `provider_cooldown_state` | Ordering | `wall_observed_at` | `record_provider_cooldown` rejects stale provider reports and compares inherited route evidence; `update_route_state` reads provider evidence for inherited wall handling. Baseline `fence_origin`, scope inheritance trigger and provider healing also compare this time; after S7b those block comparisons use block evidence, and the trigger is removed. Persist provider ordering time with the block in the same batch. |
| `provider_cooldown_state` | Route data | `provider_key`, `quota_pressure`, `last_error`, `last_error_at`, `updated_at` | `record_provider_cooldown` and `update_route_state` retain provider lookup/telemetry and pressure propagation. Replacement `model_facts_inherit_provider_cooldown` may copy pressure, error and error time to new route rows, preserving route telemetry. Its route-state row timestamp comes from the inserted fact. `get_provider_cooldown` must no longer use these fields to reconstruct frozen enforcement; status uses the block. |
| `pool_key_state` | Block | `walled_until`, `wall_reason`, `cooldown_until`, `cooldown_reason` | Today `read_key_states`, `diagnostic_key_walls`, operator clear, `fence_origin`, retire/heal. All enforcement/reason readers use blocks after S7b. |
| `pool_key_state` | Ordering | `wall_observed_at`, `cooldown_observed_at` | **Both stay live**: `AccountTx::pool_wall` dynamically chooses the per-kind column and its UPSERT applies only when stored time is null or incoming time ≥ stored time (`S/key_pool.rs:549–575`). Baseline `fence_origin` also reads them with `updated_at` fallback; after S7b that view is frozen. Lease touches do not advance either guard. |
| `pool_key_state` | Route data | `provider`, `credential_id`, `last_used_at`, `updated_at` | `read_key_states` → `pick_pool_key` reads identity/last use; `touch` updates last-use/bookkeeping; `diagnostic_key_walls` keeps identity/bookkeeping; `AccountTx::pool_wall` keeps monotonic update bookkeeping. These SQL reads remain live while deadlines/reasons come from blocks. |

The two important non-deadline block fields are **`wall_class` and
`wall_unscoped`**. They control block masking/quarantine
(`S/lib.rs:5101–5129`, `S/fence_rechecks.rs:236–262`), not route telemetry and not
report order. Both freeze. The replacement catalogue trigger copies **neither**;
it may still copy provider `quota_pressure`, `last_error` and `last_error_at`.
The owner accepted one named schema exception: `wall_unscoped` remains
`NOT NULL DEFAULT 0`, so the frozen-data test expects **0** on a newly created
row; every other frozen column is null there. Existing rows remain unchanged.
S7b's source check must fail if a production query names `wall_unscoped` or
`wall_class` outside the migration and frozen-table adapter. Zero is a real
scoped value, so no production enforcement reader may interpret this sentinel.

`last_error` remains route data despite today's quarantine parser. After S7b
the block carries the enforced evidence; parsing live route error telemetry
must not reconstruct a block or read its frozen flags. The stale-wall and
sub-threshold-failure ordering tests must still hold after heal and reload:
neither a missing clock nor a streak without a deadline permits dropping the
ordering columns.

## Whole-schema S9a file assignment

The registered state migration chain ends at **085** (`S/lib.rs:323–661`).
There are **54 surviving application tables**, plus the shared runner's
`cortexkit_schema_version`. Each has exactly one row below. File assignment is
for the actual S9a position in the chain, **after S7b**: the thirteen legacy
fence/bookkeeping tables then have no production reader/writer and are frozen
audit copies, so they go to history with no ongoing outbox traffic. Their
current enforcing role is recorded in the T census, not silently discarded.

`model_route_reservations`, `catalog_generation`, `aa_index_cache` and
`provider_reset_fences` were dropped (003, 031, 055, 065). `fence_origin` is a
view; `fence_migration_clock`, `expired_fence_origin`, `current_fence_origin` and
catalogue-change tracking objects are migration/connection-local temporary
objects, not additional persistent tables to split. SQLite's own internal
schema/index bookkeeping is rebuilt/copied with its owning file, not treated as
an application-history table.

**S9a does not age the six mandatory tables.** They remain whole in state, as do
the four additional mutable-history tables in the owner's placement ruling.
S9b-1 later adds the live windows, retention predicates, triggers/foreign keys,
decision column readers and NOT NULL placeholders; no unverified duration is
being frozen by this inventory. New S7b tables `block`, `probe_authorization`
and `success_proof`, and S9a's new outbox, are not part of the 54-table base
census; all are assigned to state as writer/loader state.

History-reader codes H1–H6 are expanded immediately below. **Audit** means no
production reader and no outbox merge; offline parity/rehearsal readers read the
frozen copy directly. A state table outside T names the lookup/command that
keeps it there, not just a generic “state” label.

| Table | S9a file | Reason / keeping reader or command |
|---|---|---|
| `model_facts` | `state.db` | Catalogue loader/`get_facts`/catalogue refresh and operator edits. Outside T except its insert-trigger effect. |
| `model_route_state` | `state.db` | T mixed table: live route data and report-ordering guards; block columns frozen. |
| `provider_cooldown_state` | `state.db` | T mixed table: telemetry and report-ordering guard. |
| `model_route_parent_failures` | `state.db` | `parent_failed_decisions` keyed head-withholding read and outcome/delete triggers maintain live index. |
| `route_winner_baseline` | `state.db` | `route_winner_baseline`/`put_route_winner_baseline` singleton comparison/update. |
| `fusiform_catalog_cursor` | `state.db` | `fusiform_catalog_cursor` and catalogue-apply command read/advance cursor atomically with facts. |
| `model_route_decisions` | `state.db` | Mandatory whole table: keyed report/outcome, probe identity, service tier, continuation copy. |
| `routing_v1_decision` | `state.db` | Mandatory whole table: `v1_decision`, selection replay, report and operator clear. |
| `routing_v1_report` | `state.db` | Mandatory whole table: `v1_report`/`v1_commit_report` body and answer replay. |
| `routing_v1_report_delivery` | `state.db` | Mandatory whole table: report-delivery key/body/answer comparison and effect commit. |
| `account_admission` | `state.db` | Mandatory whole table; T open admissions, charged-unreleased demand, keyed closed replay/association. |
| `account_observation` | `state.db` | Mandatory whole table; event-ID de-duplication, effects and bound backoff association. |
| `account_decision` | `state.db` | Owner ruling: keyed account service-tier read/update; ages at S9b, not S9a. |
| `model_route_usage` | `state.db` | Owner ruling: `record_usage` enriches by message ID and `record_usage_service_tier` updates existing rows. Ages at S9b. |
| `quota_snapshot` | `state.db` | `quota_snapshot`/`put_quota_snapshot` restart-loaded process-wide quota snapshot (`M/main.rs:708–756`). |
| `account_quota_reading` | `state.db` | T write target for quota delta/poll; process-wide quota restore, not `AccountState`. |
| `account_quota_completeness` | `state.db` | T write target for current poll completeness; same process-wide restore. |
| `account_vault_roster` | `state.db` | T current listing persistence/restore; not a history log or account-snapshot field. |
| `account_selection_salt` | `state.db` | T allocator tie-break salt loaded with state. |
| `account_affinity` | `state.db` | T current allocator affinity/lease updates. |
| `account_pin` | `state.db` | T pin identity/lifecycle references and admission ownership. |
| `account_fence` | `history.db` | Audit after S7b; live successor is `block`. No production reader/outbox. |
| `account_principal_exclusion` | `history.db` | Audit after S7b; live principal blocks/observation references live in `block`. |
| `account_observation_event` | `state.db` | T retained event-ID replay keys; command de-duplication lookup. |
| `credential_entitlement_fences` | `history.db` | Audit after S7b; block owns refusal class/evidence, no production reader/outbox. |
| `account_eligibility` | `state.db` | T current listing/eligibility reconcile. |
| `account_size_refusal` | `history.db` | Audit after S7b; size blocks remain active in `block`. |
| `account_route_fence` | `history.db` | Audit after S7b; account-route blocks remain active in `block`. |
| `account_non_quota_route_fence` | `history.db` | Audit after S7b; non-quota/transient route blocks move to `block`. |
| `account_non_quota_size_refusal` | `history.db` | Audit after S7b; non-quota size blocks move to `block`. |
| `account_transient_backoff` | `history.db` | Audit after S7b; observation-origin blocks move to `block`. |
| `pool_key_state` | `state.db` | T last-use data and two report-ordering guards remain live. |
| `pool_key_lease` | `state.db` | T live lease state; keyed renew of expired/released/unknown lease. |
| `pool_session_affinity` | `state.db` | T session→credential binding for acquire/renew/release. |
| `account_continue` | `state.db` | T open continuation and keyed continuation reservation/replay. |
| `routing_v1_affinity` | `state.db` | T live decision affinity/reassignment/end. |
| `routing_v1_admission` | `state.db` | `v1_admission`/`v1_record_admission` keyed request fingerprint/answer replay. |
| `routing_v1_panel` | `state.db` | `v1_panel`/`v1_update_panel` keyed panel lifecycle state. |
| `fence_recheck` | `history.db` | Audit after S7b; latest origin clock/tombstone lives in `block`. |
| `fence_probe_authorization` | `history.db` | Audit after S7b; live successor `probe_authorization`. |
| `fence_recheck_route_clear` | `history.db` | Audit after S7b; partial route-clear state lives with blocks. |
| `fence_success_pool` | `history.db` | Audit after S7b; live successor `success_proof`. |
| `fence_retired_origin` | `history.db` | Audit after S7b; retirement tombstones and their retention move to `block`. |
| `routing_operator_quality_state` | `state.db` | Current active singleton read by `overlay_operator_quality`/`operator_quality`, replaced with grades by `replace_operator_quality`. |
| `routing_operator_quality` | `state.db` | Keyed current operator grades read by `overlay_operator_quality` and replaced by `replace_operator_quality`. |
| `routing_unscored_admissible_models` | `state.db` | `operator_quality.rs::observe_decision` UPSERTs current sightings; `unscored_admissible_models` reads them and retention prunes them. |
| `routing_engine_eval` | `state.db` | `record_engine_evaluation` latest-value UPSERT; `engine_measurements` selection read. |
| `model_route_decision_detail_cursor` | `state.db` | `strip_decision_detail` reads/advances cursor while whole decision table remains in state at S9a; moves with record stripping at S9b. |
| `routing_call_window` | `state.db` | Owner ruling: mutable current-window keyed counters (`flush_routing_calls`, `routing_call_counts`) remain whole at S9a; S9b ages only buckets past the closed-bucket window derived by S9b-1. |
| `routing_call_count` | `state.db` | Owner ruling: same flush/read/prune paths, not an append-only answer log; remains whole at S9a and joins S9b closed-bucket aging. |
| `account_hold` | `history.db` | H1; append-only diagnostic answer, not admission replay authority. |
| `quota_reading_log` | `history.db` | H2; append-only quota time series. |
| `model_route_speed_samples` | `history.db` | H3; observed-speed history, freshest sample affects next selection. |
| `routing_engine_speed` | `history.db` | H4; engine timing history, freshest report affects next selection/inspection. |
| `cortexkit_schema_version` | `state.db` | Runner's `route_state` migration ledger stays with state. Create a separate history-chain namespace/ledger on the history connection; do not mistake the old state chain for history migrations. |

### Every production history reader and outbox visibility

The **mover** is S9a's background process that copies committed history records
from the state outbox to `history.db`, commits the history batch, then deletes
the copied outbox rows on the state writer connection. Its separate connection
is the only writer of `history.db`; history retention also runs there. A reader
needing the immediately preceding commit must merge pending outbox rows rather
than wait for that copy.

| Code | Table / readers at the base | Must also read outbox? |
|---|---|---|
| H1 | `account_hold`: `diagnostics.rs::diagnostic_account_holds` (142–170); retention `prune_account_holds`; `DECISION_UNREFERENCED` in `delete_unreferenced_decisions` tests hold references when pruning account decisions | **Yes** for diagnostic answers and reference guards: a just-committed hold must be visible before mover runs. History retention runs on mover's connection; it must also prevent pending outbox rows past retention from later being reintroduced. |
| H2 | `quota_reading_log`: `lib.rs::get_quota_readings` (4945–4985), and retention `prune_quota_reading_log` / trait `prune_quota_readings`. The getter has no production daemon caller at this base. | **No current daemon freshness consumer**. Getter reads history; any newly wired live consumer needing just-committed rows must add an outbox merge. Prune is history-side and must preserve total lifetime across pending outbox rows. |
| H3 | `model_route_speed_samples`: `lib.rs::get_observed_speed_aggregates` → `get_observed_speeds` → `overlay_observed_speed` → `get_facts_with_observed_speed_and_generation`, used by `M/select.rs::prepare_selection_inputs` and `M/routing_v1/select.rs::RequestInputs::read_with_config`; also `get_observed_speed_for_tier` and `get_observed_speed`; retention `prune_speed_samples` | **Yes** for every speed getter/aggregate/overlay: report-to-next-selection freshness cannot wait for mover. History-side prune on mover connection. |
| H4 | `routing_engine_speed`: `engine_measurements.rs::engine_measurements` → `M/routing_v1/engines.rs::select_inner`; `engine_speed_samples` → `M/main.rs::handle_model_facts`; retention `prune_speed_samples` | **Yes** for both readers: the next selection/inspection must see the preceding report's sample. History-side prune on mover connection. |
| H5 (after S9b only) | `model_route_usage`: H3's usage branch plus `compute_class_vector`/`get_class_vector`, `task_usage.rs::{provider_token_reporting,closed_task_usage}` → `M/task_profile_refresh.rs`; usage keyed replay/enrichment paths retain live-window rows in state. `account_decision`: `account_decision`, `account_decisions`, `account_decision_service_tier`, `decision_context`; current-tier rows remain live until the derived window permits aging. | **Yes** for history readers after aging: merge history → outbox → state by key; state supplies mutable live fields while present. Class-vector invalidation on report commit must rebuild over the merged population. S9b-1 must derive windows for both added tables and settle late enrichment/update behaviour before moving rows. |
| H6 (after S9b only) | `routing_call_window`, `routing_call_count`: `call_counts.rs::routing_call_counts`; `flush_routing_calls` updates live buckets and `prune_routing_call_counts` enforces retention. | **Yes** when reading aged buckets: merge history → outbox → state by bucket key without double-counting copies, and preserve today's pending-buffer overlay. The closed-bucket window must ensure no later flush can increment an aged bucket. |

No other table assigned to history above has a production reader at S9a: the
thirteen audit tables are intentionally frozen and not kept in step. Offline
read-only clients outside the daemon, such as the deployed health sentinel and
`scripts/experiments/overqualification-replay/extract.py`, need owner updates
under the reviewed specification's S9a reader constraint: two-file layout at
S9a, slim-row marker and merged-record rules at S9b. They are not daemon readers
covered by this inventory's counter. The split script must still
copy audit tables and preserve their evidence for parity/backup verification.

The owner's call-count placement ruling follows the in-place update paths in
`S/call_counts.rs:69–158`. S9b-1 must cite the closed-bucket boundary before S9b
ages either counter table. As with usage, this inventory authorizes aging only
after the live/update window, not a new cross-file counter-update protocol.

## S8c: coverage of the former dry-run SQL copy

The production copy is replaced by `account_selection::Sweep`: one published
`AccountState` and one `RequestInputs` catalogue/configuration view per sweep.
The sweep's private account copy retains classification, listing, expiry and
headroom repairs across IQ points and panel seats; it is never published. Route
measurements, quota-cache windows, process quota state and the clock remain
per-evaluation inputs. `head_choices` deliberately makes its separate catalogue
read before the sweep. Probe grants and admission writes are disabled.

The former SQL copy contains 23 tables, including
`routing_operator_decommissioned`, as listed below.
The SQL copy survives only under test/test-support as an independent oracle;
its connection wrapper, row-capture hook and DELETE authorizer now live in
`crates/striatum-store/src/connection_store.rs`.

| Former copied object | Coverage in the dry-run path |
|---|---|
| `model_facts` | `RequestInputs.catalog_snapshot` and its resolved/priced facts; one catalogue generation for IQs and panels. |
| `routing_operator_decommissioned` | Resolved catalogue facts carry the operator decommissioning overlay; no separate account read. |
| `fence_recheck` | `AccountState.blocks` clocks, including classification, generation, claimant and cleared evidence. |
| `fence_recheck_route_clear` | `AccountState.blocks` route clears and each derived block's cleared models. |
| `fence_success_pool` | `AccountState.blocks` success pools remove recovered pools from account-fence recovery sets. |
| `fence_probe_authorization` | `AccountState.blocks` authorizations; unstarted-expiry release changes only matching evidence/generations in the private copy. |
| `fence_retired_origin` | Not needed: retirement records guard later SQL evidence insertion; dry runs report no new evidence and never insert into the live store. Expired origins are omitted by the pure derivation. |
| `account_non_quota_route_fence` | `AccountState.blocks` non-quota/transient route origins and raw rows. |
| `account_non_quota_size_refusal` | `AccountState.blocks` size origins; no size-admission command is run. |
| `account_transient_backoff` | `AccountState.blocks` transient-backoff origins. |
| `account_principal_exclusion` | `AccountState.blocks` exclusion origins/rows; no admission principal is allocated by an evaluation. |
| `model_route_state` | Captured `AccountState.blocks` route block columns and clocks. `dry_run_route_snapshot` reads only route measurements from live SQL per evaluation, under the separately counted `route_data` category, excluded from the request-path account-state SQL-read counter. Refreshed diagnostic `last_error` cannot reclassify the captured block's scope. |
| `provider_cooldown_state` | Captured `AccountState.blocks` provider origin, quarantine and inherited-route overlay. |
| `credential_entitlement_fences` | `AccountState.blocks` credential sources; private listing reconciliation, classification and quota-heal operations retain their effects. |
| `account_eligibility` | `AccountState.blocks` eligibility facts; private listing reconciliation removes obsolete facts. |
| `account_size_refusal` | `AccountState.blocks` size sources; expired entries are filtered at read and fresh headroom clears the private source. |
| `account_route_fence` | `AccountState.blocks` raw route evidence, latest-report ordering and clocks; private recovery/reset reconciliation. |
| `account_fence` | `AccountState.blocks` active account/pool fences, recovery sets and origins; private fresh-headroom heals and quota holds. Cleared rows are not loaded. |
| `account_admission` | `AccountState.admissions` open admissions and `inflight` ledger; concurrency diagnostics use the initial snapshot, and expired unstarted probe owners are released privately. No `Admit` is sent. |
| `account_continue` | `AccountState.admissions` open offers/chains; private expiry releases offers. No continuation is created or consumed. |
| `account_observation` | Not needed by selection evaluation: signed-out run exclusions and event de-duplication are admission/observation-handler work, which a dry run never invokes. Account/principal enforcement is already represented by block sources. |
| `pool_key_state` | `AccountState.blocks` pool billing/rate origins and effective probe due times supply the wall flags ClassContext consumes. `last_used_at` is not used by ClassContext. |
| `pool_key_lease` | Not needed: ClassContext tests only pool wall flags; it does not consume the `in_flight` count. Dry runs never choose/acquire a pool key or reserve a probe. |
| `fence_origin` view | `BlockState` source origins plus `fence_rechecks::derive_blocks`, the same derivation used by the SQL adapter. |
| `fence_retire_enforcement` trigger | No durable retirement is needed in a dry run: pure lifetime evaluation omits expired enforcement without a live delete/insert. |
| `model_route_state_inherit_fence_scope` trigger | No route-state row is written in a dry run. Captured source provenance/quarantine fields are already held by `BlockState`, and so are the route measurements: a sweep judges every evaluation against the rows it captured. |

S8c filter: keep the invariant test filters above, use
`connection_store::tests::` for the moved SQL-reference tests, and add
`test(what_if_tests::sweep::) | test(panels_estimate_tests::) | test(media_samples_tests::) | test(key_pool_tests::s8c_pool_wall_and_open_offer_match_the_sql_reference)`.
`account_writer::tests::` and `block_state::tests::` also protect the moved
capture/authorizer hooks and the private account-repair support. Route
measurements are captured with the rest of the account state, so a route-state
write during a sweep changes neither later evaluations nor the scope of a
captured wall (`a_route_state_write_during_a_sweep_leaves_its_later_evaluations_alone`,
in the sweep filter). Capped sweeps and standalone panel estimates also
assert one account snapshot, no SQL reads counted as account-state reads and
no writer commands.
The new sweep tests compare whole responses (timings removed), including every
pick/refusal/exclusion, bar, top-routable and adequate list, panels and head
choices. The quota-replacement fixtures are recorded through the test-only SQL
repair path and replayed through the memory path. The acquisition-count pin in
`s4b_what_if_fresh_headroom_repairs_only_its_snapshot` remains 2: one catalogue
facts/generation checkout and one observed-speed aggregate checkout; its warm
class-vector cache takes no writer checkout.

The routing-service campaign owner accepted this coverage mapping. Before
merge, whole-response SQL comparisons must also cover a pool wall on a key-pool
route and an unused open continuation offer.

## Native block storage: representation-only assertion ports

Owner approval permits physical table/column/query ports inside existing assertions only when their expected values, comparisons and policy predicates are unchanged. A count must retain the old logical source-row meaning, not merely count every row in the consolidated table. Any different expected value or policy claim needs separate approval. Each port is checked by a named deliberate defect: that defect must make the ported assertion fail without changing its expected value.

| Test / location | Old expression | New physical expression | Equivalence and unchanged claim | Failure-control evidence |
|---|---|---|---|---|
| `backup_exclusion::a_store_restored_without_its_history_passes_the_foreign_key_check_and_keeps_serving`, `crates/striatum-store/tests/it/backup_exclusion.rs` | `assert_eq!(rig.count("account_fence"), 1)` | `assert_eq!(SELECT count(*) FROM block WHERE kind='account' AND is_origin=1 AND json_type(meta,'$.fence_json')='text', 1)` (executed through the test connection) | The old table held account-fence source records, including cleared records. Account source payloads retain `fence_json`; source-less imported clock rows do not. The physical count therefore includes cleared source audit rows, excludes principal and other origins and excludes orphan clocks. The expected count remains exactly **1**: restoring without excludable history must not lose the backed-up fence. | `block_marked_excludable` moves `block` from required to excludable backup tables. The filtered test failed exactly at this assertion with `left: 0`, `right: 1`; the unmodified test passed. Evidence: `.cortexkit/internal-docs/research/s7b-activation/backup-control/{backup-proof.json,block_marked_excludable.log}`; runner `.cortexkit/scripts/red-control-s7b-backup.py`. |

### Frozen audit-copy correction

The owner approved correcting an erroneous assertion introduced by the native-block branch: its prototype migration dropped the thirteen legacy tables and `fence_origin`, and its new audit required their absence. That was not the reviewed freeze contract. The reviewed spec's lines 111, 114 and 116 retain these objects as an audit copy, unchanged by later native writers; only a later migration may drop them. This correction is not a reversal of an original policy invariant.

The block migration now retains all thirteen tables, `fence_origin` and the inactive `fence_retire_enforcement` trigger. The required removal of `model_route_state_inherit_fence_scope` remains. The shared committed name catalogue distinguishes retained tables, views and triggers from the one removed trigger; all four categories remain forbidden to production SQL. `FrozenBlockData` compares complete legacy rows, physical rowids and complete owned schema objects (table/view definitions, indexes, autoindexes and triggers), before and after migration and between later observations. The surviving `model_route_state`, `provider_cooldown_state` and `pool_key_state` tables mix frozen enforcement columns with live telemetry and ordering columns. They retain their separate frozen-column/default audit; telemetry rows may still be pruned.

`frozen_block_data::tests::migration_keeps_complete_frozen_rows_and_schema` borrows the schema-93 fixture that compares legacy SQL origins with typed blocks, and adds authorization records for its probe clocks that have an owner, matching their evidence signature, time and claim generation. It asserts that all fourteen frozen relations (thirteen tables and one view) have data, captures the audit before migration, runs the real block migration and compares full rows and schema afterwards. The existing registered-upgrade audit now requires these objects, rather than treating a missing audit copy as success. Separate remote scratch controls append a real `DROP TABLE credential_entitlement_fences` or mutate exactly the seeded credential's `listing_fingerprint`; each fails only this named test, respectively with `frozen audit object missing or replaced` and `frozen legacy rows changed`. As-run SQL lines and logs are retained under `.cortexkit/internal-docs/research/s7b-activation/frozen-copy-control/`; runner `.cortexkit/scripts/red-control-s7b-frozen-copy.py` discovers the migration by its suffix, not a fixed version number.

| Test | Old storage expression | Native storage expression | Unchanged policy assertion and defect control |
|---|---|---|---|
| `account_state::admission_tests::a_probe_release_leaves_a_started_admission_and_its_authorization_alone` | Setup inserts the authorization's seven fields into `fence_probe_authorization`; the scalar count reads that table. | The same seven values are inserted into `probe_authorization`; the same scalar count reads the successor table. The migration copies these seven columns 1:1. | `(authorizations, state.as_str()) == (1, "started")` is byte-for-byte unchanged: an admission that started keeps its probe authorization. Removing the real native unstarted-admission SQL guard in a remote scratch copy makes only this test fail, `(0, "started")` versus `(1, "started")`. This prevents the retained but inactive legacy table from becoming a false oracle. |

### Main integration and observation-success regression ports

The consumer-name migration from main occupies version 094 and its observation signed-out loader index occupies version 095; native block storage is version 096. The registered chain, migration include, version assertions and active control scripts use the new block migration path. Historical control logs retain their actual earlier filenames.

The typed-success observation regressions brought by main retain all of their expectations. Their shared `observation_rows` helper now counts physical `block` sources with `is_origin=1 AND source_present=1`, `retirement_tombstone` rows and `probe_authorization` rows, instead of reading the inactive `fence_origin`, `fence_retired_origin` and `fence_probe_authorization` audit copies. A source-less imported clock or retired source is not an origin of the old view, so the physical-source predicate is essential rather than a relaxed count. No expected zero, one or five, comparison operator, effect assertion or negative-case assertion changes.

| Regression | Preserved claim |
|---|---|
| `observe_success_claimed_observation_retires_origin_and_authorizations` | A successful authorized test retires the observation source, records one retirement and spends all five authorizations, including older inconclusive copies. |
| `observe_success_observation_requires_claim_and_exact_credential` | Unclaimed work, another credential and an ordinary success do not retire the source; the valid claimed success does. |
| `observe_success_observation_newer_than_admission_survives` | Evidence newer than the admission remains an origin and adds no retirement. |
| `observe_success_observation_retirement_rejects_old_replay_but_accepts_new_evidence` | Replayed equal or older receipts stay retired; a newer receipt creates a live origin without losing the earlier retirement. |

The source insertion now applies that receipt-time guard against the native retirement archive inside its insert; archive records remain outside the loaded state. Native observation retirement clears the source through `retirement_tombstone_enforce`, the trigger on the native archive, and deletes matching native authorizations. Migration normalization retires matching, already-cleared observation sources left by the older clock-only success path, preserving the earlier clear time and spending their native authorizations without changing the frozen copies. A migration regression seeds one such source and five authorizations and audits the complete legacy copy around normalization. Verification and named controls for this integration remain pending until the merge commit; the pre-merge successes above are not evidence for the newly merged tree.

### Rare observation-effect retirement lookup

An observation that can create a transient block must reject receipts no newer than a retired generation of that same origin. Retirement history stays outside `AccountState`. The permitted exception is one keyed seek inside this rare writer operation, not a request-path lookup and not a widened own-row or insertion-order category. The counter catalogue names it `REVIEWED_OBSERVATION_RETIREMENT_GUARD`, and `BlockReads::observation_retirement_guards` counts it separately. Recognition requires the writer connection, an active writer command, the actual retirement-table read and the exact observation insert; copying a marker or using the lookup from a reader grants no exemption. Every other source kind keeps its original VALUES insert with no archive lookup.

`observation_retirement_guard(kind,origin_key,evidence_at)` covers the lookup. The shared predicate has `LIMIT 1`; the query-plan test requires SEARCH on that covering index with all three bounds, no SCAN and no temporary B-tree. A populated test with fifty matching generations must return one row and zero full-scan steps. Counter tests require exactly one guarded read of the account-state tables for an accepted or rejected observation insert, zero for an uncovered credential insert, and unchanged zero ordinary request reads. The retirement facts themselves remain absent from the loaded state. After `cargo check --locked --tests` for the store and module packages, the filtered nextest run of block migration, frozen-data, whole-writer, native read-counter and observation-success tests passed all 46 selected tests. Three deliberate source faults in separate scratch copies on the build server each failed only their named test: trusting the marker on a reader (`guard_exemption_reused_by_reader`), dropping the covering index (`guard_index_dropped`), and removing the receipt guard (`guard_removed_recreates_older_block`). The last let a delayed observation recreate a retired transient-backoff block: the upstream replay regression counted one enforcing observation origin instead of the required zero (`left: 1`, `right: 0`). Exact applied lines, full logs and proof JSON are retained under `.cortexkit/internal-docs/research/s7b-activation/observation-guard-control/`; runner `.cortexkit/scripts/red-control-s7b-observation-guard.py`.

The new `AccountTx::delete_account_pins` method also receives its test-only entry trace. Each generated maintenance case now creates two historical finished admissions through the existing typed fixture, deletes that history through the indexed module command, deletes one actual unreferenced pin through the reader-planned module adapter, and deletes the other through the public native prune. Both `AccountTx::delete_account_pins` and `AccountTx::prune_account_pins` must report durable row changes. The missing-producer assertion is unchanged; an empty retention call is no longer counted as evidence that these deleting paths ran.

### Unchanged credential evidence and quota inputs

The real-report witness `whole_writer::credential_witnesses::classified_credential_keeps_its_class_when_same_evidence_is_reported_after_a_reading_change` selects distinct decisions before recording their billing refusals. The first refusal is contradicted by current headroom and is classified non-quota. Replacing the process quota reading with exhaustion does not itself change that classified origin. The second report has a new delivery id but the same code and millisecond receipt time, so its evidence signature is unchanged and its interpretation must remain non-quota. A third, genuinely later receipt must be interpreted again against the exhausted reading. Each mode runs through real requests with strict memory-versus-reload comparison after every operation.

The first execution exposed a cutover defect in `report_delivery::apply_account_effects`: SQLite reports a matched upsert as a changed row even when the evidence is identical. The following unconditional reset of `classified` and `claim_owner` therefore defeated the source upsert's preservation rule. The second report retained `["billing_exhausted",1800000000000]` but changed its quota interpretation to `true`. The writer now resets interpretation and claim only when the code or retained receipt time changes; an older receipt of the same code retains the existing maximum timestamp and therefore the same evidence. `fence_rechecks::register_on` still registers the credential source with its probe clock. The full pre-fix failure is retained at `.cortexkit/internal-docs/research/s7b-activation/unchanged-credential-before-fix.log`; the corrected implementation passed this regression in both reload modes, including the third newer receipt, in the 27-test filtered run. The isolated fault test in `.cortexkit/scripts/red-control-s7b-credential-evidence.py` then restored the unconditional interpretation reset in a scratch copy: only this named real-report regression failed, with identical evidence reclassified as quota. Its printed applied line, full log and proof JSON are retained under `.cortexkit/internal-docs/research/s7b-activation/credential-evidence-control/`.

### Canonical clocks when a source stops supplying an origin

A removed source's non-observation clock must be gone before the same source can return. The shared-size request witness exposed why this is separate from ordinary field equality: both reinserted size records were present, but their retained cleared clock metadata allowed a new 169k-token Admit. The full pre-fix failure is retained at `.cortexkit/internal-docs/research/s7b-activation/shared-size-source-before-fix.log`. The bounded state had already omitted the removed rows, so its cleanup loop could not reset their SQL clock fields.

The approved boundary is transition-time normalization in `block_clock_drop`, not a source-reinsert exception. The trigger is `AFTER UPDATE OF source_present,kind,wall_class`, the inputs to source presence and the stored `is_origin` flag. Only a real source-present 1→0 or origin 1→0 transition runs its own primary-key update, and observation kinds are excluded. It resets `managed`, `claim_owner`, `last_claimed_at` and `claim_generation`; evidence, cleared audit fields and a still-live source's claim history stay. Migration 096 applies the same normalization to imported absent and record-only rows, including stale claim fields on an unmanaged row.

`REVIEWED_CLOCK_NORMALIZATION_TRIGGER` is a distinct keyed-read catalogue entry. `BlockReads::clock_normalization_reads` records actual body executions on the writer inside a command, and `clock_normalizations` counts those executions. SQLite's authorizer sees potential trigger reads even when `WHEN` is false, so preparation is not used as a transition count. A schema test pins the column mask, condition and single primary-key update. The counter tests require one real transition to normalize once, an identical live-evidence rewrite to normalize zero times, the origin-to-record transition to drop its clock and the observation exception to retain its claim history.

The full `AccountState` comparator is unchanged and still does not see rows intentionally dropped from both representations. The separate `assert_canonical_block_clocks_for_test` SQL assertion checks the otherwise unheld fields: a non-observation row with no source or no origin must have no managed or claim clock. It is test/maintenance validation, not request traffic, and does not change the production loader's error contract. Every generated-world step, every transition/counter step and the registered store-copy rehearsal calls it beside the state comparison.

The focused on-disk rehearsal starts at schema93 with one source-absent clock and one record-only clock, runs the actual registered upgrade, and checks the mapped rows requiring normalization against the old copy's independent count (expected2), with zero stale clocks afterwards. It also checks live-origin generation7 and observation-exception generation9 survive, and the complete frozen rows/schema stay unchanged. The registered fixture passed with2 normalized rows and zero stale clocks; live generation7, observation generation9, native load and full frozen audit also passed. The two-package `cargo check --locked --tests` passed. The six other selected checks, including both shared-key families and the full six-seed generator, passed; `becoming_record_only_drops_the_clock_but_observation_source_removal_keeps_its_history` then passed its focused rerun: it now makes an actual class 1→0 transition rather than expecting a transient report to downgrade quota, scopes absence to that exact origin, and changes only observation source presence rather than explicitly clearing the refusal. Production behavior was unchanged by these fixture corrections. The full live-copy rehearsal must still report its actual normalization count and zero remainder. Three separate build-server scratch faults then failed only their named test: dropping the trigger tripped maintenance parity at the first quota-source removal before any reinsert; dropping it while bypassing only that extra physical assertion reproduced the wrongly allowed `delayed-size-origins` Admit with the full-state comparison intact; removing the 1→0 guard made an identical rewrite normalize once instead of zero. The exact applied lines, last-step markers, full logs and proof JSON are retained under `.cortexkit/internal-docs/research/s7b-activation/clock-normalization-control/`; runner `.cortexkit/scripts/red-control-s7b-clock-normalization.py`.

### S7b generated semantic and source-entry coverage

Every one of the six real-writer seeds now asserts an independent 64-scenario obligation set, separate from command selection and observed entry names. The ten basic states are a claimed probe, an expiry-spent authorization, fresh-headroom healing, admission replay, an exact durable fence tombstone, provider and route walls, a pool lease holding a probe generation, an operator clear and actual retention deletion. The remaining grid covers both reload modes of size/principal Admit changes, credential reading/listing cycles, the two causal finished-charge cases, both shared-key families in both clear orders, all five omitted-state cases (with both route/provider older-wall cases), every public report class and effective allocator config publication. Markers follow the actual assertion-bearing operations, never just a call label. The unchanged full-state comparator and source-derived producer completeness check remain separate. The module compiler and complete property passed at `c6db2f5c`; the new wall fixture was corrected to retain the provider's raw cooldown term through a partial route clear, so the combined status stays true while that provider wall is active, not to change production clearing.

The canonical store entry file, `scripts/account-writer-entry-points.txt`, has 49 names on 52 lines. The new source-entry reconciliation reads that actual file and resolves each name against production-declared module adapters, mutable account commands and public native `AccountTx` producers. The three generic store callback boundaries are covered by the observed replacement writer commands: `direct_account_transaction` → `execute_with_error`, `account_transaction` → `execute`, and `account_repair` → `repair`. They are not non-writer exclusions. Every other catalogue name has a same-basename production declaration; an unknown or stale name fails reconciliation, and a writer name needs an observed real producer. `pool_key_states` alone may resolve solely to the already guarded zero-write reader; its per-entry durable-write guard remains mandatory. The independent discovery still includes producers absent from the canonical store-wrapper list, such as the pin-retention adapter and native leaves. Adding a production-declared producer without driving it therefore fails even when the canonical file is unchanged. Reconciliation passed the module compiler and `routing_v1::conformance_tests::whole_writer::generated_request_groups_compare_all_state_after_every_real_step` at `b5734fd6` (1 test passed; 1898 skipped). That property runs six deterministic command-order seeds, asserting complete producer and semantic coverage plus full published-versus-loaded state equality after each real operation. Three isolated remote controls then each failed only that same property: removing the sole unsuffixed `apply_fusiform_catalog` generator call left its source-declared producer unobserved; omitting the paired-charge scenarios left exactly four independent semantic obligations missing: finished-admission demand changes actual round-robin selection and reading-floor allocation, each before and after reload; and omitting only `AccountState::apply_block_rows`'s RAM update failed full published-versus-loaded state equality at `authorization/refusal on newly listed credential`, step 6, before a reload could hide it. Expected declarations, semantic names, the full-state comparator and generation handling stayed unchanged. Proofs and complete logs are retained under `.cortexkit/internal-docs/research/s7b-activation/whole-writer-control/`; the runner is `.cortexkit/scripts/red-control-s7b-whole-writer.py`. These are additional coverage controls, not substitutes for `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py` and `.cortexkit/scripts/red-controls-expiry.sh`, whose activation reruns remain pending.

### Additional native storage-expression ports

These are physical fixture/query ports, not a change to any expected value or policy predicate, and not a sign-off of any module replacement still required by the invariant table. The store compiler passed at `86583af5`. The first completed 20-test batch had 16 passes and four fixture failures: native audit triggers were installed on schema77, the new update witness used a different credential key from its existing report helper, and the strengthened missing-table setup exercised native table deletion rather than opening the schema93 store from before `block` existed. After those fixture-only corrections, all four affected tests passed (4/4, 438 skipped); the 16 unchanged tests were not repeated. Ten isolated controls then each failed only their named test with the invariant expected values unchanged. The controls leave reset/generation fixtures or authorization counts on inactive legacy tables, bypass the native loader's missing-table guard, count clock writes as source writes, omit a source update whose metadata value is unchanged, or corrupt a native account timestamp, route deadline or credential code.

| Location | Native physical expression | Preserved assertion / source-row meaning |
|---|---|---|
| `account_selection::account_fact_tests::credential_probe_helper_consumes_the_extra_reset_check_once` | The setup sets `block.reset_check_at=200` for `kind='credential'`, not `fence_recheck`, which is now an inactive audit copy. | The first due reset check is claimed and the next attempt is blocked; the sibling quota-interval test still requires exactly 15 minutes. |
| `account_selection::account_fact_tests::route_fence_reads_tolerate_a_preupgrade_store_without_the_table` | Open a genuine schema93 fixture, before `block` exists, assert its native table is absent, and retain the original removal of the old `account_route_fence` fixture table. This fixture preserves the historical schema boundary without implying that production supports deleting native tables at runtime. A fault control bypasses the native loader's missing-table guard. | Both existing empty-read assertions remain unchanged. The schema-absence check only pins the fixture's historical precondition; no native schema is deleted at runtime. |
| `report_delivery::tests::completed_reused_decision_without_admission_cannot_resurrect_interrupted_claim` and `report_admission_probe_settlement_retires_only_its_matching_decision_copy` | Migration copies each seven-column `fence_probe_authorization` row unchanged into `probe_authorization`. The fixture changes the latter table's `claim_generation`, and its scalar queries count rows in that same successor table. | Interrupted reports leave zero authorizations. Healing consumes the decision authorization only when its generation and evidence match the admission's authorization. A different decision-copy generation stays; a newer refusal stays active and keeps its old decision authorization. |
| `report_delivery::tests::seed` audit triggers | Source inserts on `block` are selected by the original source's `kind`; refusal updates use `AFTER UPDATE OF meta` on that kind. | Source upserts always name `meta`, even when the metadata value is unchanged. The updates to `managed`, `next_recheck_at`, `quota`, `classified` and `source_order` do not name `meta`. Thus the existing exactly-one transition assertions count source writes, not source-plus-clock bookkeeping. |
| `report_delivery::tests::assert_effects` | Read account `created_at` by the exact account origin key, route `deadline,last_reported_at` from the present account-route source, and credential `code` from the present credential source. | The same scalar expected values and comparisons remain: report time, `(100 + value, value)` and `refusal-{value}`. Kind/key/source predicates retain each old logical source-table meaning; unrelated blocks cannot satisfy them. |

The report audit helper is used by the existing concurrent identical/different outcome/run and crash/replay tests. Genuine schema77 report-migration fixtures reuse decision and continuation setup but never inspect source-transition counts, so native-only audit triggers are not installed on those pre-block schemas. The new update witness seeds the same `credential` key as its existing report helper and tests updates, not insertion beside an unrelated key. Its assertions are not rewritten to accept additional clock transitions, and these ports do not replace the invariant table's separately required module tests that submit real reports or concurrently claim probes through the writer queue.

The ten fault results are retained in `.cortexkit/internal-docs/research/s7b-activation/store-fixture-port-controls/proofs.json` and the adjacent named logs; `.cortexkit/scripts/red-control-s7b-store-fixture-ports.py` reproduces them in a scratch copy. Leaving the reset setup on `fence_recheck` yields `Blocked` instead of `Claimed`; bypassing the loader guard makes the historical empty-read case fail with `no such table: block`. Leaving either the decision-generation fixture or its count on `fence_probe_authorization` fails the settlement test at `other-generation`, zero copies versus one. Counting every native update instead of source upserts yields four route transitions or three credential transitions instead of one; requiring a metadata value change yields zero credential transitions instead of one. The existing unchanged scalar assertions also fail on deliberately corrupted native values: account creation `11` versus `10`, route `(111,10)` versus `(110,10)`, and credential `faulty-refusal` versus `refusal-10`. No expected value was computed from the fault or rewritten to accommodate it. These physical-port controls do not replace the invariant table's outstanding module report sequences and writer-queued probe races, or `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py` and `.cortexkit/scripts/red-controls-expiry.sh`.

### Additional native diagnostic fixture port

`diagnostics::tests::holds_blocks_and_key_walls_list_only_what_stands_now` keeps every existing hold, standing-fence and unexpired-key assertion. Its hold rows still use the live `account_hold` table. The account evidence is now inserted through the real `AccountTx::insert_pool_fence` writer, with the same two ids, provider, account, metadata, receipt and clear times; the key billing/rate evidence uses `pool_wall` with the same two keys, reasons and deadlines. Inserting those sources into frozen legacy columns would leave the native readers empty, not test their standing-state filters. The native producers also maintain the live key metadata needed by the diagnostic joins. At `ac7be9dc`, `cargo check --locked --tests -p striatum-store` and the exact diagnostic test passed (1/1, 441 skipped). Two isolated controls separately put the account evidence or the key-wall evidence back only in the frozen legacy tables, keeping all expected assertions intact; each failed only that diagnostic test. The account control returned `[]` instead of `["f-standing"]`; the key control returned zero instead of one. The actual manifest and named logs are retained in `.cortexkit/internal-docs/research/s7b-activation/diagnostic-fixture-port-controls/`, reproduced by the two `diagnostic_*_fixture_left_frozen` cases of `.cortexkit/scripts/red-control-s7b-store-fixture-ports.py`. No production reader or existing expected result changed.

### Dedicated native rowid and replacement witnesses

`block_state::tests::a_replace_that_moves_a_key_to_a_new_rowid_leaves_no_stale_row` now replaces native credential, account and account-route rows rather than frozen source copies. Its logical counts remain one credential, one then zero active account fences, and one transient account-route record. A retained record is not the same as an origin. `a_replaced_pool_key_row_is_held_once_at_its_new_rowid` retains the exact existing live-metadata count, final reader tuple and new-rowid fingerprint assertions; its formerly single-row wall removal is represented by replacing the live key metadata and removing its separate native billing source in the same command. Both tests still require a physical rowid move.

These standalone tests compare the complete published native state and its physical held-row fingerprints with a fresh native load. The legacy `fence_origin` SQL view exposes the old source columns; those columns stop changing after consolidation, so the view cannot serve as a reference for new native writes. The existing rowid-schema test already iterates all eight current native capture tables and keeps its assertions unchanged. The old generated store property and its helper remain unchanged. Retiring that property requires a reviewed mapping of all 16 decision operations and 13 coverage flags to named native tests and assertions, with any new test for a previously unasserted decision or state shown to fail under an isolated implementation fault. The store compiler passed at `999049ef`. The unchanged eight-table schema and live-pool replacement tests passed in the initial three-test filter; only the affected credential/account/route replacement test was rerun while correcting three fixture expressions. Its native credential receipt now uses `observed_at`, its account row preserves the original empty recovery set and the real source factory's `kept` clear representation, and its record-only account-route count selects that exact source rather than the unrelated `transient_route` reader. No expected logical count changed. The full replacement sequence then passed (1/1, 441 skipped). Four isolated capture faults each failed only its named standalone test: retaining the prior credential row yielded two instead of one; retaining the prior live pool row yielded two instead of one; forgetting to evict a source replaced by an absent row yielded one active account fence instead of zero; and dropping record-only rows yielded zero held account-route records instead of one. The actual proof manifest and logs are retained in `.cortexkit/internal-docs/research/s7b-activation/native-replace-controls/`, reproduced by the four `native_*replace*` cases of `.cortexkit/scripts/red-control-s7b-store-fixture-ports.py`.

### Explicit native claim answers before a renewed deadline

The old generated property's plain, unowned-admission and authorized-admission claim decisions are now covered by the standalone module test `routing_v1::conformance_tests::whole_writer::claims::native_claim_decisions_refuse_a_second_unowned_claim_before_its_deadline`. Each real native producer first accepts its due quota origin, then refuses the same unowned origin before the renewed clock is due. The test repeats both answers with and without a forced reload between them, checks actual production-entry traces, and uses the unchanged complete-state comparator after every command. The existing generated whole-writer property also runs the new refusal assertions; none of its old positive assertions changed.

These assertions were run red **before** their first green run. Three independent scratch faults preserve each wrapper's real claim operation but force its returned Boolean to `true`; each failed only the new named test at its corresponding refusal assertion. The restored standalone test and full six-seed property then passed together (2/2, 1,898 skipped), and remote `cargo check --locked --tests -p striatum-store -p striatum-module` passed at `0711bc031bae5e0dfba45da79ea705beb9b14558`. Actual applied lines, three red logs and the restored green log are retained in `.cortexkit/internal-docs/research/s7b-activation/native-claim-outcome-controls/`, reproduced by `.cortexkit/scripts/red-control-s7b-native-claim-outcomes.py`. This closes those three decision-outcome gaps only. It does not retire the old generated property: its other thirteen operations and thirteen reached-state flags, defined by `decide_both` and `Reached` in `crates/striatum-store/src/block_state/tests.rs`, still require a complete, reviewed replacement-coverage mapping.

### Native probe ownership, report matching and inconclusive release

The separate module test `routing_v1::conformance_tests::whole_writer::authorization::native_probe_authorization_decisions_preserve_exclusive_ownership` uses two real selected decisions. It asserts accepted and refused grant/bind results while one owner holds the probe interval, exact-credential naming and rejection of another credential, accepted exact-report matching and rejection of either a mismatched credential or a report whose caller requested preservation of the existing block. It also asserts old-owner removal and new-owner retention after authorization transfer. Each case runs without a reload and with an explicit reload while the authorization exists. Every command compares published memory with a fresh load, checks canonical stored clocks and frozen legacy data, and rejects writes outside the command batch; entry traces establish that the real native producers ran. The existing `generated_request_groups_compare_all_state_after_every_real_step` property still calls its original authorization group without adding the second decision or the standalone outcome checks.

For the old `heal:inconclusive probe` decision's changed/no-op distinction, `World::native_block_rows` is a test-only maintenance snapshot of every typed cell and rowid in all eight native block tables, including SQL audit rows omitted from bounded memory. The first inconclusive release must change this snapshot; repeating that release must leave it exactly equal, including after reload. The original assertion that an inconclusive result keeps the refusal remains. SQL write counts alone cannot prove this contract because an update may write the same values.

Seven independent implementation mutations failed only this new test **before** its first green run: a foreign grant returned true; a foreign selected-decision bind returned true; another credential was named; another report credential matched; a transfer retained the old owner; an inconclusive release did not delete its `probe_authorization` rows; and a repeated release changed an active origin's recheck generation. After removing every mutation, the standalone test and `generated_request_groups_compare_all_state_after_every_real_step` passed together: two tests ran and passed, including all six generated seeds; the filter skipped 1,899 unrelated tests. Remote `cargo check --locked --tests -p striatum-store -p striatum-module` then passed at `aec7683be45547595dd38a6ae7d3ad9b553f06f5`. Actual manifests and logs are retained under `.cortexkit/internal-docs/research/s7b-activation/native-authorization-outcome-controls/`, with the runner under `.cortexkit/scripts/red-control-s7b-native-authorization-outcomes.py`.

The first runner located three applied lines by searching for the replacement text, which incorrectly named earlier unchanged `return Ok(...)` statements. The initial locator-error manifest is preserved and is not the evidence to cite. The locator now uses the original replaced span; only those three deliberate mutation runs were repeated, and the final manifest and their actual logs name the changed lines 1348, 2064 and 2154. The four other mutation runs and the two passing tests with all mutations removed were not repeated. This new evidence does not remove `block_state::tests::generated_block_writes_keep_memory_equal_to_a_fresh_load`; the rest of its healing outcomes and the thirteen stored-state conditions recorded by `Reached` still need a complete, reviewed replacement mapping.

### Native healing outcomes and partial-pool proof

Four tests in `whole_writer/outcomes/healing/tests.rs` now give exact changed/no-op coverage for authorized-probe healing, failed-probe settlement, public admission-success healing and its private report helper, and public serving-success healing and its private origin helper: `native_authorized_healing_changes_once_then_is_a_durable_noop`, `native_failed_probe_spends_its_proof_once_without_healing_the_source`, `native_admission_success_uses_the_authorized_report_healer_once`, and `native_partial_pool_success_progresses_once_while_the_whole_fence_stands`. They use real issued admissions, actual native producers and full published-state/fresh-load comparison after every command, with and without an explicit reload. The first call changes all-cell durable native storage and the repeated call leaves every cell and rowid unchanged. Terminal probes explicitly spend their authorizations; successful authorized probes clear their exact source, while failed probes keep it.

The public admission-success test uses a whole-account refusal that names no recovery pools. Ordinary serving success cannot clear that refusal, so bypassing private report healing cannot be hidden by the public method's ordinary-success fallback. The public serving test instead uses two distinct named recovery pools and covers only the first. Its source stays active, its original stored recovery set stays exactly two pools, its public active-pool projection correctly lists only the remaining second pool, and its single success proof names the exact active fence, first pool, original evidence time and success time. Repeating the same scope and time must not change that proof. Both private helpers are exercised through these real public consumers; no helper was exposed for testing.

At final fixture source `8a3d0dc560e4a2cc17c8d16075c8a2c8b77cf1d7`, all four tests plus the complete six-seed whole-writer property passed together (five tests passed, 1,900 unrelated tests skipped). Remote `cargo check --locked --tests -p striatum-store -p striatum-module` passed at initial source `79dd1bd9257f81f6c764a37709484216b239c894`; after the partial-pool fixture correction, the affected module's check passed again. Ten independent implementation mutations each failed only the expected new test before its first passing run. The actual applied lines, ten red logs and restored passing log are retained under `.cortexkit/internal-docs/research/s7b-activation/native-healing-outcome-controls/`; `.cortexkit/scripts/red-control-s7b-native-healing-outcomes.py` reproduces them.

The initial tenth mutation did not reach its modified existing-proof update: the new test had incorrectly asserted two pools from `active_pool_fences`, whose contract returns only pools not already proved recovered. The failed 1-versus-2 assertion and initial manifest are preserved as `initial-partial-fixture-failure.*`, not counted as evidence that the test catches the mutation. The new fixture now checks the exact remaining second pool and separately checks both original pools through `account_fence`'s stored JSON. Only the previously unreached tenth mutation was rerun before the first restored green run; the other nine already-named red failures were not repeated. No production code or pre-existing invariant expected assertion changed.

The old `repeated_exclusion` flag means two distinct admissions' source rows for the same principal and credential. Its replacement will assert that exact count and both real scoped Admit refusals, with both reload modes. An earlier coverage draft proposed additionally clearing one source, but routing has no such source-removal policy, so that guarantee was not added. No removal API or policy was added. The coverage mapping for `decide_both`'s sixteen operations and `Reached`'s thirteen stored-state conditions in `block_state/tests.rs` is not yet approved: exact assertions for the remaining held-state cases still need to be completed and reviewed. The old generated store property remains unchanged.

### Two remaining stored-state cases in the generated-property mapping

`two_admissions_keep_distinct_exclusions_for_the_same_principal_and_credential` now asserts two different admission ids' exclusions against the same principal and each same credential, exact cross-admission count 2, one row under each admission, and two actual Admit refusals with `account_not_authorized`. It runs without a reload and with an explicit reload before the count and requests. This test does not add a guarantee about removing one admission's principal-exclusion source while another admission remains excluded.

`an_unclassified_credential_infers_quota_only_from_its_standing_hold` uses an actual issued admission's identity and a real credential-refusal writer without ever calling classification. It asserts unclassified/non-quota with no matching account hold, and unclassified/quota with a standing account hold carrying the exact credential-quota identity prefix; it separately checks whether that hold exists. Both reload modes run. Both tests retain full published-state/fresh-load, canonical-clock, immutable legacy-data and writer-batch checks after each command.

Each new test ran red before its first pass: capping the native exclusion count at one failed only the two-source test, and disabling native standing-hold inference failed only the quota-inference test. After restoring both implementations, the two tests, `reconciliation_writes_once_and_rechecks_new_facts_with_the_same_listing` and the full six-seed whole-writer property passed together (four tests passed, 1,903 unrelated tests skipped). The existing reconciliation test is the nonempty native eligibility-row assertion, rather than the generator's record-and-delete in one batch. Remote module `cargo check --locked --tests` also passed at `c065b7a594e386b2f1d276e03b9d8b893ed632db`. Exact two-fault manifest, both red logs and passing log are retained under `.cortexkit/internal-docs/research/s7b-activation/native-state-flag-controls/`; the runner is `.cortexkit/scripts/red-control-s7b-native-state-flags.py`. No pre-existing invariant expected assertion or production implementation changed, and the old generated store property has not yet been retired.

### Approved one-property retirement and remaining-test census

After reviewing the mapping from each of the old property's sixteen operations and thirteen stored-state conditions to a named native test assertion, and the twenty-two intentional implementation mutations that only their named tests rejected, the routing maintainer approved retiring only `generated_block_writes_keep_memory_equal_to_a_fresh_load` and helpers proved exclusive to it. Commit `0405d5be5f2fcfe8a379250050c6ba327f7513b5` deletes only that test and its private `Reached` struct/`observe` method (155 deleted lines, no additions). The complete pre-deletion search found six tracker references: struct, impl, constructor, one field update, the sole `observe` call and destructuring; all runtime uses were within the one test. Remote `cargo check --locked --tests -p striatum-store` passed with the tracker gone. Shared `Rig::check` assertions, `step`, `Rng`, `decide_both`, every other old invariant, both standalone rowid/REPLACE tests and the module generator are unchanged.

Nextest 0.9.138 registered 442 store tests before deletion and 441 afterwards. The exact set comparison found only the approved retired name removed and no added name; all eight pre-existing ignored measurements are unchanged. All other nonignored store tests were rerun without fail-fast: **433 tests ran, 375 passed, 58 failed, 8 skipped**. These failures remain work, not a passing gate; it has not yet been determined which failures come from test setups that still write retired tables and which expose implementation defects. In particular, `frozen_columns_survive_real_writers_heals_and_operator_clears`, which checks that real writers preserve the legacy audit data, and `bound_observation_replay_keeps_its_last_effect_and_healed_clock`, which checks that replaying an observation's effects does not re-arm its healed block, also failed and need investigation. No assertion was changed to hide any failure. Complete before/after registries, exact name delta, ignored measurements, all failure names and the actual run log are retained under `.cortexkit/internal-docs/research/s7b-activation/old-invariant-registry/`; the runner is `.cortexkit/scripts/s7b-remaining-store-invariants.py`. This narrow approval does not retire any other test or substitute for the three previously requested implementation-mutation audits, the pre-/post-migration answer comparison over a copied store, or the slice's nextest filter and `scripts/rust-test-gate.sh`.

### Retired view output and authorized observation fixture

The reviewed ordering inventory keeps pool wall and cooldown report stamps live, but the retired `fence_origin` view embeds them in `evidence` and `evidence_at`. The accepted audit contract distinguishes stored data from derived output: those two named derived output columns may follow live ordering; every other view output, the view row count and definition, all frozen table rows and all listed frozen columns remain exact. Commit `3331cc9887a0bb13ccf744424bbc9a08945dcf68` implements that boundary without altering production code or discarding any captured raw data. The real-writer audit also updates only the two live pool stamps, asserts that their derived view output actually moved, and checks the frozen audit against both the post-writer and migration baselines, including reload.

Two independent remote scratch controls ran before the first restored pass: changing a frozen pool reason failed only `frozen_columns_survive_real_writers_heals_and_operator_clears` at its frozen-column comparison, and deleting a frozen `fence_recheck` row failed only the same test at its exact legacy-row comparison. Restored writer and migration/schema audits passed together (two tests passed, 439 skipped). The complete six-seed module round-trip property then passed (one test passed, 1,906 skipped), retaining the frozen-data guard after every real command. Store and module `cargo check --locked --tests` passed at the source commit. Exact applied SQL/path/line evidence, both failing logs and both restored run logs are retained under `.cortexkit/internal-docs/research/s7b-activation/derived-view-audit-controls/`; runner `.cortexkit/scripts/red-control-s7b-derived-view-audit.py`. These two controls do not replace the broader fault-injection scripts `red-controls-audit4.py`, `red-controls-audit.py` and `red-controls-expiry.sh` carried under `.cortexkit/scripts/`.

The observation replay failure was mapped separately before any fix. Its setup granted `observation-owner` but called ordinary serving success, which correctly cannot retire an observation-kind origin. Commit `e600efbf796dcaa76dc0466b0119ae5051a3360f` uses the existing exact owner's authorized-probe success instead; it retains every original last-effect, deadline, empty-after-heal, replay and fresh-load assertion, and adds exact physical retirement-receipt and spent-authorization checks before and after replay. No production behavior, API or expected assertion changed. The focused existing test passed (one passed, 440 skipped), and the store test-enabled compiler check passed. The frozen-data writer audit and observation replay test now pass individually over native `block` storage. This is not a new passing claim for the 433-test store census; `old-invariant-registry/remaining-invariants.json` records the complete measured failure list, including tests whose setup or assertions still name retired tables.

### Approved historical/native query plans and current-store snapshot port

The original `every_exempt_read_is_keyed` body, including its `>= 30` guard, now runs unchanged as `block_state::legacy::keyed_reads::every_exempt_read_is_keyed` on genuine schema 93. Its historical catalogue owns 40 statements; thirteen probe/route queries were recovered from this branch's pre-writer-switch `34f91809` checkpoint rather than continuing to call current native SQL helpers. The separately named `block_state::tests::every_native_exempt_read_is_keyed` pins exactly 23 current reviewed entries and preserves the same no-watched-table-SCAN rule. This representation replacement was explicitly approved: 30 was a minimum-count guard against an empty or truncated catalogue, not a requirement to retain obsolete query shapes. A mechanical comparison against `8304d1fa` proves the historical function body is byte-for-byte unchanged.

The separately approved `private_account_copy_for_test` port copies current native state: writable columns only, generated definitions retained, owned indexes and reviewed native triggers restored after data. Its independent literal copy list is checked against the eight native rowid tables; the snapshot test also compares owned index names/SQL, requires the covering observation-retirement index and forbids copied frozen compatibility objects. Original authorization **1**, retirement **1**, copied enforcement **0**, and untouched source **1** assertions retain those values on native physical queries. Trace spelling and diagnostic fixtures likewise retain every original counter, route-name, class and timestamp expectation. The thirteen frozen audit tables are again classified as required backup evidence; the whole-schema file assignment above requires preserving their evidence for parity and backup verification. Source commits: `a2b540711ace61e75f4f7493d7f07ea12c1f169a` and fixture-only required-quota correction `9e95e1a4ab7cb0a7a533aae8faf19e6a3f76ec98`.

Six independent scratch defects each failed only its named test: an installed missing provider index exposed real table scans; an unwatched native block read failed **0/1**; ignoring a route's future quota-exhaustion deadline omitted it from diagnostics; a missing copied authorization failed the snapshot read; missing native retirement enforcement failed its original **1/0** assertion; and a missing frozen backup classification failed schema equality. The restored group passed **seven** tests, and the accidentally omitted restored diagnostic then passed separately (**one** test); this is not claimed as one eight-test run. Both direct module copy callers passed together (**two** tests, 1,905 skipped), and the affected store/module test-enabled compiler checks passed. Both the genuine schema-93 and native-23 query-plan tests passed in the seven-test group.

Unsuccessful attempts remain evidence, not defended claims: runtime writer-side index removal did not make a pooled-reader plan test fail; a diagnostic mutation initially stopped in the new fixture at a required quota-field constraint; and a shadowed test-name variable caused the restored runner to select seven rather than eight tests, which its exact-count guard correctly rejected. Only the affected missing test was run after correcting the runner. Actual logs, six final proofs, historical-body equality and the complete representation map are under `.cortexkit/internal-docs/research/s7b-activation/` (`native-catalogue-port-controls/`, `native-representation-replacements.md`); runner `.cortexkit/scripts/red-control-s7b-native-catalogue-ports.py`. No other old invariant is retired by these approvals. The copied-store differential, `red-controls-audit4.py`, `red-controls-audit.py`, `red-controls-expiry.sh`, slice nextest filter and `scripts/rust-test-gate.sh` remain outstanding.

### Four unchanged-assertion native fixture ports

Commit `1a020cff8119c94bd7c6dd59b012e811948f669b` ports only the physical setup of four more existing invariants: `read_first_repairs::pool_repair_rechecks_newer_writer_without_repeating_row_changes`, `read_first_repairs::pool_sync_repairs_expired_unmanaged_wall_once`, `fence_rechecks::tests::classification_preserves_old_credentials_and_tolerates_removed_credentials` and `fence_rechecks::tests::strict_probe_claim_is_atomic_across_distinct_connections`. Pool refusals now enter native `block` as unregistered sources alongside live pool telemetry. Credential, account and account-route fixtures use existing typed source constructors; operator removal changes native source presence. The competing-writer hook, eight distinct SQLite connections, immediate transactions, timestamps and all original command/row-change/commit assertions remain. Mechanical comparison against `f3f5498b` confirms that all **19 original assertion macros** are byte-for-byte identical (10, 4, 2 and 3 respectively).

Four isolated scratch defects each failed only its named test before the restored pass: a repeated durable clock write failed the stale-repair **1 versus 0** assertion; skipping the expired-source repair failed the commit **0 versus 1** assertion; rejecting the old credential failed **false versus true**; and ignoring the retry time set by an earlier probe claim on a non-quota refusal let **8** connections win instead of **1**. With all defects removed, all four tests passed together (four passed, 438 skipped), and `cargo check --locked --tests -p striatum-store` passed. Applied source lines, actual failing logs, the unchanged-assertion comparison and restored log are retained in `.cortexkit/internal-docs/research/s7b-activation/four-native-port-controls/`; runner `.cortexkit/scripts/red-control-s7b-four-native-ports.py`. No production implementation or expected assertion changed, no test was retired, and no unrun full-suite result is inferred from these four checks.

### Independent native SQL reference for the shared state invariant

Commits `32213fdc88afd1311a1fa28f035be525f608564d` and `33fb61eaa7c395c2bf5eec44ad0238804862ec89` implement the `Rig::check` representation port [explicitly approved by the campaign task-giver](../../.cortexkit/internal-docs/research/s7b-activation/native-representation-decisions.md#shared-sql-oracle). Its expected source origins now come from `block_state/native_sql/tests.rs`: test-only, independently written scalar-column SQL and row decoding, sharing no RAM loader/projection helper or production SQL constant and creating no production view. It includes live report stamps, credential-linked quota classification and inherited-route exclusion. All existing shared comparisons remain. Five original rollback/delete/outside-write/stale-claim tests retain their logical expectations and real races; four retired table-slot operands now address native observation sources, and the task-giver's [separate sampled-name ruling](../../.cortexkit/internal-docs/research/s7b-activation/native-representation-decisions.md#outside-writer-table-sample) changes only `account_transient_backoff` to `block`. The whole-table deletion still has no WHERE clause.

Mechanical comparison of **41 original assertions** permits exactly those four physical operands and the one sampled physical name; every other assertion byte, numeric value, result and reload count is unchanged. Three isolated defects each failed only its expected test: dropped RAM origin and shifted SQL reference deadline both failed the shared source comparison, while skipping the real command-savepoint rollback failed the original memory/fresh comparison. The initial six-test restored run passed five and failed only the obsolete sampled table name; after the sampled-name correction, only the affected test was rerun and passed (one passed, 442 skipped), and the store test-enabled compiler check passed again. All six checks are therefore covered, not claimed as a new combined six-test green run. The three controls and five already passing tests were not repeated. The initial failure, both assertion proofs, actual controls and focused green log are retained under `.cortexkit/internal-docs/research/s7b-activation/native-shared-oracle-controls/`; full representation map `native-representation-replacements.md`, runner `.cortexkit/scripts/red-control-s7b-native-shared-oracle.py`. No further invariant test was retired. Remaining work includes the other invariant ports, copied-store differential, `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py`, `.cortexkit/scripts/red-controls-expiry.sh`, slice nextest filter and `scripts/rust-test-gate.sh`.

### Native missed-write reconciliation and stale-heal fixtures

Commit `96b2947e65a6585de6f2071a323ced444f313aab` ports only the physical setup and queries of four existing invariants. Native observation sources drive tick detection and the periodic in-place deadline comparison. The two-store comparison retains all three cases—native pool deadline, live key last use and native managed clock evidence—and the original alternating six-tick schedule per store. The stale-heal test uses the real account-route writer and native live report ordering, retaining its intentional stale-state injection and exact two-load retry. A refusal reported after the serving request was admitted still survives that request's success; if the retried command also loads stale evidence, it still fails with the original divergence error instead of healing the refusal. The unseen-write hooks and real writer bypasses remain; no manual refresh or expected-value change was added.

Python 3.13.5 verified all **13 original assertion macros byte-identical** to `df5fab8f` (4, 3, 2 and 4, without normalization). The test-enabled store compiler check passed, and the first exact nextest filter passed all four tests (four passed, 439 skipped). Evidence is carried in `.cortexkit/internal-docs/research/s7b-activation/reconciliation-fixture-ports/`; comparison script `.cortexkit/scripts/check-s7b-reconcile-fixture-assertions.py`. No production change, invariant retirement or added negative-control run is claimed. No claim is made here about whether the full suite passes.

### Native due-credential and stale clock fixtures

Commit `18e3eb1a611fdbb912f2a924c678603ea9916dea` ports the shared due-credential fixture to the real refusal writer and current synchronization, with its provider, credential, refusal code and observation time unchanged. Five callers verify that a stale read of a preserved report does not settle a probe, an unseen owner change forces a retry, an admission releases the generation it actually holds, binding a selection moves both clocks to the decision, and both authorized-success entry points heal only after reading the grant. The owner and generation SQL now names the `block` rows whose `managed=1` and `is_origin=1` flags identify stored probe clocks. The principal-exclusion fixture uses the existing typed native source constructor with all original fields and no new registration or expiry effect. The reserved-admission row and its surrounding foreign-key disable/restore remain. Tests still change SQL while suppressing capture, then deliberately load the older snapshot to prove the command detects it; no extra refresh is added.

Python 3.13.5 confirms all **23 original assertion macros byte-identical** to `b5bc734b`, across the five helper callers, scoped-exclusion check and already-native pool-lease check. The test-enabled store compiler check passed. The first exact seven-test nextest filter passed all seven (seven passed, 436 skipped), retaining every original result expectation and the requirement to load the deliberately stale state once, detect its disagreement with SQL, then load a fresh state for the retry. Artifacts are carried in `.cortexkit/internal-docs/research/s7b-activation/stale-clock-fixture-ports/`; comparator `.cortexkit/scripts/check-s7b-stale-clock-fixture-assertions.py`. No production change, invariant retirement, added negative control or claim that the full suite passes accompanies these fixture ports.

### Native serving-scope, recovery-set and superseded-probe fixtures

Commits `a4c45e8a11296acfa57c0e60105add7a32b97ff5` and `5316d0ee6369d81146e89f8bc0398378a4a139d1` port four fixture/query paths. Ordinary serving success sees the same credential/model refusals, old/new credential observation times and account/pool holds through existing typed native constructors, then samples current native sources. The unchanged expected rows preserve the other login, model, account and uncovered pool. The growing-recovery-set test still makes a clear decision from memory with one pool while SQL holds two, detects the disagreement, retries on a fresh load and retains the block. Both superseded-probe tests use current synchronization while preserving both claims, inconclusive release, all success entry points and the expectation that one clock remains.

All **eight original assertion macros are byte-identical** to `c6de8ed5` (Python 3.13.5, without normalization). The initial store check found `E0507` because the repeatable fixture closure consumed its captured recovery vector; cloning that vector per invocation fixed the compiler error without weakening the existing `Fn` contract. The affected store check then passed, and the first four-test filter passed all four (four passed, 439 skipped). Evidence is carried in `.cortexkit/internal-docs/research/s7b-activation/serving-pool-fixture-ports/`; comparator `.cortexkit/scripts/check-s7b-serving-pool-fixture-assertions.py`. No production change, assertion rewrite, extra refresh, invariant retirement or additional negative-control run is claimed; this is not a full-suite result.

### Native per-input pool decisions and raw observation fixture fidelity

Commit `a599170d3440cd2a9efb2671ab5fced99b1b7774` ports all **18 original stale pool-input cases**, retaining their labels, actions, fixed fresh answers, separate fresh-store comparison and exact two-load retry. Refusal evidence and probe schedules now occupy `block` rows, model-specific clears occupy their `cleared_models` arrays, and successful pool coverage occupies `success_proof`. The old provider/route tables still hold ordering and error metadata, which these fixtures continue to insert and compare. Raw sources stay unregistered, so the missing-clock case is not replaced by a writer that already installs its clock. Account IDs, fence IDs, times and pool/recovery membership are unchanged.

The shared raw observation stub also now exactly matches the old stored row: `json_array(evidence_key,effects_json)` produces `["k","[]"]`, not a real receipt's `[receipt,index]`, and its non-JSON key names no observation. `observed_at` remains NULL as the migration requires. Its evidence time and deadlines did not change. All six affected capture/reconciliation callers were rerun: whole-table deletion, failed-command rollback, an outside-command write, another connection's pre-BEGIN commit, tick detection and periodic in-place comparison. They retain their original assertions, separate SQL connections and deliberate suppression of captured writes where used. Python 3.13.5 verified **23 original assertion macros byte-identical** to `d97e3e29`, all 18 case labels in order and every fixed fresh-answer literal count. The affected store check passed. The filter selecting the six named callers and `a_pool_decision_on_a_state_stale_in_any_one_input_runs_again_on_a_fresh_load` passed on its first invocation (seven passed, 436 skipped); the assertion-proof artifact lists all seven selected test names. Artifacts are carried in `.cortexkit/internal-docs/research/s7b-activation/pool-input-fixture-ports/`; comparator `.cortexkit/scripts/check-s7b-pool-input-fixture-assertions.py`. No production change, assertion rewrite, omitted case, extra refresh, invariant retirement, added negative control or full-suite passing result is claimed.

### Native authorized whole-account recovery

Commit `86a89fe622c6666413b824a377a9b2d105b27935` ports only the shared account-source fixture and physical reset-clock operand for the four authorized recovery tests. The existing native constructor keeps the minted `whole` fence ID, older observation/creation time, source metadata and original recovery pools without registering a new clock as a fixture side effect. All four test bodies and shared claim/settlement helpers retain their original assertions. Each stage still runs under a SQL savepoint and rollback before the working-state path, comparing its answer and every block-table row. The tests preserve successive `a`/`b` pool proofs, simultaneous coverage by `both`, unknown-scope refusal to heal, empty-set clearing, exact retry/reset scheduling, claim release and authorization retirement.

Python 3.13.5 verified **22 original assertion macros byte-identical** to the pre-port fixture revision `def4b665` across the four tests and three helpers, without normalization. The affected store check passed. The filter selecting all four `authorized_whole_account_recovery_*` tests passed on its first invocation (four passed, 439 skipped). Actual proof and test output are carried in `.cortexkit/internal-docs/research/s7b-activation/authorized-recovery-fixture-ports/`; comparator `.cortexkit/scripts/check-s7b-authorized-recovery-fixture-assertions.py`. No production change, assertion rewrite, extra refresh, invariant retirement, added negative control or full-suite result is claimed.

### Migrated source deletion: production freshness rather than a poisoned load

Commit `bbd44c7e20ed31cf2e3238bbf04991ae9e5e1e9f` first ran the real migration over the old `stale-sync-drop` before/after fixtures. Migration retains a source-deleted provider clock only as an unloaded audit row; recreating the source yields a live native source and clock. Injecting the earlier empty snapshot instead of returning the writer's requested fresh SQL load failed the original two-load requirement (one load observed, two required). The two-load count was left unchanged while production reachability was investigated. Production refresh checks `data_version` before the batch, checks again inside the write transaction and loads block state from SQL; only the test bridge `TestOwner::load` substitutes the earlier snapshot for that fresh SQL result. This is therefore the test-path-only branch, not a demonstrated production sync gap.

Commit `eb5b37277b8b94533c82e0d9c1398dcea2bc05ed` keeps the two-load requirement and drives native removal/restoration through two real outside-connection commits. The before-BEGIN barrier confirms the first freshness load saw no source or clock, then commits restoration; the locked version check performs the second load and preserves the live provider clock. No injected stale result, suppression of captured SQL writes, manual reload or production code change is used. The affected store check passed. The first invocation of `stale_sync_drop_migration_representability_probe` using the real-commit fixture passed (one passed, 443 skipped). The original failed output, before/after evidence, reachability citations and actual passing output are retained under `.cortexkit/internal-docs/research/s7b-activation/stale-sync-migration-probe/`. At that diagnostic checkpoint, the original `sync_decisions_taken_on_a_stale_state_run_again_on_a_fresh_load` ten-case test was still unported. Its later native implementation and isolated controls are recorded below. That diagnostic checkpoint did not include the independent SQL reference or the deliberately wrong-reference control; their later results are recorded below. No original assertion was changed, no original test was retired and no full-suite pass is claimed.

### Main's loader index and native block migration 096

Merge `73f5ee25f4f09bfbb74f1835c9d2f9bbe9904632` retains main `31d6209e` and its observation-loader index as migration 095; the byte-identical block migration is now **096**. Registration, its include, temporary-function installation, physical version assertions, active control-script paths and current inventory/map references move together. Historical control outputs retain their actual filenames. Both admission-start merge conflicts keep writer-entry tracing that verifies the account-writer command boundary, alongside main's late/terminal start behavior. Incoming main dependency updates were installed/validated offline with cargo 1.100.0: 239 locked packages, three workspace members.

The first merged check found an array-extent mismatch; `ea7d441f106b547260c67afd49d7f893e49cfbd7` counts 95 registry entries while retaining last schema version 96. The next check reached main's one-model dry-run caller, whose native counterpart was missing. `7d0e2d66d22f5b91fce8835429dea94f1e90850d` ports the existing contract: match the provider, credential and model, and clear only route evidence whose latest report time, or recorded time when unreported, is strictly before the cutoff. A new test covers the cutoff boundary and keeps other models, credentials, providers and refusal kinds. No original logical assertion changed.

The three-crate test-enabled compiler check passed after those disclosed fixes. Disabling only the new clear body in a writable scratch copy failed exactly its named regression (one failed, 445 skipped); the implementation was then restored byte-for-byte. The first restored filter passed **15/15** (431 skipped): all 12 native migration/metadata/retirement tests, frozen-data migration, the migrated provider test that verifies removal/restoration commits from separate SQLite connections and the one-model regression. An earlier control-runner setup attempt was refused by tracked-file protection before any mutation or test, so it is not counted as a test failure caused by the intended defect. Actual proof/logs and the integration map are carried in `.cortexkit/internal-docs/research/s7b-activation/merge-096-one-model/`; runner `.cortexkit/scripts/red-control-s7b-one-model-dry-run.py`. All jobs ended and root staging was removed. At that integration checkpoint, the original ten-case `sync_decisions_taken_on_a_stale_state_run_again_on_a_fresh_load` test still needed independent SQL expected rows for install, renew, drop, retire and quarantine decisions; its later completion is recorded below. At that integration checkpoint, remaining fixture work included `memory_matches_the_view_and_the_adapter_on_the_parity_fixture` for all fourteen source kinds, `sync_and_register_decided_on_the_state_equal_sql_over_generated_writes` for generated registration/renewal sequences, and unported legacy source setups in the fence-recheck and route-state-store tests. Also pending are the copied-store rehearsal, `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py`, `.cortexkit/scripts/red-controls-expiry.sh` and final gates. No full-suite pass is claimed.

### Native ten-case synchronization decisions and independent SQL

Commit `7e53b0596594839592e3dc102317563329aebd12` ports `sync_decisions_taken_on_a_stale_state_run_again_on_a_fresh_load` without changing either original assertion: exactly two loads, and row equality with the SQL reference. Python 3.13.5 confirms both macros byte-identical to `8da4a97b`, with all ten case labels in the same order. Original before/after fixtures run on schema 095 and reopen through migration 096 before routing-state owners are installed. The independently written `cfg(test)` SQL reference snapshots sources, handles retirement and retry scheduling, registers pending rows and drops clocks absent from the source view; it uses no production sync helper, scheduler, SQL constant or RAM loader. The comparison includes every column of retained source/clock rows, provider metadata and all retirement-tombstone entries.

Renewal fixtures change only current pending evidence or its pending retry; their replaced old-clock changes were proven to disappear during migration. `stale-sync-drop-evidence` explicitly changes carrier from a removed provider orphan to a real-migrated covered-route clock: only that route's current evidence changes, its clock is dropped after retry, and its source and provider clock remain. Four cases whose earlier state was removed by migration instead use actual separate-connection removal/restoration commits across both production freshness checks. No production synchronization code changed, no case was merged or removed, and no original expected count or decision was weakened. Full before/after mappings are in `.cortexkit/internal-docs/research/s7b-activation/native-sync-controls/case-map.md`.

The affected store check and the first original-test filter passed (one passed, 446 skipped), executing all ten cases. Each of three scratch field-guard controls then failed only its corresponding renewal-evidence, renewal-schedule or drop-evidence case; each run explicitly completed and passed the other nine cases. The evidence field is checked both in the source-record comparison and in the keyed registration/drop update. Leaving either check active would still catch the same perturbation, so the controls neutralize only evidence in those two locations; all other source fields remain checked. A fourth control made the independent SQL reference skip the install fixture's registration: only the install case failed the original row-equality assertion, while the other nine passed. Scratch-only failure collection preserved every original assertion while allowing later cases to run. All mutations and instrumentation were restored byte-for-byte, and the restored original test passed. Actual eight proof/log artifacts and both runners are retained under the same internal research directory and `.cortexkit/scripts/check-s7b-native-sync-assertions.py` / `.cortexkit/scripts/red-control-s7b-native-sync.py`. All jobs ended before root staging was removed. This completes this invariant's port. The later fourteen-kind parity entry below records its completion. The generated-registration and source-store tests named above remain pending, along with comparison of legacy SQL evaluation and new block/loader evaluation on a copied live store at a fixed timestamp, the three audit scripts named above and final gates.

### Captured native parity across all fourteen source kinds

Commit `e7028785c128959445e9adf3e95dc20b55973096` keeps both original assertions of `memory_matches_the_view_and_the_adapter_on_the_parity_fixture` byte-identical. The complete legacy coverage checks and boundary times run on the schema-095 dataset before migration 096 introduces the `block` table, then the real migration supplies the new stored rows. All eight table datasets are inserted through one writer command into an initially empty destination, preserving row IDs and persisted columns. Writer hooks must record and read back those new records before the original memory/fresh/SQL-source/adapter comparisons; a populated destination loader or no-op write is not used to satisfy the read-back assertion. A direct SQL check also requires all fourteen source kinds.

Python 3.13.5 verified the two original assertion macros, `times.len() > 3` and `block_read_backs() > 0`, against the pre-port revision `0c3bd709`, without normalization. The affected store check and first exact parity filter passed (one passed, 446 skipped). Actual proof, output and the fixture map are carried in `.cortexkit/internal-docs/research/s7b-activation/native-parity-capture/`; comparator `.cortexkit/scripts/check-s7b-native-parity-capture-assertions.py`. All jobs ended before the temporary root input/output directories were deleted. No production change, original expected-value edit, manual refresh, invariant retirement or extra mutation run accompanies this port. At this parity checkpoint, still pending were `sync_and_register_decided_on_the_state_equal_sql_over_generated_writes`, the legacy fence-recheck/route-state fixture ports listed in this inventory, fixed-time old-SQL versus new-block evaluation on a copied live store, `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py`, `.cortexkit/scripts/red-controls-expiry.sh`, the final slice nextest filter and `scripts/rust-test-gate.sh`.

### Native generated registration, meaningful Tick capture and bounded fixes

Source `9bcfd07d34c60880c836847473d1d93dcf375df2` ports the 44-choice source-write generator to explicit `block`, `probe_authorization`, `success_proof` and live-metadata fixtures, retaining every operation label and RNG call. Actual provider/route/catalogue/clear/decision producers remain. The old renewal inequality compared independent clock/source evidence; the combined row cannot retain that inequality. Under the approved mapping, test-only bookkeeping records the existing registered clock's evidence immediately before a fixture write and the actual source evidence the write leaves. A renewal counts only for a genuine changed managed generation whose following register-alone operation actually writes rows. Source removal invalidates its witness chain; installs are not automatically renewals. Four seeds × 100 steps, every original threshold and exact SQL/working-row comparison remain.

The first runtime filter exposed two failures, neither hidden by expectation changes. Generated seed 2 step 19 retired a provider before checking a later route's provider dependency against the same captured state. A real schema-095 → registered-096 import, natural wall-clock expiry and public Tick reproduced the resulting retry error; no freshness bypass participated. The first bounded setup had incorrectly used Tick's expiry argument as synchronization time, so its empty-state assertion failure is disclosed but not claimed as the desired red proof. Approved fix `305d8c4113dd50bee1cea905be0ee751d9f6613e` preflights all existing source/clock guards before the same tombstone writes under the same writer transaction, retaining the retiring list, decisions, timestamps and subsequent fresh-state phase. A new negative test covers source and clock staleness at all three positions, including the last, with the existing retry error, unchanged full SQL/memory and zero tombstone attempts counted outside rollback. Reverting only preflight reddens both the real Tick regression and the early-write counter; restoring it passes them and the original ten-case exact-two-load invariant.

The writer-boundary test's separate failure was the first Tick changing no block row: native source removal already drops the cleared credential's orphan clock. Approved fixture `99582ea27747ca21f6c684ada767f30e11f57d40` uses real public catalogue/route/provider writers to leave a route clock whose source has the same cause and deadlines as a provider-wide cooldown. Sync omits that covered route origin from its live set, but the stored route clock still needs to be dropped. The read-back baseline is sampled after setup, so only Tick's actual drop can satisfy the original step. New helper assertions preserve the route source and provider clock. All six original assertion macros, 21 named steps, four allowed-silent labels and the Tick label remain unchanged. Disabling only that covered-route drop fails the original silent-step assertion with the Tick's read-back delta zero; no dummy write or silent-label relaxation substitutes for capture.

After preflight, the unchanged generated sequence reached a later authorized-heal mismatch differing only by six milliseconds in a retained credential's `cleared_at`. The red-first captured-time regression failed exactly on that audit tuple. Approved source `63ca22e3e2a6a4928f198c59d4101b25102e0ee5` adds a crate-private keyed clear taking the report's existing captured `now`; typed healing uses it. The public `clear_held_entitlement_fence` wrapper keeps its signature and wall-clock behavior for other callers. No timestamp is omitted, normalized or overridden, and no other clock site is changed.

Python 3.13.5 verifies all 13 original assertion macros byte-for-byte against `f426003b`, plus all operation choices/labels/RNG calls and the four-seed/100-step domain. The final affected store compiler check passed. The first captured-time/generated filter passed **2/2** (448 skipped), including every unchanged threshold. The required renewal-skip control then failed only the generated property on `renewals > 0` (43 installs, zero renewals, ten expired), while the writer-boundary test passed. Both that mutation and the separate covered-route-drop mutation were restored byte-for-byte; the first restored two-invariant filter passed **2/2** (448 skipped). Actual proof/logs and the full mapping are carried in `.cortexkit/internal-docs/research/s7b-activation/native-generator/`; all three control runners and the strict comparator are under `.cortexkit/scripts/`. All jobs ended before temporary root input/output directories were deleted. This completes these two invariant ports, not the remaining legacy source fixtures, copied-live-store comparison, three original audit scripts or final gates. Other wall-clock reads inside account commands remain unchanged; the event-time fix above applies only to typed credential healing.

### Native six-seed route snapshots and observed quarantine producers

Source `1fcb6f9e933b5a3bb1521a197210f72c4c3c24a3` ports `the_route_state_snapshot_from_memory_equals_sql_over_generated_route_writes` to explicit native route/provider/failure sources and live telemetry. Six seeds × 120 steps, all seven operation choices/labels, every RNG call and all seven original assertion macros remain unchanged. Source scan order follows the actual metadata rowid; route deletion removes its native route/failure sources in the same command. The unchanged schedule-coverage predicate reads the source-present route block's raw cooldown instead of a frozen-null metadata column. Shared-capacity, billing and both status outcomes keep their original coverage operands.

The old quarantine enumerator reads frozen metadata scope/class columns. Migration 096 excludes quarantined provider/route origins and orphan clocks entirely; it does not create a quarantine audit block. Under the native fixture mapping, raw fixture preparation retains telemetry and independent failure data but does not count as a producer witness. Afterwards the actual public route/provider writer receives the quarantined signal, and `writer_entry_trace` must observe its real completed AccountTx entry. Only that executed input plus keyed native-source absence can set the original quarantine flag. A separate per-signal quarantine assertion prevents one bad input from being masked by the final OR coverage flag. Nothing removes a source after the producer, so fixture cleanup cannot hide an erroneous import.

Python 3.13.5 verifies the seven original assertion macros byte-for-byte against `d1df4c78`, plus all choices/labels/RNG calls and the six-seed/120-step domain. The affected store compiler check and first exact property filter passed (one passed, 449 skipped); nextest marked the 129-second test slow, but it completed normally. The required scratch control selects a real non-null provider signal from that baseline output: `p0`, observed `1800008400002`, cooldown `1800012000001`, permanent credential-scoped `billing_exhausted`, reported class `billing`. Allowing only that provider/observation-time pair through the real production quarantine guard creates a native source and fails only the route property's per-signal quarantine assertion; the production-writer boundary passes (two tests: one passed, one failed). The baseline contains exactly one matching quarantined producer witness. The modified production guard in `crates/striatum-store/src/lib.rs` is restored byte-for-byte, then the first restored filter passes **2/2** (448 skipped), including all 720 route steps. Actual five proof/log artifacts and the complete mapping are carried under `.cortexkit/internal-docs/research/s7b-activation/native-route-generator/`; strict comparator and control runner are under `.cortexkit/scripts/`. All jobs ended before root input/output directories were deleted. No production change, clock override, invariant retirement or original expected-value edit accompanies this port. Still pending are the legacy fence-recheck/source fixtures listed earlier in this inventory, fixed-time old-SQL versus new-block evaluation on a migrated copy of the live store, `.cortexkit/scripts/red-controls-audit4.py`, `.cortexkit/scripts/red-controls-audit.py`, `.cortexkit/scripts/red-controls-expiry.sh`, the final filtered store/module nextest run, test-enabled checks of touched crates and `scripts/rust-test-gate.sh`.
