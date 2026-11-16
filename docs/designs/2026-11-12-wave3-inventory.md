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

The consumer-name migration from main occupies version 094; native block storage is version 095. The registered chain, migration include, version assertions and active control scripts use the new block migration path. Historical control logs retain their actual earlier filenames.

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
