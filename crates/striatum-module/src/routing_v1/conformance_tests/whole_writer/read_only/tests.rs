//! Reviewed non-writer entries remain observed: a durable account-row write in
//! any of these scopes is a coverage failure, not an exemption from tracing.

pub(super) const READ_ONLY: &[(&str, &str)] = &[
    (
        "module_adapter::pool_key_states",
        "reads key state from the published block snapshot",
    ),
    (
        "account_tx::account_selection::size_admission_for_model_decision",
        "selects a decision's admitted size and commit time",
    ),
    (
        "account_tx::account_selection::admission_reported_failure",
        "tests for a persisted refusal observation",
    ),
    (
        "account_tx::account_selection::parent_used_account",
        "selects the latest live account and its matching affinity",
    ),
    (
        "account_tx::account_selection::pin_has_started",
        "tests whether a matching pin has a started admission",
    ),
    (
        "account_tx::account_selection::admission",
        "selects one admission by principal and id",
    ),
    (
        "account_tx::account_selection::admission_size_class",
        "selects one admission's input size class",
    ),
    (
        "account_tx::account_selection::decision_context",
        "selects a decision's recorded allocation context",
    ),
    (
        "account_tx::account_selection::observation_by_event",
        "looks up the observation log or its retained event record",
    ),
    (
        "account_tx::account_selection::observation_by_principal_event",
        "finds an event's runner and delegates to the observation lookup",
    ),
    (
        "account_tx::account_selection::observation_backoffs",
        "collects bound historical observation backoffs",
    ),
    (
        "account_tx::account_selection::demand",
        "collects charged admissions within the requested reading floor",
    ),
    (
        "account_tx::account_selection::affinity",
        "selects one conversation's affinity",
    ),
    (
        "account_tx::account_selection::existing_salt",
        "reads the singleton salt without creating it",
    ),
    (
        "account_tx::account_selection::route_fence_latest_reports",
        "projects report-ordering timestamps from captured blocks",
    ),
    (
        "account_tx::account_selection::credential_entitlement_fence",
        "projects a credential refusal from captured blocks",
    ),
    (
        "account_tx::account_selection::active_fences",
        "projects standing account holds from captured blocks",
    ),
    (
        "account_tx::account_selection::active_pool_fences",
        "projects standing quota-pool holds from captured blocks",
    ),
    (
        "account_tx::account_selection::active_fence_providers",
        "projects providers with standing account holds",
    ),
    (
        "account_tx::account_selection::admission_exclusions",
        "projects the named admission's principal exclusions",
    ),
    (
        "account_tx::account_selection::exclusion_count",
        "counts the named credential's principal exclusions",
    ),
    (
        "account_tx::account_selection::state_rows::load_admission_group",
        "loads admission, demand, affinity and continuation state",
    ),
    (
        "account_tx::account_selection::state_rows::admission_success_by_key",
        "loads a possibly closed admission for success evidence",
    ),
    (
        "account_tx::account_selection::state_rows::admissions_for_run",
        "selects a decision and run's admissions",
    ),
    (
        "account_tx::account_selection::state_rows::admission_by_key",
        "performs the keyed admission replay lookup",
    ),
    (
        "account_tx::account_selection::state_rows::fork_parent_by_key",
        "selects a parent admission and whether it has a consumed continuation",
    ),
    (
        "account_tx::account_selection::state_rows::fork_used_binding_by_key",
        "selects a parent's latest consumed reservation admission",
    ),
    (
        "account_tx::account_selection::state_rows::continuation_by_key",
        "performs the keyed continuation replay lookup",
    ),
    (
        "account_tx::account_selection::state_rows::latest_admission_for_conversation",
        "selects a conversation's surviving newest admission after retention",
    ),
    (
        "account_tx::account_selection::state_rows::latest_used_continuation_row",
        "selects the newest consumed continuation",
    ),
    (
        "account_tx::account_selection::state_rows::last_insert_rowid",
        "reads SQLite's connection-local insertion identity",
    ),
    (
        "account_tx::block_state::reconcile_block_state",
        "compares row fingerprints and periodic full loads without writing",
    ),
    (
        "account_tx::block_state::check_decision_inputs",
        "checks captured provider sources against keyed SQL rows",
    ),
    (
        "account_tx::block_state::check_admission_exclusions",
        "checks captured principal exclusions against keyed SQL rows",
    ),
    (
        "account_tx::block_state::load_block_state",
        "loads physical block and telemetry rows",
    ),
    (
        "account_tx::block_state::pending_block_rows",
        "reads captured rowids back from the writer transaction",
    ),
    (
        "account_tx::block_state::working_blocks",
        "overlays captured SQL rows onto a private in-memory snapshot",
    ),
    (
        "account_tx::report_delivery::report_credential_refusals",
        "projects credential refusals from captured blocks",
    ),
    (
        "account_tx::report_delivery::report_route_fences",
        "projects quota route fences from captured blocks",
    ),
];

pub(super) const ADDITIONAL_READ_ONLY: &[(&str, &str)] = &[
    (
        "account_tx::account_selection::continuation::continuation",
        "selects a continuation's original request and answer",
    ),
    (
        "account_tx::account_selection::continuation::continuation_has_used_successor",
        "tests for a consumed successor of a decision",
    ),
    (
        "account_tx::account_selection::continuation::continued_reservation",
        "selects a live continuation's reservation identity",
    ),
    (
        "account_tx::account_selection::continuation::has_expired_continuations",
        "tests for unconsumed offers past their deadline",
    ),
    (
        "account_tx::account_selection::continuation::latest_used_continuation",
        "selects the newest consumed offer and reads its admission",
    ),
    (
        "account_tx::account_selection::continuation::signed_out_credentials",
        "collects credentials named by bound signed-out observations",
    ),
    (
        "account_tx::fence_rechecks::credential_quota_classification",
        "selects an already classified credential's quota flag",
    ),
    (
        "account_tx::fence_rechecks::has_expired_admission_probes",
        "tests whether an admission's authorized probe has expired",
    ),
    (
        "account_tx::fence_rechecks::probe_authorizations::probe_authorization_owners",
        "selects distinct authorization owners",
    ),
    (
        "account_tx::fence_rechecks::probe_authorizations::probe_authorizations_for_origins",
        "selects exact origin evidence and current generation proofs without changing them",
    ),
    (
        "account_command::inflight_count",
        "expires and counts only the process-local inflight ledger",
    ),
    (
        "account_command::inflight_count_for_model",
        "expires and counts only the model's process-local inflight ledger",
    ),
];

pub(super) fn entries() -> impl Iterator<Item = &'static (&'static str, &'static str)> {
    READ_ONLY.iter().chain(ADDITIONAL_READ_ONLY)
}

#[tokio::test]
async fn reviewed_fork_selectors_remain_non_writing_on_real_rows() {
    use super::*;

    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("whole-writer-fork-reader-scope").await;
    world.continuation_group(4_019).await;
    // Take identities from the real issued and consumed offer, not empty keys
    // that could make a conditional writer appear harmless.
    let (principal, parent, reservation, account): (String, String, String, String) = world
        .harness
        .store()
        .with_reader_for_test(|conn| {
            conn.query_row(
                "SELECT c.principal,c.admission_id,c.reservation_id,r.account_id
                FROM account_continue c JOIN account_admission r
                ON r.principal=c.principal AND r.admission_id=c.reservation_id
                WHERE c.used_at_ms IS NOT NULL ORDER BY c.rowid DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
        })
        .unwrap();
    assert_ne!(
        parent, reservation,
        "the used offer really moved to another admission"
    );
    let trace = world.trace.clone();
    // Run on the writer, where writes are permitted, so a write attempt is
    // detected by the trace rather than prevented by a read-only connection.
    let entries = world.command("fork/read parent and consumed reservation", move |tx| {
        let trace_start = trace.entries().len();
        let (row, moved) = striatum_store::AccountTx::fork_parent_by_key(tx, &principal, &parent)?
            .expect("the real parent is persisted");
        assert_eq!(row.admission_id, parent);
        assert!(moved, "the real parent has a consumed successor");
        let used = striatum_store::AccountTx::fork_used_binding_by_key(tx, &principal, &parent)?
            .expect("the real used reservation is persisted");
        assert_eq!(used.admission_id, reservation);
        assert_eq!(used.account_id, account);
        assert!(
            striatum_store::AccountTx::fork_parent_by_key(tx, &principal, "absent-parent")?
                .is_none()
        );
        assert!(striatum_store::AccountTx::fork_used_binding_by_key(
            tx,
            &principal,
            "absent-parent"
        )?
        .is_none());
        Ok(trace.entries()[trace_start..].to_vec())
    });
    assert_trace_is_declared(&world.trace);
    for name in [
        "account_tx::account_selection::state_rows::fork_parent_by_key",
        "account_tx::account_selection::state_rows::fork_used_binding_by_key",
    ] {
        let reads = entries
            .iter()
            .filter(|entry| entry.name == name)
            .collect::<Vec<_>>();
        assert_eq!(
            reads.len(),
            2,
            "both present and absent lookups actually entered {name}"
        );
        assert!(
            reads
                .iter()
                .all(|entry| entry.account_rows_written == Some(0)),
            "the real fork selector wrote durable account rows: {name}: {reads:?}"
        );
    }
}
