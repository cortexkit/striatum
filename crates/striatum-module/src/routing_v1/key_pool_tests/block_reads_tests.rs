//! The block tables (fences, refusals, eligibility, exclusions, probe clocks
//! and authorizations) are served from the in-memory account state. These
//! tests trace every statement SQLite runs on the store's writer and pooled
//! readers while real handlers run, and check that none of the request
//! readers that now answer from the state reads SQL.
//!
//! One read is a declared exception (`group_reads::declared_exception`)
//! and still reads SQL, because its caller does not yet take the account
//! state: "pool key states", key-pool lease state joined with the probe
//! clocks, which the pool group owns. The route-state snapshot that
//! `route.select` scores against and `route.cooldown_status` answers from,
//! with the provider cooldown row, is built from the state's route and
//! provider rows.
//!
//! Reads inside the writer are forbidden too, except on the two pool paths
//! (pool acquisition, and a finished run that ends its lease), where the
//! pool group still syncs clocks and reads key states on SQL. The claim and
//! probe decisions (claims, decision-probe binding, report matching,
//! authorization transfer and the check that a probe authorization names a
//! reported credential) and the
//! healers (success healing, failed and neutral probe outcomes, credential
//! classification) decide on the command's working state, so their SQL must
//! not run on the writer either. The state's own load and read-backs are
//! counted apart.

use std::collections::BTreeSet;

use super::*;
use striatum_store::group_reads::{reads_tables, BLOCK_READ_TABLES};

async fn ok(route: &super::super::conformance_tests::Route, method: &str, params: Value) -> Value {
    let reply = route.call(method, &params).await;
    assert_eq!(reply.0, FrameType::Response, "{method}: {}", reply.1);
    reply.1["result"].clone()
}

fn switched() -> Vec<&'static str> {
    BLOCK_READ_TABLES.to_vec()
}

/// Each SQL reader the request paths used to call, run on a pooled reader:
/// every one must be seen by the trace, and their statements are the ones
/// the writer must no longer run either.
fn oracle_reads(store: &striatum_store::SqliteRouteStateStore) -> Vec<(&'static str, usize)> {
    let trace = store.trace_block_reads_for_test().unwrap();
    let mut seen = Vec::new();
    let mut record = |name: &'static str| seen.push((name, trace.take().reader.len()));
    store.account_eligibility().unwrap();
    record("account_eligibility");
    store.account_size_refusals(0).unwrap();
    record("account_size_refusals");
    store.account_route_fences(0).unwrap();
    record("account_route_fences");
    store.transient_account_route_fences(0).unwrap();
    record("transient_account_route_fences");
    store.account_route_fence_latest_reports().unwrap();
    record("account_route_fence_latest_reports");
    store.credential_entitlement_fences().unwrap();
    record("credential_entitlement_fences");
    store.fence_rechecks().unwrap();
    record("fence_rechecks");
    store
        .account_read(|tx| tx.credential_entitlement_fence(POOL, "x"))
        .unwrap();
    record("credential_entitlement_fence");
    store.account_read(|tx| tx.active_fences(POOL)).unwrap();
    record("active_fences");
    store
        .account_read(|tx| tx.active_pool_fences(POOL))
        .unwrap();
    record("active_pool_fences");
    store
        .account_read(|tx| tx.active_fence_providers())
        .unwrap();
    record("active_fence_providers");
    store
        .account_read(|tx| tx.credential_quota_classification(POOL, "x", false))
        .unwrap();
    record("credential_quota_classification");
    store
        .account_read(|tx| tx.admission_exclusions("p", "a"))
        .unwrap();
    record("admission_exclusions");
    store
        .account_read(|tx| tx.exclusion_count("p", "c"))
        .unwrap();
    record("exclusion_count");
    store
        .account_read(|tx| tx.probe_authorization_owners())
        .unwrap();
    record("probe_authorization_owners");
    store
        .account_read(|tx| tx.report_credential_refusals())
        .unwrap();
    record("report_credential_refusals");
    store
        .account_read(|tx| tx.report_route_fences(0))
        .unwrap();
    record("report_route_fences");
    seen
}

/// The statements the route-state snapshot and the provider cooldown lookup
/// run when they read SQL, on the writer (the connection the store's
/// `build_snapshot` and `get_provider_cooldown` use): their reads of the
/// route and provider rows. The snapshot's probe-clock reads, and the
/// quarantine read, are left out: the clock synchronization and pool
/// acquisition still run the same statements on the writer.
fn route_state_oracle() -> BTreeSet<String> {
    use striatum_core::RouteStateStore;
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let store = harness.store();
    let trace = store.trace_block_reads_for_test().unwrap();
    store
        .account_transaction(|tx| tx.sync_fence_rechecks())
        .unwrap();
    let shared: BTreeSet<String> = trace.take().writer.into_iter().collect();
    store.build_snapshot(1_000.0).unwrap();
    let snapshot = trace.take();
    assert!(
        snapshot.exempt.is_empty(),
        "the SQL snapshot is no declared exception: {snapshot:#?}"
    );
    store.get_provider_cooldown(POOL).unwrap();
    let provider = trace.take();
    let rows: BTreeSet<String> = snapshot
        .writer
        .into_iter()
        .chain(provider.writer)
        .filter(|sql| {
            reads_tables(sql, &["model_route_state", "provider_cooldown_state"])
                && !shared.contains(sql)
        })
        .collect();
    assert_eq!(
        rows.len(),
        2,
        "the trace sees the SQL snapshot's route rows and the provider lookup: {rows:#?}"
    );
    rows
}

/// The statements each claim, probe and healing decision ran on the writer
/// when it decided on SQL. Each runs once through a test seam, whose
/// transaction has no account state, on a fresh store holding one due
/// provider wall. The clock synchronization's own statements are left out:
/// it still reads SQL before every decision.
fn decision_oracle(now: i64) -> Vec<(&'static str, BTreeSet<String>)> {
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let store = harness.store();
    striatum_core::RouteStateStore::record_provider_cooldown(
        store.as_ref(),
        "oracle-provider",
        "oracle-provider/model",
        "oracle-provider/model",
        &striatum_core::route_state_store::RouteStateUpdate {
            observed_at_ms: Some(now - 2 * 3_600_000),
            cooldown_until: Some((now + 3_600_000) as f64),
            last_error: Some("provider capacity".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let trace = store.trace_block_reads_for_test().unwrap();
    store
        .account_transaction(|tx| tx.sync_fence_rechecks())
        .unwrap();
    let mut sync: BTreeSet<String> = trace.take().writer.into_iter().collect();
    // The effective-recheck read is left out too: pool acquisition, which
    // the pool group owns, still runs it on the writer.
    store
        .account_transaction(|tx| tx.fence_rechecks())
        .unwrap();
    sync.extend(trace.take().writer);
    let origins = store.fence_rechecks().unwrap();
    assert_eq!(origins.len(), 1, "{origins:?}");
    trace.take();
    let mut seen = Vec::new();
    let mut record = |name: &'static str| {
        let statements = trace
            .take()
            .writer
            .into_iter()
            .filter(|sql| !sync.contains(sql))
            .collect();
        seen.push((name, statements));
    };
    store
        .account_transaction(|tx| tx.grant_fence_rechecks(&origins, now, Some("first")))
        .unwrap();
    record("claim");
    store
        .account_transaction(|tx| tx.probe_report_matches("first", None, false))
        .unwrap();
    record("report matches probe");
    store
        .account_transaction(|tx| tx.probe_authorization_names_credential("first", "c", "model"))
        .unwrap();
    record("authorization names credential");
    store
        .account_transaction(|tx| tx.transfer_probe_authorization("first", "second"))
        .unwrap();
    record("authorization transfer");
    // The healers, each on a probe it holds: a failed probe (inconclusive,
    // so its clock is due again at once), a neutral release, then a
    // success, on SQL as before.
    store
        .account_transaction(|tx| tx.fail_authorized_probe("second", now, true))
        .unwrap();
    record("heal: failed probe");
    let regrant = |owner: &'static str| {
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, now, Some(owner)))
            .unwrap());
        trace.take();
    };
    regrant("third");
    store
        .account_transaction(|tx| {
            tx.release_inconclusive_probe("third", "oracle-provider", "model", now)
        })
        .unwrap();
    record("heal: inconclusive release");
    store
        .account_transaction(|tx| {
            tx.classify_credential_quota("oracle-provider", "c", true)
        })
        .unwrap();
    record("heal: credential classification");
    // A success on SQL heals the provider wall the serving login's origins
    // include. A newer wall then gives the authorized-probe success a block to
    // heal.
    store
        .account_transaction(|tx| {
            tx.heal_serving_fences("oracle-provider", "a", "c", "model", None, now, now + 1)
        })
        .unwrap();
    record("heal: serving fences");
    striatum_core::RouteStateStore::record_provider_cooldown(
        store.as_ref(),
        "oracle-provider",
        "oracle-provider/model",
        "oracle-provider/model",
        &striatum_core::route_state_store::RouteStateUpdate {
            observed_at_ms: Some(now - 3_600_000),
            cooldown_until: Some((now + 3_600_000) as f64),
            last_error: Some("provider capacity".into()),
            ..Default::default()
        },
    )
    .unwrap();
    trace.take();
    regrant("fourth");
    store
        .account_transaction(|tx| {
            tx.heal_authorized_probe("fourth", "oracle-provider", "model", now + 1)
        })
        .unwrap();
    record("heal: authorized probe");
    seen
}

#[tokio::test]
async fn request_paths_read_no_block_table_outside_the_writer() {
    let now = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(now);
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let owner = harness.bind(reserved("example-consumer"), None).await;
    let runner = harness.bind(reserved("zeta-runner"), None).await;
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let store = harness.store();
    crate::account_state::initialize(store).unwrap();

    // Negatives: the trace sees every SQL reader the request paths used to
    // call, once each at least.
    let oracle = oracle_reads(store);
    for (name, reads) in &oracle {
        assert!(*reads >= 1, "the trace missed {name}: {oracle:?}");
    }
    let trace = store.trace_block_reads_for_test().unwrap();
    // The instrument tells the writer from the readers.
    store
        .account_transaction(|tx| tx.active_fences(POOL))
        .unwrap();
    let seen = trace.take();
    assert_eq!((seen.reader.len(), seen.writer.len()), (0, 1), "{seen:#?}");
    // Collect the oracle statements' text, to look for them on the writer.
    // `fence_rechecks`' statements are left out: the store's own claim
    // statements still run them inside the writer.
    let oracle_text: BTreeSet<String> = {
        let trace = store.trace_block_reads_for_test().unwrap();
        oracle_reads_text(store, trace)
    };
    // Negatives for the decisions: the trace sees each one's SQL.
    let decisions = decision_oracle(now as i64);
    for (name, statements) in &decisions {
        assert!(
            !statements.is_empty(),
            "the trace missed {name}'s SQL: {decisions:?}"
        );
    }
    let decision_text: BTreeSet<String> = decisions
        .into_iter()
        .flat_map(|(_, statements)| statements)
        .collect();
    // Negatives for the route-state snapshot: the trace sees its SQL.
    let route_state_text = route_state_oracle();
    let trace = store.trace_block_reads_for_test().unwrap();
    let switched = switched();
    let mut writer_reads = Vec::new();
    let reloads = striatum_store::block_reconcile_reloads();
    let mut check = |op: &str| {
        // Every block write the op made ran inside an account command: the
        // capture saw it, so the state never needs a reload to catch up.
        assert_eq!(
            store.block_writes_outside_command(),
            0,
            "{op} wrote block tables outside an account command: {:?}",
            store.block_tables_written_outside_command()
        );
        let seen = trace.take();
        // The clock synchronization (`sync`/`register`) decides on the
        // command's working state, so no command reads the block tables on
        // the writer, except where pool lease state still syncs and reads
        // them on SQL ("pool key states").
        if !matches!(op, "pool_acquire" | "observe finish") {
            assert!(
                seen.writer.is_empty(),
                "{op} read block tables on the writer: {:#?}",
                seen.writer
            );
        }
        let ordinary: Vec<_> = seen
            .reader
            .iter()
            .filter(|sql| reads_tables(sql, &switched))
            .collect();
        assert!(
            ordinary.is_empty(),
            "{op} read block tables on a pooled reader: {ordinary:#?}"
        );
        let switched_on_writer: Vec<_> = seen
            .writer
            .iter()
            .filter(|sql| oracle_text.contains(sql.as_str()))
            .collect();
        assert!(
            switched_on_writer.is_empty(),
            "{op} ran a switched reader's SQL on the writer: {switched_on_writer:#?}"
        );
        let decided_on_sql: Vec<_> = seen
            .writer
            .iter()
            .filter(|sql| decision_text.contains(sql.as_str()))
            .collect();
        assert!(
            decided_on_sql.is_empty(),
            "{op} decided a claim, probe or heal on SQL: {decided_on_sql:#?}"
        );
        let route_state_on_sql: Vec<_> = seen
            .writer
            .iter()
            .chain(&seen.reader)
            .chain(seen.exempt.iter().map(|(_, sql)| sql))
            .filter(|sql| route_state_text.contains(sql.as_str()))
            .collect();
        assert!(
            route_state_on_sql.is_empty(),
            "{op} read the route-state snapshot or a provider row from SQL: {route_state_on_sql:#?}"
        );
        let snapshot_exceptions: Vec<_> = seen
            .exempt
            .iter()
            .filter(|(reason, _)| *reason != "pool key states")
            .collect();
        assert!(
            snapshot_exceptions.is_empty(),
            "{op} made a declared exception other than the pool's: {snapshot_exceptions:#?}"
        );
        writer_reads.push((op.to_owned(), seen.writer, seen.exempt, seen.maintenance));
    };

    let selected = ok(&owner, "route.select", json!({"consumer":"mason","requirements":{"iq":30},"pin":format!("{POOL}/{POOL_MODEL_SHORT}"),"affinity":"block-reads"})).await;
    check("select a pool route");
    let admission = json!({"provider":POOL,"model":POOL_MODEL_SHORT,"decision_id":selected["decision_id"],"conversation_key":"block-reads","admission_id":"block-reads"});
    let admitted = ok(&runner, "route.account_for", admission.clone()).await;
    check("account_for");
    ok(&runner, "route.account_for", admission).await;
    check("account_for replay");
    ok(&runner, "route.account_observe", json!({"kind":"admission_started","event_id":"block-start","admission_id":"block-reads","run_id":"block-run"})).await;
    check("observe start");
    let lease = ok(
        &operator,
        "route.pool_acquire",
        json!({"provider":POOL,"model":POOL_MODEL_SHORT,"session":"block-session"}),
    )
    .await;
    check("pool_acquire");
    ok(&operator, "route.pool_release", json!({"provider":POOL,"session":"block-session","leaseId":lease["leaseId"],"endSession":true})).await;
    check("pool_release");
    ok(&runner, "route.account_for", json!({"mode":"continue","run_id":"block-run","admission_id":"block-reads","conversation_key":"block-reads","previous_decision_id":selected["decision_id"],"selection_id":"block-continuation",
        "exhausted":{"credential_id":admitted["account"]["credential_id"],"account":admitted["account"]["account_id"],"limit_window":"five_hour","observed_at_ms":now,"resets_at_ms":now+300_000}})).await;
    check("continue after exhaustion");
    ok(&runner, "route.account_observe", json!({"kind":"run_finished","event_id":"block-finish","admission_id":"block-reads","run_id":"block-run"})).await;
    check("observe finish");
    ok(
        &owner,
        "route.report",
        json!({"decision_id":selected["decision_id"],"outcome":"completed"}),
    )
    .await;
    check("report completed");

    let plain = ok(
        &owner,
        "route.select",
        json!({"consumer":"mason","requirements":{"iq":30},"pin":PLAIN}),
    )
    .await;
    check("select a plain route");
    let plain_admission = ok(&runner, "route.account_for", json!({"provider":BORROWED,"model":"model-a","decision_id":plain["decision_id"],"conversation_key":"block-plain","admission_id":"block-plain"})).await;
    check("account_for plain");
    let plain_key = plain_admission["account"]["credential_id"].clone();
    // A credential refusal reported for a second plain decision: the report
    // reconciles quota holds inside its command.
    let refused = ok(
        &owner,
        "route.select",
        json!({"consumer":"mason","requirements":{"iq":30},"pin":PLAIN}),
    )
    .await;
    check("select a plain route to refuse");
    ok(&owner, "route.report", json!({"decision_id":refused["decision_id"],"outcome":"refused","credential_id":plain_key,"runner_code":"billing_exhausted"})).await;
    check("report credential refusal");
    ok(&runner, "route.account_observe", json!({"event_id":"block-ineligible","kind":"scoped_not_found","admission_id":"block-plain","credential_id":plain_key,"view_digest":"key-pool-listing"})).await;
    check("observe scoped not-found");
    ok(&runner, "route.account_observe", json!({"event_id":"block-credit","kind":"account_limit_exhausted","admission_id":"block-plain","limit_window":"credit"})).await;
    check("observe account limit");
    ok(&runner, "route.account_observe", json!({"event_id":"block-credential","kind":"credential_refused","admission_id":"block-plain","provider_code":"ENTITLEMENT_ERROR"})).await;
    check("observe credential refused");
    ok(&owner, "route.report", json!({"decision_id":plain["decision_id"],"outcome":"refused","credential_id":plain_key,"runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":now+60_000})).await;
    check("report size refusal");
    ok(
        &owner,
        "route.cooldown_status",
        json!({"routeKey":format!("{POOL}/{POOL_MODEL_SHORT}"),"capacityKey":format!("{POOL}/{POOL_MODEL_SHORT}"),"now":now}),
    )
    .await;
    check("cooldown status");
    // Every reader now has rows to return.
    assert!(!store.account_eligibility().unwrap().is_empty());
    assert!(!store.credential_entitlement_fences().unwrap().is_empty());
    trace.take();

    // Panel selection: the class context and its probe reservations.
    let credentials = harness.credentials();
    let mut context = crate::account_selection::ClassContext::load(
        store,
        &credentials,
        harness.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        now as i64,
    )
    .unwrap();
    check("class context load");
    context
        .reserve_selection_probes(store, &credentials)
        .unwrap();
    check("selection probe reservation");
    // These may be refused now that the pool is fenced; the reads matter.
    owner
        .call("route.select", &json!({"consumer":"mason","requirements":{"iq":30},"pin":format!("{POOL}/{POOL_MODEL_SHORT}"),"affinity":"block-reads-2"}))
        .await;
    check("select a fenced pool route");
    operator
        .call(
            "route.pool_acquire",
            &json!({"provider":POOL,"model":POOL_MODEL_SHORT,"session":"block-session-2"}),
        )
        .await;
    check("pool_acquire with fences");

    // A tick reconciles the state with SQL; after the whole mix it finds
    // nothing the capture missed.
    crate::account_writer::tick(store, now as i64).unwrap();
    assert_eq!(
        striatum_store::block_reconcile_reloads(),
        reloads,
        "the tick's reconcile reloaded a state the request mix left unequal to SQL"
    );
    assert_eq!(store.block_writes_outside_command(), 0);
    for (op, reads, exempt, maintenance) in &writer_reads {
        eprintln!(
            "block reads: {op}: {} ordinary on the writer, {} declared exceptions, {maintenance} state maintenance",
            reads.len(),
            exempt.len(),
        );
        for sql in reads.iter().collect::<BTreeSet<_>>() {
            eprintln!(
                "    writer: {}",
                sql.split_whitespace().collect::<Vec<_>>().join(" ")
            );
        }
        for (reason, sql) in exempt.iter().collect::<BTreeSet<_>>() {
            eprintln!(
                "    {reason}: {}",
                sql.split_whitespace().collect::<Vec<_>>().join(" ")
            );
        }
    }
}

fn oracle_reads_text(
    store: &striatum_store::SqliteRouteStateStore,
    trace: &'static striatum_store::group_reads::BlockReadTrace,
) -> BTreeSet<String> {
    trace.take();
    let _ = store.account_eligibility();
    let _ = store.account_size_refusals(0);
    let _ = store.account_route_fences(0);
    let _ = store.transient_account_route_fences(0);
    let _ = store.account_route_fence_latest_reports();
    let _ = store.credential_entitlement_fences();
    let _ = store.account_read(|tx| tx.credential_entitlement_fence(POOL, "x"));
    let _ = store.account_read(|tx| tx.active_fences(POOL));
    let _ = store.account_read(|tx| tx.active_pool_fences(POOL));
    let _ = store.account_read(|tx| tx.active_fence_providers());
    let _ = store.account_read(|tx| tx.credential_quota_classification(POOL, "x", false));
    let _ = store.account_read(|tx| tx.admission_exclusions("p", "a"));
    let _ = store.account_read(|tx| tx.exclusion_count("p", "c"));
    let _ = store.account_read(|tx| tx.probe_authorization_owners());
    let _ = store.account_read(|tx| tx.report_credential_refusals());
    let _ = store.account_read(|tx| tx.report_route_fences(0));
    trace.take().reader.into_iter().collect()
}
