//! A backup may restore some routing tables empty while it keeps every other
//! table. These tests class every table, check the foreign-key rule a backup
//! tool enforces, and start routing on a store whose skippable history tables
//! were restored empty while the tables it keeps are full.

use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;
use striatum_core::{
    DecisionInput, DecisionOutcomeUpdate, FailureBackoffConfig, RouteOutcome, RouteStateStore,
};
use striatum_store::backup::{
    BACKUP_EXCLUDABLE_TABLES, BACKUP_HISTORY_EXCLUDABLE_TABLES, BACKUP_HISTORY_REQUIRED_TABLES,
    BACKUP_REQUIRED_TABLES,
};
use striatum_store::{
    dev_descriptor, AccountAdmissionRow, AccountDecisionRecord, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, SqliteRouteStateStore,
    DECISION_MODE_SERVE,
};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

const HOUR: i64 = 60 * 60_000;
/// The test clock, 2026-10-12T00:00:00Z.
const NOW: i64 = 1_791_763_200_000;
const PRINCIPAL: &str = "reserved:broca";
const ROUTE: &str = "openai/gpt-5.5";
const CONTENT_POLICY: &str = "invalid_prompt: flagged as potentially violating our usage policy";

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-backup-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn db_path(&self) -> std::path::PathBuf {
        self.dir.join("store.db")
    }

    fn open(&self) -> SqliteRouteStateStore {
        let path = self.db_path().to_string_lossy().into_owned();
        SqliteRouteStateStore::open(&dev_descriptor("striatum", path))
            .expect("open route-state store")
    }

    fn connection(&self) -> Connection {
        Connection::open(self.db_path()).unwrap()
    }

    /// Empty `tables` the way a restore that skipped them does: their rows
    /// are gone and no trigger ran. Triggers on a table are saved, dropped
    /// for the delete and created again, in one transaction.
    fn restore_empty(&self, tables: &[&str]) {
        let mut connection = self.connection();
        connection
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        let tx = connection.transaction().unwrap();
        for table in tables {
            let triggers: Vec<(String, String)> = {
                let mut statement = tx
                    .prepare(
                        "SELECT name, sql FROM sqlite_schema \
                         WHERE type = 'trigger' AND tbl_name = ?1",
                    )
                    .unwrap();
                let rows = statement
                    .query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap();
                rows.collect::<rusqlite::Result<_>>().unwrap()
            };
            for (name, _) in &triggers {
                tx.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
            }
            tx.execute(&format!("DELETE FROM {table}"), []).unwrap();
            for (_, sql) in &triggers {
                tx.execute_batch(sql).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    fn count(&self, table: &str) -> i64 {
        self.connection()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    /// Rows `PRAGMA foreign_key_check` reports, as "table -> parent".
    fn foreign_key_violations(&self) -> Vec<String> {
        let connection = self.connection();
        let mut statement = connection.prepare("PRAGMA foreign_key_check").unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok(format!(
                    "{} -> {}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(2)?
                ))
            })
            .unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn excludable() -> Vec<&'static str> {
    BACKUP_EXCLUDABLE_TABLES
        .iter()
        .map(|(name, _)| *name)
        .collect()
}

fn user_tables(connection: &Connection) -> Vec<String> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' \
             AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .unwrap();
    let rows = statement.query_map([], |row| row.get(0)).unwrap();
    rows.collect::<rusqlite::Result<_>>().unwrap()
}

#[test]
fn every_routing_table_is_classed_exactly_once_with_a_reason() {
    let rig = Rig::new("classes");
    drop(rig.open());
    check_classes(
        &rig.connection(),
        BACKUP_EXCLUDABLE_TABLES,
        BACKUP_REQUIRED_TABLES,
    );
    check_classes(
        &Connection::open(rig.dir.join("history.db")).unwrap(),
        BACKUP_HISTORY_EXCLUDABLE_TABLES,
        BACKUP_HISTORY_REQUIRED_TABLES,
    );
}

fn check_classes(connection: &Connection, excluded: &[(&str, &str)], required: &[(&str, &str)]) {
    let mut classed: Vec<String> = excluded
        .iter()
        .chain(required)
        .map(|(name, reason)| {
            assert!(!reason.trim().is_empty(), "{name} has no reason");
            name.to_string()
        })
        .collect();
    classed.sort();
    let mut unique = classed.clone();
    unique.dedup();
    assert_eq!(unique, classed, "a table is classed twice");
    assert_eq!(user_tables(connection), classed);
}

/// A backup tool refuses to skip a table that a table it keeps references by
/// foreign key: restored empty, every such reference would dangle.
#[test]
fn no_backed_up_table_holds_a_foreign_key_into_a_skippable_one() {
    let rig = Rig::new("foreign-keys");
    drop(rig.open());
    check_backup_foreign_keys(
        &rig.connection(),
        BACKUP_EXCLUDABLE_TABLES,
        BACKUP_REQUIRED_TABLES,
    );
    check_backup_foreign_keys(
        &Connection::open(rig.dir.join("history.db")).unwrap(),
        BACKUP_HISTORY_EXCLUDABLE_TABLES,
        BACKUP_HISTORY_REQUIRED_TABLES,
    );
}

fn check_backup_foreign_keys(
    connection: &Connection,
    excluded: &[(&str, &str)],
    required: &[(&str, &str)],
) {
    let skippable: Vec<_> = excluded.iter().map(|(name, _)| *name).collect();
    let mut offending = Vec::new();
    for (table, _) in required {
        let mut statement = connection
            .prepare("SELECT \"table\" FROM pragma_foreign_key_list(?1)")
            .unwrap();
        let parents: Vec<String> = statement
            .query_map([table], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for parent in parents {
            if skippable.contains(&parent.as_str()) {
                offending.push(format!("{table} -> {parent}"));
            }
        }
    }
    assert!(offending.is_empty(), "{offending:?}");
}

fn account_decision(id: &str) -> AccountDecisionRecord {
    AccountDecisionRecord {
        decision_id: id.into(),
        mode: DECISION_MODE_SERVE.into(),
        trigger: "request".into(),
        principal: PRINCIPAL.into(),
        admission_id: None,
        request_mode: Some("admit".into()),
        provider: "anthropic".into(),
        model: "claude-opus-5-5".into(),
        family: "opus".into(),
        outcome: "admit".into(),
        code: "cold_scored".into(),
        account_id: Some("acct-a".into()),
        credential_id: Some("oauth:anthropic".into()),
        policy_version: 2,
        decision_json: "{}".into(),
        context_json: None,
        created_at: NOW - HOUR,
    }
}

fn pin(pin_id: &str, generation: u64, decision_id: &str) -> AccountPinRow {
    AccountPinRow {
        pin_id: pin_id.into(),
        principal: PRINCIPAL.into(),
        conversation_hash: "conversation".into(),
        provider: "anthropic".into(),
        pin_generation: generation,
        affinity_model: "claude-opus-5-5".into(),
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        pin_mode: "run_frozen".into(),
        previous_pin_id: None,
        decision_id: decision_id.into(),
        created_at: NOW - HOUR,
    }
}

fn admission(admission_id: &str, decision_id: &str, pin: &AccountPinRow) -> AccountAdmissionRow {
    AccountAdmissionRow {
        principal: PRINCIPAL.into(),
        admission_id: admission_id.into(),
        request_fingerprint: "fp".into(),
        request_mode: "admit".into(),
        decision_id: decision_id.into(),
        conversation_hash: pin.conversation_hash.clone(),
        provider: "anthropic".into(),
        model: "claude-opus-5-5".into(),
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        pin_id: pin.pin_id.clone(),
        pin_generation: pin.pin_generation,
        state: "reserved".into(),
        charged: true,
        request_bytes: 12_000,
        request_bytes_estimated: false,
        committed_at: NOW - HOUR,
        admission_valid_until: NOW + HOUR,
        lease_valid_until: NOW + HOUR,
        reply_json: "{\"admission\":\"committed\"}".into(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    }
}

fn observation(id: &str, event_id: &str) -> AccountObservationRow {
    AccountObservationRow {
        observation_id: id.into(),
        principal: PRINCIPAL.into(),
        runner: "broca".into(),
        event_id: event_id.into(),
        kind: "attempt".into(),
        admission_id: Some("adm-1".into()),
        bound: false,
        observation_json: "{\"headers\":{}}".into(),
        effects_json: "[]".into(),
        received_at: NOW - HOUR,
    }
}

const FENCE_EFFECTS: &str = "[{\"effect\":\"fence\"}]";

/// Account-selection history (decisions, pins, an admission, a hold, an
/// observation with the fence and exclusion it caused).
fn populate_account_history(store: &SqliteRouteStateStore) -> AccountPinRow {
    let first_pin = pin("pin-1", 1, "dec-1");
    store
        .account_transaction(|tx| {
            tx.insert_decision(&account_decision("dec-1"))?;
            tx.insert_decision(&account_decision("dec-hold"))?;
            tx.insert_pin(&first_pin)?;
            tx.insert_admission(&admission("adm-1", "dec-1", &first_pin))?;
            tx.insert_hold(&AccountHoldRow {
                decision_id: "dec-hold".into(),
                principal: PRINCIPAL.into(),
                conversation_hash: "conversation".into(),
                provider: "anthropic".into(),
                pin_id: "pin-1".into(),
                pin_generation: 1,
                account_id: "acct-a".into(),
                credential_id: "oauth:anthropic".into(),
                basis: "five_hour".into(),
                reset_at: NOW + HOUR,
                retry_after_s: 60,
                created_at: NOW - HOUR,
            })?;
            tx.insert_observation(&observation("aco-1", "event-1"))?;
            tx.set_observation_effects("aco-1", true, FENCE_EFFECTS)?;
            tx.insert_fence(&AccountFenceRow {
                fence_id: "fence-1".into(),
                provider: "anthropic".into(),
                account_id: "acct-a".into(),
                scope_json: "{}".into(),
                fence_json: "{}".into(),
                source: "observation".into(),
                observed_at: NOW - HOUR,
                principal: PRINCIPAL.into(),
                admission_id: Some("adm-1".into()),
                observation_id: Some("aco-1".into()),
                created_at: NOW - HOUR,
                cleared_at: None,
                cleared_reason: None,
            })?;
            tx.insert_exclusion(&AccountExclusionRow {
                principal: PRINCIPAL.into(),
                admission_id: "adm-1".into(),
                credential_id: "oauth:anthropic".into(),
                runner_view_digest: "runner-view".into(),
                roster_view_digest: "roster-view".into(),
                observation_id: Some("aco-1".into()),
                recorded_at: NOW - HOUR,
            })
            .map(|_| ())
        })
        .unwrap();
    first_pin
}

/// With every skippable table restored empty, the kept tables pass the
/// foreign-key check, and account serving keeps writing: a new pin
/// generation and admission on the same conversation, and every retention
/// batch, succeed with foreign keys enforced.
#[test]
fn a_store_restored_without_its_history_passes_the_foreign_key_check_and_keeps_serving() {
    let rig = Rig::new("restored-history");
    let first_pin = populate_account_history(&rig.open());
    rig.restore_empty(&excludable());
    assert_eq!(rig.count("account_decision"), 0);
    assert_eq!(rig.count("account_observation"), 0);
    assert_eq!(rig.count("account_admission"), 1);
    assert_eq!(rig.connection().query_row(
        "SELECT count(*) FROM block WHERE kind='account' AND is_origin=1 AND json_type(source_metadata_json,'$.fence_json')='text'",
        [],|row|row.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(rig.foreign_key_violations(), Vec::<String>::new());

    let store = rig.open();
    let replayed = store
        .account_transaction(|tx| tx.admission(PRINCIPAL, "adm-1"))
        .unwrap()
        .expect("the admission survives the restore");
    assert_eq!(replayed.reply_json, "{\"admission\":\"committed\"}");
    let next_pin = AccountPinRow {
        previous_pin_id: Some(first_pin.pin_id.clone()),
        ..pin("pin-2", 2, "dec-2")
    };
    store
        .account_transaction(|tx| {
            tx.insert_decision(&account_decision("dec-2"))?;
            tx.insert_pin(&next_pin)?;
            tx.insert_admission(&admission("adm-2", "dec-2", &next_pin))
        })
        .unwrap();
    let later = NOW + 400 * 24 * HOUR;
    store.prune_account_admissions(later, 256, later).unwrap();
    store.prune_account_holds(later, 256).unwrap();
    store.prune_account_affinities(later, 256).unwrap();
    store.prune_serve_decisions(later, 256).unwrap();
    store.prune_account_observations(later, 256).unwrap();
    store.prune_observation_events(later, 256).unwrap();
    store.delete_decisions_older_than(later, 256).unwrap();
    store.prune_parent_failures(later, 256).unwrap();
    drop(store);
    assert_eq!(rig.foreign_key_violations(), Vec::<String>::new());
}

/// A runner re-sending an event after `account_observation` was restored
/// empty is still recognised from `account_observation_event`, with the
/// observation id, binding and effects the first report got.
#[test]
fn a_re_sent_event_is_recognised_after_the_observation_log_is_restored_empty() {
    let rig = Rig::new("observation-event");
    populate_account_history(&rig.open());
    rig.restore_empty(&["account_observation"]);
    assert_eq!(rig.count("account_observation_event"), 1);

    let store = rig.open();
    let seen = store
        .account_transaction(|tx| tx.observation_by_event(PRINCIPAL, "broca", "event-1"))
        .unwrap()
        .expect("the event key outlives the observation log");
    assert_eq!(seen.observation_id, "aco-1");
    assert!(seen.bound);
    assert_eq!(seen.effects_json, FENCE_EFFECTS);
    assert_eq!(seen.received_at, NOW - HOUR);
    let unseen = store
        .account_transaction(|tx| tx.observation_by_event(PRINCIPAL, "broca", "event-2"))
        .unwrap();
    assert!(unseen.is_none());
}

/// Event keys are pruned on their own window, oldest first and bounded per
/// batch, whether or not the observation row is still there.
#[test]
fn event_key_retention_deletes_only_old_keys_and_is_bounded() {
    let rig = Rig::new("observation-event-retention");
    let store = rig.open();
    store
        .account_transaction(|tx| {
            for index in 0..3 {
                tx.insert_observation(&AccountObservationRow {
                    received_at: NOW - 10 * HOUR - index,
                    ..observation(&format!("old-{index}"), &format!("old-{index}"))
                })?;
            }
            tx.insert_observation(&AccountObservationRow {
                received_at: NOW,
                ..observation("young", "young")
            })
        })
        .unwrap();
    let before = NOW - 6 * HOUR;
    assert_eq!(store.prune_observation_events(before, 2).unwrap(), 2);
    assert_eq!(store.prune_observation_events(before, 2).unwrap(), 1);
    assert_eq!(store.prune_observation_events(before, 2).unwrap(), 0);
    drop(store);
    let connection = rig.connection();
    let kept: String = connection
        .query_row(
            "SELECT group_concat(event_id) FROM account_observation_event",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kept, "young");
    assert_eq!(rig.count("account_observation"), 4);
}

fn decision_input(id: &str, parent: &str) -> DecisionInput {
    DecisionInput {
        decision_id: Some(id.into()),
        task_id: Some("backup-task".into()),
        parent_session_id: Some(parent.into()),
        target_agent: "mason".into(),
        requirements_json: "{}".into(),
        selected_route_key: Some(ROUTE.into()),
        selected_provider_id: Some("openai".into()),
        selected_model_id: Some("gpt-5.5".into()),
        score_json: "{}".into(),
        rejected_json: "[]".into(),
        anchor_version: 2,
        ..Default::default()
    }
}

fn outcome(outcome: RouteOutcome, error: Option<&str>, at: i64) -> DecisionOutcomeUpdate {
    DecisionOutcomeUpdate {
        provider_outcome: None,
        outcome,
        last_error: error.map(str::to_owned),
        observed_at_ms: at,
        failure_backoff: FailureBackoffConfig::default(),
    }
}

fn route_health(rig: &Rig) -> Option<(i64, i64, Option<f64>)> {
    rig.connection()
        .query_row(
            "SELECT failure_count, consecutive_terminal_failures, failure_backoff_until \
             FROM model_route_state WHERE route_key = ?1",
            [ROUTE],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok()
}

/// With the decision ledger restored empty: the head's content-policy
/// refusal is still found (it is read from `model_route_parent_failures`),
/// and an outcome for a decision made before the backup is accepted and
/// changes nothing, where it used to fail on the missing row.
#[test]
fn routing_with_the_decision_ledger_restored_empty_keeps_withholding_and_drops_lost_outcomes() {
    let rig = Rig::new("decision-ledger");
    let refused_at = NOW - 10 * 60_000;
    {
        let store = rig.open();
        store
            .record_decision(&decision_input("dec-refused", "head-a"))
            .unwrap();
        store
            .set_decision_outcome(
                "dec-refused",
                &outcome(RouteOutcome::Error, Some(CONTENT_POLICY), refused_at),
            )
            .unwrap();
        store
            .record_decision(&decision_input("dec-in-flight", "head-a"))
            .unwrap();
    }
    let health_before = route_health(&rig);
    rig.restore_empty(&["model_route_decisions"]);
    assert_eq!(rig.count("model_route_decisions"), 0);
    assert_eq!(rig.count("model_route_parent_failures"), 1);

    let store = rig.open();
    let failed = store
        .recent_failed_decisions_for_parent("head-a", NOW - HOUR)
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].route_key, ROUTE);
    assert_eq!(failed[0].provider_id, "openai");
    assert_eq!(failed[0].outcome_error, CONTENT_POLICY);

    store
        .set_decision_outcome(
            "dec-in-flight",
            &outcome(
                RouteOutcome::Error,
                Some("HTTP 503 provider unavailable"),
                NOW,
            ),
        )
        .expect("an outcome for a decision the ledger lost is accepted");
    drop(store);
    assert_eq!(route_health(&rig), health_before);
    assert_eq!(rig.count("model_route_decisions"), 0);
}

/// After the ledger was restored empty, no ledger delete reaches the per-head
/// failure copies that migration 54's triggers made from it; retention prunes
/// them on the decision window by outcome time, bounded per batch.
#[test]
fn parent_failure_copies_left_by_a_restore_are_pruned_on_the_decision_window() {
    let rig = Rig::new("parent-failure-retention");
    {
        let store = rig.open();
        for (index, at) in [NOW - 40 * 24 * HOUR, NOW - 35 * 24 * HOUR, NOW - HOUR]
            .into_iter()
            .enumerate()
        {
            let id = format!("dec-{index}");
            store
                .record_decision(&decision_input(&id, "head-a"))
                .unwrap();
            store
                .set_decision_outcome(&id, &outcome(RouteOutcome::Error, Some(CONTENT_POLICY), at))
                .unwrap();
        }
    }
    rig.restore_empty(&["model_route_decisions"]);
    let store = rig.open();
    let cutoff = NOW - 30 * 24 * HOUR;
    assert_eq!(store.prune_parent_failures(cutoff, 1).unwrap(), 1);
    assert_eq!(store.prune_parent_failures(cutoff, 1).unwrap(), 1);
    assert_eq!(store.prune_parent_failures(cutoff, 1).unwrap(), 0);
    let kept = store
        .recent_failed_decisions_for_parent("head-a", 0)
        .unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].outcome_at, NOW - HOUR);
    drop(store);
    assert_eq!(
        rig.connection()
            .query_row(
                "SELECT decision_id FROM model_route_parent_failures",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "dec-2"
    );
}
