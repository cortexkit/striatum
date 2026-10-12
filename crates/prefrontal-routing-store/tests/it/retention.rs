//! Retention batches: each deletes only rows past their window, keeps
//! notable account decisions (holds and refusals) longer than routine ones,
//! never deletes serve decisions or rows something still references, and
//! touches at most the requested number of rows per call.

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::{DecisionInput, RouteStateStore};
use prefrontal_routing_store::{
    dev_descriptor, AccountAdmissionRow, AccountDecisionRecord, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, DecisionRetentionCursor,
    RetentionBatch, SqliteRouteStateStore, DECISION_MODE_SERVE, DECISION_MODE_SHADOW,
    ROUTINE_REFUSAL_CODE,
};
use rusqlite::{params, Connection};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

const HOUR: i64 = 60 * 60_000;
const DAY: i64 = 24 * HOUR;
/// The test clock, 2026-10-12T00:00:00Z.
const NOW: i64 = 1_791_763_200_000;
/// The default windows: 72 hours for routine decisions, 14 days for notable.
const ROUTINE_BEFORE: i64 = NOW - 72 * HOUR;
const NOTABLE_BEFORE: i64 = NOW - 14 * DAY;

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-retention-{label}-{}-{id}",
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
        SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open route-state store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn decision(
    id: &str,
    mode: &str,
    outcome: &str,
    code: &str,
    created_at: i64,
) -> AccountDecisionRecord {
    AccountDecisionRecord {
        decision_id: id.into(),
        mode: mode.into(),
        trigger: "observation".into(),
        principal: "reserved:broca".into(),
        admission_id: None,
        request_mode: Some("admit".into()),
        provider: "anthropic".into(),
        model: "claude-opus-5-5".into(),
        family: "opus".into(),
        outcome: outcome.into(),
        code: code.into(),
        account_id: None,
        credential_id: None,
        policy_version: 2,
        decision_json: "{}".into(),
        context_json: None,
        created_at,
    }
}

fn insert_decisions(store: &SqliteRouteStateStore, rows: &[AccountDecisionRecord]) {
    store
        .account_transaction(|tx| {
            for row in rows {
                tx.insert_decision(row)?;
            }
            Ok(())
        })
        .unwrap();
}

fn decision_ids(store: &SqliteRouteStateStore) -> Vec<String> {
    let mut ids: Vec<String> = [DECISION_MODE_SHADOW, DECISION_MODE_SERVE]
        .iter()
        .flat_map(|mode| store.account_decisions(mode, 10_000).unwrap())
        .map(|row| row.decision_id)
        .collect();
    ids.sort();
    ids
}

/// Run batches until one comes back short, as the routing module's pass does.
fn prune_all(store: &SqliteRouteStateStore, limit: usize) -> usize {
    let mut cursor = DecisionRetentionCursor::default();
    let mut deleted = 0;
    for _ in 0..1_000 {
        let batch = store
            .prune_account_decisions(ROUTINE_BEFORE, NOTABLE_BEFORE, &mut cursor, limit)
            .unwrap();
        assert!(batch.examined <= limit, "{batch:?}");
        deleted += batch.deleted;
        if batch.examined < limit {
            return deleted;
        }
    }
    panic!("retention never finished");
}

#[test]
fn account_decision_retention_deletes_only_rows_past_their_window_and_keeps_refusals_longer() {
    let rig = Rig::new("decision-windows");
    let store = rig.open();
    let shadow = DECISION_MODE_SHADOW;
    insert_decisions(
        &store,
        &[
            // Routine shadow rows: due once older than 72 hours.
            decision(
                "admit-old",
                shadow,
                "admit",
                "cold_scored",
                ROUTINE_BEFORE - 1,
            ),
            decision(
                "admit-at-cutoff",
                shadow,
                "admit",
                "cold_scored",
                ROUTINE_BEFORE,
            ),
            decision("admit-young", shadow, "admit", "cold_scored", NOW - HOUR),
            decision(
                "unsupported-old",
                shadow,
                "refuse",
                ROUTINE_REFUSAL_CODE,
                ROUTINE_BEFORE - 1,
            ),
            // Notable shadow rows: past 72 hours but kept until 14 days.
            decision(
                "refuse-4d",
                shadow,
                "refuse",
                "account_quota_unknown",
                NOW - 4 * DAY,
            ),
            decision("hold-4d", shadow, "hold", "account_hold", NOW - 4 * DAY),
            decision(
                "refuse-at-cutoff",
                shadow,
                "refuse",
                "account_quota_unknown",
                NOTABLE_BEFORE,
            ),
            decision(
                "refuse-15d",
                shadow,
                "refuse",
                "account_pool_reserve_limited",
                NOW - 15 * DAY,
            ),
            decision("hold-15d", shadow, "hold", "account_hold", NOW - 15 * DAY),
            // Serve rows are the served audit trail and are never deleted.
            decision(
                "serve-admit-old",
                DECISION_MODE_SERVE,
                "admit",
                "cold_scored",
                NOW - 60 * DAY,
            ),
            decision(
                "serve-refuse-old",
                DECISION_MODE_SERVE,
                "refuse",
                "account_quota_unknown",
                NOW - 60 * DAY,
            ),
        ],
    );

    let deleted = prune_all(&store, 256);

    assert_eq!(deleted, 4);
    assert_eq!(
        decision_ids(&store),
        vec![
            "admit-at-cutoff",
            "admit-young",
            "hold-4d",
            "refuse-4d",
            "refuse-at-cutoff",
            "serve-admit-old",
            "serve-refuse-old",
        ]
    );
}

#[test]
fn account_decision_retention_is_bounded_per_batch() {
    let rig = Rig::new("decision-bounded");
    let store = rig.open();
    // Ten rows older than both windows, which the batch deletes outright,
    // and ten routine rows between the two cutoffs, which it deletes after
    // checking their class. The bound must hold on both paths.
    let rows: Vec<AccountDecisionRecord> = (0..10)
        .map(|index| {
            decision(
                &format!("very-old-{index:02}"),
                DECISION_MODE_SHADOW,
                "admit",
                "cold_scored",
                NOW - 20 * DAY + index,
            )
        })
        .chain((0..10).map(|index| {
            decision(
                &format!("old-{index:02}"),
                DECISION_MODE_SHADOW,
                "admit",
                "cold_scored",
                NOW - 10 * DAY + index,
            )
        }))
        .collect();
    insert_decisions(&store, &rows);

    let mut cursor = DecisionRetentionCursor::default();
    let mut batches = Vec::new();
    loop {
        let batch = store
            .prune_account_decisions(ROUTINE_BEFORE, NOTABLE_BEFORE, &mut cursor, 3)
            .unwrap();
        batches.push(batch);
        if batch.examined < 3 {
            break;
        }
    }

    let deleted: Vec<usize> = batches.iter().map(|batch| batch.deleted).collect();
    // The fourth batch finishes the very old rows and spends its remaining
    // two rows on the band.
    assert_eq!(deleted, vec![3, 3, 3, 3, 3, 3, 2]);
    assert!(decision_ids(&store).is_empty());
    assert_eq!(
        store
            .prune_account_decisions(ROUTINE_BEFORE, NOTABLE_BEFORE, &mut cursor, 0)
            .unwrap(),
        RetentionBatch::default()
    );
}

/// Between the two cutoffs, kept notable rows sit ahead of due routine rows
/// in time order. A batch smaller than that run of kept rows must still reach
/// the routine rows behind it on later batches instead of re-reading the same
/// kept rows forever.
#[test]
fn routine_rows_behind_a_run_of_kept_refusals_are_still_reached() {
    let rig = Rig::new("decision-band");
    let store = rig.open();
    let mut rows = Vec::new();
    for index in 0..6 {
        rows.push(decision(
            &format!("kept-refusal-{index}"),
            DECISION_MODE_SHADOW,
            "refuse",
            "account_quota_unknown",
            NOW - 10 * DAY + index,
        ));
    }
    for index in 0..3 {
        rows.push(decision(
            &format!("due-admit-{index}"),
            DECISION_MODE_SHADOW,
            "admit",
            "cold_scored",
            NOW - 9 * DAY + index,
        ));
    }
    insert_decisions(&store, &rows);

    let deleted = prune_all(&store, 2);

    assert_eq!(deleted, 3);
    let kept = decision_ids(&store);
    assert_eq!(kept.len(), 6, "{kept:?}");
    assert!(
        kept.iter().all(|id| id.starts_with("kept-refusal-")),
        "{kept:?}"
    );
}

/// The child table that keeps one shadow decision alive in the reference
/// tests below.
#[derive(Debug, Clone, Copy)]
enum DecisionChild {
    Pin,
    Admission,
    Hold,
}

fn pin_row(pin_id: &str, decision_id: &str) -> AccountPinRow {
    AccountPinRow {
        pin_id: pin_id.into(),
        principal: "reserved:broca".into(),
        conversation_hash: format!("conversation-{pin_id}"),
        provider: "anthropic".into(),
        pin_generation: 1,
        affinity_model: "claude-opus-5-5".into(),
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        pin_mode: "run_frozen".into(),
        previous_pin_id: None,
        decision_id: decision_id.into(),
        created_at: NOW - 30 * DAY,
    }
}

/// Make `child` the only row that references `decision_id`. An admission
/// must name a pin, so its pin points at a separate serve decision, which
/// retention never deletes.
fn reference_decision(store: &SqliteRouteStateStore, child: DecisionChild, decision_id: &str) {
    if let DecisionChild::Admission = child {
        insert_decisions(
            store,
            &[decision(
                &format!("anchor-{decision_id}"),
                DECISION_MODE_SERVE,
                "admit",
                "cold_scored",
                NOW - 30 * DAY,
            )],
        );
    }
    store
        .account_transaction(|tx| match child {
            DecisionChild::Pin => {
                tx.insert_pin(&pin_row(&format!("pin-{decision_id}"), decision_id))
            }
            DecisionChild::Admission => {
                let pin_id = format!("pin-anchor-{decision_id}");
                tx.insert_pin(&pin_row(&pin_id, &format!("anchor-{decision_id}")))?;
                tx.insert_admission(&AccountAdmissionRow {
                    principal: "reserved:broca".into(),
                    admission_id: format!("admission-{decision_id}"),
                    request_fingerprint: "fp".into(),
                    request_mode: "admit".into(),
                    decision_id: decision_id.into(),
                    conversation_hash: format!("conversation-{pin_id}"),
                    provider: "anthropic".into(),
                    model: "claude-opus-5-5".into(),
                    account_id: "acct-a".into(),
                    credential_id: "oauth:anthropic".into(),
                    pin_id,
                    pin_generation: 1,
                    state: "finished".into(),
                    charged: true,
                    request_bytes: 12_000,
                    request_bytes_estimated: false,
                    committed_at: NOW - 30 * DAY,
                    admission_valid_until: NOW - 30 * DAY,
                    lease_valid_until: NOW - 30 * DAY,
                    reply_json: "{}".into(),
                    run_id: None,
                    started_at: None,
                    started_late: false,
                    released_at: None,
                    finished_at: Some(NOW - 30 * DAY),
                })
            }
            DecisionChild::Hold => tx.insert_hold(&AccountHoldRow {
                decision_id: decision_id.into(),
                principal: "reserved:broca".into(),
                conversation_hash: "conversation".into(),
                provider: "anthropic".into(),
                pin_id: "pin-none".into(),
                pin_generation: 1,
                account_id: "acct-a".into(),
                credential_id: "oauth:anthropic".into(),
                basis: "five_hour".into(),
                reset_at: NOW,
                retry_after_s: 60,
                created_at: NOW - 30 * DAY,
            }),
        })
        .unwrap();
}

/// One old routine decision older than both windows and one between the
/// cutoffs are each referenced only by `child`; unreferenced neighbours sit
/// on both sides of them in time. Retention runs with foreign keys on, so
/// deleting a referenced row would fail the batch: the referenced rows must
/// be skipped, and every neighbour still deleted, both with one large batch
/// and with batches smaller than the run of rows.
fn a_decision_only_one_child_references_survives(child: DecisionChild) {
    for limit in [256, 2] {
        let rig = Rig::new(&format!("decision-child-{child:?}-{limit}"));
        let store = rig.open();
        let mut rows = Vec::new();
        for (prefix, base) in [("old", NOW - 30 * DAY), ("band", NOW - 10 * DAY)] {
            for offset in [0, 2, 3] {
                rows.push(decision(
                    &format!("free-{prefix}-{offset}"),
                    DECISION_MODE_SHADOW,
                    "admit",
                    "cold_scored",
                    base + offset,
                ));
            }
            rows.push(decision(
                &format!("referenced-{prefix}"),
                DECISION_MODE_SHADOW,
                "admit",
                "cold_scored",
                base + 1,
            ));
        }
        insert_decisions(&store, &rows);
        reference_decision(&store, child, "referenced-old");
        reference_decision(&store, child, "referenced-band");

        let deleted = prune_all(&store, limit);

        assert_eq!(deleted, 6, "{child:?} limit {limit}");
        let mut expected = vec!["referenced-band".to_string(), "referenced-old".to_string()];
        if let DecisionChild::Admission = child {
            expected.extend([
                "anchor-referenced-band".into(),
                "anchor-referenced-old".into(),
            ]);
            expected.sort();
        }
        assert_eq!(decision_ids(&store), expected, "{child:?} limit {limit}");
    }
}

#[test]
fn a_decision_only_a_pin_references_survives_and_its_neighbours_go() {
    a_decision_only_one_child_references_survives(DecisionChild::Pin);
}

#[test]
fn a_decision_only_an_admission_references_survives_and_its_neighbours_go() {
    a_decision_only_one_child_references_survives(DecisionChild::Admission);
}

#[test]
fn a_decision_only_a_hold_references_survives_and_its_neighbours_go() {
    a_decision_only_one_child_references_survives(DecisionChild::Hold);
}

fn observation(id: &str, received_at: i64) -> AccountObservationRow {
    AccountObservationRow {
        observation_id: id.into(),
        principal: "reserved:broca".into(),
        runner: "broca".into(),
        event_id: id.into(),
        kind: "attempt".into(),
        admission_id: None,
        bound: false,
        observation_json: "{}".into(),
        effects_json: "{}".into(),
        received_at,
    }
}

#[test]
fn observation_retention_keeps_young_and_fenced_rows_and_is_bounded() {
    let rig = Rig::new("observations");
    let store = rig.open();
    let before = NOW - 7 * DAY;
    store
        .account_transaction(|tx| {
            for index in 0..5 {
                tx.insert_observation(&observation(&format!("old-{index}"), before - 10 + index))?;
            }
            tx.insert_observation(&observation("at-cutoff", before))?;
            tx.insert_observation(&observation("young", NOW))?;
            tx.insert_observation(&observation("fenced", before - 100))?;
            tx.insert_fence(&AccountFenceRow {
                fence_id: "fence-1".into(),
                provider: "anthropic".into(),
                account_id: "acct-a".into(),
                scope_json: "{}".into(),
                fence_json: "{}".into(),
                source: "observation".into(),
                observed_at: before - 100,
                principal: "reserved:broca".into(),
                admission_id: None,
                observation_id: Some("fenced".into()),
                created_at: before - 100,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();

    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 2);
    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 2);
    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 1);
    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 0);

    let connection = Connection::open(rig.db_path()).unwrap();
    let mut statement = connection
        .prepare("SELECT observation_id FROM account_observation ORDER BY observation_id")
        .unwrap();
    let kept: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(kept, vec!["at-cutoff", "fenced", "young"]);
}

/// An old observation referenced only by a scoped not-found exclusion is
/// kept, and its unreferenced neighbours on both sides are still deleted
/// without a foreign-key error.
#[test]
fn an_observation_only_an_exclusion_references_survives_and_its_neighbours_go() {
    let rig = Rig::new("observation-exclusion");
    let store = rig.open();
    let before = NOW - 7 * DAY;
    store
        .account_transaction(|tx| {
            tx.insert_observation(&observation("free-0", before - 30))?;
            tx.insert_observation(&observation("excluded", before - 20))?;
            tx.insert_observation(&observation("free-1", before - 10))?;
            tx.insert_observation(&observation("free-2", before - 5))?;
            tx.insert_exclusion(&AccountExclusionRow {
                principal: "reserved:broca".into(),
                admission_id: "admission-1".into(),
                credential_id: "oauth:anthropic".into(),
                runner_view_digest: "runner-view".into(),
                roster_view_digest: "roster-view".into(),
                observation_id: Some("excluded".into()),
                recorded_at: before - 20,
            })
            .map(|_| ())
        })
        .unwrap();

    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 2);
    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 1);
    assert_eq!(store.prune_account_observations(before, 2).unwrap(), 0);

    let connection = Connection::open(rig.db_path()).unwrap();
    let kept: String = connection
        .query_row(
            "SELECT group_concat(observation_id) FROM account_observation",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kept, "excluded");
}

#[test]
fn usage_retention_deletes_only_old_rows_and_is_bounded() {
    let rig = Rig::new("usage");
    let store = rig.open();
    let before = NOW - 30 * DAY;
    {
        let connection = Connection::open(rig.db_path()).unwrap();
        let mut insert = connection
            .prepare(
                "INSERT INTO model_route_usage (id, route_key, capacity_key, provider_id, observed_at) \
                 VALUES (?1, 'anthropic/claude-opus-5-5', 'anthropic/claude-opus-5-5', 'anthropic', ?2)",
            )
            .unwrap();
        for index in 0..3 {
            insert
                .execute(params![format!("old-{index}"), before - 1 - index])
                .unwrap();
        }
        insert.execute(params!["at-cutoff", before]).unwrap();
        insert.execute(params!["young", NOW]).unwrap();
    }

    assert_eq!(store.prune_route_usage(before, 2).unwrap(), 2);
    assert_eq!(store.prune_route_usage(before, 2).unwrap(), 1);
    assert_eq!(store.prune_route_usage(before, 2).unwrap(), 0);

    let connection = Connection::open(rig.db_path()).unwrap();
    let mut statement = connection
        .prepare("SELECT id FROM model_route_usage ORDER BY id")
        .unwrap();
    let kept: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(kept, vec!["at-cutoff", "young"]);
}

#[test]
fn route_decision_retention_is_bounded_per_call() {
    let rig = Rig::new("route-decisions");
    let store = rig.open();
    for index in 0..5 {
        store
            .record_decision(&DecisionInput {
                decision_id: Some(format!("dec-old-{index}")),
                task_id: Some("retention-task".into()),
                target_agent: "mason".into(),
                requirements_json: "{}".into(),
                score_json: "{}".into(),
                rejected_json: "[]".into(),
                anchor_version: 2,
                ..Default::default()
            })
            .unwrap();
    }
    {
        let connection = Connection::open(rig.db_path()).unwrap();
        connection
            .execute("UPDATE model_route_decisions SET created_at = ?1", [1_000])
            .unwrap();
    }

    assert_eq!(store.delete_decisions_older_than(2_000, 2).unwrap(), 2);
    assert_eq!(store.delete_decisions_older_than(2_000, 2).unwrap(), 2);
    assert_eq!(store.delete_decisions_older_than(2_000, 2).unwrap(), 1);
    assert_eq!(store.delete_decisions_older_than(2_000, 2).unwrap(), 0);
}
