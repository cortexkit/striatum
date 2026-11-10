//! Retention batches: each deletes only rows past their window, purges the
//! retired shadow decisions whatever their age, never deletes a row
//! something still references, and touches at most the requested number of
//! rows per call.

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::{DecisionInput, RouteStateStore};
use prefrontal_routing_store::{
    dev_descriptor, AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord,
    AccountExclusionRow, AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow,
    AffinityRetentionBatch, SqliteRouteStateStore, DECISION_MODE_SERVE,
};
use rusqlite::{params, Connection};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

const HOUR: i64 = 60 * 60_000;
const DAY: i64 = 24 * HOUR;
/// The test clock, 2026-10-12T00:00:00Z.
const NOW: i64 = 1_791_763_200_000;
/// The default account-selection window: 30 days.
const BEFORE: i64 = NOW - 30 * DAY;
/// The mode the retired shadow mode recorded its decisions under.
const SHADOW: &str = "shadow";
const PRINCIPAL: &str = "reserved:broca";

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-retention-{label}-{}-{id}",
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

    /// The ids in `column` of `table`, sorted.
    fn ids(&self, table: &str, column: &str) -> Vec<String> {
        let connection = Connection::open(self.db_path()).unwrap();
        let mut statement = connection
            .prepare(&format!("SELECT {column} FROM {table} ORDER BY {column}"))
            .unwrap();
        let ids = statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        ids
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
        trigger: if mode == SHADOW {
            "observation"
        } else {
            "request"
        }
        .into(),
        principal: PRINCIPAL.into(),
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
    let mut ids: Vec<String> = [SHADOW, DECISION_MODE_SERVE]
        .iter()
        .flat_map(|mode| store.account_decisions(mode, 10_000).unwrap())
        .map(|row| row.decision_id)
        .collect();
    ids.sort();
    ids
}

/// Run `batch` until one comes back short, as the routing module's pass does.
fn drain(limit: usize, mut batch: impl FnMut(usize) -> usize) -> usize {
    let mut deleted = 0;
    for _ in 0..1_000 {
        let removed = batch(limit);
        assert!(removed <= limit, "{removed} > {limit}");
        deleted += removed;
        if removed < limit {
            return deleted;
        }
    }
    panic!("retention never finished");
}

#[test]
fn the_shadow_purge_deletes_every_shadow_decision_whatever_its_age_and_no_serve_row() {
    let rig = Rig::new("shadow-purge");
    let store = rig.open();
    let mut rows: Vec<AccountDecisionRecord> = (0..7)
        .map(|index| {
            decision(
                &format!("shadow-{index}"),
                SHADOW,
                "admit",
                "cold_scored",
                // From sixty days old to one written this instant.
                NOW - index * 10 * DAY,
            )
        })
        .collect();
    rows.push(decision(
        "shadow-refusal",
        SHADOW,
        "refuse",
        "account_selection_unsupported",
        NOW - HOUR,
    ));
    rows.push(decision(
        "serve-old",
        DECISION_MODE_SERVE,
        "admit",
        "cold_scored",
        NOW - 60 * DAY,
    ));
    rows.push(decision(
        "serve-young",
        DECISION_MODE_SERVE,
        "refuse",
        "account_quota_unknown",
        NOW,
    ));
    insert_decisions(&store, &rows);

    let mut batches = Vec::new();
    let deleted = drain(3, |limit| {
        let removed = store.purge_shadow_decisions(limit).unwrap();
        batches.push(removed);
        removed
    });

    assert_eq!(deleted, 8);
    assert_eq!(batches, vec![3, 3, 2], "bounded per batch");
    assert_eq!(decision_ids(&store), vec!["serve-old", "serve-young"]);
    assert_eq!(store.purge_shadow_decisions(256).unwrap(), 0);
}

fn pin_row(pin_id: &str, conversation: &str, generation: u64, decision_id: &str) -> AccountPinRow {
    AccountPinRow {
        pin_id: pin_id.into(),
        principal: PRINCIPAL.into(),
        conversation_hash: conversation.into(),
        provider: "anthropic".into(),
        pin_generation: generation,
        affinity_model: "claude-opus-5-5".into(),
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        pin_mode: "run_frozen".into(),
        previous_pin_id: None,
        decision_id: decision_id.into(),
        created_at: NOW - 60 * DAY,
    }
}

/// A finished admission whose every time is `at`.
fn admission_row(
    admission_id: &str,
    decision_id: &str,
    pin: &AccountPinRow,
    at: i64,
) -> AccountAdmissionRow {
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
        state: "finished".into(),
        charged: true,
        request_bytes: 12_000,
        request_bytes_estimated: false,
        committed_at: at,
        admission_valid_until: at,
        lease_valid_until: at,
        reply_json: "{}".into(),
        run_id: None,
        started_at: Some(at),
        started_late: false,
        released_at: None,
        finished_at: Some(at),
    }
}

fn affinity_row(conversation: &str, pin: &AccountPinRow, expires_at: i64) -> AccountAffinityRow {
    AccountAffinityRow {
        principal: PRINCIPAL.into(),
        conversation_hash: conversation.into(),
        provider: "anthropic".into(),
        affinity_model: "claude-opus-5-5".into(),
        pin_id: pin.pin_id.clone(),
        pin_generation: pin.pin_generation,
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        credential_binding_revision: 1,
        policy_version: 2,
        initial_demand_bytes: 12_000,
        created_at: expires_at - 7 * DAY,
        last_used_at: expires_at - 7 * DAY,
        expires_at,
        lease_valid_until: Some(expires_at - 7 * DAY),
    }
}

/// An old serve decision referenced only by `child` survives; its
/// unreferenced neighbours on both sides in time are deleted without a
/// foreign-key error, with one large batch and with batches smaller than the
/// run of rows. Young serve decisions are kept.
#[derive(Debug, Clone, Copy)]
enum DecisionChild {
    Pin,
    Admission,
    Hold,
}

fn reference_decision(store: &SqliteRouteStateStore, child: DecisionChild, decision_id: &str) {
    store
        .account_transaction(|tx| match child {
            DecisionChild::Pin => tx.insert_pin(&pin_row(
                &format!("pin-{decision_id}"),
                &format!("conversation-{decision_id}"),
                1,
                decision_id,
            )),
            DecisionChild::Admission => {
                // An admission must name a pin; its pin points at a young
                // anchor decision the window keeps anyway.
                tx.insert_decision(&decision(
                    &format!("anchor-{decision_id}"),
                    DECISION_MODE_SERVE,
                    "admit",
                    "cold_scored",
                    NOW,
                ))?;
                let pin = pin_row(
                    &format!("pin-anchor-{decision_id}"),
                    &format!("conversation-{decision_id}"),
                    1,
                    &format!("anchor-{decision_id}"),
                );
                tx.insert_pin(&pin)?;
                // A young admission: the window keeps it, so it keeps the
                // old decision it names.
                tx.insert_admission(&admission_row(
                    &format!("admission-{decision_id}"),
                    decision_id,
                    &pin,
                    NOW,
                ))
            }
            DecisionChild::Hold => tx.insert_hold(&AccountHoldRow {
                decision_id: decision_id.into(),
                principal: PRINCIPAL.into(),
                conversation_hash: "conversation".into(),
                provider: "anthropic".into(),
                pin_id: "pin-none".into(),
                pin_generation: 1,
                account_id: "acct-a".into(),
                credential_id: "oauth:anthropic".into(),
                basis: "five_hour".into(),
                reset_at: NOW,
                retry_after_s: 60,
                created_at: NOW,
            }),
        })
        .unwrap();
}

fn an_old_serve_decision_only_one_child_references_survives(child: DecisionChild) {
    for limit in [256, 2] {
        let rig = Rig::new(&format!("serve-decision-child-{child:?}-{limit}"));
        let store = rig.open();
        let mut rows = Vec::new();
        for offset in [0, 2, 3] {
            rows.push(decision(
                &format!("free-{offset}"),
                DECISION_MODE_SERVE,
                "refuse",
                "account_quota_unknown",
                BEFORE - 10 + offset,
            ));
        }
        rows.push(decision(
            "referenced",
            DECISION_MODE_SERVE,
            "admit",
            "cold_scored",
            BEFORE - 9,
        ));
        rows.push(decision(
            "at-cutoff",
            DECISION_MODE_SERVE,
            "refuse",
            "account_quota_unknown",
            BEFORE,
        ));
        insert_decisions(&store, &rows);
        reference_decision(&store, child, "referenced");

        let deleted = drain(limit, |limit| {
            store.prune_serve_decisions(BEFORE, limit).unwrap()
        });

        assert_eq!(deleted, 3, "{child:?} limit {limit}");
        let mut expected = vec!["at-cutoff".to_string(), "referenced".to_string()];
        if let DecisionChild::Admission = child {
            expected.push("anchor-referenced".into());
            expected.sort();
        }
        assert_eq!(decision_ids(&store), expected, "{child:?} limit {limit}");
    }
}

#[test]
fn an_old_serve_decision_only_a_pin_references_survives_and_its_neighbours_go() {
    an_old_serve_decision_only_one_child_references_survives(DecisionChild::Pin);
}

#[test]
fn an_old_serve_decision_only_an_admission_references_survives_and_its_neighbours_go() {
    an_old_serve_decision_only_one_child_references_survives(DecisionChild::Admission);
}

#[test]
fn an_old_serve_decision_only_a_hold_references_survives_and_its_neighbours_go() {
    an_old_serve_decision_only_one_child_references_survives(DecisionChild::Hold);
}

#[test]
fn admission_retention_waits_for_every_time_the_admission_carries() {
    let rig = Rig::new("admissions");
    let store = rig.open();
    let old = BEFORE - DAY;
    store
        .account_transaction(|tx| {
            tx.insert_decision(&decision(
                "d",
                DECISION_MODE_SERVE,
                "admit",
                "cold_scored",
                old,
            ))?;
            let pin = pin_row("pin", "conversation", 1, "d");
            tx.insert_pin(&pin)?;
            for index in 0..3 {
                tx.insert_admission(&admission_row(
                    &format!("old-{index}"),
                    "d",
                    &pin,
                    old + index,
                ))?;
            }
            tx.insert_admission(&admission_row("at-cutoff", "d", &pin, BEFORE))?;
            // Committed long ago, but one later time is still inside the
            // window: a run that finished recently, a lease renewed, a
            // reservation released, a late start.
            let mut finished = admission_row("finished-recently", "d", &pin, old);
            finished.finished_at = Some(NOW - DAY);
            tx.insert_admission(&finished)?;
            let mut leased = admission_row("leased-recently", "d", &pin, old);
            leased.lease_valid_until = NOW;
            tx.insert_admission(&leased)?;
            let mut released = admission_row("released-recently", "d", &pin, old);
            released.released_at = Some(NOW - DAY);
            tx.insert_admission(&released)?;
            let mut started = admission_row("started-recently", "d", &pin, old);
            started.started_at = Some(NOW - DAY);
            tx.insert_admission(&started)?;
            let mut reserved = admission_row("valid-recently", "d", &pin, old);
            reserved.admission_valid_until = NOW;
            tx.insert_admission(&reserved)
        })
        .unwrap();

    let mut batches = Vec::new();
    let deleted = drain(2, |limit| {
        let removed = store.prune_account_admissions(BEFORE, limit).unwrap();
        batches.push(removed);
        removed
    });

    assert_eq!(deleted, 3);
    assert_eq!(batches, vec![2, 1]);
    assert_eq!(
        rig.ids("account_admission", "admission_id"),
        vec![
            "at-cutoff",
            "finished-recently",
            "leased-recently",
            "released-recently",
            "started-recently",
            "valid-recently",
        ]
    );
}

#[test]
fn hold_retention_deletes_only_old_holds_and_is_bounded() {
    let rig = Rig::new("holds");
    let store = rig.open();
    store
        .account_transaction(|tx| {
            for (id, created_at) in [
                ("old-0", BEFORE - 3),
                ("old-1", BEFORE - 2),
                ("old-2", BEFORE - 1),
                ("at-cutoff", BEFORE),
                ("young", NOW),
            ] {
                tx.insert_decision(&decision(
                    id,
                    DECISION_MODE_SERVE,
                    "hold",
                    "account_hold",
                    created_at,
                ))?;
                tx.insert_hold(&AccountHoldRow {
                    decision_id: id.into(),
                    principal: PRINCIPAL.into(),
                    conversation_hash: "conversation".into(),
                    provider: "anthropic".into(),
                    pin_id: "pin-none".into(),
                    pin_generation: 1,
                    account_id: "acct-a".into(),
                    credential_id: "oauth:anthropic".into(),
                    basis: "five_hour".into(),
                    reset_at: created_at + HOUR,
                    retry_after_s: 60,
                    created_at,
                })?;
            }
            Ok(())
        })
        .unwrap();

    assert_eq!(store.prune_account_holds(BEFORE, 2).unwrap(), 2);
    assert_eq!(store.prune_account_holds(BEFORE, 2).unwrap(), 1);
    assert_eq!(store.prune_account_holds(BEFORE, 2).unwrap(), 0);
    assert_eq!(
        rig.ids("account_hold", "decision_id"),
        vec!["at-cutoff", "young"]
    );
    // No hold references those decisions any more, so the serve-decision
    // retention deletes them.
    assert_eq!(store.prune_serve_decisions(BEFORE, 256).unwrap(), 3);
}

/// One conversation's affinity row and every pin generation it went through.
fn insert_conversation(
    store: &SqliteRouteStateStore,
    conversation: &str,
    generations: u64,
    expires_at: i64,
) -> AccountPinRow {
    store
        .account_transaction(|tx| {
            let mut current = None;
            for generation in 1..=generations {
                let decision_id = format!("d-{conversation}-{generation}");
                tx.insert_decision(&decision(
                    &decision_id,
                    DECISION_MODE_SERVE,
                    "admit",
                    "cold_scored",
                    NOW - 60 * DAY,
                ))?;
                let pin = pin_row(
                    &format!("pin-{conversation}-{generation}"),
                    conversation,
                    generation,
                    &decision_id,
                );
                tx.insert_pin(&pin)?;
                current = Some(pin);
            }
            let pin = current.expect("at least one generation");
            assert!(tx.put_affinity(&affinity_row(conversation, &pin, expires_at), None)?);
            Ok(pin)
        })
        .unwrap()
}

#[test]
fn affinity_retention_deletes_a_long_expired_conversation_with_all_its_pins() {
    let rig = Rig::new("affinity");
    let store = rig.open();
    // Expired more than the window ago, two generations: deleted.
    insert_conversation(&store, "expired", 2, BEFORE - 1);
    // Expired, but less than the window ago: kept.
    insert_conversation(&store, "recent", 1, BEFORE);
    // Still live: kept.
    insert_conversation(&store, "live", 1, NOW + DAY);
    // Expired long ago, but a stored admission still names its pin: kept
    // until admission retention removes that admission.
    let named = insert_conversation(&store, "named", 1, BEFORE - 1);
    store
        .account_transaction(|tx| {
            tx.insert_admission(&admission_row(
                "young-admission",
                &named.decision_id,
                &named,
                NOW,
            ))
        })
        .unwrap();
    // Expired long ago, lease renewed inside the window: kept.
    let leased = insert_conversation(&store, "leased", 1, BEFORE - 1);
    store
        .account_transaction(|tx| {
            let mut row = affinity_row("leased", &leased, BEFORE - 1);
            row.lease_valid_until = Some(NOW);
            assert!(tx.put_affinity(&row, Some((&leased.pin_id, leased.pin_generation)))?);
            Ok(())
        })
        .unwrap();

    assert_eq!(
        store.prune_account_affinities(BEFORE, 256).unwrap(),
        AffinityRetentionBatch {
            affinities: 1,
            pins: 2
        }
    );
    assert_eq!(
        rig.ids("account_affinity", "conversation_hash"),
        vec!["leased", "live", "named", "recent"]
    );
    assert!(rig
        .ids("account_pin", "pin_id")
        .iter()
        .all(|pin| !pin.starts_with("pin-expired-")));
    // No pin references those decisions any more, so the serve-decision
    // retention deletes them.
    assert_eq!(store.prune_serve_decisions(BEFORE, 256).unwrap(), 2);

    // The conversation can start again at generation 1 without colliding
    // with a stored older generation.
    let restarted = insert_conversation(&store, "expired", 1, NOW + DAY);
    assert_eq!(restarted.pin_generation, 1);

    // With the young admission gone, the named conversation goes too.
    let connection = Connection::open(rig.db_path()).unwrap();
    connection
        .execute(
            "DELETE FROM account_admission WHERE admission_id = 'young-admission'",
            [],
        )
        .unwrap();
    assert_eq!(
        store.prune_account_affinities(BEFORE, 256).unwrap(),
        AffinityRetentionBatch {
            affinities: 1,
            pins: 1
        }
    );
}

#[test]
fn affinity_retention_is_bounded_per_batch() {
    let rig = Rig::new("affinity-bounded");
    let store = rig.open();
    for index in 0..5 {
        insert_conversation(&store, &format!("expired-{index}"), 1, BEFORE - 10 + index);
    }
    let mut batches = Vec::new();
    loop {
        let batch = store.prune_account_affinities(BEFORE, 2).unwrap();
        batches.push(batch.affinities);
        if batch.affinities < 2 {
            break;
        }
    }
    assert_eq!(batches, vec![2, 2, 1]);
    assert!(rig.ids("account_affinity", "conversation_hash").is_empty());
    assert!(rig.ids("account_pin", "pin_id").is_empty());
    assert_eq!(
        store.prune_account_affinities(BEFORE, 0).unwrap(),
        AffinityRetentionBatch::default()
    );
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
