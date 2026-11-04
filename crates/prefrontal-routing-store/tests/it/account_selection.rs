//! The account-selection store: pins and fences survive a reopen, affinity
//! writes are compare-and-set on the pin, and a failed transaction writes
//! nothing.

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_store::{
    dev_descriptor, AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord,
    AccountDemandRow, AccountFenceRow, AccountPinRow, SqliteRouteStateStore, ACCOUNT_DEMAND_SQL,
    DECISION_MODE_SERVE,
};
use rusqlite::{params, Connection};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "prefrontal-account-selection-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn open(&self) -> SqliteRouteStateStore {
        let path = self.dir.join("store.db").to_string_lossy().into_owned();
        SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open route-state store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

const PRINCIPAL: &str = "reserved:broca";
const CONVERSATION: &str = "conversation-hash-1";

fn decision(id: &str) -> AccountDecisionRecord {
    AccountDecisionRecord {
        decision_id: id.into(),
        mode: DECISION_MODE_SERVE.into(),
        trigger: "request".into(),
        principal: PRINCIPAL.into(),
        admission_id: Some("adm-1".into()),
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
        created_at: 1_000,
    }
}

fn pin(id: &str, generation: u64, account: &str, decision_id: &str) -> AccountPinRow {
    AccountPinRow {
        pin_id: id.into(),
        principal: PRINCIPAL.into(),
        conversation_hash: CONVERSATION.into(),
        provider: "anthropic".into(),
        pin_generation: generation,
        affinity_model: "claude-opus-5-5".into(),
        account_id: account.into(),
        credential_id: "oauth:anthropic".into(),
        pin_mode: "run_frozen".into(),
        previous_pin_id: None,
        decision_id: decision_id.into(),
        created_at: 1_000,
    }
}

fn affinity(pin_id: &str, generation: u64, account: &str) -> AccountAffinityRow {
    AccountAffinityRow {
        principal: PRINCIPAL.into(),
        conversation_hash: CONVERSATION.into(),
        provider: "anthropic".into(),
        affinity_model: "claude-opus-5-5".into(),
        pin_id: pin_id.into(),
        pin_generation: generation,
        account_id: account.into(),
        credential_id: "oauth:anthropic".into(),
        credential_binding_revision: 1,
        policy_version: 2,
        initial_demand_bytes: 12_000,
        created_at: 1_000,
        last_used_at: 1_000,
        expires_at: 1_000 + 7 * 24 * 3_600_000,
        lease_valid_until: Some(121_000),
    }
}

fn admission(id: &str, pin_id: &str, decision_id: &str) -> AccountAdmissionRow {
    AccountAdmissionRow {
        principal: PRINCIPAL.into(),
        admission_id: id.into(),
        request_fingerprint: "fp".into(),
        request_mode: "admit".into(),
        decision_id: decision_id.into(),
        conversation_hash: CONVERSATION.into(),
        provider: "anthropic".into(),
        model: "claude-opus-5-5".into(),
        account_id: "acct-a".into(),
        credential_id: "oauth:anthropic".into(),
        pin_id: pin_id.into(),
        pin_generation: 1,
        state: "reserved".into(),
        charged: true,
        request_bytes: 12_000,
        request_bytes_estimated: false,
        committed_at: 1_000,
        admission_valid_until: 61_000,
        lease_valid_until: 121_000,
        reply_json: "{}".into(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
    }
}

#[test]
fn a_pin_and_its_admission_survive_a_store_reopen() {
    let rig = Rig::new("reopen");
    {
        let store = rig.open();
        store
            .account_transaction(|tx| {
                tx.insert_decision(&decision("dec-1"))?;
                tx.insert_pin(&pin("pin-1", 1, "acct-a", "dec-1"))?;
                assert!(tx.put_affinity(&affinity("pin-1", 1, "acct-a"), None)?);
                tx.insert_admission(&admission("adm-1", "pin-1", "dec-1"))?;
                tx.insert_fence(&AccountFenceRow {
                    fence_id: "fence-1".into(),
                    provider: "anthropic".into(),
                    account_id: "acct-a".into(),
                    scope_json: r#"{"kind":"general","value":"seven_day"}"#.into(),
                    fence_json: "{}".into(),
                    source: "broca_account_limit".into(),
                    observed_at: 1_500,
                    principal: PRINCIPAL.into(),
                    admission_id: Some("adm-1".into()),
                    observation_id: None,
                    created_at: 1_500,
                    cleared_at: None,
                    cleared_reason: None,
                })?;
                Ok(())
            })
            .unwrap();
    }
    let store = rig.open();
    let (affinity_row, admission_row, fences, demand) = store
        .account_transaction(|tx| {
            Ok((
                tx.affinity(PRINCIPAL, CONVERSATION, "anthropic")?,
                tx.admission(PRINCIPAL, "adm-1")?,
                tx.active_fences("anthropic")?,
                tx.demand("anthropic", "acct-a", None)?,
            ))
        })
        .unwrap();
    let affinity_row = affinity_row.expect("affinity restored");
    assert_eq!(affinity_row.pin_id, "pin-1");
    assert_eq!(affinity_row.pin_generation, 1);
    assert_eq!(affinity_row.account_id, "acct-a");
    let admission_row = admission_row.expect("admission restored");
    assert_eq!(admission_row.pin_id, "pin-1");
    assert_eq!(admission_row.state, "reserved");
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].fence_id, "fence-1");
    assert_eq!(demand.len(), 1);
    assert_eq!(demand[0].request_bytes, 12_000);
}

#[test]
fn compare_and_set_refuses_a_stale_expected_pin() {
    let rig = Rig::new("cas");
    let store = rig.open();
    store
        .account_transaction(|tx| {
            tx.insert_decision(&decision("dec-1"))?;
            tx.insert_pin(&pin("pin-1", 1, "acct-a", "dec-1"))?;
            assert!(tx.put_affinity(&affinity("pin-1", 1, "acct-a"), None)?);
            Ok(())
        })
        .unwrap();
    // A move to generation 2 names the current pin and applies.
    let moved = store
        .account_transaction(|tx| {
            tx.insert_decision(&decision("dec-2"))?;
            tx.insert_pin(&pin("pin-2", 2, "acct-b", "dec-2"))?;
            tx.put_affinity(&affinity("pin-2", 2, "acct-b"), Some(("pin-1", 1)))
        })
        .unwrap();
    assert!(moved);
    // A second writer still holding generation 1 changes nothing.
    let stale = store
        .account_transaction(|tx| {
            tx.put_affinity(&affinity("pin-3", 2, "acct-c"), Some(("pin-1", 1)))
        })
        .unwrap();
    assert!(!stale, "a write naming an older generation must not apply");
    let stale_touch = store
        .account_transaction(|tx| {
            tx.touch_affinity(
                PRINCIPAL,
                CONVERSATION,
                "anthropic",
                ("pin-1", 1),
                5,
                6,
                Some(7),
            )
        })
        .unwrap();
    assert!(!stale_touch);
    // A fresh insert over an existing conversation is also refused.
    let fresh = store
        .account_transaction(|tx| tx.put_affinity(&affinity("pin-4", 1, "acct-d"), None))
        .unwrap();
    assert!(!fresh);
    let current = store
        .account_transaction(|tx| tx.affinity(PRINCIPAL, CONVERSATION, "anthropic"))
        .unwrap()
        .unwrap();
    assert_eq!(current.pin_id, "pin-2");
    assert_eq!(current.pin_generation, 2);
    assert_eq!(current.account_id, "acct-b");
}

#[test]
fn a_failed_transaction_writes_nothing_and_release_happens_once() {
    let rig = Rig::new("rollback");
    let store = rig.open();
    let failed: Result<(), _> = store.account_transaction(|tx| {
        tx.insert_decision(&decision("dec-1"))?;
        Err(prefrontal_routing_core::RouteStateStoreError(
            "abort after the first write".into(),
        ))
    });
    assert!(failed.is_err());
    assert!(store.account_decision("dec-1").unwrap().is_none());

    store
        .account_transaction(|tx| {
            tx.insert_decision(&decision("dec-1"))?;
            tx.insert_pin(&pin("pin-1", 1, "acct-a", "dec-1"))?;
            tx.insert_admission(&admission("adm-1", "pin-1", "dec-1"))
        })
        .unwrap();
    let (first, second, demand) = store
        .account_transaction(|tx| {
            Ok((
                tx.mark_admission_released(PRINCIPAL, "adm-1", 2_000)?,
                tx.mark_admission_released(PRINCIPAL, "adm-1", 3_000)?,
                tx.demand("anthropic", "acct-a", None)?,
            ))
        })
        .unwrap();
    assert!(first);
    assert!(!second, "a second release is a no-op");
    assert!(demand.is_empty(), "a released admission charges no demand");
}

#[test]
fn account_demand_query_plan_uses_partial_covering_index_without_temp_btree() {
    let rig = Rig::new("demand-plan");
    let store = rig.open();
    let connection = Connection::open(rig.dir.join("store.db")).unwrap();
    let explain_sql = format!("EXPLAIN QUERY PLAN {ACCOUNT_DEMAND_SQL}");

    for provider in ["anthropic", "openai", "custom"] {
        let mut statement = connection.prepare(&explain_sql).unwrap();
        let details = statement
            .query_map(params![provider, "acct-a", 1_000], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert!(
            details.iter().any(|detail| {
                detail.contains(
                    "SEARCH account_admission USING COVERING INDEX account_admission_demand",
                ) && detail.contains("provider=? AND account_id=? AND committed_at>?")
            }),
            "expected covering index search using account_admission_demand for {provider}, got {details:?}"
        );
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE")),
            "expected no temporary sort for {provider}, got {details:?}"
        );
    }

    drop(connection);
    drop(store);
}

#[test]
fn account_demand_results_identical_with_and_without_covering_index() {
    let rig = Rig::new("demand-results-parity");
    let store = rig.open();

    store
        .account_transaction(|tx| {
            tx.insert_decision(&decision("dec-1"))?;
            tx.insert_pin(&pin("pin-1", 1, "acct-a", "dec-1"))?;

            // 1. anthropic, charged=1, state='reserved'
            let mut adm1 = admission("adm-1", "pin-1", "dec-1");
            adm1.provider = "anthropic".into();
            adm1.committed_at = 3_000;
            adm1.request_bytes = 10_000;
            tx.insert_admission(&adm1)?;

            // 2. anthropic, charged=1, state='started', earlier committed_at
            let mut adm2 = admission("adm-2", "pin-1", "dec-1");
            adm2.provider = "anthropic".into();
            adm2.committed_at = 1_000;
            adm2.state = "started".into();
            adm2.request_bytes = 20_000;
            tx.insert_admission(&adm2)?;

            // 3. anthropic, charged=1, state='finished', tie-break committed_at with adm1
            let mut adm3 = admission("adm-3", "pin-1", "dec-1");
            adm3.provider = "anthropic".into();
            adm3.committed_at = 3_000;
            adm3.state = "finished".into();
            adm3.request_bytes = 30_000;
            tx.insert_admission(&adm3)?;

            // 4. anthropic, charged=1, state='released' (MUST NOT be in demand)
            let mut adm4 = admission("adm-4", "pin-1", "dec-1");
            adm4.provider = "anthropic".into();
            adm4.committed_at = 500;
            adm4.state = "released".into();
            adm4.request_bytes = 40_000;
            tx.insert_admission(&adm4)?;

            // 5. anthropic, charged=0, state='started' (MUST NOT be in demand)
            let mut adm5 = admission("adm-5", "pin-1", "dec-1");
            adm5.provider = "anthropic".into();
            adm5.committed_at = 2_000;
            adm5.charged = false;
            adm5.state = "started".into();
            adm5.request_bytes = 50_000;
            tx.insert_admission(&adm5)?;

            // 6. openai, charged=1, state='reserved'
            let mut adm6 = admission("adm-6", "pin-1", "dec-1");
            adm6.provider = "openai".into();
            adm6.committed_at = 4_000;
            adm6.request_bytes = 60_000;
            tx.insert_admission(&adm6)?;

            // 7. anthropic, another account, charged=1, state='reserved'
            let mut adm7 = admission("adm-7", "pin-1", "dec-1");
            adm7.account_id = "acct-b".into();
            adm7.committed_at = 2_500;
            adm7.request_bytes = 70_000;
            tx.insert_admission(&adm7)?;

            Ok(())
        })
        .unwrap();

    // The floor is exclusive and the account is exact: anthropic's acct-a
    // after 1_000 is adm-1 and adm-3 (adm-2 sits on the floor), in commit
    // then admission-id order.
    let after_floor = store
        .account_transaction(|tx| tx.demand("anthropic", "acct-a", Some(1_000)))
        .unwrap();
    assert_eq!(
        after_floor
            .iter()
            .map(|row| (row.state.as_str(), row.committed_at, row.request_bytes))
            .collect::<Vec<_>>(),
        vec![("reserved", 3_000, 10_000), ("finished", 3_000, 30_000)]
    );

    let connection = Connection::open(rig.dir.join("store.db")).unwrap();

    let query_unindexed =
        "SELECT account_id, state, committed_at, request_bytes FROM account_admission NOT INDEXED \
         WHERE provider = ?1 AND account_id = ?2 AND charged = 1 AND state != 'released' \
           AND committed_at > ?3 \
         ORDER BY committed_at, principal, admission_id";
    let cases = ["anthropic", "openai", "other"]
        .into_iter()
        .flat_map(|provider| {
            ["acct-a", "acct-b"].into_iter().flat_map(move |account| {
                [None, Some(999), Some(1_000), Some(2_999)]
                    .into_iter()
                    .map(move |floor| (provider, account, floor))
            })
        })
        .collect::<Vec<_>>();
    let unindexed = |provider: &str, account: &str, floor: Option<i64>| {
        let mut statement = connection.prepare(query_unindexed).unwrap();
        statement
            .query_map(
                params![provider, account, floor.unwrap_or(i64::MIN)],
                |row| {
                    Ok(AccountDemandRow {
                        account_id: row.get(0)?,
                        state: row.get(1)?,
                        committed_at: row.get(2)?,
                        request_bytes: row.get::<_, i64>(3)? as u64,
                    })
                },
            )
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };

    for &(provider, account, floor) in &cases {
        let indexed_demand = store
            .account_transaction(|tx| tx.demand(provider, account, floor))
            .unwrap();
        assert_eq!(
            indexed_demand,
            unindexed(provider, account, floor),
            "demand results mismatch for {provider}/{account} after {floor:?}"
        );
    }

    // Now drop the index and verify tx.demand still returns the exact same results
    connection
        .execute("DROP INDEX account_admission_demand", [])
        .unwrap();

    for (provider, account, floor) in cases {
        let demand_after_drop = store
            .account_transaction(|tx| tx.demand(provider, account, floor))
            .unwrap();
        assert_eq!(
            demand_after_drop,
            unindexed(provider, account, floor),
            "demand results mismatch after drop for {provider}/{account} after {floor:?}"
        );
    }

    drop(connection);
    drop(store);
}
