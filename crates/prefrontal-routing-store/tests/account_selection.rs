//! The account-selection store: pins and fences survive a reopen, affinity
//! writes are compare-and-set on the pin, and a failed transaction writes
//! nothing.

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_store::{
    dev_descriptor, AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord,
    AccountFenceRow, AccountPinRow, SqliteRouteStateStore, DECISION_MODE_SERVE,
};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = prefrontal_test_support::scratch_root().join(format!(
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
                tx.demand("anthropic")?,
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
            tx.touch_affinity(PRINCIPAL, CONVERSATION, "anthropic", ("pin-1", 1), 5, 6, Some(7))
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
                tx.demand("anthropic")?,
            ))
        })
        .unwrap();
    assert!(first);
    assert!(!second, "a second release is a no-op");
    assert!(demand.is_empty(), "a released admission charges no demand");
}
