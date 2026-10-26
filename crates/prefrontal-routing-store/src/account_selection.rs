//! Persistence for account selection: served decisions,
//! conversation affinity, pin generations, admissions and the demand they
//! charge, short holds, runner observations, confirmed-exhaustion fences and
//! per-admission refusal exclusions. Specification:
//! `docs/designs/account-selection.md` ("Proposed protocol", "Affinity,
//! persistence and freeze semantics"); the schema is migration 44.
//!
//! The routing module runs one whole admission (read demand, affinity and
//! fences, decide, write the result) inside [`SqliteRouteStateStore::account_transaction`],
//! so concurrent admissions serialize and the reply is sent only after the
//! commit. Nothing here deletes a row: expiry is a comparison with the clock.
//! Old account history and runner observations are removed only by the
//! bounded retention batches in `retention.rs`.

use prefrontal_routing_core::{
    accounts::{FenceScope, PoolKey},
    RouteStateResult,
};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};

use super::{err, SqliteRouteStateStore};

/// `account_decision.mode` for a decision routing answered a caller with,
/// the only mode routing writes. (The schema also allows `shadow`, from the
/// retired shadow mode; retention purges those rows.)
pub const DECISION_MODE_SERVE: &str = "serve";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialProbeClaim {
    Claimed,
    Unfenced,
    Blocked,
}

#[cfg(test)]
mod account_fact_tests {
    use super::*;

    #[test]
    fn stale_credential_probe_is_claimed_once_with_a_strict_cutoff() {
        let scratch = prefrontal_test_support::ScratchDir::new("account-credential-probe");
        let store = std::sync::Arc::new(
            SqliteRouteStateStore::open(&crate::dev_descriptor(
                "account-credential-probe",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap(),
        );
        store
            .record_credential_entitlement_fence(
                &prefrontal_routing_core::route_state_store::CredentialRefusal {
                    provider_id: "p".into(),
                    credential_id: "c".into(),
                    code: "billing_exhausted".into(),
                },
                Some("listing"),
                100,
            )
            .unwrap();
        assert_eq!(
            store
                .account_transaction(|tx| tx.claim_credential_probe("p", "c", 100, 200))
                .unwrap(),
            CredentialProbeClaim::Blocked
        );
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let attempts: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .account_transaction(|tx| tx.claim_credential_probe("p", "c", 101, 200))
                        .unwrap()
                })
            })
            .collect();
        let claims: Vec<_> = attempts
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            claims
                .iter()
                .filter(|claim| **claim == CredentialProbeClaim::Claimed)
                .count(),
            1
        );
        assert_eq!(
            claims
                .iter()
                .filter(|claim| **claim == CredentialProbeClaim::Blocked)
                .count(),
            7
        );
        store
            .record_credential_entitlement_fence(
                &prefrontal_routing_core::route_state_store::CredentialRefusal {
                    provider_id: "p".into(),
                    credential_id: "c".into(),
                    code: "ENTITLEMENT_ERROR".into(),
                },
                Some("listing"),
                150,
            )
            .unwrap();
        assert_eq!(
            store.credential_entitlement_fences().unwrap()[0].observed_at_ms,
            200,
            "a late refusal cannot rewind a reserved probe"
        );
        assert_eq!(
            store
                .account_transaction(|tx| tx.claim_credential_probe("p", "c", 151, 201))
                .unwrap(),
            CredentialProbeClaim::Blocked
        );
        store.clear_credential_entitlement_fence("p", "c").unwrap();
        assert_eq!(
            store
                .account_transaction(|tx| tx.claim_credential_probe("p", "c", 101, 200))
                .unwrap(),
            CredentialProbeClaim::Unfenced
        );
    }

    #[test]
    fn eligibility_survives_restart_and_clears_only_on_listing_change() {
        let scratch = prefrontal_test_support::ScratchDir::new("account-eligibility-clear");
        let descriptor = crate::dev_descriptor(
            "account-eligibility-clear",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        store
            .record_account_eligibility(&AccountEligibilityFact {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                listing_fingerprint: Some("v1".into()),
                code: "404".into(),
                observed_at: 1,
            })
            .unwrap();
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        store
            .reconcile_account_eligibility(&[("c".into(), "v1".into())])
            .unwrap();
        assert_eq!(store.account_eligibility().unwrap().len(), 1);
        store
            .reconcile_account_eligibility(&[("c".into(), "v2".into())])
            .unwrap();
        assert!(store.account_eligibility().unwrap().is_empty());
        store
            .record_account_eligibility(&AccountEligibilityFact {
                provider: "p".into(),
                credential_id: "c".into(),
                model: String::new(),
                listing_fingerprint: None,
                code: "ineligible".into(),
                observed_at: 2,
            })
            .unwrap();
        store
            .reconcile_account_eligibility(&[("c".into(), "v2".into())])
            .unwrap();
        assert_eq!(
            store.account_eligibility().unwrap()[0]
                .listing_fingerprint
                .as_deref(),
            Some("v2")
        );
        store.reconcile_account_eligibility(&[]).unwrap();
        assert!(store.account_eligibility().unwrap().is_empty());
    }

    #[test]
    fn size_refusals_expire_or_clear_on_later_success_in_the_same_band() {
        let scratch = prefrontal_test_support::ScratchDir::new("account-size-clear");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "account-size-clear",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let fact = AccountSizeRefusal {
            provider: "p".into(),
            credential_id: "c".into(),
            model: "m".into(),
            size_class: 17,
            expires_at: 100,
            observed_at: 10,
        };
        store.record_account_size_refusal(&fact).unwrap();
        store
            .clear_account_size_refusal("p", "c", "m", 17, 10)
            .unwrap();
        store
            .clear_account_size_refusal("p", "c", "m", 12, 11)
            .unwrap();
        assert_eq!(store.account_size_refusals(11).unwrap().len(), 1);
        store
            .clear_account_size_refusal("p", "c", "m", 17, 12)
            .unwrap();
        assert!(store.account_size_refusals(12).unwrap().is_empty());
        store.record_account_size_refusal(&fact).unwrap();
        assert!(store.account_size_refusals(100).unwrap().is_empty());
        assert!(
            store.account_size_refusals(0).unwrap().is_empty(),
            "expiry deletes the row"
        );
    }
}

/// An account/model fact, valid only for the credential listing it was read on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountEligibilityFact {
    pub provider: String,
    pub credential_id: String,
    /// Empty means every model on the provider.
    pub model: String,
    pub listing_fingerprint: Option<String>,
    pub code: String,
    pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSizeRefusal {
    pub provider: String,
    pub credential_id: String,
    pub model: String,
    pub size_class: u32,
    pub expires_at: i64,
    pub observed_at: i64,
}

impl SqliteRouteStateStore {
    /// A run outcome is an audit fact, not quota or model-health evidence.
    pub fn record_routing_outcome(
        &self,
        decision: &str,
        outcome: &prefrontal_routing_core::RouteOutcome,
        error: Option<&str>,
        at: i64,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|conn: &Connection| {
            conn.execute("UPDATE model_route_decisions SET outcome=?1,outcome_at=?2,outcome_error=?3 WHERE id=?4",
                params![outcome.as_str(),at,error,decision]).map(|_| ())
        }).map_err(err)
    }

    pub fn record_account_eligibility(
        &self,
        fact: &AccountEligibilityFact,
    ) -> RouteStateResult<()> {
        self.account_transaction(|tx| tx.record_eligibility(fact))
    }
}

impl AccountTx<'_> {
    /// Reserve one stale credential probe in the same write transaction as
    /// its admission. The strict cutoff also rejects a concurrently renewed
    /// refusal or a probe already reserved from an older roster snapshot.
    pub fn claim_credential_probe(
        &self,
        provider: &str,
        credential: &str,
        before: i64,
        at: i64,
    ) -> RouteStateResult<CredentialProbeClaim> {
        let changed=self.conn.execute("UPDATE credential_entitlement_fences SET observed_at_ms=?4 WHERE provider_id=?1 AND credential_id=?2 AND observed_at_ms<?3",params![provider,credential,before,at]).map_err(err)?;
        if changed > 0 {
            return Ok(CredentialProbeClaim::Claimed);
        }
        let exists: bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM credential_entitlement_fences WHERE provider_id=?1 AND credential_id=?2)",params![provider,credential],|row|row.get(0)).map_err(err)?;
        Ok(if exists {
            CredentialProbeClaim::Blocked
        } else {
            CredentialProbeClaim::Unfenced
        })
    }

    /// Freeze the admitted serving identity on the model decision so a later
    /// completion report heals only the credential that actually ran.
    pub fn bind_model_account(
        &self,
        decision: &str,
        provider: &str,
        model: &str,
        account: &str,
        credential: &str,
    ) -> RouteStateResult<()> {
        let identity = serde_json::to_string(
            &prefrontal_routing_core::route_state_store::ProviderOutcome {
                account_ref: account.into(),
                credential_id: Some(credential.into()),
                resets_at_ms: None,
                credential_refusal: None,
            },
        )
        .map_err(err)?;
        self.conn.execute("UPDATE model_route_decisions SET provider_identity_json=?1 WHERE id=?2 AND selected_provider_id=?3 AND selected_model_id=?4",params![identity,decision,provider,model]).map(|_|()).map_err(err)
    }

    pub fn record_eligibility(&self, fact: &AccountEligibilityFact) -> RouteStateResult<()> {
        self.conn.execute(
                "INSERT INTO account_eligibility VALUES (?1,?2,?3,?4,?5,?6) \
                 ON CONFLICT (provider,credential_id,model) DO UPDATE SET \
                 listing_fingerprint=excluded.listing_fingerprint,code=excluded.code,observed_at=excluded.observed_at \
                 WHERE excluded.observed_at >= account_eligibility.observed_at",
                params![fact.provider, fact.credential_id, fact.model, fact.listing_fingerprint, fact.code, fact.observed_at],
            ).map(|_| ()).map_err(err)
    }

    /// Prefer a parent's account only after a runner reported using it. The
    /// reported serving account wins over a reservation the runner did not use.
    pub fn parent_used_account(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
    ) -> RouteStateResult<Option<String>> {
        self.conn.query_row("SELECT COALESCE(json_extract(o.observation_json,'$.served_account_id'),a.account_id) \
            FROM account_observation o JOIN account_admission a ON a.principal=o.principal AND a.admission_id=o.admission_id \
            WHERE a.principal=?1 AND a.conversation_hash=?2 AND a.provider=?3 \
              AND o.kind IN ('admission_started','run_finished','used_account','attempt') \
            ORDER BY o.received_at DESC,o.rowid DESC LIMIT 1", params![principal,hash,provider], |row| row.get(0)).optional().map_err(err)
    }
}

impl SqliteRouteStateStore {
    /// Reconcile facts only after a successful vault listing. Unknown listing
    /// state must not erase a previously established eligibility restriction.
    pub fn reconcile_account_eligibility(
        &self,
        listings: &[(String, String)],
    ) -> RouteStateResult<()> {
        self.store.with_conn(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_eligibility")? { return Ok(()); }
            let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
            let credentials = {
                let mut statement = tx.prepare("SELECT DISTINCT credential_id FROM account_eligibility")?;
                let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for credential in credentials {
                match listings.iter().find(|(id, _)| id == &credential) {
                    Some((_, fingerprint)) => {
                        tx.execute("DELETE FROM account_eligibility WHERE credential_id=?1 AND listing_fingerprint IS NOT NULL AND listing_fingerprint<>?2", params![credential, fingerprint])?;
                        tx.execute("UPDATE account_eligibility SET listing_fingerprint=?2 WHERE credential_id=?1 AND listing_fingerprint IS NULL", params![credential, fingerprint])?;
                    }
                    None => { tx.execute("DELETE FROM account_eligibility WHERE credential_id=?1", [&credential])?; }
                }
            }
            tx.commit()
        }).map_err(err)
    }

    pub fn account_eligibility(&self) -> RouteStateResult<Vec<AccountEligibilityFact>> {
        self.store.with_conn(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_eligibility")? { return Ok(Vec::new()); }
            let mut statement = conn.prepare("SELECT provider,credential_id,model,listing_fingerprint,code,observed_at FROM account_eligibility ORDER BY provider,credential_id,model")?;
            let rows = statement.query_map([], |row| Ok(AccountEligibilityFact {
                provider: row.get(0)?, credential_id: row.get(1)?, model: row.get(2)?,
                listing_fingerprint: row.get(3)?, code: row.get(4)?, observed_at: row.get(5)?,
            }))?;
            rows.collect()
        }).map_err(err)
    }

    pub fn record_account_size_refusal(&self, fact: &AccountSizeRefusal) -> RouteStateResult<()> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "INSERT INTO account_size_refusal VALUES (?1,?2,?3,?4,?5,?6) \
                ON CONFLICT (provider,credential_id,model,size_class) DO UPDATE SET \
                expires_at=excluded.expires_at,observed_at=excluded.observed_at \
                WHERE excluded.observed_at >= account_size_refusal.observed_at",
                    params![
                        fact.provider,
                        fact.credential_id,
                        fact.model,
                        fact.size_class,
                        fact.expires_at,
                        fact.observed_at
                    ],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Expired refusals are removed on reads, not just ignored in memory.
    pub fn account_size_refusals(&self, now: i64) -> RouteStateResult<Vec<AccountSizeRefusal>> {
        self.store.with_conn(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_size_refusal")? { return Ok(Vec::new()); }
            conn.execute("DELETE FROM account_size_refusal WHERE expires_at<=?1", [now])?;
            let mut statement = conn.prepare("SELECT provider,credential_id,model,size_class,expires_at,observed_at FROM account_size_refusal")?;
            let rows = statement.query_map([], |row| Ok(AccountSizeRefusal {
                provider: row.get(0)?,credential_id: row.get(1)?,model: row.get(2)?,size_class: row.get(3)?,expires_at: row.get(4)?,observed_at: row.get(5)?,
            }))?;
            rows.collect()
        }).map_err(err)
    }

    pub fn clear_account_size_refusal(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
        size_class: u32,
        observed_at: i64,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|conn: &Connection| {
            conn.execute("DELETE FROM account_size_refusal WHERE provider=?1 AND credential_id=?2 AND model=?3 AND size_class=?4 AND observed_at<?5",
                params![provider,credential,model,size_class,observed_at]).map(|_| ())
        }).map_err(err)
    }
}

// Upgrade fixtures issue admissions on the old schema before reopening it with
// migrations enabled. Absence of the new fact tables means no such facts exist.
fn account_fact_table_exists(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )
}

/// One recorded account decision. `decision_json` is the allocator's whole
/// decision (outcome and explanation); `context_json` is what the caller of
/// the allocator wants kept beside it (the trigger, roster gaps, a triggering
/// observation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDecisionRecord {
    pub decision_id: String,
    pub mode: String,
    pub trigger: String,
    pub principal: String,
    pub admission_id: Option<String>,
    pub request_mode: Option<String>,
    pub provider: String,
    pub model: String,
    pub family: String,
    /// `admit`, `hold` or `refuse`.
    pub outcome: String,
    /// The admit reason code, or the hold or refusal code.
    pub code: String,
    pub account_id: Option<String>,
    pub credential_id: Option<String>,
    pub policy_version: u32,
    pub decision_json: String,
    pub context_json: Option<String>,
    pub created_at: i64,
}

/// A conversation's current pin, keyed by principal, hashed conversation key
/// and provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAffinityRow {
    pub principal: String,
    pub conversation_hash: String,
    pub provider: String,
    pub affinity_model: String,
    pub pin_id: String,
    pub pin_generation: u64,
    pub account_id: String,
    pub credential_id: String,
    pub credential_binding_revision: u64,
    pub policy_version: u32,
    pub initial_demand_bytes: u64,
    pub created_at: i64,
    pub last_used_at: i64,
    pub expires_at: i64,
    pub lease_valid_until: Option<i64>,
}

/// One issued pin generation. Immutable once written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPinRow {
    pub pin_id: String,
    pub principal: String,
    pub conversation_hash: String,
    pub provider: String,
    pub pin_generation: u64,
    pub affinity_model: String,
    pub account_id: String,
    pub credential_id: String,
    /// `run_frozen` or `conversation_lease`.
    pub pin_mode: String,
    pub previous_pin_id: Option<String>,
    pub decision_id: String,
    pub created_at: i64,
}

/// One admission and the demand it charged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAdmissionRow {
    pub principal: String,
    pub admission_id: String,
    pub request_fingerprint: String,
    /// `admit`, `renew` or `reassign`.
    pub request_mode: String,
    pub decision_id: String,
    pub conversation_hash: String,
    pub provider: String,
    pub model: String,
    pub account_id: String,
    pub credential_id: String,
    pub pin_id: String,
    pub pin_generation: u64,
    /// `reserved`, `started`, `released`, `expired` or `finished`.
    pub state: String,
    /// False for a renewal, which charges no demand.
    pub charged: bool,
    pub request_bytes: u64,
    pub request_bytes_estimated: bool,
    pub committed_at: i64,
    pub admission_valid_until: i64,
    pub lease_valid_until: i64,
    /// The reply routing sent, replayed verbatim to a duplicate request.
    pub reply_json: String,
    pub run_id: Option<String>,
    pub started_at: Option<i64>,
    pub started_late: bool,
    pub released_at: Option<i64>,
    pub finished_at: Option<i64>,
}

/// The demand one charged admission contributes to its account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDemandRow {
    pub account_id: String,
    pub state: String,
    pub committed_at: i64,
    pub request_bytes: u64,
}

/// One short hold routing answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountHoldRow {
    pub decision_id: String,
    pub principal: String,
    pub conversation_hash: String,
    pub provider: String,
    pub pin_id: String,
    pub pin_generation: u64,
    pub account_id: String,
    pub credential_id: String,
    pub basis: String,
    pub reset_at: i64,
    pub retry_after_s: u64,
    pub created_at: i64,
}

/// One runner observation as received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountObservationRow {
    pub observation_id: String,
    pub principal: String,
    pub runner: String,
    pub event_id: String,
    pub kind: String,
    pub admission_id: Option<String>,
    pub bound: bool,
    pub observation_json: String,
    pub effects_json: String,
    pub received_at: i64,
}

/// One confirmed-exhaustion fence. `fence_json` is the allocator's `Fence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountFenceRow {
    pub fence_id: String,
    pub provider: String,
    pub account_id: String,
    pub scope_json: String,
    pub fence_json: String,
    pub source: String,
    pub observed_at: i64,
    pub principal: String,
    pub admission_id: Option<String>,
    pub observation_id: Option<String>,
    pub created_at: i64,
    pub cleared_at: Option<i64>,
    pub cleared_reason: Option<String>,
}

/// Pool identity and whole-account recovery requirements alongside the original
/// exhaustion evidence. A missing key blocks the entire account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPoolFenceRow {
    pub evidence: AccountFenceRow,
    pub pool_key: Option<PoolKey>,
    pub recovery_set: Vec<PoolKey>,
}

/// A runner's scoped not-found for one credential in one admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountExclusionRow {
    pub principal: String,
    pub admission_id: String,
    pub credential_id: String,
    pub runner_view_digest: String,
    pub roster_view_digest: String,
    pub observation_id: Option<String>,
    pub recorded_at: i64,
}

/// Row counts of the tables that serving writes (everything but the decision
/// log), for diagnostics and for proving a code path wrote none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg(any(test, feature = "test-support"))]
pub struct AccountServingCounts {
    pub affinity: i64,
    pub pins: i64,
    pub admissions: i64,
    pub holds: i64,
    pub observations: i64,
    pub fences: i64,
    pub exclusions: i64,
}

#[cfg(any(test, feature = "test-support"))]
impl AccountServingCounts {
    pub fn total(&self) -> i64 {
        self.affinity
            + self.pins
            + self.admissions
            + self.holds
            + self.observations
            + self.fences
            + self.exclusions
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

const DECISION_COLUMNS: &str =
    "decision_id, mode, trigger, principal, admission_id, request_mode, \
     provider, model, family, outcome, code, account_id, credential_id, policy_version, \
     decision_json, context_json, created_at";

fn decision_from_row(row: &Row<'_>) -> rusqlite::Result<AccountDecisionRecord> {
    Ok(AccountDecisionRecord {
        decision_id: row.get(0)?,
        mode: row.get(1)?,
        trigger: row.get(2)?,
        principal: row.get(3)?,
        admission_id: row.get(4)?,
        request_mode: row.get(5)?,
        provider: row.get(6)?,
        model: row.get(7)?,
        family: row.get(8)?,
        outcome: row.get(9)?,
        code: row.get(10)?,
        account_id: row.get(11)?,
        credential_id: row.get(12)?,
        policy_version: u32::try_from(row.get::<_, i64>(13)?).unwrap_or(0),
        decision_json: row.get(14)?,
        context_json: row.get(15)?,
        created_at: row.get(16)?,
    })
}

const AFFINITY_COLUMNS: &str = "principal, conversation_hash, provider, affinity_model, pin_id, \
     pin_generation, account_id, credential_id, credential_binding_revision, policy_version, \
     initial_demand_bytes, created_at, last_used_at, expires_at, lease_valid_until";

fn affinity_from_row(row: &Row<'_>) -> rusqlite::Result<AccountAffinityRow> {
    Ok(AccountAffinityRow {
        principal: row.get(0)?,
        conversation_hash: row.get(1)?,
        provider: row.get(2)?,
        affinity_model: row.get(3)?,
        pin_id: row.get(4)?,
        pin_generation: to_u64(row.get(5)?),
        account_id: row.get(6)?,
        credential_id: row.get(7)?,
        credential_binding_revision: to_u64(row.get(8)?),
        policy_version: u32::try_from(row.get::<_, i64>(9)?).unwrap_or(0),
        initial_demand_bytes: to_u64(row.get(10)?),
        created_at: row.get(11)?,
        last_used_at: row.get(12)?,
        expires_at: row.get(13)?,
        lease_valid_until: row.get(14)?,
    })
}

const ADMISSION_COLUMNS: &str = "principal, admission_id, request_fingerprint, request_mode, \
     decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id, \
     pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at, \
     admission_valid_until, lease_valid_until, reply_json, run_id, started_at, started_late, \
     released_at, finished_at";

fn admission_from_row(row: &Row<'_>) -> rusqlite::Result<AccountAdmissionRow> {
    Ok(AccountAdmissionRow {
        principal: row.get(0)?,
        admission_id: row.get(1)?,
        request_fingerprint: row.get(2)?,
        request_mode: row.get(3)?,
        decision_id: row.get(4)?,
        conversation_hash: row.get(5)?,
        provider: row.get(6)?,
        model: row.get(7)?,
        account_id: row.get(8)?,
        credential_id: row.get(9)?,
        pin_id: row.get(10)?,
        pin_generation: to_u64(row.get(11)?),
        state: row.get(12)?,
        charged: row.get::<_, i64>(13)? != 0,
        request_bytes: to_u64(row.get(14)?),
        request_bytes_estimated: row.get::<_, i64>(15)? != 0,
        committed_at: row.get(16)?,
        admission_valid_until: row.get(17)?,
        lease_valid_until: row.get(18)?,
        reply_json: row.get(19)?,
        run_id: row.get(20)?,
        started_at: row.get(21)?,
        started_late: row.get::<_, i64>(22)? != 0,
        released_at: row.get(23)?,
        finished_at: row.get(24)?,
    })
}

const FENCE_COLUMNS: &str = "fence_id, provider, account_id, scope_json, fence_json, source, \
     observed_at, principal, admission_id, observation_id, created_at, cleared_at, cleared_reason";

fn fence_from_row(row: &Row<'_>) -> rusqlite::Result<AccountFenceRow> {
    Ok(AccountFenceRow {
        fence_id: row.get(0)?,
        provider: row.get(1)?,
        account_id: row.get(2)?,
        scope_json: row.get(3)?,
        fence_json: row.get(4)?,
        source: row.get(5)?,
        observed_at: row.get(6)?,
        principal: row.get(7)?,
        admission_id: row.get(8)?,
        observation_id: row.get(9)?,
        created_at: row.get(10)?,
        cleared_at: row.get(11)?,
        cleared_reason: row.get(12)?,
    })
}

const OBSERVATION_COLUMNS: &str =
    "observation_id, principal, runner, event_id, kind, admission_id, \
     bound, observation_json, effects_json, received_at";

fn observation_from_row(row: &Row<'_>) -> rusqlite::Result<AccountObservationRow> {
    Ok(AccountObservationRow {
        observation_id: row.get(0)?,
        principal: row.get(1)?,
        runner: row.get(2)?,
        event_id: row.get(3)?,
        kind: row.get(4)?,
        admission_id: row.get(5)?,
        bound: row.get::<_, i64>(6)? != 0,
        observation_json: row.get(7)?,
        effects_json: row.get(8)?,
        received_at: row.get(9)?,
    })
}

pub const ACCOUNT_DEMAND_SQL: &str =
    "SELECT account_id, state, committed_at, request_bytes FROM account_admission \
     WHERE provider = ?1 AND charged = 1 AND state != 'released' \
     ORDER BY committed_at, principal, admission_id";

/// The account-selection tables inside one open write transaction. Every
/// method reads or writes through the same transaction, so a whole admission
/// commits or rolls back as one.
pub struct AccountTx<'a> {
    conn: &'a Connection,
}

impl AccountTx<'_> {
    /// The installation-local salt for hashing conversation keys, created with
    /// `fresh` the first time it is needed and never changed afterwards.
    pub fn salt(&self, now: i64, fresh: impl FnOnce() -> String) -> RouteStateResult<String> {
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT salt FROM account_selection_salt WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        if let Some(salt) = existing {
            return Ok(salt);
        }
        let salt = fresh();
        self.conn
            .execute(
                "INSERT INTO account_selection_salt (singleton, salt, created_at) VALUES (1, ?1, ?2)",
                params![salt, now],
            )
            .map_err(err)?;
        Ok(salt)
    }

    pub fn insert_decision(&self, row: &AccountDecisionRecord) -> RouteStateResult<()> {
        self.conn
            .execute(
                &format!(
                    "INSERT INTO account_decision ({DECISION_COLUMNS}) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)"
                ),
                params![
                    row.decision_id,
                    row.mode,
                    row.trigger,
                    row.principal,
                    row.admission_id,
                    row.request_mode,
                    row.provider,
                    row.model,
                    row.family,
                    row.outcome,
                    row.code,
                    row.account_id,
                    row.credential_id,
                    i64::from(row.policy_version),
                    row.decision_json,
                    row.context_json,
                    row.created_at,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }

    pub fn admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        self.conn
            .query_row(
                &format!(
                    "SELECT {ADMISSION_COLUMNS} FROM account_admission \
                     WHERE principal = ?1 AND admission_id = ?2"
                ),
                params![principal, admission_id],
                admission_from_row,
            )
            .optional()
            .map_err(err)
    }

    pub fn insert_admission(&self, row: &AccountAdmissionRow) -> RouteStateResult<()> {
        self.conn
            .execute(
                &format!(
                    "INSERT INTO account_admission ({ADMISSION_COLUMNS}) VALUES \
                     (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
                      ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)"
                ),
                params![
                    row.principal,
                    row.admission_id,
                    row.request_fingerprint,
                    row.request_mode,
                    row.decision_id,
                    row.conversation_hash,
                    row.provider,
                    row.model,
                    row.account_id,
                    row.credential_id,
                    row.pin_id,
                    to_i64(row.pin_generation),
                    row.state,
                    i64::from(row.charged),
                    to_i64(row.request_bytes),
                    i64::from(row.request_bytes_estimated),
                    row.committed_at,
                    row.admission_valid_until,
                    row.lease_valid_until,
                    row.reply_json,
                    row.run_id,
                    row.started_at,
                    i64::from(row.started_late),
                    row.released_at,
                    row.finished_at,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// A reservation nobody started within its window can no longer start.
    /// Returns whether the row changed.
    pub fn mark_admission_expired(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_admission SET state = 'expired' \
                 WHERE principal = ?1 AND admission_id = ?2 AND state = 'reserved'",
                params![principal, admission_id],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// The runner started the admitted run. A start after the reservation
    /// expired is still accepted, and marked late, because the run's freeze
    /// wins. Returns whether the row changed.
    pub fn mark_admission_started(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        at: i64,
        late: bool,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_admission SET state = 'started', run_id = ?3, started_at = ?4, \
                 started_late = ?5 \
                 WHERE principal = ?1 AND admission_id = ?2 AND state IN ('reserved', 'expired')",
                params![principal, admission_id, run_id, at, i64::from(late)],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// Release an admission that never started, removing its demand. A second
    /// release, or a release of a started run, changes nothing.
    pub fn mark_admission_released(
        &self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_admission SET state = 'released', released_at = ?3 \
                 WHERE principal = ?1 AND admission_id = ?2 AND state IN ('reserved', 'expired')",
                params![principal, admission_id, at],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// The runner reported the run's end. Its demand keeps counting until a
    /// quota reading reflects it; affinity is untouched.
    pub fn mark_admission_finished(
        &self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_admission SET state = 'finished', finished_at = ?3 \
                 WHERE principal = ?1 AND admission_id = ?2 \
                   AND state IN ('reserved', 'expired', 'started')",
                params![principal, admission_id, at],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// Charged demand of every admission for `provider` that is not released.
    pub fn demand(&self, provider: &str) -> RouteStateResult<Vec<AccountDemandRow>> {
        let mut statement = self.conn.prepare(ACCOUNT_DEMAND_SQL).map_err(err)?;
        let rows = statement
            .query_map(params![provider], |row| {
                Ok(AccountDemandRow {
                    account_id: row.get(0)?,
                    state: row.get(1)?,
                    committed_at: row.get(2)?,
                    request_bytes: to_u64(row.get(3)?),
                })
            })
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    pub fn affinity(
        &self,
        principal: &str,
        conversation_hash: &str,
        provider: &str,
    ) -> RouteStateResult<Option<AccountAffinityRow>> {
        self.conn
            .query_row(
                &format!(
                    "SELECT {AFFINITY_COLUMNS} FROM account_affinity \
                     WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3"
                ),
                params![principal, conversation_hash, provider],
                affinity_from_row,
            )
            .optional()
            .map_err(err)
    }

    /// Install the conversation's pin. With `expected` set, the write is a
    /// compare-and-set: it applies only while the stored pin is still that pin
    /// id and generation. With `expected` unset it applies only when the
    /// conversation has no row yet. Returns whether it applied.
    pub fn put_affinity(
        &self,
        row: &AccountAffinityRow,
        expected: Option<(&str, u64)>,
    ) -> RouteStateResult<bool> {
        let changed = match expected {
            None => self.conn.execute(
                &format!(
                    "INSERT INTO account_affinity ({AFFINITY_COLUMNS}) VALUES \
                     (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) \
                     ON CONFLICT (principal, conversation_hash, provider) DO NOTHING"
                ),
                params![
                    row.principal,
                    row.conversation_hash,
                    row.provider,
                    row.affinity_model,
                    row.pin_id,
                    to_i64(row.pin_generation),
                    row.account_id,
                    row.credential_id,
                    to_i64(row.credential_binding_revision),
                    i64::from(row.policy_version),
                    to_i64(row.initial_demand_bytes),
                    row.created_at,
                    row.last_used_at,
                    row.expires_at,
                    row.lease_valid_until,
                ],
            ),
            Some((expected_pin, expected_generation)) => self.conn.execute(
                "UPDATE account_affinity SET affinity_model = ?4, pin_id = ?5, pin_generation = ?6, \
                   account_id = ?7, credential_id = ?8, credential_binding_revision = ?9, \
                   policy_version = ?10, initial_demand_bytes = ?11, created_at = ?12, \
                   last_used_at = ?13, expires_at = ?14, lease_valid_until = ?15 \
                 WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3 \
                   AND pin_id = ?16 AND pin_generation = ?17",
                params![
                    row.principal,
                    row.conversation_hash,
                    row.provider,
                    row.affinity_model,
                    row.pin_id,
                    to_i64(row.pin_generation),
                    row.account_id,
                    row.credential_id,
                    to_i64(row.credential_binding_revision),
                    i64::from(row.policy_version),
                    to_i64(row.initial_demand_bytes),
                    row.created_at,
                    row.last_used_at,
                    row.expires_at,
                    row.lease_valid_until,
                    expected_pin,
                    to_i64(expected_generation),
                ],
            ),
        }
        .map_err(err)?;
        Ok(changed == 1)
    }

    /// Record accepted use of the current pin: move the idle expiry and, for a
    /// lease, its end. Compare-and-set on the pin like [`Self::put_affinity`].
    #[allow(clippy::too_many_arguments)]
    pub fn touch_affinity(
        &self,
        principal: &str,
        conversation_hash: &str,
        provider: &str,
        expected: (&str, u64),
        last_used_at: i64,
        expires_at: i64,
        lease_valid_until: Option<i64>,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_affinity SET last_used_at = ?6, expires_at = ?7, \
                   lease_valid_until = COALESCE(?8, lease_valid_until) \
                 WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3 \
                   AND pin_id = ?4 AND pin_generation = ?5",
                params![
                    principal,
                    conversation_hash,
                    provider,
                    expected.0,
                    to_i64(expected.1),
                    last_used_at,
                    expires_at,
                    lease_valid_until,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// End the lease of the pin `pin_id` names, if it is still the
    /// conversation's current pin. Affinity itself stays.
    pub fn end_lease(&self, pin_id: &str, at: i64) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_affinity SET lease_valid_until = ?2 \
                 WHERE pin_id = ?1 AND (lease_valid_until IS NULL OR lease_valid_until > ?2)",
                params![pin_id, at],
            )
            .map(|changed| changed >= 1)
            .map_err(err)
    }

    pub fn insert_pin(&self, row: &AccountPinRow) -> RouteStateResult<()> {
        self.conn
            .execute(
                "INSERT INTO account_pin (pin_id, principal, conversation_hash, provider, \
                   pin_generation, affinity_model, account_id, credential_id, pin_mode, \
                   previous_pin_id, decision_id, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    row.pin_id,
                    row.principal,
                    row.conversation_hash,
                    row.provider,
                    to_i64(row.pin_generation),
                    row.affinity_model,
                    row.account_id,
                    row.credential_id,
                    row.pin_mode,
                    row.previous_pin_id,
                    row.decision_id,
                    row.created_at,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }

    pub fn insert_hold(&self, row: &AccountHoldRow) -> RouteStateResult<()> {
        self.conn
            .execute(
                "INSERT INTO account_hold (decision_id, principal, conversation_hash, provider, \
                   pin_id, pin_generation, account_id, credential_id, basis, reset_at, \
                   retry_after_s, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    row.decision_id,
                    row.principal,
                    row.conversation_hash,
                    row.provider,
                    row.pin_id,
                    to_i64(row.pin_generation),
                    row.account_id,
                    row.credential_id,
                    row.basis,
                    row.reset_at,
                    to_i64(row.retry_after_s),
                    row.created_at,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// Every fence on `provider` not yet cleared.
    pub fn active_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountFenceRow>> {
        let mut statement = self
            .conn
            .prepare(&format!(
                "SELECT {FENCE_COLUMNS} FROM account_fence \
                 WHERE provider = ?1 AND cleared_at IS NULL ORDER BY created_at, fence_id"
            ))
            .map_err(err)?;
        let rows = statement
            .query_map(params![provider], fence_from_row)
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    pub fn insert_fence(&self, row: &AccountFenceRow) -> RouteStateResult<()> {
        let pool_key = serde_json::from_str::<FenceScope>(&row.scope_json)
            .ok()
            .map(PoolKey::from);
        self.insert_pool_fence(&AccountPoolFenceRow {
            evidence: row.clone(),
            pool_key,
            recovery_set: Vec::new(),
        })
    }

    /// Store the recovery set in the same transaction as the confirming evidence.
    pub fn insert_pool_fence(&self, block: &AccountPoolFenceRow) -> RouteStateResult<()> {
        let row = &block.evidence;
        let pool_key = block
            .pool_key
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(err)?;
        let recovery_set = serde_json::to_string(&block.recovery_set).map_err(err)?;
        self.conn
            .execute(
                &format!(
                    "INSERT INTO account_fence ({FENCE_COLUMNS}, pool_key_json, recovery_set_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
                ),
                params![
                    row.fence_id,
                    row.provider,
                    row.account_id,
                    row.scope_json,
                    row.fence_json,
                    row.source,
                    row.observed_at,
                    row.principal,
                    row.admission_id,
                    row.observation_id,
                    row.created_at,
                    row.cleared_at,
                    row.cleared_reason,
                    pool_key,
                    recovery_set,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }

    pub fn active_pool_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountPoolFenceRow>> {
        let mut statement = self
            .conn
            .prepare(&format!(
                "SELECT {FENCE_COLUMNS}, pool_key_json, recovery_set_json FROM account_fence
             WHERE provider=?1 AND cleared_at IS NULL ORDER BY created_at, fence_id"
            ))
            .map_err(err)?;
        let rows = statement
            .query_map([provider], |row| {
                Ok((
                    fence_from_row(row)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, String>(14)?,
                ))
            })
            .map_err(err)?;
        rows.map(|row| {
            let (evidence, key, recovery) = row.map_err(err)?;
            Ok(AccountPoolFenceRow {
                evidence,
                pool_key: key
                    .map(|key| serde_json::from_str(&key))
                    .transpose()
                    .map_err(err)?,
                recovery_set: serde_json::from_str(&recovery).map_err(err)?,
            })
        })
        .collect()
    }

    /// Mark a standing block ended, recording when and why. Returns whether
    /// the block was still standing.
    pub fn clear_fence(&self, fence_id: &str, at: i64, reason: &str) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_fence SET cleared_at = ?2, cleared_reason = ?3 \
                 WHERE fence_id = ?1 AND cleared_at IS NULL",
                params![fence_id, at, reason],
            )
            .map(|changed| changed > 0)
            .map_err(err)
    }

    /// Replace a standing whole-account block's recovery set, which only
    /// grows as later readings report more pools.
    pub fn set_fence_recovery_set(
        &self,
        fence_id: &str,
        recovery_set: &[PoolKey],
    ) -> RouteStateResult<()> {
        let recovery_set = serde_json::to_string(recovery_set).map_err(err)?;
        self.conn
            .execute(
                "UPDATE account_fence SET recovery_set_json = ?2 \
                 WHERE fence_id = ?1 AND cleared_at IS NULL",
                params![fence_id, recovery_set],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// Every provider with at least one standing block.
    pub fn active_fence_providers(&self) -> RouteStateResult<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT DISTINCT provider FROM account_fence WHERE cleared_at IS NULL \
                 ORDER BY provider",
            )
            .map_err(err)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    /// The scoped not-found exclusions reported against this one admission.
    pub fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Vec<AccountExclusionRow>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT principal, admission_id, credential_id, runner_view_digest, \
                   roster_view_digest, observation_id, recorded_at \
                 FROM account_principal_exclusion WHERE principal = ?1 AND admission_id = ?2 \
                 ORDER BY credential_id",
            )
            .map_err(err)?;
        let rows = statement
            .query_map(params![principal, admission_id], |row| {
                Ok(AccountExclusionRow {
                    principal: row.get(0)?,
                    admission_id: row.get(1)?,
                    credential_id: row.get(2)?,
                    runner_view_digest: row.get(3)?,
                    roster_view_digest: row.get(4)?,
                    observation_id: row.get(5)?,
                    recorded_at: row.get(6)?,
                })
            })
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    /// Record a scoped not-found. Returns false when this admission already
    /// excluded the credential.
    pub fn insert_exclusion(&self, row: &AccountExclusionRow) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "INSERT INTO account_principal_exclusion (principal, admission_id, credential_id, \
                   runner_view_digest, roster_view_digest, observation_id, recorded_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (principal, admission_id, credential_id) DO NOTHING",
                params![
                    row.principal,
                    row.admission_id,
                    row.credential_id,
                    row.runner_view_digest,
                    row.roster_view_digest,
                    row.observation_id,
                    row.recorded_at,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// How many admissions of `principal` reported a scoped not-found for
    /// `credential_id`: more than one for a credential the vault still lists
    /// is the repeated-refusal alarm.
    pub fn exclusion_count(&self, principal: &str, credential_id: &str) -> RouteStateResult<i64> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM account_principal_exclusion \
                 WHERE principal = ?1 AND credential_id = ?2",
                params![principal, credential_id],
                |row| row.get(0),
            )
            .map_err(err)
    }

    /// The first report of a runner event, if routing already applied it.
    ///
    /// The observation log holds the verbatim report, but a backup may restore
    /// that log empty. `account_observation_event` is the small backed-up
    /// record of recent event keys with the answer routing gave (observation
    /// id, binding and effects), so a replayed event is still recognised then.
    /// A row rebuilt from it has an empty `kind` and `observation_json` and no
    /// `admission_id`: the duplicate answer does not carry them.
    pub fn observation_by_event(
        &self,
        principal: &str,
        runner: &str,
        event_id: &str,
    ) -> RouteStateResult<Option<AccountObservationRow>> {
        let logged = self
            .conn
            .query_row(
                &format!(
                    "SELECT {OBSERVATION_COLUMNS} FROM account_observation \
                     WHERE principal = ?1 AND runner = ?2 AND event_id = ?3"
                ),
                params![principal, runner, event_id],
                observation_from_row,
            )
            .optional()
            .map_err(err)?;
        if logged.is_some() {
            return Ok(logged);
        }
        self.conn
            .query_row(
                "SELECT observation_id, bound, effects_json, received_at \
                 FROM account_observation_event \
                 WHERE principal = ?1 AND runner = ?2 AND event_id = ?3",
                params![principal, runner, event_id],
                |row| {
                    Ok(AccountObservationRow {
                        observation_id: row.get(0)?,
                        principal: principal.to_string(),
                        runner: runner.to_string(),
                        event_id: event_id.to_string(),
                        kind: String::new(),
                        admission_id: None,
                        bound: row.get::<_, i64>(1)? != 0,
                        observation_json: String::new(),
                        effects_json: row.get(2)?,
                        received_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(err)
    }

    /// Record what an observation changed, once its effects are known.
    pub fn set_observation_effects(
        &self,
        observation_id: &str,
        bound: bool,
        effects_json: &str,
    ) -> RouteStateResult<()> {
        self.conn
            .execute(
                "UPDATE account_observation SET bound = ?2, effects_json = ?3 \
                 WHERE observation_id = ?1",
                params![observation_id, i64::from(bound), effects_json],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// The salt if one was ever created. A read-only caller uses this: with no
    /// salt no conversation can have affinity yet.
    pub fn existing_salt(&self) -> RouteStateResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT salt FROM account_selection_salt WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)
    }

    pub fn insert_observation(&self, row: &AccountObservationRow) -> RouteStateResult<()> {
        self.conn
            .execute(
                &format!(
                    "INSERT INTO account_observation ({OBSERVATION_COLUMNS}) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
                ),
                params![
                    row.observation_id,
                    row.principal,
                    row.runner,
                    row.event_id,
                    row.kind,
                    row.admission_id,
                    i64::from(row.bound),
                    row.observation_json,
                    row.effects_json,
                    row.received_at,
                ],
            )
            .map(|_| ())
            .map_err(err)
    }
}

impl SqliteRouteStateStore {
    /// Run `f` inside one IMMEDIATE write transaction over the account
    /// tables. `Ok` commits before this returns, so a reply built from the
    /// result is sent only once the decision is durable; `Err` rolls every
    /// write back.
    pub fn account_transaction<T>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        self.store
            .with_conn(|conn: &Connection| {
                let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
                let result = f(&AccountTx { conn: &tx });
                if result.is_ok() {
                    tx.commit()?;
                }
                Ok(result)
            })
            .map_err(err)?
    }

    /// The newest decisions recorded in `mode`, newest first.
    pub fn account_decisions(
        &self,
        mode: &str,
        limit: usize,
    ) -> RouteStateResult<Vec<AccountDecisionRecord>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(&format!(
                    "SELECT {DECISION_COLUMNS} FROM account_decision WHERE mode = ?1 \
                     ORDER BY created_at DESC, rowid DESC LIMIT ?2"
                ))?;
                let rows = statement.query_map(
                    params![mode, i64::try_from(limit).unwrap_or(i64::MAX)],
                    decision_from_row,
                )?;
                rows.collect()
            })
            .map_err(err)
    }

    /// One recorded decision by id.
    pub fn account_decision(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<AccountDecisionRecord>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    &format!(
                        "SELECT {DECISION_COLUMNS} FROM account_decision WHERE decision_id = ?1"
                    ),
                    params![decision_id],
                    decision_from_row,
                )
                .optional()
            })
            .map_err(err)
    }

    /// Row counts of the tables serving writes.
    #[cfg(any(test, feature = "test-support"))]
    pub fn account_serving_counts(&self) -> RouteStateResult<AccountServingCounts> {
        self.store
            .with_conn(|c: &Connection| {
                let count = |table: &str| -> rusqlite::Result<i64> {
                    c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                };
                Ok(AccountServingCounts {
                    affinity: count("account_affinity")?,
                    pins: count("account_pin")?,
                    admissions: count("account_admission")?,
                    holds: count("account_hold")?,
                    observations: count("account_observation")?,
                    fences: count("account_fence")?,
                    exclusions: count("account_principal_exclusion")?,
                })
            })
            .map_err(err)
    }
}
