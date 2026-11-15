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

use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use striatum_core::{
    accounts::{FenceScope, PoolKey},
    RouteStateResult,
};

use super::{err, SqliteRouteStateStore};

/// `account_decision.mode` for a decision routing answered a caller with,
/// the only mode routing writes. (The schema also allows `shadow`, from the
/// retired shadow mode; retention purges those rows.)
pub const DECISION_MODE_SERVE: &str = "serve";

const END_LEASE_SQL: &str = "UPDATE account_affinity SET lease_valid_until = ?2 \
    WHERE pin_id = ?1 AND (lease_valid_until IS NULL OR lease_valid_until > ?2)";

/// Read account-wide observation backoffs with `(now_ms, oldest_receipt_ms)`.
/// The bound-kind predicates match migration 79's sparse partial index.
pub const OBSERVATION_BACKOFFS_SQL: &str =
    "SELECT json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id'),\
     MAX(MIN(json_extract(e.value,'$.retry_at_ms'),o.received_at+3600000)) \
     FROM account_observation o,json_each(CASE WHEN o.kind='transient_backoff' AND o.bound=1 \
     AND json_valid(o.effects_json) THEN o.effects_json ELSE '[]' END) e \
     WHERE o.bound=1 AND o.kind='transient_backoff' AND o.received_at>?2 \
     AND json_extract(e.value,'$.effect')='transient_backoff' \
     AND MIN(json_extract(e.value,'$.retry_at_ms'),o.received_at+3600000)>?1 \
     GROUP BY json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id')";

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
    fn admission_column_count_matches_the_column_list() {
        assert_eq!(ADMISSION_COLUMNS.split(',').count(), ADMISSION_COLUMN_COUNT);
    }

    #[test]
    fn end_lease_query_plan_uses_affinity_pin_index() {
        let scratch = cortexkit_test_support::ScratchDir::new("end-lease-query-plan");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "end-lease-query-plan",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .with_writer_for_test(|conn| {
                let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {END_LEASE_SQL}"))?;
                let plan = statement
                    .query_map(params!["pin", 200], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(
                    plan.iter().any(|line| line.contains(
                        "SEARCH account_affinity USING INDEX account_affinity_by_pin (pin_id=?)"
                    )),
                    "{plan:?}"
                );
                assert!(
                    !plan
                        .iter()
                        .any(|line| line.contains("SCAN account_affinity")),
                    "{plan:?}"
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn headroom_clear_preserves_a_newer_refusal_and_other_provider() {
        let scratch = cortexkit_test_support::ScratchDir::new("headroom-evidence-order");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "headroom-evidence-order",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let fence = |provider: &str, model: &str, at| AccountRouteFence {
            provider: provider.into(),
            credential_id: "c".into(),
            model: model.into(),
            recorded_at: at,
            until_ms: 10_000,
            decision_id: "d".into(),
        };
        store
            .record_account_route_fence(&fence("p", "old", 100))
            .unwrap();
        // Reproduce a new refusal committed after the recovery snapshot was
        // read, but before its delayed cleanup acquires the writer lock.
        store
            .record_account_route_fence(&fence("p", "new", 300))
            .unwrap();
        store
            .record_account_route_fence(&fence("other", "old", 100))
            .unwrap();
        store
            .clear_account_route_fences_before("p", "c", 200)
            .unwrap();
        let remaining = store.account_route_fences(400).unwrap();
        assert_eq!(
            remaining.len(),
            2,
            "a reading heals only older evidence on its provider"
        );
        assert!(remaining
            .iter()
            .any(|row| row.provider == "p" && row.model == "new"));
        assert!(remaining.iter().any(|row| row.provider == "other"));
    }

    #[test]
    fn observation_backoffs_use_only_committed_bound_effects_and_expire() {
        let scratch = cortexkit_test_support::ScratchDir::new("observation-backoffs");
        let descriptor = crate::dev_descriptor(
            "observation-backoffs",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let connection = Connection::open(scratch.path().join("store.db")).unwrap();
        let mut plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {OBSERVATION_BACKOFFS_SQL}"))
            .unwrap();
        let plan: Vec<String> = plan
            .query_map(params![200, -86_399_800], |row| row.get(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.contains("account_observation_backoff_by_received")),
            "{plan:?}"
        );
        store.account_transaction(|tx| {
            for (id, bound, kind, effects) in [
                ("standing", true, "transient_backoff", r#"[{"effect":"transient_backoff","provider":"p","account_id":"a","retry_at_ms":300}]"#),
                ("advisory", false, "transient_backoff", r#"[{"effect":"transient_backoff","provider":"p","account_id":"b","retry_at_ms":500}]"#),
                ("historical", true, "attempt", "opaque legacy effects"),
            ] {
                tx.insert_observation(&AccountObservationRow {
                    observation_id:id.into(),principal:"owner".into(),runner:"runner".into(),event_id:id.into(),
                    kind:kind.into(),admission_id:None,bound,observation_json:"{}".into(),effects_json:effects.into(),received_at:100,
                })?;
            }
            Ok(())
        }).unwrap();
        let writes = store.account_write_transaction_count_for_test();
        assert_eq!(
            store
                .account_read(|tx| tx.observation_backoffs(200))
                .unwrap(),
            vec![("p".into(), "a".into(), 300)]
        );
        assert_eq!(store.account_write_transaction_count_for_test(), writes);
        assert_eq!(
            store
                .account_read(|tx| tx.observation_by_principal_event("owner", "standing"))
                .unwrap()
                .unwrap()
                .runner,
            "runner"
        );
        assert_eq!(store.prune_account_observations(200, 10).unwrap(), 2);
        assert_eq!(
            store
                .account_read(|tx| tx.observation_backoffs(200))
                .unwrap()
                .len(),
            1
        );
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.observation_backoffs(299))
                .unwrap()
                .len(),
            1
        );
        assert!(store
            .account_read(|tx| tx.observation_backoffs(300))
            .unwrap()
            .is_empty());
        assert_eq!(store.prune_account_observations(301, 10).unwrap(), 1);
    }

    #[test]
    fn credential_probe_helper_uses_the_quota_recheck_interval() {
        quota_probe_helper_control(false);
    }
    #[test]
    fn credential_probe_helper_consumes_the_extra_reset_check_once() {
        quota_probe_helper_control(true);
    }

    fn quota_probe_helper_control(reset: bool) {
        let scratch = cortexkit_test_support::ScratchDir::new("quota-credential-probe-helper");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "quota-credential-probe-helper",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: "p".into(),
                    credential_id: "c".into(),
                    code: "billing_exhausted".into(),
                },
                None,
                100,
            )
            .unwrap();
        store
            .account_transaction(|tx| {
                tx.classify_credential_quota("p", "c", true)?;
                Ok(())
            })
            .unwrap();
        if reset {
            store
                .store
                .with_conn(|conn| {
                    conn.execute(
                        "UPDATE fence_recheck SET reset_check_at=200 WHERE kind='credential'",
                        [],
                    )
                })
                .unwrap();
        }
        let at = if reset { 200 } else { 900_100 };
        assert_eq!(
            store
                .account_transaction(|tx| tx.claim_credential_probe("p", "c", 101, at))
                .unwrap(),
            CredentialProbeClaim::Claimed
        );
        if reset {
            assert_eq!(
                store
                    .account_transaction(|tx| tx.claim_credential_probe("p", "c", 101, at))
                    .unwrap(),
                CredentialProbeClaim::Blocked,
                "the provider reset is an extra check, not a permanent due flag"
            );
        } else {
            let clock = store
                .fence_rechecks()
                .unwrap()
                .into_iter()
                .find(|clock| clock.kind == "credential")
                .unwrap();
            assert_eq!(
                clock.next_recheck_at,
                at + 900_000,
                "quota credential helpers must use 15 minutes, not the non-quota hour"
            );
        }
    }

    #[test]
    fn stale_credential_probe_is_claimed_once_with_a_strict_cutoff() {
        let scratch = cortexkit_test_support::ScratchDir::new("account-credential-probe");
        let store = std::sync::Arc::new(
            SqliteRouteStateStore::open(&crate::dev_descriptor(
                "account-credential-probe",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap(),
        );
        store
            .record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
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
                        .account_transaction(|tx| {
                            tx.claim_credential_probe("p", "c", 101, 3_600_100)
                        })
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
                &striatum_core::route_state_store::CredentialRefusal {
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
            150,
            "a probe does not overwrite the refusal's evidence time"
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
        let scratch = cortexkit_test_support::ScratchDir::new("account-eligibility-clear");
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
            .account_transaction(|tx| {
                tx.reconcile_account_eligibility(&[("c".into(), "v1".into())])
            })
            .unwrap();
        assert_eq!(store.account_eligibility().unwrap().len(), 1);
        store
            .account_transaction(|tx| {
                tx.reconcile_account_eligibility(&[("c".into(), "v2".into())])
            })
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
            .account_transaction(|tx| {
                tx.reconcile_account_eligibility(&[("c".into(), "v2".into())])
            })
            .unwrap();
        assert_eq!(
            store.account_eligibility().unwrap()[0]
                .listing_fingerprint
                .as_deref(),
            Some("v2")
        );
        store
            .account_transaction(|tx| tx.reconcile_account_eligibility(&[]))
            .unwrap();
        assert!(store.account_eligibility().unwrap().is_empty());
    }

    #[test]
    fn size_quota_refusals_remain_until_recovery_in_the_same_band() {
        let scratch = cortexkit_test_support::ScratchDir::new("account-size-clear");
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
        assert_eq!(store.account_size_refusals(100).unwrap().len(), 1);
        assert_eq!(
            store.account_size_refusals(0).unwrap().len(),
            1,
            "a reset must not delete quota evidence"
        );
    }

    #[test]
    fn route_quota_fences_replace_reset_evidence_without_expiring() {
        let scratch = cortexkit_test_support::ScratchDir::new("account-route-fence");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "account-route-fence",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let fence = |until_ms, decision: &str| AccountRouteFence {
            provider: "p".into(),
            credential_id: "c".into(),
            model: "m".into(),
            until_ms,
            decision_id: decision.into(),
            recorded_at: 10,
        };
        store.record_account_route_fence(&fence(500, "d1")).unwrap();
        store.record_account_route_fence(&fence(200, "d2")).unwrap();
        assert_eq!(
            store.account_route_fences(100).unwrap(),
            vec![fence(200, "d1")],
            "a new reset replaces check evidence, not a fixed lifetime"
        );
        store.record_account_route_fence(&fence(900, "d3")).unwrap();
        assert_eq!(store.account_route_fences(100).unwrap()[0].until_ms, 900);
        assert_eq!(store.account_route_fences(900).unwrap().len(), 1);
        assert_eq!(
            store.account_route_fences(0).unwrap().len(),
            1,
            "the reset never deletes quota evidence"
        );
    }

    #[test]
    fn route_fence_keeps_first_receipt_and_latest_evidence_separately() {
        let scratch = cortexkit_test_support::ScratchDir::new("route-fence-receipts");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "route-fence-receipts",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for (recorded_at, until_ms, decision_id) in
            [(10, 500, "first"), (20, 900, "later"), (30, 200, "shorter")]
        {
            store
                .record_account_route_fence(&AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms,
                    decision_id: decision_id.into(),
                    recorded_at,
                })
                .unwrap();
        }
        let fences = store.account_route_fences(100).unwrap();
        assert_eq!(fences[0].recorded_at, 10);
        assert_eq!(fences[0].until_ms, 200);
        assert_eq!(fences[0].decision_id, "first");
        assert_eq!(store.account_route_fence_latest_reports().unwrap()["c"], 30);
    }

    #[test]
    fn route_fence_reads_tolerate_a_preupgrade_store_without_the_table() {
        let scratch = cortexkit_test_support::ScratchDir::new("route-fence-preupgrade");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "route-fence-preupgrade",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .store
            .with_conn(|conn: &Connection| conn.execute_batch("DROP TABLE account_route_fence"))
            .unwrap();
        assert!(store.account_route_fences(100).unwrap().is_empty());
        assert!(store
            .account_route_fence_latest_reports()
            .unwrap()
            .is_empty());
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

/// One provider/model closed on one account until `until_ms`, from a quota
/// reset report or a transient provider failure. The credential identifies
/// the serving account; the two classes have different admission policies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRouteFence {
    pub provider: String,
    pub credential_id: String,
    pub model: String,
    pub until_ms: i64,
    /// The decision whose report set the fence; empty for cooldown telemetry.
    pub decision_id: String,
    pub recorded_at: i64,
}

impl SqliteRouteStateStore {
    pub fn account_decision_service_tier(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<(Option<String>, String)>> {
        self.store
            .with_read(|conn: &Connection| {
                conn.query_row(
                    "SELECT service_tier_requested, service_tier_chosen FROM account_decision
                 WHERE decision_id=?1 AND service_tier_chosen IS NOT NULL",
                    params![decision_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
            })
            .map_err(err)
    }

    /// A run outcome is an audit fact, not quota or model-health evidence.
    pub fn record_routing_outcome(
        &self,
        decision: &str,
        outcome: &striatum_core::RouteOutcome,
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
        let fact = fact.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_account_eligibility(&fact)
        })
    }
}

impl AccountTx<'_> {
    /// Latest report times in this transaction's view, including reports on
    /// sibling routes of the same credential.
    pub fn route_fence_latest_reports(
        &self,
    ) -> RouteStateResult<std::collections::BTreeMap<String, i64>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT credential_id,MAX(COALESCE(last_reported_at,recorded_at))
             FROM account_route_fence GROUP BY credential_id",
            )
            .map_err(err)?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    /// Clear older route evidence in the same transaction that checked its
    /// account's pool recovery requirements.
    pub fn clear_account_route_fences_before(
        &self,
        provider: &str,
        credential: &str,
        reading_at: i64,
    ) -> RouteStateResult<usize> {
        self.conn
            .execute(
                "DELETE FROM account_route_fence
            WHERE provider=?1 AND credential_id=?2 AND COALESCE(last_reported_at,recorded_at)<?3",
                params![provider, credential, reading_at],
            )
            .map_err(err)
    }

    pub fn record_admission_probe_scope(
        &self,
        principal: &str,
        id: &str,
        size: u32,
        decision: Option<&str>,
    ) -> RouteStateResult<bool> {
        // Returns whether the scope was stored: stores without the
        // fence-recheck schema have no scope columns.
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(false);
        }
        self.conn.execute("UPDATE account_admission SET input_size_class=?3,model_decision_id=?4 WHERE principal=?1 AND admission_id=?2",params![principal,id,size,decision]).map(|changed|changed==1).map_err(err)
    }

    pub fn size_admission_for_model_decision(
        &self,
        decision: &str,
        provider: &str,
        credential: &str,
        model: &str,
    ) -> RouteStateResult<Option<(u32, i64)>> {
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(None);
        }
        // Keyed exception: a report reaches this after its admission has
        // usually finished, and closed admissions stay in SQL only. The model
        // decision id is indexed, so this never scans.
        note_keyed_exception();
        self.conn.query_row(&format!("SELECT input_size_class,committed_at FROM account_admission {KEYED_EXCEPTION}
            WHERE model_decision_id=?1 AND provider=?2 AND credential_id=?3 AND model=?4 AND input_size_class IS NOT NULL
            ORDER BY committed_at DESC LIMIT 1"),params![decision,provider,credential,model],|row|Ok((row.get(0)?,row.get(1)?))).optional().map_err(err)
    }

    pub fn admission_size_class(&self, principal: &str, id: &str) -> RouteStateResult<Option<u32>> {
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(None);
        }
        self.conn.query_row("SELECT input_size_class FROM account_admission WHERE principal=?1 AND admission_id=?2",params![principal,id],|row|row.get::<_,Option<u32>>(0)).optional().map(Option::flatten).map_err(err)
    }
    pub fn decision_context(&self, decision_id: &str) -> RouteStateResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT context_json FROM account_decision WHERE decision_id = ?1",
                [decision_id],
                |row| row.get(0),
            )
            .optional()
            .map(|context| context.flatten())
            .map_err(err)
    }

    /// A failed attempt must not be healed by its later run_finished event.
    pub fn admission_reported_failure(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM account_observation
                 WHERE principal = ?1 AND admission_id = ?2
                 AND kind IN ('credential_refused', 'transient_backoff', 'account_limit_exhausted'))",
                params![principal, admission_id],
                |row| row.get(0),
            )
            .map_err(err)
    }

    pub fn credential_entitlement_fence(
        &self,
        provider: &str,
        credential: &str,
    ) -> RouteStateResult<Option<striatum_core::route_state_store::CredentialEntitlementFence>>
    {
        self.conn
            .query_row(
                "SELECT provider_id, credential_id, code, listing_fingerprint, observed_at_ms
             FROM credential_entitlement_fences WHERE provider_id = ?1 AND credential_id = ?2",
                params![provider, credential],
                |row| rows::entitlement_at(row, 0),
            )
            .optional()
            .map_err(err)
    }

    /// Account-wide backoffs are derived from committed, bound observation
    /// effects. Keeping the evidence and effect in one row makes event retries
    /// and a restart preserve the same deadline without another write.
    /// Returns `(provider, account_id, retry_at_ms)` for each blocked account.
    pub fn observation_backoffs(&self, now: i64) -> RouteStateResult<Vec<(String, String, i64)>> {
        let mut statement = self.conn.prepare(OBSERVATION_BACKOFFS_SQL).map_err(err)?;
        let rows = statement
            .query_map(params![now, now.saturating_sub(24 * 60 * 60_000)], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(err)?;
        rows.collect::<Result<_, _>>().map_err(err)
    }

    pub fn record_account_service_tier(
        &self,
        decision_id: &str,
        requested: Option<&str>,
        chosen: &str,
    ) -> RouteStateResult<()> {
        self.conn.execute(
            "UPDATE account_decision SET service_tier_requested=?2, service_tier_chosen=?3 WHERE decision_id=?1",
            params![decision_id, requested, chosen],
        ).map(|_| ()).map_err(err)
    }

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
        crate::fence_rechecks::sync_on(self, crate::now_ms()).map_err(err)?;
        let changed=self.conn.execute("UPDATE fence_recheck SET last_claimed_at=?4,
            next_recheck_at=?4+CASE WHEN quota=1 THEN ?5 ELSE ?6 END,reset_check_at=NULL,claim_owner=NULL
            WHERE kind='credential' AND provider=?1 AND credential_id=?2 AND evidence_at<?3
            AND MIN(next_recheck_at,COALESCE(reset_check_at,next_recheck_at))<=?4 AND cleared_at IS NULL",params![provider,credential,before,at,
                striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
                striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS]).map_err(err)?;
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
        let identity = serde_json::to_string(&striatum_core::route_state_store::ProviderOutcome {
            account_ref: account.into(),
            credential_id: Some(credential.into()),
            resets_at_ms: None,
            credential_refusal: None,
            ..Default::default()
        })
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

    /// A parent's latest admission holds its current account until its pin
    /// lapses: the admission deadline before start, the lease deadline after.
    pub fn parent_used_account(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
        now_ms: i64,
    ) -> RouteStateResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT a.account_id FROM (
                SELECT * FROM account_admission
                 WHERE principal=?1 AND conversation_hash=?2 AND provider=?3
                 ORDER BY committed_at DESC, rowid DESC LIMIT 1
             ) a JOIN account_affinity f
               ON f.principal=a.principal AND f.conversation_hash=a.conversation_hash
              AND f.provider=a.provider AND f.pin_id=a.pin_id AND f.pin_generation=a.pin_generation
             WHERE (a.state='reserved' AND a.admission_valid_until>?4)
                OR (a.state='started' AND a.lease_valid_until>?4)",
                params![principal, hash, provider, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)
    }

    /// A reserved first send has no prompt cache yet. A started admission on
    /// this pin establishes account affinity even after that run has finished.
    pub fn pin_has_started(&self, affinity: &AccountAffinityRow) -> RouteStateResult<bool> {
        self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM account_admission WHERE principal=?1 AND conversation_hash=?2 AND provider=?3 AND pin_id=?4 AND pin_generation=?5 AND started_at IS NOT NULL)",
            params![affinity.principal, affinity.conversation_hash, affinity.provider, affinity.pin_id, affinity.pin_generation],
            |row| row.get(0),
        ).map_err(err)
    }
}

impl SqliteRouteStateStore {
    pub fn account_eligibility(&self) -> RouteStateResult<Vec<AccountEligibilityFact>> {
        self.store.with_read(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_eligibility")? { return Ok(Vec::new()); }
            let mut statement = conn.prepare("SELECT provider,credential_id,model,listing_fingerprint,code,observed_at FROM account_eligibility ORDER BY provider,credential_id,model")?;
            let rows = statement.query_map([], |row| rows::eligibility_at(row, 0))?;
            rows.collect()
        }).map_err(err)
    }

    pub fn record_account_size_refusal(&self, fact: &AccountSizeRefusal) -> RouteStateResult<()> {
        let fact = fact.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_account_size_refusal(&fact)
        })
    }

    /// Size-specific quota evidence remains until fresh headroom or success;
    /// its reported reset is a recheck hint, not an expiry.
    pub fn account_size_refusals(&self, _now: i64) -> RouteStateResult<Vec<AccountSizeRefusal>> {
        self.store.with_read(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_size_refusal")? { return Ok(Vec::new()); }
            let extra=if account_fact_table_exists(conn,"account_non_quota_size_refusal")? {
                " UNION ALL SELECT provider,credential_id,model,size_class,expires_at,observed_at FROM account_non_quota_size_refusal"
            }else{""};
            let mut statement = conn.prepare(&format!("SELECT provider,credential_id,model,size_class,expires_at,observed_at FROM account_size_refusal{extra}"))?;
            let rows = statement.query_map([], |row| rows::size_refusal_at(row, 0))?;
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
        let provider = provider.to_owned();
        let credential = credential.to_owned();
        let model = model.to_owned();
        self.account_command(crate::AccountCommandKind::Repair, move |tx| {
            tx.clear_account_size_refusal(&provider, &credential, &model, size_class, observed_at)
        })
    }

    /// Record quota evidence; the reported reset schedules an extra check, not
    /// an expiry. New evidence replaces the old reset without extending a hold.
    pub fn record_account_route_fence(&self, fence: &AccountRouteFence) -> RouteStateResult<()> {
        let fence = fence.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_account_route_fence(&fence)
        })
    }

    pub fn record_transient_account_route_fence(
        &self,
        fence: &AccountRouteFence,
    ) -> RouteStateResult<()> {
        let fence = fence.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_transient_account_route_fence(&fence)
        })
    }

    pub fn record_billing_account_route_fence(
        &self,
        fence: &AccountRouteFence,
    ) -> RouteStateResult<()> {
        let fence = fence.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_billing_account_route_fence(&fence)
        })
    }

    pub fn non_quota_account_route_fences(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<AccountRouteFence>> {
        self.classified_account_route_fences(now, "billing")
    }

    /// Replace a quota horizon without reporting another refusal. Compare the
    /// evidence originally read so a concurrent report cannot be overwritten.
    pub fn reconcile_account_route_fence(
        &self,
        fence: &AccountRouteFence,
        until_ms: i64,
        last_reported_at: i64,
    ) -> RouteStateResult<()> {
        let fence = fence.to_owned();
        self.account_command(crate::AccountCommandKind::Repair, move |tx| {
            tx.reconcile_account_route_fence(&fence, until_ms, last_reported_at)
        })
    }

    /// Latest evidence per credential, separate from the fence's first receipt.
    pub fn account_route_fence_latest_reports(
        &self,
    ) -> RouteStateResult<std::collections::BTreeMap<String, i64>> {
        self.store.with_read(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_route_fence")? { return Ok(std::collections::BTreeMap::new()); }
            let mut statement = conn.prepare("SELECT credential_id,MAX(COALESCE(last_reported_at,recorded_at)) FROM account_route_fence GROUP BY credential_id")?;
            let rows = statement.query_map([], |row| Ok((row.get(0)?,row.get(1)?)))?;
            rows.collect()
        }).map_err(err)
    }

    /// Retained quota route evidence. Provider dates schedule checks rather
    /// than expiring rows; recovery and operator actions clear the evidence.
    pub fn account_route_fences(&self, now: i64) -> RouteStateResult<Vec<AccountRouteFence>> {
        self.classified_account_route_fences(now, "quota")
    }

    pub fn transient_account_route_fences(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<AccountRouteFence>> {
        self.classified_account_route_fences(now, "transient")
    }

    fn classified_account_route_fences(
        &self,
        _now: i64,
        class: &str,
    ) -> RouteStateResult<Vec<AccountRouteFence>> {
        self.store.with_read(|conn: &Connection| {
            if !account_fact_table_exists(conn, "account_route_fence")? { return Ok(Vec::new()); }
            let table=if class!="quota" {"account_non_quota_route_fence"} else {"account_route_fence"};
            let filter=if class=="transient" {"WHERE wall_kind='transient'"} else if class=="billing" {"WHERE wall_kind='billing'"} else {"WHERE wall_class='quota'"};
            let mut statement = conn.prepare(&format!("SELECT provider,credential_id,model,until_ms,decision_id,recorded_at FROM {table} {filter} ORDER BY provider,credential_id,model"))?;
            let rows = statement.query_map([], |row| rows::route_fence_at(row, 0))?;
            rows.collect()
        }).map_err(err)
    }

    /// Revalidate evidence ordering under the writer lock. The reading may
    /// have been copied before another request recorded a newer refusal.
    pub fn clear_account_route_fences_before(
        &self,
        provider: &str,
        credential: &str,
        reading_at: i64,
    ) -> RouteStateResult<usize> {
        let provider = provider.to_owned();
        let credential = credential.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_account_route_fences_before(&provider, &credential, reading_at)
        })
    }

    /// An explicit operator clear lifts quota, billing and transient route
    /// evidence. Quota-reading reconciliation must remain quota-only.
    pub fn clear_all_account_route_fences_for_credential(
        &self,
        credential_id: &str,
    ) -> RouteStateResult<usize> {
        let credential_id = credential_id.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_all_account_route_fences_for_credential(&credential_id)
        })
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
    /// Routing's latest attempt time, or the start time if later.
    pub last_observed_at: Option<i64>,
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
     released_at, finished_at, last_observed_at";

/// How many columns `ADMISSION_COLUMNS` selects. Queries that select extra
/// columns after it read them from this index on, so adding an admission
/// column moves them all at once instead of leaving a stale literal index.
const ADMISSION_COLUMN_COUNT: usize = 26;

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
        charged: row.get::<_, i64>(13)? == 1,
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
        last_observed_at: row.get(25)?,
    })
}

const FENCE_COLUMNS: &str = "fence_id, provider, account_id, scope_json, fence_json, source, \
     observed_at, principal, admission_id, observation_id, created_at, cleared_at, cleared_reason";

fn fence_from_row(row: &Row<'_>) -> rusqlite::Result<AccountFenceRow> {
    rows::fence_at(row, 0)
}

/// Row decoders shared by the SQL readers and the in-memory block state,
/// which reads the same columns back after each write. Each decodes its
/// reader's columns, in that reader's order, starting at column `at`.
pub(crate) mod rows {
    use super::{
        AccountEligibilityFact, AccountExclusionRow, AccountFenceRow, AccountRouteFence,
        AccountSizeRefusal,
    };
    use rusqlite::Row;
    use striatum_core::route_state_store::CredentialEntitlementFence;

    /// `FENCE_COLUMNS`.
    pub(crate) fn fence_at(row: &Row<'_>, at: usize) -> rusqlite::Result<AccountFenceRow> {
        Ok(AccountFenceRow {
            fence_id: row.get(at)?,
            provider: row.get(at + 1)?,
            account_id: row.get(at + 2)?,
            scope_json: row.get(at + 3)?,
            fence_json: row.get(at + 4)?,
            source: row.get(at + 5)?,
            observed_at: row.get(at + 6)?,
            principal: row.get(at + 7)?,
            admission_id: row.get(at + 8)?,
            observation_id: row.get(at + 9)?,
            created_at: row.get(at + 10)?,
            cleared_at: row.get(at + 11)?,
            cleared_reason: row.get(at + 12)?,
        })
    }

    /// `provider,credential_id,model,listing_fingerprint,code,observed_at`.
    pub(crate) fn eligibility_at(
        row: &Row<'_>,
        at: usize,
    ) -> rusqlite::Result<AccountEligibilityFact> {
        Ok(AccountEligibilityFact {
            provider: row.get(at)?,
            credential_id: row.get(at + 1)?,
            model: row.get(at + 2)?,
            listing_fingerprint: row.get(at + 3)?,
            code: row.get(at + 4)?,
            observed_at: row.get(at + 5)?,
        })
    }

    /// `provider,credential_id,model,size_class,expires_at,observed_at`.
    pub(crate) fn size_refusal_at(
        row: &Row<'_>,
        at: usize,
    ) -> rusqlite::Result<AccountSizeRefusal> {
        Ok(AccountSizeRefusal {
            provider: row.get(at)?,
            credential_id: row.get(at + 1)?,
            model: row.get(at + 2)?,
            size_class: row.get(at + 3)?,
            expires_at: row.get(at + 4)?,
            observed_at: row.get(at + 5)?,
        })
    }

    /// `provider,credential_id,model,until_ms,decision_id,recorded_at`.
    pub(crate) fn route_fence_at(row: &Row<'_>, at: usize) -> rusqlite::Result<AccountRouteFence> {
        Ok(AccountRouteFence {
            provider: row.get(at)?,
            credential_id: row.get(at + 1)?,
            model: row.get(at + 2)?,
            until_ms: row.get(at + 3)?,
            decision_id: row.get(at + 4)?,
            recorded_at: row.get(at + 5)?,
        })
    }

    /// `principal,admission_id,credential_id,runner_view_digest,
    /// roster_view_digest,observation_id,recorded_at`.
    pub(crate) fn exclusion_at(row: &Row<'_>, at: usize) -> rusqlite::Result<AccountExclusionRow> {
        Ok(AccountExclusionRow {
            principal: row.get(at)?,
            admission_id: row.get(at + 1)?,
            credential_id: row.get(at + 2)?,
            runner_view_digest: row.get(at + 3)?,
            roster_view_digest: row.get(at + 4)?,
            observation_id: row.get(at + 5)?,
            recorded_at: row.get(at + 6)?,
        })
    }

    /// `provider_id,credential_id,code,listing_fingerprint,observed_at_ms`.
    pub(crate) fn entitlement_at(
        row: &Row<'_>,
        at: usize,
    ) -> rusqlite::Result<CredentialEntitlementFence> {
        Ok(CredentialEntitlementFence {
            provider_id: row.get(at)?,
            credential_id: row.get(at + 1)?,
            code: row.get(at + 2)?,
            listing_fingerprint: row.get(at + 3)?,
            observed_at_ms: row.get(at + 4)?,
        })
    }
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

/// One account's charged, non-released admissions committed after a floor.
/// `account_admission_demand` (migration 068) is keyed provider, account,
/// commit time, so the planner answers this with one covering range search
/// in commit order, whatever the size of the older history.
pub const ACCOUNT_DEMAND_SQL: &str =
    "SELECT account_id, state, committed_at, request_bytes FROM account_admission \
     WHERE provider = ?1 AND account_id = ?2 AND charged = 1 AND state != 'released' \
       AND committed_at > ?3 \
     ORDER BY committed_at, principal, admission_id";

/// The account-selection tables inside one open write transaction. Every
/// method reads or writes through the same transaction, so a whole admission
/// commits or rolls back as one.
pub struct AccountTx<'a> {
    pub(crate) conn: &'a Connection,
    /// The block state of the writer's working copy when the command runs
    /// in the account writer with the module's state loaded. Claim and probe
    /// decisions read it instead of SQL; `None` (a projection, a test seam,
    /// the state loader) makes them read SQL.
    pub(crate) blocks: Option<&'a crate::BlockState>,
    /// `blocks` with the rows this command has changed so far laid over it,
    /// and how many captured rows that copy includes, so each read applies
    /// only the rows captured since the last one.
    pub(crate) overlay: std::cell::RefCell<Option<(usize, crate::BlockState)>>,
}

impl<'a> AccountTx<'a> {
    /// A transaction whose decisions read SQL.
    pub(crate) fn sql(conn: &'a Connection) -> Self {
        Self::with_blocks(conn, None)
    }

    pub(crate) fn with_blocks(conn: &'a Connection, blocks: Option<&'a crate::BlockState>) -> Self {
        Self {
            conn,
            blocks,
            overlay: std::cell::RefCell::new(None),
        }
    }
}

mod continuation;
mod state_rows;
pub use state_rows::*;

impl AccountTx<'_> {
    /// The installation-local salt for hashing conversation keys, created with
    /// `fresh` the first time it is needed and never changed afterwards.
    pub fn salt(&self, now: i64, fresh: impl FnOnce() -> String) -> RouteStateResult<String> {
        let existing = self.existing_salt()?;
        if let Some(salt) = existing {
            return Ok(salt);
        }
        let salt = fresh();
        self.insert_selection_salt(&salt, now)?;
        Ok(salt)
    }

    /// Persist the module's first salt. The owning command decides whether it
    /// is absent; this writer never reads the module's working state.
    pub fn insert_selection_salt(&self, salt: &str, now: i64) -> RouteStateResult<()> {
        self.conn
            .execute(
                "INSERT INTO account_selection_salt (singleton, salt, created_at) VALUES (1, ?1, ?2)",
                params![salt, now],
            )
            .map_err(err)?;
        Ok(())
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
                      ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
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
                    row.last_observed_at,
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
                 started_late = ?5, last_observed_at = MAX(COALESCE(last_observed_at, ?4), ?4) \
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

    /// Charged demand of `account_id`'s admissions for `provider` that are not
    /// released and were committed after `committed_after` (Unix
    /// milliseconds). `None` reads the account's whole stored history, for a
    /// caller whose policy still counts admissions of any age.
    pub fn demand(
        &self,
        provider: &str,
        account_id: &str,
        committed_after: Option<i64>,
    ) -> RouteStateResult<Vec<AccountDemandRow>> {
        let mut statement = self.conn.prepare(ACCOUNT_DEMAND_SQL).map_err(err)?;
        let rows = statement
            .query_map(
                params![provider, account_id, committed_after.unwrap_or(i64::MIN)],
                |row| {
                    Ok(AccountDemandRow {
                        account_id: row.get(0)?,
                        state: row.get(1)?,
                        committed_at: row.get(2)?,
                        request_bytes: to_u64(row.get(3)?),
                    })
                },
            )
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
            .execute(END_LEASE_SQL, params![pin_id, at])
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
            let healed = self.successful_quota_pools(
                &evidence.fence_id,
                evidence.observed_at.min(evidence.created_at),
            )?;
            let mut recovery_set: Vec<PoolKey> = serde_json::from_str(&recovery).map_err(err)?;
            recovery_set.retain(|pool| !healed.contains(pool));
            Ok(AccountPoolFenceRow {
                evidence,
                pool_key: key
                    .map(|key| serde_json::from_str(&key))
                    .transpose()
                    .map_err(err)?,
                recovery_set,
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

    /// End a standing block that a decision read as `row`: the update is
    /// keyed and names the row's evidence, so it fails with
    /// [`crate::block_state::diverged`] if SQL no longer holds that block.
    pub fn clear_held_fence(
        &self,
        row: &AccountFenceRow,
        at: i64,
        reason: &str,
    ) -> RouteStateResult<()> {
        let changed = self
            .conn
            .execute(
                "UPDATE account_fence SET cleared_at = ?2, cleared_reason = ?3 \
                 WHERE fence_id = ?1 AND cleared_at IS NULL AND observed_at = ?4 \
                   AND created_at = ?5 AND fence_json = ?6",
                params![
                    row.fence_id,
                    at,
                    reason,
                    row.observed_at,
                    row.created_at,
                    row.fence_json
                ],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("clear of fence {}", row.fence_id),
        )
        .map_err(err)
    }

    /// [`Self::clear_held_fence`] for a block whose clear was decided on its
    /// pools: the update also names the pool key and recovery set as JSON
    /// and the number of successes recorded for `successes.0` (the evidence
    /// time), which must be `successes.1`.
    pub(crate) fn clear_held_pool_fence(
        &self,
        row: &AccountFenceRow,
        pool_key_json: Option<&str>,
        recovery_set_json: &str,
        successes: (i64, usize),
        at: i64,
        reason: &str,
    ) -> RouteStateResult<()> {
        let changed = self
            .conn
            .execute(
                "UPDATE account_fence SET cleared_at = ?2, cleared_reason = ?3 \
                 WHERE fence_id = ?1 AND cleared_at IS NULL AND observed_at = ?4 \
                   AND created_at = ?5 AND fence_json = ?6 AND pool_key_json IS ?7 \
                   AND recovery_set_json = ?8 AND (SELECT count(*) FROM fence_success_pool \
                   WHERE fence_id = ?1 AND evidence_at = ?9) = ?10",
                params![
                    row.fence_id,
                    at,
                    reason,
                    row.observed_at,
                    row.created_at,
                    row.fence_json,
                    pool_key_json,
                    recovery_set_json,
                    successes.0,
                    successes.1 as i64
                ],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("clear of fence {} on its pools", row.fence_id),
        )
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
                rows::exclusion_at(row, 0)
            })
            .map_err(err)?;
        rows.collect::<rusqlite::Result<_>>().map_err(err)
    }

    /// Record a scoped not-found. Returns false when this admission already
    /// excluded the credential.
    pub fn insert_exclusion(&self, row: &AccountExclusionRow) -> RouteStateResult<bool> {
        let changed = self
            .conn
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
            .map_err(err)?;
        if changed == 1 {
            crate::fence_rechecks::register_on(
                self,
                "principal",
                &serde_json::json!([row.principal, row.admission_id, row.credential_id])
                    .to_string(),
            )
            .map_err(err)?;
        }
        Ok(changed == 1)
    }

    pub fn insert_exclusion_with_recheck(
        &self,
        row: &AccountExclusionRow,
        after_ms: i64,
    ) -> RouteStateResult<bool> {
        let inserted = self.insert_exclusion(row)?;
        if inserted && crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            self.conn
                .execute(
                    "UPDATE fence_recheck SET next_recheck_at=MIN(next_recheck_at,?2)
                WHERE kind='principal' AND origin_key=?1",
                    params![
                        serde_json::json!([row.principal, row.admission_id, row.credential_id])
                            .to_string(),
                        row.recorded_at.saturating_add(
                            after_ms
                                .clamp(0, striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS)
                        )
                    ],
                )
                .map_err(err)?;
        }
        Ok(inserted)
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

    /// Observation event ids are unique within a principal, even when
    /// different observations name different runners.
    pub fn observation_by_principal_event(
        &self,
        principal: &str,
        event_id: &str,
    ) -> RouteStateResult<Option<AccountObservationRow>> {
        let runner: Option<String> = self.conn.query_row(
            "SELECT runner FROM account_observation WHERE principal=?1 AND event_id=?2 \
             UNION ALL SELECT runner FROM account_observation_event WHERE principal=?1 AND event_id=?2 LIMIT 1",
            params![principal, event_id], |row| row.get(0),
        ).optional().map_err(err)?;
        match runner {
            Some(runner) => self.observation_by_event(principal, &runner, event_id),
            None => Ok(None),
        }
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
            .map_err(err)?;
        if bound {
            self.record_transient_observation_effects(observation_id, effects_json)?;
        }
        Ok(())
    }

    fn record_transient_observation_effects(
        &self,
        id: &str,
        effects: &str,
    ) -> RouteStateResult<()> {
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        // The rows this observation inserted or moved, returned by the
        // write itself rather than read back from the block table.
        let keys = self
            .conn
            .prepare(INSERT_TRANSIENT_BACKOFFS)
            .map_err(err)?
            .query_map(params![id, effects], |row| row.get::<_, String>(0))
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(err)?;
        for key in keys {
            crate::fence_rechecks::register_on(self, "observation", &key).map_err(err)?;
        }
        Ok(())
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
            .map_err(err)?;
        if row.bound {
            self.record_transient_observation_effects(&row.observation_id, &row.effects_json)?;
        }
        Ok(())
    }
}

impl SqliteRouteStateStore {
    fn begin_account_write<'a>(&self, conn: &'a Connection) -> rusqlite::Result<Transaction<'a>> {
        let tx = crate::write_transaction(conn)?;
        #[cfg(any(test, feature = "test-support"))]
        self.account_write_transactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(tx)
    }

    /// Number of account write transactions begun by this store: batches from
    /// the account writer (the single thread that groups account commands into
    /// shared commits), plus account transactions some callers still open
    /// directly through `begin_account_write`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn account_write_transaction_count_for_test(&self) -> u64 {
        self.account_write_transactions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Read account state in a deferred transaction without reserving the writer.
    /// The callback must not write; admissions must use `account_transaction`.
    pub fn account_read<T>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        self.store
            .with_read(|conn: &Connection| {
                // A pooled reader already has a snapshot; a bare connection
                // needs a deferred transaction.
                let tx = if conn.is_autocommit() {
                    Some(Transaction::new_unchecked(
                        conn,
                        TransactionBehavior::Deferred,
                    )?)
                } else {
                    None
                };
                let result = f(&AccountTx::sql(conn));
                if result.is_ok() {
                    if let Some(tx) = tx {
                        crate::connection_store::timed_commit(tx)?;
                    }
                }
                Ok(result)
            })
            .map_err(err)?
    }

    /// Run `f` inside one IMMEDIATE write transaction over the account
    /// tables. `Ok` commits before this returns, so a reply built from the
    /// result is sent only once the decision is durable; `Err` rolls every
    /// write back.
    #[cfg(any(test, feature = "test-support"))]
    pub fn account_transaction<T>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        let _seeding = crate::block_state::Seeding::begin();
        self.direct_account_transaction(f)
    }

    pub(crate) fn direct_account_transaction<T>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        self.direct_account_transaction_with_error(f)
    }

    pub(crate) fn direct_account_transaction_with_error<
        T,
        E: From<striatum_core::RouteStateStoreError>,
    >(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        #[cfg(not(any(test, feature = "test-support")))]
        assert!(
            self.store.is_projection(),
            "live account writes require a writer command"
        );
        #[cfg_attr(
            not(any(test, feature = "test-support")),
            allow(unused_variables, unused_assignments, unused_mut)
        )]
        let mut wrote = false;
        let result = self
            .store
            .with_conn(|conn: &Connection| {
                let before = conn.total_changes();
                let tx = self.begin_account_write(conn)?;
                let result = f(&AccountTx::sql(&tx));
                if result.is_ok() {
                    crate::connection_store::timed_commit(tx)?;
                    wrote = conn.total_changes() != before;
                }
                Ok(result)
            })
            .map_err(err)?;
        #[cfg(any(test, feature = "test-support"))]
        if wrote {
            self.reload_account_state_after_direct_write()?;
        }
        result
    }

    /// Revalidate a read-path repair under the IMMEDIATE writer. A competing
    /// writer may already have fixed the rows; commit only if the callback
    /// actually changed durable state, otherwise release the transaction.
    #[cfg(any(test, feature = "test-support"))]
    pub fn account_repair(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<bool>,
    ) -> RouteStateResult<()> {
        let seeding = crate::block_state::Seeding::begin();
        let result = self
            .store
            .with_conn(|conn: &Connection| {
                let tx = self.begin_account_write(conn)?;
                let result = f(&AccountTx::sql(&tx));
                if matches!(result, Ok(true)) {
                    crate::connection_store::timed_commit(tx)?;
                }
                Ok(result)
            })
            .map_err(err)??;
        drop(seeding);
        if result {
            self.reload_account_state_after_direct_write()?;
        }
        Ok(())
    }

    /// The newest decisions recorded in `mode`, newest first.
    pub fn account_decisions(
        &self,
        mode: &str,
        limit: usize,
    ) -> RouteStateResult<Vec<AccountDecisionRecord>> {
        self.store
            .with_read(|c: &Connection| {
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
            .with_read(|c: &Connection| {
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
            .with_read(|c: &Connection| {
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

#[cfg(test)]
mod transient_fence_tests {
    use super::*;

    #[test]
    fn exhausted_transient_retries_never_install_a_model_failure_backoff() {
        use striatum_core::{DecisionInput, DecisionOutcomeUpdate, RouteOutcome, RouteStateStore};
        let scratch = cortexkit_test_support::ScratchDir::new("transient-terminal-backoff");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "transient-terminal-backoff",
            scratch.path().join("routing.db").to_string_lossy(),
        ))
        .unwrap();
        let error = "provider retry budget exhausted";
        for _ in 0..3 {
            let decision = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some("openai/gpt-6-astra/xhigh".into()),
                    selected_provider_id: Some("openai".into()),
                    selected_model_id: Some("gpt-6-astra".into()),
                    target_agent: "mason".into(),
                    requirements_json: "{}".into(),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &decision,
                    &DecisionOutcomeUpdate {
                        provider_outcome: Some(striatum_core::route_state_store::ProviderOutcome {
                            error_class: Some("transient".into()),
                            provider_code: Some("server_is_overloaded".into()),
                            ..Default::default()
                        }),
                        outcome: RouteOutcome::SessionError,
                        last_error: Some(error.into()),
                        observed_at_ms: 1_000,
                        failure_backoff: Default::default(),
                    },
                )
                .unwrap();
        }
        let snapshot = store.build_snapshot(1_000.0).unwrap();
        assert_eq!(
            snapshot
                .by_route
                .get("openai/gpt-6-astra/xhigh")
                .and_then(|state| state.failure_backoff_until),
            None
        );
    }

    #[test]
    fn transient_fences_never_replace_extend_or_soften_a_live_quota_fence() {
        let scratch = cortexkit_test_support::ScratchDir::new("transient-quota-fence-merge");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "transient-quota-fence-merge",
            scratch.path().join("routing.db").to_string_lossy(),
        ))
        .unwrap();
        let mut fence = AccountRouteFence {
            provider: "openai".into(),
            credential_id: "chatgpt:openai:gmail".into(),
            model: "gpt-6-astra".into(),
            until_ms: 10_000,
            decision_id: "quota".into(),
            recorded_at: 1_000,
        };
        store.record_account_route_fence(&fence).unwrap();
        fence.until_ms = 20_000;
        fence.recorded_at = 2_000;
        store.record_transient_account_route_fence(&fence).unwrap();
        assert_eq!(
            store.account_route_fences(2_000).unwrap()[0].until_ms,
            10_000
        );
        assert_eq!(
            store.transient_account_route_fences(2_000).unwrap().len(),
            1,
            "independent transient evidence survives quota"
        );
        assert_eq!(
            store.account_route_fence_latest_reports().unwrap()["chatgpt:openai:gmail"],
            1_000
        );
        fence.recorded_at = 11_000;
        fence.until_ms = 16_000;
        store.record_transient_account_route_fence(&fence).unwrap();
        assert_eq!(store.account_route_fences(11_000).unwrap().len(), 1);
        assert_eq!(
            store.transient_account_route_fences(11_000).unwrap()[0].until_ms,
            16_000
        );
        fence.recorded_at = 12_000;
        fence.until_ms = 14_000;
        store.record_account_route_fence(&fence).unwrap();
        assert_eq!(
            store.transient_account_route_fences(12_000).unwrap().len(),
            1
        );
        assert_eq!(
            store.account_route_fences(12_000).unwrap()[0].until_ms,
            14_000
        );
        assert_eq!(store.account_route_fences(14_000).unwrap().len(), 1);
        let clocks = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, 14_000))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, 16_000))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, 16_000))
            .unwrap());
    }
}

impl crate::AccountTx<'_> {
    pub fn record_account_eligibility(
        &self,
        fact: &AccountEligibilityFact,
    ) -> RouteStateResult<()> {
        self.command_scope(|tx| tx.record_eligibility(fact))
    }

    pub fn reconcile_account_eligibility(
        &self,
        listings: &[(String, String)],
    ) -> RouteStateResult<()> {
        let mut fingerprints = std::collections::HashMap::with_capacity(listings.len());
        for (credential, fingerprint) in listings {
            fingerprints
                .entry(credential.as_str())
                .or_insert(fingerprint.as_str());
        }
        let changed_credentials =
            |conn: &Connection| -> rusqlite::Result<std::collections::BTreeSet<String>> {
                let mut changed = std::collections::BTreeSet::new();
                if !account_fact_table_exists(conn, "account_eligibility")? {
                    return Ok(changed);
                }
                let mut statement = conn.prepare(
                    "SELECT DISTINCT credential_id,listing_fingerprint FROM account_eligibility",
                )?;
                for row in statement.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })? {
                    let (credential, recorded) = row?;
                    if recorded.as_deref().is_none()
                        || recorded.as_deref() != fingerprints.get(credential.as_str()).copied()
                    {
                        changed.insert(credential);
                    }
                }
                Ok(changed)
            };
        let changed = self
            .command_sql(|conn: &Connection| {
                // A connection in autocommit mode needs its own read transaction
                // to keep the comparison on one view.
                let read = if conn.is_autocommit() {
                    Some(Transaction::new_unchecked(
                        conn,
                        TransactionBehavior::Deferred,
                    )?)
                } else {
                    None
                };
                let changed = changed_credentials(conn)?;
                if let Some(read) = read {
                    read.commit()?;
                }
                Ok(changed)
            })
            .map_err(err)?;
        if changed.is_empty() {
            return Ok(());
        }
        self.command_sql(|conn: &Connection| {
            // Another caller may have reconciled while this caller waited for
            // the connection gate. Do not open an empty write transaction.
            if changed_credentials(conn)?.is_empty() { return Ok(()); }
            let tx = conn;
            // Inspect current rows again, including restrictions reported after
            // the read snapshot. No remembered listing can hide a new row.
            let credentials = changed_credentials(&tx)?;
            if credentials.is_empty() { return Ok(()); }
            for credential in credentials {
                match fingerprints.get(credential.as_str()) {
                    Some(fingerprint) => {
                        tx.execute("DELETE FROM account_eligibility WHERE credential_id=?1 AND listing_fingerprint IS NOT NULL AND listing_fingerprint<>?2", params![credential, fingerprint])?;
                        tx.execute("UPDATE account_eligibility SET listing_fingerprint=?2 WHERE credential_id=?1 AND listing_fingerprint IS NULL", params![credential, fingerprint])?;
                    }
                    None => { tx.execute("DELETE FROM account_eligibility WHERE credential_id=?1", [&credential])?; }
                }
            }
            Ok(())
        }).map_err(err)
    }

    pub fn record_account_size_refusal(&self, fact: &AccountSizeRefusal) -> RouteStateResult<()> {
        self.command_sql(|conn: &Connection| {
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

    pub fn clear_account_size_refusal(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
        size_class: u32,
        observed_at: i64,
    ) -> RouteStateResult<()> {
        self.command_sql(|conn: &Connection| {
            conn.execute("DELETE FROM account_size_refusal WHERE provider=?1 AND credential_id=?2 AND model=?3 AND size_class=?4 AND observed_at<?5",
                params![provider,credential,model,size_class,observed_at]).map(|_| ())
        }).map_err(err)
    }

    pub fn record_account_route_fence(&self, fence: &AccountRouteFence) -> RouteStateResult<()> {
        self.record_classified_account_route_fence(fence, "quota")
    }

    pub fn record_transient_account_route_fence(
        &self,
        fence: &AccountRouteFence,
    ) -> RouteStateResult<()> {
        self.command_scope(|tx| tx.record_account_route_backoff(fence, "transient"))
    }

    pub fn record_billing_account_route_fence(
        &self,
        fence: &AccountRouteFence,
    ) -> RouteStateResult<()> {
        self.command_scope(|tx| tx.record_non_quota_route_fence(fence))
    }

    fn record_classified_account_route_fence(
        &self,
        fence: &AccountRouteFence,
        class: &str,
    ) -> RouteStateResult<()> {
        self.command_sql(|conn: &Connection| {
                conn.execute(
                    "INSERT INTO account_route_fence (provider,credential_id,model,until_ms,decision_id,recorded_at,last_reported_at,wall_class) VALUES (?1,?2,?3,?4,?5,?6,?6,?7) \
                ON CONFLICT (provider,credential_id,model) DO UPDATE SET \
                until_ms=CASE WHEN excluded.recorded_at>=COALESCE(account_route_fence.last_reported_at,account_route_fence.recorded_at) THEN excluded.until_ms ELSE account_route_fence.until_ms END, \
                wall_class=CASE WHEN account_route_fence.until_ms<=excluded.recorded_at THEN excluded.wall_class \
                  WHEN account_route_fence.wall_class='quota' THEN 'quota' ELSE excluded.wall_class END, \
                recorded_at=MIN(account_route_fence.recorded_at,excluded.recorded_at), \
                last_reported_at=CASE \
                  WHEN account_route_fence.wall_class='quota' AND excluded.wall_class='transient' AND account_route_fence.until_ms>excluded.recorded_at \
                    THEN account_route_fence.last_reported_at \
                  ELSE MAX(COALESCE(account_route_fence.last_reported_at,account_route_fence.recorded_at),excluded.recorded_at) END",
                    params![
                        fence.provider,
                        fence.credential_id,
                        fence.model,
                        fence.until_ms,
                        fence.decision_id,
                        fence.recorded_at,
                        class
                    ],
                )?;
                if crate::fence_rechecks::enabled(&conn)? {conn.execute("UPDATE fence_recheck SET claim_owner=NULL WHERE kind='account_route' AND origin_key=?1",[serde_json::json!([fence.provider,fence.credential_id,fence.model]).to_string()])?;}
                Ok(())
            })
            .map_err(err)
    }

    pub fn reconcile_account_route_fence(
        &self,
        fence: &AccountRouteFence,
        until_ms: i64,
        last_reported_at: i64,
    ) -> RouteStateResult<()> {
        self.command_sql(|conn: &Connection| {
            conn.execute("UPDATE account_route_fence SET until_ms=?1 WHERE provider=?2 AND credential_id=?3 AND model=?4 AND wall_class='quota' AND until_ms=?5 AND COALESCE(last_reported_at,recorded_at)=?6",
                params![until_ms, fence.provider, fence.credential_id, fence.model, fence.until_ms, last_reported_at]).map(|_| ())
        }).map_err(err)
    }

    pub fn clear_all_account_route_fences_for_credential(
        &self,
        credential_id: &str,
    ) -> RouteStateResult<usize> {
        self.command_sql(|conn: &Connection| {
            let tx = conn;
            let mut cleared = 0;
            for table in ["account_route_fence", "account_non_quota_route_fence"] {
                if account_fact_table_exists(&tx, table)? {
                    cleared += tx.execute(
                        &format!("DELETE FROM {table} WHERE credential_id=?1"),
                        [credential_id],
                    )?;
                }
            }
            if crate::fence_rechecks::enabled(&tx)? {
                tx.execute(
                    "DELETE FROM fence_recheck WHERE credential_id=?1
                    AND kind IN ('account_route','non_quota_route','transient_route')",
                    [credential_id],
                )?;
            }

            Ok(cleared)
        })
        .map_err(err)
    }
}

/// A bound transient-backoff observation's effects, written as backoff rows.
/// It runs a `SELECT` over the observation and its `json_each` list, and
/// reads its target only for its upsert guard and `RETURNING`: a reviewed
/// own-row write for the block read trace (`group_reads::REVIEWED_OWN_ROW_WRITES`).
pub(crate) const INSERT_TRANSIENT_BACKOFFS: &str = "INSERT INTO account_transient_backoff
            SELECT json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id'),MIN(json_extract(e.value,'$.retry_at_ms'),o.received_at+3600000),
                o.received_at,json_array(o.observation_id,e.key),e.value
            FROM account_observation o,json_each(CASE WHEN json_valid(?2) THEN ?2 ELSE '[]' END) e
            WHERE o.observation_id=?1 AND o.kind='transient_backoff' AND o.bound=1 AND e.type='object'
                AND json_extract(e.value,'$.effect')='transient_backoff'
                AND json_type(e.value,'$.provider')='text' AND json_type(e.value,'$.account_id')='text'
                AND json_type(e.value,'$.retry_at_ms') IN ('integer','real')
            ON CONFLICT(provider,account_id) DO UPDATE SET retry_at_ms=excluded.retry_at_ms,evidence_at=excluded.evidence_at,
                evidence_key=excluded.evidence_key,effects_json=excluded.effects_json WHERE excluded.evidence_at>=account_transient_backoff.evidence_at
            RETURNING json_array(provider,account_id)";
