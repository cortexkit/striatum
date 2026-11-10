//! One report claim and all its durable effects share the connection transaction.

use super::*;
use prefrontal_routing_core::{route_state_store::CredentialRefusal, RouteOutcome};

#[derive(Default)]
pub struct V1ReportEffects {
    pub quota_scope: Option<crate::QuotaSuccessScope>,
    pub route_fence: Option<AccountRouteFence>,
    pub non_quota_route_fence: Option<AccountRouteFence>,
    pub eligibility: Option<AccountEligibilityFact>,
    pub credential_refusal: Option<(CredentialRefusal, Option<String>)>,
    pub size_refusal: Option<AccountSizeRefusal>,
    pub non_quota_size_refusal: Option<AccountSizeRefusal>,
    pub lift_credential: Option<(String, String)>,
}

pub struct V1ReportUsage {
    pub observation: UsageObservation,
    pub tier_applied: Option<String>,
    pub tier_served: Option<String>,
    pub cost: Option<f64>,
}

impl SqliteRouteStateStore {
    /// `None` is a conflict. Run-attributed retries return the standing answer
    /// regardless of their new body; the other identities compare canonical bodies.
    pub fn v1_commit_report(
        &self,
        decision_id: &str,
        delivery_key: &str,
        report_id: &str,
        recorded: &V1Recorded,
        outcome: RouteOutcome,
        error: Option<&str>,
        failed: bool,
        effects: &V1ReportEffects,
        usage: Option<&V1ReportUsage>,
        reconcile: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<()>,
        now: i64,
    ) -> RouteStateResult<Option<V1Recorded>> {
        let result = self.store.with_conn(|conn: &Connection| {
            let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
            let standing = tx.query_row(
                "SELECT body_json, answer_json FROM routing_v1_report_delivery
                 WHERE decision_id=?1 AND delivery_key=?2",
                params![decision_id, delivery_key],
                |row| Ok(V1Recorded { body_json: row.get(0)?, answer_json: row.get(1)? }),
            ).optional()?;
            if let Some(standing) = standing {
                return Ok((if delivery_key.starts_with("run:") || standing.body_json == recorded.body_json {
                    Some(standing)
                } else { None }, false));
            }
            let incompatible: bool = if delivery_key == "usage" || delivery_key == "outcome" || delivery_key.starts_with("run:") {
                tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM routing_v1_report_delivery
                     WHERE decision_id=?1 AND ((?2='usage' AND delivery_key!='usage')
                         OR (?2!='usage' AND delivery_key='usage')))",
                    params![decision_id, delivery_key], |row| row.get(0),
                )?
            } else { false };
            if incompatible { return Ok((None, false)); }
            tx.execute(
                "INSERT INTO routing_v1_report_delivery VALUES (?1,?2,?3,?4,?5,?6)",
                params![decision_id, delivery_key, report_id, recorded.body_json, recorded.answer_json, now],
            )?;
            let first = tx.execute(
                "INSERT INTO routing_v1_report VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT(decision_id) DO NOTHING",
                params![decision_id, report_id, recorded.body_json, recorded.answer_json, now],
            )? == 1;
            if first {
                let account = AccountTx { conn: &tx };
                account.use_continuation(decision_id, now).map_err(sql_error)?;
                let admitted_at:i64=tx.query_row("SELECT COALESCE((SELECT created_at_ms FROM routing_v1_decision WHERE decision_id=?1),created_at) FROM model_route_decisions WHERE id=?1",[decision_id],|row|row.get(0))?;
                apply_account_effects(&account, effects, now)?;
                if outcome==RouteOutcome::Completed {
                    if let Some((provider,credential))=&effects.lift_credential {
                        let (model,identity):(String,Option<String>)=tx.query_row("SELECT selected_model_id,provider_identity_json FROM model_route_decisions WHERE id=?1",[decision_id],|row|Ok((row.get(0)?,row.get(1)?)))?;
                        let identity=identity.and_then(|raw|serde_json::from_str::<prefrontal_routing_core::route_state_store::ProviderOutcome>(&raw).ok());
                        if identity.as_ref().and_then(|identity|identity.credential_id.as_deref()).is_none_or(|id|id==credential) {
                            account.heal_authorized_probe(decision_id,provider,&model,now).map_err(sql_error)?;
                            let account_id=identity.as_ref().map_or(credential.as_str(),|identity|identity.account_ref.as_str());
                            let size=account.size_admission_for_model_decision(decision_id,provider,credential,&model).map_err(sql_error)?;
                            account.heal_serving_fences_in_scope(provider,account_id,credential,&model,size.map(|(class,_)|class),size.map_or(admitted_at,|(_,at)|at),now,effects.quota_scope.as_ref()).map_err(sql_error)?;
                        }
                    } else {
                        let (provider,model,identity):(Option<String>,Option<String>,Option<String>)=tx.query_row(
                            "SELECT selected_provider_id,selected_model_id,provider_identity_json FROM model_route_decisions WHERE id=?1",
                            [decision_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
                        let pinned=identity.and_then(|raw|serde_json::from_str::<prefrontal_routing_core::route_state_store::ProviderOutcome>(&raw).ok()).and_then(|identity|identity.credential_id);
                        if let (None,Some(provider),Some(model))=(pinned,provider,model) {
                            account.heal_authorized_probe(decision_id,&provider,&model,now).map_err(sql_error)?;
                        }
                    }
                }
                reconcile(&account).map_err(sql_error)?;
                if failed {
                    tx.execute("UPDATE routing_v1_affinity SET ended_at_ms=?2 WHERE decision_id=?1 AND ended_at_ms IS NULL",
                        params![decision_id, now])?;
                }
                report_outcome(&tx, decision_id, outcome, error, now)?;
            }
            if let Some(usage) = usage { insert_usage(&tx, decision_id, usage)?; }
            #[cfg(test)]
            report_crash("before_commit");
            tx.commit()?;
            Ok((Some(recorded.clone()), usage.is_some()))
        }).map_err(err)?;
        #[cfg(test)]
        report_crash("after_commit");
        // The only in-memory change is a cache invalidation, after commit.
        if result.1 {
            self.usage_generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok(result.0)
    }
}

impl AccountTx<'_> {
    pub fn record_non_quota_size_refusal(
        &self,
        fact: &AccountSizeRefusal,
        now: i64,
    ) -> RouteStateResult<()> {
        self.conn.execute("INSERT INTO account_non_quota_size_refusal VALUES (?1,?2,?3,?4,?5,?6)
            ON CONFLICT(provider,credential_id,model,size_class) DO UPDATE SET expires_at=excluded.expires_at,observed_at=excluded.observed_at
            WHERE excluded.observed_at>=account_non_quota_size_refusal.observed_at",
            params![fact.provider,fact.credential_id,fact.model,fact.size_class,fact.expires_at.min(now.saturating_add(3_600_000)),fact.observed_at]).map(|_|()).map_err(err)
    }
    pub fn record_non_quota_route_fence(&self, fence: &AccountRouteFence) -> RouteStateResult<()> {
        self.record_account_route_backoff(fence, "billing")
    }
    pub(crate) fn record_account_route_backoff(
        &self,
        fence: &AccountRouteFence,
        kind: &str,
    ) -> RouteStateResult<()> {
        self.conn.execute("INSERT INTO account_non_quota_route_fence VALUES (?1,?2,?3,?4,?5,?6,?6,?7)
            ON CONFLICT(provider,credential_id,model,wall_kind) DO UPDATE SET until_ms=excluded.until_ms,
            last_reported_at=MAX(account_non_quota_route_fence.last_reported_at,excluded.last_reported_at),decision_id=excluded.decision_id",
            params![fence.provider,fence.credential_id,fence.model,fence.until_ms.min(fence.recorded_at.saturating_add(3_600_000)),fence.decision_id,fence.recorded_at,kind]).map_err(err)?;
        self.conn
            .execute(
                "UPDATE fence_recheck SET claim_owner=NULL WHERE kind=?1 AND origin_key=?2",
                params![
                    if kind == "transient" {
                        "transient_route"
                    } else {
                        "non_quota_route"
                    },
                    serde_json::json!([fence.provider, fence.credential_id, fence.model])
                        .to_string()
                ],
            )
            .map(|_| ())
            .map_err(err)
    }
    /// Record a credential refusal in the same transaction as its observation.
    pub fn record_credential_refusal(
        &self,
        refusal: &prefrontal_routing_core::route_state_store::CredentialRefusal,
        fingerprint: Option<&str>,
        now: i64,
    ) -> RouteStateResult<()> {
        let effects = V1ReportEffects {
            credential_refusal: Some((refusal.clone(), fingerprint.map(str::to_owned))),
            ..Default::default()
        };
        apply_account_effects(self, &effects, now).map_err(err)
    }

    pub fn clear_credential_entitlement_fence(
        &self,
        provider: &str,
        credential: &str,
    ) -> RouteStateResult<()> {
        self.conn.execute("DELETE FROM credential_entitlement_fences WHERE provider_id=?1 AND credential_id=?2",params![provider,credential]).map(|_|()).map_err(err)
    }

    pub fn report_credential_refusals(
        &self,
    ) -> RouteStateResult<Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>>
    {
        let mut statement=self.conn.prepare("SELECT provider_id,credential_id,code,listing_fingerprint,observed_at_ms FROM credential_entitlement_fences ORDER BY provider_id,credential_id").map_err(err)?;
        let rows = statement
            .query_map([], |r| {
                Ok(
                    prefrontal_routing_core::route_state_store::CredentialEntitlementFence {
                        provider_id: r.get(0)?,
                        credential_id: r.get(1)?,
                        code: r.get(2)?,
                        listing_fingerprint: r.get(3)?,
                        observed_at_ms: r.get(4)?,
                    },
                )
            })
            .map_err(err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(err)
    }

    pub fn report_route_fences(&self, _now: i64) -> RouteStateResult<Vec<AccountRouteFence>> {
        let mut statement=self.conn.prepare("SELECT provider,credential_id,model,until_ms,decision_id,recorded_at FROM account_route_fence WHERE wall_class='quota' ORDER BY provider,credential_id,model").map_err(err)?;
        let rows = statement
            .query_map([], |r| {
                Ok(AccountRouteFence {
                    provider: r.get(0)?,
                    credential_id: r.get(1)?,
                    model: r.get(2)?,
                    until_ms: r.get(3)?,
                    decision_id: r.get(4)?,
                    recorded_at: r.get(5)?,
                })
            })
            .map_err(err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(err)
    }
}

#[cfg(test)]
fn report_crash(point: &str) {
    if std::env::var("ROUTING_REPORT_CRASH").ok().as_deref() == Some(point) {
        std::process::exit(86);
    }
}

fn sql_error(error: RouteStateStoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(error.to_string())))
}

fn apply_account_effects(
    tx: &AccountTx<'_>,
    effects: &V1ReportEffects,
    now: i64,
) -> rusqlite::Result<()> {
    let conn = tx.conn;
    if let Some(fence) = &effects.non_quota_route_fence {
        tx.record_non_quota_route_fence(fence).map_err(sql_error)?;
    }
    if let Some(fence) = &effects.route_fence {
        conn.execute(
            "INSERT INTO account_route_fence (provider,credential_id,model,until_ms,decision_id,recorded_at,last_reported_at,wall_class)
             VALUES (?1,?2,?3,?4,?5,?6,?6,'quota') ON CONFLICT(provider,credential_id,model) DO UPDATE SET
             until_ms=CASE WHEN excluded.recorded_at>=COALESCE(account_route_fence.last_reported_at,account_route_fence.recorded_at)
               THEN excluded.until_ms ELSE account_route_fence.until_ms END,
             wall_class='quota', recorded_at=MIN(account_route_fence.recorded_at,excluded.recorded_at),
             last_reported_at=MAX(COALESCE(account_route_fence.last_reported_at,account_route_fence.recorded_at),excluded.recorded_at)",
            params![fence.provider, fence.credential_id, fence.model, fence.until_ms, fence.decision_id, fence.recorded_at],
        )?;
        conn.execute("UPDATE fence_recheck SET claim_owner=NULL WHERE kind='account_route' AND origin_key=?1",[serde_json::json!([fence.provider,fence.credential_id,fence.model]).to_string()])?;
    }
    if let Some(fact) = &effects.eligibility {
        tx.record_eligibility(fact).map_err(sql_error)?;
    }
    if let Some((refusal, fingerprint)) = &effects.credential_refusal {
        conn.execute(
            "INSERT INTO credential_entitlement_fences (provider_id,credential_id,code,listing_fingerprint,observed_at_ms)
             VALUES (?1,?2,?3,?4,?5) ON CONFLICT(provider_id,credential_id) DO UPDATE SET
             code=excluded.code, observed_at_ms=MAX(credential_entitlement_fences.observed_at_ms,excluded.observed_at_ms),
             listing_fingerprint=COALESCE(credential_entitlement_fences.listing_fingerprint,excluded.listing_fingerprint)",
            params![refusal.provider_id, refusal.credential_id, refusal.code, fingerprint, now],
        )?;
        if crate::fence_rechecks::enabled(conn)? {
            conn.execute("UPDATE fence_recheck SET claim_owner=NULL,classified=0 WHERE kind='credential' AND provider=?1 AND credential_id=?2",params![refusal.provider_id,refusal.credential_id])?;
        }
    }
    if let Some(fact) = &effects.size_refusal {
        conn.execute("INSERT INTO account_size_refusal VALUES (?1,?2,?3,?4,?5,?6)
            ON CONFLICT(provider,credential_id,model,size_class) DO UPDATE SET expires_at=excluded.expires_at,observed_at=excluded.observed_at
            WHERE excluded.observed_at>=account_size_refusal.observed_at",
            params![fact.provider, fact.credential_id, fact.model, fact.size_class, fact.expires_at, fact.observed_at])?;
    }
    if let Some(fact) = &effects.non_quota_size_refusal {
        tx.record_non_quota_size_refusal(fact, now)
            .map_err(sql_error)?;
    }
    Ok(())
}

fn insert_usage(
    conn: &Connection,
    decision_id: &str,
    usage: &V1ReportUsage,
) -> rusqlite::Result<()> {
    let obs = &usage.observation;
    conn.execute(
        "INSERT INTO model_route_usage (id,task_id,route_key,capacity_key,provider_id,input_tokens,output_tokens,
         duration_ms,observed_at,reasoning_tokens,cached_read_tokens,cache_write_tokens,workload_class,account_key,task_workload)
         SELECT ?1,task_id,?2,?3,?4,?5,?6,NULL,?7,?8,?9,?10,?11,?12,
           CASE WHEN task_id IS NOT NULL THEN COALESCE(consumer,selected_workload,target_agent) END
         FROM model_route_decisions WHERE id=?13 ON CONFLICT(id) DO NOTHING",
        params![obs.id, obs.route_key, obs.capacity_key, obs.provider_id,
            obs.input_tokens.and_then(stored_token_count), stored_output_tokens(obs.output_tokens),
            floored_i64(obs.observed_at), obs.reasoning_tokens.and_then(stored_token_count),
            obs.cached_read_tokens.and_then(stored_token_count), obs.cache_write_tokens.and_then(stored_token_count),
            stored_workload_class(obs.workload_class.as_deref()), obs.account_key, decision_id],
    )?;
    let price_status = if usage.tier_served.is_some() && usage.cost.is_none() {
        Some("served_unpriced")
    } else if usage.tier_served.is_some() {
        Some("confirmed")
    } else if usage.tier_applied.is_some() {
        Some("unconfirmed_estimate")
    } else {
        None
    };
    conn.execute(
        "UPDATE model_route_usage SET service_tier_applied=?2, service_tier_served=?3,
        service_tier_price_status=?4, service_tier_cost_usd=?5 WHERE id=?1",
        params![
            obs.id,
            usage.tier_applied,
            usage.tier_served,
            price_status,
            usage.cost
        ],
    )?;
    Ok(())
}

fn report_outcome(
    conn: &Connection,
    decision_id: &str,
    outcome: RouteOutcome,
    error: Option<&str>,
    now: i64,
) -> rusqlite::Result<()> {
    // Reports preserve the run audit without creating model-health or quota
    // evidence. The first-report claim updates affinity separately.
    conn.execute(
        "UPDATE model_route_decisions SET outcome=?2,outcome_at=?3,outcome_error=?4 WHERE id=?1",
        params![decision_id, outcome.as_str(), now, error],
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests;
