//! One report claim and all its durable effects share the connection transaction.

use super::*;
use striatum_core::{route_state_store::CredentialRefusal, RouteOutcome};

#[derive(Clone, Default)]
pub struct V1ReportEffects {
    /// Identity-free route telemetry for a module that owns its credentials.
    pub route_state: Option<(
        String,
        String,
        striatum_core::route_state_store::RouteStateUpdate,
    )>,
    /// Explicit serving credential from the report, before identity fallback.
    pub reported_credential: Option<String>,
    /// Rejected and unknown outcomes do not settle a decision's probe.
    pub preserve_probe: bool,
    pub quota_scope: Option<crate::QuotaSuccessScope>,
    pub route_fence: Option<AccountRouteFence>,
    pub non_quota_route_fence: Option<AccountRouteFence>,
    pub eligibility: Option<AccountEligibilityFact>,
    pub credential_refusal: Option<(CredentialRefusal, Option<String>)>,
    pub size_refusal: Option<AccountSizeRefusal>,
    pub non_quota_size_refusal: Option<AccountSizeRefusal>,
    pub lift_credential: Option<(String, String)>,
}

#[derive(Clone)]
pub struct V1ReportUsage {
    pub observation: UsageObservation,
    pub tier_applied: Option<String>,
    pub tier_served: Option<String>,
    pub cost: Option<f64>,
}

/// The committed answer and whether the delivery already existed. The duplicate
/// flag comes from the transaction's claim, not a separate preflight read.
pub struct V1ReportOutcome {
    pub recorded: V1Recorded,
    pub duplicate: bool,
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
        reconcile: impl FnMut(&AccountTx<'_>) -> RouteStateResult<()> + Send + 'static,
        now: i64,
    ) -> RouteStateResult<Option<V1Recorded>> {
        // Its continuation use runs SQL alone; the module reports through
        // `v1_commit_report_with_continuation` with its state.
        self.refuse_state_blind_admission_write("v1_commit_report")?;
        self.v1_commit_report_with_continuation(
            decision_id,
            delivery_key,
            report_id,
            recorded,
            outcome,
            error,
            failed,
            effects,
            usage,
            reconcile,
            |account, _, decision_id, now| account.use_continuation(decision_id, now),
            now,
        )
        .map(|outcome| outcome.map(|outcome| outcome.recorded))
    }

    /// As [`Self::v1_commit_report`], with the first accepted report's
    /// continuation use delegated to `use_continuation`. It receives the
    /// writer's opaque working state, so the module can keep its in-memory
    /// admission state in step with the rows that use changes.
    #[allow(clippy::too_many_arguments)]
    pub fn v1_commit_report_with_continuation(
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
        // Both may run again if the writer re-runs the report's batch.
        mut reconcile: impl FnMut(&AccountTx<'_>) -> RouteStateResult<()> + Send + 'static,
        mut use_continuation: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
                &str,
                i64,
            ) -> RouteStateResult<()>
            + Send
            + 'static,
        now: i64,
    ) -> RouteStateResult<Option<V1ReportOutcome>> {
        let decision_id = decision_id.to_owned();
        let delivery_key = delivery_key.to_owned();
        let report_id = report_id.to_owned();
        let recorded = recorded.clone();
        let error = error.map(str::to_owned);
        let effects = effects.clone();
        let usage = usage.cloned();
        let mut command =
            move |account: &AccountTx<'_>,
                  state: Option<&mut (dyn crate::AccountWorkingState + 'static)>| {
                let decision_id = decision_id.as_str();
                let delivery_key = delivery_key.as_str();
                let report_id = report_id.as_str();
                let error = error.as_deref();
                let effects = &effects;
                let usage = usage.as_ref();
                let tx = account.conn;
                (|| -> rusqlite::Result<(Option<V1ReportOutcome>, bool)> {
            let standing = tx.query_row(
                "SELECT body_json, answer_json FROM routing_v1_report_delivery
                 WHERE decision_id=?1 AND delivery_key=?2",
                params![decision_id, delivery_key],
                |row| Ok(V1Recorded { body_json: row.get(0)?, answer_json: row.get(1)? }),
            ).optional()?;
            if let Some(standing) = standing {
                return Ok((if delivery_key.starts_with("run:") || standing.body_json == recorded.body_json {
                     Some(V1ReportOutcome { recorded: standing, duplicate: true })
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
                use_continuation(account, state, decision_id, now).map_err(sql_error)?;
                let settle = account.probe_report_matches(
                    decision_id, effects.reported_credential.as_deref(), effects.preserve_probe).map_err(sql_error)?;
                if settle { apply_account_effects(account, effects, now)?; }
                if settle && failed {
                    account.fail_authorized_probe(decision_id, now,
                        striatum_core::fence_lifetimes::probe_is_inconclusive(outcome,error)).map_err(sql_error)?;
                } else if settle && striatum_core::fence_lifetimes::probe_is_inconclusive(outcome,error) {
                    let (provider,model):(Option<String>,Option<String>)=tx.query_row(
                        "SELECT selected_provider_id,selected_model_id FROM model_route_decisions WHERE id=?1",
                        [decision_id],|row|Ok((row.get(0)?,row.get(1)?)))?;
                    if let (Some(provider),Some(model))=(provider,model) {
                        account.release_inconclusive_probe(decision_id,&provider,&model,now).map_err(sql_error)?;
                    }
                }
                if settle { reconcile(account).map_err(sql_error)?; }
                if failed {
                    tx.execute("UPDATE routing_v1_affinity SET ended_at_ms=?2 WHERE decision_id=?1 AND ended_at_ms IS NULL",
                        params![decision_id, now])?;
                }
                report_outcome(&tx, decision_id, outcome, error, now)?;
            }
            // Core can reuse one routing decision for several runs, and each
            // run reports separately under a `run:` delivery key. Each new
            // delivery settles that run's own test request, so healing keys on
            // the delivery rather than on the decision's first recorded outcome.
            if outcome == RouteOutcome::Completed && !effects.preserve_probe {
                account.heal_report_run(decision_id, delivery_key.strip_prefix("run:"), effects, first, now)
                    .map_err(sql_error)?;
            }
            if let Some(usage) = usage { insert_usage(&tx, decision_id, usage)?; }
            #[cfg(test)]
            report_crash("before_commit");
             Ok((Some(V1ReportOutcome { recorded: recorded.clone(), duplicate: false }), usage.is_some()))
            })().map_err(err)
            };
        let result = if self.is_private_account_copy() {
            self.account_command(AccountCommandKind::Report, move |account| {
                command(account, None)
            })?
        } else {
            self.enqueue_account_state_command(AccountCommandKind::Report, command)?
                .wait()?
        };
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
    /// Settle success using the admission's identity, `committed_at` cutoff and
    /// exact probe proof. Return the origins actually cleared for observation effects.
    pub fn heal_admission_success(
        &self,
        admission: &AccountAdmissionRow,
        size: Option<u32>,
        decision: Option<&str>,
        scope: Option<&crate::QuotaSuccessScope>,
        now: i64,
    ) -> RouteStateResult<Vec<crate::FenceRecheck>> {
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let owner = serde_json::json!([
            "account_admission",
            admission.principal,
            admission.admission_id
        ])
        .to_string();
        let origins: Vec<_> = self
            .fence_rechecks()?
            .into_iter()
            .filter(|origin| {
                origin.applies(
                    &admission.provider,
                    &admission.account_id,
                    &admission.credential_id,
                    &admission.model,
                ) && origin
                    .principal
                    .as_deref()
                    .is_none_or(|p| p == admission.principal)
                    && origin
                        .admission_id
                        .as_deref()
                        .is_none_or(|id| id == admission.admission_id)
                    && origin
                        .size_class
                        .is_none_or(|class| Some(class as u32) == size)
            })
            .collect();
        // Pool-scoped fences require the success scope to cover their pool
        // and model. Whole-account fences pass the scope to the probe healer,
        // which proves only the recovery-set pools the request used.
        let mut captured = Vec::new();
        for origin in &origins {
            if origin.kind == "account" {
                let key: Vec<serde_json::Value> =
                    serde_json::from_str(&origin.origin_key).map_err(err)?;
                let pool: Option<String> = match self.working_blocks()? {
                    // Only active fences are held; one already cleared has
                    // nothing left to heal.
                    Some(blocks) => match key[0].as_str().and_then(|id| blocks.account_fence(id)) {
                        Some((_, pool, _)) => pool.map(str::to_owned),
                        None => continue,
                    },
                    None => self
                        .conn
                        .query_row(
                            "SELECT pool_key_json FROM account_fence WHERE fence_id=?1",
                            [key[0].as_str()],
                            |row| row.get(0),
                        )
                        .map_err(err)?,
                };
                if let Some(pool) = pool {
                    let pool = serde_json::from_str(&pool).map_err(err)?;
                    if !scope.is_some_and(|scope| scope.used(&pool, &admission.model)) {
                        continue;
                    }
                }
            }
            captured.push(origin.clone());
        }
        self.heal_report_probe(
            &owner,
            decision,
            true,
            &admission.model,
            admission.committed_at,
            &captured,
            scope,
            now,
        )?;
        let serving: Vec<_> = self
            .fence_rechecks()?
            .into_iter()
            .filter(|origin| {
                origin.principal.is_none()
                    && origin.evidence_at < admission.committed_at
                    && origins.iter().any(|scoped| {
                        scoped.kind == origin.kind
                            && scoped.origin_key == origin.origin_key
                            && scoped.evidence == origin.evidence
                    })
            })
            .collect();
        self.heal_origins(
            &serving,
            &admission.model,
            admission.committed_at,
            now,
            scope,
            false,
        )?;
        let remaining = self.fence_rechecks()?;
        Ok(origins
            .into_iter()
            .filter(|origin| {
                !remaining.iter().any(|other| {
                    other.kind == origin.kind
                        && other.origin_key == origin.origin_key
                        && other.evidence == origin.evidence
                })
            })
            .collect())
    }

    fn heal_report_run(
        &self,
        decision: &str,
        run: Option<&str>,
        effects: &V1ReportEffects,
        first: bool,
        now: i64,
    ) -> RouteStateResult<()> {
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        let admissions = self.admissions_for_run(decision, run)?;
        if admissions.len() > 1 {
            tracing::warn!(
                decision,
                run,
                "completed report has ambiguous admissions; no fences healed"
            );
            return Ok(());
        }
        if let Some((admission, size)) = admissions.into_iter().next() {
            if effects
                .reported_credential
                .as_deref()
                .is_some_and(|id| id != admission.credential_id)
            {
                tracing::warn!(
                    decision,
                    run,
                    "completed report credential differs from admission; no fences healed"
                );
                return Ok(());
            }
            self.heal_admission_success(
                &admission,
                size,
                Some(decision),
                effects.quota_scope.as_ref(),
                now,
            )?;
        } else {
            // A decision without an account admission still owns its selection
            // probe. The decision clock is the conservative launch bound when
            // no admission supplies a run-specific clock.
            let (model, identity, at): (String, Option<String>, i64) = self.conn.query_row(
                "SELECT selected_model_id,provider_identity_json,COALESCE((SELECT created_at_ms FROM routing_v1_decision
                 WHERE decision_id=?1),created_at) FROM model_route_decisions WHERE id=?1",
                [decision], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).map_err(err)?;
            if self.probe_report_matches(decision, effects.reported_credential.as_deref(), false)? {
                let authorized: bool = match self.working_blocks()? {
                    Some(blocks) => !blocks.authorizations_of(decision).is_empty(),
                    None => self
                        .conn
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM fence_probe_authorization WHERE owner=?1)",
                            [decision],
                            |row| row.get(0),
                        )
                        .map_err(err)?,
                };
                self.heal_report_probe(
                    decision,
                    None,
                    false,
                    &model,
                    at,
                    &self.fence_rechecks()?,
                    effects.quota_scope.as_ref(),
                    now,
                )?;
                // Only the first report may heal older matching serving evidence
                // without a probe authorization. If this decision held one (a
                // `fence_probe_authorization` row it owns), only
                // `heal_report_probe` above may settle it. That requires the row's
                // evidence (the refusal that started the block) and its claim
                // generation (which numbered test request this was) to still match
                // the block's recheck row, with the claim made no later than the
                // decision's creation. Ordinary serving recovery must not bypass
                // those checks.
                if authorized || !first {
                    return Ok(());
                }
                let identity = identity.and_then(|raw| {
                    serde_json::from_str::<striatum_core::route_state_store::ProviderOutcome>(&raw)
                        .ok()
                });
                if let Some((provider, credential)) = &effects.lift_credential {
                    if identity
                        .as_ref()
                        .and_then(|identity| identity.credential_id.as_deref())
                        .is_none_or(|id| id == credential)
                    {
                        let account = identity.as_ref().map_or(credential.as_str(), |identity| {
                            identity.account_ref.as_str()
                        });
                        self.heal_serving_fences_in_scope(
                            provider,
                            account,
                            credential,
                            &model,
                            None,
                            at,
                            now,
                            effects.quota_scope.as_ref(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn record_non_quota_size_refusal(
        &self,
        fact: &AccountSizeRefusal,
        now: i64,
    ) -> RouteStateResult<()> {
        self.conn.execute("INSERT INTO account_non_quota_size_refusal VALUES (?1,?2,?3,?4,?5,?6)
            ON CONFLICT(provider,credential_id,model,size_class) DO UPDATE SET expires_at=excluded.expires_at,observed_at=excluded.observed_at
            WHERE excluded.observed_at>=account_non_quota_size_refusal.observed_at",
            params![fact.provider,fact.credential_id,fact.model,fact.size_class,fact.expires_at.min(now.saturating_add(3_600_000)),fact.observed_at]).map_err(err)?;
        crate::fence_rechecks::register_on(
            self,
            "non_quota_size",
            &serde_json::json!([
                fact.provider,
                fact.credential_id,
                fact.model,
                fact.size_class
            ])
            .to_string(),
        )
        .map_err(err)
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
            .map_err(err)?;
        crate::fence_rechecks::register_on(
            self,
            if kind == "transient" {
                "transient_route"
            } else {
                "non_quota_route"
            },
            &serde_json::json!([fence.provider, fence.credential_id, fence.model]).to_string(),
        )
        .map_err(err)
    }
    /// Record a credential refusal in the same transaction as its observation.
    pub fn record_credential_refusal(
        &self,
        refusal: &striatum_core::route_state_store::CredentialRefusal,
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

    /// Delete a credential refusal that a decision read as `fence`: keyed and
    /// naming every column, so it fails with [`crate::block_state::diverged`]
    /// if SQL holds another refusal for the credential.
    pub fn clear_held_entitlement_fence(
        &self,
        fence: &striatum_core::route_state_store::CredentialEntitlementFence,
    ) -> RouteStateResult<()> {
        let changed = self.conn.execute("DELETE FROM credential_entitlement_fences WHERE provider_id=?1 AND credential_id=?2
            AND code=?3 AND listing_fingerprint IS ?4 AND observed_at_ms=?5",
            params![fence.provider_id,fence.credential_id,fence.code,fence.listing_fingerprint,fence.observed_at_ms]).map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!(
                "clear of credential refusal {} {}",
                fence.provider_id, fence.credential_id
            ),
        )
        .map_err(err)
    }

    pub fn report_credential_refusals(
        &self,
    ) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
        let mut statement=self.conn.prepare("SELECT provider_id,credential_id,code,listing_fingerprint,observed_at_ms FROM credential_entitlement_fences ORDER BY provider_id,credential_id").map_err(err)?;
        let rows = statement
            .query_map([], |r| {
                Ok(
                    striatum_core::route_state_store::CredentialEntitlementFence {
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
    if let Some((route, capacity, update)) = &effects.route_state {
        tx.update_route_state(route, capacity, update)
            .map_err(sql_error)?;
    }
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
        let changed = conn.execute(
            "INSERT INTO credential_entitlement_fences (provider_id,credential_id,code,listing_fingerprint,observed_at_ms)
             VALUES (?1,?2,?3,?4,?5) ON CONFLICT(provider_id,credential_id) DO UPDATE SET
              code=excluded.code, observed_at_ms=MAX(credential_entitlement_fences.observed_at_ms,excluded.observed_at_ms),
              listing_fingerprint=CASE WHEN credential_entitlement_fences.code='signed_out' AND excluded.code!='signed_out'
                THEN excluded.listing_fingerprint ELSE COALESCE(credential_entitlement_fences.listing_fingerprint,excluded.listing_fingerprint) END
              WHERE excluded.code!='signed_out' OR credential_entitlement_fences.code='signed_out'",
            params![refusal.provider_id, refusal.credential_id, refusal.code, fingerprint, now],
        )?;
        // A sign-in report must not replace or postpone an independent refusal.
        if changed != 0 && crate::fence_rechecks::enabled(conn)? {
            conn.execute("UPDATE fence_recheck SET claim_owner=NULL,classified=0 WHERE kind='credential' AND provider=?1 AND credential_id=?2",params![refusal.provider_id,refusal.credential_id])?;
            crate::fence_rechecks::register_on(
                tx,
                "credential",
                &serde_json::json!([refusal.provider_id, refusal.credential_id]).to_string(),
            )?;
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
