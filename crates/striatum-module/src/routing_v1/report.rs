//! `route.report` for `routing/v1`: how running on a decision went.
//!
//! Only the principal that selected a decision may report it; any other
//! caller is answered exactly as for a decision that never existed. The
//! first report owns a decision's effects. Usage is counted once per
//! decision and run; a repeated pair returns its first answer even if the
//! body differs. Reports without run attribution retain body idempotency.
//! Outcomes are audit
//! facts, not evidence that quota is exhausted. Eligibility and explicitly
//! size-shaped refusals restrict only the account that actually served, and
//! a failed report with `runner_code` `account_limit_exhausted`/`quota_exhausted`
//! or a future `reset_at_ms` keeps that account's route fenced until fresh
//! recovery evidence arrives. A reset schedules an extra check, not a fixed hold.
//! Reported usage joins the route's usage history untimed, with an absent count kept
//! unknown; a report without output adds no history row.

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::report::{Outcome, ReportAnswer, ReportRequest};
use serde_json::Value;
use striatum_core::{RouteOutcome, RouteStateStore};
use striatum_store::V1Recorded;

use super::{now_ms, provider_refusal, store_refusal, Caller, Deps};

mod attempt;
#[cfg(test)]
mod attempt_samples_tests;
#[cfg(test)]
mod attempt_tests;

const MEMBERS: &[&str] = &[
    "decision_id",
    "outcome",
    "runner_code",
    "credential_id",
    "reset_at_ms",
    "usage",
    "pool",
    "eligibility",
    "request_input_tokens",
    "refusal_class",
];

pub(crate) fn report(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<(Value, bool), Refusal> {
    report_at(params, caller, deps, now_ms())
}

pub(super) fn report_at(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    now: u64,
) -> Result<(Value, bool), Refusal> {
    let request = attempt::parse(params)?;
    let decision = deps
        .store
        .v1_decision(&request.decision_id)
        .map_err(store_refusal)?;
    if decision
        .as_ref()
        .is_none_or(|decision| decision.principal != caller.principal)
    {
        return Err(Refusal::unknown_decision());
    }
    let decision = decision.unwrap();
    if !super::engines::is_decision(deps, &request.decision_id).map_err(store_refusal)? {
        super::consumers::effective(&super::operator_config(deps))
            .admit(&decision.consumer, caller)?;
    }
    // Bodies are compared as decoded: the canonical encoding leaves absent
    // and null members out alike and fixes the member order.
    let body_json = serde_json::to_string(&request)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let effect_request = attempt::for_effects(&request, now);
    let credentials = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .snapshot();
    let quota = crate::ACCOUNT_QUOTA
        .get()
        .and_then(|q| q.lock().ok().map(|q| q.clone()))
        .unwrap_or_default();
    let account_ids = credentials.quota_account_ids(&quota);
    let search = deps
        .store
        .get_decision_by_id(&request.decision_id)
        .map_err(store_refusal)?
        .is_some_and(|decision| {
            serde_json::from_str::<Value>(&decision.requirements_json)
                .ok()
                .is_some_and(|params| params["kind"] == "search")
        });
    let mut effects = if search {
        attempt::search_effects(&effect_request, &decision, now)
    } else {
        attempt::account_effects(&effect_request, &credentials, |params| {
            prepare_account_facts(
                params,
                &decision,
                deps,
                now as i64,
                &credentials,
                &quota,
                &account_ids,
            )
        })?
    };
    let usage = request
        .usage
        .as_ref()
        .map(|usage| {
            prepare_usage(
                usage,
                &decision,
                request.credential_id.as_deref(),
                deps,
                crate::ACCOUNT_QUOTA.get(),
                now,
            )
        })
        .transpose()?
        .flatten();
    // Only success lifts the credential fence, never a neutral cancellation.
    if !search && effect_request.outcome == Outcome::Completed {
        let credential = deps
            .store
            .decision_provider_identity(&request.decision_id)
            .map_err(store_refusal)?
            .and_then(|identity| identity.credential_id)
            .or_else(|| request.credential_id.clone());
        if let Some(credential) = credential {
            effects.lift_credential = Some((decision.provider.clone(), credential));
        }
    }

    let delivery_key = match &request.usage {
        Some(usage) => usage
            .run_id
            .as_ref()
            .map(|run| format!("run:{run}"))
            .unwrap_or_else(|| "usage".into()),
        None => "outcome".into(),
    };
    let report_id = if delivery_key.starts_with("run:") {
        format!("report:{}:{delivery_key}", request.decision_id)
    } else {
        format!("report:{}", request.decision_id)
    };
    let answer = ReportAnswer {
        decision_id: request.decision_id.clone(),
        report_id: report_id.clone(),
        outcome: request.outcome.clone(),
    };
    let answer_json = serde_json::to_string(&answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let reconcile = effects.credential_refusal.is_some();
    let config = super::operator_config(deps);
    let standing = deps
        .store
        .v1_commit_report_with_continuation(
            &request.decision_id,
            &delivery_key,
            &report_id,
            &V1Recorded {
                body_json: body_json.clone(),
                answer_json,
            },
            ledger_outcome(&effect_request, now),
            effect_request
                .outcome
                .is_failure()
                .then(|| {
                    request
                        .runner_code
                        .clone()
                        .unwrap_or_else(|| request.outcome.to_string())
                })
                .as_deref(),
            effect_request.outcome.is_failure(),
            &effects,
            usage.as_ref(),
            move |tx| {
                if reconcile {
                    // The writer gives the report's transaction its working
                    // state, so these reads and the reconcile's decisions
                    // answer from it, with the report's own writes laid over.
                    let command = crate::account_state::AccountCommand::new(tx, None);
                    crate::account_selection::refresh_credential_quota_holds_in(
                        &command,
                        &account_ids,
                        &quota,
                        &config,
                        command.report_credential_refusals()?,
                        &command.report_route_fences(now as i64)?,
                        now as i64,
                    )?;
                }
                Ok(())
            },
            |tx, state, decision, now| {
                crate::account_state::AccountCommand::new(tx, state).use_continuation(decision, now)
            },
            now as i64,
        )
        .map_err(store_refusal)?;
    match standing {
        Some(first) => serde_json::from_str(&first.recorded.answer_json)
            .map(|answer| (answer, first.duplicate))
            .map_err(|error| store_refusal(format!("stored report answer undecodable: {error}"))),
        None => Err(Refusal::new(
            RefusalCode::ReportConflict,
            "a report with a different body was already recorded for this decision",
        )),
    }
}

fn prepare_account_facts(
    params: &Value,
    decision: &striatum_store::V1Decision,
    deps: &Deps<'_>,
    now: i64,
    credentials: &crate::broca_credentials::BrocaCredentials,
    quota: &crate::account_quota::AccountQuotaState,
    account_ids: &crate::broca_credentials::CredentialAccounts,
) -> Result<striatum_store::V1ReportEffects, Refusal> {
    let mut effects = striatum_store::V1ReportEffects::default();
    effects.reported_credential = params
        .get("credential_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    effects.quota_scope = Some(striatum_store::QuotaSuccessScope {
        family: super::operator_config(deps)
            .allocator_policy(&decision.provider)
            .family_for_model(&decision.model),
        pool: striatum_core::quota_extract::antigravity_pool_for_capacity(&format!(
            "{}/{}",
            decision.provider, decision.model
        ))
        .map(str::to_string),
    });
    let identity = deps
        .store
        .decision_provider_identity(&decision.decision_id)
        .map_err(store_refusal)?;
    let credential = params
        .get("credential_id")
        .and_then(Value::as_str)
        .or_else(|| {
            identity
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
        });
    let Some(credential) = credential else {
        return Ok(effects);
    };
    // Prepare from read-only snapshots. ClassContext::load also releases
    // reservations and reconciles fences, which must not run for a losing claim.
    let mut facts = deps.store.get_facts().map_err(store_refusal)?;
    let targets = credentials.catalog_targets(&facts);
    striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
    let Some(fact) = facts
        .iter()
        .find(|fact| fact.provider_id == decision.provider && fact.model_id == decision.model)
    else {
        return Ok(effects);
    };
    // Reports cannot turn an arbitrary credential into an account fact.
    let catalog = facts.iter().collect();
    let serves = |credential: &str| {
        credentials.listed_credentials().iter().any(|row| {
            row.id == credential
                && crate::broca_credentials::credential_serves_fact(
                    row,
                    fact,
                    &catalog,
                    credentials,
                )
        })
    };
    let quota_contradicted = |credential: &str| {
        crate::credential_fences::credential_account_id(account_ids, credential).is_some_and(
            |account| {
                crate::credential_fences::fresh_headroom_after_for_account(
                    quota,
                    &account,
                    i64::MIN,
                    now,
                    &super::operator_config(deps).allocator_policy(&decision.provider),
                )
            },
        )
    };
    let code = params
        .get("runner_code")
        .and_then(Value::as_str)
        .unwrap_or("");
    let size_quota = params
        .get("request_input_tokens")
        .and_then(Value::as_u64)
        .is_some()
        && code.to_ascii_lowercase().contains("size")
        && (code.contains("429") || code.contains("exhaust") || code.contains("quota"));
    // Quota evidence belongs to the serving identity, not its healthy siblings.
    // A reset schedules an extra check; a missing date does not erase a typed
    // quota refusal. Fresh headroom contradicts the quota interpretation and
    // retains the refusal as an independent non-quota wall instead.
    if params["outcome"] == "failed" && !size_quota {
        let reset = params.get("reset_at_ms").and_then(Value::as_i64);
        if reset.is_some_and(|reset| reset > now)
            || matches!(
                params["runner_code"].as_str(),
                Some("account_limit_exhausted" | "quota_exhausted")
            )
        {
            let served = identity
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
                .or(Some(credential))
                .filter(|served| serves(served));
            if let Some(served) = served {
                effects.route_fence = Some(striatum_store::AccountRouteFence {
                    provider: decision.provider.clone(),
                    credential_id: served.into(),
                    model: decision.model.clone(),
                    until_ms: reset.unwrap_or(0),
                    decision_id: decision.decision_id.clone(),
                    recorded_at: now,
                });
                let contradicted = quota_contradicted(served);
                if contradicted {
                    let mut fence = effects.route_fence.take().unwrap();
                    fence.until_ms =
                        now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
                    if let Some(reset) = reset.filter(|reset| *reset > now) {
                        fence.until_ms = fence.until_ms.min(reset);
                    }
                    effects.non_quota_route_fence = Some(fence);
                }
            }
        }
    }
    if !serves(credential) {
        return Ok(effects);
    }
    let fingerprint = match credentials.listing_fingerprint(credential) {
        crate::broca_credentials::ListingLookup::Listed(fingerprint) => Some(fingerprint),
        _ => None,
    };
    if let Some(eligibility) = params.get("eligibility").filter(|value| !value.is_null()) {
        effects.eligibility = Some(striatum_store::AccountEligibilityFact {
            provider: decision.provider.clone(),
            credential_id: credential.into(),
            model: if eligibility["scope"] == "account" {
                String::new()
            } else {
                decision.model.clone()
            },
            listing_fingerprint: fingerprint,
            code: eligibility["code"].as_str().unwrap_or("ineligible").into(),
            observed_at: now,
        });
        return Ok(effects);
    }
    if matches!(
        code,
        "ENTITLEMENT_ERROR"
            | "client_attestation_denied"
            | "billing_exhausted"
            | "credit_insufficient"
            | "insufficient_quota"
    ) || params["refusal_class"] == "credential_refused"
    {
        effects.credential_refusal = Some((
            striatum_core::route_state_store::CredentialRefusal {
                provider_id: decision.provider.clone(),
                credential_id: credential.into(),
                code: if code.is_empty() {
                    "credential_refused".into()
                } else {
                    code.into()
                },
            },
            fingerprint,
        ));
    }
    if let Some(tokens) = params.get("request_input_tokens").and_then(Value::as_u64) {
        let class = striatum_core::accounts::input_size_class(tokens);
        if params["outcome"] != "completed" && size_quota {
            effects.size_refusal = Some(striatum_store::AccountSizeRefusal {
                provider: decision.provider.clone(),
                credential_id: credential.into(),
                model: decision.model.clone(),
                size_class: class,
                expires_at: params
                    .get("reset_at_ms")
                    .and_then(Value::as_i64)
                    .filter(|reset| *reset > now)
                    .unwrap_or(0),
                observed_at: now,
            });
            if quota_contradicted(credential) {
                let mut fact = effects.size_refusal.take().unwrap();
                let cap =
                    now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
                fact.expires_at = if fact.expires_at > now {
                    fact.expires_at.min(cap)
                } else {
                    cap
                };
                effects.non_quota_size_refusal = Some(fact);
            }
        }
    }
    Ok(effects)
}

/// Usage retries share an identity only within their decision and run.
fn usage_row_id(decision_id: &str, run_id: Option<&str>) -> String {
    match run_id {
        Some(run) => format!("routing-v1-report:{decision_id}:run:{run}"),
        None => format!("routing-v1-report:{decision_id}"),
    }
}

/// Prepare usage for the atomic report write into the history table that
/// `route.record_usage` writes. A count the report leaves out stays unknown
/// (NULL), never zero. The history's output count cannot be unknown, so a
/// report without `output_tokens` adds no row: its counts stay only in the
/// stored report body. `output_tokens` includes `reasoning_tokens` on the
/// wire, as in the runner contract's usage; the history keeps visible output
/// with reasoning beside it. `duration_ms` is the run's wall clock, mostly
/// tool time on an agentic run, so the row is recorded untimed: it has no
/// duration and is never a speed sample. The timing members stay in the
/// stored report body.
fn prepare_usage(
    usage: &cortexkit_role_routing::report::Usage,
    decision: &striatum_store::V1Decision,
    reported_credential: Option<&str>,
    deps: &Deps<'_>,
    accounts: Option<&crate::account_quota::SharedAccountQuota>,
    now: u64,
) -> Result<Option<striatum_store::V1ReportUsage>, Refusal> {
    let Some(output) = usage.output_tokens else {
        return Ok(None);
    };
    let reasoning = usage.reasoning_tokens;
    let observed_ms = now as f64;
    let facts = striatum_core::NativeUsageFacts {
        message_id: usage_row_id(&decision.decision_id, usage.run_id.as_deref()),
        provider_id: decision.provider.clone(),
        model_id: decision.model.clone(),
        variant: decision.variant.clone(),
        agent: Some(decision.consumer.clone()),
        input_tokens: usage.input_tokens.map(|count| count as f64),
        output_tokens: output.saturating_sub(reasoning.unwrap_or(0)) as f64,
        reasoning_tokens: reasoning.map(|count| count as f64),
        cached_read_tokens: usage.cached_input_tokens.map(|count| count as f64),
        cache_write_tokens: usage.cache_write_tokens.map(|count| count as f64),
        created_ms: observed_ms,
        completed_ms: observed_ms,
        untimed: true,
        ..Default::default()
    };
    let Some(observation) =
        crate::model_resolution::usage_observation_from_store(facts, deps.store)
            .map_err(store_refusal)?
    else {
        return Ok(None);
    };
    let cost = usage
        .service_tier_served
        .as_deref()
        .or(usage.service_tier_applied.as_deref())
        .and_then(|tier| {
            let identity = deps
                .store
                .decision_provider_identity(&decision.decision_id)
                .ok()
                .flatten();
            let credential = identity
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
                .or(reported_credential)?;
            // Pricing also reads the store and account quota. Keep only owned
            // credential data here, never a guard across those other locks.
            let credentials = deps.broca_credentials.lock().ok()?.snapshot();
            #[cfg(test)]
            crate::account_quota::after_join_snapshot_for_test();
            let row = credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == credential)?;
            let mut fact = deps.store.get_facts().ok()?.into_iter().find(|fact| {
                fact.provider_id == decision.provider && fact.model_id == decision.model
            })?;
            credentials.price_for_method(
                &mut fact,
                row.auth_method.as_deref()?,
                &Default::default(),
            );
            let credits = accounts
                .and_then(|state| state.lock().ok())
                .and_then(|state| {
                    state
                        .readings
                        .readings
                        .values()
                        .filter(|reading| {
                            reading.account_id == row.account_id.as_deref().unwrap_or(&row.id)
                                && reading.tombstoned_at_ms.is_none()
                        })
                        .max_by_key(|reading| reading.received_at_ms)
                        .map(|reading| crate::service_tiers::drawing_credits(&reading.entry))
                })
                .unwrap_or(false);
            let rate = crate::service_tiers::plan_rate(
                &credentials.plan_prices,
                row,
                &fact.provider_id,
                tier,
                credits,
            )?;
            crate::service_tiers::price_fact(
                &credentials,
                &mut fact,
                tier,
                row.auth_method.as_deref() != Some("apikey"),
                rate,
            );
            crate::service_tiers::usage_cost(&fact, usage)
        });
    Ok(Some(striatum_store::V1ReportUsage {
        observation,
        tier_applied: usage.service_tier_applied.clone(),
        tier_served: usage.service_tier_served.clone(),
        cost,
    }))
}

/// A timed failure is still only an error. Confirmed exhaustion belongs to
/// admission-bound account observations, never a model outcome report.
fn ledger_outcome(request: &ReportRequest, _now_ms: u64) -> RouteOutcome {
    match &request.outcome {
        Outcome::Completed => RouteOutcome::Completed,
        Outcome::Cancelled => RouteOutcome::Cancelled,
        _ if !request.outcome.is_failure() => RouteOutcome::Interrupted,
        _ => RouteOutcome::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roster_refresh_and_tier_report_do_not_deadlock() {
        use std::sync::{mpsc, Arc, Barrier, Mutex};
        use std::time::Duration;

        let scratch = cortexkit_test_support::ScratchDir::new("roster-tier-report-locks");
        let store = Arc::new(
            striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
                "roster-tier-report-locks",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap(),
        );
        let fact = crate::tests::fact("provider", "model");
        store
            .upsert_fact(&fact, striatum_core::FactSource::Override)
            .unwrap();
        let mut credentials =
            crate::broca_credentials::BrocaCredentials::api_test_credentials(&[fact]);
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "roster-tier-report-locks".into(),
                credentials: vec![cortexkit_claustrum_client::ListedCredential {
                    id: "credential".into(),
                    account_id: Some("account".into()),
                    provider_ids: vec!["provider".into()],
                    auth_method: Some("apikey".into()),
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            1_700_000_000_000,
        );
        let credentials = Arc::new(Mutex::new(credentials));
        let accounts = Arc::new(Mutex::new(
            crate::account_quota::AccountQuotaState::default(),
        ));
        let config_path = scratch.path().join("routing.jsonc");
        crate::test_config::write(&config_path, "{}").unwrap();

        // Both production paths stop immediately after reading their first
        // cache. Retaining both original guards forms the ABBA cycle regardless
        // of which thread the scheduler runs first after the barrier.
        let barrier = Arc::new(Barrier::new(2));
        let (done, finished) = mpsc::channel();
        let roster = {
            let store = store.clone();
            let credentials = credentials.clone();
            let config_path = config_path.clone();
            let accounts = accounts.clone();
            let barrier = barrier.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                crate::account_quota::JOIN_SNAPSHOT_BARRIER
                    .with(|slot| *slot.borrow_mut() = Some(barrier));
                crate::account_quota::refresh_serving_methods(
                    &accounts,
                    &store,
                    &credentials,
                    &config_path,
                );
                done.send(()).unwrap();
            })
        };
        let report = std::thread::spawn(move || {
            crate::account_quota::JOIN_SNAPSHOT_BARRIER
                .with(|slot| *slot.borrow_mut() = Some(barrier));
            let quota_cache = Arc::new(Mutex::new(crate::QuotaCache::default()));
            let deps = Deps {
                quota_cache: &quota_cache,
                store: &store,
                routing_config_path: &config_path,
                operator_config_path: &config_path,
                broca_credentials: &credentials,
            };
            let decision = striatum_store::V1Decision {
                decision_id: "decision".into(),
                principal: "owner".into(),
                consumer: "mason".into(),
                allowed_account_class: Default::default(),
                project_id: None,
                provider: "provider".into(),
                model: "model".into(),
                variant: None,
                runner_module: "runner".into(),
                affinity: None,
                created_at_ms: 0,
                expires_at_ms: i64::MAX,
            };
            let usage = serde_json::from_value(serde_json::json!({
                "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0,
                "cached_input_tokens": 0, "cache_write_tokens": 0,
                "service_tier_served": "standard"
            }))
            .unwrap();
            let prepared = prepare_usage(
                &usage,
                &decision,
                Some("credential"),
                &deps,
                Some(&accounts),
                1_700_000_000_000,
            )
            .unwrap()
            .expect("usage must reach the tier pricing branch");
            assert_eq!(prepared.cost, Some(0.0));
            done.send(()).unwrap();
        });
        // Do not join a deadlocked thread. Each worker owns only this fixture's
        // caches, so a timeout fails this test without wedging other tests.
        for _ in 0..2 {
            finished.recv_timeout(Duration::from_secs(10))
                .expect("deadlock: roster refresh and tier report did not finish after the snapshot barrier");
        }
        roster.join().unwrap();
        report.join().unwrap();
    }

    fn request(outcome: &str, reset_at_ms: Option<u64>) -> ReportRequest {
        let request = ReportRequest::new("d", Outcome::parse(outcome));
        match reset_at_ms {
            Some(reset_at_ms) => request.with_reset_at_ms(reset_at_ms),
            None => request,
        }
    }

    #[test]
    fn declared_outcomes_follow_the_contract_penalty_matrix_in_the_ledger() {
        assert_eq!(
            ledger_outcome(&request("completed", None), 0),
            RouteOutcome::Completed
        );
        assert_eq!(
            ledger_outcome(&request("cancelled", None), 0),
            RouteOutcome::Cancelled
        );
        for outcome in [
            "contract_error",
            "session_error",
            "interrupted",
            "launch_failed",
        ] {
            assert_eq!(
                ledger_outcome(&request(outcome, None), 0),
                RouteOutcome::Interrupted
            );
        }
        for outcome in ["failed", "refused", "timed_out", "vanished"] {
            assert_eq!(
                ledger_outcome(&request(outcome, None), 0),
                RouteOutcome::Error,
                "{outcome}"
            );
        }
        assert_eq!(
            ledger_outcome(&request("refused", Some(5_500)), 1_000),
            RouteOutcome::Error
        );
    }
}
