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
//! a failure with a future reset closes only the decision's route on that
//! account until the reset, for at most 24 hours. Usage the report carries
//! joins the route's usage history untimed, with an absent count kept
//! unknown; a report without output adds no history row.

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::report::{
    parse_report_request_serving, Outcome, ReportAnswer, ReportRequest, RESET_FENCE_CAP_MS,
};
use prefrontal_routing_core::{RouteOutcome, RouteStateStore};
use prefrontal_routing_store::V1Recorded;
use serde_json::Value;

use super::{now_ms, provider_refusal, store_refusal, Caller, Deps};

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

pub(super) fn report(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    report_at(params, caller, deps, now_ms())
}

pub(super) fn report_at(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    now: u64,
) -> Result<Value, Refusal> {
    super::refuse_unknown_members(params, MEMBERS)?;
    super::shapes::closed(params, "eligibility", &["scope", "code"])?;
    let request = parse_report_request_serving(params, super::describe::CAPABILITIES)?;
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
    let mut effects = prepare_account_facts(params, &decision, deps, now as i64)?;
    let usage = request
        .usage
        .as_ref()
        .map(|usage| {
            prepare_usage(
                usage,
                &decision,
                request.credential_id.as_deref(),
                deps,
                now,
            )
        })
        .transpose()?
        .flatten();
    // Only success lifts the credential fence, never a neutral cancellation.
    if request.outcome == Outcome::Completed {
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
    let credentials = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .clone();
    let quota = crate::ACCOUNT_QUOTA
        .get()
        .and_then(|q| q.lock().ok().map(|q| q.clone()))
        .unwrap_or_default();
    let config = super::operator_config(deps);
    let standing = deps
        .store
        .v1_commit_report(
            &request.decision_id,
            &delivery_key,
            &report_id,
            &V1Recorded {
                body_json: body_json.clone(),
                answer_json,
            },
            ledger_outcome(&request, now),
            request
                .outcome
                .is_failure()
                .then(|| {
                    request
                        .runner_code
                        .clone()
                        .unwrap_or_else(|| request.outcome.to_string())
                })
                .as_deref(),
            request.outcome.is_failure(),
            &effects,
            usage.as_ref(),
            |tx| {
                if reconcile {
                    crate::account_selection::refresh_credential_quota_holds_in(
                        tx,
                        &credentials,
                        &quota,
                        &config,
                        tx.report_credential_refusals()?,
                        &tx.report_route_fences(now as i64)?,
                        now as i64,
                    )?;
                }
                Ok(())
            },
            now as i64,
        )
        .map_err(store_refusal)?;
    match standing {
        Some(first) => serde_json::from_str(&first.answer_json)
            .map_err(|error| store_refusal(format!("stored report answer undecodable: {error}"))),
        None => Err(Refusal::new(
            RefusalCode::ReportConflict,
            "a report with a different body was already recorded for this decision",
        )),
    }
}

fn prepare_account_facts(
    params: &Value,
    decision: &prefrontal_routing_store::V1Decision,
    deps: &Deps<'_>,
    now: i64,
) -> Result<prefrontal_routing_store::V1ReportEffects, Refusal> {
    let mut effects = prefrontal_routing_store::V1ReportEffects::default();
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
    let credentials = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .clone();
    let targets = credentials.catalog_targets(&facts);
    prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &targets);
    let Some(fact) = facts
        .iter()
        .find(|fact| fact.provider_id == decision.provider && fact.model_id == decision.model)
    else {
        return Ok(effects);
    };
    // Reports cannot turn an arbitrary credential into an account fact.
    let catalog = facts.iter().map(|fact| fact.provider_id.clone()).collect();
    let serves = |credential: &str| {
        credentials.listed_credentials().iter().any(|row| {
            row.id == credential
                && crate::broca_credentials::credential_serves_fact(row, fact, &catalog)
        })
    };
    // A failed run with a future reset closes this route on the account that
    // served it, and nothing wider. The serving identity routing admitted for
    // the decision names that account; the report's own claim is the
    // fallback. A reset already past is a normal race ("retry in 5 s" landing
    // 6 s later), so the failure stands with no fence.
    if params["outcome"] == "failed" {
        if let Some(reset) = params
            .get("reset_at_ms")
            .and_then(Value::as_i64)
            .filter(|reset| *reset > now)
        {
            let served = identity
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
                .or(Some(credential))
                .filter(|served| serves(served));
            if let Some(served) = served {
                effects.route_fence = Some(prefrontal_routing_store::AccountRouteFence {
                    provider: decision.provider.clone(),
                    credential_id: served.into(),
                    model: decision.model.clone(),
                    until_ms: reset.min(now.saturating_add(RESET_FENCE_CAP_MS as i64)),
                    decision_id: decision.decision_id.clone(),
                    recorded_at: now,
                });
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
        effects.eligibility = Some(prefrontal_routing_store::AccountEligibilityFact {
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
    let code = params
        .get("runner_code")
        .and_then(Value::as_str)
        .unwrap_or("");
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
            prefrontal_routing_core::route_state_store::CredentialRefusal {
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
        let class = prefrontal_routing_core::accounts::input_size_class(tokens);
        if params["outcome"] == "completed" {
            effects.clear_size = Some((
                decision.provider.clone(),
                credential.into(),
                decision.model.clone(),
                class,
            ));
        } else if code.to_ascii_lowercase().contains("size")
            && (code.contains("429") || code.contains("exhaust") || code.contains("quota"))
        {
            effects.size_refusal = Some(prefrontal_routing_store::AccountSizeRefusal {
                provider: decision.provider.clone(),
                credential_id: credential.into(),
                model: decision.model.clone(),
                size_class: class,
                expires_at: params
                    .get("reset_at_ms")
                    .and_then(Value::as_i64)
                    .filter(|reset| *reset > now)
                    .unwrap_or(now.saturating_add(6 * 60 * 60_000)),
                observed_at: now,
            });
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
    decision: &prefrontal_routing_store::V1Decision,
    reported_credential: Option<&str>,
    deps: &Deps<'_>,
    now: u64,
) -> Result<Option<prefrontal_routing_store::V1ReportUsage>, Refusal> {
    let Some(output) = usage.output_tokens else {
        return Ok(None);
    };
    let reasoning = usage.reasoning_tokens;
    let observed_ms = now as f64;
    let facts = prefrontal_routing_core::NativeUsageFacts {
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
            let credentials = deps.broca_credentials.lock().ok()?;
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
            let credits = crate::ACCOUNT_QUOTA
                .get()
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
    Ok(Some(prefrontal_routing_store::V1ReportUsage {
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
