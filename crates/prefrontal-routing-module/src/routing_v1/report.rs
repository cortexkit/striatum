//! `route.report` for `routing/v1`: how running on a decision went.
//!
//! Only the principal that selected a decision may report it; any other
//! caller is answered exactly as for a decision that never existed. The
//! first report on a decision stands: the same body again returns its
//! answer, and a different body is `report_conflict`. Outcomes are audit
//! facts, not evidence that quota is exhausted. Eligibility and explicitly
//! size-shaped refusals restrict only the account that actually served, and
//! a failure with a future reset closes only the decision's route on that
//! account until the reset, for at most 24 hours. Usage the report carries
//! joins the route's usage history untimed, with an absent count kept
//! unknown; a report without output adds no history row.

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::report::{
    parse_report_request, Outcome, ReportAnswer, ReportRequest, RESET_FENCE_CAP_MS,
};
use prefrontal_routing_core::RouteOutcome;
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
    let request = parse_report_request(params)?;
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
    super::consumers::effective(&super::operator_config(deps)).admit(&decision.consumer, caller)?;
    // Bodies are compared as decoded: the canonical encoding leaves absent
    // and null members out alike and fixes the member order.
    let body_json = serde_json::to_string(&request)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    if let Some(first) = deps
        .store
        .v1_report(&request.decision_id)
        .map_err(store_refusal)?
    {
        return standing_answer(first, &body_json);
    }

    record_account_facts(params, &decision, deps, now as i64)?;
    if let Some(usage) = &request.usage {
        record_usage(usage, &decision, deps, now)?;
    }
    deps.store
        .record_routing_outcome(
            &request.decision_id,
            &ledger_outcome(&request, now),
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
            now as i64,
        )
        .map_err(store_refusal)?;
    // A completed run proves the credential it launched on serves again: lift
    // the fence on that pair, if any. The remembered serving identity names
    // the credential; the report's own claim is the fallback.
    if request.outcome == Outcome::Completed {
        let credential = deps
            .store
            .decision_provider_identity(&request.decision_id)
            .map_err(store_refusal)?
            .and_then(|identity| identity.credential_id)
            .or_else(|| request.credential_id.clone());
        if let Some(credential) = credential {
            let decision = deps
                .store
                .v1_decision(&request.decision_id)
                .map_err(store_refusal)?;
            if let Some(decision) = decision {
                crate::credential_fences::lift_after_success(
                    deps.store,
                    &decision.provider,
                    &credential,
                )
                .map_err(store_refusal)?;
            }
        }
    }

    // One report per decision, so the decision names its report.
    let report_id = format!("report:{}", request.decision_id);
    let answer = ReportAnswer {
        decision_id: request.decision_id.clone(),
        report_id: report_id.clone(),
        outcome: request.outcome.clone(),
    };
    let answer_json = serde_json::to_string(&answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let standing = deps
        .store
        .v1_record_report(
            &request.decision_id,
            &report_id,
            &V1Recorded {
                body_json: body_json.clone(),
                answer_json,
            },
            request.outcome.is_failure(),
            now as i64,
        )
        .map_err(store_refusal)?;
    standing_answer(standing, &body_json)
}

fn record_account_facts(
    params: &Value,
    decision: &prefrontal_routing_store::V1Decision,
    deps: &Deps<'_>,
    now: i64,
) -> Result<(), Refusal> {
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
        return Ok(());
    };
    let classes = crate::account_selection::ClassContext::load(
        deps.store,
        deps.broca_credentials,
        super::operator_config(deps),
        crate::ACCOUNT_QUOTA.get(),
        now,
    )
    .map_err(store_refusal)?;
    let Some(fact) = classes
        .facts
        .iter()
        .find(|fact| fact.provider_id == decision.provider && fact.model_id == decision.model)
    else {
        return Ok(());
    };
    // Reports cannot turn an arbitrary credential into an account fact.
    let catalog = classes
        .facts
        .iter()
        .map(|fact| fact.provider_id.clone())
        .collect();
    let serves = |credential: &str| {
        classes.credentials.listed_credentials().iter().any(|row| {
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
                deps.store
                    .record_account_route_fence(&prefrontal_routing_store::AccountRouteFence {
                        provider: decision.provider.clone(),
                        credential_id: served.into(),
                        model: decision.model.clone(),
                        until_ms: reset.min(now.saturating_add(RESET_FENCE_CAP_MS as i64)),
                        decision_id: decision.decision_id.clone(),
                        recorded_at: now,
                    })
                    .map_err(store_refusal)?;
            }
        }
    }
    if !serves(credential) {
        return Ok(());
    }
    let fingerprint = match classes.credentials.listing_fingerprint(credential) {
        crate::broca_credentials::ListingLookup::Listed(fingerprint) => Some(fingerprint),
        _ => None,
    };
    if let Some(eligibility) = params.get("eligibility").filter(|value| !value.is_null()) {
        deps.store
            .record_account_eligibility(&prefrontal_routing_store::AccountEligibilityFact {
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
            })
            .map_err(store_refusal)?;
        return Ok(());
    }
    let code = params
        .get("runner_code")
        .and_then(Value::as_str)
        .unwrap_or("");
    if matches!(
        code,
        "ENTITLEMENT_ERROR" | "billing_exhausted" | "credit_insufficient" | "insufficient_quota"
    ) {
        // A subscription is a quota: when the account's latest fresh reading
        // already shows an exhausted window, the refusal is a quota hold
        // until that window's reset, not a billing fence with a 6h probe.
        // The pool fence below holds the account (and a fresh headroom
        // reading lifts it via the usual block refresh); a run-reported reset
        // on the same report still fences its route, so the later of the two
        // resets wins. Only a credential with no quota reading at all (a
        // pay-per-token key) becomes a billing fence.
        let account_id = classes
            .credentials
            .listed_credentials()
            .iter()
            .find(|row| row.id == credential)
            .map(|row| row.account_id.clone().unwrap_or_else(|| row.id.clone()));
        let reading_reset = account_id.as_deref().and_then(|account| {
            crate::credential_fences::exhausted_quota_reset_for_account(
                &classes.quota,
                account,
                now,
            )
        });
        // A paid balance that is itself exhausted must also block paid-credit
        // placement, which pool fences never do (quota exhaustion leaves paid
        // credits usable). When the reading still reports spendable credits,
        // keep the entitlement fence so even paid placement holds; it is still
        // treated as a quota hold (no probe, reset from the reading).
        let has_spendable = account_id.as_deref().and_then(|account| {
            classes
                .quota
                .readings
                .readings
                .values()
                .find(|reading| reading.account_id == account && reading.tombstoned_at_ms.is_none())
                .and_then(|reading| {
                    let entry: crate::account_quota::AccountEntry =
                        serde_json::from_value(reading.entry.clone()).ok()?;
                    let policy = classes.config.allocator_policy(&decision.provider);
                    if !policy.paid_credits {
                        return None;
                    }
                    for pool in entry.spend.iter() {
                        let id = pool.get("id").and_then(serde_json::Value::as_str)?;
                        if !matches!(id, "extra_usage" | "credits") {
                            continue;
                        }
                        let remaining = pool
                            .get("remaining")
                            .and_then(|value| {
                                serde_json::from_value::<
                                    prefrontal_routing_core::quota::SpendBalance,
                                >(value.clone())
                                .ok()
                            })
                            .map(|balance| balance.minor as f64 * 10_f64.powi(-balance.exponent))?;
                        if remaining.is_finite()
                            && remaining > 0.0
                            && pool.get("spendable").and_then(serde_json::Value::as_bool)
                                != Some(false)
                        {
                            return Some(());
                        }
                    }
                    None
                })
        });
        if has_spendable.is_some() {
            crate::credential_fences::record(
                deps.store,
                deps.broca_credentials,
                &prefrontal_routing_core::route_state_store::CredentialRefusal {
                    provider_id: decision.provider.clone(),
                    credential_id: credential.into(),
                    code: code.into(),
                },
                now,
            )
            .map_err(store_refusal)?;
        } else if let (Some(account_id), Some(reading_reset)) = (account_id, reading_reset) {
            let run_reset = params
                .get("reset_at_ms")
                .and_then(serde_json::Value::as_i64)
                .filter(|reset| *reset > now);
            let reset = run_reset
                .map(|run| run.max(reading_reset))
                .unwrap_or(reading_reset);
            let fence = prefrontal_routing_core::accounts::Fence {
                account_id: account_id.clone(),
                pool: None,
                recovery_set: Vec::new(),
                observed_at_ms: now,
                reset: prefrontal_routing_core::accounts::ResetTime::At(reset),
                retry_hint_ms: None,
                source: prefrontal_routing_core::accounts::ConfirmationSource::BrocaAccountLimit,
            };
            let fence_json = serde_json::to_value(&fence)
                .map(|value| value.to_string())
                .unwrap_or_default();
            let scope_json = serde_json::to_value(&fence.pool)
                .map(|value| value.to_string())
                .unwrap_or_else(|_| "null".into());
            let source = match serde_json::to_value(&fence.source) {
                Ok(serde_json::Value::String(text)) => text,
                Ok(other) => other.to_string(),
                Err(_) => "broca_account_limit".into(),
            };
            let row = prefrontal_routing_store::AccountPoolFenceRow {
                evidence: prefrontal_routing_store::AccountFenceRow {
                    fence_id: format!("qhold-{}-{now}", decision.decision_id),
                    provider: decision.provider.clone(),
                    account_id,
                    scope_json,
                    fence_json,
                    source,
                    observed_at: now,
                    principal: decision.principal.clone(),
                    admission_id: None,
                    observation_id: None,
                    created_at: now,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set: Vec::new(),
            };
            deps.store
                .account_transaction(|tx| tx.insert_pool_fence(&row))
                .map_err(store_refusal)?;
        } else {
            crate::credential_fences::record(
                deps.store,
                deps.broca_credentials,
                &prefrontal_routing_core::route_state_store::CredentialRefusal {
                    provider_id: decision.provider.clone(),
                    credential_id: credential.into(),
                    code: code.into(),
                },
                now,
            )
            .map_err(store_refusal)?;
        }
    }
    if let Some(tokens) = params.get("request_input_tokens").and_then(Value::as_u64) {
        let class = prefrontal_routing_core::accounts::input_size_class(tokens);
        if params["outcome"] == "completed" {
            deps.store
                .clear_account_size_refusal(
                    &decision.provider,
                    credential,
                    &decision.model,
                    class,
                    now,
                )
                .map_err(store_refusal)?;
        } else if code.to_ascii_lowercase().contains("size")
            && (code.contains("429") || code.contains("exhaust") || code.contains("quota"))
        {
            deps.store
                .record_account_size_refusal(&prefrontal_routing_store::AccountSizeRefusal {
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
                })
                .map_err(store_refusal)?;
        }
    }
    Ok(())
}

/// The usage-history row id of a v1 decision's report. One report stands per
/// decision, and the row is keyed by this id, so usage is counted once.
fn usage_row_id(decision_id: &str) -> String {
    format!("routing-v1-report:{decision_id}")
}

/// Add a report's usage to the route's usage history, the table
/// `route.record_usage` writes. A count the report leaves out stays unknown
/// (NULL), never zero. The history's output count cannot be unknown, so a
/// report without `output_tokens` adds no row: its counts stay only in the
/// stored report body. `output_tokens` includes `reasoning_tokens` on the
/// wire, as in the runner contract's usage; the history keeps visible output
/// with reasoning beside it. `duration_ms` is the run's wall clock, mostly
/// tool time on an agentic run, so the row is recorded untimed: it has no
/// duration and is never a speed sample. The timing members stay in the
/// stored report body.
fn record_usage(
    usage: &cortexkit_role_routing::report::Usage,
    decision: &prefrontal_routing_store::V1Decision,
    deps: &Deps<'_>,
    now: u64,
) -> Result<(), Refusal> {
    let Some(output) = usage.output_tokens else {
        return Ok(());
    };
    let reasoning = usage.reasoning_tokens;
    let observed_ms = now as f64;
    let facts = prefrontal_routing_core::NativeUsageFacts {
        message_id: usage_row_id(&decision.decision_id),
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
        return Ok(());
    };
    deps.store
        .record_usage_delivery_for_decision(&observation, &decision.decision_id)
        .map(|_| ())
        .map_err(store_refusal)
}

/// The recorded first report's answer when `body_json` is the same body, and
/// `report_conflict` when it is not.
fn standing_answer(first: V1Recorded, body_json: &str) -> Result<Value, Refusal> {
    if first.body_json != body_json {
        return Err(Refusal::new(
            RefusalCode::ReportConflict,
            "a report with a different body was already recorded for this decision",
        ));
    }
    serde_json::from_str(&first.answer_json)
        .map_err(|error| store_refusal(format!("stored report answer undecodable: {error}")))
}

/// A timed failure is still only an error. Confirmed exhaustion belongs to
/// admission-bound account observations, never a model outcome report.
fn ledger_outcome(request: &ReportRequest, _now_ms: u64) -> RouteOutcome {
    match &request.outcome {
        Outcome::Completed => RouteOutcome::Completed,
        Outcome::Cancelled => RouteOutcome::Cancelled,
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
    fn only_completed_and_cancelled_are_not_failures_in_the_ledger() {
        assert_eq!(
            ledger_outcome(&request("completed", None), 0),
            RouteOutcome::Completed
        );
        assert_eq!(
            ledger_outcome(&request("cancelled", None), 0),
            RouteOutcome::Cancelled
        );
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
