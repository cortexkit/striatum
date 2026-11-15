use cortexkit_role_routing::attempt::AttemptStatus;
use cortexkit_role_routing::capabilities::REPORT_ATTEMPT_OUTCOMES;
use cortexkit_role_routing::errors::Refusal;
use cortexkit_role_routing::report::Outcome;
use cortexkit_role_routing::report::{parse_report_request_serving, ReportRequest};
use serde_json::Value;
use std::borrow::Cow;
use striatum_store::V1ReportEffects;

pub(super) fn parse(params: &Value) -> Result<ReportRequest, Refusal> {
    let capabilities = super::super::describe::capabilities();
    // Keep the closed member list aligned with the serving parser, including
    // refusing a null attempt_outcome while its group is not advertised.
    let members: Cow<'_, [&str]> = if capabilities.contains(&REPORT_ATTEMPT_OUTCOMES) {
        Cow::Owned(
            super::MEMBERS
                .iter()
                .copied()
                .chain(["attempt_outcome"])
                .collect(),
        )
    } else {
        Cow::Borrowed(super::MEMBERS)
    };
    super::super::refuse_unknown_members(params, &members)?;
    super::super::shapes::closed(params, "eligibility", &["scope", "code"])?;
    parse_report_request_serving(params, &capabilities)
}

/// Use a separate request for effects so the recorded body and answer stay unchanged.
pub(super) fn for_effects(request: &ReportRequest, now: u64) -> ReportRequest {
    let mut effects = request.clone();
    if let Some(attempt) = &request.attempt_outcome {
        effects.outcome = attempt.legacy_outcome();
        if attempt.status == AttemptStatus::Throttled {
            if let Some(wait) = attempt.retry_after_ms {
                // The legacy account writer reads signed dates, not durations.
                effects.reset_at_ms = Some(now.saturating_add(wait).min(i64::MAX as u64));
            }
        }
    }
    effects
}

pub(super) fn search_effects(
    request: &ReportRequest,
    decision: &striatum_store::V1Decision,
    now: u64,
) -> V1ReportEffects {
    use striatum_core::route_state_store::{RouteStateUpdate, RouteWallClass};
    let Some(attempt) = &request.attempt_outcome else {
        return V1ReportEffects::default();
    };
    let mut update = RouteStateUpdate {
        observed_at_ms: Some(now.min(i64::MAX as u64) as i64),
        unscoped_wall: true,
        ..Default::default()
    };
    match attempt.status {
        AttemptStatus::Served | AttemptStatus::ServedEmpty => {
            update.success_ema = Some(1.0);
        }
        AttemptStatus::Throttled | AttemptStatus::Unavailable => {
            let wait = attempt
                .retry_after_ms
                .unwrap_or(striatum_core::account_wall::TRANSIENT_BACKOFF_MS as u64);
            update.cooldown_until = Some(now.saturating_add(wait) as f64);
        }
        AttemptStatus::Quota | AttemptStatus::Credential => {
            // The module owns the account. Without an account identity, the
            // refusal can only block this route, for the non-quota maximum.
            update.cooldown_until = Some(
                now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS as u64)
                    as f64,
            );
            update.wall_class = Some(RouteWallClass::Billing);
        }
        _ => return V1ReportEffects::default(),
    }
    V1ReportEffects {
        route_state: Some((
            striatum_core::make_route_key(&decision.provider, &decision.model, None),
            striatum_core::make_capacity_key(&decision.provider, &decision.model),
            update,
        )),
        ..Default::default()
    }
}

pub(super) fn account_effects(
    request: &ReportRequest,
    credentials: &crate::broca_credentials::BrocaCredentials,
    mut prepare: impl FnMut(&Value) -> Result<V1ReportEffects, Refusal>,
) -> Result<V1ReportEffects, Refusal> {
    let neutral = request
        .attempt_outcome
        .as_ref()
        .is_some_and(|attempt| attempt.legacy_outcome() == Outcome::Cancelled);
    if neutral {
        // A neutral result applies no effect, but the commit still needs the
        // credential: if selection reserved this decision's single recovery test
        // (probe) for a blocked account, the commit checks that the report came
        // from that same credential before settling the probe.
        return Ok(V1ReportEffects {
            reported_credential: request.credential_id.clone(),
            ..Default::default()
        });
    }
    let mut params = serde_json::to_value(request)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    let mut effects = prepare(&params)?;
    effects.preserve_probe = !matches!(
        request.outcome,
        Outcome::Completed | Outcome::Failed | Outcome::Cancelled
    );
    // An eligibility result must not also infer exhaustion or credential refusal.
    if request.eligibility.is_none() {
        match request
            .attempt_outcome
            .as_ref()
            .map(|attempt| &attempt.status)
        {
            Some(AttemptStatus::Quota) => {
                params["runner_code"] = "account_limit_exhausted".into();
                let extra = prepare(&params)?;
                effects.route_fence = effects.route_fence.or(extra.route_fence);
                effects.non_quota_route_fence = effects
                    .non_quota_route_fence
                    .or(extra.non_quota_route_fence);
            }
            Some(AttemptStatus::Credential) => {
                params["refusal_class"] = "credential_refused".into();
                // A refused sign-in needs a new login, not a fence shared by every runner.
                let extra = prepare(&params)?.credential_refusal.filter(|(refusal, _)| {
                    credentials
                        .listed_credentials()
                        .iter()
                        .find(|row| row.id == refusal.credential_id)
                        .is_some_and(|row| {
                            matches!(row.auth_method.as_deref(), Some("apikey" | "gateway"))
                        })
                });
                effects.credential_refusal = effects.credential_refusal.or(extra);
            }
            _ => {}
        }
    }
    Ok(effects)
}
