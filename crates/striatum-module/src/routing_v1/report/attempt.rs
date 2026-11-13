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
        // The commit must still check that this credential owns any selected probe.
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
