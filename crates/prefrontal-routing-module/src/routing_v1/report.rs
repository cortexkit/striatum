//! `route.report` for `routing/v1`: how running on a decision went.
//!
//! Only the principal that selected a decision may report it; any other
//! caller is answered exactly as for a decision that never existed. The
//! first report on a decision stands: the same body again returns its
//! answer, and a different body is `report_conflict`. A report also feeds
//! routing's own route health: the outcome is recorded on the decision's
//! ledger row, the same write the older `route.set_decision_outcome` makes,
//! so failure backoff and quota walls learn from v1 work too.

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::report::{parse_report_request, Outcome, ReportAnswer, ReportRequest};
use prefrontal_routing_core::{DecisionOutcomeUpdate, RouteOutcome, RouteStateStore};
use prefrontal_routing_store::V1Recorded;
use serde_json::Value;

use super::{now_ms, provider_refusal, store_refusal, Caller, Deps};
use crate::config_overrides;

const MEMBERS: &[&str] = &[
    "decision_id",
    "outcome",
    "runner_code",
    "credential_id",
    "reset_at_ms",
    "usage",
];

pub(super) fn report(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    super::refuse_unknown_members(params, MEMBERS)?;
    let request = parse_report_request(params)?;
    let decision = deps
        .store
        .v1_decision(&request.decision_id)
        .map_err(store_refusal)?;
    if decision.is_none_or(|decision| decision.principal != caller.principal) {
        return Err(Refusal::unknown_decision());
    }
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

    let now = now_ms();
    let provider_outcome = if ledger_outcome(&request, now) == RouteOutcome::Completed {
        // The serving identity frozen at selection, as the older outcome op
        // reads it, so a success clears the right credential's walls.
        deps.store
            .decision_provider_identity(&request.decision_id)
            .map_err(store_refusal)?
    } else {
        None
    };
    deps.store
        .set_decision_outcome(
            &request.decision_id,
            &DecisionOutcomeUpdate {
                provider_outcome,
                outcome: ledger_outcome(&request, now),
                last_error: request.outcome.is_failure().then(|| {
                    request
                        .runner_code
                        .clone()
                        .unwrap_or_else(|| request.outcome.to_string())
                }),
                observed_at_ms: now as i64,
                failure_backoff: config_overrides::live_config(deps.routing_config_path)
                    .failure_backoff(),
            },
        )
        .map_err(store_refusal)?;

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
            now as i64,
        )
        .map_err(store_refusal)?;
    standing_answer(standing, &body_json)
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

/// The outcome the decision ledger records. A failure the runner timed (it
/// named when the cause ends) is a quota wall until then; any other failure,
/// an outcome this provider does not name included, is an error.
fn ledger_outcome(request: &ReportRequest, now_ms: u64) -> RouteOutcome {
    match &request.outcome {
        Outcome::Completed => RouteOutcome::Completed,
        Outcome::Cancelled => RouteOutcome::Cancelled,
        _ => match request.reset_at_ms.filter(|reset| *reset > now_ms) {
            Some(reset) => RouteOutcome::QuotaExhausted {
                retry_after_secs: (reset - now_ms).div_ceil(1000),
            },
            None => RouteOutcome::Error,
        },
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
            RouteOutcome::QuotaExhausted {
                retry_after_secs: 5
            }
        );
    }
}
