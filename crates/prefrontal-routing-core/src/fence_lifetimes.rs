//! Maximum demand-driven recheck intervals; neither interval expires evidence.
pub const QUOTA_FENCE_RECHECK_MS: i64 = 15 * 60_000;
pub const NON_QUOTA_FENCE_MAX_MS: i64 = 60 * 60_000;

/// A neutral outcome releases a non-quota probe claim, but does not heal its
/// refusal. Typed request refusals and local failures say nothing about whether
/// a different request can use the selected route.
pub fn probe_is_inconclusive(outcome: crate::RouteOutcome, error: Option<&str>) -> bool {
    use crate::failure_backoff::{
        classify_failure_attribution, is_terminal_send_failure, FailureAttribution,
    };
    outcome != crate::RouteOutcome::Completed
        && (!is_terminal_send_failure(outcome)
            || matches!(
                error,
                Some("request_invalid" | "content_policy" | "provider_policy_refusal")
            )
            || classify_failure_attribution(outcome, error)
                != FailureAttribution::RouteAttributable)
}

#[cfg(test)]
mod tests {
    #[test]
    fn fence_policy_intervals_are_fifteen_minutes_and_one_hour() {
        assert_eq!(super::QUOTA_FENCE_RECHECK_MS, 900_000);
        assert_eq!(super::NON_QUOTA_FENCE_MAX_MS, 3_600_000);
    }
}
