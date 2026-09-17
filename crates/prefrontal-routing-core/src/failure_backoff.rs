//! Class-independent route backoff after repeated terminal send failures.
//!
//! Provider-specific quota and refusal classifiers remain authoritative. This
//! policy is the shorter safety net underneath them when a terminal send error
//! reaches the router without a recognized class.

use crate::RouteOutcome;

pub const DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD: u32 = 3;
pub const FAILURE_BACKOFF_INITIAL_MS: i64 = 10 * 60 * 1_000;
pub const FAILURE_BACKOFF_MAX_MS: i64 = 6 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureBackoffConfig {
    pub consecutive_terminal_failure_threshold: u32,
}

impl Default for FailureBackoffConfig {
    fn default() -> Self {
        Self {
            consecutive_terminal_failure_threshold: DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureBackoffTransition {
    Unchanged,
    Reset,
    Failure {
        consecutive_failures: u32,
        cooldown_until: Option<i64>,
        reason: Option<String>,
    },
}

/// Whether a terminal outcome is evidence about the selected provider route.
///
/// `LocalNetwork` identifies failures that every route on this host would see;
/// `NoDispatch` identifies launch failures before the host prompt reaches a provider;
/// `ContentPolicy` identifies a provider refusing the REQUEST on policy grounds (the
/// prompt, not the route, is what failed, and a reworded prompt succeeds on the same
/// route). None of the three may charge an individual route's terminal-failure streak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAttribution {
    RouteAttributable,
    LocalNetwork,
    NoDispatch,
    ContentPolicy,
}

/// Only outcomes that prove the selected route could not complete its send count.
/// `error`, `session_error`, and `launch_failed` count. `cancelled` is a user abort,
/// `interrupted` is a harness cancellation, and quota exhaustion has its own exact cooldown;
/// none of those three belongs in the generic failure streak.
pub fn is_terminal_send_failure(outcome: RouteOutcome) -> bool {
    matches!(
        outcome,
        RouteOutcome::Error | RouteOutcome::SessionError | RouteOutcome::LaunchFailed
    )
}

/// Classify the evidence feeding the route-local terminal-failure counter.
///
/// Route-attributable evidence explicitly includes provider 5xx responses, non-quota
/// provider 4xx responses, model refusals, stream errors after the first token, and
/// timeouts after dispatch. Provider-specific quota handling remains separate. This
/// generic safety net keeps other terminal provider-side errors attributable unless
/// they match a host-local-network or pre-dispatch launch signature below.
pub fn classify_failure_attribution(
    outcome: RouteOutcome,
    last_error: Option<&str>,
) -> FailureAttribution {
    let error = last_error.unwrap_or_default().to_ascii_lowercase();
    if [
        "getaddrinfo",
        "enotfound",
        "etimeout",
        "econnrefused",
        "eai_again",
        "network is unreachable",
        "dns",
    ]
    .iter()
    .any(|class| error.contains(class))
    {
        return FailureAttribution::LocalNetwork;
    }
    // Provider policy refusals name the request as the cause. Signatures observed live:
    // OpenAI's cyber-program refusal (2026-09-06, a review brief), plus the generic
    // content-filter wordings the major providers use.
    if [
        "flagged for possible cybersecurity risk",
        "trusted access for cyber",
        "content_filter",
        "content filter",
        "content management policy",
        "content policy",
        "safety system",
    ]
    .iter()
    .any(|class| error.contains(class))
    {
        return FailureAttribution::ContentPolicy;
    }

    match outcome {
        RouteOutcome::LaunchFailed => {
            if [
                "zombie_start",
                "without assistant output",
                "create_failed",
                "launch_unbound_timeout",
                // A gather refused before its model send (read-tool catalog absent,
                // cache or git unreadable, runner unreachable) never reached the route.
                "pre-dispatch refusal",
            ]
            .iter()
            .any(|signature| error.contains(signature))
            {
                FailureAttribution::NoDispatch
            } else {
                FailureAttribution::RouteAttributable
            }
        }
        RouteOutcome::Completed => FailureAttribution::RouteAttributable,
        RouteOutcome::Error => FailureAttribution::RouteAttributable,
        RouteOutcome::SessionError => FailureAttribution::RouteAttributable,
        RouteOutcome::Cancelled => FailureAttribution::RouteAttributable,
        RouteOutcome::Interrupted => FailureAttribution::RouteAttributable,
        RouteOutcome::QuotaExhausted { .. } => FailureAttribution::RouteAttributable,
    }
}

/// Advance route-local failure backoff only for provider-attributable terminal evidence.
///
/// The allow-list includes provider 5xx responses, non-quota provider 4xx responses,
/// model refusals, stream errors after the first token, and timeouts after dispatch.
/// Local network failures and pre-dispatch launch failures preserve the existing streak
/// and backoff because they do not establish that this provider route was reached.
pub fn failure_backoff_transition(
    previous_consecutive_failures: u32,
    outcome: RouteOutcome,
    last_error: Option<&str>,
    now_ms: i64,
    config: FailureBackoffConfig,
) -> FailureBackoffTransition {
    if outcome == RouteOutcome::Completed {
        return FailureBackoffTransition::Reset;
    }
    if !is_terminal_send_failure(outcome) {
        return FailureBackoffTransition::Unchanged;
    }
    match classify_failure_attribution(outcome, last_error) {
        FailureAttribution::RouteAttributable => {}
        FailureAttribution::LocalNetwork => return FailureBackoffTransition::Unchanged,
        FailureAttribution::NoDispatch => return FailureBackoffTransition::Unchanged,
        FailureAttribution::ContentPolicy => return FailureBackoffTransition::Unchanged,
    }

    let consecutive_failures = previous_consecutive_failures.saturating_add(1);
    let threshold = config.consecutive_terminal_failure_threshold.max(1);
    if consecutive_failures < threshold {
        return FailureBackoffTransition::Failure {
            consecutive_failures,
            cooldown_until: None,
            reason: None,
        };
    }

    let exponent = consecutive_failures.saturating_sub(threshold).min(31);
    let duration = FAILURE_BACKOFF_INITIAL_MS
        .saturating_mul(1_i64 << exponent)
        .min(FAILURE_BACKOFF_MAX_MS);
    let error = last_error
        .filter(|error| !error.is_empty())
        .unwrap_or_else(|| outcome.as_str());
    FailureBackoffTransition::Failure {
        consecutive_failures,
        cooldown_until: Some(now_ms.saturating_add(duration)),
        reason: Some(format!(
            "consecutive terminal send failures: {consecutive_failures}; last error: {error}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPECIMEN_ERROR: &str =
        "Access to model denied. Please make sure you are eligible for using the model.";

    /// Verbatim from the live routing store (openai/gpt-6-astra, 2026-09-06).
    const CONTENT_POLICY_SPECIMEN: &str = "This content was flagged for possible cybersecurity risk. If this seems wrong, try rephrasing your request. To get authorized for security work, join the Trusted Access for Cyber program: https://chatgpt.com/cyber";

    #[test]
    fn a_provider_content_policy_refusal_does_not_charge_the_route() {
        assert_eq!(
            classify_failure_attribution(RouteOutcome::SessionError, Some(CONTENT_POLICY_SPECIMEN)),
            FailureAttribution::ContentPolicy
        );
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::SessionError,
                Some(CONTENT_POLICY_SPECIMEN),
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Unchanged,
            "the third strike would have opened a 10m backoff; a policy refusal is not route evidence"
        );
        // Control: the eligibility refusal stays route evidence.
        assert!(matches!(
            failure_backoff_transition(
                2,
                RouteOutcome::SessionError,
                Some(SPECIMEN_ERROR),
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Failure {
                consecutive_failures: 3,
                ..
            }
        ));
    }

    #[test]
    fn third_consecutive_session_error_starts_ten_minute_backoff_with_verbatim_reason() {
        let transition = failure_backoff_transition(
            2,
            RouteOutcome::SessionError,
            Some(SPECIMEN_ERROR),
            1_000,
            FailureBackoffConfig::default(),
        );
        assert_eq!(
            transition,
            FailureBackoffTransition::Failure {
                consecutive_failures: 3,
                cooldown_until: Some(1_000 + FAILURE_BACKOFF_INITIAL_MS),
                reason: Some(format!(
                    "consecutive terminal send failures: 3; last error: {SPECIMEN_ERROR}"
                )),
            }
        );
    }

    #[test]
    fn success_at_two_resets_the_consecutive_failure_counter() {
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::Completed,
                None,
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Reset
        );
    }

    #[test]
    fn user_abort_cancelled_is_excluded_from_the_terminal_send_failure_outcome_filter() {
        assert!(!is_terminal_send_failure(RouteOutcome::Cancelled));
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::Cancelled,
                Some("user aborted"),
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Unchanged
        );
    }

    #[test]
    fn harness_interruption_is_excluded_from_the_terminal_send_failure_outcome_filter() {
        assert!(!is_terminal_send_failure(RouteOutcome::Interrupted));
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::Interrupted,
                Some("harness stopped"),
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Unchanged
        );
    }

    #[test]
    fn three_local_network_failures_leave_the_route_streak_and_backoff_unchanged() {
        let mut consecutive_failures = 0;
        let mut cooldown_until = None;
        for _ in 0..3 {
            match failure_backoff_transition(
                consecutive_failures,
                RouteOutcome::SessionError,
                Some("getaddrinfo ENOTFOUND chatgpt.com"),
                1_000,
                FailureBackoffConfig::default(),
            ) {
                FailureBackoffTransition::Unchanged => {}
                FailureBackoffTransition::Reset => {
                    consecutive_failures = 0;
                    cooldown_until = None;
                }
                FailureBackoffTransition::Failure {
                    consecutive_failures: next,
                    cooldown_until: next_cooldown,
                    ..
                } => {
                    consecutive_failures = next;
                    cooldown_until = next_cooldown;
                }
            }
        }

        assert_eq!(consecutive_failures, 0);
        assert_eq!(cooldown_until, None);
    }

    #[test]
    fn two_pre_dispatch_launch_failures_then_a_provider_503_count_once() {
        let mut consecutive_failures = 0;
        let mut cooldown_until = None;
        for (outcome, error) in [
            (
                RouteOutcome::LaunchFailed,
                "launch_failed: session became idle without assistant output (zombie_start)",
            ),
            (
                RouteOutcome::LaunchFailed,
                "launch_failed: session became idle without assistant output (zombie_start)",
            ),
            (
                RouteOutcome::Error,
                "provider returned HTTP 503 after dispatch",
            ),
        ] {
            match failure_backoff_transition(
                consecutive_failures,
                outcome,
                Some(error),
                1_000,
                FailureBackoffConfig::default(),
            ) {
                FailureBackoffTransition::Unchanged => {}
                FailureBackoffTransition::Reset => {
                    consecutive_failures = 0;
                    cooldown_until = None;
                }
                FailureBackoffTransition::Failure {
                    consecutive_failures: next,
                    cooldown_until: next_cooldown,
                    ..
                } => {
                    consecutive_failures = next;
                    cooldown_until = next_cooldown;
                }
            }
        }

        assert_eq!((consecutive_failures, cooldown_until), (1, None));
    }

    #[test]
    fn consecutive_failures_double_backoff_to_the_six_hour_cap() {
        let config = FailureBackoffConfig::default();
        let deadline = |previous| match failure_backoff_transition(
            previous,
            RouteOutcome::Error,
            Some("send failed"),
            0,
            config,
        ) {
            FailureBackoffTransition::Failure {
                cooldown_until: Some(until),
                ..
            } => until,
            other => panic!("expected active backoff, got {other:?}"),
        };
        assert_eq!(deadline(2), FAILURE_BACKOFF_INITIAL_MS);
        assert_eq!(deadline(3), 2 * FAILURE_BACKOFF_INITIAL_MS);
        assert_eq!(deadline(30), FAILURE_BACKOFF_MAX_MS);
    }
}
