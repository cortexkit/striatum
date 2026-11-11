//! Class-independent route backoff after repeated terminal send failures.
//!
//! Provider-specific quota and refusal classifiers remain authoritative. This
//! policy is the shorter safety net underneath them when a terminal send error
//! reaches the router without a recognized class.

use crate::RouteOutcome;

pub const DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD: u32 = 3;
pub const FAILURE_BACKOFF_INITIAL_MS: i64 = 10 * 60 * 1_000;
pub const FAILURE_BACKOFF_MAX_MS: i64 = crate::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS;

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
/// `NoDispatch` identifies failures before a request reaches a provider; `RunnerState`
/// identifies Broca refusing a resume because its own run state is incompatible;
/// `ContentPolicy` identifies a provider refusing the REQUEST on policy grounds (the
/// prompt, not the route, is what failed, and a reworded prompt succeeds on the same
/// route). `RequestInvalid` is Broca's typed request-shape refusal. None may charge
/// an individual route's terminal-failure streak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAttribution {
    RouteAttributable,
    LocalNetwork,
    NoDispatch,
    RunnerState,
    ContentPolicy,
    RequestInvalid,
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
/// provider 4xx responses other than typed request refusals, model refusals, stream
/// errors after the first token, and timeouts after dispatch. Provider-specific quota
/// handling remains separate. This
/// generic safety net keeps other terminal provider-side errors attributable unless
/// they match a host-local-network or pre-dispatch launch signature below.
pub fn classify_failure_attribution(
    outcome: RouteOutcome,
    last_error: Option<&str>,
) -> FailureAttribution {
    let error = last_error.unwrap_or_default().to_ascii_lowercase();
    // Core renders the runner's typed code either as a caller error prefix or as a
    // labelled field in the worker's terminal diagnostic. Do not inspect the raw
    // provider body (after "; <runner> said:"): it can mention request errors
    // without classifying this run.
    let diagnostic_fields = runner_diagnostic(&error);
    if error.starts_with("request_invalid:")
        || diagnostic_fields
            .split("; ")
            .any(|field| field == "provider code request_invalid")
    {
        return FailureAttribution::RequestInvalid;
    }
    // Only inspect the runner's diagnostic, not text it attributes to the
    // provider's response. These markers describe a failed session.send admission,
    // not an error returned by a provider after dispatch.
    if is_pre_dispatch_runner_failure(diagnostic_fields) {
        return FailureAttribution::NoDispatch;
    }
    // A refusal to resume because Broca's saved run state no longer matches its
    // current renderer is not evidence against the provider route.
    if diagnostic_fields.starts_with("resume refused:")
        || diagnostic_fields.starts_with("renderer_version_drift:")
    {
        return FailureAttribution::RunnerState;
    }
    if error
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|token| {
            matches!(
                token,
                "getaddrinfo" | "enotfound" | "etimedout" | "econnrefused" | "eai_again"
            )
        })
        || [
            "network is unreachable",
            "dns lookup failed",
            "dns resolution failed",
            "dns lookup timed out",
            "dns resolution timed out",
        ]
        .iter()
        .any(|signature| error.contains(signature))
    {
        return FailureAttribution::LocalNetwork;
    }
    // Provider policy refusals name the request as the cause. Signatures observed live:
    // OpenAI's cyber-program refusal (2026-09-06, a review brief), plus the generic
    // content-filter wordings the major providers use.
    if [
        "provider_policy_refusal",
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
        RouteOutcome::ContractError => FailureAttribution::RouteAttributable,
        RouteOutcome::Error => FailureAttribution::RouteAttributable,
        RouteOutcome::SessionError => FailureAttribution::RouteAttributable,
        RouteOutcome::Cancelled => FailureAttribution::RouteAttributable,
        RouteOutcome::Interrupted => FailureAttribution::RouteAttributable,
        RouteOutcome::QuotaExhausted { .. } => FailureAttribution::RouteAttributable,
    }
}

/// The part of a lowercased terminal diagnostic before the provider body the
/// runner quotes as "; <runner> said: …". The runner's name is a single word
/// read from the text.
fn runner_diagnostic(error: &str) -> &str {
    let mut from = 0;
    while let Some(at) = error[from..].find(" said:") {
        let end = from + at;
        if let Some(start) = error[..end].rfind("; ") {
            let runner = &error[start + 2..end];
            if !runner.is_empty() && !runner.contains(char::is_whitespace) {
                return &error[..start];
            }
        }
        from = end + " said:".len();
    }
    error
}

/// Recognize the concrete runner send and runner-unavailable diagnostics that prove
/// the provider was never given a request. A provider response body is removed by the
/// caller before these structural markers are checked. A runner whose management
/// surface could not be opened never received the request, whichever runner it is.
fn is_pre_dispatch_runner_failure(diagnostic: &str) -> bool {
    diagnostic.contains("llm_runner_unavailable:")
        || diagnostic.contains("session.send: request not sent:")
        || diagnostic.contains("route.open failed for target management_surface:")
}

/// Advance route-local failure backoff only for provider-attributable terminal evidence.
///
/// The allow-list includes provider 5xx responses, non-quota provider 4xx responses
/// other than typed request refusals, model refusals, stream errors after the first
/// token, and timeouts after dispatch.
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
        FailureAttribution::RunnerState => return FailureBackoffTransition::Unchanged,
        FailureAttribution::ContentPolicy => return FailureBackoffTransition::Unchanged,
        FailureAttribution::RequestInvalid => return FailureBackoffTransition::Unchanged,
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
    fn typed_request_invalid_preserves_the_route_streak_and_backoff() {
        for error in [
            "request_invalid: provider openai: invalid schema",
            "class permanent; provider code request_invalid; provider subcode invalid_json_schema; HTTP 400; Broca said: invalid schema",
        ] {
            assert_eq!(
                classify_failure_attribution(RouteOutcome::SessionError, Some(error)),
                FailureAttribution::RequestInvalid
            );
            for previous in [0, 2, 10] {
                assert_eq!(
                    failure_backoff_transition(previous, RouteOutcome::SessionError, Some(error), 1_000, FailureBackoffConfig::default()),
                    FailureBackoffTransition::Unchanged
                );
            }
        }
        for error in [
            "provider returned HTTP 503 after dispatch",
            "provider code invalid_prompt; HTTP 400; Broca said: bad prompt",
            "provider code provider_error; HTTP 503; Broca said: request_invalid",
            "provider code provider_error; HTTP 503; Broca said: provider code request_invalid;",
        ] {
            assert!(matches!(
                failure_backoff_transition(
                    2,
                    RouteOutcome::SessionError,
                    Some(error),
                    1_000,
                    FailureBackoffConfig::default()
                ),
                FailureBackoffTransition::Failure {
                    consecutive_failures: 3,
                    ..
                }
            ));
        }
    }

    #[test]
    fn broca_restart_send_failures_preserve_the_route_streak() {
        let specimens = [
            (
                RouteOutcome::Error,
                6,
                "no research part answered: send_failed: session.send: request not sent: route.open failed for target management_surface:broca: module_reloading (module_id 'broca' is reloading)",
                FailureAttribution::NoDispatch,
            ),
            (
                RouteOutcome::SessionError,
                8,
                "llm_runner_error: provider openai: session.send: request not sent: route.open failed for target management_surface:broca: module_reloading (module_id 'broca' is reloading)",
                FailureAttribution::NoDispatch,
            ),
            (
                RouteOutcome::SessionError,
                4,
                "llm_runner_error: provider openai: llm_runner_unavailable: Broca module is unavailable to the runner",
                FailureAttribution::NoDispatch,
            ),
            (
                RouteOutcome::SessionError,
                7,
                "resume refused: the provider renderer version differs from the version recorded at run start",
                FailureAttribution::RunnerState,
            ),
        ];

        for (outcome, previous, error, attribution) in specimens {
            assert_eq!(
                classify_failure_attribution(outcome, Some(error)),
                attribution,
                "{error}"
            );
            assert_eq!(
                failure_backoff_transition(
                    previous,
                    outcome,
                    Some(error),
                    1_000,
                    FailureBackoffConfig::default(),
                ),
                FailureBackoffTransition::Unchanged,
                "runner failures must preserve the existing streak: {error}"
            );
        }
    }

    #[test]
    fn provider_503_after_dispatch_counts_even_when_body_mentions_module_reloading() {
        let error = "class transient; provider code provider_error; HTTP 503; Broca said: provider body mentions module_reloading";
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::SessionError,
                Some(error),
                1_000,
                FailureBackoffConfig::default(),
            ),
            FailureBackoffTransition::Failure {
                consecutive_failures: 3,
                cooldown_until: Some(1_000 + FAILURE_BACKOFF_INITIAL_MS),
                reason: Some(format!(
                    "consecutive terminal send failures: 3; last error: {error}"
                )),
            }
        );
    }

    #[test]
    fn typed_provider_policy_refusal_does_not_charge_any_route_strike() {
        let error = "provider_policy_refusal: provider openai: cyber_policy; HTTP 400";
        assert_eq!(
            classify_failure_attribution(RouteOutcome::SessionError, Some(error)),
            FailureAttribution::ContentPolicy
        );
        assert_eq!(
            failure_backoff_transition(
                2,
                RouteOutcome::SessionError,
                Some(error),
                1_000,
                FailureBackoffConfig::default()
            ),
            FailureBackoffTransition::Unchanged
        );
    }

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
    fn user_abort_and_harness_interruption_are_excluded_from_the_terminal_send_failure_outcome_filter(
    ) {
        for (outcome, reason) in [
            (RouteOutcome::Cancelled, "user aborted"),
            (RouteOutcome::Interrupted, "harness stopped"),
        ] {
            assert!(!is_terminal_send_failure(outcome), "{outcome:?}");
            assert_eq!(
                failure_backoff_transition(
                    2,
                    outcome,
                    Some(reason),
                    1_000,
                    FailureBackoffConfig::default(),
                ),
                FailureBackoffTransition::Unchanged,
                "{outcome:?}"
            );
        }
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
    fn local_connection_timeout_codes_preserve_the_route_streak_and_backoff() {
        for error in [
            "connect ETIMEDOUT 203.0.113.1:443",
            "Error [ETIMEDOUT]: connection failed",
            "getaddrinfo EAI_AGAIN api.example.com",
            "DNS lookup failed for api.example.com",
            "DNS resolution failed for api.example.com",
            "DNS lookup timed out for api.example.com",
        ] {
            assert_eq!(
                failure_backoff_transition(
                    2,
                    RouteOutcome::SessionError,
                    Some(error),
                    1_000,
                    FailureBackoffConfig::default(),
                ),
                FailureBackoffTransition::Unchanged,
                "{error}"
            );
        }
    }

    #[test]
    fn provider_dns_mentions_and_post_dispatch_timeouts_still_charge_the_route() {
        for error in [
            "provider returned HTTP 503: upstream DNS service unavailable",
            "provider returned HTTP 500: rdns_backend failed",
            "provider returned HTTP 503: dns configuration rejected",
            "provider returned HTTP 500: enotfound_backend failed",
            "provider stream timed out after the first token",
        ] {
            assert_eq!(
                failure_backoff_transition(
                    2,
                    RouteOutcome::SessionError,
                    Some(error),
                    1_000,
                    FailureBackoffConfig::default(),
                ),
                FailureBackoffTransition::Failure {
                    consecutive_failures: 3,
                    cooldown_until: Some(1_000 + FAILURE_BACKOFF_INITIAL_MS),
                    reason: Some(format!(
                        "consecutive terminal send failures: 3; last error: {error}"
                    )),
                },
                "{error}"
            );
        }
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
    fn consecutive_failures_double_backoff_to_the_one_hour_cap() {
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
