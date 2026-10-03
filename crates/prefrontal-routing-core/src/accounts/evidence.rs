//! Exhaustion evidence: header normalization, the three confirmation sources,
//! the short-hold rule and the Retry-After jitter.
//!
//! Design reference: `docs/designs/account-selection.md`, "Exhaustion is
//! evidence, not a status-word shortcut". The rounding, short-hold and jitter
//! functions port anthropic-auth at b504bc84
//! (`packages/core/src/quota-headers.ts:40`,
//! `packages/core/src/sticky-routing.ts:369-413,422-428`) exactly; the golden
//! vectors in `tests/fixtures/antauth-routing-vectors-b504bc84-20260929.json`
//! pin them.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::{
    ConfirmationSource, EpochMs, Fence, FenceScope, GeneralWindow, ModelFamily, ObservationSource,
    QuotaWindow, ResetTime,
};

/// The reference's header normalization: `round(utilization * 100)` clamped to
/// 0-100, so raw 0.995 and 0.999 read as 100. This value feeds weighting and
/// display only; it never confirms exhaustion.
pub fn normalize_used_percent(raw_utilization: f64) -> f64 {
    (raw_utilization * 100.0).round().clamp(0.0, 100.0)
}

/// Builds the general window a header frame describes, keeping the raw value
/// next to the normalized percent so later confirmation can read the raw one.
/// `reset_epoch_seconds` is the `-reset` header value.
pub fn header_window(
    window: GeneralWindow,
    raw_utilization: f64,
    reset_epoch_seconds: Option<f64>,
    source: ObservationSource,
) -> QuotaWindow {
    let resets_at = match reset_epoch_seconds {
        Some(seconds) if seconds.is_finite() => ResetTime::At((seconds * 1000.0) as EpochMs),
        Some(_) => ResetTime::Unparsable,
        None => ResetTime::Missing,
    };
    QuotaWindow {
        used_percent: normalize_used_percent(raw_utilization),
        raw_utilization: Some(raw_utilization),
        resets_at,
        window_minutes: window.window_minutes(),
        window_kind: None,
        source,
    }
}

/// The raw rule: header-derived evidence confirms exhaustion only when the
/// original raw utilization is finite and at least 1.0, on every path.
pub fn raw_confirms_exhaustion(raw_utilization: f64) -> bool {
    raw_utilization.is_finite() && raw_utilization >= 1.0
}

/// Which upstream frame a header observation came from. Only genuine frames
/// count as evidence; relay-generated optimistic headers never do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameOrigin {
    /// A genuine upstream response reported by Broca, independent of transport.
    Upstream,
    /// A direct upstream response.
    Direct,
    /// An upstream frame carried by an HTTP relay.
    HttpRelayUpstream,
    /// The WebSocket `response_start` frame.
    WsResponseStart,
    /// Headers a relay or wrapper synthesized.
    OptimisticWrapper,
}

impl FrameOrigin {
    pub fn is_genuine(self) -> bool {
        !matches!(self, FrameOrigin::OptimisticWrapper)
    }
}

/// How a reading reached routing. Forwarding never changes its observation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Carrier {
    Direct,
    PassiveFeed,
    Insula,
}

/// How an evidence value was measured.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Derivation {
    /// From response headers; `raw_utilization` is the original value before
    /// normalization, `None` when the carrier dropped it.
    Header {
        frame: FrameOrigin,
        raw_utilization: Option<f64>,
    },
    /// An authoritative usage-endpoint poll.
    Poll { used_percent: f64 },
}

/// The scope an evidence reading describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum EvidenceScope {
    General(GeneralWindow),
    /// A family's scoped window. Only a poll can describe one: headers carry
    /// no scoped metadata.
    Family(ModelFamily),
}

/// One window reading offered as exhaustion evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowEvidence {
    pub scope: EvidenceScope,
    pub derivation: Derivation,
    pub carrier: Carrier,
    /// The original observation time. Causality is judged on this alone.
    pub observed_at_ms: EpochMs,
    /// When the reading was forwarded, ingested or re-read. Recorded for audit,
    /// never used to decide causality.
    pub received_at_ms: EpochMs,
    pub reset: ResetTime,
}

/// Where Broca's `account_limit_exhausted.resets_at_ms` came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetSource {
    Header,
    RetryAfter,
}

/// An upstream HTTP 429 or initial streaming rate-limit error on the served account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitEvent {
    pub at_ms: EpochMs,
    /// False for a relay-generated 429.
    pub upstream: bool,
    /// The upstream Retry-After hint, in seconds. It drives the transient
    /// retry of an unconfirmed 429 and is never a quota fence's reset.
    pub retry_after_s: Option<u64>,
}

/// The observation kinds that can confirm exhaustion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ObservationKind {
    /// Source 1: Broca's typed `account_limit_exhausted` for the frozen account.
    BrocaLimitExhausted {
        provider: String,
        /// Verbatim value of the `anthropic-ratelimit-unified-representative-claim`
        /// response header;
        /// absent when the header was absent (and always for ChatGPT).
        limit_window: Option<String>,
        requested_family: ModelFamily,
        resets_at_ms: Option<EpochMs>,
        resets_at_source: Option<ResetSource>,
        observed_at_ms: EpochMs,
    },
    /// Source 2: a rate limit plus a reading of the same account.
    PostRateLimit {
        rate_limit: RateLimitEvent,
        reading: WindowEvidence,
    },
    /// Source 3: a genuine served response's own utilization header.
    ServedHeader(WindowEvidence),
}

/// One piece of evidence about one account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExhaustionObservation {
    pub account_id: String,
    /// True only when the evidence names an admission or pin routing issued to
    /// the same principal and the served account is the one it assigned.
    /// Unbound evidence is advisory and never fences.
    pub bound: bool,
    pub kind: ObservationKind,
}

/// Why evidence did not confirm exhaustion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotConfirmedReason {
    Unbound,
    /// The 429 was relay-generated, not upstream.
    RelayRateLimit,
    OptimisticFrame,
    /// The reading's original observation time is not after the 429.
    ObservedBeforeRateLimit,
    /// Raw utilization is below 1.0 (a rounded 100 cannot promote it).
    RawBelowThreshold,
    /// The carrier dropped the raw value, or it is not finite.
    RawMissing,
    PollNotExhausted,
    /// Source 3 needs a header-derived general window.
    NotAServedHeader,
}

/// The result of evaluating one observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum ConfirmationOutcome {
    Confirmed {
        fence: Fence,
    },
    NotConfirmed {
        reason: NotConfirmedReason,
        /// For an unconfirmed upstream rate limit: its real Retry-After, kept
        /// for the transient bounded retry while affinity stays put. It is
        /// never used as a fence reset.
        transient_retry_after_s: Option<u64>,
    },
    /// Broca reported a `limit_window` value routing has no mapping for. The
    /// design names `seven_day` and `five_hour` and the absent case only, so
    /// this is left for the caller to surface rather than guessed.
    Unclassified {
        limit_window: String,
    },
}

fn not_confirmed(reason: NotConfirmedReason) -> ConfirmationOutcome {
    ConfirmationOutcome::NotConfirmed {
        reason,
        transient_retry_after_s: None,
    }
}

fn fence_scope(scope: EvidenceScope) -> FenceScope {
    match scope {
        EvidenceScope::General(window) => FenceScope::General(window),
        EvidenceScope::Family(family) => FenceScope::Family(family),
    }
}

/// Header-derived evidence value check: genuine frame, raw >= 1.0.
fn header_value_confirms(frame: FrameOrigin, raw: Option<f64>) -> Result<(), NotConfirmedReason> {
    if !frame.is_genuine() {
        return Err(NotConfirmedReason::OptimisticFrame);
    }
    match raw {
        Some(raw) if raw.is_finite() => {
            if raw_confirms_exhaustion(raw) {
                Ok(())
            } else {
                Err(NotConfirmedReason::RawBelowThreshold)
            }
        }
        _ => Err(NotConfirmedReason::RawMissing),
    }
}

/// Evaluates one observation against the three confirmation sources. Only a
/// `Confirmed` outcome may become a shared fence.
pub fn confirm_exhaustion(observation: &ExhaustionObservation) -> ConfirmationOutcome {
    if !observation.bound {
        return not_confirmed(NotConfirmedReason::Unbound);
    }
    let fence =
        |scope, observed_at_ms, reset, retry_hint_ms, source| ConfirmationOutcome::Confirmed {
            fence: Fence {
                account_id: observation.account_id.clone(),
                scope,
                observed_at_ms,
                reset,
                retry_hint_ms,
                source,
            },
        };
    match &observation.kind {
        ObservationKind::BrocaLimitExhausted {
            provider,
            limit_window,
            requested_family,
            resets_at_ms,
            resets_at_source,
            observed_at_ms,
        } => {
            // A Retry-After-derived time is a retry hint, never a reset: it can
            // neither start a short hold nor end a fence.
            let (reset, retry_hint) = match (resets_at_ms, resets_at_source) {
                (Some(at), Some(ResetSource::Header)) => (ResetTime::At(*at), None),
                (Some(at), _) => (ResetTime::Missing, Some(*at)),
                (None, _) => (ResetTime::Missing, None),
            };
            let scope = if provider == "openai" {
                // ChatGPT publishes a single weekly window, and its
                // `usage_limit_reached` error names no window, so the wall maps
                // to that one window.
                FenceScope::General(GeneralWindow::SevenDay)
            } else {
                match limit_window.as_deref() {
                    Some("five_hour") => FenceScope::General(GeneralWindow::FiveHour),
                    Some("seven_day") => FenceScope::General(GeneralWindow::SevenDay),
                    None => FenceScope::UnknownScope(*requested_family),
                    Some(other) => {
                        return ConfirmationOutcome::Unclassified {
                            limit_window: other.to_string(),
                        }
                    }
                }
            };
            fence(
                scope,
                *observed_at_ms,
                reset,
                retry_hint,
                ConfirmationSource::BrocaAccountLimit,
            )
        }
        ObservationKind::PostRateLimit {
            rate_limit,
            reading,
        } => {
            if !rate_limit.upstream {
                return not_confirmed(NotConfirmedReason::RelayRateLimit);
            }
            // An unconfirmed upstream 429 keeps its own Retry-After for the
            // transient retry; a confirmed fence below takes its reset from
            // the reading, never from this hint.
            let not_confirmed = |reason| ConfirmationOutcome::NotConfirmed {
                reason,
                transient_retry_after_s: rate_limit.retry_after_s,
            };
            // Causality uses the original observation time, never the
            // forwarding, ingestion or re-read time.
            if reading.observed_at_ms <= rate_limit.at_ms {
                return not_confirmed(NotConfirmedReason::ObservedBeforeRateLimit);
            }
            match reading.derivation {
                Derivation::Header {
                    frame,
                    raw_utilization,
                } => match header_value_confirms(frame, raw_utilization) {
                    Ok(()) => fence(
                        fence_scope(reading.scope),
                        reading.observed_at_ms,
                        reading.reset,
                        None,
                        ConfirmationSource::PostRateLimitHeader,
                    ),
                    Err(reason) => not_confirmed(reason),
                },
                Derivation::Poll { used_percent } => {
                    if used_percent.is_finite() && used_percent >= 100.0 {
                        fence(
                            fence_scope(reading.scope),
                            reading.observed_at_ms,
                            reading.reset,
                            None,
                            ConfirmationSource::PostRateLimitPoll,
                        )
                    } else {
                        not_confirmed(NotConfirmedReason::PollNotExhausted)
                    }
                }
            }
        }
        ObservationKind::ServedHeader(reading) => {
            let (
                EvidenceScope::General(_),
                Derivation::Header {
                    frame,
                    raw_utilization,
                },
            ) = (reading.scope, reading.derivation)
            else {
                return not_confirmed(NotConfirmedReason::NotAServedHeader);
            };
            match header_value_confirms(frame, raw_utilization) {
                Ok(()) => fence(
                    fence_scope(reading.scope),
                    reading.observed_at_ms,
                    reading.reset,
                    None,
                    ConfirmationSource::RawHeader,
                ),
                Err(reason) => not_confirmed(reason),
            }
        }
    }
}

/// Why an established conversation leaves its account on confirmed exhaustion.
/// Serialized with the reference's spellings so the golden vectors compare
/// directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MigrateReason {
    ModelScoped,
    SevenDay,
    FiveHour,
}

/// The reference's quota-failure decision over confirmed exhaustion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum ExhaustionAction {
    Retain,
    /// Reason `five-hour-short-reset` in the reference.
    Hold {
        reset_at_ms: EpochMs,
        base_retry_after_s: u64,
    },
    Migrate {
        reason: MigrateReason,
    },
}

/// Ports `decideStickyQuotaFailure`: model-scoped exhaustion migrates first,
/// then 7d; a 5h exhaustion holds only when its reset is finite, strictly
/// later than `now`, and at most `short_hold_max_ms` away. There is no
/// clock-skew tolerance. `five_hour` is `Some(reset)` when the 5h window is
/// confirmed exhausted.
pub fn short_hold_rule(
    scoped_exhausted: bool,
    seven_day_exhausted: bool,
    five_hour: Option<ResetTime>,
    now: EpochMs,
    short_hold_max_ms: i64,
) -> ExhaustionAction {
    if scoped_exhausted {
        return ExhaustionAction::Migrate {
            reason: MigrateReason::ModelScoped,
        };
    }
    if seven_day_exhausted {
        return ExhaustionAction::Migrate {
            reason: MigrateReason::SevenDay,
        };
    }
    let Some(reset) = five_hour else {
        return ExhaustionAction::Retain;
    };
    match reset.at_ms() {
        Some(at) if at > now && at - now <= short_hold_max_ms => ExhaustionAction::Hold {
            reset_at_ms: at,
            base_retry_after_s: base_retry_after_s(at - now),
        },
        _ => ExhaustionAction::Migrate {
            reason: MigrateReason::FiveHour,
        },
    }
}

/// `max(1, ceil(seconds_to_reset))`.
pub fn base_retry_after_s(ms_to_reset: i64) -> u64 {
    let seconds = (ms_to_reset.max(0) as u64).div_ceil(1000);
    seconds.max(1)
}

/// The jitter added to a Retry-After: the first four bytes of SHA-256 of the
/// conversation key, big-endian, modulo 21, so 0-20 seconds.
pub fn jitter_s(conversation_key: &str) -> u64 {
    let digest = Sha256::digest(conversation_key.as_bytes());
    let n = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    u64::from(n % 21)
}

/// `max(1, ceil(base)) + jitter`, the reference's `stickyRetryAfterWithJitter`.
pub fn retry_after_with_jitter(conversation_key: &str, base_s: f64) -> u64 {
    let base = if base_s.is_finite() {
        base_s.ceil().max(1.0) as u64
    } else {
        1
    };
    base + jitter_s(conversation_key)
}
