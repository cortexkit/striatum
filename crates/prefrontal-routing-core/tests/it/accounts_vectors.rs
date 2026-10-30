//! Golden fixtures for the account allocator: ANTAUTH's evaluated vectors
//! (anthropic-auth b504bc84, fixed clock 2026-09-29T12:00:00Z), the raw
//! confirmation rule, and the three 0.995 laundering paths from
//! `docs/designs/account-selection.md` ("Golden fixtures").

#[path = "../accounts_support/mod.rs"]
mod accounts_support;

use accounts_support::*;
use prefrontal_routing_core::accounts::evidence::{
    confirm_exhaustion, header_window, hold_until, jitter_s, normalize_used_percent,
    retry_after_with_jitter, Carrier, ConfirmationOutcome, Derivation, EvidenceScope,
    ExhaustionObservation, FrameOrigin, NotConfirmedReason, ObservationKind, RateLimitEvent,
    ResetSource, WindowEvidence,
};
use prefrontal_routing_core::accounts::{
    decide, eligibility::scope_check, AllocatorPolicy, ConfirmationSource, FenceScope,
    GeneralWindow, HoldBasis, ModelFamily, ObservationSource, PoolKey, PoolScope, ResetTime,
    ScopedList, ScopedWindow,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const VECTORS: &str = include_str!("../fixtures/antauth-routing-vectors-b504bc84-20260929.json");
/// The digest `docs/designs/account-selection.md` records for the vectors (ref N).
const VECTORS_SHA256: &str = "82cfd614d8d7de8f1a533debe2a78f2a3639c90d0e14e4eca0ca3919e38478ff";

#[test]
fn broca_fresh_admission_leaves_weekly_affinity_for_a_healthy_sibling() {
    use prefrontal_routing_core::accounts::{AffinityOutcome, CallerKind, Outcome};
    for provider in ["openai", "anthropic"] {
        let mut request = with_model(
            request(),
            if provider == "openai" {
                "gpt-6.1-sol"
            } else {
                "claude-opus-5-5"
            },
        );
        request.provider = provider.into();
        request.caller = CallerKind::Broca;
        let mut state = state(vec![
            account("walled", Some(reading(10.0, 100.0))),
            account("healthy", Some(reading(10.0, 21.0))),
        ]);
        for account in &mut state.roster.as_mut().unwrap().accounts {
            account.routing_provider = provider.into();
        }
        state.affinity = Some(affinity("walled", &request.affinity_model, 1));
        state.fences.push(fence(
            "walled",
            FenceScope::General(GeneralWindow::SevenDay),
            ResetTime::At(NOW + 7 * 24 * HOUR),
        ));
        let decision = decide(&request, &state, NOW);
        let Outcome::Admit(admission) = decision.outcome else {
            panic!("healthy sibling not admitted: {decision:?}");
        };
        assert_eq!(admission.account_id, "healthy");
        assert!(matches!(
            admission.affinity,
            AffinityOutcome::New {
                pin_generation: 2,
                previous: Some(_)
            }
        ));
    }
}

#[test]
fn broca_all_weekly_accounts_walled_refuses_with_the_real_pool_horizon() {
    use prefrontal_routing_core::accounts::{CallerKind, Outcome, RefusalCode};
    let mut request = request();
    request.caller = CallerKind::Broca;
    let reset = NOW + 7 * 24 * HOUR;
    let mut quota = reading(10.0, 100.0);
    quota.five_hour.as_mut().unwrap().resets_at = ResetTime::At(reset);
    quota.seven_day.as_mut().unwrap().resets_at = ResetTime::At(reset);
    let mut state = state(vec![
        account("a", Some(quota.clone())),
        account("b", Some(quota)),
    ]);
    for account in ["a", "b"] {
        state.fences.push(fence(
            account,
            FenceScope::General(GeneralWindow::SevenDay),
            ResetTime::At(reset),
        ));
    }
    state.affinity = Some(affinity("a", &request.affinity_model, 1));
    let decision = decide(&request, &state, NOW);
    let Outcome::Refuse(refusal) = decision.outcome else {
        panic!("walled pool admitted or held: {decision:?}");
    };
    assert_eq!(refusal.code, RefusalCode::AccountPoolExhausted);
    assert_eq!(
        refusal.retry_after_s,
        Some((reset - NOW) as u64 / 1000 + state.policy.pool_retry_margin_s)
    );
}

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("vectors parse")
}

/// Parses the fixtures' `YYYY-MM-DDTHH:MM:SS.mmmZ` timestamps.
fn parse_iso(value: &str) -> Option<i64> {
    let b = value.as_bytes();
    if b.len() != 24 || b[4] != b'-' || b[10] != b'T' || b[23] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| value.get(range)?.parse::<i64>().ok();
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mm, ss, ms) = (num(11..13)?, num(14..16)?, num(17..19)?, num(20..23)?);
    // Days from civil (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + hh) * 60 + mm) * 60_000 + ss * 1000 + ms)
}

fn reset_from(value: &Value) -> ResetTime {
    match value.as_str() {
        None => ResetTime::Missing,
        Some(text) => parse_iso(text).map_or(ResetTime::Unparsable, ResetTime::At),
    }
}

#[test]
fn vectors_fixture_keeps_the_digest_the_design_records() {
    let digest = Sha256::digest(VECTORS.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, VECTORS_SHA256);
    let value = vectors();
    assert_eq!(
        value["sourceRef"],
        "b504bc843c561e911bf6e405b7efecf82ec62886"
    );
    assert_eq!(value["now"].as_i64(), Some(NOW));
}

/// The plugin identifies scoped windows by display name or title; routing does
/// not port substring matching, so this adapter maps a Fable-named window to
/// routing's Fable scope id and keeps any other name as an (unmapped) id.
fn adapt_scoped(quota: &Value) -> ScopedList {
    let Some(items) = quota.get("scoped").and_then(Value::as_array) else {
        return ScopedList::Missing;
    };
    ScopedList::Listed(
        items
            .iter()
            .map(|item| {
                let label = ["modelId", "modelName", "title"]
                    .iter()
                    .filter_map(|key| item.get(*key).and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join(" ");
                let scope_id = if label.to_lowercase().contains("fable") {
                    "Fable".to_string()
                } else {
                    label
                };
                // JSON holds null for the NaN the plugin evaluated.
                let remaining = item["remainingPercent"].as_f64().unwrap_or(f64::NAN);
                ScopedWindow {
                    scope_id,
                    window_minutes: 10_080,
                    used_percent: 100.0 - remaining,
                    resets_at: ResetTime::Missing,
                }
            })
            .collect(),
    )
}

#[test]
fn scope_vectors_family_scope_and_absent_scope_predicate() {
    let value = vectors();
    let rows = value["scopes"].as_array().unwrap();
    assert_eq!(rows.len(), 9);
    for row in rows {
        let id = row["id"].as_str().unwrap();
        let check = scope_check(row["model"].as_str().unwrap(), &adapt_scoped(&row["quota"]));
        assert_eq!(
            check.matched,
            row["matched"].as_bool().unwrap(),
            "{id}: matched"
        );
        assert_eq!(
            check.passes,
            row["passesScope"].as_bool().unwrap(),
            "{id}: passesScope"
        );
    }
    // The two absent-scope cases pass but stay distinguishable.
    let missing = scope_check("claude-fable-5", &ScopedList::Missing);
    let empty = scope_check("claude-fable-5", &ScopedList::Listed(Vec::new()));
    assert_ne!(missing.list_state, empty.list_state);
}

/// The reference's reset vectors against the hold rule's reset check. The
/// reference applied them to the 5h window only; routing applies the same
/// bounds to whichever window blocks continuation, so a vector the reference
/// migrates is one the hold rule moves.
#[test]
fn reset_vectors_hold_exact_bounds() {
    let policy = AllocatorPolicy::default();
    let value = vectors();
    let rows = value["resets"].as_array().unwrap();
    assert_eq!(rows.len(), 7);
    for row in rows {
        let id = row["id"].as_str().unwrap();
        let held = hold_until(reset_from(&row["resetsAt"]), NOW, policy.short_hold_max_ms);
        let expected = &row["decision"];
        match expected["action"].as_str().unwrap() {
            "hold" => {
                let (_, base_retry_after_s) =
                    held.unwrap_or_else(|| panic!("{id}: expected hold, got a move"));
                assert_eq!(
                    Some(base_retry_after_s),
                    expected["retryAfterSeconds"].as_u64(),
                    "{id}"
                );
            }
            "migrate" => assert_eq!(held, None, "{id}"),
            other => panic!("{id}: unexpected action {other}"),
        }
    }
}

#[test]
fn jitter_vectors_sha256_first_four_bytes_mod_21() {
    let value = vectors();
    let rows = value["jitter"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    for row in rows {
        let session = row["sessionId"].as_str().unwrap();
        let base = row["base"].as_f64().unwrap();
        assert_eq!(
            Some(retry_after_with_jitter(session, base)),
            row["result"].as_u64(),
            "{session}"
        );
    }
    assert_eq!(retry_after_with_jitter("ses-golden-a", 840.0), 846);
    assert_eq!(retry_after_with_jitter("ses-golden-b", 840.0), 856);
    assert_eq!(retry_after_with_jitter("ses-golden-c", 840.0), 860);
    // `max(1, ceil(base))`: a fractional or zero base never drops below 1.
    assert_eq!(
        retry_after_with_jitter("ses-golden-a", 0.2),
        1 + jitter_s("ses-golden-a")
    );
    assert_eq!(retry_after_with_jitter("ses-golden-a", 839.1), 846);
}

#[test]
fn header_vectors_faithful_rounding_of_the_normalized_percent() {
    let value = vectors();
    let rows = value["headers"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    for row in rows {
        let raw = row["utilization"].as_f64().unwrap();
        let expected = &row["normalized"]["five_hour"];
        let reset_ms = parse_iso(expected["resetsAt"].as_str().unwrap()).unwrap();
        let window = header_window(
            GeneralWindow::FiveHour,
            raw,
            Some(reset_ms as f64 / 1000.0),
            ObservationSource::Header,
        );
        assert_eq!(
            Some(window.used_percent),
            expected["usedPercent"].as_f64(),
            "{raw}"
        );
        assert_eq!(
            Some(100.0 - window.used_percent),
            expected["remainingPercent"].as_f64(),
            "{raw}"
        );
        assert_eq!(window.resets_at, ResetTime::At(reset_ms), "{raw}");
        assert_eq!(window.raw_utilization, Some(raw));
    }
    assert_eq!(normalize_used_percent(0.995), 100.0);
    assert_eq!(normalize_used_percent(-0.2), 0.0);
}

// ---- raw confirmation and laundering ------------------------------------

const T_429: i64 = NOW; // 12:00:00Z
const T_AFTER: i64 = NOW + 1000; // 12:00:01Z
const T_BEFORE: i64 = NOW - 1000; // 11:59:59Z
const T_FORWARD: i64 = NOW + 2000; // 12:00:02Z

fn header_evidence(
    raw: f64,
    carrier: Carrier,
    frame: FrameOrigin,
    observed_at: i64,
) -> WindowEvidence {
    WindowEvidence {
        scope: EvidenceScope::General(GeneralWindow::FiveHour),
        derivation: Derivation::Header {
            frame,
            raw_utilization: Some(raw),
        },
        carrier,
        observed_at_ms: observed_at,
        received_at_ms: if carrier == Carrier::Direct {
            observed_at
        } else {
            T_FORWARD
        },
        reset: ResetTime::At(NOW + 15 * MINUTE),
    }
}

fn served(evidence: WindowEvidence) -> ExhaustionObservation {
    ExhaustionObservation {
        account_id: "acct-a".into(),
        bound: true,
        kind: ObservationKind::ServedHeader(evidence),
    }
}

fn after_429(evidence: WindowEvidence) -> ExhaustionObservation {
    ExhaustionObservation {
        account_id: "acct-a".into(),
        bound: true,
        kind: ObservationKind::PostRateLimit {
            rate_limit: RateLimitEvent {
                at_ms: T_429,
                upstream: true,
                retry_after_s: None,
            },
            reading: evidence,
        },
    }
}

fn not_confirmed(outcome: ConfirmationOutcome) -> NotConfirmedReason {
    match outcome {
        ConfirmationOutcome::NotConfirmed { reason, .. } => reason,
        other => panic!("expected no confirmation, got {other:?}"),
    }
}

fn confirmed(outcome: ConfirmationOutcome) -> prefrontal_routing_core::accounts::Fence {
    match outcome {
        ConfirmationOutcome::Confirmed { fence } => fence,
        other => panic!("expected confirmation, got {other:?}"),
    }
}

/// Routing's own expectation for the `headers` rows, kept apart from the
/// rounding test so the two policies cannot be confused: raw 0.9949, 0.995
/// and 0.999 never confirm although they normalize to 99-100; raw 1 and 1.01
/// confirm. The same raw 0.995 reading, submitted directly, through the
/// passive feed, or carried by Insula after a 429 and observed at 12:00:01Z,
/// creates no shared fence on any path.
#[test]
fn raw_utilization_below_one_never_confirms_on_any_path() {
    for (raw, confirms) in [
        (0.9949, false),
        (0.995, false),
        (0.999, false),
        (1.0, true),
        (1.01, true),
    ] {
        let outcome = confirm_exhaustion(&served(header_evidence(
            raw,
            Carrier::Direct,
            FrameOrigin::Direct,
            T_AFTER,
        )));
        assert_eq!(
            matches!(outcome, ConfirmationOutcome::Confirmed { .. }),
            confirms,
            "raw {raw}: {outcome:?}"
        );
    }
    // Direct submission.
    assert_eq!(
        not_confirmed(confirm_exhaustion(&served(header_evidence(
            0.995,
            Carrier::Direct,
            FrameOrigin::Direct,
            T_AFTER
        )))),
        NotConfirmedReason::RawBelowThreshold
    );
    // Passive feed and Insula-carried headers, both as source 2 (complete,
    // post-429) and as source 3.
    for carrier in [Carrier::PassiveFeed, Carrier::Insula] {
        let evidence = header_evidence(0.995, carrier, FrameOrigin::Direct, T_AFTER);
        assert_eq!(
            not_confirmed(confirm_exhaustion(&after_429(evidence.clone()))),
            NotConfirmedReason::RawBelowThreshold,
            "{carrier:?} after 429"
        );
        assert_eq!(
            not_confirmed(confirm_exhaustion(&served(evidence))),
            NotConfirmedReason::RawBelowThreshold,
            "{carrier:?} served"
        );
    }
}

#[test]
fn source_two_causality_uses_the_original_observation_time() {
    // Observed 11:59:59Z, forwarded or re-read at 12:00:02Z: not causal.
    for carrier in [Carrier::PassiveFeed, Carrier::Insula] {
        let evidence = header_evidence(0.995, carrier, FrameOrigin::Direct, T_BEFORE);
        assert_eq!(evidence.received_at_ms, T_FORWARD);
        assert_eq!(
            not_confirmed(confirm_exhaustion(&after_429(evidence))),
            NotConfirmedReason::ObservedBeforeRateLimit,
            "{carrier:?}"
        );
    }
    // Authoritative poll-sourced exhaustion confirms only when originally
    // observed after the 429, whatever its forwarding time.
    let poll = |observed_at| WindowEvidence {
        scope: EvidenceScope::General(GeneralWindow::SevenDay),
        derivation: Derivation::Poll {
            used_percent: 100.0,
        },
        carrier: Carrier::Insula,
        observed_at_ms: observed_at,
        received_at_ms: T_FORWARD,
        reset: ResetTime::At(NOW + 48 * HOUR),
    };
    assert_eq!(
        not_confirmed(confirm_exhaustion(&after_429(poll(T_BEFORE)))),
        NotConfirmedReason::ObservedBeforeRateLimit
    );
    let fence = confirmed(confirm_exhaustion(&after_429(poll(T_AFTER))));
    assert_eq!(fence.pool, Some(PoolKey::general(GeneralWindow::SevenDay)));
    assert_eq!(fence.observed_at_ms, T_AFTER);
    assert_eq!(fence.source, ConfirmationSource::PostRateLimitPoll);
    // A poll reading below 100 is not exhaustion.
    let mut healthy = poll(T_AFTER);
    healthy.derivation = Derivation::Poll { used_percent: 99.0 };
    assert_eq!(
        not_confirmed(confirm_exhaustion(&after_429(healthy))),
        NotConfirmedReason::PollNotExhausted
    );
}

/// The design's raw-429 row: an unconfirmed upstream 429 keeps its real
/// Retry-After for the transient retry, and that hint never becomes the reset
/// of a fence confirmed from a post-429 reading.
#[test]
fn transient_raw_429_keeps_its_real_retry_after_separately() {
    let observation = |used_percent| ExhaustionObservation {
        account_id: "acct-a".into(),
        bound: true,
        kind: ObservationKind::PostRateLimit {
            rate_limit: RateLimitEvent {
                at_ms: T_429,
                upstream: true,
                retry_after_s: Some(30),
            },
            reading: WindowEvidence {
                scope: EvidenceScope::General(GeneralWindow::FiveHour),
                derivation: Derivation::Poll { used_percent },
                carrier: Carrier::Insula,
                observed_at_ms: T_AFTER,
                received_at_ms: T_FORWARD,
                reset: ResetTime::At(NOW + 2 * HOUR),
            },
        },
    };
    assert_eq!(
        confirm_exhaustion(&observation(99.0)),
        ConfirmationOutcome::NotConfirmed {
            reason: NotConfirmedReason::PollNotExhausted,
            transient_retry_after_s: Some(30),
        }
    );
    let fence = confirmed(confirm_exhaustion(&observation(100.0)));
    assert_eq!(fence.reset, ResetTime::At(NOW + 2 * HOUR));
    assert_eq!(fence.retry_hint_ms, None);
}

#[test]
fn positive_controls_raw_one_on_genuine_frames_confirm() {
    for frame in [
        FrameOrigin::Direct,
        FrameOrigin::HttpRelayUpstream,
        FrameOrigin::WsResponseStart,
    ] {
        for carrier in [Carrier::Direct, Carrier::PassiveFeed, Carrier::Insula] {
            let fence = confirmed(confirm_exhaustion(&served(header_evidence(
                1.0, carrier, frame, T_AFTER,
            ))));
            assert_eq!(fence.pool, Some(PoolKey::general(GeneralWindow::FiveHour)));
            assert_eq!(fence.source, ConfirmationSource::RawHeader);
            assert_eq!(fence.reset, ResetTime::At(NOW + 15 * MINUTE));
        }
    }
    let fence = confirmed(confirm_exhaustion(&after_429(header_evidence(
        1.0,
        Carrier::PassiveFeed,
        FrameOrigin::Direct,
        T_AFTER,
    ))));
    assert_eq!(fence.source, ConfirmationSource::PostRateLimitHeader);
}

#[test]
fn optimistic_unbound_and_relay_evidence_never_confirm() {
    for carrier in [Carrier::Direct, Carrier::PassiveFeed, Carrier::Insula] {
        assert_eq!(
            not_confirmed(confirm_exhaustion(&served(header_evidence(
                1.0,
                carrier,
                FrameOrigin::OptimisticWrapper,
                T_AFTER
            )))),
            NotConfirmedReason::OptimisticFrame
        );
    }
    let mut unbound = served(header_evidence(
        1.0,
        Carrier::Direct,
        FrameOrigin::Direct,
        T_AFTER,
    ));
    unbound.bound = false;
    assert_eq!(
        not_confirmed(confirm_exhaustion(&unbound)),
        NotConfirmedReason::Unbound
    );

    let mut relay = after_429(header_evidence(
        1.0,
        Carrier::Direct,
        FrameOrigin::Direct,
        T_AFTER,
    ));
    if let ObservationKind::PostRateLimit { rate_limit, .. } = &mut relay.kind {
        rate_limit.upstream = false;
    }
    assert_eq!(
        not_confirmed(confirm_exhaustion(&relay)),
        NotConfirmedReason::RelayRateLimit
    );

    let mut dropped = header_evidence(1.0, Carrier::Insula, FrameOrigin::Direct, T_AFTER);
    dropped.derivation = Derivation::Header {
        frame: FrameOrigin::Direct,
        raw_utilization: None,
    };
    assert_eq!(
        not_confirmed(confirm_exhaustion(&served(dropped))),
        NotConfirmedReason::RawMissing
    );
}

#[test]
fn broca_limit_windows_map_to_pool_keys_and_retry_after_is_not_a_reset() {
    let broca = |window: Option<&str>, pool: Option<&str>, source| ExhaustionObservation {
        account_id: "acct-a".into(),
        bound: true,
        kind: ObservationKind::BrocaLimitExhausted {
            limit_window: window.map(str::to_string),
            pool: pool.map(str::to_string),
            requested_family: ModelFamily::Opus,
            resets_at_ms: Some(NOW + 10 * MINUTE),
            resets_at_source: source,
            observed_at_ms: NOW,
        },
    };
    let five = confirmed(confirm_exhaustion(&broca(
        Some("five_hour"),
        None,
        Some(ResetSource::Header),
    )));
    assert_eq!(five.pool, Some(PoolKey::general(GeneralWindow::FiveHour)));
    assert_eq!(five.reset, ResetTime::At(NOW + 10 * MINUTE));
    let seven = confirmed(confirm_exhaustion(&broca(Some("seven_day"), None, None)));
    assert_eq!(seven.pool, Some(PoolKey::general(GeneralWindow::SevenDay)));
    // An exhaustion naming no pool blocks the whole account, whatever the
    // provider (a ChatGPT `usage_limit_reached` names none either).
    let windowless = confirmed(confirm_exhaustion(&broca(
        None,
        None,
        Some(ResetSource::Header),
    )));
    assert_eq!(windowless.pool, None);
    let hinted = confirmed(confirm_exhaustion(&broca(
        Some("five_hour"),
        None,
        Some(ResetSource::RetryAfter),
    )));
    assert_eq!(hinted.reset, ResetTime::Missing);
    assert_eq!(hinted.retry_hint_ms, Some(NOW + 10 * MINUTE));
    // A window routing has no length for is a named pool of unknown length,
    // scoped to the reported family; it is no longer left unclassified.
    let overage = confirmed(confirm_exhaustion(&broca(Some("overage"), None, None)));
    assert_eq!(
        overage.pool,
        Some(PoolKey {
            window_minutes: None,
            scope: PoolScope::Family(ModelFamily::Opus),
            name: Some("overage".into()),
        })
    );
    // The report's pool member names the pool; its window gives the length.
    let pooled = confirmed(confirm_exhaustion(&broca(
        Some("seven_day"),
        Some("3p"),
        None,
    )));
    assert_eq!(
        pooled.pool,
        Some(PoolKey {
            window_minutes: Some(10_080),
            scope: PoolScope::Family(ModelFamily::Opus),
            name: Some("3p".into()),
        })
    );
}

/// The three laundering paths' allocator side: routing holds the rounded-100
/// reading (raw 0.995) in the full band with no fence, and the 5h reset is 14
/// minutes away. The conversation gets a soft, local proactive hold, as the
/// reference's pre-send check gives it: the hold's basis is the reading, not a
/// confirmed fence, and the account is not fenced for cold work, only
/// zero-weighted.
#[test]
fn laundering_paths_give_only_a_soft_local_hold_and_no_shared_fence() {
    // Direct submission and the passive feed both land as header readings;
    // Insula-carried headers keep their Insula source.
    for (path, source) in [
        ("direct", ObservationSource::Header),
        ("passive-feed", ObservationSource::Header),
        ("insula", ObservationSource::Insula),
    ] {
        let mut laundered = reading(0.0, 20.0);
        laundered.observed_at_ms = Some(T_AFTER);
        laundered.five_hour = Some(header_window(
            GeneralWindow::FiveHour,
            0.995,
            Some((NOW + 14 * MINUTE) as f64 / 1000.0),
            source,
        ));
        let mut st = state(vec![
            account("acct-a", Some(laundered)),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
        let decision = decide(&request(), &st, NOW);
        let hold = decision
            .hold()
            .unwrap_or_else(|| panic!("{path}: expected a hold, got {:?}", decision.outcome));
        assert_eq!(hold.account_id, "acct-a", "{path}");
        assert_eq!(hold.basis, HoldBasis::ProactiveReading, "{path}");
        assert_eq!(
            (hold.base_retry_after_s, hold.retry_after_s),
            (840, 846),
            "{path}"
        );
        let note = decision.account_note("acct-a").unwrap();
        assert!(
            !note.exclusions.iter().any(|reason| matches!(
                reason,
                prefrontal_routing_core::accounts::ExclusionReason::ConfirmedFence { .. }
            )),
            "{:?}",
            note.exclusions
        );
    }
}
