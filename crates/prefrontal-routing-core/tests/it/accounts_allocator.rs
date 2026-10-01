//! One test per allocator rule in `docs/designs/account-selection.md`,
//! "New-session rule: sticky-balanced first" (steps 1-7 and the precedence
//! table), plus the operator's reply-code ruling for pools emptied by model
//! reach or runner-candidate narrowing.

#[path = "../accounts_support/mod.rs"]
mod accounts_support;

use accounts_support::*;
use prefrontal_routing_core::accounts::evidence::MigrateReason;
use prefrontal_routing_core::accounts::{
    decide, AccountRef, AdmissionState, AdmitReason, AffinityNote, AffinityOutcome, AgeBand,
    ApplicableWindow, CallerKind, CredentialRecord, Decision, ExclusionReason, FenceScope,
    GeneralWindow, HoldBasis, ImportNote, InvalidationReason, ModelFamily, ModelReach,
    OperatorOverride, OverrideNote, PreviousPin, PrincipalExclusion, RefusalCode,
    RequestedAccountIgnored, RequestedAccountNote, ResetTime, ScopedList, ScopedListState,
    ScopedWindow, StaleDisclosure,
};

fn admitted(decision: &Decision) -> &str {
    &decision
        .admission()
        .unwrap_or_else(|| panic!("expected admission, got {:?}", decision.outcome))
        .account_id
}

fn refused(decision: &Decision) -> RefusalCode {
    decision
        .refusal()
        .unwrap_or_else(|| panic!("expected refusal, got {:?}", decision.outcome))
        .code
}

fn exclusions<'a>(decision: &'a Decision, account: &str) -> &'a [ExclusionReason] {
    &decision
        .account_note(account)
        .expect("account note")
        .exclusions
}

fn invalidated(decision: &Decision) -> InvalidationReason {
    match &decision.explanation.affinity {
        AffinityNote::Invalidated { reason, .. } => reason.clone(),
        other => panic!("expected invalidated affinity, got {other:?}"),
    }
}

// ---- step 1 and the precedence table -----------------------------------

#[test]
fn unsupported_provider_is_account_selection_unsupported() {
    let mut req = request();
    req.provider = "google".into();
    let decision = decide(
        &req,
        &state(vec![account("acct-a", Some(reading(10.0, 10.0)))]),
        NOW,
    );
    assert_eq!(refused(&decision), RefusalCode::AccountSelectionUnsupported);
    assert!(!decision.refusal().unwrap().retryable);
}

#[test]
fn provider_and_models_are_normalized() {
    let mut req = request();
    req.provider = " Anthropic ".into();
    req.model = "Claude-Opus-5-5".into();
    req.affinity_model = "Claude-Opus-5-5".into();
    let decision = decide(
        &req,
        &state(vec![account("acct-a", Some(reading(10.0, 10.0)))]),
        NOW,
    );
    assert_eq!(admitted(&decision), "acct-a");
    assert_eq!(decision.explanation.family, ModelFamily::Opus);
    assert_eq!(decision.explanation.model, "claude-opus-5-5");
}

#[test]
fn operator_override_beats_affinity_and_scoring() {
    let mut st = state(vec![
        account("acct-a", Some(reading(10.0, 10.0))),
        account("acct-b", Some(reading(80.0, 80.0))),
    ]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    st.operator_override = Some(OperatorOverride {
        target: AccountRef::AccountId("acct-b".into()),
    });
    let decision = decide(&request(), &st, NOW);
    let admission = decision.admission().unwrap();
    assert_eq!(admission.account_id, "acct-b");
    assert_eq!(admission.reason, AdmitReason::OperatorOverride);
    assert_eq!(
        admission.affinity,
        AffinityOutcome::New {
            previous: Some(PreviousPin {
                pin_id: "pin-acct-a".into(),
                account_id: "acct-a".into(),
                credential_id: credential_id("acct-a"),
                pin_generation: 3,
            }),
            pin_generation: 4,
        }
    );
    assert_eq!(invalidated(&decision), InvalidationReason::OperatorOverride);
}

#[test]
fn operator_override_admits_unknown_or_below_reserve_quota() {
    let mut st = state(vec![
        account("acct-a", Some(reading(10.0, 10.0))),
        account("acct-b", None),
    ]);
    st.operator_override = Some(OperatorOverride {
        target: AccountRef::CredentialId(credential_id("acct-b")),
    });
    let decision = decide(&request(), &st, NOW);
    let admission = decision.admission().unwrap();
    assert_eq!(admission.account_id, "acct-b");
    assert_eq!(admission.reason, AdmitReason::OperatorOverrideUnknownQuota);

    let mut st = state(vec![account("acct-b", Some(reading(10.0, 99.5)))]);
    st.operator_override = Some(OperatorOverride {
        target: AccountRef::AccountId("acct-b".into()),
    });
    let decision = decide(&request(), &st, NOW);
    assert_eq!(
        decision.admission().unwrap().reason,
        AdmitReason::OperatorOverride
    );
}

/// The override fails visibly with the code its hard filter implies and never
/// picks another account (operator ruling on the design's reply codes).
#[test]
fn operator_override_failing_a_hard_filter_refuses_with_that_filters_code() {
    let base = || {
        let mut st = state(vec![
            account("acct-a", Some(reading(10.0, 10.0))),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.operator_override = Some(OperatorOverride {
            target: AccountRef::AccountId("acct-b".into()),
        });
        st
    };
    let check = |st: &prefrontal_routing_core::accounts::AllocatorState,
                 req: &prefrontal_routing_core::accounts::AccountRequest,
                 code| {
        let decision = decide(req, st, NOW);
        assert_eq!(refused(&decision), code);
        assert!(matches!(
            decision.explanation.operator_override,
            Some(OverrideNote::Rejected { .. })
        ));
    };
    // Model reach.
    let mut st = base();
    st.roster.as_mut().unwrap().accounts[1].model_reach =
        ModelReach::Only(vec!["claude-opus-5-5".into()]);
    check(&st, &request(), RefusalCode::AccountSelectionUnsupported);
    // Runner candidacy.
    let mut req = request();
    req.candidates = Some(candidates(&["acct-a"]));
    check(&base(), &req, RefusalCode::AccountSelectionUnavailable);
    // Refused for this principal.
    let mut st = base();
    st.principal_exclusions.push(PrincipalExclusion {
        principal: request().principal,
        credential_id: credential_id("acct-b"),
        runner_view_digest: "runner-view-1".into(),
        roster_view_digest: "roster-1".into(),
        recorded_at_ms: NOW - MINUTE,
    });
    check(&st, &request(), RefusalCode::AccountNotAuthorized);
    // Re-auth.
    let mut st = base();
    st.roster.as_mut().unwrap().accounts[1].credentials[0].needs_reauth = true;
    check(&st, &request(), RefusalCode::AccountReauthRequired);
    // Confirmed fence.
    let mut st = base();
    st.fences.push(fence(
        "acct-b",
        FenceScope::General(GeneralWindow::SevenDay),
        ResetTime::At(NOW + HOUR),
    ));
    check(&st, &request(), RefusalCode::AccountPoolExhausted);
    // Not in routing's roster.
    let mut st = base();
    st.operator_override = Some(OperatorOverride {
        target: AccountRef::AccountId("acct-missing".into()),
    });
    check(&st, &request(), RefusalCode::AccountSelectionUnavailable);
}

#[test]
fn requested_account_needs_a_grant_and_a_score_within_factor_two() {
    let accounts = |b_seven: f64| {
        state(vec![
            account("acct-a", Some(reading(10.0, 10.0))),
            account("acct-b", Some(reading(10.0, b_seven))),
        ])
    };
    let mut req = request();
    req.requested_account = Some(AccountRef::AccountId("acct-b".into()));

    let decision = decide(&req, &accounts(40.0), NOW);
    assert_eq!(admitted(&decision), "acct-a");
    assert_eq!(
        decision.explanation.requested_account,
        Some(RequestedAccountNote::Ignored {
            reason: RequestedAccountIgnored::NotGranted
        })
    );

    let granted = |b_seven| {
        let mut st = accounts(b_seven);
        st.policy.account_request_principals = vec![request().principal];
        st
    };
    // 7d at 40% scores 89/59 = 1.51 times the best: within the factor.
    let decision = decide(&req, &granted(40.0), NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        decision.admission().unwrap().reason,
        AdmitReason::RequestedAccount
    );
    // 7d at 70% scores 89/29 = 3.07 times the best: outside it.
    let decision = decide(&req, &granted(70.0), NOW);
    assert_eq!(admitted(&decision), "acct-a");
    assert_eq!(
        decision.explanation.requested_account,
        Some(RequestedAccountNote::Ignored {
            reason: RequestedAccountIgnored::OutsideFactor
        })
    );
}

#[test]
fn preferred_existing_account_import_is_validated_once_and_never_beats_affinity() {
    let pool = || {
        state(vec![
            account("acct-a", Some(reading(10.0, 10.0))),
            account("acct-b", Some(reading(50.0, 50.0))),
        ])
    };
    let mut req = request();
    req.preferred_existing_account = Some(AccountRef::AccountId("acct-b".into()));

    let decision = decide(&req, &pool(), NOW);
    let admission = decision.admission().unwrap();
    assert_eq!(admission.account_id, "acct-b");
    assert_eq!(admission.reason, AdmitReason::ImportedAffinity);
    assert_eq!(
        admission.affinity,
        AffinityOutcome::New {
            previous: None,
            pin_generation: 1
        }
    );

    let mut st = pool();
    st.roster.as_mut().unwrap().accounts[1].credentials[0].needs_reauth = true;
    let decision = decide(&req, &st, NOW);
    assert_eq!(admitted(&decision), "acct-a");
    assert_eq!(
        decision.explanation.import,
        Some(ImportNote::Rejected {
            reasons: vec![ExclusionReason::ReauthRequired],
            not_in_roster: false
        })
    );

    // Step 3 applies too: a reading in the excluded age band is rejected.
    let mut st = pool();
    st.roster.as_mut().unwrap().accounts[1]
        .quota
        .as_mut()
        .unwrap()
        .observed_at_ms = Some(NOW - 25 * MINUTE);
    let decision = decide(&req, &st, NOW);
    assert_eq!(admitted(&decision), "acct-a");
    assert!(matches!(
        decision.explanation.import,
        Some(ImportNote::Rejected { .. })
    ));

    let mut st = pool();
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 2));
    let decision = decide(&req, &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
    assert_eq!(decision.explanation.import, None);
}

// ---- step 2: affinity ----------------------------------------------------

/// Break-it target: a better percentage elsewhere, a reading below the
/// reserve, or a stale reading is not a migration reason.
#[test]
fn affinity_is_retained_despite_better_score_below_reserve_or_stale_reading() {
    let mut st = state(vec![
        account("acct-a", Some(reading(50.0, 50.0))),
        account("acct-b", Some(reading(0.0, 0.0))),
    ]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    let decision = decide(&request(), &st, NOW);
    let admission = decision.admission().unwrap();
    assert_eq!(admission.account_id, "acct-a");
    assert_eq!(admission.reason, AdmitReason::StickyHit);
    assert_eq!(
        admission.affinity,
        AffinityOutcome::Retained {
            pin_id: "pin-acct-a".into(),
            pin_generation: 3
        }
    );

    // Below the reserve (99.5% used), and the Broca floor never moves an
    // established conversation either.
    st.roster.as_mut().unwrap().accounts[0].quota = Some(reading(10.0, 99.5));
    let mut broca = request();
    broca.caller = CallerKind::Broca;
    let decision = decide(&broca, &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
    assert_eq!(admitted(&decision), "acct-a");

    // Stale: a 100% reading in the excluded band is not fresh, so the
    // conversation stays, with the degraded reason recorded.
    let mut stale = reading(10.0, 100.0);
    stale.observed_at_ms = Some(NOW - 30 * MINUTE);
    st.roster.as_mut().unwrap().accounts[0].quota = Some(stale);
    let decision = decide(&request(), &st, NOW);
    assert_eq!(admitted(&decision), "acct-a");
    let AffinityNote::Retained { degraded, .. } = &decision.explanation.affinity else {
        panic!("{:?}", decision.explanation.affinity);
    };
    assert!(matches!(
        degraded[..],
        [ExclusionReason::AgeExcluded { .. }]
    ));

    // In the discounted band a 100% reading is not fresh either: retained.
    let mut discounted = reading(10.0, 100.0);
    discounted.observed_at_ms = Some(NOW - 6 * MINUTE);
    st.roster.as_mut().unwrap().accounts[0].quota = Some(discounted);
    let decision = decide(&request(), &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
    assert_eq!(admitted(&decision), "acct-a");
}

#[test]
fn user_model_change_is_cold_work_with_a_new_generation() {
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.affinity = Some(affinity("acct-a", "claude-opus-5-5", 3));
    let decision = decide(&request(), &st, NOW);
    assert_eq!(invalidated(&decision), InvalidationReason::UserModelChange);
    let admission = decision.admission().unwrap();
    assert_eq!(admission.reason, AdmitReason::ColdScored);
    assert!(matches!(
        admission.affinity,
        AffinityOutcome::New {
            pin_generation: 4,
            ..
        }
    ));
}

#[test]
fn internal_safety_recovery_keeps_the_affinity_model() {
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.affinity = Some(affinity("acct-a", "claude-fable-5-1", 3));
    let mut req = with_model(request(), "claude-fable-5-1");
    req.model = "claude-opus-5-5".into();
    req.via_safety_recovery = true;
    let decision = decide(&req, &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
    assert!(!decision.explanation.direct_opus);
}

#[test]
fn idle_expiry_invalidates_unless_a_lease_is_active() {
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    let mut expired = affinity("acct-a", "claude-sonnet-5", 3);
    expired.expires_at_ms = NOW;
    st.affinity = Some(expired.clone());
    let decision = decide(&request(), &st, NOW);
    assert_eq!(invalidated(&decision), InvalidationReason::IdleExpired);

    expired.lease_active = true;
    st.affinity = Some(expired);
    let decision = decide(&request(), &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
}

#[test]
fn fresh_full_band_reading_at_100_on_seven_day_moves_without_a_fence() {
    let mut full = reading(10.0, 100.0);
    full.observed_at_ms = Some(NOW - MINUTE);
    let mut st = state(vec![
        account("acct-a", Some(full)),
        account("acct-b", Some(reading(10.0, 10.0))),
    ]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    let decision = decide(&request(), &st, NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::FreshFullReading {
            window: ApplicableWindow::SevenDay
        }
    );
    // It is not a fence: for cold work the account is only zero-weighted.
    assert!(matches!(
        exclusions(&decision, "acct-a"),
        [ExclusionReason::BelowReserve { .. }]
    ));
}

#[test]
fn confirmed_short_hold_returns_account_hold_within_the_callers_wait() {
    let pool = |reset| {
        let mut st = state(vec![
            account("acct-a", Some(reading(10.0, 10.0))),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
        st.fences.push(five_hour_fence("acct-a", reset));
        st
    };
    let st = pool(ResetTime::At(NOW + 14 * MINUTE));
    let decision = decide(&request(), &st, NOW);
    let hold = decision.hold().expect("hold");
    assert_eq!(hold.account_id, "acct-a");
    assert_eq!(hold.base_retry_after_s, 840);
    assert_eq!(hold.retry_after_s, 846);
    assert_eq!(hold.pin_generation, 3);

    // Retry-After above `hold_wait_max_s`, or a wait of 0: a sibling.
    for wait in [845, 0] {
        let mut req = request();
        req.hold_wait_max_s = wait;
        let decision = decide(&req, &st, NOW);
        assert_eq!(admitted(&decision), "acct-b");
        assert_eq!(
            invalidated(&decision),
            InvalidationReason::HoldExceedsWait { retry_after_s: 846 }
        );
    }

    // Past the 15-minute bound, or with only a Retry-After hint: migrate.
    for reset in [ResetTime::At(NOW + 15 * MINUTE + 1), ResetTime::Missing] {
        let decision = decide(&request(), &pool(reset), NOW);
        assert_eq!(admitted(&decision), "acct-b");
        assert_eq!(
            invalidated(&decision),
            InvalidationReason::ConfirmedExhaustion {
                migrate: MigrateReason::FiveHour
            }
        );
    }
}

/// A full-band 5h reading at 100% (not confirmation) holds softly with the
/// same exact bounds as a confirmed fence; anything else about the reading or
/// its reset moves the conversation or keeps it, never holds it.
#[test]
fn soft_proactive_hold_bounds_and_reevaluation() {
    let pool = |five: prefrontal_routing_core::accounts::QuotaReading| {
        let mut st = state(vec![
            account("acct-a", Some(five)),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
        st
    };
    let full_100 = |reset| {
        let mut r = reading(100.0, 10.0);
        r.five_hour.as_mut().unwrap().resets_at = reset;
        r
    };
    // At +15 min the jittered Retry-After is 906 s, so the caller must be
    // willing to wait that long.
    let mut patient = request();
    patient.hold_wait_max_s = 906;
    let hold = decide(
        &patient,
        &pool(full_100(ResetTime::At(NOW + 15 * MINUTE))),
        NOW,
    );
    let hold = hold.hold().expect("+15 min holds");
    assert_eq!((hold.base_retry_after_s, hold.retry_after_s), (900, 906));

    for reset in [
        ResetTime::At(NOW + 15 * MINUTE + 1),
        ResetTime::At(NOW),
        ResetTime::At(NOW - MINUTE),
        ResetTime::Unparsable,
        ResetTime::Missing,
    ] {
        // Even a caller willing to wait migrates: the reset rules out a hold.
        patient.hold_wait_max_s = 3600;
        let decision = decide(&patient, &pool(full_100(reset)), NOW);
        assert_eq!(admitted(&decision), "acct-b", "{reset:?}");
        assert_eq!(
            invalidated(&decision),
            InvalidationReason::FreshFullReading {
                window: ApplicableWindow::FiveHour
            },
            "{reset:?}"
        );
    }

    // A stale reading never justifies it: the conversation stays.
    let mut stale = full_100(ResetTime::At(NOW + 14 * MINUTE));
    stale.observed_at_ms = Some(NOW - 6 * MINUTE);
    let decision = decide(&request(), &pool(stale), NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);

    // Re-evaluated each admission: a newer reading below 100 removes it.
    let decision = decide(&request(), &pool(reading(60.0, 10.0)), NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);

    // The caller's wait bounds it like a confirmed hold.
    let mut req = request();
    req.hold_wait_max_s = 845;
    let decision = decide(&req, &pool(full_100(ResetTime::At(NOW + 14 * MINUTE))), NOW);
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::HoldExceedsWait { retry_after_s: 846 }
    );
}

/// A full-band family-scope or 7d reading at 100% outranks both a confirmed
/// and a soft 5h short hold.
#[test]
fn fresh_scope_or_seven_day_100_outranks_any_five_hour_hold() {
    let mut fable_full = reading(100.0, 10.0);
    fable_full.five_hour.as_mut().unwrap().resets_at = ResetTime::At(NOW + 14 * MINUTE);
    fable_full.scoped = ScopedList::Listed(vec![fable_window(100.0)]);
    let mut seven_full = reading(100.0, 100.0);
    seven_full.five_hour.as_mut().unwrap().resets_at = ResetTime::At(NOW + 14 * MINUTE);
    for (model, quota, window) in [
        ("claude-fable-5", fable_full, ApplicableWindow::FamilyScope),
        ("claude-sonnet-5", seven_full, ApplicableWindow::SevenDay),
    ] {
        let mut healthy = reading(10.0, 10.0);
        healthy.scoped = ScopedList::Listed(vec![fable_window(10.0)]);
        let mut st = state(vec![
            account("acct-a", Some(quota)),
            account("acct-b", Some(healthy)),
        ]);
        st.affinity = Some(affinity("acct-a", model, 3));
        // Soft 5h hold candidate only.
        let decision = decide(&with_model(request(), model), &st, NOW);
        assert_eq!(
            invalidated(&decision),
            InvalidationReason::FreshFullReading { window }
        );
        assert_eq!(admitted(&decision), "acct-b");
        // Plus a confirmed 5h fence 14 minutes from reset.
        st.fences
            .push(five_hour_fence("acct-a", ResetTime::At(NOW + 14 * MINUTE)));
        let decision = decide(&with_model(request(), model), &st, NOW);
        assert_eq!(
            invalidated(&decision),
            InvalidationReason::FreshFullReading { window }
        );
        assert_eq!(admitted(&decision), "acct-b");
    }
}

/// A move justified only by a fresh (unconfirmed) 100% reading, with no
/// sibling to take the conversation, refuses as reserve-limited, not
/// exhausted, and issues no pin: the old pin stays in the explanation as the
/// conversation's historical identity, generation unchanged.
#[test]
fn fresh_only_move_without_a_sibling_is_reserve_limited_with_no_new_pin() {
    let mut seven = reading(10.0, 100.0);
    seven.observed_at_ms = Some(NOW - MINUTE);
    let mut five_no_reset = reading(100.0, 10.0);
    five_no_reset.five_hour.as_mut().unwrap().resets_at = ResetTime::Missing;
    let mut fable = reading(10.0, 10.0);
    fable.scoped = ScopedList::Listed(vec![fable_window(100.0)]);
    for (model, quota) in [
        ("claude-sonnet-5", seven),
        ("claude-sonnet-5", five_no_reset),
        ("claude-fable-5", fable),
    ] {
        let mut st = state(vec![account("acct-a", Some(quota))]);
        st.affinity = Some(affinity("acct-a", model, 3));
        let decision = decide(&with_model(request(), model), &st, NOW);
        assert_eq!(refused(&decision), RefusalCode::AccountPoolReserveLimited);
        assert!(decision.admission().is_none());
        assert_eq!(
            decision.explanation.previous_pin,
            Some(PreviousPin {
                pin_id: "pin-acct-a".into(),
                account_id: "acct-a".into(),
                credential_id: credential_id("acct-a"),
                pin_generation: 3,
            })
        );
    }
}

/// With both a confirmed 5h fence and a fresh 5h 100% reading present and
/// disagreeing on the reset, the confirmed fence governs the hold decision.
#[test]
fn confirmed_five_hour_fence_governs_over_a_conflicting_fresh_reading() {
    let pool = |fence_reset, reading_reset| {
        let mut fresh = reading(100.0, 10.0);
        fresh.five_hour.as_mut().unwrap().resets_at = reading_reset;
        let mut st = state(vec![
            account("acct-a", Some(fresh)),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
        st.fences.push(five_hour_fence("acct-a", fence_reset));
        st
    };
    // Fence resets in 2 h, reading says 14 min: migrate, no soft hold.
    let st = pool(
        ResetTime::At(NOW + 2 * HOUR),
        ResetTime::At(NOW + 14 * MINUTE),
    );
    let decision = decide(&request(), &st, NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::ConfirmedExhaustion {
            migrate: MigrateReason::FiveHour
        }
    );
    // Fence resets in 14 min, reading says 2 h: a confirmed hold.
    let st = pool(
        ResetTime::At(NOW + 14 * MINUTE),
        ResetTime::At(NOW + 2 * HOUR),
    );
    let decision = decide(&request(), &st, NOW);
    let hold = decision.hold().expect("confirmed hold");
    assert_eq!(hold.basis, HoldBasis::ConfirmedFence);
    assert_eq!(hold.reset_at_ms, NOW + 14 * MINUTE);
    assert_eq!(hold.retry_after_s, 846);
}

#[test]
fn confirmed_long_and_scoped_exhaustion_migrate_in_reference_order() {
    let pool = |model: &str| {
        let mut st = state(vec![
            account("acct-a", Some(reading(10.0, 10.0))),
            account("acct-b", Some(reading(10.0, 10.0))),
        ]);
        st.affinity = Some(affinity("acct-a", model, 3));
        st
    };
    let mut st = pool("claude-sonnet-5");
    st.fences.push(fence(
        "acct-a",
        FenceScope::General(GeneralWindow::SevenDay),
        ResetTime::At(NOW + HOUR),
    ));
    st.fences
        .push(five_hour_fence("acct-a", ResetTime::At(NOW + 5 * MINUTE)));
    let decision = decide(&request(), &st, NOW);
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::ConfirmedExhaustion {
            migrate: MigrateReason::SevenDay
        }
    );

    // A Fable fence moves a Fable conversation, never an Opus one.
    let fable_fence = fence(
        "acct-a",
        FenceScope::Family(ModelFamily::Fable),
        ResetTime::At(NOW + HOUR),
    );
    let mut st = pool("claude-fable-5-1");
    st.fences.push(fable_fence.clone());
    let decision = decide(&with_model(request(), "claude-fable-5-1"), &st, NOW);
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::ConfirmedExhaustion {
            migrate: MigrateReason::ModelScoped
        }
    );
    let mut st = pool("claude-opus-5-5");
    st.fences.push(fable_fence);
    let decision = decide(&with_model(request(), "claude-opus-5-5"), &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);
}

#[test]
fn unknown_scope_fence_moves_only_its_family() {
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    st.fences.push(fence(
        "acct-a",
        FenceScope::UnknownScope(ModelFamily::Opus),
        ResetTime::Missing,
    ));
    let decision = decide(&request(), &st, NOW);
    assert_eq!(decision.admission().unwrap().reason, AdmitReason::StickyHit);

    st.affinity = Some(affinity("acct-a", "claude-opus-5-5", 3));
    let decision = decide(&with_model(request(), "claude-opus-5-5"), &st, NOW);
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::UnknownScopeFence
    );
    // Cold Opus work is blocked on that account too.
    assert_eq!(refused(&decision), RefusalCode::AccountPoolExhausted);
}

#[test]
fn principal_refusal_exclusion_invalidates_affinity_until_a_digest_moves() {
    let exclusion = PrincipalExclusion {
        principal: request().principal,
        credential_id: credential_id("acct-a"),
        runner_view_digest: "runner-view-1".into(),
        roster_view_digest: "roster-1".into(),
        recorded_at_ms: NOW - HOUR,
    };
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    st.principal_exclusions.push(exclusion.clone());
    let mut req = request();
    req.candidates = Some(candidates(&["acct-a"]));
    let decision = decide(&req, &st, NOW);
    assert_eq!(
        invalidated(&decision),
        InvalidationReason::PrincipalExcluded
    );
    assert_eq!(refused(&decision), RefusalCode::AccountNotAuthorized);

    // Another principal is unaffected.
    let mut other = req.clone();
    other.principal = "reserved:broca".into();
    assert_eq!(
        decide(&other, &st, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );

    // The runner reports a different view digest: the exclusion ends.
    let mut moved = req.clone();
    moved.candidates.as_mut().unwrap().view_digest = "runner-view-2".into();
    assert_eq!(
        decide(&moved, &st, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );

    // Routing's roster digest moves, or 24 hours pass: it ends too.
    let mut rolled = st.clone();
    rolled.roster.as_mut().unwrap().view_digest = "roster-2".into();
    assert_eq!(
        decide(&req, &rolled, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );
    let mut aged = st.clone();
    aged.principal_exclusions[0].recorded_at_ms = NOW - 24 * HOUR;
    assert_eq!(
        decide(&req, &aged, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );
}

#[test]
fn identity_withdrawal_and_credential_rebind_invalidate_affinity() {
    let mut st = state(vec![account("acct-b", Some(reading(10.0, 10.0)))]);
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    assert_eq!(
        invalidated(&decide(&request(), &st, NOW)),
        InvalidationReason::IdentityWithdrawn
    );

    // `--replace` rebound acct-a's credential id to acct-b.
    let mut st = state(vec![
        account("acct-a", Some(reading(10.0, 10.0))),
        account("acct-b", Some(reading(10.0, 10.0))),
    ]);
    st.roster.as_mut().unwrap().accounts[0].credentials[0].credential_id =
        "oauth:anthropic:new".into();
    st.roster.as_mut().unwrap().accounts[1]
        .credentials
        .push(CredentialRecord {
            credential_id: credential_id("acct-a"),
            default_record: false,
            needs_reauth: false,
        });
    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    assert_eq!(
        invalidated(&decide(&request(), &st, NOW)),
        InvalidationReason::CredentialRebound
    );
}

#[test]
fn roster_outage_blocks_cold_work_but_not_affinity() {
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.roster.as_mut().unwrap().fetched_at_ms = NOW - 24 * HOUR;
    let decision = decide(&request(), &st, NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountSelectionUnavailable);
    assert!(decision.explanation.roster.as_ref().unwrap().expired);

    st.affinity = Some(affinity("acct-a", "claude-sonnet-5", 3));
    assert_eq!(
        decide(&request(), &st, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );

    // Boot with no persisted roster.
    st.roster = None;
    assert_eq!(
        decide(&request(), &st, NOW).admission().unwrap().reason,
        AdmitReason::StickyHit
    );
    st.affinity = None;
    assert_eq!(
        refused(&decide(&request(), &st, NOW)),
        RefusalCode::AccountSelectionUnavailable
    );
}

// ---- step 3: cold exclusions ---------------------------------------------

#[test]
fn cold_work_excludes_each_unusable_account_with_its_reason() {
    let with = |f: &dyn Fn(&mut prefrontal_routing_core::accounts::QuotaReading)| {
        let mut r = reading(10.0, 10.0);
        f(&mut r);
        Some(r)
    };
    let mut reauth = account("acct-reauth", Some(reading(10.0, 10.0)));
    reauth.credentials[0].needs_reauth = true;
    let accounts = vec![
        reauth,
        account(
            "acct-error",
            with(&|r| r.error_class = Some("credential_unusable".into())),
        ),
        account(
            "acct-future",
            with(&|r| r.observed_at_ms = Some(NOW + 2 * MINUTE)),
        ),
        account(
            "acct-old",
            with(&|r| r.observed_at_ms = Some(NOW - 20 * MINUTE)),
        ),
        account("acct-no-7d", with(&|r| r.seven_day = None)),
        account(
            "acct-bad-pct",
            with(&|r| r.seven_day.as_mut().unwrap().used_percent = 101.0),
        ),
        account("acct-no-time", with(&|r| r.observed_at_ms = None)),
        account(
            "acct-stale-odd",
            with(&|r| {
                r.stale = Some(StaleDisclosure {
                    since_ms: Some(NOW - MINUTE),
                    class: Some("auth_failed".into()),
                })
            }),
        ),
        account("acct-unlisted", Some(reading(0.0, 0.0))),
        account("acct-missing", None),
        account(
            "acct-ok",
            with(&|r| {
                r.stale = Some(StaleDisclosure {
                    since_ms: Some(NOW - MINUTE),
                    class: Some("upstream_failed".into()),
                });
                r.observed_at_ms = Some(NOW - 4 * MINUTE);
            }),
        ),
    ];
    let listed: Vec<&str> = [
        "acct-reauth",
        "acct-error",
        "acct-future",
        "acct-old",
        "acct-no-7d",
        "acct-bad-pct",
        "acct-no-time",
        "acct-stale-odd",
        "acct-missing",
        "acct-ok",
    ]
    .to_vec();
    let mut req = request();
    req.candidates = Some(candidates(&listed));
    let decision = decide(&req, &state(accounts), NOW);
    assert_eq!(admitted(&decision), "acct-ok");
    let only = |account: &str| exclusions(&decision, account).to_vec();
    assert_eq!(only("acct-reauth"), vec![ExclusionReason::ReauthRequired]);
    assert_eq!(
        only("acct-error"),
        vec![ExclusionReason::EntryErrorClass {
            error_class: "credential_unusable".into()
        }]
    );
    assert_eq!(
        only("acct-future"),
        vec![ExclusionReason::ClockInvalid {
            observed_at_ms: NOW + 2 * MINUTE
        }]
    );
    assert_eq!(
        only("acct-old"),
        vec![ExclusionReason::AgeExcluded {
            age_ms: 20 * MINUTE
        }]
    );
    assert_eq!(
        only("acct-no-7d"),
        vec![ExclusionReason::GeneralWindowMissing {
            window: GeneralWindow::SevenDay
        }]
    );
    assert_eq!(
        only("acct-bad-pct"),
        vec![ExclusionReason::InvalidPercent {
            window: ApplicableWindow::SevenDay
        }]
    );
    assert_eq!(
        only("acct-no-time"),
        vec![ExclusionReason::TimestampMissing]
    );
    assert_eq!(
        only("acct-stale-odd"),
        vec![ExclusionReason::StaleClassUnknown {
            class: "auth_failed".into()
        }]
    );
    assert_eq!(
        only("acct-unlisted"),
        vec![ExclusionReason::NotInRunnerCandidates]
    );
    assert_eq!(only("acct-missing"), vec![ExclusionReason::QuotaMissing]);
}

#[test]
fn family_scope_excludes_only_its_family_and_absent_scope_passes() {
    let fable_exhausted = || {
        let mut r = reading(10.0, 10.0);
        r.scoped = ScopedList::Listed(vec![fable_window(100.0)]);
        r
    };
    let mut healthy = reading(10.0, 10.0);
    healthy.scoped = ScopedList::Listed(vec![fable_window(90.0)]);
    let st = state(vec![
        account("acct-a", Some(fable_exhausted())),
        account("acct-b", Some(healthy)),
    ]);
    let decision = decide(&with_model(request(), "claude-fable-5-1[1m]"), &st, NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        exclusions(&decision, "acct-a"),
        [ExclusionReason::ScopeExhausted {
            family: ModelFamily::Fable
        }]
    );
    // A present, valid scoped window joins the weight.
    let note = decision.account_note("acct-b").unwrap();
    assert!(note
        .window_weights
        .iter()
        .any(|w| w.window == ApplicableWindow::FamilyScope && w.spendable == 10.0));

    // Fable exhaustion never excludes Mythos or Opus.
    let only_a = state(vec![account("acct-a", Some(fable_exhausted()))]);
    for model in ["claude-mythos-5-1", "claude-opus-5-5"] {
        assert_eq!(
            admitted(&decide(&with_model(request(), model), &only_a, NOW)),
            "acct-a"
        );
    }

    // Missing and explicitly empty scoped lists pass, and stay distinct.
    for (scoped, expected) in [
        (ScopedList::Missing, ScopedListState::Missing),
        (
            ScopedList::Listed(Vec::new()),
            ScopedListState::ExplicitEmpty,
        ),
    ] {
        let mut r = reading(10.0, 10.0);
        r.scoped = scoped;
        let decision = decide(
            &with_model(request(), "claude-fable-5"),
            &state(vec![account("acct-a", Some(r))]),
            NOW,
        );
        assert_eq!(admitted(&decision), "acct-a");
        assert_eq!(
            decision.account_note("acct-a").unwrap().scoped_list,
            Some(expected)
        );
    }
}

#[test]
fn unmapped_scope_id_makes_only_fable_unknown() {
    let mut r = reading(10.0, 10.0);
    r.scoped = ScopedList::Listed(vec![ScopedWindow {
        scope_id: "claude-fable-weekly-v2".into(),
        window_minutes: 10_080,
        used_percent: 5.0,
        resets_at: ResetTime::Missing,
    }]);
    let st = state(vec![account("acct-a", Some(r))]);
    let decision = decide(&with_model(request(), "claude-fable-5"), &st, NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountQuotaUnknown);
    assert_eq!(
        exclusions(&decision, "acct-a"),
        [ExclusionReason::ScopeUnmapped {
            family: ModelFamily::Fable,
            unmapped_scope_ids: vec!["claude-fable-weekly-v2".into()],
        }]
    );
    assert_eq!(
        admitted(&decide(&with_model(request(), "claude-opus-5-5"), &st, NOW)),
        "acct-a"
    );
}

// ---- steps 4 and 5: reserve and weight -----------------------------------

#[test]
fn reserve_excludes_zero_weight_and_broca_needs_five_points() {
    let st = state(vec![
        account("acct-a", Some(reading(10.0, 99.5))),
        account("acct-b", Some(reading(10.0, 60.0))),
    ]);
    let decision = decide(&request(), &st, NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        exclusions(&decision, "acct-a"),
        [ExclusionReason::BelowReserve {
            windows: vec![ApplicableWindow::SevenDay]
        }]
    );

    let st = state(vec![account("acct-a", Some(reading(10.0, 95.0)))]);
    assert_eq!(admitted(&decide(&request(), &st, NOW)), "acct-a");
    let mut broca = request();
    broca.caller = CallerKind::Broca;
    let decision = decide(&broca, &st, NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountPoolReserveLimited);
    assert_eq!(
        exclusions(&decision, "acct-a"),
        [ExclusionReason::BelowBrocaFloor {
            window: GeneralWindow::SevenDay,
            spendable: 4.0
        }]
    );
}

#[test]
fn weight_is_the_minimum_window_rate_with_clamp_and_window_horizons() {
    let mut r = reading(40.0, 40.0);
    r.five_hour.as_mut().unwrap().resets_at = ResetTime::At(NOW + 30_000);
    r.seven_day.as_mut().unwrap().resets_at = ResetTime::Missing;
    let decision = decide(&request(), &state(vec![account("acct-a", Some(r))]), NOW);
    let note = decision.account_note("acct-a").unwrap();
    let five = &note.window_weights[0];
    assert_eq!(five.horizon_hours, 1.0 / 60.0);
    assert!((five.weight - 59.0 * 60.0).abs() < 1e-9);
    let seven = &note.window_weights[1];
    assert_eq!(seven.horizon_hours, 168.0);
    assert!((note.weight.unwrap() - 59.0 / 168.0).abs() < 1e-12);

    // A passed reset spreads the old spendable over one full window.
    let mut r = reading(40.0, 10.0);
    r.five_hour.as_mut().unwrap().resets_at = ResetTime::At(NOW - MINUTE);
    let decision = decide(&request(), &state(vec![account("acct-a", Some(r))]), NOW);
    let five = &decision.account_note("acct-a").unwrap().window_weights[0];
    assert!(five.reset_passed);
    assert_eq!(five.horizon_hours, 5.0);
    assert!((five.weight - 59.0 / 5.0).abs() < 1e-12);
}

#[test]
fn age_bands_full_discounted_and_excluded() {
    let at = |age: i64| {
        let mut r = reading(10.0, 10.0);
        r.observed_at_ms = Some(NOW - age);
        decide(&request(), &state(vec![account("acct-a", Some(r))]), NOW)
    };
    let note = |d: &Decision| {
        (
            d.account_note("acct-a").unwrap().age_band,
            d.account_note("acct-a").unwrap().age_factor,
        )
    };
    assert_eq!(note(&at(5 * MINUTE - 1)), (Some(AgeBand::Full), Some(1.0)));
    assert_eq!(
        note(&at(5 * MINUTE)),
        (Some(AgeBand::Discounted), Some(1.0))
    );
    assert_eq!(
        note(&at(12 * MINUTE + 30_000)),
        (Some(AgeBand::Discounted), Some(0.625))
    );
    let excluded = at(20 * MINUTE);
    assert_eq!(note(&excluded).0, Some(AgeBand::Excluded));
    assert_eq!(refused(&excluded), RefusalCode::AccountQuotaUnknown);
    // The factor scales the weight.
    let full = at(0).account_note("acct-a").unwrap().weight.unwrap();
    let discounted = at(12 * MINUTE + 30_000)
        .account_note("acct-a")
        .unwrap()
        .weight
        .unwrap();
    assert!((discounted - full * 0.625).abs() < 1e-12);
}

// ---- step 6: score and demand --------------------------------------------

#[test]
fn pending_demand_counts_from_the_observation_point_minus_reflection_lag() {
    let mut st = state(vec![
        account("acct-a", Some(reading(10.0, 10.0))),
        account("acct-b", Some(reading(10.0, 10.0))),
    ]);
    // Equal accounts: the tie goes to account id order.
    assert_eq!(admitted(&decide(&request(), &st, NOW)), "acct-a");
    // A reservation newer than the reading minus 60 s pushes the next to b.
    st.admissions.push(demand(
        "acct-a",
        AdmissionState::Reserved,
        NOW - 30_000,
        1000,
    ));
    let decision = decide(&request(), &st, NOW);
    assert_eq!(admitted(&decision), "acct-b");
    assert_eq!(
        decision.account_note("acct-a").unwrap().pending_bytes,
        Some(1000)
    );
    // Older than the reflection lag: assumed reflected in the reading.
    st.admissions[0].committed_at_ms = NOW - 61_000;
    assert_eq!(
        decide(&request(), &st, NOW)
            .account_note("acct-a")
            .unwrap()
            .pending_bytes,
        Some(0)
    );
    // Released never counts; expired, started and finished do.
    st.admissions[0].committed_at_ms = NOW - 30_000;
    for (state_, counts) in [
        (AdmissionState::Released, false),
        (AdmissionState::Expired, true),
        (AdmissionState::Started, true),
        (AdmissionState::Finished, true),
    ] {
        st.admissions[0].state = state_;
        let pending = decide(&request(), &st, NOW)
            .account_note("acct-a")
            .unwrap()
            .pending_bytes;
        assert_eq!(pending, Some(if counts { 1000 } else { 0 }), "{state_:?}");
    }
}

#[test]
fn request_bytes_use_the_32_kib_estimate_or_at_least_one_byte() {
    let st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    let mut req = request();
    req.initial_input_estimated = true;
    let admission = decide(&req, &st, NOW).admission().unwrap().clone();
    assert_eq!(
        (admission.request_bytes, admission.request_bytes_estimated),
        (32 * 1024, true)
    );
    let mut req = request();
    req.initial_input_bytes = 0;
    assert_eq!(decide(&req, &st, NOW).admission().unwrap().request_bytes, 1);
}

#[test]
fn ties_go_to_configured_order_then_account_id() {
    let mut st = state(vec![
        account("acct-a", Some(reading(10.0, 10.0))),
        account("acct-b", Some(reading(10.0, 10.0))),
    ]);
    st.policy.account_order = vec!["acct-b".into()];
    assert_eq!(admitted(&decide(&request(), &st, NOW)), "acct-b");
}

#[test]
fn no_candidate_reply_follows_step_six_precedence() {
    let reauth = |id: &str, quota| {
        let mut view = account(id, quota);
        view.credentials[0].needs_reauth = true;
        view
    };
    let fenced_state = |accounts| {
        let mut st = state(accounts);
        st.fences.push(fence(
            "acct-c",
            FenceScope::General(GeneralWindow::SevenDay),
            ResetTime::Missing,
        ));
        st
    };
    // Unknown, reserve-limited, exhausted and re-auth: quota unknown wins,
    // retrying after a jittered 60 s.
    let decision = decide(
        &request(),
        &fenced_state(vec![
            account("acct-a", None),
            account("acct-b", Some(reading(10.0, 99.5))),
            account("acct-c", Some(reading(10.0, 10.0))),
            reauth("acct-d", Some(reading(10.0, 10.0))),
        ]),
        NOW,
    );
    assert_eq!(refused(&decision), RefusalCode::AccountQuotaUnknown);
    assert_eq!(decision.refusal().unwrap().retry_after_s, Some(66));

    // Reserve-limited, exhausted and re-auth: reserve limited, retrying at the
    // earliest reset (5h, two hours away) plus 60 s.
    let decision = decide(
        &request(),
        &fenced_state(vec![
            account("acct-b", Some(reading(10.0, 99.5))),
            account("acct-c", Some(reading(10.0, 10.0))),
            reauth("acct-d", Some(reading(10.0, 10.0))),
        ]),
        NOW,
    );
    assert_eq!(refused(&decision), RefusalCode::AccountPoolReserveLimited);
    assert_eq!(decision.refusal().unwrap().retry_after_s, Some(7200 + 60));

    // Exhausted and re-auth with no known reset: exhausted after 300 s.
    let decision = decide(
        &request(),
        &fenced_state(vec![account("acct-c", None), reauth("acct-d", None)]),
        NOW,
    );
    assert_eq!(refused(&decision), RefusalCode::AccountPoolExhausted);
    assert_eq!(decision.refusal().unwrap().retry_after_s, Some(300));

    // Every remaining account needs re-auth.
    let decision = decide(&request(), &state(vec![reauth("acct-d", None)]), NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountReauthRequired);

    // Every candidate is refused for this principal.
    let mut st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    st.principal_exclusions.push(PrincipalExclusion {
        principal: request().principal,
        credential_id: credential_id("acct-a"),
        runner_view_digest: "runner-view-1".into(),
        roster_view_digest: "roster-1".into(),
        recorded_at_ms: NOW,
    });
    assert_eq!(
        refused(&decide(&request(), &st, NOW)),
        RefusalCode::AccountNotAuthorized
    );

    // Every route blocked on the Fable scope: the retry uses the scoped
    // reset (96 h) and ignores the much earlier general 5h reset (2 h).
    let mut blocked = reading(10.0, 10.0);
    blocked.scoped = ScopedList::Listed(vec![fable_window(100.0)]);
    let st = state(vec![
        account("acct-a", Some(blocked.clone())),
        account("acct-b", Some(blocked)),
    ]);
    let decision = decide(&with_model(request(), "claude-fable-5"), &st, NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountPoolReserveLimited);
    assert_eq!(
        decision.refusal().unwrap().retry_after_s,
        Some(96 * 3600 + 60)
    );

    // Every account at or above 99% gives reserve-limited, never a pick.
    let st = state(vec![
        account("acct-a", Some(reading(99.0, 10.0))),
        account("acct-b", Some(reading(10.0, 99.0))),
    ]);
    assert_eq!(
        refused(&decide(&request(), &st, NOW)),
        RefusalCode::AccountPoolReserveLimited
    );
}

/// Operator ruling: model reach emptying the pool is
/// `account_selection_unsupported` (not retryable); runner-candidate narrowing
/// emptying it is `account_selection_unavailable` (retryable) and names both
/// sides; `account_pool_exhausted` needs every account that survived reach and
/// candidacy to be confirmed exhausted.
#[test]
fn reach_and_candidacy_empty_pools_use_the_ruled_codes() {
    let opus_only = |id| {
        let mut view = account(id, Some(reading(10.0, 10.0)));
        view.model_reach = ModelReach::Only(vec!["claude-opus-5-5".into()]);
        view
    };
    let decision = decide(
        &request(),
        &state(vec![opus_only("acct-a"), opus_only("acct-b")]),
        NOW,
    );
    assert_eq!(refused(&decision), RefusalCode::AccountSelectionUnsupported);
    assert!(!decision.refusal().unwrap().retryable);

    let mut req = request();
    req.candidates = Some(candidates(&["acct-z"]));
    let st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    let decision = decide(&req, &st, NOW);
    assert_eq!(refused(&decision), RefusalCode::AccountSelectionUnavailable);
    assert!(decision.refusal().unwrap().retryable);
    assert_eq!(
        decision.explanation.roster.as_ref().unwrap().account_ids,
        vec!["acct-a"]
    );
    assert_eq!(
        decision.explanation.runner_candidates.as_ref().unwrap()[0].account_id,
        "acct-z"
    );

    let mut req = request();
    req.candidates = Some(candidates(&["acct-a", "acct-c"]));
    let mut st = state(vec![
        opus_only("acct-a"),
        account("acct-b", Some(reading(10.0, 10.0))),
        account("acct-c", Some(reading(10.0, 10.0))),
    ]);
    st.fences.push(fence(
        "acct-c",
        FenceScope::General(GeneralWindow::SevenDay),
        ResetTime::Missing,
    ));
    assert_eq!(
        refused(&decide(&req, &st, NOW)),
        RefusalCode::AccountPoolExhausted
    );
}

// ---- step 7: direct Opus and Fable preservation --------------------------

fn fable_spent(five: f64, seven: f64) -> prefrontal_routing_core::accounts::QuotaReading {
    let mut r = reading(five, seven);
    r.scoped = ScopedList::Listed(vec![fable_window(100.0)]);
    r
}

#[test]
fn direct_opus_prefers_fresh_fable_exhausted_accounts_within_factor_two() {
    let mut st = state(vec![
        account("acct-a", Some(fable_spent(10.0, 10.0))),
        account("acct-b", Some(reading(10.0, 10.0))),
        account("acct-c", Some(reading(10.0, 10.0))),
        account("acct-d", Some(reading(10.0, 10.0))),
        account("acct-e", Some(reading(10.0, 10.0))),
    ]);
    let req = with_model(request(), "claude-opus-5-5");
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for _ in 0..20 {
        let decision = decide(&req, &st, NOW);
        let id = admitted(&decision).to_string();
        st.admissions
            .push(demand(&id, AdmissionState::Reserved, NOW, 1000));
        *counts.entry(id).or_default() += 1;
    }
    // More than its one-fifth weighted share, but work spills to every sibling.
    assert!(counts["acct-a"] > 4, "{counts:?}");
    assert_eq!(counts.len(), 5, "{counts:?}");
    let first = decide(
        &req,
        &state(vec![account("acct-a", Some(fable_spent(10.0, 10.0)))]),
        NOW,
    );
    assert!(first.account_note("acct-a").unwrap().fable_preserved);
}

#[test]
fn fable_preservation_never_fails_healthy_siblings_or_applies_to_recovery() {
    // Zero general weight on the Fable-exhausted account: a sibling, not a failure.
    let st = state(vec![
        account("acct-a", Some(fable_spent(10.0, 100.0))),
        account("acct-b", Some(reading(10.0, 10.0))),
    ]);
    assert_eq!(
        admitted(&decide(&with_model(request(), "claude-opus-5-5"), &st, NOW)),
        "acct-b"
    );

    // Equal accounts: preservation picks the Fable-exhausted one for direct
    // Opus only, not for work reached through safety recovery or in the
    // discounted band.
    let st = state(vec![
        account("acct-b", Some(reading(10.0, 10.0))),
        account("acct-z", Some(fable_spent(10.0, 10.0))),
    ]);
    assert_eq!(
        admitted(&decide(&with_model(request(), "claude-opus-5-5"), &st, NOW)),
        "acct-z"
    );
    let mut recovery = with_model(request(), "claude-fable-5");
    recovery.model = "claude-opus-5-5".into();
    recovery.via_safety_recovery = true;
    assert_eq!(admitted(&decide(&recovery, &st, NOW)), "acct-b");
    let mut discounted = st.clone();
    for view in &mut discounted.roster.as_mut().unwrap().accounts {
        view.quota.as_mut().unwrap().observed_at_ms = Some(NOW - 6 * MINUTE);
    }
    assert_eq!(
        admitted(&decide(
            &with_model(request(), "claude-opus-5-5"),
            &discounted,
            NOW
        )),
        "acct-b"
    );
}

// ---- providers and bindings ----------------------------------------------

#[test]
fn chatgpt_needs_only_its_weekly_window() {
    let mut view = account("acct-g", Some(reading(10.0, 10.0)));
    view.refresh_adapter = "openai".into();
    view.quota.as_mut().unwrap().five_hour = None;
    let mut req = request();
    req.provider = "openai".into();
    req.model = "gpt-6".into();
    req.affinity_model = "gpt-6".into();
    let st = state(vec![view, account("acct-claude", Some(reading(0.0, 0.0)))]);
    let decision = decide(&req, &st, NOW);
    assert_eq!(admitted(&decision), "acct-g");
    // Claude accounts are a different pool.
    assert!(decision.account_note("acct-claude").is_none());
}

#[test]
fn record_binding_prefers_the_default_record_then_smallest_id() {
    let mut view = account("acct-a", Some(reading(10.0, 10.0)));
    view.credentials = vec![
        CredentialRecord {
            credential_id: "oauth:anthropic:zed".into(),
            default_record: false,
            needs_reauth: false,
        },
        CredentialRecord {
            credential_id: "oauth:anthropic:abe".into(),
            default_record: false,
            needs_reauth: false,
        },
        CredentialRecord {
            credential_id: "oauth:anthropic".into(),
            default_record: true,
            needs_reauth: false,
        },
    ];
    let st = state(vec![view.clone()]);
    assert_eq!(
        decide(&request(), &st, NOW)
            .admission()
            .unwrap()
            .credential_id,
        "oauth:anthropic"
    );
    view.credentials.pop();
    let st = state(vec![view]);
    assert_eq!(
        decide(&request(), &st, NOW)
            .admission()
            .unwrap()
            .credential_id,
        "oauth:anthropic:abe"
    );
    // The record the runner names wins.
    let mut req = request();
    req.candidates = Some(prefrontal_routing_core::accounts::RunnerCandidates {
        view_digest: "runner-view-1".into(),
        accounts: vec![prefrontal_routing_core::accounts::CandidateRef {
            account_id: "acct-a".into(),
            credential_id: "oauth:anthropic:zed".into(),
        }],
    });
    assert_eq!(
        decide(&req, &st, NOW).admission().unwrap().credential_id,
        "oauth:anthropic:zed"
    );
}

#[test]
fn every_decision_is_machine_readable() {
    let st = state(vec![account("acct-a", Some(reading(10.0, 10.0)))]);
    let decision = decide(&request(), &st, NOW);
    let value = serde_json::to_value(&decision).unwrap();
    assert_eq!(value["outcome"]["kind"], "admit");
    assert_eq!(value["outcome"]["reason"], "cold_scored");
    assert_eq!(value["explanation"]["policy_version"], 2);
    assert_eq!(value["explanation"]["accounts"][0]["account_id"], "acct-a");
    let back: Decision = serde_json::from_value(value).unwrap();
    assert_eq!(back, decision);
}
