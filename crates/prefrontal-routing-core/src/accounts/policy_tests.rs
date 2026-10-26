use super::*;

const NOW: i64 = 1_800_000_000_000;

fn request() -> AccountRequest {
    serde_json::from_value(serde_json::json!({
        "principal":"runner", "provider":"test-provider", "model":"model", "affinity_model":"model",
        "conversation_key":"abc", "conversation_key_source":"caller", "caller":"plugin",
        "via_safety_recovery":false, "initial_input_bytes":4096, "initial_input_estimated":false,
        "candidates":null, "requested_account":null, "preferred_existing_account":null, "hold_wait_max_s":900
    })).unwrap()
}

fn view(id: &str) -> AccountView {
    AccountView {
        account_id: id.into(),
        routing_provider: "test-provider".into(),
        refresh_adapter: "adapter".into(),
        credentials: vec![CredentialRecord {
            credential_id: format!("credential-{id}"),
            default_record: true,
            needs_reauth: false,
        }],
        model_reach: ModelReach::All,
        quota: None,
    }
}

fn state(ids: &[&str]) -> AllocatorState {
    AllocatorState {
        roster: Some(Roster {
            view_digest: "vault".into(),
            fetched_at_ms: NOW,
            accounts: ids.iter().map(|id| view(id)).collect(),
        }),
        affinity: None,
        fences: Vec::new(),
        principal_exclusions: Vec::new(),
        admissions: Vec::new(),
        operator_override: None,
        policy: AllocatorPolicy::default(),
    }
}

fn fence(id: &str, reset: ResetTime) -> Fence {
    Fence {
        account_id: id.into(),
        pool: None,
        recovery_set: vec![],
        observed_at_ms: NOW,
        reset,
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    }
}

fn affinity() -> ConversationAffinity {
    ConversationAffinity {
        pin_id: "pin".into(),
        account_id: "a".into(),
        credential_id: "credential-a".into(),
        affinity_model: "model".into(),
        pin_generation: 1,
        expires_at_ms: NOW + 60_000,
        lease_active: true,
    }
}

fn chosen(decision: Decision) -> String {
    match decision.outcome {
        Outcome::Admit(admission) => admission.account_id,
        other => panic!("{other:?}"),
    }
}

#[test]
fn hold_controls_apply_in_order_to_any_blocking_window() {
    let mut state = state(&["a", "b"]);
    state.affinity = Some(affinity());
    let mut request = request();
    for pool in [
        None,
        Some(PoolKey::general(GeneralWindow::FiveHour)),
        Some(PoolKey::general(GeneralWindow::SevenDay)),
    ] {
        let mut block = fence("a", ResetTime::At(NOW + 600_001));
        block.pool = pool;
        state.fences = vec![block];
        let decision = decide(&request, &state, NOW);
        let Outcome::Hold(hold) = decision.outcome else {
            panic!("{decision:?}");
        };
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad.
        // This published digest's first four bytes are 3128432319; mod 21 is 12.
        assert_eq!(hold.base_retry_after_s, 601);
        assert_eq!(hold.retry_after_s, 613);
        let no_wait = AllocationContext {
            hold_ok: Some(false),
            ..Default::default()
        };
        assert_eq!(
            chosen(decide_with_context(&request, &state, NOW, &no_wait)),
            "b"
        );
        state.policy.max_hold_s = 612;
        assert_eq!(chosen(decide(&request, &state, NOW)), "b");
        state.policy.max_hold_s = 900;
        request.hold_wait_max_s = 612;
        assert_eq!(chosen(decide(&request, &state, NOW)), "b");
        request.hold_wait_max_s = 900;
    }
    for reset in [ResetTime::Missing, ResetTime::At(NOW + 3 * 60 * 60_000)] {
        state.fences = vec![fence("a", reset)];
        assert_eq!(chosen(decide(&request, &state, NOW)), "b");
    }
}

#[test]
fn cold_model_holds_only_when_every_account_can_hold() {
    let mut state = state(&["a", "b"]);
    state.fences = vec![
        fence("a", ResetTime::At(NOW + 600_000)),
        fence("b", ResetTime::At(NOW + 700_000)),
    ];
    assert!(matches!(
        decide(&request(), &state, NOW).outcome,
        Outcome::Hold(_)
    ));
    state.fences[1].reset = ResetTime::Missing;
    assert!(matches!(
        decide(&request(), &state, NOW).outcome,
        Outcome::Refuse(_)
    ));
}

#[test]
fn children_avoid_only_a_known_parent_when_an_alternative_exists() {
    let inputs = AllocationContext {
        parent_account_id: Some("a".into()),
        ..Default::default()
    };
    assert_eq!(
        chosen(decide_with_context(
            &request(),
            &state(&["a", "b"]),
            NOW,
            &inputs
        )),
        "b"
    );
    assert_eq!(
        chosen(decide_with_context(
            &request(),
            &state(&["a"]),
            NOW,
            &inputs
        )),
        "a"
    );
    assert_eq!(chosen(decide(&request(), &state(&["a", "b"]), NOW)), "a");
}

#[test]
fn strategies_order_within_the_supplied_account_class() {
    let mut state = state(&["a", "b"]);
    state.policy.strategy = AccountStrategy::Ordered;
    state.policy.account_order = vec!["a".into(), "b".into()];
    assert_eq!(chosen(decide(&request(), &state, NOW)), "a");
    state.policy.fallback_first = true;
    assert_eq!(chosen(decide(&request(), &state, NOW)), "b");
    state.policy.strategy = AccountStrategy::RoundRobin;
    state.admissions.push(AdmissionDemand {
        account_id: "a".into(),
        state: AdmissionState::Reserved,
        request_bytes: 4096,
        committed_at_ms: NOW,
    });
    assert_eq!(chosen(decide(&request(), &state, NOW)), "b");
    state.affinity = Some(affinity());
    state.policy.strategy = AccountStrategy::Hybrid;
    assert_eq!(chosen(decide(&request(), &state, NOW)), "a");
    state.policy.strategy = AccountStrategy::Sticky;
    assert_eq!(chosen(decide(&request(), &state, NOW)), "a");
}

#[test]
fn credits_require_all_exhausted_and_a_reading_under_fifteen_minutes() {
    let mut state = state(&["a", "b"]);
    state.policy.paid_credits = true;
    let mut inputs = AllocationContext::default();
    inputs.credits.insert(
        "a".into(),
        CreditReading {
            observed_at_ms: NOW,
            spendable: true,
        },
    );
    let decision = decide_with_context(&request(), &state, NOW, &inputs);
    assert!(matches!(
        decision.outcome,
        Outcome::Admit(Admission {
            reason: AdmitReason::ColdScored,
            ..
        })
    ));
    state.fences = vec![fence("a", ResetTime::At(NOW + 600_000))];
    assert_eq!(
        chosen(decide_with_context(&request(), &state, NOW, &inputs)),
        "b"
    );
    state.fences.push(fence("b", ResetTime::At(NOW + 600_000)));
    assert!(matches!(
        decide_with_context(&request(), &state, NOW, &inputs).outcome,
        Outcome::Admit(Admission {
            reason: AdmitReason::PaidCredits,
            ..
        })
    ));
    inputs.credits.get_mut("a").unwrap().observed_at_ms = NOW - 15 * 60_000;
    assert!(!matches!(
        decide_with_context(&request(), &state, NOW, &inputs).outcome,
        Outcome::Admit(_)
    ));
}

#[test]
fn spend_budget_has_its_own_reset() {
    let mut state = state(&["a"]);
    state.policy.spend_budget = Some(10.0);
    let mut inputs = AllocationContext::default();
    inputs.spend.insert(
        "a".into(),
        SpendReading {
            used: 10.0,
            reset_at_ms: Some(NOW + 60_000),
        },
    );
    assert!(matches!(
        decide_with_context(&request(), &state, NOW, &inputs).outcome,
        Outcome::Hold(_)
    ));
    assert_eq!(
        chosen(decide_with_context(
            &request(),
            &state,
            NOW + 60_001,
            &inputs
        )),
        "a"
    );
}

fn reading(used: f64, pool: &str) -> QuotaReading {
    let window = QuotaWindow {
        used_percent: used,
        raw_utilization: None,
        source: ObservationSource::Poll,
        resets_at: ResetTime::At(NOW + 600_000),
        window_minutes: 300,
        window_kind: None,
    };
    QuotaReading {
        observed_at_ms: Some(NOW),
        error_class: None,
        stale: None,
        five_hour: Some(window.clone()),
        seven_day: None,
        general_windows: Some(vec![window]),
        scoped: ScopedList::Missing,
        pool_windows: Some(vec![PoolWindowReading {
            pool: Some(pool.into()),
            window_minutes: 300,
            used_percent: used,
        }]),
    }
}

#[test]
fn soft_quota_exempts_one_account_and_floors_match_the_pool_key() {
    let mut state = state(&["a", "b"]);
    state.roster.as_mut().unwrap().accounts[0].quota = Some(reading(90.0, "p"));
    state.roster.as_mut().unwrap().accounts[1].quota = Some(reading(10.0, "p"));
    state.policy.soft_quota_remaining_percent = Some(20.0);
    assert_eq!(chosen(decide(&request(), &state, NOW)), "b");
    state.roster.as_mut().unwrap().accounts.pop();
    assert_eq!(chosen(decide(&request(), &state, NOW)), "a");
    state.policy.pool_floors = vec![PoolFloor {
        key: PoolKey {
            name: Some("q".into()),
            window_minutes: Some(300),
            scope: PoolScope::EveryModel,
        },
        remaining_percent: 20.0,
    }];
    assert_eq!(chosen(decide(&request(), &state, NOW)), "a");
    state.policy.pool_floors[0].key.name = Some("p".into());
    assert!(matches!(
        decide(&request(), &state, NOW).outcome,
        Outcome::Refuse(_)
    ));
    state.affinity = Some(affinity());
    assert!(
        matches!(decide(&request(), &state, NOW).outcome, Outcome::Refuse(_)),
        "an existing pin cannot bypass an explicit floor"
    );
}
