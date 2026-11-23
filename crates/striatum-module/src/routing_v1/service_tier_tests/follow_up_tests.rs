use super::*;

use super::super::conformance_tests::Route;

async fn finished_fast_turn() -> (Arc<Harness>, Route, Value, Value) {
    let (harness, mut credentials) = legacy_broca_setup();
    let mut rows = credentials.listed_credentials().to_vec();
    for (index, row) in rows.iter_mut().enumerate() {
        row.account_id = Some(format!("00000000-0000-4000-8000-{index:012}"));
    }
    assert!(credentials.apply_listing(
        &ScopedCredentialListing {
            view: "uuid-accounts".into(),
            credentials: rows.clone(),
            ..Default::default()
        },
        NOW,
    ));
    harness.set_credentials(credentials);

    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select_sol("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision_id = selected.1["result"]["decision_id"].as_str().unwrap();
    assert_eq!(
        harness.store().decision_service_tier(decision_id).unwrap(),
        Some(("fast".into(), vec!["chatgpt:0".into(), "chatgpt:1".into()]))
    );

    let runner = harness.bind(reserved("broca"), None).await;
    let mut request = legacy_broca_request("fast-turn-1");
    request.as_object_mut().unwrap().remove("caller");
    request["decision_id"] = json!(decision_id);
    request["service_tier"] = json!("fast");
    request["hold_wait_max_s"] = json!(0);
    request["candidates"]["view_digest"] = json!("uuid-accounts");
    let offered: Vec<_> = rows
        .iter()
        .map(|row| json!({"account_id": row.account_id, "credential_id": row.id}))
        .collect();
    request["candidates"]["accounts"] = json!(offered);
    let first = runner.call("route.account_for", &request).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(first.1["result"]["service_tier_chosen"], "fast");

    for kind in ["admission_started", "run_finished"] {
        let observed = runner
            .call(
                "route.account_observe",
                &json!({"event_id":kind,"kind":kind,"admission_id":"fast-turn-1","run_id":"fast-run-1"}),
            )
            .await;
        assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
    }
    let first_admission = harness
        .store()
        .account_read(|tx| tx.admission("reserved:broca", "fast-turn-1"))
        .unwrap()
        .unwrap();
    assert_eq!(first_admission.state, "finished");

    request["admission_id"] = json!("fast-turn-2");
    (harness, runner, request, first.1["result"].clone())
}

#[tokio::test]
async fn bound_fast_follow_up_moves_to_sibling_when_pinned_pair_is_not_offered() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, mut request, first) = finished_fast_turn().await;
    let _second_turn_clock = crate::InjectedClock::set(NOW + 121);
    let pinned = &first["account"];
    let offered = request["candidates"]["accounts"]
        .as_array()
        .unwrap()
        .clone();
    let mut stale_offer = offered.clone();
    let pinned_offer = stale_offer
        .iter_mut()
        .find(|row| row["credential_id"] == pinned["credential_id"])
        .unwrap();
    assert_eq!(pinned_offer["account_id"], pinned["account_id"]);
    // The credential name is still offered, but not with the pinned account ID.
    pinned_offer["account_id"] = json!("00000000-0000-4000-8000-999999999999");
    request["candidates"]["accounts"] = json!(stale_offer);
    crate::take_operator_infos();
    let second = runner.call("route.account_for", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["service_tier_chosen"], "fast");
    let sibling = offered
        .iter()
        .find(|row| row["credential_id"] != pinned["credential_id"])
        .unwrap();
    for field in ["account_id", "credential_id"] {
        assert_eq!(second.1["result"]["account"][field], sibling[field]);
    }
    assert_eq!(second.1["result"]["pin"]["generation"], 2);
    let admitted = harness
        .store()
        .account_read(|tx| tx.admission("reserved:broca", "fast-turn-2"))
        .unwrap()
        .unwrap();
    let decision = harness
        .store()
        .account_decision(&admitted.decision_id)
        .unwrap()
        .unwrap();
    let recorded: Value = serde_json::from_str(&decision.decision_json).unwrap();
    assert_eq!(recorded["explanation"]["affinity"]["kind"], "invalidated");
    assert_eq!(
        recorded["explanation"]["affinity"]["reason"]["reason"],
        "not_in_runner_candidates"
    );
    let infos = crate::take_operator_infos();
    let logged: Value = serde_json::from_str(
        infos
            .iter()
            .find_map(|line| line.strip_prefix("routing moved a bound-tier pin: "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        logged,
        json!({"admission_id":"fast-turn-2", "credential_id":pinned["credential_id"],
        "reason":"not_in_runner_candidates", "new_credential_id":sibling["credential_id"]})
    );
}

#[tokio::test]
async fn bound_fast_follow_up_keeps_the_eligible_pin() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, runner, request, first) = finished_fast_turn().await;
    crate::take_operator_infos();
    let second = runner.call("route.account_for", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["service_tier_chosen"], "fast");
    assert_eq!(second.1["result"]["account"], first["account"]);
    assert_eq!(second.1["result"]["pin"], first["pin"]);
    assert_eq!(second.1["result"]["reason"]["code"], "sticky_hit");
    assert!(crate::take_operator_infos()
        .iter()
        .all(|line| !line.starts_with("routing moved a bound-tier pin: ")));
}

fn block(harness: &Harness, account: &Value, reset_at_ms: u64) {
    use striatum_core::accounts::{
        ConfirmationSource, Fence, FenceScope, GeneralWindow, ResetTime,
    };
    let account = account.as_str().unwrap();
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let fence = Fence {
        account_id: account.into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(reset_at_ms as i64),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&striatum_store::AccountFenceRow {
                fence_id: format!("fast-fence-{account}"),
                provider: "openai".into(),
                account_id: account.into(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at: NOW as i64,
                principal: "reserved:broca".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW as i64,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
}

#[tokio::test]
async fn bound_fast_follow_up_waits_for_the_earliest_allowed_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, request, first) = finished_fast_turn().await;
    let pinned = &first["account"]["account_id"];
    let sibling = request["candidates"]["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|account| account["account_id"] != *pinned)
        .unwrap();
    block(&harness, pinned, NOW + 120_000);
    block(&harness, &sibling["account_id"], NOW + 60_000);
    crate::take_operator_warnings();
    let wait = runner.call("route.account_for", &request).await;
    assert_eq!(wait.0, FrameType::Error, "{}", wait.1);
    assert_eq!(wait.1["code"], "account_pool_exhausted");
    assert_eq!(wait.1["detail"]["reset_at_ms"], NOW + 60_000);
    let warnings = crate::take_operator_warnings();
    let line = warnings
        .iter()
        .find_map(|line| line.strip_prefix("routing refused an account call: "))
        .unwrap();
    let logged: Value = serde_json::from_str(line).unwrap();
    assert_eq!(
        logged["pinned_ineligibility"]["credential_id"],
        first["account"]["credential_id"]
    );
    assert!(!logged["pinned_ineligibility"]["reason"].is_null());
    assert!(!line.contains(pinned.as_str().unwrap()));
}

#[tokio::test]
async fn bound_fast_follow_up_keeps_the_ordinary_short_hold() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, mut request, first) = finished_fast_turn().await;
    block(&harness, &first["account"]["account_id"], NOW + 10_000);
    request["hold_wait_max_s"] = json!(300);
    let bound = runner.call("route.account_for", &request).await;
    assert_eq!(bound.0, FrameType::Error, "{}", bound.1);
    assert_eq!(bound.1["code"], "account_hold");
    let refusal: cortexkit_role_routing::errors::Refusal =
        serde_json::from_value(bound.1.clone()).unwrap();
    assert!(refusal.wait_until_ms().is_some_and(|until| until > NOW));

    request.as_object_mut().unwrap().remove("decision_id");
    request.as_object_mut().unwrap().remove("service_tier");
    let ordinary = runner.call("route.account_for", &request).await;
    assert_eq!(ordinary, bound);
}

#[tokio::test]
async fn bound_fast_follow_up_does_not_move_outside_the_allowance() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, request, first) = finished_fast_turn().await;
    harness
        .store()
        .record_service_tier(
            request["decision_id"].as_str().unwrap(),
            "fast",
            &[first["account"]["credential_id"].as_str().unwrap().into()],
        )
        .unwrap();
    block(&harness, &first["account"]["account_id"], NOW + 60_000);

    let wait = runner.call("route.account_for", &request).await;
    assert_eq!(wait.0, FrameType::Error, "{}", wait.1);
    assert_eq!(wait.1["code"], "account_pool_exhausted");
    assert_eq!(wait.1["detail"]["reset_at_ms"], NOW + 60_000);
}
