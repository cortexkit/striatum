use super::*;

#[tokio::test]
async fn bound_fast_follow_up_requires_the_exact_candidate_pair_after_finish() {
    let _clock = crate::InjectedClock::set(NOW);
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
    let pinned = &first.1["result"]["account"];
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

    let _second_turn_clock = crate::InjectedClock::set(NOW + 121);
    request["admission_id"] = json!("fast-turn-2");
    let mut stale_offer = offered.clone();
    let pinned_offer = stale_offer
        .iter_mut()
        .find(|row| row["credential_id"] == pinned["credential_id"])
        .unwrap();
    assert_eq!(pinned_offer["account_id"], pinned["account_id"]);
    // Credential IDs are unchanged, so the refusal log cannot reveal which
    // account ID the runner paired with the pinned credential.
    pinned_offer["account_id"] = json!("00000000-0000-4000-8000-999999999999");
    request["candidates"]["accounts"] = json!(stale_offer);
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_unservable");
    assert_eq!(
        refused.1["detail"],
        json!({"reason":"bound_tier_unavailable"})
    );
    assert!(harness
        .store()
        .v1_admission("reserved:broca", "fast-turn-2")
        .unwrap()
        .is_none());

    // Only the candidate pair changes. The same conversation, decision and
    // admission ID must still work at fast with the original pin.
    request["candidates"]["accounts"] = json!(offered);
    let second = runner.call("route.account_for", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["service_tier_chosen"], "fast");
    assert_eq!(second.1["result"]["account"], *pinned);
    for field in ["id", "generation"] {
        assert_eq!(
            second.1["result"]["pin"][field],
            first.1["result"]["pin"][field]
        );
    }
}
