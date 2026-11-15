use super::*;

#[tokio::test]
async fn stable_refusal_bound_tier_loss_is_unservable() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup(&["pro_200"], true, true);
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut rows = credentials.listed_credentials().to_vec();
    rows[0].plan_tier = Some("unknown".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("lost-tier", None);
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_unservable", "{}", refused.1);
    assert_eq!(
        refused.1["detail"],
        json!({"reason":"bound_tier_unavailable"})
    );
}

#[tokio::test]
async fn stable_refusal_unknown_stored_tier_is_unservable() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_200"], true, true);
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"].as_str().unwrap();
    harness
        .store()
        .record_service_tier(decision, "future-tier", &[])
        .unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("invalid-stored-tier", None);
    request["decision_id"] = json!(decision);
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_unservable", "{}", refused.1);
    assert_eq!(
        refused.1["detail"],
        json!({"reason":"stored_state_invalid"})
    );
}
