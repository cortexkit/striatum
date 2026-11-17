use super::*;
use crate::routing_v1::conformance_tests::Route;

async fn blocked(retry_at: u64) -> (Arc<Harness>, Route, Route) {
    let harness = one_account();
    let runner = harness.bind(reserved("broca"), None).await;
    let owner = harness.bind(reserved("prefrontal-core"), None).await;
    assert_credential(
        &runner
            .call("route.account_for", &admission("older-run"))
            .await,
        CREDENTIAL,
    );
    let observed = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"backoff", "kind":"transient_backoff", "admission_id":"older-run",
                "served_account_id":ACCOUNT, "retry_at_ms":retry_at
            }),
        )
        .await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
    (harness, owner, runner)
}

fn select() -> Value {
    json!({"consumer":"prefrontal-core.sidekick.map", "requirements":{"iq":45,"eq":0}})
}

async fn claim(owner: &Route, runner: &Route, admitted: bool) {
    let selected = owner
        .call(
            "route.select",
            &json!({
                "consumer":"prefrontal-core.mason", "pin":format!("openai/{}", MODELS[0]),
                "requirements":{"iq":45,"eq":0}
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    if admitted {
        let mut request = admission("probe-run");
        request["decision_id"] = selected.1["result"]["decision_id"].clone();
        assert_credential(
            &runner.call("route.account_for", &request).await,
            CREDENTIAL,
        );
        let started = runner.call("route.account_observe", &json!({
            "event_id":"probe-start", "kind":"admission_started", "admission_id":"probe-run",
            "run_id":"streaming-probe"
        })).await;
        assert_eq!(started.0, FrameType::Response, "{}", started.1);
    }
}

#[tokio::test]
async fn unclaimed_non_quota_backoff_keeps_its_real_deadline() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, _) = blocked(NOW + 60_000).await;
    let _later = crate::InjectedClock::set(NOW + 5_000);
    let reply = owner.call("route.select", &select()).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1["detail"]["reset_at_ms"],
        NOW + 60_000,
        "{}",
        reply.1
    );
    assert!(!reply.1["message"]
        .as_str()
        .unwrap()
        .contains("being tested"));
    let origin = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|origin| origin.kind == "observation")
        .unwrap();
    assert_eq!(origin.last_claimed_at, None);
}

#[tokio::test]
async fn in_flight_test_with_less_than_thirty_seconds_remaining_uses_its_clock() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let claimed_at = NOW + 5_001;
    let _due = crate::InjectedClock::set(claimed_at);
    claim(&owner, &runner, true).await;
    let clock = claimed_at + 3_600_000;
    let _near = crate::InjectedClock::set(clock - 10_000);
    let credentials = harness.credentials();
    let mut credentials = credentials.lock().unwrap();
    let rows = credentials.listed_credentials().to_vec();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "one-account".into(),
            credentials: rows,
            ..Default::default()
        },
        clock - 10_000,
    );
    drop(credentials);
    let reply = owner.call("route.select", &select()).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["detail"]["reset_at_ms"], clock, "{}", reply.1);
    assert!(reply.1["message"]
        .as_str()
        .unwrap()
        .contains("being tested"));
}

#[tokio::test]
async fn claimed_non_quota_test_gives_panel_seats_a_short_check_time() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    claim(&owner, &runner, false).await;
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"topK":1,"by":"iq","extra":0,"floor":0});
    });
    let pinned = owner
        .call(
            "route.select",
            &json!({
                "consumer":"prefrontal-core.sidekick.map", "pin":format!("openai/{}", MODELS[0]),
                "requirements":{"iq":45,"eq":0}
            }),
        )
        .await;
    assert_eq!(pinned.1["code"], "pin_unavailable", "{}", pinned.1);
    assert_eq!(pinned.1["detail"]["reason"], "backoff", "{}", pinned.1);
    assert_eq!(
        pinned.1["detail"]["reset_at_ms"],
        NOW + 35_001,
        "{}",
        pinned.1
    );
    assert!(pinned.1["message"]
        .as_str()
        .unwrap()
        .contains("being tested"));
    let reply = owner
        .call(
            "route.select_panel",
            &json!({
                "class":"audit", "consumer":"prefrontal-core.sidekick.map", "seed":"fixed",
                "available_routes":MODELS.map(|model| format!("openai/{model}"))
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let panel: cortexkit_role_routing::panel::SelectPanelAnswer =
        serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert!(panel.seats.is_empty());
    let detail = panel.shortfall.unwrap().typed_detail().unwrap().unwrap();
    assert!(!detail.causes.is_empty());
    for cause in detail.causes {
        assert_eq!(cause.reset_at_ms, Some(NOW + 35_001), "{cause:?}");
        assert_eq!(cause.reason, "quota_exhausted");
        assert!(cause
            .detail
            .unwrap()
            .as_str()
            .unwrap()
            .contains("being tested"));
    }
}

fn quota_route(harness: &Harness) {
    let mut credentials = harness.credentials().lock().unwrap().clone();
    let mut fact = crate::tests::fact("quota-provider", "model");
    fact.auth_methods = Some(vec!["oauth".into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "quota-provider".into(),
        auth_method: "oauth".into(),
        kind: "subscription".into(),
        selection_reason: "independent quota account fixture".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
            .into_iter()
            .collect(),
    });
    harness.install_facts(std::slice::from_ref(&fact));
    catalog_fact(harness, &fact);
    let mut rows = credentials.listed_credentials().to_vec();
    rows.push(ListedCredential {
        id: "oauth:quota-only".into(),
        account_id: Some("quota-account".into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["quota-provider".into()],
        serves: vec!["quota-provider".into()],
        credential_type: "oauth".into(),
        auth_method: Some("oauth".into()),
        ..Default::default()
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "one-account-and-quota".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW + 5_001,
    );
    let facts = harness.store().get_facts().unwrap();
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64 + 5_001, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    harness.set_credentials(credentials);
    harness
        .store()
        .update_route_state(
            "quota-provider/model",
            "quota-provider/model",
            &striatum_core::RouteStateUpdate {
                quota_exhausted_until: Some((NOW + 86_400_000) as f64),
                observed_at_ms: Some(NOW as i64),
                wall_class: Some(striatum_core::RouteWallClass::Quota),
                ..Default::default()
            },
        )
        .unwrap();
}

#[tokio::test]
async fn in_flight_test_shortens_mixed_quota_retry_only_on_a_usable_route() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    claim(&owner, &runner, false).await;
    quota_route(&harness);
    let _later = crate::InjectedClock::set(NOW + 6_000);
    let allowed = owner.call("route.select", &select()).await;
    assert_eq!(allowed.0, FrameType::Error, "{}", allowed.1);
    assert_eq!(
        allowed.1["detail"]["reset_at_ms"],
        NOW + 36_000,
        "{}",
        allowed.1
    );
    assert!(allowed.1["message"]
        .as_str()
        .unwrap()
        .contains("being tested"));
    let mut excluded = select();
    excluded["exclude"] = json!(MODELS.map(|model| format!("openai/{model}")));
    let reply = owner.call("route.select", &excluded).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1["detail"]["reset_at_ms"],
        NOW + 6_000 + 900_000,
        "{}",
        reply.1
    );
    assert!(!reply.1["message"]
        .as_str()
        .unwrap()
        .contains("being tested"));
}
