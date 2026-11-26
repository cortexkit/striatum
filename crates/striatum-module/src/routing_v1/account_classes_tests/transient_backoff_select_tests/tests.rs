use super::*;
use crate::routing_v1::conformance_tests::Route;

async fn blocked(retry_at: u64) -> (Arc<Harness>, Route, Route) {
    let harness = one_account();
    let runner = harness.bind(reserved("broca"), None).await;
    let owner = harness.bind(reserved("prefrontal"), None).await;
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
    json!({"consumer":"prefrontal.sidekick.map", "requirements":{"iq":45,"eq":0}})
}

async fn concurrent_admissions(
    runner: &Route,
    first: &str,
    second: &str,
) -> ((FrameType, Value), (FrameType, Value)) {
    let first = admission(first);
    let second = admission(second);
    tokio::join!(
        runner.call("route.account_for", &first),
        runner.call("route.account_for", &second)
    )
}

async fn claim(owner: &Route, runner: &Route, admitted: bool) {
    let selected = owner
        .call(
            "route.select",
            &json!({
                "consumer":"prefrontal.mason", "pin":format!("openai/{}", MODELS[0]),
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
async fn account_for_reopens_transient_route_backoff_to_concurrent_requests() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    // Set up the route-scoped effect by itself: an account-wide observation
    // would exercise the account-wide origin instead of this route origin.
    harness
        .store()
        .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "openai".into(),
            model: MODELS[0].into(),
            credential_id: CREDENTIAL.into(),
            until_ms: NOW as i64 + 5_000,
            decision_id: String::new(),
            recorded_at: NOW as i64,
        })
        .unwrap();
    let runner = harness.bind(reserved("broca"), None).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    let (first, second) =
        concurrent_admissions(&runner, "route-reopen-one", "route-reopen-two").await;
    assert_credential(&first, CREDENTIAL);
    assert_credential(&second, CREDENTIAL);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "transient_route"));
}

#[tokio::test]
async fn account_for_reopens_explicit_route_rate_limit_to_concurrent_requests() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    let route = format!("openai/{}", MODELS[0]);
    harness
        .store()
        .update_route_state(
            &route,
            &route,
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 5_000) as f64),
                observed_at_ms: Some(NOW as i64),
                unscoped_wall: true,
                wall_class: Some(striatum_core::RouteWallClass::Transient),
                ..Default::default()
            },
        )
        .unwrap();
    let runner = harness.bind(reserved("broca"), None).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    let (first, second) = concurrent_admissions(
        &runner,
        "explicit-route-reopen-one",
        "explicit-route-reopen-two",
    )
    .await;
    assert_credential(&first, CREDENTIAL);
    assert_credential(&second, CREDENTIAL);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "route"));
}

#[tokio::test]
async fn account_for_reopens_explicit_provider_rate_limit_to_concurrent_requests() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    let route = format!("openai/{}", MODELS[0]);
    harness
        .store()
        .record_provider_cooldown(
            "openai",
            &route,
            &route,
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 5_000) as f64),
                observed_at_ms: Some(NOW as i64),
                unscoped_wall: true,
                wall_class: Some(striatum_core::RouteWallClass::Transient),
                ..Default::default()
            },
        )
        .unwrap();
    let runner = harness.bind(reserved("broca"), None).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    let (first, second) = concurrent_admissions(
        &runner,
        "explicit-provider-reopen-one",
        "explicit-provider-reopen-two",
    )
    .await;
    assert_credential(&first, CREDENTIAL);
    assert_credential(&second, CREDENTIAL);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| !matches!(origin.kind.as_str(), "route" | "provider")));
}

#[tokio::test]
async fn account_for_refusal_while_shared_route_recovery_test_is_running() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    let route = format!("openai/{}", MODELS[0]);
    harness
        .store()
        .update_route_state(
            &route,
            &route,
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 5_000) as f64),
                observed_at_ms: Some(NOW as i64),
                unscoped_wall: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_account_wait_for_running_test(&harness).await;
}

async fn assert_account_wait_for_running_test(harness: &Arc<Harness>) {
    let runner = harness.bind(reserved("broca"), None).await;
    let claimed_at = NOW + 5_001;
    let _due = crate::InjectedClock::set(claimed_at);
    assert_credential(
        &runner
            .call("route.account_for", &admission("account-probe"))
            .await,
        CREDENTIAL,
    );
    let started = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"account-probe-start", "kind":"admission_started",
                "admission_id":"account-probe", "run_id":"account-probe-run"}),
        )
        .await;
    assert_eq!(started.0, FrameType::Response, "{}", started.1);

    let refused_at = claimed_at + 10_000;
    let _during = crate::InjectedClock::set(refused_at);
    let before = harness.store().fence_rechecks().unwrap();
    crate::take_operator_warnings();
    let refused = runner
        .call("route.account_for", &admission("waiting-account"))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
    let fields = crate::take_operator_warnings()
        .iter()
        .filter_map(|line| line.strip_prefix("routing refused an account call: "))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|fields| fields["admission_id"] == "waiting-account")
        .expect("account refusal log");
    assert_eq!(
        (
            &refused.1["detail"]["reset_at_ms"],
            &refused.1["detail"]["retry_after_s"],
            &fields["reason"],
        ),
        (
            &json!(refused_at + 30_000),
            &json!(30),
            &json!("recovery_test_in_progress")
        ),
        "refusal: {}; operator warning: {fields}",
        refused.1,
    );
    assert_eq!(harness.store().fence_rechecks().unwrap(), before);
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
async fn expired_observation_backoff_admits_concurrent_requests_and_selects() {
    let _clock = crate::InjectedClock::set(NOW);
    let retry_at = NOW + 5_000;
    let (harness, owner, runner) = blocked(retry_at).await;

    let _before = crate::InjectedClock::set(retry_at - 1);
    let refused = runner
        .call("route.account_for", &admission("before-retry"))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["detail"]["reset_at_ms"], retry_at);

    let _due = crate::InjectedClock::set(retry_at + 1);
    let (first, second) = concurrent_admissions(&runner, "reopen-one", "reopen-two").await;
    assert_credential(&first, CREDENTIAL);
    assert_credential(&second, CREDENTIAL);

    let first_request = select();
    let second_request = select();
    let (first, second) = tokio::join!(
        owner.call("route.select", &first_request),
        owner.call("route.select", &second_request)
    );
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "observation"));
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let (active, retired): (i64, i64) = conn
        .query_row(
            "SELECT (SELECT count(*) FROM block WHERE kind='observation' AND source_present=1),
                    (SELECT count(*) FROM retirement_tombstone WHERE kind='observation')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((active, retired), (0, 1));
}

#[tokio::test]
async fn expiry_retirement_rejects_older_backoff_replay_and_accepts_newer_evidence() {
    let _clock = crate::InjectedClock::set(NOW);
    let retry_at = NOW + 5_000;
    let (harness, _, runner) = blocked(retry_at).await;
    let _due = crate::InjectedClock::set(retry_at + 1);
    assert_credential(
        &runner
            .call("route.account_for", &admission("retire-at-deadline"))
            .await,
        CREDENTIAL,
    );
    let retired: i64 = rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .query_row(
            "SELECT count(*) FROM retirement_tombstone WHERE kind='observation'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retired, 1, "expiry must retire, not delete, the evidence");

    let _older = crate::InjectedClock::set(NOW - 1);
    let replayed = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"replayed-older-backoff", "kind":"transient_backoff",
                "admission_id":"older-run", "served_account_id":ACCOUNT,
                "retry_at_ms":retry_at - 1_000}),
        )
        .await;
    assert_eq!(replayed.0, FrameType::Response, "{}", replayed.1);
    let _current = crate::InjectedClock::set(retry_at + 2);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "observation"));

    let newer_at = retry_at + 3;
    let newer_retry = retry_at + 10_000;
    let _newer = crate::InjectedClock::set(newer_at);
    let newer = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"newer-backoff", "kind":"transient_backoff",
                "admission_id":"older-run", "served_account_id":ACCOUNT,
                "retry_at_ms":newer_retry}),
        )
        .await;
    assert_eq!(newer.0, FrameType::Response, "{}", newer.1);
    let origins = harness.store().fence_rechecks().unwrap();
    let origin = origins
        .iter()
        .find(|origin| origin.kind == "observation")
        .expect("newer evidence creates a fresh block");
    assert_eq!(origin.evidence_at, newer_at as i64);
    assert_eq!(origin.next_recheck_at, newer_retry as i64);
    let _before_newer = crate::InjectedClock::set(newer_at + 1);
    let refused = runner
        .call("route.account_for", &admission("newer-still-blocked"))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["detail"]["reset_at_ms"], newer_retry);
}

#[tokio::test]
async fn expired_pool_rate_limit_admits_concurrent_requests() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    harness
        .store()
        .pool_wall(
            "openai",
            CREDENTIAL,
            striatum_store::PoolWallKind::RateLimited,
            NOW as i64 + 5_000,
            "provider 429",
            NOW as i64,
        )
        .unwrap();
    let runner = harness.bind(reserved("broca"), None).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    let (first, second) =
        concurrent_admissions(&runner, "pool-reopen-one", "pool-reopen-two").await;
    assert_credential(&first, CREDENTIAL);
    assert_credential(&second, CREDENTIAL);
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "pool_rate"));
}

#[tokio::test]
async fn quota_wall_still_allows_only_one_recovery_request() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "openai".into(),
            model: MODELS[0].into(),
            credential_id: CREDENTIAL.into(),
            until_ms: NOW as i64 + 5_000,
            decision_id: String::new(),
            recorded_at: NOW as i64,
        })
        .unwrap();
    let runner = harness.bind(reserved("broca"), None).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    let (first, second) =
        concurrent_admissions(&runner, "quota-probe-one", "quota-probe-two").await;
    assert_eq!(
        [first.0, second.0]
            .into_iter()
            .filter(|ty| *ty == FrameType::Response)
            .count(),
        1,
        "first: {}; second: {}",
        first.1,
        second.1
    );
    assert_eq!(
        [first.0, second.0]
            .into_iter()
            .filter(|ty| *ty == FrameType::Error)
            .count(),
        1,
        "first: {}; second: {}",
        first.1,
        second.1
    );
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .any(|origin| origin.kind == "account_route" && origin.last_claimed_at.is_some()));
}

#[tokio::test]
async fn expired_backoff_reopens_select_after_the_old_test_window() {
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
    publish_fixture_providers(&mut credentials, "broca", &["openai"], true);
    drop(credentials);
    let reply = owner.call("route.select", &select()).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert!(
        crate::account_state::published(harness.store(), (clock - 10_000) as i64)
            .unwrap()
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| origin.kind != "observation")
    );
}

#[tokio::test]
async fn expired_backoff_reopens_pinned_and_panel_selection() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    claim(&owner, &runner, false).await;
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq","extra":0,"floor":0});
    });
    let pinned = owner
        .call(
            "route.select",
            &json!({
                "consumer":"prefrontal.sidekick.map", "pin":format!("openai/{}", MODELS[0]),
                "requirements":{"iq":45,"eq":0}
            }),
        )
        .await;
    assert_eq!(pinned.0, FrameType::Response, "{}", pinned.1);
    let reply = owner
        .call(
            "route.select_panel",
            &json!({
                "class":"audit", "consumer":"prefrontal.sidekick.map", "seed":"fixed",
                "available_routes":MODELS.map(|model| format!("openai/{model}"))
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let panel: cortexkit_role_routing::panel::SelectPanelAnswer =
        serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert_eq!(panel.seats.len(), 1);
    assert!(panel.shortfall.is_none());
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
    publish_fixture_providers(
        &mut credentials,
        "broca",
        &["openai", "quota-provider"],
        true,
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
async fn reopened_backoff_leaves_quota_only_retry_unchanged() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    claim(&owner, &runner, false).await;
    quota_route(&harness);
    let _later = crate::InjectedClock::set(NOW + 6_000);
    let allowed = owner.call("route.select", &select()).await;
    assert_eq!(allowed.0, FrameType::Response, "{}", allowed.1);
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

#[tokio::test]
async fn reopened_backoff_is_not_hidden_by_a_quota_alternative() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, owner, runner) = blocked(NOW + 5_000).await;
    let _due = crate::InjectedClock::set(NOW + 5_001);
    claim(&owner, &runner, false).await;
    let mut credentials = harness.credentials().lock().unwrap().clone();
    let mut rows = credentials.listed_credentials().to_vec();
    rows.push(ListedCredential {
        id: "oauth:quota-sibling".into(),
        account_id: Some("quota-sibling".into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["openai".into()],
        serves: vec!["openai".into()],
        credential_type: "oauth".into(),
        auth_method: Some("oauth".into()),
        ..Default::default()
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-openai-accounts".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW + 5_001,
    );
    credentials.refresh_serving_methods(
        &Default::default(),
        &harness.store().get_facts().unwrap(),
        NOW as i64 + 5_001,
        &[],
    );
    harness.set_credentials(credentials);
    let scope = FenceScope::General(GeneralWindow::SevenDay);
    let fence = Fence {
        account_id: "quota-sibling".into(),
        pool: Some(scope.clone().into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(NOW as i64 + 60_000),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: "quota-sibling".into(),
                provider: "openai".into(),
                account_id: fence.account_id.clone(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at: NOW as i64,
                principal: "fixture".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW as i64,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
    let _later = crate::InjectedClock::set(NOW + 6_000);
    let automatic = owner.call("route.select", &select()).await;
    assert_eq!(automatic.0, FrameType::Response, "{}", automatic.1);
    let mut request = select();
    request["pin"] = json!(format!("openai/{}", MODELS[0]));
    request["hold_wait_max_s"] = json!(0);
    let pinned = owner.call("route.select", &request).await;
    assert_eq!(pinned.0, FrameType::Response, "{}", pinned.1);
}
