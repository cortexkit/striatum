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

async fn probe_blocked() -> (Arc<Harness>, Route, Route) {
    let harness = one_probe_account();
    for model in MODELS[..1].iter().copied() {
        harness
            .store()
            .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "openai".into(),
                model: model.into(),
                credential_id: CREDENTIAL.into(),
                until_ms: NOW as i64 + 5_000,
                decision_id: String::new(),
                recorded_at: NOW as i64,
            })
            .unwrap();
    }
    let runner = harness.bind(reserved("broca"), None).await;
    let owner = harness.bind(reserved("prefrontal"), None).await;
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

#[tokio::test]
async fn account_for_refusal_while_route_recovery_test_is_running() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _, _) = probe_blocked().await;
    assert_account_wait_for_running_test(&harness).await;
}

#[tokio::test]
async fn legacy_record_cooldown_keeps_exactly_one_recovery_request() {
    for provider_wide in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = one_account();
        let runner = harness.bind(reserved("broca"), None).await;
        let route = format!("openai/{}", MODELS[0]);
        for (event, wall_class) in [("first", Some("transient")), ("second", None)] {
            let mut params = json!({
                "routeKey":route, "capacityKey":route, "providerWide":provider_wide,
                "cooldownUntil":NOW + 5_000, "eventId":event,
            });
            if !provider_wide {
                params["credentialId"] = json!(CREDENTIAL);
            }
            if let Some(wall_class) = wall_class {
                params["wallClass"] = json!(wall_class);
            }
            let reply = runner.call("route.record_cooldown", &params).await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        }
        let _due = crate::InjectedClock::set(NOW + 5_001);
        let (first, second) =
            concurrent_admissions(&runner, "legacy-probe-one", "legacy-probe-two").await;
        assert_eq!(
            [first.0, second.0]
                .into_iter()
                .filter(|ty| *ty == FrameType::Response)
                .count(),
            1,
            "provider_wide={provider_wide}: first={} second={}",
            first.1,
            second.1
        );
        assert_eq!(
            [first.0, second.0]
                .into_iter()
                .filter(|ty| *ty == FrameType::Error)
                .count(),
            1,
            "provider_wide={provider_wide}: first={} second={}",
            first.1,
            second.1
        );
    }
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
                provider_retry: true,
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
                provider_retry: true,
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

    let older_retry = retry_at + 60_000;
    let _older = crate::InjectedClock::set(NOW - 1);
    let replayed = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"replayed-older-backoff", "kind":"transient_backoff",
                "admission_id":"older-run", "served_account_id":ACCOUNT,
                "retry_at_ms":older_retry}),
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
    let active: i64 = rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .query_row(
            "SELECT count(*) FROM block WHERE kind='observation' AND source_present=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        active, 0,
        "an older receipt stays retired even with a future retry time"
    );

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
