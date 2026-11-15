use super::*;

const RECHECK: u64 = RECOVERY_NOW + 900_000;

#[tokio::test]
async fn pin_quota_accounts_retry_by_recheck_and_keep_near_reset() {
    for (reset, expected) in [(ACCOUNT_RESET, ACCOUNT_RESET), (DISTANT_WALL, RECHECK)] {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (_, route) = configured().await;
        exhaust_recovery_accounts(Some(reset));
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "pin":ACCOUNT_ROUTE,
                    "requirements":{"iq":50}, "hold_wait_max_s":0
                }),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "quota_exhausted", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.code.as_str(), "pin_unavailable");
        assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
    }
}

#[tokio::test]
async fn pin_quota_wall_retry_by_recheck_and_keep_near_reset() {
    for (reset, expected) in [(ACCOUNT_RESET, ACCOUNT_RESET), (DISTANT_WALL, RECHECK)] {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (harness, route) = configured().await;
        harness
            .store()
            .update_route_state(
                WALL_ROUTE,
                WALL_ROUTE,
                &striatum_core::RouteStateUpdate {
                    quota_exhausted_until: Some(reset as f64),
                    observed_at_ms: Some(RECOVERY_NOW as i64 + 1),
                    wall_class: Some(striatum_core::RouteWallClass::Quota),
                    ..Default::default()
                },
            )
            .unwrap();
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "pin":WALL_ROUTE, "requirements":{"iq":50}
                }),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "quota_exhausted", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
    }
}

#[tokio::test]
async fn no_route_quota_accounts_retry_by_recheck_and_keep_near_reset() {
    for (reset, expected) in [(ACCOUNT_RESET, ACCOUNT_RESET), (DISTANT_WALL, RECHECK)] {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (_, route) = configured().await;
        exhaust_recovery_accounts(Some(reset));
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "available_routes":[ACCOUNT_ROUTE],
                    "requirements":{"iq":50}, "hold_wait_max_s":0
                }),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "accounts_blocked", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
    }
}

fn scorer_error(reason: RouteFailureReason, cause: &str, reset: Option<u64>) -> NoRouteError {
    NoRouteError {
        candidates: Vec::new(),
        reason,
        message: "no usable route".into(),
        rejected: vec![striatum_core::model_router::RejectedRoute {
            route_key: WALL_ROUTE.into(),
            reason: cause.into(),
            detail: None,
            credential_ids: Vec::new(),
        }],
        requirements: None,
        best_rejected: None,
        earliest_reset: reset.map(|at| at as f64),
        earliest_reset_route: Some(WALL_ROUTE.into()),
        adequate_blockers: [(WALL_ROUTE.into(), reset.map(|at| at as f64))].into(),
    }
}

fn assert_scorer_quota_retry(reason: RouteFailureReason, cause: &str) {
    for (reset, expected) in [(ACCOUNT_RESET, ACCOUNT_RESET), (DISTANT_WALL, RECHECK)] {
        let error = scorer_error(reason, cause, Some(reset));
        let refusal = no_route_refusal(&error, RECOVERY_NOW as f64);
        assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
        assert_eq!(
            refusal.detail["blockingCauses"][0]["adequateRoutes"][0]["resetAtMs"],
            json!(reset as f64)
        );
        assert!(refusal.message.contains(&format!("until_ms={reset}")));
    }
    let untimed = no_route_refusal(&scorer_error(reason, cause, None), RECOVERY_NOW as f64);
    assert_eq!(untimed.wait_until_ms(), None);
}

#[test]
fn no_route_quota_wall_retry_by_recheck_and_keep_near_reset() {
    assert_scorer_quota_retry(
        RouteFailureReason::QuotaOrCooldownExhausted,
        "quota_exhausted",
    );
}

#[test]
fn no_route_subscription_reserve_retry_by_recheck_and_keep_near_reset() {
    assert_scorer_quota_retry(
        RouteFailureReason::QuotaOrCooldownExhausted,
        "subscription_reserve",
    );
}

#[test]
fn all_inadequate_quota_wall_retry_by_recheck_and_keep_near_reset() {
    for reason in [
        RouteFailureReason::NoAdequateRoute,
        RouteFailureReason::AllRoutesInadequate,
    ] {
        assert_scorer_quota_retry(reason, "quota_exhausted");
    }
}

#[test]
fn non_quota_and_unrelated_quota_recoveries_keep_their_deadlines() {
    for cause in ["cooldown_active", "backoff_active"] {
        let error = scorer_error(
            RouteFailureReason::QuotaOrCooldownExhausted,
            cause,
            Some(DISTANT_WALL),
        );
        assert_eq!(
            no_route_refusal(&error, RECOVERY_NOW as f64).wait_until_ms(),
            Some(DISTANT_WALL)
        );
    }
    let mut error = scorer_error(
        RouteFailureReason::AllRoutesInadequate,
        "quota_exhausted",
        Some(DISTANT_WALL),
    );
    error.earliest_reset_route = Some("unrelated/model".into());
    assert_eq!(
        no_route_refusal(&error, RECOVERY_NOW as f64).wait_until_ms(),
        Some(DISTANT_WALL)
    );
    error.reason = RouteFailureReason::ConcurrencyFull;
    error.earliest_reset_route = Some(WALL_ROUTE.into());
    assert_eq!(
        no_route_refusal(&error, RECOVERY_NOW as f64).wait_until_ms(),
        Some(DISTANT_WALL)
    );
}

#[test]
fn key_pool_quota_retry_by_recheck_and_keep_near_reset() {
    for pin in [Some("pool/model"), None] {
        for (reset, expected) in [
            (Some(ACCOUNT_RESET), Some(ACCOUNT_RESET)),
            (Some(DISTANT_WALL), Some(RECHECK)),
            (None, None),
        ] {
            let refusal = pool_select_refusal(
                key_pool::PoolRefusal {
                    code: striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED.into(),
                    message: "pool exhausted".into(),
                    detail: Some(json!({"earliestResetAtMs":reset})),
                },
                pin,
                RECOVERY_NOW,
            );
            assert_eq!(refusal.wait_until_ms(), expected, "{refusal:?}");
        }
    }
}
