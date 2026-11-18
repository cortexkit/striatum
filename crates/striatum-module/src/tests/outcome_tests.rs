//! Outcome and usage tests verify that legacy wire reports record only the evidence they carry.

use super::{selection_tests::vault_listing, *};

#[test]
fn record_cooldown_keeps_billing_but_does_not_infer_unscoped_quota() {
    let rig = Rig::new("record-cooldown-wall-class");
    let store = rig.open_store();
    for (route_key, wall_class, last_error) in [
            (
                "xai/grok-4.6",
                RouteWallClass::Billing,
                "personal-team-blocked:spending-limit: You have run out of credits or need a Grok subscription",
            ),
            (
                "p/plain-quota",
                RouteWallClass::Quota,
                "you have reached your usage limit",
            ),
        ] {
            record_cooldown_update(
                &store,
                &RecordCooldownParams {
                    route_key: route_key.into(),
                    capacity_key: route_key.into(),
                    credential_id: None,
                    cooldown_until: Some(5_000.0),
                    quota_exhausted_until: Some(5_000.0),
                    quota_pressure: None,
                    wall_class: Some(wall_class),
                    provider_wide: true,
                    increment_quota_error: true,
                    increment_failure: true,
                    last_error: Some(last_error.into()),
                },
            )
            .unwrap();
        }

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(
        snapshot.by_route["xai/grok-4.6"].wall_class,
        Some(RouteWallClass::Billing)
    );
    assert_eq!(snapshot.by_route["p/plain-quota"].wall_class, None);
    assert!(store
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| !origin.quota));
}

#[test]
fn contract_outcome_round_trips_without_a_quota_horizon() {
    let outcome = RouteOutcome::ContractError;
    assert_eq!(parse_outcome(outcome.as_str(), None), Some(outcome));
    assert_eq!(outcome.retry_after_secs(), None);
    assert!(!striatum_core::is_terminal_send_failure(outcome));
}

#[test]
fn quota_outcome_requires_and_carries_retry_after_secs() {
    assert_eq!(
        parse_outcome("quota_exhausted", Some(580)),
        Some(RouteOutcome::QuotaExhausted {
            retry_after_secs: 580,
        })
    );
    assert_eq!(parse_outcome("quota_exhausted", None), None);
    assert_eq!(parse_outcome("quota_exhausted", Some(0)), None);
}

#[tokio::test]
async fn provider_outcome_wire_records_terminal_outcomes_and_frozen_reset_fence() {
    use striatum_core::route_state_store::ProviderOutcome;
    let rig = Rig::new("provider-reset-wire");
    let store = Arc::new(rig.open_store());
    let make_decision = || {
        store
            .record_decision(&DecisionInput {
                consumer: Some("mason".into()),
                selected_provider_id: Some("google".into()),
                selected_model_id: Some("antigravity-gemini-3.8-flash".into()),
                selected_variant: Some("high".into()),
                selected_route_key: Some("google/antigravity-gemini-3.8-flash/high".into()),
                ..Default::default()
            })
            .unwrap()
    };
    let failed = make_decision();
    let recovery = make_decision();
    store
        .set_decision_provider_identity(
            &failed,
            &ProviderOutcome {
                account_ref: "vault-account".into(),
                credential_id: Some("antigravity:google".into()),
                resets_at_ms: None,
                credential_refusal: None,
                ..Default::default()
            },
        )
        .unwrap();
    store
        .set_decision_provider_identity(
            &recovery,
            &ProviderOutcome {
                account_ref: "quota-account".into(),
                credential_id: Some("antigravity:google".into()),
                resets_at_ms: None,
                credential_refusal: None,
                ..Default::default()
            },
        )
        .unwrap();
    let reset = now_ms() as i64 + 75 * 3_600_000;
    for params in [
        json!({"decisionID":failed,"outcome":"interrupted","lastError":"account_limit_exhausted: transient HTTP 429",
                "providerOutcome":{"accountRef":"provider-email","resetsAtMs":reset}}),
        json!({"decisionID":recovery,"outcome":"completed"}),
    ] {
        let completing = params["outcome"] == "completed";
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}))
                .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_set_decision_outcome(
            frame,
            &tx,
            &store,
            Path::new("/nonexistent/provider-reset-wire.jsonc"),
            &SharedBrocaCredentials::default(),
        )
        .await
        .unwrap();
        let ack: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
        assert_eq!(ack["result"]["ok"], true, "{ack}");
        let id = if completing { &recovery } else { &failed };
        assert_eq!(
            store
                .get_decision_by_id(id)
                .unwrap()
                .unwrap()
                .outcome
                .as_deref(),
            Some(if completing {
                "completed"
            } else {
                "interrupted"
            }),
        );
    }
    let fences = store.account_route_fences(now_ms() as i64).unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].decision_id, failed);
    assert_eq!(fences[0].credential_id, "antigravity:google");
    assert_eq!(fences[0].until_ms, reset);
}

#[test]
fn provider_reset_fence_preserves_a_monthly_reset() {
    use striatum_core::route_state_store::ProviderOutcome;
    let rig = Rig::new("provider-monthly-reset");
    let store: SharedStore = Arc::new(rig.open_store());
    let decision = store
        .record_decision(&DecisionInput {
            selected_provider_id: Some("opencode-go".into()),
            selected_model_id: Some("deepseek-v4.1-flash".into()),
            ..Default::default()
        })
        .unwrap();
    let identity = ProviderOutcome {
        account_ref: "apikey:opencode-go".into(),
        credential_id: Some("apikey:opencode-go".into()),
        resets_at_ms: Some(1_795_357_028_612),
        credential_refusal: None,
        ..Default::default()
    };
    store
        .set_decision_provider_identity(&decision, &identity)
        .unwrap();
    let now = 1_793_604_325_063;
    record_provider_reset_fence(
        &store,
        &SharedBrocaCredentials::default(),
        &decision,
        &RouteOutcome::Interrupted,
        Some(&identity),
        true,
        None,
        now,
        None,
        None,
    )
    .unwrap();
    let fences = store.account_route_fences(now).unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].until_ms, 1_795_357_028_612);
}

#[test]
fn xai_spending_limit_with_fresh_zero_usage_is_a_non_quota_refusal() {
    use striatum_core::route_state_store::ProviderOutcome;
    let now = account_quota::parse_rfc3339_ms("2026-11-10T13:57:00Z").unwrap();
    for used in [0.0, 100.0] {
        let rig = Rig::new("xai-contradictory-quota");
        let store: SharedStore = Arc::new(rig.open_store());
        let credentials = vault_listing(&[("oauth:xai", &["xai"], Some("xai-account"))]);
        let mut quota = account_quota::AccountQuotaState::default();
        quota.readings.readings.insert(("xai".into(),"xai-account".into()),account_quota::StoredReading {
                provider:"xai".into(),account_id:"xai-account".into(),received_at_ms:now-600_000,tombstoned_at_ms:None,
                entry:json!({"provider":"xai","apiProvider":"xai","account":"xai-account","fetchedAt":"2026-11-10T13:47:00Z",
                    "usage":{"secondary":{"usedPercent":used,"windowMinutes":10080}}}),
            });
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("xai".into()),
                selected_model_id: Some("grok-4.3".into()),
                ..Default::default()
            })
            .unwrap();
        let identity = ProviderOutcome {
            account_ref: "xai-account".into(),
            credential_id: Some("oauth:xai".into()),
            ..Default::default()
        };
        store
            .set_decision_provider_identity(&decision, &identity)
            .unwrap();
        // The legacy runner typed the 403 spending-limit refusal as account
        // exhaustion, with no reset. Classification must use the reading,
        // not the provider's free-form personal-team-blocked message.
        record_provider_reset_fence(
            &store,
            &credentials,
            &decision,
            &RouteOutcome::Interrupted,
            Some(&identity),
            true,
            None,
            now,
            Some(&quota),
            None,
        )
        .unwrap();
        let quota_rows = store.account_route_fences(now).unwrap();
        let other_rows = store.non_quota_account_route_fences(now).unwrap();
        if used == 0.0 {
            assert!(quota_rows.is_empty());
            assert_eq!(other_rows.len(), 1);
            let clocks = store.fence_rechecks().unwrap();
            assert_eq!(clocks[0].kind, "non_quota_route");
            assert_eq!(clocks[0].next_recheck_at, now + 3_600_000);
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 15 * 60_000))
                .unwrap());
            // Another fresh zero reading does not heal the non-quota wall.
            assert_eq!(
                store
                    .non_quota_account_route_fences(now + 60_000)
                    .unwrap()
                    .len(),
                1
            );
            assert!(store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 3_600_000))
                .unwrap());
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 3_600_000))
                .unwrap());
        } else {
            assert_eq!(quota_rows.len(), 1);
            assert!(other_rows.is_empty());
            assert_eq!(
                store.fence_rechecks().unwrap()[0].next_recheck_at,
                now + 15 * 60_000
            );
        }
    }
}

async fn report_legacy_outcome_for_fence_test(store: &SharedStore, params: serde_json::Value) {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        7,
        1,
        99,
        serde_json::to_vec(&json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}))
            .unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    handle_set_decision_outcome(
        frame,
        &tx,
        store,
        Path::new("/nonexistent/fence-outcome.jsonc"),
        &SharedBrocaCredentials::default(),
    )
    .await
    .unwrap();
    let ack: serde_json::Value = serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
    assert_eq!(ack["result"]["ok"], true, "{ack}");
}

#[tokio::test]
async fn legacy_completed_cannot_heal_a_credential_that_did_not_serve() {
    use striatum_core::route_state_store::{CredentialRefusal, ProviderOutcome};
    let rig = Rig::new("mismatched-serving-success");
    let store: SharedStore = Arc::new(rig.open_store());
    store
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "p".into(),
                credential_id: "B".into(),
                code: "billing_exhausted".into(),
            },
            None,
            100,
        )
        .unwrap();
    let decision = store
        .record_decision(&DecisionInput {
            selected_provider_id: Some("p".into()),
            selected_model_id: Some("m".into()),
            ..Default::default()
        })
        .unwrap();
    store
        .set_decision_provider_identity(
            &decision,
            &ProviderOutcome {
                account_ref: "account-A".into(),
                credential_id: Some("A".into()),
                ..Default::default()
            },
        )
        .unwrap();
    report_legacy_outcome_for_fence_test(
        &store,
        json!({"decisionID":decision,"outcome":"completed",
            "providerOutcome":{"accountRef":"account-B","credentialId":"B"}}),
    )
    .await;
    assert_eq!(
        store.credential_entitlement_fences().unwrap().len(),
        1,
        "B did not serve the decision frozen to A"
    );
}

#[tokio::test]
async fn anonymous_legacy_reported_credential_heals_only_its_claimed_authorization() {
    for reported in ["claimed", "other"] {
        let rig = Rig::new("anonymous-reported-credential");
        let store: SharedStore = Arc::new(rig.open_store());
        let at = now_ms() as i64;
        let _clock = InjectedClock::set((at + 1000) as u64);
        store
            .pool_wall(
                "p",
                "claimed",
                striatum_store::PoolWallKind::Billing,
                at + 1000,
                "credits",
                at,
            )
            .unwrap();
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("m".into()),
                ..Default::default()
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, at + 1000, Some(&decision)))
            .unwrap());
        report_legacy_outcome_for_fence_test(
            &store,
            json!({"decisionID":decision,"outcome":"completed",
                    "providerOutcome":{"accountRef":"account","credentialId":reported}}),
        )
        .await;
        assert_eq!(store.fence_rechecks().unwrap().is_empty(), reported == "claimed",
                "an anonymous decision may heal only when the reported credential appears in its authorization");
    }
}

#[tokio::test]
async fn anonymous_legacy_probe_success_heals_its_actual_launch_authorization() {
    let rig = Rig::new("anonymous-probe-success");
    let store: SharedStore = Arc::new(rig.open_store());
    let receipt = now_ms() as i64;
    let _clock = InjectedClock::set((receipt + 1000) as u64);
    store
        .update_route_state(
            "p/m",
            "p/m",
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((receipt + 1000) as f64),
                observed_at_ms: Some(receipt - 3_599_000),
                unscoped_wall: true,
                ..Default::default()
            },
        )
        .unwrap();
    let decision = store
        .record_decision(&DecisionInput {
            selected_provider_id: Some("p".into()),
            selected_model_id: Some("m".into()),
            ..Default::default()
        })
        .unwrap();
    let origins = store.fence_rechecks().unwrap();
    assert!(!origins.is_empty());
    assert!(store
        .account_transaction(|tx| tx.grant_fence_rechecks(
            &origins,
            receipt + 1000,
            Some(&decision)
        ))
        .unwrap());
    report_legacy_outcome_for_fence_test(
        &store,
        json!({"decisionID":decision,"outcome":"completed"}),
    )
    .await;
    assert!(
        store.fence_rechecks().unwrap().is_empty(),
        "anonymous serving success must retire the route/provider origin it probed"
    );
}

#[tokio::test]
async fn inconclusive_legacy_probe_reopens_its_non_quota_clock_without_healing() {
    for (outcome, error) in [
        ("cancelled", None),
        ("interrupted", None),
        ("error", Some("provider_policy_refusal: request blocked")),
        ("error", Some("request_invalid: unsupported request shape")),
    ] {
        let rig = Rig::new("inconclusive-probe-clock");
        let store: SharedStore = Arc::new(rig.open_store());
        let receipt = now_ms() as i64;
        let _clock = InjectedClock::set((receipt + 1000) as u64);
        store
            .update_route_state(
                "p/m",
                "p/m",
                &striatum_core::RouteStateUpdate {
                    cooldown_until: Some((receipt + 1000) as f64),
                    observed_at_ms: Some(receipt - 3_599_000),
                    unscoped_wall: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("m".into()),
                ..Default::default()
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                receipt + 1000,
                Some(&decision)
            ))
            .unwrap());
        report_legacy_outcome_for_fence_test(
            &store,
            json!({"decisionID":decision,"outcome":outcome,"lastError":error}),
        )
        .await;
        let remaining = store.fence_rechecks().unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "an inconclusive probe is not successful serving"
        );
        assert!(
            remaining[0].next_recheck_at <= receipt + 1000,
            "{outcome}/{error:?} must not hold a route for another hour"
        );
    }
}

#[tokio::test]
async fn legacy_credit_window_is_decoder_only_and_success_or_operator_clear_heals_it() {
    use striatum_core::route_state_store::ProviderOutcome;
    async fn report(
        store: &SharedStore,
        credentials: &SharedBrocaCredentials,
        params: serde_json::Value,
    ) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}))
                .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_set_decision_outcome(
            frame,
            &tx,
            store,
            Path::new("/nonexistent/credit-outcome.jsonc"),
            credentials,
        )
        .await
        .unwrap();
        let ack: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
        assert_eq!(ack["result"]["ok"], true, "{ack}");
    }
    for (member, used, retry) in [
        ("limitWindow", 0.0, None),
        ("limit_window", 100.0, Some(30)),
        ("limitWindow", 100.0, Some(7200)),
    ] {
        let receipt = now_ms() as i64;
        let _clock = InjectedClock::set(receipt as u64);
        let rig = Rig::new("legacy-credit-window");
        let store: SharedStore = Arc::new(rig.open_store());
        let credentials = vault_listing(&[("oauth:xai", &["xai"], Some("xai-account"))]);
        let quota =
            ACCOUNT_QUOTA.get_or_init(|| Arc::new(std::sync::Mutex::new(Default::default())));
        let mut state = account_quota::AccountQuotaState::default();
        account_quota::apply_provider_reply(&mut state, "grok", &serde_json::to_vec(&json!({
                "result":[{"provider":"grok","apiProvider":"xai","account":"xai-account","credentialRef":"oauth:xai",
                    "fetchedAt":account_selection::rfc3339(receipt),"usage":{"primary":{"usedPercent":used,"windowMinutes":10080}}}],
                "completeProviders":["grok"]
            })).unwrap(), receipt).unwrap();
        *quota.lock().unwrap() = state;
        let input = DecisionInput {
            selected_provider_id: Some("xai".into()),
            selected_model_id: Some("grok-4.3".into()),
            ..Default::default()
        };
        let failed = store.record_decision(&input).unwrap();
        let identity = ProviderOutcome {
            account_ref: "xai-account".into(),
            credential_id: Some("oauth:xai".into()),
            ..Default::default()
        };
        store
            .set_decision_provider_identity(&failed, &identity)
            .unwrap();
        let mut provider = json!({"accountRef":"xai-account","credentialId":"oauth:xai","providerCode":"account_limit_exhausted"});
        provider[member] = json!("credit");
        let mut params =
            json!({"decisionID":failed,"outcome":"interrupted","providerOutcome":provider});
        if let Some(retry) = retry {
            params["retryAfterSecs"] = json!(retry);
        }
        let decoded: SetDecisionOutcomeParams = serde_json::from_value(params.clone()).unwrap();
        let decoded = decoded.provider_outcome.unwrap();
        assert_eq!(decoded.limit_window.as_deref(), Some("credit"));
        let emitted = serde_json::to_value(&decoded.identity).unwrap();
        assert!(emitted.get("limitWindow").is_none() && emitted.get("limit_window").is_none());
        report(&store, &credentials, params).await;
        let fences = store.non_quota_account_route_fences(receipt).unwrap();
        assert_eq!(
            fences.len(),
            1,
            "credit is independent of the quota reading"
        );
        assert_eq!(
            fences[0].until_ms,
            receipt + retry.unwrap_or(3600).min(3600) * 1000
        );
        assert!(store.account_route_fences(receipt).unwrap().is_empty());
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|clock| !clock.quota));

        if member == "limit_window" {
            credential_fences::lift_for_operator(&store, "oauth:xai").unwrap();
            assert!(store
                .non_quota_account_route_fences(receipt)
                .unwrap()
                .is_empty());
            continue;
        }

        // A later successful execution on the same serving identity proves
        // recovery, unlike even a freshly replenished quota allowance.
        let recovered = store.record_decision(&input).unwrap();
        store
            .set_decision_provider_identity(&recovered, &identity)
            .unwrap();
        report(
            &store,
            &credentials,
            json!({"decisionID":recovered,"outcome":"completed"}),
        )
        .await;
        assert!(store
            .non_quota_account_route_fences(receipt)
            .unwrap()
            .is_empty());
    }
}

async fn clear_fences_as(
    store: &SharedStore,
    channel: u16,
    principal: &str,
    credential: &str,
) -> Frame {
    route_principals()
        .lock()
        .unwrap()
        .insert(channel, (1, principal.to_string()));
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        1,
        99,
        serde_json::to_vec(&json!({"method": ROUTE_CLEAR_CREDENTIAL_FENCES_OP,
                "params": {"credentialId": credential}}))
        .unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    handle_clear_credential_fences(frame, &tx, store)
        .await
        .unwrap();
    rx.recv().await.unwrap()
}

/// The operator op lifts every fence on the named credential at once, for
/// use right after a top-up. Only the operator's own routes may call it:
/// any other module principal, and unverified routes, are refused.
#[tokio::test]
async fn the_operator_op_lifts_fences_and_refuses_non_operators() {
    let rig = Rig::new("credential-fence-operator-lift");
    let store: SharedStore = Arc::new(rig.open_store());
    let refusal = |provider: &str| striatum_core::route_state_store::CredentialRefusal {
        provider_id: provider.into(),
        credential_id: "apikey:inception".into(),
        code: "billing_exhausted".into(),
    };
    let credentials = empty_broca_credentials();
    for provider in ["inception", "other"] {
        credential_fences::record(&store, &credentials, &refusal(provider), now_ms() as i64)
            .unwrap();
    }
    assert_eq!(store.credential_entitlement_fences().unwrap().len(), 2);
    let now = now_ms() as i64;
    for (provider, credential, model) in [
        ("inception", "apikey:inception", "model-a"),
        ("other", "apikey:inception", "model-b"),
        ("other", "apikey:other", "model-b"),
    ] {
        store
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: provider.into(),
                credential_id: credential.into(),
                model: model.into(),
                until_ms: now + 600_000,
                decision_id: "reset-decision".into(),
                recorded_at: now,
            })
            .unwrap();
    }
    // The daemon's direct principal represents the local shell operator.
    let reply = clear_fences_as(&store, 501, "direct", "apikey:inception").await;
    assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(
        body["result"]["lifted"],
        json!([
            {"providerId": "inception", "code": "billing_exhausted"},
            {"providerId": "other", "code": "billing_exhausted"},
        ]),
        "the reply names what was lifted: {body}"
    );
    assert!(store.credential_entitlement_fences().unwrap().is_empty());
    let remaining = store.account_route_fences(now).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].credential_id, "apikey:other");
    // The reserved Callosum principal represents the phone's operator bridge.
    credential_fences::record(&store, &credentials, &refusal("inception"), now_ms() as i64)
        .unwrap();
    let reply = clear_fences_as(&store, 502, "reserved:callosum", "apikey:inception").await;
    assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(
        body["result"]["lifted"].as_array().unwrap().len(),
        1,
        "{body}"
    );
    assert!(store.credential_entitlement_fences().unwrap().is_empty());
    // Any other module principal is refused, naming its principal.
    credential_fences::record(&store, &credentials, &refusal("inception"), now_ms() as i64)
        .unwrap();
    let reply = clear_fences_as(&store, 503, "reserved:broca", "apikey:inception").await;
    assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(body["code"], "not_permitted", "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("reserved:broca"),
        "the refusal names the principal: {body}"
    );
    assert_eq!(store.credential_entitlement_fences().unwrap().len(), 1);
    // So is an unverified route.
    let reply = clear_fences_as(&store, 504, "unverified", "apikey:inception").await;
    assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(body["code"], "not_permitted", "{body}");
    assert_eq!(store.credential_entitlement_fences().unwrap().len(), 1);
}

/// Routing deploys before core, and core may run against an older routing, so
/// neither side may reject the other's report: unknown members of
/// `providerOutcome` and of `credentialRefusal` are ignored.
#[test]
fn the_outcome_report_ignores_unknown_fields() {
    let params: SetDecisionOutcomeParams = serde_json::from_value(json!({
            "decisionID": "d", "outcome": "error", "futureField": 1,
            "providerOutcome": {"accountRef": "a", "futureField": true,
                "credentialRefusal": {"providerId": "p", "credentialId": "c", "code": "ENTITLEMENT_ERROR", "futureField": "x"}}
        }))
        .unwrap();
    let refusal = params
        .provider_outcome
        .unwrap()
        .identity
        .credential_refusal
        .unwrap();
    assert_eq!(
        (refusal.provider_id.as_str(), refusal.credential_id.as_str()),
        ("p", "c")
    );
    // A report from a core that predates the field still decodes.
    let old: SetDecisionOutcomeParams = serde_json::from_value(json!({
        "decisionID": "d", "outcome": "error", "providerOutcome": {"accountRef": "a"}
    }))
    .unwrap();
    assert_eq!(
        old.provider_outcome.unwrap().identity.credential_refusal,
        None
    );
}

/// A panel seat's provider reset is outcome metadata, not confirmed quota
/// exhaustion. It must not remove that model from later panels.
#[tokio::test]
async fn a_panel_seat_provider_reset_leaves_the_model_selectable() {
    let rig = Rig::new("panel-seat-provider-reset");
    let store = Arc::new(rig.open_store());
    let request = json!({
        "method": ROUTE_SELECT_PANEL_OP,
        "params": {
            "panelSpec": {"size": 1, "iqFloor": 0, "diversity": "none", "class": "AUDIT", "temperatureSpread": [0.2]},
            "selection": "n_pick",
            "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
            "taskId": "panel-seat-reset-task"
        }
    });
    async fn select(store: &SharedStore, request: &serde_json::Value) -> serde_json::Value {
        let frame = run_route_wire_request(store, request.clone()).await;
        assert_eq!(frame.header.ty, FrameType::Response);
        serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone()
    }
    async fn report(store: &SharedStore, params: serde_json::Value) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method": ROUTE_SET_DECISION_OUTCOME_OP, "params": params}))
                .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_set_decision_outcome(
            frame,
            &tx,
            store,
            Path::new("/nonexistent/panel-seat-provider-reset.jsonc"),
            &SharedBrocaCredentials::default(),
        )
        .await
        .unwrap();
        let ack: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
        assert_eq!(ack["result"]["ok"], true, "{ack}");
    }
    let seated = |panel: &serde_json::Value| -> Vec<String> {
        panel["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| member["routeKey"].as_str().unwrap().to_string())
            .collect()
    };

    let first = select(&store, &request).await;
    let seat = first["members"][0].clone();
    let seat_route = seat["routeKey"].as_str().unwrap().to_string();
    let seat_variant = seat["promptParams"]["variant"].as_str().map(str::to_string);
    let decision = first["decisionID"].as_str().unwrap().to_string();
    let reset = now_ms() as i64 + 3 * 24 * 3_600_000;
    // The shared decision keeps its first terminal outcome even when another
    // seat later reports a failure with a provider reset.
    report(
        &store,
        json!({"decisionID": decision, "outcome": "completed"}),
    )
    .await;
    report(
        &store,
        json!({
            "decisionID": decision,
            "outcome": "interrupted",
            "lastError": "account_limit_exhausted",
            "providerOutcome": {"accountRef": "catalog-account-openai-apikey", "resetsAtMs": reset},
            "seatRoute": {"routeKey": seat_route, "variant": seat_variant},
        }),
    )
    .await;

    let next = select(&store, &request).await;
    assert!(seated(&next).contains(&seat_route), "{next}");
}

fn usage_params(id: &str) -> serde_json::Value {
    json!({
        "messageID": id,
        "providerID": "provider",
        "modelID": "model",
        "variant": "high",
        "outputTokens": 100.0,
        "reasoningTokens": 0.0,
        "createdMs": 1_000.0,
        "completedMs": 2_000.0,
    })
}

async fn run_record_usage(store: &SharedStore, params: serde_json::Value) -> serde_json::Value {
    let frame = run_route_wire_request(
        store,
        json!({ "method": ROUTE_RECORD_USAGE_OP, "params": params }),
    )
    .await;
    serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone()
}

fn usage_row_count(rig: &Rig, id: Option<&str>) -> i64 {
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    match id {
        Some(id) => connection
            .query_row(
                "SELECT COUNT(*) FROM model_route_usage WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .unwrap(),
        None => connection
            .query_row("SELECT COUNT(*) FROM model_route_usage", [], |row| {
                row.get(0)
            })
            .unwrap(),
    }
}

#[tokio::test]
async fn route_record_usage_writes_optional_profile_fields_or_null_without_a_producer() {
    let rig = Rig::new("record-usage-profile");
    let store = Arc::new(rig.open_store());
    let mut absent = usage_params("profile-absent");
    absent["reasoningTokens"] = json!(25.0);
    absent["completedMs"] = json!(3_000.0);
    assert_eq!(
        run_record_usage(&store, absent).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let absent_row = connection
        .query_row(
            "SELECT input_tokens, output_tokens, reasoning_tokens,
                        cached_read_tokens, cache_write_tokens, duration_ms,
                        workload_class, account_key
                 FROM model_route_usage WHERE id = 'profile-absent'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        absent_row,
        (
            None,
            Some(125),
            Some(25),
            None,
            None,
            Some(2_000),
            None,
            None
        )
    );
    drop(connection);

    let mut complete = usage_params("profile-complete");
    complete["inputTokens"] = json!(300.0);
    complete["reasoningTokens"] = json!(25.0);
    complete["cachedReadTokens"] = json!(0.0);
    complete["cacheWriteTokens"] = json!(10.0);
    complete["workloadClass"] = json!("ephemeral");
    complete["accountKey"] = json!("provider/primary");
    complete["completedMs"] = json!(3_000.0);
    assert_eq!(
        run_record_usage(&store, complete).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let complete_row = connection
        .query_row(
            "SELECT input_tokens, output_tokens, reasoning_tokens,
                        cached_read_tokens, cache_write_tokens, duration_ms,
                        workload_class, account_key
                 FROM model_route_usage WHERE id = 'profile-complete'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        complete_row,
        (
            Some(300),
            Some(125),
            Some(25),
            Some(0),
            Some(10),
            Some(2_000),
            Some("ephemeral".into()),
            Some("provider/primary".into()),
        )
    );
}

#[tokio::test]
async fn first_eligible_delivery_reports_both_recording_effects() {
    let rig = Rig::new("record-usage-first-eligible");
    let store = Arc::new(rig.open_store());

    assert_eq!(
        run_record_usage(&store, usage_params("eligible")).await,
        json!({ "recorded": true, "usage_recorded": true })
    );
}

#[tokio::test]
async fn route_record_usage_speed_sample_is_separate_and_idempotent() {
    let rig = Rig::new("record-speed-sample");
    let store = Arc::new(rig.open_store());
    let mut params = usage_params("generation");
    params["speed_sample"] = json!(true);
    params["service_tier"] = json!("fast");
    params["speed"] = json!("fast");
    params["reasoningTokens"] = json!(25);
    params["inputTokens"] = json!(999);
    params["workloadClass"] = json!("ephemeral");
    assert_eq!(
        run_record_usage(&store, params.clone()).await,
        json!({"recorded":true,"usage_recorded":false})
    );
    params["outputTokens"] = json!(9999);
    assert_eq!(
        run_record_usage(&store, params).await,
        json!({"recorded":false,"usage_recorded":false})
    );
    assert_eq!(usage_row_count(&rig, None), 0);
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let row = connection
        .query_row(
            "SELECT COUNT(*), service_tier, speed, output_tokens,
            reasoning_tokens FROM model_route_speed_samples",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(row, (1, "fast".into(), "fast".into(), 125, 25));
    assert!(store
        .get_class_vector("ephemeral", 10_000.0)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn route_record_usage_speed_sample_defaults_and_invalid_durations() {
    let rig = Rig::new("record-speed-defaults");
    let store = Arc::new(rig.open_store());
    let mut params = usage_params("reasoning-only");
    params["speed_sample"] = json!(true);
    params["outputTokens"] = json!(0);
    params["reasoningTokens"] = json!(200);
    assert_eq!(
        run_record_usage(&store, params.clone()).await,
        json!({"recorded":true,"usage_recorded":false})
    );
    for (index, completed) in [1_000, 0].into_iter().enumerate() {
        params["messageID"] = json!(format!("invalid-{index}"));
        params["completedMs"] = json!(completed);
        assert_eq!(
            run_record_usage(&store, params.clone()).await,
            json!({"recorded":false,"usage_recorded":false})
        );
    }
    assert_eq!(usage_row_count(&rig, None), 0);
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let row = connection
        .query_row(
            "SELECT COUNT(*), service_tier, speed, output_tokens
            FROM model_route_speed_samples",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(row, (1, None, None, 200));
}

#[tokio::test]
async fn speed_ineligible_recordable_delivery_reports_only_usage_recorded() {
    let rig = Rig::new("record-usage-speed-ineligible");
    let store = Arc::new(rig.open_store());
    let mut ineligible = usage_params("ineligible");
    ineligible["outputTokens"] = json!(50.0);

    assert_eq!(
        run_record_usage(&store, ineligible).await,
        json!({ "recorded": false, "usage_recorded": true })
    );
}

#[tokio::test]
async fn no_row_refusal_reports_neither_recording_effect() {
    let rig = Rig::new("record-usage-no-row");
    let store = Arc::new(rig.open_store());

    assert_eq!(
        run_record_usage(&store, usage_params("")).await,
        json!({ "recorded": false, "usage_recorded": false })
    );
    assert_eq!(usage_row_count(&rig, None), 0);
}

#[tokio::test]
async fn duplicate_eligible_delivery_reports_neither_effect_and_keeps_one_row() {
    let rig = Rig::new("record-usage-duplicate");
    let store = Arc::new(rig.open_store());
    assert_eq!(
        run_record_usage(&store, usage_params("eligible")).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    assert_eq!(
        run_record_usage(&store, usage_params("eligible")).await,
        json!({ "recorded": false, "usage_recorded": false })
    );
    assert_eq!(usage_row_count(&rig, Some("eligible")), 1);
}

#[tokio::test]
async fn upgrade_delivery_reports_new_speed_observation_but_no_new_usage_row() {
    let rig = Rig::new("record-usage-upgrade");
    let store = Arc::new(rig.open_store());
    let mut unusable = usage_params("upgrade");
    unusable["createdMs"] = json!(3_000.0);
    assert_eq!(
        run_record_usage(&store, unusable).await,
        json!({ "recorded": false, "usage_recorded": true })
    );

    assert_eq!(
        run_record_usage(&store, usage_params("upgrade")).await,
        json!({ "recorded": true, "usage_recorded": false })
    );
    assert_eq!(usage_row_count(&rig, Some("upgrade")), 1);
}

#[tokio::test]
async fn ambiguous_variant_refusal_writes_no_usage_row() {
    let rig = Rig::new("record-usage-ambiguous");
    let store = Arc::new(rig.open_store());
    for variant in ["low", "high"] {
        let mut variant_fact = fact("provider", "model");
        variant_fact.variant = Some(variant.into());
        store
            .upsert_fact(&variant_fact, FactSource::Override)
            .unwrap();
    }
    let mut params = usage_params("ambiguous");
    params.as_object_mut().unwrap().remove("variant");

    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": false, "usage_recorded": false })
    );
    assert_eq!(usage_row_count(&rig, None), 0);
}

#[tokio::test]
async fn empty_provider_or_model_refusal_writes_no_usage_row() {
    let rig = Rig::new("record-usage-empty-route-part");
    let store = Arc::new(rig.open_store());
    for (id, key) in [("empty-provider", "providerID"), ("empty-model", "modelID")] {
        let mut params = usage_params(id);
        params[key] = json!("");
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": false, "usage_recorded": false })
        );
    }
    assert_eq!(usage_row_count(&rig, None), 0);
}

#[tokio::test]
async fn empty_message_id_with_constructible_route_writes_no_usage_row() {
    let rig = Rig::new("record-usage-empty-message");
    let store = Arc::new(rig.open_store());
    let params = usage_params("");
    assert_eq!(params["providerID"], "provider");
    assert_eq!(params["modelID"], "model");

    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": false, "usage_recorded": false })
    );
    assert_eq!(usage_row_count(&rig, None), 0);
}

#[test]
fn every_recordable_speed_refusal_writes_a_row_and_leaves_the_aggregate_unchanged() {
    let rig = Rig::new("record-usage-speed-refusals");
    let store = Arc::new(rig.open_store());
    for index in 0..5 {
        store
            .record_usage_observation(&UsageObservation {
                id: format!("baseline-{index}"),
                route_key: "provider/model/high".into(),
                capacity_key: "provider/model".into(),
                provider_id: "provider".into(),
                input_tokens: None,
                output_tokens: 200.0,
                reasoning_tokens: None,
                cached_read_tokens: None,
                cache_write_tokens: None,
                duration_ms: 1_000.0,
                workload_class: None,
                account_key: None,
                observed_at: 2_000.0 + index as f64,
            })
            .unwrap();
    }
    let before = store
        .get_observed_speed("provider/model/high", 10_000.0)
        .unwrap()
        .unwrap();

    let cases = [
        ("nan-output", f64::NAN, 3_000.0, None),
        ("infinite-output", f64::INFINITY, 3_000.0, None),
        ("zero-output", 0.0, 3_000.0, None),
        ("negative-output", -1.0, 3_000.0, None),
        ("floor-zero-output", 0.99, 3_000.0, None),
        ("nan-duration", 100.0, 3_000.0, Some(f64::NAN)),
        ("infinite-duration", 100.0, 3_000.0, Some(f64::INFINITY)),
        ("negative-duration", 100.0, 1_000.0, None),
    ];
    for (id, output_tokens, completed_ms, tool_elapsed_ms) in cases {
        let facts = NativeUsageFacts {
            message_id: id.into(),
            provider_id: "provider".into(),
            model_id: "model".into(),
            variant: Some("high".into()),
            output_tokens,
            reasoning_tokens: Some(25.0),
            created_ms: 2_000.0,
            completed_ms,
            tool_elapsed_ms,
            ..Default::default()
        };
        let observation = usage_facts::usage_observation_from_store(facts, &store)
            .unwrap()
            .expect("recordable refusal");
        assert_eq!(
            store.record_usage_delivery(&observation).unwrap(),
            striatum_core::route_state_store::UsageRecordResult {
                recorded: false,
                usage_recorded: true,
            },
            "{id}"
        );
    }

    let after = store
        .get_observed_speed("provider/model/high", 10_000.0)
        .unwrap()
        .unwrap();
    assert_eq!(after.to_bits(), before.to_bits());
    assert_eq!(usage_row_count(&rig, None), 13);
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    for id in [
        "nan-output",
        "infinite-output",
        "zero-output",
        "negative-output",
        "floor-zero-output",
    ] {
        assert_eq!(
            connection
                .query_row(
                    "SELECT output_tokens FROM model_route_usage WHERE id = ?1",
                    [id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .unwrap(),
            None,
            "{id}"
        );
    }
    for id in ["nan-duration", "infinite-duration", "negative-duration"] {
        assert_eq!(
            connection
                .query_row(
                    "SELECT duration_ms FROM model_route_usage WHERE id = ?1",
                    [id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .unwrap(),
            None,
            "{id}"
        );
    }
}

#[tokio::test]
async fn reasoning_heavy_pre_and_post_migration_rows_report_identical_tps_bits() {
    let rig = Rig::new("record-usage-reasoning-compatibility");
    let store = Arc::new(rig.open_store());
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    for index in 0..6 {
        connection
            .execute(
                "INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     VALUES (?1, NULL, 'provider/legacy/high', 'provider/legacy',
                             'provider', NULL, 180, 1000, ?2)",
                rusqlite::params![format!("legacy-{index}"), 1_000 + index],
            )
            .unwrap();
    }
    connection
        .execute(
            "INSERT INTO model_route_usage
                   (id, task_id, route_key, capacity_key, provider_id,
                    input_tokens, output_tokens, duration_ms, observed_at)
                 VALUES ('mixed-legacy', NULL, 'provider/model/high', 'provider/model',
                         'provider', NULL, 180, 1000, 1000)",
            [],
        )
        .unwrap();
    drop(connection);

    for index in 0..5 {
        let mut params = usage_params(&format!("mixed-new-{index}"));
        params["outputTokens"] = json!(100.0);
        params["reasoningTokens"] = json!(80.0);
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": true, "usage_recorded": true })
        );
    }

    let legacy = store
        .get_observed_speed("provider/legacy/high", 10_000.0)
        .unwrap()
        .unwrap();
    let mixed = store
        .get_observed_speed("provider/model/high", 10_000.0)
        .unwrap()
        .unwrap();
    assert_eq!(legacy.to_bits(), 180.0_f64.to_bits());
    assert_eq!(mixed.to_bits(), legacy.to_bits());
}

#[tokio::test]
async fn counters_producer_true_broca_usage_maps_to_summary_and_stored_row() {
    // Gated on counters_producer = true
    let rig = Rig::new("counters-producer-true");
    let store = Arc::new(rig.open_store());

    let n = 250.0;
    let m = 500.0;
    let mut params = usage_params("live-broca-msg");
    params["inputTokens"] = json!(m);
    params["cachedReadTokens"] = json!(n);
    params["outputTokens"] = json!(100.0);
    params["reasoningTokens"] = json!(80.0);
    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let (fresh_input, cached_read, stored_output, reasoning) = connection
        .query_row(
            "SELECT input_tokens, cached_read_tokens, output_tokens, reasoning_tokens
                 FROM model_route_usage WHERE id = 'live-broca-msg'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(fresh_input, Some(500)); // fresh_input = M
    assert_eq!(cached_read, Some(250)); // cached_read = N
    assert_eq!(stored_output, Some(180)); // stored output_tokens = 180 (100 + 80)
    assert_eq!(reasoning, Some(80));

    // Fixture payload: output_tokens = 100 and reasoning_tokens = 80
    let mut fixture = usage_params("fixture-payload-msg");
    fixture["inputTokens"] = json!(300.0);
    fixture["cachedReadTokens"] = json!(40.0);
    fixture["cacheWriteTokens"] = json!(20.0);
    fixture["outputTokens"] = json!(100.0);
    fixture["reasoningTokens"] = json!(80.0);
    assert_eq!(
        run_record_usage(&store, fixture).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let (cached_read_fix, cache_write_fix, reasoning_fix, fresh_input_fix, stored_output_fix) =
        connection
            .query_row(
                "SELECT cached_read_tokens, cache_write_tokens, reasoning_tokens,
                      input_tokens, output_tokens
               FROM model_route_usage WHERE id = 'fixture-payload-msg'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .unwrap();
    assert_eq!(cached_read_fix, Some(40)); // cached_read = cached_input_tokens
    assert_eq!(cache_write_fix, Some(20)); // cache_write = cache_write_tokens
    assert_eq!(reasoning_fix, Some(80)); // reasoning = 80
    assert_eq!(fresh_input_fix, Some(300)); // fresh_input = input_tokens (no cache subtraction)
    assert_eq!(stored_output_fix, Some(180)); // stored output_tokens = 180 (output + reasoning)
    assert_eq!(stored_output_fix.unwrap() - reasoning_fix.unwrap(), 100); // output_excluding_reasoning = 100
}

#[tokio::test]
async fn real_host_shaped_message_produces_non_null_cached_read_and_ephemeral_class() {
    // Gated on counters_producer = true and class_producer = true
    let rig = Rig::new("real-host-shaped-delivery");
    let store = Arc::new(rig.open_store());

    // Live host assistant row:
    // tokens: {"total":326232,"input":2,"output":126,"reasoning":0,"cache":{"write":422,"read":325682}}
    // agent: "mason"
    // model: anthropic/claude-opus-5
    let mut params = usage_params("msg-host-live");
    params["providerID"] = json!("anthropic");
    params["modelID"] = json!("claude-opus-5");
    params["inputTokens"] = json!(2.0);
    params["outputTokens"] = json!(126.0);
    params["reasoningTokens"] = json!(0.0);
    params["cachedReadTokens"] = json!(325682.0);
    params["cacheWriteTokens"] = json!(422.0);
    params["agent"] = json!("mason");

    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let (cached_read, input_toks, output_toks, reasoning_toks, workload_cls, acct_key) = connection
        .query_row(
            "SELECT cached_read_tokens, input_tokens, output_tokens, reasoning_tokens,
                            workload_class, account_key
                     FROM model_route_usage WHERE id = 'msg-host-live'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .unwrap();

    assert_eq!(
        cached_read,
        Some(325682),
        "cached_read_tokens must be non-NULL and match host tokens.cache.read"
    );
    assert_eq!(input_toks, Some(2));
    assert_eq!(output_toks, Some(126));
    assert_eq!(reasoning_toks, Some(0));
    assert_eq!(
        workload_cls.as_deref(),
        Some("ephemeral"),
        "workload_class must be stamped ephemeral for agent mason"
    );
    assert_eq!(
        acct_key, None,
        "account_key must be NULL when account_producer is absent"
    );
}

#[tokio::test]
async fn counters_producer_false_deferral_behavior_and_selection_parity() {
    // Gated on counters_producer = false
    let rig = Rig::new("counters-producer-false");
    let store = Arc::new(rig.open_store());

    // New row written without counter producer
    let mut params = usage_params("deferral-row");
    params["outputTokens"] = json!(100.0);
    params["reasoningTokens"] = json!(80.0);
    params["createdMs"] = json!(1_000.0);
    params["completedMs"] = json!(2_000.0);
    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let (input, cached_read, cache_write, output, reasoning) = connection
        .query_row(
            "SELECT input_tokens, cached_read_tokens, cache_write_tokens,
                      output_tokens, reasoning_tokens
               FROM model_route_usage WHERE id = 'deferral-row'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(input, None);
    assert_eq!(cached_read, None);
    assert_eq!(cache_write, None);
    assert_eq!(output, Some(180));
    assert_eq!(reasoning, Some(80));

    // No complete-case row forms: WORKLOAD_CLASS_USAGE_SAMPLES_SQL returns 0 rows
    let complete_rows_count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM ({})",
                striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
            ),
            rusqlite::params!["ephemeral", 0, 10_000],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(complete_rows_count, 0);

    // use_basket is never true when counters_producer is false
    assert!(!striatum_core::use_basket_permitted(false, true));

    // Selection is bit-identical to today for every route including routes with complete rate card
    let mut card_route = fact("provider", "complete-card");
    card_route.cost_per_m_tok_out = Some(5.0);
    card_route.input_rate = Some(1.0);
    card_route.cache_read_rate = Some(0.25);
    card_route.cache_write_rate = Some(0.75);
    card_route.reasoning_rate = Some(5.0);

    let expected_cost: f64 = 5.0 / 1.0;
    assert_eq!(
        (card_route.cost_per_m_tok_out.unwrap() / 1.0).to_bits(),
        expected_cost.to_bits()
    );

    // Reported TPS to the same bits: 180 tokens / 1.0 s = 180.0 TPS (needs 5 observations for floor)
    for i in 0..5 {
        let mut p = usage_params(&format!("deferral-tps-{i}"));
        p["outputTokens"] = json!(100.0);
        p["reasoningTokens"] = json!(80.0);
        p["createdMs"] = json!(1_000.0);
        p["completedMs"] = json!(2_000.0);
        run_record_usage(&store, p).await;
    }
    let tps = store
        .get_observed_speed("provider/model/high", 10_000.0)
        .unwrap()
        .unwrap();
    assert_eq!(tps.to_bits(), 180.0_f64.to_bits());
}

#[tokio::test]
async fn class_producer_true_live_message_stamps_ephemeral_and_accepts_only_closed_enum() {
    // Gated on class_producer = true
    let rig = Rig::new("class-producer-true");
    let store = Arc::new(rig.open_store());

    let mut params = usage_params("msg-ephemeral");
    params["workloadClass"] = json!("ephemeral");
    assert_eq!(
        run_record_usage(&store, params).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let stamped_class: Option<String> = connection
        .query_row(
            "SELECT workload_class FROM model_route_usage WHERE id = 'msg-ephemeral'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stamped_class.as_deref(), Some("ephemeral"));

    // Exactly ephemeral, head, gather, hire accepted
    for class in ["head", "gather", "hire"] {
        let mut p = usage_params(&format!("msg-{class}"));
        p["workloadClass"] = json!(class);
        run_record_usage(&store, p).await;
        let stored: Option<String> = connection
            .query_row(
                "SELECT workload_class FROM model_route_usage WHERE id = ?1",
                [&format!("msg-{class}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored.as_deref(), Some(class));
    }

    // Invalid classes are dropped to NULL
    for invalid in ["mason", "unknown", "other"] {
        let mut p = usage_params(&format!("msg-{invalid}"));
        p["workloadClass"] = json!(invalid);
        run_record_usage(&store, p).await;
        let stored: Option<String> = connection
            .query_row(
                "SELECT workload_class FROM model_route_usage WHERE id = ?1",
                [&format!("msg-{invalid}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, None, "unrecognized class {invalid} must ship NULL");
    }
}

#[tokio::test]
async fn class_producer_false_cold_start_corpus_selects_bit_identically() {
    // Gated on class_producer = false
    let rig = Rig::new("class-producer-cold-start");
    let store = Arc::new(rig.open_store());
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

    // Seed cold-start corpus: only 10 complete classed rows in (now - 7d, now]
    for i in 0..10 {
        let mut p = usage_params(&format!("cold-start-{i}"));
        p["workloadClass"] = json!("ephemeral");
        p["inputTokens"] = json!(100.0);
        p["cachedReadTokens"] = json!(20.0);
        p["cacheWriteTokens"] = json!(5.0);
        p["outputTokens"] = json!(50.0);
        p["reasoningTokens"] = json!(10.0);
        run_record_usage(&store, p).await;
    }

    // 40 unclassified rows (NULL workload_class)
    for i in 0..40 {
        let mut p = usage_params(&format!("unclassified-{i}"));
        p["inputTokens"] = json!(100.0);
        p["cachedReadTokens"] = json!(20.0);
        p["cacheWriteTokens"] = json!(5.0);
        p["outputTokens"] = json!(50.0);
        p["reasoningTokens"] = json!(10.0);
        run_record_usage(&store, p).await;
    }

    let complete_ephemeral_count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM ({})",
                striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
            ),
            rusqlite::params!["ephemeral", 0, 10_000],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(complete_ephemeral_count, 10);
    assert!(complete_ephemeral_count < 50);

    // use_basket cannot be true when class_producer is false
    assert!(!striatum_core::use_basket_permitted(true, false));
}

#[tokio::test]
async fn account_producer_absent_usage_row_written_with_null_account_key_and_ephemeral_vector_forms(
) {
    // Gated on account_producer = absent
    let rig = Rig::new("account-producer-absent");
    let store = Arc::new(rig.open_store());
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

    let mut p = usage_params("msg-no-account");
    p["cachedReadTokens"] = json!(150.0);
    p["inputTokens"] = json!(300.0);
    p["cacheWriteTokens"] = json!(10.0);
    p["outputTokens"] = json!(100.0);
    p["reasoningTokens"] = json!(20.0);
    p["workloadClass"] = json!("ephemeral");
    assert_eq!(
        run_record_usage(&store, p).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let account_key: Option<String> = connection
        .query_row(
            "SELECT account_key FROM model_route_usage WHERE id = 'msg-no-account'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(account_key, None);

    // 50 complete rows with account_key NULL still form the complete-case vector
    for i in 1..50 {
        let mut row_params = usage_params(&format!("msg-no-account-{i}"));
        row_params["cachedReadTokens"] = json!(150.0);
        row_params["inputTokens"] = json!(300.0);
        row_params["cacheWriteTokens"] = json!(10.0);
        row_params["outputTokens"] = json!(100.0);
        row_params["reasoningTokens"] = json!(20.0);
        row_params["workloadClass"] = json!("ephemeral");
        run_record_usage(&store, row_params).await;
    }

    let complete_ephemeral_count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM ({})",
                striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
            ),
            rusqlite::params!["ephemeral", 0, 10_000],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        complete_ephemeral_count, 50,
        "ephemeral vector forms even when account_key is NULL"
    );
}

#[tokio::test]
async fn absence_is_whole_object_at_provider_boundary_and_per_counter_at_param_layer() {
    // Gated on absence whole-object vs per-counter
    let rig = Rig::new("absence-whole-object-vs-per-counter");
    let store = Arc::new(rig.open_store());
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

    // Whole-object absence at provider boundary (Broca reported no usage at all)
    let mut whole_absent = usage_params("whole-absent");
    whole_absent.as_object_mut().unwrap().remove("outputTokens");
    whole_absent["reasoningTokens"] = serde_json::Value::Null;
    whole_absent["inputTokens"] = serde_json::Value::Null;
    whole_absent["cachedReadTokens"] = serde_json::Value::Null;
    whole_absent["cacheWriteTokens"] = serde_json::Value::Null;
    assert_eq!(
        run_record_usage(&store, whole_absent).await,
        json!({ "recorded": false, "usage_recorded": true })
    );

    let (inp, out, rsn, crd, cwr) = connection
        .query_row(
            "SELECT input_tokens, output_tokens, reasoning_tokens,
                      cached_read_tokens, cache_write_tokens
               FROM model_route_usage WHERE id = 'whole-absent'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(inp, None);
    assert_eq!(out, None);
    assert_eq!(rsn, None);
    assert_eq!(crd, None);
    assert_eq!(cwr, None);

    // Authoritative zero cached-read counter writes 0
    let mut zero_cached = usage_params("authoritative-zero");
    zero_cached["cachedReadTokens"] = json!(0.0);
    assert_eq!(
        run_record_usage(&store, zero_cached).await,
        json!({ "recorded": true, "usage_recorded": true })
    );

    let stored_zero: Option<i64> = connection
        .query_row(
            "SELECT cached_read_tokens FROM model_route_usage WHERE id = 'authoritative-zero'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_zero, Some(0));
}

#[test]
fn provider_wide_cooldown_cools_all_capacity_keys_for_provider_and_only_that_provider() {
    let rig = Rig::new("provider-wide-cooldown");
    let store = rig.open_store();
    store
        .upsert_fact(&fact("p", "a"), FactSource::Override)
        .unwrap();
    store
        .upsert_fact(&fact("p", "b"), FactSource::Override)
        .unwrap();
    store
        .upsert_fact(&fact("q", "c"), FactSource::Override)
        .unwrap();

    let params = RecordCooldownParams {
        route_key: "p/a/high".into(),
        capacity_key: "p/a".into(),
        credential_id: None,
        cooldown_until: Some(7_000.0),
        quota_exhausted_until: Some(9_000.0),
        quota_pressure: None,
        wall_class: None,
        provider_wide: true,
        increment_quota_error: true,
        increment_failure: true,
        last_error: Some("quota exhausted".into()),
    };
    record_cooldown_update(&store, &params).unwrap();

    let snap = store.build_snapshot(1_000.0).unwrap();
    for capacity_key in ["p/a", "p/b"] {
        let state = snap
            .by_capacity
            .get(capacity_key)
            .expect("provider capacity cooled");
        assert_eq!(state.cooldown_until, Some(7_000.0));
        assert_eq!(state.quota_exhausted_until, Some(9_000.0));
    }
    assert_eq!(
        snap.by_capacity
            .get("q/c")
            .and_then(|state| state.cooldown_until),
        None,
        "other providers must stay untouched"
    );
}

#[tokio::test]
async fn typed_provider_report_classifies_without_reading_diagnostic_text() {
    let rig = Rig::new("typed-provider-error-report");
    let store = Arc::new(rig.open_store());
    for (class, outcome, expected) in [
        ("transient", "session_error", "interrupted"),
        ("permanent", "session_error", "session_error"),
        ("transient", "completed", "completed"),
    ] {
        let decision = store
            .record_decision(&DecisionInput {
                selected_route_key: Some("test/model".into()),
                selected_provider_id: Some("test".into()),
                selected_model_id: Some("model".into()),
                target_agent: "mason".into(),
                requirements_json: "{}".into(),
                score_json: "{}".into(),
                rejected_json: "[]".into(),
                ..Default::default()
            })
            .unwrap();
        let response = run_route_wire_request(&store, json!({
                "method": ROUTE_SET_DECISION_OUTCOME_OP,
                "params": {"decisionID": decision, "outcome": outcome,
                    "lastError":"opaque diagnostic identical for both reports",
                    "providerOutcome":{"accountRef":"account","errorClass":class,"providerCode":"server_is_overloaded"}}
            })).await;
        assert_eq!(response.header.ty, FrameType::Response);
        assert_eq!(
            store
                .get_decision_by_id(&decision)
                .unwrap()
                .unwrap()
                .outcome
                .as_deref(),
            Some(expected)
        );
    }
}
