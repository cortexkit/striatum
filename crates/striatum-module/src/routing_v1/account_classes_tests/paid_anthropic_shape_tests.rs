use super::*;
mod unservable_tests;

const MODEL: &str = "claude-haiku-5-5";
const POOL: &str = "anthropic-platform";
const KEYS: [&str; 2] = ["apikey:anthropic:dave", "apikey:anthropic:erin"];

#[tokio::test]
async fn select_ledger_paid_pool_uses_the_admissions_pool_provider_count() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    harness.edit_config(|config| {
        config["concurrency"] = json!({"enforcement_enabled":true,"models":{
            format!("{POOL}/{MODEL}"):{"limit":1}
        }});
    });
    let owner = harness.bind(reserved(OWNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let body =
        json!({"consumer":"codemode","pin":format!("{POOL}/{MODEL}"),"requirements":{"iq":0}});
    for id in ["first", "second"] {
        let selected = owner.call("route.select", &body).await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        let mut request = credential_only_request(id);
        offer_key_account_ids(&mut request);
        request["decision_id"] = selected.1["result"]["decision_id"].clone();
        let admitted = broca.call("route.account_for", &request).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    }
    let full = owner.call("route.select", &body).await;
    assert_eq!(full.0, FrameType::Error, "{}", full.1);
    assert_eq!(full.1["code"], "no_route");
    assert_eq!(full.1["detail"]["reason"], "concurrency_full");
    assert_eq!(full.1["detail"]["reset_at_ms"], NOW + 30_000);
    // The catalog provider's model cap must not cover a pool admission.
    harness.edit_config(|config| {
        config["concurrency"]["models"] = json!({format!("anthropic/{MODEL}"):{"limit":1}});
    });
    assert_eq!(
        owner.call("route.select", &body).await.0,
        FrameType::Response
    );
}

fn paid_anthropic_harness() -> Arc<Harness> {
    let harness = Harness::new("paid-anthropic-shape");
    let mut fact = account_fact("anthropic");
    fact.model_id = MODEL.into();
    harness.install_facts(&[fact.clone()]);
    catalog_fact(&harness, &fact);
    let mut subscription = fact.clone();
    subscription.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
    subscription.billing_plane.as_mut().unwrap().kind = "subscription".into();
    let mut rows: Vec<_> = KEYS
        .iter()
        .map(|id| ListedCredential {
            id: (*id).into(),
            account_id: None,
            state: "active".into(),
            categories: vec!["llm-provider".into()],
            provider_ids: vec![POOL.into()],
            serves: vec!["anthropic".into()],
            credential_type: "apikey".into(),
            auth_method: Some("apikey".into()),
            ..Default::default()
        })
        .collect();
    rows.push(ListedCredential {
        id: "oauth:anthropic:subscription".into(),
        account_id: Some("anthropic-subscription".into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["anthropic".into()],
        serves: vec!["anthropic".into()],
        credential_type: "oauth".into(),
        auth_method: Some("oauth".into()),
        ..Default::default()
    });
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "paid-anthropic-vault".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
    credentials.runner_declarations.insert(
        "broca".into(),
        super::super::runners::parse_declaration(&json!({"runner": {
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"},
                {"wire_api":"anthropic.messages", "auth_method":"oauth"}],
            "follows_account_choice":true
        }}))
        .unwrap()
        .unwrap(),
    );
    credentials
        .runner_routed_accounts
        .insert("broca".into(), vec!["apikey".into(), "oauth".into()]);
    credentials
        .wire_apis
        .insert(format!("anthropic/{MODEL}"), "anthropic.messages".into());
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact, subscription])
            .with_wire_apis(&credentials.wire_apis),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca","via_vault":true}]);
        config["consumers"] = json!({
            "mason":{"owner":OWNER,"mode":"demand"},
            "codemode":{"owner":OWNER,"mode":"demand","pay_per_token":true}
        });
        config["providers"][POOL] = json!({"pin_only":true,"not_for":["mason"]});
    });
    harness
}

fn credential_only_request(id: &str) -> Value {
    json!({
        "provider":POOL,"model":MODEL,"mode":"admit",
        "conversation_key":format!("conversation-{id}"),"admission_id":id,
        "candidates":{"view_digest":"paid-anthropic-vault","accounts":
            KEYS.iter().map(|id| json!({"credential_id":id})).collect::<Vec<_>>()}
    })
}

fn offer_key_account_ids(request: &mut Value) {
    for account in request["candidates"]["accounts"].as_array_mut().unwrap() {
        account["account_id"] = account["credential_id"].clone();
    }
}

fn assert_missing_candidate_account_id(reply: &(FrameType, Value)) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1,
        json!({
            "code":"invalid_request", "detail":{"field":"candidates"},
            "message":"candidates is invalid: missing field `account_id`"
        })
    );
}

#[tokio::test]
async fn broca_paid_anthropic_candidates_require_account_ids_with_pinned_decision() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let owner = harness.bind(reserved(OWNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let selected = owner
        .call(
            "route.select",
            &json!({
                "consumer":"codemode","pin":format!("{POOL}/{MODEL}"),"requirements":{"iq":0}
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    assert_eq!(
        selected.1["result"]["route"]["provider"], POOL,
        "{}",
        selected.1
    );
    assert_eq!(
        selected.1["result"]["runner"]["module"], "broca",
        "{}",
        selected.1
    );
    assert_eq!(
        selected.1["result"]["runner"]["model"]["provider"], "anthropic",
        "{}",
        selected.1
    );
    let mut request = credential_only_request("paid-anthropic-with-decision");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    assert_missing_candidate_account_id(&broca.call("route.account_for", &request).await);
    offer_key_account_ids(&mut request);
    let admitted = broca.call("route.account_for", &request).await;
    assert_eq!(
        admitted.0,
        FrameType::Response,
        "request={request}; reply={}",
        admitted.1
    );
    assert!(KEYS.contains(
        &admitted.1["result"]["account"]["credential_id"]
            .as_str()
            .unwrap()
    ));
    assert_eq!(admitted.1["result"]["account"]["provider"], POOL);
    assert_eq!(admitted.1["result"]["account"]["method"], "apikey");
}

#[tokio::test]
async fn broca_paid_anthropic_candidates_require_account_ids_without_decision() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let broca = harness.bind(reserved("broca"), None).await;
    let mut request = credential_only_request("paid-anthropic-without-decision");
    assert_missing_candidate_account_id(&broca.call("route.account_for", &request).await);
    offer_key_account_ids(&mut request);
    let admitted = broca.call("route.account_for", &request).await;
    assert_eq!(
        admitted.0,
        FrameType::Response,
        "request={request}; reply={}",
        admitted.1
    );
    assert!(KEYS.contains(
        &admitted.1["result"]["account"]["credential_id"]
            .as_str()
            .unwrap()
    ));
    assert_eq!(
        admitted.1["result"]["account"]["account_id"],
        admitted.1["result"]["account"]["credential_id"]
    );
    assert_eq!(admitted.1["result"]["account"]["provider"], POOL);
    assert_eq!(admitted.1["result"]["account"]["method"], "apikey");

    // Naming the subscription provider instead cannot force a paid fallback.
    request["provider"] = json!("anthropic");
    request["admission_id"] = json!("subscription-provider-without-decision");
    request["conversation_key"] = json!("subscription-provider-without-decision");
    let refused = broca.call("route.account_for", &request).await;
    assert_eq!(
        refused.0,
        FrameType::Error,
        "request={request}; reply={}",
        refused.1
    );
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["field"], "decision_id",
        "request={request}; reply={}",
        refused.1
    );
    assert_eq!(
        refused.1["detail"]["exclusions"],
        json!(KEYS.iter().map(|id|
        json!({"account_id":id,"credential_id":id,"reason":"routing_decision_required"})
    ).collect::<Vec<_>>())
    );
}

async fn observe_ok(broca: &super::super::conformance_tests::Route, params: &Value) -> Value {
    let reply = broca.call("route.account_observe", params).await;
    assert_eq!(
        reply.0,
        FrameType::Response,
        "request={params}; reply={}",
        reply.1
    );
    assert_eq!(
        reply.1["result"]["observation"]["bound"], true,
        "{}",
        reply.1
    );
    assert_eq!(
        reply.1["result"]["observation"]["duplicate"], false,
        "{}",
        reply.1
    );
    reply.1["result"]["observation"].clone()
}

#[tokio::test]
async fn broca_paid_anthropic_attempts_and_limits_use_the_admission_pool() {
    let _clock = crate::InjectedClock::set(NOW);
    for with_decision in [true, false] {
        for reported_provider in [POOL, "anthropic"] {
            let harness = paid_anthropic_harness();
            let broca = harness.bind(reserved("broca"), None).await;
            let mut request = credential_only_request("paid-observed");
            offer_key_account_ids(&mut request);
            if with_decision {
                let owner = harness.bind(reserved(OWNER), None).await;
                let selected = owner.call("route.select", &json!({
                    "consumer":"codemode","pin":format!("{POOL}/{MODEL}"),"requirements":{"iq":0}
                })).await;
                assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
                request["decision_id"] = selected.1["result"]["decision_id"].clone();
            }
            let admitted = broca.call("route.account_for", &request).await;
            assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
            let account = &admitted.1["result"]["account"];
            assert_eq!(account["provider"], POOL);
            assert_eq!(account["method"], "apikey");
            assert_eq!(account["account_id"], account["credential_id"]);
            let key = account["credential_id"].as_str().unwrap();
            assert!(KEYS.contains(&key));
            observe_ok(
                &broca,
                &json!({
                    "event_id":"paid-start","kind":"admission_started",
                    "admission_id":"paid-observed","run_id":"paid-run"
                }),
            )
            .await;

            let mut attempt = json!({
                "event_id":"paid-attempt","kind":"attempt",
                "admission_id":"paid-observed","run_id":"paid-run",
                "served_account_id":key,"credential_id":key,"provider":reported_provider,
                "observed_at_ms":NOW,"upstream_origin":"upstream",
                "headers":{
                    "anthropic-ratelimit-requests-limit":"50",
                    "anthropic-ratelimit-requests-remaining":"49",
                    "anthropic-ratelimit-requests-reset":"2027-01-15T08:01:00Z",
                    "anthropic-ratelimit-tokens-limit":"30000",
                    "anthropic-ratelimit-tokens-remaining":"29000"
                }
            });
            // The closed v1 observation has no provider member. Broca's older
            // caller-bearing shape preserves it as raw, non-authoritative data.
            let refused = broca.call("route.account_observe", &attempt).await;
            assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
            assert_eq!(
                refused.1,
                json!({"code":"invalid_request",
                "detail":{"field":"provider"},"message":"provider is not a member of this request"})
            );
            attempt["caller"] = json!({"runner":"broca"});
            let observed = observe_ok(&broca, &attempt).await;
            assert_eq!(observed["effects"], json!([]));
            harness
                .store()
                .account_read(|tx| {
                    let admission = tx.admission("reserved:broca", "paid-observed")?.unwrap();
                    assert_eq!(admission.provider, POOL);
                    assert_eq!(admission.account_id, key);
                    assert_eq!(admission.credential_id, key);
                    let observation = tx
                        .observation_by_event("reserved:broca", "broca", "paid-attempt")?
                        .unwrap();
                    assert!(observation.bound);
                    assert_eq!(observation.admission_id.as_deref(), Some("paid-observed"));
                    assert_eq!(
                        serde_json::from_str::<Value>(&observation.observation_json).unwrap(),
                        attempt
                    );
                    assert!(tx.active_fences(POOL)?.is_empty());
                    assert!(tx.active_fences("anthropic")?.is_empty());
                    Ok(())
                })
                .unwrap();

            // A separate explicit exhaustion event creates a quota fence in the
            // admitted pool; the plain per-minute API headers above do not.
            let limited = observe_ok(
                &broca,
                &json!({
                    "event_id":"paid-limit","kind":"account_limit_exhausted",
                    "admission_id":"paid-observed","credential_id":key,
                    "served_account_id":key,"limit_window":"five_hour",
                    "observed_at_ms":NOW,"resets_at_ms":NOW + 300_000,"resets_at_source":"header"
                }),
            )
            .await;
            assert!(
                limited["effects"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|effect| effect.as_str().is_some_and(|text| text.contains("fence"))),
                "{limited}"
            );
            let fences = harness
                .store()
                .account_read(|tx| tx.active_fences(POOL))
                .unwrap();
            assert_eq!(fences.len(), 1);
            assert_eq!(fences[0].account_id, key);
            assert_eq!(fences[0].provider, POOL);
            assert_eq!(fences[0].admission_id.as_deref(), Some("paid-observed"));
            assert_eq!(fences[0].observation_id.as_deref(), limited["id"].as_str());

            let scope = FenceScope::General(GeneralWindow::FiveHour);
            let subscription_fence = Fence {
                account_id: "anthropic-subscription".into(),
                pool: Some(scope.into()),
                recovery_set: Vec::new(),
                observed_at_ms: NOW as i64 - 1_000,
                reset: ResetTime::At(NOW as i64 + 300_000),
                retry_hint_ms: None,
                source: ConfirmationSource::BrocaAccountLimit,
            };
            harness
                .store()
                .account_transaction(|tx| {
                    tx.insert_fence(&AccountFenceRow {
                        fence_id: "subscription-sentinel".into(),
                        provider: "anthropic".into(),
                        account_id: subscription_fence.account_id.clone(),
                        scope_json: serde_json::to_string(&scope).unwrap(),
                        fence_json: serde_json::to_string(&subscription_fence).unwrap(),
                        source: "broca_limit_exhausted".into(),
                        observed_at: NOW as i64 - 1_000,
                        principal: "reserved:broca".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: NOW as i64 - 1_000,
                        cleared_at: None,
                        cleared_reason: None,
                    })
                })
                .unwrap();

            attempt["event_id"] = json!("paid-served-after-limit");
            assert_eq!(observe_ok(&broca, &attempt).await["effects"], json!([]));
            observe_ok(
                &broca,
                &json!({
                    "event_id":"paid-finish","kind":"run_finished",
                    "admission_id":"paid-observed","run_id":"paid-run"
                }),
            )
            .await;
            harness
                .store()
                .account_read(|tx| {
                    let admission = tx.admission("reserved:broca", "paid-observed")?.unwrap();
                    assert_eq!(admission.state, "finished");
                    let affinity = tx
                        .affinity("reserved:broca", &admission.conversation_hash, POOL)?
                        .unwrap();
                    assert_eq!(affinity.lease_valid_until, Some(NOW as i64));
                    // Ordinary LLM account observations do not claim serving-success
                    // healing: even the admitted key's quota fence remains in place.
                    let remaining = tx.active_fences(POOL)?;
                    assert_eq!(remaining.len(), 1);
                    assert_eq!(remaining[0].fence_id, fences[0].fence_id);
                    let subscription = tx.active_fences("anthropic")?;
                    assert_eq!(subscription.len(), 1);
                    assert_eq!(subscription[0].fence_id, "subscription-sentinel");
                    Ok(())
                })
                .unwrap();
        }
    }
}

fn single_key_request(id: &str) -> Value {
    let mut request = credential_only_request(id);
    offer_key_account_ids(&mut request);
    request["candidates"]["accounts"]
        .as_array_mut()
        .unwrap()
        .truncate(1);
    request
}

#[path = "observe_success_tests.rs"]
mod observe_success_tests;

fn success_observation(id: &str, legacy: bool) -> Value {
    let mut params = json!({"event_id":format!("{id}/success"),"kind":"attempt",
        "admission_id":id,"served_account_id":KEYS[0],"credential_id":KEYS[0],
        "attempt_outcome":{"status":"served"}});
    if legacy {
        params["caller"] = json!({"runner":"broca"});
    }
    params
}

async fn served_attempt_and_completed_run(
    broca: &super::super::conformance_tests::Route,
    id: &str,
) {
    observe_ok(
        broca,
        &json!({
            "event_id":format!("{id}/start"),"kind":"admission_started",
            "admission_id":id,"run_id":id
        }),
    )
    .await;
    let attempt = observe_ok(
        broca,
        &json!({
            "caller":{"runner":"broca"},
            "event_id":format!("{id}/served"),"kind":"attempt",
            "admission_id":id,"run_id":id,"served_account_id":KEYS[0],
            "credential_id":KEYS[0],"upstream_origin":"upstream"
        }),
    )
    .await;
    assert_eq!(attempt["effects"], json!([]));
    observe_ok(
        broca,
        &json!({
            "caller":{"runner":"broca"},
            "event_id":format!("{id}/finish"),"kind":"run_finished",
            "admission_id":id,"run_id":id,"terminal_reason":"completed",
            "upstream_origin":"upstream"
        }),
    )
    .await;
}

#[tokio::test]
async fn decisionless_paid_key_quota_probe_heals_only_after_typed_served_attempt() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let observed_at = NOW as i64 - 900_001;
    let fence = Fence {
        account_id: KEYS[0].into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: observed_at,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: "decisionless-quota".into(),
                provider: POOL.into(),
                account_id: KEYS[0].into(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at,
                principal: "reserved:broca".into(),
                admission_id: None,
                observation_id: None,
                created_at: observed_at,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
    let broca = harness.bind(reserved("broca"), None).await;
    let admitted = broca
        .call("route.account_for", &single_key_request("quota-probe"))
        .await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    harness
        .store()
        .account_read(|tx| {
            let admission = tx.admission("reserved:broca", "quota-probe")?.unwrap();
            assert_eq!(admission.account_id, KEYS[0]);
            let rechecks = tx.fence_rechecks()?;
            assert_eq!(rechecks.len(), 1);
            assert_eq!(rechecks[0].last_claimed_at, Some(NOW as i64));
            assert_eq!(rechecks[0].next_recheck_at, NOW as i64 + 900_000);
            Ok(())
        })
        .unwrap();
    served_attempt_and_completed_run(&broca, "quota-probe").await;
    let blocked = broca
        .call("route.account_for", &single_key_request("quota-too-soon"))
        .await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(blocked.1["code"], "account_pool_exhausted");

    let success = observe_ok(&broca, &success_observation("quota-probe", false)).await;
    assert!(!success["effects"].as_array().unwrap().is_empty());
    let retried = broca
        .call("route.account_for", &single_key_request("quota-retest"))
        .await;
    assert_eq!(retried.0, FrameType::Response, "{}", retried.1);
    harness
        .store()
        .account_read(|tx| {
            let fences = tx.active_fences(POOL)?;
            assert!(fences.is_empty());
            assert!(tx.fence_rechecks()?.is_empty());
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn decisionless_paid_key_non_quota_probe_heals_only_after_typed_served_attempt() {
    for code in ["credential_refused", "billing_exhausted"] {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = paid_anthropic_harness();
        let observed_at = NOW as i64 - 3_600_001;
        harness
            .store()
            .record_credential_entitlement_fence(
                &CredentialRefusal {
                    provider_id: POOL.into(),
                    credential_id: KEYS[0].into(),
                    code: code.into(),
                },
                None,
                observed_at,
            )
            .unwrap();
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = broca
            .call("route.account_for", &single_key_request("non-quota-probe"))
            .await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        harness
            .store()
            .account_read(|tx| {
                let rechecks = tx.fence_rechecks()?;
                assert_eq!(rechecks.len(), 1);
                assert_eq!(rechecks[0].last_claimed_at, Some(NOW as i64));
                assert_eq!(rechecks[0].next_recheck_at, NOW as i64 + 3_600_000);
                Ok(())
            })
            .unwrap();
        served_attempt_and_completed_run(&broca, "non-quota-probe").await;
        let next = broca
            .call(
                "route.account_for",
                &single_key_request("non-quota-next-probe"),
            )
            .await;
        assert_eq!(next.0, FrameType::Response, "{}", next.1);
        assert_eq!(next.1["result"]["reason"]["code"], "credential_fence_probe");
        let blocked = broca
            .call(
                "route.account_for",
                &single_key_request("non-quota-too-soon"),
            )
            .await;
        assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
        assert_eq!(blocked.1["code"], "account_pool_exhausted");
        assert_eq!(blocked.1["detail"]["reset_at_ms"], NOW + 3_600_000);

        let success = observe_ok(&broca, &success_observation("non-quota-next-probe", true)).await;
        assert!(!success["effects"].as_array().unwrap().is_empty());
        let retried = broca
            .call("route.account_for", &single_key_request("non-quota-retest"))
            .await;
        assert_eq!(retried.0, FrameType::Response, "{}", retried.1);
        let fences = harness.store().credential_entitlement_fences().unwrap();
        assert!(fences.is_empty(), "{code}: {fences:?}");
        harness
            .store()
            .account_read(|tx| {
                assert!(tx.fence_rechecks()?.is_empty());
                Ok(())
            })
            .unwrap();
    }
}

#[tokio::test]
async fn unanswered_finish_releases_non_quota_but_keeps_quota_probe_cadence() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let store = harness.store();
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let observed_at = NOW as i64 - 3_600_001;
    let quota = Fence {
        account_id: KEYS[0].into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: observed_at,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    store
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: "quota-split".into(),
                provider: POOL.into(),
                account_id: KEYS[0].into(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&quota).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at,
                principal: "reserved:broca".into(),
                admission_id: None,
                observation_id: None,
                created_at: observed_at,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
    store
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: POOL.into(),
                credential_id: KEYS[0].into(),
                code: "billing_exhausted".into(),
            },
            None,
            observed_at,
        )
        .unwrap();
    let broca = harness.bind(reserved("broca"), None).await;
    let admitted = result(
        broca
            .call("route.account_for", &single_key_request("split-probe"))
            .await,
    );
    assert_eq!(admitted["reason"]["code"], "credential_fence_probe");
    let read_clock = |quota| {
        store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|origin| origin.quota == quota)
            .unwrap()
    };
    let claimed_quota = read_clock(true);
    let claimed_non_quota = read_clock(false);
    assert_eq!(claimed_quota.next_recheck_at, NOW as i64 + 900_000);
    assert_eq!(claimed_non_quota.next_recheck_at, NOW as i64 + 3_600_000);
    for claim in [&claimed_quota, &claimed_non_quota] {
        assert_eq!(claim.last_claimed_at, Some(NOW as i64));
    }

    served_attempt_and_completed_run(&broca, "split-probe").await;
    let quota = read_clock(true);
    let non_quota = read_clock(false);
    assert_eq!(non_quota.next_recheck_at, NOW as i64);
    assert_eq!(non_quota.evidence, claimed_non_quota.evidence);
    assert_eq!(quota.next_recheck_at, claimed_quota.next_recheck_at);
    assert_eq!(quota.last_claimed_at, claimed_quota.last_claimed_at);
    assert_eq!(quota.evidence, claimed_quota.evidence);
    let blocked = broca
        .call(
            "route.account_for",
            &single_key_request("split-probe-too-soon"),
        )
        .await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(blocked.1["code"], "account_pool_exhausted");
    let _clock = crate::InjectedClock::set(NOW + 900_000);
    let after_quota = result(
        broca
            .call(
                "route.account_for",
                &single_key_request("split-probe-after-quota"),
            )
            .await,
    );
    assert_eq!(after_quota["reason"]["code"], "credential_fence_probe");
}

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

#[tokio::test]
async fn completed_report_heals_its_admission_probe_after_completed_run() {
    assert_later_report_heals("completed", "unique").await;
}

#[tokio::test]
async fn completed_report_heals_its_admission_probe_after_interrupted_run() {
    assert_later_report_heals("interrupted", "unique").await;
}

#[tokio::test]
async fn completed_report_with_ambiguous_admissions_heals_nothing() {
    assert_later_report_heals("completed", "ambiguous").await;
}

#[tokio::test]
async fn completed_report_with_another_login_heals_nothing() {
    assert_later_report_heals("completed", "other-login").await;
}

async fn assert_later_report_heals(earlier: &str, shape: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let owner = harness.bind(reserved(OWNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let selected = result(
        owner
            .call(
                "route.select",
                &json!({
                    "consumer":"codemode","pin":format!("{POOL}/{MODEL}"),"requirements":{"iq":0}
                }),
            )
            .await,
    );
    let decision = selected["decision_id"].as_str().unwrap();
    let mut first = single_key_request("earlier-run");
    first["decision_id"] = json!(decision);
    result(broca.call("route.account_for", &first).await);
    served_attempt_and_completed_run(&broca, "earlier-run").await;
    result(
        owner
            .call(
                "route.report",
                &json!({
                    "decision_id":decision,"outcome":earlier,
                    "usage":{"run_id":"earlier-run","output_tokens":1}
                }),
            )
            .await,
    );

    let _refusal = crate::InjectedClock::set(NOW + 1_000);
    observe_ok(
        &broca,
        &json!({
            "event_id":"old-transient","kind":"transient_backoff",
            "admission_id":"earlier-run","retry_at_ms":NOW + 2_000
        }),
    )
    .await;
    let _probe = crate::InjectedClock::set(NOW + 3_000);
    let mut request = single_key_request("later-probe");
    request["decision_id"] = json!(decision);
    result(broca.call("route.account_for", &request).await);
    served_attempt_and_completed_run(&broca, "later-probe").await;
    harness
        .store()
        .account_read(|tx| {
            let rechecks = tx.fence_rechecks()?;
            assert_eq!(rechecks.len(), 1);
            assert_eq!(rechecks[0].kind, "observation");
            assert_eq!(rechecks[0].last_claimed_at, Some(NOW as i64 + 3_000));
            Ok(())
        })
        .unwrap();
    if shape == "ambiguous" {
        let mut sibling = single_key_request("same-run-sibling");
        sibling["decision_id"] = json!(decision);
        sibling["candidates"]["accounts"][0] =
            json!({"account_id":KEYS[1],"credential_id":KEYS[1]});
        result(broca.call("route.account_for", &sibling).await);
        observe_ok(
            &broca,
            &json!({"event_id":"second-start","kind":"admission_started",
                "admission_id":"same-run-sibling","run_id":"later-probe"}),
        )
        .await;
    }
    // A sibling's older refusal and a newer refusal of the serving login
    // must survive even a genuine success of this admission.
    for (credential, at) in [(KEYS[1], NOW as i64 - 1_000), (KEYS[0], NOW as i64 + 3_001)] {
        harness
            .store()
            .record_credential_entitlement_fence(
                &CredentialRefusal {
                    provider_id: POOL.into(),
                    credential_id: credential.into(),
                    code: "credential_refused".into(),
                },
                None,
                at,
            )
            .unwrap();
    }
    let mut report = json!({"decision_id":decision,"outcome":"completed",
            "usage":{"run_id":"later-probe","output_tokens":1}});
    if shape == "other-login" {
        report["credential_id"] = json!(KEYS[1]);
    }
    result(owner.call("route.report", &report).await);
    harness
        .store()
        .account_read(|tx| {
            let remaining = tx.fence_rechecks()?;
            assert_eq!(
                remaining.iter().any(|origin| origin.kind == "observation"),
                shape != "unique",
                "only an unambiguous serving run can settle its admission probe after {earlier}"
            );
            assert_eq!(
                tx.report_credential_refusals()?.len(),
                2,
                "neither the sibling nor the newer serving-login refusal can be healed"
            );
            Ok(())
        })
        .unwrap();
    if shape == "unique" {
        let _newer = crate::InjectedClock::set(NOW + 4_000);
        observe_ok(
            &broca,
            &json!({"event_id":"new-transient","kind":"transient_backoff",
                "admission_id":"later-probe","retry_at_ms":NOW+5_000}),
        )
        .await;
        result(owner.call("route.report", &report).await);
        harness
            .store()
            .account_read(|tx| {
                assert!(
                    tx.fence_rechecks()?
                        .iter()
                        .any(|origin| origin.kind == "observation"),
                    "duplicate report delivery cannot heal new evidence"
                );
                Ok(())
            })
            .unwrap();
    }
}
