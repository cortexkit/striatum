use super::*;

const CONSUMER: &str = "prefrontal-core.mason";
const SETTINGS_CONSUMER: &str = "mason";
const MODEL: &str = "synthetic/model";
const EXEMPT: &str = "pinned: keep-free margin not applied";

#[derive(Clone, Copy, Debug)]
enum Origin {
    Consumer,
    Ladder,
    Request,
    Project,
    Slot,
}

fn fixture(origin: Origin, used: f64) -> (Arc<Harness>, impl Drop) {
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["keep_free_percent"] = json!(30);
        match origin {
            Origin::Consumer => {
                config["consumers"][SETTINGS_CONSUMER]["pin"] = json!({"model":MODEL})
            }
            Origin::Ladder => {
                config["consumers"][SETTINGS_CONSUMER]["mode"] = json!("ladder");
                config["consumers"][SETTINGS_CONSUMER]["ladder"] = json!(["absent/model", MODEL]);
            }
            Origin::Project => {
                config["consumers"][SETTINGS_CONSUMER]["projects"]["keep-free-project"] =
                    json!({"pin":MODEL})
            }
            Origin::Slot => config["consumers"]["settings-slot"] = json!({"pin":{"model":MODEL}}),
            Origin::Request => {
                let policy = config["consumers"]
                    .as_object_mut()
                    .unwrap()
                    .remove("mason")
                    .unwrap();
                config["consumers"][CONSUMER] = policy;
            }
        }
    });
    oauth_weekly_reading_at(used, (NOW + 86_400_000) as i64, NOW as i64);
    let pools = crate::subscription_economics::test_pools(std::collections::HashMap::from([(
        "synthetic".into(),
        striatum_core::subscription::SubscriptionPool {
            provider: "synthetic".into(),
            now_ms: NOW as i64,
            plans: vec![striatum_core::subscription::SubscriptionPlan {
                tier: None,
                plan: None,
                measured: None,
                fee_usd: 200.0,
                accounts: vec![striatum_core::subscription::QuotaAccount {
                    weekly_used: used,
                    weekly_remaining_fraction: 1.0,
                    hours_to_reset: 24.0,
                    short_walled: false,
                    short_windows: vec![],
                    banked_resets: 0,
                    banked_expires_at_ms: None,
                    banked_credit_expiries_ms: vec![],
                    prepaid_credits: false,
                }],
            }],
            ..Default::default()
        },
    )]));
    (harness, pools)
}

fn selection_request(origin: Origin, consumer: &str) -> Value {
    let mut request =
        json!({"consumer":consumer,"requirements":{"iq":0},"detail":true,"hold_wait_max_s":0});
    if matches!(origin, Origin::Request) {
        request["pin"] = json!(MODEL);
    }
    if matches!(origin, Origin::Slot) {
        request["pin_slot"] = json!("settings-slot");
    }
    request
}

async fn assert_below_margin(origin: Origin, consumer: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = fixture(origin, 85.0);
    let caller = harness
        .bind(
            reserved(OWNER),
            matches!(origin, Origin::Project).then_some("keep-free-project"),
        )
        .await;
    let reply = caller
        .call("route.select", &selection_request(origin, consumer))
        .await;
    assert_eq!(reply.0, FrameType::Response, "{origin:?}: {reply:?}");
    assert_eq!(
        reply.1["result"]["route"]["provider"], "synthetic",
        "{reply:?}"
    );
    assert_eq!(reply.1["result"]["route"]["model"], "model", "{reply:?}");
    let id = reply.1["result"]["decision_id"].as_str().unwrap();
    let decision = harness.store().get_decision_by_id(id).unwrap().unwrap();
    assert!(decision
        .explanation_json
        .as_deref()
        .unwrap()
        .contains(EXEMPT));
    assert!(
        reply.1.to_string().contains("keep-free margin 0.0%"),
        "{reply:?}"
    );
    assert_eq!(
        reply.1["result"]["detail"]["score"]["objective"]["subscriptionPricing"]["availablePoints"],
        15.0,
        "{reply:?}"
    );

    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("synthetic", &format!("pinned-{origin:?}-{consumer}"));
    request["decision_id"] = json!(id);
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:test",
    );
}

#[tokio::test]
async fn settings_consumer_pin_spends_below_keep_free() {
    assert_below_margin(Origin::Consumer, SETTINGS_CONSUMER).await;
}

#[tokio::test]
async fn settings_ladder_step_spends_below_keep_free() {
    assert_below_margin(Origin::Ladder, SETTINGS_CONSUMER).await;
}

#[tokio::test]
async fn settings_project_pin_spends_below_keep_free() {
    assert_below_margin(Origin::Project, SETTINGS_CONSUMER).await;
}

#[tokio::test]
async fn settings_slot_pin_spends_below_keep_free() {
    assert_below_margin(Origin::Slot, SETTINGS_CONSUMER).await;
}

#[tokio::test]
async fn mason_request_pin_spends_below_keep_free() {
    assert_below_margin(Origin::Request, CONSUMER).await;
}

#[tokio::test]
async fn mason_dotted_request_pin_spends_below_keep_free() {
    assert_below_margin(Origin::Request, "prefrontal-core.mason.retry.launch").await;
}

#[tokio::test]
async fn pinned_origins_cannot_spend_a_walled_account() {
    let _clock = crate::InjectedClock::set(NOW);
    for origin in [
        Origin::Consumer,
        Origin::Ladder,
        Origin::Request,
        Origin::Project,
        Origin::Slot,
    ] {
        let (harness, _pools) = fixture(origin, 100.0);
        let caller = harness
            .bind(
                reserved(OWNER),
                matches!(origin, Origin::Project).then_some("keep-free-project"),
            )
            .await;
        let consumer = if matches!(origin, Origin::Request) {
            CONSUMER
        } else {
            SETTINGS_CONSUMER
        };
        let reply = caller
            .call("route.select", &selection_request(origin, consumer))
            .await;
        assert_eq!(reply.0, FrameType::Error, "{origin:?}: {reply:?}");
        assert!(
            matches!(
                reply.1["code"].as_str(),
                Some("pin_unavailable" | "no_route")
            ),
            "{origin:?}: {reply:?}"
        );
    }
}

#[tokio::test]
async fn non_mason_request_pins_and_demand_keep_the_reserve() {
    let _clock = crate::InjectedClock::set(NOW);
    for (consumer, pin) in [
        ("mason", true),
        ("prefrontal-core.masonish", true),
        (CONSUMER, false),
    ] {
        let (harness, _pools) = fixture(Origin::Request, 85.0);
        harness.edit_config(|config| {
            if consumer == "prefrontal-core.masonish" {
                config["consumers"][consumer] = json!({"owner":OWNER,"mode":"demand"});
            }
        });
        let caller = harness.bind(reserved(OWNER), None).await;
        let mut request = selection_request(Origin::Request, consumer);
        if !pin {
            request.as_object_mut().unwrap().remove("pin");
        }
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{consumer} pin={pin}: {reply:?}");
        assert!(
            reply.1.to_string().contains("subscription_reserve"),
            "{reply:?}"
        );
    }
}

#[tokio::test]
async fn settings_pins_and_ladders_explain_the_exemption_in_what_if() {
    let _clock = crate::InjectedClock::set(NOW);
    for origin in [Origin::Consumer, Origin::Ladder] {
        let (harness, _pools) = fixture(origin, 85.0);
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let reply = operator
            .call(
                "route.what_if",
                &json!({"consumer":SETTINGS_CONSUMER,"iqs":[0]}),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        assert_eq!(
            reply.1["result"]["results"][0]["pick"]["route"], MODEL,
            "{reply:?}"
        );
        assert_eq!(
            reply.1["result"]["results"][0]["pick"]["explanation"], EXEMPT,
            "{reply:?}"
        );
    }
}

#[tokio::test]
async fn module_declared_ladder_keeps_the_reserve() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = fixture(Origin::Request, 85.0);
    let consumer = format!("{OWNER}.module");
    let caller = harness.bind(reserved(OWNER), None).await;
    super::super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        super::super::consumers::replace_catalog(&json!([{
            "module_id":OWNER,"nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":consumer,"ladder":[MODEL]}
            ]}}
        }]));
        let mut request = selection_request(Origin::Request, &consumer);
        request.as_object_mut().unwrap().remove("pin");
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert!(
            reply.1.to_string().contains("subscription_reserve"),
            "{reply:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn an_exempt_pin_cannot_lift_the_allocator_floor() {
    let _clock = crate::InjectedClock::set(NOW);
    for origin in [Origin::Consumer, Origin::Ladder, Origin::Request] {
        let (harness, _pools) = fixture(origin, 99.5);
        let caller = harness.bind(reserved(OWNER), None).await;
        let consumer = if matches!(origin, Origin::Request) {
            CONSUMER
        } else {
            SETTINGS_CONSUMER
        };
        let reply = caller
            .call("route.select", &selection_request(origin, consumer))
            .await;
        assert_eq!(reply.0, FrameType::Error, "{origin:?}: {reply:?}");
    }
}

#[tokio::test]
async fn failed_pins_do_not_exempt_automatic_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = fixture(Origin::Slot, 85.0);
    harness.edit_config(|config| {
        config["consumers"]["settings-slot"]["pin"]["model"] = json!("absent/model");
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let mut request = selection_request(Origin::Slot, SETTINGS_CONSUMER);
    request["pin_fallback"] = json!("route");
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert!(
        reply.1.to_string().contains("subscription_reserve"),
        "{reply:?}"
    );
    let (harness, _pools) = fixture(Origin::Request, 85.0);
    harness.attach_reasoning_catalog(striatum_store::ReasoningCatalog {
        models: [(
            MODEL.into(),
            striatum_store::PublishedEfforts::Published(vec!["high".into()]),
        )]
        .into_iter()
        .collect(),
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let mut request = selection_request(Origin::Request, CONSUMER);
    request["pin"] = json!("synthetic/model:not-a-real-variant");
    request["pin_fallback"] = json!("route");
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert!(
        reply.1.to_string().contains("subscription_reserve"),
        "{reply:?}"
    );
}

const SYNTHESIZER: &str = "prefrontal-core.athena.synthesizer";
const SYNTHESIZER_PIN: &str = "synthetic/model:medium";

const USER_PIN_CONSUMER: &str = "pin-origin-test";
const USER_PIN_CALLER: &str = "prefrontal-core";

fn user_pin_fixture(used: f64, module: &str) -> (Arc<Harness>, impl Drop) {
    let (harness, pools) = fixture(Origin::Request, used);
    harness.edit_config(|config| {
        config["consumers"][USER_PIN_CONSUMER] = json!({"owner":module,"mode":"demand"});
    });
    (harness, pools)
}

fn user_pin_request() -> Value {
    let mut request = selection_request(Origin::Request, USER_PIN_CONSUMER);
    request["pin_origin"] = json!("user");
    request
}

fn assert_reserve_limited(reply: &(FrameType, Value)) {
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert_eq!(reply.1["code"], "pin_unavailable", "{reply:?}");
    assert_eq!(reply.1["detail"]["reason"], "quota_exhausted", "{reply:?}");
    assert_eq!(reply.1["detail"]["pin"], MODEL, "{reply:?}");
}

#[tokio::test]
async fn enrolled_user_pin_spends_below_keep_free() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = user_pin_fixture(85.0, USER_PIN_CALLER);
    let caller = harness.bind(reserved(USER_PIN_CALLER), None).await;
    let mut request = user_pin_request();
    request.as_object_mut().unwrap().remove("pin_origin");
    assert_reserve_limited(&caller.call("route.select", &request).await);
    let reply = caller.call("route.select", &user_pin_request()).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["route"]["provider"], "synthetic");
    assert_eq!(reply.1["result"]["route"]["model"], "model");
    assert!(reply.1.to_string().contains(EXEMPT), "{reply:?}");
    assert_eq!(
        reply.1["result"]["detail"]["score"]["objective"]["subscriptionPricing"]["availablePoints"],
        15.0,
        "{reply:?}"
    );
}

#[tokio::test]
async fn unenrolled_user_pin_keeps_the_reserve() {
    let _clock = crate::InjectedClock::set(NOW);
    for module in [
        "prefrontal-core-other",
        "other-prefrontal-core",
        "prefrontal-core.child",
    ] {
        let (harness, _pools) = user_pin_fixture(85.0, module);
        // A core-owned scope must not enroll its caller.
        let caller = harness
            .bind(reserved(module), Some("pin-origin-project"))
            .await;
        assert_reserve_limited(&caller.call("route.select", &user_pin_request()).await);
    }
}

#[tokio::test]
async fn empty_pin_origin_callers_disables_default_enrollment() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = user_pin_fixture(85.0, USER_PIN_CALLER);
    harness.edit_config(|config| config["pin_origin_callers"] = json!([]));
    let caller = harness.bind(reserved(USER_PIN_CALLER), None).await;
    assert_reserve_limited(&caller.call("route.select", &user_pin_request()).await);
}

#[tokio::test]
async fn configured_pin_origin_caller_is_honoured() {
    let _clock = crate::InjectedClock::set(NOW);
    let module = "custom-router-client";
    let (harness, _pools) = user_pin_fixture(85.0, module);
    harness
        .edit_config(|config| config["pin_origin_callers"] = json!([format!("reserved:{module}")]));
    let caller = harness.bind(reserved(module), None).await;
    let reply = caller.call("route.select", &user_pin_request()).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert!(reply.1.to_string().contains(EXEMPT), "{reply:?}");
}

#[tokio::test]
async fn reserve_limited_pin_without_known_reset_is_quota_exhausted() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = user_pin_fixture(85.0, USER_PIN_CALLER);
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "test-source",
        &serde_json::to_vec(&json!({"result":[{
            "provider":"test-source","apiProvider":"synthetic","account":"oauth-account",
            "source":"vault","fetchedAt":crate::account_selection::rfc3339(NOW as i64),
            "usage":{"primary":{"usedPercent":85.0,"windowMinutes":10080}}
        }]}))
        .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
    let caller = harness.bind(reserved(USER_PIN_CALLER), None).await;
    let mut request = user_pin_request();
    request.as_object_mut().unwrap().remove("pin_origin");
    let reply = caller.call("route.select", &request).await;
    assert_reserve_limited(&reply);
    assert!(reply.1["detail"].get("reset_at_ms").is_none(), "{reply:?}");
}

#[tokio::test]
async fn user_pin_cannot_spend_a_walled_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = user_pin_fixture(100.0, USER_PIN_CALLER);
    let caller = harness.bind(reserved(USER_PIN_CALLER), None).await;
    assert_reserve_limited(&caller.call("route.select", &user_pin_request()).await);
}

#[tokio::test]
async fn pin_origin_invalid_value_is_refused() {
    let _clock = crate::InjectedClock::set(NOW);
    for module in [USER_PIN_CALLER, "unenrolled-client"] {
        let (harness, _pools) = user_pin_fixture(10.0, module);
        let caller = harness.bind(reserved(module), None).await;
        for invalid in [
            json!("agent"),
            json!(""),
            json!(true),
            json!({}),
            json!([]),
            json!(7),
        ] {
            let mut request = user_pin_request();
            request["pin_origin"] = invalid;
            let reply = caller.call("route.select", &request).await;
            assert_eq!(reply.0, FrameType::Error, "{reply:?}");
            assert_eq!(reply.1["code"], "invalid_request", "{reply:?}");
            assert_eq!(reply.1["detail"]["field"], "pin_origin", "{reply:?}");
        }
    }
}

#[tokio::test]
async fn pin_origin_without_pin_is_refused() {
    let _clock = crate::InjectedClock::set(NOW);
    for module in [USER_PIN_CALLER, "unenrolled-client"] {
        let (harness, _pools) = user_pin_fixture(10.0, module);
        let caller = harness.bind(reserved(module), None).await;
        let mut request = user_pin_request();
        request.as_object_mut().unwrap().remove("pin");
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "invalid_request", "{reply:?}");
        assert_eq!(reply.1["detail"]["field"], "pin_origin", "{reply:?}");
    }
}

#[tokio::test]
async fn null_pin_origin_is_absent_and_failed_user_pin_does_not_exempt_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = user_pin_fixture(85.0, USER_PIN_CALLER);
    let caller = harness.bind(reserved(USER_PIN_CALLER), None).await;
    let mut request = user_pin_request();
    request["pin_origin"] = Value::Null;
    assert_reserve_limited(&caller.call("route.select", &request).await);
    let mut request = user_pin_request();
    request["pin"] = json!("absent/model");
    request["pin_fallback"] = json!("route");
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert!(
        reply.1.to_string().contains("subscription_reserve"),
        "{reply:?}"
    );
}

fn synthesizer_fixture(policy: Value) -> (Arc<Harness>, impl Drop) {
    let (harness, pools) = fixture(Origin::Request, 85.0);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal-core.athena"] = policy;
        config["consumers"]["prefrontal-core.athena"]["owner"] = json!(OWNER);
    });
    harness.attach_reasoning_catalog(striatum_store::ReasoningCatalog {
        models: [(
            MODEL.into(),
            striatum_store::PublishedEfforts::Published(vec!["medium".into(), "high".into()]),
        )]
        .into_iter()
        .collect(),
    });
    (harness, pools)
}

async fn assert_synthesizer_reask_below_margin(policy: Value) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = synthesizer_fixture(policy);
    let caller = harness.bind(reserved(OWNER), None).await;
    let mut request = selection_request(Origin::Request, SYNTHESIZER);
    request["pin"] = json!(SYNTHESIZER_PIN);
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(
        reply.1["result"]["route"]["provider"], "synthetic",
        "{reply:?}"
    );
    assert_eq!(reply.1["result"]["route"]["model"], "model", "{reply:?}");
    assert_eq!(reply.1["result"]["route"]["variant"], "medium", "{reply:?}");
    let id = reply.1["result"]["decision_id"].as_str().unwrap();
    let decision = harness.store().get_decision_by_id(id).unwrap().unwrap();
    assert!(decision
        .explanation_json
        .as_deref()
        .unwrap()
        .contains(EXEMPT));
    assert!(
        reply.1.to_string().contains("keep-free margin 0.0%"),
        "{reply:?}"
    );
    assert_eq!(
        reply.1["result"]["detail"]["score"]["objective"]["subscriptionPricing"]["availablePoints"],
        15.0,
        "{reply:?}"
    );
}

#[tokio::test]
async fn synthesizer_request_pin_matching_inherited_settings_pin_spends_below_keep_free() {
    assert_synthesizer_reask_below_margin(json!({
        "pin":{"model":MODEL,"variant":"medium"}
    }))
    .await;
}

#[tokio::test]
async fn synthesizer_request_pin_matching_inherited_settings_ladder_spends_below_keep_free() {
    assert_synthesizer_reask_below_margin(json!({
        "mode":"ladder","ladder":["absent/model", SYNTHESIZER_PIN]
    }))
    .await;
}

#[tokio::test]
async fn synthesizer_request_pin_different_from_settings_keeps_the_reserve() {
    let _clock = crate::InjectedClock::set(NOW);
    for settings_route in [
        "other/model:medium",
        "synthetic/other:medium",
        "synthetic/model:high",
        MODEL,
    ] {
        for policy in [
            json!({"pin":{"model":settings_route}}),
            json!({"mode":"ladder","ladder":[settings_route]}),
        ] {
            let (harness, _pools) = synthesizer_fixture(policy.clone());
            let caller = harness.bind(reserved(OWNER), None).await;
            let mut request = selection_request(Origin::Request, SYNTHESIZER);
            request["pin"] = json!(SYNTHESIZER_PIN);
            let reply = caller.call("route.select", &request).await;
            assert_eq!(reply.0, FrameType::Error, "{policy}: {reply:?}");
            assert!(
                reply.1.to_string().contains("subscription_reserve"),
                "{policy}: {reply:?}"
            );
        }
    }
}

#[tokio::test]
async fn request_pin_matching_module_declared_ladder_keeps_the_reserve() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _pools) = fixture(Origin::Request, 85.0);
    let consumer = format!("{OWNER}.module");
    let caller = harness.bind(reserved(OWNER), None).await;
    super::super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        super::super::consumers::replace_catalog(&json!([{
            "module_id":OWNER,"nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":consumer,"ladder":[MODEL]}
            ]}}
        }]));
        let request = selection_request(Origin::Request, &consumer);
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert!(
            reply.1.to_string().contains("subscription_reserve"),
            "{reply:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn exempt_pins_still_obey_settings_exclusions() {
    let _clock = crate::InjectedClock::set(NOW);
    for rule in ["not_for", "only_for"] {
        let (harness, _pools) = fixture(Origin::Request, 85.0);
        harness.edit_config(|config| {
            config["models"][MODEL][rule] = if rule == "not_for" {
                json!([CONSUMER])
            } else {
                json!(["somebody-else"])
            };
        });
        let caller = harness.bind(reserved(OWNER), None).await;
        let reply = caller
            .call(
                "route.select",
                &selection_request(Origin::Request, CONSUMER),
            )
            .await;
        assert_eq!(reply.0, FrameType::Error, "{rule}: {reply:?}");
    }
}
