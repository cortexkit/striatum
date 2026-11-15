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
