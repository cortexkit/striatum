use super::{prepare_selects, read, GROUP};
use crate::routing_v1::{describe, dispatch_tests::configured};
use cortexkit_role_routing::capabilities::DECISION_HARNESS;
use serde_json::{json, Value};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

fn slot(limits: Value) -> Value {
    let mut value = super::slot(limits);
    value["runner"]
        .as_object_mut()
        .unwrap()
        .remove("standing_limits");
    value
}

fn install(harness: &crate::routing_v1::conformance_tests::Harness, module: &str, value: &Value) {
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(module.into(), read(value));
}

fn request(requirements: Value) -> Value {
    json!({"consumer":"example-consumer.harness_choice_test", "requirements":requirements})
}

fn setup(harness: &crate::routing_v1::conformance_tests::Harness, runners: Value) {
    for mut fact in harness.store().get_facts().unwrap() {
        fact.max_output_tokens = Some(32000);
        harness
            .store()
            .upsert_fact(&fact, striatum_core::FactSource::Override)
            .unwrap();
    }
    prepare_selects(harness);
    harness.edit_config(|config| {
        config["runners"] = runners;
        config["consumers"]["example-consumer.harness_choice_test"] = json!({});
    });
}

fn selected(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert!(reply.1["result"]["runner"].is_object(), "{}", reply.1);
    reply.1["result"].clone()
}

fn refused(reply: (FrameType, Value), reason: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], reason, "{}", reply.1);
}

fn legacy_unbound_slot(limits: Value) -> Value {
    let mut value = slot(limits);
    value["runner"]["harnesses"] = json!(["primary", "secondary"]);
    value["runner"]
        .as_object_mut()
        .unwrap()
        .remove("default_harness");
    value
}

#[tokio::test]
async fn decision_harness_binds_first_admitting_legacy_harness_without_changing_other_evidence() {
    describe::with_candidate_capabilities(&[DECISION_HARNESS], async {
        let _clock = crate::InjectedClock::set(1_792_000_000_000);
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"legacy"}]));
        let mut value = legacy_unbound_slot(json!({
            "primary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]},
            "secondary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]}
        }));
        install(&harness, "legacy", &value);
        let params = request(json!({"iq":0}));
        let baseline = describe::without_capability(DECISION_HARNESS, async {
            selected(route.call("route.select", &params).await)
        })
        .await;
        assert!(baseline["runner"].get("harness").is_none(), "{baseline}");
        let answer = selected(route.call("route.select", &params).await);
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
        let baseline_id = baseline["decision_id"].as_str().unwrap();
        let answer_id = answer["decision_id"].as_str().unwrap();
        let mut expected = baseline.clone();
        expected["decision_id"] = answer["decision_id"].clone();
        expected["runner"]["harness"] = json!("secondary");
        assert_eq!(answer, expected);
        let retained: Value = serde_json::from_str(
            &harness
                .store()
                .v1_selection_json(answer_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retained, expected);
        let old_retained: Value = serde_json::from_str(
            &harness
                .store()
                .v1_selection_json(baseline_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(old_retained, baseline);
        let old_row = harness
            .store()
            .get_decision_by_id(baseline_id)
            .unwrap()
            .unwrap();
        let mut new_row = harness
            .store()
            .get_decision_by_id(answer_id)
            .unwrap()
            .unwrap();
        assert!(old_row.catalog_fingerprint.is_some());
        new_row.id = old_row.id.clone();
        // The stored decision row uses its own wall clock for created_at, not the request clock.
        new_row.created_at = old_row.created_at;
        assert_eq!(new_row, old_row);
        let old_binding = harness.store().v1_decision(baseline_id).unwrap().unwrap();
        let mut new_binding = harness.store().v1_decision(answer_id).unwrap().unwrap();
        new_binding.decision_id = old_binding.decision_id.clone();
        assert_eq!(new_binding, old_binding);

        value["runner"]["harness_limits"]["primary"]["pairs"] =
            value["runner"]["harness_limits"]["secondary"]["pairs"].clone();
        install(&harness, "legacy", &value);
        let first = selected(route.call("route.select", &params).await);
        assert_eq!(first["runner"]["harness"], "primary", "{first}");
        assert_eq!(first["route"], baseline["route"]);
    })
    .await;
}

#[tokio::test]
async fn decision_harness_legacy_runner_loses_route_when_no_harness_admits_it() {
    describe::with_candidate_capabilities(&[DECISION_HARNESS], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"legacy"},{"module":"fallback"}]));
        install(
            &harness,
            "legacy",
            &legacy_unbound_slot(json!({
                "primary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]},
                "secondary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]}
            })),
        );
        install(&harness, "fallback", &slot(json!({})));
        let params = request(json!({"iq":0}));
        let baseline = describe::without_capability(DECISION_HARNESS, async {
            selected(route.call("route.select", &params).await)
        })
        .await;
        assert_eq!(baseline["runner"]["module"], "legacy", "{baseline}");
        let answer = selected(route.call("route.select", &params).await);
        assert_eq!(answer["runner"]["module"], "fallback", "{answer}");
        assert_eq!(answer["runner"]["harness"], "primary", "{answer}");
        assert_eq!(answer["route"], baseline["route"]);
        let mut only_legacy = params;
        only_legacy["runners"] = json!(["legacy"]);
        only_legacy["available_routes"] = json!([format!(
            "{}/{}",
            baseline["route"]["provider"].as_str().unwrap(),
            baseline["route"]["model"].as_str().unwrap()
        )]);
        let reply = route.call("route.select", &only_legacy).await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    })
    .await;
}

#[tokio::test]
async fn decision_harness_advertisement_preserves_harnessless_runner_wire_shape() {
    describe::with_candidate_capabilities(&[DECISION_HARNESS], async {
        let _clock = crate::InjectedClock::set(1_792_000_000_000);
        for declared in [false, true] {
            let (harness, route) = configured().await;
            setup(&harness, json!([{"module":"harnessless"}]));
            if declared {
                let mut value = slot(json!({}));
                let runner = value["runner"].as_object_mut().unwrap();
                runner.remove("harnesses");
                runner.remove("default_harness");
                install(&harness, "harnessless", &value);
            }
            let params = request(json!({"iq":0}));
            let mut named = params.clone();
            named["harness"] = json!("harnessless");
            let baseline = describe::without_capability(DECISION_HARNESS, async {
                let described = route.call("role.describe", &json!({})).await;
                assert!(!described.1["result"]["capabilities"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("decision_harness")));
                refused(
                    route.call("route.select", &named).await,
                    "harness_unavailable",
                );
                selected(route.call("route.select", &params).await)
            })
            .await;
            let described = route.call("role.describe", &json!({})).await;
            assert!(described.1["result"]["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!("decision_harness")));
            let answer = selected(route.call("route.select", &params).await);
            assert_eq!(answer["runner"]["module"], "harnessless");
            assert!(answer["runner"].get("harness").is_none(), "{answer}");
            assert!(baseline["runner"].get("harness").is_none(), "{baseline}");
            let mut expected = baseline;
            expected["decision_id"] = answer["decision_id"].clone();
            assert_eq!(answer, expected);
            refused(
                route.call("route.select", &named).await,
                "harness_unavailable",
            );
        }
    })
    .await;
}

#[tokio::test]
async fn decision_harness_panel_seat_binds_the_same_harness_as_select() {
    describe::with_candidate_capabilities(&[DECISION_HARNESS], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"legacy"}]));
        install(
            &harness,
            "legacy",
            &legacy_unbound_slot(json!({
                "primary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]},
                "secondary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]}
            })),
        );
        harness.edit_config(|config| {
            config["panels"]["audit"] = json!({"topK":1,"by":"iq","extra":0,"floor":0});
        });
        let select = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        let key = format!(
            "{}/{}",
            select["route"]["provider"].as_str().unwrap(),
            select["route"]["model"].as_str().unwrap()
        );
        let params = json!({
            "class":"audit", "consumer":"example-consumer.harness_choice_test",
            "available_routes":[key], "seed":"fixed"
        });
        let before = describe::without_capability(
            DECISION_HARNESS,
            route.call("route.select_panel", &params),
        )
        .await;
        assert_eq!(before.0, FrameType::Response, "{}", before.1);
        assert!(before.1["result"]["seats"][0]["answer"]["runner"]
            .get("harness")
            .is_none());
        let panel = route.call("route.select_panel", &params).await;
        assert_eq!(panel.0, FrameType::Response, "{}", panel.1);
        assert_eq!(panel.1["result"]["seats"].as_array().unwrap().len(), 1);
        let answer = &panel.1["result"]["seats"][0]["answer"];
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
        assert_eq!(answer["runner"], select["runner"]);
        let request = cortexkit_role_routing::panel::parse_select_panel_request_serving(
            &params,
            &describe::capabilities(),
        )
        .unwrap();
        cortexkit_role_routing::panel::check_panel_answer(
            &request,
            &serde_json::from_value(panel.1["result"].clone()).unwrap(),
        )
        .unwrap();
        let retained: Value = serde_json::from_str(
            &harness
                .store()
                .v1_selection_json(answer["decision_id"].as_str().unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(&retained, answer);
    })
    .await;
}

#[tokio::test]
async fn harness_refusals_caller_runners_follow_consumer_placements() {
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"legacy"}]));
    install(&harness, "legacy", &slot(json!({})));
    let mut tagged = request(json!({"iq":0,"tool_capabilities":["test:absent/v1"]}));
    tagged["harness"] = json!("primary");
    refused(
        route.call("route.select", &tagged).await,
        "missing_capability",
    );
    let mut params = request(json!({"iq":0}));
    params["runners"] = json!(["missing"]);
    harness.edit_config(|config| {
        config["consumers"]["example-consumer.harness_choice_test"]["runner"] =
            json!("not-configured");
    });
    for callers in [None, Some(json!(["missing"]))] {
        let mut missing_placement = request(json!({"iq":0}));
        if let Some(callers) = callers {
            missing_placement["runners"] = callers;
        }
        refused(
            route.call("route.select", &missing_placement).await,
            "no_available_model",
        );
        missing_placement["harness"] = json!("primary");
        refused(
            route.call("route.select", &missing_placement).await,
            "harness_unavailable",
        );
    }
    harness.edit_config(|config| {
        config["runners"] = json!([]);
        config["consumers"]["example-consumer.harness_choice_test"] = json!({});
    });
    refused(
        route.call("route.select", &params).await,
        "no_available_model",
    );
    params["harness"] = json!("primary");
    refused(
        route.call("route.select", &params).await,
        "harness_unavailable",
    );
    params.as_object_mut().unwrap().remove("harness");
    harness.edit_config(|config| config["runners"] = json!([{"module":"legacy"}]));
    for callers in [json!(["missing"]), json!([])] {
        params["runners"] = callers;
        let reply = route.call("route.select", &params).await;
        assert!(
            reply.1["detail"].get("reset_at_ms").is_none(),
            "{}",
            reply.1
        );
        refused(reply, "harness_unavailable");
    }
}

fn exclude_on_every_harness(exclude: Value) -> Value {
    slot(
        json!({"primary":{"exclude":exclude}, "secondary":{"exclude":exclude}, "inherited":{"exclude":exclude}}),
    )
}

#[tokio::test]
async fn harness_refusals_route_exclusions_preserve_cooldowns_and_policy() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({})));
        let params = request(json!({"iq":0}));
        let first = selected(route.call("route.select", &params).await);
        let key = format!(
            "{}/{}",
            first["route"]["provider"].as_str().unwrap(),
            first["route"]["model"].as_str().unwrap()
        );
        install(
            &harness,
            "declaring",
            &exclude_on_every_harness(json!([key])),
        );
        let next = selected(route.call("route.select", &params).await);
        let other = format!(
            "{}/{}",
            next["route"]["provider"].as_str().unwrap(),
            next["route"]["model"].as_str().unwrap()
        );
        let mut only = params.clone();
        only["available_routes"] = json!([key, other]);
        let reset = crate::now_ms() as u64 + 60_000;
        let cooldown = route
            .call(
                "route.record_cooldown",
                &json!({"routeKey":other,"capacityKey":other,"cooldownUntil":reset}),
            )
            .await;
        assert_eq!(cooldown.0, FrameType::Response, "{}", cooldown.1);
        let timed = route.call("route.select", &only).await;
        assert_eq!(timed.1["detail"]["reset_at_ms"], reset, "{}", timed.1);
        refused(timed, "quota_or_cooldown");
        install(
            &harness,
            "declaring",
            &exclude_on_every_harness(json!([key, other])),
        );
        let excluded = route.call("route.select", &only).await;
        assert!(
            excluded.1["detail"].get("reset_at_ms").is_none(),
            "{}",
            excluded.1
        );
        refused(excluded, "harness_unavailable");
        only["exclude"] = json!([key, other]);
        refused(
            route.call("route.select", &only).await,
            "no_available_model",
        );
    })
    .await;
}

#[tokio::test]
async fn harness_refusals_unhostable_pins_keep_pin_reason_and_fallback() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({})));
        let first = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        let key = format!(
            "{}/{}",
            first["route"]["provider"].as_str().unwrap(),
            first["route"]["model"].as_str().unwrap()
        );
        harness
            .edit_config(|config| config["consumers"]["harness_pin"]["pin"] = json!({"model":key}));
        for pin in [json!({"pin":key}), json!({"pin_slot":"harness_pin"})] {
            install(
                &harness,
                "declaring",
                &exclude_on_every_harness(json!([key])),
            );
            let mut params = request(json!({"iq":0}));
            params
                .as_object_mut()
                .unwrap()
                .extend(pin.as_object().unwrap().clone());
            let unavailable = route.call("route.select", &params).await;
            assert_eq!(
                unavailable.1["code"], "pin_unavailable",
                "{}",
                unavailable.1
            );
            assert_eq!(
                unavailable.1["detail"]["reason"], "not_available",
                "{}",
                unavailable.1
            );
            params["pin_fallback"] = json!("route");
            let fallback = selected(route.call("route.select", &params).await);
            assert_eq!(fallback["pin_outcome"], "fell_back", "{fallback}");
            assert_eq!(
                fallback["detail"]["pin_reason"], "not_available",
                "{fallback}"
            );
            assert_ne!(fallback["route"], first["route"], "{fallback}");
            let providers: Vec<_> = harness
                .store()
                .get_facts()
                .unwrap()
                .into_iter()
                .map(|fact| fact.provider_id)
                .collect();
            install(
                &harness,
                "declaring",
                &exclude_on_every_harness(json!(providers)),
            );
            params.as_object_mut().unwrap().remove("pin_fallback");
            let all_excluded = route.call("route.select", &params).await;
            assert_eq!(
                all_excluded.1["code"], "pin_unavailable",
                "{}",
                all_excluded.1
            );
            assert_eq!(
                all_excluded.1["detail"]["reason"], "not_available",
                "{}",
                all_excluded.1
            );
        }
    })
    .await;
}

#[tokio::test]
async fn harness_choice_capability_needs_are_per_runner_and_do_not_require_fork_answer() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(
            &harness,
            json!([{"module":"declaring"}, {"module":"legacy"}]),
        );
        install(
            &harness,
            "declaring",
            &slot(json!({"primary":{"capabilities":["one_off"]}})),
        );
        install(&harness, "legacy", &slot(json!({})));
        let limited = selected(
            route
                .call(
                    "route.select",
                    &request(json!({"iq":0,"max_output_tokens":1})),
                )
                .await,
        );
        assert_eq!(limited["runner"]["module"], "legacy", "{limited}");
        let one_off = selected(
            route
                .call("route.select", &request(json!({"iq":0,"tools":"none"})))
                .await,
        );
        assert_eq!(one_off["runner"]["module"], "declaring", "{one_off}");
        for limits in [
            json!({"starts_sessions":true}),
            json!({"capabilities":[]}),
            json!({"capabilities":["future_token"]}),
        ] {
            install(&harness, "declaring", &slot(json!({"primary":limits})));
            for requirement in [
                json!({"iq":0,"tools":"none"}),
                json!({"iq":0,"max_output_tokens":1}),
            ] {
                let answer = selected(route.call("route.select", &request(requirement)).await);
                assert_eq!(answer["runner"]["module"], "legacy", "{answer}");
            }
            for requirement in [json!({"iq":0}), json!({"iq":0,"tools":"required"})] {
                let answer = selected(route.call("route.select", &request(requirement)).await);
                assert_eq!(answer["runner"]["module"], "declaring", "{answer}");
            }
        }
    })
    .await;
}

#[tokio::test]
async fn harness_choice_fact_step_precedes_tool_tags_and_distinguishes_route_filtering() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({"primary":{"starts_sessions":false}, "secondary":{"starts_sessions":false}, "inherited":{"starts_sessions":false}})));
        refused(route.call("route.select", &request(json!({"iq":0,"tool_capabilities":["test:absent/v1"]}))).await, "harness_unavailable");
        install(&harness, "declaring", &slot(json!({"primary":{"capabilities":[]}})));
        for requirement in [json!({"iq":0,"tools":"none"}), json!({"iq":0,"max_output_tokens":1})] {
            refused(route.call("route.select", &request(requirement)).await, "harness_unavailable");
        }
        let mut stopped = slot(json!({"primary":{"starts_sessions":true}}));
        stopped["runner"]["starts_sessions"] = json!(false);
        install(&harness, "declaring", &stopped);
        refused(route.call("route.select", &request(json!({"iq":0}))).await, "harness_unavailable");
        harness.edit_config(|config| config["runners"] = json!([{"module":"declaring"}, {"module":"legacy"}]));
        let mut legacy = slot(json!({}));
        legacy["runner"]["tool_capabilities"] = json!([]);
        install(&harness, "legacy", &legacy);
        refused(route.call("route.select", &request(json!({"iq":0,"max_output_tokens":1,"tool_capabilities":["test:absent/v1"]}))).await, "missing_capability");
    }).await;
}

#[tokio::test]
async fn harness_choice_candidate_facts_do_not_leak_into_held_legacy_picks_or_reparse() {
    // Production serves the group; withholding it here pins what a router that
    // doesn't serve it answers for the same declaration.
    describe::without_capability(GROUP, async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({})));
        let params = request(json!({"iq":0}));
        let baseline = selected(route.call("route.select", &params).await);
        let fingerprint = harness
            .store()
            .get_decision_by_id(baseline["decision_id"].as_str().unwrap())
            .unwrap()
            .unwrap()
            .catalog_fingerprint;
        let bad = request(json!({"iq":0,"tool_capabilities":["test:absent/v1"]}));
        let baseline_refusal = route.call("route.select", &bad).await;
        let value = slot(json!({"primary":{"starts_sessions":false}}));
        describe::with_candidate_capabilities(&[GROUP], async {
            install(&harness, "declaring", &value);
            let parsed = super::PARSES.load(std::sync::atomic::Ordering::Relaxed);
            for _ in 0..3 {
                let answer = selected(route.call("route.select", &params).await);
                assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
            }
            assert_eq!(
                super::PARSES.load(std::sync::atomic::Ordering::Relaxed),
                parsed
            );
        })
        .await;
        assert!(!describe::capabilities().contains(&GROUP));
        install(&harness, "declaring", &value);
        let parsed = super::PARSES.load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..3 {
            let answer = selected(route.call("route.select", &params).await);
            assert_eq!(answer["route"], baseline["route"]);
            assert_eq!(answer["runner"], baseline["runner"]);
            assert_eq!(
                harness
                    .store()
                    .get_decision_by_id(answer["decision_id"].as_str().unwrap())
                    .unwrap()
                    .unwrap()
                    .catalog_fingerprint,
                fingerprint
            );
            assert_eq!(route.call("route.select", &bad).await, baseline_refusal);
        }
        assert_eq!(
            super::PARSES.load(std::sync::atomic::Ordering::Relaxed),
            parsed
        );
    })
    .await;
}

#[tokio::test]
async fn harness_choice_tries_default_then_declared_order_for_task_and_route() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        let providers: Vec<_> = harness.store().get_facts().unwrap().into_iter().map(|fact| fact.provider_id).collect();
        for primary in [json!({"starts_sessions":false,"capabilities":["output_cap"]}), json!({"exclude":providers,"capabilities":["output_cap"]}), json!({"capabilities":["one_off"]})] {
            install(&harness, "declaring", &slot(json!({"primary":primary,"secondary":{"capabilities":["output_cap"]},"inherited":{"capabilities":["output_cap"]}})));
            let answer = selected(route.call("route.select", &request(json!({"iq":0,"max_output_tokens":1}))).await);
            assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
        }
        let mut value = slot(json!({"primary":{"capabilities":["output_cap"]},"secondary":{"capabilities":["output_cap"]}}));
        value["runner"]["default_harness"] = json!("secondary");
        install(&harness, "declaring", &value);
        let answer = selected(route.call("route.select", &request(json!({"iq":0,"max_output_tokens":1}))).await);
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
    }).await;
}

#[tokio::test]
async fn harness_choice_request_is_the_only_candidate_and_unhosted_runners_are_removed() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(
            &harness,
            json!([{"module":"declaring"}, {"module":"legacy"}]),
        );
        install(
            &harness,
            "declaring",
            &slot(json!({"primary":{"starts_sessions":false}})),
        );
        let mut legacy = slot(json!({}));
        legacy["runner"]["harnesses"] = json!(["secondary"]);
        legacy["runner"]["default_harness"] = json!("secondary");
        install(&harness, "legacy", &legacy);
        let mut params = request(json!({"iq":0}));
        params["harness"] = json!("primary");
        refused(
            route.call("route.select", &params).await,
            "harness_unavailable",
        );
        params["harness"] = json!("secondary");
        let answer = selected(route.call("route.select", &params).await);
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
        params["harness"] = json!("absent");
        refused(
            route.call("route.select", &params).await,
            "harness_unavailable",
        );
    })
    .await;
}

#[tokio::test]
async fn harness_choice_exclusions_remove_routes_before_ranking_not_runners() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({})));
        let first = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        let excluded = format!("{}/{}", first["route"]["provider"].as_str().unwrap(), first["route"]["model"].as_str().unwrap());
        install(&harness, "declaring", &slot(json!({"primary":{"exclude":[excluded]},"secondary":{"exclude":[excluded]},"inherited":{"exclude":[excluded]}})));
        let next = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        assert_ne!(next["route"], first["route"], "{next}");
        assert_eq!(next["runner"]["module"], "declaring", "{next}");
        assert_eq!(next["runner"]["harness"], "primary", "{next}");
    }).await;
}

#[tokio::test]
async fn harness_choice_own_limits_allow_fallback_only_after_a_fact_is_declared() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        let mut value = slot(json!({"primary":{"explicit_providers":["unreachable"]}}));
        let mut providers: std::collections::BTreeSet<_> = harness
            .store()
            .get_facts()
            .unwrap()
            .into_iter()
            .map(|fact| fact.provider_id)
            .collect();
        providers.insert("unreachable".into());
        value["runner"]["standing_limits"] = json!({"explicit_providers":providers});
        install(&harness, "declaring", &value);
        let legacy = route.call("route.select", &request(json!({"iq":0}))).await;
        assert_eq!(legacy.0, FrameType::Error, "{}", legacy.1);
        value["runner"]["harness_limits"]["primary"]["exclude"] = json!([]);
        install(&harness, "declaring", &value);
        let answer = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
        let config = harness.operator_config();
        let credentials = harness.credentials();
        let credentials = credentials.lock().unwrap();
        let facts = harness.store().get_facts().unwrap();
        let registry = super::super::Registry::build_for_selection(
            config.as_ref(),
            &credentials,
            &facts,
            crate::now_ms(),
            Some("primary"),
        );
        let key = format!(
            "{}/{}",
            answer["route"]["provider"].as_str().unwrap(),
            answer["route"]["model"].as_str().unwrap()
        );
        assert!(!registry.can_run("declaring", &key, &key));
    })
    .await;
}

#[tokio::test]
async fn harness_choice_pair_limits_are_not_cross_combined_between_candidates() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}]));
        install(&harness, "declaring", &slot(json!({})));
        let baseline = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        let key = format!("{}/{}", baseline["route"]["provider"].as_str().unwrap(), baseline["route"]["model"].as_str().unwrap());
        let value = slot(json!({
            "primary":{"exclude":[],"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]},
            "secondary":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]},
            "inherited":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]}
        }));
        install(&harness, "declaring", &value);
        {
            let config = harness.operator_config();
            let shared = harness.credentials();
            let credentials = shared.lock().unwrap();
            let facts = harness.store().get_facts().unwrap();
            for (name, admitted) in [("primary", false), ("secondary", true)] {
                let registry = super::super::Registry::build_for_selection(config.as_ref(), &credentials, &facts, crate::now_ms(), Some(name));
                assert_eq!(registry.can_run("declaring", &key, &key), admitted, "{name}: {key}");
            }
        }
        let mut params = request(json!({"iq":0}));
        params["available_routes"] = json!([key]);
        let answer = selected(route.call("route.select", &params).await);
        assert_eq!(answer["runner"]["harness"], "secondary", "{answer}");
    }).await;
}

#[tokio::test]
async fn harness_choice_output_need_uses_declared_and_operator_defaults() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"declaring"}, {"module":"legacy"}]));
        install(&harness, "declaring", &slot(json!({"primary":{"capabilities":["one_off"]}})));
        install(&harness, "legacy", &slot(json!({})));
        let owner = cortexkit_role_routing_conformance::fixture::CONSUMER.trim_start_matches("reserved:");
        crate::routing_v1::consumers::replace_catalog(&json!([{
            "module_id":owner,"nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"example-consumer.harness_choice_test","max_output_tokens":1}]}}
        }]));
        let declared = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        assert_eq!(declared["runner"]["module"], "legacy", "{declared}");
        crate::routing_v1::consumers::replace_catalog(&json!([]));
        harness.edit_config(|config| config["consumers"]["example-consumer.harness_choice_test"]["max_output_tokens"] = json!(1));
        let operator = selected(route.call("route.select", &request(json!({"iq":0}))).await);
        assert_eq!(operator["runner"]["module"], "legacy", "{operator}");
    }).await;
}

#[tokio::test]
async fn broca_dream_settings_ladder_names_harness_from_verified_declaration() {
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"broca"}]));
    prepare_broca_endpoints(&harness);
    let fixture: Value =
        serde_json::from_slice(include_bytes!("broca-runner-slot.after.json")).unwrap();
    let ladder = ["example-provider/model-a", "example-provider/model-b"];
    let owner =
        cortexkit_role_routing_conformance::fixture::CONSUMER.trim_start_matches("reserved:");
    harness.edit_config(|config| {
        config["consumers"]["magic-context.dream"] =
            json!({"owner":owner, "mode":"ladder", "ladder":ladder});
    });

    // The old declaration has the same sole harness but no task capabilities.
    // Both shapes must name it, including when the first ladder step is excluded.
    let mut legacy = fixture.clone();
    legacy["runner"]
        .as_object_mut()
        .unwrap()
        .remove("harness_limits");
    for declaration in [&fixture, &legacy] {
        install(&harness, "broca", declaration);
        for consumer in [
            "magic-context.dream.compress-cues",
            "magic-context.dream.verify",
        ] {
            for tools in [None, Some("none"), Some("required")] {
                for requested in [None, Some("broca")] {
                    for exclude_first in [false, true] {
                        for iq in [30, 60] {
                            let mut params = json!({"consumer":consumer, "requirements":{"iq":iq}});
                            if let Some(tools) = tools {
                                params["requirements"]["tools"] = json!(tools);
                            }
                            if let Some(requested) = requested {
                                params["harness"] = json!(requested);
                            }
                            if exclude_first {
                                params["exclude"] = json!([ladder[0]]);
                            }
                            let answer = selected(route.call("route.select", &params).await);
                            assert_eq!(answer["runner"]["module"], "broca", "{params}: {answer}");
                            assert_eq!(answer["runner"]["harness"], "broca", "{params}: {answer}");
                            assert!(answer.get("pin_outcome").is_none(), "{answer}");
                        }
                    }
                }
            }
        }
    }

    install(&harness, "broca", &fixture);
    let mut capped = json!({"consumer":"magic-context.dream.compress-cues", "requirements":{"iq":0,"max_output_tokens":1}});
    refused(
        route.call("route.select", &capped).await,
        "harness_unavailable",
    );
    capped["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("max_output_tokens");
    capped["harness"] = json!("not-hosted");
    refused(
        route.call("route.select", &capped).await,
        "harness_unavailable",
    );
}

fn prepare_broca_endpoints(harness: &crate::routing_v1::conformance_tests::Harness) {
    let shared = harness.credentials();
    let mut credentials = shared.lock().unwrap();
    // Broca requires endpoint facts as well as wire-family names. The shared
    // conformance catalog supplies only families until a test marks endpoints.
    credentials.wire_api_models = credentials.wire_apis.keys().cloned().collect();
    let catalog = credentials
        .billing_catalog
        .clone()
        .with_wire_api_models(&credentials.wire_api_models);
    credentials.publish_billing_catalog(catalog);
    credentials.set_watch_confirmed(Some("conformance-credentials"));
}

#[tokio::test]
async fn broca_dream_settings_ladder_catalog_gap_retains_harness() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"broca"}]));
    prepare_broca_endpoints(&harness);
    harness.edit_config(|config| {
        config["consumers"]["magic-context.dream"] = json!({
            "owner":cortexkit_role_routing_conformance::fixture::CONSUMER.trim_start_matches("reserved:"),
            "mode":"ladder", "ladder":["example-provider/model-a"]
        });
    });
    let fixture: Value =
        serde_json::from_slice(include_bytes!("broca-runner-slot.after.json")).unwrap();
    let modules = json!([{"module_id":"broca", "nonce_verified":true, "first_party":true,
        "role_declarations":{"routing/v1":fixture}}]);
    let policies = harness.operator_config().v1_runners();
    let now = crate::now_ms() as i64;
    let mut cache = super::super::DescribeCache::default();
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        cache.catalog(&modules, &policies, &mut credentials, now);
        cache.reply(
            "broca",
            Some(&json!({"capabilities":["service_tier","routed_accounts"],
            "service_tiers":{"anthropic.messages":{"levels":["fast"],"served":"reported"}},
            "routed_accounts":["apikey"]})),
            &mut credentials,
            now,
        );
    }
    let params = json!({"consumer":"magic-context.dream.compress-cues", "requirements":{"iq":30}});
    let declared = selected(route.call("route.select", &params).await);
    assert_eq!(declared["runner"]["harness"], "broca", "{declared}");

    let _gap_clock = crate::InjectedClock::set((now + 120_000) as u64);
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        cache.catalog(&json!([]), &policies, &mut credentials, now + 120_000);
        assert!(credentials.runner_service_tiers.contains_key("broca"));
        assert!(credentials.runner_routed_accounts.contains_key("broca"));
        assert_eq!(credentials.runner_describe_observed["broca"], now);
        assert!(!credentials.first_party_runners.contains_key("broca"));
    }
    let retained = selected(route.call("route.select", &params).await);
    assert_eq!(retained["runner"]["module"], "broca", "{retained}");
    assert_eq!(retained["runner"]["harness"], "broca", "{retained}");
    assert_eq!(retained["route"], declared["route"]);

    let expired_at = now + super::super::describe_cache::LAST_GOOD_MS;
    let _expired_clock = crate::InjectedClock::set(expired_at as u64);
    // Eligibility must notice expiry even before the next cache refresh.
    assert!(harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .contains_key("broca"));
    refused(
        route.call("route.select", &params).await,
        "no_available_model",
    );
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        cache.missed_all(&mut credentials, expired_at);
        assert!(!credentials.runner_declarations.contains_key("broca"));
        assert!(!credentials.runner_service_tiers.contains_key("broca"));
        assert!(!credentials.runner_routed_accounts.contains_key("broca"));
        assert!(credentials.runner_declarations_seen.contains("broca"));
        cache.catalog(&modules, &policies, &mut credentials, expired_at);
        cache.reply(
            "broca",
            Some(&json!({"capabilities":[]})),
            &mut credentials,
            expired_at,
        );
    }
    let reappeared = selected(route.call("route.select", &params).await);
    assert_eq!(reappeared["runner"]["harness"], "broca", "{reappeared}");
}

#[tokio::test]
async fn broca_dream_settings_ladder_never_declared_runner_keeps_legacy_settings_only() {
    let (harness, route) = configured().await;
    setup(&harness, json!([]));
    harness.edit_config(|config| {
        config["consumers"]["magic-context.dream"] = json!({
            "owner":cortexkit_role_routing_conformance::fixture::CONSUMER.trim_start_matches("reserved:"),
            "mode":"ladder", "ladder":["example-provider/model-a"]
        });
    });
    let mut cache = super::super::DescribeCache::default();
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        cache.catalog(
            &json!([{"module_id":"broca", "first_party":true}]),
            &[],
            &mut credentials,
            crate::now_ms() as i64,
        );
        assert!(!credentials.runner_declarations.contains_key("broca"));
        assert!(!credentials.first_party_runners.contains_key("broca"));
    }
    refused(
        route
            .call(
                "route.select",
                &json!({"consumer":"magic-context.dream.compress-cues", "requirements":{"iq":30}}),
            )
            .await,
        "no_available_model",
    );
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca"}]));
    let legacy = selected(
        route
            .call(
                "route.select",
                &json!({"consumer":"magic-context.dream.compress-cues", "requirements":{"iq":30}}),
            )
            .await,
    );
    assert_eq!(legacy["runner"]["module"], "broca", "{legacy}");
    assert!(legacy["runner"].get("harness").is_none(), "{legacy}");
}

#[tokio::test]
async fn harness_choice_broca_fixture_never_hosts_output_limited_tasks() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"broca"}, {"module":"legacy"}]));
        let fixture =
            serde_json::from_slice(include_bytes!("broca-runner-slot.after.json")).unwrap();
        install(&harness, "broca", &fixture);
        install(&harness, "legacy", &slot(json!({})));
        let answer = selected(
            route
                .call(
                    "route.select",
                    &request(json!({"iq":0,"max_output_tokens":1})),
                )
                .await,
        );
        assert_eq!(answer["runner"]["module"], "legacy", "{answer}");
        harness.edit_config(|config| config["runners"] = json!([{"module":"broca"}]));
        refused(
            route
                .call(
                    "route.select",
                    &request(json!({"iq":0,"max_output_tokens":1})),
                )
                .await,
            "harness_unavailable",
        );
    })
    .await;
}

#[tokio::test]
async fn harness_choice_what_if_exclusion_filters_only_the_disposable_overlay() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"broca"}]));
        install(&harness, "broca", &slot(json!({})));
        let params = request(json!({"iq":0}));
        let live = selected(route.call("route.select", &params).await);
        let key = format!("{}/{}", live["route"]["provider"].as_str().unwrap(), live["route"]["model"].as_str().unwrap());
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let overlay = slot(json!({"primary":{"exclude":[key]},"secondary":{"exclude":[key]},"inherited":{"exclude":[key]}}));
        let reply = operator.call("route.what_if", &json!({"consumer":"example-consumer.harness_choice_test","iqs":[0],"runner":"broca","runner_declarations":{"broca":overlay}})).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let pick = &reply.1["result"]["results"][0]["pick"]["route"];
        assert!(pick.is_string(), "{}", reply.1);
        assert_ne!(pick, &json!(key), "{}", reply.1);
        let after = selected(route.call("route.select", &params).await);
        assert_eq!(after["route"], live["route"]);
        assert_eq!(after["runner"], live["runner"]);
    }).await;
}
