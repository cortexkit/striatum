use super::{configured, describe, install, refused, request, selected, setup, slot};
use cortexkit_role_routing::capabilities::SELECT_HARNESS_CAPABILITIES as GROUP;
use serde_json::{json, Value};
use striatum_core::{FactSource, RouteStateStore};
use subc_protocol::FrameType;

#[tokio::test]
async fn harness_refusal_names_unregistered_runner_without_explicit_needs() {
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"pending","harness":"primary"}]));
    let reply = route.call("route.select", &request(json!({"iq":0}))).await;
    refused(reply.clone(), "harness_unavailable");
    let detail = &reply.1["detail"];
    assert!(detail.get("missing").is_none(), "{detail}");
    assert_eq!(
        detail["runners"],
        json!([
            {"module":"pending", "harness":"primary", "reason":"not_registered"}
        ])
    );
}

#[tokio::test]
async fn harness_refusal_names_declared_but_stopped_harness() {
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"runner","harness":"primary"}]));
    install(
        &harness,
        "runner",
        &slot(json!({"primary":{"starts_sessions":false}})),
    );
    let reply = route.call("route.select", &request(json!({"iq":0}))).await;
    refused(reply.clone(), "harness_unavailable");
    let detail = &reply.1["detail"];
    assert!(detail.get("missing").is_none(), "{detail}");
    assert_eq!(
        detail["runners"],
        json!([
            {"module":"runner", "harness":"primary", "reason":"harness_stopped"}
        ])
    );
}

#[tokio::test]
async fn harness_refusal_names_undeclared_and_unregistered_entries_in_policy_order() {
    let (harness, route) = configured().await;
    setup(
        &harness,
        json!([
            {"module":"runner"}, {"module":"pending"}
        ]),
    );
    install(&harness, "runner", &slot(json!({})));
    let mut params = request(json!({"iq":0, "harness_capabilities":[]}));
    params["harness"] = json!("unhosted");
    let reply = route.call("route.select", &params).await;
    refused(reply.clone(), "harness_unavailable");
    let detail = &reply.1["detail"];
    assert_eq!(detail["missing"], json!({}));
    assert_eq!(
        detail["runners"],
        json!([
            {"module":"runner", "harness":"unhosted", "reason":"harness_not_declared",
             "declared":["primary", "secondary", "inherited"]},
            {"module":"pending", "harness":"unhosted", "reason":"not_registered"}
        ])
    );
}

#[tokio::test]
async fn harness_refusal_names_capability_gap_with_legacy_missing_unchanged() {
    let (harness, route) = configured().await;
    setup(&harness, json!([{"module":"runner"}]));
    install(
        &harness,
        "runner",
        &slot(json!({"primary":{"capabilities":["one_off"]}})),
    );
    let mut params = request(json!({"iq":0, "tools":"none", "harness_capabilities":["plans"]}));
    params["harness"] = json!("primary");
    let reply = route.call("route.select", &params).await;
    refused(reply.clone(), "harness_unavailable");
    let detail = &reply.1["detail"];
    assert_eq!(detail["missing"], json!({"runner/primary":["plans"]}));
    assert_eq!(
        detail["runners"],
        json!([
            {"module":"runner", "harness":"primary", "reason":"capabilities_missing",
             "missing":{"runner/primary":["plans"]}}
        ])
    );
    assert_eq!(reply.1["message"], "runner/primary lacks plans");
}

#[tokio::test]
async fn explicit_harness_needs_union_with_inferred_needs_on_one_pair() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"runner"}]));
        install(
            &harness,
            "runner",
            &slot(json!({
                "primary":{"capabilities":["one_off", "output_cap"]},
                "secondary":{"capabilities":["submit_tool", "final_text", "plans", "one_off"]},
                "inherited":{"capabilities":["submit_tool", "final_text", "plans", "one_off", "output_cap"]}
            })),
        );
        let mut params = request(json!({"iq":0, "harness_capabilities":["submit_tool", "final_text", "plans"]}));
        let explicit = selected(route.call("route.select", &params).await);
        assert_eq!(explicit["runner"]["harness"], "secondary");
        params["requirements"]["tools"] = json!("none");
        params["requirements"]["max_output_tokens"] = json!(0);
        let union = selected(route.call("route.select", &params).await);
        assert_eq!(union["runner"]["harness"], "inherited");
    })
    .await;
}

#[tokio::test]
async fn explicit_harness_needs_exclude_undeclaring_runners_only_for_explicit_names() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"legacy"}]));
        install(&harness, "legacy", &slot(json!({})));
        let mut params = request(json!({"iq":0, "tools":"none"}));
        let inferred = selected(route.call("route.select", &params).await);
        assert_eq!(inferred["runner"]["module"], "legacy");
        params["requirements"]["harness_capabilities"] = json!(["one_off"]);
        refused(
            route.call("route.select", &params).await,
            "harness_unavailable",
        );
    })
    .await;
}

#[tokio::test]
async fn explicit_harness_needs_report_each_pairs_missing_names() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"runner"}, {"module":"other"}]));
        install(
            &harness,
            "runner",
            &slot(json!({
                "primary":{"capabilities":["one_off", "output_cap"]},
                "secondary":{"capabilities":["one_off"]},
                "inherited":{"capabilities":["one_off", "output_cap"]}
            })),
        );
        install(&harness, "other", &slot(json!({})));
        let mut params = request(json!({
            "iq":0, "tools":"none", "max_output_tokens":0,
            "harness_capabilities":["fork_answer"]
        }));
        params["runners"] = json!(["runner"]);
        let reply = route.call("route.select", &params).await;
        refused(reply.clone(), "harness_unavailable");
        let detail = &reply.1["detail"];
        assert!(detail.get("reset_at_ms").is_none(), "{detail}");
        let missing = detail["missing"].as_object().expect("detail.missing");
        assert_eq!(missing.len(), 3, "{detail}");
        for (pair, expected) in [
            ("runner/primary", vec!["fork_answer"]),
            ("runner/secondary", vec!["fork_answer", "output_cap"]),
            ("runner/inherited", vec!["fork_answer"]),
        ] {
            let mut names: Vec<String> = serde_json::from_value(missing[pair].clone()).unwrap();
            names.sort();
            assert_eq!(names, expected, "{pair}: {detail}");
        }
        assert_eq!(
            detail["runners"],
            json!([
                {"module":"runner", "reason":"capabilities_missing", "missing":{
                    "runner/primary":["fork_answer"],
                    "runner/secondary":["output_cap", "fork_answer"],
                    "runner/inherited":["fork_answer"]
                }},
                {"module":"other", "reason":"excluded_by_policy", "filter":"request_runners"}
            ])
        );

        install(
            &harness,
            "runner",
            &slot(json!({"primary":{"capabilities":["fork_answer", "one_off", "output_cap"]}})),
        );
        let key = "example-provider/model-b";
        params["available_routes"] = json!([key]);
        let eligible = selected(route.call("route.select", &params).await);
        assert_eq!(eligible["runner"]["harness"], "primary");
        let reset = crate::now_ms() as u64 + 60_000;
        let cooldown = route
            .call(
                "route.record_cooldown",
                &json!({
                    "routeKey":key, "capacityKey":key, "cooldownUntil":reset
                }),
            )
            .await;
        assert_eq!(cooldown.0, FrameType::Response, "{}", cooldown.1);
        let waiting = route.call("route.select", &params).await;
        assert_eq!(waiting.1["detail"]["reset_at_ms"], reset, "{}", waiting.1);
        refused(waiting, "quota_or_cooldown");
    })
    .await;
}

fn panel(capabilities: Value) -> Value {
    json!({
        "class":"audit", "consumer":"example-consumer.harness_choice_test",
        "requirements":{"harness_capabilities":capabilities}
    })
}

#[tokio::test]
async fn explicit_harness_needs_refuse_unknown_names_by_request_path() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (_harness, route) = configured().await;
        let unknown = json!(["structured_output"]);
        for (op, params, field) in [
            (
                "route.select_panel",
                panel(unknown.clone()),
                "requirements.harness_capabilities",
            ),
            (
                "route.select",
                request(json!({"iq":0, "harness_capabilities":unknown})),
                "harness_capabilities",
            ),
        ] {
            let reply = route.call(op, &params).await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "invalid_requirements", "{}", reply.1);
            assert_eq!(reply.1["detail"]["field"], field, "{}", reply.1);
        }
    })
    .await;
}

#[tokio::test]
async fn explicit_harness_needs_apply_to_each_panel_seat() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        setup(&harness, json!([{"module":"runner"}]));
        install(
            &harness,
            "runner",
            &slot(json!({"secondary":{"capabilities":["plans"]}})),
        );
        harness.edit_config(|config| {
            config["panels"]["audit"] = json!({"top_k":2, "by":"iq", "extra":0, "floor":0});
        });
        for mut fact in harness.store().get_facts().unwrap() {
            if fact.provider_id == "example-provider" {
                fact.family = Some(fact.model_id.clone());
                harness
                    .store()
                    .upsert_fact(&fact, FactSource::Override)
                    .unwrap();
            }
        }
        let mut params = panel(json!(["plans"]));
        params["available_routes"] =
            json!(["example-provider/model-a", "example-provider/model-b"]);
        let reply = route.call("route.select_panel", &params).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let seats = reply.1["result"]["seats"].as_array().unwrap();
        assert_eq!(seats.len(), 2, "{}", reply.1);
        for seat in seats {
            assert_eq!(seat["answer"]["runner"]["harness"], "secondary", "{seat}");
        }
    })
    .await;
}

#[tokio::test]
async fn explicit_harness_needs_advertisement_and_parsing_follow_the_group() {
    let (_harness, route) = configured().await;
    let production = route.call("role.describe", &json!({})).await;
    assert_advertised(&production.1["result"], true);
    describe::without_capability(GROUP, async {
        let off = route.call("role.describe", &json!({})).await;
        assert_advertised(&off.1["result"], false);
        for value in [Value::Null, json!(["submit_tool"])] {
            let params = request(json!({"iq":0, "harness_capabilities":value}));
            let reply = route.call("route.select", &params).await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "invalid_requirements", "{}", reply.1);
            assert_eq!(
                reply.1["detail"]["field"], "harness_capabilities",
                "{}",
                reply.1
            );
            let reply = route.call("route.select_panel", &panel(value)).await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
            assert_eq!(
                reply.1["detail"]["field"], "requirements.harness_capabilities",
                "{}",
                reply.1
            );
        }
    })
    .await;
    describe::with_candidate_capabilities(&[GROUP], async {
        let on = route.call("role.describe", &json!({})).await;
        assert_advertised(&on.1["result"], true);
    })
    .await;
}

fn assert_advertised(answer: &Value, expected: bool) {
    for (list, name) in [
        ("capabilities", GROUP),
        ("requirement_fields", "harness_capabilities"),
    ] {
        assert_eq!(
            answer[list].as_array().unwrap().contains(&json!(name)),
            expected,
            "{list}: {name}"
        );
    }
}
