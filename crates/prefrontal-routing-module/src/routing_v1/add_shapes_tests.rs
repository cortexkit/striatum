use cortexkit_role_routing::{budget, explain, select::SelectAnswer};
use cortexkit_role_routing_conformance::fixture;
use prefrontal_routing_core::RouteStateStore;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::reserved;
use super::dispatch_tests::configured;

fn field(reply: &(FrameType, Value)) -> (&str, &str) {
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    (
        reply.1["code"].as_str().unwrap(),
        reply.1["detail"]["field"].as_str().unwrap_or(""),
    )
}

#[tokio::test]
async fn explain_every_legacy_spelling_and_contract_marker_dispatch() {
    let (_harness, route) = configured().await;
    let legacy = route
        .call(
            "route.select",
            &json!({
                "targetAgent":"mason", "requirements":{"iq":50,"eq":50},
                "availableModels":["example-provider/model-a"], "sendID":"shapes-send"
            }),
        )
        .await;
    assert_eq!(legacy.0, FrameType::Response, "{legacy:?}");
    let id = &legacy.1["result"]["decisionID"];
    for spelling in ["decisionID", "decisionId", "sendID", "sendId", "send_id"] {
        let value = if spelling.starts_with("send") {
            json!("shapes-send")
        } else {
            id.clone()
        };
        let reply = route.call("route.explain", &json!({spelling:value})).await;
        assert_eq!(reply.0, FrameType::Response, "{spelling}: {reply:?}");
        assert_eq!(&reply.1["result"]["decisionID"], id);
        let mixed = route
            .call("route.explain", &json!({spelling:value,"decision_id":id}))
            .await;
        assert_eq!(field(&mixed), ("invalid_request", spelling));
    }
    let v1 = route
        .call("route.explain", &json!({"decision_id":id}))
        .await;
    assert_eq!(v1.1["code"], "unknown_decision");
    let empty = route.call("route.explain", &json!({})).await;
    assert_eq!(field(&empty), ("invalid_request", "decision_id"));
}

#[tokio::test]
async fn panel_shapes_are_served_and_validate_unknown_members() {
    let (_harness, route) = configured().await;
    let describe = route.call("role.describe", &json!({})).await;
    assert!(describe.1["result"]["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("select_panel")));
    let panel = route
        .call(
            "route.select_panel",
            &json!({"class":"audit","consumer":fixture::SUMMARY}),
        )
        .await;
    assert_eq!(panel.0, FrameType::Response, "{panel:?}");
    assert_eq!(
        route
            .call(
                "route.select_panel",
                &json!({"replace":{"panel_id":"p","slot":0}})
            )
            .await
            .1["code"],
        "unknown_decision"
    );
    for (request, expected) in [
        (json!({}), "class"),
        (
            json!({"class":"athena","consumer":fixture::SUMMARY,"panelSpec":{}}),
            "panelSpec",
        ),
        (
            json!({"class":"athena","consumer":fixture::SUMMARY,"requirements":{"gpu_count":1}}),
            "requirements.gpu_count",
        ),
        (
            json!({"class":"athena","consumer":fixture::SUMMARY,"settings":{"injected":true}}),
            "settings.injected",
        ),
        (
            json!({"replace":{"panel_id":"p","slot":0,"spare":1}}),
            "replace.spare",
        ),
        (
            json!({"replace":{"panel_id":"p","slot":0},"extra":true}),
            "extra",
        ),
    ] {
        let reply = route.call("route.select_panel", &request).await;
        assert_eq!(
            field(&reply),
            ("invalid_request", expected),
            "{request}: {reply:?}"
        );
    }
}

#[tokio::test]
async fn injected_select_context_is_legacy_only_and_requirements_keep_their_code() {
    let (_harness, route) = configured().await;
    for injected in ["project_root", "workspace_root", "tier_consumer"] {
        let mut body = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}});
        body[injected] = json!("legacy-context");
        let old = route.call("route.select", &body).await;
        assert_eq!(old.0, FrameType::Response, "{old:?}");
        for marker in [
            "settings",
            "pin_slot",
            "available_routes",
            "prefer_prompt_cache",
        ] {
            let mut mixed = body.clone();
            mixed[marker] = if marker == "settings" {
                json!({})
            } else {
                json!(true)
            };
            assert_eq!(
                field(&route.call("route.select", &mixed).await),
                ("invalid_request", injected)
            );
        }
    }
    for (addition, code, name) in [
        (json!({"extra":true}), "invalid_request", "extra"),
        (
            json!({"requirements":{"iq":50,"gpu_count":1}}),
            "invalid_requirements",
            "gpu_count",
        ),
        (
            json!({"settings":{"extra":1}}),
            "invalid_request",
            "settings.extra",
        ),
        (
            json!({"concurrency_slots":[{"key":"a/b","running":0,"limit":1,"extra":1}]}),
            "invalid_request",
            "concurrency_slots[0].extra",
        ),
    ] {
        let mut body = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}});
        body.as_object_mut()
            .unwrap()
            .extend(addition.as_object().unwrap().clone());
        assert_eq!(
            field(&route.call("route.select", &body).await),
            (code, name)
        );
    }
}

#[tokio::test]
async fn selection_reads_keep_capacities_and_full_runner_after_catalog_changes() {
    let (harness, route) = configured().await;
    let mut facts = harness.store().get_facts().unwrap();
    for fact in &mut facts {
        fact.context_window = Some(100_000.0);
        fact.max_output_tokens = Some(20_000);
    }
    harness.install_facts(&facts);
    let reply = route.call("route.select", &json!({
        "consumer":fixture::SUMMARY,"requirements":{"iq":50},"pin":"example-provider/model-a"
    })).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let selection: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert_eq!(
        (selection.context_window, selection.max_output_tokens),
        (100_000, 20_000)
    );
    for fact in &mut facts {
        fact.context_window = Some(1_000_000.0);
        fact.max_output_tokens = Some(200_000);
    }
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([]);
    });
    let request = explain::ExplainRequest::new(&selection.decision_id);
    let reply = route
        .call("route.explain", &serde_json::to_value(&request).unwrap())
        .await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let answer: explain::ExplainAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    explain::check_explain(&selection, &request, &answer).unwrap();
    let quote = route
        .call(
            "route.budget_quote",
            &json!({
                "decisions":[selection.decision_id], "prompt_tokens":10_000
            }),
        )
        .await;
    assert_eq!(quote.0, FrameType::Response, "{quote:?}");
    assert_eq!(quote.1["result"]["package"]["tokens"], 32_000);
    assert_eq!(
        quote.1["result"]["seats"][0]["output_limit"]["tokens"],
        20_000
    );
    let answer: budget::BudgetQuote = serde_json::from_value(quote.1["result"].clone()).unwrap();
    budget::check_budget_quote(
        &[budget::BudgetSeat::from_select_answer(&selection)],
        Some(10_000),
        &answer,
    )
    .unwrap();
    let foreign = harness.bind(reserved("foreign"), None).await;
    for (op, body) in [
        (
            "route.explain",
            json!({"decision_id":selection.decision_id}),
        ),
        (
            "route.budget_quote",
            json!({"decisions":[selection.decision_id]}),
        ),
    ] {
        assert_eq!(foreign.call(op, &body).await.1["code"], "unknown_decision");
    }
}

#[tokio::test]
async fn reads_refuse_pre_add_decisions_and_bare_account_allocations() {
    let (harness, route) = configured().await;
    let selected = route
        .call(
            "route.select",
            &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    let mut old = harness
        .store()
        .v1_decision(selected.1["result"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    old.decision_id = "pre-add".into();
    harness.store().v1_record_decision(&old).unwrap();
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let allocation = runner
        .call(
            "route.account_for",
            &json!({
                "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                "admission_id":"bare","conversation_key":"bare"
            }),
        )
        .await;
    assert_eq!(allocation.0, FrameType::Response, "{allocation:?}");
    let allocation_id = allocation.1["result"]["reason"]["decision_id"]
        .as_str()
        .unwrap();
    for id in ["pre-add", allocation_id, "missing"] {
        for caller in [&route, &runner] {
            assert_eq!(
                caller
                    .call("route.explain", &json!({"decision_id":id}))
                    .await
                    .1["code"],
                "unknown_decision"
            );
            assert_eq!(
                caller
                    .call("route.budget_quote", &json!({"decisions":[id]}))
                    .await
                    .1["code"],
                "unknown_decision"
            );
        }
    }
}

#[tokio::test]
async fn identical_status_shape_keeps_legacy_fields_and_adds_contract_state() {
    let (_harness, route) = configured().await;
    let body = json!({"provider":"unknown","account_id":"unknown"});
    let reply = route.call("route.account_status", &body).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["warm_ok"], false);
    assert_eq!(reply.1["result"]["reason"]["code"], "account_unknown");
    assert_eq!(reply.1["result"]["limit_state"], "unknown");
    use cortexkit_role_routing::account_status::{
        check_account_status, parse_account_status_request,
    };
    check_account_status(
        &parse_account_status_request(&body).unwrap(),
        &serde_json::from_value(reply.1["result"].clone()).unwrap(),
    )
    .unwrap();
    for (op, body, expected) in [
        ("route.account_status", json!({}), "provider"),
        (
            "route.account_status",
            json!({"provider":"p","account_id":"a","caller":{}}),
            "caller",
        ),
        (
            "route.head_choices",
            json!({"runner_module":"unknown","extra":true}),
            "extra",
        ),
        (
            "route.budget_quote",
            json!({"decisions":["missing"],"extra":true}),
            "extra",
        ),
        (
            "route.explain",
            json!({"decision_id":"missing","extra":true}),
            "extra",
        ),
    ] {
        assert_eq!(
            field(&route.call(op, &body).await),
            ("invalid_request", expected)
        );
    }
}

#[tokio::test]
async fn account_and_report_closed_records_name_the_unread_member() {
    let (harness, owner) = configured().await;
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    for (addition, expected) in [
        (json!({"extra":true}), "extra"),
        (
            json!({"expected_pin":{"id":"p","generation":1,"extra":true}}),
            "expected_pin.extra",
        ),
        (
            json!({"requested_account":{"account_id":"a","extra":true}}),
            "requested_account.extra",
        ),
        (
            json!({"candidates":{"view_digest":"v","accounts":[],"extra":true}}),
            "candidates.extra",
        ),
        (
            json!({"candidates":{"view_digest":"v","accounts":[{"account_id":"a","credential_id":"c","extra":true}]}}),
            "candidates.accounts[0].extra",
        ),
    ] {
        let mut body =
            json!({"provider":"p","model":"m","conversation_key":"k","admission_id":"a"});
        body.as_object_mut()
            .unwrap()
            .extend(addition.as_object().unwrap().clone());
        assert_eq!(
            field(&runner.call("route.account_for", &body).await),
            ("invalid_request", expected)
        );
    }
    for (addition, expected) in [
        (json!({"extra":true}), "extra"),
        (json!({"usage":{"output_tokens":1,"extra":true}}), "usage"),
        (
            json!({"eligibility":{"scope":"model","code":"not_on_plan","extra":true}}),
            "eligibility.extra",
        ),
    ] {
        let mut body = json!({"decision_id":"missing","outcome":"completed"});
        body.as_object_mut()
            .unwrap()
            .extend(addition.as_object().unwrap().clone());
        assert_eq!(
            field(&owner.call("route.report", &body).await),
            ("invalid_request", expected)
        );
    }
    assert_eq!(
        field(
            &runner
                .call(
                    "route.account_observe",
                    &json!({"event_id":"e","kind":"attempt","extra":true})
                )
                .await
        ),
        ("invalid_request", "extra")
    );
    let lowercase = runner.call("route.account_observe", &json!({"event_id":"lowercase","kind":"attempt","headers":{"x-arbitrary-header":"kept"}})).await;
    assert_eq!(lowercase.0, FrameType::Response, "{lowercase:?}");
    let uppercase = runner.call("route.account_observe", &json!({"event_id":"uppercase","kind":"attempt","headers":{"X-Arbitrary-Header":"refused"}})).await;
    assert_eq!(field(&uppercase), ("invalid_request", "headers"));
    assert_eq!(
        field(&runner.call("route.account_for", &json!({})).await),
        ("invalid_request", "provider")
    );
    assert_eq!(
        field(&runner.call("route.account_observe", &json!({})).await),
        ("invalid_request", "kind")
    );
}

#[tokio::test]
async fn mixed_account_markers_never_reach_a_permissive_legacy_decoder() {
    let (harness, owner) = configured().await;
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    for (method, request) in [
        (
            "route.account_for",
            json!({"caller":{"runner":fixture::RUNNER_ONE},"mode":"continue"}),
        ),
        (
            "route.account_observe",
            json!({"caller":{"runner":fixture::RUNNER_ONE},"event_id":"mixed","kind":"attempt","retry_at_ms":1}),
        ),
    ] {
        assert_eq!(
            field(&runner.call(method, &request).await),
            ("invalid_request", "caller")
        );
    }
    let markerless = owner.call("route.select", &json!({})).await;
    assert_eq!(markerless.0, FrameType::Error);
    assert_eq!(markerless.1["code"], "invalid_request");
}
