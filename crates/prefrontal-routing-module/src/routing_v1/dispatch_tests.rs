//! Which surface answers a request on a name the older ops and `routing/v1`
//! share. Each marker is pinned both ways over the module's frame handler:
//! an older body carrying it gets the older reply, the same op without it
//! gets the v1 reply, and a v1 body carrying a member only the older op
//! defines is refused naming that member.

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::claims;
use super::conformance_tests::{install_catalog, reserved, Harness, Route};

fn body(method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap()
}

#[test]
fn the_older_shapes_are_recognised_by_their_marker_alone() {
    let v1_select = json!({"consumer": "flow", "requirements": {"iq": 50}});
    assert!(claims(&body("route.select", v1_select)));
    assert!(!claims(&body(
        "route.select",
        json!({"targetAgent": "mason", "requirements": {"iq": 50, "eq": 50}})
    )));
    for method in ["route.account_for", "route.account_observe"] {
        assert!(claims(&body(method, json!({"event_id": "e"}))), "{method}");
        assert!(
            !claims(&body(method, json!({"caller": {"runner": "broca"}}))),
            "{method}"
        );
    }
    for method in ["role.describe", "route.report"] {
        assert!(claims(&body(method, json!({}))), "{method}");
    }
    for method in [
        "route.select_panel",
        "route.account_status",
        "route.model_facts",
    ] {
        assert!(!claims(&body(method, json!({}))), "{method}");
    }
}

#[test]
fn the_manifest_registers_one_selection_pass() {
    let operations = crate::served_operations();
    let selects: Vec<_> = operations
        .iter()
        .filter(|operation| operation.name == "route.select")
        .collect();
    assert_eq!(
        selects.len(),
        1,
        "route.select is one pass, not two selectors"
    );
    let description = selects[0].description.as_deref().unwrap_or_default();
    assert!(description.contains("One selection pass"), "{description}");
    assert!(crate::handles("route.select"));
    assert!(!crate::handles("route.select_again"));
}

/// A harness with the kit's consumers and catalog, and a consumer route.
async fn configured() -> (std::sync::Arc<Harness>, Route) {
    let harness = Harness::new("dispatch");
    let consumers: Vec<String> = fixture::consumers()
        .into_iter()
        .map(|consumer| consumer.id)
        .collect();
    harness.edit_config(|config| {
        config["consumers"] = consumers
            .iter()
            .map(|id| {
                (
                    (*id).to_owned(),
                    json!({"owner": fixture::CONSUMER.trim_start_matches("reserved:")}),
                )
            })
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into();
    });
    install_catalog(&harness, &fixture::catalog());
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;
    (harness, route)
}

fn refused_field(reply: &(FrameType, Value)) -> (&str, &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    (
        reply.1["code"].as_str().unwrap(),
        reply.1["detail"]["field"].as_str().unwrap_or_default(),
    )
}

#[tokio::test]
async fn route_select_is_older_with_target_agent_and_v1_without() {
    let (_harness, route) = configured().await;
    let older = route
        .call(
            "route.select",
            &json!({
                "targetAgent": "mason",
                "requirements": {"iq": 50, "eq": 50},
                "availableModels": ["example-provider/model-a"],
            }),
        )
        .await;
    assert_eq!(older.0, FrameType::Response, "{}", older.1);
    assert!(older.1["result"]["decisionID"].is_string(), "{}", older.1);
    assert_eq!(
        older.1["result"]["decision_id"],
        older.1["result"]["decisionID"]
    );
    assert!(older.1["result"]["selected"].is_object(), "{}", older.1);

    let v1 = route
        .call(
            "route.select",
            &json!({"consumer": fixture::SUMMARY, "requirements": {"iq": 50}}),
        )
        .await;
    assert_eq!(v1.0, FrameType::Response, "{}", v1.1);
    assert!(v1.1["result"]["decision_id"].is_string(), "{}", v1.1);
    assert!(v1.1["result"]["runner"]["module"].is_string(), "{}", v1.1);

    let mixed = route
        .call(
            "route.select",
            &json!({
                "consumer": fixture::SUMMARY,
                "requirements": {"iq": 50},
                "availableModels": ["example-provider/model-a"],
            }),
        )
        .await;
    assert_eq!(
        refused_field(&mixed),
        ("invalid_request", "availableModels")
    );
}

fn admission(admission_id: &str) -> Value {
    json!({
        "provider": fixture::SUBSCRIPTION_PROVIDER,
        "model": "model-c",
        "conversation_key": format!("conversation-{admission_id}"),
        "admission_id": admission_id,
    })
}

#[tokio::test]
async fn converted_disabled_account_selection_serves_after_one_conversion() {
    let (harness, _) = configured().await;
    let converted = crate::config_convert::convert_value(json!({
        "account_selection": {"enabled": false}
    }))
    .unwrap();
    // The converter writes the shipped account families into `accounts`, but
    // never the retired flag.
    assert!(converted["accounts"].get("enabled").is_none());
    assert!(harness.operator_config().account_flags().enabled);
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let response = runner
        .call("route.account_for", &admission("disabled"))
        .await;
    assert_eq!(response.0, FrameType::Response, "{}", response.1);
}

#[tokio::test]
async fn named_model_cannot_bypass_operator_not_for() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["models"]["example-provider/model-a"] = json!({"not_for": [fixture::SUMMARY]});
    });
    let response = route
        .call(
            "route.select",
            &json!({
                "consumer": fixture::SUMMARY, "requirements": {"iq": 50},
                "pin": "example-provider/model-a"
            }),
        )
        .await;
    assert_eq!(response.0, FrameType::Error, "{}", response.1);
    assert_eq!(response.1["code"], "pin_unavailable", "{}", response.1);
    assert_eq!(response.1["detail"]["reason"], "excluded", "{}", response.1);
}

#[tokio::test]
async fn describe_omits_all_pin_slots_and_select_refuses_them_as_consumers() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        for slot in ["synthesizer", "lab_planner", "spec_designer"] {
            config["consumers"][slot] = json!({"pin": {"model": "example-provider/model-a"}});
        }
    });
    let describe = route.call("role.describe", &json!({})).await;
    assert_eq!(describe.0, FrameType::Response, "{}", describe.1);
    let ids: Vec<_> = describe.1["result"]["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect();
    for slot in ["synthesizer", "lab_planner", "spec_designer"] {
        assert!(!ids.contains(&slot), "{ids:?}");
        let selected = route
            .call(
                "route.select",
                &json!({"consumer": slot, "requirements": {"iq": 50}}),
            )
            .await;
        assert_eq!(selected.1["code"], "unknown_consumer", "{}", selected.1);
    }
}

#[tokio::test]
async fn route_account_for_is_older_with_caller_and_v1_without() {
    let (harness, _) = configured().await;
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;

    let mut older = admission("older");
    older["caller"] = json!({"runner": fixture::RUNNER_ONE});
    let older = runner.call("route.account_for", &older).await;
    assert_eq!(older.0, FrameType::Response, "{}", older.1);
    // The older answer states times as RFC 3339 text.
    assert!(
        older.1["result"]["pin"]["lease_valid_until"].is_string(),
        "{}",
        older.1
    );

    let v1 = runner.call("route.account_for", &admission("v1")).await;
    assert_eq!(v1.0, FrameType::Response, "{}", v1.1);
    assert!(
        v1.1["result"]["pin"]["lease_valid_until_ms"].is_u64(),
        "{}",
        v1.1
    );
    assert!(
        v1.1["result"]["pin"].get("lease_valid_until").is_none(),
        "{}",
        v1.1
    );

    let mut mixed = admission("mixed");
    mixed["model_decision_id"] = json!("dec_1");
    let mixed = runner.call("route.account_for", &mixed).await;
    assert_eq!(
        refused_field(&mixed),
        ("invalid_request", "model_decision_id")
    );
}

#[tokio::test]
async fn route_account_observe_is_older_with_caller_and_v1_without() {
    let (harness, _) = configured().await;
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let observation = |event: &str| json!({"event_id": event, "kind": "attempt"});

    let mut older = observation("older");
    older["caller"] = json!({"runner": fixture::RUNNER_ONE});
    let older = runner.call("route.account_observe", &older).await;
    assert_eq!(older.0, FrameType::Response, "{}", older.1);
    // The older answer lists every effect as an object, the note that the
    // observation bound no admission included.
    assert!(
        older.1["result"]["observation"]["effects"][0].is_object(),
        "{}",
        older.1
    );

    let v1 = runner
        .call("route.account_observe", &observation("v1"))
        .await;
    assert_eq!(v1.0, FrameType::Response, "{}", v1.1);
    assert_eq!(
        v1.1["result"]["observation"]["effects"],
        json!([]),
        "{}",
        v1.1
    );

    let mut mixed = observation("mixed");
    mixed["harness"] = json!("pi");
    let mixed = runner.call("route.account_observe", &mixed).await;
    assert_eq!(refused_field(&mixed), ("invalid_request", "harness"));
}

#[tokio::test]
async fn runner_ops_refuse_a_caller_that_is_not_a_known_runner() {
    let (harness, consumer) = configured().await;
    let direct = harness.bind(json!({"kind": "direct"}), None).await;
    for route in [&consumer, &direct] {
        for (method, params) in [
            ("route.account_for", admission("not-a-runner")),
            (
                "route.account_observe",
                json!({"event_id": "e", "kind": "attempt"}),
            ),
        ] {
            let reply = route.call(method, &params).await;
            assert_eq!(reply.0, FrameType::Error, "{method}: {}", reply.1);
            assert_eq!(reply.1["code"], "unscoped_caller", "{method}");
            assert_eq!(reply.1["detail"], json!({}), "{method}");
        }
    }
}
