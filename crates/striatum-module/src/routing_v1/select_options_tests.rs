//! `route.select` with `pin_fallback` and `prefer`, over the module's frame
//! handler with the conformance kit's consumers and catalog. In that catalog
//! the plain select for [`fixture::SUMMARY`] answers the cheapest model,
//! `example-provider/model-b`, and every model meets the plain demand.

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::Route;
use super::dispatch_tests::configured;

const MODEL_A: &str = "example-provider/model-a";
const MODEL_B: &str = "example-provider/model-b";
const MODEL_C: &str = "example-subscription/model-c";
const MODEL_D: &str = "example-subscription/model-d";

fn select_body(extra: Value) -> Value {
    let mut body = json!({"consumer": fixture::SUMMARY, "requirements": {"iq": 50}});
    for (key, value) in extra.as_object().unwrap() {
        body[key] = value.clone();
    }
    body
}

#[tokio::test]
async fn decommissioned_pin_uses_contract_refusal_even_when_policy_would_admit_it() {
    let (harness, route) = configured().await;
    let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version":91,"resolved_at_ms":123,"models":{
            (MODEL_B):{"existence":"present","quality.decommissioned":true,"capability.tool_call":true}
        }
    })).unwrap()).unwrap();
    crate::fusiform_catalog::apply_mapped_catalog(
        &harness.store(),
        &snapshot,
        None,
        Some("example-provider"),
    )
    .unwrap();
    let refused = route
        .call("route.select", &select_body(json!({"pin":MODEL_B})))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    assert_eq!(refused.1["code"], "pin_unavailable");
    assert_eq!(
        refused.1["detail"],
        json!({"pin":MODEL_B,"reason":"decommissioned"})
    );
    let parsed: cortexkit_role_routing::errors::Refusal =
        serde_json::from_value(refused.1).unwrap();
    assert_eq!(parsed.wait_until_ms(), None);
}

async fn answered(route: &Route, extra: Value) -> Value {
    let reply = route.call("route.select", &select_body(extra)).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

fn route_key(answer: &Value) -> String {
    format!(
        "{}/{}",
        answer["route"]["provider"].as_str().unwrap(),
        answer["route"]["model"].as_str().unwrap()
    )
}

#[tokio::test]
async fn an_unservable_pin_falls_back_only_when_asked_and_says_why() {
    let (_harness, route) = configured().await;
    let plain = answered(&route, json!({})).await;
    assert_eq!(route_key(&plain), MODEL_B, "{plain}");
    assert!(plain.get("pin_outcome").is_none(), "{plain}");

    let missing = "example-provider/model-z";
    let refused = route
        .call("route.select", &select_body(json!({"pin": missing})))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "pin_unavailable", "{}", refused.1);

    let fell_back = answered(&route, json!({"pin": missing, "pin_fallback": "route"})).await;
    assert_eq!(fell_back["pin_outcome"], "fell_back", "{fell_back}");
    assert_eq!(
        fell_back["detail"],
        json!({"pin_reason": "unknown_model"}),
        "the reason is given even when detail was not asked for: {fell_back}"
    );
    assert_eq!(route_key(&fell_back), route_key(&plain), "{fell_back}");

    let asked = answered(
        &route,
        json!({"pin": missing, "pin_fallback": "route", "detail": true}),
    )
    .await;
    assert_eq!(asked["detail"]["pin_reason"], "unknown_model", "{asked}");
    assert!(asked["detail"]["basis"].is_string(), "{asked}");
}

#[tokio::test]
async fn a_served_pin_is_labelled_pinned_with_or_without_fallback() {
    let (_harness, route) = configured().await;
    for extra in [
        json!({"pin": MODEL_A}),
        json!({"pin": MODEL_A, "pin_fallback": "route"}),
    ] {
        let pinned = answered(&route, extra.clone()).await;
        assert_eq!(route_key(&pinned), MODEL_A, "{extra}: {pinned}");
        assert_eq!(pinned["pin_outcome"], "pinned", "{extra}: {pinned}");
        assert!(pinned.get("detail").is_none(), "{extra}: {pinned}");
    }
}

#[tokio::test]
async fn an_excluded_pin_falls_back_to_a_route_the_request_allows() {
    let (_harness, route) = configured().await;
    let fell_back = answered(
        &route,
        json!({"pin": MODEL_A, "pin_fallback": "route", "exclude": [MODEL_A, MODEL_B]}),
    )
    .await;
    assert_eq!(fell_back["pin_outcome"], "fell_back", "{fell_back}");
    assert_eq!(fell_back["detail"]["pin_reason"], "excluded", "{fell_back}");
    let answered_key = route_key(&fell_back);
    assert!(
        answered_key != MODEL_A && answered_key != MODEL_B,
        "a fallback still honours the request's exclusions: {fell_back}"
    );
}

/// `exclude_providers` keeps every route of a listed provider out of the
/// answer: the plain answer's provider is passed over for another provider,
/// a pin on it is refused as excluded, and listing every provider leaves no
/// route.
#[tokio::test]
async fn an_excluded_provider_is_never_answered_or_pinned() {
    let (_harness, route) = configured().await;
    let other = answered(&route, json!({"exclude_providers": ["example-provider"]})).await;
    assert_eq!(
        other["route"]["provider"], "example-subscription",
        "{other}"
    );

    let pinned = route
        .call(
            "route.select",
            &select_body(json!({"pin": MODEL_A, "exclude_providers": ["example-provider"]})),
        )
        .await;
    assert_eq!(pinned.0, FrameType::Error, "{}", pinned.1);
    assert_eq!(pinned.1["code"], "pin_unavailable", "{}", pinned.1);
    assert_eq!(pinned.1["detail"]["reason"], "excluded", "{}", pinned.1);

    let none = route
        .call(
            "route.select",
            &select_body(
                json!({"exclude_providers": ["example-provider", "example-subscription"]}),
            ),
        )
        .await;
    assert_eq!(none.0, FrameType::Error, "{}", none.1);
    assert_eq!(none.1["code"], "no_route", "{}", none.1);
}

#[tokio::test]
async fn prefer_answers_the_first_listed_adequate_route_in_list_order() {
    let (_harness, route) = configured().await;
    for (prefer, expected) in [
        (json!([MODEL_D, MODEL_C]), MODEL_D),
        (json!([MODEL_C, MODEL_D]), MODEL_C),
        (json!(["example-provider/model-z", MODEL_A]), MODEL_A),
    ] {
        let preferred = answered(&route, json!({"prefer": prefer, "detail": true})).await;
        assert_eq!(route_key(&preferred), expected, "{prefer}: {preferred}");
        assert_eq!(
            preferred["detail"]["basis"], "the request's preference among adequate routes",
            "{preferred}"
        );
    }
}

#[tokio::test]
async fn prefer_never_admits_a_route_the_request_would_not_get() {
    let (_harness, route) = configured().await;
    // Only model-c (64k output) meets a 40k output floor; model-a has 32k.
    let floor = json!({"iq": 50, "context_window": 150_000, "max_output_tokens": 40_000});
    let preferred = answered(&route, json!({"requirements": floor, "prefer": [MODEL_A]})).await;
    assert_eq!(route_key(&preferred), MODEL_C, "{preferred}");

    let excluded = answered(
        &route,
        json!({"prefer": [MODEL_D, MODEL_A], "exclude": [MODEL_D]}),
    )
    .await;
    assert_eq!(
        route_key(&excluded),
        MODEL_A,
        "an excluded listed route is skipped, the next listed one answered: {excluded}"
    );
}

#[tokio::test]
async fn a_pin_decides_before_prefer() {
    let (_harness, route) = configured().await;
    let pinned = answered(&route, json!({"pin": MODEL_A, "prefer": [MODEL_C]})).await;
    assert_eq!(route_key(&pinned), MODEL_A, "{pinned}");
    assert_eq!(pinned["pin_outcome"], "pinned", "{pinned}");
}

#[tokio::test]
async fn slot_pin_answers_pinned_unknown_slot_and_unavailable_fallback() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["consumers"]["spec_designer"]["pin"] = json!({"model":MODEL_A,"variant":"high"});
    });
    for fallback in [None, Some("route")] {
        let mut extra = json!({"pin_slot":"spec_designer"});
        if let Some(fallback) = fallback {
            extra["pin_fallback"] = json!(fallback);
        }
        let answer = answered(&route, extra).await;
        assert_eq!(answer["pin_outcome"], "pinned");
        assert_eq!(route_key(&answer), MODEL_A);
        assert_eq!(answer["route"]["variant"], "high");
    }
    let unknown = route
        .call("route.select", &select_body(json!({"pin_slot":"unset"})))
        .await;
    assert_eq!(unknown.1["code"], "pin_unavailable");
    assert_eq!(unknown.1["detail"]["pin"], "unset");
    assert_eq!(unknown.1["detail"]["reason"], "unknown_slot");
    let fallback = answered(&route, json!({"pin_slot":"unset","pin_fallback":"route"})).await;
    assert_eq!(fallback["pin_outcome"], "fell_back");
    assert_eq!(fallback["detail"]["pin_reason"], "unknown_slot");
    harness.edit_config(|config| {
        config["consumers"]["spec_designer"]["pin"] = json!({"model":"example-provider/missing"});
    });
    let fallback = answered(
        &route,
        json!({"pin_slot":"spec_designer","pin_fallback":"route"}),
    )
    .await;
    assert_eq!(fallback["pin_outcome"], "fell_back");
    assert_eq!(fallback["detail"]["pin_reason"], "unknown_model");
    let unavailable = route
        .call(
            "route.select",
            &select_body(json!({"pin_slot":"spec_designer"})),
        )
        .await;
    assert_eq!(unavailable.1["detail"]["pin"], "spec_designer");
}

#[tokio::test]
async fn available_routes_restricts_automatic_pinned_and_preferred_selection() {
    let (_harness, route) = configured().await;
    let answer = answered(
        &route,
        json!({"available_routes":[MODEL_A],"prefer":[MODEL_B]}),
    )
    .await;
    assert_eq!(route_key(&answer), MODEL_A);
    let request = select_body(json!({"pin":MODEL_B,"available_routes":[MODEL_A]}));
    let refused = route.call("route.select", &request).await;
    assert_eq!(refused.0, FrameType::Error);
    let answer = answered(
        &route,
        json!({"pin":MODEL_B,"pin_fallback":"route","available_routes":[MODEL_A]}),
    )
    .await;
    assert_eq!(route_key(&answer), MODEL_A);
    assert_eq!(answer["pin_outcome"], "fell_back");
    let empty = route
        .call("route.select", &select_body(json!({"available_routes":[]})))
        .await;
    assert_eq!(empty.1["detail"]["field"], "available_routes");
}

#[tokio::test]
async fn prompt_cache_preference_reaches_the_adequate_scorer_only() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["providers"]["example-provider"]["prompt_cache"] = json!(false);
    });
    assert_eq!(
        route_key(&answered(&route, json!({"prefer_prompt_cache":false})).await),
        MODEL_B
    );
    let cached = answered(&route, json!({"prefer_prompt_cache":true})).await;
    assert_eq!(cached["route"]["provider"], "example-subscription");
    let excluded = answered(
        &route,
        json!({"prefer_prompt_cache":true,"exclude_providers":["example-subscription"]}),
    )
    .await;
    assert_eq!(excluded["route"]["provider"], "example-provider");
}

#[tokio::test]
async fn settings_results_are_answered_and_pass_the_contract_answer_check() {
    use crate::project_settings::tests::{census, root, write, Fake, ID};
    use cortexkit_role_routing::select::{
        check_answer, parse_select_request_serving, SelectAnswer,
    };
    for status in ["applied", "none", "unresolved"] {
        let (harness, route) = configured().await;
        let project = root();
        if status == "applied" {
            write(&project, "{}");
        }
        let source = Fake::new(census(&[project], None));
        source
            .fail
            .store(status == "unresolved", std::sync::atomic::Ordering::Relaxed);
        harness.settings_source(source);
        let request = select_body(json!({"settings":{"project_id":ID}}));
        let result = route.call("route.select", &request).await;
        assert_eq!(result.0, FrameType::Response, "{}", result.1);
        assert_eq!(result.1["result"]["project_settings"], status);
        let parsed = parse_select_request_serving(&request, super::describe::CAPABILITIES).unwrap();
        let answer: SelectAnswer = serde_json::from_value(result.1["result"].clone()).unwrap();
        check_answer(&parsed, &answer).unwrap();
        let persisted = harness
            .store()
            .v1_selection_json(&answer.decision_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&persisted).unwrap()["project_settings"],
            status
        );
    }
}

#[tokio::test]
async fn empty_settings_is_global_only_and_a_mismatched_stamp_is_named() {
    use super::conformance_tests::reserved;
    let (harness, _route) = configured().await;
    let project = crate::project_settings::tests::root();
    crate::project_settings::tests::write(&project, "broken");
    harness.edit_config(|config| {
        config["consumers"][fixture::SUMMARY]["projects"]["project-a"] = json!({"pin":MODEL_A});
    });
    let route = harness
        .bind(reserved(fixture::CONSUMER), Some("project-a"))
        .await;
    let answer = answered(&route, json!({"settings":{}})).await;
    assert_eq!(route_key(&answer), MODEL_B);
    assert!(answer.get("project_settings").is_none());
    let refused = route
        .call(
            "route.select",
            &select_body(json!({"settings":{"project_id":"project-b"}})),
        )
        .await;
    assert_eq!(refused.1["code"], "invalid_request");
    assert_eq!(refused.1["detail"]["field"], "settings.project_id");
}
