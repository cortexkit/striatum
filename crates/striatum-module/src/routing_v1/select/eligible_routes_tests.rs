use cortexkit_role_routing::select::{
    check_answer, parse_select_request_serving, RouteRef, SelectAnswer,
};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::super::conformance_tests::Route;
use super::super::dispatch_tests::configured;

const A: &str = "example-provider/model-a";
const B: &str = "example-provider/model-b";

fn route_ref(pin: &str) -> Value {
    serde_json::to_value(RouteRef::parse_pin(pin).unwrap()).unwrap()
}

fn body(extra: Value) -> Value {
    let mut body = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}});
    for (key, value) in extra.as_object().unwrap() {
        body[key] = value.clone();
    }
    body
}

async fn answer(route: &Route, extra: Value) -> Value {
    let request = body(extra);
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let parsed =
        parse_select_request_serving(&request, super::super::describe::CAPABILITIES).unwrap();
    let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    check_answer(&parsed, &answer).unwrap();
    reply.1["result"].clone()
}

async fn refusal(route: &Route, extra: Value) -> Value {
    let reply = route.call("route.select", &body(extra)).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    reply.1
}

#[tokio::test]
async fn empty_and_unknown_subsets_are_no_available_model() {
    let (_harness, route) = configured().await;
    for eligible in [json!([]), json!([route_ref("unknown-provider/missing")])] {
        let refused = refusal(&route, json!({"eligible_routes":eligible})).await;
        assert_eq!(refused["code"], "no_route", "{refused}");
        assert_eq!(
            refused["detail"]["reason"], "no_available_model",
            "{refused}"
        );
        assert!(refused["detail"].get("reset_at_ms").is_none(), "{refused}");
    }
}

#[tokio::test]
async fn subsets_rank_only_listed_routes_and_repetitions_add_nothing() {
    let (_harness, route) = configured().await;
    assert_eq!(answer(&route, json!({})).await["route"], route_ref(B));
    for eligible in [
        json!([route_ref(A)]),
        json!([
            route_ref(A),
            route_ref(A),
            route_ref("example-provider/missing")
        ]),
    ] {
        let chosen = answer(&route, json!({"eligible_routes":eligible,"prefer":[B]})).await;
        assert_eq!(chosen["route"], route_ref(A), "{chosen}");
    }
    let refused = refusal(
        &route,
        json!({"eligible_routes":[route_ref(A)],"available_routes":[B]}),
    )
    .await;
    assert_eq!(refused["code"], "no_route", "{refused}");
}

#[tokio::test]
async fn subsets_never_widen_operator_consumer_or_request_policy() {
    let (harness, route) = configured().await;
    for policy in ["operator", "consumer", "request"] {
        harness.edit_config(|config| {
            config["model_routing"]["exclude"] = json!({"all":[]});
            if policy == "operator" {
                config["model_routing"]["exclude"] = json!({"all":[A]});
            } else if policy == "consumer" {
                config["model_routing"]["exclude"][fixture::SUMMARY] = json!([A]);
            }
        });
        let mut extra = json!({"eligible_routes":[route_ref(A)]});
        if policy == "request" {
            extra["exclude"] = json!([A]);
        }
        let refused = refusal(&route, extra).await;
        assert_eq!(refused["code"], "no_route", "{policy}: {refused}");
        assert_eq!(
            refused["detail"]["reason"], "no_available_model",
            "{policy}: {refused}"
        );
    }
}

#[tokio::test]
async fn request_and_slot_pins_outside_subset_refuse_or_fall_back_with_reason() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["consumers"]["spec_designer"]["pin"] = json!({"model":B});
    });
    for pin in [json!({"pin":B}), json!({"pin_slot":"spec_designer"})] {
        let mut extra = pin.clone();
        extra["eligible_routes"] = json!([route_ref(A)]);
        let refused = refusal(&route, extra.clone()).await;
        assert_eq!(refused["code"], "pin_unavailable", "{refused}");
        assert_eq!(refused["detail"]["reason"], "not_available", "{refused}");
        extra["pin_fallback"] = json!("route");
        let chosen = answer(&route, extra).await;
        assert_eq!(chosen["route"], route_ref(A), "{chosen}");
        assert_eq!(chosen["pin_outcome"], "fell_back", "{chosen}");
        assert_eq!(chosen["detail"]["pin_reason"], "not_available", "{chosen}");
    }
}

#[tokio::test]
async fn explicit_pin_variants_require_exact_eligible_identity() {
    let (_harness, route) = configured().await;
    let high = format!("{A}:high");
    for eligible in [route_ref(A), route_ref(&format!("{A}:low"))] {
        let refused = refusal(&route, json!({"pin":high,"eligible_routes":[eligible]})).await;
        assert_eq!(refused["code"], "pin_unavailable", "{refused}");
        assert_eq!(refused["detail"]["reason"], "not_available", "{refused}");
    }
    let chosen = answer(
        &route,
        json!({"pin":high,"eligible_routes":[route_ref(&high)]}),
    )
    .await;
    assert_eq!(chosen["route"], route_ref(&high), "{chosen}");
    let fallback = answer(
        &route,
        json!({"pin":high,"pin_fallback":"route","eligible_routes":[route_ref(A)]}),
    )
    .await;
    assert_eq!(fallback["route"], route_ref(A), "{fallback}");
    assert_eq!(fallback["pin_outcome"], "fell_back", "{fallback}");
    assert_eq!(
        fallback["detail"]["pin_reason"], "not_available",
        "{fallback}"
    );
}

#[tokio::test]
async fn curated_variants_match_exactly_without_stripping_or_substitution() {
    let (harness, route) = configured().await;
    for catalog_variant in [false, true] {
        let mut facts: Vec<_> = fixture::catalog()
            .models
            .iter()
            .enumerate()
            .map(|(rank, model)| {
                let mut fact = super::super::conformance_tests::catalog_fact(model, rank);
                if model.route_key() == B {
                    fact.variant = catalog_variant.then(|| "high".into());
                    fact.prompt_defaults
                        .get_or_insert_with(Default::default)
                        .variant = Some("high".into());
                }
                fact
            })
            .collect();
        harness.store().delete_fact(B).unwrap();
        harness.store().delete_fact(&format!("{B}/high")).unwrap();
        if catalog_variant {
            let mut low = facts
                .iter()
                .find(|fact| fact.model_id == "model-b")
                .unwrap()
                .clone();
            low.variant = Some("low".into());
            low.prompt_defaults.as_mut().unwrap().variant = Some("low".into());
            facts.push(low);
        }
        harness.install_facts(&facts);
        let mut unavailable = vec![route_ref(B)];
        if !catalog_variant {
            unavailable.push(route_ref(&format!("{B}:low")));
        } else {
            let low = answer(
                &route,
                json!({"eligible_routes":[route_ref(&format!("{B}:low"))]}),
            )
            .await;
            assert_eq!(low["route"], route_ref(&format!("{B}:low")), "{low}");
        }
        for eligible in unavailable {
            let refused = refusal(&route, json!({"eligible_routes":[eligible]})).await;
            assert_eq!(refused["code"], "no_route", "{refused}");
            assert_eq!(
                refused["detail"]["reason"], "no_available_model",
                "{refused}"
            );
        }
        let chosen = answer(
            &route,
            json!({"eligible_routes":[route_ref(&format!("{B}:high"))]}),
        )
        .await;
        assert_eq!(chosen["route"], route_ref(&format!("{B}:high")), "{chosen}");
        let next = answer(
            &route,
            json!({"eligible_routes":[route_ref(B),route_ref(A)]}),
        )
        .await;
        assert_eq!(next["route"], route_ref(A), "{next}");
    }
}

#[tokio::test]
async fn routing_variant_policy_is_resolved_before_eligibility_not_changed_to_fit_it() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["model_routing"]["variants"] = json!({"all":{B:"high"}});
    });
    assert_eq!(
        answer(&route, json!({})).await["route"],
        route_ref(&format!("{B}:high"))
    );
    for eligible in [route_ref(B), route_ref(&format!("{B}:low"))] {
        let refused = refusal(&route, json!({"eligible_routes":[eligible]})).await;
        assert_eq!(refused["code"], "no_route", "{refused}");
        assert_eq!(
            refused["detail"]["reason"], "no_available_model",
            "{refused}"
        );
    }
    let chosen = answer(
        &route,
        json!({"eligible_routes":[route_ref(&format!("{B}:high"))]}),
    )
    .await;
    assert_eq!(chosen["route"], route_ref(&format!("{B}:high")), "{chosen}");
    let fallback = answer(
        &route,
        json!({"eligible_routes":[route_ref(B),route_ref(A)]}),
    )
    .await;
    assert_eq!(fallback["route"], route_ref(A), "{fallback}");
    let refused = refusal(&route, json!({"pin":B,"eligible_routes":[route_ref(B)]})).await;
    assert_eq!(refused["code"], "pin_unavailable", "{refused}");
    assert_eq!(refused["detail"]["reason"], "not_available", "{refused}");
}

#[tokio::test]
async fn listed_pin_only_model_still_requires_a_pin() {
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["models"][A] = json!({"pin_only":true});
    });
    let refused = refusal(&route, json!({"eligible_routes":[route_ref(A)]})).await;
    assert_eq!(refused["code"], "no_route", "{refused}");
    assert_eq!(
        refused["detail"]["reason"], "no_available_model",
        "{refused}"
    );
    let pinned = answer(&route, json!({"pin":A,"eligible_routes":[route_ref(A)]})).await;
    assert_eq!(pinned["route"], route_ref(A), "{pinned}");
}

#[tokio::test]
async fn affinity_continuation_and_ladder_steps_cannot_escape_subset() {
    let (harness, route) = configured().await;
    let first = answer(&route, json!({"pin":B,"affinity":"eligible-session"})).await;
    let reported = route
        .call(
            "route.report",
            &json!({"decision_id":first["decision_id"],"outcome":"completed"}),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let continuation = answer(
        &route,
        json!({"affinity":"eligible-session","eligible_routes":[route_ref(A)]}),
    )
    .await;
    assert_eq!(continuation["route"], route_ref(A), "{continuation}");
    harness.edit_config(|config| {
        config["consumers"][fixture::SUMMARY]["mode"] = json!("ladder");
        config["consumers"][fixture::SUMMARY]["ladder"] = json!([B, A]);
    });
    let ladder = answer(&route, json!({"eligible_routes":[route_ref(A)]})).await;
    assert_eq!(ladder["route"], route_ref(A), "{ladder}");
}

#[tokio::test]
async fn affinity_on_an_ineligible_variant_does_not_override_automatic_preference() {
    let (_harness, route) = configured().await;
    let first = answer(
        &route,
        json!({"pin":format!("{B}:high"),"affinity":"variant-session"}),
    )
    .await;
    let reported = route
        .call(
            "route.report",
            &json!({"decision_id":first["decision_id"],"outcome":"completed"}),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let chosen = answer(
        &route,
        json!({
            "affinity":"variant-session","eligible_routes":[route_ref(A),route_ref(B)],"prefer":[A]
        }),
    )
    .await;
    assert_eq!(chosen["route"], route_ref(A), "{chosen}");
}

#[tokio::test]
async fn subset_refusal_preserves_candidate_state_reason_and_reset_only() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (_harness, route) = configured().await;
    for (key, delay) in [(A, 600_000), (B, 100_000)] {
        let cooldown = route
            .call(
                "route.record_cooldown",
                &json!({
                    "routeKey":key,"capacityKey":key,"cooldownUntil":super::super::now_ms()+delay
                }),
            )
            .await;
        assert_eq!(cooldown.0, FrameType::Response, "{}", cooldown.1);
    }
    let old = refusal(&route, json!({"available_routes":[A]})).await;
    let subset = refusal(&route, json!({"eligible_routes":[route_ref(A)]})).await;
    assert_eq!(subset["code"], "no_route", "{subset}");
    assert_eq!(
        subset["detail"]["reason"], old["detail"]["reason"],
        "{subset}"
    );
    assert_eq!(subset["detail"]["reason"], "quota_or_cooldown", "{subset}");
    assert_eq!(
        subset["detail"]["reset_at_ms"],
        json!(1_800_000_600_000_u64),
        "{subset}"
    );
    assert_eq!(
        subset["detail"]["reset_at_ms"], old["detail"]["reset_at_ms"],
        "{subset}"
    );
}

#[tokio::test]
async fn subset_keeps_the_existing_capability_refusal_for_its_candidates() {
    let (_harness, route) = configured().await;
    let requirements = json!({"iq":50,"max_output_tokens":40_000});
    let old = refusal(
        &route,
        json!({"available_routes":[B],"requirements":requirements}),
    )
    .await;
    let subset = refusal(
        &route,
        json!({"eligible_routes":[route_ref(B)],"requirements":requirements}),
    )
    .await;
    assert_eq!(subset, old);
    assert_eq!(subset["detail"]["reason"], "missing_capability", "{subset}");
}
