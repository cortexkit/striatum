use std::collections::BTreeSet;
use std::sync::Arc;

use cortexkit_role_routing::{budget, panel::*};
use cortexkit_role_routing_conformance::fixture;
use prefrontal_routing_core::RouteStateStore;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{
    install_catalog, install_panel_families, reserved, upgrade_harness, Harness, Route,
};

async fn configured() -> (Arc<Harness>, Route, Value) {
    let h = Harness::new("public-panels");
    let mut catalog = fixture::catalog();
    let original = catalog.models[0].clone();
    catalog.models = ["claude", "gpt", "gemini", "grok", "qwen"]
        .iter()
        .enumerate()
        .map(|(i, family)| {
            let mut model = original.clone();
            model.model = format!("panel-{i}");
            model.family = Some((*family).into());
            model.iq = 90;
            model.context_window = 200_000;
            model.max_output_tokens = 32_000;
            model
        })
        .collect();
    let routes: Vec<_> = catalog.models.iter().map(|m| m.route_key()).collect();
    for runner in &mut catalog.runners {
        runner.route_keys = routes.clone();
    }
    install_catalog(&h, &catalog);
    install_panel_families(&h, &catalog);
    h.edit_config(|c| {
        c["panels"]["audit"] = json!({"topK":2,"by":"iq","extra":0,"floor":0});
        c["consumers"][fixture::SUMMARY] = json!({"owner":"prefrontal-core"});
    });
    let route = h.bind(reserved("prefrontal-core"), None).await;
    let request = json!({"class":"audit","consumer":fixture::SUMMARY,"available_routes":routes,"temperatures":[0.2,0.9],"seed":"fixed"});
    (h, route, request)
}

async fn panel(route: &Route, request: &Value) -> SelectPanelAnswer {
    let reply = route.call("route.select_panel", request).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let answer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    let request =
        parse_select_panel_request_serving(request, super::describe::CAPABILITIES).unwrap();
    check_panel_answer(&request, &answer).unwrap();
    answer
}

#[tokio::test]
async fn panel_seats_are_independent_retained_decisions_and_replace_without_cycles_after_restart() {
    let (h, route, request) = configured().await;
    let answer = panel(&route, &request).await;
    assert_eq!(answer.seats.len(), 2);
    let ids: Vec<_> = answer
        .seats
        .iter()
        .map(|s| s.answer.decision_id.clone())
        .collect();
    assert_ne!(ids[0], ids[1]);
    let quote = route
        .call("route.budget_quote", &json!({"decisions":ids}))
        .await;
    assert_eq!(quote.0, FrameType::Response, "{quote:?}");
    let expected = budget::quote(
        &answer
            .seats
            .iter()
            .map(|s| budget::BudgetSeat::from_select_answer(&s.answer))
            .collect::<Vec<_>>(),
        None,
    )
    .unwrap();
    assert_eq!(quote.1["result"], serde_json::to_value(expected).unwrap());
    for seat in &answer.seats {
        assert_eq!(
            route
                .call(
                    "route.explain",
                    &json!({"decision_id":seat.answer.decision_id})
                )
                .await
                .1["result"]["runner"],
            serde_json::to_value(&seat.answer.runner).unwrap()
        );
        assert_eq!(
            route
                .call(
                    "route.report",
                    &json!({"decision_id":seat.answer.decision_id,"outcome":"cancelled"})
                )
                .await
                .0,
            FrameType::Response
        );
    }
    let foreign = h.bind(reserved("foreign"), None).await;
    assert_eq!(
        foreign
            .call(
                "route.select_panel",
                &json!({"replace":{"panel_id":answer.panel_id,"slot":0}})
            )
            .await
            .1["code"],
        "unknown_decision"
    );
    drop(foreign);
    drop(route);
    let h = upgrade_harness(h);
    let route = h.bind(reserved("prefrontal-core"), None).await;
    let parsed =
        parse_select_panel_request_serving(&request, super::describe::CAPABILITIES).unwrap();
    let mut state = PanelState::new(parsed, answer.clone());
    let mut seen: BTreeSet<_> = ids.into_iter().collect();
    for _ in 0..3 {
        let request = PanelReplacementRequest::new(&answer.panel_id, 0);
        let reply = route
            .call(
                "route.select_panel",
                &serde_json::to_value(&request).unwrap(),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        let replacement: PanelReplacement =
            serde_json::from_value(reply.1["result"].clone()).unwrap();
        check_panel_replacement(&state, &request, &replacement).unwrap();
        assert!(seen.insert(
            replacement
                .seat
                .as_ref()
                .unwrap()
                .answer
                .decision_id
                .clone()
        ));
        state.apply(&replacement);
    }
    let before = state.clone();
    let request = PanelReplacementRequest::new(&answer.panel_id, 0);
    let reply = route
        .call(
            "route.select_panel",
            &serde_json::to_value(&request).unwrap(),
        )
        .await;
    let replacement: PanelReplacement = serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert_eq!(replacement.cause, Some(ReplacementCause::NoCandidate));
    check_panel_replacement(&state, &request, &replacement).unwrap();
    state.apply(&replacement);
    assert_eq!(state, before);
    let invalid = route
        .call(
            "route.select_panel",
            &json!({"replace":{"panel_id":answer.panel_id,"slot":answer.policy.size}}),
        )
        .await;
    assert_eq!(invalid.1["code"], "invalid_request");
    assert_eq!(invalid.1["detail"]["field"], "replace.slot");
    h.store()
        .delete_decisions_older_than(i64::MAX, 100_000)
        .unwrap();
    assert_eq!(
        route
            .call(
                "route.select_panel",
                &serde_json::to_value(request).unwrap()
            )
            .await
            .1["code"],
        "unknown_decision"
    );
}

#[tokio::test]
async fn panel_replacement_package_cause_preserves_the_standing_seat() {
    let (h, route, mut request) = configured().await;
    request["package_tokens"] = json!(70_000);
    let answer = panel(&route, &request).await;
    let previous = &answer.seats[0];
    for fact in h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .filter(|f| f.model_id.starts_with("panel-"))
    {
        if answer
            .seats
            .iter()
            .any(|s| s.answer.route.model == fact.model_id)
        {
            continue;
        }
        let mut fact = fact;
        fact.context_window = Some(50_000.0);
        h.store()
            .upsert_fact(
                &fact,
                prefrontal_routing_core::route_state_store::FactSource::Override,
            )
            .unwrap();
    }
    let request = PanelReplacementRequest::new(&answer.panel_id, previous.slot);
    let standing = h
        .store()
        .v1_panel(&answer.panel_id, "reserved:prefrontal-core")
        .unwrap();
    let reply = route
        .call(
            "route.select_panel",
            &serde_json::to_value(&request).unwrap(),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let replacement: PanelReplacement = serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert_eq!(replacement.cause, Some(ReplacementCause::PackageUnmeetable));
    assert_eq!(
        h.store()
            .v1_panel(&answer.panel_id, "reserved:prefrontal-core")
            .unwrap(),
        standing
    );
    let state = PanelState::new(
        parse_select_panel_request_serving(
            &serde_json::to_value(SelectPanelRequest::new(PanelClass::Audit, fixture::SUMMARY))
                .unwrap(),
            super::describe::CAPABILITIES,
        )
        .unwrap(),
        answer,
    );
    check_panel_replacement(&state, &request, &replacement).unwrap();
}

#[tokio::test]
async fn panel_refuses_non_panel_members_and_invalid_boundaries() {
    let (_h, route, request) = configured().await;
    for member in [
        "pin",
        "pin_slot",
        "pin_fallback",
        "prefer",
        "prefer_prompt_cache",
        "affinity",
        "exclude_providers",
        "concurrency_slots",
        "detail",
        "hold_ok",
        "hold_wait_max_s",
    ] {
        let mut body = request.clone();
        body[member] = json!(true);
        let reply = route.call("route.select_panel", &body).await;
        assert_eq!(reply.1["code"], "invalid_request", "{reply:?}");
        assert_eq!(reply.1["detail"]["field"], member, "{reply:?}");
    }
    for (key, value) in [
        ("temperatures", json!([2.1])),
        ("temperatures", json!(vec![0; 9])),
        ("seed", json!("x".repeat(257))),
        ("class", json!("unknown")),
    ] {
        let mut body = request.clone();
        body[key] = value;
        let reply = route.call("route.select_panel", &body).await;
        assert_eq!(reply.1["code"], "invalid_request", "{reply:?}");
        assert_eq!(reply.1["detail"]["field"], key, "{reply:?}");
    }
}

#[tokio::test]
async fn panel_constraints_filter_vision_context_and_excluded_routes() {
    let (h, route, mut request) = configured().await;
    for mut fact in h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .filter(|f| f.model_id.starts_with("panel-"))
    {
        fact.supports_vision = Some(fact.model_id != "panel-0");
        if fact.model_id == "panel-1" {
            fact.context_window = Some(1000.0);
        }
        h.store()
            .upsert_fact(
                &fact,
                prefrontal_routing_core::route_state_store::FactSource::Override,
            )
            .unwrap();
    }
    let provider = request["available_routes"][0]
        .as_str()
        .unwrap()
        .split_once('/')
        .unwrap()
        .0
        .to_owned();
    request["requirements"] = json!({"context_window":100_000,"modalities":["image"]});
    request["exclude"] = json!([format!("{provider}/panel-2")]);
    let answer = panel(&route, &request).await;
    assert_eq!(answer.seats.len(), 2);
    assert!(answer
        .seats
        .iter()
        .all(|seat| ["panel-3", "panel-4"].contains(&seat.answer.route.model.as_str())));
}

#[tokio::test]
async fn panel_athena_and_spec_match_frozen_seats_variants_and_budgets() {
    let frozen: Value =
        serde_json::from_str(include_str!("../../fixtures/replay/panels.golden.json")).unwrap();
    let _clock = crate::InjectedClock::set(frozen["clock_ms"].as_u64().unwrap());
    let _inputs =
        prefrontal_routing_store::replay::Inputs::new(frozen["clock_ms"].as_i64().unwrap())
            .install();
    let h = Harness::new("public-panel-goldens");
    let facts: Vec<prefrontal_routing_core::ModelFact> =
        serde_json::from_value(frozen["catalog"].clone()).unwrap();
    let mut catalog = fixture::catalog();
    let original = catalog.models[0].clone();
    catalog.models = facts
        .iter()
        .map(|fact| {
            let mut model = original.clone();
            model.provider = fact.provider_id.clone();
            model.model = fact.model_id.clone();
            model.family = fact.family.clone();
            model
        })
        .collect();
    catalog.runners.truncate(1);
    catalog.runners[0].module = "broca".into();
    catalog.runners[0].route_keys = catalog.models.iter().map(|m| m.route_key()).collect();
    install_catalog(&h, &catalog);
    h.install_facts(&facts);
    h.edit_config(|c| {
        c["demand_exponent"] = json!(1.0);
        c["consumers"]["athena"] = json!({"owner":"prefrontal-core"});
        c["consumers"]["athena"]["variants"] =
            frozen["policy"]["model_routing"]["variants"]["athena"].clone();
    });
    let route = h.bind(reserved("prefrontal-core"), None).await;
    for case in frozen["panels"].as_array().unwrap() {
        let old = &case["request"];
        let request = json!({"class":old["panelSpec"]["class"].as_str().unwrap().to_ascii_lowercase(),"consumer":"athena","available_routes":old["availableModels"],"seed":old["seed"],"task_ref":old["taskId"],"runners":["broca"]});
        let answer = panel(&route, &request).await;
        let expected = case["reply"]["result"]["members"].as_array().unwrap();
        assert_eq!(answer.seats.len(), expected.len(), "{}", case["name"]);
        assert_eq!(
            answer.policy.top,
            case["reply"]["result"]["policy"]["topK"].as_u64().unwrap()
        );
        assert_eq!(
            answer.policy.extra,
            case["reply"]["result"]["policy"]["extra"].as_u64().unwrap()
        );
        for (seat, member) in answer.seats.iter().zip(expected) {
            assert_eq!(
                seat.answer.route.route_key(),
                member["routeKey"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                seat.answer.route.variant.as_deref(),
                member["promptParams"]["variant"].as_str()
            );
            assert_eq!(
                seat.answer.runner.module,
                member["harness"].as_str().unwrap()
            );
            assert_eq!(seat.family.as_deref(), member["family"].as_str());
            assert_eq!(
                seat.answer.service_tier_chosen.as_deref(),
                member["promptParams"]["service_tier_chosen"].as_str()
            );
            let quote = budget::quote(
                &[budget::BudgetSeat::from_select_answer(&seat.answer)],
                None,
            )
            .unwrap();
            let expected_budget = case["budgets"]
                .as_array()
                .unwrap()
                .iter()
                .find(|b| b["routeKey"] == member["routeKey"])
                .unwrap();
            assert_eq!(
                quote.package.tokens,
                expected_budget["tokens"].as_u64().unwrap()
            );
        }
    }
}
