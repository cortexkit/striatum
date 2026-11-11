use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use cortexkit_role_routing::{budget, panel::*};
use cortexkit_role_routing_conformance::fixture;
use striatum_core::RouteStateStore;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{
    install_catalog, install_panel_families, reserved, upgrade_harness, Harness, Route,
};

#[path = "panels_tests/tests.rs"]
mod shortfall_diagnostics;

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
async fn new_panel_inserts_final_state_without_update() {
    let (h, route, request) = configured().await;
    let connection = rusqlite::Connection::open(h.store_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE panel_writes(kind TEXT NOT NULL);
         CREATE TRIGGER panel_insert AFTER INSERT ON routing_v1_panel BEGIN
           INSERT INTO panel_writes VALUES ('insert'); END;
         CREATE TRIGGER panel_update AFTER UPDATE ON routing_v1_panel BEGIN
           INSERT INTO panel_writes VALUES ('update'); END;",
        )
        .unwrap();
    let answer = panel(&route, &request).await;
    let writes: Vec<String> = connection
        .prepare("SELECT kind FROM panel_writes ORDER BY rowid")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(writes, ["insert"]);
    let retained: Value = serde_json::from_str(
        &h.store()
            .v1_panel(&answer.panel_id, "reserved:prefrontal-core")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(retained["answer"], serde_json::to_value(&answer).unwrap());
}

#[tokio::test]
async fn same_panel_replacements_serialize_and_consume_each_others_history() {
    let (h, route, body) = configured().await;
    let answer = panel(&route, &body).await;
    let lock = super::panels::replacement_lock(&answer.panel_id).unwrap();
    let guard = lock.lock().await;
    let request = PanelReplacementRequest::new(&answer.panel_id, 0);
    let params = serde_json::to_value(&request).unwrap();
    let calls = async {
        tokio::join!(
            route.call("route.select_panel", &params),
            route.call("route.select_panel", &params)
        )
    };
    tokio::pin!(calls);
    // Both handlers must reach the same held lock before either can read the
    // standing seats. Their strong references keep the weak registry entry live.
    tokio::select! {
        biased;
        _ = &mut calls => panic!("replacements completed while their panel lock was held"),
        result = tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&lock) < 3 { tokio::task::yield_now().await; }
        }) => result.expect("both replacements should queue on their panel lock"),
    }
    drop(guard);
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), calls)
        .await
        .unwrap();
    let mut replacements: Vec<PanelReplacement> = [first, second]
        .into_iter()
        .map(|reply| {
            assert_eq!(reply.0, FrameType::Response, "{reply:?}");
            serde_json::from_value(reply.1["result"].clone()).unwrap()
        })
        .collect();
    let first_seat = replacements[0].seat.as_ref().unwrap();
    let second_seat = replacements[1].seat.as_ref().unwrap();
    assert_ne!(
        first_seat.answer.route.route_key(),
        second_seat.answer.route.route_key()
    );
    assert_ne!(
        first_seat.answer.decision_id,
        second_seat.answer.decision_id
    );
    let retained: Value = serde_json::from_str(
        &h.store()
            .v1_panel(&answer.panel_id, "reserved:prefrontal-core")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let standing = retained["answer"]["seats"][0]["answer"]["decision_id"]
        .as_str()
        .unwrap();
    // Handler scheduling may choose either request first; the final standing
    // decision identifies the replacement that must validate against new history.
    replacements.sort_by_key(|replacement| {
        replacement.seat.as_ref().unwrap().answer.decision_id == standing
    });
    let mut state = PanelState::new(
        parse_select_panel_request_serving(&body, super::describe::CAPABILITIES).unwrap(),
        answer,
    );
    for replacement in replacements {
        check_panel_replacement(&state, &request, &replacement).unwrap();
        assert!(retained["history"]
            .as_array()
            .unwrap()
            .contains(&json!(replacement
                .seat
                .as_ref()
                .unwrap()
                .answer
                .route
                .route_key())));
        state.apply(&replacement);
    }
}

#[tokio::test]
async fn unrelated_panel_replacement_and_creation_do_not_wait_for_a_held_panel_lock() {
    let (_h, route, request) = configured().await;
    let a = panel(&route, &request).await;
    let b = panel(&route, &request).await;
    let lock = super::panels::replacement_lock(&b.panel_id).unwrap();
    let _guard = lock.lock().await;
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        route.call(
            "route.select_panel",
            &json!({"replace":{"panel_id":a.panel_id,"slot":0}}),
        ),
    )
    .await
    .expect("panel A must not wait for panel B");
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let replacement: PanelReplacement = serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert!(replacement.seat.is_some());
    tokio::time::timeout(Duration::from_secs(5), panel(&route, &request))
        .await
        .expect("creation must not wait for a replacement lock");
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
                striatum_core::route_state_store::FactSource::Override,
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
    let detail: PanelShortfallDetail =
        serde_json::from_value(replacement.detail.clone().unwrap()).unwrap();
    assert_eq!(detail.package_tokens, Some(70_000));
    let package = detail
        .causes
        .iter()
        .find(|cause| cause.reason == "package_too_large")
        .unwrap();
    assert_eq!(
        package.typed_detail().unwrap().unwrap().budget_tokens,
        1_000
    );
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
                striatum_core::route_state_store::FactSource::Override,
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
        striatum_store::replay::Inputs::new(frozen["clock_ms"].as_i64().unwrap())
            .install();
    let h = Harness::new("public-panel-goldens");
    let facts: Vec<striatum_core::ModelFact> =
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
