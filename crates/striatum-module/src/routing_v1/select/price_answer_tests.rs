use cortexkit_role_routing::{
    price::RatesPerMtok,
    select::{check_answer_serving, parse_select_request_serving, SelectAnswer},
};
use cortexkit_role_routing_conformance::{fixture, SelectionFacts};
use serde_json::{json, Value};
use striatum_core::RouteStateStore;
use striatum_core::{
    fact_to_route,
    task_profile::{ProfileLevel, TaskProfile, TokenVector},
    ModelFact,
};
use subc_protocol::FrameType;

use super::super::{
    conformance_tests::{reserved, selection_facts, Route},
    dispatch_tests::configured,
};

const A: &str = "example-provider/model-a";
const C: &str = "example-subscription/model-c";

fn request(pin: &str) -> Value {
    json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"pin":pin,"detail":true})
}

fn rates() -> RatesPerMtok {
    RatesPerMtok::new(4.into(), 20.into())
        .with_cache_read(1.into())
        .with_cache_write(5.into())
        .with_cache_write_ttl([("5m".into(), 5.into()), ("1h".into(), 8.into())].into())
}

fn checked(params: &Value, answer: &Value) {
    let caps = super::super::describe::capabilities();
    let request = parse_select_request_serving(params, &caps).unwrap();
    let answer: SelectAnswer = serde_json::from_value(answer.clone()).unwrap();
    check_answer_serving(&request, &answer, &caps).unwrap();
}

fn lookup(
    tokens: TokenVector,
) -> impl Fn(&str, f64, &[ModelFact]) -> crate::select::TaskProfileLookup + Sync {
    move |workload, iq, facts| {
        assert_eq!(workload, fixture::SUMMARY);
        assert_eq!(iq, 50.0);
        crate::select::TaskProfileLookup {
            profiles: facts
                .iter()
                .map(|fact| {
                    (
                        fact_to_route(fact).route_key,
                        TaskProfile {
                            tokens,
                            level: ProfileLevel::OwnCell,
                            task_count: 20,
                            own_task_count: 20,
                            own_weight: 1.0,
                            keeps_tokenizer: true,
                        },
                    )
                })
                .collect(),
            providers_without_cache_write_reports: Default::default(),
        }
    }
}

async fn answered(route: &Route, params: &Value) -> Value {
    let reply = route.call("route.select", params).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    checked(params, &reply.1["result"]);
    reply.1["result"].clone()
}

#[tokio::test]
async fn prices_are_unconditional_and_only_change_the_stored_answer_member() {
    let _clock = crate::InjectedClock::set(crate::now_ms());
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input.list_rates.insert(A.into(), rates());
    selection_facts(&h, &input);
    let params = request(A);
    let mut plain = super::super::describe::without_price(answered(&route, &params)).await;
    assert!(plain.get("price").is_none());
    let priced = answered(&route, &params).await;
    assert_eq!(
        priced["price"],
        json!({"basis":"metered","rates_per_mtok":{"input":4,"output":20,"cache_read":1,"cache_write":5,"cache_write_ttl":{"5m":5,"1h":8}}})
    );
    let connection = rusqlite::Connection::open(h.store_path()).unwrap();
    let stored = |answer: &Value| -> (String, String, String) {
        connection.query_row("SELECT requirements_json, score_json, allowed_account_class FROM model_route_decisions WHERE id=?1", [answer["decision_id"].as_str().unwrap()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap()
    };
    assert_eq!(stored(&plain), stored(&priced));
    let selection = h
        .store()
        .v1_selection_json(priced["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&selection).unwrap(), priced);
    let mut without = priced.clone();
    without.as_object_mut().unwrap().remove("price");
    plain["decision_id"] = without["decision_id"].clone();
    assert_eq!(plain, without);
}

#[tokio::test]
async fn estimate_maps_reasoning_and_zero_cache_buckets_without_role_fallbacks() {
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input.list_rates.insert(A.into(), rates());
    selection_facts(&h, &input);
    let params = request(A);
    let tokens = TokenVector {
        fresh_input: 100_000.0,
        cached_read: 200_000.0,
        cache_write: 300_000.0,
        output_excluding_reasoning: 400_000.0,
        reasoning: 500_000.0,
    };
    let answer = route
        .select_with_profiles(&params, &lookup(tokens))
        .await
        .unwrap();
    checked(&params, &answer);
    assert_eq!(answer["price"]["estimated_task_usd"], json!(20.1));
    input
        .list_rates
        .insert(A.into(), RatesPerMtok::new(4.into(), 20.into()));
    selection_facts(&h, &input);
    let zero_cache = TokenVector {
        fresh_input: 100_000.0,
        output_excluding_reasoning: 400_000.0,
        reasoning: 500_000.0,
        ..Default::default()
    };
    let answer = route
        .select_with_profiles(&params, &lookup(zero_cache))
        .await
        .unwrap();
    assert_eq!(answer["price"]["estimated_task_usd"], json!(18.4));
    assert!(answer["price"]["rates_per_mtok"]
        .get("cache_read")
        .is_none());
    assert!(answer["price"]["rates_per_mtok"]
        .get("cache_write")
        .is_none());
    let no_rate = route
        .select_with_profiles(&params, &lookup(tokens))
        .await
        .unwrap();
    assert!(no_rate.get("price").is_some());
    assert!(no_rate["price"].get("estimated_task_usd").is_none());
    input.list_rates.insert(
        A.into(),
        RatesPerMtok::new(4.into(), 20.into())
            .with_cache_write_ttl([("5m".into(), 5.into())].into()),
    );
    selection_facts(&h, &input);
    let ttl = route
        .select_with_profiles(
            &params,
            &lookup(TokenVector {
                cache_write: 10.0,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(ttl.get("price").is_some());
    assert!(ttl["price"].get("estimated_task_usd").is_none());
    checked(&params, &ttl);
}

#[tokio::test]
async fn kit_estimate_facts_become_tokens_not_an_echoed_reserve() {
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input
        .list_rates
        .insert(C.into(), RatesPerMtok::new(20.into(), 100.into()));
    input.multipliers.insert(C.into(), 20.into());
    input.estimates.insert(C.into(), 1.into());
    let profiles = selection_facts(&h, &input);
    let lookup = move |_: &str, _: f64, _: &[ModelFact]| crate::select::TaskProfileLookup {
        profiles: profiles.clone(),
        providers_without_cache_write_reports: Default::default(),
    };
    let params = request(C);
    let first = route.select_with_profiles(&params, &lookup).await.unwrap();
    assert_eq!(first["price"]["estimated_task_usd"], 1, "{first}");
    input
        .list_rates
        .insert(C.into(), RatesPerMtok::new(20.into(), 200.into()));
    selection_facts(&h, &input);
    let changed = route.select_with_profiles(&params, &lookup).await.unwrap();
    assert_eq!(changed["price"]["estimated_task_usd"], 2);
    assert_eq!(changed["price"]["multiplier"], 20);
    assert_eq!(changed["price"]["rates_per_mtok"]["output"], json!(10.0));
    checked(&params, &changed);
}

#[tokio::test]
async fn mixed_credentials_quote_subscription_or_metered_and_fallback_uses_list_reserve() {
    use striatum_core::billing::{BillingPlane, BillingRate};
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (h, _) = super::super::account_classes_tests::setup("synthetic", false);
    let mut fact = h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "synthetic")
        .unwrap();
    let plane = |method: &str, input, output| BillingPlane {
        provider_id: "synthetic".into(),
        auth_method: method.into(),
        kind: if method == "oauth" {
            "subscription"
        } else {
            "upstream"
        }
        .into(),
        selection_reason: "test account class".into(),
        rates: [
            ("rate.input".into(), BillingRate::Priced { rate: input }),
            ("rate.output".into(), BillingRate::Priced { rate: output }),
        ]
        .into(),
    };
    fact.input_rate = Some(40.0);
    fact.cost_per_m_tok_out = Some(200.0);
    fact.billing_plane = Some(plane("apikey", 40.0, 200.0));
    let mut subscription = fact.clone();
    subscription.billing_plane = Some(plane("oauth", 4.0, 20.0));
    h.store()
        .upsert_fact(
            &fact,
            striatum_core::route_state_store::FactSource::Override,
        )
        .unwrap();
    let credentials = h.credentials();
    credentials.lock().unwrap().publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact, subscription]),
    );
    h.edit_config(|config| {
        config["providers"]["synthetic"]["subscription_multiplier"] = json!(22);
    });
    let route = h.bind(reserved("class-owner"), None).await;
    let profiles = |_: &str, _: f64, facts: &[ModelFact]| crate::select::TaskProfileLookup {
        profiles: facts
            .iter()
            .map(|fact| {
                (
                    fact_to_route(fact).route_key,
                    TaskProfile {
                        tokens: TokenVector {
                            fresh_input: 1_000_000.0,
                            output_excluding_reasoning: 1_000_000.0,
                            ..Default::default()
                        },
                        level: ProfileLevel::OwnCell,
                        task_count: 20,
                        own_task_count: 20,
                        own_weight: 1.0,
                        keeps_tokenizer: true,
                    },
                )
            })
            .collect(),
        providers_without_cache_write_reports: Default::default(),
    };
    let body =
        |consumer| json!({"consumer":consumer,"requirements":{"iq":50},"pin":"synthetic/model"});
    let sub = route
        .select_with_profiles(&body("mason"), &profiles)
        .await
        .unwrap();
    assert_eq!(sub["price"]["basis"], "subscription_equivalent", "{sub}");
    assert_eq!(sub["price"]["multiplier"], 22);
    assert_eq!(sub["price"]["rates_per_mtok"]["input"], json!(4.0 / 22.0));
    assert_eq!(
        sub["price"]["estimated_task_usd"],
        json!(4.0 / 22.0 + 20.0 / 22.0)
    );
    let paid = route
        .select_with_profiles(&body("codemode"), &profiles)
        .await
        .unwrap();
    assert_eq!(
        paid["price"],
        json!({"basis":"metered","rates_per_mtok":{"input":40,"output":200},"estimated_task_usd":240})
    );
    h.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"]["metered_fallback"] = json!(true)
    });
    let fallback = route
        .select_with_profiles(&body("mason"), &profiles)
        .await
        .unwrap();
    assert_eq!(fallback["price"], paid["price"]);
    for (consumer, answer) in [("mason", sub), ("codemode", paid), ("mason", fallback)] {
        checked(&body(consumer), &answer);
    }
}

#[tokio::test]
async fn no_price_for_unmeasured_subscription_or_unpriced_antigravity() {
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input.list_rates.insert(C.into(), rates());
    input.list_rates.insert(A.into(), rates());
    selection_facts(&h, &input);
    assert!(answered(&route, &request(A)).await.get("price").is_some());
    let unmeasured = answered(&route, &request(C)).await;
    assert!(unmeasured.get("price").is_none());
    let (h, _) = super::super::account_classes_tests::setup("antigravity", false);
    let mut facts = h.store().get_facts().unwrap();
    for fact in &mut facts {
        if fact.provider_id == "antigravity" {
            fact.input_rate = None;
            fact.cost_per_m_tok_out = None;
            if let Some(plane) = &mut fact.billing_plane {
                plane.rates.clear();
            }
        }
    }
    for fact in &facts {
        h.store()
            .upsert_fact(fact, striatum_core::route_state_store::FactSource::Override)
            .unwrap();
    }
    let credentials = h.credentials();
    credentials.lock().unwrap().publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    let route = h.bind(reserved("class-owner"), None).await;
    let unpriced = answered(
        &route,
        &json!({"consumer":"mason","requirements":{"iq":50},"pin":"antigravity/model"}),
    )
    .await;
    assert!(unpriced.get("price").is_none());
}

#[tokio::test]
async fn nonstandard_tier_uses_catalog_modes_not_quota_draw_and_omits_unknown_modes() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    for method in ["apikey", "oauth"] {
        let (h, _) = super::super::service_tier_tests::setup_for_method(
            &["pro_500"],
            true,
            true,
            method,
            "openai.responses",
        );
        h.edit_config(|config| {
            config["providers"]["openai"]["subscription_multiplier"] = json!(22);
            config["providers"]["openai"]["subscription"] = json!(method == "oauth");
        });
        let route = h.bind(reserved("tier-owner"), None).await;
        let params = json!({"consumer":"mason","requirements":{"iq":50},"pin":"openai/gpt-6.1-sol","service_tier":"fast"});
        let answer = answered(&route, &params).await;
        assert_eq!(answer["service_tier_chosen"], "fast");
        let divisor = if method == "oauth" { 22.0 } else { 1.0 };
        assert_eq!(
            answer["price"]["rates_per_mtok"]["input"].as_f64(),
            Some(4.0 / divisor),
            "{answer}"
        );
        assert_eq!(
            answer["price"]["rates_per_mtok"]["output"].as_f64(),
            Some(20.0 / divisor),
            "{answer}"
        );
        let credentials = h.credentials();
        let mut credentials = credentials.lock().unwrap();
        let facts = h.store().get_facts().unwrap();
        // The plan still admits fast, but the model no longer quotes mode rates.
        let payload: Value = serde_json::from_str(include_str!(
            "../../../fixtures/service-tier-v1/fusiform-served-payloads.json"
        ))
        .unwrap();
        let mut catalog = payload["catalog.get — provider default endpoint"]["response"].clone();
        catalog["models"]["openai/gpt-6.1-sol"]
            .as_object_mut()
            .unwrap()
            .retain(|key, _| {
                !(key.starts_with("rate.input") || key.starts_with("rate.output"))
                    || !key.ends_with(".mode.fast")
            });
        let plane_catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
            .with_test_catalog(&serde_json::to_vec(&catalog).unwrap())
            .with_wire_apis(&credentials.wire_apis);
        credentials.publish_billing_catalog(plane_catalog);
        drop(credentials);
        let unknown = answered(&route, &params).await;
        assert!(unknown.get("price").is_none(), "{unknown}");
    }
}

#[tokio::test]
async fn real_history_lends_the_answered_routes_workload_and_iq_profile() {
    use striatum_core::{
        route_state_store::{DecisionInput, UsageObservation},
        task_profile::TASK_CLOSE_MS,
    };
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input
        .list_rates
        .insert(A.into(), RatesPerMtok::new(4.into(), 20.into()));
    input
        .list_rates
        .insert(C.into(), RatesPerMtok::new(20.into(), 100.into()));
    selection_facts(&h, &input);
    for (iq, workload, output) in [
        (50.0, fixture::SUMMARY, 10_000.0),
        (70.0, fixture::SUMMARY, 900_000.0),
        (50.0, "other-workload", 800_000.0),
    ] {
        for index in 0..20 {
            let id = format!("history-{workload}-{iq}-{index}");
            h.store()
                .record_decision_with_task_ref(
                    &DecisionInput {
                        decision_id: Some(id.clone()),
                        consumer: Some(workload.into()),
                        target_agent: workload.into(),
                        selected_model_id: Some("model-c".into()),
                        requirements_json: json!({"iq":iq}).to_string(),
                        ..Default::default()
                    },
                    None,
                    None,
                    Some(&id),
                )
                .unwrap();
            h.store()
                .record_usage_delivery_for_decision(
                    &UsageObservation {
                        id: id.clone(),
                        route_key: C.into(),
                        capacity_key: C.into(),
                        provider_id: "example-subscription".into(),
                        input_tokens: Some(1000.0),
                        cached_read_tokens: Some(0.0),
                        cache_write_tokens: Some(0.0),
                        output_tokens: output,
                        reasoning_tokens: Some(0.0),
                        duration_ms: 1000.0,
                        workload_class: None,
                        account_key: None,
                        observed_at: (1_800_000_000_000 - 2 * TASK_CLOSE_MS) as f64,
                    },
                    &id,
                )
                .unwrap();
        }
    }
    let rows = h.store().closed_task_usage(1_800_000_000_000).unwrap();
    assert_eq!(rows.len(), 60);
    let facts = h.store().get_facts().unwrap();
    let snapshot = crate::task_profile_refresh::build_snapshot(&rows, &facts);
    let borrowed = snapshot.lookup(
        fixture::SUMMARY,
        50.0,
        &[facts
            .iter()
            .find(|fact| fact.provider_id == "example-provider" && fact.model_id == "model-a")
            .unwrap()
            .clone()],
    );
    assert_ne!(borrowed[A].level, ProfileLevel::OwnCell);
    assert_eq!(borrowed[A].tokens.output_excluding_reasoning, 10_000.0);
    let lookup =
        move |workload: &str, iq: f64, facts: &[ModelFact]| crate::select::TaskProfileLookup {
            profiles: snapshot.lookup(workload, iq, facts),
            providers_without_cache_write_reports: Default::default(),
        };
    let params = request(A);
    let answer = route.select_with_profiles(&params, &lookup).await.unwrap();
    assert!((answer["price"]["estimated_task_usd"].as_f64().unwrap() - 0.204).abs() < 1e-12);
    checked(&params, &answer);
}

#[tokio::test]
async fn panel_seats_share_select_price_and_capability_gate() {
    let (h, route) = configured().await;
    let mut input = SelectionFacts::new();
    input.list_rates.insert(A.into(), rates());
    selection_facts(&h, &input);
    h.edit_config(|config| {
        config["panels"]["audit"] = json!({"topK":1,"by":"iq","extra":0,"floor":0})
    });
    let params =
        json!({"class":"audit","consumer":fixture::SUMMARY,"available_routes":[A],"seed":"fixed"});
    let panel = route.call("route.select_panel", &params).await;
    assert_eq!(panel.0, FrameType::Response, "{panel:?}");
    let answer: &Value = &panel.1["result"]["seats"][0]["answer"];
    assert!(answer.get("price").is_some());
    let select = answered(&route, &request(A)).await;
    assert_eq!(answer["price"], select["price"]);
    let typed = cortexkit_role_routing::panel::parse_select_panel_request_serving(
        &params,
        super::super::describe::CAPABILITIES,
    )
    .unwrap();
    cortexkit_role_routing::panel::check_panel_answer(
        &typed,
        &serde_json::from_value(panel.1["result"].clone()).unwrap(),
    )
    .unwrap();
    let off =
        super::super::describe::without_price(route.call("route.select_panel", &params)).await;
    assert!(off.1["result"]["seats"][0]["answer"].get("price").is_none());
}

#[tokio::test]
async fn answer_thresholds_use_request_size_before_context_and_profile() {
    use striatum_core::billing::{BillingPlane, BillingRate};
    let (h, route) = configured().await;
    let mut fact = h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact_to_route(fact).route_key == A)
        .unwrap();
    fact.context_window = Some(1_000_000.0);
    fact.billing_plane = Some(BillingPlane {
        provider_id: fact.provider_id.clone(),
        auth_method: "apikey".into(),
        kind: "upstream".into(),
        selection_reason: "context price test".into(),
        rates: [
            ("rate.input", 4.0),
            ("rate.output", 20.0),
            ("rate.input.above_context.200000", 40.0),
            ("rate.output.above_context.200000", 200.0),
        ]
        .into_iter()
        .map(|(key, rate)| (key.into(), BillingRate::Priced { rate }))
        .collect(),
    });
    h.install_facts(&[fact.clone()]);
    let credentials = h.credentials();
    let mut credentials = credentials.lock().unwrap();
    let catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact])
        .with_wire_apis(&credentials.wire_apis);
    credentials.publish_billing_catalog(catalog);
    drop(credentials);
    let tokens = TokenVector {
        fresh_input: 100_000.0,
        output_excluding_reasoning: 10_000.0,
        ..Default::default()
    };
    for (input, context, expected) in [
        (Some(300_000), None, 40),
        (Some(100_000), Some(300_000), 4),
        (Some(0), Some(300_000), 40),
        (None, None, 4),
        (Some(200_000), None, 4),
    ] {
        let mut params = request(A);
        if let Some(input) = input {
            params["requirements"]["input_tokens"] = json!(input);
        }
        if let Some(context) = context {
            params["requirements"]["context_window"] = json!(context);
        }
        let answer = route
            .select_with_profiles(&params, &lookup(tokens))
            .await
            .unwrap();
        assert_eq!(
            answer["price"]["rates_per_mtok"]["input"], expected,
            "{answer}"
        );
        checked(&params, &answer);
    }
    let params = request(A);
    let answer = route
        .select_with_profiles(
            &params,
            &lookup(TokenVector {
                fresh_input: 300_000.0,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(answer["price"]["rates_per_mtok"]["input"], 40);
    assert_eq!(answer["price"]["estimated_task_usd"], 12);
}
