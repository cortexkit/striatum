use super::*;
use striatum_core::{
    model_router::{
        price_selection_shortlist_with_profiles, select_model_route_with_profiles,
        RouteTaskProfiles,
    },
    task_profile::{ProfileLevel, TaskProfile, TokenVector},
};

fn cost_scaled_config() -> RoutingConfig {
    RoutingConfig {
        overqualification: OverqualificationConfig {
            mode: striatum_core::routing_config::OverqualificationMode::CostScaled,
            ..DEFAULT_OVERQUALIFICATION
        },
        ..DEFAULT_ROUTING_CONFIG
    }
}

fn task_profile(tokens: TokenVector) -> TaskProfile {
    TaskProfile {
        tokens,
        level: ProfileLevel::OwnCell,
        task_count: 30,
        own_task_count: 30,
        own_weight: 1.0,
        keeps_tokenizer: true,
    }
}

fn cost_pair(far_cost: f64, iq: f64) -> striatum_core::SelectedModelRoute {
    // One million output tokens makes the rate card's dollar cost explicit;
    // profile pricing has no legacy effectively-free bucket or cost clamp.
    let facts = vec![
        priced("far", "flagship", 1960.0, 1500.0, 100.0, far_cost * 0.05),
        priced("close", "adequate", 1000.0, 1500.0, 60.0, 110.0 * 0.05),
    ];
    let profiles: RouteTaskProfiles = facts
        .iter()
        .map(|fact| {
            (
                make_route_key(&fact.provider_id, &fact.model_id, None),
                task_profile(TokenVector {
                    output_excluding_reasoning: 1_000_000.0,
                    ..Default::default()
                }),
            )
        })
        .collect();
    select_model_route_with_profiles(
        input(facts, req(iq, 0.0), covered()),
        &cost_scaled_config(),
        None,
        &[],
        &HashMap::new(),
        &profiles,
    )
    .unwrap()
}

#[test]
fn overqualification_cost_scaled_slightly_cheaper_flagship_loses_to_adequate_model() {
    let selected = cost_pair(100.0, 10.0);
    assert_eq!(selected.route_key, "close/adequate");
    assert!((selected.score.selection_cost - 110.0).abs() < 1e-10);
    assert_eq!(selected.score.objective.overqualification, 0.0);
}

#[test]
fn overqualification_cost_scaled_much_cheaper_flagship_still_wins_with_scaled_price() {
    let selected = cost_pair(20.0, 10.0);
    assert_eq!(selected.route_key, "far/flagship");
    // Surplus 90: 1 + .05 * (2^5 - 1) = 2.55, so cost 20 becomes 51.
    assert!((selected.score.selection_cost - 51.0).abs() < 1e-10);
    assert!((selected.score.objective.overqualification - 31.0).abs() < 1e-10);
}

#[test]
fn overqualification_cost_scaled_threshold_leaves_high_tier_work_unchanged() {
    for iq in [60.0, 70.0, 80.0, 90.0, 100.0] {
        let selected = cost_pair(100.0, iq);
        assert_eq!(selected.route_key, "far/flagship");
        assert_eq!(selected.score.objective.overqualification, 0.0);
        assert!((selected.score.selection_cost - 100.0).abs() < 1e-10);
    }
}

fn replay(config: &RoutingConfig) -> Vec<serde_json::Value> {
    use serde_json::{json, Value};
    use striatum_core::subscription::SubscriptionPool;
    let fixture: Value = serde_json::from_str(include_str!(
        "../fixtures/overqualification-cost-replay.json"
    ))
    .unwrap();
    let mut records = Vec::new();
    for decision in fixture["decisions"].as_array().unwrap() {
        let consumer = decision["consumer"].as_str().unwrap();
        let iq = decision["requirements_json"]["iq"].as_f64().unwrap();
        let tokens = &fixture["task_tokens"][consumer];
        let mut profiles = RouteTaskProfiles::new();
        let mut snapshot = ModelRouteStateSnapshot {
            now: 1_000_000.0,
            ..Default::default()
        };
        let facts: Vec<_> = fixture["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                let provider = row["provider"].as_str().unwrap();
                let model = row["model"].as_str().unwrap();
                let mut fact = priced(
                    provider,
                    model,
                    row["elo"].as_f64().unwrap(),
                    1500.0,
                    row["speed"].as_f64().unwrap(),
                    row["output"].as_f64().unwrap(),
                );
                fact.input_rate = row["input"].as_f64();
                fact.cache_read_rate = row["cache_read"].as_f64();
                fact.subscription_multiplier = row["multiplier"].as_f64();
                let scale = row["task_scale"]
                    .as_f64()
                    .or_else(|| row["task_scale"][consumer].as_f64())
                    .unwrap();
                profiles.insert(
                    make_route_key(provider, model, None),
                    task_profile(TokenVector {
                        fresh_input: tokens["freshInput"].as_f64().unwrap() * scale,
                        cached_read: tokens["cachedRead"].as_f64().unwrap() * scale,
                        output_excluding_reasoning: tokens["outputExcludingReasoning"]
                            .as_f64()
                            .unwrap()
                            * scale,
                        reasoning: tokens["reasoning"].as_f64().unwrap() * scale,
                        ..Default::default()
                    }),
                );
                let used: Vec<_> = row["used"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_f64().unwrap())
                    .collect();
                let mut plan = subscription_plan("scenario", 200.0, &used);
                for account in &mut plan.accounts {
                    account.banked_resets = 0;
                }
                snapshot.by_capacity.insert(
                    make_capacity_key(provider, model),
                    RouteRuntimeState {
                        subscription_pool: Some(std::sync::Arc::new(SubscriptionPool {
                            provider: provider.into(),
                            plans: vec![plan],
                            now_ms: 1_000_000,
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                );
                fact
            })
            .collect();
        let request = || {
            let mut request = input(
                facts.clone(),
                req(iq, 0.0),
                Extra {
                    runtime_snapshot: Some(snapshot.clone()),
                    ..covered()
                },
            );
            request.target_agent = decision["target_agent"].as_str().unwrap().into();
            request
        };
        let shortlist =
            price_selection_shortlist_with_profiles(request(), config, false, false, &profiles)
                .unwrap();
        let selected = select_model_route_with_profiles(
            request(),
            config,
            None,
            &[],
            &HashMap::new(),
            &profiles,
        )
        .unwrap();
        assert!(selected.score.adequacy >= 0.95);
        if iq >= 60.0 {
            assert_eq!(selected.score.objective.overqualification, 0.0);
        }
        records.push(json!({
            "consumer":consumer, "target_agent":consumer, "requirements_json":decision["requirements_json"],
            "selected_route_key":selected.route_key, "score_json":selected.score,
            "explanation_json":selected.explanation,
            "shortlist":shortlist.routes.iter().map(|route| json!({
                "route":route.route_key, "cost":route.cost,
                "selectionCost":route.selection_cost, "overqualification":route.overqualification,
            })).collect::<Vec<_>>(),
        }));
    }
    assert_eq!(records.len(), 20);
    records
}

#[test]
fn overqualification_cost_replay_mason_and_sidekick_ledger_sweep() {
    let records = replay(&cost_scaled_config());
    println!(
        "PRICING_REPLAY={}",
        serde_json::to_string(&records).unwrap()
    );
}

#[test]
fn overqualification_default_scores_are_byte_identical_to_main_replay() {
    let baseline: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/overqualification-default-main.json"
    ))
    .unwrap();
    let actual: Vec<_> = replay(&DEFAULT_ROUTING_CONFIG)
        .into_iter()
        .map(|row| {
            serde_json::json!({"consumer":row["consumer"], "iq":row["requirements_json"]["iq"],
            "pick":row["selected_route_key"], "score":row["score_json"]})
        })
        .collect();
    assert_eq!(
        serde_json::to_vec(&actual).unwrap(),
        serde_json::to_vec(&baseline["rows"]).unwrap()
    );
}
