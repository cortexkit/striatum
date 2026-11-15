use super::*;
use striatum_core::{
    model_router::RouteTaskProfiles,
    subscription::{QuotaAccount, SubscriptionPlan, SubscriptionPool},
    task_profile::{ProfileLevel, TaskProfile, TokenVector},
};

const SCENARIO: &str =
    include_str!("../../../striatum-core/tests/fixtures/overqualification-cost-replay.json");

fn setup(banked: bool) -> (std::sync::Arc<Harness>, impl Drop, Value) {
    let fixture: Value = serde_json::from_str(SCENARIO).unwrap();
    let harness = Harness::new("what-if-pricing");
    let mut pools = std::collections::HashMap::new();
    let facts: Vec<_> = fixture["models"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(rank, row)| {
            let provider = row["provider"].as_str().unwrap();
            let model = row["model"].as_str().unwrap();
            let mut fact = super::super::conformance_tests::catalog_fact(
                &cortexkit_role_routing_conformance::CatalogModel {
                    provider: provider.into(),
                    model: model.into(),
                    family: None,
                    iq: 100,
                    context_window: 200_000,
                    max_output_tokens: 32_000,
                    tool_calls: true,
                    modalities: vec![],
                    structured_output: false,
                    priced_per_token: true,
                },
                rank,
            );
            fact.scores.elo = row["elo"].as_f64().unwrap();
            fact.scores.eq = 1500.0;
            fact.scores.speed = row["speed"].as_f64().unwrap();
            fact.cost_per_m_tok_out = row["output"].as_f64();
            fact.input_rate = row["input"].as_f64();
            fact.cache_read_rate = row["cache_read"].as_f64();
            pools.insert(
                provider.into(),
                SubscriptionPool {
                    provider: provider.into(),
                    now_ms: crate::now_ms() as i64,
                    plans: vec![SubscriptionPlan {
                        tier: Some("scenario".into()),
                        plan: None,
                        measured: None,
                        fee_usd: 200.0,
                        accounts: row["used"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|used| QuotaAccount {
                                weekly_used: used.as_f64().unwrap(),
                                weekly_remaining_fraction: 0.5,
                                hours_to_reset: 84.0,
                                short_walled: false,
                                short_windows: vec![],
                                banked_resets: if banked { 2 } else { 0 },
                                banked_expires_at_ms: Some(2_000_000),
                                banked_credit_expiries_ms: vec![],
                                prepaid_credits: false,
                            })
                            .collect(),
                    }],
                    ..Default::default()
                },
            );
            fact
        })
        .collect();
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca"}]);
        for row in fixture["models"].as_array().unwrap() {
            config["providers"][row["provider"].as_str().unwrap()] = json!({
                "pin_only":false, "subscription":true,
                "subscription_multiplier":row["multiplier"], "models":[row["model"]],
            });
        }
    });
    let guard = crate::subscription_economics::test_pools(pools);
    (harness, guard, fixture)
}

fn profiles(consumer: &str, fixture: &Value) -> RouteTaskProfiles {
    let tokens = &fixture["task_tokens"][consumer];
    fixture["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let scale = row["task_scale"]
                .as_f64()
                .or_else(|| row["task_scale"][consumer].as_f64())
                .unwrap();
            (
                format!(
                    "{}/{}",
                    row["provider"].as_str().unwrap(),
                    row["model"].as_str().unwrap()
                ),
                TaskProfile {
                    tokens: TokenVector {
                        fresh_input: tokens["freshInput"].as_f64().unwrap() * scale,
                        cached_read: tokens["cachedRead"].as_f64().unwrap() * scale,
                        output_excluding_reasoning: tokens["outputExcludingReasoning"]
                            .as_f64()
                            .unwrap()
                            * scale,
                        reasoning: tokens["reasoning"].as_f64().unwrap() * scale,
                        ..Default::default()
                    },
                    level: ProfileLevel::OwnCell,
                    task_count: 30,
                    own_task_count: 30,
                    own_weight: 1.0,
                    keeps_tokenizer: true,
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn what_if_pricing_patch_reproduces_cost_scaled_after_table_without_live_writes() {
    // A frozen clock preserves the replay's half-week quota horizon for the
    // entire sweep, including the unpatched control after the experiment.
    let _clock = crate::InjectedClock::set(1_000_000);
    let (harness, _pools, fixture) = setup(false);
    let operator = operator(&harness).await;
    let before_rows = rows(&harness);
    let before_config = harness.operator_config();
    let before_files: BTreeMap<_, _> = std::fs::read_dir(harness.store_path().parent().unwrap())
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().and_then(|x| x.to_str()) == Some("jsonc"))
                .then(|| (path.clone(), std::fs::read(path).unwrap()))
        })
        .collect();
    let writes = harness.store().account_write_transaction_count_for_test();
    for consumer in ["mason", "sidekick"] {
        let profiles = profiles(consumer, &fixture);
        let lookup = move |_: &str, _: f64, facts: &[striatum_core::ModelFact]| {
            crate::select::TaskProfileLookup {
                profiles: facts
                    .iter()
                    .filter_map(|fact| {
                        profiles
                            .get(&format!("{}/{}", fact.provider_id, fact.model_id))
                            .map(|profile| {
                                (
                                    striatum_core::fact_to_route(fact).route_key,
                                    profile.clone(),
                                )
                            })
                    })
                    .collect(),
                providers_without_cache_write_reports: Default::default(),
            }
        };
        let request = json!({"consumer":consumer,"iqs":[10,20,30,40,50,60,70,80,90,100]});
        let control = operator
            .what_if_with_profiles(&request, &lookup)
            .await
            .unwrap();
        let mut request = request.clone();
        request["pricing_patch"] = json!({"overqualification":"cost_scaled"});
        let after = operator
            .what_if_with_profiles(&request, &lookup)
            .await
            .unwrap();
        for (index, row) in after["results"].as_array().unwrap().iter().enumerate() {
            let expected = if index < 6 && (consumer == "mason" || index > 0) {
                "google/antigravity-gemini-3.8-flash"
            } else {
                "anthropic/claude-opus-5-5"
            };
            assert_eq!(
                row["pick"]["route"],
                expected,
                "{consumer} IQ{}: {row}",
                10 * (index + 1)
            );
            assert_eq!(
                control["results"][index]["pick"]["route"], "anthropic/claude-opus-5-5",
                "{consumer} control: {control}"
            );
        }
        request.as_object_mut().unwrap().remove("pricing_patch");
        assert_eq!(
            control["results"],
            operator
                .what_if_with_profiles(&request, &lookup)
                .await
                .unwrap()["results"]
        );
    }
    assert_eq!(before_rows, rows(&harness));
    assert_eq!(
        writes,
        harness.store().account_write_transaction_count_for_test()
    );
    assert!(
        std::sync::Arc::ptr_eq(&before_config, &harness.operator_config()),
        "preview must not replace the live cache"
    );
    for (path, bytes) in before_files {
        assert_eq!(bytes, std::fs::read(path).unwrap());
    }
}

#[tokio::test]
async fn what_if_banked_capacity_patch_is_provider_scoped_and_does_not_publish() {
    let _clock = crate::InjectedClock::set(1_000_000);
    let (harness, _pools, fixture) = setup(true);
    let operator = operator(&harness).await;
    let profiles = profiles("mason", &fixture);
    let lookup = move |_: &str, _: f64, facts: &[striatum_core::ModelFact]| {
        crate::select::TaskProfileLookup {
            profiles: facts
                .iter()
                .filter_map(|fact| {
                    profiles
                        .get(&format!("{}/{}", fact.provider_id, fact.model_id))
                        .map(|profile| {
                            (
                                striatum_core::fact_to_route(fact).route_key,
                                profile.clone(),
                            )
                        })
                })
                .collect(),
            providers_without_cache_write_reports: Default::default(),
        }
    };
    let before = rows(&harness);
    let live = harness.operator_config();
    let request = json!({"consumer":"mason","iqs":[10]});
    let control = operator
        .what_if_with_profiles(&request, &lookup)
        .await
        .unwrap();
    let mut patched = request.clone();
    patched["pricing_patch"] =
        json!({"overqualification":"cost_scaled", "banked_resets":{"openai":"capacity"}});
    let after = operator
        .what_if_with_profiles(&patched, &lookup)
        .await
        .unwrap();
    let price = |answer: &Value, provider: &str| {
        answer["results"][0]["adequate"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["route"].as_str().unwrap().starts_with(provider))
            .unwrap()["cost_usd"]
            .as_f64()
            .unwrap_or_else(|| panic!("missing task dollars: {answer}"))
    };
    assert!(price(&after, "openai/") < price(&control, "openai/"));
    assert_eq!(price(&after, "anthropic/"), price(&control, "anthropic/"));
    assert_eq!(after["results"][0]["pick"]["route"], "openai/gpt-6.1-sol");
    assert_eq!(
        control["results"],
        operator
            .what_if_with_profiles(&request, &lookup)
            .await
            .unwrap()["results"]
    );
    assert_eq!(before, rows(&harness));
    assert!(std::sync::Arc::ptr_eq(&live, &harness.operator_config()));
    assert!(!harness
        .operator_config()
        .pricing()
        .banked_resets_capacity
        .contains("openai"));
}
