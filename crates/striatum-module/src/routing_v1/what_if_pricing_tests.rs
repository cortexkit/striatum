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
    let guard = crate::subscription_economics::test_pools(scenario_pools(&fixture, banked));
    (harness, guard, fixture)
}

fn scenario_pools(
    fixture: &Value,
    banked: bool,
) -> std::collections::HashMap<String, SubscriptionPool> {
    fixture["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let provider = row["provider"].as_str().unwrap();
            (
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
            )
        })
        .collect()
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

const KIMI_PROVIDER: &str = "kimi-code-plan-global";
const KIMI_ROUTE: &str = "kimi-code-plan-global/k3.1-pro";
const KIMI_CREDENTIAL: &str = "apikey:kimi-for-coding";
const KIMI_NOW: i64 = 1_795_100_907_000;

fn kimi_fact() -> striatum_core::ModelFact {
    let mut fact = super::super::conformance_tests::catalog_fact(
        &cortexkit_role_routing_conformance::CatalogModel {
            provider: KIMI_PROVIDER.into(),
            model: "k3.1-pro".into(),
            family: None,
            iq: 100,
            context_window: 200_000,
            max_output_tokens: 32_000,
            tool_calls: true,
            modalities: vec![],
            structured_output: false,
            priced_per_token: true,
        },
        3,
    );
    fact.scores.elo = 1_900.0;
    fact.scores.eq = 1_500.0;
    fact.scores.speed = 100.0;
    fact.cost_per_m_tok_out = Some(5.0);
    fact.input_rate = Some(1.0);
    fact.cache_read_rate = Some(0.1);
    fact.auth_methods = Some(vec!["apikey".into()]);
    fact
}

fn install_key_only_kimi(
    harness: &std::sync::Arc<Harness>,
) -> cortexkit_claustrum_client::ScopedCredentialListing {
    let mut facts = harness.store().get_facts().unwrap();
    facts.push(kimi_fact());
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["providers"][KIMI_PROVIDER] = json!({
            "pin_only":false, "subscription":true,
            "subscription_multiplier":20, "models":["k3.1-pro"],
        });
        let owner = fixture::CONSUMER.trim_start_matches("reserved:");
        for consumer in ["prefrontal.mason", "prefrontal.sidekick", "basal.codemode"] {
            config["consumers"][consumer] = json!({"owner":owner});
        }
    });
    let shared = harness.credentials();
    let mut credentials = shared.lock().unwrap().clone();
    let rows = credentials
        .listed_credentials()
        .iter()
        .cloned()
        .map(|mut row| {
            if row.provider_ids.iter().any(|id| id == KIMI_PROVIDER) {
                row.id = KIMI_CREDENTIAL.into();
                row.account_id = None;
                row.credential_type = "apikey".into();
                row.auth_method = Some("apikey".into());
                row.plan_tier = Some("pro_500".into());
            }
            row
        })
        .collect();
    let listing = cortexkit_claustrum_client::ScopedCredentialListing {
        view: "kimi-key-only".into(),
        credentials: rows,
        ..Default::default()
    };
    credentials.apply_listing(&listing, KIMI_NOW as u64);
    credentials.refresh_serving_methods(&Default::default(), &facts, KIMI_NOW, &[]);
    drop(shared);
    harness.set_credentials(credentials);
    listing
}

fn kimi_state(
    listing: &cortexkit_claustrum_client::ScopedCredentialListing,
    with_reading: bool,
) -> crate::account_quota::AccountQuotaState {
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_roster_listing(&mut state, listing, KIMI_NOW).unwrap();
    if with_reading {
        let mut reading: Value = serde_json::from_str(
            r#"{"provider":"kimi-for-coding","apiProvider":"kimi-code-plan-global","account":null,"credentialRef":"apikey:kimi-for-coding","source":"vault","usage":{"primary":{"usedPercent":6.21,"resetsAt":"2026-11-19T18:08:27Z","windowMinutes":10080},"secondary":{"usedPercent":0.0,"resetsAt":"2026-11-19T17:08:27Z","windowMinutes":300}}}"#,
        )
        .unwrap();
        // Account economics, like account admission, treats an undated reading
        // as unknown. Add only the producer timestamp at the frozen poll time.
        reading["fetchedAt"] = json!(crate::account_selection::rfc3339(KIMI_NOW));
        crate::account_quota::apply_usage_reply(
            &mut state,
            &serde_json::to_vec(&json!({"result":[reading]})).unwrap(),
            KIMI_NOW,
        )
        .unwrap();
    }
    state
}

fn publish_kimi_state(state: crate::account_quota::AccountQuotaState) {
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

async fn kimi_sweep(operator: &Route, consumer: &str, fixture: &Value, iqs: Vec<f64>) -> Value {
    let profile_consumer = if consumer == "prefrontal.mason" {
        "mason"
    } else {
        "sidekick"
    };
    let mut profiles = profiles(profile_consumer, fixture);
    let tokens = &fixture["task_tokens"][profile_consumer];
    profiles.insert(
        KIMI_ROUTE.into(),
        TaskProfile {
            tokens: TokenVector {
                fresh_input: tokens["freshInput"].as_f64().unwrap(),
                cached_read: tokens["cachedRead"].as_f64().unwrap(),
                output_excluding_reasoning: tokens["outputExcludingReasoning"].as_f64().unwrap(),
                reasoning: tokens["reasoning"].as_f64().unwrap(),
                ..Default::default()
            },
            level: ProfileLevel::OwnCell,
            task_count: 30,
            own_task_count: 30,
            own_weight: 1.0,
            keeps_tokenizer: true,
        },
    );
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
    operator
        .what_if_with_profiles(&json!({"consumer":consumer,"iqs":iqs}), &lookup)
        .await
        .unwrap()
}

fn route_pressure(answer: &Value, index: usize, route: &str) -> f64 {
    answer["results"][index]["adequate"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["route"] == route)
        .unwrap_or_else(|| panic!("{route} is not adequate: {}", answer["results"][index]))
        ["task_pricing"]["pressure"]
        .as_f64()
        .unwrap()
}

#[tokio::test]
async fn credential_ref_kimi_reading_drives_mason_and_effort_preview_sweeps() {
    let _clock = crate::InjectedClock::set(KIMI_NOW as u64);
    let (harness, before_pools, fixture) = setup(false);
    let listing = install_key_only_kimi(&harness);
    let operator = operator(&harness).await;
    let levels: Vec<f64> = (0..=100).map(|level| level as f64).collect();
    publish_kimi_state(kimi_state(&listing, false));
    let mut before = BTreeMap::new();
    for consumer in ["prefrontal.mason", "prefrontal.sidekick", "basal.codemode"] {
        before.insert(
            consumer,
            kimi_sweep(&operator, consumer, &fixture, levels.clone()).await,
        );
    }
    drop(before_pools);

    let after_state = kimi_state(&listing, true);
    let kimi_pool = crate::subscription_economics::test_account_pools(
        &after_state,
        &json!({"prices":[]}),
        KIMI_NOW,
    );
    assert!(kimi_pool.contains_key(KIMI_PROVIDER), "{kimi_pool:?}");
    let account = &kimi_pool[KIMI_PROVIDER].plans[0].accounts[0];
    let weekly_pressure = striatum_core::subscription::plan_pressure(
        account.weekly_remaining_fraction,
        (100.0 - account.weekly_used) / 100.0,
    );
    assert!(
        weekly_pressure > 0.02 && weekly_pressure < 0.021,
        "{weekly_pressure}"
    );
    publish_kimi_state(after_state);
    let mut pools = scenario_pools(&fixture, false);
    pools.extend(kimi_pool);
    let _after_pools = crate::subscription_economics::test_pools(pools);
    let mut changes = Vec::new();
    let mut sweep = Vec::new();
    for (consumer, before) in before {
        let after = kimi_sweep(&operator, consumer, &fixture, levels.clone()).await;
        for (index, level) in levels.iter().enumerate() {
            assert_eq!(route_pressure(&before, index, KIMI_ROUTE), 1.0);
            let pressure = route_pressure(&after, index, KIMI_ROUTE);
            assert!(
                pressure > 0.0 && pressure < 1.0,
                "{consumer} IQ{level}: {pressure}"
            );
            let before_pick = before["results"][index]["pick"]["route"].clone();
            let after_pick = after["results"][index]["pick"]["route"].clone();
            if before_pick != after_pick {
                changes.push(json!({"consumer":consumer,"level":level,
                    "before":before_pick,"after":after_pick}));
            }
            sweep.push(json!({"consumer":consumer,"level":level,
                "beforePick":before_pick,"afterPick":after_pick,
                "beforePressure":1.0,"afterPressure":pressure}));
        }
    }
    assert!(!changes.is_empty(), "the fixture must expose changed picks");
    if std::env::var_os("STRIATUM_KIMI_SWEEP").is_some() {
        println!(
            "KIMI_CREDENTIAL_REF_SWEEP={}",
            json!({"rows":sweep,"changes":changes})
        );
    }
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
