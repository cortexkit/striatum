//! Regression tests on two captured Claude subscription states (11 and 17
//! October 2026), run through `route.select`. They pin the 30% keep-free
//! margin: heads keep access below it, unpinned workers stop at it, an IQ-85
//! worker may use all quota above it, and the paid API plane stays metered.
//! The all-accounts-at-margin refusal is checked at selector level, with saved
//! resets switched off as in the original capture, and a separate test pins
//! today's answer, which spends one saved reset instead.

use std::{collections::BTreeMap, sync::Arc};

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::{json, Value};
use striatum_core::{
    billing::{BillingPlane, BillingRate},
    subscription::SubscriptionPool,
    ClassVector, ModelFact,
};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use crate::account_quota::{AccountQuotaState, RosterIdentity, StoredReading, VaultRoster};

const MODEL: &str = "anthropic/claude-opus-5-5";
const WORKER: &str = "prefrontal-core.mason";

fn pi_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../fixtures/pi-mason-subscription-reserve.json"
    ))
    .unwrap()
}

fn october_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../fixtures/claude-head-reserve-2026-10-17.json"
    ))
    .unwrap()
}

fn model_fact(api: bool) -> ModelFact {
    // Captured account readings are priced against this fixed task vector:
    // 1,000 output and 1,000 cached-read tokens, with no fresh input or reasoning.
    let method = if api { "apikey" } else { "oauth" };
    let mut fact: ModelFact = serde_json::from_value(json!({
        "providerID":"anthropic", "modelID":"claude-opus-5-5", "confidence":"curated", "family":"claude",
        "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "costPerMTokOut":20.0
    })).unwrap();
    fact.class_vector = Some(ClassVector {
        workload_class: "ephemeral".into(),
        sample_count: 50,
        trailing_window_days: 7.0,
        statistic: "mean".into(),
        is_overlay: true,
        fresh_input: 0.0,
        output_excluding_reasoning: 1000.0,
        reasoning: 0.0,
        cached_read: 1000.0,
        cache_write: 0.0,
    });
    fact.class_vector_is_overlay = true;
    fact.auth_methods = Some(vec![method.into()]);
    fact.login_methods_documented = true;
    fact.billing_plane = Some(BillingPlane {
        provider_id: "anthropic".into(),
        auth_method: method.into(),
        kind: if api { "upstream" } else { "quota_proxy" }.into(),
        selection_reason: "roster".into(),
        rates: BTreeMap::from([
            ("rate.output".into(), BillingRate::Priced { rate: 20.0 }),
            ("rate.cache_read".into(), BillingRate::Priced { rate: 0.25 }),
        ]),
    });
    fact
}

fn setup(fixture: &Value, api: bool) -> (Arc<Harness>, impl Drop, SubscriptionPool) {
    let harness = Harness::new("subscription-incident");
    // Match the original one-model shortlist, not migration-seeded catalog rows.
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    let now = fixture["now_ms"].as_i64().unwrap();
    assert_eq!(crate::now_ms(), now as u64);
    let mut state = AccountQuotaState::default();
    let mut identities = Vec::new();
    for (index, account) in fixture["accounts"].as_array().unwrap().iter().enumerate() {
        let account_id = format!("anonymous-{index}");
        let credential = format!("fixture:{index}");
        identities.push(RosterIdentity {
            refresh_adapter: "anthropic".into(),
            account_id: account_id.clone(),
            credential_ids: vec![credential.clone()],
            active_credential_ids: vec![credential],
            first_seen_ms: now - 86_400_000,
            plan_tier: Some("max_20x".into()),
        });
        state.readings.readings.insert(("claude".into(), account_id.clone()), StoredReading {
            provider: "claude".into(), account_id: account_id.clone(),
            entry: json!({"provider":"claude", "apiProvider":"anthropic", "account":account_id,
                "savedResets":account["saved_resets"], "fetchedAt":fixture["fetched_at"],
                "usage":{"secondary":{"usedPercent":account["used_percent"],
                    "windowKind":"weekly", "windowMinutes":10080, "resetsAt":account["resets_at"]}}}),
            received_at_ms: now, tombstoned_at_ms: None,
        });
    }
    state.roster = Some(VaultRoster {
        view: "incident".into(),
        fetched_at_ms: now,
        grants: Vec::new(),
        identities,
    });
    let prices =
        serde_json::from_str(include_str!("../../fixtures/fusiform-plan-prices.json")).unwrap();
    let mut pools = crate::subscription_economics::test_account_pools(&state, &prices, now);
    let pool = pools.get_mut("anthropic").unwrap();
    pool.keep_free_percent = fixture["keep_free_percent"].as_f64().unwrap();
    let pool = pool.clone();

    let method = if api { "apikey" } else { "oauth" };
    let fact = model_fact(api);
    harness.install_facts(&[fact.clone()]);
    super::account_classes_tests::catalog_fact(&harness, &fact);
    install_profiles(&fact, now);
    let mut credentials = crate::BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "incident".into(),
            credentials: state
                .roster
                .as_ref()
                .unwrap()
                .identities
                .iter()
                .flat_map(|identity| {
                    identity.credential_ids.iter().map(|id| ListedCredential {
                        id: id.clone(),
                        account_id: Some(identity.account_id.clone()),
                        refresh_adapter: Some("anthropic".into()),
                        provider_ids: vec!["anthropic".into()],
                        credential_type: method.into(),
                        auth_method: Some(method.into()),
                        categories: vec!["llm-provider".into()],
                        state: "active".into(),
                        ..Default::default()
                    })
                })
                .collect(),
            ..Default::default()
        },
        now as u64,
    );
    credentials.refresh_serving_methods(&state, &[fact.clone()], now, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact]),
    );
    credentials.publish_plan_prices(&prices);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["anthropic"]["keep_free_percent"] =
            fixture["keep_free_percent"].clone();
        config["head_agents"] = json!(["head"]);
        config["consumers"] = json!({
            WORKER:{"owner":"prefrontal-core", "mode":"demand"},
            "head":{"owner":"prefrontal-core", "mode":"demand"}
        });
        config["runners"] = json!([{"module":"broca", "via_vault":true}]);
    });
    (
        harness,
        crate::subscription_economics::test_pools(pools),
        pool,
    )
}

async fn bind(harness: &Arc<Harness>, host: Option<&str>) -> Route {
    let channel = harness.reserve_channel();
    let request = json!({
        "op":"route.bind", "route_channel":channel, "epoch":1,
        "target":{"kind":"management_surface", "module_id":"striatum"},
        "identity":{"project_root":cortexkit_test_support::scratch_root(), "session":format!("incident-{channel}"), "harness":host.unwrap_or("unhosted")},
        "principal":reserved("prefrontal-core")
    });
    harness.bind_frame(request, channel, 1).await
}

async fn select(
    harness: &Arc<Harness>,
    host: Option<&str>,
    consumer: &str,
    requirements: Value,
) -> (FrameType, Value) {
    // Host identity is bound, not an execution choice or OAuth credential claim.
    // No model is explicitly chosen by caller/settings to waive the margin.
    bind(harness, host)
        .await
        .call(
            "route.select",
            &json!({
                "consumer":consumer, "requirements":requirements,
                "available_routes":[MODEL], "detail":true
            }),
        )
        .await
}

fn subscription(reply: &(FrameType, Value)) -> &Value {
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["route"]["provider"], "anthropic");
    assert_eq!(reply.1["result"]["route"]["model"], "claude-opus-5-5");
    let task = &reply.1["result"]["detail"]["score"]["objective"]["taskPricing"];
    assert_eq!(task["profile"]["taskCount"], 50);
    assert_eq!(
        task["tokens"],
        json!({
            "freshInput":0.0, "cachedRead":1000.0, "cacheWrite":0.0,
            "outputExcludingReasoning":1000.0, "reasoning":0.0
        })
    );
    let pricing = &reply.1["result"]["detail"]["score"]["objective"]["subscriptionPricing"];
    assert!(pricing.is_object(), "{reply:?}");
    pricing
}

#[tokio::test]
async fn pi_masons_share_corrected_incident_subscription_admission_with_opencode() {
    let fixture = pi_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    for decision in fixture["decisions"].as_array().unwrap() {
        let mut selections = Vec::new();
        for host in [Some("pi"), Some("opencode"), None] {
            let (harness, _pools, pool) = setup(&fixture, false);
            let detail: Value =
                serde_json::from_str(&pool.reserve_diagnostic(20.0, false)).unwrap();
            assert_eq!(detail["pool"], "anthropic");
            assert_eq!(detail["unit"], "weekly_percentage_points_per_account");
            for (key, value) in fixture["expected_plan"].as_object().unwrap() {
                assert_eq!(&detail["plans"][0][key], value, "{key}");
            }
            // V1 encodes these unchanged whole-number demands as u8, not floats.
            let iq = decision["requirements"]["iq"].as_f64().unwrap();
            let eq = decision["requirements"]["eq"].as_f64().unwrap();
            assert_eq!(iq.fract(), 0.0);
            assert_eq!(eq.fract(), 0.0);
            let reply = select(
                &harness,
                host,
                WORKER,
                json!({"iq":iq as u8, "eq":eq as u8}),
            )
            .await;
            let pricing = subscription(&reply);
            assert_eq!(pricing["availablePoints"], 166.0, "{reply:?}");
            selections.push((reply.1["result"]["route"].clone(), pricing.clone()));
        }
        assert_eq!(selections[0], selections[1]);
        assert_eq!(selections[0], selections[2]);
    }
}

fn reserve_refusal(reply: &(FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert_eq!(reply.1["code"], "no_route", "{reply:?}");
    let rejected = reply.1["detail"]["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 1, "{reply:?}");
    assert_eq!(rejected[0]["routeKey"], MODEL);
    assert_eq!(rejected[0]["reason"], "subscription_reserve");
    let detail: Value = serde_json::from_str(rejected[0]["detail"].as_str().unwrap()).unwrap();
    assert!(detail["rule"]
        .as_str()
        .unwrap()
        .contains("keep-free margin"));
    assert_eq!(detail["plans"][0]["availablePoints"], 0.0);
    assert!(detail.get("headPace").is_none());
    for removed in [
        "headPointsPerHour",
        "headReservePoints",
        "headReserveSteps",
        "tightestPoint",
        "resetHorizonHours",
    ] {
        assert!(detail["plans"][0].get(removed).is_none());
    }
    detail
}

#[tokio::test]
async fn incident_subscription_heads_keep_access_and_subscription_pricing() {
    let fixture = pi_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    for host in ["pi", "opencode", "claude-code"] {
        let (harness, _pools, _) = setup(&fixture, false);
        assert!(harness.operator_config().is_head_agent("head"));
        assert!(!harness.operator_config().is_head_agent(WORKER));
        let reply = select(&harness, Some(host), "head", json!({"iq":85,"eq":15})).await;
        assert_eq!(subscription(&reply)["availablePoints"], 232.0);
    }

    // Every account is below the live 30% margin, not exhausted. The same
    // readings admit a head, but no unpinned worker: classification must matter.
    let mut below_margin = fixture;
    below_margin["keep_free_percent"] = json!(30.0);
    for account in below_margin["accounts"].as_array_mut().unwrap() {
        account["used_percent"] = json!(85.0);
    }
    for host in ["pi", "opencode", "claude-code"] {
        let (harness, _pools, _) = setup(&below_margin, false);
        let head = select(&harness, Some(host), "head", json!({"iq":85,"eq":15})).await;
        assert_eq!(subscription(&head)["availablePoints"], 75.0);
        let worker = select(&harness, Some(host), WORKER, json!({"iq":85,"eq":15})).await;
        reserve_refusal(&worker);
    }
}

#[tokio::test]
async fn incident_subscription_workers_get_fixed_margin_subscription_pricing() {
    let fixture = pi_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    for (margin, expected) in [(20.0, 166.0), (30.0, 136.0)] {
        for host in ["pi", "opencode"] {
            let mut fixture = fixture.clone();
            fixture["keep_free_percent"] = json!(margin);
            let (harness, _pools, _) = setup(&fixture, false);
            let reply = select(&harness, Some(host), WORKER, json!({"iq":85,"eq":15})).await;
            assert_eq!(subscription(&reply)["availablePoints"], expected);
            assert_eq!(
                reply.1["result"]["detail"]["score"]["pricingMode"],
                "basket"
            );
        }
    }
}

#[tokio::test]
async fn incident_api_plane_is_still_metered_not_subscription_reserved() {
    let fixture = pi_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    {
        // The OAuth control makes the plane distinction observable: its budget
        // includes the reserve, while the API-key plane below must be metered.
        let (harness, _pools, _) = setup(&fixture, false);
        let oauth = select(&harness, Some("pi"), WORKER, json!({"iq":85,"eq":15})).await;
        assert_eq!(subscription(&oauth)["availablePoints"], 166.0);
    }
    let (harness, _pools, _) = setup(&fixture, true);
    let api = select(&harness, Some("pi"), WORKER, json!({"iq":85,"eq":15})).await;
    assert_eq!(api.0, FrameType::Response, "{api:?}");
    assert_eq!(api.1["result"]["route"]["provider"], "anthropic");
    assert_eq!(api.1["result"]["route"]["model"], "claude-opus-5-5");
    assert!(
        api.1["result"]["detail"]["score"]["objective"]["subscriptionPricing"].is_null(),
        "{api:?}"
    );
    // Metered tasks have no subscription coverage or quota-pressure multiplier;
    // the final task cost equals the list cost.
    let objective = &api.1["result"]["detail"]["score"]["objective"];
    // The false subscriptionCovered flag is omitted by its wire serializer.
    assert!(objective.get("subscriptionCovered").is_none());
    assert_eq!(objective["taskPricing"]["pressure"], 1.0);
    assert_eq!(objective["taskPricing"]["listCostUsd"], 0.02025);
    assert_eq!(objective["taskPricing"]["finalCostUsd"], 0.02025);
}

#[tokio::test]
async fn october_17_iq85_worker_can_use_all_live_quota_above_fixed_margin() {
    let fixture = october_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    for (margin, expected) in [(0.0, 256.0), (10.0, 223.0), (20.0, 193.0), (30.0, 163.0)] {
        for host in [Some("pi"), Some("opencode"), None] {
            let mut fixture = fixture.clone();
            fixture["keep_free_percent"] = json!(margin);
            let (harness, _pools, pool) = setup(&fixture, false);
            assert!(!pool.allow_banked);
            assert_eq!(
                pool.plans[0]
                    .accounts
                    .iter()
                    .map(|account| account.banked_resets)
                    .sum::<u32>(),
                4
            );
            let reply = select(&harness, host, WORKER, json!({"iq":85,"eq":0})).await;
            let pricing = subscription(&reply);
            assert_eq!(pricing["availablePoints"], expected, "{reply:?}");
            assert_eq!(pricing["keepFreePercent"], margin);
            assert_eq!(pricing["bankedResetsApplied"], 0);
            for removed in ["headReservePoints", "headReserveSteps", "tightestPoint"] {
                assert!(pricing.get(removed).is_none());
            }
        }
    }
}

#[test]
fn iq85_worker_is_refused_only_when_every_live_account_is_at_margin() {
    use std::collections::HashMap;
    use striatum_core::model_router::{
        select_model_route, RouteRuntimeState, SelectModelRouteInput,
    };
    use striatum_core::{ModelRequirements, ModelRouteStateSnapshot};

    let fixture = october_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    for (margin, used) in [(0.0, 100.0), (20.0, 80.0), (30.0, 70.0)] {
        let mut fixture = fixture.clone();
        fixture["keep_free_percent"] = json!(margin);
        for account in fixture["accounts"].as_array_mut().unwrap() {
            account["used_percent"] = json!(used);
        }
        let (_harness, _pools, pool) = setup(&fixture, false);
        // This pins the original live-only selector contract: captured saved
        // resets exist, but allow_banked is false. The separate real-v1 test
        // pins the module's emergency-pricing policy instead of faking it here.
        assert!(!pool.allow_banked);
        let input = SelectModelRouteInput {
            target_agent: WORKER.into(),
            target_is_head: false,
            requirements: ModelRequirements { iq: 85.0, eq: 0.0 },
            available_models: [MODEL.into()].into_iter().collect(),
            facts: vec![model_fact(false)],
            runtime_snapshot: Some(ModelRouteStateSnapshot {
                now: fixture["now_ms"].as_f64().unwrap(),
                by_capacity: HashMap::from([(
                    MODEL.into(),
                    RouteRuntimeState {
                        subscription_pool: Some(Arc::new(pool)),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }),
            exclude_route_keys: vec![],
            operator_exclude_keys: vec![],
            required_capabilities: None,
            is_subscription_route: Some(Box::new(|_| false)),
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: Default::default(),
            providers_without_cache_write_reports: Default::default(),
        };
        let refusal = select_model_route(input).unwrap_err();
        assert_eq!(refusal.rejected.len(), 1);
        assert_eq!(refusal.rejected[0].route_key, MODEL);
        assert_eq!(refusal.rejected[0].reason, "subscription_reserve");
        let detail: Value =
            serde_json::from_str(refusal.rejected[0].detail.as_deref().unwrap()).unwrap();
        assert!(detail["rule"]
            .as_str()
            .unwrap()
            .contains("keep-free margin"));
        assert_eq!(detail["plans"][0]["availablePoints"], 0.0);
        assert!(detail.get("headPace").is_none());
        for removed in [
            "headPointsPerHour",
            "headReservePoints",
            "headReserveSteps",
            "tightestPoint",
            "resetHorizonHours",
        ] {
            assert!(detail["plans"][0].get(removed).is_none());
        }
    }
}

fn install_profiles(fact: &ModelFact, now: i64) {
    // V1 reads task profiles rather than the old in-memory class-vector field.
    // Preserve its 50 samples and exact 1,000 output + 1,000 cached-read tokens.
    let mut rows = Vec::new();
    for consumer in [WORKER, "head"] {
        for bucket in [7, 8] {
            for sample in 0..50 {
                rows.push(striatum_store::ClosedTaskUsage {
                    workload: consumer.into(),
                    task_ref: format!("{consumer}-{bucket}-{sample}"),
                    provider_id: Some("anthropic".into()),
                    selected_model_id: Some("claude-opus-5-5".into()),
                    selected_variant: None,
                    iq_bucket: Some(bucket),
                    multi_model: false,
                    multi_provider: false,
                    totals: striatum_store::TaskTokenTotals {
                        fresh_input: Some(0.0),
                        cached_read: Some(1000.0),
                        cache_write: Some(0.0),
                        output_excluding_reasoning: Some(1000.0),
                        reasoning: Some(0.0),
                    },
                    closed_at_ms: now - 86_400_000,
                });
            }
        }
    }
    crate::task_profile_refresh::publish_snapshot_for_test(
        crate::task_profile_refresh::build_snapshot(&rows, &[fact.clone()]),
    );
}

#[tokio::test]
async fn october_17_v1_emergency_answer_prices_one_saved_reset() {
    let mut fixture = october_fixture();
    let _clock = crate::InjectedClock::set(fixture["now_ms"].as_u64().unwrap());
    fixture["keep_free_percent"] = json!(30.0);
    for account in fixture["accounts"].as_array_mut().unwrap() {
        account["used_percent"] = json!(100.0);
    }
    // Unlike the live-only selector test, the real module recomputes banked
    // eligibility. With no other provider headroom it may price one saved reset.
    let (harness, _pools, _) = setup(&fixture, false);
    let reply = select(&harness, Some("pi"), WORKER, json!({"iq":85,"eq":0})).await;
    let pricing = subscription(&reply);
    assert_eq!(pricing["bankedResetsApplied"], 1);
    assert_eq!(pricing["availablePoints"], 70.0);
    assert_eq!(pricing["keepFreePercent"], 30.0);
    let task_price = &reply.1["result"]["detail"]["score"]["objective"]["taskPricing"];
    assert_eq!(task_price["listCostUsd"], 0.02025);
    assert_eq!(task_price["pressure"], 30.50845742562898);
    assert_eq!(task_price["finalCostUsd"], 0.6177962628689868);
    // resolve_price_rates omits the public quote for unmeasured subscription
    // value (price_rates.rs:74-80); the unchanged fixture also has no input rate.
    assert!(reply.1["result"].get("price").is_none());
}
