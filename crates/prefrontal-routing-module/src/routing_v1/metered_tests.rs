//! A `routing/v1` select for codemode or flow runs only on routes priced per
//! token: it takes the same route-local refusal the older select applies to
//! those consumers ([`crate::select_policy::candidate_rejections`]), so a
//! provider that publishes a subscription plan is never answered, even when
//! it is the cheapest route and the request pins it.

use std::sync::Arc;

use cortexkit_role_routing_conformance::CatalogModel;
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use prefrontal_routing_core::ModelFact;
use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

/// The provider that publishes a subscription plan; its model is the
/// cheapest, so any consumer without the priced-per-token rule picks it.
const PLAN: &str = "plan-provider/model-a";
const METERED: &str = "metered-provider/model-b";

fn model(route_key: &str) -> CatalogModel {
    let (provider, model) = route_key.split_once('/').unwrap();
    CatalogModel {
        provider: provider.into(),
        model: model.into(),
        iq: 80,
        context_window: 200_000,
        max_output_tokens: 32_000,
        tool_calls: true,
        modalities: Vec::new(),
        structured_output: false,
        priced_per_token: true,
    }
}

/// API-key routes on a metered plane with a known output price, cheapest
/// first; only the plan list and the configured subsidies tell them apart.
fn facts(route_keys: &[&str]) -> Vec<ModelFact> {
    route_keys
        .iter()
        .enumerate()
        .map(|(rank, key)| {
            let mut fact = catalog_fact(&model(key), rank);
            fact.billing_plane = Some(BillingPlane {
                provider_id: fact.provider_id.clone(),
                auth_method: "apikey".into(),
                kind: "upstream".into(),
                selection_reason: "test credential".into(),
                rates: [(
                    "rate.output".to_owned(),
                    BillingRate::Priced {
                        rate: fact.cost_per_m_tok_out.unwrap(),
                    },
                )]
                .into_iter()
                .collect(),
            });
            fact
        })
        .collect()
}

/// A harness serving both routes on one runner, with `plan_prices` as the
/// subscription plans the catalog publishes.
fn harness(plan_prices: Value) -> Arc<Harness> {
    harness_of(&[PLAN, METERED], plan_prices)
}

/// A harness serving `route_keys` (cheapest first) on one runner, with
/// `plan_prices` as the subscription plans the catalog publishes.
fn harness_of(route_keys: &[&str], plan_prices: Value) -> Arc<Harness> {
    let harness = Harness::new("metered");
    let facts = facts(route_keys);
    harness.install_facts(&facts);
    for fact in &facts {
        // Login methods come from the catalog, as they do in production.
        harness
            .store()
            .apply_fusiform_catalog(
                &[FusiformFactUpdate {
                    provider_id: fact.provider_id.clone(),
                    model_id: fact.model_id.clone(),
                    auth_methods: Some(vec!["apikey".into()]),
                    present: true,
                    context_window: fact.context_window,
                    max_output_tokens: fact.max_output_tokens,
                    cost_tier: fact.cost_tier,
                    cost_per_mtok_out: fact.cost_per_m_tok_out,
                    input_rate: None,
                    cache_read_rate: None,
                    cache_write_rate: None,
                    reasoning_rate: None,
                    supports_vision: fact.supports_vision,
                    supports_tool_use: fact.supports_tool_use,
                    reasoning_efforts: None,
                    canonical_id: None,
                    upstream_status: None,
                }],
                FusiformCatalogCursor {
                    catalog_version: 1,
                    resolved_at_ms: 1,
                },
            )
            .unwrap();
    }
    let mut credentials = BrocaCredentials::api_test_credentials(&facts);
    credentials.publish_plan_prices(&plan_prices);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{ "module": "runner-one" }]);
    });
    harness
}

fn select(consumer: &str) -> Value {
    json!({ "consumer": consumer, "requirements": { "iq": 30 } })
}

fn route_key(reply: &(FrameType, Value)) -> String {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let route = &reply.1["result"]["route"];
    format!(
        "{}/{}",
        route["provider"].as_str().unwrap(),
        route["model"].as_str().unwrap()
    )
}

#[tokio::test]
async fn magic_context_consumers_select_with_default_policy() {
    let harness = harness(json!({ "prices": [{ "provider_id": "plan-provider" }] }));
    let route = harness.bind(reserved("example-consumer"), None).await;
    // Keep this list independent of the registry so a missing default is caught.
    let consumers = [
        "mc_summary",
        "mc_dream",
        "mc_dream_verify",
        "mc_dream_map",
        "mc_dream_curate",
        "mc_dream_classify",
        "mc_dream_review_user",
        "mc_dream_retrospective",
        "mc_dream_primers",
        "mc_dream_compress_cues",
        "mc_dream_docs",
    ];
    for consumer in consumers {
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(route_key(&reply), PLAN, "{consumer}");
    }

    harness.edit_config(|config| {
        config["models"][PLAN] = json!({"pin_only": true});
    });
    for consumer in consumers {
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(route_key(&reply), METERED, "{consumer}");
    }
}

#[tokio::test]
async fn unknown_magic_context_consumer_is_refused() {
    let harness = harness(json!({ "prices": [{ "provider_id": "plan-provider" }] }));
    let route = harness.bind(reserved("example-consumer"), None).await;
    let reply = route
        .call("route.select", &select("mc_dream_unknown"))
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "unknown_consumer", "{}", reply.1);
}

#[tokio::test]
async fn codemode_and_flow_never_answer_a_subscription_plan_route() {
    let harness = harness(json!({ "prices": [{ "provider_id": "plan-provider" }] }));
    let route = harness.bind(reserved("example-consumer"), None).await;

    // Without the rule the cheaper plan route wins.
    let mason = route.call("route.select", &select("mason")).await;
    assert_eq!(route_key(&mason), PLAN);

    for consumer in ["codemode", "flow"] {
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(route_key(&reply), METERED, "{consumer}");

        let mut pinned = select(consumer);
        pinned["pin"] = json!(PLAN);
        let reply = route.call("route.select", &pinned).await;
        assert_eq!(reply.0, FrameType::Error, "{consumer}: {}", reply.1);
        assert_eq!(
            reply.1["code"], "pin_unavailable",
            "{consumer}: {}",
            reply.1
        );
    }
}

#[tokio::test]
async fn codemode_fails_closed_while_plan_membership_is_unknown() {
    // No plan list has been read: no provider is known to be plan-free.
    let harness = harness(Value::Null);
    let route = harness.bind(reserved("example-consumer"), None).await;
    let reply = route.call("route.select", &select("codemode")).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    let mason = route.call("route.select", &select("mason")).await;
    assert_eq!(route_key(&mason), PLAN);
}

/// Two plan-free metered routes: `CHEAP` has the lower list price, and the
/// operator states a subscription subsidy for `SUBSIDISED`'s provider that
/// makes it cheaper than `CHEAP` once divided in. Codemode pays list price,
/// so its select reads the facts without that subsidy and answers `CHEAP`.
#[tokio::test]
async fn codemode_ranks_routes_at_list_price_never_at_a_subscription_subsidy() {
    const CHEAP: &str = "metered-a/model-a";
    const SUBSIDISED: &str = "metered-b/model-b";
    let harness = harness_of(
        &[CHEAP, SUBSIDISED],
        json!({ "prices": [{ "provider_id": "plan-provider" }] }),
    );
    harness.edit_config(|config| {
        config["providers"]["metered-b"] = json!({ "subscription_multiplier": 40.0 });
    });
    let route = harness.bind(reserved("example-consumer"), None).await;

    // A consumer that may use a subsidy takes the subsidised route.
    let mason = route.call("route.select", &select("mason")).await;
    assert_eq!(route_key(&mason), SUBSIDISED);

    for consumer in ["codemode", "flow"] {
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(route_key(&reply), CHEAP, "{consumer}");
    }
}
