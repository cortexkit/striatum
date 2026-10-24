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

/// Both routes are API-key routes on a metered plane with a known output
/// price; only the plan list tells them apart.
fn facts() -> Vec<ModelFact> {
    [PLAN, METERED]
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
    let harness = Harness::new("metered");
    let facts = facts();
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
        config["model_routing"]["runners"] = json!([{ "module": "runner-one" }]);
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
