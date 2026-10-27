//! What `route.select` answers for a key-pool provider.
//!
//! A key-pool provider (`key_pool`) has no catalog rows of its own: its
//! credentials name the provider and `serves` a catalog provider whose rows it
//! borrows, and the key itself is leased for one send. A `routing/v1` answer
//! cannot name that leased key yet, so v1 leaves such routes out of its
//! answers while the legacy `route.select` still selects and leases them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cortexkit_role_routing_conformance::CatalogModel;
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use prefrontal_routing_core::ModelFact;
use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

/// The catalog provider whose rows the pool provider borrows.
const BORROWED: &str = "catalog-provider";
/// The key-pool provider: no rows of its own, so it is selected only through
/// the API keys that name it.
const POOL: &str = "pool-provider";
/// An ordinary route on the catalog provider.
const PLAIN: &str = "catalog-provider/model-a";

/// A catalog model on the borrowed catalog provider, cheapest first.
fn model(rank: usize) -> CatalogModel {
    CatalogModel {
        provider: BORROWED.into(),
        model: format!("model-{rank}"),
        iq: 80,
        context_window: 200_000,
        max_output_tokens: 32_000,
        tool_calls: true,
        modalities: Default::default(),
        structured_output: false,
        priced_per_token: true,
    }
}

fn facts() -> Vec<ModelFact> {
    let mut facts: Vec<ModelFact> = (0..2)
        .map(|rank| {
            let mut fact = catalog_fact(&model(rank), rank);
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
            fact.auth_methods = Some(vec!["apikey".into()]);
            fact
        })
        .collect();
    // A pool provider borrows its catalog provider's rows, and its own are
    // the cheapest, so it wins wherever the two surfaces agree.
    let mut pooled = catalog_fact(&model(0), 0);
    pooled.provider_id = POOL.into();
    pooled.cost_tier = Some(0.1);
    pooled.cost_per_m_tok_out = Some(0.1);
    pooled.auth_methods = Some(vec!["apikey".into()]);
    facts.push(pooled);
    facts
}

/// The harness's routes plus the pool provider's, with one API key whose
/// `provider_ids` name the pool provider and whose `serves` name the catalog
/// provider it borrows from.
fn harness() -> Arc<Harness> {
    let harness = Harness::new("key-pool");
    let facts = facts();
    harness.install_facts(&facts);
    let updates: Vec<_> = facts
        .iter()
        .map(|fact| FusiformFactUpdate {
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
        })
        .collect();
    harness
        .store()
        .apply_fusiform_catalog(
            &updates,
            FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: 1,
            },
        )
        .unwrap();
    let mut credentials = BrocaCredentials::api_test_credentials(&facts);
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "pool-credentials".into(),
            credentials: vec![cortexkit_claustrum_client::ListedCredential {
                id: "apikey:pool:main".into(),
                account_id: Some("pool-workspace".into()),
                categories: vec!["llm-provider".into()],
                credential_type: "apikey".into(),
                auth_method: Some("apikey".into()),
                provider_ids: vec![POOL.into()],
                serves: vec![BORROWED.into()],
                state: "active".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        crate::now_ms(),
    );
    credentials.publish_plan_prices(&json!({ "prices": [] }));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{ "module": "zeta-runner" }]);
    });
    harness
}

fn select(consumer: &str) -> Value {
    json!({ "consumer": consumer, "requirements": { "iq": 30 } })
}

fn legacy_select() -> Value {
    json!({
        "targetAgent": "mason", "consumer": "mason", "executor": "broca",
        "substrate": "broca", "requirements": {"iq": 30, "eq": 0},
    })
}

/// The route a v1 answer names, or the refusal code it answered instead.
fn v1_route(reply: &(FrameType, Value)) -> Value {
    if reply.0 == FrameType::Error {
        return json!({ "code": reply.1["code"].clone(), "reason": reply.1["detail"]["reason"].clone() });
    }
    let route = &reply.1["result"]["route"];
    json!({
        "route": format!("{}/{}", route["provider"].as_str().unwrap(), route["model"].as_str().unwrap()),
        "auth": reply.1["result"]["runner"]["auth"].clone(),
    })
}

#[tokio::test]
async fn a_key_pool_winner_is_left_out_of_the_v1_answer_and_still_answered_on_legacy() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    // The pool route is the cheapest and would win the automatic order.
    let v1 = route.call("route.select", &select("mason")).await;
    assert_eq!(v1.0, FrameType::Response, "{}", v1.1);
    assert_eq!(v1_route(&v1), json!({ "route": PLAIN, "auth": Value::Null }));

    let legacy = route.call("route.select", &legacy_select()).await;
    assert_eq!(legacy.0, FrameType::Response, "{}", legacy.1);
    assert_eq!(
        legacy.1["result"]["selected"]["routeKey"],
        format!("{POOL}/model-0"),
        "{}",
        legacy.1
    );
    // The legacy answer names the leased key, which is exactly what v1 cannot.
    assert_eq!(
        legacy.1["result"]["selected"]["harness"], "broca",
        "{}",
        legacy.1
    );
}

#[tokio::test]
async fn only_key_pool_routes_adequate_answers_the_typed_refusal_on_v1() {
    let harness = harness();
    harness.edit_config(|config| {
        // Only the one model the pool provider also sells stays adequate.
        config["models"][PLAIN]["not_for"] = json!(["mason"]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let v1 = route.call("route.select", &select("mason")).await;
    assert_eq!(v1.0, FrameType::Error, "{}", v1.1);
    assert_eq!(v1.1["code"], "no_route", "{}", v1.1);
    assert_eq!(v1.1["detail"]["reason"], "no_available_model", "{}", v1.1);
    assert!(
        v1.1["message"]
            .as_str()
            .unwrap()
            .contains("key-pool"),
        "the refusal says why: {}",
        v1.1
    );

    // The legacy shape still answers the pool route.
    let legacy = route.call("route.select", &legacy_select()).await;
    assert_eq!(legacy.0, FrameType::Response, "{}", legacy.1);
    assert_eq!(
        legacy.1["result"]["selected"]["routeKey"],
        format!("{POOL}/model-0"),
        "{}",
        legacy.1
    );
}

#[tokio::test]
async fn a_v1_pin_on_a_key_pool_route_is_not_served() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut pinned = select("mason");
    pinned["pin"] = json!(format!("{POOL}/model-0"));
    let reply = route.call("route.select", &pinned).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert!(
        reply.1["message"].as_str().unwrap().contains("key-pool"),
        "{}",
        reply.1
    );
}

#[tokio::test]
async fn a_cold_billing_plane_refuses_routing_not_ready_within_the_v1_bound() {
    let harness = harness();
    // A cold billing plane: no price has been published yet.
    harness.set_credentials(BrocaCredentials::default());
    let route = harness.bind(reserved("example-consumer"), None).await;
    let started = Instant::now();
    let reply = route.call("route.select", &select("mason")).await;
    let waited = started.elapsed();
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "routing_not_ready", "{}", reply.1);
    assert!(
        waited < Duration::from_secs(2),
        "the typed refusal must beat a caller's 3 s timeout, waited {waited:?}"
    );
}
