//! What `route.select` answers for a key-pool provider.
//!
//! A key-pool provider (`key_pool`) is one an API key names but the catalog
//! does not: its routes are the rows it borrows from the one catalog provider
//! its active keys `serves`, and the key a send runs on is leased per send. A
//! `routing/v1` answer cannot name that leased key yet, so v1 leaves such
//! routes out of its answers while the legacy `route.select` still selects and
//! leases them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use cortexkit_role_routing_conformance::CatalogModel;
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use prefrontal_routing_core::ModelFact;
use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

/// The catalog provider whose rows the key pool borrows.
const BORROWED: &str = "catalog-provider";
/// The key-pool provider: no catalog rows of its own.
const POOL: &str = "pool-provider";
/// The catalog provider's own route.
const PLAIN: &str = "catalog-provider/model-a";
/// A second catalog provider that the key pool borrows from. It sells nothing
/// of its own; every row it has comes from the pool key's `serves`.
const POOL_VENDOR: &str = "pool-vendor";
/// The one route `POOL_VENDOR` sells, which the key pool borrows and is the
/// cheapest route of all.
const POOL_ROUTE: &str = "pool-vendor/model-pool";

fn model(route_key: &str, iq: u8) -> CatalogModel {
    let (provider, model) = route_key.split_once('/').unwrap();
    CatalogModel {
        provider: provider.into(),
        model: model.into(),
        iq,
        context_window: 200_000,
        max_output_tokens: 32_000,
        tool_calls: true,
        modalities: Default::default(),
        structured_output: false,
        priced_per_token: true,
    }
}

/// The catalog's rows: the catalog provider's own, and the row the key pool
/// borrows from `POOL_VENDOR`.
fn catalog_models() -> Vec<CatalogModel> {
    // `catalog_fact` prices each row by its rank, so the first entry is the
    // cheapest. The key pool's route is therefore both the cheapest and the
    // ablest: it wins wherever it is allowed to decide, and it is the only
    // route a high demand can use.
    vec![model(POOL_ROUTE, 95), model(PLAIN, 40)]
}

/// One routing fact per catalog row, each with a billing plane.
fn routing_facts() -> Vec<ModelFact> {
    catalog_models()
        .iter()
        .enumerate()
        .map(|(rank, model)| {
            let mut fact = catalog_fact(model, rank);
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
            // The key pool's route is the cheaper of the two, so cost alone
            // would choose it wherever nothing stops it.
            if fact.model_id.ends_with("model-pool") {
                fact.cost_tier = Some(0.5);
                fact.cost_per_m_tok_out = Some(0.5);
                if let Some(plane) = fact.billing_plane.as_mut() {
                    plane.rates.insert(
                        "rate.output".to_owned(),
                        BillingRate::Priced { rate: 0.5 },
                    );
                }
            }
            fact
        })
        .collect()
}

fn key(id: &str, providers: &[&str], serves: &[&str]) -> ListedCredential {
    ListedCredential {
        id: id.into(),
        account_id: Some(format!("{id}-workspace")),
        categories: vec!["llm-provider".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        provider_ids: providers.iter().map(|p| (*p).into()).collect(),
        serves: serves.iter().map(|p| (*p).into()).collect(),
        state: "active".into(),
        ..Default::default()
    }
}

/// Routing with a catalog provider, its own key, and a key pool: the pool key
/// names the pool provider and `serves` the catalog provider whose rows the
/// pool borrows.
fn harness() -> Arc<Harness> {
    let harness = Harness::new("key-pool");
    let facts = routing_facts();
    harness.install_facts(&facts);
    let updates: Vec<_> = facts
        .iter()
        .map(|fact| FusiformFactUpdate {
            provider_id: fact.provider_id.clone(),
            model_id: fact.model_id.clone(),
            auth_methods: fact.auth_methods.clone(),
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
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "key-pool-listing".into(),
            credentials: vec![
                key("apikey:catalog:main", &[BORROWED], &[BORROWED]),
                key("apikey:pool-vendor:main", &[POOL], &[POOL_VENDOR]),
            ],
            ..Default::default()
        },
        crate::now_ms(),
    );
    credentials.refresh_serving_methods(
        &Default::default(),
        &facts,
        crate::now_ms() as i64,
        &[],
    );
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
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

fn answered_route(reply: &(FrameType, Value)) -> String {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let route = &reply.1["result"]["route"];
    format!(
        "{}/{}",
        route["provider"].as_str().unwrap(),
        route["model"].as_str().unwrap()
    )
}

fn legacy_route(reply: &(FrameType, Value)) -> String {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"]["selected"]["routeKey"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn a_key_pool_winner_is_left_out_of_the_v1_answer() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let answered = answered_route(&route.call("route.select", &select("mason")).await);
    assert!(
        !answered.starts_with(&format!("{POOL}/")) && !answered.contains("model-pool"),
        "a key-pool route must not be answered on v1, got {answered}"
    );
    assert_eq!(answered, PLAIN, "the catalog provider's own route wins");
}

#[tokio::test]
async fn the_legacy_surface_answers_the_key_pool_route() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    // A pin names the route outright, so the legacy surface must serve it: it
    // selects and leases a pool key for exactly this case.
    let mut legacy = legacy_select();
    legacy["modelPin"] = json!(format!("{POOL_VENDOR}/model-pool"));
    let legacy = route.call("route.select", &legacy).await;
    assert_eq!(
        legacy_route(&legacy),
        format!("{POOL}/model-pool"),
        "the legacy shape leases a pool key for a pinned key-pool route: {}",
        legacy.1
    );
}

#[tokio::test]
async fn a_v1_pin_on_a_key_pool_route_is_not_served() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut pinned = select("mason");
    pinned["pin"] = json!(format!("{POOL}/model-pool"));
    let reply = route.call("route.select", &pinned).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert!(
        reply.1["message"].as_str().unwrap().contains("key-pool")
            || reply.1["code"] == "pin_unavailable",
        "a pin on a key-pool route is never served: {}",
        reply.1
    );
}

#[tokio::test]
async fn a_slow_billing_plane_answers_routing_not_ready_within_the_v1_bound() {
    let harness = harness();
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
