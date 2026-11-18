//! A `no_route` refusal reads the account recovery of every walled catalog
//! route it may name. These tests pin that refusal's exact bytes on fixtures
//! with several walled providers and many routes, count the private recovery
//! views one refusal builds, and measure its CPU on the production-sized
//! fixture.

use super::conformance_tests::{reserved, Route};
use super::request_cpu_bench::{build_fixture, cpu_us, RequestFixture, FIXTURE_NOW};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::RouteStateStore;

/// Four providers of eighty routes each, four accounts per provider.
const FACTS: usize = 320;
const CREDENTIALS: usize = 16;

/// The refusal bodies these fixtures produced before one recovery view was
/// shared across a refusal's routes, captured from the code that built one
/// view per route. A length and SHA-256 digest stand in for each full body.
const ALL_WALLED: (usize, &str) = (
    290,
    "7740673ee08be6fa11a542362aec834473e0ba73450d1e8805b098bfe5396a13",
);
const ONE_PROVIDER_COOLING: (usize, &str) = (
    4291,
    "b764f4478b6d40a78122171d57637919e39cb0e7b308c37a34911d91e5ce890c",
);

/// Each provider's accounts reset at a different hour, so a refusal must name
/// the earliest.
fn provider_reset(provider: i64) -> i64 {
    FIXTURE_NOW as i64 + (provider + 1) * 3_600_000
}

/// Every account at its weekly limit, except those of `spared` (a provider
/// number), which keep the fixture's readings. The fixture's billing fences
/// stay on most accounts.
fn walled_fixture(
    catalog_facts: usize,
    listed_credentials: usize,
    spared: Option<i64>,
) -> RequestFixture {
    let fixture = build_fixture(catalog_facts, listed_credentials);
    let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
    for reading in quota.readings.readings.values_mut() {
        let account = fixture
            .catalog
            .accounts
            .iter()
            .find(|account| account.account_id == reading.account_id)
            .unwrap();
        let provider: i64 = account
            .provider
            .strip_prefix("cpu-provider-")
            .unwrap()
            .parse()
            .unwrap();
        if spared == Some(provider) {
            continue;
        }
        reading.received_at_ms = FIXTURE_NOW as i64;
        reading.entry["fetchedAt"] = json!(crate::account_selection::rfc3339(FIXTURE_NOW as i64));
        for name in ["primary", "secondary"] {
            if let Some(window) = reading.entry["usage"][name].as_object_mut() {
                window.insert("usedPercent".into(), json!(100.0));
                window.insert(
                    "resetsAt".into(),
                    json!(crate::account_selection::rfc3339(provider_reset(provider))),
                );
            }
        }
    }
    drop(quota);
    fixture
}

/// A quota cooldown on every route of one provider, so its routes are refused
/// by route state while every other provider's are refused by its accounts.
fn cool_provider_routes(fixture: &RequestFixture, provider: i64) {
    let prefix = format!("cpu-provider-{provider}/");
    for model in &fixture.catalog.models {
        let key = model.route_key();
        if !key.starts_with(&prefix) {
            continue;
        }
        fixture
            .store()
            .update_route_state(
                &key,
                &key,
                &striatum_core::RouteStateUpdate {
                    cooldown_until: Some(provider_reset(provider) as f64 + 600_000.0),
                    observed_at_ms: Some(FIXTURE_NOW as i64 + 1),
                    wall_class: Some(striatum_core::RouteWallClass::Quota),
                    last_error: Some("quota exhausted".into()),
                    ..Default::default()
                },
            )
            .unwrap();
    }
}

async fn bind(fixture: &RequestFixture) -> Route {
    fixture
        .harness
        .bind(reserved(fixture::CONSUMER), None)
        .await
}

/// The raw reply body of one refused selection through the real frame handler.
async fn refusal_bytes(fixture: &RequestFixture, route: &Route) -> Vec<u8> {
    let frame = route.request_frame(
        "route.select",
        &json!({"consumer":fixture::SUMMARY, "requirements":{"iq":50}, "hold_wait_max_s":0}),
    );
    let reply = fixture.harness.handle(frame).await;
    let body = reply.body.to_vec();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        parsed["code"],
        "no_route",
        "{}",
        String::from_utf8_lossy(&body)
    );
    body
}

fn fingerprint(body: &[u8]) -> (usize, String) {
    (body.len(), format!("{:x}", Sha256::digest(body)))
}

#[tokio::test]
async fn walled_many_provider_refusals_keep_their_exact_bytes() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = walled_fixture(FACTS, CREDENTIALS, None);
    let all_walled = refusal_bytes(&fixture, &bind(&fixture).await).await;
    println!("all walled: {}", String::from_utf8_lossy(&all_walled));

    let fixture = walled_fixture(FACTS, CREDENTIALS, Some(0));
    cool_provider_routes(&fixture, 0);
    let cooling = refusal_bytes(&fixture, &bind(&fixture).await).await;
    println!(
        "one provider cooling: {}",
        String::from_utf8_lossy(&cooling)
    );

    // On a mismatch, the bodies printed above show what changed.
    assert_eq!(
        [fingerprint(&all_walled), fingerprint(&cooling)],
        [
            (ALL_WALLED.0, ALL_WALLED.1.to_owned()),
            (ONE_PROVIDER_COOLING.0, ONE_PROVIDER_COOLING.1.to_owned())
        ]
    );
}

#[tokio::test]
async fn a_refusal_builds_one_recovery_view_for_all_its_walled_routes() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = walled_fixture(FACTS, CREDENTIALS, None);
    let credentials = fixture.credentials();
    let route = bind(&fixture).await;
    let before = crate::account_selection::recovery_view::recovery_view_builds(&credentials);
    let body = refusal_bytes(&fixture, &route).await;
    // The named wall comes from the recovery view; every one of the 320 walled
    // routes was read from it, so one view per route would count 320.
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains(&format!(
            "cpu-provider-0/model-0 (until_ms={})",
            provider_reset(0)
        )),
        "{text}"
    );
    assert_eq!(
        crate::account_selection::recovery_view::recovery_view_builds(&credentials) - before,
        1
    );
}

/// Process CPU of one all-walled refusal on the production-sized fixture that
/// `request_cpu_bench` measures (800 routes, 40 accounts).
#[tokio::test]
#[ignore = "release refusal CPU: cargo test --release walled_refusal_request_cpu -- --ignored --nocapture"]
async fn walled_refusal_request_cpu() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = walled_fixture(800, 40, None);
    let samples: usize = std::env::var("STRIATUM_REFUSAL_SAMPLES")
        .map(|n| n.parse().unwrap())
        .unwrap_or(20);
    let route = bind(&fixture).await;
    let warm = refusal_bytes(&fixture, &route).await;
    let started = cpu_us();
    for _ in 0..samples {
        assert_eq!(refusal_bytes(&fixture, &route).await, warm);
    }
    let cpu = cpu_us() - started;
    println!(
        "walled_refusal facts=800 credentials=40 n={samples} cpu_us/op={:.0} body={:?}",
        cpu as f64 / samples as f64,
        fingerprint(&warm)
    );
}
