//! Explicit pay_per_token consumer settings restrict both select surfaces to
//! metered routes. Subscription routes remain refused even when pinned; the
//! ordinary code-mode and flow defaults do not impose this restriction.

use std::sync::Arc;

use cortexkit_role_routing_conformance::CatalogModel;
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::ModelFact;
use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate};
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
        family: None,
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
                    upstream_status: None, model_type: None, model_kind: None,
                    lineage: None,
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
        // These tests exercise an explicit metered-only operator policy, not
        // the ordinary code-mode/flow subscription defaults.
        for consumer in ["codemode", "flow"] {
            config["consumers"][consumer]["pay_per_token"] = json!(true);
        }
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

const BROCA_ROUTES: &[&str] = &["inception/mercury-3", "muse/model", "cline-pass/model"];
const FENCE_NOW: u64 = 1_800_000_000_000;

fn broca_fence_harness() -> Arc<Harness> {
    let harness = harness_of(BROCA_ROUTES, json!({"prices":[]}));
    let mut credentials = BrocaCredentials::api_test_credentials(&facts(BROCA_ROUTES));
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "broca-loadable-api-keys".into(),
            credentials: ["cline-pass", "inception", "muse"]
                .into_iter()
                .map(|provider| cortexkit_claustrum_client::ListedCredential {
                    id: format!("apikey:{provider}"),
                    provider_ids: vec![provider.into()],
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    credential_type: "apikey".into(),
                    auth_method: Some("apikey".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        FENCE_NOW,
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca"}]);
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!(BROCA_ROUTES);
    });
    harness
}

fn broca_account_request(decision: &Value, id: &str, legacy: bool) -> Value {
    let mut params = json!({
        "provider":"inception", "model":"mercury-3", "mode":"admit",
        "conversation_key":format!("broca:{id}"), "admission_id":id,
        "decision_id":decision,
        "candidates":{"view_digest":"broca-loadable-api-keys", "accounts":[
            {"account_id":"apikey:cline-pass", "credential_id":"apikey:cline-pass"},
            {"account_id":"apikey:inception", "credential_id":"apikey:inception"},
            {"account_id":"apikey:muse", "credential_id":"apikey:muse"}
        ]}
    });
    if legacy {
        params["caller"] = json!({"runner":"broca"});
    }
    params
}

fn fence_inception(harness: &Harness, observed: i64) {
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "inception".into(),
                credential_id: "apikey:inception".into(),
                code: "billing_exhausted".into(),
            },
            None,
            observed,
        )
        .unwrap();
}

async fn broca_select(harness: &Arc<Harness>, legacy: bool) -> (FrameType, Value) {
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut params = select("mason");
    params["detail"] = json!(true);
    if legacy {
        params["targetAgent"] = json!("mason");
        params["executor"] = json!("broca");
        params["substrate"] = json!("broca");
        params["requirements"]["eq"] = json!(0);
    }
    route.call("route.select", &params).await
}

fn broca_selected_route(reply: &(FrameType, Value), legacy: bool) -> String {
    if legacy {
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        reply.1["result"]["selected"]["routeKey"]
            .as_str()
            .unwrap()
            .into()
    } else {
        route_key(reply)
    }
}

#[tokio::test]
async fn broca_fresh_billing_fence_skips_the_route_on_both_select_surfaces() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    for legacy in [false, true] {
        let harness = broca_fence_harness();
        let before = broca_select(&harness, legacy).await;
        assert_eq!(broca_selected_route(&before, legacy), "inception/mercury-3");
        let runner = harness.bind(reserved("broca"), None).await;
        let unfenced =
            broca_account_request(&before.1["result"]["decision_id"], "unfenced", legacy);
        let admitted = runner.call("route.account_for", &unfenced).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        assert_eq!(
            admitted.1["result"]["account"]["credential_id"],
            "apikey:inception"
        );
        fence_inception(&harness, FENCE_NOW as i64);
        let after = broca_select(&harness, legacy).await;
        assert_eq!(broca_selected_route(&after, legacy), "muse/model");

        let request = broca_account_request(&before.1["result"]["decision_id"], "fenced", legacy);
        let refused = runner.call("route.account_for", &request).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
        let exclusions = refused.1["detail"]["exclusions"].as_array().unwrap();
        assert_eq!(exclusions.len(), 3, "{}", refused.1);
        for (credential, reason) in [
            ("apikey:cline-pass", "wrong_provider"),
            ("apikey:inception", "billing_exhausted"),
            ("apikey:muse", "wrong_provider"),
        ] {
            let exclusion = exclusions
                .iter()
                .find(|entry| entry["credential_id"] == credential)
                .unwrap();
            assert_eq!(exclusion["reason"], reason, "{exclusion}");
        }
        assert_eq!(
            refused.1["detail"]["reset_at_ms"],
            FENCE_NOW + crate::credential_fences::FENCE_PROBE_RECHECK_MS as u64
        );
    }
}

#[tokio::test]
async fn broca_selected_one_hour_probe_admits_only_once_on_both_surfaces() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    for legacy in [false, true] {
        let harness = broca_fence_harness();
        fence_inception(&harness, FENCE_NOW as i64 - 400 * 60_000);
        let selected = broca_select(&harness, legacy).await;
        assert_eq!(
            broca_selected_route(&selected, legacy),
            "inception/mercury-3"
        );
        let runner = harness.bind(reserved("broca"), None).await;
        let request = broca_account_request(&selected.1["result"]["decision_id"], "probe", legacy);
        let admitted = runner.call("route.account_for", &request).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        assert_eq!(
            admitted.1["result"]["account"]["credential_id"],
            "apikey:inception"
        );
        assert_eq!(
            admitted.1["result"]["reason"]["code"],
            "credential_fence_probe"
        );
        let replayed = runner.call("route.account_for", &request).await;
        assert_eq!(replayed.0, FrameType::Response, "{}", replayed.1);
        let after = broca_select(&harness, legacy).await;
        assert_eq!(broca_selected_route(&after, legacy), "muse/model");
        let duplicate =
            broca_account_request(&selected.1["result"]["decision_id"], "second-probe", legacy);
        let refused = runner.call("route.account_for", &duplicate).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
    }
}

#[tokio::test]
async fn broca_concurrent_selects_reserve_one_probe_and_admit_its_decision() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    for legacy in [false, true] {
        let harness = broca_fence_harness();
        fence_inception(&harness, FENCE_NOW as i64 - 400 * 60_000);
        let (a, b) = tokio::join!(
            broca_select(&harness, legacy),
            broca_select(&harness, legacy)
        );
        let mut selected = [a, b];
        selected.sort_by_key(|reply| broca_selected_route(reply, legacy));
        assert_eq!(
            broca_selected_route(&selected[0], legacy),
            "inception/mercury-3"
        );
        assert_eq!(broca_selected_route(&selected[1], legacy), "muse/model");

        let runner = harness.bind(reserved("broca"), None).await;
        let unbound = broca_account_request(&Value::Null, "unbound-probe", legacy);
        let refused = runner.call("route.account_for", &unbound).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
        // Legacy and v1 selections both claim a chosen probe's durable
        // interval before a runner asks for its account admission.
        assert_eq!(
            refused.1["detail"]["reset_at_ms"],
            FENCE_NOW + 3_600_000
        );

        let request = broca_account_request(
            &selected[0].1["result"]["decision_id"],
            "reserved-probe",
            legacy,
        );
        let admitted = runner.call("route.account_for", &request).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        assert_eq!(
            admitted.1["result"]["account"]["credential_id"],
            "apikey:inception"
        );
        assert_eq!(
            admitted.1["result"]["reason"]["code"],
            "credential_fence_probe"
        );
    }
}

#[tokio::test]
async fn broca_unstarted_probe_admission_expires_after_one_minute() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    for quota in [false, true] {
        let _now = crate::InjectedClock::set(FENCE_NOW);
        let harness = broca_fence_harness();
        fence_inception(&harness, FENCE_NOW as i64 - 400 * 60_000);
        if quota {
            harness.store().account_transaction(|tx| {
                tx.classify_credential_quota("inception", "apikey:inception", true)
            }).unwrap();
        }
        let runner = harness.bind(reserved("broca"), None).await;
        let first = broca_account_request(&Value::Null, "abandoned", false);
        let admitted = runner.call("route.account_for", &first).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        assert_eq!(admitted.1["result"]["reason"]["code"], "credential_fence_probe");

        let _before_expiry = crate::InjectedClock::set(FENCE_NOW + 59_999);
        let waiting = broca_account_request(&Value::Null, "too-soon", false);
        let refused = runner.call("route.account_for", &waiting).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_pool_exhausted");

        let _after_expiry = crate::InjectedClock::set(FENCE_NOW + 61_000);
        let a = broca_account_request(&Value::Null, "replacement-a", false);
        let b = broca_account_request(&Value::Null, "replacement-b", false);
        let (a, b) = tokio::join!(
            runner.call("route.account_for", &a),
            runner.call("route.account_for", &b),
        );
        let replies = [a, b];
        assert_eq!(replies.iter().filter(|r| r.0 == FrameType::Response).count(), 1,
            "one replacement must take the unused probe, quota={quota}: {replies:?}");
        let admitted = replies.iter().find(|r| r.0 == FrameType::Response).unwrap();
        assert_eq!(admitted.1["result"]["account"]["credential_id"], "apikey:inception");
        assert_eq!(admitted.1["result"]["reason"]["code"], "credential_fence_probe");
        let refused = replies.iter().find(|r| r.0 == FrameType::Error).unwrap();
        assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
        assert!(!harness.store().fence_rechecks().unwrap().is_empty(),
            "expiry releases the claim, not the underlying refusal");
    }
}

#[tokio::test]
async fn broca_started_probe_admission_keeps_its_hold_after_one_minute() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = broca_fence_harness();
    fence_inception(&harness, FENCE_NOW as i64 - 400 * 60_000);
    let runner = harness.bind(reserved("broca"), None).await;
    let request = broca_account_request(&Value::Null, "started-probe", false);
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    let started = runner.call("route.account_observe", &json!({
        "kind":"admission_started", "event_id":"start-probe", "admission_id":"started-probe",
        "run_id":"probe-run", "served_account_id":"apikey:inception",
    })).await;
    assert_eq!(started.0, FrameType::Response, "{}", started.1);
    let _later = crate::InjectedClock::set(FENCE_NOW + 61_000);
    let request = broca_account_request(&Value::Null, "started-replacement", false);
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted");
    assert_eq!(refused.1["detail"]["reset_at_ms"], FENCE_NOW + 3_600_000);
}

#[tokio::test]
async fn broca_abandoned_probe_selection_holds_until_its_interval() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = broca_fence_harness();
    fence_inception(&harness, FENCE_NOW as i64 - 400 * 60_000);
    assert_eq!(
        broca_selected_route(&broca_select(&harness, false).await, false),
        "inception/mercury-3"
    );
    let _later = crate::InjectedClock::set(FENCE_NOW + 120_000);
    assert_eq!(
        broca_selected_route(&broca_select(&harness, false).await, false),
        "muse/model"
    );
    let _interval = crate::InjectedClock::set(FENCE_NOW + 3_600_000);
    let selected = broca_select(&harness, false).await;
    assert_eq!(
        broca_selected_route(&selected, false),
        "inception/mercury-3"
    );
    let runner = harness.bind(reserved("broca"), None).await;
    let request =
        broca_account_request(&selected.1["result"]["decision_id"], "after-expiry", false);
    assert_eq!(
        runner.call("route.account_for", &request).await.0,
        FrameType::Response
    );
}

fn quota_probe_harness() -> Arc<Harness> {
    let harness = broca_fence_harness();
    harness.store().record_account_route_fence(&striatum_store::AccountRouteFence {
        provider: "inception".into(),
        credential_id: "apikey:inception".into(),
        model: "mercury-3".into(),
        until_ms: FENCE_NOW as i64 + 5 * 86_400_000,
        decision_id: "previous-refusal".into(),
        recorded_at: FENCE_NOW as i64 - 900_000,
    }).unwrap();
    harness
}

fn quota_probe_clock(harness: &Harness) -> striatum_store::FenceRecheck {
    harness.store().fence_rechecks().unwrap().into_iter()
        .find(|origin| origin.kind == "account_route").unwrap()
}

#[tokio::test]
async fn decision_probe_without_account_admission_holds_for_fifteen_minutes() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let selected = broca_select(&harness, false).await;
    assert_eq!(broca_selected_route(&selected, false), "inception/mercury-3");
    let clock = quota_probe_clock(&harness);
    assert_eq!(clock.last_claimed_at, Some(FENCE_NOW as i64));
    assert_eq!(clock.next_recheck_at, FENCE_NOW as i64 + 900_000);
    assert_eq!(clock.claim_owner.as_deref(), selected.1["result"]["decision_id"].as_str());
    for elapsed in [120_000, 600_001, 899_999] {
        let _later = crate::InjectedClock::set(FENCE_NOW + elapsed);
        assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "muse/model");
    }
    let _interval = crate::InjectedClock::set(FENCE_NOW + 900_000);
    assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "inception/mercury-3");
}

#[tokio::test]
async fn decision_probe_failure_without_account_admission_advances_once() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let selected = broca_select(&harness, false).await;
    let reporter = harness.bind(reserved("example-consumer"), None).await;
    let body = json!({"decision_id":selected.1["result"]["decision_id"],"outcome":"failed"});
    let _failure = crate::InjectedClock::set(FENCE_NOW + 200_000);
    let reported = reporter.call("route.report", &body).await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    assert_eq!(quota_probe_clock(&harness).next_recheck_at, FENCE_NOW as i64 + 1_100_000);
    let _duplicate = crate::InjectedClock::set(FENCE_NOW + 300_000);
    assert_eq!(reporter.call("route.report", &body).await.0, FrameType::Response);
    assert_eq!(quota_probe_clock(&harness).next_recheck_at, FENCE_NOW as i64 + 1_100_000);
    let _original_interval = crate::InjectedClock::set(FENCE_NOW + 900_000);
    assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "muse/model");
    let _next = crate::InjectedClock::set(FENCE_NOW + 1_100_000);
    assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "inception/mercury-3");
    assert_eq!(harness.store().account_route_fences(FENCE_NOW as i64).unwrap().len(), 1);
}

#[tokio::test]
async fn decision_probe_ignores_rejected_unknown_and_other_credential_reports() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    for outcome in ["rejected", "unknown", "failed"] {
        let _now = crate::InjectedClock::set(FENCE_NOW);
        let harness = quota_probe_harness();
        let selected = broca_select(&harness, false).await;
        let reporter = harness.bind(reserved("example-consumer"), None).await;
        let before = quota_probe_clock(&harness);
        let mut body = json!({"decision_id":selected.1["result"]["decision_id"],"outcome":outcome});
        if outcome == "failed" {
            body["credential_id"] = json!("apikey:muse");
            body["runner_code"] = json!("account_limit_exhausted");
        }
        let _later = crate::InjectedClock::set(FENCE_NOW + 200_000);
        let reported = reporter.call("route.report", &body).await;
        assert_eq!(reported.0, FrameType::Response, "{outcome}: {}", reported.1);
        let after = quota_probe_clock(&harness);
        assert_eq!(after.next_recheck_at, FENCE_NOW as i64 + 900_000, "{outcome}");
        assert_eq!(after.claim_owner, before.claim_owner, "{outcome}");
    }
}

#[tokio::test]
async fn decision_probe_success_without_account_admission_heals_its_fence() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let selected = broca_select(&harness, false).await;
    let reporter = harness.bind(reserved("example-consumer"), None).await;
    let reported = reporter.call("route.report", &json!({
        "decision_id":selected.1["result"]["decision_id"],"outcome":"completed"
    })).await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    assert!(harness.store().account_route_fences(FENCE_NOW as i64).unwrap().is_empty());
}

#[tokio::test]
async fn decision_probe_late_failure_cannot_advance_a_newer_generation() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let first = broca_select(&harness, false).await;
    let _next = crate::InjectedClock::set(FENCE_NOW + 900_000);
    let second = broca_select(&harness, false).await;
    assert_eq!(broca_selected_route(&second, false), "inception/mercury-3");
    let reporter = harness.bind(reserved("example-consumer"), None).await;
    let _later = crate::InjectedClock::set(FENCE_NOW + 1_000_000);
    let reported = reporter.call("route.report", &json!({
        "decision_id":first.1["result"]["decision_id"],"outcome":"failed"
    })).await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let clock = quota_probe_clock(&harness);
    assert_eq!(clock.next_recheck_at, FENCE_NOW as i64 + 1_800_000);
    assert_eq!(clock.claim_owner.as_deref(), second.1["result"]["decision_id"].as_str());
}

#[tokio::test]
async fn decision_probe_unstarted_admission_returns_its_exact_generation() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let selected = broca_select(&harness, false).await;
    let _admission = crate::InjectedClock::set(FENCE_NOW + 30_000);
    let runner = harness.bind(reserved("broca"), None).await;
    let request = broca_account_request(&selected.1["result"]["decision_id"], "selected-unused", false);
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_eq!(quota_probe_clock(&harness).next_recheck_at, FENCE_NOW as i64 + 900_000);
    let _expiry = crate::InjectedClock::set(FENCE_NOW + 91_000);
    let replacement = broca_select(&harness, false).await;
    assert_eq!(broca_selected_route(&replacement, false), "inception/mercury-3");
    let reporter = harness.bind(reserved("example-consumer"), None).await;
    let _late = crate::InjectedClock::set(FENCE_NOW + 92_000);
    assert_eq!(reporter.call("route.report", &json!({
        "decision_id":selected.1["result"]["decision_id"],"outcome":"failed"
    })).await.0, FrameType::Response);
    assert_eq!(quota_probe_clock(&harness).next_recheck_at, FENCE_NOW as i64 + 991_000);
}

#[tokio::test]
async fn decision_probe_reset_check_cannot_bypass_a_held_slot() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    harness.store().record_account_route_fence(&striatum_store::AccountRouteFence {
        provider: "inception".into(), credential_id: "apikey:inception".into(), model: "mercury-3".into(),
        until_ms: FENCE_NOW as i64 + 60_000, decision_id: "earlier-reset".into(),
        recorded_at: FENCE_NOW as i64 - 900_000,
    }).unwrap();
    let selected = broca_select(&harness, false).await;
    assert_eq!(broca_selected_route(&selected, false), "inception/mercury-3");
    let _reset = crate::InjectedClock::set(FENCE_NOW + 120_000);
    assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "muse/model");
    let runner = harness.bind(reserved("broca"), None).await;
    let denied = runner.call("route.account_for", &broca_account_request(&Value::Null, "reset-bypass", false)).await;
    assert_eq!(denied.0, FrameType::Error, "{}", denied.1);
    let reused = runner.call("route.account_for", &broca_account_request(
        &selected.1["result"]["decision_id"], "reset-owner", false)).await;
    assert_eq!(reused.0, FrameType::Response, "{}", reused.1);
    let clock = quota_probe_clock(&harness);
    assert_eq!(clock.last_claimed_at, Some(FENCE_NOW as i64));
    assert_eq!(clock.next_recheck_at, FENCE_NOW as i64 + 900_000);
}

#[tokio::test]
async fn decision_probe_reported_admission_cannot_expire_back_to_due() {
    let _clock = crate::InjectedClock::set(FENCE_NOW);
    let harness = quota_probe_harness();
    let selected = broca_select(&harness, false).await;
    let runner = harness.bind(reserved("broca"), None).await;
    let admitted = runner.call("route.account_for", &broca_account_request(
        &selected.1["result"]["decision_id"], "reported-before-start", false)).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    let reporter = harness.bind(reserved("example-consumer"), None).await;
    let _failure = crate::InjectedClock::set(FENCE_NOW + 30_000);
    let reported = reporter.call("route.report", &json!({
        "decision_id":selected.1["result"]["decision_id"], "outcome":"failed",
        "credential_id":"apikey:inception", "runner_code":"account_limit_exhausted",
        "reset_at_ms":FENCE_NOW + 5 * 86_400_000,
    })).await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let _expiry = crate::InjectedClock::set(FENCE_NOW + 61_000);
    assert_eq!(broca_selected_route(&broca_select(&harness, false).await, false), "muse/model");
    assert_eq!(quota_probe_clock(&harness).next_recheck_at, FENCE_NOW as i64 + 930_000);
}

#[tokio::test]
async fn credentialed_ladders_skip_an_unentitled_entry_on_both_wire_shapes() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let harness = harness(json!({"prices": []}));
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!([PLAN, METERED]);
    });
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "plan-provider".into(),
                credential_id: "catalog-credential-plan-provider-apikey".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            None,
            crate::now_ms() as i64,
        )
        .unwrap();
    let route = harness.bind(reserved("example-consumer"), None).await;
    for legacy in [false, true] {
        let mut params = select("mason");
        params["detail"] = json!(true);
        if legacy {
            params["requirements"]["eq"] = json!(0);
            params["targetAgent"] = json!("mason");
            params["executor"] = json!("broca");
            params["substrate"] = json!("broca");
        }
        let reply = route.call("route.select", &params).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let rejected = if legacy {
            assert_eq!(
                reply.1["result"]["selected"]["routeKey"], METERED,
                "{}",
                reply.1
            );
            &reply.1["result"]["selected"]["rejected"]
        } else {
            assert_eq!(route_key(&reply), METERED);
            &reply.1["result"]["detail"]["rejected"]
        };
        let first = rejected
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["routeKey"] == PLAN)
            .unwrap();
        assert_eq!(first["reason"], "credential_unentitled");
        assert_eq!(
            first["credentialIds"],
            json!(["catalog-credential-plan-provider-apikey"])
        );
    }
    harness.edit_config(|config| {
        config["consumers"]["mason"]["ladder"] = json!([PLAN]);
    });
    let reply = route.call("route.select", &select("mason")).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route");
    assert!(
        reply.1["detail"].get("reset_at_ms").is_none(),
        "{}",
        reply.1
    );
    assert_eq!(
        reply.1["detail"]["rejected"][0]["reason"], "credential_unentitled",
        "{}",
        reply.1
    );
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
async fn explicit_metered_consumers_never_answer_a_subscription_plan_route() {
    let harness = harness(json!({ "prices": [{ "provider_id": "plan-provider" }] }));
    harness
        .edit_config(|config| config["providers"]["plan-provider"]["subscription"] = json!(true));
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
async fn codemode_uses_a_known_metered_account_when_plan_membership_is_unknown() {
    // The account class and metered plane prove privacy without a provider-wide plan list.
    let harness = harness(Value::Null);
    let route = harness.bind(reserved("example-consumer"), None).await;
    let reply = route.call("route.select", &select("codemode")).await;
    assert_eq!(route_key(&reply), PLAN);
    let runner = harness.bind(reserved("runner-one"), None).await;
    let account = runner.call("route.account_for", &json!({"provider":"plan-provider", "model":"model-a",
        "conversation_key":"unknown-plan", "admission_id":"unknown-plan", "decision_id":reply.1["result"]["decision_id"]})).await;
    assert_eq!(account.0, FrameType::Response, "{}", account.1);
    assert_eq!(
        account.1["result"]["account"]["credential_id"],
        "catalog-credential-plan-provider-apikey"
    );
    let mason = route.call("route.select", &select("mason")).await;
    assert_eq!(route_key(&mason), PLAN);
}

/// Two plan-free metered routes: `CHEAP` has the lower list price, and the
/// operator states a subscription subsidy for `SUBSIDISED`'s provider that
/// makes it cheaper than `CHEAP` once divided in. Codemode pays list price,
/// so its select reads the facts without that subsidy and answers `CHEAP`.
#[tokio::test]
async fn explicit_metered_consumers_rank_at_list_price_without_subscription_subsidy() {
    const CHEAP: &str = "metered-a/model-a";
    const SUBSIDISED: &str = "metered-b/model-b";
    let harness = harness_of(
        &[CHEAP, SUBSIDISED],
        json!({ "prices": [{ "provider_id": "plan-provider" }] }),
    );
    harness.edit_config(|config| {
        config["providers"]["metered-b"] = json!({ "subscription_multiplier": 40.0, "pin_only": false });
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

#[tokio::test]
async fn profile_metered_routes_pay_list_price_even_with_provider_multiplier_overrides() {
    const CHEAP: &str = "metered-a/model-a";
    const DEAR: &str = "metered-b/model-b";
    let harness = harness_of(&[CHEAP, DEAR], json!({"prices": []}));
    harness.edit_config(|config| {
        config["providers"]["metered-b"] = json!({"subscription_multiplier": 40.0});
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    for consumer in ["mason", "codemode", "flow"] {
        let profiles = super::profile_tests::lookup(
            consumer,
            30.0,
            &[(CHEAP, 1_000_000.0), (DEAR, 1_000_000.0)],
        );
        let answer = route
            .select_with_profiles(&select(consumer), &profiles)
            .await
            .unwrap();
        assert_eq!(
            answer["route"]["provider"], "metered-a",
            "{consumer}: {answer}"
        );
        let pricing =
            super::profile_tests::task_pricing(&harness, answer["decision_id"].as_str().unwrap());
        assert_eq!(pricing["valueMultiplier"], 1.0, "{consumer}: {pricing}");
        assert_eq!(
            pricing["listCostUsd"], pricing["finalCostUsd"],
            "{consumer}: {pricing}"
        );
    }
}
