use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::FrameType;

use super::{
    conformance_tests::{reserved, Harness},
    runners,
};
use crate::broca_credentials::BrocaCredentials;

const NOW: u64 = 1_800_000_000_000;
const MODEL: &str = "gpt-6.1-sol";
const RUNNER: &str = "tier-runner";

fn served() -> Value {
    serde_json::from_str(include_str!(
        "../../fixtures/service-tier-v1/fusiform-served-payloads.json"
    ))
    .unwrap()
}

fn setup(
    plans: &[&str],
    model_ultrafast: bool,
    runner_ultrafast: bool,
) -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::new("service-tiers");
    let mut fact = crate::tests::fact("openai", MODEL);
    fact.auth_methods = Some(vec!["oauth".into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "openai".into(),
        auth_method: "oauth".into(),
        kind: "quota_proxy".into(),
        selection_reason: "test account".into(),
        rates: [
            ("rate.input".into(), BillingRate::Priced { rate: 2.0 }),
            ("rate.output".into(), BillingRate::Priced { rate: 10.0 }),
        ]
        .into_iter()
        .collect(),
    });
    harness.install_facts(&[fact.clone()]);
    let mut catalog = served()["catalog.get — provider default endpoint"]["response"].clone();
    if !model_ultrafast {
        catalog["models"][format!("openai/{MODEL}")]
            .as_object_mut()
            .unwrap()
            .retain(|key, _| !key.ends_with(".mode.ultrafast"));
    }
    let wires = [(format!("openai/{MODEL}"), "openai.responses".into())]
        .into_iter()
        .collect();
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "tier-accounts".into(),
            credentials: plans
                .iter()
                .enumerate()
                .map(|(index, plan)| ListedCredential {
                    id: format!("oauth:{index}"),
                    account_id: Some(format!("account-{index}")),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["openai".into()],
                    serves: vec!["openai".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    plan_tier: Some((*plan).into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact])
            .with_test_catalog(&serde_json::to_vec(&catalog).unwrap())
            .with_wire_apis(&wires),
    );
    credentials.wire_apis = wires;
    credentials.publish_plan_prices(&served()["plan.prices — every curated row"]["response"]);
    credentials.runner_declarations.insert(
        RUNNER.into(),
        runners::parse_declaration(&json!({"runner":{
            "pairs":[{"wire_api":"openai.responses","auth_method":"oauth"}],
            "follows_account_choice":true
        }}))
        .unwrap()
        .unwrap(),
    );
    let levels = if runner_ultrafast {
        json!(["fast", "ultrafast"])
    } else {
        json!(["fast"])
    };
    credentials.runner_service_tiers.insert(RUNNER.into(), runners::service_tiers_from_describe(&json!({
        "capabilities":["service_tier"], "service_tiers":{"openai.responses":{"levels":levels,"served":"reported"}}
    })).unwrap());
    harness.set_credentials(credentials.clone());
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["providers"]["openai"]["subscription"] = json!(true);
        config["consumers"] = json!({"mason":{"owner":"tier-owner","mode":"demand"}});
    });
    (harness, credentials)
}

fn select(tier: &str) -> Value {
    json!({"consumer":"mason","requirements":{"iq":30},"service_tier":tier})
}

#[tokio::test]
async fn service_tier_ultrafast_pro_200_falls_back_to_fast() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_200"], true, true);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select("ultrafast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["service_tier_chosen"], "fast");
    let id = reply.1["result"]["decision_id"].as_str().unwrap();
    assert_eq!(
        harness.store().decision_service_tier(id).unwrap(),
        Some(("fast".into(), vec!["oauth:0".into()]))
    );
}

#[tokio::test]
async fn service_tier_sol_pro_500_selects_ultrafast_and_admits_only_eligible_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_200", "pro_500"], true, true);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select("ultrafast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["service_tier_chosen"], "ultrafast");
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner.call("route.account_for", &json!({
        "provider":"openai", "model":MODEL, "conversation_key":"session", "admission_id":"one",
        "decision_id":reply.1["result"]["decision_id"]
    })).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_eq!(admitted.1["result"]["account"]["credential_id"], "oauth:1");
}

#[tokio::test]
async fn service_tier_missing_model_mode_or_runner_level_falls_back() {
    let _clock = crate::InjectedClock::set(NOW);
    for (model, runner) in [(false, true), (true, false)] {
        let (harness, _) = setup(&["pro_500"], model, runner);
        let route = harness.bind(reserved("tier-owner"), None).await;
        let reply = route.call("route.select", &select("ultrafast")).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["service_tier_chosen"], "fast");
    }
}

#[tokio::test]
async fn service_tier_affinity_keeps_fast_when_account_gains_ultrafast() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup(&["pro_200"], true, true);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let mut request = select("ultrafast");
    request["affinity"] = json!("conversation");
    let first = route.call("route.select", &request).await;
    assert_eq!(
        first.1["result"]["service_tier_chosen"], "fast",
        "{}",
        first.1
    );
    let mut rows = credentials.listed_credentials().to_vec();
    rows[0].plan_tier = Some("pro_500".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "upgraded-plan".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let second = route.call("route.select", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["service_tier_chosen"], "fast");
}

#[tokio::test]
async fn service_tier_absent_keeps_existing_answer_bytes_and_no_tier_decision() {
    let _clock = crate::InjectedClock::set(NOW);
    let baseline: Value = serde_json::from_str(include_str!(
        "../../fixtures/service-tier-v1/route-select-baseline.json"
    ))
    .unwrap();
    for case in baseline["answers"].as_array().unwrap() {
        let answer: cortexkit_role_routing::select::SelectAnswer =
            serde_json::from_value(case["answer"].clone()).unwrap();
        assert_eq!(
            serde_json::to_vec(&serde_json::to_value(answer).unwrap()).unwrap(),
            serde_json::to_vec(&case["answer"]).unwrap()
        );
    }
    let (harness, _) = setup(&["pro_500"], true, true);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert!(reply.1["result"].get("service_tier_chosen").is_none());
    assert!(harness
        .store()
        .decision_service_tier(reply.1["result"]["decision_id"].as_str().unwrap())
        .unwrap()
        .is_none());
}

#[test]
fn service_tier_shared_samples_are_pinned_and_decode_open_records() {
    let bytes = include_bytes!("../../fixtures/service-tier-v1/cases.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        "7e8b37e09ec91fde56738d29514e45de8063bd48a96acfdb88b1bb70ded5abe5"
    );
    let samples: Value = serde_json::from_slice(bytes).unwrap();
    for tiers in samples["describe_valid"].as_array().unwrap() {
        assert!(runners::service_tiers_from_describe(
            &json!({"capabilities":["service_tier"],"service_tiers":tiers})
        )
        .is_some());
    }
    for tiers in samples["describe_refused"].as_array().unwrap() {
        assert!(runners::service_tiers_from_describe(
            &json!({"capabilities":["service_tier"],"service_tiers":tiers})
        )
        .is_none());
    }
    for usage in samples["usage"].as_array().unwrap() {
        let request = cortexkit_role_routing::report::parse_report_request(
            &json!({"decision_id":"d","outcome":"completed","usage":usage}),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(request).unwrap()["usage"], *usage);
    }
}

#[tokio::test]
async fn service_tier_report_stores_both_members_and_prices_served_not_applied() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    let route = harness.bind(reserved("tier-owner"), None).await;
    for (index, served, cost, status) in [
        (0, Some("fast"), Some(0.025), "confirmed"),
        (1, None, Some(0.08), "unconfirmed_estimate"),
        (2, Some("provider-special"), None, "served_unpriced"),
    ] {
        let selected = route.call("route.select", &select("ultrafast")).await;
        let id = selected.1["result"]["decision_id"].as_str().unwrap();
        let mut usage = json!({"input_tokens":0,"output_tokens":1000,"reasoning_tokens":0,"cached_input_tokens":0,"cache_write_tokens":0,"service_tier_applied":"ultrafast"});
        if let Some(served) = served {
            usage["service_tier_served"] = json!(served);
        }
        let report = route
            .call(
                "route.report",
                &json!({"decision_id":id,"outcome":"completed","usage":usage}),
            )
            .await;
        assert_eq!(report.0, FrameType::Response, "{index}: {}", report.1);
        let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
        let stored: (String, Option<String>, String, Option<f64>) = connection.query_row(
            "SELECT service_tier_applied, service_tier_served, service_tier_price_status, service_tier_cost_usd FROM model_route_usage WHERE id=?1",
            [format!("routing-v1-report:{id}")], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))
        ).unwrap();
        assert_eq!(
            stored,
            (
                "ultrafast".into(),
                served.map(str::to_owned),
                status.into(),
                cost
            )
        );
    }
}

#[test]
fn service_tier_plan_facts_fail_closed_and_credit_draw_uses_credit_rate() {
    let (_, credentials) = setup(&["pro_500"], true, true);
    let row = &credentials.listed_credentials()[0];
    let rate = |prices: &Value, credits| {
        crate::service_tiers::plan_rate(prices, row, "openai", "ultrafast", credits)
    };
    assert_eq!(rate(&credentials.plan_prices, false), Some(8.0));
    assert_eq!(rate(&credentials.plan_prices, true), Some(6.0));
    assert_eq!(
        rate(
            &json!({"prices":[{"provider_id":"openai","tier":"pro_500"}]}),
            false
        ),
        None
    );
    assert_eq!(
        rate(
            &json!({"prices":[{"provider_id":"openai","tier":"pro_500","service_tiers":{"state":"not_established"}}]}),
            false
        ),
        None
    );
    assert!(!crate::service_tiers::drawing_credits(
        &json!({"spend":[{"id":"credits","remaining":{"minor":100}}]})
    ));
    assert!(crate::service_tiers::drawing_credits(
        &json!({"usage":{"primary":{"usedPercent":100}},"spend":[{"id":"credits","remaining":{"minor":100}}]})
    ));
}

#[tokio::test]
async fn service_tier_never_upgrades_and_unknown_plan_or_runner_falls_to_standard() {
    let _clock = crate::InjectedClock::set(NOW);
    for missing in ["neither", "plan", "runner"] {
        let (harness, mut credentials) = setup(&["pro_500"], true, true);
        if missing == "plan" {
            credentials.publish_plan_prices(&json!({"prices":[]}));
        }
        if missing == "runner" {
            credentials.runner_service_tiers.clear();
        }
        harness.set_credentials(credentials);
        let route = harness.bind(reserved("tier-owner"), None).await;
        let reply = route.call("route.select", &select("fast")).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["service_tier_chosen"],
            if missing == "neither" {
                "fast"
            } else {
                "standard"
            }
        );
    }
}

#[test]
fn service_tier_prices_subscription_draw_and_api_mode_without_inheriting_base_dimensions() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, credentials) = setup(&["pro_500"], true, true);
    let mut fact = crate::tests::fact("openai", MODEL);
    credentials.price_for_method(&mut fact, "oauth", &Default::default());
    let usage = cortexkit_role_routing::report::parse_report_request(&json!({
        "decision_id":"d","outcome":"completed","usage":{"input_tokens":0,"output_tokens":1_000_000,"reasoning_tokens":0,"cached_input_tokens":0,"cache_write_tokens":0}
    })).unwrap().usage.unwrap();
    let mut subscription = fact.clone();
    crate::service_tiers::price_fact(&credentials, &mut subscription, "ultrafast", true, 8.0);
    assert_eq!(
        crate::service_tiers::usage_cost(&subscription, &usage),
        Some(80.0)
    );
    crate::service_tiers::price_fact(&credentials, &mut fact, "ultrafast", false, 1.0);
    assert_eq!(crate::service_tiers::usage_cost(&fact, &usage), Some(60.0));
    assert!(!fact
        .billing_plane
        .as_ref()
        .unwrap()
        .rates
        .contains_key("rate.reasoning"));
}
