use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::{
    conformance_tests::{reserved, Harness},
    runners,
};
use crate::broca_credentials::BrocaCredentials;
mod finish_rollback_tests;
mod follow_up_tests;
mod unservable_tests;

const NOW: u64 = 1_800_000_000_000;
const MODEL: &str = "gpt-6.1-sol";
const RUNNER: &str = "tier-runner";

mod observation_tests;

fn served() -> Value {
    serde_json::from_str(include_str!(
        "../../fixtures/service-tier-v1/fusiform-served-payloads.json"
    ))
    .unwrap()
}

pub(super) fn setup(
    plans: &[&str],
    model_ultrafast: bool,
    runner_ultrafast: bool,
) -> (Arc<Harness>, BrocaCredentials) {
    setup_for_method(
        plans,
        model_ultrafast,
        runner_ultrafast,
        "oauth",
        "openai.responses",
    )
}

pub(super) fn setup_for_method(
    plans: &[&str],
    model_ultrafast: bool,
    runner_ultrafast: bool,
    method: &str,
    wire: &str,
) -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::new("service-tiers");
    let mut fact = crate::tests::fact("openai", MODEL);
    fact.auth_methods = Some(vec![method.into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "openai".into(),
        auth_method: method.into(),
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
    let wires = [(format!("openai/{MODEL}"), wire.into())]
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
                    id: format!("{method}:{index}"),
                    account_id: Some(format!("account-{index}")),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["openai".into()],
                    serves: vec!["openai".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some(method.into()),
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
    super::conformance_tests::publish_fixture_providers(
        &mut credentials,
        RUNNER,
        &["openai"],
        true,
    );
    let levels = if runner_ultrafast {
        json!(["fast", "ultrafast"])
    } else {
        json!(["fast"])
    };
    credentials.runner_service_tiers.insert(RUNNER.into(), runners::service_tiers_from_describe(&json!({
        "capabilities":["service_tier"], "service_tiers":{wire:{"levels":levels,"served":"reported"}}
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

fn select_sol(tier: &str) -> Value {
    let mut request = select(tier);
    request["pin"] = json!("openai/gpt-6.1-sol");
    request
}

#[tokio::test]
async fn speed_twin_pin_matches_base_pin_with_explicit_tier() {
    let _clock = crate::InjectedClock::set(NOW);
    const TWIN: &str = "openai/gpt-6.1-sol-highspeed";
    for (runner_offers_fast, chosen) in [(true, "fast"), (false, "standard")] {
        let (harness, mut credentials) = setup(&["pro_200"], true, false);
        if !runner_offers_fast {
            credentials.runner_service_tiers.clear();
        }
        let mut catalog = served()["catalog.get — provider default endpoint"]["response"].clone();
        catalog["models"][TWIN] = json!({
            "existence":"present", "model.mode_of":{"mode":"fast", "model_id":MODEL}
        });
        let snapshot = crate::fusiform_catalog::decode_catalog_get(catalog.to_string()).unwrap();
        let (facts, cursor) = snapshot.parts();
        harness
            .store()
            .apply_fusiform_catalog(facts, cursor)
            .unwrap();
        harness.set_credentials(credentials);
        let route = harness.bind(reserved("tier-owner"), None).await;

        let mut twin_request = select_sol("fast");
        twin_request["pin"] = json!(TWIN);
        twin_request.as_object_mut().unwrap().remove("service_tier");
        let twin = route.call("route.select", &twin_request).await;
        let base = route.call("route.select", &select_sol("fast")).await;
        for (ty, reply) in [&twin, &base] {
            assert_eq!(*ty, FrameType::Response, "{reply}");
            let answer = &reply["result"];
            assert_eq!(answer["route"], json!({"provider":"openai", "model":MODEL}));
            assert_eq!(answer["service_tier_chosen"], chosen);
            assert_eq!(answer["pin_outcome"], "pinned");
            let id = answer["decision_id"].as_str().unwrap();
            assert_eq!(
                harness.store().decision_service_tier(id).unwrap(),
                Some((chosen.into(), vec!["oauth:0".into()]))
            );
        }
        let explanation = |reply: &Value| {
            let id = reply["result"]["decision_id"].as_str().unwrap();
            let decision = harness.store().get_decision_by_id(id).unwrap().unwrap();
            serde_json::from_str::<Value>(&decision.explanation_json.unwrap()).unwrap()
        };
        assert_eq!(explanation(&twin.1), explanation(&base.1));
        let fallback = stored_tier_fallback(&harness, &twin.1);
        assert_eq!(fallback, stored_tier_fallback(&harness, &base.1));
        if runner_offers_fast {
            assert!(fallback.is_null());
        } else {
            assert_eq!(fallback["requested"], "fast");
            assert_eq!(fallback["chosen"], "standard");
            assert_eq!(sol_tier_rejections(&fallback)[0]["check"], "runner");
        }

        twin_request["service_tier"] = json!("standard");
        let (ty, refusal) = route.call("route.select", &twin_request).await;
        assert_eq!(ty, FrameType::Error, "{refusal}");
        assert_eq!(refusal["code"], "invalid_request");
        let message = refusal["message"].as_str().unwrap();
        assert!(
            message.contains(TWIN) && message.contains("fast") && message.contains("standard"),
            "{refusal}"
        );

        harness.edit_config(|config| {
            config["consumers"]["spec_designer"]["pin"] = json!({"model":TWIN});
            config["consumers"]["mason"]["pin"] = json!({"model":TWIN});
        });
        for (request, pin_outcome) in [
            (
                json!({"consumer":"mason", "requirements":{"iq":30}, "pin_slot":"spec_designer"}),
                json!("pinned"),
            ),
            (
                json!({"consumer":"mason", "requirements":{"iq":30}}),
                Value::Null,
            ),
        ] {
            let (ty, reply) = route.call("route.select", &request).await;
            assert_eq!(ty, FrameType::Response, "{reply}");
            let answer = &reply["result"];
            assert_eq!(answer["route"], base.1["result"]["route"]);
            assert_eq!(answer["service_tier_chosen"], chosen);
            assert_eq!(answer["pin_outcome"], pin_outcome);
        }
    }
}

#[tokio::test]
async fn project_pin_recognizes_speed_twin_and_uses_its_tier() {
    let _clock = crate::InjectedClock::set(NOW);
    const TWIN: &str = "openai/gpt-6.1-sol-highspeed";
    let (harness, credentials) = setup(&["pro_200"], true, false);
    let mut catalog = served()["catalog.get — provider default endpoint"]["response"].clone();
    catalog["models"][TWIN] = json!({
        "existence":"present", "model.mode_of":{"mode":"fast", "model_id":MODEL}
    });
    let snapshot = crate::fusiform_catalog::decode_catalog_get(catalog.to_string()).unwrap();
    let (facts, cursor) = snapshot.parts();
    harness
        .store()
        .apply_fusiform_catalog(facts, cursor)
        .unwrap();
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["projects"]["review-project"] = json!({"pin":TWIN});
    });
    let route = harness
        .bind(reserved("tier-owner"), Some("review-project"))
        .await;
    let (ty, reply) = route
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    assert_eq!(
        reply["result"]["route"],
        json!({"provider":"openai", "model":MODEL})
    );
    assert_eq!(reply["result"]["service_tier_chosen"], "fast");
    assert!(reply["result"]["explanation"]
        .as_str()
        .unwrap()
        .contains("the project's operator pin"));

    let (ty, fallback) = route
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":30},
                "pin":"openai/not-in-the-catalog", "pin_fallback":"route", "service_tier":"standard"
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Error, "{fallback}");
    assert_eq!(
        serde_json::to_vec(&fallback).unwrap(),
        serde_json::to_vec(&json!({
            "code":"pin_unavailable", "detail":{"pin":TWIN,"reason":"unknown_model"},
            "message":"the pin \"openai/gpt-6.1-sol-highspeed\" cannot be served: unknown_model"
        }))
        .unwrap()
    );
}

fn account_request(id: &str, tier: Option<&str>) -> Value {
    let mut request = json!({
        "provider":"openai", "model":MODEL, "conversation_key":id, "admission_id":id,
    });
    if let Some(tier) = tier {
        request["service_tier"] = json!(tier);
    }
    request
}

fn legacy_broca_setup() -> (Arc<Harness>, BrocaCredentials) {
    let (harness, mut credentials) = setup_for_method(
        &["pro_200", "pro_200"],
        true,
        true,
        "chatgpt",
        "openai.chatgpt_responses",
    );
    live_broca_credentials(&mut credentials);
    let tiers = credentials.runner_service_tiers.remove(RUNNER).unwrap();
    credentials
        .runner_service_tiers
        .insert("broca".into(), tiers);
    harness.set_credentials(credentials.clone());
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca","via_vault":true}]);
    });
    (harness, credentials)
}

fn legacy_broca_request(id: &str) -> Value {
    json!({
        "provider": "openai", "model": MODEL, "affinity_model": MODEL,
        "conversation_key": "legacy-fast", "admission_id": id,
        "caller": {"runner": "broca"}, "mode": "admit",
        "initial_input_bytes": 12345, "initial_input_estimated": true,
        "hold_wait_max_s": 300,
        "candidates": {
            "view_digest": "tier-accounts",
            "accounts": [
                {"account_id": "account-0", "credential_id": "chatgpt:0"},
                {"account_id": "account-1", "credential_id": "chatgpt:1"}
            ]
        }
    })
}

#[tokio::test]
async fn account_service_tier_fast_prefers_eligible_plan_and_records_tier() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["unknown", "pro_200"], true, true);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let request = account_request("fast-head", Some("fast"));
    let reply = runner.call("route.account_for", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let answer = &reply.1["result"];
    assert_eq!(answer["account"]["account_id"], "account-1");
    assert_eq!(answer["service_tier_chosen"], "fast");
    assert_eq!(
        harness
            .store()
            .account_decision_service_tier(answer["reason"]["decision_id"].as_str().unwrap())
            .unwrap(),
        Some((Some("fast".into()), "fast".into()))
    );
    let replay = runner.call("route.account_for", &request).await;
    assert_eq!(replay.1, reply.1);
    let mut conflict = request;
    conflict["service_tier"] = json!("standard");
    let conflict = runner.call("route.account_for", &conflict).await;
    assert_eq!(conflict.1["code"], "admission_conflict", "{}", conflict.1);
}

#[tokio::test]
async fn account_service_tier_ultrafast_steps_down_to_fast_then_standard() {
    let _clock = crate::InjectedClock::set(NOW);
    for (plan, expected) in [("pro_200", "fast"), ("unknown", "standard")] {
        let (harness, _) = setup(&[plan], true, true);
        let runner = harness.bind(reserved(RUNNER), None).await;
        let reply = runner
            .call(
                "route.account_for",
                &account_request("fallback", Some("ultrafast")),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["service_tier_chosen"], expected);
        assert_eq!(
            harness
                .store()
                .account_decision_service_tier(
                    reply.1["result"]["reason"]["decision_id"].as_str().unwrap()
                )
                .unwrap(),
            Some((Some("ultrafast".into()), expected.into()))
        );
    }
}

#[tokio::test]
async fn bound_fast_resume_is_admitted_after_a_missed_describe_poll() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = legacy_broca_setup();
    let modules = json!([{"module_id":"broca","nonce_verified":true}]);
    let policies = serde_json::from_value::<Vec<crate::config_policy::RunnerPolicy>>(
        json!([{"module":"broca","via_vault":true}]),
    )
    .unwrap();
    let describe: Value = serde_json::from_slice(include_bytes!(
        "../../fixtures/contracts/broca-service-tiers-v1/role-describe-live.json"
    ))
    .unwrap();
    let mut cache = runners::DescribeCache::default();
    cache.catalog(&modules, &policies, &mut credentials, NOW as i64);
    cache.reply("broca", Some(&describe), &mut credentials, NOW as i64);
    harness.set_credentials(credentials.clone());
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select_sol("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let runner = harness.bind(reserved("broca"), None).await;
    let mut request = legacy_broca_request("fast-before-missed-poll");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    request["service_tier"] = json!("fast");
    let initial = runner.call("route.account_for", &request).await;
    assert_eq!(initial.0, FrameType::Response, "{}", initial.1);

    let _later = crate::InjectedClock::set(NOW + 30_000);
    cache.reply("broca", None, &mut credentials, (NOW + 30_000) as i64);
    harness.set_credentials(credentials.clone());
    request["admission_id"] = json!("fast-after-missed-poll");
    let resumed = runner.call("route.account_for", &request).await;
    assert_eq!(resumed.0, FrameType::Response, "{}", resumed.1);
    assert_eq!(resumed.1["result"]["service_tier_chosen"], "fast");
    assert_eq!(
        resumed.1["result"]["account"],
        initial.1["result"]["account"]
    );
    assert_eq!(
        resumed.1["result"]["pin"]["id"],
        initial.1["result"]["pin"]["id"]
    );

    cache.reply(
        "broca",
        Some(&json!({"capabilities":[]})),
        &mut credentials,
        (NOW + 30_000) as i64,
    );
    harness.set_credentials(credentials);
    request["admission_id"] = json!("fast-after-settled-tier-removal");
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_unservable");
    assert_eq!(
        refused.1["detail"],
        json!({"reason":"bound_tier_unavailable"})
    );
}

#[tokio::test]
async fn bound_fast_resume_survives_a_restart_catalog_gap_until_describe_expires() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = legacy_broca_setup();
    let modules = json!([{"module_id":"broca","nonce_verified":true}]);
    let policies = serde_json::from_value::<Vec<crate::config_policy::RunnerPolicy>>(
        json!([{"module":"broca","via_vault":true}]),
    )
    .unwrap();
    let describe: Value = serde_json::from_slice(include_bytes!(
        "../../fixtures/contracts/broca-service-tiers-v1/role-describe-live.json"
    ))
    .unwrap();
    let mut cache = runners::DescribeCache::default();
    cache.catalog(&modules, &policies, &mut credentials, NOW as i64);
    cache.reply("broca", Some(&describe), &mut credentials, NOW as i64);
    harness.set_credentials(credentials.clone());
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select_sol("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let runner = harness.bind(reserved("broca"), None).await;
    let mut request = legacy_broca_request("fast-before-catalog-gap");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    request["service_tier"] = json!("fast");
    let initial = runner.call("route.account_for", &request).await;
    assert_eq!(initial.0, FrameType::Response, "{}", initial.1);

    cache.reply("broca", None, &mut credentials, (NOW + 30_000) as i64);
    let _later = crate::InjectedClock::set(NOW + 60_000);
    cache.catalog(
        &json!([]),
        &policies,
        &mut credentials,
        (NOW + 60_000) as i64,
    );
    harness.set_credentials(credentials.clone());
    request["admission_id"] = json!("fast-during-catalog-gap");
    let resumed = runner.call("route.account_for", &request).await;
    assert_eq!(resumed.0, FrameType::Response, "{}", resumed.1);
    assert_eq!(resumed.1["result"]["service_tier_chosen"], "fast");
    assert_eq!(
        resumed.1["result"]["account"],
        initial.1["result"]["account"]
    );
    for field in ["id", "generation"] {
        assert_eq!(
            resumed.1["result"]["pin"][field],
            initial.1["result"]["pin"][field]
        );
    }

    let _expired = crate::InjectedClock::set(NOW + 300_000);
    cache.catalog(
        &json!([]),
        &policies,
        &mut credentials,
        (NOW + 300_000) as i64,
    );
    harness.set_credentials(credentials);
    request["admission_id"] = json!("fast-after-catalog-gap-expiry");
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_selection_unavailable");
    assert!(harness
        .store()
        .v1_admission("reserved:broca", "fast-after-catalog-gap-expiry")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn account_service_tier_legacy_admission_continues_at_bound_fast_tier() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = legacy_broca_setup();
    let runner = harness.bind(reserved("broca"), None).await;
    let mut request = legacy_broca_request("legacy-fast");
    let initial = runner.call("route.account_for", &request).await;
    assert_eq!(initial.0, FrameType::Response, "{}", initial.1);
    assert!(initial.1["result"].get("service_tier_chosen").is_none());

    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select_sol("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision_id = selected.1["result"]["decision_id"].as_str().unwrap();
    let (tier, allowed) = harness
        .store()
        .decision_service_tier(decision_id)
        .unwrap()
        .unwrap();
    assert_eq!(tier, "fast");
    assert!(allowed.contains(
        &initial.1["result"]["account"]["credential_id"]
            .as_str()
            .unwrap()
            .to_owned()
    ));
    // A bound decision keeps its allowed credentials even if their relative
    // tier prices change; a retained account need not win a new price ranking.
    let pinned = initial.1["result"]["account"]["credential_id"]
        .as_str()
        .unwrap();
    let mut rows = credentials.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|row| row.id == pinned)
        .unwrap()
        .plan_tier = Some("pro_500".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    let mut prices = credentials.plan_prices.clone();
    let plan = prices["prices"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|row| row["provider_id"] == "openai" && row["tier"] == "pro_500")
        .unwrap();
    plan["service_tiers"]["tiers"][0]["included_usage_rate"] = json!({"units": 3, "exponent": 0});
    credentials.publish_plan_prices(&prices);
    harness.set_credentials(credentials);
    request["admission_id"] = json!("legacy-fast-follow-up");
    request["decision_id"] = json!(decision_id);
    request["service_tier"] = json!("fast");
    request["pool"] = json!("pool-example");
    request["requested_account"] = json!({
        "credential_id": initial.1["result"]["account"]["credential_id"]
    });
    let continued = runner.call("route.account_for", &request).await;
    let recorded = harness.store().account_decisions("serve", 10).unwrap();
    let recorded_tiers: Vec<_> = recorded
        .iter()
        .map(|row| {
            (
                row.code.clone(),
                harness
                    .store()
                    .account_decision_service_tier(&row.decision_id)
                    .unwrap(),
            )
        })
        .collect();
    assert_eq!(
        continued.0,
        FrameType::Response,
        "answer: {}; recorded: {recorded_tiers:?}",
        continued.1
    );
    assert_eq!(
        continued.1["result"]["service_tier_chosen"], "fast",
        "answer: {}; recorded: {recorded_tiers:?}",
        continued.1
    );
    assert_eq!(
        continued.1["result"]["account"],
        initial.1["result"]["account"]
    );
    assert_eq!(
        continued.1["result"]["pin"]["id"],
        initial.1["result"]["pin"]["id"]
    );
    assert_eq!(
        recorded_tiers[0].1,
        Some((Some("fast".into()), "fast".into()))
    );
    let explanation: Value = serde_json::from_str(&recorded[0].decision_json).unwrap();
    assert_eq!(explanation["explanation"]["affinity"]["kind"], "retained");
    assert_eq!(runner.call("route.account_for", &request).await, continued);
    request["service_tier"] = json!("standard");
    let conflict = runner.call("route.account_for", &request).await;
    assert_eq!(conflict.1["code"], "admission_conflict", "{}", conflict.1);
}

/// Historical standard-tier decisions without a v1 binding still authorize legacy account admissions.
#[tokio::test]
async fn account_service_tier_legacy_standard_decision_without_v1_row_admits() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = legacy_broca_setup();
    let request = legacy_broca_request("legacy-standard");
    let decision_id = "historical-standard";
    harness
        .store()
        .seed_preupgrade_decision_for_test(
            &striatum_store::V1Decision {
                decision_id: decision_id.into(),
                principal: "reserved:prefrontal-core".into(),
                consumer: "prefrontal-core.mason".into(),
                allowed_account_class: Default::default(),
                project_id: None,
                provider: request["provider"].as_str().unwrap().into(),
                model: request["model"].as_str().unwrap().into(),
                variant: None,
                runner_module: "broca".into(),
                affinity: None,
                created_at_ms: NOW as i64,
                expires_at_ms: NOW as i64 + 60_000,
            },
            false,
        )
        .unwrap();
    // The live refused decision dec_c1e8aa40025f carries exactly this: tier
    // `standard` (a fast project's tier stepped down for a model without fast)
    // and every account allowed. This harness has no project tier setting, so
    // record it the way the older select does for one.
    harness
        .store()
        .record_service_tier(
            decision_id,
            "standard",
            &["chatgpt:0".into(), "chatgpt:1".into()],
        )
        .unwrap();
    assert!(
        harness.store().v1_decision(decision_id).unwrap().is_none(),
        "the older select writes no v1 row, which is the case under test"
    );
    let runner = harness.bind(reserved("broca"), None).await;
    let mut request = legacy_broca_request("legacy-standard");
    request["decision_id"] = json!(decision_id);
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert!(admitted.1["result"]["account"]["credential_id"].is_string());
}

#[tokio::test]
async fn account_service_tier_legacy_bound_decision_moves_only_when_a_sibling_supports_it() {
    let _clock = crate::InjectedClock::set(NOW);
    for (failure, hint) in [
        ("plan", true),
        ("runner", true),
        ("model", true),
        ("allowed", true),
        ("plan", false),
        ("candidates", false),
    ] {
        let (harness, mut credentials) = legacy_broca_setup();
        let runner = harness.bind(reserved("broca"), None).await;
        let initial = runner
            .call("route.account_for", &legacy_broca_request("initial"))
            .await;
        assert_eq!(initial.0, FrameType::Response, "{}", initial.1);
        let pinned = initial.1["result"]["account"]["credential_id"]
            .as_str()
            .unwrap();
        let sibling = credentials
            .listed_credentials()
            .iter()
            .find(|row| row.id != pinned)
            .unwrap()
            .clone();
        let owner = harness.bind(reserved("tier-owner"), None).await;
        let selected = owner.call("route.select", &select_sol("fast")).await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        let decision_id = selected.1["result"]["decision_id"].as_str().unwrap();
        assert_eq!(
            harness.store().decision_service_tier(decision_id).unwrap(),
            Some(("fast".into(), vec!["chatgpt:0".into(), "chatgpt:1".into()]))
        );
        let mut request = legacy_broca_request("unavailable");
        request["decision_id"] = json!(decision_id);
        request["service_tier"] = json!("fast");
        request["pool"] = json!("pool-example");
        if hint {
            request["requested_account"] = json!({"credential_id": pinned});
        }
        match failure {
            "plan" => {
                let mut rows = credentials.listed_credentials().to_vec();
                rows.iter_mut()
                    .find(|row| row.id == pinned)
                    .unwrap()
                    .plan_tier = Some("unknown".into());
                credentials.apply_listing(
                    &ScopedCredentialListing {
                        credentials: rows,
                        ..Default::default()
                    },
                    NOW,
                );
            }
            "runner" => {
                runners::DescribeCache::default().reply(
                    "broca",
                    Some(&json!({"capabilities":[]})),
                    &mut credentials,
                    NOW as i64,
                );
            }
            "model" => {
                let mut catalog =
                    served()["catalog.get — provider default endpoint"]["response"].clone();
                catalog["models"][format!("openai/{MODEL}")]
                    .as_object_mut()
                    .unwrap()
                    .retain(|key, _| !key.ends_with(".mode.fast"));
                credentials.publish_billing_catalog(
                    credentials
                        .billing_catalog
                        .clone()
                        .with_test_catalog(&serde_json::to_vec(&catalog).unwrap()),
                );
            }
            "allowed" => harness
                .store()
                .record_service_tier(decision_id, "fast", &[sibling.id.clone()])
                .unwrap(),
            "candidates" => {
                request["candidates"]["accounts"] = json!([{
                    "account_id": sibling.account_id,
                    "credential_id": sibling.id
                }]);
            }
            _ => unreachable!(),
        }
        harness.set_credentials(credentials);
        let refused = runner.call("route.account_for", &request).await;
        if !matches!(failure, "runner" | "model") {
            assert_eq!(
                refused.0,
                FrameType::Response,
                "{failure}, hint={hint}: {}",
                refused.1
            );
            assert_eq!(refused.1["result"]["service_tier_chosen"], "fast");
            assert_eq!(refused.1["result"]["account"]["credential_id"], sibling.id);
            assert_eq!(refused.1["result"]["pin"]["generation"], 2);
            continue;
        }
        assert_eq!(
            refused.0,
            FrameType::Error,
            "{failure}, hint={hint}: {}",
            refused.1
        );
        assert_eq!(refused.1["code"], "account_unservable");
        assert_eq!(
            refused.1["detail"],
            json!({"reason":"bound_tier_unavailable"})
        );
        assert_eq!(
            refused.1["message"],
            "No account can serve this routing decision's service tier any more, and a bound decision never steps down to a lower tier. Ask routing for a new decision."
        );
        assert!(harness
            .store()
            .account_transaction(|tx| tx.admission("reserved:broca", "unavailable"))
            .unwrap()
            .is_none());
        assert_eq!(
            harness
                .store()
                .account_decisions("serve", 10)
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn account_bound_tier_mismatch_is_a_permanent_invalid_request() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_200"], true, true);
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"].clone();
    assert_eq!(selected.1["result"]["service_tier_chosen"], "fast");
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("tier-mismatch", Some("standard"));
    request["decision_id"] = decision.clone();
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(
        refused.1["detail"],
        json!({
            "field":"service_tier", "decision_id":decision,
            "requested_service_tier":"standard", "bound_service_tier":"fast",
        })
    );
    assert_eq!(
        cortexkit_role_routing::refusals::retry_class("invalid_request", &refused.1["detail"]),
        cortexkit_role_routing::refusals::RetryClass::Permanent,
    );
    assert!(harness
        .store()
        .v1_admission(&format!("reserved:{RUNNER}"), "tier-mismatch")
        .unwrap()
        .is_none());
    harness
        .store()
        .record_service_tier(decision.as_str().unwrap(), "future-tier", &[])
        .unwrap();
    let mut unfamiliar = account_request("unfamiliar-recorded-tier", None);
    unfamiliar["decision_id"] = decision;
    let refused = runner.call("route.account_for", &unfamiliar).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_unservable", "{}", refused.1);
    assert_eq!(
        refused.1["detail"],
        json!({"reason":"stored_state_invalid"})
    );
}

#[tokio::test]
async fn account_service_tier_bound_decision_keeps_exact_tier_or_refuses() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup(&["unknown", "pro_200"], true, true);
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"].clone();
    let runner = harness.bind(reserved(RUNNER), None).await;
    for (id, tier) in [
        ("inherited", None),
        ("repeated", Some("fast")),
        ("higher-ceiling", Some("ultrafast")),
    ] {
        let mut request = account_request(id, tier);
        request["decision_id"] = decision.clone();
        let reply = runner.call("route.account_for", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["service_tier_chosen"], "fast");
        assert_eq!(reply.1["result"]["account"]["account_id"], "account-1");
    }
    let mut lower = account_request("lower-ceiling", Some("standard"));
    lower["decision_id"] = decision.clone();
    let refused = runner.call("route.account_for", &lower).await;
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    let mut rows = credentials.listed_credentials().to_vec();
    rows[1].plan_tier = Some("unknown".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "revoked-tier".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let mut revoked = account_request("revoked", None);
    revoked["decision_id"] = decision;
    let refused = runner.call("route.account_for", &revoked).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert!(harness
        .store()
        .v1_admission(&format!("reserved:{RUNNER}"), "revoked")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn account_service_tier_requires_calling_runner_and_model_support() {
    let _clock = crate::InjectedClock::set(NOW);
    for missing in ["runner", "model"] {
        let (harness, mut credentials) = setup(&["pro_200"], true, true);
        if missing == "runner" {
            let declaration = credentials.runner_declarations[RUNNER].clone();
            let tiers = credentials.runner_service_tiers[RUNNER].clone();
            credentials
                .runner_declarations
                .insert("other-tier-runner".into(), declaration);
            credentials
                .runner_service_tiers
                .insert("other-tier-runner".into(), tiers);
            runners::DescribeCache::default().reply(
                RUNNER,
                Some(&json!({"capabilities":[]})),
                &mut credentials,
                NOW as i64,
            );
            harness.edit_config(|config| {
                config["runners"] = json!([
                    {"module":RUNNER,"via_vault":true},
                    {"module":"other-tier-runner","via_vault":true}
                ]);
            });
        } else {
            let mut catalog =
                served()["catalog.get — provider default endpoint"]["response"].clone();
            catalog["models"][format!("openai/{MODEL}")]
                .as_object_mut()
                .unwrap()
                .retain(|key, _| !key.ends_with(".mode.fast"));
            credentials.publish_billing_catalog(
                credentials
                    .billing_catalog
                    .clone()
                    .with_test_catalog(&serde_json::to_vec(&catalog).unwrap()),
            );
        }
        harness.set_credentials(credentials);
        let runner = harness.bind(reserved(RUNNER), None).await;
        let reply = runner
            .call("route.account_for", &account_request(missing, Some("fast")))
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["service_tier_chosen"], "standard");
    }
}

#[tokio::test]
async fn account_service_tier_does_not_replace_a_pinned_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["unknown", "pro_200"], true, true);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("pinned", Some("fast"));
    request["requested_account"] = json!({"account_id":"account-0"});
    let reply = runner.call("route.account_for", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["account"]["account_id"], "account-0");
    assert_eq!(reply.1["result"]["service_tier_chosen"], "standard");
}

fn live_broca_credentials(credentials: &mut BrocaCredentials) {
    let bytes =
        include_bytes!("../../fixtures/contracts/broca-service-tiers-v1/role-describe-live.json");
    assert_eq!(bytes.len(), 2072);
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        "dd0ef1d533f267ea3422e508c7e16f7ece51239fcc93580924572c610bf8e581"
    );
    let reply: Value = serde_json::from_slice(bytes).unwrap();
    credentials.runner_service_tiers.clear();
    let modules = json!([{"module_id":RUNNER,"nonce_verified":true,"role_declarations":null}]);
    let policies = vec![serde_json::from_value(json!({"module":RUNNER,"via_vault":true})).unwrap()];
    for module in runners::tier_discovery_modules(&modules, &Default::default(), &policies) {
        if let Some(tiers) = runners::service_tiers_from_describe(&reply) {
            credentials.runner_service_tiers.insert(module, tiers);
        }
    }
}

#[test]
fn service_tier_discovery_polls_only_verified_declared_or_vault_runners() {
    let modules = json!([
        {"module_id":RUNNER,"nonce_verified":true,"role_declarations":null},
        {"module_id":"unverified","nonce_verified":false,"role_declarations":null},
        {"module_id":"not-vault","nonce_verified":true,"role_declarations":null},
        {"module_id":"declared","nonce_verified":true,"role_declarations":{"routing/v1":{
            "runner":{"follows_account_choice":true}
        }}}
    ]);
    let policies: Vec<crate::config_policy::RunnerPolicy> = serde_json::from_value(json!([
        {"module":RUNNER,"via_vault":true},
        {"module":"unverified","via_vault":true},
        {"module":"not-vault","via_vault":false},
        {"module":"not-in-catalog","via_vault":true}
    ]))
    .unwrap();
    let declarations = runners::declarations_from_catalog(&modules);
    assert_eq!(
        runners::tier_discovery_modules(&modules, &declarations, &policies),
        [RUNNER.to_owned(), "declared".into()].into_iter().collect()
    );
    assert!(runners::tier_discovery_modules(&json!([]), &declarations, &policies).is_empty());
}

#[tokio::test]
async fn service_tier_live_broca_pro_200_mason_selects_fast() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup_for_method(
        &["pro_200"],
        true,
        true,
        "chatgpt",
        "openai.chatgpt_responses",
    );
    live_broca_credentials(&mut credentials);
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select_sol("fast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["service_tier_chosen"],
        "fast",
        "{}",
        stored_tier_fallback(&harness, &reply.1)
    );
    assert_eq!(
        reply.1["result"]["route"],
        json!({"provider":"openai","model":"gpt-6.1-sol"})
    );
    let id = reply.1["result"]["decision_id"].as_str().unwrap();
    assert_eq!(
        harness.store().decision_service_tier(id).unwrap(),
        Some(("fast".into(), vec!["chatgpt:0".into()]))
    );
    assert!(stored_tier_fallback(&harness, &reply.1).is_null());
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner.call("route.account_for", &json!({
        "provider":"openai", "model":MODEL, "conversation_key":"live-broca", "admission_id":"one",
        "decision_id":id
    })).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_eq!(
        admitted.1["result"]["account"]["credential_id"],
        "chatgpt:0"
    );
}

fn stored_tier_fallback(harness: &Harness, reply: &Value) -> Value {
    let id = reply["result"]["decision_id"].as_str().unwrap();
    let decision = harness.store().get_decision_by_id(id).unwrap().unwrap();
    let input: Value = serde_json::from_str(&decision.requirements_json).unwrap();
    input["service_tier_fallback"].clone()
}

fn sol_tier_rejections(fallback: &Value) -> Vec<Value> {
    fallback["attempts"][0]["rejections"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|rejection| rejection["route"] == "openai/gpt-6.1-sol")
        .cloned()
        .collect()
}

#[tokio::test]
async fn service_tier_live_broca_without_fast_records_runner_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup_for_method(
        &["pro_200"],
        true,
        true,
        "chatgpt",
        "openai.chatgpt_responses",
    );
    live_broca_credentials(&mut credentials);
    credentials
        .runner_service_tiers
        .get_mut(RUNNER)
        .unwrap()
        .get_mut("openai.chatgpt_responses")
        .unwrap()
        .levels
        .retain(|tier| tier.as_str() != "fast");
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select_sol("fast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["service_tier_chosen"], "standard");
    let fallback = stored_tier_fallback(&harness, &reply.1);
    assert_eq!(fallback["requested"], "fast");
    assert_eq!(fallback["chosen"], "standard");
    assert_eq!(fallback["attempts"].as_array().unwrap().len(), 1);
    assert_eq!(fallback["attempts"][0]["tier"], "fast");
    assert_eq!(
        sol_tier_rejections(&fallback),
        vec![json!({
            "route": "openai/gpt-6.1-sol", "check": "runner",
            "credential_id": "chatgpt:0", "wire_family": "openai.chatgpt_responses"
        })]
    );
}

#[tokio::test]
async fn service_tier_live_broca_unknown_plan_records_plan_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup_for_method(
        &["unpublished"],
        true,
        true,
        "chatgpt",
        "openai.chatgpt_responses",
    );
    live_broca_credentials(&mut credentials);
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select_sol("fast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["service_tier_chosen"], "standard");
    let fallback = stored_tier_fallback(&harness, &reply.1);
    let rejections = sol_tier_rejections(&fallback);
    assert_eq!(rejections[0]["check"], "plan");
    assert_eq!(rejections[0]["credential_id"], "chatgpt:0");
}

#[tokio::test]
async fn service_tier_declared_runner_account_choice_opt_out_is_respected() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup(&["pro_200"], true, true);
    credentials
        .runner_declarations
        .get_mut(RUNNER)
        .unwrap()
        .follows_account_choice = false;
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select", &select("fast")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["service_tier_chosen"], "standard");
    assert_eq!(
        sol_tier_rejections(&stored_tier_fallback(&harness, &reply.1))[0]["check"],
        "runner"
    );
}

#[tokio::test]
async fn service_tier_pinned_variant_follow_up_steps_down_without_changing_model_or_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup(&["pro_200", "pro_500"], true, true);
    credentials.runner_service_tiers.clear();
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("tier-owner"), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = json!({
        "consumer":"mason", "requirements":{"iq":30},
        "pin":"openai/gpt-6.1-sol:xhigh", "task_ref":"wi-pinned-follow-up"
    });
    let launched = route.call("route.select", &request).await;
    assert_eq!(launched.0, FrameType::Response, "{}", launched.1);
    let first_account = runner
        .call(
            "route.account_for",
            &json!({
                "provider":"openai", "model":MODEL, "conversation_key":"pinned-session",
                "admission_id":"launch", "decision_id":launched.1["result"]["decision_id"]
            }),
        )
        .await;
    assert_eq!(first_account.0, FrameType::Response, "{}", first_account.1);
    assert_eq!(
        first_account.1["result"]["account"]["credential_id"],
        "oauth:0"
    );

    request["service_tier"] = json!("fast");
    let follow_up = route.call("route.select", &request).await;
    assert_eq!(follow_up.0, FrameType::Response, "{}", follow_up.1);
    let chosen = &follow_up.1["result"];
    assert_eq!(
        chosen["route"],
        json!({"provider":"openai", "model":MODEL, "variant":"xhigh"})
    );
    assert_eq!(chosen["runner"]["module"], RUNNER);
    assert_eq!(chosen["pin_outcome"], "pinned");
    assert_eq!(chosen["service_tier_chosen"], "standard");
    let id = chosen["decision_id"].as_str().unwrap();
    assert_eq!(
        harness.store().decision_service_tier(id).unwrap(),
        Some(("standard".into(), vec!["oauth:0".into(), "oauth:1".into()]))
    );
    let next_account = runner
        .call(
            "route.account_for",
            &json!({
                "provider":"openai", "model":MODEL, "conversation_key":"pinned-session",
                "admission_id":"follow-up", "decision_id":id
            }),
        )
        .await;
    assert_eq!(next_account.0, FrameType::Response, "{}", next_account.1);
    assert_eq!(
        next_account.1["result"]["account"]["credential_id"],
        first_account.1["result"]["account"]["credential_id"]
    );
}

#[tokio::test]
async fn stamped_select_inherits_daemon_project_tier_without_project_id() {
    use crate::managed_settings::test_daemon::ID;
    const CONSUMER: &str = "prefrontal.sidekick";

    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        config["consumers"][CONSUMER] = json!({"owner":"tier-owner","mode":"demand"});
    });
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{
            CONSUMER:{"service_tier":"fast"},
            "tier-override":{"service_tier":"ultrafast"}
        }}),
    );
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    for (consumer, settings, expected) in [
        ("prefrontal.sidekick.review", None, "fast"),
        (CONSUMER, None, "fast"),
        (CONSUMER, Some(json!({})), "fast"),
        (
            CONSUMER,
            Some(json!({"tier_consumer":"tier-override"})),
            "ultrafast",
        ),
    ] {
        let mut body = json!({"consumer":consumer,"requirements":{"iq":30}});
        if let Some(settings) = settings {
            body["settings"] = settings;
        }
        let reply = route.call("route.select", &body).await;
        assert_eq!(reply.0, FrameType::Response, "{body}: {}", reply.1);
        let result = &reply.1["result"];
        assert_eq!(result["service_tier_chosen"], expected, "{body}: {result}");
        assert!(result.get("project_settings").is_none(), "{result}");
    }
}

#[tokio::test]
async fn stamped_select_inherits_project_tier_advertised_only_to_consumers() {
    use crate::managed_settings::test_daemon::{Advertise, ID};
    use std::time::Duration;

    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.sidekick"] =
            json!({"owner":"tier-owner","mode":"demand","service_tier":"standard"});
    });
    let daemon = harness
        .project_daemon_advertising(Advertise::Describe)
        .await;
    daemon.set(
        ID,
        json!({"consumers":{"prefrontal.sidekick":{"service_tier":"fast"}}}),
    );
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    let request = json!({"consumer":"prefrontal.sidekick.review","requirements":{"iq":30}});
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let reply = route.call("route.select", &request).await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            let result = reply.1["result"].clone();
            if daemon.module_describes() > 0 || daemon.gets(ID) > 0 {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("project discovery did not finish");
    assert_eq!(result["service_tier_chosen"], "fast", "{result}");
    assert_eq!(daemon.module_describes(), 0);
    assert_eq!(daemon.describes(), 1);
    assert_eq!(daemon.gets(ID), 1);
}

#[tokio::test]
async fn daemon_service_tier_view_wins_over_project_file_and_updates_on_change() {
    use crate::managed_settings::test_daemon::ID;
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        config["consumers"]["sidekick"] =
            json!({"owner":"tier-owner","mode":"demand","service_tier":"standard"})
    });
    let project = harness.store_path().parent().unwrap().join("tier-project");
    let path = project.join(".cortexkit/striatum.jsonc");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        r#"{"consumers":{"sidekick":{"service_tier":"ultrafast"}}}"#,
    )
    .unwrap();
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{"sidekick":{"service_tier":"fast"}}}),
    );
    let route = harness.bind(reserved("tier-owner"), None).await;
    let legacy = route.call("route.select", &json!({"consumer":"sidekick", "requirements":{"iq":30}, "project_root":project, "workspace_root":project})).await;
    assert_eq!(legacy.0, FrameType::Response, "{}", legacy.1);
    assert_eq!(legacy.1["result"]["service_tier_chosen"], "standard");
    let request =
        json!({"consumer":"sidekick", "requirements":{"iq":30}, "settings":{"project_id":ID}});
    let first = route.call("route.select", &request).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(first.1["result"]["service_tier_chosen"], "fast");
    daemon.set(
        ID,
        json!({"consumers":{"sidekick":{"service_tier":"ultrafast"}}}),
    );
    daemon.changed(ID).await;
    let second = route.call("route.select", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["service_tier_chosen"], "ultrafast");
    let mut pinned = request;
    pinned["service_tier"] = json!("standard");
    let standard = route.call("route.select", &pinned).await;
    assert_eq!(standard.1["result"]["service_tier_chosen"], "standard");
}

#[tokio::test]
async fn service_tier_refused_pin_records_its_real_reason_once() {
    use striatum_store::DecisionListFilter;

    let _clock = crate::InjectedClock::set(NOW);
    for (pin, exclude, reason) in [
        ("openai/absent:xhigh", json!([]), "unknown_model"),
        ("openai/gpt-6.1-sol:xhigh", json!(["openai"]), "excluded"),
    ] {
        let (harness, mut credentials) = setup(&["pro_500"], true, true);
        credentials.runner_service_tiers.clear();
        harness.set_credentials(credentials);
        let route = harness
            .bind(reserved("tier-owner"), Some("pinned-project"))
            .await;
        let mut request = select("fast");
        request["pin"] = json!(pin);
        request["exclude_providers"] = exclude;
        request["task_ref"] = json!("wi-refused-follow-up");
        let refused = route.call("route.select", &request).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "pin_unavailable", "{}", refused.1);
        assert_eq!(refused.1["detail"]["reason"], reason, "{}", refused.1);
        let rows = harness
            .store()
            .list_decisions(&DecisionListFilter {
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        let row = harness
            .store()
            .get_decision_by_id(&rows[0].id)
            .unwrap()
            .unwrap();
        assert!(row.selected_route_key.is_none(), "{row:?}");
        assert_eq!(row.target_agent, "mason");
        assert_eq!(row.project_id.as_deref(), Some("pinned-project"));
        let explanation: Value =
            serde_json::from_str(row.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(explanation["failureReason"], reason);
        assert_eq!(explanation["refusal"]["code"], "pin_unavailable");
        assert_eq!(explanation["pin"], pin);
    }
}

#[tokio::test]
async fn stamped_legacy_panel_inherits_daemon_project_tier() {
    use crate::managed_settings::test_daemon::ID;

    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq"});
    });
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{"prefrontal-core.athena":{"service_tier":"fast"}}}),
    );
    let excluded: Vec<_> = harness
        .store()
        .get_facts()
        .unwrap()
        .iter()
        .filter(|fact| fact.provider_id != "openai" || fact.model_id != MODEL)
        .map(|fact| striatum_core::fact_to_route(fact).route_key)
        .collect();
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    let reply = route.call("route.select_panel", &json!({"taskId":"tier-panel","substrate":"broca","panelSpec":{"class":"AUDIT","diversity":"family"},"excludeRouteKeys":excluded})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let members = reply.1["result"]["members"].as_array().unwrap();
    assert_eq!(members.len(), 1, "{}", reply.1);
    assert_eq!(members[0]["promptParams"]["service_tier_chosen"], "fast");
}

#[tokio::test]
async fn service_tier_user_settings_reach_legacy_panel_members() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq"});
        config["consumers"]["prefrontal-core.athena"]["service_tier"] = json!("fast");
    });
    let excluded: Vec<_> = harness
        .store()
        .get_facts()
        .unwrap()
        .iter()
        .filter(|fact| fact.provider_id != "openai" || fact.model_id != MODEL)
        .map(|fact| striatum_core::fact_to_route(fact).route_key)
        .collect();
    let route = harness.bind(reserved("tier-owner"), None).await;
    let reply = route.call("route.select_panel", &json!({"taskId":"tier-panel","substrate":"broca","panelSpec":{"class":"AUDIT","diversity":"family"},"excludeRouteKeys":excluded})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let members = reply.1["result"]["members"].as_array().unwrap();
    assert_eq!(members.len(), 1, "{}", reply.1);
    assert_eq!(members[0]["promptParams"]["service_tier_chosen"], "fast");
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
    let fallback = stored_tier_fallback(&harness, &second.1);
    assert_eq!(fallback["affinity"], true);
    assert_eq!(fallback["attempts"], json!([]));
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
fn broca_turn_system_runner_discovery_keeps_absent_service_tiers() {
    let before = include_bytes!(
        "../../fixtures/contracts/broca-turn-system-v1/before/role-describe.response.json"
    );
    let after = include_bytes!(
        "../../fixtures/contracts/broca-turn-system-v1/after/role-describe.response.json"
    );
    for bytes in [before.as_slice(), after.as_slice()] {
        let reply: Value = serde_json::from_slice(bytes).unwrap();
        assert!(runners::service_tiers_from_describe(&reply).is_none());
    }
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
fn service_tier_kimi_apikey_plane_prices_fast_without_changing_subscription_draw() {
    // Captured Kimi responses: the ordinary row states zero, while the API-key
    // billing plane quotes the fast tier (Kimi's "Highspeed" twin) at $4 input
    // and $20 output per million tokens.
    let payload: Value = serde_json::from_str(include_str!(
        "../../fixtures/service-tier-v1/kimi-served-payloads.json"
    ))
    .unwrap();
    let catalog = crate::fusiform_catalog::PlaneCatalog::default()
        .with_test_catalog(
            &serde_json::to_vec(&payload["catalog.get — speed twin k3.1-pro plain"]["response"])
                .unwrap(),
        )
        .with_test_plane(
            &serde_json::to_vec(&payload["catalog.get — speed twin k3.1-pro apikey"]["response"])
                .unwrap(),
        );
    let mut credentials = BrocaCredentials::default();
    credentials.publish_billing_catalog(catalog);
    let mut fact = crate::tests::fact("kimi-code-plan-global", "k3.1-pro");
    credentials.price_for_method(&mut fact, "apikey", &Default::default());
    assert!(crate::service_tiers::model_offers(
        &credentials,
        &fact,
        "fast"
    ));
    let mut subscription = fact.clone();
    crate::service_tiers::price_fact(&credentials, &mut fact, "fast", false, 1.0);
    let rates = &fact.billing_plane.as_ref().unwrap().rates;
    assert_eq!(rates["rate.input"], BillingRate::Priced { rate: 4.0 });
    assert_eq!(rates["rate.output"], BillingRate::Priced { rate: 20.0 });
    assert_eq!(rates["rate.cache_read"], BillingRate::Priced { rate: 0.2 });
    assert_eq!(rates["rate.cache_write"], BillingRate::Priced { rate: 4.0 });
    assert!(!rates.contains_key("rate.reasoning"));
    crate::service_tiers::price_fact(&credentials, &mut subscription, "fast", true, 3.0);
    let rates = &subscription.billing_plane.as_ref().unwrap().rates;
    assert_eq!(rates["rate.input"], BillingRate::Priced { rate: 6.0 });
    assert_eq!(rates["rate.output"], BillingRate::Priced { rate: 30.0 });
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

#[tokio::test]
async fn daemon_settings_tier_consumer_precedence_and_request_cap() {
    use crate::managed_settings::test_daemon::ID;
    use cortexkit_role_routing::select::{
        check_answer, parse_select_request_serving, SelectAnswer,
    };
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| {
        for slot in ["spec_designer", "lab_planner"] {
            config["consumers"][slot]["pin"] = json!({"model":format!("openai/{MODEL}")});
        }
    });
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{
            "mason":{"service_tier":"standard"},
            "spec_designer":{"service_tier":"ultrafast"},
            "explicit":{"service_tier":"fast"},
            "lab_planner":{"service_tier":"fast"},
            "athena_synthesizer":{"service_tier":"ultrafast"}
        }}),
    );
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    for (slot, explicit, requested, expected) in [
        (Some("spec_designer"), Some("explicit"), None, "fast"),
        (
            Some("lab_planner"),
            Some("athena_synthesizer"),
            None,
            "ultrafast",
        ),
        (Some("spec_designer"), None, None, "ultrafast"),
        (Some("lab_planner"), None, None, "fast"),
        (None, None, None, "standard"),
        (
            Some("spec_designer"),
            Some("explicit"),
            Some("standard"),
            "standard",
        ),
    ] {
        let mut body =
            json!({"consumer":"mason","requirements":{"iq":30},"settings":{"project_id":ID}});
        if let Some(slot) = slot {
            body["pin_slot"] = json!(slot);
        }
        if let Some(explicit) = explicit {
            body["settings"]["tier_consumer"] = json!(explicit);
        }
        if let Some(requested) = requested {
            body["service_tier"] = json!(requested);
        }
        let reply = route.call("route.select", &body).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["service_tier_chosen"], expected,
            "{}",
            reply.1
        );
        let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
        check_answer(
            &parse_select_request_serving(&body, super::describe::CAPABILITIES).unwrap(),
            &answer,
        )
        .unwrap();
    }
    let reply = route
        .call(
            "route.select",
            &json!({"consumer":"mason","service_tier":"unknown","settings":{"project_id":ID}}),
        )
        .await;
    assert_eq!(reply.1["code"], "invalid_request");
    assert_eq!(reply.1["detail"]["field"], "service_tier");
}

#[tokio::test]
async fn stamped_panel_inherits_daemon_project_tier_without_project_id() {
    use crate::managed_settings::test_daemon::ID;
    use cortexkit_role_routing::panel::{
        check_panel_answer, parse_select_panel_request_serving, SelectPanelAnswer,
    };

    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| config["panels"]["audit"] = json!({"top_k":1,"by":"iq"}));
    let daemon = harness.project_daemon().await;
    daemon.set(ID, json!({"consumers":{"mason":{"service_tier":"fast"}}}));
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    for settings in [None, Some(json!({}))] {
        let mut body = json!({"class":"audit","consumer":"mason.review","available_routes":[format!("openai/{MODEL}")]});
        if let Some(settings) = settings {
            body["settings"] = settings;
        }
        let reply = route.call("route.select_panel", &body).await;
        assert_eq!(reply.0, FrameType::Response, "{body}: {}", reply.1);
        let answer: SelectPanelAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
        assert_eq!(answer.seats.len(), 1, "{body}: {}", reply.1);
        assert_eq!(
            answer.seats[0].answer.service_tier_chosen.as_deref(),
            Some("fast"),
            "{body}: {}",
            reply.1
        );
        assert!(answer.seats[0].answer.project_settings.is_none());
        check_panel_answer(
            &parse_select_panel_request_serving(&body, super::describe::CAPABILITIES).unwrap(),
            &answer,
        )
        .unwrap();
    }
}

#[tokio::test]
async fn panel_settings_only_fast_passes_contract_checker_and_requested_tier_caps_it() {
    use crate::managed_settings::test_daemon::ID;
    use cortexkit_role_routing::panel::{
        check_panel_answer, parse_select_panel_request_serving, SelectPanelAnswer,
    };
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_500"], true, true);
    harness.edit_config(|config| config["panels"]["audit"] = json!({"top_k":1,"by":"iq"}));
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{"explicit":{"service_tier":"fast"}}}),
    );
    let route = harness.bind(reserved("tier-owner"), Some(ID)).await;
    for (requested, expected) in [(None, "fast"), (Some("standard"), "standard")] {
        let mut body = json!({"class":"audit","consumer":"mason","available_routes":[format!("openai/{MODEL}")],"settings":{"project_id":ID,"tier_consumer":"explicit"}});
        if let Some(tier) = requested {
            body["service_tier"] = json!(tier);
        }
        let reply = route.call("route.select_panel", &body).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        let answer: SelectPanelAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
        assert_eq!(answer.seats.len(), 1, "{reply:?}");
        assert_eq!(
            answer.seats[0].answer.service_tier_chosen.as_deref(),
            Some(expected)
        );
        assert!(answer.seats[0].answer.project_settings.is_some());
        check_panel_answer(
            &parse_select_panel_request_serving(&body, super::describe::CAPABILITIES).unwrap(),
            &answer,
        )
        .unwrap();
    }
    let refused = route
        .call(
            "route.select_panel",
            &json!({"class":"audit","consumer":"mason","service_tier":"unknown"}),
        )
        .await;
    assert_eq!(refused.1["detail"]["field"], "service_tier");
    let mismatched = route
        .call(
            "route.select_panel",
            &json!({"class":"audit","consumer":"mason","settings":{"project_id":"other-project"}}),
        )
        .await;
    assert_eq!(mismatched.1["detail"]["field"], "settings.project_id");
}
