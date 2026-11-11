//! Selection order, runner choice, requirements and the one answer.
//!
//! Both wire shapes read the same order: a request pin, then a project pin,
//! then a continuing model, then the consumer's mode. A pin is one ladder
//! entry. The runner is the consumer's runner, otherwise the first runner
//! entry in array order, not a runner inferred from session data.

use std::sync::Arc;

use cortexkit_role_routing_conformance::CatalogModel;
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::{ModelFact, RouteStateStore};
use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

const CHEAP: &str = "cheap-provider/model-a";
const DEAR: &str = "dear-provider/model-b";

fn model(route_key: &str, tool_calls: bool, vision: bool, context: u64) -> CatalogModel {
    let (provider, model) = route_key.split_once('/').unwrap();
    CatalogModel {
        provider: provider.into(),
        model: model.into(),
        family: None,
        iq: 80,
        context_window: context,
        max_output_tokens: 32_000,
        tool_calls,
        modalities: vision.then(|| "image".to_owned()).into_iter().collect(),
        structured_output: false,
        priced_per_token: true,
    }
}

fn facts(specs: &[(&str, bool, bool, u64)]) -> Vec<ModelFact> {
    specs
        .iter()
        .enumerate()
        .map(|(rank, (key, tools, vision, context))| {
            let mut fact = catalog_fact(&model(key, *tools, *vision, *context), rank);
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
        .collect()
}

fn harness(specs: &[(&str, bool, bool, u64)]) -> Arc<Harness> {
    let harness = Harness::new("order");
    let facts = facts(specs);
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
            upstream_status: None, model_type: None, model_kind: None,
            lineage: fact.family.clone(),
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
    credentials.publish_plan_prices(&json!({ "prices": [] }));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([
            { "module": "zeta-runner" },
            { "module": "alpha-runner" }
        ]);
    });
    harness
}

fn select(consumer: &str) -> Value {
    json!({ "consumer": consumer, "requirements": { "iq": 30 } })
}

#[tokio::test]
async fn non_chat_catalog_rows_cannot_be_selected_or_pinned() {
    for (classification, reason) in [
        (json!({"model.type": "transcription"}), "not_chat_model:transcription"),
        (json!({"model.type": "image"}), "not_chat_model:image"),
        (json!({"model.type": "future-engine"}), "not_chat_model:future-engine"),
        (json!({"model.kind": "decision"}), "not_chat_kind:decision"),
        (json!({"model.kind": "extraction"}), "not_chat_kind:extraction"),
        (json!({"model.type": "chat"}), ""),
        (json!({}), ""),
    ] {
        check_chat_classification(classification, reason).await;
    }
}

async fn check_chat_classification(classification: Value, reason: &str) {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"topK": 1, "by": "iq", "extra": 0, "floor": 0});
    });
    let mut payload = json!({"catalog_version": 2, "resolved_at_ms": 2, "models": {
        CHEAP: {"existence": "present",
            "capability.input_modalities": ["audio", "text"],
            "capability.output_modalities": ["text"], "wire.family": "openai-chat",
            "capability.tool_call": true, "limit.context": 200000, "limit.output": 32000}
    }});
    payload["models"][CHEAP].as_object_mut().unwrap().extend(classification.as_object().unwrap().clone());
    if classification["model.type"] == "image" {
        payload["models"][CHEAP]["capability.output_modalities"] = json!(["image"]);
    }
    let catalog = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&payload).unwrap()).unwrap();
    let (updates, cursor) = catalog.parts();
    harness.store().apply_fusiform_catalog(updates, cursor).unwrap();
    harness.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(catalog));
    let route = harness.bind(reserved("example-consumer"), None).await;
    let automatic = route.call("route.select", &select("mason")).await;
    let excluded = !reason.is_empty();
    assert_eq!(route_of(&automatic), if excluded { DEAR } else { CHEAP }, "{}", automatic.1);
    let legacy = json!({"consumer": "mason", "targetAgent": "mason",
        "executor": "broca", "substrate": "broca", "requirements": {"iq": 30, "eq": 0}});
    let legacy_selected = route.call("route.select", &legacy).await;
    assert_eq!(legacy_route(&legacy_selected), if excluded { DEAR } else { CHEAP });
    for pin in [CHEAP.to_owned(), format!("{CHEAP}:high")] {
        let pinned = route.call("route.select", &json!({"consumer": "mason", "requirements": {"iq": 30}, "pin": pin})).await;
        let mut legacy_pin = legacy.clone();
        legacy_pin["modelPin"] = json!(pin);
        let legacy_pinned = route.call("route.select", &legacy_pin).await;
        for reply in [pinned, legacy_pinned] {
            if excluded {
                assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
                assert!(reply.1.to_string().contains(reason), "{}", reply.1);
            } else {
                assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            }
        }
    }
    let choices = route.call("route.head_choices", &json!({"runner_module": "zeta-runner"})).await;
    assert_eq!(choices.0, FrameType::Response, "{}", choices.1);
    let models = choices.1["result"]["models"].as_array().unwrap();
    assert!(models.iter().any(|model| model["provider"] == "dear-provider"), "{}", choices.1);
    assert_eq!(models.iter().any(|model| model["provider"] == "cheap-provider"), !excluded, "{}", choices.1);
    if excluded {
        let panel = route.call("route.select_panel", &json!({"consumer": "mason", "class": "audit", "available_routes": [CHEAP, DEAR]})).await;
        assert_eq!(panel.0, FrameType::Response, "{}", panel.1);
        let seats = panel.1["result"]["seats"].as_array().unwrap();
        assert_eq!(seats.len(), 1, "{}", panel.1);
        assert_eq!(seats[0]["answer"]["route"]["provider"], "dear-provider", "{}", panel.1);
        let operator = harness.bind(json!({"kind": "direct"}), None).await;
        let sweep = operator.call("route.what_if", &json!({"consumer": "mason", "iqs": [30]})).await;
        assert_eq!(sweep.0, FrameType::Response, "{}", sweep.1);
        assert!(sweep.1["result"]["results"][0]["excluded"].as_array().unwrap().iter()
            .any(|row| row["route"] == CHEAP && row["reason"] == reason), "{}", sweep.1);
        let decision = legacy_selected.1["result"]["decisionID"].as_str().unwrap();
        let explanation = route.call("route.explain", &json!({"decisionID": decision})).await;
        assert_eq!(explanation.0, FrameType::Response, "{}", explanation.1);
        assert!(explanation.1.to_string().contains(reason), "{}", explanation.1);
    }
}

#[tokio::test]
async fn task_profiles_use_the_recorded_workload_and_effective_iq_without_store_reads() {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["iq"] = json!(75);
        config["models"][CHEAP] = json!({"subscription_multiplier": 10_000.0});
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let cold = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&cold), CHEAP);

    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    connection
        .execute_batch("DROP INDEX model_route_usage_task_idx")
        .unwrap();
    assert!(harness
        .store()
        .closed_task_usage(super::now_ms() as i64)
        .is_err());
    let profiles =
        super::profile_tests::lookup("mason", 75.0, &[(CHEAP, 10_000_000.0), (DEAR, 100_000.0)]);
    let answer = route
        .select_with_profiles(&select("mason"), &profiles)
        .await
        .unwrap();
    assert_eq!(
        format!(
            "{}/{}",
            answer["route"]["provider"].as_str().unwrap(),
            answer["route"]["model"].as_str().unwrap()
        ),
        DEAR
    );
    let pricing =
        super::profile_tests::task_pricing(&harness, answer["decision_id"].as_str().unwrap());
    assert_eq!(pricing["tokens"]["outputExcludingReasoning"], 100_000.0);
    assert_eq!(pricing["profile"]["taskCount"], 20);
    assert_eq!(pricing["valueMultiplier"], 1.0);

    let mut pinned = select("mason");
    pinned["pin"] = json!(CHEAP);
    let answer = route
        .select_with_profiles(&pinned, &profiles)
        .await
        .unwrap();
    assert_eq!(answer["route"]["provider"], "cheap-provider");
    let pricing =
        super::profile_tests::task_pricing(&harness, answer["decision_id"].as_str().unwrap());
    assert_eq!(pricing["tokens"]["outputExcludingReasoning"], 10_000_000.0);
    assert_eq!(
        pricing["valueMultiplier"], 1.0,
        "per-route multipliers never affect task prices"
    );
}

fn route_of(reply: &(FrameType, Value)) -> String {
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

fn below_bar_harness() -> Arc<Harness> {
    let harness = harness(&[
        (CHEAP, true, true, 200_000),
        ("strong-provider/top", true, true, 200_000),
        ("fenced-provider/top", true, true, 200_000),
        ("manual-provider/top", true, true, 200_000),
    ]);
    for mut fact in harness.store().get_facts().unwrap() {
        fact.scores.elo = match fact.provider_id.as_str() {
            "cheap-provider" => 620.0,
            "fenced-provider" => 1_000.0,
            "strong-provider" | "manual-provider" => 1_100.0,
            _ => continue,
        };
        harness
            .store()
            .upsert_fact(
                &fact,
                striatum_core::route_state_store::FactSource::Override,
            )
            .unwrap();
    }
    harness.edit_config(|config| {
        config["models"]["manual-provider/top"] = json!({"pin_only": true});
    });
    // The quota-blocked route still sets the maximum Elo used to compute the
    // iq-80 quality bar. Excluding the failed provider and reserving another
    // strong route for pins must not make the remaining weaker route adequate.
    harness
        .store()
        .update_route_state(
            "fenced-provider/top",
            "fenced-provider/top",
            &striatum_core::RouteStateUpdate {
                quota_exhausted_until: Some(crate::now_ms() as f64 + 600_000.0),
                ..Default::default()
            },
        )
        .unwrap();
    harness
}

#[tokio::test]
async fn below_bar_policy_is_shared_by_v1_legacy_and_what_if_and_bypassed_by_pins() {
    let harness = below_bar_harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let v1 = json!({"consumer": "mason", "requirements": {"iq": 80},
        "exclude": ["strong-provider/top"], "detail": true});
    let legacy = json!({"consumer": "mason", "targetAgent": "mason",
        "executor": "broca", "substrate": "broca", "requirements": {"iq": 80, "eq": 0},
        "excludeRouteKeys": ["strong-provider"]});
    let refused = route.call("route.select", &v1).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["detail"]["reason"], "all_inadequate");
    assert_eq!(route.call("route.select", &legacy).await.0, FrameType::Error);
    harness.edit_config(|config| config["consumers"]["mason"]["below_bar"] = json!("best_effort"));
    let selected = route.call("route.select", &v1).await;
    assert_eq!(route_of(&selected), CHEAP);
    assert!(
        (selected.1["result"]["detail"]["score"]["adequacy"]
            .as_f64()
            .unwrap()
            - 0.67)
            .abs()
            < 0.01
    );
    assert_eq!(
        legacy_route(&route.call("route.select", &legacy).await),
        CHEAP
    );

    harness.edit_config(|config| {
        config["consumers"]["mason"]["below_bar"] = json!("refuse");
        config["consumers"]["mason.review"] = json!({});
    });
    for consumer in ["mason", "mason.review"] {
        let mut request = v1.clone();
        request["consumer"] = json!(consumer);
        let refused = route.call("route.select", &request).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "no_route", "{}", refused.1);
        assert_eq!(
            refused.1["detail"]["reason"], "all_inadequate",
            "{}",
            refused.1
        );
        assert_eq!(
            refused.1["detail"]["adequacyFloor"],
            striatum_core::ADEQUACY_FLOOR
        );
        assert!(
            (refused.1["detail"]["bestRejected"]["adequacy"]
                .as_f64()
                .unwrap()
                - 0.67)
                .abs()
                < 0.01
        );
        let details = refused.1["detail"]["rejected"].to_string();
        for reason in ["excluded", "pin_only", "quota_exhausted"] {
            assert!(details.contains(reason), "{details}");
        }
        request["pin"] = json!(CHEAP);
        assert_eq!(route_of(&route.call("route.select", &request).await), CHEAP);
    }
    let refused = route.call("route.select", &legacy).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "no_adequate_route", "{}", refused.1);
    assert!(
        refused.1["message"]
            .as_str()
            .unwrap()
            .contains("adequacy 0.67"),
        "{}",
        refused.1
    );
    let mut pinned = legacy.clone();
    pinned["modelPin"] = json!(CHEAP);
    assert_eq!(
        legacy_route(&route.call("route.select", &pinned).await),
        CHEAP
    );

    let operator = harness.bind(json!({"kind": "direct"}), None).await;
    let sweep = json!({"consumer": "mason.review", "iqs": [80], "eq": 0,
        "policy_patch": {"models": {"strong-provider/top": {"not_for": ["mason"]}}}});
    let evaluation = operator.call("route.what_if", &sweep).await;
    assert_eq!(evaluation.0, FrameType::Response, "{}", evaluation.1);
    let row = &evaluation.1["result"]["results"][0];
    assert!(row["pick"].is_null(), "{row}");
    assert_eq!(row["refusal"]["code"], "no_route", "{row}");
    assert_eq!(
        row["refusal"]["detail"]["reason"], "all_inadequate",
        "{row}"
    );
    assert!(
        (row["refusal"]["detail"]["bestRejected"]["adequacy"]
            .as_f64()
            .unwrap()
            - 0.67)
            .abs()
            < 0.01
    );
}

/// A fresh ladder answers its first allowed entry. Demand answers the cheaper
/// adequate model, which is listed second so the two modes cannot agree by accident.
#[tokio::test]
async fn a_fresh_ladder_takes_the_first_allowed_entry_and_demand_takes_the_cheapest() {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!([DEAR, CHEAP]);
        config["consumers"]["sidekick"]["mode"] = json!("demand");
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let ladder = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&ladder), DEAR);
    let demand = route.call("route.select", &select("sidekick")).await;
    assert_eq!(route_of(&demand), CHEAP);
}

/// A continuing conversation keeps its model when an earlier ladder entry
/// recovers or a cheaper model would now win. A failure ends route affinity,
/// without cooling the route or changing account affinity.
#[tokio::test]
async fn a_continuing_conversation_keeps_its_model_until_failure_or_exclusion() {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!([DEAR]);
        config["models"][CHEAP]["not_for"] = json!(["mason"]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut first = select("mason");
    first["affinity"] = json!("conversation-1");
    let first = route.call("route.select", &first).await;
    assert_eq!(route_of(&first), DEAR);

    // The earlier entry recovers, and a cheaper model is adequate. Affinity holds.
    harness.edit_config(|config| {
        config["models"][CHEAP]["not_for"] = json!([]);
        config["consumers"]["mason"]["ladder"] = json!([CHEAP, DEAR]);
    });
    let mut sticky = select("mason");
    sticky["affinity"] = json!("conversation-1");
    let again = route.call("route.select", &sticky).await;
    assert_eq!(route_of(&again), DEAR, "{}", again.1);
    let standing = again.1["result"]["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let report = route
        .call(
            "route.report",
            &json!({ "decision_id": standing, "outcome": "failed" }),
        )
        .await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    let after_failure = route.call("route.select", &sticky).await;
    assert_eq!(route_of(&after_failure), CHEAP, "{}", after_failure.1);

    harness.edit_config(|config| {
        config["models"][CHEAP]["not_for"] = json!(["mason"]);
    });
    let left = route.call("route.select", &sticky).await;
    assert_eq!(route_of(&left), DEAR, "{}", left.1);
}

#[tokio::test]
async fn request_and_project_pins_beat_a_continuing_conversation_on_the_live_handler() {
    const PIN: &str = "pin-provider/model-c";
    let harness = harness(&[
        (CHEAP, true, true, 200_000),
        (DEAR, true, true, 200_000),
        (PIN, true, true, 200_000),
    ]);
    harness.edit_config(|config| {
        config.as_object_mut().unwrap().remove("accounts");
    });
    let route = harness
        .bind(reserved("example-consumer"), Some("order-project"))
        .await;
    let mut request = select("mason");
    request["affinity"] = json!("pin-precedence");
    request["pin"] = json!(DEAR);
    assert_eq!(route_of(&route.call("route.select", &request).await), DEAR);

    harness.edit_config(|config| {
        config["consumers"]["mason"]["projects"]["order-project"] = json!({"pin": PIN});
    });
    request.as_object_mut().unwrap().remove("pin");
    assert_eq!(route_of(&route.call("route.select", &request).await), PIN);

    request["pin"] = json!(CHEAP);
    assert_eq!(route_of(&route.call("route.select", &request).await), CHEAP);
    request["pin"] = json!("absent/model");
    assert_eq!(
        route.call("route.select", &request).await.0,
        FrameType::Error,
        "a request pin must not fall through to a project pin or affinity"
    );
}

/// A legacy targetAgent body carries no affinity, so a later body follows the
/// current ladder instead of the model the first body answered.
#[tokio::test]
async fn a_legacy_target_agent_body_is_fresh() {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!([DEAR]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut legacy = select("mason");
    legacy["targetAgent"] = json!("mason");
    legacy["requirements"]["eq"] = json!(0);
    legacy["executor"] = json!("broca");
    legacy["substrate"] = json!("broca");
    let first = route.call("route.select", &legacy).await;
    assert_eq!(legacy_route(&first), DEAR);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["ladder"] = json!([CHEAP]);
    });
    let second = route.call("route.select", &legacy).await;
    assert_eq!(legacy_route(&second), CHEAP);
}

/// Tools, context and image modality each drop a cheaper model that cannot
/// serve them. Pay-per-token follows the consumer flag, not the consumer's name.
#[tokio::test]
async fn requirements_follow_tools_context_modality_and_the_pay_per_token_flag() {
    let harness = harness(&[(CHEAP, false, false, 8_000), (DEAR, true, true, 200_000)]);
    let route = harness.bind(reserved("example-consumer"), None).await;
    let plain = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&plain), CHEAP);

    let mut tools = select("mason");
    tools["requirements"]["tools"] = json!("required");
    assert_eq!(route_of(&route.call("route.select", &tools).await), DEAR);

    let mut context = select("mason");
    context["requirements"]["context_window"] = json!(100_000);
    assert_eq!(route_of(&route.call("route.select", &context).await), DEAR);

    let mut image = select("mason");
    image["requirements"]["modalities"] = json!(["image"]);
    let image = route.call("route.select", &image).await;
    assert_eq!(route_of(&image), DEAR);
}

/// The priced-per-token rule follows the consumer's flag. A name that used to
/// imply the rule does not, once the flag says otherwise.
#[tokio::test]
async fn pay_per_token_follows_the_consumer_flag_not_the_name() {
    let specs = [(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)];
    let harness = harness(&specs);
    let mut credentials = BrocaCredentials::api_test_credentials(&facts(&specs));
    credentials.publish_plan_prices(&json!({"prices": [{"provider_id": "cheap-provider"}]}));
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("example-consumer"), None).await;

    let open = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&open), CHEAP, "{}", open.1);

    harness.edit_config(|config| {
        config["consumers"]["mason"]["pay_per_token"] = json!(true);
        config["providers"]["cheap-provider"]["subscription"] = json!(true);
    });
    let priced = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&priced), DEAR, "{}", priced.1);

    harness.edit_config(|config| {
        config["consumers"]["codemode"]["pay_per_token"] = json!(false);
    });
    let named = route.call("route.select", &select("codemode")).await;
    assert_eq!(
        route_of(&named),
        CHEAP,
        "an explicit pay_per_token false is not the historical class: {}",
        named.1
    );
    harness.edit_config(|config| {
        config["consumers"]["codemode"]
            .as_object_mut()
            .unwrap()
            .remove("pay_per_token");
    });
    assert_eq!(
        route_of(&route.call("route.select", &select("codemode")).await),
        CHEAP
    );
    let legacy = route
        .call(
            "route.select",
            &json!({
                "targetAgent": "codemode", "consumer": "codemode",
                "executor": "broca", "substrate": "broca",
                "requirements": {"iq": 10, "eq": 0}
            }),
        )
        .await;
    assert_eq!(legacy_route(&legacy), CHEAP, "{}", legacy.1);
}

/// Runner choice is the consumer's runner, otherwise the first array entry,
/// including when the module ids are in reverse alphabetical order. A converted
/// two-runner array keeps that order. Session data does not choose the runner.
#[tokio::test]
async fn runner_choice_is_the_consumer_or_the_first_array_entry() {
    let converted = crate::config_convert::convert_value(json!({
        "model_routing": {
            "runners": [
                { "module": "zeta-runner" },
                { "module": "alpha-runner" }
            ]
        }
    }))
    .unwrap();
    assert_eq!(converted["runners"][0]["module"], "zeta-runner");
    assert_eq!(converted["runners"][1]["module"], "alpha-runner");

    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["runners"] = converted["runners"].clone();
    });
    let route = harness
        .bind(reserved("example-consumer"), Some("project-1"))
        .await;
    let reply = route.call("route.select", &select("mason")).await;
    assert_eq!(reply.1["result"]["runner"]["module"], "zeta-runner");

    harness.edit_config(|config| {
        config["consumers"]["mason"]["runner"] = json!("alpha-runner");
    });
    let named = route.call("route.select", &select("mason")).await;
    assert_eq!(named.1["result"]["runner"]["module"], "alpha-runner");
}

#[tokio::test]
async fn the_consumer_runner_must_reach_the_chosen_route_on_both_wire_shapes() {
    let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module": "zeta-runner", "routes": [CHEAP]},
            {"module": "alpha-runner", "routes": [DEAR]}
        ]);
        config["consumers"]["mason"]["runner"] = json!("alpha-runner");
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let answer = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&answer), DEAR);
    assert_eq!(answer.1["result"]["runner"]["module"], "alpha-runner");
    let legacy = route
        .call(
            "route.select",
            &json!({
                "targetAgent": "mason", "consumer": "mason", "substrate": "broca",
                "requirements": {"iq": 10, "eq": 0}
            }),
        )
        .await;
    assert_eq!(legacy_route(&legacy), DEAR, "{}", legacy.1);
    assert_eq!(legacy.1["result"]["runner"]["module"], "alpha-runner");
    assert_eq!(
        legacy.1["result"]["runner"]["route"],
        legacy.1["result"]["route"]
    );
    assert_eq!(
        legacy.1["result"]["decision_id"],
        legacy.1["result"]["decisionID"]
    );
    assert_eq!(legacy.1["result"]["context_window"], 200_000);
    assert_eq!(legacy.1["result"]["max_output_tokens"], 32_000);
}

/// The v1 answer carries the route, runner, variant, limits, decision id and
/// expiry from the one pass. The caller does not select a second time.
#[tokio::test]
async fn the_answer_carries_route_runner_variant_limits_decision_and_expiry() {
    let _clock = crate::InjectedClock::set(1_792_000_000_000);
    let harness = harness(&[(CHEAP, true, true, 200_000)]);
    let route = harness.bind(reserved("example-consumer"), None).await;
    let reply = route.call("route.select", &select("mason")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let result = &reply.1["result"];
    assert!(result["decision_id"].is_string(), "{result}");
    assert_eq!(result["route"]["provider"], "cheap-provider");
    assert_eq!(result["route"]["model"], "model-a");
    assert_eq!(result["route"]["variant"], Value::Null);
    assert_eq!(result["runner"]["module"], "zeta-runner");
    assert_eq!(result["runner"]["model"], result["route"]);
    assert!(result["context_window"].as_u64().unwrap() > 0, "{result}");
    assert!(
        result["max_output_tokens"].as_u64().unwrap() > 0,
        "{result}"
    );
    assert_eq!(result["expires_at_ms"], 1_792_000_600_000_u64, "{result}");
    // One decision was recorded for the one select.
    assert_eq!(
        harness
            .store()
            .v1_decision(result["decision_id"].as_str().unwrap())
            .unwrap()
            .unwrap()
            .runner_module,
        "zeta-runner"
    );
}

#[tokio::test]
async fn a_nonlegacy_consumer_runs_its_inherited_variant_in_scoring_and_launch() {
    let harness = harness(&[(CHEAP, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["variants"] = json!({ CHEAP: "low" });
        config["consumers"]["mc_summary"]["variants"] = json!({ CHEAP: "xhigh" });
        config["consumers"]["mc_summary.child"] = json!({});
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let profiles = super::profile_tests::lookup(
        "mc_summary.child",
        30.0,
        &[(CHEAP, 1_000.0), ("cheap-provider/model-a/xhigh", 2_000.0)],
    );
    let result = route
        .select_with_profiles(&select("mc_summary.child"), &profiles)
        .await
        .unwrap();
    assert_eq!(result["route"]["variant"], "xhigh", "{result}");
    assert_eq!(result["runner"]["model"]["variant"], "xhigh");
    let decision = result["decision_id"].as_str().unwrap();
    assert_eq!(
        harness
            .store()
            .v1_decision(decision)
            .unwrap()
            .unwrap()
            .variant
            .as_deref(),
        Some("xhigh")
    );
    let pricing = super::profile_tests::task_pricing(&harness, decision);
    assert_eq!(pricing["tokens"]["outputExcludingReasoning"], 2_000.0);
}

/// A mason's reasoning variant comes from the consumer's `variants` config on
/// both wire shapes. Core moves unpinned masons from the older select to the
/// v1 one, so a v1 answer that dropped the variant would launch every such
/// mason at the route's default effort instead of the configured one.
#[tokio::test]
async fn a_v1_mason_runs_the_consumer_variant_the_older_select_chose() {
    let harness = harness(&[(CHEAP, true, true, 200_000)]);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["variants"] = json!({ CHEAP: "xhigh" });
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let v1 = route.call("route.select", &select("mason")).await;
    assert_eq!(route_of(&v1), CHEAP);
    let result = &v1.1["result"];
    assert_eq!(result["route"]["variant"], "xhigh", "{result}");
    // Core builds the Broca launch target from the runner's model.
    assert_eq!(result["runner"]["model"]["variant"], "xhigh", "{result}");
    assert_eq!(
        harness
            .store()
            .v1_decision(result["decision_id"].as_str().unwrap())
            .unwrap()
            .unwrap()
            .variant
            .as_deref(),
        Some("xhigh")
    );

    let profiles = super::profile_tests::lookup(
        "mason",
        30.0,
        &[(CHEAP, 1_000.0), ("cheap-provider/model-a/xhigh", 2_000.0)],
    );
    let profiled = route
        .select_with_profiles(&select("mason"), &profiles)
        .await
        .unwrap();
    assert_eq!(profiled["route"]["variant"], "xhigh");
    let pricing =
        super::profile_tests::task_pricing(&harness, profiled["decision_id"].as_str().unwrap());
    assert_eq!(pricing["tokens"]["outputExcludingReasoning"], 2_000.0);

    let mut legacy = select("mason");
    legacy["targetAgent"] = json!("mason");
    legacy["executor"] = json!("broca");
    legacy["substrate"] = json!("broca");
    legacy["requirements"]["eq"] = json!(0);
    let older = route.call("route.select", &legacy).await;
    assert_eq!(legacy_route(&older), CHEAP);
    assert_eq!(
        older.1["result"]["selected"]["promptParams"]["variant"], "xhigh",
        "{}",
        older.1
    );
}

#[tokio::test]
async fn legacy_provider_reset_evidence_schedules_checks_on_both_select_wires() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    let now = 1_793_149_650_000;
    let _clock = crate::InjectedClock::set(now as u64);
    for v1 in [true, false] {
        // Readable reset dates remain evidence, even after the date passes.
        // Missing/unreadable dates remain unknown, never an invented expiry.
        // An ordinary interruption without a reset is not quota evidence.
        for (reset, expected_until, sibling, account_exhausted) in [
            (
                Some(now + 5 * 86_400_000),
                Some(now + 5 * 86_400_000),
                false,
                true,
            ),
            (Some(now - 6_000), Some(now - 6_000), false, true),
            (Some(i64::MAX), Some(0), false, true),
            (None, Some(0), false, true),
            (
                Some(now + 20 * 86_400_000),
                Some(now + 20 * 86_400_000),
                false,
                true,
            ),
            (
                Some(now + 5 * 86_400_000),
                Some(now + 5 * 86_400_000),
                true,
                true,
            ),
            (None, None, false, false),
        ] {
            let harness = harness(&[(CHEAP, true, true, 200_000), (DEAR, true, true, 200_000)]);
            harness.edit_config(|config| {
                config.as_object_mut().unwrap().remove("accounts");
            });
            let mut credentials = BrocaCredentials::api_test_credentials(&facts(&[
                (CHEAP, true, true, 200_000),
                (DEAR, true, true, 200_000),
            ]));
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "worker-keys".into(),
                    credentials: ["cheap-provider", "dear-provider"]
                        .into_iter()
                        .map(|provider| (provider, format!("apikey:{provider}")))
                        .chain(sibling.then(|| ("cheap-provider", "apikey:cheap-sibling".into())))
                        .map(|(provider, id)| ListedCredential {
                            id,
                            state: "active".into(),
                            categories: vec!["llm-provider".into()],
                            provider_ids: vec![provider.into()],
                            credential_type: "apikey".into(),
                            auth_method: Some("apikey".into()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                now as u64,
            );
            harness.set_credentials(credentials);
            let route = harness.bind(reserved("example-consumer"), None).await;
            let mut legacy = select("mason");
            legacy["targetAgent"] = json!("mason");
            legacy["executor"] = json!("broca");
            legacy["substrate"] = json!("broca");
            legacy["requirements"]["eq"] = json!(0);
            let v1_select = select("mason");
            let first = route
                .call("route.select", if v1 { &v1_select } else { &legacy })
                .await;
            let decision = if v1 {
                assert_eq!(route_of(&first), CHEAP);
                &first.1["result"]["decision_id"]
            } else {
                assert_eq!(legacy_route(&first), CHEAP);
                &first.1["result"]["decisionID"]
            };
            if v1 {
                // Multiple serving keys leave no single frozen identity. The
                // worker's reported key must still be checked against the listing.
                harness
                    .store()
                    .set_decision_provider_identity(
                        decision.as_str().unwrap(),
                        &striatum_core::route_state_store::ProviderOutcome {
                            account_ref: "provider-alias".into(),
                            credential_id: None,
                            resets_at_ms: None,
                            credential_refusal: None,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                assert!(harness
                    .store()
                    .decision_provider_identity(decision.as_str().unwrap())
                    .unwrap()
                    .unwrap()
                    .credential_id
                    .is_none());
            }
            let outcome = route
                .call(
                    "route.set_decision_outcome",
                    &json!({
                        "decisionID": decision,
                        "outcome": "interrupted",
                        "lastError": if account_exhausted { "account_limit_exhausted: Go usage limit exceeded" } else { "caller stopped the run" },
                        "providerOutcome": {"accountRef": "apikey:cheap-provider",
                            "credentialId": "apikey:cheap-provider", "resetsAtMs": reset},
                    }),
                )
                .await;
            assert_eq!(outcome.0, FrameType::Response, "{}", outcome.1);
            assert_eq!(outcome.1["result"]["ok"], true, "{}", outcome.1);
            let blocked = expected_until.is_some() && !sibling;
            let expected = if blocked { DEAR } else { CHEAP };
            assert_eq!(
                route_of(&route.call("route.select", &select("mason")).await),
                expected
            );
            assert_eq!(
                legacy_route(&route.call("route.select", &legacy).await),
                expected
            );
            let mut pinned = select("mason");
            pinned["pin"] = json!(CHEAP);
            let pinned = route.call("route.select", &pinned).await;
            let mut legacy_pin = legacy.clone();
            legacy_pin["modelPin"] = json!(CHEAP);
            let legacy_pin = route.call("route.select", &legacy_pin).await;
            if blocked {
                assert_eq!(pinned.0, FrameType::Error, "{}", pinned.1);
                assert_eq!(legacy_pin.0, FrameType::Error, "{}", legacy_pin.1);
                let mut only_blocked = select("mason");
                only_blocked["exclude"] = json!([DEAR]);
                let no_alternative = route.call("route.select", &only_blocked).await;
                assert_eq!(no_alternative.0, FrameType::Error, "{}", no_alternative.1);
                assert_eq!(no_alternative.1["code"], "no_route");
                assert_eq!(no_alternative.1["detail"]["reason"], "accounts_blocked");
                assert_eq!(no_alternative.1["detail"]["reset_at_ms"], now + 15 * 60_000,);
            } else {
                assert_eq!(route_of(&pinned), CHEAP);
                assert_eq!(legacy_route(&legacy_pin), CHEAP);
            }
            let fences = harness.store().account_route_fences(now).unwrap();
            if let Some(expected_until) = expected_until {
                assert_eq!(fences.len(), 1);
                assert_eq!(fences[0].credential_id, "apikey:cheap-provider");
                assert_eq!(fences[0].model, "model-a");
                assert_eq!(fences[0].until_ms, expected_until);
                assert!(!harness
                    .store()
                    .account_route_fences(fences[0].until_ms)
                    .unwrap()
                    .is_empty());
                assert_eq!(
                    route_of(&route.call("route.select", &select("mason")).await),
                    expected,
                    "reading a future diagnostic time cannot expire quota evidence"
                );
            } else {
                assert!(fences.is_empty());
            }
            let row = harness
                .store()
                .get_decision_by_id(decision.as_str().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(row.outcome.as_deref(), Some("interrupted"));
        }
    }
}

#[tokio::test]
async fn both_decision_wires_freeze_the_selected_account_class() {
    use striatum_core::accounts::AllowedAccountClass;
    let harness = harness(&[(CHEAP, true, true, 200_000)]);
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut ids = Vec::new();
    for pay_per_token in [false, true] {
        harness.edit_config(|config| {
            config["consumers"]["mason"]["pay_per_token"] = json!(pay_per_token)
        });
        let expected = if pay_per_token {
            AllowedAccountClass::PricedPerTokenOnly
        } else {
            AllowedAccountClass::NotPayPerToken
        };
        let v1 = route.call("route.select", &select("mason")).await;
        assert_eq!(route_of(&v1), CHEAP);
        let id = v1.1["result"]["decision_id"].as_str().unwrap().to_owned();
        assert_eq!(
            harness
                .store()
                .v1_decision(&id)
                .unwrap()
                .unwrap()
                .allowed_account_class,
            expected
        );
        ids.push((id, expected));
        let mut legacy = select("mason");
        legacy["targetAgent"] = json!("mason");
        legacy["executor"] = json!("broca");
        legacy["substrate"] = json!("broca");
        legacy["requirements"]["eq"] = json!(0);
        let answer = route.call("route.select", &legacy).await;
        assert_eq!(legacy_route(&answer), CHEAP);
        ids.push((
            answer.1["result"]["decisionID"]
                .as_str()
                .unwrap()
                .to_owned(),
            expected,
        ));
    }
    harness.edit_config(|config| config["consumers"]["mason"]["pay_per_token"] = json!(false));
    for (id, expected) in ids {
        assert_eq!(
            harness.store().decision_account_class(&id).unwrap(),
            Some(expected)
        );
    }
}
