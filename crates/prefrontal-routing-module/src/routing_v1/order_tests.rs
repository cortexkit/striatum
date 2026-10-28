//! Selection order, runner choice, requirements and the one answer.
//!
//! Both wire shapes read the same order: a request pin, then a project pin,
//! then a continuing model, then the consumer's mode. A pin is one ladder
//! entry. The runner is the consumer's runner, otherwise the first runner
//! entry in array order, not a runner inferred from session data.

use std::sync::Arc;

use cortexkit_role_routing_conformance::CatalogModel;
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use prefrontal_routing_core::ModelFact;
use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate};
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
async fn legacy_provider_reset_fences_serving_api_key_until_reset_on_both_select_wires() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    let now = 1_793_149_650_000;
    let _clock = crate::InjectedClock::set(now as u64);
    for v1 in [true, false] {
        for (reset, sibling) in [
            (now + 5 * 86_400_000, false),
            (now - 6_000, false),
            (i64::MAX, false),
            (now + 5 * 86_400_000, true),
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
                        &prefrontal_routing_core::route_state_store::ProviderOutcome {
                            account_ref: "provider-alias".into(),
                            credential_id: None,
                            resets_at_ms: None,
                            credential_refusal: None,
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
                        "lastError": "account_limit_exhausted: Go usage limit exceeded",
                        "providerOutcome": {"accountRef": "apikey:cheap-provider",
                            "credentialId": "apikey:cheap-provider", "resetsAtMs": reset},
                    }),
                )
                .await;
            assert_eq!(outcome.0, FrameType::Response, "{}", outcome.1);
            assert_eq!(outcome.1["result"]["ok"], true, "{}", outcome.1);
            let blocked = reset > now && !sibling;
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
            } else {
                assert_eq!(route_of(&pinned), CHEAP);
                assert_eq!(legacy_route(&legacy_pin), CHEAP);
            }
            let fences = harness.store().account_route_fences(now).unwrap();
            if reset > now {
                assert_eq!(fences.len(), 1);
                assert_eq!(fences[0].credential_id, "apikey:cheap-provider");
                assert_eq!(fences[0].model, "model-a");
                assert_eq!(fences[0].until_ms, reset.min(now + 8 * 86_400_000));
                assert!(harness
                    .store()
                    .account_route_fences(fences[0].until_ms)
                    .unwrap()
                    .is_empty());
                assert_eq!(
                    route_of(&route.call("route.select", &select("mason")).await),
                    CHEAP
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
    use prefrontal_routing_core::accounts::AllowedAccountClass;
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
