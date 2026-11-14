//! Classifier routing through the ordinary role handler and account allocator.

use std::sync::Arc;

use cortexkit_role_classifier::describe::{check_describe, RoleDescribe};
use cortexkit_role_routing_conformance::CatalogModel;
use striatum_core::ModelFact;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness, Route};
use crate::broca_credentials::BrocaCredentials;

const JEV: &str = "typesafe/jev-1.13.0";
const CLEF: &str = "cloudflare-workers-ai/@cf/cloudflare/clef";
const FLASH: &str = "cloudflare-workers-ai/@cf/cloudflare/clef-flash";
const LIVE: &[u8] = include_bytes!("../../tests/fixtures/classifier-describe.reply.json");

fn describe() -> Value {
    serde_json::from_slice(LIVE).unwrap()
}

fn catalog() -> Value {
    let mut models = serde_json::Map::new();
    for entry in describe()["result"]["models"].as_array().unwrap() {
        let rate = entry["price"]["input_per_mtok_usd"].as_f64().unwrap();
        let mut row = json!({
            "existence":"present", "model.kind":"decision",
            "capability.input_modalities": if entry["images"].is_null() { vec!["text"] } else { vec!["text", "image"] },
            "rate.input":{"state":"priced","currency":"USD","units":(rate * 1e9).round() as i64,"exponent":9},
            "rate.output":{"state":"stated_zero"}
        });
        if let Some(context) = entry.get("context_tokens") {
            row["limit.context"] = context.clone();
        }
        models.insert(entry["model"].as_str().unwrap().into(), row);
    }
    json!({"source":"models.dev", "catalog_version":1, "resolved_at_ms":1, "models":models})
}

pub(super) fn rig(
    edit_catalog: impl FnOnce(&mut Value),
    edit_describe: impl FnOnce(&mut Value),
) -> Arc<Harness> {
    let harness = Harness::new("classifier");
    let mut catalog = catalog();
    edit_catalog(&mut catalog);
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap())
            .unwrap();
    let facts: Vec<ModelFact> = snapshot
        .parts()
        .0
        .iter()
        .enumerate()
        .map(|(rank, update)| {
            let mut fact = catalog_fact(
                &CatalogModel {
                    provider: update.provider_id.clone(),
                    model: update.model_id.clone(),
                    family: None,
                    iq: 80,
                    context_window: 200_000,
                    max_output_tokens: 32_000,
                    tool_calls: false,
                    modalities: Vec::new(),
                    structured_output: false,
                    priced_per_token: true,
                },
                rank,
            );
            fact.context_window = update.context_window;
            fact.input_rate = update.input_rate;
            fact.cost_per_m_tok_out = update.cost_per_mtok_out;
            fact.supports_vision = update.supports_vision;
            fact.supports_tool_use = Some(false);
            fact
        })
        .collect();
    harness.install_facts(&facts);
    harness
        .store()
        .apply_fusiform_catalog(snapshot.parts().0, snapshot.parts().1)
        .unwrap();
    let mut credentials = BrocaCredentials::api_test_credentials(&facts);
    credentials.publish_plan_prices(&json!({"prices":[]}));
    credentials.catalog_snapshot = Some(Arc::new(snapshot));
    let mut description = describe();
    edit_describe(&mut description);
    let mut cache = super::classifier::DescribeCache::default();
    cache.catalog(&json!([{"module_id":"broca", "nonce_verified":true,"capabilities":{"provides":["classifier/v1"]}}]), &mut credentials, crate::now_ms() as i64);
    cache.reply(
        "broca",
        Some(&description),
        &mut credentials,
        crate::now_ms() as i64,
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"]["classifier"] = json!({"owner":"broca"});
        config["runners"] = json!([{"module":"chat-runner"}]);
    });
    harness
}

fn request(requirements: Value) -> Value {
    json!({"consumer":"classifier","requirements":requirements})
}
fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}
async fn selected(route: &Route, requirements: Value) -> Value {
    result(route.call("route.select", &request(requirements)).await)
}
fn route_key(answer: &Value) -> String {
    format!(
        "{}/{}",
        answer["route"]["provider"].as_str().unwrap(),
        answer["route"]["model"].as_str().unwrap()
    )
}
async fn refused(route: &Route, requirements: Value) {
    let reply = route.call("route.select", &request(requirements)).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
}

#[test]
fn live_describe_bytes_are_checksum_pinned_and_contract_valid() {
    assert_eq!(
        super::classifier::describe_options().role_versions,
        Some(std::collections::BTreeMap::from([(
            "classifier".into(),
            "v1".into()
        )]))
    );
    assert_eq!(
        serde_json::from_slice::<Value>(super::classifier::DESCRIBE_REQUEST).unwrap(),
        json!({"method":"role.describe","params":{}})
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(LIVE)),
        "7e8ec45b7b431d2f04d5fdc6aa74120caf8737fcfbab242783e44ce724886f2f"
    );
    let describe: RoleDescribe = serde_json::from_value(describe()["result"].clone()).unwrap();
    check_describe(&describe).unwrap();
    assert_eq!(describe.models.len(), 3);
    assert_eq!(describe.model(JEV).unwrap().context_tokens, None);
    assert_eq!(describe.model(JEV).unwrap().images, None);
    assert_eq!(describe.model(CLEF).unwrap().images.unwrap().max, 4);
    assert_eq!(
        describe
            .model(FLASH)
            .unwrap()
            .price
            .as_ref()
            .unwrap()
            .output_per_mtok_usd
            .as_ref()
            .unwrap()
            .as_u64(),
        Some(0)
    );
    assert_eq!(
        super::classifier::models_from_describe(&json!({"result":{"role":"llm-runner/v1"}}))
            .is_none(),
        true
    );
}

#[tokio::test]
async fn native_listing_routes_without_chat_wire_families_and_input_price_ignores_iq() {
    let harness = rig(|_| {}, |_| {});
    let route = harness.bind(reserved("broca"), None).await;
    let answer = selected(&route, json!({"iq":100,"questions":64})).await;
    assert_eq!(route_key(&answer), FLASH);
    assert_eq!(answer["runner"]["module"], "broca");
    let describe = result(route.call("role.describe", &json!({})).await);
    for field in ["questions", "input_tokens", "max_cost_usd"] {
        assert!(describe["requirement_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == field));
    }
}

#[tokio::test]
async fn roster_precedes_price_and_silent_or_ineligible_entries_fall_back_to_price() {
    let harness = rig(|_| {}, |_| {});
    let route = harness.bind(reserved("broca"), None).await;
    harness.edit_config(|config| config["consumers"]["classifier"]["ladder"] = json!([JEV]));
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), JEV);
    harness.edit_config(|config| config["consumers"]["classifier"]["mode"] = json!("ladder"));
    assert_eq!(
        route_key(&selected(&route, json!({"iq":0,"modalities":["image"]})).await),
        FLASH
    );
    harness.edit_config(|config| {
        config["consumers"]["classifier"]["ladder"] = json!(["missing/model"])
    });
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), FLASH);
}

#[tokio::test]
async fn equal_input_prices_have_a_deterministic_route_key_tie_break() {
    let harness = rig(
        |catalog| {
            let rate = catalog["models"][FLASH]["rate.input"].clone();
            for model in [JEV, CLEF] {
                catalog["models"][model]["rate.input"] = rate.clone();
            }
        },
        |_| {},
    );
    let route = harness.bind(reserved("broca"), None).await;
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), CLEF);
}

#[tokio::test]
async fn question_limits_images_and_stated_context_limits_filter_models() {
    let harness = rig(
        |_| {},
        |description| description["result"]["models"][0]["max_questions"] = json!(400),
    );
    let route = harness.bind(reserved("broca"), None).await;
    assert_eq!(
        route_key(&selected(&route, json!({"iq":0,"questions":65})).await),
        JEV
    );
    refused(&route, json!({"iq":0,"questions":401})).await;
    assert_eq!(
        route_key(&selected(&route, json!({"iq":0,"context_window":100_000})).await),
        JEV
    );
    assert_eq!(
        route_key(
            &selected(
                &route,
                json!({"iq":0,"context_window":30_000,"modalities":["image"]})
            )
            .await
        ),
        CLEF
    );
    refused(
        &route,
        json!({"iq":0,"context_window":100_000,"modalities":["image"]}),
    )
    .await;
    refused(
        &route,
        json!({"iq":0,"questions":65,"modalities":["image"]}),
    )
    .await;
}

#[tokio::test]
async fn caps_use_total_input_tokens_catalog_price_and_stated_zero() {
    let harness = rig(|_| {}, |_| {});
    let route = harness.bind(reserved("broca"), None).await;
    assert_eq!(
        route_key(
            &selected(
                &route,
                json!({"iq":0,"input_tokens":1000,"max_cost_usd":0.000038})
            )
            .await
        ),
        FLASH
    );
    refused(
        &route,
        json!({"iq":0,"input_tokens":2000,"max_cost_usd":0.000038}),
    )
    .await;
    let zero = rig(
        |catalog| catalog["models"][JEV]["rate.input"] = json!({"state":"stated_zero"}),
        |_| {},
    );
    let route = zero.bind(reserved("broca"), None).await;
    assert_eq!(
        route_key(
            &selected(
                &route,
                json!({"iq":0,"input_tokens":1000,"max_cost_usd":0.000001})
            )
            .await
        ),
        JEV
    );
}

#[tokio::test]
async fn unstated_prices_are_ineligible_under_a_cap_but_allowed_without_one() {
    let harness = rig(
        |catalog| {
            for model in [JEV, CLEF, FLASH] {
                catalog["models"][model]["rate.input"] = json!({"state":"unpriced"});
            }
        },
        |_| {},
    );
    let route = harness.bind(reserved("broca"), None).await;
    refused(&route, json!({"iq":0,"input_tokens":1000,"max_cost_usd":1})).await;
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), CLEF);
}

#[tokio::test]
async fn extraction_and_missing_catalog_rows_are_never_chosen() {
    for kind in ["extraction", "chat"] {
        let harness = rig(
            |catalog| catalog["models"][FLASH]["model.kind"] = json!(kind),
            |_| {},
        );
        let route = harness.bind(reserved("broca"), None).await;
        assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), JEV);
    }
    let harness = rig(
        |catalog| {
            catalog["models"].as_object_mut().unwrap().remove(FLASH);
        },
        |_| {},
    );
    let route = harness.bind(reserved("broca"), None).await;
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), JEV);
}

#[tokio::test]
async fn missing_question_limit_fails_closed_and_another_modules_listing_is_ignored() {
    let harness = rig(
        |_| {},
        |description| {
            description["result"]["models"][0]
                .as_object_mut()
                .unwrap()
                .remove("max_questions");
        },
    );
    let route = harness.bind(reserved("broca"), None).await;
    refused(&route, json!({"iq":0})).await;
    let harness = rig(|_| {}, |_| {});
    harness
        .edit_config(|config| config["consumers"]["classifier"]["owner"] = json!("another-module"));
    let route = harness.bind(reserved("another-module"), None).await;
    refused(&route, json!({"iq":0})).await;
    let route = harness.bind(reserved("example-consumer"), None).await;
    let reply = route.call("route.select",
        &json!({"consumer":"mason","requirements":{"iq":50}})).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route");
    assert!(reply.1.to_string().contains("not_chat_kind:decision"), "{}", reply.1);
}

#[tokio::test]
async fn classifier_pins_choose_only_an_account_and_reports_and_observations_use_existing_ledgers()
{
    let harness = rig(|_| {}, |_| {});
    let route = harness.bind(reserved("broca"), None).await;
    let answer = selected(&route, json!({"iq":0})).await;
    assert_eq!(route_key(&answer), FLASH);
    let admission = json!({
        "provider":"cloudflare-workers-ai", "model":"@cf/cloudflare/clef-flash",
        "conversation_key":"classifier-batch", "admission_id":"classifier-admission",
        "decision_id":answer["decision_id"]
    });
    let account = result(route.call("route.account_for", &admission).await);
    assert!(account["account"]["credential_id"]
        .as_str()
        .unwrap()
        .contains("cloudflare-workers-ai"));
    let report = json!({"decision_id":answer["decision_id"], "outcome":"completed",
        "usage":{"run_id":"batch","input_tokens":1000,"output_tokens":0,"cost_usd":0.000038}});
    result(route.call("route.report", &report).await);
    result(route.call("route.report", &report).await);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let totals: (i64, i64) = conn
        .query_row(
            "SELECT count(*),sum(input_tokens) FROM model_route_usage",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(totals, (1, 1000));
    let observation = json!({"event_id":"classifier-key-refused", "kind":"credential_refused",
        "admission_id":"classifier-admission", "provider_code":"billing_exhausted"});
    result(route.call("route.account_observe", &observation).await);
    result(route.call("route.account_observe", &json!({"event_id":"classifier-finished", "kind":"run_finished", "admission_id":"classifier-admission"})).await);
    let pin = json!({"provider":"cloudflare-workers-ai", "model":"@cf/cloudflare/clef-flash",
        "conversation_key":"pinned-batch", "admission_id":"pinned-admission"});
    let refused = route.call("route.account_for", &pin).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert!(
        matches!(
            refused.1["code"].as_str(),
            Some("account_pool_exhausted" | "account_unavailable" | "account_walled")
        ),
        "{}",
        refused.1
    );
    assert_eq!(route_key(&selected(&route, json!({"iq":0})).await), JEV);
    let mut jev = pin;
    jev["provider"] = json!("typesafe");
    jev["model"] = json!("jev-1.13.0");
    jev["admission_id"] = json!("jev-pin");
    let account = result(route.call("route.account_for", &jev).await);
    assert!(account["account"]["credential_id"]
        .as_str()
        .unwrap()
        .contains("typesafe"));
}

#[test]
fn classifier_cache_expires_and_loses_nonce_verified_discovery() {
    let mut credentials = BrocaCredentials::default();
    let mut cache = super::classifier::DescribeCache::default();
    let modules = json!([{"module_id":"broca","nonce_verified":true,"capabilities":{"provides":["classifier/v1"]}}]);
    cache.catalog(&modules, &mut credentials, 1);
    cache.reply("broca", Some(&describe()), &mut credentials, 1);
    assert_eq!(credentials.classifier_models["broca"].len(), 3);
    cache.reply("broca", None, &mut credentials, 1000);
    assert!(credentials.classifier_models.contains_key("broca"));
    cache.expire(&mut credentials, 300_001);
    assert!(!credentials.classifier_models.contains_key("broca"));
    cache.reply("broca", Some(&describe()), &mut credentials, 300_002);
    cache.reply(
        "broca",
        Some(&json!({"result":{"role":"llm-runner/v1"}})),
        &mut credentials,
        300_003,
    );
    assert!(credentials.classifier_models.is_empty());
    cache.reply("broca", Some(&describe()), &mut credentials, 300_004);
    cache.catalog(&json!([]), &mut credentials, 300_003);
    assert!(credentials.classifier_models.is_empty());
}

#[test]
fn selection_requests_and_refusals_match_published_routing_vectors() {
    use cortexkit_role_routing::select::parse_select_request;
    let vectors: Value = serde_json::from_slice(include_bytes!(
        "../../tests/fixtures/routing-select-vectors.json"
    ))
    .unwrap();
    for vector in vectors["requests"].as_array().unwrap() {
        assert!(parse_select_request(&vector["params"]).is_ok(), "{vector}");
    }
    for vector in vectors["refused"].as_array().unwrap() {
        let refusal = parse_select_request(&vector["params"]).unwrap_err();
        assert_eq!(refusal.code.as_str(), vector["refusal"]["code"]);
        for (field, value) in vector["refusal"]["detail"].as_object().unwrap() {
            assert_eq!(&refusal.detail[field], value);
        }
    }
    for input in [
        json!({"iq":0,"questions":0}),
        json!({"iq":0,"max_cost_usd":1}),
        json!({"iq":0,"input_tokens":0,"max_cost_usd":1}),
    ] {
        let refusal = parse_select_request(&request(input)).unwrap_err();
        assert_eq!(refusal.code.as_str(), "invalid_requirements");
    }
}
