use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::accounts::AllowedAccountClass;
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::{ModelFact, RouteStateStore};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use super::effort_preview::{seed_for_test, test_graded};
use super::runners::declarations_from_catalog;
use crate::broca_credentials::BrocaCredentials;
use crate::fusiform_catalog::{decode_catalog_get, PlaneCatalog};

const NOW: u64 = 1_800_000_000_000;
const CHATGPT_MODEL: &str = "gpt-6.1-sol";
const METERED_MODEL: &str = "claude-sonnet-4-5";
const CURRENT: Slot = Slot {
    bytes: include_bytes!(
        "../../fixtures/broca-output-cap-harness-1124/current-live-routing-slot.json"
    ),
    size: 1277,
    sha256: "6c54624ca04c67c6a6d295849837ccbff28ac0cc9fe840f0298cc51a46a1fa5d",
};
const PROPOSED: Slot = Slot {
    bytes: include_bytes!("../../fixtures/broca-output-cap-harness-1124/routing-slot.json"),
    size: 2191,
    sha256: "168755640c483cbd479e915a103a8f335d39f0d4cc8ab4579da1140c6d6b9987",
};
const BASAL: &[u8] = br#"{"consumer":"broca.complete.basal.run","requirements":{"iq":30,"max_output_tokens":16,"input_tokens":200,"max_cost_usd":0.01},"runners":["broca"]}"#;

struct Slot {
    bytes: &'static [u8],
    size: usize,
    sha256: &'static str,
}

fn install_slot(
    harness: &Harness,
    Slot {
        bytes,
        size,
        sha256,
    }: Slot,
) {
    assert_eq!(bytes.len(), size);
    assert_eq!(format!("{:x}", Sha256::digest(bytes)), sha256);
    let slot: Value = serde_json::from_slice(bytes).unwrap();
    let declarations = declarations_from_catalog(&json!([{
        "module_id":"broca", "nonce_verified":true, "role_declarations":{"routing/v1":slot}
    }]));
    assert!(declarations.contains_key("broca"));
    harness.credentials().lock().unwrap().runner_declarations = declarations;
}

fn model(provider: &str, model: &str, method: &str, kind: &str) -> ModelFact {
    let mut fact = crate::tests::fact(provider, model);
    fact.auth_methods = Some(vec![method.into()]);
    fact.max_output_tokens = Some(32_000);
    fact.billing_plane = Some(BillingPlane {
        provider_id: provider.into(),
        auth_method: method.into(),
        kind: kind.into(),
        selection_reason: "test account".into(),
        rates: [
            ("rate.input".into(), BillingRate::Priced { rate: 4.0 }),
            ("rate.output".into(), BillingRate::Priced { rate: 20.0 }),
        ]
        .into_iter()
        .collect(),
    });
    fact
}

fn setup(include_metered: bool) -> Arc<Harness> {
    let harness = Harness::new("broca-output-cap-harness");
    // Migrations seed unrelated models; only the fixture routes should be present.
    for fact in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&fact).route_key)
            .unwrap();
    }
    let mut facts = vec![model("openai", CHATGPT_MODEL, "chatgpt", "subscription")];
    if include_metered {
        facts.push(model("anthropic", METERED_MODEL, "apikey", "upstream"));
    }
    harness.install_facts(&facts);
    let models: serde_json::Map<String, Value> = facts
        .iter()
        .map(|fact| {
            let wire = if fact.provider_id == "openai" {
                "openai.chatgpt_responses"
            } else {
                "anthropic.messages"
            };
            let mut entry = json!({
                "existence":"present", "capability.tool_call":true, "wire.family":wire,
                "rate.input":{"state":"priced","currency":"USD","units":4,"exponent":0},
                "rate.output":{"state":"priced","currency":"USD","units":20,"exponent":0}
            });
            if fact.provider_id == "anthropic" {
                entry["wire.api"] = json!({"value":"https://api.anthropic.com","from":"curated"});
            }
            (format!("{}/{}", fact.provider_id, fact.model_id), entry)
        })
        .collect();
    let catalog = json!({"catalog_version":1,"resolved_at_ms":NOW,"models":models}).to_string();
    let snapshot = decode_catalog_get(&catalog).unwrap();
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "broca-split-harness-accounts".into(),
            credentials: facts
                .iter()
                .map(|fact| {
                    let method = &fact.auth_methods.as_ref().unwrap()[0];
                    ListedCredential {
                        id: format!("{method}:test"),
                        account_id: Some(format!("{method}-account")),
                        categories: vec!["llm-provider".into()],
                        state: "active".into(),
                        credential_type: if method == "chatgpt" {
                            "oauth"
                        } else {
                            "apikey"
                        }
                        .into(),
                        auth_method: Some(method.clone()),
                        provider_ids: vec![fact.provider_id.clone()],
                        serves: vec![fact.provider_id.clone()],
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        PlaneCatalog::ordinary_test_catalog(&facts)
            .with_test_catalog(catalog.as_bytes())
            .with_wire_apis(&snapshot.wire_apis)
            .with_wire_api_models(&snapshot.wire_api_models),
    );
    credentials.wire_apis = snapshot.wire_apis.clone();
    credentials.wire_api_models = snapshot.wire_api_models.clone();
    credentials.catalog_snapshot = Some(snapshot);
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["openai"]["subscription"] = json!(true);
        config["runners"] = json!([{"module":"broca","via_vault":true}]);
        config["consumers"] = json!({
            "broca.complete":{"owner":"broca"},
            "prefrontal.sidekick.review":{"owner":"prefrontal-core","mode":"demand"},
            "prefrontal-core.mason":{"owner":"prefrontal-core","mode":"demand"}
        });
    });
    harness
}

async fn basal(route: &Route) -> (FrameType, Value) {
    let (ty, bytes) = route.call_raw_params("route.select", BASAL).await;
    (ty, serde_json::from_slice(&bytes).unwrap())
}

fn assert_refusal(label: &str, (ty, reply): (FrameType, Value), reason: &str) {
    assert_eq!(ty, FrameType::Error, "{label}: {reply}");
    assert_eq!(reply["code"], "no_route", "{label}: {reply}");
    assert_eq!(reply["detail"]["reason"], reason, "{label}: {reply}");
    println!("{label}: {reply}");
}

fn selected(label: &str, (ty, reply): (FrameType, Value), harness: &str) -> Value {
    assert_eq!(ty, FrameType::Response, "{label}: {reply}");
    let result = &reply["result"];
    assert_eq!(result["runner"]["module"], "broca", "{label}: {result}");
    assert_eq!(result["runner"]["harness"], harness, "{label}: {result}");
    result.clone()
}

#[tokio::test]
async fn basal_gets_a_metered_route_only_when_the_split_has_a_capped_pair() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = setup(true);
    let route = harness.bind(reserved("broca"), None).await;
    install_slot(&harness, CURRENT);
    assert_refusal(
        "Basal, current, mixed",
        basal(&route).await,
        "harness_unavailable",
    );

    install_slot(&harness, PROPOSED);
    let answer = selected("Basal, proposed, mixed", basal(&route).await, "broca");
    assert_eq!(answer["route"]["provider"], "anthropic", "{answer}");
    assert_eq!(answer["route"]["model"], METERED_MODEL, "{answer}");
    assert_eq!(answer["price"]["basis"], "metered", "{answer}");
    println!("Basal, proposed, mixed: {answer}");

    let only_chatgpt = setup(false);
    install_slot(&only_chatgpt, PROPOSED);
    let route = only_chatgpt.bind(reserved("broca"), None).await;
    assert_refusal(
        "Basal, proposed, ChatGPT only",
        basal(&route).await,
        "missing_capability",
    );
    let uncapped =
        json!({"consumer":"broca.complete.basal.run","requirements":{"iq":30},"runners":["broca"]});
    let answer = selected(
        "Basal without caps, proposed, ChatGPT only",
        route.call("route.select", &uncapped).await,
        "broca-uncapped",
    );
    assert_eq!(
        answer["route"],
        json!({"provider":"openai","model":CHATGPT_MODEL}),
        "{answer}"
    );
    println!("Basal without caps, proposed, ChatGPT only: {answer}");
}

#[tokio::test]
async fn split_names_the_uncapped_harness_without_changing_sidekick_or_mason_routes() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = setup(true);
    let mut efforts = test_graded("low", &["low", "high"]);
    efforts.workloads.entry("code".into()).or_default().default = Some("high".into());
    seed_for_test(
        harness.store_path(),
        &format!("openai/{CHATGPT_MODEL}"),
        efforts,
    );
    harness.edit_config(|config| {
        for consumer in ["prefrontal.sidekick.review", "prefrontal-core.mason"] {
            config["consumers"][consumer]["workload"] = json!("code");
            config["consumers"][consumer]["service_tier"] = json!("standard");
        }
    });
    let route = harness.bind(reserved("prefrontal-core"), None).await;
    for (consumer, needs) in [
        ("prefrontal.sidekick.review", ["final_text", "plans"]),
        ("prefrontal-core.mason", ["submit_tool", "plans"]),
    ] {
        let request =
            json!({"consumer":consumer,"requirements":{"iq":30,"harness_capabilities":needs}});
        let mut before = None;
        for (slot, name) in [(CURRENT, "broca"), (PROPOSED, "broca-uncapped")] {
            install_slot(&harness, slot);
            let result = selected(consumer, route.call("route.select", &request).await, name);
            assert_eq!(
                result["route"]["provider"], "openai",
                "{consumer}: {result}"
            );
            assert_eq!(
                result["route"]["model"], CHATGPT_MODEL,
                "{consumer}: {result}"
            );
            assert_eq!(result["route"]["variant"], "high", "{consumer}: {result}");
            assert_eq!(
                result["service_tier_chosen"], "standard",
                "{consumer}: {result}"
            );
            let id = result["decision_id"].as_str().unwrap();
            let class = harness.store().decision_account_class(id).unwrap().unwrap();
            assert_eq!(
                class,
                AllowedAccountClass::NotPayPerToken,
                "{consumer}: {result}"
            );
            println!("{consumer}, {name}: {result}, account class: {class:?}");
            let identity = (
                result["route"].clone(),
                result.get("service_tier_chosen").cloned(),
                class,
            );
            if let Some(before) = before.as_ref() {
                assert_eq!(&identity, before, "{consumer}: {result}");
            } else {
                before = Some(identity);
            }
        }
    }
}
