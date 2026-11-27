use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use cortexkit_role_routing::capabilities::SELECT_WORKLOAD;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::accounts::AllowedAccountClass;
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::{ModelFact, RouteStateStore};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use super::consumers;
use super::effort_preview::{seed_for_test, test_graded};
use super::runners::{declarations_from_catalog, DescribeCache};
use crate::broca_credentials::BrocaCredentials;
use crate::fusiform_catalog::{decode_catalog_get, PlaneCatalog};

const NOW: u64 = 1_800_000_000_000;
const CHATGPT_MODEL: &str = "gpt-6.1-sol";
const METERED_MODEL: &str = "claude-sonnet-4-5";
const CURRENT: Slot = Slot {
    bytes: include_bytes!(
        "../../fixtures/broca-output-cap-harness-1124/synthetic-unsplit-routing-slot.json"
    ),
    size: 214,
    sha256: "62d274540e42a86127119d8bf4edd9f1cfcb7d98e48872fe154974cc8fa5cfe0",
    split: false,
};
const PROPOSED: Slot = Slot {
    bytes: include_bytes!("../../fixtures/broca-output-cap-harness-1124/routing-slot.json"),
    size: 2191,
    sha256: "168755640c483cbd479e915a103a8f335d39f0d4cc8ab4579da1140c6d6b9987",
    split: true,
};
const QUICK: &[u8] =
    include_bytes!("../../fixtures/broca-runner-serves-1126/hello-routing-slot.json");
const BASAL: &[u8] = br#"{"consumer":"broca.complete.basal.run","requirements":{"iq":30,"max_output_tokens":16,"input_tokens":200,"max_cost_usd":0.01},"runners":["broca"]}"#;

struct Slot {
    bytes: &'static [u8],
    size: usize,
    sha256: &'static str,
    split: bool,
}

fn install_slot(
    harness: &Harness,
    Slot {
        bytes,
        size,
        sha256,
        split,
    }: Slot,
) {
    assert_eq!(bytes.len(), size);
    assert_eq!(format!("{:x}", Sha256::digest(bytes)), sha256);
    let slot: Value = serde_json::from_slice(if split { QUICK } else { bytes }).unwrap();
    let declarations = declarations_from_catalog(&json!([{
        "module_id":"broca", "nonce_verified":true, "role_declarations":{"routing/v1":slot}
    }]));
    assert!(declarations.contains_key("broca"));
    harness.credentials().lock().unwrap().runner_declarations = declarations;
    publish_fixture_serves(harness, split, true);
}

fn publish_fixture_serves(harness: &Harness, split: bool, allow_opencode: bool) {
    let mut capped = Vec::new();
    let mut uncapped = Vec::new();
    for fact in harness.store().get_facts().unwrap() {
        if !allow_opencode && matches!(fact.provider_id.as_str(), "opencode" | "opencode-go") {
            continue;
        }
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        if split && fact.provider_id == "openai" {
            uncapped.push(key);
        } else {
            capped.push(key);
        }
    }
    let entries =
        |keys| super::conformance_tests::test_serves(keys, None, "unused")["entries"].clone();
    let mut lists = json!({"broca":entries(&capped)});
    if split {
        lists["broca-uncapped"] = entries(&uncapped);
    }
    let generation = format!("{:x}", Sha256::digest(lists.to_string()));
    harness.publish_runner_serves("broca", &json!({"generation":generation,"harnesses":lists}));
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
    let mut facts = vec![model("openai", CHATGPT_MODEL, "chatgpt", "subscription")];
    if include_metered {
        facts.push(model("anthropic", METERED_MODEL, "apikey", "upstream"));
    }
    setup_with_models("broca-output-cap-harness", &facts)
}

fn setup_with_models(label: &str, facts: &[ModelFact]) -> Arc<Harness> {
    let harness = Harness::new(label);
    // Migrations seed unrelated models; only the fixture routes should be present.
    for fact in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&fact).route_key)
            .unwrap();
    }
    harness.install_facts(facts);
    let models: serde_json::Map<String, Value> = facts
        .iter()
        .map(|fact| {
            let (wire, endpoint) = match fact.provider_id.as_str() {
                "openai" => ("openai.chatgpt_responses", None),
                "anthropic" => ("anthropic.messages", Some("https://api.anthropic.com")),
                "opencode" => (
                    "openai.chat_completions",
                    Some("https://opencode.ai/zen/v1"),
                ),
                "opencode-go" => (
                    "openai.chat_completions",
                    Some("https://opencode.ai/zen/go/v1"),
                ),
                other => panic!("missing test catalog wire family for {other}"),
            };
            let mut entry = json!({
                "existence":"present", "capability.tool_call":true, "wire.family":wire,
                "rate.input":{"state":"priced","currency":"USD","units":4,"exponent":0},
                "rate.output":{"state":"priced","currency":"USD","units":20,"exponent":0}
            });
            if let Some(endpoint) = endpoint {
                entry["wire.api"] = json!({"value":endpoint,"from":"curated"});
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
                    let (id, account_id) =
                        if matches!(fact.provider_id.as_str(), "opencode" | "opencode-go") {
                            (
                                format!("{method}:{}", fact.provider_id),
                                format!("{}-{method}-account", fact.provider_id),
                            )
                        } else {
                            (format!("{method}:test"), format!("{method}-account"))
                        };
                    ListedCredential {
                        id,
                        account_id: Some(account_id),
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
    credentials.refresh_serving_methods(&Default::default(), facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        PlaneCatalog::ordinary_test_catalog(facts)
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
            "prefrontal.sidekick.review":{"owner":"prefrontal","mode":"demand"},
            "prefrontal.mason":{"owner":"prefrontal","mode":"demand"}
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
async fn basal_gets_a_metered_route_only_when_the_split_has_a_capped_route() {
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
        for consumer in ["prefrontal.sidekick.review", "prefrontal.mason"] {
            config["consumers"][consumer]["workload"] = json!("code");
            config["consumers"][consumer]["service_tier"] = json!("standard");
        }
    });
    let route = harness.bind(reserved("prefrontal"), None).await;
    for (consumer, needs) in [
        ("prefrontal.sidekick.review", ["final_text", "plans"]),
        ("prefrontal.mason", ["submit_tool", "plans"]),
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

fn publish_broca_catalog_slot(harness: &Harness, allow_opencode: bool) {
    let slot: Value = serde_json::from_slice(QUICK).unwrap();
    // Decode the daemon's module entry before handing it to both catalog readers.
    let catalog: subc_client_rs::CatalogList = serde_json::from_value(json!({
        "op":"catalog.list", "generation":1, "subc_ops":[], "modules":[{
            "module_id":"broca", "nonce_verified":true, "roles":[], "control_ops":[],
            "role_declarations":{"routing/v1":slot}
        }]
    }))
    .unwrap();
    let modules = serde_json::to_value(catalog.modules).unwrap();
    crate::take_operator_warnings();
    consumers::replace_catalog(&modules);
    DescribeCache::default().catalog(
        &modules,
        &harness.operator_config().v1_runners(),
        &mut harness.credentials().lock().unwrap(),
        NOW as i64,
    );
    let health = consumers::health();
    assert_eq!(health["issues"], json!([]), "{health}");
    let warnings = crate::take_operator_warnings();
    assert!(warnings.is_empty(), "{warnings:?}");
    publish_fixture_serves(harness, true, allow_opencode);
}

#[tokio::test]
async fn broca_completion_slot_declares_quick_for_both_basal_children() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = setup(true);
    let mut efforts = test_graded("high", &["low", "high"]);
    // Only quick picks low instead of the model's graded effort.
    efforts.workloads.entry("quick".into()).or_default().default = Some("low".into());
    seed_for_test(
        harness.store_path(),
        &format!("anthropic/{METERED_MODEL}"),
        efforts,
    );

    let route = harness.bind(reserved("broca"), None).await;
    let (ty, description) = route.call("role.describe", &json!({})).await;
    assert_eq!(ty, FrameType::Response, "{description}");
    assert!(
        description["result"]["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(SELECT_WORKLOAD)),
        "{description}"
    );

    publish_broca_catalog_slot(&harness, false);
    let registry = consumers::effective(&harness.operator_config());
    let mut request: Value = serde_json::from_slice(BASAL).unwrap();
    request["detail"] = json!(true);
    for consumer in ["broca.complete.basal.run", "broca.complete.basal.flow"] {
        assert_eq!(
            registry.defaults_for(consumer).workload.as_deref(),
            Some("quick"),
            "{consumer}"
        );
        request["consumer"] = json!(consumer);
        let result = selected(
            consumer,
            route.call("route.select", &request).await,
            "broca",
        );
        assert_eq!(result["route"]["variant"], "low", "{consumer}: {result}");
        let reasoning = &result["detail"]["reasoning"];
        assert_eq!(
            reasoning["effort_source"], "workload_default",
            "{consumer}: {result}"
        );
        assert_eq!(
            reasoning["workload_source"], "declaration",
            "{consumer}: {result}"
        );
    }
}

#[tokio::test]
async fn broca_completion_cannot_pin_an_unlisted_opencode_route() {
    let _clock = crate::InjectedClock::set(NOW);
    const PROVIDERS: [&str; 2] = ["opencode-go", "opencode"];
    const MODEL: &str = "deepseek-v4-flash";
    let facts = PROVIDERS.map(|provider| model(provider, MODEL, "apikey", "upstream"));
    let harness = setup_with_models("broca-completion-exclude", &facts);
    harness.edit_config(|config| {
        config["consumers"]["broca.complete"]["runners"] = json!([{"module":"broca"}]);
    });
    let route = harness.bind(reserved("broca"), None).await;

    for allow_opencode in [true, false] {
        publish_broca_catalog_slot(&harness, allow_opencode);
        for provider in PROVIDERS {
            let pin = format!("{provider}/{MODEL}");
            let request = json!({
                "consumer":"broca.complete.basal.run",
                "requirements":{"iq":0}, "runners":["broca"], "pin":pin
            });
            let (ty, answer) = route.call("route.select", &request).await;
            if !allow_opencode {
                assert_eq!(ty, FrameType::Error, "{pin}: {answer}");
                assert_eq!(answer["code"], "pin_unavailable", "{pin}: {answer}");
                assert_eq!(
                    answer["detail"]["reason"], "not_available",
                    "{pin}: {answer}"
                );
            } else {
                // The same route and account must be selectable without the exclusion.
                let result = selected(&pin, (ty, answer), "broca");
                assert_eq!(result["route"], json!({"provider":provider,"model":MODEL}));
            }
        }
    }
}

#[tokio::test]
async fn broca_head_choices_uses_opencode_membership_from_the_serves_list() {
    let _clock = crate::InjectedClock::set(NOW);
    const MODEL: &str = "deepseek-v4-flash";
    let facts = [
        model("anthropic", METERED_MODEL, "apikey", "upstream"),
        model("opencode", MODEL, "apikey", "upstream"),
        model("opencode-go", MODEL, "apikey", "upstream"),
    ];
    let harness = setup_with_models("broca-head-choices-exclude", &facts);
    let route = harness.bind(reserved("broca"), None).await;
    let ordinary = ("anthropic", METERED_MODEL);
    let all = [ordinary, ("opencode", MODEL), ("opencode-go", MODEL)];

    for (allow_opencode, expected) in [(true, all.as_slice()), (false, &all[..1])] {
        publish_broca_catalog_slot(&harness, allow_opencode);
        let (ty, answer) = route
            .call("route.head_choices", &json!({"runner_module":"broca"}))
            .await;
        assert_eq!(ty, FrameType::Response, "{answer}");
        let offered: Vec<_> = answer["result"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row["provider"].as_str().unwrap(),
                    row["model"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(offered, expected);
    }
}
