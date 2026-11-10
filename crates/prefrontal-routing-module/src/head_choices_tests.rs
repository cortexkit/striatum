use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use prefrontal_routing_core::{make_route_key, ModelPromptDefaults, RouteStateStore};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use crate::{broca_credentials::BrocaCredentials, fusiform_catalog, tests::fact};

const RUNNER: &str = "thalamus-harness";
const LEGACY: &str = "legacy-runner";
const NO_TIERS: &str = "no-tiers-runner";
const API_KEY_RUNNER: &str = "api-key-runner";
const WIRE: &str = "openai.responses";

fn fixture() -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::new("head-choices");
    for fact in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&make_route_key(
                &fact.provider_id,
                &fact.model_id,
                fact.variant.as_deref(),
            ))
            .unwrap();
    }
    let mut facts: Vec<_> = [
        "a-head",
        "pinned",
        "blocked",
        "only-mason",
        "no-tools",
        "deprecated",
        "unscored",
        "unlisted",
    ]
    .into_iter()
    .map(|model| fact("openai", model))
    .chain([
        fact("anthropic", "claude"),
        fact("unmapped", "absent-account"),
    ])
    .collect();
    facts[0].prompt_defaults = Some(ModelPromptDefaults {
        variant: Some("medium".into()),
        ..Default::default()
    });
    facts
        .iter_mut()
        .find(|fact| fact.model_id == "unscored")
        .unwrap()
        .score_provenance = prefrontal_routing_core::ScoreProvenance::Unscored;
    harness.install_facts(&facts);
    harness
        .store()
        .upsert_fact(
            facts
                .iter()
                .find(|fact| fact.model_id == "unscored")
                .unwrap(),
            prefrontal_routing_core::FactSource::Unscored,
        )
        .unwrap();
    let models: serde_json::Map<_, _> = facts.iter().map(|fact| {
        (format!("{}/{}", fact.provider_id, fact.model_id), json!({
            "existence": "present", "capability.tool_call": fact.model_id != "no-tools",
            "capability.reasoning_options": [{"type":"effort", "values":["low","medium","high","xhigh"]}],
            "model.status": if fact.model_id == "deprecated" { json!("deprecated") } else { Value::Null },
            "rate.output.mode.fast": {"state":"priced", "currency":"USD", "units":10, "exponent":0},
            "rate.output.mode.ultrafast": {"state":"priced", "currency":"USD", "units":20, "exponent":0},
        }))
    }).collect();
    let catalog = json!({"catalog_version":41,"resolved_at_ms":123,"models":models});
    let snapshot =
        fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap()).unwrap();
    let (updates, cursor) = snapshot.parts();
    harness
        .store()
        .apply_fusiform_catalog(updates, cursor)
        .unwrap();
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "head-choices-accounts".into(),
            credentials: ["openai", "anthropic"]
                .into_iter()
                .map(|provider| ListedCredential {
                    id: format!("{provider}-account"),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec![provider.into()],
                    serves: vec![provider.into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    plan_tier: Some("pro".into()),
                    ..Default::default()
                })
                .chain([ListedCredential {
                    id: "openai-key".into(),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["openai".into()],
                    serves: vec!["openai".into()],
                    credential_type: "apikey".into(),
                    auth_method: Some("apikey".into()),
                    ..Default::default()
                }])
                .collect(),
            ..Default::default()
        },
        crate::now_ms(),
    );
    credentials.catalog_snapshot = Some(Arc::new(snapshot));
    credentials.wire_apis = facts
        .iter()
        .map(|fact| {
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                if fact.provider_id == "openai" {
                    WIRE
                } else {
                    "anthropic.messages"
                }
                .into(),
            )
        })
        .collect();
    credentials.publish_billing_catalog(
        fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
            .with_test_catalog(&serde_json::to_vec(&catalog).unwrap())
            .with_wire_apis(&credentials.wire_apis),
    );
    credentials.publish_plan_prices(&json!({"prices":[{
        "provider_id":"openai", "tier":"pro", "service_tiers":{
            "state":"established", "tiers":[
                {"tier":"fast", "included_usage_rate":{"units":25,"exponent":1}, "credit_rate":{"units":2,"exponent":0}},
                {"tier":"ultrafast", "included_usage_rate":{"units":8,"exponent":0}, "credit_rate":{"units":6,"exponent":0}}
            ]
        }
    }]}));
    for runner in [RUNNER, NO_TIERS, "policy-runner", "pin-runner"] {
        credentials.runner_declarations.insert(runner.into(), super::runners::parse_declaration(
            &json!({"runner":{"pairs":[{"wire_api":WIRE,"auth_method":"oauth"}],"follows_account_choice":true}})
        ).unwrap().unwrap());
    }
    credentials.runner_declarations.insert(API_KEY_RUNNER.into(), super::runners::parse_declaration(
        &json!({"runner":{"pairs":[{"wire_api":WIRE,"auth_method":"apikey"}],"follows_account_choice":true}})
    ).unwrap().unwrap());
    credentials.runner_service_tiers.insert(RUNNER.into(), super::runners::service_tiers_from_describe(
        &json!({"capabilities":["service_tier"],"service_tiers":{WIRE:{"levels":["fast"],"served":"reported"}}})
    ).unwrap());
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":RUNNER,"via_vault":true}, {"module":NO_TIERS,"via_vault":true},
            {"module":LEGACY,"via_vault":true},
            {"module":"policy-runner","routes":["openai/a-head","openai/blocked"]},
            {"module":"pin-runner","routes":["openai/pinned"]},
            {"module":API_KEY_RUNNER,"via_vault":true,"routes":["openai/pinned"]}
        ]);
        config["providers"] = json!({"openai":{"models":["a-head","pinned","blocked","only-mason","no-tools","deprecated","unscored"]}});
        config["models"] = json!({"openai/pinned":{"pin_only":true},
            "openai/blocked":{"not_for":["head"]}, "openai/only-mason":{"only_for":["mason"]}});
        config["variants"] = json!({"openai/a-head":"low"});
        config["consumers"]["head"] = json!({"variants":{"openai/a-head":"high"}});
    });
    harness.set_credentials(credentials.clone());
    (harness, credentials)
}

async fn query(harness: &Arc<Harness>, runner: &str) -> Value {
    let route = harness.bind(reserved("occipital"), None).await;
    let (ty, reply) = route
        .call("route.head_choices", &json!({"runner_module":runner}))
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    check_contract(runner, &reply["result"]);
    reply["result"].clone()
}

fn check_contract(runner: &str, reply: &Value) {
    use cortexkit_role_routing::head_choices::{
        check_head_choices, HeadChoicesAnswer, HeadChoicesRequest,
    };
    let answer: HeadChoicesAnswer = serde_json::from_value(reply.clone()).unwrap();
    check_head_choices(&HeadChoicesRequest::new(runner), &answer).unwrap();
}

fn ids(reply: &Value) -> Vec<String> {
    reply["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            format!(
                "{}/{}",
                row["provider"].as_str().unwrap(),
                row["model"].as_str().unwrap()
            )
        })
        .collect()
}

fn declare_harness_limits(credentials: &mut BrocaCredentials, limits: Value) {
    credentials.runner_declarations.insert(
        RUNNER.into(),
        super::runners::parse_declaration(&json!({"runner": {
            "pairs":[{"wire_api":WIRE,"auth_method":"oauth"},
                {"wire_api":"anthropic.messages","auth_method":"oauth"}],
            "follows_account_choice":true,
            "harnesses":["claude-code","codex"],
            "default_harness":"claude-code",
            "harness_limits":limits
        }}))
        .unwrap()
        .unwrap(),
    );
}

#[tokio::test]
async fn harness_choices_narrow_pairs_and_absent_harness_keeps_runner_union() {
    let (harness, mut credentials) = fixture();
    declare_harness_limits(
        &mut credentials,
        json!({"claude-code": {
            "pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]
        }}),
    );
    harness.set_credentials(credentials);
    let route = harness.bind(reserved("occipital"), None).await;
    let (ty, reply) = route
        .call(
            "route.head_choices",
            &json!({
                "runner_module":RUNNER,"harness":"claude-code"
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    assert_eq!(ids(&reply["result"]), vec!["anthropic/claude"]);
    let union = query(&harness, RUNNER).await;
    assert!(ids(&union).contains(&"openai/a-head".into()), "{union}");
    assert!(ids(&union).contains(&"anthropic/claude".into()), "{union}");
    let (ty, reply) = route
        .call(
            "route.head_choices",
            &json!({
                "runner_module":RUNNER,"harness":"unhosted"
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "no_route", "{reply}");
    assert_eq!(reply["detail"]["reason"], "harness_unavailable", "{reply}");
    for malformed in [json!(42), json!("not a token"), json!("Claude-Code")] {
        let (ty, reply) = route
            .call(
                "route.head_choices",
                &json!({
                    "runner_module":RUNNER,"harness":malformed
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{reply}");
        assert_eq!(reply["code"], "invalid_request", "{reply}");
    }
}

#[tokio::test]
async fn harness_choices_narrow_providers_and_malformed_map_falls_back_for_every_harness() {
    let (harness, mut credentials) = fixture();
    declare_harness_limits(
        &mut credentials,
        json!({"claude-code": {
            "explicit_providers":["anthropic"]
        }}),
    );
    let narrowed = crate::head_choices::choices_for_harness(
        harness.store(),
        &harness.operator_config(),
        &credentials,
        RUNNER,
        Some("claude-code"),
    )
    .unwrap();
    assert_eq!(ids(&narrowed), vec!["anthropic/claude"]);
    declare_harness_limits(
        &mut credentials,
        json!({
            "claude-code":{"explicit_providers":["anthropic"]},
            "codex":{"pairs":[]}
        }),
    );
    crate::take_operator_warnings();
    for hosted in ["claude-code", "codex"] {
        let fallback = crate::head_choices::choices_for_harness(
            harness.store(),
            &harness.operator_config(),
            &credentials,
            RUNNER,
            Some(hosted),
        )
        .unwrap();
        assert!(
            ids(&fallback).contains(&"openai/a-head".into()),
            "{fallback}"
        );
        assert!(
            ids(&fallback).contains(&"anthropic/claude".into()),
            "{fallback}"
        );
    }
    let warnings = crate::take_operator_warnings();
    assert!(
        warnings.iter().any(|warning| warning
            .contains("runner.harness_limits.codex.pairs is empty")
            && warning.contains("using runner-level limits")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn reachability_uses_declared_pairs_and_vault_fallback() {
    let (harness, _) = fixture();
    assert_eq!(
        ids(&query(&harness, RUNNER).await),
        ["openai/a-head", "openai/pinned"]
    );
    assert_eq!(
        ids(&query(&harness, LEGACY).await),
        ["anthropic/claude", "openai/a-head", "openai/pinned"]
    );
}

#[tokio::test]
async fn unknown_and_non_starting_runners_have_no_choices() {
    let (harness, mut credentials) = fixture();
    credentials
        .runner_declarations
        .get_mut(RUNNER)
        .unwrap()
        .starts_sessions = false;
    harness.set_credentials(credentials);
    for runner in [RUNNER, "missing", "two words"] {
        let reply = query(&harness, runner).await;
        assert_eq!(reply["models"], json!([]), "{reply}");
    }
}

#[tokio::test]
async fn policy_and_standing_pin_refusals_filter_but_pin_only_is_listed() {
    let (harness, _) = fixture();
    let reply = query(&harness, RUNNER).await;
    assert_eq!(ids(&reply), ["openai/a-head", "openai/pinned"]);
    assert_eq!(reply["models"][0]["default_variant"], "high");
    assert_eq!(
        reply["models"][0]["variants"],
        json!(["high", "low", "medium", "xhigh"])
    );
    assert!(reply["models"][1].get("default_variant").is_none());
}

#[tokio::test]
async fn pin_only_distinguishes_pins_from_automatic_head_routes() {
    let (harness, _) = fixture();
    let reply = query(&harness, RUNNER).await;
    assert_eq!(ids(&reply), ["openai/a-head", "openai/pinned"]);
    assert_eq!(reply["models"][0]["pin_only"], false);
    assert_eq!(reply["models"][1]["pin_only"], true);
}

#[tokio::test]
async fn pin_only_uses_automatic_variants_reachable_on_the_runner() {
    let (harness, _) = fixture();
    harness.edit_config(|config| {
        config["models"]["openai/a-head"] = json!({"pin_only_for":["head"]});
        config["models"]["openai/a-head/high"] = json!({"pin_only_for":[]});
        config["runners"][0]["routes"] = json!(["openai/a-head/low"]);
    });
    let reply = query(&harness, RUNNER).await;
    assert_eq!(ids(&reply), ["openai/a-head"]);
    assert_eq!(reply["models"][0]["pin_only"], true);
    harness.edit_config(|config| {
        config["runners"][0]["routes"] = json!(["openai/a-head/low", "openai/a-head/high"]);
    });
    assert_eq!(
        query(&harness, RUNNER).await["models"][0]["pin_only"],
        false
    );
}

#[tokio::test]
async fn billing_distinguishes_reachable_subscriptions_from_per_token_keys() {
    let (harness, _) = fixture();
    let subscription = query(&harness, RUNNER).await;
    assert_eq!(ids(&subscription), ["openai/a-head", "openai/pinned"]);
    assert_eq!(subscription["models"][0]["billing"], "subscription");
    assert_eq!(subscription["models"][1]["billing"], "subscription");

    let metered = query(&harness, API_KEY_RUNNER).await;
    assert_eq!(ids(&metered), ["openai/pinned"]);
    assert_eq!(metered["models"][0]["billing"], "per_token");
    assert_eq!(metered["models"][0]["pin_only"], true);

    let mixed = query(&harness, LEGACY).await;
    assert_eq!(
        ids(&mixed),
        ["anthropic/claude", "openai/a-head", "openai/pinned"]
    );
    assert!(mixed["models"]
        .as_array()
        .unwrap()
        .iter()
        .all(|model| model["billing"] == "subscription"));
}

#[tokio::test]
async fn billing_uses_account_selection_subscription_settings() {
    let (harness, mut credentials) = fixture();
    let mut rows = credentials.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|row| row.id == "openai-key")
        .unwrap()
        .plan_tier = Some("go".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "head-choices-named-plan".into(),
            credentials: rows,
            ..Default::default()
        },
        crate::now_ms(),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["openai"]["subscription"] = json!(true);
    });
    assert_eq!(
        query(&harness, API_KEY_RUNNER).await["models"][0]["billing"],
        "subscription"
    );
    harness.edit_config(|config| {
        config["models"]["openai/pinned"]["subscription"] = json!(false);
    });
    assert_eq!(
        query(&harness, API_KEY_RUNNER).await["models"][0]["billing"],
        "per_token"
    );
    harness.edit_config(|config| {
        config["providers"]["openai"]["subscription"] = json!(false);
        config["models"]["openai/pinned"]["subscription"] = json!(true);
    });
    assert_eq!(
        query(&harness, API_KEY_RUNNER).await["models"][0]["billing"],
        "subscription"
    );
}

#[tokio::test]
async fn tiers_require_model_plan_and_runner_wire_support() {
    let (harness, mut credentials) = fixture();
    assert_eq!(
        query(&harness, RUNNER).await["models"][0]["service_tiers"],
        json!(["fast", "standard"])
    );
    assert_eq!(
        query(&harness, NO_TIERS).await["models"][0]["service_tiers"],
        json!(["standard"])
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials.clone());
    assert_eq!(
        query(&harness, RUNNER).await["models"][0]["service_tiers"],
        json!(["standard"])
    );
    let (other, mut credentials) = fixture();
    credentials.publish_billing_catalog(
        fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&other.store().get_facts().unwrap())
            .with_wire_apis(&credentials.wire_apis),
    );
    other.set_credentials(credentials);
    assert_eq!(
        query(&other, RUNNER).await["models"][0]["service_tiers"],
        json!(["standard"])
    );
}

#[tokio::test]
async fn read_writes_no_decisions_leases_reservations_or_other_rows() {
    let (harness, _) = fixture();
    let observer = rusqlite::Connection::open(harness.store_path()).unwrap();
    let version = || {
        observer
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap()
    };
    let counts = || {
        let tables: Vec<String> = observer
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        tables
            .into_iter()
            .map(|table| {
                let count: i64 = observer
                    .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                (table, count)
            })
            .collect::<Vec<_>>()
    };
    let before = (version(), counts());
    assert!(!query(&harness, RUNNER).await["models"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        (version(), counts()),
        before,
        "a read must not commit any database write"
    );
}

async fn bind_operator(
    harness: &Arc<Harness>,
    principal: Value,
    harness_id: &str,
    session: &str,
    scoped: bool,
) -> Route {
    let channel = harness.reserve_channel();
    let mut bind = json!({
        "op":"route.bind", "route_channel":channel, "epoch":1,
        "target":{"kind":"management_surface","module_id":crate::DEFAULT_MODULE_ID},
        "identity":{"project_root":cortexkit_test_support::scratch_root(),"harness":harness_id,"session":session},
        "principal":principal,
    });
    if scoped {
        bind["scope"] = json!({"owner":reserved("prefrontal-core"),"ref":"head-scope", "scope_epoch":1,"kind":"head","owner_authorized":true});
    }
    harness.bind_frame(bind, channel, 1).await
}

#[tokio::test]
async fn every_caller_can_read_choices_regardless_of_operator_claims() {
    let (harness, _) = fixture();
    let params = json!({"runner_module":RUNNER});
    for (principal, host, session) in [
        (reserved("callosum"), "fed-phone:fingerprint", "ckios"),
        (reserved("callosum"), "fed-desktop:fingerprint", "ckmac"),
        (json!({"kind":"direct"}), "ck-desktop", "install"),
        (reserved("occipital"), "observer", "catalog"),
    ] {
        let route = bind_operator(&harness, principal, host, session, false).await;
        let (ty, reply) = route.call("route.head_choices", &params).await;
        assert_eq!(ty, FrameType::Response, "{host}: {reply}");
    }
    for (principal, host, session, scoped) in [
        (reserved("broca"), "ck-desktop", "install", false),
        (json!({"kind":"unverified"}), "ck-desktop", "install", false),
        (json!({"kind":"direct"}), "fed-phone:forged", "ckios", false),
        (
            json!({"kind":"direct"}),
            "alfonso",
            "prefrontal-core",
            false,
        ),
        (json!({"kind":"direct"}), "ck-desktop", "install", true),
    ] {
        let route = bind_operator(&harness, principal, host, session, scoped).await;
        let mut frame = route.request_frame("route.head_choices", &params);
        let mut forged: Value = serde_json::from_slice(&frame.body).unwrap();
        forged["principal"] = reserved("callosum");
        forged["harness"] = json!("fed-phone:forged");
        frame.body = serde_json::to_vec(&forged).unwrap();
        let frame = harness.handle(frame).await;
        let reply: Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(frame.header.ty, FrameType::Response, "{host}: {reply}");
        check_contract(RUNNER, &reply["result"]);
    }
}

#[tokio::test]
async fn public_choices_do_not_depend_on_operator_bind_bookkeeping() {
    let (harness, _) = fixture();
    let route = bind_operator(
        &harness,
        json!({"kind":"direct"}),
        "ck-desktop",
        "install",
        false,
    )
    .await;
    let frame = route.request_frame("route.head_choices", &json!({"runner_module":RUNNER}));
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    crate::head_choices::remember_bind(
        channel,
        epoch + 1,
        Some(&subc_protocol::Principal::Direct),
        false,
        "ck-desktop",
        "install",
    );
    crate::head_choices::forget_bind(channel, epoch);
    let (ty, reply) = route
        .call("route.head_choices", &json!({"runner_module":RUNNER}))
        .await;
    assert_eq!(ty, FrameType::Response);
    check_contract(RUNNER, &reply["result"]);
    crate::head_choices::forget_bind(channel, epoch + 1);
}

#[tokio::test]
async fn warming_unknown_runner_and_invalid_params_are_typed() {
    let (harness, credentials) = fixture();
    let route = harness.bind(reserved("occipital"), None).await;
    for params in [
        json!({}),
        json!({"runner_module":""}),
        json!({"runner_module":RUNNER,"extra":true}),
        json!(null),
    ] {
        let (ty, reply) = route.call("route.head_choices", &params).await;
        assert_eq!(ty, FrameType::Error);
        assert_eq!(reply["code"], "invalid_request", "{reply}");
    }
    let (ty, reply) = route
        .call("route.head_choices", &json!({"runner_module":"unknown"}))
        .await;
    assert_eq!(ty, FrameType::Response);
    assert_eq!(reply["result"]["models"], json!([]));
    check_contract("unknown", &reply["result"]);
    for missing in ["listing", "snapshot"] {
        let mut warming = if missing == "listing" {
            BrocaCredentials::default()
        } else {
            credentials.clone()
        };
        warming.catalog_snapshot = if missing == "listing" {
            credentials.catalog_snapshot.clone()
        } else {
            None
        };
        harness.set_credentials(warming);
        let (ty, reply) = route
            .call("route.head_choices", &json!({"runner_module":RUNNER}))
            .await;
        assert_eq!(ty, FrameType::Error);
        assert_eq!(reply["code"], "routing_not_ready");
        assert_eq!(
            reply["detail"],
            json!({"retryable":true,"retry_after_ms":1000})
        );
    }
}

#[tokio::test]
async fn variants_obey_exact_head_policy_and_runner_routes() {
    let (harness, _) = fixture();
    harness.edit_config(|config| {
        config["models"]["openai/a-head/xhigh"] = json!({"not_for":["head"]});
        config["runners"][0]["routes"] = json!([
            "openai/a-head/low",
            "openai/a-head/high",
            "openai/a-head/xhigh"
        ]);
    });
    let reply = query(&harness, RUNNER).await;
    assert_eq!(ids(&reply), ["openai/a-head"]);
    assert_eq!(reply["models"][0]["variants"], json!(["high", "low"]));
    assert_eq!(reply["models"][0]["default_variant"], "high");
}

#[test]
fn registered_query_is_handled_and_in_the_manifest() {
    let ops = crate::served_operations();
    let op = ops
        .iter()
        .find(|op| op.name == "route.head_choices")
        .unwrap();
    assert_eq!(
        op.kind,
        subc_protocol::manifest::ManagementOperationKind::Query
    );
    assert!(crate::handles(&op.name));
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    use sha2::{Digest, Sha256};
    let (harness, mut credentials) = fixture();
    let non_starting_runner = "non-starting-runner";
    let mut declaration = credentials.runner_declarations[RUNNER].clone();
    declaration.starts_sessions = false;
    credentials
        .runner_declarations
        .insert(non_starting_runner.into(), declaration);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"]
            .as_array_mut()
            .unwrap()
            .push(json!({"module":non_starting_runner,"via_vault":true}));
    });
    let route = harness.bind(reserved("occipital"), None).await;
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/route-head-choices-v1");
    let update = std::env::var_os("UPDATE_ROUTE_HEAD_CHOICES_VECTORS").is_some();
    let mut sums = String::new();
    for (name, runner) in [
        ("with-tiers.json", RUNNER),
        ("without-tiers.json", NO_TIERS),
        ("policy-excluded.json", "policy-runner"),
        ("pin-only.json", "pin-runner"),
        ("pin-only-pay-per-token.json", API_KEY_RUNNER),
        ("unknown-runner.json", "missing"),
        ("non-starting-runner.json", non_starting_runner),
    ] {
        let request = json!({"method":"route.head_choices","params":{"runner_module":runner}});
        let (ty, bytes) = route
            .call_raw("route.head_choices", &request["params"])
            .await;
        assert_eq!(
            ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        check_contract(runner, &reply["result"]);
        if ["missing", non_starting_runner].contains(&runner) {
            assert_eq!(reply["result"]["models"], json!([]), "{name}");
        }
        let bytes = serde_json::to_vec(&json!({"request":request,"reply":reply})).unwrap();
        let path = directory.join(name);
        if update {
            std::fs::write(&path, &bytes).unwrap();
        }
        assert_eq!(std::fs::read(path).unwrap(), bytes, "{name}");
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
    }
    let path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&path, &sums).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), sums);
}
