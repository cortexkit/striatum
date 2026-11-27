use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::super::{parse_declaration, DescribeCache, Registry};
use super::LAST_GOOD_MS;
use crate::broca_credentials::BrocaCredentials;
use crate::config_policy::RunnerPolicy;
use crate::routing_v1::conformance_tests::{
    install_catalog, install_head_snapshot, reserved, Harness,
};
use cortexkit_role_routing_conformance::fixture;
use subc_protocol::FrameType;

const MODULE: &str = "thalamus-harness";
const THALAMUS_HELLO: &[u8] =
    include_bytes!("../../../../fixtures/thalamus-routing-runner-serves-v1/all-harnesses.json");
const THALAMUS_DESCRIBE_SERVES: &[u8] =
    include_bytes!("../../../../fixtures/thalamus-routing-runner-serves-v1/describe-serves.json");

fn declaration(credentials: &mut BrocaCredentials, runner: Value) {
    credentials.runner_declarations.insert(
        MODULE.into(),
        parse_declaration(&json!({"runner": runner}))
            .unwrap()
            .unwrap(),
    );
}

fn policy() -> Vec<RunnerPolicy> {
    serde_json::from_value(json!([{"module": MODULE, "via_vault": true}])).unwrap()
}

fn describe(list: Value) -> Value {
    json!({"capabilities": ["serves"], "serves": list})
}

#[test]
fn imported_agent_runner_hello_and_describe_use_the_real_readers() {
    let one = include_bytes!(
        "../../../../fixtures/thalamus-routing-runner-serves-v1/opencode2-only.json"
    );
    for (bytes, hash) in [
        (
            THALAMUS_HELLO,
            "2ccb7b9d5041a4c5fe995151b809458624ac3d6220645eabc934b4b161a350e2",
        ),
        (
            one.as_slice(),
            "45b8141f858ec34d9c53cb51ee1647ec07d6f2599b255aa4ac876ee3535ebf6f",
        ),
        (
            THALAMUS_DESCRIBE_SERVES,
            "48356960c5ced2483c6a2f93e46a5d4c9526cfbb530374cf9a95a655b9d23496",
        ),
    ] {
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), hash);
    }
    let mut credentials = BrocaCredentials::default();
    declaration(&mut credentials, serde_json::from_slice(one).unwrap());
    assert_eq!(
        credentials.runner_declarations[MODULE]
            .harnesses
            .as_deref()
            .unwrap(),
        ["opencode2"]
    );
    declaration(
        &mut credentials,
        serde_json::from_slice(THALAMUS_HELLO).unwrap(),
    );
    assert_eq!(
        credentials.runner_declarations[MODULE]
            .harnesses
            .as_deref()
            .unwrap(),
        ["opencode2", "pi", "claude-code"]
    );
    let reply = serde_json::from_slice(THALAMUS_DESCRIBE_SERVES).unwrap();
    assert!(DescribeCache::default().reply(MODULE, Some(&reply), &mut credentials, 100));
    let lists: std::collections::BTreeMap<_, _> =
        credentials.runner_serves[MODULE].lists(100).collect();
    assert!(lists[&Some("pi")].is_empty());
    assert!(lists[&Some("claude-code")].is_empty());
    let opencode = lists[&Some("opencode2")];
    let go = opencode
        .iter()
        .find(|entry| entry.provider == "opencode-go")
        .unwrap();
    for model in ["probe", "wide"] {
        assert!(go.placement(model).is_some());
    }
    assert!(go.placement("unsupported").is_none());
}

#[test]
fn imported_thalamus_describe_bodies_keep_the_opt_in_serves_placements() {
    use super::ListStatus;
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    use std::collections::{BTreeMap, HashSet};

    const NOW: i64 = 100;
    const HARNESSES: [&str; 3] = ["opencode2", "claude-code", "pi"];
    const PROVIDER: &str = "opencode-go";

    let current: Value = serde_json::from_slice(THALAMUS_DESCRIBE_SERVES).unwrap();
    let serves = &current["serves"];
    let models: HashSet<String> =
        serde_json::from_value(serves["harnesses"]["opencode2"][0]["models"].clone()).unwrap();
    assert!(!models.is_empty());
    let facts: Vec<_> = models
        .iter()
        .map(String::as_str)
        .chain(["unsupported"])
        .map(|model| crate::tests::fact(PROVIDER, model))
        .collect();

    let check = |sample: &str, reply: &Value, with_serves: bool| {
        let mut credentials = BrocaCredentials::default();
        declaration(
            &mut credentials,
            serde_json::from_slice(THALAMUS_HELLO).unwrap(),
        );
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "thalamus-opencode-go".into(),
                credentials: vec![ListedCredential {
                    id: "test-opencode-go-key".into(),
                    provider_ids: vec![PROVIDER.into()],
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    auth_method: Some("apikey".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            NOW as u64,
        );

        crate::take_operator_warnings();
        assert_eq!(
            DescribeCache::default().reply(MODULE, Some(reply), &mut credentials, NOW),
            with_serves,
            "{sample}, with serves: {with_serves}"
        );
        assert_eq!(credentials.runner_describe_observed.get(MODULE), Some(&NOW));
        let warnings = crate::take_operator_warnings();
        assert_eq!(warnings.len(), 1, "{sample}: {warnings:?}");
        assert!(
            warnings[0].contains("cause=observed_describe_without_tiers"),
            "{sample}: {warnings:?}"
        );

        if with_serves {
            let lists: BTreeMap<_, _> = credentials.runner_serves[MODULE].lists(NOW).collect();
            assert_eq!(lists.len(), HARNESSES.len(), "{sample}");
            let [go] = lists[&Some("opencode2")] else {
                panic!("{sample}: expected one OpenCode 2 provider");
            };
            assert_eq!(go.provider, PROVIDER, "{sample}");
            assert!(!go.all && !go.keyless, "{sample}: {go:?}");
            assert_eq!(go.ids, models, "{sample}");
            assert!(lists[&Some("claude-code")].is_empty(), "{sample}");
            assert!(lists[&Some("pi")].is_empty(), "{sample}");
        } else {
            assert!(!credentials.runner_serves.contains_key(MODULE), "{sample}");
        }

        for harness in HARNESSES {
            let registry = Registry::build_for_harness(
                policy(),
                &credentials,
                &facts,
                NOW as u64,
                Some(harness),
            );
            let runner = registry.get(MODULE).unwrap();
            assert!(runner.starts_sessions, "{sample}: {harness}");
            let status = runner.serves_status[&Some(harness.into())];
            assert!(
                matches!(
                    (with_serves, status),
                    (true, ListStatus::Ready) | (false, ListStatus::Missing)
                ),
                "{sample}: {harness}, with serves: {with_serves}, got {status:?}"
            );
            for fact in &facts {
                let route = format!("{PROVIDER}/{}", fact.model_id);
                assert_eq!(
                    registry.runs(MODULE, &route, &route),
                    with_serves && harness == "opencode2" && fact.model_id != "unsupported",
                    "{sample}: {harness}, with serves: {with_serves}, model {route}"
                );
            }
        }
    };

    check("current", &current, true);
    for (harness, bytes, hash) in [
        (
            "opencode2",
            include_bytes!(
                "../../../../fixtures/thalamus-describe-530d6c9f9/role-describe-opencode2.json"
            )
            .as_slice(),
            "80a83f8d9a2ff00039c2e40539ed5547fabb1b4ee6692a150a2a846ddb517da8",
        ),
        (
            "claude-code",
            include_bytes!(
                "../../../../fixtures/thalamus-describe-530d6c9f9/role-describe-claude-code.json"
            )
            .as_slice(),
            "4213dd487460a6b25d75cf772c545584286ced9c4f4c4815993d9b8d5310077d",
        ),
        (
            "pi",
            include_bytes!(
                "../../../../fixtures/thalamus-describe-530d6c9f9/role-describe-pi.json"
            )
            .as_slice(),
            "738526b0fedb8c8420b1efea9ec4b945f6dd231fbc02a3363cc8455031c82c51",
        ),
    ] {
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), hash, "{harness}");
        let mut reply: Value = serde_json::from_slice(bytes).unwrap();
        check(harness, &reply, false);
        // The opt-in response includes the whole runner's harness map.
        assert!(reply
            .as_object_mut()
            .unwrap()
            .insert("serves".into(), serves.clone())
            .is_none());
        check(harness, &reply, true);
    }
}

#[test]
fn imported_broca_serves_platform_aliases_by_the_vaults_catalog_link() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    use cortexkit_role_routing::placement::HarnessCapability;

    let hello =
        include_bytes!("../../../../fixtures/broca-runner-serves-1126/hello-routing-slot.json");
    let before = include_bytes!(
        "../../../../fixtures/broca-runner-serves-1126/role-describe-default-before.json"
    );
    let after = include_bytes!(
        "../../../../fixtures/broca-runner-serves-1126/role-describe-default-after.json"
    );
    let reply =
        include_bytes!("../../../../fixtures/broca-runner-serves-1126/role-describe-serves.json");
    for (bytes, hash) in [
        (
            hello.as_slice(),
            "d11a7a6f00ef76fcf80f8b6e719b71f4417a7e992884c6c5247028979e9bc387",
        ),
        (
            before.as_slice(),
            "6adbe2f178d85bb257eccaba637bbd4eb20fc87fc5f233e6f7222a46947b7110",
        ),
        (
            after.as_slice(),
            "39fbc7fe630d8c3e5d5aed7e477ce1d1f6361d4329077f9a8ae75dc3b705e4e4",
        ),
        (
            reply.as_slice(),
            "ee737df8a5b7dac07983e1d10433cc62412a4469b5bfe896f64223d5a261bf31",
        ),
    ] {
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), hash);
    }
    let mut credentials = BrocaCredentials::default();
    let hello = serde_json::from_slice(hello).unwrap();
    let runner = parse_declaration(&hello).unwrap().unwrap();
    assert!(runner.harness_supports("broca", &HarnessCapability::OutputCap));
    assert!(!runner.harness_supports("broca-uncapped", &HarnessCapability::OutputCap));
    credentials
        .runner_declarations
        .insert("broca".into(), runner);
    let mut cache = DescribeCache::default();
    let before: Value = serde_json::from_slice(before).unwrap();
    let mut after: Value = serde_json::from_slice(after).unwrap();
    assert!(cache.reply("broca", Some(&before), &mut credentials, 0));
    assert!(!cache.reply("broca", Some(&after), &mut credentials, 1));
    let capabilities = after["capabilities"].as_array_mut().unwrap();
    let index = capabilities
        .iter()
        .position(|value| value == "serves")
        .unwrap();
    capabilities.remove(index);
    assert_eq!(after, before);
    let reply = serde_json::from_slice(reply).unwrap();
    assert!(cache.reply("broca", Some(&reply), &mut credentials, 2));
    let lists: std::collections::BTreeMap<_, _> =
        credentials.runner_serves["broca"].lists(2).collect();
    let capped = lists[&Some("broca")];
    let uncapped = lists[&Some("broca-uncapped")];
    assert_eq!(
        uncapped
            .iter()
            .map(|entry| entry.provider.as_str())
            .collect::<Vec<_>>(),
        ["openai"]
    );
    for (provider, excluded) in [("mixed", "unsupported"), ("google", "no-endpoint")] {
        let entry = capped
            .iter()
            .find(|entry| entry.provider == provider)
            .unwrap();
        assert!(entry.placement(excluded).is_none());
    }

    let mut facts = vec![
        crate::tests::fact("anthropic", "claude"),
        crate::tests::fact("openai", "gpt"),
    ];
    facts[1].auth_methods = Some(vec!["apikey".into(), "chatgpt".into()]);
    facts[1].login_methods_documented = true;
    let policies: Vec<RunnerPolicy> =
        serde_json::from_value(json!([{"module":"broca","via_vault":true}])).unwrap();
    let check = |credentials: &BrocaCredentials, alias: &str, route: &str, expected: bool| {
        let targets = credentials.catalog_targets(&facts);
        let mut facts = facts.clone();
        striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
        assert_eq!(
            Registry::build_for_harness(policies.clone(), credentials, &facts, 3, Some("broca"))
                .runs("broca", route, route),
            expected,
            "{alias}"
        );
    };
    for (alias, target, route) in [
        (
            "anthropic-platform",
            "anthropic",
            "anthropic-platform/claude",
        ),
        ("openai-platform", "openai", "openai-platform/gpt"),
    ] {
        check(&credentials, alias, route, false);
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: alias.into(),
                credentials: vec![ListedCredential {
                    id: format!("test-{alias}-key"),
                    provider_ids: vec![alias.into()],
                    serves: vec![target.into()],
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    auth_method: Some("apikey".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            3,
        );
        assert_eq!(
            credentials
                .catalog_targets(&facts)
                .get(alias)
                .map(String::as_str),
            Some(target)
        );
        check(&credentials, alias, route, true);
    }
}

#[test]
fn runner_serves_expires_a_missing_harness_without_extending_the_last_good_read() {
    let mut credentials = BrocaCredentials::default();
    declaration(
        &mut credentials,
        json!({"harnesses":["alpha","beta"], "default_harness":"alpha"}),
    );
    let facts = [crate::tests::fact("opencode", "free")];
    let place = |credentials: &BrocaCredentials, now: i64| {
        Registry::build_for_selection(policy(), credentials, &facts, now as u64, None).runs(
            MODULE,
            "opencode/free",
            "opencode/free",
        )
    };
    assert!(
        !place(&credentials, 0),
        "a runner that never answered serves nothing"
    );
    let mut cache = DescribeCache::default();
    let ready = describe(
        json!({"generation":"ready", "harnesses":{"alpha":[{"provider":"opencode", "models":"all", "login":"none"}]}}),
    );
    assert!(cache.reply(MODULE, Some(&ready), &mut credentials, 0));
    assert!(place(&credentials, 0));
    assert!(cache.reply(MODULE, Some(&ready), &mut credentials, 100));
    let absent = describe(json!({"generation":"not-ready", "harnesses":{"beta":[]}}));
    assert!(cache.reply(MODULE, Some(&absent), &mut credentials, LAST_GOOD_MS / 2));
    let invalid = describe(json!({"generation":"ready", "harnesses":{"alpha":[],"unknown":[]}}));
    assert!(!cache.reply(MODULE, Some(&invalid), &mut credentials, LAST_GOOD_MS - 1));
    assert!(place(&credentials, LAST_GOOD_MS + 99));
    assert!(
        !place(&credentials, LAST_GOOD_MS + 100),
        "missing list expires at the original deadline"
    );
    assert!(cache.reply(MODULE, Some(&ready), &mut credentials, LAST_GOOD_MS + 101));
    assert!(place(&credentials, LAST_GOOD_MS + 101));
    let empty = describe(json!({"generation":"empty", "harnesses":{"alpha":[]}}));
    assert!(cache.reply(MODULE, Some(&empty), &mut credentials, LAST_GOOD_MS + 102));
    assert!(!place(&credentials, LAST_GOOD_MS + 102));
}

#[tokio::test]
async fn keyless_selection_does_not_admit_an_account_even_when_a_login_appears() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

    let harness = Harness::new("keyless-runner");
    let mut catalog = fixture::catalog();
    catalog.accounts.clear();
    catalog.runners.truncate(1);
    catalog.runners[0].module = MODULE.into();
    catalog.runners[0].harnesses = Some(vec!["opencode2".into()]);
    catalog.runners[0].default_harness = Some("opencode2".into());
    catalog.models.truncate(1);
    catalog.models[0].provider = "opencode".into();
    let key = catalog.models[0].route_key();
    let model = catalog.models[0].model.clone();
    catalog.runners[0].route_keys = vec![key.clone()];
    install_catalog(&harness, &catalog);
    harness.credentials().lock().unwrap().apply_listing(
        &ScopedCredentialListing {
            view: "no-opencode-accounts".into(),
            ..Default::default()
        },
        crate::now_ms(),
    );
    install_head_snapshot(&harness);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":MODULE,"via_vault":true}]);
        config["providers"]
            .as_object_mut()
            .unwrap()
            .remove("opencode");
    });
    harness.publish_runner_serves(
        MODULE,
        &json!({
            "generation":"keyless", "harnesses":{"opencode2":[
                {"provider":"opencode", "models":[model], "login":"none"}
            ]}
        }),
    );
    assert!(harness
        .credentials()
        .lock()
        .unwrap()
        .listed_credentials()
        .is_empty());
    let consumer = harness.bind(reserved(fixture::CONSUMER), None).await;
    let (ty, reply) = consumer
        .call(
            "route.select",
            &json!({
                "consumer":"sidekick", "requirements":{"iq":0}
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    let decision = &reply["result"];
    assert_eq!(decision["route"]["provider"], "opencode");
    assert_eq!(decision["runner"]["harness"], "opencode2");
    assert!(decision["runner"].get("auth").is_none());
    assert!(decision.get("account_class").is_none());

    {
        let shared = harness.credentials();
        let credentials = shared.lock().unwrap();
        let mut cold = BrocaCredentials::default();
        cold.catalog_snapshot = credentials.catalog_snapshot.clone();
        cold.runner_declarations = credentials.runner_declarations.clone();
        cold.runner_serves = credentials.runner_serves.clone();
        let error = crate::head_choices::choices_for_harness(
            harness.store(),
            &harness.operator_config(),
            &cold,
            MODULE,
            Some("opencode2"),
        )
        .unwrap_err();
        assert_eq!(error.code, "routing_not_ready");
    }

    let choices = |pin_only| {
        let credentials = harness.credentials();
        let credentials = credentials.lock().unwrap();
        let choices = crate::head_choices::choices_for_harness(
            harness.store(),
            &harness.operator_config(),
            &credentials,
            MODULE,
            Some("opencode2"),
        )
        .unwrap();
        let model = &choices["models"][0];
        assert_eq!(model["provider"], "opencode", "{choices}");
        assert_eq!(model["pin_only"], pin_only, "{choices}");
        assert!(model.get("billing").is_none(), "{choices}");
    };
    choices(false);
    harness.edit_config(|config| config["providers"]["opencode"]["pin_only"] = json!(true));
    choices(true);

    let shared = harness.credentials();
    shared.lock().unwrap().apply_listing(
        &ScopedCredentialListing {
            view: "later-login".into(),
            credentials: vec![ListedCredential {
                id: "unused-key".into(),
                provider_ids: vec!["opencode".into()],
                categories: vec!["llm-provider".into()],
                credential_type: "apikey".into(),
                auth_method: Some("apikey".into()),
                state: "active".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        crate::now_ms(),
    );
    let runner = harness.bind(reserved(MODULE), None).await;
    let (ty, admission) = runner
        .call(
            "route.account_for",
            &json!({
                "provider":"opencode", "model":model, "decision_id":decision["decision_id"],
                "conversation_key":"keyless", "admission_id":"keyless-test"
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Error, "{admission}");
    assert_eq!(admission["code"], "account_unservable", "{admission}");
    assert_eq!(
        admission["detail"],
        json!({"reason":"login_not_required"}),
        "{admission}"
    );
}

#[tokio::test]
async fn unbound_account_for_considers_all_fresh_harnesses_and_catalog_aliases() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

    let harness = Harness::new("mixed-keyless-harnesses");
    let mut catalog = fixture::catalog();
    catalog.runners.truncate(1);
    catalog.runners[0].module = MODULE.into();
    catalog.runners[0].harnesses = Some(vec!["alpha".into(), "gateway".into()]);
    catalog.runners[0].default_harness = Some("alpha".into());
    install_catalog(&harness, &catalog);
    let model = catalog.models[0].model.as_str();
    let provider = catalog.models[0].provider.as_str();
    let alias = "alias-account-pool";
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        declaration(
            &mut credentials,
            json!({"follows_account_choice":true,
            "harnesses":["alpha","gateway"],"default_harness":"alpha",
            "harness_limits":{"gateway":{"starts_sessions":false}}}),
        );
        let mut rows = credentials.listed_credentials().to_vec();
        rows.push(ListedCredential {
            id: "alias-key".into(),
            provider_ids: vec![alias.into()],
            serves: vec![provider.into()],
            categories: vec!["llm-provider".into()],
            credential_type: "apikey".into(),
            auth_method: Some("apikey".into()),
            state: "active".into(),
            ..Default::default()
        });
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "mixed-keyless-harnesses".into(),
                credentials: rows,
                ..Default::default()
            },
            crate::now_ms(),
        );
    }
    harness.edit_config(|config| config["runners"] = json!([{"module":MODULE,"via_vault":true}]));
    let runner = harness.bind(reserved(MODULE), None).await;
    let keyless = json!([{"provider":alias,"models":"all","login":"none"}]);
    let credentialed = json!([{"provider":alias,"models":[model]}]);
    for (id, alpha, gateway, expects_refusal) in [
        ("agree", keyless.clone(), keyless.clone(), true),
        ("only-non-starting", json!([]), keyless.clone(), true),
        ("disagree", keyless.clone(), credentialed, false),
    ] {
        harness.publish_runner_serves(
            MODULE,
            &json!({"generation":id,"harnesses":{
                "alpha":alpha,"gateway":gateway
            }}),
        );
        let shared = harness.credentials();
        assert_eq!(
            super::super::unbound_route_is_keyless_with(
                &shared,
                MODULE,
                alias,
                model,
                crate::now_ms(),
                || {
                    assert!(
                        shared.try_lock().is_ok(),
                        "the catalog read must not hold the credentials lock"
                    );
                    use striatum_core::RouteStateStore;
                    harness.store().get_facts().ok()
                }
            ),
            expects_refusal,
            "{id}"
        );
        let (ty, reply) = runner
            .call(
                "route.account_for",
                &json!({
                    "provider":alias,"model":model,"conversation_key":id,"admission_id":id
                }),
            )
            .await;
        if expects_refusal {
            assert_eq!(ty, FrameType::Error, "{id}: {reply}");
            assert_eq!(reply["code"], "account_unservable", "{id}: {reply}");
            assert_eq!(
                reply["detail"],
                json!({"reason":"login_not_required"}),
                "{id}: {reply}"
            );
        } else {
            assert_ne!(
                reply["detail"]["reason"], "login_not_required",
                "{id}: {reply}"
            );
        }
    }
}

#[test]
fn runner_list_uses_catalog_login_methods_not_transport_to_split_google_models() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

    let mut alias = crate::tests::fact("google", "antigravity-example");
    alias.auth_methods = Some(vec!["antigravity".into()]);
    alias.login_methods_documented = true;
    let facts = [crate::tests::fact("google", "gemini-example"), alias];
    for (method, allowed, refused) in [
        (
            "apikey",
            "google/gemini-example",
            "google/antigravity-example",
        ),
        (
            "antigravity",
            "google/antigravity-example",
            "google/gemini-example",
        ),
    ] {
        let mut credentials = BrocaCredentials::default();
        declaration(&mut credentials, json!({"follows_account_choice":true}));
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: format!("only-{method}"),
                credentials: vec![ListedCredential {
                    id: "google-test-credential".into(),
                    auth_method: Some(method.into()),
                    provider_ids: vec!["google".into()],
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            0,
        );
        assert!(DescribeCache::default().reply(
            MODULE,
            Some(&describe(json!({
                "generation":"google", "entries":[{"provider":"google", "models":"all"}]
            }))),
            &mut credentials,
            0
        ));
        let registry = Registry::build(policy(), &credentials, &facts, 0);
        assert!(registry.runs(MODULE, allowed, allowed), "{method}");
        assert!(!registry.runs(MODULE, refused, refused), "{method}");
    }
}

#[tokio::test]
async fn two_configured_vault_runners_keep_consumer_order_and_account_authority() {
    const BROCA: &str = "broca";
    let harness = Harness::new("two-vault-runners");
    let mut catalog = fixture::catalog();
    catalog.runners[0].module = BROCA.into();
    catalog.runners[0].harnesses = Some(vec![BROCA.into()]);
    catalog.runners[0].default_harness = Some(BROCA.into());
    catalog.runners[1].module = MODULE.into();
    catalog.runners[1].harnesses = Some(vec!["opencode2".into()]);
    catalog.runners[1].default_harness = Some("opencode2".into());
    install_catalog(&harness, &catalog);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":"broca","via_vault":true},
            {"module":"thalamus-harness","via_vault":true}
        ]);
        for consumer in ["sidekick", "mason"] {
            config["consumers"][consumer]["runners"] = json!([
                {"module":"broca"}, {"module":"thalamus-harness"}
            ]);
        }
    });
    let consumer = harness.bind(reserved(fixture::CONSUMER), None).await;
    let model = &catalog.models[2];
    let pin = model.route_key();
    for name in ["sidekick", "mason"] {
        for (runners, expected) in [(vec![BROCA, MODULE], BROCA), (vec![MODULE], MODULE)] {
            let (ty, reply) = consumer
                .call(
                    "route.select",
                    &json!({
                        "consumer":name, "requirements":{"iq":0}, "pin":pin, "runners":runners
                    }),
                )
                .await;
            assert_eq!(ty, FrameType::Response, "{reply}");
            let pick = &reply["result"];
            assert_eq!(pick["route"]["provider"], model.provider);
            assert_eq!(pick["route"]["model"], model.model);
            assert_eq!(pick["runner"]["module"], expected);
            let runner = harness.bind(reserved(expected), None).await;
            let (ty, answer) = runner.call("route.account_for", &json!({
                "provider":model.provider, "model":model.model,
                "conversation_key":format!("{name}-{expected}"), "admission_id":format!("{name}-{expected}"),
                "decision_id":pick["decision_id"]
            })).await;
            assert_eq!(ty, FrameType::Response, "{answer}");
            assert_eq!(answer["result"]["account"]["provider"], model.provider);
        }
    }
}

#[tokio::test]
async fn harness_unavailable_names_the_missing_excluded_and_expired_lists() {
    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("serve-list-diagnostics");
    let mut catalog = fixture::catalog();
    catalog.runners.truncate(1);
    let runner = &mut catalog.runners[0];
    runner.module = MODULE.into();
    runner.harnesses = Some(vec!["alpha".into(), "beta".into()]);
    runner.default_harness = Some("alpha".into());
    install_catalog(&harness, &catalog);
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_serves
        .remove(MODULE);
    let served = json!([{"provider":fixture::PRICED_PROVIDER, "models":[catalog.models[0].model]}]);
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let request = json!({"consumer":"mason","requirements":{"iq":0},
        "available_routes":[catalog.models[0].route_key()], "harness":"alpha"});
    for (now, serves, reason) in [
        (
            NOW,
            json!({"generation":"beta", "harnesses":{"beta":served}}),
            "serves_list_missing",
        ),
        (
            NOW,
            json!({"generation":"alpha-empty", "harnesses":{"alpha":[],"beta":served}}),
            "model_not_served",
        ),
        (
            NOW + LAST_GOOD_MS as u64 + 1,
            json!({"generation":"beta", "harnesses":{"beta":served}}),
            "serves_list_expired",
        ),
    ] {
        let _at = crate::InjectedClock::set(now);
        harness.publish_runner_serves(MODULE, &serves);
        let (ty, reply) = caller.call("route.select", &request).await;
        assert_eq!(ty, FrameType::Error, "{reply}");
        assert_eq!(reply["detail"]["reason"], "harness_unavailable", "{reply}");
        let row = &reply["detail"]["runners"][0];
        assert_eq!(row["module"], MODULE, "{reply}");
        assert_eq!(
            row["unavailable_harnesses"][0]["harness"], "alpha",
            "{reply}"
        );
        assert_eq!(row["unavailable_harnesses"][0]["reason"], reason, "{reply}");
        assert!(
            row["unavailable_harnesses"][0]["message"].is_string(),
            "{reply}"
        );
    }
}

#[tokio::test]
async fn unreachable_runner_lists_distinguish_pending_expired_and_unlisted() {
    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("unreachable-runner-lists");
    let mut catalog = fixture::catalog();
    catalog.runners.truncate(1);
    catalog.runners[0].module = MODULE.into();
    catalog.runners[0].harnesses = Some(vec!["alpha".into(), "beta".into()]);
    catalog.runners[0].default_harness = Some("alpha".into());
    install_catalog(&harness, &catalog);
    let mut cache = DescribeCache::default();
    let modules = json!([{"module_id":MODULE,"nonce_verified":true,
        "role_declarations":{"routing/v1":{"runner":{"harnesses":["alpha","beta"],"default_harness":"alpha"}}}}]);
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        credentials.runner_serves.remove(MODULE);
        cache.catalog(&modules, &policy(), &mut credentials, NOW as i64);
    }
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;
    let key = catalog.models[0].route_key();
    let mut request = json!({"consumer":"mason","requirements":{"iq":0},"available_routes":[key],"harness":"alpha"});
    for (case, serves, at, reason, delay) in [
        ("first", None, NOW, "serves_list_missing", Some(10_000)),
        (
            "omitted",
            Some(json!({"generation":"omitted","harnesses":{"beta":[]}})),
            NOW,
            "serves_list_missing",
            Some(10_000),
        ),
        (
            "expired",
            Some(json!({"generation":"ready","harnesses":{"alpha":[],"beta":[]}})),
            NOW + LAST_GOOD_MS as u64,
            "serves_list_expired",
            None,
        ),
        (
            "unlisted",
            Some(json!({"generation":"empty","harnesses":{"alpha":[],"beta":[]}})),
            NOW + LAST_GOOD_MS as u64,
            "model_not_served",
            None,
        ),
    ] {
        if let Some(serves) = serves {
            let _published = crate::InjectedClock::set(if case == "expired" { NOW } else { at });
            harness.publish_runner_serves(MODULE, &serves);
        }
        let _at = crate::InjectedClock::set(at);
        let (ty, reply) = route.call("route.select", &request).await;
        assert_eq!(ty, FrameType::Error, "{case}: {reply}");
        assert_eq!(
            reply["detail"]["reason"], "missing_capability",
            "{case}: {reply}"
        );
        assert_eq!(
            reply["detail"]["reset_at_ms"].as_u64(),
            delay.map(|delay| at + delay),
            "{case}: {reply}"
        );
        let row = &reply["detail"]["runners"][0];
        assert_eq!(row["module"], MODULE, "{case}: {reply}");
        assert_eq!(
            row["unavailable_harnesses"][0]["harness"], "alpha",
            "{case}: {reply}"
        );
        assert_eq!(
            row["unavailable_harnesses"][0]["reason"], reason,
            "{case}: {reply}"
        );
    }

    request["pin"] = json!(key);
    let _at = crate::InjectedClock::set(NOW + LAST_GOOD_MS as u64);
    let (_, reply) = route.call("route.select", &request).await;
    assert_eq!(reply["code"], "pin_unavailable", "{reply}");
    assert!(reply["detail"]["runners"].is_array(), "{reply}");
    let model = catalog.models[0].model.as_str();
    harness.publish_runner_serves(
        MODULE,
        &json!({"generation":"requires-login","harnesses":{
            "alpha":[{"provider":catalog.models[0].provider,"models":[model]}],"beta":[]
        }}),
    );
    harness.credentials().lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "stale-vault".into(),
            ..Default::default()
        },
        crate::now_ms() - 60 * 60_000,
    );
    let (_, reply) = route.call("route.select", &request).await;
    assert_eq!(
        reply["code"], "no_route",
        "a valid list cannot mask stale vault evidence: {reply}"
    );
    assert_eq!(reply["detail"]["reason"], "missing_capability", "{reply}");
    assert!(reply["detail"].get("reset_at_ms").is_none(), "{reply}");
}

#[tokio::test]
async fn keyless_panel_ignores_a_blocked_key_and_does_not_reserve_its_probe() {
    use crate::routing_v1::conformance_tests::install_panel_families;
    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("keyless-panel");
    let mut catalog = fixture::catalog();
    catalog.runners.truncate(1);
    catalog.models.truncate(1);
    catalog.models[0].provider = "opencode".into();
    catalog.models[0].model = "hy4-flash-free".into();
    catalog.models[0].family = Some("free-model".into());
    let key = catalog.models[0].route_key();
    catalog.runners[0].module = MODULE.into();
    catalog.runners[0].harnesses = Some(vec!["opencode2".into()]);
    catalog.runners[0].default_harness = Some("opencode2".into());
    catalog.runners[0].route_keys = vec![key.clone()];
    install_catalog(&harness, &catalog);
    install_panel_families(&harness, &catalog);
    harness.edit_config(|config| {
        config["runners"][0]["via_vault"] = json!(true);
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq","extra":0,"floor":0});
        config["providers"]["opencode"]["pin_only"] = json!(false);
    });
    let credential = {
        let shared = harness.credentials();
        let credentials = shared.lock().unwrap();
        credentials
            .listed_credentials()
            .iter()
            .find(|row| row.provider_ids.iter().any(|p| p == "opencode"))
            .expect("fixture opencode key")
            .id
            .clone()
    };
    harness.publish_runner_serves(
        MODULE,
        &json!({"generation":"keyless","harnesses":{"opencode2":[
            {"provider":"opencode","models":["hy4-flash-free"],"login":"none"}
        ]}}),
    );
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "opencode".into(),
            credential_id: credential.clone(),
            model: "hy4-flash-free".into(),
            until_ms: NOW as i64 + 86_400_000,
            decision_id: "earlier-key-turn".into(),
            recorded_at: NOW as i64,
        })
        .unwrap();
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;
    let (ty, reply) = route
        .call(
            "route.select_panel",
            &json!({"class":"audit","consumer":"mason","available_routes":[key],"seed":"fixed"}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    assert_eq!(
        reply["result"]["seats"][0]["answer"]["route"]["model"], "hy4-flash-free",
        "{reply}"
    );
    assert!(
        reply["result"]["seats"][0]["answer"]["runner"]
            .get("auth")
            .is_none(),
        "{reply}"
    );
    let clocks = harness.store().fence_rechecks().unwrap();
    let clock = clocks
        .iter()
        .find(|clock| clock.credential_id.as_deref() == Some(&credential))
        .expect("fence is due for recovery");
    assert!(
        clock.last_claimed_at.is_none() && clock.claim_owner.is_none(),
        "{clock:?}"
    );
}

#[tokio::test]
async fn a_keyless_base_does_not_remove_a_credentialed_variants_quota_wall() {
    use striatum_core::RouteStateStore;

    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("shared-capacity-keyless");
    let mut catalog = fixture::catalog();
    catalog.models.truncate(1);
    catalog.models[0].provider = "opencode".into();
    catalog.models[0].model = "free-and-high".into();
    let model = &catalog.models[0].model;
    let route = catalog.models[0].route_key();
    let variant = format!("{route}/high");
    for (runner, module) in catalog.runners.iter_mut().zip([MODULE, "broca"]) {
        runner.module = module.into();
        runner.harnesses = None;
        runner.default_harness = None;
        runner.route_keys = vec![route.clone()];
    }
    install_catalog(&harness, &catalog);
    let mut high = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "opencode" && &fact.model_id == model)
        .unwrap();
    high.variant = Some("high".into());
    harness
        .store()
        .upsert_fact(&high, striatum_core::FactSource::Override)
        .unwrap();
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":"broca","via_vault":true,"routes":[variant]},
            {"module":MODULE,"via_vault":true,"routes":[route]},
        ]);
        config["providers"]["opencode"]["pin_only"] = json!(false);
    });
    harness.publish_runner_serves(
        MODULE,
        &json!({"generation":"keyless","entries":[
            {"provider":"opencode","models":[model],"login":"none"}
        ]}),
    );
    let owner = harness.bind(reserved(fixture::CONSUMER), None).await;
    let pin = |key: String| json!({"consumer":"mason","requirements":{"iq":0},"pin":key,"hold_wait_max_s":0});
    let high = pin(format!("{route}:high"));
    let (frame, before) = owner.call("route.select", &high).await;
    assert_eq!(frame, FrameType::Response, "{before}");
    assert_eq!(before["result"]["runner"]["module"], "broca", "{before}");
    let until = NOW + 120_000;
    harness
        .store()
        .update_route_state(
            &route,
            &route,
            &striatum_core::RouteStateUpdate {
                quota_exhausted_until: Some(until as f64),
                observed_at_ms: Some(NOW as i64),
                wall_class: Some(striatum_core::RouteWallClass::Quota),
                ..Default::default()
            },
        )
        .unwrap();
    let (frame, base) = owner.call("route.select", &pin(route)).await;
    assert_eq!(frame, FrameType::Response, "{base}");
    assert_eq!(base["result"]["runner"]["module"], MODULE, "{base}");
    let (frame, reply) = owner.call("route.select", &high).await;
    assert_eq!(
        frame,
        FrameType::Error,
        "the credentialed variant must not bypass quota: {reply}"
    );
    assert_eq!(reply["code"], "pin_unavailable", "{reply}");
    assert_eq!(reply["detail"]["reason"], "quota_exhausted", "{reply}");
    assert_eq!(reply["detail"]["reset_at_ms"], until, "{reply}");
}
