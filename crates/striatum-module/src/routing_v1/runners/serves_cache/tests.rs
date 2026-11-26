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
    let all =
        include_bytes!("../../../../fixtures/thalamus-routing-runner-serves-v1/all-harnesses.json");
    let one = include_bytes!(
        "../../../../fixtures/thalamus-routing-runner-serves-v1/opencode2-only.json"
    );
    let reply = include_bytes!(
        "../../../../fixtures/thalamus-routing-runner-serves-v1/describe-serves.json"
    );
    for (bytes, hash) in [
        (
            all.as_slice(),
            "2ccb7b9d5041a4c5fe995151b809458624ac3d6220645eabc934b4b161a350e2",
        ),
        (
            one.as_slice(),
            "45b8141f858ec34d9c53cb51ee1647ec07d6f2599b255aa4ac876ee3535ebf6f",
        ),
        (
            reply.as_slice(),
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
    declaration(&mut credentials, serde_json::from_slice(all).unwrap());
    assert_eq!(
        credentials.runner_declarations[MODULE]
            .harnesses
            .as_deref()
            .unwrap(),
        ["opencode2", "pi", "claude-code"]
    );
    let reply = serde_json::from_slice(reply).unwrap();
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
        assert!(go.includes(model, &Default::default()));
    }
    assert!(!go.includes("unsupported", &Default::default()));
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
    assert!(capped
        .iter()
        .all(|entry| !matches!(entry.provider.as_str(), "opencode" | "opencode-go")));
    for (provider, excluded) in [("mixed", "unsupported"), ("google", "no-endpoint")] {
        let entry = capped
            .iter()
            .find(|entry| entry.provider == provider)
            .unwrap();
        assert!(!entry.includes(excluded, &[excluded.into()].into()));
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
