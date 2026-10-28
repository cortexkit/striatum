//! Runner facts never expand operator routes or derive login methods.

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use cortexkit_role_routing_conformance::fixture;
use prefrontal_routing_core::{ModelFact, RouteStateStore};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::{
    conformance_tests::{install_catalog, reserved, Harness},
    runners::{self, Registry},
};
use crate::{broca_credentials::BrocaCredentials, config_policy::RunnerPolicy};

const NOW: u64 = 1_700_000_000_000;

fn fact(provider: &str, model: &str, methods: Option<&[&str]>) -> ModelFact {
    let mut fact = crate::tests::fact(provider, model);
    fact.auth_methods =
        methods.map(|methods| methods.iter().map(|method| (*method).into()).collect());
    fact
}

fn row(providers: &[&str], serves: &[&str], method: Option<&str>) -> ListedCredential {
    ListedCredential {
        id: "opaque-credential".into(),
        account_id: Some("account-a".into()),
        categories: vec!["llm-provider".into()],
        state: "active".into(),
        credential_type: "oauth".into(),
        refresh_adapter: Some("anthropic".into()),
        auth_method: method.map(str::to_owned),
        provider_ids: providers
            .iter()
            .map(|provider| (*provider).into())
            .collect(),
        serves: serves.iter().map(|vendor| (*vendor).into()).collect(),
        ..Default::default()
    }
}

fn credentials(facts: &[ModelFact], row: ListedCredential, now: u64) -> BrocaCredentials {
    let mut credentials = BrocaCredentials::api_test_credentials(facts);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "runner-fixture".into(),
            credentials: vec![row],
            ..Default::default()
        },
        now,
    );
    credentials.wire_apis = facts
        .iter()
        .map(|fact| {
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                "messages".into(),
            )
        })
        .collect();
    credentials
}

fn policy(routes: Option<Vec<&str>>) -> RunnerPolicy {
    RunnerPolicy {
        module: "fixture-runner".into(),
        routes: routes.map(|routes| routes.into_iter().map(str::to_owned).collect()),
        tool_capabilities: vec!["interim/v1".into()],
        via_vault: true,
    }
}

fn declaration(pairs: Value) -> runners::RunnerDeclaration {
    runners::parse_declaration(&json!({"runner": {
        "pairs": pairs, "follows_account_choice": true, "tool_capabilities": ["test:declared/v1"]
    }}))
    .unwrap()
    .unwrap()
}

fn routes(registry: Registry) -> Vec<String> {
    registry
        .entries
        .into_iter()
        .next()
        .unwrap()
        .policy
        .routes
        .unwrap()
}

#[test]
fn converted_via_vault_runner_matches_the_frozen_served_set_without_a_declaration() {
    let facts = vec![
        fact("vendor-a", "unrestricted", None),
        fact("vendor-a", "allowed", Some(&["oauth"])),
        fact("vendor-a", "wrong-method", Some(&["apikey"])),
        fact("vendor-b", "uncredentialed", Some(&["oauth"])),
    ];
    let credentials = credentials(&facts, row(&["vendor-a"], &[], Some("oauth")), NOW);
    let defaults: Value =
        serde_json::from_str(include_str!("../../data/config_convert_defaults.json")).unwrap();
    let mut converted: RunnerPolicy =
        serde_json::from_value(defaults["runners"][0].clone()).unwrap();
    assert!(converted.via_vault);
    converted.module = "fixture-runner".into();
    let actual = routes(Registry::build(vec![converted], &credentials, &facts, NOW));
    assert_eq!(actual, ["vendor-a/unrestricted", "vendor-a/allowed"]);
    let (previous, fallback) = credentials.served_routes(&facts, NOW);
    assert_eq!(fallback, None);
    assert_eq!(actual, previous);
}

#[test]
fn declaration_pairs_intersect_routes_and_replace_interim_facts() {
    let facts = vec![
        fact("vendor-a", "allowed", Some(&["oauth"])),
        fact("vendor-b", "outside", Some(&["oauth"])),
    ];
    let mut credentials = credentials(
        &facts,
        row(&["vendor-a", "vendor-b"], &[], Some("oauth")),
        NOW,
    );
    let policies = vec![policy(Some(vec!["vendor-a/allowed"]))];
    credentials.runner_declarations.insert("fixture-runner".into(), declaration(json!([
        {"wire_api": "messages", "auth_method": "oauth"}, {"wire_api": "other", "auth_method": "apikey"}
    ])));
    let broad = Registry::build(policies.clone(), &credentials, &facts, NOW);
    assert_eq!(
        broad.entries[0].policy.routes.as_ref().unwrap(),
        &["vendor-a/allowed"]
    );
    assert_eq!(
        broad.entries[0].policy.tool_capabilities,
        ["test:declared/v1"]
    );
    assert!(broad.entries[0].follows_account_choice);
    credentials.runner_declarations.insert(
        "fixture-runner".into(),
        declaration(json!([
            {"wire_api": "other", "auth_method": "oauth"}
        ])),
    );
    assert!(routes(Registry::build(policies, &credentials, &facts, NOW)).is_empty());
}

#[test]
fn missing_auth_method_never_inherits_a_refresh_adapter_method() {
    let facts = vec![
        fact("vendor-a", "open", None),
        fact("vendor-a", "restricted", Some(&["oauth"])),
    ];
    let mut credentials = credentials(&facts, row(&["vendor-a"], &[], None), NOW);
    assert_eq!(
        routes(Registry::build(
            vec![policy(None)],
            &credentials,
            &facts,
            NOW
        )),
        ["vendor-a/open"]
    );
    credentials.runner_declarations.insert(
        "fixture-runner".into(),
        declaration(json!([{"wire_api":"messages", "auth_method":"oauth"}])),
    );
    assert!(routes(Registry::build(
        vec![policy(None)],
        &credentials,
        &facts,
        NOW
    ))
    .is_empty());
}

#[test]
fn runner_reach_uses_resolved_account_membership_before_and_after_declaration() {
    let facts = vec![
        fact("vendor-a", "model", Some(&["oauth"])),
        fact("vendor-b", "model", Some(&["oauth"])),
    ];
    for (providers, serves, expected) in [
        (vec!["vendor-a"], vec!["vendor-b"], vec!["vendor-a/model"]),
        (vec![], vec!["vendor-a"], vec!["vendor-a/model"]),
        (vec![], vec!["vendor-a", "vendor-b"], vec![]),
    ] {
        let mut credentials = credentials(&facts, row(&providers, &serves, Some("oauth")), NOW);
        for declared in [false, true] {
            if declared {
                credentials.runner_declarations.insert(
                    "fixture-runner".into(),
                    declaration(json!([{"wire_api":"messages", "auth_method":"oauth"}])),
                );
            }
            assert_eq!(
                routes(Registry::build(
                    vec![policy(None)],
                    &credentials,
                    &facts,
                    NOW
                )),
                expected
            );
        }
        assert!(routes(Registry::build(
            vec![policy(None)],
            &credentials,
            &facts,
            NOW + 15 * 60 * 1000 + 1
        ))
        .is_empty());
    }
}

#[test]
fn standing_provider_limits_and_verified_catalog_replacement_are_enforced() {
    let slot = json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}],
        "standing_limits": {"explicit_providers": ["vendor-a"]}}});
    let entry = |verified| {
        json!({"module_id":"fixture-runner", "nonce_verified": verified,
        "role_declarations": {"routing/v1": slot}})
    };
    assert!(runners::declarations_from_catalog(&json!([entry(false)])).is_empty());
    let facts = vec![
        fact("vendor-a", "model", None),
        fact("vendor-b", "model", None),
    ];
    let mut credentials = credentials(
        &facts,
        row(&["vendor-a", "vendor-b"], &[], Some("oauth")),
        NOW,
    );
    credentials.runner_declarations = runners::declarations_from_catalog(&json!([entry(true)]));
    let registry = Registry::build(vec![policy(None)], &credentials, &facts, NOW);
    assert_eq!(
        registry.entries[0].policy.routes.as_ref().unwrap(),
        &["vendor-a/model"]
    );
    assert!(!registry.entries[0].follows_account_choice);
    credentials.runner_declarations = runners::declarations_from_catalog(&json!([entry(false)]));
    assert_eq!(
        routes(Registry::build(
            vec![policy(None)],
            &credentials,
            &facts,
            NOW
        )),
        ["vendor-a/model", "vendor-b/model"]
    );
}

#[test]
fn runner_declaration_validation_matches_the_shared_contract_shape() {
    let valid = json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}]}});
    assert!(
        runners::parse_declaration(&valid)
            .unwrap()
            .unwrap()
            .starts_sessions
    );
    assert!(
        !runners::parse_declaration(&valid)
            .unwrap()
            .unwrap()
            .follows_account_choice
    );
    for invalid in [
        json!({"runner": {"pairs": []}}),
        json!({"runner": {"pairs": [{"wire_api":"Messages", "auth_method":"oauth"}]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}, {"wire_api":"messages", "auth_method":"oauth"}]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "unknown": true}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth", "unknown": true}]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "tool_capabilities": ["test:cap_0/v1", "test:cap_0/v1"]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "standing_limits": {"explicit_providers": []}}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "standing_limits": {"unknown": true}}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "harnesses": ["Not A Token"]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "harnesses": ["pi", "pi"]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "harnesses": ["pi"], "default_harness": "other"}}),
        json!({"runner": {"pairs": vec![json!({"wire_api":"messages", "auth_method":"oauth"}); 65]}}),
        json!({"runner": {"pairs": [{"wire_api":"messages", "auth_method":"oauth"}], "tool_capabilities": (0..65).map(|n| format!("test:cap_{n}/v1")).collect::<Vec<_>>()}}),
        json!({"unknown": true}),
    ] {
        assert!(runners::parse_declaration(&invalid).is_err(), "{invalid}");
    }
}

#[tokio::test]
async fn select_then_account_for_keeps_resolved_membership_across_runner_declaration() {
    let _clock = crate::InjectedClock::set(NOW);
    let facts = vec![
        fact("anthropic", "runner-model", Some(&["oauth"])),
        fact("second-vendor", "model", Some(&["oauth"])),
    ];
    for (providers, serves, reachable) in [
        (vec!["anthropic"], vec![], true),
        (vec![], vec!["anthropic"], true),
        (vec![], vec!["anthropic", "second-vendor"], false),
    ] {
        let harness = Harness::new("runner-membership");
        harness.install_facts(&facts);
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":"fixture-runner", "via_vault":true, "routes":["anthropic/runner-model"]}]);
            config["consumers"]["mason"]["pin"] = json!({"model":"anthropic/runner-model"});
        });
        let row = row(&providers, &serves, Some("oauth"));
        let listing = ScopedCredentialListing {
            view: "runner-fixture".into(),
            credentials: vec![row.clone()],
            grant_tuples: vec![cortexkit_claustrum_client::ListedGrant {
                selector_kind: "category".into(),
                selector: "llm-provider".into(),
                operation: "list".into(),
            }],
            ..Default::default()
        };
        let mut state = crate::account_quota::AccountQuotaState::default();
        crate::account_quota::apply_roster_listing(&mut state, &listing, NOW as i64);
        crate::account_quota::apply_provider_reply(&mut state, "fixture-source", &serde_json::to_vec(&json!({
            "result": [{"provider":"fixture-source", "apiProvider":"anthropic", "account":"account-a", "source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(NOW as i64), "usage":{"primary":{"usedPercent":10.0,"windowMinutes":300,
                    "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 3_600_000)}}}], "completeProviders":["fixture-source"]
        })).unwrap(), NOW as i64).unwrap();
        *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
        let mut credentials = credentials(&facts, row, NOW);
        let caller = harness.bind(reserved("example-consumer"), None).await;
        let runner = harness.bind(reserved("fixture-runner"), None).await;
        for declared in [false, true] {
            if declared {
                credentials.runner_declarations.insert(
                    "fixture-runner".into(),
                    declaration(json!([{"wire_api":"messages", "auth_method":"oauth"}])),
                );
            }
            harness.set_credentials(credentials.clone());
            let selection = caller
                .call(
                    "route.select",
                    &json!({"consumer":"mason", "requirements":{"iq":0}}),
                )
                .await;
            let legacy = caller.call("route.select", &json!({"targetAgent":"mason", "consumer":"mason", "substrate":"broca", "requirements":{"iq":1,"eq":0}})).await;
            if !reachable {
                assert_eq!(selection.0, FrameType::Error, "{selection:?}");
                assert_eq!(legacy.0, FrameType::Error, "{legacy:?}");
                continue;
            }
            assert_eq!(selection.0, FrameType::Response, "{selection:?}");
            assert_eq!(legacy.0, FrameType::Response, "{legacy:?}");
            assert_eq!(legacy.1["result"]["runner"]["module"], "fixture-runner");
            assert!(legacy.1["result"]["runner"].get("harness").is_none());
            assert_eq!(selection.1["result"]["runner"]["module"], "fixture-runner");
            assert!(selection.1["result"]["runner"].get("harness").is_none());
            let admission = runner.call("route.account_for", &json!({
                "provider":"anthropic", "model":"runner-model", "conversation_key":format!("conversation-{declared}"),
                "admission_id":format!("admission-{declared}"), "decision_id":selection.1["result"]["decision_id"]
            })).await;
            assert_eq!(admission.0, FrameType::Response, "{admission:?}");
            assert_eq!(
                admission.1["result"]["account"]["credential_id"],
                "opaque-credential"
            );
            let describe = caller.call("role.describe", &json!({})).await;
            assert_eq!(
                describe.1["result"]["runners"][0]["module"],
                "fixture-runner"
            );
            assert_eq!(
                describe.1["result"]["runners"][0]["routes"],
                json!(["anthropic/runner-model"])
            );
        }
    }
}

#[tokio::test]
async fn consumer_ownership_never_grants_unscoped_account_operations() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("runner-admission");
    harness.edit_config(|config| {
        config["consumers"]["mason"]["owner"] = json!("consumer-owner");
        config["runners"] = json!([{"module":"fixture-runner"}]);
    });
    let mut credentials = BrocaCredentials::api_test_credentials(&[]);
    credentials.runner_declarations = runners::declarations_from_catalog(&json!([
        {"module_id":"consumer-declarer", "nonce_verified":true, "role_declarations":{"routing/v1":{"consumers":[{"name":"consumer-declarer.task"}]}}}
    ]));
    harness.set_credentials(credentials);
    for module in ["consumer-owner", "consumer-declarer"] {
        let caller = harness.bind(reserved(module), None).await;
        for decision in [None, Some("any-decision")] {
            for method in ["route.account_for", "route.account_observe"] {
                let mut params = json!({});
                if let Some(decision) = decision {
                    params["decision_id"] = json!(decision);
                }
                let reply = caller.call(method, &params).await;
                assert_eq!(reply.0, FrameType::Error);
                assert_eq!(reply.1["code"], "account_not_authorized", "{reply:?}");
            }
        }
    }
}

#[test]
fn variants_share_model_reach_but_never_bypass_operator_variant_rules() {
    let facts = vec![fact("vendor-a", "model", Some(&["oauth"]))];
    let mut credentials = credentials(&facts, row(&["vendor-a"], &[], Some("oauth")), NOW);
    for declared in [false, true] {
        if declared {
            credentials.runner_declarations.insert(
                "fixture-runner".into(),
                declaration(json!([{"wire_api":"messages", "auth_method":"oauth"}])),
            );
        }
        let registry = Registry::build(
            vec![policy(Some(vec!["vendor-a/model/high"]))],
            &credentials,
            &facts,
            NOW,
        );
        assert!(registry.runs("fixture-runner", "vendor-a/model/high", "vendor-a/model"));
        assert!(!registry.runs("fixture-runner", "vendor-a/model/low", "vendor-a/model"));
        assert!(!registry.runs("fixture-runner", "vendor-a/model", "vendor-a/model"));
    }
}

#[tokio::test]
async fn catalog_wire_api_reaches_selection_without_provider_name_inference() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("runner-transport");
    let facts = vec![fact("vendor-a", "model", Some(&["oauth"]))];
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"fixture-runner", "via_vault":true}])
    });
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for (version, api, expected) in [
        (1, "messages", FrameType::Response),
        (2, "different", FrameType::Error),
    ] {
        let mut credentials = credentials(&facts, row(&["vendor-a"], &[], Some("oauth")), NOW);
        credentials.wire_apis.clear();
        credentials.runner_declarations.insert(
            "fixture-runner".into(),
            declaration(json!([{"wire_api":"messages", "auth_method":"oauth"}])),
        );
        let shared = std::sync::Arc::new(std::sync::Mutex::new(credentials));
        let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
            "catalog_version":version, "resolved_at_ms":NOW,
            "models":{"vendor-a/model":{"existence":"present", "transport.wire_api":api, "limit.context":200_000}},
            "curated":[{"model":"vendor-a/model", "auth_methods":["oauth"]}]
        })).unwrap()).unwrap();
        crate::fusiform_catalog::apply_mapped_catalog(
            harness.store(),
            &snapshot,
            Some(&shared),
            None,
        )
        .unwrap();
        harness.set_credentials(shared.lock().unwrap().clone());
        let reply = caller
            .call(
                "route.select",
                &json!({"consumer":"mason", "requirements":{"iq":0}}),
            )
            .await;
        assert_eq!(reply.0, expected, "{reply:?}");
    }
}

#[tokio::test]
async fn requested_harness_filters_runners_and_echoes_only_a_hosted_program() {
    let harness = Harness::new("requested-program");
    let catalog = fixture::harness_catalog();
    install_catalog(&harness, &catalog);
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for program in ["alpha", "beta", "solo"] {
        let reply = caller
            .call(
                "route.select",
                &json!({
                    "consumer":"mason", "requirements":{"iq":0}, "harness":program
                }),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        let expected = if program == "solo" {
            "runner-two"
        } else {
            "runner-one"
        };
        assert_eq!(reply.1["result"]["runner"]["module"], expected);
        assert_eq!(reply.1["result"]["runner"]["harness"], program);
    }
    for extra in [
        json!({"harness":"unhosted"}),
        json!({"harness":"solo", "runners":["runner-one"]}),
    ] {
        let mut request = json!({"consumer":"mason", "requirements":{"iq":0}});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "no_route");
        assert_eq!(
            reply.1["detail"]["reason"], "harness_unavailable",
            "{reply:?}"
        );
    }
}

#[tokio::test]
async fn unrequested_harness_uses_declared_default_or_single_program() {
    let harness = Harness::new("default-program");
    let mut catalog = fixture::harness_catalog();
    catalog.runners[1].default_harness = Some("beta".into());
    install_catalog(&harness, &catalog);
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for (module, program) in [("runner-one", "beta"), ("runner-two", "solo")] {
        let reply = caller
            .call(
                "route.select",
                &json!({
                    "consumer":"mason", "requirements":{"iq":0}, "runners":[module]
                }),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        assert_eq!(reply.1["result"]["runner"]["harness"], program);
    }
}

#[tokio::test]
async fn multiple_programs_without_default_never_guess_the_first() {
    let harness = Harness::new("ambiguous-program");
    let mut catalog = fixture::catalog();
    catalog.runners[0].default_harness = None;
    install_catalog(&harness, &catalog);
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let reply = caller
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":0}, "runners":["runner-one"]
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["runner"]["module"], "runner-one");
    assert!(
        reply.1["result"]["runner"].get("harness").is_none(),
        "{reply:?}"
    );
}

#[tokio::test]
async fn non_starting_runner_never_starts_but_can_allocate_its_own_turn() {
    let harness = Harness::new("gateway-program");
    let catalog = fixture::harness_catalog();
    install_catalog(&harness, &catalog);
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for extra in [json!({}), json!({"harness":"alpha"})] {
        let mut request = json!({"consumer":"mason", "requirements":{"iq":0}});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        assert_eq!(reply.1["result"]["runner"]["module"], "runner-one");
        request["runners"] = json!(["runner-gateway"]);
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "no_route");
    }
    let runner = harness.bind(reserved("runner-gateway"), None).await;
    let model = catalog
        .models
        .iter()
        .find(|model| !model.priced_per_token)
        .unwrap();
    let reply = runner
        .call(
            "route.account_for",
            &json!({
                "provider":model.provider, "model":model.model,
                "conversation_key":"person-started", "admission_id":"person-turn"
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert!(reply.1["result"]["account"].is_object(), "{reply:?}");
}

#[tokio::test]
async fn harness_comes_only_from_the_selected_runners_verified_declaration() {
    let harness = Harness::new("program-provenance");
    let mut catalog = fixture::catalog();
    catalog.runners[0].harnesses = Some(vec!["alpha".into(), "beta".into()]);
    catalog.runners[0].default_harness = Some("beta".into());
    install_catalog(&harness, &catalog);
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let request = json!({"consumer":"mason", "requirements":{"iq":0}, "runners":["runner-one"]});
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["runner"]["harness"], "beta");

    // Reset credentials to remove the verified runner declarations while
    // keeping operator policy and models. Neither supplies a missing harness.
    let facts = harness.store().get_facts().unwrap();
    harness.set_credentials(BrocaCredentials::api_test_credentials(&facts));
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert!(
        reply.1["result"]["runner"].get("harness").is_none(),
        "{reply:?}"
    );
}

#[tokio::test]
async fn undeclared_production_runners_keep_the_same_selection_envelope() {
    let harness = Harness::new("legacy-program-absence");
    let facts = vec![fact("vendor-a", "model", Some(&["oauth"]))];
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"fixture-runner", "via_vault":true}]);
    });
    harness.set_credentials(credentials(
        &facts,
        row(&["vendor-a"], &[], Some("oauth")),
        crate::now_ms(),
    ));
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for request in [
        json!({"consumer":"mason", "requirements":{"iq":0}}),
        json!({"targetAgent":"mason", "consumer":"mason", "substrate":"broca", "requirements":{"iq":1,"eq":0}}),
    ] {
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        if request.get("targetAgent").is_none() {
            assert_eq!(
                reply.1["result"]["runner"],
                json!({
                    "module":"fixture-runner", "model":{"provider":"vendor-a", "model":"model"}
                })
            );
        } else {
            assert_eq!(reply.1["result"]["runner"]["module"], "fixture-runner");
            assert!(reply.1["result"]["runner"].get("harness").is_none());
        }
    }
    let reply = caller.call("role.describe", &json!({})).await;
    let runner = &reply.1["result"]["runners"][0];
    for field in ["harnesses", "default_harness", "starts_sessions"] {
        assert!(runner.get(field).is_none(), "{reply:?}");
    }
}
