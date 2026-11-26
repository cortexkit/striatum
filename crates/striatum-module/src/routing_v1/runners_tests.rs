//! Runner facts never expand operator routes or derive login methods.

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use striatum_core::{ModelFact, RouteStateStore};
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
                "anthropic.messages".into(),
            )
        })
        .collect();
    credentials
}

fn policy(routes: Option<Vec<&str>>) -> RunnerPolicy {
    RunnerPolicy {
        harness: None,
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

fn served_wire_example(name: &str) -> Value {
    let examples: Value =
        serde_json::from_str(include_str!("../../fixtures/runner-wire-families.json")).unwrap();
    examples[name].clone()
}

fn opus_wire_catalog() -> Value {
    let mut catalog = served_wire_example("catalog.get — one model, current");
    // The upstream unfiltered example is Sonnet. Re-key its served entry to
    // Opus to exercise THALAMUS's model choice without inventing a wire fact.
    let entry = catalog["models"]
        .as_object_mut()
        .unwrap()
        .remove("anthropic/claude-sonnet-4-5")
        .unwrap();
    catalog["models"]["anthropic/claude-opus-5-5"] = entry;
    catalog
}

#[tokio::test]
async fn thalamus_opencode_selects_anthropic_opus_through_oauth() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("thalamus-opus-wire");
    let facts = vec![fact("anthropic", "claude-opus-5-5", Some(&["oauth"]))];
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"thalamus", "via_vault":true}]);
    });
    let mut credentials = credentials(&facts, row(&["anthropic"], &[], Some("oauth")), NOW);
    credentials.wire_apis.clear();
    credentials.runner_declarations.insert(
        "thalamus".into(),
        runners::parse_declaration(&json!({
            "runner": {"pairs":[{"wire_api":"anthropic.messages", "auth_method":"oauth"}],
            "harnesses":["opencode"], "default_harness":"opencode", "follows_account_choice":true}
        }))
        .unwrap()
        .unwrap(),
    );
    let shared = std::sync::Arc::new(std::sync::Mutex::new(credentials));
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&opus_wire_catalog()).unwrap(),
    )
    .unwrap();
    crate::fusiform_catalog::apply_mapped_catalog(harness.store(), &snapshot, Some(&shared), None)
        .unwrap();
    harness.set_credentials(shared.lock().unwrap().clone());
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let reply = caller.call("route.select", &json!({"consumer":"mason", "requirements":{"iq":0}, "harness":"opencode", "detail":true})).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["route"]["provider"], "anthropic");
    assert_eq!(reply.1["result"]["route"]["model"], "claude-opus-5-5");
    assert_eq!(reply.1["result"]["runner"]["module"], "thalamus");
    assert_eq!(reply.1["result"]["runner"]["harness"], "opencode");
}

#[tokio::test]
async fn broca_without_pairs_keeps_pick_and_rejected_bytes_when_wire_facts_change() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("broca-wire-baseline");
    for seeded in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&seeded).route_key)
            .unwrap();
    }
    let facts = vec![
        fact("anthropic", "claude-opus-5-5", None),
        fact("anthropic", "claude-sonnet-4-5", None),
    ];
    harness.install_facts(&facts);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca", "via_vault":true}]));
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let mut baseline = None;
    for wire in [None, Some("anthropic.messages"), Some("future.wire")] {
        let mut credentials = credentials(&facts, row(&["anthropic"], &[], Some("oauth")), NOW);
        credentials.wire_apis.clear();
        if let Some(wire) = wire {
            for fact in &facts {
                credentials.wire_apis.insert(
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    wire.into(),
                );
            }
        }
        assert!(credentials.runner_declarations.is_empty());
        harness.set_credentials(credentials);
        let reply = caller.call("route.select", &json!({"consumer":"mason", "requirements":{"iq":0}, "exclude":["anthropic/claude-sonnet-4-5"], "detail":true})).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        let result = &reply.1["result"];
        assert_eq!(result["route"]["provider"], "anthropic");
        assert_eq!(result["route"]["model"], "claude-opus-5-5");
        assert_eq!(result["runner"]["module"], "broca");
        assert_eq!(
            result["detail"]["rejected"],
            json!([
                {"routeKey":"anthropic/claude-sonnet-4-5", "reason":"excluded"}
            ])
        );
        let bytes = serde_json::to_vec(&json!({"route":result["route"], "runner":result["runner"], "rejected":result["detail"]["rejected"]})).unwrap();
        if let Some(baseline) = &baseline {
            assert_eq!(
                &bytes, baseline,
                "wire fact changed Broca's pick or rejection list"
            );
        } else {
            baseline = Some(bytes);
        }
    }
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
        {"wire_api": "anthropic.messages", "auth_method": "oauth"}, {"wire_api": "other", "auth_method": "apikey"}
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
        declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"oauth"}])),
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
                    declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"oauth"}])),
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
    let slot = json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}],
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
    let valid =
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}]}});
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
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}, {"wire_api":"anthropic.messages", "auth_method":"oauth"}]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "unknown": true}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth", "unknown": true}]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "tool_capabilities": ["test:cap_0/v1", "test:cap_0/v1"]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "standing_limits": {"explicit_providers": []}}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "standing_limits": {"unknown": true}}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "harnesses": ["Not A Token"]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "harnesses": ["pi", "pi"]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "harnesses": ["pi"], "default_harness": "other"}}),
        json!({"runner": {"pairs": vec![json!({"wire_api":"anthropic.messages", "auth_method":"oauth"}); 65]}}),
        json!({"runner": {"pairs": [{"wire_api":"anthropic.messages", "auth_method":"oauth"}], "tool_capabilities": (0..65).map(|n| format!("test:cap_{n}/v1")).collect::<Vec<_>>()}}),
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
                    declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"oauth"}])),
                );
            }
            harness.set_credentials(credentials.clone());
            let selection = caller
                .call(
                    "route.select",
                    &json!({"consumer":"mason", "requirements":{"iq":0}}),
                )
                .await;
            if !reachable {
                assert_eq!(selection.0, FrameType::Error, "{selection:?}");
                continue;
            }
            assert_eq!(selection.0, FrameType::Response, "{selection:?}");
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
                declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"oauth"}])),
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
        (1, "anthropic.messages", FrameType::Response),
        (2, "different", FrameType::Error),
    ] {
        let mut credentials = credentials(&facts, row(&["vendor-a"], &[], Some("oauth")), NOW);
        credentials.wire_apis.clear();
        credentials.runner_declarations.insert(
            "fixture-runner".into(),
            declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"oauth"}])),
        );
        let shared = std::sync::Arc::new(std::sync::Mutex::new(credentials));
        let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
            "catalog_version":version, "resolved_at_ms":NOW,
            "models":{"vendor-a/model":{"existence":"present", "wire.family":api, "limit.context":200_000}},
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
async fn ambiguous_legacy_program_is_unnamed_until_decision_harness_then_uses_declared_order() {
    super::describe::with_candidate_capabilities(
        &[cortexkit_role_routing::capabilities::DECISION_HARNESS],
        async {
            let harness = Harness::new("ambiguous-program");
            let mut catalog = fixture::catalog();
            catalog.runners[0].default_harness = None;
            install_catalog(&harness, &catalog);
            let caller = harness.bind(reserved("example-consumer"), None).await;
            // Only decision_harness binds the first admitting declared harness when the runner has no default.
            let params = json!({
                "consumer":"mason", "requirements":{"iq":0}, "runners":["runner-one"]
            });
            let reply = super::describe::without_capability(
                cortexkit_role_routing::capabilities::DECISION_HARNESS,
                caller.call("route.select", &params),
            )
            .await;
            assert_eq!(reply.0, FrameType::Response, "{reply:?}");
            assert_eq!(reply.1["result"]["runner"]["module"], "runner-one");
            assert!(
                reply.1["result"]["runner"].get("harness").is_none(),
                "{reply:?}"
            );
            let bound = caller.call("route.select", &params).await;
            assert_eq!(bound.0, FrameType::Response, "{bound:?}");
            assert_eq!(bound.1["result"]["runner"]["module"], "runner-one");
            assert_eq!(bound.1["result"]["runner"]["harness"], "alpha");
            assert_eq!(bound.1["result"]["route"], reply.1["result"]["route"]);
        },
    )
    .await;
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

#[test]
fn broca_contract_0748_routing_tier_discovery() {
    for bytes in [
        include_bytes!("../../fixtures/contracts/broca-contract-0748-v1/role-describe/role_describe_reader.json").as_slice(),
        include_bytes!("../../fixtures/contracts/broca-contract-0748-v1/role-describe/role_describe.json").as_slice(),
        include_bytes!("../../fixtures/contracts/broca-contract-0748-v1/role-describe/role_describe.head_additions.json").as_slice(),
    ] {
        let raw: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        let describe = raw.get("result").unwrap_or(&raw);
        assert!(describe["capabilities"].as_array().unwrap().contains(&serde_json::json!("finish_reason")));
        assert!(super::runners::service_tiers_from_describe(&raw).is_none());
        assert!(super::runners::service_tiers_from_describe(describe).is_none());
    }
}

#[test]
fn routed_account_discovery_requires_the_group_and_valid_unique_methods() {
    assert_eq!(
        runners::routed_accounts_from_describe(&json!({
            "result":{"capabilities":["routed_accounts"], "routed_accounts":["apikey"]}
        })),
        Some(vec!["apikey".into()])
    );
    for describe in [
        json!({"capabilities":[], "routed_accounts":["apikey"]}),
        json!({"capabilities":["routed_accounts"]}),
        json!({"capabilities":["routed_accounts"], "routed_accounts":[]}),
        json!({"capabilities":["routed_accounts"], "routed_accounts":["apikey","apikey"]}),
        json!({"capabilities":["routed_accounts"], "routed_accounts":["bad method"]}),
    ] {
        assert!(runners::routed_accounts_from_describe(&describe).is_none());
    }
}

#[test]
fn routed_account_methods_require_follows_account_choice() {
    let facts = [fact("anthropic", "model", Some(&["apikey"]))];
    let mut credentials = credentials(&facts, row(&["anthropic"], &[], Some("apikey")), NOW);
    credentials
        .runner_routed_accounts
        .insert("fixture-runner".into(), vec!["apikey".into()]);
    let registry = Registry::build(vec![policy(None)], &credentials, &facts, NOW);
    assert!(registry
        .get("fixture-runner")
        .unwrap()
        .routed_accounts
        .is_empty());
    for follows in [false, true] {
        let mut declared =
            declaration(json!([{"wire_api":"anthropic.messages", "auth_method":"apikey"}]));
        declared.follows_account_choice = follows;
        credentials
            .runner_declarations
            .insert("fixture-runner".into(), declared);
        let registry = Registry::build(vec![policy(None)], &credentials, &facts, NOW);
        assert_eq!(
            registry
                .get("fixture-runner")
                .unwrap()
                .routed_accounts
                .contains(&"apikey".into()),
            follows
        );
    }
}

#[test]
fn broca_head_selection_v2_routing_tier_discovery() {
    let expected = serde_json::json!({
        "anthropic.messages": {"levels": ["fast"], "served": "reported"},
        "google.generate_content": {"levels": ["fast"], "served": "reported"},
        "openai.chat_completions": {"levels": ["fast", "ultrafast"], "served": "reported"},
        "openai.chatgpt_responses": {"levels": ["fast", "ultrafast"], "served": "reported"},
        "openai.responses": {"levels": ["fast", "ultrafast"], "served": "reported"}
    });
    for bytes in [
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/contracts/broca-head-selection-v2/before/role.describe.json"
        ))
        .as_slice(),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/contracts/broca-head-selection-v2/before-routed/role.describe.json"
        ))
        .as_slice(),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/contracts/broca-head-selection-v2/after/role.describe.json"
        ))
        .as_slice(),
    ] {
        let raw: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        for reply in [&raw, &raw["result"]] {
            let tiers = super::runners::service_tiers_from_describe(reply)
                .expect("served role.describe must retain service-tier discovery");
            assert_eq!(serde_json::to_value(tiers).unwrap(), expected);
        }
    }
}

#[test]
fn broca_account_reach_uses_raw_vault_identity_but_api_keys_keep_their_fallback() {
    let facts = vec![fact("xai", "grok-4.7", Some(&["oauth", "apikey"]))];
    for method in ["oauth", "apikey"] {
        let mut missing = row(&["xai"], &[], Some(method));
        missing.id = format!("{method}:xai");
        missing.account_id = None;
        let credentials = credentials(&facts, missing.clone(), NOW);
        for module in ["broca", "other-runner"] {
            let mut runner = policy(None);
            runner.module = module.into();
            let registry = Registry::build(vec![runner], &credentials, &facts, NOW);
            let expected = method == "apikey" || module != "broca";
            assert_eq!(
                registry.runs(module, "xai/grok-4.7", "xai/grok-4.7"),
                expected
            );
            assert_eq!(
                registry.no_runner_account(module, "xai/grok-4.7", "xai/grok-4.7"),
                !expected
            );
        }
        let mut known = missing;
        known.id = "second-record".into();
        known.account_id = Some("provider-account".into());
        let mut credentials = credentials.clone();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "healthy-sibling".into(),
                credentials: vec![credentials.listed_credentials()[0].clone(), known],
                ..Default::default()
            },
            NOW,
        );
        let mut runner = policy(None);
        runner.module = "broca".into();
        let registry = Registry::build(vec![runner], &credentials, &facts, NOW);
        assert!(registry.runs("broca", "xai/grok-4.7", "xai/grok-4.7"));
        assert!(!registry.no_runner_account("broca", "xai/grok-4.7", "xai/grok-4.7"));
    }
}

#[tokio::test]
async fn broca_missing_provider_account_is_rejected_on_select_despite_insula_linkage() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("broca-no-runner-account");
    for seeded in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&seeded).route_key)
            .unwrap();
    }
    let facts = vec![fact("xai", "grok-4.7", Some(&["oauth"]))];
    harness.install_facts(&facts);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca", "via_vault":true}]));
    let mut missing = row(&["xai"], &[], Some("oauth"));
    missing.id = "oauth:xai".into();
    missing.account_id = None;
    harness.set_credentials(credentials(&facts, missing, NOW));
    let mut quota = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_usage_reply(
        &mut quota,
        &serde_json::to_vec(&json!({
            "result":[{"provider":"grok", "apiProvider":"xai", "credentialRef":"oauth:xai",
                "account":"819c167d-1ebc-4945-a7b9-7197a98a207e", "source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                "usage":{"primary":{"usedPercent":0.0,"windowMinutes":10080,
                    "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 2 * 86_400_000)}}}]
        }))
        .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for request in [json!({"consumer":"mason", "requirements":{"iq":0}, "detail":true})] {
        let reply = caller.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert!(
            reply.1.to_string().contains("no_runner_account"),
            "{reply:?}"
        );
    }
}

#[tokio::test]
async fn pin_availability_unknown_cause_is_untimed_and_keeps_its_detail() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("pin-no-runner-account");
    let facts = vec![fact("xai", "grok-4.7", Some(&["oauth"]))];
    harness.install_facts(&facts);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca", "via_vault":true}]));
    let mut missing = row(&["xai"], &[], Some("oauth"));
    missing.id = "oauth:xai".into();
    missing.account_id = None;
    harness.set_credentials(credentials(&facts, missing, NOW));
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let reply = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","pin":"xai/grok-4.7",
        "requirements":{"iq":0}}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    let refusal: cortexkit_role_routing::errors::Refusal = serde_json::from_value(reply.1).unwrap();
    assert_eq!(refusal.code.as_str(), "pin_unavailable", "{refusal:?}");
    assert_eq!(refusal.detail["reason"], "not_available", "{refusal:?}");
    assert_eq!(refusal.wait_until_ms(), None, "{refusal:?}");
    assert!(!refusal.detail.contains_key("reset_at_ms"), "{refusal:?}");
    assert!(refusal.message.contains("no_runner_account"), "{refusal:?}");
}
