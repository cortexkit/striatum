use std::{collections::BTreeMap, sync::Arc};

use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use serde_json::json;
use striatum_core::{
    billing::{BillingPlane, BillingRate},
    RouteStateStore,
};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness};
use crate::{broca_credentials::BrocaCredentials, fusiform_catalog::PlaneCatalog};

const GATEWAY: &str = "anthropic-gw-acme";
const GATEWAY_ROUTE: &str = "anthropic-gw-acme/claude";
const DIRECT_ROUTE: &str = "anthropic-platform/claude";
const RUNNER: &str = "thalamus-like";

#[test]
fn gateway_is_not_an_implicit_fallback_for_an_exhausted_subscription() {
    for method in ["apikey", "gateway"] {
        let rows = ["oauth", method]
            .into_iter()
            .map(|auth| ListedCredential {
                id: auth.into(),
                state: "active".into(),
                categories: vec![if auth == "gateway" {
                    "llm-gateway"
                } else {
                    "llm-provider"
                }
                .into()],
                provider_ids: vec!["anthropic".into()],
                credential_type: auth.into(),
                auth_method: Some(auth.into()),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let fact = crate::tests::fact("anthropic", "claude");
        let catalog = [&fact].into_iter().collect();
        for fallback in [false, true] {
            let class = crate::account_selection::resolve_class(
                &rows,
                &fact,
                &catalog,
                &Default::default(),
                &[],
                false,
                false,
                fallback,
                |row| row.auth_method.as_deref() == Some(method),
            );
            assert_eq!(class.subscription, !fallback);
            assert_eq!(class.rows.len(), 1);
            assert_eq!(
                class.rows[0].auth_method.as_deref(),
                Some(if fallback { method } else { "oauth" })
            );
        }
        assert!(super::runners::can_offer_account(RUNNER, true, &rows[1]));
    }
}

#[test]
fn explicit_metered_account_class_accepts_gateways_but_not_subscriptions() {
    let facts = crate::tests::fact("anthropic", "claude");
    let catalog = [&facts].into_iter().collect();
    let rows: Vec<_> = ["oauth", "gateway", "apikey"]
        .into_iter()
        .map(|method| ListedCredential {
            id: method.into(),
            state: "active".into(),
            categories: vec![if method == "gateway" {
                "llm-gateway"
            } else {
                "llm-provider"
            }
            .into()],
            provider_ids: vec!["anthropic".into()],
            credential_type: method.into(),
            auth_method: Some(method.into()),
            ..Default::default()
        })
        .collect();
    let private = crate::account_selection::resolve_class(
        &rows,
        &facts,
        &catalog,
        &Default::default(),
        &[],
        false,
        true,
        true,
        |_| true,
    );
    assert!(!private.subscription);
    assert_eq!(
        private
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        ["gateway", "apikey"]
    );
    let gateway_only = crate::account_selection::resolve_class(
        &rows[1..2],
        &facts,
        &catalog,
        &Default::default(),
        &[],
        false,
        true,
        true,
        |_| true,
    );
    assert_eq!(gateway_only.rows.len(), 1);
}

#[test]
fn ordinary_consumers_do_not_reject_gateway_billing() {
    use crate::select_policy::{candidate_rejections, Consumer};

    let no_subscription_plans = Default::default();
    for method in ["apikey", "gateway"] {
        let mut fact = crate::tests::fact("anthropic", "claude");
        fact.auth_methods = Some(vec![method.into()]);
        fact.billing_plane = Some(BillingPlane {
            provider_id: "anthropic".into(),
            auth_method: method.into(),
            kind: "upstream".into(),
            selection_reason: "listed credential".into(),
            rates: BTreeMap::from([("rate.output".into(), BillingRate::Priced { rate: 4.5 })]),
        });
        assert!(fact.billing_plane.as_ref().unwrap().is_api_metered());
        for consumer in [Consumer::Codemode, Consumer::Flow] {
            let rejected = candidate_rejections(
                &[fact.clone()],
                Some(consumer),
                None,
                Some(&no_subscription_plans),
            );
            assert!(rejected.is_empty());
        }
    }
}

fn fixture() -> Arc<Harness> {
    fixture_for_catalog("anthropic", "claude")
}

fn fixture_for_catalog(served_provider: &str, model: &str) -> Arc<Harness> {
    let harness = Harness::new("company-gateway");
    for fact in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&fact).route_key)
            .unwrap();
    }
    let facts = vec![crate::tests::fact(served_provider, model)];
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca"}, {"module":RUNNER}]);
        config["providers"][GATEWAY]["pin_only"] = json!(true);
        config["providers"]["anthropic-platform"]["pin_only"] = json!(true);
    });
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "gateway-and-direct".into(),
            credentials: [
                ("company-gateway", GATEWAY, "gateway", "llm-gateway"),
                (
                    "direct-api-key",
                    "anthropic-platform",
                    "apikey",
                    "llm-provider",
                ),
            ]
            .into_iter()
            .map(|(id, provider, method, category)| ListedCredential {
                id: id.into(),
                categories: vec![category.into()],
                provider_ids: vec![provider.into()],
                serves: vec![served_provider.into()],
                auth_method: Some(method.into()),
                credential_type: "apikey".into(),
                state: "active".into(),
                ..Default::default()
            })
            .collect(),
            grant_tuples: ["llm-provider", "llm-gateway"]
                .into_iter()
                .map(|category| ListedGrant {
                    selector_kind: "category".into(),
                    selector: category.into(),
                    operation: "list".into(),
                })
                .collect(),
            ..Default::default()
        },
        crate::now_ms(),
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, crate::now_ms() as i64, &[]);
    let billing: Vec<_> = ["apikey", "gateway"]
        .into_iter()
        .map(|method| {
            let mut fact = facts[0].clone();
            fact.billing_plane = Some(BillingPlane {
                provider_id: served_provider.into(),
                auth_method: method.into(),
                kind: "upstream".into(),
                selection_reason: "listed credential".into(),
                rates: BTreeMap::from([("rate.output".into(), BillingRate::Priced { rate: 4.5 })]),
            });
            fact
        })
        .collect();
    credentials.wire_apis.insert(
        format!("{served_provider}/{model}"),
        "anthropic.messages".into(),
    );
    credentials.publish_billing_catalog(
        PlaneCatalog::ordinary_test_catalog(&billing).with_wire_apis(&credentials.wire_apis),
    );
    credentials.catalog_snapshot = Some(crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41, "resolved_at_ms":123,
            "models":{format!("{served_provider}/{model}"):{"existence":"present", "model.type":"chat", "capability.tool_call":true}}}))
            .unwrap()).unwrap());
    credentials.runner_declarations = super::runners::declarations_from_catalog(&json!([
        {"module_id":"broca", "nonce_verified":true, "role_declarations":{"routing/v1":{"runner":{
            "follows_account_choice":true,
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"}],
            "harnesses":["broca"]
        }}}},
        {"module_id":RUNNER, "nonce_verified":true, "role_declarations":{"routing/v1":{"runner":{
            "follows_account_choice":true,
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":"gateway", "requires_wire_api":false},
                {"wire_api":"anthropic.messages", "auth_method":"apikey"}],
            "harnesses":["claude-code", "direct-api"], "default_harness":"claude-code",
            "harness_limits":{
                "claude-code":{"pairs":[{"wire_api":"anthropic.messages", "auth_method":"gateway"}],
                    "explicit_providers":[served_provider]},
                "direct-api":{"pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"}],
                    "explicit_providers":["anthropic-platform"]}
            }
        }}}}
    ]));
    assert_eq!(credentials.runner_declarations.len(), 2);
    assert!(!credentials.row_serves_wire_api(&facts[0], "gateway"));
    harness.set_credentials(credentials);
    harness
}

#[tokio::test]
async fn gateway_reach_is_runner_and_harness_local_on_select_choices_and_what_if() {
    let harness = fixture();
    let consumer = harness.bind(reserved("example-consumer"), None).await;
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    for (module, harness_name, expected) in [
        ("broca", "broca", None),
        (RUNNER, "claude-code", Some(GATEWAY_ROUTE)),
        (RUNNER, "direct-api", None),
    ] {
        let (ty, choices) = operator
            .call(
                "route.head_choices",
                &json!({
                    "runner_module":module, "harness":harness_name
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Response, "{choices}");
        let models = choices["result"]["models"].as_array().unwrap();
        if let Some(expected) = expected {
            assert_eq!(models.len(), 1, "{choices}");
            assert_eq!(
                format!(
                    "{}/{}",
                    models[0]["provider"].as_str().unwrap(),
                    models[0]["model"].as_str().unwrap()
                ),
                expected
            );
            assert_eq!(models[0]["billing"], "per_token", "{choices}");
            assert_eq!(models[0]["pin_only"], true, "{choices}");
        } else {
            // API-key pools are not model-plan provider ids, unlike gateways.
            assert!(models.is_empty(), "{choices}");
        }

        let (ty, preview) = operator
            .call(
                "route.what_if",
                &json!({
                    "consumer":"mason", "iqs":[0],
                    "head_choices":{"runner_module":module, "harness":harness_name}
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Response, "{preview}");
        assert_eq!(
            preview["result"]["head_choices"]["models"], choices["result"]["models"],
            "{preview}"
        );
    }
    for (module, harness_name) in [("broca", "broca"), (RUNNER, "direct-api")] {
        let reply = consumer
            .call(
                "route.select",
                &json!({
                    "consumer":"mason", "requirements":{"iq":0}, "pin":GATEWAY_ROUTE,
                    "runners":[module], "harness":harness_name
                }),
            )
            .await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    }
    let direct = consumer
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":0}, "pin":DIRECT_ROUTE,
                "runners":[RUNNER], "harness":"claude-code"
            }),
        )
        .await;
    assert_eq!(direct.0, FrameType::Error, "{direct:?}");
    let automatic = consumer.call("route.select", &json!({
        "consumer":"mason", "requirements":{"iq":0}, "runners":[RUNNER], "harness":"claude-code"
    })).await;
    assert_eq!(automatic.0, FrameType::Error, "{automatic:?}");
}

#[tokio::test]
async fn head_choices_show_gateway_pool_but_hide_api_key_pool_on_mixed_runner() {
    let harness = fixture();
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let (ty, choices) = operator
        .call("route.head_choices", &json!({"runner_module":RUNNER}))
        .await;
    assert_eq!(ty, FrameType::Response, "{choices}");
    let models = choices["result"]["models"].as_array().unwrap();
    assert_eq!(models.len(), 1, "{choices}");
    assert_eq!(models[0]["provider"], GATEWAY, "{choices}");
    assert_eq!(models[0]["model"], "claude", "{choices}");
    assert_eq!(models[0]["billing"], "per_token", "{choices}");
    assert_eq!(models[0]["pin_only"], true, "{choices}");
}

#[tokio::test]
async fn head_choices_hide_gateway_pool_without_a_runner_declaration() {
    let harness = fixture();
    harness.edit_config(|config| {
        config["runners"].as_array_mut().unwrap().push(json!({
            "module":"legacy-gateway", "via_vault":true
        }));
    });
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let (ty, choices) = operator
        .call(
            "route.head_choices",
            &json!({"runner_module":"legacy-gateway"}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{choices}");
    assert_eq!(choices["result"]["models"], json!([]), "{choices}");
}

#[tokio::test]
async fn codemode_and_flow_accept_pinned_or_operator_opted_in_gateways() {
    let harness = fixture();
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for consumer in ["codemode", "flow"] {
        let request = json!({
            "consumer":consumer, "requirements":{"iq":0}, "detail":true,
            "runners":[RUNNER], "harness":"claude-code"
        });
        let refused = caller.call("route.select", &request).await;
        assert_eq!(refused.0, FrameType::Error, "{refused:?}");
        let mut pinned = request.clone();
        pinned["pin"] = json!(GATEWAY_ROUTE);
        let selected = caller.call("route.select", &pinned).await;
        assert_eq!(selected.0, FrameType::Response, "{consumer}: {selected:?}");
        assert_eq!(selected.1["result"]["route"]["provider"], GATEWAY);
    }
    harness.edit_config(|config| config["providers"][GATEWAY]["pin_only"] = json!(false));
    for consumer in ["codemode", "flow"] {
        let selected = caller.call("route.select", &json!({
            "consumer":consumer, "requirements":{"iq":0}, "runners":[RUNNER], "harness":"claude-code"
        })).await;
        assert_eq!(selected.0, FrameType::Response, "{consumer}: {selected:?}");
        assert_eq!(selected.1["result"]["route"]["provider"], GATEWAY);
    }
}

#[tokio::test]
async fn gateway_decision_admits_only_its_gateway_account_without_subscription_pressure() {
    let harness = fixture();
    let consumer = harness.bind(reserved("example-consumer"), None).await;
    let selected = consumer
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":0}, "pin":GATEWAY_ROUTE,
                "runners":[RUNNER], "harness":"claude-code"
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    let result = &selected.1["result"];
    assert_eq!(result["route"]["provider"], GATEWAY, "{result}");
    assert_eq!(result["runner"]["model"]["provider"], GATEWAY, "{result}");
    assert_eq!(result["runner"]["harness"], "claude-code", "{result}");
    assert!(result["runner"].get("auth").is_none(), "{result}");
    let request = json!({
        "provider":GATEWAY, "model":"claude", "decision_id":result["decision_id"],
        "conversation_key":"gateway-turn", "admission_id":"gateway-admission"
    });
    let runner = harness.bind(reserved(RUNNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let wrong_runner = broca.call("route.account_for", &request).await;
    assert_eq!(wrong_runner.0, FrameType::Error, "{wrong_runner:?}");
    assert_eq!(
        wrong_runner.1["code"], "unknown_decision",
        "{wrong_runner:?}"
    );
    let mut wrong_route = request.clone();
    wrong_route["provider"] = json!("anthropic-platform");
    let refused = runner.call("route.account_for", &wrong_route).await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    assert_eq!(refused.1["code"], "unknown_decision", "{refused:?}");
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{admitted:?}");
    assert_eq!(
        admitted.1["result"]["account"]["credential_id"], "company-gateway",
        "{admitted:?}"
    );
    assert_eq!(
        admitted.1["result"]["account"]["method"], "gateway",
        "{admitted:?}"
    );
    let shared = harness.credentials();
    let credentials = shared.lock().unwrap();
    let mut facts = harness.store().get_facts().unwrap();
    let targets = credentials.catalog_targets(&facts);
    striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
    credentials.price_facts(&mut facts, &Default::default());
    let gateway = facts
        .iter()
        .find(|fact| fact.provider_id == GATEWAY)
        .unwrap();
    assert!(gateway.billing_plane.as_ref().unwrap().is_api_metered());
    assert_eq!(gateway.subscription_multiplier, None);
}

fn set_gateway_limit(harness: &Harness, providers: &[&str], per_harness: bool) {
    let mut runner = json!({
        "pairs":[{"wire_api":"anthropic.messages", "auth_method":"gateway"}],
        "follows_account_choice":true,
        "harnesses":["claude-code", "other"], "default_harness":"claude-code"
    });
    if per_harness {
        runner["harness_limits"] = json!({"claude-code":{"explicit_providers":providers}});
    } else {
        runner["standing_limits"] = json!({"explicit_providers":providers});
    }
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(
            RUNNER.into(),
            super::runners::parse_declaration(&json!({"runner":runner}))
                .unwrap()
                .unwrap(),
        );
}

async fn check_gateway_provider_limits(operation: &str) {
    let model = "claude-opus-5-5";
    let route = format!("{GATEWAY}/{model}");
    for served_provider in ["anthropic", "openai"] {
        for per_harness in [false, true] {
            for providers in [
                vec!["anthropic"],
                vec!["anthropic", GATEWAY],
                vec![GATEWAY],
                vec!["anthropic", "openai"],
            ] {
                let harness = fixture_for_catalog(served_provider, model);
                let allowed = providers.contains(&served_provider);
                let context = format!("{operation}: serves={served_provider}, per_harness={per_harness}, providers={providers:?}");
                let consumer = harness.bind(reserved("example-consumer"), None).await;
                let request = json!({
                    "consumer":"mason", "requirements":{"iq":0}, "pin":route,
                    "runners":[RUNNER], "harness":"claude-code"
                });
                set_gateway_limit(&harness, &providers, per_harness);
                let expected = if allowed {
                    FrameType::Response
                } else {
                    FrameType::Error
                };
                // Admission keeps the route and runner bound by selection. Use
                // the same policy throughout and verify the gateway's own pool.
                let decision = if operation == "route.account_for" {
                    let selected = consumer.call("route.select", &request).await;
                    assert_eq!(selected.0, expected, "{context}: {selected:?}");
                    if !allowed {
                        continue;
                    }
                    Some(selected.1["result"]["decision_id"].clone())
                } else {
                    None
                };
                if operation == "route.head_choices" {
                    let operator = harness.bind(json!({"kind":"direct"}), None).await;
                    let choices = operator
                        .call(
                            operation,
                            &json!({
                                "runner_module":RUNNER, "harness":"claude-code"
                            }),
                        )
                        .await;
                    assert_eq!(choices.0, FrameType::Response, "{context}: {choices:?}");
                    let models = choices.1["result"]["models"].as_array().unwrap();
                    assert_eq!(models.len(), usize::from(allowed), "{context}: {choices:?}");
                    if allowed {
                        assert_eq!(models[0]["provider"], GATEWAY, "{context}");
                        assert_eq!(models[0]["model"], model, "{context}");
                    }
                    continue;
                }
                let reply =
                    if let Some(decision) = decision {
                        let runner = harness.bind(reserved(RUNNER), None).await;
                        runner.call(operation, &json!({
                        "provider":GATEWAY, "model":model, "decision_id":decision,
                        "conversation_key":"provider-limit", "admission_id":"provider-limit"
                    })).await
                    } else {
                        consumer.call(operation, &request).await
                    };
                assert_eq!(reply.0, expected, "{context}: {reply:?}");
                if allowed && operation == "route.account_for" {
                    assert_eq!(
                        reply.1["result"]["account"]["provider"], GATEWAY,
                        "{context}"
                    );
                    assert_eq!(
                        reply.1["result"]["account"]["credential_id"], "company-gateway",
                        "{context}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn gateway_provider_limits_selection() {
    check_gateway_provider_limits("route.select").await;
}

#[tokio::test]
async fn gateway_provider_limits_head_choices() {
    check_gateway_provider_limits("route.head_choices").await;
}

#[tokio::test]
async fn gateway_provider_limits_account_admission() {
    check_gateway_provider_limits("route.account_for").await;
}

async fn check_provider_limit_pin_refusal(method: &str, absent_model: bool) {
    let provider = if method == "gateway" {
        GATEWAY
    } else {
        "anthropic"
    };
    for (per_harness, allowed) in [(false, true), (false, false), (true, true), (true, false)] {
        let harness = fixture_for_catalog("anthropic", "claude-opus-5-5");
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":RUNNER}]);
        });
        {
            let shared = harness.credentials();
            let mut credentials = shared.lock().unwrap();
            let mut row = credentials
                .listed_credentials()
                .iter()
                .find(|row| row.auth_method.as_deref() == Some(method))
                .unwrap()
                .clone();
            row.provider_ids = vec![provider.into()];
            credentials.apply_listing(
                &ScopedCredentialListing {
                    credentials: vec![row],
                    view: "single-method".into(),
                    ..Default::default()
                },
                crate::now_ms(),
            );
            credentials.refresh_serving_methods(
                &Default::default(),
                &harness.store().get_facts().unwrap(),
                crate::now_ms() as i64,
                &[],
            );
        }
        let providers = if allowed {
            vec!["anthropic"]
        } else {
            vec!["openai"]
        };
        let mut runner = json!({
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":method}],
            "follows_account_choice":true,
            "harnesses":["alpha"], "default_harness":"alpha"
        });
        if per_harness {
            runner["harness_limits"] = json!({"alpha":{"explicit_providers":providers}});
        } else {
            runner["standing_limits"] = json!({"explicit_providers":providers});
        }
        harness.credentials().lock().unwrap().runner_declarations =
            super::runners::declarations_from_catalog(&json!([{
                "module_id":RUNNER, "nonce_verified":true,
                "role_declarations":{"routing/v1":{"runner":runner}}
            }]));
        let consumer = harness.bind(reserved("example-consumer"), None).await;
        let model = if absent_model && !allowed {
            "unserved-model"
        } else {
            "claude-opus-5-5"
        };
        let request = json!({
            "consumer":"mason", "requirements":{"iq":0},
            "pin":format!("{provider}/{model}"), "runners":[RUNNER],
            "harness":"alpha"
        });
        let reply = consumer.call("route.select", &request).await;
        let context = format!("method={method}, per_harness={per_harness}, allowed={allowed}");
        if allowed {
            assert_eq!(reply.0, FrameType::Response, "{context}: {reply:?}");
        } else {
            assert_eq!(reply.0, FrameType::Error, "{context}: {reply:?}");
            assert_eq!(reply.1["code"], "pin_unavailable", "{context}: {reply:?}");
            assert_eq!(reply.1["detail"]["pin"], format!("{provider}/{model}"));
            assert_eq!(
                reply.1["detail"]["reason"],
                if absent_model {
                    "unknown_model"
                } else {
                    "not_available"
                },
                "{context}: {reply:?}"
            );
            if !absent_model {
                assert_eq!(
                    reply.1["detail"]["cause"], "missing_capability",
                    "{context}: {reply:?}"
                );
            }
            assert!(
                reply.1["detail"].get("reset_at_ms").is_none(),
                "{context}: {reply:?}"
            );
            for fallback in [false, true] {
                let mut routing = request.clone();
                if fallback {
                    routing["pin_fallback"] = json!("route");
                } else {
                    routing.as_object_mut().unwrap().remove("pin");
                }
                let unpinned = consumer.call("route.select", &routing).await;
                assert_eq!(unpinned.0, FrameType::Error, "{context}: {unpinned:?}");
                assert_eq!(unpinned.1["code"], "no_route", "{context}: {unpinned:?}");
                assert_eq!(
                    unpinned.1["detail"]["reason"], "missing_capability",
                    "{context}: {unpinned:?}"
                );
            }
            harness
                .credentials()
                .lock()
                .unwrap()
                .runner_declarations
                .get_mut(RUNNER)
                .unwrap()
                .pairs[0]
                .wire_api = "openai.responses".into();
            let incompatible = consumer.call("route.select", &request).await;
            assert_eq!(
                incompatible.0,
                FrameType::Error,
                "{context}: {incompatible:?}"
            );
            assert_eq!(
                incompatible.1["code"], "no_route",
                "{context}: {incompatible:?}"
            );
            assert_eq!(
                incompatible.1["detail"]["reason"], "missing_capability",
                "{context}: {incompatible:?}"
            );
        }
    }
}

#[tokio::test]
async fn apikey_provider_limits_refuse_unservable_pin() {
    check_provider_limit_pin_refusal("apikey", false).await;
}

#[tokio::test]
async fn gateway_provider_limits_refuse_unservable_pin() {
    check_provider_limit_pin_refusal("gateway", false).await;
}

// Routing 0.7.20 CONTRACT.md:533–536 requires pin_unavailable for an
// unservable pin; lines 694–698 define unknown_model for an absent row.
// Conformance 0.10.22 src/checks/gateway.rs:105,264–270 pins an unserved
// row even when provider limits have emptied every real route.
#[tokio::test]
async fn gateway_provider_limits_preserve_absent_pin_admission() {
    check_provider_limit_pin_refusal("gateway", true).await;
}

#[tokio::test]
async fn apikey_provider_limits_preserve_absent_pin_admission() {
    check_provider_limit_pin_refusal("apikey", true).await;
}
