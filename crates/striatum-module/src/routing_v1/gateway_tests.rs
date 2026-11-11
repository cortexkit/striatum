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
        let catalog = ["anthropic".into()].into();
        for fallback in [false, true] {
            let class = crate::account_selection::resolve_class(
                &rows,
                &fact,
                &catalog,
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
fn private_context_account_class_rejects_gateway_credentials() {
    let facts = crate::tests::fact("anthropic", "claude");
    let catalog = ["anthropic".into()].into();
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
        &[],
        false,
        true,
        true,
        |_| true,
    );
    assert!(!private.subscription);
    assert_eq!(private.rows.len(), 1);
    assert_eq!(private.rows[0].auth_method.as_deref(), Some("apikey"));
    let gateway_only = crate::account_selection::resolve_class(
        &rows[1..2],
        &facts,
        &catalog,
        &[],
        false,
        true,
        true,
        |_| true,
    );
    assert!(gateway_only.rows.is_empty());
}

#[test]
fn private_context_policy_rejects_gateway_despite_metered_billing() {
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
            if method == "gateway" {
                assert!(rejected["anthropic/claude"].contains("known apikey auth methods"));
            } else {
                assert!(rejected.is_empty());
            }
        }
    }
}

fn fixture() -> Arc<Harness> {
    let harness = Harness::new("company-gateway");
    for fact in harness.store().get_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::fact_to_route(&fact).route_key)
            .unwrap();
    }
    let facts = vec![crate::tests::fact("anthropic", "claude")];
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
                serves: vec!["anthropic".into()],
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
                provider_id: "anthropic".into(),
                auth_method: method.into(),
                kind: "upstream".into(),
                selection_reason: "listed credential".into(),
                rates: BTreeMap::from([("rate.output".into(), BillingRate::Priced { rate: 4.5 })]),
            });
            fact
        })
        .collect();
    credentials
        .wire_apis
        .insert("anthropic/claude".into(), "anthropic.messages".into());
    credentials.publish_billing_catalog(
        PlaneCatalog::ordinary_test_catalog(&billing).with_wire_apis(&credentials.wire_apis),
    );
    credentials.catalog_snapshot = Some(Arc::new(crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41, "resolved_at_ms":123,
            "models":{"anthropic/claude":{"existence":"present", "model.type":"chat", "capability.tool_call":true}}}))
            .unwrap()).unwrap()));
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
                    "explicit_providers":[GATEWAY]},
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
        ("broca", "broca", DIRECT_ROUTE),
        (RUNNER, "claude-code", GATEWAY_ROUTE),
        (RUNNER, "direct-api", DIRECT_ROUTE),
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
async fn private_context_consumers_never_select_a_declared_gateway() {
    let harness = fixture();
    // Exercise the privacy filter even when operator policy permits automatic
    // gateway selection and the runner declares anthropic.messages/gateway.
    harness.edit_config(|config| config["providers"][GATEWAY]["pin_only"] = json!(false));
    let caller = harness.bind(reserved("example-consumer"), None).await;
    for consumer in ["codemode", "flow"] {
        for pinned in [false, true] {
            let mut request = json!({
                "consumer":consumer, "requirements":{"iq":0}, "detail":true,
                "runners":[RUNNER], "harness":"claude-code"
            });
            if pinned {
                request["pin"] = json!(GATEWAY_ROUTE);
            }
            let refused = caller.call("route.select", &request).await;
            assert_eq!(refused.0, FrameType::Error, "{consumer}: {refused:?}");
            assert_eq!(
                refused.1["code"],
                if pinned {
                    "pin_unavailable"
                } else {
                    "no_route"
                },
                "{refused:?}"
            );
            if pinned {
                assert_eq!(
                    refused.1["detail"]["reason"], "not_available",
                    "{refused:?}"
                );
            } else {
                assert!(
                    refused.1.to_string().contains("no_eligible_account"),
                    "{refused:?}"
                );
            }
        }
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
