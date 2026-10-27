//! Consumer admission through the same catalog and route-bind seams as production.

use std::sync::Arc;

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{install_catalog, reserved, Harness, Route};
use super::{consumers, Caller};

fn harness() -> Arc<Harness> {
    let harness = Harness::new("consumer-admission");
    harness.edit_config(|config| {
        config["consumers"] = json!({
            "codemode": { "owner": "prefrontal-core", "pay_per_token": true },
            "mc_dream_curate": { "owner": "magic-context" }
        });
    });
    install_catalog(&harness, &fixture::catalog());
    harness
}

fn entry(module: &str, verified: bool, names: &[&str]) -> Value {
    json!({
        "module_id": module, "nonce_verified": verified,
        "role_declarations": { "routing/v1": { "consumers": names.iter().map(|name| json!({"name": name})).collect::<Vec<_>>() } }
    })
}

fn select(consumer: &str) -> Value {
    json!({"consumer": consumer, "requirements": {"iq": 30}})
}

#[tokio::test]
async fn ownerless_config_consumer_is_selectable_by_unrelated_callers() {
    let harness = harness();
    let consumer = "example.public";
    harness.edit_config(|config| config["consumers"][consumer] = json!({}));
    for principal in [
        reserved("first-unrelated"),
        reserved("second-unrelated"),
        Value::Null,
    ] {
        let route = harness.bind(principal, None).await;
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    }

    let channel = harness.reserve_channel();
    let old_route = scoped(&harness, "unrelated-owner", false, channel, 1).await;
    let captured = Caller::for_route(channel, 1);
    let dispatched = old_route.request_frame("route.select", &select(consumer));
    scoped(&harness, "unrelated-owner", false, channel, 2).await;
    refused(
        harness.finish_dispatched(dispatched).await,
        "consumer_not_owned",
        consumer,
    );
    assert_eq!(
        consumers::effective(&harness.operator_config())
            .admit(consumer, &captured)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
}

fn refused(reply: (FrameType, Value), code: &str, consumer: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], code, "{}", reply.1);
    assert_eq!(reply.1["detail"]["consumer"], consumer, "{}", reply.1);
}

async fn scoped(
    harness: &Arc<Harness>,
    owner: &str,
    authorized: bool,
    channel: u16,
    epoch: u32,
) -> Route {
    harness.bind_frame(json!({
        "op": "route.bind", "route_channel": channel, "epoch": epoch,
        "target": { "kind": "management_surface", "module_id": crate::DEFAULT_MODULE_ID },
        "identity": { "project_root": prefrontal_test_support::scratch_root(), "harness": "consumer-admission", "session": "session-1" },
        "principal": reserved("broca"),
        "scope": { "owner": reserved(owner), "ref": "codemode-run", "kind": "ephemeral", "scope_epoch": 7, "project_id": "project-1", "owner_authorized": authorized }
    }), channel, epoch).await
}

#[tokio::test]
async fn verified_declarer_and_configured_owner_are_admitted_without_a_project() {
    let harness = harness();
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        true,
        &["example.worker"]
    )]));
    for (module, consumer) in [
        ("declaring-module", "example.worker"),
        ("magic-context", "mc_dream_curate"),
    ] {
        let route = harness.bind(reserved(module), None).await;
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    }
    let stranger = harness.bind(reserved("unrelated"), Some("project-1")).await;
    refused(
        stranger
            .call("route.select", &select("mc_dream_curate"))
            .await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
}

#[tokio::test]
async fn only_verified_catalog_declarations_create_ownership() {
    let harness = harness();
    let route = harness.bind(reserved("declaring-module"), None).await;
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        true,
        &["example.worker"]
    )]));
    assert_eq!(
        route
            .call("route.select", &select("example.worker"))
            .await
            .0,
        FrameType::Response
    );
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        false,
        &["example.worker"]
    )]));
    refused(
        route.call("route.select", &select("example.worker")).await,
        "unknown_consumer",
        "example.worker",
    );
    let description = route.call("role.describe", &json!({})).await;
    assert!(!description.1["result"]["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["id"] == "example.worker"));
    consumers::replace_catalog(&json!([]));
    refused(
        route.call("route.select", &select("example.worker")).await,
        "unknown_consumer",
        "example.worker",
    );
}

#[tokio::test]
async fn different_verified_owners_lock_the_name_in_both_catalog_orders() {
    let harness = harness();
    let first = entry("first-module", true, &["example.worker"]);
    let second = entry("second-module", true, &["example.worker"]);
    for catalog in [json!([first, second]), json!([second, first])] {
        consumers::replace_catalog(&catalog);
        for module in ["first-module", "second-module"] {
            let route = harness.bind(reserved(module), None).await;
            refused(
                route.call("route.select", &select("example.worker")).await,
                "consumer_not_owned",
                "example.worker",
            );
            let description = route.call("role.describe", &json!({})).await;
            assert!(!description.1["result"]["consumers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["id"] == "example.worker"));
        }
    }
}

#[tokio::test]
async fn configured_owners_join_the_declaration_lock_without_overriding_it() {
    let harness = harness();
    let owner = harness.bind(reserved("magic-context"), None).await;
    consumers::replace_catalog(&json!([entry("magic-context", true, &["mc_dream_curate"])]));
    assert_eq!(
        owner
            .call("route.select", &select("mc_dream_curate"))
            .await
            .0,
        FrameType::Response
    );
    consumers::replace_catalog(&json!([entry("other-module", true, &["mc_dream_curate"])]));
    refused(
        owner.call("route.select", &select("mc_dream_curate")).await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
    let other = harness.bind(reserved("other-module"), None).await;
    refused(
        other.call("route.select", &select("mc_dream_curate")).await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
}

#[tokio::test]
async fn delegated_select_requires_current_owner_authorized_scope_not_project_equality() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let authority = Caller::for_route(channel, 1).scope_authority.unwrap();
    assert_eq!(authority.owner, "reserved:prefrontal-core");
    assert_eq!(authority.reference, "codemode-run");
    assert_eq!(serde_json::to_value(authority.kind).unwrap(), "ephemeral");
    assert_eq!(authority.scope_epoch, 7);
    assert_eq!(authority.project_id.as_deref(), Some("project-1"));
    assert!(authority.owner_authorized);
    assert_eq!(
        route.call("route.select", &select("codemode")).await.0,
        FrameType::Response
    );
    for (owner, authorized, epoch) in [("prefrontal-core", false, 2), ("other-module", true, 3)] {
        let route = scoped(&harness, owner, authorized, channel, epoch).await;
        refused(
            route.call("route.select", &select("codemode")).await,
            "consumer_not_owned",
            "codemode",
        );
    }
    let unscoped = harness.bind(reserved("broca"), None).await;
    refused(
        unscoped.call("route.select", &select("codemode")).await,
        "consumer_not_owned",
        "codemode",
    );
    // The transport drops stale frames; the ownership boundary must also deny
    // a stale epoch if a request was already dispatched when the channel rebound.
    let config = harness.operator_config();
    let registry = consumers::effective(&config);
    let stale = Caller::for_route(channel, 1);
    assert!(stale.scope_authority.is_none());
    assert_eq!(
        registry
            .admit("codemode", &stale)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
    crate::clear_route_scope(channel, 1);
    assert!(Caller::for_route(channel, 3).scope_authority.is_some());
    crate::clear_route_scope(channel, 3);
    assert!(Caller::for_route(channel, 3).scope_authority.is_none());
}

#[tokio::test]
async fn legacy_core_selection_stays_open_but_non_core_cannot_borrow_target_agent_authority() {
    let harness = harness();
    let route = harness.bind(reserved("unrelated"), None).await;
    let core = route
        .call(
            "route.select",
            &json!({"targetAgent": "codemode", "executor":"broca", "substrate":"broca", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]}),
        )
        .await;
    assert_eq!(core.0, FrameType::Response, "{}", core.1);
    for params in [
        json!({"targetAgent": "mc_dream_curate", "requirements": {"iq": 30, "eq": 0}}),
        json!({"targetAgent": "mason", "consumer": "mc_dream_curate", "requirements": {"iq": 30, "eq": 0}, "owner": "magic-context", "project_id": "project-1"}),
    ] {
        refused(
            route.call("route.select", &params).await,
            "consumer_not_owned",
            "mc_dream_curate",
        );
    }
}

#[tokio::test]
async fn legacy_explore_admission_preserves_the_selected_route_without_a_consumer_group() {
    let harness = harness();
    let caller = harness.bind(reserved("unrelated"), None).await;
    let request = json!({"targetAgent": "explore", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]});
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["selected"]["routeKey"],
        "example-provider/model-a"
    );
    let mut canonical = request;
    canonical["targetAgent"] = json!("mason");
    let baseline = caller.call("route.select", &canonical).await;
    assert_eq!(baseline.0, FrameType::Response, "{}", baseline.1);
    assert_eq!(
        baseline.1["result"]["selected"]["routeKey"],
        reply.1["result"]["selected"]["routeKey"]
    );
}

#[tokio::test]
async fn an_unregistered_legacy_target_label_has_no_core_admission_exception() {
    let harness = harness();
    let caller = harness.bind(reserved("unrelated"), None).await;
    let request = json!({"targetAgent": "unregistered-label", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]});
    refused(
        caller.call("route.select", &request).await,
        "unknown_consumer",
        "unregistered-label",
    );
    harness.edit_config(|config| {
        config["consumers"]["unregistered-label"] = json!({"owner": "consumer-owner"})
    });
    refused(
        caller.call("route.select", &request).await,
        "consumer_not_owned",
        "unregistered-label",
    );
    let owner = harness.bind(reserved("consumer-owner"), None).await;
    let reply = owner.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[tokio::test]
async fn a_report_cannot_reuse_a_delegates_decision_without_owner_authorization() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let selected = route.call("route.select", &select("codemode")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let report =
        json!({"decision_id": selected.1["result"]["decision_id"], "outcome": "completed"});
    let unscoped = harness.bind(reserved("broca"), None).await;
    refused(
        unscoped.call("route.report", &report).await,
        "consumer_not_owned",
        "codemode",
    );
    let reply = route.call("route.report", &report).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[tokio::test]
async fn a_dispatched_select_loses_authority_when_the_route_rebinds_before_admission() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let old_route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let captured_caller = Caller::for_route(channel, 1);
    let dispatched = old_route.request_frame("route.select", &select("codemode"));
    let current_route = scoped(&harness, "prefrontal-core", true, channel, 2).await;
    assert_eq!(
        consumers::effective(&harness.operator_config())
            .admit("codemode", &captured_caller)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
    refused(
        harness.finish_dispatched(dispatched).await,
        "consumer_not_owned",
        "codemode",
    );
    assert_eq!(
        current_route
            .call("route.select", &select("codemode"))
            .await
            .0,
        FrameType::Response
    );
}

#[test]
fn body_claims_and_unattested_principals_never_grant_configured_ownership() {
    let harness = harness();
    let registry = consumers::effective(&harness.operator_config());
    for principal in [
        "direct",
        "unverified",
        "unattested",
        "reserved:",
        "reserved:unrelated",
    ] {
        let caller = Caller {
            binding: None,
            principal: principal.into(),
            scope: None,
            scope_authority: None,
        };
        assert_eq!(
            registry
                .admit("mc_dream_curate", &caller)
                .unwrap_err()
                .code
                .as_str(),
            "consumer_not_owned"
        );
    }
}

#[tokio::test]
async fn fractional_operator_score_targets_do_not_cross_the_integer_wire_validator() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["codemode"]["iq"] = json!(30.5);
        config["consumers"]["codemode"]["eq"] = json!(20.5);
    });
    let route = harness.bind(reserved("prefrontal-core"), None).await;
    let reply = route.call("route.select", &select("codemode")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[test]
fn declaration_defaults_inherit_within_the_declaring_module_and_requests_override_them() {
    let catalog = consumers::Catalog::from_catalog(&json!([
        {"module_id": "module-a", "nonce_verified": true, "role_declarations": {"routing/v1": {"consumers": [
             {"name": "example", "eq": 40, "context_window": 32000, "max_output_tokens":4096},
             {"name": "example.worker", "eq": 50, "tools": "required", "max_output_tokens":8192}
        ]}}},
        entry("module-b", true, &["example.foreign"])
    ]));
    let harness = harness();
    let config = harness.operator_config();
    let registry = catalog.effective(&config);
    let demands = registry.with_defaults(
        "example.worker",
        &json!({"requirements": {"iq": 30, "eq": 0, "max_output_tokens":2048}}),
        &config,
    );
    assert_eq!(
        demands["requirements"],
        json!({"iq": 30, "eq": 0, "context_window": 32000, "tools": "required", "max_output_tokens":2048})
    );
    let foreign = registry.with_defaults(
        "example.foreign",
        &json!({"requirements": {"iq": 30}}),
        &config,
    );
    assert_eq!(foreign["requirements"], json!({"iq": 30}));
    let declared = registry.with_defaults(
        "example.worker",
        &json!({"requirements":{"iq":30}}),
        &config,
    );
    assert_eq!(declared["requirements"]["max_output_tokens"], 8192);
    harness.edit_config(|config| config["consumers"]["example"]["max_output_tokens"] = json!(1024));
    let operator = registry.with_defaults(
        "example.worker",
        &json!({"requirements":{"iq":30,"max_output_tokens":2048}}),
        &harness.operator_config(),
    );
    assert_eq!(operator["requirements"]["max_output_tokens"], 1024);
}

#[test]
fn catalog_wire_decodes_routing_declarations_with_the_pinned_protocol() {
    use serde::de::DeserializeOwned;
    use std::future::Future;
    use subc_client_rs::SubcConsumer;

    // Infer the actual SDK result type rather than mirroring its catalog schema.
    // The callback is never invoked: this test decodes the wire without a daemon.
    fn decode<T: DeserializeOwned, E, Fut: Future<Output = Result<T, E>>>(
        _catalog_list: impl FnOnce(SubcConsumer) -> Fut,
        bytes: &[u8],
    ) -> T {
        serde_json::from_slice(bytes).unwrap()
    }
    let mut module = entry("declaring-module", true, &["example.worker"]);
    module["roles"] = json!([]);
    module["control_ops"] = json!([]);
    module["generation"] = json!(1);
    let wire = json!({"modules": [module], "generation": 1});
    let catalog = decode(
        |consumer| async move { consumer.catalog_list().await },
        &serde_json::to_vec(&wire).unwrap(),
    );
    let modules = serde_json::to_value(catalog.modules).unwrap();
    let slot = &modules[0]["role_declarations"]["routing/v1"];
    let declarations =
        cortexkit_role_routing::declarations::parse_consumer_declarations(slot).unwrap();
    assert_eq!(declarations.consumers[0].name, "example.worker");
    assert_eq!(modules[0]["nonce_verified"], true);
    assert_eq!(modules[0]["module_id"], "declaring-module");
}
