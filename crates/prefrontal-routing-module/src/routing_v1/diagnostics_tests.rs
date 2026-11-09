//! `diagnostics.findings` on the wire: who may read it, what a flow-scoped
//! route may reach, and a seeded store read end to end.

use std::sync::Arc;

use prefrontal_routing_core::route_state_store::CredentialRefusal;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};

const NOW: u64 = 1_800_000_000_000;
const OP: &str = crate::diagnostics_findings::OP;

fn assert_vector(name: &str, value: &Value) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/diagnostics-findings-v1")
        .join(name);
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    if std::env::var_os("UPDATE_DIAGNOSTICS_FINDINGS_VECTORS").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(bytes, std::fs::read(path).unwrap(), "vector {name}");
}

#[tokio::test]
async fn diagnostics_findings_v1_vectors_match_serving_replies() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("diagnostics-v1-vectors");
    hosts(&[("prefrontal-core", true)]);
    let head = scoped(&harness, "bridge-relay", head_scope("prefrontal-core")).await;
    for (name, params, window) in [
        ("request-empty.json", json!({}), 21_600_000),
        ("request-window.json", json!({"window_ms": 60_000}), 60_000),
        (
            "request-max-window.json",
            json!({"window_ms": 604_800_000}),
            604_800_000,
        ),
    ] {
        assert_vector(name, &json!({"method": OP, "params": params}));
        let (ty, reply) = head.call(OP, &params).await;
        assert_eq!(ty, FrameType::Response, "{reply}");
        assert_eq!(reply["result"]["window_ms"], window);
        assert_eq!(reply["result"]["findings"], json!([]));
    }
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "inception".into(),
                credential_id: "apikey:inception:main".into(),
                code: "billing_exhausted".into(),
            },
            None,
            NOW as i64 - 10 * 60_000,
        )
        .unwrap();
    let (ty, reply) = head.call(OP, &json!({})).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    assert_eq!(reply["result"]["findings"].as_array().unwrap().len(), 1);
    assert_vector("reply-routing-findings.json", &reply);
}

fn hosts(entries: &[(&str, bool)]) {
    super::consumers::replace_catalog(&Value::Array(
        entries
            .iter()
            .map(|(module, host)| {
                json!({"module_id":module,"nonce_verified":true,
                       "provides":if *host {vec!["agent-host/v1"]} else {vec![]}})
            })
            .collect(),
    ));
}

/// A route bound under a scope stamp the daemon would send.
async fn scoped(harness: &Arc<Harness>, principal: &str, scope: Value) -> Route {
    let channel = harness.reserve_channel();
    harness
        .bind_frame(
            json!({
                "op":"route.bind", "route_channel":channel, "epoch":1,
                "target":{"kind":"management_surface","module_id":crate::DEFAULT_MODULE_ID},
                "identity":{"project_root":cortexkit_test_support::scratch_root(),
                            "harness":"diagnostics","session":format!("session-{channel}")},
                "principal":reserved(principal),
                "scope":scope,
            }),
            channel,
            1,
        )
        .await
}

fn head_scope(owner: &str) -> Value {
    json!({"owner":reserved(owner),"ref":"head-session","scope_epoch":3,"kind":"head",
           "owner_authorized":true})
}

fn flow_scope(owner_authorized: bool) -> Value {
    json!({"owner":reserved("prefrontal-core"),"ref":"flow-scope","scope_epoch":4,"kind":"head",
           "owner_authorized":owner_authorized,"attributes":{"flow_id":"routing-watch"}})
}

#[tokio::test]
async fn head_routes_and_flow_scopes_read_findings_and_everyone_else_is_refused() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("diagnostics-admission");
    hosts(&[("prefrontal-core", true), ("plain-module", false)]);

    let head = scoped(&harness, "bridge-relay", head_scope("prefrontal-core")).await;
    let reply = head.call(OP, &json!({})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"],
        json!({"generated_at_ms": NOW, "window_ms": 21_600_000, "findings": []}),
        "a clean store reports no finding"
    );

    let flow = scoped(&harness, "basal", flow_scope(true)).await;
    let reply = flow.call(OP, &json!({"window_ms": 60_000})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["window_ms"], 60_000);

    let refused: Vec<(&str, Route)> = vec![
        (
            "unscoped module",
            harness.bind(reserved("plain-module"), None).await,
        ),
        (
            "operator shell",
            harness.bind(json!({"kind": "direct"}), None).await,
        ),
        (
            "head scope of a module that hosts no agents",
            scoped(&harness, "bridge-relay", head_scope("plain-module")).await,
        ),
        (
            "ephemeral scope of an agent host",
            scoped(
                &harness,
                "bridge-relay",
                json!({"owner":reserved("prefrontal-core"),"ref":"e","scope_epoch":1,
                       "kind":"ephemeral","owner_authorized":true}),
            )
            .await,
        ),
        (
            "flow id from an unauthorized owner",
            scoped(&harness, "basal", flow_scope(false)).await,
        ),
    ];
    for (who, route) in refused {
        let reply = route.call(OP, &json!({})).await;
        assert_eq!(reply.0, FrameType::Error, "{who}: {}", reply.1);
        assert_eq!(reply.1["code"], "not_permitted", "{who}: {}", reply.1);
    }

    let reply = head.call(OP, &json!({"window_ms": 0})).await;
    assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
    let reply = head.call(OP, &json!({"since_ms": 1})).await;
    assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
}

#[tokio::test]
async fn a_flow_scoped_route_reaches_only_the_allowlisted_ops() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("diagnostics-flow-fence");
    hosts(&[("prefrontal-core", true)]);
    let flow = scoped(&harness, "broca", flow_scope(true)).await;
    // The exact refusal Basal maps as final and never retries.
    let (ty, body) = flow.call_raw("route.decisions_list", &json!({})).await;
    assert_eq!(ty, FrameType::Error);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"code":"flow_scope_not_supported","message":"route.decisions_list does not run under a flow scope; flows may call: diagnostics.findings","detail":{"allowed_ops":["diagnostics.findings"],"op":"route.decisions_list"}}"#
    );
    for (method, params) in [
        ("route.select", json!({"consumer": "flow"})),
        (
            "route.set_decision_outcome",
            json!({"decisionID":"d","outcome":"success"}),
        ),
        (
            "route.account_for",
            json!({"provider":"p","model":"m","admission_id":"a"}),
        ),
        ("role.describe", json!({})),
    ] {
        let reply = flow.call(method, &params).await;
        assert_eq!(reply.0, FrameType::Error, "{method}: {}", reply.1);
        assert_eq!(
            reply.1["code"], "flow_scope_not_supported",
            "{method}: {}",
            reply.1
        );
        assert_eq!(reply.1["detail"]["op"], method, "{}", reply.1);
    }
    let reply = flow.call(OP, &json!({})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    // A head route is not fenced.
    let head = scoped(&harness, "bridge-relay", head_scope("prefrontal-core")).await;
    let reply = head.call("route.decisions_list", &json!({})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

/// Basal's own model selection and outcome calls go out on its unscoped
/// `reserved:basal` route, which the flow gate never touches.
#[tokio::test]
async fn basal_unscoped_routing_calls_are_not_fenced() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("diagnostics-basal-unscoped");
    let basal = harness.bind(reserved("basal"), None).await;
    // The body Basal's routing selector sends for a flow's model call.
    let select = basal
        .call(
            "route.select",
            &json!({"excludeRouteKeys":[],"requirements":{"eq":20,"iq":70},
                    "sendID":"send-1","substrate":"broca","targetAgent":"flow",
                    "taskId":"flow:flow-a:run-a"}),
        )
        .await;
    assert_ne!(select.1["code"], "flow_scope_not_supported", "{}", select.1);
    let outcome = basal
        .call(
            "route.set_decision_outcome",
            &json!({"decisionID":"decision-1","outcome":"success"}),
        )
        .await;
    assert_ne!(
        outcome.1["code"], "flow_scope_not_supported",
        "{}",
        outcome.1
    );
    let listed = basal.call("route.decisions_list", &json!({})).await;
    assert_eq!(listed.0, FrameType::Response, "{}", listed.1);
    // An unscoped route is no flow scope, so the diagnosis stays closed to it.
    let diagnosis = basal.call(OP, &json!({})).await;
    assert_eq!(diagnosis.1["code"], "not_permitted", "{}", diagnosis.1);
}

#[test]
fn the_manifest_declares_flow_scope_support() {
    let manifest = crate::manifest(crate::DEFAULT_MODULE_ID);
    let provides = &manifest.capabilities.as_ref().unwrap().provides;
    assert!(
        provides
            .iter()
            .any(|capability| capability == subc_protocol::scope::FLOW_SCOPES_CAPABILITY),
        "{provides:?}"
    );
    assert_eq!(crate::diagnostics_findings::FLOW_SCOPE_OPS, [OP]);
    let flow_operations = manifest
        .flow_operations
        .as_ref()
        .expect("routing must declare its flow operations");
    assert_eq!(
        flow_operations.as_slice(),
        crate::diagnostics_findings::FLOW_SCOPE_OPS
    );
    for name in flow_operations {
        assert!(
            manifest.provides.iter().any(|role| matches!(
                role,
                subc_protocol::manifest::ProviderRole::ManagementSurface { operations, .. }
                    if operations.iter().any(|operation| &operation.name == name)
            )),
            "flow operation {name} must be a manifest management operation"
        );
    }
    assert!(crate::served_operations().iter().any(|operation| {
        operation.name == OP
            && operation.kind == subc_protocol::manifest::ManagementOperationKind::Query
    }));
}

#[tokio::test]
async fn the_diagnosis_reads_the_live_store() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("diagnostics-store");
    hosts(&[("prefrontal-core", true)]);
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "inception".into(),
                credential_id: "apikey:inception:main".into(),
                code: "billing_exhausted".into(),
            },
            None,
            NOW as i64 - 3 * 60 * 60 * 1000,
        )
        .unwrap();
    let head = scoped(&harness, "bridge-relay", head_scope("prefrontal-core")).await;
    let reply = head.call(OP, &json!({})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let findings = reply.1["result"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 1, "{findings:#?}");
    assert_eq!(findings[0]["check"], "credential_fences");
    assert_eq!(findings[0]["key"], "inception/apikey:inception:main");
    assert_eq!(findings[0]["severity"], "error");
    assert_eq!(
        harness
            .store()
            .credential_entitlement_fences()
            .unwrap()
            .len(),
        1,
        "the diagnosis lifts nothing"
    );
}
