//! A decision records the project and session folder the daemon stamped on the
//! caller's scope, and records nothing when the route had no scope. Both values
//! come from the bind's scope stamp; a project named in the request body is
//! refused like any other member `routing/v1` does not define.

use std::sync::Arc;

use cortexkit_role_routing_conformance::fixture;
use prefrontal_routing_store::{DecisionExplainRow, DecisionListFilter};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{install_catalog, reserved, Harness, Route};

const PROJECT: &str = "entorhinal-project-7";
const WORKDIR: &str = "/work/projects/example";

fn harness() -> Arc<Harness> {
    let harness = Harness::new("decision-scope");
    let consumers: Vec<String> = fixture::consumers()
        .into_iter()
        .map(|consumer| consumer.id)
        .collect();
    harness.edit_config(|config| {
        config["consumers"] = consumers
            .iter()
            .map(|id| {
                (
                    (*id).to_owned(),
                    json!({"owner": fixture::CONSUMER.trim_start_matches("reserved:")}),
                )
            })
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into();
        config["runners"] = json!([{ "module": "runner-one" }]);
    });
    install_catalog(&harness, &fixture::catalog());
    harness
}

/// Bind a route whose scope stamp names `project` and `workdir`, as the daemon
/// sends them to a target that provides `scope-workdir/v1`. No scope is sent
/// when neither is given.
async fn scoped(
    harness: &Arc<Harness>,
    principal: Value,
    project: Option<&str>,
    workdir: Option<&str>,
) -> Route {
    let channel = harness.reserve_channel();
    let epoch = 1;
    let mut bind = json!({
        "op": "route.bind",
        "route_channel": channel,
        "epoch": epoch,
        "target": { "kind": "management_surface", "module_id": crate::DEFAULT_MODULE_ID },
        "identity": {
            "project_root": prefrontal_test_support::scratch_root(),
            "harness": "routing-scope-attribution",
            "session": format!("session-{channel}"),
        },
        "principal": principal,
    });
    if project.is_some() || workdir.is_some() {
        let mut stamp = json!({
            "owner": { "kind": "reserved", "module_id": "prefrontal-core" },
            "ref": format!("scope-{channel}"),
            "scope_epoch": 1,
            "kind": "ephemeral",
            "owner_authorized": true,
        });
        if let Some(project) = project {
            stamp["project_id"] = json!(project);
        }
        if let Some(workdir) = workdir {
            stamp["workdir"] = json!(workdir);
        }
        bind["scope"] = stamp;
    }
    harness.bind_frame(bind, channel, epoch).await
}

fn v1_select() -> Value {
    json!({ "consumer": fixture::SUMMARY, "requirements": { "iq": 50 } })
}

fn decision(harness: &Harness, reply: &(FrameType, Value)) -> DecisionExplainRow {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let id = reply.1["result"]["decision_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no decision id in {}", reply.1));
    harness
        .store()
        .get_decision_by_id(id)
        .unwrap()
        .unwrap_or_else(|| panic!("decision {id} was not recorded"))
}

#[tokio::test]
async fn a_scoped_v1_select_records_the_stamped_project_and_workdir() {
    let harness = harness();
    let route = scoped(
        &harness,
        reserved(fixture::CONSUMER),
        Some(PROJECT),
        Some(WORKDIR),
    )
    .await;

    let row = decision(&harness, &route.call("route.select", &v1_select()).await);
    assert_eq!(row.project_id.as_deref(), Some(PROJECT));
    assert_eq!(row.workdir.as_deref(), Some(WORKDIR));
    // The attribution is the stamp's: nothing about it is read from the path.
    assert_ne!(row.project_root.as_deref(), Some(PROJECT));
}

#[tokio::test]
async fn a_scoped_legacy_select_records_the_stamped_project() {
    let harness = harness();
    let route = scoped(
        &harness,
        reserved(fixture::CONSUMER),
        Some(PROJECT),
        Some(WORKDIR),
    )
    .await;

    let reply = route
        .call(
            "route.select",
            &json!({
                "targetAgent": "mason",
                "requirements": {"iq": 50, "eq": 50},
                "availableModels": ["example-provider/model-a"],
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let id = reply.1["result"]["decisionID"].as_str().unwrap();
    let row = harness.store().get_decision_by_id(id).unwrap().unwrap();
    assert_eq!(row.project_id.as_deref(), Some(PROJECT));
    assert_eq!(row.workdir.as_deref(), Some(WORKDIR));
}

#[tokio::test]
async fn an_unscoped_select_records_no_project() {
    let harness = harness();
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;

    let row = decision(&harness, &route.call("route.select", &v1_select()).await);
    assert_eq!(row.project_id, None);
    assert_eq!(row.workdir, None);
}

#[tokio::test]
async fn a_scope_naming_no_project_records_none_rather_than_a_derived_one() {
    let harness = harness();
    // A scope whose stamp carries a folder but no project id, as a daemon whose
    // owner never resolved one sends it. Nothing is derived from the folder.
    let route = scoped(&harness, reserved(fixture::CONSUMER), None, Some(WORKDIR)).await;

    let row = decision(&harness, &route.call("route.select", &v1_select()).await);
    assert_eq!(row.project_id, None);
    assert_eq!(row.workdir.as_deref(), Some(WORKDIR));
}

#[tokio::test]
async fn a_project_named_in_the_request_body_is_refused_not_recorded() {
    let harness = harness();
    let route = scoped(
        &harness,
        reserved(fixture::CONSUMER),
        Some(PROJECT),
        Some(WORKDIR),
    )
    .await;
    let before = harness
        .store()
        .list_decisions(&DecisionListFilter::default())
        .unwrap();

    for member in ["project_id", "projectId", "project"] {
        let mut body = v1_select();
        body[member] = json!("body-named-project");
        let reply = route.call("route.select", &body).await;
        assert_eq!(reply.0, FrameType::Error, "{member}: {}", reply.1);
        assert_eq!(reply.1["code"], "invalid_request", "{member}: {}", reply.1);
        assert_eq!(
            reply.1["detail"]["field"], member,
            "{member}: the refusal must name the member: {}",
            reply.1
        );
    }
    // A refused request selects nothing, so it records no decision either.
    let after = harness
        .store()
        .list_decisions(&DecisionListFilter::default())
        .unwrap();
    assert_eq!(after.len(), before.len());
}

#[test]
fn the_manifest_provides_the_scope_workdir_capability() {
    let declared = crate::manifest(crate::DEFAULT_MODULE_ID)
        .capabilities
        .expect("the manifest declares capabilities");
    assert!(
        declared
            .provides
            .iter()
            .any(|capability| capability == subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY),
        "got {:?}",
        declared.provides
    );
}
