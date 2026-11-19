use super::*;
use crate::routing_v1::{consumers, describe};
use cortexkit_role_routing::capabilities::SELECT_WORKLOAD;

const OWNER: &str = "workload-fixture";
const CONSUMER: &str = "workload-fixture.review";
const CHILD: &str = "workload-fixture.review.inherited.child";

async fn declared() -> (std::sync::Arc<Harness>, Route) {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |_, row| {
        row["quality.graded_effort"] = json!("high");
        row["workload.code.effort_default"] = json!("low");
        row["workload.reason.effort_default"] = json!("medium");
        row["workload.lookup.effort_default"] = json!("xhigh");
    });
    let route = harness.bind(reserved(OWNER), None).await;
    consumers::replace_catalog(&json!([{
        "module_id": OWNER, "nonce_verified": true,
        "role_declarations":{"routing/v1":{"consumers":[
            {"name":CONSUMER, "workload":"code"},
            {"name":"workload-fixture.review.inherited", "iq":0}
        ]}}
    }]));
    (harness, route)
}

async fn pick(route: &Route, consumer: &str, workload: Option<&str>) -> Value {
    let params = json!({"consumer":consumer,
        "requirements":{"iq":0,"workload":workload},
        "available_routes":[A],"detail":true});
    let (ty, response) = route.call("route.select", &params).await;
    assert_eq!(ty, FrameType::Response, "{response}");
    response["result"].clone()
}

fn assert_workload(pick: &Value, variant: &str, source: &str) {
    assert_eq!(pick["route"]["variant"], variant, "{pick}");
    assert_eq!(
        pick["detail"]["reasoning"]["effort_source"],
        "workload_default"
    );
    assert_eq!(
        pick["detail"]["reasoning"]["workload_source"], source,
        "{pick}"
    );
}

#[tokio::test]
async fn declared_workload_drives_effort_and_is_recorded() {
    describe::with_candidate_capabilities(&[SELECT_WORKLOAD], async {
        let (harness, route) = declared().await;
        let selected = pick(&route, CONSUMER, None).await;
        assert_workload(&selected, "low", "declaration");
        let id = selected["decision_id"].as_str().unwrap();
        let row = harness.store().get_decision_by_id(id).unwrap().unwrap();
        let recorded: Value = serde_json::from_str(&row.requirements_json).unwrap();
        assert_eq!(recorded["workload_source"], "declaration");
        let (ty, response) = route
            .call("route.explain", &json!({"decision_id":id}))
            .await;
        assert_eq!(ty, FrameType::Response, "{response}");
        assert_eq!(response["result"]["effort_source"], "workload_default");
        assert_eq!(response["result"]["workload_source"], "declaration");
    })
    .await;
}

#[tokio::test]
async fn settings_workload_overrides_declaration() {
    describe::with_candidate_capabilities(&[SELECT_WORKLOAD], async {
        let (harness, route) = declared().await;
        harness.edit_config(|c| c["consumers"][CONSUMER] = json!({"workload":"reason"}));
        assert_workload(&pick(&route, CONSUMER, None).await, "medium", "settings");
    })
    .await;
}

#[tokio::test]
async fn request_workload_overrides_declaration_but_not_settings() {
    describe::with_candidate_capabilities(&[SELECT_WORKLOAD], async {
        let (harness, route) = declared().await;
        assert_workload(
            &pick(&route, CHILD, Some("lookup")).await,
            "xhigh",
            "request",
        );
        harness.edit_config(|c| c["consumers"][CONSUMER] = json!({"workload":"reason"}));
        assert_workload(
            &pick(&route, CHILD, Some("lookup")).await,
            "medium",
            "settings",
        );

        let outsider = harness.bind(reserved("other-workload-fixture"), None).await;
        let (ty, response) = outsider
            .call(
                "route.select",
                &json!({"consumer":CHILD,
            "requirements":{"iq":0,"workload":"lookup"}}),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{response}");
        assert_eq!(response["code"], "consumer_not_owned", "{response}");
    })
    .await;
}

#[tokio::test]
async fn dotted_child_inherits_declared_workload() {
    describe::with_candidate_capabilities(&[SELECT_WORKLOAD], async {
        let (harness, route) = declared().await;
        let direct_child = format!("{CONSUMER}.direct");
        harness.edit_config(|c| c["consumers"][&direct_child] = json!({}));
        assert_workload(&pick(&route, CHILD, None).await, "low", "declaration");
        assert_workload(
            &pick(&route, &direct_child, None).await,
            "low",
            "declaration",
        );
    })
    .await;
}

#[tokio::test]
async fn unknown_workload_shadows_declaration_without_applying_policy_and_warns_once() {
    describe::with_candidate_capabilities(&[SELECT_WORKLOAD], async {
        let (_harness, route) = declared().await;
        crate::take_operator_warnings();
        for (consumer, workload) in [
            (CONSUMER, "future"),
            (CONSUMER, "future"),
            (CONSUMER, "other_future"),
            (CHILD, "future"),
        ] {
            let selected = pick(&route, consumer, Some(workload)).await;
            assert_eq!(selected["route"]["variant"], "high", "{selected}");
            let reasoning = &selected["detail"]["reasoning"];
            assert_eq!(reasoning["effort_source"], "graded");
            assert!(reasoning.get("workload_source").is_none(), "{reasoning}");
        }
        let warnings: Vec<_> = crate::take_operator_warnings()
            .into_iter()
            .filter(|warning| warning.contains("unknown workload") && warning.contains(CONSUMER))
            .collect();
        assert_eq!(warnings.len(), 3, "{warnings:?}");
    })
    .await;
}

#[tokio::test]
async fn role_describe_advertises_select_workload() {
    let (_harness, route) = configured().await;
    let (ty, response) = route.call("role.describe", &json!({})).await;
    assert_eq!(ty, FrameType::Response, "{response}");
    let description = &response["result"];
    assert!(
        description["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(SELECT_WORKLOAD)),
        "{description}"
    );
    assert!(
        description["requirement_fields"]
            .as_array()
            .unwrap()
            .contains(&json!("workload")),
        "{description}"
    );
}
