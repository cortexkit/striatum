use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::dispatch_tests::configured;

#[tokio::test]
async fn select_task_ref_is_recorded_on_the_decision_and_report_usage() {
    let (harness, route) = configured().await;
    let task_ref = "wi:campaign:0:s6";
    let (ty, body) = route
        .call(
            "route.select",
            &json!({
                "consumer": fixture::SUMMARY, "requirements": {"iq": 50},
                "task_ref": task_ref, "correlation_id": "log-only-not-the-task",
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{body}");
    let decision = body["result"]["decision_id"].as_str().unwrap();
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    let stored: String = connection
        .query_row(
            "SELECT task_id FROM model_route_decisions WHERE id = ?1",
            [decision],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, task_ref);

    let (ty, body) = route
        .call(
            "route.report",
            &json!({
                "decision_id": decision, "outcome": "completed", "usage": {"output_tokens": 50},
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{body}");
    let (stored, workload): (String, String) = connection
        .query_row(
            "SELECT task_id, task_workload FROM model_route_usage WHERE id = ?1",
            [format!("routing-v1-report:{decision}")],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, task_ref);
    assert_eq!(workload, fixture::SUMMARY);
}

#[tokio::test]
async fn select_without_task_ref_is_accepted_and_correlation_id_stays_log_only() {
    let (harness, route) = configured().await;
    let (ty, body) = route
        .call(
            "route.select",
            &json!({
                "consumer": fixture::SUMMARY, "requirements": {"iq": 50},
                "correlation_id": "not-a-task",
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{body}");
    let decision = body["result"]["decision_id"].as_str().unwrap();
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    let stored: Option<String> = connection
        .query_row(
            "SELECT task_id FROM model_route_decisions WHERE id = ?1",
            [decision],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, None);
    let (ty, body) = route
        .call(
            "route.report",
            &json!({
                "decision_id": decision, "outcome": "completed", "usage": {"output_tokens": 1},
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{body}");
    let stored: Option<String> = connection
        .query_row(
            "SELECT task_id FROM model_route_usage WHERE id = ?1",
            [format!("routing-v1-report:{decision}")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, None);
}

#[tokio::test]
async fn task_ref_accepts_printable_ascii_at_both_length_boundaries() {
    let (_harness, route) = configured().await;
    for task_ref in [
        "!".to_owned(),
        format!(
            "{}{}",
            (b'!'..=b'~').map(char::from).collect::<String>(),
            "~".repeat(34)
        ),
    ] {
        let (ty, body) = route
            .call(
                "route.select",
                &json!({
                    "consumer": fixture::SUMMARY, "requirements": {"iq": 50}, "task_ref": task_ref,
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Response, "{body}");
    }
}

#[tokio::test]
async fn invalid_task_ref_is_refused_by_name() {
    let (_harness, route) = configured().await;
    for task_ref in [
        json!(""),
        json!("x".repeat(129)),
        json!("has space"),
        json!("\t"),
        json!("\n"),
        json!("\u{1f}"),
        json!("\u{7f}"),
        json!("é"),
        json!(42),
        json!(false),
        json!([]),
        json!({}),
        Value::Null,
    ] {
        let (ty, body) = route
            .call(
                "route.select",
                &json!({
                    "consumer": fixture::SUMMARY, "requirements": {"iq": 50}, "task_ref": task_ref,
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{task_ref}: {body}");
        assert_eq!(body["code"], "invalid_request", "{body}");
        assert!(
            body["message"].as_str().unwrap().contains("task_ref"),
            "{body}"
        );
    }
}

#[tokio::test]
async fn role_describe_declares_select_task_ref() {
    let (_harness, route) = configured().await;
    let (ty, body) = route.call("role.describe", &json!({})).await;
    assert_eq!(ty, FrameType::Response, "{body}");
    assert!(
        body["result"]["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("select_task_ref")),
        "{body}"
    );
}
