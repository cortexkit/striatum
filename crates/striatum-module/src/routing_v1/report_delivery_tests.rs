use super::conformance_tests::{reserved, Route};
use super::dispatch_tests::configured;
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

async fn decision(route: &Route, affinity: &str) -> String {
    result(
        route
            .call(
                "route.select",
                &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},
        "pin":"example-subscription/model-c","affinity":affinity}),
            )
            .await,
    )["decision_id"]
        .as_str()
        .unwrap()
        .into()
}

#[tokio::test]
async fn summary_request_reports_record_usage_without_repeating_effects() {
    use rusqlite::types::Value as SqlValue;

    let (harness, route) = configured().await;
    let id = decision(&route, "summary-request-reports").await;
    let bodies = [
        r#"{"decision_id":"decision-1","outcome":"completed","usage":{"cached_input_tokens":20,"input_tokens":100,"output_tokens":10,"run_id":"head:mc-summary:1-4:decision-1:turn-one:turn1"}}"#,
        r#"{"decision_id":"decision-1","outcome":"completed","usage":{"cached_input_tokens":20,"input_tokens":100,"output_tokens":10,"run_id":"head:mc-summary:1-4:decision-1:turn-one:turn2"}}"#,
        r#"{"decision_id":"decision-1","outcome":"completed","usage":{"cached_input_tokens":20,"input_tokens":100,"output_tokens":10,"run_id":"head:mc-summary:1-4:decision-1:turn-one:check:stable-check"}}"#,
    ].map(|body| body.replace("decision-1", &id));
    let snapshot = || {
        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        tables
            .into_iter()
            .map(|table| {
                let mut query = conn
                    .prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))
                    .unwrap();
                let columns = query.column_count();
                let rows: Vec<Vec<SqlValue>> = query
                    .query_map([], |row| {
                        (0..columns).map(|column| row.get(column)).collect()
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                (table, rows)
            })
            .collect::<Vec<_>>()
    };
    let first = route
        .call_raw_params("route.report", bodies[0].as_bytes())
        .await;
    assert_eq!(first.0, FrameType::Response, "{:?}", first.1);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let ledger = || -> String {
        conn.query_row(
            "SELECT outcome FROM model_route_decisions WHERE id=?1",
            [&id],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(ledger(), "completed");
    let before = snapshot();
    for body in &bodies[1..] {
        let answer = route.call_raw_params("route.report", body.as_bytes()).await;
        assert_eq!(answer.0, FrameType::Response, "{:?}", answer.1);
    }
    let after = snapshot();
    let effects_only = |rows: &Vec<(String, Vec<Vec<SqlValue>>)>| {
        rows.iter()
            .filter(|(table, _)| {
                !matches!(
                    table.as_str(),
                    "routing_v1_report_delivery" | "model_route_usage"
                )
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(effects_only(&after), effects_only(&before));
    assert_eq!(ledger(), "completed");
    let totals: (i64, i64, i64, i64) = conn.query_row(
        "SELECT count(*), sum(input_tokens), sum(cached_read_tokens), sum(output_tokens) FROM model_route_usage WHERE substr(id,1,length(?1))=?1",
        [format!("routing-v1-report:{id}:run:")], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).unwrap();
    assert_eq!(totals, (3, 300, 60, 30));
    assert_eq!(
        route
            .call_raw_params("route.report", bodies[0].as_bytes())
            .await,
        first
    );
    assert_eq!(snapshot(), after);

    let legacy_id = decision(&route, "summary-legacy-conflict").await;
    result(route.call("route.report", &json!({"decision_id":legacy_id,"outcome":"completed","usage":{"cached_input_tokens":20,"input_tokens":100,"output_tokens":10}})).await);
    let legacy_run_body = bodies[0].replace(&id, &legacy_id);
    let conflict = route
        .call_raw_params("route.report", legacy_run_body.as_bytes())
        .await;
    assert_eq!(conflict.0, FrameType::Error);
    let error: Value = serde_json::from_slice(&conflict.1).unwrap();
    assert_eq!(error["code"], "report_conflict");
}

#[tokio::test]
async fn a_replayed_report_returns_identical_bytes_without_preparing_effects() {
    use crate::test_thread_state::{LifecycleProbe, LIFECYCLE_PROBE};
    use std::sync::{atomic::Ordering::Relaxed, Arc};
    let (_harness, route) = configured().await;
    let id = decision(&route, "prepare-replay").await;
    let body = json!({"decision_id":id,"outcome":"completed","usage":{"run_id":"replay","output_tokens":20}});
    let probe = Arc::new(LifecycleProbe::default());
    LIFECYCLE_PROBE.with(|slot| slot.replace(Some(probe.clone())));
    let first = route.call_raw("route.report", &body).await;
    assert_eq!(first.0, FrameType::Response);
    assert_eq!(probe.report_snapshots.swap(0, Relaxed), 1);
    assert!(probe.report_catalog_reads.swap(0, Relaxed) >= 1);
    assert_eq!(route.call_raw("route.report", &body).await, first);
    assert_eq!(
        (
            probe.report_snapshots.load(Relaxed),
            probe.report_catalog_reads.load(Relaxed)
        ),
        (0, 0)
    );
}

#[tokio::test]
pub(super) async fn report_run_repeat_returns_first_answer_and_usage_but_new_runs_count() {
    let (harness, route) = configured().await;
    let id = decision(&route, "repeat").await;
    let body =
        json!({"decision_id":id,"outcome":"completed","usage":{"run_id":"run","output_tokens":20}});
    let first = result(route.call("route.report", &body).await);
    let changed = json!({"decision_id":id,"outcome":"failed","reset_at_ms":super::now_ms()+10000,
        "usage":{"run_id":"run","output_tokens":99}});
    assert_eq!(result(route.call("route.report", &changed).await), first);
    result(route.call("route.report",&json!({"decision_id":id,"outcome":"failed","usage":{"run_id":"second","output_tokens":30}})).await);
    result(
        route
            .call(
                "route.report",
                &json!({"decision_id":id,"outcome":"cancelled"}),
            )
            .await,
    );
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let totals: (i64, i64) = conn
        .query_row(
            "SELECT count(*),sum(output_tokens) FROM model_route_usage",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(totals, (2, 50));
    let outcome: String = conn
        .query_row(
            "SELECT outcome FROM model_route_decisions WHERE id=?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "completed");
    assert!(harness
        .store()
        .account_route_fences(super::now_ms() as i64)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn outcome_only_idempotency_is_separate_from_run_usage_in_both_orders() {
    for usage_first in [false, true] {
        let (_harness, route) = configured().await;
        let id = decision(&route, "order").await;
        let outcome = json!({"decision_id":id,"outcome":"completed"});
        let usage = json!({"decision_id":id,"outcome":"failed","usage":{"run_id":"late","output_tokens":10}});
        let (first, second) = if usage_first {
            (&usage, &outcome)
        } else {
            (&outcome, &usage)
        };
        result(route.call("route.report", first).await);
        result(route.call("route.report", second).await);
        let standing = result(route.call("route.report", &outcome).await);
        assert_eq!(
            result(
                route
                    .call(
                        "route.report",
                        &json!({"decision_id":id,"outcome":"completed","usage":null})
                    )
                    .await
            ),
            standing
        );
        let conflict = route
            .call(
                "route.report",
                &json!({"decision_id":id,"outcome":"cancelled"}),
            )
            .await;
        assert_eq!(conflict.0, FrameType::Error);
        assert_eq!(conflict.1["code"], "report_conflict");
    }
}

#[tokio::test]
async fn mixed_run_and_legacy_usage_conflict_in_both_orders() {
    for run_first in [false, true] {
        let (_harness, route) = configured().await;
        let id = decision(&route, "mixed").await;
        let legacy = json!({"decision_id":id,"outcome":"completed","usage":{"output_tokens":10}});
        let run = json!({"decision_id":id,"outcome":"completed","usage":{"run_id":"r","output_tokens":10}});
        let (first, second) = if run_first {
            (&run, &legacy)
        } else {
            (&legacy, &run)
        };
        result(route.call("route.report", first).await);
        let conflict = route.call("route.report", second).await;
        assert_eq!(conflict.0, FrameType::Error);
        assert_eq!(conflict.1["code"], "report_conflict");
    }
}

#[tokio::test]
async fn report_unknown_members_are_refused_with_exact_paths() {
    let (_harness, route) = configured().await;
    let id = decision(&route, "unknown").await;
    for (extra, field) in [
        (json!({"surprise":true}), "surprise"),
        (json!({"usage":{"gpu_count":1}}), "usage"),
    ] {
        let mut body = json!({"decision_id":id,"outcome":"completed"});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let refusal = route.call("route.report", &body).await;
        assert_eq!(refusal.0, FrameType::Error);
        assert_eq!(refusal.1["code"], "invalid_request");
        assert_eq!(refusal.1["detail"]["field"], field);
    }
}

#[tokio::test]
async fn declared_report_outcomes_keep_affinity_without_penalty() {
    let (harness, route) = configured().await;
    let describe = result(route.call("role.describe", &json!({})).await);
    for group in ["report_outcomes", "report_usage_run_id"] {
        assert!(describe["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(group)));
    }
    let mut cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../fixtures/replay/report-outcomes-difference-8.json"
    ))
    .unwrap();
    cases.push(json!({"outcome":"cancelled","availability_penalty":false,"affinity":"kept","after_ledger":"cancelled"}));
    for case in cases {
        let outcome = case["outcome"].as_str().unwrap();
        assert_eq!(case["availability_penalty"], false);
        assert_eq!(case["affinity"], "kept");
        let id = decision(&route, outcome).await;
        result(
            route
                .call("route.report", &json!({"decision_id":id,"outcome":outcome}))
                .await,
        );
        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        let ended: Option<i64> = conn
            .query_row(
                "SELECT ended_at_ms FROM routing_v1_affinity WHERE decision_id=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ended, None, "{outcome}");
        let ledger: String = conn
            .query_row(
                "SELECT outcome FROM model_route_decisions WHERE id=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ledger, case["after_ledger"].as_str().unwrap());
        let failures: i64 = conn
            .query_row(
                "SELECT COALESCE(sum(failure_count),0) FROM model_route_state",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(failures, 0, "{outcome}");
    }
}

#[tokio::test]
async fn routing_consumer_legacy_calls_and_mixed_shapes_are_counted_by_principal() {
    let (harness, route) = configured().await;
    let consumer_body: Value = serde_json::from_str(include_str!(
        "../../fixtures/replay/clients/routing-consumer/route.select-1.json"
    ))
    .unwrap();
    route.call("route.select", &consumer_body).await;
    route
        .call(
            "route.select",
            &json!({"targetAgent":"mason","settings":{}}),
        )
        .await;
    let other = harness.bind(reserved("prefrontal"), None).await;
    other
        .call(
            "route.model_fact",
            &json!({"providerID":"example","modelID":"m"}),
        )
        .await;
    let counts = harness.store().routing_call_counts().unwrap();
    assert!(counts.counts.iter().any(|c| c.method == "route.select"
        && c.shape == "legacy"
        && c.principal == format!("reserved:{}", fixture::CONSUMER)
        && c.calls == 1));
    assert!(counts
        .counts
        .iter()
        .any(|c| c.method == "route.select" && c.shape == "mixed" && c.calls == 1));
    assert!(counts.counts.iter().any(|c| c.method == "route.model_fact"
        && c.shape == "legacy"
        && c.principal == "reserved:prefrontal"
        && c.calls == 1));
}
