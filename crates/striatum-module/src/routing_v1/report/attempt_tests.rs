use super::super::conformance_tests::{reserved, Harness, Route};
use super::super::describe::with_candidate_capabilities;
use super::super::dispatch_tests::configured;
use cortexkit_role_routing::capabilities::REPORT_ATTEMPT_OUTCOMES;
use cortexkit_role_routing_conformance::fixture;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use subc_protocol::FrameType;

const COMPANIONS: &[(&str, &str)] = &[
    ("served", "completed"),
    ("served_empty", "completed"),
    ("quota", "failed"),
    ("throttled", "failed"),
    ("credential", "failed"),
    ("unavailable", "failed"),
    ("rejected", "cancelled"),
];

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

fn refusal(reply: (FrameType, Value), code: &str, field: Option<&str>) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], code, "{}", reply.1);
    if let Some(field) = field {
        assert_eq!(reply.1["detail"]["field"], field, "{}", reply.1);
    }
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

fn report(id: &str, status: &str, outcome: &str, usage: Option<Value>) -> Value {
    let mut body = json!({"decision_id":id,"outcome":outcome,"attempt_outcome":{"status":status}});
    if let Some(usage) = usage {
        body["usage"] = usage;
    }
    body
}

// Comparing all durable rows catches a refused or replayed report that still
// writes usage, an account restriction, affinity, or a delivery claim before answering.
fn snapshot(harness: &Harness) -> Vec<(String, Vec<Vec<SqlValue>>)> {
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
            let rows = query
                .query_map([], |row| {
                    (0..columns).map(|column| row.get(column)).collect()
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            (table, rows)
        })
        .collect()
}

fn delivery_keys(harness: &Harness, id: &str) -> Vec<String> {
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .prepare("SELECT delivery_key FROM routing_v1_report_delivery WHERE decision_id=?1 ORDER BY delivery_key")
        .unwrap()
        .query_map([id], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[tokio::test]
async fn attempt_member_is_candidate_only_and_echoes_every_companion() {
    let (harness, route) = configured().await;
    let id = decision(&route, "production-attempt").await;
    let before = snapshot(&harness);
    for attempt in [json!({"status":"rejected"}), Value::Null] {
        refusal(
            route
                .call(
                    "route.report",
                    &json!({"decision_id":id,"outcome":"cancelled","attempt_outcome":attempt}),
                )
                .await,
            "invalid_request",
            Some("attempt_outcome"),
        );
        assert_eq!(snapshot(&harness), before);
    }
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for (status, outcome) in COMPANIONS {
            let id = decision(&route, status).await;
            let body = report(&id, status, outcome, None);
            let answer = result(route.call("route.report", &body).await);
            assert_eq!(
                answer,
                json!({"decision_id":id,"report_id":format!("report:{id}"),"outcome":outcome})
            );
            assert_eq!(delivery_keys(&harness, &id), ["outcome"]);
        }
    })
    .await;
    refusal(
        route
            .call("route.report", &report(&id, "rejected", "cancelled", None))
            .await,
        "invalid_request",
        Some("attempt_outcome"),
    );
}

#[tokio::test]
async fn known_companions_precede_other_cross_field_checks_without_writes() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        let (harness, route) = configured().await;
        let id = decision(&route, "companion-validation").await;
        let before = snapshot(&harness);
        for (status, companion) in COMPANIONS {
            for outcome in [
                "completed",
                "failed",
                "cancelled",
                "refused",
                "future-outcome",
            ] {
                if outcome == *companion {
                    continue;
                }
                let mut body = report(&id, status, outcome, None);
                body["refusal_class"] = json!("credential_refused");
                body["eligibility"] = json!({"scope":"model","code":"not_enabled"});
                refusal(
                    route.call("route.report", &body).await,
                    "invalid_request",
                    Some("outcome"),
                );
                assert_eq!(snapshot(&harness), before, "{status} / {outcome}");
            }
        }
        let mut rejected = report(
            &id,
            "rejected",
            "cancelled",
            Some(json!({"output_tokens":17})),
        );
        rejected["refusal_class"] = json!("credential_refused");
        rejected["credential_id"] = json!("opaque");
        rejected["reset_at_ms"] = json!(super::super::now_ms() + 60_000);
        refusal(
            route.call("route.report", &rejected).await,
            "invalid_request",
            Some("refusal_class"),
        );
        assert_eq!(snapshot(&harness), before);
        let mut eligibility = report(&id, "unavailable", "failed", None);
        eligibility["eligibility"] = json!({"scope":"model","code":"not_enabled"});
        eligibility["reset_at_ms"] = json!(1);
        refusal(
            route.call("route.report", &eligibility).await,
            "invalid_request",
            Some("eligibility"),
        );
        assert_eq!(snapshot(&harness), before);
        let mut size = report(&id, "unavailable", "failed", None);
        size["refusal_class"] = json!("request_too_large");
        refusal(
            route.call("route.report", &size).await,
            "invalid_request",
            Some("request_input_tokens"),
        );
        assert_eq!(snapshot(&harness), before);
        result(
            route
                .call("route.report", &report(&id, "rejected", "cancelled", None))
                .await,
        );
    })
    .await;
}

#[tokio::test]
async fn retry_after_validation_uses_the_nested_field_path() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        let (harness, route) = configured().await;
        let id = decision(&route, "retry-validation").await;
        let before = snapshot(&harness);
        for (status, outcome) in COMPANIONS
            .iter()
            .copied()
            .chain([("future-status", "failed")])
        {
            for retry in [
                json!(-1),
                json!(1.5),
                json!("12"),
                json!(true),
                json!({}),
                json!([]),
            ] {
                let mut body = report(&id, status, outcome, None);
                body["attempt_outcome"]["retry_after_ms"] = retry;
                refusal(
                    route.call("route.report", &body).await,
                    "invalid_request",
                    Some("attempt_outcome.retry_after_ms"),
                );
                assert_eq!(snapshot(&harness), before);
            }
            if status != "throttled" {
                let mut body = report(&id, status, outcome, None);
                body["attempt_outcome"]["retry_after_ms"] = json!(0);
                refusal(
                    route.call("route.report", &body).await,
                    "invalid_request",
                    Some("attempt_outcome.retry_after_ms"),
                );
                assert_eq!(snapshot(&harness), before);
            }
        }
        for retry in [json!(0), json!(1), json!(u64::MAX), Value::Null] {
            let id = decision(&route, &format!("valid-retry-{retry}")).await;
            let mut body = report(&id, "throttled", "failed", None);
            body["attempt_outcome"]["retry_after_ms"] = retry;
            assert_eq!(
                result(route.call("route.report", &body).await)["outcome"],
                "failed"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn unknown_attempt_status_has_no_companion_check_and_echoes_legacy_outcome() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        let (harness, route) = configured().await;
        for outcome in [
            "completed",
            "failed",
            "cancelled",
            "refused",
            "future-outcome",
        ] {
            let id = decision(&route, outcome).await;
            let mut body = report(&id, "future-status", outcome, None);
            if outcome == "failed" || outcome == "refused" {
                body["refusal_class"] = json!("credential_refused");
            }
            assert_eq!(
                result(route.call("route.report", &body).await)["outcome"],
                outcome
            );
        }
        let id = decision(&route, "unknown-cross-field").await;
        let before = snapshot(&harness);
        let mut body = report(&id, "future-status", "cancelled", None);
        body["refusal_class"] = json!("credential_refused");
        refusal(
            route.call("route.report", &body).await,
            "invalid_request",
            Some("refusal_class"),
        );
        assert_eq!(snapshot(&harness), before);
    })
    .await;
}

#[tokio::test]
async fn attempt_delivery_keys_replay_first_answers_and_conflict_without_writes() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for (key, usage) in [
            ("outcome", None),
            ("usage", Some(json!({"output_tokens":10}))),
            ("run:one", Some(json!({"run_id":"one","output_tokens":10}))),
        ] {
            let (harness, route) = configured().await;
            let id = decision(&route, key).await;
            let body = report(&id, "rejected", "cancelled", usage);
            let first = result(route.call("route.report", &body).await);
            let report_id = if key.starts_with("run:") {
                format!("report:{id}:{key}")
            } else {
                format!("report:{id}")
            };
            assert_eq!(
                first,
                json!({"decision_id":id,"report_id":report_id,"outcome":"cancelled"})
            );
            assert_eq!(delivery_keys(&harness, &id), [key]);
            let before = snapshot(&harness);
            assert_eq!(result(route.call("route.report", &body).await), first);
            assert_eq!(snapshot(&harness), before);
            let mut null_retry = body.clone();
            null_retry["attempt_outcome"]["retry_after_ms"] = Value::Null;
            assert_eq!(result(route.call("route.report", &null_retry).await), first);
            assert_eq!(snapshot(&harness), before);
            let mut changed = body.clone();
            changed["outcome"] = json!("completed");
            changed["attempt_outcome"]["status"] = json!("served");
            if key.starts_with("run:") {
                changed["usage"]["output_tokens"] = json!(99);
                assert_eq!(result(route.call("route.report", &changed).await), first);
            } else {
                refusal(
                    route.call("route.report", &changed).await,
                    "report_conflict",
                    None,
                );
                let mut status_only = body.clone();
                status_only["attempt_outcome"]["status"] = json!("future-status");
                refusal(
                    route.call("route.report", &status_only).await,
                    "report_conflict",
                    None,
                );
            }
            assert_eq!(snapshot(&harness), before);
        }
    })
    .await;
}

#[tokio::test]
async fn attempt_usage_identity_conflicts_with_outcome_and_run_in_both_orders() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for other in [None, Some(json!({"run_id":"one","output_tokens":10}))] {
            for usage_first in [false, true] {
                let (harness, route) = configured().await;
                let id = decision(&route, "mixed-attempt-identity").await;
                let usage = report(
                    &id,
                    "rejected",
                    "cancelled",
                    Some(json!({"output_tokens":10})),
                );
                let other = report(&id, "rejected", "cancelled", other.clone());
                let (first, second) = if usage_first {
                    (&usage, &other)
                } else {
                    (&other, &usage)
                };
                result(route.call("route.report", first).await);
                let before = snapshot(&harness);
                refusal(
                    route.call("route.report", second).await,
                    "report_conflict",
                    None,
                );
                assert_eq!(snapshot(&harness), before);
            }
        }
    })
    .await;
}

#[tokio::test]
async fn later_attempt_runs_are_recorded_without_replacing_first_effects() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for first_usage in [None, Some(json!({"run_id":"first","output_tokens":10}))] {
            let (harness, route) = configured().await;
            let id = decision(&route, "later-attempt-run").await;
            harness.store().record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: "example-subscription".into(),
                    credential_id: "opaque".into(),
                    code: "credential_refused".into(),
                },
                None,
                super::super::now_ms() as i64,
            ).unwrap();
            let first = report(&id, "rejected", "cancelled", first_usage);
            result(route.call("route.report", &first).await);
            let fences = harness.store().credential_entitlement_fences().unwrap();
            assert_eq!(fences.len(), 1);
            let before = snapshot(&harness);
            let mut second = report(&id, "served", "completed", Some(json!({"run_id":"second","output_tokens":20})));
            second["credential_id"] = json!("opaque");
            let answer = result(route.call("route.report", &second).await);
            assert_eq!(answer, json!({"decision_id":id,"report_id":format!("report:{id}:run:second"),"outcome":"completed"}));
            let after = snapshot(&harness);
            let effects_only = |rows: &Vec<(String, Vec<Vec<SqlValue>>)>| {
                rows.iter().filter(|(table, _)| !matches!(table.as_str(),
                    "routing_v1_report_delivery" | "model_route_usage")).cloned().collect::<Vec<_>>()
            };
            assert_eq!(effects_only(&after), effects_only(&before));
            assert_eq!(harness.store().credential_entitlement_fences().unwrap(), fences);
            let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
            let ledger: String = conn.query_row("SELECT outcome FROM model_route_decisions WHERE id=?1", [&id], |row| row.get(0)).unwrap();
            assert_eq!(ledger, "cancelled");
            let (deliveries, reports, output): (i64, i64, i64) = conn.query_row(
                "SELECT (SELECT count(*) FROM routing_v1_report_delivery WHERE decision_id=?1),
                    (SELECT count(*) FROM routing_v1_report WHERE decision_id=?1),
                    (SELECT sum(output_tokens) FROM model_route_usage)",
                [&id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).unwrap();
            assert_eq!((deliveries, reports), (2, 1));
            assert_eq!(output, if first["usage"].is_null() { 20 } else { 30 });
            assert_eq!(result(route.call("route.report", &second).await), answer);
            assert_eq!(snapshot(&harness), after);
        }
    }).await;
}

#[tokio::test]
async fn engine_decisions_use_the_same_attempt_parser_and_report_identity() {
    let harness = Harness::new("engine-attempts");
    harness.edit_config(|config| {
        config["consumers"] = json!({"colliculus":{"owner":"colliculus"}});
    });
    super::super::consumers::replace_catalog(&json!([{
        "module_id":"colliculus","nonce_verified":true,
        "role_declarations":{"media-provider/v1":{"engines":[{
            "kind":"transcribe","provider":"media-vendor","model":"local-transcriber","local":true,
            "formats":["audio/wav"],"languages":["en"],"latency_class":"interactive"
        }]}}
    }]));
    let owner = harness.bind(reserved("colliculus"), None).await;
    let other = harness.bind(reserved("other"), None).await;
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for (status, outcome) in COMPANIONS
            .iter()
            .copied()
            .chain([("future-status", "refused")])
        {
            let selected = result(owner.call("route.select_engine", &json!({
                "consumer":"colliculus","kind":"transcribe","requirements":{},"privacy":"local_only"
            })).await);
            let id = selected["decision_id"].as_str().unwrap();
            let body = report(id, status, outcome, None);
            let before = snapshot(&harness);
            refusal(
                other.call("route.report", &body).await,
                "unknown_decision",
                None,
            );
            assert_eq!(snapshot(&harness), before);
            let mut invalid = body.clone();
            invalid["attempt_outcome"]["retry_after_ms"] = json!("not-a-duration");
            refusal(
                owner.call("route.report", &invalid).await,
                "invalid_request",
                Some("attempt_outcome.retry_after_ms"),
            );
            assert_eq!(snapshot(&harness), before);
            if status == "rejected" {
                invalid = body.clone();
                invalid["refusal_class"] = json!("credential_refused");
                refusal(
                    owner.call("route.report", &invalid).await,
                    "invalid_request",
                    Some("refusal_class"),
                );
                assert_eq!(snapshot(&harness), before);
            }
            let answer = result(owner.call("route.report", &body).await);
            assert_eq!(answer["outcome"], outcome);
            let before = snapshot(&harness);
            assert_eq!(result(owner.call("route.report", &body).await), answer);
            assert_eq!(snapshot(&harness), before);
        }
    })
    .await;
}
