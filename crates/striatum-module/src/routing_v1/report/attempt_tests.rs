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
async fn attempt_member_is_advertised_and_echoes_every_companion() {
    let (harness, route) = configured().await;
    let id = decision(&route, "production-attempt").await;
    assert!(
        result(route.call("role.describe", &json!({})).await)["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(REPORT_ATTEMPT_OUTCOMES))
    );
    let before = snapshot(&harness);
    for attempt in [json!({"status":12})] {
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
    assert_eq!(
        result(
            route
                .call("route.report", &report(&id, "rejected", "cancelled", None))
                .await
        )["outcome"],
        "cancelled"
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
            let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
            let ledger: String = conn
                .query_row(
                    "SELECT outcome FROM model_route_decisions WHERE id=?1",
                    [id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                ledger,
                match status {
                    "served" | "served_empty" => "completed",
                    "rejected" | "future-status" => "cancelled",
                    _ => "error",
                }
            );
            for table in [
                "model_route_state",
                "account_route_fence",
                "credential_entitlement_fences",
            ] {
                assert_eq!(
                    snapshot(&harness).iter().find(|(name, _)| name == table),
                    before.iter().find(|(name, _)| name == table),
                    "engine {status}: {table}"
                );
            }
            let before = snapshot(&harness);
            assert_eq!(result(owner.call("route.report", &body).await), answer);
            assert_eq!(snapshot(&harness), before);
        }
    })
    .await;
}

const EFFECT_NOW: u64 = 1_800_000_000_000;

fn effect_snapshot(harness: &Harness) -> Vec<(String, Vec<Vec<SqlValue>>)> {
    let mut rows = snapshot(harness);
    rows.retain(|(table, _)| {
        !matches!(
            table.as_str(),
            "routing_v1_report_delivery"
                | "routing_v1_report"
                | "model_route_decisions"
                | "routing_call_window"
                | "cortexkit_schema_version"
        )
    });
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let ledger = conn
        .prepare("SELECT outcome,outcome_at FROM model_route_decisions ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok(vec![row.get(0)?, row.get(1)?]))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    rows.push(("ledger_outcome".into(), ledger));
    rows
}

// Supplemental presence counts use block rows where source_present=1.
// The complete snapshots still compare every persisted table and column.
fn source_snapshot(harness: &Harness) -> Vec<Vec<SqlValue>> {
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let mut query = conn
        .prepare(
            "SELECT kind,* FROM block WHERE source_present=1 ORDER BY kind,source_order,origin_key",
        )
        .unwrap();
    let columns = query.column_count();
    let rows = query
        .query_map([], |row| {
            (0..columns).map(|column| row.get(column)).collect()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    rows
}

struct EffectCase {
    before: Vec<(String, Vec<Vec<SqlValue>>)>,
    after: Vec<(String, Vec<Vec<SqlValue>>)>,
    sources: Vec<Vec<SqlValue>>,
    before_sources: Vec<Vec<SqlValue>>,
}

impl EffectCase {
    fn source_rows_before(&self, kinds: &[&str]) -> Vec<&Vec<SqlValue>> {
        self.before_sources
            .iter()
            .filter(|row| match row.first() {
                Some(SqlValue::Text(kind)) => kinds.contains(&kind.as_str()),
                _ => panic!("the explicit kind column must be text"),
            })
            .collect()
    }

    fn source_rows(&self, kinds: &[&str]) -> Vec<&Vec<SqlValue>> {
        self.sources
            .iter()
            .filter(|row| match row.first() {
                Some(SqlValue::Text(kind)) => kinds.contains(&kind.as_str()),
                _ => panic!("the explicit kind column must be text"),
            })
            .collect()
    }
}

// Each report gets its own deterministic store so the comparison shares no state.
async fn effect_case(status: Option<&str>, outcome: &str, extra: Value, seed: bool) -> EffectCase {
    let _clock = crate::InjectedClock::set(EFFECT_NOW);
    let _inputs = striatum_store::replay::Inputs::new(EFFECT_NOW as i64).install();
    let (harness, route) = configured().await;
    let id = decision(&route, "effect-twin").await;
    let shared = harness.credentials();
    let credential = shared
        .lock()
        .unwrap()
        .listed_credentials()
        .iter()
        .find(|row| {
            row.provider_ids
                .iter()
                .any(|provider| provider == "example-subscription")
        })
        .expect("the subscription fixture has a serving credential")
        .id
        .clone();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    // The twins need identical input cells, including the normally random
    // file identity. Leave it in the whole-state comparison after seeding.
    let identity = format!("{EFFECT_NOW:032x}");
    let archive =
        rusqlite::Connection::open(harness.store_path().with_file_name("history.db")).unwrap();
    assert_eq!(
        archive
            .execute("UPDATE history_binding SET split_identity=?1", [&identity])
            .unwrap(),
        1
    );
    assert_eq!(
        conn.execute("UPDATE history_layout SET split_identity=?1", [&identity])
            .unwrap(),
        1
    );
    drop(archive);
    // Catalog migrations use SQLite's clock rather than the injected report clock.
    conn.execute("UPDATE model_facts SET updated_at=?1", [EFFECT_NOW as i64])
        .unwrap();
    // Nonzero route counters make accidental healing or backoff observable.
    conn.execute(
        "UPDATE model_route_state SET consecutive_terminal_failures=2,failure_count=3",
        [],
    )
    .unwrap();
    if seed {
        harness
            .store()
            .record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: "example-subscription".into(),
                    credential_id: credential.clone(),
                    code: "credential_refused".into(),
                },
                None,
                EFFECT_NOW as i64 - 100,
            )
            .unwrap();
        harness
            .store()
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "example-subscription".into(),
                credential_id: credential.clone(),
                model: "model-c".into(),
                until_ms: EFFECT_NOW as i64 + 1000,
                decision_id: id.clone(),
                recorded_at: EFFECT_NOW as i64 - 100,
            })
            .unwrap();
        // Report preparation may persist a recheck schedule for this refusal.
        // Do that through normal synchronization before taking either snapshot.
        harness
            .store()
            .account_transaction(|tx| tx.sync_fence_rechecks())
            .unwrap();
        let managed: i64 = harness.store().with_reader_for_test(|conn| conn.query_row(
            "SELECT managed FROM block WHERE kind='account_route' AND origin_key=json_array('example-subscription',?1,'model-c') AND source_present=1",
            [credential.as_str()], |row| row.get(0),
        )).unwrap();
        assert_eq!(
            managed, 1,
            "the seeded account-route clock must be registered before the test acts"
        );
    }
    let mut body = json!({"decision_id":id,"outcome":outcome,"credential_id":credential});
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    if let Some(status) = status {
        body["attempt_outcome"] = json!({"status":status});
        if let Some(wait) = body.as_object_mut().unwrap().remove("wait") {
            body["attempt_outcome"]["retry_after_ms"] = wait;
        }
    }
    let before = effect_snapshot(&harness);
    let before_sources = source_snapshot(&harness);
    let answer = result(route.call("route.report", &body).await);
    assert_eq!(answer["outcome"], outcome);
    assert_eq!(delivery_keys(&harness, &id), ["outcome"]);
    EffectCase {
        before,
        after: effect_snapshot(&harness),
        sources: source_snapshot(&harness),
        before_sources,
    }
}

#[tokio::test]
async fn attempt_effects_match_legacy_twins_and_typed_account_additions() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for status in ["served", "served_empty"] {
            let attempt = effect_case(Some(status), "completed", json!({}), true).await;
            let legacy = effect_case(None, "completed", json!({}), true).await;
            assert_eq!(attempt.after, legacy.after, "{status}");
            assert!(attempt.source_rows(&["credential"]).is_empty());
            assert!(attempt.source_rows(&["account_route"]).is_empty());
        }
        for extra in [
            json!({}),
            json!({"reset_at_ms":EFFECT_NOW+60_000}),
            json!({"eligibility":{"scope":"model","code":"not_served"}}),
            json!({"runner_code":"size_quota_429","request_input_tokens":32000}),
        ] {
            let attempt = effect_case(Some("unavailable"), "failed", extra.clone(), false).await;
            let legacy = effect_case(None, "failed", extra, false).await;
            assert_eq!(attempt.after, legacy.after, "unavailable");
            assert_ne!(attempt.before, attempt.after);
        }
        for (status, typed) in [
            ("quota", json!({"runner_code":"account_limit_exhausted"})),
            // OAuth sign-ins need reauthentication, so their status adds no credential-refusal field.
            ("credential", json!({})),
        ] {
            for extra in [
                json!({}),
                json!({"reset_at_ms":EFFECT_NOW+60_000}),
                json!({"credential_id":"not-listed"}),
                json!({"credential_id":null}),
            ] {
                let attempt = effect_case(Some(status), "failed", extra.clone(), false).await;
                let mut twin = extra.clone();
                twin.as_object_mut()
                    .unwrap()
                    .extend(typed.as_object().unwrap().clone());
                let legacy = effect_case(None, "failed", twin, false).await;
                assert_eq!(attempt.after, legacy.after, "{status}: {extra}");
                if extra.get("credential_id").is_some() {
                    assert!(attempt.source_rows(&["credential"]).is_empty());
                    assert!(attempt.source_rows(&["account_route"]).is_empty());
                    assert!(attempt
                        .source_rows(&["non_quota_route", "transient_route"])
                        .is_empty());
                } else {
                    let walls = if status == "quota" {
                        attempt.source_rows(&["account_route"]).len()
                            + attempt
                                .source_rows(&["non_quota_route", "transient_route"])
                                .len()
                    } else {
                        attempt.source_rows(&["credential"]).len()
                    };
                    assert_eq!(
                        walls,
                        usize::from(status == "quota"),
                        "typed {status} effects"
                    );
                }
            }
            let eligibility = json!({"eligibility":{"scope":"account","code":"not_served"}});
            assert_eq!(
                effect_case(Some(status), "failed", eligibility.clone(), false)
                    .await
                    .after,
                effect_case(None, "failed", eligibility, false).await.after
            );
        }
    })
    .await;
}

#[tokio::test]
async fn throttled_wait_uses_legacy_reset_evidence_without_route_backoff_or_wrap() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        for wait in [None, Some(0), Some(60000), Some(86_400_001), Some(u64::MAX)] {
            let mut extra = json!({});
            let mut legacy = json!({});
            if let Some(wait) = wait {
                extra["wait"] = json!(wait);
                // An independent expected date, including the signed-date ceiling.
                legacy["reset_at_ms"] = json!(match wait {
                    u64::MAX => i64::MAX as u64,
                    _ => EFFECT_NOW + wait,
                });
            }
            let expected = effect_case(None, "failed", legacy, false).await;
            let attempt = effect_case(Some("throttled"), "failed", extra.clone(), false).await;
            assert_eq!(attempt.after, expected.after, "wait {wait:?}");
            assert_eq!(
                attempt
                    .before
                    .iter()
                    .find(|(name, _)| name == "model_route_state"),
                attempt
                    .after
                    .iter()
                    .find(|(name, _)| name == "model_route_state")
            );
            if wait.is_some() {
                extra["reset_at_ms"] = json!(EFFECT_NOW + 999_999);
                assert_eq!(
                    effect_case(Some("throttled"), "failed", extra, false)
                        .await
                        .after,
                    expected.after
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn rejected_and_unknown_suppress_all_legacy_evidence_but_commit_cancelled() {
    with_candidate_capabilities(&[REPORT_ATTEMPT_OUTCOMES], async {
        let legacy = effect_case(None, "cancelled", json!({}), true).await;
        for (status, outcome, refusal_class) in [("rejected", "cancelled", None),
            ("future-status", "failed", Some("credential_refused")), ("future-status", "completed", None)] {
            let mut extra = json!({"runner_code":"ENTITLEMENT_ERROR","reset_at_ms":EFFECT_NOW+60_000,
                "request_input_tokens":32000});
            if let Some(class) = refusal_class { extra["refusal_class"] = json!(class); }
            let attempt = effect_case(Some(status), outcome, extra, true).await;
            assert_eq!(attempt.after, legacy.after, "{status} {outcome}");
            assert_eq!(attempt.source_rows(&["credential"]).len(), 1);
            assert_eq!(attempt.source_rows_before(&["account_route"]), attempt.source_rows(&["account_route"]), "account_route_fence");
            for table in ["model_route_state", "routing_v1_affinity"] {
                assert_eq!(attempt.before.iter().find(|(name, _)| name == table),
                    attempt.after.iter().find(|(name, _)| name == table), "{table}");
            }
        }
    }).await;
}

async fn report_credential_method(method: &str) -> (std::sync::Arc<Harness>, Route) {
    let (harness, route) = configured().await;
    let mut catalog = fixture::catalog();
    catalog.accounts.truncate(1);
    catalog.accounts[0].method = method.into();
    super::super::conformance_tests::install_catalog(&harness, &catalog);
    let credentials = harness.credentials();
    let mut credentials = credentials.lock().unwrap();
    let mut rows = credentials.listed_credentials().to_vec();
    // The vault type and opaque ID must not override the listed auth method.
    let row = rows
        .iter_mut()
        .find(|row| row.id == "credential-1")
        .unwrap();
    row.credential_type = if method == "oauth" { "apikey" } else { "oauth" }.into();
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "attempt-method-metadata".into(),
            credentials: rows,
            ..Default::default()
        },
        EFFECT_NOW,
    );
    drop(credentials);
    let selected = result(
        route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY,"requirements":{"iq":50},
                    "pin":"example-subscription/model-c","runners":[fixture::RUNNER_ONE]
                }),
            )
            .await,
    );
    assert_eq!(selected["runner"]["module"], fixture::RUNNER_ONE);
    let body = json!({"decision_id":selected["decision_id"], "outcome":"failed",
        "credential_id":"credential-1", "attempt_outcome":{"status":"credential"}});
    assert_eq!(
        result(route.call("route.report", &body).await)["outcome"],
        "failed"
    );
    (harness, route)
}

#[tokio::test]
async fn oauth_credential_attempt_does_not_fence_sign_in_for_another_runner() {
    let _clock = crate::InjectedClock::set(EFFECT_NOW);
    let (harness, route) = report_credential_method("oauth").await;
    assert!(
        harness
            .store()
            .credential_entitlement_fences()
            .unwrap()
            .is_empty(),
        "a refused sign-in must not become a credential-wide refusal"
    );
    let selected = result(
        route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY,"requirements":{"iq":50},
                    "pin":"example-subscription/model-c","runners":[fixture::RUNNER_TWO]
                }),
            )
            .await,
    );
    assert_eq!(selected["runner"]["module"], fixture::RUNNER_TWO);
    let runner = harness.bind(reserved(fixture::RUNNER_TWO), None).await;
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &json!({
                    "provider":"example-subscription","model":"model-c",
                    "decision_id":selected["decision_id"], "conversation_key":"other-runner",
                    "admission_id":"other-runner-after-sign-in-refusal"
                }),
            )
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], "credential-1");
}

#[tokio::test]
async fn apikey_credential_attempt_keeps_the_credential_refusal_effect() {
    let _clock = crate::InjectedClock::set(EFFECT_NOW);
    let (harness, _) = report_credential_method("apikey").await;
    let fences = harness.store().credential_entitlement_fences().unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].credential_id, "credential-1");
    assert_eq!(fences[0].code, "credential_refused");
}
