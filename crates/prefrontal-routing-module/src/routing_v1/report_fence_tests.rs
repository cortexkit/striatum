//! What a `route.report` leaves behind: a failed report's reset fences one
//! route on one account, its usage joins the route's usage history, and the
//! runner's `run_finished` releases the account pin. Driven over the module's
//! frame handler with the conformance kit's consumers and catalog, whose
//! subscription route `example-subscription/model-c` is served by
//! `account-1` and `account-2`.

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use super::dispatch_tests::configured;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// The principal the allocator records `runner-one`'s admissions under.
const RUNNER_PRINCIPAL: &str = "reserved:runner-one";

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

fn admission(admission: &str, model: &str, account: &Value) -> Value {
    json!({
        "provider": fixture::SUBSCRIPTION_PROVIDER, "model": model,
        "conversation_key": admission, "admission_id": admission,
        "candidates": {"view_digest": "test", "accounts": [{
            "account_id": account["account_id"], "credential_id": account["credential_id"],
        }]},
    })
}

/// A select pinned to model-c and the runner's admission naming it: the
/// decision id and the account that serves it.
async fn served_decision(route: &Route, runner: &Route, admission_id: &str) -> (String, Value) {
    let selected = result(
        route
            .call(
                "route.select",
                &json!({"consumer": fixture::SUMMARY, "requirements": {"iq": 50},
                    "pin": "example-subscription/model-c"}),
            )
            .await,
    );
    let decision = selected["decision_id"].as_str().unwrap().to_owned();
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &json!({
                    "provider": fixture::SUBSCRIPTION_PROVIDER, "model": "model-c",
                    "conversation_key": admission_id, "admission_id": admission_id,
                    "decision_id": decision,
                }),
            )
            .await,
    );
    (decision, admitted["account"].clone())
}

async fn report_failed(route: &Route, decision: &str, account: &Value, reset_at_ms: i64) {
    result(
        route
            .call(
                "route.report",
                &json!({"decision_id": decision, "outcome": "failed",
                    "credential_id": account["credential_id"], "reset_at_ms": reset_at_ms}),
            )
            .await,
    );
}

fn other_account(account: &Value) -> Value {
    let other = fixture::catalog()
        .accounts
        .into_iter()
        .find(|other| other.account_id != account["account_id"])
        .unwrap();
    json!({"account_id": other.account_id, "credential_id": other.credential_id})
}

async fn runner(harness: &std::sync::Arc<Harness>) -> Route {
    harness.bind(reserved(fixture::RUNNER_ONE), None).await
}

#[tokio::test]
async fn a_failed_report_with_a_future_reset_fences_only_that_route_on_that_account() {
    let (harness, route) = configured().await;
    let runner = runner(&harness).await;
    let (decision, account) = served_decision(&route, &runner, "fence-first").await;
    let reset = crate::account_selection::now_ms() + 600_000;
    report_failed(&route, &decision, &account, reset).await;

    let fenced = runner
        .call(
            "route.account_for",
            &admission("fence-same", "model-c", &account),
        )
        .await;
    assert_eq!(fenced.0, FrameType::Error, "{}", fenced.1);
    assert_eq!(fenced.1["code"], "account_pool_exhausted", "{}", fenced.1);
    assert_eq!(
        fenced.1["detail"]["reset_at_ms"],
        json!(reset),
        "{}",
        fenced.1
    );

    let other = other_account(&account);
    result(
        runner
            .call(
                "route.account_for",
                &admission("fence-other-account", "model-c", &other),
            )
            .await,
    );
    result(
        runner
            .call(
                "route.account_for",
                &admission("fence-other-route", "model-d", &account),
            )
            .await,
    );
    let fences = harness
        .store()
        .non_quota_account_route_fences(crate::account_selection::now_ms())
        .unwrap();
    assert_eq!(fences.len(), 1, "{fences:?}");
    assert_eq!(fences[0].model, "model-c");
    assert_eq!(fences[0].credential_id, account["credential_id"]);
    assert_eq!(fences[0].decision_id, decision);
}

#[tokio::test]
async fn a_contradicted_quota_reset_is_a_non_quota_wall_capped_at_one_hour() {
    let (harness, route) = configured().await;
    let runner = runner(&harness).await;
    let (decision, account) = served_decision(&route, &runner, "cap-first").await;
    let before = crate::account_selection::now_ms();
    report_failed(&route, &decision, &account, before + 2 * DAY_MS).await;
    let after = crate::account_selection::now_ms();
    let fences = harness
        .store()
        .non_quota_account_route_fences(after)
        .unwrap();
    assert_eq!(fences.len(), 1, "{fences:?}");
    assert!(
        (before + 3_600_000..=after + 3_600_000).contains(&fences[0].until_ms),
        "{fences:?}"
    );
}

#[tokio::test]
async fn a_past_reset_or_another_outcome_records_the_report_and_fences_nothing() {
    let (harness, route) = configured().await;
    let runner = runner(&harness).await;
    let (decision, account) = served_decision(&route, &runner, "past-first").await;
    let now = crate::account_selection::now_ms();
    report_failed(&route, &decision, &account, now - 6_000).await;
    let (refused, _) = served_decision(&route, &runner, "refused-first").await;
    result(
        route
            .call(
                "route.report",
                &json!({"decision_id": refused, "outcome": "refused",
                    "credential_id": account["credential_id"], "reset_at_ms": now + 600_000}),
            )
            .await,
    );
    assert!(harness
        .store()
        .account_route_fences(crate::account_selection::now_ms())
        .unwrap()
        .is_empty());
    assert!(harness.store().v1_report(&decision).unwrap().is_some());
    result(
        runner
            .call(
                "route.account_for",
                &admission("past-same", "model-c", &account),
            )
            .await,
    );
}

/// One column of the usage-history row a report wrote.
fn usage_column(harness: &Harness, decision: &str, column: &str) -> Option<i64> {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    connection
        .query_row(
            &format!("SELECT {column} FROM model_route_usage WHERE id = ?1"),
            [format!("routing-v1-report:{decision}")],
            |row| row.get::<_, Option<f64>>(0),
        )
        .unwrap()
        .map(|value| value as i64)
}

/// Whether the report on `decision` wrote a usage-history row.
fn usage_row_exists(harness: &Harness, decision: &str) -> bool {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM model_route_usage WHERE id = ?1)",
            [format!("routing-v1-report:{decision}")],
            |row| row.get(0),
        )
        .unwrap()
}

/// How many usage rows of the route `decision`'s report row is filed under
/// the observed-speed query takes as samples, over all time and with no
/// minimum output or duration. Usage rows count only toward the standard
/// tier and speed, so the query is asked for that population.
fn speed_samples(harness: &Harness, decision: &str) -> usize {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    let route_key: String = connection
        .query_row(
            "SELECT route_key FROM model_route_usage WHERE id = ?1",
            [format!("routing-v1-report:{decision}")],
            |row| row.get(0),
        )
        .unwrap();
    let mut statement = connection
        .prepare(prefrontal_routing_store::OBSERVED_SPEED_SAMPLES_SQL)
        .unwrap();
    statement
        .query_map(
            rusqlite::params![
                route_key,
                i64::MIN,
                i64::MAX,
                0,
                0,
                1_000,
                "standard",
                "standard"
            ],
            |_| Ok(()),
        )
        .unwrap()
        .count()
}

#[tokio::test]
async fn report_usage_joins_usage_history_and_an_absent_count_stays_unknown() {
    let (harness, route) = configured().await;
    let decision = result(
        route
            .call(
                "route.select",
                &json!({"consumer": fixture::SUMMARY, "requirements": {"iq": 50}}),
            )
            .await,
    )["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    result(
        route
            .call(
                "route.report",
                &json!({"decision_id": decision, "outcome": "completed", "usage": {
                    "input_tokens": 100, "cached_input_tokens": 30, "output_tokens": 50,
                    "reasoning_tokens": 10, "duration_ms": 5_000, "first_token_ms": 400,
                }}),
            )
            .await,
    );
    assert_eq!(usage_column(&harness, &decision, "input_tokens"), Some(100));
    assert_eq!(
        usage_column(&harness, &decision, "cached_read_tokens"),
        Some(30)
    );
    assert_eq!(
        usage_column(&harness, &decision, "output_tokens"),
        Some(50),
        "reasoning is inside output on the wire and in the history"
    );
    assert_eq!(
        usage_column(&harness, &decision, "reasoning_tokens"),
        Some(10)
    );
    assert_eq!(
        usage_column(&harness, &decision, "cache_write_tokens"),
        None,
        "a count the report leaves out is unknown, not zero"
    );
    assert_eq!(
        usage_column(&harness, &decision, "duration_ms"),
        None,
        "a run's wall clock is never a speed sample"
    );
    assert_eq!(
        speed_samples(&harness, &decision),
        0,
        "a report's usage row is never a speed sample"
    );
}

#[tokio::test]
async fn a_report_without_output_adds_no_usage_row_rather_than_a_zero_output() {
    let (harness, route) = configured().await;
    let decision = result(
        route
            .call(
                "route.select",
                &json!({"consumer": fixture::SUMMARY, "requirements": {"iq": 50}}),
            )
            .await,
    )["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    result(
        route
            .call(
                "route.report",
                &json!({"decision_id": decision, "outcome": "completed", "usage": {
                    "input_tokens": 100, "cached_input_tokens": 30, "duration_ms": 5_000,
                }}),
            )
            .await,
    );
    assert!(harness.store().v1_report(&decision).unwrap().is_some());
    assert!(
        !usage_row_exists(&harness, &decision),
        "an unknown output must not enter the history as zero"
    );
}

#[tokio::test]
async fn run_finished_releases_the_runner_account_pin() {
    let (harness, route) = configured().await;
    let runner = runner(&harness).await;
    let (_decision, _account) = served_decision(&route, &runner, "finish-admission").await;
    result(
        runner
            .call(
                "route.account_observe",
                &json!({"event_id": "finish-1", "kind": "admission_started",
                    "admission_id": "finish-admission", "run_id": "finish-run"}),
            )
            .await,
    );
    let finished = result(
        runner
            .call(
                "route.account_observe",
                &json!({"event_id": "finish-2", "kind": "run_finished",
                    "admission_id": "finish-admission", "run_id": "finish-run"}),
            )
            .await,
    );
    assert_eq!(finished["observation"]["bound"], true, "{finished}");
    let now = crate::account_selection::now_ms();
    let (state, lease_until) = harness
        .store()
        .account_transaction(|tx| {
            let row = tx.admission(RUNNER_PRINCIPAL, "finish-admission")?.unwrap();
            let affinity = tx
                .affinity(RUNNER_PRINCIPAL, &row.conversation_hash, &row.provider)?
                .unwrap();
            Ok((row.state, affinity.lease_valid_until))
        })
        .unwrap();
    assert_eq!(state, "finished");
    assert!(
        lease_until.is_some_and(|until| until <= now),
        "the pin's lease ends at run end, not at its backstop: {lease_until:?}"
    );
}
