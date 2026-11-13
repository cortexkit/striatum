use super::*;

mod account_decision_tests;
mod request_reads_tests;
mod signed_out_tests;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

fn offer_observation(offer: &Value, kind: &str, event: &str) -> Value {
    json!({"event_id":event,"kind":kind,"admission_id":"admission","run_id":"run",
        "served_account_id":offer["account"],"credential_id":offer["credential_id"]})
}

fn offer_state(
    subject: &KitSubject,
    offer: &Value,
) -> (
    Option<i64>,
    Option<i64>,
    String,
    Option<String>,
    Option<i64>,
) {
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    conn.query_row(
        "SELECT c.used_at_ms,c.released_at_ms,a.state,a.run_id,a.started_at
         FROM account_continue c JOIN account_admission a
         ON a.principal=c.principal AND a.admission_id=c.reservation_id WHERE c.decision_id=?1",
        [offer["decision_id"].as_str().unwrap()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )
    .unwrap()
}

fn expire_offers(subject: &KitSubject, until: i64) {
    subject
        .harness()
        .store()
        .account_transaction(|tx| tx.release_expired_continuations(until))
        .unwrap();
}

async fn next_offer(runner: &Route, request: &Value, previous: &Value) -> Value {
    let mut next = request.clone();
    next["selection_id"] = json!(format!(
        "next-{}",
        previous["decision_id"].as_str().unwrap()
    ));
    next["previous_decision_id"] = previous["decision_id"].clone();
    next["exhausted"]["account"] = previous["account"].clone();
    next["exhausted"]["credential_id"] = previous["credential_id"].clone();
    let offer = result(runner.call("route.account_for", &next).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    offer
}

async fn paused() -> (KitSubject, Route, Route, Value) {
    paused_with_pin(false).await
}

async fn paused_with_pin(pinned: bool) -> (KitSubject, Route, Route, Value) {
    paused_in_scope(pinned, None).await
}

async fn paused_in_scope(pinned: bool, scope: Option<&str>) -> (KitSubject, Route, Route, Value) {
    let subject = KitSubject {
        harness: Mutex::new(Harness::new("continue")),
    };
    subject
        .configure_consumers(&fixture::consumers())
        .await
        .unwrap();
    let mut catalog = fixture::continue_catalog();
    catalog.accounts.retain(|a| a.account_id == "account-1");
    subject.configure_catalog(&catalog).await.unwrap();
    let harness = subject.harness();
    let consumer = harness.bind(reserved(fixture::CONSUMER), scope).await;
    let selected=result(consumer.call("route.select", &json!({"consumer":fixture::SUMMARY,"pin":fixture::HOLD_FIRST,"requirements":{"iq":50},"task_ref":"continued-task"})).await);
    let runner = harness
        .bind(
            reserved(selected["runner"]["module"].as_str().unwrap()),
            scope,
        )
        .await;
    let mut admission = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","decision_id":selected["decision_id"],"admission_id":"admission","conversation_key":"conversation","initial_input_bytes":65536});
    if pinned {
        admission["requested_account"] = json!({"account_id":"account-1"});
    }
    let admitted = result(runner.call("route.account_for", &admission).await);
    result(runner.call("route.account_observe",&json!({"event_id":"started","kind":"admission_started","admission_id":"admission","run_id":"run"})).await);
    subject
        .configure_continue_credentials(&fixture::continue_credentials())
        .await
        .unwrap();
    let request = json!({"mode":"continue","run_id":"run","admission_id":"admission","conversation_key":"conversation","previous_decision_id":selected["decision_id"],"selection_id":"selection",
        "exhausted":{"credential_id":admitted["account"]["credential_id"],"account":admitted["account"]["account_id"],"limit_window":"five_hour","observed_at_ms":account_selection::now_ms(),"resets_at_ms":account_selection::now_ms()+300000}});
    (subject, consumer, runner, request)
}

#[tokio::test]
async fn public_reads_reject_continuation_offers_before_and_after_settlement() {
    let (_subject, consumer, runner, request) = paused().await;
    let explained = result(
        consumer
            .call(
                "route.explain",
                &json!({"decision_id":request["previous_decision_id"]}),
            )
            .await,
    );
    assert_eq!(
        explained["serving"]["account_id"],
        request["exhausted"]["account"]
    );
    assert_eq!(
        explained["serving"]["credential_id"],
        request["exhausted"]["credential_id"]
    );
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    for settled in [false, true] {
        if settled {
            result(consumer.call("route.report", &json!({
                "decision_id":offer["decision_id"],"outcome":"completed", "usage":{"input_tokens":100,"output_tokens":20}
            })).await);
        }
        for (method, params) in [
            ("route.explain", json!({"decision_id":offer["decision_id"]})),
            (
                "route.budget_quote",
                json!({"decisions":[offer["decision_id"]]}),
            ),
        ] {
            let reply = consumer.call(method, &params).await;
            assert_eq!(reply.0, FrameType::Error, "{reply:?}");
            assert_eq!(reply.1["code"], "unknown_decision");
        }
    }
}

#[tokio::test]
async fn continuation_offer_is_durable_idempotent_linked_and_releases_unused_demand() {
    let (subject, _, runner, request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    assert_eq!(first["kind"], "offer", "{first}");
    let harness = subject.harness();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let decision = first["decision_id"].as_str().unwrap();
    let (previous,task,scope,price):(String,String,Option<String>,String)=conn.query_row("SELECT previous_decision_id,task_id,project_id,score_json FROM model_route_decisions WHERE id=?1",[decision],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(previous, request["previous_decision_id"].as_str().unwrap());
    assert_eq!(task, "continued-task");
    assert_eq!(scope, None);
    let original_price: String = conn
        .query_row(
            "SELECT score_json FROM model_route_decisions WHERE id=?1",
            [&previous],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(price, original_price);
    let (principal, reservation, until): (String, String, i64) = conn
        .query_row(
            "SELECT principal,reservation_id,valid_until_ms FROM account_continue",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(until, first["valid_until_ms"].as_i64().unwrap());
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM account_continue", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let mut changed = request.clone();
    changed["run_id"] = json!("different");
    let refusal = runner.call("route.account_for", &changed).await;
    assert_eq!(refusal.0, FrameType::Error);
    assert_eq!(refusal.1["code"], "invalid_request");
    assert_eq!(refusal.1["detail"]["field"], "selection_id");
    harness
        .store()
        .account_transaction(|tx| {
            let row = tx.admission(&principal, &reservation)?.unwrap();
            assert_eq!(row.state, "reserved");
            assert!(row.charged);
            assert_eq!(row.request_bytes, 65536);
            tx.release_expired_continuations(until - 1)?;
            assert_eq!(
                tx.admission(&principal, &reservation)?.unwrap().state,
                "reserved"
            );
            tx.release_expired_continuations(until)?;
            assert_eq!(
                tx.admission(&principal, &reservation)?.unwrap().state,
                "released"
            );
            assert!(tx
                .demand(
                    fixture::SUBSCRIPTION_PROVIDER,
                    first["account"].as_str().unwrap(),
                    None
                )?
                .is_empty());
            Ok(())
        })
        .unwrap();
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
}

#[tokio::test]
async fn continuation_first_usage_consumes_reservation_and_keeps_task_profile() {
    let (subject, consumer, runner, request) = paused().await;
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    let report = json!({"decision_id":offer["decision_id"],"outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}});
    result(consumer.call("route.report", &report).await);
    let harness = subject.harness();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let (principal, reservation, until, used): (String, String, i64, Option<i64>) = conn
        .query_row(
            "SELECT principal,reservation_id,valid_until_ms,used_at_ms FROM account_continue",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert!(used.is_some());
    harness
        .store()
        .account_transaction(|tx| {
            tx.release_expired_continuations(until + 1)?;
            assert_eq!(
                tx.admission(&principal, &reservation)?.unwrap().state,
                "started"
            );
            Ok(())
        })
        .unwrap();
    let task: (String, String) = conn
        .query_row(
            "SELECT task_id,task_workload FROM model_route_usage",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(task, ("continued-task".into(), fixture::SUMMARY.into()));
    assert_eq!(
        result(consumer.call("route.report", &report).await)["decision_id"],
        offer["decision_id"]
    );
}

#[tokio::test]
async fn continuation_first_usageless_report_consumes_reservation_for_every_end_state() {
    for outcome in ["completed", "interrupted", "timed_out"] {
        let (subject, consumer, runner, request) = paused().await;
        let offer = result(runner.call("route.account_for", &request).await);
        assert_eq!(offer["kind"], "offer");
        let report = json!({"decision_id":offer["decision_id"],"outcome":outcome});
        let first = result(consumer.call("route.report", &report).await);
        let harness = subject.harness();
        let path = harness.store_path();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let (principal, reservation, until, used): (String, String, i64, i64) = conn
            .query_row(
                "SELECT principal,reservation_id,valid_until_ms,used_at_ms FROM account_continue",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        harness
            .store()
            .account_transaction(|tx| {
                tx.release_expired_continuations(until + 1)?;
                assert_eq!(
                    tx.admission(&principal, &reservation)?.unwrap().state,
                    "started"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(result(consumer.call("route.report", &report).await), first);
        assert_eq!(
            conn.query_row("SELECT used_at_ms FROM account_continue", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            used
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM model_route_usage", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let restart_path = cortexkit_test_support::scratch_root().join(format!(
            "usageless-continue-{}-{}-{}.db",
            std::process::id(),
            outcome,
            crate::account_selection::now_ms()
        ));
        conn.execute("VACUUM INTO ?1", [restart_path.to_str().unwrap()])
            .unwrap();
        if outcome == "completed" {
            let mut next = request.clone();
            next["previous_decision_id"] = offer["decision_id"].clone();
            next["selection_id"] = json!("after-usageless-report");
            next["exhausted"]["credential_id"] = offer["credential_id"].clone();
            next["exhausted"]["account"] = offer["account"].clone();
            let continued = result(runner.call("route.account_for", &next).await);
            assert_eq!(continued["kind"], "offer", "{continued}");
            assert_ne!(continued["account"], offer["account"]);
        }
        drop(conn);
        drop(consumer);
        drop(runner);
        drop(harness);
        drop(subject);
        let reopened = striatum_store::SqliteRouteStateStore::open(
            &striatum_store::dev_descriptor(
                "striatum",
                restart_path.to_string_lossy().into_owned(),
            ),
        )
        .unwrap();
        reopened
            .account_transaction(|tx| {
                tx.release_expired_continuations(until + 2)?;
                assert_eq!(
                    tx.admission(&principal, &reservation)?.unwrap().state,
                    "started"
                );
                Ok(())
            })
            .unwrap();
    }
}

#[tokio::test]
async fn continuation_refuses_unbound_runs_and_disabled_selection() {
    let (subject, _, runner, request) = paused().await;
    for field in [
        "run_id",
        "admission_id",
        "conversation_key",
        "previous_decision_id",
    ] {
        let mut wrong = request.clone();
        wrong[field] = json!("unknown");
        wrong["selection_id"] = json!(field);
        let answer = result(runner.call("route.account_for", &wrong).await);
        assert_eq!(answer["kind"], "refuse", "{field}: {answer}");
        assert_eq!(answer["routing_code"], "unknown_decision");
    }
    let harness = subject.harness();
    let policy = striatum_core::accounts::AllocatorPolicy::default();
    let parsed: cortexkit_role_routing::account_continue::ContinueRequest =
        serde_json::from_value(request.clone()).unwrap();
    let previous = harness
        .store()
        .v1_decision(parsed.previous_decision_id.as_str())
        .unwrap()
        .unwrap();
    let principal = format!("reserved:{}", previous.runner_module);
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: false },
        policy: &policy,
        now_ms: account_selection::now_ms(),
        classes: None,
    };
    let answer = account_selection::continue_account(
        &parsed,
        &principal,
        &previous.runner_module,
        None,
        &ctx,
    )
    .unwrap();
    assert_eq!(answer["kind"], "refuse");
    assert_eq!(answer["routing_code"], "account_selection_disabled");
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        answer
    );
}

#[tokio::test]
async fn continuation_hold_uses_earliest_known_candidate_reset() {
    let (subject, _, runner, request) = paused().await;
    let now = account_selection::now_ms() as u64 / 1000 * 1000;
    for credential in fixture::continue_credentials() {
        let reset = if credential.account.account_id == "account-2" {
            now + 180000
        } else {
            now + 240000
        };
        subject
            .block_account(&credential.account.account_id, Some(reset))
            .await
            .unwrap();
    }
    let answer = result(runner.call("route.account_for", &request).await);
    assert_eq!(answer["kind"], "hold", "{answer}");
    assert_eq!(answer["until_ms"], now + 180000);
}

#[tokio::test]
async fn continuation_refuses_a_caller_pinned_account_and_records_exhaustion() {
    let (subject, _, runner, request) = paused_with_pin(true).await;
    let answer = result(runner.call("route.account_for", &request).await);
    assert_eq!(answer["kind"], "refuse", "{answer}");
    assert_eq!(answer["routing_code"], "account_pinned");
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let evidence: String = conn
        .query_row(
            "SELECT observation_json FROM account_observation WHERE kind='account_limit_exhausted'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    for field in [
        "credential_id",
        "limit_window",
        "pool",
        "resets_at_ms",
        "retry_after_s",
        "observed_at_ms",
    ] {
        assert_eq!(evidence[field], request["exhausted"][field], "{field}");
    }
    assert_eq!(
        evidence["served_account_id"],
        request["exhausted"]["account"]
    );
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM account_fence WHERE cleared_at IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 0,
        "fresh headroom contradicts this typed quota refusal"
    );
    let billing: i64 = conn
        .query_row(
            "SELECT count(*) FROM account_non_quota_route_fence WHERE wall_kind='billing'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        billing, 1,
        "retain the refusal as non-quota, never erase it on headroom"
    );
}

#[tokio::test]
async fn continuation_refuses_credit_without_creating_quota_or_billing_evidence() {
    let (subject, _, runner, mut request) = paused().await;
    request["exhausted"]["limit_window"] = json!("credit");
    request["exhausted"]
        .as_object_mut()
        .unwrap()
        .remove("resets_at_ms");
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let rows: i64 = conn
        .query_row(
            "SELECT (SELECT count(*) FROM account_fence) +
        (SELECT count(*) FROM account_non_quota_route_fence)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rows, 0,
        "continuation is quota-only, not a credit observation"
    );
}

#[tokio::test]
async fn continuation_offer_replays_after_store_reopen() {
    let (subject, consumer, runner, request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    assert_eq!(first["kind"], "offer", "{first}");
    drop(consumer);
    drop(runner);
    let mut state = Arc::try_unwrap(subject.harness.into_inner().unwrap())
        .ok()
        .unwrap();
    let descriptor = striatum_store::dev_descriptor(
        DEFAULT_MODULE_ID,
        state.dir.join("store.db").to_string_lossy(),
    );
    let placeholder = Arc::new(
        striatum_store::SqliteRouteStateStore::open(
            &striatum_store::dev_descriptor(
                "continue-placeholder",
                state.dir.join("placeholder.db").to_string_lossy(),
            ),
        )
        .unwrap(),
    );
    drop(std::mem::replace(&mut state.store, placeholder));
    state.store =
        Arc::new(striatum_store::SqliteRouteStateStore::open(&descriptor).unwrap());
    state.route_epochs = tokio::sync::Mutex::new(RouteEpochs::new());
    let harness = Arc::new(state);
    let previous = harness
        .store()
        .v1_decision(request["previous_decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let runner = harness.bind(reserved(&previous.runner_module), None).await;
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM account_continue", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn continuation_follows_the_offered_decision_and_rejects_the_used_predecessor() {
    let (subject, consumer, runner, request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    assert_eq!(first["kind"], "offer", "{first}");
    result(consumer.call("route.report",&json!({"decision_id":first["decision_id"],"outcome":"failed","usage":{"output_tokens":20}})).await);
    let mut stale = request.clone();
    stale["selection_id"] = json!("stale-selection");
    let answer = result(runner.call("route.account_for", &stale).await);
    assert_eq!(answer["kind"], "refuse");
    assert_eq!(answer["routing_code"], "unknown_decision");
    let mut switched = request.clone();
    switched["previous_decision_id"] = first["decision_id"].clone();
    switched["selection_id"] = json!("next-selection");
    switched["exhausted"]["credential_id"] = first["credential_id"].clone();
    switched["exhausted"]["account"] = first["account"].clone();
    let next = result(runner.call("route.account_for", &switched).await);
    assert_eq!(next["kind"], "offer", "{next}");
    assert_ne!(next["account"], first["account"]);
    assert_ne!(next["account"], request["exhausted"]["account"]);
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let previous: String = conn
        .query_row(
            "SELECT previous_decision_id FROM model_route_decisions WHERE id=?1",
            [next["decision_id"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(previous, first["decision_id"].as_str().unwrap());
}

#[tokio::test]
async fn continuation_preserves_owner_scope_and_refuses_a_different_project() {
    let (subject, _, runner, mut request) = paused_in_scope(false, Some("continue-project")).await;
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    let harness = subject.harness();
    let previous = harness
        .store()
        .v1_decision(request["previous_decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let offered = harness
        .store()
        .v1_decision(offer["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(offered.principal, previous.principal);
    assert_eq!(offered.project_id.as_deref(), Some("continue-project"));
    assert_eq!(offered.consumer, previous.consumer);
    let other = harness
        .bind(reserved(&previous.runner_module), Some("different-project"))
        .await;
    request["selection_id"] = json!("wrong-project-selection");
    let refusal = result(other.call("route.account_for", &request).await);
    assert_eq!(refusal["kind"], "refuse", "{refusal}");
    assert_eq!(refusal["routing_code"], "unknown_decision");
}

#[tokio::test]
async fn continuation_observation_started_uses_offer_and_is_idempotent() {
    let (subject, _, runner, request) = paused().await;
    let offer = result(runner.call("route.account_for", &request).await);
    let observation = offer_observation(&offer, "admission_started", "moved-started");
    result(runner.call("route.account_observe", &observation).await);
    let used = offer_state(&subject, &offer);
    assert!(used.0.is_some(), "the offered reservation must be used");
    assert_eq!(used.1, None);
    assert_eq!(used.2, "started");
    assert_eq!(used.3.as_deref(), Some("run"));
    assert_eq!(used.4, used.0);
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    conn.execute_batch("CREATE TABLE continuation_use_audit (writes INTEGER);
        CREATE TRIGGER continuation_use_repeat AFTER UPDATE OF used_at_ms ON account_continue
        BEGIN INSERT INTO continuation_use_audit VALUES (1); END;").unwrap();
    let duplicate = result(runner.call("route.account_observe", &observation).await);
    assert_eq!(duplicate["observation"]["duplicate"], true);
    let mut later = observation;
    later["event_id"] = json!("later-started");
    result(runner.call("route.account_observe", &later).await);
    later["event_id"] = json!("later-attempt");
    later["kind"] = json!("attempt");
    result(runner.call("route.account_observe", &later).await);
    expire_offers(&subject, offer["valid_until_ms"].as_i64().unwrap());
    assert_eq!(offer_state(&subject, &offer), used);
    assert_eq!(conn.query_row("SELECT count(*) FROM continuation_use_audit", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
}

#[tokio::test]
async fn continuation_observation_attempt_without_credential_is_backstop() {
    let (subject, _, runner, request) = paused().await;
    let offer = result(runner.call("route.account_for", &request).await);
    let mut attempt = offer_observation(&offer, "attempt", "lost-start");
    attempt.as_object_mut().unwrap().remove("credential_id");
    result(runner.call("route.account_observe", &attempt).await);
    let state = offer_state(&subject, &offer);
    assert!(state.0.is_some());
    assert_eq!(state.2, "started");
    expire_offers(&subject, offer["valid_until_ms"].as_i64().unwrap());
    assert_eq!(offer_state(&subject, &offer), state);
}

#[tokio::test]
async fn continuation_observation_identity_guards_record_only() {
    let (subject, _, runner, request) = paused().await;
    let offer = result(runner.call("route.account_for", &request).await);
    result(runner.call("route.account_for", &json!({"provider":fixture::SUBSCRIPTION_PROVIDER,
        "model":"model-c","decision_id":request["previous_decision_id"],
        "admission_id":"different-admission","conversation_key":"other-conversation","initial_input_bytes":65536})).await);
    result(runner.call("route.account_observe", &json!({"event_id":"other-start", "kind":"admission_started",
        "admission_id":"different-admission","run_id":"run"})).await);
    let initial = offer_state(&subject, &offer);
    for (index, kind, field, value) in [
        (
            0,
            "admission_started",
            "served_account_id",
            json!("unoffered-account"),
        ),
        (
            1,
            "attempt",
            "served_account_id",
            json!("unoffered-account"),
        ),
        (
            2,
            "admission_started",
            "credential_id",
            json!("different-credential"),
        ),
        (3, "attempt", "credential_id", json!("different-credential")),
        (4, "admission_started", "run_id", json!("different-run")),
        (5, "attempt", "run_id", json!("different-run")),
        (
            6,
            "admission_started",
            "admission_id",
            json!("different-admission"),
        ),
        (7, "attempt", "admission_id", json!("different-admission")),
        (8, "admission_started", "credential_id", Value::Null),
        (
            9,
            "used_account",
            "credential_id",
            offer["credential_id"].clone(),
        ),
    ] {
        let mut observation = offer_observation(&offer, kind, &format!("mismatch-{index}"));
        observation[field] = value;
        let reply = result(runner.call("route.account_observe", &observation).await);
        assert_eq!(reply["observation"]["duplicate"], false);
        assert_eq!(offer_state(&subject, &offer), initial, "guard {index}");
    }
    expire_offers(&subject, offer["valid_until_ms"].as_i64().unwrap());
    let released = offer_state(&subject, &offer);
    assert_eq!(released.0, None);
    assert!(released.1.is_some());
    assert_eq!(released.2, "released");
}

#[tokio::test]
async fn continuation_observation_deadline_and_released_offers_record_only() {
    for kind in ["admission_started", "attempt"] {
        let (subject, _, runner, request) = paused().await;
        let offer = result(runner.call("route.account_for", &request).await);
        let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
        conn.execute("UPDATE account_continue SET valid_until_ms=0", [])
            .unwrap();
        let mut observation = offer_observation(&offer, kind, "late");
        result(runner.call("route.account_observe", &observation).await);
        assert_eq!(offer_state(&subject, &offer).0, None);
        expire_offers(&subject, account_selection::now_ms());
        let released = offer_state(&subject, &offer);
        assert_eq!(released.2, "released");
        // Even a restored deadline cannot reopen a released offer.
        conn.execute(
            "UPDATE account_continue SET valid_until_ms=?1",
            [offer["valid_until_ms"].as_i64().unwrap()],
        )
        .unwrap();
        observation["event_id"] = json!("after-release");
        result(runner.call("route.account_observe", &observation).await);
        assert_eq!(offer_state(&subject, &offer), released);
    }
}

#[tokio::test]
async fn continuation_observation_newest_same_account_sibling_wins() {
    let (subject, _, runner, mut request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    request["selection_id"] = json!("newest-sibling");
    let newest = result(runner.call("route.account_for", &request).await);
    assert_eq!(newest["kind"], "offer", "{newest}");
    assert_eq!(newest["account"], first["account"]);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&newest, "attempt", "choose-newest"),
            )
            .await,
    );
    assert!(offer_state(&subject, &newest).0.is_some());
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let lease: i64 = conn.query_row("SELECT f.lease_valid_until FROM account_affinity f
        JOIN account_admission a ON a.pin_id=f.pin_id JOIN account_continue c
        ON c.principal=a.principal AND c.reservation_id=a.admission_id WHERE c.decision_id=?1",
        [newest["decision_id"].as_str().unwrap()], |r| r.get(0)).unwrap();
    assert!(lease > account_selection::now_ms(), "releasing a sibling must not end the serving pin's lease");
    let released = offer_state(&subject, &first);
    assert_eq!(released.0, None);
    assert!(released.1.is_some());
    assert_eq!(released.2, "released");
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&first, "admission_started", "old-sibling"),
            )
            .await,
    );
    assert_eq!(offer_state(&subject, &first), released);
}

#[tokio::test]
async fn continuation_report_releases_unused_siblings() {
    let (subject, consumer, runner, mut request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    request["selection_id"] = json!("unused-sibling");
    let sibling = result(runner.call("route.account_for", &request).await);
    result(
        consumer
            .call(
                "route.report",
                &json!({"decision_id":first["decision_id"],"outcome":"failed"}),
            )
            .await,
    );
    assert!(offer_state(&subject, &first).0.is_some());
    assert_eq!(offer_state(&subject, &sibling).2, "released");
}

#[tokio::test]
async fn continuation_observation_second_move_and_finish_close_all_reservations() {
    let (subject, _, runner, request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&first, "admission_started", "first-move"),
            )
            .await,
    );
    let second = next_offer(&runner, &request, &first).await;
    assert_ne!(second["account"], first["account"]);
    assert_ne!(second["account"], request["exhausted"]["account"]);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&second, "attempt", "second-move"),
            )
            .await,
    );
    assert!(offer_state(&subject, &second).0.is_some());
    let mut catalog = fixture::continue_catalog();
    let mut extra = catalog.accounts[0].clone();
    extra.account_id = "account-four".into();
    extra.credential_id = "credential-four".into();
    catalog.accounts.push(extra);
    subject.configure_catalog(&catalog).await.unwrap();
    let unused = next_offer(&runner, &request, &second).await;
    let mut finished = offer_observation(&second, "run_finished", "finished");
    finished["run_id"] = json!("different-run");
    result(runner.call("route.account_observe", &finished).await);
    assert_eq!(offer_state(&subject, &second).2, "started");
    finished["run_id"] = json!("run");
    finished["event_id"] = json!("correct-finish");
    result(runner.call("route.account_observe", &finished).await);
    assert_eq!(offer_state(&subject, &first).2, "finished");
    assert_eq!(offer_state(&subject, &second).2, "finished");
    assert_eq!(offer_state(&subject, &unused).2, "released");
    assert!(offer_state(&subject, &unused).1.is_some());
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let original: (String, i64) = conn
        .query_row(
            "SELECT state,finished_at FROM account_admission WHERE admission_id='admission'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(original.0, "finished");
    let open_leases: i64 = conn.query_row("SELECT count(*) FROM account_affinity WHERE lease_valid_until IS NULL OR lease_valid_until>?1", [original.1], |r| r.get(0)).unwrap();
    assert_eq!(open_leases, 0);
    result(runner.call("route.account_observe", &finished).await);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&unused, "attempt", "after-finish"),
            )
            .await,
    );
    assert_eq!(offer_state(&subject, &unused).0, None);
}

async fn replay_original(runner: &Route, request: &Value) -> Value {
    result(
        runner
            .call(
                "route.account_for",
                &json!({
                    "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                    "decision_id":request["previous_decision_id"],"admission_id":"admission",
                    "conversation_key":"conversation","initial_input_bytes":65536
                }),
            )
            .await,
    )
}

#[tokio::test]
async fn continuation_resume_replay_tracks_last_used_offer() {
    let (subject, _, runner, request) = paused().await;
    let first = result(runner.call("route.account_for", &request).await);
    let original = replay_original(&runner, &request).await;
    assert_eq!(
        original["account"]["account_id"],
        request["exhausted"]["account"]
    );
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&first, "admission_started", "move"),
            )
            .await,
    );
    for offer in [&first, &next_offer(&runner, &request, &first).await] {
        result(
            runner
                .call(
                    "route.account_observe",
                    &offer_observation(offer, "attempt", offer["decision_id"].as_str().unwrap()),
                )
                .await,
        );
        let replay = replay_original(&runner, &request).await;
        assert_eq!(replay["admission"]["id"], "admission");
        assert_eq!(replay["admission"]["state"], "started");
        assert_eq!(replay["account"]["account_id"], offer["account"]);
        assert_eq!(replay["account"]["credential_id"], offer["credential_id"]);
        assert_eq!(replay["reason"]["decision_id"], offer["decision_id"]);
    }
    assert_eq!(offer_state(&subject, &first).2, "started");
}

#[tokio::test]
async fn continuation_resume_unmoved_replay_is_byte_identical_and_capability_is_advertised() {
    let (subject, _, runner, request) = paused().await;
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let original: String = conn
        .query_row("SELECT answer_json FROM routing_v1_admission", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        serde_json::to_string(&replay_original(&runner, &request).await).unwrap(),
        original
    );
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer");
    assert_eq!(
        serde_json::to_string(&replay_original(&runner, &request).await).unwrap(),
        original
    );
    let describe = result(runner.call("role.describe", &json!({})).await);
    let capabilities = describe["capabilities"].as_array().unwrap();
    assert!(capabilities.contains(&json!("account_continue")));
    assert!(capabilities.contains(&json!("account_continue_resume")));
}
