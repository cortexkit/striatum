use super::*;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
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
        let reopened = prefrontal_routing_store::SqliteRouteStateStore::open(
            &prefrontal_routing_store::dev_descriptor(
                "prefrontal-routing",
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
    let policy = prefrontal_routing_core::accounts::AllocatorPolicy::default();
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
    let descriptor = prefrontal_routing_store::dev_descriptor(
        DEFAULT_MODULE_ID,
        state.dir.join("store.db").to_string_lossy(),
    );
    let placeholder = Arc::new(
        prefrontal_routing_store::SqliteRouteStateStore::open(
            &prefrontal_routing_store::dev_descriptor(
                "continue-placeholder",
                state.dir.join("placeholder.db").to_string_lossy(),
            ),
        )
        .unwrap(),
    );
    drop(std::mem::replace(&mut state.store, placeholder));
    state.store =
        Arc::new(prefrontal_routing_store::SqliteRouteStateStore::open(&descriptor).unwrap());
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
