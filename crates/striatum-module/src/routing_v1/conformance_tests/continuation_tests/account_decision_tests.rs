use super::*;

async fn paused_without_selection() -> (KitSubject, Route, Value, Value) {
    let subject = KitSubject {
        observe_blocks: Mutex::new(false),
        media: Mutex::new(Default::default()),
        harness: Mutex::new(Harness::new("continue-account-decision")),
    };
    subject
        .configure_consumers(&fixture::consumers())
        .await
        .unwrap();
    let mut catalog = fixture::continue_catalog();
    catalog.accounts.retain(|a| a.account_id == "account-1");
    subject.configure_catalog(&catalog).await.unwrap();
    let harness = subject.harness();
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &json!({
                    "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                    "admission_id":"run-sid-head-example","conversation_key":"head-conversation",
                    "initial_input_bytes":65536
                }),
            )
            .await,
    );
    result(
        runner
            .call(
                "route.account_observe",
                &json!({
                    "event_id":"head-started","kind":"admission_started",
                    "admission_id":"run-sid-head-example","run_id":"run-sid-head-example"
                }),
            )
            .await,
    );
    subject
        .configure_continue_credentials(&fixture::continue_credentials())
        .await
        .unwrap();
    // A model-pinned head has only its admission's account decision to name
    // when a seven-day wall pauses the run, not a route.select decision.
    let request = json!({
        "mode":"continue","run_id":"run-sid-head-example",
        "admission_id":"run-sid-head-example","conversation_key":"head-conversation",
        "previous_decision_id":admitted["reason"]["decision_id"],
        "selection_id":"head-seven-day-wall",
        "exhausted":{
            "credential_id":admitted["account"]["credential_id"],
            "account":admitted["account"]["account_id"],"limit_window":"seven_day",
            "observed_at_ms":account_selection::now_ms(),
            "resets_at_ms":account_selection::now_ms()+300000
        }
    });
    let previous = request["previous_decision_id"].as_str().unwrap();
    assert!(previous.starts_with("acd_"));
    assert!(harness.store().v1_decision(previous).unwrap().is_none());
    assert!(harness
        .store()
        .account_decision(previous)
        .unwrap()
        .is_some());
    (subject, runner, request, admitted)
}

fn assert_account_offer(subject: &KitSubject, request: &Value, admitted: &Value, offer: &Value) {
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_ne!(offer["account"], request["exhausted"]["account"]);
    let harness = subject.harness();
    let id = offer["decision_id"].as_str().unwrap();
    let decision = harness.store().account_decision(id).unwrap().unwrap();
    let previous = harness
        .store()
        .account_decision(request["previous_decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        decision.admission_id.as_deref(),
        Some("run-sid-head-example")
    );
    assert_eq!(decision.principal, previous.principal);
    assert_eq!(decision.provider, fixture::SUBSCRIPTION_PROVIDER);
    assert_eq!(decision.model, "model-c");
    assert_eq!(decision.account_id.as_deref(), offer["account"].as_str());
    assert_eq!(
        decision.credential_id.as_deref(),
        offer["credential_id"].as_str()
    );
    // No model was selected, so a switch must not invent selection provenance.
    assert!(harness.store().v1_decision(id).unwrap().is_none());
    assert!(harness.store().get_decision_by_id(id).unwrap().is_none());
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let (linked, reply): (String, String) = conn
        .query_row(
            "SELECT c.previous_decision_id,a.reply_json FROM account_continue c
         JOIN account_admission a ON a.principal=c.principal AND a.admission_id=c.reservation_id
         WHERE c.decision_id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(linked, request["previous_decision_id"].as_str().unwrap());
    let reply: Value = serde_json::from_str(&reply).unwrap();
    let method = admitted["account"]["method"].as_str().unwrap();
    assert_ne!(method, "unknown");
    assert_eq!(reply["account"]["method"], method);
}

#[tokio::test]
async fn continuation_from_account_decision_preserves_admission_boundaries() {
    let (subject, runner, request, admitted) = paused_without_selection().await;
    let offer = result(runner.call("route.account_for", &request).await);
    assert_account_offer(&subject, &request, &admitted, &offer);
}

#[tokio::test]
async fn continuation_used_via_started_leaves_served_attempt_unbound() {
    let (subject, runner, request, admitted) = paused_without_selection().await;
    let offer = result(runner.call("route.account_for", &request).await);
    assert_account_offer(&subject, &request, &admitted, &offer);

    let started = json!({
        "kind": "admission_started", "event_id": "head-moved",
        "admission_id": request["admission_id"], "run_id": request["run_id"],
        "served_account_id": offer["account"], "credential_id": offer["credential_id"],
    });
    let mismatch = json!({
        "effect": "served_account_mismatch",
        "selected_account_id": admitted["account"]["account_id"],
        "served_account_id": offer["account"],
        "pin_generation": admitted["pin"]["generation"].as_u64().unwrap(),
    });
    let effects = |reply: &Value| {
        reply["observation"]["effects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|effect| serde_json::from_str::<Value>(effect.as_str().unwrap()).unwrap())
            .collect::<Vec<_>>()
    };
    let start = result(runner.call("route.account_observe", &started).await);
    assert_eq!(start["observation"]["bound"], false, "{start}");
    assert_eq!(
        effects(&start),
        vec![json!({"effect": "continuation_used"}), mismatch.clone()]
    );
    let (used, _, state, run_id, started_at) = offer_state(&subject, &offer);
    assert!(used.is_some());
    assert_eq!(state, "started");
    assert_eq!(run_id.as_deref(), request["run_id"].as_str());
    assert_eq!(started_at, used);

    let attempt = json!({
        "kind": "attempt", "event_id": "head-served",
        "admission_id": request["admission_id"],
        "served_account_id": offer["account"], "credential_id": offer["credential_id"],
        "attempt_outcome": {"status": "served"}, "upstream_origin": "upstream",
    });
    let served = result(runner.call("route.account_observe", &attempt).await);
    assert_eq!(served["observation"]["bound"], false, "{served}");
    assert_eq!(effects(&served), vec![mismatch]);
}

#[tokio::test]
async fn continuation_from_account_decision_chains_offered_account_decisions() {
    let (subject, runner, request, admitted) = paused_without_selection().await;
    let first = result(runner.call("route.account_for", &request).await);
    assert_account_offer(&subject, &request, &admitted, &first);
    let mut next = request.clone();
    next["selection_id"] = json!("second-head-wall");
    next["previous_decision_id"] = first["decision_id"].clone();
    next["exhausted"]["account"] = first["account"].clone();
    next["exhausted"]["credential_id"] = first["credential_id"].clone();
    let second = result(runner.call("route.account_for", &next).await);
    assert_account_offer(&subject, &next, &admitted, &second);
    assert_ne!(second["account"], request["exhausted"]["account"]);
}

#[tokio::test]
async fn continuation_from_account_decision_refuses_another_admissions_decision() {
    let (subject, runner, request, admitted) = paused_without_selection().await;
    let other = result(
        runner
            .call(
                "route.account_for",
                &json!({
                    "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                    "admission_id":"other-head-admission","conversation_key":"other-conversation"
                }),
            )
            .await,
    );
    let mut foreign = request.clone();
    foreign["selection_id"] = json!("foreign-account-decision");
    foreign["previous_decision_id"] = other["reason"]["decision_id"].clone();
    let refused = result(runner.call("route.account_for", &foreign).await);
    assert_eq!(refused["kind"], "refuse", "{refused}");
    assert_eq!(refused["routing_code"], "unknown_decision");
    // The same run can continue with its own decision; rejection must not
    // merely mean that every account decision is unknown.
    let offer = result(runner.call("route.account_for", &request).await);
    assert_account_offer(&subject, &request, &admitted, &offer);

    let mut foreign_offer = foreign;
    foreign_offer["selection_id"] = json!("foreign-offered-decision");
    foreign_offer["admission_id"] = json!("other-head-admission");
    foreign_offer["previous_decision_id"] = offer["decision_id"].clone();
    let refused = result(runner.call("route.account_for", &foreign_offer).await);
    assert_eq!(refused["kind"], "refuse", "{refused}");
    assert_eq!(refused["routing_code"], "unknown_decision");
}

#[tokio::test]
async fn continuation_from_account_decision_replays_identical_offer() {
    let (subject, runner, request, admitted) = paused_without_selection().await;
    let first = result(runner.call("route.account_for", &request).await);
    assert_account_offer(&subject, &request, &admitted, &first);
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let (answers, offers): (i64, i64) = conn
        .query_row(
            "SELECT count(*),count(decision_id) FROM account_continue",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((answers, offers), (1, 1));
}
