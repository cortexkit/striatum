use super::*;

async fn legacy(extra: Value, selected: bool) -> (KitSubject, Route, Value, Value, Value) {
    let subject = KitSubject {
        observe_blocks: Mutex::new(false),
        media: Mutex::new(Default::default()),
        harness: Mutex::new(Harness::new("continue-legacy")),
    };
    subject
        .configure_consumers(&fixture::consumers())
        .await
        .unwrap();
    let mut catalog = fixture::continue_catalog();
    catalog
        .accounts
        .retain(|account| account.account_id == "account-1");
    subject.configure_catalog(&catalog).await.unwrap();
    let harness = subject.harness();
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let mut admit = json!({
        "caller":{"runner":fixture::RUNNER_ONE},
        "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "admission_id":"admission","conversation_key":"conversation","initial_input_bytes":65536,
    });
    if selected {
        let consumer = harness.bind(reserved(fixture::CONSUMER), None).await;
        let selection = result(consumer.call("route.select", &json!({
            "consumer":fixture::SUMMARY,"pin":fixture::HOLD_FIRST,"requirements":{"iq":50},
            "task_ref":"legacy-selected-task",
        })).await);
        admit["decision_id"] = selection["decision_id"].clone();
    }
    admit
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let admitted = result(runner.call("route.account_for", &admit).await);
    result(
        runner
            .call(
                "route.account_observe",
                &json!({
                    "caller":{"runner":fixture::RUNNER_ONE},"event_id":"legacy-started",
                    "kind":"admission_started","admission_id":"admission","run_id":"run",
                }),
            )
            .await,
    );
    subject
        .configure_continue_credentials(&fixture::continue_credentials())
        .await
        .unwrap();
    let predecessor = admit
        .get("decision_id")
        .unwrap_or(&admitted["reason"]["decision_id"]);
    let request = json!({
        "mode":"continue","admission_id":"admission","run_id":"run",
        "conversation_key":"conversation","previous_decision_id":predecessor,"selection_id":"legacy-wall",
        "exhausted":{"credential_id":admitted["account"]["credential_id"],
            "account":admitted["account"]["account_id"],"limit_window":"seven_day",
            "observed_at_ms":account_selection::now_ms(),"resets_at_ms":account_selection::now_ms()+300000},
    });
    assert!(harness
        .store()
        .v1_admission(&format!("reserved:{}", fixture::RUNNER_ONE), "admission")
        .unwrap()
        .is_none());
    (subject, runner, admit, admitted, request)
}

fn edit_snapshot(subject: &KitSubject, admitted: &Value, update: impl FnOnce(&mut Value)) {
    let harness = subject.harness();
    let decision = admitted["reason"]["decision_id"].as_str().unwrap();
    let row = harness.store().account_decision(decision).unwrap().unwrap();
    let mut context: Value = serde_json::from_str(row.context_json.as_ref().unwrap()).unwrap();
    update(&mut context);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    assert_eq!(
        conn.execute(
            "UPDATE account_decision SET context_json=?1 WHERE decision_id=?2",
            rusqlite::params![context.to_string(), decision]
        )
        .unwrap(),
        1
    );
}

async fn use_offer(runner: &Route, offer: &Value, event: &str) {
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(offer, "admission_started", event),
            )
            .await,
    );
}

#[tokio::test]
async fn legacy_continue_snapshot_chains_and_resume_uses_only_used_offers() {
    let (subject, runner, admit, admitted, request) = legacy(json!({}), false).await;
    let decision = subject
        .harness()
        .store()
        .account_decision(admitted["reason"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let context: Value = serde_json::from_str(decision.context_json.as_ref().unwrap()).unwrap();
    let source = &context["continuation_input"];
    assert_eq!(source["version"], 1);
    assert_eq!(source["auth_method"], "oauth");
    assert_eq!(source["subscription"], true);
    assert_eq!(source["harness"], fixture::RUNNER_ONE);
    assert!(source["owner_scope"].get("session").is_none());
    assert!(source["owner_scope"].get("run").is_none());
    assert_eq!(
        source["owner_scope"]["owner"],
        format!("reserved:{}", fixture::RUNNER_ONE)
    );
    assert!(source.get("conversation_key").is_none());
    let unmoved = result(runner.call("route.account_for", &admit).await);
    let first = result(runner.call("route.account_for", &request).await);
    assert_eq!(first["kind"], "offer", "{first}");
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
    assert_eq!(
        result(runner.call("route.account_for", &admit).await),
        unmoved
    );
    use_offer(&runner, &first, "use-first").await;
    let resumed = result(runner.call("route.account_for", &admit).await);
    assert_eq!(resumed["admission"]["id"], "admission");
    assert_eq!(resumed["account"]["account_id"], first["account"]);
    assert_eq!(resumed["reason"]["decision_id"], first["decision_id"]);
    let second = next_offer(&runner, &request, &first).await;
    use_offer(&runner, &second, "use-second").await;
    let resumed = result(runner.call("route.account_for", &admit).await);
    assert_eq!(resumed["account"]["account_id"], second["account"]);
    assert_eq!(resumed["reason"]["decision_id"], second["decision_id"]);
    assert_eq!(resumed["admission"]["id"], "admission");
    let mut conflict = admit;
    conflict["model"] = json!("another-model");
    let refused = runner.call("route.account_for", &conflict).await;
    assert_eq!(refused.1["code"], "admission_conflict", "{refused:?}");
}

#[tokio::test]
async fn legacy_select_bound_continue_requires_selection_not_account_decision() {
    let (subject, runner, _admit, admitted, request) = legacy(json!({}), true).await;
    let mut wrong = request.clone();
    wrong["selection_id"] = json!("wrong-predecessor");
    wrong["previous_decision_id"] = admitted["reason"]["decision_id"].clone();
    let refused = result(runner.call("route.account_for", &wrong).await);
    assert_eq!(refused["routing_code"], "unknown_decision");
    assert_eq!(
        refused["detail"],
        "previous decision is not the run's selection"
    );
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
    assert_eq!(offered.project_id, previous.project_id);
    assert_eq!(offered.runner_module, previous.runner_module);
    assert_eq!(
        offered.allowed_account_class,
        previous.allowed_account_class
    );
}

#[tokio::test]
async fn legacy_continue_missing_snapshot_is_truthful_and_finished_replays_first() {
    let (subject, runner, _admit, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context
            .as_object_mut()
            .unwrap()
            .remove("continuation_input");
    });
    let first = result(runner.call("route.account_for", &request).await);
    assert_eq!(first["routing_code"], "unknown_decision");
    assert_eq!(first["detail"], "admission predates continuation support");
    result(runner.call("route.account_observe",&json!({
        "event_id":"finished","kind":"run_finished","admission_id":"admission","run_id":"run",
    })).await);
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        first
    );
    let mut fresh = request;
    fresh["selection_id"] = json!("after-finish");
    let refused = result(runner.call("route.account_for", &fresh).await);
    assert_eq!(refused["routing_code"], "unknown_decision");
    assert_eq!(
        refused["detail"],
        "admission is finished; it cannot continue"
    );
}

#[tokio::test]
async fn legacy_continue_version_and_harness_are_not_inferred() {
    for (field, value) in [
        ("version", json!(99)),
        ("harness", json!("other-runner")),
        ("auth_method", json!("")),
    ] {
        let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
        edit_snapshot(&subject, &admitted, |context| {
            context["continuation_input"][field] = value
        });
        let refused = result(runner.call("route.account_for", &request).await);
        assert_eq!(refused["kind"], "refuse", "{refused}");
        assert_eq!(
            refused["routing_code"], "account_unservable",
            "{field}: {refused}"
        );
    }
}

#[tokio::test]
async fn legacy_continue_inherits_candidate_set_and_input_size() {
    let candidates = json!({"view_digest":"legacy-view","accounts":[
        {"account_id":"account-1","credential_id":"credential-1"},
        {"account_id":"account-2","credential_id":"credential-2"},
    ]});
    let (subject, runner, _, admitted, request) = legacy(json!({"candidates":candidates,"request_input_tokens":24000,"parent_conversation_key":"parent"}), false).await;
    let row = subject
        .harness()
        .store()
        .account_decision(admitted["reason"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let context: Value = serde_json::from_str(row.context_json.as_ref().unwrap()).unwrap();
    assert_eq!(context["continuation_input"]["request_input_tokens"], 24000);
    assert_eq!(
        context["continuation_input"]["parent_conversation_key"],
        "parent"
    );
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_eq!(offer["account"], "account-2");
    let mut next = request;
    next["selection_id"] = json!("next-candidate-wall");
    next["previous_decision_id"] = offer["decision_id"].clone();
    next["exhausted"]["account"] = offer["account"].clone();
    next["exhausted"]["credential_id"] = offer["credential_id"].clone();
    let held = result(runner.call("route.account_for", &next).await);
    assert_eq!(held["kind"], "hold", "{held}");
}

#[tokio::test]
async fn legacy_continue_explicit_pin_uses_contract_caller_pinned_reason() {
    let (_, runner, _, _, request) = legacy(
        json!({"requested_account":{"account_id":"account-1"}}),
        false,
    )
    .await;
    let refused = result(runner.call("route.account_for", &request).await);
    // CONTRACT.md 1493-1499 forbids switching away from an explicit caller pin.
    assert_eq!(refused["routing_code"], "pin_unavailable");
    assert_eq!(refused["detail"]["reason"], "caller_pinned");
}

#[tokio::test]
async fn legacy_continue_keeps_frozen_auth_method() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["auth_method"] = json!("apikey")
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_ne!(refused["kind"], "offer", "{refused}");
}

#[tokio::test]
async fn legacy_continue_snapshot_route_must_match_authoritative_admission() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["model"] = json!("not-the-admitted-model")
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_eq!(refused["kind"], "refuse", "{refused}");
    assert_eq!(refused["routing_code"], "account_unservable", "{refused}");
}

#[tokio::test]
async fn legacy_continue_keeps_frozen_payment_basis() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["subscription"] = json!(false)
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_ne!(refused["kind"], "offer", "{refused}");
}

#[tokio::test]
async fn legacy_continue_keeps_frozen_priced_account_class() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["allowed_account_class"] = json!("priced_per_token_only")
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_ne!(refused["kind"], "offer", "{refused}");
}

#[tokio::test]
async fn legacy_continue_waits_for_unobserved_frozen_service_tier() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["service_tier"] = json!("fast")
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_eq!(refused["kind"], "hold", "{refused}");
    assert_eq!(
        refused["routing_code"], "account_selection_unavailable",
        "{refused}"
    );
}

#[tokio::test]
async fn legacy_continue_never_steps_down_a_frozen_service_tier() {
    let (subject, runner, _, admitted, request) = legacy(json!({}), false).await;
    let shared = subject.harness().credentials();
    {
        let mut credentials = shared.lock().unwrap();
        credentials.publish_billing_catalog(
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[]),
        );
        credentials.publish_plan_prices(&json!({"prices":[]}));
        credentials
            .runner_service_tiers
            .insert(fixture::RUNNER_ONE.into(), Default::default());
        assert!(crate::service_tiers::unobserved_account_input(&credentials).is_none());
        assert!(!super::super::super::runners::tier_view_unknown(
            &credentials,
            fixture::RUNNER_ONE,
            account_selection::now_ms()
        ));
    }
    edit_snapshot(&subject, &admitted, |context| {
        context["continuation_input"]["service_tier"] = json!("fast")
    });
    let refused = result(runner.call("route.account_for", &request).await);
    assert_eq!(refused["kind"], "refuse", "{refused}");
    assert_eq!(refused["routing_code"], "account_unservable", "{refused}");
}

#[tokio::test]
async fn v1_admission_before_snapshots_remains_a_compatibility_source() {
    let (subject, _, runner, request) = paused().await;
    let admission = subject
        .harness()
        .store()
        .account_read(|tx| tx.admission(&format!("reserved:{}", fixture::RUNNER_ONE), "admission"))
        .unwrap()
        .unwrap();
    edit_snapshot(
        &subject,
        &json!({"reason":{"decision_id":admission.decision_id}}),
        |context| {
            context
                .as_object_mut()
                .unwrap()
                .remove("continuation_input");
        },
    );
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
}

#[tokio::test]
async fn legacy_continue_selectless_does_not_require_v1_ledger() {
    let (_, runner, _, _, request) = legacy(json!({}), false).await;
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
}

#[tokio::test]
async fn legacy_continue_released_admission_is_not_unknown() {
    let (_, runner, mut admit, _, mut request) = legacy(json!({}), false).await;
    admit["admission_id"] = json!("unstarted");
    let admitted = result(runner.call("route.account_for", &admit).await);
    result(
        runner
            .call(
                "route.account_release",
                &json!({"admission_id":"unstarted"}),
            )
            .await,
    );
    request["admission_id"] = json!("unstarted");
    request["previous_decision_id"] = admitted["reason"]["decision_id"].clone();
    let refused = result(runner.call("route.account_for", &request).await);
    assert_eq!(refused["routing_code"], "unknown_decision");
    assert_eq!(
        refused["detail"],
        "admission is released; it cannot continue"
    );
}
