use super::*;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

async fn setup(label: &str) -> (KitSubject, Route) {
    let subject = KitSubject {
        observe_blocks: Mutex::new(false),
        harness: Mutex::new(Harness::new(label)),
    };
    subject
        .configure_consumers(&fixture::consumers())
        .await
        .unwrap();
    subject
        .configure_catalog(&fixture::catalog())
        .await
        .unwrap();
    let runner = subject
        .harness()
        .bind(reserved(fixture::RUNNER_ONE), None)
        .await;
    (subject, runner)
}

fn request(id: &str) -> Value {
    json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "conversation_key":id,"admission_id":id,"warm_credential":"credential-2"})
}

fn diagnostics(subject: &KitSubject, answer: &Value) -> (Value, Value) {
    let row = subject
        .harness()
        .store()
        .account_decision(answer["reason"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let decision: Value = serde_json::from_str(&row.decision_json).unwrap();
    (
        decision["explanation"].clone(),
        serde_json::from_str(row.context_json.as_deref().unwrap()).unwrap(),
    )
}

fn quota_used(subject: &KitSubject, used: f64) {
    let now = account_selection::now_ms();
    let body = json!({"result":[{"provider":QUOTA_SOURCE,
    "apiProvider":fixture::SUBSCRIPTION_PROVIDER,"account":"account-2","source":"vault",
    "fetchedAt":account_selection::rfc3339(now),"usage":{
        "primary":{"usedPercent":used,"windowMinutes":300,
            "resetsAt":account_selection::rfc3339(now+7_200_000)},
        "secondary":{"usedPercent":20.0,"windowMinutes":10080,
            "resetsAt":account_selection::rfc3339(now+259_200_000)}
    }}]});
    account_quota::apply_provider_reply(
        &mut crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap(),
        QUOTA_SOURCE,
        &serde_json::to_vec(&body).unwrap(),
        now,
    )
    .unwrap();
    // Quota evidence is written through the same up-front writer helper used
    // by live refreshes, so tests exercise the allocator's durable block path.
    let harness = subject.harness();
    let state = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap().clone();
    account_selection::refresh_all_blocks(harness.store(), &state, &harness.operator_config(), now)
        .unwrap();
}

#[tokio::test]
async fn warm_credential_fresh_beats_cold_and_becomes_affinity() {
    let (_cold_subject, cold_runner) = setup("cold-reference").await;
    let mut cold = request("cold");
    cold.as_object_mut().unwrap().remove("warm_credential");
    assert_eq!(
        result(cold_runner.call("route.account_for", &cold).await)["account"]["credential_id"],
        "credential-1"
    );
    // Reset demand as well as affinity: the reference admission must not make
    // the hinted account become the next cold winner merely by charging work.
    let (subject, runner) = setup("warm-fresh").await;
    let mut warm = request("fresh");
    let first = result(runner.call("route.account_for", &warm).await);
    assert_eq!(first["account"]["credential_id"], "credential-2");
    assert_eq!(
        diagnostics(&subject, &first).0["import"]["kind"],
        "accepted"
    );
    warm.as_object_mut().unwrap().remove("warm_credential");
    warm["admission_id"] = json!("follow-up");
    let next = result(runner.call("route.account_for", &warm).await);
    assert_eq!(next["account"], first["account"]);
    assert_eq!(next["pin"]["id"], first["pin"]["id"]);
}

#[tokio::test]
async fn warm_credential_quota_block_is_ignored_and_recorded() {
    let (subject, runner) = setup("warm-block").await;
    let now = account_selection::now_ms();
    let fence = striatum_core::accounts::Fence {
        account_id: "account-2".into(),
        pool: None,
        recovery_set: Vec::new(),
        observed_at_ms: now,
        reset: striatum_core::accounts::ResetTime::At(now + 7_200_000),
        retry_hint_ms: None,
        source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
    };
    subject
        .harness()
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&striatum_store::AccountFenceRow {
                fence_id: "warm-block".into(),
                provider: fixture::SUBSCRIPTION_PROVIDER.into(),
                account_id: "account-2".into(),
                scope_json: "null".into(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: serde_json::to_string(&fence.source).unwrap(),
                observed_at: now,
                principal: "routing".into(),
                admission_id: None,
                observation_id: None,
                created_at: now,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
    let answer = result(runner.call("route.account_for", &request("blocked")).await);
    assert_eq!(answer["account"]["credential_id"], "credential-1");
    let (explanation, _) = diagnostics(&subject, &answer);
    assert_eq!(explanation["import"]["kind"], "rejected");
    assert!(
        explanation["import"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason["reason"] == "confirmed_fence"),
        "{explanation}"
    );
}

#[tokio::test]
async fn warm_credential_allocator_reserve_is_ignored_and_recorded() {
    let (subject, runner) = setup("warm-reserve").await;
    quota_used(&subject, 99.5);
    let answer = result(runner.call("route.account_for", &request("reserve")).await);
    assert_eq!(answer["account"]["credential_id"], "credential-1");
    let (explanation, _) = diagnostics(&subject, &answer);
    assert_eq!(explanation["import"]["kind"], "rejected");
    assert!(
        explanation["import"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason["reason"] == "below_reserve"),
        "{explanation}"
    );
}

#[tokio::test]
async fn warm_credential_existing_affinity_is_never_moved_even_inside_reserve() {
    let (subject, runner) = setup("warm-existing").await;
    let mut initial = request("affinity");
    initial["warm_credential"] = json!("credential-2");
    let first = result(runner.call("route.account_for", &initial).await);
    assert_eq!(first["account"]["credential_id"], "credential-2");
    quota_used(&subject, 99.5);
    initial["admission_id"] = json!("second");
    initial["warm_credential"] = json!("credential-1");
    let next = result(runner.call("route.account_for", &initial).await);
    assert_eq!(next["account"], first["account"]);
    assert_eq!(next["pin"]["id"], first["pin"]["id"]);
    assert_eq!(
        diagnostics(&subject, &next).1["warm_credential"]["result"]["reason"],
        "existing_affinity"
    );
}

#[tokio::test]
async fn warm_credential_changed_hint_conflicts_without_replacing_replay() {
    let (_subject, runner) = setup("warm-conflict").await;
    let first = request("same-id");
    let admitted = runner.call("route.account_for", &first).await;
    result(admitted.clone());
    let mut changed = first.clone();
    changed["warm_credential"] = json!("credential-1");
    let refusal = runner.call("route.account_for", &changed).await;
    assert_eq!(refusal.0, FrameType::Error);
    assert_eq!(refusal.1["code"], "admission_conflict");
    assert_eq!(runner.call("route.account_for", &first).await, admitted);
    changed.as_object_mut().unwrap().remove("warm_credential");
    assert_eq!(
        runner.call("route.account_for", &changed).await.1["code"],
        "admission_conflict"
    );
}

fn legacy_request(id: &str, runner: &str) -> Value {
    let mut params = request(id);
    params.as_object_mut().unwrap().remove("warm_credential");
    params["caller"] = json!({"runner":runner});
    params["preferred_existing_account"] = json!({"credential_id":"credential-2"});
    params
}

#[tokio::test]
async fn legacy_head_import_inside_keep_free_but_above_allocator_reserve_is_honoured() {
    let (subject, runner) = setup("legacy-head-import").await;
    subject.harness().edit_config(|config| {
        config["model_routing"]["keep_free_percent"] = json!({fixture::SUBSCRIPTION_PROVIDER:20});
    });
    quota_used(&subject, 90.0);
    // A session plugin uses CallerKind::Plugin. The model router's keep-free
    // share is separate from the allocator's fleet-wide one-percent reserve.
    let params = legacy_request("head-import", "opencode");
    let answer = result(runner.call("route.account_for", &params).await);
    assert_eq!(answer["account"]["credential_id"], "credential-2");
    assert_eq!(
        diagnostics(&subject, &answer).0["import"]["kind"],
        "accepted"
    );
}

async fn legacy_import_rejected(case: &str, used: f64, caller: &str, expected: &str) {
    let (subject, runner) = setup("legacy-import-limits").await;
    subject.harness().edit_config(|config| {
        config["providers"][fixture::SUBSCRIPTION_PROVIDER]["accounts"] = match case {
            "soft" => json!({"soft_quota_remaining_percent":20}),
            "pool" => json!({"floors":[{"window_minutes":300,"remaining_percent":20}]}),
            _ => json!({}),
        };
    });
    quota_used(&subject, used);
    let answer = result(
        runner
            .call("route.account_for", &legacy_request(case, caller))
            .await,
    );
    assert_eq!(
        answer["account"]["credential_id"], "credential-1",
        "{case}: {answer}"
    );
    let (explanation, _) = diagnostics(&subject, &answer);
    assert_eq!(
        explanation["import"]["kind"], "rejected",
        "{case}: {explanation}"
    );
    assert!(
        explanation["import"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason["reason"] == expected),
        "{case}: {explanation}"
    );
}

#[tokio::test]
async fn legacy_import_allocator_reserve_is_rejected() {
    legacy_import_rejected("reserve", 99.5, "opencode", "below_reserve").await;
}

#[tokio::test]
async fn legacy_import_soft_floor_is_rejected() {
    legacy_import_rejected("soft", 90.0, "opencode", "policy_floor").await;
}

#[tokio::test]
async fn legacy_import_pool_floor_rejection_is_recorded() {
    legacy_import_rejected("pool", 90.0, "opencode", "policy_floor").await;
}

#[tokio::test]
async fn legacy_import_broca_cold_floor_is_rejected() {
    legacy_import_rejected("broca", 97.0, "broca", "below_broca_floor").await;
}

#[tokio::test]
async fn warm_credential_candidates_unknown_and_explicit_pin_use_normal_policy() {
    for case in ["candidates", "unknown", "pin"] {
        let (subject, runner) = setup("warm-filtered").await;
        let mut params = request(case);
        match case {
            "candidates" => {
                params["candidates"] = json!({"view_digest":"only-one", "accounts":[
                {"account_id":"account-1","credential_id":"credential-1"}]})
            }
            "unknown" => params["warm_credential"] = json!("unknown-credential"),
            "pin" => params["requested_account"] = json!({"account_id":"account-1"}),
            _ => unreachable!(),
        }
        let answer = result(runner.call("route.account_for", &params).await);
        assert_eq!(
            answer["account"]["credential_id"], "credential-1",
            "{case}: {answer}"
        );
        let (explanation, context) = diagnostics(&subject, &answer);
        if case == "pin" {
            assert!(explanation["import"].is_null());
            assert_eq!(
                context["warm_credential"]["result"]["reason"],
                "explicit_pin"
            );
        } else {
            assert_eq!(explanation["import"]["kind"], "rejected");
            if case == "unknown" {
                assert_eq!(explanation["import"]["not_in_roster"], true);
            } else {
                assert_eq!(
                    explanation["import"]["reasons"][0]["reason"],
                    "not_in_runner_candidates"
                );
            }
        }
    }
}

#[tokio::test]
async fn warm_credential_legacy_fingerprint_conflicts_on_changed_hint() {
    let (_subject, runner) = setup("warm-legacy-fingerprint").await;
    let mut params = request("fingerprint");
    params["caller"] = json!({"runner":fixture::RUNNER_ONE});
    let admitted = runner.call("route.account_for", &params).await;
    result(admitted.clone());
    params["warm_credential"] = json!("credential-1");
    assert_eq!(
        runner.call("route.account_for", &params).await.1["code"],
        "admission_conflict"
    );
    params["warm_credential"] = json!("credential-2");
    assert_eq!(runner.call("route.account_for", &params).await, admitted);
}

#[tokio::test]
async fn warm_credential_outside_decision_credentials_is_ignored() {
    let (subject, runner) = setup("warm-decision-credentials").await;
    let consumer = subject
        .harness()
        .bind(reserved(fixture::CONSUMER), None)
        .await;
    let selected = result(
        consumer
            .call(
                "route.select",
                &json!({"consumer":fixture::SUMMARY,
        "pin":fixture::HOLD_FIRST,"requirements":{"iq":50}}),
            )
            .await,
    );
    subject
        .harness()
        .store()
        .record_service_tier(
            selected["decision_id"].as_str().unwrap(),
            "standard",
            &["credential-1".into()],
        )
        .unwrap();
    let mut params = request("decision-credentials");
    params["decision_id"] = selected["decision_id"].clone();
    let answer = result(runner.call("route.account_for", &params).await);
    assert_eq!(answer["account"]["credential_id"], "credential-1");
    // A decision-bound wire answer names the model decision, not the allocator
    // decision. This fixture has exactly one admission, so inspect that row.
    let rows = subject
        .harness()
        .store()
        .account_decisions(striatum_store::DECISION_MODE_SERVE, 2)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let decision: Value = serde_json::from_str(&rows[0].decision_json).unwrap();
    assert_eq!(decision["explanation"]["import"]["kind"], "rejected");
    assert_eq!(decision["explanation"]["import"]["not_in_roster"], true);
}

#[tokio::test]
async fn legacy_import_missing_quota_uses_cold_eligibility() {
    let (subject, runner) = setup("legacy-import-no-reading").await;
    crate::ACCOUNT_QUOTA
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .readings
        .readings
        .remove(&(QUOTA_SOURCE.into(), "account-2".into()));
    // Cold placement treats no reading as unknown, not exhaustion. Importing
    // the caller's local account must use that same eligibility rule.
    let answer = result(
        runner
            .call(
                "route.account_for",
                &legacy_request("no-reading", "opencode"),
            )
            .await,
    );
    assert_eq!(answer["account"]["credential_id"], "credential-2");
    assert_eq!(
        diagnostics(&subject, &answer).0["import"]["kind"],
        "accepted"
    );
}
