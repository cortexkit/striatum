use super::*;

fn signed_out(mut request: Value) -> Value {
    let evidence = request["exhausted"].as_object_mut().unwrap();
    evidence.insert("limit_window".into(), json!("signed_out"));
    evidence.remove("resets_at_ms");
    request
}

fn fresh_request(id: &str) -> Value {
    json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "admission_id":id,"conversation_key":id,"initial_input_bytes":65536})
}

fn relist(subject: &KitSubject, credential: &str, state: &str, new_record: bool) {
    let harness = subject.harness();
    let shared = harness.broca_credentials.lock().unwrap().clone();
    let mut credentials = shared.lock().unwrap();
    let mut rows = credentials.listed_credentials().to_vec();
    let row = rows.iter_mut().find(|row| row.id == credential).unwrap();
    row.state = state.into();
    if new_record {
        row.record_version += 1;
    }
    let listing = ScopedCredentialListing {
        view: format!(
            "signed-out-{}",
            NEXT_HARNESS.fetch_add(1, Ordering::Relaxed)
        ),
        credentials: rows,
        ..Default::default()
    };
    credentials.apply_listing(&listing, crate::now_ms());
    account_quota::apply_roster_listing(
        &mut crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap(),
        &listing,
        account_selection::now_ms(),
    );
}

fn age_fence(subject: &KitSubject) {
    let harness = subject.harness();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    conn.execute(
        "UPDATE credential_entitlement_fences SET observed_at_ms=?1 WHERE code='signed_out'",
        [account_selection::now_ms() - 3_600_001],
    )
    .unwrap();
}

fn target_original(request: &Value, id: &str) -> Value {
    let mut fresh = fresh_request(id);
    fresh["candidates"] = json!({"view_digest":"signed-out-test", "accounts":[{"account_id":request["exhausted"]["account"],"credential_id":request["exhausted"]["credential_id"]}]});
    fresh
}

#[test]
fn signed_out_undeclared_refusal_is_byte_identical_to_unknown_window() {
    use cortexkit_role_routing::{
        account_continue::parse_continue_request, describe::RoleDescribe,
    };
    let describe = RoleDescribe::new(Vec::new(), true).with_capability("account_continue");
    let request = json!({"mode":"continue","run_id":"run","admission_id":"admission",
        "conversation_key":"conversation","previous_decision_id":"decision","selection_id":"selection",
        "exhausted":{"credential_id":"credential","account":"account","limit_window":"signed_out","observed_at_ms":1}});
    let refusal = parse_continue_request(&request, &describe).unwrap_err();
    let mut unknown = request;
    unknown["exhausted"]["limit_window"] = json!("unknown");
    let control = parse_continue_request(&unknown, &describe).unwrap_err();
    assert_eq!(
        serde_json::to_vec(&refusal).unwrap(),
        serde_json::to_vec(&control).unwrap()
    );
    assert_eq!(refusal.detail["field"], "exhausted.limit_window");
}

#[tokio::test]
async fn signed_out_moves_and_preserves_continuation_reservations_and_resume() {
    let (subject, consumer, runner, request) = paused().await;
    let request = signed_out(request);
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_ne!(
        offer["credential_id"],
        request["exhausted"]["credential_id"]
    );
    assert_eq!(
        result(runner.call("route.account_for", &request).await),
        offer
    );
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&offer, "admission_started", "signed-out-moved"),
            )
            .await,
    );
    assert_eq!(
        replay_original(&runner, &request).await["reason"]["decision_id"],
        offer["decision_id"]
    );
    result(runner.call("route.account_observe", &json!({"event_id":"signed-out-finished","kind":"run_finished","admission_id":"admission","run_id":"run"})).await);
    assert_eq!(offer_state(&subject, &offer).2, "finished");
    let describe = result(consumer.call("role.describe", &json!({})).await);
    assert!(describe["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("account_continue_signed_out")));
}

#[tokio::test]
async fn signed_out_no_spare_holds_without_waiting_for_its_own_fence() {
    let (subject, _, runner, request) = paused().await;
    subject
        .configure_continue_credentials(&fixture::continue_credentials()[..1])
        .await
        .unwrap();
    let answer = result(runner.call("route.account_for", &signed_out(request)).await);
    assert_eq!(answer["kind"], "hold", "{answer}");
    assert_eq!(answer["routing_code"], "account_pool_exhausted", "{answer}");
    assert!(
        answer.get("until_ms").is_none(),
        "the signed-out credential is not a wake target: {answer}"
    );
}

#[tokio::test]
async fn signed_out_fences_fresh_admissions_for_every_runner_without_credential_refused() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer");
    for (index, route) in [
        runner,
        subject
            .harness()
            .bind(reserved(fixture::RUNNER_TWO), None)
            .await,
    ]
    .iter()
    .enumerate()
    {
        let fresh = target_original(&request, &format!("signed-out-fresh-{index}"));
        let reply = route.call("route.account_for", &fresh).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "account_pool_exhausted", "{reply:?}");
        assert!(reply.1["detail"]["reset_at_ms"].as_i64().is_some());
    }
    let fences = subject
        .harness()
        .store()
        .credential_entitlement_fences()
        .unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].code, "signed_out");
    let conn = rusqlite::Connection::open(subject.harness().store_path()).unwrap();
    let refused: i64 = conn
        .query_row(
            "SELECT count(*) FROM account_observation WHERE kind='credential_refused'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(refused, 0, "a refused sign-in is not a refused credential");
    let parked: i64 = conn
        .query_row(
            "SELECT count(*) FROM pool_key_state WHERE walled_until IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(parked, 0, "sign-in loss must not park an API key");
}

#[tokio::test]
async fn signed_out_vault_state_blocks_hourly_probe_until_sign_in_returns() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    result(runner.call("route.account_for", &request).await);
    let credential = request["exhausted"]["credential_id"].as_str().unwrap();
    relist(&subject, credential, "needs_reauth", false);
    age_fence(&subject);
    let denied = runner
        .call(
            "route.account_for",
            &target_original(&request, "still-signed-out"),
        )
        .await;
    assert_eq!(
        denied.0,
        FrameType::Error,
        "vault state cannot be probed: {denied:?}"
    );
    relist(&subject, credential, "active", false);
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &target_original(&request, "signed-back-in"),
            )
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], credential);
    assert!(subject
        .harness()
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn signed_out_new_sign_in_record_clears_fence_even_without_needs_reauth_poll() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    result(runner.call("route.account_for", &request).await);
    let credential = request["exhausted"]["credential_id"].as_str().unwrap();
    relist(&subject, credential, "active", true);
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &target_original(&request, "new-sign-in"),
            )
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], credential);
    assert!(subject
        .harness()
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
}

// CONTRACT.md 1269–1275 and 1311–1313 require vault-confirmed recovery. When
// the vault never marks the credential unusable, routing instead caps its own
// non-quota evidence at one hour and allows one probe to settle that disagreement.
#[tokio::test]
async fn signed_out_unchanged_usable_vault_departure_allows_one_probe_at_one_hour() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    result(runner.call("route.account_for", &request).await);
    let fence = subject
        .harness()
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .remove(0);
    let clocks = subject.harness().store().fence_rechecks().unwrap();
    let clock = clocks
        .iter()
        .find(|clock| {
            clock.kind == "credential"
                && clock.credential_id.as_deref() == Some(&fence.credential_id)
        })
        .unwrap();
    assert!(!clock.quota);
    assert_eq!(clock.next_recheck_at - fence.observed_at_ms, 3_600_000);
    assert_eq!(
        fence.observed_at_ms,
        request["exhausted"]["observed_at_ms"].as_i64().unwrap()
    );
    relist(&subject, &fence.credential_id, "active", false);
    assert_eq!(
        runner
            .call(
                "route.account_for",
                &target_original(&request, "before-cap")
            )
            .await
            .0,
        FrameType::Error
    );
    age_fence(&subject);
    let probe = result(
        runner
            .call(
                "route.account_for",
                &target_original(&request, "hour-probe"),
            )
            .await,
    );
    assert_eq!(
        probe["account"]["credential_id"],
        request["exhausted"]["credential_id"]
    );
    let denied = runner
        .call(
            "route.account_for",
            &target_original(&request, "second-probe"),
        )
        .await;
    assert_eq!(
        denied.0,
        FrameType::Error,
        "only one probe may claim the hour: {denied:?}"
    );
}

#[tokio::test]
async fn signed_out_is_never_reoffered_to_reporting_run_after_global_recovery() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    let offer = result(runner.call("route.account_for", &request).await);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&offer, "attempt", "used-signed-out-offer"),
            )
            .await,
    );
    let credential = request["exhausted"]["credential_id"].as_str().unwrap();
    let snapshot = subject
        .harness()
        .store()
        .account_evaluation_snapshot(account_selection::now_ms())
        .unwrap();
    let excluded = snapshot
        .account_read(|tx| tx.signed_out_credentials("reserved:runner-one", "admission", "run"))
        .unwrap();
    assert_eq!(
        excluded,
        vec![credential.to_owned()],
        "what-if must copy run-scoped sign-in evidence"
    );
    relist(&subject, credential, "active", true);
    result(
        runner
            .call(
                "route.account_for",
                &target_original(&request, "other-run-after-recovery"),
            )
            .await,
    );
    subject
        .harness()
        .store()
        .prune_account_observations(account_selection::now_ms() + 86_400_000, 10000)
        .unwrap();
    let mut credentials = fixture::continue_credentials();
    credentials.retain(|c| {
        c.account.credential_id == credential || c.account.credential_id == offer["credential_id"]
    });
    subject
        .configure_continue_credentials(&credentials)
        .await
        .unwrap();
    let mut next = request.clone();
    next["selection_id"] = json!("after-recovery");
    next["previous_decision_id"] = offer["decision_id"].clone();
    next["exhausted"] = json!({"credential_id":offer["credential_id"],"account":offer["account"],"limit_window":"five_hour","observed_at_ms":account_selection::now_ms(),"resets_at_ms":account_selection::now_ms()+300_000});
    let held = result(runner.call("route.account_for", &next).await);
    assert_eq!(
        held["kind"], "hold",
        "the run must remember its signed-out credential: {held}"
    );
    assert!(
        held.get("until_ms").is_none(),
        "an excluded credential is not a wake target: {held}"
    );
}

#[tokio::test]
async fn signed_out_hold_uses_only_alternate_accounts_known_reset() {
    let (subject, _, runner, request) = paused().await;
    let now = account_selection::now_ms() as u64 / 1000 * 1000;
    for credential in fixture::continue_credentials() {
        if credential.account.account_id != request["exhausted"]["account"] {
            let reset = if credential.account.account_id == "account-2" {
                now + 180_000
            } else {
                now + 240_000
            };
            subject
                .block_account(&credential.account.account_id, Some(reset))
                .await
                .unwrap();
        }
    }
    let held = result(runner.call("route.account_for", &signed_out(request)).await);
    assert_eq!(held["kind"], "hold", "{held}");
    assert_eq!(held["until_ms"], now + 180_000, "{held}");
}

#[tokio::test]
async fn signed_out_hold_wakes_for_an_alternate_credentials_recovery_probe() {
    let (subject, _, runner, request) = paused().await;
    subject
        .configure_continue_credentials(&fixture::continue_credentials()[..2])
        .await
        .unwrap();
    let harness = subject.harness();
    let credential = &fixture::continue_credentials()[1].account.credential_id;
    let crate::broca_credentials::ListingLookup::Listed(fingerprint) = harness
        .credentials()
        .lock()
        .unwrap()
        .signed_out_fingerprint(credential)
    else {
        panic!("listed credential");
    };
    let observed = account_selection::now_ms() - 1000;
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: fixture::SUBSCRIPTION_PROVIDER.into(),
                credential_id: credential.clone(),
                code: "signed_out".into(),
            },
            Some(&fingerprint),
            observed,
        )
        .unwrap();
    let held = result(runner.call("route.account_for", &signed_out(request)).await);
    assert_eq!(held["kind"], "hold", "{held}");
    assert_eq!(
        held["until_ms"],
        observed + 3_600_000,
        "the alternate's probe is a known wake target: {held}"
    );
}

#[tokio::test]
async fn signed_out_rejects_quota_members_even_when_null() {
    let (_, _, runner, request) = paused().await;
    for field in ["pool", "resets_at_ms", "retry_after_s"] {
        let mut request = signed_out(request.clone());
        request["exhausted"][field] = Value::Null;
        let reply = runner.call("route.account_for", &request).await;
        assert_eq!(reply.0, FrameType::Error);
        assert_eq!(reply.1["code"], "invalid_request");
        assert_eq!(reply.1["detail"]["field"], format!("exhausted.{field}"));
    }
}

#[tokio::test]
async fn signed_out_does_not_replace_or_postpone_an_independent_credential_refusal() {
    let (subject, _, runner, request) = paused().await;
    let request = signed_out(request);
    let harness = subject.harness();
    let credential = request["exhausted"]["credential_id"].as_str().unwrap();
    let crate::broca_credentials::ListingLookup::Listed(fingerprint) = harness
        .credentials()
        .lock()
        .unwrap()
        .listing_fingerprint(credential)
    else {
        panic!("listed credential");
    };
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: fixture::SUBSCRIPTION_PROVIDER.into(),
                credential_id: credential.into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            Some(&fingerprint),
            account_selection::now_ms() - 1000,
        )
        .unwrap();
    let original = harness.store().credential_entitlement_fences().unwrap();
    let clocks = harness.store().fence_rechecks().unwrap();
    assert_eq!(
        result(runner.call("route.account_for", &request).await)["kind"],
        "offer"
    );
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap(),
        original
    );
    let current = harness.store().fence_rechecks().unwrap();
    let before = clocks
        .iter()
        .find(|clock| clock.kind == "credential")
        .unwrap();
    let after = current
        .iter()
        .find(|clock| clock.kind == "credential")
        .unwrap();
    assert_eq!(before.next_recheck_at, after.next_recheck_at);
}
