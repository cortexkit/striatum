use super::*;

async fn observed(route: &super::super::super::conformance_tests::Route, params: &Value) -> Value {
    result(route.call("route.account_observe", params).await)["observation"].clone()
}

fn credential_block(h: &Harness, credential: &str, at: i64) {
    h.store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: POOL.into(),
                credential_id: credential.into(),
                code: "credential_refused".into(),
            },
            None,
            at,
        )
        .unwrap();
}

fn assert_blocked(h: &Harness, credential: &str, at: i64) {
    let fences = h.store().credential_entitlement_fences().unwrap();
    assert!(
        fences
            .iter()
            .any(|f| f.credential_id == credential && f.observed_at_ms == at),
        "expected {credential} at {at}, got {fences:?}"
    );
}

#[tokio::test]
async fn observe_success_exact_identity_and_both_shapes() {
    for legacy in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let h = paid_anthropic_harness();
        let at = NOW as i64 - 3_600_001;
        credential_block(&h, KEYS[0], at);
        credential_block(&h, KEYS[1], at);
        let route = h.bind(reserved("broca"), None).await;
        result(
            route
                .call("route.account_for", &single_key_request("identity"))
                .await,
        );
        for (index, field, value) in [
            (0, "served_account_id", KEYS[1]),
            (1, "credential_id", KEYS[1]),
            (2, "admission_id", "not-issued"),
        ] {
            let mut params = success_observation("identity", legacy);
            params["event_id"] = json!(format!("mismatch-{index}"));
            params[field] = json!(value);
            let answer = observed(&route, &params).await;
            assert!(!answer["effects"].to_string().contains("block_cleared"));
            assert_blocked(&h, KEYS[0], at);
            assert_blocked(&h, KEYS[1], at);
        }
        let mut params = success_observation("identity", legacy);
        if legacy {
            params["attempt_outcome"]["status"] = json!("served_empty");
        }
        let answer = observed(&route, &params).await;
        assert!(!answer["effects"].as_array().unwrap().is_empty());
        assert_eq!(h.store().credential_entitlement_fences().unwrap().len(), 1);
        assert_blocked(&h, KEYS[1], at);
    }
}

#[tokio::test]
async fn observe_success_newer_block_survives_admission_cutoff() {
    for legacy in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let h = paid_anthropic_harness();
        credential_block(&h, KEYS[0], NOW as i64 - 3_600_001);
        let route = h.bind(reserved("broca"), None).await;
        result(
            route
                .call("route.account_for", &single_key_request("older"))
                .await,
        );
        credential_block(&h, KEYS[0], NOW as i64 + 1);
        let answer = observed(&route, &success_observation("older", legacy)).await;
        assert_eq!(answer["effects"], json!([]));
        assert_blocked(&h, KEYS[0], NOW as i64 + 1);
        let _later = crate::InjectedClock::set(NOW + 3_600_002);
        result(
            route
                .call("route.account_for", &single_key_request("newer"))
                .await,
        );
        assert!(
            !observed(&route, &success_observation("newer", legacy)).await["effects"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(h
            .store()
            .credential_entitlement_fences()
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn observe_success_duplicate_applies_once_even_after_reblock() {
    for legacy in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let h = paid_anthropic_harness();
        credential_block(&h, KEYS[0], NOW as i64 - 3_600_001);
        let route = h.bind(reserved("broca"), None).await;
        result(
            route
                .call("route.account_for", &single_key_request("duplicate"))
                .await,
        );
        let params = success_observation("duplicate", legacy);
        let first = observed(&route, &params).await;
        assert!(!first["effects"].as_array().unwrap().is_empty());
        let replay = observed(&route, &params).await;
        assert_eq!(replay["id"], first["id"]);
        assert_eq!(replay["duplicate"], true);
        assert_eq!(replay["effects"], json!([]));
        let mut other_shape = params.clone();
        if legacy {
            other_shape.as_object_mut().unwrap().remove("caller");
        } else {
            other_shape["caller"] = json!({"runner":"broca"});
        }
        let replay = observed(&route, &other_shape).await;
        assert_eq!(replay["id"], first["id"]);
        assert_eq!(replay["duplicate"], true);
        assert_eq!(replay["effects"], json!([]));
        credential_block(&h, KEYS[0], NOW as i64 + 1);
        let replay = observed(&route, &params).await;
        assert_eq!(replay["duplicate"], true);
        assert_eq!(replay["effects"], json!([]));
        assert_blocked(&h, KEYS[0], NOW as i64 + 1);
    }
}

#[tokio::test]
async fn observe_success_member_requires_advertised_group_in_both_shapes() {
    let h = paid_anthropic_harness();
    let route = h.bind(reserved("broca"), None).await;
    super::super::super::describe::without_capability("account_observe_success", async {
        for legacy in [false, true] {
            let (frame, body) = route
                .call(
                    "route.account_observe",
                    &success_observation("unissued", legacy),
                )
                .await;
            assert_eq!(frame, FrameType::Error, "{body}");
            assert_eq!(body["code"], "invalid_request");
            assert_eq!(body["detail"]["field"], "attempt_outcome");
        }
    })
    .await;
}

#[tokio::test]
async fn observe_success_status_rules_apply_to_both_shapes() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    credential_block(&h, KEYS[0], NOW as i64 - 3_600_001);
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("status"))
            .await,
    );
    for legacy in [false, true] {
        for status in [
            "quota",
            "throttled",
            "credential",
            "unavailable",
            "rejected",
        ] {
            let mut params = success_observation("status", legacy);
            params["attempt_outcome"]["status"] = json!(status);
            let (frame, body) = route.call("route.account_observe", &params).await;
            assert_eq!(frame, FrameType::Error, "{body}");
            assert_eq!(body["detail"]["field"], "attempt_outcome.status");
        }
        let mut params = success_observation("status", legacy);
        params["event_id"] = json!(format!("unknown-{legacy}"));
        params["attempt_outcome"]["status"] = json!("future");
        assert_eq!(observed(&route, &params).await["effects"], json!([]));
        assert_blocked(&h, KEYS[0], NOW as i64 - 3_600_001);
    }
}

#[tokio::test]
async fn observe_success_whole_account_probe_preserves_unknown_named_pool() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let at = NOW as i64 - 900_001;
    let insert = |id: &str, pool: Option<striatum_core::accounts::PoolKey>| {
        let fence = Fence {
            account_id: KEYS[0].into(),
            pool: pool.clone(),
            recovery_set: Vec::new(),
            observed_at_ms: at,
            reset: ResetTime::Missing,
            retry_hint_ms: None,
            source: ConfirmationSource::BrocaAccountLimit,
        };
        h.store()
            .account_transaction(|tx| {
                tx.insert_pool_fence(&striatum_store::AccountPoolFenceRow {
                    evidence: AccountFenceRow {
                        fence_id: id.into(),
                        provider: POOL.into(),
                        account_id: KEYS[0].into(),
                        scope_json: "null".into(),
                        fence_json: serde_json::to_string(&fence).unwrap(),
                        source: "broca_limit_exhausted".into(),
                        observed_at: at,
                        principal: "reserved:broca".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: at,
                        cleared_at: None,
                        cleared_reason: None,
                    },
                    pool_key: pool,
                    recovery_set: Vec::new(),
                })
            })
            .unwrap();
    };
    insert("whole", None);
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("unknown-pool"))
            .await,
    );
    let mut named: striatum_core::accounts::PoolKey =
        FenceScope::General(GeneralWindow::FiveHour).into();
    named.name = Some("sibling-pool".into());
    insert("named", Some(named));
    let sibling_hold = format!("credential-quota:{}:hold", json!([POOL, KEYS[1]]));
    insert(
        &sibling_hold,
        Some(FenceScope::General(GeneralWindow::FiveHour).into()),
    );
    let answer = observed(&route, &success_observation("unknown-pool", true)).await;
    assert!(answer["effects"].to_string().contains("whole"), "{answer}");
    assert!(!answer["effects"].to_string().contains("named"), "{answer}");
    h.store()
        .account_read(|tx| {
            let fences = tx.active_fences(POOL)?;
            assert_eq!(fences.len(), 1);
            assert_eq!(fences[0].fence_id, "named");
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn observe_success_quota_is_shared_by_credentials_but_not_accounts() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let credentials = h.credentials();
    let mut listing = credentials.lock().unwrap();
    let mut rows = listing.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|r| r.id == KEYS[1])
        .unwrap()
        .account_id = Some(KEYS[0].into());
    let mut other = rows.iter().find(|r| r.id == KEYS[0]).unwrap().clone();
    other.id = "apikey:anthropic:other".into();
    rows.push(other);
    listing.apply_listing(
        &ScopedCredentialListing {
            view: "shared-quota".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    drop(listing);
    let at = NOW as i64 - 900_001;
    for account in [KEYS[0], "apikey:anthropic:other"] {
        let scope = FenceScope::General(GeneralWindow::FiveHour);
        let fence = Fence {
            account_id: account.into(),
            pool: Some(scope.into()),
            recovery_set: Vec::new(),
            observed_at_ms: at,
            reset: ResetTime::Missing,
            retry_hint_ms: None,
            source: ConfirmationSource::BrocaAccountLimit,
        };
        h.store()
            .account_transaction(|tx| {
                tx.insert_fence(&AccountFenceRow {
                    fence_id: format!("shared/{account}"),
                    provider: POOL.into(),
                    account_id: account.into(),
                    scope_json: serde_json::to_string(&scope).unwrap(),
                    fence_json: serde_json::to_string(&fence).unwrap(),
                    source: "broca_limit_exhausted".into(),
                    observed_at: at,
                    principal: "reserved:broca".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: at,
                    cleared_at: None,
                    cleared_reason: None,
                })
            })
            .unwrap();
    }
    let route = h.bind(reserved("broca"), None).await;
    let mut request = single_key_request("shared-quota");
    request["candidates"]["accounts"][0]["credential_id"] = json!(KEYS[1]);
    let admitted = result(route.call("route.account_for", &request).await);
    assert_eq!(admitted["account"]["account_id"], KEYS[0]);
    assert_eq!(admitted["account"]["credential_id"], KEYS[1]);
    let mut success = success_observation("shared-quota", true);
    success["credential_id"] = json!(KEYS[1]);
    assert!(!observed(&route, &success).await["effects"]
        .as_array()
        .unwrap()
        .is_empty());
    result(
        route
            .call("route.account_for", &single_key_request("shared-now-open"))
            .await,
    );
    h.store()
        .account_read(|tx| {
            let fences = tx.active_fences(POOL)?;
            assert_eq!(fences.len(), 1);
            assert_eq!(fences[0].account_id, "apikey:anthropic:other");
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn expired_transient_backoff_reopens_all_credentials() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    for credential in KEYS {
        h.store()
            .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
                provider: POOL.into(),
                credential_id: credential.into(),
                model: MODEL.into(),
                until_ms: NOW as i64 - 1,
                recorded_at: NOW as i64 - 3_600_001,
                decision_id: "earlier-refusal".into(),
            })
            .unwrap();
    }
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("backoff-probe"))
            .await,
    );
    let answer = observed(&route, &success_observation("backoff-probe", true)).await;
    assert!(
        !answer["effects"].to_string().contains("transient_route"),
        "{answer}"
    );
    let origins = h.store().fence_rechecks().unwrap();
    assert!(origins.is_empty(), "{origins:?}");
}

async fn assert_sole_offered_block_is_timed(kind: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let route = h.bind(reserved("broca"), None).await;
    let until = if matches!(kind, "credit" | "credential_refused") {
        NOW + 3_600_000 - 1
    } else {
        NOW + 5000
    };
    match kind {
        "quota" => {
            let scope = FenceScope::General(GeneralWindow::FiveHour);
            let fence = Fence {
                account_id: KEYS[0].into(),
                pool: Some(scope.into()),
                recovery_set: Vec::new(),
                observed_at_ms: NOW as i64 - 1,
                reset: ResetTime::At(until as i64),
                retry_hint_ms: None,
                source: ConfirmationSource::BrocaAccountLimit,
            };
            h.store()
                .account_transaction(|tx| {
                    tx.insert_fence(&AccountFenceRow {
                        fence_id: "sole-offered-quota".into(),
                        provider: POOL.into(),
                        account_id: KEYS[0].into(),
                        scope_json: serde_json::to_string(&scope).unwrap(),
                        fence_json: serde_json::to_string(&fence).unwrap(),
                        source: "broca_limit_exhausted".into(),
                        observed_at: NOW as i64 - 1,
                        principal: "reserved:broca".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: NOW as i64 - 1,
                        cleared_at: None,
                        cleared_reason: None,
                    })
                })
                .unwrap();
        }
        "credit" | "credential_refused" => {
            h.store()
                .record_credential_entitlement_fence(
                    &CredentialRefusal {
                        provider_id: POOL.into(),
                        credential_id: KEYS[0].into(),
                        code: if kind == "credit" {
                            "billing_exhausted"
                        } else {
                            kind
                        }
                        .into(),
                    },
                    None,
                    NOW as i64 - 1,
                )
                .unwrap();
        }
        "transient_route" => {
            h.store()
                .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
                    provider: POOL.into(),
                    credential_id: KEYS[0].into(),
                    model: MODEL.into(),
                    until_ms: until as i64,
                    recorded_at: NOW as i64 - 1,
                    decision_id: "real-transient-refusal".into(),
                })
                .unwrap();
        }
        "transient_account" => {
            result(
                route
                    .call("route.account_for", &single_key_request("block-source"))
                    .await,
            );
            observe_ok(
                &route,
                &json!({"event_id":"account-backoff","kind":"transient_backoff",
                "admission_id":"block-source","served_account_id":KEYS[0],"credential_id":KEYS[0],
                "retry_at_ms":until}),
            )
            .await;
        }
        _ => panic!("unknown block kind {kind}"),
    }
    let mut request = single_key_request("blocked-listed-key");
    request["hold_wait_max_s"] = json!(0);
    let (frame, body) = route.call("route.account_for", &request).await;
    assert_eq!(frame, FrameType::Error, "{kind}: {body}");
    assert_eq!(body["code"], "account_pool_exhausted", "{kind}: {body}");
    assert_eq!(body["detail"]["reset_at_ms"], until, "{kind}: {body}");
}

#[tokio::test]
async fn sole_offered_quota_block_returns_timed_pool_exhaustion() {
    assert_sole_offered_block_is_timed("quota").await;
}

#[tokio::test]
async fn sole_offered_credit_block_returns_timed_pool_exhaustion() {
    assert_sole_offered_block_is_timed("credit").await;
}

#[tokio::test]
async fn sole_offered_credential_refused_block_returns_timed_pool_exhaustion() {
    assert_sole_offered_block_is_timed("credential_refused").await;
}

#[tokio::test]
async fn sole_offered_transient_route_block_returns_timed_pool_exhaustion() {
    assert_sole_offered_block_is_timed("transient_route").await;
}

#[tokio::test]
async fn sole_offered_transient_account_block_returns_timed_pool_exhaustion() {
    assert_sole_offered_block_is_timed("transient_account").await;
}

async fn assert_sibling_block_scope(kind: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let credentials = h.credentials();
    let mut listing = credentials.lock().unwrap();
    let mut rows = listing.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|r| r.id == KEYS[1])
        .unwrap()
        .account_id = Some(KEYS[0].into());
    let mut other = rows.iter().find(|r| r.id == KEYS[0]).unwrap().clone();
    other.id = "apikey:anthropic:other".into();
    rows.push(other);
    listing.apply_listing(
        &ScopedCredentialListing {
            view: "sibling-block-scope".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    drop(listing);
    let until = NOW + 5000;
    let fence = striatum_store::AccountRouteFence {
        provider: POOL.into(),
        credential_id: KEYS[0].into(),
        model: MODEL.into(),
        until_ms: until as i64,
        recorded_at: NOW as i64 - 1,
        decision_id: "credential-scoped-refusal".into(),
    };
    match kind {
        "quota" => h.store().record_account_route_fence(&fence),
        "credit" => h.store().record_billing_account_route_fence(&fence),
        "transient_route" => h.store().record_transient_account_route_fence(&fence),
        _ => unreachable!(),
    }
    .unwrap();
    let route = h.bind(reserved("broca"), None).await;
    let mut sibling = single_key_request("sibling-unblocked");
    sibling["hold_wait_max_s"] = json!(0);
    sibling["candidates"]["accounts"][0]["credential_id"] = json!(KEYS[1]);
    let reply = route.call("route.account_for", &sibling).await;
    if kind == "quota" {
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "account_pool_exhausted", "{}", reply.1);
        assert_eq!(reply.1["detail"]["reset_at_ms"], until, "{}", reply.1);
        return;
    }
    let admitted = result(reply);
    assert_eq!(admitted["account"]["credential_id"], KEYS[1]);
    let mut success = success_observation("sibling-unblocked", true);
    success["credential_id"] = json!(KEYS[1]);
    assert_eq!(observed(&route, &success).await["effects"], json!([]));
    let mut blocked = single_key_request("original-still-blocked");
    blocked["hold_wait_max_s"] = json!(0);
    let (frame, body) = route.call("route.account_for", &blocked).await;
    assert_eq!(frame, FrameType::Error, "{body}");
    assert_eq!(body["code"], "account_pool_exhausted", "{body}");
    assert_eq!(body["detail"]["reset_at_ms"], until, "{body}");
}

#[tokio::test]
async fn same_account_sibling_is_admitted_under_transient_backoff() {
    assert_sibling_block_scope("transient_route").await;
}

#[tokio::test]
async fn same_account_sibling_is_admitted_under_credit_route_block() {
    assert_sibling_block_scope("credit").await;
}

#[tokio::test]
async fn same_account_sibling_remains_blocked_under_shared_quota() {
    assert_sibling_block_scope("quota").await;
}

fn observation_backoff(h: &Harness, id: &str, account: &str, at: i64) {
    let effects = json!([{"effect":"transient_backoff", "provider":POOL,
        "account_id":account, "retry_at_ms":at + 5_000}]);
    h.store()
        .account_transaction(|tx| {
            tx.insert_observation(&striatum_store::AccountObservationRow {
                observation_id: id.into(),
                principal: "reserved:broca".into(),
                runner: "broca".into(),
                event_id: id.into(),
                kind: "transient_backoff".into(),
                admission_id: None,
                bound: true,
                observation_json: "{}".into(),
                effects_json: effects.to_string(),
                received_at: at,
            })
        })
        .unwrap();
}

fn observation_rows(h: &Harness, table: &str, account: &str) -> i64 {
    let conn = rusqlite::Connection::open(h.store_path()).unwrap();
    let source = if table == "block" {
        " AND is_origin=1 AND source_present=1"
    } else {
        ""
    };
    conn.query_row(
        &format!("SELECT count(*) FROM {table} WHERE kind='observation' AND origin_key=?1{source}"),
        [json!([POOL, account]).to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

#[tokio::test]
async fn expired_observation_retires_origin_without_probe_authorizations() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let evidence_at = NOW as i64 - 2 * 86_400_000;
    observation_backoff(&h, "live-shaped-backoff", KEYS[0], evidence_at);
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call(
                "route.account_for",
                &single_key_request("observation-probe"),
            )
            .await,
    );
    assert_eq!(observation_rows(&h, "probe_authorization", KEYS[0]), 0);
    let answer = observed(&route, &success_observation("observation-probe", true)).await;
    assert!(
        !answer["effects"].to_string().contains("block_cleared"),
        "{answer}"
    );
    assert_eq!(
        observation_rows(&h, "block", KEYS[0]),
        0,
        "expiry retires the source, not just its clock"
    );
    assert_eq!(observation_rows(&h, "retirement_tombstone", KEYS[0]), 1);
    assert_eq!(observation_rows(&h, "probe_authorization", KEYS[0]), 0);
    assert!(h
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "observation"));
}

#[tokio::test]
async fn expired_observation_needs_no_claim_or_exact_credential() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("not-a-probe"))
            .await,
    );
    // Delayed evidence is older than both admissions, but only the second
    // admission is authorized to test it.
    observation_backoff(&h, "delayed-backoff", KEYS[0], NOW as i64 - 6_000);
    let _later = crate::InjectedClock::set(NOW + 1);
    result(
        route
            .call("route.account_for", &single_key_request("actual-probe"))
            .await,
    );
    let mut wrong_credential = success_observation("actual-probe", true);
    wrong_credential["credential_id"] = json!(KEYS[1]);
    observed(&route, &wrong_credential).await;
    let mut sibling = single_key_request("other-credential");
    sibling["candidates"]["accounts"][0] = json!({"account_id":KEYS[1], "credential_id":KEYS[1]});
    result(route.call("route.account_for", &sibling).await);
    let mut sibling_success = success_observation("other-credential", true);
    sibling_success["served_account_id"] = json!(KEYS[1]);
    sibling_success["credential_id"] = json!(KEYS[1]);
    observed(&route, &sibling_success).await;
    observed(&route, &success_observation("not-a-probe", true)).await;
    assert_eq!(observation_rows(&h, "retirement_tombstone", KEYS[0]), 1);
    assert_eq!(observation_rows(&h, "block", KEYS[0]), 0);
    assert_eq!(
        h.store()
            .fence_rechecks()
            .unwrap()
            .iter()
            .filter(|origin| origin.kind == "observation")
            .count(),
        0
    );
    let mut valid = success_observation("actual-probe", true);
    valid["event_id"] = json!("actual-probe-valid");
    observed(&route, &valid).await;
    assert_eq!(observation_rows(&h, "retirement_tombstone", KEYS[0]), 1);
}

#[tokio::test]
async fn observe_success_observation_newer_than_admission_survives() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    observation_backoff(&h, "old-backoff", KEYS[0], NOW as i64 - 6_000);
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("older-probe"))
            .await,
    );
    observation_backoff(&h, "newer-backoff", KEYS[0], NOW as i64 + 1);
    observed(&route, &success_observation("older-probe", true)).await;
    assert_eq!(observation_rows(&h, "retirement_tombstone", KEYS[0]), 1);
    assert_eq!(observation_rows(&h, "block", KEYS[0]), 1);
}

#[tokio::test]
async fn observe_success_observation_retirement_rejects_old_replay_but_accepts_new_evidence() {
    let _clock = crate::InjectedClock::set(NOW);
    let h = paid_anthropic_harness();
    let at = NOW as i64 - 6_000;
    observation_backoff(&h, "retired-backoff", KEYS[0], at);
    let route = h.bind(reserved("broca"), None).await;
    result(
        route
            .call("route.account_for", &single_key_request("replay-probe"))
            .await,
    );
    observed(&route, &success_observation("replay-probe", true)).await;
    assert_eq!(observation_rows(&h, "block", KEYS[0]), 0);
    let effects = json!([{"effect":"transient_backoff", "provider":POOL,
        "account_id":KEYS[0], "retry_at_ms":at + 5_000}])
    .to_string();
    h.store()
        .account_transaction(|tx| tx.set_observation_effects("retired-backoff", true, &effects))
        .unwrap();
    observation_backoff(&h, "older-delayed-backoff", KEYS[0], at - 1);
    assert_eq!(
        observation_rows(&h, "block", KEYS[0]),
        0,
        "a retired generation and older evidence must stay retired"
    );
    observation_backoff(&h, "fresh-backoff", KEYS[0], NOW as i64 + 1);
    assert_eq!(observation_rows(&h, "block", KEYS[0]), 1);
    assert_eq!(observation_rows(&h, "retirement_tombstone", KEYS[0]), 1);
    assert_eq!(
        h.store()
            .fence_rechecks()
            .unwrap()
            .iter()
            .filter(|origin| origin.kind == "observation")
            .count(),
        1
    );
}
