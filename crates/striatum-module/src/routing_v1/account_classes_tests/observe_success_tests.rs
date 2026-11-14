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
            assert_eq!(fences.len(), 2);
            assert!(fences.iter().any(|fence| fence.fence_id == "named"));
            assert!(fences.iter().any(|fence| fence.fence_id == sibling_hold));
            Ok(())
        })
        .unwrap();
}
