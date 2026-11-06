use super::super::conformance_tests::Route;
use super::*;
use prefrontal_routing_core::route_state_store::CredentialRefusal;
use prefrontal_routing_store::PoolWallKind;

const POOL_KEY: &str = "apikey:pool-vendor:main";
const PLAIN_KEY: &str = "apikey:catalog:main";
const SIX_HOURS: i64 = 21_600_000;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    reply.1["result"].clone()
}

fn admit(id: &str, provider: &str, model: &str, credential: &str) -> Value {
    json!({"provider":provider, "model":model, "conversation_key":id,
        "admission_id":id, "requested_account":{"credential_id":credential}})
}

async fn observe(route: &Route, params: Value) -> Value {
    result(route.call("route.account_observe", &params).await)
}

#[tokio::test]
async fn credential_refused_runner_blocks_pool_and_non_pool_credentials() {
    for (provider, model, credential) in [
        (POOL, POOL_MODEL_SHORT, POOL_KEY),
        (BORROWED, "model-a", PLAIN_KEY),
    ] {
        let harness = harness_with_routed_accounts(Some(&["apikey"]));
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        result(
            runner
                .call(
                    "route.account_for",
                    &admit("first", provider, model, credential),
                )
                .await,
        );
        let params = json!({"event_id":"refused", "kind":"credential_refused",
            "admission_id":"first", "provider_code":"  credit_exhausted/Vendor  ",
            "observed_at_ms":1});
        let before = crate::now_ms() as i64;
        let observation = observe(&runner, params.clone()).await;
        assert_eq!(observation["observation"]["bound"], true);
        let duplicate = observe(&runner, params).await;
        assert_eq!(duplicate["observation"]["duplicate"], true);
        assert_eq!(duplicate["observation"]["effects"], json!([]));
        observe(
            &runner,
            json!({"event_id":"finish", "kind":"run_finished", "admission_id":"first"}),
        )
        .await;

        // After run_finished releases the first admission's pin, affinity must
        // not hide a missing block: a fresh admission must still refuse the
        // sole requested credential.
        let after = runner
            .call(
                "route.account_for",
                &admit("after", provider, model, credential),
            )
            .await;
        assert_eq!(after.0, FrameType::Error, "{after:?}");
        if provider == POOL {
            let states = harness.store().pool_key_states(provider, before).unwrap();
            let until = states[credential].walled_until_ms.unwrap();
            assert!(until >= before + SIX_HOURS);
            assert!(until <= crate::now_ms() as i64 + SIX_HOURS);
            assert!(harness
                .store()
                .credential_entitlement_fences()
                .unwrap()
                .is_empty());
            let blocked = harness
                .store()
                .pool_acquire(&prefrontal_routing_store::PoolAcquire {
                    provider,
                    session_id: "another-runner",
                    keys: &[prefrontal_routing_core::key_pool::PoolKey::new(
                        credential, None, "active",
                    )],
                    now_ms: until - 1,
                    lease_ms: 60_000,
                    affinity_idle_ms: 60_000,
                })
                .unwrap();
            assert!(matches!(
                blocked,
                prefrontal_routing_store::PoolAcquireOutcome::Exhausted(_)
            ));
            let reprobe = harness
                .store()
                .pool_acquire(&prefrontal_routing_store::PoolAcquire {
                    provider,
                    session_id: "reprobe",
                    keys: &[prefrontal_routing_core::key_pool::PoolKey::new(
                        credential, None, "active",
                    )],
                    now_ms: until,
                    lease_ms: 60_000,
                    affinity_idle_ms: 60_000,
                })
                .unwrap();
            assert!(matches!(
                reprobe,
                prefrontal_routing_store::PoolAcquireOutcome::Leased(_)
            ));
        } else {
            let fences = harness.store().credential_entitlement_fences().unwrap();
            let fence = fences
                .iter()
                .find(|f| f.credential_id == credential)
                .unwrap();
            assert_eq!(fence.code, "credential_refused");
            assert!(fence.observed_at_ms >= before);
            assert!(harness
                .store()
                .pool_key_states(provider, before)
                .unwrap()
                .is_empty());
            assert!(!crate::credential_fences::probe_allowed(
                &fence.code,
                true,
                SIX_HOURS - 1
            ));
            assert!(crate::credential_fences::probe_allowed(
                &fence.code,
                true,
                SIX_HOURS + 1
            ));
        }
        let stored = harness
            .store()
            .account_read(|tx| tx.observation_by_principal_event("reserved:zeta-runner", "refused"))
            .unwrap()
            .unwrap();
        let raw: Value = serde_json::from_str(&stored.observation_json).unwrap();
        assert_eq!(raw["provider_code"], "  credit_exhausted/Vendor  ");
    }
}

#[tokio::test]
async fn credential_refused_validates_target_retry_and_runner_authority() {
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let runner = harness.bind(reserved("zeta-runner"), None).await;
    for (params, field) in [
        (
            json!({"event_id":"missing", "kind":"credential_refused"}),
            "admission_id",
        ),
        (
            json!({"event_id":"retry", "kind":"credential_refused", "credential_id":POOL_KEY, "retry_at_ms":1}),
            "retry_at_ms",
        ),
    ] {
        let refused = runner.call("route.account_observe", &params).await;
        assert_eq!(refused.0, FrameType::Error, "{refused:?}");
        assert_eq!(refused.1["code"], "invalid_request", "{refused:?}");
        assert_eq!(refused.1["detail"]["field"], field, "{refused:?}");
    }
    let stranger = harness.bind(reserved("stranger"), None).await;
    let refused = stranger
        .call(
            "route.account_observe",
            &json!({"event_id":"foreign", "kind":"credential_refused", "credential_id":POOL_KEY}),
        )
        .await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    assert_eq!(refused.1["code"], "account_not_authorized");
    assert!(harness
        .store()
        .pool_key_states(POOL, crate::now_ms() as i64)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn credential_refused_explicit_target_wins_even_when_vault_marks_it_invalid() {
    for (provider, credential, pooled) in [
        ("refused-pool", "apikey:pool-vendor:invalid", true),
        (BORROWED, "apikey:catalog:invalid", false),
    ] {
        let mut refused_key = key(
            credential,
            &[provider],
            &[if pooled { POOL_VENDOR } else { BORROWED }],
        );
        refused_key.state = "invalid".into();
        let harness = harness_with_pool_keys(Some(&["apikey"]), &[refused_key]);
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        result(
            runner
                .call(
                    "route.account_for",
                    &admit("healthy", BORROWED, "model-a", PLAIN_KEY),
                )
                .await,
        );
        observe(
            &runner,
            json!({"event_id":"invalid-key", "kind":"credential_refused",
            "admission_id":"healthy", "credential_id":credential}),
        )
        .await;
        if pooled {
            assert!(harness
                .store()
                .pool_key_states(provider, crate::now_ms() as i64)
                .unwrap()[credential]
                .walled_until_ms
                .is_some());
        } else {
            assert!(harness
                .store()
                .credential_entitlement_fences()
                .unwrap()
                .iter()
                .any(|f| f.provider_id == provider && f.credential_id == credential));
        }
        // The explicit key, not the admission's healthy credential, was refused.
        observe(
            &runner,
            json!({"event_id":"healthy-finished", "kind":"run_finished", "admission_id":"healthy"}),
        )
        .await;
        result(
            runner
                .call(
                    "route.account_for",
                    &admit("healthy-again", BORROWED, "model-a", PLAIN_KEY),
                )
                .await,
        );
    }
}

#[tokio::test]
async fn credential_refused_decision_owner_blocks_only_carried_admissions() {
    for (provider, model, credential) in [
        (POOL, POOL_MODEL_SHORT, POOL_KEY),
        (BORROWED, "model-a", PLAIN_KEY),
    ] {
        let harness = harness_with_routed_accounts(Some(&["apikey"]));
        let owner = harness.bind(reserved("example-consumer"), None).await;
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        let mut request = select("mason");
        request["pin"] = json!(format!("{provider}/{model}"));
        let selected = result(owner.call("route.select", &request).await);
        let decision = selected["decision_id"].clone();
        let mut admission = admit("carried", provider, model, credential);
        admission["decision_id"] = decision.clone();
        result(runner.call("route.account_for", &admission).await);
        result(
            runner
                .call(
                    "route.account_for",
                    &admit("not-carried", provider, model, credential),
                )
                .await,
        );
        let mut params = json!({"event_id":"owner-wall", "kind":"credential_refused",
            "decision_id":decision, "admission_id":"not-carried"});
        let denied = owner.call("route.account_observe", &params).await;
        assert_eq!(denied.0, FrameType::Error, "{denied:?}");
        assert_eq!(denied.1["code"], "account_not_authorized");
        params["admission_id"] = json!("carried");
        let accepted = observe(&owner, params.clone()).await;
        assert_eq!(accepted["observation"]["bound"], true);
        assert!(!accepted["observation"]["effects"]
            .as_array()
            .unwrap()
            .is_empty());
        let standing = harness
            .store()
            .account_read(|tx| tx.admission("reserved:zeta-runner", "carried"))
            .unwrap()
            .unwrap();
        assert!(standing.released_at.is_none());
        params["event_id"] = json!("owner-finish");
        params["kind"] = json!("run_finished");
        let denied = owner.call("route.account_observe", &params).await;
        assert_eq!(denied.0, FrameType::Error, "{denied:?}");
        assert_eq!(denied.1["code"], "account_not_authorized");
        observe(
            &runner,
            json!({"event_id":"finish", "kind":"run_finished", "admission_id":"carried"}),
        )
        .await;
        let after = runner
            .call(
                "route.account_for",
                &admit("owner-after", provider, model, credential),
            )
            .await;
        assert_eq!(after.0, FrameType::Error, "{after:?}");
    }
}

#[tokio::test]
async fn credential_refused_extends_never_shortens_and_operator_clears() {
    for (provider, model, credential) in [
        (POOL, POOL_MODEL_SHORT, POOL_KEY),
        (BORROWED, "model-a", PLAIN_KEY),
    ] {
        let harness = harness_with_routed_accounts(Some(&["apikey"]));
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        result(
            runner
                .call(
                    "route.account_for",
                    &admit("direct", provider, model, credential),
                )
                .await,
        );
        observe(
            &runner,
            json!({"event_id":"direct-finished", "kind":"run_finished", "admission_id":"direct"}),
        )
        .await;
        let start = crate::now_ms() as i64;
        let seed = |at: i64| {
            if provider == POOL {
                harness
                    .store()
                    .pool_wall(
                        provider,
                        credential,
                        PoolWallKind::Billing,
                        at + SIX_HOURS,
                        "old",
                        at,
                    )
                    .unwrap();
            } else {
                harness
                    .store()
                    .record_credential_entitlement_fence(
                        &CredentialRefusal {
                            provider_id: provider.into(),
                            credential_id: credential.into(),
                            code: "credential_refused".into(),
                        },
                        None,
                        at,
                    )
                    .unwrap();
            }
        };
        let deadline = || {
            if provider == POOL {
                harness.store().pool_key_states(provider, start).unwrap()[credential]
                    .walled_until_ms
                    .unwrap()
            } else {
                harness
                    .store()
                    .credential_entitlement_fences()
                    .unwrap()
                    .into_iter()
                    .find(|f| f.provider_id == provider && f.credential_id == credential)
                    .unwrap()
                    .observed_at_ms
                    + SIX_HOURS
            }
        };
        seed(start - 60_000);
        let params = json!({"event_id":"extend", "kind":"credential_refused", "credential_id":credential,
            "observed_at_ms":start - 120_000, "provider_code":"billing_exhausted"});
        observe(&runner, params.clone()).await;
        assert!(deadline() >= start + SIX_HOURS);
        // A previously received wall may already reach further than this
        // delayed observation. Neither block mechanism may move it backwards.
        seed(start + 60_000);
        let longer = deadline();
        let mut delayed = params;
        delayed["event_id"] = json!("delayed");
        observe(&runner, delayed).await;
        assert_eq!(deadline(), longer);

        let operator = harness.bind(reserved("core"), None).await;
        let cleared = operator
            .call(
                "route.clear_cooldown",
                &json!({"routeKey":format!("{provider}/{model}")}),
            )
            .await;
        assert_eq!(cleared.0, FrameType::Response, "{cleared:?}");
        let after = runner
            .call(
                "route.account_for",
                &admit("cleared", provider, model, credential),
            )
            .await;
        assert_eq!(after.0, FrameType::Response, "{after:?}");
    }
}
