use super::*;

const OPENAI_KEY: &str = "opaque-openai";
const OTHER_PROVIDER: &str = "z-media-vendor";

fn provider_engine(provider: &str) -> Value {
    let mut engine = engine(&format!("{provider}-transcriber"), false);
    engine["provider"] = json!(provider);
    engine
}

fn provider_key(id: &str, provider: &str) -> ListedCredential {
    let mut key = key(id);
    key.provider_ids = vec![provider.into()];
    key
}

fn providers(sibling: bool, fallback: bool) -> Arc<Harness> {
    let mut engines = vec![provider_engine("openai")];
    let mut keys = vec![provider_key(OPENAI_KEY, "openai")];
    if sibling {
        keys.push(provider_key("opaque-openai-sibling", "openai"));
    }
    if fallback {
        engines.push(provider_engine(OTHER_PROVIDER));
        keys.push(provider_key("opaque-fallback", OTHER_PROVIDER));
    }
    cortexkit_role_routing::parse_engine_declarations(&json!({"engines":engines})).unwrap();
    let harness = setup(engines);
    harness.set_credentials(media_credentials(keys));
    harness
}

async fn admit(route: &Route, decision: &Value, id: &str) -> Value {
    let params = json!({"admission_id":id,"conversation_key":id,
        "provider":decision["engine"]["provider"],"model":decision["engine"]["model"],
        "decision_id":decision["decision_id"],"hold_wait_max_s":0});
    let (kind, value) = route.call("route.account_for", &params).await;
    assert_eq!(kind, FrameType::Response, "{value}");
    value["result"].clone()
}

async fn observe(route: &Route, id: &str, kind: &str, extra: Value) -> Value {
    let mut params = json!({"event_id":format!("{id}/{kind}"),"admission_id":id,"kind":kind});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (kind, value) = route.call("route.account_observe", &params).await;
    assert_eq!(kind, FrameType::Response, "{value}");
    value["result"].clone()
}

#[tokio::test]
async fn finished_unsent_admission_removes_demand_but_started_admission_keeps_it() {
    for started in [false, true] {
        let harness = providers(false, false);
        let route = harness.bind(reserved("colliculus"), None).await;
        let decision = selected(&route, &request()).await;
        let params = json!({"admission_id":"demand", "conversation_key":"demand",
            "provider":decision["engine"]["provider"], "model":decision["engine"]["model"],
            "decision_id":decision["decision_id"], "hold_wait_max_s":0,
            "initial_input_bytes":32768, "initial_input_estimated":true});
        let (kind, reply) = route.call("route.account_for", &params).await;
        assert_eq!(kind, FrameType::Response, "{reply}");
        let account = reply["result"]["account"]["account_id"].as_str().unwrap();
        let demand = || {
            crate::account_state::snapshot(harness.store())
                .unwrap()
                .admissions
                .demand("openai", account, None)
        };
        assert_eq!(
            demand().iter().map(|row| row.request_bytes).sum::<u64>(),
            32768
        );
        if started {
            observe(
                &route,
                "demand",
                "admission_started",
                json!({"run_id":"run"}),
            )
            .await;
        }
        let finished = observe(&route, "demand", "run_finished", json!({})).await;
        assert!(effects(&finished)
            .iter()
            .any(|effect| effect["effect"] == "admission_finished"));
        assert_eq!(
            demand().iter().map(|row| row.request_bytes).sum::<u64>(),
            if started { 32768 } else { 0 }
        );
        let persisted = harness
            .store()
            .account_read(|tx| tx.demand("openai", account, None))
            .unwrap();
        assert_eq!(demand(), persisted);
        crate::account_state::assert_published_equals_fresh_load(
            harness.store(),
            "after run_finished",
        );
    }
}

fn effects(reply: &Value) -> Vec<Value> {
    reply["observation"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|effect| serde_json::from_str(effect.as_str().unwrap()).unwrap())
        .collect()
}

async fn refused(route: &Route, id: &str) {
    observe(
        route,
        id,
        "credential_refused",
        json!({"credential_id":OPENAI_KEY,
        "provider_code":"insufficient_quota"}),
    )
    .await;
}

fn age_fence(harness: &Harness) {
    assert_eq!(
        harness
            .store()
            .touch_credential_entitlement_fence(
                "openai",
                OPENAI_KEY,
                crate::now_ms() as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1,
            )
            .unwrap(),
        1
    );
}

fn probe_note(harness: &Harness, admission: &str) -> Value {
    let decision = harness
        .store()
        .account_decisions(striatum_store::DECISION_MODE_SERVE, 100)
        .unwrap()
        .into_iter()
        .find(|row| row.admission_id.as_deref() == Some(admission))
        .unwrap();
    let decision: Value = serde_json::from_str(&decision.decision_json).unwrap();
    decision["explanation"]["credential_fence_probe"].clone()
}

#[tokio::test]
async fn engine_media_refusal_sequence_falls_back_but_keeps_healthy_siblings_and_stale_probes() {
    for (sibling, fallback) in [(false, true), (false, false), (true, true)] {
        let harness = providers(sibling, fallback);
        let route = harness.bind(reserved("colliculus"), None).await;
        let decision = selected(&route, &request()).await;
        assert_eq!(decision["engine"]["provider"], "openai");
        let first = admit(&route, &decision, "first").await;
        let credential = first["account"]["credential_id"].as_str().unwrap();
        observe(
            &route,
            "first",
            "credential_refused",
            json!({"credential_id":credential,
            "provider_code":"insufficient_quota"}),
        )
        .await;
        observe(&route, "first", "run_finished", json!({})).await;
        let fences = harness.store().credential_entitlement_fences().unwrap();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].code, "credential_refused");
        let (kind, next) = route.call("route.select_engine", &request()).await;
        if sibling || fallback {
            assert_eq!(kind, FrameType::Response, "{next}");
            assert_eq!(
                next["result"]["engine"]["provider"],
                if sibling { "openai" } else { OTHER_PROVIDER },
                "{next}"
            );
            let next_account = admit(&route, &next["result"], "next").await;
            assert_ne!(next_account["account"]["credential_id"], credential);
        } else {
            assert_eq!(kind, FrameType::Error, "{next}");
            assert_eq!(next["code"], "no_engine", "{next}");
            assert_eq!(next["detail"]["filter"], "reachability", "{next}");
        }
        harness
            .store()
            .touch_credential_entitlement_fence(
                "openai",
                credential,
                crate::now_ms() as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1,
            )
            .unwrap();
        assert_eq!(
            selected(&route, &request()).await["engine"]["provider"],
            "openai"
        );
    }
}

#[tokio::test]
async fn engine_media_successful_probe_lifts_fence_and_next_admission_is_not_a_probe() {
    let harness = providers(false, false);
    let route = harness.bind(reserved("colliculus"), None).await;
    let first = selected(&route, &request()).await;
    admit(&route, &first, "first").await;
    refused(&route, "first").await;
    observe(&route, "first", "run_finished", json!({})).await;
    age_fence(&harness);
    let decision = selected(&route, &request()).await;
    let probe = admit(&route, &decision, "probe").await;
    assert!(probe_note(&harness, "probe").is_string());
    observe(
        &route,
        "probe",
        "admission_started",
        json!({"run_id":"probe-run"}),
    )
    .await;
    let finished = observe(
        &route,
        "probe",
        "run_finished",
        json!({
            "served_account_id":probe["account"]["account_id"],
            "credential_id":probe["account"]["credential_id"]
        }),
    )
    .await;
    assert!(
        effects(&finished)
            .iter()
            .any(|effect| effect["effect"] == "credential_fence_lifted"
                && effect["provider"] == "openai"
                && effect["credential_id"] == OPENAI_KEY),
        "{finished}"
    );
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
    let decision = selected(&route, &request()).await;
    let next = admit(&route, &decision, "normal").await;
    assert_eq!(next["account"]["credential_id"], OPENAI_KEY);
    assert!(probe_note(&harness, "normal").is_null());
}

#[tokio::test]
async fn engine_media_finish_requires_exact_serving_identity_before_healing() {
    for shape in [
        "other-account",
        "other-credential",
        "missing-account",
        "missing-credential",
    ] {
        let harness = providers(false, false);
        let route = harness.bind(reserved("colliculus"), None).await;
        let first = selected(&route, &request()).await;
        admit(&route, &first, "first").await;
        refused(&route, "first").await;
        observe(&route, "first", "run_finished", json!({})).await;
        age_fence(&harness);
        let decision = selected(&route, &request()).await;
        let probe = admit(&route, &decision, "probe").await;
        assert!(probe_note(&harness, "probe").is_string());
        observe(
            &route,
            "probe",
            "admission_started",
            json!({"run_id":"probe-run"}),
        )
        .await;
        let older = harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms;
        let mut serving = json!({"run_id":"probe-run",
            "served_account_id":probe["account"]["account_id"],
            "credential_id":probe["account"]["credential_id"]});
        match shape {
            "other-account" => serving["served_account_id"] = json!("served-B"),
            "other-credential" => serving["credential_id"] = json!("credential-B"),
            "missing-account" => {
                serving.as_object_mut().unwrap().remove("served_account_id");
            }
            _ => {
                serving.as_object_mut().unwrap().remove("credential_id");
            }
        }
        let finished = observe(&route, "probe", "run_finished", serving).await;
        let fences = harness.store().credential_entitlement_fences().unwrap();
        assert_eq!(
            fences.len(),
            1,
            "{shape}: finishing another or unidentified login cannot heal A"
        );
        assert_eq!(fences[0].observed_at_ms, older);
        assert!(!effects(&finished)
            .iter()
            .any(|effect| effect["effect"] == "credential_fence_lifted"));
        harness
            .store()
            .account_read(|tx| {
                let admission = tx.admission("reserved:colliculus", "probe")?.unwrap();
                assert_eq!(
                    admission.state, "finished",
                    "{shape}: lifecycle still finishes"
                );
                assert!(admission.finished_at.is_some());
                assert!(
                    !tx.fence_rechecks()?.is_empty(),
                    "{shape}: the probe evidence must remain"
                );
                Ok(())
            })
            .unwrap();
    }
}

#[tokio::test]
async fn engine_media_failed_probe_keeps_fence_on_run_finished() {
    for failure in [
        "credential_refused",
        "transient_backoff",
        "account_limit_exhausted",
    ] {
        let _clock = crate::InjectedClock::set(1_800_000_000_000);
        let harness = providers(false, false);
        let route = harness.bind(reserved("colliculus"), None).await;
        let decision = selected(&route, &request()).await;
        admit(&route, &decision, "first").await;
        refused(&route, "first").await;
        observe(&route, "first", "run_finished", json!({})).await;
        age_fence(&harness);
        let decision = selected(&route, &request()).await;
        let probe = admit(&route, &decision, "probe").await;
        assert!(probe_note(&harness, "probe").is_string());
        observe(
            &route,
            "probe",
            "admission_started",
            json!({"run_id":"probe-run"}),
        )
        .await;
        let claimed_at = harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms;
        let _later = crate::InjectedClock::set(crate::now_ms() + 1000);
        let extra = match failure {
            "credential_refused" => {
                json!({"credential_id":OPENAI_KEY,"provider_code":"insufficient_quota"})
            }
            "transient_backoff" => json!({"retry_at_ms":crate::now_ms() + 60_000}),
            _ => json!({}),
        };
        observe(&route, "probe", failure, extra).await;
        let finished = observe(
            &route,
            "probe",
            "run_finished",
            json!({
                "served_account_id":probe["account"]["account_id"],
                "credential_id":probe["account"]["credential_id"]
            }),
        )
        .await;
        assert!(
            !effects(&finished)
                .iter()
                .any(|effect| effect["effect"] == "credential_fence_lifted"),
            "{failure}: {finished}"
        );
        let fences = harness.store().credential_entitlement_fences().unwrap();
        assert_eq!(fences.len(), 1, "{failure}");
        if failure == "credential_refused" {
            assert_eq!(fences[0].observed_at_ms, crate::now_ms() as i64);
            assert!(fences[0].observed_at_ms > claimed_at);
        }
        let (kind, value) = route.call("route.select_engine", &request()).await;
        assert_eq!(kind, FrameType::Error, "{failure}: {value}");
        assert_eq!(value["code"], "no_engine", "{value}");
    }
}

#[tokio::test]
async fn engine_media_selection_checks_pool_walls_and_account_route_fences() {
    for route_fence in [false, true] {
        let harness = providers(false, true);
        let now = crate::now_ms() as i64;
        if route_fence {
            harness
                .store()
                .record_account_route_fence(&striatum_store::AccountRouteFence {
                    provider: "openai".into(),
                    credential_id: OPENAI_KEY.into(),
                    model: "openai-transcriber".into(),
                    until_ms: now + 60_000,
                    decision_id: String::new(),
                    recorded_at: now,
                })
                .unwrap();
        } else {
            harness
                .store()
                .pool_wall(
                    "openai",
                    OPENAI_KEY,
                    striatum_store::PoolWallKind::Billing,
                    now + 60_000,
                    "insufficient_quota",
                    now,
                )
                .unwrap();
        }
        let route = harness.bind(reserved("colliculus"), None).await;
        assert_eq!(
            selected(&route, &request()).await["engine"]["provider"],
            OTHER_PROVIDER
        );
    }
}
