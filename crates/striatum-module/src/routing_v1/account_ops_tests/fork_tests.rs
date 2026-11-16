use super::*;
use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

const PARENT: &str = "oauth:anthropic:bob";
const MODEL: &str = "claude-opus-5-5";
const MOVED: &str = "oauth:anthropic:alice";

fn fork_request(id: &str, parent: &str) -> Value {
    json!({"provider":"anthropic", "model":MODEL, "conversation_key":id,
        "admission_id":id, "fork_of":{"admission_id":parent}, "head_turn":true})
}

fn parent_reads(
    harness: &Harness,
    principal: &'static str,
    id: &'static str,
    expected_account: &'static str,
    expected_reads: u64,
) {
    crate::account_writer::execute(
        harness.store(),
        striatum_store::AccountCommandKind::Repair,
        move |tx| {
            let before = striatum_store::keyed_exception_reads();
            let parent = tx.fork_parent(principal, id, "anthropic")?.unwrap();
            assert_eq!(parent.account_id, expected_account);
            assert_eq!(
                striatum_store::keyed_exception_reads() - before,
                expected_reads
            );
            Ok(())
        },
    )
    .unwrap();
}

fn fixture() -> (Arc<Harness>, crate::broca_credentials::BrocaCredentials) {
    let (harness, mut credentials) = setup("anthropic", false);
    let mut fact = super::super::account_classes_tests::account_fact("anthropic");
    fact.model_id = MODEL.into();
    harness.install_facts(&[fact.clone()]);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "phone-call".into(),
            credentials: [
                PARENT,
                "oauth:anthropic:alice",
                "oauth:anthropic:carol",
            ]
            .into_iter()
            .map(|id| ListedCredential {
                id: id.into(),
                account_id: Some(id.into()),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["anthropic".into()],
                serves: vec!["anthropic".into()],
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
    fact.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
    fact.billing_plane.as_mut().unwrap().kind = "subscription".into();
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact]),
    );
    harness.set_credentials(credentials.clone());
    healthy_parent_reading(NOW);
    (harness, credentials)
}

async fn parent(runner: &Route) -> Value {
    parent_with_id(runner, "parent").await
}

async fn parent_with_id(runner: &Route, id: &str) -> Value {
    let mut body = fork_request(id, "unused");
    body.as_object_mut().unwrap().remove("fork_of");
    body["initial_input_bytes"] = json!(1_800_000);
    body["candidates"] = json!({"view_digest":"parent-only","accounts":[{"account_id":PARENT,"credential_id":PARENT}]});
    let reply = runner.call("route.account_for", &body).await;
    let answer = response(&reply).clone();
    assert_eq!(answer["account"]["account_id"], PARENT);
    response(&runner.call("route.account_observe", &json!({"admission_id":id, "event_id":format!("{id}-start"), "kind":"admission_started", "run_id":"head-run"})).await);
    answer
}

#[tokio::test]
async fn fork_of_finished_broca_head_uses_its_exact_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = fixture();
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca","via_vault":true}]));
    let broca = harness.bind(reserved("broca"), None).await;
    let id = "run-sid-8a9c0f21";
    parent_with_id(&broca, id).await;
    response(
        &broca
            .call(
                "route.account_observe",
                &json!({"admission_id":id,"event_id":"head-finished","kind":"run_finished"}),
            )
            .await,
    );
    let reply = broca
        .call("route.account_for", &fork_request("fork:81f0a32e", id))
        .await;
    assert_eq!(response(&reply)["account"]["account_id"], PARENT);
    assert_eq!(response(&reply)["account"]["credential_id"], PARENT);
    crate::account_state::assert_published_equals_fresh_load(
        harness.store(),
        "finished Broca head fork",
    );
}

fn healthy_parent_reading(at: u64) {
    let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
    crate::account_quota::apply_provider_reply(&mut quota, "claude", &serde_json::to_vec(&json!({
        "result":([PARENT, MOVED, "oauth:anthropic:carol"].into_iter().map(|account| json!({"provider":"claude", "apiProvider":"anthropic", "account":account, "source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(at as i64),
            "usage":{"primary":{"usedPercent":10.0,"windowMinutes":300,
                "resetsAt":crate::account_selection::rfc3339(at as i64 + 3_600_000)},
                "secondary":{"usedPercent":10.0,"windowMinutes":10080,"resetsAt":crate::account_selection::rfc3339(at as i64 + 7*86_400_000)}}})).collect::<Vec<_>>()),
        "completeProviders":["claude"]
    })).unwrap(), at as i64).unwrap();
}

#[tokio::test]
async fn fork_of_uses_only_a_consumed_move_for_open_and_closed_broca_parents() {
    let _clock = crate::InjectedClock::set(NOW);
    for state in ["used", "unused", "expired"] {
        let (harness, mut credentials) = fixture();
        harness
            .edit_config(|config| config["runners"] = json!([{"module":"broca","via_vault":true}]));
        // Disable the third credential so alice is the only alternate
        // account. The expected move does not depend on the score tie breaker.
        let mut rows = credentials.listed_credentials().to_vec();
        rows.iter_mut()
            .filter(|row| row.id != PARENT && row.id != MOVED)
            .for_each(|row| row.state = "needs_reauth".into());
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "move-roster".into(),
                credentials: rows,
                ..Default::default()
            },
            NOW,
        );
        harness.set_credentials(credentials);
        let owner = harness.bind(reserved("class-owner"), None).await;
        let broca = harness.bind(reserved("broca"), None).await;
        let selection = owner.call("route.select", &json!({"consumer":"mason","pin":format!("anthropic/{MODEL}"),"requirements":{"iq":0},"task_ref":"phone-head"})).await;
        let selection = response(&selection).clone();
        let id = "run-sid-moved-head";
        let mut body = fork_request(id, "unused");
        body.as_object_mut().unwrap().remove("fork_of");
        body["decision_id"] = selection["decision_id"].clone();
        body["warm_credential"] = json!(PARENT);
        assert_eq!(
            response(&broca.call("route.account_for", &body).await)["account"]["account_id"],
            PARENT
        );
        response(&broca.call("route.account_observe", &json!({"admission_id":id,"event_id":"head-start","kind":"admission_started","run_id":"head-run"})).await);
        let offer = broca.call("route.account_for", &json!({"mode":"continue","run_id":"head-run","admission_id":id,
            "conversation_key":id,"previous_decision_id":selection["decision_id"],"selection_id":"move",
            "exhausted":{"credential_id":PARENT,"account":PARENT,"limit_window":"five_hour","observed_at_ms":NOW,"resets_at_ms":NOW+300_000}
        })).await;
        let offer = response(&offer).clone();
        assert_eq!(offer["kind"], "offer", "{offer}");
        assert_eq!(offer["account"], MOVED);
        if state == "used" {
            response(&broca.call("route.account_observe", &json!({"admission_id":id,"event_id":"move-start","kind":"admission_started","run_id":"head-run",
                "served_account_id":offer["account"],"credential_id":offer["credential_id"]})).await);
        } else if state == "expired" {
            let until = offer["valid_until_ms"].as_i64().unwrap() + 1;
            crate::account_writer::execute(
                harness.store(),
                striatum_store::AccountCommandKind::Repair,
                move |tx| tx.release_expired_continuations(until),
            )
            .unwrap();
        }
        // Recovery makes the original account eligible again. An unused offer
        // must not redirect the fork, and a used offer must still redirect it.
        let _later = crate::InjectedClock::set(NOW + 1_000);
        healthy_parent_reading(NOW + 1_000);
        let expected = if state == "used" { MOVED } else { PARENT };
        parent_reads(&harness, "reserved:broca", id, expected, 0);
        let reads = striatum_store::keyed_exception_reads();
        let reply = broca
            .call("route.account_for", &fork_request("fork:live-move", id))
            .await;
        assert_eq!(
            response(&reply)["account"]["account_id"],
            expected,
            "{state}"
        );
        assert_eq!(
            striatum_store::keyed_exception_reads() - reads,
            1,
            "only the child's absent id is read: {state}"
        );
        crate::account_state::assert_published_equals_fresh_load(harness.store(), state);
        response(
            &broca
                .call(
                    "route.account_observe",
                    &json!({"admission_id":id,"event_id":"head-finish","kind":"run_finished"}),
                )
                .await,
        );
        parent_reads(
            &harness,
            "reserved:broca",
            id,
            expected,
            if state == "used" { 2 } else { 1 },
        );
        let reads = striatum_store::keyed_exception_reads();
        let reply = broca
            .call("route.account_for", &fork_request("fork:closed-move", id))
            .await;
        assert_eq!(
            response(&reply)["account"]["credential_id"],
            expected,
            "{state}"
        );
        assert_eq!(
            striatum_store::keyed_exception_reads() - reads,
            if state == "used" { 3 } else { 2 },
            "{state}"
        );
        crate::account_state::assert_published_equals_fresh_load(
            harness.store(),
            "closed move fork",
        );
    }
}

#[tokio::test]
async fn fork_of_admits_exact_parent_despite_load_and_better_siblings() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = fixture();
    harness.edit_config(|config| {
        config["concurrency"] =
            json!({"enforcement_enabled":true,"providers":{"anthropic":{"limit":1}}})
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    parent(&runner).await;
    let mut ordinary = fork_request("ordinary", "parent");
    ordinary.as_object_mut().unwrap().remove("fork_of");
    let reply = runner.call("route.account_for", &ordinary).await;
    assert_ne!(response(&reply)["account"]["account_id"], PARENT);
    let body = fork_request("fork", "parent");
    let first = runner.call("route.account_for", &body).await;
    assert_eq!(response(&first)["account"]["account_id"], PARENT);
    assert_eq!(response(&first)["account"]["credential_id"], PARENT);
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    assert_eq!(
        state
            .inflight
            .count_for_model(&Default::default(), "anthropic", PARENT, MODEL, NOW as i64),
        2
    );
    let decision: Value = serde_json::from_str(
        &harness.store().account_decisions("serve", 1).unwrap()[0].decision_json,
    )
    .unwrap();
    assert_eq!(
        decision["explanation"]["fork_of"],
        json!({"kind":"honoured","admission_id":"parent"})
    );
    assert_eq!(runner.call("route.account_for", &body).await, first);
    let mut changed = body;
    changed["fork_of"]["admission_id"] = json!("ordinary");
    assert_eq!(
        runner.call("route.account_for", &changed).await.1["code"],
        "admission_conflict"
    );
    crate::account_state::assert_published_equals_fresh_load(harness.store(), "fork admitted");
}

#[tokio::test]
async fn fork_of_finished_parent_uses_one_keyed_read_and_keeps_affinity() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = fixture();
    let runner = harness.bind(reserved("class-runner"), None).await;
    parent(&runner).await;
    for (finished, expected_reads, id) in [(false, 1, "live-fork"), (true, 2, "finished-fork")] {
        if finished {
            response(&runner.call("route.account_observe", &json!({"event_id":"finish", "kind":"run_finished", "admission_id":"parent"})).await);
        }
        parent_reads(
            &harness,
            "reserved:class-runner",
            "parent",
            PARENT,
            expected_reads - 1,
        );
        let reads = striatum_store::keyed_exception_reads();
        let reply = runner
            .call("route.account_for", &fork_request(id, "parent"))
            .await;
        assert_eq!(response(&reply)["account"]["credential_id"], PARENT);
        // Every fresh child checks its own absent id once. The live parent
        // adds zero reads; the closed parent adds exactly one keyed read.
        assert_eq!(
            striatum_store::keyed_exception_reads() - reads,
            expected_reads
        );
        crate::account_state::assert_published_equals_fresh_load(harness.store(), id);
    }
    let reply = runner.call("route.account_for", &json!({"provider":"anthropic", "model":MODEL, "conversation_key":"finished-fork", "admission_id":"follow-up"})).await;
    assert_eq!(response(&reply)["account"]["account_id"], PARENT);
}

#[tokio::test]
async fn fork_of_refuses_parent_block_and_needs_sign_in_without_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    for needs_sign_in in [false, true] {
        let (harness, mut credentials) = fixture();
        let runner = harness.bind(reserved("class-runner"), None).await;
        parent(&runner).await;
        if needs_sign_in {
            let mut rows = credentials.listed_credentials().to_vec();
            rows.iter_mut().find(|row| row.id == PARENT).unwrap().state = "needs_reauth".into();
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "signed-out".into(),
                    credentials: rows,
                    ..Default::default()
                },
                NOW,
            );
            harness.set_credentials(credentials);
        } else {
            response(&runner.call("route.account_observe", &json!({
                "event_id":"blocked", "kind":"account_limit_exhausted", "admission_id":"parent",
                "limit_window":"five_hour", "observed_at_ms":NOW, "resets_at_ms":NOW+3_600_000, "resets_at_source":"header"
            })).await);
        }
        crate::account_state::assert_published_equals_fresh_load(
            harness.store(),
            "parent unavailable",
        );
        let reply = runner
            .call("route.account_for", &fork_request("refused-fork", "parent"))
            .await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "account_hold", "{}", reply.1);
        assert_eq!(
            reply.1["detail"]["reason"],
            "fork_parent_account_unavailable"
        );
        assert_eq!(reply.1["detail"]["until_ms"], NOW + 900_000);
        let decision = harness
            .store()
            .account_decisions("serve", 1)
            .unwrap()
            .remove(0);
        let decision: Value = serde_json::from_str(&decision.decision_json).unwrap();
        assert_eq!(decision["explanation"]["fork_of"]["kind"], "refused");
        assert_eq!(decision["explanation"]["fork_of"]["admission_id"], "parent");
        assert!(harness
            .store()
            .v1_admission("reserved:class-runner", "refused-fork")
            .unwrap()
            .is_none());
        crate::account_state::assert_published_equals_fresh_load(harness.store(), "fork refused");
    }
}

#[tokio::test]
async fn fork_of_rejects_other_modes_and_conflicting_hints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = fixture();
    let runner = harness.bind(reserved("class-runner"), None).await;
    for mode in ["renew", "reassign", "continue"] {
        let mut body = fork_request("fork", "parent");
        body["mode"] = json!(mode);
        let reply = runner.call("route.account_for", &body).await;
        assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
        assert_eq!(reply.1["detail"]["field"], "fork_of");
    }
    for (field, value) in [
        ("requested_account", json!({"account_id":PARENT})),
        ("warm_credential", json!(PARENT)),
        ("parent_conversation_key", json!("parent")),
        ("candidates", json!({"view_digest":"none","accounts":[]})),
    ] {
        let mut body = fork_request("fork", "parent");
        body[field] = value;
        let reply = runner.call("route.account_for", &body).await;
        assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
        assert_eq!(reply.1["detail"]["field"], "fork_of", "{}", reply.1);
    }
}

#[tokio::test]
async fn fork_of_all_bad_parents_have_identical_answer_bytes() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut expected = None;
    for case in [
        "unknown",
        "refused",
        "other-principal",
        "other-provider",
        "expired",
        "past-retention",
    ] {
        let (harness, _) = fixture();
        harness.edit_config(|config| config["runners"] = json!([
            {"module":"class-runner","via_vault":true}, {"module":"other-runner","via_vault":true}
        ]));
        let runner = harness.bind(reserved("class-runner"), None).await;
        let parent_id = match case {
            "unknown" => "missing",
            "refused" => {
                let reply = runner
                    .call(
                        "route.account_for",
                        &fork_request("refused-parent", "missing"),
                    )
                    .await;
                assert_eq!(reply.1["code"], "invalid_request");
                "refused-parent"
            }
            "other-principal" => {
                let other = harness.bind(reserved("other-runner"), None).await;
                parent(&other).await;
                "parent"
            }
            _ => {
                parent(&runner).await;
                match case {
                    "other-provider" => {
                        crate::account_writer::execute(
                            harness.store(),
                            striatum_store::AccountCommandKind::Repair,
                            |tx| {
                                let mut row =
                                    tx.admission("reserved:class-runner", "parent")?.unwrap();
                                row.admission_id = "wrong-provider".into();
                                row.provider = "synthetic".into();
                                row.pin_id = "wrong-provider-pin".into();
                                tx.insert_pin(&striatum_store::AccountPinRow {
                                    pin_id: row.pin_id.clone(),
                                    principal: row.principal.clone(),
                                    conversation_hash: row.conversation_hash.clone(),
                                    provider: row.provider.clone(),
                                    pin_generation: 1,
                                    affinity_model: MODEL.into(),
                                    account_id: PARENT.into(),
                                    credential_id: PARENT.into(),
                                    pin_mode: "run_frozen".into(),
                                    previous_pin_id: None,
                                    decision_id: row.decision_id.clone(),
                                    created_at: NOW as i64,
                                })?;
                                tx.insert_admission(&row)
                            },
                        )
                        .unwrap();
                        "wrong-provider"
                    }
                    "expired" => {
                        let mut body = fork_request("expired-parent", "unused");
                        body.as_object_mut().unwrap().remove("fork_of");
                        response(&runner.call("route.account_for", &body).await);
                        crate::account_writer::execute(
                            harness.store(),
                            striatum_store::AccountCommandKind::Repair,
                            |tx| {
                                tx.mark_admission_expired("reserved:class-runner", "expired-parent")
                            },
                        )
                        .unwrap();
                        "expired-parent"
                    }
                    "past-retention" => {
                        response(&runner.call("route.account_observe", &json!({"event_id":"finished", "kind":"run_finished", "admission_id":"parent"})).await);
                        assert_eq!(
                            crate::account_writer::prune_account_admissions(
                                harness.store(),
                                NOW as i64 + 86_400_000,
                                256,
                                NOW as i64 + 40 * 86_400_000
                            )
                            .unwrap(),
                            1
                        );
                        "parent"
                    }
                    _ => unreachable!(),
                }
            }
        };
        crate::account_state::assert_published_equals_fresh_load(harness.store(), case);
        let reply = runner
            .call_raw("route.account_for", &fork_request("bad-fork", parent_id))
            .await;
        assert_eq!(reply.0, FrameType::Error);
        let body: Value = serde_json::from_slice(&reply.1).unwrap();
        assert_eq!(body["code"], "invalid_request", "{case}: {body}");
        assert_eq!(body["detail"]["field"], "fork_of");
        if let Some(expected) = &expected {
            assert_eq!(&reply, expected, "{case}");
        } else {
            expected = Some(reply);
        }
        crate::account_state::assert_published_equals_fresh_load(
            harness.store(),
            "bad fork refused",
        );
    }
}
