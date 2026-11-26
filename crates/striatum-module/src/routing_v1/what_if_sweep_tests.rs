use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use striatum_core::accounts::{ConfirmationSource, Fence, FenceScope, GeneralWindow, ResetTime};

const NOW: i64 = 1_800_000_000_000;

fn normalize(mut answer: Value) -> Value {
    answer.as_object_mut().unwrap().remove("snapshot_ms");
    answer.as_object_mut().unwrap().remove("elapsed_ms");
    answer
}

fn quota(fresh: bool) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    let rows: Vec<_> = ["account-1", "account-2"].into_iter().map(|account| json!({
        "provider":"fixture", "apiProvider":"example-subscription", "account":account, "source":"vault",
        "fetchedAt":crate::account_selection::rfc3339(NOW),
        "usage":{"primary":{"usedPercent":if fresh {10} else {100},"windowMinutes":300,
            "resetsAt":crate::account_selection::rfc3339(NOW+3_600_000)}}
    })).collect();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "fixture",
        &serde_json::to_vec(&json!({"result":rows,"completeProviders":["fixture"]})).unwrap(),
        NOW,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

async fn fixture_with_repairs() -> (Arc<Harness>, Route) {
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    super::super::conformance_tests::install_panel_families(&harness, &fixture::catalog());
    harness.edit_config(|config| {
        config["panels"]["audit"] =
            json!({"top_k":2,"by":"iq","extra":0,"extra_by":"random","floor":0})
    });
    quota(true);
    let fence = Fence {
        account_id: "account-1".into(),
        pool: Some(FenceScope::General(GeneralWindow::FiveHour).into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW - 100,
        reset: ResetTime::At(NOW + 3_600_000),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&striatum_store::AccountFenceRow {
                fence_id: "fresh-headroom-fixture".into(),
                provider: "example-subscription".into(),
                account_id: "account-1".into(),
                scope_json: serde_json::to_string(&fence.pool).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at: NOW - 100,
                principal: "fixture".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW - 100,
                cleared_at: None,
                cleared_reason: None,
            })?;
            tx.record_continuation(
                "fixture",
                "expired-offer",
                &striatum_store::V1Recorded {
                    body_json: "{}".into(),
                    answer_json: "{}".into(),
                },
                "run",
                "admission",
                "previous",
                Some(("decision", "reservation", NOW - 1)),
            )
        })
        .unwrap();
    let credential = harness
        .credentials()
        .lock()
        .unwrap()
        .listed_credentials()
        .iter()
        .find(|row| row.account_id.as_deref() == Some("account-1"))
        .unwrap()
        .id
        .clone();
    seed_open_offer(
        &harness,
        "example-subscription",
        "model-d",
        "account-1",
        &credential,
    );
    rusqlite::Connection::open(harness.store_path().with_file_name("history.db")).unwrap().execute_batch(
        "INSERT INTO account_size_refusal VALUES ('example-provider','old','model-a',17,1,0);
         INSERT INTO credential_entitlement_fences VALUES ('example-provider','removed','auth_required','old-listing',0);"
    ).unwrap();
    // This billing refusal has fresh headroom at receipt, so its first
    // classification is non-quota and must survive a contradictory reading.
    let (provider, credential, fingerprint) = {
        let credentials = harness.credentials();
        let credentials = credentials.lock().unwrap();
        let row = credentials
            .listed_credentials()
            .iter()
            .find(|row| row.account_id.as_deref() == Some("account-2"))
            .unwrap();
        let fingerprint = match credentials.listing_fingerprint(&row.id) {
            crate::broca_credentials::ListingLookup::Listed(fp) => fp,
            _ => panic!("listed fixture"),
        };
        ("example-subscription", row.id.clone(), fingerprint)
    };
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: provider.into(),
                credential_id: credential,
                code: "billing_exhausted".into(),
            },
            Some(&fingerprint),
            NOW,
        )
        .unwrap();
    harness.store().reload_account_state_for_test().unwrap();
    let operator = operator(&harness).await;
    (harness, operator)
}

fn request() -> Value {
    json!({"iqs":[0,5,50,100], "runner":"broca", "head_choices":{"runner_module":"broca"},
        "runner_declarations":{"broca":broad_runner_slot()}, "panel":{"class":"audit","seed":"sweep"}})
}

fn golden(name: &str, answer: &Value) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    if std::env::var_os("STRIATUM_RECORD_SWEEP").is_some() {
        std::fs::write(path, serde_json::to_string_pretty(answer).unwrap() + "\n").unwrap();
    } else {
        let expected: Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(*answer, expected);
    }
}

#[tokio::test]
async fn s8c_sweep_matches_sql_base_and_reads_one_state_without_commands() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (harness, operator) = fixture_with_repairs().await;
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let before = rows(&harness);
    let trace = harness.store().trace_account_reads_for_test().unwrap();
    trace.take();
    let commands = harness.store().account_writer_batch_sizes_for_test();
    let snapshots = crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed);
    let answer = normalize(evaluate(&operator, request()).await);
    let reads = trace.take();
    if std::env::var_os("STRIATUM_SWEEP_SQL_ORACLE").is_none() {
        assert_eq!(
            crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed) - snapshots,
            1
        );
        assert!(reads.reader.is_empty(), "{:?}", reads.reader);
        assert!(reads.writer.is_empty(), "{:?}", reads.writer);
    }
    assert_eq!(
        commands,
        harness.store().account_writer_batch_sizes_for_test()
    );
    assert_eq!(before, rows(&harness));
    assert_eq!(
        *state,
        *crate::account_state::snapshot(harness.store()).unwrap()
    );
    assert!(
        answer["panels"][0]["seats"].as_array().unwrap().len() >= 2,
        "{answer}"
    );
    golden("s8c-sweep-sql-base.json", &answer);
}

#[tokio::test]
async fn s8c_quota_replacement_after_classification_and_heal_is_sweep_local() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    for panel in [false, true] {
        let (harness, operator) = fixture_with_repairs().await;
        let initial = crate::account_state::snapshot(harness.store()).unwrap();
        let before = rows(&harness);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let checks = Arc::new(AtomicUsize::new(0));
        let checked = checks.clone();
        let _hook = crate::account_selection::Sweep::hook(
            &harness.config_path_for_test(),
            move |sweep, is_panel, point| {
                if is_panel != panel {
                    return;
                }
                let blocks = sweep.published();
                assert!(blocks
                    .active_fences("example-subscription")
                    .unwrap()
                    .iter()
                    .all(|row| row.fence_id != "fresh-headroom-fixture"));
                assert!(blocks
                    .fence_rechecks()
                    .unwrap()
                    .iter()
                    .any(|origin| origin.kind == "credential"
                        && origin.provider == "example-subscription"
                        && origin.classified
                        && !origin.quota));
                checked.fetch_add(1, Ordering::Relaxed);
                if point == 1 {
                    seen.fetch_add(1, Ordering::Relaxed);
                    quota(false);
                }
            },
        );
        let mut params = request();
        if panel {
            params.as_object_mut().unwrap().remove("iqs");
        }
        let answer = normalize(if panel {
            dry_panels(&operator, params["panel"].clone(), params).await
        } else {
            evaluate(&operator, params).await
        });
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(checks.load(Ordering::Relaxed) >= 2);
        assert_eq!(before, rows(&harness));
        assert_eq!(
            *initial,
            *crate::account_state::snapshot(harness.store()).unwrap()
        );
        golden(
            if panel {
                "s8c-quota-panel-sql-base.json"
            } else {
                "s8c-quota-iq-sql-base.json"
            },
            &answer,
        );
    }
}

fn commit_block_and_admission(harness: &Harness) {
    let facts = harness.store().get_facts().unwrap();
    for provider in facts
        .iter()
        .map(|fact| fact.provider_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let fact = facts
            .iter()
            .find(|fact| fact.provider_id == provider)
            .unwrap();
        crate::account_writer::record_provider_cooldown(
            harness.store(),
            &provider,
            &format!("{}/{}", provider, fact.model_id),
            &format!("{}/{}", provider, fact.model_id),
            &striatum_core::route_state_store::RouteStateUpdate {
                observed_at_ms: Some(NOW),
                unscoped_wall: true,
                cooldown_until: Some((NOW + 3_600_000) as f64),
                wall_class: Some(striatum_core::RouteWallClass::Billing),
                last_error: Some("billing_exhausted".into()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let provider = facts[0].provider_id.clone();
    let model = facts[0].model_id.clone();
    crate::account_writer::repair(harness.store(), move |tx| {
        let row: striatum_store::AccountAdmissionRow = striatum_store::AccountAdmissionRow {
            principal: "p".into(),
            admission_id: "interleaved".into(),
            request_fingerprint: "fp".into(),
            request_mode: "admit".into(),
            decision_id: "interleaved-decision".into(),
            conversation_hash: "interleaved-conversation".into(),
            provider: provider.clone(),
            model: model.clone(),
            account_id: "interleaved-account".into(),
            credential_id: "interleaved-credential".into(),
            pin_id: "interleaved-pin".into(),
            pin_generation: 1,
            state: "reserved".into(),
            charged: true,
            request_bytes: 1,
            request_bytes_estimated: false,
            committed_at: NOW,
            admission_valid_until: NOW + 60_000,
            lease_valid_until: NOW + 120_000,
            reply_json: "{}".into(),
            run_id: None,
            started_at: None,
            started_late: false,
            released_at: None,
            finished_at: None,
            last_observed_at: None,
        };
        tx.insert_pin(&striatum_store::AccountPinRow {
            pin_id: row.pin_id.clone(),
            principal: row.principal.clone(),
            conversation_hash: row.conversation_hash.clone(),
            provider: provider.clone(),
            pin_generation: 1,
            affinity_model: model.clone(),
            account_id: row.account_id.clone(),
            credential_id: row.credential_id.clone(),
            pin_mode: "conversation_lease".into(),
            previous_pin_id: None,
            decision_id: row.decision_id.clone(),
            created_at: NOW,
        })?;
        tx.insert_admission(&row)
    })
    .unwrap();
}

#[tokio::test]
async fn s8c_committed_blocks_and_admissions_between_points_and_seats_do_not_leak() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    for panel in [false, true] {
        let (harness, _, _) = panel_harness().await;
        let operator = operator(&harness).await;
        let params = if panel {
            json!({"class":"audit","seed":"interleaved"})
        } else {
            json!({"iqs":[0,50,100]})
        };
        let baseline = normalize(if panel {
            dry_panels(&operator, params.clone(), json!({})).await
        } else {
            evaluate(&operator, params.clone()).await
        });
        let seen = Arc::new(AtomicUsize::new(0));
        let calls = seen.clone();
        let changed = harness.clone();
        let guard = crate::account_selection::Sweep::hook(
            &harness.config_path_for_test(),
            move |_, is_panel, point| {
                if is_panel == panel && point == 1 {
                    commit_block_and_admission(&changed);
                    calls.fetch_add(1, Ordering::Relaxed);
                }
            },
        );
        let answer = normalize(if panel {
            dry_panels(&operator, params.clone(), json!({})).await
        } else {
            evaluate(&operator, params.clone()).await
        });
        drop(guard);
        assert_eq!(seen.load(Ordering::Relaxed), 1);
        assert_eq!(answer, baseline);
        let next = normalize(if panel {
            dry_panels(&operator, params, json!({})).await
        } else {
            evaluate(&operator, params).await
        });
        assert_ne!(next, baseline, "the committed wall must affect a new sweep");
        assert!(crate::account_state::snapshot(harness.store())
            .unwrap()
            .admissions
            .open_admission("p", "interleaved")
            .is_some());
    }
}

/// Seed a started source run and an unstarted replacement reservation joined
/// by an unused continuation offer. Both admissions contribute to the reported
/// in-flight count in this fixture.
pub(super) fn seed_open_offer(
    harness: &Harness,
    provider: &str,
    model: &str,
    account: &str,
    credential: &str,
) {
    harness
        .store()
        .account_transaction(|tx| {
            for id in ["offer-source", "offer-reservation"] {
                let row: striatum_store::AccountAdmissionRow =
                    striatum_store::AccountAdmissionRow {
                        principal: "p".into(),
                        admission_id: id.into(),
                        request_fingerprint: "fp".into(),
                        request_mode: "admit".into(),
                        decision_id: format!("decision-{id}"),
                        conversation_hash: format!("conversation-{id}"),
                        provider: provider.into(),
                        model: model.into(),
                        account_id: account.into(),
                        credential_id: credential.into(),
                        pin_id: format!("pin-{id}"),
                        pin_generation: 1,
                        state: "reserved".into(),
                        charged: true,
                        request_bytes: 1,
                        request_bytes_estimated: false,
                        committed_at: NOW,
                        admission_valid_until: NOW + 60_000,
                        lease_valid_until: NOW + 120_000,
                        reply_json: "{}".into(),
                        run_id: None,
                        started_at: None,
                        started_late: false,
                        released_at: None,
                        finished_at: None,
                        last_observed_at: None,
                    };
                tx.insert_pin(&striatum_store::AccountPinRow {
                    pin_id: row.pin_id.clone(),
                    principal: row.principal.clone(),
                    conversation_hash: row.conversation_hash.clone(),
                    provider: provider.into(),
                    pin_generation: 1,
                    affinity_model: model.into(),
                    account_id: account.into(),
                    credential_id: credential.into(),
                    pin_mode: "conversation_lease".into(),
                    previous_pin_id: None,
                    decision_id: row.decision_id.clone(),
                    created_at: NOW,
                })?;
                tx.insert_admission(&row)?;
                if id == "offer-source" {
                    tx.mark_admission_started("p", id, Some("offer-run"), NOW, false)?;
                }
            }
            tx.record_continuation(
                "p",
                "open-offer",
                &striatum_store::V1Recorded {
                    body_json: "{}".into(),
                    answer_json: "{}".into(),
                },
                "offer-run",
                "offer-source",
                "decision-offer-source",
                Some((
                    "decision-offer-reservation",
                    "offer-reservation",
                    NOW + 60_000,
                )),
            )
        })
        .unwrap();
}

#[tokio::test]
async fn s8c_capped_sweep_reuses_the_account_snapshot() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (harness, owner, routes) = panel_harness().await;
    let provider = routes[0].split_once('/').unwrap().0;
    harness.edit_config(|config| {
        config["concurrency"]["enforcement_enabled"] = json!(true);
        config["concurrency"]["providers"][provider] = json!({"limit":1});
    });
    let operator = operator(&harness).await;
    crate::account_state::snapshot(harness.store()).unwrap();
    let before = rows(&harness);
    let trace = harness.store().trace_account_reads_for_test().unwrap();
    trace.take();
    let commands = harness.store().account_writer_batch_sizes_for_test();
    let snapshots = crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed);
    let answer = evaluate(
        &operator,
        json!({"iqs":[0,50], "panel":{"class":"audit","seed":"capped"}}),
    )
    .await;
    let reads = trace.take();
    assert!(answer["results"][0]["pick"].is_object(), "{answer}");
    assert!(
        answer["concurrency"]["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["provider"] == provider
                && row["cap_counts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|count| count["effective_cap"] == 1)),
        "{answer}"
    );
    assert_eq!(
        crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed) - snapshots,
        1
    );
    assert!(reads.reader.is_empty(), "{:?}", reads.reader);
    assert!(reads.writer.is_empty(), "{:?}", reads.writer);
    assert_eq!(
        commands,
        harness.store().account_writer_batch_sizes_for_test()
    );
    assert_eq!(before, rows(&harness));

    trace.take();
    let snapshots = crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed);
    let (ty, estimate) = owner
        .call(
            "route.select_panel",
            &json!({"consumer":fixture::SUMMARY,"class":"audit","seed":"capped","estimate":true}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{estimate}");
    assert!(
        estimate["result"]["seats"].as_array().unwrap().len() >= 2,
        "{estimate}"
    );
    let reads = trace.take();
    assert_eq!(
        crate::account_state::SNAPSHOT_COUNT.load(Ordering::Relaxed) - snapshots,
        1
    );
    assert!(reads.reader.is_empty(), "{:?}", reads.reader);
    assert!(reads.writer.is_empty(), "{:?}", reads.writer);
    assert_eq!(
        commands,
        harness.store().account_writer_batch_sizes_for_test()
    );
    assert_eq!(before, rows(&harness));
}

/// A sweep judges every evaluation against the route rows it captured,
/// measurements included. A route-state write committed during the sweep
/// (here a new wall and error text on a captured route) changes
/// neither a later evaluation's snapshot nor its error-based wall scope; a
/// new sweep sees it.
#[tokio::test]
async fn a_route_state_write_during_a_sweep_leaves_its_later_evaluations_alone() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (harness, _) = fixture_with_repairs().await;
    let sweep = crate::account_selection::Sweep::capture(harness.store()).unwrap();
    let before = sweep.route_snapshot(NOW as f64).unwrap();
    let route = before
        .by_route
        .keys()
        .next()
        .expect("the fixture holds route rows")
        .clone();
    // Written through route.cooldown_update's handler, a production
    // route-state writer that runs in an account command.
    crate::record_cooldown_update(
        harness.store(),
        &serde_json::from_value(json!({
            "routeKey": route, "capacityKey": route,
            "cooldownUntil": NOW + 600_000,
            "lastError": "{\"class\":\"transient\",\"provider_code\":\"socket_timeout\"}"
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        sweep.route_snapshot(NOW as f64).unwrap(),
        before,
        "an evaluation later in the same sweep"
    );
    let next = crate::account_selection::Sweep::capture(harness.store())
        .unwrap()
        .route_snapshot(NOW as f64)
        .unwrap();
    assert_ne!(next, before, "a new sweep sees the committed write");
}

#[tokio::test]
async fn what_if_cleared_account_source_drops_its_clock_like_sql_without_live_writes() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (harness, operator) = fixture_with_repairs().await;
    harness
        .store()
        .account_transaction(|tx| tx.sync_fence_rechecks())
        .unwrap();
    let initial = crate::account_state::snapshot(harness.store()).unwrap();
    let key = serde_json::json!(["fresh-headroom-fixture"]).to_string();
    assert!(
        initial.blocks.clock("account", &key).is_some(),
        "the fixture must start with a registered clock"
    );
    let reference = harness.store().private_account_copy_for_test(NOW).unwrap();
    reference
        .account_transaction(|tx| tx.clear_fence("fresh-headroom-fixture", NOW, "fresh headroom"))
        .unwrap();
    let (expected, expected_sync) = reference
        .account_read(|tx| {
            let state = tx.load_block_state()?;
            Ok((
                state
                    .clock("account", &key)
                    .map(|(clock, cleared)| (clock.clone(), cleared)),
                state.needs_sync(),
            ))
        })
        .unwrap();
    assert!(
        expected.is_none(),
        "the ordinary SQL clear trigger and loader remove the clock"
    );
    let before = rows(&harness);
    let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = observations.clone();
    let _hook = crate::account_selection::Sweep::hook(
        &harness.config_path_for_test(),
        move |sweep, _, _| {
            let crate::account_state::Published::State { state, .. } = sweep.published() else {
                panic!("this regression must exercise native private sweep state");
            };
            observed.lock().unwrap().push((
                state
                    .blocks
                    .clock("account", &key)
                    .map(|(clock, cleared)| (clock.clone(), cleared)),
                state.blocks.needs_sync(),
                state
                    .blocks
                    .active_fences("example-subscription")
                    .iter()
                    .all(|fence| fence.fence_id != "fresh-headroom-fixture"),
            ));
        },
    );
    let trace = harness.store().trace_block_reads_for_test().unwrap();
    let _answer = evaluate(&operator, request()).await;
    assert!(
        observations.lock().unwrap().len() >= 2,
        "multiple points use the same cleared private state"
    );
    for (clock, sync, source_cleared) in observations.lock().unwrap().iter() {
        assert!(*source_cleared);
        assert_eq!(
            *clock, expected,
            "a what-if clear must drop the complete clock, not only hide its source"
        );
        assert_eq!(
            *sync, expected_sync,
            "the private clear must not leave a repairable orphan clock"
        );
    }
    assert!(
        trace.take().reader.is_empty(),
        "a live what-if must not read block SQL"
    );
    assert_eq!(
        before,
        rows(&harness),
        "the sweep cannot clear durable state"
    );
    assert_eq!(
        *initial,
        *crate::account_state::snapshot(harness.store()).unwrap()
    );
}
