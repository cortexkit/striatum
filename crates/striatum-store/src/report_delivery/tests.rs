use super::*;
use std::sync::{Arc, Barrier};

fn descriptor() -> StorageDescriptor {
    let path = cortexkit_test_support::scratch_root().join(format!(
        "report-{}-{}.db",
        std::process::id(),
        short_id()
    ));
    dev_descriptor("report-delivery", path.to_string_lossy().into_owned())
}

fn report_outcome_for_run(
    store: &SqliteRouteStateStore,
    run: &str,
    outcome: RouteOutcome,
    now: i64,
) {
    store
        .v1_commit_report(
            "d",
            &format!("run:{run}"),
            &format!("report:d:{run}"),
            &V1Recorded {
                body_json: "{}".into(),
                answer_json: "{}".into(),
            },
            outcome,
            None,
            false,
            &V1ReportEffects {
                lift_credential: Some(("example".into(), "key".into())),
                ..Default::default()
            },
            None,
            |_| Ok(()),
            now,
        )
        .unwrap()
        .unwrap();
}

fn claim_decision_probe(store: &SqliteRouteStateStore, at: i64) {
    store
        .record_billing_account_route_fence(&AccountRouteFence {
            provider: "example".into(),
            credential_id: "key".into(),
            model: "model".into(),
            until_ms: at,
            recorded_at: at - 3_600_000,
            decision_id: "old".into(),
        })
        .unwrap();
    let origins = store.fence_rechecks().unwrap();
    assert!(store
        .account_transaction(|tx| tx.grant_fence_rechecks(&origins, at, Some("d")))
        .unwrap());
}

#[test]
fn completed_report_without_admission_heals_current_selection_time_claim() {
    let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
    seed(&store, "d", "p");
    claim_decision_probe(&store, 1);
    report_outcome_for_run(&store, "selected", RouteOutcome::Completed, 2);
    assert!(store.fence_rechecks().unwrap().is_empty());
}

#[test]
fn completed_reused_decision_without_admission_cannot_resurrect_interrupted_claim() {
    let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
    seed(&store, "d", "p");
    claim_decision_probe(&store, 1);
    report_outcome_for_run(&store, "interrupted", RouteOutcome::Interrupted, 2);
    store
        .store
        .with_conn(|c| {
            assert_eq!(
                c.query_row("SELECT count(*) FROM probe_authorization", [], |row| row
                    .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
    report_outcome_for_run(&store, "later-no-claim", RouteOutcome::Completed, 3);
    assert_eq!(store.fence_rechecks().unwrap().len(), 1);
    let origins = store.fence_rechecks().unwrap();
    assert!(store
        .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 4, Some("d")))
        .unwrap());
    report_outcome_for_run(&store, "later-claim", RouteOutcome::Completed, 5);
    assert_eq!(store.fence_rechecks().unwrap().len(), 1,
        "without a run-specific launch clock, a claim after selection cannot be attributed to success");
}

#[test]
fn completed_decision_report_does_not_consume_a_replaced_claim_generation() {
    let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
    seed(&store, "d", "p");
    claim_decision_probe(&store, 1);
    let origins = store.fence_rechecks().unwrap();
    assert!(store
        .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 3_600_002, Some("other")))
        .unwrap());
    report_outcome_for_run(&store, "stale", RouteOutcome::Completed, 3_600_003);
    let remaining = store.fence_rechecks().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].claim_owner.as_deref(), Some("other"));
}

#[test]
fn report_admission_probe_settlement_retires_only_its_matching_decision_copy() {
    for changed in ["matching", "other-generation", "newer-evidence"] {
        let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
        seed(&store, "d", "p");
        claim_decision_probe(&store, 1);
        let owner = serde_json::json!(["account_admission", "runner", "admission"]).to_string();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| {
                tx.claim_fence_rechecks_for_authorized_admission(&origins, 2, Some("d"), &owner)
            })
            .unwrap());
        if changed == "other-generation" {
            store.store.with_conn(|c| {
                c.execute("UPDATE probe_authorization SET claim_generation=claim_generation+1 WHERE owner='d'", [])?;
                Ok(())
            }).unwrap();
        } else if changed == "newer-evidence" {
            store
                .record_billing_account_route_fence(&AccountRouteFence {
                    provider: "example".into(),
                    credential_id: "key".into(),
                    model: "model".into(),
                    until_ms: 100,
                    recorded_at: 3,
                    decision_id: "new".into(),
                })
                .unwrap();
        }
        let origins = store.fence_rechecks().unwrap();
        store
            .account_transaction(|tx| {
                tx.heal_report_probe(&owner, Some("d"), true, "model", 2, &origins, None, 4)
            })
            .unwrap();
        assert_eq!(
            store.fence_rechecks().unwrap().is_empty(),
            changed != "newer-evidence"
        );
        store
            .store
            .with_conn(|c| {
                let copies: i64 = c.query_row(
                    "SELECT count(*) FROM probe_authorization WHERE owner='d'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(copies, i64::from(changed != "matching"), "{changed}");
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn inconclusive_v1_reports_release_only_their_non_quota_probe_claim() {
    for (outcome, error, neutral) in [
        (RouteOutcome::Cancelled, None, true),
        (RouteOutcome::Interrupted, None, true),
        (RouteOutcome::Error, Some("content_policy"), true),
        (RouteOutcome::Error, Some("request_invalid"), true),
        (RouteOutcome::Error, Some("account_limit_exhausted"), false),
        (RouteOutcome::Error, Some("server_error"), false),
    ] {
        let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
        seed(&store, "d", "p");
        store
            .record_billing_account_route_fence(&AccountRouteFence {
                provider: "example".into(),
                credential_id: "key".into(),
                model: "model".into(),
                until_ms: 1000,
                recorded_at: 100,
                decision_id: "old".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 1000, Some("d")))
            .unwrap());
        store
            .v1_commit_report(
                "d",
                "outcome",
                "r",
                &V1Recorded {
                    body_json: "{}".into(),
                    answer_json: "{}".into(),
                },
                outcome,
                error,
                outcome == RouteOutcome::Error,
                &V1ReportEffects::default(),
                None,
                |_| Ok(()),
                1001,
            )
            .unwrap()
            .unwrap();
        let remaining = store.fence_rechecks().unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "an inconclusive report is not serving success"
        );
        assert_eq!(
            remaining[0].next_recheck_at <= 1001,
            neutral,
            "{outcome:?}/{error:?}"
        );
    }
}

fn seed(store: &SqliteRouteStateStore, id: &str, principal: &str) {
    store
        .seed_preupgrade_decision_for_test(
            &V1Decision {
                decision_id: id.into(),
                principal: principal.into(),
                consumer: "summary".into(),
                allowed_account_class: striatum_core::accounts::AllowedAccountClass::NotPayPerToken,
                project_id: None,
                provider: "example".into(),
                model: "model".into(),
                variant: None,
                runner_module: "runner".into(),
                affinity: Some("key".into()),
                created_at_ms: 1,
                expires_at_ms: 10000,
            },
            true,
        )
        .unwrap();
    // Source upserts mention source_metadata_json even when its value is unchanged. Updates
    // to managed probe clocks, quota/classified or source_order do not name
    // source_metadata_json, so they must not add a second source transition to the report audit.
    store.store.with_conn(|c| {
        c.execute_batch("CREATE TABLE IF NOT EXISTS report_transition(kind TEXT NOT NULL);
            CREATE TRIGGER IF NOT EXISTS report_audit_outcome AFTER UPDATE OF outcome ON model_route_decisions BEGIN INSERT INTO report_transition VALUES ('outcome'); END;
            CREATE TRIGGER IF NOT EXISTS report_audit_continuation AFTER UPDATE OF used_at_ms ON account_continue BEGIN INSERT INTO report_transition VALUES ('continuation'); END;
            CREATE TRIGGER IF NOT EXISTS report_audit_affinity AFTER UPDATE OF ended_at_ms ON routing_v1_affinity BEGIN INSERT INTO report_transition VALUES ('affinity'); END;
            CREATE TRIGGER IF NOT EXISTS report_audit_usage AFTER INSERT ON model_route_usage BEGIN INSERT INTO report_transition VALUES ('usage'); END;")?;
        // Schema-77 report-migration fixtures reuse this decision setup but
        // predate block and never inspect the source-transition audit.
        if c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='block')", [], |row|row.get::<_,bool>(0))? {
            c.execute_batch("CREATE TRIGGER IF NOT EXISTS report_audit_route_insert AFTER INSERT ON block WHEN NEW.kind='account_route' BEGIN INSERT INTO report_transition VALUES ('route_fence'); END;
                CREATE TRIGGER IF NOT EXISTS report_audit_route_update AFTER UPDATE OF source_metadata_json ON block WHEN NEW.kind='account_route' BEGIN INSERT INTO report_transition VALUES ('route_fence'); END;
                CREATE TRIGGER IF NOT EXISTS report_audit_credential_insert AFTER INSERT ON block WHEN NEW.kind='credential' BEGIN INSERT INTO report_transition VALUES ('credential'); END;
                CREATE TRIGGER IF NOT EXISTS report_audit_credential_update AFTER UPDATE OF source_metadata_json ON block WHEN NEW.kind='credential' BEGIN INSERT INTO report_transition VALUES ('credential'); END;
                CREATE TRIGGER IF NOT EXISTS report_audit_pool AFTER INSERT ON block WHEN NEW.kind='account' BEGIN INSERT INTO report_transition VALUES ('pool'); END;")?;
        }
        c.execute("INSERT INTO routing_v1_affinity (principal,consumer,affinity,decision_id,updated_at_ms) VALUES (?1,'summary',?2,?2,1)",params![principal,id])?;
        c.execute("INSERT INTO account_continue (principal,selection_id,request_json,answer_json,run_id,admission_id,previous_decision_id,decision_id,valid_until_ms)
            VALUES (?1,?2,'{}','{}','run','admission','previous',?2,1000)",params![principal,id])?;
        Ok(())
    }).unwrap();
}

fn commit(
    store: &SqliteRouteStateStore,
    id: &str,
    key: &str,
    value: i64,
) -> RouteStateResult<Option<V1Recorded>> {
    let body = serde_json::json!({"decision_id":id,"outcome":"failed","marker":value}).to_string();
    let recorded = V1Recorded {
        body_json: body.clone(),
        answer_json: body,
    };
    let effects = V1ReportEffects {
        route_fence: Some(AccountRouteFence {
            provider: "example".into(),
            credential_id: "credential".into(),
            model: "model".into(),
            until_ms: 100 + value,
            decision_id: id.into(),
            recorded_at: value,
        }),
        credential_refusal: Some((
            CredentialRefusal {
                provider_id: "example".into(),
                credential_id: "credential".into(),
                code: format!("refusal-{value}"),
            },
            None,
        )),
        ..Default::default()
    };
    let usage = (key != "outcome").then(|| V1ReportUsage {
        observation: UsageObservation {
            id: format!("routing-v1-report:{id}:{key}"),
            route_key: "example/model".into(),
            capacity_key: "example/model".into(),
            provider_id: "example".into(),
            output_tokens: value as f64,
            observed_at: value as f64,
            input_tokens: None,
            duration_ms: -1.0,
            reasoning_tokens: None,
            cached_read_tokens: None,
            cache_write_tokens: None,
            workload_class: None,
            account_key: None,
        },
        tier_applied: None,
        tier_served: None,
        cost: None,
    });
    let fence_id = format!("fence:{id}");
    store.v1_commit_report(
        id,
        key,
        &format!("report:{id}:{key}"),
        &recorded,
        RouteOutcome::Error,
        Some("failed"),
        true,
        &effects,
        usage.as_ref(),
        move |tx| {
            tx.insert_pool_fence(&AccountPoolFenceRow {
                evidence: AccountFenceRow {
                    fence_id: fence_id.clone(),
                    provider: "example".into(),
                    account_id: "account".into(),
                    scope_json: "null".into(),
                    fence_json: "{}".into(),
                    source: "test".into(),
                    observed_at: value,
                    principal: "owner".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: value,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set: vec![],
            })
        },
        value,
    )
}

fn assert_effects(store: &SqliteRouteStateStore, value: i64, usage_rows: i64) {
    store
        .store
        .with_conn(|c| {
            for kind in [
                "outcome",
                "continuation",
                "route_fence",
                "credential",
                "affinity",
                "pool",
            ] {
                let transitions: i64 = c.query_row(
                    "SELECT count(*) FROM report_transition WHERE kind=?1",
                    [kind],
                    |r| r.get(0),
                )?;
                assert_eq!(transitions, 1, "{kind}");
            }
            let usage_transitions: i64 = c.query_row(
                "SELECT count(*) FROM report_transition WHERE kind='usage'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(usage_transitions, usage_rows);
            let at: i64 = c.query_row(
                "SELECT outcome_at FROM model_route_decisions WHERE id='d'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(at, value);
            let block_at: i64 = c.query_row(
                "SELECT created_at FROM block WHERE kind='account' AND origin_key='[\"fence:d\"]' AND is_origin=1",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(block_at, value);
            let used: i64 = c.query_row(
                "SELECT used_at_ms FROM account_continue WHERE decision_id='d'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(used, value);
            let (until, observed): (i64, i64) = c.query_row(
                "SELECT deadline,last_reported_at FROM block WHERE kind='account_route' AND source_present=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!((until, observed), (100 + value, value));
            let code: String =
                c.query_row("SELECT code FROM block WHERE kind='credential' AND source_present=1", [], |r| {
                    r.get(0)
                })?;
            assert_eq!(code, format!("refusal-{value}"));
            let ended: i64 = c.query_row(
                "SELECT ended_at_ms FROM routing_v1_affinity WHERE decision_id='d'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(ended, value);
            let count: i64 =
                c.query_row("SELECT count(*) FROM model_route_usage", [], |r| r.get(0))?;
            assert_eq!(count, usage_rows);
            if usage_rows == 1 {
                let output: i64 =
                    c.query_row("SELECT output_tokens FROM model_route_usage", [], |r| {
                        r.get(0)
                    })?;
                assert_eq!(output, value);
            }
            Ok(())
        })
        .unwrap();
}

fn concurrent(key: &str, differing: bool) {
    let descriptor = descriptor();
    let store = Arc::new(SqliteRouteStateStore::open(&descriptor).unwrap());
    seed(&store, "d", "owner");
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [10, if differing { 11 } else { 10 }]
        .into_iter()
        .map(|value| {
            let store = store.clone();
            let barrier = barrier.clone();
            let key = key.to_owned();
            std::thread::spawn(move || {
                barrier.wait();
                commit(&store, "d", &key, value).unwrap()
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(
        results.iter().filter(|r| r.is_some()).count(),
        if differing && key == "outcome" { 1 } else { 2 }
    );
    let first = results.iter().flatten().next().unwrap();
    for standing in results.iter().flatten() {
        assert_eq!(standing, first);
    }
    let value = serde_json::from_str::<serde_json::Value>(&first.answer_json).unwrap()["marker"]
        .as_i64()
        .unwrap();
    assert_effects(&store, value, if key == "outcome" { 0 } else { 1 });
    drop(store);
    let reopened = SqliteRouteStateStore::open(&descriptor).unwrap();
    assert_eq!(
        commit(&reopened, "d", key, value).unwrap(),
        Some(first.clone())
    );
    assert_effects(&reopened, value, if key == "outcome" { 0 } else { 1 });
}

#[test]
fn concurrent_identical_outcome_reports_apply_effects_once() {
    concurrent("outcome", false);
}

#[test]
fn report_effect_source_updates_are_counted_once_without_their_clock_updates() {
    let descriptor = descriptor();
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    seed(&store, "d", "owner");
    store
        .record_account_route_fence(&AccountRouteFence {
            provider: "example".into(),
            credential_id: "credential".into(),
            model: "model".into(),
            until_ms: 101,
            recorded_at: 1,
            decision_id: "old".into(),
        })
        .unwrap();
    store
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "example".into(),
                credential_id: "credential".into(),
                code: "old-refusal".into(),
            },
            None,
            1,
        )
        .unwrap();
    store
        .store
        .with_conn(|conn| conn.execute("DELETE FROM report_transition", []))
        .unwrap();
    // A credential's metadata is still {} when its code changes. The source
    // update must count even though that metadata value is unchanged, while
    // registering the new source's clock must not count again.
    let first = commit(&store, "d", "run:r", 10).unwrap().unwrap();
    assert_effects(&store, 10, 1);
    drop(store);
    let reopened = SqliteRouteStateStore::open(&descriptor).unwrap();
    assert_eq!(commit(&reopened, "d", "run:r", 20).unwrap(), Some(first));
    assert_effects(&reopened, 10, 1);
}

#[test]
fn concurrent_differing_outcome_reports_conflict_without_losing_effects() {
    concurrent("outcome", true);
}
#[test]
fn concurrent_differing_run_reports_return_the_first_and_count_once() {
    concurrent("run:r", true);
}

#[test]
fn outcome_and_run_reports_coexist_but_mixed_usage_identities_conflict() {
    for first in ["outcome", "run:r"] {
        let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
        seed(&store, "d", "owner");
        assert!(commit(&store, "d", first, 10).unwrap().is_some());
        let second = if first == "outcome" {
            "run:r"
        } else {
            "outcome"
        };
        assert!(commit(&store, "d", second, 11).unwrap().is_some());
        assert!(commit(&store, "d", "run:r2", 12).unwrap().is_some());
        assert!(commit(&store, "d", "usage", 13).unwrap().is_none());
        store
            .store
            .with_conn(|c| {
                assert_eq!(
                    c.query_row("SELECT count(*) FROM model_route_usage", [], |r| r
                        .get::<_, i64>(0))?,
                    2
                );
                assert_eq!(
                    c.query_row("SELECT used_at_ms FROM account_continue", [], |r| r
                        .get::<_, i64>(0))?,
                    10
                );
                Ok(())
            })
            .unwrap();
    }
    let store = SqliteRouteStateStore::open(&descriptor()).unwrap();
    seed(&store, "d", "owner");
    assert!(commit(&store, "d", "usage", 10).unwrap().is_some());
    assert!(commit(&store, "d", "run:r", 11).unwrap().is_none());
    assert!(commit(&store, "d", "outcome", 11).unwrap().is_none());
}

#[test]
fn run_id_is_scoped_to_decision_and_principal_across_restart() {
    let descriptor = descriptor();
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    for (id, owner) in [("d1", "one"), ("d2", "one"), ("d3", "two")] {
        seed(&store, id, owner);
        commit(&store, id, "run:shared", 10).unwrap().unwrap();
    }
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    for id in ["d1", "d2", "d3"] {
        commit(&store, id, "run:shared", 20).unwrap().unwrap();
    }
    store
        .store
        .with_conn(|c| {
            assert_eq!(
                c.query_row("SELECT count(*) FROM model_route_usage", [], |r| r
                    .get::<_, i64>(0))?,
                3
            );
            assert_eq!(
                c.query_row(
                    "SELECT sum(output_tokens) FROM model_route_usage",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                30
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn report_crash_child() {
    let Ok(path) = std::env::var("ROUTING_REPORT_CRASH_DB") else {
        return;
    };
    let store = SqliteRouteStateStore::open(&dev_descriptor("report-delivery", path)).unwrap();
    commit(&store, "d", "run:r", 10).unwrap();
    panic!("crash injection did not exit");
}

#[test]
fn crash_before_commit_rolls_back_and_after_commit_replays_without_effects() {
    for point in ["before_commit", "after_commit"] {
        let descriptor = descriptor();
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        seed(&store, "d", "owner");
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            unreachable!()
        };
        let path = path.clone();
        drop(store);
        let status = std::process::Command::new(crate::test_exe::current_test_exe().unwrap())
            .args([
                "--exact",
                "report_delivery::tests::report_crash_child",
                "--nocapture",
            ])
            .env("ROUTING_REPORT_CRASH", point)
            .env("ROUTING_REPORT_CRASH_DB", path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let recorded = store.v1_report("d").unwrap();
        assert_eq!(recorded.is_some(), point == "after_commit");
        commit(&store, "d", "run:r", 10).unwrap().unwrap();
        assert_effects(&store, 10, 1);
    }
}

#[test]
fn call_counts_keep_partial_window_across_restart_and_sum_staggered_instances() {
    let descriptor = descriptor();
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    store
        .store
        .with_conn(|c| c.execute("UPDATE routing_call_window SET started_at_ms=100", []))
        .unwrap();
    store
        .record_routing_call("route.select", "legacy", "reserved:prefrontal-core")
        .unwrap();
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    store
        .record_routing_call("route.select", "legacy", "reserved:prefrontal-core")
        .unwrap();
    let first = store.routing_call_counts().unwrap();
    assert_eq!(first.started_at_ms, 100);
    assert_eq!(first.counts[0].calls, 2);
    let second = RoutingCallCounts {
        started_at_ms: 200,
        counts: vec![RoutingCallCount {
            calls: 3,
            ..first.counts[0].clone()
        }],
    };
    let total = RoutingCallCounts::combine(&[first, second]).unwrap();
    assert_eq!(total.started_at_ms, 200);
    assert_eq!(total.counts[0].calls, 5);
}

#[test]
fn report_delivery_migration_preserves_legacy_answers_and_usage_mode() {
    let descriptor = descriptor();
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 77).unwrap();
    seed(&old, "d", "owner");
    let first = V1Recorded {
        body_json: r#"{"decision_id":"d","outcome":"completed","usage":{"output_tokens":10}}"#
            .into(),
        answer_json: r#"{"decision_id":"d","report_id":"report:d","outcome":"completed"}"#.into(),
    };
    old.v1_record_report("d", "report:d", &first, false, 10)
        .unwrap();
    drop(old);
    let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
    assert_eq!(
        upgraded
            .v1_commit_report(
                "d",
                "usage",
                "report:d",
                &first,
                RouteOutcome::Completed,
                None,
                false,
                &V1ReportEffects::default(),
                None,
                |_| Ok(()),
                20
            )
            .unwrap(),
        Some(first)
    );
    assert!(commit(&upgraded, "d", "run:r", 20).unwrap().is_none());
}

#[test]
fn report_delivery_migration_preserves_opaque_legacy_evidence() {
    let descriptor = descriptor();
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 77).unwrap();
    seed(&old, "d", "owner");
    let first = V1Recorded {
        body_json: "opaque legacy report body".into(),
        answer_json: "opaque legacy report answer".into(),
    };
    old.v1_record_report("d", "report:d", &first, false, 10)
        .unwrap();
    drop(old);
    let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
    assert_eq!(upgraded.v1_report("d").unwrap(), Some(first.clone()));
    let standing = upgraded
        .v1_commit_report(
            "d",
            "outcome",
            "report:d",
            &first,
            RouteOutcome::Completed,
            None,
            false,
            &V1ReportEffects::default(),
            None,
            |_| Ok(()),
            20,
        )
        .unwrap();
    assert_eq!(standing, Some(first));
}

#[test]
fn split_restart_report_settles_a_probe_bound_before_the_split() {
    let descriptor = descriptor();
    let StorageBackend::Sqlite { path } = &descriptor.backend else {
        unreachable!()
    };
    let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    seed(&store, "d", "p");
    claim_decision_probe(&store, 1);
    assert_eq!(store.fence_rechecks().unwrap().len(), 1);
    drop(store);
    crate::history::run_split_for_test(path, 2);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    report_outcome_for_run(&store, "selected", RouteOutcome::Completed, 2);
    assert!(store.fence_rechecks().unwrap().is_empty());
}
