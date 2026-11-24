mod rebind_tests {
    use super::*;
    use cortexkit_bus_inmemory::{InMemoryBus, InMemoryConfig};
    use cortexkit_bus_naming::AccountNames;
    use cortexkit_bus_trait::{ContentDigest, DurableOwner, Headers, Stream, StreamCursor};
    use std::collections::VecDeque;
    use striatum_store::AccountCommandKind;

    struct WakeWire {
        inner: FakeWire,
        credentials: crate::broca_credentials::SharedBrocaCredentials,
        status_calls: usize,
        local_grades: VecDeque<Result<media_grades::LocalGrades, FusiformCatalogError>>,
    }

    impl CatalogWire for WakeWire {
        fn credentials(&self) -> Option<crate::broca_credentials::SharedBrocaCredentials> {
            Some(self.credentials.clone())
        }
        async fn catalog_get(&mut self) -> Result<Arc<CatalogSnapshot>, FusiformCatalogError> {
            self.inner.catalog_get().await
        }
        async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError> {
            self.status_calls += 1;
            self.inner.catalog_status().await
        }
        async fn local_grades(&mut self) -> Result<media_grades::LocalGrades, FusiformCatalogError> {
            self.local_grades.pop_front().unwrap_or_else(|| Ok(Default::default()))
        }
        async fn close(&mut self) -> Result<(), FusiformCatalogError> {
            self.inner.close().await
        }
    }

    struct WakeRig {
        _rig: Rig,
        handler: crate::route_facts::Handler,
        health: SharedCatalogHealth,
        wire: WakeWire,
    }

    impl WakeRig {
        async fn new(label: &str) -> Self {
            let rig = Rig::new(label);
            let store = Arc::new(rig.store());
            let credentials = Arc::new(Mutex::new(
                crate::broca_credentials::BrocaCredentials::api_test_credentials(&[
                    crate::tests::fact("anthropic", "claude-sonnet-4-5"),
                ]),
            ));
            let handler = crate::route_facts::Handler {
                quota_cache: Arc::new(Mutex::new(crate::QuotaCache::default())),
                store,
                credentials: credentials.clone(),
                config_path: rig.dir.join("config.jsonc"),
            };
            let health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
            let mut wire = WakeWire {
                inner: FakeWire {
                    status: CatalogStatusReply { catalog_version: 0 },
                    snapshot: decode_catalog_get(&get_reply("catalog.get — one model, current"))
                        .unwrap(),
                    get_calls: 0,
                    closed: false,
                },
                credentials,
                status_calls: 0,
                local_grades: VecDeque::new(),
            };
            bootstrap_cycle(&mut wire, &handler.store, &health, None, None)
                .await
                .unwrap();
            assert_eq!(wire.inner.get_calls, 1, "startup must still bootstrap");
            assert_eq!(wire.status_calls, 0);
            let wake = handler.credentials.lock().unwrap().catalog_wake.clone();
            let _ = tokio::time::timeout(Duration::ZERO, wake.notified()).await;
            Self {
                _rig: rig,
                handler,
                health,
                wire,
            }
        }

        async fn cycle_after_wake(&mut self) -> CycleOutcome {
            let (wake, rebind) = {
                let guard = self.handler.credentials.lock().unwrap();
                (
                    guard.catalog_wake.clone(),
                    guard.catalog_rebind_wake.clone(),
                )
            };
            let forced = tokio::time::timeout(
                Duration::from_secs(1),
                wait_for_catalog_cycle(&wake, &rebind, Duration::from_secs(600)),
            )
            .await
            .expect("pending wake must not wait for the polling timer");
            if forced {
                bootstrap_cycle(
                    &mut self.wire,
                    &self.handler.store,
                    &self.health,
                    None,
                    None,
                )
                .await
                .unwrap()
            } else {
                poll_cycle(
                    &mut self.wire,
                    &self.handler.store,
                    &self.health,
                    None,
                    None,
                )
                .await
                .unwrap()
            }
        }

        fn change_catalog(&mut self, version: i64) {
            self.wire.inner.status.catalog_version = version;
            Arc::make_mut(&mut self.wire.inner.snapshot).cursor.catalog_version = version;
            Arc::make_mut(&mut self.wire.inner.snapshot).facts[0].cost_per_mtok_out = Some(42.0);
        }

        async fn newer_event(&self) {
            let names = AccountNames::derive("box_test").unwrap();
            let owner = DurableOwner::Module("striatum");
            let bus = InMemoryBus::new(InMemoryConfig::default().with_durable(owner));
            let mut cursor = bus.consumer(owner).await.unwrap();
            let subject = names
                .event_subject("fusiform", "catalog_changed", 1)
                .unwrap();
            let bytes = serde_json::to_vec(&json!({"catalog_version":1})).unwrap();
            let receipt = bus
                .publish(
                    &subject,
                    "catalog-new",
                    ContentDigest::of_bytes(&bytes),
                    Headers::new(),
                )
                .await
                .unwrap();
            assert!(!receipt.duplicate());
            let message = cursor.next().await.unwrap().unwrap().message;
            let delivery = crate::route_facts::Delivery {
                message,
                body: bytes,
                delivery_count: 1,
            };
            assert!(self
                .handler
                .consume(&mut cursor, &names, &delivery)
                .await
                .unwrap());
            assert!(
                cursor.next().await.unwrap().is_none(),
                "event must be acknowledged"
            );
        }
    }

    #[tokio::test]
    async fn incomplete_catalog_apply_retries_and_wakes_at_the_committed_version() {
        let mut rig = WakeRig::new("rebind-partial-apply").await;
        rig.change_catalog(1);
        let sql = rusqlite::Connection::open(rig._rig.dir.join("route.db")).unwrap();
        sql.execute_batch("CREATE TRIGGER refuse_quality BEFORE INSERT ON routing_operator_quality_state BEGIN SELECT RAISE(ABORT, 'quality write failed'); END;").unwrap();
        let failed = poll_cycle(&mut rig.wire, &rig.handler.store, &rig.health, None, None)
            .await.err().expect("quality write should fail");
        assert!(failed.to_string().contains("quality write failed"), "{failed}");
        assert_eq!(rig.handler.store.fusiform_catalog_cursor().unwrap().unwrap().catalog_version, 1);
        assert_eq!(rig.wire.credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().cursor.catalog_version, 0);
        sql.execute_batch("DROP TRIGGER refuse_quality").unwrap();

        rig.newer_event().await;
        let wake = rig.wire.credentials.lock().unwrap().catalog_wake.clone();
        let woke = tokio::time::timeout(Duration::ZERO, wake.notified()).await.is_ok();
        let retried = poll_cycle(&mut rig.wire, &rig.handler.store, &rig.health, None, None)
            .await.unwrap();
        assert_eq!((woke, retried.get_calls), (true, 1));
        assert_eq!(rig.wire.credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().cursor.catalog_version, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_local_grade_failure_recovers_at_same_catalog_version_without_refetch() {
        use media_grades::LocalGrades;
        let mut rig = WakeRig::new("retry-local-grade").await;
        let route = "runner/example";
        let grade = |score| Ok(LocalGrades::from_test_grade("local:runner/example", "transcription", score));
        let current_grade = |rig: &WakeRig| rig.wire.credentials.lock().unwrap().catalog_snapshot.as_ref().unwrap().local_media_grade("transcribe", route);
        rig.wire.local_grades = VecDeque::from([grade(59.0), Err(error("temporary timeout")), grade(63.0)]);
        rig.change_catalog(1);
        poll_cycle(&mut rig.wire, &rig.handler.store, &rig.health, None, None).await.unwrap();
        assert_eq!(current_grade(&rig), Some(59.0));
        rig.change_catalog(2);
        poll_cycle(&mut rig.wire, &rig.handler.store, &rig.health, None, None).await.unwrap();
        assert_eq!(current_grade(&rig), Some(59.0));
        tokio::time::advance(Duration::from_secs(61)).await;
        let cycle = poll_cycle(&mut rig.wire, &rig.handler.store, &rig.health, None, None).await.unwrap();
        assert_eq!(cycle.get_calls, 0);
        assert_eq!(current_grade(&rig), Some(63.0));
    }

    #[tokio::test]
    async fn catalog_rebind_unchanged_version_does_not_apply_or_write() {
        let mut rig = WakeRig::new("rebind-unchanged").await;
        let store = rig.handler.store.clone();
        let commands = store.account_command_count_for_test(AccountCommandKind::Maintenance);
        let acquisitions = store.writer_acquisition_count_for_test();
        let commits = store.writer_commit_count_for_test();
        rig.handler.wake_catalog_rebind().unwrap();
        assert_eq!(rig.cycle_after_wake().await.get_calls, 0);
        assert_eq!(rig.wire.status_calls, 1);
        assert_eq!(rig.wire.inner.get_calls, 1);
        assert_eq!(
            store.account_command_count_for_test(AccountCommandKind::Maintenance),
            commands
        );
        // The cursor read uses the existing connection helper, but submits no
        // maintenance command and opens no write transaction.
        assert_eq!(store.writer_acquisition_count_for_test(), acquisitions + 1);
        assert_eq!(store.writer_commit_count_for_test(), commits);
    }

    #[tokio::test]
    async fn catalog_rebind_changed_version_applies_once() {
        let mut rig = WakeRig::new("rebind-changed").await;
        rig.change_catalog(1);
        let store = rig.handler.store.clone();
        let commands = store.account_command_count_for_test(AccountCommandKind::Maintenance);
        rig.handler.wake_catalog_rebind().unwrap();
        assert_eq!(rig.cycle_after_wake().await.get_calls, 1);
        assert_eq!(rig.wire.status_calls, 1);
        assert_eq!(rig.wire.inner.get_calls, 2);
        assert_eq!(
            store.account_command_count_for_test(AccountCommandKind::Maintenance) - commands,
            1
        );
        assert_eq!(
            store
                .fusiform_catalog_cursor()
                .unwrap()
                .unwrap()
                .catalog_version,
            1
        );
    }

    #[tokio::test]
    async fn catalog_rebind_rolled_back_version_refreshes_different_content() {
        let mut rig = WakeRig::new("rebind-rollback").await;
        let store = rig.handler.store.clone();
        store
            .apply_fusiform_catalog(
                &[],
                FusiformCatalogCursor {
                    catalog_version: 10,
                    resolved_at_ms: 1,
                },
            )
            .unwrap();
        rig.change_catalog(1);
        rig.handler.wake_catalog_rebind().unwrap();
        assert_eq!(rig.cycle_after_wake().await.get_calls, 1);
        assert_eq!(rig.wire.status_calls, 1);
        assert_eq!(
            store
                .fusiform_catalog_cursor()
                .unwrap()
                .unwrap()
                .catalog_version,
            1
        );
        assert_eq!(
            rig.handler
                .credentials
                .lock()
                .unwrap()
                .catalog_snapshot
                .as_ref()
                .unwrap()
                .facts[0]
                .cost_per_mtok_out,
            Some(42.0)
        );
        assert_eq!(
            store
                .get_fact_by_route_key("anthropic/claude-sonnet-4-5")
                .unwrap()
                .unwrap()
                .cost_per_m_tok_out,
            Some(42.0)
        );
    }

    #[tokio::test]
    async fn catalog_newer_event_forces_refresh_and_covers_simultaneous_rebind() {
        for rebind in [false, true] {
            let mut rig = WakeRig::new(if rebind {
                "event-and-rebind"
            } else {
                "event-only"
            })
            .await;
            rig.change_catalog(1);
            let store = rig.handler.store.clone();
            let commands = store.account_command_count_for_test(AccountCommandKind::Maintenance);
            if rebind {
                rig.handler.wake_catalog_rebind().unwrap();
            }
            rig.newer_event().await;
            assert_eq!(rig.cycle_after_wake().await.get_calls, 1);
            assert_eq!(
                rig.wire.status_calls, 0,
                "a newer event must not wait for status"
            );
            assert_eq!(rig.wire.inner.get_calls, 2);
            assert_eq!(
                store.account_command_count_for_test(AccountCommandKind::Maintenance) - commands,
                1
            );
            let guard = rig.handler.credentials.lock().unwrap();
            for wake in [&guard.catalog_wake, &guard.catalog_rebind_wake] {
                assert!(
                    tokio::time::timeout(Duration::ZERO, wake.notified())
                        .await
                        .is_err(),
                    "one full refresh must cover both pending wakes"
                );
            }
        }
    }
}
