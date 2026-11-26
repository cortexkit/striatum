use super::*;
use crate::catalog::{install_catalog_change_tracking, next_catalog_cache_identity};

impl SqliteRouteStateStore {
    pub(super) fn open_with_migrations(
        descriptor: &StorageDescriptor,
        migrations: &[Migration],
    ) -> RouteStateResult<Self> {
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            return Err(err("route state requires a sqlite descriptor"));
        };
        let memory = path.is_empty()
            || path == ":memory:"
            || path == "file::memory:"
            || path.starts_with("file::memory:?");
        let split = migrations
            .iter()
            .any(|migration| migration.version == history::STATE_MIGRATION.version);
        if split && !memory {
            history::inspect_files(path)?;
        }
        let store = open_sqlite(descriptor).map_err(err)?;
        let mut archive = if split {
            let recorded = store
                .with_conn(|conn| history::version(conn, ROUTE_STATE_NAMESPACE))
                .map_err(err)?;
            if recorded > 0 && recorded < history::STATE_MIGRATION.version {
                let before_split = migrations
                    .iter()
                    .position(|migration| migration.version == history::STATE_MIGRATION.version)
                    .unwrap();
                migrate_route_state(&store, &migrations[..before_split])?;
            }
            // Check again with the state writer lease held. For example,
            // migration 082 can create archived retry clocks in fence_recheck.
            store
                .with_conn(|conn| {
                    history::refuse_moved_rows(conn)
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
                })
                .map_err(err)?;
            let state = store
                .with_conn(|conn| {
                    history::state_binding(conn, path, recorded)
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
                })
                .map_err(err)?;
            let archive_path = history::path_for(std::path::Path::new(path));
            let archive = history::History::open(path, memory, &descriptor.module_id)?;
            let identity = archive.bind(&archive_path, state.as_ref())?;
            // Binding history first makes an interrupted first open retryable
            // only while its archive is still empty.
            history::migrate_split(&store, migrations, identity)?;
            Some(archive)
        } else {
            migrate_route_state(&store, migrations)?;
            None
        };
        validate_family_spellings(&store)?;
        // Changing temp_store discards existing TEMP objects, so configure it
        // before installing the catalog write counter and its TEMP triggers.
        store
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)?;
                conn.pragma_update(None, "journal_size_limit", WAL_SIZE_CAP_BYTES)?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                // Savepoint statement journals otherwise create and delete a
                // temp file per account command once they spill out of memory.
                conn.pragma_update(None, "temp_store", "MEMORY")
            })
            .map_err(err)?;
        // A failure here costs only the cache: every fingerprint read finds the
        // tracking missing, computes the value fresh and tries to install it again.
        if let Err(error) = store.with_conn(install_catalog_change_tracking) {
            tracing::warn!(%error, "catalog change tracking not installed at open; fingerprints are computed fresh until it is");
        }
        // Hold each checkout until the next is configured, filling the lazy pool.
        // Move this to cortexkit-store 0.4's open_sqlite_with_init hook when upgrading,
        // so replacements after failed rollbacks get the larger cache too. A rare
        // replacement keeps SQLite's default cache; configuring every checkout is
        // more expensive than the larger cache can save.
        fn prewarm(store: &SqliteStore, count: usize) -> Result<(), cortexkit_store::StoreError> {
            if count == 0 {
                return Ok(());
            }
            let mut nested = Ok(());
            store.with_read(|conn| {
                conn.pragma_update(None, "cache_size", READER_CACHE_SIZE_KIB)?;
                nested = prewarm(store, count - 1);
                Ok(())
            })?;
            nested
        }
        if let Some(history) = archive.as_mut().filter(|_| !memory) {
            history.open_reader(path)?;
        }
        prewarm(
            &store,
            if memory {
                1
            } else {
                cortexkit_store::SqliteOpenOptions::default().read_pool_size
            },
        )
        .map_err(err)?;
        let mut result = Self::from_inner(connection_store::ConnectionStore::live(store));
        result.history = archive;
        if !memory {
            let maintenance = Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .map_err(err)?;
            maintenance
                .busy_timeout(WAL_TRUNCATE_WAIT_BOUND)
                .map_err(err)?;
            maintenance
                .pragma_update(None, "synchronous", "NORMAL")
                .map_err(err)?;
            result.maintenance = Some(Mutex::new(maintenance));
        }
        Ok(result)
    }

    pub(super) fn from_inner(store: connection_store::ConnectionStore) -> Self {
        Self {
            maintenance: None,
            history: None,
            store: std::sync::Arc::new(store),
            account_writer: std::sync::OnceLock::new(),
            account_state: std::sync::Arc::new(std::sync::OnceLock::new()),
            account_state_retry: Mutex::new(None),
            account_state_retries: AtomicU64::new(0),
            call_counts: Default::default(),
            class_vectors: Mutex::new(HashMap::new()),
            class_generations: Default::default(),
            reported_score_conflicts: Mutex::new(HashSet::new()),
            catalog_fingerprint: Mutex::new(None),
            resolved_facts: Mutex::new(None),
            selection_catalog: Mutex::new(None),
            catalog_facts: Mutex::new(None),
            catalog_cache_identity: AtomicU64::new(next_catalog_cache_identity()),
            #[cfg(any(test, feature = "test-support"))]
            account_write_transactions: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::decision_ledger::short_id;

    /// Insert one ledger row the way an older routing build left it: a
    /// `payload_bytes`-long score payload sits before the outcome columns, as
    /// real score JSON does.
    pub(crate) fn insert_ledger_row(
        connection: &Connection,
        id: &str,
        parent_session_id: Option<&str>,
        created_at: i64,
        payload_bytes: i64,
    ) -> rusqlite::Result<usize> {
        connection.execute(
            "INSERT INTO model_route_decisions
               (id, parent_session_id, target_agent, requirements_json, selected_route_key,
                selected_provider_id, selected_model_id, score_json, rejected_json,
                anchor_version, created_at)
             VALUES (?1, ?2, 'mason', '{}', 'p/' || ?1, 'p', ?1,
                     printf('%.*c', ?4, 'x'), '[]', 1, ?3)",
            params![id, parent_session_id, created_at, payload_bytes],
        )
    }

    #[cfg(unix)]
    #[test]
    fn route_store_state_is_owner_only_under_test_scratch_root() {
        use std::os::unix::fs::PermissionsExt;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = cortexkit_test_support::scratch_root().join(format!(
            "striatum-private-state-{}-{timestamp}",
            std::process::id()
        ));
        let db = root.join("store.db");
        let descriptor = dev_descriptor("striatum-private-state", db.to_string_lossy());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open scratch routing store");

        let mut pending = vec![root.clone()];
        while let Some(path) = pending.pop() {
            let metadata = std::fs::symlink_metadata(&path).expect("stat route-state path");
            let mode = metadata.permissions().mode() & 0o777;
            if metadata.is_dir() {
                assert_eq!(
                    mode,
                    0o700,
                    "routing directory is not owner-only: {}",
                    path.display()
                );
                pending.extend(
                    std::fs::read_dir(&path)
                        .expect("list route-state directory")
                        .map(|entry| entry.unwrap().path()),
                );
            } else {
                assert_eq!(
                    mode,
                    0o600,
                    "routing file is not owner-only: {}",
                    path.display()
                );
            }
        }

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn migration_065_preserves_062_evidence_and_freezes_historical_classes() {
        use striatum_core::accounts::{AllowedAccountClass, ModelFamily, PoolScope};
        let scratch = cortexkit_test_support::ScratchDir::new("routing-policy-065");
        let descriptor = dev_descriptor(
            "routing-policy-065",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 62).unwrap();
        let observed_at = 1_000_000;
        legacy.store.with_conn(|c| {
            populate_account_tables(c, observed_at)?;
            for (id, consumer) in [("cm", Some("codemode")), ("fl", Some("flow")), ("ma", Some("mason")), ("null", None)] {
                insert_ledger_row(c, id, None, observed_at, 2)?;
                c.execute("UPDATE model_route_decisions SET consumer=?2 WHERE id=?1", params![id, consumer])?;
                c.execute("INSERT INTO routing_v1_decision (decision_id, principal, consumer, provider, model,
                    runner_module, affinity, created_at_ms, expires_at_ms)
                    VALUES (?1, 'owner', ?2, 'p', 'm', 'runner', 'conversation', ?3, ?4)",
                    params![id, consumer.unwrap_or("other"), observed_at, observed_at+60_000])?;
            }
            for (id, scope) in [
                ("general", r#"{"kind":"general","value":"seven_day"}"#),
                ("short", r#"{"kind":"general","value":"five_hour"}"#),
                ("reported", r#"{"kind":"general","value":{"reported":90}}"#),
                ("family", r#"{"kind":"family","value":"opus"}"#),
                ("unknown", r#"{"kind":"unknown_scope","value":"fable"}"#),
            ] {
                let evidence = format!(r#"{{"account_id":"a","scope":{scope},"observed_at_ms":{observed_at},"reset":{{"kind":"at","at_ms":2000000}},"retry_hint_ms":null,"source":"raw_header"}}"#);
                c.execute("INSERT INTO account_fence (fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at)
                    VALUES (?1,'anthropic','a',?2,?3,'raw_header',?4,'p',?4)", params![id,scope,evidence,observed_at])?;
            }
            c.execute_batch("INSERT INTO routing_v1_affinity VALUES ('owner','mason','conversation','ma',1000000,NULL);
                INSERT INTO routing_v1_report VALUES ('ma','report','report body','report answer',1000000);
                INSERT INTO routing_v1_admission VALUES ('runner','admission','original body','original answer',1000000);
                INSERT INTO credential_entitlement_fences VALUES ('p','cred','ENTITLEMENT_ERROR','listing',1000000);
                INSERT INTO provider_reset_fences VALUES ('p/m','mason','a','cred',2000000,1000000);")?;
            Ok(())
        }).unwrap();
        // Today's typed row includes columns the old schema does not have.
        // Freeze every old column and compare its stored value after upgrade.
        let (old_columns, old_admission) = legacy
            .store
            .with_conn(|conn| {
                let mut statement = conn.prepare(
                    "SELECT * FROM account_admission WHERE principal='p' AND admission_id='adm-1'",
                )?;
                let columns: Vec<String> = statement
                    .column_names()
                    .iter()
                    .map(|name| format!("\"{}\"", name.replace('"', "\"\"")))
                    .collect();
                let values = statement.query_row([], |row| {
                    (0..columns.len())
                        .map(|i| row.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })?;
                Ok((columns, values))
            })
            .unwrap();
        let old_fences = legacy_fixture_inputs::legacy_fences(&legacy, "anthropic");
        drop(legacy);

        let migrated = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
        drop(migrated);
        let restarted = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
        assert_eq!(
            restarted
                .store
                .with_conn(|conn| {
                    conn.query_row(
                        &format!(
                    "SELECT {} FROM account_admission WHERE principal='p' AND admission_id='adm-1'",
                    old_columns.join(","),
                ),
                        [],
                        |row| {
                            (0..old_columns.len())
                                .map(|i| row.get::<_, rusqlite::types::Value>(i))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        },
                    )
                })
                .unwrap(),
            old_admission
        );
        let blocks = restarted
            .account_transaction(|tx| tx.active_pool_fences("anthropic"))
            .unwrap();
        for block in &blocks {
            assert_eq!(
                &block.evidence,
                old_fences
                    .iter()
                    .find(|row| row.fence_id == block.evidence.fence_id)
                    .unwrap()
            );
            assert!(block.recovery_set.is_empty());
            if block.evidence.fence_id == "fence-1" {
                continue;
            }
            let key = block.pool_key.as_ref().unwrap();
            assert_eq!(key.name, None);
            match block.evidence.fence_id.as_str() {
                "general" => {
                    assert_eq!(key.window_minutes, Some(10080));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "short" => {
                    assert_eq!(key.window_minutes, Some(300));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "reported" => {
                    assert_eq!(key.window_minutes, Some(90));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "family" => {
                    assert_eq!(key.window_minutes, None);
                    assert_eq!(key.scope, PoolScope::Family(ModelFamily::Opus));
                }
                "unknown" => {
                    assert_eq!(key.window_minutes, None);
                    assert_eq!(key.scope, PoolScope::Family(ModelFamily::Fable));
                }
                other => panic!("unexpected fence: {other}"),
            }
        }
        assert_eq!(blocks.len(), 6);
        for (id, expected) in [
            ("cm", AllowedAccountClass::PricedPerTokenOnly),
            ("fl", AllowedAccountClass::PricedPerTokenOnly),
            ("ma", AllowedAccountClass::NotPayPerToken),
            ("null", AllowedAccountClass::NotPayPerToken),
        ] {
            assert_eq!(
                restarted.decision_account_class(id).unwrap(),
                Some(expected)
            );
            assert_eq!(
                restarted
                    .v1_decision(id)
                    .unwrap()
                    .unwrap()
                    .allowed_account_class,
                expected
            );
        }
        assert_eq!(
            restarted
                .v1_affinity_decision("owner", "prefrontal-core.mason", "conversation")
                .unwrap()
                .unwrap()
                .decision_id,
            "ma"
        );
        assert_eq!(
            restarted.v1_report("ma").unwrap().unwrap().answer_json,
            "report answer"
        );
        let standing = restarted
            .v1_admission("runner", "admission")
            .unwrap()
            .unwrap();
        assert_eq!(standing.body_json, "original body");
        assert_eq!(standing.answer_json, "original answer");
        assert_eq!(
            restarted
                .v1_record_admission(
                    "runner",
                    "admission",
                    &V1Recorded {
                        body_json: "different body".into(),
                        answer_json: "different answer".into()
                    },
                    2_000_000
                )
                .unwrap(),
            standing
        );
        assert_eq!(restarted.account_serving_counts().unwrap().holds, 0);
        assert_eq!(
            restarted.credential_entitlement_fences().unwrap().len(),
            1,
            "credential probe eligibility is not evidence expiry"
        );
        restarted
            .store
            .with_conn(|c| {
                let (code,due):(String,i64)=c.query_row("SELECT json_extract(evidence,'$[0]'),next_recheck_at FROM block WHERE kind='credential' AND managed=1",[],|row|Ok((row.get(0)?,row.get(1)?)))?;
                assert_eq!(code,"ENTITLEMENT_ERROR","migration preserves the standing refusal evidence");
                assert!(due<=now_ms(),"old credentials get one immediately due probe");
                let dropped: i64 = c.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name='provider_reset_fences'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(dropped, 0);
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    0
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn migration_065_upgrades_a_deployed_064_database() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-migration-064");
        let descriptor = dev_descriptor(
            "routing-migration-064",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 64).unwrap();
        legacy
            .store
            .with_conn(|c| {
                insert_ledger_row(c, "deployed", None, 1_000, 2)?;
                c.execute(
                    "UPDATE model_route_decisions SET consumer='codemode' WHERE id='deployed'",
                    [],
                )
            })
            .unwrap();
        drop(legacy);
        let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            upgraded.decision_account_class("deployed").unwrap(),
            Some(striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly)
        );
    }

    #[test]
    fn migration_031_drops_a_populated_catalog_generation_table_through_the_store_runner() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-catalog-generation-migration-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "striatum-catalog-generation-migration",
            path.to_string_lossy(),
        );

        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..30])
            .unwrap();
        legacy
            .with_conn(|connection| {
                connection.execute(
                    "INSERT INTO catalog_generation(fingerprint,created_at) VALUES ('legacy',1)",
                    [],
                )
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let table_exists: bool = migrated
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT EXISTS(
                       SELECT 1 FROM sqlite_schema
                       WHERE type='table' AND name='catalog_generation'
                     )",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert!(!table_exists);
        drop(migrated);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_053_keeps_legacy_execution_identity_unknown() {
        let scratch = cortexkit_test_support::scratch_root().join(format!(
            "routing-selected-identity-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-selected-identity",
            scratch.join("store.db").to_string_lossy(),
        );
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..52])
            .unwrap();
        legacy.with_conn(|connection| {
            connection.execute(
                "INSERT INTO model_route_decisions
                 (id, harness, send_id, target_agent, requirements_json, selected_route_key,
                  selected_provider_id, selected_model_id, score_json, rejected_json, anchor_version, created_at)
                 VALUES ('legacy', 'pi', 'legacy-send', 'explore', '{}', 'provider/model',
                         'provider', 'model', '{}', '[]', 1, 1)",
                [],
            )
        }).unwrap();
        drop(legacy);

        for _ in 0..2 {
            let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
            let row = migrated.get_decision_by_id("legacy").unwrap().unwrap();
            assert_eq!(row.harness.as_deref(), Some("pi"));
            assert_eq!(row.selected_harness, None);
            assert_eq!(row.selected_workload, None);
            assert_eq!(row.selected_route_key.as_deref(), Some("provider/model"));
            assert_eq!(
                migrated.get_decision_by_send_id("legacy-send").unwrap(),
                Some(row)
            );
        }
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn migration_052_retires_the_shadow_index_and_the_purge_removes_only_shadow_rows() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-shadow-retirement-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor("striatum-shadow-retirement", path.to_string_lossy());

        // A store as the previous binary left it: shadow decisions from the
        // timer, observations and refused requests beside served history
        // (a pinned admit with its admission and affinity, and a refusal).
        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..51])
            .unwrap();
        legacy
            .with_conn(|connection| {
                let mut insert = connection.prepare(
                    "INSERT INTO account_decision (decision_id, mode, trigger, principal, provider, \
                       model, family, outcome, code, policy_version, decision_json, created_at) \
                     VALUES (?1, ?2, ?3, 'reserved:broca', ?4, 'claude-opus-5-5', 'opus', ?5, ?6, 2, '{}', ?7)",
                )?;
                for index in 0..5_i64 {
                    insert.execute(rusqlite::params![
                        format!("shadow-timer-{index}"), "shadow", "timer", "anthropic",
                        "admit", "cold_scored", 1_000 + index
                    ])?;
                }
                insert.execute(rusqlite::params![
                    "shadow-observation", "shadow", "observation", "xai", "refuse",
                    "account_selection_unsupported", 2_000
                ])?;
                insert.execute(rusqlite::params![
                    "shadow-request", "shadow", "request", "anthropic", "admit", "cold_scored", 3_000
                ])?;
                insert.execute(rusqlite::params![
                    "serve-admit", "serve", "request", "anthropic", "admit", "cold_scored", 1_500
                ])?;
                insert.execute(rusqlite::params![
                    "serve-refuse", "serve", "request", "anthropic", "refuse",
                    "account_quota_unknown", 2_500
                ])?;
                connection.execute_batch(
                    "INSERT INTO account_pin (pin_id, principal, conversation_hash, provider, \
                       pin_generation, affinity_model, account_id, credential_id, pin_mode, \
                       decision_id, created_at) \
                     VALUES ('pin-1', 'reserved:broca', 'h', 'anthropic', 1, 'claude-opus-5-5', \
                       'acct-a', 'oauth:a', 'run_frozen', 'serve-admit', 1500); \
                     INSERT INTO account_affinity (principal, conversation_hash, provider, \
                       affinity_model, pin_id, pin_generation, account_id, credential_id, \
                       credential_binding_revision, policy_version, initial_demand_bytes, \
                       created_at, last_used_at, expires_at, lease_valid_until) \
                     VALUES ('reserved:broca', 'h', 'anthropic', 'claude-opus-5-5', 'pin-1', 1, \
                       'acct-a', 'oauth:a', 1, 2, 10, 1500, 1500, 9000, 1620); \
                     INSERT INTO account_admission (principal, admission_id, request_fingerprint, \
                       request_mode, decision_id, conversation_hash, provider, model, account_id, \
                       credential_id, pin_id, pin_generation, state, charged, request_bytes, \
                       request_bytes_estimated, committed_at, admission_valid_until, \
                       lease_valid_until, reply_json) \
                     VALUES ('reserved:broca', 'adm-1', 'fp', 'admit', 'serve-admit', 'h', \
                       'anthropic', 'claude-opus-5-5', 'acct-a', 'oauth:a', 'pin-1', 1, \
                       'finished', 1, 10, 0, 1500, 1560, 1620, '{}');",
                )
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap();
        let indexes: Vec<String> = migrated
            .store
            .with_conn(|connection| {
                let mut statement = connection.prepare(
                    "SELECT name FROM sqlite_schema WHERE type = 'index' AND name IN \
                     ('account_decision_latest', 'account_admission_by_committed', \
                      'account_admission_by_pin', 'account_affinity_by_expiry', \
                      'account_hold_by_created') ORDER BY name",
                )?;
                let names = statement.query_map([], |row| row.get(0))?;
                names.collect()
            })
            .unwrap();
        assert_eq!(
            indexes,
            vec![
                "account_admission_by_committed",
                "account_admission_by_pin",
                "account_affinity_by_expiry",
                "account_hold_by_created",
            ],
            "the shadow heartbeat index is dropped and the retention indexes exist"
        );

        let mut purged = 0;
        loop {
            let removed = migrated.purge_shadow_decisions(2).unwrap();
            purged += removed;
            if removed < 2 {
                break;
            }
        }
        assert_eq!(purged, 7);
        let remaining = migrated
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT group_concat(decision_id, ','), \
                       (SELECT count(*) FROM account_pin), \
                       (SELECT count(*) FROM account_affinity), \
                       (SELECT count(*) FROM account_admission) \
                     FROM (SELECT decision_id FROM account_decision ORDER BY decision_id)",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
            })
            .unwrap();
        assert_eq!(remaining, ("serve-admit,serve-refuse".to_string(), 1, 1, 1));
        drop(migrated);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_035_normalizes_every_historical_family_spelling() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-family-migration-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor("striatum-family-migration", path.to_string_lossy());
        let fixtures = [
            (
                "anthropic/claude-opus-4-5",
                "anthropic",
                GrandFamily::Claude,
            ),
            ("openai/gpt-5.4", "openai", GrandFamily::Gpt),
            ("openai/gpt-5.5", "openai-gpt", GrandFamily::Gpt),
            (
                "google/antigravity-gemini-3.1-pro",
                "google-gemini",
                GrandFamily::Gemini,
            ),
            ("xai/grok-4.3", "xai-grok", GrandFamily::Grok),
        ];

        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..34])
            .unwrap();
        legacy
            .with_conn(|connection| {
                for (route_key, spelling, _) in fixtures {
                    connection.execute(
                        "UPDATE model_facts SET family = ?2 WHERE route_key = ?1",
                        params![route_key, spelling],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        for (route_key, _, expected) in fixtures {
            assert_eq!(
                migrated.get_model_family(route_key).unwrap(),
                Some((Some(expected), FamilySource::Curated))
            );
        }
        drop(migrated);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn migration_035_refuses_and_lists_unknown_family_rows() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-family-refusal-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor("striatum-family-refusal", path.to_string_lossy());
        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..34])
            .unwrap();
        legacy
            .with_conn(|connection| {
                connection.execute(
                    "UPDATE model_facts SET family = 'mystery-family' WHERE route_key = 'openai/gpt-5.4'",
                    [],
                )
            })
            .unwrap();
        drop(legacy);

        let error = match SqliteRouteStateStore::open(&descriptor) {
            Ok(_) => panic!("unknown family spelling was accepted"),
            Err(error) => error,
        };
        assert!(error.0.contains("openai/gpt-5.4=mystery-family"), "{error}");
        remove_observed_speed_test_store(&path);
    }

    fn history_foreign_keys(connection: &Connection) -> Vec<String> {
        let mut statement = connection
            .prepare(
                "SELECT s.name || ' -> ' || f.\"table\" \
                 FROM sqlite_schema AS s, pragma_foreign_key_list(s.name) AS f \
                 WHERE s.type = 'table' ORDER BY 1",
            )
            .unwrap();
        let rows = statement.query_map([], |row| row.get(0)).unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }

    /// The tables migration 58 rebuilds.
    const REBUILT_TABLES: [&str; 5] = [
        "account_pin",
        "account_admission",
        "account_hold",
        "account_fence",
        "account_principal_exclusion",
    ];

    /// Everything about the rebuilt tables a rebuild must keep: every column
    /// (name, type, NOT NULL, default, key position) and every index and
    /// trigger on them (name and SQL with whitespace collapsed; autoindexes,
    /// which have no SQL, by name).
    fn rebuilt_table_shape(connection: &Connection) -> Vec<String> {
        let mut shape = Vec::new();
        for table in REBUILT_TABLES {
            let mut columns = connection
                .prepare(
                    "SELECT name, type, \"notnull\", COALESCE(dflt_value, ''), pk \
                     FROM pragma_table_xinfo(?1) ORDER BY cid",
                )
                .unwrap();
            let rows = columns
                .query_map([table], |row| {
                    Ok(format!(
                        "{table} column {} {} notnull={} default={} pk={}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?
                    ))
                })
                .unwrap();
            shape.extend(rows.map(Result::unwrap));
            let mut objects = connection
                .prepare(
                    "SELECT type, name, COALESCE(sql, '') FROM sqlite_schema \
                     WHERE tbl_name = ?1 AND type IN ('index', 'trigger') ORDER BY type, name",
                )
                .unwrap();
            let rows = objects
                .query_map([table], |row| {
                    let sql: String = row.get(2)?;
                    Ok(format!(
                        "{table} {} {} {}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ))
                })
                .unwrap();
            shape.extend(rows.map(Result::unwrap));
        }
        shape
    }

    fn populate_account_tables(c: &Connection, now: i64) -> rusqlite::Result<()> {
        c.execute_batch(&format!(
            "INSERT INTO account_decision (decision_id, mode, trigger, principal, provider,
               model, family, outcome, code, policy_version, decision_json, created_at)
             VALUES ('dec-1', 'serve', 'request', 'p', 'anthropic', 'm', 'opus', 'admit',
               'cold_scored', 2, '{{}}', {now});
             INSERT INTO account_pin (pin_id, principal, conversation_hash, provider,
               pin_generation, affinity_model, account_id, credential_id, pin_mode,
               decision_id, created_at)
             VALUES ('pin-1', 'p', 'c', 'anthropic', 1, 'm', 'a', 'cred', 'run_frozen',
               'dec-1', {now});
             INSERT INTO account_admission (principal, admission_id, request_fingerprint,
               request_mode, decision_id, conversation_hash, provider, model, account_id,
               credential_id, pin_id, pin_generation, state, charged, request_bytes,
               request_bytes_estimated, committed_at, admission_valid_until,
               lease_valid_until, reply_json)
             VALUES ('p', 'adm-1', 'fp', 'admit', 'dec-1', 'c', 'anthropic', 'm', 'a',
               'cred', 'pin-1', 1, 'reserved', 1, 10, 0, {now}, {now}, {now}, '{{}}');
             INSERT INTO account_hold (decision_id, principal, conversation_hash, provider,
               pin_id, pin_generation, account_id, credential_id, basis, reset_at,
               retry_after_s, created_at)
             VALUES ('dec-1', 'p', 'c', 'anthropic', 'pin-1', 1, 'a', 'cred', 'five_hour',
               {now}, 60, {now});
             INSERT INTO account_observation (observation_id, principal, runner, event_id,
               kind, bound, observation_json, effects_json, received_at)
             VALUES ('aco-recent', 'p', 'broca', 'recent', 'attempt', 1, '{{}}',
               '[\"fence\"]', {recent}),
               ('aco-old', 'p', 'broca', 'old', 'attempt', 0, '{{}}', '[]', {old});
             INSERT INTO account_fence (fence_id, provider, account_id, scope_json,
               fence_json, source, observed_at, principal, observation_id, created_at)
             VALUES ('fence-1', 'anthropic', 'a', '{{}}', '{{}}', 'observation', {now},
               'p', 'aco-recent', {now});
             INSERT INTO account_principal_exclusion (principal, admission_id,
               credential_id, runner_view_digest, roster_view_digest, observation_id,
               recorded_at)
             VALUES ('p', 'adm-1', 'cred', 'r', 'v', 'aco-recent', {now});",
            recent = now - 60_000,
            old = now - 7 * 3_600_000,
        ))
    }

    fn admission_insert(id: &str, decision: &str, pin: &str) -> String {
        format!(
            "INSERT INTO account_admission (principal, admission_id, request_fingerprint,
               request_mode, decision_id, conversation_hash, provider, model, account_id,
               credential_id, pin_id, pin_generation, state, charged, request_bytes,
               request_bytes_estimated, committed_at, admission_valid_until,
               lease_valid_until, reply_json)
             VALUES ('p', '{id}', 'fp', 'admit', '{decision}', 'c', 'anthropic', 'm', 'a',
               'cred', '{pin}', 1, 'reserved', 1, 10, 0, 1, 1, 1, '{{}}')"
        )
    }

    fn route_state_version(c: &Connection) -> rusqlite::Result<i64> {
        c.query_row(
            "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace = ?1",
            [ROUTE_STATE_NAMESPACE],
            |row| row.get(0),
        )
    }

    #[test]
    fn retention_migration_093_has_exact_nonpartial_lookup_indexes() {
        let scratch = cortexkit_test_support::ScratchDir::new("pin-retention-indexes");
        let store = SqliteRouteStateStore::open_at_version_for_test(
            &dev_descriptor(
                "striatum",
                scratch.path().join("store.db").to_string_lossy(),
            ),
            97,
        )
        .unwrap();
        store
            .store
            .with_read(|conn| {
                for (name, table, column, definition) in [
                    (
                        "account_pin_by_created",
                        "account_pin",
                        "created_at",
                        "CREATE INDEX account_pin_by_created ON account_pin(created_at)",
                    ),
                    (
                        "account_hold_by_pin",
                        "account_hold",
                        "pin_id",
                        "CREATE INDEX account_hold_by_pin ON account_hold(pin_id)",
                    ),
                ] {
                    let actual: (String, String) = conn.query_row(
                        "SELECT tbl_name,sql FROM sqlite_schema WHERE type='index' AND name=?1",
                        [name],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    assert_eq!(actual, (table.to_string(), definition.to_string()));
                    let (unique, partial): (bool, bool) = conn.query_row(
                        "SELECT \"unique\",partial FROM pragma_index_list(?1) WHERE name=?2",
                        rusqlite::params![table, name],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    assert!(
                        !unique && !partial,
                        "{name} must be nonunique and nonpartial"
                    );
                    let mut statement = conn.prepare(
                        "SELECT name,desc,coll FROM pragma_index_xinfo(?1) WHERE key=1 ORDER BY seqno"
                    )?;
                    let terms = statement
                        .query_map([name], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, bool>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    assert_eq!(
                        terms,
                        vec![(column.to_string(), false, "BINARY".to_string())]
                    );
                }
                Ok(())
            })
            .unwrap();
    }

    /// A store at migration 55, with every account table populated while
    /// foreign keys are enforced, opens through migrations 56 and 58: the
    /// foreign keys into the two history logs are gone, the admission-to-pin
    /// key holds, every row, column, index and trigger of the rebuilt tables
    /// is kept, the integrity check passes, and migration 56 copied only the
    /// recent runner event's key.
    #[test]
    fn migration_058_rebuilds_the_account_tables_without_history_foreign_keys() {
        let scratch = cortexkit_test_support::scratch_root().join(format!(
            "routing-history-foreign-keys-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-history-foreign-keys",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..55])
            .unwrap();
        let (legacy_keys, legacy_shape) = legacy
            .with_conn(|c| {
                assert_eq!(route_state_version(c)?, 55);
                populate_account_tables(c, now)?;
                Ok((history_foreign_keys(c), rebuilt_table_shape(c)))
            })
            .unwrap();
        drop(legacy);
        assert_eq!(
            legacy_keys,
            [
                "account_admission -> account_decision",
                "account_admission -> account_pin",
                "account_fence -> account_observation",
                "account_hold -> account_decision",
                "account_pin -> account_decision",
                "account_principal_exclusion -> account_observation",
            ]
        );
        assert!(legacy_shape.iter().any(|line| line.contains(" index ")));

        let migrated = SqliteRouteStateStore::open_at_version_for_test(
            &descriptor,
            history::STATE_MIGRATION.version - 1,
        )
        .unwrap();
        migrated
            .store
            .with_conn(|c| {
                assert_eq!(
                    route_state_version(c)?,
                    route_state_migrations()
                        .iter()
                        .rev()
                        .find(|migration| migration.version < history::STATE_MIGRATION.version)
                        .unwrap()
                        .version as i64
                );
                assert_eq!(
                    history_foreign_keys(c),
                    [
                        "account_admission -> account_pin",
                        "routing_v1_report_delivery -> routing_v1_decision",
                    ]
                );
                // Later migrations add pool and admission-proof columns and
                // indexes. Their dedicated tests check those additive shapes;
                // the history-key removal must preserve the original columns.
                let demand_index = "account_admission index account_admission_demand ";
                let shape: Vec<_> = rebuilt_table_shape(c)
                    .into_iter()
                    .filter(|line| {
                        !line.starts_with("account_fence column pool_key_json ")
                            && !line.starts_with("account_fence column recovery_set_json ")
                            && !line.starts_with("account_admission column input_size_class ")
                            && !line.starts_with("account_admission column model_decision_id ")
                            && !line.starts_with("account_admission column last_observed_at ")
                            && !line.starts_with(
                                "account_admission index account_admission_model_decision ",
                            )
                            && !line.starts_with(
                                "account_admission index account_admission_by_conversation ",
                            )
                            && !line.starts_with(
                                "account_continue index account_continue_by_admission ",
                            )
                            && !line.starts_with("account_pin index account_pin_by_created ")
                            && !line.starts_with("account_hold index account_hold_by_pin ")
                            && !line.starts_with(demand_index)
                    })
                    .collect();
                let legacy_shape: Vec<_> = legacy_shape
                    .iter()
                    .filter(|line| !line.starts_with(demand_index))
                    .cloned()
                    .collect();
                assert_eq!(shape, legacy_shape);
                let integrity: String =
                    c.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
                assert_eq!(integrity, "ok");
                let leftovers: i64 = c.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE '%_rebuilt'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(leftovers, 0);
                for (table, rows) in [
                    ("account_decision", 1),
                    ("account_pin", 1),
                    ("account_admission", 1),
                    ("account_hold", 0),
                    ("account_observation", 2),
                    ("account_fence", 1),
                    ("account_principal_exclusion", 1),
                ] {
                    let count: i64 =
                        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                            row.get(0)
                        })?;
                    assert_eq!(count, rows, "{table}");
                }
                let keys: String = c.query_row(
                    "SELECT group_concat(event_id || '=' || observation_id || ':' || bound
                       || effects_json)
                     FROM account_observation_event",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(keys, "recent=aco-recent:1[\"fence\"]");
                // Enforcement is back on for serving, and now ignores the
                // history logs: an admission naming a decision that was never
                // recorded is accepted; one naming an unknown pin is refused.
                c.execute_batch(&admission_insert("adm-2", "dec-gone", "pin-1"))?;
                assert!(
                    c.execute_batch(&admission_insert("adm-3", "dec-1", "pin-missing"))
                        .is_err(),
                    "the pin foreign key must still hold"
                );
                Ok(())
            })
            .unwrap();
        drop(migrated);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    /// Migration 58 checks its own result: a store whose admission names a
    /// pin that does not exist fails the foreign-key check, migration 58
    /// rolls itself back, and the store stays at migration 57 with its tables
    /// and their foreign keys as they were before that rebuild.
    #[test]
    fn migration_058_refuses_a_store_with_a_dangling_pin_reference() {
        let scratch = cortexkit_test_support::scratch_root().join(format!(
            "routing-history-foreign-keys-dangling-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-history-foreign-keys-dangling",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..55])
            .unwrap();
        legacy
            .with_conn(|c| {
                populate_account_tables(c, now)?;
                c.pragma_update(None, "foreign_keys", "OFF")?;
                c.execute_batch(&admission_insert("adm-dangling", "dec-1", "pin-missing"))?;
                c.pragma_update(None, "foreign_keys", "ON")
            })
            .unwrap();
        drop(legacy);

        let refused = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96)
            .err()
            .expect("open refused");
        assert!(refused.0.contains("migration 58"), "{}", refused.0);
        let connection = Connection::open(scratch.join("store.db")).unwrap();
        assert_eq!(route_state_version(&connection).unwrap(), 57);
        assert_eq!(history_foreign_keys(&connection).len(), 6);
        let admissions: i64 = connection
            .query_row("SELECT COUNT(*) FROM account_admission", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(admissions, 2);
        drop(connection);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    pub(crate) fn observed_speed_test_store(
        label: &str,
    ) -> (std::path::PathBuf, SqliteRouteStateStore) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-observed-speed-{label}-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            &format!("striatum-observed-speed-{label}"),
            path.to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        (path, store)
    }

    pub(crate) fn remove_observed_speed_test_store(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }
}
