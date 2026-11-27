use super::*;
use cortexkit_test_support::ScratchDir;
use striatum_core::RouteStateStore;

const NOW: i64 = 1_800_000_000_000;

fn open(path: &Path) -> SqliteRouteStateStore {
    SqliteRouteStateStore::open(&dev_descriptor("split-test", path.to_str().unwrap())).unwrap()
}

fn quota(window: &str, at: i64) -> QuotaReadingLogRow {
    QuotaReadingLogRow {
        account_key: "provider/account".into(),
        window_key: window.into(),
        observed_at: at,
        used_percent: 15.0,
        raw_used_percent: Some(14.8),
        resets_at: Some(at + 1000),
        window_minutes: Some(60),
        banked_credit_count: None,
        fetched_at: Some(at - 50),
        stale: None,
    }
}

#[test]
fn split_refuses_unsplit_before_writing_or_creating_history() {
    let scratch = ScratchDir::new("split-refusal");
    let path = scratch.path().join("state.db");
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap();
    assert_eq!(
        legacy
            .append_quota_readings(&[quota("baseline", NOW)])
            .unwrap(),
        1
    );
    drop(legacy);
    let before = std::fs::read(&path).unwrap();
    let error = SqliteRouteStateStore::open(&descriptor).err().unwrap();
    assert!(
        error.to_string().contains("script/split-state-history.sh"),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!scratch.path().join("history.db").exists());
}

#[test]
fn accountless_quota_reading_starts_a_separate_moved_watermark() {
    let scratch = ScratchDir::new("split-credential-watermark");
    let store = open(&scratch.path().join("store.db"));
    let at = NOW - QUOTA_PENDING_MS - 1;
    let entry: striatum_core::quota::ProviderUsage = serde_json::from_value(serde_json::json!({
        "provider":"claude", "apiProvider":"anthropic", "credentialRef":"oauth:listed-login",
        "usage":{"primary":{"usedPercent":7.0,"windowMinutes":300}}
    }))
    .unwrap();
    let mut rows =
        striatum_core::quota_poll::reading_log_rows_for_poll(&entry, "anthropic", &[], at - 1);
    assert_eq!(rows.len(), 1);
    let reading = rows.pop().unwrap();
    let mut legacy = reading.clone();
    legacy.account_key = "anthropic/".into();
    legacy.observed_at = at;
    assert_eq!(store.append_quota_readings(&[legacy.clone()]).unwrap(), 1);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);

    // Even an older sample starts a fresh series under the credential key.
    assert_eq!(store.append_quota_readings(&[reading.clone()]).unwrap(), 1);
    assert_eq!(reading.account_key, "anthropic/oauth:listed-login");
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(
        store
            .append_quota_readings(&[legacy.clone(), reading.clone()])
            .unwrap(),
        0
    );
    assert_eq!(store.move_history_batch(NOW).unwrap(), 0);
    assert_eq!(
        store.get_quota_readings(&legacy.account_key).unwrap(),
        vec![legacy.clone()]
    );
    assert_eq!(
        store.get_quota_readings(&reading.account_key).unwrap(),
        vec![reading.clone()]
    );
    store
        .with_history_reader(|conn| {
            for row in [&legacy, &reading] {
                let moved: i64 =
                    conn.query_row(QUOTA_WATERMARK, [&row.account_key, &row.window_key], |r| {
                        r.get(0)
                    })?;
                assert_eq!(moved, row.observed_at);
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn history_mover_crash_does_not_drop_or_duplicate_readings_or_advance_watermark() {
    let scratch = ScratchDir::new("split-crash");
    let path = scratch.path().join("state.db");
    let store = open(&path);
    let reading = quota("idle-window", NOW - QUOTA_PENDING_MS - 1);
    let mut retry = reading.clone();
    retry.used_percent = 99.0;
    let quota_count = |store: &SqliteRouteStateStore, schema: &str| {
        store
            .with_history_reader(|c| c.query_row(schema, [], |row| row.get::<_, i64>(0)))
            .unwrap()
    };
    assert_eq!(store.append_quota_readings(&[reading.clone()]).unwrap(), 1);
    assert_eq!(store.append_quota_readings(&[retry.clone()]).unwrap(), 0);
    assert_eq!(
        store.get_quota_readings(&reading.account_key).unwrap(),
        vec![reading.clone()]
    );
    assert_eq!(
        quota_count(&store, "SELECT count(*) FROM archive.quota_reading_log"),
        0
    );
    let crashed = store.move_history_batch_after_commit(NOW, || {
        assert_eq!(
            quota_count(&store, "SELECT count(*) FROM archive.quota_reading_log"),
            1
        );
        assert_eq!(
            quota_count(&store, "SELECT count(*) FROM pending_quota_reading_log"),
            1
        );
        assert_eq!(
            quota_count(&store, "SELECT count(*) FROM quota_reading_moved"),
            0
        );
        assert_eq!(
            store.get_quota_readings(&reading.account_key).unwrap(),
            vec![reading.clone()]
        );
        assert_eq!(
            quota_count(&store, "SELECT move_generation FROM history_layout"),
            0
        );
        assert_eq!(
            quota_count(
                &store,
                "SELECT move_generation FROM archive.history_binding"
            ),
            1
        );
        Err(err("stop before ack"))
    });
    assert!(crashed.unwrap_err().to_string().contains("stop before ack"));
    drop(store);
    let store = open(&path);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(
        quota_count(&store, "SELECT move_generation FROM history_layout"),
        2
    );
    assert_eq!(
        quota_count(
            &store,
            "SELECT move_generation FROM archive.history_binding"
        ),
        2
    );
    assert_eq!(
        quota_count(&store, "SELECT count(*) FROM pending_quota_reading_log"),
        0
    );
    assert_eq!(
        quota_count(&store, "SELECT count(*) FROM quota_reading_moved"),
        1
    );
    assert_eq!(
        store.get_quota_readings(&reading.account_key).unwrap(),
        vec![reading.clone()]
    );
    assert_eq!(store.append_quota_readings(&[retry]).unwrap(), 0);
    assert_eq!(
        store
            .append_quota_readings(&[quota("idle-window", reading.observed_at - 1)])
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .append_quota_readings(&[quota("idle-window", reading.observed_at + 1)])
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .append_quota_readings(&[quota("another-window", reading.observed_at)])
            .unwrap(),
        1
    );
    assert!(store.history_tables_in_state_empty_for_test().unwrap());
}

#[test]
fn memory_history_uses_scratch_leases_and_never_borrows_state_writer() {
    const CHILD: &str = "STRIATUM_MEMORY_HISTORY_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let store =
            SqliteRouteStateStore::open(&dev_descriptor("split-memory", ":memory:")).unwrap();
        let before = store.writer_acquisition_count_for_test();
        assert!(store
            .get_quota_readings("unused")
            .unwrap_err()
            .to_string()
            .contains("file-backed"));
        assert_eq!(store.writer_acquisition_count_for_test(), before);
        store
            .history
            .as_ref()
            .unwrap()
            .writer
            .with_conn(|conn| {
                assert_eq!(conn.path(), Some(""));
                assert_eq!(
                    conn.query_row(
                        "SELECT namespace,version FROM cortexkit_schema_version",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    )?,
                    ("route_history".into(), 1)
                );
                Ok(())
            })
            .unwrap();
        return;
    }
    let scratch = ScratchDir::new("split-memory-lease");
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let leases = |dir: &Path| {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "lease"))
            .collect::<std::collections::BTreeSet<_>>()
    };
    let preexisting = leases(crate_dir);
    let output = std::process::Command::new(crate::test_exe::current_test_exe().unwrap())
        .args([
            "--exact",
            "history::tests::memory_history_uses_scratch_leases_and_never_borrows_state_writer",
        ])
        .current_dir(scratch.path())
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!leases(scratch.path()).is_empty());
    assert_eq!(leases(crate_dir), preexisting);
}

// Unix-only: script/split-state-history.sh upgrades stores from before history moved into history.db, and no Windows install is that old.
#[cfg(unix)]
#[test]
fn split_and_fresh_stores_have_the_same_history_schema_and_ledger() {
    let scratch = ScratchDir::new("split-schema");
    let path = scratch.path().join("offline.db");
    let fresh_path = scratch.path().join("fresh.db");
    let descriptor = dev_descriptor("split-offline-fixture", path.to_str().unwrap());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap();
    old.append_quota_readings(&[quota("fixture", NOW)]).unwrap();
    let table_shapes = |conn: &Connection,
                        schema: &str|
     -> rusqlite::Result<Vec<(String, String, String)>> {
        let mut result = Vec::new();
        for table in TABLES.lines() {
            let mut sql = conn.prepare(&format!("SELECT type,name,sql FROM {schema}.sqlite_schema WHERE tbl_name=?1 AND name!='fence_retire_enforcement' AND sql IS NOT NULL ORDER BY type,name"))?;
            for row in sql.query_map([table], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })? {
                let (kind, name, ddl) = row?;
                result.push((
                    kind,
                    name,
                    ddl.split_whitespace().collect::<Vec<_>>().join(" "),
                ));
            }
        }
        Ok(result)
    };
    let before = old
        .with_reader_for_test(|conn| table_shapes(conn, "main"))
        .unwrap();
    drop(old);
    run_split_for_test(path.to_str().unwrap(), NOW);
    let split = open(&path);
    let schema = |store: &SqliteRouteStateStore| {
        store.with_history_reader(|conn| {
        let version: i64 = conn.query_row("SELECT version FROM archive.cortexkit_schema_version WHERE namespace='route_history'",[],|row|row.get(0))?;
        assert_eq!(version, 1);
        let mut statement = conn.prepare("SELECT type,name,coalesce(sql,'') FROM archive.sqlite_schema ORDER BY type,name")?;
        let rows = statement.query_map([], |r| Ok([r.get::<_,String>(0)?,r.get(1)?,r.get(2)?]))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
    }).unwrap()
    };
    let archived = split
        .with_history_reader(|conn| table_shapes(conn, "archive"))
        .unwrap();
    assert_eq!(
        archived, before,
        "history baseline preserves every v97 table, index and audit trigger definition"
    );
    let split_schema = schema(&split);
    drop(split);
    let fresh = open(&fresh_path);
    assert_eq!(schema(&fresh), split_schema);
}

#[test]
fn quota_outbox_rolls_back_with_state_and_watermark_lookup_is_keyed() {
    let scratch = ScratchDir::new("split-atomicity");
    let store = open(&scratch.path().join("store.db"));
    let reading = quota("atomic", NOW);
    store.with_writer_for_test(|c| {
        c.execute_batch("CREATE TEMP TRIGGER reject_quota_poll BEFORE INSERT ON main.account_quota_reading BEGIN SELECT RAISE(ABORT,'planted quota state failure'); END;")?;
        let plan = |sql| {
            let mut statement = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
            let steps = statement.query_map(["acct", "window"], |r| r.get::<_,String>(3))?;
            steps.collect::<Result<Vec<_>,_>>()
        };
        let keyed = |steps: &[String]| steps.iter().any(|step| step.contains("SEARCH quota_reading_moved USING PRIMARY KEY (account_key=? AND window_key=?)"))
            && !steps.iter().any(|step| step.contains("SCAN quota_reading_moved"));
        let steps = plan(QUOTA_WATERMARK)?;
        assert!(keyed(&steps), "{steps:?}");
        let near_miss = QUOTA_WATERMARK.replace("account_key=?1", "account_key||''=?1");
        assert!(!keyed(&plan(&near_miss)?));
        Ok(())
    }).unwrap();
    let poll = crate::PersistedAccountReading {
        provider: "p".into(),
        account_id: "a".into(),
        entry_json: "{}".into(),
        fetched_at: None,
        received_at: NOW,
        tombstoned_at: None,
    };
    assert!(store
        .apply_quota_delta(&[reading.clone()], &[poll], &[])
        .unwrap_err()
        .to_string()
        .contains("planted quota state failure"));
    assert!(store
        .get_quota_readings(&reading.account_key)
        .unwrap()
        .is_empty());
}

#[test]
fn history_reader_sees_one_row_when_move_commits_between_snapshot_pins() {
    let scratch = ScratchDir::new("split-reader-order");
    let store = open(&scratch.path().join("state.db"));
    let reading = quota("during-read", NOW - QUOTA_PENDING_MS - 1);
    assert_eq!(store.append_quota_readings(&[reading]).unwrap(), 1);
    let copies = store
        .with_history_reader_pins(
            || assert_eq!(store.move_history_batch(NOW).unwrap(), 1),
            |conn| {
                conn.query_row("SELECT count(*) FROM visible_quota_readings", [], |r| {
                    r.get::<_, i64>(0)
                })
            },
        )
        .unwrap();
    assert_eq!(
        copies, 1,
        "the row must remain visible through the copy and ack"
    );
}

#[test]
fn history_maintenance_never_leaves_an_archive_on_the_state_writer() {
    use crate::AccountCommandKind::{HistoryMaintenance, Selection};
    let scratch = ScratchDir::new("split-attach-scope");
    let store = open(&scratch.path().join("state.db"));
    let before = store.writer_hold_ns_for_test();
    let archive_count = |tx: &crate::AccountTx<'_>| {
        tx.conn
            .query_row(
                "SELECT count(*) FROM pragma_database_list WHERE name='archive'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(err)
    };
    for fail in [false, true] {
        let completed = store.account_command(HistoryMaintenance, move |tx| {
            assert_eq!(archive_count(tx)?, 1);
            if fail {
                tx.conn.execute_batch("ROLLBACK").map_err(err)?;
                return Err(err("planted failed transaction"));
            }
            Ok(())
        });
        assert_eq!(completed.is_err(), fail);
        assert_eq!(store.account_command(Selection, archive_count).unwrap(), 0);
    }
    println!(
        "history_maintenance_checked_writer_hold_ns={}",
        store.writer_hold_ns_for_test() - before
    );
    store
        .with_writer_for_test(|conn| conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)"))
        .unwrap();
    store.checkpoint_wal().unwrap();
}

#[test]
fn hold_in_history_still_protects_the_state_decision() {
    let scratch = ScratchDir::new("split-hold-reference");
    let store = open(&scratch.path().join("state.db"));
    store.with_writer_for_test(|conn| conn.execute_batch(
        "INSERT INTO account_decision (decision_id,principal,mode,trigger,provider,model,family,outcome,code,policy_version,decision_json,created_at)
           VALUES ('held','principal','serve','acct','provider','model','model','hold','account_hold',1,'{}',1);
         INSERT INTO pending_account_hold VALUES ('held','principal','conversation','provider','pin',1,'acct','cred','quota',100,60,1);"
    )).unwrap();
    assert_eq!(store.prune_serve_decisions(NOW, 10).unwrap(), 0);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(store.prune_serve_decisions(NOW, 10).unwrap(), 0);
    assert_eq!(store.prune_account_holds(NOW, 10).unwrap(), 1);
    assert_eq!(store.prune_serve_decisions(NOW, 10).unwrap(), 1);
}

#[test]
fn full_mover_batch_acknowledges_only_a_bounded_prefix() {
    let scratch = ScratchDir::new("split-mover-hold");
    let store = open(&scratch.path().join("state.db"));
    let readings = (0..(BATCH + 1))
        .map(|ix| quota("busy", NOW - 7_200_000 + ix as i64))
        .collect::<Vec<_>>();
    assert_eq!(store.append_quota_readings(&readings).unwrap(), BATCH + 1);
    let before = store.writer_hold_ns_for_test();
    assert_eq!(store.move_history_batch(NOW).unwrap(), BATCH);
    println!(
        "full_history_ack_rows={BATCH} writer_hold_ns={}",
        store.writer_hold_ns_for_test() - before
    );
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
}

#[test]
fn clean_split_drop_leaves_no_sidecars() {
    let scratch = ScratchDir::new("split-clean-drop");
    let store = open(&scratch.path().join("store.db"));
    let reading = quota("written", NOW - QUOTA_PENDING_MS - 1);
    assert_eq!(store.append_quota_readings(&[reading]).unwrap(), 1);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    drop(store);
    for name in [
        "store.db-wal",
        "store.db-shm",
        "history.db-wal",
        "history.db-shm",
    ] {
        assert!(
            !scratch.path().join(name).exists(),
            "clean shutdown left {name}"
        );
    }
}

#[test]
fn missing_archive_after_split_is_not_recreated() {
    let scratch = ScratchDir::new("split-missing-history");
    let path = scratch.path().join("store.db");
    let desc = dev_descriptor("split-missing-history", path.to_str().unwrap());
    drop(open(&path));
    let history = scratch.path().join("history.db");
    std::fs::rename(&history, scratch.path().join("old-history.db")).unwrap();
    let error = SqliteRouteStateStore::open(&desc)
        .err()
        .expect("must not start without prior history");
    assert!(error.to_string().contains("restore history.db"), "{error}");
    assert!(!history.exists());
}

// Unix-only: script/split-state-history.sh upgrades stores from before history moved into history.db, and no Windows install is that old.
#[cfg(unix)]
#[test]
fn fresh_and_split_state_have_identical_schema() {
    let scratch = ScratchDir::new("split-state-schema");
    let path = scratch.path().join("offline.db");
    let desc = dev_descriptor("split-state-schema", path.to_str().unwrap());
    drop(SqliteRouteStateStore::open_at_version_for_test(&desc, 97).unwrap());
    run_split_for_test(path.to_str().unwrap(), NOW);
    let migrated = open(&path);
    let schema = |store: &SqliteRouteStateStore| {
        store
            .with_reader_for_test(|c| {
                let mut q = c.prepare(
                    "SELECT type,name,coalesce(sql,'') FROM main.sqlite_schema ORDER BY type,name",
                )?;
                let rows =
                    q.query_map([], |r| Ok([r.get::<_, String>(0)?, r.get(1)?, r.get(2)?]))?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
    };
    let staged = schema(&migrated);
    drop(migrated);
    let fresh = open(&scratch.path().join("fresh.db"));
    assert_eq!(schema(&fresh), staged);
}

#[test]
fn only_attaching_maintenance_expires_request_statements() {
    let scratch = ScratchDir::new("split-statement-cache");
    let store = open(&scratch.path().join("store.db"));
    let read = "SELECT route_key FROM model_facts WHERE route_key = ?1";
    let request = move |tx: &crate::AccountTx<'_>| {
        assert_eq!(
            tx.conn
                .query_row(
                    "SELECT count(*) FROM pragma_database_list WHERE name='archive'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .map_err(err)?,
            0
        );
        let mut statement = tx.conn.prepare_cached(read).map_err(err)?;
        statement.reset_status(rusqlite::StatementStatus::RePrepare);
        statement
            .query_row(["absent"], |_| Ok(()))
            .optional()
            .map_err(err)?;
        Ok(statement.get_status(rusqlite::StatementStatus::RePrepare))
    };
    use crate::AccountCommandKind::{HistoryMaintenance, Maintenance, Selection};
    store.account_command(Selection, request).unwrap();
    for (kind, expected) in [(Maintenance, 0), (HistoryMaintenance, 1), (Maintenance, 0)] {
        store.account_command(kind, |_| Ok(())).unwrap();
        assert_eq!(store.account_command(Selection, request).unwrap(), expected);
    }
}

#[test]
fn interrupted_first_open_reuses_archive_and_completes_same_layout() {
    let scratch = ScratchDir::new("split-interrupted-open");
    let path = scratch.path().join("store.db");
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    drop(SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap());
    let still_unsplit = Connection::open(&path).unwrap();
    assert!(!has_split_layout(&still_unsplit).unwrap());
    drop(still_unsplit);
    drop(History::open(path.to_str().unwrap(), false, &descriptor.module_id).unwrap());
    assert!(scratch.path().join("history.db").exists());
    let store = open(&path);
    assert!(store.with_reader_for_test(has_split_layout).unwrap());
    assert!(store.history_tables_in_state_empty_for_test().unwrap());
    let schema = |store: &SqliteRouteStateStore| {
        store.with_history_reader(|c| {
        let mut statement = c.prepare("SELECT type,name,coalesce(sql,'') FROM main.sqlite_schema UNION ALL SELECT type,name,coalesce(sql,'') FROM archive.sqlite_schema ORDER BY 1,2,3")?;
        let rows = statement.query_map([], |r| Ok([r.get::<_,String>(0)?,r.get(1)?,r.get(2)?]))?;
        rows.collect::<Result<Vec<_>,_>>()
    }).unwrap()
    };
    let clean = ScratchDir::new("split-clean-schema");
    assert_eq!(
        schema(&store),
        schema(&open(&clean.path().join("store.db")))
    );
}

#[test]
fn unsplit_hot_rollback_journal_is_refused_without_modification() {
    let scratch = ScratchDir::new("split-hot-journal");
    let path = scratch.path().join("store.db");
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    drop(SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap());
    let sidecar = scratch.path().join("store.db-journal");
    std::fs::write(&sidecar, b"planted rollback journal").unwrap();
    let original = std::fs::read(&path).unwrap();
    let error = SqliteRouteStateStore::open(&descriptor)
        .err()
        .expect("hot journal must prevent opening");
    assert!(
        error.to_string().contains("refusing rollback journal"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(&sidecar).unwrap(),
        b"planted rollback journal"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(!scratch.path().join("history.db").exists());
}

#[test]
fn history_only_prune_does_not_acquire_state_writer_and_history_checkpoint_backfills() {
    let scratch = ScratchDir::new("split-history-only-prune");
    let store = open(&scratch.path().join("store.db"));
    let row = quota("old", NOW - 7_200_000);
    store.append_quota_readings(&[row]).unwrap();
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    let history = store.history.as_ref().unwrap();
    assert_eq!(
        history
            .writer
            .with_conn(
                |conn| conn.pragma_query_value(None, "journal_size_limit", |r| r.get::<_, u64>(0))
            )
            .unwrap(),
        WAL_SIZE_CAP_BYTES
    );
    let archive = scratch.path().join("history.db");
    let before = std::fs::read(&archive).unwrap();
    store.checkpoint_history_wal().unwrap();
    assert_ne!(std::fs::read(&archive).unwrap(), before);
    let writer = store.writer_acquisition_count_for_test();
    assert_eq!(store.prune_quota_reading_log(NOW, 256).unwrap(), 1);
    assert_eq!(store.writer_acquisition_count_for_test(), writer);
}

#[test]
fn history_prune_crash_after_archive_delete_keeps_pending_replay_guard() {
    let scratch = ScratchDir::new("split-prune-crash");
    let path = scratch.path().join("store.db");
    let store = open(&path);
    let row = quota("prune-crash", NOW - 7_200_000);
    store.append_quota_readings(&[row.clone()]).unwrap();
    store
        .move_history_batch_after_commit(NOW, || Err(err("planted pre-ack crash")))
        .unwrap_err();
    store
        .prune_history_after_archive_delete(Kind::Quota, NOW, 256, || {
            Err(err("planted prune crash"))
        })
        .unwrap_err();
    drop(store);
    let store = open(&path);
    assert_eq!(
        store.get_quota_readings(&row.account_key).unwrap(),
        vec![row.clone()]
    );
    assert_eq!(store.append_quota_readings(&[row.clone()]).unwrap(), 0);
    assert_eq!(store.prune_quota_reading_log(NOW, 256).unwrap(), 1);
    assert!(store
        .get_quota_readings(&row.account_key)
        .unwrap()
        .is_empty());
}

#[test]
fn hold_pruning_merges_both_created_indexes_without_sorting() {
    let scratch = ScratchDir::new("split-hold-plan");
    let store = open(&scratch.path().join("store.db"));
    store.with_history_reader(|conn| {
        let plan = |sql: &str| {
            let mut s = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
            let rows = s.query_map([NOW,64],|r|r.get::<_,String>(3))?;
            rows.collect::<Result<Vec<_>,_>>()
        };
        let merges = |rows: &[String]| rows.iter().any(|row| row.contains("MERGE (UNION ALL)"))
            && ["pending_account_hold_by_created","account_hold_by_created"].iter().all(|index| rows.iter().any(|row| row.split_whitespace().any(|word| word == *index)))
            && rows.iter().all(|row| !row.contains("TEMP B-TREE"));
        let actual = plan(HOLD_PRUNE_KEYS)?;
        assert!(merges(&actual),"{actual:?}");
        assert!(!merges(&plan("SELECT decision_id FROM visible_account_holds WHERE created_at<?1 ORDER BY created_at LIMIT ?2")?));
        Ok(())
    }).unwrap();
}

#[test]
fn history_trace_retains_writer_delete_authorizer() {
    let scratch = ScratchDir::new("history-authorizer");
    let store = open(&scratch.path().join("store.db"));
    store.with_writer_for_test(|conn| conn.execute_batch(
        "INSERT INTO account_observation_event (principal,runner,event_id,observation_id,received_at,bound,effects_json)
         VALUES ('p','r','a','x',1,0,'[]'),('p','r','b','y',1,0,'[]')",
    )).unwrap();
    store.watch_history_reads_for_test().unwrap();
    store.stop_watching_history_reads_for_test().unwrap();
    let before = store.outside_account_writer_count_for_test();
    store
        .with_writer_for_test(|conn| conn.execute_batch("DELETE FROM account_observation_event"))
        .unwrap();
    assert_eq!(store.outside_account_writer_count_for_test() - before, 2);
}

#[test]
fn rejects_a_stale_archive_but_accepts_a_newer_archive_from_the_same_store() {
    let scratch = ScratchDir::new("archive-generation");
    let path = scratch.path().join("store.db");
    let archive = scratch.path().join("history.db");
    let older_state = scratch.path().join("older-state");
    let older_archive = scratch.path().join("older-archive");
    let current_archive = scratch.path().join("current-archive");
    let store = open(&path);
    store
        .append_quota_readings(&[quota("first", NOW - QUOTA_PENDING_MS - 2)])
        .unwrap();
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    drop(store);
    std::fs::copy(&path, &older_state).unwrap();
    std::fs::copy(&archive, &older_archive).unwrap();
    let store = open(&path);
    store
        .append_quota_readings(&[quota("second", NOW - QUOTA_PENDING_MS - 1)])
        .unwrap();
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    drop(store);
    std::fs::copy(&archive, &current_archive).unwrap();
    std::fs::copy(&older_archive, &archive).unwrap();
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    let error = SqliteRouteStateStore::open(&descriptor)
        .err()
        .expect("stale archive must be refused");
    assert!(
        error
            .to_string()
            .contains("state generation 2, history generation 1"),
        "{error}"
    );
    std::fs::copy(&older_state, &path).unwrap();
    std::fs::copy(&current_archive, &archive).unwrap();
    assert_eq!(
        open(&path)
            .get_quota_readings("provider/account")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn refuses_orphaned_archive_with_rows_before_creating_state() {
    let scratch = ScratchDir::new("archive-no-state");
    let path = scratch.path().join("store.db");
    let store = open(&path);
    store
        .append_quota_readings(&[quota("first", NOW - QUOTA_PENDING_MS - 1)])
        .unwrap();
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    drop(store);
    std::fs::rename(&path, scratch.path().join("lost-state")).unwrap();
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    let error = SqliteRouteStateStore::open(&descriptor)
        .err()
        .expect("fresh state must not adopt data");
    let error = error.to_string();
    assert!(
        error.contains("history.db") && error.contains("state identity none, history identity "),
        "{error}"
    );
    assert!(!path.exists());
}

#[test]
fn empty_legacy_archive_tables_are_rechecked_after_migrating_through_97() {
    let scratch = ScratchDir::new("split-recheck-81");
    let path = scratch.path().join("store.db");
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
    old.with_writer_for_test(|conn| {
        assert_eq!(conn.execute("UPDATE model_route_state SET failure_backoff_until=?1 WHERE route_key=(SELECT min(route_key) FROM model_route_state)", [crate::now_ms()+3_600_000])?, 1);
        Ok(())
    }).unwrap();
    drop(old);
    let error = SqliteRouteStateStore::open(&descriptor)
        .err()
        .expect("migrated frozen evidence cannot be dropped");
    assert!(
        error
            .to_string()
            .contains("unsplit state database has rows in fence_recheck"),
        "{error}"
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap();
    old.with_reader_for_test(|conn| {
        assert_eq!(
            conn.query_row("SELECT count(*) FROM fence_recheck", [], |r| r
                .get::<_, i64>(0))?,
            1
        );
        Ok(())
    })
    .unwrap();
    assert!(!scratch.path().join("history.db").exists());
}

#[test]
fn refuses_recorded_098_without_a_complete_layout() {
    for ddl in ["", "CREATE TABLE history_layout(singleton INTEGER PRIMARY KEY); INSERT INTO history_layout VALUES(1);"] {
        let scratch = ScratchDir::new("old-098-layout");
        let path = scratch.path().join("store.db");
        let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap();
        old.with_writer_for_test(|conn| {
            conn.execute_batch(ddl)?;
            conn.execute("INSERT INTO cortexkit_schema_version VALUES ('route_state',98,0)", [])?;
            Ok(())
        }).unwrap();
        drop(old);
        let error = SqliteRouteStateStore::open(&descriptor).err().expect("old on-disk 098 shape must be refused");
        let error = error.to_string();
        assert!(error.contains("store.db") && error.contains("incomplete history_layout"), "{error}");
        assert!(!scratch.path().join("history.db").exists());
    }
}

#[test]
fn shutdown_history_checkpoint_uses_the_state_wait_bound() {
    let scratch = ScratchDir::new("history-close-timeout");
    let mut store = open(&scratch.path().join("store.db"));
    let outsider = Connection::open(scratch.path().join("history.db")).unwrap();
    outsider
        .execute_batch("BEGIN; SELECT * FROM history_binding;")
        .unwrap();
    assert_eq!(
        store
            .append_quota_readings(&[quota("shutdown", NOW - QUOTA_PENDING_MS - 1)])
            .unwrap(),
        1
    );
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    let history = store.history.as_mut().unwrap();
    history.close_reader_and_checkpoint();
    let timeout: u64 = history
        .writer
        .with_conn(|c| c.pragma_query_value(None, "busy_timeout", |r| r.get(0)))
        .unwrap();
    assert_eq!(timeout, WAL_TRUNCATE_WAIT_BOUND.as_millis() as u64);
}

#[test]
fn history_copy_uses_full_sync_while_state_ack_uses_normal() {
    let scratch = ScratchDir::new("split-durability");
    let store = open(&scratch.path().join("store.db"));
    let sync = |c: &Connection| c.pragma_query_value(None, "synchronous", |r| r.get::<_, i64>(0));
    assert_eq!(store.with_writer_for_test(sync).unwrap(), 1); // NORMAL
    assert_eq!(
        store
            .history
            .as_ref()
            .unwrap()
            .writer
            .with_conn(sync)
            .unwrap(),
        2
    ); // FULL
}

#[test]
fn empty_archive_bound_before_state_is_retryable() {
    for existing_state in [false, true] {
        let scratch = ScratchDir::new("split-bound-before-state");
        let path = scratch.path().join("store.db");
        let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
        if existing_state {
            drop(SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97).unwrap());
        }
        let archive_path = scratch.path().join("history.db");
        let archive = History::open(path.to_str().unwrap(), false, &descriptor.module_id).unwrap();
        let old_id = archive.bind(&archive_path, None).unwrap();
        drop(archive);
        let store = open(&path);
        let pair = store.with_history_reader(|c| c.query_row(
            "SELECT s.split_identity, h.split_identity, s.move_generation, h.move_generation
             FROM main.history_layout s CROSS JOIN archive.history_binding h",
            [], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?)),
        )).unwrap();
        assert_eq!(pair.0, pair.1);
        assert_ne!(pair.0, old_id);
        assert_eq!((pair.2, pair.3), (0, 0));
    }
}

#[test]
fn refuses_a_different_archive_or_one_without_binding() {
    let scratch = ScratchDir::new("split-mismatched-pair");
    let path = scratch.path().join("store.db");
    let store = open(&path);
    let real_id = store
        .with_reader_for_test(|c| {
            c.query_row("SELECT split_identity FROM history_layout", [], |r| {
                r.get::<_, String>(0)
            })
        })
        .unwrap();
    drop(store);
    let history = scratch.path().join("history.db");
    let descriptor = dev_descriptor("split-test", path.to_str().unwrap());
    for (sql, expected) in [
        (
            "UPDATE history_binding SET split_identity='0123456789abcdef0123456789abcdef'",
            "0123456789abcdef0123456789abcdef",
        ),
        ("DELETE FROM history_binding", "none"),
    ] {
        Connection::open(&history)
            .unwrap()
            .execute(sql, [])
            .unwrap();
        let error = SqliteRouteStateStore::open(&descriptor)
            .err()
            .expect("mismatched pair must be refused");
        assert!(
            error.to_string().contains(&format!(
                "state identity {real_id}, history identity {expected}"
            )),
            "{error}"
        );
    }
    Connection::open(&history)
        .unwrap()
        .execute("DROP TABLE history_binding", [])
        .unwrap();
    let error = SqliteRouteStateStore::open(&descriptor)
        .err()
        .expect("old archive schema must be refused");
    assert!(
        error
            .to_string()
            .contains("history.db: route_history version 1 has incomplete history_binding"),
        "{error}"
    );
}

#[test]
fn archive_with_no_rows_but_prior_moves_is_not_adopted() {
    let scratch = ScratchDir::new("archive-empty-after-moves");
    let path = scratch.path().join("store.db");
    let store = open(&path);
    let reading = quota("first", NOW - QUOTA_PENDING_MS - 1);
    store.append_quota_readings(&[reading]).unwrap();
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(store.prune_quota_reading_log(NOW, 1).unwrap(), 1);
    drop(store);
    std::fs::rename(&path, scratch.path().join("lost-state")).unwrap();
    let desc = dev_descriptor("split-test", path.to_str().unwrap());
    assert!(SqliteRouteStateStore::open(&desc)
        .err()
        .unwrap()
        .to_string()
        .contains("state identity none, history identity"));
    assert!(!path.exists());
}

#[test]
fn state_first_backup_replays_a_move_included_in_both_captures() {
    let scratch = ScratchDir::new("state-first-backup");
    let restore = ScratchDir::new("state-first-restore");
    let path = scratch.path().join("store.db");
    let store = open(&path);
    let reading = quota("copied-during-capture", NOW - QUOTA_PENDING_MS - 1);
    assert_eq!(store.append_quota_readings(&[reading.clone()]).unwrap(), 1);
    drop(store);
    let restored_path = restore.path().join("store.db");
    std::fs::copy(&path, &restored_path).unwrap();
    let store = open(&path);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
    drop(store);
    std::fs::copy(
        scratch.path().join("history.db"),
        restore.path().join("history.db"),
    )
    .unwrap();
    let restored = open(&restored_path);
    assert_eq!(
        restored.get_quota_readings(&reading.account_key).unwrap(),
        vec![reading.clone()]
    );
    assert_eq!(
        restored.append_quota_readings(&[reading.clone()]).unwrap(),
        0
    );
    assert_eq!(restored.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(restored.append_quota_readings(&[reading]).unwrap(), 0);
}
