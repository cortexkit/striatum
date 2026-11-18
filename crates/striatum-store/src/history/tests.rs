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
        Err(err("stop before ack"))
    });
    assert!(crashed.unwrap_err().to_string().contains("stop before ack"));
    drop(store);
    let store = open(&path);
    assert_eq!(store.move_history_batch(NOW).unwrap(), 1);
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
        "INSERT INTO account_observation_event (principal,runner,event_id,observation_id,received_at)
         VALUES ('p','r','a','x',1),('p','r','b','y',1)",
    )).unwrap();
    store.watch_history_reads_for_test().unwrap();
    store.stop_watching_history_reads_for_test().unwrap();
    let before = store.outside_account_writer_count_for_test();
    store
        .with_writer_for_test(|conn| conn.execute_batch("DELETE FROM account_observation_event"))
        .unwrap();
    assert_eq!(store.outside_account_writer_count_for_test() - before, 2);
}
