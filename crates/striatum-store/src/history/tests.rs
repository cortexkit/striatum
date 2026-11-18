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
    let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
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
fn memory_history_has_its_own_chain_and_is_not_shared_between_stores() {
    let first = SqliteRouteStateStore::open(&dev_descriptor("split-memory", ":memory:")).unwrap();
    let second =
        SqliteRouteStateStore::open(&dev_descriptor("split-memory-peer", ":memory:")).unwrap();
    let reading = quota("idle-window", NOW - QUOTA_PENDING_MS - 1);
    assert_eq!(first.append_quota_readings(&[reading.clone()]).unwrap(), 1);
    assert_eq!(first.move_history_batch(NOW).unwrap(), 1);
    assert_eq!(
        first.get_quota_readings(&reading.account_key).unwrap(),
        vec![reading.clone()]
    );
    assert_eq!(
        second.get_quota_readings(&reading.account_key).unwrap(),
        vec![]
    );
    first
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
}

#[test]
fn split_and_fresh_stores_have_the_same_history_schema_and_ledger() {
    let scratch = ScratchDir::new("split-schema");
    let path = scratch.path().join("offline.db");
    let fresh_path = scratch.path().join("fresh.db");
    let descriptor = dev_descriptor("split-offline-fixture", path.to_str().unwrap());
    let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    legacy
        .append_quota_readings(&[quota("fixture", NOW)])
        .unwrap();
    drop(legacy);
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
        let explain = format!("EXPLAIN QUERY PLAN {APPEND_QUOTA}");
        let mut plan = c.prepare(&explain)?;
        let bindings: Vec<Value> = (0..12).map(|_| Value::Null).collect();
        let steps = plan.query_map(rusqlite::params_from_iter(bindings), |r| r.get::<_,String>(3))?.collect::<Result<Vec<_>,_>>()?;
        assert!(steps.iter().any(|step| step.contains("SEARCH quota_reading_moved USING PRIMARY KEY (account_key=? AND window_key=?)")), "{steps:?}");
        assert!(!steps.iter().any(|step| step.contains("SCAN quota_reading_moved")), "{steps:?}");
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
