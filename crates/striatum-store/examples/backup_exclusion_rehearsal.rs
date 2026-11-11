//! Rehearse a backup that skips the routing history tables, on a copy.
//!
//! Usage: `backup_exclusion_rehearsal SOURCE_DB WORK_DIR`. SOURCE_DB (a
//! `VACUUM INTO` copy of a live routing store) is cloned twice into WORK_DIR,
//! which must not exist; the source itself is never written. On the first
//! clone it times a normal store open (migrations, including dropping the
//! foreign keys into the history logs) and runs the integrity check. The
//! second clone is that upgraded store with every table
//! `backup::BACKUP_EXCLUDABLE_TABLES` names emptied the way a restore that
//! skipped them leaves them (no trigger runs); it checks the foreign keys,
//! reopens the store and exercises the reads and writes that used to depend
//! on the emptied tables.
use std::path::PathBuf;
use std::time::Instant;

use striatum_core::{
    DecisionOutcomeUpdate, FailureBackoffConfig, RouteOutcome, RouteStateStore,
};
use striatum_store::backup::{BACKUP_EXCLUDABLE_TABLES, BACKUP_REQUIRED_TABLES};
use striatum_store::{dev_descriptor, SqliteRouteStateStore};
use rusqlite::Connection;

fn open(path: &std::path::Path) -> SqliteRouteStateStore {
    SqliteRouteStateStore::open(&dev_descriptor(
        "striatum",
        path.to_string_lossy().into_owned(),
    ))
    .expect("open route-state store")
}

fn scalar<T: rusqlite::types::FromSql>(conn: &Connection, sql: &str) -> T {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn table_bytes(conn: &Connection, table: &str) -> i64 {
    scalar(
        conn,
        &format!(
            "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE aggregate = 0 AND name IN \
             (SELECT name FROM sqlite_schema WHERE tbl_name = '{table}')"
        ),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: backup_exclusion_rehearsal SOURCE_DB WORK_DIR"
    );
    let source = PathBuf::from(&args[1]).canonicalize().unwrap();
    let work = PathBuf::from(&args[2]);
    assert!(!work.exists(), "WORK_DIR must be new");
    std::fs::create_dir_all(work.join("upgraded")).unwrap();
    std::fs::create_dir_all(work.join("restored")).unwrap();
    let upgraded = work.join("upgraded/store.db");
    let restored = work.join("restored/store.db");
    std::fs::copy(&source, &upgraded).unwrap();

    {
        let conn = Connection::open(&upgraded).unwrap();
        let version: i64 = scalar(
            &conn,
            "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace = 'route_state'",
        );
        println!("source route_state version: {version}");
    }
    let started = Instant::now();
    drop(open(&upgraded));
    println!(
        "open applying the pending migrations (through 58): {:?}",
        started.elapsed()
    );

    let conn = Connection::open(&upgraded).unwrap();
    let started = Instant::now();
    let integrity: String = scalar(&conn, "PRAGMA integrity_check");
    println!("integrity_check: {integrity} ({:?})", started.elapsed());
    assert_eq!(integrity, "ok");
    let foreign_keys: Vec<String> = conn
        .prepare(
            "SELECT s.name || ' -> ' || f.\"table\" FROM sqlite_schema AS s, \
             pragma_foreign_key_list(s.name) AS f WHERE s.type = 'table' ORDER BY 1",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    println!("foreign keys after open: {foreign_keys:?}");
    let mut kept_bytes = 0;
    let mut skipped_bytes = 0;
    for (table, _) in BACKUP_EXCLUDABLE_TABLES {
        let bytes = table_bytes(&conn, table);
        skipped_bytes += bytes;
        println!("  skippable {table}: {bytes} bytes");
    }
    for (table, _) in BACKUP_REQUIRED_TABLES {
        let bytes = table_bytes(&conn, table);
        kept_bytes += bytes;
        let rows: i64 = scalar(&conn, &format!("SELECT COUNT(*) FROM {table}"));
        println!("  kept {table}: {rows} rows, {bytes} bytes");
    }
    println!("skippable total {skipped_bytes} bytes; kept total {kept_bytes} bytes");
    let head: Option<String> = conn
        .query_row(
            "SELECT parent_session_id FROM model_route_parent_failures \
             ORDER BY outcome_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();
    let in_flight: Option<String> = conn
        .query_row(
            "SELECT id FROM model_route_decisions INDEXED BY model_route_decisions_created_at_idx \
             WHERE outcome IS NULL AND selected_route_key IS NOT NULL \
             ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();
    let event: Option<(String, String, String, String)> = conn
        .query_row(
            "SELECT principal, runner, event_id, observation_id FROM account_observation_event \
             ORDER BY received_at DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .ok();
    drop(conn);

    std::fs::copy(&upgraded, &restored).unwrap();
    let started = Instant::now();
    {
        let mut conn = Connection::open(&restored).unwrap();
        conn.pragma_update(None, "foreign_keys", false).unwrap();
        let tx = conn.transaction().unwrap();
        for (table, _) in BACKUP_EXCLUDABLE_TABLES {
            let triggers: Vec<(String, String)> = tx
                .prepare(
                    "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ?1",
                )
                .unwrap()
                .query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            for (name, _) in &triggers {
                tx.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
            }
            tx.execute(&format!("DELETE FROM {table}"), []).unwrap();
            for (_, sql) in &triggers {
                tx.execute_batch(sql).unwrap();
            }
        }
        tx.commit().unwrap();
        let violations: i64 = scalar(&conn, "SELECT COUNT(*) FROM pragma_foreign_key_check");
        println!(
            "skippable tables emptied in {:?}; foreign_key_check violations: {violations}",
            started.elapsed()
        );
        assert_eq!(violations, 0);
    }

    let started = Instant::now();
    let store = open(&restored);
    println!("open of the restored store: {:?}", started.elapsed());
    if let Some(head) = head {
        let failed = store.recent_failed_decisions_for_parent(&head, 0).unwrap();
        println!(
            "head {head}: {} failed decisions still visible",
            failed.len()
        );
        assert!(!failed.is_empty());
    }
    if let Some(id) = in_flight {
        store
            .set_decision_outcome(
                &id,
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: RouteOutcome::Completed,
                    last_error: None,
                    observed_at_ms: 0,
                    failure_backoff: FailureBackoffConfig::default(),
                },
            )
            .expect("an outcome for a lost decision is accepted");
        println!("outcome for lost decision {id}: accepted, no change");
    }
    if let Some((principal, runner, event_id, observation_id)) = event {
        let seen = store
            .account_transaction(|tx| tx.observation_by_event(&principal, &runner, &event_id))
            .unwrap()
            .expect("recent event key survives");
        assert_eq!(seen.observation_id, observation_id);
        println!("re-sent event {event_id}: recognised as {observation_id}");
    }
    println!("rehearsal ok");
}
