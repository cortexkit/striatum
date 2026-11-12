//! Rehearse only the observation-index SQL on a disposable store.
//!
//! Usage: `observation_event_index_rehearsal --fixture WORK_DIR [ROWS]`, or
//! `observation_event_index_rehearsal --copy SNAPSHOT_DB WORK_DIR`.
//! WORK_DIR must not exist. SNAPSHOT_DB must be an operator-created, offline
//! copy, never a live store. It is opened read-only and cloned with VACUUM INTO
//! so its WAL is included. The source is never migrated or otherwise written.
//! The result is for measurement only: applying the SQL here deliberately does
//! not advance migration metadata. Normal store opens apply registered migrations.

use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

const MIGRATION: &str = include_str!("../migrations/085_observation_principal_event.sql");
const LOOKUP: &str = "SELECT runner FROM account_observation WHERE principal=?1 AND event_id=?2 \
    UNION ALL SELECT runner FROM account_observation_event WHERE principal=?1 AND event_id=?2 LIMIT 1";
const TABLES: [&str; 2] = ["account_observation", "account_observation_event"];
const PRINCIPALS: [&str; 2] = ["reserved:broca", "reserved:thalamus"];

fn fixture(path: &Path, rows: usize) {
    drop(
        SqliteRouteStateStore::open(&dev_descriptor(
            "observation-index-fixture",
            path.to_string_lossy(),
        ))
        .unwrap(),
    );
    let mut conn = Connection::open(path).unwrap();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    // A fresh store already has the indexes. Remove only those two from this
    // disposable fixture so the same SQL can be measured before and after.
    for table in TABLES {
        tx.execute_batch(&format!("DROP INDEX {table}_by_principal_event"))
            .unwrap();
    }
    tx.execute(
        "WITH RECURSIVE n(i) AS (SELECT 0 WHERE ?1>0 UNION ALL SELECT i+1 FROM n WHERE i+1<?1) \
         INSERT INTO account_observation \
         (observation_id,principal,runner,event_id,kind,bound,observation_json,effects_json,received_at) \
         SELECT 'history-'||i, CASE WHEN i%2=0 THEN ?2 ELSE ?3 END, \
         'runner-'||(i%4), 'history-event-'||i, 'advisory',0,'{}','[]',i FROM n",
        params![i64::try_from(rows).unwrap(), PRINCIPALS[0], PRINCIPALS[1]],
    )
    .unwrap();
    tx.commit().unwrap();
    println!("synthetic fixture: {rows} observations per table, two principals, four runners");
}

fn counts(conn: &Connection) -> [i64; 2] {
    TABLES.map(|table| {
        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    })
}

fn plans(conn: &Connection, label: &str) -> Vec<String> {
    let details = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {LOOKUP}"))
        .unwrap()
        .query_map(params![PRINCIPALS[0], "new-event"], |row| row.get(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<String>>>()
        .unwrap();
    println!("{label} query plan: {}", details.join("; "));
    details
}

fn lookup_times(conn: &Connection, label: &str) {
    let mut statement = conn.prepare(LOOKUP).unwrap();
    for principal in PRINCIPALS {
        let mut times = Vec::with_capacity(1000);
        for i in 0..1010 {
            let started = Instant::now();
            let found: Option<String> = statement
                .query_row(params![principal, "new-event"], |row| row.get(0))
                .optional()
                .unwrap();
            assert!(found.is_none());
            if i >= 10 {
                times.push(started.elapsed().as_secs_f64() * 1_000_000.0);
            }
        }
        times.sort_by(f64::total_cmp);
        println!(
            "{label} missing-event lookup {principal}: n={} mean_us={:.3} p50_us={:.3} p99_us={:.3}",
            times.len(),
            times.iter().sum::<f64>() / times.len() as f64,
            times[499],
            times[989]
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        (args.len() == 3 || args.len() == 4) && args[1] == "--fixture"
            || args.len() == 4 && args[1] == "--copy",
        "usage: observation_event_index_rehearsal --fixture WORK_DIR [ROWS] | --copy SNAPSHOT_DB WORK_DIR"
    );
    let work = PathBuf::from(if args[1] == "--fixture" {
        &args[2]
    } else {
        &args[3]
    });
    assert!(!work.exists(), "WORK_DIR must be new");
    std::fs::create_dir_all(&work).unwrap();
    let database = work.join("store.db");
    if args[1] == "--fixture" {
        let rows = args.get(3).map(|n| n.parse().unwrap()).unwrap_or(300_000);
        fixture(&database, rows);
    } else {
        let source = Connection::open_with_flags(&args[2], OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open offline snapshot read-only");
        source
            .execute("VACUUM INTO ?1", [database.to_string_lossy().as_ref()])
            .expect("clone the offline snapshot, including its WAL");
    }
    let mut conn = Connection::open(&database).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let before_bytes = std::fs::metadata(&database).unwrap().len();
    let before_counts = counts(&conn);
    plans(&conn, "before");
    lookup_times(&conn, "before");
    let started = Instant::now();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    tx.execute_batch(MIGRATION).unwrap();
    tx.commit().unwrap();
    println!("index migration duration: {:?}", started.elapsed());
    let started = Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    println!(
        "post-migration checkpoint duration: {:?}",
        started.elapsed()
    );
    let after_bytes = std::fs::metadata(&database).unwrap().len();
    println!(
        "store bytes: before={before_bytes} after={after_bytes} change={}",
        i128::from(after_bytes) - i128::from(before_bytes)
    );
    let after_counts = counts(&conn);
    for (i, table) in TABLES.iter().enumerate() {
        println!(
            "row-count parity {table}: before={} after={} equal={}",
            before_counts[i],
            after_counts[i],
            before_counts[i] == after_counts[i]
        );
    }
    assert_eq!(before_counts, after_counts);
    let details = plans(&conn, "after");
    for table in TABLES {
        assert!(details.iter().any(|detail| detail.contains(&format!(
            "SEARCH {table} USING COVERING INDEX {table}_by_principal_event (principal=? AND event_id=?)"
        ))));
    }
    lookup_times(&conn, "after");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    println!("integrity_check: {integrity}");
    assert_eq!(integrity, "ok");
}
