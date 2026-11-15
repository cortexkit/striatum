//! Rehearse the sparse observation-backoff index on an already cloned snapshot.

use rusqlite::{params, Connection, OpenFlags, StatementStatus};
use std::time::Instant;
use striatum_store::{dev_descriptor, SqliteRouteStateStore, OBSERVATION_BACKOFFS_SQL};

fn read_plan(conn: &Connection, now: i64, label: &str) -> Vec<String> {
    let cutoff = now.saturating_sub(24 * 60 * 60_000);
    let mut plan = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {OBSERVATION_BACKOFFS_SQL}"))
        .unwrap();
    let details: Vec<String> = plan
        .query_map(params![now, cutoff], |row| row.get(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    println!("{label} query plan: {}", details.join("; "));
    // Count the outer rows in the range the selected access path visits.
    // FullscanStep alone cannot count rows visited by an index range search.
    let count_sql = if details
        .iter()
        .any(|line| line.contains("account_observation_backoff_by_received"))
    {
        "SELECT count(*) FROM account_observation WHERE bound=1 AND kind='transient_backoff' AND received_at>?1"
    } else if details
        .iter()
        .any(|line| line.contains("account_observation_by_received"))
    {
        "SELECT count(*) FROM account_observation WHERE received_at>?1"
    } else {
        "SELECT count(*) FROM account_observation WHERE ?1 IS NOT NULL"
    };
    let visited: i64 = conn
        .query_row(count_sql, [cutoff], |row| row.get(0))
        .unwrap();
    let mut statement = conn.prepare(OBSERVATION_BACKOFFS_SQL).unwrap();
    let rows = statement
        .query_map(params![now, cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("{label} outer access-path rows: {visited}; answer rows: {}; fullscan steps: {}; VM steps: {}",
        rows.len(), statement.get_status(StatementStatus::FullscanStep), statement.get_status(StatementStatus::VmStep));
    details
}

fn main() {
    let path = std::env::args().nth(1).expect("cloned snapshot path");
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let now: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(received_at),0) FROM account_observation",
            [],
            |row| row.get(0),
        )
        .unwrap();
    read_plan(&conn, now, "before");
    drop(conn);
    let started = Instant::now();
    let store = SqliteRouteStateStore::open(&dev_descriptor("backoff-rehearsal", &path)).unwrap();
    println!("migration wall time: {:?}", started.elapsed());
    drop(store);
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let details = read_plan(&conn, now, "after");
    assert!(details
        .iter()
        .any(|line| line.contains("account_observation_backoff_by_received")));
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    println!("integrity_check: {integrity}");
    assert_eq!(integrity, "ok");
    let foreign_keys: i64 = conn
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    println!("foreign_key_check: {foreign_keys} violations");
    assert_eq!(foreign_keys, 0);
}
