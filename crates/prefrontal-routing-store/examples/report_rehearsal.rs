//! Apply routing migrations to an already cloned live-store snapshot, never the live DB.

use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
use std::{
    path::Path,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

fn clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
fn wal_bytes(path: &str) -> u64 {
    std::fs::metadata(format!("{path}-wal")).map_or(0, |file| file.len())
}

fn schedules(path: &str) -> Vec<(String, String, i64)> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut statement = conn
        .prepare(
            "SELECT kind,origin_key,next_recheck_at FROM fence_recheck ORDER BY kind,origin_key",
        )
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn main() {
    let path = std::env::args().nth(1).expect("cloned snapshot path");
    assert!(
        Path::new(&path).is_file(),
        "rehearsal requires an existing VACUUM INTO copy"
    );
    let before_ms = clock_ms();
    let wal_before = wal_bytes(&path);
    let started = Instant::now();
    let store =
        SqliteRouteStateStore::open(&dev_descriptor("report-rehearsal", path.clone())).unwrap();
    println!("migration wall time: {:?}", started.elapsed());
    println!(
        "WAL bytes before/after migration: {wal_before}/{}",
        wal_bytes(&path)
    );
    let after_ms = clock_ms();
    let clocks = store.fence_rechecks().unwrap();
    let mut counts = std::collections::BTreeMap::new();
    for clock in &clocks {
        *counts
            .entry((clock.kind.clone(), clock.quota))
            .or_insert(0usize) += 1;
        if clock.quota {
            assert!(
                (before_ms - 1000..=after_ms).contains(&clock.next_recheck_at),
                "existing quota must recheck now"
            );
        } else {
            assert!(
                clock.next_recheck_at <= after_ms + 3_600_000,
                "non-quota conversion exceeds one hour"
            );
        }
    }
    println!("schedule counts (kind,quota): {counts:?}");
    drop(store);
    let first = schedules(&path);
    let reopened =
        SqliteRouteStateStore::open(&dev_descriptor("report-rehearsal", path.clone())).unwrap();
    drop(reopened);
    assert_eq!(
        first,
        schedules(&path),
        "reopening must not renew converted clocks"
    );
    #[cfg(feature = "test-support")]
    {
        let error = SqliteRouteStateStore::open_at_version_for_test(
            &dev_descriptor("report-rehearsal", path.clone()),
            81,
        )
        .err()
        .expect("previous chain must refuse schema 82");
        assert!(error.to_string().contains("store_ahead"));
        println!("previous-chain guard: {error}");
    }
    #[cfg(not(feature = "test-support"))]
    println!("previous-chain guard skipped: rerun with --features test-support");
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let extended: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fence_recheck r JOIN fence_origin o USING(kind,origin_key)
        WHERE r.quota=0 AND o.deadline IS NOT NULL AND r.next_recheck_at>o.deadline",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        extended, 0,
        "conversion must preserve every shorter existing end"
    );
    println!("conversion extensions: {extended}");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    println!("integrity_check: {integrity}");
    assert_eq!(integrity, "ok");
    let foreign_keys: i64 = conn
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    println!("foreign_key_check: {foreign_keys} violations");
    assert_eq!(foreign_keys, 0);
}
