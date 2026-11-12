use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{params, Connection, ErrorCode};
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-observation-event-plan-{}-{timestamp}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn principal_event_lookup_searches_both_covering_indexes() {
    let fixture = Fixture::new();
    let database = fixture.0.join("store.db");
    let _store = SqliteRouteStateStore::open(&dev_descriptor(
        "observation-event-plan",
        database.to_string_lossy(),
    ))
    .expect("open applies the index migration");
    let connection = Connection::open(database).unwrap();
    // Match the runner discovery query in observation_by_principal_event,
    // including the fallback used when a restore omitted the observation log.
    let mut explain = connection
        .prepare(
            "EXPLAIN QUERY PLAN \
             SELECT runner FROM account_observation WHERE principal=?1 AND event_id=?2 \
             UNION ALL SELECT runner FROM account_observation_event \
             WHERE principal=?1 AND event_id=?2 LIMIT 1",
        )
        .unwrap();
    let details = explain
        .query_map(params!["reserved:broca", "new-event"], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for table in ["account_observation", "account_observation_event"] {
        let expected = format!(
            "SEARCH {table} USING COVERING INDEX {table}_by_principal_event \
             (principal=? AND event_id=?)"
        );
        assert!(
            details.iter().any(|detail| detail.contains(&expected)),
            "expected {expected}, got {details:?}"
        );
        let columns = connection
            .prepare(&format!("PRAGMA index_info('{table}_by_principal_event')"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(2))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(columns, ["principal", "event_id", "runner"]);
    }
}

#[test]
fn observation_tables_keep_principal_runner_event_uniqueness() {
    let fixture = Fixture::new();
    let database = fixture.0.join("store.db");
    let _store = SqliteRouteStateStore::open(&dev_descriptor(
        "observation-event-unique",
        database.to_string_lossy(),
    ))
    .unwrap();
    let connection = Connection::open(database).unwrap();
    for table in ["account_observation", "account_observation_event"] {
        let unique_columns: Vec<String> = connection
            .prepare(
                "SELECT group_concat(ii.name, ',') FROM pragma_index_list(?1) AS il \
                 JOIN pragma_index_info(il.name) AS ii WHERE il.\"unique\"=1 \
                 GROUP BY il.name HAVING group_concat(ii.name, ',')='principal,runner,event_id'",
            )
            .unwrap()
            .query_map([table], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(unique_columns, ["principal,runner,event_id"], "{table}");
        let sql = if table == "account_observation" {
            "INSERT INTO account_observation \
             (observation_id,principal,runner,event_id,kind,bound,observation_json,effects_json,received_at) \
             VALUES (?1,'principal',?2,'log-event','advisory',0,'{}','[]',1)"
        } else {
            "INSERT INTO account_observation_event \
             (observation_id,principal,runner,event_id,bound,effects_json,received_at) \
             VALUES (?1,'principal',?2,'compact-event',0,'[]',1)"
        };
        connection.execute(sql, ["first", "runner-a"]).unwrap();
        let duplicate = connection.execute(sql, ["different-id", "runner-a"]);
        assert_eq!(
            duplicate.unwrap_err().sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation),
            "{table} must reject a duplicate event for the same runner"
        );
        connection.execute(sql, ["second", "runner-b"]).unwrap();
    }
}
