use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::{
    OBSERVED_SPEED_SAMPLE_LIMIT, OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS,
    USAGE_MIN_OUTPUT_TOKENS,
};
use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore, OBSERVED_SPEED_SAMPLES_SQL};
use rusqlite::{params, Connection};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

#[test]
fn observed_speed_sample_query_searches_the_route_time_index_without_a_temp_btree() {
    let id = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "prefrontal-routing-observed-speed-plan-{}-{timestamp}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("store.db");
    let descriptor = dev_descriptor(
        &format!("prefrontal-routing-observed-speed-plan-{id}"),
        database.to_string_lossy(),
    );
    let store = SqliteRouteStateStore::open(&descriptor).expect("open applies migrations");
    let connection = Connection::open(&database).expect("open migrated database for explain");
    let explain_sql = format!("EXPLAIN QUERY PLAN {OBSERVED_SPEED_SAMPLES_SQL}");

    for route_key in ["provider/model", "provider/model/high", "unused/route"] {
        let mut statement = connection.prepare(&explain_sql).unwrap();
        let details = statement
            .query_map(
                params![
                    route_key,
                    1_000_i64,
                    (1_000.0 + OBSERVED_SPEED_WINDOW_MS) as i64,
                    USAGE_MIN_OUTPUT_TOKENS as i64,
                    USAGE_MIN_DURATION_MS as i64,
                    OBSERVED_SPEED_SAMPLE_LIMIT,
                ],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            details.iter().any(|detail| {
                detail.contains(
                    "SEARCH model_route_usage USING INDEX \
                     model_route_usage_route_observed_idx",
                ) && detail.contains("route_key=?")
                    && detail.contains("observed_at>?")
                    && detail.contains("observed_at<?")
            }),
            "expected a bounded route/time index search for {route_key}, got {details:?}"
        );
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE")),
            "expected no temporary sort for {route_key}, got {details:?}"
        );
    }

    drop(connection);
    drop(store);
    let _ = std::fs::remove_dir_all(directory);
}
