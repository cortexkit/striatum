use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::{
    OBSERVED_SPEED_SAMPLE_LIMIT, OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS,
    USAGE_MIN_OUTPUT_TOKENS,
};
use prefrontal_routing_store::{
    dev_descriptor, SqliteRouteStateStore, OBSERVED_SPEED_SAMPLES_SQL,
    WORKLOAD_CLASS_USAGE_SAMPLES_SQL,
};
use rusqlite::{params, Connection};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

#[test]
fn observed_speed_sample_query_searches_the_route_time_index_without_a_temp_btree() {
    let id = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = cortexkit_test_support::scratch_root().join(format!(
        "striatum-observed-speed-plan-{}-{timestamp}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("store.db");
    let descriptor = dev_descriptor(
        &format!("striatum-observed-speed-plan-{id}"),
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
                    "standard",
                    "standard",
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
        assert!(details.iter().any(|detail| detail.contains(
            "SEARCH model_route_speed_samples USING INDEX model_route_speed_samples_route_tier_observed_idx"
        )), "expected an indexed tier/speed lookup, got {details:?}");
    }

    drop(connection);
    drop(store);
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn workload_class_usage_query_uses_the_workload_class_observed_index() {
    let id = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = cortexkit_test_support::scratch_root().join(format!(
        "striatum-workload-class-plan-{}-{timestamp}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("store.db");
    let descriptor = dev_descriptor(
        &format!("striatum-workload-class-plan-{id}"),
        database.to_string_lossy(),
    );
    let store = SqliteRouteStateStore::open(&descriptor).expect("open applies migrations");
    let connection = Connection::open(&database).expect("open migrated database for explain");
    let explain_sql = format!("EXPLAIN QUERY PLAN {WORKLOAD_CLASS_USAGE_SAMPLES_SQL}");
    let details = connection
        .prepare(&explain_sql)
        .unwrap()
        .query_map(params!["ephemeral", 1_000_i64, 2_000_i64], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    assert!(
        details.iter().any(|detail| {
            detail.contains(
                "SEARCH model_route_usage USING INDEX \
                 model_route_usage_workload_class_observed_idx",
            ) && detail.contains("workload_class=?")
                && detail.contains("observed_at>?")
                && detail.contains("observed_at<?")
        }),
        "expected a bounded workload-class/time index search, got {details:?}"
    );
    assert!(
        details
            .iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE")),
        "expected no temporary sort, got {details:?}"
    );

    drop(connection);
    drop(store);
    let _ = std::fs::remove_dir_all(directory);
}
