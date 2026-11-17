use crate::SqliteRouteStateStore;
use rusqlite::{params, Connection};

/// A refusal can change after its clock was claimed. Prepare that split
/// source/clock state before migration so tests can inspect the resulting block record.
pub(super) fn changed_evidence_parity_store(
    now: i64,
) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new("block-parity-overlay");
    let descriptor = crate::dev_descriptor(
        "block-parity-overlay",
        scratch.path().join("store.db").to_string_lossy(),
    );
    let migrations = crate::route_state_migrations();
    let block = migrations
        .iter()
        .position(|migration| migration.statements == crate::block_storage::MIGRATION)
        .expect("the registered block migration exists");
    let previous = migrations[..block]
        .last()
        .expect("the block migration has a predecessor")
        .version;
    let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, previous).unwrap();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| conn.execute_batch(&format!(r#"
            UPDATE fence_recheck SET last_claimed_at={now},next_recheck_at={now}+900000 WHERE kind='credential' AND credential_id='oauth:cpu-0';
            UPDATE credential_entitlement_fences SET code='new-refusal' WHERE credential_id='oauth:cpu-0';
            DELETE FROM account_transient_backoff;
            UPDATE pool_key_state SET wall_reason='new-evidence',walled_until={now}-1;
        "#))).unwrap();
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    (scratch, store)
}

pub(super) fn failure_state(
    conn: &Connection,
    route: &str,
) -> rusqlite::Result<(i64, Option<f64>)> {
    conn.query_row("SELECT m.consecutive_terminal_failures,b.deadline FROM model_route_state m
        LEFT JOIN block b ON b.kind='failure' AND b.origin_key=json_array(m.route_key) AND b.source_present=1
        WHERE m.route_key=?1", [route], |row| Ok((row.get(0)?,row.get(1)?)))
}

pub(super) fn route_unscoped(conn: &Connection, route: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT wall_unscoped FROM block WHERE kind='route' AND source_present=1
        AND origin_key=json_array(?1)",
        params![route],
        |row| row.get(0),
    )
}
