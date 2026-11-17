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

/// `block_parity` compares the legacy adapter with its SQL evaluator. Also
/// compare the public loader over block records and its functions with frozen
/// SQL evaluated from the same pre-migration input.
pub(super) fn load_and_compare(store: &SqliteRouteStateStore, now: i64) -> super::LoadedBlocks {
    let loaded = store.load_blocks(now).unwrap();
    store
        .with_reader_for_test(|conn| {
            let canonical = |rows: Vec<super::FenceRecheck>| -> std::collections::BTreeMap<_, _> {
                rows.into_iter()
                    .map(|mut row| {
                        row.cleared_models.sort();
                        ((row.kind.clone(), row.origin_key.clone()), row)
                    })
                    .collect()
            };
            let expected = canonical(super::legacy::read_effective_at(conn, now)?);
            let actual = canonical(
                loaded
                    .blocks
                    .iter()
                    .map(super::FenceRecheck::from)
                    .collect(),
            );
            assert_eq!(
                actual, expected,
                "the native loader matches the frozen SQL effective rows"
            );
            let mut report = super::BlockParity {
                load_time: std::time::Duration::ZERO,
                active: loaded.blocks.len(),
                cleared_evidence: loaded.cleared_evidence.len(),
                identities: 0,
                combinations_per_identity: 0,
                function_comparisons: 0,
                differences: Vec::new(),
            };
            super::compare_block_functions(conn, now, &expected, &loaded.blocks, &mut report)?;
            assert!(
                report.identities > 0 && report.function_comparisons > 0,
                "the native/SQL function sweep actually ran: {report:#?}"
            );
            assert!(
                report.differences.is_empty(),
                "native functions versus frozen SQL: {report:#?}"
            );
            Ok(())
        })
        .unwrap();
    loaded
}
