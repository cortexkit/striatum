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
    let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
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

pub(super) fn seed_pool_fences(
    store: &SqliteRouteStateStore,
    entries: &[(
        &str,
        Option<&striatum_core::accounts::PoolKey>,
        Vec<striatum_core::accounts::PoolKey>,
    )],
) {
    store
        .account_transaction(|tx| {
            for (id, pool, recovery) in entries {
                tx.insert_pool_fence(&crate::AccountPoolFenceRow {
                    evidence: crate::AccountFenceRow {
                        fence_id: (*id).into(),
                        provider: "p".into(),
                        account_id: "a".into(),
                        scope_json: "{}".into(),
                        fence_json: "{\"reset\":{\"kind\":\"missing\"}}".into(),
                        source: "broca_account_limit".into(),
                        observed_at: 100,
                        principal: "test".into(),
                        created_at: 100,
                        admission_id: None,
                        observation_id: None,
                        cleared_at: None,
                        cleared_reason: None,
                    },
                    pool_key: pool.cloned(),
                    recovery_set: recovery.clone(),
                })?;
            }
            Ok(())
        })
        .unwrap();
}

/// Keep both raw failure deadlines: a short retry and an overlong refusal.
/// Clock registration, rather than a pre-clamped fixture, must enforce the cap.
pub(super) fn cap_inputs(store: &SqliteRouteStateStore, now: i64, until: i64) {
    store
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "p".into(),
                credential_id: "c".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            None,
            now,
        )
        .unwrap();
    store.account_transaction(|tx| {
        tx.pool_sql(|conn| {
            conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,failure_observed_at)
                VALUES ('failure/m','failure/m','failure','m',?1,?1)", [now])?;
            let row = crate::block_storage::failure_source(conn, crate::block_storage::FailureSource {
                key: "failure/m", provider: "failure", model: "m", deadline: Some(until as f64),
                reason: Some("failure"), observed_at: now, error: None, class: None, unscoped: false,
            })?.expect("the fixture supplies a failure deadline");
            crate::block_storage::write_source(conn,&row)?;
            Ok(())
        })?;
        super::register_on(tx,"failure",&serde_json::json!(["failure/m"]).to_string()).map_err(crate::err)
    }).unwrap();
}

/// Leave this expired refusal without a registered probe retry. Sync must
/// archive it; a registered retry would instead keep it available for probing.
pub(super) fn expired_unmanaged_provider(store: &SqliteRouteStateStore, now: i64) {
    store.account_transaction(|tx| tx.pool_sql(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,updated_at,wall_observed_at) VALUES ('legacy',100,100)", [])?;
        let row = crate::block_storage::wall_source(conn, crate::block_storage::WallSource {
            kind: "provider", key: "legacy", provider: "legacy", model: None,
            cooldown: Some((now-10_000) as f64), exhausted: None, wall_observed: Some(100), observed_at: 100,
            error: None, class: None, unscoped: false,
        })?.expect("the fixture supplies a provider deadline");
        crate::block_storage::write_source(conn,&row)?;
        Ok(())
    })).unwrap();
}

pub(super) fn retired_count(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT COUNT(*) FROM retirement_tombstone", [], |row| {
        row.get(0)
    })
}
