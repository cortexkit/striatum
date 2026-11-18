use super::*;
use crate::block_storage::keyed_updates;

fn fixture(
    label: &str,
) -> (
    cortexkit_test_support::ScratchDir,
    crate::SqliteRouteStateStore,
    i64,
) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let now = crate::now_ms();
    let store =
        crate::legacy_fixture_inputs::migrated_parity_store(&scratch.path().join("state.db"), now);
    (scratch, store, now)
}

fn columns(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    conn.prepare("PRAGMA table_xinfo(block)")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(6)?))
        })?
        .filter_map(|row| match row {
            Ok((name, 0)) => Some(Ok(name)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn plan(conn: &Connection, sql: &str) -> Vec<String> {
    let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let values = vec![rusqlite::types::Value::Text("plan-key".into()); statement.parameter_count()];
    statement
        .query_map(rusqlite::params_from_iter(values), |row| row.get(3))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn keyed_plan(conn: &Connection, sql: &str, index: &str) {
    let steps = plan(conn, sql);
    assert!(
        steps
            .iter()
            .any(|step| step.contains("SEARCH block") && step.contains(index)),
        "writer must seek its held-row key through {index}: {steps:?}; SQL={sql}"
    );
    assert!(
        steps.iter().all(|step| !step.contains("SCAN block")),
        "writer must not scan block history: {steps:?}"
    );
}

fn seed_account_history(conn: &Connection, now: i64) -> rusqlite::Result<()> {
    let names = columns(conn)?;
    let values = names
        .iter()
        .map(|name| match name.as_str() {
            "origin_key" => "json_array('history-'||seq.n)".into(),
            "source_present" | "managed" | "claim_generation" => "0".into(),
            "claim_owner" | "last_claimed_at" => "NULL".into(),
            "cleared_by" => "'kept'".into(),
            "cleared_at" => "?1".into(),
            _ => format!("t.\"{name}\""),
        })
        .collect::<Vec<String>>();
    conn.execute(&format!("INSERT INTO block({}) WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10000)
        SELECT {} FROM block t CROSS JOIN seq WHERE t.kind='account' AND t.origin_key=json_array('parity-fence')",names.join(","),values.join(",")),[now-31*86400000])?;
    Ok(())
}

#[test]
fn owner_rebinds_and_credential_updates_seek_held_partial_indexes() {
    let (_scratch, store, _now) = fixture("held-writer-plans");
    store
        .with_reader_for_test(|conn| {
            for sql in [
                keyed_updates::rebind_claim_owner_sql(),
                keyed_updates::bind_claim_owner_sql(),
            ] {
                keyed_plan(conn, &sql, "block_held_owner");
            }
            for sql in [
                keyed_updates::clear_credential_sql(),
                keyed_updates::clear_matching_credential_sql(),
                keyed_updates::adopt_credential_fingerprint_sql(),
                keyed_updates::clear_credential_routes_sql(),
                keyed_updates::drop_credential_route_clocks_sql(),
            ] {
                keyed_plan(conn, &sql, "block_held_credential");
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn provider_verification_and_reload_do_not_visit_ten_thousand_cleared_accounts() {
    let (_scratch, store, now) = fixture("held-provider-history");
    store
        .with_writer_for_test(|conn| seed_account_history(conn, now))
        .unwrap();
    store
        .with_reader_for_test(|conn| {
            let state = BlockState::load(conn)?;
            let sql = provider_check_sql(0, "provider");
            let keys = conn
                .prepare(&sql)?
                .query_map(["cpu-provider-0"], |row| row.get::<_, String>("origin_key"))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert!(
                keys.iter().all(|key| !key.contains("history-")),
                "the SQL read must exclude history before Rust decoding"
            );
            assert_eq!(
                keys.len(),
                state.tables[0]
                    .scoped(&provider_scope("cpu-provider-0"))
                    .count()
            );
            keyed_plan(conn, &sql, "block_held_provider");
            keyed_plan(conn, &principal_clock_check_sql(), "block_held_provider");
            let load = load_select(0);
            let loaded = conn
                .prepare(&load)?
                .query_map([], |row| row.get::<_, String>("origin_key"))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert!(
                loaded.iter().all(|key| !key.contains("history-")),
                "startup and full reconciliation must not decode cleared history either"
            );
            assert_eq!(loaded.len(), state.tables[0].rows.len);
            Ok(())
        })
        .unwrap();
}

#[test]
fn periodic_fingerprint_scans_only_the_held_index_not_permanent_account_history() {
    let (_scratch, store, now) = fixture("held-fingerprint-history");
    store
        .with_writer_for_test(|conn| seed_account_history(conn, now))
        .unwrap();
    store
        .with_reader_for_test(|conn| {
            let sql = crate::block_state::fingerprint_sql(0, "block");
            let steps = plan(conn, &sql);
            assert!(
                steps
                    .iter()
                    .any(|step| step.contains("USING") && step.contains("block_held_provider")),
                "the aggregate may scan held entries, never permanent history: {steps:?}"
            );
            let actual = conn.query_row(&sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)? as usize,
                    row.get::<_, Option<i64>>(1)?,
                ))
            })?;
            let expected = BlockState::load(conn)?.fingerprints()[0];
            assert_eq!(actual, expected);
            assert!(
                actual.0 < 100,
                "ten thousand discarded records cannot grow the periodic active-row scan"
            );
            Ok(())
        })
        .unwrap();
}

fn copy_residue(
    conn: &Connection,
    kind: &str,
    key: &str,
    by: Option<&str>,
    at: Option<i64>,
    present: bool,
    managed: bool,
) -> rusqlite::Result<()> {
    let names = columns(conn)?;
    let values = names
        .iter()
        .map(|name| match name.as_str() {
            "origin_key" => "?1".into(),
            "source_present" => "?2".into(),
            "cleared_by" => "?3".into(),
            "cleared_at" => "?4".into(),
            "managed" => "?5".into(),
            "claim_owner" | "last_claimed_at" => "NULL".into(),
            "claim_generation" => "0".into(),
            "provider" if matches!(kind, "credential" | "observation") => {
                "json_extract(?1,'$[0]')".into()
            }
            "credential_id" if kind == "credential" => "json_extract(?1,'$[1]')".into(),
            "account_id" if kind == "observation" => "json_extract(?1,'$[1]')".into(),
            _ => format!("t.\"{name}\""),
        })
        .collect::<Vec<String>>();
    let inserted=conn.execute(&format!("INSERT INTO block({}) SELECT {} FROM block t WHERE t.kind=?6 AND t.source_present=1 ORDER BY t.rowid LIMIT 1",
        names.join(","),values.join(",")),rusqlite::params![key,i64::from(present),by,at,i64::from(managed),kind])?;
    assert_eq!(
        inserted, 1,
        "each residue has a real decoded source template"
    );
    Ok(())
}

#[test]
fn inactive_residue_retention_is_bounded_without_losing_account_replay_barriers() {
    let (_scratch, store, now) = fixture("held-history-retention");
    let before = now - 30 * 86400000;
    let account_kept = serde_json::json!(["retention-account-kept"]).to_string();
    let account_retired = serde_json::json!(["retention-account-retired"]).to_string();
    let credential = |name: &str| serde_json::json!(["cpu-provider-0", name]).to_string();
    let removed = credential("retention-old-removed");
    let kept = credential("retention-old-kept");
    let young = credential("retention-young");
    let boundary = credential("retention-boundary");
    let held = credential("retention-held-source");
    let sparse = serde_json::json!(["cpu-provider-0", "retention-sparse-observation"]).to_string();
    let kept_sparse =
        serde_json::json!(["cpu-provider-0", "retention-kept-observation"]).to_string();
    let removed_observation =
        serde_json::json!(["cpu-provider-0", "retention-removed-observation"]).to_string();
    store
        .with_writer_for_test(|conn| {
            copy_residue(
                conn,
                "account",
                &account_kept,
                Some("kept"),
                Some(before - 1),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "account",
                &account_retired,
                Some("retired"),
                Some(before - 1),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "credential",
                &removed,
                Some("removed"),
                Some(before - 1),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "credential",
                &kept,
                Some("kept"),
                Some(before - 1),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "credential",
                &young,
                Some("removed"),
                Some(before + 1),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "credential",
                &boundary,
                Some("removed"),
                Some(before),
                false,
                false,
            )?;
            copy_residue(
                conn,
                "credential",
                &held,
                Some("kept"),
                Some(before - 1),
                true,
                true,
            )?;
            copy_residue(conn, "observation", &sparse, None, None, false, true)?;
            copy_residue(
                conn,
                "observation",
                &kept_sparse,
                Some("kept"),
                Some(before - 1),
                false,
                true,
            )?;
            copy_residue(
                conn,
                "observation",
                &removed_observation,
                Some("removed"),
                Some(before - 1),
                false,
                false,
            )?;
            Ok(())
        })
        .unwrap();
    for _ in 0..5 {
        let count = || {
            store
                .with_reader_for_test(|conn| {
                    conn.query_row("SELECT count(*) FROM block", [], |row| row.get::<_, i64>(0))
                })
                .unwrap()
        };
        let prior = count();
        store.prune_retired_fence_origins(before, 1).unwrap();
        assert!(
            prior - count() <= 2,
            "each of the retired and absent-source relations has its own short batch limit"
        );
    }
    store
        .with_reader_for_test(|conn| {
            for key in [&removed, &kept, &removed_observation] {
                assert_eq!(
                    conn.query_row(
                        "SELECT count(*) FROM block WHERE origin_key=?1",
                        [key],
                        |r| r.get::<_, i64>(0)
                    )?,
                    0,
                    "old non-held residue is collected: {key}"
                );
            }
            for key in [
                &account_kept,
                &account_retired,
                &young,
                &boundary,
                &held,
                &sparse,
                &kept_sparse,
            ] {
                assert_eq!(
                    conn.query_row(
                        "SELECT count(*) FROM block WHERE origin_key=?1",
                        [key],
                        |r| r.get::<_, i64>(0)
                    )?,
                    1,
                    "permanent ids, cutoff boundary and held records survive: {key}"
                );
            }
            Ok(())
        })
        .unwrap();
    let template = store
        .account_read(|tx| Ok(tx.load_block_state()?.active_fences("cpu-provider-0")[0].clone()))
        .unwrap();
    for id in ["retention-account-kept", "retention-account-retired"] {
        let mut replay = template.clone();
        replay.fence_id = id.into();
        replay.cleared_at = None;
        replay.cleared_reason = None;
        store
            .account_transaction(|tx| tx.insert_fence(&replay))
            .unwrap();
        store.with_reader_for_test(|conn| {
            assert_eq!(conn.query_row("SELECT source_present FROM block WHERE kind='account' AND origin_key=json_array(?1)",[id],|r|r.get::<_,i64>(0))?,0,"a cleared permanent fence id cannot be replayed after retention");
            Ok(())
        }).unwrap();
    }
}

#[test]
fn residue_and_retirement_batches_seek_only_their_age_indexes() {
    let (_scratch, store, _now) = fixture("held-retention-plans");
    store
        .with_reader_for_test(|conn| {
            for (sql, index) in [
                (
                    keyed_updates::inactive_block_prune_sql(),
                    "block_absent_time",
                ),
                (
                    keyed_updates::retired_block_prune_sql(),
                    "block_retired_time",
                ),
            ] {
                let steps = plan(conn, &sql);
                assert!(
                    steps
                        .iter()
                        .any(|step| step.contains("SEARCH block") && step.contains(index)),
                    "short retention batches must seek eligible ages: {steps:?}"
                );
                assert!(
                    steps.iter().all(|step| !step.contains("SCAN block")),
                    "retention must not walk permanent account ids: {steps:?}"
                );
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn source_order_allocation_seeks_live_sources_without_account_history() {
    let (_scratch, store, now) = fixture("held-source-order-plan");
    store
        .with_writer_for_test(|conn| seed_account_history(conn, now))
        .unwrap();
    store
        .with_reader_for_test(|conn| {
            let steps = plan(conn, crate::block_storage::NEXT_SOURCE_ORDER);
            assert!(
                steps.iter().any(|step| step.contains("SEARCH block")
                    && step.contains("block_live_source_order")),
                "allocating the next live ordinal must not walk cleared ids: {steps:?}"
            );
            Ok(())
        })
        .unwrap();
}
