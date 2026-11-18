use super::*;

#[path = "clock_normalization/tests.rs"]
mod clock_normalization;

#[path = "observation_receipts/tests.rs"]
mod observation_receipts;

#[path = "claim_generation/tests.rs"]
mod claim_generation;

fn migration_function_is_absent(conn: &Connection) {
    let error = conn
        .query_row(
            "SELECT block_migration_quarantined(0,NULL,NULL)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_err();
    assert!(error.to_string().contains("no such function"), "{error}");
    assert_eq!(
        conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn registered_migration_opens_fresh_and_older_chains_without_leaving_its_function() {
    assert_eq!(
        crate::SqliteRouteStateStore::latest_migration_version_for_test(),
        97
    );
    let versions = std::iter::once(0).chain(
        crate::route_state_migrations()
            .into_iter()
            .filter(|migration| migration.version < 96)
            .map(|migration| migration.version),
    );
    for version in versions {
        let scratch = cortexkit_test_support::ScratchDir::new("block-registered-upgrade");
        let descriptor = crate::dev_descriptor(
            "block-registered-upgrade",
            scratch.path().join("state.db").to_string_lossy(),
        );
        if version > 0 {
            let store = crate::open_sqlite(&descriptor).unwrap();
            let older = crate::route_state_migrations()
                .into_iter()
                .filter(|migration| migration.version <= version)
                .collect::<Vec<_>>();
            crate::migrate_route_state(&store, &older).unwrap();
        }
        let store = crate::SqliteRouteStateStore::open(&descriptor).unwrap();
        store.with_writer_for_test(|conn| {
            migration_function_is_absent(conn);
            assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('block','probe_authorization','success_proof','retirement_tombstone')",[],|row|row.get::<_,i64>(0))?,4);
            Ok(())
        }).unwrap();
        store.frozen_block_data_for_test().unwrap();
        drop(store);
        let error = crate::SqliteRouteStateStore::open_at_version_for_test(&descriptor, 93)
            .err()
            .unwrap();
        assert!(error.to_string().contains("store_ahead"), "{error}");
        assert!(error.to_string().contains("version 97"), "{error}");
        assert!(error.to_string().contains("93"), "{error}");
    }
}

#[test]
fn failed_registered_migration_is_atomic_and_removes_the_function_on_its_writer() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-failed-upgrade");
    let descriptor = crate::dev_descriptor(
        "block-failed-upgrade",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let store = crate::open_sqlite(&descriptor).unwrap();
    let chain = crate::route_state_migrations();
    let block = chain
        .iter()
        .position(|migration| migration.version == 96)
        .unwrap();
    crate::migrate_route_state(&store, &chain[..block]).unwrap();
    store.with_conn(|conn| {
        conn.execute("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_observed_at,updated_at) VALUES ('bad-real','key',1.5,1,1)",[])?;
        Ok(())
    }).unwrap();
    let before = store
        .with_conn(|conn| {
            let mut query = conn.prepare("SELECT * FROM cortexkit_schema_version ORDER BY 1")?;
            let width = query.column_count();
            let rows = query.query_map([], |row| {
                (0..width)
                    .map(|index| row.get::<_, rusqlite::types::Value>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap();
    let error = crate::migrate_route_state(&store, &chain).unwrap_err();
    assert!(
        error.to_string().contains("CHECK constraint failed"),
        "{error}"
    );
    store.with_conn(|conn| {
        migration_function_is_absent(conn);
        assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_schema WHERE name IN ('block','probe_authorization','success_proof','retirement_tombstone')",[],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(conn.query_row("SELECT walled_until FROM pool_key_state WHERE provider='bad-real'",[],|row|row.get::<_,f64>(0))?,1.5);
        assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_schema WHERE name='fence_origin'",[],|row|row.get::<_,i64>(0))?,1);
        let mut query=conn.prepare("SELECT * FROM cortexkit_schema_version ORDER BY 1")?;
        let width=query.column_count();
        let after=query.query_map([],|row|(0..width).map(|index|row.get::<_,rusqlite::types::Value>(index)).collect::<rusqlite::Result<Vec<_>>>())?.collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(after,before);
        Ok(())
    }).unwrap();
}
use crate::SqliteRouteStateStore;
use std::collections::BTreeSet;

#[test]
fn migration_preserves_each_kind_metadata_and_claimed_generation() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-migration");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-migration",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.assert_block_parity_fixture_coverage_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO account_size_refusal VALUES ('unmanaged','credential','model',13,?1,?2)",
            rusqlite::params![now+700,now-500])?;
        Ok(())
    }).unwrap();
    let before = store
        .with_reader_for_test(|conn| crate::fence_rechecks::legacy::load_blocks(conn, now))
        .unwrap();
    let times = store.block_parity_times(now).unwrap();
    let expected = times
        .iter()
        .map(|at| {
            let mut blocks = store
                .with_reader_for_test(|conn| crate::fence_rechecks::legacy::load_blocks(conn, *at))
                .unwrap()
                .blocks;
            blocks.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
            blocks
        })
        .collect::<Vec<_>>();
    store
        .with_writer_for_test(|conn| {
            install_migration_function(conn)?;
            conn.execute_batch(MIGRATION)?;
            let rows = read(conn)?;
            assert_eq!(
                rows.iter()
                    .map(|r| r.block.kind.as_str())
                    .collect::<BTreeSet<_>>(),
                striatum_core::blocks::SOURCE_KINDS.into_iter().collect()
            );
            for row in &rows {
                let encoded = row.meta.encode().unwrap();
                let kind = if !row.source_present && encoded == "{}" {
                    ""
                } else {
                    row.block.kind.as_str()
                };
                assert_eq!(
                    Meta::decode(kind, &encoded).unwrap(),
                    row.meta,
                    "{} {}",
                    row.block.kind,
                    row.block.origin_key
                );
            }
            for expected in &before.blocks {
                let actual = rows
                    .iter()
                    .find(|r| {
                        r.block.kind == expected.kind && r.block.origin_key == expected.origin_key
                    })
                    .unwrap();
                let mut block = actual.block.clone();
                block.cleared_models = actual.cleared_models.clone();
                block.cleared_models.sort();
                assert_eq!(&block, expected);
                assert!(actual.cleared_at.is_none());
            }
            assert!(rows
                .iter()
                .any(|r| r.block.claim_owner.is_some() && r.block.claim_generation > 0));
            assert!(rows
                .iter()
                .any(|r| r.block.kind == "credential" && r.block.classified));
            assert!(rows.iter().any(|r| !r.cleared_models.is_empty()));
            for (at, expected) in times.iter().zip(&expected) {
                assert_eq!(
                    &effective(&rows, *at)?.blocks,
                    expected,
                    "effective block parity at {at}"
                );
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn metadata_rejects_unknown_fields_for_every_kind() {
    for kind in striatum_core::blocks::SOURCE_KINDS {
        assert!(Meta::decode(kind, r#"{"unknown":true}"#).is_err(), "{kind}");
    }
}

#[test]
fn migration_keeps_heals_retirement_generations_and_quota_hold_identity() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-evidence-parity");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-evidence-parity",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,observation_id,created_at,cleared_at,cleared_reason,pool_key_json,recovery_set_json)
          SELECT 'credential-quota:[\"cpu-provider-0\",\"oauth:cpu-0\"]:minted-id',provider,account_id,scope_json,fence_json,source,observed_at,principal,'retained-observation',created_at,NULL,NULL,pool_key_json,recovery_set_json
          FROM account_fence WHERE fence_id='parity-fence'",[])?;
        conn.execute("UPDATE fence_recheck SET cleared_at=?1 WHERE kind='principal'",[now])?;
        conn.execute("UPDATE account_fence SET cleared_at=?1,cleared_reason='healed' WHERE fence_id='parity-fence'",[now])?;
        conn.execute("INSERT INTO fence_retired_origin VALUES ('route','[\"retired/route\"]','first',?1,?2),('route','[\"retired/route\"]','second',?2,?2)",rusqlite::params![now-10,now])?;
        let retired=conn.prepare("SELECT kind,origin_key,evidence,evidence_at,retired_at FROM fence_retired_origin ORDER BY kind,origin_key,evidence")?
            .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let after=conn.prepare("SELECT kind,origin_key,evidence,evidence_at,retired_at FROM retirement_tombstone ORDER BY kind,origin_key,evidence")?
            .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(after,retired,"every retirement generation survives verbatim");
        let rows=read(conn)?;
        let healed=rows.iter().find(|r|r.block.kind=="principal").unwrap();
        assert_eq!(healed.cleared_at,Some(now));
        assert_eq!(healed.cleared_by.as_deref(),Some("kept"));
        assert!(healed.held(),"the source still carries the cleared evidence");
        let cleared=rows.iter().find(|r|r.block.origin_key=="[\"parity-fence\"]").unwrap();
        assert!(!cleared.held(),"cleared minted fence ids are not live snapshot records");
        let hold=rows.iter().find(|r|r.observation_id.as_deref()==Some("retained-observation")).unwrap();
        assert_eq!(hold.block.kind,"account");
        assert_eq!(hold.block.origin_key,r#"["credential-quota:[\"cpu-provider-0\",\"oauth:cpu-0\"]:minted-id"]"#);
        assert_eq!(hold.block.lifetime,striatum_core::blocks::LifetimeClass::Quota);
        assert!(!effective(&rows,now)?.blocks.iter().any(|b|b.kind=="principal" || b.origin_key=="[\"parity-fence\"]"));
        Ok(())
    }).unwrap();
}

#[test]
fn migration_keeps_hidden_route_deadlines_and_removes_quarantined_providers() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-hidden-route");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-hidden-route",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit',?1,?2,1,'capacity',?2)",rusqlite::params![now+2000,now-1])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit/old','inherit/old','inherit','old',?1,?2,1,'capacity',?2)",rusqlite::params![now+1000,now-1])?;
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_class,last_error,updated_at) VALUES ('quarantined',?1,?2,'quota','{\"provider_code\":\"quota_exhausted\",\"account_ref\":\"account\"}',?2)",rusqlite::params![now+9000,now-1])?;
        crate::fence_rechecks::sync_at(conn,now)?;
        let old=crate::fence_rechecks::legacy::read_effective_at(conn,now)?;
        assert!(!old.iter().any(|b|b.kind=="route" && b.provider=="inherit"));
        let mixed_before=conn.query_row("SELECT cooldown_until,wall_observed_at,wall_unscoped,last_error FROM model_route_state WHERE route_key='inherit/old'",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,bool>(2)?,r.get::<_,String>(3)?)))?;
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let rows=read(conn)?;
        assert!(!rows.iter().any(|r|r.block.scope.provider=="quarantined"));
        let route=rows.iter().find(|r|r.block.kind=="route").unwrap();
        assert_eq!(route.cooldown_until.map(SqlReal::as_f64),Some((now+1000) as f64));
        assert!(route.source_present && route.is_origin(),"a covered route wall remains a real stored origin");
        assert_eq!(effective(&rows,now)?.blocks.iter().filter(|b|b.scope.provider=="inherit").map(|b|b.kind.as_str()).collect::<Vec<_>>(),vec!["provider"]);
        let mixed_after=conn.query_row("SELECT cooldown_until,wall_observed_at,wall_unscoped,last_error FROM model_route_state WHERE route_key='inherit/old'",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,bool>(2)?,r.get::<_,String>(3)?)))?;
        assert_eq!(mixed_after,mixed_before,"the migration does not rewrite mixed-table evidence");
        conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at) VALUES ('inherit/new','inherit/new','inherit','new','{}','curated',?1)",[now])?;
        let fresh=conn.query_row("SELECT cooldown_until,quota_exhausted_until,failure_backoff_until,failure_backoff_reason,wall_class,wall_unscoped FROM model_route_state WHERE route_key='inherit/new'",[],|r|Ok((r.get::<_,Option<i64>>(0)?,r.get::<_,Option<i64>>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,bool>(5)?)))?;
        assert_eq!(fresh,(None,None,None,None,None,false));
        assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='model_route_state_inherit_fence_scope'",[],|r|r.get::<_,i64>(0))?,0);
        Ok(())
    }).unwrap();
}

#[test]
fn migration_new_evidence_keeps_the_prior_claim_time_without_its_authorization() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-new-evidence");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-new-evidence",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("UPDATE account_route_fence SET until_ms=?1,last_reported_at=?2 WHERE provider='cpu-provider-0'",rusqlite::params![now+50,now-10])?;
        conn.execute("UPDATE fence_recheck SET claim_owner='older-owner',claim_generation=41,last_claimed_at=?1,next_recheck_at=?2 WHERE kind='account_route'",rusqlite::params![now,now+990000])?;
        let before=crate::fence_rechecks::legacy::read_effective_at(conn,now)?.into_iter().find(|b|b.kind=="account_route").unwrap();
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let rows=read(conn)?;
        let after=effective(&rows,now)?.blocks.into_iter().find(|b|b.kind=="account_route").unwrap();
        assert_eq!(crate::fence_rechecks::FenceRecheck::from(&after),before);
        assert_eq!(after.last_claimed_at,Some(now));
        assert_eq!(after.next_recheck_at,now+990000);
        assert_eq!(after.claim_owner,None);
        // The changed refusal signature invalidates the old probe authorization.
        // Preserve claim_generation so a later claim cannot match that stale
        // authorization if the earlier refusal returns.
        assert_eq!(after.claim_generation,41);
        Ok(())
    }).unwrap();
}

#[test]
fn successor_tables_are_rowid_tables_and_retirement_enforces_only_its_exact_evidence() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-retirement");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-retirement",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store
        .with_writer_for_test(|conn| {
            install_migration_function(conn)?;
            conn.execute_batch(MIGRATION)?;
            for table in [
                "block",
                "probe_authorization",
                "success_proof",
                "retirement_tombstone",
            ] {
                conn.prepare(&format!("SELECT rowid FROM {table}"))?;
                let unique = conn
                    .prepare(&format!("PRAGMA index_list({table})"))?
                    .query_map([], |r| Ok((r.get::<_, bool>(2)?, r.get::<_, String>(3)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert_eq!(
                    unique
                        .into_iter()
                        .filter(|(unique, _)| *unique)
                        .map(|(_, origin)| origin)
                        .collect::<Vec<_>>(),
                    vec!["pk"],
                    "{table}"
                );
            }
            let before = read(conn)?
                .into_iter()
                .find(|r| r.block.kind == "credential" && r.source_present)
                .unwrap();
            conn.execute(
                "INSERT INTO retirement_tombstone VALUES (?1,?2,'another generation',?3,?4)",
                rusqlite::params![before.block.kind, before.block.origin_key, now - 10, now],
            )?;
            assert!(read(conn)?.iter().any(|r| r.block.kind == before.block.kind
                && r.block.origin_key == before.block.origin_key
                && r.cleared_at.is_none()));
            conn.execute(
                "INSERT INTO retirement_tombstone VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    before.block.kind,
                    before.block.origin_key,
                    before.block.evidence,
                    before.block.evidence_at,
                    now
                ],
            )?;
            let after = read(conn)?
                .into_iter()
                .find(|r| {
                    r.block.kind == before.block.kind
                        && r.block.origin_key == before.block.origin_key
                })
                .unwrap();
            assert_eq!(after.cleared_at, Some(now));
            assert_eq!(after.cleared_by.as_deref(), Some("retired"));
            assert!(!after.held());
            assert!(conn
                .execute("UPDATE block SET source_metadata_json='not json'", [])
                .is_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn retirement_prevents_reinserted_evidence_from_reblocking() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-retired-reinsert");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-retired-reinsert",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let persisted=conn.prepare("SELECT name FROM pragma_table_xinfo('block') WHERE hidden=0 ORDER BY cid")?
            .query_map([],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?.join(",");
        conn.execute_batch(&format!("CREATE TEMP TABLE delayed_block AS SELECT {persisted} FROM block WHERE kind='credential' AND source_present=1;
            INSERT INTO retirement_tombstone SELECT kind,origin_key,evidence,evidence_at,evidence_at+1 FROM delayed_block;
            DELETE FROM block WHERE kind='credential';
            INSERT INTO block({persisted}) SELECT {persisted} FROM delayed_block;"))?;
        let rows = read(conn)?;
        assert!(!rows.iter().any(|row| row.block.kind == "credential" && row.held()));
        conn.execute("UPDATE block SET evidence='different generation',cleared_at=NULL,cleared_by=NULL,source_present=1 WHERE kind='credential'",[])?;
        assert!(read(conn)?.iter().any(|row| row.block.kind == "credential" && row.held()));
        conn.execute("UPDATE block SET (evidence,source_present)=(SELECT evidence,1 FROM delayed_block d WHERE d.kind=block.kind AND d.origin_key=block.origin_key) WHERE kind='credential'",[])?;
        assert!(!read(conn)?.iter().any(|row| row.block.kind == "credential" && row.held()));
        Ok(())
    }).unwrap();
}

#[test]
fn migration_keeps_null_wall_stamp_fallback_separate_from_an_older_clock() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-fallback-evidence");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "block-fallback-evidence",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = 1_800_000_000_000_i64;
    let source_time = now - 100;
    let clock_time = now - 200;
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,last_error,last_error_at,updated_at) VALUES ('fallback-provider',?1,'{\"code\":\"throttled\"}',?2,?2)",rusqlite::params![now+1_000,source_time])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,last_error,last_error_at,updated_at) VALUES ('fallback-provider/model','fallback-provider/model','fallback-provider','model',?1,'{\"code\":\"throttled\"}',?2,?2)",rusqlite::params![now+1_000,source_time])?;
        conn.execute("UPDATE provider_cooldown_state SET wall_observed_at=NULL",[])?;
        conn.execute("UPDATE model_route_state SET wall_observed_at=NULL,last_error_at=?1,updated_at=?1",[source_time])?;
        crate::fence_rechecks::legacy::sync(conn)?;
        assert_eq!(conn.execute("UPDATE fence_recheck SET evidence_at=?1 WHERE kind='provider'",[clock_time])?,1);
        let expected = crate::fence_rechecks::legacy::load_blocks(conn,now)?;
        let expected_origins=crate::block_state::legacy::BlockState::load(conn)?.view_origins();
        assert_eq!(expected.blocks.iter().map(|b|b.kind.as_str()).collect::<Vec<_>>(),vec!["provider"]);
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let source_times=conn.prepare("SELECT kind,observed_at FROM block WHERE kind IN ('provider','route') ORDER BY kind")?.query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,f64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(source_times,vec![("provider".into(),source_time as f64),("route".into(),source_time as f64)]);
        let state=crate::BlockState::load(conn)?;
        // An old matching clock can carry an earlier probe timestamp. Coverage
        // still compares the source rows' shared fallback evidence timestamp.
        assert_eq!(effective(&read(conn)?,now)?.blocks,expected.blocks);
        assert_eq!(state.blocks(now).unwrap().blocks,expected.blocks);
        assert_eq!(state.blocks(now).unwrap().blocks[0].evidence_at,clock_time);
        assert_eq!(state.view_origins(),expected_origins);
        Ok(())
    }).unwrap();
}

#[test]
fn migration_retires_pre_fix_cleared_observation_orphans_and_spends_all_authorizations() {
    let scratch = cortexkit_test_support::ScratchDir::new("cleared-observation-migration");
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "cleared-observation-migration",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("UPDATE fence_recheck SET cleared_at=?1,claim_generation=3 WHERE kind='observation'",[now-500])?;
        for owner in ["old-admission-1","old-decision-1","old-admission-2","old-decision-2","final-admission"] {
            conn.execute("INSERT INTO fence_probe_authorization SELECT ?1,kind,origin_key,evidence,evidence_at,evidence_at+1,claim_generation FROM fence_recheck WHERE kind='observation'",[owner])?;
        }
        let before=crate::FrozenBlockData::before_migration(conn)?;
        let key=serde_json::json!(["cpu-provider-0","account-0"]).to_string();
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_origin WHERE kind='observation' AND origin_key=?1",[&key],|row|row.get::<_,i64>(0))?,1);
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_probe_authorization WHERE kind='observation' AND origin_key=?1",[&key],|row|row.get::<_,i64>(0))?,5);
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        assert_eq!(conn.query_row("SELECT count(*) FROM block WHERE kind='observation' AND origin_key=?1 AND is_origin=1 AND source_present=1",[&key],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(conn.query_row("SELECT retired_at FROM retirement_tombstone WHERE kind='observation' AND origin_key=?1",[&key],|row|row.get::<_,i64>(0))?,now-500);
        assert_eq!(conn.query_row("SELECT count(*) FROM probe_authorization WHERE kind='observation' AND origin_key=?1",[&key],|row|row.get::<_,i64>(0))?,0);
        let physical:(String,i64)=conn.query_row("SELECT cleared_by,cleared_at FROM block WHERE kind='observation' AND origin_key=?1",[&key],|row|Ok((row.get(0)?,row.get(1)?)))?;
        assert_eq!(physical,("retired".into(),now-500));
        assert!(!crate::BlockState::load(conn)?.blocks(now).unwrap().blocks.iter().any(|block|block.kind=="observation"));
        // Capture the post-migration audit through its normal presence checks.
        // It must still contain the original source, clock and five permissions.
        let after=crate::frozen_block_data::read(conn)?;
        after.assert_unchanged(&before);
        Ok(())
    }).unwrap();
}
