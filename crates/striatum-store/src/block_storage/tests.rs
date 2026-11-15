use super::*;
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
    let before = store.load_blocks(now).unwrap();
    let times = store.block_parity_times(now).unwrap();
    let expected = times
        .iter()
        .map(|at| {
            let mut blocks = store.load_blocks(*at).unwrap().blocks;
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
                assert_eq!(&actual.block, expected);
                assert!(actual.cleared_at.is_none());
            }
            assert!(rows
                .iter()
                .any(|r| r.block.claim_owner.is_some() && r.block.claim_generation > 0));
            assert!(rows
                .iter()
                .any(|r| r.block.kind == "credential" && r.block.classified));
            assert!(rows.iter().any(|r| !r.block.cleared_models.is_empty()));
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
        let old=crate::fence_rechecks::read_effective_at(conn,now)?;
        assert!(!old.iter().any(|b|b.kind=="route" && b.provider=="inherit"));
        let mixed_before=conn.query_row("SELECT cooldown_until,wall_observed_at,wall_unscoped,last_error FROM model_route_state WHERE route_key='inherit/old'",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,bool>(2)?,r.get::<_,String>(3)?)))?;
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let rows=read(conn)?;
        assert!(!rows.iter().any(|r|r.block.scope.provider=="quarantined"));
        let route=rows.iter().find(|r|r.block.kind=="route").unwrap();
        assert_eq!(route.cooldown_until,Some((now+1000) as f64));
        assert!(route.inherited_from_provider);
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
        let before=crate::fence_rechecks::read_effective_at(conn,now)?.into_iter().find(|b|b.kind=="account_route").unwrap();
        install_migration_function(conn)?;
        conn.execute_batch(MIGRATION)?;
        let rows=read(conn)?;
        let after=effective(&rows,now)?.blocks.into_iter().find(|b|b.kind=="account_route").unwrap();
        assert_eq!(crate::fence_rechecks::FenceRecheck::from(&after),before);
        assert_eq!(after.last_claimed_at,Some(now));
        assert_eq!(after.next_recheck_at,now+990000);
        assert_eq!(after.claim_owner,None);
        assert_eq!(after.claim_generation,0);
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
                .execute("UPDATE block SET meta='not json'", [])
                .is_err());
            Ok(())
        })
        .unwrap();
}
