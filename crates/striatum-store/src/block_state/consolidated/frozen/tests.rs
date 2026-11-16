use super::*;
use striatum_core::{RouteStateStore, RouteStateUpdate};

#[test]
fn frozen_columns_survive_real_writers_heals_and_operator_clears() {
    let rig = crate::block_state::tests::rig_before_block_migration("native-frozen-data");
    let now = crate::now_ms();
    rig.store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,wall_observed_at,last_error,last_error_at,updated_at)
            VALUES ('frozen',?1,?2,'billing',1,?3,'capacity',?3,?3)",rusqlite::params![now+90_000,now+80_000,now-500])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,quota_exhausted_until,wall_class,wall_unscoped,wall_observed_at,last_error,last_error_at,failure_backoff_until,failure_backoff_reason,failure_observed_at,updated_at)
            VALUES ('frozen/old','frozen/old','frozen','old',?1,?2,'billing',1,?3,'capacity',?3,?4,'terminal',?3,?3)",rusqlite::params![now+90_000,now+80_000,now-500,now+50_000])?;
        conn.execute("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,wall_observed_at,cooldown_until,cooldown_reason,cooldown_observed_at,updated_at)
            VALUES ('frozen','key',?1,'billing',?3,?2,'rate',?3,?3)",rusqlite::params![now+70_000,now+30_000,now-500])?;
        fence_rechecks::legacy::sync_at(conn,now)?;
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)
    }).unwrap();
    rig.reload();
    let baseline = rig.store.frozen_block_data_for_test().unwrap();
    rig.store
        .record_provider_cooldown(
            "frozen",
            "frozen/old",
            "frozen/old",
            &RouteStateUpdate {
                cooldown_until: Some((now + 60_000) as f64),
                last_error: Some("renewed capacity".into()),
                unscoped_wall: true,
                observed_at_ms: Some(now),
                ..Default::default()
            },
        )
        .unwrap();
    rig.store
        .update_route_state(
            "frozen/new",
            "frozen/new",
            &RouteStateUpdate {
                cooldown_until: Some((now + 40_000) as f64),
                last_error: Some("independent capacity".into()),
                unscoped_wall: true,
                observed_at_ms: Some(now + 1),
                ..Default::default()
            },
        )
        .unwrap();
    for key in ["key", "new-key"] {
        rig.store
            .pool_wall(
                "frozen",
                key,
                crate::PoolWallKind::Billing,
                now + 20_000,
                "renewed billing",
                now + 2,
            )
            .unwrap();
        rig.store
            .pool_wall(
                "frozen",
                key,
                crate::PoolWallKind::RateLimited,
                now + 10_000,
                "renewed rate",
                now + 3,
            )
            .unwrap();
    }
    let cleared = rig
        .store
        .clear_route_cooldowns("frozen/old")
        .unwrap()
        .unwrap();
    assert_eq!(cleared.routes.len(), 1);
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            assert!(tx.clear_failure_backoff("frozen/old")?.is_some());
            tx.heal_serving_fences(
                "frozen",
                "account",
                "key",
                "new",
                None,
                now + 100,
                now + 101,
            )
        })
        .unwrap();
    assert_eq!(rig.published(), rig.fresh());
    assert!(rig
        .store
        .diagnostic_route_walls(now + 102)
        .unwrap()
        .is_empty());
    let keys = rig.store.diagnostic_key_walls(now + 102).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].credential_id, "new-key");
    assert_eq!(keys[0].walled_until, None);
    assert_eq!(keys[0].cooldown_until, Some(now + 10_000));
    let before_live_stamp = rig.store.frozen_block_data_for_test().unwrap();
    let read_pool_evidence = |conn: &rusqlite::Connection| {
        conn.prepare("SELECT evidence,evidence_at FROM fence_origin WHERE kind IN ('pool_billing','pool_rate') AND provider='frozen' ORDER BY kind")?
            .query_map([], |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()
    };
    let prior_evidence = rig.store.with_reader_for_test(read_pool_evidence).unwrap();
    rig.store.with_writer_for_test(|conn| {
        assert_eq!(conn.execute(
            "UPDATE pool_key_state SET wall_observed_at=?1,cooldown_observed_at=?2 WHERE provider='frozen' AND credential_id='key'",
            rusqlite::params![now+4,now+5],
        )?,1);
        Ok(())
    }).unwrap();
    let changed_evidence = rig.store.with_reader_for_test(read_pool_evidence).unwrap();
    assert_eq!(prior_evidence.len(), 2);
    assert_eq!(changed_evidence.len(), 2);
    assert_ne!(
        changed_evidence, prior_evidence,
        "live ordering stamps must actually change the retired view's derived output"
    );
    rig.store
        .frozen_block_data_for_test()
        .unwrap()
        .assert_unchanged(&before_live_stamp);
    rig.store
        .frozen_block_data_for_test()
        .unwrap()
        .assert_unchanged(&baseline);
    rig.reload();
    rig.store
        .frozen_block_data_for_test()
        .unwrap()
        .assert_unchanged(&baseline);
    // Keep the seeded rows present so clearing old non-NULL audit values
    // cannot masquerade as an allowed delete followed by a fresh default row.
    rig.store.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT count(*) FROM model_route_state WHERE route_key='frozen/old'",[],|row|row.get::<_,i64>(0))?,1);
        assert_eq!(conn.query_row("SELECT count(*) FROM provider_cooldown_state WHERE provider_key='frozen'",[],|row|row.get::<_,i64>(0))?,1);
        assert_eq!(conn.query_row("SELECT count(*) FROM pool_key_state WHERE provider='frozen' AND credential_id='key'",[],|row|row.get::<_,i64>(0))?,1);
        Ok(())
    }).unwrap();
}
