use super::*;

#[test]
fn retirement_prune_keeps_young_and_nonretirement_tombstones() {
    let rig = super::credential::native_rig("native-retirement-prune");
    let now = crate::now_ms();
    let day = 86_400_000;
    rig.store.account_command(crate::AccountCommandKind::Maintenance,move |tx|tx.pool_sql(move |conn| {
        for (key,at) in [("retention/old",now-31*day),("retention/young",now-29*day)] {
            conn.execute("INSERT INTO block(kind,origin_key,provider,model,evidence,evidence_at,quota,next_recheck_at,deadline,observed_at,cooldown_until)
                VALUES ('route',json_array(?1),'retention',?1,json_array(?2,NULL,?2),?2,0,?2,?2,?2,?2)",rusqlite::params![key,at])?;
            conn.execute("INSERT INTO retirement_tombstone SELECT kind,origin_key,evidence,evidence_at,?2 FROM block WHERE kind='route' AND origin_key=json_array(?1)",rusqlite::params![key,at])?;
        }
        conn.execute("INSERT INTO block(kind,origin_key,provider,model,evidence,evidence_at,quota,next_recheck_at,cleared_at,cleared_by,managed)
            VALUES ('route','[\"retention/kept\"]','retention','kept','kept',?1,0,?1,?1,'kept',1)",[now-40*day])?;
        Ok(())
    })).unwrap();
    let outside = rig.store.block_writes_outside_command();
    // The count includes both the retired block and its retirement archive row.
    assert_eq!(
        rig.store
            .prune_retired_fence_origins(now - 30 * day, 10)
            .unwrap(),
        2
    );
    assert_eq!(rig.store.block_writes_outside_command(), outside);
    assert_eq!(rig.published(), rig.fresh());
    rig.store.with_reader_for_test(|conn| {
        let keys=conn.prepare("SELECT origin_key FROM block ORDER BY origin_key")?.query_map([],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(keys,vec!["[\"retention/kept\"]","[\"retention/young\"]"]);
        assert_eq!(conn.query_row("SELECT count(*) FROM retirement_tombstone WHERE origin_key='[\"retention/old\"]'",[],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(conn.query_row("SELECT count(*) FROM retirement_tombstone WHERE origin_key='[\"retention/young\"]'",[],|row|row.get::<_,i64>(0))?,1);
        Ok(())
    }).unwrap();
}

#[test]
fn cleared_fence_keeps_its_observation_through_the_prune() {
    let rig = crate::block_state::tests::rig_before_block_migration("native-observation-retention");
    let now = crate::now_ms();
    rig.store.seed_block_parity_fixture_for_test(now);
    rig.store.with_writer_for_test(|conn| {
        conn.execute("UPDATE account_fence SET observation_id='protected-observation' WHERE fence_id='parity-fence'",[])?;
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)
    }).unwrap();
    rig.reload();
    let fence = rig
        .published()
        .active_fences("cpu-provider-0")
        .into_iter()
        .find(|row| row.fence_id == "parity-fence")
        .unwrap();
    assert_eq!(
        fence.observation_id.as_deref(),
        Some("protected-observation")
    );
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            for id in ["protected-observation", "unreferenced-observation"] {
                tx.insert_observation(&crate::AccountObservationRow {
                    observation_id: id.into(),
                    principal: "principal".into(),
                    runner: "runner".into(),
                    event_id: id.into(),
                    kind: "account_limit_exhausted".into(),
                    admission_id: None,
                    bound: false,
                    observation_json: "{}".into(),
                    effects_json: "[]".into(),
                    received_at: now - 40 * 86_400_000,
                })?;
            }
            tx.clear_held_fence(&fence, now, "recovered")
        })
        .unwrap();
    assert!(rig.published().account_fence("parity-fence").is_none());
    assert_eq!(
        rig.store
            .prune_account_observations(now - 1000, 10)
            .unwrap(),
        1
    );
    assert_eq!(rig.published(), rig.fresh());
    rig.store.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT count(*) FROM account_observation WHERE observation_id='protected-observation'",[],|row|row.get::<_,i64>(0))?,1);
        assert_eq!(conn.query_row("SELECT count(*) FROM account_observation WHERE observation_id='unreferenced-observation'",[],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(conn.query_row("SELECT source_present,cleared_at IS NOT NULL FROM block WHERE kind='account' AND origin_key='[\"parity-fence\"]'",[],|row|Ok((row.get::<_,bool>(0)?,row.get::<_,bool>(1)?)))?,(false,true));
        Ok(())
    }).unwrap();
}
