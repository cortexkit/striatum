use super::*;

#[test]
fn inherited_route_uses_claimed_provider_clock_but_provider_status_keeps_raw_deadline() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-inherited-provider");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-inherited-provider",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    let deadline = now + 2000;
    let query = deadline + 2;
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit',?1,?2,1,'capacity',?2)",rusqlite::params![deadline,now-1])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit/old','inherit/old','inherit','old',?1,?2,1,'capacity',?2)",rusqlite::params![now+1000,now-1])?;
        fence_rechecks::sync_at(conn,now)?;
        assert_eq!(conn.execute("UPDATE fence_recheck SET last_claimed_at=?1,next_recheck_at=?2,claim_owner='owner',claim_generation=1 WHERE kind='provider'",rusqlite::params![deadline+1,deadline+60_000])?,1);
        let legacy = super::super::BlockState::load(conn)?;
        let expected = legacy.route_state_snapshot(query as f64,query).unwrap();
        assert!(expected.by_route["inherit/old"].cooldown_until.is_some_and(|at| at > query as f64));
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(conn)?;
        assert_eq!(state.route_state_snapshot(query as f64,query).unwrap(),expected);
        assert_eq!(state.provider_cooldown("inherit").unwrap(),legacy.provider_cooldown("inherit").unwrap());
        assert_eq!(state.provider_cooldown("inherit").unwrap().unwrap().cooldown_until,Some(deadline as f64));
        conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at) VALUES ('inherit/new','inherit/new','inherit','new','{}','curated',?1)",[now])?;
        assert_eq!(conn.query_row("SELECT cooldown_until,quota_exhausted_until,wall_class FROM model_route_state WHERE route_key='inherit/new'",[],|row| Ok((row.get::<_,Option<i64>>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,Option<String>>(2)?)))?,(None,None,None));
        let state = BlockState::load(conn)?;
        assert!(state.fence_rechecks(query).unwrap().iter().any(|block| block.applies("inherit","","","new")));
        conn.execute("UPDATE block SET cleared_at=?1,cleared_by='kept' WHERE kind='provider'",[query])?;
        let state = BlockState::load(conn)?;
        assert!(!state.fence_rechecks(query).unwrap().iter().any(|block| block.applies("inherit","","","new")));
        Ok(())
    }).unwrap();
}

#[test]
fn quarantined_provider_future_deadline_does_not_survive_in_status() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-quarantine-status");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-quarantine-status",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_class,last_error,updated_at) VALUES ('quarantined',?1,?2,'quota','{\"provider_code\":\"quota_exhausted\",\"account_ref\":\"account\"}',?2)",rusqlite::params![now+9000,now-1])?;
        assert!(super::super::BlockState::load(conn)?.quarantined().iter().any(|(kind,_)| kind == "provider"));
        let legacy = super::super::BlockState::load(conn)?;
        assert!(legacy.provider_cooldown("quarantined").unwrap().unwrap().cooldown_until.is_some_and(|at| at > now as f64));
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(conn)?;
        let status = state.provider_cooldown("quarantined").unwrap().unwrap();
        assert_eq!(status.cooldown_until,None);
        assert_eq!(status.quota_exhausted_until,None);
        assert!(state.blocks(now).unwrap().blocks.is_empty());
        assert!(!state.block_rows().any(|row| row.block.kind == "route"));
        Ok(())
    }).unwrap();
}
