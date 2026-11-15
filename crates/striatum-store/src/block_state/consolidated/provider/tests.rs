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
        let legacy = crate::block_state::legacy::BlockState::load(conn)?;
        let expected = legacy.route_state_snapshot(query as f64,query).unwrap();
        assert!(expected.by_route["inherit/old"].cooldown_until.is_some_and(|at| at > query as f64));
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(conn)?;
        assert_eq!(state.route_state_snapshot(query as f64,query).unwrap(),expected);
        assert_eq!(state.provider_cooldown("inherit").unwrap(),legacy.provider_cooldown("inherit").unwrap());
        assert_eq!(state.provider_cooldown("inherit").unwrap().unwrap().cooldown_until,Some(deadline as f64));
        let raw=crate::route_snapshot::read_provider_cooldown(conn,"inherit").unwrap();
        let snapshot=crate::route_snapshot::read_snapshot(conn,query as f64,query).unwrap();
        assert_eq!(snapshot,expected);
        assert!(striatum_core::route_state_store::route_cooled(raw.as_ref(),&snapshot,"inherit/old","inherit/old",query as f64));
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
        assert!(crate::block_state::legacy::BlockState::load(conn)?.quarantined().iter().any(|(kind,_)| kind == "provider"));
        let legacy = crate::block_state::legacy::BlockState::load(conn)?;
        assert!(legacy.provider_cooldown("quarantined").unwrap().unwrap().cooldown_until.is_some_and(|at| at > now as f64));
        let before=legacy.provider_cooldown("quarantined").unwrap();
        let before_snapshot=legacy.route_state_snapshot(now as f64,now).unwrap();
        assert!(striatum_core::route_state_store::route_cooled(before.as_ref(),&before_snapshot,"quarantined/model","quarantined/model",now as f64));
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(conn)?;
        let status = state.provider_cooldown("quarantined").unwrap().unwrap();
        assert_eq!(status.cooldown_until,None);
        assert_eq!(status.quota_exhausted_until,None);
        let after_snapshot=state.route_state_snapshot(now as f64,now).unwrap();
        assert!(!striatum_core::route_state_store::route_cooled(Some(&status),&after_snapshot,"quarantined/model","quarantined/model",now as f64));
        assert_eq!(crate::route_snapshot::read_provider_cooldown(conn,"quarantined").unwrap(),Some(status));
        assert!(state.blocks(now).unwrap().blocks.is_empty());
        assert!(!state.block_rows().any(|row| row.block.kind == "route"));
        Ok(())
    }).unwrap();
}

#[test]
fn real_provider_heal_clears_inherited_rows_but_keeps_different_evidence_after_reload() {
    let rig = crate::block_state::tests::rig_before_block_migration("native-provider-heal");
    let now = crate::now_ms();
    rig.store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at)
            VALUES ('healing-provider',?1,?2,1,'provider-capacity',?2,?2)",rusqlite::params![now+90_000,now-1000])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at)
            VALUES ('healing-provider/inherited','healing-provider/inherited','healing-provider','inherited',?1,?2,1,'provider-capacity',?2,?2)",rusqlite::params![now+60_000,now-1000])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at)
            VALUES ('healing-provider/independent','healing-provider/independent','healing-provider','independent',?1,?2,1,'independent-capacity',?2,?2)",rusqlite::params![now+50_000,now-500])?;
        fence_rechecks::legacy::sync_at(conn,now)?;
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)
    }).unwrap();
    rig.reload();
    rig.store.account_command(crate::AccountCommandKind::Maintenance,move |tx| {
        tx.pool_sql(move |conn|conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at)
            VALUES ('healing-provider/new','healing-provider/new','healing-provider','new','{}','curated',?1)",[now]).map(|_|()))
    }).unwrap();
    let before = rig.published();
    let active = before.fence_rechecks(now).unwrap();
    assert!(active
        .iter()
        .any(|row| row.kind == "provider" && row.provider == "healing-provider"));
    assert!(!active
        .iter()
        .any(|row| row.kind == "route" && row.origin_key == "[\"healing-provider/inherited\"]"));
    assert!(
        active.iter().any(
            |row| row.kind == "route" && row.origin_key == "[\"healing-provider/independent\"]"
        ),
        "different evidence never becomes a hidden inherited copy"
    );
    assert_eq!(before, rig.fresh());
    assert!(active
        .iter()
        .any(|row| row.applies("healing-provider", "", "", "new")));
    assert!(!before.block_rows().any(
        |row| row.block.kind == "route" && row.block.origin_key == "[\"healing-provider/new\"]"
    ));
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.heal_serving_fences(
                "healing-provider",
                "account",
                "credential",
                "inherited",
                None,
                now + 1,
                now + 2,
            )
        })
        .unwrap();
    let healed = rig.published();
    assert_eq!(healed, rig.fresh());
    let active = healed.fence_rechecks(now + 2).unwrap();
    assert!(!active
        .iter()
        .any(|row| row.applies("healing-provider", "", "", "new")));
    assert!(!active
        .iter()
        .any(|row| row.kind == "provider" && row.provider == "healing-provider"));
    assert!(!active
        .iter()
        .any(|row| row.kind == "route" && row.origin_key == "[\"healing-provider/inherited\"]"));
    assert!(active
        .iter()
        .any(|row| row.kind == "route" && row.origin_key == "[\"healing-provider/independent\"]"));
    rig.store.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT cooldown_until,quota_exhausted_until,wall_class FROM model_route_state WHERE route_key='healing-provider/new'",[],|row|Ok((row.get::<_,Option<i64>>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,Option<String>>(2)?)))?,(None,None,None));
        assert_eq!(conn.query_row("SELECT count(*) FROM block WHERE provider='healing-provider' AND source_present=1",[],|row|row.get::<_,i64>(0))?,1);
        assert_eq!(conn.query_row("SELECT source_present FROM block WHERE kind='route' AND origin_key='[\"healing-provider/inherited\"]'",[],|row|row.get::<_,bool>(0))?,false);
        Ok(())
    }).unwrap();
}

#[test]
fn provider_status_term_matches_frozen_raw_read_in_five_cases() {
    let scratch = cortexkit_test_support::ScratchDir::new("provider-status-five-cases");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "provider-status-five-cases",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.with_writer_for_test(|conn| {
        for key in ["status-active","status-healed","status-partial","status-credential","status-claimed"] {
            let deadline=if key=="status-claimed" {now-2} else {now+2000};
            let error=if key=="status-credential" {r#"{"provider":{"class":"permanent","provider_code":"ENTITLEMENT_ERROR","credential_id":"apikey:credential"}}"#} else {"provider capacity"};
            conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at)
                VALUES (?1,?2,?3,?4,?5,?3,?3)",rusqlite::params![key,deadline,now-5000,key!="status-credential",error])?;
        }
        fence_rechecks::legacy::sync_at(conn,now-10)?;
        conn.execute("UPDATE provider_cooldown_state SET cooldown_until=NULL WHERE provider_key='status-healed'",[])?;
        conn.execute("UPDATE fence_recheck SET cleared_at=?1 WHERE kind='provider' AND provider='status-healed'",[now-3])?;
        conn.execute("INSERT INTO fence_recheck_route_clear(kind,origin_key,provider,model,evidence) SELECT kind,origin_key,provider,'model',evidence FROM fence_recheck WHERE kind='provider' AND provider='status-partial'",[])?;
        assert_eq!(conn.execute("UPDATE fence_recheck SET last_claimed_at=?1,next_recheck_at=?2,claim_generation=1,claim_owner='late-owner' WHERE kind='provider' AND provider='status-claimed'",rusqlite::params![now-1,now-1+3_600_000])?,1);
        let cases=[("status-active",true),("status-healed",false),("status-partial",true),("status-credential",false),("status-claimed",false)];
        let expected=cases.iter().map(|(key,term)| {
            let raw=crate::route_snapshot::legacy_read_provider_cooldown(conn,key).unwrap();
            let cooled=raw.as_ref().is_some_and(|row|row.cooldown_until.is_some_and(|at|at>now as f64)||row.quota_exhausted_until.is_some_and(|at|at>now as f64));
            assert_eq!(cooled,*term,"the frozen raw term for {key}");
            raw
        }).collect::<Vec<_>>();
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state=BlockState::load(conn)?;
        assert_eq!(state.fence_rechecks(now).unwrap().into_iter().find(|row|row.kind=="provider" && row.provider=="status-partial").unwrap().cleared_models,vec!["model"]);
        for ((key,term),expected) in cases.into_iter().zip(expected) {
            let published=state.provider_cooldown(key).unwrap();
            let raw=crate::route_snapshot::read_provider_cooldown(conn,key).unwrap();
            assert_eq!(published,raw,"independent durable reader for {key}");
            assert_eq!(raw,expected,"frozen raw provider answer for {key}");
            let cooled=published.as_ref().is_some_and(|row|row.cooldown_until.is_some_and(|at|at>now as f64)||row.quota_exhausted_until.is_some_and(|at|at>now as f64));
            assert_eq!(cooled,term,"published provider term for {key}");
        }
        Ok(())
    }).unwrap();
}

#[test]
fn native_mixed_writers_keep_frozen_columns_null_and_preserve_independent_route_evidence() {
    use striatum_core::{RouteStateStore, RouteStateUpdate};
    let rig = crate::block_state::tests::rig_before_block_migration("native-mixed-writers");
    rig.store
        .with_writer_for_test(|conn| {
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)
        })
        .unwrap();
    rig.reload();
    let now = crate::now_ms();
    rig.store
        .record_provider_cooldown(
            "writer",
            "writer/inherited",
            "writer/inherited",
            &RouteStateUpdate {
                cooldown_until: Some((now + 60_000) as f64),
                last_error: Some("provider capacity".into()),
                unscoped_wall: true,
                observed_at_ms: Some(now),
                ..Default::default()
            },
        )
        .unwrap();
    rig.store
        .update_route_state(
            "writer/independent",
            "writer/independent",
            &RouteStateUpdate {
                cooldown_until: Some((now + 45_000) as f64),
                last_error: Some("independent capacity".into()),
                unscoped_wall: true,
                observed_at_ms: Some(now - 10),
                ..Default::default()
            },
        )
        .unwrap();
    let independent = rig
        .published()
        .fence_rechecks(now)
        .unwrap()
        .into_iter()
        .find(|row| row.origin_key == "[\"writer/independent\"]")
        .unwrap();
    rig.store
        .record_provider_cooldown(
            "writer",
            "writer/inherited",
            "writer/inherited",
            &RouteStateUpdate {
                quota_pressure: Some(0.5),
                observed_at_ms: Some(now + 1),
                ..Default::default()
            },
        )
        .unwrap();
    let state = rig.published();
    assert_eq!(state, rig.fresh());
    assert_eq!(
        state
            .fence_rechecks(now + 1)
            .unwrap()
            .into_iter()
            .find(|row| row.origin_key == "[\"writer/independent\"]")
            .unwrap(),
        independent
    );
    rig.store.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT cooldown_until,quota_exhausted_until,wall_class,wall_unscoped FROM provider_cooldown_state WHERE provider_key='writer'",[],|row|Ok((row.get::<_,Option<i64>>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,bool>(3)?)))?,(None,None,None,false));
        assert_eq!(conn.query_row("SELECT count(*) FROM model_route_state WHERE provider_id='writer' AND (cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL OR wall_class IS NOT NULL OR wall_unscoped!=0)",[],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(state.provider_cooldown("writer").unwrap(),crate::route_snapshot::read_provider_cooldown(conn,"writer").unwrap());
        assert_eq!(state.route_state_snapshot((now+1) as f64,now+1).unwrap(),crate::route_snapshot::read_snapshot(conn,(now+1) as f64,now+1).unwrap());
        Ok(())
    }).unwrap();
}
