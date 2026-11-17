use super::*;

#[test]
fn decision_checks_match_legacy_for_clock_source_clear_success_and_scope_changes() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-decision-checks");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-decision-checks",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        fence_rechecks::sync_at(conn,now)?;
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,updated_at) VALUES ('unrelated',?1)",[now])?;
        let legacy = crate::block_state::legacy::BlockState::load(conn)?;
        let credential = legacy.view_origins().into_iter().find(|origin| origin.kind == "credential").unwrap();
        let principal = legacy.view_origins().into_iter().find(|origin| origin.kind == "principal").unwrap();
        let principal_id = principal.principal.as_deref().unwrap();
        let admission = principal.admission_id.as_deref().unwrap();
        conn.execute("VACUUM INTO ?1",[scratch.path().join("migrated.db").to_string_lossy().as_ref()])?;
        let migrated = Connection::open(scratch.path().join("migrated.db"))?;
        block_storage::install_migration_function(&migrated)?;
        migrated.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(&migrated)?;
        state.check_block(&migrated,&credential.kind,&credential.origin_key)?;
        let cases = [
            (
                "UPDATE fence_recheck SET next_recheck_at=next_recheck_at+1 WHERE kind='credential'",
                "UPDATE block SET next_recheck_at=next_recheck_at+1 WHERE kind='credential' AND source_present=1",
                [true,false,false,true,true],
            ),
            (
                "UPDATE credential_entitlement_fences SET listing_fingerprint='changed'",
                "UPDATE block SET source_metadata_json=json_set(source_metadata_json,'$.listing_fingerprint','changed') WHERE kind='credential' AND source_present=1",
                [false,true,false,true,true],
            ),
            (
                "INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'new-model',evidence FROM fence_recheck WHERE kind='credential'",
                "UPDATE block SET cleared_models=json_insert(cleared_models,'$[#]','new-model') WHERE kind='credential' AND source_present=1",
                [true,true,false,true,true],
            ),
            (
                "UPDATE fence_recheck SET next_recheck_at=next_recheck_at+1 WHERE kind='principal'",
                "UPDATE block SET next_recheck_at=next_recheck_at+1 WHERE kind='principal' AND source_present=1",
                [true,true,true,true,false],
            ),
            (
                "UPDATE account_principal_exclusion SET runner_view_digest='changed'",
                "UPDATE block SET source_metadata_json=json_set(source_metadata_json,'$.runner_view_digest','changed') WHERE kind='principal' AND source_present=1",
                [true,true,true,false,true],
            ),
            (
                "INSERT INTO fence_success_pool VALUES ('parity-fence','{}',1,1)",
                "INSERT INTO success_proof VALUES ('parity-fence','{}',1,1)",
                [true,true,false,true,true],
            ),
            (
                "INSERT INTO credential_entitlement_fences(provider_id,credential_id,code,observed_at_ms) VALUES ('cpu-provider-0','unseen','refused',1)",
                "INSERT INTO block(kind,origin_key,provider,credential_id,evidence,evidence_at,quota,next_recheck_at) VALUES ('credential',json_array('cpu-provider-0','unseen'),'cpu-provider-0','unseen','new',1,0,1)",
                [true,true,false,true,true],
            ),
            (
                "UPDATE provider_cooldown_state SET quota_pressure=0.9 WHERE provider_key='unrelated'",
                "UPDATE provider_cooldown_state SET quota_pressure=0.9 WHERE provider_key='unrelated'",
                [true;5],
            ),
        ];
        for (old_change,new_change,expected) in cases {
            conn.execute_batch("SAVEPOINT stale")?;
            migrated.execute_batch("SAVEPOINT stale")?;
            conn.execute_batch(old_change)?;
            migrated.execute_batch(new_change)?;
            assert!(conn.changes()>0 && migrated.changes()>0,"both controls changed a row: {old_change}");
            let old_verdicts = [
                legacy.check_source(conn,&credential.kind,&credential.origin_key,&credential.provider).is_ok(),
                legacy.check_clock(conn,&credential.kind,&credential.origin_key).is_ok(),
                legacy.check_provider_rows(conn,&credential.provider).is_ok(),
                legacy.check_exclusions(conn,principal_id,admission).is_ok(),
                legacy.check_provider_rows(conn,"").is_ok(),
            ];
            let new_verdicts = [
                state.check_source(&migrated,&credential.kind,&credential.origin_key,&credential.provider).is_ok(),
                state.check_clock(&migrated,&credential.kind,&credential.origin_key).is_ok(),
                state.check_provider_rows(&migrated,&credential.provider).is_ok(),
                state.check_exclusions(&migrated,principal_id,admission).is_ok(),
                state.check_provider_rows(&migrated,"").is_ok(),
            ];
            assert_eq!(old_verdicts,expected,"legacy guard contract: {old_change}");
            assert_eq!(new_verdicts,expected,"consolidated guard contract: {new_change}");
            conn.execute_batch("ROLLBACK TO stale; RELEASE stale")?;
            migrated.execute_batch("ROLLBACK TO stale; RELEASE stale")?;
        }
        Ok(())
    }).unwrap();
}
