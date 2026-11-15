//! The generated origin flag is shared by loaders, counts and probe compare-and-swap operations.
use super::*;

fn origin_store(
    name: &str,
    class: &str,
    quota: bool,
) -> (
    cortexkit_test_support::ScratchDir,
    SqliteRouteStateStore,
    i64,
    FenceRecheck,
) {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy()),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    let class = class.to_owned();
    let origin=store.with_writer_for_test(move |conn| {
        conn.execute("INSERT INTO account_route_fence(provider,credential_id,model,until_ms,decision_id,recorded_at,last_reported_at,wall_class)
            VALUES ('origin-rule','credential','model',?1,'record',?2,?2,?3)",params![now+60_000,now-1000,class])?;
        conn.execute("INSERT INTO fence_recheck(kind,origin_key,provider,credential_id,model,evidence,evidence_at,quota,next_recheck_at)
            SELECT 'account_route',json_array(provider,credential_id,model),provider,credential_id,model,
                json_array(wall_class,until_ms,last_reported_at),last_reported_at,?1,?2 FROM account_route_fence",params![quota,now-1])?;
        assert_eq!(legacy::read_effective_at(conn,now)?.len(),usize::from(class=="quota"),"the pre-migration oracle fixes the expected membership");
        crate::block_storage::install_migration_function(conn)?;
        conn.execute_batch(crate::block_storage::MIGRATION)?;
        let row=crate::block_storage::read(conn)?.into_iter().find(|row|row.block.kind=="account_route").unwrap();
        Ok(FenceRecheck::from(&row.block))
    }).unwrap();
    (scratch, store, now, origin)
}

#[test]
fn generated_origin_flag_pins_every_kind_and_class() {
    let (_scratch, store, now, _) = origin_store("origin-rule-table", "transient", false);
    store.with_writer_for_test(move |conn| {
        let kinds=["account","credential","account_route","non_quota_route","transient_route","pool_billing","pool_rate","provider","route","failure","size","non_quota_size","principal","observation"];
        let classes=[None,Some(""),Some("quota"),Some("transient"),Some("billing"),Some("non_quota"),Some("quota "),Some("QUOTA"),Some("unknown")];
        for kind in kinds {
            for (index,class) in classes.into_iter().enumerate() {
                let key=serde_json::json!([kind,index]).to_string();
                conn.execute("INSERT INTO block(kind,origin_key,provider,evidence,evidence_at,quota,next_recheck_at,wall_class)
                    VALUES (?1,?2,'table-rule','evidence',?3,0,?3,?4)",params![kind,key,now,class])?;
                let generated:bool=conn.query_row("SELECT is_origin FROM block WHERE kind=?1 AND origin_key=?2",params![kind,key],|row|row.get(0))?;
                // All source kinds are origins except legacy account-route
                // records whose typed class is not exactly the quota class.
                let expected=match (kind,class) { ("account_route",Some("quota"))=>true,("account_route",_)=>false,_=>true };
                assert_eq!(generated,expected,"kind={kind}, class={class:?}");
            }
        }
        println!("origin rule: {} kinds x {} classes = {} rows",kinds.len(),classes.len(),kinds.len()*classes.len());
        Ok(())
    }).unwrap();
}

#[test]
fn imported_record_only_rows_are_not_loaded_as_origins() {
    let (_scratch, store, now, _) = origin_store("record-only-loader", "transient", false);
    store
        .with_reader_for_test(move |conn| {
            let state = crate::BlockState::load(conn)?;
            let (_, class, reported) = state
                .account_route_fence("origin-rule", "credential", "model")
                .expect("the record remains readable by its source key");
            assert_eq!(class.as_deref(), Some("transient"));
            assert_eq!(reported, Some(now - 1000));
            assert_eq!(
                state.route_fence_latest_reports().get("credential"),
                Some(&(now - 1000))
            );
            assert!(state.blocks(now).unwrap().blocks.is_empty());
            assert!(state.fence_rechecks(now).unwrap().is_empty());
            assert!(state.clock_rechecks().is_empty());
            assert!(read(conn)?.is_empty());
            assert!(read_effective_at(conn, now)?.is_empty());
            Ok(())
        })
        .unwrap();
}

#[test]
fn imported_record_only_rows_do_not_count_as_active_blocks() {
    let (_scratch, store, _, _) = origin_store("record-only-count", "transient", false);
    store
        .with_reader_for_test(|conn| {
            let stored: i64 = conn.query_row("SELECT count(*) FROM block", [], |row| row.get(0))?;
            let active: i64 = conn.query_row(
                "SELECT count(*) FROM block WHERE cleared_at IS NULL AND is_origin=1",
                [],
                |row| row.get(0),
            )?;
            let records: i64 =
                conn.query_row("SELECT count(*) FROM block WHERE is_origin=0", [], |row| {
                    row.get(0)
                })?;
            assert_eq!((stored, active, records), (1, 0, 1));
            Ok(())
        })
        .unwrap();
}

#[test]
fn imported_record_only_rows_cannot_authorize_a_probe() {
    let (_scratch, store, now, origin) = origin_store("record-only-claim", "transient", false);
    store
        .with_writer_for_test(move |conn| {
            assert!(!claim(conn, &[origin.clone()], now)?);
            assert_eq!(
                conn.query_row("SELECT last_claimed_at FROM block", [], |row| row
                    .get::<_, Option<i64>>(0))?,
                None
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn claim_cas_rejects_a_clock_reclassified_as_record_only() {
    let (_scratch, store, now, origin) = origin_store("record-only-stale-cas", "quota", true);
    store
        .with_writer_for_test(move |conn| {
            let state = crate::BlockState::load(conn)?;
            assert_eq!(state.blocks(now).unwrap().blocks.len(), 1);
            conn.execute(
                "UPDATE block SET wall_class='transient' WHERE kind='account_route'",
                [],
            )?;
            assert!(!conn.query_row("SELECT is_origin FROM block", [], |row| row
                .get::<_, bool>(0))?);
            conn.execute_batch("SAVEPOINT claim_guard")?;
            let result = claim_decided(
                conn,
                Some(&state),
                &[origin.clone()],
                now,
                None,
                Some("guard-owner"),
                Some("guard-proof"),
            );
            conn.execute_batch("ROLLBACK TO claim_guard; RELEASE claim_guard")?;
            assert!(
                result
                    .as_ref()
                    .is_err_and(|error| error.to_string().contains(crate::STATE_DIVERGED)),
                "the clock CAS must reject a row that ceased to be an origin: {result:?}"
            );
            assert_eq!(
                conn.query_row("SELECT count(*) FROM probe_authorization", [], |row| row
                    .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
}
