use super::*;

#[test]
fn pool_deadlines_reject_real_storage_and_preserve_integer_precision() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-pool-deadlines");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-pool-deadlines",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,wall_observed_at,updated_at) VALUES ('precision','credential',?1,'billing',1,1)",[i64::MAX])?;
        conn.execute("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,wall_observed_at,updated_at) VALUES ('precision','odd-credential',?1,'billing',1,1)",[9_007_199_254_740_993_i64])?;
        let legacy = crate::block_state::legacy::BlockState::load(conn)?;
        let mut expected = legacy.pool_key_rows("precision")?;
        expected.sort();
        assert_eq!(expected,vec![("credential".to_owned(),None,Some(i64::MAX),None),("odd-credential".to_owned(),None,Some(9_007_199_254_740_993_i64),None)]);
        conn.execute_batch("SAVEPOINT fractional; UPDATE pool_key_state SET walled_until=1.5 WHERE provider='precision'")?;
        assert!(crate::block_state::legacy::BlockState::load(conn)?.pool_key_rows("precision").is_err(),"the old raw reader rejects non-integer storage");
        conn.execute_batch("ROLLBACK TO fractional; RELEASE fractional")?;
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        let state = BlockState::load(conn)?;
        let mut actual = state.pool_key_rows("precision")?;
        actual.sort();
        assert_eq!(actual,expected,"pool timestamps never pass through f64");
        let error = conn.execute("UPDATE block SET deadline=1.5 WHERE kind='pool_billing'",[]).unwrap_err();
        assert!(matches!(error,rusqlite::Error::SqliteFailure(ref failure,_) if failure.code == rusqlite::ErrorCode::ConstraintViolation));
        conn.execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE block SET deadline=1.5 WHERE kind='pool_billing'")?;
        assert!(matches!(BlockState::load(conn),Err(rusqlite::Error::InvalidColumnType(_,_,rusqlite::types::Type::Real))),"the loader rejects a malformed row even if an external writer bypassed the constraint");
        Ok(())
    }).unwrap();
}
