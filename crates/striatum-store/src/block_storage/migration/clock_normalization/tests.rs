#[test]
fn registered_store_copy_normalizes_absent_and_record_only_clocks_without_touching_live_or_observation_history(
) {
    let scratch = cortexkit_test_support::ScratchDir::new("clock-normalization-store-copy");
    let descriptor = crate::dev_descriptor(
        "clock-normalization-store-copy",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = crate::SqliteRouteStateStore::open_at_version_for_test(&descriptor, 93).unwrap();
    let now = crate::now_ms();
    let at = now - 100;
    let cleared = now - 25;
    let (before,expected)=old.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO credential_entitlement_fences VALUES ('normalization-live','live-credential','ENTITLEMENT_ERROR',NULL,?1)",[at])?;
        conn.execute("INSERT INTO account_route_fence VALUES ('normalization-record','record-credential','model',?1,'record-receipt',?2,?2,'quota')",rusqlite::params![now+600_000,at])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_unscoped,last_error,wall_observed_at,updated_at)
            VALUES ('normalization-orphan/model','normalization-orphan/model','normalization-orphan','model',?1,1,'HTTP 503 upstream unavailable',?2,?2)",rusqlite::params![now+600_000,at])?;
        crate::fence_rechecks::sync_at(conn,now)?;
        conn.execute("UPDATE fence_recheck SET last_claimed_at=?1,claim_generation=7,claim_owner='retained-owner'",[now-50])?;
        conn.execute("UPDATE account_route_fence SET wall_class='transient' WHERE provider='normalization-record'",[])?;
        // Import a record-only clock that still names the current source's
        // signature. It must not become a block merely because it has history.
        conn.execute("UPDATE fence_recheck SET evidence=?1 WHERE kind='account_route'",
            [serde_json::json!(["transient",now+600_000,at]).to_string()])?;
        conn.execute("UPDATE model_route_state SET cooldown_until=NULL WHERE route_key='normalization-orphan/model'",[])?;
        conn.execute("UPDATE fence_recheck SET cleared_at=?1 WHERE kind IN ('account_route','route')",[cleared])?;
        conn.execute("INSERT INTO fence_recheck(kind,origin_key,provider,account_id,evidence,evidence_at,quota,next_recheck_at,last_claimed_at,claim_generation,claim_owner)
            VALUES ('observation','[\"normalization-observation\",\"account\"]','normalization-observation','account','observation-evidence',?1,0,?2,?3,9,'observation-owner')",
            rusqlite::params![at,now+600_000,now-50])?;
        let expected=conn.query_row("SELECT count(*) FROM fence_recheck r WHERE r.kind!='observation' AND NOT EXISTS
            (SELECT 1 FROM fence_origin o WHERE o.kind=r.kind AND o.origin_key=r.origin_key)",[],|row|row.get::<_,i64>(0))?;
        assert_eq!(expected,2,"the old copy has one absent source and one record-only source");
        Ok((crate::FrozenBlockData::before_migration(conn)?,expected))
    }).unwrap();
    drop(old);
    // Normal open runs the registered chain on the on-disk copy, not a custom
    // test-only variant of the block migration.
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    let (normalized,remaining)=store.with_reader_for_test(|conn| {
        let normalized=conn.query_row("SELECT count(*) FROM block b JOIN fence_recheck r
            ON r.kind=b.kind AND r.origin_key=b.origin_key
            WHERE b.kind!='observation' AND (b.source_present=0 OR b.is_origin=0)
              AND (r.evidence=b.evidence OR r.last_claimed_at IS NOT NULL)",[],|row|row.get::<_,i64>(0))?;
        let remaining=conn.query_row("SELECT count(*) FROM block WHERE kind!='observation' AND (source_present=0 OR is_origin=0)
            AND (managed=1 OR claim_owner IS NOT NULL OR last_claimed_at IS NOT NULL OR claim_generation!=0)",[],|row|row.get::<_,i64>(0))?;
        assert_eq!(normalized,expected,"count the mapped legacy clocks requiring normalization, not all record-only rows");
        assert_eq!(remaining,0);
        let audit_preserved=conn.query_row("SELECT count(*) FROM block b JOIN fence_recheck r ON r.kind=b.kind AND r.origin_key=b.origin_key
            WHERE b.kind IN ('account_route','route') AND b.evidence=r.evidence AND b.cleared_at=r.cleared_at",[],|row|row.get::<_,i64>(0))?;
        assert_eq!(audit_preserved,2,"dropping a clock must retain its evidence and cleared receipt time");
        for (provider,generation,owner) in [("normalization-live",7,"retained-owner"),("normalization-observation",9,"observation-owner")] {
            let actual=conn.query_row("SELECT managed,last_claimed_at,claim_generation,claim_owner FROM block WHERE provider=?1",[provider],|row|
                Ok((row.get::<_,i64>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,i64>(2)?,row.get::<_,Option<String>>(3)?)))?;
            assert_eq!(actual,(1,Some(now-50),generation,Some(owner.into())),"live origin and observation exception keep their history");
        }
        Ok((normalized,remaining))
    }).unwrap();
    println!(
        "clock normalization rehearsal: rows_normalized={normalized}, remaining_stale={remaining}"
    );
    store.assert_canonical_block_clocks_for_test().unwrap();
    let loaded = store.with_reader_for_test(crate::BlockState::load).unwrap();
    assert!(loaded
        .clock(
            "account_route",
            "[\"normalization-record\",\"record-credential\",\"model\"]"
        )
        .is_none());
    assert!(loaded
        .clock("route", "[\"normalization-orphan/model\"]")
        .is_none());
    assert_eq!(
        loaded
            .clock("credential", "[\"normalization-live\",\"live-credential\"]")
            .unwrap()
            .0
            .claim_generation,
        7
    );
    assert_eq!(
        loaded
            .clock("observation", "[\"normalization-observation\",\"account\"]")
            .unwrap()
            .0
            .claim_generation,
        9
    );
    store
        .frozen_block_data_for_test()
        .unwrap()
        .assert_unchanged(&before);
}
