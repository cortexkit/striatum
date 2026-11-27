use super::*;

#[test]
fn changed_evidence_migration_preserves_generation_and_rejects_an_aba_stale_proof() {
    let scratch = cortexkit_test_support::ScratchDir::new("migration-aba-generation");
    let descriptor = crate::dev_descriptor(
        "migration-aba-generation",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    let now = crate::now_ms() + 60_000;
    old.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO account_route_fence VALUES ('aba-p','aba-credential','aba-model',?1,'aba-decision',?2,?2,'quota')",
            rusqlite::params![now+60000,now-1000])?;
        crate::fence_rechecks::legacy::sync_at(conn,now)?;
        conn.execute("UPDATE fence_recheck SET claim_generation=1,claim_owner=NULL,last_claimed_at=?1,next_recheck_at=?2,reset_check_at=NULL
            WHERE kind='account_route'",rusqlite::params![now+60001,now+960001])?;
        conn.execute("INSERT INTO fence_probe_authorization SELECT 'old-aba-owner',kind,origin_key,evidence,evidence_at,?1,claim_generation
            FROM fence_recheck WHERE kind='account_route'",[now+60001])?;
        // Reconciliation changes the fence's deadline without changing its
        // report timestamp. The fence_recheck row still contains the earlier
        // deadline's evidence signature when the upgrade starts.
        conn.execute("UPDATE account_route_fence SET until_ms=?1 WHERE provider='aba-p'",[now+120000])?;
        Ok(())
    }).unwrap();
    drop(old);
    let native = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    let back_to_a = crate::AccountRouteFence {
        provider: "aba-p".into(),
        credential_id: "aba-credential".into(),
        model: "aba-model".into(),
        until_ms: now + 120000,
        decision_id: "aba-decision".into(),
        recorded_at: now - 1000,
    };
    let probe_at = now + 1800001;
    native
        .account_transaction(|tx| {
            tx.reconcile_account_route_fence(&back_to_a, now + 60000, now - 1000)?;
            tx.sync_fence_rechecks(crate::now_ms())?;
            let origins = tx.load_block_state()?.fence_rechecks(probe_at)?;
            assert_eq!(origins.len(), 1);
            assert!(tx.grant_fence_rechecks(&origins, probe_at, Some("new-aba-owner"))?);
            Ok(())
        })
        .unwrap();
    native
        .account_read(|tx| {
            let blocks = tx.load_block_state()?;
            let origins = blocks.fence_rechecks(probe_at)?;
            assert_eq!(
                blocks
                    .clock("account_route", &origins[0].origin_key)
                    .unwrap()
                    .0
                    .claim_generation,
                2,
                "a new claim must advance the pre-upgrade generation, never reuse it"
            );
            let memory = blocks.probe_authorizations_for_origins(&origins);
            let sql = tx.probe_authorizations_for_origins(&origins)?;
            assert_eq!(
                memory, sql,
                "all proof fields agree after the real A to B to A transition"
            );
            assert_eq!(
                memory.len(),
                1,
                "the old matching-evidence proof remains a stale generation"
            );
            assert_eq!(memory[0].owner, "new-aba-owner");
            assert_eq!(memory[0].claim_generation, 2);
            Ok(())
        })
        .unwrap();
}
