use rusqlite::OptionalExtension;

use super::*;

fn effects(until: i64) -> String {
    serde_json::json!([{"effect":"transient_backoff","provider":"receipt-p","account_id":"receipt-a","retry_at_ms":until}]).to_string()
}

// Seed the complete old source and a real observation before normal registry
// opening upgrades the database. Receipt times stay independent of clock time.
fn migrated(
    claimed: bool,
) -> (
    cortexkit_test_support::ScratchDir,
    SqliteRouteStateStore,
    i64,
    String,
) {
    let scratch = cortexkit_test_support::ScratchDir::new("observation-receipt-upgrade");
    let descriptor = crate::dev_descriptor(
        "observation-receipt-upgrade",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    let now = crate::now_ms() + 60_000;
    let body = effects(now + 60_000);
    old.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO account_observation(observation_id,principal,runner,event_id,kind,admission_id,bound,observation_json,effects_json,received_at)
            VALUES ('new-receipt','receipt-principal','runner','new-event','transient_backoff',NULL,1,'{}',?1,?2)",rusqlite::params![body,now-1000])?;
        conn.execute("INSERT INTO account_observation(observation_id,principal,runner,event_id,kind,admission_id,bound,observation_json,effects_json,received_at)
            VALUES ('old-receipt','receipt-principal','runner','old-event','transient_backoff',NULL,0,'{}','[]',?1)",[now-2000])?;
        conn.execute("INSERT INTO account_transient_backoff(provider,account_id,retry_at_ms,evidence_at,evidence_key,effects_json)
            SELECT 'receipt-p','receipt-a',?1,?2,json_array('new-receipt',0),json_extract(?3,'$[0]')",
            rusqlite::params![now+60000,now-1000,body])?;
        crate::fence_rechecks::legacy::sync_at(conn, now)?;
        if claimed {
            conn.execute("UPDATE fence_recheck SET claim_owner='receipt-owner',claim_generation=7,last_claimed_at=?1,next_recheck_at=?2,evidence_at=?3
                WHERE kind='observation'",rusqlite::params![now+60001,now+3660001,now-4000])?;
            conn.execute("INSERT INTO fence_probe_authorization SELECT 'receipt-owner',kind,origin_key,evidence,evidence_at,?1,claim_generation
                FROM fence_recheck WHERE kind='observation'",[now+60001])?;
        }
        Ok(())
    }).unwrap();
    drop(old);
    (
        scratch,
        SqliteRouteStateStore::open(&descriptor).unwrap(),
        now,
        body,
    )
}

fn physical(store: &SqliteRouteStateStore) -> Vec<StoredBlock> {
    store.with_reader_for_test(read).unwrap()
}

#[test]
fn migrated_observation_receipt_rejects_delayed_older_effects() {
    let (_scratch, store, now, _body) = migrated(true);
    let before = physical(&store);
    let older = effects(now + 1000);
    store
        .account_transaction(|tx| tx.set_observation_effects("old-receipt", true, &older))
        .unwrap();
    assert_eq!(
        physical(&store),
        before,
        "an older receipt cannot replace any source, clock, owner or generation field"
    );
    assert_eq!(
        physical(&store)[0].observed_at,
        Some(now - 1000),
        "source receipt survives independently of the claimed clock"
    );
}

#[test]
fn migrated_identical_observation_effect_keeps_authorization_and_can_heal() {
    let (_scratch, store, now, body) = migrated(true);
    let before = physical(&store);
    let authorizations = store
        .account_read(|tx| {
            Ok(tx
                .load_block_state()?
                .authorizations()
                .cloned()
                .collect::<Vec<_>>())
        })
        .unwrap();
    store
        .account_transaction(|tx| tx.set_observation_effects("new-receipt", true, &body))
        .unwrap();
    assert_eq!(
        physical(&store),
        before,
        "identical effect reapplication keeps every migrated source/clock field"
    );
    assert_eq!(
        store
            .account_read(|tx| Ok(tx
                .load_block_state()?
                .authorizations()
                .cloned()
                .collect::<Vec<_>>()))
            .unwrap(),
        authorizations
    );
    store
        .account_transaction(|tx| {
            tx.heal_authorized_probe("receipt-owner", "receipt-p", "model", now + 60003)
        })
        .unwrap();
    store
        .with_reader_for_test(|conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM block WHERE kind='observation' AND source_present=1",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM retirement_tombstone WHERE kind='observation'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                1
            );
            assert_eq!(
                conn.query_row("SELECT count(*) FROM probe_authorization", [], |r| r
                    .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn changed_observation_effect_at_the_same_key_is_new_evidence_after_upgrade() {
    let (_scratch, store, now, body) = migrated(true);
    store
        .account_transaction(|tx| tx.set_observation_effects("new-receipt", true, &body))
        .unwrap();
    let before = physical(&store).remove(0);
    let changed = effects(now + 120000);
    store
        .account_transaction(|tx| tx.set_observation_effects("new-receipt", true, &changed))
        .unwrap();
    let after = physical(&store).remove(0);
    let expected = store.with_reader_for_test(|conn| conn.query_row(
        "SELECT json_array(evidence_key,json_extract(?1,'$[0]') || '') FROM account_transient_backoff WHERE provider='receipt-p' AND account_id='receipt-a'",
        [&changed],|row|row.get::<_,String>(0))).unwrap();
    assert_ne!(
        after.block.evidence, before.block.evidence,
        "effect contents participate in the evidence signature"
    );
    assert_eq!(
        after.block.evidence, expected,
        "same TEXT evidence-key/effect-body signature as the old origin view"
    );
    assert_eq!(
        after.block.claim_owner, None,
        "new evidence cannot keep the old proof owner"
    );
    assert_eq!(after.deadline, Some(Deadline::Integer(now + 120000)));
}

fn migrated_orphan(
    cleared: bool,
) -> (
    cortexkit_test_support::ScratchDir,
    SqliteRouteStateStore,
    i64,
    String,
) {
    let scratch = cortexkit_test_support::ScratchDir::new("orphan-observation-receipt");
    let descriptor = crate::dev_descriptor(
        "orphan-observation-receipt",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    let receipt = crate::now_ms() + 50_000;
    let body = effects(receipt + 60_000);
    old.with_writer_for_test(|conn| {
        for (id,at,bound,contents) in [("prior-observation",receipt,1,body.as_str()),("older-orphan",receipt-1000,0,"[]")] {
            conn.execute("INSERT INTO account_observation(observation_id,principal,runner,event_id,kind,bound,observation_json,effects_json,received_at)
                VALUES (?1,'receipt-principal','runner',?1,'transient_backoff',?3,'{}',?4,?2)",rusqlite::params![id,at,bound,contents])?;
        }
        conn.execute("INSERT INTO fence_recheck(kind,origin_key,provider,account_id,evidence,evidence_at,quota,classified,next_recheck_at,last_claimed_at,claim_generation,claim_owner,cleared_at)
            VALUES ('observation',json_array('receipt-p','receipt-a'),'receipt-p','receipt-a',json_array(json_array('prior-observation',0)||'',json_extract(?1,'$[0]')||''),?2,0,0,?3,?4,7,'orphan-owner',?5)",
            rusqlite::params![body,receipt,receipt+60000,receipt+1000,cleared.then_some(receipt+2000)])?;
        conn.execute("INSERT INTO fence_probe_authorization SELECT 'orphan-owner',kind,origin_key,evidence,evidence_at,?1,claim_generation FROM fence_recheck WHERE kind='observation'",[receipt+1000])?;
        assert_eq!(conn.query_row("SELECT count(*) FROM account_transient_backoff",[],|r|r.get::<_,i64>(0))?,0);
        Ok(())
    }).unwrap();
    drop(old);
    (
        scratch,
        SqliteRouteStateStore::open(&descriptor).unwrap(),
        receipt,
        body,
    )
}

#[test]
fn migrated_sparse_observation_clock_rejects_an_older_receipt_without_a_source_row() {
    let (_scratch, store, receipt, _body) = migrated_orphan(false);
    let before = physical(&store);
    assert!(!before[0].source_present);
    assert_eq!(before[0].block.claim_owner.as_deref(), Some("orphan-owner"));
    let older = effects(receipt + 1000);
    store
        .account_transaction(|tx| tx.set_observation_effects("older-orphan", true, &older))
        .unwrap();
    assert_eq!(
        physical(&store),
        before,
        "a sparse clock's receipt protects every source, scheduling and ownership field"
    );
    assert_eq!(
        before[0].observed_at,
        Some(receipt),
        "the last known receipt must be durable even after the source lookback expires"
    );
}

#[test]
fn migrated_cleared_orphan_observation_retires_and_spends_proofs_before_replay() {
    let (_scratch, store, receipt, body) = migrated_orphan(true);
    store.with_reader_for_test(|conn| {
        let key=serde_json::json!(["receipt-p","receipt-a"]).to_string();
        let row=conn.query_row("SELECT evidence_at,retired_at FROM retirement_tombstone WHERE kind='observation' AND origin_key=?1",[&key],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?))).optional()?;
        assert_eq!(row,Some((receipt,receipt+2000)),"a cleared orphan needs the same receipt replay barrier as any retired observation");
        assert_eq!(conn.query_row("SELECT count(*) FROM probe_authorization WHERE kind='observation'",[],|r|r.get::<_,i64>(0))?,0);
        Ok(())
    }).unwrap();
    let before = physical(&store);
    for (id, contents) in [
        ("older-orphan", effects(receipt + 1000)),
        ("prior-observation", body),
    ] {
        store
            .account_transaction(|tx| tx.set_observation_effects(id, true, &contents))
            .unwrap();
        assert_eq!(physical(&store),before,"neither a different older receipt nor the identical cleared effect may recreate the source");
        assert!(store
            .account_read(|tx| Ok(tx
                .load_block_state()?
                .fence_rechecks(receipt + 3000)?
                .is_empty()))
            .unwrap());
    }
}
