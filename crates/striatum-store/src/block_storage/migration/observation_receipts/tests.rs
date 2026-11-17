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
