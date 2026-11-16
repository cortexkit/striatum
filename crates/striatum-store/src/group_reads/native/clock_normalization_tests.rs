use super::native_rig;
use crate::{AccountCommandKind, AccountObservationRow};

fn parity(rig: &crate::block_state::tests::Rig) {
    // Bounded-state equality does not see dropped rows. Check their stored
    // clock fields separately, as test/maintenance validation, not traffic.
    rig.store.assert_canonical_block_clocks_for_test().unwrap();
    assert_eq!(rig.published(), rig.fresh());
}

#[test]
fn source_clock_normalization_counts_real_transitions_not_identical_evidence_rewrites() {
    let rig = native_rig("clock-drop-transition");
    let now = crate::now_ms();
    let refusal = striatum_core::route_state_store::CredentialRefusal {
        provider_id: "normalization".into(),
        credential_id: "credential".into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    rig.store
        .record_credential_entitlement_fence(&refusal, None, now - 900_001)
        .unwrap();
    parity(&rig);
    rig.store
        .account_command(AccountCommandKind::Repair, move |tx| {
            assert!(tx.classify_credential_quota("normalization", "credential", true)?);
            let origins = tx.fence_rechecks()?;
            assert_eq!(origins.len(), 1);
            assert!(tx.grant_fence_rechecks(&origins, now, Some("live-claim"))?);
            Ok(())
        })
        .unwrap();
    parity(&rig);
    let held = rig.published();
    let key = "[\"normalization\",\"credential\"]";
    let prior = held.clock("credential", key).unwrap().0.clone();
    assert_eq!(prior.last_claimed_at, Some(now));
    assert_eq!(prior.claim_owner.as_deref(), Some("live-claim"));
    assert!(prior.claim_generation > 0);
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    rig.store
        .record_credential_entitlement_fence(&refusal, None, now - 900_001)
        .unwrap();
    parity(&rig);
    let reads = trace.take();
    assert_eq!(
        reads.clock_normalizations, 0,
        "an identical live source never drops claim history: {reads:#?}"
    );
    assert!(
        reads.clock_normalization_reads.is_empty(),
        "an unexecuted WHEN body has no keyed lookup: {reads:#?}"
    );
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "{reads:#?}"
    );
    assert_eq!(rig.published().clock("credential", key).unwrap().0, &prior);
    rig.store
        .account_command(AccountCommandKind::Repair, |tx| {
            tx.clear_credential_entitlement_fence("normalization", "credential")
        })
        .unwrap();
    parity(&rig);
    let reads = trace.take();
    assert_eq!(
        reads.clock_normalizations, 1,
        "one actual source transition must normalize exactly once: {reads:#?}"
    );
    assert_eq!(
        reads.clock_normalization_reads.len(),
        1,
        "normalization has its own reviewed keyed-read category: {reads:#?}"
    );
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "{reads:#?}"
    );
    assert!(rig.published().clock("credential", key).is_none());
}

#[test]
fn becoming_record_only_drops_the_clock_but_observation_source_removal_keeps_its_history() {
    let rig = native_rig("clock-drop-class-and-observation");
    let now = crate::now_ms();
    let route = crate::AccountRouteFence {
        provider: "normalization".into(),
        credential_id: "route-credential".into(),
        model: "model".into(),
        until_ms: now + 60_000,
        recorded_at: now - 900_001,
        decision_id: "route-receipt".into(),
    };
    rig.store.record_account_route_fence(&route).unwrap();
    parity(&rig);
    rig.store
        .account_command(AccountCommandKind::Repair, move |tx| {
            let origins = tx.fence_rechecks()?;
            assert_eq!(origins.len(), 1);
            assert!(tx.grant_fence_rechecks(&origins, now, Some("route-claim"))?);
            Ok(())
        })
        .unwrap();
    parity(&rig);
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    rig.store
        .record_transient_account_route_fence(&route)
        .unwrap();
    parity(&rig);
    let reads = trace.take();
    assert_eq!(
        reads.clock_normalizations, 0,
        "a transient report must not downgrade a standing quota origin: {reads:#?}"
    );
    let key = "[\"normalization\",\"route-credential\",\"model\"]";
    assert!(rig.published().clock("account_route", key).is_some());
    // The transient recorder deliberately preserves quota evidence. Exercise
    // an actual typed-class loss in the schema through a writer command.
    rig.store.account_command(AccountCommandKind::Repair,move|tx| {
        let changed=tx.conn.execute("UPDATE block SET wall_class='transient' WHERE kind='account_route' AND origin_key=?1",[key])
            .map_err(crate::err)?;
        assert_eq!(changed,1);
        Ok(())
    }).unwrap();
    parity(&rig);
    let reads = trace.take();
    assert_eq!(
        reads.clock_normalizations, 1,
        "an origin becoming a record drops one clock: {reads:#?}"
    );
    assert_eq!(reads.clock_normalization_reads.len(), 1);
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "{reads:#?}"
    );
    assert!(rig.published().fence_rechecks(now).unwrap().is_empty());
    let row = AccountObservationRow {
        observation_id: "clock-drop-observation".into(),
        principal: "normalization".into(),
        runner: "runner".into(),
        event_id: "clock-drop-observation".into(),
        kind: "transient_backoff".into(),
        admission_id: None,
        bound: true,
        observation_json: "{}".into(),
        received_at: now - 900_001,
        effects_json:
            serde_json::json!([{"effect":"transient_backoff","provider":"observation-provider",
            "account_id":"observation-account","retry_at_ms":now+60_000}])
            .to_string(),
    };
    rig.store
        .account_command(AccountCommandKind::Observe, move |tx| {
            tx.insert_observation(&row)
        })
        .unwrap();
    parity(&rig);
    trace.take();
    // Seed the observation's claim metadata without waiting for its backoff.
    // This schema test concerns source-loss history, not probe scheduling.
    rig.store.account_command(AccountCommandKind::Repair,move|tx| {
        let changed=tx.conn.execute("UPDATE block SET last_claimed_at=?1,claim_generation=1,claim_owner='observation-claim'
            WHERE kind='observation' AND source_present=1 AND managed=1",[now]).map_err(crate::err)?;
        assert_eq!(changed,1);
        Ok(())
    }).unwrap();
    parity(&rig);
    trace.take();
    let key = "[\"observation-provider\",\"observation-account\"]";
    let prior = rig.published().clock("observation", key).unwrap().0.clone();
    rig.store
        .account_command(AccountCommandKind::Repair, move |tx| {
            crate::block_storage::remove_source(
                tx.conn,
                "observation",
                key,
                now,
                "sparse source removed",
            )
            .map_err(crate::err)?;
            Ok(())
        })
        .unwrap();
    parity(&rig);
    let reads = trace.take();
    assert_eq!(
        reads.clock_normalizations, 0,
        "observation clocks outlive their sparse source: {reads:#?}"
    );
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "{reads:#?}"
    );
    let state = rig.published();
    let clock = state.clock("observation", key).unwrap().0;
    assert_eq!(clock.last_claimed_at, prior.last_claimed_at);
    assert_eq!(clock.claim_generation, prior.claim_generation);
}

#[test]
fn clock_normalization_trigger_is_only_a_keyed_update_on_real_source_or_origin_loss() {
    let rig = native_rig("clock-drop-schema-pin");
    rig.store.with_reader_for_test(|conn| {
        let actual:String=conn.query_row("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='block_clock_drop'",[],|row|row.get(0))?;
        let expected=format!("CREATE TRIGGER block_clock_drop AFTER UPDATE OF source_present,kind,wall_class ON block
            WHEN NEW.kind!='observation' AND ((OLD.source_present=1 AND NEW.source_present=0) OR
            (OLD.is_origin=1 AND NEW.is_origin=0)) BEGIN {}; END",crate::block_storage::CLOCK_NORMALIZATION_BODY);
        let compact=|sql: &str|sql.chars().filter(|ch|!ch.is_whitespace() && *ch!=';')
            .flat_map(char::to_lowercase).collect::<String>();
        assert_eq!(compact(&actual),compact(&expected),"additional reads or updates need a separate review");
        Ok(())
    }).unwrap();
    parity(&rig);
}
