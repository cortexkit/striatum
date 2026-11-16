#[test]
fn bound_observation_replay_keeps_its_last_effect_and_healed_clock() {
    let rig = super::credential::native_rig("native-observation-replay");
    let now = crate::now_ms();
    let effects=serde_json::json!([
        {"effect":"transient_backoff","provider":"observation-provider","account_id":"account","retry_at_ms":now+1000},
        {"effect":"transient_backoff","provider":"observation-provider","account_id":"account","retry_at_ms":now+2000}
    ]).to_string();
    let observation = crate::AccountObservationRow {
        observation_id: "observation".into(),
        principal: "principal".into(),
        runner: "runner".into(),
        event_id: "event".into(),
        kind: "transient_backoff".into(),
        admission_id: None,
        bound: true,
        observation_json: "{}".into(),
        effects_json: effects.clone(),
        received_at: now - 10,
    };
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.insert_observation(&observation)
        })
        .unwrap();
    let state = rig.published();
    assert_eq!(state, rig.fresh());
    let rows = state.fence_rechecks(now).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].evidence, "[\"observation\",1]");
    assert_eq!(rows[0].next_recheck_at, now + 2000);
    let (key, evidence) = (rows[0].origin_key.clone(), rows[0].evidence.clone());
    let probe = now + 2001;
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            let rows = tx.current_blocks()?.fence_rechecks(probe)?;
            assert!(tx.grant_fence_rechecks(&rows, probe, Some("observation-owner"))?);
            tx.heal_authorized_probe(
                "observation-owner",
                "observation-provider",
                "model",
                probe + 2,
            )
        })
        .unwrap();
    let healed = rig.published();
    assert!(healed.fence_rechecks(probe + 2).unwrap().is_empty());
    assert_eq!(healed, rig.fresh());
    let assert_retired = || {
        rig.store.with_reader_for_test(|conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT evidence,evidence_at,retired_at FROM retirement_tombstone WHERE kind='observation' AND origin_key=?1",
                    [&key],
                    |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?,row.get::<_,i64>(2)?)),
                )?,
                (evidence.clone(),now-10,probe+2),
                "authorized success must retire the exact bound-observation evidence"
            );
            assert_eq!(conn.query_row(
                "SELECT count(*) FROM probe_authorization WHERE kind='observation' AND origin_key=?1",
                [&key], |row| row.get::<_,i64>(0),
            )?,0,"retirement must spend this observation's authorizations");
            Ok(())
        }).unwrap();
    };
    assert_retired();
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.set_observation_effects("observation", true, &effects)
        })
        .unwrap();
    assert_eq!(
        rig.published(),
        healed,
        "repeated effects must not temporarily re-arm another signature in the same observation"
    );
    assert_eq!(rig.published(), rig.fresh());
    assert_retired();
}
