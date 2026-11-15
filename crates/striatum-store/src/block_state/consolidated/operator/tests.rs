use striatum_core::{RouteStateStore, RouteStateUpdate};

#[test]
fn real_operator_partial_clear_keeps_a_claimed_classified_probe_after_reload() {
    let rig = super::credential::native_rig("native-operator-restart");
    let now = crate::now_ms();
    rig.store
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "credential-provider".into(),
                credential_id: "credential".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            Some("listing"),
            now - 900_000,
        )
        .unwrap();
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            assert!(tx.classify_credential_quota("credential-provider", "credential", true)?);
            let origins = tx.current_blocks()?.fence_rechecks(now)?;
            assert_eq!(origins.len(), 1);
            assert!(tx.grant_fence_rechecks(&origins, now, Some("claimed-owner"))?);
            Ok(())
        })
        .unwrap();
    let claimed_state = rig.published();
    let claimed = claimed_state
        .clock("credential", "[\"credential-provider\",\"credential\"]")
        .unwrap()
        .0;
    assert!(claimed.classified);
    assert_eq!(claimed.claim_owner.as_deref(), Some("claimed-owner"));
    rig.store
        .record_provider_cooldown(
            "operator",
            "operator/cleared",
            "operator/cleared",
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
            "operator/standing",
            "operator/standing",
            &RouteStateUpdate {
                observed_at_ms: Some(now + 1),
                ..Default::default()
            },
        )
        .unwrap();
    let cleared = rig
        .store
        .clear_route_cooldowns("operator/cleared")
        .unwrap()
        .unwrap();
    assert_eq!(cleared.route_key, "operator/cleared");
    assert_eq!(cleared.routes.len(), 1);
    assert_eq!(cleared.routes[0].0, "operator/cleared");
    assert_eq!(
        cleared.routes[0].1.cooldown_until,
        Some((now + 60_000) as f64)
    );
    let state = rig.published();
    let provider = state
        .fence_rechecks(now + 2)
        .unwrap()
        .into_iter()
        .find(|row| row.kind == "provider" && row.provider == "operator")
        .unwrap();
    assert_eq!(provider.cleared_models, vec!["cleared"]);
    assert!(!provider.applies("operator", "", "", "cleared"));
    assert!(provider.applies("operator", "", "", "standing"));
    assert_eq!(
        state
            .provider_cooldown("operator")
            .unwrap()
            .unwrap()
            .cooldown_until,
        Some((now + 60_000) as f64)
    );
    assert_eq!(
        state
            .clock("credential", "[\"credential-provider\",\"credential\"]")
            .unwrap()
            .0,
        claimed
    );
    assert_eq!(state, rig.fresh());
    rig.reload();
    assert_eq!(rig.published(), state);
    assert_eq!(
        rig.published()
            .clock("credential", "[\"credential-provider\",\"credential\"]")
            .unwrap()
            .0,
        claimed
    );
    assert_eq!(
        rig.published()
            .fence_rechecks(now + 2)
            .unwrap()
            .into_iter()
            .find(|row| row.kind == "provider" && row.provider == "operator")
            .unwrap()
            .cleared_models,
        vec!["cleared"]
    );
    rig.store.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT source_present,cleared_by FROM block WHERE kind='route' AND origin_key='[\"operator/cleared\"]'",[],|row|Ok((row.get::<_,bool>(0)?,row.get::<_,String>(1)?)))?,(false,"removed".into()));
        assert_eq!(conn.query_row("SELECT cooldown_until,quota_exhausted_until,wall_class FROM model_route_state WHERE route_key='operator/cleared'",[],|row|Ok((row.get::<_,Option<i64>>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,Option<String>>(2)?)))?,(None,None,None));
        Ok(())
    }).unwrap();
}
