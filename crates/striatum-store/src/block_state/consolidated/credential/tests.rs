use super::*;

pub(super) fn native_rig(name: &str) -> crate::block_state::tests::Rig {
    let rig = crate::block_state::tests::rig_before_block_migration(name);
    rig.store
        .with_writer_for_test(|conn| {
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)
        })
        .unwrap();
    rig.reload();
    rig
}

#[test]
fn credential_writers_preserve_priority_fingerprint_claim_and_class_after_reload() {
    let rig = native_rig("native-credential-writers");
    let now = crate::now_ms();
    let refusal = |code: &str| striatum_core::route_state_store::CredentialRefusal {
        provider_id: "credential-provider".into(),
        credential_id: "credential".into(),
        code: code.into(),
    };
    rig.store
        .record_credential_entitlement_fence(&refusal("signed_out"), None, now)
        .unwrap();
    rig.store
        .adopt_credential_entitlement_fingerprint("credential", "signed-vault")
        .unwrap();
    assert_eq!(rig.published(), rig.fresh());
    let signed = rig
        .published()
        .entitlement_fence("credential-provider", "credential")
        .unwrap();
    assert_eq!(signed.listing_fingerprint.as_deref(), Some("signed-vault"));
    // A real refusal replaces sign-in evidence and its fingerprint, but the
    // report-ordering timestamp does not move backwards with a delayed report.
    rig.store
        .record_credential_entitlement_fence(
            &refusal("ENTITLEMENT_ERROR"),
            Some("active-vault"),
            now - 100,
        )
        .unwrap();
    let fence = rig
        .published()
        .entitlement_fence("credential-provider", "credential")
        .unwrap();
    assert_eq!(fence.code, "ENTITLEMENT_ERROR");
    assert_eq!(fence.observed_at_ms, now);
    assert_eq!(fence.listing_fingerprint.as_deref(), Some("active-vault"));
    assert_eq!(rig.published(), rig.fresh());
    let probe_at = now + 900_000;
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            assert!(tx.classify_credential_quota("credential-provider", "credential", true)?);
            let origins = tx.current_blocks()?.fence_rechecks(probe_at)?;
            assert_eq!(origins.len(), 1);
            assert!(tx.grant_fence_rechecks(&origins, probe_at, Some("credential-owner"))?);
            Ok(())
        })
        .unwrap();
    let claimed = rig.published();
    let (clock, _) = claimed
        .clock("credential", "[\"credential-provider\",\"credential\"]")
        .unwrap();
    assert!(clock.classified && clock.lifetime == striatum_core::blocks::LifetimeClass::Quota);
    assert_eq!(clock.claim_owner.as_deref(), Some("credential-owner"));
    // A lower-priority sign-in report must not postpone or reclassify an
    // independently refused credential, nor take its claimed probe away.
    rig.store
        .record_credential_entitlement_fence(
            &refusal("signed_out"),
            Some("unusable-vault"),
            now + 100,
        )
        .unwrap();
    assert_eq!(rig.published(), claimed);
    assert_eq!(rig.published(), rig.fresh());
    rig.store
        .account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.update_signed_out_fingerprint(&signed, "stale-sign-in")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rig.published(),
        claimed,
        "a stale sign-in CAS cannot change the typed fingerprint"
    );
    let unchanged = rig
        .store
        .clear_credential_entitlement_fences("credential", Some("stale-sign-in"))
        .unwrap();
    assert_eq!(unchanged, 0);
    assert_eq!(rig.published(), rig.fresh());
    let removed = rig
        .store
        .clear_credential_entitlement_fences_for_credential("credential")
        .unwrap();
    assert_eq!(removed, vec![fence]);
    assert!(rig.published().entitlement_fences().is_empty());
    assert_eq!(rig.published(), rig.fresh());
}
