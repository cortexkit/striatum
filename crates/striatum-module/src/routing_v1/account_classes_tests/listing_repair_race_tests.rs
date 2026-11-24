//! A vault listing repair decided from an older read must not undo evidence
//! recorded after that read.

use super::*;

/// A request reads a refusal recorded against an older vault listing and
/// decides to clear it. Before its repair runs, another request clears that
/// refusal and a newer refusal for the same login arrives against the current
/// listing, with its quota hold. The late repair must leave the newer refusal
/// and hold in place, and the request must still see the newer refusal.
#[test]
fn a_stale_listing_repair_keeps_a_newer_refusal_and_its_quota_hold() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let current = match credentials.listing_fingerprint("apikey:test") {
        crate::broca_credentials::ListingLookup::Listed(value) => value,
        _ => panic!("the fixture must list the key"),
    };
    let refusal = CredentialRefusal {
        provider_id: "synthetic".into(),
        credential_id: "apikey:test".into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    crate::account_writer::record_credential_entitlement_fence(
        harness.store(),
        &refusal,
        Some("an-older-listing"),
        NOW as i64 - 60_000,
    )
    .unwrap();
    let store = harness.store().clone();
    let newer = refusal.clone();
    let hold_id = std::rc::Rc::new(std::cell::RefCell::new(String::new()));
    let hook_hold = hold_id.clone();
    crate::credential_fences::before_listing_repair_for_test(move || {
        // Another request's repair clears the old refusal first, so the newer
        // refusal is recorded fresh against the current listing.
        assert_eq!(
            store
                .clear_credential_entitlement_fences("apikey:test", Some("an-older-listing"))
                .unwrap(),
            1
        );
        crate::account_writer::record_credential_entitlement_fence(
            &store,
            &newer,
            Some(&current),
            NOW as i64,
        )
        .unwrap();
        let fence = store
            .credential_entitlement_fences()
            .unwrap()
            .into_iter()
            .find(|fence| fence.credential_id == "apikey:test")
            .unwrap();
        let id = format!(
            "{}newer",
            crate::credential_fences::quota_hold_prefix(&fence)
        );
        let scope = FenceScope::General(GeneralWindow::FiveHour);
        let block = Fence {
            account_id: "apikey-account".into(),
            pool: Some(scope.into()),
            recovery_set: Vec::new(),
            observed_at_ms: NOW as i64,
            reset: ResetTime::At(NOW as i64 + 3_600_000),
            retry_hint_ms: None,
            source: ConfirmationSource::BrocaAccountLimit,
        };
        store
            .account_transaction(|tx| {
                tx.insert_fence(&AccountFenceRow {
                    fence_id: id.clone(),
                    provider: "synthetic".into(),
                    account_id: block.account_id.clone(),
                    scope_json: serde_json::to_string(&scope).unwrap(),
                    fence_json: serde_json::to_string(&block).unwrap(),
                    source: "broca_limit_exhausted".into(),
                    observed_at: NOW as i64,
                    principal: "test".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: NOW as i64,
                    cleared_at: None,
                    cleared_reason: None,
                })
            })
            .unwrap();
        *hook_hold.borrow_mut() = id;
    });
    let live = crate::credential_fences::live(harness.store(), &harness.credentials()).unwrap();
    let hold_id = hold_id.borrow().clone();
    assert!(!hold_id.is_empty(), "the hook ran");
    let active =
        crate::account_writer::repair(harness.store(), |tx| tx.active_pool_fences("synthetic"))
            .unwrap();
    assert!(
        active
            .iter()
            .any(|block| block.evidence.fence_id == hold_id),
        "the newer refusal's quota hold survives the late repair"
    );
    let stored = harness.store().credential_entitlement_fences().unwrap();
    assert!(
        stored
            .iter()
            .any(|fence| fence.credential_id == "apikey:test" && fence.observed_at_ms == NOW as i64),
        "the newer refusal survives: {stored:?}"
    );
    assert!(
        live.iter()
            .any(|fence| fence.credential_id == "apikey:test" && fence.observed_at_ms == NOW as i64),
        "the request still sees the newer refusal: {live:?}"
    );
}
