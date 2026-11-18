//! Writer statements shared with the query-plan regressions. Keeping the actual
//! statements here lets tests inspect the writers' keys, not copied test SQL.

pub(crate) fn rebind_claim_owner_sql() -> String {
    "UPDATE block SET claim_owner=?2 WHERE claim_owner=?1 AND is_origin=1 AND managed=1".into()
}

pub(crate) fn bind_claim_owner_sql() -> String {
    "UPDATE block SET claim_owner=?2 WHERE is_origin=1 AND managed=1 AND claim_owner=?1".into()
}

pub(crate) fn clear_credential_sql() -> String {
    "UPDATE block SET source_present=0,cleared_at=?2,cleared_by='removed',claim_owner=NULL
        WHERE kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1"
        .into()
}

pub(crate) fn clear_credential_for_id(
    tx: &crate::AccountTx<'_>,
    conn: &rusqlite::Connection,
    credential: &str,
) -> rusqlite::Result<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
    let lifted = tx
        .current_blocks()
        .map_err(crate::fence_rechecks::command_error)?
        .entitlement_fences()
        .into_iter()
        .filter(|row| row.credential_id == credential)
        .collect();
    conn.execute(
        &clear_credential_sql(),
        rusqlite::params![credential, crate::now_ms()],
    )?;
    Ok(lifted)
}

pub(crate) fn clear_matching_credential_sql() -> String {
    "UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
        WHERE kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1 AND listing_fingerprint IS ?2".into()
}

pub(crate) fn adopt_credential_fingerprint_sql() -> String {
    "UPDATE block SET listing_fingerprint=?2
        WHERE kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1 AND listing_fingerprint IS NULL".into()
}

pub(crate) fn clear_credential_routes_sql() -> String {
    "UPDATE block SET source_present=0,cleared_at=COALESCE(cleared_at,?2),cleared_by='removed',claim_owner=NULL
        WHERE credential_id=?1 AND source_present=1 AND kind IN ('account_route','non_quota_route','transient_route')".into()
}

pub(crate) fn drop_credential_route_clocks_sql() -> String {
    "UPDATE block SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0
        WHERE credential_id=?1 AND kind IN ('account_route','non_quota_route','transient_route')"
        .into()
}
