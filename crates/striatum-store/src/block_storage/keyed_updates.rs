//! Writer statements shared with the query-plan regressions. Keeping the actual
//! statements here lets tests inspect the writers' keys, not copied test SQL.

pub(crate) fn rebind_claim_owner_sql() -> String {
    format!(
        "UPDATE block SET claim_owner=?2 WHERE {} AND claim_owner=?1 AND is_origin=1 AND managed=1",
        super::HELD_SQL
    )
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn bind_claim_owner_sql() -> String {
    format!(
        "UPDATE block SET claim_owner=?2 WHERE {} AND is_origin=1 AND managed=1 AND claim_owner=?1",
        super::HELD_SQL
    )
}

pub(crate) fn operator_account_routes_sql() -> String {
    format!("UPDATE block INDEXED BY block_held_provider SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
        WHERE {} AND kind IN ('account_route','non_quota_route','transient_route') AND source_present=1 AND provider=?1 AND model=?2",super::HELD_SQL)
}

// Use `block_held_credential` here and in the other credential updates below
// so SQLite seeks by `credential_id` instead of scanning every row of a `kind`.
pub(crate) fn clear_credential_sql() -> String {
    format!(
        "UPDATE block INDEXED BY block_held_credential SET source_present=0,cleared_at=?2,cleared_by='removed',claim_owner=NULL
        WHERE {} AND kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1",
        super::HELD_SQL
    )
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
    format!("UPDATE block INDEXED BY block_held_credential SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
        WHERE {} AND kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1 AND listing_fingerprint IS ?2",super::HELD_SQL)
}

pub(crate) fn adopt_credential_fingerprint_sql() -> String {
    format!("UPDATE block INDEXED BY block_held_credential SET listing_fingerprint=?2
        WHERE {} AND kind='credential' AND is_origin=1 AND source_present=1 AND credential_id=?1 AND listing_fingerprint IS NULL",super::HELD_SQL)
}

pub(crate) fn clear_credential_routes_sql() -> String {
    format!("UPDATE block INDEXED BY block_held_credential SET source_present=0,cleared_at=COALESCE(cleared_at,?2),cleared_by='removed',claim_owner=NULL
        WHERE {} AND credential_id=?1 AND source_present=1 AND kind IN ('account_route','non_quota_route','transient_route')",super::HELD_SQL)
}

pub(crate) fn drop_credential_route_clocks_sql() -> String {
    format!("UPDATE block INDEXED BY block_held_credential SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0
        WHERE {} AND credential_id=?1 AND kind IN ('account_route','non_quota_route','transient_route')",super::HELD_SQL)
}

pub(crate) fn retired_block_prune_sql() -> String {
    "DELETE FROM block WHERE rowid IN (SELECT rowid FROM block
        WHERE is_origin=1 AND cleared_by='retired' AND kind!='account' AND cleared_at<?1
        ORDER BY cleared_at LIMIT ?2)"
        .into()
}

pub(crate) fn inactive_block_prune_sql() -> String {
    format!(
        "DELETE FROM block WHERE rowid IN (SELECT rowid FROM block WHERE {} AND cleared_at<?1
        ORDER BY cleared_at LIMIT ?2)",
        super::COLLECTIBLE_SQL
    )
}
