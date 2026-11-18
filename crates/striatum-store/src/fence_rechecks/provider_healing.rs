use super::*;
use rusqlite::params;

pub(super) fn heal_provider(
    tx: &AccountTx<'_>,
    blocks: &crate::BlockState,
    origin: &FenceRecheck,
    admitted_at: i64,
    now: i64,
    what: &str,
) -> striatum_core::RouteStateResult<()> {
    let provider = &origin.provider;
    // A retry clock can retain an older evidence time. Inheritance instead
    // belongs to the source's observed_at stamp, the same column guarded by SQL.
    if let Some(observed_at) = blocks
        .source_record("provider", &origin.origin_key)
        .and_then(|row| row.observed_at)
    {
        for (route, evidence, last_error, observed) in
            blocks.routes_stamped_by_provider(provider, observed_at, admitted_at)
        {
            let changed = tx.conn.execute("UPDATE block SET source_present=0,cleared_at=?5,cleared_by='removed',claim_owner=NULL
                WHERE kind='route' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2
                AND scope_error IS ?3 AND observed_at=?4",
                params![route,evidence,last_error,observed,now]).map_err(err)?;
            expect_changes(changed, 1, format_args!("{what}: route {route}")).map_err(err)?;
        }
    }
    if let Some(held) = blocks.source_origin("provider", &origin.origin_key) {
        let changed = tx.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
            WHERE kind='provider' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
            params![provider,held.evidence,now]).map_err(err)?;
        expect_changes(changed, 1, what).map_err(err)?;
    }
    Ok(())
}
