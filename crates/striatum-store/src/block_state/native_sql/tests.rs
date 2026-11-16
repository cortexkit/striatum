//! Independent SQL reference for source origins in the native test store.
//!
//! Read scalar columns directly rather than loading block state. A mistake in
//! memory's source projection must not also change the expected result.

use crate::fence_rechecks::ViewOrigin;
use rusqlite::Connection;

pub(super) fn origins(conn: &Connection) -> rusqlite::Result<Vec<ViewOrigin>> {
    let mut rows = conn
        .prepare(
            "SELECT src.kind,src.origin_key,src.provider,src.account_id,
               src.credential_id,src.model,src.size_class,src.principal,src.admission_id,
               src.evidence,
               CASE src.kind
                 WHEN 'account' THEN COALESCE(MIN(src.observed_at,src.created_at),src.evidence_at)
                 WHEN 'account_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
                 WHEN 'non_quota_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
                 WHEN 'transient_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
                 WHEN 'principal' THEN COALESCE(src.recorded_at,src.evidence_at)
                 ELSE COALESCE(src.observed_at,src.evidence_at)
               END,
               CASE WHEN src.kind='credential' THEN EXISTS (
                 SELECT 1 FROM block hold
                 WHERE hold.kind='account' AND hold.source_present=1
                   AND (hold.cleared_by IS NULL OR hold.cleared_by='kept')
                   AND substr(json_extract(hold.origin_key,'$[0]'),1,
                     length('credential-quota:'||src.origin_key||':'))
                     = 'credential-quota:'||src.origin_key||':'
               ) ELSE src.quota END,
               src.deadline,src.reset_at
             FROM block src
             WHERE src.source_present=1 AND src.is_origin=1
               AND (src.cleared_by IS NULL OR src.cleared_by='kept')
               AND NOT (src.kind='route' AND EXISTS (
                 SELECT 1 FROM block parent
                 WHERE parent.kind='provider' AND parent.origin_key=json_array(src.provider)
                   AND parent.source_present=1 AND parent.cleared_at IS NULL
                   AND (parent.cleared_by IS NULL OR parent.cleared_by='kept')
                   AND parent.scope_error IS src.scope_error
                   AND parent.observed_at IS src.observed_at
                   AND COALESCE(parent.cooldown_until,0)>=COALESCE(src.cooldown_until,0)
                   AND COALESCE(parent.quota_exhausted_until,0)>=COALESCE(src.quota_exhausted_until,0)
               ))",
        )?
        .query_map([], |row| {
            Ok(ViewOrigin {
                kind: row.get(0)?,
                origin_key: row.get(1)?,
                provider: row.get(2)?,
                account_id: row.get(3)?,
                credential_id: row.get(4)?,
                model: row.get(5)?,
                size_class: row.get(6)?,
                principal: row.get(7)?,
                admission_id: row.get(8)?,
                evidence: row.get(9)?,
                evidence_at: row.get(10)?,
                quota: row.get(11)?,
                deadline: row.get(12)?,
                reset_at: row.get(13)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
    Ok(rows)
}
