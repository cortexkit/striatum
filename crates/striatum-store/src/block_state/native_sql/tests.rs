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
               src.deadline,src.reset_at,
               CASE WHEN src.kind IN ('route','provider','transient_route')
                 AND src.reason='provider_retry' THEN 1 ELSE 0 END
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
                rate_limit_retry: row.get(14)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
    Ok(rows)
}

/// Independent SQL decisions for the provider and covered-route sync fixtures.
/// The temporary rows come from this module's SQL source reader, not
/// from the command's working state or the production synchronization code.
pub(super) fn synchronize(conn: &Connection, now: i64) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS test_sync_source (
           kind TEXT NOT NULL, origin_key TEXT NOT NULL, evidence TEXT NOT NULL,
           observed INTEGER NOT NULL, deadline, first_retry INTEGER,
           PRIMARY KEY(kind,origin_key));
         DELETE FROM test_sync_source;",
    )?;
    for source in origins(conn)? {
        if !matches!(source.kind.as_str(), "provider" | "route") || source.quota != Some(false) {
            return Err(rusqlite::Error::InvalidQuery);
        }
        conn.execute(
            "INSERT INTO test_sync_source(kind,origin_key,evidence,observed,deadline)
             VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![
                source.kind,
                source.origin_key,
                source.evidence,
                source.evidence_at,
                source.deadline
            ],
        )?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO retirement_tombstone(kind,origin_key,evidence,evidence_at,retired_at)
         SELECT s.kind,s.origin_key,s.evidence,s.observed,?1
         FROM test_sync_source AS s JOIN block AS b USING(kind,origin_key)
         WHERE b.managed=0 AND s.deadline<?1",
        [now],
    )?;
    conn.execute_batch(
        "DELETE FROM test_sync_source WHERE NOT EXISTS (
           SELECT 1 FROM block AS b WHERE b.kind=test_sync_source.kind
             AND b.origin_key=test_sync_source.origin_key AND b.source_present=1
             AND (b.cleared_by IS NULL OR b.cleared_by='kept'));
         UPDATE test_sync_source SET first_retry=
           CAST(MIN(COALESCE(deadline,observed+3600000),observed+3600000) AS INTEGER);
         UPDATE block SET
           next_recheck_at=(SELECT CASE
             WHEN s.observed<COALESCE(block.last_claimed_at,-9223372036854775808)
             THEN MAX(block.next_recheck_at,s.first_retry) ELSE s.first_retry END
             FROM test_sync_source AS s WHERE s.kind=block.kind AND s.origin_key=block.origin_key),
           quota=0,classified=0,reset_check_at=NULL,managed=1,
           cleared_at=NULL,cleared_by=NULL,claim_owner=NULL,cleared_models='[]'
         WHERE managed=0 AND source_present=1 AND is_origin=1
           AND (kind,origin_key) IN (SELECT kind,origin_key FROM test_sync_source);
         UPDATE block SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0
         WHERE kind IN ('provider','route') AND managed=1 AND is_origin=1
           AND source_present=1 AND (cleared_by IS NULL OR cleared_by='kept')
           AND NOT EXISTS (SELECT 1 FROM test_sync_source AS s
             WHERE s.kind=block.kind AND s.origin_key=block.origin_key);",
    )?;
    Ok(())
}

/// Sample every column of rows that still supply source or clock state.
/// Removed audit rows are checked by migration and retention tests separately.
pub(super) fn rows(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT * FROM block WHERE (cleared_by IS NULL OR cleared_by='kept')
           AND (source_present=1 OR kind='observation') ORDER BY rowid",
    )?;
    let columns = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            let values = (0..columns)
                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(format!("block {values:?}"))
        })?
        .collect();
    rows
}
