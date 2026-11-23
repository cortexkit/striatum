//! Read-only SQL reference for the post-migration physical records.
//!
//! Scheduling and effective-row rules are written here independently. This
//! module reads scalar columns; it does not use the in-memory representation.

use super::super::FenceRecheck;
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn enabled(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='block')",
        [],
        |row| row.get(0),
    )
}

pub(super) fn times(conn: &Connection, now: i64) -> rusqlite::Result<BTreeSet<i64>> {
    let mut times = BTreeSet::from([now]);
    let mut statement = conn.prepare(
        "SELECT next_recheck_at FROM block UNION SELECT reset_check_at FROM block
         UNION SELECT last_claimed_at FROM block UNION SELECT deadline FROM block
         UNION SELECT reset_at FROM block",
    )?;
    for value in statement.query_map([], |row| row.get::<_, Option<f64>>(0))? {
        if let Some(value) = value? {
            let at = value as i64;
            times.extend([at.saturating_sub(1), at, at.saturating_add(1)]);
        }
    }
    Ok(times)
}

pub(super) fn identities(conn: &Connection) -> rusqlite::Result<Vec<[String; 4]>> {
    let mut statement = conn.prepare(
        "SELECT provider,account_id,credential_id,model FROM block
         UNION SELECT provider_id,NULL,NULL,model_id FROM model_route_state
         UNION SELECT provider_key,NULL,NULL,NULL FROM provider_cooldown_state
         UNION SELECT b.provider,NULL,NULL,CAST(j.value AS TEXT)
         FROM block b,json_each(b.cleared_models) j",
    )?;
    let scopes = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut identities = BTreeSet::new();
    for provider in scopes.iter().map(|scope| &scope.0).collect::<BTreeSet<_>>() {
        let related: Vec<_> = scopes
            .iter()
            .filter(|scope| scope.0.is_empty() || &scope.0 == provider)
            .collect();
        let accounts: BTreeSet<_> = related
            .iter()
            .filter_map(|s| s.1.as_deref())
            .chain([""])
            .collect();
        let credentials: BTreeSet<_> = related
            .iter()
            .filter_map(|s| s.2.as_deref())
            .chain([""])
            .collect();
        let models: BTreeSet<_> = related
            .iter()
            .filter_map(|s| s.3.as_deref())
            .chain([""])
            .collect();
        for account in accounts {
            for credential in &credentials {
                for model in &models {
                    identities.insert([
                        provider.clone(),
                        account.into(),
                        (*credential).into(),
                        (*model).into(),
                    ]);
                }
            }
        }
    }
    Ok(identities.into_iter().collect())
}

const SOURCE_SQL: &str = "SELECT src.kind,src.origin_key,src.provider,src.account_id,
    src.credential_id,src.model,src.size_class,src.principal,src.admission_id,src.evidence,
    CASE src.kind
      WHEN 'account' THEN COALESCE(MIN(src.observed_at,src.created_at),src.evidence_at)
      WHEN 'account_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
      WHEN 'non_quota_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
      WHEN 'transient_route' THEN COALESCE(src.last_reported_at,src.recorded_at,src.evidence_at)
      WHEN 'principal' THEN COALESCE(src.recorded_at,src.evidence_at)
      ELSE COALESCE(src.observed_at,src.evidence_at)
    END AS source_at,
    CASE WHEN src.kind='credential' THEN EXISTS (
      SELECT 1 FROM block hold WHERE hold.kind='account' AND hold.source_present=1
      AND (hold.cleared_by IS NULL OR hold.cleared_by='kept')
      AND substr(json_extract(hold.origin_key,'$[0]'),1,length('credential-quota:'||src.origin_key||':'))
        = 'credential-quota:'||src.origin_key||':'
    ) WHEN src.kind IN ('account','account_route','size') THEN 1 ELSE 0 END AS source_quota,
    src.deadline,src.reset_at
    FROM block src WHERE src.source_present=1 AND src.is_origin=1
    AND (src.cleared_by IS NULL OR src.cleared_by='kept')
    AND NOT(src.kind='route' AND EXISTS (
      SELECT 1 FROM block parent WHERE parent.kind='provider'
      AND parent.origin_key=json_array(src.provider) AND parent.source_present=1
      AND parent.cleared_at IS NULL AND (parent.cleared_by IS NULL OR parent.cleared_by='kept')
      AND parent.scope_error IS src.scope_error AND parent.observed_at IS src.observed_at
      AND COALESCE(parent.cooldown_until,0)>=COALESCE(src.cooldown_until,0)
      AND COALESCE(parent.quota_exhausted_until,0)>=COALESCE(src.quota_exhausted_until,0)
    ))";

/// An existing managed row supplies its captured clock. An unregistered source
/// gets the old receipt-based schedule, bounded by any retained prior claim.
pub(super) fn effective(conn: &Connection, now: i64) -> rusqlite::Result<Vec<FenceRecheck>> {
    let sql = format!("WITH source AS ({SOURCE_SQL}), pending AS (
        SELECT s.*,
          CAST(CASE WHEN s.source_quota=1 THEN s.source_at+900000
            ELSE MIN(COALESCE(s.deadline,s.source_at+3600000),s.source_at+3600000) END AS INTEGER) AS first_retry
        FROM source s
      )
      SELECT s.kind,s.origin_key,s.provider,s.account_id,s.credential_id,s.model,s.size_class,s.principal,s.admission_id,
        s.evidence,CASE WHEN b.managed=1 THEN b.evidence_at ELSE s.source_at END,
        CASE WHEN b.managed=1 THEN b.quota ELSE s.source_quota END,
        CASE WHEN b.managed=1 THEN b.classified ELSE 0 END,
        CASE WHEN b.managed=1 THEN b.next_recheck_at
          WHEN s.source_at<COALESCE(b.last_claimed_at,-9223372036854775808)
          THEN MAX(b.next_recheck_at,s.first_retry) ELSE s.first_retry END,
        CASE WHEN b.managed=1 THEN b.reset_check_at
          WHEN s.source_quota=1 AND s.reset_at>s.source_at
            AND s.reset_at>COALESCE(b.last_claimed_at,-9223372036854775808) THEN s.reset_at END,
        b.last_claimed_at,CASE WHEN b.managed=1 THEN b.claim_owner END,
        CASE WHEN b.managed=1 THEN b.cleared_models ELSE '[]' END
      FROM pending s JOIN block b USING(kind,origin_key)
      WHERE b.cleared_at IS NULL AND (b.managed=1 OR s.source_quota=1 OR s.deadline IS NULL OR s.deadline>=?1)
      UNION ALL
      SELECT b.kind,b.origin_key,b.provider,b.account_id,b.credential_id,b.model,b.size_class,b.principal,b.admission_id,
        b.evidence,b.evidence_at,b.quota,b.classified,b.next_recheck_at,b.reset_check_at,b.last_claimed_at,b.claim_owner,b.cleared_models
      FROM block b WHERE b.kind='observation' AND b.source_present=0 AND b.managed=1 AND b.is_origin=1
        AND b.cleared_at IS NULL AND (b.cleared_by IS NULL OR b.cleared_by='kept')
      ORDER BY 1,2");
    conn.prepare(&sql)?
        .query_map([now], |row| {
            let json: String = row.get(17)?;
            let mut cleared_models: Vec<String> = serde_json::from_str(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    17,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            cleared_models.sort();
            Ok(FenceRecheck {
                kind: row.get(0)?,
                origin_key: row.get(1)?,
                provider: row.get(2)?,
                account_id: row.get(3)?,
                credential_id: row.get(4)?,
                console_credit_micros_at_refusal: None,
                model: row.get(5)?,
                size_class: row.get(6)?,
                principal: row.get(7)?,
                admission_id: row.get(8)?,
                evidence: row.get(9)?,
                evidence_at: row.get(10)?,
                quota: row.get(11)?,
                classified: row.get(12)?,
                next_recheck_at: row.get(13)?,
                reset_check_at: row.get::<_, Option<f64>>(14)?.map(|at| at as i64),
                last_claimed_at: row.get(15)?,
                claim_owner: row.get(16)?,
                cleared_models,
            })
        })?
        .collect()
}

pub(super) fn generations(
    conn: &Connection,
) -> rusqlite::Result<BTreeMap<(String, String), (String, i64)>> {
    conn.prepare("SELECT kind,origin_key,evidence,claim_generation FROM block WHERE managed=1 AND is_origin=1")?
        .query_map([],|row|Ok(((row.get(0)?,row.get(1)?),(row.get(2)?,row.get(3)?))))?.collect()
}
