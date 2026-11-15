//! The durable representation of refusal evidence. Scheduling and identity
//! fields are columns so indexed checks cannot silently inspect JSON detail.

use rusqlite::{Connection, Row};
use serde::{Deserialize, Serialize};
use striatum_core::blocks::Block;

pub(crate) const MIGRATION: &str = include_str!("../../migrations/094_block_storage.sql");

/// Migration SQL calls the same quarantine predicate as routing. Keeping the
/// predicate in Rust avoids a second, incomplete SQL copy of error parsing.
pub(crate) fn install_migration_function(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "block_migration_quarantined",
        3,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let unscoped: bool = ctx.get(0)?;
            let error: Option<String> = ctx.get(1)?;
            let class: Option<String> = ctx.get(2)?;
            Ok(crate::fence_rechecks::quarantined_row(
                unscoped,
                error.as_deref(),
                class.as_deref(),
            ))
        },
    )
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AccountMeta {
    pub scope_json: String,
    pub fence_json: String,
    pub source: String,
    pub principal: String,
    pub admission_id: Option<String>,
    pub recovery_set_json: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialMeta {
    pub listing_fingerprint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouteMeta {
    pub decision_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrincipalMeta {
    pub runner_view_digest: String,
    pub roster_view_digest: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyMeta {}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Meta {
    Account(AccountMeta),
    Credential(CredentialMeta),
    Route(RouteMeta),
    Principal(PrincipalMeta),
    Empty(EmptyMeta),
}

impl Meta {
    pub(crate) fn decode(kind: &str, json: &str) -> serde_json::Result<Self> {
        Ok(match kind {
            "account" => Self::Account(serde_json::from_str(json)?),
            "credential" => Self::Credential(serde_json::from_str(json)?),
            "account_route" | "non_quota_route" | "transient_route" => {
                Self::Route(serde_json::from_str(json)?)
            }
            "principal" => Self::Principal(serde_json::from_str(json)?),
            _ => Self::Empty(serde_json::from_str(json)?),
        })
    }

    pub(crate) fn encode(&self) -> serde_json::Result<String> {
        match self {
            Self::Account(value) => serde_json::to_string(value),
            Self::Credential(value) => serde_json::to_string(value),
            Self::Route(value) => serde_json::to_string(value),
            Self::Principal(value) => serde_json::to_string(value),
            Self::Empty(value) => serde_json::to_string(value),
        }
    }
}

pub(crate) const CLOCK_COLUMNS: &str = "kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,classified,next_recheck_at,reset_check_at,last_claimed_at,claim_owner,claim_generation,cleared_at";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredBlock {
    pub rowid: i64,
    pub block: Block,
    pub cleared_at: Option<i64>,
    pub cleared_by: Option<String>,
    pub cleared_reason: Option<String>,
    pub source_present: bool,
    pub managed: bool,
    pub source_order: i64,
    pub observation_id: Option<String>,
    pub pool_key_json: Option<String>,
    pub deadline: Option<f64>,
    pub reset_at: Option<f64>,
    pub observed_at: Option<i64>,
    pub created_at: Option<i64>,
    pub recorded_at: Option<i64>,
    pub last_reported_at: Option<i64>,
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
    pub reason: Option<String>,
    pub code: Option<String>,
    pub wall_class: Option<String>,
    pub wall_unscoped: bool,
    pub scope_error: Option<String>,
    pub inherited_from_provider: bool,
    pub meta: Meta,
}

pub(crate) fn select() -> String {
    format!("SELECT rowid,{CLOCK_COLUMNS},cleared_by,cleared_reason,source_present,managed,source_order,observation_id,pool_key_json,deadline,reset_at,observed_at,created_at,recorded_at,last_reported_at,cooldown_until,quota_exhausted_until,reason,code,wall_class,wall_unscoped,scope_error,inherited_from_provider,cleared_models,meta FROM block")
}

fn json_error(column: usize, error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(error))
}

pub(crate) fn decode(row: &Row<'_>) -> rusqlite::Result<StoredBlock> {
    let (mut block, cleared_at) = crate::fence_rechecks::clock_row(row, 1)?;
    let clears: String = row.get("cleared_models")?;
    block.cleared_models = serde_json::from_str(&clears).map_err(|e| json_error(41, e))?;
    let meta: String = row.get("meta")?;
    let source_present: bool = row.get("source_present")?;
    let meta = Meta::decode(
        if !source_present && meta == "{}" {
            ""
        } else {
            &block.kind
        },
        &meta,
    )
    .map_err(|e| json_error(42, e))?;
    Ok(StoredBlock {
        rowid: row.get(0)?,
        block,
        cleared_at,
        cleared_by: row.get("cleared_by")?,
        cleared_reason: row.get("cleared_reason")?,
        source_present,
        managed: row.get("managed")?,
        source_order: row.get("source_order")?,
        observation_id: row.get("observation_id")?,
        pool_key_json: row.get("pool_key_json")?,
        deadline: row.get("deadline")?,
        reset_at: row.get("reset_at")?,
        observed_at: row.get("observed_at")?,
        created_at: row.get("created_at")?,
        recorded_at: row.get("recorded_at")?,
        last_reported_at: row.get("last_reported_at")?,
        cooldown_until: row.get("cooldown_until")?,
        quota_exhausted_until: row.get("quota_exhausted_until")?,
        reason: row.get("reason")?,
        code: row.get("code")?,
        wall_class: row.get("wall_class")?,
        wall_unscoped: row.get("wall_unscoped")?,
        scope_error: row.get("scope_error")?,
        inherited_from_provider: row.get("inherited_from_provider")?,
        meta,
    })
}

pub(crate) fn read(conn: &Connection) -> rusqlite::Result<Vec<StoredBlock>> {
    conn.prepare(&format!(
        "{} {}",
        crate::block_state::MAINTENANCE_READ,
        select()
    ))?
    .query_map([], decode)?
    .collect()
}

impl StoredBlock {
    /// A tombstone with removed evidence is kept on disk for audit, not in the
    /// routing snapshot. Observation clocks alone survive their source's prune.
    pub(crate) fn held(&self) -> bool {
        !matches!(self.cleared_by.as_deref(), Some("removed" | "retired"))
            && (self.source_present || self.block.kind == "observation")
    }

    fn origin(&self) -> crate::fence_rechecks::ViewOrigin {
        let scope = &self.block.scope;
        crate::fence_rechecks::ViewOrigin {
            kind: self.block.kind.clone(),
            origin_key: self.block.origin_key.clone(),
            provider: scope.provider.clone(),
            account_id: scope.account_id.clone(),
            credential_id: scope.credential_id.clone(),
            model: scope.model.clone(),
            size_class: scope.size_class,
            principal: scope.principal.clone(),
            admission_id: scope.admission_id.clone(),
            evidence: self.block.evidence.clone(),
            evidence_at: self.block.evidence_at,
            quota: Some(self.block.lifetime == striatum_core::blocks::LifetimeClass::Quota),
            deadline: self.deadline,
            reset_at: self.reset_at,
        }
    }
}

/// Derive the effective set without SQL. A hidden route is still stored, so
/// the route-state snapshot can schedule its own cooldown deadline, but it
/// does not become a second effective origin while matching provider-wide
/// refusal evidence already covers it.
pub(crate) fn effective(
    rows: &[StoredBlock],
    now: i64,
) -> rusqlite::Result<crate::fence_rechecks::LoadedBlocks> {
    let mut clocks = crate::fence_rechecks::Clocks::new();
    let mut origins = Vec::new();
    for row in rows.iter().filter(|r| r.held()) {
        if row.managed
            || row.block.lifetime == striatum_core::blocks::LifetimeClass::Quota
            || row.deadline.is_none_or(|at| at >= now as f64)
        {
            clocks.insert(
                (row.block.kind.clone(), row.block.origin_key.clone()),
                (row.block.clone(), row.cleared_at),
            );
        }
        if !row.source_present {
            continue;
        }
        if row.block.kind == "account_route" && row.wall_class.as_deref() != Some("quota") {
            continue;
        }
        if row.block.kind == "route"
            && rows.iter().any(|p| {
                p.block.kind == "provider"
                    && p.source_present
                    && p.held()
                    && p.block.scope.provider == row.block.scope.provider
                    && p.scope_error == row.scope_error
                    && p.block.evidence_at == row.block.evidence_at
                    && p.cooldown_until.unwrap_or(0.0) >= row.cooldown_until.unwrap_or(0.0)
                    && p.quota_exhausted_until.unwrap_or(0.0)
                        >= row.quota_exhausted_until.unwrap_or(0.0)
            })
        {
            continue;
        }
        origins.push(row.origin());
    }
    let mut loaded = crate::fence_rechecks::derive_blocks(clocks, &[], origins, now)?;
    loaded
        .blocks
        .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
    loaded
        .cleared_evidence
        .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
    Ok(loaded)
}

#[cfg(test)]
#[path = "migration/tests.rs"]
mod tests;
