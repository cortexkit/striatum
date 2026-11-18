//! The durable representation of refusal evidence. Scheduling and identity
//! fields are columns so indexed checks cannot silently inspect JSON detail.

use crate::sql_real::SqlReal;
use rusqlite::{Connection, Row};
use serde::{Deserialize, Serialize};
use striatum_core::blocks::Block;

pub(crate) mod keyed_updates;

// Keep SQL's loaded-row boundary identical to StoredBlock::held(). Inactive
// audit history must never be decoded by an admission or periodic state check.
pub(crate) const HELD_SQL: &str =
    "(cleared_by IS NULL OR cleared_by='kept') AND (source_present=1 OR kind='observation')";

// Keep account block rows so a cleared fence ID cannot be replayed.
// A source still held (including a sparse observation clock) is not garbage.
pub(crate) const COLLECTIBLE_SQL: &str = "source_present=0 AND kind!='account' AND (cleared_by='removed' OR (cleared_by='kept' AND kind!='observation'))";

pub(crate) const MIGRATION: &str = include_str!("../migrations/096_block_storage.sql");

#[cfg(any(test, feature = "test-support"))]
pub(crate) const CLOCK_NORMALIZATION_BODY: &str =
    "UPDATE block SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0
  WHERE kind=NEW.kind AND origin_key=NEW.origin_key";

#[cfg(any(test, feature = "test-support"))]
impl crate::SqliteRouteStateStore {
    /// Check that `block_clock_drop` resets `managed`, `claim_owner`,
    /// `last_claimed_at`, and `claim_generation` for absent or non-origin
    /// sources other than observations. Request-path state omits these clocks.
    pub fn assert_canonical_block_clocks_for_test(&self) -> crate::RouteStateResult<()> {
        let sql=format!("{} SELECT kind,origin_key,managed,claim_owner,last_claimed_at,claim_generation FROM block
            WHERE kind!='observation' AND (source_present=0 OR is_origin=0)
              AND (managed=1 OR claim_owner IS NOT NULL OR last_claimed_at IS NOT NULL OR claim_generation!=0)
            ORDER BY kind,origin_key LIMIT 10",crate::block_state::MAINTENANCE_READ);
        let stale = self
            .store
            .with_read(|conn| {
                conn.prepare(&sql)?
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(crate::err)?;
        assert!(
            stale.is_empty(),
            "maintenance parity: dropped sources carry stale managed or claim clocks: {stale:?}"
        );
        Ok(())
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AccountMeta {
    pub scope_json: String,
    pub fence_json: String,
    pub source: String,
    pub principal: String,
    pub admission_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialMeta {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouteMeta {
    pub decision_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrincipalMeta {
    pub runner_view_digest: String,
    pub roster_view_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyMeta {}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Deadline {
    Integer(i64),
    Real(SqlReal),
}

impl Deadline {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Real(value) => value.as_f64(),
        }
    }

    pub(crate) fn integer(self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(value),
            Self::Real(_) => None,
        }
    }
}

impl rusqlite::types::FromSql for Deadline {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Integer(value) => Ok(Self::Integer(value)),
            rusqlite::types::ValueRef::Real(value) => Ok(Self::Real(value.into())),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl rusqlite::ToSql for Deadline {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(match self {
            Self::Integer(value) => rusqlite::types::ToSqlOutput::Owned((*value).into()),
            Self::Real(value) => rusqlite::types::ToSqlOutput::Owned(value.as_f64().into()),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredBlock {
    pub rowid: i64,
    pub block: Block,
    pub cleared_models: Vec<String>,
    pub cleared_at: Option<i64>,
    pub cleared_by: Option<String>,
    pub cleared_reason: Option<String>,
    pub source_present: bool,
    pub managed: bool,
    pub source_order: i64,
    pub observation_id: Option<String>,
    pub pool_key_json: Option<String>,
    pub listing_fingerprint: Option<String>,
    pub recovery_set_json: Option<String>,
    pub deadline: Option<Deadline>,
    pub reset_at: Option<SqlReal>,
    pub observed_at: Option<i64>,
    pub created_at: Option<i64>,
    pub recorded_at: Option<i64>,
    pub last_reported_at: Option<i64>,
    pub cooldown_until: Option<SqlReal>,
    pub quota_exhausted_until: Option<SqlReal>,
    pub reason: Option<String>,
    pub code: Option<String>,
    pub wall_class: Option<String>,
    pub is_origin: bool,
    pub wall_unscoped: bool,
    pub scope_error: Option<String>,
    pub meta: Meta,
}

pub(crate) fn select() -> String {
    format!("SELECT rowid,{CLOCK_COLUMNS},cleared_by,cleared_reason,source_present,managed,source_order,observation_id,pool_key_json,listing_fingerprint,recovery_set_json,deadline,reset_at,observed_at,created_at,recorded_at,last_reported_at,cooldown_until,quota_exhausted_until,reason,code,wall_class,is_origin,wall_unscoped,scope_error,cleared_models,source_metadata_json FROM block")
}

/// Writer bookkeeping: next source order per kind. A recreated source must
/// follow surviving sources even though its block row keeps its identity.
/// The two non-quota route kinds shared one legacy table, so they share an
/// order here. Separate scalar MAX queries keep each lookup an index seek.
pub(crate) const NEXT_SOURCE_ORDER: &str = "/* writer bookkeeping: next source order per kind */
    SELECT CASE WHEN ?1 IN ('non_quota_route','transient_route') THEN
        MAX(COALESCE((SELECT MAX(source_order) FROM block WHERE kind='non_quota_route'),0),
            COALESCE((SELECT MAX(source_order) FROM block WHERE kind='transient_route'),0))+1
    ELSE COALESCE((SELECT MAX(source_order) FROM block WHERE kind=?1),0)+1 END";

/// Use the `model_route_state` rowid as `source_order` so recording a route
/// refusal does not change the order in which routes are scanned.
pub(crate) const ROUTE_SOURCE_ORDER: &str =
    "SELECT rowid FROM model_route_state WHERE route_key=json_extract(?1,'$[0]')";

pub(crate) const OBSERVATION_RETIREMENT_GUARD_MARKER: &str =
    "/* writer guard: observation-effect retirement */";
pub(crate) const OBSERVATION_RETIREMENT_LOOKUP: &str =
    "SELECT 1 FROM retirement_tombstone WHERE kind='observation' AND origin_key=:key AND evidence_at>=:at LIMIT 1";

fn source_insert_sql(kind: &str) -> String {
    const BINDINGS: &str = ":kind,:key,:provider,:account,:credential,:model,:size,:principal,:admission,
        :evidence,:at,:quota,:next,:reset,:observation,:pool,:deadline,:reset_at,
        :observed,:created,:recorded,:reported,:cooldown,:exhausted,
        :reason,:code,:class,:unscoped,:error,:fingerprint,:recovery,:source_metadata_json,:clear_reason,:clear_at,:clear_by,:present,:order";
    let (marker, values) = if kind == "observation" {
        (
            format!("{OBSERVATION_RETIREMENT_GUARD_MARKER} "),
            format!("SELECT {BINDINGS} WHERE NOT EXISTS ({OBSERVATION_RETIREMENT_LOOKUP})"),
        )
    } else {
        (String::new(), format!("VALUES ({BINDINGS})"))
    };
    format!("{marker}INSERT INTO block(kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
        evidence,evidence_at,quota,next_recheck_at,reset_check_at,observation_id,pool_key_json,deadline,reset_at,
        observed_at,created_at,recorded_at,last_reported_at,cooldown_until,quota_exhausted_until,
        reason,code,wall_class,wall_unscoped,scope_error,listing_fingerprint,recovery_set_json,source_metadata_json,cleared_reason,cleared_at,cleared_by,source_present,source_order)
        {values}
        ON CONFLICT(kind,origin_key) DO UPDATE SET
        provider=excluded.provider,account_id=excluded.account_id,credential_id=excluded.credential_id,
        model=excluded.model,size_class=excluded.size_class,principal=excluded.principal,admission_id=excluded.admission_id,
        evidence_at=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.evidence_at ELSE excluded.evidence_at END,
        quota=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.quota ELSE excluded.quota END,
        classified=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.classified ELSE 0 END,
        managed=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN 1 ELSE 0 END,
        claim_owner=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.claim_owner END,
        cleared_at=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.cleared_at END,
        cleared_by=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence AND block.cleared_at IS NOT NULL THEN 'kept' END,
        cleared_models=CASE WHEN block.managed=1 AND block.evidence=excluded.evidence THEN block.cleared_models ELSE '[]' END,
        evidence=excluded.evidence,observation_id=excluded.observation_id,pool_key_json=excluded.pool_key_json,
        deadline=excluded.deadline,reset_at=excluded.reset_at,observed_at=excluded.observed_at,created_at=excluded.created_at,
        recorded_at=excluded.recorded_at,last_reported_at=excluded.last_reported_at,cooldown_until=excluded.cooldown_until,
        quota_exhausted_until=excluded.quota_exhausted_until,reason=excluded.reason,code=excluded.code,
        wall_class=excluded.wall_class,wall_unscoped=excluded.wall_unscoped,scope_error=excluded.scope_error,
        listing_fingerprint=excluded.listing_fingerprint,recovery_set_json=excluded.recovery_set_json,
        source_metadata_json=excluded.source_metadata_json,cleared_reason=excluded.cleared_reason,
        source_order=CASE WHEN excluded.kind='route' THEN excluded.source_order
            WHEN block.source_present=1 THEN block.source_order ELSE excluded.source_order END,source_present=1
        WHERE excluded.kind NOT IN ('account','principal') OR (excluded.kind='principal' AND block.source_present=0)")
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn is_observation_retirement_insert(sql: &str) -> bool {
    sql == source_insert_sql("observation")
}

/// Write refusal evidence to `block`. For an already managed row with matching
/// `evidence`, keep its classification, claim owner, and cleared state.
/// `fence_rechecks::register_on` schedules rechecks using the prior claim time.
/// An observation insert is skipped if a `retirement_tombstone` with the same
/// `kind` and `origin_key` has an equal or later `evidence_at`.
pub(crate) fn write_source(conn: &Connection, row: &StoredBlock) -> rusqlite::Result<usize> {
    let block = &row.block;
    let scope = &block.scope;
    let source_order = if row.source_order == 0 {
        if block.kind == "route" {
            conn.query_row(ROUTE_SOURCE_ORDER, [&block.origin_key], |row| {
                row.get::<_, i64>(0)
            })?
        } else {
            conn.query_row(NEXT_SOURCE_ORDER, [&block.kind], |row| row.get::<_, i64>(0))?
        }
    } else {
        row.source_order
    };
    conn.execute(
        &source_insert_sql(&block.kind),
        rusqlite::named_params! {
            ":kind":block.kind,":key":block.origin_key,":provider":scope.provider,":account":scope.account_id,
            ":credential":scope.credential_id,":model":scope.model,":size":scope.size_class,":principal":scope.principal,
            ":admission":scope.admission_id,":evidence":block.evidence,":at":block.evidence_at,
            ":quota":block.lifetime==striatum_core::blocks::LifetimeClass::Quota,":next":block.next_recheck_at,
            ":reset":block.reset_check_at,":observation":row.observation_id,":pool":row.pool_key_json,
            ":deadline":row.deadline,":reset_at":row.reset_at,":observed":row.observed_at,":created":row.created_at,
            ":recorded":row.recorded_at,":reported":row.last_reported_at,":cooldown":row.cooldown_until,
            ":exhausted":row.quota_exhausted_until,":reason":row.reason,":code":row.code,":class":row.wall_class,
            ":unscoped":row.wall_unscoped,":error":row.scope_error,
            ":fingerprint":row.listing_fingerprint,":recovery":row.recovery_set_json,
            ":source_metadata_json":row.meta.encode().map_err(|error|json_error(0,error))?,":clear_reason":row.cleared_reason,":order":source_order,
            ":clear_at":row.cleared_at,":clear_by":row.cleared_by,":present":row.source_present,
        },
    )
}

pub(crate) fn remove_source(
    conn: &Connection,
    kind: &str,
    key: &str,
    now: i64,
    reason: &str,
) -> rusqlite::Result<usize> {
    conn.execute("UPDATE block SET source_present=0,cleared_at=COALESCE(cleared_at,?3),cleared_by='removed',cleared_reason=?4,claim_owner=NULL
        WHERE kind=?1 AND origin_key=?2 AND source_present=1",rusqlite::params![kind,key,now,reason])
}

fn json_error(column: usize, error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(error))
}

pub(crate) fn decode(row: &Row<'_>) -> rusqlite::Result<StoredBlock> {
    let (block, cleared_at) = crate::fence_rechecks::clock_row(row, 1)?;
    // Pool timestamps are integer-valued storage, not floating point route
    // deadlines. Keep their full precision and reject malformed external rows.
    let deadline = if matches!(block.kind.as_str(), "pool_billing" | "pool_rate") {
        row.get::<_, Option<i64>>("deadline")?
            .map(Deadline::Integer)
    } else {
        row.get("deadline")?
    };
    let clears: String = row.get("cleared_models")?;
    let cleared_models = serde_json::from_str(&clears).map_err(|e| json_error(41, e))?;
    let meta: String = row.get("source_metadata_json")?;
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
    let recovery_set_json = if source_present && block.kind == "account" {
        Some(row.get("recovery_set_json")?)
    } else {
        row.get("recovery_set_json")?
    };
    Ok(StoredBlock {
        rowid: row.get(0)?,
        block,
        cleared_models,
        cleared_at,
        cleared_by: row.get("cleared_by")?,
        cleared_reason: row.get("cleared_reason")?,
        source_present,
        managed: row.get("managed")?,
        source_order: row.get("source_order")?,
        observation_id: row.get("observation_id")?,
        pool_key_json: row.get("pool_key_json")?,
        listing_fingerprint: row.get("listing_fingerprint")?,
        recovery_set_json,
        deadline,
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
        is_origin: row.get("is_origin")?,
        wall_unscoped: row.get("wall_unscoped")?,
        scope_error: row.get("scope_error")?,
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
    pub(crate) fn is_origin(&self) -> bool {
        self.is_origin
    }
    pub(crate) fn from_origin(origin: crate::fence_rechecks::ViewOrigin) -> rusqlite::Result<Self> {
        let reset_at = origin.reset_at.map(SqlReal::from);
        let deadline = origin.deadline.map(|value| Deadline::Real(value.into()));
        let block =
            crate::fence_rechecks::derive_blocks(Default::default(), &[], [origin], i64::MIN)?
                .blocks
                .pop()
                .ok_or_else(|| rusqlite::Error::InvalidQuery)?;
        Ok(Self {
            rowid: 0,
            block,
            cleared_models: Vec::new(),
            cleared_at: None,
            cleared_by: None,
            cleared_reason: None,
            source_present: true,
            managed: false,
            source_order: 0,
            observation_id: None,
            pool_key_json: None,
            listing_fingerprint: None,
            recovery_set_json: None,
            deadline,
            reset_at,
            observed_at: None,
            created_at: None,
            recorded_at: None,
            last_reported_at: None,
            cooldown_until: None,
            quota_exhausted_until: None,
            reason: None,
            code: None,
            wall_class: None,
            is_origin: true,
            wall_unscoped: false,
            scope_error: None,
            meta: Meta::Empty(EmptyMeta {}),
        })
    }

    /// A tombstone with removed evidence is kept on disk for audit, not in the
    /// routing snapshot. Observation clocks alone survive their source's prune.
    pub(crate) fn held(&self) -> bool {
        !matches!(self.cleared_by.as_deref(), Some("removed" | "retired"))
            && (self.source_present || self.block.kind == "observation")
    }

    pub(crate) fn origin(&self) -> crate::fence_rechecks::ViewOrigin {
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
            evidence_at: match self.block.kind.as_str() {
                "account" => self
                    .observed_at
                    .zip(self.created_at)
                    .map(|(observed, created)| observed.min(created)),
                "account_route" | "non_quota_route" | "transient_route" => {
                    self.last_reported_at.or(self.recorded_at)
                }
                "principal" => self.recorded_at,
                _ => self.observed_at,
            }
            .unwrap_or(self.block.evidence_at),
            quota: Some(self.block.lifetime == striatum_core::blocks::LifetimeClass::Quota),
            deadline: self.deadline.map(Deadline::as_f64),
            reset_at: self.reset_at.map(SqlReal::as_f64),
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
    for row in rows.iter().filter(|r| r.held() && r.is_origin()) {
        if row.managed
            || row.block.lifetime == striatum_core::blocks::LifetimeClass::Quota
            || row.deadline.is_none_or(|at| at.as_f64() >= now as f64)
        {
            let mut block = row.block.clone();
            block.cleared_models = row.cleared_models.clone();
            block.cleared_models.sort();
            clocks.insert(
                (row.block.kind.clone(), row.block.origin_key.clone()),
                (block, row.cleared_at),
            );
        }
        if !row.source_present {
            continue;
        }
        if row.block.kind == "route"
            && rows.iter().any(|p| {
                p.block.kind == "provider"
                    && p.source_present
                    && p.cleared_at.is_none()
                    && p.held()
                    && p.block.scope.provider == row.block.scope.provider
                    && p.scope_error == row.scope_error
                    && p.observed_at == row.observed_at
                    && p.cooldown_until.map(SqlReal::as_f64).unwrap_or(0.0)
                        >= row.cooldown_until.map(SqlReal::as_f64).unwrap_or(0.0)
                    && p.quota_exhausted_until.map(SqlReal::as_f64).unwrap_or(0.0)
                        >= row
                            .quota_exhausted_until
                            .map(SqlReal::as_f64)
                            .unwrap_or(0.0)
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
#[cfg(test)]
#[path = "block_storage/migration/tests.rs"]
mod tests;

/// Convert the public quota-hold row without deriving a new origin identity.
/// The fence id remains the first component of its account-origin key.
pub(crate) fn account_source(
    conn: &Connection,
    fence: &crate::AccountPoolFenceRow,
) -> rusqlite::Result<StoredBlock> {
    let evidence = &fence.evidence;
    let reset_at = conn.query_row(
        "SELECT json_extract(?1,'$.reset.at_ms')",
        [&evidence.fence_json],
        |row| row.get(0),
    )?;
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: "account".into(),
        origin_key: serde_json::json!([evidence.fence_id]).to_string(),
        provider: evidence.provider.clone(),
        account_id: Some(evidence.account_id.clone()),
        evidence: evidence.fence_json.clone(),
        evidence_at: evidence.observed_at.min(evidence.created_at),
        quota: Some(true),
        reset_at,
        ..Default::default()
    })?;
    row.observation_id = evidence.observation_id.clone();
    row.pool_key_json = fence
        .pool_key
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| json_error(0, error))?;
    row.observed_at = Some(evidence.observed_at);
    row.created_at = Some(evidence.created_at);
    row.cleared_at = evidence.cleared_at;
    row.cleared_reason = evidence.cleared_reason.clone();
    if row.cleared_at.is_some() {
        row.cleared_by = Some("kept".into());
        row.source_present = false;
    }
    row.recovery_set_json =
        Some(serde_json::to_string(&fence.recovery_set).map_err(|error| json_error(0, error))?);
    row.meta = Meta::Account(AccountMeta {
        scope_json: evidence.scope_json.clone(),
        fence_json: evidence.fence_json.clone(),
        source: evidence.source.clone(),
        principal: evidence.principal.clone(),
        admission_id: evidence.admission_id.clone(),
    });
    Ok(row)
}

pub(crate) fn principal_source(
    conn: &Connection,
    exclusion: &crate::AccountExclusionRow,
) -> rusqlite::Result<StoredBlock> {
    let deadline: Deadline =
        conn.query_row("SELECT ?1+86400000", [exclusion.recorded_at], |row| {
            row.get(0)
        })?;
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: "principal".into(),
        origin_key: serde_json::json!([
            exclusion.principal,
            exclusion.admission_id,
            exclusion.credential_id
        ])
        .to_string(),
        credential_id: Some(exclusion.credential_id.clone()),
        principal: Some(exclusion.principal.clone()),
        admission_id: Some(exclusion.admission_id.clone()),
        evidence: serde_json::json!([
            exclusion.recorded_at,
            exclusion.runner_view_digest,
            exclusion.roster_view_digest
        ])
        .to_string(),
        evidence_at: exclusion.recorded_at,
        quota: Some(false),
        deadline: Some(deadline.as_f64()),
        ..Default::default()
    })?;
    row.deadline = Some(deadline);
    row.recorded_at = Some(exclusion.recorded_at);
    row.observation_id = exclusion.observation_id.clone();
    row.meta = Meta::Principal(PrincipalMeta {
        runner_view_digest: exclusion.runner_view_digest.clone(),
        roster_view_digest: exclusion.roster_view_digest.clone(),
    });
    Ok(row)
}

pub(crate) fn size_source(
    fact: &crate::AccountSizeRefusal,
    quota: bool,
) -> rusqlite::Result<StoredBlock> {
    let kind = if quota { "size" } else { "non_quota_size" };
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: kind.into(),
        origin_key: serde_json::json!([
            fact.provider,
            fact.credential_id,
            fact.model,
            fact.size_class
        ])
        .to_string(),
        provider: fact.provider.clone(),
        credential_id: Some(fact.credential_id.clone()),
        model: Some(fact.model.clone()),
        size_class: Some(fact.size_class.into()),
        evidence: serde_json::json!([fact.observed_at, fact.expires_at]).to_string(),
        evidence_at: fact.observed_at,
        quota: Some(quota),
        deadline: Some(fact.expires_at as f64),
        reset_at: quota.then_some(fact.expires_at as f64),
        ..Default::default()
    })?;
    row.deadline = Some(Deadline::Integer(fact.expires_at));
    row.observed_at = Some(fact.observed_at);
    Ok(row)
}

pub(crate) fn account_route_source(
    fence: &crate::AccountRouteFence,
    kind: &str,
    class: &str,
    reported: Option<i64>,
) -> rusqlite::Result<StoredBlock> {
    let quota = kind == "account_route" && class == "quota";
    let at = reported.unwrap_or(fence.recorded_at);
    let evidence = if kind == "account_route" {
        serde_json::json!([class, fence.until_ms, at])
    } else {
        serde_json::json!([fence.until_ms, at])
    };
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: kind.into(),
        origin_key: serde_json::json!([fence.provider, fence.credential_id, fence.model])
            .to_string(),
        provider: fence.provider.clone(),
        credential_id: Some(fence.credential_id.clone()),
        model: Some(fence.model.clone()),
        evidence: evidence.to_string(),
        evidence_at: at,
        quota: Some(quota),
        deadline: Some(fence.until_ms as f64),
        reset_at: quota.then_some(fence.until_ms as f64),
        ..Default::default()
    })?;
    row.deadline = Some(Deadline::Integer(fence.until_ms));
    row.recorded_at = Some(fence.recorded_at);
    row.last_reported_at = reported;
    row.wall_class = Some(class.into());
    row.meta = Meta::Route(RouteMeta {
        decision_id: fence.decision_id.clone(),
    });
    Ok(row)
}

pub(crate) struct WallSource<'a> {
    pub kind: &'a str,
    pub key: &'a str,
    pub provider: &'a str,
    pub model: Option<&'a str>,
    pub cooldown: Option<f64>,
    pub exhausted: Option<f64>,
    pub wall_observed: Option<i64>,
    pub observed_at: i64,
    pub error: Option<&'a str>,
    pub class: Option<&'a str>,
    pub unscoped: bool,
}

pub(crate) fn wall_source(
    conn: &Connection,
    wall: WallSource<'_>,
) -> rusqlite::Result<Option<StoredBlock>> {
    if wall.cooldown.is_none() && wall.exhausted.is_none() {
        return Ok(None);
    }
    // The old deadline columns have INTEGER affinity. Match their JSON
    // signature for whole-valued reals as well as fractional/out-of-range
    // reals, so exact retirement signatures survive the storage switch.
    let (evidence, deadline): (String, Deadline) = conn.query_row(
        "SELECT json_array(
        CASE WHEN ?1=CAST(?1 AS INTEGER) THEN CAST(?1 AS INTEGER) ELSE ?1 END,
        CASE WHEN ?2=CAST(?2 AS INTEGER) THEN CAST(?2 AS INTEGER) ELSE ?2 END,?3),
        MAX(COALESCE(?1,0),COALESCE(?2,0))",
        rusqlite::params![wall.cooldown, wall.exhausted, wall.wall_observed],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: wall.kind.into(),
        origin_key: serde_json::json!([wall.key]).to_string(),
        provider: wall.provider.into(),
        model: wall.model.map(str::to_owned),
        evidence,
        evidence_at: wall.observed_at,
        quota: Some(false),
        deadline: Some(deadline.as_f64()),
        ..Default::default()
    })?;
    row.deadline = Some(deadline);
    row.observed_at = Some(wall.observed_at);
    row.cooldown_until = wall.cooldown.map(SqlReal::from);
    row.quota_exhausted_until = wall.exhausted.map(SqlReal::from);
    row.scope_error = wall.error.map(str::to_owned);
    row.wall_class = wall.class.map(str::to_owned);
    row.wall_unscoped = wall.unscoped;
    Ok(Some(row))
}

pub(crate) fn credential_source(
    fence: &striatum_core::route_state_store::CredentialEntitlementFence,
) -> rusqlite::Result<StoredBlock> {
    let key = serde_json::json!([fence.provider_id, fence.credential_id]).to_string();
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: "credential".into(),
        origin_key: key,
        provider: fence.provider_id.clone(),
        credential_id: Some(fence.credential_id.clone()),
        evidence: serde_json::json!([fence.code, fence.observed_at_ms]).to_string(),
        evidence_at: fence.observed_at_ms,
        ..Default::default()
    })?;
    row.observed_at = Some(fence.observed_at_ms);
    row.code = Some(fence.code.clone());
    row.listing_fingerprint = fence.listing_fingerprint.clone();
    row.meta = Meta::Credential(CredentialMeta {});
    Ok(row)
}

pub(crate) struct FailureSource<'a> {
    pub key: &'a str,
    pub provider: &'a str,
    pub model: &'a str,
    pub deadline: Option<f64>,
    pub reason: Option<&'a str>,
    pub observed_at: i64,
    pub error: Option<&'a str>,
    pub class: Option<&'a str>,
    pub unscoped: bool,
}

pub(crate) fn failure_source(
    conn: &Connection,
    source: FailureSource<'_>,
) -> rusqlite::Result<Option<StoredBlock>> {
    let Some(deadline) = source.deadline else {
        return Ok(None);
    };
    let evidence:String=conn.query_row("SELECT json_array(CASE WHEN ?1=CAST(?1 AS INTEGER) THEN CAST(?1 AS INTEGER) ELSE ?1 END,?2,?3)",rusqlite::params![deadline,source.reason,source.observed_at],|row|row.get(0))?;
    let mut row = StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: "failure".into(),
        origin_key: serde_json::json!([source.key]).to_string(),
        provider: source.provider.into(),
        model: Some(source.model.into()),
        evidence,
        evidence_at: source.observed_at,
        quota: Some(false),
        deadline: Some(deadline),
        ..Default::default()
    })?;
    row.observed_at = Some(source.observed_at);
    row.reason = source.reason.map(str::to_owned);
    row.scope_error = source.error.map(str::to_owned);
    row.wall_class = source.class.map(str::to_owned);
    row.wall_unscoped = source.unscoped;
    Ok(Some(row))
}
