//! The route-state snapshot `route.select` scores against and
//! `route.cooldown_status` answers from: every `model_route_state` row, with
//! its cooldown, quota and failure deadlines replaced by the probe clocks'
//! schedule, and the provider-wide cooldown row.
//!
//! One overlay serves two sources of rows. Cold/private-store reads join the
//! typed block sources to live telemetry. The in-memory block state holds
//! the same typed sources and telemetry, decoded by the same row decoders,
//! and answers live request paths
//! through [`crate::BlockState::route_state_snapshot`] and
//! [`crate::BlockState::provider_cooldown`].

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Row};
use striatum_core::model_router::{ModelRouteStateSnapshot, RouteRuntimeState};
use striatum_core::{derive_provider_key, ProviderCooldownState, RouteStateResult, RouteWallClass};

use crate::fence_rechecks::{self, FenceRecheck};

/// The `model_route_state` columns the snapshot reads, in [`route_row_at`]'s
/// order, before the `wall_unscoped` flag. A macro so the block state's
/// constant projection can name the same list.
#[cfg(any(test, feature = "test-support"))]
macro_rules! route_state_columns {
    () => {
        "route_key,capacity_key,cooldown_until,quota_exhausted_until,quota_pressure,latency_ema_ms,success_ema,failure_backoff_until,failure_backoff_reason,last_error,wall_class"
    };
}
#[cfg(any(test, feature = "test-support"))]
pub(crate) use route_state_columns;

/// The `provider_cooldown_state` columns `get_provider_cooldown` reads, in
/// [`provider_row_at`]'s order.
#[cfg(any(test, feature = "test-support"))]
macro_rules! provider_cooldown_columns {
    () => {
        "provider_key,cooldown_until,quota_exhausted_until,quota_pressure,wall_class,last_error"
    };
}
#[cfg(any(test, feature = "test-support"))]
pub(crate) use provider_cooldown_columns;

/// One `model_route_state` row as the snapshot reads it. The wall class is
/// kept as stored and parsed when a snapshot is built, so a row the snapshot
/// cannot read fails that snapshot, as the SQL reader does, rather than the
/// write that changed it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RouteStateRow {
    pub(crate) route_key: String,
    pub(crate) capacity_key: String,
    pub(crate) cooldown_until: Option<f64>,
    pub(crate) quota_exhausted_until: Option<f64>,
    pub(crate) quota_pressure: Option<f64>,
    pub(crate) latency_ema_ms: Option<f64>,
    pub(crate) success_ema: Option<f64>,
    pub(crate) failure_backoff_until: Option<f64>,
    pub(crate) failure_backoff_reason: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) wall_class: Option<String>,
    pub(crate) wall_unscoped: bool,
}

/// Decode [`route_state_columns`] and then `wall_unscoped`, from column `at`.
pub(crate) fn route_row_at(r: &Row<'_>, at: usize) -> rusqlite::Result<RouteStateRow> {
    Ok(RouteStateRow {
        route_key: r.get(at)?,
        capacity_key: r.get(at + 1)?,
        cooldown_until: r.get(at + 2)?,
        quota_exhausted_until: r.get(at + 3)?,
        quota_pressure: r.get(at + 4)?,
        latency_ema_ms: r.get(at + 5)?,
        success_ema: r.get(at + 6)?,
        failure_backoff_until: r.get(at + 7)?,
        failure_backoff_reason: r.get(at + 8)?,
        last_error: r.get(at + 9)?,
        wall_class: r.get(at + 10)?,
        wall_unscoped: r.get(at + 11)?,
    })
}

/// One `provider_cooldown_state` row as `get_provider_cooldown` reads it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProviderCooldownRow {
    pub(crate) provider_key: String,
    pub(crate) cooldown_until: Option<f64>,
    pub(crate) quota_exhausted_until: Option<f64>,
    pub(crate) quota_pressure: Option<f64>,
    pub(crate) wall_class: Option<String>,
    pub(crate) last_error: Option<String>,
}

impl ProviderCooldownRow {
    pub(crate) fn provider_key(&self) -> &str {
        &self.provider_key
    }
}

/// Decode [`provider_cooldown_columns`] from column `at`.
pub(crate) fn provider_row_at(r: &Row<'_>, at: usize) -> rusqlite::Result<ProviderCooldownRow> {
    Ok(ProviderCooldownRow {
        provider_key: r.get(at)?,
        cooldown_until: r.get(at + 1)?,
        quota_exhausted_until: r.get(at + 2)?,
        quota_pressure: r.get(at + 3)?,
        wall_class: r.get(at + 4)?,
        last_error: r.get(at + 5)?,
    })
}

fn wall_class(stored: Option<&str>) -> RouteStateResult<Option<RouteWallClass>> {
    stored
        .map(|value| RouteWallClass::parse(value).map_err(crate::err))
        .transpose()
}

/// The canonical provider key `get_provider_cooldown` looks up.
pub(crate) fn provider_lookup_key(provider_key: &str) -> RouteStateResult<&str> {
    derive_provider_key(provider_key)
        .ok_or_else(|| crate::err("provider cooldown lookup has no canonical provider key"))
}

/// What `get_provider_cooldown` answers for the row it found, if any.
pub(crate) fn provider_cooldown(
    row: Option<&ProviderCooldownRow>,
) -> RouteStateResult<Option<ProviderCooldownState>> {
    let Some(row) = row else {
        return Ok(None);
    };
    // A provider row written from one credential's refusal (before such
    // refusals were kept off the provider) is read as absent: it is evidence
    // about that credential, which its fence covers.
    if crate::credential_refusal_wall(row.last_error.as_deref()) {
        return Ok(None);
    }
    Ok(Some(ProviderCooldownState {
        provider_key: row.provider_key.clone(),
        cooldown_until: row.cooldown_until,
        quota_exhausted_until: row.quota_exhausted_until,
        quota_pressure: row.quota_pressure,
        wall_class: wall_class(row.wall_class.as_deref())?,
    }))
}

/// The frozen schema-93 provider reader used only by differential tests.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn legacy_read_provider_cooldown(
    conn: &Connection,
    provider_key: &str,
) -> RouteStateResult<Option<ProviderCooldownState>> {
    let key = provider_lookup_key(provider_key)?;
    let row = conn
        .query_row(
            concat!(
                "SELECT ",
                provider_cooldown_columns!(),
                " FROM provider_cooldown_state WHERE provider_key = ?1"
            ),
            [key],
            |r| provider_row_at(r, 0),
        )
        .optional()
        .map_err(crate::err)?;
    provider_cooldown(row.as_ref())
}

/// The frozen schema-93 snapshot reader used only by differential tests.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn legacy_read_snapshot(
    conn: &Connection,
    now: f64,
    recheck_now: i64,
) -> RouteStateResult<ModelRouteStateSnapshot> {
    let clock_schema = fence_rechecks::legacy::enabled(conn).map_err(crate::err)?;
    let rechecks =
        fence_rechecks::legacy::read_effective_at(conn, recheck_now).map_err(crate::err)?;
    let mut statement = conn
        .prepare_cached(if clock_schema {
            concat!(
                "SELECT ",
                route_state_columns!(),
                ",wall_unscoped FROM model_route_state ORDER BY rowid"
            )
        } else {
            concat!(
                "SELECT ",
                route_state_columns!(),
                ",0 FROM model_route_state ORDER BY rowid"
            )
        })
        .map_err(crate::err)?;
    let rows = statement
        .query_map([], |r| route_row_at(r, 0))
        .map_err(crate::err)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(crate::err)?;
    overlay(&rows, &rechecks, clock_schema, now)
}

/// Build the snapshot at `now` from route rows, in table (rowid) order, and
/// the effective probe clocks. Without the clock schema a row's own
/// deadlines stand; with it, a deadline holds only while a clock of its
/// origin (or its provider's) schedules it.
pub(crate) fn overlay(
    rows: &[impl std::borrow::Borrow<RouteStateRow>],
    rechecks: &[FenceRecheck],
    clock_schema: bool,
    now: f64,
) -> RouteStateResult<ModelRouteStateSnapshot> {
    let mut by_route: HashMap<String, RouteRuntimeState> = HashMap::new();
    let mut by_capacity: HashMap<String, RouteRuntimeState> = HashMap::new();
    for row in rows {
        let row = row.borrow();
        let scope_error = row.last_error.as_deref();
        let mut state = RouteRuntimeState {
            cooldown_until: row.cooldown_until,
            quota_exhausted_until: row.quota_exhausted_until,
            wall_class: wall_class(row.wall_class.as_deref())?,
            last_error: row.last_error.clone(),
            latency_ema_ms: row.latency_ema_ms,
            success_ema: row.success_ema,
            failure_backoff_until: row.failure_backoff_until,
            failure_backoff_reason: row.failure_backoff_reason.clone(),
            ..Default::default()
        };
        let provider = derive_provider_key(&row.route_key).unwrap_or("");
        let model = row
            .capacity_key
            .strip_prefix(provider)
            .and_then(|key| key.strip_prefix('/'))
            .unwrap_or("");
        let route_origin = serde_json::json!([row.route_key]).to_string();
        let schedule = |kinds: &[&str], raw: f64| {
            rechecks
                .iter()
                .filter(|origin| {
                    kinds.contains(&origin.kind.as_str())
                        && ((origin.kind == "provider" && origin.applies(provider, "", "", model))
                            || origin.origin_key == route_origin)
                })
                .map(|origin| {
                    if origin
                        .last_claimed_at
                        .is_some_and(|at| at >= origin.evidence_at)
                    {
                        origin.due_at() as f64
                    } else {
                        raw.min(origin.due_at() as f64)
                    }
                })
                .reduce(f64::max)
                .or_else(|| (!clock_schema).then_some(raw))
        };
        if let Some(raw) = state.cooldown_until {
            state.cooldown_until = schedule(&["route", "provider"], raw);
        }
        if let Some(raw) = state.quota_exhausted_until {
            state.quota_exhausted_until = schedule(&["route", "provider"], raw);
        }
        if let Some(raw) = state.failure_backoff_until {
            state.failure_backoff_until = schedule(&["failure"], raw);
        }
        // These legacy origins have no account identity. Preserve the
        // reported quota label in storage, but enforce a non-quota retry
        // clock rather than infer an account quota.
        if row.wall_unscoped
            && striatum_core::account_wall::account_scoped_wall(
                state.last_error.as_deref(),
                state.wall_class,
            )
        {
            // Older downstream readers classify that text as an account
            // wall. Keep it only in the original store row, not in the
            // effective identity-free cooldown view.
            state.last_error = Some("identity-free legacy cooldown".into());
        }
        if state.wall_class == Some(RouteWallClass::Quota)
            || (row.wall_unscoped && state.wall_class == Some(RouteWallClass::Transient))
        {
            state.wall_class = None;
        }
        // Read-side quarantine leaves the original row available for
        // diagnosis. Its single-account horizon cannot reject another
        // credential, including through a sibling capacity key.
        if !row.wall_unscoped
            && striatum_core::account_wall::account_scoped_wall(scope_error, state.wall_class)
        {
            state.cooldown_until = None;
            state.quota_exhausted_until = None;
            state.wall_class = None;
            if striatum_core::account_wall::transient_error(scope_error.unwrap_or_default()) {
                state.failure_backoff_until = None;
                state.failure_backoff_reason = None;
            }
        }
        // Capacity-shared signals propagate across variants of the same
        // provider/model. Soft pressure lives ONLY on the capacity key so a
        // fresh quota reading can replace a stale stored value for every
        // sibling route at once.
        let cap = by_capacity.entry(row.capacity_key.clone()).or_default();
        cap.cooldown_until = crate::max_defined(cap.cooldown_until, state.cooldown_until);
        cap.quota_exhausted_until =
            crate::max_defined(cap.quota_exhausted_until, state.quota_exhausted_until);
        let row_wall_is_live = state.cooldown_until.is_some_and(|until| until > now)
            || state.quota_exhausted_until.is_some_and(|until| until > now);
        if row_wall_is_live
            && (cap.wall_class.is_none() || state.wall_class == Some(RouteWallClass::Billing))
        {
            cap.wall_class = state.wall_class;
            cap.last_error = state.last_error.clone();
        }
        if let Some(pressure) = row.quota_pressure {
            cap.quota_pressure = Some(match cap.quota_pressure {
                Some(existing) => existing.max(pressure),
                None => pressure,
            });
        }
        by_route.insert(row.route_key.clone(), state);
    }
    Ok(ModelRouteStateSnapshot {
        now,
        by_route,
        by_capacity,
    })
}

/// Read raw provider deadlines, not the provider's independently renewed probe
/// horizon. A quarantined old provider row has telemetry but no block to join.
pub(crate) fn read_provider_cooldown(
    conn: &Connection,
    provider_key: &str,
) -> RouteStateResult<Option<ProviderCooldownState>> {
    let key = provider_lookup_key(provider_key)?;
    let row=conn.query_row("SELECT p.provider_key,b.cooldown_until,b.quota_exhausted_until,p.quota_pressure,b.wall_class,p.last_error
        FROM provider_cooldown_state p LEFT JOIN block b ON b.kind='provider' AND b.origin_key=json_array(p.provider_key)
            AND b.is_origin=1 AND b.source_present=1 WHERE p.provider_key=?1",[key],|row|provider_row_at(row,0)).optional().map_err(crate::err)?;
    provider_cooldown(row.as_ref())
}

/// Cold/private-store snapshot path. The live request path uses the published
/// BlockState; this SQL path reads the same durable sources independently.
pub(crate) fn read_snapshot(
    conn: &Connection,
    now: f64,
    recheck_now: i64,
) -> RouteStateResult<ModelRouteStateSnapshot> {
    let rechecks = fence_rechecks::read_effective_at(conn, recheck_now).map_err(crate::err)?;
    struct ProviderFields {
        cooldown: Option<f64>,
        quota: Option<f64>,
        class: Option<String>,
        unscoped: bool,
        cleared_models: Vec<String>,
    }
    // Fetch both sets in one SQL statement, preserving route row order.
    // derive_provider_key resolves each provider from the routing key;
    // unrelated provider_id telemetry must not change another route's cause.
    let (mut rows, providers) = (|| -> rusqlite::Result<_> {
        let mut statement = conn.prepare_cached("SELECT 0 AS record_kind,m.rowid,m.route_key,m.capacity_key,b.cooldown_until,b.quota_exhausted_until,
            m.quota_pressure,m.latency_ema_ms,m.success_ema,f.deadline,f.reason,m.last_error,
            CASE WHEN b.kind IS NOT NULL THEN b.wall_class ELSE f.wall_class END,
            CASE WHEN b.kind IS NOT NULL THEN b.wall_unscoped ELSE COALESCE(f.wall_unscoped,0) END,
            b.kind IS NOT NULL,NULL
            FROM model_route_state m LEFT JOIN block b ON b.kind='route' AND b.origin_key=json_array(m.route_key) AND b.is_origin=1 AND b.source_present=1
            LEFT JOIN block f ON f.kind='failure' AND f.origin_key=json_array(m.route_key) AND f.is_origin=1 AND f.source_present=1
            UNION ALL
            SELECT 1,p.rowid,p.origin_key,NULL,p.cooldown_until,p.quota_exhausted_until,
            NULL,NULL,NULL,NULL,NULL,NULL,p.wall_class,p.wall_unscoped,0,p.cleared_models
            FROM block p WHERE p.kind='provider' AND p.is_origin=1 AND p.source_present=1
            ORDER BY record_kind,2")?;
        let mut cursor = statement.query([])?;
        let mut rows = Vec::new();
        let mut providers = HashMap::<String, ProviderFields>::new();
        while let Some(row) = cursor.next()? {
            if row.get::<_, i64>(0)? == 0 {
                rows.push((route_row_at(row, 2)?, row.get::<_, bool>(14)?));
            } else {
                let cleared: String = row.get(15)?;
                let cleared_models = serde_json::from_str(&cleared).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(15, rusqlite::types::Type::Text, Box::new(error))
                })?;
                providers.insert(row.get(2)?, ProviderFields {
                    cooldown: row.get(4)?, quota: row.get(5)?, class: row.get(12)?,
                    unscoped: row.get(13)?, cleared_models,
                });
            }
        }
        Ok((rows, providers))
    })().map_err(crate::err)?;
    for (row, own_wall) in &mut rows {
        if *own_wall {
            continue;
        }
        let Some(provider) = derive_provider_key(&row.route_key) else {
            continue;
        };
        let key = serde_json::json!([provider]).to_string();
        let Some(source) = providers.get(&key) else {
            continue;
        };
        let model = row
            .capacity_key
            .strip_prefix(provider)
            .and_then(|key| key.strip_prefix('/'))
            .unwrap_or("");
        if source
            .cleared_models
            .iter()
            .any(|cleared| cleared.as_str() == model)
        {
            continue;
        }
        row.cooldown_until = source.cooldown;
        row.quota_exhausted_until = source.quota;
        row.wall_class = source.class.clone();
        row.wall_unscoped = source.unscoped;
    }
    let rows: Vec<_> = rows.into_iter().map(|(row, _)| row).collect();
    overlay(&rows, &rechecks, true, now)
}
