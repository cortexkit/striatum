//! The route-state snapshot `route.select` scores against and
//! `route.cooldown_status` answers from: every `model_route_state` row, with
//! its cooldown, quota and failure deadlines replaced by the probe clocks'
//! schedule, and the provider-wide cooldown row.
//!
//! One overlay serves two sources of rows. The store's `build_snapshot` and
//! `get_provider_cooldown` read SQL; they stay the oracle, and the answer for
//! a store without the block schema or an account-evaluation projection
//! (a private copy of the store that holds no account state). The in-memory
//! block state holds every `model_route_state` and `provider_cooldown_state`
//! row, decoded by the same row decoders below, and answers request paths
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
macro_rules! route_state_columns {
    () => {
        "route_key,capacity_key,cooldown_until,quota_exhausted_until,quota_pressure,latency_ema_ms,success_ema,failure_backoff_until,failure_backoff_reason,last_error,wall_class"
    };
}
pub(crate) use route_state_columns;

/// The `provider_cooldown_state` columns `get_provider_cooldown` reads, in
/// [`provider_row_at`]'s order.
macro_rules! provider_cooldown_columns {
    () => {
        "provider_key,cooldown_until,quota_exhausted_until,quota_pressure,wall_class,last_error"
    };
}
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

/// Read one provider's cooldown row from SQL.
pub(crate) fn read_provider_cooldown(
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

/// Build the snapshot from SQL: route rows in rowid order and, on a store
/// with probe clocks, the effective clocks evaluated at `recheck_now`.
pub(crate) fn read_snapshot(
    conn: &Connection,
    now: f64,
    recheck_now: i64,
) -> RouteStateResult<ModelRouteStateSnapshot> {
    let clock_schema = fence_rechecks::enabled(conn).map_err(crate::err)?;
    let rechecks = fence_rechecks::read_effective_at(conn, recheck_now).map_err(crate::err)?;
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
