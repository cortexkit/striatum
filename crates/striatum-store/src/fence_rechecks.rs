//! Durable, per-origin retry clocks. Selection reads these clocks without
//! consuming them; admission claims every applicable clock in one transaction.

use crate::{err, AccountTx, SqliteRouteStateStore};
use rusqlite::{params, Connection, OptionalExtension};
use striatum_core::blocks::{Block, Cause, LifetimeClass, Origin, Scope};
use striatum_core::RouteStateResult;

pub use striatum_core::fence_lifetimes::{NON_QUOTA_FENCE_MAX_MS, QUOTA_FENCE_RECHECK_MS};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceRecheck {
    pub evidence: String,
    pub kind: String,
    pub origin_key: String,
    pub provider: String,
    pub account_id: Option<String>,
    pub credential_id: Option<String>,
    pub model: Option<String>,
    pub size_class: Option<i64>,
    pub principal: Option<String>,
    pub admission_id: Option<String>,
    pub evidence_at: i64,
    pub quota: bool,
    pub classified: bool,
    pub next_recheck_at: i64,
    pub reset_check_at: Option<i64>,
    pub last_claimed_at: Option<i64>,
    pub claim_owner: Option<String>,
    pub cleared_models: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct QuotaSuccessScope {
    pub family: striatum_core::accounts::ModelFamily,
    pub pool: Option<String>,
}

impl QuotaSuccessScope {
    pub(crate) fn used(&self, key: &striatum_core::accounts::PoolKey, model: &str) -> bool {
        key.scope.applies_to(model, self.family)
            && match (&key.name, &self.pool) {
                (Some(expected), Some(actual)) => expected == actual,
                (None, _) => true,
                _ => false,
            }
    }
}

impl FenceRecheck {
    pub fn origin(&self) -> Origin<'_> {
        Origin {
            kind: &self.kind,
            scope: Scope {
                provider: &self.provider,
                account_id: self.account_id.as_deref(),
                credential_id: self.credential_id.as_deref(),
                model: self.model.as_deref(),
                size_class: self.size_class,
                principal: self.principal.as_deref(),
                admission_id: self.admission_id.as_deref(),
            },
            lifetime: LifetimeClass::from_quota(self.quota),
            cleared_models: &self.cleared_models,
        }
    }

    pub fn quota_group_due<'a>(
        origins: impl IntoIterator<Item = &'a Self>,
        now: i64,
        owner: Option<&str>,
    ) -> bool {
        Self::quota_group_at(origins, owner) <= now
    }
    pub fn quota_group_at<'a>(
        origins: impl IntoIterator<Item = &'a Self>,
        owner: Option<&str>,
    ) -> i64 {
        let origins: Vec<_> = origins
            .into_iter()
            .filter(|origin| {
                origin.quota && !(owner.is_some() && origin.claim_owner.as_deref() == owner)
            })
            .collect();
        let latest = origins
            .iter()
            .map(|origin| origin.evidence_at)
            .max()
            .unwrap_or(i64::MIN);
        // A reset can schedule an extra check, but cannot lend a second probe
        // to another decision while the first decision still owns the slot.
        if origins.iter().any(|origin| origin.claim_owner.is_some()) {
            return origins
                .iter()
                .map(|origin| origin.next_recheck_at)
                .max()
                .unwrap_or(i64::MIN);
        }
        origins
            .iter()
            .map(|origin| origin.next_recheck_at)
            .max()
            .unwrap_or(i64::MIN)
            .min(
                origins
                    .iter()
                    .filter_map(|origin| origin.reset_check_at)
                    .filter(|reset| *reset > latest)
                    .min()
                    .unwrap_or(i64::MAX),
            )
    }
    pub fn due_at(&self) -> i64 {
        self.reset_check_at.map_or(self.next_recheck_at, |reset| {
            reset.min(self.next_recheck_at)
        })
    }

    pub fn applies(&self, provider: &str, account: &str, credential: &str, model: &str) -> bool {
        (self.provider.is_empty() || self.provider == provider)
            && self.account_id.as_deref().is_none_or(|id| id == account)
            && self
                .credential_id
                .as_deref()
                .is_none_or(|id| id == credential)
            && self.model.as_deref().is_none_or(|id| id == model)
            && !self.cleared_models.iter().any(|cleared| cleared == model)
    }
}

pub(crate) fn enabled(conn: &Connection) -> rusqlite::Result<bool> {
    conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='block')",
    )?
    .query_row([], |row| row.get(0))
}

pub(crate) fn needs_sync(conn: &Connection) -> rusqlite::Result<bool> {
    if !enabled(conn)? {
        return Ok(false);
    }
    Ok(crate::BlockState::read(conn)?.needs_sync())
}

#[cfg(any(test, feature = "test-support"))]
#[path = "fence_rechecks/legacy/tests.rs"]
pub(crate) mod legacy;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use legacy::{quarantined_origins, retire_expired, snapshot_origins, sync, sync_at};

pub(crate) fn register(conn: &Connection, kind: &str, key: &str) -> rusqlite::Result<()> {
    if !enabled(conn)? {
        return Ok(());
    }
    let tx = crate::AccountTx::sql(conn);
    register_on(&tx, kind, key)
}

/// Reconcile the command's captured source rows with their durable clocks.
/// Evidence retired here is archived before its enforcement trigger clears it.
pub(crate) fn sync_on(tx: &crate::AccountTx<'_>, now: i64) -> rusqlite::Result<()> {
    if !enabled(tx.conn)? {
        return Ok(());
    }
    let state = match tx.working_blocks().map_err(command_error)? {
        Some(state) => state.into_owned(),
        None => crate::BlockState::read(tx.conn)?,
    };
    let retiring: Vec<_> = state
        .view_origins_in_scan_order()
        .into_iter()
        .filter(|origin| {
            origin.quota == Some(false)
                && origin
                    .deadline
                    .is_some_and(|deadline| deadline < now as f64)
                && state
                    .clock(&origin.kind, &origin.origin_key)
                    .is_none_or(|(clock, _)| clock.evidence != origin.evidence)
        })
        .collect();
    for origin in &retiring {
        state.check_source(tx.conn, &origin.kind, &origin.origin_key, &origin.provider)?;
        state.check_clock(tx.conn, &origin.kind, &origin.origin_key)?;
        tx.conn.execute(
            "INSERT OR REPLACE INTO retirement_tombstone VALUES (?1,?2,?3,?4,?5)",
            params![
                origin.kind,
                origin.origin_key,
                origin.evidence,
                origin.evidence_at,
                now
            ],
        )?;
    }
    let state = match tx.working_blocks().map_err(command_error)? {
        Some(state) => state.into_owned(),
        None => crate::BlockState::read(tx.conn)?,
    };
    let view = state.view_origins_in_scan_order();
    for origin in &view {
        upsert_clock_on(tx, &state, origin)?;
    }
    let live: std::collections::HashSet<_> = view
        .iter()
        .map(|origin| (origin.kind.as_str(), origin.origin_key.as_str()))
        .collect();
    let state = match tx.working_blocks().map_err(command_error)? {
        Some(state) => state.into_owned(),
        None => crate::BlockState::read(tx.conn)?,
    };
    for (clock, _) in state.stored_clocks().filter(|(clock, _)| {
        clock.kind != "observation"
            && !live.contains(&(clock.kind.as_str(), clock.origin_key.as_str()))
    }) {
        state.check_source(
            tx.conn,
            &clock.kind,
            &clock.origin_key,
            &clock.scope.provider,
        )?;
        delete_clock_on(tx, &clock.kind, &clock.origin_key, &clock.evidence)?;
    }
    Ok(())
}

pub(crate) fn register_on(
    tx: &crate::AccountTx<'_>,
    kind: &str,
    key: &str,
) -> rusqlite::Result<()> {
    if !enabled(tx.conn)? {
        return Ok(());
    }
    let state = match tx.working_blocks().map_err(command_error)? {
        Some(state) => state.into_owned(),
        None => crate::BlockState::read(tx.conn)?,
    };
    if let Some(origin) = state
        .view_origins_in_scan_order()
        .into_iter()
        .find(|origin| origin.kind == kind && origin.origin_key == key)
    {
        upsert_clock_on(tx, &state, &origin)?;
    }
    Ok(())
}

/// A number as SQLite computes it: an integer, or a real once integer
/// arithmetic overflows.
#[derive(Clone, Copy, Debug, PartialEq)]
enum SqlNumber {
    Integer(i64),
    Real(f64),
}

impl SqlNumber {
    /// `a + b` as SQLite adds two integers: on overflow the sum is real.
    fn add(a: i64, b: i64) -> Self {
        a.checked_add(b)
            .map_or(Self::Real(a as f64 + b as f64), Self::Integer)
    }

    /// `CAST(x AS INTEGER)`: a real truncates toward zero and saturates at
    /// the integer range, as Rust's `as` does.
    fn cast_integer(self) -> i64 {
        match self {
            Self::Integer(value) => value,
            Self::Real(value) => value as i64,
        }
    }

    /// Order as SQLite orders an integer and a real: by exact value, not
    /// by rounding the integer to a real.
    fn cmp(self, other: Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        fn real_vs_integer(real: f64, integer: i64) -> Ordering {
            if real < -9_223_372_036_854_775_808.0 {
                return Ordering::Less;
            }
            if real >= 9_223_372_036_854_775_808.0 {
                return Ordering::Greater;
            }
            let whole = real.trunc() as i64;
            whole.cmp(&integer).then_with(|| {
                (real - real.trunc())
                    .partial_cmp(&0.0)
                    .unwrap_or(Ordering::Equal)
            })
        }
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(&b),
            (Self::Real(a), Self::Real(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            (Self::Real(a), Self::Integer(b)) => real_vs_integer(a, b),
            (Self::Integer(a), Self::Real(b)) => real_vs_integer(b, a).reverse(),
        }
    }
}

/// A new clock's `next_recheck_at` and `reset_check_at`, exactly as
/// `upsert_clocks` computes them in SQL:
/// `CAST(CASE WHEN quota=1 THEN evidence_at+900000 ELSE
/// MIN(COALESCE(deadline,evidence_at+3600000),evidence_at+3600000) END AS INTEGER)`
/// and `CASE WHEN quota=1 AND reset_at>evidence_at THEN reset_at END`,
/// including SQLite's promotion of an overflowing sum to a real and its
/// saturating cast.
pub(crate) fn clock_schedule(
    quota: bool,
    evidence_at: i64,
    deadline: Option<f64>,
    reset_at: Option<f64>,
) -> (i64, Option<f64>) {
    let next = if quota {
        SqlNumber::add(evidence_at, 900_000)
    } else {
        let cap = SqlNumber::add(evidence_at, 3_600_000);
        let first = deadline.map_or(cap, SqlNumber::Real);
        // SQLite's min() keeps the first of equal arguments.
        if cap.cmp(first) == std::cmp::Ordering::Less {
            cap
        } else {
            first
        }
    };
    let reset = reset_at.filter(|reset| {
        quota
            && SqlNumber::Real(*reset).cmp(SqlNumber::Integer(evidence_at))
                == std::cmp::Ordering::Greater
    });
    (next.cast_integer(), reset)
}

/// Install the clock on the same row as the newly captured source. History
/// kept while replacing evidence prevents a late report from shortening a
/// clock that an already-claimed probe moved into the future.
fn upsert_clock_on(
    tx: &crate::AccountTx<'_>,
    state: &crate::BlockState,
    origin: &ViewOrigin,
) -> rusqlite::Result<()> {
    if state.clock(&origin.kind, &origin.origin_key).is_some() {
        return Ok(());
    }
    state.check_source(tx.conn, &origin.kind, &origin.origin_key, &origin.provider)?;
    let Some(prior) = state.pending_clock(&origin.kind, &origin.origin_key) else {
        return Err(crate::block_state::diverged(
            "source missing while registering its clock",
        ));
    };
    let quota = origin.quota.unwrap_or(false);
    let (mut next, reset) =
        clock_schedule(quota, origin.evidence_at, origin.deadline, origin.reset_at);
    let claimed = prior.last_claimed_at.unwrap_or(i64::MIN);
    if origin.evidence_at < claimed {
        next = next.max(prior.next_recheck_at);
    }
    let reset = reset.filter(|reset| *reset > claimed as f64);
    let changed = tx.conn.execute(
        "UPDATE block SET quota=?3,classified=0,next_recheck_at=?4,reset_check_at=?5,
        managed=1,cleared_at=NULL,cleared_by=NULL,claim_owner=NULL,cleared_models='[]'
        WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND source_present=1 AND managed=0 AND evidence=?6
            AND next_recheck_at=?7 AND last_claimed_at IS ?8 AND claim_generation=?9",
        params![
            origin.kind,
            origin.origin_key,
            quota,
            next,
            reset,
            origin.evidence,
            prior.next_recheck_at,
            prior.last_claimed_at,
            prior.claim_generation
        ],
    )?;
    crate::block_state::expect_changes(
        changed,
        1,
        format_args!(
            "registering the clock of {} {}",
            origin.kind, origin.origin_key
        ),
    )
}

fn delete_clock_on(
    tx: &crate::AccountTx<'_>,
    kind: &str,
    key: &str,
    evidence: &str,
) -> rusqlite::Result<()> {
    let changed=tx.conn.execute("UPDATE block SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0 WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND evidence=?3 AND managed=1",params![kind,key,evidence])?;
    crate::block_state::expect_changes(
        changed,
        1,
        format_args!("dropping the clock of {kind} {key}"),
    )
}

/// A command-level error carried through SQL code: its text, which keeps a
/// `STATE_DIVERGED` marker, is what the writer looks for.
pub(crate) fn command_error(error: striatum_core::RouteStateStoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(error.0.into())
}

/// Whether a route or provider row holds one account's error rather than a
/// route or provider wall. Such a row is kept for diagnosis but enforces
/// nothing, so its origin gets no block.
pub(crate) fn quarantined_row(unscoped: bool, error: Option<&str>, class: Option<&str>) -> bool {
    let class = class.and_then(|class| striatum_core::RouteWallClass::parse(class).ok());
    !unscoped && striatum_core::account_wall::account_scoped_wall(error, class)
}

pub(crate) fn read(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    if !enabled(conn)? {
        return Ok(Vec::new());
    }
    Ok(crate::BlockState::read(conn)?.clock_rechecks())
}

pub(crate) fn read_effective(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    read_effective_at(conn, crate::now_ms())
}

pub(crate) fn read_effective_at(
    conn: &Connection,
    now: i64,
) -> rusqlite::Result<Vec<FenceRecheck>> {
    if !enabled(conn)? {
        return Ok(Vec::new());
    }
    crate::BlockState::read(conn)?
        .fence_rechecks(now)
        .map_err(command_error)
}

/// Source evidence and clocks loaded without modifying either. Cleared evidence
/// is kept separately so a rehearsal can account for cleared entries as well.
#[derive(Clone, Debug, Default)]
pub struct LoadedBlocks {
    pub blocks: Vec<Block>,
    pub cleared_evidence: Vec<Block>,
}

impl From<&Block> for FenceRecheck {
    fn from(block: &Block) -> Self {
        Self {
            kind: block.kind.clone(),
            origin_key: block.origin_key.clone(),
            provider: block.scope.provider.clone(),
            account_id: block.scope.account_id.clone(),
            credential_id: block.scope.credential_id.clone(),
            model: block.scope.model.clone(),
            size_class: block.scope.size_class,
            principal: block.scope.principal.clone(),
            admission_id: block.scope.admission_id.clone(),
            evidence: block.evidence.clone(),
            evidence_at: block.evidence_at,
            quota: block.lifetime == LifetimeClass::Quota,
            classified: block.classified,
            next_recheck_at: block.next_recheck_at,
            reset_check_at: block.reset_check_at,
            last_claimed_at: block.last_claimed_at,
            claim_owner: block.claim_owner.clone(),
            cleared_models: block.cleared_models.clone(),
        }
    }
}

fn cause(kind: &str) -> rusqlite::Result<Cause> {
    Cause::for_kind(kind)
        .ok_or_else(|| rusqlite::Error::InvalidParameterName(format!("unknown block kind {kind}")))
}

fn load_blocks(conn: &Connection, now: i64) -> rusqlite::Result<LoadedBlocks> {
    if !enabled(conn)? {
        return Ok(LoadedBlocks::default());
    }
    crate::BlockState::read(conn)?
        .blocks(now)
        .map_err(command_error)
}

/// The `block` columns [`clock_row`] decodes, in its order.
pub(crate) const CLOCK_COLUMNS: &str = "kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,classified,next_recheck_at,reset_check_at,last_claimed_at,claim_owner,claim_generation,cleared_at";

/// Stored clocks keyed by source kind and origin key, each with its
/// `cleared_at`.
pub(crate) type Clocks = std::collections::BTreeMap<(String, String), (Block, Option<i64>)>;

/// One `block` row, decoded from [`CLOCK_COLUMNS`] starting at `at`.
pub(crate) fn clock_row(
    r: &rusqlite::Row<'_>,
    at: usize,
) -> rusqlite::Result<(Block, Option<i64>)> {
    let kind: String = r.get(at)?;
    Ok((
        Block {
            cause: cause(&kind)?,
            kind,
            origin_key: r.get(at + 1)?,
            scope: Scope {
                provider: r.get(at + 2)?,
                account_id: r.get(at + 3)?,
                credential_id: r.get(at + 4)?,
                model: r.get(at + 5)?,
                size_class: r.get(at + 6)?,
                principal: r.get(at + 7)?,
                admission_id: r.get(at + 8)?,
            },
            evidence: r.get(at + 9)?,
            evidence_at: r.get(at + 10)?,
            lifetime: LifetimeClass::from_quota(r.get(at + 11)?),
            classified: r.get(at + 12)?,
            next_recheck_at: r.get(at + 13)?,
            reset_check_at: r.get(at + 14)?,
            last_claimed_at: r.get(at + 15)?,
            claim_owner: r.get(at + 16)?,
            claim_generation: r.get(at + 17)?,
            cleared_models: Vec::new(),
        },
        r.get::<_, Option<i64>>(at + 18)?,
    ))
}

/// One `fence_recheck_route_clear` row: a model cleared from a route-wide
/// origin while that origin's evidence is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteClear {
    pub(crate) kind: String,
    pub(crate) origin_key: String,
    pub(crate) evidence: String,
    pub(crate) model: String,
}

/// Attach each clear, in the order given, to the clock whose current evidence
/// it names. A clear recorded for older evidence no longer applies.
pub(crate) fn attach_route_clears(clocks: &mut Clocks, clears: &[RouteClear]) {
    for clear in clears {
        if let Some((block, _)) = clocks.get_mut(&(clear.kind.clone(), clear.origin_key.clone())) {
            if block.evidence == clear.evidence {
                block.cleared_models.push(clear.model.clone());
            }
        }
    }
}

/// One row of the `fence_origin` view: an origin's current source evidence.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ViewOrigin {
    pub kind: String,
    pub origin_key: String,
    pub provider: String,
    pub account_id: Option<String>,
    pub credential_id: Option<String>,
    pub model: Option<String>,
    pub size_class: Option<i64>,
    pub principal: Option<String>,
    pub admission_id: Option<String>,
    pub evidence: String,
    pub evidence_at: i64,
    pub quota: Option<bool>,
    pub deadline: Option<f64>,
    pub reset_at: Option<f64>,
}

/// Decode the view's 14 columns starting at `at`, in the view's order.
pub(crate) fn view_origin(r: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<ViewOrigin> {
    Ok(ViewOrigin {
        kind: r.get(at)?,
        origin_key: r.get(at + 1)?,
        provider: r.get(at + 2)?,
        account_id: r.get(at + 3)?,
        credential_id: r.get(at + 4)?,
        model: r.get(at + 5)?,
        size_class: r.get(at + 6)?,
        principal: r.get(at + 7)?,
        admission_id: r.get(at + 8)?,
        evidence: r.get(at + 9)?,
        evidence_at: r.get(at + 10)?,
        quota: r.get(at + 11)?,
        deadline: r.get(at + 12)?,
        reset_at: r.get(at + 13)?,
    })
}

/// Merge source evidence with the stored clocks. The SQL adapter and the
/// in-memory block state both call this, so they differ only in where the
/// rows come from. Clocks must already carry their route clears. An unchanged
/// source signature selects the stored class and schedule; a new one derives
/// them.
pub(crate) fn derive_blocks(
    mut clocks: Clocks,
    quarantined: &[(String, String)],
    origins: impl IntoIterator<Item = ViewOrigin>,
    now: i64,
) -> rusqlite::Result<LoadedBlocks> {
    let mut loaded = LoadedBlocks {
        blocks: Vec::new(),
        cleared_evidence: clocks
            .values()
            .filter(|(_, cleared)| cleared.is_some())
            .map(|(block, _)| block.clone())
            .collect(),
    };
    for origin in origins {
        let key = (origin.kind.clone(), origin.origin_key.clone());
        if quarantined.contains(&key) {
            continue;
        }
        let prior = clocks.remove(&key);
        if let Some((block, cleared)) = &prior {
            if block.evidence == origin.evidence {
                if cleared.is_none() {
                    loaded.blocks.push(block.clone());
                }
                continue;
            }
        }
        let evidence_at = origin.evidence_at;
        let quota = origin.quota.unwrap_or(false);
        let deadline = origin.deadline.map(|at| at as i64);
        if !quota && deadline.is_some_and(|at| at < now) {
            continue;
        }
        let last_claimed_at = prior.as_ref().and_then(|(b, _)| b.last_claimed_at);
        let mut next = if quota {
            evidence_at.saturating_add(QUOTA_FENCE_RECHECK_MS)
        } else {
            let cap = evidence_at.saturating_add(NON_QUOTA_FENCE_MAX_MS);
            deadline.unwrap_or(cap).min(cap)
        };
        if last_claimed_at.is_some_and(|at| evidence_at < at) {
            next = next.max(prior.as_ref().unwrap().0.next_recheck_at);
        }
        let reset = origin.reset_at.map(|at| at as i64).filter(|at| {
            quota && *at > evidence_at && last_claimed_at.is_none_or(|claimed| *at > claimed)
        });
        loaded.blocks.push(Block {
            cause: cause(&origin.kind)?,
            kind: origin.kind,
            origin_key: origin.origin_key,
            scope: Scope {
                provider: origin.provider,
                account_id: origin.account_id,
                credential_id: origin.credential_id,
                model: origin.model,
                size_class: origin.size_class,
                principal: origin.principal,
                admission_id: origin.admission_id,
            },
            lifetime: LifetimeClass::from_quota(quota),
            classified: false,
            evidence: origin.evidence,
            evidence_at,
            next_recheck_at: next,
            reset_check_at: reset,
            last_claimed_at,
            claim_owner: None,
            claim_generation: 0,
            cleared_models: Vec::new(),
        });
    }
    loaded.blocks.extend(
        clocks
            .into_values()
            .filter(|(block, cleared)| block.kind == "observation" && cleared.is_none())
            .map(|(block, _)| block),
    );
    Ok(loaded)
}

/// Whether `row` authorizes the test request its clock is on now: the clock
/// is uncleared, holds the authorized evidence, and is on the authorized
/// claim generation with no other owner, or, for an admission's own
/// authorization (`admission_owned`), is claimed by that admission. The
/// SQL form is [`current_test_join`].
fn test_is_current(
    blocks: &crate::BlockState,
    row: &crate::block_state::ProbeAuthorization,
    admission_owned: bool,
) -> bool {
    blocks
        .clock(&row.kind, &row.origin_key)
        .is_some_and(|(clock, cleared)| {
            let owner = clock.claim_owner.as_deref();
            cleared.is_none()
                && clock.evidence == row.evidence
                && clock.evidence_at == row.evidence_at
                && ((owner.is_none_or(|by| by == row.owner)
                    && clock.claim_generation == row.claim_generation)
                    || (admission_owned && owner == Some(row.owner.as_str())))
        })
}

/// [`test_is_current`] as a join of authorization `a` to its clock `r`, with
/// the admission flag bound at `?{flag}`.
fn current_test_join(flag: usize) -> String {
    format!(
        "JOIN block r ON r.is_origin=1 AND r.managed=1 AND r.kind=a.kind AND r.origin_key=a.origin_key
         AND r.evidence=a.evidence AND r.evidence_at=a.evidence_at AND r.cleared_at IS NULL
         AND (((r.claim_owner IS NULL OR r.claim_owner=a.owner) AND r.claim_generation=a.claim_generation)
             OR (?{flag} AND r.claim_owner=a.owner))"
    )
}

/// The clause naming the uncleared clock a decision read. Its seven values
/// are bound from `?{first}` on, in this order: evidence, evidence_at, quota,
/// next_recheck_at, reset_check_at, claim_owner, claim_generation. An update
/// carrying it changes the clock only if SQL still holds what the working
/// state showed.
fn clock_held(first: usize) -> String {
    let at = |offset: usize| first + offset;
    format!(
        "is_origin=1 AND managed=1 AND evidence=?{} AND evidence_at=?{} AND cleared_at IS NULL AND quota=?{} AND next_recheck_at=?{}
         AND reset_check_at IS ?{} AND claim_owner IS ?{} AND claim_generation=?{}",
        at(0), at(1), at(2), at(3), at(4), at(5), at(6)
    )
}

/// Claim `origins` on SQL after synchronizing the clocks, as a connection
/// without the account state would.
#[cfg(test)]
pub(crate) fn claim(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
) -> rusqlite::Result<bool> {
    sync_on(&crate::AccountTx::sql(conn), now)?;
    claim_decided(conn, None, origins, now, None, None, None)
}

#[cfg(test)]
pub(crate) fn claim_with_authorization(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
    owner: &str,
) -> rusqlite::Result<bool> {
    sync(conn)?;
    claim_decided(conn, None, origins, now, None, None, Some(owner))
}

/// The claim fields of the probe clock that holds one origin's exact
/// evidence, as a claim decides on them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ClaimState {
    quota: bool,
    next: i64,
    reset: Option<i64>,
    owner: Option<String>,
    generation: i64,
}

/// Each origin's uncleared clock that still holds the origin's evidence:
/// from the working state when the command has one, else from SQL.
fn claim_states(
    conn: &Connection,
    blocks: Option<&crate::BlockState>,
    origins: &[&FenceRecheck],
) -> rusqlite::Result<Vec<Option<ClaimState>>> {
    if let Some(blocks) = blocks {
        let clocks = blocks.live_clocks();
        return Ok(origins
            .iter()
            .map(|origin| {
                clocks
                    .get(&(origin.kind.as_str(), origin.origin_key.as_str()))
                    .filter(|clock| {
                        clock.evidence_at == origin.evidence_at && clock.evidence == origin.evidence
                    })
                    .map(|clock| ClaimState {
                        quota: clock.lifetime == LifetimeClass::Quota,
                        next: clock.next_recheck_at,
                        reset: clock.reset_check_at,
                        owner: clock.claim_owner.clone(),
                        generation: clock.claim_generation,
                    })
            })
            .collect());
    }
    origins
        .iter()
        .map(|origin| {
            conn.query_row(
                "SELECT quota,next_recheck_at,reset_check_at,claim_owner,claim_generation FROM block
             WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1 AND evidence_at=?3 AND evidence=?4 AND cleared_at IS NULL",
                params![origin.kind, origin.origin_key, origin.evidence_at, origin.evidence],
                |row| {
                    Ok(ClaimState {
                        quota: row.get(0)?,
                        next: row.get(1)?,
                        reset: row.get(2)?,
                        owner: row.get(3)?,
                        generation: row.get(4)?,
                    })
                },
            )
            .optional()
        })
        .collect()
}

/// A keyed check written as a no-op update: it changes no row, and its
/// count of matched rows is the answer. Whether SQL still holds the clock an
/// authorization names, with that evidence and claim generation (and, with
/// `model`, as a non-quota clock of that model or of none).
pub(crate) fn no_authorized_clock_check_sql(model: bool) -> String {
    format!(
        "{} UPDATE block SET claim_owner=claim_owner WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1
            AND cleared_at IS NULL AND evidence=?3 AND evidence_at=?4 AND claim_generation=?5{}",
        crate::block_state::STATE_CHECK,
        if model {
            " AND quota=0 AND (model IS NULL OR model=?6)"
        } else {
            ""
        }
    )
}

/// A keyed no-op update that answers whether an admission is still
/// unstarted (`reserved` or `expired`) and, if so, its deadline.
pub(crate) fn unstarted_admission_check_sql() -> String {
    format!(
        "{} UPDATE account_admission SET state = state WHERE principal=?1 \
           AND admission_id=?2 AND state IN ('reserved','expired') \
           AND started_at IS NULL RETURNING admission_valid_until",
        crate::block_state::STATE_CHECK
    )
}

/// The keyed check of one owner's probe authorizations against the state.
pub(crate) fn authorization_check_sql() -> String {
    format!(
        "{} SELECT owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation
                FROM probe_authorization WHERE owner=?1 ORDER BY kind,origin_key",
        crate::block_state::STATE_CHECK
    )
}

/// The keyed check of the clock an authorization names against the state.
pub(crate) fn probe_clock_check_sql() -> String {
    format!(
        "{} SELECT evidence,evidence_at,claim_generation,credential_id,model,cleared_at,claim_owner
                    FROM block WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1",
        crate::block_state::STATE_CHECK
    )
}

/// Claim every origin's probe clock or none of them, deciding on `blocks`
/// when given (the command's working state) and on SQL otherwise. The caller
/// synchronizes the clocks first. Each clock update is keyed and names the
/// state it was decided on; a clock SQL holds differently fails the command
/// with [`crate::block_state::diverged`].
///
/// It checks only the clocks it is given. Which clocks apply was decided by
/// the caller from the state (a route clear, for one, removes a clock), so an
/// empty or shortened list claims nothing and checks nothing: the caller must
/// have checked the rows that chose the list, as pool leases do with
/// `check_lease_inputs` before calling it.
fn claim_decided(
    conn: &Connection,
    blocks: Option<&crate::BlockState>,
    origins: &[FenceRecheck],
    now: i64,
    consume_owner: Option<&str>,
    grant_owner: Option<&str>,
    authorization: Option<&str>,
) -> rusqlite::Result<bool> {
    // A clock is claimed once per call, even if a caller lists its origin
    // twice: a second update would not find the state the first one left.
    let mut seen = std::collections::HashSet::new();
    let origins: Vec<&FenceRecheck> = origins
        .iter()
        .filter(|origin| seen.insert((origin.kind.as_str(), origin.origin_key.as_str())))
        .collect();
    // Check the entire conjunction before updating any clock. BEGIN IMMEDIATE
    // serializes distinct connections; a lost race consumes none of the clocks.
    let mut states = Vec::new();
    let mut quota_next = i64::MIN;
    let mut quota_evidence = i64::MIN;
    let mut quota_resets = Vec::new();
    for (origin, state) in origins
        .iter()
        .copied()
        .zip(claim_states(conn, blocks, &origins)?)
    {
        let Some(state) = state else {
            return Ok(false);
        };
        let ClaimState {
            quota,
            next,
            reset,
            ref owner,
            ..
        } = state;
        if quota != origin.quota {
            return Ok(false);
        }
        let owned = consume_owner.is_some() && owner.as_deref() == consume_owner;
        if !owned && owner.is_some() && next > now {
            return Ok(false);
        }
        // Admission reuses its decision's generation even if a provider reset
        // arrived while the decision held the slot.
        let due = if owned {
            next <= now
        } else {
            reset.map_or(next, |reset| reset.min(next)) <= now
        };
        if !quota && !due && !owned {
            return Ok(false);
        }
        if quota && !owned {
            quota_next = quota_next.max(next);
            quota_evidence = quota_evidence.max(origin.evidence_at);
            quota_resets.extend(reset);
        }
        states.push((origin, owned, due, state));
    }
    // Quota representations of the same selected account share one periodic
    // check. A reset after its latest refusal adds a check for the whole
    // conjunction, so an individual credential's quota record on that account
    // cannot block the account-wide probe.
    if quota_next > now
        && !quota_resets
            .into_iter()
            .any(|reset| reset > quota_evidence && reset <= now)
    {
        return Ok(false);
    }
    // The clock state each update was decided on, after the keyed columns.
    const DECIDED: &str = "is_origin=1 AND managed=1 AND evidence=:evidence AND evidence_at=:evidence_at AND cleared_at IS NULL
        AND quota=:quota AND next_recheck_at=:next AND reset_check_at IS :reset
        AND claim_owner IS :owner AND claim_generation=:generation";
    for (origin, owned, due, state) in states {
        // Admission changes the launch time but preserves the claim generation.
        // A cancellation can release only that claim, not a later request's.
        let generation = if owned && !due {
            state.generation
        } else {
            state.generation.saturating_add(1)
        };
        for owner in [authorization.or(grant_owner), consume_owner]
            .into_iter()
            .flatten()
        {
            conn.execute("INSERT INTO probe_authorization(owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation)
                VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(owner,kind,origin_key) DO UPDATE SET
                evidence=excluded.evidence,evidence_at=excluded.evidence_at,claimed_at=excluded.claimed_at,
                claim_generation=excluded.claim_generation",
                params![owner,origin.kind,origin.origin_key,origin.evidence,origin.evidence_at,now,generation])?;
        }
        let decided = rusqlite::named_params! {
            ":kind": origin.kind, ":key": origin.origin_key, ":evidence": origin.evidence,
            ":evidence_at": origin.evidence_at, ":quota": state.quota, ":next": state.next,
            ":reset": state.reset, ":owner": state.owner, ":generation": state.generation,
            ":grant": grant_owner, ":now": now,
        };
        let changed = if owned && !due {
            conn.execute(
                &format!(
                    "UPDATE block SET claim_owner=:grant,
                    reset_check_at=CASE WHEN reset_check_at>:now THEN reset_check_at END
                    WHERE kind=:kind AND origin_key=:key AND {DECIDED}"
                ),
                decided,
            )?
        } else {
            let interval = if origin.quota {
                QUOTA_FENCE_RECHECK_MS
            } else {
                NON_QUOTA_FENCE_MAX_MS
            };
            let mut with_next = decided.to_vec();
            let next = now.saturating_add(interval);
            with_next.push((":renewed", &next as &dyn rusqlite::ToSql));
            with_next.push((":generation_new", &generation as &dyn rusqlite::ToSql));
            conn.execute(
                &format!(
                    "UPDATE block SET next_recheck_at=:renewed,last_claimed_at=:now,claim_owner=:grant,
                    claim_generation=:generation_new,reset_check_at=CASE WHEN reset_check_at>:now THEN reset_check_at END
                    WHERE kind=:kind AND origin_key=:key AND {DECIDED}"
                ),
                with_next.as_slice(),
            )?
        };
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("claim of {} {}", origin.kind, origin.origin_key),
        )?;
    }
    Ok(true)
}

impl SqliteRouteStateStore {
    pub fn load_blocks(&self, now: i64) -> RouteStateResult<LoadedBlocks> {
        self.store
            .with_read(|conn| load_blocks(conn, now))
            .map_err(err)
    }

    /// Boundary domain includes source deadlines even when a stored clock has
    /// capped them, and all persisted probe clocks, each with its neighbours.
    #[cfg(any(test, feature = "test-support"))]
    pub fn block_parity_times(&self, now: i64) -> RouteStateResult<Vec<i64>> {
        self.store
            .with_read(|conn| Ok(parity_times(conn, now)?.into_iter().collect()))
            .map_err(err)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn block_parity_identities(&self) -> RouteStateResult<Vec<[String; 4]>> {
        self.store.with_read(parity_identities).map_err(err)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn effective_blocks_oracle_for_test(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<FenceRecheck>> {
        self.store
            .with_read(|conn| legacy::read_effective_at(conn, now))
            .map_err(err)
    }

    /// Compare in one read snapshot, so a concurrent report cannot produce a
    /// false difference between the adapter and the SQL oracle.
    #[cfg(any(test, feature = "test-support"))]
    pub fn block_parity(&self, now: i64) -> RouteStateResult<BlockParity> {
        self.store.with_read(|conn| {
            let tx = conn;
            let started = std::time::Instant::now();
            let loaded = legacy::load_blocks(&tx, now)?;
            let load_time = started.elapsed();
            let rows = legacy::read_effective_at(&tx, now)?;
            let mut report = BlockParity { load_time, active: loaded.blocks.len(), cleared_evidence: loaded.cleared_evidence.len(), identities: 0, combinations_per_identity: 0, function_comparisons: 0, differences: Vec::new() };
            let canonical = |rows: Vec<FenceRecheck>| -> std::collections::BTreeMap<_,_> {
                rows.into_iter().map(|mut row| {
                    row.cleared_models.sort();
                    ((row.kind.clone(), row.origin_key.clone()),row)
                }).collect()
            };
            let expected = canonical(rows);
            let actual = canonical(loaded.blocks.iter().map(FenceRecheck::from).collect());
            for key in expected.keys().chain(actual.keys()).collect::<std::collections::BTreeSet<_>>() {
                if expected.get(key) != actual.get(key) {
                    report.differences.push(format!("row {key:?}: SQL={:?}, block={:?}", expected.get(key), actual.get(key)));
                }
            }
            for block in &loaded.blocks {
                let clock: Option<(String,i64)> = tx.query_row("SELECT evidence,claim_generation FROM fence_recheck WHERE kind=?1 AND origin_key=?2", params![block.kind,block.origin_key], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
                if let Some((evidence,generation)) = clock {
                    if evidence == block.evidence && generation != block.claim_generation {
                        report.differences.push(format!("claim generation {} {}: clock={generation}, block={}",block.kind,block.origin_key,block.claim_generation));
                    }
                }
            }
            compare_block_functions(&tx, now, &expected, &loaded.blocks, &mut report)?;
            Ok(report)
        }).map_err(err)
    }

    pub fn bind_fence_probe_owner(&self, from: &str, to: &str) -> RouteStateResult<()> {
        let from = from.to_owned();
        let to = to.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.bind_fence_probe_owner(&from, &to)
        })
    }
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        // Scoring overlays newly written evidence without opening a writer.
        // Admission resyncs and claims the durable schedules atomically.
        self.store.with_read(read_effective).map_err(err)
    }
}

#[derive(Clone, Debug)]
pub struct BlockParity {
    pub load_time: std::time::Duration,
    pub active: usize,
    pub cleared_evidence: usize,
    pub identities: usize,
    pub combinations_per_identity: usize,
    pub function_comparisons: u64,
    pub differences: Vec<String>,
}

#[cfg(any(test, feature = "test-support"))]
fn parity_times(conn: &Connection, now: i64) -> rusqlite::Result<std::collections::BTreeSet<i64>> {
    let mut times = std::collections::BTreeSet::from([now]);
    let mut statement = conn.prepare("SELECT next_recheck_at FROM fence_recheck UNION SELECT reset_check_at FROM fence_recheck UNION SELECT last_claimed_at FROM fence_recheck UNION SELECT deadline FROM fence_origin UNION SELECT reset_at FROM fence_origin")?;
    for at in statement.query_map([], |r| r.get::<_, Option<f64>>(0))? {
        if let Some(at) = at? {
            let at = at as i64;
            times.extend([at.saturating_sub(1), at, at.saturating_add(1)]);
        }
    }
    Ok(times)
}

#[cfg(any(test, feature = "test-support"))]
fn parity_identities(conn: &Connection) -> rusqlite::Result<Vec<[String; 4]>> {
    use std::collections::BTreeSet;
    let mut statement = conn.prepare("SELECT provider,account_id,credential_id,model FROM fence_origin UNION SELECT provider,account_id,credential_id,model FROM fence_recheck UNION SELECT provider,NULL,NULL,model FROM fence_recheck_route_clear")?;
    let scopes: Vec<(String, Option<String>, Option<String>, Option<String>)> = statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut identities = BTreeSet::new();
    for provider in scopes.iter().map(|s| &s.0).collect::<BTreeSet<_>>() {
        let related: Vec<_> = scopes
            .iter()
            .filter(|s| s.0.is_empty() || &s.0 == provider)
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
                        account.to_owned(),
                        (*credential).to_owned(),
                        (*model).to_owned(),
                    ]);
                }
            }
        }
    }
    Ok(identities.into_iter().collect())
}

#[cfg(any(test, feature = "test-support"))]
fn compare_block_functions(
    conn: &Connection,
    now: i64,
    rows: &std::collections::BTreeMap<(String, String), FenceRecheck>,
    blocks: &[Block],
    report: &mut BlockParity,
) -> rusqlite::Result<()> {
    use striatum_core::blocks::{self, Target};
    let identities = parity_identities(conn)?;
    let times = parity_times(conn, now)?;
    report.identities = identities.len();
    // Alias mapping, account-pool applicability, recovery and binding reading:
    // all 16 boolean combinations, three owners and both continuation/media
    // flags. The last two flags are deliberately inert in per-origin decisions.
    report.combinations_per_identity = times.len() * 16 * 3 * 2 * 2;
    let pairs: Vec<_> = blocks
        .iter()
        .filter_map(|block| {
            rows.get(&(block.kind.clone(), block.origin_key.clone()))
                .map(|row| (block, row))
        })
        .collect();
    for [provider, account, credential, model] in &identities {
        let (provider, account, credential, model) = (
            provider.as_str(),
            account.as_str(),
            credential.as_str(),
            model.as_str(),
        );
        let target = Target {
            provider,
            account,
            credential,
            model,
        };
        for at in &times {
            for mask in 0..16 {
                for owner in 0..3 {
                    for continuation in [false, true] {
                        for media in [false, true] {
                            let alias = mask & 1 != 0;
                            let pool = mask & 2 != 0;
                            let recovered = mask & 4 != 0;
                            let binding = mask & 8 != 0;
                            for (block, row) in &pairs {
                                let named = row
                                    .credential_id
                                    .as_deref()
                                    .filter(|_| {
                                        (row.quota
                                            || matches!(
                                                row.kind.as_str(),
                                                "account_route"
                                                    | "non_quota_route"
                                                    | "transient_route"
                                            ))
                                            && alias
                                    })
                                    .unwrap_or(credential);
                                let old_targets = row.applies(provider, account, named, model);
                                let new_targets = blocks::targets(
                                    &block.origin(),
                                    target,
                                    alias.then_some(account),
                                );
                                let old_applies = row.principal.is_none()
                                    && row.size_class.is_none()
                                    && old_targets
                                    && (row.kind != "account" || pool);
                                let new_applies = blocks::origin_applies(
                                    &block.origin(),
                                    || new_targets,
                                    || pool,
                                );
                                let owned = owner == 1 && row.claim_owner.is_some();
                                let old_due = recovered
                                    || ((row.due_at() <= *at || owned) && (!row.quota || !binding));
                                let new_due = blocks::due(
                                    block.lifetime,
                                    recovered,
                                    block.reset_check_at.map_or(block.next_recheck_at, |reset| {
                                        reset.min(block.next_recheck_at)
                                    }),
                                    *at,
                                    owned,
                                    || binding,
                                );
                                let old_eligible = matches!(
                                    row.kind.as_str(),
                                    "account"
                                        | "account_route"
                                        | "non_quota_route"
                                        | "transient_route"
                                        | "observation"
                                        | "route"
                                        | "provider"
                                        | "failure"
                                ) || !old_applies
                                    || old_due;
                                let new_eligible =
                                    blocks::eligible(&block.origin(), || new_applies, || new_due);
                                let old_probe = old_applies && old_due;
                                let new_probe = blocks::probe_eligible(new_applies, || new_due);
                                report.function_comparisons += 3;
                                if old_targets != new_targets
                                    || old_applies != new_applies
                                    || old_eligible != new_eligible
                                    || old_probe != new_probe
                                {
                                    report.differences.push(format!("functions origin=({}, {}) identity={target:?} now={at} mask={mask} owner={owner} continuation={continuation} media={media}: targets {old_targets}/{new_targets}, applies {old_applies}/{new_applies}, eligible {old_eligible}/{new_eligible}, probe {old_probe}/{new_probe}",block.kind,block.origin_key));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// Start from the sparse authorization table, then look up each admission by
// its primary key instead of scanning the admission history on every request.
// CROSS JOIN is SQLite's way to pin that loop order: with a plain JOIN the
// planner preferred scanning all of account_admission (hundreds of thousands
// of rows live) and probing the authorization index for each one, which held
// the account writer for hundreds of milliseconds on every Tick.
/// What [`AccountTx::release_admission_probes`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReleasedAdmissionProbes {
    /// Admissions whose probe was released: each was unstarted, in
    /// `reserved` or `expired`.
    pub released: usize,
    /// Of those, the admissions it moved from `reserved` to `expired`.
    pub expired: Vec<(String, String)>,
}

pub(crate) const EXPIRED_ADMISSION_PROBES: &str =
    "SELECT DISTINCT a.principal,a.admission_id,p.owner
    FROM probe_authorization p CROSS JOIN account_admission a
      ON a.principal=json_extract(CASE WHEN json_valid(p.owner) THEN p.owner END,'$[1]')
      AND a.admission_id=json_extract(CASE WHEN json_valid(p.owner) THEN p.owner END,'$[2]')
    WHERE p.owner=json_array('account_admission',a.principal,a.admission_id)
      AND a.state IN ('reserved','expired') AND a.started_at IS NULL
      AND a.admission_valid_until<=?1";

impl AccountTx<'_> {
    /// Arm real fixture blocks without changing their evidence or admissions.
    /// A block's retest clock is a `block` row, and reads derive a
    /// default clock for any block that has none stored. `sync` writes those
    /// missing rows first, so the update below has a row to make due even for
    /// a quota block that has only been read so far.
    #[cfg(any(test, feature = "test-support"))]
    pub fn arm_observe_retest_for_test(
        &self,
        provider: &str,
        account: &str,
        credential: &str,
    ) -> RouteStateResult<()> {
        sync_on(self, crate::now_ms()).map_err(err)?;
        self.conn
            .execute(
                "UPDATE block SET next_recheck_at=0,reset_check_at=NULL,claim_owner=NULL
            WHERE provider=?1 AND (account_id=?2 OR credential_id=?3)",
                params![provider, account, credential],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// A selected probe may be sent without account admission. Persist its
    /// interval and intended credential together so its report can settle only
    /// that probe, even after the selection's in-memory hold expires.
    pub fn bind_decision_probe(
        &self,
        origins: &[FenceRecheck],
        from: &str,
        decision: &str,
        account: &str,
        credential: &str,
        now: i64,
    ) -> RouteStateResult<bool> {
        if origins.is_empty() {
            return Ok(true);
        }
        if origins
            .iter()
            .all(|origin| origin.claim_owner.as_deref() == Some(from))
        {
            self.transfer_probe_authorization(from, decision)?;
            self.rebind_claim_owner(from, decision)?;
        } else if !self.grant_fence_rechecks(origins, now, Some(decision))? {
            return Ok(false);
        }
        let identity = striatum_core::route_state_store::ProviderOutcome {
            account_ref: account.into(),
            credential_id: Some(credential.into()),
            ..Default::default()
        };
        self.conn
            .execute(
                "UPDATE model_route_decisions SET provider_identity_json=?2 WHERE id=?1",
                params![decision, serde_json::to_string(&identity).map_err(err)?],
            )
            .map_err(err)?;
        Ok(true)
    }

    /// A report naming another credential, or an expired/replaced generation,
    /// cannot settle the selected probe. Reports without a probe authorization
    /// keep their ordinary account-evidence behavior.
    pub fn probe_report_matches(
        &self,
        owner: &str,
        credential: Option<&str>,
        preserve: bool,
    ) -> RouteStateResult<bool> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(true);
        }
        sync_on(self, crate::now_ms()).map_err(err)?;
        if let Some(blocks) = self.working_blocks()? {
            // The answer gates the report's effects and healing.
            self.check_probe_inputs(&blocks, owner, !preserve)?;
            let authorizations = blocks.authorizations_of(owner);
            if authorizations.is_empty() {
                return Ok(true);
            }
            if preserve {
                return Ok(false);
            }
            let clocks = blocks.live_clocks();
            let current = authorizations.iter().any(|authorization| {
                clocks
                    .get(&(
                        authorization.kind.as_str(),
                        authorization.origin_key.as_str(),
                    ))
                    .is_some_and(|clock| {
                        clock.evidence == authorization.evidence
                            && clock.evidence_at == authorization.evidence_at
                            && clock.claim_generation == authorization.claim_generation
                    })
            });
            if !current {
                return Ok(false);
            }
            return self.decision_names_credential(owner, credential);
        }
        let authorized: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM probe_authorization WHERE owner=?1)",
                [owner],
                |row| row.get(0),
            )
            .map_err(err)?;
        if !authorized {
            return Ok(true);
        }
        if preserve {
            return Ok(false);
        }
        let current: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM probe_authorization a JOIN block r
             ON r.is_origin=1 AND r.managed=1 AND a.kind=r.kind AND a.origin_key=r.origin_key AND a.evidence=r.evidence
             AND a.evidence_at=r.evidence_at AND a.claim_generation=r.claim_generation
             WHERE a.owner=?1 AND r.cleared_at IS NULL)",
                [owner],
                |row| row.get(0),
            )
            .map_err(err)?;
        if !current {
            return Ok(false);
        }
        self.decision_names_credential(owner, credential)
    }

    /// Whether the decision's remembered serving identity names `credential`;
    /// true when no credential is reported.
    fn decision_names_credential(
        &self,
        decision: &str,
        credential: Option<&str>,
    ) -> RouteStateResult<bool> {
        let Some(credential) = credential else {
            return Ok(true);
        };
        self.conn
            .query_row(
                "SELECT COALESCE(json_extract(provider_identity_json,'$.credentialId')=?2,0)
            FROM model_route_decisions WHERE id=?1",
                params![decision, credential],
                |row| row.get(0),
            )
            .map_err(err)
    }

    /// Claim, renew or consume `origins`' clocks for the named owners after
    /// synchronizing them, deciding on the working state when the command
    /// has one.
    fn claim_owned(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        consume_owner: Option<&str>,
        grant_owner: Option<&str>,
        authorization: Option<&str>,
    ) -> RouteStateResult<bool> {
        sync_on(self, crate::now_ms()).map_err(err)?;
        let blocks = self.working_blocks()?;
        claim_decided(
            self.conn,
            blocks.as_deref(),
            origins,
            now,
            consume_owner,
            grant_owner,
            authorization,
        )
        .map_err(err)
    }

    /// Claim `origins` for a pool key lease, which holds their authorization.
    pub(crate) fn claim_for_lease(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        lease: &str,
    ) -> RouteStateResult<bool> {
        self.claim_owned(origins, now, None, None, Some(lease))
    }

    /// Hand every clock claimed by `from` to `to`. With a working state each
    /// clock is updated by key and must still hold the claim the state shows.
    fn rebind_claim_owner(&self, from: &str, to: &str) -> RouteStateResult<()> {
        let Some(blocks) = self.working_blocks()? else {
            self.conn
                .execute(
                    "UPDATE block SET claim_owner=?2 WHERE claim_owner=?1 AND is_origin=1 AND managed=1",
                    params![from, to],
                )
                .map_err(err)?;
            return Ok(());
        };
        for (clock, _) in blocks.clocks_claimed_by(from) {
            let changed = self
                .conn
                .execute(
                    "UPDATE block SET claim_owner=?3 WHERE kind=?1 AND origin_key=?2
                AND claim_owner=?4 AND evidence=?5 AND evidence_at=?6 AND claim_generation=?7",
                    params![
                        clock.kind,
                        clock.origin_key,
                        to,
                        from,
                        clock.evidence,
                        clock.evidence_at,
                        clock.claim_generation
                    ],
                )
                .map_err(err)?;
            crate::block_state::expect_changes(
                changed,
                1,
                format_args!("rebind of {} {} from {from}", clock.kind, clock.origin_key),
            )
            .map_err(err)?;
        }
        // No clock the state does not show may still be claimed by `from`.
        let left = self
            .conn
            .execute(
                "UPDATE block SET claim_owner=?2 WHERE claim_owner=?1 AND is_origin=1 AND managed=1",
                params![from, to],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            left,
            0,
            format_args!("clocks claimed by {from} the state does not hold"),
        )
        .map_err(err)
    }

    /// Fail with [`crate::block_state::diverged`] if SQL holds an uncleared
    /// clock on `row`'s authorized evidence and generation, which the working
    /// state decided it does not. With `model`, only a non-quota clock of
    /// that model or with no model counts, as `release_inconclusive_probe`
    /// requires. The update sets nothing, so it is a keyed check, not a
    /// change.
    fn expect_no_authorized_clock(
        &self,
        row: &crate::block_state::ProbeAuthorization,
        model: Option<&str>,
    ) -> RouteStateResult<()> {
        let changed = match model {
            None => self.conn.execute(
                &no_authorized_clock_check_sql(false),
                params![
                    row.kind,
                    row.origin_key,
                    row.evidence,
                    row.evidence_at,
                    row.claim_generation
                ],
            ),
            Some(model) => self.conn.execute(
                &no_authorized_clock_check_sql(true),
                params![
                    row.kind,
                    row.origin_key,
                    row.evidence,
                    row.evidence_at,
                    row.claim_generation,
                    model
                ],
            ),
        }
        .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            0,
            format_args!(
                "authorized clock of {} {} for {}",
                row.kind, row.origin_key, row.owner
            ),
        )
        .map_err(err)
    }

    /// Compare what a decision read about `owner`'s probe from the working
    /// state with SQL, keyed by the owner and the origins it holds: its
    /// authorization rows and, with `clocks`, the clock of each. A decision
    /// that only reads (whether a report settles a probe, whether a probe
    /// names a credential) writes nothing a keyed update could check, so it
    /// checks its inputs instead.
    fn check_probe_inputs(
        &self,
        blocks: &crate::BlockState,
        owner: &str,
        clocks: bool,
    ) -> RouteStateResult<()> {
        use crate::block_state::{diverged, ProbeAuthorization};
        let held = blocks.authorizations_of(owner);
        let stored = self
            .conn
            .prepare_cached(&authorization_check_sql())
            .map_err(err)?
            .query_map([owner], |r| {
                Ok(ProbeAuthorization {
                    owner: r.get(0)?,
                    kind: r.get(1)?,
                    origin_key: r.get(2)?,
                    evidence: r.get(3)?,
                    evidence_at: r.get(4)?,
                    claimed_at: r.get(5)?,
                    claim_generation: r.get(6)?,
                })
            })
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(err)?;
        if stored != held {
            return Err(err(diverged(format_args!(
                "authorizations of {owner}: state {held:?}, SQL {stored:?}"
            ))));
        }
        if !clocks {
            return Ok(());
        }
        for row in &held {
            let shown = blocks
                .clock(&row.kind, &row.origin_key)
                .map(|(clock, cleared)| {
                    (
                        clock.evidence.clone(),
                        clock.evidence_at,
                        clock.claim_generation,
                        clock.scope.credential_id.clone(),
                        clock.scope.model.clone(),
                        cleared,
                        clock.claim_owner.clone(),
                    )
                });
            let stored = self
                .conn
                .prepare_cached(&probe_clock_check_sql())
                .map_err(err)?
                .query_row([&row.kind, &row.origin_key], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                })
                .optional()
                .map_err(err)?;
            if stored != shown {
                return Err(err(diverged(format_args!(
                    "clock of {} {}: state {shown:?}, SQL {stored:?}",
                    row.kind, row.origin_key
                ))));
            }
        }
        Ok(())
    }

    /// Retire every probe authorization `owner` still holds, for an owner
    /// whose run ended: keyed by the working state's rows when the command
    /// has one.
    pub fn retire_probe_authorizations(&self, owner: &str) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        match self.working_blocks()? {
            Some(blocks) => {
                self.delete_authorizations(owner, Some(&blocks.authorizations_of(owner)))
            }
            None => self.delete_authorizations(owner, None),
        }
    }

    /// Delete one probe authorization the working state holds, by key and
    /// naming every value the state shows.
    fn delete_authorization(
        &self,
        row: &crate::block_state::ProbeAuthorization,
    ) -> RouteStateResult<()> {
        let changed = self
            .conn
            .execute(
                "DELETE FROM probe_authorization WHERE owner=?1 AND kind=?2 AND origin_key=?3
            AND evidence=?4 AND evidence_at=?5 AND claimed_at=?6 AND claim_generation=?7",
                params![
                    row.owner,
                    row.kind,
                    row.origin_key,
                    row.evidence,
                    row.evidence_at,
                    row.claimed_at,
                    row.claim_generation
                ],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!(
                "authorization of {} for {} {}",
                row.owner, row.kind, row.origin_key
            ),
        )
        .map_err(err)
    }

    /// Delete every probe authorization `owner` holds. With a working state
    /// each row is deleted by key and must hold the values the state shows,
    /// and none may be left over.
    fn delete_authorizations(
        &self,
        owner: &str,
        held: Option<&[crate::block_state::ProbeAuthorization]>,
    ) -> RouteStateResult<()> {
        if let Some(held) = held {
            for row in held {
                self.delete_authorization(row)?;
            }
        }
        let left = self
            .conn
            .execute("DELETE FROM probe_authorization WHERE owner=?1", [owner])
            .map_err(err)?;
        if held.is_some() {
            crate::block_state::expect_changes(
                left,
                0,
                format_args!("authorizations of {owner} the state does not hold"),
            )
            .map_err(err)?;
        }
        Ok(())
    }

    /// Failure consumes the exact authorized generation from the time it was
    /// reported, not the earlier selection time. Retire admission authorizations
    /// too, so a later unstarted-admission expiry cannot reopen a reported send.
    /// Inconclusive local/request failures still release non-quota probes;
    /// quota probes retain their periodic limit regardless of failure attribution.
    pub fn fail_authorized_probe(
        &self,
        owner: &str,
        now: i64,
        inconclusive: bool,
    ) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        sync_on(self, crate::now_ms()).map_err(err)?;
        if let Some(blocks) = self.working_blocks()? {
            let held = blocks.authorizations_of(owner);
            // Consume each clock still on the authorized evidence and
            // generation, keyed and naming the state it was decided on.
            for row in &held {
                let held_clock =
                    blocks
                        .clock(&row.kind, &row.origin_key)
                        .filter(|(clock, cleared)| {
                            cleared.is_none()
                                && clock.evidence == row.evidence
                                && clock.evidence_at == row.evidence_at
                                && clock.claim_generation == row.claim_generation
                        });
                let Some((clock, _)) = held_clock else {
                    self.expect_no_authorized_clock(row, None)?;
                    continue;
                };
                let changed = self.conn.execute(&format!("UPDATE block SET next_recheck_at=?3+CASE WHEN quota=1 THEN ?4 WHEN ?6 THEN 0 ELSE ?5 END,
                    claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?3 THEN reset_check_at END
                    WHERE kind=?1 AND origin_key=?2 AND {}", clock_held(7)),
                    params![row.kind,row.origin_key,now,QUOTA_FENCE_RECHECK_MS,NON_QUOTA_FENCE_MAX_MS,inconclusive,
                        clock.evidence,clock.evidence_at,clock.lifetime == LifetimeClass::Quota,clock.next_recheck_at,
                        clock.reset_check_at,clock.claim_owner,clock.claim_generation]).map_err(err)?;
                crate::block_state::expect_changes(
                    changed,
                    1,
                    format_args!(
                        "failed probe of {} {} for {owner}",
                        row.kind, row.origin_key
                    ),
                )
                .map_err(err)?;
            }
            // Other owners' copies of the same generations are retired too.
            let copies: Vec<_> = blocks
                .authorizations()
                .filter(|other| {
                    other.owner != owner
                        && held.iter().any(|row| {
                            row.kind == other.kind
                                && row.origin_key == other.origin_key
                                && row.evidence == other.evidence
                                && row.evidence_at == other.evidence_at
                                && row.claim_generation == other.claim_generation
                        })
                })
                .cloned()
                .collect();
            for copy in &copies {
                self.delete_authorization(copy)?;
            }
            return self.delete_authorizations(owner, Some(&held));
        }
        self.conn.execute("UPDATE block AS r SET next_recheck_at=?2+CASE WHEN quota=1 THEN ?3 WHEN ?5 THEN 0 ELSE ?4 END,
            claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?2 THEN reset_check_at END
            WHERE r.is_origin=1 AND r.managed=1 AND r.cleared_at IS NULL AND EXISTS (SELECT 1 FROM probe_authorization a
                WHERE a.owner=?1 AND a.kind=r.kind AND a.origin_key=r.origin_key AND a.evidence=r.evidence
                AND a.evidence_at=r.evidence_at AND a.claim_generation=r.claim_generation)",
            params![owner,now,QUOTA_FENCE_RECHECK_MS,NON_QUOTA_FENCE_MAX_MS,inconclusive]).map_err(err)?;
        self.conn.execute("DELETE FROM probe_authorization AS a WHERE owner!=?1 AND EXISTS (
            SELECT 1 FROM probe_authorization p WHERE p.owner=?1 AND p.kind=a.kind AND p.origin_key=a.origin_key
            AND p.evidence=a.evidence AND p.evidence_at=a.evidence_at AND p.claim_generation=a.claim_generation)",
            [owner]).map_err(err)?;
        self.conn
            .execute("DELETE FROM probe_authorization WHERE owner=?1", [owner])
            .map_err(err)?;
        Ok(())
    }

    /// Every owner holding a probe authorization, without joining admissions:
    /// a caller that holds the admission rows in memory decides which owners
    /// are expired unstarted admissions, then passes them to
    /// [`Self::release_admission_probes`].
    pub fn probe_authorization_owners(&self) -> RouteStateResult<Vec<String>> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        self.conn
            .prepare("SELECT DISTINCT owner FROM probe_authorization ORDER BY owner")
            .map_err(err)?
            .query_map([], |row| row.get(0))
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<String>>>()
            .map_err(err)
    }

    /// Preview cleanup without acquiring a writer. The write path checks again
    /// because a runner may start the admission after this snapshot is read.
    pub fn has_expired_admission_probes(&self, now: i64) -> RouteStateResult<bool> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(false);
        }
        self.conn
            .query_row(
                &format!("SELECT EXISTS({EXPIRED_ADMISSION_PROBES})"),
                [now],
                |row| row.get(0),
            )
            .map_err(err)
    }

    /// An unstarted admission sent nothing, so it has not used its probe. The
    /// quota interval limits real sends after a provider refused a request, not
    /// reservations; releasing an unused probe restores that budget without
    /// adding a send.
    /// Returns the admissions it moved from `reserved` to `expired`.
    pub fn release_expired_admission_probes(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<(String, String)>> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let expired = self
            .conn
            .prepare(EXPIRED_ADMISSION_PROBES)
            .map_err(err)?
            .query_map([now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(err)?;
        self.release_admission_probes(expired, now)
            .map(|released| released.expired)
    }

    /// Release the probes of unstarted admissions already found expired, each
    /// given as (principal, admission id, authorization owner).
    pub fn release_admission_probes(
        &self,
        expired: Vec<(String, String, String)>,
        now: i64,
    ) -> RouteStateResult<ReleasedAdmissionProbes> {
        let mut released = ReleasedAdmissionProbes::default();
        if expired.is_empty() {
            return Ok(released);
        }
        sync_on(self, crate::now_ms()).map_err(err)?;
        for (principal, admission, owner) in expired {
            // Only an admission that is still unstarted, in `reserved` or
            // `expired`, gives up its probe; one that started or closed keeps
            // its authorization. (`expired` is included because a replay
            // expires an admission without releasing its probe.) The check
            // comes first, so nothing changes for an admission that fails it.
            let deadline: Option<i64> = self
                .conn
                .query_row(
                    &unstarted_admission_check_sql(),
                    params![principal, admission],
                    |row| row.get(0),
                )
                .optional()
                .map_err(err)?;
            let Some(deadline) = deadline else {
                continue;
            };
            // The probe reopens as of the admission's own deadline, not when
            // a command noticed it lapsed, so every releaser (a reader's view
            // of the state, any command, the Tick) records the same time. The
            // deadline has passed, so the reopened probe is due at once.
            let now = deadline.min(now);
            released.released += 1;
            // Reopen only the authorized evidence and claim generation. The
            // fence stays in place, and an older expiry cannot undo a new probe.
            match self.working_blocks()? {
                Some(blocks) => {
                    // The same decision the account state's `as_of` view
                    // applies in memory (`BlockState::apply_probe_release`).
                    let release = blocks.probe_release(&owner);
                    let held: Vec<_> = release.iter().map(|(row, _)| row.clone()).collect();
                    for (row, current) in &release {
                        let current = *current;
                        // Run for a clock the state calls superseded too: SQL
                        // must then change nothing, or it disagrees.
                        let changed = self.conn.execute("UPDATE block SET next_recheck_at=?3,last_claimed_at=NULL,claim_owner=NULL,
                            claim_generation=claim_generation+1 WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1 AND cleared_at IS NULL
                            AND evidence=?4 AND evidence_at=?5 AND claim_generation=?6",
                            params![row.kind,row.origin_key,now,row.evidence,row.evidence_at,row.claim_generation]).map_err(err)?;
                        crate::block_state::expect_changes(
                            changed,
                            usize::from(current),
                            format_args!("release of {} {} for {owner}", row.kind, row.origin_key),
                        )
                        .map_err(err)?;
                    }
                    self.delete_authorizations(&owner, Some(&held))?;
                }
                None => {
                    self.conn.execute("UPDATE block AS r SET next_recheck_at=?2,last_claimed_at=NULL,claim_owner=NULL,
                         claim_generation=claim_generation+1
                        WHERE r.is_origin=1 AND r.managed=1 AND r.cleared_at IS NULL AND EXISTS (
                            SELECT 1 FROM probe_authorization p WHERE p.owner=?1
                              AND p.kind=r.kind AND p.origin_key=r.origin_key AND p.evidence=r.evidence
                              AND p.evidence_at=r.evidence_at AND p.claim_generation=r.claim_generation)",
                        params![owner,now]).map_err(err)?;
                    self.delete_authorizations(&owner, None)?;
                }
            }
            if self.mark_admission_expired(&principal, &admission)? {
                released.expired.push((principal, admission));
            }
        }
        Ok(released)
    }

    /// Anonymous decisions may report a credential discovered only at launch.
    /// Accept it only when the owner's recorded probe authorization names the
    /// credential for the active fence evidence.
    pub fn probe_authorization_names_credential(
        &self,
        owner: &str,
        credential: &str,
        model: &str,
    ) -> RouteStateResult<bool> {
        sync_on(self, crate::now_ms()).map_err(err)?;
        if let Some(blocks) = self.working_blocks()? {
            // The answer decides whether the report may heal the probe.
            self.check_probe_inputs(&blocks, owner, true)?;
            let clocks = blocks.live_clocks();
            return Ok(blocks.authorizations_of(owner).iter().any(|row| {
                clocks
                    .get(&(row.kind.as_str(), row.origin_key.as_str()))
                    .is_some_and(|clock| {
                        clock.evidence == row.evidence
                            && clock.evidence_at == row.evidence_at
                            && clock.scope.credential_id.as_deref() == Some(credential)
                            && clock
                                .scope
                                .model
                                .as_deref()
                                .is_none_or(|named| named == model)
                    })
            }));
        }
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM probe_authorization a
            JOIN block r ON r.is_origin=1 AND r.managed=1 AND a.kind=r.kind AND a.origin_key=r.origin_key
                AND a.evidence=r.evidence AND a.evidence_at=r.evidence_at
            WHERE a.owner=?1 AND r.credential_id=?2 AND (r.model IS NULL OR r.model=?3)
                AND r.cleared_at IS NULL)",
                params![owner, credential, model],
                |row| row.get(0),
            )
            .map_err(err)
    }

    pub fn transfer_probe_authorization(&self, from: &str, to: &str) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        if let Some(blocks) = self.working_blocks()? {
            // Moving an owner's rows to itself changes nothing.
            if from == to {
                return Ok(());
            }
            let held = blocks.authorizations_of(from);
            for row in &held {
                self.conn.execute("INSERT INTO probe_authorization(owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation)
                    VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(owner,kind,origin_key) DO UPDATE SET evidence=excluded.evidence,
                    evidence_at=excluded.evidence_at,claimed_at=excluded.claimed_at,claim_generation=excluded.claim_generation",
                    params![to,row.kind,row.origin_key,row.evidence,row.evidence_at,row.claimed_at,row.claim_generation]).map_err(err)?;
            }
            return self.delete_authorizations(from, Some(&held));
        }
        self.conn.execute("INSERT INTO probe_authorization SELECT ?2,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation
            FROM probe_authorization WHERE owner=?1
            ON CONFLICT(owner,kind,origin_key) DO UPDATE SET evidence=excluded.evidence,
            evidence_at=excluded.evidence_at,claimed_at=excluded.claimed_at,
            claim_generation=excluded.claim_generation",params![from,to]).map_err(err)?;
        self.conn
            .execute(
                "DELETE FROM probe_authorization WHERE owner=?1 AND owner!=?2",
                params![from, to],
            )
            .map_err(err)?;
        Ok(())
    }

    /// A neutral terminal outcome is not evidence of recovery or refusal. Let
    /// another request test the same non-quota generation, without undoing a
    /// newer launch's claim or shortening the quota probe interval.
    pub fn release_inconclusive_probe(
        &self,
        owner: &str,
        _provider: &str,
        model: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        sync_on(self, crate::now_ms()).map_err(err)?;
        // An alias/target launch may own origins from multiple providers. Only
        // its exact authorized generations may reopen, regardless of provider.
        if let Some(blocks) = self.working_blocks()? {
            let held = blocks.authorizations_of(owner);
            for row in &held {
                let held_clock =
                    blocks
                        .clock(&row.kind, &row.origin_key)
                        .filter(|(clock, cleared)| {
                            cleared.is_none()
                                && clock.lifetime != LifetimeClass::Quota
                                && clock
                                    .scope
                                    .model
                                    .as_deref()
                                    .is_none_or(|named| named == model)
                                && clock.evidence == row.evidence
                                && clock.evidence_at == row.evidence_at
                                && clock.claim_generation == row.claim_generation
                        });
                let Some((clock, _)) = held_clock else {
                    self.expect_no_authorized_clock(row, Some(model))?;
                    continue;
                };
                let changed = self
                    .conn
                    .execute(
                        &format!(
                            "UPDATE block SET next_recheck_at=?3,claim_owner=NULL
                    WHERE kind=?1 AND origin_key=?2 AND {}",
                            clock_held(4)
                        ),
                        params![
                            row.kind,
                            row.origin_key,
                            now,
                            clock.evidence,
                            clock.evidence_at,
                            false,
                            clock.next_recheck_at,
                            clock.reset_check_at,
                            clock.claim_owner,
                            clock.claim_generation
                        ],
                    )
                    .map_err(err)?;
                crate::block_state::expect_changes(
                    changed,
                    1,
                    format_args!(
                        "inconclusive release of {} {} for {owner}",
                        row.kind, row.origin_key
                    ),
                )
                .map_err(err)?;
            }
            return self.delete_authorizations(owner, Some(&held));
        }
        self.conn
            .execute(
                "UPDATE block AS r SET next_recheck_at=?3,claim_owner=NULL
            WHERE r.is_origin=1 AND r.managed=1 AND r.quota=0 AND r.cleared_at IS NULL AND (r.model IS NULL OR r.model=?2)
            AND EXISTS (SELECT 1 FROM probe_authorization a WHERE a.owner=?1
                AND a.kind=r.kind AND a.origin_key=r.origin_key AND a.evidence=r.evidence
                 AND a.evidence_at=r.evidence_at AND a.claim_generation=r.claim_generation)",
                params![owner, model, now],
            )
            .map_err(err)?;
        self.conn
            .execute("DELETE FROM probe_authorization WHERE owner=?1", [owner])
            .map_err(err)?;
        Ok(())
    }

    /// The durable launch authorization, not a reported credential or guessed
    /// pool, is the scope of a successful probe. New evidence never matches it.
    pub fn heal_authorized_probe(
        &self,
        owner: &str,
        _provider: &str,
        model: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        let origins = self.fence_rechecks()?;
        // Which test this owner holds (if any) decides both the heal and the
        // authorization delete below, and a missed heal is never retried:
        // check the owner's authorizations and their clocks against SQL
        // first. On a mismatch this returns STATE_DIVERGED before healing or
        // deleting anything, and the account writer reloads its state and
        // runs the batch again.
        if let Some(blocks) = self.working_blocks()? {
            self.check_probe_inputs(&blocks, owner, true)?;
        }
        // A pool lease can probe both an alias key's billing refusal and the
        // target provider's cooldown. Each proof keeps its own provider scope.
        for origin in origins
            .iter()
            .filter(|origin| origin.model.as_deref().is_none_or(|value| value == model))
        {
            // Only the test request this owner holds now: its authorization
            // must name the clock's current evidence and claim generation, so
            // a test a later one superseded heals nothing.
            let claimed: Option<i64> = match self.working_blocks()? {
                Some(blocks) => blocks
                    .authorizations_of(owner)
                    .into_iter()
                    .find(|row| {
                        row.kind == origin.kind
                            && row.origin_key == origin.origin_key
                            && row.evidence == origin.evidence
                            && row.evidence_at == origin.evidence_at
                            && test_is_current(&blocks, row, false)
                    })
                    .map(|row| row.claimed_at),
                None => self
                    .conn
                    .query_row(
                        &format!("SELECT a.claimed_at FROM probe_authorization a {}
                    WHERE a.owner=?1 AND a.kind=?2 AND a.origin_key=?3 AND a.evidence=?4 AND a.evidence_at=?5", current_test_join(6)),
                        params![
                            owner,
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at,
                            false
                        ],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(err)?,
            };
            if let Some(at) = claimed.filter(|at| origin.evidence_at < *at) {
                self.heal_origins(std::slice::from_ref(origin), model, at, now, None, true)?;
            }
        }
        match self.working_blocks()? {
            Some(blocks) => {
                self.delete_authorizations(owner, Some(&blocks.authorizations_of(owner)))
            }
            None => self.delete_authorizations(owner, None),
        }
    }

    /// Settles one run's test request after a successful report. The owner's
    /// authorization must name the block's exact evidence (the refusal that
    /// started it) and its current claim generation (which numbered test this
    /// was), with no other request owning the claim; an admission that owns
    /// the claim itself is current as well. An admission owner may also
    /// retire the same test's duplicate authorization held under the
    /// decision id (`decision_copy`), but never a later run's re-claim.
    pub(crate) fn heal_report_probe(
        &self,
        owner: &str,
        decision_copy: Option<&str>,
        admission_owned: bool,
        model: &str,
        admitted_at: i64,
        origins: &[FenceRecheck],
        scope: Option<&QuotaSuccessScope>,
        now: i64,
    ) -> RouteStateResult<()> {
        // The report is recorded as delivered whatever this finds, so a heal
        // skipped on a stale state would be lost: check the owner's (and the
        // decision copy's) authorizations and their clocks against SQL first.
        if let Some(blocks) = self.working_blocks()? {
            self.check_probe_inputs(&blocks, owner, true)?;
            if let Some(copy) = decision_copy.filter(|copy| *copy != owner) {
                self.check_probe_inputs(&blocks, copy, true)?;
            }
        }
        for origin in origins.iter().filter(|origin| {
            origin.evidence_at < admitted_at
                && origin.model.as_deref().is_none_or(|value| value == model)
        }) {
            let proof: Option<(i64, i64)> = match self.working_blocks()? {
                Some(blocks) => blocks
                    .authorizations_of(owner)
                    .into_iter()
                    .find(|row| {
                        row.kind == origin.kind
                            && row.origin_key == origin.origin_key
                            && row.evidence == origin.evidence
                            && row.evidence_at == origin.evidence_at
                            && row.claimed_at <= admitted_at
                    })
                    .filter(|row| test_is_current(&blocks, row, admission_owned))
                    .map(|row| (row.claimed_at, row.claim_generation)),
                None => self
                    .conn
                    .query_row(
                        &format!(
                            "SELECT a.claimed_at,a.claim_generation FROM probe_authorization a {}
                     WHERE a.owner=?1 AND a.kind=?2 AND a.origin_key=?3
                     AND a.evidence=?4 AND a.evidence_at=?5 AND a.claimed_at<=?6",
                            current_test_join(7)
                        ),
                        params![
                            owner,
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at,
                            admitted_at,
                            admission_owned
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(err)?,
            };
            let Some((claimed_at, generation)) = proof.filter(|(at, _)| origin.evidence_at < *at)
            else {
                continue;
            };
            self.heal_origins(
                std::slice::from_ref(origin),
                model,
                admitted_at,
                now,
                scope,
                true,
            )?;
            // The owner's row, and the decision's copy of the same test when
            // the state holds one.
            let expected = match self.working_blocks()? {
                Some(blocks) => Some(
                    1 + usize::from(decision_copy.is_some_and(|copy| {
                        copy != owner
                            && blocks.authorizations_of(copy).iter().any(|row| {
                                row.kind == origin.kind
                                    && row.origin_key == origin.origin_key
                                    && row.evidence == origin.evidence
                                    && row.evidence_at == origin.evidence_at
                                    && row.claimed_at == claimed_at
                                    && row.claim_generation == generation
                            })
                    })),
                ),
                None => None,
            };
            let changed = self
                .conn
                .execute(
                    "DELETE FROM probe_authorization WHERE (owner=?1 OR owner=?2)
                 AND kind=?3 AND origin_key=?4 AND evidence=?5 AND evidence_at=?6
                 AND claimed_at=?7 AND claim_generation=?8",
                    params![
                        owner,
                        decision_copy,
                        origin.kind,
                        origin.origin_key,
                        origin.evidence,
                        origin.evidence_at,
                        claimed_at,
                        generation
                    ],
                )
                .map_err(err)?;
            if let Some(expected) = expected {
                crate::block_state::expect_changes(
                    changed,
                    expected,
                    format_args!(
                        "settled test of {} {} for {owner}",
                        origin.kind, origin.origin_key
                    ),
                )
                .map_err(err)?;
            }
        }
        Ok(())
    }
    pub(crate) fn successful_quota_pools(
        &self,
        id: &str,
        observed: i64,
    ) -> RouteStateResult<Vec<striatum_core::accounts::PoolKey>> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare("SELECT pool_key_json FROM success_proof WHERE fence_id=?1 AND evidence_at=?2")
            .map_err(err)?;
        let rows = statement
            .query_map(params![id, observed], |row| row.get::<_, String>(0))
            .map_err(err)?;
        rows.map(|raw| serde_json::from_str(&raw.map_err(err)?).map_err(err))
            .collect()
    }
    pub fn claim_fence_rechecks_for_admission(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
    ) -> RouteStateResult<bool> {
        self.claim_owned(origins, now, owner, None, owner)
    }
    pub fn claim_fence_rechecks_for_authorized_admission(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
        admission: &str,
    ) -> RouteStateResult<bool> {
        self.claim_owned(origins, now, owner, None, Some(admission))
    }
    pub fn grant_fence_rechecks(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
    ) -> RouteStateResult<bool> {
        self.claim_owned(origins, now, owner, owner, owner)
    }
    /// Success can heal only evidence older than this request's admission. A
    /// late completion of an older run must not erase a more recent refusal.
    #[allow(clippy::too_many_arguments)]
    pub fn heal_serving_fences(
        &self,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
        size: Option<u32>,
        admitted_at: i64,
        now: i64,
    ) -> RouteStateResult<()> {
        self.heal_serving_fences_in_scope(
            provider,
            account,
            credential,
            model,
            size,
            admitted_at,
            now,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heal_serving_fences_in_scope(
        &self,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
        size: Option<u32>,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
    ) -> RouteStateResult<()> {
        let origins = self.fence_rechecks()?;
        let origins: Vec<_> = origins
            .into_iter()
            .filter(|origin| {
                origin.applies(provider, account, credential, model)
                    && origin.evidence_at < admitted_at
                    && origin.principal.is_none()
                    && origin
                        .size_class
                        .is_none_or(|class| Some(class as u32) == size)
            })
            .collect();
        self.heal_origins(&origins, model, admitted_at, now, scope, false)
    }

    /// Heal `origins` after a success: end each one's source evidence where
    /// it is older than `admitted_at` and clear its clock. With a working
    /// state every decision reads the state and every write is keyed and
    /// names the row it was decided on; without one, SQL decides.
    pub(crate) fn heal_origins(
        &self,
        origins: &[FenceRecheck],
        model: &str,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
        authorized: bool,
    ) -> RouteStateResult<()> {
        for origin in origins {
            self.heal_origin(origin, model, admitted_at, now, scope, authorized)?;
        }
        Ok(())
    }

    /// The current command's captured rows, or a SQL read for a cold store
    /// without a publisher. Both paths use the same keyed healing checks.
    fn working(&self) -> RouteStateResult<std::borrow::Cow<'_, crate::BlockState>> {
        match self.working_blocks()? {
            Some(state) => Ok(state),
            None => crate::BlockState::read(self.conn)
                .map(std::borrow::Cow::Owned)
                .map_err(err),
        }
    }

    /// [`Self::heal_origins`] for one origin, deciding on the working state.
    /// Each keyed write names the current values of its row, so it fails
    /// with [`crate::block_state::diverged`] if SQL holds something else; a
    /// field the state shows already cleared is not written again.
    fn heal_origin(
        &self,
        origin: &FenceRecheck,
        model: &str,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
        authorized: bool,
    ) -> RouteStateResult<()> {
        use crate::block_state::expect_changes;
        let provider = origin.provider.as_str();
        let credential = origin.credential_id.as_deref().unwrap_or("");
        let key: Vec<serde_json::Value> = serde_json::from_str(&origin.origin_key).map_err(err)?;
        let what = format!("heal of {} {}", origin.kind, origin.origin_key);
        let blocks = self.working()?;
        match origin.kind.as_str() {
            "account" => {
                // Only active fences are held: a fence already cleared is
                // not cleared again.
                let fence = key[0].as_str().and_then(|id| blocks.account_fence(id));
                let older = |row: &crate::AccountFenceRow| {
                    row.observed_at.min(row.created_at) < admitted_at
                };
                let Some((row, pool, recovery)) = fence else {
                    return if authorized {
                        self.clear_held_clock(origin, now)
                    } else {
                        Ok(())
                    };
                };
                let needed: Vec<striatum_core::accounts::PoolKey> = match pool {
                    Some(pool) => vec![serde_json::from_str(pool).map_err(err)?],
                    None => serde_json::from_str(recovery).map_err(err)?,
                };
                // Authorization permits the test, not recovery of pools the
                // request did not use. An empty whole-account recovery set
                // and pool-keyed fences retain their authorized clear.
                if authorized && (pool.is_some() || needed.is_empty()) {
                    if older(&row) {
                        self.clear_held_fence(&row, now, "probe_succeeded")?;
                    }
                    return self.clear_held_clock(origin, now);
                }
                if scope.is_none() && !authorized {
                    return Ok(());
                }
                if needed.is_empty() {
                    return Ok(());
                }
                let row = row.clone();
                let (pool_json, recovery_json) = (pool.map(str::to_owned), recovery.to_owned());
                drop(blocks);
                for pool in needed
                    .iter()
                    .filter(|pool| scope.is_some_and(|scope| scope.used(pool, model)))
                {
                    self.put_success_pool(
                        &row.fence_id,
                        &serde_json::to_string(pool).map_err(err)?,
                        origin.evidence_at,
                        now,
                    )?;
                }
                let healed = self
                    .working()?
                    .success_pools()
                    .filter(|pool| {
                        pool.fence_id == row.fence_id && pool.evidence_at == origin.evidence_at
                    })
                    .map(|pool| serde_json::from_str(&pool.pool_key_json).map_err(err))
                    .collect::<RouteStateResult<Vec<striatum_core::accounts::PoolKey>>>()?;
                if !needed.iter().all(|pool| healed.contains(pool)) {
                    return if authorized {
                        self.defer_recovery_test(origin, now)
                    } else {
                        Ok(())
                    };
                }
                if older(&row) {
                    // Decided on the fence's pools and on the successes
                    // recorded for them: the clear names both.
                    self.clear_held_pool_fence(
                        &row,
                        pool_json.as_deref(),
                        &recovery_json,
                        (origin.evidence_at, healed.len()),
                        now,
                        if authorized {
                            "probe_succeeded"
                        } else {
                            "run_succeeded"
                        },
                    )?;
                }
            }
            "credential" => {
                if origin.quota {
                    let prefix = format!(
                        "credential-quota:{}:",
                        serde_json::json!([provider, credential])
                    );
                    if blocks
                        .active_fences(provider)
                        .iter()
                        .any(|fence| fence.fence_id.starts_with(&prefix))
                    {
                        return Ok(());
                    }
                }
                if let Some(fence) = blocks
                    .entitlement_fence(provider, credential)
                    .filter(|fence| fence.observed_at_ms < admitted_at)
                {
                    self.clear_held_entitlement_fence(&fence)?;
                }
            }
            "account_route" => {
                if let Some((row, class, last)) = blocks
                    .account_route_fence(provider, credential, model)
                    .filter(|(row, _, last)| last.unwrap_or(row.recorded_at) < admitted_at)
                {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?8,cleared_by='removed',claim_owner=NULL
                        WHERE kind='account_route' AND source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND deadline=?4 AND recorded_at=?5 AND last_reported_at IS ?6 AND wall_class IS ?7",
                        params![provider,credential,model,row.until_ms,row.recorded_at,last,class,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "non_quota_route" | "transient_route" => {
                let kind = if origin.kind == "transient_route" {
                    "transient"
                } else {
                    "billing"
                };
                if let Some((row, last)) = blocks
                    .non_quota_route_fence(provider, credential, model, kind)
                    .filter(|(_, last)| *last < admitted_at)
                {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?8,cleared_by='removed',claim_owner=NULL
                        WHERE source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND kind=CASE ?4 WHEN 'transient' THEN 'transient_route' ELSE 'non_quota_route' END
                        AND deadline=?5 AND recorded_at=?6 AND last_reported_at=?7",
                        params![provider,credential,model,kind,row.until_ms,row.recorded_at,last,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "size" | "non_quota_size" => {
                let quota = origin.kind == "size";
                let kind = if quota { "size" } else { "non_quota_size" };
                if let Some(row) = origin.size_class.and_then(|class| {
                    blocks
                        .size_refusal(quota, provider, credential, model, class)
                        .filter(|row| row.observed_at < admitted_at)
                }) {
                    let changed = self.conn.execute(&format!("UPDATE block SET source_present=0,cleared_at=?7,cleared_by='removed',claim_owner=NULL
                        WHERE kind='{kind}' AND source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND size_class=?4 AND deadline IS ?5 AND observed_at=?6"),
                        params![provider,credential,model,row.size_class,row.expires_at,row.observed_at,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "pool_billing" | "pool_rate" => {
                if let Some(held) = blocks.source_origin(&origin.kind, &origin.origin_key) {
                    let changed = self
                        .conn
                        .execute(
                            "UPDATE block SET source_present=0,cleared_at=?4,cleared_by='removed',claim_owner=NULL
                                WHERE provider=?1 AND credential_id=?2 AND evidence=?3 AND kind=?5 AND source_present=1",
                            params![provider, credential, held.evidence,now,origin.kind],
                        )
                        .map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "route" => {
                if let Some(held) = blocks.source_origin("route", &origin.origin_key) {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                        WHERE kind='route' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
                        params![key[0].as_str(),held.evidence,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "provider" => {
                // Only the rows stamped by this provider evidence inherit
                // its recovery. A separate model refusal has its own clock.
                for (route, evidence, last_error, observed) in
                    blocks.routes_stamped_by_provider(provider, origin.evidence_at, admitted_at)
                {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?5,cleared_by='removed',claim_owner=NULL
                        WHERE kind='route' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2
                        AND scope_error IS ?3 AND observed_at=?4",
                        params![route,evidence,last_error,observed,now]).map_err(err)?;
                    expect_changes(changed, 1, format_args!("{what}: route {route}"))
                        .map_err(err)?;
                }
                if let Some(held) = blocks.source_origin("provider", &origin.origin_key) {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                        WHERE kind='provider' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
                        params![provider,held.evidence,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "failure" => {
                if let Some(held) = blocks.source_origin("failure", &origin.origin_key) {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                        WHERE kind='failure' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
                        params![key[0].as_str(),held.evidence,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            _ => {}
        }
        self.clear_held_clock(origin, now)
    }

    /// Record that `pool` succeeded for one account fence's evidence, keyed
    /// and naming the success the working state holds for it, if any.
    fn put_success_pool(
        &self,
        fence_id: &str,
        pool: &str,
        evidence_at: i64,
        now: i64,
    ) -> RouteStateResult<()> {
        let held = self
            .working()?
            .success_pools()
            .find(|row| row.fence_id == fence_id && row.pool_key_json == pool)
            .cloned();
        let changed = match held {
            Some(held) => self.conn.execute(
                "UPDATE success_proof SET evidence_at=?3,healed_at=?4
                WHERE fence_id=?1 AND pool_key_json=?2 AND evidence_at=?5 AND healed_at=?6",
                params![
                    fence_id,
                    pool,
                    evidence_at,
                    now,
                    held.evidence_at,
                    held.healed_at
                ],
            ),
            None => self.conn.execute(
                "INSERT INTO success_proof VALUES (?1,?2,?3,?4)
                ON CONFLICT(fence_id,pool_key_json) DO NOTHING",
                params![fence_id, pool, evidence_at, now],
            ),
        }
        .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("success of {fence_id} for {pool}"),
        )
        .map_err(err)
    }

    /// Clear an origin's stored clock, naming the clock the working state
    /// holds. An origin with no stored clock has none to clear.
    fn clear_held_clock(&self, origin: &FenceRecheck, now: i64) -> RouteStateResult<()> {
        let blocks = self.working()?;
        let Some((clock, cleared)) = blocks.clock(&origin.kind, &origin.origin_key) else {
            return Ok(());
        };
        let changed = self
            .conn
            .execute(
                 "UPDATE block SET cleared_at=?3,cleared_by=CASE WHEN source_present=1 THEN 'kept' ELSE 'removed' END WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1
            AND evidence=?4 AND evidence_at=?5 AND cleared_at IS ?6 AND next_recheck_at=?7
            AND claim_owner IS ?8 AND claim_generation=?9",
                params![
                    origin.kind,
                    origin.origin_key,
                    now,
                    clock.evidence,
                    clock.evidence_at,
                    cleared,
                    clock.next_recheck_at,
                    clock.claim_owner,
                    clock.claim_generation
                ],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("clock heal of {} {}", origin.kind, origin.origin_key),
        )
        .map_err(err)
    }

    /// A successful test with pools still unproved consumes its quota test
    /// slot without clearing the block. Another request may test it after
    /// the same interval used for an unsuccessful quota test.
    fn defer_recovery_test(&self, origin: &FenceRecheck, now: i64) -> RouteStateResult<()> {
        if let Some(blocks) = self.working_blocks()? {
            let Some((clock, _)) = blocks.clock(&origin.kind, &origin.origin_key) else {
                return Ok(());
            };
            let changed = self
                .conn
                .execute(
                    &format!(
                        "UPDATE block SET next_recheck_at=?3+?4,
                claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?3 THEN reset_check_at END
                WHERE kind=?1 AND origin_key=?2 AND {}",
                        clock_held(5)
                    ),
                    params![
                        origin.kind,
                        origin.origin_key,
                        now,
                        QUOTA_FENCE_RECHECK_MS,
                        clock.evidence,
                        clock.evidence_at,
                        clock.lifetime == LifetimeClass::Quota,
                        clock.next_recheck_at,
                        clock.reset_check_at,
                        clock.claim_owner,
                        clock.claim_generation
                    ],
                )
                .map_err(err)?;
            return crate::block_state::expect_changes(
                changed,
                1,
                format_args!("partial recovery of {} {}", origin.kind, origin.origin_key),
            )
            .map_err(err);
        }
        self.conn
            .execute(
                "UPDATE block SET next_recheck_at=?3+?4,
            claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?3 THEN reset_check_at END
            WHERE kind=?1 AND origin_key=?2",
                params![origin.kind, origin.origin_key, now, QUOTA_FENCE_RECHECK_MS],
            )
            .map_err(err)?;
        Ok(())
    }

    /// Preview classification without materializing or renewing retry clocks.
    /// Only a classification of the same evidence can be reused; admission or
    /// reconciliation must classify new evidence under the writer lock.
    pub fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> RouteStateResult<(bool, bool)> {
        if !enabled(self.conn).map_err(err)? {
            return Ok((proposed, false));
        }
        let prior = self
            .conn
            .query_row(
                "SELECT quota FROM block WHERE kind='credential' AND is_origin=1 AND provider=?1 AND credential_id=?2
                    AND managed=1 AND source_present=1 AND classified=1",
                params![provider, credential],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        Ok((prior.unwrap_or(proposed), prior.is_none()))
    }

    pub fn classify_credential_quota(
        &self,
        provider: &str,
        credential: &str,
        quota: bool,
    ) -> RouteStateResult<bool> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(quota);
        }
        sync_on(self, crate::now_ms()).map_err(err)?;
        if let Some(blocks) = self.working_blocks()? {
            let clocks: Vec<(Block, Option<i64>)> = blocks
                .stored_clocks()
                .filter(|(clock, _)| {
                    clock.kind == "credential"
                        && clock.scope.provider == provider
                        && clock.scope.credential_id.as_deref() == Some(credential)
                })
                .map(|(clock, cleared)| (clock.clone(), cleared))
                .collect();
            for (clock, cleared) in clocks.iter().filter(|(clock, _)| !clock.classified) {
                let changed = self.conn.execute("UPDATE block SET quota=?3,classified=1,
                    next_recheck_at=MIN(next_recheck_at,evidence_at+CASE WHEN ?3 THEN 900000 ELSE 3600000 END)
                    WHERE kind='credential' AND origin_key=?1 AND is_origin=1 AND managed=1 AND source_present=1 AND classified=0 AND provider=?2 AND credential_id=?4
                    AND evidence=?5 AND evidence_at=?6 AND quota=?7 AND next_recheck_at=?8 AND cleared_at IS ?9",
                    params![clock.origin_key,provider,quota,credential,clock.evidence,clock.evidence_at,
                        clock.lifetime == LifetimeClass::Quota,clock.next_recheck_at,cleared]).map_err(err)?;
                crate::block_state::expect_changes(
                    changed,
                    1,
                    format_args!("classification of credential {}", clock.origin_key),
                )
                .map_err(err)?;
            }
            // The class each clock now holds: a new classification, or the
            // one an earlier decision stored.
            return Ok(clocks.first().is_some_and(|(clock, _)| {
                if clock.classified {
                    clock.lifetime == LifetimeClass::Quota
                } else {
                    quota
                }
            }));
        }
        self.conn.execute("UPDATE block SET quota=?3,classified=1,next_recheck_at=MIN(next_recheck_at,evidence_at+CASE WHEN ?3 THEN 900000 ELSE 3600000 END)
            WHERE kind='credential' AND is_origin=1 AND provider=?1 AND credential_id=?2 AND managed=1 AND source_present=1 AND classified=0",params![provider,credential,quota]).map_err(err)?;
        self.conn.query_row("SELECT quota FROM block WHERE kind='credential' AND is_origin=1 AND provider=?1 AND credential_id=?2 AND managed=1 AND source_present=1",params![provider,credential],|row|row.get(0)).optional().map(|quota|quota.unwrap_or(false)).map_err(err)
    }

    /// The effective rechecks, as `SqliteRouteStateStore::fence_rechecks`
    /// reads them: from the command's working state when it has one, which
    /// evaluates expiry at the same wall clock, else from SQL.
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        match self.working_blocks()? {
            Some(blocks) => blocks.fence_rechecks(crate::now_ms()),
            None => read_effective(self.conn).map_err(err),
        }
    }

    pub fn claim_fence_rechecks(
        &self,
        origins: &[FenceRecheck],
        now: i64,
    ) -> RouteStateResult<bool> {
        self.claim_owned(origins, now, None, None, None)
    }
}

#[cfg(test)]
mod tests {
    /// Tick and every selection preview run this lookup. The authorization
    /// table holds a handful of rows while account_admission keeps months of
    /// history, so the plan must loop over authorizations and look admissions
    /// up by key. A plan that scans account_admission held the account writer
    /// for hundreds of milliseconds per Tick on a live-sized history.
    #[test]
    fn expired_admission_probe_lookup_never_scans_admission_history() {
        let scratch = cortexkit_test_support::ScratchDir::new("expired-probe-plan");
        let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
            "expired-probe-plan",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let preview = format!("SELECT EXISTS({})", super::EXPIRED_ADMISSION_PROBES);
        for sql in [super::EXPIRED_ADMISSION_PROBES, preview.as_str()] {
            let plan = store
                .store
                .with_conn(|conn| {
                    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                        .query_map([0_i64], |row| row.get::<_, String>(3))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap();
            assert!(
                plan.iter()
                    .any(|step| step.starts_with("SEARCH a USING INDEX"))
                    && !plan.iter().any(|step| step.starts_with("SCAN a")),
                "account_admission must be searched by key, not scanned: {plan:?}"
            );
        }
    }

    #[test]
    fn blocks_parity_rows_cover_all_kinds_states_and_lifetimes() {
        let scratch = cortexkit_test_support::ScratchDir::new("block-parity");
        let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
            "block-parity",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = 1_900_000_000_000;
        store.seed_block_parity_fixture_for_test(now);
        store.assert_block_parity_fixture_coverage_for_test(now);
        let report = store.block_parity(now).unwrap();
        assert!(report.differences.is_empty(), "{report:#?}");
        assert!(report.identities > 0 && report.function_comparisons > 0);
        assert_eq!(
            report.combinations_per_identity,
            store.block_parity_times(now).unwrap().len() * 192
        );
    }

    #[test]
    fn blocks_parity_new_evidence_and_orphan_observation() {
        let scratch = cortexkit_test_support::ScratchDir::new("block-parity-overlay");
        let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
            "block-parity-overlay",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = 1_900_000_000_000;
        store.seed_block_parity_fixture_for_test(now);
        store.with_writer_for_test(|conn| conn.execute_batch(&format!(r#"
            UPDATE fence_recheck SET last_claimed_at={now},next_recheck_at={now}+900000 WHERE kind='credential' AND credential_id='oauth:cpu-0';
            UPDATE credential_entitlement_fences SET code='new-refusal' WHERE credential_id='oauth:cpu-0';
            DELETE FROM account_transient_backoff;
            UPDATE pool_key_state SET wall_reason='new-evidence',walled_until={now}-1;
        "#))).unwrap();
        let loaded = store.load_blocks(now).unwrap();
        let credential = loaded
            .blocks
            .iter()
            .find(|b| {
                b.kind == "credential" && b.scope.credential_id.as_deref() == Some("oauth:cpu-0")
            })
            .unwrap();
        assert_eq!(
            credential.lifetime,
            striatum_core::blocks::LifetimeClass::NonQuota
        );
        assert!(!credential.classified);
        assert_eq!(credential.claim_generation, 0);
        assert_eq!(credential.last_claimed_at, Some(now));
        assert_eq!(credential.next_recheck_at, now + 3_599_000);
        assert!(
            loaded.blocks.iter().any(|b| b.kind == "observation"),
            "source-less observation clock remains effective"
        );
        assert!(
            !loaded.blocks.iter().any(|b| b.kind == "pool_billing"),
            "expired unscheduled evidence is not revived"
        );
        let report = store.block_parity(now).unwrap();
        assert!(report.differences.is_empty(), "{report:#?}");
    }

    use super::*;

    #[test]
    fn expired_admission_probe_cannot_reopen_newer_evidence_or_claim_generation() {
        for newer_evidence in [false, true] {
            let scratch = cortexkit_test_support::ScratchDir::new("expired-probe-generation");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "expired-probe-generation",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap();
            let fence = crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 1000,
                recorded_at: 100,
                decision_id: "d".into(),
            };
            store.record_billing_account_route_fence(&fence).unwrap();
            let origins = store.fence_rechecks().unwrap();
            let owner = serde_json::json!(["account_admission", "p", "a"]).to_string();
            store.account_transaction(|tx| {
                tx.insert_pin(&crate::AccountPinRow {
                    pin_id:"pin".into(), principal:"p".into(), conversation_hash:"".into(),
                    provider:"p".into(), pin_generation:1, affinity_model:"m".into(),
                    account_id:"acct".into(), credential_id:"c".into(), pin_mode:"run_frozen".into(),
                    previous_pin_id:None, decision_id:"d".into(), created_at:1000,
                })?;
                tx.conn.execute("INSERT INTO account_admission
                    (principal,admission_id,request_fingerprint,request_mode,decision_id,conversation_hash,
                    provider,model,account_id,credential_id,pin_id,pin_generation,state,charged,
                    request_bytes,request_bytes_estimated,committed_at,admission_valid_until,
                    lease_valid_until,reply_json,started_late)
                    VALUES ('p','a','','admit','d','','p','m','acct','c','pin',1,'reserved',1,
                    1,0,1000,61000,121000,'{}',0)", []).map_err(err)?;
                assert!(tx.claim_fence_rechecks_for_authorized_admission(&origins,1000,None,&owner)?);
                Ok(())
            }).unwrap();
            let now = 3_601_001;
            if newer_evidence {
                store
                    .record_billing_account_route_fence(&crate::AccountRouteFence {
                        recorded_at: now,
                        until_ms: now + 100_000,
                        ..fence
                    })
                    .unwrap();
            } else {
                let origins = store.fence_rechecks().unwrap();
                assert!(store
                    .account_transaction(|tx| tx.grant_fence_rechecks(
                        &origins,
                        now,
                        Some("new-owner"),
                    ))
                    .unwrap());
            }
            let before = store.fence_rechecks().unwrap().remove(0);
            store
                .account_transaction(|tx| {
                    assert!(tx.has_expired_admission_probes(now)?);
                    tx.release_expired_admission_probes(now)?;
                    assert!(!tx.has_expired_admission_probes(now)?);
                    Ok(())
                })
                .unwrap();
            let after = store.fence_rechecks().unwrap().remove(0);
            assert_eq!(after.next_recheck_at, before.next_recheck_at);
            assert_eq!(after.last_claimed_at, before.last_claimed_at);
            assert_eq!(after.claim_owner, before.claim_owner);
            assert_eq!(after.evidence, before.evidence);
            assert_eq!(after.evidence_at, before.evidence_at);
        }
    }

    #[test]
    fn successful_probe_heals_only_its_authorized_empty_and_named_generations() {
        let scratch = cortexkit_test_support::ScratchDir::new("authorized-quota-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "authorized-quota-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let named = striatum_core::accounts::PoolKey {
            name: Some("P".into()),
            scope: striatum_core::accounts::PoolScope::EveryModel,
            window_minutes: Some(300),
        };
        store.store.with_conn(|conn| {
            for (id,pool) in [("empty",None),("named",Some(&named)),("unclaimed",Some(&named))] {
                conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
                    VALUES (?1,'p','a','{}','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',100,'test',100,?2,'[]')",
                    params![id,pool.map(|pool|serde_json::to_string(pool).unwrap())])?;
            }
            Ok(())
        }).unwrap();
        let claimed: Vec<_> = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|origin| !origin.origin_key.contains("unclaimed"))
            .collect();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &claimed,
                900_100,
                Some("probe-decision")
            ))
            .unwrap());
        // Unknown-pool successes unrelated to this authorization remain unable
        // to heal anything, including the unclaimed origin on the same account.
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "m", None, 900_100, 900_101)
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3
        );
        store
            .account_transaction(|tx| tx.heal_authorized_probe("probe-decision", "p", "m", 900_101))
            .unwrap();
        let remaining = store.account_read(|tx| tx.active_fences("p")).unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "the exact claimed empty and named origins must heal"
        );
        assert_eq!(remaining[0].fence_id, "unclaimed");
    }

    #[test]
    fn authorized_sibling_probe_heals_claimed_rows_but_never_a_newer_generation() {
        let scratch = cortexkit_test_support::ScratchDir::new("authorized-sibling-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "authorized-sibling-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for credential in ["c2", "newer", "unclaimed"] {
            store
                .record_billing_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 1000,
                    recorded_at: 100,
                    decision_id: "old".into(),
                })
                .unwrap();
        }
        let claimed: Vec<_> = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|origin| origin.credential_id.as_deref() != Some("unclaimed"))
            .collect();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &claimed,
                1000,
                Some("sibling-probe")
            ))
            .unwrap());
        store
            .record_billing_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "newer".into(),
                model: "m".into(),
                until_ms: 10_000,
                recorded_at: 1001,
                decision_id: "repeat".into(),
            })
            .unwrap();
        store
            .account_transaction(|tx| tx.heal_authorized_probe("sibling-probe", "p", "m", 1002))
            .unwrap();
        let remaining = store.non_quota_account_route_fences(1002).unwrap();
        assert_eq!(
            remaining.len(),
            2,
            "only the claimed older generation heals, regardless of serving sibling"
        );
        assert!(remaining.iter().any(|f| f.credential_id == "newer"));
        assert!(remaining.iter().any(|f| f.credential_id == "unclaimed"));
    }

    #[test]
    fn provider_probe_success_preserves_a_newer_independent_model_refusal() {
        let scratch = cortexkit_test_support::ScratchDir::new("provider-evidence-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "provider-evidence-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn|conn.execute_batch("
            INSERT INTO provider_cooldown_state(provider_key,cooldown_until,last_error,last_error_at,updated_at,wall_observed_at)
                VALUES ('p',86400100,'old',100,100,100);
            INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,cooldown_until,last_error,last_error_at,wall_observed_at)
                VALUES ('p/m','p/m','p','m',100,86400100,'old',100,100);
        ")).unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                3_600_100,
                Some("provider-probe")
            ))
            .unwrap());
        store.store.with_conn(|conn|conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,cooldown_until,last_error,last_error_at,wall_observed_at)
            VALUES ('p/other','p/other','p','other',3600101,3610101,'independent',3600101,3600101)",[])).unwrap();
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "m", None, 3_600_100, 3_600_102)
            })
            .unwrap();
        let until: Option<i64> = store
            .store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT cooldown_until FROM model_route_state WHERE route_key='p/other'",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(
            until,
            Some(3_610_101),
            "old provider recovery cannot erase an independent model's newer generation"
        );
    }

    #[test]
    fn pool_probe_authorization_survives_renewal_and_moves_to_the_serving_decision() {
        use striatum_core::key_pool::PoolKey;
        let scratch = cortexkit_test_support::ScratchDir::new("pool-success-authorization");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "pool-success-authorization",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                1000,
                "credits",
                100,
            )
            .unwrap();
        let keys = vec![PoolKey::new("apikey:p", Some("a"), "active")];
        let crate::PoolAcquireOutcome::Leased(lease) = store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "p",
                    session_id: "session",
                    keys: &keys,
                    now_ms: 1000,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                },
                &["p"],
                "m",
                &[],
                &Default::default(),
            )
            .unwrap()
        else {
            panic!("due pool probe must lease")
        };
        assert!(store
            .pool_renew(
                "p",
                "session",
                &lease.lease_id,
                Some("apikey:p"),
                1001,
                120_000
            )
            .unwrap()
            .is_some());
        assert!(
            !store.fence_rechecks().unwrap().is_empty(),
            "renewal is not success"
        );
        store
            .bind_fence_probe_owner(&lease.lease_id, "decision")
            .unwrap();
        store
            .pool_release("p", "session", Some(&lease.lease_id), true, 1002)
            .unwrap();
        assert!(
            !store.fence_rechecks().unwrap().is_empty(),
            "release is not success either"
        );
        store
            .account_transaction(|tx| tx.heal_authorized_probe("decision", "p", "m", 1003))
            .unwrap();
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "a linked typed success heals the claimed pool wall"
        );
    }

    #[test]
    fn linked_quota_reset_opens_the_whole_conjunction_but_not_after_a_newer_refusal() {
        let scratch = cortexkit_test_support::ScratchDir::new("quota-conjunction");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "quota-conjunction",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: "p".into(),
                    credential_id: "c".into(),
                    code: "billing_exhausted".into(),
                },
                None,
                0,
            )
            .unwrap();
        store
            .account_transaction(|tx| tx.classify_credential_quota("p", "c", true))
            .unwrap();
        let fence = crate::AccountRouteFence {
            provider: "p".into(),
            credential_id: "c".into(),
            model: "m".into(),
            until_ms: 60_000,
            decision_id: "d".into(),
            recorded_at: 0,
        };
        store.record_account_route_fence(&fence).unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 59_999))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 60_000))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 60_000))
            .unwrap());
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| origin.next_recheck_at == 960_000));
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                recorded_at: 70_000,
                until_ms: 60_000,
                ..fence
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(
            !store
                .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 70_000))
                .unwrap(),
            "a reset before the newer refusal is not another extra check"
        );
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 970_000))
            .unwrap());
    }

    #[test]
    fn inconclusive_release_preserves_quota_interval_and_a_newer_claim() {
        let scratch = cortexkit_test_support::ScratchDir::new("neutral-claim-generation");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "neutral-claim-generation",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_billing_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 1000,
                recorded_at: 100,
                decision_id: "d".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 1000, Some("old")))
            .unwrap());
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 3_601_000, Some("new")))
            .unwrap());
        store
            .account_transaction(|tx| tx.release_inconclusive_probe("old", "p", "m", 3_601_001))
            .unwrap();
        assert_eq!(
            store.fence_rechecks().unwrap()[0].next_recheck_at,
            7_201_000,
            "an older cancellation cannot undo the new launch's atomic claim"
        );
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                recorded_at: 4_000_000,
                decision_id: "quota".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                8_000_000,
                Some("quota-probe")
            ))
            .unwrap());
        store
            .account_transaction(|tx| {
                tx.release_inconclusive_probe("quota-probe", "p", "m", 8_000_001)
            })
            .unwrap();
        let quota = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|f| f.quota)
            .unwrap();
        assert_eq!(
            quota.next_recheck_at, 8_900_000,
            "neutral non-quota recovery never widens the quota probe budget"
        );
    }

    #[test]
    fn consumed_authorization_neutral_release_reopens_the_claim_generation() {
        let scratch = cortexkit_test_support::ScratchDir::new("consumed-neutral-probe");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "consumed-neutral-probe",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_billing_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 1000,
                recorded_at: 100,
                decision_id: "d".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 1000, Some("selection")))
            .unwrap());
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_authorized_admission(
                &origins,
                1001,
                Some("selection"),
                "admission",
            ))
            .unwrap());
        store
            .account_transaction(|tx| tx.release_inconclusive_probe("admission", "p", "m", 1002))
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(
            store
                .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 1002))
                .unwrap(),
            "consumption must retain the clock generation independently of admission time"
        );
    }

    #[test]
    fn linked_pool_success_heals_alias_and_target_provider_origins() {
        use striatum_core::key_pool::PoolKey;
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("alias-pool-probe");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "alias-pool-probe",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .pool_wall(
                "alias",
                "key",
                crate::PoolWallKind::Billing,
                1000,
                "credits",
                100,
            )
            .unwrap();
        store
            .record_provider_cooldown(
                "target",
                "target/m",
                "target/m",
                &RouteStateUpdate {
                    cooldown_until: Some(1000.0),
                    observed_at_ms: Some(100),
                    unscoped_wall: true,
                    last_error: Some("rate limit".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let keys = vec![PoolKey::new("key", Some("a"), "active")];
        let crate::PoolAcquireOutcome::Leased(lease) = store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "alias",
                    session_id: "s",
                    keys: &keys,
                    now_ms: 1000,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                },
                &["alias", "target"],
                "m",
                &[],
                &Default::default(),
            )
            .unwrap()
        else {
            panic!("both due origins must admit one lease")
        };
        assert_eq!(store.fence_rechecks().unwrap().len(), 2);
        store
            .bind_fence_probe_owner(&lease.lease_id, "decision")
            .unwrap();
        store
            .account_transaction(|tx| tx.heal_authorized_probe("decision", "target", "m", 1001))
            .unwrap();
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "success must heal the whole authorized alias/target launch, not only one provider"
        );
    }

    #[test]
    fn linked_pool_neutral_outcome_releases_alias_and_target_provider_origins() {
        use striatum_core::key_pool::PoolKey;
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("alias-pool-neutral-probe");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "alias-pool-neutral-probe",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .pool_wall(
                "alias",
                "key",
                crate::PoolWallKind::Billing,
                1000,
                "credits",
                100,
            )
            .unwrap();
        store
            .record_provider_cooldown(
                "target",
                "target/m",
                "target/m",
                &RouteStateUpdate {
                    cooldown_until: Some(1000.0),
                    observed_at_ms: Some(100),
                    unscoped_wall: true,
                    last_error: Some("rate limit".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .pool_wall(
                "unrelated",
                "key",
                crate::PoolWallKind::Billing,
                2000,
                "credits",
                100,
            )
            .unwrap();
        let keys = vec![PoolKey::new("key", Some("a"), "active")];
        let crate::PoolAcquireOutcome::Leased(lease) = store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "alias",
                    session_id: "s",
                    keys: &keys,
                    now_ms: 1000,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                },
                &["alias", "target"],
                "m",
                &[],
                &Default::default(),
            )
            .unwrap()
        else {
            panic!("both due origins must admit one lease")
        };
        store
            .bind_fence_probe_owner(&lease.lease_id, "decision")
            .unwrap();
        store
            .account_transaction(|tx| {
                tx.release_inconclusive_probe("decision", "target", "m", 1001)
            })
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks.len(),
            3,
            "a neutral outcome releases claims without healing evidence"
        );
        let mut released = Vec::new();
        for clock in clocks {
            if clock.provider == "unrelated" {
                assert_eq!(
                    clock.next_recheck_at, 2000,
                    "an unclaimed origin is untouched"
                );
            } else {
                assert_eq!(
                    clock.next_recheck_at, 1001,
                    "{} must reopen its exact authorization",
                    clock.provider
                );
                assert!(clock.claim_owner.is_none());
                released.push(clock);
            }
        }
        assert_eq!(released.len(), 2);
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&released, 1001))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&released, 1001))
            .unwrap());
    }

    #[test]
    fn admission_sync_uses_linear_origin_membership_not_a_correlated_view_scan() {
        use std::sync::atomic::{AtomicU64, Ordering};
        unsafe extern "C" fn count(pointer: *mut std::ffi::c_void) -> std::ffi::c_int {
            // SAFETY: the counter lives until the connection's callback is removed.
            unsafe { &*(pointer as *const AtomicU64) }.fetch_add(1000, Ordering::Relaxed);
            0
        }
        let scratch = cortexkit_test_support::ScratchDir::new("linear-admission-sync");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "linear-admission-sync",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn| {
            conn.execute_batch("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO account_non_quota_route_fence SELECT 'p','c'||i,'m',8640000000000000,'d',100,100,'billing' FROM n;")?;
            sync(conn)?;
            let steps = AtomicU64::new(0);
            // SAFETY: the store mutex owns this live handle, and the callback
            // touches only the counter, never SQLite or the surrounding store.
            unsafe { rusqlite::ffi::sqlite3_progress_handler(conn.handle(),1000,Some(count),&steps as *const _ as *mut std::ffi::c_void); }
            let started = std::time::Instant::now();
            let result = sync(conn);
            let elapsed = started.elapsed();
            // SAFETY: remove the callback before its stack counter goes away.
            unsafe { rusqlite::ffi::sqlite3_progress_handler(conn.handle(),0,None,std::ptr::null_mut()); }
            result?;
            let steps = steps.load(Ordering::Relaxed);
            eprintln!("admission_sync origins=10000 steps={steps} elapsed_us={}",elapsed.as_micros());
            assert!(steps<40_000_000,"origin membership must be indexed, not quadratic: {steps} VM steps");
            Ok(())
        }).unwrap();
    }

    #[test]
    fn a_delayed_selected_probe_renews_its_expired_claim_before_admission() {
        let scratch = cortexkit_test_support::ScratchDir::new("late-probe-transfer");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "late-probe-transfer",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                decision_id: "old".into(),
                recorded_at: 0,
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 900_000, Some("d")))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &origins,
                1_800_001,
                Some("d")
            ))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 1_800_001))
            .unwrap());
        assert_eq!(
            store.fence_rechecks().unwrap()[0].next_recheck_at,
            2_700_001
        );
    }

    #[test]
    fn a_repeat_transient_observation_replaces_the_claimed_origins_short_retry() {
        let scratch = cortexkit_test_support::ScratchDir::new("transient-retry");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "transient-retry",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let observe = |id: &str, at, until| {
            store.account_transaction(|tx|tx.insert_observation(&crate::AccountObservationRow {
            observation_id:id.into(),principal:"test".into(),runner:"broca".into(),event_id:id.into(),kind:"transient_backoff".into(),admission_id:None,bound:true,
            observation_json:"{}".into(),effects_json:serde_json::json!([{"effect":"transient_backoff","provider":"p","account_id":"a","retry_at_ms":until}]).to_string(),received_at:at,
        })).unwrap()
        };
        observe("first", 0, 10_000);
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 10_000))
            .unwrap());
        observe("repeat", 10_001, 20_000);
        let origins = store.fence_rechecks().unwrap();
        assert_eq!(origins.len(), 1);
        assert_eq!(origins[0].due_at(), 20_000);
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 20_000))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 20_000))
            .unwrap());
    }

    #[test]
    fn a_second_pool_acquire_sees_the_fence_without_revoking_the_probe_holders_lease() {
        use striatum_core::key_pool::PoolKey;
        let scratch = cortexkit_test_support::ScratchDir::new("pool-probe-renew");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "pool-probe-renew",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let keys = vec![PoolKey::new("apikey:p", Some("a"), "active")];
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                10_000,
                "empty",
                0,
            )
            .unwrap();
        let request = crate::PoolAcquire {
            provider: "p",
            session_id: "session",
            keys: &keys,
            now_ms: 10_000,
            lease_ms: 120_000,
            affinity_idle_ms: 120_000,
        };
        let crate::PoolAcquireOutcome::Leased(lease) = store.pool_acquire(&request).unwrap() else {
            panic!("first probe must lease");
        };
        let crate::PoolAcquireOutcome::Exhausted(blocked) = store.pool_acquire(&request).unwrap()
        else {
            panic!("a second acquire is a second send authorization");
        };
        assert_eq!(blocked[0].reset_at_ms, Some(3_610_000));
        assert_eq!(
            store
                .pool_renew(
                    "p",
                    "session",
                    &lease.lease_id,
                    Some("apikey:p"),
                    10_001,
                    120_000
                )
                .unwrap(),
            Some(130_001)
        );
        assert_eq!(
            store
                .pool_renew(
                    "p",
                    "session",
                    "unknown-lease",
                    Some("apikey:p"),
                    10_001,
                    120_000
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn success_heals_only_the_known_pool_and_family_and_retains_other_evidence() {
        use striatum_core::accounts::{ModelFamily, PoolKey, PoolScope};
        let scratch = cortexkit_test_support::ScratchDir::new("quota-success-scope");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "quota-success-scope",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let gemini = PoolKey {
            name: Some("gemini".into()),
            scope: PoolScope::EveryModel,
            window_minutes: Some(300),
        };
        let claude = PoolKey {
            name: Some("claude_gpt".into()),
            scope: PoolScope::Family(ModelFamily::Opus),
            window_minutes: Some(300),
        };
        store.store.with_conn(|conn| {
            for (id,pool,recovery) in [("gemini",Some(&gemini),vec![]),("claude",Some(&claude),vec![]),("whole",None,vec![gemini.clone(),claude.clone()])] {
                conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
                    VALUES (?1,'p','a','{}','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',100,'test',100,?2,?3)",
                    params![id,pool.map(|pool|serde_json::to_string(pool).unwrap()),serde_json::to_string(&recovery).unwrap()])?;
            }
            Ok(())
        }).unwrap();
        // With no request scope, even a newer success cannot guess a pool.
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "claude", None, 101, 200)
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3
        );
        let scope = QuotaSuccessScope {
            family: ModelFamily::Opus,
            pool: Some("claude_gpt".into()),
        };
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "claude",
                    None,
                    99,
                    200,
                    Some(&scope),
                )
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3,
            "an older admitted run cannot heal later evidence"
        );
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "claude",
                    None,
                    101,
                    200,
                    Some(&scope),
                )
            })
            .unwrap();
        let remaining = store.account_read(|tx| tx.active_pool_fences("p")).unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining
            .iter()
            .any(|fence| fence.evidence.fence_id == "gemini"));
        assert_eq!(
            remaining
                .iter()
                .find(|fence| fence.evidence.fence_id == "whole")
                .unwrap()
                .recovery_set,
            vec![gemini.clone()]
        );
        let scope = QuotaSuccessScope {
            family: ModelFamily::Other,
            pool: Some("gemini".into()),
        };
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "gemini",
                    None,
                    102,
                    201,
                    Some(&scope),
                )
            })
            .unwrap();
        assert!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .is_empty(),
            "whole-account evidence requires recovery of both pools"
        );
    }

    #[test]
    fn selected_probe_authorization_transfers_once_without_a_second_interval() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-probe-transfer");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fence-probe-transfer",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                decision_id: "old".into(),
                recorded_at: 0,
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        let now = QUOTA_FENCE_RECHECK_MS;
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                now,
                Some("selection-owner")
            ))
            .unwrap());
        store
            .bind_fence_probe_owner("selection-owner", "model-decision")
            .unwrap();
        let claimed = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("other-decision")
            ))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("model-decision")
            ))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("model-decision")
            ))
            .unwrap());
        let after = store.fence_rechecks().unwrap();
        assert_eq!(after[0].next_recheck_at, now + QUOTA_FENCE_RECHECK_MS);
        assert_eq!(after[0].evidence_at, 0);
        assert_eq!(after[0].last_claimed_at, Some(now));
        assert_eq!(after[0].claim_owner, None);
    }

    #[test]
    fn every_non_quota_origin_caps_at_one_hour_preserves_shorter_retry_and_claims_once() {
        for duration in [10_000, 86_400_000] {
            let scratch = cortexkit_test_support::ScratchDir::new("all-fence-caps");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "all-fence-caps",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap();
            let now = crate::now_ms();
            let until = now + duration;
            store.store.with_conn(|conn|conn.execute_batch(&format!(
                "INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,{now});
                 INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,failure_backoff_until,failure_backoff_reason,failure_observed_at)
                   VALUES ('failure/m','failure/m','failure','m',{now},{until},'failure',{now});"))).unwrap();
            for kind in [
                crate::PoolWallKind::Billing,
                crate::PoolWallKind::RateLimited,
            ] {
                store
                    .pool_wall("pool", "key", kind, until, "refused", now)
                    .unwrap();
            }
            store
                .account_transaction(|tx| {
                    tx.insert_exclusion_with_recheck(
                        &crate::AccountExclusionRow {
                            principal: "test".into(),
                            admission_id: "d".into(),
                            credential_id: "c".into(),
                            runner_view_digest: "runner".into(),
                            roster_view_digest: "roster".into(),
                            observation_id: None,
                            recorded_at: now,
                        },
                        duration,
                    )?;
                    tx.record_non_quota_size_refusal(
                        &crate::AccountSizeRefusal {
                            provider: "p".into(),
                            credential_id: "c".into(),
                            model: "m".into(),
                            size_class: 17,
                            expires_at: until,
                            observed_at: now,
                        },
                        now,
                    )
                })
                .unwrap();
            store
                .record_transient_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: until,
                    decision_id: "d".into(),
                    recorded_at: now,
                })
                .unwrap();
            store
                .record_billing_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: until,
                    decision_id: "d".into(),
                    recorded_at: now,
                })
                .unwrap();
            use striatum_core::{RouteStateStore, RouteStateUpdate, RouteWallClass};
            let update = RouteStateUpdate {
                cooldown_until: Some(until as f64),
                wall_class: Some(RouteWallClass::Quota),
                ..Default::default()
            };
            // Even a typed quota label cannot supply the missing identity on a
            // legacy route/provider cooldown.
            store
                .update_route_state("route/m", "route/m", &update)
                .unwrap();
            store
                .record_provider_cooldown("provider", "provider/m", "provider/m", &update)
                .unwrap();
            store.account_transaction(|tx|tx.insert_observation(&crate::AccountObservationRow {
                observation_id:"o".into(),principal:"test".into(),runner:"broca".into(),event_id:"e".into(),kind:"transient_backoff".into(),
                admission_id:None,bound:true,observation_json:"{}".into(),received_at:now,
                effects_json:serde_json::json!([{"effect":"transient_backoff","provider":"p","account_id":"a","retry_at_ms":until}]).to_string(),
            })).unwrap();
            let clocks = store.fence_rechecks().unwrap();
            let actual: std::collections::BTreeSet<_> =
                clocks.iter().map(|clock| clock.kind.as_str()).collect();
            let expected: std::collections::BTreeSet<_> = [
                "credential",
                "principal",
                "pool_billing",
                "pool_rate",
                "non_quota_route",
                "transient_route",
                "non_quota_size",
                "route",
                "provider",
                "failure",
                "observation",
            ]
            .into_iter()
            .collect();
            assert_eq!(
                actual, expected,
                "every temporal non-quota kind must be scheduled"
            );
            for clock in &clocks {
                assert!(!clock.quota, "{}", clock.kind);
                assert!(
                    clock.next_recheck_at <= clock.evidence_at + 3_600_000,
                    "{}",
                    clock.kind
                );
                if duration == 10_000 && clock.kind != "credential" {
                    assert_eq!(clock.next_recheck_at, until, "{}", clock.kind);
                }
                assert!(
                    store
                        .account_transaction(|tx| tx
                            .claim_fence_rechecks(std::slice::from_ref(clock), clock.due_at()))
                        .unwrap(),
                    "{}",
                    clock.kind
                );
                assert!(
                    !store
                        .account_transaction(|tx| tx
                            .claim_fence_rechecks(std::slice::from_ref(clock), clock.due_at()))
                        .unwrap(),
                    "{}",
                    clock.kind
                );
            }
        }
    }

    #[test]
    fn direct_pool_acquire_admits_exactly_one_concurrent_probe() {
        let scratch = cortexkit_test_support::ScratchDir::new("pool-probe-concurrency");
        let store = std::sync::Arc::new(
            SqliteRouteStateStore::open(&crate::dev_descriptor(
                "pool-probe-concurrency",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap(),
        );
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                86_400_000,
                "empty",
                0,
            )
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let keys = vec![striatum_core::key_pool::PoolKey::new(
                        "apikey:p",
                        Some("a"),
                        "active",
                    )];
                    barrier.wait();
                    store
                        .pool_acquire(&crate::PoolAcquire {
                            provider: "p",
                            session_id: &format!("session-{i}"),
                            keys: &keys,
                            now_ms: NON_QUOTA_FENCE_MAX_MS,
                            lease_ms: 120_000,
                            affinity_idle_ms: 120_000,
                        })
                        .unwrap()
                })
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .filter(|outcome| matches!(outcome, crate::PoolAcquireOutcome::Leased(_)))
                .count(),
            1
        );
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                NON_QUOTA_FENCE_MAX_MS + 10_000,
                "still empty",
                NON_QUOTA_FENCE_MAX_MS + 1,
            )
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks[0].due_at(),
            NON_QUOTA_FENCE_MAX_MS + 10_000,
            "a repeat refusal preserves a shorter retry"
        );
    }

    #[test]
    fn success_heals_only_serving_identity_and_evidence_older_than_admission() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-success-boundary");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fence-success-boundary",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for credential in ["served", "sibling"] {
            store
                .record_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 0,
                    decision_id: "d".into(),
                    recorded_at: 100,
                })
                .unwrap();
            store
                .record_transient_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 10_000,
                    decision_id: "d".into(),
                    recorded_at: 100,
                })
                .unwrap();
        }
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "served", "m", None, 99, 200)
            })
            .unwrap();
        assert_eq!(store.account_route_fences(200).unwrap().len(), 2);
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "served", "m", None, 101, 200)
            })
            .unwrap();
        assert_eq!(
            store.account_route_fences(200).unwrap()[0].credential_id,
            "sibling"
        );
        assert_eq!(
            store.transient_account_route_fences(200).unwrap()[0].credential_id,
            "sibling"
        );
    }

    #[test]
    fn quota_rechecks_every_fifteen_minutes_and_reset_only_adds_a_check() {
        for reset in [0, 60_000, 2 * QUOTA_FENCE_RECHECK_MS] {
            let scratch = cortexkit_test_support::ScratchDir::new("quota-recheck");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "quota-recheck",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap();
            store
                .record_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: reset,
                    decision_id: "d".into(),
                    recorded_at: 0,
                })
                .unwrap();
            let clocks = store.fence_rechecks().unwrap();
            assert_eq!(clocks[0].next_recheck_at, QUOTA_FENCE_RECHECK_MS);
            let first = if reset == 60_000 {
                reset
            } else {
                QUOTA_FENCE_RECHECK_MS
            };
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first - 1))
                .unwrap());
            assert!(store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first))
                .unwrap());
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first))
                .unwrap());
            assert_eq!(
                store
                    .account_route_fences(100 * QUOTA_FENCE_RECHECK_MS)
                    .unwrap()
                    .len(),
                1,
                "a date never expires quota evidence"
            );
            let updated = store.fence_rechecks().unwrap();
            assert_eq!(updated[0].next_recheck_at, first + QUOTA_FENCE_RECHECK_MS);
            assert_eq!(updated[0].evidence_at, 0);
            if reset > first {
                assert_eq!(updated[0].due_at(), reset);
            } else {
                assert_eq!(updated[0].reset_check_at, None);
            }
        }
    }

    #[test]
    fn migration_retires_real_deadlines_but_preserves_quota_and_credential_evidence() {
        let scratch = cortexkit_test_support::ScratchDir::new("retire-expired-migration");
        let descriptor = crate::dev_descriptor(
            "retire-expired-migration",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let now = crate::now_ms();
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        old.store.with_conn(|conn|conn.execute_batch(&format!("
            INSERT INTO account_route_fence VALUES ('p','quota','m',0,'d',100,100,'quota');
            INSERT INTO account_route_fence VALUES ('p','transient','m',{},'d',100,100,'transient');
            INSERT INTO credential_entitlement_fences VALUES ('p','expired','ENTITLEMENT_ERROR',NULL,{});
            INSERT INTO pool_key_state(provider,credential_id,walled_until,cooldown_until,updated_at) VALUES ('p','expired',{0},{0},100);
            INSERT INTO pool_key_state(provider,credential_id,walled_until,updated_at) VALUES ('p','live',{},100);
            INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,cooldown_until,failure_backoff_until)
                VALUES ('p/m','p/m','p','m',100,{0},{0});
            INSERT INTO provider_cooldown_state(provider_key,cooldown_until,updated_at) VALUES ('old',{0},100);
        ",now-10_000,now-7*3_600_000,now+86_400_000))).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks.len(),
            3,
            "expired non-quota rows must not return as route-wide probe gates: {clocks:?}"
        );
        assert!(clocks
            .iter()
            .any(|f| f.quota && f.credential_id.as_deref() == Some("quota")));
        assert!(clocks
            .iter()
            .any(|f| f.kind == "pool_billing" && f.credential_id.as_deref() == Some("live")));
        assert_eq!(
            store.credential_entitlement_fences().unwrap().len(),
            1,
            "credential refusals had probe eligibility, not an expiry"
        );
        let credential: Vec<_> = clocks
            .into_iter()
            .filter(|f| f.kind == "credential")
            .collect();
        assert_eq!(credential.len(), 1);
        assert!(credential[0].due_at() <= crate::now_ms());
        let at = crate::now_ms();
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&credential, at))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&credential, at))
            .unwrap());
    }

    #[test]
    fn sync_retires_unmanaged_expired_origins_but_preserves_managed_short_retries() {
        let scratch = cortexkit_test_support::ScratchDir::new("retire-expired-sync");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "retire-expired-sync",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = crate::now_ms();
        store.store.with_conn(|conn|conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,updated_at,wall_observed_at) VALUES ('legacy',?1,100,100)",[now-10_000])).unwrap();
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "unmanaged historical evidence must retire at sync too"
        );
        let archived_before: i64 = store
            .store
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM fence_retired_origin", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(archived_before, 0, "scoring must remain read-only");
        store
            .account_transaction(|tx| {
                assert!(tx.claim_fence_rechecks(&[], now)?);
                Ok(())
            })
            .unwrap();
        let archived_after: i64 = store
            .store
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM fence_retired_origin", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(
            archived_after, 1,
            "writer synchronization archives the retired generation"
        );
        store
            .pool_wall(
                "p",
                "managed",
                crate::PoolWallKind::RateLimited,
                now - 1,
                "new refusal, immediate retry",
                now - 100,
            )
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks.len(),
            1,
            "new typed evidence must get its clock at ingress, even when already due"
        );
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now))
            .unwrap());
        let clocks = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now))
            .unwrap());
    }

    #[test]
    fn migrated_model_wall_uses_a_new_refusals_short_retry() {
        migrated_wall_short_retry("route");
    }
    #[test]
    fn migrated_provider_wall_uses_a_new_refusals_short_retry() {
        migrated_wall_short_retry("provider");
    }
    #[test]
    fn provider_repeat_keeps_an_independent_model_origin_and_its_generation() {
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("independent-model-provider-repeat");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "independent-model-provider-repeat",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = crate::now_ms();
        store
            .record_provider_cooldown(
                "p",
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    cooldown_until: Some((now + 1000) as f64),
                    observed_at_ms: Some(now),
                    unscoped_wall: true,
                    last_error: Some("provider-old".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .update_route_state(
                "p/other",
                "p/other",
                &RouteStateUpdate {
                    cooldown_until: Some((now + 10_000) as f64),
                    observed_at_ms: Some(now + 1),
                    unscoped_wall: true,
                    last_error: Some("independent".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .record_provider_cooldown(
                "p",
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    cooldown_until: Some((now + 100) as f64),
                    observed_at_ms: Some(now + 2),
                    unscoped_wall: true,
                    last_error: Some("provider-new".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert!(clocks.iter().any(|f|f.kind=="route" && f.model.as_deref()==Some("other") && f.evidence_at==now+1 && f.next_recheck_at==now+10_000),"provider fanout must not replace an independent model's refusal generation: {clocks:?}");
    }
    #[test]
    fn migrated_failure_wall_uses_a_new_refusals_short_retry() {
        migrated_wall_short_retry("failure");
    }

    fn migrated_wall_short_retry(kind: &str) {
        use striatum_core::{
            DecisionOutcomeUpdate, RouteOutcome, RouteStateStore, RouteStateUpdate,
        };
        let scratch = cortexkit_test_support::ScratchDir::new("migrated-route-short-retry");
        let descriptor = crate::dev_descriptor(
            "migrated-route-short-retry",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let now = crate::now_ms();
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        let decision = format!("historical-{kind}");
        old.store.with_conn(|conn| {
            if kind=="provider" {conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,last_error,last_error_at,updated_at) VALUES ('p',?1,'old',?2,?2)",params![now+86_400_000,now-100])?;}
            conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,last_error,last_error_at,cooldown_until,failure_backoff_until,consecutive_terminal_failures)
                VALUES ('p/m','p/m','p','m',?1,'old',?1,?2,?3,2)",params![now-100,if kind=="failure" {None} else {Some(now+86_400_000)},if kind=="failure" {Some(now+86_400_000)} else {None}])?;
            // Seed the old ledger directly; today's decision writer hashes the
            // catalog using model_facts columns absent from schema 81.
            conn.execute("INSERT INTO model_route_decisions (id,target_agent,requirements_json,
                selected_route_key,selected_provider_id,selected_model_id,score_json,rejected_json,
                anchor_version,created_at)
                VALUES (?1,'worker','{}','p/m','p','m','null','[]',2,?2)",params![decision,now])?;
            Ok(())
        }).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert_eq!(origins.len(), 1);
        let at = origins[0].due_at() + 1;
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, at - 1, Some(&decision)))
            .unwrap());
        let update = RouteStateUpdate {
            cooldown_until: Some((at + 10_000) as f64),
            observed_at_ms: Some(at),
            unscoped_wall: true,
            last_error: Some("new refusal".into()),
            ..Default::default()
        };
        match kind {
            "route" => store.update_route_state("p/m", "p/m", &update).unwrap(),
            "provider" => store
                .record_provider_cooldown("p", "p/m", "p/m", &update)
                .unwrap(),
            _ => store
                .set_decision_outcome(
                    &decision,
                    &DecisionOutcomeUpdate {
                        provider_outcome: None,
                        outcome: RouteOutcome::Error,
                        last_error: Some("server_error".into()),
                        observed_at_ms: at,
                        failure_backoff: Default::default(),
                    },
                )
                .unwrap(),
        }
        let expected = at + if kind == "failure" { 600_000 } else { 10_000 };
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(clocks.len(), 1);
        assert_eq!(
            clocks[0].next_recheck_at, expected,
            "{kind}: a new refusal must not inherit the old raw 24h deadline"
        );
    }

    #[test]
    fn classification_preserves_old_credentials_and_tolerates_removed_credentials() {
        let scratch = cortexkit_test_support::ScratchDir::new("retired-credential-classification");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "retired-credential-classification",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn|conn.execute("INSERT INTO credential_entitlement_fences VALUES ('p','c','billing_exhausted',NULL,0)",[])).unwrap();
        let result = store.account_transaction(|tx| tx.classify_credential_quota("p", "c", true));
        assert_eq!(
            result.unwrap(),
            true,
            "age alone must not retire standing credential evidence"
        );
        store
            .store
            .with_conn(|conn| conn.execute("DELETE FROM credential_entitlement_fences", []))
            .unwrap();
        assert!(
            !store
                .account_transaction(|tx| tx.classify_credential_quota("p", "c", true))
                .unwrap(),
            "an operator removal before synchronization is tolerated"
        );
    }

    #[test]
    fn an_older_completed_decision_cannot_reset_a_newer_failure_backoff() {
        use striatum_core::{
            DecisionInput, DecisionOutcomeUpdate, FailureBackoffConfig, RouteOutcome,
            RouteStateStore,
        };
        let scratch = cortexkit_test_support::ScratchDir::new("failure-reset-evidence-age");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "failure-reset-evidence-age",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let decision = |at| {
            let id = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some("p/m".into()),
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                })
                .unwrap();
            store
                .store
                .with_conn(|conn| {
                    conn.execute(
                        "UPDATE model_route_decisions SET created_at=?2 WHERE id=?1",
                        params![id, at],
                    )
                })
                .unwrap();
            id
        };
        let old_success = decision(10);
        let failure = decision(20);
        store
            .set_decision_outcome(
                &failure,
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: RouteOutcome::Error,
                    last_error: Some("server_error".into()),
                    observed_at_ms: 30,
                    failure_backoff: FailureBackoffConfig {
                        consecutive_terminal_failure_threshold: 1,
                    },
                },
            )
            .unwrap();
        let before: (i64,Option<f64>)=store.store.with_conn(|conn|conn.query_row("SELECT consecutive_terminal_failures,failure_backoff_until FROM model_route_state WHERE route_key='p/m'",[],|row|Ok((row.get(0)?,row.get(1)?)))).unwrap();
        assert_eq!(before, (1, Some(600_030.0)));
        store
            .set_decision_outcome(
                &old_success,
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: RouteOutcome::Completed,
                    last_error: None,
                    observed_at_ms: 40,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
        let after: (i64,Option<f64>)=store.store.with_conn(|conn|conn.query_row("SELECT consecutive_terminal_failures,failure_backoff_until FROM model_route_state WHERE route_key='p/m'",[],|row|Ok((row.get(0)?,row.get(1)?)))).unwrap();
        assert_eq!(after,before,"an in-flight success admitted before the refusal cannot erase its newer failure evidence");
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .any(|fence| fence.kind == "failure" && fence.evidence_at == 30));
        let new_success = decision(31);
        store
            .set_decision_outcome(
                &new_success,
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: RouteOutcome::Completed,
                    last_error: None,
                    observed_at_ms: 50,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
        let healed: (i64,Option<f64>)=store.store.with_conn(|conn|conn.query_row("SELECT consecutive_terminal_failures,failure_backoff_until FROM model_route_state WHERE route_key='p/m'",[],|row|Ok((row.get(0)?,row.get(1)?)))).unwrap();
        assert_eq!(
            healed,
            (0, None),
            "a success admitted after the refusal still resets the streak and fence"
        );
    }

    #[test]
    fn outcome_retry_wall_replaces_migrated_horizon_and_refreshes_evidence() {
        use striatum_core::{DecisionInput, DecisionOutcomeUpdate, RouteOutcome, RouteStateStore};
        let scratch = cortexkit_test_support::ScratchDir::new("outcome-wall-generation");
        let descriptor = crate::dev_descriptor(
            "outcome-wall-generation",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let now = crate::now_ms();
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        old.store.with_conn(|conn| conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,last_error_at,quota_exhausted_until)
            VALUES ('p/m','p/m','p','m',?1,?1,?2)", params![now-100,now+86_400_000])).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let report = |at, seconds| {
            let id = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some("p/m".into()),
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &id,
                    &DecisionOutcomeUpdate {
                        outcome: RouteOutcome::QuotaExhausted {
                            retry_after_secs: seconds,
                        },
                        observed_at_ms: at,
                        last_error: Some("route capacity".into()),
                        provider_outcome: None,
                        failure_backoff: Default::default(),
                    },
                )
                .unwrap();
        };
        report(now, 10);
        let clock = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|o| o.kind == "route")
            .unwrap();
        assert_eq!(
            clock.evidence_at, now,
            "outcome refusals must stamp their own generation"
        );
        assert_eq!(
            clock.next_recheck_at,
            now + 10_000,
            "short retry replaces the migrated raw horizon"
        );
        report(now - 1, 3600);
        let clock = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|o| o.kind == "route")
            .unwrap();
        assert_eq!(clock.evidence_at, now);
        assert_eq!(
            clock.next_recheck_at,
            now + 10_000,
            "a delayed older refusal cannot overwrite the newer one"
        );
    }

    #[test]
    fn new_outcome_retry_wall_preserves_its_stamp_beneath_provider_horizon() {
        new_model_refusal_beneath_provider_horizon(true);
    }

    #[test]
    fn new_route_update_wall_preserves_its_stamp_beneath_provider_horizon() {
        new_model_refusal_beneath_provider_horizon(false);
    }

    fn new_model_refusal_beneath_provider_horizon(outcome: bool) {
        use striatum_core::{
            DecisionInput, DecisionOutcomeUpdate, RouteOutcome, RouteStateStore, RouteStateUpdate,
        };
        let scratch = cortexkit_test_support::ScratchDir::new("new-model-wall-generation");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "new-model-wall-generation",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = crate::now_ms();
        store
            .record_provider_cooldown(
                "p",
                "p/seed",
                "p/seed",
                &RouteStateUpdate {
                    quota_exhausted_until: Some((now + NON_QUOTA_FENCE_MAX_MS) as f64),
                    observed_at_ms: Some(now - 100),
                    last_error: Some("provider capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .update_route_state(
                "p/telemetry",
                "p/telemetry",
                &RouteStateUpdate {
                    observed_at_ms: Some(now),
                    latency_ema_ms: Some(50.0),
                    ..Default::default()
                },
            )
            .unwrap();
        store.store.with_conn(|conn| {
            let inherited: (i64, bool) = conn.query_row(
                "SELECT wall_observed_at,wall_unscoped FROM model_route_state WHERE route_key='p/telemetry'",
                [], |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(inherited, (now - 100, false), "telemetry-only discovery still inherits provider provenance");
            Ok(())
        }).unwrap();
        let report = |at, seconds| {
            if outcome {
                let id = store
                    .record_decision(&DecisionInput {
                        selected_route_key: Some("p/new".into()),
                        selected_provider_id: Some("p".into()),
                        selected_model_id: Some("new".into()),
                        ..Default::default()
                    })
                    .unwrap();
                store
                    .set_decision_outcome(
                        &id,
                        &DecisionOutcomeUpdate {
                            outcome: RouteOutcome::QuotaExhausted {
                                retry_after_secs: seconds,
                            },
                            observed_at_ms: at,
                            last_error: Some("model capacity".into()),
                            provider_outcome: None,
                            failure_backoff: Default::default(),
                        },
                    )
                    .unwrap();
            } else {
                store
                    .update_route_state(
                        "p/new",
                        "p/new",
                        &RouteStateUpdate {
                            quota_exhausted_until: Some((at + seconds as i64 * 1000) as f64),
                            observed_at_ms: Some(at),
                            unscoped_wall: true,
                            last_error: Some("model capacity".into()),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
        };
        report(now, 10);
        let assert_generation = || {
            let clocks = store.fence_rechecks().unwrap();
            let route = clocks
                .iter()
                .find(|o| o.kind == "route" && o.model.as_deref() == Some("new"))
                .unwrap();
            assert_eq!(
                route.evidence_at, now,
                "a direct new-row refusal must retain its own stamp"
            );
            assert_eq!(route.next_recheck_at, now + 10_000);
            store
                .store
                .with_conn(|conn| {
                    let unscoped: bool = conn.query_row(
                        "SELECT wall_unscoped FROM model_route_state WHERE route_key='p/new'",
                        [],
                        |row| row.get(0),
                    )?;
                    assert!(
                        unscoped,
                        "the model refusal must not inherit the provider's scope"
                    );
                    Ok(())
                })
                .unwrap();
        };
        assert_generation();
        report(now - 50, 3600);
        assert_generation();
    }

    #[test]
    fn below_raised_threshold_failure_keeps_the_managed_fence_generation() {
        use striatum_core::{
            DecisionInput, DecisionOutcomeUpdate, FailureBackoffConfig, RouteOutcome,
            RouteStateStore,
        };
        let scratch = cortexkit_test_support::ScratchDir::new("raised-threshold-generation");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "raised-threshold-generation",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for (at, threshold) in [(100, 1), (700_101, 10)] {
            let id = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some("p/m".into()),
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &id,
                    &DecisionOutcomeUpdate {
                        outcome: RouteOutcome::Error,
                        observed_at_ms: at,
                        last_error: Some("server_error".into()),
                        provider_outcome: None,
                        failure_backoff: FailureBackoffConfig {
                            consecutive_terminal_failure_threshold: threshold,
                        },
                    },
                )
                .unwrap();
        }
        store
            .account_transaction(|tx| {
                tx.claim_fence_rechecks(&[], 700_102)?;
                Ok(())
            })
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert!(clocks.iter().any(|o| o.kind == "failure" && o.evidence_at == 700_101),
            "new failure evidence retaining an old deadline is managed, not expired legacy history: {clocks:?}");
    }

    #[test]
    fn first_route_wall_survives_a_later_non_wall_update() {
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("first-route-wall");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "first-route-wall",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .update_route_state(
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    observed_at_ms: Some(101),
                    latency_ema_ms: Some(20.0),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .update_route_state(
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    observed_at_ms: Some(100),
                    cooldown_until: Some(1100.0),
                    unscoped_wall: true,
                    last_error: Some("capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            store
                .fence_rechecks()
                .unwrap()
                .iter()
                .any(|o| o.kind == "route" && o.evidence_at == 100),
            "an unrelated update is not a prior refusal"
        );
    }

    #[test]
    fn first_provider_wall_survives_a_later_non_wall_update() {
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("first-provider-wall");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "first-provider-wall",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_provider_cooldown(
                "p",
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    observed_at_ms: Some(101),
                    quota_pressure: Some(0.5),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .record_provider_cooldown(
                "p",
                "p/m",
                "p/m",
                &RouteStateUpdate {
                    observed_at_ms: Some(100),
                    cooldown_until: Some(1100.0),
                    unscoped_wall: true,
                    last_error: Some("capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            store
                .fence_rechecks()
                .unwrap()
                .iter()
                .any(|o| o.kind == "provider" && o.evidence_at == 100),
            "an unrelated provider update is not a prior refusal"
        );
    }

    #[test]
    fn migration_does_not_turn_non_wall_updates_into_refusal_evidence() {
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("migrated-first-refusal");
        let descriptor = crate::dev_descriptor(
            "migrated-first-refusal",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        old.store.with_conn(|conn| conn.execute_batch("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at) VALUES ('p/m','p/m','p','m',101);
            INSERT INTO provider_cooldown_state(provider_key,updated_at) VALUES ('q',101);")).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let update = RouteStateUpdate {
            observed_at_ms: Some(100),
            cooldown_until: Some(1100.0),
            unscoped_wall: true,
            last_error: Some("capacity".into()),
            ..Default::default()
        };
        store.update_route_state("p/m", "p/m", &update).unwrap();
        store
            .record_provider_cooldown("q", "q/m", "q/m", &update)
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert_eq!(
            origins.len(),
            2,
            "migration must not invent a prior refusal from unrelated activity: {origins:?}"
        );
    }

    #[test]
    fn migration_82_converts_quota_now_and_caps_non_quota_once() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-migration");
        let descriptor = crate::dev_descriptor(
            "fence-migration",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        let now = crate::now_ms();
        old.store.with_conn(|conn| conn.execute_batch(&format!(
            "INSERT INTO account_route_fence VALUES ('p','c','m',{},'d',{}, {},'quota');
             INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,{});
             INSERT INTO pool_key_state(provider,credential_id,walled_until,cooldown_until,updated_at)
               VALUES ('p','key',{},{},{});",
            now+99_000_000,now-100,now-100,now-100,now+99_000_000,now+10_000,now-100))).unwrap();
        drop(old);
        // The migration stamps its own clock in whole seconds when the store
        // opens, so bound it by the second containing `now` and a reading taken
        // after the open, rather than assuming both land in the same second.
        let earliest = now - now.rem_euclid(1000);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let latest = crate::now_ms();
        let clocks = store.fence_rechecks().unwrap();
        let quota = clocks
            .iter()
            .find(|clock| clock.kind == "account_route")
            .unwrap();
        assert!(
            (earliest..=latest).contains(&quota.next_recheck_at),
            "quota recheck {} outside migration window {earliest}..={latest}",
            quota.next_recheck_at
        );
        let billing = clocks
            .iter()
            .find(|clock| clock.kind == "pool_billing")
            .unwrap();
        assert!(
            (earliest + NON_QUOTA_FENCE_MAX_MS..=latest + NON_QUOTA_FENCE_MAX_MS)
                .contains(&billing.next_recheck_at),
            "non-quota cap {} is not one hour after the migration clock",
            billing.next_recheck_at
        );
        let short = clocks
            .iter()
            .find(|clock| clock.kind == "pool_rate")
            .unwrap();
        assert_eq!(short.next_recheck_at, now + 10_000);
        let first: Vec<_> = clocks
            .iter()
            .map(|clock| (clock.kind.clone(), clock.next_recheck_at))
            .collect();
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            first,
            store
                .fence_rechecks()
                .unwrap()
                .iter()
                .map(|clock| (clock.kind.clone(), clock.next_recheck_at))
                .collect::<Vec<_>>()
        );
        drop(store);
        let error = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81)
            .err()
            .unwrap();
        assert!(error.to_string().contains("store_ahead"));
        // The refusal names the store's version, the newest migration.
        let latest = crate::route_state_migrations().last().unwrap().version;
        assert!(error.to_string().contains(&latest.to_string()));
    }

    #[test]
    fn strict_probe_claim_is_atomic_across_distinct_connections() {
        for case in ["credential", "account", "combined"] {
            let scratch = cortexkit_test_support::ScratchDir::new("fence-concurrency");
            let path = scratch.path().join("store.db");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "fence-concurrency",
                path.to_string_lossy(),
            ))
            .unwrap();
            if case != "account" {
                store
                    .record_credential_entitlement_fence(
                        &striatum_core::route_state_store::CredentialRefusal {
                            provider_id: "p".into(),
                            credential_id: "c".into(),
                            code: "ENTITLEMENT_ERROR".into(),
                        },
                        None,
                        0,
                    )
                    .unwrap();
            }
            store.store.with_conn(|conn| {
            if case!="credential" {conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at)
                VALUES ('f','p','a','null','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',0,'test',0)",[])?;}
            if case=="combined" {conn.execute("INSERT INTO account_route_fence VALUES ('p','c','m',0,'d',0,0,'quota')",[])?;}
            Ok(())
        }).unwrap();
            let origins = store.fence_rechecks().unwrap();
            assert_eq!(
                origins.len(),
                if case == "combined" { 3 } else { 1 },
                "the concurrency proof must claim a real managed origin set"
            );
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let path = path.clone();
                    let barrier = barrier.clone();
                    let origins = origins.clone();
                    std::thread::spawn(move || {
                        let mut conn = Connection::open(path).unwrap();
                        conn.busy_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                        barrier.wait();
                        let tx = conn
                            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                            .unwrap();
                        let won = claim(&tx, &origins, NON_QUOTA_FENCE_MAX_MS).unwrap();
                        tx.commit().unwrap();
                        won
                    })
                })
                .collect();
            assert_eq!(
                threads
                    .into_iter()
                    .map(|thread| thread.join().unwrap() as usize)
                    .sum::<usize>(),
                1
            );
            if case != "account" {
                assert_eq!(
                    store.credential_entitlement_fences().unwrap()[0].observed_at_ms,
                    0,
                    "claim preserves refusal evidence"
                );
            }
        }
    }
}

impl crate::AccountTx<'_> {
    pub fn bind_fence_probe_owner(&self, from: &str, to: &str) -> RouteStateResult<()> {
        self.command_scope(|tx| {
            tx.transfer_probe_authorization(from, to)?;
            tx.conn
                .execute(
                    "UPDATE fence_recheck SET claim_owner=?2 WHERE claim_owner=?1",
                    params![from, to],
                )
                .map(|_| ())
                .map_err(err)
        })
    }
}

#[cfg(test)]
#[path = "fence_rechecks/origin/tests.rs"]
mod origin_tests;
