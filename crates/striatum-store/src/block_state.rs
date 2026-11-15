//! In-memory copy of the block tables: the eleven `fence_origin` source
//! tables, the probe clocks, route clears, probe authorizations, success
//! pools and account eligibility.
//!
//! The SQL statements stay the only definition of every write. The writer
//! connection's update hook records each row a command changes in these
//! tables (triggers included), and the writer reads those rows back by rowid
//! inside the same transaction, after the command and before its savepoint is
//! released. A rolled-back command's rows are discarded with the forked
//! working state that received them. The loader reads the same projection of
//! every row in the state's single load transaction, so memory always equals
//! a fresh load.
//!
//! Each source row is held as the `fence_origin` rows SQLite computes for it
//! (evidence signatures and keys come from SQLite's own JSON functions),
//! together with the raw columns the request readers return. Two
//! view predicates span rows and are applied when blocks are derived: a
//! `credential` origin's class depends on active `credential-quota:` account
//! fences, and a `route` origin is hidden by a provider row that records the
//! same wall. [`BlockState::blocks`] then merges sources and clocks exactly as
//! the SQL adapter does, through `fence_rechecks::derive_blocks`.
//!
//! Cleared account fences and retirement records are never held: the view
//! ignores both, and nothing derived from them is needed. Every
//! `provider_cooldown_state` and `model_route_state` row is held, walled or
//! not, because the route-state snapshot and `route.cooldown_status` read
//! them whole (see `route_snapshot.rs`).

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row};
use striatum_core::blocks::Block;

use crate::fence_rechecks::{self, Clocks, FenceRecheck, LoadedBlocks, RouteClear, ViewOrigin};

mod dry_run;
pub use dry_run::DryRunRepair;

#[cfg(test)]
#[path = "block_storage/state/tests.rs"]
mod consolidated;

/// Every table whose rows the block state holds. The writer connection's
/// update hook and delete authorizer watch exactly these.
pub const BLOCK_TABLES: [&str; 16] = [
    "account_fence",
    "credential_entitlement_fences",
    "account_route_fence",
    "account_non_quota_route_fence",
    "pool_key_state",
    "provider_cooldown_state",
    "model_route_state",
    "account_size_refusal",
    "account_non_quota_size_refusal",
    "account_principal_exclusion",
    "account_transient_backoff",
    "fence_recheck",
    "fence_recheck_route_clear",
    "fence_probe_authorization",
    "fence_success_pool",
    "account_eligibility",
];

const ACCOUNT_FENCE_TABLE: usize = 0;
const CREDENTIAL_TABLE: usize = 1;
const ROUTE_FENCE_TABLE: usize = 2;
const NON_QUOTA_ROUTE_FENCE_TABLE: usize = 3;
const PROVIDER_TABLE: usize = 5;
const ROUTE_TABLE: usize = 6;
const SIZE_TABLE: usize = 7;
const NON_QUOTA_SIZE_TABLE: usize = 8;
const EXCLUSION_TABLE: usize = 9;
const CLOCK_TABLE: usize = 11;
const ROUTE_CLEAR_TABLE: usize = 12;
const AUTHORIZATION_TABLE: usize = 13;
const SUCCESS_POOL_TABLE: usize = 14;
const ELIGIBILITY_TABLE: usize = 15;

pub(crate) fn table_index(name: &str) -> Option<u8> {
    BLOCK_TABLES
        .iter()
        .position(|table| *table == name)
        .map(|index| index as u8)
}

thread_local! {
    /// Rows changed in the block tables by the account command running on
    /// this thread. `None` while no command runs.
    static CAPTURE: RefCell<Option<Vec<(u8, i64)>>> = const { RefCell::new(None) };
    /// Whether a test seeding seam is writing on this thread.
    static SEEDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether a test makes writes on this thread that the capture misses.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) static UNSEEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Called by the writer connection's update hook. Returns false when no
/// account command is running on this thread, which means the write bypassed
/// the account writer.
pub(crate) fn capture(table: u8, rowid: i64) -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if UNSEEN.with(std::cell::Cell::get) {
        // A write the capture misses, and nothing reports.
        return true;
    }
    CAPTURE.with(|capture| match capture.borrow_mut().as_mut() {
        Some(rows) => {
            rows.push((table, rowid));
            true
        }
        None => false,
    })
}

/// Whether the write the update hook reports comes from a test seeding seam
/// such as `with_writer_for_test`. Such writes bypass the account writer on
/// purpose; they still make the writer reload, but are not violations.
pub(crate) fn seeding() -> bool {
    SEEDING.with(|seeding| seeding.get())
}

/// Marks writes on this thread as test seeding until dropped.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct Seeding(bool);

#[cfg(any(test, feature = "test-support"))]
impl Seeding {
    pub(crate) fn begin() -> Self {
        Self(SEEDING.with(|seeding| seeding.replace(true)))
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for Seeding {
    fn drop(&mut self) {
        SEEDING.with(|seeding| seeding.set(self.0));
    }
}

/// Collects the block rows one account command changes.
pub(crate) struct Capture(());

impl Capture {
    pub(crate) fn begin() -> Self {
        CAPTURE.with(|capture| *capture.borrow_mut() = Some(Vec::new()));
        Self(())
    }

    pub(crate) fn finish(self) -> Vec<(u8, i64)> {
        CAPTURE.with(|capture| capture.borrow_mut().take().unwrap_or_default())
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        CAPTURE.with(|capture| *capture.borrow_mut() = None);
    }
}

static READ_BACKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Keyed reads the writer made to copy changed block rows into memory. They
/// run inside the writer's transaction and are counted apart from request
/// reads.
pub fn block_read_backs() -> u64 {
    READ_BACKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// The projection of each table's rows. Source tables select, per view
/// branch the table feeds, whether the row is in the view and then the
/// view's 14 columns, computed with the view's own expressions; then the
/// columns the cross-row predicates need; then the raw columns the request
/// readers return, in the order those readers' row decoders expect. Each
/// select starts with the rowid and the table's primary key, as JSON, which
/// lets a row replaced by a REPLACE conflict resolution (whose delete the update hook
/// does not report) be found by key. None has a WHERE clause: the read-back
/// appends one.
const SELECTS: [&str; 16] = [
    // account_fence: only active fences are in the view.
    "SELECT rowid,json_array(fence_id),
        cleared_at IS NULL,'account',json_array(fence_id),provider,account_id,NULL,NULL,NULL,NULL,NULL,
        fence_json,MIN(observed_at,created_at),1,NULL,json_extract(fence_json,'$.reset.at_ms'),
        fence_id,
        fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,admission_id,
        observation_id,created_at,cleared_at,cleared_reason,pool_key_json,recovery_set_json
     FROM account_fence",
    // credential_entitlement_fences: the class is derived later from the
    // quota-hold prefix in the last column.
    "SELECT rowid,json_array(provider_id,credential_id),
        1,'credential',json_array(provider_id,credential_id),provider_id,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(code,observed_at_ms),observed_at_ms,NULL,NULL,NULL,
        'credential-quota:'||json_array(provider_id,credential_id)||':',
        provider_id,credential_id,code,listing_fingerprint,observed_at_ms
     FROM credential_entitlement_fences",
    "SELECT rowid,json_array(provider,credential_id,model),
        wall_class='quota','account_route',json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
        json_array(wall_class,until_ms,COALESCE(last_reported_at,recorded_at)),COALESCE(last_reported_at,recorded_at),
        wall_class='quota',until_ms,CASE WHEN wall_class='quota' THEN until_ms END,
        provider,credential_id,model,until_ms,decision_id,recorded_at,wall_class,last_reported_at
     FROM account_route_fence",
    "SELECT rowid,json_array(provider,credential_id,model,wall_kind),
        1,CASE WHEN wall_kind='transient' THEN 'transient_route' ELSE 'non_quota_route' END,
        json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
        json_array(until_ms,last_reported_at),last_reported_at,0,until_ms,NULL,
        provider,credential_id,model,until_ms,decision_id,recorded_at,wall_kind
     FROM account_non_quota_route_fence",
    "SELECT rowid,json_array(provider,credential_id),
        walled_until IS NOT NULL,'pool_billing',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(walled_until,wall_reason,COALESCE(wall_observed_at,updated_at)),COALESCE(wall_observed_at,updated_at),0,walled_until,NULL,
        cooldown_until IS NOT NULL,'pool_rate',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(cooldown_until,cooldown_reason,COALESCE(cooldown_observed_at,updated_at)),COALESCE(cooldown_observed_at,updated_at),0,cooldown_until,NULL,
        provider,credential_id,last_used_at,walled_until,cooldown_until
     FROM pool_key_state",
    // provider_cooldown_state: every row is held, because a route origin
    // is compared with its provider's row whether or not that row walls, and
    // `route.cooldown_status` reads the row itself.
    concat!("SELECT rowid,json_array(provider_key),
        cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL,'provider',json_array(provider_key),provider_key,NULL,NULL,NULL,NULL,NULL,NULL,
        json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
        0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL,
        provider_key,last_error,COALESCE(wall_observed_at,last_error_at,updated_at),
        COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0),wall_class,wall_unscoped,",
        crate::route_snapshot::provider_cooldown_columns!(),
     " FROM provider_cooldown_state"),
    // model_route_state: every row is held, walled or not: the route-state
    // snapshot reads each route's latency, success and pressure too.
    concat!("SELECT rowid,json_array(route_key),
        cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL,'route',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
        json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
        0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL,
        failure_backoff_until IS NOT NULL,'failure',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
        json_array(failure_backoff_until,failure_backoff_reason,failure_observed_at),COALESCE(failure_observed_at,last_error_at,updated_at),
        0,failure_backoff_until,NULL,
        provider_id,last_error,COALESCE(wall_observed_at,last_error_at,updated_at),
        COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0),wall_class,wall_unscoped,route_key,",
        crate::route_snapshot::route_state_columns!(),
     ",wall_unscoped FROM model_route_state"),
    "SELECT rowid,json_array(provider,credential_id,model,size_class),
        1,'size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
        json_array(observed_at,expires_at),observed_at,1,expires_at,expires_at,
        provider,credential_id,model,size_class,expires_at,observed_at
     FROM account_size_refusal",
    "SELECT rowid,json_array(provider,credential_id,model,size_class),
        1,'non_quota_size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
        json_array(observed_at,expires_at),observed_at,0,expires_at,NULL,
        provider,credential_id,model,size_class,expires_at,observed_at
     FROM account_non_quota_size_refusal",
    "SELECT rowid,json_array(principal,admission_id,credential_id),
        1,'principal',json_array(principal,admission_id,credential_id),'',NULL,credential_id,NULL,NULL,principal,admission_id,
        json_array(recorded_at,runner_view_digest,roster_view_digest),recorded_at,0,recorded_at+86400000,NULL,
        principal,admission_id,credential_id,runner_view_digest,roster_view_digest,observation_id,recorded_at
     FROM account_principal_exclusion",
    "SELECT rowid,json_array(provider,account_id),
        1,'observation',json_array(provider,account_id),provider,account_id,NULL,NULL,NULL,NULL,NULL,
        json_array(evidence_key,effects_json),evidence_at,0,retry_at_ms,NULL
     FROM account_transient_backoff",
    "SELECT rowid,json_array(kind,origin_key),kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,classified,next_recheck_at,reset_check_at,last_claimed_at,claim_owner,claim_generation,cleared_at
     FROM fence_recheck",
    "SELECT rowid,json_array(kind,origin_key,provider,model),kind,origin_key,evidence,model
     FROM fence_recheck_route_clear",
    "SELECT rowid,json_array(owner,kind,origin_key),owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation
     FROM fence_probe_authorization",
    "SELECT rowid,json_array(fence_id,pool_key_json),fence_id,pool_key_json,evidence_at,healed_at
     FROM fence_success_pool",
    "SELECT rowid,json_array(provider,credential_id,model),
        provider,credential_id,model,listing_fingerprint,code,observed_at
     FROM account_eligibility",
];

/// Marks the block state's own statements, its load and its read-backs, so
/// a read trace can tell them from request reads.
pub(crate) const MAINTENANCE_READ: &str = "/* block state maintenance */";

/// Marks a keyed read a decision makes to check that SQL holds the rows it
/// read from the working state, where the decision writes nothing a keyed
/// update could check.
pub(crate) const STATE_CHECK: &str = "/* block state check */";

/// For each table, the rows the state holds, as a predicate on the table:
/// the rows some view branch selects (every row for a table whose reader
/// returns all of them). [`crate::AccountTx::reconcile_block_state`]
/// compares their count and highest rowid with memory.
const HELD_ROWS: [&str; 16] = [
    "cleared_at IS NULL",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
    "1",
];

/// The primary-key columns of each source table, in the order of the key
/// `SELECTS` computes for its rows (`json_array(pk…)`).
const SOURCE_KEYS: [&[&str]; 11] = [
    &["fence_id"],
    &["provider_id", "credential_id"],
    &["provider", "credential_id", "model"],
    &["provider", "credential_id", "model", "wall_kind"],
    &["provider", "credential_id"],
    &["provider_key"],
    &["route_key"],
    &["provider", "credential_id", "model", "size_class"],
    &["provider", "credential_id", "model", "size_class"],
    &["principal", "admission_id", "credential_id"],
    &["provider", "account_id"],
];

/// The block tables a provider's pool decisions read, each with the column
/// that names the provider; an index or the primary key leads with it.
/// Principal exclusions are not by provider and never apply to a pool.
const PROVIDER_COLUMNS: [(usize, &str); 11] = [
    (ACCOUNT_FENCE_TABLE, "provider"),
    (CREDENTIAL_TABLE, "provider_id"),
    (ROUTE_FENCE_TABLE, "provider"),
    (NON_QUOTA_ROUTE_FENCE_TABLE, "provider"),
    (4, "provider"),
    (PROVIDER_TABLE, "provider_key"),
    (ROUTE_TABLE, "provider_id"),
    (SIZE_TABLE, "provider"),
    (NON_QUOTA_SIZE_TABLE, "provider"),
    (10, "provider"),
    (CLOCK_TABLE, "provider"),
];

/// The provider a held row of `table` belongs to, as [`PROVIDER_COLUMNS`]
/// names it: an account fence's provider column, a route row's
/// `provider_id`, a clock's scope, and otherwise the first column of the
/// row's primary key.
fn row_provider(table: usize, key: &str, held: &Held) -> Option<String> {
    match held {
        Held::Clock(block, _) => return Some(block.scope.provider.clone()),
        Held::Source {
            raw: Raw::AccountFence { row, .. },
            ..
        } => return Some(row.provider.clone()),
        Held::Source {
            link: Link::Route { provider, .. },
            ..
        } => return Some(provider.clone()),
        _ => {}
    }
    if table == ACCOUNT_FENCE_TABLE || table == ROUTE_TABLE {
        return None;
    }
    let parsed: Vec<serde_json::Value> = serde_json::from_str(key).ok()?;
    parsed.first()?.as_str().map(str::to_owned)
}

/// The keyed check of one source row by its primary key.
fn source_check_sql(table: usize) -> String {
    let predicate = SOURCE_KEYS[table]
        .iter()
        .enumerate()
        .map(|(at, column)| format!("{column}=?{}", at + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("{STATE_CHECK} {} WHERE {predicate}", SELECTS[table])
}

/// The keyed check of a credential's quota holds: a primary-key range of
/// `account_fence` ids.
fn quota_hold_check_sql() -> String {
    format!(
        "{STATE_CHECK} SELECT fence_id FROM account_fence
                         WHERE fence_id>=?1 AND fence_id<?2 AND cleared_at IS NULL ORDER BY fence_id"
    )
}

/// The check of one table's rows of a provider, by its provider column.
fn provider_rows_check_sql(table: usize, column: &str) -> String {
    format!("{STATE_CHECK} {} WHERE {column}=?1", SELECTS[table])
}

/// The keyed check of one admission's principal exclusions.
fn exclusions_check_sql() -> String {
    format!(
        "{STATE_CHECK} {} WHERE principal=?1 AND admission_id=?2",
        SELECTS[EXCLUSION_TABLE]
    )
}

/// The check of a provider's active account fences' success pools, by the
/// pool table's primary key under the fences' provider index.
fn success_pools_check_sql() -> String {
    format!(
        "{STATE_CHECK} {} WHERE fence_id IN (SELECT fence_id FROM account_fence WHERE provider=?1 AND cleared_at IS NULL)",
        SELECTS[SUCCESS_POOL_TABLE]
    )
}

/// The check of the route clears of a provider's clocks, by the clear
/// table's primary key under the clocks' provider index.
fn route_clears_check_sql() -> String {
    format!(
        "{STATE_CHECK} {} WHERE (kind,origin_key) IN (SELECT kind,origin_key FROM fence_recheck WHERE provider=?1)",
        SELECTS[ROUTE_CLEAR_TABLE]
    )
}

/// The keyed check of a clock by origin.
fn by_origin_check_sql(table: usize) -> String {
    format!(
        "{STATE_CHECK} {} WHERE kind=?1 AND origin_key=?2",
        SELECTS[table]
    )
}

/// Every statement a decision runs to check the state against SQL
/// (`STATE_CHECK`) or a route-state writer runs to read the row it writes
/// (`ROUTE_WRITE_READ`): the reviewed list of exempt reads. The block read
/// trace fails on any marked read not listed here, and
/// `every_exempt_read_is_keyed` proves each listed one keyed.
#[cfg(any(test, feature = "test-support"))]
pub fn reviewed_exempt_reads() -> Vec<String> {
    let mut reads: Vec<String> = (0..SOURCE_KEYS.len()).map(source_check_sql).collect();
    reads.push(quota_hold_check_sql());
    reads.extend(
        PROVIDER_COLUMNS
            .iter()
            .map(|(table, column)| provider_rows_check_sql(*table, column)),
    );
    reads.push(route_clears_check_sql());
    reads.push(exclusions_check_sql());
    reads.push(success_pools_check_sql());
    reads.push(by_origin_check_sql(CLOCK_TABLE));
    reads.push(fence_rechecks::authorization_check_sql());
    reads.push(fence_rechecks::probe_clock_check_sql());
    reads.push(fence_rechecks::no_authorized_clock_check_sql(false));
    reads.push(fence_rechecks::no_authorized_clock_check_sql(true));
    reads.push(fence_rechecks::unstarted_admission_check_sql());
    reads.extend(
        [
            crate::ROUTE_READ_PROVIDER_PRIOR_OBSERVED,
            crate::ROUTE_READ_PROVIDER_INHERITED_ROUTES,
            crate::ROUTE_READ_PROVIDER_EXISTING,
            crate::ROUTE_READ_ROUTE_PRIOR_OBSERVED,
            crate::ROUTE_READ_ROUTE_EXISTING,
            crate::ROUTE_READ_ROUTE_PROVIDER_ROW,
            crate::ROUTE_READ_OUTCOME_FAILURE_STATE,
            crate::ROUTE_READ_CLEAR_FAILURE_STATE,
        ]
        .map(str::to_owned),
    );
    reads
}

/// The source table of an origin kind, and the key column a kind adds to its
/// origin key (the wall kind of an `account_non_quota_route_fence` row).
fn source_of(kind: &str) -> Option<(usize, Option<&'static str>)> {
    Some(match kind {
        "account" => (ACCOUNT_FENCE_TABLE, None),
        "credential" => (CREDENTIAL_TABLE, None),
        "account_route" => (ROUTE_FENCE_TABLE, None),
        "non_quota_route" => (NON_QUOTA_ROUTE_FENCE_TABLE, Some("billing")),
        "transient_route" => (NON_QUOTA_ROUTE_FENCE_TABLE, Some("transient")),
        "pool_billing" | "pool_rate" => (4, None),
        "provider" => (PROVIDER_TABLE, None),
        "route" | "failure" => (ROUTE_TABLE, None),
        "size" => (SIZE_TABLE, None),
        "non_quota_size" => (NON_QUOTA_SIZE_TABLE, None),
        "principal" => (EXCLUSION_TABLE, None),
        "observation" => (10, None),
        _ => return None,
    })
}

static RECONCILE_RELOADS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// Calls of the reconcile so far on this thread, to run its full
    /// comparison on every [`FULL_RECONCILE_EVERY`]th. The reconcile runs in
    /// the Tick command, on the store's own account-writer thread, so this
    /// counts per store: stores sharing a process cannot take each other's
    /// full comparisons.
    static RECONCILE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many reconciles apart the full comparison runs: six ten-second ticks.
const FULL_RECONCILE_EVERY: u64 = 6;

/// How many times the periodic reconcile found the state's rows differing
/// from SQL and had it reloaded. Any count means a write reached a block
/// table without the capture seeing it.
pub fn block_reconcile_reloads() -> u64 {
    RECONCILE_RELOADS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Each table's held-row count and highest held rowid, read from SQL.
pub(crate) fn sql_fingerprints(conn: &Connection) -> rusqlite::Result<Vec<(usize, Option<i64>)>> {
    BLOCK_TABLES
        .iter()
        .zip(HELD_ROWS)
        .map(|(table, held)| {
            conn.prepare_cached(&format!(
                "{MAINTENANCE_READ} SELECT count(*),max(rowid) FROM {table} WHERE {held}"
            ))?
            .query_row([], |r| Ok((r.get::<_, i64>(0)? as usize, r.get(1)?)))
        })
        .collect()
}

/// The read-back form of [`SELECTS`]: one row by rowid.
fn select_one(table: usize) -> &'static str {
    static ONE: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    &ONE.get_or_init(|| {
        SELECTS
            .iter()
            .map(|select| format!("{MAINTENANCE_READ} {select} WHERE rowid=?1"))
            .collect()
    })[table]
}

/// The fields of a route or provider row that the view's route predicate
/// compares: a route origin is hidden while its provider's row records the
/// same error at the same time with deadlines at least as late.
#[derive(Clone, Debug, PartialEq)]
struct WallFields {
    last_error: Value,
    observed: Value,
    cooldown: Value,
    quota: Value,
}

/// What a source row needs beyond its own view rows.
#[derive(Clone, Debug, PartialEq)]
enum Link {
    None,
    /// An active account fence, by id: a `credential-quota:` id classifies a
    /// credential origin as quota.
    AccountFence(String),
    /// A credential origin's quota-hold prefix, as the view computes it.
    Credential(String),
    Provider {
        key: String,
        quarantined: bool,
        wall: WallFields,
    },
    Route {
        key: String,
        provider: String,
        quarantined: bool,
        wall: WallFields,
    },
}

/// A probe authorization: a decision or admission that holds a claimed
/// origin's generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeAuthorization {
    pub owner: String,
    pub kind: String,
    pub origin_key: String,
    pub evidence: String,
    pub evidence_at: i64,
    pub claimed_at: i64,
    pub claim_generation: i64,
}

/// A pool whose success healed part of an account fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuccessPool {
    pub fence_id: String,
    pub pool_key_json: String,
    pub evidence_at: i64,
    pub healed_at: i64,
}

/// The raw columns of a source row that request readers return, decoded with
/// those readers' own row decoders.
#[derive(Clone, Debug, PartialEq)]
enum Raw {
    None,
    /// An active account fence with its pool key and recovery set, as JSON.
    AccountFence {
        row: crate::AccountFenceRow,
        pool_key_json: Option<String>,
        recovery_set_json: String,
    },
    Entitlement(striatum_core::route_state_store::CredentialEntitlementFence),
    /// An `account_route_fence` row of any wall class.
    RouteFence {
        row: crate::AccountRouteFence,
        wall_class: Option<String>,
        last_reported_at: Option<i64>,
    },
    NonQuotaRouteFence {
        row: crate::AccountRouteFence,
        wall_kind: String,
    },
    Size(crate::AccountSizeRefusal),
    Exclusion(crate::AccountExclusionRow),
    /// A `provider_cooldown_state` row as `get_provider_cooldown` reads it.
    Provider(crate::route_snapshot::ProviderCooldownRow),
    /// A `model_route_state` row as the route-state snapshot reads it.
    Route(crate::route_snapshot::RouteStateRow),
    /// A `pool_key_state` row as pool acquisition reads it. The times are
    /// kept as stored and read as integers when a pool reads them, so a row
    /// a pool cannot read fails that read, not the write that changed it.
    PoolKey {
        provider: String,
        credential_id: String,
        last_used_at: Value,
        walled_until: Value,
        cooldown_until: Value,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum Held {
    Source {
        origins: Vec<ViewOrigin>,
        link: Link,
        raw: Raw,
    },
    Clock(Block, Option<i64>),
    RouteClear(RouteClear),
    Authorization(ProbeAuthorization),
    SuccessPool(SuccessPool),
    Eligibility(crate::AccountEligibilityFact),
}

/// Decode one row of `SELECTS[table]`: its rowid, its primary key and what
/// the state holds for it, if anything.
fn decode(table: usize, r: &Row<'_>) -> rusqlite::Result<(i64, String, Option<Held>)> {
    let rowid: i64 = r.get(0)?;
    let key: String = r.get(1)?;
    let slot = |at: usize| -> rusqlite::Result<Option<ViewOrigin>> {
        Ok(if r.get::<_, Option<bool>>(at)?.unwrap_or(false) {
            Some(fence_rechecks::view_origin(r, at + 1)?)
        } else {
            None
        })
    };
    let wall = |at: usize| -> rusqlite::Result<WallFields> {
        Ok(WallFields {
            last_error: r.get(at)?,
            observed: r.get(at + 1)?,
            cooldown: r.get(at + 2)?,
            quota: r.get(at + 3)?,
        })
    };
    // The error is the first wall field; the class and the unscoped flag
    // follow the four wall fields.
    let quarantined = |at: usize| -> rusqlite::Result<bool> {
        let error: Option<String> = r.get(at)?;
        let class: Option<String> = r.get(at + 4)?;
        Ok(fence_rechecks::quarantined_row(
            r.get(at + 5)?,
            error.as_deref(),
            class.as_deref(),
        ))
    };
    let source = |origins: Vec<Option<ViewOrigin>>, link: Link, raw: Raw, always: bool| {
        let origins: Vec<ViewOrigin> = origins.into_iter().flatten().collect();
        (always || !origins.is_empty()).then_some(Held::Source { origins, link, raw })
    };
    use crate::account_selection::rows;
    let held = match table {
        ACCOUNT_FENCE_TABLE => source(
            vec![slot(2)?],
            Link::AccountFence(r.get(17)?),
            Raw::AccountFence {
                row: rows::fence_at(r, 18)?,
                pool_key_json: r.get(31)?,
                recovery_set_json: r.get(32)?,
            },
            false,
        ),
        CREDENTIAL_TABLE => source(
            vec![slot(2)?],
            Link::Credential(r.get(17)?),
            Raw::Entitlement(rows::entitlement_at(r, 18)?),
            false,
        ),
        // Every row is held, whatever its wall class: the latest-report
        // reader groups all of them.
        ROUTE_FENCE_TABLE => source(
            vec![slot(2)?],
            Link::None,
            Raw::RouteFence {
                row: rows::route_fence_at(r, 17)?,
                wall_class: r.get(23)?,
                last_reported_at: r.get(24)?,
            },
            true,
        ),
        NON_QUOTA_ROUTE_FENCE_TABLE => source(
            vec![slot(2)?],
            Link::None,
            Raw::NonQuotaRouteFence {
                row: rows::route_fence_at(r, 17)?,
                wall_kind: r.get(23)?,
            },
            false,
        ),
        SIZE_TABLE | NON_QUOTA_SIZE_TABLE => source(
            vec![slot(2)?],
            Link::None,
            Raw::Size(rows::size_refusal_at(r, 17)?),
            false,
        ),
        EXCLUSION_TABLE => source(
            vec![slot(2)?],
            Link::None,
            Raw::Exclusion(rows::exclusion_at(r, 17)?),
            false,
        ),
        10 => source(vec![slot(2)?], Link::None, Raw::None, false),
        // Every key row is held, walled or not: pool acquisition reads each
        // key's last use too.
        4 => source(
            vec![slot(2)?, slot(17)?],
            Link::None,
            Raw::PoolKey {
                provider: r.get(32)?,
                credential_id: r.get(33)?,
                last_used_at: r.get(34)?,
                walled_until: r.get(35)?,
                cooldown_until: r.get(36)?,
            },
            true,
        ),
        PROVIDER_TABLE => source(
            vec![slot(2)?],
            Link::Provider {
                key: r.get(17)?,
                quarantined: quarantined(18)?,
                wall: wall(18)?,
            },
            Raw::Provider(crate::route_snapshot::provider_row_at(r, 24)?),
            true,
        ),
        ROUTE_TABLE => source(
            vec![slot(2)?, slot(17)?],
            Link::Route {
                key: r.get(39)?,
                provider: r.get(32)?,
                quarantined: quarantined(33)?,
                wall: wall(33)?,
            },
            Raw::Route(crate::route_snapshot::route_row_at(r, 40)?),
            true,
        ),
        CLOCK_TABLE => {
            let (block, cleared) = fence_rechecks::clock_row(r, 2)?;
            Some(Held::Clock(block, cleared))
        }
        ROUTE_CLEAR_TABLE => Some(Held::RouteClear(RouteClear {
            kind: r.get(2)?,
            origin_key: r.get(3)?,
            evidence: r.get(4)?,
            model: r.get(5)?,
        })),
        AUTHORIZATION_TABLE => Some(Held::Authorization(ProbeAuthorization {
            owner: r.get(2)?,
            kind: r.get(3)?,
            origin_key: r.get(4)?,
            evidence: r.get(5)?,
            evidence_at: r.get(6)?,
            claimed_at: r.get(7)?,
            claim_generation: r.get(8)?,
        })),
        SUCCESS_POOL_TABLE => Some(Held::SuccessPool(SuccessPool {
            fence_id: r.get(2)?,
            pool_key_json: r.get(3)?,
            evidence_at: r.get(4)?,
            healed_at: r.get(5)?,
        })),
        ELIGIBILITY_TABLE => Some(Held::Eligibility(rows::eligibility_at(r, 2)?)),
        _ => unreachable!("block table index"),
    };
    Ok((rowid, key, held))
}

const SHARDS: usize = 64;

/// A map whose clone copies one pointer and whose first write after a clone
/// copies 64 shard pointers and one shard. A savepoint fork therefore copies
/// no rows, and a command copies only the shards it writes.
#[derive(Clone)]
struct Shards<K, V> {
    shards: Arc<Vec<Arc<HashMap<K, V>>>>,
    len: usize,
}

impl<K, V> Default for Shards<K, V> {
    fn default() -> Self {
        Self {
            shards: Arc::new(vec![Arc::new(HashMap::new()); SHARDS]),
            len: 0,
        }
    }
}

fn shard_of<K: Hash>(key: &K) -> usize {
    // `DefaultHasher::new` uses fixed keys: two maps built independently
    // shard a key alike.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % SHARDS as u64) as usize
}

impl<K: Eq + Hash + Clone, V: Clone> Shards<K, V> {
    fn get(&self, key: &K) -> Option<&V> {
        self.shards[shard_of(key)].get(key)
    }

    fn insert(&mut self, key: K, value: V) {
        let shard = shard_of(&key);
        if Arc::make_mut(&mut Arc::make_mut(&mut self.shards)[shard])
            .insert(key, value)
            .is_none()
        {
            self.len += 1;
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let shard = shard_of(key);
        if !self.shards[shard].contains_key(key) {
            return None;
        }
        self.len -= 1;
        Arc::make_mut(&mut Arc::make_mut(&mut self.shards)[shard]).remove(key)
    }

    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.shards.iter().flat_map(|shard| shard.iter())
    }
}

impl<K: Eq + Hash + Clone, V: Clone + PartialEq> PartialEq for Shards<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

/// One table's held rows by rowid, the rowid of each primary key, and the
/// rowids of each scope ([`scope_of`]) so a decision check can find the
/// rows it compares without visiting the rest.
#[derive(Clone, Default, PartialEq)]
struct Table {
    /// This table's position in [`BLOCK_TABLES`], which decides its scopes.
    table: usize,
    rows: Shards<i64, (String, Arc<Held>)>,
    keys: Shards<String, i64>,
    scopes: Shards<String, Shards<i64, ()>>,
}

/// The scope a decision check looks a held row up by, if its table has one:
/// the provider of a table in [`PROVIDER_COLUMNS`] (as [`row_provider`]
/// names it), an exclusion's principal and admission ([`exclusion_scope`]),
/// a route clear's clock ([`clear_scope`]), and a success pool's fence.
fn scope_of(table: usize, key: &str, held: &Held) -> Option<String> {
    if PROVIDER_COLUMNS
        .iter()
        .any(|(provider_table, _)| *provider_table == table)
    {
        return row_provider(table, key, held);
    }
    match (table, held) {
        (EXCLUSION_TABLE, _) => {
            let parts: Vec<serde_json::Value> = serde_json::from_str(key).ok()?;
            Some(exclusion_scope(
                parts.first()?.as_str()?,
                parts.get(1)?.as_str()?,
            ))
        }
        (ROUTE_CLEAR_TABLE, Held::RouteClear(clear)) => {
            Some(clear_scope(&clear.kind, &clear.origin_key))
        }
        (SUCCESS_POOL_TABLE, Held::SuccessPool(pool)) => Some(pool.fence_id.clone()),
        _ => None,
    }
}

/// The scope of one admission's principal exclusions.
fn exclusion_scope(principal: &str, admission_id: &str) -> String {
    serde_json::json!([principal, admission_id]).to_string()
}

/// The scope of one clock's route clears.
fn clear_scope(kind: &str, origin_key: &str) -> String {
    serde_json::json!([kind, origin_key]).to_string()
}

impl Table {
    fn new(table: usize) -> Self {
        Self {
            table,
            ..Self::default()
        }
    }

    /// The held rows of one scope, with their rowids, in no particular order.
    fn scope_rows(&self, scope: &str) -> impl Iterator<Item = (i64, &Held)> {
        self.scopes
            .get(&scope.to_owned())
            .into_iter()
            .flat_map(|rowids| rowids.iter())
            .filter_map(|(rowid, ())| self.rows.get(rowid).map(|(_, held)| (*rowid, &**held)))
    }

    fn remove_rowid(&mut self, rowid: i64) {
        if let Some((key, held)) = self.rows.remove(&rowid) {
            if self.keys.get(&key) == Some(&rowid) {
                self.keys.remove(&key);
            }
            if let Some(scope) = scope_of(self.table, &key, &held) {
                if let Some(mut rowids) = self.scopes.get(&scope).cloned() {
                    rowids.remove(&rowid);
                    if rowids.len == 0 {
                        self.scopes.remove(&scope);
                    } else {
                        self.scopes.insert(scope, rowids);
                    }
                }
            }
        }
    }

    fn put(&mut self, rowid: i64, key: &str, held: Option<&Arc<Held>>) {
        // A REPLACE conflict resolution deletes the row it conflicts with without
        // telling the update hook. The new row's key finds it.
        if let Some(other) = self.keys.get(&key.to_owned()).copied() {
            if other != rowid {
                self.remove_rowid(other);
            }
        }
        // An UPDATE can change the primary key while keeping the rowid.
        self.remove_rowid(rowid);
        if let Some(held) = held {
            self.rows.insert(rowid, (key.to_owned(), Arc::clone(held)));
            self.keys.insert(key.to_owned(), rowid);
            if let Some(scope) = scope_of(self.table, key, held) {
                let mut rowids = self.scopes.get(&scope).cloned().unwrap_or_default();
                rowids.insert(rowid, ());
                self.scopes.insert(scope, rowids);
            }
        }
    }
}

/// The block tables' rows as SQLite currently holds them, minus cleared
/// account fences and rows no view branch selects.
#[derive(Clone, Default, PartialEq)]
pub struct BlockState {
    /// False on a schema without probe clocks, where there are no blocks.
    enabled: bool,
    tables: Vec<Table>,
}

// Every float held comes from SQLite, which stores no NaN.
impl Eq for BlockState {}

impl std::fmt::Debug for BlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (name, table) in BLOCK_TABLES.iter().zip(&self.tables) {
            let rows: std::collections::BTreeMap<_, _> = table.rows.iter().collect();
            map.entry(name, &rows);
        }
        map.finish()
    }
}

/// The changed block rows of one command, read back by rowid.
#[derive(Clone, Debug, Default)]
pub struct BlockRows(Vec<Change>);

#[derive(Clone, Debug)]
enum Change {
    Gone {
        table: u8,
        rowid: i64,
    },
    Row {
        table: u8,
        rowid: i64,
        key: String,
        held: Option<Arc<Held>>,
    },
}

impl BlockRows {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Read the current projection of every captured row, in the order first
/// captured. A rowid with no row any more was deleted.
pub(crate) fn read_back(conn: &Connection, captured: &[(u8, i64)]) -> rusqlite::Result<BlockRows> {
    let mut seen = std::collections::HashSet::new();
    let mut changes = Vec::new();
    for &(table, rowid) in captured {
        if !seen.insert((table, rowid)) {
            continue;
        }
        READ_BACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let row = conn
            .prepare_cached(select_one(table as usize))?
            .query_row([rowid], |r| decode(table as usize, r))
            .optional()?;
        changes.push(match row {
            None => Change::Gone { table, rowid },
            Some((rowid, key, held)) => Change::Row {
                table,
                rowid,
                key,
                held: held.map(Arc::new),
            },
        });
    }
    Ok(BlockRows(changes))
}

/// SQLite's ordering of two values without affinity: NULL compares as
/// unknown, numbers before text before blobs, text by bytes.
fn sql_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b),
        (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (Value::Blob(a), Value::Blob(b)) => Some(a.cmp(b)),
        _ => Some(rank(a).cmp(&rank(b))).filter(|order| *order != Ordering::Equal),
    }
}

impl WallFields {
    /// The view's `NOT EXISTS` for a route origin: the provider row records
    /// the same error (`IS`), at the same time, with each deadline at least
    /// as late.
    fn covers(&self, route: &Self) -> bool {
        use std::cmp::Ordering::{Equal, Greater};
        self.same_error(route)
            && sql_cmp(&self.observed, &route.observed) == Some(Equal)
            && matches!(
                sql_cmp(&self.cooldown, &route.cooldown),
                Some(Equal | Greater)
            )
            && matches!(sql_cmp(&self.quota, &route.quota), Some(Equal | Greater))
    }

    /// `route.last_error IS self.last_error`.
    fn same_error(&self, route: &Self) -> bool {
        match (&self.last_error, &route.last_error) {
            (Value::Null, Value::Null) => true,
            (a, b) => sql_cmp(a, b) == Some(std::cmp::Ordering::Equal),
        }
    }
}

impl BlockState {
    /// Read every block table in the caller's transaction.
    pub(crate) fn load(conn: &Connection) -> rusqlite::Result<Self> {
        if !fence_rechecks::enabled(conn)? {
            return Ok(Self::default());
        }
        let mut state = Self {
            enabled: true,
            tables: (0..BLOCK_TABLES.len()).map(Table::new).collect(),
        };
        for (index, select) in SELECTS.iter().enumerate() {
            let mut statement = conn.prepare(&format!("{MAINTENANCE_READ} {select}"))?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let (rowid, key, held) = decode(index, row)?;
                if let Some(held) = held {
                    state.tables[index].put(rowid, &key, Some(&Arc::new(held)));
                }
            }
        }
        Ok(state)
    }

    /// Apply rows read back after a command. A state without the clock
    /// schema holds nothing.
    pub fn apply(&mut self, rows: &BlockRows) {
        if !self.enabled {
            return;
        }
        for change in &rows.0 {
            match change {
                Change::Gone { table, rowid } => self.tables[*table as usize].remove_rowid(*rowid),
                Change::Row {
                    table,
                    rowid,
                    key,
                    held,
                } => self.tables[*table as usize].put(*rowid, key, held.as_ref()),
            }
        }
    }

    /// Rows held per table, for size reports.
    pub fn table_sizes(&self) -> Vec<(&'static str, usize)> {
        BLOCK_TABLES
            .iter()
            .zip(&self.tables)
            .map(|(name, table)| (*name, table.rows.len))
            .collect()
    }

    /// Each table's held-row count and highest held rowid, as
    /// [`sql_fingerprints`] reads them from SQL.
    pub(crate) fn fingerprints(&self) -> Vec<(usize, Option<i64>)> {
        self.tables
            .iter()
            .map(|table| {
                (
                    table.rows.len,
                    table.rows.iter().map(|(rowid, _)| *rowid).max(),
                )
            })
            .collect()
    }

    /// Whether another state shares every table's storage, so a fork copied
    /// no rows.
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        self.tables.len() == other.tables.len()
            && self.tables.iter().zip(&other.tables).all(|(a, b)| {
                Arc::ptr_eq(&a.rows.shards, &b.rows.shards)
                    && Arc::ptr_eq(&a.keys.shards, &b.keys.shards)
                    && Arc::ptr_eq(&a.scopes.shards, &b.scopes.shards)
            })
    }

    /// Each source row with its table and rowid.
    fn sources(&self) -> impl Iterator<Item = (usize, i64, &Vec<ViewOrigin>, &Link, &Raw)> {
        self.tables
            .iter()
            .take(CLOCK_TABLE)
            .enumerate()
            .flat_map(|(table, rows)| {
                rows.rows
                    .iter()
                    .filter_map(move |(rowid, (_, held))| match &**held {
                        Held::Source { origins, link, raw } => {
                            Some((table, *rowid, origins, link, raw))
                        }
                        _ => None,
                    })
            })
    }

    /// One table's held rows with their rowids, in no particular order.
    fn rows_of(&self, table: usize) -> impl Iterator<Item = (i64, &Held)> {
        self.tables
            .get(table)
            .into_iter()
            .flat_map(|rows| rows.rows.iter().map(|(rowid, (_, held))| (*rowid, &**held)))
    }

    /// One source table's raw rows with their rowids, in no particular order.
    fn raws(&self, table: usize) -> impl Iterator<Item = (i64, &Raw)> {
        self.rows_of(table).filter_map(|(rowid, held)| match held {
            Held::Source { raw, .. } => Some((rowid, raw)),
            _ => None,
        })
    }

    /// The rows of the `fence_origin` view, each with the position SQLite
    /// returns it at: its view branch, then its table rowid.
    fn view_rows(&self) -> Vec<((u8, i64), ViewOrigin)> {
        let fences: std::collections::BTreeSet<&str> = self
            .sources()
            .filter_map(|(_, _, _, link, _)| match link {
                Link::AccountFence(id) => Some(id.as_str()),
                _ => None,
            })
            .collect();
        let providers: HashMap<&str, &WallFields> = self
            .sources()
            .filter_map(|(_, _, _, link, _)| match link {
                Link::Provider { key, wall, .. } => Some((key.as_str(), wall)),
                _ => None,
            })
            .collect();
        let mut view = Vec::new();
        for (_, rowid, origins, link, _) in self.sources() {
            for origin in origins {
                let mut origin = origin.clone();
                match link {
                    Link::Credential(prefix) => {
                        origin.quota = Some(
                            fences
                                .range(prefix.as_str()..)
                                .next()
                                .is_some_and(|id| id.starts_with(prefix.as_str())),
                        );
                    }
                    Link::Route { provider, wall, .. }
                        if origin.kind == "route"
                            && providers
                                .get(provider.as_str())
                                .is_some_and(|covering| covering.covers(wall)) =>
                    {
                        continue;
                    }
                    _ => {}
                }
                view.push(((view_branch(&origin.kind), rowid), origin));
            }
        }
        view
    }

    /// The rows of the `fence_origin` view, sorted by kind and origin key.
    pub fn view_origins(&self) -> Vec<ViewOrigin> {
        let mut view: Vec<ViewOrigin> = self.view_rows().into_iter().map(|(_, row)| row).collect();
        view.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        view
    }

    /// The rows of the `fence_origin` view in the order a full scan of the
    /// view returns them: branch by branch, each table in rowid order.
    pub(crate) fn view_origins_in_scan_order(&self) -> Vec<ViewOrigin> {
        let mut view = self.view_rows();
        view.sort_by_key(|(position, _)| *position);
        view.into_iter().map(|(_, row)| row).collect()
    }

    /// The route and provider origins whose rows hold one account's error,
    /// keyed as `fence_rechecks::quarantined_origins` keys them.
    pub(crate) fn quarantined(&self) -> Vec<(String, String)> {
        self.sources()
            .filter_map(|(_, _, _, link, _)| match link {
                Link::Provider {
                    key,
                    quarantined: true,
                    ..
                } => Some(("provider", key)),
                Link::Route {
                    key,
                    quarantined: true,
                    ..
                } => Some(("route", key)),
                _ => None,
            })
            .map(|(kind, key)| (kind.to_owned(), serde_json::json!([key]).to_string()))
            .collect()
    }

    /// The stored clocks with their route clears attached, the clears in
    /// model order (as `load_blocks` reads them) or, with `rowid_order`, in
    /// table order (as the join in `fence_rechecks::read` returns them).
    fn clocks(&self, rowid_order: bool) -> Clocks {
        let mut clocks = Clocks::new();
        for (_, held) in self.rows_of(CLOCK_TABLE) {
            if let Held::Clock(block, cleared) = held {
                clocks.insert(
                    (block.kind.clone(), block.origin_key.clone()),
                    (block.clone(), *cleared),
                );
            }
        }
        let mut clears: Vec<(i64, RouteClear)> = self
            .rows_of(ROUTE_CLEAR_TABLE)
            .filter_map(|(rowid, held)| match held {
                Held::RouteClear(clear) => Some((rowid, clear.clone())),
                _ => None,
            })
            .collect();
        if rowid_order {
            clears.sort_by_key(|(rowid, _)| *rowid);
        } else {
            clears.sort_by(|a, b| a.1.model.cmp(&b.1.model));
        }
        let clears: Vec<RouteClear> = clears.into_iter().map(|(_, clear)| clear).collect();
        fence_rechecks::attach_route_clears(&mut clocks, &clears);
        clocks
    }

    /// The effective blocks at `now`, as `SqliteRouteStateStore::load_blocks`
    /// computes them from SQL. Blocks are sorted by kind and origin key.
    pub fn blocks(&self, now: i64) -> striatum_core::RouteStateResult<LoadedBlocks> {
        if !self.enabled {
            return Ok(LoadedBlocks::default());
        }
        let mut loaded = fence_rechecks::derive_blocks(
            self.clocks(false),
            &self.quarantined(),
            self.view_origins(),
            now,
        )
        .map_err(crate::err)?;
        loaded
            .cleared_evidence
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        loaded
            .blocks
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        Ok(loaded)
    }

    /// The answer of `SqliteRouteStateStore::fence_rechecks` at `now`, in its
    /// order: view rows in scan order, then the observation clocks whose
    /// source is gone, by kind and key. `derive_blocks` keeps that order.
    pub fn fence_rechecks(&self, now: i64) -> striatum_core::RouteStateResult<Vec<FenceRecheck>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let loaded = fence_rechecks::derive_blocks(
            self.clocks(true),
            &self.quarantined(),
            self.view_origins_in_scan_order(),
            now,
        )
        .map_err(crate::err)?;
        Ok(loaded.blocks.iter().map(FenceRecheck::from).collect())
    }

    /// Check, by primary key, that SQL holds the source row of origin
    /// `origin_key` of `kind` exactly as this state does: the same row, or
    /// none, or a row the state does not hold. A decision about the origin
    /// (archive it, install or move its clock, drop its clock) runs this
    /// first, and fails with [`diverged`] when SQL disagrees: these writes
    /// are keyed on the origin, not on the source values the state read.
    /// A route origin's presence also depends on its provider's row, and a
    /// credential origin's class on the account fences named after it; both
    /// are checked too.
    pub(crate) fn check_source(
        &self,
        conn: &Connection,
        kind: &str,
        origin_key: &str,
        provider: &str,
    ) -> rusqlite::Result<()> {
        let Some((table, extra)) = source_of(kind) else {
            return Ok(());
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(origin_key)
            .map_err(|error| diverged(format_args!("{kind} origin key {origin_key}: {error}")))?;
        let mut values: Vec<Value> = parsed
            .into_iter()
            .map(|value| match value {
                serde_json::Value::String(text) => Value::Text(text),
                serde_json::Value::Number(number) => number
                    .as_i64()
                    .map(Value::Integer)
                    .unwrap_or_else(|| Value::Real(number.as_f64().unwrap_or_default())),
                serde_json::Value::Null => Value::Null,
                other => Value::Text(other.to_string()),
            })
            .collect();
        values.extend(extra.map(|wall_kind| Value::Text(wall_kind.into())));
        let columns = SOURCE_KEYS[table];
        if values.len() != columns.len() {
            return Err(diverged(format_args!(
                "{kind} origin key {origin_key} does not name a {} row",
                BLOCK_TABLES[table]
            )));
        }
        let in_sql = conn
            .prepare_cached(&source_check_sql(table))?
            .query_row(rusqlite::params_from_iter(&values), |r| decode(table, r))
            .optional()?;
        let key: String = conn.query_row(
            &format!(
                "SELECT json_array({})",
                (1..=values.len())
                    .map(|at| format!("?{at}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            rusqlite::params_from_iter(&values),
            |r| r.get(0),
        )?;
        let held = &self.tables[table];
        let in_state = held
            .keys
            .get(&key)
            .and_then(|rowid| held.rows.get(rowid).map(|(_, row)| (*rowid, &**row)));
        let same = match (&in_sql, in_state) {
            (None | Some((_, _, None)), None) => true,
            (Some((rowid, _, Some(row))), Some((held_rowid, held_row))) => {
                *rowid == held_rowid && row == held_row
            }
            _ => false,
        };
        if !same {
            return Err(diverged(format_args!(
                "the {} row of {kind} origin {origin_key} differs from the state's",
                BLOCK_TABLES[table]
            )));
        }
        match kind {
            "route" => self.check_source(
                conn,
                "provider",
                &serde_json::json!([provider]).to_string(),
                provider,
            ),
            "credential" => {
                // The view's quota-hold prefix: the origin key is the same
                // `json_array(provider_id,credential_id)` text.
                let prefix = format!("credential-quota:{origin_key}:");
                // The prefix ends with ':'; ';' is the next character.
                let end = format!("{};", &prefix[..prefix.len() - 1]);
                let in_sql = conn
                    .prepare_cached(&quota_hold_check_sql())?
                    .query_map([&prefix, &end], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut in_state: Vec<String> = self
                    .sources()
                    .filter_map(|(_, _, _, link, _)| match link {
                        Link::AccountFence(id) if id.starts_with(&prefix) => Some(id.clone()),
                        _ => None,
                    })
                    .collect();
                in_state.sort();
                if in_sql != in_state {
                    return Err(diverged(format_args!(
                        "the quota holds of credential origin {origin_key} differ from the state's"
                    )));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Check that SQL holds every block row of `provider` exactly as this
    /// state does: each source table's rows (whatever view branch they feed,
    /// and the pool key rows a pool reads), the probe clocks, and the route
    /// clears of those clocks. A decision about `provider`'s pools (a lease
    /// granted, reused or rehydrated, a lease released or refused because
    /// its key is blocked) runs this first, because none of its writes name
    /// the rows that decided it: the key states, the clocks that apply to
    /// the route (a clear removes a clock from it), and sync's finding that
    /// no further source row needs a clock. A difference raises
    /// [`diverged`].
    ///
    /// Every statement is keyed by `provider`: each table's primary key or
    /// an index leads with its provider column (`account_fence_by_account`,
    /// `model_route_state(provider_id)`, `fence_recheck_scope`), and the
    /// clocks' clears by the clear table's primary key. The state side finds
    /// its rows through the table's scope index, so the check visits only
    /// `provider`'s rows.
    ///
    /// The empty provider names the principal clocks: the probe clocks of
    /// principal exclusions (a caller excluding one of its own credentials),
    /// whose provider the view projects as `''`. Every other source row
    /// carries the provider it was recorded for. So for it only the clocks
    /// and their clears are compared; the other tables and the success pools
    /// hold no row of the empty provider
    /// (`only_principal_rows_have_the_empty_provider`).
    pub(crate) fn check_provider_rows(
        &self,
        conn: &Connection,
        provider: &str,
    ) -> rusqlite::Result<()> {
        let tables: &[(usize, &str)] = if provider.is_empty() {
            &[(CLOCK_TABLE, "provider")]
        } else {
            &PROVIDER_COLUMNS
        };
        for (table, column) in tables {
            self.compare_rows(
                conn,
                *table,
                &provider_rows_check_sql(*table, column),
                [provider],
                self.tables[*table].scope_rows(provider),
                format_args!("the {} rows of provider {provider}", BLOCK_TABLES[*table]),
            )?;
        }
        // The clocks agree by now, so their clears are those of the
        // provider's clocks in SQL too.
        let clears = self.tables[CLOCK_TABLE]
            .scope_rows(provider)
            .filter_map(|(_, held)| match held {
                Held::Clock(block, _) => Some(clear_scope(&block.kind, &block.origin_key)),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.compare_rows(
            conn,
            ROUTE_CLEAR_TABLE,
            &route_clears_check_sql(),
            [provider],
            clears
                .iter()
                .flat_map(|scope| self.tables[ROUTE_CLEAR_TABLE].scope_rows(scope)),
            format_args!("the route clears of provider {provider}'s clocks"),
        )?;
        if provider.is_empty() {
            return Ok(());
        }
        // The pools a success healed of the provider's active account
        // fences, which decide which of a fence's pools still block. The
        // fences themselves agree by now.
        let fences = self.tables[ACCOUNT_FENCE_TABLE]
            .scope_rows(provider)
            .filter_map(|(_, held)| match held {
                Held::Source {
                    raw: Raw::AccountFence { row, .. },
                    ..
                } => Some(row.fence_id.as_str()),
                _ => None,
            });
        self.compare_rows(
            conn,
            SUCCESS_POOL_TABLE,
            &success_pools_check_sql(),
            [provider],
            fences.flat_map(|fence| self.tables[SUCCESS_POOL_TABLE].scope_rows(fence)),
            format_args!("the success pools of provider {provider}"),
        )
    }

    /// Compare the rows `statement` (bound to `params`) returns from SQL with
    /// `held`, the rows of `table` this state holds for it, by rowid. Neither
    /// side's rows are cloned.
    fn compare_rows<'a>(
        &self,
        conn: &Connection,
        table: usize,
        statement: &str,
        params: impl rusqlite::Params,
        held: impl Iterator<Item = (i64, &'a Held)>,
        what: std::fmt::Arguments<'_>,
    ) -> rusqlite::Result<()> {
        let mut in_sql: Vec<(i64, Held)> = Vec::new();
        let mut prepared = conn.prepare_cached(statement)?;
        let mut rows = prepared.query(params)?;
        while let Some(row) = rows.next()? {
            if let (rowid, _, Some(held)) = decode(table, row)? {
                in_sql.push((rowid, held));
            }
        }
        in_sql.sort_by_key(|(rowid, _)| *rowid);
        let mut in_state: Vec<(i64, &Held)> = held.collect();
        in_state.sort_by_key(|(rowid, _)| *rowid);
        let same = in_sql.len() == in_state.len()
            && in_sql
                .iter()
                .zip(&in_state)
                .all(|((sql_rowid, sql), (rowid, held))| sql_rowid == rowid && sql == *held);
        if !same {
            return Err(diverged(format_args!("{what} differ from the state's")));
        }
        Ok(())
    }

    /// Check that SQL holds the principal exclusions of one admission as this
    /// state does (by the table's primary-key prefix). An admission that
    /// skips excluded credentials decided on these.
    pub(crate) fn check_exclusions(
        &self,
        conn: &Connection,
        principal: &str,
        admission_id: &str,
    ) -> rusqlite::Result<()> {
        self.compare_rows(
            conn,
            EXCLUSION_TABLE,
            &exclusions_check_sql(),
            [principal, admission_id],
            self.tables[EXCLUSION_TABLE].scope_rows(&exclusion_scope(principal, admission_id)),
            format_args!("the exclusions of admission {principal} {admission_id}"),
        )
    }

    /// Check, by primary key, that SQL holds the probe clock of one origin
    /// as this state does, or holds none where the state holds none.
    pub(crate) fn check_clock(
        &self,
        conn: &Connection,
        kind: &str,
        origin_key: &str,
    ) -> rusqlite::Result<()> {
        let in_sql = conn
            .prepare_cached(&by_origin_check_sql(CLOCK_TABLE))?
            .query_row([kind, origin_key], |r| decode(CLOCK_TABLE, r))
            .optional()?
            .and_then(|(_, _, held)| held);
        let in_state = self.clock(kind, origin_key).map(|(block, cleared)| {
            let mut block = block.clone();
            block.cleared_models.clear();
            Held::Clock(block, cleared)
        });
        if in_sql != in_state {
            return Err(diverged(format_args!(
                "the clock of {kind} origin {origin_key} differs from the state's"
            )));
        }
        Ok(())
    }

    /// The route-state snapshot at `now`, as `SqliteRouteStateStore::build_snapshot`
    /// builds it from SQL: every route row in table order, its deadlines
    /// scheduled by the effective probe clocks evaluated at `recheck_now`
    /// (the wall clock when the SQL reader runs, which a request's `now`
    /// need not equal).
    pub fn route_state_snapshot(
        &self,
        now: f64,
        recheck_now: i64,
    ) -> striatum_core::RouteStateResult<striatum_core::model_router::ModelRouteStateSnapshot> {
        let rechecks = self.fence_rechecks(recheck_now)?;
        let mut rows: Vec<(i64, &crate::route_snapshot::RouteStateRow)> = self
            .raws(ROUTE_TABLE)
            .filter_map(|(rowid, raw)| match raw {
                Raw::Route(row) => Some((rowid, row)),
                _ => None,
            })
            .collect();
        // The capacity entry takes the first live wall it meets, so the rows
        // go in the order SQL returns them.
        rows.sort_by_key(|(rowid, _)| *rowid);
        let rows: Vec<_> = rows.into_iter().map(|(_, row)| row).collect();
        crate::route_snapshot::overlay(&rows, &rechecks, self.enabled, now)
    }

    /// One provider's cooldown row, as `SqliteRouteStateStore::get_provider_cooldown`
    /// reads it from SQL.
    pub fn provider_cooldown(
        &self,
        provider_key: &str,
    ) -> striatum_core::RouteStateResult<Option<striatum_core::ProviderCooldownState>> {
        let key = crate::route_snapshot::provider_lookup_key(provider_key)?;
        let row = self.raws(PROVIDER_TABLE).find_map(|(_, raw)| match raw {
            Raw::Provider(row) if row.provider_key() == key => Some(row),
            _ => None,
        });
        crate::route_snapshot::provider_cooldown(row)
    }

    /// The stored clocks that are not cleared, each with its route clears,
    /// as `fence_rechecks::read` returns them: by kind and origin key.
    pub fn clock_rechecks(&self) -> Vec<FenceRecheck> {
        if !self.enabled {
            return Vec::new();
        }
        let mut clocks: Vec<FenceRecheck> = self
            .clocks(true)
            .into_values()
            .filter(|(_, cleared)| cleared.is_none())
            .map(|(block, _)| FenceRecheck::from(&block))
            .collect();
        clocks.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        clocks
    }

    /// Every `pool_key_state` row of `provider`: its credential, last use,
    /// and wall and cooldown deadlines, as `key_pool` reads them from SQL.
    pub fn pool_key_rows(
        &self,
        provider: &str,
    ) -> rusqlite::Result<Vec<(String, Option<i64>, Option<i64>, Option<i64>)>> {
        fn time(value: &Value, column: usize) -> rusqlite::Result<Option<i64>> {
            match value {
                Value::Null => Ok(None),
                Value::Integer(at) => Ok(Some(*at)),
                other => Err(rusqlite::Error::InvalidColumnType(
                    column,
                    "pool_key_state time".into(),
                    other.data_type(),
                )),
            }
        }
        self.raws(4)
            .filter_map(|(_, raw)| match raw {
                Raw::PoolKey {
                    provider: owner,
                    credential_id,
                    last_used_at,
                    walled_until,
                    cooldown_until,
                } if owner == provider => {
                    Some((credential_id, last_used_at, walled_until, cooldown_until))
                }
                _ => None,
            })
            .map(|(credential, last_used, walled, cooldown)| {
                Ok((
                    credential.clone(),
                    time(last_used, 1)?,
                    time(walled, 2)?,
                    time(cooldown, 3)?,
                ))
            })
            .collect()
    }

    /// What `fence_rechecks::needs_sync` answers on SQL: whether a sync
    /// would change any clock. A quarantined row holds a clock, a view
    /// origin's clock is missing or holds other evidence, or a clock other
    /// than an observation's has no origin.
    pub fn needs_sync(&self) -> bool {
        if !self.enabled {
            return false;
        }
        let quarantined: std::collections::HashSet<(String, String)> =
            self.quarantined().into_iter().collect();
        let mut clocks: HashMap<(String, String), String> = self
            .stored_clocks()
            .map(|(block, _)| {
                (
                    (block.kind.clone(), block.origin_key.clone()),
                    block.evidence.clone(),
                )
            })
            .collect();
        if quarantined.iter().any(|key| clocks.contains_key(key)) {
            return true;
        }
        for origin in self.view_origins_in_scan_order() {
            let key = (origin.kind, origin.origin_key);
            if quarantined.contains(&key) {
                continue;
            }
            if clocks.remove(&key).as_deref() != Some(origin.evidence.as_str()) {
                return true;
            }
        }
        clocks.keys().any(|(kind, _)| kind != "observation")
    }

    /// Whether the store has the block schema. Without it the state holds
    /// nothing and readers must ask SQL, which knows the older tables.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// `account_eligibility()`: by provider, credential and model.
    pub fn eligibility(&self) -> Vec<crate::AccountEligibilityFact> {
        let mut facts: Vec<_> = self
            .rows_of(ELIGIBILITY_TABLE)
            .filter_map(|(_, held)| match held {
                Held::Eligibility(fact) => Some(fact.clone()),
                _ => None,
            })
            .collect();
        facts.sort_by(|a, b| {
            (&a.provider, &a.credential_id, &a.model).cmp(&(
                &b.provider,
                &b.credential_id,
                &b.model,
            ))
        });
        facts
    }

    /// `account_size_refusals()`: quota size refusals, then non-quota ones,
    /// each table in rowid order, as SQL's `UNION ALL` scans them.
    pub fn size_refusals(&self) -> Vec<crate::AccountSizeRefusal> {
        let mut refusals = Vec::new();
        for table in [SIZE_TABLE, NON_QUOTA_SIZE_TABLE] {
            let mut rows: Vec<_> = self
                .raws(table)
                .filter_map(|(rowid, raw)| match raw {
                    Raw::Size(row) => Some((rowid, row)),
                    _ => None,
                })
                .collect();
            rows.sort_by_key(|(rowid, _)| *rowid);
            refusals.extend(rows.into_iter().map(|(_, row)| row.clone()));
        }
        refusals
    }

    /// The route fences of one class, by provider, credential and model:
    /// `quota` reads `account_route_fence`, `transient` and `billing` read
    /// `account_non_quota_route_fence` rows of that kind.
    pub fn route_fences(&self, class: &str) -> Vec<crate::AccountRouteFence> {
        let mut fences: Vec<crate::AccountRouteFence> = if class == "quota" {
            self.raws(ROUTE_FENCE_TABLE)
                .filter_map(|(_, raw)| match raw {
                    Raw::RouteFence {
                        row, wall_class, ..
                    } if wall_class.as_deref() == Some("quota") => Some(row.clone()),
                    _ => None,
                })
                .collect()
        } else {
            self.raws(NON_QUOTA_ROUTE_FENCE_TABLE)
                .filter_map(|(_, raw)| match raw {
                    Raw::NonQuotaRouteFence { row, wall_kind } if wall_kind == class => {
                        Some(row.clone())
                    }
                    _ => None,
                })
                .collect()
        };
        fences.sort_by(|a, b| {
            (&a.provider, &a.credential_id, &a.model).cmp(&(
                &b.provider,
                &b.credential_id,
                &b.model,
            ))
        });
        fences
    }

    /// `account_route_fence_latest_reports()`: the latest report per
    /// credential over every `account_route_fence` row, of any class.
    pub fn route_fence_latest_reports(&self) -> std::collections::BTreeMap<String, i64> {
        let mut latest = std::collections::BTreeMap::new();
        for (_, raw) in self.raws(ROUTE_FENCE_TABLE) {
            if let Raw::RouteFence {
                row,
                last_reported_at,
                ..
            } = raw
            {
                let at = last_reported_at.unwrap_or(row.recorded_at);
                latest
                    .entry(row.credential_id.clone())
                    .and_modify(|latest: &mut i64| *latest = (*latest).max(at))
                    .or_insert(at);
            }
        }
        latest
    }

    fn entitlements(
        &self,
    ) -> impl Iterator<Item = &striatum_core::route_state_store::CredentialEntitlementFence> {
        self.raws(CREDENTIAL_TABLE)
            .filter_map(|(_, raw)| match raw {
                Raw::Entitlement(row) => Some(row),
                _ => None,
            })
    }

    /// `credential_entitlement_fences()`: by provider and credential.
    pub fn entitlement_fences(
        &self,
    ) -> Vec<striatum_core::route_state_store::CredentialEntitlementFence> {
        let mut fences: Vec<_> = self.entitlements().cloned().collect();
        fences.sort_by(|a, b| {
            (&a.provider_id, &a.credential_id).cmp(&(&b.provider_id, &b.credential_id))
        });
        fences
    }

    /// `credential_entitlement_fence(provider, credential)`.
    pub fn entitlement_fence(
        &self,
        provider: &str,
        credential: &str,
    ) -> Option<striatum_core::route_state_store::CredentialEntitlementFence> {
        self.entitlements()
            .find(|fence| fence.provider_id == provider && fence.credential_id == credential)
            .cloned()
    }

    fn active_fence_rows(
        &self,
        provider: &str,
    ) -> Vec<(&crate::AccountFenceRow, &Option<String>, &String)> {
        let mut rows: Vec<_> = self
            .raws(ACCOUNT_FENCE_TABLE)
            .filter_map(|(_, raw)| match raw {
                Raw::AccountFence {
                    row,
                    pool_key_json,
                    recovery_set_json,
                } if row.provider == provider => Some((row, pool_key_json, recovery_set_json)),
                _ => None,
            })
            .collect();
        rows.sort_by(|a, b| (a.0.created_at, &a.0.fence_id).cmp(&(b.0.created_at, &b.0.fence_id)));
        rows
    }

    /// `active_fences(provider)`: by creation time and id. Only active
    /// fences are held.
    pub fn active_fences(&self, provider: &str) -> Vec<crate::AccountFenceRow> {
        self.active_fence_rows(provider)
            .into_iter()
            .map(|(row, _, _)| row.clone())
            .collect()
    }

    /// `active_pool_fences(provider)`: each active fence's pool key and the
    /// part of its recovery set no success has healed for this evidence.
    pub fn active_pool_fences(
        &self,
        provider: &str,
    ) -> striatum_core::RouteStateResult<Vec<crate::AccountPoolFenceRow>> {
        use striatum_core::accounts::PoolKey;
        // The healed pools of each fence's evidence, gathered in one pass.
        let mut healed_by_fence: HashMap<(&str, i64), Vec<&str>> = HashMap::new();
        for pool in self.success_pools() {
            healed_by_fence
                .entry((pool.fence_id.as_str(), pool.evidence_at))
                .or_default()
                .push(pool.pool_key_json.as_str());
        }
        self.active_fence_rows(provider)
            .into_iter()
            .map(|(row, pool_key_json, recovery_set_json)| {
                let evidence_at = row.observed_at.min(row.created_at);
                let healed = healed_by_fence
                    .get(&(row.fence_id.as_str(), evidence_at))
                    .into_iter()
                    .flatten()
                    .map(|pool| serde_json::from_str::<PoolKey>(pool))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::err)?;
                let mut recovery_set: Vec<PoolKey> =
                    serde_json::from_str(recovery_set_json).map_err(crate::err)?;
                recovery_set.retain(|pool| !healed.contains(pool));
                Ok(crate::AccountPoolFenceRow {
                    evidence: row.clone(),
                    pool_key: pool_key_json
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(crate::err)?,
                    recovery_set,
                })
            })
            .collect()
    }

    /// `active_fence_providers()`: every provider with an active fence,
    /// sorted.
    pub fn active_fence_providers(&self) -> Vec<String> {
        self.raws(ACCOUNT_FENCE_TABLE)
            .filter_map(|(_, raw)| match raw {
                Raw::AccountFence { row, .. } => Some(row.provider.clone()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// `credential_quota_classification`: the stored class of a classified
    /// credential clock whose evidence the source still records, or the
    /// proposed class and whether it is new.
    pub fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> (bool, bool) {
        if !self.enabled {
            return (proposed, false);
        }
        let sources: std::collections::HashSet<(&str, &str)> = self
            .sources()
            .filter(|(table, ..)| *table == CREDENTIAL_TABLE)
            .flat_map(|(_, _, origins, _, _)| origins)
            .map(|origin| (origin.origin_key.as_str(), origin.evidence.as_str()))
            .collect();
        let prior = self.rows_of(CLOCK_TABLE).find_map(|(_, held)| match held {
            Held::Clock(block, _)
                if block.kind == "credential"
                    && block.classified
                    && block.scope.provider == provider
                    && block.scope.credential_id.as_deref() == Some(credential)
                    && sources.contains(&(block.origin_key.as_str(), block.evidence.as_str())) =>
            {
                Some(block.lifetime == striatum_core::blocks::LifetimeClass::Quota)
            }
            _ => None,
        });
        (prior.unwrap_or(proposed), prior.is_none())
    }

    /// `admission_exclusions(principal, admission)`: by credential.
    pub fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> Vec<crate::AccountExclusionRow> {
        let mut rows: Vec<_> = self
            .raws(EXCLUSION_TABLE)
            .filter_map(|(_, raw)| match raw {
                Raw::Exclusion(row)
                    if row.principal == principal && row.admission_id == admission_id =>
                {
                    Some(row.clone())
                }
                _ => None,
            })
            .collect();
        rows.sort_by(|a, b| a.credential_id.cmp(&b.credential_id));
        rows
    }

    /// `exclusion_count(principal, credential)`.
    pub fn exclusion_count(&self, principal: &str, credential_id: &str) -> i64 {
        self.raws(EXCLUSION_TABLE)
            .filter(|(_, raw)| {
                matches!(raw, Raw::Exclusion(row)
                    if row.principal == principal && row.credential_id == credential_id)
            })
            .count() as i64
    }

    /// `probe_authorization_owners()`: distinct, sorted.
    pub fn authorization_owners(&self) -> Vec<String> {
        self.authorizations()
            .map(|row| row.owner.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Probe authorizations, in no particular order.
    pub fn authorizations(&self) -> impl Iterator<Item = &ProbeAuthorization> {
        self.rows_of(AUTHORIZATION_TABLE)
            .filter_map(|(_, held)| match held {
                Held::Authorization(row) => Some(row),
                _ => None,
            })
    }

    /// One owner's probe authorizations, ordered by kind and origin key.
    pub(crate) fn authorizations_of(&self, owner: &str) -> Vec<ProbeAuthorization> {
        let mut rows: Vec<ProbeAuthorization> = self
            .authorizations()
            .filter(|row| row.owner == owner)
            .cloned()
            .collect();
        rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        rows
    }

    /// What releasing `owner`'s probe gives up, as `release_admission_probes`
    /// decides it: each authorization `owner` holds, with whether its clock
    /// is still the one it claimed (uncleared, same evidence, evidence time
    /// and claim generation). Only such a clock is reopened.
    pub fn probe_release(&self, owner: &str) -> Vec<(ProbeAuthorization, bool)> {
        let clocks = self.live_clocks();
        self.authorizations_of(owner)
            .into_iter()
            .map(|row| {
                let current = clocks
                    .get(&(row.kind.as_str(), row.origin_key.as_str()))
                    .is_some_and(|clock| {
                        clock.evidence == row.evidence
                            && clock.evidence_at == row.evidence_at
                            && clock.claim_generation == row.claim_generation
                    });
                (row, current)
            })
            .collect()
    }

    /// Apply in memory exactly what `release_admission_probes` writes for
    /// `owner` at `now`, from the same [`Self::probe_release`] decision:
    /// each current clock is due at `now`, unclaimed, one generation on, and
    /// every authorization of `owner` is gone. A reader sees an expired
    /// admission's probe released before any command releases it.
    pub fn apply_probe_release(&mut self, owner: &str, now: i64) {
        for (row, current) in self.probe_release(owner) {
            if !current {
                continue;
            }
            let found = self.tables[CLOCK_TABLE]
                .rows
                .iter()
                .find_map(|(rowid, (key, held))| match &**held {
                    Held::Clock(block, None)
                        if block.kind == row.kind && block.origin_key == row.origin_key =>
                    {
                        Some((*rowid, key.clone(), block.clone()))
                    }
                    _ => None,
                });
            if let Some((rowid, key, mut block)) = found {
                block.next_recheck_at = now;
                block.last_claimed_at = None;
                block.claim_owner = None;
                block.claim_generation += 1;
                self.tables[CLOCK_TABLE].put(
                    rowid,
                    &key,
                    Some(&Arc::new(Held::Clock(block, None))),
                );
            }
        }
        let held: Vec<(i64, String)> = self.tables[AUTHORIZATION_TABLE]
            .rows
            .iter()
            .filter_map(|(rowid, (key, held))| match &**held {
                Held::Authorization(row) if row.owner == owner => Some((*rowid, key.clone())),
                _ => None,
            })
            .collect();
        for (rowid, key) in held {
            self.tables[AUTHORIZATION_TABLE].put(rowid, &key, None);
        }
    }

    /// Every stored probe clock that is not cleared, by kind and origin key.
    pub(crate) fn live_clocks(&self) -> HashMap<(&str, &str), &Block> {
        self.rows_of(CLOCK_TABLE)
            .filter_map(|(_, held)| match held {
                Held::Clock(block, None) => {
                    Some(((block.kind.as_str(), block.origin_key.as_str()), block))
                }
                _ => None,
            })
            .collect()
    }

    /// Every stored probe clock whose claim owner is `owner`, with its
    /// `cleared_at`.
    pub(crate) fn clocks_claimed_by(&self, owner: &str) -> Vec<(&Block, Option<i64>)> {
        self.rows_of(CLOCK_TABLE)
            .filter_map(|(_, held)| match held {
                Held::Clock(block, cleared) if block.claim_owner.as_deref() == Some(owner) => {
                    Some((block, *cleared))
                }
                _ => None,
            })
            .collect()
    }

    /// Success pools, in no particular order.
    pub fn success_pools(&self) -> impl Iterator<Item = &SuccessPool> {
        self.rows_of(SUCCESS_POOL_TABLE)
            .filter_map(|(_, held)| match held {
                Held::SuccessPool(row) => Some(row),
                _ => None,
            })
    }

    /// The stored probe clock of one origin, cleared or not, with its
    /// `cleared_at`.
    pub(crate) fn clock(&self, kind: &str, origin_key: &str) -> Option<(&Block, Option<i64>)> {
        self.stored_clocks()
            .find(|(block, _)| block.kind == kind && block.origin_key == origin_key)
    }

    /// Every stored probe clock, cleared or not, with its `cleared_at`, in no
    /// particular order.
    pub(crate) fn stored_clocks(&self) -> impl Iterator<Item = (&Block, Option<i64>)> {
        self.rows_of(CLOCK_TABLE)
            .filter_map(|(_, held)| match held {
                Held::Clock(block, cleared) => Some((block, *cleared)),
                _ => None,
            })
    }

    /// One active account fence, with its pool key and recovery set as JSON.
    pub(crate) fn account_fence(
        &self,
        fence_id: &str,
    ) -> Option<(&crate::AccountFenceRow, Option<&str>, &str)> {
        self.raws(ACCOUNT_FENCE_TABLE)
            .find_map(|(_, raw)| match raw {
                Raw::AccountFence {
                    row,
                    pool_key_json,
                    recovery_set_json,
                } if row.fence_id == fence_id => {
                    Some((row, pool_key_json.as_deref(), recovery_set_json.as_str()))
                }
                _ => None,
            })
    }

    /// The `account_route_fence` row of one credential's model, of any wall
    /// class, with its class and its latest report time.
    pub(crate) fn account_route_fence(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
    ) -> Option<(&crate::AccountRouteFence, Option<&str>, Option<i64>)> {
        self.raws(ROUTE_FENCE_TABLE).find_map(|(_, raw)| match raw {
            Raw::RouteFence {
                row,
                wall_class,
                last_reported_at,
            } if row.provider == provider
                && row.credential_id == credential
                && row.model == model =>
            {
                Some((row, wall_class.as_deref(), *last_reported_at))
            }
            _ => None,
        })
    }

    /// The `account_non_quota_route_fence` row of one credential's model and
    /// wall kind, with its latest report time (the evidence time of its
    /// origin).
    pub(crate) fn non_quota_route_fence(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
        wall_kind: &str,
    ) -> Option<(&crate::AccountRouteFence, i64)> {
        self.rows_of(NON_QUOTA_ROUTE_FENCE_TABLE)
            .find_map(|(_, held)| match held {
                Held::Source {
                    origins,
                    raw:
                        Raw::NonQuotaRouteFence {
                            row,
                            wall_kind: kind,
                        },
                    ..
                } if row.provider == provider
                    && row.credential_id == credential
                    && row.model == model
                    && kind == wall_kind =>
                {
                    origins.first().map(|origin| (row, origin.evidence_at))
                }
                _ => None,
            })
    }

    /// One size refusal, from the quota table or the non-quota one.
    pub(crate) fn size_refusal(
        &self,
        quota: bool,
        provider: &str,
        credential: &str,
        model: &str,
        size_class: i64,
    ) -> Option<&crate::AccountSizeRefusal> {
        let table = if quota {
            SIZE_TABLE
        } else {
            NON_QUOTA_SIZE_TABLE
        };
        self.raws(table).find_map(|(_, raw)| match raw {
            Raw::Size(row)
                if row.provider == provider
                    && row.credential_id == credential
                    && row.model == model
                    && i64::from(row.size_class) == size_class =>
            {
                Some(row)
            }
            _ => None,
        })
    }

    /// The origin of `kind` and key that its source row selects, before the
    /// view's cross-row predicates: a route origin hidden by its provider's
    /// row is still returned. `None` when the row is gone or no longer
    /// selects that origin (for a wall, when its deadlines are all NULL).
    pub(crate) fn source_origin(&self, kind: &str, origin_key: &str) -> Option<&ViewOrigin> {
        self.sources()
            .flat_map(|(_, _, origins, _, _)| origins)
            .find(|origin| origin.kind == kind && origin.origin_key == origin_key)
    }

    /// The route rows a success on provider evidence observed at
    /// `evidence_at` recovers with it: rows of that provider stamped by the
    /// same evidence (the same error, observed at that time, before
    /// `admitted_at`, with deadlines no later than the provider row's) that
    /// still hold a cooldown or quota deadline. Each comes with its route
    /// origin's evidence and the two wall fields a keyed update names.
    pub(crate) fn routes_stamped_by_provider(
        &self,
        provider: &str,
        evidence_at: i64,
        admitted_at: i64,
    ) -> Vec<(String, String, Value, Value)> {
        use std::cmp::Ordering::{Equal, Less};
        let Some(covering) = self.sources().find_map(|(_, _, _, link, _)| match link {
            Link::Provider { key, wall, .. } if key == provider => Some(wall),
            _ => None,
        }) else {
            return Vec::new();
        };
        self.sources()
            .filter_map(|(_, _, origins, link, _)| match link {
                Link::Route {
                    key,
                    provider: owner,
                    wall,
                    ..
                } if owner == provider
                    && covering.same_error(wall)
                    && sql_cmp(&wall.observed, &Value::Integer(evidence_at)) == Some(Equal)
                    && sql_cmp(&wall.observed, &Value::Integer(admitted_at)) == Some(Less)
                    && matches!(
                        sql_cmp(&wall.cooldown, &covering.cooldown),
                        Some(Less | Equal)
                    )
                    && matches!(sql_cmp(&wall.quota, &covering.quota), Some(Less | Equal)) =>
                {
                    origins
                        .iter()
                        .find(|origin| origin.kind == "route")
                        .map(|origin| {
                            (
                                key.clone(),
                                origin.evidence.clone(),
                                wall.last_error.clone(),
                                wall.observed.clone(),
                            )
                        })
                }
                _ => None,
            })
            .collect()
    }
}

/// The position of a kind's branch in the `fence_origin` view's `UNION ALL`.
fn view_branch(kind: &str) -> u8 {
    match kind {
        "account" => 0,
        "credential" => 1,
        "account_route" => 2,
        "transient_route" | "non_quota_route" => 3,
        "pool_billing" => 4,
        "pool_rate" => 5,
        "provider" => 6,
        "route" => 7,
        "failure" => 8,
        "size" => 9,
        "non_quota_size" => 10,
        "principal" => 11,
        _ => 12,
    }
}

impl crate::AccountTx<'_> {
    /// Compare the working state with SQL cheaply, table by table: the count
    /// of rows the state holds and the highest of their rowids. A difference
    /// means some write reached SQL without the capture seeing it; it is
    /// counted, and the command fails with [`diverged`], so the writer
    /// reloads the state and runs the batch again. A command without a
    /// state has nothing to compare.
    pub fn reconcile_block_state(&self) -> striatum_core::RouteStateResult<()> {
        let Some(blocks) = self.working_blocks()? else {
            return Ok(());
        };
        let sql = sql_fingerprints(self.conn).map_err(crate::err)?;
        for ((table, memory), stored) in BLOCK_TABLES.iter().zip(blocks.fingerprints()).zip(sql) {
            if memory != stored {
                RECONCILE_RELOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    table,
                    ?memory,
                    sql = ?stored,
                    "block state differs from SQL; reloading it"
                );
                return Err(crate::err(diverged(format_args!(
                    "{table}: state holds {memory:?} (rows, highest rowid), SQL {stored:?}"
                ))));
            }
        }
        // Counts and rowids cannot see a row changed in place. Every few
        // calls (about once a minute at the tick's period), compare a full
        // load, about a millisecond at live size.
        let call = RECONCILE_CALLS.with(|calls| {
            let call = calls.get();
            calls.set(call + 1);
            call
        });
        if call % FULL_RECONCILE_EVERY == FULL_RECONCILE_EVERY - 1 {
            let loaded = BlockState::load(self.conn).map_err(crate::err)?;
            if loaded != *blocks {
                RECONCILE_RELOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!("block state differs from a full load of SQL; reloading it");
                return Err(crate::err(diverged("block state differs from a full load")));
            }
        }
        Ok(())
    }

    /// Check that SQL holds every block row of each of `providers` as the
    /// working state does (`BlockState::check_provider_rows`), raising
    /// `STATE_DIVERGED` on a difference. A probe clock (a `fence_recheck`
    /// row) schedules the single recovery test of one block. A decision that
    /// chose which probe
    /// clocks apply from the state (a route clear removes one; an account
    /// fence decides whether its clock applies; sync's finding that no
    /// other source row needs a clock) runs this before it claims or
    /// admits, because neither the claim nor the admission names the rows
    /// that left a clock out. The empty provider names the principal
    /// clocks (see `check_provider_rows`). Without a working state SQL
    /// decided, and there is nothing to
    /// check.
    pub fn check_decision_inputs(&self, providers: &[&str]) -> striatum_core::RouteStateResult<()> {
        let Some(state) = self.working_blocks()? else {
            return Ok(());
        };
        for provider in providers {
            state
                .check_provider_rows(self.conn, provider)
                .map_err(crate::err)?;
        }
        Ok(())
    }

    /// Check that SQL holds one admission's principal exclusions as the
    /// working state does, raising `STATE_DIVERGED` on a difference: an
    /// admission decides which credentials it may use on them.
    pub fn check_admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> striatum_core::RouteStateResult<()> {
        let Some(state) = self.working_blocks()? else {
            return Ok(());
        };
        state
            .check_exclusions(self.conn, principal, admission_id)
            .map_err(crate::err)
    }

    /// The block tables, read in the state loader's transaction.
    pub fn load_block_state(&self) -> striatum_core::RouteStateResult<BlockState> {
        BlockState::load(self.conn).map_err(crate::err)
    }

    /// The block rows the running command has changed so far, read back so
    /// the command can lay them over its working state and read its own
    /// writes. The capture keeps them: the writer reads every captured row
    /// back again when the command ends, so a row changed again later is
    /// not missed.
    pub fn pending_block_rows(&self) -> striatum_core::RouteStateResult<BlockRows> {
        let captured = CAPTURE.with(|capture| capture.borrow().clone().unwrap_or_default());
        if captured.is_empty() {
            return Ok(BlockRows::default());
        }
        read_back(self.conn, &captured).map_err(crate::err)
    }

    /// The block tables this command decides on: the writer's working state
    /// with the rows this command has changed so far laid over it, so a
    /// decision sees the command's own earlier writes. `None` outside the
    /// account writer or without a loaded state, where callers read SQL.
    ///
    /// The overlay is kept between calls and extended with the rows
    /// captured since, each read back by rowid. A row changed again is
    /// captured again, so the copy never keeps an older value of it.
    pub fn working_blocks(
        &self,
    ) -> striatum_core::RouteStateResult<Option<std::borrow::Cow<'_, BlockState>>> {
        use std::borrow::Cow;
        let Some(base) = self.blocks.filter(|blocks| blocks.enabled()) else {
            return Ok(None);
        };
        let mut overlay = self.overlay.borrow_mut();
        let done = overlay.as_ref().map_or(0, |(applied, _)| *applied);
        let new: Vec<(u8, i64)> = CAPTURE.with(|capture| {
            capture
                .borrow()
                .as_ref()
                .and_then(|rows| rows.get(done..))
                .map(<[_]>::to_vec)
                .unwrap_or_default()
        });
        if done == 0 && new.is_empty() {
            return Ok(Some(Cow::Borrowed(base)));
        }
        let (applied, blocks) = overlay.get_or_insert_with(|| (0, base.clone()));
        if !new.is_empty() {
            blocks.apply(&read_back(self.conn, &new).map_err(crate::err)?);
            *applied += new.len();
        }
        Ok(Some(Cow::Owned(blocks.clone())))
    }
}

/// The error a claim or probe write returns when SQL did not hold the row
/// the working state decided on. The writer rolls the batch back, reloads
/// the state and runs the batch again (`account_writer::STATE_DIVERGED`).
pub(crate) fn diverged(what: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(format!("{}: {what}", crate::STATE_DIVERGED).into())
}

/// Fail with [`diverged`] unless a keyed write changed exactly `expected` rows.
pub(crate) fn expect_changes(
    changed: usize,
    expected: usize,
    what: impl std::fmt::Display,
) -> rusqlite::Result<()> {
    if changed == expected {
        Ok(())
    } else {
        Err(diverged(format_args!(
            "{what}: expected {expected} changed rows, SQL changed {changed}"
        )))
    }
}

#[cfg(test)]
mod tests;
