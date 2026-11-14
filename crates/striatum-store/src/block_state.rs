//! In-memory copy of the block tables: the eleven `fence_origin` source
//! tables, the probe clocks, route clears, probe authorizations and success
//! pools.
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
//! (evidence signatures and keys come from SQLite's own JSON functions). Two
//! view predicates span rows and are applied when blocks are derived: a
//! `credential` origin's class depends on active `credential-quota:` account
//! fences, and a `route` origin is hidden by a provider row that records the
//! same wall. [`BlockState::blocks`] then merges sources and clocks exactly as
//! the SQL adapter does, through `fence_rechecks::derive_blocks`.
//!
//! Cleared account fences and retirement records are never held: the view
//! ignores both, and nothing derived from them is needed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row};
use striatum_core::blocks::Block;

use crate::fence_rechecks::{self, Clocks, LoadedBlocks, RouteClear, ViewOrigin};

/// Every table whose rows the block state holds. The writer connection's
/// update hook and delete authorizer watch exactly these.
pub const BLOCK_TABLES: [&str; 15] = [
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
];

const PROVIDER_TABLE: usize = 5;
const ROUTE_TABLE: usize = 6;
const CLOCK_TABLE: usize = 11;
const ROUTE_CLEAR_TABLE: usize = 12;

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
}

/// Called by the writer connection's update hook. Returns false when no
/// account command is running on this thread, which means the write bypassed
/// the account writer.
pub(crate) fn capture(table: u8, rowid: i64) -> bool {
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
/// columns the cross-row predicates need. Each select starts with the rowid
/// and the table's primary key, as JSON, which lets a row replaced by
/// `INSERT OR REPLACE` (whose delete the update hook does not report) be
/// found by key. None has a WHERE clause: the read-back appends one.
const SELECTS: [&str; 15] = [
    // account_fence: only active fences are in the view.
    "SELECT rowid,json_array(fence_id),
        cleared_at IS NULL,'account',json_array(fence_id),provider,account_id,NULL,NULL,NULL,NULL,NULL,
        fence_json,MIN(observed_at,created_at),1,NULL,json_extract(fence_json,'$.reset.at_ms'),
        fence_id
     FROM account_fence",
    // credential_entitlement_fences: the class is derived later from the
    // quota-hold prefix in the last column.
    "SELECT rowid,json_array(provider_id,credential_id),
        1,'credential',json_array(provider_id,credential_id),provider_id,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(code,observed_at_ms),observed_at_ms,NULL,NULL,NULL,
        'credential-quota:'||json_array(provider_id,credential_id)||':'
     FROM credential_entitlement_fences",
    "SELECT rowid,json_array(provider,credential_id,model),
        wall_class='quota','account_route',json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
        json_array(wall_class,until_ms,COALESCE(last_reported_at,recorded_at)),COALESCE(last_reported_at,recorded_at),
        wall_class='quota',until_ms,CASE WHEN wall_class='quota' THEN until_ms END
     FROM account_route_fence",
    "SELECT rowid,json_array(provider,credential_id,model,wall_kind),
        1,CASE WHEN wall_kind='transient' THEN 'transient_route' ELSE 'non_quota_route' END,
        json_array(provider,credential_id,model),provider,NULL,credential_id,model,NULL,NULL,NULL,
        json_array(until_ms,last_reported_at),last_reported_at,0,until_ms,NULL
     FROM account_non_quota_route_fence",
    "SELECT rowid,json_array(provider,credential_id),
        walled_until IS NOT NULL,'pool_billing',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(walled_until,wall_reason,COALESCE(wall_observed_at,updated_at)),COALESCE(wall_observed_at,updated_at),0,walled_until,NULL,
        cooldown_until IS NOT NULL,'pool_rate',json_array(provider,credential_id),provider,NULL,credential_id,NULL,NULL,NULL,NULL,
        json_array(cooldown_until,cooldown_reason,COALESCE(cooldown_observed_at,updated_at)),COALESCE(cooldown_observed_at,updated_at),0,cooldown_until,NULL
     FROM pool_key_state",
    // provider_cooldown_state: every row is held, because a route origin
    // is compared with its provider's row whether or not that row walls.
    "SELECT rowid,json_array(provider_key),
        cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL,'provider',json_array(provider_key),provider_key,NULL,NULL,NULL,NULL,NULL,NULL,
        json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
        0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL,
        provider_key,last_error,COALESCE(wall_observed_at,last_error_at,updated_at),
        COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0),wall_class,wall_unscoped
     FROM provider_cooldown_state",
    "SELECT rowid,json_array(route_key),
        cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL,'route',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
        json_array(cooldown_until,quota_exhausted_until,wall_observed_at),COALESCE(wall_observed_at,last_error_at,updated_at),
        0,MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),NULL,
        failure_backoff_until IS NOT NULL,'failure',json_array(route_key),provider_id,NULL,NULL,model_id,NULL,NULL,NULL,
        json_array(failure_backoff_until,failure_backoff_reason,failure_observed_at),COALESCE(failure_observed_at,last_error_at,updated_at),
        0,failure_backoff_until,NULL,
        provider_id,last_error,COALESCE(wall_observed_at,last_error_at,updated_at),
        COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0),wall_class,wall_unscoped,route_key
     FROM model_route_state",
    "SELECT rowid,json_array(provider,credential_id,model,size_class),
        1,'size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
        json_array(observed_at,expires_at),observed_at,1,expires_at,expires_at
     FROM account_size_refusal",
    "SELECT rowid,json_array(provider,credential_id,model,size_class),
        1,'non_quota_size',json_array(provider,credential_id,model,size_class),provider,NULL,credential_id,model,size_class,NULL,NULL,
        json_array(observed_at,expires_at),observed_at,0,expires_at,NULL
     FROM account_non_quota_size_refusal",
    "SELECT rowid,json_array(principal,admission_id,credential_id),
        1,'principal',json_array(principal,admission_id,credential_id),'',NULL,credential_id,NULL,NULL,principal,admission_id,
        json_array(recorded_at,runner_view_digest,roster_view_digest),recorded_at,0,recorded_at+86400000,NULL
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
];

/// The read-back form of [`SELECTS`]: one row by rowid.
fn select_one(table: usize) -> &'static str {
    static ONE: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    &ONE.get_or_init(|| {
        SELECTS
            .iter()
            .map(|select| format!("{select} WHERE rowid=?1"))
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

#[derive(Clone, Debug, PartialEq)]
enum Held {
    Source {
        origins: Vec<ViewOrigin>,
        link: Link,
    },
    Clock(Block, Option<i64>),
    RouteClear(RouteClear),
    Authorization(ProbeAuthorization),
    SuccessPool(SuccessPool),
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
    let source = |origins: Vec<Option<ViewOrigin>>, link: Link, always: bool| {
        let origins: Vec<ViewOrigin> = origins.into_iter().flatten().collect();
        (always || !origins.is_empty()).then_some(Held::Source { origins, link })
    };
    let held = match table {
        0 => source(vec![slot(2)?], Link::AccountFence(r.get(17)?), false),
        1 => source(vec![slot(2)?], Link::Credential(r.get(17)?), false),
        2 | 3 | 7 | 8 | 9 | 10 => source(vec![slot(2)?], Link::None, false),
        4 => source(vec![slot(2)?, slot(17)?], Link::None, false),
        PROVIDER_TABLE => source(
            vec![slot(2)?],
            Link::Provider {
                key: r.get(17)?,
                quarantined: quarantined(18)?,
                wall: wall(18)?,
            },
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
            false,
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
        13 => Some(Held::Authorization(ProbeAuthorization {
            owner: r.get(2)?,
            kind: r.get(3)?,
            origin_key: r.get(4)?,
            evidence: r.get(5)?,
            evidence_at: r.get(6)?,
            claimed_at: r.get(7)?,
            claim_generation: r.get(8)?,
        })),
        14 => Some(Held::SuccessPool(SuccessPool {
            fence_id: r.get(2)?,
            pool_key_json: r.get(3)?,
            evidence_at: r.get(4)?,
            healed_at: r.get(5)?,
        })),
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

/// One table's held rows by rowid, and the rowid of each primary key.
#[derive(Clone, Default, PartialEq)]
struct Table {
    rows: Shards<i64, (String, Arc<Held>)>,
    keys: Shards<String, i64>,
}

impl Table {
    fn remove_rowid(&mut self, rowid: i64) {
        if let Some((key, _)) = self.rows.remove(&rowid) {
            if self.keys.get(&key) == Some(&rowid) {
                self.keys.remove(&key);
            }
        }
    }

    fn put(&mut self, rowid: i64, key: &str, held: Option<&Arc<Held>>) {
        // `INSERT OR REPLACE` deletes the row it conflicts with without
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
        }
    }

    fn held(&self) -> impl Iterator<Item = &Held> {
        self.rows.iter().map(|(_, (_, held))| &**held)
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
        let same_error = match (&self.last_error, &route.last_error) {
            (Value::Null, Value::Null) => true,
            (a, b) => sql_cmp(a, b) == Some(Equal),
        };
        same_error
            && sql_cmp(&self.observed, &route.observed) == Some(Equal)
            && matches!(
                sql_cmp(&self.cooldown, &route.cooldown),
                Some(Equal | Greater)
            )
            && matches!(sql_cmp(&self.quota, &route.quota), Some(Equal | Greater))
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
            tables: vec![Table::default(); BLOCK_TABLES.len()],
        };
        for (index, select) in SELECTS.iter().enumerate() {
            let mut statement = conn.prepare(select)?;
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

    /// Whether another state shares every table's storage, so a fork copied
    /// no rows.
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        self.tables.len() == other.tables.len()
            && self.tables.iter().zip(&other.tables).all(|(a, b)| {
                Arc::ptr_eq(&a.rows.shards, &b.rows.shards)
                    && Arc::ptr_eq(&a.keys.shards, &b.keys.shards)
            })
    }

    fn sources(&self) -> impl Iterator<Item = (&Vec<ViewOrigin>, &Link)> {
        self.tables[..CLOCK_TABLE]
            .iter()
            .flat_map(Table::held)
            .filter_map(|held| match held {
                Held::Source { origins, link } => Some((origins, link)),
                _ => None,
            })
    }

    /// The rows of the `fence_origin` view, sorted by kind and origin key.
    pub fn view_origins(&self) -> Vec<ViewOrigin> {
        let fences: std::collections::BTreeSet<&str> = self
            .sources()
            .filter_map(|(_, link)| match link {
                Link::AccountFence(id) => Some(id.as_str()),
                _ => None,
            })
            .collect();
        let providers: HashMap<&str, &WallFields> = self
            .sources()
            .filter_map(|(_, link)| match link {
                Link::Provider { key, wall, .. } => Some((key.as_str(), wall)),
                _ => None,
            })
            .collect();
        let mut view = Vec::new();
        for (origins, link) in self.sources() {
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
                view.push(origin);
            }
        }
        view.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        view
    }

    /// The route and provider origins whose rows hold one account's error,
    /// keyed as `fence_rechecks::quarantined_origins` keys them.
    fn quarantined(&self) -> Vec<(String, String)> {
        self.sources()
            .filter_map(|(_, link)| match link {
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

    /// The effective blocks at `now`, as `SqliteRouteStateStore::load_blocks`
    /// computes them from SQL. Blocks are sorted by kind and origin key.
    pub fn blocks(&self, now: i64) -> striatum_core::RouteStateResult<LoadedBlocks> {
        if !self.enabled {
            return Ok(LoadedBlocks::default());
        }
        let mut clocks = Clocks::new();
        for held in self.tables[CLOCK_TABLE].held() {
            if let Held::Clock(block, cleared) = held {
                clocks.insert(
                    (block.kind.clone(), block.origin_key.clone()),
                    (block.clone(), *cleared),
                );
            }
        }
        let mut clears: Vec<RouteClear> = self.tables[ROUTE_CLEAR_TABLE]
            .held()
            .filter_map(|held| match held {
                Held::RouteClear(clear) => Some(clear.clone()),
                _ => None,
            })
            .collect();
        clears.sort_by(|a, b| a.model.cmp(&b.model));
        fence_rechecks::attach_route_clears(&mut clocks, &clears);
        let mut loaded =
            fence_rechecks::derive_blocks(clocks, &self.quarantined(), self.view_origins(), now)
                .map_err(crate::err)?;
        loaded
            .cleared_evidence
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        loaded
            .blocks
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        Ok(loaded)
    }

    /// Probe authorizations, in no particular order.
    pub fn authorizations(&self) -> impl Iterator<Item = &ProbeAuthorization> {
        self.tables[13].held().filter_map(|held| match held {
            Held::Authorization(row) => Some(row),
            _ => None,
        })
    }

    /// Success pools, in no particular order.
    pub fn success_pools(&self) -> impl Iterator<Item = &SuccessPool> {
        self.tables[14].held().filter_map(|held| match held {
            Held::SuccessPool(row) => Some(row),
            _ => None,
        })
    }
}

impl crate::AccountTx<'_> {
    /// The block tables, read in the state loader's transaction.
    pub fn load_block_state(&self) -> striatum_core::RouteStateResult<BlockState> {
        BlockState::load(self.conn).map_err(crate::err)
    }
}

#[cfg(test)]
mod tests;
