//! Captured durable block rows and their sharded request-path state.
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
mod consolidated;
pub(crate) use consolidated::read_back;
pub use consolidated::{BlockRows, BlockState, DryRunRepair};
#[cfg(any(test, feature = "test-support"))]
#[path = "block_state/legacy/tests.rs"]
pub(crate) mod legacy;
#[cfg(test)]
pub(crate) mod tests;
pub const BLOCK_TABLES: [&str; 8] = [
    "block",
    "probe_authorization",
    "success_proof",
    "retirement_tombstone",
    "pool_key_state",
    "provider_cooldown_state",
    "model_route_state",
    "account_eligibility",
];
pub(crate) const MAINTENANCE_READ: &str = "/* block state maintenance */";
pub(crate) const STATE_CHECK: &str = "/* block state check */";

pub(crate) fn sql_fingerprints(conn: &Connection) -> rusqlite::Result<Vec<(usize, Option<i64>)>> {
    BLOCK_TABLES
        .iter()
        .enumerate()
        .map(|(table, name)| {
            conn.prepare_cached(&fingerprint_sql(table, name))?
                .query_row([], |row| Ok((row.get::<_, i64>(0)? as usize, row.get(1)?)))
        })
        .collect()
}

pub(crate) fn fingerprint_sql(table: usize, name: &str) -> String {
    let held = match table {
        0 => crate::block_storage::HELD_SQL,
        3 => "0",
        _ => "1",
    };
    format!("{MAINTENANCE_READ} SELECT count(*),max(rowid) FROM {name} WHERE {held}")
}

#[cfg(any(test, feature = "test-support"))]
pub fn reviewed_exempt_reads() -> Vec<String> {
    let mut reads = consolidated::reviewed_exempt_reads();
    reads.extend([
        crate::fence_rechecks::authorization_check_sql(),
        crate::fence_rechecks::probe_clock_check_sql(),
        crate::fence_rechecks::no_authorized_clock_check_sql(false),
        crate::fence_rechecks::no_authorized_clock_check_sql(true),
        crate::fence_rechecks::unstarted_admission_check_sql(),
    ]);
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

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn in_writer_command() -> bool {
    CAPTURE.with(|capture| capture.borrow().is_some())
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

impl<K: Eq + Hash + Clone, V: Clone + Eq> Eq for Shards<K, V> {}

impl<K: Eq + Hash + Clone, V: Clone + PartialEq> PartialEq for Shards<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
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
    pub(crate) fn store_source(
        &self,
        row: &crate::block_storage::StoredBlock,
    ) -> striatum_core::RouteStateResult<usize> {
        self.current_blocks()?
            .check_source(
                self.conn,
                &row.block.kind,
                &row.block.origin_key,
                &row.block.scope.provider,
            )
            .map_err(crate::err)?;
        crate::block_storage::write_source(self.conn, row).map_err(crate::err)
    }
    pub(crate) fn current_blocks(
        &self,
    ) -> striatum_core::RouteStateResult<std::borrow::Cow<'_, BlockState>> {
        match self.working_blocks()? {
            Some(state) => Ok(state),
            None => BlockState::read(self.conn)
                .map(std::borrow::Cow::Owned)
                .map_err(crate::err),
        }
    }
    /// Compare the working state with SQL cheaply, table by table: the count
    /// of rows the state holds and the highest of their rowids. A difference
    /// means some write reached SQL without the capture seeing it; it is
    /// counted, and the command fails with [`diverged`], so the writer
    /// reloads the state and runs the batch again. A command without a
    /// state has nothing to compare.
    pub fn reconcile_block_state(&self) -> striatum_core::RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::reconcile_block_state");
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
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::check_decision_inputs");
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
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::check_admission_exclusions");
        let Some(state) = self.working_blocks()? else {
            return Ok(());
        };
        state
            .check_exclusions(self.conn, principal, admission_id)
            .map_err(crate::err)
    }

    /// The block tables, read in the state loader's transaction.
    pub fn load_block_state(&self) -> striatum_core::RouteStateResult<BlockState> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::load_block_state");
        BlockState::load(self.conn).map_err(crate::err)
    }

    /// The block rows the running command has changed so far, read back so
    /// the command can lay them over its working state and read its own
    /// writes. The capture keeps them: the writer reads every captured row
    /// back again when the command ends, so a row changed again later is
    /// not missed.
    pub fn pending_block_rows(&self) -> striatum_core::RouteStateResult<BlockRows> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::pending_block_rows");
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
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::block_state::working_blocks");
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
