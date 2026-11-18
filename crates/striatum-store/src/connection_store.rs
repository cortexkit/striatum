//! Connection ownership, capture hooks and cumulative writer timing.
//! Test-only SQL copies remain an independent reference for checking repairs.

use super::*;
use std::sync::Arc;

#[cfg(any(test, feature = "test-support"))]
const ACCOUNT_COPY_BLOCK_TABLES: [&str; 8] = [
    "block",
    "probe_authorization",
    "success_proof",
    "retirement_tombstone",
    "pool_key_state",
    "provider_cooldown_state",
    "model_route_state",
    "account_eligibility",
];

// Durable stores keep the shared library's lease and pooled reads unchanged.
// A projection needs neither a lease file nor a migration/seed pass.
enum Backend {
    Live(SqliteStore),
    #[cfg(any(test, feature = "test-support"))]
    Projection(Mutex<Connection>),
}

#[derive(Default)]
struct WriterMetrics {
    #[cfg(any(test, feature = "test-support"))]
    writer_hold_ns: AtomicU64,
    archive_attachments: AtomicU64,
    commits: AtomicU64,
    acquisitions: AtomicU64,
    queue_ns: AtomicU64,
    command_queue_ns: AtomicU64,
    sql_ns: AtomicU64,
    commit_ns: AtomicU64,
    pending_commits: AtomicU64,
    /// Set when a block table changed outside an account command: the
    /// in-memory block state then misses that change, so the writer reloads
    /// it before its next batch.
    block_reload: Arc<std::sync::atomic::AtomicBool>,
    /// Full loads of the account state the writer ran, the first included.
    /// Each reads every admission and affinity: seconds at live size.
    state_loads: Arc<AtomicU64>,
    block_writes_outside: AtomicU64,
    /// Block-table rows written by test seeding code, which bypass the
    /// account writer on purpose. They reload the state but are not counted
    /// as violations.
    block_seed_writes: AtomicU64,
    /// Each (table, thread) that wrote a block table outside an account
    /// command, warned about once.
    block_outside_tables: Mutex<BTreeSet<(String, String)>>,
    #[cfg(any(test, feature = "test-support"))]
    outside_account_writer: AtomicU64,
}

pub const ACCOUNT_TABLES: &[&str] = &[
    "block",
    "probe_authorization",
    "success_proof",
    "retirement_tombstone",
    "account_eligibility",
    "account_admission",
    "account_observation",
    "account_continue",
    "pool_key_state",
    "pool_key_lease",
    "model_route_state",
    "provider_cooldown_state",
    "account_observation_event",
    "account_affinity",
    "account_quota_reading",
    "account_quota_completeness",
    "account_vault_roster",
    "account_pin",
    "pool_session_affinity",
    "routing_v1_affinity",
    "account_selection_salt",
];

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static IN_ACCOUNT_BATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static ACCOUNT_ROWS_CHANGED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn account_rows_changed() -> u64 {
    ACCOUNT_ROWS_CHANGED.with(|rows| rows.get())
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) struct AccountBatchGuard;
#[cfg(any(test, feature = "test-support"))]
impl AccountBatchGuard {
    pub(crate) fn enter() -> Self {
        IN_ACCOUNT_BATCH.with(|inside| assert!(!inside.replace(true)));
        Self
    }
}
#[cfg(any(test, feature = "test-support"))]
impl Drop for AccountBatchGuard {
    fn drop(&mut self) {
        IN_ACCOUNT_BATCH.with(|inside| inside.set(false));
    }
}

/// Cumulative connection timings. SQL includes row decoding inside the store
/// callback; queue time ends as soon as the connection has been acquired.
#[derive(Clone, Copy, Debug, Default)]
pub struct StoreStageTiming {
    pub writer_queue_ns: u64,
    pub sql_ns: u64,
    pub commit_ns: u64,
}

thread_local! {
    static COMMIT_START: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
    static COMMIT_NS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static THREAD_TIMING: std::cell::Cell<StoreStageTiming> = const { std::cell::Cell::new(StoreStageTiming { writer_queue_ns: 0, sql_ns: 0, commit_ns: 0 }) };
    static CALL_TIMING: std::cell::Cell<Option<StoreStageTiming>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn record_stage_timing(delta: StoreStageTiming) {
    let add = |mut timing: StoreStageTiming| {
        timing.writer_queue_ns += delta.writer_queue_ns;
        timing.sql_ns += delta.sql_ns;
        timing.commit_ns += delta.commit_ns;
        timing
    };
    THREAD_TIMING.with(|timing| timing.set(add(timing.get())));
    CALL_TIMING.with(|timing| timing.set(timing.get().map(add)));
}

struct CallTimingGuard {
    previous: Option<StoreStageTiming>,
    finished: bool,
}

impl CallTimingGuard {
    fn enter(timing: StoreStageTiming) -> Self {
        Self {
            previous: CALL_TIMING.with(|current| current.replace(Some(timing))),
            finished: false,
        }
    }

    fn finish(mut self) -> StoreStageTiming {
        let timing = CALL_TIMING.with(|current| current.replace(self.previous));
        self.finished = true;
        timing.expect("call timing is installed during its future's poll")
    }
}

impl Drop for CallTimingGuard {
    fn drop(&mut self) {
        if !self.finished {
            CALL_TIMING.with(|current| current.set(self.previous));
        }
    }
}

/// Accumulate only store work polled by this future, even if it yields or moves
/// between runtime threads. Restore the previous scope before returning Pending
/// or Ready, and on panic, so another request cannot inherit the accumulator.
/// Work spawned onto another task or blocking thread needs its own scope.
pub async fn scope_stage_timing<F: std::future::Future>(
    future: F,
) -> (F::Output, StoreStageTiming) {
    let mut future = std::pin::pin!(future);
    let mut timing = StoreStageTiming::default();
    let result = std::future::poll_fn(|cx| {
        let scope = CallTimingGuard::enter(timing);
        let result = future.as_mut().poll(cx);
        timing = scope.finish();
        result
    })
    .await;
    (result, timing)
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_stage_timing_for_test(timing: StoreStageTiming) {
    record_stage_timing(timing);
}

fn finish_commit_timing() {
    COMMIT_START.with(|start| {
        if let Some(start) = start.take() {
            COMMIT_NS.with(|total| total.set(total.get() + start.elapsed().as_nanos() as u64));
        }
    });
}

/// Finish measuring the commit before subsequent SQL in the same callback.
pub(crate) fn timed_commit(tx: rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    let start = std::time::Instant::now();
    let before = COMMIT_NS.with(|total| total.get());
    let result = tx.commit();
    COMMIT_START.with(|pending| pending.set(None));
    COMMIT_NS.with(|total| total.set(before + start.elapsed().as_nanos() as u64));
    result
}

pub(super) struct ConnectionStore {
    backend: Backend,
    metrics: Arc<WriterMetrics>,
}

// SQLITE_IGNORE disables SQLite's whole-table delete shortcut, which would
// otherwise bypass the row hooks that keep the account snapshot current.
pub(super) fn writer_authorization(
    context: rusqlite::hooks::AuthContext<'_>,
) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};
    if matches!(context.action, AuthAction::Delete { table_name }
        if crate::block_state::table_index(table_name).is_some()
            || (cfg!(any(test, feature = "test-support")) && ACCOUNT_TABLES.contains(&table_name)))
    {
        Authorization::Ignore
    } else {
        Authorization::Allow
    }
}

impl ConnectionStore {
    pub(crate) fn is_projection(&self) -> bool {
        #[cfg(any(test, feature = "test-support"))]
        {
            return matches!(self.backend, Backend::Projection(_));
        }
        #[cfg(not(any(test, feature = "test-support")))]
        false
    }

    pub(crate) fn block_reload_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.metrics.block_reload)
    }

    pub(crate) fn state_load_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.metrics.state_loads)
    }

    pub(crate) fn record_archive_attachment(&self) {
        self.metrics
            .archive_attachments
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_command_wait(&self, ns: u64) {
        self.metrics
            .command_queue_ns
            .fetch_add(ns, Ordering::Relaxed);
    }
    fn new(backend: Backend) -> Self {
        let metrics = Arc::new(WriterMetrics::default());
        let install = |conn: &Connection| {
            if matches!(&backend, Backend::Live(_)) {
                conn.authorizer(Some(writer_authorization));
                let changes = Arc::clone(&metrics);
                conn.update_hook(Some(move |_, database: &str, table: &str, rowid: i64| {
                    if database == "main" {
                        if let Some(index) = crate::block_state::table_index(table) {
                            if !crate::block_state::capture(index, rowid) {
                                changes.block_reload.store(true, Ordering::Release);
                                if crate::block_state::seeding() {
                                    changes.block_seed_writes.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    changes.block_writes_outside.fetch_add(1, Ordering::Relaxed);
                                    let thread = std::thread::current()
                                        .name()
                                        .unwrap_or("unnamed")
                                        .to_owned();
                                    if changes
                                        .block_outside_tables
                                        .lock()
                                        .unwrap()
                                        .insert((table.to_owned(), thread.clone()))
                                    {
                                        tracing::warn!(
                                            table,
                                            thread,
                                            "block table written outside an account command; \
                                             the account writer reloads its state before the next batch"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    #[cfg(any(test, feature = "test-support"))]
                    if ACCOUNT_TABLES.contains(&table) {
                        crate::writer_entry_trace::account_row_written();
                        if IN_ACCOUNT_BATCH.with(|inside| inside.get()) {
                            ACCOUNT_ROWS_CHANGED.with(|rows| rows.set(rows.get() + 1));
                        } else {
                            changes
                                .outside_account_writer
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }
            let commits = Arc::clone(&metrics);
            conn.commit_hook(Some(move || {
                commits.pending_commits.fetch_add(1, Ordering::Relaxed);
                COMMIT_START.with(|start| start.set(Some(std::time::Instant::now())));
                false
            }));
            let rollbacks = Arc::clone(&metrics);
            conn.rollback_hook(Some(move || {
                // A failed commit may invoke the commit hook before SQLite
                // rolls it back. Do not publish that attempt as a commit.
                if COMMIT_START.with(|start| start.get().is_some())
                    && rollbacks.pending_commits.load(Ordering::Relaxed) > 0
                {
                    rollbacks.pending_commits.fetch_sub(1, Ordering::Relaxed);
                }
                finish_commit_timing();
            }));
            Ok(())
        };
        match &backend {
            Backend::Live(store) => store.with_conn(install).expect("installing writer hooks"),
            #[cfg(any(test, feature = "test-support"))]
            Backend::Projection(conn) => install(&conn.lock().unwrap()).unwrap(),
        }
        Self { backend, metrics }
    }

    pub fn live(store: SqliteStore) -> Self {
        Self::new(Backend::Live(store))
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn projection(conn: Mutex<Connection>) -> Self {
        Self::new(Backend::Projection(conn))
    }

    #[track_caller]
    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        let caller = std::panic::Location::caller();
        let queued = std::time::Instant::now();
        let held = std::cell::Cell::new(std::time::Duration::ZERO);
        let measured = |conn: &Connection| {
            let queue = queued.elapsed().as_nanos() as u64;
            self.metrics.acquisitions.fetch_add(1, Ordering::Relaxed);
            self.metrics.queue_ns.fetch_add(queue, Ordering::Relaxed);
            let start = std::time::Instant::now();
            let commit_before = COMMIT_NS.with(|n| n.get());
            let result = f(conn);
            finish_commit_timing();
            let commit = COMMIT_NS.with(|n| n.get()) - commit_before;
            let elapsed = start.elapsed();
            held.set(elapsed);
            #[cfg(any(test, feature = "test-support"))]
            self.metrics
                .writer_hold_ns
                .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
            let sql = (elapsed.as_nanos() as u64).saturating_sub(commit);
            self.metrics.sql_ns.fetch_add(sql, Ordering::Relaxed);
            self.metrics.commit_ns.fetch_add(commit, Ordering::Relaxed);
            self.metrics.commits.fetch_add(
                self.metrics.pending_commits.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            record_stage_timing(StoreStageTiming {
                writer_queue_ns: queue,
                sql_ns: sql,
                commit_ns: commit,
            });
            result
        };
        let result = match &self.backend {
            Backend::Live(store) => store.with_conn(measured),
            #[cfg(any(test, feature = "test-support"))]
            Backend::Projection(connection) => {
                measured(&connection.lock().unwrap_or_else(|error| error.into_inner()))
                    .map_err(|error| cortexkit_store::StoreError::Backend(error.to_string()))
            }
        };
        // Release the writer before formatting or printing a slow-holder line.
        crate::job_timing::writer_held(held.get(), caller);
        result
    }

    pub fn with_read<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        match &self.backend {
            Backend::Live(store) => store.with_read(|conn| {
                let start = std::time::Instant::now();
                let result = f(conn);
                let sql = start.elapsed().as_nanos() as u64;
                self.metrics.sql_ns.fetch_add(sql, Ordering::Relaxed);
                record_stage_timing(StoreStageTiming {
                    sql_ns: sql,
                    ..Default::default()
                });
                result
            }),
            #[cfg(any(test, feature = "test-support"))]
            Backend::Projection(_) => self.with_conn(f),
        }
    }

    pub fn epoch(&self) -> u64 {
        match &self.backend {
            Backend::Live(store) => store.epoch(),
            #[cfg(any(test, feature = "test-support"))]
            Backend::Projection(_) => 0,
        }
    }
}

impl SqliteRouteStateStore {
    /// Number of maintenance acquisitions that attached the archive.
    #[cfg(any(test, feature = "test-support"))]
    pub fn archive_attachment_count_for_test(&self) -> u64 {
        self.store
            .metrics
            .archive_attachments
            .load(Ordering::Relaxed)
    }

    /// Exclusive writer-callback time, excluding pooled-reader SQL time.
    #[cfg(any(test, feature = "test-support"))]
    pub fn writer_hold_ns_for_test(&self) -> u64 {
        self.store.metrics.writer_hold_ns.load(Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn projection_command<T>(
        &self,
        command: impl FnOnce(&crate::AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        assert!(
            self.store.is_projection(),
            "direct commands require a private projection"
        );
        self.direct_account_transaction(command)
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn outside_account_writer_count_for_test(&self) -> u64 {
        self.store
            .metrics
            .outside_account_writer
            .load(Ordering::Relaxed)
    }
    /// Block-table rows changed outside an account command. Each such change
    /// makes the writer reload the in-memory state before its next batch;
    /// production code makes none.
    pub fn block_writes_outside_command(&self) -> u64 {
        self.store
            .metrics
            .block_writes_outside
            .load(Ordering::Relaxed)
    }
    /// The tables written outside an account command so far, each with the
    /// thread that wrote it.
    pub fn block_tables_written_outside_command(&self) -> Vec<(String, String)> {
        self.store
            .metrics
            .block_outside_tables
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }
    /// Block-table rows written by test seeding code.
    #[cfg(any(test, feature = "test-support"))]
    pub fn block_seed_writes_for_test(&self) -> u64 {
        self.store.metrics.block_seed_writes.load(Ordering::Relaxed)
    }
    /// Full loads of the account state the writer has run, the first one
    /// included. Every load after the first one reloads the whole state.
    pub fn account_state_loads(&self) -> u64 {
        self.store.metrics.state_loads.load(Ordering::Relaxed)
    }
    /// Whether a block-table write outside an account command is waiting for
    /// the writer to reload the state.
    pub fn account_state_reload_pending(&self) -> bool {
        self.store.metrics.block_reload.load(Ordering::Acquire)
    }
    /// Queue time from command enqueue to batch start, excluding connection wait.
    pub fn account_command_queue_ns(&self) -> u64 {
        self.store.metrics.command_queue_ns.load(Ordering::Relaxed)
    }
    pub fn stage_timing(&self) -> StoreStageTiming {
        StoreStageTiming {
            writer_queue_ns: self.store.metrics.queue_ns.load(Ordering::Relaxed),
            sql_ns: self.store.metrics.sql_ns.load(Ordering::Relaxed),
            commit_ns: self.store.metrics.commit_ns.load(Ordering::Relaxed),
        }
    }

    /// Thread-local totals for synchronous measurements without an intervening await.
    pub fn thread_stage_timing() -> StoreStageTiming {
        THREAD_TIMING.with(|timing| timing.get())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn writer_commit_count_for_test(&self) -> u64 {
        self.store.metrics.commits.load(Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn writer_acquisition_count_for_test(&self) -> u64 {
        self.store.metrics.acquisitions.load(Ordering::Relaxed)
    }

    /// Copy only the evidence account preparation reads. Reconciliation may
    /// mutate this disposable in-memory store, but cannot reach the source.
    /// The pooled, read-only transaction ends before constructing the copy.
    #[cfg(any(test, feature = "test-support"))]
    pub fn private_account_copy_for_test(&self, now_ms: i64) -> RouteStateResult<Self> {
        let mut tables = ACCOUNT_COPY_BLOCK_TABLES
            .map(|table| (table, "1".to_string()))
            .to_vec();
        tables.extend([
            ("model_facts", "1".to_string()),
            ("routing_operator_decommissioned", "1".to_string()),
            (
                "account_admission",
                "state IN ('reserved','started')".to_string(),
            ),
            (
                "account_continue",
                "used_at_ms IS NULL AND released_at_ms IS NULL".to_string(),
            ),
            (
                "account_observation",
                // A run's sign-in exclusions survive global fence recovery.
                format!(
                    "bound=1 AND (kind='signed_out' OR (kind='transient_backoff' AND received_at>{}))",
                    now_ms.saturating_sub(24 * 60 * 60_000)
                ),
            ),
            (
                "pool_key_lease",
                format!("released_at IS NULL AND valid_until>{now_ms}"),
            ),
        ]);
        let (copied, indexes, fence_triggers) = self
            .store
            .with_read(|source| {
                let mut copied = Vec::new();
                let mut indexes = Vec::new();
                for (table, predicate) in &tables {
                    let schema = source.query_row(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                        [table],
                        |row| row.get::<_, String>(0),
                    );
                    let schema = match schema {
                        Ok(schema) => schema,
                        Err(rusqlite::Error::QueryReturnedNoRows) => continue,
                        Err(error) => return Err(error),
                    };
                    let mut info = source.prepare(&format!("PRAGMA table_xinfo({table})"))?;
                    let writable = info.query_map([], |row| {
                        Ok((row.get::<_,String>(1)?,row.get::<_,i64>(6)?))
                    })?.filter_map(|row| match row {
                        Ok((name,0)) => Some(Ok(format!("\"{}\"",name.replace('"',"\"\"")))),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }).collect::<rusqlite::Result<Vec<_>>>()?.join(",");
                    let mut query = source.prepare(&format!(
                        "SELECT {writable} FROM {table} WHERE {predicate}"
                    ))?;
                    let columns = query.column_count();
                    let rows = query
                        .query_map([], |row| {
                            (0..columns)
                                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    copied.push((*table, schema, writable, columns, rows));
                    let mut statement = source.prepare(
                        "SELECT sql FROM sqlite_schema WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL ORDER BY name"
                    )?;
                    indexes.extend(statement.query_map([table], |row| row.get::<_,String>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()?);
                }
                let mut statement = source.prepare(
                    "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name IN
                     ('model_facts_inherit_provider_cooldown','retirement_tombstone_enforce',
                      'block_retirement_insert','block_retirement_update','block_clock_drop') ORDER BY name",
                )?;
                let fence_triggers = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((copied, indexes, fence_triggers))
            })
            .map_err(err)?;
        let store =
            ConnectionStore::projection(Mutex::new(Connection::open_in_memory().map_err(err)?));
        store
            .with_conn(|destination| {
                destination.pragma_update(None, "foreign_keys", false)?;
                let tx = crate::write_transaction(destination)?;
                for (table, schema, writable, columns, rows) in copied {
                    tx.execute_batch(&schema)?;
                    let placeholders = (0..columns).map(|_| "?").collect::<Vec<_>>().join(",");
                    let mut insert = tx.prepare(&format!(
                        "INSERT INTO {table} ({writable}) VALUES ({placeholders})"
                    ))?;
                    for row in rows {
                        insert.execute(rusqlite::params_from_iter(row))?;
                    }
                }
                for index in indexes {
                    tx.execute_batch(&index)?;
                }
                // Install triggers after loading the copied rows so the inserts
                // cannot fire retirement or clock-reset logic in the copy.
                for trigger in fence_triggers {
                    tx.execute_batch(&trigger)?;
                }
                tx.commit()
            })
            .map_err(err)?;
        Ok(Self::from_inner(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(store: &SqliteRouteStateStore, sql: &str) -> i64 {
        store
            .store
            .with_conn(|conn| conn.query_row(sql, [], |row| row.get(0)))
            .unwrap()
    }

    /// A snapshot must run the same fence synchronization as the live store:
    /// retiring expired evidence writes the archive table, whose trigger lifts
    /// the enforcement row. A missing table or trigger fails selection outright.
    #[test]
    fn snapshot_carries_fence_archive_authorizations_and_triggers() {
        let mut copied_tables = ACCOUNT_COPY_BLOCK_TABLES.to_vec();
        let mut native_tables = crate::block_state::BLOCK_TABLES.to_vec();
        copied_tables.sort();
        native_tables.sort();
        assert_eq!(
            copied_tables, native_tables,
            "the disposable copy must cover every native block table"
        );
        let scratch = cortexkit_test_support::ScratchDir::new("evaluation-snapshot-fences");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "evaluation-snapshot-fences",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = crate::now_ms();
        store
            .store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO block(kind,origin_key,provider,evidence,evidence_at,quota,deadline,cooldown_until,observed_at,next_recheck_at)
                     VALUES ('provider',json_array('legacy'),'legacy',json_array(?1,NULL,100),100,0,?1,?1,100,?1)",
                    [now - 10_000],
                )?;
                conn.execute(
                    "INSERT INTO probe_authorization VALUES ('owner','provider','[\"other\"]','e',1,2,3)",
                    [],
                )
            })
            .unwrap();

        let snapshot = store.private_account_copy_for_test(now).unwrap();
        assert_eq!(count(&snapshot,"SELECT count(*) FROM sqlite_schema WHERE name IN ('account_fence','credential_entitlement_fences','account_route_fence','account_non_quota_route_fence','account_size_refusal','account_non_quota_size_refusal','account_principal_exclusion','account_transient_backoff','fence_recheck','fence_probe_authorization','fence_recheck_route_clear','fence_success_pool','fence_retired_origin','fence_origin','fence_retire_enforcement','model_route_state_inherit_fence_scope')"),0,"the disposable copy has no frozen compatibility relations or legacy enforcement triggers");
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM probe_authorization"),
            1
        );
        snapshot
            .account_transaction(|tx| {
                assert!(tx.claim_fence_rechecks(&[], now)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM retirement_tombstone"),
            1
        );
        assert_eq!(
            count(
                &snapshot,
                "SELECT COUNT(*) FROM block WHERE kind='provider' AND source_present=1 AND cooldown_until IS NOT NULL"
            ),
            0,
            "the retirement trigger lifts the copied enforcement row"
        );
        assert_eq!(
            count(
                &store,
                "SELECT COUNT(*) FROM block WHERE kind='provider' AND source_present=1 AND cooldown_until IS NOT NULL"
            ),
            1,
            "the source store is untouched"
        );
        let indexes = |store: &SqliteRouteStateStore| {
            store.with_reader_for_test(|conn| {
                conn.prepare("SELECT name,sql FROM sqlite_schema WHERE type='index' AND sql IS NOT NULL AND tbl_name IN ('block','probe_authorization','success_proof','retirement_tombstone','pool_key_state','provider_cooldown_state','model_route_state','account_eligibility') ORDER BY name")?
                    .query_map([], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            }).unwrap()
        };
        let source_indexes = indexes(&store);
        assert!(source_indexes
            .iter()
            .any(|(name, _)| name == "observation_retirement_guard"));
        assert_eq!(
            indexes(&snapshot),
            source_indexes,
            "owned native indexes survive the disposable copy"
        );
    }
}
