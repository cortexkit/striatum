//! Private account evidence for an operator evaluation. Never copies routing's
//! decision/usage history and never opens a writer on the source database.

use super::*;
use std::sync::Arc;

// Durable stores keep the shared library's lease and pooled reads unchanged.
// A projection needs neither a lease file nor a migration/seed pass.
enum Backend {
    Live(SqliteStore),
    Projection(Mutex<Connection>),
}

#[derive(Default)]
struct WriterMetrics {
    commits: AtomicU64,
    acquisitions: AtomicU64,
    queue_ns: AtomicU64,
    sql_ns: AtomicU64,
    commit_ns: AtomicU64,
    pending_commits: AtomicU64,
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

impl ConnectionStore {
    fn new(backend: Backend) -> Self {
        let metrics = Arc::new(WriterMetrics::default());
        let install = |conn: &Connection| {
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
            Backend::Projection(conn) => install(&conn.lock().unwrap()).unwrap(),
        }
        Self { backend, metrics }
    }

    pub fn live(store: SqliteStore) -> Self {
        Self::new(Backend::Live(store))
    }
    pub fn projection(conn: Mutex<Connection>) -> Self {
        Self::new(Backend::Projection(conn))
    }

    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        let queued = std::time::Instant::now();
        let measured = |conn: &Connection| {
            let queue = queued.elapsed().as_nanos() as u64;
            self.metrics.acquisitions.fetch_add(1, Ordering::Relaxed);
            self.metrics.queue_ns.fetch_add(queue, Ordering::Relaxed);
            let start = std::time::Instant::now();
            let commit_before = COMMIT_NS.with(|n| n.get());
            let result = f(conn);
            finish_commit_timing();
            let commit = COMMIT_NS.with(|n| n.get()) - commit_before;
            let sql = (start.elapsed().as_nanos() as u64).saturating_sub(commit);
            self.metrics.sql_ns.fetch_add(sql, Ordering::Relaxed);
            self.metrics.commit_ns.fetch_add(commit, Ordering::Relaxed);
            self.metrics.commits.fetch_add(
                self.metrics.pending_commits.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            THREAD_TIMING.with(|timing| {
                let mut value = timing.get();
                value.writer_queue_ns += queue;
                value.sql_ns += sql;
                value.commit_ns += commit;
                timing.set(value);
            });
            result
        };
        match &self.backend {
            Backend::Live(store) => store.with_conn(measured),
            Backend::Projection(connection) => {
                measured(&connection.lock().unwrap_or_else(|error| error.into_inner()))
                    .map_err(|error| cortexkit_store::StoreError::Backend(error.to_string()))
            }
        }
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
                THREAD_TIMING.with(|timing| {
                    let mut value = timing.get();
                    value.sql_ns += sql;
                    timing.set(value);
                });
                result
            }),
            Backend::Projection(_) => self.with_conn(f),
        }
    }

    pub fn epoch(&self) -> u64 {
        match &self.backend {
            Backend::Live(store) => store.epoch(),
            Backend::Projection(_) => 0,
        }
    }
}

impl SqliteRouteStateStore {
    pub fn stage_timing(&self) -> StoreStageTiming {
        StoreStageTiming {
            writer_queue_ns: self.store.metrics.queue_ns.load(Ordering::Relaxed),
            sql_ns: self.store.metrics.sql_ns.load(Ordering::Relaxed),
            commit_ns: self.store.metrics.commit_ns.load(Ordering::Relaxed),
        }
    }

    /// Thread-local totals for synchronous account calls, excluding other callers.
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
    pub fn account_evaluation_snapshot(&self, now_ms: i64) -> RouteStateResult<Self> {
        let tables = [
            ("model_facts", "1".to_string()),
            ("fence_recheck", "1".to_string()),
            ("fence_recheck_route_clear", "1".to_string()),
            ("fence_success_pool", "1".to_string()),
            ("fence_probe_authorization", "1".to_string()),
            ("fence_retired_origin", "1".to_string()),
            ("account_non_quota_route_fence", "1".to_string()),
            ("account_non_quota_size_refusal", "1".to_string()),
            ("account_transient_backoff", "1".to_string()),
            ("account_principal_exclusion", "1".to_string()),
            ("model_route_state", "1".to_string()),
            ("provider_cooldown_state", "1".to_string()),
            ("credential_entitlement_fences", "1".to_string()),
            ("account_eligibility", "1".to_string()),
            ("account_size_refusal", "1".to_string()),
            ("account_route_fence", "1".to_string()),
            ("account_fence", "cleared_at IS NULL".to_string()),
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
                format!(
                    "bound=1 AND kind='transient_backoff' AND received_at>{}",
                    now_ms.saturating_sub(24 * 60 * 60_000)
                ),
            ),
            ("pool_key_state", "1".to_string()),
            (
                "pool_key_lease",
                format!("released_at IS NULL AND valid_until>{now_ms}"),
            ),
        ];
        let (copied, origin_view, fence_triggers) = self
            .store
            .with_read(|source| {
                let mut copied = Vec::new();
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
                    let mut query =
                        source.prepare(&format!("SELECT * FROM {table} WHERE {predicate}"))?;
                    let columns = query.column_count();
                    let rows = query
                        .query_map([], |row| {
                            (0..columns)
                                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    copied.push((*table, schema, columns, rows));
                }
                let origin_view = source
                    .query_row(
                        "SELECT sql FROM sqlite_schema WHERE type='view' AND name='fence_origin'",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                // Fence writers rely on these triggers to retire enforcement and
                // keep route provenance; without them the copy diverges from the
                // live outcome. Both reference only tables copied above.
                let mut statement = source.prepare(
                    "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name IN
                     ('fence_retire_enforcement','model_route_state_inherit_fence_scope')",
                )?;
                let fence_triggers = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((copied, origin_view, fence_triggers))
            })
            .map_err(err)?;
        let store =
            ConnectionStore::projection(Mutex::new(Connection::open_in_memory().map_err(err)?));
        store
            .with_conn(|destination| {
                destination.pragma_update(None, "foreign_keys", false)?;
                let tx = crate::write_transaction(destination)?;
                for (table, schema, columns, rows) in copied {
                    tx.execute_batch(&schema)?;
                    let placeholders = (0..columns).map(|_| "?").collect::<Vec<_>>().join(",");
                    let mut insert =
                        tx.prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))?;
                    for row in rows {
                        insert.execute(rusqlite::params_from_iter(row))?;
                    }
                }
                if let Some(view) = origin_view {
                    tx.execute_batch(&view)?;
                }
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
                    "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,updated_at,wall_observed_at)
                     VALUES ('legacy',?1,100,100)",
                    [now - 10_000],
                )?;
                conn.execute(
                    "INSERT INTO fence_probe_authorization VALUES ('owner','provider','[\"other\"]','e',1,2,3)",
                    [],
                )
            })
            .unwrap();

        let snapshot = store.account_evaluation_snapshot(now).unwrap();
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM fence_probe_authorization"),
            1
        );
        snapshot
            .account_transaction(|tx| {
                assert!(tx.claim_fence_rechecks(&[], now)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM fence_retired_origin"),
            1
        );
        assert_eq!(
            count(
                &snapshot,
                "SELECT COUNT(*) FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL"
            ),
            0,
            "the retirement trigger lifts the copied enforcement row"
        );
        assert_eq!(
            count(
                &store,
                "SELECT COUNT(*) FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL"
            ),
            1,
            "the source store is untouched"
        );
    }
}
