//! History has its own writer and migration namespace. State writes put typed
//! rows in the outbox; moving a committed batch never enlists both file writers.

use super::*;
use rusqlite::{types::Value, OpenFlags};
use std::path::{Path, PathBuf};

mod pair;
pub(super) use pair::{inspect_files, migrate_split, refuse_moved_rows, state_binding, version};

pub(super) const STATE_MIGRATION: Migration = Migration {
    version: 98,
    statements: include_str!("../migrations/098_history_outbox.sql"),
};
const NAMESPACE: &str = "route_history";
const CHAIN: [Migration; 1] = [Migration {
    version: 1,
    statements: include_str!("../history-migrations/001_split_baseline.sql"),
}];
const TABLES: &str = include_str!("../history-migrations/tables.txt");
const BATCH: usize = 64;
const QUOTA_PENDING_MS: i64 = 60 * 60 * 1000;

pub(super) struct History {
    writer: SqliteStore,
    reader: Option<Mutex<Connection>>,
    pub(super) attachment: String,
    // Serializes copy/ack with history retention, not with state requests.
    mover: Mutex<()>,
}

fn file_uri(path: &Path) -> String {
    let mut uri = String::from("file:");
    for byte in path.to_string_lossy().bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
            uri.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut uri, "%{byte:02X}").unwrap();
        }
    }
    uri
}

pub(super) fn path_for(state: &Path) -> PathBuf {
    let name = state.file_name().unwrap_or_default();
    if name == "state.db" || name == "store.db" {
        state.with_file_name("history.db")
    } else {
        let mut name = name.to_os_string();
        name.push(".history.db");
        state.with_file_name(name)
    }
}

fn has_split_layout(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row("SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='history_layout')", [], |row| row.get(0))
}

pub fn open_offline(path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        format!("{}?mode=ro&immutable=1", file_uri(path)),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
}

const HOLD_UNION: &str = "SELECT * FROM main.pending_account_hold
    UNION ALL SELECT h.* FROM archive.account_hold h
    WHERE NOT EXISTS (SELECT 1 FROM main.pending_account_hold p WHERE p.decision_id=h.decision_id)";

fn attach_reader(conn: &Connection, uri: &str) -> rusqlite::Result<()> {
    conn.execute("ATTACH DATABASE ?1 AS archive", [uri])?;
    conn.execute_batch(&format!(
        "CREATE TEMP VIEW visible_account_holds AS {HOLD_UNION};
         CREATE TEMP VIEW visible_quota_readings AS
           SELECT * FROM main.pending_quota_reading_log
           UNION ALL SELECT h.* FROM archive.quota_reading_log h
           WHERE NOT EXISTS (SELECT 1 FROM main.pending_quota_reading_log p
             WHERE (p.account_key,p.window_key,p.observed_at)=(h.account_key,h.window_key,h.observed_at));"
    ))
}

fn detach(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("DETACH DATABASE archive")
}

pub(super) fn with_archive<T>(
    conn: &Connection,
    uri: Option<&str>,
    f: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let Some(uri) = uri else { return f() };
    conn.execute("ATTACH DATABASE ?1 AS archive", [uri])?;
    // Even a panic must detach after the transaction has rolled back; the
    // state connection may still be used by non-account writers afterward.
    struct Attached<'a>(&'a Connection, bool);
    impl Drop for Attached<'_> {
        fn drop(&mut self) {
            if self.1 {
                detach(self.0).expect("detach history after unwinding");
            }
        }
    }
    let mut guard = Attached(conn, true);
    let result = f();
    detach(conn).expect("detach history before releasing state writer");
    guard.1 = false;
    result
}

impl History {
    pub(super) fn open(path: &str, memory: bool, module_id: &str) -> RouteStateResult<Self> {
        static NEXT_MEMORY: AtomicU64 = AtomicU64::new(0);
        let (source, attachment) = if memory {
            let next = NEXT_MEMORY.fetch_add(1, Ordering::Relaxed);
            let uri = format!("file:striatum-history-{next}?mode=memory&cache=shared");
            (uri.clone(), uri)
        } else {
            let state_path = Path::new(path);
            let history = path_for(state_path);
            (
                history.to_string_lossy().into_owned(),
                format!("{}?mode=ro", file_uri(&history)),
            )
        };
        let lease_id = if memory {
            format!("{module_id}-history")
        } else {
            // The shared runner keys leases by module, not by the basename.
            // Alternate state basenames in one directory have separate archives.
            let filename = Path::new(&source)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            format!("striatum-history-{filename}")
        };
        let mut descriptor = dev_descriptor(&lease_id, source);
        descriptor.storage_namespace = NAMESPACE.into();
        let writer = open_sqlite(&descriptor).map_err(err)?;
        writer
            .with_conn(|conn| {
                // A state acknowledgement must never outlive its archive
                // copy after power loss. State can still commit at NORMAL.
                conn.pragma_update(None, "synchronous", "FULL")?;
                conn.pragma_update(None, "journal_size_limit", WAL_SIZE_CAP_BYTES)?;
                conn.pragma_update(None, "wal_autocheckpoint", 0)
            })
            .map_err(err)?;
        let migration = writer.migrate(NAMESPACE, &CHAIN).map_err(err)?;
        if migration.store_ahead() {
            return Err(err(format!("store_ahead: namespace {NAMESPACE} is at version {} but this binary's chain ends at {}", migration.recorded,migration.chain_max)));
        }
        Ok(Self {
            writer,
            reader: None,
            attachment,
            mover: Mutex::new(()),
        })
    }

    pub(super) fn open_reader(&mut self, path: &str) -> RouteStateResult<()> {
        let conn =
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)?;
        conn.busy_timeout(WAL_TRUNCATE_WAIT_BOUND).map_err(err)?;
        attach_reader(&conn, &self.attachment).map_err(err)?;
        self.reader = Some(Mutex::new(conn));
        Ok(())
    }

    pub(super) fn checkpoint(&self) -> RouteStateResult<()> {
        self.writer
            .with_conn(|conn| conn.query_row("PRAGMA main.wal_checkpoint(PASSIVE)", [], |_| Ok(())))
            .map_err(err)
    }

    pub(super) fn close_reader_and_checkpoint(&mut self) {
        if self.reader.take().is_some() {
            if let Err(error) = self.writer.with_conn(|conn| {
                conn.busy_timeout(WAL_TRUNCATE_WAIT_BOUND)?;
                checkpoint_on_close(conn)
            }) {
                tracing::warn!(%error, "could not checkpoint routing history on shutdown");
            }
        }
    }
}

impl SqliteRouteStateStore {
    pub(super) fn with_history_reader<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.with_history_reader_pins(|| {}, f)
    }

    fn with_history_reader_pins<T>(
        &self,
        between_pins: impl FnOnce(),
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        match &self.history {
            Some(history) => match &history.reader {
                Some(reader) => {
                    let conn = reader.lock().unwrap_or_else(|p| p.into_inner());
                    let tx = conn.unchecked_transaction().map_err(err)?;
                    // Pin state first even if the query planner starts with the
                    // archive arm. After a state ack is visible, the archive
                    // commit that preceded it must be visible as well.
                    tx.query_row("SELECT count(*) FROM main.sqlite_schema", [], |_| Ok(()))
                        .map_err(err)?;
                    between_pins();
                    tx.query_row("SELECT count(*) FROM archive.sqlite_schema", [], |_| Ok(()))
                        .map_err(err)?;
                    let result = f(&tx).map_err(err)?;
                    tx.commit().map_err(err)?;
                    Ok(result)
                }
                // Test-only memory stores have no independent file reader.
                // Use a disk fixture for tests exercising archived data.
                None => Err(err("history reads require a file-backed store")),
            },
            None => self.store.with_read(f).map_err(err),
        }
    }

    /// Copy at most one batch from each outbox. Quota samples stay pending for
    /// an hour, so slightly out-of-order deliveries can still fill gaps.
    pub fn move_history_batch(&self, now_ms: i64) -> RouteStateResult<usize> {
        self.move_history_batch_status(now_ms)
            .map(|(moved, _, _)| moved)
    }

    /// Return rows moved, whether either lane filled its batch, and whether
    /// the reader saw an eligible row beyond the batch limit.
    pub fn move_history_batch_status(&self, now_ms: i64) -> RouteStateResult<(usize, bool, bool)> {
        self.move_history_batch_after_commit(now_ms, || Ok(()))
    }

    fn move_history_batch_after_commit(
        &self,
        now_ms: i64,
        after_commit: impl FnOnce() -> RouteStateResult<()>,
    ) -> RouteStateResult<(usize, bool, bool)> {
        let Some(history) = self.history.as_ref() else {
            return Ok((0, false, false));
        };
        let _guard = history.mover.lock().unwrap_or_else(|p| p.into_inner());
        let mut holds = self.pending_batch(Kind::Hold, i64::MAX, BATCH + 1)?;
        let mut readings = self.pending_batch(
            Kind::Quota,
            now_ms.saturating_sub(QUOTA_PENDING_MS),
            BATCH + 1,
        )?;
        if holds.is_empty() && readings.is_empty() {
            return Ok((0, false, false));
        }
        let more = holds.len() > BATCH || readings.len() > BATCH;
        let full = holds.len() >= BATCH || readings.len() >= BATCH;
        holds.truncate(BATCH);
        readings.truncate(BATCH);
        let generation = history.copy(&holds, &readings)?;
        after_commit()?;
        self.delete_pending(&holds, &readings, Some(generation))?;
        Ok((holds.len() + readings.len(), full, more))
    }

    fn pending_batch(
        &self,
        kind: Kind,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<Vec<Vec<Value>>> {
        self.store
            .with_read(|conn| {
                let mut statement = conn.prepare(kind.pending_sql())?;
                let columns = statement.column_count();
                let rows = statement.query_map(params![before, limit as i64], |row| {
                    (0..columns).map(|idx| row.get(idx)).collect()
                })?;
                rows.collect()
            })
            .map_err(err)
    }

    fn delete_pending(
        &self,
        holds: &[Vec<Value>],
        readings: &[Vec<Value>],
        generation: Option<i64>,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|conn| {
            let tx = write_transaction(conn)?;
            {
                let mut delete_hold = tx.prepare("DELETE FROM pending_account_hold WHERE decision_id=?1")?;
                for row in holds { delete_hold.execute([&row[0]])?; }
                let mut delete_quota = tx.prepare("DELETE FROM pending_quota_reading_log WHERE (account_key,window_key,observed_at)=(?1,?2,?3)")?;
                let mut advance = tx.prepare("INSERT INTO quota_reading_moved VALUES (?1,?2,?3)
                    ON CONFLICT(account_key,window_key) DO UPDATE SET moved_through_observed_at = max(excluded.moved_through_observed_at, moved_through_observed_at)")?;
                for row in readings {
                    let key = [&row[0],&row[1],&row[2]];
                    delete_quota.execute(key)?;
                    if generation.is_some() { advance.execute(key)?; }
                }
            }
            if let Some(generation) = generation {
                tx.execute("UPDATE history_layout SET move_generation=?1 WHERE singleton=1", [generation])?;
            }
            tx.commit()
        }).map_err(err)
    }
}

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Hold,
    Quota,
}

impl Kind {
    fn pending_sql(self) -> &'static str {
        match self {
            Self::Hold => "SELECT * FROM pending_account_hold WHERE created_at<?1 ORDER BY created_at LIMIT ?2",
            // Picking a prefix by observed_at also picks a prefix within each window.
            Self::Quota => "SELECT * FROM pending_quota_reading_log WHERE observed_at<?1 ORDER BY observed_at,account_key,window_key LIMIT ?2",
        }
    }
}

impl History {
    fn copy(&self, holds: &[Vec<Value>], readings: &[Vec<Value>]) -> RouteStateResult<i64> {
        self.writer.with_conn(|conn| {
            let tx = write_transaction(conn)?;
            {
                for (sql, rows) in [
                    ("INSERT INTO account_hold VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(decision_id) DO NOTHING", holds),
                    ("INSERT INTO quota_reading_log VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(account_key,window_key,observed_at) DO NOTHING", readings),
                ] {
                    let mut insert = tx.prepare(sql)?;
                    for row in rows { insert.execute(rusqlite::params_from_iter(row))?; }
                }
            }
            let generation = tx.query_row(
                "UPDATE history_binding SET move_generation=move_generation+1 WHERE singleton=1 RETURNING move_generation",
                [], |row| row.get(0),
            )?;
            tx.commit()?;
            Ok(generation)
        }).map_err(err)
    }
}

const QUOTA_WATERMARK: &str = "SELECT moved_through_observed_at FROM quota_reading_moved
    WHERE account_key=?1 AND window_key=?2";
const APPEND_QUOTA: &str = "INSERT INTO pending_quota_reading_log
    VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
    ON CONFLICT(account_key,window_key,observed_at) DO NOTHING";
const APPEND_LEGACY_QUOTA: &str =
    "INSERT OR IGNORE INTO quota_reading_log VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)";

pub(super) fn append_quota(
    conn: &Connection,
    split: bool,
    rows: &[QuotaReadingLogRow],
) -> rusqlite::Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(if split {
        APPEND_QUOTA
    } else {
        APPEND_LEGACY_QUOTA
    })?;
    let mut read_watermark = split
        .then(|| conn.prepare_cached(QUOTA_WATERMARK))
        .transpose()?;
    let mut watermarks = HashMap::new();
    let mut inserted = 0;
    for row in rows {
        if row.account_key.is_empty()
            || row.window_key.is_empty()
            || row.observed_at < 0
            || !row.used_percent.is_finite()
            || row.raw_used_percent.is_some_and(|value| !value.is_finite())
        {
            continue;
        }
        if let Some(statement) = read_watermark.as_mut() {
            use std::collections::hash_map::Entry;
            let key = (row.account_key.as_str(), row.window_key.as_str());
            let moved_through: i64 = match watermarks.entry(key) {
                Entry::Occupied(value) => *value.get(),
                Entry::Vacant(entry) => *entry.insert(
                    statement
                        .query_row(params![key.0, key.1], |r| r.get(0))
                        .optional()?
                        .unwrap_or(-1),
                ),
            };
            // A delivery over an hour late can fall behind a sample already
            // moved for its window. Treat it as a replay: live quota ignores
            // older readings too, though the log could otherwise retain one.
            if row.observed_at <= moved_through {
                continue;
            }
        }
        let (is_stale, stale_since, stale_class) = match row.stale.as_ref() {
            Some(stale) => (1_i64, stale.since.as_deref(), stale.class.as_deref()),
            None => (0_i64, None, None),
        };
        inserted += insert.execute(params![
            row.account_key,
            row.window_key,
            row.observed_at,
            row.used_percent,
            row.raw_used_percent,
            row.resets_at,
            row.window_minutes,
            row.banked_credit_count,
            row.fetched_at,
            is_stale,
            stale_since,
            stale_class
        ])?;
    }
    Ok(inserted)
}

pub(super) fn hold_sink(conn: &Connection) -> &'static str {
    #[cfg(any(test, feature = "test-support"))]
    if conn
        .prepare("SELECT 1 FROM main.pending_account_hold LIMIT 0")
        .is_err()
    {
        return "account_hold";
    }
    let _ = conn;
    "pending_account_hold"
}

pub(super) fn hold_source(conn: &Connection) -> rusqlite::Result<String> {
    // Historical migration rehearsals select their old schema deliberately.
    // In the split layout, missing attachment is an error, not an empty table.
    Ok(if has_split_layout(conn)? {
        format!("({HOLD_UNION})")
    } else {
        "account_hold".into()
    })
}

pub(super) fn checkpoint_on_close(conn: &Connection) -> rusqlite::Result<()> {
    let busy: i64 = conn.query_row("PRAGMA main.wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("WAL truncate blocked by another connection".into()),
        ));
    }
    Ok(())
}

const HOLD_PRUNE_KEYS: &str = "SELECT decision_id,
      EXISTS(SELECT 1 FROM main.pending_account_hold p WHERE p.decision_id=keys.decision_id)
    FROM (
      SELECT decision_id,created_at FROM main.pending_account_hold INDEXED BY pending_account_hold_by_created WHERE created_at<?1
      UNION ALL
      SELECT h.decision_id,h.created_at FROM archive.account_hold h INDEXED BY account_hold_by_created
      WHERE h.created_at<?1 AND NOT EXISTS(SELECT 1 FROM main.pending_account_hold p WHERE p.decision_id=h.decision_id)
      ORDER BY created_at LIMIT ?2
    ) AS keys";
const QUOTA_PRUNE_KEYS: &str = "SELECT account_key,window_key,observed_at,
      EXISTS(SELECT 1 FROM main.pending_quota_reading_log p WHERE
        (p.account_key,p.window_key,p.observed_at)=(keys.account_key,keys.window_key,keys.observed_at))
    FROM (SELECT account_key,window_key,observed_at FROM visible_quota_readings
      WHERE observed_at<?1 ORDER BY observed_at LIMIT ?2) AS keys";

impl SqliteRouteStateStore {
    pub(super) fn prune_history(
        &self,
        kind: Kind,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<Option<usize>> {
        self.prune_history_after_archive_delete(kind, before, limit, || Ok(()))
    }

    fn prune_history_after_archive_delete(
        &self,
        kind: Kind,
        before: i64,
        limit: usize,
        mut after_archive_delete: impl FnMut() -> RouteStateResult<()>,
    ) -> RouteStateResult<Option<usize>> {
        let Some(history) = &self.history else {
            return Ok(None);
        };
        let _guard = history.mover.lock().unwrap_or_else(|p| p.into_inner());
        let mut total = 0;
        let (candidates, delete) = match kind {
            Kind::Hold => (HOLD_PRUNE_KEYS,"DELETE FROM account_hold WHERE decision_id=?1"),
            Kind::Quota => (QUOTA_PRUNE_KEYS,"DELETE FROM quota_reading_log WHERE (account_key,window_key,observed_at)=(?1,?2,?3)"),
        };
        while total < limit {
            let size = (limit - total).min(BATCH);
            let keys: Vec<(Vec<Value>, bool)> = self.with_history_reader(|conn| {
                let mut statement = conn.prepare(candidates)?;
                let columns = statement.column_count() - 1;
                let rows = statement.query_map(params![before, size as i64], |row| {
                    Ok((
                        (0..columns)
                            .map(|index| row.get(index))
                            .collect::<rusqlite::Result<_>>()?,
                        row.get(columns)?,
                    ))
                })?;
                rows.collect()
            })?;
            if keys.is_empty() {
                break;
            }
            history
                .writer
                .with_conn(|conn| {
                    let tx = write_transaction(conn)?;
                    {
                        let mut statement = tx.prepare(delete)?;
                        for (key, _) in &keys {
                            statement.execute(rusqlite::params_from_iter(key))?;
                        }
                    }
                    tx.commit()
                })
                .map_err(err)?;
            after_archive_delete()?;
            // The archive copy is deleted first. If we stop before the pending
            // copy is deleted too, the reading is still pending, so a replay of
            // it is recognised as already stored. That matters for a reading
            // newer than its window's moved-through mark in quota_reading_moved,
            // which is the only other way a replay is recognised. Retention
            // never advances that mark.
            let batch_size = keys.len();
            let pending: Vec<_> = keys
                .into_iter()
                .filter_map(|(key, pending)| pending.then_some(key))
                .collect();
            if !pending.is_empty() {
                let (holds, quota) = match kind {
                    Kind::Hold => (pending.as_slice(), &[][..]),
                    Kind::Quota => (&[][..], pending.as_slice()),
                };
                self.delete_pending(holds, quota, None)?;
            }
            total += batch_size;
            if batch_size < size {
                break;
            }
        }
        Ok(Some(total))
    }
}

#[cfg(any(test, feature = "test-support"))]
impl SqliteRouteStateStore {
    pub fn with_history_reader_for_test<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.with_history_reader(f)
    }

    pub fn history_tables_in_state_empty_for_test(&self) -> RouteStateResult<bool> {
        self.store
            .with_read(|conn| {
                for name in TABLES.lines() {
                    if conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name=?1)",
                        [name],
                        |row| row.get::<_, bool>(0),
                    )? {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .map_err(err)
    }
}

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
impl SqliteRouteStateStore {
    pub fn watch_history_reads_for_test(&self) -> RouteStateResult<Arc<AtomicU64>> {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
        let reads = Arc::new(AtomicU64::new(0));
        let install = |conn: &Connection, writer: bool| {
            let reads = Arc::clone(&reads);
            conn.authorizer(Some(move |context: AuthContext<'_>| {
                if let AuthAction::Read { table_name, .. } = context.action {
                    if context.database_name == Some("archive")
                        || TABLES.lines().any(|name| name == table_name)
                    {
                        reads.fetch_add(1, Ordering::Relaxed);
                        return Authorization::Deny;
                    }
                }
                if writer {
                    crate::connection_store::writer_authorization(context)
                } else {
                    Authorization::Allow
                }
            }));
            Ok(())
        };
        self.store
            .with_conn(|conn| install(conn, true))
            .map_err(err)?;
        self.with_history_test_pool(&|conn| install(conn, false))?;
        if let Some(reader) = self.history.as_ref().and_then(|h| h.reader.as_ref()) {
            install(&reader.lock().unwrap_or_else(|p| p.into_inner()), false).map_err(err)?;
        }
        Ok(reads)
    }

    fn with_history_test_pool(
        &self,
        f: &impl Fn(&Connection) -> rusqlite::Result<()>,
    ) -> RouteStateResult<()> {
        fn each(
            store: &connection_store::ConnectionStore,
            remaining: usize,
            f: &impl Fn(&Connection) -> rusqlite::Result<()>,
        ) -> RouteStateResult<()> {
            if remaining == 0 {
                return Ok(());
            }
            let mut nested = Ok(());
            store
                .with_read(|conn| {
                    f(conn)?;
                    nested = each(store, remaining - 1, f);
                    Ok(())
                })
                .map_err(err)?;
            nested
        }
        if self
            .history
            .as_ref()
            .is_some_and(|history| history.reader.is_some())
        {
            each(
                &self.store,
                cortexkit_store::SqliteOpenOptions::default().read_pool_size,
                f,
            )?;
        }
        Ok(())
    }

    pub fn stop_watching_history_reads_for_test(&self) -> RouteStateResult<()> {
        let clear = |conn: &Connection| {
            conn.authorizer(
                None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
            );
            Ok(())
        };
        self.store
            .with_conn(|conn| {
                conn.authorizer(Some(crate::connection_store::writer_authorization));
                Ok(())
            })
            .map_err(err)?;
        self.with_history_test_pool(&clear)?;
        if let Some(reader) = self.history.as_ref().and_then(|h| h.reader.as_ref()) {
            clear(&reader.lock().unwrap_or_else(|p| p.into_inner())).map_err(err)?;
        }
        Ok(())
    }
}

#[cfg(all(unix, any(test, feature = "test-support")))]
pub fn run_split_for_test(path: &str, now: i64) {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../script/split-state-history.sh");
    let verifier = std::env::var_os("STRIATUM_SPLIT_VERIFIER")
        .expect("build the verify_history_split example and set STRIATUM_SPLIT_VERIFIER, or use scripts/rust-test-gate.sh");
    let output = std::process::Command::new("bash")
        .arg(script)
        .args([path, "--now-ms", &now.to_string(), "--verifier"])
        .arg(verifier)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(all(test, unix))]
pub(super) fn complete_test_upgrade(descriptor: &StorageDescriptor, now: i64) {
    drop(SqliteRouteStateStore::open_at_version_for_test(descriptor, 97).unwrap());
    let StorageBackend::Sqlite { path } = &descriptor.backend else {
        unreachable!()
    };
    run_split_for_test(path, now);
    drop(SqliteRouteStateStore::open(descriptor).unwrap());
}

impl SqliteRouteStateStore {
    /// Checkpoint the archive on its own connection, never on the state writer.
    pub fn checkpoint_history_wal(&self) -> RouteStateResult<()> {
        self.history.as_ref().map_or(Ok(()), History::checkpoint)
    }
}
