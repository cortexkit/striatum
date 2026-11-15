//! State ownership belongs to the module; the store only coordinates its
//! transaction, rollback and publication boundaries.

use std::any::Any;

use crate::{AccountTx, RouteStateResult};

/// A private working copy for a writer batch. Forking must share unchanged
/// data rather than copying all rows for every command savepoint.
pub trait AccountWorkingState: Send {
    fn fork(&self) -> Box<dyn AccountWorkingState>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Copy the block rows a command changed into this working copy. The
    /// writer calls it after the command's statements ran and before its
    /// savepoint is released, so a failed command's rows never arrive.
    fn apply_block_rows(&mut self, rows: &crate::BlockRows);
    /// The block tables this copy holds, if it holds them. Commands decide
    /// probe claims on them instead of reading SQL.
    fn blocks(&self) -> Option<&crate::BlockState> {
        None
    }
}

/// Implemented by the module's snapshot publisher, without a store-to-module
/// dependency. `load` is called inside one deferred read transaction.
pub trait AccountStateBridge: Send + Sync {
    fn load(&self, tx: &AccountTx<'_>) -> RouteStateResult<Box<dyn AccountWorkingState>>;
    fn publish(&self, state: &dyn AccountWorkingState, generation: u64);
    fn snapshot(&self) -> RouteStateResult<std::sync::Arc<dyn Any + Send + Sync>>;
    fn failed(&self, error: &str);
    fn reload_requested(&self) -> bool {
        false
    }
    /// Ask the writer to reload before its next batch.
    fn request_reload(&self) {}
    /// The longest a caller waits on a load of the state: the installing
    /// caller and a retry after a failed load. A load still running after it
    /// keeps running and publishes; the caller gets
    /// [`ACCOUNT_STATE_LOAD_PENDING`].
    fn load_wait(&self) -> std::time::Duration {
        std::time::Duration::from_secs(60)
    }
}

/// The start of the error a caller gets when a load of the account state is
/// still running at the end of its wait.
pub const ACCOUNT_STATE_LOAD_PENDING: &str = "account state not loaded within";

pub(crate) struct WriterAccountState {
    bridge: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn AccountStateBridge>>>,
    pub(crate) working: Option<Box<dyn AccountWorkingState>>,
    data_version: Option<i64>,
    generation: u64,
    /// Set by the update hook when a block table changed outside an account
    /// command, which no captured row reports.
    block_reload: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl WriterAccountState {
    pub(crate) fn new(
        bridge: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn AccountStateBridge>>>,
        block_reload: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            bridge,
            working: None,
            data_version: None,
            generation: 0,
            block_reload,
        }
    }

    pub(crate) fn refresh(&mut self, conn: &rusqlite::Connection) -> RouteStateResult<()> {
        let Some(bridge) = self.bridge.get() else {
            return Ok(());
        };
        let load = || -> RouteStateResult<_> {
            // Read the version before establishing the read snapshot. A commit
            // concurrent with loading can cause an extra reload, never a missed one.
            let version: i64 = conn
                .query_row("PRAGMA data_version", [], |row| row.get(0))
                .map_err(crate::err)?;
            // Both flags are cleared here, whichever is set.
            let outside = self
                .block_reload
                .swap(false, std::sync::atomic::Ordering::AcqRel);
            let reload = bridge.reload_requested() | outside;
            if !reload && self.working.is_some() && self.data_version == Some(version) {
                return Ok(None);
            }
            let tx =
                rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Deferred)
                    .map_err(crate::err)?;
            let state = bridge.load(&AccountTx::sql(&tx))?;
            tx.commit().map_err(crate::err)?;
            Ok(Some((state, version)))
        };
        match load() {
            Ok(Some((state, version))) => {
                self.working = Some(state);
                self.data_version = Some(version);
                self.publish();
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => {
                // Never compute on a copy the failed load meant to replace:
                // without a working state the next batch loads again.
                self.working = None;
                bridge.failed(&error.to_string());
                Err(error)
            }
        }
    }

    /// Inside a write transaction: load the state again if another
    /// connection committed since it was loaded. The load reads through the
    /// open transaction, which already holds the write lock, so nothing can
    /// commit between this load and the batch's commands.
    pub(crate) fn reload_if_moved(&mut self, conn: &rusqlite::Connection) -> RouteStateResult<()> {
        let Some(bridge) = self.bridge.get() else {
            return Ok(());
        };
        if self.working.is_none() {
            return Ok(());
        }
        let version: i64 = conn
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(crate::err)?;
        if self.data_version == Some(version) {
            return Ok(());
        }
        match bridge.load(&AccountTx::sql(conn)) {
            Ok(state) => {
                self.working = Some(state);
                self.data_version = Some(version);
                Ok(())
            }
            Err(error) => {
                self.working = None;
                Err(error)
            }
        }
    }

    pub(crate) fn publish(&mut self) {
        if let (Some(bridge), Some(state)) = (self.bridge.get(), self.working.as_deref()) {
            self.generation += 1;
            bridge.publish(state, self.generation);
        }
    }

    pub(crate) fn invalidate(&mut self) {
        // A failed COMMIT may have an ambiguous outcome. Keep the old published
        // snapshot, but reload durable rows before computing another command.
        self.working = None;
    }
}

impl crate::SqliteRouteStateStore {
    pub fn is_private_account_copy(&self) -> bool {
        self.store.is_projection()
    }

    /// Install the module's state owner before serving account work. Concurrent
    /// callers wait for the same initial publication instead of refusing work.
    pub fn install_account_state_bridge(
        &self,
        bridge: std::sync::Arc<dyn AccountStateBridge>,
    ) -> RouteStateResult<()> {
        if self.store.is_projection() {
            return Err(crate::err(
                "private projections do not publish account state",
            ));
        }
        if self.account_state.set(bridge).is_ok() {
            self.initialize_account_writer_state()?;
        }
        self.account_state.get().unwrap().snapshot().map(|_| ())
    }

    pub fn account_state_bridge(&self) -> Option<&std::sync::Arc<dyn AccountStateBridge>> {
        self.account_state.get()
    }

    /// Load the module's state again after an earlier load failed, through
    /// the same read-only barrier as the first load. The writer stays alive
    /// across load failures, so a caller can retry instead of restarting.
    ///
    /// Single flight: callers that arrive while a retry runs wait for it and
    /// share its result, instead of each reloading the whole state in turn.
    pub fn retry_account_state_load(&self) -> RouteStateResult<()> {
        self.retry_account_state_load_after(self.account_state_retry_count())
    }

    /// How many retries of the load have finished, for
    /// [`Self::retry_account_state_load_after`].
    pub fn account_state_retry_count(&self) -> u64 {
        self.account_state_retries.load(std::sync::atomic::Ordering::Acquire)
    }

    /// [`Self::retry_account_state_load`] for a caller that saw the load
    /// fail after `arrived` retries had finished: a retry that finished
    /// since (one this caller may have waited on) answers it instead of a
    /// new load.
    pub fn retry_account_state_load_after(&self, arrived: u64) -> RouteStateResult<()> {
        use std::sync::atomic::Ordering;
        let mut flight = self.account_state_retry.lock().unwrap();
        if self.account_state_retries.load(Ordering::Acquire) != arrived {
            // A retry finished while this caller waited for the lock.
            return match flight.as_ref() {
                None => Ok(()),
                Some(error) => Err(crate::err(error.clone())),
            };
        }
        if let Some(bridge) = self.account_state.get() {
            if bridge.snapshot().is_ok() {
                return Ok(());
            }
            bridge.request_reload();
        }
        let result = self.initialize_account_writer_state();
        *flight = result.as_ref().err().map(ToString::to_string);
        self.account_state_retries.fetch_add(1, Ordering::Release);
        result
    }

    /// Refuse a store-level writer that changes the admission group's tables
    /// without the module's in-memory state. Once the module has installed
    /// its state, those rows change only through its state-aware commands.
    pub(crate) fn refuse_state_blind_admission_write(&self, writer: &str) -> RouteStateResult<()> {
        if self.account_state.get().is_some() {
            return Err(crate::err(format!(
                "{writer} writes admission, affinity or continuation rows without the module's \
                 account state; use the module's state-aware writer command"
            )));
        }
        Ok(())
    }

    /// Test seeds may write account rows directly on the writer connection,
    /// which `PRAGMA data_version` does not report to that same connection.
    /// After such a write, reload and republish the module's state before
    /// anything reads it, as a restart would. Runs as a read-only barrier, so
    /// it adds no write transaction or command count.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn reload_account_state_after_direct_write(&self) -> RouteStateResult<()> {
        let Some(bridge) = self.account_state.get() else {
            return Ok(());
        };
        bridge.request_reload();
        self.account_writer_engine()
            .enqueue_inner(|_, _| Ok(()), true)?
            .wait()
    }

    /// Reload and republish the module's state now, as the next writer batch
    /// would after another connection's commit. For tests that write the
    /// store through their own connection, as another process would.
    #[cfg(any(test, feature = "test-support"))]
    pub fn reload_account_state_for_test(&self) -> RouteStateResult<()> {
        self.reload_account_state_after_direct_write()
    }

    /// Before a test reads the published state, reload it if a test seed
    /// wrote a block table outside an account command since the last batch.
    /// Production waits for the next batch instead, so a reader never blocks
    /// on the writer. The writer thread itself never waits on its own queue.
    #[cfg(any(test, feature = "test-support"))]
    pub fn reload_account_state_if_pending_for_test(&self) -> RouteStateResult<()> {
        if !self.account_state_reload_pending()
            || self.account_state.get().is_none()
            || std::thread::current().name() == Some("account-writer")
        {
            return Ok(());
        }
        self.account_writer_engine()
            .enqueue_inner(|_, _| Ok(()), true)?
            .wait()
    }

    /// Write a block row outside any account command and outside the test
    /// seeding seams, as a production bypass would.
    #[cfg(any(test, feature = "test-support"))]
    pub fn bypass_account_writer_for_test(&self, sql: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|conn: &rusqlite::Connection| conn.execute_batch(sql))
            .map_err(crate::err)
    }
}
