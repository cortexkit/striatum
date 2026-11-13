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
}

pub(crate) struct WriterAccountState {
    bridge: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn AccountStateBridge>>>,
    pub(crate) working: Option<Box<dyn AccountWorkingState>>,
    data_version: Option<i64>,
    generation: u64,
}

impl WriterAccountState {
    pub(crate) fn new(
        bridge: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn AccountStateBridge>>>,
    ) -> Self {
        Self {
            bridge,
            working: None,
            data_version: None,
            generation: 0,
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
            let reload = bridge.reload_requested();
            if !reload && self.working.is_some() && self.data_version == Some(version) {
                return Ok(None);
            }
            let tx =
                rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Deferred)
                    .map_err(crate::err)?;
            let state = bridge.load(&AccountTx { conn: &tx })?;
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
                bridge.failed(&error.to_string());
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
    pub fn is_account_evaluation_projection(&self) -> bool {
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
}
