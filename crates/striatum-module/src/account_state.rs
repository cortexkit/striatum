//! Immutable account snapshots. Each migrated table group is authoritative in
//! this state; unmigrated groups continue to use their existing SQL path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use striatum_core::route_state_store::{RouteStateResult, RouteStateStoreError};
use striatum_store::{AccountStateBridge, AccountTx, AccountWorkingState, SqliteRouteStateStore};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountState {
    pub(crate) generation: u64,
    selection_salt: Option<Arc<String>>,
}

impl AccountState {
    // The writer calls this once inside a read transaction, with no attached
    // working state. Quota readings and the vault roster are deliberately absent.
    fn load(tx: &AccountTx<'_>) -> RouteStateResult<Self> {
        Ok(Self {
            generation: 0,
            selection_salt: tx.existing_salt()?.map(Arc::new),
        })
    }
}

impl AccountWorkingState for AccountState {
    fn fork(&self) -> Box<dyn AccountWorkingState> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Publication swaps one Arc under a short write lock;
/// readers only hold the read lock long enough to clone it. This provides
/// atomic snapshot publication without another synchronization dependency.
#[derive(Default)]
struct StateOwner {
    published: RwLock<Option<Arc<AccountState>>>,
    ready: (Mutex<Option<Result<(), String>>>, Condvar),
    reload: AtomicBool,
    #[cfg(test)]
    loads: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    load_hook: Mutex<Option<Box<dyn Fn() -> RouteStateResult<()> + Send>>>,
}

impl AccountStateBridge for StateOwner {
    fn load(&self, tx: &AccountTx<'_>) -> RouteStateResult<Box<dyn AccountWorkingState>> {
        #[cfg(test)]
        {
            self.loads.fetch_add(1, Ordering::Relaxed);
            if let Some(hook) = self.load_hook.lock().unwrap().as_ref() {
                hook()?;
            }
        }
        AccountState::load(tx).map(|state| Box::new(state) as _)
    }

    fn publish(&self, state: &dyn AccountWorkingState, generation: u64) {
        let mut state = state
            .as_any()
            .downcast_ref::<AccountState>()
            .unwrap()
            .clone();
        state.generation = generation;
        *self.published.write().unwrap() = Some(Arc::new(state));
        *self.ready.0.lock().unwrap() = Some(Ok(()));
        self.ready.1.notify_all();
    }

    fn snapshot(&self) -> RouteStateResult<Arc<dyn std::any::Any + Send + Sync>> {
        self.published_snapshot().map(|snapshot| snapshot as _)
    }

    fn failed(&self, error: &str) {
        *self.ready.0.lock().unwrap() = Some(Err(error.to_owned()));
        self.ready.1.notify_all();
    }

    fn reload_requested(&self) -> bool {
        self.reload.swap(false, Ordering::AcqRel)
    }
}

impl StateOwner {
    fn published_snapshot(&self) -> RouteStateResult<Arc<AccountState>> {
        if let Some(snapshot) = self.published.read().unwrap().clone() {
            return Ok(snapshot);
        }
        let mut ready = self.ready.0.lock().unwrap();
        while ready.is_none() {
            ready = self.ready.1.wait(ready).unwrap();
        }
        if let Some(Err(error)) = ready.as_ref() {
            return Err(RouteStateStoreError(error.clone()));
        }
        drop(ready);
        let snapshot = self
            .published
            .read()
            .unwrap()
            .clone()
            .expect("ready state must be published");
        Ok(snapshot)
    }
}

pub(crate) fn snapshot(store: &SqliteRouteStateStore) -> RouteStateResult<Arc<AccountState>> {
    initialize(store)?;
    store
        .account_state_bridge()
        .unwrap()
        .snapshot()?
        .downcast::<AccountState>()
        .map_err(|_| RouteStateStoreError("account state publisher has the wrong type".into()))
}

/// A module command computes on typed working state, then persists its effects
/// through the store transaction. Only private SQL projections omit the state.
pub(crate) struct AccountCommand<'a> {
    sql: &'a AccountTx<'a>,
    state: Option<&'a mut AccountState>,
}

impl<'a> AccountCommand<'a> {
    pub(crate) fn new(
        sql: &'a AccountTx<'a>,
        state: Option<&'a mut (dyn AccountWorkingState + 'static)>,
    ) -> Self {
        Self {
            sql,
            state: state.map(|state| {
                state
                    .as_any_mut()
                    .downcast_mut::<AccountState>()
                    .expect("module working state")
            }),
        }
    }

    pub(crate) fn existing_salt(&self) -> RouteStateResult<Option<String>> {
        match &self.state {
            Some(state) => Ok(state.selection_salt.as_deref().cloned()),
            None => self.sql.existing_salt(),
        }
    }

    pub(crate) fn salt(
        &mut self,
        now: i64,
        fresh: impl FnOnce() -> String,
    ) -> RouteStateResult<String> {
        let Some(state) = self.state.as_deref_mut() else {
            return self.sql.salt(now, fresh);
        };
        if let Some(salt) = &state.selection_salt {
            return Ok(salt.as_ref().clone());
        }
        let salt = fresh();
        self.sql.insert_selection_salt(&salt, now)?;
        state.selection_salt = Some(Arc::new(salt.clone()));
        Ok(salt)
    }
}

impl<'a> std::ops::Deref for AccountCommand<'a> {
    type Target = AccountTx<'a>;
    fn deref(&self) -> &Self::Target {
        self.sql
    }
}

pub(crate) fn initialize(store: &SqliteRouteStateStore) -> RouteStateResult<()> {
    if let Some(bridge) = store.account_state_bridge() {
        return bridge.snapshot().map(|_| ());
    }
    store.install_account_state_bridge(Arc::new(StateOwner::default()))
}

#[cfg(test)]
mod tests;
