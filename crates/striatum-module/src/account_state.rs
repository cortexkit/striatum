//! Immutable account snapshots. Each migrated table group is authoritative in
//! this state; unmigrated groups continue to use their existing SQL path.

use std::cell::RefCell;
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

struct WorkingState(RefCell<AccountState>);

impl AccountWorkingState for WorkingState {
    fn fork(&self) -> Box<dyn AccountWorkingState> {
        Box::new(Self(RefCell::new(self.0.borrow().clone())))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn selection_salt(&self) -> Option<String> {
        self.0.borrow().selection_salt.as_deref().cloned()
    }

    fn set_selection_salt(&self, salt: String) {
        self.0.borrow_mut().selection_salt = Some(Arc::new(salt));
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
        AccountState::load(tx).map(|state| Box::new(WorkingState(RefCell::new(state))) as _)
    }

    fn publish(&self, state: &dyn AccountWorkingState, generation: u64) {
        let mut state = state
            .as_any()
            .downcast_ref::<WorkingState>()
            .unwrap()
            .0
            .borrow()
            .clone();
        state.generation = generation;
        *self.published.write().unwrap() = Some(Arc::new(state));
        *self.ready.0.lock().unwrap() = Some(Ok(()));
        self.ready.1.notify_all();
    }

    fn snapshot(&self) -> RouteStateResult<Box<dyn AccountWorkingState>> {
        if let Some(snapshot) = self.published.read().unwrap().clone() {
            return Ok(Box::new(WorkingState(RefCell::new((*snapshot).clone()))));
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
        Ok(Box::new(WorkingState(RefCell::new((*snapshot).clone()))))
    }

    fn failed(&self, error: &str) {
        *self.ready.0.lock().unwrap() = Some(Err(error.to_owned()));
        self.ready.1.notify_all();
    }

    fn reload_requested(&self) -> bool {
        self.reload.swap(false, Ordering::AcqRel)
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
