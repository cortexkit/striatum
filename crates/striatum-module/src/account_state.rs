//! Immutable account snapshots. Each migrated table group is authoritative in
//! this state; unmigrated groups continue to use their existing SQL path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use striatum_core::route_state_store::{RouteStateResult, RouteStateStoreError};
use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountStateBridge, AccountTx, AccountWorkingState,
    AffinityRetentionBatch, SqliteRouteStateStore, V1Recorded,
};

mod admissions;

pub(crate) use admissions::AdmissionState;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountState {
    pub(crate) generation: u64,
    selection_salt: Option<Arc<String>>,
    /// Maintained by every admission, affinity and continuation writer; not
    /// yet read by request paths.
    pub(crate) admissions: AdmissionState,
}

impl AccountState {
    // The writer calls this once inside a read transaction, with no attached
    // working state. Quota readings and the vault roster are deliberately absent.
    fn load(tx: &AccountTx<'_>) -> RouteStateResult<Self> {
        Ok(Self {
            generation: 0,
            selection_salt: tx.existing_salt()?.map(Arc::new),
            admissions: AdmissionState::from_rows(tx.load_admission_group()?),
        })
    }
}

/// Keyed SQL reads a pruning command makes to recompute a derived index whose
/// source row it deleted. Counted apart from request-path reads, which the
/// admission state must not need.
static MAINTENANCE_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn maintenance_reads() -> u64 {
    MAINTENANCE_READS.load(Ordering::Relaxed)
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

// Each admission, affinity and continuation writer runs its SQL and then the
// same change on the working state, inside one writer command. These methods
// shadow the `AccountTx` methods of the same name reached through `Deref`, so a
// module command cannot write those tables without maintaining the state.
// Without a state (private SQL projections) they are the SQL writers alone.
impl AccountCommand<'_> {
    fn admissions(&mut self) -> Option<&mut AdmissionState> {
        self.state.as_deref_mut().map(|state| &mut state.admissions)
    }

    pub(crate) fn insert_admission(&mut self, row: &AccountAdmissionRow) -> RouteStateResult<()> {
        self.sql.insert_admission(row)?;
        let rowid = self.sql.last_insert_rowid();
        if let Some(state) = self.admissions() {
            state.insert_admission(row, rowid);
        }
        Ok(())
    }

    pub(crate) fn record_admission_probe_scope(
        &mut self,
        principal: &str,
        id: &str,
        size: u32,
        decision: Option<&str>,
    ) -> RouteStateResult<()> {
        if self
            .sql
            .record_admission_probe_scope(principal, id, size, decision)?
        {
            if let Some(state) = self.admissions() {
                state.set_probe_scope(principal, id, size, decision);
            }
        }
        Ok(())
    }

    pub(crate) fn mark_admission_expired(
        &mut self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<bool> {
        let changed = self.sql.mark_admission_expired(principal, admission_id)?;
        if let Some(state) = self.admissions() {
            state.expire(principal, admission_id);
        }
        Ok(changed)
    }

    pub(crate) fn mark_admission_started(
        &mut self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        at: i64,
        late: bool,
    ) -> RouteStateResult<bool> {
        let changed = self
            .sql
            .mark_admission_started(principal, admission_id, run_id, at, late)?;
        if let Some(state) = self.admissions() {
            state.start(principal, admission_id, run_id, at, late);
        }
        Ok(changed)
    }

    pub(crate) fn mark_admission_released(
        &mut self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        let changed = self
            .sql
            .mark_admission_released(principal, admission_id, at)?;
        if let Some(state) = self.admissions() {
            state.release(principal, admission_id, at);
        }
        Ok(changed)
    }

    pub(crate) fn mark_admission_finished(
        &mut self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        let changed = self
            .sql
            .mark_admission_finished(principal, admission_id, at)?;
        if let Some(state) = self.admissions() {
            state.finish(principal, admission_id, at);
        }
        Ok(changed)
    }

    pub(crate) fn put_affinity(
        &mut self,
        row: &AccountAffinityRow,
        expected: Option<(&str, u64)>,
    ) -> RouteStateResult<bool> {
        let changed = self.sql.put_affinity(row, expected)?;
        if let Some(state) = self.admissions() {
            state.put_affinity(row, expected);
        }
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn touch_affinity(
        &mut self,
        principal: &str,
        conversation_hash: &str,
        provider: &str,
        expected: (&str, u64),
        last_used_at: i64,
        expires_at: i64,
        lease_valid_until: Option<i64>,
    ) -> RouteStateResult<bool> {
        let changed = self.sql.touch_affinity(
            principal,
            conversation_hash,
            provider,
            expected,
            last_used_at,
            expires_at,
            lease_valid_until,
        )?;
        if let Some(state) = self.admissions() {
            state.touch_affinity(
                principal,
                conversation_hash,
                provider,
                expected,
                last_used_at,
                expires_at,
                lease_valid_until,
            );
        }
        Ok(changed)
    }

    pub(crate) fn end_lease(&mut self, pin_id: &str, at: i64) -> RouteStateResult<bool> {
        let changed = self.sql.end_lease(pin_id, at)?;
        if let Some(state) = self.admissions() {
            state.end_lease(pin_id, at);
        }
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_continuation(
        &mut self,
        principal: &str,
        selection: &str,
        record: &V1Recorded,
        run: &str,
        admission: &str,
        previous: &str,
        offer: Option<(&str, &str, i64)>,
    ) -> RouteStateResult<()> {
        self.sql
            .record_continuation(principal, selection, record, run, admission, previous, offer)?;
        let rowid = self.sql.last_insert_rowid();
        if let Some(state) = self.admissions() {
            state.record_continuation(principal, selection, run, admission, previous, offer, rowid);
        }
        Ok(())
    }

    pub(crate) fn release_expired_continuations(&mut self, now: i64) -> RouteStateResult<()> {
        self.sql.release_expired_continuations(now)?;
        if let Some(state) = self.admissions() {
            state.release_expired_continuations(now);
        }
        Ok(())
    }

    pub(crate) fn use_continuation(&mut self, decision: &str, now: i64) -> RouteStateResult<()> {
        self.sql.use_continuation(decision, now)?;
        if let Some(state) = self.admissions() {
            state.use_continuation(decision, now);
        }
        Ok(())
    }

    pub(crate) fn use_observed_continuation(
        &mut self,
        principal: &str,
        admission: &str,
        run: &str,
        account: &str,
        credential: Option<&str>,
        now: i64,
    ) -> RouteStateResult<bool> {
        let chosen = self.state.as_deref().and_then(|state| {
            state
                .admissions
                .observed_continuation(principal, admission, run, account, credential, now)
        });
        let used = self
            .sql
            .use_observed_continuation(principal, admission, run, account, credential, now)?;
        if let Some(state) = self.admissions() {
            if let Some(decision) = chosen {
                state.use_continuation(&decision, now);
            }
        }
        Ok(used)
    }

    pub(crate) fn finish_continuations(
        &mut self,
        principal: &str,
        admission: &str,
        run: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        self.sql
            .finish_continuations(principal, admission, run, now)?;
        if let Some(state) = self.admissions() {
            state.finish_continuations(principal, admission, run, now);
        }
        Ok(())
    }

    /// Which admissions expire depends on fence probe authorizations, which
    /// are not in memory yet, so SQL decides and returns the admissions it
    /// expired; the state applies the same expiry to each.
    pub(crate) fn release_expired_admission_probes(&mut self, now: i64) -> RouteStateResult<()> {
        let expired = self.sql.release_expired_admission_probes(now)?;
        if let Some(state) = self.admissions() {
            for (principal, admission) in expired {
                state.expire(&principal, &admission);
            }
        }
        Ok(())
    }

    pub(crate) fn tick(&mut self, now_ms: i64) -> RouteStateResult<()> {
        self.release_expired_admission_probes(now_ms)?;
        self.release_expired_continuations(now_ms)?;
        self.sql.sync_fence_rechecks()
    }

    pub(crate) fn prune_account_admissions(
        &mut self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        let deleted = self.sql.prune_account_admissions_returning(before, limit)?;
        let sql = self.sql;
        if let Some(state) = self.admissions() {
            for conversation in state.prune_admissions(&deleted) {
                MAINTENANCE_READS.fetch_add(1, Ordering::Relaxed);
                let latest = sql.latest_admission_for_conversation(
                    &conversation.0,
                    &conversation.1,
                    &conversation.2,
                )?;
                state.set_latest(conversation, latest);
            }
        }
        Ok(deleted.len())
    }

    pub(crate) fn prune_account_affinities(
        &mut self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<AffinityRetentionBatch> {
        let (batch, deleted) = self.sql.prune_account_affinities_returning(before, limit)?;
        if let Some(state) = self.admissions() {
            state.prune_affinities(&deleted);
        }
        Ok(batch)
    }

    pub(crate) fn prune_account_continuations(
        &mut self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        let deleted = self
            .sql
            .prune_account_continuations_returning(before, limit)?;
        let sql = self.sql;
        if let Some(state) = self.admissions() {
            for original in state.prune_continuations(&deleted) {
                MAINTENANCE_READS.fetch_add(1, Ordering::Relaxed);
                let latest = sql.latest_used_continuation_row(&original.0, &original.1)?;
                state.set_latest_used(&original, latest);
            }
        }
        Ok(deleted.len())
    }
}

impl Drop for AccountCommand<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.admissions() {
            state.settle();
        }
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

/// After a request mix: the published admission state must equal a fresh
/// load of the rows that mix wrote. Prints its index sizes, the cost of a
/// writer fork of it, and the load time.
#[cfg(test)]
pub(crate) fn report_admission_state(store: &SqliteRouteStateStore) {
    let published = snapshot(store).unwrap();
    let loaded = store.account_transaction(AccountState::load).unwrap();
    assert_eq!(
        loaded.admissions, published.admissions,
        "the request mix left the admission state unequal to a fresh load"
    );
    let state = &published.admissions;
    let demand_entries: usize = state.demand.iter().map(|(_, rows)| rows.len()).sum();
    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        std::hint::black_box(published.fork());
    }
    let fork_ns = start.elapsed().as_nanos() / 10_000;
    let start = std::time::Instant::now();
    for _ in 0..10 {
        std::hint::black_box(store.account_transaction(AccountState::load).unwrap());
    }
    println!(
        "account_state admissions: open={} demand_accounts={} demand_entries={demand_entries} \
         latest={} affinity={} pin_started={} started_pins={} offers={} chains={} \
         fork_ns/op={fork_ns} load_us/op={}",
        state.open.len(),
        state.demand.len(),
        state.latest.len(),
        state.affinity.len(),
        state.pin_started.len(),
        state.started_by_pin.len(),
        state.offers.len(),
        state.chains.len(),
        start.elapsed().as_micros() / 10,
    );
}

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod tests;
