//! Immutable account snapshots. Each migrated table group is authoritative in
//! this state; unmigrated groups continue to use their existing SQL path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use striatum_core::route_state_store::{RouteStateResult, RouteStateStoreError};
use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDemandRow, AccountStateBridge, AccountTx,
    AccountWorkingState, AffinityRetentionBatch, SqliteRouteStateStore, V1Recorded,
};

mod admissions;

pub(crate) use admissions::AdmissionState;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountState {
    pub(crate) generation: u64,
    selection_salt: Option<Arc<String>>,
    /// Maintained by every admission, affinity and continuation writer and
    /// read by request paths instead of those tables.
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

/// The sanctioned request-path SQL reads of the admission group: a primary-key
/// lookup of an admission the state does not hold because it is closed or
/// absent, or of a continuation's stored bodies, which the state never holds.
/// Their SQL carries `striatum_store::KEYED_EXCEPTION`. Every other request
/// read of the group is answered from the state.
static KEYED_EXCEPTION_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn keyed_exception_reads() -> u64 {
    KEYED_EXCEPTION_READS.load(Ordering::Relaxed)
}

fn keyed_exception() {
    KEYED_EXCEPTION_READS.fetch_add(1, Ordering::Relaxed);
}

/// Before an account request runs, look its admission up in the published
/// state. An open admission answers from memory. Otherwise one keyed SQL
/// exception tells a closed admission (whose body only SQL keeps) from an
/// absent one. Returns the row and whether it was open. A private projection
/// has no state and reads SQL.
///
/// The keyed read runs here, on a pooled reader, not inside the admission's
/// writer command: a replay must answer before roster preparation (which may
/// refuse), and probing inside the writer would cost a second command. The
/// command itself checks only open admissions in memory. If an identical
/// request admits and closes the id in between, the late INSERT conflicts on
/// the primary key and fails, and the retry replays the closed admission.
pub(crate) fn preflight_admission(
    store: &SqliteRouteStateStore,
    principal: &str,
    admission_id: &str,
) -> RouteStateResult<Option<(AccountAdmissionRow, bool)>> {
    if store.is_account_evaluation_projection() {
        return store
            .account_read(|tx| tx.admission(principal, admission_id))
            .map(|row| row.map(|row| (row, false)));
    }
    if let Some(open) = snapshot(store)?
        .admissions
        .open_admission(principal, admission_id)
    {
        return Ok(Some((open.row.clone(), true)));
    }
    keyed_exception();
    store
        .account_read(|tx| tx.admission_by_key(principal, admission_id))
        .map(|row| row.map(|row| (row, false)))
}

/// A continuation's stored request and answer, read before its command: the
/// bodies live only in SQL, so this is always one keyed exception.
pub(crate) fn preflight_continuation(
    store: &SqliteRouteStateStore,
    principal: &str,
    selection: &str,
) -> RouteStateResult<Option<V1Recorded>> {
    if store.is_account_evaluation_projection() {
        return store.account_read(|tx| tx.continuation(principal, selection));
    }
    keyed_exception();
    store.account_read(|tx| tx.continuation_by_key(principal, selection))
}

/// The published state's answer to `has_expired_continuations`.
pub(crate) fn has_expired_continuations(
    store: &SqliteRouteStateStore,
    now: i64,
) -> RouteStateResult<bool> {
    if store.is_account_evaluation_projection() {
        return store.account_read(|tx| tx.has_expired_continuations(now));
    }
    Ok(snapshot(store)?.admissions.has_expired_offers(now))
}

/// The published state's answer to `has_expired_admission_probes`. The probe
/// authorizations are not in memory, so their owners come from SQL; whether
/// each owner is an expired unstarted admission comes from the state.
pub(crate) fn has_expired_admission_probes(
    store: &SqliteRouteStateStore,
    now: i64,
) -> RouteStateResult<bool> {
    if store.is_account_evaluation_projection() {
        return store.account_read(|tx| tx.has_expired_admission_probes(now));
    }
    let state = snapshot(store)?;
    let owners = store.account_read(|tx| tx.probe_authorization_owners())?;
    Ok(!state.admissions.expired_probe_admissions(owners, now).is_empty())
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

    fn request_reload(&self) {
        self.reload.store(true, Ordering::Release);
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

// Reads of the admission group. These shadow the `AccountTx` methods of the
// same name reached through `Deref`, so a command answers them from its
// working state, which already holds every earlier change of its batch.
// Without a state (private SQL projections) they are the SQL reads.
impl AccountCommand<'_> {
    fn admission_state(&self) -> Option<&AdmissionState> {
        self.state.as_deref().map(|state| &state.admissions)
    }

    /// An open admission from the state; a closed or absent one costs one
    /// keyed exception.
    pub(crate) fn admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        let Some(state) = self.admission_state() else {
            return self.sql.admission(principal, admission_id);
        };
        if let Some(open) = state.open_admission(principal, admission_id) {
            return Ok(Some(open.row.clone()));
        }
        keyed_exception();
        self.sql.admission_by_key(principal, admission_id)
    }

    /// The admission only if it is open (`reserved`, `started`, `expired`),
    /// for a caller that has already handled closed ones, or for whom a
    /// closed one means the same as none. No SQL read with a state.
    pub(crate) fn open_admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        match self.admission_state() {
            Some(state) => Ok(state
                .open_admission(principal, admission_id)
                .map(|open| open.row.clone())),
            None => Ok(self
                .sql
                .admission(principal, admission_id)?
                .filter(|row| matches!(row.state.as_str(), "reserved" | "started" | "expired"))),
        }
    }

    /// Serve's idempotency check. The caller looked the admission up before
    /// its command, so with a state only an admission opened since then (a
    /// concurrent identical request) can be found here; a closed one would
    /// make the new row's INSERT fail and the retry replay it. Without a
    /// state, the SQL row in any state.
    pub(crate) fn existing_admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        match self.admission_state() {
            Some(_) => self.open_admission(principal, admission_id),
            None => self.sql.admission(principal, admission_id),
        }
    }

    /// The size class an open admission's probe was scoped to. A caller
    /// finishing the admission reads it first, while it is still open.
    pub(crate) fn admission_size_class(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<u32>> {
        match self.admission_state() {
            Some(state) => Ok(state
                .open_admission(principal, admission_id)
                .and_then(|open| open.input_size_class)),
            None => self.sql.admission_size_class(principal, admission_id),
        }
    }

    pub(crate) fn affinity(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
    ) -> RouteStateResult<Option<AccountAffinityRow>> {
        match self.admission_state() {
            Some(state) => Ok(state.affinity(principal, hash, provider).cloned()),
            None => self.sql.affinity(principal, hash, provider),
        }
    }

    pub(crate) fn pin_has_started(&self, affinity: &AccountAffinityRow) -> RouteStateResult<bool> {
        match self.admission_state() {
            Some(state) => Ok(state.pin_has_started(affinity)),
            None => self.sql.pin_has_started(affinity),
        }
    }

    pub(crate) fn parent_used_account(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
        now: i64,
    ) -> RouteStateResult<Option<String>> {
        match self.admission_state() {
            Some(state) => Ok(state.parent_used_account(principal, hash, provider, now)),
            None => self.sql.parent_used_account(principal, hash, provider, now),
        }
    }

    pub(crate) fn demand(
        &self,
        provider: &str,
        account_id: &str,
        committed_after: Option<i64>,
    ) -> RouteStateResult<Vec<AccountDemandRow>> {
        match self.admission_state() {
            Some(state) => Ok(state.demand(provider, account_id, committed_after)),
            None => self.sql.demand(provider, account_id, committed_after),
        }
    }

    /// A continuation's stored bodies. With a state, only an open offer can
    /// have appeared since the caller's keyed preflight read; another record
    /// would make the new row's INSERT fail and the retry replay it.
    pub(crate) fn continuation(
        &self,
        principal: &str,
        selection: &str,
    ) -> RouteStateResult<Option<V1Recorded>> {
        let Some(state) = self.admission_state() else {
            return self.sql.continuation(principal, selection);
        };
        if !state.offers.contains_key(&(principal.to_owned(), selection.to_owned())) {
            return Ok(None);
        }
        keyed_exception();
        self.sql.continuation_by_key(principal, selection)
    }

    pub(crate) fn continued_reservation(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        decision: &str,
    ) -> RouteStateResult<Option<String>> {
        match self.admission_state() {
            Some(state) => Ok(state.continued_reservation(principal, admission_id, run_id, decision)),
            None => self
                .sql
                .continued_reservation(principal, admission_id, run_id, decision),
        }
    }

    pub(crate) fn continuation_has_used_successor(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        previous: &str,
    ) -> RouteStateResult<bool> {
        match self.admission_state() {
            Some(state) => Ok(state.has_used_successor(principal, admission_id, run_id, previous)),
            None => self
                .sql
                .continuation_has_used_successor(principal, admission_id, run_id, previous),
        }
    }

    /// The reservation admission of the original's newest used offer. The
    /// chain names it; an open reservation comes from the state and a closed
    /// one costs one keyed exception.
    pub(crate) fn latest_used_continuation(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        let Some(state) = self.admission_state() else {
            return self.sql.latest_used_continuation(principal, admission_id);
        };
        match state.latest_used_reservation(principal, admission_id) {
            Some(reservation) => self.admission(principal, &reservation),
            None => Ok(None),
        }
    }
}

// Each admission, affinity and continuation writer runs its SQL and then the
// same change on the working state, inside one writer command. These methods
// shadow the `AccountTx` methods of the same name reached through `Deref`, so a
// module command cannot write those tables without maintaining the state.
// The continuation transitions are the reverse: the state decides them and its
// decided writes are persisted by key. Without a state (private SQL
// projections) they are the SQL writers alone.
impl AccountCommand<'_> {
    fn admissions(&mut self) -> Option<&mut AdmissionState> {
        self.state.as_deref_mut().map(|state| &mut state.admissions)
    }

    /// Apply a change SQL has already made to the state.
    fn mirror(&mut self, change: impl FnOnce(&mut AdmissionState)) {
        if let Some(state) = self.admissions() {
            change(state);
            state.pending.clear();
        }
    }

    /// Decide a change on the state, then persist the writes it decided.
    fn decide<T>(&mut self, change: impl FnOnce(&mut AdmissionState) -> T) -> RouteStateResult<T> {
        let sql = self.sql;
        let state = self.admissions().expect("decide needs a working state");
        let decided = change(state);
        for effect in std::mem::take(&mut state.pending) {
            match effect {
                admissions::Effect::Admission { row, from } => {
                    if !sql.set_admission_lifecycle(&row, &from)? {
                        tracing::warn!(
                            admission = %row.admission_id,
                            "account state expected admission in state {from}; SQL row differs"
                        );
                    }
                }
                admissions::Effect::LeaseEnded { conversation, at } => {
                    sql.set_affinity_lease_end(&conversation.0, &conversation.1, &conversation.2, at)?
                }
                admissions::Effect::Offer {
                    selection,
                    used_at,
                    released_at,
                } => sql.settle_continuation_offer(&selection.0, &selection.1, used_at, released_at)?,
            }
        }
        Ok(decided)
    }

    pub(crate) fn insert_admission(&mut self, row: &AccountAdmissionRow) -> RouteStateResult<()> {
        self.sql.insert_admission(row)?;
        let rowid = self.sql.last_insert_rowid();
        self.mirror(|state| state.insert_admission(row, rowid));
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
            self.mirror(|state| state.set_probe_scope(principal, id, size, decision));
        }
        Ok(())
    }

    pub(crate) fn mark_admission_expired(
        &mut self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<bool> {
        let changed = self.sql.mark_admission_expired(principal, admission_id)?;
        self.mirror(|state| {
            state.expire(principal, admission_id);
        });
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
        self.mirror(|state| {
            state.start(principal, admission_id, run_id, at, late);
        });
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
        self.mirror(|state| {
            state.release(principal, admission_id, at);
        });
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
        self.mirror(|state| {
            state.finish(principal, admission_id, at);
        });
        Ok(changed)
    }

    pub(crate) fn put_affinity(
        &mut self,
        row: &AccountAffinityRow,
        expected: Option<(&str, u64)>,
    ) -> RouteStateResult<bool> {
        let changed = self.sql.put_affinity(row, expected)?;
        self.mirror(|state| {
            state.put_affinity(row, expected);
        });
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
        self.mirror(|state| {
            state.touch_affinity(
                principal,
                conversation_hash,
                provider,
                expected,
                last_used_at,
                expires_at,
                lease_valid_until,
            );
        });
        Ok(changed)
    }

    pub(crate) fn end_lease(&mut self, pin_id: &str, at: i64) -> RouteStateResult<bool> {
        let changed = self.sql.end_lease(pin_id, at)?;
        self.mirror(|state| {
            state.end_lease(pin_id, at);
        });
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
        self.mirror(|state| {
            state.record_continuation(principal, selection, run, admission, previous, offer, rowid)
        });
        Ok(())
    }

    pub(crate) fn release_expired_continuations(&mut self, now: i64) -> RouteStateResult<()> {
        if self.state.is_none() {
            return self.sql.release_expired_continuations(now);
        }
        self.decide(|state| state.release_expired_continuations(now))
    }

    pub(crate) fn use_continuation(&mut self, decision: &str, now: i64) -> RouteStateResult<()> {
        if self.state.is_none() {
            return self.sql.use_continuation(decision, now);
        }
        self.decide(|state| state.use_continuation(decision, now))
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
        let Some(state) = self.admission_state() else {
            return self
                .sql
                .use_observed_continuation(principal, admission, run, account, credential, now);
        };
        let Some(decision) =
            state.observed_continuation(principal, admission, run, account, credential, now)
        else {
            return Ok(false);
        };
        self.decide(|state| state.use_continuation(&decision, now))?;
        Ok(true)
    }

    pub(crate) fn finish_continuations(
        &mut self,
        principal: &str,
        admission: &str,
        run: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if self.state.is_none() {
            return self.sql.finish_continuations(principal, admission, run, now);
        }
        self.decide(|state| state.finish_continuations(principal, admission, run, now))
    }

    /// Fence probe authorizations are not in memory yet, so SQL supplies
    /// their owners; the state decides which name expired unstarted
    /// admissions, and SQL releases those probes and expires the admissions.
    pub(crate) fn release_expired_admission_probes(&mut self, now: i64) -> RouteStateResult<()> {
        let Some(state) = self.admission_state() else {
            self.sql.release_expired_admission_probes(now)?;
            return Ok(());
        };
        let expired = state.expired_probe_admissions(self.sql.probe_authorization_owners()?, now);
        let marked = self.sql.release_admission_probes(expired, now)?;
        self.mirror(|state| {
            for (principal, admission) in marked {
                state.expire(&principal, &admission);
            }
        });
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
            state.pending.clear();
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
            state.pending.clear();
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
            state.pending.clear();
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
         fork_ns/op={fork_ns} load_us/op={} keyed_exception_reads={} maintenance_reads={}",
        state.open.len(),
        state.demand.len(),
        state.latest.len(),
        state.affinity.len(),
        state.pin_started.len(),
        state.started_by_pin.len(),
        state.offers.len(),
        state.chains.len(),
        start.elapsed().as_micros() / 10,
        keyed_exception_reads(),
        maintenance_reads(),
    );
}

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod tests;
