//! Immutable account snapshots. Each migrated table group is authoritative in
//! this state; unmigrated groups continue to use their existing SQL path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::{cell::RefCell, collections::BTreeSet};

use striatum_core::route_state_store::{RouteStateResult, RouteStateStoreError};
use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDemandRow, AccountStateBridge, AccountTx,
    AccountWorkingState, AffinityRetentionBatch, SqliteRouteStateStore, V1Recorded,
};

mod admissions;
mod blocks;
mod inflight;
mod waiters;

pub(crate) use admissions::AdmissionState;
pub(crate) use blocks::{published, BlockReads, Published};
pub(crate) use inflight::{InflightLedger, InflightScope};
pub(crate) use waiters::{matched as matched_waiters, CapCount, Waiter, Waiters};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountState {
    pub(crate) generation: u64,
    selection_salt: Option<Arc<String>>,
    /// Maintained by every admission, affinity and continuation writer and
    /// read by request paths instead of those tables.
    pub(crate) admissions: AdmissionState,
    pub(crate) inflight: InflightLedger,
    /// The block tables as SQL holds them. The writer copies every row a
    /// command changes into this state, so it always equals a fresh load.
    /// Request paths read it through `blocks::BlockReads` and `published`.
    pub(crate) blocks: striatum_store::BlockState,
    /// Conversations refused for concurrency that a free slot is held for.
    /// Memory only: a load starts with none.
    pub(crate) waiters: Waiters,
}

impl AccountState {
    /// This state as the two expiry releases leave it at `now`: the probes of
    /// unstarted admissions past their deadline released (their clocks
    /// reopened one generation on, their authorizations gone, the admissions
    /// expired), then the continuation offers past their deadline released.
    /// These are the transitions the Tick and every command make before
    /// deciding (`AccountCommand::release_expired`), applied in memory with
    /// the same decisions (`BlockState::apply_probe_release`,
    /// `AdmissionState::release_expired_continuations`). A reader sees an
    /// expired item released without sending a command.
    pub(crate) fn as_of(self: Arc<Self>, now: i64) -> Arc<Self> {
        let owners = if self.blocks.enabled() {
            self.blocks.authorization_owners()
        } else {
            Vec::new()
        };
        let probes = self.admissions.expired_probe_admissions(owners, now);
        if probes.is_empty() && !self.admissions.has_expired_offers(now) {
            return self;
        }
        let mut state = (*self).clone();
        for (principal, admission, owner) in probes {
            // Reopened as of the admission's own deadline, as the command
            // records it (`release_admission_probes`).
            let deadline = state
                .admissions
                .open_admission(&principal, &admission)
                .map_or(now, |open| open.row.admission_valid_until.min(now));
            state.blocks.apply_probe_release(&owner, deadline);
            state.admissions.expire(&principal, &admission);
        }
        state.admissions.release_expired_continuations(now);
        state.inflight.apply(&state.admissions, now);
        state.admissions.pending.clear();
        Arc::new(state)
    }

    // The writer calls this once inside a read transaction, with no attached
    // working state. Quota readings and the vault roster are deliberately absent.
    fn load(tx: &AccountTx<'_>) -> RouteStateResult<Self> {
        Self::load_at(tx, crate::account_selection::now_ms())
    }

    fn load_at(tx: &AccountTx<'_>, now: i64) -> RouteStateResult<Self> {
        let mut rows = tx.load_admission_group()?;
        let sources = std::mem::take(&mut rows.continuation_sources);
        let admissions = AdmissionState::from_rows(rows);
        let inflight = InflightLedger::from_admissions(&admissions, sources, now);
        Ok(Self {
            generation: 0,
            selection_salt: tx.existing_salt()?.map(Arc::new),
            admissions,
            inflight,
            blocks: tx.load_block_state()?,
            waiters: Waiters::default(),
        })
    }
}

/// Keyed SQL reads a pruning command makes to recompute a derived index whose
/// source row it deleted. Counted apart from request-path reads, which the
/// admission state must not need.
static MAINTENANCE_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CONTINUATION_SOURCE_READS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn continuation_source_reads() -> u64 {
    CONTINUATION_SOURCE_READS.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn maintenance_reads() -> u64 {
    MAINTENANCE_READS.load(Ordering::Relaxed)
}

/// The sanctioned request-path SQL reads of the admission group: a primary-key
/// lookup of an admission the state does not hold because it is closed or
/// absent, or of a continuation's stored bodies, which the state never holds.
/// Their SQL carries `striatum_store::KEYED_EXCEPTION`, and the store keeps
/// their count. Every other request read of the group is answered from the
/// state.
fn keyed_exception() {
    striatum_store::note_keyed_exception();
}

/// The error a command returns when SQL and its working state disagree. The
/// writer recognises it, rolls the command back and reloads the state.
fn diverged(detail: String) -> RouteStateStoreError {
    tracing::error!(%detail, "account state diverged from SQL; reloading");
    RouteStateStoreError(format!("{}: {detail}", striatum_store::STATE_DIVERGED))
}

/// The admission an account request names, if the published state holds it
/// open. No SQL read: an id that is not open is checked by key later, inside
/// the writer command or, if the request is refused before reaching the
/// writer, by [`closed_admission`]. A private projection has no state and
/// reads SQL, returning the row in any state.
pub(crate) fn open_preflight_admission(
    store: &SqliteRouteStateStore,
    principal: &str,
    admission_id: &str,
    now_ms: i64,
) -> RouteStateResult<Option<AccountAdmissionRow>> {
    if store.is_private_account_copy() {
        return store.account_read(|tx| tx.admission(principal, admission_id));
    }
    Ok(published_at(store, now_ms)?
        .admissions
        .open_admission(principal, admission_id)
        .map(|open| open.row.clone()))
}

/// Keyed exception for a request refused before its writer command: a
/// closed admission of the same id must still replay its stored answer. One
/// primary-key read on a pooled reader. A projection already read SQL in
/// [`open_preflight_admission`].
pub(crate) fn closed_admission(
    store: &SqliteRouteStateStore,
    principal: &str,
    admission_id: &str,
) -> RouteStateResult<Option<AccountAdmissionRow>> {
    if store.is_private_account_copy() {
        return Ok(None);
    }
    keyed_exception();
    store.account_read(|tx| tx.admission_by_key(principal, admission_id))
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

    fn apply_block_rows(&mut self, rows: &striatum_store::BlockRows) {
        self.blocks.apply(rows);
    }

    fn blocks(&self) -> Option<&striatum_store::BlockState> {
        Some(&self.blocks)
    }
}

/// The longest a reader waits for the first publication of the state.
const FIRST_LOAD_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// A shorter first-load bound a test sets, in milliseconds; 0 keeps
/// [`FIRST_LOAD_WAIT`].
#[cfg(test)]
static FIRST_LOAD_WAIT_FOR_TEST: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn first_load_wait() -> std::time::Duration {
    #[cfg(test)]
    match FIRST_LOAD_WAIT_FOR_TEST.load(Ordering::Relaxed) {
        0 => {}
        ms => return std::time::Duration::from_millis(ms),
    }
    FIRST_LOAD_WAIT
}

/// The start of the error a reader gets when the first load is still
/// running at the end of the wait.
const FIRST_LOAD_PENDING: &str = striatum_store::ACCOUNT_STATE_LOAD_PENDING;

/// Publication swaps one Arc under a short write lock;
/// readers only hold the read lock long enough to clone it. This provides
/// atomic snapshot publication without another synchronization dependency.
#[derive(Default)]
struct StateOwner {
    published: RwLock<Option<Arc<AccountState>>>,
    ready: (Mutex<Option<Result<(), String>>>, Condvar),
    reload: AtomicBool,
    #[cfg(test)]
    loads: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    load_hook: Mutex<Option<Box<dyn Fn() -> RouteStateResult<()> + Send>>>,
    #[cfg(test)]
    load_now_ms: Option<i64>,
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
        #[cfg(test)]
        if let Some(now) = self.load_now_ms {
            return AccountState::load_at(tx, now).map(|state| Box::new(state) as _);
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
        // Before anything is published, a reload is a new first load:
        // readers wait for it (within the bound) rather than answering the
        // failure of the load before it and queueing yet another.
        if self.published.read().unwrap().is_none() {
            *self.ready.0.lock().unwrap() = None;
        }
    }

    fn load_wait(&self) -> std::time::Duration {
        first_load_wait()
    }
}

impl StateOwner {
    fn published_snapshot(&self) -> RouteStateResult<Arc<AccountState>> {
        if let Some(snapshot) = self.published.read().unwrap().clone() {
            return Ok(snapshot);
        }
        // The writer reports every outcome of the first load, but a reader
        // never waits on it without a bound.
        let (ready, waited) = self
            .ready
            .1
            .wait_timeout_while(self.ready.0.lock().unwrap(), first_load_wait(), |ready| {
                ready.is_none()
            })
            .unwrap();
        if waited.timed_out() {
            return Err(RouteStateStoreError(format!(
                "{FIRST_LOAD_PENDING} {}ms; retry the operation",
                first_load_wait().as_millis()
            )));
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

/// The published state as request paths read it at `now`: expired probes
/// and continuation offers already released ([`AccountState::as_of`]).
/// [`snapshot`] is the state exactly as stored, for the writer and tests.
pub(crate) fn published_at(
    store: &SqliteRouteStateStore,
    now: i64,
) -> RouteStateResult<Arc<AccountState>> {
    Ok(snapshot(store)?.as_of(now))
}

#[cfg(test)]
pub(crate) static SNAPSHOT_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn snapshot(store: &SqliteRouteStateStore) -> RouteStateResult<Arc<AccountState>> {
    #[cfg(test)]
    SNAPSHOT_COUNT.fetch_add(1, Ordering::Relaxed);
    initialize(store)?;
    #[cfg(test)]
    {
        // A block-table write outside an account command that no test
        // seeding seam made is a bypass of the account writer: fail the test
        // rather than answer from a state that missed it.
        let bypasses = store.block_tables_written_outside_command();
        assert!(
            bypasses.is_empty(),
            "block tables written outside an account command (table, thread): {bypasses:?}"
        );
        store.reload_account_state_if_pending_for_test()?;
    }
    store
        .account_state_bridge()
        .unwrap()
        .snapshot()?
        .downcast::<AccountState>()
        .map_err(|_| RouteStateStoreError("account state publisher has the wrong type".into()))
}

#[cfg(test)]
pub(crate) fn assert_fresh_load_for_test(store: &SqliteRouteStateStore, now: i64, context: &str) {
    // Capture memory first: loading SQL must not repair the value being checked.
    let mut memory = (*snapshot(store).unwrap()).clone();
    memory.generation = 0;
    let loaded = store
        .account_transaction(|tx| AccountState::load_at(tx, now))
        .unwrap();
    assert_eq!(
        memory, loaded,
        "{context}: published account state differs from a fresh load"
    );
}

/// A module command computes on typed working state, then persists its effects
/// through the store transaction. Only private SQL projections omit the state.
pub(crate) struct AccountCommand<'a> {
    sql: &'a AccountTx<'a>,
    state: Option<&'a mut AccountState>,
    now_ms: i64,
    visited_accounts: RefCell<BTreeSet<(String, String)>>,
}

impl<'a> AccountCommand<'a> {
    pub(crate) fn new(
        sql: &'a AccountTx<'a>,
        state: Option<&'a mut (dyn AccountWorkingState + 'static)>,
    ) -> Self {
        Self {
            sql,
            now_ms: crate::account_selection::now_ms(),
            visited_accounts: RefCell::default(),
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
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter("account_command::salt");
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
    /// Counts only holds assigned to the requested model's effective count.
    pub(crate) fn inflight_count_for_model(
        &mut self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        now: i64,
    ) -> usize {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::inflight_count_for_model");
        let state = self
            .state
            .as_deref_mut()
            .expect("ledger needs module state");
        state.inflight.touch(provider, account, now);
        state
            .inflight
            .count_for_model(caps, provider, account, model, now)
    }

    /// When the request's cap count on `account` first falls below `cap`,
    /// read from the working state like the count itself.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn inflight_next_free_at(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        cap: u64,
        now: i64,
    ) -> Option<i64> {
        self.state.as_deref().and_then(|state| {
            state
                .inflight
                .next_free_at(caps, provider, account, model, cap, now)
        })
    }

    pub(crate) fn inflight_conversation(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        principal: &str,
        conversation: &str,
        now: i64,
    ) -> bool {
        self.state.as_deref().is_some_and(|state| {
            state.inflight.conversation(
                caps,
                provider,
                account,
                model,
                principal,
                conversation,
                now,
            )
        })
    }

    /// The ledger and the waiters of the working state, for the concurrency
    /// check. `None` when this view has no working state (a private copy
    /// built for a preview or a test), which keeps neither.
    pub(crate) fn concurrency_view(&self) -> Option<(&InflightLedger, &Waiters)> {
        self.state
            .as_deref()
            .map(|state| (&state.inflight, &state.waiters))
    }

    /// Make the conversation a waiter, or update its wait, after a computed
    /// `concurrency_full` refusal.
    pub(crate) fn record_waiter(
        &mut self,
        conversation: waiters::Conversation,
        class: cortexkit_role_routing::declarations::ConsumerPriority,
        retry_at_ms: i64,
        counts: Vec<CapCount>,
        now: i64,
    ) {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::record_waiter");
        if let Some(state) = self.state.as_deref_mut() {
            state
                .waiters
                .record(conversation, class, retry_at_ms, counts, now);
        }
    }

    /// Move a waiting conversation's retry time to the one its answer gave.
    pub(crate) fn retime_waiter(
        &mut self,
        conversation: &waiters::Conversation,
        retry_at_ms: i64,
        now: i64,
    ) {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::retime_waiter");
        if let Some(state) = self.state.as_deref_mut() {
            state.waiters.retime(conversation, retry_at_ms, now);
        }
    }

    /// The conversation took a slot on `count`, which ends a wait on it.
    pub(crate) fn took_slot(&mut self, conversation: &waiters::Conversation, count: &CapCount) {
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter("account_command::took_slot");
        if let Some(state) = self.state.as_deref_mut() {
            state.waiters.took(conversation, count);
        }
    }

    // A replay or a read-only writer command can visit an account without a
    // lifecycle effect. Remember its key so command cleanup still drops lapsed
    // entries without scanning any other account or changing durable rows.
    fn visit_account(&self, provider: &str, account: &str) {
        if self.state.is_some() {
            self.visited_accounts
                .borrow_mut()
                .insert((provider.to_owned(), account.to_owned()));
        }
    }

    /// A writer count also discards lapsed entries for the account it visits.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "cap enforcement will consume ledger counts")
    )]
    pub(crate) fn inflight_count(
        &mut self,
        provider: &str,
        account: &str,
        scope: InflightScope<'_>,
        now: i64,
    ) -> usize {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::inflight_count");
        let state = self
            .state
            .as_deref_mut()
            .expect("ledger needs module state");
        state.inflight.touch(provider, account, now);
        state.inflight.count(provider, account, scope, now)
    }

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
            self.visit_account(&open.row.provider, &open.row.account_id);
            return Ok(Some(open.row.clone()));
        }
        keyed_exception();
        let row = self.sql.admission_by_key(principal, admission_id)?;
        if let Some(row) = &row {
            self.visit_account(&row.provider, &row.account_id);
        }
        Ok(row)
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
            Some(state) => Ok(state.open_admission(principal, admission_id).map(|open| {
                self.visit_account(&open.row.provider, &open.row.account_id);
                open.row.clone()
            })),
            None => Ok(self
                .sql
                .admission(principal, admission_id)?
                .filter(|row| matches!(row.state.as_str(), "reserved" | "started" | "expired"))),
        }
    }

    /// Success needs both the admission and its probe metadata. Open runs use
    /// memory; a closed run reads the row and metadata in one keyed exception.
    pub(crate) fn admission_for_success(
        &self,
        principal: &str,
        id: &str,
    ) -> RouteStateResult<Option<striatum_store::AdmissionStateRow>> {
        if let Some(open) = self
            .admission_state()
            .and_then(|state| state.open_admission(principal, id))
        {
            return Ok(Some(striatum_store::AdmissionStateRow {
                row: open.row.clone(),
                input_size_class: open.input_size_class,
                model_decision_id: open.model_decision_id.clone(),
                rowid: open.rowid,
            }));
        }
        self.sql.admission_success_by_key(principal, id)
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
                .and_then(|open| {
                    self.visit_account(&open.row.provider, &open.row.account_id);
                    open.input_size_class
                })),
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
            Some(state) => Ok(state.affinity(principal, hash, provider).map(|row| {
                self.visit_account(&row.provider, &row.account_id);
                row.clone()
            })),
            None => self.sql.affinity(principal, hash, provider),
        }
    }

    pub(crate) fn pin_has_started(&self, affinity: &AccountAffinityRow) -> RouteStateResult<bool> {
        self.visit_account(&affinity.provider, &affinity.account_id);
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
            Some(state) => Ok(state
                .parent_used_account(principal, hash, provider, now)
                .inspect(|account| {
                    self.visit_account(provider, account);
                })),
            None => self.sql.parent_used_account(principal, hash, provider, now),
        }
    }

    pub(crate) fn demand(
        &self,
        provider: &str,
        account_id: &str,
        committed_after: Option<i64>,
    ) -> RouteStateResult<Vec<AccountDemandRow>> {
        self.visit_account(provider, account_id);
        match self.admission_state() {
            Some(state) => Ok(state.demand(provider, account_id, committed_after)),
            None => self.sql.demand(provider, account_id, committed_after),
        }
    }

    /// A continuation's stored request and answer. The state holds no
    /// bodies, and an answer that made no offer (a hold or refusal) or an
    /// offer already used or released is not in memory at all, so with a
    /// state this is always one keyed exception, inside the writer command.
    pub(crate) fn continuation(
        &self,
        principal: &str,
        selection: &str,
    ) -> RouteStateResult<Option<V1Recorded>> {
        if self.admission_state().is_none() {
            return self.sql.continuation(principal, selection);
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
            Some(state) => {
                Ok(state.continued_reservation(principal, admission_id, run_id, decision))
            }
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
            None => {
                self.sql
                    .continuation_has_used_successor(principal, admission_id, run_id, previous)
            }
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
        if let Some(state) = self.state.as_deref_mut() {
            change(&mut state.admissions);
            state.inflight.apply(&state.admissions, self.now_ms);
            state.admissions.pending.clear();
        }
    }

    /// As [`Self::mirror`], for a change SQL reported as applied or not. The
    /// state must agree; if it does not, the command fails, so SQL and the
    /// state's fork both roll back, and the writer reloads the state.
    fn mirror_checked(
        &mut self,
        sql_changed: bool,
        writer: &str,
        change: impl FnOnce(&mut AdmissionState) -> bool,
    ) -> RouteStateResult<()> {
        let Some(state) = self.state.as_deref_mut() else {
            return Ok(());
        };
        let applied = change(&mut state.admissions);
        state.inflight.apply(&state.admissions, self.now_ms);
        state.admissions.pending.clear();
        if applied != sql_changed {
            return Err(diverged(format!(
                "{writer}: SQL changed {sql_changed}, state changed {applied}"
            )));
        }
        Ok(())
    }

    /// Run a state-deciding change as the continuation writers do, for tests
    /// that set the state up by hand.
    #[cfg(test)]
    pub(crate) fn decide_for_test(
        &mut self,
        change: impl FnOnce(&mut AdmissionState),
    ) -> RouteStateResult<()> {
        self.decide(change)
    }

    /// Decide a change on the state, then persist the writes it decided.
    fn decide<T>(&mut self, change: impl FnOnce(&mut AdmissionState) -> T) -> RouteStateResult<T> {
        let sql = self.sql;
        let state = self
            .state
            .as_deref_mut()
            .expect("decide needs a working state");
        let decided = change(&mut state.admissions);
        state.inflight.apply(&state.admissions, self.now_ms);
        for effect in std::mem::take(&mut state.admissions.pending) {
            match effect {
                admissions::Effect::Observed {
                    admission,
                    previous,
                    at,
                } => {
                    if !sql.set_admission_last_observed(&admission.0, &admission.1, previous, at)? {
                        return Err(diverged(format!(
                            "admission {admission:?} observation time differs from state"
                        )));
                    }
                }
                admissions::Effect::Admission { row, from } => {
                    if !sql.set_admission_lifecycle(&row, &from)? {
                        return Err(diverged(format!(
                            "admission {} was not {from} in SQL",
                            row.admission_id
                        )));
                    }
                }
                admissions::Effect::LeaseEnded {
                    conversation,
                    pin_id,
                    pin_generation,
                    previous,
                    at,
                } => {
                    if !sql.set_affinity_lease_end(
                        &conversation,
                        (&pin_id, pin_generation),
                        previous,
                        at,
                    )? {
                        return Err(diverged(format!(
                            "affinity {conversation:?} is not pin {pin_id} generation \
                             {pin_generation} with lease {previous:?} in SQL"
                        )));
                    }
                }
                admissions::Effect::Unchanged { admission, from } => {
                    if sql.admission_in_states(&admission.0, &admission.1, &from)? {
                        return Err(diverged(format!(
                            "admission {} is {from:?} in SQL but not open so in state",
                            admission.1
                        )));
                    }
                }
                admissions::Effect::Offer {
                    selection,
                    used_at,
                    released_at,
                } => {
                    if !sql.settle_continuation_offer(
                        &selection.0,
                        &selection.1,
                        used_at,
                        released_at,
                    )? {
                        return Err(diverged(format!("offer {selection:?} is not open in SQL")));
                    }
                }
            }
        }
        Ok(decided)
    }

    pub(crate) fn insert_admission(&mut self, row: &AccountAdmissionRow) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::insert_admission");
        self.sql.insert_admission(row)?;
        let rowid = self.sql.last_insert_rowid();
        self.mirror(|state| state.insert_admission(row, rowid));
        if let Some(state) = self.state.as_deref_mut() {
            state.inflight.update(row, self.now_ms);
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
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter(
            "account_command::record_admission_probe_scope",
        );
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::mark_admission_expired");
        let changed = self.sql.mark_admission_expired(principal, admission_id)?;
        self.mirror_checked(changed, "mark_admission_expired", |state| {
            state.expire(principal, admission_id)
        })?;
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::mark_admission_started");
        let changed = self
            .sql
            .mark_admission_started(principal, admission_id, run_id, at, late)?;
        self.mirror_checked(changed, "mark_admission_started", |state| {
            state.start(principal, admission_id, run_id, at, late)
        })?;
        Ok(changed)
    }

    pub(crate) fn observe_admission(
        &mut self,
        row: &AccountAdmissionRow,
        at: i64,
    ) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::observe_admission");
        self.visit_account(&row.provider, &row.account_id);
        if self.state.is_none() {
            return self.sql.observe_admission(row, at);
        }
        self.decide(|state| state.observe_admission(row, at))
    }

    pub(crate) fn mark_admission_released(
        &mut self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::mark_admission_released");
        let changed = self
            .sql
            .mark_admission_released(principal, admission_id, at)?;
        self.mirror_checked(changed, "mark_admission_released", |state| {
            state.release(principal, admission_id, at)
        })?;
        Ok(changed)
    }

    pub(crate) fn mark_admission_finished(
        &mut self,
        principal: &str,
        admission_id: &str,
        at: i64,
    ) -> RouteStateResult<bool> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::mark_admission_finished");
        let changed = self
            .sql
            .mark_admission_finished(principal, admission_id, at)?;
        self.mirror_checked(changed, "mark_admission_finished", |state| {
            state.finish(principal, admission_id, at)
        })?;
        Ok(changed)
    }

    pub(crate) fn put_affinity(
        &mut self,
        row: &AccountAffinityRow,
        expected: Option<(&str, u64)>,
    ) -> RouteStateResult<bool> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::put_affinity");
        self.visit_account(&row.provider, &row.account_id);
        let changed = self.sql.put_affinity(row, expected)?;
        self.mirror_checked(changed, "put_affinity", |state| {
            state.put_affinity(row, expected)
        })?;
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::touch_affinity");
        let changed = self.sql.touch_affinity(
            principal,
            conversation_hash,
            provider,
            expected,
            last_used_at,
            expires_at,
            lease_valid_until,
        )?;
        self.mirror_checked(changed, "touch_affinity", |state| {
            state.touch_affinity(
                principal,
                conversation_hash,
                provider,
                expected,
                last_used_at,
                expires_at,
                lease_valid_until,
            )
        })?;
        Ok(changed)
    }

    pub(crate) fn end_lease(&mut self, pin_id: &str, at: i64) -> RouteStateResult<bool> {
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter("account_command::end_lease");
        let changed = self.sql.end_lease(pin_id, at)?;
        self.mirror_checked(changed, "end_lease", |state| state.end_lease(pin_id, at))?;
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::record_continuation");
        self.sql.record_continuation(
            principal, selection, record, run, admission, previous, offer,
        )?;
        let rowid = self.sql.last_insert_rowid();
        let source = if offer.is_some() && self.state.is_some() {
            let state = self.admission_state().expect("checked above");
            let left = state
                .chains
                .get(&(principal.to_owned(), admission.to_owned()))
                .and_then(|chain| chain.runs.get(run))
                .and_then(|rows| rows.values().find(|row| row.decision_id == previous))
                .map(|row| row.reservation_id.clone())
                .unwrap_or_else(|| admission.to_owned());
            if state.open_admission(principal, &left).is_none() {
                // Real continuation requests require an open source. Keep the
                // low-level writer coherent for old or synthetic closed sources,
                // but make an unexpected production lookup visible.
                tracing::warn!(%principal, admission_id = %left, "continuation source is not open; using keyed admission lookup");
                CONTINUATION_SOURCE_READS.fetch_add(1, Ordering::Relaxed);
            }
            self.admission(principal, &left)?
                .map(|row| striatum_store::ContinuationSourceRow {
                    principal: principal.to_owned(),
                    selection_id: selection.to_owned(),
                    run_id: run.to_owned(),
                    left_admission_id: left,
                    provider: row.provider,
                    account_id: row.account_id,
                    used_at_ms: None,
                })
        } else {
            None
        };
        self.mirror(|state| {
            state.record_continuation(principal, selection, run, admission, previous, offer, rowid)
        });
        if let (Some(state), Some(source)) = (self.state.as_deref_mut(), source) {
            state.inflight.cache_source(source);
        }
        Ok(())
    }

    pub(crate) fn release_expired_continuations(&mut self, now: i64) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter(
            "account_command::release_expired_continuations",
        );
        if self.state.is_none() {
            return self.sql.release_expired_continuations(now);
        }
        self.decide(|state| state.release_expired_continuations(now))
    }

    pub(crate) fn use_continuation(&mut self, decision: &str, now: i64) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::use_continuation");
        // Report delivery runs directly on the store writer. Evaluate windows
        // at the report's received time, just as the durable start does, rather
        // than sampling another clock after the report has waited in the queue.
        self.now_ms = now;
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::use_observed_continuation");
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
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::finish_continuations");
        if self.state.is_none() {
            return self
                .sql
                .finish_continuations(principal, admission, run, now);
        }
        self.decide(|state| state.finish_continuations(principal, admission, run, now))
    }

    /// The working state supplies the probe authorizations' owners and
    /// decides which name expired unstarted admissions; SQL releases those
    /// probes and expires the admissions.
    pub(crate) fn release_expired_admission_probes(&mut self, now: i64) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter(
            "account_command::release_expired_admission_probes",
        );
        let Some(state) = self.admission_state() else {
            self.sql.release_expired_admission_probes(now)?;
            return Ok(());
        };
        let expired = state.expired_probe_admissions(self.probe_authorization_owners()?, now);
        let chosen: Vec<(String, String)> = expired
            .iter()
            .map(|(principal, admission, _)| (principal.clone(), admission.clone()))
            .collect();
        let released = self.sql.release_admission_probes(expired, now)?;
        // The state chose only admissions it holds as unstarted; SQL must have
        // released every one of them, and expired exactly those the state
        // holds as reserved.
        if released.released != chosen.len() {
            return Err(diverged(format!(
                "release_expired_admission_probes: state chose {} unstarted admissions, \
                 SQL released {}",
                chosen.len(),
                released.released
            )));
        }
        for (principal, admission) in &chosen {
            let sql_expired = released
                .expired
                .iter()
                .any(|(p, a)| p == principal && a == admission);
            self.mirror_checked(sql_expired, "release_expired_admission_probes", |state| {
                state.expire(principal, admission)
            })?;
        }
        Ok(())
    }

    /// Release at `now`, inside this command and before it decides, what
    /// the readers' `AccountState::as_of` view already shows released:
    /// expired unstarted admissions' probes, then expired continuation
    /// offers. Every command that claims a probe or uses a continuation
    /// calls this first. A command without a working state (a projection)
    /// releases nothing here; it writes nothing when nothing expired.
    pub(crate) fn release_expired(&mut self, now: i64) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::release_expired");
        let Some(state) = self.state.as_deref() else {
            return Ok(());
        };
        let owners = if state.blocks.enabled() {
            state.blocks.authorization_owners()
        } else {
            Vec::new()
        };
        let probes = !state
            .admissions
            .expired_probe_admissions(owners, now)
            .is_empty();
        let offers = state.admissions.has_expired_offers(now);
        if probes {
            self.release_expired_admission_probes(now)?;
        }
        if offers {
            self.release_expired_continuations(now)?;
        }
        Ok(())
    }

    pub(crate) fn tick(&mut self, now_ms: i64) -> RouteStateResult<()> {
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter("account_command::tick");
        // A cheap check that the block state still matches SQL; a mismatch
        // reloads the state and runs the tick again.
        self.sql.reconcile_block_state()?;
        self.release_expired_admission_probes(now_ms)?;
        self.release_expired_continuations(now_ms)?;
        if let Some(state) = self.state.as_deref_mut() {
            state.inflight.expire_lapsed(now_ms);
        }
        self.sql.sync_fence_rechecks()
    }

    pub(crate) fn prune_account_admissions(
        &mut self,
        before: i64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<usize> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::prune_account_admissions");
        let deleted = self
            .sql
            .prune_account_admissions_returning(before, limit, now_ms)?;
        if let Some(state) = self.state.as_deref_mut() {
            for row in &deleted {
                state.inflight.remove(
                    &row.provider,
                    &row.account_id,
                    &(row.principal.clone(), row.admission_id.clone()),
                    self.now_ms,
                );
            }
        }
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
        keys: &[striatum_store::AffinityRetentionKey],
        before: i64,
        lease_before: i64,
    ) -> RouteStateResult<AffinityRetentionBatch> {
        #[cfg(test)]
        let _writer_entry =
            striatum_store::writer_entry_trace::enter("account_command::prune_account_affinities");
        let (batch, deleted) =
            self.sql
                .delete_account_affinities_returning(keys, before, lease_before)?;
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
        #[cfg(test)]
        let _writer_entry = striatum_store::writer_entry_trace::enter(
            "account_command::prune_account_continuations",
        );
        let deleted = self
            .sql
            .prune_account_continuations_returning(before, limit)?;
        let sql = self.sql;
        if let Some(state) = self.state.as_deref_mut() {
            state.inflight.prune_continuations(&deleted);
        }
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
        if let Some(state) = self.state.as_deref_mut() {
            state.admissions.settle();
            for (provider, account) in self.visited_accounts.get_mut().iter() {
                state.inflight.touch(provider, account, self.now_ms);
            }
        }
    }
}

impl<'a> std::ops::Deref for AccountCommand<'a> {
    type Target = AccountTx<'a>;
    fn deref(&self) -> &Self::Target {
        self.sql
    }
}

/// Make sure the state is loaded and published. If an earlier load failed and
/// nothing has been published yet, load again: a failed load must leave the
/// module serving (and retrying), not exiting into a restart loop.
pub(crate) fn initialize(store: &SqliteRouteStateStore) -> RouteStateResult<()> {
    let Some(bridge) = store.account_state_bridge() else {
        return store.install_account_state_bridge(Arc::new(StateOwner::default()));
    };
    // Counted before reading: a reader that waited on a retry another
    // caller started shares that retry's answer instead of loading again.
    let arrived = store.account_state_retry_count();
    match bridge.snapshot() {
        Ok(_) => Ok(()),
        // The first load is still running: answer that. A retry would
        // queue a second load behind it and wait for both without a bound.
        Err(error) if error.to_string().contains(FIRST_LOAD_PENDING) => Err(error),
        Err(_) => {
            store.retry_account_state_load_after(arrived)?;
            bridge.snapshot().map(|_| ())
        }
    }
}

#[cfg(test)]
pub(crate) fn initialize_at(store: &SqliteRouteStateStore, now_ms: i64) -> RouteStateResult<()> {
    initialize_counted_at(store, now_ms).map(|_| ())
}

#[cfg(test)]
pub(crate) fn initialize_counted_at(
    store: &SqliteRouteStateStore,
    now_ms: i64,
) -> RouteStateResult<Arc<std::sync::atomic::AtomicU64>> {
    // The loader runs on the writer thread, where a test's thread-local clock
    // is not installed. Pin its clock before starting that thread's load.
    let loads = Arc::new(std::sync::atomic::AtomicU64::new(0));
    store.install_account_state_bridge(Arc::new(StateOwner {
        load_now_ms: Some(now_ms),
        loads: loads.clone(),
        ..StateOwner::default()
    }))?;
    Ok(loads)
}

#[cfg(test)]
pub(crate) fn report_inflight_expiry_cost_for_test(
    store: &SqliteRouteStateStore,
    now: i64,
    iterations: usize,
) {
    snapshot(store)
        .unwrap()
        .inflight
        .report_expiry_cost(now, iterations);
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
    assert_eq!(
        loaded.blocks, published.blocks,
        "the request mix left the block state unequal to a fresh load"
    );
    assert_eq!(
        store.block_writes_outside_command(),
        0,
        "the request mix wrote block tables outside an account command: {:?}",
        store.block_tables_written_outside_command()
    );
    // The route-state snapshot and the provider rows `route.cooldown_status`
    // reads answer from the state as from SQL, for requests an hour either
    // side of now against clocks evaluated at each of those times.
    let wall = crate::now_ms() as i64;
    let times = [wall - 3_600_000, wall, wall + 3_600_000];
    for now in times {
        for recheck in times {
            let sql = store
                .build_snapshot_at_for_test(now as f64, recheck)
                .unwrap();
            assert_eq!(
                published
                    .blocks
                    .route_state_snapshot(now as f64, recheck)
                    .unwrap(),
                sql,
                "route-state snapshot at {now} with clocks at {recheck}"
            );
        }
    }
    let routes = store.build_snapshot_at_for_test(wall as f64, wall).unwrap();
    let providers: std::collections::BTreeSet<&str> = routes
        .by_route
        .keys()
        .filter_map(|route| striatum_core::derive_provider_key(route))
        .collect();
    for provider in providers {
        assert_eq!(
            published.blocks.provider_cooldown(provider).unwrap(),
            striatum_core::RouteStateStore::get_provider_cooldown(store, provider).unwrap(),
            "provider cooldown of {provider}"
        );
    }
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
        striatum_store::keyed_exception_reads(),
        maintenance_reads(),
    );
    println!(
        "account_state blocks: {} block_read_backs={} block_writes_outside_command={}",
        published
            .blocks
            .table_sizes()
            .iter()
            .map(|(table, rows)| format!("{table}={rows}"))
            .collect::<Vec<_>>()
            .join(" "),
        striatum_store::block_read_backs(),
        store.block_writes_outside_command(),
    );
}

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod inflight_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod waiters_tests;
