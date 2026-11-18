//! Group commit on the store's existing writer connection. A command owns its
//! inputs and its reply stays private until the enclosing commit has returned.

use super::*;
use std::sync::{mpsc, Arc};
use std::time::Instant;

const MAX_BATCH: usize = 32;

/// Error text a module command returns when SQL changed a different number of
/// rows than its working state expected. The writer then rolls the whole batch
/// back, reloads the state and runs the batch once more (see `write_batch`).
pub const STATE_DIVERGED: &str = "account state diverged from SQL";

/// How a batch ended.
#[derive(Debug, PartialEq, Eq)]
enum BatchOutcome {
    Committed,
    /// Only the startup barrier ran: nothing to commit or publish.
    Barrier,
    /// A command found the working state disagreeing with SQL. Every command
    /// in the batch was rolled back and the working state dropped.
    Diverged(String),
}

impl AccountTx<'_> {
    pub(crate) fn pool_sql<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        f(self.conn).map_err(err)
    }
    // SQL operations receive the batch connection, never a store handle. The
    // engine owns commit and rollback, including the command's savepoint.
    pub(crate) fn command_sql<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        f(self.conn).map_err(|error| cortexkit_store::StoreError::Backend(error.to_string()))
    }

    pub(crate) fn command_scope<T>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        f(self)
    }

    pub fn tick(&self, now_ms: i64) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::account_writer::tick");
        crate::job_timing::sync("state_drift_check", || {
            let started = Instant::now();
            let result = self.reconcile_block_state();
            crate::job_timing::record(crate::job_timing::Work {
                writer_ns: started.elapsed().as_nanos() as u64,
                ..Default::default()
            });
            result
        })?;
        self.release_expired_admission_probes(now_ms)?;
        self.release_expired_continuations(now_ms)?;
        self.sync_fence_rechecks()
    }

    /// The part of a Tick that synchronizes fence rechecks, for a caller that
    /// applies the admission and continuation expiries itself.
    pub fn sync_fence_rechecks(&self) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::account_writer::sync_fence_rechecks");
        crate::fence_rechecks::sync_on(self, crate::now_ms()).map_err(err)
    }
}

fn write_batch(
    conn: &Connection,
    batch: &mut [Queued],
    writer: &mut crate::account_state::WriterAccountState,
    #[cfg(any(test, feature = "test-support"))] transactions: &AtomicU64,
) -> rusqlite::Result<BatchOutcome> {
    let state = &mut writer.working;
    // Startup must not manufacture a write transaction or a second publication.
    // The loader already published its consistent read before this barrier runs.
    if batch.iter().all(|queued| queued.initialize_only) {
        for queued in batch {
            queued
                .command
                .run(
                    &AccountTx::sql(conn),
                    state
                        .as_mut()
                        .map(|state| &mut **state as &mut dyn crate::AccountWorkingState),
                )
                .expect("initialization barrier cannot fail");
        }
        return Ok(BatchOutcome::Barrier);
    }
    #[cfg(any(test, feature = "test-support"))]
    let _batch = connection_store::AccountBatchGuard::enter();
    #[cfg(test)]
    before_begin_for_test(conn);
    let transaction = crate::write_transaction(conn)?;
    #[cfg(any(test, feature = "test-support"))]
    transactions.fetch_add(1, Ordering::Relaxed);
    // Another connection may have committed between the state's freshness
    // check and this transaction's start. Holding the write lock now, check
    // again, and load the state inside the transaction if it moved, so no
    // command decides on rows older than the ones it writes over.
    if let Err(error) = writer.reload_if_moved(conn) {
        transaction.rollback()?;
        return Ok(BatchOutcome::Diverged(format!(
            "reload account state: {error}"
        )));
    }
    let state = &mut writer.working;
    for queued in batch.iter_mut() {
        conn.execute_batch("SAVEPOINT account_command")?;
        let before = state.as_ref().map(|state| state.fork());
        // The fork equals the working state until the command ends (its
        // block rows change only below), so the command's claim decisions
        // read the fork's block tables without a second copy.
        let blocks = before.as_ref().and_then(|before| before.blocks());
        // Every block row this command changes, triggers included, is copied
        // into the working state before the savepoint is released.
        let capture = crate::block_state::Capture::begin();
        let status = queued.command.run(
            &AccountTx::with_blocks(conn, blocks),
            state
                .as_mut()
                .map(|state| &mut **state as &mut dyn crate::AccountWorkingState),
        );
        let captured = capture.finish();
        if status.is_ok() && !captured.is_empty() {
            if let Some(state) = state.as_mut() {
                // A failed read-back fails the batch: the state would
                // otherwise miss a committed row.
                let rows = crate::block_state::read_back(conn, &captured)?;
                state.apply_block_rows(&rows);
            }
        }
        if let Err(error) = status {
            *state = before;
            // The command found its working state disagreeing with SQL. The
            // commands before it in this batch computed from the same state,
            // so their replies may be stale too: undo the whole batch and drop
            // the state. The caller reloads it and runs the batch again.
            if error.to_string().contains(STATE_DIVERGED) {
                *state = None;
                transaction.rollback()?;
                return Ok(BatchOutcome::Diverged(error.to_string()));
            }
            // A full rollback requested by a SQLite trigger, disk-full and
            // I/O failures can destroy the
            // transaction, including savepoints. Never run a sibling in
            // autocommit after such a failure.
            if conn.is_autocommit() {
                return Err(rusqlite::Error::InvalidQuery);
            }
            conn.execute_batch("ROLLBACK TO account_command")?;
            if conn.is_autocommit() {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tracing::debug!(%error, "account command rolled back");
        }
        conn.execute_batch("RELEASE account_command")?;
    }
    #[cfg(any(test, feature = "test-support"))]
    crash_for_test("before_commit");
    connection_store::timed_commit(transaction).map(|()| BatchOutcome::Committed)
}

/// Run a batch on a state loaded for it. If a command finds the state
/// disagreeing with SQL, nothing of the batch was committed and every command
/// is deterministic in its inputs, the stored rows and the state: reload the
/// state and run the whole batch once more, so every caller gets an answer
/// computed from current rows. A second disagreement fails the whole batch;
/// the next batch loads afresh.
fn run_batch(
    conn: &Connection,
    batch: &mut [Queued],
    state: &mut crate::account_state::WriterAccountState,
    #[cfg(any(test, feature = "test-support"))] transactions: &AtomicU64,
) -> Result<bool, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        if let Err(error) = state.refresh(conn) {
            return Err(format!("load account state: {error}"));
        }
        let outcome = write_batch(
            conn,
            batch,
            state,
            #[cfg(any(test, feature = "test-support"))]
            transactions,
        );
        match outcome {
            Ok(BatchOutcome::Committed) => {
                state.publish();
                return Ok(true);
            }
            Ok(BatchOutcome::Barrier) => return Ok(false),
            Ok(BatchOutcome::Diverged(error)) => {
                state.invalidate();
                if attempts == 2 {
                    return Err(error);
                }
                tracing::warn!(%error, "account state diverged; reloading and running the batch again");
            }
            Err(error) => {
                state.invalidate();
                return Err(error.to_string());
            }
        }
    }
}

/// A hook run once on the writer thread just before the next batch on the
/// database at the given path opens its write transaction.
#[cfg(test)]
pub(crate) static BEFORE_BEGIN: std::sync::Mutex<
    Vec<(std::path::PathBuf, Box<dyn FnOnce() + Send>)>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn before_begin_for_test(conn: &Connection) {
    let Some(path) = conn.path().map(std::path::PathBuf::from) else {
        return;
    };
    let hook = {
        let mut hooks = BEFORE_BEGIN.lock().unwrap();
        hooks
            .iter()
            .position(|(target, _)| *target == path)
            .map(|at| hooks.remove(at).1)
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(any(test, feature = "test-support"))]
fn crash_for_test(point: &str) {
    if std::env::var("STRIATUM_TEST_ACCOUNT_WRITER_CRASH").as_deref() == Ok(point) {
        std::process::exit(86);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountCommandKind {
    Admit,
    Replay,
    Observe,
    Report,
    QuotaDelta,
    Release,
    Maintenance,
    Tick,
    Repair,
    Selection,
    HistoryMaintenance,
}

impl AccountCommandKind {
    fn label(self) -> &'static str {
        match self {
            Self::Admit => "admit",
            Self::Replay => "replay",
            Self::Observe => "observe",
            Self::Report => "report",
            Self::QuotaDelta => "quota_delta",
            Self::Release => "release",
            Self::Maintenance => "maintenance",
            Self::Tick => "tick",
            Self::Repair => "repair",
            Self::Selection => "selection",
            Self::HistoryMaintenance => "history_maintenance",
        }
    }
}

fn stopped() -> RouteStateStoreError {
    err("store_error: account writer stopped; retry the operation")
}

trait Command: Send {
    fn run(
        &mut self,
        tx: &AccountTx<'_>,
        state: Option<&mut (dyn crate::AccountWorkingState + 'static)>,
    ) -> RouteStateResult<()>;
    fn publish(self: Box<Self>, failure: Option<&str>, timing: ReplyTiming);
}

/// A command's error may carry a domain refusal without losing its type.
/// Only a store error can request the writer's state-divergence retry.
pub trait AccountCommandError: From<RouteStateStoreError> + Send + 'static {
    fn store_error(&self) -> Option<&RouteStateStoreError>;
}

impl AccountCommandError for RouteStateStoreError {
    fn store_error(&self) -> Option<&RouteStateStoreError> {
        Some(self)
    }
}

struct TypedCommand<F, T, E = RouteStateStoreError> {
    run: F,
    result: Option<Result<T, E>>,
    reply: mpsc::Sender<Result<T, E>>,
    timing: Arc<Mutex<ReplyTiming>>,
}

impl<F, T, E> Command for TypedCommand<F, T, E>
where
    F: FnMut(
            &AccountTx<'_>,
            Option<&mut (dyn crate::AccountWorkingState + 'static)>,
        ) -> Result<T, E>
        + Send,
    T: Send,
    E: AccountCommandError,
{
    fn run(
        &mut self,
        tx: &AccountTx<'_>,
        state: Option<&mut (dyn crate::AccountWorkingState + 'static)>,
    ) -> RouteStateResult<()> {
        // A batch that is run again (see `run_batch`) runs each command again;
        // only the last run's result is replied.
        let result = (self.run)(tx, state);
        let status = result.as_ref().map(|_| ()).map_err(|error| {
            error
                .store_error()
                .map(err)
                .unwrap_or_else(|| err("account command refused"))
        });
        self.result = Some(result);
        status
    }

    fn publish(self: Box<Self>, failure: Option<&str>, timing: ReplyTiming) {
        *self.timing.lock().unwrap() = timing;
        let result = match failure {
            Some(error) => Err(err(format!("store_error: {error}; retry the operation")).into()),
            None => self.result.expect("committed command has a result"),
        };
        // A caller may have disconnected after COMMIT. Its durable idempotency
        // row, not delivery of this message, determines the retry's answer.
        let _ = self.reply.send(result);
    }
}

struct Queued {
    enqueued: Instant,
    label: &'static str,
    background: bool,
    initialize_only: bool,
    history_maintenance: bool,
    command: Box<dyn Command>,
}

#[derive(Clone, Copy, Default)]
struct ReplyTiming {
    stages: StoreStageTiming,
    work: crate::job_timing::Work,
}

impl ReplyTiming {
    fn record(self) {
        connection_store::record_stage_timing(self.stages);
        crate::job_timing::record(self.work);
    }
}

/// An owned pending reply. Dropping it does not cancel a queued write.
pub struct AccountReply<T, E = RouteStateStoreError>(
    mpsc::Receiver<Result<T, E>>,
    Arc<Mutex<ReplyTiming>>,
);

impl<T> AccountReply<T> {
    /// [`Self::wait`], giving up after `bound` with
    /// [`crate::ACCOUNT_STATE_LOAD_PENDING`]; the queued command still runs.
    pub(crate) fn wait_load(self, bound: std::time::Duration) -> RouteStateResult<T> {
        match self.0.recv_timeout(bound) {
            Ok(result) => {
                self.1.lock().unwrap().record();
                result
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Err(err(format!(
                "{} {}ms; retry the operation",
                crate::ACCOUNT_STATE_LOAD_PENDING,
                bound.as_millis()
            ))),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(stopped()),
        }
    }
}

impl<T, E: AccountCommandError> AccountReply<T, E> {
    pub fn wait(self) -> Result<T, E> {
        let result = self.0.recv().unwrap_or_else(|_| Err(stopped().into()));
        // The connection work ran on the writer, but belongs in this caller's
        // timing scope too. Store-wide burst counters still count the batch once.
        self.1.lock().unwrap().record();
        result
    }
}

pub(crate) struct AccountWriter {
    sender: Option<mpsc::Sender<Queued>>,
    dead: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    #[cfg(any(test, feature = "test-support"))]
    batch_sizes: Arc<Mutex<Vec<usize>>>,
    #[cfg(any(test, feature = "test-support"))]
    commands: [AtomicU64; 11],
    #[cfg(any(test, feature = "test-support"))]
    command_changes: Arc<Mutex<Vec<(AccountCommandKind, u64)>>>,
}

impl AccountWriter {
    fn start(
        store: Arc<connection_store::ConnectionStore>,
        bridge: Arc<std::sync::OnceLock<Arc<dyn crate::AccountStateBridge>>>,
        archive: Option<String>,
        #[cfg(any(test, feature = "test-support"))] transactions: Arc<AtomicU64>,
        exit: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<Queued>();
        // Kept to wake readers still waiting for a first publication if the
        // writer thread stops before it published.
        let exit_bridge = bridge.clone();
        let dead = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread_dead = dead.clone();
        let thread_exit = exit.clone();
        #[cfg(any(test, feature = "test-support"))]
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        #[cfg(any(test, feature = "test-support"))]
        let sizes = batch_sizes.clone();
        let spawn = std::thread::Builder::new()
            .name("account-writer".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> RouteStateResult<()> {
                        let mut state = crate::account_state::WriterAccountState::new(
                            bridge,
                            store.block_reload_flag(),
                            store.state_load_counter(),
                        );
                        let mut deferred = None;
                        loop {
                            let first = match deferred.take().map_or_else(|| receiver.recv(), Ok) {
                                Ok(first) => first,
                                Err(_) => break,
                            };
                            let history_maintenance = first.history_maintenance;
                            let mut batch = vec![first];
                            // No linger: an idle writer begins immediately. After a
                            // commit, drain only commands already queued, up to 32.
                            while batch.len() < MAX_BATCH {
                                match receiver.try_recv() {
                                    Ok(command)
                                        if command.history_maintenance == history_maintenance =>
                                    {
                                        batch.push(command)
                                    }
                                    // Do not expose the archive to request commands mixed
                                    // into a maintenance batch. Preserve queue order.
                                    Ok(command) => {
                                        deferred = Some(command);
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }
                            let started = Instant::now();
                            #[cfg(any(test, feature = "test-support"))]
                            if batch.iter().any(|queued| !queued.initialize_only) {
                                sizes.lock().unwrap().push(batch.len());
                            }
                            for command in &batch {
                                store.record_command_wait(
                                    started.duration_since(command.enqueued).as_nanos() as u64,
                                );
                            }
                            let before = SqliteRouteStateStore::thread_stage_timing();
                            let mut labels = [None; MAX_BATCH];
                            for (slot, queued) in labels.iter_mut().zip(&batch) {
                                *slot = Some(queued.label);
                            }
                            let _labels = crate::job_timing::BatchScope::enter(labels);
                            let background = batch.iter().any(|queued| queued.background);
                            let cpu = background.then(crate::job_timing::thread_cpu_ns);
                            // A failed state load or connection fails this batch
                            // only: its callers get the error, the writer keeps
                            // running, and the next batch loads again. Exiting
                            // here would turn one bad read into a restart loop.
                            let result: Result<bool, String> = store
                                .with_conn(|conn| {
                                    let uri = if history_maintenance {
                                        archive.as_deref()
                                    } else {
                                        None
                                    };
                                    crate::history::with_archive(conn, uri, || {
                                        Ok(run_batch(
                                            conn,
                                            &mut batch,
                                            &mut state,
                                            #[cfg(any(test, feature = "test-support"))]
                                            &transactions,
                                        ))
                                    })
                                })
                                .unwrap_or_else(|error| Err(error.to_string()));
                            let after = SqliteRouteStateStore::thread_stage_timing();
                            let timing = StoreStageTiming {
                                writer_queue_ns: after.writer_queue_ns - before.writer_queue_ns,
                                sql_ns: after.sql_ns - before.sql_ns,
                                commit_ns: after.commit_ns - before.commit_ns,
                            };
                            // A group commit is indivisible: each waiting job is
                            // charged the inclusive batch hold/CPU, not a guessed
                            // fraction of shared state loading and commit work.
                            let timing = ReplyTiming {
                                stages: timing,
                                work: crate::job_timing::Work {
                                    cpu_ns: cpu.map_or(0, |cpu| {
                                        crate::job_timing::thread_cpu_ns().saturating_sub(cpu)
                                    }),
                                    writer_ns: timing.sql_ns + timing.commit_ns,
                                    ..Default::default()
                                },
                            };
                            #[cfg(any(test, feature = "test-support"))]
                            if matches!(result, Ok(true)) {
                                crash_for_test("after_commit");
                            }
                            let failure = result.err();
                            for queued in batch {
                                queued.command.publish(failure.as_deref(), timing);
                            }
                        }
                        Ok(())
                    },
                ));
                drop(receiver);
                // Dropping receiver and any in-flight batch wakes every blocked
                // caller even if a command panicked before producing a result.
                thread_dead.store(true, Ordering::Release);
                if !matches!(result, Ok(Ok(()))) {
                    if let Some(bridge) = exit_bridge.get() {
                        bridge.failed("account writer stopped");
                    }
                    thread_exit();
                }
            });
        let thread = match spawn {
            Ok(thread) => Some(thread),
            Err(_) => {
                dead.store(true, Ordering::Release);
                exit();
                None
            }
        };
        Self {
            sender: Some(sender),
            dead,
            thread,
            #[cfg(any(test, feature = "test-support"))]
            batch_sizes,
            #[cfg(any(test, feature = "test-support"))]
            commands: std::array::from_fn(|_| AtomicU64::new(0)),
            #[cfg(any(test, feature = "test-support"))]
            command_changes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn enqueue<T: Send + 'static, E: AccountCommandError>(
        &self,
        kind: AccountCommandKind,
        f: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
            ) -> Result<T, E>
            + Send
            + 'static,
    ) -> RouteStateResult<AccountReply<T, E>> {
        self.enqueue_inner_with_error(
            f,
            false,
            kind.label(),
            kind == AccountCommandKind::HistoryMaintenance,
        )
    }

    pub(crate) fn enqueue_inner<T: Send + 'static>(
        &self,
        f: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
            ) -> RouteStateResult<T>
            + Send
            + 'static,
        initialize_only: bool,
    ) -> RouteStateResult<AccountReply<T>> {
        self.enqueue_inner_with_error(f, initialize_only, "initialize", false)
    }

    fn enqueue_inner_with_error<T: Send + 'static, E: AccountCommandError>(
        &self,
        f: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
            ) -> Result<T, E>
            + Send
            + 'static,
        initialize_only: bool,
        operation: &'static str,
        history_maintenance: bool,
    ) -> RouteStateResult<AccountReply<T, E>> {
        if self.dead.load(Ordering::Acquire) {
            return Err(stopped());
        }
        let (reply, receiver) = mpsc::channel();
        let timing = Arc::new(Mutex::new(ReplyTiming::default()));
        self.sender
            .as_ref()
            .expect("writer sender exists until drop")
            .send(Queued {
                enqueued: Instant::now(),
                label: crate::job_timing::label().unwrap_or(operation),
                background: crate::job_timing::label().is_some(),
                initialize_only,
                history_maintenance,
                command: Box::new(TypedCommand {
                    run: f,
                    result: None,
                    reply,
                    timing: timing.clone(),
                }),
            })
            .map_err(|_| stopped())?;
        Ok(AccountReply(receiver, timing))
    }
}

impl Drop for AccountWriter {
    fn drop(&mut self) {
        // Closing the queue drains committed replies before releasing the
        // connection and its lease. A queued command is never acknowledged on
        // a shutdown-only path that skips COMMIT.
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl SqliteRouteStateStore {
    pub(crate) fn initialize_account_writer_state(&self) -> RouteStateResult<()> {
        let bound = self
            .account_state
            .get()
            .map_or(std::time::Duration::from_secs(60), |bridge| {
                bridge.load_wait()
            });
        let result = self
            .account_writer_engine()
            .enqueue_inner(|_, _| Ok(()), true)
            .and_then(|reply| reply.wait_load(bound));
        // A load that failed reports itself, but a writer that could not run
        // the barrier (no connection, a stopped or never-started thread) does
        // not: tell the publisher, so readers waiting for the first
        // publication get the error instead of waiting forever. A load still
        // running is not a failure: it publishes when it ends.
        if let (Err(error), Some(bridge)) = (&result, self.account_state.get()) {
            if !error
                .to_string()
                .contains(crate::ACCOUNT_STATE_LOAD_PENDING)
            {
                bridge.failed(&error.to_string());
            }
        }
        result
    }

    pub(crate) fn account_writer_engine(&self) -> &AccountWriter {
        self.account_writer.get_or_init(|| {
            AccountWriter::start(
                self.store.clone(),
                self.account_state.clone(),
                self.history
                    .as_ref()
                    .map(|history| history.attachment.clone()),
                #[cfg(any(test, feature = "test-support"))]
                self.account_write_transactions.clone(),
                Arc::new(|| {
                    tracing::error!("account writer died; restarting is required");
                    #[cfg(not(any(test, feature = "test-support")))]
                    std::process::exit(1);
                }),
            )
        })
    }

    pub fn enqueue_account_command<T: Send + 'static>(
        &self,
        kind: AccountCommandKind,
        mut f: impl FnMut(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
    ) -> RouteStateResult<AccountReply<T>> {
        self.enqueue_account_state_command(kind, move |tx, _| f(tx))
    }

    /// The store transports opaque module-owned state only. Domain computation
    /// belongs to the callback; SQL helpers do not inspect the working copy.
    pub fn enqueue_account_state_command<T: Send + 'static>(
        &self,
        kind: AccountCommandKind,
        f: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
            ) -> RouteStateResult<T>
            + Send
            + 'static,
    ) -> RouteStateResult<AccountReply<T>> {
        self.enqueue_account_state_command_with_error(kind, f)
    }

    /// Enqueue a command whose typed failure survives its savepoint rollback.
    /// Batch-level I/O and commit failures still replace it with a store error.
    pub fn enqueue_account_state_command_with_error<T: Send + 'static, E: AccountCommandError>(
        &self,
        kind: AccountCommandKind,
        mut f: impl FnMut(
                &AccountTx<'_>,
                Option<&mut (dyn crate::AccountWorkingState + 'static)>,
            ) -> Result<T, E>
            + Send
            + 'static,
    ) -> RouteStateResult<AccountReply<T, E>> {
        if self.store.is_projection() {
            return Err(err(
                "private projections cannot enqueue account writer commands",
            ));
        }
        let writer = self.account_writer_engine();
        #[cfg(any(test, feature = "test-support"))]
        let inputs = crate::replay::current();
        #[cfg(any(test, feature = "test-support"))]
        let writer_trace = crate::writer_entry_trace::current();
        #[cfg(any(test, feature = "test-support"))]
        let command_changes = writer.command_changes.clone();
        let reply = writer.enqueue(kind, move |tx, state| {
            #[cfg(any(test, feature = "test-support"))]
            let _inputs = inputs.as_ref().map(crate::replay::Inputs::install);
            #[cfg(any(test, feature = "test-support"))]
            let _writer_trace = writer_trace.as_ref().map(|trace| trace.install());
            #[cfg(any(test, feature = "test-support"))]
            let before = connection_store::account_rows_changed();
            let result = f(tx, state);
            #[cfg(any(test, feature = "test-support"))]
            command_changes
                .lock()
                .unwrap()
                .push((kind, connection_store::account_rows_changed() - before));
            result
        })?;
        #[cfg(any(test, feature = "test-support"))]
        writer.commands[kind as usize].fetch_add(1, Ordering::Relaxed);
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = kind;
        Ok(reply)
    }

    pub fn account_command<T: Send + 'static>(
        &self,
        kind: AccountCommandKind,
        f: impl FnMut(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
    ) -> RouteStateResult<T> {
        #[cfg(any(test, feature = "test-support"))]
        if self.store.is_projection() {
            return self.projection_command(f);
        }
        self.enqueue_account_command(kind, f)?.wait()
    }

    pub fn account_command_with_error<T: Send + 'static, E: AccountCommandError>(
        &self,
        kind: AccountCommandKind,
        mut f: impl FnMut(&AccountTx<'_>) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E> {
        #[cfg(any(test, feature = "test-support"))]
        if self.store.is_projection() {
            return self.direct_account_transaction_with_error(f);
        }
        self.enqueue_account_state_command_with_error(kind, move |tx, _| f(tx))?
            .wait()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn account_writer_batch_sizes_for_test(&self) -> Vec<usize> {
        self.account_writer.get().map_or_else(Vec::new, |writer| {
            writer.batch_sizes.lock().unwrap().clone()
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn account_command_count_for_test(&self, kind: AccountCommandKind) -> u64 {
        self.account_writer.get().map_or(0, |writer| {
            writer.commands[kind as usize].load(Ordering::Relaxed)
        })
    }

    /// Account row-change counts in command execution order, including rolled-back
    /// attempts. Await successful replies before using them as commit evidence.
    #[cfg(any(test, feature = "test-support"))]
    pub fn account_command_row_changes_for_test(&self, kind: AccountCommandKind) -> Vec<u64> {
        self.account_writer.get().map_or_else(Vec::new, |writer| {
            writer
                .command_changes
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(command, rows)| (*command == kind).then_some(*rows))
                .collect()
        })
    }
}

#[cfg(test)]
mod tests;
