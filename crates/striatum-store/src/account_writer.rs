//! Group commit on the store's existing writer connection. A command owns its
//! inputs and its reply stays private until the enclosing commit has returned.

use super::*;
use std::sync::{mpsc, Arc};
use std::time::Instant;

const MAX_BATCH: usize = 32;

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
        self.release_expired_admission_probes(now_ms)?;
        self.release_expired_continuations(now_ms)?;
        crate::fence_rechecks::sync(self.conn).map_err(err)
    }
}

fn write_batch(
    conn: &Connection,
    batch: &mut [Queued],
    state: &mut Option<Box<dyn crate::AccountWorkingState>>,
    #[cfg(any(test, feature = "test-support"))] transactions: &AtomicU64,
) -> rusqlite::Result<bool> {
    // Startup must not manufacture a write transaction or a second publication.
    // The loader already published its consistent read before this barrier runs.
    if batch.iter().all(|queued| queued.initialize_only) {
        for queued in batch {
            queued
                .command
                .run(&AccountTx {
                    conn,
                    state: state.as_deref(),
                })
                .expect("initialization barrier cannot fail");
        }
        return Ok(false);
    }
    #[cfg(any(test, feature = "test-support"))]
    let _batch = evaluation_snapshot::AccountBatchGuard::enter();
    let transaction = crate::write_transaction(conn)?;
    #[cfg(any(test, feature = "test-support"))]
    transactions.fetch_add(1, Ordering::Relaxed);
    for queued in batch {
        conn.execute_batch("SAVEPOINT account_command")?;
        let before = state.as_ref().map(|state| state.fork());
        let status = queued.command.run(&AccountTx {
            conn,
            state: state.as_deref(),
        });
        if let Err(error) = status {
            *state = before;
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
    evaluation_snapshot::timed_commit(transaction).map(|()| true)
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
}

fn stopped() -> RouteStateStoreError {
    err("store_error: account writer stopped; retry the operation")
}

trait Command: Send {
    fn run(&mut self, tx: &AccountTx<'_>) -> RouteStateResult<()>;
    fn publish(self: Box<Self>, failure: Option<&str>, timing: StoreStageTiming);
}

struct TypedCommand<F, T> {
    run: Option<F>,
    result: Option<RouteStateResult<T>>,
    reply: mpsc::Sender<RouteStateResult<T>>,
    timing: Arc<Mutex<StoreStageTiming>>,
}

impl<F, T> Command for TypedCommand<F, T>
where
    F: FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send,
    T: Send,
{
    fn run(&mut self, tx: &AccountTx<'_>) -> RouteStateResult<()> {
        let result = self.run.take().expect("command runs once")(tx);
        let status = result.as_ref().map(|_| ()).map_err(err);
        self.result = Some(result);
        status
    }

    fn publish(self: Box<Self>, failure: Option<&str>, timing: StoreStageTiming) {
        *self.timing.lock().unwrap() = timing;
        let result = match failure {
            Some(error) => Err(err(format!("store_error: {error}; retry the operation"))),
            None => self.result.expect("committed command has a result"),
        };
        // A caller may have disconnected after COMMIT. Its durable idempotency
        // row, not delivery of this message, determines the retry's answer.
        let _ = self.reply.send(result);
    }
}

struct Queued {
    enqueued: Instant,
    initialize_only: bool,
    command: Box<dyn Command>,
}

/// An owned pending reply. Dropping it does not cancel a queued write.
pub struct AccountReply<T>(
    mpsc::Receiver<RouteStateResult<T>>,
    Arc<Mutex<StoreStageTiming>>,
);

impl<T> AccountReply<T> {
    pub fn wait(self) -> RouteStateResult<T> {
        let result = self.0.recv().unwrap_or_else(|_| Err(stopped()));
        // The connection work ran on the writer, but belongs in this caller's
        // timing scope too. Store-wide burst counters still count the batch once.
        evaluation_snapshot::record_stage_timing(*self.1.lock().unwrap());
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
    commands: [AtomicU64; 10],
    #[cfg(any(test, feature = "test-support"))]
    command_changes: Arc<Mutex<Vec<(AccountCommandKind, u64)>>>,
}

impl AccountWriter {
    fn start(
        store: Arc<evaluation_snapshot::ConnectionStore>,
        bridge: Arc<std::sync::OnceLock<Arc<dyn crate::AccountStateBridge>>>,
        #[cfg(any(test, feature = "test-support"))] transactions: Arc<AtomicU64>,
        exit: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<Queued>();
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
                        let mut state = crate::account_state::WriterAccountState::new(bridge);
                        while let Ok(first) = receiver.recv() {
                            let mut batch = vec![first];
                            // No linger: an idle writer begins immediately. After a
                            // commit, drain only commands already queued, up to 32.
                            while batch.len() < MAX_BATCH {
                                match receiver.try_recv() {
                                    Ok(command) => batch.push(command),
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
                            let result = store
                                .with_conn(|conn| {
                                    Ok((|| -> RouteStateResult<_> {
                                        state.refresh(conn)?;
                                        let result = write_batch(
                                            conn,
                                            &mut batch,
                                            &mut state.working,
                                            #[cfg(any(test, feature = "test-support"))]
                                            &transactions,
                                        );
                                        match result {
                                            Ok(true) => state.publish(),
                                            Ok(false) => {}
                                            Err(_) => state.invalidate(),
                                        }
                                        Ok(result)
                                    })())
                                })
                                .map_err(err)??;
                            let after = SqliteRouteStateStore::thread_stage_timing();
                            let timing = StoreStageTiming {
                                writer_queue_ns: after.writer_queue_ns - before.writer_queue_ns,
                                sql_ns: after.sql_ns - before.sql_ns,
                                commit_ns: after.commit_ns - before.commit_ns,
                            };
                            #[cfg(any(test, feature = "test-support"))]
                            if matches!(result, Ok(true)) {
                                crash_for_test("after_commit");
                            }
                            let failure = result.err().map(|error| error.to_string());
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

    fn enqueue<T: Send + 'static>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
    ) -> RouteStateResult<AccountReply<T>> {
        self.enqueue_inner(f, false)
    }

    fn enqueue_inner<T: Send + 'static>(
        &self,
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
        initialize_only: bool,
    ) -> RouteStateResult<AccountReply<T>> {
        if self.dead.load(Ordering::Acquire) {
            return Err(stopped());
        }
        let (reply, receiver) = mpsc::channel();
        let timing = Arc::new(Mutex::new(StoreStageTiming::default()));
        self.sender
            .as_ref()
            .expect("writer sender exists until drop")
            .send(Queued {
                enqueued: Instant::now(),
                initialize_only,
                command: Box::new(TypedCommand {
                    run: Some(f),
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
        self.account_writer_engine()
            .enqueue_inner(|_| Ok(()), true)?
            .wait()
    }

    fn account_writer_engine(&self) -> &AccountWriter {
        self.account_writer.get_or_init(|| {
            AccountWriter::start(
                self.store.clone(),
                self.account_state.clone(),
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
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
    ) -> RouteStateResult<AccountReply<T>> {
        if self.store.is_projection() {
            return Err(err(
                "private projections cannot enqueue account writer commands",
            ));
        }
        let writer = self.account_writer_engine();
        #[cfg(any(test, feature = "test-support"))]
        let inputs = crate::replay::current();
        #[cfg(any(test, feature = "test-support"))]
        let command_changes = writer.command_changes.clone();
        let reply = writer.enqueue(move |tx| {
            #[cfg(any(test, feature = "test-support"))]
            let _inputs = inputs.as_ref().map(crate::replay::Inputs::install);
            #[cfg(any(test, feature = "test-support"))]
            let before = evaluation_snapshot::account_rows_changed();
            let result = f(tx);
            #[cfg(any(test, feature = "test-support"))]
            command_changes
                .lock()
                .unwrap()
                .push((kind, evaluation_snapshot::account_rows_changed() - before));
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
        f: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
    ) -> RouteStateResult<T> {
        if self.store.is_projection() {
            return self.projection_command(f);
        }
        self.enqueue_account_command(kind, f)?.wait()
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
