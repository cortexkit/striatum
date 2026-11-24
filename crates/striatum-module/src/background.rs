//! Startup and supervision of the long-lived tasks owned by a serving connection.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use subc_protocol::Frame;
use subc_transport::read_frame;
use tokio::io::AsyncRead;

use crate::{
    account_quota, config_overrides, fusiform_catalog, quota_events, retention, routing_v1,
    winner_ladder, BrocaCredentials, ModuleConfig, ModuleError, SharedBrocaCredentials,
    SharedCatalogHealth, SharedQuotaCache, SharedStore, ACCOUNT_QUOTA, DEFAULT_MODULE_ID,
};

/// The long-lived background loops one serving connection starts: retention,
/// quota events, runner declarations, the account roster,
/// config refresh, catalog refresh and the winner-ladder watch. Each is written
/// to run until the process exits, so one
/// that finishes, by panicking or by returning, has silently stopped a duty
/// route.select depends on (quota readings, catalog facts, config reloads).
/// The serving loop watches this set and turns the first stop into a module
/// error: the process exits non-zero, and the subc daemon's crash-restart
/// policy starts a fresh module that restores its caches from the store and
/// runs every loop again. Dropping the set aborts the remaining loops.
#[derive(Default)]
pub(super) struct BackgroundLoops {
    tasks: tokio::task::JoinSet<()>,
    names: HashMap<tokio::task::Id, &'static str>,
    retention_stop: crate::retention::RetentionStop,
}

impl BackgroundLoops {
    /// Stop delivery before the final cache flush. Aborting alone does not wait
    /// for a task already polling to finish its publication and acknowledgement.
    pub(super) async fn shutdown(mut self) {
        self.retention_stop.request();
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }

    pub(super) fn retention_stop(&self) -> crate::retention::RetentionStop {
        self.retention_stop.clone()
    }

    pub(super) fn spawn<F>(&mut self, name: &'static str, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = self.tasks.spawn(task);
        self.names.insert(handle.id(), name);
    }

    /// Wait until one loop stops and describe which one and how. Stays pending
    /// while every loop runs, including when no loop was started.
    async fn next_stop(&mut self) -> ModuleError {
        let Some(joined) = self.tasks.join_next_with_id().await else {
            return std::future::pending().await;
        };
        let (id, how) = match joined {
            Ok((id, ())) => (id, "returned".to_string()),
            Err(error) if error.is_panic() => {
                let id = error.id();
                (
                    id,
                    format!("panicked: {}", panic_message(error.into_panic().as_ref())),
                )
            }
            Err(error) => (error.id(), "was cancelled".to_string()),
        };
        let name = self.names.remove(&id).unwrap_or("unnamed");
        ModuleError::Message(format!(
            "routing background loop `{name}` stopped unexpectedly ({how}); \
             exiting so the daemon restarts the module"
        ))
    }
}

impl Drop for BackgroundLoops {
    fn drop(&mut self) {
        self.retention_stop.request();
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Read the next frame from subc unless a background loop stops first. A stop
/// is printed to stderr, so the module's output names the loop, and returned
/// as the error that ends the process.
pub(super) async fn next_frame_or_loop_stop<R>(
    read_half: &mut R,
    loops: &mut BackgroundLoops,
    writer: Option<&tokio::sync::mpsc::Sender<Frame>>,
) -> Result<Option<Frame>, ModuleError>
where
    R: AsyncRead + Unpin,
{
    frame_or_loop_stop(read_next_frame(read_half), loops, writer).await
}

pub(super) async fn read_next_frame<R: AsyncRead + Unpin>(
    read_half: &mut R,
) -> Result<Option<Frame>, ModuleError> {
    read_frame(read_half)
        .await
        .map_err(|error| ModuleError::Message(error.to_string()))
}

pub(super) async fn frame_or_loop_stop(
    frame: impl std::future::Future<Output = Result<Option<Frame>, ModuleError>>,
    loops: &mut BackgroundLoops,
    writer: Option<&tokio::sync::mpsc::Sender<Frame>>,
) -> Result<Option<Frame>, ModuleError> {
    tokio::select! {
        // The writer's receiver closes on failure. Leave via normal cleanup;
        // the connection owner joins the writer and returns the actual error.
        () = async {
            match writer {
                Some(writer) => writer.closed().await,
                None => std::future::pending().await,
            }
        } => Ok(None),
        result = frame => result,
        stopped = loops.next_stop() => {
            eprintln!("[striatum] background-loop-stopped: {stopped}");
            Err(stopped)
        }
    }
}

pub(super) struct BackgroundState {
    pub(super) loops: BackgroundLoops,
    pub(super) broca_credentials: SharedBrocaCredentials,
    pub(super) override_path: PathBuf,
    pub(super) operator_config_path: PathBuf,
}

pub(super) async fn start(
    config: &ModuleConfig,
    store: &SharedStore,
    quota_cache: &SharedQuotaCache,
    catalog_health: &SharedCatalogHealth,
) -> Result<BackgroundState, ModuleError> {
    // Restore both quota identities and the full credential listing before any
    // request can run. Store reads belong on the blocking pool, not Tokio workers.
    let restore_store = Arc::clone(store);
    let (restored, mut credentials) = tokio::task::spawn_blocking(move || {
        let state = account_quota::restore_account_state(restore_store.as_ref());
        let mut credentials = BrocaCredentials::default();
        account_quota::restore_credential_listing(restore_store.as_ref(), &mut credentials);
        (state, credentials)
    })
    .await
    .map_err(|error| ModuleError::Message(format!("restore account roster: {error}")))?;
    credentials.connection_file_path = Some(config.connection_file_path.clone());
    let broca_credentials = Arc::new(Mutex::new(credentials));
    let account_state = Arc::new(Mutex::new(restored));
    // Old audit, usage and account-selection rows are deleted in small batches
    // on their own timer. Each retention pass re-reads how long every table keeps its
    // rows from the routing file's `retention` block.
    let mut loops = BackgroundLoops::default();
    loops.spawn(
        "account maintenance",
        account_tick_loop(Arc::clone(store), ACCOUNT_TICK_PERIOD),
    );
    loops.spawn(
        "WAL checkpoints",
        checkpoint_loop(Arc::clone(store), std::time::Duration::from_secs(15)),
    );
    loops.spawn(
        "routing call counts",
        flush_call_counts_loop(Arc::clone(store), std::time::Duration::from_secs(30)),
    );
    loops.spawn("history mover", history_mover_loop(Arc::clone(store)));
    loops.spawn(
        "retention",
        retention::retention_loop(
            Arc::clone(store),
            config.config_path.clone(),
            loops.retention_stop(),
        ),
    );
    // The quota event consumer starts from the restored per-account views.
    loops.spawn(
        "quota events",
        quota_events::run(
            config.connection_file_path.clone(),
            Arc::clone(&quota_cache),
            Arc::clone(&account_state),
            Arc::clone(&store),
            crate::route_facts::Handler {
                quota_cache: Arc::clone(quota_cache),
                store: Arc::clone(store),
                credentials: Arc::clone(&broca_credentials),
                config_path: config.config_path.clone(),
            },
        ),
    );
    loops.spawn(
        "runner declarations",
        routing_v1::runners::refresh_loop(
            config.connection_file_path.clone(),
            Arc::clone(&broca_credentials),
            config.config_path.clone(),
        ),
    );
    // Derive serving methods from the restored listing before the first catalog poll.
    let (join_state, join_store, join_credentials, join_config_path) = (
        Arc::clone(&account_state),
        Arc::clone(store),
        Arc::clone(&broca_credentials),
        config.config_path.clone(),
    );
    tokio::task::spawn_blocking(move || {
        account_quota::refresh_serving_methods(
            &join_state,
            &join_store,
            &join_credentials,
            &join_config_path,
        )
    })
    .await
    .map_err(|error| ModuleError::Message(format!("restore serving methods: {error}")))?;
    if ACCOUNT_QUOTA.set(Arc::clone(&account_state)).is_ok() {
        let roster_resolver: Arc<dyn cortexkit_claustrum_client::CredentialResolver> =
            Arc::new(crate::vault::VaultResolver::new(
                config.connection_file_path.clone(),
                DEFAULT_MODULE_ID,
            ));
        loops.spawn(
            "account roster",
            account_quota::account_quota_loop(
                config.connection_file_path.clone(),
                account_state,
                Arc::clone(&store),
                roster_resolver,
                Arc::clone(&broca_credentials),
                config.config_path.clone(),
            ),
        );
    }
    // Every placement and config reload compares the winner ladder over the same
    // config a selection reads (the operator policy lives in the routing file)
    // and the availability live selections report.
    let ladder_sources =
        winner_ladder::LadderSources::live(&config.config_path, &config.config_path);

    // Load a validated config snapshot before serving. The watcher publishes
    // replacements; model overrides remain request overlays so withdrawing one
    // exposes the stored fact without rewriting operator score placements.
    let override_path = config.config_path.clone();
    // Selection policy and model-fact overrides share the routing-owned file.
    let operator_config_path = override_path.clone();
    config_overrides::live_config(&override_path);
    loops.spawn(
        "config refresh",
        config_overrides::config_refresh_loop(
            override_path.clone(),
            Arc::clone(&broca_credentials),
        ),
    );

    // Seed catalog health with the active polling cadence; reloads wake the
    // catalog loop, which updates both its cadence and the health threshold.
    let catalog_poll_ms = config_overrides::live_config(&override_path).catalog_poll_ms();
    if let Ok(mut health) = catalog_health.lock() {
        health.set_poll_ms(catalog_poll_ms);
    }
    loops.spawn(
        "catalog refresh",
        fusiform_catalog::catalog_refresh_loop(
            config.connection_file_path.clone(),
            Arc::clone(&store),
            Arc::clone(&catalog_health),
            config.config_path.clone(),
            ladder_sources.clone(),
            Arc::clone(&broca_credentials),
        ),
    );
    // Compares a ladder when a config file the ladder reads changed or live
    // selection reported a different availability set (including the first one
    // after boot, which is compared with the ladder stored before the restart).
    loops.spawn(
        "winner ladder watch",
        winner_ladder::config_reload_watch_loop(Arc::clone(&store), ladder_sources),
    );

    Ok(BackgroundState {
        loops,
        broca_credentials,
        override_path,
        operator_config_path,
    })
}

const ACCOUNT_TICK_PERIOD: std::time::Duration = std::time::Duration::from_secs(10);

fn account_ticks(period: std::time::Duration) -> tokio::time::Interval {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    // A congested writer needs one reconciliation, not one for each missed tick.
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticks
}

async fn account_tick_loop(store: SharedStore, period: std::time::Duration) {
    let mut ticks = account_ticks(period);
    let mut bypasses = 0;
    loop {
        ticks.tick().await;
        striatum_store::job_timing::run("account_maintenance", async {
            // Production writes block tables only through account commands. A
            // write outside one makes the writer reload its whole state; report
            // the running count each time it grows, so the live log shows it.
            let outside = store.block_writes_outside_command();
            if outside > bypasses {
                bypasses = outside;
                tracing::warn!(
                    block_writes_outside_command = outside,
                    tables = ?store.block_tables_written_outside_command(),
                    "block tables were written outside an account command"
                );
            }
            // Tick waits on a thread until the account writer commits it, which can
            // take as long as every batch queued ahead of it. This task is woken by
            // its own timer, so it runs on the worker that was driving Tokio's IO and
            // timers; blocking that worker would freeze every socket read and timer
            // in the routing runtime until the writer answered. Wait on the blocking
            // pool instead.
            let store = Arc::clone(&store);
            let outcome = crate::stage_timing::spawn_job_blocking(move || {
                crate::account_writer::tick(&store, crate::now_ms() as i64)
            })
            .await;
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "account maintenance failed"),
                // A panic in the maintenance pass still stops this loop, so the
                // serving loop reports it and the module restarts.
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(error) => tracing::warn!(%error, "account maintenance worker failed"),
            }
        })
        .await;
    }
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn account_ticks_do_not_replay_each_missed_pass() {
    use tokio::time::{advance, Instant};
    let mut ticks = account_ticks(ACCOUNT_TICK_PERIOD);
    ticks.tick().await;
    advance(std::time::Duration::from_secs(35)).await;
    ticks.tick().await;
    let after_catch_up = Instant::now();
    ticks.tick().await;
    assert_eq!(after_catch_up.elapsed(), std::time::Duration::from_secs(5));
}

async fn history_mover_loop(store: SharedStore) {
    let period = std::time::Duration::from_secs(5);
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut backlog = BacklogTicks::default();
    loop {
        ticks.tick().await;
        if backlog.observe(drain_history_tick(&store, crate::now_ms() as i64).await) {
            tracing::warn!(
                ticks = backlog.0,
                "routing history has exceeded one mover batch on successive ticks"
            );
        }
    }
}

#[derive(Default)]
struct BacklogTicks(u32);

impl BacklogTicks {
    fn observe(&mut self, backlogged: bool) -> bool {
        self.0 = if backlogged {
            self.0.saturating_add(1)
        } else {
            0
        };
        self.0 >= 3 && (self.0 - 3) % 12 == 0
    }
}

const HISTORY_BATCHES_PER_TICK: usize = 64;

async fn drain_history_tick(store: &SharedStore, now: i64) -> bool {
    let mut backlogged = false;
    for batch in 0..HISTORY_BATCHES_PER_TICK {
        let store = Arc::clone(store);
        let result = striatum_store::job_timing::run(
            "history_move",
            crate::stage_timing::spawn_job_blocking(move || store.move_history_batch_status(now)),
        )
        .await;
        match result {
            Ok(Ok((_, full, more))) => {
                if batch == 0 {
                    backlogged = more;
                }
                if !full {
                    break;
                }
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "history move failed; retrying next tick");
                break;
            }
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                tracing::warn!(%error, "history mover worker failed");
                break;
            }
        }
    }
    backlogged
}

fn checkpoint_files<E>(
    state: impl FnOnce() -> Result<(), E>,
    history: impl FnOnce() -> Result<(), E>,
) -> (Result<(), E>, Result<(), E>) {
    (state(), history())
}

async fn checkpoint_loop(store: SharedStore, period: std::time::Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticks.tick().await;
        let store = Arc::clone(&store);
        match striatum_store::job_timing::run(
            "wal_checkpoint",
            crate::stage_timing::spawn_job_blocking(move || {
                checkpoint_files(|| store.checkpoint_wal(), || store.checkpoint_history_wal())
            }),
        )
        .await
        {
            Ok((state, history)) => {
                if let Err(error) = state {
                    tracing::warn!(%error, "state WAL checkpoint failed; retrying next tick");
                }
                if let Err(error) = history {
                    tracing::warn!(%error, "history WAL checkpoint failed; retrying next tick");
                }
            }
            Err(error) => tracing::warn!(%error, "WAL checkpoint worker failed"),
        }
    }
}

async fn flush_call_counts_loop(store: SharedStore, period: std::time::Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticks.tick().await;
        striatum_store::job_timing::run(
            "routing_call_counts",
            flush_call_counts(Arc::clone(&store)),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// While a slow account writer batch is in progress, start `waiter`, a
    /// background loop that will queue a command behind it. Once that command
    /// is queued, the routing runtime must still answer an unrelated socket
    /// request and fire an unrelated timer before the writer lets go.
    ///
    /// Each loop is woken by a timer, so it runs on the worker that was driving
    /// Tokio's IO and timer events. If that worker blocks its thread while the
    /// command waits, no other worker takes the driver over, and every socket
    /// read and timer in the runtime waits for the writer too.
    ///
    /// The check is about order, not speed: the slow batch holds the writer
    /// until the test releases it, and the test releases it only after the echo
    /// and the timer have both been served. A runtime that waits for the writer
    /// can serve neither, so the batch falls back to its safety cap, and the
    /// test fails on that. No wall-clock bound is asserted, so a loaded build
    /// machine can't fail it.
    fn assert_runtime_serves_while_writer_is_busy(
        name: &str,
        queued: striatum_store::AccountCommandKind,
        seed: impl FnOnce(&std::path::Path),
        waiter: impl FnOnce(
            SharedStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) {
        use std::io::{Read, Write};
        use striatum_store::AccountCommandKind;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // How long the slow batch holds the writer if the test never releases
        // it. Reaching this cap is the failure signal, so it only needs to be
        // far longer than serving one echo and one timer takes.
        const WRITER_SAFETY_CAP: std::time::Duration = std::time::Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let dir = cortexkit_test_support::ScratchDir::new(name);
        let path = dir.join("store.db");
        let store = Arc::new(
            striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
                name,
                path.to_string_lossy().into_owned(),
            ))
            .unwrap(),
        );
        seed(&path);

        // An unrelated one-byte echo service stands in for a route request.
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        runtime.spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0u8; 1];
            while socket.read_exact(&mut byte).await.is_ok() {
                if socket.write_all(&byte).await.is_err() {
                    break;
                }
            }
        });
        let mut client = std::net::TcpStream::connect(address).unwrap();
        // Longer than the writer's safety cap, so a stalled runtime shows up as
        // the cap being reached rather than as a read timeout.
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(20)))
            .unwrap();
        let mut round_trip = || {
            let started = std::time::Instant::now();
            client.write_all(b"x").unwrap();
            let mut byte = [0u8; 1];
            client.read_exact(&mut byte).unwrap();
            started.elapsed()
        };
        round_trip();

        let (held, holding) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released_by_test = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released_flag = Arc::clone(&released_by_test);
        let slow = store
            .enqueue_account_command(AccountCommandKind::Maintenance, move |_| {
                let _ = held.send(());
                // The writer can run a command body again when it replays a
                // batch; only the first run holds the writer for the test.
                if released_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    return Ok(());
                }
                let by_test = released.recv_timeout(WRITER_SAFETY_CAP).is_ok();
                released_flag.store(by_test, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        holding.recv().unwrap();
        let background = runtime.spawn(waiter(Arc::clone(&store)));
        // A timer that falls due while the loop's command is waiting behind the
        // slow batch. The loop ticks every few milliseconds, so it is already
        // waiting on the writer long before the timer is due.
        let (fired, timer_fired) = std::sync::mpsc::channel();
        runtime.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            let _ = fired.send(());
        });
        let timer = timer_fired.recv_timeout(std::time::Duration::from_secs(20));
        let echo = round_trip();
        let _ = release.send(());
        slow.wait().unwrap();
        // The loop really did queue its command behind the slow batch.
        while store.account_command_count_for_test(queued) == 0 {
            assert!(!background.is_finished(), "{name}: loop stopped early");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        background.abort();
        runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        eprintln!(
            "{name}: echo round trip {echo:?}; timer served: {}",
            timer.is_ok()
        );
        assert!(timer.is_ok(), "{name}: the timer never fired");
        assert!(
            released_by_test.load(std::sync::atomic::Ordering::SeqCst),
            "{name}: the echo and timer were served only after the writer reached its \
             safety cap, so the runtime waited for a busy account writer"
        );
    }

    #[test]
    fn a_tick_waiting_on_a_busy_account_writer_leaves_the_runtime_serving() {
        assert_runtime_serves_while_writer_is_busy(
            "tick-runtime-stall",
            striatum_store::AccountCommandKind::Tick,
            |_| {},
            |store| {
                Box::pin(account_tick_loop(
                    store,
                    std::time::Duration::from_millis(20),
                ))
            },
        );
    }

    /// Lists no credentials, after a short timer wait so the account roster
    /// loop resumes from a timer, like a real listing reply read off a socket.
    struct SlowEmptyResolver;

    impl cortexkit_claustrum_client::CredentialResolver for SlowEmptyResolver {
        fn resolve<'a>(
            &'a self,
            _raw_handle: &'a str,
            _min_ttl_ms: Option<u64>,
            _force_refresh: bool,
        ) -> cortexkit_claustrum_client::CredentialResolverFuture<
            'a,
            Result<
                cortexkit_claustrum_client::ResolvedCredential,
                cortexkit_claustrum_client::CredentialResolverError,
            >,
        > {
            Box::pin(async {
                Err(cortexkit_claustrum_client::CredentialResolverError::unavailable())
            })
        }

        fn get_scoped<'a>(
            &'a self,
            _credential_id: &'a str,
        ) -> cortexkit_claustrum_client::CredentialResolverFuture<
            'a,
            Result<
                cortexkit_claustrum_client::ResolvedCredential,
                cortexkit_claustrum_client::CredentialResolverError,
            >,
        > {
            Box::pin(async {
                Err(cortexkit_claustrum_client::CredentialResolverError::unavailable())
            })
        }

        fn list_scoped(
            &self,
        ) -> cortexkit_claustrum_client::CredentialResolverFuture<
            '_,
            Result<
                cortexkit_claustrum_client::ScopedCredentialListing,
                cortexkit_claustrum_client::CredentialResolverError,
            >,
        > {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                Err(cortexkit_claustrum_client::CredentialResolverError::unavailable())
            })
        }
    }

    #[test]
    fn the_account_roster_loop_waiting_on_a_busy_account_writer_leaves_the_runtime_serving() {
        let scratch = cortexkit_test_support::ScratchDir::new("roster-loop-runtime-stall-files");
        let connection_file = scratch.join("subc.json");
        let config_path = scratch.join("striatum.jsonc");
        assert_runtime_serves_while_writer_is_busy(
            "roster-loop-runtime-stall",
            striatum_store::AccountCommandKind::Repair,
            // An unused continuation offer past its deadline makes the loop's
            // selection context release it through the account writer.
            |path| {
                rusqlite::Connection::open(path)
                    .unwrap()
                    .execute(
                        "INSERT INTO account_continue (principal, selection_id, request_json,
                           answer_json, run_id, admission_id, previous_decision_id,
                           decision_id, reservation_id, valid_until_ms)
                         VALUES ('reserved:runner', 'sel', '{}', '{}', 'run', 'adm', 'dec-0',
                           'dec-1', 'adm-1', 1)",
                        [],
                    )
                    .unwrap();
            },
            move |store| {
                let state = Arc::new(Mutex::new(account_quota::restore_account_state(
                    store.as_ref(),
                )));
                let credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
                Box::pin(account_quota::account_quota_loop(
                    connection_file,
                    state,
                    store,
                    Arc::new(SlowEmptyResolver),
                    credentials,
                    config_path,
                ))
            },
        );
    }

    #[tokio::test]
    async fn checkpoint_timer_allows_next_write_to_cap_an_over_limit_wal() {
        let dir = cortexkit_test_support::ScratchDir::new("wal-checkpoint-timer");
        let store = Arc::new(
            striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
                "wal-timer",
                dir.join("store.db").to_string_lossy().into_owned(),
            ))
            .unwrap(),
        );
        let payload = "x".repeat(striatum_store::WAL_SIZE_CAP_BYTES as usize + 4096);
        store.put_quota_snapshot(&payload, 0).unwrap();
        let bytes = || std::fs::metadata(dir.join("store.db-wal")).unwrap().len();
        assert!(bytes() > striatum_store::WAL_SIZE_CAP_BYTES);
        let timer = tokio::spawn(checkpoint_loop(
            Arc::clone(&store),
            std::time::Duration::from_millis(10),
        ));
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut sequence = 0;
            while bytes() > striatum_store::WAL_SIZE_CAP_BYTES {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                // Change a value each time: an identical UPSERT may write no
                // pages and therefore need no WAL restart.
                sequence += 1;
                store
                    .put_quota_snapshot("next writer commit", sequence)
                    .unwrap();
            }
        })
        .await;
        timer.abort();
        let _ = timer.await;
        result.unwrap();
    }

    #[tokio::test]
    async fn call_counts_flush_on_the_timer_and_at_clean_shutdown() {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "call-flush-timer-{}-{}.db",
            std::process::id(),
            crate::now_ms()
        ));
        let store = Arc::new(
            striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
                "call-flush-timer",
                path.to_string_lossy().into_owned(),
            ))
            .unwrap(),
        );
        let observer = rusqlite::Connection::open(&path).unwrap();
        let persisted = || {
            observer
                .query_row(
                    "SELECT COALESCE(sum(calls),0) FROM routing_call_count",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
        };
        store
            .record_routing_call("route.head_choices", "contract", "app")
            .unwrap();
        assert_eq!(persisted(), 0);
        let timer = tokio::spawn(flush_call_counts_loop(
            Arc::clone(&store),
            std::time::Duration::from_millis(10),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while persisted() == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        timer.abort();
        let _ = timer.await;
        assert_eq!(persisted(), 1);
        store
            .record_routing_call("route.head_choices", "contract", "app")
            .unwrap();
        flush_call_counts(Arc::clone(&store)).await;
        assert_eq!(persisted(), 2);
    }
}

pub(super) async fn flush_call_counts(store: SharedStore) {
    match crate::stage_timing::spawn_job_blocking(move || {
        let result = store.flush_routing_calls();
        if let Ok(rows) = &result {
            striatum_store::job_timing::rows(*rows as u64);
        }
        result
    })
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error, "routing call count flush failed; the next tick will retry")
        }
        Err(error) => tracing::warn!(%error, "routing call count flush worker failed"),
    }
}

#[cfg(test)]
#[tokio::test]
async fn history_mover_drains_full_lanes_without_waiting_for_another_tick() {
    use striatum_core::{route_state_store::QuotaReadingLogRow, RouteStateStore};
    let scratch = cortexkit_test_support::ScratchDir::new("history-backlog");
    let store = Arc::new(
        striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            "history-backlog",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap(),
    );
    let now = 1_800_000_000_000;
    let readings = (0..129)
        .map(|i| QuotaReadingLogRow {
            account_key: "provider/account".into(),
            window_key: "w".into(),
            observed_at: now - 7_200_000 + i,
            used_percent: 5.0,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: None,
            banked_credit_count: None,
            fetched_at: None,
            stale: None,
        })
        .collect::<Vec<_>>();
    store.append_quota_readings(&readings).unwrap();
    assert!(drain_history_tick(&store, now).await);
    assert_eq!(store.move_history_batch(now).unwrap(), 0);
    let mut backlog = BacklogTicks::default();
    assert!(!backlog.observe(true));
    assert!(!backlog.observe(true));
    assert!(backlog.observe(true));
    assert!(!backlog.observe(true));
    assert!(!backlog.observe(false));
    assert!(!backlog.observe(true));
}

#[cfg(test)]
#[tokio::test]
async fn history_mover_yields_after_sixty_four_full_batches() {
    use striatum_core::{route_state_store::QuotaReadingLogRow, RouteStateStore};
    let scratch = cortexkit_test_support::ScratchDir::new("history-tick-cap");
    let store = Arc::new(
        striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            "history-tick-cap",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap(),
    );
    let now = 1_800_000_000_000;
    let rows = (0..(64 * 64 + 1))
        .map(|index| QuotaReadingLogRow {
            account_key: "provider/account".into(),
            window_key: "w".into(),
            observed_at: now - 7_200_000 + index,
            used_percent: 5.0,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: None,
            banked_credit_count: None,
            fetched_at: None,
            stale: None,
        })
        .collect::<Vec<_>>();
    assert_eq!(store.append_quota_readings(&rows).unwrap(), rows.len());
    assert!(drain_history_tick(&store, now).await);
    assert_eq!(
        store.move_history_batch(now).unwrap(),
        1,
        "the next tick must get the remainder"
    );
}

#[cfg(test)]
#[test]
fn history_checkpoint_runs_after_state_checkpoint_error() {
    let (state, history) = checkpoint_files(|| Err("state failed"), || Err("history failed"));
    assert_eq!(state, Err("state failed"));
    assert_eq!(history, Err("history failed"));
}
