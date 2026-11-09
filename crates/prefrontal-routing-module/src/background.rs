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
}

impl BackgroundLoops {
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
) -> Result<Option<Frame>, ModuleError>
where
    R: AsyncRead + Unpin,
{
    tokio::select! {
        frame = read_frame(read_half) => frame.map_err(|e| ModuleError::Message(e.to_string())),
        stopped = loops.next_stop() => {
            eprintln!("[prefrontal-routing] background-loop-stopped: {stopped}");
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

pub(super) fn start(
    config: &ModuleConfig,
    store: &SharedStore,
    quota_cache: &SharedQuotaCache,
    catalog_health: &SharedCatalogHealth,
) -> Result<BackgroundState, ModuleError> {
    // Old audit, usage and account-selection rows are deleted in small batches
    // on their own timer. Each retention pass re-reads how long every table keeps its
    // rows from the routing file's `retention` block.
    let mut loops = BackgroundLoops::default();
    loops.spawn(
        "routing call counts",
        flush_call_counts_loop(Arc::clone(store), std::time::Duration::from_secs(30)),
    );
    loops.spawn(
        "retention",
        retention::retention_loop(Arc::clone(&store), config_overrides::config_path()),
    );
    // Restore per-account views before the quota event consumer starts.
    let account_state: account_quota::SharedAccountQuota = Arc::new(Mutex::new(
        account_quota::restore_account_state(store.as_ref()),
    ));
    loops.spawn(
        "quota events",
        quota_events::run(
            config.connection_file_path.clone(),
            Arc::clone(&quota_cache),
            Arc::clone(&account_state),
            Arc::clone(&store),
        ),
    );
    let broca_credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
    broca_credentials.lock().unwrap().connection_file_path =
        Some(config.connection_file_path.clone());
    loops.spawn(
        "runner declarations",
        routing_v1::runners::refresh_loop(
            config.connection_file_path.clone(),
            Arc::clone(&broca_credentials),
            config_overrides::config_path(),
        ),
    );
    // Restore credential serving methods from cached account identities before
    // the first catalog poll; API-only providers wait for the non-secret listing.
    account_quota::refresh_serving_methods(
        &account_state,
        store,
        &broca_credentials,
        &config_overrides::config_path(),
    );
    if ACCOUNT_QUOTA.set(Arc::clone(&account_state)).is_ok() {
        let roster_resolver: Arc<dyn cortexkit_claustrum_client::CredentialResolver> = Arc::new(
            cortexkit_claustrum_client::ClaustrumCredentialResolver::new(
                config.connection_file_path.clone(),
                std::env::current_dir().map_err(|error| {
                    ModuleError::Message(format!("resolve routing roster project root: {error}"))
                })?,
                DEFAULT_MODULE_ID,
            ),
        );
        loops.spawn(
            "account roster",
            account_quota::account_quota_loop(
                config.connection_file_path.clone(),
                account_state,
                Arc::clone(&store),
                roster_resolver,
                Arc::clone(&broca_credentials),
                config_overrides::config_path(),
            ),
        );
    }
    // Every placement and config reload compares the winner ladder over the same
    // config a selection reads (the operator policy lives in the routing file)
    // and the availability live selections report.
    let ladder_sources = winner_ladder::LadderSources::live(
        &config_overrides::config_path(),
        &config_overrides::config_path(),
    );

    // Load a validated config snapshot before serving. The watcher publishes
    // replacements; model overrides remain request overlays so withdrawing one
    // exposes the stored fact without rewriting operator score placements.
    let override_path = config_overrides::config_path();
    // Selection policy and model-fact overrides share the routing-owned file.
    let operator_config_path = override_path.clone();
    config_overrides::live_config(&override_path);
    loops.spawn(
        "config refresh",
        config_overrides::config_refresh_loop(
            override_path.clone(),
            Arc::clone(&broca_credentials.lock().unwrap().catalog_wake),
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
            catalog_poll_ms,
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

async fn flush_call_counts_loop(store: SharedStore, period: std::time::Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticks.tick().await;
        flush_call_counts(Arc::clone(&store)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn call_counts_flush_on_the_timer_and_at_clean_shutdown() {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "call-flush-timer-{}-{}.db",
            std::process::id(),
            crate::now_ms()
        ));
        let store = Arc::new(
            prefrontal_routing_store::SqliteRouteStateStore::open(
                &prefrontal_routing_store::dev_descriptor(
                    "call-flush-timer",
                    path.to_string_lossy().into_owned(),
                ),
            )
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
    match tokio::task::spawn_blocking(move || store.flush_routing_calls()).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error, "routing call count flush failed; the next tick will retry")
        }
        Err(error) => tracing::warn!(%error, "routing call count flush worker failed"),
    }
}
