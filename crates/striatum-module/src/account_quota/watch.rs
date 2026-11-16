//! Hold one caller-scoped vault watch; the listing remains the source of truth.

use super::{SharedAccountQuota, ACCOUNT_REFRESH_INTERVAL};
use cortexkit_claustrum_client::{
    CredentialResolver, CredentialResolverError, ScopedCredentialListing, ScopedWatch,
    ScopedWatchOutcome, WatchAction, WatchNotice, WatchTracker,
};
use serde::Serialize;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::time::Instant;

const WATCH_BACKSTOP: Duration = Duration::from_secs(15 * 60);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub live: bool,
    pub state: &'static str,
    pub epoch: Option<String>,
    pub last_seq: Option<u64>,
    pub last_reread_at_ms: Option<i64>,
    pub listing_poll_secs: u64,
}

impl Default for Health {
    fn default() -> Self {
        Self {
            live: false,
            state: "not_started",
            epoch: None,
            last_seq: None,
            last_reread_at_ms: None,
            listing_poll_secs: ACCOUNT_REFRESH_INTERVAL.as_secs(),
        }
    }
}

#[async_trait::async_trait]
pub(super) trait NoticeStream: Send {
    async fn event(&mut self) -> Result<WatchNotice, ScopedWatchOutcome>;
}

#[async_trait::async_trait]
impl NoticeStream for ScopedWatch {
    async fn event(&mut self) -> Result<WatchNotice, ScopedWatchOutcome> {
        match self.next().await {
            Some(notice) => Ok(notice),
            None => Err(self.closed().await),
        }
    }
}

// Keep the transport seam narrow: tests can hold reads open and deliver notices
// without a daemon or wall-clock deadlines.
#[async_trait::async_trait]
pub(super) trait Route: Send + Sync {
    async fn open(&self) -> Result<Box<dyn NoticeStream>, CredentialResolverError>;
    async fn list(&self) -> Result<ScopedCredentialListing, CredentialResolverError>;
}

pub(super) struct ResolverRoute(pub Arc<dyn CredentialResolver>);

#[async_trait::async_trait]
impl Route for ResolverRoute {
    async fn open(&self) -> Result<Box<dyn NoticeStream>, CredentialResolverError> {
        self.0
            .watch_scoped(None)
            .await
            .map(|watch| Box::new(watch) as _)
    }

    async fn list(&self) -> Result<ScopedCredentialListing, CredentialResolverError> {
        self.0.list_scoped().await
    }
}

fn cadence(live: bool) -> Duration {
    if live {
        WATCH_BACKSTOP
    } else {
        ACCOUNT_REFRESH_INTERVAL
    }
}

struct Inventory {
    tracker: WatchTracker,
    pending: bool,
    confirmed: bool,
    latest_view: Option<String>,
}

impl Default for Inventory {
    fn default() -> Self {
        Self {
            tracker: WatchTracker::default(),
            pending: true,
            confirmed: false,
            latest_view: None,
        }
    }
}

impl Inventory {
    fn notice(&mut self, notice: &WatchNotice) {
        self.latest_view = Some(notice.view.clone());
        match self.tracker.notice(notice) {
            WatchAction::Reread {
                inventory: true, ..
            } => {
                self.pending = true;
                self.confirmed = false;
            }
            WatchAction::Reread {
                inventory: false, ..
            } => {
                // A list read can get ahead of the coalesced notice. When that
                // notice catches up, it confirms the already-applied digest.
                self.confirmed = true;
            }
            _ => {}
        }
    }

    fn take_pending(&mut self) -> bool {
        std::mem::take(&mut self.pending)
    }

    fn applied(&mut self, view: &str, live: bool) {
        self.tracker.inventory_read(view);
        self.confirmed = live && self.latest_view.as_deref() == Some(view);
    }

    fn closed(&mut self, outcome: ScopedWatchOutcome) -> WatchAction {
        self.confirmed = false;
        // A watch can outlive the age limit of its last read. On loss, renew
        // the listing immediately, not at the next polling tick.
        self.pending = true;
        match outcome {
            ScopedWatchOutcome::Unsupported => self.tracker.unknown_method(),
            ScopedWatchOutcome::Refused(_) | ScopedWatchOutcome::Stopped => self.tracker.refused(),
            ScopedWatchOutcome::Ended | ScopedWatchOutcome::NotAccepting => self.tracker.closed(),
            _ => self.tracker.route_lost(),
        }
    }
}

type Read = Pin<Box<dyn Future<Output = Option<String>> + Send>>;
type Opening =
    Pin<Box<dyn Future<Output = Result<Box<dyn NoticeStream>, CredentialResolverError>> + Send>>;

fn publish(
    health: &Health,
    confirmed_view: Option<&str>,
    state: &SharedAccountQuota,
    credentials: &crate::broca_credentials::SharedBrocaCredentials,
) {
    if let Ok(mut state) = state.lock() {
        state.vault_watch = health.clone();
    }
    if let Ok(mut credentials) = credentials.lock() {
        credentials.set_watch_confirmed(confirmed_view);
    }
}

/// Reads and applies are serialized, but notices continue to be consumed during
/// either operation. A boolean remembers all changes received during one read.
pub(super) async fn run<F, A>(
    route: Arc<dyn Route>,
    state: SharedAccountQuota,
    credentials: crate::broca_credentials::SharedBrocaCredentials,
    apply: F,
) where
    F: Fn(Result<ScopedCredentialListing, CredentialResolverError>) -> A + Clone + Send + 'static,
    A: Future<Output = Option<String>> + Send + 'static,
{
    let mut inventory = Inventory::default();
    let mut health = Health {
        state: "connecting",
        listing_poll_secs: 60,
        ..Health::default()
    };
    let mut stream: Option<Box<dyn NoticeStream>> = None;
    let mut opening: Option<Opening> = None;
    let mut reading: Option<Read> = None;
    let mut reconnect = Some(Instant::now());
    let mut next_poll = Instant::now();
    let mut confirming_close = false;
    let mut source = None;
    loop {
        let interval = cadence(health.live);
        health.listing_poll_secs = interval.as_secs();
        if source != Some(health.live) {
            // An operator line, like the quota and multiplier source lines:
            // production's log filter drops plain info events.
            crate::info_operator(format!(
                "[striatum] quota source={} listing_poll_secs={} watch_state={}",
                if health.live {
                    "vault-watch"
                } else {
                    "vault-poll"
                },
                interval.as_secs(),
                health.state,
            ));
            source = Some(health.live);
            next_poll = next_poll.min(Instant::now() + interval);
        }
        publish(
            &health,
            inventory
                .latest_view
                .as_deref()
                .filter(|_| inventory.confirmed),
            &state,
            &credentials,
        );
        if reading.is_none() && inventory.take_pending() {
            let (route, apply) = (Arc::clone(&route), apply.clone());
            reading = Some(Box::pin(async move {
                let listing = route.list().await;
                apply(listing).await
            }));
        }
        if stream.is_none() && opening.is_none() && reconnect.is_some_and(|at| at <= Instant::now())
        {
            reconnect = None;
            inventory.tracker.opened();
            let route = Arc::clone(&route);
            opening = Some(Box::pin(async move { route.open().await }));
        }
        tokio::select! {
            result = async { opening.as_mut().unwrap().await }, if opening.is_some() => {
                opening = None;
                match result {
                    Ok(watch) => {
                        stream = Some(watch);
                        health.state = "awaiting_notice";
                    }
                    Err(error) => {
                        let action = if error.code() == "unknown_method" {
                            inventory.tracker.unknown_method()
                        } else {
                            inventory.tracker.route_lost()
                        };
                        health.state = if action == WatchAction::Fallback { "unsupported" } else { "reconnecting" };
                        reconnect = (action != WatchAction::Fallback).then(|| Instant::now() + ACCOUNT_REFRESH_INTERVAL);
                    }
                }
            }
            event = async { stream.as_mut().unwrap().event().await }, if stream.is_some() => {
                match event {
                    Ok(notice) => {
                        health.live = true;
                        health.state = "live";
                        health.epoch = Some(notice.epoch.clone());
                        health.last_seq = Some(notice.seq);
                        inventory.notice(&notice);
                    }
                    Err(outcome) => {
                        stream = None;
                        health.live = false;
                        let action = inventory.closed(outcome);
                        confirming_close = action == WatchAction::ReadListOnce;
                        health.state = match action {
                            WatchAction::Fallback => "unsupported",
                            WatchAction::Stop => "refused",
                            _ => "reconnecting",
                        };
                        reconnect = match action {
                            WatchAction::ResubscribeAndReread | WatchAction::ReconnectAndReread => Some(Instant::now() + RECONNECT_DELAY),
                            _ => None,
                        };
                    }
                }
            }
            view = async { reading.as_mut().unwrap().await }, if reading.is_some() => {
                reading = None;
                health.last_reread_at_ms = Some(super::now_ms());
                if let Some(view) = view {
                    inventory.applied(&view, health.live);
                    if confirming_close {
                        if inventory.tracker.list_result(true) == WatchAction::RetryLater {
                            reconnect = Some(Instant::now() + ACCOUNT_REFRESH_INTERVAL);
                        }
                    }
                    next_poll = Instant::now() + cadence(health.live);
                } else {
                    // A healthy stream cannot repair a failed read until another
                    // change occurs. Retry failures on the polling cadence too.
                    next_poll = Instant::now() + ACCOUNT_REFRESH_INTERVAL;
                    if confirming_close {
                        inventory.tracker.route_lost();
                        reconnect = Some(Instant::now() + ACCOUNT_REFRESH_INTERVAL);
                    }
                }
                confirming_close = false;
            }
            _ = tokio::time::sleep_until(next_poll), if reading.is_none() => {
                inventory.pending = true;
            }
            _ = async { tokio::time::sleep_until(reconnect.unwrap()).await }, if reconnect.is_some() && opening.is_none() && stream.is_none() => {}
        }
    }
}

#[cfg(test)]
mod tests;
