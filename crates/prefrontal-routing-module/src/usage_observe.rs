//! Coalesces Claude subscription response headers before forwarding them to Insula.
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;

pub(crate) const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const ADVERTISEMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
const MAX_ACCOUNTS: usize = 512;
const IGNORE_LOG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageObservation {
    pub provider: &'static str,
    pub account: String,
    pub observed_at_ms: i64,
    pub headers: BTreeMap<String, String>,
}

/// Build a reading only from an admitted Anthropic subscription credential.
pub(crate) fn observation_for_admission(
    provider: &str,
    account_id: Option<&str>,
    anthropic_subscription: bool,
    observed_at_ms: i64,
    headers: BTreeMap<String, String>,
) -> Option<UsageObservation> {
    if !anthropic_subscription || !matches!(provider, "anthropic" | "claude") || headers.is_empty()
    {
        return None;
    }
    let account = account_id?.trim();
    if account.is_empty() {
        return None;
    }
    let headers = headers
        .into_iter()
        .filter(|(name, _)| name.starts_with("anthropic-ratelimit-unified-"))
        .collect::<BTreeMap<_, _>>();
    (!headers.is_empty()).then(|| UsageObservation {
        provider: "claude",
        account: account.to_string(),
        observed_at_ms,
        headers,
    })
}

#[derive(Clone, Default)]
pub(crate) struct SharedUsageObserve(Arc<Inner>);

#[derive(Default)]
struct Inner {
    state: Mutex<Buffer>,
    wake: Notify,
}

#[derive(Default)]
struct Buffer {
    accounts: HashMap<String, AccountReading>,
    ignored_logs: HashMap<(String, String), Instant>,
}

struct AccountReading {
    latest: UsageObservation,
    signature: HeaderSignature,
    first_seen: Instant,
    last_sent: Option<Instant>,
    pending: bool,
    immediate: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct HeaderSignature(BTreeMap<String, String>);

impl HeaderSignature {
    fn from_headers(headers: &BTreeMap<String, String>) -> Self {
        let relevant = headers
            .iter()
            .filter(|(name, _)| {
                let Some(window) = name
                    .strip_prefix("anthropic-ratelimit-unified-")
                    .and_then(|rest| rest.split_once('-'))
                    .map(|(window, _)| window)
                else {
                    return false;
                };
                matches!(window, "5h" | "7d" | "overall")
                    && (name.ends_with("-status") || name.ends_with("-reset"))
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        Self(relevant)
    }
}

impl SharedUsageObserve {
    pub(crate) fn record(&self, observation: UsageObservation) {
        self.record_at(observation, Instant::now());
    }

    fn record_at(&self, observation: UsageObservation, now: Instant) {
        let mut wake = false;
        if let Ok(mut buffer) = self.0.state.lock() {
            let signature = HeaderSignature::from_headers(&observation.headers);
            if let Some(reading) = buffer.accounts.get_mut(&observation.account) {
                let changed = reading.signature != signature;
                if changed {
                    reading.immediate = true;
                }
                wake = changed || !reading.pending;
                reading.signature = signature;
                reading.latest = observation;
                reading.pending = true;
            } else {
                if buffer.accounts.len() >= MAX_ACCOUNTS {
                    let oldest_clean = buffer
                        .accounts
                        .iter()
                        .filter(|(_, reading)| !reading.pending)
                        .min_by_key(|(_, reading)| reading.last_sent.unwrap_or(reading.first_seen))
                        .map(|(account, _)| account.clone());
                    if let Some(account) = oldest_clean {
                        buffer.accounts.remove(&account);
                    } else {
                        return;
                    }
                }
                wake = true;
                buffer.accounts.insert(
                    observation.account.clone(),
                    AccountReading {
                        latest: observation,
                        signature,
                        first_seen: now,
                        last_sent: None,
                        pending: true,
                        immediate: false,
                    },
                );
            }
        }
        if wake {
            self.0.wake.notify_one();
        }
    }

    pub(crate) fn next_due(&self) -> Option<Instant> {
        let buffer = self.0.state.lock().ok()?;
        buffer
            .accounts
            .values()
            .filter(|reading| reading.pending)
            .map(|reading| {
                if reading.immediate {
                    Instant::now()
                } else {
                    reading.last_sent.unwrap_or(reading.first_seen) + FLUSH_INTERVAL
                }
            })
            .min()
    }

    pub(crate) async fn notified(&self) {
        self.0.wake.notified().await;
    }

    pub(crate) fn take_due_at(&self, now: Instant) -> Vec<UsageObservation> {
        let Ok(mut buffer) = self.0.state.lock() else {
            return Vec::new();
        };
        let due_accounts: Vec<_> = buffer
            .accounts
            .iter()
            .filter(|(_, reading)| {
                reading.pending
                    && (reading.immediate
                        || now >= reading.last_sent.unwrap_or(reading.first_seen) + FLUSH_INTERVAL)
            })
            .map(|(account, _)| account.clone())
            .collect();
        due_accounts
            .into_iter()
            .filter_map(|account| {
                let reading = buffer.accounts.get_mut(&account)?;
                reading.pending = false;
                reading.immediate = false;
                reading.last_sent = Some(now);
                Some(reading.latest.clone())
            })
            .collect()
    }

    pub(crate) fn log_ignored_once(&self, account: &str, reason: &str, now: Instant) -> bool {
        if !matches!(
            reason,
            "unknown_account"
                | "no_poll_reading_yet"
                | "older_than_held"
                | "no_usable_window"
                | "unsupported_provider"
        ) {
            return false;
        }
        let Ok(mut buffer) = self.0.state.lock() else {
            return false;
        };
        buffer
            .ignored_logs
            .retain(|_, logged_at| now.duration_since(*logged_at) < IGNORE_LOG_INTERVAL);
        let key = (account.to_string(), reason.to_string());
        if buffer
            .ignored_logs
            .get(&key)
            .is_some_and(|logged_at| now.duration_since(*logged_at) < IGNORE_LOG_INTERVAL)
        {
            return false;
        }
        if buffer.ignored_logs.len() >= MAX_ACCOUNTS * 5 {
            if let Some(oldest) = buffer
                .ignored_logs
                .iter()
                .min_by_key(|(_, logged_at)| **logged_at)
                .map(|(key, _)| key.clone())
            {
                buffer.ignored_logs.remove(&oldest);
            }
        }
        buffer.ignored_logs.insert(key, now);
        true
    }
}

/// Extract ignored observations from Insula's normal, non-failing batch reply.
pub(crate) fn ignored_observations(reply: &Value) -> Vec<(String, String)> {
    reply
        .get("ignored")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item.get("account")?.as_str()?.to_string(),
                item.get("reason")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

#[async_trait::async_trait]
trait UsageObserveRoute: Send {
    async fn usage_observe(&mut self, observations: &[UsageObservation]) -> Result<Value, String>;
}

#[async_trait::async_trait]
impl UsageObserveRoute for crate::quota_consumer::QuotaConsumer {
    async fn usage_observe(&mut self, observations: &[UsageObservation]) -> Result<Value, String> {
        self.usage_observe_on_open_route(observations)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn send_if_advertised<R: UsageObserveRoute + ?Sized>(
    route: &mut R,
    advertised: bool,
    observations: &[UsageObservation],
) -> Result<Option<Value>, String> {
    if !advertised {
        return Ok(None);
    }
    route.usage_observe(observations).await.map(Some)
}

#[derive(Default)]
struct AdvertisementCache {
    advertised: Option<bool>,
    checked_at: Option<Instant>,
}

impl AdvertisementCache {
    fn due(&self, now: Instant) -> bool {
        self.checked_at
            .is_none_or(|checked_at| now >= checked_at + ADVERTISEMENT_REFRESH_INTERVAL)
    }

    fn update(&mut self, advertised: bool, now: Instant) {
        self.advertised = Some(advertised);
        self.checked_at = Some(now);
    }

    fn next_check(&self) -> Option<Instant> {
        self.checked_at
            .map(|checked_at| checked_at + ADVERTISEMENT_REFRESH_INTERVAL)
    }
}

async fn refresh_advertisement(
    route: &mut Option<crate::quota_consumer::QuotaConsumer>,
    cache: &mut AdvertisementCache,
) -> Result<(), String> {
    let result = match route.as_mut() {
        Some(client) => client
            .usage_observe_advertised_on_open_route()
            .await
            .map_err(|error| error.to_string()),
        None => return Ok(()),
    };
    match result {
        Ok(advertised) => {
            cache.update(advertised, Instant::now());
            Ok(())
        }
        Err(error) => {
            *route = None;
            *cache = AdvertisementCache::default();
            Err(error)
        }
    }
}

pub(crate) async fn run(connection_file: PathBuf, buffer: SharedUsageObserve) {
    let project_root =
        prefrontal_core_store::scratch_root().join("prefrontal-routing-usage-observe");
    let _ = std::fs::create_dir_all(&project_root);
    let mut route = None::<crate::quota_consumer::QuotaConsumer>;
    let mut advertisement = AdvertisementCache::default();
    let mut retry_at = Instant::now();

    loop {
        let now = Instant::now();
        let batch = buffer.take_due_at(now);
        if !batch.is_empty() {
            if route.is_none() && now >= retry_at {
                match crate::quota_consumer::QuotaConsumer::connect(&connection_file, &project_root)
                    .await
                {
                    Ok(connected) => {
                        route = Some(connected);
                        advertisement = AdvertisementCache::default();
                    }
                    Err(error) => {
                        eprintln!(
                            "[prefrontal-routing] Insula usage observer connection failed: {error}"
                        );
                        retry_at = Instant::now() + RECONNECT_INTERVAL;
                    }
                }
            }
            if route.is_some() && advertisement.due(Instant::now()) {
                if let Err(error) = refresh_advertisement(&mut route, &mut advertisement).await {
                    eprintln!("[prefrontal-routing] Insula operation catalog failed: {error}");
                    retry_at = Instant::now() + RECONNECT_INTERVAL;
                }
            }
            if route.is_some() {
                let result = send_if_advertised(
                    route.as_mut().unwrap(),
                    advertisement.advertised == Some(true),
                    &batch,
                )
                .await;
                match result {
                    Ok(Some(reply)) => {
                        let result = reply.get("result").unwrap_or(&reply);
                        for (account, reason) in ignored_observations(result) {
                            if buffer.log_ignored_once(&account, &reason, Instant::now()) {
                                eprintln!(
                                    "[prefrontal-routing] Insula ignored usage observation for account {account}: {reason}"
                                );
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!(
                            "[prefrontal-routing] dropped Insula usage observation batch: {error}"
                        );
                        route = None;
                        advertisement = AdvertisementCache::default();
                        retry_at = Instant::now() + RECONNECT_INTERVAL;
                    }
                }
            }
        }

        if route.is_some() && advertisement.due(Instant::now()) {
            if let Err(error) = refresh_advertisement(&mut route, &mut advertisement).await {
                eprintln!("[prefrontal-routing] Insula operation catalog failed: {error}");
                retry_at = Instant::now() + RECONNECT_INTERVAL;
            }
        }

        let now = Instant::now();
        let mut wake_at = buffer
            .next_due()
            .unwrap_or(now + Duration::from_secs(24 * 60 * 60));
        if route.is_some() {
            if let Some(check) = advertisement.next_check() {
                wake_at = wake_at.min(check);
            }
        }
        tokio::select! {
            _ = buffer.notified() => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct UnadvertisedRoute {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl UsageObserveRoute for UnadvertisedRoute {
        async fn usage_observe(
            &mut self,
            _observations: &[UsageObservation],
        ) -> Result<Value, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({ "result": { "accepted": 1, "ignored": [] } }))
        }
    }

    fn observation(account: &str, extra: &[(&str, &str)]) -> UsageObservation {
        let mut headers = BTreeMap::from([(
            "anthropic-ratelimit-unified-5h-utilization".into(),
            "0.4".into(),
        )]);
        headers.extend(
            extra
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        );
        UsageObservation {
            provider: "claude",
            account: account.into(),
            observed_at_ms: 1,
            headers,
        }
    }

    #[test]
    fn observations_coalesce_to_latest_per_account_per_thirty_seconds() {
        let start = Instant::now();
        let buffer = SharedUsageObserve::default();
        for index in 0..20 {
            let mut item = observation("account-a", &[]);
            item.observed_at_ms = index;
            item.headers.insert(
                "anthropic-ratelimit-unified-5h-utilization".into(),
                index.to_string(),
            );
            buffer.record_at(item, start);
        }

        assert!(buffer
            .take_due_at(start + FLUSH_INTERVAL - Duration::from_millis(1))
            .is_empty());
        let batch = buffer.take_due_at(start + FLUSH_INTERVAL);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].observed_at_ms, 19);
        assert!(buffer.take_due_at(start + FLUSH_INTERVAL * 2).is_empty());
    }

    #[test]
    fn status_change_flushes_immediately() {
        let start = Instant::now();
        let buffer = SharedUsageObserve::default();
        buffer.record_at(
            observation(
                "account-a",
                &[("anthropic-ratelimit-unified-5h-status", "allowed")],
            ),
            start,
        );
        assert!(buffer.take_due_at(start + FLUSH_INTERVAL).len() == 1);

        buffer.record_at(
            observation(
                "account-a",
                &[("anthropic-ratelimit-unified-5h-status", "limited")],
            ),
            start + FLUSH_INTERVAL,
        );
        let batch = buffer.take_due_at(start + FLUSH_INTERVAL + Duration::from_millis(1));
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].headers["anthropic-ratelimit-unified-5h-status"],
            "limited"
        );
    }

    #[test]
    fn moved_reset_flushes_immediately() {
        let start = Instant::now();
        let buffer = SharedUsageObserve::default();
        buffer.record_at(
            observation(
                "account-a",
                &[("anthropic-ratelimit-unified-7d-reset", "100")],
            ),
            start,
        );
        buffer.take_due_at(start + FLUSH_INTERVAL);
        buffer.record_at(
            observation(
                "account-a",
                &[("anthropic-ratelimit-unified-7d-reset", "200")],
            ),
            start + FLUSH_INTERVAL,
        );

        assert_eq!(
            buffer
                .take_due_at(start + FLUSH_INTERVAL + Duration::from_millis(1))
                .len(),
            1
        );
    }

    #[test]
    fn missing_accounts_and_non_anthropic_credentials_are_not_forwardable() {
        let headers = BTreeMap::from([(
            "anthropic-ratelimit-unified-5h-status".into(),
            "allowed".into(),
        )]);
        assert!(observation_for_admission("anthropic", None, true, 1, headers.clone()).is_none());
        assert!(
            observation_for_admission("openai", Some("account-a"), true, 1, headers.clone())
                .is_none()
        );
        assert!(
            observation_for_admission("anthropic", Some("account-a"), false, 1, headers).is_none()
        );
    }

    #[test]
    fn only_unified_headers_are_forwarded_verbatim() {
        let observation = observation_for_admission(
            "anthropic",
            Some("account-a"),
            true,
            123,
            BTreeMap::from([
                (
                    "anthropic-ratelimit-unified-overall-status".into(),
                    "limited".into(),
                ),
                ("x-unrelated".into(), "ignore".into()),
            ]),
        )
        .unwrap();
        assert_eq!(observation.provider, "claude");
        assert_eq!(observation.observed_at_ms, 123);
        assert_eq!(observation.headers.len(), 1);
        assert_eq!(
            observation.headers["anthropic-ratelimit-unified-overall-status"],
            "limited"
        );
    }

    #[tokio::test]
    async fn absent_usage_observe_advertisement_makes_no_operation_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut route = UnadvertisedRoute {
            calls: calls.clone(),
        };
        let batch = [observation("account-a", &[])];

        assert!(send_if_advertised(&mut route, false, &batch)
            .await
            .unwrap()
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn operation_advertisement_is_rechecked_after_ten_minutes() {
        let start = Instant::now();
        let mut cache = AdvertisementCache::default();
        assert!(cache.due(start));
        cache.update(false, start);
        assert!(!cache.due(start + ADVERTISEMENT_REFRESH_INTERVAL - Duration::from_millis(1)));
        assert!(cache.due(start + ADVERTISEMENT_REFRESH_INTERVAL));
    }

    #[tokio::test]
    async fn notify_only_when_a_reading_becomes_pending_or_changes_status() {
        let start = Instant::now();
        let buffer = SharedUsageObserve::default();
        buffer.record_at(observation("account-a", &[]), start);
        tokio::time::timeout(Duration::from_millis(10), buffer.notified())
            .await
            .expect("first pending reading wakes the flush task");

        let mut routine = observation("account-a", &[]);
        routine.headers.insert(
            "anthropic-ratelimit-unified-5h-utilization".into(),
            "0.5".into(),
        );
        buffer.record_at(routine, start + Duration::from_secs(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), buffer.notified())
                .await
                .is_err()
        );

        buffer.record_at(
            observation(
                "account-a",
                &[("anthropic-ratelimit-unified-5h-status", "limited")],
            ),
            start + Duration::from_secs(2),
        );
        tokio::time::timeout(Duration::from_millis(10), buffer.notified())
            .await
            .expect("status change wakes the flush task");
    }
}
