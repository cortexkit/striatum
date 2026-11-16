//! Quota delivery uses insula's body-by-key notices; snapshots recover missing history.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cortexkit_bus_trait::{ContentDigest, Message, StreamCursor};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};

use crate::{
    account_quota, quota_bus, quota_consumer::QuotaConsumer, SharedQuotaCache, SharedStore,
};

const FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
const BACKSTOP_INTERVAL: Duration = Duration::from_secs(600);
const REDELIVERY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConsumerHealth {
    pub attached: bool,
    pub lag: Option<u64>,
    pub ack_pending: Option<usize>,
    pub last_error: Option<String>,
    pub route_facts: crate::route_facts::Health,
}

struct SnapshotSchedule {
    attached: bool,
    next: Instant,
}

impl SnapshotSchedule {
    fn new(now: Instant) -> Self {
        Self {
            attached: false,
            next: now,
        }
    }
    fn set_attached(&mut self, attached: bool, now: Instant) {
        if self.attached != attached {
            self.attached = attached;
            self.next = now;
        }
    }
    fn due(&self, now: Instant) -> bool {
        now >= self.next
    }
    fn completed(&mut self, now: Instant, success: bool) {
        self.next = now
            + if self.attached {
                BACKSTOP_INTERVAL
            } else if !success {
                crate::QUOTA_RETRY_INTERVAL
            } else {
                FALLBACK_INTERVAL
            };
    }
}

#[derive(Clone)]
struct LiveQuota {
    cache: SharedQuotaCache,
    accounts: account_quota::SharedAccountQuota,
    store: SharedStore,
    snapshot_baseline: Arc<Mutex<HashMap<AccountKey, crate::ProviderUsage>>>,
}

struct CommittedQuota {
    cache: crate::QuotaCache,
    readings: account_quota::AccountReadings,
    baseline: Option<Vec<(AccountKey, crate::ProviderUsage)>>,
}

/// Generations belong to one live cache, not the persisted snapshot format.
/// Only flush workers take `written_generation`; request readers never do.
#[derive(Clone, Default)]
pub(super) struct SnapshotPersistence {
    publication_generation: u64,
    written_generation: Arc<Mutex<u64>>,
}

struct PendingSnapshot {
    windows: Vec<crate::ProviderUsage>,
    observed_at_ms: u64,
    generation: u64,
    written_generation: Arc<Mutex<u64>>,
}

impl LiveQuota {
    /// Both event bodies and full replies commit their reading log and account
    /// poll together. The periodically flushed quota cache is only a boot bridge.
    fn commit_body(&self, body: &[u8], incremental: bool) -> Result<CommittedQuota, String> {
        let envelope: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
        let windows = crate::quota_consumer::decode_usage_result(&envelope["result"])
            .map_err(|e| e.to_string())?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "quota cache poisoned")?
            .clone();
        let readings = self
            .accounts
            .lock()
            .map_err(|_| "account cache poisoned")?
            .readings
            .clone();
        // Usage merges only readings; the independently refreshed vault roster
        // need not be copied or restored by an Insula event.
        let mut accounts = account_quota::AccountQuotaState {
            readings,
            ..Default::default()
        };
        let at_ms = crate::now_ms();
        // A single account event cannot replace the entire restored snapshot.
        if incremental {
            cache.restored_from_ms = None;
        }
        let (rows, completeness) =
            account_quota::apply_usage_reply(&mut accounts, body, at_ms as i64)?;
        let log_rows = crate::quota_reading_rows(self.store.as_ref(), &windows, at_ms)?;
        let baseline = (!incremental).then(|| {
            windows
                .iter()
                .map(|entry| {
                    (
                        (entry.provider.clone(), entry.account.clone()),
                        entry.clone(),
                    )
                })
                .collect::<Vec<_>>()
        });
        crate::apply_quota_refresh(&mut cache, windows, at_ms);
        self.store
            .apply_quota_delta(&log_rows, &rows, &completeness)
            .map_err(|e| e.to_string())?;
        Ok(CommittedQuota {
            cache,
            readings: accounts.readings,
            baseline,
        })
    }

    fn publish(&self, committed: CommittedQuota) -> Result<(), String> {
        let mut cache = self.cache.lock().map_err(|_| "quota cache poisoned")?;
        let mut guard = self.accounts.lock().map_err(|_| "account cache poisoned")?;
        let mut baseline = self
            .snapshot_baseline
            .lock()
            .map_err(|_| "quota baseline poisoned")?;
        // Delivery health can change while the blocking store job is running.
        let consumer = cache.consumer.clone();
        let mut persistence = cache.snapshot_persistence.clone();
        persistence.publication_generation = persistence
            .publication_generation
            .checked_add(1)
            .ok_or("quota publication generation exhausted")?;
        *cache = committed.cache;
        cache.consumer = consumer;
        cache.snapshot_persistence = persistence;
        // Roster refresh has its own writer; do not overwrite its concurrent result.
        guard.readings = committed.readings;
        guard.last_insula_error = None;
        if let Some(entries) = committed.baseline {
            for (key, entry) in entries {
                baseline.insert(key, entry);
            }
        }
        Ok(())
    }

    async fn store_body(&self, body: Vec<u8>, incremental: bool) -> Result<(), String> {
        let live = self.clone();
        #[cfg(test)]
        let test_thread = crate::TestThreadState::capture();
        let committed = crate::stage_timing::spawn_job_blocking(move || {
            #[cfg(test)]
            let _test_thread = test_thread.install();
            live.commit_body(&body, incremental)
        })
        .await
        .map_err(|e| e.to_string())??;
        // Publish on the consumer task, not in the detached blocking job. A
        // clean shutdown aborts delivery before taking its final cache snapshot.
        self.publish(committed)
    }

    #[cfg(test)]
    fn store(&self, body: &[u8], incremental: bool) -> Result<(), String> {
        self.publish(self.commit_body(body, incremental)?)
    }

    /// A recovery snapshot may already be ahead of the durable's backlog. Until
    /// the matching sample arrives, use snapshots rather than replaying older
    /// states over it. No wall-clock or opaque-epoch ordering is needed.
    fn covered_by_snapshot(&self, body: &Value) -> Result<Option<bool>, String> {
        let entry: crate::ProviderUsage =
            serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
        let key = (entry.provider.clone(), entry.account.clone());
        let mut baseline = self
            .snapshot_baseline
            .lock()
            .map_err(|_| "quota baseline poisoned")?;
        let matches = baseline.get(&key).map(|previous| *previous == entry);
        if matches == Some(true) {
            baseline.remove(&key);
        }
        Ok(matches)
    }

    fn sample_body(&self, mut body: Value) -> Result<Vec<u8>, String> {
        // Notices deliberately omit display metadata; retain it from the last inventory.
        if let (Some(provider), Some(account)) =
            (body["provider"].as_str(), body["account"].as_str())
        {
            let info = self
                .accounts
                .lock()
                .map_err(|_| "account cache poisoned")?
                .readings
                .readings
                .get(&(provider.to_owned(), account.to_owned()))
                .and_then(|r| r.entry.get("accountInfo"))
                .cloned();
            if let Some(info) = info {
                body["accountInfo"] = info;
            }
        }
        serde_json::to_vec(&json!({"result": [body]})).map_err(|e| e.to_string())
    }

    #[cfg(test)]
    fn store_sample(&self, body: Value) -> Result<(), String> {
        self.store(&self.sample_body(body)?, true)
    }

    fn attached(&self, attached: bool, error: Option<String>) {
        if let Ok(mut cache) = self.cache.lock() {
            if cache.consumer.attached != attached || cache.consumer.last_error != error {
                eprintln!(
                    "[striatum] quota source={}{}",
                    if attached {
                        "insula events (10m snapshot backstop)"
                    } else {
                        "usage.get fallback (60s)"
                    },
                    error.as_ref().map(|e| format!("; {e}")).unwrap_or_default()
                );
            }
            cache.consumer.attached = attached;
            cache.consumer.last_error = error;
            if !attached {
                cache.consumer.lag = None;
                cache.consumer.ack_pending = None;
                cache.consumer.route_facts.filters_ready = false;
            }
        }
    }
}

trait SampleSource {
    async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String>;
    async fn snapshot(&mut self) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
trait InsulaRoute: Send {
    async fn fetch(
        &mut self,
        key: &str,
    ) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError>;
    async fn snapshot(&mut self) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError>;
}

#[async_trait::async_trait]
trait InsulaConnector: Send {
    async fn connect(
        &mut self,
    ) -> Result<Box<dyn InsulaRoute>, crate::quota_consumer::QuotaConsumerError>;
}

struct DaemonInsulaConnector {
    connection_file: PathBuf,
    project_root: PathBuf,
}

#[async_trait::async_trait]
impl InsulaConnector for DaemonInsulaConnector {
    async fn connect(
        &mut self,
    ) -> Result<Box<dyn InsulaRoute>, crate::quota_consumer::QuotaConsumerError> {
        Ok(Box::new(
            QuotaConsumer::connect(&self.connection_file, &self.project_root).await?,
        ))
    }
}

#[async_trait::async_trait]
impl InsulaRoute for QuotaConsumer {
    async fn fetch(
        &mut self,
        key: &str,
    ) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        self.events_get_raw_on_open_route(key).await
    }

    async fn snapshot(&mut self) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        self.usage_get_raw_on_open_route().await
    }
}

struct Insula {
    connector: Box<dyn InsulaConnector>,
    route: Option<Box<dyn InsulaRoute>>,
}

impl Insula {
    fn new(connection_file: PathBuf, project_root: PathBuf) -> Self {
        Self::with_connector(Box::new(DaemonInsulaConnector {
            connection_file,
            project_root,
        }))
    }

    fn with_connector(connector: Box<dyn InsulaConnector>) -> Self {
        Self {
            connector,
            route: None,
        }
    }

    async fn ensure_route(&mut self) -> Result<(), crate::quota_consumer::QuotaConsumerError> {
        if self.route.is_none() {
            self.route = Some(self.connector.connect().await?);
        }
        Ok(())
    }

    fn invalidate_on_error(
        &mut self,
        result: &Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError>,
    ) {
        if result
            .as_ref()
            .is_err_and(crate::quota_consumer::QuotaConsumer::should_reconnect)
        {
            // Let the next request reconnect instead of retrying this operation in place.
            self.route.take();
        }
    }
}

impl SampleSource for Insula {
    async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String> {
        self.ensure_route().await.map_err(|e| e.to_string())?;
        let result = self.route.as_mut().unwrap().fetch(key).await;
        self.invalidate_on_error(&result);
        result.map_err(|e| e.to_string())
    }

    async fn snapshot(&mut self) -> Result<Vec<u8>, String> {
        self.ensure_route().await.map_err(|e| e.to_string())?;
        let result = self.route.as_mut().unwrap().snapshot().await;
        self.invalidate_on_error(&result);
        result.map_err(|e| e.to_string())
    }
}

#[derive(Deserialize)]
struct EventEnvelope {
    result: EventResult,
}
#[derive(Deserialize)]
struct EventResult {
    body: Box<RawValue>,
    digest: String,
    event_key: String,
    event_name: String,
}
#[derive(Deserialize)]
struct SampleIdentity {
    provider: String,
    account: Option<String>,
    epoch: String,
    seq: u64,
}

type AccountKey = (String, Option<String>);
#[derive(Default)]
struct SequenceState {
    epoch: Option<String>,
    retired: HashSet<String>,
    sequences: HashMap<AccountKey, u64>,
    /// Set when samples were acknowledged without their bodies; one `usage.get`
    /// snapshot then stands in for everything they carried.
    owe_snapshot: bool,
    last_catch_up_attempt: Option<Instant>,
    /// Subjects this consumer doesn't read, already logged once each.
    skipped_subjects: HashSet<String>,
}

/// Insula keeps only its newest samples per account and none from before its
/// own restart, so past this many pending events the backlog is acknowledged
/// unread and replaced by one snapshot instead of fetched event by event.
const CATCH_UP_LAG: u64 = 64;
/// Snapshot attempts while catching up, so a failing `usage.get` can't turn a
/// backlog into one route open per event.
const CATCH_UP_SNAPSHOT_EVERY: Duration = Duration::from_secs(30);

async fn catch_up_snapshot<S: SampleSource>(
    source: &mut S,
    live: &LiveQuota,
    state: &mut SequenceState,
) {
    let now = Instant::now();
    if !state.owe_snapshot
        || state
            .last_catch_up_attempt
            .is_some_and(|at| now.duration_since(at) < CATCH_UP_SNAPSHOT_EVERY)
    {
        return;
    }
    state.last_catch_up_attempt = Some(now);
    if snapshot(source, live).await {
        state.owe_snapshot = false;
    }
}

/// Insula answers a refused `events_get` with a normal reply body
/// `{"error":{"code":..}}` rather than an error frame.
fn insula_error_code(bytes: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    value.get("result").is_none().then_some(())?;
    value
        .get("error")?
        .get("code")?
        .as_str()
        .map(str::to_string)
}

impl SequenceState {
    fn stale(&self, sample: &SampleIdentity) -> bool {
        self.retired.contains(&sample.epoch)
            || (self.epoch.as_deref() == Some(&sample.epoch)
                && self
                    .sequences
                    .get(&(sample.provider.clone(), sample.account.clone()))
                    .is_some_and(|seq| sample.seq <= *seq))
    }
    fn needs_snapshot(&self, sample: &SampleIdentity) -> bool {
        self.epoch.as_deref() != Some(&sample.epoch)
            || sample.seq
                > self
                    .sequences
                    .get(&(sample.provider.clone(), sample.account.clone()))
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1)
    }
    fn commit(&mut self, sample: SampleIdentity) {
        if self.epoch.as_deref() != Some(&sample.epoch) {
            if let Some(old) = self.epoch.replace(sample.epoch) {
                self.retired.insert(old);
            }
            self.sequences.clear();
        }
        self.sequences
            .insert((sample.provider, sample.account), sample.seq);
    }
}

fn verify(message: &Message, bytes: &[u8]) -> Result<(SampleIdentity, Value), String> {
    let envelope: EventEnvelope = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let result = envelope.result;
    if result.event_key != message.id
        || result.event_name != "quota_sampled"
        || result.digest != message.digest.to_string()
        || ContentDigest::of_bytes(result.body.get().as_bytes()) != message.digest
    {
        return Err("quota sample digest or event identity mismatch".into());
    }
    let identity: SampleIdentity =
        serde_json::from_str(result.body.get()).map_err(|e| e.to_string())?;
    let hints = &message.headers;
    if identity.epoch.is_empty()
        || identity.provider.is_empty()
        || identity.seq == 0
        || hints.get("kind").map(String::as_str) != Some("quota_sampled")
        || hints.get("provider") != Some(&identity.provider)
        || hints.get("account") != identity.account.as_ref()
        || hints.get("epoch") != Some(&identity.epoch)
        || hints.get("seq") != Some(&identity.seq.to_string())
    {
        return Err("quota sample header/body mismatch".into());
    }
    let mut key = BTreeMap::from([
        ("provider", json!(identity.provider)),
        ("epoch", json!(identity.epoch)),
        ("seq", json!(identity.seq)),
    ]);
    if let Some(account) = &identity.account {
        key.insert("account", json!(account));
    }
    // Identity keys have a canonical sorted order independent of JSON map features.
    let key =
        ContentDigest::of_bytes(&serde_json::to_vec(&key).map_err(|e| e.to_string())?).to_string();
    if key != message.id {
        return Err("quota sample event key mismatch".into());
    }
    let value = serde_json::from_str(result.body.get()).map_err(|e| e.to_string())?;
    Ok((identity, value))
}

#[derive(Debug)]
enum ConsumeError {
    Bus(String),
    Rejected(String),
}

async fn consume_one<C: StreamCursor, S: SampleSource>(
    cursor: &mut C,
    source: &mut S,
    live: &LiveQuota,
    state: &mut SequenceState,
    subject: &str,
) -> Result<(), ConsumeError> {
    let delivery = cursor
        .next()
        .await
        .map_err(|e| ConsumeError::Bus(e.to_string()))?;
    let Some(delivery) = delivery else {
        return Ok(());
    };
    striatum_store::job_timing::run("quota_feed", async {
        let message = delivery.message;
        if message.subject != subject {
            // Only Insula's quota samples are read here. Another subject in this
            // durable's filter will never become readable by redelivery, so it is
            // acknowledged and skipped; NAKing it would redeliver it forever and fill
            // the ack-pending window ahead of quota samples.
            if state.skipped_subjects.insert(message.subject.clone()) {
                eprintln!(
                    "[striatum] quota consumer skipped unread event subject {}",
                    message.subject
                );
            }
            return cursor
                .ack()
                .await
                .map_err(|e| ConsumeError::Bus(e.to_string()));
        }
        let far_behind = live
            .cache
            .lock()
            .ok()
            .and_then(|cache| cache.consumer.lag)
            .is_some_and(|lag| lag > CATCH_UP_LAG);
        let retired_epoch = message
            .headers
            .get("epoch")
            .is_some_and(|epoch| state.retired.contains(epoch));
        if retired_epoch {
            // The newer epoch's own snapshot already replaced this sample's reading.
            return cursor
                .ack()
                .await
                .map_err(|e| ConsumeError::Bus(e.to_string()));
        }
        if far_behind {
            state.owe_snapshot = true;
            catch_up_snapshot(source, live, state).await;
            return cursor
                .ack()
                .await
                .map_err(|e| ConsumeError::Bus(e.to_string()));
        }
        catch_up_snapshot(source, live, state).await;
        let mut permanent = false;
        let result = async {
            let bytes = match source.fetch(&message.id).await {
                Ok(bytes) => bytes,
                Err(error) if error.contains("event_not_found") => {
                    state.owe_snapshot = true;
                    catch_up_snapshot(source, live, state).await;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            // A refusal is final for this event: retrying returns the same answer,
            // so acknowledge it. An expired body is replaced by one snapshot.
            if let Some(code) = insula_error_code(&bytes) {
                if code == "event_not_found" {
                    state.owe_snapshot = true;
                    catch_up_snapshot(source, live, state).await;
                } else {
                    eprintln!(
                        "[striatum] insula refused quota event {}: {code}",
                        message.id
                    );
                }
                return Ok(());
            }
            let (sample, body) = verify(&message, &bytes).map_err(|error| {
                permanent = true;
                error
            })?;
            if !state.stale(&sample) {
                let covered = live.covered_by_snapshot(&body)?;
                if state.needs_snapshot(&sample) || covered == Some(false) {
                    cursor.in_progress().await.map_err(|e| e.to_string())?;
                    // A snapshot read after this event includes at least its observation;
                    // applying the earlier body afterwards could roll that snapshot back.
                    live.store_body(source.snapshot().await?, false).await?;
                    if live.covered_by_snapshot(&body)?.is_none() {
                        live.store_body(live.sample_body(body)?, true).await?;
                    }
                } else if covered.is_none() {
                    live.store_body(live.sample_body(body)?, true).await?;
                }
                state.commit(sample);
            }
            Ok::<_, String>(())
        }
        .await;
        if let Err(error) = result {
            if permanent {
                tracing::warn!(id = message.id, error, "quota notice terminated");
                cursor
                    .term(Some("invalid_quota_notice"))
                    .await
                    .map_err(|e| ConsumeError::Bus(e.to_string()))?;
            } else {
                cursor
                    .nak(REDELIVERY_DELAY)
                    .await
                    .map_err(|e| ConsumeError::Bus(e.to_string()))?;
            }
            return Err(ConsumeError::Rejected(error));
        }
        cursor
            .ack()
            .await
            .map_err(|e| ConsumeError::Bus(e.to_string()))
    })
    .await
}

async fn snapshot<S: SampleSource>(source: &mut S, live: &LiveQuota) -> bool {
    striatum_store::job_timing::run("quota_snapshot_poll", async {
        let result = match source.snapshot().await {
            Ok(body) => live.store_body(body, false).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            if let Ok(mut guard) = live.cache.lock() {
                crate::record_quota_refresh_failure(&mut guard, "usage.get", &error);
            }
            if let Ok(mut guard) = live.accounts.lock() {
                guard.last_insula_error = Some(error);
            }
            false
        } else {
            true
        }
    })
    .await
}

async fn poll_if_due<S: SampleSource>(
    source: &mut S,
    live: &LiveQuota,
    schedule: &mut SnapshotSchedule,
    now: Instant,
) {
    if schedule.due(now) {
        let success = snapshot(source, live).await;
        schedule.completed(now, success);
    }
}

const SNAPSHOT_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

/// Persist only a changed, live cache. The row's timestamp and JSON remain the
/// existing snapshot format; account readings are already durable at each ACK.
pub(super) async fn flush_snapshot(
    cache: SharedQuotaCache,
    store: SharedStore,
) -> Result<(), String> {
    let Some(snapshot) = snapshot_for_flush(&cache)? else {
        return Ok(());
    };
    write_pending_snapshot(snapshot, store).await
}

fn snapshot_for_flush(cache: &SharedQuotaCache) -> Result<Option<PendingSnapshot>, String> {
    let cache = cache.lock().map_err(|_| "quota cache poisoned")?;
    if cache.refreshed_at_ms == 0 || cache.restored_from_ms.is_some() {
        return Ok(None);
    }
    Ok(Some(PendingSnapshot {
        windows: cache.windows.clone(),
        observed_at_ms: cache.refreshed_at_ms,
        generation: cache.snapshot_persistence.publication_generation,
        written_generation: Arc::clone(&cache.snapshot_persistence.written_generation),
    }))
}

async fn write_pending_snapshot(
    snapshot: PendingSnapshot,
    store: SharedStore,
) -> Result<(), String> {
    crate::stage_timing::spawn_job_blocking(move || {
        // A timer's blocking job can outlive its task. Serialize only flushes,
        // skipping an old capture if a newer final flush has already succeeded.
        // Neither the cache lock nor any lock taken by request readers is held.
        let mut written = snapshot
            .written_generation
            .lock()
            .map_err(|_| "quota flush poisoned")?;
        if snapshot.generation <= *written {
            return Ok(());
        }
        crate::write_quota_snapshot(&store, &snapshot.windows, snapshot.observed_at_ms)?;
        *written = snapshot.generation;
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn flush_snapshot_loop(cache: SharedQuotaCache, store: SharedStore, interval: Duration) {
    loop {
        // Sleep after each completed attempt: even a slow flush cannot make the
        // next attempt issue another snapshot commit less than a minute later.
        tokio::time::sleep(interval).await;
        if let Err(error) = striatum_store::job_timing::run(
            "quota_snapshot_flush",
            flush_snapshot(Arc::clone(&cache), Arc::clone(&store)),
        )
        .await
        {
            tracing::warn!(error, "quota snapshot not persisted; will retry");
        }
    }
}

pub(super) async fn finish_clean_exit(
    loops: crate::background::BackgroundLoops,
    cache: SharedQuotaCache,
    store: SharedStore,
) -> Result<SharedStore, String> {
    loops.shutdown().await;
    flush_snapshot(cache, Arc::clone(&store)).await?;
    Ok(store)
}

pub(super) async fn run(
    connection_file: PathBuf,
    cache: SharedQuotaCache,
    accounts: account_quota::SharedAccountQuota,
    store: SharedStore,
    handler: crate::route_facts::Handler,
) {
    let live = LiveQuota {
        cache,
        accounts,
        store,
        snapshot_baseline: Default::default(),
    };
    let project_root =
        std::env::temp_dir().join(format!("{}-quota-consumer", crate::DEFAULT_MODULE_ID));
    let _ = std::fs::create_dir_all(&project_root);
    // Keep the flush timer polling even while delivery waits for a notice or a
    // usage.get reply. Neither side cancels an in-flight delivery at each tick.
    tokio::select! {
        _ = flush_snapshot_loop(Arc::clone(&live.cache), Arc::clone(&live.store), SNAPSHOT_FLUSH_INTERVAL) => {},
        _ = delivery_loop(connection_file, &live, project_root, handler) => {},
    }
}

async fn delivery_loop(
    connection_file: PathBuf,
    live: &LiveQuota,
    project_root: PathBuf,
    handler: crate::route_facts::Handler,
) {
    let mut source = Insula::new(connection_file.clone(), project_root);
    let mut state = SequenceState::default();
    let mut bus = None::<quota_bus::AttachedBus>;
    let mut schedule = SnapshotSchedule::new(Instant::now());
    let mut reconnect = quota_bus::reconnect::Backoff::new(
        Instant::now(),
        crate::now_ms() ^ u64::from(std::process::id()),
    );
    let mut unbound_since = Some(Instant::now());
    live.attached(false, Some("connecting to quota durable".into()));
    loop {
        if bus.is_none() {
            if let Some(result) = reconnect
                .attempt(
                    Instant::now(),
                    striatum_store::job_timing::run(
                        "quota_feed_attach",
                        quota_bus::attach(connection_file.clone(), handler.clone()),
                    ),
                )
                .await
            {
                match result {
                    Ok(attached) => {
                        bus = Some(attached);
                        live.attached(true, None);
                        schedule.set_attached(true, Instant::now());
                        unbound_since = None;
                    }
                    Err(error) => {
                        quota_bus::unbound(&handler, &error);
                    }
                }
            }
        }
        observe_binding(&live.cache, &mut reconnect, Instant::now());
        poll_if_due(&mut source, &live, &mut schedule, Instant::now()).await;
        let Some(attached) = bus.as_mut() else {
            let error = live
                .cache
                .lock()
                .ok()
                .and_then(|cache| cache.consumer.last_error.clone());
            if unbound_since.is_some_and(|since| since.elapsed() > Duration::from_secs(60))
                && !error.as_deref().is_some_and(quota_bus::no_durable)
            {
                tracing::warn!(error, "event reader unbound for more than 60 seconds");
            }
            tokio::time::sleep_until(reconnect.deadline().min(schedule.next).into()).await;
            continue;
        };
        let mut down = None;
        let received = tokio::select! {
            result = attached.deliveries.recv() => result,
            _ = tokio::time::sleep(Duration::from_secs(1)) => { continue; }
        };
        if let Some(Ok(event)) = received {
            let payload = quota_bus::payload(&attached.names, &event);
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error, id = event.msg_id, "malformed event terminated");
                    if let Err(error) = event.term().await {
                        down = Some(error.to_string());
                    }
                    if down.is_none() {
                        continue;
                    }
                    // A settlement failure must reconnect rather than silently losing a notice.
                    bus = None;
                    quota_bus::unbound(
                        &handler,
                        down.as_deref().unwrap_or("event settlement failed"),
                    );
                    reconnect.failed(Instant::now());
                    unbound_since.get_or_insert_with(Instant::now);
                    schedule.set_attached(false, Instant::now());
                    continue;
                }
            };
            let mut cursor = quota_bus::Cursor {
                event: &event,
                acknowledged: false,
                notice: Some(cortexkit_bus_trait::StreamDelivery {
                    stream_seq: event.stream_sequence,
                    delivery_count: payload.delivery_count,
                    message: payload.message.clone(),
                }),
            };
            let handled = handler
                .consume(&mut cursor, &attached.names, &payload)
                .await;
            if handled == Ok(true) {
                if cursor.acknowledged {
                    reconnect.healthy();
                }
                continue;
            }
            if let Err(error) = handled {
                down = Some(error);
            } else if payload.body.len() > 4096 || payload.message.subject != attached.subject {
                tracing::warn!(
                    subject = payload.message.subject,
                    "unknown event terminated"
                );
                if let Err(error) = event.term().await {
                    down = Some(error.to_string());
                }
            } else {
                let before_generation = live
                    .cache
                    .lock()
                    .ok()
                    .map(|cache| cache.snapshot_persistence.publication_generation);
                match consume_one(
                    &mut cursor,
                    &mut source,
                    &live,
                    &mut state,
                    &attached.subject,
                )
                .await
                {
                    Ok(()) => {
                        reset_after_quota_ack(
                            &live,
                            before_generation,
                            cursor.acknowledged,
                            &mut reconnect,
                        );
                    }
                    Err(ConsumeError::Rejected(error)) => {
                        tracing::warn!(error, "quota event refused")
                    }
                    Err(ConsumeError::Bus(error)) => down = Some(error),
                }
            }
        } else {
            down = Some(
                received
                    .and_then(Result::err)
                    .unwrap_or_else(|| "event reader stopped".into()),
            );
        }
        if let Some(error) = down {
            bus = None;
            quota_bus::unbound(&handler, &error);
            unbound_since.get_or_insert_with(Instant::now);
            schedule.set_attached(false, Instant::now());
            reconnect.failed(Instant::now());
        }
    }
}

fn reset_after_quota_ack(
    live: &LiveQuota,
    before_generation: Option<u64>,
    acknowledged: bool,
    reconnect: &mut quota_bus::reconnect::Backoff,
) {
    // Expired quota notices can be ACKed while snapshot recovery is still owed.
    // Only a published reading/snapshot plus its ACK proves application progress.
    let after_generation = live
        .cache
        .lock()
        .ok()
        .map(|cache| cache.snapshot_persistence.publication_generation);
    if acknowledged
        && before_generation
            .zip(after_generation)
            .is_some_and(|(before, after)| after > before)
    {
        reconnect.healthy();
    }
}

pub(crate) fn observe_binding(
    cache: &SharedQuotaCache,
    reconnect: &mut quota_bus::reconnect::Backoff,
    now: Instant,
) {
    let bound_since = cache.lock().ok().and_then(|mut cache| {
        if cache
            .consumer
            .route_facts
            .reader_bound(cache.consumer.attached)
        {
            Some(*cache.consumer.route_facts.bound_since.get_or_insert(now))
        } else {
            cache.consumer.route_facts.bound_since = None;
            None
        }
    });
    reconnect.observe_bound(now, bound_since);
}

#[cfg(test)]
#[path = "quota_events_tests.rs"]
mod tests;

// The request CPU bench depends on this adapter to measure the real ingestion path.
#[cfg(test)]
pub(crate) mod bench {
    use super::*;
    use cortexkit_bus_inmemory::{InMemoryBus, InMemoryConfig, InMemoryStreamCursor};
    use cortexkit_bus_trait::{DurableOwner, Headers, Stream};

    struct Source {
        replies: HashMap<String, Vec<u8>>,
        snapshots: usize,
    }
    impl SampleSource for Source {
        async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String> {
            self.replies
                .get(key)
                .cloned()
                .ok_or_else(|| "event_not_found".into())
        }
        async fn snapshot(&mut self) -> Result<Vec<u8>, String> {
            self.snapshots += 1;
            // With no matching reading in the snapshot, ingestion applies
            // both the full reply and the epoch-start event's body.
            Ok(br#"{"result":[]}"#.to_vec())
        }
    }

    pub(crate) struct QuotaBurst {
        live: LiveQuota,
        source: Source,
        cursor: InMemoryStreamCursor,
        sequence: SequenceState,
        events: usize,
    }
    impl QuotaBurst {
        pub(crate) async fn new(store: SharedStore, events: usize) -> Self {
            let owner = DurableOwner::Module("striatum");
            let bus = InMemoryBus::new(InMemoryConfig::default().with_durable(owner));
            let mut source = Source {
                replies: HashMap::new(),
                snapshots: 0,
            };
            for seq in 1..=events {
                let body = json!({"provider":"codex","account":"cpu-burst","apiProvider":"cpu-provider-0",
                    "fetchedAt": crate::account_selection::rfc3339(crate::now_ms() as i64 + seq as i64 * 1_000),
                    "usage":{"primary":{"usedPercent":seq % 90,"windowMinutes":300}},
                    "epoch":"cpu-epoch","seq":seq});
                let key = ContentDigest::of_bytes(
                    &serde_json::to_vec(&BTreeMap::from([
                        ("provider", json!("codex")),
                        ("account", json!("cpu-burst")),
                        ("epoch", json!("cpu-epoch")),
                        ("seq", json!(seq)),
                    ]))
                    .unwrap(),
                )
                .to_string();
                let digest = ContentDigest::of_bytes(&serde_json::to_vec(&body).unwrap());
                let headers = Headers::from([
                    ("kind".into(), "quota_sampled".into()),
                    ("provider".into(), "codex".into()),
                    ("account".into(), "cpu-burst".into()),
                    ("epoch".into(), "cpu-epoch".into()),
                    ("seq".into(), seq.to_string()),
                ]);
                let _ = bus
                    .publish("cpu.quota", &key, digest, headers)
                    .await
                    .unwrap();
                source.replies.insert(
                    key.clone(),
                    serde_json::to_vec(&json!({"result":{
                    "event_name":"quota_sampled","event_key":key,"digest":digest.to_string(),
                    "body":body,"published_at":1}}))
                    .unwrap(),
                );
            }
            Self {
                live: LiveQuota {
                    cache: Default::default(),
                    accounts: Default::default(),
                    store,
                    snapshot_baseline: Default::default(),
                },
                source,
                cursor: bus.consumer(owner).await.unwrap(),
                sequence: Default::default(),
                events: 0,
            }
        }
        pub(crate) async fn next(&mut self) {
            consume_one(
                &mut self.cursor,
                &mut self.source,
                &self.live,
                &mut self.sequence,
                "cpu.quota",
            )
            .await
            .unwrap();
            self.events += 1;
        }
        pub(crate) fn applied_bodies(&self) -> usize {
            self.events + self.source.snapshots
        }
    }
}
