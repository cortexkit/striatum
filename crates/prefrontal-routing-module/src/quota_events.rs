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

impl LiveQuota {
    /// Event bodies and usage.get snapshots share the reading-log, account, and
    /// quota-snapshot stores. Publish cached readings only after those writes succeed.
    fn store(&self, body: &[u8], incremental: bool) -> Result<(), String> {
        let envelope: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
        let windows = crate::quota_consumer::decode_usage_result(&envelope["result"])
            .map_err(|e| e.to_string())?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "quota cache poisoned")?
            .clone();
        let mut accounts = self
            .accounts
            .lock()
            .map_err(|_| "account cache poisoned")?
            .clone();
        let at_ms = crate::now_ms();
        // A single account event cannot replace the entire restored snapshot.
        if incremental {
            cache.restored_from_ms = None;
        }
        crate::apply_quota_refresh(&mut cache, windows.clone(), at_ms);
        let (rows, completeness) =
            account_quota::apply_usage_reply(&mut accounts, body, at_ms as i64)?;
        crate::write_quota_readings(self.store.as_ref(), &windows, at_ms)?;
        self.store
            .put_account_quota_poll(&rows, &completeness)
            .map_err(|e| e.to_string())?;
        crate::write_quota_snapshot(self.store.as_ref(), &cache.windows, at_ms)?;
        *self.cache.lock().map_err(|_| "quota cache poisoned")? = cache;
        let mut guard = self.accounts.lock().map_err(|_| "account cache poisoned")?;
        // Roster refresh has its own writer; do not overwrite its concurrent result.
        guard.readings = accounts.readings;
        guard.last_insula_error = None;
        if !incremental {
            let mut baseline = self
                .snapshot_baseline
                .lock()
                .map_err(|_| "quota baseline poisoned")?;
            for entry in windows {
                baseline.insert((entry.provider.clone(), entry.account.clone()), entry);
            }
        }
        Ok(())
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

    fn store_sample(&self, mut body: Value) -> Result<(), String> {
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
        self.store(
            &serde_json::to_vec(&json!({"result": [body]})).map_err(|e| e.to_string())?,
            true,
        )
    }

    fn attached(&self, attached: bool, error: Option<String>) {
        if let Ok(mut cache) = self.cache.lock() {
            if cache.consumer.attached != attached || cache.consumer.last_error != error {
                eprintln!(
                    "[prefrontal-routing] quota source={}{}",
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
            }
        }
    }
}

trait SampleSource {
    async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String>;
    async fn snapshot(&mut self) -> Result<Vec<u8>, String>;
}

struct Insula {
    connection_file: PathBuf,
    project_root: PathBuf,
}

impl SampleSource for Insula {
    async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String> {
        let mut consumer = QuotaConsumer::connect(&self.connection_file, &self.project_root)
            .await
            .map_err(|e| e.to_string())?;
        consumer
            .events_get_raw(key)
            .await
            .map_err(|e| e.to_string())
    }
    async fn snapshot(&mut self) -> Result<Vec<u8>, String> {
        let mut consumer = QuotaConsumer::connect(&self.connection_file, &self.project_root)
            .await
            .map_err(|e| e.to_string())?;
        consumer.usage_get_raw().await.map_err(|e| e.to_string())
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
    let message = delivery.message;
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
    if far_behind || retired_epoch {
        state.owe_snapshot = true;
        catch_up_snapshot(source, live, state).await;
        return cursor
            .ack()
            .await
            .map_err(|e| ConsumeError::Bus(e.to_string()));
    }
    catch_up_snapshot(source, live, state).await;
    let result = async {
        if message.subject != subject {
            return Err("unexpected insula event subject".into());
        }
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
                eprintln!("[prefrontal-routing] insula refused quota event {}: {code}", message.id);
            }
            return Ok(());
        }
        let (sample, body) = verify(&message, &bytes)?;
        if !state.stale(&sample) {
            let covered = live.covered_by_snapshot(&body)?;
            if state.needs_snapshot(&sample) || covered == Some(false) {
                cursor.in_progress().await.map_err(|e| e.to_string())?;
                // A snapshot read after this event includes at least its observation;
                // applying the earlier body afterwards could roll that snapshot back.
                live.store(&source.snapshot().await?, false)?;
                if live.covered_by_snapshot(&body)?.is_none() {
                    live.store_sample(body)?;
                }
            } else if covered.is_none() {
                live.store_sample(body)?;
            }
            state.commit(sample);
        }
        Ok::<_, String>(())
    }
    .await;
    if let Err(error) = result {
        // NAK releases the cursor without acknowledging processing; the durable retries it.
        cursor
            .nak(REDELIVERY_DELAY)
            .await
            .map_err(|e| ConsumeError::Bus(e.to_string()))?;
        return Err(ConsumeError::Rejected(error));
    }
    cursor
        .ack()
        .await
        .map_err(|e| ConsumeError::Bus(e.to_string()))
}

async fn snapshot<S: SampleSource>(source: &mut S, live: &LiveQuota) -> bool {
    let result = match source.snapshot().await {
        Ok(body) => live.store(&body, false),
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

async fn update_lag(bus: &quota_bus::AttachedBus, live: &LiveQuota) -> Result<(), String> {
    let context = async_nats::jetstream::new(bus.connection.client());
    let consumer = context
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            "m_prefrontal-routing",
            &bus.stream,
        )
        .await
        .map_err(|e| format!("quota consumer info unavailable: {e}"))?;
    let info = consumer.cached_info();
    if let Ok(mut cache) = live.cache.lock() {
        cache.consumer.lag = Some(info.num_pending);
        cache.consumer.ack_pending = Some(info.num_ack_pending);
    }
    Ok(())
}

pub(super) async fn run(
    connection_file: PathBuf,
    cache: SharedQuotaCache,
    accounts: account_quota::SharedAccountQuota,
    store: SharedStore,
) {
    let live = LiveQuota {
        cache,
        accounts,
        store,
        snapshot_baseline: Default::default(),
    };
    let project_root =
        prefrontal_core_store::scratch_root().join("prefrontal-routing-quota-consumer");
    let _ = std::fs::create_dir_all(&project_root);
    let mut source = Insula {
        connection_file: connection_file.clone(),
        project_root,
    };
    let mut state = SequenceState::default();
    let mut bus = None::<quota_bus::AttachedBus>;
    let mut schedule = SnapshotSchedule::new(Instant::now());
    let mut next_attach = Instant::now();
    let mut next_lag = Instant::now();
    live.attached(false, Some("connecting to quota durable".into()));
    loop {
        if bus.is_none() && Instant::now() >= next_attach {
            match quota_bus::attach(connection_file.clone()).await {
                Ok(attached) => {
                    bus = Some(attached);
                    live.attached(true, None);
                    schedule.set_attached(true, Instant::now());
                    next_lag = Instant::now();
                }
                Err(error) => live.attached(false, Some(error)),
            }
            next_attach = Instant::now() + crate::QUOTA_RETRY_INTERVAL;
        }
        poll_if_due(&mut source, &live, &mut schedule, Instant::now()).await;
        let Some(attached) = bus.as_mut() else {
            tokio::time::sleep_until(next_attach.into()).await;
            continue;
        };
        let mut down = None;
        if attached.connection.client().connection_state()
            != async_nats::connection::State::Connected
        {
            down = Some("quota bus disconnected".into());
        } else if crate::now_ms() >= attached.renew_at_ms {
            down = Some("refreshing quota bus credential".into());
        } else if Instant::now() >= next_lag {
            if let Err(error) = update_lag(attached, &live).await {
                down = Some(error);
            }
            next_lag = Instant::now() + Duration::from_secs(10);
        }
        if down.is_none() {
            match consume_one(
                &mut attached.cursor,
                &mut source,
                &live,
                &mut state,
                &attached.subject,
            )
            .await
            {
                Ok(()) => live.attached(true, None),
                Err(ConsumeError::Rejected(error)) => live.attached(true, Some(error)),
                Err(ConsumeError::Bus(error)) => down = Some(error),
            }
        }
        if let Some(error) = down {
            bus = None;
            live.attached(false, Some(error));
            schedule.set_attached(false, Instant::now());
            next_attach = Instant::now() + crate::QUOTA_RETRY_INTERVAL;
        }
    }
}

#[cfg(test)]
#[path = "quota_events_tests.rs"]
mod tests;
