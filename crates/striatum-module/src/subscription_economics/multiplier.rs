//! Astrocyte's latest plan and pool rows, read on the existing quota bus connection.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use cortexkit_bus_client::{BucketItem, BusClient};
use serde_json::{json, value::RawValue, Map, Value};

const FALLBACK: Duration = Duration::from_secs(60);
const BACKSTOP: Duration = Duration::from_secs(15 * 60);
const BATCH_WINDOW: Duration = Duration::from_millis(20);
const MAX_BATCH: usize = 64;

#[derive(Default)]
pub(super) struct PollSchedule {
    bound: bool,
    next: Option<Instant>,
}

impl PollSchedule {
    pub(super) fn due(&mut self, bound: bool, now: Instant) -> bool {
        if self.bound != bound {
            self.bound = bound;
            self.next = None;
            crate::info_operator(format!(
                "[striatum] multiplier source={}",
                if bound {
                    "astrocyte KV (15m report backstop)"
                } else {
                    "spend.multiplier.report fallback (60s)"
                }
            ));
        }
        self.next.is_none_or(|next| now >= next)
    }

    pub(super) fn completed(&mut self, now: Instant) {
        self.next = Some(now + self.interval());
    }

    pub(super) fn interval(&self) -> Duration {
        if self.bound {
            BACKSTOP
        } else {
            FALLBACK
        }
    }
}

#[derive(Default)]
pub(super) struct State {
    connection: Option<async_nats::Client>,
    revisions: Option<tokio::sync::watch::Receiver<u64>>,
    entries: Entries,
    initial: Option<Entries>,
    warned: BTreeSet<String>,
    pub(super) ready: bool,
    pub(super) generation: u64,
}

type Entries = BTreeMap<String, Option<Arc<Value>>>;

pub(super) enum Update {
    Put(String, Option<Arc<Value>>),
    Delete(String),
    CaughtUp,
    Reset,
    Other,
}

impl From<BucketItem> for Update {
    fn from(item: BucketItem) -> Self {
        match item {
            BucketItem::Put { key, value, .. } => {
                let row = decode(&value).filter(|row| pool_key_matches(&key, row));
                Self::Put(key, row.map(Arc::new))
            }
            BucketItem::Delete { key, .. } => Self::Delete(key),
            BucketItem::CaughtUp => Self::CaughtUp,
            _ => Self::Other,
        }
    }
}

fn pool_key_prefix(key: &str) -> Option<&str> {
    let (plan_key, pool) = key.rsplit_once('.')?;
    let (provider, plan) = plan_key.split_once('.')?;
    [provider, plan, pool]
        .into_iter()
        .all(|segment| {
            segment.strip_prefix('x').is_some_and(|hex| {
                hex.len() % 2 == 0
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        })
        .then_some(plan_key)
}

fn pool_key_matches(key: &str, row: &Value) -> bool {
    if pool_key_prefix(key).is_none() {
        // Legacy keys are opaque to the consumer.
        return true;
    }
    const HEX: &[u8] = b"0123456789abcdef";
    row["pool_scope"].is_string()
        && key
            .split('.')
            .zip(["quota_provider", "plan", "pool_id"])
            .all(|(segment, field)| {
                row[field].as_str().is_some_and(|text| {
                    let hex = &segment.as_bytes()[1..];
                    hex.len() == text.len() * 2
                        && hex.chunks_exact(2).zip(text.bytes()).all(|(pair, byte)| {
                            pair == [HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]
                        })
                })
            })
}

fn parse_row(value: &[u8]) -> Option<Value> {
    serde_json::from_slice(value).ok().or_else(|| {
        // serde_json rejects overflowing numbers in a Value. Isolate the
        // optional history so an unrepresentable field doesn't lose the plan.
        let mut fields: BTreeMap<String, &RawValue> = serde_json::from_slice(value).ok()?;
        let history = fields.remove("last_eligible")?;
        let mut row = fields
            .into_iter()
            .map(|(key, value)| serde_json::from_str(value.get()).map(|value| (key, value)))
            .collect::<Result<Map<String, Value>, _>>()
            .ok()?;
        if let Ok(fields) = serde_json::from_str::<BTreeMap<String, &RawValue>>(history.get()) {
            let history = fields
                .into_iter()
                .filter_map(|(key, value)| serde_json::from_str(value.get()).ok().map(|v| (key, v)))
                .collect();
            row.insert("last_eligible".into(), Value::Object(history));
        }
        Some(Value::Object(row))
    })
}

fn decode(value: &[u8]) -> Option<Value> {
    let row = parse_row(value)?;
    row["quota_provider"].as_str()?;
    if !matches!(row.get("plan"), Some(Value::Null | Value::String(_))) {
        return None;
    }
    let optional = |object: &Value, field: &str, valid: fn(&Value) -> bool| {
        object
            .get(field)
            .is_none_or(|value| value.is_null() || valid(value))
    };
    if !optional(&row, "routing_eligible", Value::is_boolean)
        || !optional(&row, "multiplier", Value::is_number)
        || !optional(&row, "interval_end_ms", Value::is_i64)
        || !optional(&row, "observation_day", Value::is_string)
        || !optional(&row, "plan_prices", Value::is_array)
    {
        return None;
    }
    for price in row["plan_prices"].as_array().into_iter().flatten() {
        if !price.is_object()
            || !["account_ref", "credential_ref", "plan"]
                .iter()
                .all(|field| optional(price, field, Value::is_string))
            || !optional(price, "monthly_nanos", Value::is_number)
            || !optional(price, "review_by_ms", Value::is_i64)
        {
            return None;
        }
    }
    // Keep ineligible and provider-wide rows for their plan metadata. If the
    // latest plan observation claims eligibility, validate it independently
    // of the optional last eligible observation.
    if row["routing_eligible"] == true
        && row["plan"].is_string()
        && super::latest_measurement(&row).is_none()
    {
        return None;
    }
    Some(row)
}

impl State {
    fn clear_binding(&mut self) {
        self.ready = false;
        self.connection = None;
        self.revisions = None;
    }

    pub(super) fn watch_ready(&self) -> bool {
        self.ready
            && self.connection.as_ref().is_none_or(|connection| {
                connection.connection_state() == async_nats::connection::State::Connected
            })
            && self
                .revisions
                .as_ref()
                .is_none_or(|changes| matches!(changes.has_changed(), Ok(false)))
    }

    pub(super) fn begin(&mut self) {
        self.ready = false;
        self.initial = Some(BTreeMap::new());
    }

    #[cfg(test)]
    pub(super) fn apply(&mut self, item: BucketItem) -> bool {
        self.apply_update(item.into())
    }

    pub(super) fn apply_update(&mut self, item: Update) -> bool {
        let changed = match item {
            Update::Put(key, row) => {
                let row = row.or_else(|| {
                    if self.warned.insert(key.clone()) {
                        tracing::warn!(
                            key,
                            "invalid Astrocyte multiplier value; retaining last good row"
                        );
                    }
                    self.initial
                        .as_ref()
                        .and_then(|rows| rows.get(&key))
                        .and_then(Option::as_ref)
                        .or_else(|| self.entries.get(&key).and_then(Option::as_ref))
                        .cloned()
                });
                // Seeing a pool key opts its plan out of the old plan-wide
                // value, even when the first value under that key is invalid.
                if row.is_some() || pool_key_prefix(&key).is_some() {
                    let rows = self.initial.as_mut().unwrap_or(&mut self.entries);
                    if rows.get(&key) == Some(&row) {
                        false
                    } else {
                        rows.insert(key, row);
                        true
                    }
                } else {
                    false
                }
            }
            Update::Delete(key) => {
                // Forget the old session's backup too, so an invalid later put
                // cannot resurrect a row explicitly deleted during the snapshot.
                let removed = self.entries.remove(&key).is_some();
                if let Some(initial) = self.initial.as_mut() {
                    initial.remove(&key);
                }
                removed
            }
            Update::CaughtUp => {
                if let Some(initial) = self.initial.take() {
                    // Only the completed initial state is authoritative: missing
                    // keys were deleted while this reader was away.
                    self.entries = initial;
                }
                self.ready = true;
                crate::info_operator(format!(
                    "[striatum] multiplier bucket caught_up plans={} keys={:?}",
                    self.entries.len(),
                    self.entries.keys().collect::<Vec<_>>()
                ));
                true
            }
            Update::Reset => {
                // The replacement bucket is a new source; its invalid rows must
                // not recover values from the deleted bucket.
                self.entries.clear();
                self.initial = Some(BTreeMap::new());
                self.warned.clear();
                self.generation = self.generation.wrapping_add(1);
                return true;
            }
            Update::Other => false,
        };
        if !changed || self.initial.is_some() {
            return false;
        }
        self.generation = self.generation.wrapping_add(1);
        true
    }

    pub(super) fn rows(&self) -> Arc<[Arc<Value>]> {
        let scoped: BTreeSet<_> = self
            .entries
            .keys()
            .filter_map(|key| pool_key_prefix(key))
            .collect();
        self.entries
            .iter()
            .filter_map(|(key, row)| {
                (!scoped.contains(key.as_str()))
                    .then_some(row.as_ref())
                    .flatten()
                    .cloned()
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn report(&self) -> Value {
        json!({"latest": self.rows().iter().map(Arc::as_ref).collect::<Vec<_>>()})
    }

    pub(super) fn health(&self) -> Value {
        json!({"caughtUp":self.ready, "planCount":self.entries.len(), "keys":self.entries.keys().collect::<Vec<_>>(), "badValueKeys":self.warned})
    }
}

struct Unbound;

impl Drop for Unbound {
    fn drop(&mut self) {
        if let Ok(mut runtime) = super::cache().lock() {
            runtime.multipliers.clear_binding();
        }
    }
}

/// Borrowing the connected client keeps credential upkeep and the socket shared
/// with the durable event reader; bucket failures never interrupt that reader.
pub(crate) async fn watch(client: &BusClient) {
    let _unbound = Unbound;
    let mut retry = Duration::from_secs(1);
    loop {
        if let Ok(mut runtime) = super::cache().lock() {
            runtime.multipliers.begin();
        }
        let started = Instant::now();
        let result = session(client).await;
        if result.is_ok() || started.elapsed() >= Duration::from_secs(60) {
            retry = Duration::from_secs(1);
        }
        if let Ok(mut runtime) = super::cache().lock() {
            runtime.multipliers.clear_binding();
        }
        if let Err(error) = result {
            tracing::warn!(
                error,
                retry_in_s = retry.as_secs(),
                "Astrocyte multiplier watch unbound"
            );
        }
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(Duration::from_secs(60));
    }
}

fn watch_update(item: cortexkit_bus_client::Result<BucketItem>) -> Result<Update, String> {
    match item {
        Ok(item) => Ok(item.into()),
        Err(cortexkit_bus_client::Error::BucketReset { .. }) => Ok(Update::Reset),
        Err(error) => Err(error.to_string()),
    }
}

async fn session(client: &BusClient) -> Result<(), String> {
    let mut changes = client.connection_changes();
    changes.borrow_and_update();
    let connection = client.connection().client();
    let connected = || connection.connection_state() == async_nats::connection::State::Connected;
    if !connected() {
        return Err("bus disconnected".into());
    }
    // Bucket::watch builds the full name with AccountNames::module_bucket_name.
    let bucket = client
        .bucket("astrocyte", "multiplier")
        .map_err(|e| e.to_string())?;
    let mut watch = tokio::time::timeout(Duration::from_secs(15), bucket.watch())
        .await
        .map_err(|_| "multiplier watch bind timeout".to_owned())?
        .map_err(|e| e.to_string())?;
    if let Ok(mut runtime) = super::cache().lock() {
        runtime.multipliers.connection = Some(connection.clone());
        runtime.multipliers.revisions = Some(changes.clone());
    }
    let mut health = tokio::time::interval(Duration::from_secs(1));
    let mut batch = Vec::new();
    let mut deadline = None;
    loop {
        tokio::select! {
            biased;
            changed = changes.changed() => {
                changed.map_err(|_| "bus credential maintenance stopped".to_owned())?;
                // A fresh watch starts from the latest value of every key, not
                // from the SDK's resumable revision, so a new binding replaces
                // the entire in-memory map.
                return Ok(());
            }
            _ = health.tick() => {
                if !connected() { return Err("bus disconnected".into()); }
            }
            _ = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {
                publish_batch(std::mem::take(&mut batch)).await;
                deadline = None;
            }
            item = watch.next() => {
                let update = watch_update(item)?;
                if matches!(update, Update::Reset) {
                    // Queued updates belong to the deleted bucket, not its replacement.
                    batch.clear();
                    publish_batch(vec![update]).await;
                    deadline = None;
                    continue;
                }
                batch.push(update);
                if batch.len() == MAX_BATCH {
                    publish_batch(std::mem::take(&mut batch)).await;
                    deadline = None;
                } else {
                    // Start a fixed window on the first update. A busy publisher
                    // can merge work but cannot keep pushing publication back.
                    deadline.get_or_insert_with(|| tokio::time::Instant::now() + BATCH_WINDOW);
                }
            }
        }
    }
}

async fn publish_batch(batch: Vec<Update>) {
    let work = super::cache()
        .lock()
        .ok()
        .and_then(|mut runtime| runtime.accept_bucket_batch(batch, crate::now_ms() as i64));
    super::build_and_publish(work).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: Value) -> BucketItem {
        BucketItem::Put {
            key: key.into(),
            revision: 1,
            value: serde_json::to_vec(&value).unwrap().into(),
        }
    }

    #[test]
    fn multiplier_bucket_reset_rebuilds_without_fallback() {
        let mut runtime = super::super::Runtime::default();
        let mut schedule = PollSchedule::default();
        let now = Instant::now();
        let a = json!({"quota_provider":"a", "plan":null, "multiplier":2.0});
        let b = json!({"quota_provider":"b", "plan":null, "multiplier":3.0});
        let b_new = json!({"quota_provider":"b", "plan":null, "multiplier":7.0});
        runtime.multipliers.begin();
        for item in [put("A", a), put("B", b), BucketItem::CaughtUp] {
            runtime.accept_bucket_batch(vec![watch_update(Ok(item)).unwrap()], 0);
        }
        assert_eq!(runtime.multipliers.health()["keys"], json!(["A", "B"]));
        let reset = cortexkit_bus_client::Error::BucketReset {
            bucket: "multiplier".into(),
        };
        runtime.accept_bucket_batch(vec![watch_update(Err(reset)).unwrap()], 0);
        assert_eq!(*runtime.report.value(), json!({"latest":[]}));
        assert!(runtime.multipliers.ready);
        assert!(schedule.due(runtime.multipliers.ready, now));
        schedule.completed(now);
        assert_eq!(schedule.interval(), BACKSTOP);
        for item in [put("B", b_new.clone()), BucketItem::CaughtUp] {
            runtime.accept_bucket_batch(vec![watch_update(Ok(item)).unwrap()], 0);
        }
        assert_eq!(runtime.multipliers.health()["keys"], json!(["B"]));
        assert_eq!(*runtime.report.value(), json!({"latest":[b_new]}));
        assert!(runtime.multipliers.ready);
        assert!(!schedule.due(runtime.multipliers.ready, now + FALLBACK));
    }

    #[test]
    fn multiplier_snapshot_bad_values_use_the_newest_good_row_and_never_resurrect_deletes() {
        let mut state = State::default();
        let old = json!({"quota_provider":"provider", "plan":null, "multiplier":2.0});
        let new = json!({"multiplier":3.0, "quota_provider":"provider", "plan":null});
        state.begin();
        state.apply(put("key", old));
        state.apply(BucketItem::CaughtUp);
        state.begin();
        state.apply(put("key", new.clone()));
        state.apply(put("key", json!({})));
        state.apply(BucketItem::CaughtUp);
        assert_eq!(state.report(), json!({"latest":[new]}));
        state.begin();
        state.apply(BucketItem::Delete {
            key: "key".into(),
            revision: 2,
        });
        state.apply(put("key", json!({})));
        state.apply(BucketItem::CaughtUp);
        assert_eq!(state.report(), json!({"latest":[]}));
        assert_eq!(state.health()["badValueKeys"], json!(["key"]));
    }

    #[test]
    fn multiplier_source_switch_is_one_operator_line() {
        let now = Instant::now();
        let mut schedule = PollSchedule::default();
        crate::take_operator_infos();
        schedule.due(true, now);
        assert_eq!(
            crate::take_operator_infos(),
            ["[striatum] multiplier source=astrocyte KV (15m report backstop)"]
        );
        schedule.due(true, now);
        assert!(crate::take_operator_infos().is_empty());
        schedule.due(false, now);
        assert_eq!(
            crate::take_operator_infos(),
            ["[striatum] multiplier source=spend.multiplier.report fallback (60s)"]
        );
    }

    #[test]
    fn caught_up_multiplier_watch_uses_backstop_even_without_event_durable() {
        let cache = crate::SharedQuotaCache::default();
        assert!(!cache.lock().unwrap().consumer.attached);
        let mut state = State::default();
        state.begin();
        state.apply(BucketItem::CaughtUp);
        let mut schedule = PollSchedule::default();
        let now = Instant::now();
        assert!(schedule.due(state.watch_ready(), now));
        schedule.completed(now);
        assert_eq!(schedule.interval(), Duration::from_secs(900));
    }

    #[test]
    fn multiplier_poll_cadence_uses_watch_binding_and_caught_up_state() {
        let (revision, changes) = tokio::sync::watch::channel(0);
        let start = Instant::now();
        let mut schedule = PollSchedule::default();
        let mut state = State {
            revisions: Some(changes),
            ..Default::default()
        };
        state.begin();
        assert!(!state.watch_ready());
        assert!(schedule.due(state.watch_ready(), start));
        schedule.completed(start);
        assert_eq!(schedule.next, Some(start + Duration::from_secs(60)));
        assert!(!schedule.due(false, start + Duration::from_secs(59)));
        assert!(schedule.due(false, start + Duration::from_secs(60)));

        state.apply(BucketItem::CaughtUp);
        assert!(schedule.due(state.watch_ready(), start));
        schedule.completed(start);
        assert_eq!(schedule.next, Some(start + Duration::from_secs(900)));
        assert!(!schedule.due(true, start + Duration::from_secs(899)));
        assert!(schedule.due(true, start + Duration::from_secs(900)));

        revision.send_replace(1);
        assert!(
            !state.watch_ready(),
            "a caught-up flag on an obsolete watch cannot relax the poll"
        );
        let down = start + Duration::from_secs(1);
        assert!(schedule.due(state.watch_ready(), down));
        schedule.completed(down);
        assert_eq!(schedule.next, Some(down + Duration::from_secs(60)));
        state.revisions.as_mut().unwrap().borrow_and_update();
        state.begin();
        assert!(!schedule.due(state.watch_ready(), down));
        state.apply(BucketItem::CaughtUp);
        assert!(schedule.due(state.watch_ready(), down));
        schedule.completed(down);
        assert_eq!(schedule.next, Some(down + Duration::from_secs(900)));
    }
}
