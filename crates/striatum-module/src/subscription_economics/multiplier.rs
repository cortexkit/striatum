//! Astrocyte's latest plan rows, read on the existing quota bus connection.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use cortexkit_bus_client::{BucketItem, BusClient};
use serde_json::{json, value::RawValue, Map, Value};

const FALLBACK: Duration = Duration::from_secs(60);
const BACKSTOP: Duration = Duration::from_secs(15 * 60);

pub(super) fn reader_bound(cache: &crate::SharedQuotaCache) -> bool {
    cache.lock().is_ok_and(|cache| {
        cache
            .consumer
            .route_facts
            .reader_bound(cache.consumer.attached)
    })
}

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
    entries: BTreeMap<String, Value>,
    initial: Option<BTreeMap<String, Value>>,
    warned: BTreeSet<String>,
    pub(super) ready: bool,
    pub(super) generation: u64,
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
    pub(super) fn begin(&mut self) {
        self.ready = false;
        self.initial = Some(BTreeMap::new());
    }

    pub(super) fn apply(&mut self, item: BucketItem) -> bool {
        match item {
            BucketItem::Put { key, value, .. } => {
                let row = decode(&value).or_else(|| {
                    if self.warned.insert(key.clone()) {
                        tracing::warn!(
                            key,
                            "invalid Astrocyte multiplier value; retaining last good row"
                        );
                    }
                    self.initial
                        .as_ref()
                        .and_then(|rows| rows.get(&key))
                        .or_else(|| self.entries.get(&key))
                        .cloned()
                });
                if let Some(row) = row {
                    self.initial
                        .as_mut()
                        .unwrap_or(&mut self.entries)
                        .insert(key, row);
                }
            }
            BucketItem::Delete { key, .. } => {
                // Forget the old session's backup too, so an invalid later put
                // cannot resurrect a row explicitly deleted during the snapshot.
                self.entries.remove(&key);
                if let Some(initial) = self.initial.as_mut() {
                    initial.remove(&key);
                }
            }
            BucketItem::CaughtUp => {
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
            }
            _ => return false,
        }
        if self.initial.is_some() {
            return false;
        }
        self.generation = self.generation.wrapping_add(1);
        true
    }

    pub(super) fn report(&self) -> Value {
        json!({"latest": self.entries.values().collect::<Vec<_>>()})
    }

    pub(super) fn health(&self) -> Value {
        json!({"caughtUp":self.ready, "planCount":self.entries.len(), "keys":self.entries.keys().collect::<Vec<_>>(), "badValueKeys":self.warned})
    }
}

struct Unbound;

impl Drop for Unbound {
    fn drop(&mut self) {
        if let Ok(mut runtime) = super::cache().lock() {
            runtime.multipliers.ready = false;
        }
    }
}

/// Borrowing the attached client keeps credential upkeep and the socket shared
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
            runtime.multipliers.ready = false;
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
    let mut health = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            biased;
            changed = changes.changed() => {
                changed.map_err(|_| "bus credential maintenance stopped".to_owned())?;
                // A fresh watch starts at LastPerSubject, not the SDK's resumable
                // revision, so a new binding replaces the entire in-memory map.
                return Ok(());
            }
            _ = health.tick() => {
                if !connected() { return Err("bus disconnected".into()); }
            }
            item = watch.next() => {
                let item = item.map_err(|e| e.to_string())?;
                if let Ok(mut runtime) = super::cache().lock() {
                    runtime.bucket_item(item, crate::now_ms() as i64);
                }
            }
        }
    }
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
    fn multiplier_poll_cadence_uses_real_reader_binding_and_caught_up_state() {
        let cache = std::sync::Arc::new(std::sync::Mutex::new(crate::QuotaCache::default()));
        let (revision, changes) = tokio::sync::watch::channel(0);
        {
            let mut cache = cache.lock().unwrap();
            cache.consumer.attached = true;
            cache.consumer.route_facts.revisions = Some(changes);
        }
        let start = Instant::now();
        let mut schedule = PollSchedule::default();
        let mut state = State::default();
        state.begin();
        assert!(reader_bound(&cache));
        assert!(schedule.due(reader_bound(&cache) && state.ready, start));
        schedule.completed(start);
        assert_eq!(schedule.next, Some(start + Duration::from_secs(60)));
        assert!(!schedule.due(false, start + Duration::from_secs(59)));
        assert!(schedule.due(false, start + Duration::from_secs(60)));

        state.apply(BucketItem::CaughtUp);
        assert!(schedule.due(reader_bound(&cache) && state.ready, start));
        schedule.completed(start);
        assert_eq!(schedule.next, Some(start + Duration::from_secs(900)));
        assert!(!schedule.due(true, start + Duration::from_secs(899)));
        assert!(schedule.due(true, start + Duration::from_secs(900)));

        revision.send_replace(1);
        assert!(
            !reader_bound(&cache),
            "an attached flag alone cannot relax the poll"
        );
        let down = start + Duration::from_secs(1);
        assert!(schedule.due(reader_bound(&cache) && state.ready, down));
        schedule.completed(down);
        assert_eq!(schedule.next, Some(down + Duration::from_secs(60)));
        cache
            .lock()
            .unwrap()
            .consumer
            .route_facts
            .revisions
            .as_mut()
            .unwrap()
            .borrow_and_update();
        state.begin();
        assert!(!schedule.due(reader_bound(&cache) && state.ready, down));
        state.apply(BucketItem::CaughtUp);
        assert!(schedule.due(reader_bound(&cache) && state.ready, down));
        schedule.completed(down);
        assert_eq!(schedule.next, Some(down + Duration::from_secs(900)));
    }
}
