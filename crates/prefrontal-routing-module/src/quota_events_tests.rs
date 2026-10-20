use super::*;
use cortexkit_bus_inmemory::{InMemoryBus, InMemoryConfig, InMemoryStreamCursor};
use cortexkit_bus_trait::{BusResult, DurableOwner, Headers, Stream, StreamDelivery};
use prefrontal_routing_core::RouteStateStore as _;
use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
use prefrontal_test_support::ScratchDir;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

const SUBJECT: &str = "ck.box_test.event.insula.quota_sampled.v1";
const OWNER: DurableOwner<'_> = DurableOwner::Module("prefrontal-routing");

fn fixture() -> (ScratchDir, LiveQuota) {
    let scratch = ScratchDir::new("routing-quota-events");
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "prefrontal-routing",
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    let live = LiveQuota {
        cache: Arc::new(Mutex::new(Default::default())),
        accounts: Arc::new(Mutex::new(Default::default())),
        store: Arc::new(store),
        snapshot_baseline: Default::default(),
    };
    (scratch, live)
}

fn bus() -> InMemoryBus {
    InMemoryBus::new(
        InMemoryConfig::default()
            .with_durable(OWNER)
            .with_deduplication_window(Duration::ZERO),
    )
}

#[derive(Default)]
struct Source {
    replies: HashMap<String, Vec<u8>>,
    snapshot: Vec<u8>,
    fetch_error: Option<String>,
    snapshot_error: Option<String>,
    snapshots: usize,
}

impl SampleSource for Source {
    async fn fetch(&mut self, key: &str) -> Result<Vec<u8>, String> {
        if let Some(error) = self.fetch_error.take() {
            return Err(error);
        }
        self.replies
            .get(key)
            .cloned()
            .ok_or_else(|| "event_not_found".into())
    }
    async fn snapshot(&mut self) -> Result<Vec<u8>, String> {
        self.snapshots += 1;
        if let Some(error) = self.snapshot_error.take() {
            return Err(error);
        }
        Ok(self.snapshot.clone())
    }
}

struct CountingConnector {
    connects: Arc<AtomicUsize>,
    fail_second_fetch: bool,
}

struct CountingRoute {
    connection: usize,
    fetches: usize,
    fail_second_fetch: bool,
}

#[async_trait::async_trait]
impl InsulaConnector for CountingConnector {
    async fn connect(
        &mut self,
    ) -> Result<Box<dyn InsulaRoute>, crate::quota_consumer::QuotaConsumerError> {
        let connection = self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(CountingRoute {
            connection,
            fetches: 0,
            fail_second_fetch: self.fail_second_fetch,
        }))
    }
}

#[async_trait::async_trait]
impl InsulaRoute for CountingRoute {
    async fn fetch(
        &mut self,
        key: &str,
    ) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        self.fetches += 1;
        if self.fail_second_fetch && self.connection == 0 && self.fetches == 2 {
            return Err(crate::quota_consumer::QuotaConsumerError::Route(
                "route expired".into(),
            ));
        }
        Ok(key.as_bytes().to_vec())
    }

    async fn snapshot(&mut self) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        Ok(b"snapshot".to_vec())
    }

    async fn advertises_usage_observe(
        &mut self,
    ) -> Result<bool, crate::quota_consumer::QuotaConsumerError> {
        Ok(true)
    }

    async fn usage_observe(
        &mut self,
        observations: &[crate::usage_observe::UsageObservation],
    ) -> Result<Value, crate::quota_consumer::QuotaConsumerError> {
        Ok(json!({ "result": { "accepted": observations.len(), "ignored": [] } }))
    }
}

#[tokio::test]
async fn quota_insula_reuses_one_connection_for_events_and_snapshot() {
    let connects = Arc::new(AtomicUsize::new(0));
    let mut source = Insula::with_connector(Box::new(CountingConnector {
        connects: connects.clone(),
        fail_second_fetch: false,
    }));

    for key in ["event-1", "event-2", "event-3"] {
        assert_eq!(source.fetch(key).await.unwrap(), key.as_bytes());
    }
    assert_eq!(source.snapshot().await.unwrap(), b"snapshot");
    let observation = crate::usage_observe::UsageObservation {
        provider: "claude",
        account: "account-a".into(),
        observed_at_ms: 1,
        headers: Default::default(),
    };
    assert!(source
        .usage_observe(&[observation])
        .await
        .unwrap()
        .is_some());

    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn quota_insula_reconnects_on_the_call_after_a_route_error() {
    let connects = Arc::new(AtomicUsize::new(0));
    let mut source = Insula::with_connector(Box::new(CountingConnector {
        connects: connects.clone(),
        fail_second_fetch: true,
    }));

    assert_eq!(source.fetch("event-1").await.unwrap(), b"event-1");
    assert!(source
        .fetch("event-2")
        .await
        .unwrap_err()
        .contains("route expired"));
    assert_eq!(
        connects.load(Ordering::SeqCst),
        1,
        "no retry in the failed call"
    );
    assert_eq!(source.fetch("event-3").await.unwrap(), b"event-3");
    assert_eq!(source.snapshot().await.unwrap(), b"snapshot");

    assert_eq!(connects.load(Ordering::SeqCst), 2);
}

struct NoObserveConnector {
    calls: Arc<AtomicUsize>,
}

struct NoObserveRoute {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl InsulaConnector for NoObserveConnector {
    async fn connect(
        &mut self,
    ) -> Result<Box<dyn InsulaRoute>, crate::quota_consumer::QuotaConsumerError> {
        Ok(Box::new(NoObserveRoute {
            calls: self.calls.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl InsulaRoute for NoObserveRoute {
    async fn fetch(
        &mut self,
        _key: &str,
    ) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        Ok(Vec::new())
    }

    async fn snapshot(&mut self) -> Result<Vec<u8>, crate::quota_consumer::QuotaConsumerError> {
        Ok(Vec::new())
    }

    async fn advertises_usage_observe(
        &mut self,
    ) -> Result<bool, crate::quota_consumer::QuotaConsumerError> {
        Ok(false)
    }

    async fn usage_observe(
        &mut self,
        _observations: &[crate::usage_observe::UsageObservation],
    ) -> Result<Value, crate::quota_consumer::QuotaConsumerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(json!({ "result": { "accepted": 1, "ignored": [] } }))
    }
}

#[tokio::test]
async fn absent_usage_observe_advertisement_makes_no_operation_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut source = Insula::with_connector(Box::new(NoObserveConnector {
        calls: calls.clone(),
    }));
    let observation = crate::usage_observe::UsageObservation {
        provider: "claude",
        account: "account-a".into(),
        observed_at_ms: 1,
        headers: Default::default(),
    };

    assert!(source
        .usage_observe(&[observation])
        .await
        .unwrap()
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct Cursor {
    inner: InMemoryStreamCursor,
    live: LiveQuota,
    acks: usize,
    deliveries: Vec<u64>,
}

impl Cursor {
    async fn new(bus: &InMemoryBus, live: &LiveQuota) -> Self {
        Self {
            inner: bus.consumer(OWNER).await.unwrap(),
            live: live.clone(),
            acks: 0,
            deliveries: vec![],
        }
    }
}

#[async_trait::async_trait]
impl StreamCursor for Cursor {
    async fn next(&mut self) -> BusResult<Option<StreamDelivery>> {
        let delivery = self.inner.next().await?;
        if let Some(delivery) = &delivery {
            self.deliveries.push(delivery.delivery_count);
        }
        Ok(delivery)
    }
    async fn ack(&mut self) -> BusResult<()> {
        assert!(
            self.live.store.quota_snapshot().unwrap().is_some(),
            "ack precedes the store write"
        );
        self.inner.ack().await?;
        self.acks += 1;
        Ok(())
    }
    async fn nak(&mut self, delay: Duration) -> BusResult<()> {
        assert_eq!(delay, REDELIVERY_DELAY);
        // Keep the backend's real settlement behavior but make retries immediate in tests.
        self.inner.nak(Duration::ZERO).await
    }
    async fn term(&mut self, reason: Option<&str>) -> BusResult<()> {
        self.inner.term(reason).await
    }
    async fn in_progress(&mut self) -> BusResult<()> {
        self.inner.in_progress().await
    }
}

fn body(epoch: &str, seq: u64, used: u64) -> String {
    // Producer field order differs from serde_json's sorted Value map. Hash these exact bytes.
    format!(
        r#"{{"provider":"codex","account":"work","apiProvider":"openai","fetchedAt":"2030-01-01T00:00:00Z","usage":{{"primary":{{"usedPercent":{used},"windowMinutes":300}}}},"epoch":"{epoch}","seq":{seq}}}"#
    )
}

fn snapshot_body(body: &str) -> Vec<u8> {
    format!(r#"{{"result":[{body}]}}"#).into_bytes()
}

async fn publish(bus: &InMemoryBus, source: &mut Source, body: &str) -> String {
    let identity: SampleIdentity = serde_json::from_str(body).unwrap();
    let mut key = BTreeMap::from([
        ("provider", json!(identity.provider)),
        ("epoch", json!(identity.epoch)),
        ("seq", json!(identity.seq)),
    ]);
    if let Some(account) = &identity.account {
        key.insert("account", json!(account));
    }
    let key = ContentDigest::of_bytes(&serde_json::to_vec(&key).unwrap()).to_string();
    let digest = ContentDigest::of_bytes(body.as_bytes());
    let mut headers = Headers::from([
        ("kind".into(), "quota_sampled".into()),
        ("provider".into(), identity.provider),
        ("epoch".into(), identity.epoch),
        ("seq".into(), identity.seq.to_string()),
    ]);
    if let Some(account) = identity.account {
        headers.insert("account".into(), account);
    }
    let _ = bus.publish(SUBJECT, &key, digest, headers).await.unwrap();
    source.replies.insert(key.clone(), format!(
        r#"{{"result":{{"event_name":"quota_sampled","event_key":"{key}","digest":"{digest}","body":{body},"published_at":1}}}}"#,
    ).into_bytes());
    key
}

fn used(live: &LiveQuota) -> f64 {
    let snapshot = live.store.quota_snapshot().unwrap().unwrap();
    let entries: Value = serde_json::from_str(&snapshot.windows_json).unwrap();
    entries[0]["usage"]["primary"]["usedPercent"]
        .as_f64()
        .unwrap()
}

#[tokio::test]
async fn exact_body_bytes_are_verified_and_stored_before_ack() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    consume_one(
        &mut cursor,
        &mut source,
        &live,
        &mut SequenceState::default(),
        SUBJECT,
    )
    .await
    .unwrap();
    assert_eq!(cursor.acks, 1);
    assert_eq!(used(&live), 10.0);
    assert_eq!(source.snapshots, 1);
    assert!(!live
        .store
        .get_quota_readings("openai/work")
        .unwrap()
        .is_empty());
    let accounts = account_quota::restore_account_state(live.store.as_ref());
    assert_eq!(
        accounts.readings.readings[&("codex".into(), "work".into())].entry["usage"]["primary"]
            ["usedPercent"],
        10
    );
}

#[test]
fn quota_samples_and_usage_snapshots_share_the_api_provider_join() {
    let (_event_scratch, event_live) = fixture();
    let (_snapshot_scratch, snapshot_live) = fixture();
    let entry = json!({
        "provider": "qwen-cloud",
        "apiProvider": "alibaba-token-plan",
        "account": "work",
        "usage": { "primary": { "usedPercent": 37.0, "windowMinutes": 300 } }
    });

    event_live.store_sample(entry.clone()).unwrap();
    snapshot_live
        .store(
            &serde_json::to_vec(&json!({"result": [entry]})).unwrap(),
            false,
        )
        .unwrap();

    let event_rows = event_live
        .store
        .get_quota_readings("alibaba-token-plan/work")
        .unwrap();
    let snapshot_rows = snapshot_live
        .store
        .get_quota_readings("alibaba-token-plan/work")
        .unwrap();
    assert!(
        !event_rows.is_empty(),
        "the event's published slug must join"
    );
    assert_eq!(event_rows.len(), snapshot_rows.len());
    assert_eq!(event_rows[0].window_key, snapshot_rows[0].window_key);
    assert_eq!(event_rows[0].used_percent, snapshot_rows[0].used_percent);
    assert!(event_live
        .store
        .get_quota_readings("qwen-cloud/work")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn digest_mismatch_refused_and_not_acked() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    let key = publish(&bus, &mut source, &sample).await;
    let reply = source.replies.get_mut(&key).unwrap();
    *reply = String::from_utf8(reply.clone())
        .unwrap()
        .replace("usedPercent\":10", "usedPercent\":99")
        .into_bytes();
    let mut cursor = Cursor::new(&bus, &live).await;
    let error = consume_one(
        &mut cursor,
        &mut source,
        &live,
        &mut SequenceState::default(),
        SUBJECT,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, ConsumeError::Rejected(ref message) if message.contains("digest")),
        "{error:?}"
    );
    assert_eq!(cursor.acks, 0);
    assert!(live.store.quota_snapshot().unwrap().is_none());
    assert!(live.cache.lock().unwrap().windows.is_empty());
    let retry = cursor.inner.next().await.unwrap().unwrap();
    assert_eq!(retry.message.id, key);
    assert_eq!(retry.delivery_count, 2);
}

#[tokio::test]
async fn body_fetch_failure_leaves_message_unacked_for_redelivery() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        fetch_error: Some("route unavailable".into()),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    assert!(matches!(
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT).await,
        Err(ConsumeError::Rejected(_))
    ));
    assert_eq!(cursor.acks, 0);
    assert!(live.store.quota_snapshot().unwrap().is_none());
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(cursor.deliveries, [1, 2]);
    assert_eq!(cursor.acks, 1);
    assert_eq!(used(&live), 10.0);
}

#[tokio::test]
async fn duplicate_and_out_of_order_sequences_are_ignored() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let initial = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&initial),
        ..Default::default()
    };
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    for (seq, percent) in [(1, 10), (2, 80), (2, 5), (1, 1)] {
        publish(&bus, &mut source, &body("epoch-z", seq, percent)).await;
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
            .await
            .unwrap();
    }
    assert_eq!(used(&live), 80.0);
    assert_eq!(
        live.cache.lock().unwrap().windows[0]
            .usage
            .as_ref()
            .unwrap()
            .primary
            .as_ref()
            .unwrap()
            .used_percent,
        80.0
    );
    assert_eq!(
        cursor.acks, 4,
        "safe duplicates are settled without rewriting quota"
    );
    assert_eq!(source.snapshots, 1);
    assert!(cursor.inner.next().await.unwrap().is_none());
}

#[tokio::test]
async fn gap_and_opaque_epoch_change_recover_without_rolling_back_snapshot() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let mut source = Source {
        snapshot: snapshot_body(&body("z", 1, 10)),
        ..Default::default()
    };
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    publish(&bus, &mut source, &body("z", 1, 10)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    source.snapshot = snapshot_body(&body("z", 5, 90));
    publish(&bus, &mut source, &body("z", 4, 60)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(used(&live), 90.0);
    publish(&bus, &mut source, &body("z", 5, 80)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(
        used(&live),
        90.0,
        "backlog after a gap must not roll back the recovery snapshot"
    );
    source.snapshot = snapshot_body(&body("a", 1, 20));
    publish(&bus, &mut source, &body("a", 1, 20)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    publish(&bus, &mut source, &body("z", 6, 99)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(
        used(&live),
        20.0,
        "a retired epoch must not replace the new process's reading"
    );
    assert_eq!(source.snapshots, 4);
    assert_eq!(cursor.acks, 5);
}

#[tokio::test]
async fn missing_body_is_acknowledged_and_replaced_by_one_snapshot() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&body("epoch-z", 20, 80)),
        ..Default::default()
    };
    let key = publish(&bus, &mut source, &sample).await;
    // Insula's exact not-found reply: a normal response body, not an error frame.
    source.replies.insert(
        key.clone(),
        br#"{"error":{"code":"event_not_found","message":"event expired or unknown; read usage.get"}}"#
            .to_vec(),
    );
    let mut cursor = Cursor::new(&bus, &live).await;
    consume_one(
        &mut cursor,
        &mut source,
        &live,
        &mut SequenceState::default(),
        SUBJECT,
    )
    .await
    .unwrap();
    assert_eq!(
        cursor.acks, 1,
        "an expired body never comes back, so retrying is a loop"
    );
    assert_eq!(source.snapshots, 1);
    assert_eq!(used(&live), 80.0);
    assert!(cursor.inner.next().await.unwrap().is_none());
}

#[tokio::test]
async fn deep_backlog_is_acknowledged_unread_with_one_snapshot() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let mut source = Source {
        snapshot: snapshot_body(&body("epoch-z", 50, 70)),
        ..Default::default()
    };
    for seq in 1..=5 {
        publish(&bus, &mut source, &body("epoch-z", seq, 10)).await;
    }
    live.cache.lock().unwrap().consumer.lag = Some(CATCH_UP_LAG + 1);
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    // No body is fetched while far behind: a fetch error would surface here.
    source.fetch_error = Some("fetched while catching up".into());
    for _ in 0..5 {
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
            .await
            .unwrap();
    }
    assert_eq!(cursor.acks, 5);
    assert_eq!(
        source.snapshots, 1,
        "one snapshot stands in for the whole backlog"
    );
    assert_eq!(used(&live), 70.0);
}

#[tokio::test]
async fn store_failure_leaves_message_unacked_and_cursor_uncommitted() {
    let (scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    let db = rusqlite::Connection::open(scratch.path().join("store.db")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_quota_write BEFORE INSERT ON quota_snapshot BEGIN SELECT RAISE(FAIL, 'write refused'); END;").unwrap();
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    assert!(matches!(
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT).await,
        Err(ConsumeError::Rejected(_))
    ));
    assert_eq!(cursor.acks, 0);
    assert!(state.sequences.is_empty());
    assert!(live.cache.lock().unwrap().windows.is_empty());
    db.execute_batch("DROP TRIGGER reject_quota_write;")
        .unwrap();
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(cursor.deliveries, [1, 2]);
    assert_eq!(used(&live), 10.0);
}

#[tokio::test]
async fn poll_fallback_engages_when_down_and_stops_when_attached() {
    let (_scratch, live) = fixture();
    let mut source = Source {
        snapshot: snapshot_body(&body("epoch-z", 1, 10)),
        ..Default::default()
    };
    let start = Instant::now();
    let mut schedule = SnapshotSchedule::new(start);
    let down = InMemoryBus::new(InMemoryConfig::default());
    assert!(
        down.consumer(OWNER).await.is_err(),
        "no durable means the consumer is down"
    );
    poll_if_due(&mut source, &live, &mut schedule, start).await;
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(59),
    )
    .await;
    assert_eq!(source.snapshots, 1);
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(60),
    )
    .await;
    assert_eq!(source.snapshots, 2, "fallback polls once per minute");
    let bus = bus();
    let _cursor = bus.consumer(OWNER).await.unwrap();
    schedule.set_attached(true, start + Duration::from_secs(61));
    live.attached(true, None);
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(61),
    )
    .await;
    for seconds in [121, 181, 241, 600] {
        poll_if_due(
            &mut source,
            &live,
            &mut schedule,
            start + Duration::from_secs(seconds),
        )
        .await;
    }
    assert_eq!(
        source.snapshots, 3,
        "no steady-state minute polling while attached"
    );
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(661),
    )
    .await;
    assert_eq!(
        source.snapshots, 4,
        "the ten-minute inventory backstop remains"
    );
    schedule.set_attached(false, start + Duration::from_secs(662));
    live.attached(false, Some("consumer down".into()));
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(662),
    )
    .await;
    assert_eq!(source.snapshots, 5, "detaching starts fallback immediately");
    let (_, metrics) = crate::quota_health(&live.cache.lock().unwrap(), crate::now_ms());
    assert_eq!(metrics["consumer"]["attached"], false);
    assert_eq!(metrics["consumer"]["lag"], Value::Null);
}

#[tokio::test]
async fn incremental_event_keeps_restored_siblings_and_account_metadata() {
    let (_scratch, live) = fixture();
    let mut entry: Value = serde_json::from_str(&body("epoch-z", 1, 10)).unwrap();
    entry["accountInfo"] = json!({"email": "display@example.test"});
    let sibling = json!({"provider": "deepseek", "usage": {"primary": {"usedPercent": 12}}});
    live.store(
        &serde_json::to_vec(&json!({"result": [entry, sibling]})).unwrap(),
        false,
    )
    .unwrap();
    live.cache.lock().unwrap().restored_from_ms = Some(1);
    live.snapshot_baseline.lock().unwrap().clear();
    let bus = bus();
    let sample = body("epoch-z", 2, 70);
    let mut source = Source::default();
    publish(&bus, &mut source, &sample).await;
    let mut state = SequenceState {
        epoch: Some("epoch-z".into()),
        sequences: HashMap::from([(("codex".into(), Some("work".into())), 1)]),
        ..Default::default()
    };
    let mut cursor = Cursor::new(&bus, &live).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(live.cache.lock().unwrap().windows.len(), 2);
    let restored = account_quota::restore_account_state(live.store.as_ref());
    let entry = &restored.readings.readings[&("codex".into(), "work".into())].entry;
    assert_eq!(entry["usage"]["primary"]["usedPercent"], 70);
    assert_eq!(entry["accountInfo"]["email"], "display@example.test");
}

#[tokio::test]
async fn sequences_are_per_provider_and_optional_account() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let mut cursor = Cursor::new(&bus, &live).await;
    let initial = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&initial),
        ..Default::default()
    };
    let mut state = SequenceState::default();
    publish(&bus, &mut source, &initial).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    for provider in ["deepseek", "qwen-cloud"] {
        let sample = format!(
            r#"{{"provider":"{provider}","usage":{{"primary":{{"usedPercent":30}}}},"epoch":"epoch-z","seq":1}}"#
        );
        publish(&bus, &mut source, &sample).await;
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
            .await
            .unwrap();
    }
    assert_eq!(live.cache.lock().unwrap().windows.len(), 3);
    assert_eq!(state.sequences.len(), 3);
    assert_eq!(cursor.acks, 3);
}

#[tokio::test]
async fn failed_backstop_does_not_restart_minute_polling_while_attached() {
    let (_scratch, live) = fixture();
    let mut source = Source {
        snapshot: snapshot_body(&body("epoch-z", 1, 10)),
        snapshot_error: Some("insula unavailable".into()),
        ..Default::default()
    };
    let start = Instant::now();
    let mut schedule = SnapshotSchedule::new(start);
    schedule.set_attached(true, start);
    poll_if_due(&mut source, &live, &mut schedule, start).await;
    for seconds in [3, 60, 300, 599] {
        poll_if_due(
            &mut source,
            &live,
            &mut schedule,
            start + Duration::from_secs(seconds),
        )
        .await;
    }
    assert_eq!(
        source.snapshots, 1,
        "backstop failure must not turn into steady polling"
    );
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        start + Duration::from_secs(600),
    )
    .await;
    assert_eq!(source.snapshots, 2);
    assert_eq!(used(&live), 10.0);
}
