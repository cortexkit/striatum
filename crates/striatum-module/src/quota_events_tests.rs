use super::*;
use cortexkit_bus_inmemory::{InMemoryBus, InMemoryConfig, InMemoryStreamCursor};
use cortexkit_bus_trait::{BusResult, DurableOwner, Headers, Stream, StreamDelivery};
use cortexkit_test_support::ScratchDir;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use striatum_core::RouteStateStore as _;
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

#[tokio::test(start_paused = true)]
async fn metadata_timeouts_keep_insula_and_catalog_deliveries_alive_without_reconnect() {
    struct Timeouts {
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl quota_bus::metadata::Source for Timeouts {
        async fn read(&mut self) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }
    let (_scratch, live) = fixture();
    let handler = crate::route_facts::Handler {
        quota_cache: live.cache.clone(),
        store: live.store.clone(),
        credentials: Arc::new(Mutex::new(
            crate::broca_credentials::BrocaCredentials::default(),
        )),
        config_path: PathBuf::new(),
    };
    let wake = handler.credentials.lock().unwrap().catalog_wake.clone();
    let mut reconnect = quota_bus::reconnect::Backoff::new(Instant::now(), 31);
    let mut connects = 0;
    reconnect
        .attempt(Instant::now(), async {
            connects += 1;
            Ok(())
        })
        .await
        .unwrap()
        .unwrap();
    {
        let mut cache = live.cache.lock().unwrap();
        cache.consumer.attached = true;
        cache.consumer.route_facts.filters_ready = true;
    }
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    let catalog = br#"{"catalog_version":1}"#;
    let _ = bus
        .publish(
            "ck.box_test.event.fusiform.catalog_changed.v1",
            "catalog",
            ContentDigest::of_bytes(catalog),
            Headers::new(),
        )
        .await
        .unwrap();
    let mut cursor = Cursor::new(&bus, &live).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut metadata = Timeouts {
        calls: calls.clone(),
    };
    quota_bus::metadata::with_metadata(&mut metadata, &handler, async {
        tokio::time::sleep(Duration::from_secs(6)).await;
        consume_one(
            &mut cursor,
            &mut source,
            &live,
            &mut SequenceState::default(),
            SUBJECT,
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
        let event = cursor.next().await.unwrap().unwrap();
        handler
            .consume(
                &mut cursor,
                &cortexkit_bus_naming::AccountNames::derive("box_test").unwrap(),
                &crate::route_facts::Delivery {
                    message: event.message,
                    body: catalog.to_vec(),
                    published_at_ms: Some(crate::now_ms()),
                    delivery_count: event.delivery_count,
                },
            )
            .await?;
        Ok(())
    })
    .await
    .expect("metadata failure must not tear down the reader session");
    assert_eq!(connects, 1);
    assert!(calls.load(Ordering::SeqCst) >= 1);
    assert_eq!((cursor.acks, cursor.naks, cursor.terms), (2, 0, 0));
    assert_eq!(used(&live), 10.0);
    assert!(tokio::time::timeout(Duration::ZERO, wake.notified())
        .await
        .is_ok());
    let cache = live.cache.lock().unwrap();
    assert!(cache.consumer.attached, "metadata is not reader liveness");
    assert!(!cache.consumer.route_facts.ready(cache.consumer.attached));
}

const SUBJECT: &str = "ck.box_test.event.insula.quota_sampled.v1";
const OWNER: DurableOwner<'_> = DurableOwner::Module("striatum");

fn catalog_handler(live: &LiveQuota) -> crate::route_facts::Handler {
    crate::route_facts::Handler {
        quota_cache: live.cache.clone(),
        store: live.store.clone(),
        credentials: Arc::new(Mutex::new(
            crate::broca_credentials::BrocaCredentials::default(),
        )),
        config_path: PathBuf::new(),
    }
}

#[tokio::test(start_paused = true)]
async fn pending_metadata_does_not_delay_delivery_work() {
    struct Pending;
    #[async_trait::async_trait]
    impl quota_bus::metadata::Source for Pending {
        async fn read(&mut self) -> Result<(), String> {
            std::future::pending().await
        }
    }
    let (_scratch, live) = fixture();
    let handler = catalog_handler(&live);
    let start = tokio::time::Instant::now();
    quota_bus::metadata::with_metadata(&mut Pending, &handler, async {
        assert_eq!(
            tokio::time::Instant::now(),
            start,
            "delivery must not wait for the metadata timeout"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn metadata_errors_back_off_thirty_seconds_to_ten_minutes() {
    struct Refused {
        attempts: Arc<Mutex<Vec<tokio::time::Instant>>>,
    }
    #[async_trait::async_trait]
    impl quota_bus::metadata::Source for Refused {
        async fn read(&mut self) -> Result<(), String> {
            self.attempts
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            Err("consumer-info permission denied".into())
        }
    }
    let (_scratch, live) = fixture();
    let handler = catalog_handler(&live);
    live.cache.lock().unwrap().consumer.attached = true;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let start = tokio::time::Instant::now();
    quota_bus::metadata::with_metadata(
        &mut Refused {
            attempts: attempts.clone(),
        },
        &handler,
        async {
            tokio::time::sleep(Duration::from_secs(2000)).await;
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(
        attempts
            .lock()
            .unwrap()
            .iter()
            .map(|at| at.duration_since(start).as_secs())
            .collect::<Vec<_>>(),
        [0, 30, 90, 210, 450, 930, 1530]
    );
    assert!(live.cache.lock().unwrap().consumer.attached);
}

#[tokio::test(start_paused = true)]
async fn metadata_consumer_not_found_becomes_no_durable_binding_state() {
    struct Absent;
    #[async_trait::async_trait]
    impl quota_bus::metadata::Source for Absent {
        async fn read(&mut self) -> Result<(), String> {
            Err("consumer not found (10014)".into())
        }
    }
    let (_scratch, live) = fixture();
    let handler = catalog_handler(&live);
    {
        let mut cache = live.cache.lock().unwrap();
        cache.consumer.attached = true;
        cache.consumer.route_facts.filters_ready = true;
    }
    let error = quota_bus::metadata::with_metadata(
        &mut Absent,
        &handler,
        std::future::pending::<Result<(), String>>(),
    )
    .await
    .unwrap_err();
    assert!(
        quota_bus::unbound(&handler, &error),
        "first missing-durable state logs its cause"
    );
    assert!(
        !quota_bus::unbound(&handler, "consumer not found on another retry (10014)"),
        "unchanged missing-durable state must not log again"
    );
    {
        let cache = live.cache.lock().unwrap();
        assert!(!cache.consumer.attached);
        assert!(!cache.consumer.route_facts.ready(cache.consumer.attached));
        assert_eq!(
            cache.consumer.last_error.as_deref(),
            Some("no durable yet (consumer not found)")
        );
    }
    assert!(
        quota_bus::unbound(&handler, "NATS connection closed"),
        "a different unbound cause is visible"
    );
    assert!(
        quota_bus::unbound(&handler, &error),
        "returning to a missing durable is a new state"
    );
    let now = Instant::now();
    let mut schedule = SnapshotSchedule::new(now);
    schedule.completed(now, true);
    assert_eq!(schedule.next.duration_since(now), Duration::from_secs(60));
}

#[test]
fn insula_only_consumer_filters_never_enable_route_fact_stream() {
    let names = cortexkit_bus_naming::AccountNames::derive("box_test").unwrap();
    let filters = vec![names.event_subject("insula", "quota_sampled", 1).unwrap()];
    assert!(!crate::route_facts::filters_ready(
        &names,
        Some("m_striatum"),
        &filters
    ));
}

#[tokio::test]
async fn expired_quota_ack_without_publication_does_not_reset_bind_backoff() {
    let (_scratch, live) = fixture();
    live.store_sample(serde_json::from_str(&body("epoch-z", 1, 10)).unwrap())
        .unwrap();
    let before = Some(
        live.cache
            .lock()
            .unwrap()
            .snapshot_persistence
            .publication_generation,
    );
    let bus = bus();
    let sample = body("epoch-z", 2, 20);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        fetch_error: Some("event_not_found".into()),
        snapshot_error: Some("usage unavailable".into()),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    // Expired notices can be ACKed even if the recovery snapshot fails.
    cursor.verify_publication = false;
    consume_one(
        &mut cursor,
        &mut source,
        &live,
        &mut SequenceState::default(),
        SUBJECT,
    )
    .await
    .unwrap();
    assert_eq!((cursor.acks, cursor.naks, cursor.terms), (1, 0, 0));
    assert_eq!(
        Some(
            live.cache
                .lock()
                .unwrap()
                .snapshot_persistence
                .publication_generation
        ),
        before
    );
    let now = Instant::now();
    let mut reconnect = quota_bus::reconnect::Backoff::new(now, 47);
    for _ in 0..8 {
        reconnect.failed(now);
    }
    reset_after_quota_ack(&live, before, true, &mut reconnect);
    reconnect.failed(now);
    assert!(
        reconnect.deadline().duration_since(now) >= Duration::from_secs(48),
        "ACK of an expired notice without a successful snapshot is not application progress"
    );
    live.store_sample(serde_json::from_str(&sample).unwrap())
        .unwrap();
    reset_after_quota_ack(&live, before, true, &mut reconnect);
    reconnect.failed(now);
    assert!(reconnect.deadline().duration_since(now) <= Duration::from_millis(1200));
}

fn fixture() -> (ScratchDir, LiveQuota) {
    let scratch = ScratchDir::new("routing-quota-events");
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "striatum",
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

struct Cursor {
    inner: InMemoryStreamCursor,
    live: LiveQuota,
    acks: usize,
    naks: usize,
    terms: usize,
    adapter_message: Option<Message>,
    verify_publication: bool,
    deliveries: Vec<u64>,
}

impl Cursor {
    async fn new(bus: &InMemoryBus, live: &LiveQuota) -> Self {
        Self {
            inner: bus.consumer(OWNER).await.unwrap(),
            live: live.clone(),
            acks: 0,
            naks: 0,
            terms: 0,
            adapter_message: None,
            verify_publication: true,
            deliveries: vec![],
        }
    }
}

#[async_trait::async_trait]
impl StreamCursor for Cursor {
    async fn next(&mut self) -> BusResult<Option<StreamDelivery>> {
        let mut delivery = self.inner.next().await?;
        if let (Some(delivery), Some(message)) = (&mut delivery, self.adapter_message.take()) {
            delivery.message = message;
        }
        if let Some(delivery) = &delivery {
            self.deliveries.push(delivery.delivery_count);
        }
        Ok(delivery)
    }
    async fn ack(&mut self) -> BusResult<()> {
        if self.verify_publication {
            assert!(
                !self.live.store.account_quota_readings().unwrap().is_empty(),
                "ack precedes the write-through account poll"
            );
            assert!(
                self.live.cache.lock().unwrap().available,
                "ack precedes cache publication"
            );
            assert!(!self
                .live
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .is_empty());
        }
        self.inner.ack().await?;
        self.acks += 1;
        Ok(())
    }
    async fn nak(&mut self, delay: Duration) -> BusResult<()> {
        assert_eq!(delay, REDELIVERY_DELAY);
        self.naks += 1;
        // Keep the backend's real settlement behavior but make retries immediate in tests.
        self.inner.nak(Duration::ZERO).await
    }
    async fn term(&mut self, reason: Option<&str>) -> BusResult<()> {
        self.terms += 1;
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
    let rows = live.store.account_quota_readings().unwrap();
    let entry: Value = serde_json::from_str(&rows[0].entry_json).unwrap();
    entry["usage"]["primary"]["usedPercent"].as_f64().unwrap()
}

fn snapshot_used(live: &LiveQuota) -> f64 {
    let row = live.store.quota_snapshot().unwrap().unwrap();
    let windows: Value = serde_json::from_str(&row.windows_json).unwrap();
    windows[0]["usage"]["primary"]["usedPercent"]
        .as_f64()
        .unwrap()
}

async fn flush(live: &LiveQuota) -> Result<(), String> {
    flush_snapshot(Arc::clone(&live.cache), Arc::clone(&live.store)).await
}

#[tokio::test]
async fn burst_commits_once_per_applied_body_including_epoch_start_reply() {
    let (_scratch, live) = fixture();
    // Load catalogue facts before sampling the writer counter so this measures
    // quota ingestion rather than the first catalogue lookup's bookkeeping.
    live.store.get_facts().unwrap();
    let bus = bus();
    let mut source = Source {
        snapshot: br#"{"result":[]}"#.to_vec(),
        ..Default::default()
    };
    for seq in 1..=30 {
        publish(&bus, &mut source, &body("epoch", seq, seq)).await;
    }
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    let before = live.store.writer_commit_count_for_test();
    for _ in 0..30 {
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
            .await
            .unwrap();
    }
    let applied_bodies = 30 + source.snapshots;
    assert_eq!(source.snapshots, 1);
    assert_eq!(cursor.acks, 30);
    assert_eq!(
        live.store.writer_commit_count_for_test() - before,
        applied_bodies as u64
    );
    assert!(live.store.quota_snapshot().unwrap().is_none());
    flush(&live).await.unwrap();
    assert_eq!(
        live.store.writer_commit_count_for_test() - before,
        applied_bodies as u64 + 1
    );
    flush(&live).await.unwrap();
    assert_eq!(
        live.store.writer_commit_count_for_test() - before,
        applied_bodies as u64 + 1
    );
}

#[tokio::test]
async fn two_events_share_one_minute_flush_and_a_lone_event_needs_no_successor() {
    assert_eq!(SNAPSHOT_FLUSH_INTERVAL, Duration::from_secs(60));
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    publish(&bus, &mut source, &sample).await;
    publish(&bus, &mut source, &body("epoch", 2, 20)).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    for _ in 0..2 {
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
            .await
            .unwrap();
    }
    assert!(live.store.quota_snapshot().unwrap().is_none());
    let before = live.store.writer_commit_count_for_test();
    // Exercise the production timer with a shortened minute, not an event-driven flush.
    let timer = tokio::spawn(flush_snapshot_loop(
        Arc::clone(&live.cache),
        Arc::clone(&live.store),
        Duration::from_millis(20),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while live.store.quota_snapshot().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshot_used(&live), 20.0);
    assert_eq!(live.store.writer_commit_count_for_test() - before, 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        live.store.writer_commit_count_for_test() - before,
        1,
        "unchanged ticks must not commit"
    );

    publish(&bus, &mut source, &body("epoch", 3, 30)).await;
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while snapshot_used(&live) != 30.0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    timer.abort();
    let _ = timer.await;
    assert_eq!(cursor.acks, 3);
    assert_eq!(
        live.store.writer_commit_count_for_test() - before,
        3,
        "one new body and one timed flush"
    );
}

#[tokio::test]
async fn failed_snapshot_flush_keeps_acked_readings_and_retries() {
    let (scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch", 1, 70);
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
    let db = rusqlite::Connection::open(scratch.path().join("store.db")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_snapshot BEFORE INSERT ON quota_snapshot BEGIN SELECT RAISE(FAIL, 'flush refused'); END;").unwrap();
    assert!(flush(&live).await.is_err());
    assert_eq!(cursor.acks, 1);
    assert_eq!(used(&live), 70.0);
    assert!(live.store.quota_snapshot().unwrap().is_none());
    db.execute_batch("DROP TRIGGER reject_snapshot;").unwrap();
    flush(&live).await.unwrap();
    assert_eq!(snapshot_used(&live), 70.0);
}

#[tokio::test]
async fn a_snapshot_waiting_for_the_writer_does_not_block_quota_cache_reads() {
    static BUSY_CALLS: AtomicUsize = AtomicUsize::new(0);
    fn wait_for_writer(_: i32) -> bool {
        BUSY_CALLS.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1));
        true
    }
    let (scratch, live) = fixture();
    live.store_body(snapshot_body(&body("epoch", 1, 70)), false)
        .await
        .unwrap();
    live.store
        .set_busy_handler_for_test(Some(wait_for_writer))
        .unwrap();
    let writer = rusqlite::Connection::open(scratch.path().join("store.db")).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let flush_live = live.clone();
    let flushing = tokio::spawn(async move { flush(&flush_live).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while BUSY_CALLS.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let read_cache = Arc::clone(&live.cache);
    let mut reading =
        tokio::task::spawn_blocking(move || read_cache.lock().unwrap().windows.clone());
    let read_without_writer_release =
        tokio::time::timeout(Duration::from_millis(250), &mut reading).await;
    // Always release the database writer before asserting, even if the reader
    // was blocked, so a failed regression cannot leave a blocking job behind.
    writer.execute_batch("ROLLBACK;").unwrap();
    flushing.await.unwrap().unwrap();
    live.store.set_busy_handler_for_test(None).unwrap();
    let read_without_writer_release = read_without_writer_release.map(|result| result.unwrap());
    if read_without_writer_release.is_err() {
        reading.await.unwrap();
    }
    assert!(
        read_without_writer_release.is_ok(),
        "a quota-cache read waited for the snapshot's SQLite writer acquisition"
    );
    assert_eq!(read_without_writer_release.unwrap().len(), 1);
    assert_eq!(snapshot_used(&live), 70.0);
}

#[tokio::test]
async fn a_late_timer_snapshot_cannot_overwrite_the_final_flush() {
    let (_scratch, live) = fixture();
    // Use identical poll times so timestamp ordering cannot mask a generation bug.
    let _clock = crate::InjectedClock::set(1_000_000);
    live.store_body(snapshot_body(&body("epoch", 1, 10)), false)
        .await
        .unwrap();
    let delayed_timer = snapshot_for_flush(&live.cache).unwrap().unwrap();
    let timer_generation = delayed_timer.generation;
    live.store_body(snapshot_body(&body("epoch", 2, 70)), false)
        .await
        .unwrap();
    assert!(
        live.cache
            .lock()
            .unwrap()
            .snapshot_persistence
            .publication_generation
            > timer_generation
    );
    finish_clean_exit(
        crate::background::BackgroundLoops::default(),
        Arc::clone(&live.cache),
        Arc::clone(&live.store),
    )
    .await
    .unwrap();
    assert_eq!(snapshot_used(&live), 70.0);
    let committed = live.store.writer_commit_count_for_test();
    // Resume the timer's captured work only after the final flush completed.
    write_pending_snapshot(delayed_timer, Arc::clone(&live.store))
        .await
        .unwrap();
    assert_eq!(snapshot_used(&live), 70.0);
    assert_eq!(live.store.writer_commit_count_for_test(), committed);
}

#[tokio::test]
async fn crash_after_ack_restores_current_accounts_and_old_cache_until_full_poll() {
    let (scratch, live) = fixture();
    live.store(&snapshot_body(&body("old", 1, 10)), false)
        .unwrap();
    flush(&live).await.unwrap();
    let bus = bus();
    let sample = body("epoch", 1, 70);
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
    assert_eq!(used(&live), 70.0);
    assert_eq!(snapshot_used(&live), 10.0);
    drop(cursor);
    drop(live); // Crash: deliberately skip the clean-exit flush.

    let store = Arc::new(
        SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap(),
    );
    let cache = crate::restore_quota_cache(&store);
    assert!(!cache.available);
    assert!(cache.restored_from_ms.is_some());
    assert_eq!(
        cache.windows[0]
            .usage
            .as_ref()
            .unwrap()
            .primary
            .as_ref()
            .unwrap()
            .used_percent,
        10.0
    );
    let accounts = account_quota::restore_account_state(&store);
    assert_eq!(accounts.restored_readings, 1);
    let live = LiveQuota {
        store,
        cache: Arc::new(Mutex::new(cache)),
        accounts: Arc::new(Mutex::new(accounts)),
        snapshot_baseline: Default::default(),
    };
    assert_eq!(used(&live), 70.0);
    let reboot_bus = self::bus();
    let mut source = Source {
        snapshot_error: Some("offline".into()),
        snapshot: snapshot_body(&body("epoch", 2, 90)),
        ..Default::default()
    };
    publish(&reboot_bus, &mut source, &body("epoch", 2, 80)).await;
    let mut cursor = Cursor::new(&reboot_bus, &live).await;
    let mut state = SequenceState::default();
    assert!(matches!(
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT).await,
        Err(ConsumeError::Rejected(_))
    ));
    assert_eq!((cursor.acks, cursor.naks), (0, 1));
    assert!(state.epoch.is_none());
    assert_eq!(used(&live), 70.0, "no event body may precede the full poll");
    assert!(live.cache.lock().unwrap().restored_from_ms.is_some());
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(source.snapshots, 2);
    assert_eq!(cursor.acks, 1);
    assert_eq!(used(&live), 90.0, "the full reply covers the older event");
    let cache = live.cache.lock().unwrap();
    assert!(cache.available);
    assert!(cache.restored_from_ms.is_none());
    assert_eq!(
        cache.windows[0]
            .usage
            .as_ref()
            .unwrap()
            .primary
            .as_ref()
            .unwrap()
            .used_percent,
        90.0
    );
}

#[tokio::test]
async fn full_replies_persist_completeness_and_tombstones_in_the_delta() {
    let (_scratch, live) = fixture();
    let work: Value = serde_json::from_str(&body("epoch", 1, 10)).unwrap();
    let mut sibling = work.clone();
    sibling["account"] = json!("sibling");
    live.store_body(
        serde_json::to_vec(
            &json!({"result":[work.clone(), sibling], "completeProviders":["codex"]}),
        )
        .unwrap(),
        false,
    )
    .await
    .unwrap();
    live.store_body(br#"{"result":[]}"#.to_vec(), false)
        .await
        .unwrap();
    assert!(live
        .store
        .account_quota_readings()
        .unwrap()
        .iter()
        .all(|row| row.tombstoned_at.is_none()));
    live.store_body(
        serde_json::to_vec(&json!({"result":[work], "completeProviders":["codex"]})).unwrap(),
        false,
    )
    .await
    .unwrap();
    let restored = account_quota::restore_account_state(&live.store);
    assert!(
        restored.readings.readings[&("codex".into(), "sibling".into())]
            .tombstoned_at_ms
            .is_some()
    );
    assert!(restored.readings.readings[&("codex".into(), "work".into())]
        .tombstoned_at_ms
        .is_none());
    assert!(restored.readings.completeness["codex"].last_poll_complete);
    assert!(live.store.quota_snapshot().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_exit_drains_a_mid_poll_publication_before_the_final_flush() {
    use std::{future::Future, task::Poll};
    let (_scratch, live) = fixture();
    let mut loops = crate::background::BackgroundLoops::default();
    let (mid_poll_tx, mid_poll_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let task_live = live.clone();
    let task_acks = Arc::clone(&acknowledgements);
    loops.spawn("quota events", async move {
        let worker_live = task_live.clone();
        let committed = tokio::task::spawn_blocking(move || {
            worker_live.commit_body(&snapshot_body(&body("epoch", 1, 70)), false)
        })
        .await
        .unwrap()
        .unwrap();
        mid_poll_tx.send(()).unwrap();
        // Hold this task inside one synchronous poll while shutdown aborts it.
        // Its already-committed publication and ACK finish before cancellation.
        release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        task_live.publish(committed).unwrap();
        task_acks.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
    });
    mid_poll_rx.await.unwrap();
    let finish = finish_clean_exit(loops, Arc::clone(&live.cache), Arc::clone(&live.store));
    tokio::pin!(finish);
    std::future::poll_fn(|cx| {
        assert!(finish.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release_tx.send(()).unwrap();
    finish.await.unwrap();
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot_used(&live), 70.0);
    assert_eq!(
        live.cache.lock().unwrap().windows[0]
            .usage
            .as_ref()
            .unwrap()
            .primary
            .as_ref()
            .unwrap()
            .used_percent,
        70.0
    );
    let before = live.store.writer_commit_count_for_test();
    tokio::task::yield_now().await;
    flush(&live).await.unwrap();
    assert_eq!(
        live.store.writer_commit_count_for_test(),
        before,
        "no publication follows the drained task's final flush"
    );
}

#[tokio::test]
async fn clean_eof_and_goodbye_flush_the_last_published_cache() {
    use subc_protocol::{Frame, FrameType};
    for goodbye in [false, true] {
        let (_scratch, live) = fixture();
        live.store_body(snapshot_body(&body("epoch", 1, 42)), false)
            .await
            .unwrap();
        let mut loops = crate::background::BackgroundLoops::default();
        loops.spawn("quota events", std::future::pending());
        let (mut peer, mut reader) = tokio::io::duplex(1024);
        if goodbye {
            subc_transport::write_frame(
                &mut peer,
                &Frame::build(
                    FrameType::Goodbye,
                    crate::control_flags(),
                    0,
                    0,
                    0,
                    Vec::new(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        }
        drop(peer);
        let frame = crate::background::next_frame_or_loop_stop(&mut reader, &mut loops)
            .await
            .unwrap();
        if let Some(frame) = frame {
            let (writer, _replies) = tokio::sync::mpsc::channel(1);
            assert!(!crate::handle_frame(
                frame,
                &writer,
                &live.cache,
                &live.store,
                &mut crate::RouteEpochs::new(),
                &Arc::new(Mutex::new(None)),
                std::path::Path::new("unused-routing"),
                std::path::Path::new("unused-operator"),
                &Arc::new(Mutex::new(crate::BrocaCredentials::default())),
                &Arc::new(Mutex::new(crate::CatalogReachHealth::new(60_000)))
            )
            .await
            .unwrap());
            assert!(goodbye);
        } else {
            assert!(!goodbye);
        }
        assert!(live.store.quota_snapshot().unwrap().is_none());
        finish_clean_exit(loops, Arc::clone(&live.cache), Arc::clone(&live.store))
            .await
            .unwrap();
        assert_eq!(snapshot_used(&live), 42.0);
    }
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
    assert_eq!(cursor.terms, 1);
    assert_eq!(cursor.naks, 0);
    assert!(cursor.inner.next().await.unwrap().is_none());
}

#[tokio::test]
async fn sdk_notice_tampered_digest_termed_without_applying() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    let key = publish(&bus, &mut source, &sample).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    let original = cursor.inner.next().await.unwrap().unwrap();
    cursor.inner.nak(Duration::ZERO).await.unwrap();
    let mut headers = original.message.headers;
    let tampered = ContentDigest::of_bytes(b"tampered").to_string();
    headers.insert("Ck-Content-Digest".into(), tampered.clone());
    let payload = quota_bus::message(
        SUBJECT.into(),
        key,
        b"",
        headers,
        Some(&tampered),
        Some(crate::now_ms()),
        1,
    )
    .unwrap();
    cursor.adapter_message = Some(payload.message);
    assert!(matches!(
        consume_one(
            &mut cursor,
            &mut source,
            &live,
            &mut SequenceState::default(),
            SUBJECT
        )
        .await,
        Err(ConsumeError::Rejected(_))
    ));
    assert_eq!((cursor.acks, cursor.naks, cursor.terms), (0, 0, 1));
    assert!(live.cache.lock().unwrap().windows.is_empty());
    assert!(live.store.account_quota_readings().unwrap().is_empty());
    assert!(cursor.inner.next().await.unwrap().is_none());
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
    db.execute_batch("CREATE TRIGGER reject_quota_write BEFORE INSERT ON account_quota_reading BEGIN SELECT RAISE(FAIL, 'write refused'); END;").unwrap();
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    assert!(matches!(
        consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT).await,
        Err(ConsumeError::Rejected(_))
    ));
    assert_eq!(cursor.acks, 0);
    assert!(state.sequences.is_empty());
    assert_eq!(cursor.naks, 1);
    assert!(live.cache.lock().unwrap().windows.is_empty());
    assert!(live.accounts.lock().unwrap().readings.readings.is_empty());
    assert!(
        live.store
            .get_quota_readings("openai/work")
            .unwrap()
            .is_empty(),
        "the earlier log append must roll back with the failed account write"
    );
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

async fn serve_nats_connection(
    listener: &tokio::net::TcpListener,
    stop: impl std::future::Future<Output = ()>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let (stream, _) = listener.accept().await.unwrap();
    let (read, mut write) = stream.into_split();
    write
        .write_all(b"INFO {\"server_id\":\"quota-test\",\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n")
        .await
        .unwrap();
    let mut read = BufReader::new(read);
    let mut line = String::new();
    tokio::pin!(stop);
    loop {
        line.clear();
        tokio::select! {
            _ = &mut stop => return,
            read = read.read_line(&mut line) => {
                if read.unwrap() == 0 {
                    return;
                }
                if line == "PING\r\n" {
                    write.write_all(b"PONG\r\n").await.unwrap();
                }
            }
        }
    }
}

async fn fact_stream_advertised(live: &LiveQuota) -> bool {
    let handler = catalog_handler(live);
    let deps = crate::routing_v1::Deps {
        quota_cache: &live.cache,
        store: &live.store,
        routing_config_path: &handler.config_path,
        operator_config_path: &handler.config_path,
        broca_credentials: &handler.credentials,
    };
    crate::routing_v1::answer(
        "role.describe",
        &json!({}),
        &crate::routing_v1::Caller::for_event("broca"),
        &deps,
    )
    .await
    .unwrap()["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("route_fact_stream"))
}

#[tokio::test]
async fn silent_disconnect_switches_snapshot_schedule_and_source_until_reconnected() {
    use async_nats::{connection::State, Event};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (disconnect, stop) = tokio::sync::oneshot::channel();
    let (resume, restart) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve_nats_connection(&listener, async { stop.await.unwrap() }).await;
        restart.await.unwrap();
        serve_nats_connection(&listener, std::future::pending()).await;
    });
    let (events, mut changes) = tokio::sync::mpsc::unbounded_channel();
    let connection = async_nats::ConnectOptions::new()
        .event_callback(move |event| {
            let events = events.clone();
            async move {
                let _ = events.send(event);
            }
        })
        .connect(address.to_string())
        .await
        .unwrap();
    while !matches!(changes.recv().await.unwrap(), Event::Connected) {}
    let (_scratch, live) = fixture();
    {
        let mut cache = live.cache.lock().unwrap();
        cache.consumer.attached = true;
        cache.consumer.route_facts.filters_ready = true;
        cache.consumer.route_facts.connection = Some(connection.clone());
    }
    assert!(fact_stream_advertised(&live).await);
    let start = Instant::now();
    let mut schedule = SnapshotSchedule::new(start);
    let mut reconnect = quota_bus::reconnect::Backoff::new(start, 31);
    let mut source = Source {
        snapshot: snapshot_body(&body("epoch-z", 1, 10)),
        ..Default::default()
    };
    crate::take_operator_infos();
    schedule.observe_binding(&live.cache, &mut reconnect, start);
    poll_if_due(&mut source, &live, &mut schedule, start).await;
    assert_eq!(schedule.next, start + Duration::from_secs(600));
    assert_eq!(
        crate::take_operator_infos(),
        ["[striatum] quota source=insula events (10m snapshot backstop)"]
    );

    disconnect.send(()).unwrap();
    while !matches!(changes.recv().await.unwrap(), Event::Disconnected) {}
    assert_eq!(connection.connection_state(), State::Disconnected);
    assert!(!fact_stream_advertised(&live).await);
    let down = start + Duration::from_secs(1);
    schedule.observe_binding(&live.cache, &mut reconnect, down);
    assert!(
        schedule.due(down),
        "a silent outage must not retain the ten-minute deadline"
    );
    assert_eq!(
        crate::take_operator_infos(),
        ["[striatum] quota source=usage.get fallback (60s)"]
    );
    {
        let cache = live.cache.lock().unwrap();
        assert!(
            cache.consumer.attached,
            "the SDK reader has not returned an error"
        );
        assert_eq!(cache.consumer.last_error, None);
        assert_eq!(cache.consumer.route_facts.bound_since, None);
    }
    poll_if_due(&mut source, &live, &mut schedule, down).await;
    assert_eq!(source.snapshots, 2);
    assert_eq!(schedule.next, down + Duration::from_secs(60));
    schedule.observe_binding(&live.cache, &mut reconnect, down + Duration::from_secs(59));
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        down + Duration::from_secs(59),
    )
    .await;
    assert_eq!(source.snapshots, 2);
    assert!(
        crate::take_operator_infos().is_empty(),
        "unchanged source stays quiet"
    );
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        down + Duration::from_secs(60),
    )
    .await;
    assert_eq!(source.snapshots, 3, "fallback continues once per minute");

    resume.send(()).unwrap();
    while !matches!(changes.recv().await.unwrap(), Event::Connected) {}
    assert_eq!(connection.connection_state(), State::Connected);
    assert!(fact_stream_advertised(&live).await);
    let rebound = down + Duration::from_secs(61);
    schedule.observe_binding(&live.cache, &mut reconnect, rebound);
    poll_if_due(&mut source, &live, &mut schedule, rebound).await;
    assert_eq!(schedule.next, rebound + Duration::from_secs(600));
    assert_eq!(
        crate::take_operator_infos(),
        ["[striatum] quota source=insula events (10m snapshot backstop)"]
    );
    poll_if_due(
        &mut source,
        &live,
        &mut schedule,
        rebound + Duration::from_secs(60),
    )
    .await;
    assert_eq!(source.snapshots, 4, "reconnecting stops minute polling");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[test]
fn revision_change_keeps_fallback_until_reader_rebinds() {
    let (_scratch, live) = fixture();
    let (revision, changes) = tokio::sync::watch::channel(0);
    {
        let mut cache = live.cache.lock().unwrap();
        cache.consumer.attached = true;
        cache.consumer.route_facts.revisions = Some(changes);
    }
    let start = Instant::now();
    let mut schedule = SnapshotSchedule::new(start);
    let mut reconnect = quota_bus::reconnect::Backoff::new(start, 31);
    crate::take_operator_infos();
    schedule.observe_binding(&live.cache, &mut reconnect, start);
    schedule.completed(start, true);
    assert_eq!(schedule.next, start + Duration::from_secs(600));
    crate::take_operator_infos();

    revision.send_replace(1);
    let down = start + Duration::from_secs(1);
    schedule.observe_binding(&live.cache, &mut reconnect, down);
    assert!(schedule.due(down));
    schedule.completed(down, true);
    assert_eq!(schedule.next, down + Duration::from_secs(60));
    assert_eq!(
        crate::take_operator_infos(),
        ["[striatum] quota source=usage.get fallback (60s)"]
    );
    schedule.observe_binding(&live.cache, &mut reconnect, down + Duration::from_secs(30));
    assert_eq!(schedule.next, down + Duration::from_secs(60));
    assert!(crate::take_operator_infos().is_empty());

    live.cache
        .lock()
        .unwrap()
        .consumer
        .route_facts
        .revisions
        .as_mut()
        .unwrap()
        .borrow_and_update();
    let rebound = down + Duration::from_secs(31);
    schedule.observe_binding(&live.cache, &mut reconnect, rebound);
    schedule.completed(rebound, true);
    assert_eq!(schedule.next, rebound + Duration::from_secs(600));
    assert_eq!(
        live.cache.lock().unwrap().consumer.route_facts.bound_since,
        Some(rebound)
    );
    assert_eq!(
        crate::take_operator_infos(),
        ["[striatum] quota source=insula events (10m snapshot backstop)"]
    );
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

#[tokio::test]
async fn an_unread_subject_is_acknowledged_and_never_blocks_quota_samples() {
    let (_scratch, live) = fixture();
    let bus = bus();
    let sample = body("epoch-z", 1, 10);
    let mut source = Source {
        snapshot: snapshot_body(&sample),
        ..Default::default()
    };
    // The test cursor checks every ack follows a stored reading, so start from one.
    live.store(&snapshot_body(&sample), false).unwrap();
    let step = "ck.box_test.event.broca.step_finished.v1";
    let _ = bus
        .publish(
            step,
            "step-1",
            ContentDigest::of_bytes(b"{}"),
            Headers::default(),
        )
        .await
        .unwrap();
    publish(&bus, &mut source, &sample).await;
    let mut cursor = Cursor::new(&bus, &live).await;
    let mut state = SequenceState::default();
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(
        cursor.acks, 1,
        "the step event is acknowledged, not redelivered"
    );
    assert!(state.skipped_subjects.contains(step));
    consume_one(&mut cursor, &mut source, &live, &mut state, SUBJECT)
        .await
        .unwrap();
    assert_eq!(cursor.deliveries, [1, 1], "each event is delivered once");
    assert_eq!(cursor.acks, 2);
    assert_eq!(used(&live), 10.0);
}
