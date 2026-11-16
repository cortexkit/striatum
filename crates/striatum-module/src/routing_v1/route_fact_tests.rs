use super::conformance_tests::{reserved, Harness, Route};
use crate::route_facts::{self, Delivery, Handler};
use cortexkit_bus_inmemory::{InMemoryBus, InMemoryConfig, InMemoryStreamCursor};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{
    BusResult, ContentDigest, DurableOwner, Headers, Stream, StreamCursor, StreamDelivery,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use subc_protocol::FrameType;

const NOW: u64 = 1_800_000_000_000;
const OWNER: DurableOwner<'_> = DurableOwner::Module("striatum");

mod order_tests;
#[path = "route_fact_tests/transport_cpu_tests.rs"]
mod transport_cpu;
mod transport_tests;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

async fn fixture() -> (Arc<Harness>, Route, Handler) {
    let (harness, _) = super::account_classes_tests::setup("synthetic", false);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca","via_vault":true}]));
    let runner = harness.bind(reserved("broca"), None).await;
    result(
        runner
            .call(
                "route.account_for",
                &json!({"provider":"synthetic", "model":"model",
        "conversation_key":"conversation", "admission_id":"admission"}),
            )
            .await,
    );
    let handler = harness.fact_handler();
    (harness, runner, handler)
}

fn finish(legacy: bool) -> Value {
    let mut params = json!({"event_id":"finish", "kind":"run_finished", "admission_id":"admission", "run_id":"run"});
    if legacy {
        params["caller"] = json!({"runner":"broca"});
    }
    json!({"op":"route.account_observe", "params":params})
}

// The in-memory message plane intentionally carries notices, not payloads.
// Keep payload bytes beside its cursor while exercising real delivery/settlement.
struct Cursor {
    inner: InMemoryStreamCursor,
    handler: Handler,
    acks: usize,
    terms: usize,
    retries: usize,
    delays: Vec<Duration>,
    expect_finished: bool,
    remove_failure: Option<std::path::PathBuf>,
}

#[async_trait::async_trait]
impl StreamCursor for Cursor {
    async fn next(&mut self) -> BusResult<Option<StreamDelivery>> {
        self.inner.next().await
    }
    async fn ack(&mut self) -> BusResult<()> {
        if self.expect_finished {
            self.handler
                .store
                .account_read(|tx| {
                    assert_eq!(
                        tx.admission("reserved:broca", "admission")?.unwrap().state,
                        "finished"
                    );
                    Ok(())
                })
                .unwrap();
            assert!(
                crate::account_state::snapshot(&self.handler.store)
                    .unwrap()
                    .admissions
                    .open_admission("reserved:broca", "admission")
                    .is_none(),
                "ack precedes terminal-state publication"
            );
        }
        self.inner.ack().await?;
        self.acks += 1;
        Ok(())
    }
    async fn nak(&mut self, delay: Duration) -> BusResult<()> {
        self.delays.push(delay);
        self.inner.nak(Duration::ZERO).await
    }
    async fn term(&mut self, reason: Option<&str>) -> BusResult<()> {
        self.inner.term(reason).await?;
        self.terms += 1;
        Ok(())
    }
    async fn in_progress(&mut self) -> BusResult<()> {
        assert_eq!(self.acks, 0, "transient failure must not ack");
        self.retries += 1;
        if let Some(path) = self.remove_failure.take() {
            rusqlite::Connection::open(path)
                .unwrap()
                .execute_batch("DROP TRIGGER fail_observation")
                .unwrap();
        }
        self.inner.in_progress().await
    }
}

async fn bus(handler: &Handler) -> (InMemoryBus, Cursor) {
    let bus = InMemoryBus::new(
        InMemoryConfig::default()
            .with_durable(OWNER)
            .with_deduplication_window(Duration::ZERO),
    );
    let inner = bus.consumer(OWNER).await.unwrap();
    (
        bus,
        Cursor {
            inner,
            handler: handler.clone(),
            acks: 0,
            terms: 0,
            retries: 0,
            delays: Vec::new(),
            expect_finished: true,
            remove_failure: None,
        },
    )
}

async fn publish(bus: &InMemoryBus, module: &str, id: &str, body: &Value) -> Vec<u8> {
    let bytes = serde_json::to_vec(body).unwrap();
    let receipt = bus
        .publish(
            &format!("ck.box_test.event.{module}.route_fact.v1"),
            id,
            ContentDigest::of_bytes(&bytes),
            Headers::new(),
        )
        .await
        .unwrap();
    assert!(!receipt.duplicate());
    bytes
}

async fn consume(handler: &Handler, cursor: &mut Cursor, bytes: &[u8]) {
    let message = cursor.next().await.unwrap().unwrap().message;
    assert!(handler
        .consume(
            cursor,
            &AccountNames::derive("box_test").unwrap(),
            &Delivery {
                message,
                body: bytes.to_vec(),
                published_at_ms: NOW - 10,
                delivery_count: 1,
            }
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn fact_apply_and_ack_after_commit_and_publication_in_both_shapes() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (_harness, _runner, handler) = fixture().await;
        let (bus, mut cursor) = bus(&handler).await;
        let bytes = publish(&bus, "broca", "finish", &finish(legacy)).await;
        consume(&handler, &mut cursor, &bytes).await;
        assert_eq!(cursor.acks, 1);
        assert_eq!(
            handler
                .quota_cache
                .lock()
                .unwrap()
                .consumer
                .route_facts
                .applied,
            1
        );
        assert!(cursor.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn fact_duplicate_redelivery_applies_once() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    let bytes = publish(&bus, "broca", "finish", &finish(false)).await;
    let first = cursor.next().await.unwrap().unwrap();
    handler.dispatch("broca", &bytes).await.unwrap();
    cursor.nak(Duration::ZERO).await.unwrap();
    let repeated = cursor.next().await.unwrap().unwrap();
    assert_eq!(repeated.stream_seq, first.stream_seq);
    assert_eq!(repeated.delivery_count, 2);
    handler
        .consume(
            &mut cursor,
            &AccountNames::derive("box_test").unwrap(),
            &Delivery {
                message: repeated.message,
                body: bytes,
                published_at_ms: NOW,
                delivery_count: repeated.delivery_count,
            },
        )
        .await
        .unwrap();
    assert_eq!(cursor.acks, 1);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM account_observation", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn fact_stream_plus_direct_call_applies_once() {
    let _clock = crate::InjectedClock::set(NOW);
    for stream_first in [true, false] {
        let (harness, runner, handler) = fixture().await;
        let body = finish(false);
        let (bus, mut cursor) = bus(&handler).await;
        let bytes = publish(&bus, "broca", "finish", &body).await;
        if stream_first {
            consume(&handler, &mut cursor, &bytes).await;
        }
        let answer = result(runner.call("route.account_observe", &body["params"]).await);
        assert_eq!(answer["observation"]["duplicate"], stream_first);
        if !stream_first {
            consume(&handler, &mut cursor, &bytes).await;
        }
        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM account_observation", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}

#[tokio::test]
async fn fact_permanent_refusal_terminated_and_counted() {
    let (_harness, runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    let mut body = finish(true);
    body["params"]["caller"]["runner"] = json!("plexus");
    let direct = runner.call("route.account_observe", &body["params"]).await;
    assert_eq!(direct.0, FrameType::Error);
    assert_eq!(direct.1["code"], "account_not_authorized");
    let bytes = publish(&bus, "broca", "forged", &body).await;
    cursor.expect_finished = false;
    consume(&handler, &mut cursor, &bytes).await;
    assert_eq!((cursor.acks, cursor.terms), (0, 1));
    assert_eq!(
        handler
            .quota_cache
            .lock()
            .unwrap()
            .consumer
            .route_facts
            .refused,
        1
    );
    assert!(cursor.next().await.unwrap().is_none());
}

#[tokio::test]
async fn fact_transient_store_failure_naks_without_applying() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    rusqlite::Connection::open(harness.store_path()).unwrap().execute_batch("CREATE TRIGGER fail_observation BEFORE INSERT ON account_observation BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    cursor.remove_failure = Some(harness.store_path());
    let bytes = publish(&bus, "broca", "finish", &finish(false)).await;
    publish(&bus, "broca", "later", &json!({"op":"route.account_observe", "params":{"event_id":"later", "kind":"attempt", "admission_id":"admission"}})).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        consume(&handler, &mut cursor, &bytes),
    )
    .await
    .unwrap();
    assert_eq!((cursor.acks, cursor.terms), (0, 0));
    assert_eq!(cursor.delays, vec![Duration::from_secs(1)]);
    assert_eq!(
        handler
            .store
            .account_read(|tx| Ok(tx.admission("reserved:broca", "admission")?.unwrap().state))
            .unwrap(),
        "reserved"
    );
    assert_eq!(
        handler
            .quota_cache
            .lock()
            .unwrap()
            .consumer
            .route_facts
            .retries,
        1
    );
}

#[tokio::test]
async fn fact_transient_redelivery_applies_exactly_once() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    rusqlite::Connection::open(harness.store_path()).unwrap().execute_batch("CREATE TRIGGER fail_observation BEFORE INSERT ON account_observation BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    cursor.remove_failure = Some(harness.store_path());
    let bytes = publish(&bus, "broca", "finish", &finish(false)).await;
    consume(&handler, &mut cursor, &bytes).await;
    assert_eq!(cursor.delays, vec![Duration::from_secs(1)]);
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute_batch("DROP TRIGGER IF EXISTS fail_observation")
        .unwrap();
    let repeated = cursor.next().await.unwrap().unwrap();
    assert_eq!(repeated.delivery_count, 2);
    handler
        .consume(
            &mut cursor,
            &AccountNames::derive("box_test").unwrap(),
            &Delivery {
                message: repeated.message,
                body: bytes.clone(),
                published_at_ms: NOW,
                delivery_count: repeated.delivery_count,
            },
        )
        .await
        .unwrap();
    handler.dispatch("broca", &bytes).await.unwrap();
    assert_eq!(cursor.acks, 1);
    let count: i64 = rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .query_row("SELECT count(*) FROM account_observation", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn fact_capability_absent_until_all_filters_exist() {
    let (harness, runner, handler) = fixture().await;
    let names = AccountNames::derive("box_test").unwrap();
    let filters = route_facts::fact_subjects(&names);
    handler.quota_cache.lock().unwrap().consumer.attached = true;
    assert_eq!(
        filters,
        vec![
            "ck.box_test.event.broca.route_fact.v1",
            "ck.box_test.event.thalamus-harness.route_fact.v1",
            "ck.box_test.event.colliculus.route_fact.v1",
            "ck.box_test.event.prefrontal-core.route_fact.v1",
            "ck.box_test.event.plexus.route_fact.v1",
        ]
    );
    assert!(!route_facts::filters_ready(&names, None, &filters));
    assert!(!route_facts::filters_ready(
        &names,
        Some("m_other"),
        &filters
    ));
    for count in 0..=filters.len() {
        let ready = route_facts::filters_ready(&names, Some("m_striatum"), &filters[..count]);
        handler
            .quota_cache
            .lock()
            .unwrap()
            .consumer
            .route_facts
            .filters_ready = ready;
        let describe = result(runner.call("role.describe", &json!({})).await);
        assert!(
            describe.get("route_fact_stream").is_none(),
            "the capability belongs in result.capabilities, not a top-level member"
        );
        assert_eq!(
            describe["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!("route_fact_stream")),
            count == filters.len()
        );
    }
    drop(harness);
}

#[tokio::test]
async fn fact_catalog_event_with_reader_token_wakes_existing_refresh_and_acks() {
    let (_harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    cursor.expect_finished = false;
    let wake = handler.credentials.lock().unwrap().catalog_wake.clone();
    let receipt = bus
        .publish(
            "ck.box_test.event.fusiform.catalog_changed.v1.striatum",
            "catalog",
            ContentDigest::of_bytes(b"{}"),
            Headers::new(),
        )
        .await
        .unwrap();
    assert!(!receipt.duplicate());
    consume(&handler, &mut cursor, br#"{"catalog_version":1}"#).await;
    tokio::time::timeout(Duration::from_millis(50), wake.notified())
        .await
        .unwrap();
    assert_eq!(cursor.acks, 1);
}

#[tokio::test]
async fn fact_subject_reader_token_preserves_publisher_identity() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    let body = serde_json::to_vec(&finish(false)).unwrap();
    let receipt = bus
        .publish(
            "ck.box_test.event.broca.route_fact.v1.striatum",
            "finish",
            ContentDigest::of_bytes(&body),
            Headers::new(),
        )
        .await
        .unwrap();
    assert!(!receipt.duplicate());
    consume(&handler, &mut cursor, &body).await;
    assert_eq!(cursor.acks, 1);
}

#[tokio::test]
async fn fact_report_stream_and_direct_delivery_apply_usage_once() {
    let _clock = crate::InjectedClock::set(NOW);
    for stream_first in [false, true] {
        let (harness, _runner, _) = fixture().await;
        harness
            .edit_config(|config| config["consumers"]["mason"]["owner"] = json!("prefrontal-core"));
        let owner = harness.bind(reserved("prefrontal-core"), None).await;
        let selected = result(
            owner
                .call(
                    "route.select",
                    &json!({"consumer":"mason", "pin":"synthetic/model", "requirements":{"iq":0}}),
                )
                .await,
        );
        let body = json!({"op":"route.report", "params":{"decision_id":selected["decision_id"], "outcome":"completed", "usage":{"run_id":"run", "output_tokens":20}}});
        let handler = harness.fact_handler();
        let (bus, mut cursor) = bus(&handler).await;
        cursor.expect_finished = false;
        let bytes = publish(&bus, "prefrontal-core", "report", &body).await;
        if stream_first {
            consume(&handler, &mut cursor, &bytes).await;
        }
        result(owner.call("route.report", &body["params"]).await);
        if !stream_first {
            consume(&handler, &mut cursor, &bytes).await;
        }
        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        let usage: (i64, i64) = conn
            .query_row(
                "SELECT count(*),sum(output_tokens) FROM model_route_usage",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(usage, (1, 20));
        assert_eq!(cursor.acks, 1);
    }
}
