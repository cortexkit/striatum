use super::*;

const DIGEST_HEADER: &str = "Ck-Content-Digest";
const BROCA_SUBJECT: &str = "ck.box_test.event.broca.route_fact.v1";

async fn consume_notice(
    handler: &Handler,
    bus: &InMemoryBus,
    cursor: &mut Cursor,
    subject: &str,
    id: &str,
    headers: Headers,
    body: &[u8],
) {
    let receipt = bus
        .publish(subject, id, ContentDigest::of_bytes(body), headers)
        .await
        .unwrap();
    assert!(!receipt.duplicate());
    let delivery = cursor.next().await.unwrap().unwrap();
    let message = delivery.message;
    let digest = message.headers.get(DIGEST_HEADER).cloned();
    let payload = crate::quota_bus::message(
        message.subject,
        message.id,
        body,
        message.headers,
        digest.as_deref(),
        Some(NOW - 10),
        delivery.delivery_count,
    )
    .unwrap();
    assert!(handler
        .consume(cursor, &AccountNames::derive("box_test").unwrap(), &payload)
        .await
        .unwrap());
}

async fn assert_digest_refused(headers: Headers, body: &[u8]) {
    let (harness, _runner, handler) = fixture().await;
    let (bus, mut cursor) = bus(&handler).await;
    cursor.expect_finished = false;
    consume_notice(
        &handler,
        &bus,
        &mut cursor,
        BROCA_SUBJECT,
        "finish",
        headers,
        body,
    )
    .await;
    assert_eq!(
        (cursor.acks, cursor.terms),
        (0, 1),
        "expected TERM, not ACK"
    );
    assert!(cursor.delays.is_empty());
    assert!(cursor.next().await.unwrap().is_none());
    assert_publisher(&handler, "broca", [0, 0, 1, 0], None);
    assert_eq!(
        handler
            .store
            .account_read(|tx| Ok(tx.admission("reserved:broca", "admission")?.unwrap().state))
            .unwrap(),
        "reserved"
    );
    let count: i64 = rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .query_row("SELECT count(*) FROM account_observation", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn fact_changed_body_with_original_digest_terminates_without_writing() {
    let original = serde_json::to_vec(&finish(false)).unwrap();
    let mut changed = finish(false);
    changed["params"]["run_id"] = json!("changed-run");
    assert_digest_refused(
        Headers::from([(
            DIGEST_HEADER.into(),
            ContentDigest::of_bytes(&original).to_string(),
        )]),
        &serde_json::to_vec(&changed).unwrap(),
    )
    .await;
}

#[tokio::test]
async fn fact_missing_digest_terminates_without_writing() {
    assert_digest_refused(Headers::new(), &serde_json::to_vec(&finish(false)).unwrap()).await;
}

#[derive(serde::Deserialize)]
struct BrocaPublication {
    subject: String,
    nats_msg_id: String,
    headers: Headers,
    body: String,
}

fn observation(handler: &Handler, module: &str, id: &str) -> striatum_store::AccountObservationRow {
    handler
        .store
        .account_read(|tx| tx.observation_by_event(&format!("reserved:{module}"), module, id))
        .unwrap()
        .expect("observation was not stored")
}

#[tokio::test]
async fn original_broca_publications_ack_and_match_direct_call_rows() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_harness, _runner, handler) = fixture_for_admission("broca", "adm1").await;
    let (_direct_harness, direct, direct_handler) = fixture_for_admission("broca", "adm1").await;
    let (bus, mut cursor) = bus(&handler).await;
    cursor.expect_finished = false;
    for sample in [
        include_str!("../../../fixtures/broca-route-fact-1119/admission-started.json"),
        include_str!("../../../fixtures/broca-route-fact-1119/run-finished.json"),
    ] {
        let publication: BrocaPublication = serde_json::from_str(sample).unwrap();
        let request: Value = serde_json::from_str(&publication.body).unwrap();
        result(
            direct
                .call("route.account_observe", &request["params"])
                .await,
        );
        consume_notice(
            &handler,
            &bus,
            &mut cursor,
            &publication.subject,
            &publication.nats_msg_id,
            publication.headers,
            publication.body.as_bytes(),
        )
        .await;
        let mut actual = observation(&handler, "broca", &publication.nats_msg_id);
        let expected = observation(&direct_handler, "broca", &publication.nats_msg_id);
        // The two independent stores assign different observation IDs.
        actual.observation_id.clone_from(&expected.observation_id);
        assert_eq!(actual, expected);
    }
    assert_eq!((cursor.acks, cursor.terms), (2, 0));
    assert_publisher(&handler, "broca", [2, 0, 0, 0], Some(NOW));
}

#[tokio::test]
async fn original_thalamus_bodies_ack_and_store_attempts() {
    let _clock = crate::InjectedClock::set(NOW);
    let module = "thalamus-harness";
    for (body, digest) in [
        (
            include_bytes!("../../../fixtures/thalamus-bus-route-fact-v1/typed-success.json")
                .as_slice(),
            "bdb59cf0af248f2f669554ae1741e06bfd8f9b25db5c6d4ee8830161cc46b63e",
        ),
        (
            include_bytes!("../../../fixtures/thalamus-bus-route-fact-v2/mixed-headers.json")
                .as_slice(),
            "389e3b7dde17afc7995b98729ca2ac01d2a833b21f245faca63cd76bea0c4009",
        ),
        (
            include_bytes!("../../../fixtures/thalamus-bus-route-fact-v2/all-headers-dropped.json")
                .as_slice(),
            "880027a45706c78c2c9fafdab55c19c90e9672b93b304f0d5f96aa9a8a995ecb",
        ),
    ] {
        let request: Value = serde_json::from_slice(body).unwrap();
        let params = &request["params"];
        let admission = params["admission_id"].as_str().unwrap();
        let id = params["event_id"].as_str().unwrap();
        let (_harness, _runner, handler) = fixture_for_admission(module, admission).await;
        let (bus, mut cursor) = bus(&handler).await;
        cursor.expect_finished = false;
        consume_notice(
            &handler,
            &bus,
            &mut cursor,
            "ck.box_test.event.thalamus-harness.route_fact.v1",
            id,
            Headers::from([(DIGEST_HEADER.into(), digest.into())]),
            body,
        )
        .await;
        assert_eq!((cursor.acks, cursor.terms), (1, 0), "{id}");
        assert_publisher(&handler, module, [1, 0, 0, 0], Some(NOW));
        let stored = observation(&handler, module, id);
        assert_eq!(stored.kind, "attempt");
        assert_eq!(stored.admission_id.as_deref(), Some(admission));
        let observation: Value = serde_json::from_str(&stored.observation_json).unwrap();
        assert_eq!(observation["attempt_outcome"], params["attempt_outcome"]);
    }
}
