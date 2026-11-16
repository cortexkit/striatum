use super::*;

#[tokio::test]
async fn fact_duplicate_keeps_last_apply_and_counts_publisher_not_reader_in_both_shapes() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (_harness, _runner, handler) = fixture().await;
        let (bus, mut cursor) = bus(&handler).await;
        let bytes = publish(&bus, "broca", "finish", &finish(legacy)).await;
        consume(&handler, &mut cursor, &bytes).await;
        assert_publisher(&handler, "broca", [1, 0, 0, 0], Some(NOW));
        let _later = crate::InjectedClock::set(NOW + 60_000);
        let _ = bus
            .publish(
                "ck.box_test.event.broca.route_fact.v1.striatum",
                "finish-copy",
                ContentDigest::of_bytes(&bytes),
                Headers::new(),
            )
            .await
            .unwrap();
        consume(&handler, &mut cursor, &bytes).await;
        assert_publisher(&handler, "broca", [1, 1, 0, 0], Some(NOW));
        let cache = handler.quota_cache.lock().unwrap();
        assert_eq!(cache.consumer.route_facts.publishers.len(), 1);
    }
}

#[tokio::test]
async fn fact_transport_refusals_count_only_identifiable_v1_fact_publishers() {
    let (_harness, _runner, handler) = fixture().await;
    let names = AccountNames::derive("box_test").unwrap();
    for subject in [
        "not-an-event",
        "ck.box_test.event.insula.quota_sampled.v1",
        "ck.box_test.event.broca.route_fact.v2",
    ] {
        handler.refused_subject(&names, subject);
    }
    assert!(handler
        .quota_cache
        .lock()
        .unwrap()
        .consumer
        .route_facts
        .publishers
        .is_empty());
    handler.refused_subject(
        &names,
        "ck.box_test.event.colliculus.route_fact.v1.striatum",
    );
    assert_publisher(&handler, "colliculus", [0, 0, 1, 0], None);
}
