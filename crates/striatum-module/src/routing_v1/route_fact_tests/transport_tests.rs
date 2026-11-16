use super::*;

#[tokio::test]
async fn fact_catalog_new_versions_coalesce_and_old_versions_do_not_refresh() {
    let (_harness, _runner, handler) = fixture().await;
    handler
        .store
        .apply_fusiform_catalog(
            &[],
            striatum_store::FusiformCatalogCursor {
                catalog_version: 5,
                resolved_at_ms: NOW as i64,
            },
        )
        .unwrap();
    let wake = handler.credentials.lock().unwrap().catalog_wake.clone();
    // Clear any wake issued while the fixture was being initialized.
    let _ = tokio::time::timeout(Duration::ZERO, wake.notified()).await;
    let (bus, mut cursor) = bus(&handler).await;
    cursor.expect_finished = false;
    for version in [4, 5] {
        let body = serde_json::to_vec(&json!({"catalog_version":version})).unwrap();
        let _ = bus
            .publish(
                "ck.box_test.event.fusiform.catalog_changed.v1",
                &version.to_string(),
                ContentDigest::of_bytes(&body),
                Headers::new(),
            )
            .await
            .unwrap();
        consume(&handler, &mut cursor, &body).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(10), wake.notified())
            .await
            .is_err()
    );
    for version in [6, 7, 8] {
        let body = serde_json::to_vec(&json!({"catalog_version":version})).unwrap();
        let _ = bus
            .publish(
                "ck.box_test.event.fusiform.catalog_changed.v1",
                &version.to_string(),
                ContentDigest::of_bytes(&body),
                Headers::new(),
            )
            .await
            .unwrap();
        consume(&handler, &mut cursor, &body).await;
    }
    tokio::time::timeout(Duration::from_millis(50), wake.notified())
        .await
        .expect("new catalog event must trigger refresh");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), wake.notified())
            .await
            .is_err(),
        "burst should leave only one refresh wake"
    );
    assert_eq!(cursor.acks, 5);
}

async fn advertised(runner: &Route) -> bool {
    result(runner.call("role.describe", &json!({})).await)["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("route_fact_stream"))
}

#[tokio::test]
async fn fact_capability_withdrawn_during_rebind_until_filters_reconfirmed() {
    let (_harness, runner, handler) = fixture().await;
    let names = AccountNames::derive("box_test").unwrap();
    let filters = route_facts::fact_subjects(&names);
    assert!(!advertised(&runner).await);
    let (revision, changes) = tokio::sync::watch::channel(0);
    {
        let mut cache = handler.quota_cache.lock().unwrap();
        cache.consumer.attached = true;
        cache.consumer.route_facts.filters_ready =
            route_facts::filters_ready(&names, Some("m_striatum"), &filters);
        cache.consumer.route_facts.revisions = Some(changes);
    }
    assert!(advertised(&runner).await);
    revision.send_replace(1);
    assert!(
        !advertised(&runner).await,
        "a new connection has not confirmed the durable yet"
    );
    {
        let mut cache = handler.quota_cache.lock().unwrap();
        cache
            .consumer
            .route_facts
            .revisions
            .as_mut()
            .unwrap()
            .borrow_and_update();
        cache.consumer.route_facts.filters_ready =
            route_facts::filters_ready(&names, Some("m_striatum"), &filters[..1]);
    }
    assert!(!advertised(&runner).await);
    handler
        .quota_cache
        .lock()
        .unwrap()
        .consumer
        .route_facts
        .filters_ready = route_facts::filters_ready(&names, Some("m_striatum"), &filters);
    assert!(advertised(&runner).await);
    handler.quota_cache.lock().unwrap().consumer.attached = false;
    assert!(!advertised(&runner).await);
    drop(revision);
    handler.quota_cache.lock().unwrap().consumer.attached = true;
    assert!(
        !advertised(&runner).await,
        "closed maintenance channel is not a bound reader"
    );
}

#[tokio::test]
async fn fact_invalid_unknown_publisher_and_oversized_bodies_terminated() {
    let (_harness, _runner, handler) = fixture().await;
    for (module, body) in [
        ("broca", b"invalid".to_vec()),
        ("unknown", serde_json::to_vec(&finish(false)).unwrap()),
        ("broca", vec![b' '; 4097]),
    ] {
        let (bus, mut cursor) = bus(&handler).await;
        cursor.expect_finished = false;
        let _ = bus
            .publish(
                &format!("ck.box_test.event.{module}.route_fact.v1"),
                "invalid",
                ContentDigest::of_bytes(&body),
                Headers::new(),
            )
            .await
            .unwrap();
        consume(&handler, &mut cursor, &body).await;
        assert_eq!((cursor.acks, cursor.terms), (0, 1));
    }
    assert_publisher(&handler, "broca", [0, 0, 2, 0], None);
    assert_publisher(&handler, "unknown", [0, 0, 1, 0], None);
}

#[test]
fn fact_backoff_uses_delivery_count_and_caps_at_thirty_seconds() {
    for (count, seconds) in [(1, 1), (2, 2), (3, 4), (5, 16), (6, 30), (u64::MAX, 30)] {
        assert_eq!(
            route_facts::retry_delay(count),
            Duration::from_secs(seconds)
        );
    }
}
