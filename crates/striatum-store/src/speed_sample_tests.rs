use super::*;
use striatum_core::model_resolution::{
    build_speed_sample_observation, build_usage_observation, NativeUsageFacts,
};

fn test_store(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        label,
        scratch.path().join("routing.db").to_string_lossy(),
    ))
    .unwrap();
    (scratch, store)
}

fn facts(id: &str, output: f64, duration: f64, observed: f64) -> NativeUsageFacts {
    NativeUsageFacts {
        message_id: id.into(),
        provider_id: "test".into(),
        model_id: "model".into(),
        variant: Some("high".into()),
        output_tokens: output,
        created_ms: observed - duration,
        completed_ms: observed,
        ..Default::default()
    }
}

#[test]
fn speed_samples_partition_tiers_and_merge_with_legacy_heads() {
    let (_scratch, store) = test_store("speed-sample-tiers");
    let now = 100_000.0;
    let route = "test/model/high";
    for index in 0..3 {
        let head = facts(&format!("head-{index}"), 200.0, 2_000.0, now);
        store
            .record_usage_delivery(&build_usage_observation(&head, &[]).unwrap())
            .unwrap();
    }
    // Neither source alone reaches five samples: their standard population must merge.
    for (tier, speed, count, output) in [
        (Some("default"), None, 1, 200.0),
        (None, Some("standard"), 1, 200.0),
        (Some("standard"), Some("fast"), 5, 400.0),
        (Some("fast"), Some("standard"), 5, 600.0),
        (Some("fast"), Some("fast"), 5, 800.0),
        (None, Some("fast"), 5, 1_000.0),
        (Some("default"), Some("fast"), 5, 1_200.0),
    ] {
        for index in 0..count {
            let mut sample = facts(&format!("{tier:?}-{speed:?}-{index}"), output, 2_000.0, now);
            sample.speed_sample = true;
            sample.service_tier = tier.map(str::to_owned);
            sample.speed = speed.map(str::to_owned);
            let observation = build_speed_sample_observation(&sample, &[]).unwrap();
            assert_eq!(observation.variant.as_deref(), Some("high"));
            assert!(
                !store
                    .record_speed_sample_delivery(&observation)
                    .unwrap()
                    .usage_recorded
            );
        }
    }
    assert_eq!(
        store
            .get_observed_speed_for_tier(route, None, Some("fast"), now)
            .unwrap(),
        Some(500.0)
    );
    assert_eq!(
        store
            .get_observed_speed_for_tier(route, Some("default"), Some("fast"), now)
            .unwrap(),
        Some(600.0)
    );
    assert_eq!(store.get_observed_speed(route, now).unwrap(), Some(100.0));
    for (tier, speed, expected) in [
        ("standard", "standard", 100.0),
        ("standard", "fast", 200.0),
        ("fast", "standard", 300.0),
        ("fast", "fast", 400.0),
    ] {
        assert_eq!(
            store
                .get_observed_speed_for_tier(route, Some(tier), Some(speed), now)
                .unwrap(),
            Some(expected)
        );
    }
    let aggregates = store
        .get_observed_speed_aggregates(&BTreeSet::from([route.into()]), now)
        .unwrap();
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].sample_count, 5);
    assert_eq!(aggregates[0].speed, Some(100.0));
    store
        .store
        .with_conn(|c| {
            assert_eq!(
                c.query_row("SELECT COUNT(*) FROM model_route_usage", [], |r| r
                    .get::<_, i64>(0))?,
                3
            );
            assert_eq!(
                c.query_row("SELECT COUNT(*) FROM model_route_speed_samples", [], |r| {
                    r.get::<_, i64>(0)
                })?,
                27
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn speed_samples_apply_window_limit_and_idempotency_per_population() {
    let (_scratch, store) = test_store("speed-sample-window");
    let now = OBSERVED_SPEED_WINDOW_MS + 100_000.0;
    for index in 0..25 {
        let mut sample = facts(
            &format!("sample-{index}"),
            200.0,
            2_000.0,
            now - index as f64,
        );
        sample.speed_sample = true;
        let observation = build_speed_sample_observation(&sample, &[]).unwrap();
        assert!(
            store
                .record_speed_sample_delivery(&observation)
                .unwrap()
                .recorded
        );
        let mut duplicate = observation.clone();
        duplicate.output_tokens = 99_999.0;
        duplicate.service_tier = Some("fast".into());
        assert_eq!(
            store.record_speed_sample_delivery(&duplicate).unwrap(),
            UsageRecordResult::default()
        );
    }
    for (id, observed) in [
        ("too-old", now - OBSERVED_SPEED_WINDOW_MS),
        ("future", now + 1.0),
    ] {
        let mut sample = facts(id, 9_999.0, 1_000.0, observed);
        sample.speed_sample = true;
        store
            .record_speed_sample_delivery(&build_speed_sample_observation(&sample, &[]).unwrap())
            .unwrap();
    }
    let aggregate = store
        .get_observed_speed_aggregates(&BTreeSet::from(["test/model/high".into()]), now)
        .unwrap();
    assert_eq!(aggregate[0].sample_count, OBSERVED_SPEED_SAMPLE_LIMIT);
    assert_eq!(aggregate[0].output_tokens, 4_000);
    assert_eq!(aggregate[0].speed, Some(100.0));
    assert_eq!(
        store
            .get_observed_speed_for_tier("test/model/high", Some("fast"), None, now)
            .unwrap(),
        None
    );
}

#[test]
fn speed_sample_migration_preserves_existing_usage() {
    let scratch = cortexkit_test_support::ScratchDir::new("speed-sample-migration");
    let descriptor = dev_descriptor(
        "speed-sample-migration",
        scratch.path().join("routing.db").to_string_lossy(),
    );
    let legacy = open_sqlite(&descriptor).unwrap();
    let migrations = route_state_migrations();
    legacy
        .migrate(ROUTE_STATE_NAMESPACE, &migrations[..migrations.len() - 1])
        .unwrap();
    legacy
        .with_conn(|c| {
            c.execute(
                "INSERT INTO model_route_usage
        (id, route_key, capacity_key, provider_id, output_tokens, duration_ms, observed_at)
        VALUES ('head', 'test/model', 'test/model', 'test', 123, 456, 789)",
                [],
            )
        })
        .unwrap();
    drop(legacy);
    let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
    migrated
        .store
        .with_conn(|c| {
            let existing: (i64, i64, i64) = c.query_row(
                "SELECT output_tokens, duration_ms, observed_at
            FROM model_route_usage WHERE id = 'head'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            assert_eq!(existing, (123, 456, 789));
            assert_eq!(
                c.query_row("SELECT COUNT(*) FROM model_route_speed_samples", [], |r| {
                    r.get::<_, i64>(0)
                })?,
                0
            );
            Ok(())
        })
        .unwrap();
}
