use super::*;
use crate::{dev_descriptor, route_state_migrations, V1Decision};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use striatum_core::accounts::AllowedAccountClass;

fn descriptor(
    scratch: &cortexkit_test_support::ScratchDir,
) -> cortexkit_store_types::StorageDescriptor {
    dev_descriptor(
        "engine-measurements",
        scratch.path().join("routing.db").to_string_lossy(),
    )
}

fn evaluation(score: f64, time: u64) -> EngineEvaluation {
    EngineEvaluation {
        kind: "transcribe".into(),
        provider: "media".into(),
        model: "engine".into(),
        metric: "wer".into(),
        set_id: "reference".into(),
        score,
        measured_at_ms: time,
    }
}

fn decision(id: &str) -> V1Decision {
    V1Decision {
        decision_id: id.into(),
        principal: "reserved:phone".into(),
        consumer: "phone.media".into(),
        allowed_account_class: AllowedAccountClass::PricedPerTokenOnly,
        project_id: None,
        provider: "media".into(),
        model: "engine".into(),
        variant: None,
        runner_module: "colliculus".into(),
        affinity: None,
        created_at_ms: 100,
        expires_at_ms: 600100,
    }
}

fn sample(wall: u64) -> EngineSpeedSample {
    EngineSpeedSample {
        kind: "transcribe".into(),
        provider: "media".into(),
        model: "engine".into(),
        outcome: "done".into(),
        duration_ms: Some(1000),
        wall_ms: wall,
        first_output_ms: Some(10),
    }
}

#[test]
fn engine_evaluations_keep_newest_per_identity_and_survive_reopen() {
    let scratch = cortexkit_test_support::ScratchDir::new("engine-eval-newest");
    let descriptor = descriptor(&scratch);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    for (score, time) in [(0.8, 200), (0.9, 100), (0.7, 200)] {
        store
            .record_engine_evaluation(&evaluation(score, time))
            .unwrap();
    }
    let mut other = evaluation(0.1, u64::MAX);
    other.set_id = "other".into();
    store.record_engine_evaluation(&other).unwrap();
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let (evals, _) = store
        .engine_measurements("transcribe", Some("wer"), &[("media", "engine")])
        .unwrap();
    assert_eq!(evals.len(), 2);
    assert!(evals.contains(&evaluation(0.7, 200)));
    assert!(evals.contains(&other));
    assert!(store
        .engine_measurements("image", Some("wer"), &[("media", "engine")])
        .unwrap()
        .0
        .is_empty());
}

#[test]
fn engine_speed_window_is_bounded_and_replay_stays_deduplicated_after_eviction() {
    let scratch = cortexkit_test_support::ScratchDir::new("engine-speed-window");
    let descriptor = descriptor(&scratch);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    for i in 0..25 {
        let id = format!("engine-decision-{i}");
        store.v1_record_selection(&decision(&id), None).unwrap();
        store.record_engine_speed(&id, &sample(i)).unwrap();
        store.record_engine_speed(&id, &sample(999)).unwrap();
    }
    store
        .record_engine_speed("engine-decision-0", &sample(999))
        .unwrap();
    let (_, samples) = store
        .engine_measurements("transcribe", None, &[("media", "engine")])
        .unwrap();
    assert_eq!(samples.len(), ENGINE_SPEED_SAMPLE_LIMIT);
    assert_eq!(samples.first().unwrap().wall_ms, 24);
    assert_eq!(samples.last().unwrap().wall_ms, 5);
    assert!(!samples.iter().any(|sample| sample.wall_ms == 999));
    let mut large = sample(u64::MAX);
    large.duration_ms = Some(u64::MAX);
    large.first_output_ms = Some(u64::MAX);
    store.v1_record_selection(&decision("large"), None).unwrap();
    store.record_engine_speed("large", &large).unwrap();
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let (_, samples) = store
        .engine_measurements("transcribe", None, &[("media", "engine")])
        .unwrap();
    assert_eq!(samples.len(), ENGINE_SPEED_SAMPLE_LIMIT);
    assert_eq!(samples[0], large);
}

#[test]
fn engine_measurement_selection_reads_do_not_acquire_the_writer() {
    let scratch = cortexkit_test_support::ScratchDir::new("engine-measurement-reader");
    let store = Arc::new(SqliteRouteStateStore::open(&descriptor(&scratch)).unwrap());
    store
        .record_engine_evaluation(&evaluation(0.9, 100))
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writing = Arc::clone(&store);
    let writer = std::thread::spawn(move || {
        writing
            .store
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction()?;
                tx.execute("UPDATE routing_engine_eval SET score = 0.1", [])?;
                ready_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                tx.rollback()
            })
            .unwrap()
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (read_tx, read_rx) = mpsc::channel();
    let reading = Arc::clone(&store);
    let reader = std::thread::spawn(move || {
        read_tx
            .send(reading.engine_measurements("transcribe", Some("wer"), &[("media", "engine")]))
            .unwrap();
    });
    let read = read_rx.recv_timeout(Duration::from_secs(5));
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    reader.join().unwrap();
    let (evals, _) = read
        .expect("selection reader waited for the writer")
        .unwrap();
    assert_eq!(evals[0].score, 0.9, "read sees only committed scores");
}

#[test]
fn engine_measurement_migration_upgrades_existing_decisions_without_samples() {
    let scratch = cortexkit_test_support::ScratchDir::new("engine-measurement-migration");
    let descriptor = descriptor(&scratch);
    let migrations = route_state_migrations();
    let old = SqliteRouteStateStore::open_with_migrations(
        &descriptor,
        &migrations[..migrations.len() - 1],
    )
    .unwrap();
    old.v1_record_selection(&decision("old"), None).unwrap();
    drop(old);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let (evals, samples) = store
        .engine_measurements("transcribe", Some("wer"), &[("media", "engine")])
        .unwrap();
    assert!(evals.is_empty() && samples.is_empty());
    assert_eq!(store.v1_decision("old").unwrap(), Some(decision("old")));
    store.record_engine_speed("old", &sample(250)).unwrap();
    assert_eq!(
        store
            .engine_measurements("transcribe", None, &[("media", "engine")])
            .unwrap()
            .1
            .len(),
        1
    );
}
