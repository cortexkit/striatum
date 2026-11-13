use super::*;
use std::cell::RefCell;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

#[derive(Debug)]
enum Progress {
    Waiting,
    Finished(Result<(), String>),
}

thread_local! {
    static BUSY_HANDSHAKE: RefCell<Option<std::sync::Arc<std::sync::Mutex<(Sender<Progress>, Receiver<()>)>>>> = const { RefCell::new(None) };
}

fn wait_for_lock_release(_: i32) -> bool {
    BUSY_HANDSHAKE.with(|handshake| {
        let handshake = handshake.borrow();
        let handshake = handshake.as_ref().unwrap().lock().unwrap();
        let (progress, release) = &*handshake;
        progress.send(Progress::Waiting).unwrap();
        release.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    true
}

fn assert_writer_waits(
    store: SqliteRouteStateStore,
    path: &std::path::Path,
    operation: impl FnOnce(&SqliteRouteStateStore) -> RouteStateResult<()> + Send + 'static,
) -> SqliteRouteStateStore {
    let (progress_tx, progress_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let shared_handshake =
        std::sync::Arc::new(std::sync::Mutex::new((progress_tx.clone(), release_rx)));
    let writer_handshake = shared_handshake.clone();
    store
        .account_command(AccountCommandKind::Maintenance, move |_| {
            BUSY_HANDSHAKE.with(|slot| *slot.borrow_mut() = Some(writer_handshake));
            Ok(())
        })
        .unwrap();
    let blocker = Connection::open(path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let worker = std::thread::spawn(move || {
        BUSY_HANDSHAKE.with(|handshake| {
            *handshake.borrow_mut() = Some(shared_handshake);
        });
        store
            .set_busy_handler_for_test(Some(wait_for_lock_release))
            .unwrap();
        let result = operation(&store).map_err(|error| error.to_string());
        progress_tx
            .send(Progress::Finished(result.clone()))
            .unwrap();
        store.set_busy_handler_for_test(None).unwrap();
        (store, result)
    });
    // A deferred read-to-write upgrade reports Finished(database is locked)
    // instead of invoking the busy handler. The timeout only detects deadlocks.
    let progress = progress_rx.recv_timeout(Duration::from_secs(10));
    blocker.execute_batch("COMMIT").unwrap();
    let _ = release_tx.send(());
    let (store, result) = worker.join().unwrap();
    match progress.unwrap() {
        Progress::Waiting => {}
        Progress::Finished(result) => panic!("writer returned before the busy handler: {result:?}"),
    }
    result.unwrap();
    store
}

fn test_store(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let path = scratch.path().join("store.db");
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "transaction-contention",
        path.to_string_lossy(),
    ))
    .unwrap();
    (scratch, store)
}

fn seed_route(store: &SqliteRouteStateStore) {
    store
        .update_route_state(
            "test-provider/test-model",
            "test-provider/test-model",
            &RouteStateUpdate {
                cooldown_until: Some((now_ms() + 60_000) as f64),
                unscoped_wall: true,
                ..Default::default()
            },
        )
        .unwrap();
}

fn integer(store: &SqliteRouteStateStore, sql: &str) -> i64 {
    store
        .store
        .with_conn(|connection| connection.query_row(sql, [], |row| row.get(0)))
        .unwrap()
}

#[test]
fn clear_route_cooldowns_waits_for_the_writer() {
    let (scratch, store) = test_store("clear-route-contention");
    seed_route(&store);
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), |store| {
        assert!(store
            .clear_route_cooldowns("test-provider/test-model")?
            .is_some());
        Ok(())
    });
    assert_eq!(integer(&store, "SELECT COUNT(*) FROM model_route_state WHERE route_key='test-provider/test-model' AND cooldown_until IS NOT NULL"), 0);
}

#[test]
fn prune_unserved_unpicked_facts_waits_for_the_writer() {
    let (scratch, store) = test_store("prune-facts-contention");
    let fact = store.get_facts().unwrap().remove(0);
    let route = fact_to_route(&fact).route_key;
    store
        .store
        .with_conn(|connection| {
            connection.execute(
                "UPDATE model_facts SET fusiform_present=0 WHERE route_key=?1",
                [&route],
            )
        })
        .unwrap();
    let expected = route.clone();
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), move |store| {
        assert!(store.prune_unserved_unpicked_facts()?.contains(&expected));
        Ok(())
    });
    assert!(store.get_fact_by_route_key(&route).unwrap().is_none());
}

#[test]
fn upsert_fact_waits_for_the_writer() {
    let (scratch, store) = test_store("upsert-fact-contention");
    let mut fact = store.get_facts().unwrap().remove(0);
    fact.context_window = Some(123456.0);
    let route = fact_to_route(&fact).route_key;
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), move |store| {
        store.upsert_fact(&fact, FactSource::Override)
    });
    assert_eq!(
        store
            .get_fact_by_route_key(&route)
            .unwrap()
            .unwrap()
            .context_window,
        Some(123456.0)
    );
}

#[test]
fn clear_operator_fact_fields_waits_for_the_writer() {
    let (scratch, store) = test_store("clear-fields-contention");
    let fact = store.get_facts().unwrap().remove(0);
    let route = fact_to_route(&fact).route_key;
    store
        .mark_operator_fact_fields(&route, &[CatalogFactField::ContextWindow])
        .unwrap();
    let expected = route.clone();
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), move |store| {
        assert_eq!(
            store.clear_operator_fact_fields(&expected, &[CatalogFactField::ContextWindow])?,
            vec![CatalogFactField::ContextWindow]
        );
        Ok(())
    });
    assert!(store
        .clear_operator_fact_fields(&route, &[CatalogFactField::ContextWindow])
        .unwrap()
        .is_empty());
}

#[test]
fn record_provider_cooldown_waits_for_the_writer() {
    let (scratch, store) = test_store("provider-cooldown-contention");
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), |store| {
        store.record_provider_cooldown(
            "test-provider",
            "test-provider/test-model",
            "test-provider/test-model",
            &RouteStateUpdate {
                cooldown_until: Some((now_ms() + 60_000) as f64),
                unscoped_wall: true,
                ..Default::default()
            },
        )
    });
    assert!(store
        .get_provider_cooldown("test-provider")
        .unwrap()
        .unwrap()
        .cooldown_until
        .is_some());
}

#[test]
fn set_decision_outcome_waits_for_the_writer() {
    let (scratch, store) = test_store("decision-outcome-contention");
    let id = store
        .record_decision(&DecisionInput {
            selected_route_key: Some("test-provider/test-model".into()),
            selected_provider_id: Some("test-provider".into()),
            selected_model_id: Some("test-model".into()),
            ..Default::default()
        })
        .unwrap();
    let expected = id.clone();
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), move |store| {
        store.set_decision_outcome(
            &expected,
            &DecisionOutcomeUpdate {
                provider_outcome: None,
                outcome: striatum_core::RouteOutcome::Completed,
                last_error: None,
                observed_at_ms: now_ms(),
                failure_backoff: Default::default(),
            },
        )
    });
    assert_eq!(
        store
            .get_decision_by_id(&id)
            .unwrap()
            .unwrap()
            .outcome
            .as_deref(),
        Some("completed")
    );
}

#[test]
fn clear_failure_backoff_waits_for_the_writer() {
    let (scratch, store) = test_store("clear-backoff-contention");
    seed_route(&store);
    store.store.with_conn(|connection| connection.execute("UPDATE model_route_state SET consecutive_terminal_failures=3,failure_backoff_until=123456,failure_backoff_reason='failure' WHERE route_key='test-provider/test-model'", [])).unwrap();
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), |store| {
        assert_eq!(
            store
                .clear_failure_backoff("test-provider/test-model")?
                .unwrap()
                .consecutive_terminal_failures,
            3
        );
        Ok(())
    });
    assert_eq!(integer(&store, "SELECT consecutive_terminal_failures FROM model_route_state WHERE route_key='test-provider/test-model'"), 0);
}

#[test]
fn clear_all_account_route_fences_waits_for_the_writer() {
    let (scratch, store) = test_store("clear-account-fences-contention");
    store
        .record_account_route_fence(&AccountRouteFence {
            provider: "test-provider".into(),
            credential_id: "credential".into(),
            model: "test-model".into(),
            until_ms: now_ms() + 60_000,
            decision_id: "decision".into(),
            recorded_at: now_ms(),
        })
        .unwrap();
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), |store| {
        assert_eq!(
            store.clear_all_account_route_fences_for_credential("credential")?,
            1
        );
        Ok(())
    });
    assert!(store.account_route_fences(now_ms()).unwrap().is_empty());
}

#[test]
fn incremental_fusiform_catalog_waits_for_the_writer() {
    let (scratch, store) = test_store("catalog-contention");
    let cursor = FusiformCatalogCursor {
        catalog_version: 123,
        resolved_at_ms: now_ms(),
    };
    let store = assert_writer_waits(store, &scratch.path().join("store.db"), move |store| {
        store.apply_fusiform_catalog(&[], cursor)
    });
    assert_eq!(store.fusiform_catalog_cursor().unwrap(), Some(cursor));
}
