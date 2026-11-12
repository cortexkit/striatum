use striatum_core::RouteStateStore;
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

fn store(dir: &cortexkit_test_support::ScratchDir) -> SqliteRouteStateStore {
    SqliteRouteStateStore::open(&dev_descriptor(
        "striatum",
        dir.join("store.db").to_string_lossy().into_owned(),
    ))
    .unwrap()
}

fn route_key(fact: &striatum_core::ModelFact) -> String {
    striatum_core::model_facts::make_route_key(
        &fact.provider_id,
        &fact.model_id,
        fact.variant.as_deref(),
    )
}

#[test]
fn pure_account_reads_never_acquire_the_writer() {
    let dir = cortexkit_test_support::ScratchDir::new("reads-off-writer");
    let store = store(&dir);
    store.put_quota_snapshot("{\"windows\":[]}", 123).unwrap();
    store
        .record_account_eligibility(&striatum_store::AccountEligibilityFact {
            provider: "p".into(),
            credential_id: "c".into(),
            model: "m".into(),
            listing_fingerprint: Some("listing".into()),
            code: "not_found".into(),
            observed_at: 123,
        })
        .unwrap();
    macro_rules! read {
        ($expression:expr) => {{
            let acquisitions = store.writer_acquisition_count_for_test();
            let commits = store.writer_commit_count_for_test();
            let value = $expression.unwrap();
            assert_eq!(
                store.writer_acquisition_count_for_test(),
                acquisitions,
                "{}",
                stringify!($expression)
            );
            assert_eq!(
                store.writer_commit_count_for_test(),
                commits,
                "{}",
                stringify!($expression)
            );
            value
        }};
    }
    assert!(read!(store.account_read(|tx| tx.admission("p", "missing"))).is_none());
    assert!(read!(store.account_read(|tx| tx.existing_salt())).is_none());
    assert!(read!(store.account_read(|tx| tx.admission_size_class("p", "missing"))).is_none());
    assert_eq!(read!(store.account_eligibility())[0].credential_id, "c");
    assert!(read!(store.account_size_refusals(123)).is_empty());
    assert!(read!(store.account_route_fence_latest_reports()).is_empty());
    assert!(read!(store.account_route_fences(123)).is_empty());
    assert!(read!(store.transient_account_route_fences(123)).is_empty());
    assert!(read!(store.non_quota_account_route_fences(123)).is_empty());
    assert!(read!(store.account_decision_service_tier("missing")).is_none());
    assert!(read!(store.account_decisions("enforce", 10)).is_empty());
    assert!(read!(store.account_decision("missing")).is_none());
    assert_eq!(read!(store.account_serving_counts()).total(), 0);
    assert_eq!(read!(store.quota_snapshot()).unwrap().observed_at, 123);
    assert!(read!(store.account_quota_readings()).is_empty());
    assert!(read!(store.account_quota_completeness()).is_empty());
    assert!(read!(store.account_vault_roster()).is_none());
    assert!(read!(store.credential_entitlement_fences()).is_empty());
    assert!(read!(store.decision_provider_identity("missing")).is_none());
    assert!(read!(store.decision_account_class("missing")).is_none());
    assert!(read!(store.get_quota_readings("missing")).is_empty());
}

#[test]
fn catalog_inventory_cache_uses_only_the_writer_local_key_check() {
    let dir = cortexkit_test_support::ScratchDir::new("catalog-off-writer");
    let store = store(&dir);
    let writer_steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let steps = writer_steps.clone();
    store
        .with_writer_for_test(|conn| {
            conn.progress_handler(
                1,
                Some(move || {
                    steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                }),
            );
            Ok(())
        })
        .unwrap();
    for expected_acquisitions in [2, 1] {
        writer_steps.store(0, std::sync::atomic::Ordering::Relaxed);
        let acquisitions = store.writer_acquisition_count_for_test();
        let commits = store.writer_commit_count_for_test();
        let rows = store.get_catalog_facts().unwrap();
        assert!(!rows.is_empty());
        assert!(rows
            .windows(2)
            .all(|pair| route_key(&pair[0].fact) <= route_key(&pair[1].fact)));
        assert_eq!(
            store.writer_acquisition_count_for_test() - acquisitions,
            expected_acquisitions
        );
        assert_eq!(store.writer_commit_count_for_test(), commits);
        assert!(
            writer_steps.load(std::sync::atomic::Ordering::Relaxed) < 200,
            "only the small connection-local change key may be read on the writer"
        );
    }
    store
        .with_writer_for_test(|conn| {
            conn.progress_handler(0, None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
    let key = route_key(&store.get_catalog_facts().unwrap()[0].fact);
    for external in [false, true] {
        let update = |conn: &rusqlite::Connection| {
            conn.execute(
            "UPDATE model_facts SET excluded=?1, fusiform_present=?1, facts_source=?2,
             fusiform_version=987, canonical_id='cache-test', reasoning_efforts_json='[\"low\",\"high\"]'
             WHERE route_key=?3",
            rusqlite::params![i64::from(external), if external { "external" } else { "local" }, key],
        )
        };
        if external {
            update(&rusqlite::Connection::open(dir.join("store.db")).unwrap()).unwrap();
        } else {
            store.with_writer_for_test(update).unwrap();
        }
        let rows = store.get_catalog_facts().unwrap();
        let row = rows.iter().find(|row| route_key(&row.fact) == key).unwrap();
        assert_eq!(row.excluded, external);
        assert_eq!(row.fusiform_present, Some(external));
        assert_eq!(row.fusiform_version, Some(987));
        assert_eq!(
            row.facts_source.as_deref(),
            Some(if external { "external" } else { "local" })
        );
        assert_eq!(row.canonical_id.as_deref(), Some("cache-test"));
        assert_eq!(row.reasoning_efforts.as_ref().unwrap(), &["low", "high"]);
    }
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "UPDATE model_facts SET fusiform_version=988 WHERE route_key=?1",
                [&key],
            )
        })
        .unwrap();
    let interleaved = store
        .get_catalog_facts_with_load_hook_for_test(|| {
            // A local writer can run before validation because the reader load no
            // longer holds the writer connection.
            store
                .with_writer_for_test(|conn| {
                    conn.execute(
                        "UPDATE model_facts SET fusiform_version=989 WHERE route_key=?1",
                        [&key],
                    )
                })
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        interleaved
            .iter()
            .find(|row| route_key(&row.fact) == key)
            .unwrap()
            .fusiform_version,
        Some(988)
    );
    assert!(!store.catalog_inventory_is_cached_for_test());
    let fresh = store.get_catalog_facts().unwrap();
    assert_eq!(
        fresh
            .iter()
            .find(|row| route_key(&row.fact) == key)
            .unwrap()
            .fusiform_version,
        Some(989)
    );
    assert!(store.catalog_inventory_is_cached_for_test());
}

#[test]
#[ignore = "release-only schema-check preparation measurement"]
fn fence_schema_check_preparation_cost() {
    let dir = cortexkit_test_support::ScratchDir::new("fence-schema-cost");
    let store = store(&dir);
    store.with_reader_for_test(|conn| {
        const SQL: &str = "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='fence_recheck')";
        for cached in [false, true] {
            let start = std::time::Instant::now();
            for _ in 0..10_000 {
                let enabled: bool = if cached {
                    conn.prepare_cached(SQL)?.query_row([], |row| row.get(0))?
                } else {
                    conn.query_row(SQL, [], |row| row.get(0))?
                };
                assert!(enabled);
            }
            eprintln!("fence schema check cached={cached} ns/call={:.2}", start.elapsed().as_nanos() as f64 / 10_000.0);
        }
        Ok(())
    }).unwrap();
}
