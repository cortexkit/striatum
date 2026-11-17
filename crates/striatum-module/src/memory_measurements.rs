//! Opt-in heap measurements. Run one ignored measurement per process with
//! `--features heap-measure -- --ignored --nocapture --test-threads=1`.
//! Counts are Rust allocator requests, not RSS or SQLite's C allocations.

use dhat::HeapStats;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

pub(crate) fn profiler(name: &str) -> dhat::Profiler {
    let directory = std::env::var("STRIATUM_HEAP_OUTPUT").unwrap_or_else(|_| ".".into());
    dhat::Profiler::builder()
        .file_name(std::path::Path::new(&directory).join(format!("{name}.dhat.json")))
        .build()
}

/// Measure ownership by destruction, so allocations moved out of a loader's
/// input vectors and allocations made by its child thread are both counted.
pub(crate) fn release<T>(name: &str, value: T) -> (usize, usize) {
    let before = HeapStats::get();
    drop(value);
    let after = HeapStats::get();
    let bytes = before.curr_bytes - after.curr_bytes;
    let blocks = before.curr_blocks - after.curr_blocks;
    println!("HEAP holder={name} bytes={bytes} blocks={blocks}");
    (bytes, blocks)
}

pub(crate) fn operation<T>(name: &str, f: impl FnOnce() -> T) -> T {
    let before = HeapStats::get();
    let value = f();
    let after = HeapStats::get();
    println!(
        "HEAP operation={name} retained_bytes={} retained_blocks={} allocated_bytes={} allocated_blocks={} process_peak_bytes={}",
        after.curr_bytes as i64 - before.curr_bytes as i64,
        after.curr_blocks as i64 - before.curr_blocks as i64,
        after.total_bytes - before.total_bytes,
        after.total_blocks - before.total_blocks,
        after.max_bytes,
    );
    value
}

#[test]
#[ignore = "representative block-table heap profile; run alone"]
fn live_shaped_block_heap() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-heap");
    let store = striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "block-heap",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    let now = 1_800_000_000_000_i64;
    store.with_writer_for_test(|conn| {
        let mut sql = String::from("BEGIN; DELETE FROM model_route_state;");
        for i in 0..80 {
            sql += &format!("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,recovery_set_json) VALUES ('fence-{i}','provider-{}','account-{i}','{{}}','{{\"reset\":{{\"at_ms\":{now}}}}}','broca_account_limit',{now},'reserved:broca',{now},'[]');", i%11);
        }
        for i in 0..4 {
            sql += &format!("INSERT INTO credential_entitlement_fences VALUES ('provider-{i}','oauth:credential-{i}','billing_exhausted','listing',{now}); INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,updated_at) VALUES ('provider-{i}','oauth:credential-{i}',{now},'billing',{now});");
        }
        for i in 0..8 {
            sql += &format!("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at) VALUES ('provider-{i}',{now},{now},1,'provider capacity',{now},{now});");
        }
        for i in 0..861 {
            sql += &format!("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at) VALUES ('provider-{}/model-{i}','provider-{}/model-{i}','provider-{}','model-{i}',{now});",i%11,i%11,i%11);
        }
        sql += &format!("INSERT INTO account_route_fence VALUES ('provider-0','oauth:credential-0','model-0',{now},'decision',{now},{now},'quota'); INSERT INTO account_transient_backoff VALUES ('provider-0','account-0',{now},{now},'event','[]');");
        for i in 0..15 {
            sql += &format!("INSERT INTO fence_recheck(kind,origin_key,provider,evidence,evidence_at,quota,next_recheck_at) VALUES ('account','[\"fence-{i}\"]','provider-{}','{{}}',{now},1,{now});",i%11);
        }
        for i in 0..6 {
            sql += &format!("INSERT INTO fence_probe_authorization VALUES ('owner-{i}','account','[\"fence-{i}\"]','{{}}',{now},{now},1);");
        }
        sql += "COMMIT;";
        conn.execute_batch(&sql)
    }).unwrap();
    let _profiler = profiler("block-state");
    let blocks = operation("BlockState::load", || {
        store.account_read(|tx| tx.load_block_state()).unwrap()
    });
    println!("BLOCK_TABLES {:?}", blocks.table_sizes());
    assert_eq!(
        blocks
            .table_sizes()
            .into_iter()
            .find(|(table, _)| *table == "model_route_state")
            .unwrap()
            .1,
        861
    );
    let clone = operation("BlockState::clone", || blocks.clone());
    release("BlockState::clone", clone);
    let check = operation("drift_full_block_load", || {
        store.account_read(|tx| tx.load_block_state()).unwrap()
    });
    assert_eq!(blocks, check);
    release("drift_full_block_copy", check);
    release("BlockState", blocks);
}

#[test]
#[ignore = "process-wide heap profiler; run alone"]
fn heap_measure_allocator_control() {
    let _profiler = profiler("allocator-control");
    let bytes = std::hint::black_box(vec![7_u8; 12345]);
    assert_eq!(release("control", bytes), (12345, 1));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "representative long-lived holders; run alone"]
async fn live_shaped_other_holders_heap() {
    use crate::routing_v1::request_cpu_bench::{representative_fixture, FixtureCounts};
    use striatum_core::RouteStateStore;
    let fixture = representative_fixture(FixtureCounts {
        subscription_rules: 8,
        exclusion_rules: 8,
        runner_route_rules: 8,
        catalog_facts: 852,
        listed_credentials: 14,
    });
    let samples: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/served-payloads.json")).unwrap();
    let mut payload = samples["catalog.get — one model, current"]["response"].clone();
    let model = payload["models"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let models = (0..852)
        .map(|i| (format!("anthropic/claude-sonnet-4-5-{i}"), model.clone()))
        .collect();
    payload["models"] = serde_json::Value::Object(models);
    let bytes = serde_json::to_vec(&payload).unwrap();
    let _profiler = profiler("other-holders");
    let catalog = operation("decode_catalog_get_852", || {
        crate::fusiform_catalog::decode_catalog_get(&bytes).unwrap()
    });
    let catalog_copy = operation("catalog_deep_clone", || catalog.clone());
    release("catalog_deep_clone", catalog_copy);
    release("CatalogSnapshot_852_sample_rows", catalog);
    let facts = operation("store_get_facts_852", || {
        fixture.store().get_facts().unwrap()
    });
    assert_eq!(facts.len(), 852);
    let planes = operation("PlaneCatalog_852", || {
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
    });
    release("billing_PlaneCatalog_852", planes);
    let shared = fixture.credentials();
    let credentials = shared.lock().unwrap();
    let snapshot = operation("BrocaCredentials_snapshot", || credentials.snapshot());
    release("BrocaCredentials_snapshot_extra", snapshot);
    release(
        "vault_listing_14_sample_rows",
        credentials.listed_credentials().to_vec(),
    );
    release("plan_prices_sample", credentials.plan_prices.clone());
    release(
        "fixture_billing_PlaneCatalog",
        credentials.billing_catalog.clone(),
    );
    let settings = fixture.operator_config();
    let catalog = fixture.store().get_selection_catalog().unwrap();
    let index = operation("ServingIndex_852", || {
        crate::serving_index::ServingIndex::get(&settings, &credentials, &catalog)
    });
    release("ServingIndex_reader_handle", index);
    drop(credentials);
    let quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
    let sample = quota.readings.readings.values().next().unwrap().clone();
    let mut shaped = crate::account_quota::AccountQuotaState::default();
    for i in 0..23 {
        let mut reading = sample.clone();
        reading.provider = format!("provider-{}", i % 21);
        reading.account_id = format!("account-{i}");
        shaped.readings.readings.insert(
            (reading.provider.clone(), reading.account_id.clone()),
            reading,
        );
    }
    for i in 0..21 {
        shaped.readings.completeness.insert(
            format!("provider-{i}"),
            crate::account_quota::ProviderCompleteness {
                last_polled_at_ms: 1,
                last_poll_complete: true,
                last_complete_at_ms: Some(1),
            },
        );
    }
    shaped.roster = quota.roster.clone();
    drop(quota);
    release(
        "AccountReadings_23_sample_entries_21_providers",
        shaped.readings.clone(),
    );
    release("AccountQuotaState_23_readings_14_roster", shaped);
    let baseline: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/routing-baseline-2026-10-24.json")).unwrap();
    let windows = serde_json::from_value(baseline["quota"]["result"].clone()).unwrap();
    let mut cache = crate::QuotaCache::default();
    crate::apply_quota_refresh(&mut cache, windows, baseline["clock_ms"].as_u64().unwrap());
    println!("COUNTS quota_window_sample_entries={}", cache.windows.len());
    release("QuotaCache_repository_windows", cache);
    release(
        "settings_clone_excluding_shared_compiled_policy",
        (*settings).clone(),
    );
    let scratch = cortexkit_test_support::ScratchDir::new("store-cache-heap");
    let store = striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "store-cache-heap",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    for row in store.get_catalog_facts().unwrap() {
        store
            .delete_fact(&striatum_core::make_route_key(
                &row.fact.provider_id,
                &row.fact.model_id,
                row.fact.variant.as_deref(),
            ))
            .unwrap();
    }
    for fact in &facts {
        store
            .upsert_fact(fact, striatum_core::route_state_store::FactSource::Seed)
            .unwrap();
    }
    let catalog_rows = operation("store_catalog_facts_cache_and_reader", || {
        store.get_catalog_facts().unwrap()
    });
    assert_eq!(catalog_rows.len(), 852);
    release("catalog_facts_reader_rows_852", catalog_rows);
    let resolved = operation("store_resolved_facts_cache_and_reader", || {
        store.get_facts().unwrap()
    });
    release("resolved_facts_reader_rows_852", resolved);
    let selection = operation("store_selection_catalog_cache", || {
        store.get_selection_catalog().unwrap()
    });
    release("selection_catalog_reader_handle", selection);
    store.with_writer_for_test(|conn| conn.execute_batch(
        "DELETE FROM model_route_state;
         WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<860)
         INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at)
         SELECT 'provider/model-'||i,'provider/model-'||i,'provider','model-'||i,1800000000000 FROM n;"
    )).unwrap();
    let route_snapshot = operation("route_state_snapshot_861", || {
        store
            .build_snapshot_at_for_test(1_800_000_000_000.0, 1_800_000_000_000)
            .unwrap()
    });
    assert_eq!(route_snapshot.by_route.len(), 861);
    release("route_state_snapshot_861", route_snapshot);
    drop(store);
    release("resolved_facts_852", facts);
}

/// Sample process RSS while the profiler is off, so SQLite's native sort and
/// aggregate allocations are visible without dhat's bookkeeping overhead.
/// The sampled maximum is a lower bound on a shorter-than-sample-interval peak.
pub(crate) fn native_rss<T>(name: &str, f: impl FnOnce() -> T) -> T {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    fn rss() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<usize>()
                    .ok()
            })
            .unwrap()
            * 1024
    }
    let before = rss();
    let peak = AtomicUsize::new(before);
    let stop = AtomicBool::new(false);
    let result = std::thread::scope(|scope| {
        let sample = scope.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss(), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        });
        let result = f();
        peak.fetch_max(rss(), Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        sample.join().unwrap();
        result
    });
    println!("NATIVE_RSS operation={name} before_bytes={before} sampled_peak_bytes={} after_bytes={} sample_interval_ms=2",peak.load(Ordering::Relaxed),rss());
    result
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live-shaped catalog decode/apply heap profile; run alone"]
async fn live_shaped_catalog_job_heap() {
    use crate::routing_v1::request_cpu_bench::{representative_fixture, FixtureCounts};
    use striatum_core::RouteStateStore;
    let fixture = representative_fixture(FixtureCounts {
        subscription_rules: 8,
        exclusion_rules: 8,
        runner_route_rules: 8,
        catalog_facts: 852,
        listed_credentials: 14,
    });
    let credentials = fixture.credentials();
    let scratch = cortexkit_test_support::ScratchDir::new("catalog-job-heap");
    let store = striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "catalog-job-heap",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    for row in store.get_catalog_facts().unwrap() {
        store
            .delete_fact(&striatum_core::make_route_key(
                &row.fact.provider_id,
                &row.fact.model_id,
                row.fact.variant.as_deref(),
            ))
            .unwrap();
    }
    let samples: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/served-payloads.json")).unwrap();
    let mut payload = samples["catalog.get — one model, current"]["response"].clone();
    let model = payload["models"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    payload["models"] = serde_json::Value::Object(
        (0..12_193)
            .map(|i| {
                (
                    format!(
                        "{}/claude-sonnet-4-5-{i}",
                        if i < 852 {
                            "anthropic"
                        } else {
                            "unmapped-provider"
                        }
                    ),
                    model.clone(),
                )
            })
            .collect(),
    );
    payload["catalog_version"] = serde_json::json!(0);
    let bytes = serde_json::to_vec(&payload).unwrap();
    drop(payload);
    drop(samples);
    let native = std::env::var_os("STRIATUM_NATIVE_RSS").is_some();
    let _profiler = (!native).then(|| profiler("catalog-job"));
    let old = crate::fusiform_catalog::decode_catalog_get(&bytes).unwrap();
    crate::fusiform_catalog::apply_mapped_catalog(
        &store,
        &old,
        Some(&credentials),
        Some("anthropic"),
    )
    .unwrap();
    drop(old);
    assert_eq!(store.get_catalog_facts().unwrap().len(), 852);
    let apply = || {
        let mut wire = bytes.clone();
        let needle = b"\"catalog_version\":0";
        let offset = wire
            .windows(needle.len())
            .position(|w| w == needle)
            .unwrap();
        wire[offset + needle.len() - 1] = b'1';
        let snapshot = crate::fusiform_catalog::decode_catalog_get(&wire).unwrap();
        crate::fusiform_catalog::apply_mapped_catalog(
            &store,
            &snapshot,
            Some(&credentials),
            Some("anthropic"),
        )
        .unwrap();
        snapshot
    };
    println!(
        "COUNTS served_catalog_rows=12193 stored_model_facts=852 wire_bytes={}",
        bytes.len()
    );
    if native {
        let snapshot = native_rss("catalog_decode_and_apply_12193", apply);
        drop(snapshot);
    } else {
        let snapshot = operation("catalog_decode_and_apply_12193", apply);
        release("catalog_job_local_snapshot_12193", snapshot);
        let cached = credentials.lock().unwrap().catalog_snapshot.take();
        release("catalog_cached_snapshot_12193", cached);
    }
}
