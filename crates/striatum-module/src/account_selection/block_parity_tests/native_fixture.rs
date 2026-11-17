use crate::routing_v1::request_cpu_bench as bench;
use std::collections::{BTreeMap, BTreeSet};
use striatum_store::{dev_descriptor, FenceRecheck, SqliteRouteStateStore};

fn canonical(rows: Vec<FenceRecheck>) -> BTreeMap<(String, String), FenceRecheck> {
    rows.into_iter()
        .map(|mut row| {
            row.cleared_models.sort();
            ((row.kind.clone(), row.origin_key.clone()), row)
        })
        .collect()
}

/// The old SQL fixture supplies the expected effective rows and full coverage
/// before registered migration. The benchmark keeps its own unrelated state.
pub(super) fn seed(fixture: &bench::RequestFixture, now: i64) {
    let scratch = cortexkit_test_support::ScratchDir::new("class-parity-migration-bridge");
    let descriptor = dev_descriptor(
        "class-parity-migration-bridge",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let source = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    source.seed_block_parity_fixture_for_test(now);
    source.assert_block_parity_fixture_coverage_for_test(now);
    let expected = canonical(source.effective_blocks_oracle_for_test(now).unwrap());
    let times = source.block_parity_times(now).unwrap();
    let identities = source.block_parity_identities().unwrap();
    assert_eq!(
        expected.len(),
        15,
        "the complete old fixture has two account origins and thirteen other kinds"
    );
    drop(source);
    let source = SqliteRouteStateStore::open(&descriptor).unwrap();
    let actual = canonical(source.effective_blocks_oracle_for_test(now).unwrap());
    assert_eq!(actual,expected,"independent scalar SQL after registered095->096 migration must equal the captured pre096effective rows");
    let native_times: BTreeSet<_> = source
        .block_parity_times(now)
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        times.iter().all(|at| native_times.contains(at)),
        "the native boundary domain retains every old fixture time"
    );
    let native_ids: BTreeSet<_> = source
        .block_parity_identities()
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        identities.iter().all(|id| native_ids.contains(id)),
        "the native identity domain retains every old fixture scope"
    );
    fixture
        .store()
        .import_native_block_fixture_for_test(&source)
        .unwrap();
    let imported = canonical(
        fixture
            .store()
            .effective_blocks_oracle_for_test(now)
            .unwrap(),
    );
    for (key, row) in &expected {
        assert_eq!(
            imported.get(key),
            Some(row),
            "the captured native fixture retains {key:?} beside benchmark state"
        );
    }
    let loaded = fixture.store().load_blocks(now).unwrap();
    let kinds: BTreeSet<_> = loaded.blocks.iter().map(|row| row.kind.as_str()).collect();
    assert_eq!(
        kinds,
        striatum_core::blocks::SOURCE_KINDS.into_iter().collect(),
        "the actual native loader still sees all fourteen source kinds"
    );
    println!("full-context migration bridge: {} captured effective rows,{} preserved boundary times,{} preserved identities,all fourteen native source kinds",expected.len(),times.len(),identities.len());
}
