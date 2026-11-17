//! Usage: block_parity_rehearsal SNAPSHOT_DB NEW_WORK_DIR NOW_MS
//! SNAPSHOT_DB must be an offline copy, not the live store. It is opened read
//! only and cloned with its write-ahead log; normal opening touches only the clone.

use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use striatum_store::BlockParity;
#[cfg(test)]
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

#[path = "block_parity_rehearsal/native.rs"]
mod native;

fn rehearse(
    source: &Path,
    work: &Path,
    now: i64,
) -> Result<BlockParity, Box<dyn std::error::Error>> {
    std::fs::create_dir(work)?;
    let target = work.join("state.db");
    let snapshot = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    snapshot.execute(
        "VACUUM INTO ?1",
        [target.to_str().ok_or("work path must be UTF-8")?],
    )?;
    drop(snapshot);
    native::rehearse(&target, now)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: block_parity_rehearsal SNAPSHOT_DB NEW_WORK_DIR NOW_MS".into());
    }
    let source = PathBuf::from(&args[0]);
    let work = PathBuf::from(&args[1]);
    let now: i64 = args[2].to_str().ok_or("NOW_MS must be UTF-8")?.parse()?;
    let report = rehearse(&source, &work, now)?;
    println!(
        "now_ms={now} reference_adapter_load_us={} reference_active_blocks={} reference_cleared_evidence={}",
        report.load_time.as_micros(),
        report.active,
        report.cleared_evidence
    );
    println!("scope=source-derived per-origin predicates; full ClassContext composition is checked on the three module fixtures");
    println!(
        "identities={} combinations_per_identity={} function_comparisons={} mismatch_count={}",
        report.identities,
        report.combinations_per_identity,
        report.function_comparisons,
        report.differences.len()
    );
    println!("combinations=all boundary times x 16 (alias,pool,recovered,binding) boolean inputs x 3 probe owners x 2 continuation flags x 2 media flags");
    for difference in &report.differences {
        println!("difference: {difference}");
    }
    if !report.differences.is_empty() {
        return Err("block parity failed".into());
    }
    println!("no differences");
    Ok(())
}

#[cfg(all(test, feature = "test-support"))]
#[test]
fn blocks_parity_rehearsal_clones_without_touching_source() {
    let scratch = cortexkit_test_support::ScratchDir::new("block-parity-rehearsal");
    let source = scratch.path().join("snapshot.db");
    // The independent SQL fixture is frozen at schema 93. The rehearsal
    // upgrades only its disposable clone through the registered migrations.
    let store = SqliteRouteStateStore::open_at_version_for_test(
        &dev_descriptor("block-parity-source", source.to_string_lossy()),
        93,
    )
    .unwrap();
    let now = 1_900_000_000_000;
    store.seed_block_parity_fixture_for_test(now);
    store.assert_block_parity_fixture_coverage_for_test(now);
    drop(store);
    let before = std::fs::read(&source).unwrap();
    let report = rehearse(&source, &scratch.path().join("rehearsal"), now).unwrap();
    assert!(report.differences.is_empty(), "{report:#?}");
    // There are fourteen source kinds and two account origins. The second
    // account-wide refusal stays active: only one of its two quota pools has
    // a recorded successful probe.
    assert_eq!(report.active, 15);
    assert_eq!(report.cleared_evidence, 1);
    assert!(report.identities > 0 && report.function_comparisons > 0);
    let migrated = SqliteRouteStateStore::open(&dev_descriptor(
        "block-parity-reloaded",
        scratch.path().join("rehearsal/state.db").to_string_lossy(),
    ))
    .unwrap();
    let loaded = migrated.load_blocks(now).unwrap();
    assert_eq!(
        loaded.blocks.iter().map(|row| (row.kind.as_str(), row.origin_key.as_str())).collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            ("account", r#"["parity-fence"]"#),
            ("account", r#"["parity-recovery"]"#),
            ("credential", r#"["cpu-provider-0","oauth:cpu-0"]"#),
            ("account_route", r#"["cpu-provider-0","oauth:cpu-0","model-0"]"#),
            ("non_quota_route", r#"["cpu-provider-0","oauth:cpu-0","model-0"]"#),
            ("transient_route", r#"["cpu-provider-0","oauth:cpu-0","model-0"]"#),
            ("pool_billing", r#"["cpu-provider-0","oauth:cpu-0"]"#),
            ("pool_rate", r#"["cpu-provider-0","oauth:cpu-0"]"#),
            ("provider", r#"["cpu-provider-0"]"#),
            ("route", r#"["parity/route"]"#),
            ("failure", r#"["parity/route"]"#),
            ("size", r#"["cpu-provider-0","oauth:cpu-0","model-0",17]"#),
            ("non_quota_size", r#"["cpu-provider-0","oauth:cpu-0","model-0",17]"#),
            ("principal", r#"["parity-principal","parity-admission","oauth:cpu-0"]"#),
            ("observation", r#"["cpu-provider-0","account-0"]"#),
        ]),
        "all fifteen origin identities, including both account fences, survive migration and reload",
    );
    assert_eq!(std::fs::read(source).unwrap(), before);
    println!("synthetic copy rehearsal: {report:?}");
}
