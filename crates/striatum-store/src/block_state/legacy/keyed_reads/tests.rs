//! The original query-plan invariant on its genuine schema-93 catalogue.
use super::*;

struct Rig {
    store: crate::SqliteRouteStateStore,
    _scratch: cortexkit_test_support::ScratchDir,
}

fn rig(name: &str) -> Rig {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy()),
        93,
    )
    .unwrap();
    Rig {
        store,
        _scratch: scratch,
    }
}

/// Every read exempt from the no-SQL-reads rule (a decision's check of the
/// state, a route-state writer's read of the row it writes) is keyed:
/// SQLite's plan for it searches the watched tables by an index or primary
/// key and never scans one.
#[test]
fn every_exempt_read_is_keyed() {
    let rig = rig("block-state-exempt-reads-keyed");
    let scans = rig
        .store
        .with_reader_for_test(|conn| {
            let mut scans = Vec::new();
            for sql in reviewed_exempt_reads() {
                let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
                let unbound = vec![rusqlite::types::Value::Null; plan.parameter_count()];
                let details: Vec<String> = plan
                    .query_map(rusqlite::params_from_iter(unbound), |r| {
                        r.get::<_, String>(3)
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                let scanned: Vec<&String> = details
                    .iter()
                    .filter(|detail| {
                        detail.starts_with("SCAN ")
                            && !detail.starts_with("SCAN json_each")
                            && !detail.starts_with("SCAN CONSTANT ROW")
                    })
                    .collect();
                if !scanned.is_empty() {
                    scans.push(format!("{sql}\n  plan: {details:?}"));
                }
            }
            Ok(scans)
        })
        .unwrap();
    assert!(
        scans.is_empty(),
        "exempt reads that scan:\n{}",
        scans.join("\n")
    );
    assert!(reviewed_exempt_reads().len() >= 30);
}
