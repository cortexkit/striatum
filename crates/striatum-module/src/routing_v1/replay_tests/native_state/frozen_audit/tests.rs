#![cfg(test)]
//! Frozen audit rows must stay equal to the fixture's initial migrated store.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

pub(super) const TABLES: &[&str] = &[
    "account_fence",
    "credential_entitlement_fences",
    "account_route_fence",
    "account_non_quota_route_fence",
    "account_size_refusal",
    "account_non_quota_size_refusal",
    "account_principal_exclusion",
    "account_transient_backoff",
    "fence_recheck",
    "fence_probe_authorization",
    "fence_recheck_route_clear",
    "fence_success_pool",
    "fence_retired_origin",
];
struct Baseline {
    tables: BTreeMap<String, Value>,
    frames: usize,
}
static BASELINES: LazyLock<Mutex<BTreeMap<PathBuf, Baseline>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn raw_table(conn: &rusqlite::Connection, table: &str) -> Value {
    let mut query = conn
        .prepare(&format!("SELECT rowid,* FROM \"{table}\" ORDER BY rowid"))
        .unwrap();
    let columns = query
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let rows = query
        .query_map([], |row| {
            let mut cells = Vec::new();
            for index in 0..columns.len() {
                cells.push(match row.get_ref(index)? {
                    ValueRef::Null => json!(["null", null]),
                    ValueRef::Integer(value) => json!(["integer", value]),
                    ValueRef::Real(value) => json!(["real", value]),
                    ValueRef::Text(value) => json!(["text", std::str::from_utf8(value).unwrap()]),
                    ValueRef::Blob(value) => json!(["blob", value]),
                });
            }
            Ok(cells)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    json!({"columns":columns,"rows":rows})
}

pub(super) struct Guard {
    path: PathBuf,
}
pub(super) fn begin(harness: &Harness) -> Guard {
    let declared = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/frozen-block-names.txt"
    ))
    .lines()
    .filter_map(|line| line.strip_prefix("table "))
    .map(str::trim)
    .collect::<BTreeSet<_>>();
    assert_eq!(TABLES.iter().copied().collect::<BTreeSet<_>>(), declared);
    let path = harness.store_path().to_path_buf();
    let connection = open_archive(harness);
    let tables = TABLES
        .iter()
        .map(|table| ((*table).into(), raw_table(&connection, table)))
        .collect();
    assert!(
        BASELINES
            .lock()
            .unwrap()
            .insert(path.clone(), Baseline { tables, frames: 0 })
            .is_none(),
        "one frozen baseline per fixture"
    );
    Guard { path }
}

pub(super) fn open_archive(harness: &Harness) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        harness.store_path().with_file_name("history.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
}

pub(super) fn check(harness: &Harness, connection: &rusqlite::Connection) {
    let mut baselines = BASELINES.lock().unwrap();
    let Some(baseline) = baselines.get_mut(&harness.store_path()) else {
        return;
    };
    // Compare raw SQL types, text bytes and physical rowids as well as values;
    // parsing JSON-looking audit text would hide a change to the frozen copy.
    let actual = TABLES
        .iter()
        .map(|table| ((*table).into(), raw_table(connection, table)))
        .collect::<BTreeMap<_, _>>();
    let expected = baseline.tables.clone();
    if actual != expected {
        drop(baselines);
        assert_eq!(
            actual, expected,
            "frozen audit rows changed since fixture migration"
        );
        return;
    }
    baseline.frames += 1;
}
impl Guard {
    pub(super) fn frame_checks(&self) -> usize {
        BASELINES.lock().unwrap()[&self.path].frames
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        BASELINES.lock().unwrap().remove(&self.path);
    }
}

#[tokio::test]
async fn a_frozen_audit_write_is_detected_at_the_next_replay_frame() {
    let (harness, _) = configured().await;
    let guard = begin(&harness);
    let conn =
        rusqlite::Connection::open(harness.store_path().with_file_name("history.db")).unwrap();
    check(&harness, &conn);
    assert_eq!(guard.frame_checks(), 1);
    conn.execute(
        "INSERT INTO fence_retired_origin(kind,origin_key,evidence,evidence_at,retired_at)
        VALUES('route','[\"baseline/model\"]','[]',1,2)",
        [],
    )
    .unwrap();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&harness, &conn)));
    assert!(
        failure.is_err(),
        "a frozen-row write must fail before a new frame is accepted"
    );
    assert_eq!(guard.frame_checks(), 1, "a rejected frame is not counted");
}
