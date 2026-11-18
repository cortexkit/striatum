#![cfg(test)]

use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
struct DigestRows {
    rows: usize,
    sha256: String,
}

fn read_only(path: &std::path::Path) -> rusqlite::Connection {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    // The writer's connection-local catalogue counter is absent from another
    // connection. Recreate only its temporary schema to prepare its read;
    // main remains read-only and no routing data or counter value is copied.
    conn.execute_batch(
        "CREATE TEMP TABLE catalog_facts_writes (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1), writes INTEGER NOT NULL
    )",
    )
    .unwrap();
    let history = path.with_file_name("history.db");
    conn.execute("ATTACH DATABASE ?1 AS archive", [history.to_str().unwrap()])
        .unwrap();
    conn.execute_batch(
        "CREATE TEMP VIEW quota_reading_log AS
        SELECT * FROM main.pending_quota_reading_log
        UNION ALL SELECT h.* FROM archive.quota_reading_log h
        WHERE NOT EXISTS (SELECT 1 FROM main.pending_quota_reading_log p WHERE
            (p.account_key,p.window_key,p.observed_at)=(h.account_key,h.window_key,h.observed_at))",
    )
    .unwrap();
    conn
}

fn excluded_digests(conn: &rusqlite::Connection) -> BTreeMap<String, DigestRows> {
    EXCLUDED_TABLES
        .iter()
        .map(|(table, _)| {
            let mut query = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            let columns = query.column_count();
            let mut rows = query
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| {
                            Ok(match row.get_ref(index)? {
                                ValueRef::Null => json!({"null":true}),
                                ValueRef::Integer(value) => json!({"integer":value}),
                                ValueRef::Real(value) => json!({"real_bits":value.to_bits()}),
                                ValueRef::Text(value) => json!({"text_bytes":value}),
                                ValueRef::Blob(value) => json!({"blob_bytes":value}),
                            })
                        })
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
            let bytes = serde_json::to_vec(&rows).unwrap();
            (
                (*table).to_string(),
                DigestRows {
                    rows: rows.len(),
                    sha256: format!("{:x}", Sha256::digest(bytes)),
                },
            )
        })
        .collect()
}

// Authorize preparation only on a separate read-only connection. This never
// replaces the live writer's authorizer or native row-capture update hook.
fn write_targets(conn: &rusqlite::Connection, sql: &str) -> Result<BTreeSet<String>, String> {
    let events = Arc::new(Mutex::new(BTreeSet::new()));
    let sink = events.clone();
    conn.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
        use rusqlite::hooks::AuthAction;
        let target = match context.action {
            AuthAction::Insert { table_name }
            | AuthAction::Update { table_name, .. }
            | AuthAction::Delete { table_name } => Some(table_name),
            _ => None,
        };
        if let Some(table) = target {
            sink.lock().unwrap().insert(table.to_string());
        }
        rusqlite::hooks::Authorization::Allow
    }));
    let prepared = conn
        .prepare(sql)
        .map(drop)
        .map_err(|error| error.to_string());
    conn.authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>);
    prepared?;
    let found = events.lock().unwrap().clone();
    Ok(found)
}

pub(super) struct WriteAudit {
    profile: &'static striatum_store::group_reads::StatementProfile,
    path: PathBuf,
    label: String,
    before: BTreeMap<String, DigestRows>,
    frozen: super::frozen_audit::Guard,
}

pub(super) fn begin(harness: &Harness, label: &str) -> WriteAudit {
    let path = harness.store_path().to_path_buf();
    let before = excluded_digests(&read_only(&path));
    let profile = harness
        .store()
        .profile_writer_statements_for_test()
        .unwrap();
    profile.take();
    WriteAudit {
        profile,
        path,
        label: label.to_string(),
        before,
        frozen: super::frozen_audit::begin(harness),
    }
}

impl WriteAudit {
    fn report(&self) -> Value {
        let runs = self.profile.take();
        let side = read_only(&self.path);
        let after = excluded_digests(&side);
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for (sql, _) in &runs {
            *counts.entry(sql.clone()).or_default() += 1;
        }
        let mut writes: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let mut unclassified = Vec::new();
        for (sql, count) in &counts {
            match write_targets(&side, sql) {
                Ok(targets) => {
                    for table in targets {
                        if EXCLUDED_TABLES.iter().any(|(name, _)| *name == table) {
                            writes
                                .entry(table)
                                .or_default()
                                .push(json!({"sql":sql,"executions":count}));
                        }
                    }
                }
                Err(error) => {
                    unclassified.push(json!({"sql":sql,"error":error,"executions":count}))
                }
            }
        }
        let mut tables = Vec::new();
        for (table, reason) in EXCLUDED_TABLES {
            let before = &self.before[*table];
            let after = &after[*table];
            let statements = writes.remove(*table).unwrap_or_default();
            tables.push(json!({"table":table,"reason":reason,"executed_write_statements":statements,
                "net_rows_before":before.rows,"net_rows_after":after.rows,
                "named_column_rowset_sha256_before":before.sha256,"named_column_rowset_sha256_after":after.sha256,
                "net_named_column_rowset_changed":before.sha256!=after.sha256}));
        }
        json!({"case":self.label,"statement_count":runs.len(),"distinct_statements":counts.len(),
            "classification":"completed writer statements; authorizer on a separate read-only prepare includes possible trigger targets, not affected-row or commit counts",
            "net_change_scope":"every named column and raw SQL value type; rowset order canonicalized, implicit rowids not included",
            "live_capture_update_hook_unchanged":true,"live_writer_authorizer_unchanged":true,
            "read_budget_proof":false,"unclassified_statements":unclassified,"excluded_tables":tables,
            "frozen_audit_tables":13,"frozen_audit_frame_checks":self.frozen.frame_checks()})
    }
}

impl Drop for WriteAudit {
    fn drop(&mut self) {
        let report = self.report();
        for table in report["excluded_tables"].as_array().unwrap() {
            if !table["executed_write_statements"]
                .as_array()
                .unwrap()
                .is_empty()
                || table["net_named_column_rowset_changed"] == true
            {
                eprintln!(
                    "replay excluded-table write: case={} table={} reason={} net_changed={}",
                    self.label,
                    table["table"],
                    table["reason"],
                    table["net_named_column_rowset_changed"]
                );
            }
        }
        if let Some(directory) = std::env::var_os("S7B_REPLAY_STATE_AUDIT_DIR") {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap();
            let directory = PathBuf::from(directory);
            assert!(
                directory.starts_with(root),
                "audit output must stay in the worktree"
            );
            std::fs::create_dir_all(&directory).unwrap();
            let label: String = self
                .label
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect();
            std::fs::write(
                directory.join(format!("{label}.json")),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
        }
    }
}

#[test]
fn excluded_write_audit_flags_executed_dml_and_classifies_without_mutating() {
    let harness = Harness::new("replay-excluded-write-plant");
    let side = read_only(&harness.store_path());
    let before = excluded_digests(&side);
    assert!(write_targets(&side, "SELECT * FROM routing_call_window")
        .unwrap()
        .is_empty());
    assert!(write_targets(
        &side,
        "SELECT writes FROM temp.catalog_facts_writes WHERE singleton = 1"
    )
    .unwrap()
    .is_empty());
    let sql = "UPDATE routing_call_window SET started_at_ms=started_at_ms+1";
    assert!(write_targets(&side, sql)
        .unwrap()
        .contains("routing_call_window"));
    assert_eq!(
        excluded_digests(&side),
        before,
        "read-only classification must never execute the write"
    );
    let audit = begin(&harness, "excluded-write-plant");
    let changed = harness
        .store()
        .with_writer_for_test(|conn| conn.execute(sql, []))
        .unwrap();
    assert_eq!(changed, 1);
    let report = audit.report();
    let window = report["excluded_tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["table"] == "routing_call_window")
        .unwrap();
    assert_eq!(window["net_named_column_rowset_changed"], true);
    assert!(window["executed_write_statements"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["sql"] == sql && row["executions"] == 1));
    assert!(
        report["unclassified_statements"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{report}"
    );
    // report() already consumed this test's profile. Avoid a second report;
    // the production capture hook and live authorizer were never replaced.
    std::mem::forget(audit);
}
