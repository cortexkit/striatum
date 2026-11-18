#![cfg(test)]

use super::*;
use std::collections::BTreeSet;

#[path = "frozen_audit/tests.rs"]
mod frozen_audit;
#[path = "write_audit/tests.rs"]
mod write_audit;

pub(super) fn audit_writes(harness: &Harness, label: &str) -> impl Drop {
    write_audit::begin(harness, label)
}

// Request-mutated state plus pre-migration refusal/probe tables, such as
// account_fence and fence_recheck, now kept only as frozen evidence. Every new
// table needs explicit classification rather than a match on its spelling.
pub(super) const INCLUDED_TABLES: &[&str] = &[
    "account_admission",
    "account_affinity",
    "account_continue",
    "account_decision",
    "account_eligibility",
    "account_fence",
    "account_hold",
    "account_non_quota_route_fence",
    "account_non_quota_size_refusal",
    "account_pin",
    "account_principal_exclusion",
    "account_route_fence",
    "account_size_refusal",
    "account_transient_backoff",
    "block",
    "credential_entitlement_fences",
    "fence_probe_authorization",
    "fence_recheck",
    "fence_recheck_route_clear",
    "fence_retired_origin",
    "fence_success_pool",
    "model_route_decision_detail_cursor",
    "model_route_decisions",
    "model_route_state",
    "model_route_usage",
    "pool_key_lease",
    "pool_key_state",
    "pool_session_affinity",
    "probe_authorization",
    "provider_cooldown_state",
    "retirement_tombstone",
    "routing_operator_quality_state",
    "routing_v1_admission",
    "routing_v1_affinity",
    "routing_v1_decision",
    "routing_v1_report",
    "routing_v1_report_delivery",
    "success_proof",
];

// Every omission has an explicit input/history/diagnostic classification.
// The schema union still rejects any newly added, unclassified table.
pub(super) const EXCLUDED_TABLES: &[(&str, &str)] = &[
    (
        "account_observation",
        "history: observation bodies and binding/effect metadata; live refusals are in block",
    ),
    (
        "account_observation_event",
        "history: observation event replay identities",
    ),
    (
        "account_quota_completeness",
        "bootstrap input: provider poll-completeness facts",
    ),
    (
        "account_quota_reading",
        "bootstrap input: latest externally polled account quotas",
    ),
    (
        "account_selection_salt",
        "bootstrap input: per-store account-selection identity salt",
    ),
    (
        "account_vault_roster",
        "bootstrap input: credential/account roster",
    ),
    (
        "cortexkit_schema_version",
        "bootstrap input: migration ledger, not a request effect",
    ),
    (
        "fusiform_catalog_cursor",
        "bootstrap input: catalog synchronization cursor",
    ),
    (
        "model_facts",
        "bootstrap input: catalog facts supplied to the replay",
    ),
    (
        "model_route_parent_failures",
        "history: failure index derived from decision history",
    ),
    (
        "model_route_speed_samples",
        "history: observed speed samples",
    ),
    (
        "quota_reading_log",
        "history: append-only external quota readings",
    ),
    ("quota_snapshot", "bootstrap input: external quota snapshot"),
    (
        "route_winner_baseline",
        "diagnostic: winner comparison baseline",
    ),
    ("routing_call_count", "diagnostic: batched RPC call counts"),
    (
        "routing_call_window",
        "diagnostic: call-count aggregation window",
    ),
    (
        "routing_engine_eval",
        "diagnostic: engine evaluation observations",
    ),
    (
        "routing_engine_speed",
        "diagnostic: engine speed observations",
    ),
    (
        "routing_operator_decommissioned",
        "bootstrap input: operator route retirement policy",
    ),
    (
        "routing_operator_quality",
        "bootstrap input: operator model grades",
    ),
    (
        "routing_unscored_admissible_models",
        "diagnostic: ungraded model sightings",
    ),
    (
        "routing_v1_panel",
        "history: persisted panel selection/answer ledger",
    ),
];

// These tables implement placement or replay of data rather than add a new
// logical relation to the existing 096 replay image.
const SPLIT_STATE_TABLES: &[&str] = &[
    "pending_account_hold",
    "pending_quota_reading_log",
    "quota_reading_moved",
];

pub(super) fn check_tables(conn: &rusqlite::Connection) -> Result<(), String> {
    let included: BTreeSet<_> = INCLUDED_TABLES.iter().copied().collect();
    let excluded: BTreeSet<_> = EXCLUDED_TABLES.iter().map(|(name, _)| *name).collect();
    if included.len() != INCLUDED_TABLES.len()
        || excluded.len() != EXCLUDED_TABLES.len()
        || !included.is_disjoint(&excluded)
    {
        return Err("replay state classifications must be unique and disjoint".into());
    }
    let expected: BTreeSet<String> = included
        .union(&excluded)
        .copied()
        .chain(SPLIT_STATE_TABLES.iter().copied())
        .map(|name| name.to_string())
        .collect();
    let mut query = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'")
        .map_err(|error| error.to_string())?;
    let actual = query
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .map_err(|error| error.to_string())?;
    if actual != expected {
        return Err(format!(
            "replay tables need explicit classification: unknown={:?}, missing={:?}",
            actual.difference(&expected).collect::<Vec<_>>(),
            expected.difference(&actual).collect::<Vec<_>>()
        ));
    }
    Ok(())
}

pub(super) fn snapshot(harness: &Harness) -> Value {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    check_tables(&connection).unwrap();
    frozen_audit::check(harness, &connection);
    let mut tables = serde_json::Map::new();
    for name in INCLUDED_TABLES {
        let source = if *name == "account_hold" {
            "visible_account_holds"
        } else {
            name
        };
        let collect = |connection: &rusqlite::Connection| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM \"{source}\""))
                .unwrap();
            let columns: Vec<String> = statement
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let rows = statement
                .query_map([], |row| {
                    let mut object = serde_json::Map::new();
                    for (index, column) in columns.iter().enumerate() {
                        let value = match row.get_ref(index)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(value) => json!(value),
                            ValueRef::Real(value) => json!(value),
                            ValueRef::Text(value) => {
                                let text = std::str::from_utf8(value).unwrap();
                                serde_json::from_str(text).unwrap_or_else(|_| json!(text))
                            }
                            ValueRef::Blob(value) => json!(value),
                        };
                        object.insert(column.clone(), value);
                    }
                    Ok(Value::Object(object))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        };
        let mut rows = if *name == "account_hold" {
            harness
                .store()
                .with_history_reader_for_test(collect)
                .unwrap()
        } else {
            collect(&connection).unwrap()
        };
        // Keep the old neutral catalogue-row omission exactly: non-neutral
        // stored health fields are never removed from the comparison.
        if *name == "model_route_state" {
            rows.retain(|row| {
                !row.as_object()
                    .unwrap()
                    .iter()
                    .all(|(field, value)| match field.as_str() {
                        "route_key" | "capacity_key" | "provider_id" | "model_id" | "variant" => {
                            true
                        }
                        "updated_at"
                        | "consecutive_terminal_failures"
                        | "failure_count"
                        | "quota_error_count"
                        | "wall_unscoped" => value == 0,
                        "wall_observed_at" | "failure_observed_at" => value.is_null() || value == 0,
                        _ => value.is_null(),
                    })
            });
        }
        rows.sort_by_key(Value::to_string);
        tables.insert((*name).to_string(), json!(rows));
    }
    Value::Object(tables)
}

#[test]
fn replay_state_schema_requires_every_table_to_be_explicitly_classified() {
    let harness = Harness::new("replay-state-table-classification");
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    check_tables(&conn).unwrap();
    assert_eq!((INCLUDED_TABLES.len(), EXCLUDED_TABLES.len()), (38, 22));
    let captured = snapshot(&harness);
    assert_eq!(
        captured
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        INCLUDED_TABLES.iter().copied().collect()
    );
    for table in [
        "block",
        "probe_authorization",
        "success_proof",
        "retirement_tombstone",
    ] {
        assert!(
            captured[table].is_array(),
            "native state missing from replay: {table}"
        );
    }
    for table in [
        "new_state_plant",
        "unclassified_table_without_state_spelling",
    ] {
        conn.execute_batch(&format!("CREATE TABLE {table}(id INTEGER)"))
            .unwrap();
        let error = check_tables(&conn).unwrap_err();
        assert!(error.contains(table), "{error}");
        conn.execute_batch(&format!("DROP TABLE {table}")).unwrap();
    }
    check_tables(&conn).unwrap();
}

/// Preserve complete physical state while the historical wire-shape comparison
/// projects additive response fields. Native expectations come from independent
/// pinned-main SQL frames migrated through the real block migration.
pub(super) struct ReplayStates {
    observed: BTreeMap<String, Value>,
    expected: BTreeMap<String, Value>,
}

fn is_state_frame(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.contains_key("model_route_state") && object.contains_key("routing_v1_decision")
    })
}

fn collect_state_frames(value: &Value, path: &str, frames: &mut BTreeMap<String, Value>) {
    if is_state_frame(value) {
        assert!(frames.insert(path.to_string(), value.clone()).is_none());
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                collect_state_frames(value, &format!("{path}.{key}"), frames);
            }
        }
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                collect_state_frames(value, &format!("{path}[{index}]"), frames);
            }
        }
        _ => {}
    }
}

pub(super) fn capture_for_comparison(file: &str, observed: &Value) -> Option<ReplayStates> {
    let count = match file {
        "report-outcomes" => 17,
        "availability" => 78,
        "tier-consumers" => 4,
        _ => return None,
    };
    assert!(std::env::var_os("PREFRONTAL_RECORD_ROUTING_REPLAY").is_none(),
        "historical wire recordings stay frozen; derive native state from the independent main reference");
    let path = Path::new(ROOT).join(format!("{file}.native-096-state.golden.json"));
    let golden: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(golden["schema_version"], 96);
    assert_eq!(
        golden["reference_commit"],
        "445f292b6285a635d041f0f23fe0c9d81d0fe18b"
    );
    let expected = golden["frames"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(path, state)| (path.clone(), state.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut captured = BTreeMap::new();
    collect_state_frames(observed, "$", &mut captured);
    assert_eq!(captured.len(), count);
    assert_eq!(
        captured.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    let included = INCLUDED_TABLES.iter().copied().collect::<BTreeSet<_>>();
    for state in expected.values().chain(captured.values()) {
        assert_eq!(
            state
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            included
        );
    }
    Some(ReplayStates {
        observed: captured,
        expected,
    })
}

fn restore_state_frames(
    value: &mut Value,
    path: &str,
    frames: &BTreeMap<String, Value>,
    seen: &mut BTreeSet<String>,
) {
    if is_state_frame(value) {
        *value = frames.get(path).unwrap().clone();
        assert!(seen.insert(path.to_string()));
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                restore_state_frames(value, &format!("{path}.{key}"), frames, seen);
            }
        }
        Value::Array(array) => {
            for (index, value) in array.iter_mut().enumerate() {
                restore_state_frames(value, &format!("{path}[{index}]"), frames, seen);
            }
        }
        _ => {}
    }
}

impl ReplayStates {
    pub(super) fn restore_after_wire_projection(self, observed: &mut Value, expected: &mut Value) {
        for (document, frames) in [(observed, &self.observed), (expected, &self.expected)] {
            let mut seen = BTreeSet::new();
            restore_state_frames(document, "$", frames, &mut seen);
            assert_eq!(seen, frames.keys().cloned().collect::<BTreeSet<_>>());
        }
    }
}

#[tokio::test]
async fn route_order_lookup_never_runs_during_select_or_account_check() {
    use striatum_core::RouteStateStore;
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _inputs = striatum_store::replay::Inputs::new(NOW).install();
    let (harness, caller) = configured().await;
    let trace = harness.store().trace_block_reads_for_test().unwrap();
    harness
        .store()
        .update_route_state(
            "bookkeeping-unrelated/model",
            "bookkeeping-unrelated/model",
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 60_000) as f64),
                observed_at_ms: Some(NOW),
                last_error: Some("route capacity".into()),
                unscoped_wall: true,
                ..Default::default()
            },
        )
        .unwrap();
    let lookup = striatum_store::group_reads::REVIEWED_WRITER_BOOKKEEPING
        .iter()
        .map(|(sql, _)| *sql)
        .find(|sql| sql.contains("FROM model_route_state"))
        .unwrap();
    assert!(trace
        .take()
        .writer_bookkeeping
        .iter()
        .any(|sql| sql == lookup));
    let verify_request = |label: &str| {
        let reads = trace.take();
        for sql in reads
            .reader
            .iter()
            .chain(&reads.writer)
            .chain(&reads.writer_bookkeeping)
            .chain(&reads.checks)
            .chain(&reads.route_writes)
        {
            assert_ne!(
                sql, lookup,
                "{label} must not run the writer's route-order lookup"
            );
        }
    };
    let (_, selected) = caller
        .call(
            "route.select",
            &json!({"consumer":fixture::SUMMARY,
        "requirements":{"iq":50},"pin":"example-subscription/model-c"}),
        )
        .await;
    assert!(selected["result"]["decision_id"].is_string(), "{selected}");
    verify_request("route.select");
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let (_, account) = runner
        .call(
            "route.account_for",
            &json!({"provider":"example-subscription","model":"model-c",
        "conversation_key":"route-order-check","admission_id":"route-order-check",
        "decision_id":selected["result"]["decision_id"]}),
        )
        .await;
    assert!(
        account["result"]["account"]["credential_id"].is_string(),
        "{account}"
    );
    verify_request("route.account_for");
}
