#![cfg(test)]

use super::*;
use std::collections::BTreeSet;

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
        .map(|name| (*name).to_string())
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
    let mut tables = serde_json::Map::new();
    for name in INCLUDED_TABLES {
        let mut statement = connection
            .prepare(&format!("SELECT * FROM \"{name}\""))
            .unwrap();
        let columns: Vec<String> = statement
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut rows = statement
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
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
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
