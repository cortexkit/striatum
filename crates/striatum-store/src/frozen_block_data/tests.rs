//! Test-support audit of the retained relations and columns in the frozen catalogue.

use rusqlite::{types::Value, Connection, OptionalExtension};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenBlockData {
    legacy_rows: BTreeMap<String, Vec<Vec<Value>>>,
    legacy_schema: BTreeMap<String, Vec<Value>>,
    rows: BTreeMap<(String, String), Vec<Value>>,
    new_row_values: BTreeMap<String, Vec<Value>>,
}

impl FrozenBlockData {
    /// Capture the legacy audit before the block migration. Its inheritance
    /// trigger must still exist here; the normal post-migration audit requires
    /// that one trigger to be gone and every retained object to be identical.
    pub fn before_migration(conn: &Connection) -> rusqlite::Result<Self> {
        read_at_boundary(conn, false)
    }

    /// Legacy relations and their schema stay identical. Surviving telemetry
    /// rows keep their frozen values; new rows use only the catalogue's NULL/zero
    /// defaults. Removed telemetry rows need not survive.
    pub fn assert_unchanged(&self, before: &Self) {
        assert_eq!(
            self.legacy_schema, before.legacy_schema,
            "frozen legacy schema changed"
        );
        assert_eq!(
            self.legacy_rows, before.legacy_rows,
            "frozen legacy rows changed"
        );
        assert_eq!(
            self.new_row_values, before.new_row_values,
            "frozen-column catalogue changed during the test"
        );
        for (key, values) in &self.rows {
            if let Some(prior) = before.rows.get(key) {
                assert_eq!(values, prior, "frozen columns changed on {key:?}");
            } else {
                assert_eq!(
                    values, &self.new_row_values[&key.0],
                    "a new row has nondefault frozen columns: {key:?}"
                );
            }
        }
    }
}

impl crate::SqliteRouteStateStore {
    pub fn frozen_block_data_for_test(&self) -> striatum_core::RouteStateResult<FrozenBlockData> {
        self.store.with_read(read).map_err(crate::err)
    }
}

fn read(conn: &Connection) -> rusqlite::Result<FrozenBlockData> {
    read_at_boundary(conn, true)
}

fn read_at_boundary(conn: &Connection, migrated: bool) -> rusqlite::Result<FrozenBlockData> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/frozen-block-names.txt");
    let catalogue = std::fs::read_to_string(path)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut columns: BTreeMap<String, Vec<(String, Value)>> = BTreeMap::new();
    let mut data = FrozenBlockData {
        legacy_rows: BTreeMap::new(),
        legacy_schema: BTreeMap::new(),
        rows: BTreeMap::new(),
        new_row_values: BTreeMap::new(),
    };
    for line in catalogue.lines() {
        let fields = line
            .split('#')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>();
        match fields.as_slice() {
            [] => {}
            [kind, name] if matches!(*kind, "table" | "view" | "trigger" | "removed_trigger") => {
                if !identifier(name) {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let actual: Option<String> = conn
                    .query_row(
                        "SELECT type FROM sqlite_schema WHERE name=?1",
                        [name],
                        |row| row.get(0),
                    )
                    .optional()?;
                if *kind == "removed_trigger" {
                    assert_eq!(
                        actual.as_deref(),
                        (!migrated).then_some("trigger"),
                        "inheritance trigger has the wrong migration-boundary state: {name}"
                    );
                    continue;
                }
                assert_eq!(
                    actual.as_deref(),
                    Some(*kind),
                    "frozen audit object missing or replaced: {name}"
                );
                let mut schema=conn.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=?1 OR tbl_name=?1 ORDER BY name")?;
                for row in schema.query_map([name], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        (0..4)
                            .map(|column| row.get(column))
                            .collect::<rusqlite::Result<Vec<Value>>>()?,
                    ))
                })? {
                    let (name, values) = row?;
                    data.legacy_schema.insert(name, values);
                }
                if *kind != "trigger" {
                    let count = conn
                        .prepare(&format!("SELECT * FROM {name}"))?
                        .column_count();
                    let (prefix, order) = if *kind == "table" {
                        ("rowid,", "rowid".into())
                    } else {
                        (
                            "",
                            (1..=count)
                                .map(|column| column.to_string())
                                .collect::<Vec<_>>()
                                .join(","),
                        )
                    };
                    let mut query =
                        conn.prepare(&format!("SELECT {prefix}* FROM {name} ORDER BY {order}"))?;
                    let width = query.column_count();
                    let rows = query
                        .query_map([], |row| {
                            (0..width)
                                .map(|column| row.get(column))
                                .collect::<rusqlite::Result<Vec<Value>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    data.legacy_rows.insert((*name).into(), rows);
                }
            }
            ["column", qualified, default] => {
                let (table, column) = qualified
                    .split_once('.')
                    .ok_or(rusqlite::Error::InvalidQuery)?;
                if ![table, column].into_iter().all(identifier) {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let default = match *default {
                    "null" => Value::Null,
                    "0" => Value::Integer(0),
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                columns
                    .entry(table.into())
                    .or_default()
                    .push((column.into(), default));
            }
            _ => return Err(rusqlite::Error::InvalidQuery),
        }
    }
    for (table, columns) in columns {
        let identity = match table.as_str() {
            "model_route_state" => "json_array(route_key)",
            "provider_cooldown_state" => "json_array(provider_key)",
            "pool_key_state" => "json_array(provider,credential_id)",
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        let names = columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let mut query = conn.prepare(&format!(
            "SELECT {identity},{names} FROM {table} ORDER BY {identity}"
        ))?;
        for row in query.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (1..=columns.len())
                    .map(|index| row.get(index))
                    .collect::<rusqlite::Result<Vec<Value>>>()?,
            ))
        })? {
            let (key, values) = row?;
            data.rows.insert((table.clone(), key), values);
        }
        data.new_row_values
            .insert(table, columns.into_iter().map(|(_, value)| value).collect());
    }
    Ok(data)
}

fn identifier(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_keeps_complete_frozen_rows_and_schema() {
        let scratch = cortexkit_test_support::ScratchDir::new("frozen-migration-audit");
        let store = crate::SqliteRouteStateStore::open_at_version_for_test(
            &crate::dev_descriptor(
                "frozen-migration-audit",
                scratch.path().join("state.db").to_string_lossy(),
            ),
            93,
        )
        .unwrap();
        let now = crate::now_ms();
        store.seed_block_parity_fixture_for_test(now);
        store.assert_block_parity_fixture_coverage_for_test(now);
        store.with_writer_for_test(|conn| {
            conn.execute("INSERT INTO fence_probe_authorization SELECT claim_owner,kind,origin_key,evidence,evidence_at,last_claimed_at,claim_generation FROM fence_recheck WHERE claim_owner IS NOT NULL",[])?;
            let before=FrozenBlockData::before_migration(conn)?;
            assert_eq!(before.legacy_rows.len(),14,"all thirteen legacy tables and their view are audited");
            assert!(before.legacy_rows.iter().all(|(_,rows)|!rows.is_empty()),"the shared parity fixture must populate every frozen relation");
            assert!(before.legacy_schema.contains_key("fence_retire_enforcement"));
            crate::block_storage::install_migration_function(conn)?;
            conn.execute_batch(crate::block_storage::MIGRATION)?;
            let after=read(conn)?;
            after.assert_unchanged(&before);
            Ok(())
        }).unwrap();
    }
}
