//! Test-support audit of the columns listed in scripts/frozen-block-names.txt.

use rusqlite::{types::Value, Connection};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenBlockData {
    rows: BTreeMap<(String, String), Vec<Value>>,
    new_row_values: BTreeMap<String, Vec<Value>>,
}

impl FrozenBlockData {
    /// Surviving rows keep their migration values; new rows use only the
    /// catalogue's NULL/zero defaults. Removed telemetry rows need not survive.
    pub fn assert_unchanged(&self, before: &Self) {
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
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/frozen-block-names.txt");
    let catalogue = std::fs::read_to_string(path)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut columns: BTreeMap<String, Vec<(String, Value)>> = BTreeMap::new();
    for line in catalogue.lines() {
        let fields = line
            .split('#')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>();
        match fields.as_slice() {
            [] => {}
            ["table" | "trigger", name] => {
                let exists: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                    [name],
                    |row| row.get(0),
                )?;
                assert!(!exists, "frozen schema object still exists: {name}");
            }
            ["column", qualified, default] => {
                let (table, column) = qualified
                    .split_once('.')
                    .ok_or(rusqlite::Error::InvalidQuery)?;
                if ![table, column]
                    .into_iter()
                    .all(|name| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
                {
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
    let mut data = FrozenBlockData {
        rows: BTreeMap::new(),
        new_row_values: BTreeMap::new(),
    };
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
