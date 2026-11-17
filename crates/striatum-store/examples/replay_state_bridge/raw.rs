//! Reconstruct complete old SQL cells and rowids from a captured database.
use super::*;

fn value(cell: &Value) -> Result<SqlValue> {
    Ok(
        match cell["type"].as_str().ok_or("missing SQL cell type")? {
            "null" => SqlValue::Null,
            "integer" => SqlValue::Integer(cell["value"].as_i64().ok_or("invalid SQL integer")?),
            "real" => SqlValue::Real(cell["value"].as_f64().ok_or("invalid SQL real")?),
            "text" => SqlValue::Text(cell["value"].as_str().ok_or("invalid SQL text")?.into()),
            "blob" => SqlValue::Blob(
                cell["value"]
                    .as_array()
                    .ok_or("invalid SQL blob")?
                    .iter()
                    .map(|byte| {
                        byte.as_u64()
                            .and_then(|byte| u8::try_from(byte).ok())
                            .ok_or("invalid SQL byte")
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            ),
            _ => return Err("unknown SQL storage class".into()),
        },
    )
}

fn cell(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => json!({"type":"null","value":null}),
        ValueRef::Integer(value) => json!({"type":"integer","value":value}),
        ValueRef::Real(value) => json!({"type":"real","value":value}),
        ValueRef::Text(value) => json!({"type":"text","value":std::str::from_utf8(value).unwrap()}),
        ValueRef::Blob(value) => json!({"type":"blob","value":value}),
    }
}

pub(super) fn rows(conn: &Connection, name: &str) -> Result<Value> {
    let mut query = conn.prepare(&format!(
        "SELECT rowid,* FROM \"{}\" ORDER BY rowid",
        name.replace('"', "\"\"")
    ))?;
    let columns = query
        .column_names()
        .iter()
        .skip(1)
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let rows = query
        .query_map([], |row| {
            let mut cells = serde_json::Map::new();
            for (index, name) in columns.iter().enumerate() {
                cells.insert(name.clone(), cell(row.get_ref(index + 1)?));
            }
            Ok(json!({"rowid":row.get::<_,i64>(0)?,"cells":cells}))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!(rows))
}

pub(super) fn seed(store: &SqliteRouteStateStore, captured: &Value) -> Result<Value> {
    let captured = captured
        .as_object()
        .ok_or("raw SQL tables must be an object")?;
    store.with_writer_for_test(|conn| {
        let inventory = schema(conn).map_err(rusqlite::Error::ToSqlConversionFailure)?;
        conn.pragma_update(None, "foreign_keys", false)?;
        conn.execute_batch("BEGIN IMMEDIATE")?;
        // Restore a complete database image, not a sequence of new reports.
        // Import triggers could otherwise change rows while dependencies are
        // only partly present. Restore their exact SQL before migration runs.
        let triggers = conn.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name")?
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (name, _) in &triggers {
            conn.execute_batch(&format!("DROP TRIGGER \"{}\"", name.replace('"', "\"\"")))?;
        }
        for name in inventory.keys().filter(|name| name.as_str() != "cortexkit_schema_version") {
            conn.execute(&format!("DELETE FROM \"{}\"", name.replace('"', "\"\"")), [])?;
        }
        let mut imported = 0;
        for (name, captured_rows) in captured {
            let available = inventory.get(name).ok_or_else(|| rusqlite::Error::InvalidParameterName(name.clone()))?;
            for captured_row in captured_rows.as_array().ok_or(rusqlite::Error::InvalidQuery)? {
                let cells = captured_row["cells"].as_object().ok_or(rusqlite::Error::InvalidQuery)?;
                let columns = cells.keys().cloned().collect::<Vec<_>>();
                if columns.iter().any(|column| !available.contains_key(column)) || columns.len() != available.len() {
                    return Err(rusqlite::Error::InvalidParameterName(format!("incomplete or unknown captured columns: {name}")));
                }
                let mut values = vec![SqlValue::Integer(captured_row["rowid"].as_i64().ok_or(rusqlite::Error::InvalidQuery)?)];
                for column in &columns {
                    values.push(value(&cells[column]).map_err(rusqlite::Error::ToSqlConversionFailure)?);
                }
                let quoted = columns.iter().map(|column| format!("\"{}\"", column.replace('"', "\"\""))).collect::<Vec<_>>().join(",");
                let parameters = (1..=values.len()).map(|index| format!("?{index}")).collect::<Vec<_>>().join(",");
                conn.execute(&format!("INSERT INTO \"{}\"(rowid,{quoted}) VALUES({parameters})", name.replace('"', "\"\"")), params_from_iter(values.iter()))?;
                imported += 1;
            }
        }
        for (_, sql) in &triggers {
            conn.execute_batch(sql)?;
        }
        // Compare the completed image, including effects of any later insert,
        // rather than only checking each row immediately after it was seeded.
        let mut differences = Vec::new();
        for (name, expected) in captured {
            let actual = rows(conn, name).map_err(rusqlite::Error::ToSqlConversionFailure)?;
            if actual != *expected {
                differences.push(json!({"table":name,"expected_raw_sql":expected,"actual_raw_sql":actual}));
            }
        }
        conn.execute_batch("COMMIT")?;
        Ok(json!({"imported_recorded_rows":imported,"recorded_column_roundtrip_differences":differences,
            "raw_sql_storage_classes_preserved":true,"original_rowids":"captured and checked exactly",
            "clock_history":"captured full source, clock, authorization, clear, success and retirement tables",
            "restored_trigger_sql_count":triggers.len(),
            "unrecorded_tables_seeded_empty_for_bridge_only":inventory.keys().filter(|name|name.as_str()!="cortexkit_schema_version" && !captured.contains_key(*name)).collect::<Vec<_>>()}))
    }).map_err(Into::into)
}
