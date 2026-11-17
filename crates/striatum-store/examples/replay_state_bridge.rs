//! Reconstruct recorded replay rows in schema 095, then apply registered 096.
//! Missing clock history and original rowids stay declared unknown; this tool
//! produces derivation evidence, never accepted expectations from live output.
use rusqlite::{
    params_from_iter,
    types::{Value as SqlValue, ValueRef},
    Connection,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};
use striatum_store::SqliteRouteStateStore;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn sql_value(value: &Value, declared: &str) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                SqlValue::Integer(integer)
            } else {
                SqlValue::Real(value.as_f64().ok_or("unrepresentable SQL number")?)
            }
        }
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(values) if declared.to_ascii_uppercase().contains("BLOB") => SqlValue::Blob(
            values
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or("non-byte blob value")
                })
                .collect::<std::result::Result<Vec<_>, _>>()?,
        ),
        _ => SqlValue::Text(serde_json::to_string(value)?),
    })
}

fn json_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => json!(value),
        ValueRef::Real(value) => json!(value),
        ValueRef::Text(value) => {
            let text = std::str::from_utf8(value).unwrap();
            serde_json::from_str(text).unwrap_or_else(|_| json!(text))
        }
        ValueRef::Blob(value) => json!(value),
    }
}

fn table(conn: &Connection, name: &str) -> Result<Value> {
    let mut query = conn.prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\"")))?;
    let columns = query
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let mut rows = query
        .query_map([], |row| {
            let mut object = serde_json::Map::new();
            for (index, name) in columns.iter().enumerate() {
                object.insert(name.clone(), json_value(row.get_ref(index)?));
            }
            Ok(Value::Object(object))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.sort_by_key(Value::to_string);
    Ok(json!(rows))
}

fn schema(conn: &Connection) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut result = BTreeMap::new();
    let names=conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name")?
        .query_map([],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for name in names {
        let columns = conn
            .prepare("SELECT name,type FROM pragma_table_xinfo(?1) WHERE hidden=0 ORDER BY cid")?
            .query_map([&name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
        result.insert(name, columns);
    }
    Ok(result)
}

fn seed(store: &SqliteRouteStateStore, tables: &Value) -> Result<Value> {
    let tables = tables
        .as_object()
        .ok_or("old frame tables must be an object")?;
    let inventory = store.with_reader_for_test(|conn| {
        schema(conn).map_err(|error| rusqlite::Error::ToSqlConversionFailure(error))
    })?;
    let mut imported = 0;
    let mut old_roundtrip_differences = Vec::new();
    store.with_writer_for_test(|conn| {
        conn.pragma_update(None,"foreign_keys",false)?;
        conn.execute_batch("BEGIN IMMEDIATE")?;
        // Clear every data table before inserting recorded rows; keep only
        // the migration ledger. Bundled catalogue data or another frame must
        // not accidentally supply a refusal to this reconstruction.
        for name in inventory.keys() {
            if name!="cortexkit_schema_version" {
                conn.execute(&format!("DELETE FROM \"{}\"",name.replace('"',"\"\"")),[])?;
            }
        }
        for (name,rows) in tables {
            let available=inventory.get(name).ok_or_else(||rusqlite::Error::InvalidParameterName(format!("unknown old table {name}")))?;
            for (index,row) in rows.as_array().ok_or(rusqlite::Error::InvalidQuery)?.iter().enumerate() {
                let row=row.as_object().ok_or(rusqlite::Error::InvalidQuery)?;
                let columns=row.keys().cloned().collect::<Vec<_>>();
                for column in &columns {
                    if !available.contains_key(column){return Err(rusqlite::Error::InvalidParameterName(format!("old table {name} column {column} is absent from095")));}
                }
                let values=columns.iter().map(|column|sql_value(&row[column],&available[column])
                    .map_err(rusqlite::Error::ToSqlConversionFailure)).collect::<rusqlite::Result<Vec<_>>>()?;
                let params=(1..=columns.len()).map(|index|format!("?{index}")).collect::<Vec<_>>().join(",");
                let quoted=columns.iter().map(|name|format!("\"{}\"",name.replace('"',"\"\""))).collect::<Vec<_>>().join(",");
                conn.execute(&format!("INSERT INTO \"{}\"({quoted}) VALUES({params})",name.replace('"',"\"\"")),params_from_iter(values.iter()))?;
                let inserted=conn.last_insert_rowid();
                let mut query=conn.prepare(&format!("SELECT {quoted} FROM \"{}\" WHERE rowid=?1",name.replace('"',"\"\"")))?;
                let reconstructed=query.query_row([inserted],|actual| {
                    let mut object=serde_json::Map::new();
                    for (column,name) in columns.iter().enumerate(){object.insert(name.clone(),json_value(actual.get_ref(column)?));}
                    Ok(Value::Object(object))
                })?;
                if reconstructed!=Value::Object(row.clone()) {
                    old_roundtrip_differences.push(json!({"table":name,"row":index,"recorded":row,"reconstructed":reconstructed}));
                }
                imported+=1;
            }
        }
        conn.execute_batch("COMMIT")?;
        Ok(())
    })?;
    Ok(
        json!({"imported_recorded_rows":imported,"recorded_column_roundtrip_differences":old_roundtrip_differences,
        "unrecorded_tables_seeded_empty_for_bridge_only":inventory.keys().filter(|name|name.as_str()!="cortexkit_schema_version" && !tables.contains_key(*name)).collect::<Vec<_>>(),
        "original_rowids":"not recorded; deterministic insertion order is not proof of original source_order",
        "clock_history":"unrecorded; bridge defaults are not an expected claim/generation/classification/proof assertion"}),
    )
}

fn run_frame(frame: &Value, work: &Path) -> Result<Value> {
    let descriptor = striatum_store::dev_descriptor(
        "replay-state-bridge",
        work.join("store.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95)?;
    let provenance = seed(&old, &frame["tables"])?;
    if !provenance["recorded_column_roundtrip_differences"]
        .as_array()
        .ok_or("missing roundtrip results")?
        .is_empty()
    {
        return Ok(
            json!({"group":frame["group"],"path":frame["path"],"status":"old recorded columns did not roundtrip; no derived expectation accepted","provenance":provenance}),
        );
    }
    drop(old);
    let native = SqliteRouteStateStore::open(&descriptor)?;
    let tables = native.with_reader_for_test(|conn| {
        let inventory = schema(conn).map_err(rusqlite::Error::ToSqlConversionFailure)?;
        let mut result = serde_json::Map::new();
        for name in inventory.keys() {
            result.insert(
                name.clone(),
                table(conn, name).map_err(rusqlite::Error::ToSqlConversionFailure)?,
            );
        }
        Ok(Value::Object(result))
    })?;
    Ok(
        json!({"group":frame["group"],"path":frame["path"],"status":"actual registered095to096 bridge; provenance limits apply",
        "provenance":provenance,"tables":tables,"clock_overridden":false,"accepted_as_expectation":false}),
    )
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: replay_state_bridge OLD_FRAMES_JSON OUTPUT_JSON".into());
    }
    let input: Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let scratch = cortexkit_test_support::ScratchDir::new("replay-state-bridge");
    let mut frames = Vec::new();
    for (index, frame) in input["frames"]
        .as_array()
        .ok_or("missing frames")?
        .iter()
        .enumerate()
    {
        let directory = scratch.path().join(format!("frame-{index}"));
        std::fs::create_dir_all(&directory)?;
        let result = match run_frame(frame, &directory) {
            Ok(result) => result,
            Err(error) => {
                json!({"group":frame["group"],"path":frame["path"],"status":"unrepresentable old frame; no expectation accepted","error":error.to_string()})
            }
        };
        frames.push(result);
    }
    let mut statuses = BTreeMap::<String, usize>::new();
    for frame in &frames {
        *statuses
            .entry(frame["status"].as_str().unwrap().to_string())
            .or_default() += 1;
    }
    let output = json!({"source_sha256":input["historical_source_sha256"],"frames":frames,"statuses":statuses,"actual_output_used_as_expected":false,"time_or_id_override":false});
    std::fs::write(&args[1], serde_json::to_vec_pretty(&output)?)?;
    println!("replay-state bridge: {} recorded frames processed; statuses={statuses:?}; every error retained; no expectation accepted",frames.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorded_source_columns_roundtrip_before_real_registered_migration() {
        let scratch = cortexkit_test_support::ScratchDir::new("replay-bridge-known-source");
        let at = 1_900_000_000_000i64;
        let frame = json!({"group":"synthetic","path":"$.state","tables":{
            "model_route_state":[{"route_key":"bridge-provider/model","capacity_key":"bridge-provider/model",
                "provider_id":"bridge-provider","model_id":"model","updated_at":at,
                "cooldown_until":(at+60000)as f64,"wall_observed_at":at}],
            "account_route_fence":[{"provider":"bridge-provider","credential_id":"credential","model":"model",
                "until_ms":at+90000,"decision_id":"decision","recorded_at":at,"last_reported_at":at,"wall_class":"quota"}]
        }});
        let result = run_frame(&frame, scratch.path()).unwrap();
        assert_eq!(
            result["status"], "actual registered095to096 bridge; provenance limits apply",
            "{result}"
        );
        assert!(
            result["provenance"]["recorded_column_roundtrip_differences"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let blocks = result["tables"]["block"].as_array().unwrap();
        let route = blocks.iter().find(|row| row["kind"] == "route").unwrap();
        assert_eq!(route["cooldown_until"], at + 60000);
        assert_eq!(route["observed_at"], at);
        let account = blocks
            .iter()
            .find(|row| row["kind"] == "account_route")
            .unwrap();
        assert_eq!(account["deadline"], at + 90000);
        assert_eq!(
            account["source_metadata_json"],
            json!({"decision_id":"decision"})
        );
        assert_eq!(result["accepted_as_expectation"], false);
        assert_eq!(result["clock_overridden"], false);
    }
}
