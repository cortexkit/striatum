//! The offline script runs this verifier on the original and staged copies
//! before it publishes either file. Build with `--features test-support`.

use rusqlite::{types::Value, Connection, OptionalExtension};

const FIXTURE_NOW: i64 = 1_800_000_000_000;
use std::error::Error;
use std::path::Path;
use striatum_store::{
    dev_descriptor, open_offline_split_copy, verify_split_evaluation, SqliteRouteStateStore,
};

fn history_path(state: &Path) -> std::path::PathBuf {
    let name = state.file_name().unwrap().to_str().unwrap();
    if matches!(name, "state.db" | "store.db") {
        state.with_file_name("history.db")
    } else {
        state.with_file_name(format!("{name}.history.db"))
    }
}

fn account_decision(conn: &Connection, id: &str) -> rusqlite::Result<Option<Vec<Value>>> {
    let mut query = conn.prepare("SELECT * FROM account_decision WHERE decision_id=?1")?;
    let width = query.column_count();
    query
        .query_row([id], |row| (0..width).map(|index| row.get(index)).collect())
        .optional()
}

fn seed_fixture(path: &str) -> Result<(), Box<dyn Error>> {
    let descriptor = dev_descriptor("split-verifier-fixture", path);
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95)?;
    old.seed_block_parity_fixture_for_test(FIXTURE_NOW);
    drop(old);
    let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97)?;
    legacy.with_writer_for_test(|conn| conn.execute_batch(
        "INSERT INTO account_decision (decision_id,principal,mode,trigger,provider,model,family,outcome,code,policy_version,decision_json,created_at)
          VALUES ('test-hold','principal','serve','request','provider','model','family','hold','account_hold',1,'{}',20);
         INSERT INTO account_hold VALUES ('test-hold','principal','conversation','provider','pin',1,'acct','cred','quota',100,60,20);
         INSERT INTO quota_reading_log VALUES ('acct','window',10,4,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL),
           ('acct','window',11,5,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL);"
    ))?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if let [flag, path] = args.as_slice() {
        if flag == "--seed-test-fixture" {
            return seed_fixture(path);
        }
    }
    let [original, staged, now] = args.as_slice() else {
        return Err("usage: verify_history_split ORIGINAL_DB STAGED_DB NOW_MS".into());
    };
    let original = open_offline_split_copy(Path::new(original))?;
    let candidate = open_offline_split_copy(Path::new(staged))?;
    let parity = verify_split_evaluation(&original, &candidate, now.parse()?)?;
    println!(
        "evaluation parity: {} comparisons over {} identities; {} mismatches",
        parity.function_comparisons,
        parity.identities,
        parity.differences.len()
    );
    if !parity.differences.is_empty() {
        return Err(parity.differences.join("\n").into());
    }
    let archive = open_offline_split_copy(&history_path(Path::new(staged)))?;
    let plan: String = archive.query_row(
        "EXPLAIN QUERY PLAN SELECT * FROM account_hold WHERE decision_id=?1",
        ["split-verification"],
        |row| row.get(3),
    )?;
    println!("history explain: {plan}");
    let mut query = archive.prepare("SELECT decision_id FROM account_hold ORDER BY decision_id")?;
    let hold_decisions = query
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(query);
    drop(archive);
    drop(candidate);

    let descriptor = dev_descriptor("split-verification", staged);
    let store = SqliteRouteStateStore::open(&descriptor)?;
    drop(store);
    let start = std::time::Instant::now();
    let reopened = SqliteRouteStateStore::open(&descriptor)?;
    let mut explained = 0;
    let mut without_decision = 0;
    for id in hold_decisions {
        let before = account_decision(&original, &id)?;
        let after = reopened.with_reader_for_test(|conn| account_decision(conn, &id))?;
        if before != after {
            return Err(format!("decision explain changed for moved hold {id}").into());
        }
        if after.is_some() {
            explained += 1;
        } else {
            without_decision += 1;
        }
    }
    println!(
        "restart: ok, {:.3}ms; explained {explained} decisions referenced by moved holds ({without_decision} previously absent)",
        start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}
