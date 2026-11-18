//! The offline script runs this verifier on the original and staged copies
//! before it publishes either file. Build with `--features test-support`.

use rusqlite::{Connection, OpenFlags};
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

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
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
    let archive = Connection::open_with_flags(
        history_path(Path::new(staged)),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
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
    for id in hold_decisions {
        if reopened.get_decision_by_id(&id)?.is_some() {
            explained += 1;
        }
    }
    println!(
        "restart: ok, {:.3}ms; explained {explained} decisions referenced by moved holds",
        start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}
