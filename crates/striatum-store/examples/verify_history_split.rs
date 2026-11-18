//! The offline script runs this verifier on the original and staged copies
//! before it publishes either file. Build with `--features test-support`.

use rusqlite::OptionalExtension;
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
        let before: Option<(String, String, String)> = original.query_row(
            "SELECT target_agent, score_json, rejected_json FROM model_route_decisions WHERE id=?1",
            [&id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        let after = reopened.get_decision_by_id(&id)?;
        if before
            .as_ref()
            .map(|(agent, score, rejected)| (agent, score, rejected))
            != after
                .as_ref()
                .map(|row| (&row.target_agent, &row.score_json, &row.rejected_json))
        {
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
