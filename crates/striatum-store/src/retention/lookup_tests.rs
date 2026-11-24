//! Query plans and bounded reads for retention and report statements that
//! must find their rows by range or key rather than walk a whole table.

use super::*;

fn scratch_store(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        label,
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    (scratch, store)
}

fn query_plan(conn: &Connection, sql: &str, values: &[rusqlite::types::Value]) -> Vec<String> {
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(rusqlite::params_from_iter(values), |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

/// Count the virtual-machine steps `run` takes on the writer connection, in
/// units of 100 instructions, through SQLite's progress callback.
fn writer_steps<T>(store: &SqliteRouteStateStore, run: impl FnOnce() -> T) -> (T, u64) {
    let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter = steps.clone();
    store
        .store
        .with_conn(|conn| {
            conn.progress_handler(
                100,
                Some(move || {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                }),
            );
            Ok(())
        })
        .unwrap();
    let result = run();
    store
        .store
        .with_conn(|conn| {
            conn.progress_handler(100, None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
    (result, steps.load(std::sync::atomic::Ordering::Relaxed))
}

/// With 20,000 recent parent-failure copies and nothing due, a retention
/// batch reads a range of the outcome-time index instead of walking every
/// copy, and it still deletes exactly the rows that are due.
#[test]
fn parent_failure_retention_reads_only_due_rows() {
    let (_scratch, store) = scratch_store("parent-failure-prune");
    store
        .store
        .with_conn(|conn| {
            conn.execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000)
                 INSERT INTO model_route_parent_failures
                   (decision_id, parent_session_id, outcome_at, outcome_error)
                 SELECT 'recent-'||i, 'parent-'||(i%500), 200, 'content_policy' FROM n;
                 INSERT INTO model_route_parent_failures
                   (decision_id, parent_session_id, outcome_at, outcome_error)
                 VALUES ('old', 'parent-0', 50, 'content_policy'),
                        ('no-time', 'parent-0', NULL, 'content_policy');
                 ANALYZE;",
            )
        })
        .unwrap();
    let (deleted, steps) = writer_steps(&store, || store.prune_parent_failures(100, 64).unwrap());
    assert_eq!(
        deleted, 2,
        "the old copy and the copy without a time are due"
    );
    let (deleted, idle_steps) =
        writer_steps(&store, || store.prune_parent_failures(100, 64).unwrap());
    assert_eq!(deleted, 0);
    // Walking 20,000 index entries costs well over 400 hundred-instruction
    // steps; a range read that finds nothing due costs a handful.
    assert!(
        steps < 50 && idle_steps < 50,
        "parent-failure retention walked the table: {steps} and {idle_steps} steps"
    );
}

/// A failed report ends its decision's affinity through a keyed lookup.
#[test]
fn ending_a_decision_affinity_seeks_by_decision() {
    let (_scratch, store) = scratch_store("affinity-end-plan");
    store
        .store
        .with_read(|conn| {
            let plan = query_plan(
                conn,
                crate::routing_v1::END_DECISION_AFFINITY_SQL,
                &["decision".to_string().into(), 1.into()],
            );
            assert!(
                plan.iter().any(|line| line
                    .contains("SEARCH routing_v1_affinity USING INDEX routing_v1_affinity_by_decision (decision_id=?)")),
                "{plan:?}"
            );
            assert!(!plan.iter().any(|line| line.starts_with("SCAN")), "{plan:?}");
            Ok(())
        })
        .unwrap();
}

/// Continuation retention plans from a range of the retention-clock index on
/// a reader, and the writer's recheck reads only the planned rows by rowid.
/// Neither materializes every used offer in the store.
#[test]
fn continuation_retention_plans_by_range_and_rechecks_by_key() {
    let (_scratch, store) = scratch_store("continuation-prune-plan");
    store
        .store
        .with_read(|conn| {
            let planned = query_plan(
                conn,
                &continuation_candidates(false),
                &[100.into(), 8.into()],
            );
            assert!(
                planned.iter().any(|line| line
                    .starts_with("SEARCH c USING INDEX account_continue_by_last_time (<expr><?)")),
                "{planned:?}"
            );
            let keyed = query_plan(
                conn,
                &continuation_candidates(true),
                &[100.into(), 8.into(), "[1]".to_string().into()],
            );
            assert!(
                keyed
                    .iter()
                    .any(|line| line.starts_with("SEARCH c USING INTEGER PRIMARY KEY")),
                "{keyed:?}"
            );
            for plan in [&planned, &keyed] {
                assert!(
                    !plan.iter().any(|line| (line.starts_with("SCAN")
                        && !line.starts_with("SCAN json_each"))
                        || line.contains("MATERIALIZE")
                        || line.contains("AUTOMATIC")),
                    "{plan:?}"
                );
            }
            Ok(())
        })
        .unwrap();
}

/// With 20,000 recent used offers and one old unused offer, continuation
/// retention deletes the old offer while the writer does only the keyed
/// recheck: planning runs on a pooled reader.
#[test]
fn continuation_retention_keeps_the_scan_off_the_writer() {
    let (_scratch, store) = scratch_store("continuation-prune-writer");
    let now: i64 = 40 * 86_400_000;
    let recent = now - 86_400_000;
    store
        .store
        .with_conn(|conn| {
            conn.execute_batch(&format!(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000)
                 INSERT INTO account_continue (principal, selection_id, request_json, answer_json,
                   run_id, admission_id, previous_decision_id, decision_id, reservation_id,
                   valid_until_ms, used_at_ms, released_at_ms)
                 SELECT 'p', 'used-'||i, '{{}}', '{{}}', 'run-'||i, 'admission-'||i,
                   'previous-'||i, 'decision-'||i, 'reservation-'||i, {recent}, {recent}, NULL
                 FROM n;
                 INSERT INTO account_continue (principal, selection_id, request_json, answer_json,
                   run_id, admission_id, previous_decision_id, decision_id, reservation_id,
                   valid_until_ms, used_at_ms, released_at_ms)
                 VALUES ('p', 'old', '{{}}', '{{}}', 'run-old', 'admission-old', 'previous-old',
                   'decision-old', 'reservation-old', 1000, NULL, NULL);
                 ANALYZE;"
            ))
        })
        .unwrap();
    let cutoff = now - 7 * 86_400_000;
    let (deleted, steps) = writer_steps(&store, || {
        store.prune_account_continuations(cutoff, 256).unwrap()
    });
    assert_eq!(deleted, 1);
    // Scanning 20,000 offers on the writer costs well over 400
    // hundred-instruction steps; the keyed recheck of one row costs a few.
    assert!(
        steps < 100,
        "continuation retention scanned on the writer: {steps} steps"
    );
}

/// Key-pool retention reads a range of each table's retention-clock index.
#[test]
fn pool_key_retention_reads_index_ranges() {
    let (_scratch, store) = scratch_store("pool-key-prune-plan");
    store
        .store
        .with_read(|conn| {
            for (sql, index) in [
                (POOL_LEASE_PRUNE_SQL, "pool_key_lease_by_end"),
                (POOL_SESSION_PRUNE_SQL, "pool_session_affinity_by_end"),
            ] {
                let plan = query_plan(conn, sql, &[100.into(), 8.into()]);
                assert!(
                    plan.iter()
                        .any(|line| line.contains(&format!("USING INDEX {index} (<expr><?)"))),
                    "{plan:?}"
                );
                assert!(
                    !plan.iter().any(|line| line.starts_with("SCAN")),
                    "{plan:?}"
                );
            }
            Ok(())
        })
        .unwrap();
}
