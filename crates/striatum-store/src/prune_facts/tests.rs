use super::*;
use crate::connection_store::StoreStageTiming;
use crate::{dev_descriptor, RouteStateStore};
use std::sync::{Arc, Mutex};

fn rig(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new(label);
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        label,
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    (scratch, store)
}

// Keep the old pruning statements as an independent reference for which rows
// are deleted and how long the writer is held.
pub(super) fn old_prune(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    old_prune_after_begin(connection, || {})
}

fn old_delete_statements() -> [String; 2] {
    let predicate = "fusiform_present=0 AND source<>'override'
        AND NOT EXISTS (SELECT 1 FROM model_route_decisions
        WHERE model_route_decisions.selected_route_key=model_facts.route_key
        AND model_route_decisions.created_at >=
        CAST(strftime('%s', 'now') AS INTEGER)*1000 - 30*24*60*60*1000)";
    [format!("DELETE FROM model_route_state WHERE route_key IN (SELECT route_key FROM model_facts WHERE {predicate})"),
     format!("DELETE FROM model_facts WHERE {predicate}")]
}

fn old_prune_after_begin(
    connection: &Connection,
    after_begin: impl FnOnce(),
) -> rusqlite::Result<Vec<String>> {
    let tx = write_transaction(connection)?;
    after_begin();
    let unserved = tx
        .prepare("SELECT route_key FROM model_facts WHERE fusiform_present=0 ORDER BY route_key")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    for sql in old_delete_statements() {
        tx.execute(&sql, [])?;
    }
    crate::connection_store::timed_commit(tx)?;
    Ok(unserved)
}

fn seed(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "DELETE FROM model_facts;
         DELETE FROM model_route_state;
         INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at,source,fusiform_present)
         SELECT 'p/'||model,'p/'||model,'p',model,'{}','curated',0,source,present
         FROM (SELECT 'served' AS model,'seed' AS source,1 AS present
               UNION ALL SELECT 'unknown','seed',NULL
               UNION ALL SELECT 'unpicked','seed',0
               UNION ALL SELECT 'old','seed',0
               UNION ALL SELECT 'recent','seed',0
               UNION ALL SELECT 'within-window','seed',0
               UNION ALL SELECT 'override','override',0);
         INSERT INTO model_route_decisions(id,target_agent,requirements_json,selected_route_key,score_json,rejected_json,anchor_version,created_at)
         VALUES ('old','mason','{}','p/old','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000-31*24*60*60*1000),
                ('recent','mason','{}','p/recent','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000-1000),
                ('within-window','mason','{}','p/within-window','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000-29*24*60*60*1000);
         INSERT OR REPLACE INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at)
         SELECT route_key,route_key,provider_id,model_id,0 FROM model_facts;"
    )
}

fn remaining(connection: &Connection, table: &str) -> Vec<String> {
    connection
        .prepare(&format!("SELECT route_key FROM {table} ORDER BY route_key"))
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn reader_planned_prune_deletes_exactly_the_old_rows() {
    let (_scratch, store) = rig("prune-equivalent");
    let oracle = Connection::open_in_memory().unwrap();
    oracle.execute_batch("CREATE TABLE model_facts(route_key TEXT PRIMARY KEY,capacity_key TEXT,provider_id TEXT,model_id TEXT,scores_json TEXT,confidence TEXT,updated_at INTEGER,source TEXT,fusiform_present INTEGER);
        CREATE TABLE model_route_state(route_key TEXT PRIMARY KEY,capacity_key TEXT,provider_id TEXT,model_id TEXT,updated_at INTEGER);
        CREATE TABLE model_route_decisions(id TEXT,target_agent TEXT,requirements_json TEXT,selected_route_key TEXT,score_json TEXT,rejected_json TEXT,anchor_version INTEGER,created_at INTEGER);").unwrap();
    seed(&oracle).unwrap();
    store.store.with_conn(seed).unwrap();
    let expected = old_prune(&oracle).unwrap();
    assert_eq!(store.prune_unserved_unpicked_facts().unwrap(), expected);
    store
        .store
        .with_read(|connection| {
            for table in ["model_facts", "model_route_state"] {
                assert_eq!(remaining(connection, table), remaining(&oracle, table));
            }
            assert_eq!(
                remaining(connection, "model_facts"),
                [
                    "p/override",
                    "p/recent",
                    "p/served",
                    "p/unknown",
                    "p/within-window"
                ]
            );
            assert_eq!(
                connection.query_row("SELECT COUNT(*) FROM model_route_decisions", [], |row| {
                    row.get::<_, i64>(0)
                })?,
                3
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn prune_rechecks_selections_and_catalog_changes_after_reader_snapshot() {
    let (_scratch, store) = rig("prune-races");
    store.store.with_conn(seed).unwrap();
    store.prune_unserved_after_plan(|| {
        store.store.with_conn(|connection| connection.execute_batch(
            "INSERT INTO model_route_decisions(id,target_agent,requirements_json,selected_route_key,score_json,rejected_json,anchor_version,created_at)
             VALUES ('race','mason','{}','p/unpicked','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000);
             UPDATE model_facts SET fusiform_present=1 WHERE route_key='p/old';"
        )).unwrap();
    }).unwrap();
    assert!(store.get_fact_by_route_key("p/unpicked").unwrap().is_some());
    assert!(store.get_fact_by_route_key("p/old").unwrap().is_some());
}

#[test]
fn prune_keeps_selection_timestamped_before_snapshot_but_inserted_after_it() {
    let (_scratch, store) = rig("prune-queued-selection");
    store.store.with_conn(seed).unwrap();
    store.store.with_read(|connection| {
        // A TEXT primary key leaves SQLite's independently assigned rowid
        // as an insertion-order cursor, rather than a caller-supplied id.
        assert_eq!(connection.query_row("SELECT wr FROM pragma_table_list WHERE name='model_route_decisions'", [], |row| row.get::<_, i64>(0))?, 0);
        assert_eq!(connection.query_row("SELECT type FROM pragma_table_info('model_route_decisions') WHERE pk=1", [], |row| row.get::<_, String>(0))?, "TEXT");
        let details = connection.prepare("EXPLAIN QUERY PLAN SELECT 1 FROM model_route_decisions WHERE rowid > ?1 AND selected_route_key=?2")?
            .query_map(params![0, "p/unpicked"], |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(details.iter().any(|detail| detail.contains("INTEGER PRIMARY KEY (rowid>?)")), "recheck must search the insertion tail: {details:?}");
        Ok(())
    }).unwrap();
    // Selection timestamps are assigned before writer acquisition. Simulate
    // a queued selection by inserting its already-old timestamp afterward.
    store.prune_unserved_after_plan(|| {
        store.store.with_conn(|connection| connection.execute_batch(
            "INSERT INTO model_route_decisions(id,target_agent,requirements_json,selected_route_key,score_json,rejected_json,anchor_version,created_at)
             VALUES ('queued','mason','{}','p/unpicked','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000-60000);"
        )).unwrap();
    }).unwrap();
    assert!(store.get_fact_by_route_key("p/unpicked").unwrap().is_some());
    assert!(store.get_fact_by_route_key("p/old").unwrap().is_none());
}

fn probe_writer(connection: &Connection, path: std::path::PathBuf) -> Arc<Mutex<Option<bool>>> {
    let result = Arc::new(Mutex::new(None));
    let observed = result.clone();
    connection.progress_handler(
        100,
        Some(move || {
            let mut observed = observed.lock().unwrap();
            if observed.is_none() {
                let other = Connection::open(&path).unwrap();
                other.busy_timeout(std::time::Duration::ZERO).unwrap();
                let acquired = other.execute_batch("BEGIN IMMEDIATE").is_ok();
                if acquired {
                    other.execute_batch("ROLLBACK").unwrap();
                }
                *observed = Some(acquired);
            }
            false
        }),
    );
    result
}

#[test]
fn old_prune_holds_writer_during_scan_but_reader_plan_does_not() {
    let (scratch, store) = rig("prune-lock-scan");
    store.store.with_conn(seed).unwrap();
    store.store.with_conn(|connection| connection.execute_batch(
        "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<999)
         INSERT INTO model_route_decisions(id,target_agent,requirements_json,selected_route_key,score_json,rejected_json,anchor_version,created_at)
         SELECT 'extra-'||x,'mason','{}','p/unrelated','{}','[]',1,CAST(strftime('%s','now') AS INTEGER)*1000-1000 FROM n;"
    )).unwrap();
    store
        .store
        .with_read(|connection| {
            let observed = probe_writer(connection, scratch.path().join("store.db"));
            let plan = plan_unserved_prune(connection)?;
            connection.progress_handler(0, None::<fn() -> bool>);
            assert!(!plan.candidates.is_empty());
            assert_eq!(
                *observed.lock().unwrap(),
                Some(true),
                "another writer can acquire during the reader scan"
            );
            Ok(())
        })
        .unwrap();
    store
        .store
        .with_conn(|connection| {
            let mut observed = None;
            old_prune_after_begin(connection, || {
                observed = Some(probe_writer(connection, scratch.path().join("store.db")));
            })?;
            connection.progress_handler(0, None::<fn() -> bool>);
            assert_eq!(
                *observed.unwrap().lock().unwrap(),
                Some(false),
                "old scan reserves the writer throughout"
            );
            Ok(())
        })
        .unwrap();
}

#[derive(Clone, Copy)]
struct Before {
    stages: StoreStageTiming,
    acquisitions: u64,
    commits: u64,
}

impl Before {
    fn new(store: &SqliteRouteStateStore) -> Self {
        Self {
            stages: store.stage_timing(),
            acquisitions: store.writer_acquisition_count_for_test(),
            commits: store.writer_commit_count_for_test(),
        }
    }
    fn print(self, store: &SqliteRouteStateStore, label: &str) -> u64 {
        let after = store.stage_timing();
        let sql = after.sql_ns - self.stages.sql_ns;
        let commit = after.commit_ns - self.stages.commit_ns;
        eprintln!(
            "{label}: sql_us={} commit_us={} queue_us={} acquisitions={} commits={}",
            sql / 1000,
            commit / 1000,
            (after.writer_queue_ns - self.stages.writer_queue_ns) / 1000,
            store.writer_acquisition_count_for_test() - self.acquisitions,
            store.writer_commit_count_for_test() - self.commits
        );
        sql + commit
    }
}

#[test]
fn no_op_prune_never_acquires_or_commits_on_writer() {
    let (_scratch, store) = rig("prune-no-op");
    store
        .store
        .with_conn(|connection| connection.execute("UPDATE model_facts SET fusiform_present=1", []))
        .unwrap();
    let before = Before::new(&store);
    assert!(store.prune_unserved_unpicked_facts().unwrap().is_empty());
    before.print(&store, "no-op reader");
    assert_eq!(
        store.writer_acquisition_count_for_test(),
        before.acquisitions
    );
    assert_eq!(store.writer_commit_count_for_test(), before.commits);
}

#[test]
#[ignore = "live-sized writer-hold comparison; run in release with --ignored --nocapture"]
fn live_sized_catalog_prune_writer_hold() {
    let (_scratch, store) = rig("prune-live-sized");
    let catalog = |connection: &Connection| {
        connection.execute_batch(
        "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<837)
         INSERT OR REPLACE INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at,source,fusiform_present)
         SELECT 'p/model-'||x,'p/model-'||x,'p','model-'||x,'{}','curated',0,'seed',1 FROM n;
         INSERT OR REPLACE INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at)
         SELECT route_key,route_key,provider_id,model_id,0 FROM model_facts;"
    )
    };
    store.store.with_conn(|connection| {
        connection.execute("DELETE FROM model_facts", [])?;
        catalog(connection)?;
        connection.execute_batch(
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<181264)
             INSERT INTO model_route_decisions(id,target_agent,requirements_json,selected_route_key,score_json,rejected_json,anchor_version,created_at)
             SELECT 'd-'||x,'mason','{}','p/model-'||(x%68),'{}','[]',1,
             CAST(strftime('%s','now') AS INTEGER)*1000 - 2*24*60*60*1000 FROM n;"
        )
    }).unwrap();
    store
        .store
        .with_read(|connection| {
            for sql in old_delete_statements() {
                let details = connection
                    .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                    .query_map([], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                eprintln!("old prune query plan: {sql}\n{details:?}");
            }
            Ok(())
        })
        .unwrap();
    let before = Before::new(&store);
    assert!(store.store.with_conn(old_prune).unwrap().is_empty());
    let old_empty = before.print(&store, "old zero-absent writer hold");
    let before = Before::new(&store);
    assert!(store.prune_unserved_unpicked_facts().unwrap().is_empty());
    before.print(&store, "new zero-absent reader scan");
    assert_eq!(
        store.writer_acquisition_count_for_test(),
        before.acquisitions
    );
    assert_eq!(store.writer_commit_count_for_test(), before.commits);
    store.store.with_conn(|connection| connection.execute("UPDATE model_facts SET fusiform_present=0 WHERE CAST(substr(model_id,7) AS INTEGER)%4=0", [])).unwrap();
    let before = Before::new(&store);
    let expected = store.store.with_conn(old_prune).unwrap();
    let old_sparse = before.print(&store, "old quarter-absent writer hold");
    let remaining_old = store
        .store
        .with_read(|connection| Ok(remaining(connection, "model_facts")))
        .unwrap();
    store.store.with_conn(|connection| {
        catalog(connection)?;
        connection.execute("UPDATE model_facts SET fusiform_present=0 WHERE CAST(substr(model_id,7) AS INTEGER)%4=0", [])?;
        Ok(())
    }).unwrap();
    let before = Before::new(&store);
    let writer_start = std::cell::Cell::new(before);
    assert_eq!(
        store
            .prune_unserved_after_plan(|| {
                before.print(&store, "new quarter-absent reader plan");
                writer_start.set(Before::new(&store));
            })
            .unwrap(),
        expected
    );
    let new_sparse = writer_start
        .get()
        .print(&store, "new quarter-absent total keyed writer hold");
    assert_eq!(
        store
            .store
            .with_read(|connection| Ok(remaining(connection, "model_facts")))
            .unwrap(),
        remaining_old
    );
    eprintln!("prune fixture: facts=838 decisions=181265 picked_routes=68; old_empty_hold_us={} new_empty_hold_us=0 old_sparse_hold_us={} new_sparse_total_hold_us={}", old_empty/1000, old_sparse/1000, new_sparse/1000);
    assert!(
        new_sparse < old_sparse,
        "keyed writer phase must be shorter than full scan"
    );
}

#[test]
fn prune_ends_route_and_failure_sources_atomically_like_the_old_schema() {
    let scratch = cortexkit_test_support::ScratchDir::new("prune-native-source-lifetime");
    let old_descriptor = dev_descriptor(
        "prune-old-source-lifetime",
        scratch.path().join("old.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&old_descriptor, 95).unwrap();
    let (_native_scratch, native) = rig("prune-native-source-lifetime");
    let now = crate::now_ms();
    old.with_writer_for_test(|conn| {
        seed(conn)?;
        conn.execute("UPDATE model_route_state SET cooldown_until=?1,wall_unscoped=1,wall_observed_at=?2,last_error='route capacity',
            failure_backoff_until=?1,failure_backoff_reason='send failure',failure_observed_at=?2 WHERE route_key='p/unpicked'",params![now+3600000,now])?;
        crate::fence_rechecks::legacy::sync_at(conn,now)?;
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_origin WHERE kind IN ('route','failure')",[],|row|row.get::<_,i64>(0))?,2);
        old_prune(conn)?;
        crate::fence_rechecks::legacy::sync_at(conn,now)?;
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_origin WHERE kind IN ('route','failure')",[],|row|row.get::<_,i64>(0))?,0);
        assert_eq!(conn.query_row("SELECT count(*) FROM fence_recheck WHERE kind IN ('route','failure')",[],|row|row.get::<_,i64>(0))?,0);
        Ok(())
    }).unwrap();
    native
        .with_writer_for_test(|conn| {
            seed(conn)?;
            let wall = crate::block_storage::wall_source(
                conn,
                crate::block_storage::WallSource {
                    kind: "route",
                    key: "p/unpicked",
                    provider: "p",
                    model: Some("unpicked"),
                    cooldown: Some((now + 3600000) as f64),
                    exhausted: None,
                    wall_observed: Some(now),
                    observed_at: now,
                    error: Some("route capacity"),
                    class: None,
                    unscoped: true,
                },
            )?
            .unwrap();
            crate::block_storage::write_source(conn, &wall)?;
            let failure = crate::block_storage::failure_source(
                conn,
                crate::block_storage::FailureSource {
                    key: "p/unpicked",
                    provider: "p",
                    model: "unpicked",
                    deadline: Some((now + 3600000) as f64),
                    reason: Some("send failure"),
                    observed_at: now,
                    error: Some("route capacity"),
                    class: None,
                    unscoped: true,
                },
            )?
            .unwrap();
            crate::block_storage::write_source(conn, &failure)?;
            Ok(())
        })
        .unwrap();
    native
        .account_transaction(|tx| tx.sync_fence_rechecks(crate::now_ms()))
        .unwrap();
    assert_eq!(
        native
            .fence_rechecks()
            .unwrap()
            .iter()
            .filter(|row| matches!(row.kind.as_str(), "route" | "failure"))
            .count(),
        2
    );
    native.prune_unserved_unpicked_facts().unwrap();
    native.with_reader_for_test(|conn| {
        assert_eq!(conn.query_row("SELECT count(*) FROM block WHERE kind IN ('route','failure') AND (source_present=1 OR managed=1)",[],|row|row.get::<_,i64>(0))?,0,
            "pruning must end source and clock in the same writer transaction");
        Ok(())
    }).unwrap();
    let readd = |conn: &Connection| {
        conn.execute("INSERT INTO model_facts(route_key,capacity_key,provider_id,model_id,scores_json,confidence,updated_at,source,fusiform_present)
        VALUES ('p/unpicked','p/unpicked','p','unpicked','{}','curated',0,'seed',1)",[]).map(|_|())
    };
    old.with_writer_for_test(|conn| {
        readd(conn)?;
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM fence_origin WHERE kind IN ('route','failure')",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        Ok(())
    })
    .unwrap();
    native.with_writer_for_test(readd).unwrap();
    assert!(
        native
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|row| !matches!(row.kind.as_str(), "route" | "failure")),
        "rediscovery cannot restore the deleted route's refusal"
    );
}
