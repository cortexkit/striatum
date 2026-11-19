use super::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[test]
fn keyed_admission_prune_seeks_reservations_and_started_runs() {
    let scratch = cortexkit_test_support::ScratchDir::new("keyed-admission-prune-plan");
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        "keyed-admission-prune-plan",
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    store.store.with_read(|conn| {
        let plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {}", admission_delete_sql(true)))?
            .query_map(params![50, 1, 10_000_000, 8_200_000, "[1]"], |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for range in [
            "SEARCH p USING INDEX account_continue_by_reservation (principal=? AND reservation_id=?)",
            "SEARCH s USING INDEX account_admission_started_by_run (principal=? AND run_id=? AND provider=? AND account_id=?)",
        ] {
            assert!(plan.iter().any(|line| line.contains(range)), "missing {range}: {plan:?}");
        }
        assert!(!plan.iter().any(|line| line.contains("MATERIALIZE sources")), "{plan:?}");
        Ok(())
    }).unwrap();
}

#[test]
fn keyed_admission_candidates_preserve_the_used_offer_source_rules() {
    let scratch = cortexkit_test_support::ScratchDir::new("keyed-admission-prune-parity");
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        "keyed-admission-prune-parity",
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    store.store.with_conn(|conn| {
        conn.execute_batch("INSERT INTO account_pin VALUES
          ('pin','owner','conv','provider',1,'model','account','credential','run_frozen',NULL,'dec',1);
          INSERT INTO account_admission (rowid, principal, admission_id, request_fingerprint, request_mode,
            decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
            pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
            admission_valid_until, lease_valid_until, reply_json, run_id, started_at)
          SELECT rowid,'owner',id,'req',mode,'dec','conv','provider','model','account','credential',
            'pin',1,state,0,0,0,rowid,12,13,'{}','run',10
          FROM (SELECT 1 AS rowid, 'original' AS id, 'finished' AS state, 'admit' AS mode
                UNION ALL SELECT 2,'middle','finished','reassign'
                UNION ALL SELECT 3,'sibling','started','admit');
          INSERT INTO account_continue (principal,selection_id,request_json,answer_json,run_id,
            admission_id,previous_decision_id,decision_id,reservation_id,valid_until_ms,used_at_ms)
            VALUES ('owner','first','{}','{}','run','original','origin','d-middle','middle',12,20),
                   ('owner','second','{}','{}','run','original','d-middle','d-final','final',13,21);")?;
        let candidates = |keyed| -> rusqlite::Result<Vec<i64>> {
            let sql = if keyed {
                admission_candidates(true)
            } else {
                format!("WITH sources AS MATERIALIZED ({USED_OFFER_SOURCES}) {}", admission_candidates(false))
            };
            let mut values: Vec<rusqlite::types::Value> = vec![50.into(), 3.into(), 10_000_000.into(), 8_200_000.into()];
            if keyed {
                values.push("[1,2,3]".to_owned().into());
            }
            conn.prepare(&sql)?.query_map(rusqlite::params_from_iter(values), |row| row.get(0))?.collect()
        };

        for (change, want) in [
            ("", &[3][..]),
            ("DELETE FROM account_continue WHERE selection_id='second'", &[2, 3]),
            ("UPDATE account_admission SET request_mode='renew' WHERE rowid=3", &[1, 2, 3]),
            ("UPDATE account_admission SET request_mode='admit' WHERE rowid=3;
              INSERT INTO account_continue VALUES ('owner','third','{}','{}','run','sibling','sibling-origin',
                'd-sibling','next',13,22,NULL)", &[1, 2, 3]),
        ] {
            conn.execute_batch(change)?;
            assert_eq!(candidates(false)?, want, "oracle after {change}");
            assert_eq!(candidates(true)?, want, "keyed after {change}");
        }
        Ok(())
    }).unwrap();
}

#[test]
fn keyed_admission_prune_ignores_unrelated_used_offers() {
    let scratch = cortexkit_test_support::ScratchDir::new("keyed-admission-prune-cost");
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        "keyed-admission-prune-cost",
        scratch.path().join("store.db").to_string_lossy(),
    ))
    .unwrap();
    store.store.with_conn(|conn| {
        conn.execute_batch("INSERT INTO account_pin VALUES
          ('pin','owner','conv','provider',1,'model','account','credential','run_frozen',NULL,'dec',1);
          INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode,
            decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
            pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
            admission_valid_until, lease_valid_until, reply_json)
          VALUES ('owner','due','req','admit','dec','conv','provider','model','account','credential',
            'pin',1,'finished',0,0,0,1,2,3,'{}');")?;
        let key = conn.query_row("SELECT rowid FROM account_admission WHERE admission_id='due'", [], |row| row.get(0))?;
        let check = || -> rusqlite::Result<(Vec<String>, u64)> {
            let tx = crate::write_transaction(conn)?;
            let steps = Arc::new(AtomicU64::new(0));
            let counter = Arc::clone(&steps);
            tx.progress_handler(1, Some(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                false
            }));
            let result = crate::AccountTx::sql(&tx).delete_account_admissions_returning(50, &[key], 10_000_000);
            tx.progress_handler(0, None::<fn() -> bool>);
            let ids = result.unwrap().into_iter().map(|row| row.admission_id).collect();
            tx.rollback()?;
            Ok((ids, steps.load(Ordering::Relaxed)))
        };
        let (empty, base_steps) = check()?;
        assert_eq!(empty, ["due"]);
        let tx = crate::write_transaction(conn)?;
        tx.execute_batch("WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<100000)
          INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode,
            decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
            pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
            admission_valid_until, lease_valid_until, reply_json, run_id, started_at)
          SELECT 'owner','other-'||n,'req','admit','dec','conv','provider','model','account','credential',
            'pin',1,'started',0,0,0,100,200,300,'{}','other-run-'||n,101 FROM ids;
          INSERT INTO account_continue (principal, selection_id, request_json, answer_json, run_id,
            admission_id, previous_decision_id, decision_id, valid_until_ms, used_at_ms)
          SELECT principal,admission_id,'{}','{}',run_id,admission_id,'prev-'||admission_id,
            'dec-'||admission_id,400,250 FROM account_admission WHERE admission_id LIKE 'other-%';")?;
        tx.commit()?;
        let (grown, grown_steps) = check()?;
        assert_eq!(grown, empty);
        assert!(grown_steps <= base_steps + 100,
            "100,000 unrelated used offers under the same principal: {base_steps} -> {grown_steps} VM steps");
        eprintln!("keyed admission prune VM steps: {base_steps} -> {grown_steps}");
        Ok(())
    }).unwrap();
}
