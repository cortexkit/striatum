#![cfg(test)]

use crate::{AccountFenceRow, SqliteRouteStateStore};
use rusqlite::params;

pub(super) fn legacy_account_wall_error() -> String {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../striatum-core/tests/fixtures/account-wall-scope.json"
    ))
    .unwrap();
    vectors[0]["error"].as_str().unwrap().into()
}

pub(super) fn insert_legacy_account_wall(store: &SqliteRouteStateStore, route: &str, class: &str) {
    store.store.with_conn(|conn| conn.execute(
        "INSERT INTO model_route_state (route_key,capacity_key,provider_id,model_id,cooldown_until,quota_exhausted_until,last_error,wall_class,updated_at)
         VALUES (?1,'anthropic/opus','anthropic','opus',1791493229401,1791493229401,?2,?3,1791048824539)",
        params![route,legacy_account_wall_error(),class],
    )).unwrap();
}

// Capture the actual schema-062 evidence, before the native table exists. A
// current runtime accessor cannot supply an old-schema reference dataset.
pub(super) fn legacy_fences(store: &SqliteRouteStateStore, provider: &str) -> Vec<AccountFenceRow> {
    store.with_reader_for_test(|conn| {
        let mut query=conn.prepare("SELECT fence_id,provider,account_id,scope_json,fence_json,source,observed_at,
            principal,admission_id,observation_id,created_at,cleared_at,cleared_reason
            FROM account_fence WHERE provider=?1 AND cleared_at IS NULL ORDER BY created_at,fence_id")?;
        let rows=query.query_map([provider],|row|Ok(AccountFenceRow {
            fence_id:row.get(0)?,provider:row.get(1)?,account_id:row.get(2)?,scope_json:row.get(3)?,
            fence_json:row.get(4)?,source:row.get(5)?,observed_at:row.get(6)?,principal:row.get(7)?,
            admission_id:row.get(8)?,observation_id:row.get(9)?,created_at:row.get(10)?,
            cleared_at:row.get(11)?,cleared_reason:row.get(12)?,
        }))?.collect();
        rows
    }).unwrap()
}

pub(super) fn seed_failure_clear(store: &SqliteRouteStateStore) {
    store.with_writer_for_test(|conn| {
        let key="test-provider/test-model";
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,consecutive_terminal_failures)
            VALUES (?1,?1,'test-provider','test-model',1,3)",[key])?;
        let wall=crate::block_storage::wall_source(conn,crate::block_storage::WallSource {
            kind:"route",key,provider:"test-provider",model:Some("test-model"),
            // Failure clearing must preserve even an anomalously long wall.
            cooldown:Some(1_500_000_000_000.0),exhausted:Some(1_500_000_000_000.0),
            wall_observed:None,observed_at:1,error:None,class:None,unscoped:false,
        })?.unwrap();
        crate::block_storage::write_source(conn,&wall)?;
        let failure=crate::block_storage::failure_source(conn,crate::block_storage::FailureSource {
            key,provider:"test-provider",model:"test-model",deadline:Some(1_000.0),
            reason:Some("consecutive terminal send failures: 3"),observed_at:1,error:None,class:None,unscoped:false,
        })?.unwrap();
        crate::block_storage::write_source(conn,&failure)?;
        Ok(())
    }).unwrap();
}

pub(super) fn failure_clear_row(
    store: &SqliteRouteStateStore,
) -> (i64, Option<f64>, Option<String>, Option<f64>, Option<f64>) {
    store.with_reader_for_test(|conn|conn.query_row(
        "SELECT m.consecutive_terminal_failures,f.deadline,f.reason,w.cooldown_until,w.quota_exhausted_until
         FROM model_route_state m
         LEFT JOIN block f ON f.kind='failure' AND f.origin_key=json_array(m.route_key) AND f.source_present=1
         LEFT JOIN block w ON w.kind='route' AND w.origin_key=json_array(m.route_key) AND w.source_present=1
         WHERE m.route_key='test-provider/test-model'",
        [],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))
    )).unwrap()
}

// Run the legacy fixture's full coverage and capture before upgrading. Every
// assertion in the caller then exercises the registered native schema.
pub(super) fn migrated_parity_store(path: &std::path::Path, now: i64) -> SqliteRouteStateStore {
    let descriptor = crate::dev_descriptor("block-parity", path.to_string_lossy());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    old.seed_block_parity_fixture_for_test(now);
    old.assert_block_parity_fixture_coverage_for_test(now);
    let mut expected = old.effective_blocks_oracle_for_test(now).unwrap();
    drop(old);
    let native = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    let mut actual = native.effective_blocks_oracle_for_test(now).unwrap();
    for rows in [&mut expected, &mut actual] {
        for row in rows.iter_mut() {
            row.cleared_models.sort();
        }
        rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
    }
    assert_eq!(
        actual, expected,
        "captured095effective rows equal independent native SQL after registered096"
    );
    let kinds = native
        .with_reader_for_test(|conn| {
            conn.prepare("SELECT DISTINCT kind FROM block")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()
        })
        .unwrap();
    assert_eq!(
        kinds,
        striatum_core::blocks::SOURCE_KINDS
            .into_iter()
            .map(str::to_string)
            .collect()
    );
    native
}
