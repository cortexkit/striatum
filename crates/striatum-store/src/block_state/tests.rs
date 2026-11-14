//! The block state must equal a fresh load after every command, and blocks
//! derived from it must equal the SQL adapter's at every `now`.

use super::*;
use crate::{
    err, AccountCommandKind, AccountStateBridge, AccountWorkingState, SqliteRouteStateStore,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use striatum_core::route_state_store::{FactSource, RouteStateStore, RouteStateUpdate};
use striatum_core::RouteStateResult;

#[derive(Clone)]
struct TestState(BlockState);

impl AccountWorkingState for TestState {
    fn fork(&self) -> Box<dyn AccountWorkingState> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn apply_block_rows(&mut self, rows: &BlockRows) {
        self.0.apply(rows);
    }
}

/// A publisher holding only the block state, standing in for the module's.
#[derive(Default)]
struct TestOwner {
    published: Mutex<Option<Arc<TestState>>>,
    reload: AtomicBool,
    loads: std::sync::atomic::AtomicU64,
}

impl AccountStateBridge for TestOwner {
    fn load(&self, tx: &crate::AccountTx<'_>) -> RouteStateResult<Box<dyn AccountWorkingState>> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(TestState(tx.load_block_state()?)))
    }
    fn publish(&self, state: &dyn AccountWorkingState, _generation: u64) {
        let state = state.as_any().downcast_ref::<TestState>().unwrap().clone();
        *self.published.lock().unwrap() = Some(Arc::new(state));
    }
    fn snapshot(&self) -> RouteStateResult<Arc<dyn std::any::Any + Send + Sync>> {
        self.published
            .lock()
            .unwrap()
            .clone()
            .map(|state| state as _)
            .ok_or_else(|| err("not loaded"))
    }
    fn failed(&self, _error: &str) {}
    fn reload_requested(&self) -> bool {
        self.reload.swap(false, Ordering::AcqRel)
    }
    fn request_reload(&self) {
        self.reload.store(true, Ordering::Release);
    }
}

struct Rig {
    store: SqliteRouteStateStore,
    owner: Arc<TestOwner>,
    // Dropped last, after the store has closed its files.
    _scratch: cortexkit_test_support::ScratchDir,
}

fn rig(name: &str) -> Rig {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        name,
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    let owner = Arc::new(TestOwner::default());
    store.install_account_state_bridge(owner.clone()).unwrap();
    Rig {
        store,
        owner,
        _scratch: scratch,
    }
}

impl Rig {
    fn published(&self) -> BlockState {
        self.owner
            .published
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .clone()
    }

    fn fresh(&self) -> BlockState {
        self.store.with_reader_for_test(BlockState::load).unwrap()
    }

    /// Run statements as one account command, as every production writer does.
    fn run(&self, f: impl Fn(&Connection) -> rusqlite::Result<()> + Send + 'static) {
        self.store
            .account_command(AccountCommandKind::Maintenance, move |tx| tx.pool_sql(&f))
            .unwrap();
    }

    fn sql(&self, sql: String) {
        self.run(move |conn| conn.execute_batch(&sql));
    }

    /// Memory equals a fresh load, its view rows equal SQLite's view, and its
    /// blocks equal the SQL adapter's at each time.
    fn check(&self, times: &[i64], context: &str) {
        let memory = self.published();
        assert_eq!(
            memory,
            self.fresh(),
            "memory equals a fresh load: {context}"
        );
        let view = self
            .store
            .with_reader_for_test(|conn| {
                let mut rows = conn
                    .prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,deadline,reset_at FROM fence_origin")?
                    .query_map([], |r| fence_rechecks::view_origin(r, 0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
                Ok(rows)
            })
            .unwrap();
        assert_eq!(memory.view_origins(), view, "view rows: {context}");
        for &now in times {
            let derived = memory.blocks(now).unwrap();
            let mut sql = self.store.load_blocks(now).unwrap();
            for blocks in [&mut sql.blocks, &mut sql.cleared_evidence] {
                blocks.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
            }
            assert_eq!(derived.blocks, sql.blocks, "blocks at {now}: {context}");
            assert_eq!(
                derived.cleared_evidence, sql.cleared_evidence,
                "cleared evidence at {now}: {context}"
            );
        }
    }
}

const NOW: i64 = 1_800_000_000_000;

#[test]
fn block_tables_are_rowid_tables_keyed_by_their_primary_key() {
    // The update hook reports no change to a WITHOUT ROWID table, and the
    // REPLACE handling finds a replaced row only through the primary key.
    let rig = rig("block-state-schema");
    rig.store
        .with_reader_for_test(|conn| {
            for table in BLOCK_TABLES {
                let sql: String = conn.query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
                    [table],
                    |r| r.get(0),
                )?;
                assert!(
                    !sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
                    "{table} must be a rowid table"
                );
                let unique: i64 = conn.query_row(
                    "SELECT count(*) FROM pragma_index_list(?1) WHERE \"unique\"=1",
                    [table],
                    |r| r.get(0),
                )?;
                assert_eq!(
                    unique, 1,
                    "{table} has exactly one unique key, its primary key"
                );
                let primary: i64 = conn.query_row(
                    "SELECT count(*) FROM pragma_index_list(?1) WHERE origin='pk'",
                    [table],
                    |r| r.get(0),
                )?;
                assert_eq!(primary, 1, "{table} has a primary key index");
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn memory_matches_the_view_and_the_adapter_on_the_parity_fixture() {
    let rig = rig("block-state-parity");
    rig.run(|conn| crate::block_parity_fixture::seed_block_parity_fixture(conn, NOW));
    rig.store.assert_block_parity_fixture_coverage_for_test(NOW);
    let times = rig.store.block_parity_times(NOW).unwrap();
    assert!(times.len() > 3);
    rig.check(&times, "parity fixture");
    assert!(block_read_backs() > 0);
}

#[test]
fn a_replace_that_moves_a_key_to_a_new_rowid_leaves_no_stale_row() {
    let rig = rig("block-state-replace");
    rig.sql(format!(
        "INSERT INTO credential_entitlement_fences VALUES ('p','c','billing_exhausted',NULL,{NOW});"
    ));
    let before: i64 = rig
        .store
        .with_reader_for_test(|conn| {
            conn.query_row("SELECT rowid FROM credential_entitlement_fences", [], |r| {
                r.get(0)
            })
        })
        .unwrap();
    // The hook reports the insert of the new row but not the delete of the
    // row it replaced.
    rig.sql(format!(
        "INSERT OR REPLACE INTO credential_entitlement_fences VALUES ('p','c','model_unsupported',NULL,{});",
        NOW + 1
    ));
    let after: i64 = rig
        .store
        .with_reader_for_test(|conn| {
            conn.query_row("SELECT rowid FROM credential_entitlement_fences", [], |r| {
                r.get(0)
            })
        })
        .unwrap();
    assert_ne!(before, after, "REPLACE gave the key a new rowid");
    assert_eq!(
        rig.published().table_sizes()[1],
        ("credential_entitlement_fences", 1)
    );
    rig.check(&[NOW], "credential replaced");
    // A replacement the view does not select must still evict the old row.
    rig.sql(format!(
        "INSERT INTO account_route_fence VALUES ('p','c','m',{},'d',{NOW},{NOW},'quota');",
        NOW + 60_000
    ));
    rig.sql(format!(
        "INSERT OR REPLACE INTO account_route_fence VALUES ('p','c','m',{},'d',{NOW},{NOW},'transient');",
        NOW + 60_000
    ));
    assert_eq!(rig.published().table_sizes()[2], ("account_route_fence", 0));
    rig.check(&[NOW], "route fence replaced by an unselected row");
}

#[test]
fn a_whole_table_delete_is_captured() {
    let rig = rig("block-state-truncate");
    rig.sql(format!(
        "INSERT INTO account_transient_backoff VALUES ('p','a',{},{NOW},'k','[]');
         INSERT INTO account_transient_backoff VALUES ('p','b',{},{NOW},'k','[]');",
        NOW + 60_000,
        NOW + 60_000
    ));
    assert_eq!(rig.published().table_sizes()[10].1, 2);
    // Without a WHERE clause SQLite may drop every row at once without
    // reporting each; the writer connection's authorizer prevents that.
    rig.sql("DELETE FROM account_transient_backoff;".into());
    assert_eq!(rig.published().table_sizes()[10].1, 0);
    rig.check(&[NOW], "after a whole-table delete");
}

#[test]
fn a_failed_command_leaves_no_captured_row() {
    let rig = rig("block-state-rollback");
    let result = rig
        .store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.pool_sql(|conn| {
                conn.execute_batch(&format!(
                    "INSERT INTO account_transient_backoff VALUES ('p','a',{},{NOW},'k','[]');",
                    NOW + 60_000
                ))
            })?;
            Err::<(), _>(err("planned failure after a block write"))
        });
    assert!(result.is_err());
    assert_eq!(rig.published().table_sizes()[10].1, 0);
    rig.check(&[NOW], "after a failed command");
}

#[test]
fn a_write_outside_a_command_reloads_the_state_before_the_next_batch() {
    let rig = rig("block-state-outside");
    let row = |account: &str| {
        format!(
            "INSERT INTO account_transient_backoff VALUES ('p','{account}',{},{NOW},'k','[]');",
            NOW + 60_000
        )
    };
    // A test seed bypasses the writer on purpose: it reloads the state but is
    // not a violation.
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    let seeds = rig.store.block_seed_writes_for_test();
    let seed = row("seeded");
    rig.store
        .with_writer_for_test(move |conn| conn.execute_batch(&seed))
        .unwrap();
    assert_eq!(rig.store.block_seed_writes_for_test(), seeds + 1);
    assert_eq!(rig.store.block_writes_outside_command(), 0);
    assert_eq!(
        rig.published().table_sizes()[10].1,
        0,
        "the hook saw no command"
    );
    rig.sql("SELECT 1;".into());
    assert_eq!(
        rig.owner.loads.load(Ordering::Relaxed),
        loads + 1,
        "one reload"
    );
    rig.check(&[NOW], "after a seed and a reload");
    // Any other write outside a command is counted, named and reloaded.
    rig.store
        .bypass_account_writer_for_test(&row("bypass"))
        .unwrap();
    assert_eq!(rig.store.block_writes_outside_command(), 1);
    let tables = rig.store.block_tables_written_outside_command();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, "account_transient_backoff");
    rig.sql("SELECT 1;".into());
    assert_eq!(
        rig.owner.loads.load(Ordering::Relaxed),
        loads + 2,
        "one more reload"
    );
    rig.check(&[NOW], "after a bypass and a reload");
}

#[test]
fn a_fork_shares_every_table() {
    let rig = rig("block-state-fork");
    rig.run(|conn| crate::block_parity_fixture::seed_block_parity_fixture(conn, NOW));
    let state = rig.published();
    let fork = state.clone();
    assert!(fork.shares_storage_with(&state));
}

/// A small deterministic generator, so a failure names a reproducible seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn pick<'a>(&mut self, values: &[&'a str]) -> &'a str {
        values[self.below(values.len() as u64) as usize]
    }
    fn time(&mut self, now: i64) -> i64 {
        now - 7_200_000 + self.below(4) as i64 * 2_400_000 + self.below(3) as i64
    }
    fn maybe_time(&mut self, now: i64) -> String {
        if self.below(3) == 0 {
            "NULL".into()
        } else {
            self.time(now).to_string()
        }
    }
}

const PROVIDERS: [&str; 2] = ["p0", "p1"];
const CREDENTIALS: [&str; 3] = ["c0", "c1", "c2"];
const MODELS: [&str; 2] = ["m0", "m1"];
const ERRORS: [&str; 4] = [
    "NULL",
    "'provider capacity'",
    "'route capacity'",
    // An account's error copied onto a route or provider row quarantines it.
    "'{\"provider_code\":\"quota_exhausted\",\"account_ref\":\"account-0\"}'",
];

/// One step of the generated sequence. Returns the operation's name.
fn step(rig: &Rig, rng: &mut Rng, now: i64) -> &'static str {
    let p = rng.pick(&PROVIDERS);
    let c = rng.pick(&CREDENTIALS);
    let m = rng.pick(&MODELS);
    let offset = rng.below(4);
    let at = rng.time(now);
    let until = rng.time(now) + 3_600_000;
    match rng.below(30) {
        0 => {
            let id = if rng.below(2) == 0 {
                format!("f{}", rng.below(4))
            } else {
                format!("credential-quota:[\"{p}\",\"{c}\"]:{}", rng.below(3))
            };
            rig.sql(format!("INSERT OR IGNORE INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json) VALUES ('{id}','{p}','a0','{{}}','{{\"reset\":{{\"at_ms\":{until}}}}}','broca_account_limit',{at},'x',{at},NULL,'[]');"));
            "insert account fence"
        }
        1 => {
            rig.sql(format!("UPDATE account_fence SET cleared_at={now} WHERE rowid IN (SELECT rowid FROM account_fence WHERE cleared_at IS NULL ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "clear account fence"
        }
        2 => {
            rig.sql(format!("INSERT INTO credential_entitlement_fences VALUES ('{p}','{c}','billing_exhausted',NULL,{at}) ON CONFLICT(provider_id,credential_id) DO UPDATE SET observed_at_ms=excluded.observed_at_ms;"));
            "upsert credential fence"
        }
        3 => {
            rig.sql(format!("INSERT OR REPLACE INTO credential_entitlement_fences VALUES ('{p}','{c}','model_unsupported',NULL,{at});"));
            "replace credential fence"
        }
        4 => {
            let class = rng.pick(&["quota", "transient"]);
            rig.sql(format!("INSERT INTO account_route_fence VALUES ('{p}','{c}','{m}',{until},'d',{at},{at},'{class}') ON CONFLICT(provider,credential_id,model) DO UPDATE SET wall_class=excluded.wall_class,until_ms=excluded.until_ms,last_reported_at=excluded.last_reported_at;"));
            "upsert account route fence"
        }
        5 => {
            let kind = rng.pick(&["billing", "transient"]);
            rig.sql(format!("INSERT OR REPLACE INTO account_non_quota_route_fence VALUES ('{p}','{c}','{m}',{until},'d',{at},{at},'{kind}');"));
            "replace non-quota route fence"
        }
        6 => {
            let (wall, cooldown) = (
                rng.maybe_time(now + 3_600_000),
                rng.maybe_time(now + 3_600_000),
            );
            rig.sql(format!("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,cooldown_until,cooldown_reason,wall_observed_at,cooldown_observed_at,updated_at) VALUES ('{p}','{c}',{wall},'billing',{cooldown},'rate',{at},{at},{at}) ON CONFLICT(provider,credential_id) DO UPDATE SET walled_until=excluded.walled_until,cooldown_until=excluded.cooldown_until,updated_at=excluded.updated_at;"));
            "upsert pool key"
        }
        7 => {
            rig.sql(format!("UPDATE pool_key_state SET last_used_at={at},updated_at={at} WHERE rowid IN (SELECT rowid FROM pool_key_state ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "touch pool key"
        }
        8 => {
            let (cooldown, quota) = (
                rng.maybe_time(now + 3_600_000),
                rng.maybe_time(now + 3_600_000),
            );
            let error = rng.pick(&ERRORS);
            let unscoped = rng.below(2);
            let class = rng.pick(&["NULL", "'quota'", "'billing'"]);
            rig.sql(format!("INSERT OR REPLACE INTO provider_cooldown_state(provider_key,cooldown_until,quota_exhausted_until,wall_observed_at,wall_unscoped,wall_class,last_error,last_error_at,updated_at) VALUES ('{p}',{cooldown},{quota},{at},{unscoped},{class},{error},{at},{at});"));
            "replace provider row"
        }
        9 => {
            // A route row recording its provider's wall exactly is hidden by
            // the provider origin; a shorter or different one is not.
            let copy = rng.below(2) == 0;
            let shorter = if rng.below(2) == 0 { "-1" } else { "" };
            if copy {
                rig.sql(format!("INSERT OR REPLACE INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,quota_exhausted_until,wall_observed_at,wall_unscoped,wall_class,last_error,last_error_at,updated_at) SELECT '{p}/{m}','{p}/{m}',provider_key,'{m}',cooldown_until{shorter},quota_exhausted_until,wall_observed_at,wall_unscoped,wall_class,last_error,last_error_at,updated_at FROM provider_cooldown_state WHERE provider_key='{p}';"));
            } else {
                let (cooldown, quota, failure) = (
                    rng.maybe_time(now + 3_600_000),
                    rng.maybe_time(now + 3_600_000),
                    rng.maybe_time(now + 3_600_000),
                );
                let error = rng.pick(&ERRORS);
                let unscoped = rng.below(2);
                rig.sql(format!("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,quota_exhausted_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at,failure_backoff_until,failure_backoff_reason,failure_observed_at) VALUES ('{p}/{m}','{p}/{m}','{p}','{m}',{cooldown},{quota},{at},{unscoped},{error},{at},{at},{failure},'error',{at}) ON CONFLICT(route_key) DO UPDATE SET cooldown_until=excluded.cooldown_until,quota_exhausted_until=excluded.quota_exhausted_until,wall_observed_at=excluded.wall_observed_at,last_error=excluded.last_error,failure_backoff_until=excluded.failure_backoff_until,wall_unscoped=excluded.wall_unscoped;"));
            }
            "write route row"
        }
        10 => {
            rig.sql(format!("UPDATE model_route_state SET latency_ema_ms={offset},updated_at={at} WHERE rowid IN (SELECT rowid FROM model_route_state ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "route data only"
        }
        11 => {
            let size = 16 + rng.below(2);
            let table = rng.pick(&["account_size_refusal", "account_non_quota_size_refusal"]);
            rig.sql(format!(
                "INSERT OR REPLACE INTO {table} VALUES ('{p}','{c}','{m}',{size},{until},{at});"
            ));
            "size refusal"
        }
        12 => {
            rig.sql(format!("UPDATE OR IGNORE account_size_refusal SET size_class=size_class+1 WHERE rowid IN (SELECT rowid FROM account_size_refusal ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "move a size refusal's key"
        }
        13 => {
            rig.sql(format!("INSERT OR IGNORE INTO account_principal_exclusion VALUES ('x','adm{offset}','{c}','runner','roster',NULL,{at});"));
            "principal exclusion"
        }
        14 => {
            rig.sql(format!("INSERT INTO account_transient_backoff VALUES ('{p}','a{offset}',{until},{at},'k{at}','[]') ON CONFLICT(provider,account_id) DO UPDATE SET retry_at_ms=excluded.retry_at_ms,evidence_key=excluded.evidence_key;"));
            "transient backoff"
        }
        15 => {
            let table = rng.pick(&[
                "account_fence",
                "credential_entitlement_fences",
                "account_route_fence",
                "account_non_quota_route_fence",
                "pool_key_state",
                "provider_cooldown_state",
                "model_route_state",
                "account_size_refusal",
                "account_principal_exclusion",
                "account_transient_backoff",
            ]);
            rig.sql(format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "delete a source row"
        }
        16 | 17 => {
            rig.run(fence_rechecks::sync);
            "sync"
        }
        18 => {
            let owner = format!("owner{}", rng.below(3));
            rig.run(move |conn| {
                let origins = fence_rechecks::read(conn)?;
                if let Some(origin) = origins.get(offset as usize) {
                    // Late enough that the clock is due and the claim succeeds.
                    let claimed_at = now + 4 * 3_600_000;
                    fence_rechecks::claim_with_authorization(
                        conn,
                        std::slice::from_ref(origin),
                        claimed_at,
                        &owner,
                    )?;
                }
                Ok(())
            });
            "claim"
        }
        19 => {
            rig.sql(format!("UPDATE fence_recheck SET cleared_at={now},claim_owner=NULL WHERE rowid IN (SELECT rowid FROM fence_recheck ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "heal a clock"
        }
        20 => {
            rig.sql(format!("UPDATE fence_recheck SET quota=1-quota,classified=1 WHERE kind='credential' AND rowid IN (SELECT rowid FROM fence_recheck WHERE kind='credential' ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "classify a credential"
        }
        21 => {
            rig.sql(format!("INSERT OR IGNORE INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'{m}',evidence FROM fence_recheck WHERE kind IN ('provider','route') ORDER BY rowid LIMIT 1 OFFSET {offset};"));
            "partial route clear"
        }
        22 => {
            rig.sql("DELETE FROM fence_recheck_route_clear;".into());
            "delete every route clear"
        }
        23 => {
            rig.sql(format!("DELETE FROM fence_probe_authorization WHERE rowid IN (SELECT rowid FROM fence_probe_authorization ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "release an authorization"
        }
        24 => {
            rig.sql(format!("INSERT OR REPLACE INTO fence_success_pool VALUES ('f{offset}','[\"{m}\"]',{at},{now});"));
            "success pool"
        }
        25 => {
            let update = RouteStateUpdate {
                observed_at_ms: Some(at),
                cooldown_until: Some(until as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            };
            rig.store
                .record_provider_cooldown(p, &format!("{p}/{m}"), &format!("{p}/{m}"), &update)
                .unwrap();
            "record provider cooldown"
        }
        26 => {
            let update = RouteStateUpdate {
                observed_at_ms: Some(at),
                quota_exhausted_until: (rng.below(2) == 0).then_some(until as f64),
                latency_ema_ms: Some(10.0),
                increment_failure: rng.below(2) == 0,
                ..Default::default()
            };
            rig.store
                .update_route_state(&format!("{p}/{m}"), &format!("{p}/{m}"), &update)
                .unwrap();
            "update route state"
        }
        27 => {
            // A catalogue insert's trigger creates the route's state row
            // with any live provider wall.
            rig.store
                .upsert_fact(&fact(p, &format!("{m}-{}", rng.below(4))), FactSource::Seed)
                .unwrap();
            "catalogue insert"
        }
        28 => {
            rig.store
                .clear_route_cooldowns(&format!("{p}/{m}"))
                .unwrap();
            "operator clear"
        }
        _ => {
            let result = rig.store.account_command(AccountCommandKind::Maintenance, move |tx| {
                tx.pool_sql(|conn| {
                    conn.execute_batch(&format!("INSERT OR REPLACE INTO account_transient_backoff VALUES ('{p}','failed',{until},{at},'k','[]'); DELETE FROM fence_recheck WHERE rowid IN (SELECT rowid FROM fence_recheck LIMIT 1);"))
                })?;
                Err::<(), _>(err("planned failure"))
            });
            assert!(result.is_err());
            "failed command"
        }
    }
}

fn fact(provider: &str, model: &str) -> striatum_core::model_facts::ModelFact {
    use striatum_core::model_facts::{Confidence, ModelFact, ModelScores, ScoreProvenance};
    ModelFact {
        billing_plane: None,
        auth_methods: None,
        login_methods_documented: false,
        speed_is_fleet_median: false,
        decommissioned: false,
        provider_id: provider.into(),
        model_id: model.into(),
        variant: None,
        family: None,
        scores: ModelScores {
            elo: 55.0,
            eq: 1700.0,
            speed: 90.0,
        },
        context_window: Some(123_456.0),
        max_output_tokens: None,
        cost_tier: Some(2.0),
        cost_per_m_tok_out: Some(4.5),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(true),
        supports_tool_use: Some(true),
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::Curated,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: None,
        class_vector_is_overlay: false,
        upstream_status: None,
        model_type: None,
        model_kind: None,
    }
}

/// States the generated sequences must reach before their comparisons count.
#[derive(Default)]
struct Reached {
    credential_quota_from_hold: bool,
    route_hidden_by_provider: bool,
    quarantined_origin: bool,
    claimed_clock: bool,
    authorization: bool,
    partial_clear: bool,
    tombstone: bool,
    replaced_rowid: bool,
}

impl Reached {
    fn observe(&mut self, rig: &Rig) {
        rig.store
            .with_reader_for_test(|conn| {
                let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0));
                self.credential_quota_from_hold |=
                    count("SELECT count(*) FROM fence_origin WHERE kind='credential' AND quota=1")? > 0;
                self.route_hidden_by_provider |= count(
                    "SELECT count(*) FROM model_route_state r WHERE (cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL)
                     AND NOT EXISTS (SELECT 1 FROM fence_origin o WHERE o.kind='route' AND o.origin_key=json_array(r.route_key))",
                )? > 0;
                self.quarantined_origin |= !fence_rechecks::quarantined_origins(conn)?.is_empty();
                self.claimed_clock |=
                    count("SELECT count(*) FROM fence_recheck WHERE last_claimed_at IS NOT NULL AND claim_generation > 0")? > 0;
                self.authorization |= count("SELECT count(*) FROM fence_probe_authorization")? > 0;
                self.partial_clear |= count("SELECT count(*) FROM fence_recheck_route_clear")? > 0;
                self.tombstone |=
                    count("SELECT count(*) FROM fence_recheck WHERE cleared_at IS NOT NULL")? > 0;
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn generated_block_writes_keep_memory_equal_to_a_fresh_load() {
    let mut operations = std::collections::BTreeMap::<&str, usize>::new();
    let mut reached = Reached::default();
    for seed in 1..=8u64 {
        let rig = rig(&format!("block-state-property-{seed}"));
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ seed.wrapping_mul(0x2545_f491_4f6c_dd1d));
        let mut now = NOW;
        for index in 0..140 {
            let rowids_before: Vec<i64> = rig
                .store
                .with_reader_for_test(|conn| {
                    conn.prepare("SELECT rowid FROM credential_entitlement_fences")?
                        .query_map([], |r| r.get(0))?
                        .collect()
                })
                .unwrap();
            let operation = step(&rig, &mut rng, now);
            *operations.entry(operation).or_default() += 1;
            if operation == "replace credential fence" && !rowids_before.is_empty() {
                reached.replaced_rowid = true;
            }
            reached.observe(&rig);
            let context = format!("seed={seed} step={index} op={operation}");
            rig.check(&[now - 3_600_000, now, now + 3_600_000], &context);
            now += rng.below(3) as i64 * 600_000;
        }
        assert_eq!(rig.store.block_writes_outside_command(), 0, "seed={seed}");
    }
    // Coverage first: a comparison over sequences that never reached these
    // states would prove nothing about them.
    assert_eq!(operations.len(), 29, "every operation ran: {operations:?}");
    let Reached {
        credential_quota_from_hold,
        route_hidden_by_provider,
        quarantined_origin,
        claimed_clock,
        authorization,
        partial_clear,
        tombstone,
        replaced_rowid,
    } = reached;
    assert!(
        credential_quota_from_hold,
        "a credential classified by an active hold"
    );
    assert!(
        route_hidden_by_provider,
        "a route wall hidden by its provider's row"
    );
    assert!(quarantined_origin, "a quarantined route or provider row");
    assert!(claimed_clock, "a claimed clock");
    assert!(authorization, "a probe authorization");
    assert!(partial_clear, "a partial route clear");
    assert!(tombstone, "a cleared clock");
    assert!(replaced_rowid, "a key moved to a new rowid");
}
