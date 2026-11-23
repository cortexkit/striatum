//! The block state must equal a fresh load after every command, blocks
//! derived from it must equal the SQL adapter's at every `now`, and every
//! request reader's answer from memory must equal its SQL answer.

use super::*;
use crate::fence_rechecks::{self, FenceRecheck};
use crate::{
    err, AccountCommandKind, AccountStateBridge, AccountWorkingState, SqliteRouteStateStore,
};
use rusqlite::{types::Value, OptionalExtension};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use striatum_core::route_state_store::{FactSource, RouteStateStore, RouteStateUpdate};
use striatum_core::RouteStateResult;

#[cfg(test)]
#[path = "native_generator/tests.rs"]
mod native_generator;
#[cfg(test)]
#[path = "native_route_generator/tests.rs"]
mod native_route_generator;
#[cfg(test)]
#[path = "native_sql/tests.rs"]
mod native_sql;

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
    fn blocks(&self) -> Option<&BlockState> {
        Some(&self.0)
    }
}

/// A publisher holding only the block state, standing in for the module's.
#[derive(Default)]
struct TestOwner {
    published: Mutex<Option<Arc<TestState>>>,
    reload: AtomicBool,
    loads: std::sync::atomic::AtomicU64,
    /// States the next loads return instead of reading SQL, last first: a
    /// stale state stands in for one that drifted from SQL.
    stale: Mutex<Vec<BlockState>>,
    renewal_witnesses: Mutex<native_generator::Witnesses>,
}

impl AccountStateBridge for TestOwner {
    fn load(&self, tx: &crate::AccountTx<'_>) -> RouteStateResult<Box<dyn AccountWorkingState>> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        if let Some(stale) = self.stale.lock().unwrap().pop() {
            return Ok(Box::new(TestState(stale)));
        }
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

pub(crate) struct Rig {
    pub(crate) store: SqliteRouteStateStore,
    owner: Arc<TestOwner>,
    // Dropped last, after the store has closed its files.
    _scratch: cortexkit_test_support::ScratchDir,
}

fn rig(name: &str) -> Rig {
    make_rig(name, None)
}

pub(crate) fn rig_before_block_migration(name: &str) -> Rig {
    make_rig(name, Some(93))
}

fn make_rig(name: &str, version: Option<u32>) -> Rig {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let descriptor = crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy());
    let store = if let Some(version) = version {
        SqliteRouteStateStore::open_at_version_for_test(&descriptor, version)
    } else {
        SqliteRouteStateStore::open(&descriptor)
    }
    .unwrap();
    let owner = Arc::new(TestOwner::default());
    store.install_account_state_bridge(owner.clone()).unwrap();
    Rig {
        store,
        owner,
        _scratch: scratch,
    }
}

fn observation_source_sql(account: &str) -> String {
    let account = account.replace('\'', "''");
    // This raw stub stores an evidence key and a JSON-text effects field, not
    // an observation receipt. Its non-JSON key therefore names no observation.
    format!(
        "INSERT INTO block(kind,origin_key,provider,account_id,evidence,evidence_at,
           quota,deadline,next_recheck_at)
         VALUES ('observation',json_array('p','{account}'),'p','{account}',
           json_array('k','[]'),{NOW},0,{until},{until});",
        until = NOW + 60_000,
    )
}

impl Rig {
    pub(crate) fn reload(&self) {
        self.owner.request_reload();
        self.store
            .account_command(AccountCommandKind::Maintenance, |_| Ok(()))
            .unwrap();
    }

    pub(crate) fn published(&self) -> BlockState {
        self.owner
            .published
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .clone()
    }

    pub(crate) fn fresh(&self) -> BlockState {
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

    /// Native structural parity does not compare against the frozen legacy
    /// view: it checks every held row and its physical rowid fingerprint.
    fn check_native(&self, context: &str) {
        let memory = self.published();
        assert_eq!(
            memory,
            self.fresh(),
            "memory equals a fresh load: {context}"
        );
        let stored = self.store.with_reader_for_test(sql_fingerprints).unwrap();
        assert_eq!(
            memory.fingerprints(),
            stored,
            "held native counts and rowids: {context}"
        );
    }

    /// Memory equals a fresh load, its source origins equal an independent SQL
    /// projection, and its blocks equal the SQL adapter's at each time.
    fn check(&self, times: &[i64], context: &str) {
        let memory = self.published();
        assert_eq!(
            memory,
            self.fresh(),
            "memory equals a fresh load: {context}"
        );
        let view = self
            .store
            .with_reader_for_test(native_sql::origins)
            .unwrap();
        assert_eq!(memory.view_origins(), view, "view rows: {context}");
        let stored = self.store.with_reader_for_test(sql_fingerprints).unwrap();
        assert_eq!(
            memory.fingerprints(),
            stored,
            "the periodic reconcile's counts and rowids: {context}"
        );
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
            let effective = self
                .store
                .with_reader_for_test(move |conn| fence_rechecks::read_effective_at(conn, now))
                .unwrap();
            assert_eq!(
                memory.fence_rechecks(now).unwrap(),
                effective,
                "fence_rechecks at {now}, in order: {context}"
            );
        }
        // Compare every reader at each time: a memory reader that ignored a
        // `now` its SQL reader filters by would differ from it at one of them.
        for &now in times {
            self.check_readers(&memory, now, &format!("{context} at {now}"));
        }
        self.check_route_state(&memory, times, context);
    }

    /// The route-state snapshot and `route.cooldown_status` answer from
    /// memory exactly as from SQL, for a request at each time against probe
    /// clocks evaluated at each time (the SQL reader evaluates them at the
    /// wall clock, which a request's `now` need not equal).
    fn check_route_state(&self, memory: &BlockState, times: &[i64], context: &str) {
        for &now in times {
            for &recheck in times {
                let at = format!("snapshot at {now}, clocks at {recheck}: {context}");
                let derived = memory.route_state_snapshot(now as f64, recheck).unwrap();
                let sql = self
                    .store
                    .with_reader_for_test(move |conn| {
                        Ok(crate::route_snapshot::read_snapshot(
                            conn, now as f64, recheck,
                        ))
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(derived, sql, "{at}");
                let mut keys: Vec<(&String, &String)> = sql
                    .by_route
                    .keys()
                    .map(|route| (route, route))
                    .chain(sql.by_capacity.keys().map(|capacity| (capacity, capacity)))
                    .collect();
                // A variant route asked with its capacity's key.
                for route in sql.by_route.keys() {
                    if let Some(capacity) = sql.by_capacity.keys().find(|capacity| {
                        route.len() > capacity.len() && route.starts_with(capacity.as_str())
                    }) {
                        keys.push((route, capacity));
                    }
                }
                for (route, capacity) in keys {
                    let provider = striatum_core::derive_provider_key(route).unwrap_or("");
                    let from_memory = memory.provider_cooldown(provider).unwrap();
                    let from_sql = self.store.get_provider_cooldown(provider).unwrap();
                    assert_eq!(from_memory, from_sql, "provider {provider}: {at}");
                    let cooled = striatum_core::route_state_store::route_cooled(
                        from_memory.as_ref(),
                        &derived,
                        route,
                        capacity,
                        now as f64,
                    );
                    assert_eq!(
                        cooled,
                        striatum_core::route_state_store::route_cooled(
                            from_sql.as_ref(),
                            &sql,
                            route,
                            capacity,
                            now as f64,
                        ),
                        "cooldown_status of {route} on {capacity}: {at}"
                    );
                    COOLED.with(|seen| seen.borrow_mut().insert(cooled));
                }
            }
        }
        for provider in PROVIDERS {
            assert_eq!(
                memory.provider_cooldown(provider).unwrap(),
                self.store.get_provider_cooldown(provider).unwrap(),
                "provider {provider}: {context}"
            );
        }
    }

    /// Every request reader of the block tables answers from memory exactly
    /// as its SQL does, order included.
    fn check_readers(&self, memory: &BlockState, now: i64, context: &str) {
        let store = &self.store;
        assert_eq!(
            memory.eligibility(),
            store.account_eligibility().unwrap(),
            "eligibility: {context}"
        );
        assert_eq!(
            memory.size_refusals(),
            store.account_size_refusals(now).unwrap(),
            "size refusals: {context}"
        );
        assert_eq!(
            memory.route_fences("quota"),
            store.account_route_fences(now).unwrap(),
            "quota route fences: {context}"
        );
        assert_eq!(
            memory.route_fences("transient"),
            store.transient_account_route_fences(now).unwrap(),
            "transient route fences: {context}"
        );
        assert_eq!(
            memory.route_fences("billing"),
            store.non_quota_account_route_fences(now).unwrap(),
            "billing route fences: {context}"
        );
        assert_eq!(
            memory.route_fence_latest_reports(),
            store.account_route_fence_latest_reports().unwrap(),
            "latest route fence reports: {context}"
        );
        assert_eq!(
            memory.entitlement_fences(),
            store.credential_entitlement_fences().unwrap(),
            "entitlement fences: {context}"
        );
        let sql = store
            .account_read(|tx| {
                let mut answers = Vec::new();
                for provider in PROVIDERS {
                    answers.push(format!(
                        "{provider} {:?} {:?}",
                        tx.active_fences(provider)?,
                        tx.active_pool_fences(provider)?
                    ));
                    for credential in CREDENTIALS {
                        answers.push(format!(
                            "{provider}/{credential} {:?} {:?} {:?}",
                            tx.credential_entitlement_fence(provider, credential)?,
                            tx.credential_quota_classification(provider, credential, false)?,
                            tx.credential_quota_classification(provider, credential, true)?,
                        ));
                    }
                }
                answers.push(format!("{:?}", tx.active_fence_providers()?));
                answers.push(format!("{:?}", tx.probe_authorization_owners()?));
                for admission in 0..4 {
                    answers.push(format!(
                        "{:?}",
                        tx.admission_exclusions("x", &format!("adm{admission}"))?
                    ));
                }
                for credential in CREDENTIALS {
                    answers.push(format!("{}", tx.exclusion_count("x", credential)?));
                }
                Ok(answers)
            })
            .unwrap();
        let mut answers = Vec::new();
        for provider in PROVIDERS {
            answers.push(format!(
                "{provider} {:?} {:?}",
                memory.active_fences(provider),
                memory.active_pool_fences(provider).unwrap()
            ));
            for credential in CREDENTIALS {
                answers.push(format!(
                    "{provider}/{credential} {:?} {:?} {:?}",
                    memory.entitlement_fence(provider, credential),
                    memory.credential_quota_classification(provider, credential, false),
                    memory.credential_quota_classification(provider, credential, true),
                ));
            }
        }
        answers.push(format!("{:?}", memory.active_fence_providers()));
        answers.push(format!("{:?}", memory.authorization_owners()));
        for admission in 0..4 {
            answers.push(format!(
                "{:?}",
                memory.admission_exclusions("x", &format!("adm{admission}"))
            ));
        }
        for credential in CREDENTIALS {
            answers.push(format!("{}", memory.exclusion_count("x", credential)));
        }
        assert_eq!(answers, sql, "keyed readers: {context}");
        // Pool key states, the stored clocks they read, and whether a sync
        // would change a clock.
        assert_eq!(
            memory.clock_rechecks(),
            store.with_reader_for_test(fence_rechecks::read).unwrap(),
            "stored clocks: {context}"
        );
        assert_eq!(
            memory.needs_sync(),
            store
                .with_reader_for_test(fence_rechecks::needs_sync)
                .unwrap(),
            "needs_sync: {context}"
        );
        for provider in PROVIDERS {
            let sql = store
                .with_reader_for_test(|conn| {
                    crate::key_pool::read_key_states(conn, provider, now, None)
                })
                .unwrap();
            let leases = store
                .with_reader_for_test(|conn| {
                    crate::key_pool::lease_counts(conn, provider, now, None)
                })
                .unwrap();
            let from_memory = crate::key_pool::key_states_from(
                provider,
                memory.pool_key_rows(provider).unwrap(),
                &memory.clock_rechecks(),
                true,
                leases,
            );
            assert_eq!(from_memory, sql, "{provider} key states: {context}");
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
    let times = seed_captured_native_parity_fixture(&rig, NOW);
    assert!(times.len() > 3);
    rig.check(&times, "parity fixture");
    assert!(block_read_backs() > 0);
}

/// Upgrade the legacy dataset, then insert its stored rows through one account
/// command on an initially empty destination. Writer hooks record the inserted
/// row ids and read those rows into working memory before publication. Loading
/// the populated source alone would not exercise that path. Coverage and
/// boundary times are still checked on the original legacy fixture.
fn seed_captured_native_parity_fixture(destination: &Rig, now: i64) -> Vec<i64> {
    let source = legacy_sync_rig("captured-parity-source");
    source.run(move |conn| crate::block_parity_fixture::seed_block_parity_fixture(conn, now));
    source
        .store
        .assert_block_parity_fixture_coverage_for_test(now);
    let times = source.store.block_parity_times(now).unwrap();
    let source = upgrade_sync_rig(source, "captured-parity-source");
    let tables = source
        .store
        .with_reader_for_test(|conn| {
            let mut tables = Vec::new();
            for table in BLOCK_TABLES {
                let mut columns = conn
                    .prepare("SELECT name FROM pragma_table_xinfo(?1) WHERE hidden=0 ORDER BY cid")?
                    .query_map([table], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                columns.insert(0, "rowid".into());
                let mut statement = conn.prepare(&format!(
                    "SELECT {} FROM {table} ORDER BY rowid",
                    columns.join(",")
                ))?;
                let rows = statement
                    .query_map([], |row| {
                        (0..columns.len())
                            .map(|column| row.get::<_, Value>(column))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                tables.push((table.to_owned(), columns, rows));
            }
            Ok(tables)
        })
        .unwrap();
    destination.run(move |conn| {
        for (table, columns, rows) in &tables {
            let placeholders = (1..=columns.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(",");
            let mut insert = conn.prepare(&format!(
                "INSERT OR REPLACE INTO {table}({}) VALUES ({placeholders})",
                columns.join(",")
            ))?;
            for row in rows {
                insert.execute(rusqlite::params_from_iter(row.iter()))?;
            }
        }
        Ok(())
    });
    destination
        .store
        .with_reader_for_test(|conn| {
            let kinds = conn
                .prepare("SELECT DISTINCT kind FROM block WHERE source_present=1 AND is_origin=1")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?;
            assert_eq!(
                kinds,
                striatum_core::blocks::SOURCE_KINDS
                    .into_iter()
                    .map(String::from)
                    .collect::<std::collections::BTreeSet<_>>(),
                "the captured dataset retains every source kind"
            );
            Ok(())
        })
        .unwrap();
    times
}

#[test]
fn a_replace_that_moves_a_key_to_a_new_rowid_leaves_no_stale_row() {
    let rig = rig("block-state-replace");
    let credential = |replace: &str, code: &str, recorded: i64| {
        format!(
            "INSERT {replace} INTO block(kind,origin_key,provider,credential_id,evidence,evidence_at,
              quota,next_recheck_at,observed_at,code)
             VALUES ('credential',json_array('p','c'),'p','c',json_array('{code}',NULL,{recorded}),
               {recorded},0,{recorded}+900000,{recorded},'{code}');"
        )
    };
    rig.sql(credential("", "billing_exhausted", NOW));
    let rowid = || {
        rig.store
            .with_reader_for_test(|conn| {
                conn.query_row("SELECT rowid FROM block WHERE kind='credential'", [], |r| {
                    r.get::<_, i64>(0)
                })
            })
            .unwrap()
    };
    let before = rowid();
    // SQLite reports the replacement insert, not the old row's deletion.
    rig.sql(credential("OR REPLACE", "model_unsupported", NOW + 1));
    let after = rowid();
    assert_ne!(before, after, "REPLACE gave the key a new rowid");
    assert_eq!(rig.published().entitlement_fences().len(), 1);
    rig.check_native("credential replaced");
    // An absent account source is not held, even when a replacement leaves
    // its cleared audit row in durable storage.
    let fence = |cleared: &str| {
        format!(
            "INSERT OR REPLACE INTO block(kind,origin_key,provider,account_id,evidence,evidence_at,
              quota,next_recheck_at,observed_at,created_at,cleared_at,cleared_by,source_present,recovery_set_json,source_metadata_json)
             VALUES ('account',json_array('f'),'p','a','{{}}',{NOW},1,{NOW}+900000,
               {NOW},{NOW},{cleared},CASE WHEN {cleared} IS NULL THEN NULL ELSE 'kept' END,
               {cleared} IS NULL,'[]',
               '{{\"scope_json\":\"{{}}\",\"fence_json\":\"{{}}\",\"source\":\"broca_account_limit\",\"principal\":\"x\",\"admission_id\":null}}');"
        )
    };
    rig.sql(fence("NULL"));
    assert_eq!(rig.published().active_fences("p").len(), 1);
    rig.sql(fence(&NOW.to_string()));
    assert_eq!(rig.published().active_fences("p").len(), 0);
    rig.check_native("active fence replaced by a cleared one");
    // A non-quota account-route source remains a held record, not an origin.
    let route = |replace: &str, class: &str| {
        format!(
            "INSERT {replace} INTO block(kind,origin_key,provider,credential_id,model,evidence,
              evidence_at,quota,next_recheck_at,deadline,recorded_at,last_reported_at,wall_class,source_metadata_json)
             VALUES ('account_route',json_array('p','c','m'),'p','c','m','route-evidence',
               {NOW},1,{NOW}+60000,{NOW}+60000,{NOW},{NOW},'{class}','{{\"decision_id\":\"d\"}}');"
        )
    };
    rig.sql(route("", "quota"));
    rig.sql(route("OR REPLACE", "transient"));
    let state = rig.published();
    let record = state.source_record("account_route", r#"["p","c","m"]"#);
    assert_eq!(record.iter().count(), 1);
    assert!(!record.unwrap().is_origin());
    rig.check_native("route fence replaced by a transient row");
}

/// Live pool-key metadata is held even without a wall. Replacing it moves
/// the physical rowid but keeps exactly one held row for the same key.
#[test]
fn a_replaced_pool_key_row_is_held_once_at_its_new_rowid() {
    let rig = rig("block-state-pool-replace");
    rig.sql(format!(
        "INSERT INTO pool_key_state(provider,credential_id,updated_at) VALUES ('p','c',{NOW});
         INSERT INTO block(kind,origin_key,provider,credential_id,evidence,evidence_at,quota,
           next_recheck_at,deadline,reason,observed_at)
         VALUES ('pool_billing',json_array('p','c'),'p','c','billing-evidence',{NOW},0,
           {NOW}+60000,{NOW}+60000,'billing',{NOW});"
    ));
    let rowid = |rig: &Rig| -> i64 {
        rig.store
            .with_reader_for_test(|conn| {
                conn.query_row("SELECT rowid FROM pool_key_state", [], |r| r.get(0))
            })
            .unwrap()
    };
    let before = rowid(&rig);
    rig.sql(format!(
        "INSERT OR REPLACE INTO pool_key_state(provider,credential_id,last_used_at,updated_at) VALUES ('p','c',{NOW},{NOW});
         UPDATE block SET source_present=0,cleared_at={NOW},cleared_by='removed'
           WHERE kind='pool_billing' AND origin_key=json_array('p','c');"
    ));
    let after = rowid(&rig);
    assert_ne!(before, after, "REPLACE gave the key a new rowid");
    let state = rig.published();
    assert_eq!(state.table_sizes()[4], ("pool_key_state", 1));
    assert_eq!(
        state.pool_key_rows("p").unwrap(),
        vec![("c".to_owned(), Some(NOW), None, None)],
        "the held row is the replacement"
    );
    assert!(
        state.fingerprints()[4] == (1, Some(after)),
        "held at its new rowid"
    );
    rig.check_native("pool key replaced");
}

/// The update hook reports a REPLACE's insert but not the delete of the row
/// it evicts, which the state finds through the new row's key. If the new row
/// were deleted or rekeyed later in the same command, nothing would name the
/// evicted row and memory would keep it. A REPLACE policy also reaches block
/// tables indirectly: SQLite applies the conflict policy of a statement to
/// every statement of the triggers it fires. So no production code or schema
/// resolves a conflict by REPLACE, on any table: no `OR REPLACE`, no
/// `REPLACE INTO`, no `ON CONFLICT REPLACE`, in any letter case, in any
/// non-test `.rs` or `.sql` file of any crate. Test files, and modules
/// declared as `#[cfg(test)] mod name;`, may use them to exercise the
/// capture. The only files skipped by name are migrations 054 and 056,
/// whose REPLACE triggers migration 092 drops and recreates; the schema of
/// a migrated store is checked as well.
#[test]
fn no_production_code_or_schema_resolves_a_conflict_by_replace() {
    use std::path::{Path, PathBuf};
    const SUPERSEDED: [&str; 2] = ["054_parent_failure_index.sql", "056_restorable_history.sql"];
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    fn files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name != "target") {
                    files(&path, out);
                }
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("rs" | "sql")
            ) {
                out.push(path);
            }
        }
    }
    let mut all = Vec::new();
    for krate in std::fs::read_dir(&crates).unwrap() {
        let krate = krate.unwrap().path();
        if krate.is_dir() {
            files(&krate, &mut all);
        }
    }
    // Modules declared `#[cfg(test)] mod name;`, as files `name.rs` or
    // `name/` beside the declaring module (or inside its directory).
    let mut test_only: Vec<PathBuf> = Vec::new();
    for path in &all {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        for pair in lines.windows(2) {
            let declared = ["mod ", "pub mod ", "pub(crate) mod ", "pub(super) mod "]
                .iter()
                .find_map(|prefix| pair[1].strip_prefix(prefix))
                .and_then(|rest| rest.strip_suffix(';'));
            if let (true, Some(name)) = (pair[0] == "#[cfg(test)]", declared) {
                let dir = path.parent().unwrap();
                let base = if path
                    .file_stem()
                    .is_some_and(|stem| stem == "lib" || stem == "main" || stem == "mod")
                {
                    dir.to_path_buf()
                } else {
                    dir.join(path.file_stem().unwrap())
                };
                test_only.push(base.join(format!("{name}.rs")));
                test_only.push(base.join(name));
            }
        }
    }
    let is_test = |path: &Path| {
        let text = path.to_string_lossy();
        text.contains("/tests/")
            || text.ends_with("tests.rs")
            || text.contains("_tests/")
            || test_only.iter().any(|prefix| path.starts_with(prefix))
    };
    // SQL comments end a word as whitespace does: `OR/**/REPLACE` and
    // `OR -- note` followed by `REPLACE` on the next line are both REPLACE.
    let uncomment = |text: &str| -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while !rest.is_empty() {
            if let Some(after) = rest.strip_prefix("/*") {
                rest = after.find("*/").map_or("", |end| &after[end + 2..]);
                out.push(' ');
            } else if let Some(after) = rest.strip_prefix("--") {
                // A comment ends at a newline, or at a `\n` escape inside
                // a Rust string literal.
                let end = [after.find('\n'), after.find("\\n")]
                    .into_iter()
                    .flatten()
                    .min();
                rest = end.map_or("", |end| &after[end..]);
                out.push(' ');
            } else {
                let next = rest.chars().next().unwrap();
                out.push(next);
                rest = &rest[next.len_utf8()..];
            }
        }
        out
    };
    // Both the text as written and the text without comments are searched,
    // so removing comments (which may pair a `/*` and `*/` from unrelated
    // strings) can only find more, never hide what the plain text shows.
    let scan = |text: &str| -> Vec<&'static str> {
        let normals: Vec<String> = [text.to_owned(), uncomment(text)]
            .iter()
            .map(|text| {
                text.to_ascii_lowercase()
                    .replace(['"', '`', '\\', '\''], " ")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        ["or replace", "replace into", "on conflict replace"]
            .into_iter()
            .filter(|form| {
                normals.iter().any(|normal| {
                    normal.match_indices(form).any(|(at, _)| {
                        let before = normal[..at].chars().next_back();
                        let after = normal[at + form.len()..].chars().next();
                        !before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                            && !after.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                    })
                })
            })
            .collect()
    };
    // The scan sees every spelling and every table.
    assert_eq!(
        scan("x(\"INSERT  OR\n   Replace INTO `fence_recheck`(kind) VALUES (1)\")"),
        ["or replace", "replace into"]
    );
    assert_eq!(scan("replace into anything values (1)"), ["replace into"]);
    assert_eq!(
        scan("UPDATE OR REPLACE pool_key_state SET a=1"),
        ["or replace"]
    );
    assert_eq!(
        scan("CREATE TABLE t (a TEXT UNIQUE ON CONFLICT REPLACE);"),
        ["on conflict replace"]
    );
    assert!(scan("let s = text.replace('a', \"b\"); color_replace").is_empty());
    assert_eq!(scan("INSERT OR/**/REPLACE t VALUES (1)"), ["or replace"]);
    assert_eq!(
        scan("INSERT OR -- keep the newest\n  REPLACE t VALUES (1)"),
        ["or replace"]
    );
    assert_eq!(
        scan("x(\"INSERT OR \\\n         REPLACE t VALUES (1)\")"),
        ["or replace"]
    );
    assert_eq!(
        scan("x(\"-- the newest\\nINSERT OR REPLACE t VALUES (1)\"); // --"),
        ["or replace"]
    );
    assert_eq!(
        scan("-- INSERT OR REPLACE, even commented out\nSELECT 1"),
        ["or replace"]
    );
    let mut violations = Vec::new();
    let mut scanned = 0;
    for path in all.iter().filter(|path| !is_test(path)) {
        if path
            .file_name()
            .is_some_and(|name| SUPERSEDED.iter().any(|superseded| name == *superseded))
        {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        scanned += 1;
        violations.extend(
            scan(&text)
                .into_iter()
                .map(|found| format!("{}: {found}", path.display())),
        );
    }
    assert!(scanned > 100, "the scan found the sources: {scanned} files");
    assert!(
        violations.is_empty(),
        "REPLACE conflict resolution: {violations:#?}"
    );

    // The migrated schema: no trigger or table resolves a conflict by
    // REPLACE, and the two recreated triggers' target tables have no
    // triggers of their own.
    let rig = rig("block-state-schema-without-replace");
    let (schema, own_triggers) = rig
        .store
        .with_reader_for_test(|conn| {
            let schema = conn
                .prepare("SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL")?
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let own: i64 = conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='trigger'
                 AND tbl_name IN ('model_route_parent_failures','account_observation_event','fence_retired_origin')
                 AND name!='fence_retire_enforcement'",
                [],
                |r| r.get(0),
            )?;
            Ok((schema, own))
        })
        .unwrap();
    assert!(
        schema.len() > 50,
        "the schema was read: {} objects",
        schema.len()
    );
    let replacing: Vec<_> = schema
        .iter()
        .filter(|(_, sql)| !scan(sql).is_empty())
        .map(|(name, _)| name)
        .collect();
    assert!(
        replacing.is_empty(),
        "schema objects resolving a conflict by REPLACE: {replacing:?}"
    );
    assert_eq!(
        own_triggers, 0,
        "no trigger on the copies' target tables or on the archive besides retirement"
    );
}

/// Migration 092's copy triggers, which update a conflicting row, leave the
/// same rows as migrations 054 and 056's, which replaced it: each insert
/// names every column of its target table.
#[test]
fn copy_triggers_without_replace_leave_the_rows_the_replace_left() {
    let old_054 = include_str!("../../migrations/054_parent_failure_index.sql");
    let old_056 = include_str!("../../migrations/056_restorable_history.sql");
    let new = include_str!("../../migrations/092_triggers_without_replace.sql");
    fn statement(sql: &str, start: &str, end: &str) -> String {
        let at = sql.find(start).unwrap_or_else(|| panic!("{start}"));
        let length = sql[at..].find(end).unwrap() + end.len();
        sql[at..at + length].to_owned()
    }
    let run = |triggers: [String; 2]| -> Vec<String> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE model_route_decisions (id TEXT PRIMARY KEY, parent_session_id TEXT,
               outcome_at INTEGER, selected_route_key TEXT, selected_provider_id TEXT, outcome_error TEXT);
             CREATE TABLE account_observation (principal TEXT, runner TEXT, event_id TEXT,
               observation_id TEXT, bound INTEGER, effects_json TEXT, received_at INTEGER);",
        )
        .unwrap();
        conn.execute_batch(&statement(
            old_054,
            "CREATE TABLE IF NOT EXISTS model_route_parent_failures",
            ");",
        ))
        .unwrap();
        conn.execute_batch(&statement(
            old_056,
            "CREATE TABLE IF NOT EXISTS account_observation_event",
            ");",
        ))
        .unwrap();
        for trigger in triggers {
            conn.execute_batch(&trigger).unwrap();
        }
        // Each table gets a plain copy and a copy over a leftover row with
        // the same key; one decision has no parent session and no copy.
        conn.execute_batch(
            "INSERT INTO model_route_decisions VALUES ('d1','s1',1,'r1','p1','e1');
             INSERT INTO model_route_parent_failures VALUES ('d2','stale',0,'old','old','old');
             INSERT INTO model_route_decisions VALUES ('d2','s2',2,'r2',NULL,'e2');
             INSERT INTO model_route_decisions VALUES ('d3',NULL,3,'r3','p3','e3');
             INSERT INTO account_observation VALUES ('p','r','e1','o1',1,'[]',10);
             INSERT INTO account_observation_event VALUES ('p','r','e2','old',0,'{}',1);
             INSERT INTO account_observation VALUES ('p','r','e2','o2',1,'[1]',20);",
        )
        .unwrap();
        let mut rows = Vec::new();
        for sql in [
            "SELECT * FROM model_route_parent_failures ORDER BY decision_id",
            "SELECT * FROM account_observation_event ORDER BY principal,runner,event_id",
        ] {
            let mut statement = conn.prepare(sql).unwrap();
            let columns = statement.column_count();
            let mut query = statement.query([]).unwrap();
            while let Some(row) = query.next().unwrap() {
                rows.push(format!(
                    "{:?}",
                    (0..columns)
                        .map(|at| row.get::<_, Value>(at).unwrap())
                        .collect::<Vec<_>>()
                ));
            }
        }
        rows
    };
    let replaced = run([
        statement(
            old_054,
            "CREATE TRIGGER IF NOT EXISTS model_route_parent_failures_on_insert",
            "END;",
        ),
        statement(
            old_056,
            "CREATE TRIGGER IF NOT EXISTS account_observation_event_on_insert",
            "END;",
        ),
    ]);
    let updated = run([
        statement(
            new,
            "CREATE TRIGGER model_route_parent_failures_on_insert",
            "END;",
        ),
        statement(
            new,
            "CREATE TRIGGER account_observation_event_on_insert",
            "END;",
        ),
    ]);
    assert_eq!(updated, replaced);
    assert_eq!(replaced.len(), 4, "{replaced:#?}");
    assert!(
        replaced.iter().any(|row| row.contains("\"s2\"")),
        "{replaced:#?}"
    );
    assert!(
        replaced.iter().any(|row| row.contains("\"o2\"")),
        "{replaced:#?}"
    );
}

#[test]
fn a_whole_table_delete_is_captured() {
    let rig = rig("block-state-truncate");
    rig.sql(format!(
        "{}{}",
        observation_source_sql("a"),
        observation_source_sql("b"),
    ));
    assert_eq!(rig.published().source_count_for_test("observation"), 2);
    // Without a WHERE clause SQLite may drop every row at once without
    // reporting each; the writer connection's authorizer prevents that.
    rig.sql("DELETE FROM block;".into());
    assert_eq!(rig.published().source_count_for_test("observation"), 0);
    rig.check(&[NOW], "after a whole-table delete");
}

#[test]
fn a_failed_command_leaves_no_captured_row() {
    let rig = rig("block-state-rollback");
    let result = rig
        .store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.pool_sql(|conn| conn.execute_batch(&observation_source_sql("a")))?;
            Err::<(), _>(err("planned failure after a block write"))
        });
    assert!(result.is_err());
    assert_eq!(rig.published().source_count_for_test("observation"), 0);
    rig.check(&[NOW], "after a failed command");
}

#[test]
fn a_write_outside_a_command_reloads_the_state_before_the_next_batch() {
    let rig = rig("block-state-outside");
    let row = observation_source_sql;
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
        rig.published().source_count_for_test("observation"),
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
    assert_eq!(tables[0].0, "block");
    rig.sql("SELECT 1;".into());
    assert_eq!(
        rig.owner.loads.load(Ordering::Relaxed),
        loads + 2,
        "one more reload"
    );
    rig.check(&[NOW], "after a bypass and a reload");
}

#[test]
fn a_claim_decided_on_a_stale_state_runs_again_on_a_fresh_load() {
    let rig = rig("block-state-stale-claim");
    // A provider wall observed two hours ago: its one-hour probe is due.
    rig.store
        .record_provider_cooldown(
            "p0",
            "p0/m0",
            "p0/m0",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW - 2 * 3_600_000),
                cooldown_until: Some((NOW + 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    rig.store
        .account_transaction(|tx| fence_rechecks::sync_on(tx, NOW).map_err(err))
        .unwrap();
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    assert_eq!(origins.len(), 1, "{origins:?}");
    let before_the_winner = rig.published();
    let claim = |owner: &'static str| {
        let origins = origins.clone();
        rig.store
            .account_command(AccountCommandKind::Maintenance, move |tx| {
                tx.grant_fence_rechecks(&origins, NOW, Some(owner))
            })
    };
    assert!(claim("winner").unwrap());

    // The loser's batch first decides on the state from before the winner
    // claimed: the clock looks due, but SQL holds the winner's claim. The
    // keyed update finds no such row, the batch rolls back, and it runs again
    // on a fresh load, which refuses the loser.
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.owner
        .stale
        .lock()
        .unwrap()
        .push(before_the_winner.clone());
    rig.owner.request_reload();
    assert!(
        !claim("loser").unwrap(),
        "the rerun sees the winner's claim"
    );
    assert_eq!(rig.owner.loads.load(Ordering::Relaxed) - loads, 2);
    let owners = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.prepare("SELECT claim_owner FROM block WHERE managed=1 AND is_origin=1 UNION ALL SELECT owner FROM probe_authorization")?
                    .query_map([], |r| r.get::<_, Option<String>>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
    };
    assert_eq!(
        owners(&rig),
        [Some("winner".to_owned()), Some("winner".to_owned())]
    );

    // Stale on the rerun too: the batch fails with the divergence and
    // writes nothing.
    rig.owner
        .stale
        .lock()
        .unwrap()
        .extend([before_the_winner.clone(), before_the_winner]);
    rig.owner.request_reload();
    let error = claim("loser").unwrap_err().to_string();
    assert!(error.contains(crate::STATE_DIVERGED), "{error}");
    assert_eq!(
        owners(&rig),
        [Some("winner".to_owned()), Some("winner".to_owned())]
    );
}

/// A success heals only blocks older than its admission, so a refusal
/// recorded after the admission survives it. Here the newer refusal reaches
/// SQL after the state the heal first decides on: that state still shows the
/// older refusal, so the keyed delete finds no such row, the batch rolls
/// back, and it runs again on a fresh load, which keeps the newer refusal.
#[test]
fn a_heal_decided_on_a_stale_state_runs_again_on_a_fresh_load() {
    let rig = rig("block-state-stale-heal");
    let until = NOW + 3_600_000;
    let older = NOW - 600_000;
    let newer = NOW + 600_000;
    rig.store
        .record_account_route_fence(&crate::AccountRouteFence {
            provider: "p0".into(),
            credential_id: "c0".into(),
            model: "m0".into(),
            until_ms: until,
            decision_id: "d".into(),
            recorded_at: older,
        })
        .unwrap();
    let before_the_refusal = rig.published();
    rig.sql(format!(
        "UPDATE block SET last_reported_at={newer} WHERE kind='account_route' AND credential_id='c0';"
    ));
    let heal = || {
        rig.store.account_command(AccountCommandKind::Report, |tx| {
            tx.heal_serving_fences_in_scope("p0", "a0", "c0", "m0", None, NOW, NOW + 1, None)
        })
    };
    let reported = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.prepare("SELECT last_reported_at FROM block WHERE kind='account_route'")?
                    .query_map([], |r| r.get::<_, i64>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
    };
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.owner
        .stale
        .lock()
        .unwrap()
        .push(before_the_refusal.clone());
    rig.owner.request_reload();
    heal().unwrap();
    assert_eq!(
        rig.owner.loads.load(Ordering::Relaxed) - loads,
        2,
        "the stale run, then a fresh one"
    );
    assert_eq!(
        reported(&rig),
        [newer],
        "the refusal after the admission stands"
    );
    rig.check(&[NOW], "after the rerun");

    // Stale on the rerun too: the batch fails with the divergence and
    // writes nothing.
    rig.owner
        .stale
        .lock()
        .unwrap()
        .extend([before_the_refusal.clone(), before_the_refusal]);
    rig.owner.request_reload();
    let error = heal().unwrap_err().to_string();
    assert!(error.contains(crate::STATE_DIVERGED), "{error}");
    assert_eq!(reported(&rig), [newer]);
}

/// A success clears only blocks of the account and login that served it and
/// older than its admission. A whole-account block on a pool clears once the
/// success's scope covers its pool; a block on a pool the scope does not
/// cover stays.
#[test]
fn a_success_heals_only_its_serving_login_and_evidence_older_than_its_admission() {
    use striatum_core::accounts::{PoolKey, PoolScope};
    let rig = rig("block-state-heal-scope");
    let until = NOW + 3_600_000;
    let older = NOW - 600_000;
    let newer = NOW + 600_000;
    let shared = pool_key(5);
    let named = serde_json::to_string(&PoolKey {
        window_minutes: Some(300),
        scope: PoolScope::EveryModel,
        name: Some("gemini".into()),
    })
    .unwrap();
    rig.run(move |conn| {
        for (credential, model) in [("c0", "m0"), ("c1", "m0"), ("c0", "m1")] {
            let row = crate::block_storage::account_route_source(
                &crate::AccountRouteFence {
                    provider: "p0".into(),
                    credential_id: credential.into(),
                    model: model.into(),
                    until_ms: until,
                    decision_id: "d".into(),
                    recorded_at: older,
                },
                "account_route",
                "quota",
                Some(older),
            )?;
            crate::block_storage::write_source(conn, &row)?;
        }
        for (credential, observed_at) in [("c0", newer), ("c1", older)] {
            let row = crate::block_storage::credential_source(
                &striatum_core::route_state_store::CredentialEntitlementFence {
                    provider_id: "p0".into(),
                    credential_id: credential.into(),
                    code: "billing_exhausted".into(),
                    listing_fingerprint: None,
                    observed_at_ms: observed_at,
                    console_credit_micros_at_refusal: None,
                },
            )?;
            crate::block_storage::write_source(conn, &row)?;
        }
        for (id, account, pool) in [
            ("shared", "a0", &shared),
            ("named", "a0", &named),
            ("other-account", "a1", &shared),
        ] {
            let row = crate::block_storage::account_source(
                conn,
                &crate::AccountPoolFenceRow {
                    evidence: crate::AccountFenceRow {
                        fence_id: id.into(),
                        provider: "p0".into(),
                        account_id: account.into(),
                        scope_json: "{}".into(),
                        fence_json: "{}".into(),
                        source: "broca_account_limit".into(),
                        observed_at: older,
                        principal: "x".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: older,
                        cleared_at: None,
                        cleared_reason: None,
                    },
                    pool_key: Some(serde_json::from_str(pool).unwrap()),
                    recovery_set: Vec::new(),
                },
            )?;
            crate::block_storage::write_source(conn, &row)?;
        }
        Ok(())
    });
    rig.store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.heal_serving_fences_in_scope(
                "p0",
                "a0",
                "c0",
                "m0",
                None,
                NOW,
                NOW + 1,
                Some(&crate::QuotaSuccessScope {
                    family: striatum_core::accounts::ModelFamily::Other,
                    pool: None,
                }),
            )
        })
        .unwrap();
    let remaining = rig
        .store
        .with_reader_for_test(|conn| {
            let list = |sql: &str| -> rusqlite::Result<Vec<String>> {
                conn.prepare(sql)?.query_map([], |r| r.get(0))?.collect()
            };
            Ok((
                list("SELECT credential_id||'/'||model FROM block WHERE kind='account_route' AND source_present=1 ORDER BY 1")?,
                list("SELECT credential_id FROM block WHERE kind='credential' AND source_present=1 ORDER BY 1")?,
                list("SELECT json_extract(origin_key,'$[0]') FROM block WHERE kind='account' AND source_present=1 AND cleared_at IS NULL ORDER BY 1")?,
            ))
        })
        .unwrap();
    assert_eq!(
        remaining,
        (
            vec!["c0/m1".to_owned(), "c1/m0".to_owned()],
            vec!["c0".to_owned(), "c1".to_owned()],
            vec!["named".to_owned(), "other-account".to_owned()],
        ),
        "only the serving login's route, and its account's block on the covered pool, heal"
    );
    rig.check(&[NOW], "after the heal");
}

/// A refusal of credential `c` on provider `p0`, two hours old, whose
/// one-hour probe is due at `NOW`, with its clock stored.
fn due_credential_refusal(rig: &Rig, credential: &str) {
    rig.store
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "p0".into(),
                credential_id: credential.into(),
                code: "billing_exhausted".into(),
            },
            None,
            NOW - 2 * 3_600_000,
        )
        .unwrap();
    rig.store
        .account_transaction(|tx| fence_rechecks::sync_on(tx, NOW).map_err(err))
        .unwrap();
}

impl Rig {
    /// Make the next batch decide on `stale` instead of a fresh load, and
    /// return the load count before it.
    fn decide_next_on(&self, stale: BlockState) -> u64 {
        let loads = self.owner.loads.load(Ordering::Relaxed);
        self.owner.stale.lock().unwrap().push(stale);
        self.owner.request_reload();
        loads
    }

    fn loads_since(&self, loads: u64) -> u64 {
        self.owner.loads.load(Ordering::Relaxed) - loads
    }
}

/// Whether a report settles a decision's probe, and whether a probe names a
/// credential, are answered from the working state and write nothing a
/// keyed update could check. Each checks the authorization and clock rows
/// it read against SQL, so a state from before the decision's grant runs
/// again on a fresh load instead of answering as if there were no probe.
#[test]
fn probe_answers_read_from_a_stale_state_run_again_on_a_fresh_load() {
    let rig = rig("block-state-checked-probe-reads");
    due_credential_refusal(&rig, "c0");
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    let before_the_grant = rig.published();
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.grant_fence_rechecks(&origins, NOW, Some("decision"))
        })
        .unwrap());

    let loads = rig.decide_next_on(before_the_grant.clone());
    let settles = rig
        .store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.probe_report_matches("decision", None, true)
        })
        .unwrap();
    assert!(
        !settles,
        "a preserved report never settles the decision's probe"
    );
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");

    let loads = rig.decide_next_on(before_the_grant);
    let names = rig
        .store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.probe_authorization_names_credential("decision", "c0", "m0")
        })
        .unwrap();
    assert!(names, "the decision's probe names the refused credential");
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");
}

/// The probe-answer check compares the clock's claim owner too: a clock
/// whose owner changed without the state seeing it makes the answer run
/// again on a fresh load.
#[test]
fn a_probe_answer_on_a_clock_whose_owner_changed_unseen_runs_again() {
    let rig = rig("block-state-checked-claim-owner");
    due_credential_refusal(&rig, "c0");
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.grant_fence_rechecks(&origins, NOW, Some("decision"))
        })
        .unwrap());
    UNSEEN.with(|unseen| unseen.set(true));
    rig.store
        .bypass_account_writer_for_test(
            "UPDATE block SET claim_owner='another' WHERE managed=1 AND is_origin=1;",
        )
        .unwrap();
    UNSEEN.with(|unseen| unseen.set(false));
    let loads = rig.decide_next_on(rig.published());
    rig.store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.probe_authorization_names_credential("decision", "c0", "m0")
        })
        .unwrap();
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");
}

/// An unstarted admission's probe is released only on the generation it
/// holds. A state that shows the clock on a later generation, while SQL
/// still holds the admission's, must not skip the release: the keyed update
/// it runs anyway finds the row, and the batch runs again on a fresh load.
#[test]
fn an_admission_release_the_state_calls_superseded_runs_again_on_a_fresh_load() {
    let rig = rig("block-state-checked-release");
    due_credential_refusal(&rig, "c0");
    rig.store
        .with_writer_for_test(|conn| {
            conn.execute_batch("PRAGMA foreign_keys=OFF")?;
            conn.execute(
                "INSERT INTO account_admission (principal, admission_id, request_fingerprint,
                   request_mode, decision_id, conversation_hash, provider, model, account_id,
                   credential_id, pin_id, pin_generation, state, charged, request_bytes,
                   request_bytes_estimated, committed_at, admission_valid_until,
                   lease_valid_until, reply_json)
                 VALUES ('x','adm','fp','admit','d','conv','p0','m0','a0','c0','pin',1,'reserved',
                   0,0,0,?1,?1,?1,'{}')",
                [NOW - 60_000],
            )?;
            conn.execute_batch("PRAGMA foreign_keys=ON")
        })
        .unwrap();
    rig.sql("SELECT 1;".into());
    let owner = serde_json::json!(["account_admission", "x", "adm"]).to_string();
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    let granted = owner.clone();
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.grant_fence_rechecks(&origins, NOW, Some(&granted))
        })
        .unwrap());
    // A state whose clock is one generation past the admission's, while
    // SQL keeps the admission's generation.
    rig.sql(
        "UPDATE block SET claim_generation=claim_generation+1 WHERE kind='credential' AND managed=1 AND is_origin=1;"
            .into(),
    );
    let superseded = rig.published();
    rig.sql(
        "UPDATE block SET claim_generation=claim_generation-1 WHERE kind='credential' AND managed=1 AND is_origin=1;"
            .into(),
    );
    let generation = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.query_row(
                    "SELECT claim_generation,claim_owner FROM block WHERE kind='credential' AND managed=1 AND is_origin=1",
                    [],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
                )
            })
            .unwrap()
    };
    let (held, _) = generation(&rig);
    let loads = rig.decide_next_on(superseded);
    let released = rig
        .store
        .account_command(AccountCommandKind::Tick, move |tx| {
            tx.release_admission_probes(vec![("x".into(), "adm".into(), owner.clone())], NOW + 1)
        })
        .unwrap();
    assert_eq!(released.released, 1);
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");
    assert_eq!(
        generation(&rig),
        (held + 1, None),
        "the admission's generation was released"
    );
    rig.check(&[NOW], "after the release");
}

/// Binding a selection's probes to its decision moves every clock the
/// selection claimed. A state that misses one of them must not leave it
/// claimed by the selection: the leftover check finds it, and the batch runs
/// again on a fresh load.
#[test]
fn a_rebind_leaves_no_clock_claimed_by_the_old_owner() {
    let rig = rig("block-state-checked-rebind");
    due_credential_refusal(&rig, "c0");
    due_credential_refusal(&rig, "c1");
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    assert_eq!(origins.len(), 2, "{origins:?}");
    let first = vec![origins[0].clone()];
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.grant_fence_rechecks(&first, NOW, Some("selection"))
        })
        .unwrap());
    let before_the_second = rig.published();
    rig.sql(format!(
        "UPDATE block SET claim_owner='selection' WHERE kind='credential' AND origin_key='{}' AND managed=1 AND is_origin=1;",
        origins[1].origin_key
    ));
    let claimed = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    let loads = rig.decide_next_on(before_the_second);
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.bind_decision_probe(&claimed, "selection", "decision", "a0", "c0", NOW)
        })
        .unwrap());
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");
    let owners = rig
        .store
        .with_reader_for_test(|conn| {
            conn.prepare(
                "SELECT claim_owner FROM block WHERE managed=1 AND is_origin=1 ORDER BY origin_key",
            )?
            .query_map([], |r| r.get::<_, Option<String>>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap();
    assert_eq!(
        owners,
        [Some("decision".to_owned()), Some("decision".to_owned())]
    );
    rig.check(&[NOW], "after the rebind");
}

/// A block write the capture misses (nothing reports it) leaves the state
/// behind SQL. The periodic tick compares each table's held-row count and
/// highest rowid with SQL, counts the difference and has the state reloaded.
#[test]
fn the_tick_finds_and_reloads_a_block_write_the_capture_missed() {
    let rig = rig("block-state-reconcile");
    let row = observation_source_sql;
    rig.sql(row("seen"));
    UNSEEN.with(|unseen| unseen.set(true));
    rig.store
        .bypass_account_writer_for_test(&row("unseen"))
        .unwrap();
    UNSEEN.with(|unseen| unseen.set(false));
    assert_eq!(
        rig.store.block_writes_outside_command(),
        0,
        "nothing reported the write"
    );
    rig.sql("SELECT 1;".into());
    assert_ne!(rig.published(), rig.fresh(), "the state missed the write");
    let reloads = block_reconcile_reloads();
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.store
        .account_command(AccountCommandKind::Tick, |tx| tx.tick(NOW))
        .unwrap();
    assert_eq!(
        block_reconcile_reloads() - reloads,
        1,
        "one mismatch counted"
    );
    assert_eq!(rig.loads_since(loads), 1, "one reload");
    rig.check(&[NOW], "after the tick");
}

/// Retirement archives expired, unmanaged origins, and the archive's insert
/// trigger ends their source evidence in the block tables. It used to
/// insert with a REPLACE policy, which SQLite also applies to every
/// statement of that trigger; it now deletes a conflicting archive row
/// first. On the parity fixture, plus an archive row for a generation that
/// is current again, both leave the same rows.
#[test]
fn retirement_without_a_replace_policy_leaves_the_rows_the_replace_left() {
    let rig = make_rig("block-state-retirement-pin", Some(97));
    rig.store
        .with_writer_for_test(|conn| {
            crate::block_parity_fixture::seed_block_parity_fixture(conn, NOW)?;
            // A wall archived once, then recorded again with the same
            // evidence: retiring it again conflicts with its archive row.
            let wall = format!(
                "UPDATE pool_key_state SET walled_until={},wall_reason='billing' WHERE provider='retire-p';",
                NOW - 1_000
            );
            conn.execute_batch(&format!(
                "INSERT INTO pool_key_state(provider,credential_id,updated_at,wall_observed_at) VALUES ('retire-p','c',{0},{0});
                 {wall}
                 INSERT INTO fence_retired_origin SELECT kind,origin_key,evidence,evidence_at,1
                   FROM fence_origin WHERE kind='pool_billing' AND provider='retire-p';
                 {wall}
                 INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,
                   last_error,last_error_at,updated_at,failure_backoff_until,failure_backoff_reason,failure_observed_at)
                   VALUES ('retire-p/m','retire-p/m','retire-p','m',{1},{0},'route capacity',{0},{0},{1},'error',{0});
                 INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
                   VALUES ('retire-provider',{1},{0},'provider capacity',{0},{0});
                 INSERT INTO account_non_quota_size_refusal VALUES ('retire-p','c','m',16,{1},{0});
                 INSERT INTO account_non_quota_route_fence VALUES ('retire-p','c','m',{1},'d',{0},{0},'transient');",
                NOW - 5_000,
                NOW - 1_000
            ))?;
            let rows = |conn: &Connection| -> rusqlite::Result<(Vec<String>, Vec<String>)> {
                let archive = conn
                    .prepare("SELECT kind,origin_key,evidence,evidence_at,retired_at FROM fence_retired_origin ORDER BY kind,origin_key,evidence")?
                    .query_map([], |r| {
                        Ok(format!("{:?}", (0..5).map(|at| r.get::<_, Value>(at)).collect::<rusqlite::Result<Vec<_>>>()?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((block_rows(conn).map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.to_string().into()))?, archive))
            };
            conn.execute_batch("SAVEPOINT pin")?;
            fence_rechecks::snapshot_origins(conn)?;
            let replaced = conn.execute(
                "INSERT OR REPLACE INTO fence_retired_origin
                SELECT o.kind,o.origin_key,o.evidence,o.evidence_at,?1 FROM current_fence_origin o
                WHERE o.quota=0 AND o.deadline<?1 AND NOT EXISTS (SELECT 1 FROM fence_recheck r
                    WHERE r.kind=o.kind AND r.origin_key=o.origin_key AND r.evidence=o.evidence)",
                [NOW],
            )?;
            let with_replace = rows(conn)?;
            conn.execute_batch("ROLLBACK TO pin")?;
            fence_rechecks::snapshot_origins(conn)?;
            let retired = fence_rechecks::retire_expired(conn, NOW)?;
            let without = rows(conn)?;
            conn.execute_batch("RELEASE pin")?;
            assert!(replaced > 1, "the fixture retires several origins: {replaced}");
            assert_eq!(retired, replaced);
            assert_eq!(without, with_replace, "block rows, then archive rows");
            assert!(
                without.1.iter().any(|row| row.contains("pool_billing") && row.contains(&NOW.to_string())),
                "the conflicting archive row was replaced: {:#?}",
                without.1
            );
            Ok(())
        })
        .unwrap();
}

/// A whole-account block on several pools clears only once a success has
/// healed every pool in its recovery set. The clear is decided on that set
/// and on the successes recorded, and its keyed update requires both to be
/// unchanged: a state from before the
/// set grew decides to clear, SQL holds the larger set, and the batch runs
/// again on a fresh load, which keeps the block.
#[test]
fn a_pool_block_whose_recovery_set_grew_after_the_state_is_kept() {
    use striatum_core::accounts::{PoolKey, PoolScope};
    let rig = rig("block-state-recovery-set-cas");
    let older = NOW - 600_000;
    let covered = pool_key(5);
    let uncovered = serde_json::to_string(&PoolKey {
        window_minutes: Some(300),
        scope: PoolScope::EveryModel,
        name: Some("gemini".into()),
    })
    .unwrap();
    let recovery_set = vec![serde_json::from_str(&covered).unwrap()];
    rig.run(move |conn| {
        let row = crate::block_storage::account_source(
            conn,
            &crate::AccountPoolFenceRow {
                evidence: crate::AccountFenceRow {
                    fence_id: "whole".into(),
                    provider: "p0".into(),
                    account_id: "a0".into(),
                    scope_json: "{}".into(),
                    fence_json: "{}".into(),
                    source: "broca_account_limit".into(),
                    observed_at: older,
                    principal: "x".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: older,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set: recovery_set.clone(),
            },
        )?;
        crate::block_storage::write_source(conn, &row).map(|_| ())
    });
    let before_it_grew = rig.published();
    rig.sql(format!(
        "UPDATE block SET recovery_set_json='[{covered},{uncovered}]' WHERE kind='account' AND origin_key=json_array('whole');"
    ));
    let loads = rig.decide_next_on(before_it_grew);
    rig.store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.heal_serving_fences_in_scope(
                "p0",
                "a0",
                "c0",
                "m0",
                None,
                NOW,
                NOW + 1,
                Some(&crate::QuotaSuccessScope {
                    family: striatum_core::accounts::ModelFamily::Other,
                    pool: None,
                }),
            )
        })
        .unwrap();
    assert_eq!(rig.loads_since(loads), 2, "the stale run, then a fresh one");
    let active: i64 = rig
        .store
        .with_reader_for_test(|conn| {
            conn.query_row(
                "SELECT count(*) FROM block WHERE kind='account' AND source_present=1 AND cleared_at IS NULL",
                [],
                |r| r.get(0),
            )
        })
        .unwrap();
    assert_eq!(active, 1, "the block waits for its uncovered pool");
    rig.check(&[NOW], "after the heal");
}

fn authorized_recovery_rig(empty: bool) -> Rig {
    use striatum_core::accounts::{PoolKey, PoolScope};
    let rig = rig("authorized-whole-account-recovery");
    let pools: Vec<_> = if empty {
        Vec::new()
    } else {
        ["a", "b"]
            .map(|model| PoolKey {
                window_minutes: Some(300),
                scope: PoolScope::Models([model.to_owned(), "both".to_owned()].into()),
                name: None,
            })
            .into()
    };
    rig.run(move |conn| {
        let row = crate::block_storage::account_source(
            conn,
            &crate::AccountPoolFenceRow {
                evidence: crate::AccountFenceRow {
                    fence_id: "whole".into(),
                    provider: "p0".into(),
                    account_id: "a0".into(),
                    scope_json: "{}".into(),
                    fence_json: "{}".into(),
                    source: "broca_account_limit".into(),
                    observed_at: NOW - 1_000_000,
                    principal: "x".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: NOW - 1_000_000,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set: pools.clone(),
            },
        )?;
        crate::block_storage::write_source(conn, &row).map(|_| ())
    });
    rig
}

/// Run each claim and settlement first on SQL under rollback, then on the
/// working state, comparing every block-table row as well as the answer.
fn authorized_recovery_success(rig: &Rig, model: &str, known: bool, at: i64) {
    let model = model.to_owned();
    rig.store
        .account_command(AccountCommandKind::Report, move |tx| {
            let owner = owner_name(2);
            let claim = |tx: &crate::AccountTx<'_>| -> RouteStateResult<()> {
                let origins = tx.fence_rechecks()?;
                assert!(tx.grant_fence_rechecks(&origins, at, Some("owner1"))?);
                assert!(tx.claim_fence_rechecks_for_authorized_admission(
                    &origins,
                    at,
                    Some("owner1"),
                    &owner
                )?);
                tx.conn
                    .execute(
                        "UPDATE block SET reset_check_at=?1 WHERE kind='account' AND managed=1 AND is_origin=1",
                        [at + if known { 100 } else { 5 }],
                    )
                    .map_err(err)?;
                Ok(())
            };
            let settle = |tx: &crate::AccountTx<'_>| {
                let admission = admission_row("p0", "c0", &model, at);
                let scope = known.then_some(crate::QuotaSuccessScope {
                    family: striatum_core::accounts::ModelFamily::Other,
                    pool: None,
                });
                tx.heal_admission_success(&admission, None, Some("owner1"), scope.as_ref(), at + 10)
            };
            for stage in ["claim", "settle"] {
                let run = |tx: &crate::AccountTx<'_>| -> RouteStateResult<String> {
                    let answer = if stage == "claim" {
                        claim(tx)?;
                        Vec::new()
                    } else {
                        settle(tx)?
                    };
                    Ok(format!("{answer:?} {:#?}", block_rows(tx.conn)?))
                };
                tx.conn
                    .execute_batch("SAVEPOINT recovery_oracle")
                    .map_err(err)?;
                let sql = run(&crate::AccountTx::sql(tx.conn))?;
                tx.conn
                    .execute_batch("ROLLBACK TO recovery_oracle; RELEASE recovery_oracle")
                    .map_err(err)?;
                assert_eq!(
                    run(tx)?,
                    sql,
                    "{stage}: working-state and SQL recovery agree"
                );
            }
            Ok(())
        })
        .unwrap();
    rig.check(&[at + 10], "authorized recovery success");
}

fn assert_outstanding_authorized_recovery(rig: &Rig, successes: usize, at: i64) {
    let blocks = rig.published();
    assert!(
        blocks.account_fence("whole").is_some(),
        "unproved pools keep the fence"
    );
    assert_eq!(blocks.success_pools().count(), successes);
    let (clock, cleared) = blocks.clock("account", "[\"whole\"]").unwrap();
    assert_eq!(cleared, None, "the outstanding block's clock stays active");
    assert_eq!(clock.claim_owner, None, "the test releases its claim");
    assert_eq!(
        clock.next_recheck_at,
        at + 10 + striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS
    );
    assert_eq!(clock.reset_check_at, (successes > 0).then_some(at + 100));
    assert_eq!(
        blocks.authorizations().count(),
        0,
        "owner and decision copy are retired"
    );
}

#[test]
fn authorized_whole_account_recovery_proves_only_a_then_b_clears() {
    let rig = authorized_recovery_rig(false);
    authorized_recovery_success(&rig, "a", true, NOW);
    assert_outstanding_authorized_recovery(&rig, 1, NOW);
    let blocks = rig.published();
    let proof = blocks.success_pools().next().unwrap();
    let pool: striatum_core::accounts::PoolKey =
        serde_json::from_str(&proof.pool_key_json).unwrap();
    assert_eq!(
        pool.scope,
        striatum_core::accounts::PoolScope::Models(["a".into(), "both".into()].into())
    );
    assert_eq!(proof.evidence_at, NOW - 1_000_000);
    assert_eq!(proof.healed_at, NOW + 10);
    authorized_recovery_success(
        &rig,
        "b",
        true,
        NOW + 10 + striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
    );
    assert!(rig.published().account_fence("whole").is_none());
    assert_eq!(rig.published().success_pools().count(), 2);
    assert!(rig.published().fence_rechecks(NOW).unwrap().is_empty());
}

#[test]
fn authorized_whole_account_recovery_both_used_clears_at_once() {
    let rig = authorized_recovery_rig(false);
    authorized_recovery_success(&rig, "both", true, NOW);
    assert!(rig.published().account_fence("whole").is_none());
    assert_eq!(rig.published().success_pools().count(), 2);
    assert!(rig.published().fence_rechecks(NOW).unwrap().is_empty());
}

#[test]
fn authorized_whole_account_recovery_unknown_scope_heals_nothing() {
    let rig = authorized_recovery_rig(false);
    authorized_recovery_success(&rig, "both", false, NOW);
    assert_outstanding_authorized_recovery(&rig, 0, NOW);
}

#[test]
fn authorized_whole_account_recovery_empty_set_still_clears() {
    let rig = authorized_recovery_rig(true);
    authorized_recovery_success(&rig, "both", false, NOW);
    assert!(rig.published().account_fence("whole").is_none());
    assert_eq!(rig.published().success_pools().count(), 0);
    assert!(rig.published().fence_rechecks(NOW).unwrap().is_empty());
}

/// A block row changed in place without the capture seeing it keeps every
/// table's row count and highest rowid, so the per-tick comparison misses
/// it. A full comparison with a fresh load, every sixth tick, finds it.
#[test]
fn a_periodic_full_comparison_finds_a_missed_change_in_place() {
    let rig = rig("block-state-full-reconcile");
    rig.sql(observation_source_sql("a"));
    // Its clock is installed first, so no sync below decides about the
    // origin (a decision would check the row and catch the change itself).
    rig.store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.sync_fence_rechecks()
        })
        .unwrap();
    UNSEEN.with(|unseen| unseen.set(true));
    rig.store
        .bypass_account_writer_for_test(&format!(
            "UPDATE block SET deadline={} WHERE kind='observation' AND account_id='a';",
            NOW + 120_000
        ))
        .unwrap();
    UNSEEN.with(|unseen| unseen.set(false));
    let tick = || {
        rig.store
            .account_command(AccountCommandKind::Tick, |tx| tx.tick(NOW))
            .unwrap()
    };
    let reloads = block_reconcile_reloads();
    tick();
    assert_eq!(
        block_reconcile_reloads(),
        reloads,
        "counts and rowids match"
    );
    assert_ne!(
        rig.published(),
        rig.fresh(),
        "the state still misses the change"
    );
    for _ in 0..5 {
        tick();
    }
    assert_eq!(
        block_reconcile_reloads() - reloads,
        1,
        "the full comparison found it"
    );
    rig.check(&[NOW], "after the full comparison");
}

#[test]
fn a_commit_from_another_connection_just_before_a_batch_is_loaded_inside_it() {
    let rig = rig("block-state-outside-before-begin");
    let row = observation_source_sql;
    rig.sql(row("first"));
    let path = std::fs::canonicalize(rig._scratch.path().join("state.db")).unwrap();
    let outside = path.clone();
    crate::account_writer::BEFORE_BEGIN.lock().unwrap().push((
        path,
        Box::new(move || {
            // Another process commits after the writer's freshness check.
            let conn = Connection::open(&outside).unwrap();
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            conn.execute_batch(&row("outside")).unwrap();
        }),
    ));
    rig.sql(row("second"));
    assert!(
        crate::account_writer::BEFORE_BEGIN
            .lock()
            .unwrap()
            .is_empty(),
        "the hook ran"
    );
    rig.check(
        &[NOW],
        "after an outside commit just before the batch began",
    );
}

#[test]
fn a_report_from_an_owner_whose_claim_was_superseded_does_not_match() {
    let rig = rig("block-state-superseded-report");
    rig.store
        .record_provider_cooldown(
            "p0",
            "p0/m0",
            "p0/m0",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW - 2 * 3_600_000),
                cooldown_until: Some((NOW + 5 * 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    rig.run(fence_rechecks::sync);
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    let claim = |owner: &'static str, at: i64| {
        let origins = origins.clone();
        rig.store
            .account_command(AccountCommandKind::Maintenance, move |tx| {
                tx.grant_fence_rechecks(&origins, at, Some(owner))
            })
            .unwrap()
    };
    // The first owner's one-hour interval ends; a second owner claims the
    // same evidence's next generation. The first keeps its authorization row,
    // now naming an older generation.
    assert!(claim("first", NOW));
    assert!(claim("second", NOW + 2 * 3_600_000));
    let matches = |owner: &'static str| {
        rig.store
            .account_command(AccountCommandKind::Report, move |tx| {
                tx.probe_report_matches(owner, None, false)
            })
            .unwrap()
    };
    assert!(
        !matches("first"),
        "a superseded claim cannot settle the probe"
    );
    assert!(matches("second"));
}

/// The same superseded test request, settled through an authorized success
/// (`heal_authorized_probe`, which the legacy decision-outcome and media
/// run-finished paths call) or as an admission's run, heals nothing
/// either: only an admission that itself owns the clock's claim is current
/// on another generation.
#[test]
fn a_superseded_test_request_heals_nothing_by_any_settling_path() {
    for path in ["authorized success", "admission run"] {
        let rig = rig("block-state-superseded-heal-paths");
        rig.store
            .record_provider_cooldown(
                "p0",
                "p0/m0",
                "p0/m0",
                &RouteStateUpdate {
                    observed_at_ms: Some(NOW - 2 * 3_600_000),
                    cooldown_until: Some((NOW + 5 * 3_600_000) as f64),
                    last_error: Some("provider capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        rig.store
            .account_transaction(|tx| fence_rechecks::sync_on(tx, NOW).map_err(err))
            .unwrap();
        let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
        for (owner, at) in [("first", NOW), ("second", NOW + 2 * 3_600_000)] {
            let origins = origins.clone();
            assert!(rig
                .store
                .account_command(AccountCommandKind::Maintenance, move |tx| {
                    tx.grant_fence_rechecks(&origins, at, Some(owner))
                })
                .unwrap());
        }
        rig.store
            .account_command(AccountCommandKind::Report, |tx| {
                tx.release_inconclusive_probe("second", "p0", "m0", NOW + 2 * 3_600_000 + 1)
            })
            .unwrap();
        let later = NOW + 3 * 3_600_000;
        rig.store
            .account_command(AccountCommandKind::Report, move |tx| match path {
                "authorized success" => tx.heal_authorized_probe("first", "p0", "m0", later),
                _ => {
                    let origins = tx.fence_rechecks()?;
                    tx.heal_report_probe("first", None, true, "m0", later, &origins, None, later)
                }
            })
            .unwrap();
        assert_eq!(
            rig.store.fence_rechecks().unwrap().len(),
            1,
            "{path}: the superseded test's success heals no block"
        );
        rig.check(&[NOW], path);
    }
}

/// A test request that a later one superseded heals nothing: its success
/// finds the block's clock on the later generation, even once that later
/// test was released without an outcome and the clock has no owner.
#[test]
fn a_superseded_test_request_heals_nothing() {
    let rig = rig("block-state-superseded-heal");
    rig.store
        .record_provider_cooldown(
            "p0",
            "p0/m0",
            "p0/m0",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW - 2 * 3_600_000),
                cooldown_until: Some((NOW + 5 * 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    rig.store
        .account_transaction(|tx| fence_rechecks::sync_on(tx, NOW).map_err(err))
        .unwrap();
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    let claim = |owner: &'static str, at: i64| {
        let origins = origins.clone();
        rig.store
            .account_command(AccountCommandKind::Maintenance, move |tx| {
                tx.grant_fence_rechecks(&origins, at, Some(owner))
            })
            .unwrap()
    };
    assert!(claim("first", NOW));
    assert!(claim("second", NOW + 2 * 3_600_000));
    rig.store
        .account_command(AccountCommandKind::Report, |tx| {
            tx.release_inconclusive_probe("second", "p0", "m0", NOW + 2 * 3_600_000 + 1)
        })
        .unwrap();
    let later = NOW + 3 * 3_600_000;
    rig.store
        .account_command(AccountCommandKind::Report, move |tx| {
            let origins = tx.fence_rechecks()?;
            tx.heal_report_probe("first", None, false, "m0", later, &origins, None, later)
        })
        .unwrap();
    assert_eq!(
        rig.store.fence_rechecks().unwrap().len(),
        1,
        "the superseded test's success heals no block"
    );
    rig.check(&[NOW], "after the superseded success");
}

#[test]
fn a_fork_shares_every_table() {
    let rig = make_rig("block-state-fork", Some(97));
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
    native_generator::step(rig, rng, now)
}

/// The claim, probe and healing decisions a command takes: one chosen at
/// random, run first deciding on SQL inside a savepoint that is then rolled
/// back, and then deciding on the command's working state, with the same
/// arguments. The answers and every block table's rows each leaves must be
/// equal. A healer has no answer of its own; its answer is whether it changed
/// any block row.
fn decide_both(rig: &Rig, rng: &mut Rng, now: i64) -> &'static str {
    const DECISIONS: [&str; 15] = [
        "decide: grant",
        "decide: admission claim",
        "decide: authorized admission claim",
        "decide: plain claim",
        "decide: bind decision probe",
        "decide: report matches probe",
        "decide: authorization names credential",
        "decide: transfer authorization",
        "heal: authorized probe",
        "heal: serving fences",
        "heal: origins",
        "heal: report probe",
        "heal: admission success",
        "heal: failed probe",
        "heal: inconclusive probe",
    ];
    // Classification is decided apart: it answers the stored class.
    let which = rng.below(DECISIONS.len() as u64 + 1) as usize;
    let first = rng.below(4) as usize;
    let count = 1 + rng.below(2) as usize;
    let at = now + [0, 900_000, 3_600_000, 4 * 3_600_000][rng.below(4) as usize];
    let owner = owner_name(rng.below(3));
    let other = owner_name(rng.below(3));
    let provider = rng.pick(&PROVIDERS).to_owned();
    let credential = rng.pick(&CREDENTIALS).to_owned();
    let model = rng.pick(&MODELS).to_owned();
    let preserve = rng.below(4) == 0;
    let named = rng.below(2) == 0;
    // Before, between or after the generated evidence times.
    let admitted = now + [-3_600_000, 0, 3_600_000][rng.below(3) as usize];
    let size = [None, Some(16), Some(17)][rng.below(3) as usize];
    let scope = (rng.below(3) != 0).then(|| crate::QuotaSuccessScope {
        family: striatum_core::accounts::ModelFamily::Other,
        pool: None,
    });
    let flag = rng.below(2) == 0;
    let anchored = rng.below(4) != 0;
    let edge = rng.below(3) as i64 - 1;
    let (sql, memory) = rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            let decide = |tx: &crate::AccountTx<'_>| -> RouteStateResult<String> {
                let origins = fence_rechecks::read(tx.conn).map_err(err)?;
                let chosen: Vec<_> = origins.iter().skip(first).take(count).cloned().collect();
                let before = block_rows(tx.conn)?;
                let healed = |done: RouteStateResult<()>| -> RouteStateResult<bool> {
                    done?;
                    Ok(block_rows(tx.conn)? != before)
                };
                let effective: Vec<FenceRecheck> = tx
                    .fence_rechecks()?
                    .into_iter()
                    .skip(first)
                    .take(count)
                    .collect();
                // Most heals are tried at the edge of their first block: on
                // its own provider, login and model, admitted just before, at
                // or just after its evidence.
                let anchor = effective.first().filter(|_| anchored && which >= 8);
                let provider = anchor.map_or_else(|| provider.clone(), |o| o.provider.clone());
                let credential = anchor
                    .and_then(|o| o.credential_id.clone())
                    .unwrap_or_else(|| credential.clone());
                let model = anchor
                    .and_then(|o| o.model.clone())
                    .unwrap_or_else(|| model.clone());
                let admitted = anchor.map_or(admitted, |o| o.evidence_at + edge);
                let answer = match which {
                    0 => tx.grant_fence_rechecks(&chosen, at, Some(&owner)),
                    1 => tx.claim_fence_rechecks_for_admission(&chosen, at, Some(&owner)),
                    2 => tx.claim_fence_rechecks_for_authorized_admission(
                        &chosen,
                        at,
                        Some(&owner),
                        &other,
                    ),
                    3 => tx.claim_fence_rechecks(&chosen, at),
                    4 => tx.bind_decision_probe(&chosen, &owner, &other, "a0", &credential, at),
                    5 => tx.probe_report_matches(&owner, named.then_some(&*credential), preserve),
                    6 => tx.probe_authorization_names_credential(&owner, &credential, &model),
                    7 => tx
                        .transfer_probe_authorization(&owner, &other)
                        .map(|()| true),
                    8 => healed(tx.heal_authorized_probe(&owner, &provider, &model, at)),
                    9 => healed(tx.heal_serving_fences_in_scope(
                        &provider,
                        "a0",
                        &credential,
                        &model,
                        size,
                        admitted,
                        at,
                        scope.as_ref(),
                    )),
                    10 => healed(tx.heal_origins(
                        &effective,
                        &model,
                        admitted,
                        at,
                        scope.as_ref(),
                        flag,
                    )),
                    11 => {
                        let effective = tx.fence_rechecks()?;
                        healed(tx.heal_report_probe(
                            &owner,
                            Some(other.as_str()),
                            flag,
                            &model,
                            admitted,
                            &effective,
                            scope.as_ref(),
                            at,
                        ))
                    }
                    12 => {
                        let admission = admission_row(&provider, &credential, &model, admitted);
                        healed(
                            tx.heal_admission_success(
                                &admission,
                                size,
                                Some(other.as_str()),
                                scope.as_ref(),
                                at,
                            )
                            .map(drop),
                        )
                    }
                    13 => healed(tx.fail_authorized_probe(&owner, at, flag)),
                    14 => healed(tx.release_inconclusive_probe(&owner, &provider, &model, at)),
                    _ => {
                        // A credential with a refusal on record, when there
                        // is one, so the classification has a clock to set.
                        let refused: Option<(String, String)> = tx
                            .conn
                            .query_row(
                                "SELECT provider,credential_id FROM block
                                 WHERE kind='credential' AND source_present=1 ORDER BY rowid LIMIT 1 OFFSET ?1",
                                [first as i64 % 2],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .optional()
                            .map_err(err)?;
                        let (provider, credential) =
                            refused.unwrap_or((provider.clone(), credential.clone()));
                        tx.classify_credential_quota(&provider, &credential, flag)
                    }
                };
                Ok(format!("{answer:?} {:#?}", block_rows(tx.conn)?))
            };
            tx.conn.execute_batch("SAVEPOINT oracle").map_err(err)?;
            let sql = decide(&crate::AccountTx::sql(tx.conn))?;
            tx.conn
                .execute_batch("ROLLBACK TO oracle; RELEASE oracle")
                .map_err(err)?;
            Ok((sql, decide(tx)?))
        })
        .unwrap();
    let name = DECISIONS
        .get(which)
        .copied()
        .unwrap_or("decide: classify credential");
    DECIDED.with(|decided| {
        decided
            .borrow_mut()
            .push((name, memory.starts_with("Ok(true)"), sql == memory))
    });
    assert_eq!(memory, sql, "{name}: the state's decision equals SQL's");
    name
}

/// The generated probe owners: two decisions and one admission of
/// principal `x`, whose owner name the admission healer derives.
fn owner_name(index: u64) -> String {
    match index {
        0 | 1 => format!("owner{index}"),
        _ => serde_json::json!(["account_admission", "x", "adm0"]).to_string(),
    }
}

/// An admission of principal `x` on account `a0`, committed at `committed_at`.
fn admission_row(
    provider: &str,
    credential: &str,
    model: &str,
    committed_at: i64,
) -> crate::AccountAdmissionRow {
    crate::AccountAdmissionRow {
        principal: "x".into(),
        admission_id: "adm0".into(),
        request_fingerprint: String::new(),
        request_mode: "admit".into(),
        decision_id: "owner1".into(),
        conversation_hash: String::new(),
        provider: provider.into(),
        model: model.into(),
        account_id: "a0".into(),
        credential_id: credential.into(),
        pin_id: String::new(),
        pin_generation: 0,
        state: "started".into(),
        charged: true,
        request_bytes: 0,
        request_bytes_estimated: false,
        committed_at,
        admission_valid_until: committed_at,
        lease_valid_until: committed_at,
        reply_json: String::new(),
        run_id: None,
        started_at: Some(committed_at),
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    }
}

/// Every block table's rows, each as its columns' values, in rowid order.
fn block_rows(conn: &Connection) -> RouteStateResult<Vec<String>> {
    let mut rows = Vec::new();
    for table in BLOCK_TABLES {
        let mut statement = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .map_err(err)?;
        let columns = statement.column_count();
        let mut query = statement.query([]).map_err(err)?;
        while let Some(row) = query.next().map_err(err)? {
            let values = (0..columns)
                .map(|at| row.get::<_, Value>(at))
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(err)?;
            rows.push(format!("{table} {values:?}"));
        }
    }
    Ok(rows)
}

thread_local! {
    /// The `route.cooldown_status` answers `check_route_state` compared.
    static COOLED: std::cell::RefCell<std::collections::BTreeSet<bool>> =
        const { std::cell::RefCell::new(std::collections::BTreeSet::new()) };
    /// Each decision `decide_both` compared: its name, whether the state
    /// answered `true`, and whether it matched SQL.
    static DECIDED: std::cell::RefCell<Vec<(&'static str, bool, bool)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn pool_key(window: u32) -> String {
    serde_json::to_string(&striatum_core::accounts::PoolKey {
        window_minutes: Some(window * 60),
        scope: striatum_core::accounts::PoolScope::EveryModel,
        name: None,
    })
    .unwrap()
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
        mode_of: None,
    }
}

/// Route and provider rows the main sequence does not write: variants
/// sharing one capacity, every stored wall class, credential refusals and
/// transient errors, route data alone, and clocks claimed by a probe. After
/// every step the route-state snapshot and `route.cooldown_status` must
/// answer from memory exactly as from SQL (`Rig::check_route_state`).
#[test]
fn the_route_state_snapshot_from_memory_equals_sql_over_generated_route_writes() {
    const ROUTE_ERRORS: [&str; 6] = [
        ERRORS[0],
        ERRORS[1],
        ERRORS[2],
        ERRORS[3],
        "'{\"class\":\"transient\",\"provider_code\":\"overloaded\"}'",
        "'{\"class\":\"permanent\",\"provider_code\":\"billing_exhausted\",\"credential_id\":\"c0\"}'",
    ];
    COOLED.with(|seen| seen.borrow_mut().clear());
    let mut operations = std::collections::BTreeMap::<&str, usize>::new();
    let (mut shared_capacity, mut clock_scheduled, mut billing_capacity, mut quarantined) =
        (false, false, false, false);
    for seed in 1..=6u64 {
        let rig = rig(&format!("route-state-snapshot-{seed}"));
        let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut now = NOW;
        for index in 0..120 {
            let p = rng.pick(&PROVIDERS);
            let m = rng.pick(&MODELS);
            let route = match rng.below(3) {
                0 => format!("{p}/{m}"),
                variant => format!("{p}/{m}/v{variant}"),
            };
            let at = rng.time(now);
            let operation = match rng.below(7) {
                0 => {
                    let (cooldown, quota, failure) = (
                        rng.maybe_time(now + 3_600_000),
                        rng.maybe_time(now + 3_600_000),
                        rng.maybe_time(now + 3_600_000),
                    );
                    let class = rng.pick(&["NULL", "'quota'", "'billing'"]);
                    let error = rng.pick(&ROUTE_ERRORS);
                    let unscoped = rng.below(2);
                    let pressure = rng.pick(&["NULL", "0.25", "0.75"]);
                    quarantined |= native_route_generator::route(
                        &rig,
                        native_route_generator::Input {
                            route: route.clone(),
                            provider: p.into(),
                            model: m.into(),
                            cooldown,
                            quota,
                            failure,
                            pressure: pressure.into(),
                            class: class.into(),
                            error: error.into(),
                            unscoped: unscoped != 0,
                            at,
                        },
                    );
                    "write a route row"
                }
                1 => {
                    let (cooldown, quota) = (
                        rng.maybe_time(now + 3_600_000),
                        rng.maybe_time(now + 3_600_000),
                    );
                    let class = rng.pick(&["NULL", "'quota'", "'billing'"]);
                    let error = rng.pick(&ROUTE_ERRORS);
                    let unscoped = rng.below(2);
                    quarantined |= native_route_generator::provider(
                        &rig,
                        native_route_generator::Input {
                            route: route.clone(),
                            provider: p.into(),
                            model: m.into(),
                            cooldown,
                            quota,
                            failure: "NULL".into(),
                            pressure: "NULL".into(),
                            class: class.into(),
                            error: error.into(),
                            unscoped: unscoped != 0,
                            at,
                        },
                    );
                    "write a provider row"
                }
                2 => {
                    rig.store
                        .account_command(AccountCommandKind::Maintenance, |tx| {
                            tx.sync_fence_rechecks()
                        })
                        .unwrap();
                    "sync clocks"
                }
                3 => {
                    // A probe claims every due clock, which schedules the
                    // walls it covers at the claim's own due time.
                    let owner = format!("probe-{index}");
                    rig.store
                        .account_command(AccountCommandKind::Maintenance, move |tx| {
                            tx.sync_fence_rechecks()?;
                            let origins = tx.fence_rechecks()?;
                            tx.grant_fence_rechecks(&origins, now, Some(&owner))
                        })
                        .unwrap();
                    "claim clocks"
                }
                4 => {
                    native_route_generator::delete_route(&rig, route.clone());
                    "delete a route row"
                }
                5 => {
                    let latency = rng.below(500);
                    let success = rng.pick(&["NULL", "0.5", "1.0"]);
                    rig.sql(format!("UPDATE model_route_state SET latency_ema_ms={latency},success_ema={success},updated_at={at} WHERE route_key='{route}';"));
                    "route data only"
                }
                _ => {
                    let update = RouteStateUpdate {
                        observed_at_ms: Some(at),
                        cooldown_until: Some((now + 3_600_000) as f64),
                        last_error: Some("provider capacity".into()),
                        ..Default::default()
                    };
                    rig.store
                        .record_provider_cooldown(p, &route, &format!("{p}/{m}"), &update)
                        .unwrap();
                    "record provider cooldown"
                }
            };
            *operations.entry(operation).or_default() += 1;
            let context = format!("seed={seed} step={index} op={operation}");
            rig.check(&[now - 3_600_000, now, now + 3_600_000], &context);
            let snapshot = rig
                .published()
                .route_state_snapshot(now as f64, now)
                .unwrap();
            billing_capacity |= snapshot
                .by_capacity
                .values()
                .any(|state| state.wall_class == Some(striatum_core::RouteWallClass::Billing));
            rig.store
                .with_reader_for_test(|conn| {
                    shared_capacity |= conn.query_row(
                        "SELECT count(*) FROM (SELECT 1 FROM model_route_state GROUP BY capacity_key HAVING count(*)>1)",
                        [],
                        |r| r.get::<_, i64>(0),
                    )? > 0;
                    let stored: Vec<(String, Option<f64>)> = conn
                        .prepare("SELECT m.route_key,b.cooldown_until FROM model_route_state m LEFT JOIN block b ON b.kind='route' AND b.origin_key=json_array(m.route_key) AND b.source_present=1")?
                        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<rusqlite::Result<_>>()?;
                    clock_scheduled |= stored.iter().any(|(route, raw)| {
                        let scheduled = snapshot.by_route[route].cooldown_until;
                        scheduled.is_some() && scheduled != *raw
                    });
                    Ok(())
                })
                .unwrap();
            now += rng.below(3) as i64 * 600_000;
        }
        assert_eq!(rig.store.block_writes_outside_command(), 0, "seed={seed}");
    }
    assert_eq!(operations.len(), 7, "every operation ran: {operations:?}");
    assert!(shared_capacity, "variants sharing one capacity");
    assert!(
        clock_scheduled,
        "a route deadline a claimed clock rescheduled"
    );
    assert!(billing_capacity, "a capacity walled by billing");
    assert!(quarantined, "a quarantined route or provider row");
    let cooled = COOLED.with(|seen| seen.borrow().clone());
    assert_eq!(
        cooled,
        [false, true].into_iter().collect(),
        "cooldown_status answered both ways"
    );
}

/// Resident memory in bytes, from `/proc/self/statm`; 0 where unavailable.
fn resident_bytes() -> i64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<i64>().ok())
        .map_or(0, |pages| pages * 4096)
}

/// Seed the block tables at `scale` times the live store's row counts (read
/// from production when this was written), plus `eligibility` rows.
fn seed_live_sized(rig: &Rig, scale: usize, eligibility: usize) {
    let n = |live: usize| live * scale;
    let mut sql = String::new();
    for i in 0..n(71) {
        let id = if i % 5 == 0 {
            format!("credential-quota:[\"p{}\",\"c{i}\"]:hold", i % 8)
        } else {
            format!("fence-{i}")
        };
        let cleared = if i % 3 == 0 {
            "NULL".to_owned()
        } else {
            NOW.to_string()
        };
        sql += &format!("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json,cleared_at) VALUES ('{id}','p{}','a{i}','{{}}','{{\"reset\":{{\"at_ms\":{}}}}}','broca_account_limit',{},'x',{},NULL,'[]',{cleared});\n", i % 8, NOW + 3_600_000, NOW - i as i64, NOW - i as i64);
    }
    for i in 0..n(4) {
        sql += &format!("INSERT INTO credential_entitlement_fences VALUES ('p{}','c{i}','billing_exhausted','l{i}',{});\n", i % 8, NOW - i as i64);
    }
    for i in 0..n(2) {
        sql += &format!("INSERT INTO account_route_fence VALUES ('p{}','c{i}','m{i}',{},'d',{NOW},{NOW},'quota');\n", i % 8, NOW + 3_600_000);
    }
    for i in 0..n(4) {
        sql += &format!("INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,updated_at) VALUES ('p{}','k{i}',{},'billing',{NOW});\n", i % 8, NOW + 3_600_000);
    }
    for i in 0..n(8) {
        sql += &format!("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,last_error_at,updated_at) VALUES ('p{i}',{},{NOW},1,'provider capacity',{NOW},{NOW});\n", NOW + 600_000);
    }
    for i in 0..n(850) {
        let wall = if i % 50 == 0 {
            (NOW + 600_000).to_string()
        } else {
            "NULL".into()
        };
        sql += &format!("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error_at,updated_at) VALUES ('p{}/m{i}','p{}/m{i}','p{}','m{i}',{wall},{NOW},1,{NOW},{NOW});\n", i % 8, i % 8, i % 8);
    }
    for i in 0..n(2) {
        sql += &format!(
            "INSERT INTO account_transient_backoff VALUES ('p{}','a{i}',{},{NOW},'k{i}','[]');\n",
            i % 8,
            NOW + 600_000
        );
    }
    for i in 0..eligibility {
        sql += &format!("INSERT INTO account_eligibility VALUES ('p{}','c{}','m{i}','l0','model_unsupported',{NOW});\n", i % 8, i % 40);
    }
    rig.sql(sql);
    rig.run(fence_rechecks::sync);
    let authorizations = n(6);
    rig.run(move |conn| {
        conn.execute(
            &format!("INSERT INTO fence_probe_authorization(owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation) SELECT 'owner'||rowid,kind,origin_key,evidence,evidence_at,{NOW},1 FROM fence_recheck LIMIT {authorizations}"),
            [],
        )?;
        Ok(())
    });
}

/// Footprint, load and read cost of the block state at the live store's row
/// counts and at ten times them with 5,000 eligibility rows. Release:
/// `cargo test --release -p striatum-store --lib live_sized_block_state_costs -- --ignored --nocapture`.
#[test]
#[ignore]
fn live_sized_block_state_costs() {
    for (name, scale, eligibility) in [("live", 1, 0), ("stress x10", 10, 5_000)] {
        let rig = rig(&format!("block-state-live-{scale}"));
        seed_live_sized(&rig, scale, eligibility);
        let memory = rig.published();
        assert_eq!(memory, rig.fresh());
        let rounds = 20;
        let start = std::time::Instant::now();
        let before = resident_bytes();
        let copies: Vec<BlockState> = (0..rounds).map(|_| rig.fresh()).collect();
        let load_us = start.elapsed().as_micros() as f64 / rounds as f64;
        let bytes = (resident_bytes() - before) / rounds as i64;
        drop(copies);
        let start = std::time::Instant::now();
        for _ in 0..1_000 {
            let fork = memory.clone();
            assert!(fork.shares_storage_with(&memory));
        }
        let fork_ns = start.elapsed().as_nanos() as f64 / 1_000.0;
        let start = std::time::Instant::now();
        for _ in 0..100 {
            std::hint::black_box(memory.fence_rechecks(NOW).unwrap());
        }
        let rechecks_us = start.elapsed().as_micros() as f64 / 100.0;
        let start = std::time::Instant::now();
        for _ in 0..100 {
            std::hint::black_box(memory.active_pool_fences("p0").unwrap());
            std::hint::black_box(memory.eligibility());
        }
        let keyed_us = start.elapsed().as_micros() as f64 / 100.0;
        let sql_us = {
            let start = std::time::Instant::now();
            for _ in 0..100 {
                std::hint::black_box(rig.store.fence_rechecks().unwrap());
            }
            start.elapsed().as_micros() as f64 / 100.0
        };
        let rows: usize = memory.table_sizes().iter().map(|(_, rows)| rows).sum();
        eprintln!(
            "block state {name}: held rows={rows} sizes={:?} resident_bytes/state={bytes} load_us={load_us:.0} fork_ns={fork_ns:.0} fence_rechecks_us={rechecks_us:.0} (sql {sql_us:.0}) pool_fences+eligibility_us={keyed_us:.0}",
            memory
                .table_sizes()
                .into_iter()
                .filter(|(_, rows)| *rows > 0)
                .collect::<Vec<_>>()
        );
    }
}

/// Every production writer of the block tables, driven through its real
/// store entry point, writes inside an account command: none changes a block
/// row the capture misses (`block_writes_outside_command` stays 0), and after
/// each one the published state equals a fresh load. The tick that ends the
/// sequence reconciles the state with SQL and must find nothing to reload.
/// The request handlers' own commands (selection, admission, observation,
/// report) are driven by the module's read-trace test, which checks the same
/// counters.
#[test]
fn every_production_block_writer_writes_inside_an_account_command() {
    use striatum_core::key_pool::PoolKey;
    use striatum_core::route_state_store::CredentialRefusal;
    use striatum_core::{DecisionInput, DecisionOutcomeUpdate};

    // The counter sees a write outside a command: the negative.
    let control = rig("block-writers-control");
    control
        .store
        .bypass_account_writer_for_test(
            "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,updated_at) VALUES ('p9',1,1)",
        )
        .unwrap();
    assert_eq!(control.store.block_writes_outside_command(), 1);

    let rig = rig("block-writers");
    let now = NOW;
    let reloads = block_reconcile_reloads();
    let mut ran = Vec::new();
    let read_backs = std::cell::Cell::new(block_read_backs());
    let mut step = |name: &'static str, rig: &Rig| {
        // The writer read back the block rows the step changed: the step
        // really wrote, inside a command.
        let now_read_backs = block_read_backs();
        ran.push((name, now_read_backs - read_backs.get()));
        read_backs.set(now_read_backs);
        assert_eq!(
            rig.store.block_writes_outside_command(),
            0,
            "{name} wrote outside a command: {:?}",
            rig.store.block_tables_written_outside_command()
        );
        rig.check(&[now], name);
    };
    let route = "p0/m0";
    rig.store
        .upsert_fact(&fact("p0", "m0"), FactSource::Seed)
        .unwrap();
    step(
        "upsert_fact (catalogue trigger creates the route row)",
        &rig,
    );
    rig.store
        .apply_fusiform_catalog(
            &[crate::FusiformFactUpdate {
                auth_methods: None,
                login_methods_documented: false,
                provider_id: "p0".into(),
                model_id: "m1".into(),
                present: true,
                context_window: Some(64_000.0),
                max_output_tokens: Some(4_096),
                cost_tier: Some(1.0),
                cost_per_mtok_out: Some(1.0),
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: Some(false),
                supports_tool_use: Some(true),
                reasoning_efforts: None,
                catalog_efforts: None,
                canonical_id: None,
                upstream_status: None,
                model_type: None,
                model_kind: None,
                mode_of: None,
                lineage: None,
            }],
            crate::FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: now,
            },
        )
        .unwrap();
    step("apply_fusiform_catalog", &rig);
    rig.store
        .record_provider_cooldown(
            "p0",
            route,
            route,
            &RouteStateUpdate {
                observed_at_ms: Some(now),
                cooldown_until: Some((now + 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    step("record_provider_cooldown", &rig);
    for _ in 0..4 {
        rig.store
            .update_route_state(
                route,
                route,
                &RouteStateUpdate {
                    observed_at_ms: Some(now),
                    quota_exhausted_until: Some((now + 600_000) as f64),
                    latency_ema_ms: Some(10.0),
                    increment_failure: true,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    step("update_route_state", &rig);
    let decision = rig
        .store
        .record_decision(&DecisionInput {
            decision_id: Some("writer-decision".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            selected_route_key: Some(route.into()),
            selected_provider_id: Some("p0".into()),
            selected_model_id: Some("m0".into()),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    for _ in 0..4 {
        rig.store
            .set_decision_outcome(
                &decision,
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: striatum_core::RouteOutcome::Error,
                    last_error: Some("upstream error".into()),
                    observed_at_ms: now,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
    }
    step(
        "set_decision_outcome (terminal failures start a backoff)",
        &rig,
    );
    rig.store.clear_failure_backoff(route).unwrap();
    step("clear_failure_backoff", &rig);
    rig.store.clear_route_cooldowns(route).unwrap();
    step("clear_route_cooldowns", &rig);
    let refusal = CredentialRefusal {
        provider_id: "p0".into(),
        credential_id: "c0".into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    rig.store
        .record_credential_entitlement_fence(&refusal, None, now)
        .unwrap();
    step("record_credential_entitlement_fence", &rig);
    rig.store
        .touch_credential_entitlement_fence("p0", "c0", now + 1)
        .unwrap();
    step("touch_credential_entitlement_fence", &rig);
    rig.store
        .adopt_credential_entitlement_fingerprint("c0", "l1")
        .unwrap();
    step("adopt_credential_entitlement_fingerprint", &rig);
    rig.store
        .clear_credential_entitlement_fences("c0", Some("l1"))
        .unwrap();
    step("clear_credential_entitlement_fences", &rig);
    rig.store
        .record_credential_entitlement_fence(&refusal, None, now)
        .unwrap();
    rig.store
        .clear_credential_entitlement_fences_for_credential("c0")
        .unwrap();
    step("clear_credential_entitlement_fences_for_credential", &rig);
    // Source removal now drops the credential clock in the same write, so it
    // leaves no orphan for Tick. Real route/provider writers instead leave a
    // route clock whose identical provider wall hides its origin from sync's
    // live set. Tick must drop that clock, without dropping either source.
    prepare_covered_route_tick(&rig, now);
    read_backs.set(block_read_backs());
    rig.store
        .account_command(AccountCommandKind::Tick, move |tx| tx.tick(now + 1))
        .unwrap();
    step("tick (expiry releases, sync, retirement)", &rig);
    let after_covered_tick = rig.published();
    let keys = [
        PoolKey::new("k0", None, "active"),
        PoolKey::new("k1", None, "active"),
    ];
    rig.store
        .pool_wall(
            "p1",
            "k0",
            crate::PoolWallKind::RateLimited,
            now + 60_000,
            "429",
            now,
        )
        .unwrap();
    rig.store
        .pool_wall(
            "p1",
            "k0",
            crate::PoolWallKind::Billing,
            now + 60_000,
            "402",
            now,
        )
        .unwrap();
    step("pool_wall", &rig);
    let lease = match rig
        .store
        .pool_acquire(&crate::PoolAcquire {
            provider: "p1",
            session_id: "s0",
            keys: &keys,
            now_ms: now,
            lease_ms: 60_000,
            affinity_idle_ms: 60_000,
        })
        .unwrap()
    {
        crate::PoolAcquireOutcome::Leased(lease) => lease,
        other => panic!("one key is free: {other:?}"),
    };
    step("pool_acquire", &rig);
    rig.store
        .pool_renew("p1", "s0", &lease.lease_id, None, now + 1, 60_000)
        .unwrap();
    step("pool_renew", &rig);
    rig.store
        .pool_release("p1", "s0", Some(&lease.lease_id), true, now + 2)
        .unwrap();
    step("pool_release", &rig);
    rig.store.pool_key_states("p1", now + 3).unwrap();
    step("pool_key_states (syncs clocks when evidence changed)", &rig);
    // A seed fact no decision selected is pruned with its route row.
    rig.store
        .upsert_fact(&fact("p1", "m9"), FactSource::Seed)
        .unwrap();
    step("upsert_fact (an unserved seed route)", &rig);
    // The catalogue no longer lists it (setup: model_facts is no block table).
    rig.sql("UPDATE model_facts SET fusiform_present=0 WHERE route_key='p1/m9';".into());
    assert!(
        !rig.store
            .prune_unserved_unpicked_facts()
            .unwrap()
            .is_empty(),
        "the unserved seed route is pruned"
    );
    step("prune_unserved_unpicked_facts", &rig);
    rig.store
        .account_command(AccountCommandKind::Tick, move |tx| tx.tick(now + 7_200_000))
        .unwrap();
    step("final tick (reconciles the state with SQL)", &rig);
    assert_eq!(
        block_reconcile_reloads(),
        reloads,
        "the tick's reconcile found the state differing from SQL"
    );
    assert_eq!(ran.len(), 21, "{ran:?}");
    // Lease renewal and release write only the lease tables, the key-state
    // read syncs clocks only when their evidence changed, and the final tick
    // has nothing left to do: those may change no block row.
    let may_be_silent = [
        "pool_renew",
        "pool_release",
        "pool_key_states (syncs clocks when evidence changed)",
        "final tick (reconciles the state with SQL)",
    ];
    let silent: Vec<_> = ran
        .iter()
        .filter(|(name, rows)| *rows == 0 && !may_be_silent.contains(name))
        .collect();
    assert!(
        silent.is_empty(),
        "steps that changed no block row: {silent:?}"
    );
    assert_covered_route_tick(&after_covered_tick);
}

/// An authorized probe success clears its credential refusal at the success
/// report's captured time. The audit row must not sample a second clock.
#[test]
fn authorized_credential_heal_keeps_the_captured_report_time() {
    let rig = rig("credential-heal-event-time");
    due_credential_refusal(&rig, "event-time-login");
    let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
    assert_eq!(origins.len(), 1);
    assert!(rig
        .store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.grant_fence_rechecks(&origins, NOW, Some("event-time-owner"))
        })
        .unwrap());
    let report_time = NOW + 456;
    rig.store
        .account_command(AccountCommandKind::Report, move |tx| {
            tx.heal_authorized_probe("event-time-owner", "p0", "m0", report_time)
        })
        .unwrap();
    let row: (Option<i64>, Option<String>, bool, bool) = rig.store.with_reader_for_test(|conn| conn.query_row(
        "SELECT cleared_at,cleared_by,source_present,managed FROM block WHERE kind='credential' AND origin_key=json_array('p0','event-time-login')",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    )).unwrap();
    assert_eq!(
        row,
        (Some(report_time), Some("removed".into()), false, false),
        "credential audit keeps the captured report time"
    );
    assert!(rig
        .store
        .account_read(|tx| tx.fence_rechecks())
        .unwrap()
        .is_empty());
    rig.check(
        &[report_time],
        "credential healed at the captured report time",
    );
}

fn prepare_covered_route_tick(rig: &Rig, now: i64) {
    let route = "writer-tick/model";
    let update = RouteStateUpdate {
        observed_at_ms: Some(now),
        cooldown_until: Some((now + 60_000) as f64),
        last_error: Some("provider capacity".into()),
        ..Default::default()
    };
    rig.store
        .upsert_fact(&fact("writer-tick", "model"), FactSource::Seed)
        .unwrap();
    rig.store.update_route_state(route, route, &update).unwrap();
    rig.store
        .record_provider_cooldown("writer-tick", route, route, &update)
        .unwrap();
    let state = rig.published();
    assert!(
        state
            .clock("route", &serde_json::json!([route]).to_string())
            .is_some(),
        "the real route write left a held clock"
    );
    assert!(
        !state
            .view_origins()
            .iter()
            .any(|origin| origin.kind == "route" && origin.provider == "writer-tick"),
        "the later matching provider wall hides that origin"
    );
    assert!(state
        .clock("provider", &serde_json::json!(["writer-tick"]).to_string())
        .is_some());
}

fn assert_covered_route_tick(state: &BlockState) {
    let key = serde_json::json!(["writer-tick/model"]).to_string();
    assert!(
        state.clock("route", &key).is_none(),
        "Tick dropped the covered route clock"
    );
    assert!(
        state
            .block("route", &key)
            .is_some_and(|row| row.source_present),
        "the covered route source survives"
    );
    assert!(
        state
            .clock("provider", &serde_json::json!(["writer-tick"]).to_string())
            .is_some(),
        "the covering provider clock survives"
    );
}

/// Migration removes a deleted source's orphan clock from memory. Separate
/// connections then remove and restore the native source across the writer's
/// freshness checks; the restored live clock must not be dropped by sync.
#[test]
fn stale_sync_drop_migration_representability_probe() {
    fn migrated(source_recreated: bool, at: i64, changed_old_evidence: bool) -> Rig {
        let scratch = cortexkit_test_support::ScratchDir::new("stale-sync-drop-migration");
        let descriptor = crate::dev_descriptor(
            "stale-sync-drop-migration",
            scratch.path().join("state.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
        store
            .with_writer_for_test(|conn| {
                let insert = || {
                    conn.execute(
                        "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,
                         wall_observed_at,last_error,last_error_at,updated_at)
                         VALUES ('p0',?1,?2,'provider capacity',?2,?2)",
                        rusqlite::params![at + 3_600_000, at],
                    )
                };
                insert()?;
                fence_rechecks::sync(conn)?;
                conn.execute("DELETE FROM provider_cooldown_state", [])?;
                if source_recreated {
                    insert()?;
                }
                if changed_old_evidence {
                    assert_eq!(conn.execute("UPDATE fence_recheck SET evidence=evidence||' ' WHERE kind='provider'", [])?, 1);
                }
                let source: Option<String> = conn
                    .query_row(
                        "SELECT evidence FROM fence_origin WHERE kind='provider'",
                        [],
                        |r| r.get(0),
                    )
                    .optional()?;
                let clock: String = conn.query_row(
                    "SELECT evidence FROM fence_recheck WHERE kind='provider'",
                    [],
                    |r| r.get(0),
                )?;
                assert_eq!(clock.ends_with(' '), changed_old_evidence);
                eprintln!("legacy source_recreated={source_recreated}, changed_old_evidence={changed_old_evidence}: source={source:?}, clock={clock}");
                Ok(())
            })
            .unwrap();
        drop(store);
        let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
        store
            .with_reader_for_test(|conn| {
                let native: (String, bool, bool, Option<String>) = conn.query_row(
                    "SELECT evidence,source_present,managed,cleared_by FROM block WHERE kind='provider'",
                    [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )?;
                eprintln!("migrated source_recreated={source_recreated}, changed_old_evidence={changed_old_evidence}: {native:?}");
                assert_eq!(native.0.ends_with(' '), changed_old_evidence);
                assert_eq!(native.1, source_recreated);
                assert_eq!(native.2, source_recreated);
                Ok(())
            })
            .unwrap();
        let owner = Arc::new(TestOwner::default());
        store.install_account_state_bridge(owner.clone()).unwrap();
        Rig {
            store,
            owner,
            _scratch: scratch,
        }
    }
    let at = crate::now_ms();
    let before = migrated(false, at, false);
    assert!(before.published().view_origins().is_empty());
    assert_eq!(before.published().stored_clocks().count(), 0);
    let changed_orphan = migrated(false, at, true);
    assert_eq!(before.published(), changed_orphan.published(),
        "stale-sync-drop-evidence: changing the removed audit clock does not create a working clock");
    let after = migrated(true, at, false);
    assert_eq!(after.published().view_origins().len(), 1);
    assert_eq!(after.published().stored_clocks().count(), 1);
    let path = std::fs::canonicalize(after._scratch.path().join("state.db")).unwrap();
    let outside = Connection::open(&path).unwrap();
    outside
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let loads = after.owner.loads.load(Ordering::Relaxed);
    // This commit makes refresh load the source's removed state. The second
    // commit below happens after that load, so only the locked check sees it.
    assert_eq!(
        outside
            .execute(
                "UPDATE block SET source_present=0,managed=0,cleared_by='removed',cleared_at=?1
             WHERE kind='provider' AND origin_key=json_array('p0')",
                [at],
            )
            .unwrap(),
        1
    );
    let owner = after.owner.clone();
    let restored = Arc::new(AtomicBool::new(false));
    let hook_ran = restored.clone();
    let hook_path = path.clone();
    crate::account_writer::BEFORE_BEGIN.lock().unwrap().push((
        path,
        Box::new(move || {
            assert_eq!(owner.loads.load(Ordering::Relaxed), loads + 1);
            let state = owner.published.lock().unwrap().clone().unwrap();
            assert!(state.0.view_origins().is_empty());
            assert_eq!(state.0.stored_clocks().count(), 0);
            let conn = Connection::open(hook_path).unwrap();
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert_eq!(
                conn.execute(
                    "UPDATE block SET source_present=1,managed=1,cleared_by=NULL,cleared_at=NULL
                     WHERE kind='provider' AND origin_key=json_array('p0')",
                    [],
                )
                .unwrap(),
                1
            );
            hook_ran.store(true, Ordering::Relaxed);
        }),
    ));
    after
        .store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.sync_fence_rechecks()
        })
        .unwrap();
    assert_eq!(
        after.loads_since(loads),
        2,
        "stale-sync-drop: the original stale decision must still force a fresh load"
    );
    assert!(restored.load(Ordering::Relaxed), "the outside commit ran");
    let clocks = after.store.fence_rechecks().unwrap();
    assert_eq!(clocks.len(), 1, "the restored clock was not dropped");
    assert_eq!(clocks[0].kind, "provider");
    assert_eq!(clocks[0].origin_key, r#"["p0"]"#);
    assert!(after.owner.stale.lock().unwrap().is_empty());
    after.check(&[at], "stale-sync-drop after the real outside commit");
}

/// Migration puts new source evidence and its pending retry schedule in one
/// block row. A replaced clock that was never claimed does not keep separate
/// evidence or a separate retry time on that row.
#[test]
fn stale_sync_renew_guard_migration_representability_probe() {
    fn migrated(name: &str, drift: Option<&str>, wall: i64) -> (Rig, String, String) {
        let scratch = cortexkit_test_support::ScratchDir::new(name);
        let descriptor =
            crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy());
        let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
        let legacy_clock = store.with_writer_for_test(|conn| {
            conn.execute(
                "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
                 VALUES ('p0',?1,?2,'provider capacity',?2,?2)",
                rusqlite::params![wall + 3_600_000, wall],
            )?;
            fence_rechecks::sync(conn)?;
            conn.execute(
                "UPDATE provider_cooldown_state SET cooldown_until=?1,wall_observed_at=?2,last_error='provider capacity' WHERE provider_key='p0'",
                rusqlite::params![wall + 7_200_000, wall + 1],
            )?;
            if let Some(sql) = drift { conn.execute_batch(sql)?; }
            let source: String = conn.query_row("SELECT evidence FROM fence_origin WHERE kind='provider'", [], |row| row.get(0))?;
            let clock: String = conn.query_row(
                "SELECT json_array(evidence,next_recheck_at,last_claimed_at,claim_generation,claim_owner) FROM fence_recheck WHERE kind='provider'",
                [], |row| row.get(0),
            )?;
            eprintln!("{name}: legacy source={source}, clock={clock}");
            assert_eq!(source, serde_json::json!([wall + 7_200_000, null, wall + 1]).to_string());
            Ok(clock)
        }).unwrap();
        drop(store);
        let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
        let native: String = store.with_reader_for_test(|conn| conn.query_row(
            "SELECT json_array(evidence,next_recheck_at,last_claimed_at,claim_generation,claim_owner,source_present,managed)
             FROM block WHERE kind='provider' AND origin_key=json_array('p0')",
            [], |row| row.get(0),
        )).unwrap();
        eprintln!("{name}: migrated pending row={native}");
        let owner = Arc::new(TestOwner::default());
        store.install_account_state_bridge(owner.clone()).unwrap();
        (
            Rig {
                store,
                owner,
                _scratch: scratch,
            },
            legacy_clock,
            native,
        )
    }
    let wall = crate::now_ms();
    for (name, drift) in [
        (
            "stale-sync-renew-evidence",
            "UPDATE fence_recheck SET evidence=evidence||' ' WHERE kind='provider';",
        ),
        (
            "stale-sync-renew-schedule",
            "UPDATE fence_recheck SET next_recheck_at=next_recheck_at-1 WHERE kind='provider';",
        ),
    ] {
        let (before, old_clock, native_before) = migrated(&format!("{name}-before"), None, wall);
        let (after, changed_clock, native_after) =
            migrated(&format!("{name}-after"), Some(drift), wall);
        assert_ne!(
            old_clock, changed_clock,
            "{name}: the original clock input really changed"
        );
        let expected = serde_json::json!([
            serde_json::json!([wall + 7_200_000, null, wall + 1]).to_string(),
            wall + 3_600_001,
            null,
            0,
            null,
            1,
            0
        ])
        .to_string();
        assert_eq!(
            native_before, expected,
            "{name}: the source replaces the old unclaimed clock"
        );
        assert_eq!(
            native_after, expected,
            "{name}: migration folds the old clock-only change"
        );
        assert_eq!(
            before.published(),
            after.published(),
            "{name}: the migrated working inputs are identical"
        );
    }
}

/// Install the state owner only after upgrade: the earlier schema has no
/// block table from which to load and publish captured routing state.
fn legacy_sync_rig(name: &str) -> Rig {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let descriptor = crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy());
    let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
    Rig {
        store,
        owner: Arc::new(TestOwner::default()),
        _scratch: scratch,
    }
}

/// Imported future refusals can expire before their first tick registers a
/// clock. Retiring the provider must not invalidate the same tick's check of
/// a different-cause route on that provider.
#[test]
fn sync_retires_provider_and_independent_route_without_invalidating_its_own_state() {
    let name = "sync-own-retirement";
    let legacy = legacy_sync_rig(name);
    let observed = crate::now_ms();
    let deadline = observed + 5_000;
    legacy.sql(format!(
        "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
         VALUES ('retire-self',{deadline}, {observed},'provider capacity',{observed},{observed});
         INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
         VALUES ('retire-self/model','retire-self/model','retire-self','model',{}, {observed},'route capacity',{observed},{observed});",
        deadline - 500,
    ));
    let rig = upgrade_sync_rig(legacy, name);
    assert!(
        crate::now_ms() < deadline - 500,
        "migration completed while both imported refusals were still live"
    );
    assert_eq!(rig.published().view_origins().len(), 2);
    assert_eq!(rig.published().stored_clocks().count(), 0);
    // Tick uses the actual clock for synchronization, not its expiry argument.
    // Let the imported evidence expire naturally; no freshness guard is bypassed.
    let remaining = (deadline + 1).saturating_sub(crate::now_ms());
    if remaining > 0 {
        std::thread::sleep(std::time::Duration::from_millis(remaining as u64));
    }
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.store
        .account_command(AccountCommandKind::Tick, |tx| tx.tick(crate::now_ms()))
        .unwrap();
    assert_eq!(
        rig.loads_since(loads),
        0,
        "fresh state is not retried because of the tick's own writes"
    );
    assert!(rig.published().view_origins().is_empty());
    assert_eq!(rig.published().stored_clocks().count(), 0);
    let retired: i64 = rig.store.with_reader_for_test(|conn| conn.query_row(
        "SELECT count(*) FROM retirement_tombstone WHERE kind IN ('provider','route') AND json_extract(origin_key,'$[0]') LIKE 'retire-self%'",
        [], |row| row.get(0),
    )).unwrap();
    assert_eq!(
        retired, 2,
        "both imported evidence generations were retired"
    );
    rig.check(&[deadline + 1], "after retiring both independent sources");
}

#[test]
fn retirement_records_sync_time_without_rewriting_source_evidence_time() {
    let rig = rig("retirement-sync-time");
    let synced_at = 1_800_000_000_000i64;
    let evidence_at = synced_at - 3_600_000;
    rig.store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.pool_sql(|conn| {
                conn.execute(
                    "INSERT INTO provider_cooldown_state(provider_key,wall_observed_at,updated_at)
                     VALUES ('retirement-time',?1,?1)",
                    [evidence_at],
                )?;
                let row = crate::block_storage::wall_source(
                    conn,
                    crate::block_storage::WallSource {
                        kind: "provider",
                        key: "retirement-time",
                        provider: "retirement-time",
                        model: None,
                        cooldown: Some((synced_at - 1) as f64),
                        exhausted: None,
                        wall_observed: Some(evidence_at),
                        observed_at: evidence_at,
                        error: None,
                        class: None,
                        unscoped: false,
                    },
                )?
                .expect("the fixture supplies a provider deadline");
                crate::block_storage::write_source(conn, &row)?;
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(rig.published().view_origins().len(), 1);
    assert!(rig
        .published()
        .clock("provider", r#"["retirement-time"]"#)
        .is_none());
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            fence_rechecks::sync_on(tx, synced_at).map_err(err)
        })
        .unwrap();
    assert_eq!(rig.owner.loads.load(Ordering::Relaxed), loads);
    // Retention ages a receipt from retirement, not from the older refusal.
    // Preserve both times so a delayed sync cannot shorten that retention.
    rig.store
        .with_reader_for_test(|conn| {
            let (stored_evidence_at, retired_at): (i64, i64) = conn.query_row(
                "SELECT evidence_at,retired_at FROM retirement_tombstone
             WHERE kind='provider' AND origin_key=json_array('retirement-time')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(stored_evidence_at, evidence_at);
            assert_eq!(retired_at, synced_at);
            Ok(())
        })
        .unwrap();
    rig.check_native("after retiring at the supplied synchronization time");
}

/// Give the tick an externally superseded snapshot to exercise its keyed
/// guards independently of the writer's earlier data-version refresh. A
/// nontransactional counter also catches attempted tombstones rolled back
/// later: checking only final SQL would miss those writes.
#[test]
fn retirement_preflight_rejects_each_stale_input_before_any_tombstone() {
    let observed = crate::now_ms();
    let deadline = observed + 5_000;
    let mut cases = Vec::new();
    for (input, stale_index) in ["source", "clock"]
        .into_iter()
        .flat_map(|input| (0..3).map(move |index| (input, index)))
    {
        let name = format!("retirement-preflight-{input}-{stale_index}");
        let legacy = legacy_sync_rig(&name);
        for index in 0..3 {
            legacy.sql(format!("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
                VALUES ('retire-{index}',{deadline},{observed},'provider capacity',{observed},{observed});"));
        }
        let rig = upgrade_sync_rig(legacy, &name);
        let stale = rig.published();
        let order: Vec<_> = stale
            .view_origins_in_scan_order()
            .into_iter()
            .map(|row| row.origin_key)
            .collect();
        assert_eq!(
            order,
            (0..3)
                .map(|index| serde_json::json!([format!("retire-{index}")]).to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(stale.stored_clocks().count(), 0);
        // The real outside commit changes source or clock data in place. Keep the store's
        // published state fresh; only the guard-level tick gets the old copy.
        let conn = Connection::open(rig._scratch.path().join("state.db")).unwrap();
        let changed = if input == "source" {
            "evidence=evidence||' '"
        } else {
            "managed=1,next_recheck_at=deadline"
        };
        conn.execute(
            &format!(
                "UPDATE block SET {changed} WHERE kind='provider' AND origin_key=json_array(?1)"
            ),
            [format!("retire-{stale_index}")],
        )
        .unwrap();
        drop(conn);
        rig.reload();
        assert_eq!(rig.published(), rig.fresh());
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = attempts.clone();
        rig.run(move |conn| {
            let count = count.clone();
            conn.create_scalar_function("count_retirement_attempt", 0, rusqlite::functions::FunctionFlags::SQLITE_UTF8, move |_| {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(0i64)
            })?;
            conn.execute_batch("CREATE TEMP TRIGGER count_retirement_attempt BEFORE INSERT ON main.retirement_tombstone BEGIN SELECT count_retirement_attempt(); END;")
        });
        cases.push((rig, stale, attempts, input, stale_index));
    }
    assert!(
        crate::now_ms() < deadline,
        "all fixtures migrated before expiry"
    );
    let remaining = (deadline + 1).saturating_sub(crate::now_ms());
    if remaining > 0 {
        std::thread::sleep(std::time::Duration::from_millis(remaining as u64));
    }
    for (rig, stale, attempts, input, stale_index) in cases {
        let before = rig.published();
        let error = rig
            .store
            .account_command(AccountCommandKind::Tick, move |tx| {
                let tick = crate::AccountTx::with_blocks(tx.conn, Some(&stale));
                let error = tick
                    .tick(crate::now_ms())
                    .expect_err("an external source change must reject the captured plan");
                Ok(error.to_string())
            })
            .unwrap();
        assert!(error.contains("account state diverged from SQL"), "{error}");
        assert!(
            error.contains(&format!(
                "the {} of provider [\"retire-{stale_index}\"] differs",
                if input == "source" { "source" } else { "clock" }
            )),
            "{error}"
        );
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            0,
            "stale {input} {stale_index}: no tombstone was attempted, even before rollback"
        );
        let tombstones: i64 = rig
            .store
            .with_reader_for_test(|conn| {
                conn.query_row("SELECT count(*) FROM retirement_tombstone", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(tombstones, 0);
        assert_eq!(rig.published(), before, "tick left memory unchanged");
        assert_eq!(rig.fresh(), before, "tick left SQL unchanged");
    }
}

fn upgrade_sync_rig(rig: Rig, name: &str) -> Rig {
    let Rig {
        store,
        owner,
        _scratch: scratch,
    } = rig;
    drop(store);
    let descriptor = crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy());
    let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 96).unwrap();
    store.install_account_state_bridge(owner.clone()).unwrap();
    Rig {
        store,
        owner,
        _scratch: scratch,
    }
}

/// A provider-covered route keeps a stored clock even though its origin is
/// hidden from the source view. Unlike a clock with no source, it survives
/// migration and can still reach synchronization's evidence-guarded drop.
fn covered_route_sync_rig(name: &str, wall: i64) -> Rig {
    let rig = legacy_sync_rig(name);
    rig.sql(format!(
        "INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,
           cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
         VALUES ('p0/m0','p0/m0','p0','m0',{}, {wall},'provider capacity',{wall},{wall});",
        wall + 3_600_000,
    ));
    rig.run(|conn| fence_rechecks::legacy::register(conn, "route", r#"["p0/m0"]"#));
    provider_wall(&rig, wall + 7_200_000, wall, "'provider capacity'");
    rig.run(|conn| fence_rechecks::legacy::register(conn, "provider", r#"["p0"]"#));
    upgrade_sync_rig(rig, name)
}

/// Schedule two meaningful commits from other connections. Refresh first
/// loads the removed source; the commit before BEGIN then restores it, so
/// the locked data-version check must load its current clock before deciding.
fn real_sync_case_change(state: &Rig, before: &Rig, name: &str) -> (u64, Arc<AtomicBool>) {
    let frame: Vec<Value> = state
        .store
        .with_reader_for_test(|conn| {
            conn.query_row(
                "SELECT managed,quota,classified,next_recheck_at,reset_check_at,last_claimed_at,
           claim_generation,claim_owner,cleared_at,cleared_by,cleared_models,source_present
         FROM block WHERE kind='provider' AND origin_key=json_array('p0')",
                [],
                |row| (0..12).map(|column| row.get(column)).collect(),
            )
        })
        .unwrap();
    let previous_error: Option<Option<String>> = before
        .store
        .with_reader_for_test(|conn| {
            conn.query_row(
                "SELECT last_error FROM provider_cooldown_state WHERE provider_key='p0'",
                [],
                |row| row.get(0),
            )
            .optional()
        })
        .unwrap();
    let restored_error: Option<String> = state
        .store
        .with_reader_for_test(|conn| {
            conn.query_row(
                "SELECT last_error FROM provider_cooldown_state WHERE provider_key='p0'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    let path = std::fs::canonicalize(state._scratch.path().join("state.db")).unwrap();
    let loads = state.owner.loads.load(Ordering::Relaxed);
    let outside = Connection::open(&path).unwrap();
    outside
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let tx = crate::transactions::write_transaction(&outside).unwrap();
    assert_eq!(
        tx.execute(
            "UPDATE block SET source_present=0,managed=0,cleared_at=?1,cleared_by='removed'
        WHERE kind='provider' AND origin_key=json_array('p0')",
            [crate::now_ms()]
        )
        .unwrap(),
        1
    );
    if let Some(error) = previous_error {
        tx.execute(
            "UPDATE provider_cooldown_state SET last_error=?1 WHERE provider_key='p0'",
            [error],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let owner = state.owner.clone();
    let committed = Arc::new(AtomicBool::new(false));
    let hook_ran = committed.clone();
    let label = name.to_owned();
    let outside_path = path.clone();
    crate::account_writer::BEFORE_BEGIN.lock().unwrap().push((
        path,
        Box::new(move || {
            assert_eq!(
                owner.loads.load(Ordering::Relaxed),
                loads + 1,
                "{label}: refresh loaded once"
            );
            let published = owner.published.lock().unwrap().clone().unwrap();
            assert!(
                published.0.view_origins().is_empty(),
                "{label}: removed source"
            );
            assert_eq!(
                published.0.stored_clocks().count(),
                0,
                "{label}: removed clock"
            );
            let conn = Connection::open(outside_path).unwrap();
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let tx = crate::transactions::write_transaction(&conn).unwrap();
            assert_eq!(
                tx.execute(
                    "UPDATE block SET managed=?1,quota=?2,classified=?3,next_recheck_at=?4,
            reset_check_at=?5,last_claimed_at=?6,claim_generation=?7,claim_owner=?8,cleared_at=?9,
            cleared_by=?10,cleared_models=?11,source_present=?12
            WHERE kind='provider' AND origin_key=json_array('p0')",
                    rusqlite::params_from_iter(frame.iter())
                )
                .unwrap(),
                1
            );
            tx.execute(
                "UPDATE provider_cooldown_state SET last_error=?1 WHERE provider_key='p0'",
                [restored_error],
            )
            .unwrap();
            tx.commit().unwrap();
            hook_ran.store(true, Ordering::Relaxed);
        }),
    ));
    (loads, committed)
}

fn sync_case_outcome(state: &Rig, name: &str, committed: Option<&AtomicBool>) {
    if let Some(committed) = committed {
        assert!(
            committed.load(Ordering::Relaxed),
            "{name}: the outside commit ran"
        );
        assert!(
            state.owner.stale.lock().unwrap().is_empty(),
            "{name}: no injected load"
        );
    }
    let published = state.published();
    assert_eq!(
        published.stored_clocks().count(),
        1,
        "{name}: one provider clock remains"
    );
    assert!(
        published.clock("provider", r#"["p0"]"#).is_some(),
        "{name}: provider clock preserved"
    );
    if name == "stale-sync-drop-evidence" {
        assert!(
            published.clock("route", r#"["p0/m0"]"#).is_none(),
            "the covered clock was dropped"
        );
        assert!(
            published.source_record("route", r#"["p0/m0"]"#).is_some(),
            "its source was not deleted"
        );
    }
}

/// Migrate the original before/after fixtures, then check the corresponding
/// guarded decision against independent SQL. Cases whose earlier state has
/// already been removed by migration use real outside-connection commits.
fn stale_sync_case(name: &str, before: impl Fn(&Rig), change: impl Fn(&Rig)) {
    let earlier_name = format!("{name}-before");
    let oracle_name = format!("{name}-oracle");
    let earlier = legacy_sync_rig(&earlier_name);
    let state = legacy_sync_rig(name);
    let oracle = legacy_sync_rig(&oracle_name);
    before(&earlier);
    before(&state);
    before(&oracle);
    change(&state);
    change(&oracle);
    let earlier = upgrade_sync_rig(earlier, &earlier_name);
    let mut state = upgrade_sync_rig(state, name);
    let mut oracle = upgrade_sync_rig(oracle, &oracle_name);
    let stale = if name == "stale-sync-drop-evidence" {
        let wall: i64 = state
            .store
            .with_reader_for_test(|conn| {
                conn.query_row(
            "SELECT evidence_at FROM block WHERE kind='provider' AND origin_key=json_array('p0')",
            [], |row| row.get(0),
        )
            })
            .unwrap();
        state = covered_route_sync_rig(&format!("{name}-covered"), wall);
        oracle = covered_route_sync_rig(&format!("{name}-covered-oracle"), wall);
        let stale = state.published();
        for rig in [&state, &oracle] {
            rig.sql("UPDATE block SET evidence=evidence||' ' WHERE kind='route' AND origin_key=json_array('p0/m0');".into());
        }
        stale
    } else {
        for rig in [&state, &oracle] {
            match name {
                "stale-sync-renew-evidence" => rig.sql("UPDATE block SET evidence=evidence||' ' WHERE kind='provider' AND origin_key=json_array('p0') AND managed=0;".into()),
                "stale-sync-renew-schedule" => rig.sql("UPDATE block SET next_recheck_at=next_recheck_at-1 WHERE kind='provider' AND origin_key=json_array('p0') AND managed=0;".into()),
                _ => {}
            }
        }
        earlier.published()
    };
    let (loads, committed) = if matches!(
        name,
        "stale-sync-drop"
            | "stale-sync-retire"
            | "stale-sync-retire-clock-only"
            | "stale-sync-quarantine"
    ) {
        let (loads, committed) = real_sync_case_change(&state, &earlier, name);
        (loads, Some(committed))
    } else {
        (state.decide_next_on(stale), None)
    };
    state
        .store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.sync_fence_rechecks()
        })
        .unwrap();
    assert_eq!(
        state.loads_since(loads),
        2,
        "{name}: the stale decision is caught and the batch runs on a fresh load"
    );
    sync_case_outcome(&state, name, committed.as_deref());
    oracle.run(|conn| native_sql::synchronize(conn, crate::now_ms()));
    // Compare current source/clock rows, provider metadata and retirement
    // tombstones. Removed audit rows are outside the loaded routing state.
    let rows = |rig: &Rig| {
        let mut rows = rig.store.with_reader_for_test(native_sql::rows).unwrap();
        rows.extend(
            rig.store
                .with_reader_for_test(|conn| Ok(block_rows(conn)))
                .unwrap()
                .unwrap()
                .into_iter()
                .filter(|row| row.starts_with("provider_cooldown_state ")),
        );
        rows.extend(
            rig.store
                .with_reader_for_test(|conn| {
                    conn.prepare(
                        "SELECT kind,origin_key,evidence FROM retirement_tombstone ORDER BY rowid",
                    )?
                    .query_map([], |r| {
                        Ok(format!(
                            "retired {:?}",
                            (
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?
                            )
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap(),
        );
        rows
    };
    let (from_state, from_sql) = (rows(&state), rows(&oracle));
    let only_state: Vec<_> = from_state
        .iter()
        .filter(|row| !from_sql.contains(row))
        .collect();
    let only_sql: Vec<_> = from_sql
        .iter()
        .filter(|row| !from_state.contains(row))
        .collect();
    assert_eq!(
        from_state, from_sql,
        "{name}: the rows a SQL sync leaves; only after the state's sync: {only_state:#?}; only after SQL's: {only_sql:#?}"
    );
    state.check(&[NOW], name);
}

/// A provider wall written as raw rows, without the clock a writer would
/// register: until `until`, observed at `observed`, with `error`.
fn provider_wall(rig: &Rig, until: i64, observed: i64, error: &str) {
    rig.sql(format!(
        "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,last_error,last_error_at,updated_at)
         VALUES ('p0',{until},{observed},{error},{observed},{observed})
         ON CONFLICT(provider_key) DO UPDATE SET cooldown_until=excluded.cooldown_until,
         wall_observed_at=excluded.wall_observed_at,last_error=excluded.last_error;"
    ));
}

/// `sync` decides on the command's working state. Each decision it takes
/// (archive an expired origin, install a clock, renew a clock for new
/// evidence, drop a clock whose origin is gone, drop a quarantined row's
/// clock) first checks the rows it decided on against SQL, so a state that
/// drifted from SQL runs the batch again instead of writing a wrong clock or
/// ending a live source.
#[test]
fn sync_decisions_taken_on_a_stale_state_run_again_on_a_fresh_load() {
    let wall = crate::now_ms();
    let capacity = "'provider capacity'";
    let sync = |rig: &Rig| rig.run(fence_rechecks::sync);
    stale_sync_case(
        "stale-sync-install",
        |rig| provider_wall(rig, wall + 3_600_000, wall, capacity),
        |rig| provider_wall(rig, wall + 7_200_000, wall, capacity),
    );
    stale_sync_case(
        "stale-sync-renew",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            provider_wall(rig, wall + 7_200_000, wall, capacity);
        },
        |rig| {
            rig.sql(format!(
                "UPDATE fence_recheck SET last_claimed_at={wall},claim_owner='other',
                 claim_generation=claim_generation+1 WHERE kind='provider';"
            ))
        },
    );
    stale_sync_case(
        "stale-sync-drop",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            rig.sql("DELETE FROM provider_cooldown_state;".into());
        },
        |rig| provider_wall(rig, wall + 3_600_000, wall, capacity),
    );
    stale_sync_case(
        "stale-sync-retire",
        |rig| provider_wall(rig, wall - 3_600_000, wall - 7_200_000, capacity),
        |rig| provider_wall(rig, wall + 3_600_000, wall - 7_200_000, capacity),
    );
    // One case per keyed guard on the clock row, with the source rows
    // agreeing: retirement's clock check (SQL holds a clock the state does
    // not), the install's insert-changed-nothing check, the renewal's
    // evidence and next_recheck_at guards, and the drop's evidence guard.
    let register =
        |rig: &Rig| rig.run(|conn| fence_rechecks::legacy::register(conn, "provider", r#"["p0"]"#));
    stale_sync_case(
        "stale-sync-retire-clock-only",
        |rig| provider_wall(rig, wall - 3_600_000, wall - 7_200_000, capacity),
        register,
    );
    stale_sync_case(
        "stale-sync-install-conflict",
        |rig| provider_wall(rig, wall + 3_600_000, wall, capacity),
        register,
    );
    stale_sync_case(
        "stale-sync-renew-evidence",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            provider_wall(rig, wall + 7_200_000, wall + 1, capacity);
        },
        |rig| {
            rig.sql("UPDATE fence_recheck SET evidence=evidence||' ' WHERE kind='provider';".into())
        },
    );
    stale_sync_case(
        "stale-sync-renew-schedule",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            provider_wall(rig, wall + 7_200_000, wall + 1, capacity);
        },
        |rig| {
            rig.sql(
                "UPDATE fence_recheck SET next_recheck_at=next_recheck_at-1 WHERE kind='provider';"
                    .into(),
            )
        },
    );
    stale_sync_case(
        "stale-sync-drop-evidence",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            rig.sql("DELETE FROM provider_cooldown_state;".into());
        },
        |rig| {
            rig.sql("UPDATE fence_recheck SET evidence=evidence||' ' WHERE kind='provider';".into())
        },
    );
    stale_sync_case(
        "stale-sync-quarantine",
        |rig| {
            provider_wall(rig, wall + 3_600_000, wall, capacity);
            sync(rig);
            provider_wall(rig, wall + 3_600_000, wall, ERRORS[3]);
        },
        |rig| provider_wall(rig, wall + 3_600_000, wall, capacity),
    );
}

/// A sync on a state that agrees with SQL writes exactly what the SQL sync
/// writes, over the generated sequence: after every step, each store syncs
/// (the first on its state, the second on SQL inside a savepoint it rolls
/// back) and their block rows and archive rows (all five columns) must be
/// equal; then `register` of every view origin is compared the same way.
/// Before that sync, `register` alone is compared too: it installs and
/// renews clocks without retiring expired origins, which a sync run first
/// would already have done.
#[test]
fn sync_and_register_decided_on_the_state_equal_sql_over_generated_writes() {
    let mut compared = 0usize;
    // Registers alone that changed rows: installs, renewals of a clock whose
    // evidence moved, and installs for an origin already past its deadline.
    let (mut installs, mut renewals, mut expired) = (0usize, 0usize, 0usize);
    let mut archived = 0usize;
    let archive = |conn: &Connection| -> RouteStateResult<Vec<String>> {
        conn.prepare("SELECT kind,origin_key,evidence,evidence_at,retired_at FROM retirement_tombstone ORDER BY rowid")
            .map_err(err)?
            .query_map([], |r| {
                Ok(format!(
                    "retired {:?}",
                    (r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?)
                ))
            })
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(err)
    };
    for seed in 1..=4u64 {
        let rig = rig(&format!("block-state-sync-{seed}"));
        let mut rng = Rng(0x5851_f42d_4c95_7f2d ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut now = NOW;
        for index in 0..100 {
            let operation = step(&rig, &mut rng, now);
            // Every other step only: a register installs a clock for each
            // origin, and an origin a clock holds is never archived, so the
            // other steps leave new origins for the sync below to archive.
            if index % 2 == 0 {
                let witnesses = rig.owner.renewal_witnesses.lock().unwrap().clone();
                let (sql, memory, counts) = rig
                    .store
                    .account_command(AccountCommandKind::Maintenance, move |tx| {
                        let origins: Vec<(String, String, bool, bool)> = tx
                            .working_blocks()?
                            .map(|state| {
                                state
                                    .view_origins_in_scan_order()
                                    .into_iter()
                                    .map(|origin| {
                                        let renew = native_generator::renewed(
                                            &witnesses,
                                            &origin.kind,
                                            &origin.origin_key,
                                            &origin.evidence,
                                        );
                                        let lapsed = origin
                                            .deadline
                                            .is_some_and(|deadline| deadline <= now as f64);
                                        (origin.kind, origin.origin_key, renew, lapsed)
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let run = |tx: &crate::AccountTx<'_>| -> RouteStateResult<(
                                Vec<String>,
                                (usize, usize, usize),
                            )> {
                                let mut rows = Vec::new();
                                let mut counts = (0, 0, 0);
                                let mut last = block_rows(tx.conn)?;
                                for (kind, key, renew, lapsed) in &origins {
                                    fence_rechecks::register_on(tx, kind, key).map_err(err)?;
                                    let after = block_rows(tx.conn)?;
                                    if after != last {
                                        counts.0 += 1;
                                        counts.1 += usize::from(*renew);
                                        counts.2 += usize::from(*lapsed);
                                    }
                                    rows.extend(after.iter().cloned());
                                    last = after;
                                }
                                rows.extend(archive(tx.conn)?);
                                Ok((rows, counts))
                            };
                        tx.conn.execute_batch("SAVEPOINT oracle").map_err(err)?;
                        let sql = run(&crate::AccountTx::sql(tx.conn))?.0;
                        tx.conn
                            .execute_batch("ROLLBACK TO oracle; RELEASE oracle")
                            .map_err(err)?;
                        let (memory, counts) = run(tx)?;
                        Ok((sql, memory, counts))
                    })
                    .unwrap();
                assert_eq!(
                    memory, sql,
                    "seed={seed} step={index} op={operation}: register alone"
                );
                installs += counts.0;
                renewals += counts.1;
                expired += counts.2;
                rig.check(&[now], &format!("seed={seed} step={index} after register"));
            }
            // At, an hour after or two hours after the generated evidence:
            // late enough that expired origins are archived.
            let at = now + [0, 3_600_000, 7_200_000][rng.below(3) as usize];
            let (sql, memory, origins) = rig
                .store
                .account_command(AccountCommandKind::Maintenance, move |tx| {
                    let origins: Vec<(String, String)> = tx
                        .working_blocks()?
                        .map(|state| {
                            state
                                .view_origins_in_scan_order()
                                .into_iter()
                                .map(|origin| (origin.kind, origin.origin_key))
                                .collect()
                        })
                        .unwrap_or_default();
                    let run = |tx: &crate::AccountTx<'_>| -> RouteStateResult<Vec<String>> {
                        fence_rechecks::sync_on(tx, at).map_err(err)?;
                        let mut rows = block_rows(tx.conn)?;
                        rows.extend(archive(tx.conn)?);
                        for (kind, key) in &origins {
                            fence_rechecks::register_on(tx, kind, key).map_err(err)?;
                            rows.extend(block_rows(tx.conn)?);
                        }
                        Ok(rows)
                    };
                    tx.conn.execute_batch("SAVEPOINT oracle").map_err(err)?;
                    let sql = run(&crate::AccountTx::sql(tx.conn))?;
                    tx.conn
                        .execute_batch("ROLLBACK TO oracle; RELEASE oracle")
                        .map_err(err)?;
                    Ok((sql, run(tx)?, origins.len()))
                })
                .unwrap();
            assert_eq!(memory, sql, "seed={seed} step={index} op={operation}");
            compared += origins;
            archived = rig
                .store
                .with_reader_for_test(|conn| {
                    conn.query_row("SELECT count(*) FROM retirement_tombstone", [], |r| {
                        r.get::<_, usize>(0)
                    })
                })
                .unwrap()
                .max(archived);
            rig.check(&[now], &format!("seed={seed} step={index} after sync"));
            now += rng.below(3) as i64 * 600_000;
        }
    }
    assert!(compared > 100, "registers compared: {compared}");
    assert!(
        installs > 10 && renewals > 0 && expired > 0,
        "registers alone that changed rows: {installs}, of which renewals {renewals}, past their deadline {expired}"
    );
    assert!(archived > 0, "a sync archived an expired origin");
}

/// Pool acquisition reads key states and clocks from the working state, and
/// its lease insert names neither. So before a lease is granted, the pool's
/// key rows and every applying provider's clocks are checked against SQL:
/// a state from before a key was walled runs again on a fresh load, which
/// refuses the walled key instead of leasing it.
#[test]
fn a_lease_decided_on_a_stale_state_runs_again_on_a_fresh_load() {
    use striatum_core::key_pool::PoolKey;
    let rig = rig("block-state-stale-lease");
    let keys = [PoolKey::new("k0", None, "active")];
    let acquire = |session: &str| {
        rig.store
            .pool_acquire(&crate::PoolAcquire {
                provider: "p1",
                session_id: session,
                keys: &keys,
                now_ms: NOW,
                lease_ms: 60_000,
                affinity_idle_ms: 60_000,
            })
            .unwrap()
    };
    let first = acquire("s0");
    assert!(
        matches!(first, crate::PoolAcquireOutcome::Leased(_)),
        "{first:?}"
    );
    let before_the_wall = rig.published();
    rig.store
        .pool_wall(
            "p1",
            "k0",
            crate::PoolWallKind::Billing,
            NOW + 600_000,
            "402",
            NOW,
        )
        .unwrap();
    let loads = rig.decide_next_on(before_the_wall);
    let second = acquire("s1");
    assert_eq!(rig.loads_since(loads), 2, "the stale grant is caught");
    assert!(
        matches!(second, crate::PoolAcquireOutcome::Exhausted(_)),
        "the walled key is refused on the fresh load: {second:?}"
    );
    rig.check(&[NOW], "after the stale lease");
}

/// A pool decision (a lease granted, reused, released, refused or
/// rehydrated) rests on rows none of its writes name: the key rows, the
/// clocks that apply to the route, the route clears that lift a clock for a
/// model, and sync's finding that no other source row needs a clock. Each
/// case lets SQL differ from a stale state in exactly one of those inputs;
/// the decision must notice it before acting (two loads) and answer as a
/// fresh load does.
#[test]
fn a_pool_decision_on_a_state_stale_in_any_one_input_runs_again_on_a_fresh_load() {
    use striatum_core::key_pool::PoolKey;
    type Step = fn(&Rig);
    type Act = fn(&Rig) -> String;
    fn keys() -> Vec<PoolKey> {
        vec![
            PoolKey::new("k0", None, "active"),
            PoolKey::new("k1", None, "active"),
        ]
    }
    fn acquire(
        rig: &Rig,
        session: &str,
        providers: &[&str],
        model: &str,
        keys: &[PoolKey],
    ) -> String {
        let outcome = rig
            .store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "p1",
                    session_id: session,
                    keys,
                    now_ms: NOW,
                    lease_ms: 60_000,
                    affinity_idle_ms: 60_000,
                },
                providers,
                model,
                &[],
                &std::collections::BTreeMap::new(),
            )
            .unwrap();
        match outcome {
            crate::PoolAcquireOutcome::Leased(lease) => {
                format!("leased {} reused={}", lease.credential_id, lease.reused)
            }
            crate::PoolAcquireOutcome::Exhausted(_) => "exhausted".into(),
        }
    }
    fn provider_wall(rig: &Rig, provider: &str) {
        rig.store
            .record_provider_cooldown(
                provider,
                &format!("{provider}/m"),
                &format!("{provider}/m"),
                &RouteStateUpdate {
                    observed_at_ms: Some(NOW - 1_000),
                    cooldown_until: Some((NOW + 3_600_000) as f64),
                    last_error: Some("provider capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        rig.store
            .account_transaction(|tx| fence_rechecks::sync_on(tx, NOW).map_err(err))
            .unwrap();
    }
    fn wall_both_keys(rig: &Rig) {
        for key in ["k0", "k1"] {
            rig.store
                .pool_wall(
                    "p1",
                    key,
                    crate::PoolWallKind::Billing,
                    NOW + 600_000,
                    "402",
                    NOW - 1_000,
                )
                .unwrap();
        }
    }
    let cases: Vec<(&str, Step, Step, Act, &str)> = vec![
        (
            "a route clear the state holds and SQL dropped",
            |rig| {
                provider_wall(rig, "p1");
                rig.sql("UPDATE block SET cleared_models=json_array('m') WHERE kind='provider' AND managed=1 AND is_origin=1;".into());
            },
            |rig| {
                rig.sql(
                    "UPDATE block SET cleared_models='[]' WHERE managed=1 AND is_origin=1;".into(),
                )
            },
            |rig| acquire(rig, "s1", &["p1"], "m", &keys()),
            "exhausted",
        ),
        (
            "a source wall in SQL that needs a clock the state never installed",
            |_| {},
            |rig| {
                drift_source(rig, |conn| {
                    conn.execute(
                        "INSERT INTO provider_cooldown_state(provider_key,wall_observed_at,last_error,last_error_at,updated_at)
                         VALUES ('p1',?1,'provider capacity',?1,?1)",
                        [NOW - 1_000],
                    )?;
                    crate::block_storage::wall_source(
                        conn,
                        crate::block_storage::WallSource {
                            kind: "provider",
                            key: "p1",
                            provider: "p1",
                            model: None,
                            cooldown: Some((NOW + 3_600_000) as f64),
                            exhausted: None,
                            wall_observed: Some(NOW - 1_000),
                            observed_at: NOW - 1_000,
                            error: Some("provider capacity"),
                            class: None,
                            unscoped: false,
                        },
                    )
                    .map(Option::unwrap)
                });
            },
            |rig| acquire(rig, "s1", &["p1"], "m", &keys()),
            "exhausted",
        ),
        (
            "only a key's last use",
            |rig| {
                acquire(
                    rig,
                    "s0",
                    &["p1"],
                    "",
                    &[PoolKey::new("k0", None, "active")],
                );
                acquire(
                    rig,
                    "s2",
                    &["p1"],
                    "",
                    &[PoolKey::new("k1", None, "active")],
                );
            },
            |rig| {
                rig.sql(format!(
                    "UPDATE pool_key_state SET last_used_at={} WHERE credential_id='k1';",
                    NOW + 1
                ))
            },
            |rig| acquire(rig, "s1", &["p1"], "", &keys()),
            "leased k0 reused=false",
        ),
        (
            "only a clock: SQL already made the walled keys' probe due",
            wall_both_keys,
            |rig| {
                rig.sql(format!(
                    "UPDATE block SET next_recheck_at={} WHERE provider='p1' AND managed=1 AND is_origin=1;",
                    NOW - 1
                ))
            },
            |rig| acquire(rig, "s1", &["p1"], "", &keys()),
            "leased k0 reused=false",
        ),
        (
            "a clock of another provider the route draws on",
            |_| {},
            |rig| provider_wall(rig, "p2"),
            |rig| acquire(rig, "s1", &["p1", "p2"], "m", &keys()),
            "exhausted",
        ),
        (
            "the reuse of a live lease on a key SQL has since walled",
            |rig| {
                acquire(
                    rig,
                    "s0",
                    &["p1"],
                    "",
                    &[PoolKey::new("k0", None, "active")],
                );
            },
            wall_both_keys,
            |rig| {
                acquire(
                    rig,
                    "s0",
                    &["p1"],
                    "",
                    &[PoolKey::new("k0", None, "active")],
                )
            },
            "exhausted",
        ),
        (
            "a release of a live lease whose key SQL has since unwalled",
            |rig| {
                acquire(
                    rig,
                    "s0",
                    &["p1"],
                    "",
                    &[PoolKey::new("k0", None, "active")],
                );
                wall_both_keys(rig);
            },
            |rig| rig.sql("DELETE FROM block WHERE kind IN ('pool_billing','pool_rate');".into()),
            |rig| {
                acquire(
                    rig,
                    "s0",
                    &["p1"],
                    "",
                    &[PoolKey::new("k0", None, "active")],
                )
            },
            "leased k0 reused=true",
        ),
        (
            "the rehydration of an unknown lease on a provider SQL has since walled",
            |_| {},
            |rig| provider_wall(rig, "p1"),
            |rig| {
                format!(
                    "{:?}",
                    rig.store
                        .pool_renew("p1", "s9", "lease-unknown", Some("k0"), NOW, 60_000)
                        .unwrap()
                )
            },
            "None",
        ),
        (
            "a refusal to rehydrate on a clock SQL has since cleared",
            |rig| provider_wall(rig, "p1"),
            |rig| {
                rig.sql("DELETE FROM provider_cooldown_state; DELETE FROM model_route_state; DELETE FROM block WHERE kind IN ('provider','route','failure');".into())
            },
            |rig| {
                format!(
                    "{:?}",
                    rig.store
                        .pool_renew("p1", "s9", "lease-unknown", Some("k0"), NOW, 60_000)
                        .unwrap()
                )
            },
            "Some(1800000060000)",
        ),
    ];
    // Case labels name the former source tables. Their refusal rows now live
    // in block; pool-success evidence lives in success_proof. SQL holds a row
    // the stale state lacks, and a separately loaded store supplies the answer.
    fn drift_source(
        rig: &Rig,
        make: impl Fn(&Connection) -> rusqlite::Result<crate::block_storage::StoredBlock>
            + Send
            + 'static,
    ) {
        rig.run(move |conn| crate::block_storage::write_source(conn, &make(conn)?).map(|_| ()));
    }
    fn account_fixture(
        pool_key: Option<striatum_core::accounts::PoolKey>,
        recovery_set: Vec<striatum_core::accounts::PoolKey>,
    ) -> crate::AccountPoolFenceRow {
        crate::AccountPoolFenceRow {
            evidence: crate::AccountFenceRow {
                fence_id: "drift-fence".into(),
                provider: "p1".into(),
                account_id: "a0".into(),
                scope_json: "{}".into(),
                fence_json: serde_json::json!({"reset": {"at_ms": NOW + 600_000}}).to_string(),
                source: "broca_account_limit".into(),
                observed_at: NOW - 1_000,
                principal: "x".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW - 1_000,
                cleared_at: None,
                cleared_reason: None,
            },
            pool_key,
            recovery_set,
        }
    }
    let any = |rig: &Rig| acquire(rig, "s1", &["p1"], "m", &keys());
    let table_cases: Vec<(&str, Step, Step)> = vec![
        (
            "account_fence",
            |_| {},
            |rig| {
                drift_source(rig, |conn| {
                    crate::block_storage::account_source(conn, &account_fixture(None, Vec::new()))
                });
            },
        ),
        (
            "credential_entitlement_fences",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    crate::block_storage::credential_source(
                        &striatum_core::route_state_store::CredentialEntitlementFence {
                            provider_id: "p1".into(),
                            credential_id: "k0".into(),
                            code: "billing_exhausted".into(),
                            listing_fingerprint: Some("l0".into()),
                            observed_at_ms: NOW - 1_000,
                            console_credit_micros_at_refusal: None,
                        },
                    )
                });
            },
        ),
        (
            "account_route_fence",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    crate::block_storage::account_route_source(
                        &crate::AccountRouteFence {
                            provider: "p1".into(),
                            credential_id: "k0".into(),
                            model: "m".into(),
                            until_ms: NOW + 600_000,
                            decision_id: "d".into(),
                            recorded_at: NOW - 1_000,
                        },
                        "account_route",
                        "quota",
                        Some(NOW - 1_000),
                    )
                });
            },
        ),
        (
            "account_non_quota_route_fence",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    crate::block_storage::account_route_source(
                        &crate::AccountRouteFence {
                            provider: "p1".into(),
                            credential_id: "k0".into(),
                            model: "m".into(),
                            until_ms: NOW + 600_000,
                            decision_id: "d".into(),
                            recorded_at: NOW - 1_000,
                        },
                        "non_quota_route",
                        "transient",
                        Some(NOW - 1_000),
                    )
                });
            },
        ),
        (
            "model_route_state",
            |_| {},
            |rig| {
                drift_source(rig, |conn| {
                    conn.execute(
                        "INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,wall_observed_at,last_error,last_error_at,updated_at)
                         VALUES ('p1/m','p1/m','p1','m',?1,'route capacity',?1,?1)",
                        [NOW - 1_000],
                    )?;
                    crate::block_storage::wall_source(
                        conn,
                        crate::block_storage::WallSource {
                            kind: "route",
                            key: "p1/m",
                            provider: "p1",
                            model: Some("m"),
                            cooldown: Some((NOW + 600_000) as f64),
                            exhausted: None,
                            wall_observed: Some(NOW - 1_000),
                            observed_at: NOW - 1_000,
                            error: Some("route capacity"),
                            class: None,
                            unscoped: false,
                        },
                    )
                    .map(Option::unwrap)
                });
            },
        ),
        (
            "account_size_refusal",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    crate::block_storage::size_source(
                        &crate::AccountSizeRefusal {
                            provider: "p1".into(),
                            credential_id: "k0".into(),
                            model: "m".into(),
                            size_class: 16,
                            expires_at: NOW + 600_000,
                            observed_at: NOW - 1_000,
                        },
                        true,
                    )
                });
            },
        ),
        (
            "account_non_quota_size_refusal",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    crate::block_storage::size_source(
                        &crate::AccountSizeRefusal {
                            provider: "p1".into(),
                            credential_id: "k0".into(),
                            model: "m".into(),
                            size_class: 16,
                            expires_at: NOW + 600_000,
                            observed_at: NOW - 1_000,
                        },
                        false,
                    )
                });
            },
        ),
        (
            "account_transient_backoff",
            |_| {},
            |rig| {
                drift_source(rig, |_| {
                    let mut row = crate::block_storage::StoredBlock::from_origin(
                        crate::fence_rechecks::ViewOrigin {
                            kind: "observation".into(),
                            origin_key: serde_json::json!(["p1", "a0"]).to_string(),
                            provider: "p1".into(),
                            account_id: Some("a0".into()),
                            evidence: serde_json::json!(["k", "[]"]).to_string(),
                            evidence_at: NOW - 1_000,
                            quota: Some(false),
                            deadline: Some((NOW + 600_000) as f64),
                            ..Default::default()
                        },
                    )?;
                    row.deadline = Some(crate::block_storage::Deadline::Integer(NOW + 600_000));
                    Ok(row)
                });
            },
        ),
        (
            "fence_success_pool",
            |rig| {
                drift_source(rig, |conn| {
                    crate::block_storage::account_source(
                        conn,
                        &account_fixture(
                            Some(serde_json::from_str(&pool_key(1)).unwrap()),
                            vec![
                                serde_json::from_str(&pool_key(1)).unwrap(),
                                serde_json::from_str(&pool_key(2)).unwrap(),
                            ],
                        ),
                    )
                });
            },
            |rig| {
                rig.sql(format!(
                    "INSERT INTO success_proof VALUES ('drift-fence','{}',{},{});",
                    pool_key(2),
                    NOW - 1_000,
                    NOW,
                ));
            },
        ),
    ];
    let cases: Vec<(&str, Step, Step, Act, &str)> = cases
        .into_iter()
        .chain(
            table_cases
                .into_iter()
                .map(|(name, setup, drift)| (name, setup, drift, any as Act, "")),
        )
        .collect();
    let _ = any;
    for (name, setup, drift, act, expected) in cases {
        // The same steps on a state that is never stale give the answer.
        let twin = rig("block-state-fresh-pool-input");
        setup(&twin);
        drift(&twin);
        let fresh = act(&twin);
        if !expected.is_empty() {
            assert_eq!(fresh, expected, "{name}: the fresh answer");
        }
        let rig = rig("block-state-stale-pool-input");
        setup(&rig);
        let stale = rig.published();
        drift(&rig);
        let loads = rig.decide_next_on(stale);
        let answer = act(&rig);
        assert_eq!(
            rig.loads_since(loads),
            2,
            "{name}: the stale decision is caught"
        );
        assert_eq!(answer, fresh, "{name}: answered as a fresh load does");
        rig.check(&[NOW], name);
    }
}

/// A change made in place where the capture did not see it (a key's wall or
/// last use, a clock's evidence) keeps every table's row count and highest
/// rowid, so only the full comparison finds it. That comparison runs within
/// six ticks of the store that missed the change, even while another
/// store's ticks interleave with them in the same process.
#[test]
fn a_missed_change_in_place_is_found_within_one_full_comparison_interval_per_store() {
    let changes: [(&str, String); 3] = [
        (
            "a key's wall",
            format!(
                "UPDATE block SET deadline={} WHERE kind='pool_billing' AND credential_id='k0';",
                NOW + 900_000
            ),
        ),
        (
            "a key's last use",
            format!(
                "UPDATE pool_key_state SET last_used_at={} WHERE credential_id='k0';",
                NOW + 5
            ),
        ),
        (
            "a clock's evidence",
            "UPDATE block SET evidence=evidence||' ' WHERE provider='p1' AND is_origin=1 AND managed=1;".into(),
        ),
    ];
    for (name, change) in changes {
        let missed = rig("block-state-missed-in-place");
        let other = rig("block-state-other-store");
        for rig in [&missed, &other] {
            rig.store
                .pool_wall(
                    "p1",
                    "k0",
                    crate::PoolWallKind::Billing,
                    NOW + 600_000,
                    "402",
                    NOW - 1_000,
                )
                .unwrap();
            rig.store
                .account_command(AccountCommandKind::Maintenance, |tx| {
                    tx.sync_fence_rechecks()
                })
                .unwrap();
        }
        UNSEEN.with(|unseen| unseen.set(true));
        missed
            .store
            .bypass_account_writer_for_test(&change)
            .unwrap();
        UNSEEN.with(|unseen| unseen.set(false));
        assert_ne!(
            missed.published(),
            missed.fresh(),
            "{name}: the state misses the change"
        );
        let tick = |rig: &Rig| {
            rig.store
                .account_command(AccountCommandKind::Tick, |tx| tx.tick(NOW))
                .unwrap()
        };
        let reloads = block_reconcile_reloads();
        // With one count shared by both stores, these alternating ticks
        // would hand every full comparison to the other store.
        for _ in 0..FULL_RECONCILE_EVERY {
            tick(&missed);
            tick(&other);
        }
        assert_eq!(
            block_reconcile_reloads() - reloads,
            1,
            "{name}: found within six of its ticks"
        );
        missed.check(&[NOW], name);
        other.check(&[NOW], name);
    }
}

/// Every read exempt from the no-SQL-reads rule (a decision's check of the
/// state, a route-state writer's read of the row it writes) is keyed:
/// SQLite's plan for it searches the watched tables by an index or primary
/// key and never scans one.
#[test]
fn every_native_exempt_read_is_keyed() {
    let rig = rig("block-state-exempt-reads-keyed");
    let scans = rig
        .store
        .with_reader_for_test(|conn| {
            let mut scans = Vec::new();
            for sql in reviewed_exempt_reads() {
                let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
                let unbound = vec![rusqlite::types::Value::Null; plan.parameter_count()];
                let details: Vec<String> = plan
                    .query_map(rusqlite::params_from_iter(unbound), |r| {
                        r.get::<_, String>(3)
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                let scanned: Vec<&String> = details
                    .iter()
                    .filter(|detail| {
                        detail.starts_with("SCAN ")
                            && !detail.starts_with("SCAN json_each")
                            && !detail.starts_with("SCAN CONSTANT ROW")
                    })
                    .collect();
                if !scanned.is_empty() {
                    scans.push(format!("{sql}\n  plan: {details:?}"));
                }
            }
            Ok(scans)
        })
        .unwrap();
    assert!(
        scans.is_empty(),
        "exempt reads that scan:\n{}",
        scans.join("\n")
    );
    assert_eq!(reviewed_exempt_reads().len(), 23);
}

/// A new clock's schedule computed on the state equals SQLite's, up to the
/// ends of the integer range: an overflowing sum becomes a real there, a
/// real compares with an integer by exact value, and the cast saturates.
#[test]
fn a_clock_schedule_equals_sqlite_at_numeric_boundaries() {
    let rig = rig("block-state-clock-schedule-bounds");
    let evidence = [
        0,
        1_800_000_000_000,
        (1 << 53) + 1,
        i64::MAX,
        i64::MAX - 1,
        i64::MAX - 900_000,
        i64::MAX - 900_000 + 1,
        i64::MAX - 3_600_000,
        i64::MAX - 3_600_000 + 1,
        i64::MIN,
        i64::MIN + 1,
    ];
    let reals = [
        None,
        Some(0.0),
        Some(-1.5),
        Some(1e30),
        Some(-1e30),
        Some(9_223_372_036_854_775_807.0),
        Some(((1_i64 << 53) + 2) as f64),
        Some(1_800_000_000_000.5),
    ];
    let mut cases = Vec::new();
    for evidence_at in evidence {
        for deadline in reals {
            for reset_at in reals {
                for quota in [false, true] {
                    cases.push((quota, evidence_at, deadline, reset_at));
                }
            }
        }
    }
    let sql: Vec<(i64, Option<f64>)> = rig
        .store
        .with_reader_for_test(move |conn| {
            let mut statement = conn.prepare(
                "SELECT CAST(CASE WHEN ?1=1 THEN ?2+900000 ELSE MIN(COALESCE(?3,?2+3600000),?2+3600000) END AS INTEGER),
                        CASE WHEN ?1=1 AND ?4>?2 THEN ?4 END",
            )?;
            cases
                .iter()
                .map(|(quota, evidence_at, deadline, reset_at)| {
                    statement.query_row(
                        rusqlite::params![quota, evidence_at, deadline, reset_at],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                })
                .collect()
        })
        .unwrap();
    let mut index = 0;
    for evidence_at in evidence {
        for deadline in reals {
            for reset_at in reals {
                for quota in [false, true] {
                    assert_eq!(
                        fence_rechecks::clock_schedule(quota, evidence_at, deadline, reset_at),
                        sql[index],
                        "quota={quota} evidence_at={evidence_at} deadline={deadline:?} reset_at={reset_at:?}"
                    );
                    index += 1;
                }
            }
        }
    }
}

/// The four route-state writers read the row each is about to write, keyed,
/// and only through the marked statements on the reviewed list: driven
/// through their real entry points, they make no other block read.
#[test]
fn the_route_state_writers_read_only_their_own_rows_through_reviewed_statements() {
    use striatum_core::{DecisionInput, DecisionOutcomeUpdate, FailureBackoffConfig, RouteOutcome};
    let rig = rig("block-state-route-writer-reads");
    let normalize = |sql: &str| sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let reviewed: Vec<String> = reviewed_exempt_reads()
        .iter()
        .map(|sql| normalize(sql))
        .collect();
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    let mut seen_writers = Vec::new();
    let mut ran: std::collections::BTreeSet<String> = Default::default();
    // Each writer's marked reads, and the key each bound: the rows it read.
    let mut expect = |writer: &'static str, statements: usize, keys: &[&str]| {
        let seen = trace.take();
        assert!(
            seen.reader.is_empty() && seen.writer.is_empty(),
            "{writer}: {seen:#?}"
        );
        assert_eq!(
            seen.route_writes.len(),
            statements,
            "{writer}: {:#?}",
            seen.route_writes
        );
        for sql in &seen.route_writes {
            assert!(
                reviewed.contains(&normalize(sql)),
                "{writer}: unreviewed {sql}"
            );
            ran.insert(normalize(sql));
        }
        for row in &seen.route_write_rows {
            assert!(
                keys.iter().any(|key| row.contains(&format!("'{key}'"))),
                "{writer} read a row other than {keys:?}: {row}"
            );
        }
        seen_writers.push(writer);
    };
    rig.store
        .record_provider_cooldown(
            "p0",
            "p0/m0",
            "p0/m0",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW - 1_000),
                cooldown_until: Some((NOW + 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    expect("record_provider_cooldown", 3, &["p0"]);
    rig.store
        .update_route_state(
            "p1/m1",
            "p1/m1",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW - 1_000),
                cooldown_until: Some((NOW + 600_000) as f64),
                last_error: Some("route capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    expect("update_route_state", 3, &["p1/m1", "p1"]);
    rig.store
        .record_decision(&DecisionInput {
            decision_id: Some("route-writer-decision".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            selected_route_key: Some("p1/m1".into()),
            selected_provider_id: Some("p1".into()),
            selected_model_id: Some("m1".into()),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    trace.take();
    rig.store
        .set_decision_outcome(
            "route-writer-decision",
            &DecisionOutcomeUpdate {
                provider_outcome: None,
                outcome: RouteOutcome::Error,
                last_error: Some("HTTP 503 provider unavailable".into()),
                observed_at_ms: NOW,
                failure_backoff: FailureBackoffConfig::default(),
            },
        )
        .unwrap();
    expect("set_decision_outcome", 1, &["p1/m1"]);
    rig.store.clear_failure_backoff("p1/m1").unwrap();
    expect("clear_failure_backoff", 1, &["p1/m1"]);
    assert_eq!(seen_writers.len(), 4);
    // Every one of the eight marked statements ran.
    let marked: std::collections::BTreeSet<String> = reviewed_exempt_reads()
        .iter()
        .filter(|sql| sql.contains(crate::ROUTE_WRITE_READ))
        .map(|sql| normalize(sql))
        .collect();
    assert_eq!(
        reviewed_exempt_reads()
            .iter()
            .filter(|sql| sql.contains(crate::ROUTE_WRITE_READ))
            .count(),
        8
    );
    assert_eq!(ran, marked);
}

/// An admission rules credentials out by its principal's exclusions, read
/// from the state. A state missing an exclusion SQL holds runs the check
/// again on a fresh load.
#[test]
fn an_exclusion_check_on_a_stale_state_runs_again_on_a_fresh_load() {
    let rig = rig("block-state-stale-exclusion");
    let stale = rig.published();
    rig.run(|conn| {
        let row = crate::block_storage::principal_source(
            conn,
            &crate::AccountExclusionRow {
                principal: "principal".into(),
                admission_id: "admission".into(),
                credential_id: "credential".into(),
                runner_view_digest: "runner".into(),
                roster_view_digest: "roster".into(),
                observation_id: None,
                recorded_at: 1,
            },
        )?;
        crate::block_storage::write_source(conn, &row).map(|_| ())
    });
    let loads = rig.decide_next_on(stale);
    rig.store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.check_admission_exclusions("principal", "admission")
        })
        .unwrap();
    assert_eq!(rig.loads_since(loads), 2, "the stale exclusions are caught");
    rig.check(&[NOW], "after the stale exclusion check");
}

/// A success heals the block its test request authorized, once: the report
/// is recorded as delivered and the authorization deleted whatever the heal
/// finds. A state from before the grant would find no test and heal
/// nothing, so each heal checks the owner's authorizations and their clocks
/// against SQL first and runs again on a fresh load, which heals.
#[test]
fn a_heal_that_finds_no_test_on_a_stale_state_runs_again_and_heals() {
    for path in ["authorized probe", "report probe"] {
        let rig = rig("block-state-stale-heal");
        due_credential_refusal(&rig, "c0");
        let origins = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
        let before_the_grant = rig.published();
        let granted = origins.clone();
        assert!(rig
            .store
            .account_command(AccountCommandKind::Maintenance, move |tx| {
                tx.grant_fence_rechecks(&granted, NOW, Some("decision"))
            })
            .unwrap());
        let loads = rig.decide_next_on(before_the_grant);
        let path_name = path;
        rig.store
            .account_command(AccountCommandKind::Report, move |tx| match path_name {
                "authorized probe" => tx.heal_authorized_probe("decision", "p0", "m0", NOW + 10),
                _ => {
                    let origins = tx.fence_rechecks()?;
                    tx.heal_report_probe(
                        "decision",
                        None,
                        false,
                        "m0",
                        NOW + 5,
                        &origins,
                        None,
                        NOW + 10,
                    )
                }
            })
            .unwrap();
        assert_eq!(
            rig.loads_since(loads),
            2,
            "{path}: the stale run, then a fresh one"
        );
        let left = rig.store.account_read(|tx| tx.fence_rechecks()).unwrap();
        assert!(
            left.iter()
                .all(|clock| clock.credential_id.as_deref() != Some("c0")),
            "{path}: the test request's success healed its block: {left:?}"
        );
        rig.check(&[NOW], path);
    }
}

/// A pool key's recovered probes are named with the evidence they recovered
/// from. A recovery read before the block was renewed does not lift the
/// renewed block.
#[test]
fn a_recovery_from_older_evidence_does_not_lift_a_renewed_pool_block() {
    use striatum_core::key_pool::PoolKey;
    let rig = rig("block-state-recovered-evidence");
    let provider_wall = |observed: i64| {
        rig.store
            .record_provider_cooldown(
                "p1",
                "p1/m",
                "p1/m",
                &RouteStateUpdate {
                    observed_at_ms: Some(observed),
                    cooldown_until: Some((NOW + 3_600_000) as f64),
                    last_error: Some("provider capacity".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        rig.run(fence_rechecks::sync);
    };
    provider_wall(NOW - 2_000);
    let old: Vec<crate::RecoveredOrigin> = rig
        .store
        .account_read(|tx| tx.fence_rechecks())
        .unwrap()
        .into_iter()
        .map(|origin| (origin.kind, origin.origin_key, origin.evidence))
        .collect();
    assert!(!old.is_empty());
    // The block is renewed with new evidence after the recovery was read.
    provider_wall(NOW - 1_000);
    let keys = [PoolKey::new("k0", None, "active")];
    let acquire = |recovered: &[crate::RecoveredOrigin]| {
        rig.store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "p1",
                    session_id: "s",
                    keys: &keys,
                    now_ms: NOW,
                    lease_ms: 60_000,
                    affinity_idle_ms: 60_000,
                },
                &["p1"],
                "m",
                recovered,
                &std::collections::BTreeMap::new(),
            )
            .unwrap()
    };
    assert!(
        matches!(acquire(&old), crate::PoolAcquireOutcome::Exhausted(_)),
        "a recovery from the older evidence leaves the renewed block"
    );
    let current: Vec<crate::RecoveredOrigin> = rig
        .store
        .account_read(|tx| tx.fence_rechecks())
        .unwrap()
        .into_iter()
        .map(|origin| (origin.kind, origin.origin_key, origin.evidence))
        .collect();
    assert!(
        matches!(acquire(&current), crate::PoolAcquireOutcome::Leased(_)),
        "a recovery from the current evidence lifts it"
    );
}

#[test]
fn native_sql_projection_is_an_independent_reference() {
    let rig = rig("native-source-sql-reference");
    rig.store
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "p0".into(),
                credential_id: "c0".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            None,
            NOW,
        )
        .unwrap();
    rig.store
        .account_transaction(|tx| {
            tx.insert_pool_fence(&crate::AccountPoolFenceRow {
                evidence: crate::AccountFenceRow {
                    fence_id: "credential-quota:[\"p0\",\"c0\"]:quota-report".into(),
                    provider: "p0".into(),
                    account_id: "a0".into(),
                    scope_json: "null".into(),
                    fence_json: "{\"reset\":{\"kind\":\"missing\"}}".into(),
                    source: "broca_account_limit".into(),
                    observed_at: NOW,
                    principal: "test".into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: NOW,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set: Vec::new(),
            })
        })
        .unwrap();
    rig.store
        .record_provider_cooldown(
            "p0",
            "p0/m0",
            "p0/m0",
            &RouteStateUpdate {
                observed_at_ms: Some(NOW),
                cooldown_until: Some((NOW + 60_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    rig.sql(format!(
        "UPDATE block SET observed_at={} WHERE kind='provider';
         UPDATE block SET observed_at={},created_at={} WHERE kind='account';",
        NOW + 1,
        NOW + 2,
        NOW + 3,
    ));
    rig.check(
        &[NOW - 1, NOW, NOW + 60_001],
        "independent native SQL reference",
    );
    let origins = rig.store.with_reader_for_test(native_sql::origins).unwrap();
    let provider = origins.iter().find(|row| row.kind == "provider").unwrap();
    assert_eq!(provider.evidence_at, NOW + 1);
    assert_eq!(provider.deadline, Some((NOW + 60_000) as f64));
    let account = origins.iter().find(|row| row.kind == "account").unwrap();
    assert_eq!(account.evidence_at, NOW + 2);
    let credential = origins.iter().find(|row| row.kind == "credential").unwrap();
    assert_eq!(credential.quota, Some(true));
}

#[test]
fn route_source_order_keeps_existing_metadata_order_through_reverse_reports_and_reload() {
    let rig = rig("route-source-metadata-order");
    let now = 1_900_000_000_000i64;
    for model in ["first", "second"] {
        let fact = serde_json::from_value(serde_json::json!({
            "providerID":"source-order-provider", "modelID":model,
            "scores":{"elo":1000,"eq":50,"speed":50}, "confidence":"unknown"
        }))
        .unwrap();
        rig.store.upsert_fact(&fact, FactSource::Override).unwrap();
    }
    // Catalogue order exists before either route reports a refusal. Receiving
    // the reports in reverse order must not replace that established order.
    for model in ["second", "first"] {
        let key = format!("source-order-provider/{model}");
        rig.store
            .update_route_state(
                &key,
                "source-order-provider/shared",
                &RouteStateUpdate {
                    cooldown_until: Some((now + 60_000) as f64),
                    observed_at_ms: Some(now),
                    last_error: Some(format!("independent route capacity: {model}")),
                    unscoped_wall: true,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let check_order = || {
        let rows = rig
            .store
            .with_reader_for_test(|conn| {
                conn.prepare(
                    "SELECT m.route_key,m.rowid,b.source_order FROM model_route_state m
                JOIN block b ON b.kind='route' AND b.origin_key=json_array(m.route_key)
                WHERE m.provider_id='source-order-provider' ORDER BY m.rowid",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "source-order-provider/first");
        assert_eq!(rows[1].0, "source-order-provider/second");
        for (key, metadata_order, source_order) in rows {
            assert_eq!(
                source_order, metadata_order,
                "source keeps existing route metadata order: {key}"
            );
        }
        rig.check(
            &[now, now + 60_001],
            "reverse route reports retain metadata order",
        );
    };
    check_order();
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.owner.request_reload();
    rig.store
        .account_command(AccountCommandKind::Tick, move |tx| tx.tick(now))
        .unwrap();
    assert_eq!(rig.owner.loads.load(Ordering::Relaxed), loads + 1);
    check_order();
}

#[test]
fn route_source_order_bookkeeping_uses_a_unique_keyed_lookup() {
    let rig = rig("route-order-keyed-plan");
    let (plan, unique_indexes) = rig
        .store
        .with_reader_for_test(|conn| {
            let mut statement = conn.prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                crate::block_storage::ROUTE_SOURCE_ORDER
            ))?;
            let parameters = if statement.parameter_count() == 0 {
                Vec::new()
            } else {
                vec![Value::Text(
                    serde_json::json!(["plan-provider/model"]).to_string(),
                )]
            };
            let plan = statement
                .query_map(rusqlite::params_from_iter(parameters), |row| {
                    row.get::<_, String>(3)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let indexes = conn
                .prepare(
                    "SELECT name FROM pragma_index_list('model_route_state') WHERE \"unique\"=1",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((plan, indexes))
        })
        .unwrap();
    assert!(
        plan.iter().all(|detail| !detail.contains("SCAN")),
        "route order must not scan metadata: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("SEARCH model_route_state")
                && detail.contains("(route_key=?)")
                && unique_indexes.iter().any(|name| detail.contains(name))),
        "route order requires the unique route_key lookup: {plan:?}"
    );
}

#[test]
fn route_source_order_bookkeeping_does_not_exempt_unkeyed_or_request_reads() {
    let rig = rig("route-order-exact-trace");
    let sql = crate::block_storage::ROUTE_SOURCE_ORDER;
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    rig.store
        .with_reader_for_test(|conn| {
            conn.query_row(
                sql,
                [serde_json::json!(["trace-provider/model"]).to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
        })
        .unwrap();
    let request = trace.take();
    assert_eq!(request.reader, vec![sql.to_string()]);
    assert!(request.writer_bookkeeping.is_empty());

    let near_miss = sql.split_once(" WHERE ").unwrap().0.to_string();
    let expected = near_miss.clone();
    rig.store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.pool_sql(&|conn: &rusqlite::Connection| {
                conn.prepare(&near_miss)?
                    .query_map([], |row| row.get::<_, i64>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(())
            })
        })
        .unwrap();
    let unkeyed = trace.take();
    assert!(
        unkeyed.writer.iter().any(|read| read == &expected),
        "unkeyed read must stay visible: {unkeyed:#?}"
    );
    assert!(unkeyed.writer_bookkeeping.is_empty());

    rig.store
        .update_route_state(
            "trace-provider/model",
            "trace-provider/model",
            &RouteStateUpdate {
                cooldown_until: Some((NOW + 60_000) as f64),
                observed_at_ms: Some(NOW),
                last_error: Some("route capacity".into()),
                unscoped_wall: true,
                ..Default::default()
            },
        )
        .unwrap();
    let writer = trace.take();
    assert!(
        writer.writer_bookkeeping.iter().any(|read| read == sql),
        "real route-source writer must record its exact lookup: {writer:#?}"
    );
}

#[path = "carbon/tests.rs"]
mod carbon;
