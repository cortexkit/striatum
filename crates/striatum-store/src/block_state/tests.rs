//! The block state must equal a fresh load after every command, blocks
//! derived from it must equal the SQL adapter's at every `now`, and every
//! request reader's answer from memory must equal its SQL answer.

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
    // (Every account route fence is held for its raw reader, so a pool key
    // row, held only while it walls, stands in.)
    rig.sql(format!(
        "INSERT INTO pool_key_state(provider,credential_id,walled_until,wall_reason,updated_at) VALUES ('p','c',{},'billing',{NOW});",
        NOW + 60_000
    ));
    assert_eq!(rig.published().table_sizes()[4], ("pool_key_state", 1));
    rig.sql(format!(
        "INSERT OR REPLACE INTO pool_key_state(provider,credential_id,updated_at) VALUES ('p','c',{NOW});"
    ));
    assert_eq!(rig.published().table_sizes()[4], ("pool_key_state", 0));
    rig.check(&[NOW], "pool key replaced by an unselected row");
    // A route fence outside the quota class is held, though not in the view.
    rig.sql(format!(
        "INSERT INTO account_route_fence VALUES ('p','c','m',{},'d',{NOW},{NOW},'quota');",
        NOW + 60_000
    ));
    rig.sql(format!(
        "INSERT OR REPLACE INTO account_route_fence VALUES ('p','c','m',{},'d',{NOW},{NOW},'transient');",
        NOW + 60_000
    ));
    assert_eq!(rig.published().table_sizes()[2], ("account_route_fence", 1));
    rig.check(&[NOW], "route fence replaced by a transient row");
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
            } else if matches!(path.extension().and_then(|e| e.to_str()), Some("rs" | "sql")) {
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
        let Ok(text) = std::fs::read_to_string(path) else { continue };
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        for pair in lines.windows(2) {
            let declared = ["mod ", "pub mod ", "pub(crate) mod ", "pub(super) mod "]
                .iter()
                .find_map(|prefix| pair[1].strip_prefix(prefix))
                .and_then(|rest| rest.strip_suffix(';'));
            if let (true, Some(name)) = (pair[0] == "#[cfg(test)]", declared) {
                let dir = path.parent().unwrap();
                let base = if path.file_stem().is_some_and(|stem| stem == "lib" || stem == "main" || stem == "mod") {
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
                let end = [after.find('\n'), after.find("\\n")].into_iter().flatten().min();
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
    assert_eq!(scan("x(\"INSERT  OR\n   Replace INTO `fence_recheck`(kind) VALUES (1)\")"), ["or replace", "replace into"]);
    assert_eq!(scan("replace into anything values (1)"), ["replace into"]);
    assert_eq!(scan("UPDATE OR REPLACE pool_key_state SET a=1"), ["or replace"]);
    assert_eq!(scan("CREATE TABLE t (a TEXT UNIQUE ON CONFLICT REPLACE);"), ["on conflict replace"]);
    assert!(scan("let s = text.replace('a', \"b\"); color_replace").is_empty());
    assert_eq!(scan("INSERT OR/**/REPLACE t VALUES (1)"), ["or replace"]);
    assert_eq!(scan("INSERT OR -- keep the newest\n  REPLACE t VALUES (1)"), ["or replace"]);
    assert_eq!(scan("x(\"INSERT OR \\\n         REPLACE t VALUES (1)\")"), ["or replace"]);
    assert_eq!(scan("x(\"-- the newest\\nINSERT OR REPLACE t VALUES (1)\"); // --"), ["or replace"]);
    assert_eq!(scan("-- INSERT OR REPLACE, even commented out\nSELECT 1"), ["or replace"]);
    let mut violations = Vec::new();
    let mut scanned = 0;
    for path in all.iter().filter(|path| !is_test(path)) {
        if path.file_name().is_some_and(|name| SUPERSEDED.iter().any(|superseded| name == *superseded)) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else { continue };
        scanned += 1;
        violations.extend(scan(&text).into_iter().map(|found| format!("{}: {found}", path.display())));
    }
    assert!(scanned > 100, "the scan found the sources: {scanned} files");
    assert!(violations.is_empty(), "REPLACE conflict resolution: {violations:#?}");

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
    assert!(schema.len() > 50, "the schema was read: {} objects", schema.len());
    let replacing: Vec<_> = schema.iter().filter(|(_, sql)| !scan(sql).is_empty()).map(|(name, _)| name).collect();
    assert!(replacing.is_empty(), "schema objects resolving a conflict by REPLACE: {replacing:?}");
    assert_eq!(own_triggers, 0, "no trigger on the copies' target tables or on the archive besides retirement");
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
        conn.execute_batch(&statement(old_054, "CREATE TABLE IF NOT EXISTS model_route_parent_failures", ");")).unwrap();
        conn.execute_batch(&statement(old_056, "CREATE TABLE IF NOT EXISTS account_observation_event", ");")).unwrap();
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
                    (0..columns).map(|at| row.get::<_, Value>(at).unwrap()).collect::<Vec<_>>()
                ));
            }
        }
        rows
    };
    let replaced = run([
        statement(old_054, "CREATE TRIGGER IF NOT EXISTS model_route_parent_failures_on_insert", "END;"),
        statement(old_056, "CREATE TRIGGER IF NOT EXISTS account_observation_event_on_insert", "END;"),
    ]);
    let updated = run([
        statement(new, "CREATE TRIGGER model_route_parent_failures_on_insert", "END;"),
        statement(new, "CREATE TRIGGER account_observation_event_on_insert", "END;"),
    ]);
    assert_eq!(updated, replaced);
    assert_eq!(replaced.len(), 4, "{replaced:#?}");
    assert!(replaced.iter().any(|row| row.contains("\"s2\"")), "{replaced:#?}");
    assert!(replaced.iter().any(|row| row.contains("\"o2\"")), "{replaced:#?}");
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
    rig.run(fence_rechecks::sync);
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
    rig.owner.stale.lock().unwrap().push(before_the_winner.clone());
    rig.owner.request_reload();
    assert!(!claim("loser").unwrap(), "the rerun sees the winner's claim");
    assert_eq!(rig.owner.loads.load(Ordering::Relaxed) - loads, 2);
    let owners = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.prepare("SELECT claim_owner FROM fence_recheck UNION ALL SELECT owner FROM fence_probe_authorization")?
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
    rig.sql(format!(
        "INSERT INTO account_route_fence VALUES ('p0','c0','m0',{until},'d',{older},{older},'quota');"
    ));
    let before_the_refusal = rig.published();
    rig.sql(format!(
        "UPDATE account_route_fence SET last_reported_at={newer} WHERE credential_id='c0';"
    ));
    let heal = || {
        rig.store
            .account_command(AccountCommandKind::Report, |tx| {
                tx.heal_serving_fences_in_scope("p0", "a0", "c0", "m0", None, NOW, NOW + 1, None)
            })
    };
    let reported = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.prepare("SELECT last_reported_at FROM account_route_fence")?
                    .query_map([], |r| r.get::<_, i64>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
    };
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.owner.stale.lock().unwrap().push(before_the_refusal.clone());
    rig.owner.request_reload();
    heal().unwrap();
    assert_eq!(rig.owner.loads.load(Ordering::Relaxed) - loads, 2, "the stale run, then a fresh one");
    assert_eq!(reported(&rig), [newer], "the refusal after the admission stands");
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
    let fence = |id: &str, account: &str, pool: &str| {
        format!("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
            VALUES ('{id}','p0','{account}','{{}}','{{}}','broca_account_limit',{older},'x',{older},'{pool}','[]');")
    };
    rig.sql(format!(
        "INSERT INTO account_route_fence VALUES ('p0','c0','m0',{until},'d',{older},{older},'quota');
         INSERT INTO account_route_fence VALUES ('p0','c1','m0',{until},'d',{older},{older},'quota');
         INSERT INTO account_route_fence VALUES ('p0','c0','m1',{until},'d',{older},{older},'quota');
         INSERT INTO credential_entitlement_fences VALUES ('p0','c0','billing_exhausted',NULL,{newer});
         INSERT INTO credential_entitlement_fences VALUES ('p0','c1','billing_exhausted',NULL,{older});
         {}{}{}",
        fence("shared", "a0", &shared),
        fence("named", "a0", &named),
        fence("other-account", "a1", &shared),
    ));
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
                list("SELECT credential_id||'/'||model FROM account_route_fence ORDER BY 1")?,
                list("SELECT credential_id FROM credential_entitlement_fences ORDER BY 1")?,
                list("SELECT fence_id FROM account_fence WHERE cleared_at IS NULL ORDER BY 1")?,
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
    rig.sql(format!(
        "INSERT INTO credential_entitlement_fences VALUES ('p0','{credential}','billing_exhausted',NULL,{});",
        NOW - 2 * 3_600_000
    ));
    rig.run(fence_rechecks::sync);
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
    assert!(!settles, "a preserved report never settles the decision's probe");
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
        .bypass_account_writer_for_test("UPDATE fence_recheck SET claim_owner='another';")
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
    rig.sql("UPDATE fence_recheck SET claim_generation=claim_generation+1 WHERE kind='credential';".into());
    let superseded = rig.published();
    rig.sql("UPDATE fence_recheck SET claim_generation=claim_generation-1 WHERE kind='credential';".into());
    let generation = |rig: &Rig| {
        rig.store
            .with_reader_for_test(|conn| {
                conn.query_row(
                    "SELECT claim_generation,claim_owner FROM fence_recheck WHERE kind='credential'",
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
        "UPDATE fence_recheck SET claim_owner='selection' WHERE kind='credential' AND origin_key='{}';",
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
            conn.prepare("SELECT claim_owner FROM fence_recheck ORDER BY origin_key")?
                .query_map([], |r| r.get::<_, Option<String>>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap();
    assert_eq!(owners, [Some("decision".to_owned()), Some("decision".to_owned())]);
    rig.check(&[NOW], "after the rebind");
}

/// A block write the capture misses (nothing reports it) leaves the state
/// behind SQL. The periodic tick compares each table's held-row count and
/// highest rowid with SQL, counts the difference and has the state reloaded.
#[test]
fn the_tick_finds_and_reloads_a_block_write_the_capture_missed() {
    let rig = rig("block-state-reconcile");
    let row = |account: &str| {
        format!(
            "INSERT INTO account_transient_backoff VALUES ('p','{account}',{},{NOW},'k','[]');",
            NOW + 60_000
        )
    };
    rig.sql(row("seen"));
    UNSEEN.with(|unseen| unseen.set(true));
    rig.store.bypass_account_writer_for_test(&row("unseen")).unwrap();
    UNSEEN.with(|unseen| unseen.set(false));
    assert_eq!(rig.store.block_writes_outside_command(), 0, "nothing reported the write");
    rig.sql("SELECT 1;".into());
    assert_ne!(rig.published(), rig.fresh(), "the state missed the write");
    let reloads = block_reconcile_reloads();
    let loads = rig.owner.loads.load(Ordering::Relaxed);
    rig.store
        .account_command(AccountCommandKind::Tick, |tx| tx.tick(NOW))
        .unwrap();
    assert_eq!(block_reconcile_reloads() - reloads, 1, "one mismatch counted");
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
    let rig = rig("block-state-retirement-pin");
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
    rig.sql(format!(
        "INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
         VALUES ('whole','p0','a0','{{}}','{{}}','broca_account_limit',{older},'x',{older},NULL,'[{covered}]');"
    ));
    let before_it_grew = rig.published();
    rig.sql(format!(
        "UPDATE account_fence SET recovery_set_json='[{covered},{uncovered}]' WHERE fence_id='whole';"
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
            conn.query_row("SELECT count(*) FROM account_fence WHERE cleared_at IS NULL", [], |r| r.get(0))
        })
        .unwrap();
    assert_eq!(active, 1, "the block waits for its uncovered pool");
    rig.check(&[NOW], "after the heal");
}

/// A block row changed in place without the capture seeing it keeps every
/// table's row count and highest rowid, so the per-tick comparison misses
/// it. A full comparison with a fresh load, every sixth tick, finds it.
#[test]
fn a_periodic_full_comparison_finds_a_missed_change_in_place() {
    let rig = rig("block-state-full-reconcile");
    rig.sql(format!(
        "INSERT INTO account_transient_backoff VALUES ('p','a',{},{NOW},'k','[]');",
        NOW + 60_000
    ));
    UNSEEN.with(|unseen| unseen.set(true));
    rig.store
        .bypass_account_writer_for_test(&format!(
            "UPDATE account_transient_backoff SET retry_at_ms={} WHERE account_id='a';",
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
    assert_eq!(block_reconcile_reloads(), reloads, "counts and rowids match");
    assert_ne!(rig.published(), rig.fresh(), "the state still misses the change");
    for _ in 0..5 {
        tick();
    }
    assert_eq!(block_reconcile_reloads() - reloads, 1, "the full comparison found it");
    rig.check(&[NOW], "after the full comparison");
}

#[test]
fn a_commit_from_another_connection_just_before_a_batch_is_loaded_inside_it() {
    let rig = rig("block-state-outside-before-begin");
    let row = |account: &str| {
        format!(
            "INSERT INTO account_transient_backoff VALUES ('p','{account}',{},{NOW},'k','[]');",
            NOW + 60_000
        )
    };
    rig.sql(row("first"));
    let path = std::fs::canonicalize(rig._scratch.path().join("state.db")).unwrap();
    let outside = path.clone();
    crate::account_writer::BEFORE_BEGIN.lock().unwrap().push((
        path,
        Box::new(move || {
            // Another process commits after the writer's freshness check.
            let conn = Connection::open(&outside).unwrap();
            conn.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
            conn.execute_batch(&row("outside")).unwrap();
        }),
    ));
    rig.sql(row("second"));
    assert!(
        crate::account_writer::BEFORE_BEGIN.lock().unwrap().is_empty(),
        "the hook ran"
    );
    rig.check(&[NOW], "after an outside commit just before the batch began");
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
    assert!(!matches("first"), "a superseded claim cannot settle the probe");
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
        rig.run(fence_rechecks::sync);
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
                    tx.heal_report_probe("first", None, true, "m0", later, &origins, later)
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
            tx.heal_report_probe("first", None, false, "m0", later, &origins, later)
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
    match rng.below(44) {
        0 => {
            let id = if rng.below(2) == 0 {
                format!("f{}", rng.below(4))
            } else {
                format!("credential-quota:[\"{p}\",\"{c}\"]:{}", rng.below(3))
            };
            let pool = match rng.below(3) {
                0 => "NULL".to_owned(),
                key => format!("'{}'", pool_key(key as u32)),
            };
            let recovery = if rng.below(2) == 0 {
                "[]".to_owned()
            } else {
                format!("[{},{}]", pool_key(1), pool_key(2))
            };
            rig.sql(format!("INSERT OR IGNORE INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json) VALUES ('{id}','{p}','a0','{{}}','{{\"reset\":{{\"at_ms\":{until}}}}}','broca_account_limit',{at},'x',{at},{pool},'{recovery}');"));
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
                "account_eligibility",
            ]);
            rig.sql(format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "delete a source row"
        }
        16 | 17 => {
            rig.run(fence_rechecks::sync);
            "sync"
        }
        18 => {
            let owner = owner_name(rng.below(3));
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
            // Either a pool healed for an active fence's current evidence,
            // which shrinks its recovery set, or one for other evidence.
            let key = pool_key(1 + rng.below(2) as u32);
            if rng.below(2) == 0 {
                rig.sql(format!("INSERT OR REPLACE INTO fence_success_pool SELECT fence_id,'{key}',MIN(observed_at,created_at),{now} FROM account_fence ORDER BY rowid LIMIT 1 OFFSET {offset};"));
            } else {
                rig.sql(format!("INSERT OR REPLACE INTO fence_success_pool VALUES ('f{offset}','{key}',{at},{now});"));
            }
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
        29 => {
            // An empty model covers every model on the provider.
            let model = if rng.below(3) == 0 { "" } else { m };
            let fingerprint = rng.pick(&["NULL", "'l0'", "'l1'"]);
            rig.sql(format!("INSERT INTO account_eligibility VALUES ('{p}','{c}','{model}',{fingerprint},'model_unsupported',{at}) ON CONFLICT(provider,credential_id,model) DO UPDATE SET listing_fingerprint=excluded.listing_fingerprint,observed_at=excluded.observed_at;"));
            "eligibility fact"
        }
        30 => {
            rig.sql(format!(
                "DELETE FROM account_eligibility WHERE provider='{p}' AND credential_id='{c}';"
            ));
            "replace a listing's eligibility"
        }
        31..=42 => decide_both(rig, rng, now),
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
                let effective: Vec<FenceRecheck> =
                    tx.fence_rechecks()?.into_iter().skip(first).take(count).collect();
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
                    7 => tx.transfer_probe_authorization(&owner, &other).map(|()| true),
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
                    10 => healed(tx.heal_origins(&effective, &model, admitted, at, scope.as_ref(), flag)),
                    11 => {
                        let effective = tx.fence_rechecks()?;
                        healed(tx.heal_report_probe(
                            &owner,
                            Some(other.as_str()),
                            flag,
                            &model,
                            admitted,
                            &effective,
                            at,
                        ))
                    }
                    12 => {
                        let admission = admission_row(&provider, &credential, &model, admitted);
                        healed(
                            tx.heal_admission_success(&admission, size, Some(other.as_str()), scope.as_ref(), at)
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
                                "SELECT provider_id,credential_id FROM credential_entitlement_fences
                                 ORDER BY rowid LIMIT 1 OFFSET ?1",
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
    let name = DECISIONS.get(which).copied().unwrap_or("decide: classify credential");
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
    eligibility: bool,
    non_quota_account_route_row: bool,
    healed_pool: bool,
    classified_credential: bool,
    repeated_exclusion: bool,
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
                self.eligibility |= count("SELECT count(*) FROM account_eligibility")? > 0;
                self.non_quota_account_route_row |=
                    count("SELECT count(*) FROM account_route_fence WHERE wall_class!='quota'")? > 0;
                self.healed_pool |= count(
                    "SELECT count(*) FROM account_fence f JOIN fence_success_pool s ON s.fence_id=f.fence_id
                     AND s.evidence_at=MIN(f.observed_at,f.created_at) WHERE f.cleared_at IS NULL",
                )? > 0;
                self.classified_credential |= count(
                    "SELECT count(*) FROM fence_recheck r JOIN fence_origin o ON o.kind=r.kind
                     AND o.origin_key=r.origin_key AND o.evidence=r.evidence
                     WHERE r.kind='credential' AND r.classified=1",
                )? > 0;
                self.repeated_exclusion |= count(
                    "SELECT count(*) FROM (SELECT 1 FROM account_principal_exclusion
                     GROUP BY principal,credential_id HAVING count(*)>1)",
                )? > 0;
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
    let decided = DECIDED.with(|decided| decided.take());
    // Every decision answered both ways, except a transfer, which has no
    // answer of its own.
    for name in operations
        .keys()
        .filter(|name| {
            (name.starts_with("decide: ") || name.starts_with("heal: "))
                && **name != "decide: transfer authorization"
        })
    {
        for answer in [true, false] {
            assert!(
                decided.iter().any(|(decision, yes, _)| decision == name && *yes == answer),
                "{name} answered {answer} at least once: {decided:?}"
            );
        }
    }
    assert_eq!(operations.len(), 31 + 16, "every operation ran: {operations:?}");
    let Reached {
        credential_quota_from_hold,
        route_hidden_by_provider,
        quarantined_origin,
        claimed_clock,
        authorization,
        partial_clear,
        tombstone,
        replaced_rowid,
        eligibility,
        non_quota_account_route_row,
        healed_pool,
        classified_credential,
        repeated_exclusion,
    } = reached;
    assert!(eligibility, "an eligibility fact");
    assert!(
        non_quota_account_route_row,
        "an account route fence outside the quota class"
    );
    assert!(healed_pool, "a pool healed for an active fence's evidence");
    assert!(
        classified_credential,
        "a classified credential clock matching its source"
    );
    assert!(repeated_exclusion, "a credential excluded by two admissions");
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
        let cleared = if i % 3 == 0 { "NULL".to_owned() } else { NOW.to_string() };
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
        let wall = if i % 50 == 0 { (NOW + 600_000).to_string() } else { "NULL".into() };
        sql += &format!("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error_at,updated_at) VALUES ('p{}/m{i}','p{}/m{i}','p{}','m{i}',{wall},{NOW},1,{NOW},{NOW});\n", i % 8, i % 8, i % 8);
    }
    for i in 0..n(2) {
        sql += &format!("INSERT INTO account_transient_backoff VALUES ('p{}','a{i}',{},{NOW},'k{i}','[]');\n", i % 8, NOW + 600_000);
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
