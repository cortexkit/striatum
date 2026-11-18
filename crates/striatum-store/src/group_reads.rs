//! Test support: record every SQL statement that reads the admission group's
//! tables (`account_admission`, `account_affinity`, `account_continue`), or
//! the block tables and the `fence_origin` view.
//!
//! Both groups are served from the module's in-memory account state,
//! so a request should not read these tables. A Rust-level counter would miss
//! raw SQL reached some other way, so this hook watches what SQLite actually
//! runs: it attaches a statement trace to the writer and to every pooled reader
//! and classifies each statement by its text. Keyed writes (`INSERT`, or an
//! `UPDATE`/`DELETE` whose predicate names its own row) are not reads; any
//! `SELECT` from these tables, including a sub-query inside a write, is.
//!
//! A statement carrying [`crate::KEYED_EXCEPTION`] in its SQL is the one sanctioned
//! read: a primary-key lookup of an admission or continuation that the memory
//! state does not hold because it is closed. Those are recorded separately.

use std::ffi::{c_int, c_uint, c_void, CStr};
use std::sync::Mutex;

use rusqlite::{ffi, Connection};
use striatum_core::route_state_store::RouteStateResult;

use crate::{err, SqliteRouteStateStore, KEYED_EXCEPTION};

const GROUP_TABLES: [&str; 3] = ["account_admission", "account_affinity", "account_continue"];

/// Whether `sql` reads one of the admission group's tables.
pub fn reads_admission_group(sql: &str) -> bool {
    reads_tables(sql, &GROUP_TABLES)
}

/// The physical tables whose rows feed the published block state.
pub const BLOCK_READ_TABLES: [&str; 8] = crate::block_state::BLOCK_TABLES;

/// Whether `sql` reads one of the block-state tables.
pub fn reads_block_tables(sql: &str) -> bool {
    reads_tables(sql, &BLOCK_READ_TABLES)
}

/// Whether `sql` selects from or joins one of `tables`.
pub fn reads_tables(sql: &str, tables: &[&str]) -> bool {
    let text = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if !text.contains("select") {
        return false;
    }
    tables.iter().any(|table| {
        ["from ", "join "].iter().any(|keyword| {
            let needle = format!("{keyword}{table}");
            text.match_indices(&needle).any(|(at, _)| {
                // `account_admission_rebuilt` is another table.
                !text[at + needle.len()..]
                    .chars()
                    .next()
                    .is_some_and(|next| next.is_ascii_alphanumeric() || next == '_')
            })
        })
    })
}

/// Whether a statement trace event is a trigger's: SQLite then passes a
/// "-- <trigger>" comment rather than the statement's own text, so the text
/// differs from the prepared statement's. A statement whose own text starts
/// with a comment is not one.
///
/// # Safety
/// `statement` is the `sqlite3_stmt` SQLite passed with the event.
unsafe fn trigger_event(statement: *mut c_void, text: &str) -> bool {
    if !text.starts_with("--") {
        return false;
    }
    // SAFETY: for SQLITE_TRACE_STMT the P argument is the running statement.
    let own = unsafe { ffi::sqlite3_sql(statement as *mut ffi::sqlite3_stmt) };
    own.is_null() || unsafe { CStr::from_ptr(own) }.to_string_lossy() != text
}

/// Statements seen since the last [`GroupReads::take`].
#[derive(Default)]
pub struct GroupReads {
    reads: Mutex<Vec<String>>,
    keyed: Mutex<Vec<String>>,
}

impl GroupReads {
    /// (ordinary reads, keyed-exception reads), clearing both.
    pub fn take(&self) -> (Vec<String>, Vec<String>) {
        (
            std::mem::take(&mut self.reads.lock().unwrap()),
            std::mem::take(&mut self.keyed.lock().unwrap()),
        )
    }

    fn record(&self, sql: &str) {
        if !reads_admission_group(sql) {
            return;
        }
        let list = if sql.contains(KEYED_EXCEPTION) {
            &self.keyed
        } else {
            &self.reads
        };
        list.lock().unwrap().push(sql.to_owned());
    }
}

unsafe extern "C" fn on_statement(
    _mask: c_uint,
    context: *mut c_void,
    statement: *mut c_void,
    sql: *mut c_void,
) -> c_int {
    if sql.is_null() {
        return 0;
    }
    // SAFETY: for SQLITE_TRACE_STMT, SQLite passes the statement's
    // NUL-terminated SQL text; `context` is the leaked `GroupReads` below,
    // which lives for the rest of the process.
    let text = unsafe { CStr::from_ptr(sql as *const std::ffi::c_char) }.to_string_lossy();
    // Statements a trigger runs arrive as "-- <trigger>"; their own reads are
    // part of the write that fired them.
    if !unsafe { trigger_event(statement, &text) } {
        unsafe { &*(context as *const GroupReads) }.record(&text);
    }
    0
}

fn attach(conn: &Connection, reads: &'static GroupReads) {
    // SAFETY: the handle belongs to a live connection we hold; the callback
    // only reads the statement text and touches `reads`, which is 'static.
    unsafe {
        ffi::sqlite3_trace_v2(
            conn.handle(),
            ffi::SQLITE_TRACE_STMT as c_uint,
            Some(on_statement),
            reads as *const GroupReads as *mut c_void,
        );
    }
}

impl SqliteRouteStateStore {
    /// Trace admission-group reads on the writer and on every pooled reader.
    /// Each reader is checked out at once, so none can escape the trace.
    pub fn trace_admission_group_reads_for_test(&self) -> RouteStateResult<&'static GroupReads> {
        let reads: &'static GroupReads = Box::leak(Box::default());
        self.store
            .with_conn(|conn| {
                attach(conn, reads);
                Ok(())
            })
            .map_err(err)?;
        fn each_reader(
            store: &SqliteRouteStateStore,
            remaining: usize,
            reads: &'static GroupReads,
        ) -> Result<(), cortexkit_store::StoreError> {
            if remaining == 0 {
                return Ok(());
            }
            let mut nested = Ok(());
            store.store.with_read(|conn| {
                attach(conn, reads);
                nested = each_reader(store, remaining - 1, reads);
                Ok(())
            })?;
            nested
        }
        each_reader(
            self,
            cortexkit_store::SqliteOpenOptions::default().read_pool_size,
            reads,
        )
        .map_err(err)?;
        Ok(reads)
    }

    /// Trace block-table reads, telling the writer's from the pooled
    /// readers'. Each reader is checked out at once, so none can escape the
    /// trace.
    pub fn trace_account_reads_for_test(&self) -> RouteStateResult<&'static BlockReadTrace> {
        self.trace_block_reads_kind(true)
    }

    pub fn trace_block_reads_for_test(&self) -> RouteStateResult<&'static BlockReadTrace> {
        self.trace_block_reads_kind(false)
    }

    fn trace_block_reads_kind(
        &self,
        all_account: bool,
    ) -> RouteStateResult<&'static BlockReadTrace> {
        let trace: &'static BlockReadTrace = Box::leak(Box::new(BlockReadTrace {
            all_account,
            ..Default::default()
        }));
        let writer: &'static BlockTap = Box::leak(Box::new(BlockTap {
            trace,
            writer: true,
        }));
        let reader: &'static BlockTap = Box::leak(Box::new(BlockTap {
            trace,
            writer: false,
        }));
        self.store
            .with_conn(|conn| {
                attach_block_tap(conn, writer);
                Ok(())
            })
            .map_err(err)?;
        fn each_reader(
            store: &SqliteRouteStateStore,
            remaining: usize,
            tap: &'static BlockTap,
        ) -> Result<(), cortexkit_store::StoreError> {
            if remaining == 0 {
                return Ok(());
            }
            let mut nested = Ok(());
            store.store.with_read(|conn| {
                attach_block_tap(conn, tap);
                nested = each_reader(store, remaining - 1, tap);
                Ok(())
            })?;
            nested
        }
        each_reader(
            self,
            cortexkit_store::SqliteOpenOptions::default().read_pool_size,
            reader,
        )
        .map_err(err)?;
        Ok(trace)
    }
}

/// The physical tables watched by [`BlockReadTrace`].
pub const BLOCK_TRACE_TABLES: [&str; 8] = crate::block_state::BLOCK_TABLES;

/// The schema triggers that read block tables when a write fires them, each
/// with the key it reads by. A statement whose only block reads come from
/// these is counted apart ([`BlockReads::triggers`]); a read from any other
/// trigger is an ordinary read, so a new reading trigger fails the trace.
pub const REVIEWED_TRIGGERS: [(&str, &str); 4] = [
    (
        "model_facts_inherit_provider_cooldown",
        "provider_cooldown_state by its primary key provider_key = NEW.provider_id",
    ),
    (
        "retirement_tombstone_enforce",
        "block by its primary key kind and origin_key, with the exact evidence signature",
    ),
    (
        "block_retirement_insert",
        "retirement_tombstone by kind, origin_key and evidence; block by kind and origin_key",
    ),
    (
        "block_retirement_update",
        "retirement_tombstone by kind, origin_key and evidence; block by kind and origin_key",
    ),
];

/// The trace counts these exact `source_order` lookups separately from ordinary
/// block-table reads, and only on the writer inside a writer command.
pub const REVIEWED_WRITER_BOOKKEEPING: [(&str, &str); 2] = [
    (
        crate::block_storage::NEXT_SOURCE_ORDER,
        "writer bookkeeping: next source order per kind",
    ),
    (
        crate::block_storage::ROUTE_SOURCE_ORDER,
        "writer bookkeeping: source order is the live route row's rowid",
    ),
];

/// A `block` insert for transient backoff from an account observation checks
/// `retirement_tombstone` for that origin. Count only this exact insert on the
/// writer inside a writer command, not other SQL with the same marker or lookup.
pub const REVIEWED_OBSERVATION_RETIREMENT_GUARD: (&str, &str) = (
    crate::block_storage::OBSERVATION_RETIREMENT_GUARD_MARKER,
    "retirement guard on the observation-effect insert: one covering keyed seek, LIMIT 1",
);

/// Count the `block_clock_drop` trigger's primary-key lookup separately from
/// ordinary request reads and other triggers.
pub const REVIEWED_CLOCK_NORMALIZATION_TRIGGER: (&str, &str) = (
    "block_clock_drop",
    "block by its primary key kind = NEW.kind and origin_key = NEW.origin_key",
);

/// Writes that run a `SELECT` of their own over other tables (a bound
/// `json_each` list, or the fields of an `account_observe` report bound as
/// parameters) yet read their own target only
/// for the rows they write: the row predicate, an upsert's guard,
/// `RETURNING`. Any other write running a `SELECT` has every read of its
/// target counted, because the authorizer does not say which select a read
/// belongs to. Matched by exact text, so an edited statement is counted
/// until it is reviewed again.
pub const REVIEWED_OWN_ROW_WRITES: [&str; 4] = [
    crate::UPDATE_INHERITED_ROUTE_WALLS,
    crate::UPDATE_INHERITED_ROUTE_WALL_SCOPE,
    crate::OPERATOR_CLEAR_ROUTE_BLOCKS,
    crate::OPERATOR_CLEAR_PROVIDER_MODEL,
];

/// Block-table reads seen since the last [`BlockReadTrace::take`].
#[derive(Default)]
pub struct BlockReadTrace {
    all_account: bool,
    reader: Mutex<Vec<String>>,
    writer: Mutex<Vec<String>>,
    maintenance: Mutex<usize>,
    checks: Mutex<Vec<String>>,
    route_writes: Mutex<Vec<String>>,
    route_write_rows: Mutex<Vec<String>>,
    writer_bookkeeping: Mutex<Vec<String>>,
    observation_retirement_guards: Mutex<Vec<String>>,
    clock_normalization_reads: Mutex<Vec<String>>,
    clock_normalizations: Mutex<usize>,
    triggers: Mutex<Vec<String>>,
    /// A connection to the traced database, used only to prepare each new
    /// statement text once under an authorizer, and what that found.
    classifier: Mutex<Option<(Connection, std::collections::HashMap<String, Classified>)>>,
}

/// What preparing one statement under the authorizer found: the watched
/// tables it reads itself (directly, through a view or a sub-query), and the
/// triggers it fires that read watched tables.
#[derive(Clone, Default)]
struct Classified {
    read: Vec<String>,
    triggers: Vec<String>,
}

/// What a [`BlockReadTrace`] saw.
#[derive(Debug, Default)]
pub struct BlockReads {
    /// Ordinary reads on pooled readers.
    pub reader: Vec<String>,
    /// Ordinary reads on the writer.
    pub writer: Vec<String>,
    /// The block state's own loads and read-backs of the rows commands
    /// changed, which keep it current and are not request reads.
    pub maintenance: usize,
    /// Keyed reads a decision made on the writer to check the rows it read
    /// from the working state against SQL (`STATE_CHECK`), by text.
    pub checks: Vec<String>,
    /// A route-state writer's read of the row it is about to write, which
    /// decides only that write (`ROUTE_WRITE_READ`), by text.
    pub route_writes: Vec<String>,
    /// The same reads with their bound values, naming the rows they read.
    pub route_write_rows: Vec<String>,
    /// Exact catalogued insertion-order reads made inside a writer command.
    pub writer_bookkeeping: Vec<String>,
    /// Writer inserts into `block` for account-observation backoff that match
    /// the exact SQL and check `retirement_tombstone` for that origin.
    pub observation_retirement_guards: Vec<String>,
    /// Executed normalization bodies with the reviewed primary-key lookup,
    /// only on the writer connection inside a writer command.
    pub clock_normalization_reads: Vec<String>,
    /// Actual executions of the normalization body. Preparation can see a
    /// trigger even when its WHEN condition is false, so reads are not a count
    /// of real transitions. The runtime statement trace counts those instead.
    pub clock_normalizations: usize,
    /// Writes whose only block reads are those of [`REVIEWED_TRIGGERS`] they
    /// fire, by text.
    pub triggers: Vec<String>,
}

impl BlockReadTrace {
    pub fn take(&self) -> BlockReads {
        BlockReads {
            reader: std::mem::take(&mut self.reader.lock().unwrap()),
            writer: std::mem::take(&mut self.writer.lock().unwrap()),
            maintenance: std::mem::take(&mut self.maintenance.lock().unwrap()),
            checks: std::mem::take(&mut self.checks.lock().unwrap()),
            route_writes: std::mem::take(&mut self.route_writes.lock().unwrap()),
            route_write_rows: std::mem::take(&mut self.route_write_rows.lock().unwrap()),
            writer_bookkeeping: std::mem::take(&mut self.writer_bookkeeping.lock().unwrap()),
            observation_retirement_guards: std::mem::take(
                &mut self.observation_retirement_guards.lock().unwrap(),
            ),
            clock_normalization_reads: std::mem::take(
                &mut self.clock_normalization_reads.lock().unwrap(),
            ),
            clock_normalizations: std::mem::take(&mut self.clock_normalizations.lock().unwrap()),
            triggers: std::mem::take(&mut self.triggers.lock().unwrap()),
        }
    }

    /// Whether a read of `table` is traced: the block tables always, and
    /// every account table when the trace was asked for all of them.
    fn watches(&self, table: &str) -> bool {
        BLOCK_TRACE_TABLES.contains(&table)
            || self.all_account && crate::connection_store::ACCOUNT_TABLES.contains(&table)
    }

    /// The watched tables `sql` reads, as SQLite's authorizer reports them
    /// while preparing it: every `SQLITE_READ` of a watched table, through
    /// a view, a sub-query, a comma join or a trigger the statement fires,
    /// whatever the spelling of the name. A write's reads of its own target
    /// table (its row predicate, an upsert's guard, `RETURNING`) are part of
    /// the write only in the reviewed plain forms, which run no `SELECT` of
    /// their own: SQLite's authorizer reports a `SELECT` for every select,
    /// sub-query or view the statement codes, and then every read of the
    /// target counts, since the authorizer does not say which of them come
    /// from the sub-query ([`REVIEWED_OWN_ROW_WRITES`] excepted). A text that cannot be prepared here is reported as
    /// read when it names a watched table.
    fn tables_read(&self, sql: &str) -> Classified {
        let mut guard = self.classifier.lock().unwrap();
        let Some((conn, cache)) = guard.as_mut() else {
            return Classified::default();
        };
        if let Some(found) = cache.get(sql) {
            return found.clone();
        }
        let events: std::sync::Arc<Mutex<Vec<(u8, String, Option<String>)>>> = Default::default();
        let sink = events.clone();
        conn.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
            use rusqlite::hooks::AuthAction;
            let inner = context.accessor.map(str::to_owned);
            let event = match context.action {
                AuthAction::Read { table_name, .. } => Some((0, table_name, inner)),
                AuthAction::Insert { table_name } => Some((1, table_name, inner)),
                AuthAction::Update { table_name, .. } => Some((1, table_name, inner)),
                AuthAction::Delete { table_name } => Some((1, table_name, inner)),
                AuthAction::Select => Some((2, "", inner)),
                _ => None,
            };
            if let Some((kind, table, inner)) = event {
                sink.lock().unwrap().push((kind, table.to_owned(), inner));
            }
            rusqlite::hooks::Authorization::Allow
        }));
        let prepared = conn.prepare(sql).map(drop);
        conn.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        );
        let triggers: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='trigger' UNION SELECT name FROM sqlite_temp_master WHERE type='trigger'")
            .and_then(|mut statement| {
                statement
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap_or_default();
        let found = match prepared {
            Ok(()) => {
                let events = events.lock().unwrap();
                // The accessor names the innermost trigger or view; a view's
                // reads are the statement's own.
                let trigger = |accessor: &Option<String>| {
                    accessor
                        .as_ref()
                        .filter(|name| triggers.contains(name))
                        .cloned()
                };
                let target = events
                    .iter()
                    .find(|(kind, _, accessor)| *kind == 1 && trigger(accessor).is_none())
                    .map(|(_, table, _)| table.clone());
                // A write reads its own target's columns for its row
                // predicate, an upsert's guard or `RETURNING`; that is part
                // of the write. A write that runs any `SELECT` of its own (an
                // `INSERT … SELECT`, a sub-query, a view) may read other rows
                // of its target however the text spells it, so every read
                // of the target then counts, unless the write is on the
                // reviewed list.
                let selects = events
                    .iter()
                    .any(|(kind, _, accessor)| *kind == 2 && trigger(accessor).is_none());
                let reviewed = REVIEWED_OWN_ROW_WRITES.contains(&sql);
                let own_row =
                    |table: &str| target.as_deref() == Some(table) && (!selects || reviewed);
                let watched = |kind: &u8, table: &String| *kind == 0 && self.watches(table);
                let mut read: Vec<String> = events
                    .iter()
                    .filter(|(kind, table, accessor)| {
                        watched(kind, table) && trigger(accessor).is_none() && !own_row(table)
                    })
                    .map(|(_, table, _)| table.clone())
                    .collect();
                read.sort();
                read.dedup();
                let mut fired: Vec<String> = events
                    .iter()
                    .filter(|(kind, table, _)| watched(kind, table))
                    .filter_map(|(_, _, accessor)| trigger(accessor))
                    .collect();
                fired.sort();
                fired.dedup();
                Classified {
                    read,
                    triggers: fired,
                }
            }
            Err(_)
                if reads_tables(sql, &BLOCK_TRACE_TABLES)
                    || self.all_account
                        && reads_tables(sql, crate::connection_store::ACCOUNT_TABLES) =>
            {
                Classified {
                    read: vec!["(unprepared)".into()],
                    triggers: Vec::new(),
                }
            }
            Err(_) => Classified::default(),
        };
        cache.insert(sql.to_owned(), found.clone());
        found
    }
}

struct BlockTap {
    trace: &'static BlockReadTrace,
    writer: bool,
}

unsafe extern "C" fn on_block_statement(
    _mask: c_uint,
    context: *mut c_void,
    statement: *mut c_void,
    sql: *mut c_void,
) -> c_int {
    if sql.is_null() {
        return 0;
    }
    // SAFETY: as in `on_statement`; `context` is a leaked `BlockTap`.
    let text = unsafe { CStr::from_ptr(sql as *const std::ffi::c_char) }.to_string_lossy();
    let tap = unsafe { &*(context as *const BlockTap) };
    // A trigger's statements arrive as "-- <trigger>"; their reads were
    // found when the statement that fires them was prepared.
    if unsafe { trigger_event(statement, &text) } {
        if text.strip_prefix("--").is_some_and(|body| {
            let compact = |sql: &str| {
                sql.chars()
                    .filter(|ch| !ch.is_whitespace() && *ch != ';')
                    .flat_map(char::to_lowercase)
                    .collect::<String>()
            };
            compact(body) == compact(crate::block_storage::CLOCK_NORMALIZATION_BODY)
        }) {
            *tap.trace.clock_normalizations.lock().unwrap() += 1;
            if tap.writer && crate::block_state::in_writer_command() {
                tap.trace
                    .clock_normalization_reads
                    .lock()
                    .unwrap()
                    .push(text.to_string());
            }
        }
        return 0;
    }
    let found = tap.trace.tables_read(&text);
    if found.read.is_empty() {
        if found.triggers.is_empty() {
            return 0;
        }
        let normalization = found
            .triggers
            .iter()
            .any(|name| name == REVIEWED_CLOCK_NORMALIZATION_TRIGGER.0);
        let other = found
            .triggers
            .iter()
            .any(|name| name != REVIEWED_CLOCK_NORMALIZATION_TRIGGER.0);
        if (!normalization || tap.writer && crate::block_state::in_writer_command())
            && found.triggers.iter().all(|name| {
                name == REVIEWED_CLOCK_NORMALIZATION_TRIGGER.0
                    || REVIEWED_TRIGGERS
                        .iter()
                        .any(|(reviewed, _)| reviewed == name)
            })
        {
            if other {
                tap.trace.triggers.lock().unwrap().push(text.into_owned());
            }
            return 0;
        }
    }
    if tap.writer
        && crate::block_state::in_writer_command()
        && found
            .read
            .iter()
            .any(|table| table == "retirement_tombstone")
        && text.starts_with(REVIEWED_OBSERVATION_RETIREMENT_GUARD.0)
        && crate::block_storage::is_observation_retirement_insert(&text)
    {
        tap.trace
            .observation_retirement_guards
            .lock()
            .unwrap()
            .push(text.into_owned());
    } else if tap.writer
        && crate::block_state::in_writer_command()
        && REVIEWED_WRITER_BOOKKEEPING
            .iter()
            .any(|(sql, _)| *sql == text)
    {
        tap.trace
            .writer_bookkeeping
            .lock()
            .unwrap()
            .push(text.into_owned());
    } else if text.contains(crate::block_state::MAINTENANCE_READ) {
        *tap.trace.maintenance.lock().unwrap() += 1;
    } else if text.contains(crate::block_state::STATE_CHECK) {
        tap.trace.checks.lock().unwrap().push(text.into_owned());
    } else if text.contains(crate::ROUTE_WRITE_READ) {
        // SAFETY: `statement` is the running statement; SQLite allocates the
        // expanded text, which is freed here after it is copied.
        let expanded = unsafe {
            let raw = ffi::sqlite3_expanded_sql(statement as *mut ffi::sqlite3_stmt);
            if raw.is_null() {
                None
            } else {
                let copy = CStr::from_ptr(raw).to_string_lossy().into_owned();
                ffi::sqlite3_free(raw as *mut c_void);
                Some(copy)
            }
        };
        tap.trace
            .route_write_rows
            .lock()
            .unwrap()
            .push(expanded.unwrap_or_else(|| text.to_string()));
        tap.trace
            .route_writes
            .lock()
            .unwrap()
            .push(text.into_owned());
    } else if tap.writer {
        tap.trace.writer.lock().unwrap().push(text.into_owned());
    } else {
        tap.trace.reader.lock().unwrap().push(text.into_owned());
    }
    0
}

fn attach_block_tap(conn: &Connection, tap: &'static BlockTap) {
    {
        let mut classifier = tap.trace.classifier.lock().unwrap();
        if classifier.is_none() {
            if let Some(path) = conn.path().filter(|path| !path.is_empty()) {
                let side = Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                )
                .expect("open the classifier connection");
                // The sync's TEMP snapshot, so statements naming it prepare.
                side.execute_batch(
                    "CREATE TEMP TABLE IF NOT EXISTS current_fence_origin AS SELECT * FROM fence_origin WHERE 0;",
                )
                .ok();
                *classifier = Some((side, Default::default()));
            }
        }
    }
    // SAFETY: as in `attach`; `tap` is 'static.
    unsafe {
        ffi::sqlite3_trace_v2(
            conn.handle(),
            ffi::SQLITE_TRACE_STMT as c_uint,
            Some(on_block_statement),
            tap as *const BlockTap as *mut c_void,
        );
    }
}

/// Every statement the writer connection finished since the last
/// [`StatementProfile::take`], with the nanoseconds SQLite reports it ran
/// (`SQLITE_TRACE_PROFILE`). A statement's triggers run inside it, so their
/// time is part of its own. Test support only: production keeps no
/// per-statement trace.
#[derive(Default)]
pub struct StatementProfile {
    runs: Mutex<Vec<(String, u64)>>,
}

impl StatementProfile {
    /// (statement text, nanoseconds) in the order the statements finished,
    /// clearing the list.
    pub fn take(&self) -> Vec<(String, u64)> {
        std::mem::take(&mut self.runs.lock().unwrap())
    }
}

unsafe extern "C" fn on_profile(
    _mask: c_uint,
    context: *mut c_void,
    statement: *mut c_void,
    elapsed: *mut c_void,
) -> c_int {
    if statement.is_null() || elapsed.is_null() {
        return 0;
    }
    // SAFETY: for SQLITE_TRACE_PROFILE, SQLite passes the finished statement
    // and a pointer to its run time in nanoseconds; `context` is the leaked
    // `StatementProfile` below, which lives for the rest of the process.
    let ns = unsafe { *(elapsed as *const i64) };
    let own = unsafe { ffi::sqlite3_sql(statement as *mut ffi::sqlite3_stmt) };
    let text = if own.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(own) }
            .to_string_lossy()
            .into_owned()
    };
    let profile = unsafe { &*(context as *const StatementProfile) };
    profile
        .runs
        .lock()
        .unwrap()
        .push((text, u64::try_from(ns).unwrap_or(0)));
    0
}

impl SqliteRouteStateStore {
    /// Time every statement the writer connection runs, which the account
    /// writer's commands use. Replaces any other trace on that connection.
    pub fn profile_writer_statements_for_test(
        &self,
    ) -> RouteStateResult<&'static StatementProfile> {
        let profile: &'static StatementProfile = Box::leak(Box::default());
        self.store
            .with_conn(|conn| {
                // SAFETY: as in `attach`; `profile` is 'static.
                unsafe {
                    ffi::sqlite3_trace_v2(
                        conn.handle(),
                        ffi::SQLITE_TRACE_PROFILE as c_uint,
                        Some(on_profile),
                        profile as *const StatementProfile as *mut c_void,
                    );
                }
                Ok(())
            })
            .map_err(err)?;
        Ok(profile)
    }
}

#[cfg(test)]
mod tests {
    use super::reads_admission_group;

    /// The block read trace finds reads by what SQLite's authorizer reports,
    /// so a comma join, a quoted or schema-qualified name, a sub-query inside
    /// a write, a write selecting from its own target (however spelled, or
    /// through a view) and a trigger fired by a write are all reads; a keyed
    /// write of a block row is not.
    #[test]
    fn the_block_trace_sees_reads_however_they_are_spelled() {
        let scratch = cortexkit_test_support::ScratchDir::new("block-trace-spellings");
        let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
            "block-trace-spellings",
            scratch.path().join("state.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .with_writer_for_test(|conn| {
                conn.execute_batch(
                    "CREATE TABLE trace_scratch(x); CREATE TABLE trace_log(n);
                     CREATE TRIGGER trace_scratch_reads AFTER INSERT ON trace_scratch
                     BEGIN INSERT INTO trace_log SELECT count(*) FROM block; END;
                     CREATE VIEW trace_keys AS SELECT credential_id FROM pool_key_state;
                     CREATE VIEW trace_blocks AS SELECT origin_key FROM block;",
                )
            })
            .unwrap();
        let trace = store.trace_block_reads_for_test().unwrap();
        let reads = [
            "SELECT count(*) FROM sqlite_master, \"block\"",
            "SELECT count(*) FROM main.pool_key_state",
            "SELECT count(*) FROM trace_blocks",
            "UPDATE trace_log SET n=1 WHERE n IN (SELECT last_used_at FROM pool_key_state)",
            "INSERT INTO pool_key_state(provider,credential_id,updated_at) SELECT provider,credential_id||'x',updated_at FROM pool_key_state WHERE 0",
            "INSERT INTO trace_scratch VALUES (1)",
            "-- a statement that starts with a comment\nSELECT count(*) FROM block",
            "UPDATE pool_key_state SET last_used_at=1 WHERE credential_id IN (SELECT credential_id FROM \"pool_key_state\")",
            "UPDATE pool_key_state SET last_used_at=1 WHERE credential_id IN (SELECT credential_id FROM main.pool_key_state)",
            "UPDATE pool_key_state SET last_used_at=1 WHERE credential_id IN (SELECT n FROM trace_log , pool_key_state)",
            // A write reading other rows of its own target, spelled so no
            // text match finds the target after `FROM`: a comment between,
            // a parenthesised table, and a view over the target.
            "UPDATE pool_key_state SET last_used_at=(SELECT max(last_used_at) FROM/*x*/pool_key_state)",
            "UPDATE pool_key_state SET last_used_at=1 WHERE credential_id IN (SELECT credential_id FROM (pool_key_state))",
            "UPDATE pool_key_state SET last_used_at=1 WHERE credential_id IN (SELECT credential_id FROM trace_keys)",
        ];
        for sql in reads {
            store
                .with_writer_for_test(|conn| conn.execute_batch(sql))
                .unwrap();
            assert_eq!(trace.take().writer.len(), 1, "a read: {sql}");
        }
        for sql in [
            "UPDATE pool_key_state SET last_used_at=1 WHERE provider='p' AND credential_id='c'",
            "INSERT INTO pool_key_state(provider,credential_id,updated_at) VALUES ('p','c',1)
               ON CONFLICT(provider,credential_id) DO UPDATE SET updated_at=excluded.updated_at
               WHERE excluded.updated_at>=pool_key_state.updated_at",
            "SELECT count(*) FROM trace_log",
        ] {
            store
                .with_writer_for_test(|conn| conn.execute_batch(sql))
                .unwrap();
            assert!(trace.take().writer.is_empty(), "not a block read: {sql}");
        }
    }

    #[test]
    fn classifies_selects_and_sub_queries_but_not_keyed_writes() {
        for read in [
            "SELECT * FROM account_admission WHERE principal=?1",
            "SELECT 1 FROM x JOIN\n   account_affinity f ON 1",
            "UPDATE account_admission SET state='released' WHERE (principal,admission_id) IN (SELECT principal,reservation_id FROM account_continue)",
        ] {
            assert!(reads_admission_group(read), "{read}");
        }
        for write in [
            "INSERT INTO account_admission (principal) VALUES (?1)",
            "UPDATE account_admission SET state='finished' WHERE principal=?1 AND admission_id=?2",
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='account_continue')",
            "SELECT * FROM account_admission_rebuilt",
        ] {
            assert!(!reads_admission_group(write), "{write}");
        }
    }
}

#[cfg(test)]
#[path = "group_reads/native/tests.rs"]
mod native_tests;
