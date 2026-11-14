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

/// The tables the block state holds, plus the view over them.
pub const BLOCK_READ_TABLES: [&str; 17] = [
    "account_fence",
    "credential_entitlement_fences",
    "account_route_fence",
    "account_non_quota_route_fence",
    "pool_key_state",
    "provider_cooldown_state",
    "model_route_state",
    "account_size_refusal",
    "account_non_quota_size_refusal",
    "account_principal_exclusion",
    "account_transient_backoff",
    "fence_recheck",
    "fence_recheck_route_clear",
    "fence_probe_authorization",
    "fence_success_pool",
    "account_eligibility",
    "fence_origin",
];

/// Whether `sql` reads one of the block tables or the `fence_origin` view.
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
    _statement: *mut c_void,
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
    if !text.starts_with("--") {
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
    pub fn trace_block_reads_for_test(&self) -> RouteStateResult<&'static BlockReadTrace> {
        let trace: &'static BlockReadTrace = Box::leak(Box::default());
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

/// Block-table reads seen since the last [`BlockReadTrace::take`].
#[derive(Default)]
pub struct BlockReadTrace {
    reader: Mutex<Vec<String>>,
    writer: Mutex<Vec<String>>,
    exempt: Mutex<Vec<(&'static str, String)>>,
    maintenance: Mutex<usize>,
}

thread_local! {
    /// The reason of the declared exception running on this thread, if any.
    static EXCEPTION: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

/// Run `read` as a declared exception to the rule that request paths read
/// the block tables from the account state: the block-table reads it makes
/// are traced under `reason`, apart from ordinary reads. Outside a trace this
/// only sets a thread-local flag.
pub fn declared_exception<T>(reason: &'static str, read: impl FnOnce() -> T) -> T {
    struct Restore(Option<&'static str>);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXCEPTION.with(|exception| exception.set(self.0));
        }
    }
    let _restore = Restore(EXCEPTION.with(|exception| exception.replace(Some(reason))));
    read()
}

/// What a [`BlockReadTrace`] saw: ordinary reads on pooled readers, ordinary
/// reads on the writer, and the block state's own loads and read-backs of the
/// rows commands changed, which keep it current and are not request reads.
#[derive(Debug, Default)]
pub struct BlockReads {
    pub reader: Vec<String>,
    pub writer: Vec<String>,
    /// Reads made inside a [`declared_exception`], with its reason.
    pub exempt: Vec<(&'static str, String)>,
    pub maintenance: usize,
}

impl BlockReadTrace {
    pub fn take(&self) -> BlockReads {
        BlockReads {
            reader: std::mem::take(&mut self.reader.lock().unwrap()),
            writer: std::mem::take(&mut self.writer.lock().unwrap()),
            exempt: std::mem::take(&mut self.exempt.lock().unwrap()),
            maintenance: std::mem::take(&mut self.maintenance.lock().unwrap()),
        }
    }
}

struct BlockTap {
    trace: &'static BlockReadTrace,
    writer: bool,
}

unsafe extern "C" fn on_block_statement(
    _mask: c_uint,
    context: *mut c_void,
    _statement: *mut c_void,
    sql: *mut c_void,
) -> c_int {
    if sql.is_null() {
        return 0;
    }
    // SAFETY: as in `on_statement`; `context` is a leaked `BlockTap`.
    let text = unsafe { CStr::from_ptr(sql as *const std::ffi::c_char) }.to_string_lossy();
    let tap = unsafe { &*(context as *const BlockTap) };
    if text.starts_with("--") || !reads_block_tables(&text) {
        return 0;
    }
    if text.contains(crate::block_state::MAINTENANCE_READ) {
        *tap.trace.maintenance.lock().unwrap() += 1;
    } else if let Some(reason) = EXCEPTION.with(std::cell::Cell::get) {
        tap.trace
            .exempt
            .lock()
            .unwrap()
            .push((reason, text.into_owned()));
    } else if tap.writer {
        tap.trace.writer.lock().unwrap().push(text.into_owned());
    } else {
        tap.trace.reader.lock().unwrap().push(text.into_owned());
    }
    0
}

fn attach_block_tap(conn: &Connection, tap: &'static BlockTap) {
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

#[cfg(test)]
mod tests {
    use super::reads_admission_group;

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
