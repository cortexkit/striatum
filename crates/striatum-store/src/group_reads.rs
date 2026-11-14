//! Test support: record every SQL statement that reads the admission group's
//! tables (`account_admission`, `account_affinity`, `account_continue`).
//!
//! The admission group is served from the module's in-memory account state,
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
    let text = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if !text.contains("select") {
        return false;
    }
    GROUP_TABLES.iter().any(|table| {
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
