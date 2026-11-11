use rusqlite::{Connection, Transaction, TransactionBehavior};

/// Reserve the WAL writer before any reads in a transaction that may write.
/// Upgrading a deferred read transaction can fail immediately with SQLITE_BUSY,
/// bypassing the connection's busy handler and timeout.
pub(crate) fn write_transaction(connection: &Connection) -> rusqlite::Result<Transaction<'_>> {
    Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
}
