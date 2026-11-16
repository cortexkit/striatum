use super::*;

#[path = "healing/tests.rs"]
mod healing;
#[path = "state_flags/tests.rs"]
mod state_flags;

impl World {
    /// Snapshot all native durable block columns, including dropped audit rows.
    /// This test-only maintenance read is outside the commands being measured.
    /// Counting writes is insufficient: a repeated SQL update can leave every
    /// stored value unchanged, which is the healing operation's no-op case.
    pub(super) fn native_block_rows(&self) -> Vec<(&'static str, Vec<Vec<String>>)> {
        self.harness
            .store()
            .with_reader_for_test(|conn| {
                striatum_store::BLOCK_TABLES
                    .into_iter()
                    .map(|table| {
                        let mut statement = conn.prepare(&format!(
                            "/* block state maintenance */ SELECT rowid,* FROM {table} ORDER BY rowid"
                        ))?;
                        let columns = statement.column_count();
                        let rows = statement
                            .query_map([], |row| {
                                (0..columns)
                                    .map(|column| {
                                        row.get_ref(column).map(|value| format!("{value:?}"))
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                            })?
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok((table, rows))
                    })
                    .collect()
            })
            .unwrap()
    }
}
