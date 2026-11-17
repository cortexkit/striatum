//! Schema-aware entry points for the parity tests. The independent SQL reader
//! is separate from this adapter-versus-reference comparison driver.

use super::*;

#[path = "native_reference.rs"]
mod native_reference;

pub(super) fn native(conn: &Connection) -> rusqlite::Result<bool> {
    native_reference::enabled(conn)
}

pub(super) fn times(
    conn: &Connection,
    now: i64,
) -> rusqlite::Result<std::collections::BTreeSet<i64>> {
    native_reference::times(conn, now)
}

pub(super) fn identities(conn: &Connection) -> rusqlite::Result<Vec<[String; 4]>> {
    native_reference::identities(conn)
}

impl SqliteRouteStateStore {
    /// Include every source deadline and persisted retry boundary, with the
    /// original millisecond neighbours, even when a managed retry capped it.
    pub fn block_parity_times(&self, now: i64) -> RouteStateResult<Vec<i64>> {
        self.store
            .with_read(|conn| Ok(parity_times(conn, now)?.into_iter().collect()))
            .map_err(err)
    }

    pub fn block_parity_identities(&self) -> RouteStateResult<Vec<[String; 4]>> {
        self.store.with_read(parity_identities).map_err(err)
    }

    pub fn effective_blocks_oracle_for_test(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<FenceRecheck>> {
        self.store
            .with_read(|conn| {
                if native(conn)? {
                    native_reference::effective(conn, now)
                } else {
                    legacy::read_effective_at(conn, now)
                }
            })
            .map_err(err)
    }

    /// Compare reference SQL and the actual adapter in one read snapshot.
    /// The reference reader never calls the adapter whose output is checked.
    pub fn block_parity(&self, now: i64) -> RouteStateResult<BlockParity> {
        self.store.with_read(|conn| {
            let current=native(conn)?;
            let started=std::time::Instant::now();
            let loaded=if current {load_blocks(conn,now)?} else {legacy::load_blocks(conn,now)?};
            let load_time=started.elapsed();
            let rows=if current {native_reference::effective(conn,now)?} else {legacy::read_effective_at(conn,now)?};
            let mut report=BlockParity {load_time,active:loaded.blocks.len(),cleared_evidence:loaded.cleared_evidence.len(),
                identities:0,combinations_per_identity:0,function_comparisons:0,differences:Vec::new()};
            let canonical=|rows:Vec<FenceRecheck>|->std::collections::BTreeMap<_,_> {
                rows.into_iter().map(|mut row| {
                    row.cleared_models.sort();
                    ((row.kind.clone(),row.origin_key.clone()),row)
                }).collect()
            };
            let expected=canonical(rows);
            let actual=canonical(loaded.blocks.iter().map(FenceRecheck::from).collect());
            for key in expected.keys().chain(actual.keys()).collect::<std::collections::BTreeSet<_>>() {
                if expected.get(key)!=actual.get(key) {
                    report.differences.push(format!("row {key:?}: SQL={:?}, block={:?}",expected.get(key),actual.get(key)));
                }
            }
            let generations=if current {native_reference::generations(conn)?} else {
                conn.prepare("SELECT kind,origin_key,evidence,claim_generation FROM fence_recheck")?
                    .query_map([],|row|Ok(((row.get(0)?,row.get(1)?),(row.get(2)?,row.get(3)?))))?
                    .collect::<rusqlite::Result<std::collections::BTreeMap<(String,String),(String,i64)>>>()?
            };
            for block in &loaded.blocks {
                if let Some((evidence,generation))=generations.get(&(block.kind.clone(),block.origin_key.clone())) {
                    if evidence==&block.evidence && generation!=&block.claim_generation {
                        report.differences.push(format!("claim generation {} {}: clock={generation}, block={}",block.kind,block.origin_key,block.claim_generation));
                    }
                }
            }
            compare_block_functions(conn,now,&expected,&loaded.blocks,&mut report)?;
            Ok(report)
        }).map_err(err)
    }
}

impl SqliteRouteStateStore {
    /// Copy a migrated synthetic fixture through one captured command. Preserve
    /// its logical keys and columns, but allocate destination rowids because
    /// this destination may already contain the benchmark's unrelated rows.
    pub fn import_native_block_fixture_for_test(&self, source: &Self) -> RouteStateResult<()> {
        let tables = source
            .store
            .with_read(|conn| {
                let mut tables = Vec::new();
                for table in crate::block_state::BLOCK_TABLES {
                    let columns = conn
                        .prepare(
                            "SELECT name FROM pragma_table_xinfo(?1) WHERE hidden=0 ORDER BY cid",
                        )?
                        .query_map([table], |row| row.get::<_, String>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    let mut statement = conn.prepare(&format!(
                        "SELECT {} FROM {table} ORDER BY rowid",
                        columns.join(",")
                    ))?;
                    let rows = statement
                        .query_map([], |row| {
                            (0..columns.len())
                                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    tables.push((table.to_string(), columns, rows));
                }
                Ok(tables)
            })
            .map_err(err)?;
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            for (table, columns, rows) in &tables {
                let params = (1..=columns.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut insert = tx
                    .conn
                    .prepare(&format!(
                        "INSERT OR REPLACE INTO {table}({}) VALUES ({params})",
                        columns.join(",")
                    ))
                    .map_err(err)?;
                for row in rows {
                    insert
                        .execute(rusqlite::params_from_iter(row.iter()))
                        .map_err(err)?;
                }
            }
            Ok(())
        })
    }
}
