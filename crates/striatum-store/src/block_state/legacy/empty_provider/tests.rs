//! The schema-93 empty-provider check retained against the legacy row oracle.
use super::*;
use crate::{AccountCommandKind, RouteStateResult, SqliteRouteStateStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const NOW: i64 = 1_800_000_000_000;

struct Owner {
    state: Mutex<BlockState>,
    stale: Mutex<Vec<BlockState>>,
    loads: AtomicU64,
}

struct Store {
    inner: SqliteRouteStateStore,
    owner: Arc<Owner>,
}

struct Rig {
    store: Store,
    _scratch: cortexkit_test_support::ScratchDir,
}

struct OracleTx<'a> {
    conn: &'a Connection,
    blocks: &'a BlockState,
}

impl OracleTx<'_> {
    fn check_decision_inputs(&self, providers: &[&str]) -> RouteStateResult<()> {
        for provider in providers {
            self.blocks
                .check_provider_rows(self.conn, provider)
                .map_err(crate::err)?;
        }
        Ok(())
    }
}

impl Store {
    fn with_reader_for_test<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> RouteStateResult<T> {
        self.inner.with_reader_for_test(f)
    }

    fn account_command(
        &self,
        _kind: AccountCommandKind,
        f: impl Fn(&OracleTx<'_>) -> RouteStateResult<()> + Send + 'static,
    ) -> RouteStateResult<()> {
        let owner = Arc::clone(&self.owner);
        self.inner.with_writer_for_test(move |conn| {
            let stale = owner.stale.lock().unwrap().pop();
            let mut blocks = if let Some(stale) = stale {
                owner.loads.fetch_add(1, Ordering::Relaxed);
                stale
            } else {
                owner.state.lock().unwrap().clone()
            };
            let mut result = f(&OracleTx {
                conn,
                blocks: &blocks,
            });
            if result
                .as_ref()
                .is_err_and(|error| error.to_string().contains(crate::STATE_DIVERGED))
            {
                owner.loads.fetch_add(1, Ordering::Relaxed);
                blocks = BlockState::load(conn)?;
                result = f(&OracleTx {
                    conn,
                    blocks: &blocks,
                });
            }
            result.map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            *owner.state.lock().unwrap() = blocks;
            Ok(())
        })
    }
}

fn rig(name: &str) -> Rig {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let inner = SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(name, scratch.path().join("state.db").to_string_lossy()),
        93,
    )
    .unwrap();
    let state = inner.with_reader_for_test(BlockState::load).unwrap();
    let owner = Arc::new(Owner {
        state: Mutex::new(state),
        stale: Mutex::new(Vec::new()),
        loads: AtomicU64::new(0),
    });
    Rig {
        store: Store { inner, owner },
        _scratch: scratch,
    }
}

impl Rig {
    fn run(&self, f: impl Fn(&Connection) -> rusqlite::Result<()> + Send + 'static) {
        let owner = Arc::clone(&self.store.owner);
        self.store
            .inner
            .with_writer_for_test(move |conn| {
                f(conn)?;
                *owner.state.lock().unwrap() = BlockState::load(conn)?;
                Ok(())
            })
            .unwrap();
    }
    fn sql(&self, sql: String) {
        self.run(move |conn| conn.execute_batch(&sql));
    }
    fn published(&self) -> BlockState {
        self.store.owner.state.lock().unwrap().clone()
    }
    fn decide_next_on(&self, stale: BlockState) -> u64 {
        self.store.owner.stale.lock().unwrap().push(stale);
        self.store.owner.loads.load(Ordering::Relaxed)
    }
    fn loads_since(&self, loads: u64) -> u64 {
        self.store.owner.loads.load(Ordering::Relaxed) - loads
    }
    fn check(&self, times: &[i64], context: &str) {
        let memory = self.published();
        let fresh = self
            .store
            .inner
            .with_reader_for_test(BlockState::load)
            .unwrap();
        assert_eq!(memory, fresh, "legacy memory equals fresh rows: {context}");
        for now in times {
            assert_eq!(
                memory.blocks(*now).unwrap().blocks,
                fresh.blocks(*now).unwrap().blocks
            );
        }
    }
}

#[test]
fn only_principal_rows_have_the_empty_provider() {
    let rig = rig("block-state-empty-provider");
    rig.run(|conn| crate::block_parity_fixture::seed_block_parity_fixture(conn, NOW));
    rig.sql(format!(
        "INSERT INTO account_principal_exclusion(principal,admission_id,credential_id,runner_view_digest,roster_view_digest,observation_id,recorded_at)
         VALUES ('principal','admission','credential','runner','roster',NULL,{NOW});"
    ));
    rig.run(fence_rechecks::sync);
    let (view, empty_sources) = rig
        .store
        .with_reader_for_test(|conn| {
            let view = conn
                .prepare("SELECT DISTINCT kind FROM fence_origin WHERE provider=''")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut empty_sources = Vec::new();
            for (table, column) in PROVIDER_COLUMNS {
                let count: i64 = conn.query_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE {column}=''",
                        BLOCK_TABLES[table]
                    ),
                    [],
                    |r| r.get(0),
                )?;
                empty_sources.push((BLOCK_TABLES[table], count));
            }
            Ok((view, empty_sources))
        })
        .unwrap();
    assert_eq!(
        view,
        vec!["principal".to_owned()],
        "only the principal branch projects ''"
    );
    for (table, count) in empty_sources {
        if table == BLOCK_TABLES[CLOCK_TABLE] {
            assert!(count > 0, "the principal clocks have the empty provider");
        } else {
            assert_eq!(count, 0, "{table} holds no row of the empty provider");
        }
    }
    let state = rig.published();
    for (table, _) in PROVIDER_COLUMNS {
        let rows: Vec<_> = state.tables[table].scope_rows("").collect();
        if table == CLOCK_TABLE {
            assert!(!rows.is_empty());
            for (_, held) in rows {
                assert!(
                    matches!(held, Held::Clock(block, _) if block.kind == "principal"),
                    "only principal clocks"
                );
            }
        } else {
            assert!(
                rows.is_empty(),
                "{} holds no empty-provider row",
                BLOCK_TABLES[table]
            );
        }
    }
    let stale = rig.published();
    rig.sql("UPDATE fence_recheck SET evidence=evidence||' ' WHERE kind='principal';".into());
    let loads = rig.decide_next_on(stale);
    rig.store
        .account_command(AccountCommandKind::Maintenance, |tx| {
            tx.check_decision_inputs(&[""])
        })
        .unwrap();
    assert_eq!(
        rig.loads_since(loads),
        2,
        "a stale principal clock is caught"
    );
    rig.check(&[NOW], "after the empty-provider check");
}
