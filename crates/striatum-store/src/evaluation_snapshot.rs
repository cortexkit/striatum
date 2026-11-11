//! Private account evidence for an operator evaluation. Never copies routing's
//! decision/usage history and never opens a writer on the source database.

use super::*;

// Durable stores keep the shared library's lease and pooled reads unchanged.
// A projection needs neither a lease file nor a migration/seed pass.
pub(super) enum ConnectionStore {
    Live(SqliteStore),
    Projection(Mutex<Connection>),
}

impl ConnectionStore {
    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        match self {
            Self::Live(store) => store.with_conn(f),
            Self::Projection(connection) => {
                f(&connection.lock().unwrap_or_else(|error| error.into_inner()))
                    .map_err(|error| cortexkit_store::StoreError::Backend(error.to_string()))
            }
        }
    }

    pub fn with_read<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, cortexkit_store::StoreError> {
        match self {
            Self::Live(store) => store.with_read(f),
            Self::Projection(_) => self.with_conn(f),
        }
    }

    pub fn epoch(&self) -> u64 {
        match self {
            Self::Live(store) => store.epoch(),
            Self::Projection(_) => 0,
        }
    }
}

impl SqliteRouteStateStore {
    /// Copy only the evidence account preparation reads. Reconciliation may
    /// mutate this disposable in-memory store, but cannot reach the source.
    /// The pooled, read-only transaction ends before constructing the copy.
    pub fn account_evaluation_snapshot(&self, now_ms: i64) -> RouteStateResult<Self> {
        let tables = [
            ("model_facts", "1".to_string()),
            ("fence_recheck", "1".to_string()),
            ("fence_recheck_route_clear", "1".to_string()),
            ("fence_success_pool", "1".to_string()),
            ("fence_probe_authorization", "1".to_string()),
            ("fence_retired_origin", "1".to_string()),
            ("account_non_quota_route_fence", "1".to_string()),
            ("account_non_quota_size_refusal", "1".to_string()),
            ("account_transient_backoff", "1".to_string()),
            ("account_principal_exclusion", "1".to_string()),
            ("model_route_state", "1".to_string()),
            ("provider_cooldown_state", "1".to_string()),
            ("credential_entitlement_fences", "1".to_string()),
            ("account_eligibility", "1".to_string()),
            ("account_size_refusal", "1".to_string()),
            ("account_route_fence", "1".to_string()),
            ("account_fence", "cleared_at IS NULL".to_string()),
            (
                "account_admission",
                "state IN ('reserved','started')".to_string(),
            ),
            (
                "account_continue",
                "used_at_ms IS NULL AND released_at_ms IS NULL".to_string(),
            ),
            (
                "account_observation",
                format!(
                    "bound=1 AND kind='transient_backoff' AND received_at>{}",
                    now_ms.saturating_sub(24 * 60 * 60_000)
                ),
            ),
            ("pool_key_state", "1".to_string()),
            (
                "pool_key_lease",
                format!("released_at IS NULL AND valid_until>{now_ms}"),
            ),
        ];
        let (copied, origin_view, fence_triggers) = self
            .store
            .with_read(|source| {
                let mut copied = Vec::new();
                for (table, predicate) in &tables {
                    let schema = source.query_row(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                        [table],
                        |row| row.get::<_, String>(0),
                    );
                    let schema = match schema {
                        Ok(schema) => schema,
                        Err(rusqlite::Error::QueryReturnedNoRows) => continue,
                        Err(error) => return Err(error),
                    };
                    let mut query =
                        source.prepare(&format!("SELECT * FROM {table} WHERE {predicate}"))?;
                    let columns = query.column_count();
                    let rows = query
                        .query_map([], |row| {
                            (0..columns)
                                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    copied.push((*table, schema, columns, rows));
                }
                let origin_view = source
                    .query_row(
                        "SELECT sql FROM sqlite_schema WHERE type='view' AND name='fence_origin'",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                // Fence writers rely on these triggers to retire enforcement and
                // keep route provenance; without them the copy diverges from the
                // live outcome. Both reference only tables copied above.
                let mut statement = source.prepare(
                    "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name IN
                     ('fence_retire_enforcement','model_route_state_inherit_fence_scope')",
                )?;
                let fence_triggers = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((copied, origin_view, fence_triggers))
            })
            .map_err(err)?;
        let store =
            ConnectionStore::Projection(Mutex::new(Connection::open_in_memory().map_err(err)?));
        store
            .with_conn(|destination| {
                destination.pragma_update(None, "foreign_keys", false)?;
                let tx = crate::write_transaction(destination)?;
                for (table, schema, columns, rows) in copied {
                    tx.execute_batch(&schema)?;
                    let placeholders = (0..columns).map(|_| "?").collect::<Vec<_>>().join(",");
                    let mut insert =
                        tx.prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))?;
                    for row in rows {
                        insert.execute(rusqlite::params_from_iter(row))?;
                    }
                }
                if let Some(view) = origin_view {
                    tx.execute_batch(&view)?;
                }
                for trigger in fence_triggers {
                    tx.execute_batch(&trigger)?;
                }
                tx.commit()
            })
            .map_err(err)?;
        Ok(Self::from_inner(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(store: &SqliteRouteStateStore, sql: &str) -> i64 {
        store
            .store
            .with_conn(|conn| conn.query_row(sql, [], |row| row.get(0)))
            .unwrap()
    }

    /// A snapshot must run the same fence synchronization as the live store:
    /// retiring expired evidence writes the archive table, whose trigger lifts
    /// the enforcement row. A missing table or trigger fails selection outright.
    #[test]
    fn snapshot_carries_fence_archive_authorizations_and_triggers() {
        let scratch = cortexkit_test_support::ScratchDir::new("evaluation-snapshot-fences");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "evaluation-snapshot-fences",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let now = crate::now_ms();
        store
            .store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO provider_cooldown_state(provider_key,cooldown_until,updated_at,wall_observed_at)
                     VALUES ('legacy',?1,100,100)",
                    [now - 10_000],
                )?;
                conn.execute(
                    "INSERT INTO fence_probe_authorization VALUES ('owner','provider','[\"other\"]','e',1,2,3)",
                    [],
                )
            })
            .unwrap();

        let snapshot = store.account_evaluation_snapshot(now).unwrap();
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM fence_probe_authorization"),
            1
        );
        snapshot
            .account_transaction(|tx| {
                assert!(tx.claim_fence_rechecks(&[], now)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            count(&snapshot, "SELECT COUNT(*) FROM fence_retired_origin"),
            1
        );
        assert_eq!(
            count(
                &snapshot,
                "SELECT COUNT(*) FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL"
            ),
            0,
            "the retirement trigger lifts the copied enforcement row"
        );
        assert_eq!(
            count(
                &store,
                "SELECT COUNT(*) FROM provider_cooldown_state WHERE cooldown_until IS NOT NULL"
            ),
            1,
            "the source store is untouched"
        );
    }
}
