//! Buffered diagnostic counts for retiring a request shape, with durable flushes.

use std::collections::BTreeMap;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use super::{err, SqliteRouteStateStore};
use striatum_core::route_state_store::RouteStateResult;

type CallKey = (String, String, String);
type CallDeltas = BTreeMap<CallKey, (u64, i64)>;

#[derive(Default)]
pub(crate) struct RoutingCallBuffer {
    pending: Mutex<CallDeltas>,
    flushing: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingCallCount {
    pub method: String,
    pub shape: String,
    pub principal: String,
    pub calls: u64,
    pub last_called_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingCallCounts {
    pub started_at_ms: i64,
    pub counts: Vec<RoutingCallCount>,
}

impl RoutingCallCounts {
    /// Every instance must have observed the entire window: the latest start
    /// bounds the shared window, while a call on any instance counts.
    pub fn combine(instances: &[Self]) -> Option<Self> {
        let started_at_ms = instances.iter().map(|s| s.started_at_ms).max()?;
        let mut counts = BTreeMap::<(String, String, String), (u64, i64)>::new();
        for instance in instances {
            for row in &instance.counts {
                let aggregate = counts
                    .entry((row.method.clone(), row.shape.clone(), row.principal.clone()))
                    .or_default();
                aggregate.0 += row.calls;
                aggregate.1 = aggregate.1.max(row.last_called_at_ms);
            }
        }
        Some(Self {
            started_at_ms,
            counts: counts
                .into_iter()
                .map(
                    |((method, shape, principal), (calls, last_called_at_ms))| RoutingCallCount {
                        method,
                        shape,
                        principal,
                        calls,
                        last_called_at_ms,
                    },
                )
                .collect(),
        })
    }
}

impl SqliteRouteStateStore {
    pub fn record_routing_call(
        &self,
        method: &str,
        shape: &str,
        principal: &str,
    ) -> RouteStateResult<()> {
        let mut pending = self.call_counts.pending.lock().map_err(err)?;
        let delta = pending
            .entry((method.into(), shape.into(), principal.into()))
            .or_default();
        delta.0 += 1;
        delta.1 = delta.1.max(crate::now_ms());
        Ok(())
    }

    /// Diagnostics are batched off the request path. A crash may lose the
    /// current batch; a failed flush restores it for the next timer tick.
    pub fn flush_routing_calls(&self) -> RouteStateResult<usize> {
        let _flushing = self.call_counts.flushing.lock().map_err(err)?;
        let deltas = std::mem::take(&mut *self.call_counts.pending.lock().map_err(err)?);
        if deltas.is_empty() {
            return Ok(0);
        }
        let result = self.store.with_conn(|conn: &Connection| {
            let tx = crate::write_transaction(conn)?;
            {
                let mut insert = tx.prepare_cached(
                    "INSERT INTO routing_call_count (method,shape,principal,calls,last_called_at_ms)
                     VALUES (?1,?2,?3,?4,?5) ON CONFLICT(method,shape,principal) DO UPDATE SET
                     calls=calls+excluded.calls, last_called_at_ms=MAX(last_called_at_ms,excluded.last_called_at_ms)",
                )?;
                for ((method,shape,principal),(calls,last)) in &deltas {
                    insert.execute(params![method,shape,principal,calls,last])?;
                }
            }
            tx.commit()
        }).map_err(err);
        if let Err(error) = result {
            let mut pending = self.call_counts.pending.lock().map_err(err)?;
            for (key, (calls, last)) in deltas {
                let delta = pending.entry(key).or_default();
                delta.0 += calls;
                delta.1 = delta.1.max(last);
            }
            return Err(error);
        }
        Ok(deltas.len())
    }

    pub fn routing_call_counts(&self) -> RouteStateResult<RoutingCallCounts> {
        // Keep the persisted read and pending snapshot on one side of a flush,
        // so the same batch cannot appear in both or disappear between them.
        let _flushing = self.call_counts.flushing.lock().map_err(err)?;
        let persisted = self.store.with_conn(|conn: &Connection| {
            let started_at_ms = conn.query_row(
                "SELECT started_at_ms FROM routing_call_window WHERE singleton=1", [], |row| row.get(0),
            )?;
            let mut statement = conn.prepare(
                "SELECT method, shape, principal, calls, last_called_at_ms FROM routing_call_count ORDER BY method, shape, principal",
            )?;
            let counts = statement.query_map([], |row| Ok(RoutingCallCount {
                method: row.get(0)?, shape: row.get(1)?, principal: row.get(2)?, calls: row.get(3)?, last_called_at_ms: row.get(4)?,
            }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(RoutingCallCounts { started_at_ms, counts })
        }).map_err(err)?;
        let counts = self
            .call_counts
            .pending
            .lock()
            .map_err(err)?
            .iter()
            .map(
                |((method, shape, principal), (calls, last_called_at_ms))| RoutingCallCount {
                    method: method.clone(),
                    shape: shape.clone(),
                    principal: principal.clone(),
                    calls: *calls,
                    last_called_at_ms: *last_called_at_ms,
                },
            )
            .collect();
        Ok(RoutingCallCounts::combine(&[
            persisted,
            RoutingCallCounts {
                started_at_ms: i64::MIN,
                counts,
            },
        ])
        .unwrap())
    }
}

impl Drop for SqliteRouteStateStore {
    fn drop(&mut self) {
        if let Err(error) = self.flush_routing_calls() {
            tracing::warn!(%error, "routing call counts could not be flushed on store close");
        }
        if let Some(history) = self.history.as_mut() {
            // Stop queued writes before checkpointing. Drop the read-only
            // attachment before either writable connection goes away.
            drop(self.account_writer.take());
            history.close_reader_and_checkpoint();
            if let Some(maintenance) = self.maintenance.as_mut() {
                let conn = maintenance.get_mut().unwrap_or_else(|p| p.into_inner());
                if let Err(error) = crate::history::checkpoint_on_close(conn) {
                    tracing::warn!(%error, "could not checkpoint routing state on shutdown");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SqliteRouteStateStore {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "call-batching-{}-{}.db",
            std::process::id(),
            crate::short_id()
        ));
        SqliteRouteStateStore::open(&crate::dev_descriptor(
            "call-batching",
            path.to_string_lossy().into_owned(),
        ))
        .unwrap()
    }

    fn persisted(store: &SqliteRouteStateStore) -> u64 {
        store
            .store
            .with_conn(|c| {
                c.query_row(
                    "SELECT COALESCE(sum(calls),0) FROM routing_call_count",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap()
    }

    #[test]
    fn request_call_counts_are_memory_only_until_a_batch_flush() {
        let store = store();
        let changes = store.store.with_conn(|c| Ok(c.total_changes())).unwrap();
        for _ in 0..100 {
            store
                .record_routing_call("route.select", "legacy", "core")
                .unwrap();
        }
        store
            .record_routing_call("route.head_choices", "contract", "app")
            .unwrap();
        assert_eq!(
            store.store.with_conn(|c| Ok(c.total_changes())).unwrap(),
            changes
        );
        assert_eq!(persisted(&store), 0);
        assert_eq!(
            store
                .routing_call_counts()
                .unwrap()
                .counts
                .iter()
                .map(|c| c.calls)
                .sum::<u64>(),
            101
        );
        assert_eq!(store.flush_routing_calls().unwrap(), 2);
        assert_eq!(persisted(&store), 101);
        assert_eq!(
            store.store.with_conn(|c| Ok(c.total_changes())).unwrap(),
            changes + 2
        );
        assert_eq!(store.flush_routing_calls().unwrap(), 0);
        assert_eq!(
            store
                .routing_call_counts()
                .unwrap()
                .counts
                .iter()
                .map(|c| c.calls)
                .sum::<u64>(),
            101
        );
    }

    #[test]
    fn failed_flush_rolls_back_the_batch_and_restores_all_pending_deltas() {
        let store = store();
        store
            .record_routing_call("route.select", "legacy", "one")
            .unwrap();
        store
            .record_routing_call("route.select", "legacy", "two")
            .unwrap();
        store.store.with_conn(|c| c.execute_batch("CREATE TRIGGER fail_call_flush BEFORE INSERT ON routing_call_count WHEN NEW.principal='two' BEGIN SELECT RAISE(ABORT,'injected flush failure'); END;")).unwrap();
        assert!(store.flush_routing_calls().is_err());
        assert_eq!(persisted(&store), 0);
        store
            .record_routing_call("route.select", "legacy", "one")
            .unwrap();
        store
            .store
            .with_conn(|c| c.execute_batch("DROP TRIGGER fail_call_flush"))
            .unwrap();
        assert_eq!(store.flush_routing_calls().unwrap(), 2);
        assert_eq!(persisted(&store), 3);
        assert_eq!(store.flush_routing_calls().unwrap(), 0);
    }

    #[test]
    fn calls_arriving_during_flushes_are_neither_lost_nor_counted_twice() {
        let store = std::sync::Arc::new(store());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let store = std::sync::Arc::clone(&store);
                scope.spawn(move || {
                    for _ in 0..1000 {
                        store
                            .record_routing_call("route.select", "contract", "core")
                            .unwrap();
                    }
                });
            }
            for _ in 0..2 {
                let store = std::sync::Arc::clone(&store);
                scope.spawn(move || {
                    for _ in 0..10 {
                        store.flush_routing_calls().unwrap();
                    }
                });
            }
        });
        assert_eq!(store.routing_call_counts().unwrap().counts[0].calls, 4000);
        store.flush_routing_calls().unwrap();
        assert_eq!(persisted(&store), 4000);
    }
}
