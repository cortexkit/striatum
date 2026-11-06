//! Durable evidence for retiring a request shape, including idle instances.

use std::collections::BTreeMap;

use rusqlite::{params, Connection};

use super::{err, SqliteRouteStateStore};
use prefrontal_routing_core::route_state_store::RouteStateResult;

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
        self.store.with_conn(|conn: &Connection| {
            conn.execute(
                "INSERT INTO routing_call_count (method, shape, principal, calls, last_called_at_ms) VALUES (?1, ?2, ?3, 1, ?4)
                 ON CONFLICT(method, shape, principal) DO UPDATE SET calls=calls+1,
                 last_called_at_ms=MAX(last_called_at_ms,excluded.last_called_at_ms)",
                params![method, shape, principal, crate::now_ms()],
            )?;
            Ok(())
        }).map_err(err)
    }

    pub fn routing_call_counts(&self) -> RouteStateResult<RoutingCallCounts> {
        self.store.with_conn(|conn: &Connection| {
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
        }).map_err(err)
    }
}
