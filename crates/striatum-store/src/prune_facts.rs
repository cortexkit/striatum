//! Plan catalog cleanup on a reader so a no-op poll never acquires the writer.

use std::collections::HashSet;

use rusqlite::{params, Connection};
use striatum_core::RouteStateResult;

#[cfg(test)]
use crate::transactions::write_transaction;
use crate::{err, SqliteRouteStateStore};

struct PrunePlan {
    unserved: Vec<String>,
    candidates: Vec<String>,
    last_decision_rowid: i64,
}

#[cfg(test)]
mod tests;

fn plan_unserved_prune(connection: &Connection) -> rusqlite::Result<PrunePlan> {
    // Pooled readers already own a read transaction. In-memory evaluation
    // projections use the writer connection and need their own read snapshot.
    let tx = connection
        .is_autocommit()
        .then(|| connection.unchecked_transaction())
        .transpose()?;
    let since = connection.query_row(
        "SELECT CAST(strftime('%s', 'now') AS INTEGER) * 1000",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let last_decision_rowid = connection.query_row(
        "SELECT COALESCE(MAX(rowid), 0) FROM model_route_decisions",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let absent = {
        let mut statement = connection.prepare(
            "SELECT route_key, source FROM model_facts WHERE fusiform_present = 0 ORDER BY route_key",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let unserved = absent.iter().map(|(route, _)| route.clone()).collect();
    let mut candidates: Vec<_> = absent
        .into_iter()
        .filter(|(_, source)| source != "override")
        .map(|(route, _)| route)
        .collect();
    if !candidates.is_empty() {
        // Scan the time range once, not once per catalog row. Decision writes
        // stay index-free by route; only the existing retention index is needed.
        let mut statement = connection.prepare(
            "SELECT DISTINCT selected_route_key FROM model_route_decisions
             INDEXED BY model_route_decisions_created_at_idx
             WHERE created_at >= ?1 AND selected_route_key IS NOT NULL",
        )?;
        let picked = statement
            .query_map([since - 30 * 24 * 60 * 60 * 1000], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        candidates.retain(|route| !picked.contains(route));
    }
    if let Some(tx) = tx {
        tx.commit()?;
    }
    Ok(PrunePlan {
        unserved,
        candidates,
        last_decision_rowid,
    })
}

impl SqliteRouteStateStore {
    /// List every absent route before pruning unpicked non-overrides whose last
    /// selection is older than 30 days. Decision history is deliberately kept.
    pub fn prune_unserved_unpicked_facts(&self) -> RouteStateResult<Vec<String>> {
        self.prune_unserved_after_plan(|| {})
    }

    fn prune_unserved_after_plan(
        &self,
        after_plan: impl FnOnce(),
    ) -> RouteStateResult<Vec<String>> {
        let plan = self.store.with_read(plan_unserved_prune).map_err(err)?;
        after_plan();
        // Small transactions limit how long the writer is held when many
        // retired routes disappear together. Most polls need no writer at all.
        for batch in plan.candidates.chunks(16) {
            let batch = batch.to_vec();
            let watermark = plan.last_decision_rowid;
            self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
                tx.prune_unserved_batch(&batch, watermark)
            })?;
        }
        Ok(plan.unserved)
    }
}

impl crate::AccountTx<'_> {
    fn prune_unserved_batch(&self, batch: &[String], watermark: i64) -> RouteStateResult<()> {
        self.command_sql(|tx| {
            for route in batch {
                // Timestamps are assigned before writer acquisition, so
                // a queued selection may predate the reader snapshot.
                // Rowids are assigned at insertion; retention removes old
                // rows, not the recent high-water mark. Scan only the tail.
                let eligible = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM model_facts
                         WHERE route_key=?1 AND fusiform_present=0 AND source<>'override')
                         AND NOT EXISTS(SELECT 1 FROM model_route_decisions
                          WHERE rowid > ?2 AND selected_route_key=?1)",
                    params![route, watermark],
                    |row| row.get::<_, bool>(0),
                )?;
                if eligible {
                    // Mark the route's `route` and `failure` blocks absent before
                    // deleting `model_route_state` so rediscovery cannot revive
                    // the refusals. `block_clock_drop` resets their claim state.
                    let key = serde_json::json!([route]).to_string();
                    for kind in ["route", "failure"] {
                        crate::block_storage::remove_source(
                            tx,
                            kind,
                            &key,
                            crate::now_ms(),
                            "catalogue prune",
                        )?;
                    }
                    tx.execute("DELETE FROM model_route_state WHERE route_key=?1", [route])?;
                    tx.execute("DELETE FROM model_facts WHERE route_key=?1", [route])?;
                }
            }
            Ok(())
        })
        .map_err(err)
    }
}
