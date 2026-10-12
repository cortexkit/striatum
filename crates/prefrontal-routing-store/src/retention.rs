//! Bounded retention deletes for the routing store's append-only logs:
//! account decisions, runner observations and per-message usage. (The
//! route-decision ledger, `model_route_decisions`, is pruned through
//! `RouteStateStore::delete_decisions_older_than`.)
//!
//! Every method here deletes at most `limit` rows in one short transaction
//! and reports how much it did, so the caller can run several small batches
//! with pauses between them, off the request path, instead of one long
//! delete that holds the store's single connection. Deleting rows does not
//! shrink the database file: SQLite reuses the freed pages for new rows, and
//! only a VACUUM gives the space back to the operating system.
//!
//! Which account decisions are kept, and why:
//! - Serve decisions (`mode = 'serve'`) are never deleted here. Each served
//!   admit or hold is referenced by an `account_admission`, `account_pin` or
//!   `account_hold` row, nothing deletes those, and foreign keys are on. The
//!   few serve refusals are kept with them as the served audit trail.
//! - Shadow decisions are diagnostics nothing answers a caller with. The
//!   only code readers are the timer heartbeat (the newest row per trigger,
//!   provider and family) and `route.account_shadow_status` (the newest few
//!   hundred rows). Routine shadow rows (admits, and the fixed
//!   `account_selection_unsupported` refusal for a provider routing selects
//!   no accounts for) are kept for the routine window. Holds and every other
//!   refusal are kept for the longer notable window, because they are what
//!   someone investigating a refusal looks for.

use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};

use super::{err, SqliteRouteStateStore, DECISION_MODE_SHADOW};

/// The refusal routing records for a provider it selects no accounts for.
/// Its shape is identical every time, so it is retained like an admit.
pub const ROUTINE_REFUSAL_CODE: &str = "account_selection_unsupported";

/// What one retention batch did. `examined` counts the rows the batch looked
/// at, deleted or kept; a batch that examined fewer rows than its limit found
/// nothing more past the window, so the caller can stop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionBatch {
    pub deleted: usize,
    pub examined: usize,
}

/// Where the account-decision scan of the band between the two windows
/// resumes, as (created_at, rowid) of the last row it examined.
///
/// Rows older than both windows are deleted outright. Rows in the band
/// between the two cutoffs are deleted only when their own class's window
/// has passed; the others (the longer-window class) are kept and will be
/// deleted once they fall below both cutoffs. Without a cursor every batch
/// would re-read those kept rows from the start of the band; with it, each
/// band row is examined once. Starting from the default re-examines the band
/// once, which is always safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionRetentionCursor {
    created_at: i64,
    rowid: i64,
}

impl Default for DecisionRetentionCursor {
    fn default() -> Self {
        Self {
            created_at: i64::MIN,
            rowid: i64::MIN,
        }
    }
}

/// True when a decision is retained for the routine window rather than the
/// notable one.
pub fn is_routine_decision(outcome: &str, code: &str) -> bool {
    outcome == "admit" || code == ROUTINE_REFUSAL_CODE
}

/// A decision still referenced by a pin, admission or hold cannot be deleted
/// while foreign keys are on; skipping it keeps one such row from failing a
/// whole batch.
const DECISION_UNREFERENCED: &str = "NOT EXISTS (SELECT 1 FROM account_pin p \
       WHERE p.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM account_admission a \
       WHERE a.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM account_hold h \
       WHERE h.decision_id = account_decision.decision_id)";

fn limit_param(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

fn prune_account_decisions_tx(
    tx: &Connection,
    routine_before: i64,
    notable_before: i64,
    cursor: &mut DecisionRetentionCursor,
    limit: usize,
) -> rusqlite::Result<RetentionBatch> {
    let older = routine_before.min(notable_before);
    let newer = routine_before.max(notable_before);

    // Every shadow row older than both cutoffs is past its window whatever
    // its class.
    let deleted_old = tx.execute(
        &format!(
            "DELETE FROM account_decision WHERE rowid IN ( \
               SELECT rowid FROM account_decision INDEXED BY account_decision_by_mode_time \
               WHERE mode = ?1 AND created_at < ?2 AND {DECISION_UNREFERENCED} \
               ORDER BY created_at LIMIT ?3)"
        ),
        params![DECISION_MODE_SHADOW, older, limit_param(limit)],
    )?;
    let mut batch = RetentionBatch {
        deleted: deleted_old,
        examined: deleted_old,
    };
    let remaining = limit.saturating_sub(deleted_old);
    if remaining == 0 || older == newer {
        return Ok(batch);
    }

    // Rows created between the two cutoffs: only the class with the shorter
    // window (routine, with the default windows) is past its window here; the
    // other class is skipped and deleted later by the statement above.
    let band_start = DecisionRetentionCursor {
        created_at: older,
        rowid: i64::MIN,
    };
    if (cursor.created_at, cursor.rowid) < (band_start.created_at, band_start.rowid) {
        *cursor = band_start;
    }
    let candidates: Vec<(i64, i64, String, String)> = {
        let mut statement = tx.prepare(
            "SELECT rowid, created_at, outcome, code \
             FROM account_decision INDEXED BY account_decision_by_mode_time \
             WHERE mode = ?1 AND created_at >= ?2 AND (created_at > ?2 OR rowid > ?3) \
               AND created_at < ?4 \
             ORDER BY created_at, rowid LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![
                DECISION_MODE_SHADOW,
                cursor.created_at,
                cursor.rowid,
                newer,
                limit_param(remaining)
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let mut delete = tx.prepare(&format!(
        "DELETE FROM account_decision WHERE rowid = ?1 AND {DECISION_UNREFERENCED}"
    ))?;
    for (rowid, created_at, outcome, code) in candidates {
        batch.examined += 1;
        *cursor = DecisionRetentionCursor { created_at, rowid };
        let before = if is_routine_decision(&outcome, &code) {
            routine_before
        } else {
            notable_before
        };
        if created_at < before {
            batch.deleted += delete.execute(params![rowid])?;
        }
    }
    Ok(batch)
}

impl SqliteRouteStateStore {
    /// Delete one bounded batch of shadow account decisions past their
    /// window: routine rows created before `routine_before`, holds and other
    /// refusals created before `notable_before` (both Unix milliseconds).
    /// Examines at most `limit` rows in one transaction. `cursor` carries the
    /// scan position between batches; see [`DecisionRetentionCursor`].
    pub fn prune_account_decisions(
        &self,
        routine_before: i64,
        notable_before: i64,
        cursor: &mut DecisionRetentionCursor,
        limit: usize,
    ) -> RouteStateResult<RetentionBatch> {
        if limit == 0 {
            return Ok(RetentionBatch::default());
        }
        self.store
            .with_conn(|conn: &Connection| {
                let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
                // Scan on a copy so a rolled-back batch leaves the cursor where
                // the last committed batch put it.
                let mut next = *cursor;
                let batch = prune_account_decisions_tx(
                    &tx,
                    routine_before,
                    notable_before,
                    &mut next,
                    limit,
                )?;
                tx.commit()?;
                *cursor = next;
                Ok(batch)
            })
            .map_err(err)
    }

    /// Delete at most `limit` runner observations received before
    /// `received_before` (Unix milliseconds), oldest first. An observation a
    /// fence or exclusion still names is kept.
    pub fn prune_account_observations(
        &self,
        received_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM account_observation WHERE rowid IN ( \
                       SELECT rowid FROM account_observation \
                         INDEXED BY account_observation_by_received \
                       WHERE received_at < ?1 \
                         AND NOT EXISTS (SELECT 1 FROM account_fence f \
                           WHERE f.observation_id = account_observation.observation_id) \
                         AND NOT EXISTS (SELECT 1 FROM account_principal_exclusion x \
                           WHERE x.observation_id = account_observation.observation_id) \
                       ORDER BY received_at LIMIT ?2)",
                    params![received_before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    /// Delete at most `limit` per-message usage rows observed before
    /// `observed_before` (Unix milliseconds), oldest first.
    pub fn prune_route_usage(&self, observed_before: i64, limit: usize) -> RouteStateResult<usize> {
        let deleted = self
            .store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM model_route_usage WHERE rowid IN ( \
                       SELECT rowid FROM model_route_usage INDEXED BY model_route_usage_observed_idx \
                       WHERE observed_at < ?1 ORDER BY observed_at LIMIT ?2)",
                    params![observed_before, limit_param(limit)],
                )
            })
            .map_err(err)?;
        if deleted > 0 {
            // Cached class vectors are keyed by this generation; moving it
            // makes them recompute from the rows that remain.
            self.usage_generation
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        Ok(deleted)
    }
}
