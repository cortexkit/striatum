//! Bounded retention deletes for the routing store's append-only logs:
//! account-selection history, runner observations and their event keys,
//! per-message usage, and the per-head failure copy of the decision ledger.
//! (The route-decision ledger, `model_route_decisions`, is pruned through
//! `RouteStateStore::delete_decisions_older_than`.)
//!
//! Every method here deletes at most `limit` rows in one short transaction
//! and reports how much it did, so the caller can run several small batches
//! with pauses between them, off the request path, instead of one long
//! delete that holds the store's single connection. Deleting rows does not
//! shrink the database file: SQLite reuses the freed pages for new rows, and
//! only a VACUUM gives the space back to the operating system.
//!
//! Account-selection history, and what keeps a row:
//! - Shadow decisions (`mode = 'shadow'`) came from the retired shadow mode,
//!   which ran the allocator for nobody. Nothing writes or reads them any
//!   more, so [`SqliteRouteStateStore::purge_shadow_decisions`] deletes them
//!   all, oldest first. A long-running install holds over a million of them;
//!   one delete of all of them took about 26 seconds on such a store, which is
//!   why they go in batches rather than inside a migration.
//! - Every other account row is served history, kept for the configured
//!   window after its last activity and then deleted leaf first: an
//!   admission once every time it carries is past the window; an affinity row
//!   (with every pin generation of its conversation) once its idle expiry and
//!   lease are past the window and no remaining admission names one of those
//!   pins; a hold once it is past the window; a serve decision once it is past
//!   the window and no pin, admission or hold references it. A conversation's
//!   pins go together with its affinity row because the next pin for that
//!   conversation starts again at generation 1, which must not collide with a
//!   stored older generation.

use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};

use super::{err, SqliteRouteStateStore, DECISION_MODE_SERVE};

/// `account_decision.mode` of the retired shadow decisions.
const RETIRED_SHADOW_MODE: &str = "shadow";

/// What one affinity retention batch deleted: conversation affinity rows,
/// and the pin generations that went with them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AffinityRetentionBatch {
    pub affinities: usize,
    pub pins: usize,
}

/// A decision a pin, admission or hold still names is kept, so the history
/// stays explainable while anything in the store points at it. (These names
/// carry no foreign key: a backup may restore `account_decision` empty, so the
/// referencing rows must not depend on it.)
const DECISION_UNREFERENCED: &str = "NOT EXISTS (SELECT 1 FROM account_pin p \
       WHERE p.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM account_admission a \
       WHERE a.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM account_hold h \
       WHERE h.decision_id = account_decision.decision_id)";

fn limit_param(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

impl SqliteRouteStateStore {
    /// Delete at most `limit` retired shadow decisions, oldest first. A shadow
    /// row never had a pin, admission or hold; one that somehow does is kept
    /// rather than failing the batch.
    pub fn purge_shadow_decisions(&self, limit: usize) -> RouteStateResult<usize> {
        self.delete_unreferenced_decisions(RETIRED_SHADOW_MODE, i64::MAX, limit)
    }

    /// Delete at most `limit` serve decisions created before `created_before`
    /// (Unix milliseconds), oldest first, skipping any a pin, admission or
    /// hold still references.
    pub fn prune_serve_decisions(
        &self,
        created_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.delete_unreferenced_decisions(DECISION_MODE_SERVE, created_before, limit)
    }

    fn delete_unreferenced_decisions(
        &self,
        mode: &str,
        created_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    &format!(
                        "DELETE FROM account_decision WHERE rowid IN ( \
                           SELECT rowid FROM account_decision INDEXED BY account_decision_by_mode_time \
                           WHERE mode = ?1 AND created_at < ?2 AND {DECISION_UNREFERENCED} \
                           ORDER BY created_at LIMIT ?3)"
                    ),
                    params![mode, created_before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    /// Delete at most `limit` admissions whose every time (commit, validity,
    /// lease, start, release and finish) is before `before` (Unix
    /// milliseconds), oldest first. Demand only counts an admission committed
    /// after its account's latest reading, so an admission this old charges
    /// nothing a fresh reading could still need.
    pub fn prune_account_admissions(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM account_admission WHERE rowid IN ( \
                       SELECT rowid FROM account_admission \
                         INDEXED BY account_admission_by_committed \
                       WHERE committed_at < ?1 \
                         AND admission_valid_until < ?1 AND lease_valid_until < ?1 \
                         AND COALESCE(started_at, 0) < ?1 \
                         AND COALESCE(released_at, 0) < ?1 \
                         AND COALESCE(finished_at, 0) < ?1 \
                       ORDER BY committed_at LIMIT ?2)",
                    params![before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    /// Delete at most `limit` holds created before `before` (Unix
    /// milliseconds), oldest first. A hold is a retry answer that lapses
    /// within minutes; only the decision row it names keeps any meaning.
    pub fn prune_account_holds(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM account_hold WHERE rowid IN ( \
                       SELECT rowid FROM account_hold INDEXED BY account_hold_by_created \
                       WHERE created_at < ?1 ORDER BY created_at LIMIT ?2)",
                    params![before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    /// Delete at most `limit` conversation affinity rows whose idle expiry and
    /// lease are both before `before` (Unix milliseconds), together with every
    /// pin generation of the same conversation, in one transaction. A
    /// conversation is kept while any stored admission still names one of its
    /// pins; [`Self::prune_account_admissions`] removes those first.
    pub fn prune_account_affinities(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<AffinityRetentionBatch> {
        if limit == 0 {
            return Ok(AffinityRetentionBatch::default());
        }
        self.store
            .with_conn(|conn: &Connection| {
                let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
                let keys: Vec<(String, String, String)> = {
                    let mut statement = tx.prepare(
                        "SELECT principal, conversation_hash, provider FROM account_affinity \
                           INDEXED BY account_affinity_by_expiry \
                         WHERE expires_at < ?1 AND COALESCE(lease_valid_until, 0) < ?1 \
                           AND NOT EXISTS (SELECT 1 FROM account_pin p \
                             JOIN account_admission a ON a.pin_id = p.pin_id \
                             WHERE p.principal = account_affinity.principal \
                               AND p.conversation_hash = account_affinity.conversation_hash \
                               AND p.provider = account_affinity.provider) \
                         ORDER BY expires_at LIMIT ?2",
                    )?;
                    let rows = statement.query_map(params![before, limit_param(limit)], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?;
                    rows.collect::<rusqlite::Result<_>>()?
                };
                let mut batch = AffinityRetentionBatch::default();
                {
                    let mut delete_pins = tx.prepare(
                        "DELETE FROM account_pin \
                         WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3",
                    )?;
                    let mut delete_affinity = tx.prepare(
                        "DELETE FROM account_affinity \
                         WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3",
                    )?;
                    for (principal, conversation_hash, provider) in &keys {
                        batch.pins +=
                            delete_pins.execute(params![principal, conversation_hash, provider])?;
                        batch.affinities += delete_affinity.execute(params![
                            principal,
                            conversation_hash,
                            provider
                        ])?;
                    }
                }
                tx.commit()?;
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

    /// Delete at most `limit` runner-event keys (`account_observation_event`)
    /// received before `received_before` (Unix milliseconds), oldest first.
    /// A key only has to outlive a runner's retry of the same event; it is
    /// pruned on its own window, independently of the observation log.
    pub fn prune_observation_events(
        &self,
        received_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM account_observation_event WHERE rowid IN ( \
                       SELECT rowid FROM account_observation_event \
                         INDEXED BY account_observation_event_by_received \
                       WHERE received_at < ?1 ORDER BY received_at LIMIT ?2)",
                    params![received_before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    /// Delete at most `limit` rows of `model_route_parent_failures` whose
    /// outcome is before `outcome_before` (Unix milliseconds). A trigger
    /// removes a row together with its decision, but a decision ledger
    /// restored empty from a backup leaves rows no ledger delete will ever
    /// reach; this keeps the table bounded by the decision window either way.
    pub fn prune_parent_failures(
        &self,
        outcome_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM model_route_parent_failures WHERE rowid IN ( \
                       SELECT rowid FROM model_route_parent_failures \
                       WHERE COALESCE(outcome_at, 0) < ?1 LIMIT ?2)",
                    params![outcome_before, limit_param(limit)],
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
