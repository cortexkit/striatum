//! Bounded retention deletes for the routing store's append-only logs:
//! account-selection history, runner observations and their event keys,
//! per-message usage, the per-head failure copy of the decision ledger,
//! routing/v1 replay records, continuation answers, speed samples, call
//! counts, retired fence origins, unscored-model sightings and the quota
//! reading log. (The route-decision ledger, `model_route_decisions`, is pruned
//! through `RouteStateStore::delete_decisions_older_than`, and its scoring
//! detail compacted in `decision_detail.rs`.)
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
//!   (with every pin generation of its conversation) once its idle expiry is
//!   past the module's grace period, its lease has ended, and no remaining
//!   admission or hold names one of those pins; a hold once it is past the
//!   window; an old orphan pin once no conversation affinity, admission or
//!   hold names it; a serve decision once it is past the window and no pin,
//!   admission or hold references it. Previous-pin links are explanatory
//!   metadata, not a chain the allocator follows.

use rusqlite::{params, Connection, Transaction};
use striatum_core::RouteStateResult;

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

pub type AffinityRetentionKey = (String, String, String);

/// Position in the expiry index. Rowid breaks ties without sorting a large
/// group of conversations that happen to expire at the same instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AffinityRetentionPosition {
    pub expires_at: i64,
    pub rowid: i64,
}

/// A bounded reader scan, including progress past rows references still keep.
#[derive(Debug, Clone, Default)]
pub struct AffinityRetentionPlan {
    pub keys: Vec<AffinityRetentionKey>,
    pub examined: usize,
    pub last: Option<AffinityRetentionPosition>,
}

// Used both on the pooled reader and for the writer's keyed recheck. Every
// retained admission protects its pin, including continuation reservations.
const AFFINITY_UNREFERENCED: &str = "NOT EXISTS (SELECT 1 FROM account_pin p \
    JOIN account_admission a ON a.pin_id=p.pin_id \
    WHERE p.principal=f.principal AND p.conversation_hash=f.conversation_hash \
      AND p.provider=f.provider) \
  AND NOT EXISTS (SELECT 1 FROM account_pin p JOIN {holds} h ON h.pin_id=p.pin_id \
    WHERE p.principal=f.principal AND p.conversation_hash=f.conversation_hash \
      AND p.provider=f.provider)";

/// A decision a pin, admission or hold still names is kept, so the history
/// stays explainable while anything in the store points at it. (These names
/// carry no foreign key: a backup may restore `account_decision` empty, so the
/// referencing rows must not depend on it.)
const DECISION_UNREFERENCED: &str = "NOT EXISTS (SELECT 1 FROM account_pin p \
       WHERE p.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM account_admission a \
       WHERE a.decision_id = account_decision.decision_id) \
     AND NOT EXISTS (SELECT 1 FROM {holds} h \
        WHERE h.decision_id = account_decision.decision_id)";

const PIN_UNREFERENCED: &str = "NOT EXISTS (SELECT 1 FROM account_affinity f \
    WHERE f.principal=p.principal AND f.conversation_hash=p.conversation_hash \
      AND f.provider=p.provider) \
  AND NOT EXISTS (SELECT 1 FROM account_admission a WHERE a.pin_id=p.pin_id) \
  AND NOT EXISTS (SELECT 1 FROM {holds} h WHERE h.pin_id=p.pin_id)";

fn limit_param(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// An account admission that still holds a run open: reserved and not yet
/// started, or started and not yet finished.
const LIVE_ADMISSION_STATES: &str = "('reserved', 'started')";

/// What one routing/v1 decision retention batch deleted: decision bindings,
/// and the report records that went with them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct V1DecisionRetentionBatch {
    pub decisions: usize,
    pub reports: usize,
    pub deliveries: usize,
}

impl SqliteRouteStateStore {
    /// Plan history deletion on a pooled reader; the writer rechecks only
    /// these row identities, including any affinity extended since planning.
    pub fn plan_account_admissions(
        &self,
        before: i64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<Vec<i64>> {
        self.store
            .with_read(|conn| {
                let mut statement = conn.prepare(&format!(
                    "WITH sources AS MATERIALIZED ({USED_OFFER_SOURCES}) {}",
                    admission_candidates(false)
                ))?;
                let rows = statement.query_map(
                    params![
                        before,
                        limit_param(limit),
                        now_ms,
                        now_ms.saturating_sub(30 * 60 * 1_000)
                    ],
                    |row| row.get(0),
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }
    /// Plan at most `scan_limit` expiry-index entries on a pooled reader, not
    /// the writer. The caller keeps `last` across passes so protected entries
    /// are examined only once per sweep. A plan never authorizes a delete:
    /// the account command rechecks every candidate against current state.
    pub fn plan_account_affinities(
        &self,
        before: i64,
        lease_before: i64,
        after: Option<AffinityRetentionPosition>,
        scan_limit: usize,
    ) -> RouteStateResult<AffinityRetentionPlan> {
        self.with_history_reader(|conn| {
            let holds = crate::history::hold_source(conn);
            let after = after.unwrap_or(AffinityRetentionPosition {
                expires_at: i64::MIN,
                rowid: i64::MIN,
            });
            let mut statement = conn.prepare(&format!(
                "WITH candidates AS MATERIALIZED ( \
                   SELECT rowid,principal,conversation_hash,provider,expires_at,lease_valid_until \
                   FROM account_affinity INDEXED BY account_affinity_by_expiry \
                   WHERE expires_at < ?1 AND (expires_at,rowid) > (?2,?3) \
                   ORDER BY expires_at,rowid LIMIT ?4) \
                 SELECT f.rowid,f.principal,f.conversation_hash,f.provider,f.expires_at, \
                   COALESCE(f.lease_valid_until,0)<=?5 AND {} \
                 FROM candidates f ORDER BY f.expires_at,f.rowid",
                AFFINITY_UNREFERENCED.replace("{holds}", holds)
            ))?;
            let mut plan = AffinityRetentionPlan::default();
            let mut rows = statement.query(params![
                before,
                after.expires_at,
                after.rowid,
                limit_param(scan_limit),
                lease_before
            ])?;
            while let Some(row) = rows.next()? {
                plan.examined += 1;
                plan.last = Some(AffinityRetentionPosition {
                    expires_at: row.get(4)?,
                    rowid: row.get(0)?,
                });
                if row.get::<_, bool>(5)? {
                    plan.keys.push((row.get(1)?, row.get(2)?, row.get(3)?));
                }
            }
            Ok(plan)
        })
        .map_err(err)
    }

    fn delete_unreferenced_decisions(
        &self,
        mode: &str,
        created_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        let keys = self
            .with_history_reader(|conn| {
                let holds = crate::history::hold_source(conn);
                let mut statement = conn.prepare(&format!(
                "SELECT decision_id FROM account_decision INDEXED BY account_decision_by_mode_time \
                 WHERE mode=?1 AND created_at<?2 AND {} \
                 ORDER BY created_at LIMIT ?3", DECISION_UNREFERENCED.replace("{holds}", holds)
            ))?;
                let rows = statement
                    .query_map(params![mode, created_before, limit_param(limit)], |row| {
                        row.get::<_, String>(0)
                    })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)?;
        if keys.is_empty() {
            return Ok(0);
        }
        let keys = serde_json::to_string(&keys).map_err(err)?;
        self.retention_write(|tx| {
            let holds = crate::history::hold_source(tx);
            tx.execute(
                &format!(
                    "DELETE FROM account_decision \
                         WHERE decision_id IN (SELECT value FROM json_each(?3)) \
                           AND mode = ?1 AND created_at < ?2 AND {}",
                    DECISION_UNREFERENCED.replace("{holds}", holds)
                ),
                params![mode, created_before, keys],
            )
        })
    }

    fn retention_write<T>(
        &self,
        batch: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.store
            .with_conn(|conn: &Connection| {
                let tx = crate::write_transaction(conn)?;
                let result = batch(&tx)?;
                tx.commit()?;
                Ok(result)
            })
            .map_err(err)
    }

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

    /// Delete at most `limit` admissions whose every time (commit, validity,
    /// lease, start, release and finish) is before `before` (Unix
    /// milliseconds), oldest first. Demand only counts an admission committed
    /// after its account's latest reading, so an admission this old charges
    /// nothing a fresh reading could still need. Its window must also be
    /// closed at `now_ms`, and its observations and moved-run reservations
    /// must be old. Used-offer sources outlive the started rows they succeed
    /// so deleting history cannot revive a departed account's count.
    pub fn prune_account_admissions(
        &self,
        before: i64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<usize> {
        self.refuse_state_blind_admission_write("prune_account_admissions")?;
        let keys = self.plan_account_admissions(before, limit, now_ms)?;
        if keys.is_empty() {
            return Ok(0);
        }
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.delete_account_admissions_returning(before, &keys, now_ms)
                .map(|rows| rows.len())
        })
    }

    /// Delete at most `limit` holds created before `before` (Unix
    /// milliseconds), oldest first. A hold is a retry answer that lapses
    /// within minutes; only the decision row it names keeps any meaning.
    pub fn prune_account_holds(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        if let Some(deleted) = self.prune_history(crate::history::Kind::Hold, before, limit)? {
            return Ok(deleted);
        }
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

    /// Delete at most `limit` old orphan pins, oldest first. A conversation
    /// with any affinity keeps its generations for whole-conversation cleanup.
    /// Every retained admission protects its pin (including a continuation's
    /// reservation), as do holds. Pin bodies are not cached in account state.
    pub fn prune_account_pins(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        let keys = self.plan_account_pins(before, limit)?;
        if keys.is_empty() {
            return Ok(0);
        }
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.delete_account_pins(before, &keys)
        })
    }

    /// Find unreferenced history on a pooled reader. The writer checks the
    /// references again by pin identity, since planning does not reserve rows.
    pub fn plan_account_pins(&self, before: i64, limit: usize) -> RouteStateResult<Vec<String>> {
        self.with_history_reader(|conn| {
            let holds = crate::history::hold_source(conn);
            let mut statement = conn.prepare(&format!(
                "SELECT p.pin_id FROM account_pin p INDEXED BY account_pin_by_created \
                 WHERE p.created_at<?1 AND {} ORDER BY p.created_at LIMIT ?2",
                PIN_UNREFERENCED.replace("{holds}", holds)
            ))?;
            let rows =
                statement.query_map(params![before, limit_param(limit)], |row| row.get(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(err)
    }

    /// Delete at most `limit` conversation affinity rows whose idle expiry and
    /// lease are before / at `before` (Unix milliseconds), together with every
    /// pin generation of the same conversation, in one transaction. A
    /// conversation is kept while any stored admission or hold still names
    /// one of its pins; their retention batches remove those first.
    pub fn prune_account_affinities(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<AffinityRetentionBatch> {
        self.refuse_state_blind_admission_write("prune_account_affinities")?;
        let keys = self
            .plan_account_affinities(before, before, None, limit)?
            .keys;
        if keys.is_empty() {
            return Ok(AffinityRetentionBatch::default());
        }
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.delete_account_affinities_returning(&keys, before, before)
                .map(|(batch, _)| batch)
        })
    }

    /// Delete at most `limit` runner observations received before
    /// `received_before` (Unix milliseconds), oldest first. An observation a
    /// fence or exclusion still names is kept, as is a bound backoff whose
    /// deadline is later than the cutoff (even with a zero-day log window).
    /// Bound sign-in evidence stays while its admission is open: a cleared
    /// global fence must not let that run be offered the same credential
    /// again, and the module's account state holds every such row of an open
    /// admission as the run's sign-in exclusions, so it must stay in SQL for
    /// that state to keep equalling a fresh load.
    pub fn prune_account_observations(
        &self,
        received_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_account_observations(received_before, limit)
        })
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
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_observation_events(received_before, limit)
        })
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

impl SqliteRouteStateStore {
    /// Run one retention batch in the store's write transaction
    /// ([`crate::write_transaction`]), committing it when the batch succeeds.

    /// Delete at most `limit` routing/v1 decision bindings created and expired
    /// before `before` (Unix milliseconds), oldest first, together with their
    /// report and per-delivery replay records. A binding is kept while a live
    /// account admission names it as its model decision, and while a stored
    /// continuation names it as its own or its previous decision (continuation
    /// rows go once their run is over; see
    /// [`Self::prune_account_continuations`]).
    pub fn prune_v1_decisions(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<V1DecisionRetentionBatch> {
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_v1_decisions(before, limit)
        })
    }

    /// Delete at most `limit` routing/v1 first-report records created before
    /// `before` whose decision binding is gone (pruned, or never written).
    /// Without its binding a report on that decision is refused as unknown,
    /// so the record can no longer answer a replay.
    pub fn prune_orphan_v1_reports(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM routing_v1_report WHERE rowid IN ( \
                   SELECT rowid FROM routing_v1_report INDEXED BY routing_v1_report_by_created \
                   WHERE created_at_ms < ?1 \
                     AND NOT EXISTS (SELECT 1 FROM routing_v1_decision d \
                       WHERE d.decision_id = routing_v1_report.decision_id) \
                   ORDER BY created_at_ms LIMIT ?2)",
                params![before, limit_param(limit)],
            )
        })
    }

    /// Delete at most `limit` per-delivery report records created before
    /// `before` whose decision binding is gone, for the same reason as
    /// [`Self::prune_orphan_v1_reports`].
    pub fn prune_orphan_v1_report_deliveries(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM routing_v1_report_delivery WHERE rowid IN ( \
                   SELECT rowid FROM routing_v1_report_delivery \
                     INDEXED BY routing_v1_report_delivery_by_created \
                   WHERE created_at_ms < ?1 \
                     AND NOT EXISTS (SELECT 1 FROM routing_v1_decision d \
                       WHERE d.decision_id = routing_v1_report_delivery.decision_id) \
                   ORDER BY created_at_ms LIMIT ?2)",
                params![before, limit_param(limit)],
            )
        })
    }

    /// Delete at most `limit` routing/v1 admission replay records created
    /// before `before`, oldest first. A record is kept while the account
    /// admission it answered is live, found by the same principal and id or by
    /// the model decision its body cites, so a runner re-sending an admission
    /// for a run still in flight gets the first answer back.
    pub fn prune_v1_admissions(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_v1_admissions(before, limit)
        })
    }

    /// Delete at most `limit` routing/v1 route-affinity rows last updated
    /// before `before`, oldest first. The row only steers the next selection
    /// for its key back to a warm prompt cache, which is long gone by then.
    pub fn prune_v1_affinities(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_v1_affinities(before, limit)
        })
    }

    /// Delete at most `limit` continuation answers (`account_continue`) whose
    /// run is over and whose own times (offer deadline, use, release) are all
    /// before `before`. The run is over when neither the run's original
    /// admission nor the offer's reservation is still reserved or started, and
    /// neither finished or was released at or after `before`. An admission
    /// already deleted by account retention counts as over. A used offer also
    /// outlives every started row it succeeds, and a predecessor outlives any
    /// continuation naming its decision, so restart resolves the same source.
    pub fn prune_account_continuations(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.refuse_state_blind_admission_write("prune_account_continuations")?;
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_account_continuations(before, limit)
        })
    }

    /// Delete at most `limit` generation-speed samples, oldest first: every
    /// sample observed before `cap_before`, and every sample at or before
    /// `read_window_start` that is not among the newest `keep_newest` of its
    /// route and served population (service tier and speed). Observed speed
    /// reads only samples after `read_window_start`, so nothing it reads is
    /// deleted.
    pub fn prune_speed_samples(
        &self,
        read_window_start: i64,
        cap_before: i64,
        keep_newest: usize,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM model_route_speed_samples WHERE rowid IN ( \
                   SELECT s.rowid FROM model_route_speed_samples s \
                     INDEXED BY model_route_speed_samples_by_observed \
                   WHERE s.observed_at <= ?1 \
                     AND (s.observed_at < ?2 OR (SELECT COUNT(*) FROM ( \
                       SELECT 1 FROM model_route_speed_samples n \
                       WHERE n.route_key = s.route_key \
                         AND n.population_service_tier IS s.population_service_tier \
                         AND n.population_speed IS s.population_speed \
                         AND n.observed_at > s.observed_at \
                       LIMIT ?3)) >= ?3) \
                   ORDER BY s.observed_at LIMIT ?4)",
                params![
                    read_window_start,
                    cap_before,
                    limit_param(keep_newest),
                    limit_param(limit)
                ],
            )
        })
    }

    /// Delete at most `limit` call-count rows last called before `before`,
    /// oldest first. Once no row that old remains, the call window's start
    /// moves up to `before`: a method, shape and principal with no row then
    /// truthfully had no call since the window start. Rows that stay keep
    /// their whole count, which can include calls before the new start; an
    /// over-count only ever keeps a handler alive.
    pub fn prune_routing_call_counts(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        self.retention_write(|tx| {
            let deleted = tx.execute(
                "DELETE FROM routing_call_count WHERE rowid IN ( \
                   SELECT rowid FROM routing_call_count INDEXED BY routing_call_count_by_last_called \
                   WHERE last_called_at_ms < ?1 ORDER BY last_called_at_ms LIMIT ?2)",
                params![before, limit_param(limit)],
            )?;
            let idle_left: bool = tx.query_row(
                "SELECT EXISTS (SELECT 1 FROM routing_call_count WHERE last_called_at_ms < ?1)",
                [before],
                |row| row.get(0),
            )?;
            if !idle_left {
                tx.execute(
                    "UPDATE routing_call_window SET started_at_ms = ?1 \
                     WHERE singleton = 1 AND started_at_ms < ?1",
                    [before],
                )?;
            }
            Ok(deleted)
        })
    }

    /// Delete at most `limit` physical rows in total across retired blocks,
    /// absent-source residue and the retirement archive, older than `before`.
    /// The returned count includes every relation so a full batch tells the
    /// retention drain to continue. These rows can no longer block routing.
    /// Account fence ids stay stored so a delayed repeat of the same fence
    /// cannot insert it again.
    pub fn prune_retired_fence_origins(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.prune_retired_fence_origins(before, limit)
        })
    }

    /// Delete at most `limit` unscored-model sightings last seen before
    /// `before`, oldest first. A model admitted again later is recorded as
    /// newly seen.
    pub fn prune_unscored_admissible_models(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM routing_unscored_admissible_models WHERE rowid IN ( \
                   SELECT rowid FROM routing_unscored_admissible_models \
                     INDEXED BY routing_unscored_admissible_models_by_last_seen \
                   WHERE last_seen_ms < ?1 ORDER BY last_seen_ms LIMIT ?2)",
                params![before, limit_param(limit)],
            )
        })
    }

    /// Delete at most `limit` quota readings observed before `before`, oldest
    /// first.
    pub fn prune_quota_reading_log(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        if let Some(deleted) = self.prune_history(crate::history::Kind::Quota, before, limit)? {
            return Ok(deleted);
        }
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM quota_reading_log \
                 WHERE (account_key, window_key, observed_at) IN ( \
                   SELECT account_key, window_key, observed_at FROM quota_reading_log \
                     INDEXED BY quota_reading_log_observed_idx \
                   WHERE observed_at < ?1 ORDER BY observed_at LIMIT ?2)",
                params![before, limit_param(limit)],
            )
        })
    }
}

// Match the loader's succession rule, including a predecessor reservation
// rather than the original admission when a run has moved more than once.
const USED_OFFER_SOURCES: &str = "
    SELECT c.principal, c.selection_id, c.run_id, l.admission_id AS left_id,
           l.provider, l.account_id, c.used_at_ms
    FROM account_continue c
    LEFT JOIN account_continue p
      ON p.principal=c.principal AND p.admission_id=c.admission_id
     AND p.run_id=c.run_id AND p.decision_id=c.previous_decision_id
    JOIN account_admission l ON l.principal=c.principal
     AND l.admission_id=COALESCE(p.reservation_id,c.admission_id)
    WHERE c.decision_id IS NOT NULL AND c.used_at_ms IS NOT NULL";

// The cutoff and the pass clock are separate: a short retention window must
// not remove a still-open reservation or a recently observed started run.
fn admission_due_by_values(a: &str) -> String {
    format!(
        "{a}.committed_at < ?1
         AND {a}.admission_valid_until < ?1 AND {a}.lease_valid_until < ?1
         AND COALESCE({a}.started_at,0) < ?1
         AND COALESCE({a}.released_at,0) < ?1
         AND COALESCE({a}.finished_at,0) < ?1
         AND ({a}.last_observed_at IS NULL OR {a}.last_observed_at < ?1)
         AND ({a}.state NOT IN ('reserved','started')
           OR ({a}.state='reserved' AND {a}.admission_valid_until <= ?3)
           OR ({a}.state='started' AND (
             ({a}.started_at IS NULL AND {a}.last_observed_at IS NULL)
             OR MAX(COALESCE({a}.last_observed_at,{a}.started_at),
                    COALESCE({a}.started_at,{a}.last_observed_at)) <= ?4)))"
    )
}

// Pins are retained by admissions, so pin existence cannot protect an
// admission without creating a retention cycle. The pin's conversation
// deadline, including the one-hour near-boundary grace, keeps the parent
// resolvable for forks that reuse its cached conversation.
fn admission_candidates(keyed: bool) -> String {
    let due = admission_due_by_values("a");
    let reservation_due = admission_due_by_values("r");
    let source = if keyed {
        "WHERE a.rowid IN (SELECT value FROM json_each(?5)) AND"
    } else {
        "INDEXED BY account_admission_by_committed WHERE"
    };
    format!("SELECT a.rowid FROM account_admission a {source} {due}
        AND NOT EXISTS (SELECT 1 FROM account_pin p
            JOIN account_affinity f ON f.principal=p.principal
             AND f.conversation_hash=p.conversation_hash AND f.provider=p.provider
            WHERE p.pin_id=a.pin_id
              AND MAX(f.expires_at,COALESCE(f.lease_valid_until,0)) >= ?3 - 3600000)
        AND NOT EXISTS (SELECT 1 FROM account_continue used
            JOIN account_admission parent ON parent.principal=used.principal
             AND parent.admission_id=used.admission_id
            WHERE used.decision_id=a.model_decision_id AND used.reservation_id=a.admission_id
              AND used.principal=a.principal AND used.used_at_ms IS NOT NULL
              AND parent.state IN ('reserved','started','finished','released'))
        AND NOT EXISTS (SELECT 1 FROM account_continue c
            JOIN account_admission r ON r.principal=c.principal AND r.admission_id=c.reservation_id
            WHERE c.principal=a.principal AND c.admission_id=a.admission_id
              AND c.run_id=a.run_id AND r.state IN ('reserved','started')
              AND NOT ({reservation_due}))
        AND NOT EXISTS (SELECT 1 FROM sources u
            JOIN account_admission s ON s.principal=u.principal AND s.run_id=u.run_id
             AND s.provider=u.provider AND s.account_id=u.account_id
            WHERE u.principal=a.principal AND u.left_id=a.admission_id AND s.state='started'
              AND (s.admission_id=u.left_id OR (s.request_mode!='renew' AND s.started_at<u.used_at_ms))
              AND NOT EXISTS (SELECT 1 FROM sources x
                WHERE x.principal=s.principal AND x.left_id=s.admission_id))
        ORDER BY a.committed_at LIMIT ?2")
}

#[cfg(test)]
mod fork_retention_tests {
    use super::*;

    #[test]
    fn fork_parent_prune_plan_uses_keyed_pin_and_affinity_lookups() {
        let scratch = cortexkit_test_support::ScratchDir::new("fork-prune-plan");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fork-prune-plan",
            scratch.path().join("state.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .store
            .with_read(|conn| {
                for keyed in [false, true] {
                    let sql = format!(
                        "EXPLAIN QUERY PLAN WITH sources AS MATERIALIZED ({USED_OFFER_SOURCES}) {}",
                        admission_candidates(keyed)
                    );
                    let mut statement = conn.prepare(&sql)?;
                    let values = if keyed {
                        vec![
                            0.into(),
                            8.into(),
                            0.into(),
                            0.into(),
                            rusqlite::types::Value::Text("[]".into()),
                        ]
                    } else {
                        vec![0.into(), 8.into(), 0.into(), 0.into()]
                    };
                    let plan = statement
                        .query_map(rusqlite::params_from_iter(values), |row| {
                            row.get::<_, String>(3)
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    assert!(
                        plan.iter()
                            .any(|line| line.starts_with("SEARCH p USING INDEX")
                                && line.contains("pin_id=?")),
                        "{plan:?}"
                    );
                    assert!(
                        plan.iter()
                            .any(|line| line.starts_with("SEARCH f USING INDEX")
                                && line.contains(
                                    "principal=? AND conversation_hash=? AND provider=?"
                                )),
                        "{plan:?}"
                    );
                    assert!(
                        !plan
                            .iter()
                            .any(|line| line.starts_with("SCAN f") || line.starts_with("SCAN p")),
                        "{plan:?}"
                    );
                    if keyed {
                        assert!(
                            plan.iter()
                                .any(|line| line.starts_with("SEARCH a USING INTEGER PRIMARY KEY")),
                            "{plan:?}"
                        );
                    }
                }
                Ok(())
            })
            .unwrap();
    }
}

impl crate::AccountTx<'_> {
    /// Delete only planned identities that are still old and unreferenced.
    pub fn delete_account_pins(&self, before: i64, keys: &[String]) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::delete_account_pins");
        let keys = serde_json::to_string(keys).map_err(err)?;
        self.command_sql(|tx| {
            let holds = crate::history::hold_source(tx);
            tx.execute(
                &format!(
                "DELETE FROM account_pin AS p WHERE pin_id IN (SELECT value FROM json_each(?2)) \
                 AND created_at<?1 AND {}", PIN_UNREFERENCED.replace("{holds}", holds)
            ),
                params![before, keys],
            )
        })
        .map_err(err)
    }

    pub fn prune_account_pins(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_account_pins");
        self.retention_write(|tx| {
            let holds = crate::history::hold_source(tx);
            tx.execute(
                &format!(
                    "DELETE FROM account_pin WHERE rowid IN ( \
                   SELECT p.rowid FROM account_pin p INDEXED BY account_pin_by_created \
                   WHERE p.created_at < ?1 \
                     AND NOT EXISTS (SELECT 1 FROM account_affinity f \
                       WHERE f.principal=p.principal AND f.conversation_hash=p.conversation_hash \
                         AND f.provider=p.provider) \
                     AND NOT EXISTS (SELECT 1 FROM account_admission a WHERE a.pin_id=p.pin_id) \
                     AND NOT EXISTS (SELECT 1 FROM {holds} h WHERE h.pin_id=p.pin_id) \
                   ORDER BY p.created_at LIMIT ?2)"
                ),
                params![before, limit_param(limit)],
            )
        })
    }

    pub fn prune_account_admissions(
        &self,
        before: i64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_account_admissions");
        self.prune_account_admissions_returning(before, limit, now_ms)
            .map(|deleted| deleted.len())
    }

    /// The same retention delete, returning each deleted row's identity so the
    /// module's in-memory admission indexes can drop exactly those rows.
    pub fn prune_account_admissions_returning(
        &self,
        before: i64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<Vec<crate::PrunedAdmission>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::retention::prune_account_admissions_returning",
        );
        self.prune_admission_rows(before, limit, now_ms, None)
    }

    pub fn delete_account_admissions_returning(
        &self,
        before: i64,
        keys: &[i64],
        now_ms: i64,
    ) -> RouteStateResult<Vec<crate::PrunedAdmission>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::retention::delete_account_admissions_returning",
        );
        self.prune_admission_rows(before, keys.len(), now_ms, Some(keys))
    }

    fn prune_admission_rows(
        &self,
        before: i64,
        limit: usize,
        now_ms: i64,
        keys: Option<&[i64]>,
    ) -> RouteStateResult<Vec<crate::PrunedAdmission>> {
        let keys_json = keys.map(serde_json::to_string).transpose().map_err(err)?;
        self.command_sql(|conn: &Connection| {
            // Read reservation values, not the delete candidate set: two idle
            // reservations must not keep each other forever. Source evidence
            // goes only after the other started rows it succeeds have gone,
            // even when a batch limit leaves those rows for the next pass.
            let candidates = admission_candidates(keys.is_some());
            let mut statement = conn.prepare(&format!(
                "WITH sources AS MATERIALIZED ({USED_OFFER_SOURCES}) \
                 DELETE FROM account_admission WHERE rowid IN ({candidates}) \
                  RETURNING rowid, principal, admission_id, conversation_hash, provider, \
                    account_id, pin_id, pin_generation, state, charged, committed_at, \
                    started_at IS NOT NULL"
            ))?;
            let mut values = vec![
                rusqlite::types::Value::Integer(before),
                rusqlite::types::Value::Integer(limit_param(limit)),
                rusqlite::types::Value::Integer(now_ms),
                rusqlite::types::Value::Integer(now_ms.saturating_sub(30 * 60 * 1_000)),
            ];
            if let Some(keys) = keys_json {
                values.push(rusqlite::types::Value::Text(keys));
            }
            let rows = statement.query_map(rusqlite::params_from_iter(values), |row| {
                Ok(crate::PrunedAdmission {
                    rowid: row.get(0)?,
                    principal: row.get(1)?,
                    admission_id: row.get(2)?,
                    conversation_hash: row.get(3)?,
                    provider: row.get(4)?,
                    account_id: row.get(5)?,
                    pin_id: row.get(6)?,
                    pin_generation: u64::try_from(row.get::<_, i64>(7)?).unwrap_or(0),
                    state: row.get(8)?,
                    charged: row.get::<_, i64>(9)? == 1,
                    committed_at: row.get(10)?,
                    started: row.get(11)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(err)
    }

    /// Recheck planned keys on the writer before deleting their generations.
    /// A selection may have refreshed the lease or added a reference after the
    /// reader planned the key. Return only keys actually deleted so the module's
    /// in-memory `AccountState.admissions` removes exactly the same rows.
    #[allow(clippy::type_complexity)]
    pub fn delete_account_affinities_returning(
        &self,
        keys: &[AffinityRetentionKey],
        before: i64,
        lease_before: i64,
    ) -> RouteStateResult<(AffinityRetentionBatch, Vec<AffinityRetentionKey>)> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::retention::delete_account_affinities_returning",
        );
        self.command_sql(|tx: &Connection| {
            let holds = crate::history::hold_source(tx);
            let mut eligible = tx.prepare(&format!(
                "SELECT EXISTS(SELECT 1 FROM account_affinity f \
                 WHERE f.principal=?1 AND f.conversation_hash=?2 AND f.provider=?3 \
                   AND f.expires_at<?4 AND COALESCE(f.lease_valid_until,0)<=?5 \
                   AND {})", AFFINITY_UNREFERENCED.replace("{holds}", holds)
            ))?;
            let mut delete_pins = tx.prepare(
                "DELETE FROM account_pin WHERE principal=?1 AND conversation_hash=?2 AND provider=?3"
            )?;
            let mut delete_affinity = tx.prepare(
                "DELETE FROM account_affinity WHERE principal=?1 AND conversation_hash=?2 AND provider=?3"
            )?;
            let mut batch = AffinityRetentionBatch::default();
            let mut deleted = Vec::new();
            for (principal, hash, provider) in keys {
                if !eligible.query_row(params![principal,hash,provider,before,lease_before], |row| row.get::<_,bool>(0))? {
                    continue;
                }
                batch.pins += delete_pins.execute(params![principal,hash,provider])?;
                batch.affinities += delete_affinity.execute(params![principal,hash,provider])?;
                deleted.push((principal.clone(),hash.clone(),provider.clone()));
            }
            Ok((batch, deleted))
        }).map_err(err)
    }

    pub fn prune_account_observations(
        &self,
        received_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_account_observations");
        self.command_sql(|conn: &Connection| {
                conn.execute(
                    "DELETE FROM account_observation WHERE rowid IN ( \
                       SELECT rowid FROM account_observation \
                         INDEXED BY account_observation_by_received \
                       WHERE received_at < ?1 \
                         AND NOT EXISTS (SELECT 1 FROM block b \
                           WHERE b.is_origin=1 AND b.kind IN ('account','principal') \
                             AND b.observation_id=account_observation.observation_id) \
                          AND NOT (kind='signed_out' AND bound=1 AND EXISTS ( \
                            SELECT 1 FROM account_admission a \
                            WHERE a.principal=account_observation.principal \
                              AND a.admission_id=account_observation.admission_id \
                              AND a.state IN ('reserved','started','expired'))) \
                          AND NOT EXISTS (SELECT 1 FROM json_each(CASE WHEN kind='transient_backoff' \
                            AND bound=1 AND json_valid(effects_json) THEN effects_json ELSE '[]' END) e \
                            WHERE json_extract(e.value,'$.effect')='transient_backoff' \
                            AND json_extract(e.value,'$.retry_at_ms')>?1) \
                       ORDER BY received_at LIMIT ?2)",
                    params![received_before, limit_param(limit)],
                )
            })
            .map_err(err)
    }

    pub fn prune_observation_events(
        &self,
        received_before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_observation_events");
        self.command_sql(|conn: &Connection| {
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

    fn retention_write<T>(
        &self,
        batch: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.command_sql(|conn: &Connection| {
            let tx = conn;
            let result = batch(&tx)?;

            Ok(result)
        })
        .map_err(err)
    }

    pub fn prune_v1_decisions(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<V1DecisionRetentionBatch> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_v1_decisions");
        if limit == 0 {
            return Ok(V1DecisionRetentionBatch::default());
        }
        self.retention_write(|tx| {
            let ids: Vec<String> = {
                let mut statement = tx.prepare(&format!(
                    "SELECT decision_id FROM routing_v1_decision \
                       INDEXED BY routing_v1_decision_by_created \
                     WHERE created_at_ms < ?1 AND expires_at_ms < ?1 \
                       AND NOT EXISTS (SELECT 1 FROM account_admission a \
                         WHERE a.model_decision_id = routing_v1_decision.decision_id \
                           AND a.state IN {LIVE_ADMISSION_STATES}) \
                       AND NOT EXISTS (SELECT 1 FROM account_continue c \
                         WHERE c.decision_id = routing_v1_decision.decision_id) \
                       AND NOT EXISTS (SELECT 1 FROM account_continue c \
                         WHERE c.previous_decision_id = routing_v1_decision.decision_id) \
                     ORDER BY created_at_ms LIMIT ?2"
                ))?;
                let rows =
                    statement.query_map(params![before, limit_param(limit)], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            let mut batch = V1DecisionRetentionBatch::default();
            let mut deliveries =
                tx.prepare("DELETE FROM routing_v1_report_delivery WHERE decision_id = ?1")?;
            let mut reports = tx.prepare("DELETE FROM routing_v1_report WHERE decision_id = ?1")?;
            let mut decisions =
                tx.prepare("DELETE FROM routing_v1_decision WHERE decision_id = ?1")?;
            for id in &ids {
                batch.deliveries += deliveries.execute([id])?;
                batch.reports += reports.execute([id])?;
                batch.decisions += decisions.execute([id])?;
            }
            Ok(batch)
        })
    }

    pub fn prune_v1_admissions(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_v1_admissions");
        self.retention_write(|tx| {
            tx.execute(
                &format!(
                    "DELETE FROM routing_v1_admission WHERE rowid IN ( \
                       SELECT v.rowid FROM routing_v1_admission v \
                         INDEXED BY routing_v1_admission_by_created \
                       WHERE v.created_at_ms < ?1 \
                         AND NOT EXISTS (SELECT 1 FROM account_admission a \
                           WHERE a.principal = v.principal AND a.admission_id = v.admission_id \
                             AND a.state IN {LIVE_ADMISSION_STATES}) \
                         AND NOT EXISTS (SELECT 1 FROM account_admission a \
                           WHERE a.model_decision_id = CASE WHEN json_valid(v.body_json) \
                               THEN json_extract(v.body_json, '$.decision_id') END \
                             AND a.state IN {LIVE_ADMISSION_STATES}) \
                       ORDER BY v.created_at_ms LIMIT ?2)"
                ),
                params![before, limit_param(limit)],
            )
        })
    }

    pub fn prune_v1_affinities(&self, before: i64, limit: usize) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_v1_affinities");
        self.retention_write(|tx| {
            tx.execute(
                "DELETE FROM routing_v1_affinity WHERE rowid IN ( \
                   SELECT rowid FROM routing_v1_affinity INDEXED BY routing_v1_affinity_by_updated \
                   WHERE updated_at_ms < ?1 ORDER BY updated_at_ms LIMIT ?2)",
                params![before, limit_param(limit)],
            )
        })
    }

    pub fn prune_account_continuations(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_account_continuations");
        self.prune_account_continuations_returning(before, limit)
            .map(|deleted| deleted.len())
    }

    /// The same retention delete, returning each deleted row's identity so the
    /// module's in-memory continuation indexes can drop exactly those rows.
    pub fn prune_account_continuations_returning(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<Vec<crate::PrunedContinuation>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::retention::prune_account_continuations_returning",
        );
        self.retention_write(|tx| {
            let mut statement = tx.prepare(&format!(
                "WITH sources AS MATERIALIZED ({USED_OFFER_SOURCES}) \
                     DELETE FROM account_continue WHERE rowid IN ( \
                       SELECT c.rowid FROM account_continue c \
                        WHERE COALESCE(c.valid_until_ms, 0) < ?1 \
                         AND COALESCE(c.used_at_ms, 0) < ?1 \
                          AND COALESCE(c.released_at_ms, 0) < ?1 \
                          AND NOT (c.used_at_ms IS NOT NULL AND EXISTS ( \
                            SELECT 1 FROM account_admission parent \
                            WHERE parent.principal=c.principal AND parent.admission_id=c.admission_id \
                              AND parent.state IN ('reserved','started','finished','released'))) \
                         AND NOT EXISTS (SELECT 1 FROM account_admission a \
                           WHERE a.principal = c.principal \
                             AND a.admission_id IN (c.admission_id, c.reservation_id) \
                             AND (a.state IN {LIVE_ADMISSION_STATES} \
                                OR COALESCE(a.finished_at, 0) >= ?1 \
                                OR COALESCE(a.released_at, 0) >= ?1)) \
                          AND NOT EXISTS (SELECT 1 FROM sources u \
                            JOIN account_admission s \
                              ON s.principal=u.principal AND s.run_id=u.run_id \
                             AND s.provider=u.provider AND s.account_id=u.account_id \
                            WHERE u.principal=c.principal AND u.selection_id=c.selection_id \
                              AND s.state='started' \
                              AND (s.admission_id=u.left_id \
                                OR (s.request_mode!='renew' AND s.started_at<u.used_at_ms))) \
                          AND NOT EXISTS (SELECT 1 FROM account_continue n \
                            WHERE n.principal=c.principal AND n.admission_id=c.admission_id \
                              AND n.run_id=c.run_id AND n.previous_decision_id=c.decision_id) \
                       ORDER BY c.rowid LIMIT ?2) \
                     RETURNING rowid, principal, selection_id, admission_id, run_id, \
                       previous_decision_id, decision_id, used_at_ms IS NOT NULL"
            ))?;
            let rows = statement.query_map(params![before, limit_param(limit)], |row| {
                Ok(crate::PrunedContinuation {
                    rowid: row.get(0)?,
                    principal: row.get(1)?,
                    selection_id: row.get(2)?,
                    admission_id: row.get(3)?,
                    run_id: row.get(4)?,
                    previous_decision_id: row.get(5)?,
                    decision_id: row.get(6)?,
                    used: row.get(7)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
    }

    pub fn prune_retired_fence_origins(
        &self,
        before: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::retention::prune_retired_fence_origins");
        self.retention_write(|tx| {
            // Never prune account block rows: even a cleared fence ID must
            // prevent replay. Delete other eligible rows in age-limited batches.
            let statements = [
                crate::block_storage::keyed_updates::retired_block_prune_sql(),
                crate::block_storage::keyed_updates::inactive_block_prune_sql(),
                "DELETE FROM retirement_tombstone WHERE rowid IN ( \
                   SELECT rowid FROM retirement_tombstone INDEXED BY retirement_tombstone_time \
                   WHERE retired_at < ?1 ORDER BY retired_at LIMIT ?2)"
                    .to_owned(),
            ];
            let mut deleted = 0;
            for sql in statements {
                let remaining = limit.saturating_sub(deleted);
                if remaining == 0 {
                    break;
                }
                deleted += tx.execute(&sql, params![before, limit_param(remaining)])?;
            }
            Ok(deleted)
        })
    }
}
