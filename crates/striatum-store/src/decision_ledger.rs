//! Decision records, diagnostic reads, and per-head failure history.
//! This keeps the durable explanation and its query projections together,
//! separate from the route health changes a decision outcome can trigger.

use super::*;

/// Stored rejection detail stays useful without growing with every catalog route.
const MAX_STORED_REJECTIONS: usize = 12;
/// How many omitted `not_available` route keys the rejection summary names.
const MAX_OMITTED_NOT_AVAILABLE_KEYS: usize = 200;

/// One head's failed decisions since a time, newest first, read from the
/// narrow per-head copy through its (parent, outcome time) index. Every row in
/// that table has an error, so it needs no `outcome_error IS NOT NULL` filter.
pub const RECENT_PARENT_FAILURES_SQL: &str =
    "SELECT selected_route_key, selected_provider_id, outcome_error, outcome_at
     FROM model_route_parent_failures INDEXED BY model_route_parent_failures_by_parent
     WHERE parent_session_id = ?1
       AND outcome_at >= ?2
     ORDER BY outcome_at DESC, decision_id DESC";

/// Retain actionable rejection detail within the ledger's fixed-size bound,
/// then append one reason-count summary for the omitted tail.
fn bound_rejected_json(raw: &str) -> String {
    let Ok(serde_json::Value::Array(rejected)) = serde_json::from_str(raw) else {
        return raw.to_string();
    };
    if rejected.len() <= MAX_STORED_REJECTIONS {
        return raw.to_string();
    }

    // Runtime walls and subscription budgets explain why otherwise usable
    // routes were refused. Preserve their deadlines and reserve arithmetic
    // ahead of catalog-tail exclusions so an outage remains diagnosable. A
    // credential refusal names the credential that has to be fixed, so it is
    // kept as well.
    let is_actionable = |entry: &&serde_json::Value| {
        matches!(
            entry.get("reason").and_then(serde_json::Value::as_str),
            Some(
                "backoff_active"
                    | "cooldown_active"
                    | "quota_exhausted"
                    | "subscription_reserve"
                    | "credential_unentitled"
                    | "credential_billing_exhausted"
            )
        )
    };
    let mut prioritized = Vec::with_capacity(rejected.len());
    prioritized.extend(rejected.iter().filter(is_actionable));
    prioritized.extend(rejected.iter().filter(|entry| !is_actionable(entry)));

    let mut reasons = BTreeMap::<String, usize>::new();
    for entry in &prioritized[MAX_STORED_REJECTIONS..] {
        let reason = entry
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *reasons.entry(reason.to_string()).or_default() += 1;
    }
    // A route the caller's host does not serve is the answer to "why was this
    // model never considered", and a count alone cannot answer it for a named
    // model. The omitted unavailable routes keep their keys in the summary, up
    // to a bound, so a model missing from a host's list reads as unavailable
    // rather than as absent from the decision.
    let omitted_not_available = prioritized[MAX_STORED_REJECTIONS..]
        .iter()
        .filter(|entry| {
            entry.get("reason").and_then(serde_json::Value::as_str) == Some("not_available")
        })
        .filter_map(|entry| entry.get("routeKey").cloned())
        .take(MAX_OMITTED_NOT_AVAILABLE_KEYS)
        .collect::<Vec<_>>();
    let truncated = prioritized.len() - MAX_STORED_REJECTIONS;
    let mut bounded = prioritized[..MAX_STORED_REJECTIONS]
        .iter()
        .map(|entry| (*entry).clone())
        .collect::<Vec<_>>();
    let mut summary = serde_json::json!({
        "truncated": truncated,
        "reasons": reasons,
    });
    if !omitted_not_available.is_empty() {
        summary["notAvailable"] = serde_json::Value::Array(omitted_not_available);
    }
    bounded.push(summary);
    serde_json::to_string(&bounded).unwrap_or_else(|_| raw.to_string())
}

/// One failed decision from a head's recent launches, enough to tell which
/// provider refused and why without loading the audit payloads.
#[derive(Debug, Clone, PartialEq)]
pub struct ParentFailedDecision {
    pub route_key: String,
    pub provider_id: String,
    pub outcome_error: String,
    pub outcome_at: i64,
}

/// One durable routing decision rendered for a diagnostic reader. JSON fields stay
/// serialized here so the wire layer can return exactly the evidence that was
/// recorded rather than reconstructing an explanation from today's catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionExplainRow {
    pub allowed_providers_json: Option<String>,
    pub id: String,
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub harness: Option<String>,
    pub project_root: Option<String>,
    /// The project the decision's scope was bound to, when it had one.
    pub project_id: Option<String>,
    /// The session folder the decision's scope was bound to, when it had one.
    pub workdir: Option<String>,
    pub send_id: Option<String>,
    pub target_agent: String,
    pub requirements_json: String,
    pub selected_route_key: Option<String>,
    pub selected_provider_id: Option<String>,
    pub selected_model_id: Option<String>,
    pub selected_variant: Option<String>,
    pub selected_harness: Option<String>,
    pub selected_workload: Option<String>,
    pub score_json: String,
    pub rejected_json: String,
    pub explanation_json: Option<String>,
    pub quota_signal_json: Option<String>,
    pub policy_revision: Option<String>,
    pub anchor_version: i64,
    pub catalog_fingerprint: Option<String>,
    pub created_at: i64,
    pub outcome: Option<String>,
    pub outcome_at: Option<i64>,
    pub outcome_error: Option<String>,
    /// When retention replaced the full scoring detail with the compact
    /// summary (see `decision_detail`); `None` while the row holds full detail.
    pub detail_stripped_at: Option<i64>,
}

/// Optional predicates for the compact decision-ledger list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecisionListFilter {
    pub send_id: Option<String>,
    pub target_agent: Option<String>,
    pub route_key: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub limit: usize,
}

/// One compact decision-ledger list row. The top rejection is derived in SQLite,
/// so callers do not need the persisted rejection payload to render a recent list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionListRow {
    pub id: String,
    pub created_at: i64,
    pub selected_route_key: Option<String>,
    pub decision_kind: String,
    pub top_rejection_reason: Option<String>,
    pub top_rejection_count: usize,
}

fn decision_explain_row(row: &Row<'_>) -> rusqlite::Result<DecisionExplainRow> {
    Ok(DecisionExplainRow {
        allowed_providers_json: row.get(26)?,
        id: row.get(0)?,
        decision_kind: row.get(1)?,
        task_id: row.get(2)?,
        parent_session_id: row.get(3)?,
        harness: row.get(4)?,
        project_root: row.get(5)?,
        send_id: row.get(6)?,
        target_agent: row.get(7)?,
        requirements_json: row.get(8)?,
        selected_route_key: row.get(9)?,
        selected_provider_id: row.get(10)?,
        selected_model_id: row.get(11)?,
        selected_variant: row.get(12)?,
        score_json: row.get(13)?,
        rejected_json: row.get(14)?,
        explanation_json: row.get(15)?,
        quota_signal_json: row.get(16)?,
        policy_revision: row.get(17)?,
        anchor_version: row.get(18)?,
        catalog_fingerprint: row.get(19)?,
        created_at: row.get(20)?,
        outcome: row.get(21)?,
        outcome_at: row.get(22)?,
        outcome_error: row.get(23)?,
        selected_harness: row.get(24)?,
        selected_workload: row.get(25)?,
        project_id: row.get(27)?,
        workdir: row.get(28)?,
        detail_stripped_at: row.get(29)?,
    })
}

/// Mint a decision ID before constructing the state that will retain it.
pub fn new_decision_id() -> String {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(id) = replay::current().and_then(|inputs| inputs.decision_id()) {
        return id;
    }
    format!("dec_{}", short_id())
}

/// A short unique-ish id (12 hex chars), process-monotonic + time-seeded. Avoids a
/// uuid dependency for decision ids (their uniqueness needs are local).
pub(super) fn short_id() -> String {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(inputs) = replay::current() {
        return inputs.next_id();
    }
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:08x}{:04x}", t & 0xffff_ffff, n & 0xffff)
}

impl SqliteRouteStateStore {
    /// Seed the decision columns an older build wrote, without supplying a
    /// payment class. Upgrade tests must exercise the migration's own backfill.
    #[cfg(any(test, feature = "test-support"))]
    pub fn seed_preupgrade_decision_for_test(
        &self,
        decision: &V1Decision,
        v1_binding: bool,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|connection| {
            connection.execute(
                "INSERT INTO model_route_decisions (id, target_agent, consumer, requirements_json,
                 selected_route_key, selected_provider_id, selected_model_id, score_json,
                 rejected_json, anchor_version, created_at)
                 VALUES (?1, ?2, ?2, '{}', ?3, ?4, ?5, '{}', '[]', 1, ?6)",
                params![decision.decision_id, decision.consumer, format!("{}/{}", decision.provider, decision.model),
                    decision.provider, decision.model, decision.created_at_ms],
            )?;
            if v1_binding {
                connection.execute(
                    "INSERT INTO routing_v1_decision (decision_id, principal, consumer, provider, model,
                     runner_module, created_at_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![decision.decision_id, decision.principal, decision.consumer, decision.provider,
                        decision.model, decision.runner_module, decision.created_at_ms, decision.expires_at_ms],
                )?;
            }
            Ok(())
        }).map_err(err)
    }

    /// Providers whose workers launched by `parent_session_id` were refused on
    /// content-policy grounds since `since_ms`, with the route and the refusal
    /// time of the newest such decision per provider.
    ///
    /// A content-policy refusal reflects the request, not the route, so it
    /// neither charges a strike nor changes route health. Unchanged health is
    /// correct for the fleet, but a retry from the same head can repeat the
    /// same request and select the same provider. The caller uses this history
    /// to distinguish that retry from unrelated work.
    ///
    /// The caller matches the stored refusal text against
    /// `classify_failure_attribution`; this read only narrows the rows to
    /// one head's recent failures.
    ///
    /// Every route.select that names a parent session runs this while holding
    /// the store's single connection, so it reads the narrow, indexed
    /// `model_route_parent_failures` copy (migration 054) rather than the
    /// decision ledger, which has no index for it and cost a whole-ledger scan.
    pub fn recent_failed_decisions_for_parent(
        &self,
        parent_session_id: &str,
        since_ms: i64,
    ) -> RouteStateResult<Vec<ParentFailedDecision>> {
        self.store
            .with_conn(|connection: &Connection| {
                let mut statement = connection.prepare(RECENT_PARENT_FAILURES_SQL)?;
                let rows = statement.query_map(params![parent_session_id, since_ms], |row| {
                    Ok(ParentFailedDecision {
                        route_key: row.get(0)?,
                        provider_id: row.get(1)?,
                        outcome_error: row.get(2)?,
                        outcome_at: row.get(3)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    pub fn set_decision_provider_identity(
        &self,
        decision_id: &str,
        identity: &striatum_core::route_state_store::ProviderOutcome,
    ) -> RouteStateResult<()> {
        let json =
            serde_json::to_string(identity).map_err(|e| RouteStateStoreError(e.to_string()))?;
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET provider_identity_json=?1 WHERE id=?2",
                    params![json, decision_id],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    pub fn decision_provider_identity(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<striatum_core::route_state_store::ProviderOutcome>> {
        let json: Option<String> = self
            .store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT provider_identity_json FROM model_route_decisions WHERE id=?1",
                    [decision_id],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)?
            .flatten();
        json.map(|raw| serde_json::from_str(&raw).map_err(|e| RouteStateStoreError(e.to_string())))
            .transpose()
    }

    /// Persist the resolved execution identity in the same insert as the audit row.
    /// The generic store trait leaves these values unknown; routing supplies them
    /// only after selection, without repurposing the caller's `input.harness`.
    pub fn record_decision_with_selection(
        &self,
        input: &DecisionInput,
        selected_harness: Option<&str>,
        selected_workload: Option<&str>,
    ) -> RouteStateResult<String> {
        self.record_decision_with_task_ref(
            input,
            selected_harness,
            selected_workload,
            input.task_id.as_deref(),
        )
    }

    /// Record an optional opaque task key alongside the selection, in `task_id`.
    /// The wire's `task_ref` is attribution only; it does not affect selection.
    pub fn record_decision_with_task_ref(
        &self,
        input: &DecisionInput,
        selected_harness: Option<&str>,
        selected_workload: Option<&str>,
        task_ref: Option<&str>,
    ) -> RouteStateResult<String> {
        self.record_decision_with_task_ref_and_identity(
            input,
            selected_harness,
            selected_workload,
            task_ref,
            None,
        )
    }

    /// Insert the audit row once admission succeeds, including the credential
    /// that owns a previously bound quota probe, when there is one.
    pub fn record_decision_with_task_ref_and_identity(
        &self,
        input: &DecisionInput,
        selected_harness: Option<&str>,
        selected_workload: Option<&str>,
        task_ref: Option<&str>,
        probe_identity: Option<&striatum_core::route_state_store::ProviderOutcome>,
    ) -> RouteStateResult<String> {
        let identity_json = probe_identity
            .map(serde_json::to_string)
            .transpose()
            .map_err(err)?;
        let id = input.decision_id.clone().unwrap_or_else(new_decision_id);
        let id_ret = id.clone();
        let created_at = now_ms();
        let rejected_json = bound_rejected_json(&input.rejected_json);
        self.store
            .with_conn(|c: &Connection| {
                let catalog_fingerprint = self.current_catalog_fingerprint(c)?;
                c.execute(
                     "INSERT INTO model_route_decisions (
                         id, decision_kind, task_id, parent_session_id, harness, project_root, send_id,
                         target_agent, requirements_json, selected_route_key, selected_provider_id,
                         selected_model_id, selected_variant, score_json, rejected_json, explanation_json,
                         quota_signal_json, policy_revision, anchor_version, catalog_fingerprint, created_at,
                         selected_harness, selected_workload, consumer, allowed_providers_json,
                         project_id, workdir, allowed_account_class, provider_identity_json
                       ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29)",
                    params![
                        id,
                        if input.decision_kind.is_empty() { "single" } else { &input.decision_kind },
                        task_ref,
                        input.parent_session_id,
                        input.harness,
                        input.project_root,
                        input.send_id,
                        input.target_agent,
                        input.requirements_json,
                        input.selected_route_key,
                        input.selected_provider_id,
                        input.selected_model_id,
                        input.selected_variant,
                        input.score_json,
                        rejected_json,
                        input.explanation_json,
                        input.quota_signal_json,
                        input.policy_revision,
                        input.anchor_version,
                        catalog_fingerprint,
                        created_at,
                        selected_harness,
                        selected_workload,
                        input.consumer,
                        input.allowed_providers_json,
                        input.project_id,
                        input.workdir,
                        input.allowed_account_class.as_str(),
                        identity_json,
                    ],
                )?;
                operator_quality::observe_decision(c, &input.score_json, created_at)?;
                Ok(())
            })
            .map_err(err)?;
        Ok(id_ret)
    }

    /// Read the account-class authority frozen when this decision was selected.
    pub fn decision_account_class(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<striatum_core::accounts::AllowedAccountClass>> {
        let value: Option<String> = self
            .store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT allowed_account_class FROM model_route_decisions WHERE id=?1",
                    [decision_id],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)?;
        value
            .map(|value| striatum_core::accounts::AllowedAccountClass::parse(&value).map_err(err))
            .transpose()
    }

    /// Look up one decision by its durable id for a diagnostic explanation.
    pub fn get_decision_by_id(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<DecisionExplainRow>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT id, decision_kind, task_id, parent_session_id, harness, project_root,
                                send_id, target_agent, requirements_json, selected_route_key,
                                selected_provider_id, selected_model_id, selected_variant, score_json,
                                rejected_json, explanation_json, quota_signal_json, policy_revision,
                                anchor_version, catalog_fingerprint, created_at, outcome, outcome_at,
                                outcome_error, selected_harness, selected_workload, allowed_providers_json,
                                project_id, workdir, detail_stripped_at
                         FROM model_route_decisions
                         WHERE id=?1",
                        [decision_id],
                        decision_explain_row,
                    )
                    .optional()
            })
            .map_err(err)
    }

    /// Look up the most recent decision correlated to one launch attempt.
    pub fn get_decision_by_send_id(
        &self,
        send_id: &str,
    ) -> RouteStateResult<Option<DecisionExplainRow>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT id, decision_kind, task_id, parent_session_id, harness, project_root,
                                send_id, target_agent, requirements_json, selected_route_key,
                                selected_provider_id, selected_model_id, selected_variant, score_json,
                                rejected_json, explanation_json, quota_signal_json, policy_revision,
                                anchor_version, catalog_fingerprint, created_at, outcome, outcome_at,
                                outcome_error, selected_harness, selected_workload, allowed_providers_json,
                                project_id, workdir, detail_stripped_at
                         FROM model_route_decisions
                         WHERE send_id=?1
                         ORDER BY created_at DESC, id DESC
                         LIMIT 1",
                        [send_id],
                        decision_explain_row,
                    )
                    .optional()
            })
            .map_err(err)
    }

    /// List recent decisions without returning their full audit payloads.
    pub fn list_decisions(
        &self,
        filter: &DecisionListFilter,
    ) -> RouteStateResult<Vec<DecisionListRow>> {
        let limit = filter.limit.min(100) as i64;
        self.store
            .with_conn(|connection: &Connection| {
                // Keep payload-scale JSON on explain; lists need only compact scalars.
                let mut statement = connection.prepare(
                    "WITH filtered AS (
                         SELECT id, created_at, selected_route_key, decision_kind, rejected_json
                         FROM model_route_decisions
                         WHERE (?1 IS NULL OR send_id=?1)
                            AND (?2 IS NULL OR target_agent IN (?2, ?7))
                           AND (?3 IS NULL OR selected_route_key=?3)
                           AND (?4 IS NULL OR created_at>=?4)
                           AND (?5 IS NULL OR created_at<=?5)
                         ORDER BY created_at DESC, id DESC
                         LIMIT ?6
                     ), rejection_counts AS (
                         SELECT filtered.id,
                                json_extract(
                                    CASE WHEN rejection.type='object' THEN rejection.value ELSE '{}' END,
                                    '$.reason'
                                ) AS reason,
                                COUNT(*) AS count
                         FROM filtered
                         JOIN json_each(
                             CASE WHEN json_valid(filtered.rejected_json)
                                  THEN filtered.rejected_json ELSE '[]' END
                         ) AS rejection
                         WHERE json_type(
                             CASE WHEN rejection.type='object' THEN rejection.value ELSE '{}' END,
                             '$.reason'
                         )='text'
                         GROUP BY filtered.id, reason
                     ), top_rejections AS (
                         SELECT id, reason, count,
                                ROW_NUMBER() OVER (
                                    PARTITION BY id ORDER BY count DESC, reason ASC
                                ) AS rank
                         FROM rejection_counts
                     )
                     SELECT filtered.id, filtered.created_at, filtered.selected_route_key,
                            filtered.decision_kind, top_rejections.reason,
                            COALESCE(top_rejections.count, 0)
                     FROM filtered
                     LEFT JOIN top_rejections
                       ON top_rejections.id=filtered.id AND top_rejections.rank=1
                     ORDER BY filtered.created_at DESC, filtered.id DESC",
                )?;
                let rows = statement.query_map(
                    params![
                        filter.send_id.as_deref(),
                        filter.target_agent.as_deref(),
                        filter.route_key.as_deref(),
                        filter.since_ms,
                        filter.until_ms,
                        limit,
                        filter.target_agent.as_deref().map(striatum_core::module_identity::alternate).as_deref(),
                    ],
                    |row| {
                        let top_rejection_count = row.get::<_, i64>(5)?;
                        Ok(DecisionListRow {
                            id: row.get(0)?,
                            created_at: row.get(1)?,
                            selected_route_key: row.get(2)?,
                            decision_kind: row.get(3)?,
                            top_rejection_reason: row.get(4)?,
                            top_rejection_count: top_rejection_count.max(0) as usize,
                        })
                    },
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_open::tests::{
        insert_ledger_row, observed_speed_test_store, remove_observed_speed_test_store,
    };

    /// The head-history query as it read the decision ledger before migration
    /// 054: no index covers it, so it visits every ledger row. Kept here as the
    /// oracle the narrow copy must agree with, and as the slow shape the
    /// benchmark measures.
    const LEDGER_PARENT_FAILURES_SQL: &str =
        "SELECT selected_route_key, selected_provider_id, outcome_error, outcome_at
         FROM model_route_decisions
         WHERE parent_session_id = ?1
           AND outcome_at >= ?2
           AND outcome_error IS NOT NULL
         ORDER BY outcome_at DESC, id DESC";

    /// The test clock, 2026-10-19T00:00:00Z.
    const HEAD_NOW: i64 = 1_792_368_000_000;

    fn ledger_parent_failures(
        connection: &Connection,
        parent_session_id: &str,
        since_ms: i64,
    ) -> rusqlite::Result<Vec<ParentFailedDecision>> {
        let mut statement = connection.prepare(LEDGER_PARENT_FAILURES_SQL)?;
        let rows = statement.query_map(params![parent_session_id, since_ms], |row| {
            Ok(ParentFailedDecision {
                route_key: row.get(0)?,
                provider_id: row.get(1)?,
                outcome_error: row.get(2)?,
                outcome_at: row.get(3)?,
            })
        })?;
        rows.collect()
    }

    fn fail_ledger_row(
        connection: &Connection,
        id: &str,
        outcome_at: i64,
        error: Option<&str>,
    ) -> rusqlite::Result<usize> {
        connection.execute(
            "UPDATE model_route_decisions
             SET outcome = 'session_error', outcome_at = ?2, outcome_error = ?3
             WHERE id = ?1",
            params![id, outcome_at, error],
        )
    }

    /// Count SQLite virtual-machine steps on the store's connection while `f`
    /// runs: a machine-independent measure of how many rows a read visited.
    fn vm_steps_during<T>(store: &SqliteRouteStateStore, f: impl FnOnce() -> T) -> (T, u64) {
        unsafe extern "C" fn count_step(counter: *mut std::ffi::c_void) -> std::ffi::c_int {
            // SAFETY: `counter` is the AtomicU64 below, which outlives the
            // handler because the handler is removed before it is dropped.
            unsafe { &*(counter as *const AtomicU64) }.fetch_add(1, Ordering::Relaxed);
            0
        }
        let counter = AtomicU64::new(0);
        let counter_ptr = &counter as *const AtomicU64 as *mut std::ffi::c_void;
        store
            .store
            .with_conn(|connection| {
                // SAFETY: the handle belongs to the live connection guarded by
                // the store mutex; the callback only touches `counter`.
                unsafe {
                    rusqlite::ffi::sqlite3_progress_handler(
                        connection.handle(),
                        1,
                        Some(count_step),
                        counter_ptr,
                    );
                }
                Ok(())
            })
            .unwrap();
        let out = f();
        store
            .store
            .with_conn(|connection| {
                // SAFETY: as above; passing no callback removes the handler.
                unsafe {
                    rusqlite::ffi::sqlite3_progress_handler(
                        connection.handle(),
                        0,
                        None,
                        std::ptr::null_mut(),
                    );
                }
                Ok(())
            })
            .unwrap();
        (out, counter.load(Ordering::Relaxed))
    }

    #[test]
    fn head_failure_history_answers_like_the_ledger_without_scanning_it() {
        let (path, store) = observed_speed_test_store("head-failures");
        let recent = HEAD_NOW - 10 * 60_000;
        let created = HEAD_NOW - 60 * 60_000;
        store
            .store
            .with_conn(|c| {
                for (id, parent) in [
                    ("a-recent", Some("head-a")),
                    ("a-tie-1", Some("head-a")),
                    ("a-tie-2", Some("head-a")),
                    ("a-old", Some("head-a")),
                    ("a-ok", Some("head-a")),
                    ("a-pending", Some("head-a")),
                    ("a-retracted", Some("head-a")),
                    ("a-real", Some("head-a")),
                    ("b-recent", Some("head-b")),
                    ("n-recent", None),
                ] {
                    insert_ledger_row(c, id, parent, created, 64)?;
                }
                // Ancient, so the retention delete below removes it.
                insert_ledger_row(c, "a-expired", Some("head-a"), 1, 64)?;
                fail_ledger_row(c, "a-recent", recent, Some("content policy"))?;
                fail_ledger_row(c, "a-tie-1", recent - 1, Some("refused"))?;
                fail_ledger_row(c, "a-tie-2", recent - 1, Some("refused"))?;
                fail_ledger_row(c, "a-old", HEAD_NOW - 2 * 60 * 60_000, Some("old"))?;
                fail_ledger_row(c, "a-ok", recent, None)?;
                fail_ledger_row(c, "a-retracted", recent, Some("refused"))?;
                fail_ledger_row(c, "b-recent", recent, Some("refused"))?;
                fail_ledger_row(c, "n-recent", recent, Some("refused"))?;
                fail_ledger_row(c, "a-expired", recent, Some("refused"))?;
                // A later change to the copied columns must reach the copy.
                c.execute(
                    "UPDATE model_route_decisions SET outcome_error = NULL WHERE id = 'a-retracted'",
                    [],
                )
            })
            .unwrap();
        // The routing module's own outcome writer.
        store
            .set_decision_outcome(
                "a-real",
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: striatum_core::RouteOutcome::SessionError,
                    last_error: Some("blocked by the content policy".into()),
                    observed_at_ms: recent + 1,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
        // The retention pass's delete.
        assert_eq!(store.delete_decisions_older_than(1_000, 10).unwrap(), 1);

        let since = HEAD_NOW - 60 * 60_000;
        for (parent, since_ms) in [
            ("head-a", since),
            ("head-a", 0),
            ("head-b", since),
            ("head-c", 0),
        ] {
            let expected = store
                .store
                .with_conn(|c| ledger_parent_failures(c, parent, since_ms))
                .unwrap();
            let actual = store
                .recent_failed_decisions_for_parent(parent, since_ms)
                .unwrap();
            assert_eq!(actual, expected, "{parent} since {since_ms}");
        }
        let head_a: Vec<String> = store
            .recent_failed_decisions_for_parent("head-a", since)
            .unwrap()
            .into_iter()
            .map(|row| row.route_key)
            .collect();
        assert_eq!(head_a, ["p/a-real", "p/a-recent", "p/a-tie-2", "p/a-tie-1"]);

        // Fill the ledger with other heads' decisions, a third of them failed.
        // The ledger query visits all of them; the copy visits head-a's rows.
        store
            .store
            .with_conn(|c| {
                let tx = c.unchecked_transaction()?;
                for index in 0..3_000 {
                    let id = format!("filler-{index}");
                    insert_ledger_row(
                        &tx,
                        &id,
                        Some(&format!("head-{}", index % 50)),
                        created,
                        2_000,
                    )?;
                    if index % 3 == 0 {
                        fail_ledger_row(&tx, &id, recent, Some("refused"))?;
                    }
                }
                crate::connection_store::timed_commit(tx)
            })
            .unwrap();
        let (copy_rows, copy_steps) = vm_steps_during(&store, || {
            store
                .recent_failed_decisions_for_parent("head-a", since)
                .unwrap()
        });
        let (ledger_rows, ledger_steps) = vm_steps_during(&store, || {
            store
                .store
                .with_conn(|c| ledger_parent_failures(c, "head-a", since))
                .unwrap()
        });
        assert_eq!(copy_rows, ledger_rows);
        println!("head_failure_vm_steps ledger_rows=3011 copy={copy_steps} ledger={ledger_steps}");
        assert!(
            copy_steps < 500,
            "the head-history read must visit only that head's failures, \
             not the decision ledger: {copy_steps} VM steps"
        );
        assert!(
            ledger_steps > 20 * copy_steps,
            "the oracle must show the scan this test guards against: \
             ledger {ledger_steps} vs copy {copy_steps} VM steps"
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn migration_054_copies_recent_failures_found_through_the_created_at_index() {
        let scratch = cortexkit_test_support::scratch_root().join(format!(
            "routing-parent-failures-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-parent-failures",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..53])
            .unwrap();
        legacy
            .with_conn(|c| {
                insert_ledger_row(c, "recent", Some("head"), now - 60 * 60_000, 64)?;
                fail_ledger_row(c, "recent", now - 30 * 60_000, Some("refused"))?;
                // Made thirty days ago, refused just now: outside the backfill's
                // seven-day decision window, as the migration documents.
                insert_ledger_row(c, "long-lived", Some("head"), now - 30 * 86_400_000, 64)?;
                fail_ledger_row(c, "long-lived", now - 60_000, Some("refused"))?;
                insert_ledger_row(c, "no-head", None, now - 60 * 60_000, 64)?;
                fail_ledger_row(c, "no-head", now - 30 * 60_000, Some("refused"))
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let rows = migrated
            .recent_failed_decisions_for_parent("head", now - 60 * 60_000)
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.route_key.as_str())
                .collect::<Vec<_>>(),
            ["p/recent"]
        );
        drop(migrated);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    /// Reproduces the route.select stall: a selection's head-history read runs
    /// while another thread keeps recording decisions, both through the store's
    /// single connection, the way concurrent route.select requests share it.
    /// Prints the reader's latency and the writer's worst wait for the ledger
    /// query shape (before migration 054) and for the narrow copy (after).
    /// `PREFRONTAL_ROUTING_STALL_ROWS` sets the ledger size (default 60,000).
    #[test]
    #[ignore = "prints a local latency comparison of the head-history read under a concurrent writer"]
    fn head_failure_history_under_a_concurrent_writer_benchmark() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let rows: i64 = std::env::var("PREFRONTAL_ROUTING_STALL_ROWS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60_000);
        let (path, store) = observed_speed_test_store("head-failure-stall");
        let store = Arc::new(store);
        let created = HEAD_NOW - 60 * 60_000;
        store
            .store
            .with_conn(|c| {
                let tx = c.unchecked_transaction()?;
                for index in 0..rows {
                    let id = format!("ledger-{index}");
                    // Score payloads of a few kilobytes, like real selections.
                    insert_ledger_row(
                        &tx,
                        &id,
                        Some(&format!("head-{}", index % 200)),
                        created,
                        6_000,
                    )?;
                    if index % 10 == 0 {
                        fail_ledger_row(&tx, &id, HEAD_NOW - 60_000, Some("refused"))?;
                    }
                }
                crate::connection_store::timed_commit(tx)
            })
            .unwrap();

        let measure = |label: &str, read: &dyn Fn() -> Vec<ParentFailedDecision>| {
            let stop = Arc::new(AtomicBool::new(false));
            let writer = {
                let store = Arc::clone(&store);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut worst = Duration::ZERO;
                    let mut writes = 0_u64;
                    while !stop.load(Ordering::Relaxed) {
                        let input = DecisionInput {
                            parent_session_id: Some("writer-head".into()),
                            target_agent: "mason".into(),
                            requirements_json: "{}".into(),
                            score_json: "{}".into(),
                            rejected_json: "[]".into(),
                            ..DecisionInput::default()
                        };
                        let started = Instant::now();
                        store
                            .record_decision_with_selection(&input, None, None)
                            .unwrap();
                        worst = worst.max(started.elapsed());
                        writes += 1;
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    (worst, writes)
                })
            };
            let mut reads = Vec::new();
            for _ in 0..10 {
                let started = Instant::now();
                assert!(!read().is_empty());
                reads.push(started.elapsed());
                std::thread::sleep(Duration::from_millis(5));
            }
            stop.store(true, Ordering::Relaxed);
            let (writer_worst, writes) = writer.join().unwrap();
            let total: Duration = reads.iter().sum();
            println!(
                "head_failure_stall rows={rows} shape={label} read_avg_ms={:.3} read_max_ms={:.3} writer_max_wait_ms={:.3} writes={writes}",
                total.as_secs_f64() * 1_000.0 / reads.len() as f64,
                reads.iter().max().unwrap().as_secs_f64() * 1_000.0,
                writer_worst.as_secs_f64() * 1_000.0,
            );
        };
        let since = HEAD_NOW - 60 * 60_000;
        measure("ledger", &|| {
            store
                .store
                .with_conn(|c| ledger_parent_failures(c, "head-10", since))
                .unwrap()
        });
        measure("copy", &|| {
            store
                .recent_failed_decisions_for_parent("head-10", since)
                .unwrap()
        });

        drop(store);
        remove_observed_speed_test_store(&path);
    }
}
