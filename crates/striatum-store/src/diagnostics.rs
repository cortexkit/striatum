//! Read-only queries behind routing's `diagnostics.findings` operation.
//!
//! Each query reads the current blocking state (route walls, account holds,
//! standing account pool blocks, key-pool walls) or the compact columns of the
//! decisions made in a time window. None of them writes: a diagnosis must never
//! lift, renew or reshape the state it reports. The decision query extracts
//! the few fields the checks need inside SQLite, so a window of large
//! rejection payloads is never copied out of the store.

use rusqlite::{params, Connection};
use striatum_core::{RouteStateResult, RouteWallClass};

use super::{err, row_wall_class, SqliteRouteStateStore};

/// Rejection reasons that describe the catalog rather than a wall: a route
/// awaiting operator scores, retired upstream, or excluded by policy is
/// rejected on every request alike. They are skipped when naming the cause of
/// a refused decision, so the cause is the wall that actually refused it.
const STATIC_REJECTION_REASONS: &[&str] = &[
    "unscored_awaiting_placement",
    "deprecated_upstream",
    "excluded_by_operator",
    "excluded",
];

/// One route whose stored state blocks it now: a cooldown, an exhausted quota
/// window or a terminal-failure backoff whose deadline is still ahead.
#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticRouteWall {
    pub route_key: String,
    pub capacity_key: String,
    pub provider_id: String,
    pub model_id: String,
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
    pub failure_backoff_until: Option<f64>,
    pub failure_backoff_reason: Option<String>,
    pub last_error: Option<String>,
    pub last_error_at: Option<i64>,
    pub wall_class: Option<RouteWallClass>,
}

/// One account hold whose retry time has not yet come.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticAccountHold {
    pub provider: String,
    pub account_id: String,
    pub credential_id: String,
    pub basis: String,
    pub reset_at: i64,
    pub retry_after_s: i64,
    pub created_at: i64,
}

/// One account pool block that has not been cleared. `pool_key_json` is
/// `None` for a block on the whole account; `fence_json` is the confirming
/// evidence, which names the reset time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticAccountBlock {
    pub fence_id: String,
    pub provider: String,
    pub account_id: String,
    pub pool_key_json: Option<String>,
    pub fence_json: String,
    pub source: String,
    pub observed_at: i64,
    pub created_at: i64,
}

/// One key-pool credential walled or cooling down now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticKeyWall {
    pub provider: String,
    pub credential_id: String,
    pub walled_until: Option<i64>,
    pub wall_reason: Option<String>,
    pub cooldown_until: Option<i64>,
    pub cooldown_reason: Option<String>,
    pub updated_at: i64,
}

/// The compact fields of one single-route decision.
#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticDecision {
    pub id: String,
    pub created_at: i64,
    /// The recorded consumer, or the target agent for rows written without one.
    pub consumer: String,
    /// `provider/model` of the selected route; `None` for a refusal.
    pub selected_model: Option<String>,
    /// The requested iq, when the requirements named one.
    pub iq: Option<f64>,
    /// The other routes that cleared the quality gate (`provider/model` or
    /// `provider/model:variant`), as the selection recorded them.
    pub also_adequate: Vec<String>,
    /// Whether the selection was a head's model pin, which has one candidate
    /// by design.
    pub pinned: bool,
    /// For a refusal: the most frequent wall among its rejected routes, else
    /// the scorer's failure reason.
    pub refusal_code: Option<String>,
}

impl SqliteRouteStateStore {
    /// Every route whose cooldown, quota wall or failure backoff is still
    /// ahead of `now_ms`, ordered by model then route.
    pub fn diagnostic_route_walls(
        &self,
        now_ms: i64,
    ) -> RouteStateResult<Vec<DiagnosticRouteWall>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT route_key, capacity_key, provider_id, model_id, cooldown_until,
                            quota_exhausted_until, failure_backoff_until, failure_backoff_reason,
                            last_error, last_error_at, wall_class
                     FROM model_route_state
                     WHERE cooldown_until > ?1 OR quota_exhausted_until > ?1
                        OR failure_backoff_until > ?1
                     ORDER BY capacity_key, route_key",
                )?;
                let rows = statement.query_map(params![now_ms], |row| {
                    Ok(DiagnosticRouteWall {
                        route_key: row.get(0)?,
                        capacity_key: row.get(1)?,
                        provider_id: row.get(2)?,
                        model_id: row.get(3)?,
                        cooldown_until: row.get(4)?,
                        quota_exhausted_until: row.get(5)?,
                        failure_backoff_until: row.get(6)?,
                        failure_backoff_reason: row.get(7)?,
                        last_error: row.get(8)?,
                        last_error_at: row.get(9)?,
                        wall_class: row_wall_class(row, 10)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Every account hold whose retry time (`created_at + retry_after_s`) is
    /// still ahead of `now_ms`, oldest first.
    pub fn diagnostic_account_holds(
        &self,
        now_ms: i64,
    ) -> RouteStateResult<Vec<DiagnosticAccountHold>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, account_id, credential_id, basis, reset_at, retry_after_s,
                            created_at
                     FROM account_hold
                     WHERE created_at + retry_after_s * 1000 > ?1
                     ORDER BY created_at, decision_id",
                )?;
                let rows = statement.query_map(params![now_ms], |row| {
                    Ok(DiagnosticAccountHold {
                        provider: row.get(0)?,
                        account_id: row.get(1)?,
                        credential_id: row.get(2)?,
                        basis: row.get(3)?,
                        reset_at: row.get(4)?,
                        retry_after_s: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Every account pool block not yet cleared, oldest first. A block whose
    /// reset has passed stays standing until the next admission on its
    /// provider clears it, so the caller compares the evidence's reset time.
    pub fn diagnostic_account_blocks(&self) -> RouteStateResult<Vec<DiagnosticAccountBlock>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT fence_id, provider, account_id, pool_key_json, fence_json, source,
                            observed_at, created_at
                     FROM account_fence WHERE cleared_at IS NULL
                     ORDER BY created_at, fence_id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(DiagnosticAccountBlock {
                        fence_id: row.get(0)?,
                        provider: row.get(1)?,
                        account_id: row.get(2)?,
                        pool_key_json: row.get(3)?,
                        fence_json: row.get(4)?,
                        source: row.get(5)?,
                        observed_at: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Every key-pool credential whose billing wall or rate-limit cooldown is
    /// still ahead of `now_ms`.
    pub fn diagnostic_key_walls(&self, now_ms: i64) -> RouteStateResult<Vec<DiagnosticKeyWall>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, credential_id, walled_until, wall_reason, cooldown_until,
                            cooldown_reason, updated_at
                     FROM pool_key_state
                     WHERE walled_until > ?1 OR cooldown_until > ?1
                     ORDER BY provider, credential_id",
                )?;
                let rows = statement.query_map(params![now_ms], |row| {
                    Ok(DiagnosticKeyWall {
                        provider: row.get(0)?,
                        credential_id: row.get(1)?,
                        walled_until: row.get(2)?,
                        wall_reason: row.get(3)?,
                        cooldown_until: row.get(4)?,
                        cooldown_reason: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// The newest `limit` single-route decisions created in
    /// `[since_ms, until_ms]`, returned oldest first. Panel decisions are
    /// left out: their singular route columns are empty even when they
    /// succeed, so they would read as refusals.
    pub fn diagnostic_decisions(
        &self,
        since_ms: i64,
        until_ms: i64,
        limit: usize,
    ) -> RouteStateResult<Vec<DiagnosticDecision>> {
        let static_reasons = serde_json::to_string(STATIC_REJECTION_REASONS).map_err(err)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "WITH windowed AS (
                         SELECT id, created_at, consumer, target_agent, selected_route_key,
                                selected_provider_id, selected_model_id, requirements_json,
                                score_json, explanation_json, rejected_json
                         FROM model_route_decisions
                         WHERE created_at >= ?1 AND created_at <= ?2
                           AND decision_kind = 'single'
                         ORDER BY created_at DESC, id DESC
                         LIMIT ?3
                     )
                     SELECT id, created_at, COALESCE(consumer, target_agent),
                            CASE WHEN selected_route_key IS NOT NULL
                                 THEN selected_provider_id || '/' || selected_model_id END,
                            CASE WHEN json_valid(requirements_json)
                                 THEN json_extract(requirements_json, '$.iq') END,
                            CASE WHEN selected_route_key IS NOT NULL AND json_valid(score_json)
                                  AND json_type(score_json, '$.alsoAdequate') = 'array'
                                 THEN json_extract(score_json, '$.alsoAdequate') END,
                            -- A caller that names its model (a head's work pin,
                            -- or a seat or session admitting the exact model it
                            -- already chose) narrows the candidates on purpose.
                            CASE WHEN selected_route_key IS NOT NULL
                                  AND (explanation_json LIKE '%model pinned by head%'
                                       OR explanation_json LIKE '%admission-only pin:%')
                                 THEN 1 ELSE 0 END,
                            CASE WHEN selected_route_key IS NULL THEN COALESCE(
                                (SELECT json_extract(rejection.value, '$.reason') AS reason
                                 FROM json_each(CASE WHEN json_valid(rejected_json)
                                                     THEN rejected_json ELSE '[]' END) AS rejection
                                 WHERE rejection.type = 'object'
                                   AND json_type(rejection.value, '$.reason') = 'text'
                                   AND json_extract(rejection.value, '$.reason')
                                       NOT IN (SELECT value FROM json_each(?4))
                                 GROUP BY reason ORDER BY COUNT(*) DESC, reason ASC LIMIT 1),
                                CASE WHEN json_valid(explanation_json)
                                      AND json_type(explanation_json) = 'object'
                                     THEN json_extract(explanation_json, '$.failureReason') END)
                            END
                     FROM windowed
                     ORDER BY created_at ASC, id ASC",
                )?;
                let rows = statement.query_map(
                    params![since_ms, until_ms, limit, static_reasons],
                    |row| {
                        let also_adequate: Option<String> = row.get(5)?;
                        Ok(DiagnosticDecision {
                            id: row.get(0)?,
                            created_at: row.get(1)?,
                            consumer: row.get(2)?,
                            selected_model: row.get(3)?,
                            iq: row.get(4)?,
                            also_adequate: also_adequate
                                .and_then(|text| serde_json::from_str(&text).ok())
                                .unwrap_or_default(),
                            pinned: row.get::<_, i64>(6)? == 1,
                            refusal_code: row.get(7)?,
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
    use crate::dev_descriptor;
    use serde_json::json;
    use striatum_core::{DecisionInput, RouteStateStore};

    const NOW: i64 = 1_800_000_000_000;

    fn store(label: &str) -> SqliteRouteStateStore {
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "routing-diagnostics-store-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        SqliteRouteStateStore::open(&dev_descriptor(
            "routing-diagnostics-store-test",
            dir.join("store.db").to_string_lossy().into_owned(),
        ))
        .unwrap()
    }

    fn execute(store: &SqliteRouteStateStore, sql: &str) {
        store
            .store
            .with_conn(|c: &Connection| c.execute_batch(sql))
            .unwrap();
    }

    fn decision(store: &SqliteRouteStateStore, input: DecisionInput, created_at: i64) -> String {
        let id = store.record_decision(&input).unwrap();
        store
            .store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET created_at = ?2 WHERE id = ?1",
                    params![id, created_at],
                )
            })
            .unwrap();
        id
    }

    #[test]
    fn route_walls_list_only_deadlines_still_ahead() {
        let store = store("walls");
        execute(
            &store,
            &format!(
                "INSERT INTO model_route_state (route_key, capacity_key, provider_id, model_id,
                   cooldown_until, quota_exhausted_until, failure_backoff_until, last_error,
                   last_error_at, wall_class, updated_at) VALUES
                 ('p/live', 'p/live', 'p', 'live', NULL, {ahead}, NULL, 'quota', {NOW}, 'billing', 0),
                 ('p/backoff', 'p/backoff', 'p', 'backoff', NULL, NULL, {ahead}, NULL, NULL, NULL, 0),
                 ('p/passed', 'p/passed', 'p', 'passed', {NOW}, {NOW}, {NOW}, NULL, NULL, NULL, 0);",
                ahead = NOW + 60_000
            ),
        );
        let walls = store.diagnostic_route_walls(NOW).unwrap();
        let keys: Vec<&str> = walls.iter().map(|wall| wall.route_key.as_str()).collect();
        assert_eq!(keys, ["p/backoff", "p/live"]);
        assert_eq!(walls[1].wall_class, Some(RouteWallClass::Billing));
        assert_eq!(walls[1].last_error_at, Some(NOW));
    }

    #[test]
    fn holds_blocks_and_key_walls_list_only_what_stands_now() {
        let store = store("holds");
        execute(
            &store,
            &format!(
                "INSERT INTO account_hold (decision_id, principal, conversation_hash, provider,
                   pin_id, pin_generation, account_id, credential_id, basis, reset_at,
                   retry_after_s, created_at) VALUES
                 ('d-live', 'p', 'h', 'prov', 'pin', 1, 'acct', 'cred', 'reset_soon', {reset}, 30, {live}),
                 ('d-over', 'p', 'h', 'prov', 'pin', 1, 'acct', 'cred', 'reset_soon', {reset}, 30, {over});
                 INSERT INTO account_fence (fence_id, provider, account_id, scope_json, fence_json,
                   source, observed_at, principal, created_at, cleared_at, pool_key_json) VALUES
                 ('f-standing', 'prov', 'acct', '{{}}', '{{}}', 'quota', {NOW}, 'p', {NOW}, NULL, NULL),
                 ('f-cleared', 'prov', 'acct', '{{}}', '{{}}', 'quota', {NOW}, 'p', {NOW}, {NOW}, NULL);
                 INSERT INTO pool_key_state (provider, credential_id, walled_until, wall_reason,
                   cooldown_until, cooldown_reason, updated_at) VALUES
                 ('anthropic-platform', 'k-walled', {ahead}, 'billing', NULL, NULL, {NOW}),
                 ('anthropic-platform', 'k-clear', {NOW}, 'billing', {NOW}, 'rate', {NOW});",
                reset = NOW + 600_000,
                live = NOW - 10_000,
                over = NOW - 40_000,
                ahead = NOW + 60_000,
            ),
        );
        let holds = store.diagnostic_account_holds(NOW).unwrap();
        assert_eq!(holds.len(), 1, "{holds:?}");
        assert_eq!(holds[0].created_at, NOW - 10_000);
        let blocks = store.diagnostic_account_blocks().unwrap();
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.fence_id.as_str())
                .collect::<Vec<_>>(),
            ["f-standing"]
        );
        let keys = store.diagnostic_key_walls(NOW).unwrap();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert_eq!(keys[0].credential_id, "k-walled");
    }

    #[test]
    fn decisions_carry_the_window_iq_shortlist_and_the_wall_behind_a_refusal() {
        let store = store("decisions");
        let served = decision(
            &store,
            DecisionInput {
                consumer: Some("mason".into()),
                target_agent: "mason".into(),
                requirements_json: json!({"iq": 45, "eq": 0}).to_string(),
                selected_route_key: Some("p/m".into()),
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("m".into()),
                score_json: json!({"alsoAdequate": ["q/n:high", "r/o"]}).to_string(),
                rejected_json: "[]".into(),
                ..Default::default()
            },
            NOW - 1_000,
        );
        // The catalog's unscored routes outnumber the wall, but the wall is
        // what refused the request.
        let refused = decision(
            &store,
            DecisionInput {
                target_agent: "sidekick".into(),
                requirements_json: json!({"iq": 30}).to_string(),
                score_json: "null".into(),
                rejected_json: json!([
                    {"route_key": "a/1", "reason": "unscored_awaiting_placement"},
                    {"route_key": "a/2", "reason": "unscored_awaiting_placement"},
                    {"route_key": "a/3", "reason": "unscored_awaiting_placement"},
                    {"route_key": "b/1", "reason": "no_eligible_account"},
                    {"route_key": "b/2", "reason": "no_eligible_account"},
                    {"route_key": "c/1", "reason": "not_available"},
                ])
                .to_string(),
                explanation_json: Some(json!({"failureReason": "no_available_model"}).to_string()),
                ..Default::default()
            },
            NOW - 500,
        );
        let only_static = decision(
            &store,
            DecisionInput {
                target_agent: "lab".into(),
                requirements_json: "{}".into(),
                score_json: "null".into(),
                rejected_json: json!([{"route_key": "a/1", "reason": "excluded_by_operator"}])
                    .to_string(),
                explanation_json: Some(
                    json!({"failureReason": "all_routes_inadequate"}).to_string(),
                ),
                ..Default::default()
            },
            NOW - 400,
        );
        decision(
            &store,
            DecisionInput {
                target_agent: "mason".into(),
                requirements_json: "{}".into(),
                score_json: "null".into(),
                rejected_json: "[]".into(),
                ..Default::default()
            },
            NOW - 10_000_000,
        );
        let rows = store.diagnostic_decisions(NOW - 60_000, NOW, 100).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            [served.as_str(), refused.as_str(), only_static.as_str()],
            "only the window, oldest first"
        );
        assert_eq!(rows[0].consumer, "mason");
        assert_eq!(rows[0].selected_model.as_deref(), Some("p/m"));
        assert_eq!(rows[0].iq, Some(45.0));
        assert_eq!(rows[0].also_adequate, ["q/n:high", "r/o"]);
        assert_eq!(rows[0].refusal_code, None);
        assert_eq!(rows[1].consumer, "sidekick");
        assert_eq!(rows[1].selected_model, None);
        assert_eq!(rows[1].refusal_code.as_deref(), Some("no_eligible_account"));
        assert_eq!(
            rows[2].refusal_code.as_deref(),
            Some("all_routes_inadequate"),
            "with only catalog rejections the scorer's reason names the refusal"
        );
        let newest = store.diagnostic_decisions(NOW - 60_000, NOW, 1).unwrap();
        assert_eq!(newest.len(), 1);
        assert_eq!(newest[0].id, only_static, "the limit keeps the newest rows");
    }

    /// Both pin spellings mark a decision pinned, so the thin-candidate check
    /// skips them: an Athena seat admitting its chosen model is not a thin set.
    #[test]
    fn head_pins_and_admission_only_pins_read_as_pinned() {
        let store = store("pins");
        let served = |explanation: &str, at: i64| {
            decision(
                &store,
                DecisionInput {
                    consumer: Some("athena".into()),
                    target_agent: "athena".into(),
                    requirements_json: json!({"iq": 0}).to_string(),
                    selected_route_key: Some("openai/gpt-6-luna".into()),
                    selected_provider_id: Some("openai".into()),
                    selected_model_id: Some("gpt-6-luna".into()),
                    score_json: "null".into(),
                    rejected_json: "[]".into(),
                    explanation_json: Some(json!([explanation]).to_string()),
                    ..Default::default()
                },
                at,
            )
        };
        let admission = served("admission-only pin: openai/gpt-6-luna", NOW - 300);
        let head = served(
            "model pinned by head: openai/gpt-6-luna; hard admission only",
            NOW - 200,
        );
        let chosen = served("route openai/gpt-6-luna for agent athena", NOW - 100);
        let rows = store.diagnostic_decisions(NOW - 60_000, NOW, 100).unwrap();
        let pinned = |id: &str| rows.iter().find(|row| row.id == id).unwrap().pinned;
        assert!(pinned(&admission));
        assert!(pinned(&head));
        assert!(!pinned(&chosen), "a scored choice is not a pin");
    }
}
