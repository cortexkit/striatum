//! Closed-task samples for background profile refreshes. Do not run this query
//! during store initialization or route selection.

use rusqlite::{params, Connection};
use striatum_core::RouteStateResult;

use super::{err, SqliteRouteStateStore};

const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

/// Token sums across a task's usage rows. `None` means the dimension was never
/// reported; an authoritative zero remains `Some(0.0)`.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskTokenTotals {
    pub fresh_input: Option<f64>,
    pub cached_read: Option<f64>,
    pub cache_write: Option<f64>,
    pub output_excluding_reasoning: Option<f64>,
    pub reasoning: Option<f64>,
}

/// One retained task, keyed by its recorded workload and opaque task reference.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedTaskUsage {
    pub workload: String,
    pub task_ref: String,
    /// Serving provider from usage, absent when several providers served the task.
    pub provider_id: Option<String>,
    /// The task's model pair, not its serving route/provider. Both fields are
    /// absent for multi-model tasks; unselected decisions do not name a model.
    pub selected_model_id: Option<String>,
    pub selected_variant: Option<String>,
    /// `floor(iq / 10)` from the earliest decision; absent for unlabelled demands.
    pub iq_bucket: Option<i64>,
    /// Exclude these tasks from every profile population, including fleet donors.
    pub multi_model: bool,
    pub multi_provider: bool,
    pub totals: TaskTokenTotals,
    pub closed_at_ms: i64,
}

/// Reporting presence across all usage rows in the profile window, including
/// untagged and still-open tasks. A reported zero counts as a report.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderTokenReporting {
    pub provider_id: String,
    pub reports_cache_write: bool,
}

const PROVIDER_TOKEN_REPORTING_SQL: &str = "
SELECT provider_id, MAX(cache_write_tokens IS NOT NULL)
FROM model_route_usage INDEXED BY model_route_usage_observed_idx
WHERE observed_at > ?1 AND observed_at <= ?2
GROUP BY provider_id";

// Find candidate tasks through the time index before reading their full history.
// Windowing the sums themselves would lose old executions of recent tasks; the
// final MAX must also see newer follow-ups that reopen a candidate task. Aggregate
// before joining decisions so multiple executions cannot multiply token sums.
const CLOSED_TASK_USAGE_SQL: &str = "
WITH candidate_tasks AS (
    SELECT task_workload, task_id
    FROM model_route_usage INDEXED BY model_route_usage_observed_idx
    WHERE observed_at > ?1 AND observed_at <= ?2
      AND task_id IS NOT NULL AND task_workload IS NOT NULL
), usage_totals AS (
    SELECT task_workload AS workload, task_id,
           SUM(CAST(input_tokens AS REAL)) AS fresh_input,
           SUM(CAST(cached_read_tokens AS REAL)) AS cached_read,
           SUM(CAST(cache_write_tokens AS REAL)) AS cache_write,
           SUM(CASE WHEN output_tokens IS NOT NULL
                    THEN MAX(CAST(output_tokens AS REAL) - COALESCE(reasoning_tokens, 0), 0)
               END) AS output_excluding_reasoning,
           SUM(CAST(reasoning_tokens AS REAL)) AS reasoning,
           MAX(observed_at) AS closed_at,
           MIN(provider_id) AS provider_id,
           COUNT(DISTINCT provider_id) > 1 AS multi_provider
    FROM model_route_usage INDEXED BY model_route_usage_task_idx
    WHERE task_id IS NOT NULL AND task_workload IS NOT NULL
      AND (task_workload, task_id) IN (SELECT task_workload, task_id FROM candidate_tasks)
    GROUP BY task_workload, task_id
    HAVING MAX(observed_at) > ?1 AND MAX(observed_at) <= ?2
), decisions AS (
    SELECT task_id, COALESCE(consumer, selected_workload, target_agent) AS workload,
           selected_model_id, selected_variant, requirements_json,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(consumer, selected_workload, target_agent), task_id
               ORDER BY created_at, id
           ) AS ordinal
    FROM model_route_decisions INDEXED BY model_route_decisions_task_idx
    WHERE task_id IN (SELECT task_id FROM usage_totals)
), model_counts AS (
    SELECT workload, task_id,
           MIN(selected_model_id) AS selected_model_id,
           MIN(CASE WHEN selected_model_id IS NOT NULL THEN selected_variant END) AS selected_variant,
           COUNT(DISTINCT CASE WHEN selected_model_id IS NOT NULL
                               THEN json_array(selected_model_id, selected_variant)
                          END) > 1 AS multi_model
    FROM decisions GROUP BY workload, task_id
)
SELECT u.workload, u.task_id,
       CASE WHEN NOT m.multi_model THEN m.selected_model_id END,
       CASE WHEN NOT m.multi_model THEN m.selected_variant END,
       d.requirements_json,
       m.multi_model, u.fresh_input, u.cached_read, u.cache_write,
       u.output_excluding_reasoning, u.reasoning, u.closed_at,
       CASE WHEN NOT u.multi_provider THEN u.provider_id END, u.multi_provider
FROM usage_totals u
JOIN decisions d ON d.task_id = u.task_id AND d.workload = u.workload AND d.ordinal = 1
JOIN model_counts m ON m.task_id = u.task_id AND m.workload = u.workload
ORDER BY u.workload, u.task_id";

impl SqliteRouteStateStore {
    fn with_profile_usage_read<T>(
        &self,
        read: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        // These scans can take seconds. A pooled read-only connection keeps the
        // request writer mutex free even when the refresh runs on a blocking thread.
        self.store.with_read(read).map_err(err)
    }

    /// Background-only reporting scan, bounded by the existing observed-time index.
    /// Providers with no rows in `(now - 30 days, now]` are not returned.
    pub fn provider_token_reporting(
        &self,
        now_ms: i64,
    ) -> RouteStateResult<Vec<ProviderTokenReporting>> {
        self.with_profile_usage_read(|c: &Connection| {
            let mut stmt = c.prepare(PROVIDER_TOKEN_REPORTING_SQL)?;
            let rows =
                stmt.query_map(params![now_ms.saturating_sub(30 * DAY_MS), now_ms], |row| {
                    Ok(ProviderTokenReporting {
                        provider_id: row.get(0)?,
                        reports_cache_write: row.get(1)?,
                    })
                })?;
            rows.collect()
        })
    }

    /// Recompute samples from window candidates and their retained history.
    /// A task closes 24 hours after its last
    /// usage and is returned when that usage is in `(now - 30 days, now - 24h]`.
    /// Later usage reopens it; later decisions can change its multi-model marker.
    /// This potentially large read belongs in the background refresh, not boot.
    pub fn closed_task_usage(&self, now_ms: i64) -> RouteStateResult<Vec<ClosedTaskUsage>> {
        self.with_profile_usage_read(|c: &Connection| {
            let mut stmt = c.prepare(CLOSED_TASK_USAGE_SQL)?;
            let rows = stmt.query_map(
                params![
                    now_ms.saturating_sub(30 * DAY_MS),
                    now_ms.saturating_sub(DAY_MS)
                ],
                |row| {
                    Ok(ClosedTaskUsage {
                        workload: row.get(0)?,
                        task_ref: row.get(1)?,
                        provider_id: row.get(12)?,
                        selected_model_id: row.get(2)?,
                        selected_variant: row.get(3)?,
                        iq_bucket: serde_json::from_str::<serde_json::Value>(
                            &row.get::<_, String>(4)?,
                        )
                        .ok()
                        .and_then(|value| value.get("iq").and_then(|iq| iq.as_f64()))
                        .map(|iq| (iq / 10.0).floor() as i64),
                        multi_model: row.get(5)?,
                        multi_provider: row.get(13)?,
                        totals: TaskTokenTotals {
                            fresh_input: row.get(6)?,
                            cached_read: row.get(7)?,
                            cache_write: row.get(8)?,
                            output_excluding_reasoning: row.get(9)?,
                            reasoning: row.get(10)?,
                        },
                        closed_at_ms: row.get(11)?,
                    })
                },
            )?;
            rows.collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_descriptor;
    use striatum_core::{DecisionInput, RouteStateStore, UsageObservation};

    const NOW: i64 = 100 * DAY_MS;

    fn descriptor(label: &str) -> cortexkit_store_types::StorageDescriptor {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "routing-task-usage-{label}-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dev_descriptor(label, path.to_string_lossy())
    }

    fn store(label: &str) -> SqliteRouteStateStore {
        SqliteRouteStateStore::open(&descriptor(label)).unwrap()
    }

    fn decision(
        store: &SqliteRouteStateStore,
        id: &str,
        task: Option<&str>,
        workload: &str,
        model: (&str, Option<&str>),
        iq: f64,
        at: i64,
    ) {
        let input = DecisionInput {
            decision_id: Some(id.into()),
            consumer: Some(workload.into()),
            target_agent: "generic-worker".into(),
            selected_model_id: Some(model.0.into()),
            selected_variant: model.1.map(str::to_owned),
            requirements_json: serde_json::json!({"iq": iq}).to_string(),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            ..Default::default()
        };
        store
            .record_decision_with_task_ref(&input, Some("broca"), None, task)
            .unwrap();
        store
            .store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET created_at=?2 WHERE id=?1",
                    params![id, at],
                )
            })
            .unwrap();
    }

    fn observation(id: &str, at: i64) -> UsageObservation {
        UsageObservation {
            id: id.into(),
            route_key: "provider/model".into(),
            capacity_key: "provider/model".into(),
            provider_id: "provider".into(),
            input_tokens: Some(11.0),
            cached_read_tokens: Some(22.0),
            cache_write_tokens: Some(33.0),
            output_tokens: 55.0,
            reasoning_tokens: Some(44.0),
            duration_ms: 1_000.0,
            // The coarse class stays independent of the decision's consumer.
            workload_class: Some("ephemeral".into()),
            account_key: None,
            observed_at: at as f64,
        }
    }

    fn usage(store: &SqliteRouteStateStore, id: &str, decision: &str, at: i64) {
        store
            .record_usage_delivery_for_decision(&observation(id, at), decision)
            .unwrap();
    }

    #[test]
    fn request_write_completes_while_profile_reader_is_paused() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let store = Arc::new(store("paused-profile-reader"));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let refresh_store = Arc::clone(&store);
        let refresh = std::thread::spawn(move || {
            refresh_store.with_profile_usage_read(|connection| {
                connection.query_row("SELECT COUNT(*) FROM model_route_usage", [], |_| Ok(()))?;
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let request = std::thread::spawn(move || {
            request_tx
                .send(store.record_usage_delivery(&observation("foreground", NOW)))
                .unwrap();
        });
        let response = request_rx.recv_timeout(Duration::from_millis(500));
        release_tx.send(()).unwrap();
        refresh.join().unwrap().unwrap();
        request.join().unwrap();
        assert!(
            response
                .expect("request waited for the paused profile reader")
                .unwrap()
                .usage_recorded
        );
    }

    #[test]
    fn both_profile_queries_complete_while_request_connection_is_held() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let store = Arc::new(store("profile-query-readers"));
        let request_store = Arc::clone(&store);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let request = std::thread::spawn(move || {
            request_store.store.with_conn(|_| {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (refresh_tx, refresh_rx) = mpsc::channel();
        let refresh = std::thread::spawn(move || {
            refresh_tx
                .send((
                    store.closed_task_usage(NOW),
                    store.provider_token_reporting(NOW),
                ))
                .unwrap();
        });
        let response = refresh_rx.recv_timeout(Duration::from_millis(500));
        release_tx.send(()).unwrap();
        request.join().unwrap().unwrap();
        refresh.join().unwrap();
        let (tasks, reporting) = response.expect("profile queries took the request connection");
        assert!(tasks.unwrap().is_empty());
        assert!(reporting.unwrap().is_empty());
    }

    #[test]
    fn closed_task_usage_work_does_not_grow_with_unrelated_expired_tasks() {
        let store = store("bounded-profile-query");
        decision(
            &store,
            "d",
            Some("recent"),
            "mason",
            ("sol", None),
            75.0,
            NOW - 3 * DAY_MS,
        );
        usage(&store, "u", "d", NOW - 2 * DAY_MS);
        let steps = || {
            store
                .store
                .with_conn(|connection| {
                    let mut statement = connection.prepare(CLOSED_TASK_USAGE_SQL)?;
                    let mut rows = statement.query(params![NOW - 30 * DAY_MS, NOW - DAY_MS])?;
                    let mut count = 0;
                    while rows.next()?.is_some() {
                        count += 1;
                    }
                    drop(rows);
                    assert_eq!(count, 1);
                    Ok(statement.get_status(rusqlite::StatementStatus::VmStep))
                })
                .unwrap()
        };
        let before = steps();
        store.store.with_conn(|connection| {
            connection.execute(
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<20000)
                 INSERT INTO model_route_usage (id, task_id, task_workload, route_key, capacity_key, provider_id, observed_at)
                 SELECT 'expired-'||x, 'expired-'||x, 'mason', 'provider/model', 'provider/model', 'provider', ?1 FROM n",
                params![NOW - 40 * DAY_MS],
            )
        }).unwrap();
        let after = steps();
        assert!(
            after <= before + 1000,
            "expired tasks increased VM steps from {before} to {after}"
        );
    }

    #[test]
    fn closed_task_provider_comes_from_usage_and_multiple_providers_are_marked() {
        let store = store("task-provider");
        decision(
            &store,
            "d",
            Some("task"),
            "mason",
            ("sol", None),
            75.0,
            NOW - 3 * DAY_MS,
        );
        usage(&store, "u1", "d", NOW - 2 * DAY_MS);
        let rows = store.closed_task_usage(NOW).unwrap();
        assert_eq!(rows[0].provider_id.as_deref(), Some("provider"));
        assert!(!rows[0].multi_provider);
        let mut second = observation("u2", NOW - 2 * DAY_MS);
        second.provider_id = "other-provider".into();
        store
            .record_usage_delivery_for_decision(&second, "d")
            .unwrap();
        let rows = store.closed_task_usage(NOW).unwrap();
        assert_eq!(rows[0].provider_id, None);
        assert!(rows[0].multi_provider);
        assert!(!rows[0].multi_model);
    }

    #[test]
    fn provider_reporting_uses_all_window_rows_and_distinguishes_absence_from_zero() {
        let store = store("provider-reporting");
        for (id, provider, at, cache_write) in [
            ("absent", "old-only", NOW - 30 * DAY_MS, Some(5.0)),
            ("future", "future-only", NOW + 1, Some(5.0)),
            ("none", "no-counter", NOW, None),
            ("old-counter", "no-counter", NOW - 31 * DAY_MS, Some(5.0)),
            ("zero", "zero-counter", NOW - DAY_MS, Some(0.0)),
            ("mixed-none", "mixed", NOW - DAY_MS, None),
            ("mixed-some", "mixed", NOW, Some(7.0)),
        ] {
            let mut row = observation(id, at);
            row.provider_id = provider.into();
            row.cache_write_tokens = cache_write;
            store.record_usage_delivery(&row).unwrap();
        }
        let rows = store.provider_token_reporting(NOW).unwrap();
        assert_eq!(
            rows,
            vec![
                ProviderTokenReporting {
                    provider_id: "mixed".into(),
                    reports_cache_write: true
                },
                ProviderTokenReporting {
                    provider_id: "no-counter".into(),
                    reports_cache_write: false
                },
                ProviderTokenReporting {
                    provider_id: "zero-counter".into(),
                    reports_cache_write: true
                },
            ]
        );
    }

    #[test]
    fn provider_reporting_query_plan_searches_the_existing_time_index() {
        let store = store("provider-reporting-plan");
        let details: Vec<String> = store
            .store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(&format!(
                    "EXPLAIN QUERY PLAN {PROVIDER_TOKEN_REPORTING_SQL}"
                ))?;
                let rows = stmt.query_map(params![NOW - 30 * DAY_MS, NOW], |row| row.get(3))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        assert!(details.iter().any(|line| line.contains("SEARCH model_route_usage USING INDEX model_route_usage_observed_idx (observed_at>? AND observed_at<?)")), "{details:?}");
        assert!(
            !details
                .iter()
                .any(|line| line.starts_with("SCAN model_route_usage")),
            "{details:?}"
        );
    }

    #[test]
    fn closed_task_usage_query_plan_searches_window_then_task_history_and_decisions() {
        let store = store("task-query-plan");
        decision(
            &store,
            "d",
            Some("task"),
            "mason",
            ("sol", None),
            45.0,
            NOW - 3 * DAY_MS,
        );
        usage(&store, "u", "d", NOW - 2 * DAY_MS);
        let details: Vec<String> = store
            .store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(&format!("EXPLAIN QUERY PLAN {CLOSED_TASK_USAGE_SQL}"))?;
                let rows =
                    stmt.query_map(params![NOW - 30 * DAY_MS, NOW - DAY_MS], |row| row.get(3))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        assert!(
            details.iter().any(|detail| detail.contains("SEARCH model_route_usage USING INDEX model_route_usage_observed_idx (observed_at>? AND observed_at<?)")),
            "candidates must be bounded by observed time: {details:?}"
        );
        assert!(
            details.iter().any(|detail| detail.contains("SEARCH model_route_usage USING INDEX model_route_usage_task_idx (task_workload=? AND task_id=?)")),
            "history must be probed by candidate workload and task: {details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.starts_with("SCAN model_route_usage")),
            "usage must not scan the entire table or task index: {details:?}"
        );
        assert!(
            details.iter().any(|detail| {
                detail.contains(
                    "SEARCH model_route_decisions USING INDEX model_route_decisions_task_idx",
                ) && detail.contains("task_id=?")
            }),
            "decisions must be probed by the closed cohort's task ids: {details:?}"
        );
    }

    #[test]
    fn migration_069_preserves_historical_usage_without_task_backfill() {
        let descriptor = descriptor("migration-069");
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 68).unwrap();
        old.store.with_conn(|c: &Connection| {
            // Seed the old decision directly; today's decision writer hashes
            // the catalog using model_facts columns absent from schema 68.
            c.execute(
                "INSERT INTO model_route_decisions (id,task_id,consumer,target_agent,
                   selected_model_id,requirements_json,score_json,rejected_json,
                   anchor_version,created_at,selected_harness)
                 VALUES ('legacy','legacy-task','mason','generic-worker','sol',
                   '{\"iq\":45.0}','{}','[]',1,?1,'broca')",
                params![NOW - 3 * DAY_MS],
            )?;
            c.execute(
                "INSERT INTO model_route_usage
                   (id, task_id, route_key, capacity_key, provider_id, input_tokens,
                    output_tokens, observed_at, workload_class)
                 VALUES ('legacy', NULL, 'provider/model', 'provider/model', 'provider', 11, 55, ?1, 'ephemeral')",
                params![NOW - 2 * DAY_MS],
            )
        }).unwrap();
        drop(old);
        let started = std::time::Instant::now();
        let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        let row: (Option<String>, Option<String>, String, i64, i64) = upgraded.store.with_conn(|c: &Connection| {
            c.query_row("SELECT task_id, task_workload, workload_class, input_tokens, output_tokens FROM model_route_usage WHERE id='legacy'", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
        }).unwrap();
        assert_eq!(row, (None, None, "ephemeral".into(), 11, 55));
        assert_eq!(
            upgraded
                .get_decisions_for_task("legacy-task")
                .unwrap()
                .len(),
            1
        );
        assert!(upgraded.closed_task_usage(NOW).unwrap().is_empty());
        usage(&upgraded, "new", "legacy", NOW - 2 * DAY_MS);
        assert_eq!(upgraded.closed_task_usage(NOW).unwrap().len(), 1);
        drop(upgraded);
        SqliteRouteStateStore::open(&descriptor).unwrap();
    }

    #[test]
    fn task_ref_is_copied_from_decision_and_redelivery_is_idempotent() {
        let store = store("task-ref");
        decision(
            &store,
            "d",
            Some("wi:one"),
            "mason",
            ("sol", None),
            45.0,
            NOW - 3 * DAY_MS,
        );
        assert_eq!(store.get_decisions_for_task("wi:one").unwrap().len(), 1);
        let obs = observation("u", NOW - 2 * DAY_MS);
        store.record_usage_delivery(&obs).unwrap();
        assert!(store.closed_task_usage(NOW).unwrap().is_empty());
        store.record_usage_delivery_for_decision(&obs, "d").unwrap();
        store.record_usage_delivery_for_decision(&obs, "d").unwrap();
        decision(
            &store,
            "other",
            Some("wi:other"),
            "sidekick",
            ("opus", None),
            75.0,
            NOW,
        );
        store
            .record_usage_delivery_for_decision(&obs, "other")
            .unwrap();
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_ref, "wi:one");
        assert_eq!(tasks[0].workload, "mason");
        assert_eq!(tasks[0].totals.fresh_input, Some(11.0));
        let stored: (String, String, String) = store
            .store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT task_id, task_workload, workload_class FROM model_route_usage WHERE id='u'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
            })
            .unwrap();
        assert_eq!(
            stored,
            ("wi:one".into(), "mason".into(), "ephemeral".into())
        );
        let class = store
            .get_class_vector("ephemeral", NOW as f64)
            .unwrap()
            .unwrap();
        assert_eq!(class.sample_count, 1);
        assert_eq!(class.fresh_input, 11.0);

        decision(&store, "untagged", None, "mason", ("sol", None), 45.0, NOW);
        usage(&store, "anonymous", "untagged", NOW - 2 * DAY_MS);
        assert_eq!(store.closed_task_usage(NOW).unwrap(), tasks);
        assert!(store
            .record_usage_delivery_for_decision(&observation("unknown", NOW), "missing")
            .is_err());
    }

    #[test]
    fn executions_and_follow_up_sum_one_task_using_earliest_decision_bucket() {
        let store = store("executions");
        // The first decision has no usage and predates the profile window.
        decision(
            &store,
            "first",
            Some("wi:one"),
            "mason",
            ("sol", Some("high")),
            45.9,
            NOW - 40 * DAY_MS,
        );
        decision(
            &store,
            "execution-1",
            Some("wi:one"),
            "mason",
            ("sol", Some("high")),
            75.0,
            NOW - 35 * DAY_MS,
        );
        decision(
            &store,
            "execution-2",
            Some("wi:one"),
            "mason",
            ("sol", Some("high")),
            75.0,
            NOW - 4 * DAY_MS,
        );
        usage(&store, "run-1", "execution-1", NOW - 35 * DAY_MS);
        usage(&store, "run-2", "execution-2", NOW - 3 * DAY_MS);
        usage(&store, "follow-up", "execution-2", NOW - 2 * DAY_MS);
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        assert_eq!(task.iq_bucket, Some(4));
        assert_eq!(task.selected_model_id.as_deref(), Some("sol"));
        assert_eq!(task.selected_variant.as_deref(), Some("high"));
        assert!(!task.multi_model);
        assert_eq!(task.closed_at_ms, NOW - 2 * DAY_MS);
        assert_eq!(
            task.totals,
            TaskTokenTotals {
                fresh_input: Some(33.0),
                cached_read: Some(66.0),
                cache_write: Some(99.0),
                output_excluding_reasoning: Some(33.0),
                reasoning: Some(132.0),
            }
        );
    }

    #[test]
    fn consult_roles_under_one_attempt_are_separate_tasks() {
        let store = store("consult-roles");
        decision(
            &store,
            "critic",
            Some("attempt:one"),
            "consult.critic",
            ("sol", None),
            45.0,
            NOW - 3 * DAY_MS,
        );
        decision(
            &store,
            "designer",
            Some("attempt:one"),
            "consult.designer",
            ("opus", None),
            75.0,
            NOW - 3 * DAY_MS,
        );
        usage(&store, "critic-run", "critic", NOW - 2 * DAY_MS);
        usage(&store, "designer-run", "designer", NOW - 2 * DAY_MS);
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.workload.as_str())
                .collect::<Vec<_>>(),
            ["consult.critic", "consult.designer"]
        );
        assert!(tasks
            .iter()
            .all(|task| !task.multi_model && task.totals.fresh_input == Some(11.0)));
        assert_eq!(tasks[0].iq_bucket, Some(4));
        assert_eq!(tasks[1].iq_bucket, Some(7));
    }

    #[test]
    fn late_follow_up_reopens_then_updates_totals_and_model_pair_marker() {
        for (label, new_model) in [
            ("model", ("opus", None)),
            ("variant", ("sol", Some("high"))),
        ] {
            let store = store(label);
            decision(
                &store,
                "d1",
                Some("wi:late"),
                "mason",
                ("sol", None),
                45.0,
                NOW - 3 * DAY_MS,
            );
            usage(&store, "initial", "d1", NOW - 2 * DAY_MS);
            assert!(!store.closed_task_usage(NOW).unwrap()[0].multi_model);

            usage(&store, "follow-up", "d1", NOW);
            assert!(store.closed_task_usage(NOW).unwrap().is_empty());
            let tasks = store.closed_task_usage(NOW + DAY_MS).unwrap();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].totals.fresh_input, Some(22.0));
            assert_eq!(tasks[0].closed_at_ms, NOW);
            assert!(!tasks[0].multi_model);

            decision(
                &store,
                "d2",
                Some("wi:late"),
                "mason",
                new_model,
                75.0,
                NOW + DAY_MS,
            );
            // Even a model-changing decision with no usage disqualifies the task.
            assert!(store.closed_task_usage(NOW + DAY_MS).unwrap()[0].multi_model);
            usage(&store, "other-model-follow-up", "d2", NOW + DAY_MS);
            assert!(store.closed_task_usage(NOW).unwrap().is_empty());
            assert!(store.closed_task_usage(NOW + DAY_MS).unwrap().is_empty());
            let tasks = store.closed_task_usage(NOW + 2 * DAY_MS).unwrap();
            assert_eq!(tasks.len(), 1);
            assert!(tasks[0].multi_model);
            assert_eq!(tasks[0].totals.fresh_input, Some(33.0));
            assert_eq!(tasks[0].iq_bucket, Some(4));
        }
    }

    #[test]
    fn closed_tasks_obey_exact_close_and_window_boundaries() {
        let store = store("boundaries");
        assert!(store.closed_task_usage(NOW).unwrap().is_empty());
        for (id, at) in [
            ("close", NOW - DAY_MS),
            ("open", NOW - DAY_MS + 1),
            ("window", NOW - 30 * DAY_MS),
            ("inside", NOW - 30 * DAY_MS + 1),
            ("future", NOW + 1),
        ] {
            decision(
                &store,
                id,
                Some(id),
                "mason",
                ("sol", None),
                50.0,
                at - DAY_MS,
            );
            usage(&store, id, id, at);
        }
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.task_ref.as_str())
                .collect::<Vec<_>>(),
            ["close", "inside"]
        );
        assert!(tasks.iter().all(|task| task.iq_bucket == Some(5)));
        assert!(store
            .closed_task_usage(NOW + 31 * DAY_MS)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unselected_decisions_do_not_invent_a_second_model_pair() {
        let store = store("unselected");
        decision(
            &store,
            "refused",
            Some("task"),
            "mason",
            ("sol", None),
            45.0,
            NOW - 4 * DAY_MS,
        );
        store
            .store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET selected_model_id=NULL WHERE id='refused'",
                    [],
                )
            })
            .unwrap();
        decision(
            &store,
            "selected",
            Some("task"),
            "mason",
            ("sol", Some("high")),
            75.0,
            NOW - 3 * DAY_MS,
        );
        usage(&store, "u", "selected", NOW - 2 * DAY_MS);
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(!tasks[0].multi_model);
        assert_eq!(tasks[0].selected_model_id.as_deref(), Some("sol"));
        assert_eq!(tasks[0].selected_variant.as_deref(), Some("high"));
        assert_eq!(tasks[0].iq_bucket, Some(4));
    }

    #[test]
    fn missing_dimensions_and_iq_stay_unknown_and_output_never_goes_negative() {
        let store = store("missing-dimensions");
        decision(
            &store,
            "a",
            Some("task"),
            "sidekick",
            ("sol", None),
            45.0,
            NOW - 3 * DAY_MS,
        );
        decision(
            &store,
            "z",
            Some("task"),
            "sidekick",
            ("sol", None),
            75.0,
            NOW - 3 * DAY_MS,
        );
        let mut obs = observation("u", NOW - 2 * DAY_MS);
        obs.input_tokens = Some(0.0);
        obs.cache_write_tokens = None;
        obs.cached_read_tokens = None;
        obs.reasoning_tokens = Some(100.0);
        store.record_usage_delivery_for_decision(&obs, "z").unwrap();
        let tasks = store.closed_task_usage(NOW).unwrap();
        assert_eq!(tasks[0].iq_bucket, Some(4));
        assert_eq!(
            tasks[0].totals,
            TaskTokenTotals {
                fresh_input: Some(0.0),
                cached_read: None,
                cache_write: None,
                output_excluding_reasoning: Some(0.0),
                reasoning: Some(100.0),
            }
        );
        for json in ["{}", "null", "not json", "{\"iq\":\"45\"}"] {
            store
                .store
                .with_conn(|c: &Connection| {
                    c.execute(
                        "UPDATE model_route_decisions SET requirements_json=?1 WHERE id='a'",
                        params![json],
                    )
                })
                .unwrap();
            assert_eq!(store.closed_task_usage(NOW).unwrap()[0].iq_bucket, None);
        }
        obs.id = "missing-reasoning".into();
        obs.reasoning_tokens = None;
        store.record_usage_delivery_for_decision(&obs, "z").unwrap();
        assert_eq!(
            store.closed_task_usage(NOW).unwrap()[0]
                .totals
                .output_excluding_reasoning,
            Some(55.0)
        );
    }
}
