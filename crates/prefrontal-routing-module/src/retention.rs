//! Routing-store retention: the periodic pass that deletes old rows from the
//! append-only logs in small batches, off the request path.
//!
//! Who reads these rows, and how far back, decides the windows:
//! - `account_decision` (shadow rows): the timer heartbeat reads the newest
//!   row per trigger, provider and family (`account_selection::shadow_pass`),
//!   and `route.account_shadow_status` reads the newest 800 shadow rows.
//!   Nothing reads further back; the windows are for people auditing.
//!   Serve rows are never deleted (see the store's `retention.rs`).
//! - `account_observation`: read back only to recognise a runner re-sending
//!   the same event id, which happens within minutes.
//! - `model_route_decisions`: `route.explain` looks a decision up by id or
//!   send id, the decision list and per-task reads walk recent rows, and the
//!   health sentinel scans the last day.
//! - `model_route_usage`: observed speed and class token profiles read a
//!   trailing seven-day window, so the usage window never goes below that.
//!
//! The windows come from the `retention` block of the routing file, re-read
//! every pass. Deleting rows frees pages inside the database file for reuse;
//! only an offline VACUUM shrinks the file itself, and this pass never runs one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use prefrontal_routing_core::RouteStateStore;
use prefrontal_routing_store::{DecisionRetentionCursor, SqliteRouteStateStore};
use serde_json::Value;

use crate::config_overrides::read_routing_config_value;

/// Time between retention passes.
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(60);
/// Most rows one batch deletes (or, for account decisions, examines). Each
/// batch is one short transaction.
pub const RETENTION_BATCH_ROWS: usize = 256;
/// Most batches one pass runs per table. With the batch size and interval
/// above, a pass can retire about 4,000 rows a minute per table, well above
/// the roughly 260 shadow decisions a minute a busy install writes, so a
/// backlog drains over a few hours without any single long delete.
pub const RETENTION_MAX_BATCHES_PER_TABLE: usize = 16;
/// Pause between batches so request handlers waiting for the store's single
/// connection get it between deletes.
const RETENTION_BATCH_PAUSE: Duration = Duration::from_millis(25);

const HOUR_MS: i64 = 60 * 60_000;
const DAY_MS: i64 = 24 * HOUR_MS;

/// How long each log keeps its rows. Every value is a whole number of the
/// unit in its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionWindows {
    /// Shadow admits and `account_selection_unsupported` refusals.
    pub account_decision_routine_hours: u64,
    /// Shadow holds and every other shadow refusal.
    pub account_decision_notable_days: u64,
    pub account_observation_days: u64,
    pub model_route_decisions_days: u64,
    pub model_route_usage_days: u64,
}

impl Default for RetentionWindows {
    fn default() -> Self {
        Self {
            account_decision_routine_hours: 72,
            account_decision_notable_days: 14,
            account_observation_days: 7,
            model_route_decisions_days: 30,
            model_route_usage_days: 30,
        }
    }
}

/// One configurable window: its key in the `retention` block, the smallest
/// accepted value, and where the parsed value goes.
struct WindowKey {
    key: &'static str,
    minimum: u64,
    field: fn(&mut RetentionWindows) -> &mut u64,
}

const WINDOW_KEYS: [WindowKey; 5] = [
    WindowKey {
        key: "account_decision_routine_hours",
        minimum: 1,
        field: |w| &mut w.account_decision_routine_hours,
    },
    WindowKey {
        key: "account_decision_notable_days",
        minimum: 1,
        field: |w| &mut w.account_decision_notable_days,
    },
    WindowKey {
        key: "account_observation_days",
        minimum: 1,
        field: |w| &mut w.account_observation_days,
    },
    WindowKey {
        key: "model_route_decisions_days",
        minimum: 1,
        field: |w| &mut w.model_route_decisions_days,
    },
    WindowKey {
        // Observed speed and class token profiles average the last seven days
        // of usage; a shorter window would silently starve them.
        key: "model_route_usage_days",
        minimum: 7,
        field: |w| &mut w.model_route_usage_days,
    },
];

/// Parse the routing file's `retention` block. An absent block or key keeps
/// the default; a value that is not a whole number keeps the default, and one
/// below its minimum is raised to the minimum. Each such correction, and each
/// unknown key, is returned as a warning.
pub fn retention_windows_from_value(block: Option<&Value>) -> (RetentionWindows, Vec<String>) {
    let mut windows = RetentionWindows::default();
    let mut warnings = Vec::new();
    let Some(block) = block else {
        return (windows, warnings);
    };
    let Some(object) = block.as_object() else {
        warnings.push("retention must be an object; using the default windows".into());
        return (windows, warnings);
    };
    for (key, value) in object {
        let Some(spec) = WINDOW_KEYS.iter().find(|spec| spec.key == key) else {
            warnings.push(format!("retention.{key} is not a known key; ignored"));
            continue;
        };
        let slot = (spec.field)(&mut windows);
        match value.as_u64() {
            Some(parsed) if parsed >= spec.minimum => *slot = parsed,
            Some(parsed) => {
                warnings.push(format!(
                    "retention.{key} = {parsed} is below its minimum {}; using {}",
                    spec.minimum, spec.minimum
                ));
                *slot = spec.minimum;
            }
            None => warnings.push(format!(
                "retention.{key} must be a whole number; using the default {}",
                *slot
            )),
        }
    }
    (windows, warnings)
}

/// The windows the routing file configures, with any warnings. A missing or
/// unreadable file gives the defaults; errors in other blocks of the file do
/// not affect this one.
pub fn retention_windows_with_warnings(path: &Path) -> (RetentionWindows, Vec<String>) {
    match read_routing_config_value(path) {
        Ok(config) => {
            retention_windows_from_value(config.as_ref().and_then(|value| value.get("retention")))
        }
        Err(error) => (
            RetentionWindows::default(),
            vec![format!(
                "routing config unreadable ({error}); using the default retention windows"
            )],
        ),
    }
}

/// The windows the routing file configures (defaults where it says nothing).
pub fn retention_windows(path: &Path) -> RetentionWindows {
    retention_windows_with_warnings(path).0
}

/// What one pass deleted, per table, and whether every table finished
/// inside its batch budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPassReport {
    pub account_decisions: usize,
    pub account_observations: usize,
    pub route_decisions: usize,
    pub route_usage: usize,
    /// Batches run across all tables.
    pub batches: usize,
    /// False when some table used its whole batch budget and may have more
    /// rows past the window; the next pass continues it.
    pub drained: bool,
}

impl RetentionPassReport {
    pub fn total(&self) -> usize {
        self.account_decisions + self.account_observations + self.route_decisions + self.route_usage
    }
}

/// State carried from one pass to the next: the account-decision scan
/// position, and the windows it was computed under (a change of windows
/// restarts the scan, because rows it skipped may now be due).
#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionState {
    cursor: DecisionRetentionCursor,
    windows: Option<RetentionWindows>,
}

/// The size of one pass: rows per batch, batches per table, and the pause
/// between batches.
#[derive(Debug, Clone, Copy)]
pub struct PassBudget {
    pub batch_rows: usize,
    pub max_batches_per_table: usize,
    pub pause: Duration,
}

impl Default for PassBudget {
    fn default() -> Self {
        Self {
            batch_rows: RETENTION_BATCH_ROWS,
            max_batches_per_table: RETENTION_MAX_BATCHES_PER_TABLE,
            pause: RETENTION_BATCH_PAUSE,
        }
    }
}

/// Run batches of one table until a batch comes back short (nothing more is
/// due), a batch fails, or the budget is spent. `batch` returns (rows
/// deleted, rows examined). Adds the batches it ran to `batches_so_far` and
/// returns (rows deleted, drained).
fn drain(
    table: &str,
    budget: &PassBudget,
    batches_so_far: &mut usize,
    mut batch: impl FnMut() -> Result<(usize, usize), String>,
) -> (usize, bool) {
    let mut deleted = 0;
    for round in 0..budget.max_batches_per_table {
        if round > 0 && !budget.pause.is_zero() {
            std::thread::sleep(budget.pause);
        }
        *batches_so_far += 1;
        match batch() {
            Ok((removed, examined)) => {
                deleted += removed;
                if examined < budget.batch_rows {
                    return (deleted, true);
                }
            }
            Err(error) => {
                tracing::warn!(table, error = %error, "retention batch failed");
                return (deleted, true);
            }
        }
    }
    (deleted, false)
}

fn before(now_ms: i64, window_ms: i64) -> i64 {
    now_ms.saturating_sub(window_ms)
}

fn window_ms(count: u64, unit_ms: i64) -> i64 {
    i64::try_from(count)
        .unwrap_or(i64::MAX)
        .saturating_mul(unit_ms)
}

/// One retention pass over every log, synchronous. Run it on a blocking
/// thread: it sleeps between batches.
pub fn run_retention_pass(
    store: &SqliteRouteStateStore,
    windows: &RetentionWindows,
    state: &mut RetentionState,
    now_ms: i64,
    budget: &PassBudget,
) -> RetentionPassReport {
    if state.windows != Some(*windows) {
        *state = RetentionState {
            cursor: DecisionRetentionCursor::default(),
            windows: Some(*windows),
        };
    }
    let rows = budget.batch_rows;
    let mut report = RetentionPassReport::default();
    let routine_before = before(
        now_ms,
        window_ms(windows.account_decision_routine_hours, HOUR_MS),
    );
    let notable_before = before(
        now_ms,
        window_ms(windows.account_decision_notable_days, DAY_MS),
    );
    let cursor = &mut state.cursor;
    let (decisions, decisions_drained) =
        drain("account_decision", budget, &mut report.batches, || {
            store
                .prune_account_decisions(routine_before, notable_before, cursor, rows)
                .map(|batch| (batch.deleted, batch.examined))
                .map_err(|error| error.0)
        });
    let observed_before = before(now_ms, window_ms(windows.account_observation_days, DAY_MS));
    let (observations, observations_drained) =
        drain("account_observation", budget, &mut report.batches, || {
            store
                .prune_account_observations(observed_before, rows)
                .map(|deleted| (deleted, deleted))
                .map_err(|error| error.0)
        });
    let decided_before = before(
        now_ms,
        window_ms(windows.model_route_decisions_days, DAY_MS),
    );
    let (route_decisions, route_decisions_drained) =
        drain("model_route_decisions", budget, &mut report.batches, || {
            store
                .delete_decisions_older_than(decided_before, rows)
                .map(|deleted| (deleted, deleted))
                .map_err(|error| error.0)
        });
    let usage_before = before(now_ms, window_ms(windows.model_route_usage_days, DAY_MS));
    let (usage, usage_drained) = drain("model_route_usage", budget, &mut report.batches, || {
        store
            .prune_route_usage(usage_before, rows)
            .map(|deleted| (deleted, deleted))
            .map_err(|error| error.0)
    });
    report.account_decisions = decisions;
    report.account_observations = observations;
    report.route_decisions = route_decisions;
    report.route_usage = usage;
    report.drained =
        decisions_drained && observations_drained && route_decisions_drained && usage_drained;
    report
}

/// The periodic retention pass. Each pass re-reads the windows from the
/// routing file and runs on a blocking thread, so selection and admission
/// never wait on it except for one short batch at a time.
pub async fn retention_loop(store: Arc<SqliteRouteStateStore>, config_path: PathBuf) {
    let mut state = RetentionState::default();
    let mut last_warnings: Vec<String> = Vec::new();
    loop {
        // The first pass waits one interval, so it never competes with boot.
        tokio::time::sleep(RETENTION_INTERVAL).await;
        let (windows, warnings) = retention_windows_with_warnings(&config_path);
        if warnings != last_warnings {
            for warning in &warnings {
                tracing::warn!(warning = %warning, "routing retention config");
            }
            last_warnings = warnings;
        }
        let pass_store = Arc::clone(&store);
        let mut pass_state = state;
        let outcome = tokio::task::spawn_blocking(move || {
            let report = run_retention_pass(
                &pass_store,
                &windows,
                &mut pass_state,
                crate::account_selection::now_ms(),
                &PassBudget::default(),
            );
            (report, pass_state)
        })
        .await;
        match outcome {
            Ok((report, next_state)) => {
                state = next_state;
                if report.total() > 0 {
                    tracing::info!(
                        account_decisions = report.account_decisions,
                        account_observations = report.account_observations,
                        route_decisions = report.route_decisions,
                        route_usage = report.route_usage,
                        batches = report.batches,
                        drained = report.drained,
                        "routing retention pass"
                    );
                }
            }
            Err(error) => tracing::warn!(error = %error, "routing retention pass panicked"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_routing_store::{dev_descriptor, AccountDecisionRecord, DECISION_MODE_SHADOW};
    use serde_json::json;

    /// The test clock, 2026-10-12T00:00:00Z.
    const NOW: i64 = 1_791_763_200_000;

    #[test]
    fn an_absent_block_gives_the_documented_defaults() {
        let (windows, warnings) = retention_windows_from_value(None);
        assert_eq!(
            windows,
            RetentionWindows {
                account_decision_routine_hours: 72,
                account_decision_notable_days: 14,
                account_observation_days: 7,
                model_route_decisions_days: 30,
                model_route_usage_days: 30,
            }
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn configured_windows_apply_and_bad_values_fall_back_with_a_warning() {
        let block = json!({
            "account_decision_routine_hours": 24,
            "account_decision_notable_days": 0,
            "account_observation_days": "seven",
            "model_route_decisions_days": 45,
            "model_route_usage_days": 3,
            "account_decision_days": 9,
        });
        let (windows, warnings) = retention_windows_from_value(Some(&block));
        assert_eq!(
            windows,
            RetentionWindows {
                account_decision_routine_hours: 24,
                // Below the one-day minimum: raised to it.
                account_decision_notable_days: 1,
                // Not a number: the default stays.
                account_observation_days: 7,
                model_route_decisions_days: 45,
                // Usage readers average seven days, so seven is the floor.
                model_route_usage_days: 7,
            }
        );
        assert_eq!(warnings.len(), 4, "{warnings:?}");
        assert!(warnings
            .iter()
            .any(|warning| warning.contains("account_decision_days is not a known key")));
    }

    #[test]
    fn a_bad_retention_block_does_not_depend_on_the_rest_of_the_file() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-retention-config-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("alfonso-routing.jsonc");
        // `model_routing.demand_exponent` has the wrong type, which fails the
        // typed routing config; the retention block is still read.
        std::fs::write(
            &path,
            r#"{
                // comment
                "model_routing": { "demand_exponent": "steep" },
                "retention": { "account_decision_routine_hours": 12 }
            }"#,
        )
        .unwrap();
        let windows = retention_windows(&path);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(windows.account_decision_routine_hours, 12);
        assert_eq!(windows.account_decision_notable_days, 14);
    }

    fn shadow_admit(id: String, created_at: i64) -> AccountDecisionRecord {
        AccountDecisionRecord {
            decision_id: id,
            mode: DECISION_MODE_SHADOW.into(),
            trigger: "observation".into(),
            principal: "reserved:broca".into(),
            admission_id: None,
            request_mode: Some("admit".into()),
            provider: "anthropic".into(),
            model: "claude-opus-5-5".into(),
            family: "opus".into(),
            outcome: "admit".into(),
            code: "cold_scored".into(),
            account_id: None,
            credential_id: None,
            policy_version: 2,
            decision_json: "{}".into(),
            context_json: None,
            created_at,
        }
    }

    #[test]
    fn one_pass_deletes_at_most_its_batch_budget_and_the_next_continues() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-retention-pass-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db").to_string_lossy().into_owned();
        let store = SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open store");
        store
            .account_transaction(|tx| {
                for index in 0..20 {
                    tx.insert_decision(&shadow_admit(
                        format!("old-{index:02}"),
                        NOW - 5 * DAY_MS + index,
                    ))?;
                }
                tx.insert_decision(&shadow_admit("young".into(), NOW - HOUR_MS))
            })
            .unwrap();
        let budget = PassBudget {
            batch_rows: 4,
            max_batches_per_table: 3,
            pause: Duration::ZERO,
        };
        let windows = RetentionWindows::default();
        let mut state = RetentionState::default();

        let first = run_retention_pass(&store, &windows, &mut state, NOW, &budget);
        assert_eq!(first.account_decisions, 12);
        assert!(!first.drained);

        let second = run_retention_pass(&store, &windows, &mut state, NOW, &budget);
        assert_eq!(second.account_decisions, 8);
        assert!(second.drained);

        let left: Vec<String> = store
            .account_decisions(DECISION_MODE_SHADOW, 100)
            .unwrap()
            .into_iter()
            .map(|row| row.decision_id)
            .collect();
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(left, vec!["young"]);
    }
}
