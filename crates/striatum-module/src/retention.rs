//! Routing-store retention: the periodic pass that deletes old rows from the
//! append-only logs in small batches, off the request path.
//!
//! Who reads these rows, and how far back, decides the windows:
//! - Account-selection history (`account_admission`, `account_affinity` with
//!   its `account_pin` generations, `account_hold`, and serve
//!   `account_decision` rows): admissions are read back for a duplicate
//!   request, a runner's report and pending demand, all within minutes of the
//!   commit; affinity is live for seven idle days; holds lapse within
//!   minutes. A row is deleted once its last activity is past the
//!   `account_selection_days` window and nothing still in the store
//!   references it (see the store's `retention.rs`).
//! - Shadow `account_decision` rows, left by the retired shadow mode, are
//!   read by nothing and purged whatever their age.
//! - `account_observation`: read back only to recognise a runner re-sending
//!   the same event id, which happens within minutes.
//! - `account_observation_event`: the small key-and-answer record of each
//!   runner event, which recognises a re-sent event even when a backup
//!   restored the observation log empty. Kept for
//!   [`OBSERVATION_EVENT_KEY_WINDOW_MS`], not configurable.
//! - `model_route_decisions`: `route.explain` looks a decision up by id or
//!   send id, the decision list and per-task reads walk recent rows, and the
//!   health sentinel scans the last day. `model_route_parent_failures`, its
//!   per-head failure copy, follows the same window.
//! - `model_route_usage`: observed speed and class token profiles read a
//!   trailing seven-day window, so the usage window never goes below that.
//!
//! The windows come from the `retention` block of the routing file, re-read
//! every pass. Deleting rows frees pages inside the database file for reuse;
//! only an offline VACUUM shrinks the file itself, and this pass never runs one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use striatum_core::RouteStateStore;
use striatum_store::SqliteRouteStateStore;
use serde_json::Value;

use crate::config_overrides::read_routing_config_value;

/// Time between retention passes.
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(60);
/// Most rows one batch deletes. Each batch is one short transaction.
pub const RETENTION_BATCH_ROWS: usize = 256;
/// Most batches one pass runs per table. With the batch size and interval
/// above, a pass can retire about 4,000 rows a minute per table, well above
/// what serving writes, so a backlog drains without any single long delete.
pub const RETENTION_MAX_BATCHES_PER_TABLE: usize = 16;
/// Most batches one pass spends purging retired shadow decisions. On a copy
/// of a 9 GB store holding 1.6 million of them, a batch of 256 took 19 ms on
/// average over the whole purge, and up to about 300 ms while the machine was
/// busy. At 32 batches a pass the purge holds the store for well under a
/// second a minute on average, in short slices, and that backlog clears in
/// about three and a half hours.
pub const SHADOW_PURGE_MAX_BATCHES: usize = 32;
/// Pause between batches so request handlers waiting for the store's single
/// connection get it between deletes.
const RETENTION_BATCH_PAUSE: Duration = Duration::from_millis(25);

const HOUR_MS: i64 = 60 * 60_000;
const DAY_MS: i64 = 24 * HOUR_MS;

/// How long a runner-event key stays in `account_observation_event`. A runner
/// re-sends an event within minutes of the first report, after a lost reply;
/// six hours covers that many times over. Every backup carries this table; on
/// a copy of a long-running install six hours of events came to about 65,000
/// rows (18 MB), where the week the observation log keeps is about 520 MB.
pub const OBSERVATION_EVENT_KEY_WINDOW_MS: i64 = 6 * HOUR_MS;

/// How long each log keeps its rows. Every value is a whole number of the
/// unit in its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionWindows {
    /// Served account-selection history, counted from each row's last
    /// activity.
    pub account_selection_days: u64,
    pub account_observation_days: u64,
    pub model_route_decisions_days: u64,
    pub model_route_usage_days: u64,
}

impl Default for RetentionWindows {
    fn default() -> Self {
        Self {
            account_selection_days: 30,
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

const WINDOW_KEYS: [WindowKey; 4] = [
    WindowKey {
        // A frozen run can report its finish days after its admission, and a
        // conversation stays pinned for seven idle days; a week is the floor.
        key: "account_selection_days",
        minimum: 7,
        field: |w| &mut w.account_selection_days,
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

/// Keys that configured the retired shadow-decision windows. They are named
/// in their own warning, so an operator knows to delete them rather than
/// suspect a typo.
const REMOVED_WINDOW_KEYS: [&str; 2] = [
    "account_decision_routine_hours",
    "account_decision_notable_days",
];

/// Parse the routing file's `retention` block. An absent block or key keeps
/// the default; a value that is not a whole number keeps the default, and one
/// below its minimum is raised to the minimum. Each such correction, each
/// removed key and each unknown key is returned as a warning.
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
        if REMOVED_WINDOW_KEYS.contains(&key.as_str()) {
            warnings.push(format!(
                "retention.{key} was removed with account-selection shadow mode; delete it \
                 (served history uses retention.account_selection_days)"
            ));
            continue;
        }
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
    /// Retired shadow decisions.
    pub shadow_decisions: usize,
    pub account_admissions: usize,
    pub account_holds: usize,
    pub account_affinities: usize,
    /// Pin generations deleted with their conversation's affinity row.
    pub account_pins: usize,
    /// Serve decisions.
    pub account_decisions: usize,
    pub account_observations: usize,
    /// Runner-event keys past [`OBSERVATION_EVENT_KEY_WINDOW_MS`].
    pub observation_events: usize,
    pub route_decisions: usize,
    /// Per-head failure copies whose decision is past the decision window.
    pub parent_failures: usize,
    pub route_usage: usize,
    /// Batches run across all tables.
    pub batches: usize,
    /// False when some table used its whole batch budget and may have more
    /// rows past the window; the next pass continues it.
    pub drained: bool,
}

impl RetentionPassReport {
    pub fn total(&self) -> usize {
        self.shadow_decisions
            + self.account_admissions
            + self.account_holds
            + self.account_affinities
            + self.account_pins
            + self.account_decisions
            + self.account_observations
            + self.observation_events
            + self.route_decisions
            + self.parent_failures
            + self.route_usage
    }
}

/// The size of one pass: rows per batch, batches per table (and for the
/// shadow purge), and the pause between batches.
#[derive(Debug, Clone, Copy)]
pub struct PassBudget {
    pub batch_rows: usize,
    pub max_batches_per_table: usize,
    pub max_shadow_purge_batches: usize,
    pub pause: Duration,
}

impl Default for PassBudget {
    fn default() -> Self {
        Self {
            batch_rows: RETENTION_BATCH_ROWS,
            max_batches_per_table: RETENTION_MAX_BATCHES_PER_TABLE,
            max_shadow_purge_batches: SHADOW_PURGE_MAX_BATCHES,
            pause: RETENTION_BATCH_PAUSE,
        }
    }
}

/// Run up to `max_batches` batches of one table until a batch comes back
/// short (nothing more is due), a batch fails, or the budget is spent.
/// `batch` returns (rows deleted, rows examined). Adds the batches it ran to
/// `batches_so_far` and returns (rows deleted, drained).
fn drain(
    table: &str,
    budget: &PassBudget,
    max_batches: usize,
    batches_so_far: &mut usize,
    mut batch: impl FnMut() -> Result<(usize, usize), String>,
) -> (usize, bool) {
    let mut deleted = 0;
    for round in 0..max_batches {
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
/// thread: it sleeps between batches. Account-selection history goes leaf
/// first (admissions, holds, then affinity with its pins, then the decisions
/// they referenced), so one pass can retire a whole expired conversation.
pub fn run_retention_pass(
    store: &SqliteRouteStateStore,
    windows: &RetentionWindows,
    now_ms: i64,
    budget: &PassBudget,
) -> RetentionPassReport {
    let rows = budget.batch_rows;
    let per_table = budget.max_batches_per_table;
    let mut report = RetentionPassReport::default();
    let batches = &mut report.batches;
    let counted = |result: Result<usize, striatum_core::RouteStateStoreError>| {
        result
            .map(|deleted| (deleted, deleted))
            .map_err(|error| error.0)
    };

    let (shadow, shadow_drained) = drain(
        "account_decision (shadow)",
        budget,
        budget.max_shadow_purge_batches,
        batches,
        || counted(store.purge_shadow_decisions(rows)),
    );
    let history_before = before(now_ms, window_ms(windows.account_selection_days, DAY_MS));
    let (admissions, admissions_drained) =
        drain("account_admission", budget, per_table, batches, || {
            counted(store.prune_account_admissions(history_before, rows))
        });
    let (holds, holds_drained) = drain("account_hold", budget, per_table, batches, || {
        counted(store.prune_account_holds(history_before, rows))
    });
    let mut pins = 0;
    let (affinities, affinities_drained) =
        drain("account_affinity", budget, per_table, batches, || {
            store
                .prune_account_affinities(history_before, rows)
                .map(|batch| {
                    pins += batch.pins;
                    (batch.affinities, batch.affinities)
                })
                .map_err(|error| error.0)
        });
    let (decisions, decisions_drained) =
        drain("account_decision", budget, per_table, batches, || {
            counted(store.prune_serve_decisions(history_before, rows))
        });
    let observed_before = before(now_ms, window_ms(windows.account_observation_days, DAY_MS));
    let (observations, observations_drained) =
        drain("account_observation", budget, per_table, batches, || {
            counted(store.prune_account_observations(observed_before, rows))
        });
    let keys_before = before(now_ms, OBSERVATION_EVENT_KEY_WINDOW_MS);
    let (observation_events, observation_events_drained) = drain(
        "account_observation_event",
        budget,
        per_table,
        batches,
        || counted(store.prune_observation_events(keys_before, rows)),
    );
    let decided_before = before(
        now_ms,
        window_ms(windows.model_route_decisions_days, DAY_MS),
    );
    let (route_decisions, route_decisions_drained) =
        drain("model_route_decisions", budget, per_table, batches, || {
            counted(store.delete_decisions_older_than(decided_before, rows))
        });
    let (parent_failures, parent_failures_drained) = drain(
        "model_route_parent_failures",
        budget,
        per_table,
        batches,
        || counted(store.prune_parent_failures(decided_before, rows)),
    );
    let usage_before = before(now_ms, window_ms(windows.model_route_usage_days, DAY_MS));
    let (usage, usage_drained) = drain("model_route_usage", budget, per_table, batches, || {
        counted(store.prune_route_usage(usage_before, rows))
    });
    report.shadow_decisions = shadow;
    report.account_admissions = admissions;
    report.account_holds = holds;
    report.account_affinities = affinities;
    report.account_pins = pins;
    report.account_decisions = decisions;
    report.account_observations = observations;
    report.observation_events = observation_events;
    report.route_decisions = route_decisions;
    report.parent_failures = parent_failures;
    report.route_usage = usage;
    report.drained = shadow_drained
        && admissions_drained
        && holds_drained
        && affinities_drained
        && decisions_drained
        && observations_drained
        && observation_events_drained
        && route_decisions_drained
        && parent_failures_drained
        && usage_drained;
    report
}

/// The periodic retention pass. Each pass re-reads the windows from the
/// routing file and runs on a blocking thread, so selection and admission
/// never wait on it except for one short batch at a time.
pub async fn retention_loop(store: Arc<SqliteRouteStateStore>, config_path: PathBuf) {
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
        let outcome = tokio::task::spawn_blocking(move || {
            run_retention_pass(
                &pass_store,
                &windows,
                crate::account_selection::now_ms(),
                &PassBudget::default(),
            )
        })
        .await;
        match outcome {
            Ok(report) => {
                if report.total() > 0 {
                    tracing::info!(
                        shadow_decisions = report.shadow_decisions,
                        account_admissions = report.account_admissions,
                        account_holds = report.account_holds,
                        account_affinities = report.account_affinities,
                        account_pins = report.account_pins,
                        account_decisions = report.account_decisions,
                        account_observations = report.account_observations,
                        observation_events = report.observation_events,
                        route_decisions = report.route_decisions,
                        parent_failures = report.parent_failures,
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
    use striatum_store::{dev_descriptor, AccountDecisionRecord, AccountObservationRow};
    use serde_json::json;

    /// The test clock, 2026-10-12T00:00:00Z.
    const NOW: i64 = 1_791_763_200_000;

    #[test]
    fn an_absent_block_gives_the_documented_defaults() {
        let (windows, warnings) = retention_windows_from_value(None);
        assert_eq!(
            windows,
            RetentionWindows {
                account_selection_days: 30,
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
            "account_selection_days": 3,
            "account_observation_days": "seven",
            "model_route_decisions_days": 45,
            "model_route_usage_days": 3,
            "account_decision_days": 9,
        });
        let (windows, warnings) = retention_windows_from_value(Some(&block));
        assert_eq!(
            windows,
            RetentionWindows {
                // Below the seven-day minimum: raised to it.
                account_selection_days: 7,
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
    fn the_retired_shadow_windows_are_named_as_removed_and_change_nothing() {
        let block = json!({
            "account_decision_routine_hours": 24,
            "account_decision_notable_days": 2,
        });
        let (windows, warnings) = retention_windows_from_value(Some(&block));
        assert_eq!(windows, RetentionWindows::default());
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        for key in [
            "account_decision_routine_hours",
            "account_decision_notable_days",
        ] {
            assert!(
                warnings.iter().any(|warning| warning.starts_with(&format!(
                    "retention.{key} was removed with account-selection shadow mode; delete it"
                ))),
                "{warnings:?}"
            );
        }
    }

    #[test]
    fn a_bad_retention_block_does_not_depend_on_the_rest_of_the_file() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-retention-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("striatum.jsonc");
        // `model_routing.demand_exponent` has the wrong type, which fails the
        // typed routing config; the retention block is still read.
        crate::test_config::write(
            &path,
            r#"{
                // comment
                "model_routing": { "demand_exponent": "steep" },
                "retention": { "account_selection_days": 12 }
            }"#,
        )
        .unwrap();
        let windows = retention_windows(&path);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(windows.account_selection_days, 12);
        assert_eq!(windows.account_observation_days, 7);
    }

    fn shadow_admit(id: String, created_at: i64) -> AccountDecisionRecord {
        AccountDecisionRecord {
            decision_id: id,
            mode: "shadow".into(),
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
    fn one_pass_purges_at_most_its_shadow_budget_and_the_next_continues() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-retention-pass-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db").to_string_lossy().into_owned();
        let store =
            SqliteRouteStateStore::open(&dev_descriptor("striatum", path)).expect("open store");
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
            max_batches_per_table: 1,
            max_shadow_purge_batches: 3,
            pause: Duration::ZERO,
        };
        let windows = RetentionWindows::default();

        // Shadow rows are purged whatever their age, so all 21 go: twelve in
        // the first pass (three batches of four), the other nine in the second
        // pass (batches of four, four and one).
        let first = run_retention_pass(&store, &windows, NOW, &budget);
        assert_eq!(first.shadow_decisions, 12);
        assert!(!first.drained);

        let second = run_retention_pass(&store, &windows, NOW, &budget);
        assert_eq!(second.shadow_decisions, 9);
        assert!(second.drained);

        let left = store.account_decisions("shadow", 100).unwrap();
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(left.is_empty(), "{left:?}");
    }

    /// The pass prunes runner-event keys on their six-hour window (the
    /// observation log keeps its week) and per-head failure copies whose
    /// decision a restore left out of the ledger, on the decision window.
    #[test]
    fn a_pass_prunes_old_event_keys_and_orphaned_parent_failures() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-retention-keys-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            path.to_string_lossy().into_owned(),
        ))
        .expect("open store");
        let observation = |id: &str, received_at: i64| AccountObservationRow {
            observation_id: id.into(),
            principal: "reserved:broca".into(),
            runner: "broca".into(),
            event_id: id.into(),
            kind: "attempt".into(),
            admission_id: None,
            bound: false,
            observation_json: "{}".into(),
            effects_json: "[]".into(),
            received_at,
        };
        store
            .account_transaction(|tx| {
                tx.insert_observation(&observation("seven-hours", NOW - 7 * HOUR_MS))?;
                tx.insert_observation(&observation("one-hour", NOW - HOUR_MS))
            })
            .unwrap();
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(&format!(
                "INSERT INTO model_route_parent_failures \
                   (decision_id, parent_session_id, outcome_at, selected_route_key, \
                    selected_provider_id, outcome_error) \
                 VALUES ('orphan-old', 'head', {old}, 'p/m', 'p', 'refused'), \
                        ('orphan-young', 'head', {young}, 'p/m', 'p', 'refused')",
                old = NOW - 40 * DAY_MS,
                young = NOW - HOUR_MS,
            ))
            .unwrap();

        let report = run_retention_pass(
            &store,
            &RetentionWindows::default(),
            NOW,
            &PassBudget {
                pause: Duration::ZERO,
                ..PassBudget::default()
            },
        );
        assert_eq!(report.observation_events, 1);
        assert_eq!(report.account_observations, 0);
        assert_eq!(report.parent_failures, 1);
        let young = store.recent_failed_decisions_for_parent("head", 0).unwrap();
        let seven_hours = store
            .account_transaction(|tx| {
                tx.observation_by_event("reserved:broca", "broca", "seven-hours")
            })
            .unwrap();
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(young.len(), 1);
        assert_eq!(young[0].outcome_at, NOW - HOUR_MS);
        // The observation itself is still in its week-long log.
        assert!(seven_hours.is_some());
    }
}
