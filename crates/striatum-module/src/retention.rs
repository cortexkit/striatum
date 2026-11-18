//! Routing-store retention: the periodic pass that deletes old rows from the
//! append-only logs in small batches, off the request path.
//!
//! Who reads these rows, and how far back, decides the windows:
//! - Account-selection history (`account_admission`, `account_affinity` with
//!   its `account_pin` generations, `account_hold`, and serve
//!   `account_decision` rows): admissions are read back for a duplicate
//!   request, a runner's report and pending demand, all within minutes of the
//!   commit; affinity is live for seven idle days, followed by a one-hour
//!   grace period and any remaining lease; holds lapse within minutes.
//!   Affinity and its pins go after that grace once no admission or hold
//!   references them. Other history uses the `account_selection_days` window
//!   (see the store's `retention.rs`).
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
//! - The scoring detail inside a decision (`score_json`'s full candidate list
//!   and `quota_signal_json`'s per-capacity signals) is kept for
//!   `model_route_decisions_full_detail_days`, then compacted in place (see
//!   the store's `decision_detail.rs`). The compact record keeps what readers
//!   use for the whole decision window: the picked route's fit and pricing
//!   (`objective.taskPricing.valueMultiplier` among them), `alsoAdequate` with
//!   those routes' candidate rows, `rejected_json`, the explanation and
//!   `policy_revision`. One decision in
//!   `model_route_decisions_full_detail_sample_one_in`, chosen by a hash of
//!   its id, keeps full detail for the whole window for offline calibration.
//! - `model_route_usage`: observed speed and class token profiles read a
//!   trailing seven-day window, so the usage window never goes below that.
//! - routing/v1 replay records (`routing_v1_decision` with its
//!   `routing_v1_report` and `routing_v1_report_delivery`, and
//!   `routing_v1_admission`): a decision is current for ten minutes and a
//!   re-sent report or admission comes within minutes, so they are kept for
//!   [`V1_REPLAY_WINDOW_MS`] (a week) and longer while a live run still needs
//!   them. `routing_v1_affinity` follows the same window.
//! - `account_continue`: kept while its run is live, then for
//!   [`CONTINUATION_WINDOW_MS`] after the run finished or the offer lapsed.
//! - `model_route_speed_samples`: observed speed reads the newest twenty
//!   samples of the last seven days; older samples are cut to the newest
//!   [`SPEED_SAMPLES_KEPT_PAST_WINDOW`] per route and served population, and
//!   none outlives [`SPEED_SAMPLE_CAP_MS`].
//! - `routing_call_count`: rows idle for [`ROUTING_CALL_COUNT_WINDOW_MS`] go,
//!   and the call window's start moves up with them.
//! - `fence_retired_origin`: an archive nothing reads back; kept for
//!   [`RETIRED_FENCE_ORIGIN_WINDOW_MS`].
//! - `routing_unscored_admissible_models`: diagnostics read sightings within
//!   their scan window; kept for the decision window after the last sighting.
//! - `quota_reading_log`: kept for `READING_LOG_RETENTION_DAYS` (three).
//!
//! The windows come from the `retention` block of the routing file, re-read
//! every pass. Deleting rows frees pages inside the database file for reuse;
//! only an offline VACUUM shrinks the file itself, and this pass never runs one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use striatum_core::RouteStateStore;
use striatum_store::SqliteRouteStateStore;

use crate::config_overrides::read_routing_config_value;

/// Time between retention passes.
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(60);
/// Most rows one batch deletes. Each batch is one short transaction.
pub const RETENTION_BATCH_ROWS: usize = 256;
/// Admission cleanup copies demand and latest-admission indexes as it deletes.
/// Eight keyed rows keep those writer commands below ten milliseconds on the
/// live-sized admission fixture; candidate discovery runs on a pooled reader.
pub const ADMISSION_DELETE_BATCH_ROWS: usize = 8;
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
/// Decisions one compaction batch walks. Each compacted decision is read
/// whole, parsed and rewritten while the batch holds the writer. Measured on
/// live-sized rows (177 KB of scoring, 17 KB of quota signals; the ignored
/// `measure_strip_batch_writer_hold` test in the store, release build, warm
/// page cache), a batch held the writer for a median of about 1.2 ms per row
/// plus 1 ms: 9.7 ms for 8 rows, 18 ms for 16, 37 ms for 32. Eight rows keep a
/// batch near 10 ms, leaving room under a 20 ms budget for pages read cold
/// from a large store.
pub const DETAIL_STRIP_BATCH_ROWS: usize = 8;
/// Most compaction batches one pass runs. With eight rows a batch a pass
/// (one a minute) compacts about 500 decisions, where serving records about
/// three a minute (4,300 a day on a busy install), so a first deployment's
/// backlog of about 175,000 decisions drains in about six hours. With the pause between batches a pass spends about two seconds
/// a minute on it, in slices of about 10 ms.
pub const DETAIL_STRIP_MAX_BATCHES: usize = 64;
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

/// How long routing/v1 decision bindings, report records and admission
/// replay records are kept. A decision is current routing for ten minutes, a
/// reservation must start within a minute, and runners re-send a report or an
/// admission within minutes of the first; a week covers a frozen run that
/// reports days later, with margin. Rows a live run still names are kept past
/// it.
pub const V1_REPLAY_WINDOW_MS: i64 = 7 * DAY_MS;
/// How long a continuation answer outlives the end of its run (the run's
/// finish, or the offer's release or deadline).
pub const CONTINUATION_WINDOW_MS: i64 = 7 * DAY_MS;
/// Idle affinity has no prompt-cache value after its seven-day expiry. Keep
/// the expired row for one extra hour so near-boundary retries retain their
/// pin checks and binding. After that hour, an unreferenced conversation starts
/// at generation 1 again, may import a preferred account again, and no longer
/// carries its former service-tier binding or operator-override pin identity.
pub const ACCOUNT_AFFINITY_GRACE_MS: i64 = HOUR_MS;
/// Affinity deletion also copies the admission state's sharded affinity
/// indexes. Smaller writer commands leave room for those copies and the keyed
/// reference rechecks; the reader still scans the ordinary per-pass row budget.
/// Eight-key commands took at most 6.3 ms end to end on a live-sized fixture
/// in release mode, including queueing, commit and state publication.
pub const AFFINITY_DELETE_BATCH_ROWS: usize = 8;

/// One bounded expiry sweep across passes. Protected conversations need not
/// be reconsidered until admission history ages out; a completed sweep restarts
/// no sooner than one hour after it began. Restarting routing loses only this
/// optimization, not any retention state or reference protection.
#[derive(Debug, Clone, Default)]
struct AffinitySweep {
    after: Option<striatum_store::AffinityRetentionPosition>,
    started_at: Option<i64>,
    complete: bool,
}

impl AffinitySweep {
    fn plan(
        &mut self,
        store: &SqliteRouteStateStore,
        before: i64,
        now_ms: i64,
        scan_limit: usize,
    ) -> Result<Option<striatum_store::AffinityRetentionPlan>, striatum_core::RouteStateStoreError>
    {
        if scan_limit == 0 {
            return Ok(None);
        }
        if self.complete {
            if now_ms.saturating_sub(self.started_at.unwrap_or(now_ms)) < HOUR_MS {
                return Ok(None);
            }
            *self = Self::default();
        }
        self.started_at.get_or_insert(now_ms);
        let plan = store.plan_account_affinities(before, now_ms, self.after, scan_limit)?;
        self.after = plan.last.or(self.after);
        self.complete = plan.examined < scan_limit;
        Ok(Some(plan))
    }
}
/// The oldest a generation-speed sample may be.
pub const SPEED_SAMPLE_CAP_MS: i64 = 30 * DAY_MS;
/// Samples kept per route and served population once they are older than the
/// seven-day observed-speed window: as many as one observed-speed read uses.
pub const SPEED_SAMPLES_KEPT_PAST_WINDOW: usize =
    striatum_core::route_state_store::OBSERVED_SPEED_SAMPLE_LIMIT as usize;
/// How long a call-count row stays after its last call. Handler retirement
/// asks for fourteen quiet days, so thirty leaves margin.
pub const ROUTING_CALL_COUNT_WINDOW_MS: i64 = 30 * DAY_MS;
/// How long an archived, retired fence origin is kept.
pub const RETIRED_FENCE_ORIGIN_WINDOW_MS: i64 = 30 * DAY_MS;

/// How long each log keeps its rows. Every value is a whole number of the
/// unit in its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionWindows {
    /// Served account-selection history, counted from each row's last
    /// activity.
    pub account_selection_days: u64,
    pub account_observation_days: u64,
    pub model_route_decisions_days: u64,
    /// How long a decision keeps its full scoring detail before it is
    /// compacted.
    pub model_route_decisions_full_detail_days: u64,
    /// One decision in this many keeps full scoring detail for the whole
    /// decision window. Zero keeps none; one keeps all.
    pub model_route_decisions_full_detail_sample_one_in: u64,
    pub model_route_usage_days: u64,
}

impl Default for RetentionWindows {
    fn default() -> Self {
        Self {
            account_selection_days: 30,
            account_observation_days: 7,
            model_route_decisions_days: 30,
            model_route_decisions_full_detail_days: 2,
            model_route_decisions_full_detail_sample_one_in: 20,
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

const WINDOW_KEYS: [WindowKey; 6] = [
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
        // Core prices a run from the picked route's task pricing minutes after
        // the decision, and the compact record keeps that; one day is the floor.
        key: "model_route_decisions_full_detail_days",
        minimum: 1,
        field: |w| &mut w.model_route_decisions_full_detail_days,
    },
    WindowKey {
        key: "model_route_decisions_full_detail_sample_one_in",
        minimum: 0,
        field: |w| &mut w.model_route_decisions_full_detail_sample_one_in,
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
    /// Unneeded pin generations, including those deleted with an affinity row.
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
    /// Decisions whose scoring detail was compacted.
    pub decision_details_stripped: usize,
    /// Decisions past the full-detail window passed over at full detail
    /// because they are in the sample.
    pub decision_details_sampled: usize,
    pub v1_decisions: usize,
    /// Report records deleted with their decision, or as orphans.
    pub v1_reports: usize,
    /// Per-delivery report records deleted with their decision, or as orphans.
    pub v1_report_deliveries: usize,
    pub v1_admissions: usize,
    pub v1_affinities: usize,
    pub account_continuations: usize,
    pub speed_samples: usize,
    pub routing_call_counts: usize,
    pub retired_fence_origins: usize,
    pub unscored_admissible_models: usize,
    pub quota_readings: usize,
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
            + self.decision_details_stripped
            + self.v1_decisions
            + self.v1_reports
            + self.v1_report_deliveries
            + self.v1_admissions
            + self.v1_affinities
            + self.account_continuations
            + self.speed_samples
            + self.routing_call_counts
            + self.retired_fence_origins
            + self.unscored_admissible_models
            + self.quota_readings
    }
}

/// The size of one pass: rows per batch, batches per table (and for the
/// shadow purge), and the pause between batches.
#[derive(Debug, Clone, Copy)]
pub struct PassBudget {
    pub batch_rows: usize,
    /// Decisions one compaction batch walks. Each decision it compacts is
    /// read whole (about 150 KB) and rewritten, so its batches are smaller
    /// than the delete batches.
    pub detail_batch_rows: usize,
    /// Most compaction batches one pass runs.
    pub max_detail_batches: usize,
    pub max_batches_per_table: usize,
    pub max_shadow_purge_batches: usize,
    pub pause: Duration,
}

impl Default for PassBudget {
    fn default() -> Self {
        Self {
            batch_rows: RETENTION_BATCH_ROWS,
            detail_batch_rows: DETAIL_STRIP_BATCH_ROWS,
            max_detail_batches: DETAIL_STRIP_MAX_BATCHES,
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
    table: &'static str,
    budget: &PassBudget,
    max_batches: usize,
    batches_so_far: &mut usize,
    batch: impl FnMut() -> Result<(usize, usize), String>,
) -> (usize, bool) {
    drain_batches(
        table,
        budget.batch_rows,
        budget,
        max_batches,
        batches_so_far,
        batch,
    )
}

/// [`drain`] for a table whose batches hold `batch_rows` rows rather than the
/// budget's delete batch size.
fn drain_batches(
    table: &'static str,
    batch_rows: usize,
    budget: &PassBudget,
    max_batches: usize,
    batches_so_far: &mut usize,
    mut batch: impl FnMut() -> Result<(usize, usize), String>,
) -> (usize, bool) {
    let name = match table {
        "account_decision (shadow)" => "retention.shadow_decisions",
        "model_route_decisions (detail)" => "retention.decision_detail",
        name => name,
    };
    striatum_store::job_timing::sync(name, || {
        let mut deleted = 0;
        for round in 0..max_batches {
            if round > 0 && !budget.pause.is_zero() {
                std::thread::sleep(budget.pause);
            }
            *batches_so_far += 1;
            match batch() {
                Ok((removed, examined)) => {
                    striatum_store::job_timing::rows(removed as u64);
                    deleted += removed;
                    if examined < batch_rows {
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
    })
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
/// first (admissions, holds, affinity and unneeded pins, then the decisions
/// they referenced), so one pass can retire a whole expired conversation.
#[cfg(test)]
pub fn run_retention_pass(
    store: &SqliteRouteStateStore,
    windows: &RetentionWindows,
    now_ms: i64,
    budget: &PassBudget,
) -> RetentionPassReport {
    run_retention_pass_with_affinity_sweep(
        store,
        windows,
        now_ms,
        budget,
        &mut AffinitySweep::default(),
    )
}

fn run_retention_pass_with_affinity_sweep(
    store: &SqliteRouteStateStore,
    windows: &RetentionWindows,
    now_ms: i64,
    budget: &PassBudget,
    affinity_sweep: &mut AffinitySweep,
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
    let (admissions, admissions_drained) = drain_batches(
        "account_admission",
        rows.min(ADMISSION_DELETE_BATCH_ROWS),
        budget,
        per_table,
        batches,
        || {
            counted(crate::account_writer::prune_account_admissions(
                store,
                history_before,
                rows.min(ADMISSION_DELETE_BATCH_ROWS),
                now_ms,
            ))
        },
    );
    let (holds, holds_drained) = drain("account_hold", budget, per_table, batches, || {
        counted(store.prune_account_holds(history_before, rows))
    });
    let mut pins = 0;
    let affinity_before = before(now_ms, ACCOUNT_AFFINITY_GRACE_MS);
    let scan_limit = rows.saturating_mul(per_table);
    let checkpoint = affinity_sweep.clone();
    let (mut affinities, mut affinities_drained) = (0, true);
    striatum_store::job_timing::sync("retention.affinity_and_pins", || {
        match affinity_sweep.plan(store, affinity_before, now_ms, scan_limit) {
            Ok(Some(plan)) => {
                affinities_drained = plan.examined < scan_limit;
                for (round, keys) in plan
                    .keys
                    .chunks(rows.min(AFFINITY_DELETE_BATCH_ROWS).max(1))
                    .enumerate()
                {
                    if round > 0 && !budget.pause.is_zero() {
                        std::thread::sleep(budget.pause);
                    }
                    *batches += 1;
                    match crate::account_writer::prune_account_affinities(
                        store,
                        keys.to_vec(),
                        affinity_before,
                        now_ms,
                    ) {
                        Ok(batch) => {
                            striatum_store::job_timing::rows(
                                (batch.affinities + batch.pins) as u64,
                            );
                            affinities += batch.affinities;
                            pins += batch.pins;
                        }
                        Err(error) => {
                            *affinity_sweep = checkpoint;
                            tracing::warn!(error=%error, "affinity retention command failed");
                            break;
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(error=%error, "affinity retention planning failed"),
        }
    });
    let (unneeded_pins, pins_drained) = drain("account_pin", budget, per_table, batches, || {
        counted(crate::account_writer::prune_account_pins(
            store,
            history_before,
            rows,
        ))
    });
    pins += unneeded_pins;
    let (decisions, decisions_drained) =
        drain("account_decision", budget, per_table, batches, || {
            counted(store.prune_serve_decisions(history_before, rows))
        });
    let observed_before = before(now_ms, window_ms(windows.account_observation_days, DAY_MS));
    let (observations, observations_drained) =
        drain("account_observation", budget, per_table, batches, || {
            counted(crate::account_writer::prune_account_observations(
                store,
                observed_before,
                rows,
            ))
        });
    let keys_before = before(now_ms, OBSERVATION_EVENT_KEY_WINDOW_MS);
    let (observation_events, observation_events_drained) = drain(
        "account_observation_event",
        budget,
        per_table,
        batches,
        || {
            counted(crate::account_writer::prune_observation_events(
                store,
                keys_before,
                rows,
            ))
        },
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
    let full_detail_before = before(
        now_ms,
        window_ms(windows.model_route_decisions_full_detail_days, DAY_MS),
    );
    let mut details_sampled = 0;
    let (details_stripped, details_drained) = drain_batches(
        "model_route_decisions (detail)",
        budget.detail_batch_rows,
        budget,
        budget.max_detail_batches,
        batches,
        || {
            store
                .strip_decision_detail(
                    full_detail_before,
                    windows.model_route_decisions_full_detail_sample_one_in,
                    budget.detail_batch_rows,
                    now_ms,
                )
                .map(|batch| {
                    details_sampled += batch.sampled;
                    (batch.stripped, batch.examined)
                })
                .map_err(|error| error.0)
        },
    );
    // Continuations go before the v1 decisions they name, so one pass can
    // retire a finished run's whole lineage.
    let continuation_before = before(now_ms, CONTINUATION_WINDOW_MS);
    let (continuations, continuations_drained) =
        drain("account_continue", budget, per_table, batches, || {
            counted(crate::account_writer::prune_account_continuations(
                store,
                continuation_before,
                rows,
            ))
        });
    let replay_before = before(now_ms, V1_REPLAY_WINDOW_MS);
    let (mut v1_reports, mut v1_report_deliveries) = (0, 0);
    let (v1_decisions, v1_decisions_drained) =
        drain("routing_v1_decision", budget, per_table, batches, || {
            crate::account_writer::prune_v1_decisions(store, replay_before, rows)
                .map(|batch| {
                    v1_reports += batch.reports;
                    v1_report_deliveries += batch.deliveries;
                    (batch.decisions, batch.decisions)
                })
                .map_err(|error| error.0)
        });
    let (orphan_reports, orphan_reports_drained) =
        drain("routing_v1_report", budget, per_table, batches, || {
            counted(store.prune_orphan_v1_reports(replay_before, rows))
        });
    let (orphan_deliveries, orphan_deliveries_drained) = drain(
        "routing_v1_report_delivery",
        budget,
        per_table,
        batches,
        || counted(store.prune_orphan_v1_report_deliveries(replay_before, rows)),
    );
    let (v1_admissions, v1_admissions_drained) =
        drain("routing_v1_admission", budget, per_table, batches, || {
            counted(crate::account_writer::prune_v1_admissions(
                store,
                replay_before,
                rows,
            ))
        });
    let (v1_affinities, v1_affinities_drained) =
        drain("routing_v1_affinity", budget, per_table, batches, || {
            counted(crate::account_writer::prune_v1_affinities(
                store,
                replay_before,
                rows,
            ))
        });
    let speed_window_start = before(
        now_ms,
        striatum_core::route_state_store::OBSERVED_SPEED_WINDOW_MS as i64,
    );
    let speed_cap_before = before(now_ms, SPEED_SAMPLE_CAP_MS);
    let (speed_samples, speed_samples_drained) = drain(
        "model_route_speed_samples",
        budget,
        per_table,
        batches,
        || {
            counted(store.prune_speed_samples(
                speed_window_start,
                speed_cap_before,
                SPEED_SAMPLES_KEPT_PAST_WINDOW,
                rows,
            ))
        },
    );
    let calls_before = before(now_ms, ROUTING_CALL_COUNT_WINDOW_MS);
    let (call_counts, call_counts_drained) =
        drain("routing_call_count", budget, per_table, batches, || {
            counted(store.prune_routing_call_counts(calls_before, rows))
        });
    let retired_before = before(now_ms, RETIRED_FENCE_ORIGIN_WINDOW_MS);
    let (retired_origins, retired_origins_drained) =
        drain("retirement_tombstone", budget, per_table, batches, || {
            counted(crate::account_writer::prune_retired_fence_origins(
                store,
                retired_before,
                rows,
            ))
        });
    let (unscored, unscored_drained) = drain(
        "routing_unscored_admissible_models",
        budget,
        per_table,
        batches,
        || counted(store.prune_unscored_admissible_models(decided_before, rows)),
    );
    let readings_before = striatum_core::quota_poll::reading_log_retention_cutoff(now_ms);
    let (quota_readings, quota_readings_drained) =
        drain("quota_reading_log", budget, per_table, batches, || {
            counted(store.prune_quota_reading_log(readings_before, rows))
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
    report.decision_details_stripped = details_stripped;
    report.decision_details_sampled = details_sampled;
    report.account_continuations = continuations;
    report.v1_decisions = v1_decisions;
    report.v1_reports = v1_reports + orphan_reports;
    report.v1_report_deliveries = v1_report_deliveries + orphan_deliveries;
    report.v1_admissions = v1_admissions;
    report.v1_affinities = v1_affinities;
    report.speed_samples = speed_samples;
    report.routing_call_counts = call_counts;
    report.retired_fence_origins = retired_origins;
    report.unscored_admissible_models = unscored;
    report.quota_readings = quota_readings;
    report.drained = shadow_drained
        && admissions_drained
        && holds_drained
        && affinities_drained
        && pins_drained
        && decisions_drained
        && observations_drained
        && observation_events_drained
        && route_decisions_drained
        && parent_failures_drained
        && usage_drained
        && details_drained
        && continuations_drained
        && v1_decisions_drained
        && orphan_reports_drained
        && orphan_deliveries_drained
        && v1_admissions_drained
        && v1_affinities_drained
        && speed_samples_drained
        && call_counts_drained
        && retired_origins_drained
        && unscored_drained
        && quota_readings_drained;
    report
}

/// The periodic retention pass. Each pass re-reads the windows from the
/// routing file and runs on a blocking thread, so selection and admission
/// never wait on it except for one short batch at a time.
pub async fn retention_loop(store: Arc<SqliteRouteStateStore>, config_path: PathBuf) {
    let mut last_warnings: Vec<String> = Vec::new();
    let mut affinity_sweep = AffinitySweep::default();
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
        let mut pass_sweep = std::mem::take(&mut affinity_sweep);
        let outcome = striatum_store::job_timing::run(
            "retention",
            crate::stage_timing::spawn_job_blocking(move || {
                let report = run_retention_pass_with_affinity_sweep(
                    &pass_store,
                    &windows,
                    crate::account_selection::now_ms(),
                    &PassBudget::default(),
                    &mut pass_sweep,
                );
                (report, pass_sweep)
            }),
        )
        .await;
        match outcome {
            Ok((report, next_sweep)) => {
                affinity_sweep = next_sweep;
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
                        decision_details_stripped = report.decision_details_stripped,
                        decision_details_sampled = report.decision_details_sampled,
                        account_continuations = report.account_continuations,
                        v1_decisions = report.v1_decisions,
                        v1_reports = report.v1_reports,
                        v1_report_deliveries = report.v1_report_deliveries,
                        v1_admissions = report.v1_admissions,
                        v1_affinities = report.v1_affinities,
                        speed_samples = report.speed_samples,
                        routing_call_counts = report.routing_call_counts,
                        retired_fence_origins = report.retired_fence_origins,
                        unscored_admissible_models = report.unscored_admissible_models,
                        quota_readings = report.quota_readings,
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
    fn seed_carbon_retention_rows(
        store: &SqliteRouteStateStore,
        residue: usize,
        retired: usize,
        archive: usize,
    ) {
        store.with_writer_for_test(|conn| {
            for index in 0..residue {
                conn.execute("INSERT INTO block(kind,origin_key,provider,credential_id,evidence,evidence_at,quota,next_recheck_at,source_present,managed,cleared_by,cleared_at)
                    VALUES ('credential',json_array('drain-p',?1),'drain-p',?1,'residue',0,0,0,0,0,'removed',?2)",rusqlite::params![format!("c-{index}"),NOW-31*DAY_MS])?;
            }
            for index in 0..retired {
                conn.execute("INSERT INTO block(kind,origin_key,provider,model,evidence,evidence_at,quota,next_recheck_at,source_present,managed,cleared_by,cleared_at,deadline,cooldown_until,observed_at)
                    VALUES ('route',json_array(?1),'drain-p',?1,json_array(?2,NULL,?2),?2,0,?2,0,0,'retired',?2,?2,?2,?2)",rusqlite::params![format!("drain-p/retired-{index}"),NOW-31*DAY_MS])?;
            }
            for index in 0..archive {
                conn.execute("INSERT INTO retirement_tombstone VALUES ('route',json_array(?1),'archive',0,?2)",rusqlite::params![format!("drain-p/archive-{index}"),NOW-31*DAY_MS])?;
            }
            Ok(())
        }).unwrap();
    }

    #[test]
    fn residue_only_retention_drains_multiple_batches_even_without_archive_rows() {
        let (dir, path, store) = scratch_store("residue-drain-count");
        seed_carbon_retention_rows(&store, 600, 0, 0);
        let budget = PassBudget {
            batch_rows: 256,
            detail_batch_rows: 256,
            max_detail_batches: 1,
            max_batches_per_table: 16,
            max_shadow_purge_batches: 1,
            pause: Duration::ZERO,
        };
        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &budget);
        assert_eq!(
            count(
                &path,
                "SELECT count(*) FROM block WHERE cleared_by='removed'"
            ),
            0,
            "an empty archive table must not stop collection while residue fills another batch"
        );
        assert_eq!(
            report.retired_fence_origins, 600,
            "retention reports every deleted physical record, not only archive rows"
        );
        assert!(report.drained);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn native_retention_relations_share_one_physical_row_budget_per_transaction() {
        let (dir, path, store) = scratch_store("retention-shared-budget");
        seed_carbon_retention_rows(&store, 300, 300, 300);
        store.account_transaction(|_| Ok(())).unwrap();
        let total = || {
            count(
                &path,
                "SELECT (SELECT count(*) FROM block)+(SELECT count(*) FROM retirement_tombstone)",
            )
        };
        let before = total();
        let held_before = store.writer_hold_ns_for_test();
        let deleted = store
            .prune_retired_fence_origins(NOW - 30 * DAY_MS, 256)
            .unwrap();
        let held_ns = store.writer_hold_ns_for_test() - held_before;
        eprintln!("retention_full_budget limit=256 deleted={deleted} writer_hold_ns={held_ns} queue_time_excluded=true");
        let physical = before - total();
        assert!(physical<=256,"one writer command gets one shared physical-row budget, not one per relation: {physical}");
        assert_eq!(
            deleted, physical as usize,
            "the public count is the complete transaction's actual deletion count"
        );
        assert_eq!(deleted, 256);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    use super::*;
    use serde_json::json;
    use striatum_store::{dev_descriptor, AccountDecisionRecord, AccountObservationRow};

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
                model_route_decisions_full_detail_days: 2,
                model_route_decisions_full_detail_sample_one_in: 20,
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
            "model_route_decisions_full_detail_days": 0,
            "model_route_decisions_full_detail_sample_one_in": 50,
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
                // Core prices runs from the compact record, but a day of full
                // detail is the floor.
                model_route_decisions_full_detail_days: 1,
                model_route_decisions_full_detail_sample_one_in: 50,
                // Usage readers average seven days, so seven is the floor.
                model_route_usage_days: 7,
            }
        );
        assert_eq!(warnings.len(), 5, "{warnings:?}");
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
            detail_batch_rows: 4,
            max_detail_batches: 1,
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

    /// A scratch store for one test. Returns the directory to delete, the
    /// database path for raw seeding, and the open store.
    fn scratch_store(
        name: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        SqliteRouteStateStore,
    ) {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-retention-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            path.to_string_lossy().into_owned(),
        ))
        .expect("open store");
        (dir, path, store)
    }

    /// A raw connection for seeding rows whose referenced account history the
    /// test leaves out, so it does not enforce foreign keys.
    fn seeding_connection(path: &std::path::Path) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        connection
    }

    fn no_pause() -> PassBudget {
        PassBudget {
            pause: Duration::ZERO,
            ..PassBudget::default()
        }
    }

    fn seed_pin_affinity(path: &Path, store: &SqliteRouteStateStore, id: &str, expires_at: i64) {
        let old = NOW - 31 * DAY_MS;
        let mut decision = shadow_admit(id.into(), old);
        decision.mode = striatum_store::DECISION_MODE_SERVE.into();
        store
            .account_transaction(|tx| tx.insert_decision(&decision))
            .unwrap();
        let connection = seeding_connection(path);
        connection
            .execute(
                "INSERT INTO account_pin (pin_id,principal,conversation_hash,provider, \
               pin_generation,affinity_model,account_id,credential_id,pin_mode, \
               decision_id,created_at) \
             VALUES (?1,'runner',?1,'p',1,'pick','acct','cred','conversation_lease',?1,?2)",
                rusqlite::params![id, old],
            )
            .unwrap();
        connection.execute(
            "INSERT INTO account_affinity (principal,conversation_hash,provider,affinity_model, \
               pin_id,pin_generation,account_id,credential_id,credential_binding_revision, \
               policy_version,initial_demand_bytes,created_at,last_used_at,expires_at,lease_valid_until) \
             VALUES ('runner',?1,'p','pick',?1,1,'acct','cred',1,1,0,?2,?2,?3,NULL)",
            rusqlite::params![id, old, expires_at],
        ).unwrap();
    }

    #[test]
    fn affinity_retention_frees_expired_pins_and_decisions_in_the_same_pass() {
        let (dir, path, store) = scratch_store("expired-pin");
        seed_pin_affinity(&path, &store, "expired", NOW - HOUR_MS - 1);
        let connection = seeding_connection(&path);
        connection.execute("INSERT INTO account_pin SELECT 'predecessor',principal,conversation_hash,provider,2,affinity_model,account_id,credential_id,'run_frozen',pin_id,decision_id,created_at FROM account_pin WHERE pin_id='expired'", []).unwrap();
        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(report.account_affinities, 1);
        assert_eq!(report.account_pins, 2);
        assert_eq!(report.account_decisions, 1);
        assert_eq!(count(&path, "SELECT count(*) FROM account_affinity"), 0);
        assert_eq!(count(&path, "SELECT count(*) FROM account_pin"), 0);
        assert_eq!(count(&path, "SELECT count(*) FROM account_decision"), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn affinity_retention_keeps_live_grace_open_admission_unused_continuation_and_hold_pins() {
        let (dir, path, store) = scratch_store("needed-pins");
        for id in ["dead", "open", "offer", "held"] {
            seed_pin_affinity(&path, &store, id, NOW - HOUR_MS - 1);
        }
        seed_pin_affinity(&path, &store, "current", NOW + DAY_MS);
        seed_pin_affinity(&path, &store, "grace", NOW - HOUR_MS);
        seed_pin_affinity(&path, &store, "leased", NOW - HOUR_MS - 1);
        let connection = seeding_connection(&path);
        connection
            .execute(
                "UPDATE account_affinity SET lease_valid_until=?1 WHERE pin_id='leased'",
                [NOW + 1],
            )
            .unwrap();
        for (id, pin) in [("open-admission", "open"), ("reservation", "offer")] {
            insert_admission(&connection, id, "reserved", None, (NOW, None, None));
            connection.execute(
                "UPDATE account_admission SET pin_id=?1, conversation_hash=?1, decision_id=?1 WHERE admission_id=?2",
                rusqlite::params![pin, id],
            ).unwrap();
        }
        connection
            .execute(
                "INSERT INTO account_continue (principal,selection_id,request_json,answer_json, \
               run_id,admission_id,previous_decision_id,decision_id,reservation_id,valid_until_ms) \
             VALUES ('runner','offer-selection','{}','{}','run','open-admission','root', \
               'model-offer','reservation',?1)",
                [NOW + DAY_MS],
            )
            .unwrap();
        connection.execute(
            "INSERT INTO account_hold (decision_id,principal,conversation_hash,provider,pin_id, \
               pin_generation,account_id,credential_id,basis,reset_at,retry_after_s,created_at) \
             VALUES ('held','runner','held','p','held',1,'acct','cred','five_hour',?1,60,?1)",
            [NOW],
        ).unwrap();
        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(
            report.account_affinities, 1,
            "only the unreferenced expired conversation goes"
        );
        assert_eq!(report.account_pins, 1);
        for pin in ["current", "grace", "leased", "open", "offer", "held"] {
            assert_eq!(
                count(
                    &path,
                    &format!("SELECT count(*) FROM account_pin WHERE pin_id='{pin}'")
                ),
                1,
                "{pin}"
            );
        }
        assert_eq!(
            count(&path, "SELECT count(*) FROM pragma_foreign_key_check"),
            0
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn affinity_sweep_resumes_past_protected_rows_and_waits_an_hour_to_restart() {
        let (dir, path, store) = scratch_store("affinity-sweep");
        for id in ["a", "b", "c", "d"] {
            seed_pin_affinity(&path, &store, id, NOW - 2 * HOUR_MS);
        }
        let conn = seeding_connection(&path);
        conn.execute(
            "UPDATE account_affinity SET lease_valid_until=?1 WHERE pin_id IN ('a','b')",
            [NOW + HOUR_MS],
        )
        .unwrap();
        let mut sweep = AffinitySweep::default();
        let first = sweep.plan(&store, NOW - HOUR_MS, NOW, 2).unwrap().unwrap();
        assert_eq!(first.examined, 2);
        assert!(first.keys.is_empty());
        let second = sweep.plan(&store, NOW - HOUR_MS, NOW, 2).unwrap().unwrap();
        assert_eq!(second.examined, 2);
        assert_eq!(
            second
                .keys
                .iter()
                .map(|key| key.1.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(
            sweep
                .plan(&store, NOW - HOUR_MS, NOW, 2)
                .unwrap()
                .unwrap()
                .examined,
            0
        );
        conn.execute(
            "UPDATE account_affinity SET lease_valid_until=?1",
            [NOW - 1],
        )
        .unwrap();
        assert!(sweep
            .plan(&store, NOW - HOUR_MS, NOW + HOUR_MS - 1, 2)
            .unwrap()
            .is_none());
        let next = sweep
            .plan(&store, NOW - HOUR_MS, NOW + HOUR_MS, 2)
            .unwrap()
            .unwrap();
        assert_eq!(
            next.keys
                .iter()
                .map(|key| key.1.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn count(path: &std::path::Path, sql: &str) -> i64 {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(sql, [], |row| row.get(0))
            .unwrap()
    }

    /// Decision ids in or out of the default 1-in-20 full-detail sample.
    fn decision_ids(sampled: bool, wanted: usize) -> Vec<String> {
        let one_in = RetentionWindows::default().model_route_decisions_full_detail_sample_one_in;
        (0..)
            .map(|index| format!("dec-{index}"))
            .filter(|id| striatum_store::in_full_detail_sample(id, one_in) == sampled)
            .take(wanted)
            .collect()
    }

    fn full_score() -> serde_json::Value {
        // Not eligible, so recording it adds no unscored-model sightings.
        let candidate = |route: &str| {
            json!({"routeKey": route, "model": route, "elo": 1.0,
            "eq": 1.0, "eloSource": "curated", "eqSource": "curated", "eligible": false})
        };
        json!({
            "candidates": [candidate("p/pick"), candidate("q/also"), candidate("r/other"),
                candidate("s/other")],
            "adequacy": 1.0,
            "selectionCost": 0.3,
            "metrics": {"elo": {"fact": 1300.0, "target": 1250.0}},
            "objective": {"cost": 0.3, "taskPricing": {"valueMultiplier": 6.0}},
            "alsoAdequate": ["q/also"],
            "pricingMode": "basket",
        })
    }

    /// Record `id` as a decision created at `created_at`, with full detail.
    fn record_decision_at(
        store: &SqliteRouteStateStore,
        path: &std::path::Path,
        id: &str,
        created_at: i64,
    ) {
        store
            .record_decision(&striatum_core::route_state_store::DecisionInput {
                decision_id: Some(id.into()),
                decision_kind: "single".into(),
                target_agent: "mason".into(),
                requirements_json: r#"{"iq":70}"#.into(),
                selected_route_key: Some("p/pick".into()),
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("pick".into()),
                score_json: full_score().to_string(),
                rejected_json: r#"[{"routeKey":"t/walled","reason":"quota_exhausted"}]"#.into(),
                explanation_json: Some(r#"["cost-primary"]"#.into()),
                quota_signal_json: Some(
                    r#"{"state":"present","signals":[{"capacityKey":"p","pressure":0.2}]}"#.into(),
                ),
                policy_revision: Some("policy-1".into()),
                ..Default::default()
            })
            .unwrap();
        rusqlite::Connection::open(path)
            .unwrap()
            .execute(
                "UPDATE model_route_decisions SET created_at = ?1 WHERE id = ?2",
                rusqlite::params![created_at, id],
            )
            .unwrap();
    }

    /// Past the two-day window a decision keeps its compact record, and a
    /// sampled decision keeps full detail until the decision window deletes it.
    #[test]
    fn a_pass_compacts_scoring_detail_after_two_days_except_for_the_sample() {
        let (dir, path, store) = scratch_store("detail");
        let plain = decision_ids(false, 3);
        let sampled = decision_ids(true, 2);
        record_decision_at(&store, &path, &plain[0], NOW - 3 * DAY_MS);
        record_decision_at(&store, &path, &plain[1], NOW - 29 * DAY_MS);
        record_decision_at(&store, &path, &plain[2], NOW - DAY_MS);
        record_decision_at(&store, &path, &sampled[0], NOW - 3 * DAY_MS);
        record_decision_at(&store, &path, &sampled[1], NOW - 29 * DAY_MS);

        // Diagnostics read the adequate set and rejections back over up to a
        // week; compaction must not change what they see.
        let diagnostics_before = store
            .diagnostic_decisions(NOW - 30 * DAY_MS, NOW, 100)
            .unwrap();
        assert_eq!(diagnostics_before.len(), 5);
        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(
            store
                .diagnostic_decisions(NOW - 30 * DAY_MS, NOW, 100)
                .unwrap(),
            diagnostics_before
        );
        assert_eq!(report.decision_details_stripped, 2);
        assert_eq!(report.decision_details_sampled, 2);
        assert!(report.drained);
        for id in &plain[..2] {
            let row = store.get_decision_by_id(id).unwrap().unwrap();
            assert_eq!(row.detail_stripped_at, Some(NOW), "{id}");
            let score: serde_json::Value = serde_json::from_str(&row.score_json).unwrap();
            assert_eq!(score["alsoAdequate"], json!(["q/also"]));
            assert_eq!(score["objective"]["taskPricing"]["valueMultiplier"], 6.0);
            assert_eq!(score["metrics"], full_score()["metrics"]);
            assert_eq!(score["candidates"].as_array().unwrap().len(), 2);
            assert_eq!(score["candidatesOmitted"], 2);
            let quota: serde_json::Value =
                serde_json::from_str(row.quota_signal_json.as_deref().unwrap()).unwrap();
            assert_eq!(quota, json!({"state": "present", "signalsOmitted": 1}));
            // The compact record keeps everything else as recorded.
            assert_eq!(
                row.rejected_json,
                r#"[{"routeKey":"t/walled","reason":"quota_exhausted"}]"#
            );
            assert_eq!(row.explanation_json.as_deref(), Some(r#"["cost-primary"]"#));
            assert_eq!(row.policy_revision.as_deref(), Some("policy-1"));
            assert_eq!(row.requirements_json, r#"{"iq":70}"#);
        }
        for id in [&plain[2], &sampled[0], &sampled[1]] {
            let row = store.get_decision_by_id(id).unwrap().unwrap();
            assert_eq!(row.detail_stripped_at, None, "{id}");
            let score: serde_json::Value = serde_json::from_str(&row.score_json).unwrap();
            assert_eq!(score, full_score(), "{id}");
        }

        // Two days later the young decision is compacted too; the sampled
        // decisions stay full until the thirty-day window deletes the older.
        let later = NOW + 2 * DAY_MS;
        let report = run_retention_pass(&store, &RetentionWindows::default(), later, &no_pause());
        assert_eq!(report.decision_details_stripped, 1);
        assert_eq!(
            report.route_decisions, 2,
            "the 29-day-old pair is past 30 days"
        );
        assert!(store.get_decision_by_id(&sampled[1]).unwrap().is_none());
        let sample = store.get_decision_by_id(&sampled[0]).unwrap().unwrap();
        assert_eq!(sample.detail_stripped_at, None);
        assert_eq!(
            store
                .get_decision_by_id(&plain[2])
                .unwrap()
                .unwrap()
                .detail_stripped_at,
            Some(later)
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Compaction walks at most its batch size per batch and its batch budget
    /// per pass, passes sampled decisions without stopping, and the next pass
    /// continues where the last stopped. Delete batches keep their own bound.
    #[test]
    fn retention_batches_respect_their_bounds() {
        let (dir, path, store) = scratch_store("bounds");
        let sampled = decision_ids(true, 1);
        let plain = decision_ids(false, 4);
        record_decision_at(&store, &path, &sampled[0], NOW - 3 * DAY_MS);
        for (index, id) in plain.iter().enumerate() {
            record_decision_at(&store, &path, id, NOW - 3 * DAY_MS + 1 + index as i64);
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        for index in 0..5 {
            connection
                .execute(
                    "INSERT INTO quota_reading_log (account_key, window_key, observed_at, \
                       used_percent, is_stale) VALUES ('a/b', 'w', ?1, 10.0, 0)",
                    [NOW - 4 * DAY_MS + index],
                )
                .unwrap();
        }
        let budget = PassBudget {
            batch_rows: 2,
            detail_batch_rows: 2,
            max_detail_batches: 1,
            max_batches_per_table: 1,
            max_shadow_purge_batches: 1,
            pause: Duration::ZERO,
        };
        let windows = RetentionWindows::default();
        let passes: Vec<(usize, usize, usize, bool)> = (0..3)
            .map(|_| {
                let report = run_retention_pass(&store, &windows, NOW, &budget);
                (
                    report.decision_details_stripped,
                    report.decision_details_sampled,
                    report.quota_readings,
                    report.drained,
                )
            })
            .collect();
        assert_eq!(
            passes,
            [(1, 1, 2, false), (2, 0, 2, false), (1, 0, 1, true)]
        );
        assert_eq!(
            count(
                &path,
                "SELECT COUNT(*) FROM model_route_decisions WHERE detail_stripped_at IS NULL"
            ),
            1
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn insert_admission(
        connection: &rusqlite::Connection,
        admission_id: &str,
        state: &str,
        model_decision_id: Option<&str>,
        times: (i64, Option<i64>, Option<i64>),
    ) {
        let (committed_at, finished_at, released_at) = times;
        connection
            .execute(
                "INSERT INTO account_admission (principal, admission_id, request_fingerprint, \
                   request_mode, decision_id, conversation_hash, provider, model, account_id, \
                   credential_id, pin_id, pin_generation, state, charged, request_bytes, \
                   request_bytes_estimated, committed_at, admission_valid_until, \
                   lease_valid_until, reply_json, run_id, started_at, finished_at, released_at, \
                   model_decision_id) \
                 VALUES ('runner', ?1, 'fp', 'admit', 'acct-dec', 'conv', 'p', 'pick', 'acct', \
                   'cred', 'pin', 1, ?2, 0, 0, 0, ?3, ?3, ?3, '{}', 'run', ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    admission_id,
                    state,
                    committed_at,
                    finished_at,
                    released_at,
                    model_decision_id
                ],
            )
            .unwrap();
    }

    /// routing/v1 replay records go a week after their decision, with their
    /// reports, unless a live run still names the decision or admission.
    #[test]
    fn a_pass_prunes_v1_replay_records_but_keeps_what_a_live_run_names() {
        let (dir, path, store) = scratch_store("v1");
        let connection = seeding_connection(&path);
        let old = NOW - 8 * DAY_MS;
        let recent = NOW - DAY_MS;
        for (id, created_at) in [
            ("old-plain", old),
            ("old-live-admission", old),
            ("old-continued", old),
            ("old-previous", old),
            ("recent", recent),
        ] {
            connection
                .execute(
                    "INSERT INTO routing_v1_decision (decision_id, principal, consumer, provider, \
                       model, runner_module, created_at_ms, expires_at_ms) \
                     VALUES (?1, 'runner', 'mason', 'p', 'pick', 'broca', ?2, ?2 + 600000)",
                    rusqlite::params![id, created_at],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO routing_v1_report (decision_id, report_id, body_json, \
                       answer_json, created_at_ms) VALUES (?1, 'r', '{}', '{}', ?2)",
                    rusqlite::params![id, created_at],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO routing_v1_report_delivery (decision_id, delivery_key, \
                       report_id, body_json, answer_json, created_at_ms) \
                     VALUES (?1, 'outcome', 'r', '{}', '{}', ?2)",
                    rusqlite::params![id, created_at],
                )
                .unwrap();
        }
        // A report and delivery whose decision binding is already gone.
        connection
            .execute_batch(&format!(
                "INSERT INTO routing_v1_report VALUES ('gone', 'r', '{{}}', '{{}}', {old}); \
                 INSERT INTO routing_v1_report_delivery VALUES \
                   ('gone', 'usage', 'r', '{{}}', '{{}}', {old});"
            ))
            .unwrap();
        // A run still in flight on one decision, and a live continuation
        // linking two others.
        insert_admission(
            &connection,
            "live",
            "started",
            Some("old-live-admission"),
            (old, None, None),
        );
        insert_admission(
            &connection,
            "live-original",
            "started",
            None,
            (old, None, None),
        );
        connection
            .execute(
                "INSERT INTO account_continue (principal, selection_id, request_json, \
                   answer_json, run_id, admission_id, previous_decision_id, decision_id, \
                   reservation_id, valid_until_ms, used_at_ms) \
                 VALUES ('runner', 'sel', '{}', '{}', 'run', 'live-original', 'old-previous', \
                   'old-continued', 'reservation', ?1, ?1)",
                [old],
            )
            .unwrap();
        for (admission_id, body, created_at) in [
            ("live", r#"{"decision_id":"other"}"#, old),
            ("other", r#"{"decision_id":"old-live-admission"}"#, old),
            ("finished", r#"{"decision_id":"old-plain"}"#, old),
            ("fresh", r#"{"decision_id":"recent"}"#, recent),
        ] {
            connection
                .execute(
                    "INSERT INTO routing_v1_admission (principal, admission_id, body_json, \
                       answer_json, created_at_ms) VALUES ('runner', ?1, ?2, '{}', ?3)",
                    rusqlite::params![admission_id, body, created_at],
                )
                .unwrap();
        }
        for (affinity, decision, updated_at) in
            [("stale", "old-plain", old), ("warm", "recent", recent)]
        {
            connection
                .execute(
                    "INSERT INTO routing_v1_affinity (principal, consumer, affinity, \
                       decision_id, updated_at_ms) VALUES ('runner', 'mason', ?1, ?2, ?3)",
                    rusqlite::params![affinity, decision, updated_at],
                )
                .unwrap();
        }

        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(report.v1_decisions, 1);
        assert_eq!(report.v1_reports, 2, "old-plain's report and the orphan");
        assert_eq!(report.v1_report_deliveries, 2);
        assert_eq!(report.v1_admissions, 1);
        assert_eq!(report.v1_affinities, 1);
        assert_eq!(report.account_continuations, 0, "its run is live");
        let left = |table: &str, key: &str| -> Vec<String> {
            let mut statement = connection
                .prepare(&format!("SELECT {key} FROM {table} ORDER BY {key}"))
                .unwrap();
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap();
            rows.collect::<rusqlite::Result<_>>().unwrap()
        };
        let kept = [
            "old-continued",
            "old-live-admission",
            "old-previous",
            "recent",
        ];
        assert_eq!(left("routing_v1_decision", "decision_id"), kept);
        assert_eq!(left("routing_v1_report", "decision_id"), kept);
        assert_eq!(left("routing_v1_report_delivery", "decision_id"), kept);
        assert_eq!(
            left("routing_v1_admission", "admission_id"),
            ["fresh", "live", "other"]
        );
        assert_eq!(left("routing_v1_affinity", "affinity"), ["warm"]);

        // Used moves remain fork bindings while their parent admission is
        // retained. Age the finished admissions past their thirty-day window
        // before expecting the old move and its replay records to retire.
        let history_old = NOW - 31 * DAY_MS;
        connection
            .execute_batch(&format!(
                "UPDATE account_admission SET state = 'finished', committed_at={history_old},
                 admission_valid_until={history_old}, lease_valid_until={history_old},
                 started_at={history_old}, finished_at={history_old}, last_observed_at=NULL"
            ))
            .unwrap();
        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(report.account_continuations, 1);
        assert_eq!(report.v1_decisions, 3);
        assert_eq!(report.v1_admissions, 2);
        assert_eq!(left("routing_v1_decision", "decision_id"), ["recent"]);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A continuation answer goes a week after its run finished or its offer
    /// lapsed, never while its run is live.
    #[test]
    fn a_pass_prunes_continuations_only_after_their_run_is_over() {
        let (dir, path, store) = scratch_store("continue");
        let connection = seeding_connection(&path);
        let old = NOW - 8 * DAY_MS;
        let recent = NOW - DAY_MS;
        insert_admission(
            &connection,
            "finished-long-ago",
            "finished",
            None,
            (old, Some(old), None),
        );
        insert_admission(&connection, "running", "started", None, (old, None, None));
        insert_admission(
            &connection,
            "finished-yesterday",
            "finished",
            None,
            (old, Some(recent), None),
        );
        for (selection, admission, valid_until, released_at) in [
            ("released-offer", "finished-long-ago", Some(old), Some(old)),
            ("hold", "finished-long-ago", None, None),
            ("live-run", "running", Some(old), Some(old)),
            (
                "run-just-finished",
                "finished-yesterday",
                Some(old),
                Some(old),
            ),
            ("admission-pruned", "gone", Some(old), None),
            (
                "released-yesterday",
                "finished-long-ago",
                Some(old),
                Some(recent),
            ),
        ] {
            connection
                .execute(
                    "INSERT INTO account_continue (principal, selection_id, request_json, \
                       answer_json, run_id, admission_id, previous_decision_id, decision_id, \
                       reservation_id, valid_until_ms, released_at_ms) \
                     VALUES ('runner', ?1, '{}', '{}', 'run', ?2, 'previous', \
                       CASE WHEN ?3 IS NULL THEN NULL ELSE 'next-' || ?1 END, \
                       CASE WHEN ?3 IS NULL THEN NULL ELSE 'reservation-' || ?1 END, ?3, ?4)",
                    rusqlite::params![selection, admission, valid_until, released_at],
                )
                .unwrap();
        }

        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(report.account_continuations, 3);
        let mut statement = connection
            .prepare("SELECT selection_id FROM account_continue ORDER BY selection_id")
            .unwrap();
        let left: Vec<String> = statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            left,
            ["live-run", "released-yesterday", "run-just-finished"]
        );
        drop(statement);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Speed samples, call counts, retired fence origins, unscored-model
    /// sightings and quota readings each go at their own window, and nothing
    /// a reader still uses goes with them.
    #[test]
    fn a_pass_bounds_the_other_growing_tables_at_their_windows() {
        let (dir, path, store) = scratch_store("growing");
        let connection = rusqlite::Connection::open(&path).unwrap();
        let mut sample = 0;
        let mut insert_sample = |route: &str, observed_at: i64| {
            sample += 1;
            connection
                .execute(
                    "INSERT INTO model_route_speed_samples (id, route_key, provider_id, \
                       model_id, output_tokens, duration_ms, observed_at, \
                       population_service_tier, population_speed) \
                     VALUES (?1, ?2, 'p', 'm', 2000, 4000, ?3, 'standard', 'standard')",
                    rusqlite::params![format!("sample-{sample}"), route, observed_at],
                )
                .unwrap();
        };
        // A busy route: 5 samples inside the seven-day read window and 25
        // past it, of which the 15 newest stay, so the route keeps its newest
        // 20.
        for index in 0..25 {
            insert_sample("p/busy", NOW - 8 * DAY_MS - index * HOUR_MS);
        }
        for index in 0..5 {
            insert_sample("p/busy", NOW - DAY_MS - index);
        }
        // A quiet route: two old samples stay, one past the 30-day cap goes.
        insert_sample("p/quiet", NOW - 10 * DAY_MS);
        insert_sample("p/quiet", NOW - 11 * DAY_MS);
        insert_sample("p/quiet", NOW - 31 * DAY_MS);
        connection
            .execute_batch(&format!(
                "UPDATE routing_call_window SET started_at_ms = {start};
                 INSERT INTO routing_call_count VALUES ('route.old', 's', 'p', 3, {idle});
                 INSERT INTO routing_call_count VALUES ('route.select', 's', 'p', 9, {recent});
                 INSERT INTO retirement_tombstone VALUES ('route', '[\"x/old\"]', 'e', 0, {idle});
                 INSERT INTO retirement_tombstone VALUES ('route', '[\"x/new\"]', 'e', 0, {recent});
                 INSERT INTO routing_unscored_admissible_models VALUES ('m/old', 0, {idle});
                 INSERT INTO routing_unscored_admissible_models VALUES ('m/new', 0, {recent});
                 INSERT INTO quota_reading_log (account_key, window_key, observed_at, used_percent, is_stale)
                   VALUES ('a/b', 'w', {four_days}, 10.0, 0), ('a/b', 'w', {two_days}, 20.0, 0);",
                start = NOW - 100 * DAY_MS,
                idle = NOW - 31 * DAY_MS,
                recent = NOW - DAY_MS,
                four_days = NOW - 4 * DAY_MS,
                two_days = NOW - 2 * DAY_MS,
            ))
            .unwrap();

        let report = run_retention_pass(&store, &RetentionWindows::default(), NOW, &no_pause());
        assert_eq!(report.speed_samples, 11);
        assert_eq!(report.routing_call_counts, 1);
        assert_eq!(report.retired_fence_origins, 1);
        assert_eq!(report.unscored_admissible_models, 1);
        assert_eq!(report.quota_readings, 1);
        assert!(report.drained);
        assert_eq!(
            count(
                &path,
                "SELECT COUNT(*) FROM model_route_speed_samples WHERE route_key = 'p/busy'"
            ),
            20
        );
        assert_eq!(
            count(
                &path,
                "SELECT MIN(observed_at) FROM model_route_speed_samples WHERE route_key = 'p/busy'"
            ),
            NOW - 8 * DAY_MS - 14 * HOUR_MS
        );
        assert_eq!(
            count(
                &path,
                "SELECT COUNT(*) FROM model_route_speed_samples WHERE route_key = 'p/quiet'"
            ),
            2
        );
        // The observed-speed read still finds its five samples in the window.
        assert!(store
            .get_observed_speed("p/busy", NOW as f64)
            .unwrap()
            .is_some());
        assert_eq!(
            count(&path, "SELECT started_at_ms FROM routing_call_window"),
            NOW - ROUTING_CALL_COUNT_WINDOW_MS
        );
        assert_eq!(count(&path, "SELECT calls FROM routing_call_count"), 9);
        assert_eq!(
            count(&path, "SELECT retired_at FROM retirement_tombstone"),
            NOW - DAY_MS
        );
        assert_eq!(
            count(
                &path,
                "SELECT COUNT(*) FROM routing_unscored_admissible_models"
            ),
            1
        );
        assert_eq!(
            count(&path, "SELECT observed_at FROM quota_reading_log"),
            NOW - 2 * DAY_MS
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
