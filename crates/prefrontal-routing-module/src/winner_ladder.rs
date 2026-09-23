//! Announce when the top route at a demand tier changes.
//!
//! A catalog placement or a config reload can quietly make a different model the
//! route every caller at some demand tier receives. Substrate owners (the
//! OpenCode host, Broca) need to hear about that before campaigns land on the new
//! model, so each triggering change computes the winner ladder and compares it
//! with the last ANNOUNCED ladder, which is kept in the routing store so the
//! comparison survives a restart.
//!
//! The ladder is a dry run of the router's own selection function
//! ([`select_model_route_with_config`]) over exactly what live selection last
//! used on each substrate: the availability set, subscription classification,
//! and runtime snapshot that `route.select` (host) and `route.select_panel`
//! (Broca legs) were handed most recently. Nothing is guessed from credential
//! files. Until a real selection has arrived since boot there is no ladder at
//! all, and nothing is compared or announced.
//!
//! Each tier carries two winners: one with no runtime snapshot (quota, walls,
//! cooldowns and backoffs ignored) and one under the recorded live snapshot. A
//! tier is announced only when BOTH moved since the last announced ladder:
//! quota drift alone moves only the live winner, and a placement that live
//! selection does not act on (because quota keeps traffic elsewhere) moves only
//! the quota-neutral one. The notice names the live winners, since that is what
//! callers are actually being sent to, and carries the quota-neutral pair and
//! the rule alongside.
//!
//! Delivery: the routing module holds no room identity and no connection to the
//! core module's room plane (`rooms.post` requires a room member as caller), so
//! the notice is written to stderr at the fixed [`NOTICE_PREFIX`]. The daemon
//! captures module stderr in `run/logs/prefrontal-routing.stderr.log`, where the
//! health sentinel's route-winner arm reads and relays it.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use prefrontal_routing_core::routing_config::DEFAULT_ROUTING_CONFIG;
use prefrontal_routing_core::{
    resolve_launch_requirements, select_model_route_with_config, ModelRoute,
    ModelRouteStateSnapshot, RequestedModelRequirements, RouteStateStore, SelectModelRouteInput,
};
use prefrontal_routing_store::SqliteRouteStateStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config_overrides;

/// Fixed prefix of every winner-change notice line. The health sentinel matches
/// on this exact text, so changing it silently disconnects the relay.
pub(crate) const NOTICE_PREFIX: &str = "[prefrontal-routing] route-winner-changed ";

/// The announcement rule, carried in every notice so a reader does not have to
/// know this module to interpret the routes it names.
pub(crate) const NOTICE_RULE: &str = "a tier is listed only when its winner moved since the last announced ladder both with quota ignored and under the latest live quota snapshot; oldRoute/newRoute are the live-snapshot winners (what selection sends to now), quotaNeutralOldRoute/quotaNeutralNewRoute the winners with quota ignored";

/// Caller IQ demand tiers the ladder evaluates, on the wire's 0-100 scale.
pub(crate) const LADDER_IQ_TIERS: [u8; 7] = [30, 50, 60, 70, 80, 90, 100];

/// The target agent the dry run resolves demand for. The scorer does not branch
/// on target agent (it only names it in the explanation), and the launch policy
/// only special-cases agents that send NO demand; with an explicit IQ every
/// routed agent resolves to the same requirements. One ladder therefore stands
/// for every routed target agent. Mason is used because it is the agent whose
/// launch policy insists on explicit demand, so the resolved EQ is the plain
/// default rather than a specialist fallback.
const LADDER_TARGET_AGENT: &str = "mason";

/// Selections arriving through `route.select`: the OpenCode host's own list.
pub(crate) const SUBSTRATE_HOST: &str = "opencode-host";
/// Panel legs that send through Broca (`route.select_panel` with substrate broca).
pub(crate) const SUBSTRATE_BROCA: &str = "broca";

/// How often the watcher checks the config files and the observed availability.
pub(crate) const CONFIG_WATCH_INTERVAL: Duration = Duration::from_secs(30);

/// What one live selection was handed: the ladder replays exactly this.
#[derive(Debug, Clone)]
pub(crate) struct ObservedSelection {
    pub available: BTreeSet<String>,
    pub subscription_keys: BTreeSet<String>,
    pub snapshot: ModelRouteStateSnapshot,
}

/// The most recent selection inputs per substrate, in memory only. Bounded by
/// construction: one entry per substrate constant, each replaced on arrival.
#[derive(Debug, Default)]
pub(crate) struct ObservedSelections {
    by_substrate: Mutex<BTreeMap<&'static str, ObservedSelection>>,
}

impl ObservedSelections {
    pub(crate) fn record(
        &self,
        substrate: &'static str,
        available: impl IntoIterator<Item = String>,
        subscription_keys: impl IntoIterator<Item = String>,
        snapshot: ModelRouteStateSnapshot,
    ) {
        let observation = ObservedSelection {
            available: available.into_iter().collect(),
            subscription_keys: subscription_keys.into_iter().collect(),
            snapshot,
        };
        if let Ok(mut guard) = self.by_substrate.lock() {
            guard.insert(substrate, observation);
        }
    }

    pub(crate) fn current(&self) -> BTreeMap<&'static str, ObservedSelection> {
        self.by_substrate
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Availability sets only: the watcher's trigger must not fire on quota drift.
    fn availability(&self) -> BTreeMap<&'static str, BTreeSet<String>> {
        self.current()
            .into_iter()
            .map(|(substrate, observation)| (substrate, observation.available))
            .collect()
    }
}

/// The process-wide record the selection handlers write and the ladder reads.
pub(crate) fn observed_selections() -> Arc<ObservedSelections> {
    static OBSERVED: OnceLock<Arc<ObservedSelections>> = OnceLock::new();
    Arc::clone(OBSERVED.get_or_init(|| Arc::new(ObservedSelections::default())))
}

/// Everything the ladder reads besides the store. Production uses the live
/// config-home path and the process-wide observations; tests supply their own.
#[derive(Debug, Clone)]
pub(crate) struct LadderSources {
    pub routing_config: PathBuf,
    pub operator_config: PathBuf,
    pub observed: Arc<ObservedSelections>,
}

impl LadderSources {
    pub(crate) fn live(routing_config: &Path, operator_config: &Path) -> Self {
        Self {
            routing_config: routing_config.to_path_buf(),
            operator_config: operator_config.to_path_buf(),
            observed: observed_selections(),
        }
    }
}

/// The two winners of one tier. `None` means selection found no route.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TierWinners {
    /// Winner with no runtime snapshot: quota, walls, and cooldowns ignored.
    pub neutral: Option<String>,
    /// Winner under the substrate's latest recorded live snapshot.
    pub live: Option<String>,
}

/// One substrate's ladder and the availability set it was computed over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SubstrateLadder {
    pub available: Vec<String>,
    pub tiers: BTreeMap<u8, TierWinners>,
}

/// Winners per substrate and IQ tier at the default EQ.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WinnerLadder {
    pub substrates: BTreeMap<String, SubstrateLadder>,
    pub eq: f64,
}

/// One move on one substrate and every tier it happened at.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WinnerChange {
    pub substrate: String,
    pub old_route: Option<String>,
    pub new_route: Option<String>,
    pub quota_neutral_old_route: Option<String>,
    pub quota_neutral_new_route: Option<String>,
    pub tiers: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WinnerChangeNotice {
    pub trigger: String,
    pub eq: f64,
    pub changes: Vec<WinnerChange>,
}

impl WinnerChangeNotice {
    /// One log line: the fixed prefix followed by a JSON object.
    pub(crate) fn render(&self, at_ms: u64) -> String {
        let changes: Vec<serde_json::Value> = self
            .changes
            .iter()
            .map(|change| {
                json!({
                    "substrate": change.substrate,
                    "oldRoute": change.old_route,
                    "newRoute": change.new_route,
                    "quotaNeutralOldRoute": change.quota_neutral_old_route,
                    "quotaNeutralNewRoute": change.quota_neutral_new_route,
                    "tiers": change.tiers,
                    // The substrate whose selection moved; kept under the name the
                    // health sentinel already reads.
                    "newRouteSubstrates": [change.substrate],
                })
            })
            .collect();
        let body = json!({
            "atMs": at_ms,
            "trigger": self.trigger,
            "eq": self.eq,
            "agents": "every routed target agent (selection does not branch on target agent)",
            "rule": NOTICE_RULE,
            "changes": changes,
        });
        format!("{NOTICE_PREFIX}{body}")
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Compute both winners at every tier for every substrate a live selection has
/// been observed on. `Ok(None)` means no selection has arrived since boot: there
/// is nothing to replay, and an empty ladder must not be mistaken for one.
///
/// Live multipliers, Elo overrides, the demand exponent, the best-effort band,
/// and the operator's exclusions are read from the routing config exactly as a
/// selection reads them.
pub(crate) fn compute_ladder(
    store: &impl RouteStateStore,
    sources: &LadderSources,
) -> Result<Option<WinnerLadder>, String> {
    let observed = sources.observed.current();
    if observed.is_empty() {
        return Ok(None);
    }
    // Multipliers are process-wide and priced into facts as they are read, so
    // they must be loaded BEFORE the facts, as `route.select` does. Read in the
    // other order, the first ladder after boot priced every subscription route
    // at list price and disagreed with every later ladder on identical facts.
    // A broken document keeps the last good multipliers, as selection does.
    let _ = config_overrides::reload_live_multipliers(&sources.routing_config);
    let now = now_ms() as f64;
    let mut facts = store
        .get_facts_with_observed_speed(now)
        .map_err(|error| format!("reading facts: {error}"))?;
    let routing_config =
        config_overrides::apply_live_selection_config(&sources.routing_config, &mut facts)
            .map(|outcome| outcome.routing_config)
            .unwrap_or(DEFAULT_ROUTING_CONFIG);
    let min_best_effort_adequacy =
        config_overrides::read_min_best_effort_adequacy(&sources.routing_config)
            .ok()
            .flatten();
    let operator = config_overrides::read_operator_routing_policy(&sources.operator_config);

    let mut substrates = BTreeMap::new();
    let mut ladder_eq = 0.0;
    for (substrate, observation) in observed {
        let available: HashSet<String> = observation.available.iter().cloned().collect();
        let mut tiers = BTreeMap::new();
        for tier in LADDER_IQ_TIERS {
            let resolved = resolve_launch_requirements(
                LADDER_TARGET_AGENT,
                Some(&RequestedModelRequirements {
                    iq: Some(f64::from(tier)),
                    eq: None,
                    caps: None,
                }),
            )
            .map_err(|error| error.message)?;
            ladder_eq = resolved.requirements.eq;
            let select = |runtime_snapshot: Option<ModelRouteStateSnapshot>| {
                let subscription = observation.subscription_keys.clone();
                // The ladder runs inside placement handlers and background loops.
                // If the router panics on this availability set (its ordering has
                // panicked with "comparison function does not correctly implement
                // a total order" on mixed subscription catalogs), the change must
                // still complete: the ladder is reported unavailable and nothing
                // is announced for it.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    select_model_route_with_config(
                        SelectModelRouteInput {
                            target_agent: LADDER_TARGET_AGENT.to_string(),
                            requirements: resolved.requirements,
                            available_models: available.clone(),
                            facts: facts.clone(),
                            runtime_snapshot,
                            exclude_route_keys: Vec::new(),
                            operator_exclude_keys: operator.exclude.clone(),
                            prefer_fast_specs: Vec::new(),
                            required_capabilities: resolved.required_capabilities,
                            is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                                subscription.contains(&route.route_key)
                                    || subscription.contains(&route.capacity_key)
                            })),
                            min_best_effort_adequacy,
                            prefer_prompt_cache: false,
                            providers_without_prompt_cache: HashSet::new(),
                        },
                        &routing_config,
                    )
                    .ok()
                    .map(|selected| selected.route_key)
                }))
                .map_err(|_| format!("route selection panicked at iq {tier} on {substrate}"))
            };
            let neutral = select(None)?;
            let live = select(Some(observation.snapshot.clone()))?;
            tiers.insert(tier, TierWinners { neutral, live });
        }
        substrates.insert(
            substrate.to_string(),
            SubstrateLadder {
                available: observation.available.into_iter().collect(),
                tiers,
            },
        );
    }
    Ok(Some(WinnerLadder {
        substrates,
        eq: ladder_eq,
    }))
}

/// Compare a ladder with the last announced one. A tier counts only when both
/// its quota-neutral and its live winner differ; a substrate the baseline has
/// never seen is not a move. Moves group by substrate and route pairs.
pub(crate) fn diff_ladders(
    baseline: &WinnerLadder,
    current: &WinnerLadder,
    trigger: &str,
) -> Option<WinnerChangeNotice> {
    type Key = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let mut grouped: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
    for (substrate, ladder) in &current.substrates {
        let Some(before) = baseline.substrates.get(substrate) else {
            continue;
        };
        for (tier, now) in &ladder.tiers {
            let then = before.tiers.get(tier).cloned().unwrap_or_default();
            if then.neutral == now.neutral || then.live == now.live {
                continue;
            }
            grouped
                .entry((
                    substrate.clone(),
                    then.live,
                    now.live.clone(),
                    then.neutral,
                    now.neutral.clone(),
                ))
                .or_default()
                .push(*tier);
        }
    }
    if grouped.is_empty() {
        return None;
    }
    Some(WinnerChangeNotice {
        trigger: trigger.to_string(),
        eq: current.eq,
        changes: grouped
            .into_iter()
            .map(
                |((substrate, old_route, new_route, neutral_old, neutral_new), tiers)| {
                    WinnerChange {
                        substrate,
                        old_route,
                        new_route,
                        quota_neutral_old_route: neutral_old,
                        quota_neutral_new_route: neutral_new,
                        tiers,
                    }
                },
            )
            .collect(),
    })
}

fn store_baseline(store: &SqliteRouteStateStore, ladder: &WinnerLadder, trigger: &str) {
    let written = serde_json::to_string(ladder)
        .map_err(|error| error.to_string())
        .and_then(|json| {
            store
                .put_route_winner_baseline(&json, trigger, now_ms() as i64)
                .map_err(|error| error.to_string())
        });
    if let Err(error) = written {
        eprintln!("[prefrontal-routing] winner ladder baseline not saved: {error}");
    }
}

/// Compute the ladder after a change and compare it with the stored baseline.
///
/// - No selection observed since boot: nothing is computed, compared, or saved.
/// - No stored baseline (the first run ever): the ladder is saved silently.
/// - A move: one notice is emitted and the new ladder becomes the baseline.
/// - No move: the baseline stays, except that a substrate it has never seen is
///   added to it silently.
///
/// Returns the notice for callers that test it. A failure never blocks the
/// change that triggered it.
pub(crate) fn announce(
    store: &SqliteRouteStateStore,
    sources: &LadderSources,
    trigger: &str,
) -> Option<WinnerChangeNotice> {
    let current = match compute_ladder(store, sources) {
        Ok(Some(ladder)) => ladder,
        Ok(None) => return None,
        Err(error) => {
            eprintln!("[prefrontal-routing] winner ladder unavailable ({trigger}): {error}");
            return None;
        }
    };
    let baseline = match store.route_winner_baseline() {
        Ok(Some(json)) => match serde_json::from_str::<WinnerLadder>(&json) {
            Ok(ladder) => Some(ladder),
            Err(error) => {
                eprintln!(
                    "[prefrontal-routing] stored winner ladder unreadable ({error}); replacing it without announcing"
                );
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            // Without the baseline there is nothing honest to compare against,
            // and overwriting it would lose a pending move.
            eprintln!("[prefrontal-routing] winner ladder baseline unreadable: {error}");
            return None;
        }
    };
    let Some(mut baseline) = baseline else {
        store_baseline(store, &current, trigger);
        return None;
    };
    let notice = diff_ladders(&baseline, &current, trigger);
    let mut changed = false;
    for (substrate, ladder) in current.substrates {
        if notice.is_some() || !baseline.substrates.contains_key(&substrate) {
            baseline.substrates.insert(substrate, ladder);
            changed = true;
        }
    }
    if changed {
        baseline.eq = current.eq;
        store_baseline(store, &baseline, trigger);
    }
    if let Some(notice) = &notice {
        emit(notice);
    }
    notice
}

fn emit(notice: &WinnerChangeNotice) {
    let line = notice.render(now_ms());
    eprintln!("{line}");
    #[cfg(test)]
    emitted_lines()
        .lock()
        .expect("emitted notice log poisoned")
        .push(line);
}

/// Every notice line this test process emitted, for asserting on the handlers'
/// wiring. Tests run in parallel, so readers filter by a route key only they use.
#[cfg(test)]
pub(crate) fn emitted_lines() -> &'static Mutex<Vec<String>> {
    static LINES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    LINES.get_or_init(|| Mutex::new(Vec::new()))
}

/// The inputs that can move a winner without a placement, reduced to one
/// comparable value each. Availability is compared as sets only: the recorded
/// runtime snapshot changes on every selection and is deliberately left out.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigFingerprint {
    routing_config: String,
    operator_config: String,
    availability: BTreeMap<&'static str, BTreeSet<String>>,
}

fn file_digest(path: &Path) -> String {
    match std::fs::read(path) {
        Ok(bytes) => format!("sha256:{:x}", Sha256::digest(&bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "absent".to_string(),
        Err(error) => format!("unreadable:{}", error.kind()),
    }
}

fn fingerprint(sources: &LadderSources) -> ConfigFingerprint {
    ConfigFingerprint {
        routing_config: file_digest(&sources.routing_config),
        operator_config: file_digest(&sources.operator_config),
        availability: sources.observed.availability(),
    }
}

/// Watches the config files the ladder reads and the availability live
/// selection reports. A change (subscription multipliers, exclusions, Elo or
/// model overrides, a provider appearing in or leaving a substrate's list, or
/// the first selection after boot) gets a ladder comparison; an unchanged
/// fingerprint costs a few small file reads and no selection.
pub(crate) struct ConfigReloadWatch {
    last: ConfigFingerprint,
}

impl ConfigReloadWatch {
    pub(crate) fn new(sources: &LadderSources) -> Self {
        Self {
            last: fingerprint(sources),
        }
    }

    pub(crate) fn check(
        &mut self,
        store: &SqliteRouteStateStore,
        sources: &LadderSources,
    ) -> Option<WinnerChangeNotice> {
        let current = fingerprint(sources);
        if current == self.last {
            return None;
        }
        let mut changed = Vec::new();
        if current.routing_config != self.last.routing_config {
            changed.push(format!(
                "routing config {}",
                sources.routing_config.display()
            ));
        }
        if current.operator_config != self.last.operator_config
            && sources.operator_config != sources.routing_config
        {
            changed.push(format!(
                "operator config {}",
                sources.operator_config.display()
            ));
        }
        let substrates: BTreeSet<&&str> = current
            .availability
            .keys()
            .chain(self.last.availability.keys())
            .collect();
        for substrate in substrates {
            if current.availability.get(*substrate) != self.last.availability.get(*substrate) {
                changed.push(format!("{substrate} availability"));
            }
        }
        self.last = current;
        announce(
            store,
            sources,
            &format!("config or availability change ({})", changed.join(", ")),
        )
    }
}

/// Re-check the config and observed availability on an interval. Nothing is
/// seeded at boot: the first comparison happens once a selection has reported
/// what live routing can reach, against the baseline stored before the restart.
pub(crate) async fn config_reload_watch_loop(
    store: Arc<SqliteRouteStateStore>,
    sources: LadderSources,
) {
    let mut watch = ConfigReloadWatch::new(&sources);
    loop {
        tokio::time::sleep(CONFIG_WATCH_INTERVAL).await;
        watch.check(store.as_ref(), &sources);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ladder(substrate: &str, routes: [(&str, &str); 7]) -> WinnerLadder {
        WinnerLadder {
            substrates: BTreeMap::from([(
                substrate.to_string(),
                SubstrateLadder {
                    available: Vec::new(),
                    tiers: LADDER_IQ_TIERS
                        .iter()
                        .zip(routes)
                        .map(|(tier, (neutral, live))| {
                            (
                                *tier,
                                TierWinners {
                                    neutral: Some(neutral.to_string()),
                                    live: Some(live.to_string()),
                                },
                            )
                        })
                        .collect(),
                },
            )]),
            eq: 0.0,
        }
    }

    /// The health sentinel relays notices by matching this prefix; a rename on
    /// either side would silently stop the relay.
    #[test]
    fn the_sentinel_relays_the_same_notice_prefix() {
        let sentinel = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../script/health-sentinel.ts"
        ))
        .unwrap();
        assert!(sentinel.contains(&format!("ROUTE_WINNER_NOTICE_PREFIX = \"{NOTICE_PREFIX}\"")));
    }

    #[test]
    fn identical_ladders_produce_no_notice() {
        let same = ladder("h", [("a", "a"); 7]);
        assert_eq!(diff_ladders(&same, &same.clone(), "t"), None);
    }

    #[test]
    fn a_move_in_only_one_of_the_two_winners_is_not_announced() {
        let before = ladder("h", [("a", "x"); 7]);
        let quota_only = ladder("h", [("a", "y"); 7]);
        assert_eq!(diff_ladders(&before, &quota_only, "t"), None);
        let neutral_only = ladder("h", [("b", "x"); 7]);
        assert_eq!(diff_ladders(&before, &neutral_only, "t"), None);
    }

    #[test]
    fn a_substrate_missing_from_the_baseline_is_not_a_move() {
        let before = ladder("h", [("a", "a"); 7]);
        let other = ladder("b", [("z", "z"); 7]);
        assert_eq!(diff_ladders(&before, &other, "t"), None);
    }

    #[test]
    fn moves_group_by_route_pair_and_render_on_the_fixed_prefix() {
        let before = ladder(
            "h",
            [
                ("a", "a"),
                ("a", "a"),
                ("b", "b"),
                ("b", "b"),
                ("c", "c"),
                ("c", "c"),
                ("c", "c"),
            ],
        );
        let after = ladder(
            "h",
            [
                ("a", "a"),
                ("a", "a"),
                ("b", "b"),
                ("b", "b"),
                ("c", "c"),
                ("d", "e"),
                ("d", "e"),
            ],
        );
        let notice = diff_ladders(&before, &after, "route.model_upsert x/d").unwrap();
        assert_eq!(notice.changes.len(), 1);
        assert_eq!(notice.changes[0].old_route.as_deref(), Some("c"));
        assert_eq!(notice.changes[0].new_route.as_deref(), Some("e"));
        assert_eq!(
            notice.changes[0].quota_neutral_new_route.as_deref(),
            Some("d")
        );
        assert_eq!(notice.changes[0].tiers, vec![90, 100]);
        let line = notice.render(42);
        let body: serde_json::Value =
            serde_json::from_str(line.strip_prefix(NOTICE_PREFIX).unwrap()).unwrap();
        assert_eq!(body["atMs"], 42);
        assert_eq!(body["trigger"], "route.model_upsert x/d");
        assert_eq!(body["rule"], NOTICE_RULE);
        assert_eq!(body["changes"][0]["tiers"], json!([90, 100]));
        assert_eq!(body["changes"][0]["newRouteSubstrates"], json!(["h"]));
    }

    #[test]
    fn a_ladder_round_trips_through_its_stored_form() {
        let original = ladder("h", [("a", "b"); 7]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(
            serde_json::from_str::<WinnerLadder>(&json).unwrap(),
            original
        );
    }
}
