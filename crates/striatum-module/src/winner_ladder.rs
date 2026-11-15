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
//! and runtime snapshot that `route.select` (host lists, or Broca's reach when
//! the caller names substrate `broca`) and `route.select_panel` (Broca legs)
//! were handed most recently. Nothing is guessed from credential
//! files. Until a real selection has arrived since boot there is no ladder at
//! all, and nothing is compared or announced.
//!
//! Availability is recorded, and the ladder computed, compared, and stored, per
//! [`LadderKey`]: the substrate plus the caller class, which is the target agent
//! `route.select` was asked to route for (panels use [`CALLER_CLASS_PANEL`]).
//! Different callers can hand the same substrate different lists. Core's
//! sidekick classifier, wake author, and comment review once routed `explore`
//! over a frozen seed catalog while the OpenCode plugin sent its live provider
//! list, and keyed by substrate alone the recorded list alternated between the
//! two, every alternation read as an availability change, and a notice fired
//! each time. Those core callers now select on substrate `broca`, but callers of
//! one class can still disagree (two hosts with different providers), so a
//! key's availability is the union of every list recorded for it within
//! [`AVAILABILITY_UNION_WINDOW`].
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
//! captures module stderr in `run/logs/striatum.stderr.log`, where the
//! health sentinel's route-winner arm reads and relays it.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;
use striatum_core::{
    resolve_launch_requirements, select_model_route_with_config, ModelRoute,
    ModelRouteStateSnapshot, RequestedModelRequirements, RouteStateStore, SelectModelRouteInput,
};
use striatum_store::SqliteRouteStateStore;

use crate::config_overrides;

/// Fixed prefix of every winner-change notice line. The health sentinel matches
/// on this exact text, so changing it silently disconnects the relay.
pub(crate) const NOTICE_PREFIX: &str = "[striatum] route-winner-changed ";

/// The announcement rule, carried in every notice so a reader does not have to
/// know this module to interpret the routes it names.
pub(crate) const NOTICE_RULE: &str = "a tier is listed only when its winner moved since the last announced ladder both with quota ignored and under the latest live quota snapshot; oldRoute/newRoute are the live-snapshot winners (what selection sends to now), quotaNeutralOldRoute/quotaNeutralNewRoute the winners with quota ignored";

/// Caller IQ demand tiers the ladder evaluates, on the wire's 0-100 scale.
pub(crate) const LADDER_IQ_TIERS: [u8; 7] = [30, 50, 60, 70, 80, 90, 100];

/// The target agent the dry run resolves demand for, whatever the key's caller
/// class. The scorer does not branch on target agent (it only names it in the
/// explanation), and the launch policy only special-cases agents that send NO
/// demand; with an explicit IQ every routed agent resolves to the same
/// requirements. The caller class therefore chooses only which recorded inputs
/// a ladder replays, not how demand is resolved. Mason is used because it is the
/// agent whose launch policy insists on explicit demand, so the resolved EQ is
/// the plain default rather than a specialist fallback.
const LADDER_TARGET_AGENT: &str = "mason";

/// Selections arriving through `route.select` on the caller's own host list.
pub(crate) const SUBSTRATE_HOST: &str = "opencode-host";
/// Selections that send through Broca: `route.select_panel` and `route.select`
/// called with substrate broca.
pub(crate) const SUBSTRATE_BROCA: &str = crate::legacy_wire::BROCA;
/// Caller class of `route.select_panel` recordings: panels carry no target agent.
pub(crate) const CALLER_CLASS_PANEL: &str = "panel";

/// How long a recorded list keeps contributing to its key's union after it was
/// last seen. Callers of one class arrive minutes apart (the live flap
/// interleaved plugin explore launches and seed-catalog explore selections
/// within five minutes), and a route that any recent caller of the class could
/// reach can receive that class's traffic, so every list seen in the window
/// counts. Six hours spans the gaps between sporadic callers such as gathers
/// and classifier calls while still letting a disconnected provider leave the
/// ladder within a working session.
pub(crate) const AVAILABILITY_UNION_WINDOW: Duration = Duration::from_secs(6 * 60 * 60);
/// Distinct lists kept per key; the least recently seen is dropped first. A
/// caller-supplied list can differ per request, so the record needs a bound.
const MAX_LISTS_PER_KEY: usize = 16;
/// Keys kept in memory; the least recently recorded is dropped first. The
/// target agent is caller-supplied text, so the key space needs a bound too.
const MAX_KEYS: usize = 64;

/// How often the watcher checks the config files and the observed availability.
pub(crate) const CONFIG_WATCH_INTERVAL: Duration = Duration::from_secs(30);

/// Which ladder a recording feeds: the substrate that sends plus the class of
/// caller that asked for the route.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LadderKey {
    pub substrate: &'static str,
    pub caller_class: String,
}

impl LadderKey {
    pub(crate) fn new(substrate: &'static str, caller_class: &str) -> Self {
        let caller_class = caller_class.trim();
        Self {
            substrate,
            caller_class: if caller_class.is_empty() {
                "unknown".to_string()
            } else {
                caller_class.to_string()
            },
        }
    }

    /// The name a notice and the stored baseline use, e.g. `opencode-host / mason`.
    pub(crate) fn label(&self) -> String {
        format!("{} / {}", self.substrate, self.caller_class)
    }
}

/// What the ladder replays for one key: the union of the lists recorded within
/// the window and the most recent runtime snapshot.
#[derive(Debug, Clone)]
pub(crate) struct ObservedSelection {
    pub available: BTreeSet<String>,
    pub subscription_keys: BTreeSet<String>,
    pub snapshot: ModelRouteStateSnapshot,
}

#[derive(Debug, Clone)]
struct SeenList {
    available: BTreeSet<String>,
    subscription_keys: BTreeSet<String>,
    seen_at: Instant,
}

#[derive(Debug, Clone)]
struct KeyRecord {
    /// Newest last.
    lists: Vec<SeenList>,
    snapshot: ModelRouteStateSnapshot,
    last_recorded: Instant,
}

impl KeyRecord {
    /// Drop lists older than the window, always keeping the newest one so a key
    /// that has gone quiet still has something to replay.
    fn prune(&mut self, now: Instant) {
        let newest = self.lists.len().saturating_sub(1);
        let mut index = 0;
        self.lists.retain(|list| {
            let keep = index == newest
                || now.saturating_duration_since(list.seen_at) <= AVAILABILITY_UNION_WINDOW;
            index += 1;
            keep
        });
    }

    fn observation(&self) -> ObservedSelection {
        ObservedSelection {
            available: self
                .lists
                .iter()
                .flat_map(|list| list.available.iter().cloned())
                .collect(),
            subscription_keys: self
                .lists
                .iter()
                .flat_map(|list| list.subscription_keys.iter().cloned())
                .collect(),
            snapshot: self.snapshot.clone(),
        }
    }
}

/// Recent selection inputs per key, in memory only, bounded by [`MAX_KEYS`] and
/// [`MAX_LISTS_PER_KEY`].
#[derive(Debug, Default)]
pub(crate) struct ObservedSelections {
    by_key: Mutex<BTreeMap<LadderKey, KeyRecord>>,
}

impl ObservedSelections {
    pub(crate) fn record(
        &self,
        key: LadderKey,
        available: impl IntoIterator<Item = String>,
        subscription_keys: impl IntoIterator<Item = String>,
        snapshot: ModelRouteStateSnapshot,
    ) {
        self.record_at(key, available, subscription_keys, snapshot, Instant::now());
    }

    /// [`Self::record`] at a given time, so tests can place a list outside the window.
    pub(crate) fn record_at(
        &self,
        key: LadderKey,
        available: impl IntoIterator<Item = String>,
        subscription_keys: impl IntoIterator<Item = String>,
        snapshot: ModelRouteStateSnapshot,
        at: Instant,
    ) {
        let seen = SeenList {
            available: available.into_iter().collect(),
            subscription_keys: subscription_keys.into_iter().collect(),
            seen_at: at,
        };
        let Ok(mut guard) = self.by_key.lock() else {
            return;
        };
        if !guard.contains_key(&key) && guard.len() >= MAX_KEYS {
            let stalest = guard
                .iter()
                .min_by_key(|(_, record)| record.last_recorded)
                .map(|(key, _)| key.clone());
            if let Some(stalest) = stalest {
                guard.remove(&stalest);
            }
        }
        let record = guard.entry(key).or_insert_with(|| KeyRecord {
            lists: Vec::new(),
            snapshot: snapshot.clone(),
            last_recorded: at,
        });
        // The same list seen again moves to the newest position rather than
        // being stored twice.
        record.lists.retain(|list| {
            list.available != seen.available || list.subscription_keys != seen.subscription_keys
        });
        record.lists.push(seen);
        if record.lists.len() > MAX_LISTS_PER_KEY {
            let excess = record.lists.len() - MAX_LISTS_PER_KEY;
            record.lists.drain(..excess);
        }
        record.snapshot = snapshot;
        record.last_recorded = at;
    }

    pub(crate) fn current(&self) -> BTreeMap<LadderKey, ObservedSelection> {
        let now = Instant::now();
        self.by_key
            .lock()
            .map(|mut guard| {
                guard
                    .iter_mut()
                    .map(|(key, record)| {
                        record.prune(now);
                        (key.clone(), record.observation())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Availability sets only: the watcher's trigger must not fire on quota drift.
    fn availability(&self) -> BTreeMap<LadderKey, BTreeSet<String>> {
        self.current()
            .into_iter()
            .map(|(key, observation)| (key, observation.available))
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

/// One key's ladder and the availability set it was computed over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct KeyLadder {
    pub substrate: String,
    pub caller_class: String,
    pub available: Vec<String>,
    pub tiers: BTreeMap<u8, TierWinners>,
}

/// Format version of the stored ladder. Version 1 (no `version` field) keyed
/// ladders by substrate alone; a stored row of any other version is treated as
/// no baseline and replaced silently, since its keys cannot be compared.
pub(crate) const LADDER_FORMAT_VERSION: u32 = 2;

/// Winners per key and IQ tier at the default EQ. `ladders` is keyed by
/// [`LadderKey::label`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WinnerLadder {
    pub version: u32,
    pub ladders: BTreeMap<String, KeyLadder>,
    pub eq: f64,
}

/// One move on one key and every tier it happened at.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WinnerChange {
    /// [`LadderKey::label`] of the ladder that moved.
    pub key: String,
    pub substrate: String,
    pub caller_class: String,
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
                    "key": change.key,
                    "substrate": change.substrate,
                    "callerClass": change.caller_class,
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
            "agents": "each change names its key: the substrate and the caller class (the target agent route.select routed for, or panel) whose recorded availability the ladder replayed",
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

/// Compute both winners at every tier for every key a live selection has been
/// observed on. `Ok(None)` means no selection has arrived since boot: there
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
    // Multipliers come from the same routing config snapshot a selection reads,
    // so the ladder prices facts exactly as `route.select` does, including its
    // first computation after boot. A broken document keeps the last good
    // multipliers, as selection does.
    let live_config = config_overrides::live_config(&sources.routing_config);
    let now = now_ms() as f64;
    let mut facts = store
        .get_facts_with_observed_speed(now, live_config.pricing())
        .map_err(|error| format!("reading facts: {error}"))?;
    let routing_config = live_config.apply(&mut facts).routing_config;
    let min_best_effort_adequacy = live_config.min_band();
    let operator = if sources.operator_config == sources.routing_config {
        live_config.operator_policy()
    } else {
        config_overrides::read_operator_routing_policy(&sources.operator_config)
    };

    let mut ladders = BTreeMap::new();
    let mut ladder_eq = 0.0;
    for (key, observation) in observed {
        let label = key.label();
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
                            target_is_head: live_config.is_head_agent(LADDER_TARGET_AGENT),
                            requirements: resolved.requirements,
                            available_models: available.clone(),
                            facts: facts.clone(),
                            runtime_snapshot,
                            exclude_route_keys: Vec::new(),
                            operator_exclude_keys: operator.exclude.clone(),
                            required_capabilities: resolved.required_capabilities,
                            is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                                subscription.contains(&route.route_key)
                                    || subscription.contains(&route.capacity_key)
                            })),
                            min_best_effort_adequacy,
                            prefer_prompt_cache: false,
                            providers_without_prompt_cache: HashSet::new(),
                            providers_without_cache_write_reports: HashSet::new(),
                        },
                        &routing_config,
                    )
                    .ok()
                    .map(|selected| selected.route_key)
                }))
                .map_err(|_| format!("route selection panicked at iq {tier} on {label}"))
            };
            let neutral = select(None)?;
            let live = select(Some(observation.snapshot.clone()))?;
            tiers.insert(tier, TierWinners { neutral, live });
        }
        ladders.insert(
            label,
            KeyLadder {
                substrate: key.substrate.to_string(),
                caller_class: key.caller_class.clone(),
                available: observation.available.into_iter().collect(),
                tiers,
            },
        );
    }
    Ok(Some(WinnerLadder {
        version: LADDER_FORMAT_VERSION,
        ladders,
        eq: ladder_eq,
    }))
}

/// Compare a ladder with the last announced one. A tier counts only when both
/// its quota-neutral and its live winner differ; a key the baseline has never
/// seen is not a move. Moves group by key and route pairs.
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
    for (label, ladder) in &current.ladders {
        let Some(before) = baseline.ladders.get(label) else {
            continue;
        };
        for (tier, now) in &ladder.tiers {
            let then = before.tiers.get(tier).cloned().unwrap_or_default();
            if then.neutral == now.neutral || then.live == now.live {
                continue;
            }
            grouped
                .entry((
                    label.clone(),
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
                |((key, old_route, new_route, neutral_old, neutral_new), tiers)| {
                    let ladder = &current.ladders[&key];
                    WinnerChange {
                        substrate: ladder.substrate.clone(),
                        caller_class: ladder.caller_class.clone(),
                        key,
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
        eprintln!("[striatum] winner ladder baseline not saved: {error}");
    }
}

/// Compute the ladder after a change and compare it with the stored baseline.
///
/// - No selection observed since boot: nothing is computed, compared, or saved.
/// - No stored baseline (the first run ever): the ladder is saved silently.
/// - A move: one notice is emitted and the new ladder becomes the baseline.
/// - No move: the baseline stays, except that a key it has never seen is added
///   to it silently.
/// - A stored baseline of another format version (written before ladders were
///   keyed by caller class): treated as no baseline and replaced silently.
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
            eprintln!("[striatum] winner ladder unavailable ({trigger}): {error}");
            return None;
        }
    };
    let baseline = match store.route_winner_baseline() {
        Ok(Some(json)) => match parse_baseline(&json) {
            Ok(ladder) => Some(ladder),
            Err(reason) => {
                eprintln!(
                    "[striatum] stored winner ladder {reason}; replacing it without announcing"
                );
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            // Without the baseline there is nothing honest to compare against,
            // and overwriting it would lose a pending move.
            eprintln!("[striatum] winner ladder baseline unreadable: {error}");
            return None;
        }
    };
    let Some(mut baseline) = baseline else {
        store_baseline(store, &current, trigger);
        return None;
    };
    let notice = diff_ladders(&baseline, &current, trigger);
    let mut changed = false;
    for (label, ladder) in current.ladders {
        if notice.is_some() || !baseline.ladders.contains_key(&label) {
            baseline.ladders.insert(label, ladder);
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

/// Read a stored baseline, refusing any format version but the current one.
fn parse_baseline(json: &str) -> Result<WinnerLadder, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| format!("unreadable ({error})"))?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(LADDER_FORMAT_VERSION)) {
        return Err(format!(
            "has format version {} but {LADDER_FORMAT_VERSION} is current (older ladders were keyed by substrate only)",
            version.map_or_else(|| "1".to_string(), |version| version.to_string())
        ));
    }
    serde_json::from_value(value).map_err(|error| format!("unreadable ({error})"))
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
    availability: BTreeMap<LadderKey, BTreeSet<String>>,
}

fn file_digest(path: &Path) -> String {
    // Compare the published document, not a pending or rejected disk edit.
    // Otherwise the watcher can consume a change before selections can use it.
    config_overrides::live_config(path).policy_revision()
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
/// fingerprint costs metadata reads and cached-document hashes, but no selection.
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
        let keys: BTreeSet<&LadderKey> = current
            .availability
            .keys()
            .chain(self.last.availability.keys())
            .collect();
        for key in keys {
            if current.availability.get(key) != self.last.availability.get(key) {
                changed.push(format!("{} availability", key.label()));
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
/// Each check reads and writes the store and recomputes the ladder, so it runs
/// on a blocking thread rather than on an async worker.
pub(crate) async fn config_reload_watch_loop(
    store: Arc<SqliteRouteStateStore>,
    sources: LadderSources,
) {
    let sources = Arc::new(sources);
    let mut watch = ConfigReloadWatch::new(&sources);
    loop {
        tokio::time::sleep(CONFIG_WATCH_INTERVAL).await;
        let (check_store, check_sources) = (Arc::clone(&store), Arc::clone(&sources));
        let checked = tokio::task::spawn_blocking(move || {
            watch.check(&check_store, &check_sources);
            watch
        })
        .await;
        watch = match checked {
            Ok(watch) => watch,
            // A panicking check ends this loop, as it did when the check ran on
            // the loop's own task.
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // The blocking pool cancels only while the runtime shuts down.
            Err(_) => return,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ladder(substrate: &str, routes: [(&str, &str); 7]) -> WinnerLadder {
        WinnerLadder {
            version: LADDER_FORMAT_VERSION,
            ladders: BTreeMap::from([(
                format!("{substrate} / mason"),
                KeyLadder {
                    substrate: substrate.to_string(),
                    caller_class: "mason".to_string(),
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

    /// Pin the consumer prefix captured from prefrontal/script/health-sentinel.ts
    /// at 3de63a65ddde07b09128808f24d0f8a54da60072. Each repository checks its
    /// side of the notice contract without reading the other's live checkout.
    #[test]
    fn notice_prefix_matches_captured_sentinel_contract() {
        let prefix: String =
            serde_json::from_str(include_str!("../fixtures/sentinel-notice-prefix.json")).unwrap();
        assert_eq!(NOTICE_PREFIX, prefix);
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
        assert_eq!(body["changes"][0]["key"], "h / mason");
        assert_eq!(body["changes"][0]["callerClass"], "mason");
    }

    #[test]
    fn a_ladder_round_trips_through_its_stored_form() {
        let original = ladder("h", [("a", "b"); 7]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(parse_baseline(&json).unwrap(), original);
    }

    /// A row written before ladders were keyed by caller class has no version
    /// and substrate-only keys; it must read as "no baseline", not as a ladder
    /// whose every key is new or whose tiers compare against the wrong caller.
    #[test]
    fn a_stored_ladder_of_the_old_shape_is_not_a_baseline() {
        let old = r#"{"substrates":{"opencode-host":{"available":["a/x"],"tiers":{"30":{"neutral":"a/x","live":"a/x"}}}},"eq":50.0}"#;
        let error = parse_baseline(old).unwrap_err();
        assert!(error.contains("format version 1"), "{error}");
    }

    fn snapshot() -> ModelRouteStateSnapshot {
        ModelRouteStateSnapshot::default()
    }

    fn list(models: &[&str]) -> Vec<String> {
        models.iter().map(|model| model.to_string()).collect()
    }

    #[test]
    fn one_class_unions_its_lists_within_the_window_and_drops_older_ones() {
        let observed = ObservedSelections::default();
        let key = LadderKey::new(SUBSTRATE_HOST, "explore");
        let long_ago = Instant::now()
            .checked_sub(AVAILABILITY_UNION_WINDOW + Duration::from_secs(60))
            .expect("the monotonic clock reaches back past the window");
        observed.record_at(
            key.clone(),
            list(&["old/x"]),
            Vec::new(),
            snapshot(),
            long_ago,
        );
        observed.record(key.clone(), list(&["a/x", "a/y"]), Vec::new(), snapshot());
        observed.record(key.clone(), list(&["b/z"]), Vec::new(), snapshot());
        let current = observed.current();
        assert_eq!(
            current[&key].available,
            BTreeSet::from(["a/x".to_string(), "a/y".to_string(), "b/z".to_string()]),
            "both recent lists count and the one outside the window does not"
        );
        // A class that has gone quiet still replays its newest list.
        let quiet = LadderKey::new(SUBSTRATE_HOST, "oracle");
        observed.record_at(
            quiet.clone(),
            list(&["q/q"]),
            Vec::new(),
            snapshot(),
            long_ago,
        );
        assert_eq!(
            observed.current()[&quiet].available,
            BTreeSet::from(["q/q".to_string()])
        );
    }

    #[test]
    fn different_classes_on_one_substrate_are_recorded_apart() {
        let observed = ObservedSelections::default();
        let mason = LadderKey::new(SUBSTRATE_HOST, "mason");
        let explore = LadderKey::new(SUBSTRATE_HOST, "explore");
        observed.record(mason.clone(), list(&["live/x"]), Vec::new(), snapshot());
        observed.record(explore.clone(), list(&["seed/y"]), Vec::new(), snapshot());
        let current = observed.current();
        assert_eq!(
            current[&mason].available,
            BTreeSet::from(["live/x".to_string()])
        );
        assert_eq!(
            current[&explore].available,
            BTreeSet::from(["seed/y".to_string()])
        );
        assert_eq!(mason.label(), "opencode-host / mason");
    }
}
