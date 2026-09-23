//! Announce when the top route at a demand tier changes.
//!
//! A catalog placement or a config reload can quietly make a different model the
//! route every caller at some demand tier receives. Substrate owners (the
//! OpenCode host, Broca) need to hear about that before campaigns land on the new
//! model, so each triggering change computes the winner ladder before and after
//! and logs one notice line when any tier's winner moved.
//!
//! The ladder is a dry run of the router's own selection function
//! ([`select_model_route_with_config`]) with NO runtime snapshot: quota pressure,
//! cooldowns, walls, and failure backoffs move winners all the time and that is
//! not news, so they are kept out of the comparison by construction. Nothing here
//! runs on the per-selection path; only placements, catalog applies, the AA
//! ingest, and a config-change watcher call it.
//!
//! Delivery: the routing module holds no room identity and no connection to the
//! core module's room plane (`rooms.post` requires a room member as caller), so
//! the notice is written to stderr at the fixed [`NOTICE_PREFIX`]. The daemon
//! captures module stderr in `run/logs/prefrontal-routing.stderr.log`, where the
//! health sentinel's route-winner arm reads and relays it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use prefrontal_routing_core::routing_config::DEFAULT_ROUTING_CONFIG;
use prefrontal_routing_core::{
    make_route_key, resolve_launch_requirements, resolve_routing_policy,
    select_model_route_with_config, ModelRoute, ModelRoutingPolicyInput,
    RequestedModelRequirements, RouteStateStore, SelectModelRouteInput,
};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config_overrides;

/// Fixed prefix of every winner-change notice line. The health sentinel matches
/// on this exact text, so changing it silently disconnects the relay.
pub(crate) const NOTICE_PREFIX: &str = "[prefrontal-routing] route-winner-changed ";

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

pub(crate) const SUBSTRATE_HOST: &str = "opencode-host";
pub(crate) const SUBSTRATE_BROCA: &str = "broca";

/// How often the config watcher fingerprints the files that can move a winner.
pub(crate) const CONFIG_WATCH_INTERVAL: Duration = Duration::from_secs(30);

/// Every file the ladder reads. Production uses the live config-home paths;
/// tests point each at scratch files.
#[derive(Debug, Clone)]
pub(crate) struct LadderSources {
    pub routing_config: PathBuf,
    pub operator_config: PathBuf,
    /// OpenCode's auth file: its provider keys stand in for what the host can reach.
    pub host_auth: PathBuf,
    /// Broca's vault-handle map: its provider keys are what Broca can reach.
    pub broca_vault_handles: PathBuf,
}

impl LadderSources {
    pub(crate) fn live(
        routing_config: &Path,
        operator_config: &Path,
        broca_vault_handles: &Path,
    ) -> Self {
        Self {
            routing_config: routing_config.to_path_buf(),
            operator_config: operator_config.to_path_buf(),
            host_auth: config_overrides::auth_json_path(),
            broca_vault_handles: broca_vault_handles.to_path_buf(),
        }
    }
}

/// The route winning one tier, with the substrates that can send to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TierWinner {
    pub route_key: String,
    pub substrates: Vec<&'static str>,
}

/// Winner per IQ tier at the default EQ. `None` means the tier has no route.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WinnerLadder {
    pub winners: BTreeMap<u8, Option<TierWinner>>,
    pub eq: f64,
}

/// One (old route, new route) move and every tier it happened at.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WinnerChange {
    pub old_route: Option<String>,
    pub new_route: Option<String>,
    pub tiers: Vec<u8>,
    pub new_route_substrates: Vec<&'static str>,
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
                    "oldRoute": change.old_route,
                    "newRoute": change.new_route,
                    "tiers": change.tiers,
                    "newRouteSubstrates": change.new_route_substrates,
                })
            })
            .collect();
        let body = json!({
            "atMs": at_ms,
            "trigger": self.trigger,
            "eq": self.eq,
            "agents": "every routed target agent (selection does not branch on target agent)",
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

/// Compute the winner at every tier with the router's own selection function.
///
/// Availability is every served catalog row whose provider the host or Broca
/// holds a credential for (all rows when neither file names a provider), minus
/// the operator's exclusions. Live multipliers, Elo overrides, the demand
/// exponent, and the best-effort band are read from the routing config exactly
/// as a selection reads them. No runtime snapshot is passed.
pub(crate) fn compute_ladder(
    store: &impl RouteStateStore,
    sources: &LadderSources,
) -> Result<WinnerLadder, String> {
    let now = now_ms() as f64;
    let mut facts = store
        .get_facts_with_observed_speed(now)
        .map_err(|error| format!("reading facts: {error}"))?;
    // A broken document keeps the last good multipliers, as selection does.
    let _ = config_overrides::reload_live_multipliers(&sources.routing_config);
    let routing_config =
        config_overrides::apply_live_selection_config(&sources.routing_config, &mut facts)
            .map(|outcome| outcome.routing_config)
            .unwrap_or(DEFAULT_ROUTING_CONFIG);
    let min_best_effort_adequacy =
        config_overrides::read_min_best_effort_adequacy(&sources.routing_config)
            .ok()
            .flatten();
    let operator = config_overrides::read_operator_routing_policy(&sources.operator_config);

    let auth_types =
        config_overrides::read_provider_auth_types(&sources.host_auth).unwrap_or_default();
    let host_providers: HashSet<String> = auth_types.keys().cloned().collect();
    let broca_providers =
        config_overrides::read_broca_credential_providers(&sources.broca_vault_handles)
            .unwrap_or_default();
    let reachable: HashSet<&String> = host_providers.iter().chain(&broca_providers).collect();

    let connected = facts
        .iter()
        .filter(|fact| reachable.is_empty() || reachable.contains(&fact.provider_id))
        .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()));
    let policy = resolve_routing_policy(
        connected,
        &operator,
        &ModelRoutingPolicyInput::default(),
        &auth_types,
        true,
    );
    let available: HashSet<String> = policy.available_models.iter().cloned().collect();
    let subscription_keys: HashSet<String> = policy.subscription_keys.iter().cloned().collect();
    let provider_by_route: HashMap<String, String> = facts
        .iter()
        .map(|fact| {
            (
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()),
                fact.provider_id.clone(),
            )
        })
        .collect();

    let mut winners = BTreeMap::new();
    let mut ladder_eq = 0.0;
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
        let subscription = subscription_keys.clone();
        // The ladder runs inside placement handlers and background loops. If the
        // router panics on this availability set (its ordering has panicked with
        // "comparison function does not correctly implement a total order" on
        // mixed subscription catalogs), the placement must still complete: the
        // ladder is reported unavailable and no notice is emitted for the change.
        let selected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            select_model_route_with_config(
                SelectModelRouteInput {
                    target_agent: LADDER_TARGET_AGENT.to_string(),
                    requirements: resolved.requirements,
                    available_models: available.clone(),
                    facts: facts.clone(),
                    runtime_snapshot: None,
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
        }))
        .map_err(|_| format!("route selection panicked at iq {tier}"))?;
        let winner = selected.map(|selected| {
            let provider = provider_by_route
                .get(&selected.route_key)
                .cloned()
                .unwrap_or_else(|| selected.model.provider_id.clone());
            let mut substrates = Vec::new();
            if host_providers.contains(&provider) {
                substrates.push(SUBSTRATE_HOST);
            }
            if broca_providers.contains(&provider) {
                substrates.push(SUBSTRATE_BROCA);
            }
            TierWinner {
                route_key: selected.route_key,
                substrates,
            }
        });
        winners.insert(tier, winner);
    }
    Ok(WinnerLadder {
        winners,
        eq: ladder_eq,
    })
}

/// Compare two ladders tier by tier. Only a different winning route counts: a
/// changed score, adequacy, or substrate list under the same winner is not a move.
pub(crate) fn diff_ladders(
    before: &WinnerLadder,
    after: &WinnerLadder,
    trigger: &str,
) -> Option<WinnerChangeNotice> {
    let mut grouped: BTreeMap<(Option<String>, Option<String>), WinnerChange> = BTreeMap::new();
    for (tier, new_winner) in &after.winners {
        let old_route = before
            .winners
            .get(tier)
            .and_then(|winner| winner.as_ref())
            .map(|winner| winner.route_key.clone());
        let new_route = new_winner.as_ref().map(|winner| winner.route_key.clone());
        if old_route == new_route {
            continue;
        }
        grouped
            .entry((old_route.clone(), new_route.clone()))
            .or_insert_with(|| WinnerChange {
                old_route,
                new_route,
                tiers: Vec::new(),
                new_route_substrates: new_winner
                    .as_ref()
                    .map(|winner| winner.substrates.clone())
                    .unwrap_or_default(),
            })
            .tiers
            .push(*tier);
    }
    if grouped.is_empty() {
        return None;
    }
    Some(WinnerChangeNotice {
        trigger: trigger.to_string(),
        eq: after.eq,
        changes: grouped.into_values().collect(),
    })
}

/// The most recent ladder any trigger computed. The config watcher diffs a
/// reload against it, so a placement between two config checks is not
/// re-announced as part of the next reload.
pub(crate) fn latest_ladder() -> &'static Mutex<Option<WinnerLadder>> {
    static LATEST: OnceLock<Mutex<Option<WinnerLadder>>> = OnceLock::new();
    LATEST.get_or_init(|| Mutex::new(None))
}

/// Compute the "before" half of a triggering change. A failure is logged and
/// skips the announcement for this change; it never blocks the change itself.
pub(crate) fn ladder_before(
    store: &impl RouteStateStore,
    sources: &LadderSources,
) -> Option<WinnerLadder> {
    match compute_ladder(store, sources) {
        Ok(ladder) => Some(ladder),
        Err(error) => {
            eprintln!("[prefrontal-routing] winner ladder unavailable before change: {error}");
            None
        }
    }
}

/// Compute the "after" half, record it as the latest ladder, and emit one notice
/// when any tier's winner moved. Returns the notice for callers that test it.
pub(crate) fn announce_after(
    store: &impl RouteStateStore,
    sources: &LadderSources,
    before: Option<WinnerLadder>,
    trigger: &str,
    latest: &Mutex<Option<WinnerLadder>>,
) -> Option<WinnerChangeNotice> {
    let after = match compute_ladder(store, sources) {
        Ok(ladder) => ladder,
        Err(error) => {
            eprintln!("[prefrontal-routing] winner ladder unavailable after change: {error}");
            return None;
        }
    };
    let notice = before
        .as_ref()
        .and_then(|before| diff_ladders(before, &after, trigger));
    if let Ok(mut guard) = latest.lock() {
        *guard = Some(after);
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

/// The parts of the config that can move a winner, reduced to one comparable
/// value per part. Provider files contribute only their provider KEY sets, so a
/// token refresh inside OpenCode's auth file is not a config change.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigFingerprint {
    routing_config: String,
    operator_config: String,
    host_providers: BTreeSet<String>,
    broca_providers: BTreeSet<String>,
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
        host_providers: config_overrides::read_provider_auth_types(&sources.host_auth)
            .map(|types| types.into_keys().collect())
            .unwrap_or_default(),
        broca_providers: config_overrides::read_broca_credential_providers(
            &sources.broca_vault_handles,
        )
        .map(|providers| providers.into_iter().collect())
        .unwrap_or_default(),
    }
}

/// Watches the config files the ladder reads. A changed fingerprint is a
/// reload (subscription multipliers, exclusions, Elo or model overrides, or a
/// provider credential appearing or leaving) and gets a ladder comparison; an
/// unchanged one costs a few small file reads and no selection.
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
        store: &impl RouteStateStore,
        sources: &LadderSources,
        latest: &Mutex<Option<WinnerLadder>>,
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
        if current.host_providers != self.last.host_providers {
            changed.push("host provider credentials".to_string());
        }
        if current.broca_providers != self.last.broca_providers {
            changed.push("Broca provider credentials".to_string());
        }
        self.last = current;
        let before = latest.lock().ok().and_then(|guard| guard.clone());
        announce_after(
            store,
            sources,
            before,
            &format!("config reload ({})", changed.join(", ")),
            latest,
        )
    }
}

/// Seed the latest ladder at boot, then re-check the config on an interval.
pub(crate) async fn config_reload_watch_loop(
    store: std::sync::Arc<prefrontal_routing_store::SqliteRouteStateStore>,
    sources: LadderSources,
) {
    let mut watch = ConfigReloadWatch::new(&sources);
    if let Some(ladder) = ladder_before(store.as_ref(), &sources) {
        if let Ok(mut guard) = latest_ladder().lock() {
            guard.get_or_insert(ladder);
        }
    }
    loop {
        tokio::time::sleep(CONFIG_WATCH_INTERVAL).await;
        watch.check(store.as_ref(), &sources, latest_ladder());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn winner(route_key: &str) -> Option<TierWinner> {
        Some(TierWinner {
            route_key: route_key.to_string(),
            substrates: vec![SUBSTRATE_BROCA],
        })
    }

    fn ladder(routes: [&str; 7]) -> WinnerLadder {
        WinnerLadder {
            winners: LADDER_IQ_TIERS
                .iter()
                .zip(routes)
                .map(|(tier, route)| (*tier, winner(route)))
                .collect(),
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
        let same = ladder(["a", "a", "b", "b", "c", "c", "c"]);
        assert_eq!(diff_ladders(&same, &same.clone(), "t"), None);
    }

    #[test]
    fn a_substrate_change_under_the_same_winner_is_not_a_move() {
        let before = ladder(["a", "a", "b", "b", "c", "c", "c"]);
        let mut after = before.clone();
        after
            .winners
            .get_mut(&100)
            .unwrap()
            .as_mut()
            .unwrap()
            .substrates = vec![];
        assert_eq!(diff_ladders(&before, &after, "t"), None);
    }

    #[test]
    fn moves_group_by_route_pair_and_render_on_the_fixed_prefix() {
        let before = ladder(["a", "a", "b", "b", "c", "c", "c"]);
        let after = ladder(["a", "a", "b", "b", "c", "d", "d"]);
        let notice = diff_ladders(&before, &after, "route.model_upsert x/d").unwrap();
        assert_eq!(notice.changes.len(), 1);
        assert_eq!(notice.changes[0].old_route.as_deref(), Some("c"));
        assert_eq!(notice.changes[0].new_route.as_deref(), Some("d"));
        assert_eq!(notice.changes[0].tiers, vec![90, 100]);
        let line = notice.render(42);
        let body: serde_json::Value =
            serde_json::from_str(line.strip_prefix(NOTICE_PREFIX).unwrap()).unwrap();
        assert_eq!(body["atMs"], 42);
        assert_eq!(body["trigger"], "route.model_upsert x/d");
        assert_eq!(body["changes"][0]["tiers"], json!([90, 100]));
        assert_eq!(body["changes"][0]["newRouteSubstrates"], json!(["broca"]));
    }
}
