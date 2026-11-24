//! Validated, last-good snapshots of the routing-owned global configuration.
//! Each selection overlays one complete snapshot onto catalog facts; reloads
//! never rewrite the catalog, nor swap out an in-flight request's configuration.

use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use crate::config_policy::{
    ConsumerPolicy, ExcludeGroups, PanelPolicies, Pins, ProviderPolicy, RunnerPolicy,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use striatum_core::model_facts::{
    Confidence, ModelFact, ModelScores, RoutePricing, ScoreProvenance,
};
use striatum_core::{
    make_route_key, ModelRoutingPolicyInput, OverqualificationConfig, RoutingConfig,
    DEFAULT_DEMAND_EXPONENT, DEFAULT_OVERQUALIFICATION,
};
#[cfg(test)]
use striatum_core::{
    model_facts_seed, CatalogFactField, FactSource, FailureBackoffConfig, RouteStateStore,
    DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD,
};
use striatum_store::ReasoningCatalog;
use subc_jsonc::jsonc_to_json;
// Fusiform's catalog-changed events reach routing through its durable NATS
// consumer (`m_striatum`) and trigger an immediate refresh, so this poll is
// only a backstop for an event lost while the reader was down.
pub const DEFAULT_CATALOG_POLL_MS: u64 = 30 * 60_000;

const RELOAD_DEBOUNCE: Duration = Duration::from_millis(250);
pub const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[cfg(test)]
mod contract_tests;
mod dormant_models;
#[cfg(test)]
mod managed_settings_tests;
mod new_shape;
mod settings_schema;
pub(crate) mod settings_shape;
pub(crate) use new_shape::SelectionPolicyMemo;
#[cfg(test)]
mod new_shape_tests;

/// A complete, validated document held for the lifetime of one selection. Model
/// overrides are overlays, not store writes: withdrawing one exposes stored facts
/// again, and a request cannot observe a partially applied edit.
#[derive(Debug, Clone)]
pub struct LiveConfig {
    config: RoutingConfigFile,
    document: Option<String>,
    pricing: RoutePricing,
    compiled: Arc<CompiledPolicy>,
    revision: String,
}

#[derive(Debug)]
struct CompiledPolicy {
    entities: new_shape::EntityPolicyIndex,
    routing: striatum_core::launch_policy::CompiledRoutingPolicy,
    allocator_default: Arc<striatum_core::accounts::AllocatorPolicy>,
    allocators: BTreeMap<String, Arc<striatum_core::accounts::AllocatorPolicy>>,
    model_pools: Vec<striatum_core::quota_extract::ModelPool>,
    runner_routes: BTreeMap<Vec<String>, Arc<striatum_core::provider_model_spec::CompiledSpecs>>,
    metered_fallback: std::collections::BTreeSet<String>,
}

#[cfg(all(test, feature = "heap-measure"))]
mod heap_measurements {
    use super::*;
    #[test]
    #[ignore = "repository settings snapshot heap profile; run alone"]
    fn repository_settings_heap() {
        use crate::memory_measurements::{operation, profiler, release};
        let document = crate::test_config::convert_archived(include_str!(
            "../fixtures/routing-config-live-2026-10-24.jsonc"
        ))
        .to_string();
        println!(
            "COUNTS repository_settings_document_bytes={}",
            document.len()
        );
        let _profiler = profiler("settings");
        let settings = operation("LiveConfig::new", || {
            LiveConfig::new(parse_config(&document).unwrap(), Some(document.clone()))
        });
        release("LiveConfig_with_compiled_policy", settings);
    }
}

impl CompiledPolicy {
    fn compile(config: &RoutingConfigFile) -> Self {
        let model_pools: Vec<_> = config
            .quota_model_pools
            .iter()
            .map(|entry| striatum_core::quota_extract::ModelPool {
                model_prefix: entry.model_prefix.clone(),
                pool: entry.pool.clone(),
            })
            .collect();
        let allocator = |provider| {
            let mut policy = allocator_policy_from(&config.accounts, provider)
                .expect("account policies are validated before publishing the config");
            policy.model_pools = model_pools.clone();
            Arc::new(policy)
        };
        Self {
            entities: new_shape::EntityPolicyIndex::compile(config),
            routing: striatum_core::launch_policy::CompiledRoutingPolicy::compile(
                ModelRoutingPolicyInput {
                    exclude: clean_spec_entries(config.model_routing.exclude.all.clone()),
                    subscription: clean_spec_entries(config.model_routing.subscription.clone()),
                    score_alias: BTreeMap::new(),
                },
            ),
            allocator_default: allocator(None),
            allocators: config
                .providers
                .iter()
                .map(|(id, provider)| (id.clone(), allocator(Some(&provider.accounts))))
                .collect(),
            runner_routes: config
                .runners
                .iter()
                .chain(
                    config
                        .consumers
                        .values()
                        .flat_map(|consumer| consumer.runners.iter().flatten()),
                )
                .filter_map(|runner| {
                    runner.routes.as_ref().map(|routes| {
                        (
                            routes.clone(),
                            Arc::new(crate::routing_v1::runners::placements::compile(routes)),
                        )
                    })
                })
                .collect(),
            metered_fallback: config
                .providers
                .iter()
                .filter(|(_, provider)| provider.accounts.metered_fallback)
                .map(|(id, _)| id.clone())
                .collect(),
            model_pools,
        }
    }
}
impl Default for LiveConfig {
    fn default() -> Self {
        Self::new(RoutingConfigFile::default(), None)
    }
}

/// The deliberately narrow, call-local policy overlay accepted by `route.what_if`.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ModelPolicyPatch {
    pub pin_only_for: Option<Vec<String>>,
    pub pin_only: Option<bool>,
    pub not_for: Option<Vec<String>>,
    pub only_for: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OverqualificationMode {
    #[default]
    Additive,
    CostScaled,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct PricingPatch {
    pub overqualification: Option<OverqualificationMode>,
    pub banked_resets: Option<BTreeMap<String, crate::config_policy::BankedResets>>,
}

impl LiveConfig {
    #[cfg(test)]
    pub(super) fn prepared_settings(&self) -> serde_json::Value {
        serde_json::to_value(&self.config).unwrap()
    }

    pub(crate) fn consumer_priorities(
        &self,
    ) -> BTreeMap<String, cortexkit_role_routing::declarations::ConsumerPriority> {
        self.config
            .consumer_priorities
            .iter()
            .map(|(name, class)| (name.clone(), (*class).into()))
            .collect()
    }

    pub(crate) fn concurrency_caps(&self) -> &crate::concurrency_caps::CapSettings {
        &self.config.concurrency
    }
    pub(crate) fn scoring_anchor_version(&self) -> u32 {
        self.config
            .model_routing
            .overqualification
            .resolve()
            .expect("validated config")
            .mode
            .anchor_version()
    }

    /// Count prepared rule-list entries without returning rule or provider names.
    pub(crate) fn rule_counts(&self) -> (usize, usize, usize) {
        let exclusions = serde_json::to_value(&self.config.model_routing.exclude).unwrap();
        (
            self.config.model_routing.subscription.len(),
            exclusions
                .as_object()
                .unwrap()
                .values()
                .map(|entries| entries.as_array().unwrap().len())
                .sum(),
            self.config
                .runners
                .iter()
                .map(|runner| runner.routes.as_ref().map_or(0, Vec::len))
                .sum(),
        )
    }

    #[cfg(test)]
    pub(crate) fn service_tier(
        &self,
        consumer: &str,
        provider: &str,
        model: &str,
    ) -> Option<cortexkit_role_routing::service_tier::ServiceTier> {
        self.service_tier_with(consumer, provider, model, None)
    }

    pub(crate) fn service_tier_with(
        &self,
        consumer: &str,
        provider: &str,
        model: &str,
        project: Option<&BTreeMap<String, crate::tier_settings::Consumer>>,
    ) -> Option<cortexkit_role_routing::service_tier::ServiceTier> {
        crate::tier_settings::at_level(
            consumer,
            provider,
            model,
            self.config
                .consumers
                .iter()
                .map(|(id, policy)| {
                    let scoped = project.and_then(|entries| entries.get(id));
                    (
                        id.clone(),
                        scoped
                            .and_then(|entry| entry.service_tier)
                            .or(policy.service_tier),
                        scoped
                            .and_then(|entry| entry.service_tiers.as_ref())
                            .or(policy.service_tiers.as_ref())
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .chain(
                    project
                        .into_iter()
                        .flat_map(|entries| entries.iter())
                        .filter(|(id, _)| !self.config.consumers.contains_key(*id))
                        .map(|(id, entry)| {
                            (
                                id.clone(),
                                entry.service_tier,
                                entry.service_tiers.clone().unwrap_or_default(),
                            )
                        }),
                ),
            self.config
                .model_routing
                .models
                .get(&format!("{provider}/{model}"))
                .and_then(|model| model.rules.service_tier),
            self.config
                .providers
                .get(provider)
                .and_then(|provider| provider.rules.service_tier),
        )
    }
    fn new(mut config: RoutingConfigFile, document: Option<String>) -> Self {
        // Expand the old spelling on its own entry, before dotted inheritance.
        // Retain the old module so a select with no global entry for it still
        // answers no_available_model, rather than the new list's harness_unavailable.
        for consumer in config.consumers.values_mut() {
            if let Some(module) = &consumer.runner {
                consumer.runners = Some(
                    config
                        .runners
                        .iter()
                        .filter(|entry| &entry.module == module)
                        .cloned()
                        .collect(),
                );
            }
        }
        let mut pricing = route_pricing(&config.model_routing);
        pricing.banked_resets_capacity = config
            .providers
            .iter()
            .filter(|(_, policy)| {
                policy.banked_resets == crate::config_policy::BankedResets::Capacity
            })
            .map(|(provider, _)| provider.clone())
            .collect();
        Self {
            compiled: Arc::new(CompiledPolicy::compile(&config)),
            revision: document
                .as_deref()
                .map(policy_revision)
                .unwrap_or_else(default_policy_revision),
            config,
            document,
            pricing,
        }
    }
}

/// Collect a validated section's pricing and reserve settings into one value.
fn route_pricing(section: &ModelRoutingSection) -> RoutePricing {
    let mut pricing = RoutePricing {
        subscription_multipliers: section
            .subscription_multipliers
            .iter()
            .filter(|(key, _)| !key.contains('/'))
            .map(|(key, value)| (key.clone(), *value))
            .collect(),
        keep_free_percent: section.keep_free_percent.clone(),
        ..Default::default()
    };
    for (key, model) in &section.models {
        if let Some(value) = model.speed_multiplier {
            pricing.speed_multipliers.insert(key.clone(), value);
        }
    }
    pricing
}

type FileStamp = Option<(SystemTime, u64, String)>;

struct LiveConfigCache {
    managed: bool,
    stamp: FileStamp,
    pending: Option<(FileStamp, Instant)>,
    active: Arc<LiveConfig>,
}

fn live_configs() -> &'static Mutex<HashMap<PathBuf, LiveConfigCache>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, LiveConfigCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Reads the reasoning efforts the catalog currently publishes.
pub type ReasoningCatalogSource = Arc<dyn Fn() -> Result<ReasoningCatalog, String> + Send + Sync>;

/// The live catalog for pin validation, attached when the routing store opens.
fn catalog_sources() -> &'static Mutex<HashMap<PathBuf, ReasoningCatalogSource>> {
    static SOURCES: OnceLock<Mutex<HashMap<PathBuf, ReasoningCatalogSource>>> = OnceLock::new();
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Attach the published reasoning options used to validate explicit pins.
pub fn attach_reasoning_catalog(path: &Path, source: ReasoningCatalogSource) -> Result<(), String> {
    catalog_sources()
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), source);
    Ok(())
}

pub(super) fn attach_store_catalog(path: &Path, store: &crate::SharedStore) -> Result<(), String> {
    // The process-global catalog registry must not keep SQLite open after serving ends.
    let store = Arc::downgrade(store);
    attach_reasoning_catalog(
        path,
        Arc::new(move || {
            store
                .upgrade()
                .ok_or("routing store has closed")?
                .reasoning_catalog()
                .map_err(|error| error.to_string())
        }),
    )
}

/// The live catalog attached to `path`, read now. `None` before the store opens.
pub fn reasoning_catalog_for(path: &Path) -> Option<ReasoningCatalog> {
    let source = catalog_sources().lock().unwrap().get(path).cloned()?;
    match source() {
        Ok(catalog) => Some(catalog),
        Err(error) => {
            eprintln!(
                "[striatum] reading reasoning options for {}: {error}",
                path.display()
            );
            None
        }
    }
}

/// Where the module's routing store lives when the daemon manages storage:
/// `$XDG_DATA_HOME/cortexkit/striatum/store.db`, defaulting
/// `XDG_DATA_HOME` to `~/.local/share`.
/// Where the module's routing store lives when the daemon manages storage.
pub fn default_store_path() -> Option<PathBuf> {
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local/share"))
        })?;
    Some(
        data_home
            .join("cortexkit")
            .join(crate::DEFAULT_MODULE_ID)
            .join("store.db"),
    )
}

/// Validate the config and report counts from the store at `store_path`, read-only.
pub fn check_config_offline(path: &Path, store_path: Option<&Path>) -> Result<String, String> {
    initialize_config(path)?;
    let active = live_config_at(path, Instant::now())?;
    let (facts, roster) = store_path
        .map(striatum_store::read_counts_offline)
        .unwrap_or_else(|| {
            (
                Err("no routing store path".into()),
                Err("no routing store path".into()),
            )
        });
    let count = |value: Result<u64, String>| match value {
        Ok(value) => value.to_string(),
        Err(reason) => format!("unavailable ({reason})"),
    };
    let (subscription_count, exclusion_count, runner_count) = active.rule_counts();
    Ok(format!("subscription_rules={}\nexclusion_rules={exclusion_count}\nrunner_route_rules={runner_count}\ncatalog_facts={}\nvault_roster_credentials={}",
        subscription_count, count(facts), count(roster)))
}

pub fn live_config(path: &Path) -> Arc<LiveConfig> {
    live_config_at(path, Instant::now()).unwrap_or_else(|error| panic!("{error}"))
}

/// Refuse invalid operator settings before serving requests, so startup cannot
/// silently allow models that the operator intended to exclude.
pub fn initialize_config(path: &Path) -> Result<(), String> {
    live_config_at(path, Instant::now()).map(|_| ())
}

#[cfg(test)]
pub(crate) fn settle_live_config(path: &Path) -> Arc<LiveConfig> {
    let now = Instant::now();
    live_config_at(path, now).unwrap();
    live_config_at(path, now + RELOAD_DEBOUNCE).unwrap()
}

fn file_stamp(path: &Path) -> Result<FileStamp, String> {
    match std::fs::read_to_string(path) {
        Ok(document) => {
            let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
            Ok(Some((
                metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                metadata.len(),
                policy_revision(&document),
            )))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("reading {}: {error}", path.display())),
    }
}

fn parse_config(document: &str) -> Result<RoutingConfigFile, String> {
    parse_config_with_reporter(document, |warning| {
        crate::warn_operator(format!("[striatum] {warning}"));
    })
}

fn parse_config_with_reporter(
    document: &str,
    report: impl FnMut(&str),
) -> Result<RoutingConfigFile, String> {
    parse_config_with_paths(document, report, |_| {})
}

fn parse_config_with_paths(
    document: &str,
    mut report: impl FnMut(&str),
    report_panel_paths: impl FnMut(Vec<String>),
) -> Result<RoutingConfigFile, String> {
    let mut json = jsonc_to_json(document)?;
    let mut value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    // Report the removed `aa` key with deletion guidance before checking unknown
    // fields, including when its value is null.
    if value.get("aa").is_some() {
        return Err("config_key_removed: aa; delete this key".into());
    }
    if value.get("substrates").is_some()
        || value
            .get("model_routing")
            .is_some_and(|section| section.get("substrates").is_some())
    {
        return Err("config_key_removed: substrates; removed along with legacy route.select; delete this key".into());
    }
    if value.get("model_naming").is_some()
        || value
            .get("model_routing")
            .is_some_and(|section| section.get("model_naming").is_some())
    {
        return Err("config_key_removed: model_naming; removed with the retired plugin model-resolution operations; delete this key".into());
    }
    if value.get("model_families").is_some() {
        return Err("config_key_removed: model_families; families now come from Fusiform's model.lineage; delete this section".into());
    }
    if value.get("elo_overrides").is_some() {
        return Err(
            "config_key_removed: elo_overrides; move each entry to models.<id>.elo, or run `ck-striatum config-convert`"
                .into(),
        );
    }
    if value
        .get("accounts")
        .and_then(|accounts| accounts.get("enabled"))
        .is_some()
    {
        return Err(format!(
            "old routing config key accounts.enabled; run {} <input> <output>",
            crate::config_convert::COMMAND
        ));
    }
    if value
        .get("accounts")
        .and_then(|accounts| accounts.get("shadow"))
        .is_some()
    {
        return Err("invalid routing config: unknown field `shadow` under accounts; delete the retired setting".into());
    }
    crate::config_convert::reject_removed_variants(&value)?;
    for key in ["model_routing", "account_selection", "pins", "score_alias"] {
        if value.get(key).is_some() {
            return Err(format!(
                "old routing config key {key}; run {} <input> <output>",
                crate::config_convert::COMMAND
            ));
        }
    }
    if let Some(root) = value.as_object_mut() {
        let removed = crate::config_convert::remove_account_model_families(root);
        if !removed.is_empty() {
            // Re-serialize only the edited sections. Every other section keeps
            // its raw text, because a round trip through Value reorders map keys.
            let mut raw: BTreeMap<String, Box<serde_json::value::RawValue>> =
                serde_json::from_str(&json).map_err(|error| error.to_string())?;
            for key in ["accounts", "providers"] {
                if let Some(section) = root.get(key) {
                    raw.insert(
                        key.into(),
                        serde_json::value::to_raw_value(section)
                            .map_err(|error| error.to_string())?,
                    );
                }
            }
            json = serde_json::to_string(&raw).map_err(|error| error.to_string())?;
            for key in removed {
                report(&format!(
                    "ignored retired key `{key}`; quota scopes are derived from Insula window ids; delete this key"
                ));
            }
        }
    }
    if let Some(consumers) = value
        .get("consumers")
        .and_then(serde_json::Value::as_object)
    {
        for (name, entry) in consumers {
            if entry.get("runner").is_some() && entry.get("runners").is_some() {
                let conflict =
                    format!("consumers.{name}: runner and runners are mutually exclusive");
                for key in ["runner", "runners"] {
                    if entry[key].is_null() {
                        let path = settings_shape::pointer("/consumers", name);
                        let error =
                            settings_shape::null_error(&settings_shape::pointer(&path, key));
                        return Err(format!("{error}; {conflict}"));
                    }
                }
                return Err(conflict);
            }
        }
    }
    settings_shape::validate(&mut value)?;
    let mut config: RoutingConfigFile =
        serde_json::from_str(&json).map_err(|error| format!("invalid routing config: {error}"))?;
    let section = &mut config.model_routing;
    let mut ignored = Vec::new();
    section.subscription_multipliers.retain(|key, _| {
        if key.contains('/') {
            ignored.push(format!("subscription_multipliers.{key}"));
            false
        } else {
            true
        }
    });
    for (key, model) in &mut section.models {
        if model.subscription_multiplier.take().is_some() {
            ignored.push(format!("models.{key}.subscription_multiplier"));
        }
    }
    ignored.sort();
    for key in ignored {
        report(&format!(
            "ignored per-route multiplier `{key}`; subscription multipliers are provider-wide"
        ));
    }
    for (name, consumer) in &config.consumers {
        if let Some(runners) = &consumer.runners {
            validate_runner_list(runners, &format!("consumers.{name}.runners"))?;
        }
    }
    config.prepare()?;
    validate_live_config(&config, report_panel_paths)?;
    Ok(config)
}

pub(crate) fn validate_document(document: &str) -> Result<(), String> {
    parse_config(document).map(|_| ())
}

pub(super) fn validate_review_document(document: &str) -> Result<(), (Vec<String>, String)> {
    let mut paths = Vec::new();
    parse_config_with_paths(document, |_| {}, |reported| paths = reported)
        .map(|_| ())
        .map_err(|error| (paths, error))
}

fn build_live_config(path: &Path, document: Option<String>) -> Result<LiveConfig, String> {
    let config = match &document {
        Some(document) => parse_config(document)
            .map_err(|error| format!("invalid config in {}: {error}", path.display()))?,
        None => RoutingConfigFile::default(),
    };
    Ok(LiveConfig::new(config, document))
}

fn publish_live_config(config: LiveConfig) -> Arc<LiveConfig> {
    crate::routing_v1::runners::placements::settings_loaded(&config);
    Arc::new(config)
}

pub(super) fn apply_managed(path: &Path, effective: &serde_json::Value) -> Result<(), String> {
    let document = serde_json::to_string(effective).map_err(|error| error.to_string())?;
    let next = build_live_config(path, Some(document))?;
    let mut caches = live_configs().lock().unwrap();
    if caches.get(path).is_some_and(|cache| {
        cache.managed && cache.active.document.as_ref() == next.document.as_ref()
    }) {
        return Ok(());
    }
    caches.insert(
        path.to_path_buf(),
        LiveConfigCache {
            managed: true,
            stamp: None,
            pending: None,
            active: publish_live_config(next),
        },
    );
    Ok(())
}

fn live_config_at(path: &Path, now: Instant) -> Result<Arc<LiveConfig>, String> {
    live_config_at_with_reporter(path, now, |error| {
        eprintln!(
            "[striatum] routing-config-reload-rejected: {}: {error}; keeping last good config",
            path.display()
        );
    })
}

fn live_config_at_with_reporter(
    path: &Path,
    now: Instant,
    mut report_rejection: impl FnMut(&str),
) -> Result<Arc<LiveConfig>, String> {
    let mut caches = live_configs().lock().unwrap();
    if let Some(cache) = caches.get(path).filter(|cache| cache.managed) {
        return Ok(Arc::clone(&cache.active));
    }
    let stamp = match file_stamp(path) {
        Ok(stamp) => stamp,
        Err(error) => {
            let Some(cache) = caches.get(path) else {
                return Err(error);
            };
            report_rejection(&error);
            return Ok(Arc::clone(&cache.active));
        }
    };
    if let Some(cache) = caches.get_mut(path) {
        if stamp.as_ref().is_some_and(|(_, _, revision)| {
            cache.active.document.is_some() && *revision == cache.active.revision
        }) {
            cache.stamp = stamp;
            cache.pending = None;
            return Ok(Arc::clone(&cache.active));
        }
        if cache.stamp == stamp {
            cache.pending = None;
            return Ok(Arc::clone(&cache.active));
        }
        match &cache.pending {
            Some((pending, since))
                if *pending == stamp
                    && now.saturating_duration_since(*since) >= RELOAD_DEBOUNCE => {}
            Some((pending, _)) if *pending == stamp => return Ok(Arc::clone(&cache.active)),
            _ => {
                cache.pending = Some((stamp.clone(), now));
                return Ok(Arc::clone(&cache.active));
            }
        }
    }
    let previous = caches.get(path).map(|cache| Arc::clone(&cache.active));
    let previously_present = previous
        .as_ref()
        .is_some_and(|active| active.document.is_some());
    // Parsing and building lookup tables must not hold the publication lock.
    drop(caches);
    let next = (|| {
        let document = match std::fs::read_to_string(path) {
            Ok(document) => Some(document),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if previously_present {
                    return Err("missing".to_owned());
                }
                None
            }
            Err(error) => return Err(error.to_string()),
        };
        build_live_config(path, document)
    })();
    let mut caches = live_configs().lock().unwrap();
    if let Some(cache) = caches.get(path) {
        if previous
            .as_ref()
            .is_none_or(|old| !Arc::ptr_eq(old, &cache.active))
        {
            return Ok(Arc::clone(&cache.active));
        }
    }
    // Do not publish a document that changed while its tables were being built.
    if file_stamp(path).ok().as_ref() != Some(&stamp) {
        if let Some(cache) = caches.get(path) {
            return Ok(Arc::clone(&cache.active));
        }
        return Err("config changed while loading; retry".into());
    }
    let active = match next {
        Ok(config) => publish_live_config(config),
        Err(error) => {
            let Some(cache) = caches.get(path) else {
                return Err(error);
            };
            report_rejection(&error);
            Arc::clone(&cache.active)
        }
    };
    caches.insert(
        path.to_path_buf(),
        LiveConfigCache {
            managed: false,
            stamp,
            pending: None,
            active: Arc::clone(&active),
        },
    );
    Ok(active)
}

fn validate_live_config(
    config: &RoutingConfigFile,
    report_panel_paths: impl FnMut(Vec<String>),
) -> Result<(), String> {
    config.concurrency.validate()?;
    if config.accounts.metered_fallback {
        return Err(
            "metered_fallback must be explicitly enabled under providers.<id>.accounts".into(),
        );
    }
    allocator_policy_from(&config.accounts, None)?;
    for (provider, entry) in &config.providers {
        allocator_policy_from(&config.accounts, Some(&entry.accounts))
            .map_err(|error| format!("providers.{provider}.accounts: {error}"))?;
    }
    let section = &config.model_routing;
    if section
        .demand_exponent
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return Err("model_routing.demand_exponent must be positive".into());
    }
    section.overqualification.resolve()?;
    if let Some(value) = section.min_best_effort_adequacy {
        validate_min_best_effort_adequacy(value)?;
    }
    if section.consecutive_terminal_failure_threshold == Some(0) {
        return Err("consecutive_terminal_failure_threshold must be at least 1".into());
    }
    if config.catalog.poll_ms == Some(0) {
        return Err("catalog.poll_ms must be positive".into());
    }
    section.panels.validate(report_panel_paths)?;
    section.pins.validate()?;
    validate_v1_policy(section)?;
    for (provider, value) in &section.subscription_multipliers {
        if provider.trim().is_empty() || !value.is_finite() || *value <= 0.0 {
            return Err(format!("invalid subscription_multiplier for {provider}"));
        }
    }
    for (provider, value) in &section.keep_free_percent {
        if provider.trim().is_empty() || !value.is_finite() || !(0.0..=100.0).contains(value) {
            return Err(format!("invalid keep_free_percent for {provider}"));
        }
    }
    for (key, model) in &section.models {
        if parse_model_key(key, None).is_none() {
            return Err(format!("invalid model key {key}"));
        }
        for value in [
            model.elo,
            model.eq,
            model.subscription_multiplier,
            model.context_window_tokens,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || value <= 0.0 {
                return Err(format!("invalid positive model override for {key}"));
            }
        }
        if model
            .output_tokens_per_second
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(format!("invalid speed override for {key}"));
        }
        if model
            .cost_per_mtok_out
            .is_some_and(|value| !value.is_finite() || value < 0.0)
            || model
                .speed_multiplier
                .is_some_and(|value| !value.is_finite() || value <= 0.0 || value > 2.0)
        {
            return Err(format!("invalid cost/speed override for {key}"));
        }
    }
    config.validate_consumer_pins()?;
    Ok(())
}

/// Shape checks for the `routing/v1` sections: consumer ids and runner modules
/// are single words, runner routes use the exclusion grammar without negation,
/// and every project pin names a configured consumer and a
/// `provider/model[:variant]` pin.
fn validate_v1_policy(section: &ModelRoutingSection) -> Result<(), String> {
    let is_word = |text: &str| !text.is_empty() && !text.chars().any(char::is_whitespace);
    if let Some(consumers) = &section.consumers {
        if let Some(id) = consumers.iter().find(|id| !is_word(id)) {
            return Err(format!(
                "model_routing.consumers: consumer id {id:?} must be one non-empty word"
            ));
        }
        let distinct: std::collections::HashSet<&String> = consumers.iter().collect();
        if distinct.len() != consumers.len() {
            return Err("model_routing.consumers: each consumer id is listed once".into());
        }
    }
    if let Some(runners) = &section.runners {
        validate_runner_list(runners, "model_routing.runners")?;
    }
    let consumers = v1_consumer_ids(section);
    for (project, pins) in &section.project_pins {
        if !is_word(project) {
            return Err(format!(
                "model_routing.project_pins: project id {project:?} must be one non-empty word"
            ));
        }
        for (consumer, pin) in pins {
            if !consumers.contains(consumer) {
                return Err(format!(
                    "model_routing.project_pins.{project}: {consumer:?} is not a configured consumer"
                ));
            }
            if !is_word(pin) || cortexkit_role_routing::select::RouteRef::parse_pin(pin).is_none() {
                return Err(format!(
                    "model_routing.project_pins.{project}.{consumer}: pin {pin:?} must be provider/model[:variant]"
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_runner_list(runners: &[RunnerPolicy], path: &str) -> Result<(), String> {
    let is_word = |text: &str| !text.is_empty() && !text.chars().any(char::is_whitespace);
    let mut seen = std::collections::HashSet::new();
    for runner in runners {
        if !is_word(&runner.module)
            || !seen.insert((&runner.module, &runner.harness))
            || runner
                .harness
                .as_deref()
                .is_some_and(|harness| !is_word(harness))
        {
            return Err(format!(
                    "{path}: module {:?} and harness must be non-empty words, each (module, harness) listed once",
                    runner.module
                ));
        }
        for spec in runner.routes.iter().flatten() {
            let parsed = striatum_core::provider_model_spec::parse_spec(spec);
            if spec.trim() != spec || parsed.is_none_or(|parsed| parsed.negated) {
                return Err(format!(
                        "{path}.{}: route {spec:?} must be provider, provider/model or provider/model/variant",
                        runner.module
                    ));
            }
        }
        if let Some(tag) = runner.tool_capabilities.iter().find(|tag| !is_word(tag)) {
            return Err(format!(
                "{path}.{}: tool capability {tag:?} must be one non-empty word",
                runner.module
            ));
        }
    }
    Ok(())
}

/// The `routing/v1` consumer ids a routing section serves, in id order.
fn v1_consumer_ids(section: &ModelRoutingSection) -> Vec<String> {
    let mut ids: Vec<String> = match &section.consumers {
        Some(consumers) => consumers.clone(),
        None => crate::select_policy::Consumer::ALL
            .into_iter()
            .map(crate::select_policy::Consumer::exclusion_group)
            .map(str::to_owned)
            .collect(),
    };
    ids.sort();
    ids
}

impl LiveConfig {
    pub fn exclude_groups(&self) -> &ExcludeGroups {
        &self.config.model_routing.exclude
    }

    pub fn panels(&self) -> &PanelPolicies {
        &self.config.model_routing.panels
    }

    pub fn policy_revision(&self) -> String {
        self.revision.clone()
    }

    pub fn operator_policy(&self) -> ModelRoutingPolicyInput {
        self.operator_policy_ref().clone()
    }

    pub fn operator_policy_ref(&self) -> &ModelRoutingPolicyInput {
        &self.compiled.routing.input
    }

    pub(crate) fn compiled_routing_policy(
        &self,
    ) -> &striatum_core::launch_policy::CompiledRoutingPolicy {
        &self.compiled.routing
    }

    pub(crate) fn runner_routes(
        &self,
        runner: &RunnerPolicy,
    ) -> Option<&Arc<striatum_core::provider_model_spec::CompiledSpecs>> {
        runner
            .routes
            .as_ref()
            .and_then(|routes| self.compiled.runner_routes.get(routes))
    }

    pub fn min_band(&self) -> Option<f64> {
        self.config
            .model_routing
            .min_best_effort_adequacy
            .map(|value| value.clamp(0.0, 1.0))
    }

    pub fn account_flags(&self) -> AccountSelectionFlags {
        AccountSelectionFlags { enabled: true }
    }

    pub(crate) fn metered_fallback(&self, provider: &str) -> bool {
        self.compiled.metered_fallback.contains(provider)
    }

    /// The `routing/v1` consumer ids, in id order.
    pub fn v1_consumers(&self) -> Vec<String> {
        self.config
            .consumers
            .keys()
            .filter(|id| !crate::config_policy::is_slot(id))
            .cloned()
            .collect()
    }

    /// Match the daemon-stamped full principal, never the scope owner or a suffix.
    pub fn honours_pin_origin(&self, principal: &str) -> bool {
        self.config
            .pin_origin_callers
            .iter()
            .any(|caller| striatum_core::module_identity::equal(caller, principal))
    }

    /// Operator owners add exact-name claims too, so conflicting owners lock
    /// the name instead of letting either source override the other.
    pub(crate) fn consumer_owners(&self) -> BTreeMap<String, String> {
        self.config
            .consumers
            .iter()
            .filter(|(name, _)| !crate::config_policy::is_slot(name))
            .filter_map(|(name, policy)| {
                policy
                    .owner
                    .as_ref()
                    .map(|owner| (name.clone(), owner.clone()))
            })
            .collect()
    }

    /// The `routing/v1` runners, in preference order.
    pub fn v1_runners(&self) -> Vec<RunnerPolicy> {
        self.config.runners.clone()
    }

    pub(crate) fn consumer_runner_lists(&self) -> BTreeMap<String, Vec<RunnerPolicy>> {
        self.config
            .consumers
            .iter()
            .filter_map(|(name, policy)| {
                policy
                    .runners
                    .clone()
                    .map(|runners| (name.clone(), runners))
            })
            .collect()
    }

    pub(crate) fn has_more_specific_runner_list(&self, consumer: &str, than: &str) -> bool {
        use crate::config_policy::{consumer_matches, consumer_specificity};
        self.config.consumers.iter().any(|(name, policy)| {
            policy.runners.is_some()
                && consumer_matches(name, consumer)
                && consumer_specificity(name) > consumer_specificity(than)
        })
    }

    /// The operator's pin for `consumer` within `project_id`, if any.
    pub fn project_pin(&self, project_id: &str, consumer: &str) -> Option<&str> {
        self.config
            .consumers
            .iter()
            .filter(|(name, _)| {
                !crate::config_policy::is_slot(name)
                    && crate::config_policy::consumer_matches(name, consumer)
            })
            .filter_map(|(name, policy)| {
                policy.projects.get(project_id).map(|project| {
                    (
                        crate::config_policy::consumer_specificity(name),
                        project.pin.as_str(),
                    )
                })
            })
            .max_by_key(|(length, _)| *length)
            .map(|(_, pin)| pin)
    }
    #[cfg(test)]
    pub fn failure_backoff(&self) -> FailureBackoffConfig {
        FailureBackoffConfig {
            consecutive_terminal_failure_threshold: self
                .config
                .model_routing
                .consecutive_terminal_failure_threshold
                .unwrap_or(DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD),
        }
    }
    pub fn catalog_poll_ms(&self) -> u64 {
        self.config
            .catalog
            .poll_ms
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CATALOG_POLL_MS)
    }

    pub fn providers_without_prompt_cache(&self) -> Vec<String> {
        self.config
            .model_routing
            .providers_without_prompt_cache
            .clone()
    }

    /// This document's speed multipliers, subscription multipliers and keep-free
    /// percentages. Built once when the document is published, so every reader
    /// holding this snapshot sees the same values.
    pub fn pricing(&self) -> &RoutePricing {
        &self.pricing
    }

    pub(crate) fn shadowed_operator_grades<'a>(
        &self,
        grades: &'a BTreeMap<String, striatum_store::OperatorQuality>,
        models: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> Vec<String> {
        let catalog = CatalogModelIds::from_models(
            models.chain(grades.keys().filter_map(|key| key.split_once('/'))),
        );
        self.config
            .model_routing
            .models
            .iter()
            .filter(|(key, entry)| {
                let (provider, model, _) =
                    parse_model_key(key, Some(&catalog)).expect("validated model key");
                (entry.elo.is_some() || entry.eq.is_some())
                    && grades.contains_key(&format!("{provider}/{model}"))
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn apply(&self, facts: &mut Vec<ModelFact>) -> SelectionOverrideOutcome {
        let section = &self.config.model_routing;
        let mut unknown_keys = section.models.keys().cloned().collect::<HashSet<_>>();
        let overrides: HashMap<_, _> = if section.models.is_empty() {
            HashMap::new()
        } else {
            let catalog = CatalogModelIds::from_facts(facts);
            section
                .models
                .iter()
                .filter_map(|(key, model)| {
                    canonical_model_key(key, &catalog).map(|route| (route, (key, model)))
                })
                .collect()
        };
        let mut applied = 0;
        for fact in facts.iter_mut() {
            if let Some(&(key, model)) = overrides.get(&fact_model_key(fact)) {
                unknown_keys.remove(key);
                applied += 1;
                let observed_speed = fact.scores.speed;
                let quality = (fact.scores.elo, fact.scores.eq, fact.score_provenance);
                *fact = merge_override(fact, model);
                if model.elo.is_some()
                    && (quality.2 != ScoreProvenance::Unscored || model.eq.is_some())
                {
                    fact.score_provenance = ScoreProvenance::EloOverride;
                } else if model.elo.is_none() {
                    fact.score_provenance = quality.2;
                }
                if model.eq.is_some() {
                    fact.eq_source = Some("override".into());
                }
                if let (Some(cost), Some(plane)) =
                    (model.cost_per_mtok_out, fact.billing_plane.as_mut())
                {
                    plane.rates.insert(
                        "rate.output".into(),
                        striatum_core::billing::BillingRate::Priced { rate: cost },
                    );
                }
                if let Some(speed) = model.output_tokens_per_second {
                    fact.scores.speed = speed * model.speed_multiplier.unwrap_or(1.0);
                } else {
                    fact.scores.speed = observed_speed;
                }
            }
            fact.subscription_multiplier = self
                .pricing
                .subscription_multipliers
                .get(&fact.provider_id)
                .copied();
        }
        let mut unknown_keys = unknown_keys.into_iter().collect::<Vec<_>>();
        unknown_keys.sort();
        SelectionOverrideOutcome {
            routing_config: RoutingConfig {
                demand_exponent: section.demand_exponent.unwrap_or(DEFAULT_DEMAND_EXPONENT),
                overqualification: section
                    .overqualification
                    .resolve()
                    .expect("validated config"),
                ..Default::default()
            },
            applied,
            unknown_keys,
            policy_revision: self.policy_revision(),
        }
    }
}

pub async fn config_refresh_loop(
    path: PathBuf,
    credentials: crate::broca_credentials::SharedBrocaCredentials,
) {
    let mut active = live_config(&path);
    if let Ok(mut credentials) = credentials.lock() {
        credentials
            .concurrency_caps
            .publish_settings(active.concurrency_caps());
    }
    loop {
        tokio::time::sleep(CONFIG_POLL_INTERVAL).await;
        striatum_store::job_timing::sync("config_refresh", || {
            let next = live_config(&path);
            if !Arc::ptr_eq(&active, &next) {
                eprintln!("[striatum] routing-config-reloaded: {}", path.display());
                active = next;
                if let Ok(mut credentials) = credentials.lock() {
                    credentials
                        .concurrency_caps
                        .publish_settings(active.concurrency_caps());
                    credentials.catalog_wake.notify_one();
                }
            }
        });
    }
}

#[cfg(test)]
mod live_snapshot_tests {
    use super::*;

    #[test]
    fn compiled_policy_is_snapshot_owned_and_revision_detects_same_stamp_edits() {
        let scratch = cortexkit_test_support::ScratchDir::new("compiled-policy-revision");
        let path = scratch.path().join("routing.jsonc");
        crate::test_config::write(&path, r#"{"model_routing":{"subscription":["aaaa"]}}"#).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let now = Instant::now();
        let old = live_config_at(&path, now).unwrap();
        assert!(old
            .compiled_routing_policy()
            .subscription
            .resolve("aaaa/model/v", false));
        assert!(std::ptr::eq(
            old.operator_policy_ref(),
            old.operator_policy_ref()
        ));
        assert!(std::ptr::eq(
            old.allocator_policy_ref("unconfigured"),
            old.allocator_policy_ref("another")
        ));
        assert!(std::ptr::eq(
            old.quota_model_pools_ref(),
            old.quota_model_pools_ref()
        ));
        assert!(Arc::ptr_eq(&old, &live_config_at(&path, now).unwrap()));

        crate::test_config::write(&path, r#"{"model_routing":{"subscription":["bbbb"]}}"#).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(metadata.modified().unwrap()))
            .unwrap();
        assert_eq!(metadata.len(), std::fs::metadata(&path).unwrap().len());
        assert!(Arc::ptr_eq(&old, &live_config_at(&path, now).unwrap()));
        let published = live_config_at(&path, now + RELOAD_DEBOUNCE).unwrap();
        assert!(!Arc::ptr_eq(&old, &published));
        assert!(published
            .compiled_routing_policy()
            .subscription
            .resolve("bbbb/model/v", false));
        assert!(!published
            .compiled_routing_policy()
            .subscription
            .resolve("aaaa/model/v", false));
        assert!(old
            .compiled_routing_policy()
            .subscription
            .resolve("aaaa/model/v", false));
    }

    #[test]
    fn changed_config_is_debounced_and_in_flight_snapshot_stays_immutable() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("routing-config-debounce-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("routing.jsonc");
        crate::test_config::write(&path, r#"{"model_routing":{"subscription":["meta"]}}"#).unwrap();
        let now = Instant::now();
        let held = live_config_at(&path, now).unwrap();
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"subscription":["other-provider"]}}"#,
        )
        .unwrap();
        assert!(Arc::ptr_eq(&held, &live_config_at(&path, now).unwrap()));
        assert!(Arc::ptr_eq(
            &held,
            &live_config_at(&path, now + RELOAD_DEBOUNCE / 2).unwrap()
        ));
        let published = live_config_at(&path, now + RELOAD_DEBOUNCE).unwrap();
        assert_eq!(
            published.operator_policy().subscription,
            ["ollama-cloud", "opencode-go", "other-provider"]
        );
        assert_eq!(
            held.operator_policy().subscription,
            ["meta", "ollama-cloud", "opencode-go"]
        );
        assert!(!Arc::ptr_eq(&held, &published));
        std::fs::remove_file(&path).unwrap();
        assert!(Arc::ptr_eq(
            &published,
            &live_config_at(&path, now + RELOAD_DEBOUNCE).unwrap()
        ));
        let retained = live_config_at(&path, now + RELOAD_DEBOUNCE * 2).unwrap();
        assert!(Arc::ptr_eq(&published, &retained));
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[derive(Debug, Clone)]
#[cfg(test)]
struct PromptCacheProvidersCache {
    /// Freshness key is (mtime, byte length), not mtime alone: two writes can
    /// land inside one filesystem clock tick (Linux coarse clocks tick ~4ms;
    /// the CI runner hit it deterministically), and a same-tick same-length
    /// rewrite is the only shape this still misses. See memory rule: a check
    /// whose resolution limit coincides with its use is worse than absent.
    modified_at: Option<SystemTime>,
    byte_len: Option<u64>,
    providers: Vec<String>,
}

#[cfg(test)]
fn prompt_cache_providers_cache() -> &'static Mutex<HashMap<PathBuf, PromptCacheProvidersCache>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, PromptCacheProvidersCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "Routing")]
struct RoutingConfigFile {
    /// Scheduling priority by consumer name. The most specific dotted name wins.
    #[serde(default)]
    #[schemars(title = "Consumer priorities")]
    consumer_priorities: BTreeMap<String, crate::config_policy::PriorityClass>,
    /// Limits on the number of requests running at once.
    #[serde(default)]
    #[schemars(title = "Concurrent request limits")]
    concurrency: crate::concurrency_caps::CapSettings,
    #[serde(flatten)]
    model_routing: ModelRoutingSection,
    /// Routing and account settings by provider ID.
    #[serde(default)]
    #[schemars(title = "Providers")]
    providers: BTreeMap<String, ProviderPolicy>,
    /// Routing preferences by consumer name.
    #[serde(default)]
    #[schemars(title = "Consumers")]
    consumers: BTreeMap<String, ConsumerPolicy>,
    /// Modules that can run models, in order of preference.
    #[serde(default)]
    #[schemars(title = "Runners routing may use")]
    runners: Vec<RunnerPolicy>,
    /// Full caller identities allowed to mark a model choice as made by the user. An empty list allows nobody.
    #[serde(default = "default_pin_origin_callers")]
    #[schemars(title = "Callers allowed to mark user choices")]
    pin_origin_callers: Vec<String>,
    /// Names of operator-facing agents allowed to use reserved subscription quota. Case is ignored.
    #[serde(default)]
    #[schemars(title = "Agents allowed to use reserved quota")]
    head_agents: Vec<String>,
    /// Additional quota pools that apply only to models with a matching name prefix.
    #[serde(default)]
    #[schemars(title = "Additional model quota pools")]
    quota_model_pools: Vec<QuotaModelPoolConfig>,
    /// Settings for refreshing the model catalog.
    #[serde(default)]
    #[schemars(title = "Model catalog refresh")]
    catalog: CatalogSection,
    /// How long to keep routing and account history, and how much scoring detail to keep.
    #[serde(default)]
    #[schemars(title = "Routing history retention")]
    retention: RetentionSection,
    /// Default account selection and spending rules, unless a provider overrides them.
    #[serde(default)]
    #[schemars(title = "Default account settings")]
    accounts: crate::config_policy::ProviderAccountsPolicy,
}

fn default_pin_origin_callers() -> Vec<String> {
    striatum_core::module_identity::spellings("reserved:prefrontal-core")
        .iter()
        .map(ToString::to_string)
        .collect()
}

impl Default for RoutingConfigFile {
    fn default() -> Self {
        Self {
            consumer_priorities: Default::default(),
            concurrency: Default::default(),
            model_routing: Default::default(),
            providers: Default::default(),
            consumers: Default::default(),
            runners: Default::default(),
            pin_origin_callers: default_pin_origin_callers(),
            head_agents: Default::default(),
            quota_model_pools: Default::default(),
            catalog: Default::default(),
            retention: Default::default(),
            accounts: Default::default(),
        }
    }
}

/// Regenerate with `cargo run --locked -p striatum-module --bin ck-striatum -- --print-config-schema`.
pub fn routing_config_schema() -> String {
    settings_schema::render()
}

fn allocator_policy_from(
    defaults: &crate::config_policy::ProviderAccountsPolicy,
    provider: Option<&crate::config_policy::ProviderAccountsPolicy>,
) -> Result<striatum_core::accounts::AllocatorPolicy, String> {
    use striatum_core::accounts::{
        AccountStrategy, AllocatorPolicy, PoolFloor, PoolKey, PoolScope,
    };
    let mut policy = AllocatorPolicy::default();
    for source in std::iter::once(defaults).chain(provider) {
        if let Some(strategy) = &source.strategy {
            policy.strategy = match strategy.as_str() {
                "sticky" => AccountStrategy::Sticky,
                "round_robin" => AccountStrategy::RoundRobin,
                "hybrid" => AccountStrategy::Hybrid,
                "ordered" => AccountStrategy::Ordered,
                _ => return Err("strategy must be sticky, round_robin, hybrid or ordered".into()),
            };
        }
        if let Some(order) = &source.order {
            policy.account_order = order.clone();
        }
        if let Some(order) = &source.ordered {
            policy.fallback_first = match order.as_str() {
                "main-first" => false,
                "fallback-first" => true,
                _ => return Err("ordered must be main-first or fallback-first".into()),
            };
        }
        if let Some(floor) = source.soft_quota_remaining_percent {
            if !floor.is_finite() || !(0.0..=100.0).contains(&floor) {
                return Err("soft_quota_remaining_percent must be in 0..100".into());
            }
            policy.soft_quota_remaining_percent = Some(floor);
        }
        if let Some(floors) = &source.floors {
            policy.pool_floors = floors
                .iter()
                .map(|floor| {
                    if let Some(pool) = &floor.pool {
                        striatum_core::accounts::validate_pool_name(pool)?;
                    }
                    if !floor.remaining_percent.is_finite()
                        || !(0.0..=100.0).contains(&floor.remaining_percent)
                    {
                        return Err("floor remaining_percent must be in 0..100".into());
                    }
                    if floor.pool.is_none() && floor.window_minutes.is_none() {
                        return Err("floor needs a pool or window_minutes".into());
                    }
                    Ok(PoolFloor {
                        key: PoolKey {
                            name: floor.pool.clone(),
                            window_minutes: floor.window_minutes,
                            scope: floor.model.as_ref().map_or(PoolScope::EveryModel, |model| {
                                PoolScope::Models(std::iter::once(model.clone()).collect())
                            }),
                        },
                        remaining_percent: floor.remaining_percent,
                    })
                })
                .collect::<Result<_, String>>()?;
        }
        if let Some(duration) = &source.max_hold {
            policy.max_hold_s = duration.seconds()?;
        }
        if let Some(duration) = &source.hold_when_reset_within {
            duration.seconds()?;
        }
        if let Some(duration) = &source.unknown_reset_retry {
            policy.unknown_reset_retry_ms = i64::try_from(duration.seconds()?)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1000))
                .ok_or("unknown_reset_retry too large")?;
        }
        if let Some(limit) = source.spend_budget_usd {
            if !limit.is_finite() || limit <= 0.0 {
                return Err("spend_budget_usd must be positive".into());
            }
            policy.spend_budget = Some(limit);
        }
        if let Some(enabled) = source.paid_credits {
            policy.paid_credits = enabled;
        }
        if let Some(reset) = source.spend_budget_reset_at_ms {
            policy.spend_budget_reset_at_ms = Some(reset);
        }
    }
    Ok(policy)
}

impl LiveConfig {
    /// With no operator threshold, selection keeps the request's
    /// `hold_wait_max_s` budget, bounded only by the allocator's `max_hold_s`.
    pub(crate) fn select_hold_threshold_s(&self, provider: &str) -> Option<u64> {
        self.config
            .providers
            .get(provider)
            .and_then(|provider| provider.accounts.hold_when_reset_within.as_ref())
            .or(self.config.accounts.hold_when_reset_within.as_ref())
            .map(|duration| {
                duration
                    .seconds()
                    .expect("account durations were validated")
            })
    }

    pub(crate) fn effective_account_policies(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.config
                .providers
                .keys()
                .map(|provider| {
                    (
                        provider.clone(),
                        serde_json::to_value(self.allocator_policy(provider))
                            .expect("account policy serializes"),
                    )
                })
                .collect(),
        )
    }
    /// The configured model-specific quota pools.
    pub(crate) fn quota_model_pools(&self) -> Vec<striatum_core::quota_extract::ModelPool> {
        self.quota_model_pools_ref().to_vec()
    }

    pub(crate) fn quota_model_pools_ref(&self) -> &[striatum_core::quota_extract::ModelPool] {
        &self.compiled.model_pools
    }

    /// Whether `agent` is a head under `head_agents`.
    pub(crate) fn is_head_agent(&self, agent: &str) -> bool {
        let agent = agent.trim();
        self.config
            .head_agents
            .iter()
            .any(|head| head.trim().eq_ignore_ascii_case(agent))
    }

    pub(crate) fn allocator_policy(
        &self,
        provider: &str,
    ) -> striatum_core::accounts::AllocatorPolicy {
        self.allocator_policy_ref(provider).clone()
    }

    pub(crate) fn allocator_policy_ref(
        &self,
        provider: &str,
    ) -> &striatum_core::accounts::AllocatorPolicy {
        self.compiled
            .allocators
            .get(provider)
            .unwrap_or(&self.compiled.allocator_default)
    }
}

#[cfg(test)]
mod account_policy_config_tests {
    use super::*;

    #[test]
    fn provider_account_policy_overrides_defaults_without_weakening_the_schema() {
        let config=parse_config(r#"{
            "accounts":{"max_hold":"90s"},
            "providers":{"synthetic":{"accounts":{"strategy":"hybrid","max_hold":"30s",
                "soft_quota_remaining_percent":10,"floors":[{"pool":"pool-a","window_minutes":300,"remaining_percent":5}],
                "spend_budget_usd":20,"spend_budget_reset_at_ms":1800000600000,"paid_credits":true,"unknown_reset_retry":"2h"}}}
        }"#).unwrap();
        let policy = allocator_policy_from(
            &config.accounts,
            Some(&config.providers["synthetic"].accounts),
        )
        .unwrap();
        assert_eq!(
            policy.strategy,
            striatum_core::accounts::AccountStrategy::Hybrid
        );
        assert_eq!(policy.max_hold_s, 30);
        assert_eq!(policy.unknown_reset_retry_ms, 7_200_000);
        assert_eq!(policy.pool_floors[0].key.name.as_deref(), Some("pool-a"));
        assert_eq!(policy.spend_budget, Some(20.0));
        assert!(policy.paid_credits);
        for invalid in [
            r#"{"providers":{"p":{"accounts":{"strategy":"random"}}}}"#,
            r#"{"providers":{"p":{"accounts":{"max_hold":"tomorrow"}}}}"#,
            r#"{"providers":{"p":{"accounts":{"unknown_policy":true}}}}"#,
            r#"{"accounts":{"hold_when_reset_within":"tomorrow"}}"#,
        ] {
            assert!(parse_config(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn select_hold_threshold_is_an_explicit_provider_or_global_limit() {
        let config = parse_config(r#"{"accounts":{"hold_when_reset_within":"2m"},"providers":{"p":{"accounts":{"hold_when_reset_within":"30s"}}}}"#).unwrap();
        let live = LiveConfig::new(config, None);
        assert_eq!(live.select_hold_threshold_s("p"), Some(30));
        assert_eq!(live.select_hold_threshold_s("other"), Some(120));
        assert_eq!(LiveConfig::default().select_hold_threshold_s("p"), None);
    }
}

/// Internal account-operation settings. Account selection always serves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountSelectionFlags {
    /// Retained for internal callers; the runtime loader always enables serving.
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CatalogSection {
    /// Time between model catalog refreshes, in milliseconds. Must be at least 1.
    #[serde(default)]
    #[schemars(range(min = 1))]
    #[schemars(title = "Refresh interval (milliseconds)")]
    poll_ms: Option<u64>,
}

/// How long the routing store keeps each history table, in whole days, and
/// how many decisions keep full scoring detail. The retention pass rereads
/// this block every minute. A value that is not a whole number keeps its
/// default and one below its minimum is raised to it, each with a warning, so
/// a bad value here never rejects the rest of the file.
#[derive(Debug, Deserialize, Serialize, Default, Clone, JsonSchema)]
struct RetentionSection {
    /// Days to keep account selections after their last activity. Minimum 7 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>", range(min = 7))]
    #[schemars(title = "Account selections (days)")]
    account_selection_days: Option<serde_json::Value>,
    /// Days to keep reports from model runners. Minimum 1 day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>", range(min = 1))]
    #[schemars(title = "Runner reports (days)")]
    account_observation_days: Option<serde_json::Value>,
    /// Days to keep the summary of each routing decision. Minimum 1 day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>", range(min = 1))]
    #[schemars(title = "Routing decision summaries (days)")]
    model_route_decisions_days: Option<serde_json::Value>,
    /// Days to keep full scores and quota details for each routing decision. Minimum 1 day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>", range(min = 1))]
    #[schemars(title = "Full routing decision details (days)")]
    model_route_decisions_full_detail_days: Option<serde_json::Value>,
    /// Keep full scoring details for one decision in this many until its summary expires. Set to 0 to keep no long-term samples.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>")]
    #[schemars(title = "Long-term detail sampling (one in N)")]
    model_route_decisions_full_detail_sample_one_in: Option<serde_json::Value>,
    /// Days to keep usage records for individual requests. Minimum 7 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<u64>", range(min = 7))]
    #[schemars(title = "Request usage records (days)")]
    model_route_usage_days: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct QuotaModelPoolConfig {
    /// Beginning of the model name that this quota rule matches.
    #[schemars(title = "Model name prefix")]
    model_prefix: String,
    /// Name of the additional quota pool used by matching models.
    #[schemars(title = "Additional quota pool")]
    pool: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModelRoutingSection {
    /// Excluded providers or routes by consumer. The `all` list applies to every
    /// consumer; a negated entry in another list cannot remove its exclusions.
    /// Exclusion keeps a route out of automatic choice only: a request that names
    /// the model (a work run's `model=`, a synthesizer or planner pin) is still
    /// admitted when the route is otherwise servable.
    #[serde(skip)]
    exclude: ExcludeGroups,
    /// How many models to choose and how to rank them for each panel type.
    #[serde(default)]
    #[schemars(title = "Model selection for panels")]
    panels: PanelPolicies,
    #[serde(skip)]
    pins: Pins,
    /// Providers and models priced as subscription-backed in addition to auth facts.
    #[serde(skip)]
    subscription: Vec<String>,
    /// Override catalog values or routing rules by provider/model or provider/model/variant.
    #[serde(default)]
    #[schemars(title = "Model overrides")]
    models: HashMap<String, ModelFactOverride>,
    /// Set how quickly the reasoning quality target rises as a request asks for more. Use a number greater than 0.
    #[serde(default)]
    #[schemars(transform = positive_number)]
    #[schemars(title = "How quickly the reasoning target rises")]
    demand_exponent: Option<f64>,
    /// How much to discourage using a stronger model than a request needs.
    #[serde(default)]
    #[schemars(title = "Penalty for using stronger models")]
    overqualification: OverqualificationSection,
    /// Minimum fit for choosing a fallback model when none meets the quality target. Use a score from 0 to 1.
    #[serde(default)]
    #[schemars(title = "Minimum fit for a fallback model")]
    min_best_effort_adequacy: Option<f64>,
    /// Number of failed sends in a row before temporarily avoiding a route. Must be at least 1.
    #[serde(default)]
    #[schemars(range(min = 1))]
    #[schemars(title = "Failures before a route is avoided")]
    consecutive_terminal_failure_threshold: Option<u32>,
    /// Providers that do not expose prompt-cache reads or writes. Gather seats
    /// avoid these providers when a cache-capable adequate route exists.
    #[serde(skip)]
    providers_without_prompt_cache: Vec<String>,
    /// Divide estimated subscription costs by these provider-specific numbers. Use numbers greater than 0; a provider's own setting takes precedence.
    #[serde(default, skip_serializing)]
    #[schemars(title = "Subscription cost divisors")]
    subscription_multipliers: HashMap<String, f64>,
    /// Percent of each weekly account window reserved from delegated workers;
    /// long-lived head agents may spend that reserved share.
    #[serde(skip)]
    keep_free_percent: HashMap<String, f64>,
    /// The `routing/v1` consumer ids `route.select` answers. Absent: every
    /// request class (mason, sidekick, athena, lab, core, codemode, flow) and
    /// Magic Context's mc_summary, mc_dream, mc_dream_verify, mc_dream_map,
    /// mc_dream_curate, mc_dream_classify, mc_dream_review_user,
    /// mc_dream_retrospective, mc_dream_primers, mc_dream_compress_cues and
    /// mc_dream_docs. When present, this array replaces the defaults; only its
    /// listed ids are accepted.
    /// Codemode and flow are answered only with routes priced per token.
    #[serde(skip)]
    consumers: Option<Vec<String>>,
    /// The runner modules `routing/v1` answers with, in preference order.
    /// Absent: Broca alone, on the routes its credentials serve.
    #[serde(skip)]
    runners: Option<Vec<RunnerPolicy>>,
    /// Operator pins per project, `{<project id>: {<consumer>:
    /// "provider/model[:variant]"}}`. A `routing/v1` select on a route bound to
    /// that project, for that consumer, that names no pin of its own runs on
    /// the pinned model when it can be served, and is routed as usual when not.
    #[serde(skip)]
    project_pins: BTreeMap<String, BTreeMap<String, String>>,
}

fn positive_number(schema: &mut schemars::Schema) {
    schema.insert("exclusiveMinimum".into(), serde_json::json!(0));
}

impl Default for ModelRoutingSection {
    fn default() -> Self {
        serde_json::from_str("{}").expect("empty model routing section")
    }
}

/// Additive mode (default) preserves the tunable cost term. Cost-scaled mode
/// multiplies selection cost by the fixed core curve, bounded to 1..20; custom
/// threshold/doubling/base values are refused in that mode.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OverqualificationSection {
    /// Add a fixed-scale penalty (`additive`) or multiply the estimated cost (`cost_scaled`).
    #[serde(default)]
    #[schemars(title = "Penalty method")]
    mode: OverqualificationMode,
    /// How far the model's reasoning score may exceed the target before adding a penalty. In additive mode only; use 0 or more. If omitted in additive mode, uses 40.
    #[schemars(range(min = 0))]
    #[schemars(title = "Extra reasoning score allowed without penalty")]
    threshold: Option<f64>,
    /// Extra reasoning score needed to double the penalty. In additive mode only; use a number greater than 0. If omitted in additive mode, uses 10.
    #[schemars(transform = positive_number)]
    #[schemars(title = "Extra reasoning score to double the penalty")]
    doubling: Option<f64>,
    /// Starting penalty before it grows with excess reasoning quality. In additive mode only; use 0 or more. If omitted in additive mode, uses 0.05.
    #[schemars(range(min = 0))]
    #[schemars(title = "Starting penalty")]
    base: Option<f64>,
}

impl OverqualificationSection {
    fn resolve(self) -> Result<OverqualificationConfig, String> {
        if self.mode == OverqualificationMode::CostScaled
            && (self.threshold.is_some() || self.doubling.is_some() || self.base.is_some())
        {
            return Err("overqualification.mode cost_scaled uses a fixed curve; threshold, doubling and base are allowed only in additive mode".into());
        }
        let config = OverqualificationConfig {
            mode: match self.mode {
                OverqualificationMode::Additive => {
                    striatum_core::routing_config::OverqualificationMode::Additive
                }
                OverqualificationMode::CostScaled => {
                    striatum_core::routing_config::OverqualificationMode::CostScaled
                }
            },
            threshold: self
                .threshold
                .unwrap_or(DEFAULT_OVERQUALIFICATION.threshold),
            doubling: self.doubling.unwrap_or(DEFAULT_OVERQUALIFICATION.doubling),
            base: self.base.unwrap_or(DEFAULT_OVERQUALIFICATION.base),
        };
        if config.is_valid() {
            Ok(config)
        } else {
            Err(format!(
                "model_routing.overqualification needs finite threshold >= 0, doubling > 0 and base >= 0, got threshold {}, doubling {}, base {}",
                config.threshold, config.doubling, config.base
            ))
        }
    }
}

/// One per-model override. Every field is optional: a present field is operator-owned,
/// while a withdrawn field returns to its non-override authority.
#[derive(Debug, Deserialize, Serialize, Default, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModelFactOverride {
    /// Override the model's reasoning score. Use a number greater than 0, or omit to use the catalog score.
    #[schemars(transform = positive_number)]
    #[schemars(title = "Reasoning score override")]
    elo: Option<f64>,
    /// Override the model's collaboration score. Use a number greater than 0, or omit to use the catalog score.
    #[schemars(transform = positive_number)]
    #[schemars(title = "Judgment score override")]
    eq: Option<f64>,
    /// Override generation speed in output tokens per second, from 0 upward. Omit to use measured or catalog speed.
    #[schemars(range(min = 0))]
    #[schemars(title = "Generation speed (tokens per second)")]
    output_tokens_per_second: Option<f64>,
    /// Override cost in US dollars per million output tokens, from 0 upward.
    #[schemars(range(min = 0))]
    #[schemars(title = "Price per million output tokens (USD)")]
    cost_per_mtok_out: Option<f64>,
    /// Override the context window in tokens. Use a number greater than 0, or omit to use the catalog size.
    #[schemars(transform = positive_number)]
    #[schemars(title = "Context window (tokens)")]
    context_window_tokens: Option<f64>,
    /// Override whether the model can read images. Omit to use catalog support.
    #[schemars(title = "Image support")]
    vision: Option<bool>,
    /// Multiply the speed used to compare this model with others. Use a number greater than 0 and up to 2.
    #[schemars(range(max = 2), transform = positive_number)]
    #[schemars(title = "Speed comparison multiplier")]
    speed_multiplier: Option<f64>,
    /// This setting is ignored. Set the subscription cost multiplier on the provider instead.
    #[schemars(title = "Subscription cost divisor (ignored)")]
    subscription_multiplier: Option<f64>,
    #[serde(flatten)]
    rules: crate::config_policy::EntityPolicy,
}

/// The absolute global file path the daemon accepts for this module identity.
pub(super) fn config_path(module_id: &str) -> Result<PathBuf, String> {
    let home = PathBuf::from(cortexkit_store_types::resolve_config_home());
    if !home.is_absolute() {
        let variables = if cfg!(windows) {
            "XDG_CONFIG_HOME, APPDATA, USERPROFILE, or HOME"
        } else {
            "XDG_CONFIG_HOME or HOME"
        };
        return Err(format!(
            "routing config home is relative ({home:?}); set {variables} to an absolute path"
        ));
    }
    let filename = format!("{module_id}.jsonc");
    if module_id.is_empty()
        || matches!(module_id, "." | "..")
        || module_id.contains(['/', '\\'])
        || !matches!(
            Path::new(&filename).components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(format!(
            "routing module id {module_id:?} cannot form a settings filename"
        ));
    }
    Ok(home.join("cortexkit").join(filename))
}

type ModelKey<'a> = (&'a str, &'a str, Option<&'a str>);

struct CatalogModelIds<'a>(HashSet<(&'a str, &'a str)>);

impl<'a> CatalogModelIds<'a> {
    fn from_facts(facts: &'a [ModelFact]) -> Self {
        Self::from_models(
            facts
                .iter()
                .map(|fact| (fact.provider_id.as_str(), fact.model_id.as_str())),
        )
    }

    fn from_models(models: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        // Only model ids with a slash could also be read as a model and variant.
        Self(models.filter(|(_, model)| model.contains('/')).collect())
    }
}

fn fact_model_key(fact: &ModelFact) -> ModelKey<'_> {
    (&fact.provider_id, &fact.model_id, fact.variant.as_deref())
}

fn parse_model_key<'a>(
    key: &'a str,
    catalog: Option<&CatalogModelIds<'_>>,
) -> Option<ModelKey<'a>> {
    let (provider, rest) = key.trim().split_once('/')?;
    let (provider, rest) = (provider.trim(), rest.trim());
    if provider.is_empty() || rest.split('/').any(|part| part.trim().is_empty()) {
        return None;
    }
    // The catalog does not use the same slash-form route for two identities.
    // If a model id and a variant ever share a spelling, prefer the model id.
    if catalog.is_some_and(|catalog| catalog.0.contains(&(provider, rest))) {
        return Some((provider, rest, None));
    }
    Some(match rest.rsplit_once('/') {
        Some((model, variant)) => (provider, model.trim(), Some(variant.trim())),
        None => (provider, rest, None),
    })
}

fn canonical_model_key<'a>(key: &'a str, catalog: &CatalogModelIds<'_>) -> Option<ModelKey<'a>> {
    let (provider, model, variant) = parse_model_key(key, Some(catalog))?;
    // Validation accepts whitespace around segments, but only keys spelled
    // exactly like catalog routes may change model facts or count as served.
    (key == make_route_key(provider, model, variant)).then_some((provider, model, variant))
}

fn merge_override(base: &ModelFact, ov: &ModelFactOverride) -> ModelFact {
    let mut fact = base.clone();
    fact.scores = ModelScores {
        elo: ov.elo.unwrap_or(base.scores.elo),
        eq: ov.eq.unwrap_or(base.scores.eq),
        speed: ov.output_tokens_per_second.unwrap_or(base.scores.speed),
    };
    if ov.cost_per_mtok_out.is_some() {
        fact.cost_per_m_tok_out = ov.cost_per_mtok_out;
    }
    if ov.context_window_tokens.is_some() {
        fact.context_window = ov.context_window_tokens;
    }
    if ov.vision.is_some() {
        fact.supports_vision = ov.vision;
    }
    if ov.speed_multiplier.is_some() {
        fact.speed_multiplier = ov.speed_multiplier;
    }
    fact.confidence = Confidence::Curated;
    fact
}

#[cfg(test)]
const CONFIG_CATALOG_FACT_FIELDS: [CatalogFactField; 3] = [
    CatalogFactField::ContextWindow,
    CatalogFactField::CostPerMtokOut,
    CatalogFactField::SupportsVision,
];

#[cfg(test)]
fn named_catalog_fields(ov: &ModelFactOverride) -> Vec<CatalogFactField> {
    let mut fields = Vec::new();
    if ov.context_window_tokens.is_some() {
        fields.push(CatalogFactField::ContextWindow);
    }
    if ov.cost_per_mtok_out.is_some() {
        fields.push(CatalogFactField::CostPerMtokOut);
    }
    if ov.vision.is_some() {
        fields.push(CatalogFactField::SupportsVision);
    }
    fields
}

#[cfg(test)]
fn restore_withdrawn_catalog_fields(
    base: &mut ModelFact,
    seed: Option<&ModelFact>,
    withdrawn: &[CatalogFactField],
) {
    for field in withdrawn {
        match field {
            CatalogFactField::ContextWindow => {
                base.context_window = seed.and_then(|fact| fact.context_window)
            }
            CatalogFactField::MaxOutputTokens => {
                base.max_output_tokens = seed.and_then(|fact| fact.max_output_tokens)
            }
            CatalogFactField::CostTier => base.cost_tier = seed.and_then(|fact| fact.cost_tier),
            CatalogFactField::CostPerMtokOut => {
                base.cost_per_m_tok_out = seed.and_then(|fact| fact.cost_per_m_tok_out)
            }
            CatalogFactField::SupportsVision => {
                base.supports_vision = seed.and_then(|fact| fact.supports_vision)
            }
            CatalogFactField::SupportsToolUse => {
                base.supports_tool_use = seed.and_then(|fact| fact.supports_tool_use)
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
#[cfg(test)]
pub struct OverrideOutcome {
    pub applied: usize,
    pub reverted: usize,
    pub skipped: Vec<String>,
}

/// Validate the supplied value and clamp finite values to [0, 1]. NaN is rejected
/// with an error. The caller handles an absent configuration field by using the
/// module default.
pub fn validate_min_best_effort_adequacy(raw: f64) -> Result<f64, String> {
    if raw.is_nan() {
        return Err("model_routing.min_best_effort_adequacy must be a number, got NaN".to_string());
    }
    Ok(raw.clamp(0.0, 1.0))
}

/// Reconcile explicit overrides and the default family rules against the store.
/// A missing file keeps catalog families complete while adding no explicit override.
#[cfg(test)]
pub fn apply_overrides_from_config<S: RouteStateStore>(
    store: &S,
    path: &std::path::Path,
) -> Result<OverrideOutcome, String> {
    let config = read_routing_config(path)?.unwrap_or_default();
    let outcome = apply_overrides(store, &config.model_routing.models)?;
    Ok(outcome)
}

#[cfg(test)]
fn read_routing_config(path: &Path) -> Result<Option<RoutingConfigFile>, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(document) => document,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&doc)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    serde_json::from_str(&json)
        .map(Some)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))
}

/// Read the active managed document or, in file mode, parse untyped JSON.
/// In file mode, the retention reader still works when other blocks in the
/// file are invalid, and a missing file is `Ok(None)`.
pub(crate) fn read_routing_config_value(path: &Path) -> Result<Option<serde_json::Value>, String> {
    let managed = live_configs()
        .lock()
        .unwrap()
        .get(path)
        .filter(|cache| cache.managed)
        .map(|cache| Arc::clone(&cache.active));
    if let Some(active) = managed {
        return active
            .document
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| error.to_string());
    }
    let doc = match std::fs::read_to_string(path) {
        Ok(document) => document,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&doc)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    serde_json::from_str(&json)
        .map(Some)
        .map_err(|error| format!("invalid json in {}: {error}", path.display()))
}

/// Read `min_best_effort_adequacy` from the routing config file, validating and
/// clamping the value. Returns `None` when absent or the file is missing.
/// Poll cadence for the Fusiform catalog consumer. Missing configuration uses
/// ten minutes; zero is refused because it would create a reconnect hot loop.
#[cfg(test)]
pub fn read_catalog_poll_ms(path: &Path) -> Result<u64, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(doc) => doc,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DEFAULT_CATALOG_POLL_MS)
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&doc).map_err(|message| format!("invalid jsonc: {message}"))?;
    let config: RoutingConfigFile =
        serde_json::from_str(&json).map_err(|error| format!("invalid config: {error}"))?;
    match config.catalog.poll_ms {
        Some(0) => Err("catalog.poll_ms must be greater than zero".to_string()),
        Some(poll_ms) => Ok(poll_ms),
        None => Ok(DEFAULT_CATALOG_POLL_MS),
    }
}

#[cfg(test)]
pub fn read_failure_backoff_config(path: &Path) -> Result<FailureBackoffConfig, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(document) => document,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(FailureBackoffConfig::default());
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&doc)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let threshold = config
        .model_routing
        .consecutive_terminal_failure_threshold
        .unwrap_or(DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD);
    if threshold == 0 {
        return Err(
            "model_routing.consecutive_terminal_failure_threshold must be at least 1".to_string(),
        );
    }
    Ok(FailureBackoffConfig {
        consecutive_terminal_failure_threshold: threshold,
    })
}

#[cfg(test)]
pub fn read_min_best_effort_adequacy(path: &std::path::Path) -> Result<Option<f64>, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("reading {}: {e}", path.display())),
    };
    let json =
        jsonc_to_json(&doc).map_err(|m| format!("invalid jsonc in {}: {m}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|e| format!("invalid config in {}: {e}", path.display()))?;
    match config.model_routing.min_best_effort_adequacy {
        None => Ok(None),
        Some(raw) => validate_min_best_effort_adequacy(raw).map(Some),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectionOverrideOutcome {
    pub routing_config: RoutingConfig,
    pub applied: usize,
    pub unknown_keys: Vec<String>,
    /// SHA-256 of the live policy document whose selection settings were applied.
    pub policy_revision: String,
}

/// The reproducible identifier for selection that intentionally used built-in defaults.
pub fn default_policy_revision() -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(
            format!(
                "model-routing-default:{DEFAULT_DEMAND_EXPONENT}:overqualification:{}:{}:{}",
                DEFAULT_OVERQUALIFICATION.threshold,
                DEFAULT_OVERQUALIFICATION.doubling,
                DEFAULT_OVERQUALIFICATION.base
            )
            .as_bytes()
        )
    )
}

fn policy_revision(document: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(document.as_bytes()))
}

/// Re-read request-time quality settings and apply known model overrides to this
/// selection's fact snapshot without persisting them.
#[cfg(test)]
pub fn apply_live_selection_config(
    path: &Path,
    facts: &mut Vec<ModelFact>,
) -> Result<SelectionOverrideOutcome, String> {
    let document = match std::fs::read_to_string(path) {
        Ok(document) => Some(document),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let config = document
        .as_deref()
        .map(parse_config)
        .transpose()?
        .unwrap_or_default();
    Ok(LiveConfig::new(config, document).apply(facts))
}

/// Read the cache capability deny-list for the gather-seat preference. The file
/// is checked at selection time so changing this operational policy does not need a
/// routing-module restart, but the JSONC is reparsed only after its mtime changes.
/// A missing file or missing field uses the safe default.
#[cfg(test)]
pub fn read_providers_without_prompt_cache(path: &Path) -> Result<Vec<String>, String> {
    read_providers_without_prompt_cache_with_reader(path, |path| {
        std::fs::read_to_string(path)
            .map_err(|error| format!("reading {}: {error}", path.display()))
    })
}

#[cfg(test)]
fn read_providers_without_prompt_cache_with_reader<F>(
    path: &Path,
    read_file: F,
) -> Result<Vec<String>, String>
where
    F: FnOnce(&Path) -> Result<String, String>,
{
    let (modified_at, byte_len) = match std::fs::metadata(path) {
        Ok(metadata) => {
            let modified = metadata.modified().map(Some).map_err(|error| {
                format!("reading modification time for {}: {error}", path.display())
            })?;
            (modified, Some(metadata.len()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, None),
        Err(error) => return Err(format!("reading metadata for {}: {error}", path.display())),
    };
    let mut cache = prompt_cache_providers_cache()
        .lock()
        .map_err(|_| "prompt-cache provider cache lock poisoned".to_string())?;
    if let Some(cached) = cache.get(path) {
        if cached.modified_at == modified_at && cached.byte_len == byte_len {
            return Ok(cached.providers.clone());
        }
    }

    let providers = match modified_at {
        None => Ok(Vec::new()),
        Some(_) => read_file(path).and_then(|doc| {
            let json = jsonc_to_json(&doc)
                .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
            let config = parse_config(&json)?;
            let mut providers = config
                .model_routing
                .providers_without_prompt_cache
                .into_iter()
                .map(|provider| provider.trim().to_string())
                .filter(|provider| !provider.is_empty())
                .collect::<Vec<_>>();
            providers.sort();
            providers.dedup();
            Ok(providers)
        }),
    };

    match providers {
        Ok(providers) => {
            cache.insert(
                path.to_path_buf(),
                PromptCacheProvidersCache {
                    modified_at,
                    byte_len,
                    providers: providers.clone(),
                },
            );
            Ok(providers)
        }
        Err(error) => {
            // A malformed live edit must not leave a previously parsed policy in
            // the cache: the caller falls back to the default until it is repaired.
            cache.remove(path);
            Err(error)
        }
    }
}

/// The operator's routing policy as it was last read, keyed by config path.
#[derive(Debug, Clone)]
#[cfg(test)]
struct OperatorPolicyCache {
    /// Freshness key of the document behind `policy`: (mtime, byte length). Same
    /// two-part key as the prompt-cache provider list, for the same reason: two
    /// writes can land inside one filesystem clock tick.
    key: (Option<SystemTime>, Option<u64>),
    /// The last document that parsed. A malformed live edit keeps this value: an
    /// operator who breaks their config while editing must not silently lose the
    /// fence and have routing start picking the models they excluded.
    policy: ModelRoutingPolicyInput,
    /// Freshness key of the malformed document already reported, so a broken file
    /// logs once per edit rather than once per selection.
    reported_failure: Option<(Option<SystemTime>, Option<u64>)>,
}

#[cfg(test)]
fn operator_policy_cache() -> &'static Mutex<HashMap<PathBuf, OperatorPolicyCache>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, OperatorPolicyCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The operator's `model_routing` policy from the routing-owned config.
///
/// This is the routing fence, so it never fails into an empty policy: a missing
/// file or missing field IS an empty policy (the operator excluded nothing), but a
/// malformed file keeps the last policy that parsed and logs once per edit. The
/// file is re-read only after its mtime or length changes, so selection reads it
/// on the hot path without re-parsing per request and an operator edit takes
/// effect on the next selection with no restart.
pub fn read_operator_routing_policy(path: &Path) -> ModelRoutingPolicyInput {
    live_config(path).operator_policy()
}

#[cfg(test)]
fn read_operator_routing_policy_with_reader<F>(path: &Path, read_file: F) -> ModelRoutingPolicyInput
where
    F: FnOnce(&Path) -> Result<String, String>,
{
    let (key, missing) = match std::fs::metadata(path) {
        Ok(metadata) => ((metadata.modified().ok(), Some(metadata.len())), false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ((None, None), true),
        // Unreadable for another reason: fall through to the read, which reports
        // the concrete error and preserves the last good policy.
        Err(_) => ((None, None), false),
    };

    // A poisoned lock means some other thread panicked while holding a plain
    // cache. Recover the map instead of returning an empty fence.
    let mut cache = operator_policy_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cache.get(path) {
        if cached.key == key || cached.reported_failure == Some(key) {
            return cached.policy.clone();
        }
    }

    let parsed = if missing {
        Ok(ModelRoutingPolicyInput::default())
    } else {
        read_file(path).and_then(|doc| parse_operator_routing_policy(path, &doc))
    };

    match parsed {
        Ok(policy) => {
            cache.insert(
                path.to_path_buf(),
                OperatorPolicyCache {
                    key,
                    policy: policy.clone(),
                    reported_failure: None,
                },
            );
            policy
        }
        Err(error) => {
            let last_good = cache.get(path).cloned();
            let policy = last_good
                .as_ref()
                .map(|cached| cached.policy.clone())
                .unwrap_or_default();
            eprintln!(
                "[striatum] operator routing policy {} is unusable ({error}); keeping the last good policy ({} exclusion entries)",
                path.display(),
                policy.exclude.len()
            );
            cache.insert(
                path.to_path_buf(),
                OperatorPolicyCache {
                    // Keep the last GOOD document's key so repairing the file (or
                    // reverting it) is seen as a change and re-read.
                    key: last_good.map(|cached| cached.key).unwrap_or((None, None)),
                    policy: policy.clone(),
                    reported_failure: Some(key),
                },
            );
            policy
        }
    }
}

#[cfg(test)]
fn parse_operator_routing_policy(
    path: &Path,
    document: &str,
) -> Result<ModelRoutingPolicyInput, String> {
    let config = parse_config(document)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let section = config.model_routing;
    Ok(ModelRoutingPolicyInput {
        exclude: clean_spec_entries(section.exclude.all),
        subscription: clean_spec_entries(section.subscription),
        score_alias: BTreeMap::new(),
    })
}

fn clean_spec_entries(entries: Vec<String>) -> Vec<String> {
    entries
        .into_iter()
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
        .collect()
}

/// A stable identity for one operator policy, recorded with a decision so
/// `route.explain` can say WHICH fence produced it. Derived from the policy's
/// entries rather than the whole config file: unrelated routing settings must
/// not read as a selection-policy change.
pub fn operator_policy_revision(policy: &ModelRoutingPolicyInput) -> String {
    let mut hasher = Sha256::new();
    for (label, entries) in [
        ("exclude", &policy.exclude),
        ("subscription", &policy.subscription),
    ] {
        hasher.update(label.as_bytes());
        hasher.update([0u8]);
        for entry in entries {
            hasher.update(entry.as_bytes());
            hasher.update([0u8]);
        }
        hasher.update([0x1eu8]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
fn apply_overrides<S: RouteStateStore>(
    store: &S,
    overrides: &HashMap<String, ModelFactOverride>,
) -> Result<OverrideOutcome, String> {
    // The seed supplies withdrawn catalog values whose prior external value is not
    // retained. Other fields stay on the live row so a partial override cannot reset
    // unrelated catalog updates or operator-placed scores.
    let seed_by_key: HashMap<String, ModelFact> = model_facts_seed()
        .iter()
        .map(|f| {
            (
                make_route_key(&f.provider_id, &f.model_id, f.variant.as_deref()),
                f.clone(),
            )
        })
        .collect();

    let mut outcome = OverrideOutcome::default();
    let mut next_override_keys: HashSet<String> = HashSet::new();

    for (key, ov) in overrides {
        let Some(mut parsed) = parse_model_key(key, None) else {
            outcome.skipped.push(key.clone());
            continue;
        };
        if parsed.2.is_some() {
            let capacity_keys = store
                .get_capacity_keys_for_provider(parsed.0)
                .map_err(|error| format!("reading catalog models for {key}: {error}"))?;
            let catalog = CatalogModelIds::from_models(
                capacity_keys.iter().filter_map(|key| key.split_once('/')),
            );
            parsed = parse_model_key(key, Some(&catalog)).expect("validated model key");
        }
        let (provider_id, model_id, variant) = parsed;
        let route_key = make_route_key(provider_id, model_id, variant);
        let existing_fact = store
            .get_fact_by_route_key(&route_key)
            .map_err(|error| format!("reading override base {route_key}: {error}"))?;
        let seed_fact = seed_by_key.get(&route_key);
        let named_fact_fields = named_catalog_fields(ov);
        let omitted_fact_fields = CONFIG_CATALOG_FACT_FIELDS
            .into_iter()
            .filter(|field| !named_fact_fields.contains(field))
            .collect::<Vec<_>>();
        let withdrawn_fact_fields = store
            .clear_operator_fact_fields(&route_key, &omitted_fact_fields)
            .map_err(|e| format!("clear withdrawn override provenance {route_key}: {e}"))?;
        let mut base = existing_fact
            .clone()
            .or_else(|| seed_fact.cloned())
            .unwrap_or_else(|| ModelFact {
                billing_plane: None,
                auth_methods: None,
                login_methods_documented: false,
                speed_is_fleet_median: false,
                decommissioned: false,
                provider_id: provider_id.into(),
                model_id: model_id.into(),
                variant: variant.map(str::to_owned),
                family: None,
                scores: ModelScores::default(),
                context_window: None,
                max_output_tokens: None,
                cost_tier: None,
                cost_per_m_tok_out: None,
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: None,
                supports_tool_use: None,
                prompt_defaults: None,
                confidence: Confidence::Curated,
                notes: None,
                speed_multiplier: None,
                subscription_multiplier: None,
                score_provenance: ScoreProvenance::Curated,
                aa_index: None,
                review_by_ms: None,
                eq_source: None,
                class_vector: None,
                class_vector_is_overlay: false,
                upstream_status: None,
                model_type: None,
                model_kind: None,
                mode_of: None,
            });
        // Score provenance does not distinguish config from route.model_upsert.
        // Preserving absent score fields avoids erasing an upsert placement on restart;
        // withdrawing a config-authored score remains ambiguous until origin is stored.
        // The store retains Fusiform provenance, but not a shadow copy of the value
        // replaced by an override. Restore only stamps that were actually withdrawn,
        // using the seed when it has the route and otherwise the unknown value.
        restore_withdrawn_catalog_fields(&mut base, seed_fact, &withdrawn_fact_fields);
        crate::account_writer::upsert_fact(store, &merge_override(&base, ov), FactSource::Override)
            .map_err(|e| format!("upsert override {route_key}: {e}"))?;
        store
            .mark_operator_fact_fields(&route_key, &named_fact_fields)
            .map_err(|e| format!("stamp override provenance {route_key}: {e}"))?;
        next_override_keys.insert(route_key);
        outcome.applied += 1;
    }

    // Revert override rows no longer present in config: back to the seed fact, or
    // delete if the model is not in the seed.
    let existing = store
        .list_override_route_keys()
        .map_err(|e| format!("listing override keys: {e}"))?;
    for route_key in existing {
        if next_override_keys.contains(&route_key) {
            continue;
        }
        match seed_by_key.get(&route_key) {
            Some(seed_fact) => {
                let mut restored = seed_fact.clone();
                if restored.max_output_tokens.is_none() {
                    restored.max_output_tokens = store
                        .get_fact_by_route_key(&route_key)
                        .map_err(|error| {
                            format!("reading stale override base {route_key}: {error}")
                        })?
                        .and_then(|fact| fact.max_output_tokens);
                }
                crate::account_writer::upsert_fact(store, &restored, FactSource::Seed)
                    .map_err(|e| format!("reverting {route_key} to seed: {e}"))?
            }
            // Not in the JSON seed: DEMOTE the source stamp, never delete the
            // row. The catalog holds migration-seeded facts the JSON seed does
            // not know (fable-5, opus-5, provider onboarding rows); deleting
            // here erased three of them when their stale config overrides were
            // cleaned up (2026-08-10) and the loss surfaced two layers away as
            // a roster seat failing missing_fact. An override row's removal
            // withdraws the OVERRIDE, not the model's existence; rows that
            // should leave the catalog leave by migration.
            None => store
                .demote_fact_source_to_seed(&route_key)
                .map_err(|e| format!("demoting stale override {route_key}: {e}"))?,
        }
        outcome.reverted += 1;
    }

    Ok(outcome)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parses_provider_model_and_variant_keys() {
        assert_eq!(
            parse_model_key("openai/gpt-5.5", None),
            Some(("openai", "gpt-5.5", None))
        );
        assert_eq!(
            parse_model_key("anthropic/claude/high", None),
            Some(("anthropic", "claude", Some("high")))
        );
        assert_eq!(
            parse_model_key("a/b/c/d", None),
            Some(("a", "b/c", Some("d")))
        );
        for invalid in ["bare", "/m", "p/", "p/m/", "p/m//x", "p/m/ /x"] {
            assert_eq!(parse_model_key(invalid, None), None, "{invalid:?}");
        }
    }

    #[test]
    fn model_overrides_preserve_catalog_family_and_refuse_family_writes() {
        let base = ModelFact {
            billing_plane: None,
            auth_methods: None,
            login_methods_documented: false,
            speed_is_fleet_median: false,
            decommissioned: false,
            provider_id: "p".into(),
            model_id: "m".into(),
            variant: None,
            family: Some("original".into()),
            scores: ModelScores {
                elo: 50.0,
                eq: 1700.0,
                speed: 90.0,
            },
            context_window: Some(100.0),
            max_output_tokens: None,
            cost_tier: Some(2.0),
            cost_per_m_tok_out: Some(4.0),
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: Some(false),
            supports_tool_use: Some(true),
            prompt_defaults: None,
            confidence: Confidence::Unknown,
            notes: None,
            speed_multiplier: None,
            subscription_multiplier: None,
            score_provenance: ScoreProvenance::Curated,
            aa_index: None,
            review_by_ms: None,
            eq_source: None,
            class_vector: None,
            class_vector_is_overlay: false,
            upstream_status: None,
            model_type: None,
            model_kind: None,
            mode_of: None,
        };
        let ov = ModelFactOverride {
            vision: Some(true),
            ..Default::default()
        };
        let merged = merge_override(&base, &ov);
        assert_eq!(merged.family.as_deref(), Some("original"));
        assert_eq!(merged.scores.elo, 50.0);
        assert_eq!(merged.scores.eq, 1700.0); // preserved
        assert_eq!(merged.scores.speed, 90.0); // preserved
        assert_eq!(merged.supports_vision, Some(true)); // overridden
        assert_eq!(merged.cost_per_m_tok_out, Some(4.0)); // preserved (not in override)
        assert_eq!(merged.confidence, Confidence::Curated); // override forces curated

        let absent: ModelFactOverride = serde_json::from_str("{}").unwrap();
        assert_eq!(
            merge_override(&base, &absent).family.as_deref(),
            Some("original")
        );
        assert!(serde_json::from_str::<ModelFactOverride>(r#"{"family":null}"#).is_err());
        assert!(serde_json::from_str::<ModelFactOverride>(r#"{"family":"gpt"}"#).is_err());
    }

    fn family_rule_rig(
        label: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        striatum_store::SqliteRouteStateStore,
    ) {
        use striatum_store::{dev_descriptor, SqliteRouteStateStore};
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = cortexkit_test_support::scratch_root().join(format!(
            "striatum-family-rules-{label}-{}-{timestamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let db_path = root.join("store.sqlite");
        let config_path = root.join("striatum.jsonc");
        let descriptor = dev_descriptor(label, db_path.to_string_lossy().into_owned());
        (
            root,
            config_path,
            SqliteRouteStateStore::open(&descriptor).unwrap(),
        )
    }

    #[test]
    fn withdrawing_vision_restores_seed_and_clears_provenance_while_eq_remains() {
        let (root, config_path, store) = family_rule_rig("withdraw-vision");
        let route_key = "anthropic/claude-opus-4-5";
        let seed = model_facts_seed()
            .iter()
            .find(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    == route_key
            })
            .unwrap();
        assert_eq!(seed.supports_vision, Some(true));

        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"vision":false,"eq":1600}}}}"#,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();
        let first = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        assert_eq!(first.supports_vision, Some(false));
        assert_eq!(first.scores.eq, 1600.0);

        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"eq":1600}}}}"#,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        let second = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        assert_eq!(second.supports_vision, Some(true));
        assert_eq!(second.scores.eq, 1600.0);
        let connection = rusqlite::Connection::open(root.join("store.sqlite")).unwrap();
        let source: Option<String> = connection
            .query_row(
                "SELECT supports_vision_source FROM model_facts WHERE route_key = ?1",
                [route_key],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(source.as_deref(), Some("override"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn upsert_placed_score_survives_config_entry_that_does_not_name_it() {
        let (root, config_path, store) = family_rule_rig("preserve-upsert-score");
        let route_key = "anthropic/claude-opus-4-5";
        let mut placed = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        placed.scores.elo = 1900.0;
        store.upsert_fact(&placed, FactSource::Override).unwrap();
        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"cost_per_mtok_out":1}}}}"#,
        )
        .unwrap();

        apply_overrides_from_config(&store, &config_path).unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        assert_eq!(
            store
                .get_fact_by_route_key(route_key)
                .unwrap()
                .unwrap()
                .scores
                .elo,
            1900.0
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// The invariant that matters in production: a config entry that names only a
    /// score must not touch catalog fields the operator never stamped. Fusiform serves
    /// a context window that differs from the seed; reconciliation on a boot whose
    /// config does not name the field must leave the served value in place, because
    /// only a withdrawn operator stamp authorizes a restore.
    #[test]
    fn unstamped_catalog_field_keeps_its_served_value_across_reconciliation() {
        let (root, config_path, store) = family_rule_rig("unstamped-served");
        let route_key = "anthropic/claude-opus-4-5";
        let seed_context = model_facts_seed()
            .iter()
            .find(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    == route_key
            })
            .unwrap()
            .context_window;
        let served_context = seed_context.map_or(Some(777_777.0), |v| Some(v + 111_111.0));
        let mut served = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        served.context_window = served_context;
        store.upsert_fact(&served, FactSource::Seed).unwrap();

        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"eq":1600}}}}"#,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        let fact = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        assert_eq!(fact.context_window, served_context);
        assert_eq!(
            store
                .clear_operator_fact_fields(route_key, &[CatalogFactField::ContextWindow])
                .unwrap(),
            Vec::<CatalogFactField>::new(),
            "an unstamped field reports nothing cleared"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reconciliation_preserves_a_catalog_field_still_named_by_override() {
        let (root, config_path, store) = family_rule_rig("preserve-named-field");
        let route_key = "anthropic/claude-opus-4-5";
        let seed_cost = model_facts_seed()
            .iter()
            .find(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    == route_key
            })
            .unwrap()
            .cost_per_m_tok_out;
        let mut unrelated = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        unrelated.supports_tool_use = Some(false);
        store.upsert_fact(&unrelated, FactSource::Override).unwrap();
        store
            .mark_operator_fact_fields(route_key, &[CatalogFactField::SupportsToolUse])
            .unwrap();

        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"vision":false,"cost_per_mtok_out":1}}}}"#,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();
        crate::test_config::write(
            &config_path,
            r#"{"model_routing":{"models":{"anthropic/claude-opus-4-5":{"vision":false}}}}"#,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        let fact = store.get_fact_by_route_key(route_key).unwrap().unwrap();
        assert_eq!(fact.supports_vision, Some(false));
        assert_eq!(fact.cost_per_m_tok_out, seed_cost);
        assert_eq!(fact.supports_tool_use, Some(false));
        let connection = rusqlite::Connection::open(root.join("store.sqlite")).unwrap();
        let sources: (Option<String>, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT supports_vision_source, cost_per_mtok_out_source,
                        supports_tool_use_source
                 FROM model_facts WHERE route_key = ?1",
                [route_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(sources.0.as_deref(), Some("override"));
        assert_ne!(sources.1.as_deref(), Some("override"));
        assert_eq!(sources.2.as_deref(), Some("override"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unreadable_override_base_is_not_replaced_from_defaults() {
        use striatum_store::{dev_descriptor, SqliteRouteStateStore};
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-override-read-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            db_path.to_string_lossy().into_owned(),
        ))
        .unwrap();
        let route_key = make_route_key(
            &model_facts_seed()[0].provider_id,
            &model_facts_seed()[0].model_id,
            model_facts_seed()[0].variant.as_deref(),
        );
        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .execute(
                "UPDATE model_facts SET context_window = 'bad', fusiform_present = 0 WHERE route_key = ?1",
                [&route_key],
            )
            .unwrap();
        assert!(store.get_fact_by_route_key(&route_key).is_err());
        let config_path = dir.join("routing.jsonc");
        crate::test_config::write(
            &config_path,
            serde_json::json!({
                "model_routing": {
                    "models": { route_key.clone(): { "output_tokens_per_second": 1200.0 } }
                }
            })
            .to_string(),
        )
        .unwrap();
        let error = apply_overrides_from_config(&store, &config_path).unwrap_err();
        assert!(error.contains("reading override base"), "{error}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn unreadable_stale_override_base_is_not_reverted_from_seed() {
        use striatum_store::{dev_descriptor, SqliteRouteStateStore};
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-stale-override-read-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            db_path.to_string_lossy().into_owned(),
        ))
        .unwrap();
        let seed = model_facts_seed()
            .iter()
            .find(|fact| fact.max_output_tokens.is_none())
            .expect("the seed has a fact without max output metadata");
        let route_key = make_route_key(&seed.provider_id, &seed.model_id, seed.variant.as_deref());
        let config_path = dir.join("routing.jsonc");
        crate::test_config::write(
            &config_path,
            serde_json::json!({
                "model_routing": {
                    "models": { route_key.clone(): { "output_tokens_per_second": 1200.0 } }
                }
            })
            .to_string(),
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();
        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .execute(
                "UPDATE model_facts SET context_window = 'bad', fusiform_present = 0 WHERE route_key = ?1",
                [&route_key],
            )
            .unwrap();
        assert!(store.get_fact_by_route_key(&route_key).is_err());
        crate::test_config::write(&config_path, r#"{ "model_routing": { "models": {} } }"#)
            .unwrap();
        let error = apply_overrides_from_config(&store, &config_path).unwrap_err();
        assert!(error.contains("reading stale override base"), "{error}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn removing_an_override_demotes_the_row_and_never_deletes_it() {
        use striatum_store::{dev_descriptor, SqliteRouteStateStore};
        // A migration-seeded fact (absent from the compiled JSON seed) whose
        // config override disappears must SURVIVE as source='seed'. The delete
        // arm erased fable-5/opus-5/opus-5-fast on 2026-08-10 when their stale
        // pre-Elo overrides were cleaned from the live config, and the loss
        // surfaced two layers away as a roster seat failing missing_fact.
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-demote-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let descriptor = dev_descriptor("striatum", db_path.to_string_lossy().into_owned());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open test store");

        // Round 1: config carries an override for a MIGRATION-ONLY model
        // (opus-5 is in the store via migration 018/024, not the JSON seed).
        let config_path = dir.join("routing.jsonc");
        crate::test_config::write(
            &config_path,
            r##"{ "model_routing": { "models": {
                "anthropic/claude-opus-5": { "speed_multiplier": 0.9 }
            } } }"##,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        // Round 2: the override is removed from config entirely.
        crate::test_config::write(&config_path, r#"{ "model_routing": { "models": {} } }"#)
            .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        // The fact must still exist, demoted to seed, not deleted.
        let fact = striatum_core::route_state_store::RouteStateStore::get_fact_by_route_key(
            &store,
            "anthropic/claude-opus-5",
        )
        .unwrap();
        assert!(
            fact.is_some(),
            "removing an override must never delete a migration-seeded fact"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validates_min_best_effort_adequacy_clamp_and_nan() {
        // Valid values pass through.
        assert!((super::validate_min_best_effort_adequacy(0.5).unwrap() - 0.5).abs() < 1e-9);
        // Values > 1 clamp to 1.
        assert!((super::validate_min_best_effort_adequacy(1.5).unwrap() - 1.0).abs() < 1e-9);
        // Values < 0 clamp to 0.
        assert!((super::validate_min_best_effort_adequacy(-0.1).unwrap() - 0.0).abs() < 1e-9);
        // NaN is rejected loudly.
        let err = super::validate_min_best_effort_adequacy(f64::NAN).unwrap_err();
        assert!(err.contains("NaN"), "expected NaN rejection, got: {err}");
        // Edge values: 0 and 1 pass through.
        assert!((super::validate_min_best_effort_adequacy(0.0).unwrap() - 0.0).abs() < 1e-9);
        assert!((super::validate_min_best_effort_adequacy(1.0).unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn consecutive_terminal_failure_threshold_is_configurable_and_rejects_zero() {
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-failure-threshold-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("routing.jsonc");
        crate::test_config::write(
            &config_path,
            r#"{ "model_routing": { "consecutive_terminal_failure_threshold": 5 } }"#,
        )
        .unwrap();
        assert_eq!(
            super::read_failure_backoff_config(&config_path)
                .unwrap()
                .consecutive_terminal_failure_threshold,
            5
        );

        crate::test_config::write(
            &config_path,
            r#"{ "model_routing": { "consecutive_terminal_failure_threshold": 0 } }"#,
        )
        .unwrap();
        assert!(super::read_failure_backoff_config(&config_path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_min_best_effort_adequacy_from_config() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-min-band-read-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // Missing file → None.
        let missing_path = dir.join("nonexistent.jsonc");
        assert_eq!(
            super::read_min_best_effort_adequacy(&missing_path).unwrap(),
            None
        );

        // File with the key → returns clamped value.
        let config_path = dir.join("striatum.jsonc");
        crate::test_config::write(
            &config_path,
            r#"{ "model_routing": { "min_best_effort_adequacy": 0.35 } }"#,
        )
        .unwrap();
        let val = super::read_min_best_effort_adequacy(&config_path).unwrap();
        assert_eq!(val, Some(0.35));

        // File without the key → None.
        let config_path2 = dir.join("striatum-empty.jsonc");
        crate::test_config::write(&config_path2, r#"{ "model_routing": { "models": {} } }"#)
            .unwrap();
        assert_eq!(
            super::read_min_best_effort_adequacy(&config_path2).unwrap(),
            None
        );

        // File with NaN key → error.
        let config_path3 = dir.join("striatum-nan.jsonc");
        crate::test_config::write(
            &config_path3,
            r#"{ "model_routing": { "min_best_effort_adequacy": NaN } }"#,
        )
        .unwrap();
        assert!(super::read_min_best_effort_adequacy(&config_path3).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prompt_cache_provider_policy_defaults_and_reloads() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-cache-policy-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.jsonc");
        assert_eq!(
            read_providers_without_prompt_cache(&missing).unwrap(),
            Vec::<String>::new()
        );

        let path = dir.join("striatum.jsonc");
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["ollama-cloud"," custom ","ollama-cloud"]}}"#,
        )
        .unwrap();
        assert_eq!(
            read_providers_without_prompt_cache(&path).unwrap(),
            vec!["custom", "ollama-cloud"]
        );
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["cacheless-two"]}}"#,
        )
        .unwrap();
        assert_eq!(
            read_providers_without_prompt_cache(&path).unwrap(),
            vec!["cacheless-two"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prompt_cache_provider_policy_caches_by_mtime_and_drops_bad_live_edits() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::{Duration, UNIX_EPOCH};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-cache-policy-mtime-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("striatum.jsonc");
        let set_mtime = |seconds| {
            let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
            file.set_times(
                std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
            )
            .unwrap();
        };
        let reads = AtomicUsize::new(0);
        let read_file = |path: &std::path::Path| {
            reads.fetch_add(1, Ordering::Relaxed);
            std::fs::read_to_string(path).map_err(|error| error.to_string())
        };

        crate::test_config::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["first"]}}"#,
        )
        .unwrap();
        set_mtime(1_700_000_000);
        assert_eq!(
            read_providers_without_prompt_cache_with_reader(&path, read_file).unwrap(),
            vec!["first"]
        );
        assert_eq!(
            read_providers_without_prompt_cache_with_reader(&path, read_file).unwrap(),
            vec!["first"]
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            1,
            "unchanged mtime reused cache"
        );

        crate::test_config::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["second"]}}"#,
        )
        .unwrap();
        set_mtime(1_700_000_001);
        assert_eq!(
            read_providers_without_prompt_cache_with_reader(&path, read_file).unwrap(),
            vec!["second"]
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            2,
            "changed mtime reparsed config"
        );

        crate::test_config::write(&path, "{ not jsonc").unwrap();
        set_mtime(1_700_000_002);
        assert!(read_providers_without_prompt_cache_with_reader(&path, read_file).is_err());
        assert_eq!(reads.load(Ordering::Relaxed), 3);

        crate::test_config::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["repaired"]}}"#,
        )
        .unwrap();
        set_mtime(1_700_000_003);
        assert_eq!(
            read_providers_without_prompt_cache_with_reader(&path, read_file).unwrap(),
            vec!["repaired"]
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            4,
            "parse failure did not serve stale policy"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn operator_routing_policy_defaults_to_empty_and_reloads_on_edit() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::{Duration, UNIX_EPOCH};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-operator-policy-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // A missing file is an empty policy: the operator excluded nothing.
        let missing = dir.join("missing.jsonc");
        assert_eq!(
            read_operator_routing_policy(&missing),
            ModelRoutingPolicyInput::default()
        );

        let path = dir.join("striatum.jsonc");
        let set_mtime = |seconds| {
            let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
            file.set_times(
                std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
            )
            .unwrap();
        };
        let reads = AtomicUsize::new(0);
        let read_file = |path: &std::path::Path| {
            reads.fetch_add(1, Ordering::Relaxed);
            std::fs::read_to_string(path).map_err(|error| error.to_string())
        };

        // Comments and trailing commas: the operator's real file is JSONC.
        crate::test_config::write(
            &path,
            r#"{
              // routing fence
              "model_routing": {
                "exclude": {"all": ["anthropic", " ollama-cloud/glm-5.1 ", ""]},
                "subscription": ["kimi-for-coding"],
              },
            }"#,
        )
        .unwrap();
        set_mtime(1_700_000_000);
        let first = read_operator_routing_policy_with_reader(&path, read_file);
        assert_eq!(first.exclude, vec!["anthropic", "ollama-cloud/glm-5.1"]);
        assert_eq!(
            first.subscription,
            vec!["kimi-for-coding", "ollama-cloud", "opencode-go"]
        );
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file),
            first
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            1,
            "unchanged mtime reused the cached policy"
        );

        // A live edit takes effect on the next read with no restart.
        crate::test_config::write(
            &path,
            r#"{ "model_routing": { "exclude": {"all": ["xai"]} } }"#,
        )
        .unwrap();
        set_mtime(1_700_000_001);
        let reloaded = read_operator_routing_policy_with_reader(&path, read_file);
        assert_eq!(reloaded.exclude, vec!["xai"]);
        assert_eq!(reads.load(Ordering::Relaxed), 2, "changed mtime reparsed");

        // An absent model_routing block is an empty policy, not a parse failure.
        crate::test_config::write(&path, r#"{}"#).unwrap();
        set_mtime(1_700_000_002);
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file),
            ModelRoutingPolicyInput::default()
        );

        // Two policies differ by revision; the same policy hashes the same.
        assert_ne!(
            operator_policy_revision(&first),
            operator_policy_revision(&reloaded)
        );
        assert_eq!(
            operator_policy_revision(&first),
            operator_policy_revision(&first.clone())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_operator_config_keeps_the_last_good_policy_and_reports_once() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::{Duration, UNIX_EPOCH};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-operator-policy-broken-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("striatum.jsonc");
        let set_mtime = |seconds| {
            let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
            file.set_times(
                std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
            )
            .unwrap();
        };
        let reads = AtomicUsize::new(0);
        let read_file = |path: &std::path::Path| {
            reads.fetch_add(1, Ordering::Relaxed);
            std::fs::read_to_string(path).map_err(|error| error.to_string())
        };

        crate::test_config::write(
            &path,
            r#"{ "model_routing": { "exclude": {"all": ["anthropic"]} } }"#,
        )
        .unwrap();
        set_mtime(1_700_000_000);
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["anthropic"]
        );

        // Half-finished edit. The fence must NOT drop to empty: routing would
        // immediately start selecting the very routes the operator excluded.
        crate::test_config::write(&path, "{ \"model_routing\": { \"exclude\": [").unwrap();
        set_mtime(1_700_000_001);
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["anthropic"],
            "a malformed file keeps the last policy that parsed"
        );
        assert_eq!(reads.load(Ordering::Relaxed), 2);

        // Still broken and unedited: no re-read, so no repeated report either.
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["anthropic"]
        );
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["anthropic"]
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            2,
            "a broken file is reported once per edit, not once per selection"
        );

        // Repairing it is an edit, so the new policy is picked up.
        crate::test_config::write(
            &path,
            r#"{ "model_routing": { "exclude": {"all": ["xai"]} } }"#,
        )
        .unwrap();
        set_mtime(1_700_000_002);
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["xai"]
        );
        assert_eq!(reads.load(Ordering::Relaxed), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn elo_overrides_and_demand_exponent_live_reload_and_warn_on_unknown_keys() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-elo-live-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("striatum.jsonc");
        let mut facts = model_facts_seed().to_vec();
        let sol = |facts: &[ModelFact]| {
            facts
                .iter()
                .find(|fact| fact.provider_id == "openai" && fact.model_id == "gpt-5.6-sol")
                .unwrap()
                .scores
                .elo
        };

        crate::test_config::write(
            &path,
            r#"{"model_routing":{"demand_exponent":0.4,"elo_overrides":{"openai/gpt-5.6-sol":1800,"typo/missing":9999}}}"#,
        )
        .unwrap();
        let first = apply_live_selection_config(&path, &mut facts).unwrap();
        assert_eq!(first.routing_config.demand_exponent, 0.4);
        assert_eq!(first.applied, 1);
        assert_eq!(first.unknown_keys, vec!["typo/missing"]);
        assert_eq!(sol(&facts), 1_800.0);

        facts = model_facts_seed().to_vec();
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"demand_exponent":0.25,"elo_overrides":{"openai/gpt-5.6-sol":1750}}}"#,
        )
        .unwrap();
        let second = apply_live_selection_config(&path, &mut facts).unwrap();
        assert_eq!(second.routing_config.demand_exponent, 0.25);
        assert_eq!(second.applied, 1);
        assert!(second.unknown_keys.is_empty());
        assert_eq!(sol(&facts), 1_750.0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod overqualification_config_tests {
    use super::*;

    fn apply(document: Option<&str>) -> Result<RoutingConfig, String> {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-overqualification-{}-{}.jsonc",
            std::process::id(),
            document.map(|d| d.len()).unwrap_or(0)
        ));
        let _ = std::fs::remove_file(&path);
        if let Some(document) = document {
            crate::test_config::write(&path, document).unwrap();
        }
        let mut facts = Vec::new();
        let result = apply_live_selection_config(&path, &mut facts).map(|o| o.routing_config);
        let _ = std::fs::remove_file(&path);
        result
    }

    #[test]
    fn overqualification_defaults_apply_when_the_file_or_the_section_is_absent() {
        assert_eq!(
            apply(None).unwrap().overqualification,
            DEFAULT_OVERQUALIFICATION
        );
        assert_eq!(
            apply(Some(r#"{ "model_routing": { "demand_exponent": 0.35 } }"#))
                .unwrap()
                .overqualification,
            DEFAULT_OVERQUALIFICATION
        );
        assert_eq!(DEFAULT_OVERQUALIFICATION.threshold, 40.0);
        assert_eq!(DEFAULT_OVERQUALIFICATION.doubling, 10.0);
        assert_eq!(DEFAULT_OVERQUALIFICATION.base, 0.05);
        assert_eq!(
            DEFAULT_OVERQUALIFICATION.mode,
            striatum_core::routing_config::OverqualificationMode::Additive
        );
    }

    #[test]
    fn overqualification_fields_override_one_at_a_time_and_bad_values_are_refused() {
        let config = apply(Some(
            r#"{ "model_routing": { "overqualification": { "threshold": 30, "base": 0.1 } } }"#,
        ))
        .unwrap();
        assert_eq!(config.overqualification.threshold, 30.0);
        assert_eq!(config.overqualification.doubling, 10.0);
        assert_eq!(config.overqualification.base, 0.1);

        let off = apply(Some(
            r#"{ "model_routing": { "overqualification": { "base": 0 } } }"#,
        ))
        .unwrap();
        assert_eq!(off.overqualification.term(70.0), 0.0);

        let error = apply(Some(
            r#"{ "model_routing": { "overqualification": { "doubling": 0 } } }"#,
        ))
        .unwrap_err();
        assert!(error.contains("model_routing.overqualification"), "{error}");
        assert!(apply(Some(
            r#"{ "model_routing": { "overqualification": { "base": -1 } } }"#
        ))
        .is_err());
        assert!(
            apply(Some(
                r#"{ "model_routing": { "overqualification": { "treshold": 30 } } }"#
            ))
            .is_err(),
            "a misspelt field is refused, not silently ignored"
        );
    }

    #[test]
    fn overqualification_mode_defaults_to_additive_and_cost_scaled_refuses_tuning() {
        let config = parse_config(r#"{"overqualification":{"mode":"cost_scaled"}}"#).unwrap();
        assert_eq!(
            config
                .model_routing
                .overqualification
                .resolve()
                .unwrap()
                .mode,
            striatum_core::routing_config::OverqualificationMode::CostScaled
        );
        for field in ["threshold", "doubling", "base"] {
            let document = serde_json::json!({"overqualification":{"mode":"cost_scaled",field:10}})
                .to_string();
            let error = parse_config(&document).unwrap_err();
            assert!(error.contains("allowed only in additive mode"), "{error}");
        }
        assert!(parse_config(r#"{"overqualification":{"mode":"other"}}"#).is_err());
    }
}

#[cfg(test)]
mod catalog_poll_tests {
    use super::*;

    #[test]
    fn catalog_poll_ms_defaults_reads_and_rejects_zero() {
        let missing = cortexkit_test_support::scratch_root().join(format!(
            "striatum-missing-catalog-poll-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&missing);
        assert_eq!(
            read_catalog_poll_ms(&missing).unwrap(),
            DEFAULT_CATALOG_POLL_MS
        );

        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-catalog-poll-{}.jsonc",
            std::process::id()
        ));
        crate::test_config::write(&path, r#"{ "catalog": { "poll_ms": 1234 } }"#).unwrap();
        assert_eq!(read_catalog_poll_ms(&path).unwrap(), 1234);
        crate::test_config::write(&path, r#"{ "catalog": { "poll_ms": 0 } }"#).unwrap();
        assert!(read_catalog_poll_ms(&path)
            .unwrap_err()
            .contains("greater than zero"));
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod route_pricing_tests {
    use super::*;

    #[test]
    fn conflicting_route_multipliers_warn_and_cannot_reappear_in_overlays() {
        let mut warnings = Vec::new();
        let config = parse_config_with_reporter(
            r#"{
                "providers":{"openai":{"subscription_multiplier":40}},
                "subscription_multipliers":{"openai/sol":9,"openai/luna/high":0},
                "models":{
                    "openai/sol":{"subscription_multiplier":5},
                    "openai/luna":{"subscription_multiplier":80}
                }
            }"#,
            |warning| warnings.push(warning.to_owned()),
        )
        .unwrap();
        assert_eq!(warnings.len(), 4);
        for key in [
            "models.openai/sol.subscription_multiplier",
            "models.openai/luna.subscription_multiplier",
            "subscription_multipliers.openai/sol",
            "subscription_multipliers.openai/luna/high",
        ] {
            assert!(
                warnings.iter().any(|warning| warning.contains(key)),
                "{key}"
            );
        }
        let live = LiveConfig::new(config, None);
        assert_eq!(live.pricing().subscription_multipliers.len(), 1);
        let mut facts = vec![
            crate::tests::fact("openai", "sol"),
            crate::tests::fact("openai", "luna"),
        ];
        for fact in &mut facts {
            fact.subscription_multiplier = Some(999.0);
        }
        live.apply(&mut facts);
        live.apply(&mut facts);
        for fact in &facts {
            assert_eq!(fact.subscription_multiplier, Some(40.0));
            assert_eq!(
                live.pricing().subscription_multiplier(&make_route_key(
                    &fact.provider_id,
                    &fact.model_id,
                    fact.variant.as_deref()
                )),
                Some(40.0)
            );
        }
        let withdrawn = LiveConfig::new(RoutingConfigFile::default(), None);
        withdrawn.apply(&mut facts);
        assert!(facts
            .iter()
            .all(|fact| fact.subscription_multiplier.is_none()));
    }

    /// Pricing and reserve settings travel with the published document: a
    /// selection that holds one snapshot keeps its prices and reserves while
    /// later edits publish, are refused, or withdraw the file.
    #[test]
    fn published_pricing_belongs_to_its_snapshot_and_reloads_never_alter_it() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("routing-pricing-snapshot-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("routing.jsonc");
        crate::test_config::write(
            &path,
            r#"{"model_routing":{
                "subscription_multipliers":{"openai":40.0},
                "keep_free_percent":{"anthropic":20},
                "models":{"openai/special-route":{"subscription_multiplier":5.0,"speed_multiplier":0.5}}
            }}"#,
        )
        .unwrap();
        let held = settle_live_config(&path);
        let pricing = held.pricing();
        // Every model on a provider inherits the provider's quota value, including
        // one the config never names; a per-route entry cannot change that value.
        assert_eq!(
            pricing.subscription_multiplier("openai/brand-new-model"),
            Some(40.0)
        );
        assert_eq!(
            pricing.subscription_multiplier("openai/special-route"),
            Some(40.0)
        );
        assert_eq!(pricing.subscription_multiplier("xai/grok-5"), None);
        assert_eq!(pricing.speed_multiplier("openai/special-route"), Some(0.5));
        assert_eq!(pricing.speed_multiplier("openai/brand-new-model"), None);
        assert_eq!(pricing.keep_free_percent("anthropic"), 20.0);
        assert_eq!(pricing.keep_free_percent("unknown"), 0.0);

        // An invalid edit is refused whole, so the published pricing stays.
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"keep_free_percent":{"anthropic":101}}}"#,
        )
        .unwrap();
        assert!(Arc::ptr_eq(&held, &settle_live_config(&path)));

        // A valid edit publishes new pricing; the held snapshot is unchanged.
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"keep_free_percent":{"anthropic":0}}}"#,
        )
        .unwrap();
        let next = settle_live_config(&path);
        assert_eq!(next.pricing().keep_free_percent("anthropic"), 0.0);
        assert_eq!(next.pricing().subscription_multiplier("openai/x"), None);
        assert_eq!(held.pricing().keep_free_percent("anthropic"), 20.0);
        assert_eq!(
            held.pricing().subscription_multiplier("openai/x"),
            Some(40.0)
        );

        // Removal keeps the published pricing; only an explicit empty document
        // withdraws every multiplier and reserve.
        std::fs::remove_file(&path).unwrap();
        assert!(Arc::ptr_eq(&next, &settle_live_config(&path)));
        crate::test_config::write_raw(&path, "{}").unwrap();
        assert_eq!(
            settle_live_config(&path).pricing(),
            &RoutePricing::default()
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
