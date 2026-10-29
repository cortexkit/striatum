//! Validated, last-good snapshots of the routing-owned global configuration.
//! Each selection overlays one complete snapshot onto catalog facts; reloads
//! never rewrite the catalog or replace an in-flight request's configuration.

#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use crate::config_policy::{
    ConsumerPolicy, ExcludeGroups, PanelPolicies, Pins, ProviderPolicy, RunnerPolicy, VariantGroups,
};
use crate::substrate_policy::SubstrateGroups;
use prefrontal_routing_core::model_facts::{
    Confidence, ModelFact, ModelScores, RoutePricing, ScoreProvenance,
};
use prefrontal_routing_core::{
    make_capacity_key, make_route_key, FailureBackoffConfig, FamilySource, GrandFamily,
    ModelRoutingPolicyInput, OverqualificationConfig, RouteStateStore, RoutingConfig,
    DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD, DEFAULT_DEMAND_EXPONENT,
    DEFAULT_OVERQUALIFICATION,
};
#[cfg(test)]
use prefrontal_routing_core::{model_facts_seed, CatalogFactField, FactSource};
use prefrontal_routing_store::ReasoningCatalog;
use schemars::JsonSchema;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use subc_jsonc::jsonc_to_json;

const CONFIG_RELATIVE_PATH: &str = "cortexkit/prefrontal-routing.jsonc";
const OLD_CONFIG_BASENAME: &str = "alfonso-routing.jsonc";
pub const DEFAULT_CATALOG_POLL_MS: u64 = 10 * 60_000;

const RELOAD_DEBOUNCE: Duration = Duration::from_millis(250);
pub const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[cfg(test)]
mod contract_tests;
mod new_shape;
#[cfg(test)]
mod new_shape_tests;

/// A complete, validated document held for the lifetime of one selection. Model
/// overrides are overlays, not store writes: withdrawing one exposes stored facts
/// again, and a request cannot observe a partially applied edit.
#[derive(Debug, Clone, Default)]
pub struct LiveConfig {
    config: RoutingConfigFile,
    document: Option<String>,
    pricing: RoutePricing,
}

impl LiveConfig {
    fn new(config: RoutingConfigFile, document: Option<String>) -> Self {
        let pricing = route_pricing(&config.model_routing);
        Self {
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

type FileStamp = Option<(SystemTime, u64)>;

#[derive(Clone, Copy, PartialEq, Eq)]
struct ConfigStamp {
    current: FileStamp,
    old: FileStamp,
}

struct LiveConfigCache {
    stamp: ConfigStamp,
    pending: Option<(ConfigStamp, Instant)>,
    active: Arc<LiveConfig>,
}

fn live_configs() -> &'static Mutex<HashMap<PathBuf, LiveConfigCache>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, LiveConfigCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Reads the reasoning efforts the catalog currently publishes.
pub type ReasoningCatalogSource = Arc<dyn Fn() -> Result<ReasoningCatalog, String> + Send + Sync>;

/// The catalog each config path's variant overrides are checked against. Absent
/// until the routing store opens; config loaded before then gets only the shape
/// checks, and the store's opening re-checks the active config.
fn catalog_sources() -> &'static Mutex<HashMap<PathBuf, ReasoningCatalogSource>> {
    static SOURCES: OnceLock<Mutex<HashMap<PathBuf, ReasoningCatalogSource>>> = OnceLock::new();
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Attach the live catalog to `path`, then check the active config against it.
/// Every later load or reload of `path` is checked the same way. An error means
/// the active config names a variant the catalog does not publish.
pub fn attach_reasoning_catalog(path: &Path, source: ReasoningCatalogSource) -> Result<(), String> {
    catalog_sources()
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), source);
    let active = live_config_at(path, Instant::now())?;
    check_variants_against_catalog(path, &active.config)
}

/// The live catalog attached to `path`, read now. `None` before the store opens.
pub fn reasoning_catalog_for(path: &Path) -> Option<ReasoningCatalog> {
    let source = catalog_sources().lock().unwrap().get(path).cloned()?;
    match source() {
        Ok(catalog) => Some(catalog),
        Err(error) => {
            eprintln!(
                "[prefrontal-routing] reading reasoning options for {}: {error}",
                path.display()
            );
            None
        }
    }
}

fn check_variants_against_catalog(path: &Path, config: &RoutingConfigFile) -> Result<(), String> {
    let section = &config.model_routing;
    if section.variants.is_empty() {
        return Ok(());
    }
    let Some(source) = catalog_sources().lock().unwrap().get(path).cloned() else {
        return Ok(());
    };
    let catalog = source().map_err(|error| format!("variant_catalog_unreadable: {error}"))?;
    report_variant_warnings(path, check_variants_with(config, &catalog)?);
    Ok(())
}

/// Check one config's variant overrides against a catalog snapshot.
fn check_variants_with(
    config: &RoutingConfigFile,
    catalog: &ReasoningCatalog,
) -> Result<Vec<String>, String> {
    let section = &config.model_routing;
    let mut groups = serde_json::to_value(&section.variants).unwrap();
    let mut warnings = Vec::new();
    for entries in groups.as_object_mut().unwrap().values_mut() {
        entries.as_object_mut().unwrap().retain(|key, _| {
            let provider = key.split('/').next().unwrap_or(key);
            if catalog.models_of(provider).next().is_some() {
                true
            } else {
                warnings.push(format!(
                    "variants for {key} await the provider's credential-to-catalog resolution"
                ));
                false
            }
        });
    }
    let groups = serde_json::from_value(groups).unwrap();
    warnings.extend(
        crate::variant_policy::check_against_catalog(&groups, &BTreeMap::new(), catalog)
            .map_err(|error| format!("variant_not_published: {error}"))?,
    );
    Ok(warnings)
}

fn report_variant_warnings(path: &Path, warnings: Vec<String>) {
    for warning in warnings {
        eprintln!("[prefrontal-routing] {}: {warning}", path.display());
    }
}

/// Where the module's routing store lives when the daemon manages storage:
/// `$XDG_DATA_HOME/cortexkit/prefrontal-routing/store.db`, defaulting
/// `XDG_DATA_HOME` to `~/.local/share`.
pub fn default_store_path() -> Option<PathBuf> {
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local/share"))
        })?;
    Some(data_home.join("cortexkit/prefrontal-routing/store.db"))
}

/// Validate the config at `path` and check its variant overrides against the
/// store at `store_path`, read-only. Returns one line describing the catalog
/// check: passed, or skipped and why.
pub fn check_config_offline(path: &Path, store_path: Option<&Path>) -> Result<String, String> {
    initialize_config(path)?;
    let active = live_config_at(path, Instant::now())?;
    if active.config.model_routing.variants.is_empty() {
        return Ok("catalog check: no model_routing.variants to check".into());
    }
    let Some(store_path) = store_path else {
        return Ok(
            "catalog check skipped: no routing store path (neither XDG_DATA_HOME nor HOME is set)"
                .into(),
        );
    };
    let catalog = match prefrontal_routing_store::read_reasoning_catalog_offline(store_path) {
        Ok(catalog) => catalog,
        Err(error) => return Ok(format!("catalog check skipped: {error}")),
    };
    let warnings = check_variants_with(&active.config, &catalog)?;
    let mut report = format!("catalog check passed against {}", store_path.display());
    for warning in warnings {
        report.push_str(&format!("\nwarning: {warning}"));
    }
    Ok(report)
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
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some((
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            metadata.len(),
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("reading {}: {error}", path.display())),
    }
}

fn old_config_path(path: &Path) -> Option<PathBuf> {
    (path.file_name()? == "prefrontal-routing.jsonc")
        .then(|| path.with_file_name(OLD_CONFIG_BASENAME))
}

fn reject_old_config(path: &Path, stamp: ConfigStamp) -> Result<(), String> {
    if stamp.old.is_some() {
        let old = old_config_path(path).expect("old-name stamp has an old-name path");
        return Err(if stamp.current.is_some() {
            format!(
                "config_renamed: delete {}; routing reads {}",
                old.display(),
                path.display()
            )
        } else {
            format!(
                "config_renamed: move {} to {}",
                old.display(),
                path.display()
            )
        });
    }
    Ok(())
}

fn parse_config(document: &str) -> Result<RoutingConfigFile, String> {
    parse_config_with_reporter(document, |warning| {
        crate::warn_operator(format!("[prefrontal-routing] {warning}"));
    })
}

fn parse_config_with_reporter(
    document: &str,
    mut report: impl FnMut(&str),
) -> Result<RoutingConfigFile, String> {
    let mut json = jsonc_to_json(document)?;
    let mut value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    // Report the removed `aa` key with deletion guidance before checking unknown
    // fields, including when its value is null.
    if value.get("aa").is_some() {
        return Err("config_key_removed: aa; delete this key".into());
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
            // Preserve the raw root diversity rules: serializing them through
            // Value would sort an ordered model_families map alphabetically.
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
    // Parse the original family-rule text so first-match ordering is retained.
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
    config.prepare()?;
    validate_live_config(&config)?;
    Ok(config)
}

pub(crate) fn validate_document(document: &str) -> Result<(), String> {
    parse_config(document).map(|_| ())
}

fn live_config_at(path: &Path, now: Instant) -> Result<Arc<LiveConfig>, String> {
    live_config_at_with_reporter(path, now, |error| {
        eprintln!("[prefrontal-routing] routing-config-reload-rejected: {}: {error}; keeping last good config", path.display());
    })
}

fn live_config_at_with_reporter(
    path: &Path,
    now: Instant,
    mut report_rejection: impl FnMut(&str),
) -> Result<Arc<LiveConfig>, String> {
    let mut caches = live_configs().lock().unwrap();
    let stamp: Result<ConfigStamp, String> = (|| {
        Ok(ConfigStamp {
            current: file_stamp(path)?,
            old: old_config_path(path)
                .map(|old| file_stamp(&old))
                .transpose()?
                .flatten(),
        })
    })();
    let stamp = match stamp {
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
                cache.pending = Some((stamp, now));
                return Ok(Arc::clone(&cache.active));
            }
        }
    }
    let next = (|| {
        reject_old_config(path, stamp)?;
        let document = match std::fs::read_to_string(path) {
            Ok(document) => Some(document),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.to_string()),
        };
        let config = match &document {
            Some(document) => parse_config(document)
                .map_err(|error| format!("invalid config in {}: {error}", path.display()))?,
            None => RoutingConfigFile::default(),
        };
        check_variants_against_catalog(path, &config)?;
        Ok(LiveConfig::new(config, document))
    })();
    let active = match next {
        Ok(config) => Arc::new(config),
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
            stamp,
            pending: None,
            active: Arc::clone(&active),
        },
    );
    Ok(active)
}

fn validate_live_config(config: &RoutingConfigFile) -> Result<(), String> {
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
    if section
        .elo_overrides
        .values()
        .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err("elo_overrides must be positive".into());
    }
    section.panels.validate()?;
    section.pins.validate()?;
    section.variants.validate()?;
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
        if parse_model_key(key).is_none() {
            return Err(format!("invalid model key {key}"));
        }
        for value in [
            model.elo,
            model.eq,
            model.subscription_multiplier,
            model.context_window,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || value <= 0.0 {
                return Err(format!("invalid positive model override for {key}"));
            }
        }
        if model
            .speed
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
        let mut seen = std::collections::HashSet::new();
        for runner in runners {
            if !is_word(&runner.module) || !seen.insert(runner.module.as_str()) {
                return Err(format!(
                    "model_routing.runners: module {:?} must be one non-empty word, listed once",
                    runner.module
                ));
            }
            for spec in runner.routes.iter().flatten() {
                let parsed = prefrontal_routing_core::provider_model_spec::parse_spec(spec);
                if spec.trim() != spec || parsed.is_none_or(|parsed| parsed.negated) {
                    return Err(format!(
                        "model_routing.runners.{}: route {spec:?} must be provider, provider/model or provider/model/variant",
                        runner.module
                    ));
                }
            }
            if let Some(tag) = runner.tool_capabilities.iter().find(|tag| !is_word(tag)) {
                return Err(format!(
                    "model_routing.runners.{}: tool capability {tag:?} must be one non-empty word",
                    runner.module
                ));
            }
        }
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

/// The `routing/v1` consumer ids a routing section serves, in id order.
fn v1_consumer_ids(section: &ModelRoutingSection) -> Vec<String> {
    let mut ids: Vec<String> = match &section.consumers {
        Some(consumers) => consumers.clone(),
        None => crate::select_policy::Consumer::ALL
            .into_iter()
            .filter_map(|consumer| crate::variant_policy::consumer_group(Some(consumer)))
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

    pub fn variant_groups(&self) -> &VariantGroups {
        &self.config.model_routing.variants
    }

    pub(crate) fn substrate_groups(&self) -> &SubstrateGroups {
        &self.config.model_routing.substrates
    }

    pub fn panels(&self) -> &PanelPolicies {
        &self.config.model_routing.panels
    }

    pub fn policy_revision(&self) -> String {
        self.document
            .as_deref()
            .map(policy_revision)
            .unwrap_or_else(default_policy_revision)
    }

    pub fn operator_policy(&self) -> ModelRoutingPolicyInput {
        let section = &self.config.model_routing;
        ModelRoutingPolicyInput {
            exclude: clean_spec_entries(section.exclude.all.clone()),
            subscription: clean_spec_entries(section.subscription.clone()),
            score_alias: BTreeMap::new(),
        }
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
        self.config
            .providers
            .get(provider)
            .is_some_and(|policy| policy.accounts.metered_fallback)
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
                policy
                    .projects
                    .get(project_id)
                    .map(|project| (name.len(), project.pin.as_str()))
            })
            .max_by_key(|(length, _)| *length)
            .map(|(_, pin)| pin)
    }

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

    pub(crate) fn shadowed_operator_grades(
        &self,
        grades: &BTreeMap<String, prefrontal_routing_store::OperatorQuality>,
    ) -> Vec<String> {
        self.config
            .model_routing
            .elo_overrides
            .keys()
            .filter(|key| {
                let (provider, model, _) = parse_model_key(key).expect("validated model key");
                grades.contains_key(&format!("{provider}/{model}"))
            })
            .cloned()
            .collect()
    }

    pub fn apply(&self, facts: &mut Vec<ModelFact>) -> SelectionOverrideOutcome {
        let section = &self.config.model_routing;
        for (key, model) in &section.models {
            if !model.has_fact_override() {
                continue;
            }
            let (provider, model, variant) = parse_model_key(key).expect("validated model key");
            if !facts.iter().any(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()) == *key
            }) {
                facts.push(ModelFact {
                    provider_id: provider,
                    model_id: model,
                    variant,
                    family: None,
                    billing_plane: None,
                    auth_methods: None,
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
                    speed_is_fleet_median: false,
                    score_provenance: ScoreProvenance::Unscored,
                    aa_index: None,
                    review_by_ms: None,
                    eq_source: None,
                    class_vector: None,
                    class_vector_is_overlay: false,
                    upstream_status: None,
                });
            }
        }
        for fact in facts.iter_mut() {
            let key = make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
            if let Some(model) = section.models.get(&key) {
                let observed_speed = fact.scores.speed;
                let quality = (fact.scores.elo, fact.scores.eq, fact.score_provenance);
                let operator_grade = fact.eq_source.as_deref() == Some("operator_grade");
                *fact = merge_override(fact, model);
                if operator_grade {
                    (fact.scores.elo, fact.scores.eq, fact.score_provenance) = quality;
                } else {
                    if model.elo.is_some()
                        && (quality.2 != ScoreProvenance::Unscored || model.eq.is_some())
                    {
                        fact.score_provenance = ScoreProvenance::EloOverride;
                    }
                    if model.eq.is_some() {
                        fact.eq_source = Some("override".into());
                    }
                }
                if let (Some(cost), Some(plane)) =
                    (model.cost_per_mtok_out, fact.billing_plane.as_mut())
                {
                    plane.rates.insert(
                        "rate.output".into(),
                        prefrontal_routing_core::billing::BillingRate::Priced { rate: cost },
                    );
                }
                if let Some(speed) = model.speed {
                    fact.scores.speed = speed * model.speed_multiplier.unwrap_or(1.0);
                } else {
                    fact.scores.speed = observed_speed;
                }
            }
            if let Some(elo) = section.elo_overrides.get(&key) {
                fact.scores.elo = *elo;
                if fact.score_provenance != ScoreProvenance::Unscored {
                    fact.score_provenance = ScoreProvenance::EloOverride;
                }
            }
            fact.subscription_multiplier = self
                .pricing
                .subscription_multipliers
                .get(&fact.provider_id)
                .copied();
        }
        SelectionOverrideOutcome {
            routing_config: RoutingConfig {
                demand_exponent: section.demand_exponent.unwrap_or(DEFAULT_DEMAND_EXPONENT),
                overqualification: section
                    .overqualification
                    .resolve()
                    .expect("validated config"),
            },
            applied: section.models.len() + section.elo_overrides.len(),
            unknown_keys: Vec::new(),
            policy_revision: self.policy_revision(),
        }
    }
}

pub async fn config_refresh_loop(path: PathBuf, catalog_wake: Arc<tokio::sync::Notify>) {
    let mut active = live_config(&path);
    loop {
        tokio::time::sleep(CONFIG_POLL_INTERVAL).await;
        let next = live_config(&path);
        if !Arc::ptr_eq(&active, &next) {
            eprintln!(
                "[prefrontal-routing] routing-config-reloaded: {}",
                path.display()
            );
            active = next;
            catalog_wake.notify_one();
        }
    }
}

#[cfg(test)]
mod live_snapshot_tests {
    use super::*;

    #[test]
    fn changed_config_is_debounced_and_in_flight_snapshot_stays_immutable() {
        let dir = prefrontal_test_support::scratch_root()
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
        let defaults = live_config_at(&path, now + RELOAD_DEBOUNCE * 2).unwrap();
        assert!(defaults.operator_policy().subscription.is_empty());
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

#[derive(Debug, Deserialize, Serialize, Default, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RoutingConfigFile {
    #[serde(flatten)]
    model_routing: ModelRoutingSection,
    #[serde(default)]
    providers: BTreeMap<String, ProviderPolicy>,
    #[serde(default, rename = "variants")]
    default_variants: BTreeMap<String, String>,
    #[serde(default)]
    consumers: BTreeMap<String, ConsumerPolicy>,
    #[serde(default)]
    runners: Vec<RunnerPolicy>,
    #[serde(default)]
    model_families: ModelFamilyRules,
    /// How providers spell model ids and which model families the capability
    /// heuristic recognizes; see `ModelNamingConfig`.
    #[serde(default)]
    model_naming: ModelNamingConfig,
    /// Target agents that are heads: operator-facing sessions that may spend
    /// a subscription's kept-free reserve. Matched case-insensitively.
    #[serde(default)]
    head_agents: Vec<String>,
    /// Quota pools that meter only some models: a routed model whose id
    /// starts with `model_prefix` draws on `pool` beside its family pool.
    #[serde(default)]
    quota_model_pools: Vec<QuotaModelPoolConfig>,
    #[serde(default)]
    catalog: CatalogSection,
    #[serde(default)]
    accounts: crate::config_policy::ProviderAccountsPolicy,
}

/// Regenerate with `cargo run -p prefrontal-routing-module --bin ck-prefrontal-routing -- --print-config-schema`.
pub fn routing_config_schema() -> String {
    serde_json::to_string_pretty(&schemars::schema_for!(RoutingConfigFile)).unwrap() + "\n"
}

fn allocator_policy_from(
    defaults: &crate::config_policy::ProviderAccountsPolicy,
    provider: Option<&crate::config_policy::ProviderAccountsPolicy>,
) -> Result<prefrontal_routing_core::accounts::AllocatorPolicy, String> {
    use prefrontal_routing_core::accounts::{
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
                        prefrontal_routing_core::accounts::validate_pool_name(pool)?;
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
            policy.short_hold_max_ms = i64::try_from(duration.seconds()?)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1000))
                .ok_or("hold_when_reset_within too large")?;
        }
        if let Some(duration) = &source.unknown_reset_retry {
            policy.unknown_reset_retry_ms = i64::try_from(duration.seconds()?)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1000))
                .ok_or("unknown_reset_retry too large")?;
        }
        if let Some(limit) = source.spend_budget {
            if !limit.is_finite() || limit <= 0.0 {
                return Err("spend_budget must be positive".into());
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
    pub(crate) fn quota_model_pools(
        &self,
    ) -> Vec<prefrontal_routing_core::quota_extract::ModelPool> {
        self.config
            .quota_model_pools
            .iter()
            .map(|entry| prefrontal_routing_core::quota_extract::ModelPool {
                model_prefix: entry.model_prefix.clone(),
                pool: entry.pool.clone(),
            })
            .collect()
    }

    /// Whether `agent` is a head under `head_agents`.
    pub(crate) fn is_head_agent(&self, agent: &str) -> bool {
        let agent = agent.trim();
        self.config
            .head_agents
            .iter()
            .any(|head| head.trim().eq_ignore_ascii_case(agent))
    }

    /// The configured model-id spelling and capability families.
    pub(crate) fn model_naming(&self) -> prefrontal_routing_core::ModelNaming {
        self.config.model_naming.naming()
    }

    pub(crate) fn allocator_policy(
        &self,
        provider: &str,
    ) -> prefrontal_routing_core::accounts::AllocatorPolicy {
        let mut policy = allocator_policy_from(
            &self.config.accounts,
            self.config
                .providers
                .get(provider)
                .map(|provider| &provider.accounts),
        )
        .expect("account policies are validated before publishing the config");
        policy.model_pools = self.quota_model_pools();
        policy
    }
}

#[cfg(test)]
mod account_policy_config_tests {
    use super::*;

    #[test]
    fn provider_account_policy_overrides_defaults_without_weakening_the_schema() {
        let config=parse_config(r#"{
            "accounts":{"max_hold":"90s","hold_when_reset_within":"15m"},
            "providers":{"synthetic":{"accounts":{"strategy":"hybrid","max_hold":"30s",
                "soft_quota_remaining_percent":10,"floors":[{"pool":"pool-a","window_minutes":300,"remaining_percent":5}],
                "spend_budget":20,"spend_budget_reset_at_ms":1800000600000,"paid_credits":true,"unknown_reset_retry":"2h"}}}
        }"#).unwrap();
        let policy = allocator_policy_from(
            &config.accounts,
            Some(&config.providers["synthetic"].accounts),
        )
        .unwrap();
        assert_eq!(
            policy.strategy,
            prefrontal_routing_core::accounts::AccountStrategy::Hybrid
        );
        assert_eq!(policy.max_hold_s, 30);
        assert_eq!(policy.short_hold_max_ms, 900_000);
        assert_eq!(policy.unknown_reset_retry_ms, 7_200_000);
        assert_eq!(policy.pool_floors[0].key.name.as_deref(), Some("pool-a"));
        assert_eq!(policy.spend_budget, Some(20.0));
        assert!(policy.paid_credits);
        for invalid in [
            r#"{"providers":{"p":{"accounts":{"strategy":"random"}}}}"#,
            r#"{"providers":{"p":{"accounts":{"max_hold":"tomorrow"}}}}"#,
            r#"{"providers":{"p":{"accounts":{"unknown_policy":true}}}}"#,
        ] {
            assert!(parse_config(invalid).is_err(), "{invalid}");
        }
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
    #[serde(default)]
    #[schemars(range(min = 1))]
    poll_ms: Option<u64>,
}

/// Operator config for `prefrontal_routing_core::ModelNaming`, the model-id
/// spelling `route.resolve_model` applies per provider and the model families
/// `route.model_capabilities` recognizes by name. The config converter writes
/// the shipped values from its data file.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModelNamingConfig {
    /// For a gateway serving other vendors' models as `vendor/model`: the
    /// vendor of a bare model id, by the id's prefix. The first match wins.
    #[serde(default)]
    vendor_prefixes: Vec<VendorPrefixConfig>,
    /// The rewrites each provider applies to a model id, in order.
    #[serde(default)]
    provider_rewrites: BTreeMap<String, Vec<ModelRewriteConfig>>,
    /// Families recognized by a substring of the normalized model id, each
    /// with the basic variants. The first match wins.
    #[serde(default)]
    basic_families: Vec<BasicFamilyConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct QuotaModelPoolConfig {
    model_prefix: String,
    pool: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VendorPrefixConfig {
    prefix: String,
    vendor: String,
}

/// `vendor_prefix` serves the id as `vendor/model`; `dot_version` spells a
/// dashed two-part Claude-style version with a dot; `{"preview_alias": a}`
/// expands the bare alias `a` to its `-preview` id.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ModelRewriteConfig {
    VendorPrefix,
    DotVersion,
    PreviewAlias(String),
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BasicFamilyConfig {
    family: String,
    needles: Vec<String>,
}

impl ModelNamingConfig {
    fn naming(&self) -> prefrontal_routing_core::ModelNaming {
        use prefrontal_routing_core::{BasicFamily, ModelNaming, ModelRewrite, VendorPrefix};
        ModelNaming {
            vendor_prefixes: self
                .vendor_prefixes
                .iter()
                .map(|entry| VendorPrefix {
                    prefix: entry.prefix.clone(),
                    vendor: entry.vendor.clone(),
                })
                .collect(),
            provider_rewrites: self
                .provider_rewrites
                .iter()
                .map(|(provider, rewrites)| {
                    let rewrites = rewrites
                        .iter()
                        .map(|rewrite| match rewrite {
                            ModelRewriteConfig::VendorPrefix => ModelRewrite::VendorPrefix,
                            ModelRewriteConfig::DotVersion => ModelRewrite::DotVersion,
                            ModelRewriteConfig::PreviewAlias(alias) => {
                                ModelRewrite::PreviewAlias(alias.clone())
                            }
                        })
                        .collect();
                    (provider.clone(), rewrites)
                })
                .collect(),
            basic_families: self
                .basic_families
                .iter()
                .map(|family| BasicFamily {
                    family: family.family.clone(),
                    needles: family.needles.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelFamilyRule {
    family: GrandFamily,
    globs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelFamilyRules(Vec<ModelFamilyRule>);

impl Serialize for ModelFamilyRules {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for rule in &self.0 {
            map.serialize_entry(&rule.family, &rule.globs)?;
        }
        map.end()
    }
}

impl JsonSchema for ModelFamilyRules {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelFamilyRules".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let globs =
            serde_json::json!({"type": "array", "items": {"type": "string", "pattern": "\\S"}});
        let families: Vec<&str> = GrandFamily::ALL
            .iter()
            .map(|family| family.as_str())
            .collect();
        let properties: serde_json::Map<String, serde_json::Value> = families
            .iter()
            .map(|family| ((*family).to_owned(), globs.clone()))
            .collect();
        schemars::json_schema!({"oneOf": [
            {"type": "object", "additionalProperties": false, "properties": properties},
            {"type": "array", "items": {
                "type": "object",
                "additionalProperties": false,
                "required": ["family", "globs"],
                "properties": {"family": {"enum": families}, "globs": globs}
            }}
        ]})
    }
}

/// No rules unless the operator config writes them. The converter writes the
/// shipped rules from its data file as an ordered list.
impl Default for ModelFamilyRules {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl ModelFamilyRules {
    fn rule(wire_family: String, globs: Vec<String>) -> Result<ModelFamilyRule, String> {
        let family = GrandFamily::from_wire(&wire_family)
            .ok_or_else(|| format!("unknown model_families key `{wire_family}`"))?;
        if globs.iter().any(|glob| glob.trim().is_empty()) {
            return Err(format!(
                "model_families.{wire_family} entries must be non-empty route globs"
            ));
        }
        Ok(ModelFamilyRule { family, globs })
    }
}

impl<'de> Deserialize<'de> for ModelFamilyRules {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RulesVisitor;

        impl<'de> Visitor<'de> for RulesVisitor {
            type Value = ModelFamilyRules;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "an ordered map of grand-family ids to route globs, or a list of \
                     {family, globs} rules",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut rules = Vec::new();
                while let Some((wire_family, globs)) = map.next_entry::<String, Vec<String>>()? {
                    rules.push(
                        ModelFamilyRules::rule(wire_family, globs)
                            .map_err(serde::de::Error::custom)?,
                    );
                }
                Ok(ModelFamilyRules(rules))
            }

            // The list form keeps first-match order through any JSON value
            // round trip, which sorts object keys.
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Entry {
                    family: String,
                    globs: Vec<String>,
                }
                let mut rules = Vec::new();
                while let Some(entry) = seq.next_element::<Entry>()? {
                    rules.push(
                        ModelFamilyRules::rule(entry.family, entry.globs)
                            .map_err(serde::de::Error::custom)?,
                    );
                }
                Ok(ModelFamilyRules(rules))
            }
        }

        deserializer.deserialize_any(RulesVisitor)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = routing_schema_constraints)]
struct ModelRoutingSection {
    /// Excluded providers or routes by consumer. The `all` list applies to every
    /// consumer; a negated entry in another list cannot remove its exclusions.
    /// Exclusion keeps a route out of automatic choice only: a request that names
    /// the model (a work run's `model=`, a synthesizer or planner pin) is still
    /// admitted when the route is otherwise servable.
    #[serde(skip)]
    exclude: ExcludeGroups,
    /// Reasoning-variant overrides by consumer, grouped like `exclude`. A
    /// selection runs at the caller's named variant, else `variants.<consumer>`,
    /// else `variants.all`, else the route's curated default. Each value must
    /// be an effort the catalog publishes for the matched models.
    #[serde(skip)]
    variants: VariantGroups,
    /// Route-to-harness mapping. Resolve the workload group, consumer group,
    /// then `all`, taking each group's longest matching route key or default.
    /// Routes not covered by any group run on Broca.
    #[serde(default)]
    substrates: SubstrateGroups,
    #[serde(default)]
    panels: PanelPolicies,
    #[serde(skip)]
    pins: Pins,
    /// Providers and models priced as subscription-backed in addition to auth facts.
    #[serde(skip)]
    subscription: Vec<String>,
    /// Keyed by "provider/model" or "provider/model/variant".
    #[serde(default)]
    models: HashMap<String, ModelFactOverride>,
    /// Request-time capability overrides. Unknown keys are ignored and surfaced
    /// to the caller for warning logs.
    #[serde(default)]
    elo_overrides: HashMap<String, f64>,
    /// Power-curve exponent mapping caller IQ demand to the request frontier.
    #[serde(default)]
    #[schemars(transform = positive_number)]
    demand_exponent: Option<f64>,
    /// Over-qualification term added to selection cost for routes far above the
    /// request's iq demand. Absent fields take the routing-core defaults.
    #[serde(default)]
    overqualification: OverqualificationSection,
    /// Minimum adequacy for best-effort selection (sanity band). Default 0.6.
    /// Clamped to [0, 1]; NaN is rejected loudly at parse time.
    #[serde(default)]
    min_best_effort_adequacy: Option<f64>,
    /// Terminal send failures required before the route-local safety backoff starts.
    #[serde(default)]
    #[schemars(range(min = 1))]
    consecutive_terminal_failure_threshold: Option<u32>,
    /// Providers that do not expose prompt-cache reads or writes. Gather seats
    /// avoid these providers when a cache-capable adequate route exists.
    #[serde(skip)]
    providers_without_prompt_cache: Vec<String>,
    /// Provider-wide subscription multipliers. Legacy route keys are accepted
    /// only to warn and discard them; model-level values never override these.
    #[serde(default, skip_serializing)]
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

fn routing_schema_constraints(schema: &mut schemars::Schema) {
    let properties = schema
        .get_mut("properties")
        .unwrap()
        .as_object_mut()
        .unwrap();
    properties["elo_overrides"]["additionalProperties"]["exclusiveMinimum"] = serde_json::json!(0);
}

fn positive_number(schema: &mut schemars::Schema) {
    schema.insert("exclusiveMinimum".into(), serde_json::json!(0));
}

impl Default for ModelRoutingSection {
    fn default() -> Self {
        serde_json::from_str("{}").expect("empty model routing section")
    }
}

/// `overqualification`: `term = 0` while a route's surplus over
/// the iq demand (both on the 0..100 demand scale) is at most `threshold`, else
/// `base * (2^((surplus - threshold) / doubling) - 1)`, added to selection cost.
/// `base: 0` switches the term off.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OverqualificationSection {
    #[schemars(range(min = 0))]
    threshold: Option<f64>,
    #[schemars(transform = positive_number)]
    doubling: Option<f64>,
    #[schemars(range(min = 0))]
    base: Option<f64>,
}

impl OverqualificationSection {
    fn resolve(self) -> Result<OverqualificationConfig, String> {
        let config = OverqualificationConfig {
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
    /// Outer None means absent; Some(None) means an explicit JSON null clears it.
    #[serde(default, deserialize_with = "deserialize_family_override")]
    family: Option<Option<String>>,
    #[schemars(transform = positive_number)]
    elo: Option<f64>,
    #[schemars(transform = positive_number)]
    eq: Option<f64>,
    #[schemars(range(min = 0))]
    speed: Option<f64>,
    #[schemars(range(min = 0))]
    cost_per_mtok_out: Option<f64>,
    #[schemars(transform = positive_number)]
    context_window: Option<f64>,
    vision: Option<bool>,
    #[schemars(range(max = 2), transform = positive_number)]
    speed_multiplier: Option<f64>,
    /// Retained for old operator files. Ignored with a warning at load because
    /// subscription value belongs to the provider, not an individual model.
    subscription_multiplier: Option<f64>,
    #[serde(flatten)]
    rules: crate::config_policy::EntityPolicy,
}

/// Resolve a path under the user's config home. Honors XDG_CONFIG_HOME, else
/// `$HOME/.config`, matching the daemon's own resolution.
fn config_home_path(relative: &str) -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg).join(relative);
    }
    if let Some(home) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(home).join(".config").join(relative);
    }
    PathBuf::from(".config").join(relative)
}

/// The config-home path for this module's own config file.
pub fn config_path() -> PathBuf {
    config_home_path(CONFIG_RELATIVE_PATH)
}

/// Parse `provider/model` or `provider/model/variant`.
fn parse_model_key(key: &str) -> Option<(String, String, Option<String>)> {
    let parts: Vec<&str> = key.trim().split('/').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }
    let provider = parts[0].trim();
    let model = parts[1].trim();
    if provider.is_empty() || model.is_empty() {
        return None;
    }
    let variant = parts
        .get(2)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());
    Some((provider.to_string(), model.to_string(), variant))
}

fn deserialize_family_override<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

fn merge_override(base: &ModelFact, ov: &ModelFactOverride) -> ModelFact {
    let mut fact = base.clone();
    if let Some(family) = &ov.family {
        fact.family = family.clone();
    }
    fact.scores = ModelScores {
        elo: ov.elo.unwrap_or(base.scores.elo),
        eq: ov.eq.unwrap_or(base.scores.eq),
        speed: ov.speed.unwrap_or(base.scores.speed),
    };
    if ov.cost_per_mtok_out.is_some() {
        fact.cost_per_m_tok_out = ov.cost_per_mtok_out;
    }
    if ov.context_window.is_some() {
        fact.context_window = ov.context_window;
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
    if ov.context_window.is_some() {
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
    apply_family_rules(store, &config.model_families, &config.model_routing.models)?;
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

/// The routing file as untyped JSON, for a reader that must not depend on
/// every other block parsing (a typo elsewhere in the file leaves it working).
/// A missing file is `Ok(None)`.
pub(crate) fn read_routing_config_value(path: &Path) -> Result<Option<serde_json::Value>, String> {
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

pub(crate) fn apply_family_rules_from_config<S: RouteStateStore>(
    store: &S,
    path: &Path,
) -> Result<usize, String> {
    let config = &live_config(path).config;
    apply_family_rules(store, &config.model_families, &config.model_routing.models)
}

fn glob_matches(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut previous = vec![false; candidate.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut next = vec![false; candidate.len() + 1];
        if *token == b'*' {
            next[0] = previous[0];
            for index in 1..=candidate.len() {
                next[index] = previous[index] || next[index - 1];
            }
        } else {
            for index in 1..=candidate.len() {
                next[index] =
                    previous[index - 1] && (*token == b'?' || *token == candidate[index - 1]);
            }
        }
        previous = next;
    }
    previous[candidate.len()]
}

fn matching_family(rules: &ModelFamilyRules, route_key: &str) -> Option<GrandFamily> {
    rules.0.iter().find_map(|rule| {
        rule.globs
            .iter()
            .any(|glob| glob_matches(glob, route_key))
            .then_some(rule.family)
    })
}

fn apply_family_rules<S: RouteStateStore>(
    store: &S,
    rules: &ModelFamilyRules,
    overrides: &HashMap<String, ModelFactOverride>,
) -> Result<usize, String> {
    let mut explicit = HashMap::new();
    for (key, model_override) in overrides {
        let Some(family) = &model_override.family else {
            continue;
        };
        let Some((provider_id, model_id, variant)) = parse_model_key(key) else {
            continue;
        };
        let family = family
            .as_deref()
            .map(|wire| {
                GrandFamily::from_wire(wire).ok_or_else(|| {
                    format!("model_routing.models.{key}.family has unknown value `{wire}`")
                })
            })
            .transpose()?;
        explicit.insert(
            make_route_key(&provider_id, &model_id, variant.as_deref()),
            family,
        );
    }

    let facts = store
        .get_facts()
        .map_err(|error| format!("listing model families: {error}"))?;
    let mut updated = 0;
    for fact in facts {
        let route_key = make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
        let (family, source) = if let Some(family) = explicit.get(&route_key) {
            (*family, FamilySource::Override)
        } else {
            let capacity_key = make_capacity_key(&fact.provider_id, &fact.model_id);
            if let Some(family) = matching_family(rules, &capacity_key) {
                (Some(family), FamilySource::Rule)
            } else {
                let current = store
                    .get_model_family(&route_key)
                    .map_err(|error| format!("reading model family {route_key}: {error}"))?;
                match current {
                    Some((_, FamilySource::Rule)) => (None, FamilySource::Absent),
                    Some((_, FamilySource::Override)) => {
                        let restored = fact.family.as_deref().and_then(GrandFamily::from_wire);
                        let source = if restored.is_some() {
                            FamilySource::Curated
                        } else {
                            FamilySource::Absent
                        };
                        (restored, source)
                    }
                    Some((family, source)) => (family, source),
                    None => continue,
                }
            }
        };
        store
            .set_model_family(&route_key, family, source)
            .map_err(|error| format!("writing model family {route_key}: {error}"))?;
        updated += 1;
    }
    Ok(updated)
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

/// Re-read request-time Elo settings and apply known route overrides to this
/// selection's fact snapshot. Nothing is persisted, so removing an override
/// restores the catalog Elo value on the next request.
#[cfg(test)]
pub fn apply_live_selection_config(
    path: &Path,
    facts: &mut [ModelFact],
) -> Result<SelectionOverrideOutcome, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(doc) => doc,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SelectionOverrideOutcome {
                routing_config: RoutingConfig::default(),
                applied: 0,
                unknown_keys: Vec::new(),
                policy_revision: default_policy_revision(),
            });
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&doc)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let exponent = config
        .model_routing
        .demand_exponent
        .unwrap_or(DEFAULT_DEMAND_EXPONENT);
    if !exponent.is_finite() || exponent <= 0.0 {
        return Err(format!(
            "model_routing.demand_exponent must be finite and positive, got {exponent}"
        ));
    }
    let overqualification = config.model_routing.overqualification.resolve()?;

    let mut fact_indices = facts
        .iter()
        .enumerate()
        .map(|(index, fact)| {
            (
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()),
                index,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut applied = 0;
    let mut unknown_keys = Vec::new();
    for (route_key, elo) in config.model_routing.elo_overrides {
        let Some(index) = fact_indices.remove(&route_key) else {
            eprintln!(
                "[prefrontal-routing] unknown elo_overrides route key `{route_key}`; ignoring"
            );
            unknown_keys.push(route_key);
            continue;
        };
        if !elo.is_finite() || elo <= 0.0 {
            eprintln!(
                "[prefrontal-routing] invalid Elo override for route `{route_key}`: {elo}; ignoring"
            );
            continue;
        }
        facts[index].scores.elo = elo;
        applied += 1;
    }
    unknown_keys.sort();

    Ok(SelectionOverrideOutcome {
        routing_config: RoutingConfig {
            demand_exponent: exponent,
            overqualification,
        },
        applied,
        unknown_keys,
        policy_revision: policy_revision(&doc),
    })
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
                "[prefrontal-routing] operator routing policy {} is unusable ({error}); keeping the last good policy ({} exclusion entries)",
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
        let Some((provider_id, model_id, variant)) = parse_model_key(key) else {
            outcome.skipped.push(key.clone());
            continue;
        };
        let route_key = make_route_key(&provider_id, &model_id, variant.as_deref());
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
                speed_is_fleet_median: false,
                provider_id,
                model_id,
                variant,
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
            });
        // Score provenance does not distinguish config from route.model_upsert.
        // Preserving absent score fields avoids erasing an upsert placement on restart;
        // withdrawing a config-authored score remains ambiguous until origin is stored.
        // The store retains Fusiform provenance, but not a shadow copy of the value
        // replaced by an override. Restore only stamps that were actually withdrawn,
        // using the seed when it has the route and otherwise the unknown value.
        restore_withdrawn_catalog_fields(&mut base, seed_fact, &withdrawn_fact_fields);
        store
            .upsert_fact(&merge_override(&base, ov), FactSource::Override)
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
                store
                    .upsert_fact(&restored, FactSource::Seed)
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
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn parses_provider_model_and_variant_keys() {
        assert_eq!(
            parse_model_key("openai/gpt-5.5"),
            Some(("openai".into(), "gpt-5.5".into(), None))
        );
        assert_eq!(
            parse_model_key("anthropic/claude/high"),
            Some(("anthropic".into(), "claude".into(), Some("high".into())))
        );
        assert_eq!(parse_model_key("bare"), None);
        assert_eq!(parse_model_key("a/b/c/d"), None);
        assert_eq!(parse_model_key("/m"), None);
    }

    #[test]
    fn family_override_distinguishes_absent_null_and_value() {
        let base = ModelFact {
            billing_plane: None,
            auth_methods: None,
            speed_is_fleet_median: false,
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
        let cleared: ModelFactOverride = serde_json::from_str(r#"{"family":null}"#).unwrap();
        assert_eq!(merge_override(&base, &cleared).family, None);
        let set: ModelFactOverride = serde_json::from_str(r#"{"family":"gpt"}"#).unwrap();
        assert_eq!(merge_override(&base, &set).family.as_deref(), Some("gpt"));
    }

    fn family_rule_rig(
        label: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        prefrontal_routing_store::SqliteRouteStateStore,
    ) {
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-family-rules-{label}-{}-{timestamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let db_path = root.join("store.sqlite");
        let config_path = root.join("prefrontal-routing.jsonc");
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
    fn explicit_family_override_beats_matching_rule() {
        let (root, config_path, store) = family_rule_rig("override-wins");
        crate::test_config::write(
            &config_path,
            r#"{
                "model_families": { "gpt": ["openai/*"] },
                "model_routing": {
                    "models": { "openai/gpt-5.4": { "family": "other" } }
                }
            }"#,
        )
        .unwrap();

        apply_overrides_from_config(&store, &config_path).unwrap();
        assert_eq!(
            store.get_model_family("openai/gpt-5.4").unwrap(),
            Some((Some(GrandFamily::Other), FamilySource::Override))
        );
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn family_glob_order_uses_the_first_written_match() {
        let (root, config_path, store) = family_rule_rig("first-match");
        crate::test_config::write(
            &config_path,
            r#"{
                "model_families": {
                    "other": ["google/*"],
                    "claude": ["google/antigravity-claude-*"]
                }
            }"#,
        )
        .unwrap();

        apply_overrides_from_config(&store, &config_path).unwrap();
        assert_eq!(
            store
                .get_model_family("google/antigravity-claude-opus-4-6-thinking")
                .unwrap(),
            Some((Some(GrandFamily::Other), FamilySource::Rule))
        );
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn example_family_rules_cover_every_seed_catalog_row() {
        let (root, _config_path, store) = family_rule_rig("example-coverage");
        let example = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../assets/alfonso-routing.example.jsonc");
        // Check that the example's model_families rules cover every catalog row
        // without validating unrelated example settings.
        #[derive(Deserialize)]
        struct FamilyOnly {
            model_families: ModelFamilyRules,
        }
        let document = jsonc_to_json(&std::fs::read_to_string(example).unwrap()).unwrap();
        let families: FamilyOnly = serde_json::from_str(&document).unwrap();
        apply_family_rules(&store, &families.model_families, &HashMap::new()).unwrap();
        let facts = store.get_facts().unwrap();
        let absent = facts.iter().filter(|fact| fact.family.is_none()).count();
        assert_eq!(
            absent, 0,
            "the shipped catch-all must cover new catalog rows"
        );
        let mut counts = BTreeMap::<String, usize>::new();
        for fact in facts {
            *counts.entry(fact.family.unwrap()).or_default() += 1;
        }
        eprintln!("example model-family counts: {counts:?}");
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unreadable_override_base_is_not_replaced_from_defaults() {
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-override-read-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "prefrontal-routing",
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
                    "models": { route_key.clone(): { "speed": 1200.0 } }
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
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-stale-override-read-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "prefrontal-routing",
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
                    "models": { route_key.clone(): { "speed": 1200.0 } }
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
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        // A migration-seeded fact (absent from the compiled JSON seed) whose
        // config override disappears must SURVIVE as source='seed'. The delete
        // arm erased fable-5/opus-5/opus-5-fast on 2026-08-10 when their stale
        // pre-Elo overrides were cleaned from the live config, and the loss
        // surfaced two layers away as a roster seat failing missing_fact.
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-demote-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let descriptor =
            dev_descriptor("prefrontal-routing", db_path.to_string_lossy().into_owned());
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
        let fact =
            prefrontal_routing_core::route_state_store::RouteStateStore::get_fact_by_route_key(
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-failure-threshold-test-{}",
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-min-band-read-test-{}-{n}",
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
        let config_path = dir.join("prefrontal-routing.jsonc");
        crate::test_config::write(
            &config_path,
            r#"{ "model_routing": { "min_best_effort_adequacy": 0.35 } }"#,
        )
        .unwrap();
        let val = super::read_min_best_effort_adequacy(&config_path).unwrap();
        assert_eq!(val, Some(0.35));

        // File without the key → None.
        let config_path2 = dir.join("prefrontal-routing-empty.jsonc");
        crate::test_config::write(&config_path2, r#"{ "model_routing": { "models": {} } }"#)
            .unwrap();
        assert_eq!(
            super::read_min_best_effort_adequacy(&config_path2).unwrap(),
            None
        );

        // File with NaN key → error.
        let config_path3 = dir.join("prefrontal-routing-nan.jsonc");
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-cache-policy-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.jsonc");
        assert_eq!(
            read_providers_without_prompt_cache(&missing).unwrap(),
            Vec::<String>::new()
        );

        let path = dir.join("prefrontal-routing.jsonc");
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-cache-policy-mtime-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("prefrontal-routing.jsonc");
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-operator-policy-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // A missing file is an empty policy: the operator excluded nothing.
        let missing = dir.join("missing.jsonc");
        assert_eq!(
            read_operator_routing_policy(&missing),
            ModelRoutingPolicyInput::default()
        );

        let path = dir.join("prefrontal-routing.jsonc");
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-operator-policy-broken-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("prefrontal-routing.jsonc");
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-elo-live-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("prefrontal-routing.jsonc");
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
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-overqualification-{}-{}.jsonc",
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
}

#[cfg(test)]
mod catalog_poll_tests {
    use super::*;

    #[test]
    fn catalog_poll_ms_defaults_reads_and_rejects_zero() {
        let missing = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-missing-catalog-poll-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&missing);
        assert_eq!(
            read_catalog_poll_ms(&missing).unwrap(),
            DEFAULT_CATALOG_POLL_MS
        );

        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-catalog-poll-{}.jsonc",
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
        let dir = prefrontal_test_support::scratch_root()
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

        // Removing the file withdraws every multiplier and reserve.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            settle_live_config(&path).pricing(),
            &RoutePricing::default()
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
