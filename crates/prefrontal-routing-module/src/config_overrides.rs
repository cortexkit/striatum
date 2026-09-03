//! Boot-time model-fact overrides from the module's OWN config-home file.
//!
//! Ported from the TS `applyModelFactOverrides` + the subscription-classifier's
//! config-read pattern. Per the locked contract with the daemon owner, routing
//! config does NOT flow over subc-mediated config (that path is unwired); instead
//! the module reads `~/.config/cortexkit/prefrontal-routing.jsonc` itself at boot and
//! merges the `model_routing.models` overrides onto the migration-applied seed.
//!
//! Overrides are upserted with source `override`; an override row that disappears
//! from config is reverted to the seed fact (or deleted if the model is not in the
//! seed). This is the user-GLOBAL tier (elo/eq/cost are not per-project); the
//! project tier (read at route.bind) is deferred.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use prefrontal_routing_core::model_facts::{Confidence, ModelFact, ModelScores, ScoreProvenance};
use prefrontal_routing_core::{
    make_route_key, model_facts_seed, FactSource, FailureBackoffConfig, RouteStateStore,
    RoutingConfig, DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD, DEFAULT_DEMAND_EXPONENT,
};
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};
use subc_jsonc::jsonc_to_json;

const CONFIG_RELATIVE_PATH: &str = "cortexkit/alfonso-routing.jsonc";
const OPENCODE_AUTH_RELATIVE_PATH: &str = "opencode/auth.json";

#[derive(Debug, Clone)]
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

fn prompt_cache_providers_cache() -> &'static Mutex<HashMap<PathBuf, PromptCacheProvidersCache>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, PromptCacheProvidersCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn default_providers_without_prompt_cache() -> Vec<String> {
    vec!["ollama-cloud".to_string()]
}

#[derive(Debug, Deserialize, Default)]
struct RoutingConfigFile {
    #[serde(default)]
    model_routing: ModelRoutingSection,
    #[serde(default)]
    aa: AaSection,
}

/// Artificial Analysis ingest settings. The API key is never logged or served.
#[derive(Deserialize, Default, Clone)]
pub struct AaSection {
    #[serde(default)]
    pub api_key: Option<String>,
    /// Explicit router key (`provider/model[/variant]`) → AA slug. No fuzzy join.
    #[serde(default)]
    pub model_map: HashMap<String, String>,
    /// Days until an `aa_inferred` row is flagged past-due. Default 14.
    #[serde(default)]
    pub review_by_days: Option<u64>,
}

impl std::fmt::Debug for AaSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AaSection")
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("model_map", &self.model_map)
            .field("review_by_days", &self.review_by_days)
            .finish()
    }
}

/// Default review window for an AA-inferred score: two weeks.
pub const DEFAULT_AA_REVIEW_BY_MS: i64 = 14 * 24 * 60 * 60 * 1000;

#[derive(Clone)]
pub struct AaConfig {
    pub api_key: Option<String>,
    pub model_map: HashMap<String, String>,
    pub review_by_ms: i64,
}

impl std::fmt::Debug for AaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AaConfig")
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("model_map", &self.model_map)
            .field("review_by_ms", &self.review_by_ms)
            .finish()
    }
}

impl Default for AaConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            model_map: HashMap::new(),
            review_by_ms: DEFAULT_AA_REVIEW_BY_MS,
        }
    }
}

/// Read the AA ingest section. Missing file → empty config. The API key is
/// returned for fetch use only; callers must never log or serialize it.
pub fn read_aa_config(path: &Path) -> Result<AaConfig, String> {
    let document = match std::fs::read_to_string(path) {
        Ok(document) => document,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AaConfig::default());
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let json = jsonc_to_json(&document)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let review_by_ms = match config.aa.review_by_days {
        Some(days) if days > 0 => (days as i64).saturating_mul(24 * 60 * 60 * 1000),
        Some(_) => {
            return Err("aa.review_by_days must be a positive integer".to_string());
        }
        None => DEFAULT_AA_REVIEW_BY_MS,
    };
    let api_key = config
        .aa
        .api_key
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty());
    Ok(AaConfig {
        api_key,
        model_map: config.aa.model_map,
        review_by_ms,
    })
}

#[derive(Debug, Deserialize, Default)]
struct ModelRoutingSection {
    /// Keyed by "provider/model" or "provider/model/variant".
    #[serde(default)]
    models: HashMap<String, ModelFactOverride>,
    /// Request-time capability overrides. Unknown keys are ignored and surfaced
    /// to the caller for warning logs.
    #[serde(default)]
    elo_overrides: HashMap<String, f64>,
    /// Power-curve exponent mapping caller IQ demand to the request frontier.
    #[serde(default)]
    demand_exponent: Option<f64>,
    /// Minimum adequacy for best-effort selection (sanity band). Default 0.6.
    /// Clamped to [0, 1]; NaN is rejected loudly at parse time.
    #[serde(default)]
    min_best_effort_adequacy: Option<f64>,
    /// Terminal send failures required before the route-local safety backoff starts.
    #[serde(default, alias = "consecutiveTerminalFailureThreshold")]
    consecutive_terminal_failure_threshold: Option<u32>,
    /// Providers that do not expose prompt-cache reads or writes. Gather seats
    /// avoid these providers when a cache-capable adequate route exists.
    #[serde(
        default = "default_providers_without_prompt_cache",
        alias = "providersWithoutPromptCache"
    )]
    providers_without_prompt_cache: Vec<String>,
    /// Provider-keyed subscription subsidy multipliers ("openai": 40). The
    /// subsidy is a property of the account serving the provider, so this is
    /// the primary shape; a per-model `subscription_multiplier` in `models`
    /// overrides its provider's value for that route only.
    #[serde(default)]
    subscription_multipliers: HashMap<String, f64>,
}

/// One per-model override. Every field optional: a present field overrides the
/// base fact, an absent one preserves it. Mirrors the TS `ModelFactOverride`.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
struct ModelFactOverride {
    /// Outer None means absent; Some(None) means an explicit JSON null clears it.
    #[serde(default, deserialize_with = "deserialize_family_override")]
    family: Option<Option<String>>,
    eq: Option<f64>,
    speed: Option<f64>,
    cost_per_mtok_out: Option<f64>,
    context_window: Option<f64>,
    vision: Option<bool>,
    speed_multiplier: Option<f64>,
    /// Subscription subsidy multiplier: divides the route's list price to yield its
    /// effective price. 1.0 (or absent) means "no subsidy known"; larger values make
    /// the route proportionally cheaper than list. Validated at load (must be finite
    /// and > 0); an invalid value is rejected and logged, not applied.
    subscription_multiplier: Option<f64>,
}

/// The config-home path for this module's own config file. Honors
/// XDG_CONFIG_HOME, else `$HOME/.config`, matching the daemon's own resolution.
pub fn config_path() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg).join(CONFIG_RELATIVE_PATH);
    }
    if let Some(home) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(home)
            .join(".config")
            .join(CONFIG_RELATIVE_PATH);
    }
    PathBuf::from(".config").join(CONFIG_RELATIVE_PATH)
}

pub fn auth_json_path() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg).join(OPENCODE_AUTH_RELATIVE_PATH);
    }
    if let Some(home) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join(OPENCODE_AUTH_RELATIVE_PATH);
    }
    PathBuf::from(".local")
        .join("share")
        .join(OPENCODE_AUTH_RELATIVE_PATH)
}

/// Read only the provider auth kind needed by subscription policy. Token values
/// remain owned by OpenCode and never enter the routing request or decision log.
pub fn read_provider_auth_types(path: &Path) -> Result<HashMap<String, String>, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("reading {}: {error}", path.display()))?;
    let parsed: HashMap<String, serde_json::Value> = serde_json::from_str(&raw)
        .map_err(|error| format!("parsing {}: {error}", path.display()))?;
    Ok(parsed
        .into_iter()
        .filter_map(|(provider, value)| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(|kind| (provider, kind.to_string()))
        })
        .collect())
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
        elo: base.scores.elo,
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

#[derive(Debug, Default, PartialEq, Eq)]
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

/// Read the config file (if present) and reconcile model-fact overrides against the
/// store. Missing file = a clean no-op. A parse error is returned so the caller can
/// log it without aborting boot (routing degrades to the seed). Pure-ish: all
/// effects go through the `RouteStateStore` trait, so it is testable with any store.
pub fn apply_overrides_from_config<S: RouteStateStore>(
    store: &S,
    path: &std::path::Path,
) -> Result<OverrideOutcome, String> {
    let doc = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(OverrideOutcome::default()),
        Err(e) => return Err(format!("reading {}: {e}", path.display())),
    };
    let json =
        jsonc_to_json(&doc).map_err(|m| format!("invalid jsonc in {}: {m}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|e| format!("invalid config in {}: {e}", path.display()))?;
    apply_overrides(store, &config.model_routing.models)
}

/// Read `min_best_effort_adequacy` from the routing config file, validating and
/// clamping the value. Returns `None` when absent or the file is missing.
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

/// Refresh route multipliers before facts are loaded for a selection. Removing
/// the config file or a multiplier clears its previous value on the next request.
pub fn reload_live_multipliers(path: &Path) -> Result<(), String> {
    let document = match std::fs::read_to_string(path) {
        Ok(document) => document,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            prefrontal_routing_core::model_facts::set_speed_multipliers(HashMap::new());
            prefrontal_routing_core::model_facts::set_subscription_multipliers(HashMap::new());
            return Ok(());
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let parsed = jsonc_to_json(&document)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))
        .and_then(|json| {
            serde_json::from_str::<RoutingConfigFile>(&json)
                .map_err(|error| format!("invalid config in {}: {error}", path.display()))
        });
    let config = match parsed {
        Ok(config) => config,
        Err(error) => {
            prefrontal_routing_core::model_facts::set_speed_multipliers(HashMap::new());
            prefrontal_routing_core::model_facts::set_subscription_multipliers(HashMap::new());
            return Err(error);
        }
    };

    let mut speed_multipliers = HashMap::new();
    let mut subscription_multipliers = HashMap::new();
    for (provider, value) in config.model_routing.subscription_multipliers {
        let provider = provider.trim().to_string();
        if provider.is_empty() {
            continue;
        }
        if value.is_finite() && value > 0.0 {
            subscription_multipliers.insert(provider, value);
        } else {
            eprintln!(
                "[prefrontal-routing] invalid subscription_multiplier for provider {provider}: {value}; ignoring"
            );
        }
    }
    for (key, model) in config.model_routing.models {
        let Some((provider, model_id, variant)) = parse_model_key(&key) else {
            continue;
        };
        let route_key = make_route_key(&provider, &model_id, variant.as_deref());
        if let Some(value) = model.speed_multiplier {
            if value.is_finite() && value > 0.0 && value <= 2.0 {
                speed_multipliers.insert(route_key.clone(), value);
            } else {
                eprintln!(
                    "[prefrontal-routing] invalid speed_multiplier for route {route_key}: {value}; ignoring"
                );
            }
        }
        if let Some(value) = model.subscription_multiplier {
            if value.is_finite() && value > 0.0 {
                subscription_multipliers.insert(route_key, value);
            } else {
                eprintln!(
                    "[prefrontal-routing] invalid subscription_multiplier for route {route_key}: {value}; ignoring"
                );
            }
        }
    }
    prefrontal_routing_core::model_facts::set_speed_multipliers(speed_multipliers);
    prefrontal_routing_core::model_facts::set_subscription_multipliers(subscription_multipliers);
    Ok(())
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
        Sha256::digest(format!("model-routing-default:{DEFAULT_DEMAND_EXPONENT}").as_bytes())
    )
}

fn policy_revision(document: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(document.as_bytes()))
}

/// Re-read request-time Elo settings and apply known route overrides to this
/// selection's fact snapshot. Nothing is persisted, so removing an override
/// restores the catalog Elo value on the next request.
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
pub fn read_providers_without_prompt_cache(path: &Path) -> Result<Vec<String>, String> {
    read_providers_without_prompt_cache_with_reader(path, |path| {
        std::fs::read_to_string(path)
            .map_err(|error| format!("reading {}: {error}", path.display()))
    })
}

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
        None => Ok(default_providers_without_prompt_cache()),
        Some(_) => read_file(path).and_then(|doc| {
            let json = jsonc_to_json(&doc)
                .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
            let config: RoutingConfigFile = serde_json::from_str(&json)
                .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
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

fn apply_overrides<S: RouteStateStore>(
    store: &S,
    overrides: &HashMap<String, ModelFactOverride>,
) -> Result<OverrideOutcome, String> {
    // The current seed is the stable base for an override (so a seed-value change
    // flows through), falling back to the live row, else a fresh skeleton so config
    // can introduce a brand-new model.
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
    let mut speed_multipliers = HashMap::new();
    let mut subscription_multipliers = HashMap::new();

    for (key, ov) in overrides {
        let Some((provider_id, model_id, variant)) = parse_model_key(key) else {
            outcome.skipped.push(key.clone());
            continue;
        };
        let route_key = make_route_key(&provider_id, &model_id, variant.as_deref());
        if let Some(val) = ov.speed_multiplier {
            if val.is_finite() && val > 0.0 && val <= 2.0 {
                speed_multipliers.insert(route_key.clone(), val);
            } else {
                eprintln!(
                    "[prefrontal-routing] invalid speed_multiplier for route {}: {}, skipping multiplier",
                    route_key, val
                );
            }
        }
        if let Some(val) = ov.subscription_multiplier {
            // A subsidy multiplier divides list price, so it must be finite and
            // strictly positive; zero or negative would divide by zero or flip the
            // price negative. There is deliberately no upper cap here (unlike
            // speed_multiplier): a measured API-to-subscription ratio may exceed 1.0.
            if val.is_finite() && val > 0.0 {
                subscription_multipliers.insert(route_key.clone(), val);
            } else {
                eprintln!(
                    "[prefrontal-routing] invalid subscription_multiplier for route {}: {}, skipping multiplier",
                    route_key, val
                );
            }
        }
        let existing_fact = store.get_fact_by_route_key(&route_key).ok().flatten();
        let mut base = seed_by_key
            .get(&route_key)
            .cloned()
            .or_else(|| existing_fact.clone())
            .unwrap_or_else(|| ModelFact {
                provider_id,
                model_id,
                variant,
                family: None,
                scores: ModelScores::default(),
                context_window: None,
                max_output_tokens: None,
                cost_tier: None,
                cost_per_m_tok_out: None,
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
            });
        if base.max_output_tokens.is_none() {
            base.max_output_tokens = existing_fact.and_then(|fact| fact.max_output_tokens);
        }
        store
            .upsert_fact(&merge_override(&base, ov), FactSource::Override)
            .map_err(|e| format!("upsert override {route_key}: {e}"))?;
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
                        .ok()
                        .flatten()
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

    prefrontal_routing_core::model_facts::set_speed_multipliers(speed_multipliers);
    prefrontal_routing_core::model_facts::set_subscription_multipliers(subscription_multipliers);

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SUBSCRIPTION_MULTIPLIERS in model_facts is one process-global map that
    /// set_subscription_multipliers REPLACES wholesale. Tests that load a config
    /// and then read the map race each other under the parallel test runner:
    /// another test's load lands between this test's load and its asserts, and
    /// the assert reads the other test's config. Serialize every such test.
    fn multiplier_map_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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
        let set: ModelFactOverride = serde_json::from_str(r#"{"family":"new-lineage"}"#).unwrap();
        assert_eq!(
            merge_override(&base, &set).family.as_deref(),
            Some("new-lineage")
        );
    }

    #[test]
    fn validates_speed_multiplier_at_load() {
        let _guard = multiplier_map_guard();
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-overrides-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let descriptor =
            dev_descriptor("prefrontal-routing", db_path.to_string_lossy().into_owned());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open test store");

        let config_content = r#"{
            "model_routing": {
                "models": {
                    "openai/gpt-5.5": {
                        "speed_multiplier": 0.5
                    },
                    "openai/gpt-5.4": {
                        "speed_multiplier": 0.0
                    },
                    "openai/gpt-5.4-mini": {
                        "speed_multiplier": -1.0
                    },
                    "openai/gpt-5.4-fast": {
                        "speed_multiplier": 2.5
                    },
                    "openai/gpt-5.4-mini-fast": {
                        "speed_multiplier": null
                    }
                }
            }
        }"#;
        let config_path = dir.join("prefrontal-routing.jsonc");
        std::fs::write(&config_path, config_content).unwrap();

        let outcome = apply_overrides_from_config(&store, &config_path).unwrap();
        assert_eq!(outcome.applied, 5);

        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.5"),
            Some(0.5)
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.4"),
            None
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.4-mini"),
            None
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.4-fast"),
            None
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.4-mini-fast"),
            None
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validates_nan_speed_multiplier() {
        let _guard = multiplier_map_guard();
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-overrides-nan-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let descriptor =
            dev_descriptor("prefrontal-routing", db_path.to_string_lossy().into_owned());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open test store");

        let mut overrides = HashMap::new();
        overrides.insert(
            "openai/gpt-5.5".to_string(),
            ModelFactOverride {
                family: None,
                eq: None,
                speed: None,
                cost_per_mtok_out: None,
                context_window: None,
                vision: None,
                speed_multiplier: Some(f64::NAN),
                subscription_multiplier: None,
            },
        );

        let outcome = apply_overrides(&store, &overrides).unwrap();
        assert_eq!(outcome.applied, 1);
        assert_eq!(
            prefrontal_routing_core::model_facts::get_speed_multiplier("openai/gpt-5.5"),
            None
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validates_subscription_multiplier_at_load() {
        let _guard = multiplier_map_guard();
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-sub-multiplier-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("store.db");
        let descriptor =
            dev_descriptor("prefrontal-routing", db_path.to_string_lossy().into_owned());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open test store");

        // 2.0 is a valid subsidy (halves effective price) and must be accepted; 0.0
        // and -1.0 would divide by zero / flip the price negative and are rejected
        // and logged rather than applied.
        let config_content = r#"{
            "model_routing": {
                "models": {
                    "openai/gpt-5.5": {
                        "subscription_multiplier": 2.0
                    },
                    "openai/gpt-5.4": {
                        "subscription_multiplier": 0.0
                    },
                    "openai/gpt-5.4-mini": {
                        "subscription_multiplier": -1.0
                    }
                }
            }
        }"#;
        let config_path = dir.join("prefrontal-routing.jsonc");
        std::fs::write(&config_path, config_content).unwrap();

        let outcome = apply_overrides_from_config(&store, &config_path).unwrap();
        assert_eq!(outcome.applied, 3);

        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier("openai/gpt-5.5"),
            Some(2.0)
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier("openai/gpt-5.4"),
            None
        );
        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier(
                "openai/gpt-5.4-mini"
            ),
            None
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn removing_an_override_demotes_the_row_and_never_deletes_it() {
        let _guard = multiplier_map_guard();
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        // A migration-seeded fact (absent from the compiled JSON seed) whose
        // config override disappears must SURVIVE as source='seed'. The delete
        // arm erased fable-5/opus-5/opus-5-fast on 2026-08-10 when their stale
        // pre-Elo overrides were cleaned from the live config, and the loss
        // surfaced two layers away as a roster seat failing missing_fact.
        let dir = std::env::temp_dir().join(format!(
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
        std::fs::write(
            &config_path,
            r##"{ "model_routing": { "models": {
                "anthropic/claude-opus-5": { "speed_multiplier": 0.9 }
            } } }"##,
        )
        .unwrap();
        apply_overrides_from_config(&store, &config_path).unwrap();

        // Round 2: the override is removed from config entirely.
        std::fs::write(&config_path, r#"{ "model_routing": { "models": {} } }"#).unwrap();
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
    fn provider_level_subscription_multipliers_apply_with_route_override() {
        let _guard = multiplier_map_guard();
        // The subsidy is a property of the account serving the provider, so the
        // provider key is the primary shape: every model inherits it, including
        // ones that did not exist when the config was written (the per-model
        // repetition let a new model ship unpriced). A per-route entry still
        // overrides its provider's value.
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-provider-mult-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("routing.jsonc");
        std::fs::write(
            &config_path,
            r##"{
                "model_routing": {
                    "subscription_multipliers": { "openai": 40.0, "bogus": -1.0 },
                    "models": {
                        "openai/special-route": { "subscription_multiplier": 5.0 }
                    }
                }
            }"##,
        )
        .unwrap();

        super::reload_live_multipliers(&config_path).unwrap();
        // Any model on the provider inherits, including never-listed ones.
        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier(
                "openai/brand-new-model"
            ),
            Some(40.0)
        );
        // A per-route entry overrides its provider's value.
        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier(
                "openai/special-route"
            ),
            Some(5.0)
        );
        // Invalid provider values are rejected, not applied.
        assert_eq!(
            prefrontal_routing_core::model_facts::get_subscription_multiplier("bogus/anything"),
            None
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
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-failure-threshold-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("routing.jsonc");
        std::fs::write(
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

        std::fs::write(
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
        let dir = std::env::temp_dir().join(format!(
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
        std::fs::write(
            &config_path,
            r#"{ "model_routing": { "min_best_effort_adequacy": 0.35 } }"#,
        )
        .unwrap();
        let val = super::read_min_best_effort_adequacy(&config_path).unwrap();
        assert_eq!(val, Some(0.35));

        // File without the key → None.
        let config_path2 = dir.join("prefrontal-routing-empty.jsonc");
        std::fs::write(&config_path2, r#"{ "model_routing": { "models": {} } }"#).unwrap();
        assert_eq!(
            super::read_min_best_effort_adequacy(&config_path2).unwrap(),
            None
        );

        // File with NaN key → error.
        let config_path3 = dir.join("prefrontal-routing-nan.jsonc");
        std::fs::write(
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
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-cache-policy-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.jsonc");
        assert_eq!(
            read_providers_without_prompt_cache(&missing).unwrap(),
            vec!["ollama-cloud"]
        );

        let path = dir.join("prefrontal-routing.jsonc");
        std::fs::write(
            &path,
            r#"{"model_routing":{"providers_without_prompt_cache":["ollama-cloud"," custom ","ollama-cloud"]}}"#,
        )
        .unwrap();
        assert_eq!(
            read_providers_without_prompt_cache(&path).unwrap(),
            vec!["custom", "ollama-cloud"]
        );
        std::fs::write(
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
        let dir = std::env::temp_dir().join(format!(
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

        std::fs::write(
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

        std::fs::write(
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

        std::fs::write(&path, "{ not jsonc").unwrap();
        set_mtime(1_700_000_002);
        assert!(read_providers_without_prompt_cache_with_reader(&path, read_file).is_err());
        assert_eq!(reads.load(Ordering::Relaxed), 3);

        std::fs::write(
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
    fn elo_overrides_and_demand_exponent_live_reload_and_warn_on_unknown_keys() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-elo-live-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("alfonso-routing.jsonc");
        let mut facts = model_facts_seed().to_vec();
        let sol = |facts: &[ModelFact]| {
            facts
                .iter()
                .find(|fact| fact.provider_id == "openai" && fact.model_id == "gpt-5.6-sol")
                .unwrap()
                .scores
                .elo
        };

        std::fs::write(
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
        std::fs::write(
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
