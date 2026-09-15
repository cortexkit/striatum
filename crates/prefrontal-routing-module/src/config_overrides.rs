//! Boot-time model-fact overrides from the module's OWN config-home file.
//!
//! Ported from the TS `applyModelFactOverrides` + the subscription-classifier's
//! config-read pattern. Per the locked contract with the daemon owner, routing
//! config does NOT flow over subc-mediated config (that path is unwired); instead
//! the module reads `~/.config/cortexkit/alfonso-routing.jsonc` itself at boot and
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
    make_capacity_key, make_route_key, model_facts_seed, CatalogFactField, FactSource,
    FailureBackoffConfig, FamilySource, GrandFamily, ModelRoutingPolicyInput, RouteStateStore,
    RoutingConfig, DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD, DEFAULT_DEMAND_EXPONENT,
};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};
use subc_jsonc::jsonc_to_json;

const CONFIG_RELATIVE_PATH: &str = "cortexkit/alfonso-routing.jsonc";
pub const DEFAULT_AA_API_KEY_REF: &str = "apikey:artificial-analysis";
pub const DEFAULT_CATALOG_POLL_MS: u64 = 10 * 60_000;
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
    model_families: ModelFamilyRules,
    #[serde(default)]
    aa: Option<AaSection>,
    #[serde(default)]
    catalog: CatalogSection,
}

#[derive(Debug, Deserialize, Default)]
struct CatalogSection {
    #[serde(default)]
    poll_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelFamilyRule {
    family: GrandFamily,
    globs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelFamilyRules(Vec<ModelFamilyRule>);

impl Default for ModelFamilyRules {
    fn default() -> Self {
        let rules = [
            (
                GrandFamily::Claude,
                &["*/claude-*", "anthropic/*", "google/antigravity-claude-*"][..],
            ),
            (
                GrandFamily::Gpt,
                &["openai/*", "*/gpt-*", "*/o1*", "*/o3*", "*/o4*"][..],
            ),
            (GrandFamily::Gemini, &["*/gemini-*", "google/*"][..]),
            (GrandFamily::Grok, &["*/grok-*", "xai/*"][..]),
            (GrandFamily::Qwen, &["*/qwen*", "alibaba-*/*"][..]),
            (
                GrandFamily::Kimi,
                &["*/kimi*", "moonshotai/*", "kimi-for-coding/*"][..],
            ),
            (GrandFamily::Glm, &["*/glm-*", "zai/*"][..]),
            (GrandFamily::Deepseek, &["*/deepseek-*", "deepseek/*"][..]),
            (GrandFamily::Minimax, &["*/minimax-*", "minimax/*"][..]),
            (GrandFamily::Mistral, &["*/mistral-*", "mistral/*"][..]),
            (GrandFamily::Llama, &["*/llama-*", "meta/*"][..]),
            (GrandFamily::Other, &["*"][..]),
        ];
        Self(
            rules
                .into_iter()
                .map(|(family, globs)| ModelFamilyRule {
                    family,
                    globs: globs.iter().map(|glob| (*glob).to_string()).collect(),
                })
                .collect(),
        )
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
                formatter.write_str("an ordered map of grand-family ids to route globs")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut rules = Vec::new();
                while let Some((wire_family, globs)) = map.next_entry::<String, Vec<String>>()? {
                    let family = GrandFamily::from_wire(&wire_family).ok_or_else(|| {
                        serde::de::Error::custom(format!(
                            "unknown model_families key `{wire_family}`"
                        ))
                    })?;
                    if globs.iter().any(|glob| glob.trim().is_empty()) {
                        return Err(serde::de::Error::custom(format!(
                            "model_families.{wire_family} entries must be non-empty route globs"
                        )));
                    }
                    rules.push(ModelFamilyRule { family, globs });
                }
                Ok(ModelFamilyRules(rules))
            }
        }

        deserializer.deserialize_map(RulesVisitor)
    }
}

/// Artificial Analysis ingest settings. Credential bytes stay in claustrum.
///
/// The operator-owned config uses explicit router-key-to-AA-slug entries because
/// accepting approximate model names could attach a score to the wrong route:
///
/// ```jsonc
/// aa: {
///   api_key_ref: "apikey:artificial-analysis",
///   model_map: {
///     "google/gemini-3.8-flash": "gemini-3-8-flash",
///     "ollama-cloud/glm-5.3": "glm-5-3",
///     "ollama-cloud/glm-5.3-flash": "glm-5-3-flash",
///   },
/// }
/// ```
#[derive(Deserialize, Default, Clone)]
pub struct AaSection {
    #[serde(default)]
    pub api_key_ref: Option<String>,
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
            .field("api_key_ref", &self.api_key_ref)
            .field("model_map", &self.model_map)
            .field("review_by_days", &self.review_by_days)
            .finish()
    }
}

/// Default review window for an AA-inferred score: two weeks.
pub const DEFAULT_AA_REVIEW_BY_MS: i64 = 14 * 24 * 60 * 60 * 1000;

#[derive(Clone)]
pub struct AaConfig {
    pub api_key_ref: Option<String>,
    pub model_map: HashMap<String, String>,
    pub review_by_ms: i64,
}

impl std::fmt::Debug for AaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AaConfig")
            .field("api_key_ref", &self.api_key_ref)
            .field("model_map", &self.model_map)
            .field("review_by_ms", &self.review_by_ms)
            .finish()
    }
}

impl Default for AaConfig {
    fn default() -> Self {
        Self {
            api_key_ref: None,
            model_map: HashMap::new(),
            review_by_ms: DEFAULT_AA_REVIEW_BY_MS,
        }
    }
}

/// Read the AA ingest section. Missing file or section disables ingest. A
/// present section defaults to the operator's claustrum credential ID.
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
    let raw: serde_json::Value = serde_json::from_str(&json)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    if raw
        .get("aa")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|aa| aa.contains_key("api_key"))
    {
        return Err(
            "aa.api_key is not accepted; set aa.api_key_ref and store the secret with `ck auth put --id apikey:artificial-analysis --kind api_key`"
                .to_string(),
        );
    }
    let config: RoutingConfigFile = serde_json::from_value(raw)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let Some(aa) = config.aa else {
        return Ok(AaConfig::default());
    };
    let review_by_ms = match aa.review_by_days {
        Some(days) if days > 0 => (days as i64).saturating_mul(24 * 60 * 60 * 1000),
        Some(_) => {
            return Err("aa.review_by_days must be a positive integer".to_string());
        }
        None => DEFAULT_AA_REVIEW_BY_MS,
    };
    let api_key_ref = match aa.api_key_ref {
        Some(reference) if reference.trim().is_empty() => {
            return Err("aa.api_key_ref must be a non-empty credential id".to_string());
        }
        Some(reference) => reference.trim().to_string(),
        None => DEFAULT_AA_API_KEY_REF.to_string(),
    };
    Ok(AaConfig {
        api_key_ref: Some(api_key_ref),
        model_map: aa.model_map,
        review_by_ms,
    })
}

#[derive(Debug, Deserialize, Default)]
struct ModelRoutingSection {
    /// Providers and models excluded from every automatic routing pool.
    #[serde(default)]
    exclude: Vec<String>,
    /// Providers and models priced as subscription-backed in addition to auth facts.
    #[serde(default)]
    subscription: Vec<String>,
    /// Routes eligible for a fast sibling while subscription quota is relaxed.
    #[serde(default, alias = "preferFastWhenRelaxed")]
    prefer_fast_when_relaxed: Vec<String>,
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

/// Reconcile explicit overrides and the default family rules against the store.
/// A missing file keeps catalog families complete while adding no explicit override.
pub fn apply_overrides_from_config<S: RouteStateStore>(
    store: &S,
    path: &std::path::Path,
) -> Result<OverrideOutcome, String> {
    let config = read_routing_config(path)?.unwrap_or_default();
    let outcome = apply_overrides(store, &config.model_routing.models)?;
    apply_family_rules(store, &config.model_families, &config.model_routing.models)?;
    Ok(outcome)
}

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

pub(crate) fn apply_family_rules_from_config<S: RouteStateStore>(
    store: &S,
    path: &Path,
) -> Result<usize, String> {
    let config = read_routing_config(path)?.unwrap_or_default();
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

/// The operator's routing policy as it was last read, keyed by config path.
#[derive(Debug, Clone)]
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
    read_operator_routing_policy_with_reader(path, |path| {
        std::fs::read_to_string(path)
            .map_err(|error| format!("reading {}: {error}", path.display()))
    })
}

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

fn parse_operator_routing_policy(
    path: &Path,
    document: &str,
) -> Result<ModelRoutingPolicyInput, String> {
    let json = jsonc_to_json(document)
        .map_err(|message| format!("invalid jsonc in {}: {message}", path.display()))?;
    let config: RoutingConfigFile = serde_json::from_str(&json)
        .map_err(|error| format!("invalid config in {}: {error}", path.display()))?;
    let section = config.model_routing;
    Ok(ModelRoutingPolicyInput {
        exclude: clean_spec_entries(section.exclude),
        subscription: clean_spec_entries(section.subscription),
        prefer_fast_when_relaxed: clean_spec_entries(section.prefer_fast_when_relaxed),
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
        ("prefer_fast_when_relaxed", &policy.prefer_fast_when_relaxed),
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
        let existing_fact = store
            .get_fact_by_route_key(&route_key)
            .map_err(|error| format!("reading override base {route_key}: {error}"))?;
        let mut base = existing_fact
            .clone()
            .or_else(|| seed_by_key.get(&route_key).cloned())
            .unwrap_or_else(|| ModelFact {
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
        let mut fact_fields = Vec::new();
        if ov.context_window.is_some() {
            fact_fields.push(CatalogFactField::ContextWindow);
        }
        if ov.cost_per_mtok_out.is_some() {
            fact_fields.push(CatalogFactField::CostPerMtokOut);
        }
        if ov.vision.is_some() {
            fact_fields.push(CatalogFactField::SupportsVision);
        }
        store
            .mark_operator_fact_fields(&route_key, &fact_fields)
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

    prefrontal_routing_core::model_facts::set_speed_multipliers(speed_multipliers);
    prefrontal_routing_core::model_facts::set_subscription_multipliers(subscription_multipliers);

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
    fn plaintext_aa_api_key_is_refused_with_vault_cutover_guidance() {
        let path = std::env::temp_dir().join(format!(
            "prefrontal-routing-aa-config-{}.jsonc",
            std::process::id()
        ));
        std::fs::write(&path, r#"{ "aa": { "api_key": "must-not-load" } }"#).unwrap();

        let error = read_aa_config(&path).expect_err("plaintext credentials must be refused");

        assert!(error.contains("api_key_ref"));
        assert!(error.contains("ck auth put --id apikey:artificial-analysis --kind api_key"));

        std::fs::write(&path, r#"{ "aa": {} }"#).unwrap();
        assert_eq!(
            read_aa_config(&path).unwrap().api_key_ref.as_deref(),
            Some(DEFAULT_AA_API_KEY_REF)
        );
        std::fs::write(&path, r#"{ "model_routing": {} }"#).unwrap();
        assert_eq!(read_aa_config(&path).unwrap().api_key_ref, None);
        let _ = std::fs::remove_file(path);
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
        let root = std::env::temp_dir().join(format!(
            "prefrontal-routing-family-rules-{label}-{}-{timestamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let db_path = root.join("store.sqlite");
        let config_path = root.join("alfonso-routing.jsonc");
        let descriptor = dev_descriptor(label, db_path.to_string_lossy().into_owned());
        (
            root,
            config_path,
            SqliteRouteStateStore::open(&descriptor).unwrap(),
        )
    }

    #[test]
    fn explicit_family_override_beats_matching_rule() {
        let _guard = multiplier_map_guard();
        let (root, config_path, store) = family_rule_rig("override-wins");
        std::fs::write(
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
        let _guard = multiplier_map_guard();
        let (root, config_path, store) = family_rule_rig("first-match");
        std::fs::write(
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
        let _guard = multiplier_map_guard();
        let (root, _config_path, store) = family_rule_rig("example-coverage");
        let example = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../assets/alfonso-routing.example.jsonc");
        apply_overrides_from_config(&store, &example).unwrap();
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
        let config_path = dir.join("alfonso-routing.jsonc");
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
        let config_path = dir.join("alfonso-routing.jsonc");
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
    fn unreadable_override_base_is_not_replaced_from_defaults() {
        use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
        // The loader replaces the process-global multiplier maps on its early
        // arms too; serialize with the other loader tests.
        let _guard = multiplier_map_guard();
        let dir = std::env::temp_dir().join(format!(
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
        std::fs::write(
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
        // The loader replaces the process-global multiplier maps on its early
        // arms too; serialize with the other loader tests.
        let _guard = multiplier_map_guard();
        let dir = std::env::temp_dir().join(format!(
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
        std::fs::write(
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
        std::fs::write(&config_path, r#"{ "model_routing": { "models": {} } }"#).unwrap();
        let error = apply_overrides_from_config(&store, &config_path).unwrap_err();
        assert!(error.contains("reading stale override base"), "{error}");
        let _ = std::fs::remove_dir_all(dir);
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
        let config_path = dir.join("alfonso-routing.jsonc");
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

        let path = dir.join("alfonso-routing.jsonc");
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
        let path = dir.join("alfonso-routing.jsonc");
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
    fn operator_routing_policy_defaults_to_empty_and_reloads_on_edit() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::{Duration, UNIX_EPOCH};

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
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

        let path = dir.join("alfonso-routing.jsonc");
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
        std::fs::write(
            &path,
            r#"{
              // routing fence
              "model_routing": {
                "exclude": ["anthropic", " ollama-cloud/glm-5.1 ", ""],
                "subscription": ["kimi-for-coding"],
                "prefer_fast_when_relaxed": ["openai"],
              },
            }"#,
        )
        .unwrap();
        set_mtime(1_700_000_000);
        let first = read_operator_routing_policy_with_reader(&path, read_file);
        assert_eq!(first.exclude, vec!["anthropic", "ollama-cloud/glm-5.1"]);
        assert_eq!(first.subscription, vec!["kimi-for-coding"]);
        assert_eq!(first.prefer_fast_when_relaxed, vec!["openai"]);
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
        std::fs::write(&path, r#"{ "model_routing": { "exclude": ["xai"] } }"#).unwrap();
        set_mtime(1_700_000_001);
        let reloaded = read_operator_routing_policy_with_reader(&path, read_file);
        assert_eq!(reloaded.exclude, vec!["xai"]);
        assert_eq!(reads.load(Ordering::Relaxed), 2, "changed mtime reparsed");

        // An absent model_routing block is an empty policy, not a parse failure.
        std::fs::write(&path, r#"{ "push": { "enabled": true } }"#).unwrap();
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
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-operator-policy-broken-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("alfonso-routing.jsonc");
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
            r#"{ "model_routing": { "exclude": ["anthropic"] } }"#,
        )
        .unwrap();
        set_mtime(1_700_000_000);
        assert_eq!(
            read_operator_routing_policy_with_reader(&path, read_file).exclude,
            vec!["anthropic"]
        );

        // Half-finished edit. The fence must NOT drop to empty: routing would
        // immediately start selecting the very routes the operator excluded.
        std::fs::write(&path, "{ \"model_routing\": { \"exclude\": [").unwrap();
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
        std::fs::write(&path, r#"{ "model_routing": { "exclude": ["xai"] } }"#).unwrap();
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

#[cfg(test)]
mod catalog_poll_tests {
    use super::*;

    #[test]
    fn catalog_poll_ms_defaults_reads_and_rejects_zero() {
        let missing = std::env::temp_dir().join(format!(
            "prefrontal-routing-missing-catalog-poll-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&missing);
        assert_eq!(
            read_catalog_poll_ms(&missing).unwrap(),
            DEFAULT_CATALOG_POLL_MS
        );

        let path = std::env::temp_dir().join(format!(
            "prefrontal-routing-catalog-poll-{}.jsonc",
            std::process::id()
        ));
        std::fs::write(&path, r#"{ "catalog": { "poll_ms": 1234 } }"#).unwrap();
        assert_eq!(read_catalog_poll_ms(&path).unwrap(), 1234);
        std::fs::write(&path, r#"{ "catalog": { "poll_ms": 0 } }"#).unwrap();
        assert!(read_catalog_poll_ms(&path)
            .unwrap_err()
            .contains("greater than zero"));
        let _ = std::fs::remove_file(path);
    }
}
