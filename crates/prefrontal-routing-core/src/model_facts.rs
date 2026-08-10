//! Model routing: core domain types.
//!
//! Ported from the Alfonso TS plugin (`src/features/model-routing/model-facts.ts`).
//!
//! Capability facts use open-ended ABSOLUTE scores, never normalized to the 0..100 demand scale.
//! Caller requirements are 0..100 task-demand weights. The router converts
//! absolute facts into per-task fit against the request's routable Elo frontier.
//!
//! Scales:
//!   - `elo`  : open model-capability Elo scale (unbounded above).
//!   - `eq`   : curated interpersonal/prose/judgment Elo-like score.
//!   - `speed`: Artificial Analysis median output tokens per second.
//!
//! Elo placements are curated facts; routing never derives them from live min/max values.
//!
//! Numeric fields the TS source typed as `number` are kept as `f64` here to
//! preserve exact arithmetic parity with the port source (a faithful-first port;
//! idiomatic narrowing to integer types can come once parity is proven).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static SPEED_MULTIPLIERS: OnceLock<Mutex<HashMap<String, f64>>> = OnceLock::new();

pub fn set_speed_multipliers(multipliers: HashMap<String, f64>) {
    let mut map = SPEED_MULTIPLIERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    *map = multipliers;
}

pub fn get_speed_multiplier(route_key: &str) -> Option<f64> {
    SPEED_MULTIPLIERS
        .get()
        .and_then(|m| m.lock().unwrap().get(route_key).copied())
}

/// Subscription subsidy multipliers. Keyed by bare provider ("openai") or by
/// full route key ("openai/gpt-5.6-sol"); a route-key entry overrides its
/// provider's. Provider keying is the primary shape: the subsidy is a property
/// of the ACCOUNT serving the provider, not of any one model - per-model
/// repetition was the drift that let a new model ship unpriced. A multiplier
/// divides the route's list price to yield its effective (subscription-
/// adjusted) price: 1.0 means "no subsidy known", larger values make the route
/// proportionally cheaper than list.
static SUBSCRIPTION_MULTIPLIERS: OnceLock<Mutex<HashMap<String, f64>>> = OnceLock::new();

pub fn set_subscription_multipliers(multipliers: HashMap<String, f64>) {
    let mut map = SUBSCRIPTION_MULTIPLIERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    *map = multipliers;
}

pub fn get_subscription_multiplier(route_key: &str) -> Option<f64> {
    SUBSCRIPTION_MULTIPLIERS.get().and_then(|m| {
        let map = m.lock().unwrap();
        if let Some(value) = map.get(route_key) {
            return Some(*value);
        }
        let provider = route_key
            .split_once('/')
            .map_or(route_key, |(provider, _)| provider);
        map.get(provider).copied()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct ModelScores {
    pub elo: f64,
    pub eq: f64,
    pub speed: f64,
}

impl ModelScores {
    pub fn get(&self, metric: ModelMetric) -> f64 {
        match metric {
            ModelMetric::Elo => self.elo,
            ModelMetric::Eq => self.eq,
            ModelMetric::Speed => self.speed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ModelRequirements {
    /// 0..100 task demand.
    pub iq: f64,
    /// 0..100 task demand.
    pub eq: f64,
    // NOTE: no `speed` demand: routing is cost-primary; speed is an always-on
    // tie-break among equal-cost adequate models, not a caller-supplied demand.
}

impl ModelRequirements {
    /// Demand for a metric. Speed has no demand axis (cost-primary), so it reads 0.
    pub fn get(&self, metric: ModelMetric) -> f64 {
        match metric {
            ModelMetric::Elo => self.iq,
            ModelMetric::Eq => self.eq,
            ModelMetric::Speed => 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelMetric {
    Elo,
    Eq,
    Speed,
}

impl ModelMetric {
    pub fn as_str(&self) -> &'static str {
        match self {
            ModelMetric::Elo => "elo",
            ModelMetric::Eq => "eq",
            ModelMetric::Speed => "speed",
        }
    }
}

pub const MODEL_METRICS: [ModelMetric; 3] = [ModelMetric::Elo, ModelMetric::Eq, ModelMetric::Speed];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ReasoningEffort {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReasoningEffort::Low => "low",
            ReasoningEffort::Medium => "medium",
            ReasoningEffort::High => "high",
            ReasoningEffort::Xhigh => "xhigh",
            ReasoningEffort::Max => "max",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelPromptDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    Curated,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelFact {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// Curated model lineage used for diversity; never inferred from provider id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    pub scores: ModelScores,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<f64>,
    /// Registry completion/output ceiling. `None` leaves send budgets uncapped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    /// Coarse 0..6 bucket (display/back-compat); prefer `cost_per_m_tok_out`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_tier: Option<f64>,
    /// Continuous output $/Mtok: the cost objective input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_per_m_tok_out: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_vision: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_tool_use: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_defaults: Option<ModelPromptDefaults>,
    pub confidence: Confidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_multiplier: Option<f64>,
    /// Subscription subsidy multiplier applied at read time (see
    /// [`get_subscription_multiplier`]). Divides `cost_per_m_tok_out` to yield the
    /// effective price. `None` (or a non-positive/non-finite value) means "no
    /// subsidy known" and behaves as 1.0; it is never persisted, only overlaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_multiplier: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRoute {
    pub provider_id: String,
    pub model_id: String,
    pub variant: Option<String>,
    /// Exact route identity: `provider/model[/variant]`.
    pub route_key: String,
    /// Shared capacity identity: usually `provider/model`.
    pub capacity_key: String,
    pub fact: ModelFact,
}

/// Canonical grand-family id grammar: `^[a-z0-9][a-z0-9._-]{0,63}$`.
///
/// One vocabulary is shared by every family consumer: model facts, persona
/// family prompt selectors, and seat family-diversity routing, so a family
/// label written for one surface always means the same thing on the others.
pub fn is_valid_grand_family_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    if !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit()) {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'))
}

impl ModelFact {
    /// The canonical optional grand-family fact for this route.
    ///
    /// Grand families are curated data carried by the fact (`family`); they are
    /// NEVER inferred from provider or model names. A curated value that does
    /// not match the canonical lowercase grammar is treated as absent rather
    /// than silently normalized, so prompt family selectors and diversity
    /// routing always consume one validated vocabulary.
    pub fn grand_family_id(&self) -> Option<&str> {
        self.family
            .as_deref()
            .filter(|id| is_valid_grand_family_id(id))
    }
}

/// Derive the canonical provider key from a routing key.
///
/// Routing keys have the grammar `provider/model[/variant]`. Provider-wide
/// state deliberately uses only the first component, so this helper is the
/// single parser used by recording, persistence, route insertion, diagnostics,
/// and tests. The returned key is trimmed but otherwise preserves the provider
/// vocabulary used by the catalog.
pub fn derive_provider_key(key: &str) -> Option<&str> {
    let provider = key.trim().split('/').next()?.trim();
    (!provider.is_empty()).then_some(provider)
}

/// Exact route identity. Variants are distinct routes.
pub fn make_route_key(provider_id: &str, model_id: &str, variant: Option<&str>) -> String {
    match variant {
        Some(v) => format!("{provider_id}/{model_id}/{v}"),
        None => format!("{provider_id}/{model_id}"),
    }
}

/// Shared provider/model capacity identity. Variants collapse to one capacity.
pub fn make_capacity_key(provider_id: &str, model_id: &str) -> String {
    format!("{provider_id}/{model_id}")
}

pub fn fact_to_route(fact: &ModelFact) -> ModelRoute {
    ModelRoute {
        provider_id: fact.provider_id.clone(),
        model_id: fact.model_id.clone(),
        variant: fact.variant.clone(),
        route_key: make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()),
        capacity_key: make_capacity_key(&fact.provider_id, &fact.model_id),
        fact: fact.clone(),
    }
}
