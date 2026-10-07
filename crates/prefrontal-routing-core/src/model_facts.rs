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

/// Suffix used for provider models that are priority-tier siblings of a base model.
pub const FAST_SUFFIX: &str = "-fast";

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

/// Closed score provenance on a catalog row.
///
/// `curated` is a migration or operator seed (`source='seed'` in the store).
/// `override` is `route.model_upsert` or config. `aa_inferred` is the provisional
/// AA-index interpolation. `unscored` is a model known to the catalog with no
/// score yet (the Fusiform consumer's target) and is not routable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreProvenance {
    #[default]
    Curated,
    Override,
    AaInferred,
    Unscored,
}

impl ScoreProvenance {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScoreProvenance::Curated => "curated",
            ScoreProvenance::Override => "override",
            ScoreProvenance::AaInferred => "aa_inferred",
            ScoreProvenance::Unscored => "unscored",
        }
    }

    pub fn is_curated(&self) -> bool {
        matches!(self, ScoreProvenance::Curated)
    }

    pub fn is_routable(&self) -> bool {
        !matches!(self, ScoreProvenance::Unscored)
    }
}

/// Closed lineage vocabulary shared by routing, persona templates, and room seats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrandFamily {
    Claude,
    Gpt,
    Gemini,
    Grok,
    Qwen,
    Kimi,
    Glm,
    Deepseek,
    Minimax,
    Mistral,
    Llama,
    Other,
}

impl GrandFamily {
    pub const ALL: [Self; 12] = [
        Self::Claude,
        Self::Gpt,
        Self::Gemini,
        Self::Grok,
        Self::Qwen,
        Self::Kimi,
        Self::Glm,
        Self::Deepseek,
        Self::Minimax,
        Self::Mistral,
        Self::Llama,
        Self::Other,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Gpt => "gpt",
            Self::Gemini => "gemini",
            Self::Grok => "grok",
            Self::Qwen => "qwen",
            Self::Kimi => "kimi",
            Self::Glm => "glm",
            Self::Deepseek => "deepseek",
            Self::Minimax => "minimax",
            Self::Mistral => "mistral",
            Self::Llama => "llama",
            Self::Other => "other",
        }
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| family.as_str() == value)
    }
}

/// Historical store spellings accepted only by the versioned normalization migration.
pub const GRAND_FAMILY_NORMALIZATIONS: [(&str, GrandFamily); 5] = [
    ("anthropic", GrandFamily::Claude),
    ("openai", GrandFamily::Gpt),
    ("openai-gpt", GrandFamily::Gpt),
    ("google-gemini", GrandFamily::Gemini),
    ("xai-grok", GrandFamily::Grok),
];

pub fn normalize_grand_family_id(value: &str) -> Option<GrandFamily> {
    GrandFamily::from_wire(value).or_else(|| {
        GRAND_FAMILY_NORMALIZATIONS
            .into_iter()
            .find_map(|(historical, family)| (historical == value).then_some(family))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FamilySource {
    Override,
    Rule,
    Curated,
    #[default]
    Absent,
}

impl FamilySource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Override => "override",
            Self::Rule => "rule",
            Self::Curated => "curated",
            Self::Absent => "absent",
        }
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "override" => Some(Self::Override),
            "rule" => Some(Self::Rule),
            "curated" => Some(Self::Curated),
            "absent" => Some(Self::Absent),
            _ => None,
        }
    }
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
    /// Catalog-served input $/Mtok. Absence is distinct from an explicit zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_rate: Option<f64>,
    /// Catalog-served cached-read $/Mtok. Absence is distinct from an explicit zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_rate: Option<f64>,
    /// Catalog-served cache-write $/Mtok. Absence is distinct from an explicit zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_rate: Option<f64>,
    /// Optional catalog-served reasoning $/Mtok.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_rate: Option<f64>,
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
    /// True when `scores.speed` is the fleet median standing in for a route that has
    /// no observation of its own, so a reader of a decision can tell a measured speed
    /// from a placeholder. Never persisted: it is applied at read time beside the
    /// observed-speed overlay and must not survive into a stored fact, or the
    /// placeholder would look like a measurement on the next read.
    #[serde(skip)]
    pub speed_is_fleet_median: bool,
    /// Subscription subsidy multiplier applied at read time (see
    /// [`get_subscription_multiplier`]). Divides `cost_per_m_tok_out` to yield the
    /// effective price. `None` (or a non-positive/non-finite value) means "no
    /// subsidy known" and behaves as 1.0; it is never persisted, only overlaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_multiplier: Option<f64>,
    /// Closed score provenance. Legacy rows and serde-defaulted facts are curated.
    #[serde(default, skip_serializing_if = "ScoreProvenance::is_curated")]
    pub score_provenance: ScoreProvenance,
    /// Raw Artificial Analysis intelligence index used to infer this row's Elo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aa_index: Option<f64>,
    /// Unix milliseconds after which an `aa_inferred` row is past due for hand review.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_by_ms: Option<i64>,
    /// How EQ was chosen for an `aa_inferred` row (`aa_index` or `neighbor:<route>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eq_source: Option<String>,
    /// Optional class vector computed at read time by `get_facts_with_observed_speed`.
    /// Never persisted onto a stored fact row.
    #[serde(skip)]
    pub class_vector: Option<ClassVector>,
    /// Flag indicating that `class_vector` is an overlay value.
    #[serde(skip)]
    pub class_vector_is_overlay: bool,
}

/// The five-counter complete-case token profile for a workload class.
/// Computed over the trailing 7-day window (now - 7d, now] on rows where
/// all five counters are non-NULL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassVector {
    pub workload_class: String,
    pub fresh_input: f64,
    pub output_excluding_reasoning: f64,
    pub reasoning: f64,
    pub cached_read: f64,
    pub cache_write: f64,
    pub sample_count: u64,
    pub trailing_window_days: f64,
    pub statistic: String,
    pub is_overlay: bool,
}

pub const CLASS_VECTOR_SAMPLE_FLOOR: u64 = 50;
pub const CLASS_VECTOR_TRAILING_WINDOW_DAYS: f64 = 7.0;
pub const CLASS_VECTOR_TRAILING_WINDOW_MS: f64 = 7.0 * 24.0 * 60.0 * 60.0 * 1_000.0;
pub const ENUMERATED_WORKLOAD_CLASSES: [&str; 4] = ["ephemeral", "head", "gather", "hire"];

impl ClassVector {
    pub fn is_usable(&self) -> bool {
        self.sample_count >= CLASS_VECTOR_SAMPLE_FLOOR
    }
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

/// Return whether a wire value belongs to the closed grand-family vocabulary.
pub fn is_valid_grand_family_id(id: &str) -> bool {
    GrandFamily::from_wire(id).is_some()
}

impl ModelFact {
    /// Grand families are curated catalog data and are never inferred from route names.
    pub fn grand_family_id(&self) -> Option<&str> {
        self.family
            .as_deref()
            .and_then(GrandFamily::from_wire)
            .map(GrandFamily::as_str)
    }

    /// The ephemeral vector attached by `get_facts_with_observed_speed`, whether
    /// it is the route's own or the fleet-wide overlay (see
    /// [`ModelFact::own_ephemeral_vector`] for the distinction).
    pub fn ephemeral_vector(&self) -> Option<&ClassVector> {
        self.class_vector
            .as_ref()
            .filter(|v| v.workload_class == "ephemeral")
    }

    /// The ephemeral vector measured on this route alone. A vector carried with
    /// `class_vector_is_overlay` set is the fleet-wide statistic laid over every
    /// fact at read time (the routing store computes one whole-population
    /// ephemeral mean and stamps it on each fact), so it is not the route's own.
    pub fn own_ephemeral_vector(&self) -> Option<&ClassVector> {
        self.ephemeral_vector()
            .filter(|_| !self.class_vector_is_overlay)
    }

    /// The fleet-wide ephemeral vector this fact carries as a read-time overlay.
    pub fn fleet_ephemeral_vector(&self) -> Option<&ClassVector> {
        self.ephemeral_vector()
            .filter(|_| self.class_vector_is_overlay)
    }

    /// True when all four catalog rates a basket can use are served. Basket
    /// pricing no longer requires this: a dimension the workload never uses
    /// needs no rate (see [`ModelFact::basket_price`]).
    pub fn has_required_basket_rates(&self) -> bool {
        self.cost_per_m_tok_out.is_some()
            && self.input_rate.is_some()
            && self.cache_read_rate.is_some()
            && self.cache_write_rate.is_some()
    }

    /// The catalog rate ($/Mtok) for one basket dimension. Reasoning tokens fall
    /// back to the output rate when no separate reasoning rate is served.
    pub fn basket_rate(&self, dimension: BasketDimension) -> Option<f64> {
        match dimension {
            BasketDimension::FreshInput => self.input_rate,
            BasketDimension::OutputExcludingReasoning => self.cost_per_m_tok_out,
            BasketDimension::Reasoning => self.reasoning_rate.or(self.cost_per_m_tok_out),
            BasketDimension::CachedRead => self.cache_read_rate,
            BasketDimension::CacheWrite => self.cache_write_rate,
        }
    }

    /// Prices one message of `vector`'s workload on this route, dimension by
    /// dimension: the sum of tokens x rate over the five token dimensions, divided
    /// by the subscription multiplier, normalized against
    /// BASKET_REFERENCE_COST_PER_MESSAGE and clamped to 0..1.
    ///
    /// A dimension with no tokens needs no rate: it contributes zero whatever its
    /// rate would be (Codex runs record no cache-write tokens, so a catalog row
    /// without a cache-write rate still prices fully). A dimension with tokens
    /// and no usable rate makes the route unpriceable; `cost` is then `None` and
    /// `unpriced` names every such dimension. The vector's sample floor is the
    /// caller's concern.
    pub fn basket_price(&self, vector: &ClassVector) -> BasketPrice {
        let multiplier = self
            .subscription_multiplier
            .filter(|m| m.is_finite() && *m > 0.0)
            .unwrap_or(1.0);
        let mut raw_cost = 0.0;
        let mut unpriced = Vec::new();
        for dimension in BasketDimension::ALL {
            let tokens = dimension.tokens(vector);
            if tokens <= 0.0 || tokens.is_nan() {
                continue;
            }
            match self.basket_rate(dimension).filter(|rate| rate.is_finite()) {
                Some(rate) => raw_cost += tokens * rate,
                None => unpriced.push(dimension),
            }
        }
        let cost = unpriced.is_empty().then(|| {
            let basket_unscaled =
                raw_cost / 1_000_000.0 / crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE;
            (basket_unscaled / multiplier).clamp(0.0, 1.0)
        });
        BasketPrice { cost, unpriced }
    }

    /// Basket price per message on this fact's attached ephemeral vector,
    /// normalized against BASKET_REFERENCE_COST_PER_MESSAGE. Returns None when
    /// the vector is absent or below the sample floor, or when a dimension the
    /// vector uses has no rate (see [`ModelFact::basket_price`]).
    pub fn basket_cost(&self) -> Option<f64> {
        let vector = self.ephemeral_vector().filter(|v| v.is_usable())?;
        self.basket_price(vector).cost
    }
}

/// One token dimension of a basket price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasketDimension {
    FreshInput,
    OutputExcludingReasoning,
    Reasoning,
    CachedRead,
    CacheWrite,
}

impl BasketDimension {
    pub const ALL: [BasketDimension; 5] = [
        BasketDimension::FreshInput,
        BasketDimension::OutputExcludingReasoning,
        BasketDimension::Reasoning,
        BasketDimension::CachedRead,
        BasketDimension::CacheWrite,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            BasketDimension::FreshInput => "fresh_input",
            BasketDimension::OutputExcludingReasoning => "output",
            BasketDimension::Reasoning => "reasoning",
            BasketDimension::CachedRead => "cached_read",
            BasketDimension::CacheWrite => "cache_write",
        }
    }

    /// The rate field a reader would look for when this dimension is unpriced.
    pub fn rate_name(self) -> &'static str {
        match self {
            BasketDimension::FreshInput => "input_rate",
            BasketDimension::OutputExcludingReasoning => "cost_per_m_tok_out",
            BasketDimension::Reasoning => "reasoning_rate or cost_per_m_tok_out",
            BasketDimension::CachedRead => "cache_read_rate",
            BasketDimension::CacheWrite => "cache_write_rate",
        }
    }

    /// Mean tokens per message on this dimension. Output excludes reasoning and
    /// is floored at zero, as the store computes it.
    pub fn tokens(self, vector: &ClassVector) -> f64 {
        match self {
            BasketDimension::FreshInput => vector.fresh_input,
            BasketDimension::OutputExcludingReasoning => vector.output_excluding_reasoning.max(0.0),
            BasketDimension::Reasoning => vector.reasoning,
            BasketDimension::CachedRead => vector.cached_read,
            BasketDimension::CacheWrite => vector.cache_write,
        }
    }
}

/// Result of [`ModelFact::basket_price`].
#[derive(Debug, Clone, PartialEq)]
pub struct BasketPrice {
    /// Normalized basket cost (0..1), or None when a used dimension has no rate.
    pub cost: Option<f64>,
    /// Dimensions with tokens > 0 and no usable rate, in dimension order.
    pub unpriced: Vec<BasketDimension>,
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
