//! Model router: pure selection core (no persistence, no host calls).
//!
//! Ported from the Alfonso TS plugin (`src/features/model-routing/model-router.ts`).
//!
//! Pipeline (quality THRESHOLD + cost/quota/speed OBJECTIVE):
//!   1. hard admission filters (availability, capability, context, cooldown,
//!      confirmed quota exhaustion)
//!   2. quality adequacy score (saturating: meeting the bar = ~1, no over-bonus)
//!   3. unknown-fact confidence penalty (in `score_adequacy`)
//!   4. adequacy GATE: keep models clearing `ADEQUACY_FLOOR`; graceful fallback
//!      to the relative top when nothing clears (extreme demand)
//!   5. OBJECTIVE among cleared models: minimize cost + quota pressure, maximize
//!      speed (tie-break) → lowest selection cost wins
//!   6. deterministic tie-break
//!
//! Runtime state is an explicit input here; persistence lands in a later slice.
//! This module stays deterministic and unit-testable in isolation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model_facts::{
    fact_to_route, BasketDimension, BasketPrice, ClassVector, ModelFact, ModelMetric,
    ModelRequirements, ModelRoute, ScoreProvenance, CLASS_VECTOR_SAMPLE_FLOOR,
};
use crate::model_resolution::{use_basket_permitted, CLASS_PRODUCER, COUNTERS_PRODUCER};
use crate::provider_model_spec::{ExclusionPolicy, ExclusionSource, ExclusionSpecs};
use crate::route_scoring::{score_adequacy, AdequacyScore, MetricFit};
use crate::routing_config::{
    demand_bar, demand_for_score, RoutingConfig, TopRoutableScores, ADEQUACY_FLOOR,
    COST_REFERENCE_PER_MTOK, DEFAULT_ROUTING_CONFIG, EFFECTIVELY_FREE_SELECTION_COST,
    MIN_BEST_EFFORT_ADEQUACY, OBJECTIVE_WEIGHT_QUOTA, RELATIVE_SHORTLIST_DELTA, SPEED_CEILING_TPS,
    SPEED_FLOOR_TPS,
};
use crate::slot_saturation::{
    pick_unsaturated, ConcurrencySlots, SlotSaturation, REASON_PROVIDER_SATURATED,
};
use crate::task_profile::{TaskProfile, TokenVector};

/// Profiles resolved for one select's workload and intelligence-demand bucket,
/// keyed by exact route key. The caller supplies a single in-memory refresh snapshot; selection
/// never loads or recomputes task statistics.
pub type RouteTaskProfiles = HashMap<String, TaskProfile>;

/// A route the operator's standing policy fences off. Distinct from [`REASON_EXCLUDED`]
/// so a reader of a rejection can tell "the machine owner said never" from "this
/// attempt skipped it". Both the producer and every consumer that maps rejection
/// reasons use these constants, so renaming one cannot silently split the mapping.
pub const REASON_EXCLUDED_BY_OPERATOR: &str = "excluded_by_operator";
/// A route the CALLER excluded for this attempt: the route that just failed on a
/// reroute, or a consult's `exclude_route_keys`.
pub const REASON_EXCLUDED: &str = "excluded";
/// A route missing from the available-model set this selection was given. Which
/// set that was is a property of the caller, so consumers that can name it
/// attach it as the rejection's detail.
pub const REASON_NOT_AVAILABLE: &str = "not_available";
pub const REASON_RUNNER_WIRE_FAMILY_UNVERIFIED: &str = "runner_wire_family_unverified";
pub const REASON_RUNNER_REQUIRES_WIRE_API: &str = "runner_requires_wire_api";
/// A route the upstream catalog marks `deprecated` (Fusiform's `model.status`):
/// the provider no longer serves it, so no selection, panel seat or pin may
/// use it. Checked before every other fence, because no policy can make a
/// withdrawn model servable.
pub const REASON_DEPRECATED_UPSTREAM: &str = "deprecated_upstream";
/// Every credential that would serve this route got a permanent entitlement or
/// authorization refusal from the provider. The rejection names those
/// credentials in `credential_ids`; it carries no prose.
pub const REASON_CREDENTIAL_UNENTITLED: &str = "credential_unentitled";
/// Every credential that would serve this route ran out of paid balance (the
/// provider's permanent `billing_exhausted`). Named like
/// [`REASON_CREDENTIAL_UNENTITLED`], in `credential_ids`.
pub const REASON_CREDENTIAL_BILLING_EXHAUSTED: &str = "credential_billing_exhausted";

/// The permanently refused credentials that leave a route with no usable one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CredentialRefusalMark {
    /// [`REASON_CREDENTIAL_UNENTITLED`] or [`REASON_CREDENTIAL_BILLING_EXHAUSTED`].
    pub reason: &'static str,
    pub credential_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RouteRuntimeState {
    /// Request-local reachability independent of runtime availability. A known
    /// credential/transport match still anchors quality when its accounts cannot
    /// send; missing credentials do not. None uses the caller's availability set/map.
    /// This affects quality normalization only, never candidate admission.
    pub statically_routable: Option<bool>,
    /// Slow-refresh account economics, shared by every route on this provider.
    pub subscription_pool: Option<std::sync::Arc<crate::subscription::SubscriptionPool>>,
    /// Confirmed cooldown/retry-after; route inadmissible until this time (ms epoch).
    pub cooldown_until: Option<f64>,
    /// Confirmed quota exhaustion until this time (ms epoch).
    pub quota_exhausted_until: Option<f64>,
    /// Provider reset for this selection's consumer and serving identity. Unlike
    /// quota telemetry this cannot be bypassed by a speculative break-glass probe.
    pub provider_reset_until: Option<f64>,
    /// The vault credentials that would serve this route, all of which the
    /// provider refused permanently, so automatic selection rejects the route.
    /// Explicit pins clear it.
    pub credential_refusal: Option<CredentialRefusalMark>,
    /// Typed cause of the hard wall. None preserves legacy/unclassified behavior.
    pub wall_class: Option<crate::route_state_store::RouteWallClass>,
    /// Last provider error associated with the wall, retained for no-route diagnostics.
    pub last_error: Option<String>,
    /// Route-local short backoff after repeated unclassified terminal send failures.
    pub failure_backoff_until: Option<f64>,
    /// Operator-facing reason containing the consecutive count and last send error.
    pub failure_backoff_reason: Option<String>,
    /// 0..1, lower = worse recent reliability. None = unknown (neutral).
    pub success_ema: Option<f64>,
    /// Soft quota pressure 0..1 (e.g. pace projection). None = unknown.
    pub quota_pressure: Option<f64>,
    /// The pace model that produced `quota_pressure`, retained for decision audit.
    pub quota_window_model: Option<crate::quota_pace::PaceWindowModel>,
    /// Perishable quota headroom relative to time remaining. None is neutral.
    pub quota_abundance: Option<f64>,
    /// Window identity that produced `quota_abundance`.
    pub quota_abundance_window: Option<String>,
    pub latency_ema_ms: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModelRouteStateSnapshot {
    pub now: f64,
    /// Keyed by routeKey for route-specific state.
    pub by_route: HashMap<String, RouteRuntimeState>,
    /// Keyed by capacityKey for shared provider/model state.
    pub by_capacity: HashMap<String, RouteRuntimeState>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequiredCapabilities {
    #[serde(default)]
    pub vision: Option<bool>,
    #[serde(default)]
    pub min_context: Option<f64>,
}

pub(crate) fn capability_rejection_reason(
    fact: &ModelFact,
    caps: Option<RequiredCapabilities>,
) -> Option<&'static str> {
    if caps.and_then(|c| c.vision) == Some(true) && fact.supports_vision != Some(true) {
        return Some("missing_vision");
    }
    if let Some(min_context) = caps.and_then(|c| c.min_context) {
        if min_context > 0.0
            && fact
                .context_window
                .is_none_or(|context_window| context_window < min_context)
        {
            return Some("context_too_small");
        }
    }
    None
}

/// Input to [`select_model_route`]. `is_subscription_route` supplies legacy
/// subscription-policy coverage; a resolved quota-proxy billing plane also
/// establishes coverage independently of the launching host's auth metadata.
/// Covered routes use subscription economics when a runtime pool is available,
/// including the fixed keep-free margin for workers, rather than treating quota as free.
pub struct SelectModelRouteInput<'a> {
    pub target_agent: String,
    /// Whether the target agent is a head, an operator-facing session listed
    /// under the operator config's `head_agents`. A head may spend a
    /// subscription's kept-free reserve and is priced as one.
    pub target_is_head: bool,
    pub requirements: ModelRequirements,
    /// Set of available routeKeys AND/OR capacityKeys the caller is logged into.
    pub available_models: HashSet<String>,
    pub facts: Vec<ModelFact>,
    pub runtime_snapshot: Option<ModelRouteStateSnapshot>,
    /// Per-attempt exclusions supplied by the caller (a route that just failed).
    pub exclude_route_keys: Vec<String>,
    /// The operator's standing exclude list, applied as the baseline under the
    /// caller's. Union only: see [`ExclusionPolicy`]. Automatic selection passes
    /// the operator's list here; a selection that names its model passes none.
    pub operator_exclude_keys: Vec<String>,
    pub required_capabilities: Option<RequiredCapabilities>,
    pub is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
    /// Sanity band for best-effort selection. When no route clears the adequacy
    /// floor (0.9), the router will still return a route if its adequacy is ≥
    /// this value (default 0.6). Routes below the band trigger an
    /// `AllRoutesInadequate` error. `None` uses the module default.
    pub min_best_effort_adequacy: Option<f64>,
    /// Prefer providers with prompt caching for high-input-volume seats. The
    /// preference is a partition after adequacy gating, never a denial or a
    /// score adjustment.
    pub prefer_prompt_cache: bool,
    /// Provider IDs that do not offer prompt caching, loaded by the routing
    /// module from its live configuration.
    pub providers_without_prompt_cache: HashSet<String>,
    /// Empty by default; the usage-window profile refresh adds providers whose rows never report a cache-write counter.
    pub providers_without_cache_write_reports: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectiveBreakdown {
    /// No usable price for the comparison unit. Cost comparisons put priced routes first,
    /// even when a known price clamps to the maximum or has quota surcharges.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub price_unknown: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_pricing: Option<crate::subscription::SubscriptionPricing>,
    /// Normalized economic cost; quota-priced subscriptions may exceed one.
    pub cost: f64,
    /// Soft quota pressure component (0..1 · weight).
    pub quota: f64,
    /// Normalized speed (0..1, higher = faster). Tie-break only, NOT in selectionCost.
    pub speed_score: f64,
    /// cost + quota + overqualification; lower is a better pick. Speed breaks ties.
    pub selection_cost: f64,
    /// Over-qualification term already included in `selection_cost`: how far the
    /// route sits above the request's iq demand beyond the configured threshold
    /// (see `OverqualificationConfig`). Zero, and absent from the record, when
    /// the route is not over-qualified or the request has no iq demand.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub overqualification: f64,
    /// Whether the route is served under a flat subscription. Account-economics
    /// snapshots price the quota consumed; legacy snapshots retain free buckets.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub subscription_covered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_abundance: Option<f64>,
    /// Owned audit identity carried into the decision record. This allocates once
    /// per scored candidate; shortlists are normally tens of routes. If they grow
    /// by an order of magnitude, sharing provider window keys with `Arc<str>` is
    /// preferable to repeating these short strings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_abundance_window: Option<String>,
    /// How the route's cost was computed when its selection priced by the token
    /// basket: which ephemeral vector it used and which dimensions, if any, had
    /// tokens but no rate. Absent for task-profile and output-only pricing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basket_pricing: Option<BasketRoutePricing>,
    /// Task economics, including the cold-start basis when no profile serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_pricing: Option<TaskRoutePricing>,
}

/// Per-candidate evidence for the economic cost, before over-qualification.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRoutePricing {
    /// None means cold start, with zero supporting tasks. A served profile
    /// includes its level, task count, transfer and blending metadata.
    pub profile: Option<TaskProfile>,
    /// None only for the cold-start million-output-token comparison unit.
    pub tokens: Option<TokenVector>,
    pub price_unit: &'static str,
    pub list_cost_usd: Option<f64>,
    pub value_multiplier: f64,
    pub value_source: String,
    pub pressure: f64,
    pub final_cost_usd: Option<f64>,
    pub normalized_cost: f64,
    pub omitted_dimensions: Vec<String>,
    pub unpriced_dimensions: Vec<String>,
}

/// Where the token profile a route was basket-priced on came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BasketVectorSource {
    /// The route's own ephemeral vector, above the sample floor.
    Own,
    /// The fleet-wide ephemeral vector, used because the route has no usable
    /// vector of its own.
    Fleet,
}

impl BasketVectorSource {
    pub fn as_str(self) -> &'static str {
        match self {
            BasketVectorSource::Own => "own",
            BasketVectorSource::Fleet => "fleet",
        }
    }
}

/// Per-route record of how a basket price was formed.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BasketRoutePricing {
    pub vector: BasketVectorSource,
    /// Dimensions the workload uses (tokens > 0) for which the route serves no
    /// rate. Non-empty means the route could not be priced and carries the
    /// unpriced-route cost instead (see [`selection_cost`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unpriced_dimensions: Vec<String>,
}

/// The unit a whole selection was priced in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingMode {
    /// Every shortlisted route priced on a token basket.
    Basket,
    /// Every shortlisted route priced on its output $/Mtok ladder.
    OutputOnly,
}

impl PricingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PricingMode::Basket => "basket",
            PricingMode::OutputOnly => "output_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedRoute {
    pub route_key: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The refused credentials behind a `credential_unentitled` rejection.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub credential_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SelectedModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptParams {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectedRouteScore {
    pub candidates: Vec<CandidateQualityScore>,
    pub adequacy: f64,
    pub raw_adequacy: f64,
    pub selection_cost: f64,
    pub metrics: BTreeMap<ModelMetric, MetricFit>,
    pub objective: ObjectiveBreakdown,
    /// Other routes that cleared the quality gate, in this decision's ranking order.
    /// Persist even an empty list so old, unrecorded shortlists remain distinguishable.
    pub also_adequate: Vec<String>,
    /// The unit the whole selection was priced in.
    pub pricing_mode: PricingMode,
    /// Why the selection priced output-only, when it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pricing_fallback: Option<String>,
}

/// Effective quality and its source for every candidate, including routes
/// rejected before ranking. Eligibility records the decision's admission gate.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateQualityScore {
    pub route_key: String,
    pub model: String,
    pub elo: f64,
    pub eq: f64,
    pub elo_source: &'static str,
    pub eq_source: &'static str,
    pub eligible: bool,
}

impl CandidateQualityScore {
    pub fn from_fact(fact: &ModelFact, eligible: bool) -> Self {
        Self {
            model: format!("{}/{}", fact.provider_id, fact.model_id),
            eligible,
            route_key: fact_to_route(fact).route_key,
            elo: fact.scores.elo,
            eq: fact.scores.eq,
            elo_source: match fact.score_provenance {
                ScoreProvenance::OperatorGrade => "operator_grade",
                ScoreProvenance::EloOverride => "override",
                _ => "curated",
            },
            eq_source: match fact.eq_source.as_deref() {
                Some("operator_grade") => "operator_grade",
                Some("override") => "override",
                _ => "curated",
            },
        }
    }
}

fn candidate_quality_scores(
    facts: &[ModelFact],
    admitted: &[AdmittedRoute],
) -> Vec<CandidateQualityScore> {
    let mut candidates = facts
        .iter()
        .map(|fact| {
            let route_key = fact_to_route(fact).route_key;
            CandidateQualityScore::from_fact(
                fact,
                admitted.iter().any(|a| a.route.route_key == route_key),
            )
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| a.route_key.cmp(&b.route_key));
    candidates
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectedModelRoute {
    pub model: SelectedModel,
    pub route_key: String,
    pub capacity_key: String,
    pub score: SelectedRouteScore,
    pub prompt_params: PromptParams,
    pub rejected: Vec<RejectedRoute>,
    pub explanation: Vec<String>,
    /// Present when the selected route clears the best-effort band but not the
    /// adequacy floor (adequacy in [band, floor)). The caller MUST surface this
    /// warning; it is NOT optional noise.
    pub below_floor: Option<BelowFloorWarning>,
    /// Whether this selection priced its shortlist by the token basket (the
    /// whole-selection verdict) rather than today's output-price ladder. Carried
    /// so a later re-pricing of the same selection, the fast-variant upgrade,
    /// stays in the units the selection was made in, and so a decision record
    /// names its unit without re-deriving it from the explanation text.
    pub priced_with_basket: bool,
    /// Present when the caller's concurrency slot counts changed this decision:
    /// a full slot was passed over, or every adequate candidate was full and
    /// the normal winner will queue. Absent when no counts were sent or they
    /// changed nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot_saturation: Option<SlotSaturation>,
}

/// One shortlisted route priced in the selection it actually sat in.
#[derive(Debug, Clone, PartialEq)]
pub struct PricedShortlistEntry {
    pub route_key: String,
    /// Cost component of the objective (clamped 0..1), not including quota.
    pub cost: f64,
    /// Economic selection cost (cost + quota + overqualification) used by the
    /// free-bucket gate.
    pub selection_cost: f64,
    /// Over-qualification term included in `selection_cost`.
    pub overqualification: f64,
    /// Cached-read term in list dollars: `cached_read * cache_read_rate / 1e6`.
    pub cached_read_term_usd: Option<f64>,
    /// How the route was cold-start basket-priced; None for task profiles and
    /// output-only pricing.
    pub basket_pricing: Option<BasketRoutePricing>,
    pub task_pricing: Option<TaskRoutePricing>,
}

/// The shortlist `select_model_route` would price, with the whole-selection
/// basket verdict. A route is effectively free in a replay when its
/// `selection_cost` here is at or below [`EFFECTIVELY_FREE_SELECTION_COST`].
#[derive(Debug, Clone, PartialEq)]
pub struct PricedShortlist {
    pub use_basket: bool,
    /// First shortlisted route with neither a usable ephemeral vector of its
    /// own nor a usable fleet vector to fall back on. A missing rate never
    /// names a route here: it prices that one route as unpriced and leaves the
    /// selection's mode alone. Independent of the producer flags: when those
    /// flags forbid the basket, this still names the vector degrader if any.
    pub degraded_by: Option<String>,
    /// Why the selection priced output-only, when it did.
    pub pricing_fallback: Option<String>,
    pub routes: Vec<PricedShortlistEntry>,
}

/// Carried on a `SelectedModelRoute` when best-effort selection picked a route
/// whose adequacy is below the quality floor but within the sanity band.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BelowFloorWarning {
    pub adequacy: f64,
    pub floor: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteFailureReason {
    NoAvailableModel,
    MissingRequiredCapability,
    ContextTooSmall,
    QuotaOrCooldownExhausted,
    NoAdequateRoute,
    InvalidRequirements,
    /// Every admissible route's adequacy is below `min_best_effort_adequacy`
    /// (the sanity band). The router refuses to return a dangerously weak route.
    AllRoutesInadequate,
}

impl RouteFailureReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RouteFailureReason::NoAvailableModel => "no_available_model",
            RouteFailureReason::MissingRequiredCapability => "missing_required_capability",
            RouteFailureReason::ContextTooSmall => "context_too_small",
            RouteFailureReason::QuotaOrCooldownExhausted => "quota_or_cooldown_exhausted",
            RouteFailureReason::NoAdequateRoute => "no_adequate_route",
            RouteFailureReason::InvalidRequirements => "invalid_requirements",
            RouteFailureReason::AllRoutesInadequate => "all_routes_inadequate",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NoRouteError {
    pub candidates: Vec<CandidateQualityScore>,
    pub reason: RouteFailureReason,
    pub message: String,
    pub rejected: Vec<RejectedRoute>,
    pub requirements: Option<ModelRequirements>,
    /// For a quality refusal: the highest-adequacy survivor, so the caller can
    /// surface which route was closest to the required floor or sanity band.
    pub best_rejected: Option<RejectedCandidate>,
    /// Earliest known recovery of a route that otherwise passes this request's
    /// admission and adequacy gates (millisecond epoch).
    pub earliest_reset: Option<f64>,
    pub earliest_reset_route: Option<String>,
    /// Routes that clear the quality gate despite their admission fence, with
    /// the effective wall expiry when the fence is timed.
    pub adequate_blockers: std::collections::BTreeMap<String, Option<f64>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdequateBlocker {
    pub route_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_at_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockingCause {
    pub reason: String,
    pub count: usize,
    pub routes: Vec<RejectedRoute>,
    pub adequate_routes: Vec<AdequateBlocker>,
}

impl NoRouteError {
    pub fn blocking_causes(&self) -> Vec<BlockingCause> {
        let mut groups = std::collections::BTreeMap::<String, Vec<RejectedRoute>>::new();
        for route in &self.rejected {
            groups
                .entry(route.reason.clone())
                .or_default()
                .push(route.clone());
        }
        groups
            .into_iter()
            .map(|(reason, routes)| BlockingCause {
                reason,
                count: routes.len(),
                adequate_routes: routes
                    .iter()
                    .filter_map(|route| {
                        self.adequate_blockers
                            .get(&route.route_key)
                            .map(|reset| AdequateBlocker {
                                route_key: route.route_key.clone(),
                                reset_at_ms: *reset,
                            })
                    })
                    .collect(),
                routes,
            })
            .collect()
    }

    /// Report every reason selection is blocked. A keep-free margin or operator
    /// exclusion can still prevent selection after a quota/cooldown wall expires.
    pub fn refusal_message(&self) -> String {
        if self.reason != RouteFailureReason::QuotaOrCooldownExhausted {
            let mut message = self.message.clone();
            self.append_recovery(&mut message);
            return message;
        }
        let groups = self
            .blocking_causes()
            .into_iter()
            .map(|group| {
                let label = match group.reason.as_str() {
                    "excluded_by_operator" => "excluded by operator",
                    "subscription_reserve" => "below keep-free margin",
                    "quota_exhausted" => "out of credit",
                    "excluded" => "excluded for this item/caller",
                    "cooldown_active" => "cooldown active",
                    "backoff_active" => "backoff active",
                    other => other,
                };
                let mut details = std::collections::BTreeSet::new();
                for route in &group.routes {
                    let provider = route
                        .route_key
                        .split('/')
                        .next()
                        .unwrap_or(&route.route_key);
                    let mut detail = provider.to_string();
                    if let Some(raw) = &route.detail {
                        if let Some(until) = raw
                            .split("until_ms=")
                            .nth(1)
                            .and_then(|tail| tail.split([',', ')']).next())
                        {
                            detail = format!("{}, until_ms={until}", route.route_key);
                        }
                    }
                    details.insert(detail);
                }
                format!(
                    "{label}: {} ({})",
                    group.count,
                    details.into_iter().collect::<Vec<_>>().join(", ")
                )
            })
            .collect::<Vec<_>>();
        let mut message = self.message.clone();
        self.append_recovery(&mut message);
        if !groups.is_empty() {
            message.push_str(&format!(" {}.", groups.join("; ")));
        }
        message
    }

    fn append_recovery(&self, message: &mut String) {
        if let (Some(until), Some(route)) = (self.earliest_reset, &self.earliest_reset_route) {
            message.push_str(&format!(". Earliest route wall expiry: {route} (until_ms={until:.0}); other fences may remain."));
        }
    }
}

/// The best route rejected by the adequacy band, carrying its route key and
/// adequacy score so the caller can name the closest survivor in the error.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RejectedCandidate {
    pub route_key: String,
    pub adequacy: f64,
}

impl std::fmt::Display for NoRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for NoRouteError {}

fn requirements_are_valid(req: &ModelRequirements) -> bool {
    for v in [req.iq, req.eq] {
        if v.is_nan() || v < 0.0 || v > 100.0 {
            return false;
        }
    }
    true
}

fn all_zero(req: &ModelRequirements) -> bool {
    req.iq == 0.0 && req.eq == 0.0
}

fn is_route_available(route: &ModelRoute, available: &HashSet<String>) -> bool {
    available.contains(&route.route_key) || available.contains(&route.capacity_key)
}

/// The subscription subsidy multiplier in effect for a route's fact: how much
/// cheaper than list price the subscription makes it. Defaults to 1.0 ("no
/// subsidy known"). Configured values are validated to be finite and strictly
/// positive before they reach the fact, so division by the multiplier is safe;
/// anything absent, non-positive, or non-finite falls back to 1.0 rather than
/// dividing by zero or flipping the price negative.
fn subscription_multiplier(fact: &ModelFact) -> f64 {
    match fact.subscription_multiplier {
        Some(multiplier) if multiplier.is_finite() && multiplier > 0.0 => multiplier,
        _ => 1.0,
    }
}

/// Effective output $/Mtok after any subscription subsidy: the list price divided
/// by the subscription multiplier. `None` when the route has no known list price
/// (an absent price is NOT treated as free).
fn effective_cost_per_m_tok_out(fact: &ModelFact) -> Option<f64> {
    fact.basket_rate(crate::model_facts::BasketDimension::OutputExcludingReasoning)
        .map(|list_price| list_price / subscription_multiplier(fact))
}

/// Normalized metered cost (0..1) of a route REGARDLESS of subscription status:
/// the same formula the objective uses, kept as a residual tie-break. The price is
/// the EFFECTIVE price (list ÷ subscription multiplier), so a subsidized route is
/// priced below its list price while an unsubsidized one sits at list. Unknown
/// metered price maps to the max (1), NOT 0: "unknown" must not masquerade as
/// "cheapest" and win the tie-break over a genuinely cheap, known-priced model.
pub(crate) fn normalized_metered_cost(fact: &ModelFact) -> f64 {
    if let Some(per_m_tok) = effective_cost_per_m_tok_out(fact) {
        return (per_m_tok / COST_REFERENCE_PER_MTOK).clamp(0.0, 1.0);
    }
    if let Some(cost_tier) = fact.cost_tier.filter(|_| fact.billing_plane.is_none()) {
        return (cost_tier / 6.0).min(1.0);
    }
    1.0
}

pub(crate) fn latest_deadline(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// `until` and `now` are epoch milliseconds as f64. Rendered as a remaining
/// duration plus both raw values so a unit mismatch is visible at a glance.
fn wall_detail(until: f64, now: f64) -> String {
    let remaining_s = ((until - now) / 1000.0).round();
    format!("wall for {remaining_s}s more (until_ms={until:.0}, now_ms={now:.0})")
}

pub(crate) fn merged_runtime_state(
    route: &ModelRoute,
    snapshot: Option<&ModelRouteStateSnapshot>,
) -> RouteRuntimeState {
    let by_route = snapshot.and_then(|s| s.by_route.get(&route.route_key));
    let by_capacity = snapshot.and_then(|s| s.by_capacity.get(&route.capacity_key));
    let mut merged = by_capacity.cloned().unwrap_or_default();
    if let Some(route_state) = by_route {
        merged.statically_routable =
            match (merged.statically_routable, route_state.statically_routable) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), _) | (_, Some(true)) => Some(true),
                _ => None,
            };
        // Hard blocks compose across both scopes: a stale route deadline cannot
        // shorten a live capacity-wide block. The wall's typed cause and error text
        // are not merged here: break-glass reads them per scope from the snapshot
        // (live_billing_wall_error), and no selection arm reads the merged copy.
        merged.cooldown_until = latest_deadline(merged.cooldown_until, route_state.cooldown_until);
        merged.quota_exhausted_until = latest_deadline(
            merged.quota_exhausted_until,
            route_state.quota_exhausted_until,
        );
        merged.provider_reset_until = latest_deadline(
            merged.provider_reset_until,
            route_state.provider_reset_until,
        );
        if merged.credential_refusal.is_none() {
            merged.credential_refusal = route_state.credential_refusal.clone();
        }
        merged.failure_backoff_until = route_state.failure_backoff_until;
        merged.failure_backoff_reason = route_state.failure_backoff_reason.clone();
        // Soft telemetry remains route-specific when present.
        if route_state.success_ema.is_some() {
            merged.success_ema = route_state.success_ema;
        }
        if route_state.quota_pressure.is_some() {
            merged.quota_pressure = route_state.quota_pressure;
            merged.quota_window_model = route_state.quota_window_model;
        }
        if route_state.latency_ema_ms.is_some() {
            merged.latency_ema_ms = route_state.latency_ema_ms;
        }
    }
    merged
}

/// When a route that admission rejects for a timed wall (`cooldown_active`,
/// `quota_exhausted`, `backoff_active`) becomes admissible again: the latest
/// still-future deadline among the walls admission checks, read from the route
/// and its capacity the way admission merges them. `None` when no wall is live,
/// so nothing names a time the route recovers.
pub fn route_wall_until(
    snapshot: &ModelRouteStateSnapshot,
    route_key: &str,
    capacity_key: &str,
) -> Option<f64> {
    let by_route = snapshot.by_route.get(route_key);
    let by_capacity = snapshot.by_capacity.get(capacity_key);
    // Admission composes the hard walls of both scopes and reads the failure
    // backoff from the route when it has state (see `merged_runtime_state`).
    let backoff = match by_route {
        Some(route) => route.failure_backoff_until,
        None => by_capacity.and_then(|capacity| capacity.failure_backoff_until),
    };
    [by_route, by_capacity]
        .into_iter()
        .flatten()
        .flat_map(|state| {
            [
                state.cooldown_until,
                state.quota_exhausted_until,
                state.provider_reset_until,
            ]
        })
        .chain([backoff])
        .flatten()
        .filter(|until| *until > snapshot.now)
        .max_by(f64::total_cmp)
}

#[derive(Debug, Clone)]
pub struct AdmittedRoute {
    pub route: ModelRoute,
    pub adequacy: AdequacyScore,
    pub runtime: RouteRuntimeState,
}

#[derive(Debug, Clone)]
pub struct AdmissionResult {
    pub admitted: Vec<AdmittedRoute>,
    pub rejected: Vec<RejectedRoute>,
    pub top_routable: TopRoutableScores,
}

/// Apply the shared hard-admission boundary used by singular, panel, and seat routing.
/// The maximum quality scores exclude unreachable routes and capability
/// mismatches, but not runtime availability, quota or cooldown state.
pub fn admit_routes(
    facts: &[ModelFact],
    runtime_snapshot: Option<&ModelRouteStateSnapshot>,
    exclusions: ExclusionPolicy<'_>,
    required_capabilities: Option<RequiredCapabilities>,
    available_models: &HashSet<String>,
    requirements: &ModelRequirements,
    config: &RoutingConfig,
) -> AdmissionResult {
    admit_routes_with_operator_group(
        facts,
        runtime_snapshot,
        exclusions,
        &[],
        required_capabilities,
        available_models,
        requirements,
        config,
    )
}

/// Admit routes using both the operator's global exclusions and a consumer's
/// exclusions. Each list is resolved separately, so neither can lift the other.
pub fn admit_routes_with_operator_group(
    facts: &[ModelFact],
    runtime_snapshot: Option<&ModelRouteStateSnapshot>,
    exclusions: ExclusionPolicy<'_>,
    operator_group: &[String],
    required_capabilities: Option<RequiredCapabilities>,
    available_models: &HashSet<String>,
    requirements: &ModelRequirements,
    config: &RoutingConfig,
) -> AdmissionResult {
    admit_routes_with_availability(
        facts,
        runtime_snapshot,
        exclusions,
        operator_group,
        required_capabilities,
        available_models,
        requirements,
        config,
        &HashMap::new(),
    )
}

/// The `availability_rejections` map uses exact route keys. A key in that map
/// is rejected as `not_available` even when its route or capacity key is
/// available; the map's value becomes the rejection detail.
/// A capacity key covers every variant of a model, but a route-local refusal
/// must still be able to deny one variant without denying its siblings.
pub fn admit_routes_with_availability(
    facts: &[ModelFact],
    runtime_snapshot: Option<&ModelRouteStateSnapshot>,
    exclusions: ExclusionPolicy<'_>,
    operator_group: &[String],
    required_capabilities: Option<RequiredCapabilities>,
    available_models: &HashSet<String>,
    requirements: &ModelRequirements,
    config: &RoutingConfig,
    availability_rejections: &HashMap<String, String>,
) -> AdmissionResult {
    let now = runtime_snapshot.map(|s| s.now).unwrap_or_else(now_millis);
    let exclusion_specs = ExclusionSpecs::parse(exclusions).with_operator_group(operator_group);
    let mut static_candidates = Vec::new();
    let mut top_routable = TopRoutableScores::default();
    let mut rejected = Vec::new();

    for fact in facts {
        let route = fact_to_route(fact);
        if fact.is_deprecated_upstream() {
            rejected.push(RejectedRoute {
                detail: Some(format!(
                    "{} is marked deprecated upstream; the provider no longer serves it",
                    route.route_key
                )),
                route_key: route.route_key,
                reason: REASON_DEPRECATED_UPSTREAM.to_string(),
                credential_ids: Vec::new(),
            });
            continue;
        }
        // Catalog-role refusals are structural, not account availability. Keep
        // their reason even for an ungraded model or a route with no account.
        if let Some(reason) = crate::model_facts::llm_classification_rejection(fact, true)
            .or_else(|| {
                availability_rejections
                    .get(&route.route_key)
                    .or_else(|| availability_rejections.get(&route.capacity_key))
                    .filter(|reason| {
                        reason.starts_with("not_chat_model:")
                            || reason.starts_with("not_chat_kind:")
                    })
                    .cloned()
            })
        {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason,
                detail: None,
                credential_ids: Vec::new(),
            });
            continue;
        }
        if fact.score_provenance == ScoreProvenance::Unscored {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "unscored_awaiting_placement".to_string(),
                detail: None,
                credential_ids: Vec::new(),
            });
            continue;
        }
        // Exclusion is checked BEFORE availability so an operator-fenced route
        // reports the fence as its reason. The fence also filters the caller's
        // available-model set upstream, and "not_available" there would hide why.
        if let Some(source) = exclusion_specs.source_for(&route.route_key) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: match source {
                    ExclusionSource::Operator => REASON_EXCLUDED_BY_OPERATOR.to_string(),
                    ExclusionSource::Caller => REASON_EXCLUDED.to_string(),
                },
                detail: None,
                credential_ids: Vec::new(),
            });
            continue;
        }
        let runtime = merged_runtime_state(&route, runtime_snapshot);
        let unavailable = availability_rejections.contains_key(&route.route_key)
            || !is_route_available(&route, available_models);
        let capability_rejection = capability_rejection_reason(fact, required_capabilities);
        // Preserve availability's rejection precedence, while computing quality
        // against every statically routable model, even when its accounts cannot send.
        if runtime.statically_routable.unwrap_or(!unavailable) && capability_rejection.is_none() {
            top_routable.include(fact.scores);
        }
        if unavailable {
            let refusal = runtime.credential_refusal.as_ref();
            let runner_rejection = availability_rejections
                .get(&route.route_key)
                .filter(|reason| {
                    matches!(
                        reason.as_str(),
                        REASON_RUNNER_WIRE_FAMILY_UNVERIFIED | REASON_RUNNER_REQUIRES_WIRE_API
                    )
                });
            rejected.push(RejectedRoute {
                detail: if refusal.is_some() || runner_rejection.is_some() {
                    None
                } else {
                    availability_rejections.get(&route.route_key).cloned()
                },
                route_key: route.route_key,
                reason: refusal
                    .map_or(
                        runner_rejection.map_or(REASON_NOT_AVAILABLE, String::as_str),
                        |mark| mark.reason,
                    )
                    .to_string(),
                credential_ids: refusal.map_or_else(Vec::new, |mark| mark.credential_ids.clone()),
            });
            continue;
        }
        if let Some(reason) = capability_rejection {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: reason.to_string(),
                detail: None,
                credential_ids: Vec::new(),
            });
            continue;
        }
        static_candidates.push((fact, route));
    }

    let mut admitted = Vec::new();
    for (fact, route) in static_candidates {
        let runtime = merged_runtime_state(&route, runtime_snapshot);
        // A refused credential is not a wall with a deadline: nothing but a change
        // to the credential's vault listing can lift it, so it is checked first.
        if let Some(mark) = runtime.credential_refusal.as_ref() {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: mark.reason.to_string(),
                detail: None,
                credential_ids: mark.credential_ids.clone(),
            });
            continue;
        }
        // A wall rejection names the deadline it was compared against. Without it a
        // ledger row reading `quota_exhausted` cannot be told apart from a stale stored
        // wall, a live overlay wall, or a clock in the wrong unit, and the operator's
        // own quota view showing free capacity has nothing to argue with.
        if let Some(until) = runtime.cooldown_until.filter(|until| *until > now) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "cooldown_active".to_string(),
                detail: Some(wall_detail(until, now)),
                credential_ids: Vec::new(),
            });
            continue;
        }
        let quota_until = runtime.quota_exhausted_until;
        let quota_until = latest_deadline(quota_until, runtime.provider_reset_until);
        if let Some(until) = quota_until.filter(|until| *until > now) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "quota_exhausted".to_string(),
                detail: Some(wall_detail(until, now)),
                credential_ids: Vec::new(),
            });
            continue;
        }
        if runtime
            .failure_backoff_until
            .is_some_and(|until| until > now)
        {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "backoff_active".to_string(),
                detail: runtime.failure_backoff_reason.clone(),
                credential_ids: Vec::new(),
            });
            continue;
        }
        admitted.push(AdmittedRoute {
            route,
            adequacy: score_adequacy(fact, requirements, top_routable, config),
            runtime,
        });
    }

    AdmissionResult {
        admitted,
        rejected,
        top_routable,
    }
}

/// Normalized speed 0..1 over the fixed tie-break range.
fn normalized_speed(speed: f64) -> f64 {
    ((speed - SPEED_FLOOR_TPS) / (SPEED_CEILING_TPS - SPEED_FLOOR_TPS)).clamp(0.0, 1.0)
}

/// Cached-read term in list dollars on `vector`. None when there is no vector
/// or the cache-read rate is missing; an explicit zero rate is a present term
/// of $0.
fn cached_read_term_usd(fact: &ModelFact, vector: Option<&ClassVector>) -> Option<f64> {
    let vector = vector?;
    let rate = fact.basket_rate(crate::model_facts::BasketDimension::CachedRead)?;
    Some((vector.cached_read * rate) / 1_000_000.0)
}

/// The comparison unit for one route, decided for the whole selection.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CostBasis<'v> {
    /// The output $/Mtok ladder (effective price, tier, unpriced rule).
    OutputOnly,
    /// A token basket on `vector`, which came from `source`.
    Basket {
        vector: &'v ClassVector,
        source: BasketVectorSource,
    },
    TaskProfile {
        profile: &'v TaskProfile,
        omit_cache_write: bool,
    },
}

/// The output-only ladder: effective output price, then tier, then the
/// unpriced-route rule.
fn output_only_cost(fact: &ModelFact, _is_subscription: bool) -> f64 {
    normalized_metered_cost(fact)
}

fn output_price_unknown(fact: &ModelFact) -> bool {
    effective_cost_per_m_tok_out(fact).is_none()
}

/// The basket cost of one route on `vector`, with the dimension-level detail.
/// A route that cannot be priced (a dimension with tokens and no rate) takes
/// the unpriced-route cost (1.0), regardless of subscription coverage. Only that
/// route is affected; the selection keeps its basket mode.
fn basket_route_cost(
    fact: &ModelFact,
    _is_subscription: bool,
    vector: &ClassVector,
) -> (f64, BasketPrice) {
    let price = fact.basket_price(vector);
    let cost = match price.cost {
        Some(cost) => cost,
        None => 1.0,
    };
    (cost, price)
}

/// Selection cost among models that already clear the quality bar. Lower wins.
/// COST-PRIMARY: selectionCost is purely economic (continuous effective cost +
/// soft quota pressure). Speed is NOT blended in: it is a strict tie-break.
///
/// Cost is a property of the ROUTE, read from its model fact: every route is
/// priced at its effective output $/Mtok (list price ÷ subscription multiplier),
/// normalized against the reference price. Subscription coverage no longer zeroes
/// the cost term — it is reported in the explanation only, so two subscription
/// routes with different list prices are no longer indistinguishable. The
/// explanation carries the effective price, the list price, the multiplier when it
/// is not 1.0, and whether the route is subscription-covered, so a reader can tell
/// "cheap because subsidized" from "cheap because it is a cheap model".
/// Subscription coverage is not evidence of zero consumption. An unpublished
/// price takes the maximum proxy cost and sorts after every priced candidate.
///
/// Under the basket (`use_basket`), the route is priced on its attached
/// ephemeral vector when that vector clears the sample floor, dimension by
/// dimension: a dimension the workload never uses needs no rate, and a route
/// with a used dimension it has no rate for takes the unpriced-route cost above.
/// Without a usable vector the output-only ladder prices it.
pub fn selection_cost(fact: &ModelFact, is_subscription: bool, use_basket: bool) -> f64 {
    if use_basket {
        if let Some(vector) = fact.ephemeral_vector().filter(|v| v.is_usable()) {
            return basket_route_cost(fact, is_subscription, vector).0;
        }
    }
    output_only_cost(fact, is_subscription)
}

/// Whether this route has no separate cache-write rate and can use its input rate
/// instead. Billing-plane facts are authoritative, so a fact-column value cannot
/// fill a missing rate when the attached plane omits it.
fn cache_write_rate_uses_input(fact: &ModelFact) -> bool {
    let write_rate_missing = match &fact.billing_plane {
        Some(plane) => !plane
            .rates
            .contains_key(crate::model_facts::BasketDimension::CacheWrite.fact_key()),
        None => fact.cache_write_rate.is_none(),
    };
    write_rate_missing
        && fact
            .basket_rate(crate::model_facts::BasketDimension::FreshInput)
            .is_some_and(|rate| rate.is_finite())
}

pub(crate) fn compute_objective(
    route: &ModelRoute,
    runtime: &RouteRuntimeState,
    is_subscription: bool,
    basis: CostBasis<'_>,
    head: bool,
) -> (ObjectiveBreakdown, Vec<String>) {
    if let CostBasis::TaskProfile {
        profile,
        omit_cache_write,
    } = basis
    {
        return compute_profile_objective(
            route,
            runtime,
            is_subscription,
            profile,
            omit_cache_write,
            head,
        );
    }
    let mut notes: Vec<String> = Vec::new();

    let multiplier = subscription_multiplier(&route.fact);
    let coverage = if is_subscription {
        "subscription-covered"
    } else {
        "metered"
    };
    let mut basket_pricing = None;
    let (cost, detail) = if let CostBasis::Basket { vector, source } = basis {
        let (cost, price) = basket_route_cost(&route.fact, is_subscription, vector);
        let quantity = if is_subscription {
            "basket-per-message quota proxy"
        } else {
            "list dollars"
        };
        let mut detail = format!("basket pricing ({quantity})");
        if price.unpriced.is_empty() {
            if let Some(cr) = cached_read_term_usd(&route.fact, Some(vector)) {
                detail.push_str(&format!(", cached-read term ${:.4}", cr));
            }
        } else {
            // Name every used dimension the catalog row serves no rate for, so a
            // route losing on cost because it could not be priced does not read
            // like one losing because it is expensive.
            let missing = price
                .unpriced
                .iter()
                .map(|dimension| {
                    format!(
                        "{} {:.0} tokens/message, no {}",
                        dimension.as_str(),
                        dimension.tokens(vector),
                        dimension.rate_name()
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            detail.push_str(&format!(
                ", price unknown, unpriceable, ranked as max ({missing})"
            ));
        }
        if vector.cache_write > 0.0 && cache_write_rate_uses_input(&route.fact) {
            detail.push_str(", cache-write rate taken from input");
        }
        detail.push_str(&format!(
            ", {} vector ({} samples)",
            source.as_str(),
            vector.sample_count
        ));
        if source == BasketVectorSource::Fleet {
            detail.push_str(", no usable own vector");
        }
        if (multiplier - 1.0).abs() > f64::EPSILON {
            detail.push_str(&format!(", x{} subsidy", fmt_num(multiplier)));
        }
        detail.push_str(&format!(", {coverage}"));
        basket_pricing = Some(BasketRoutePricing {
            vector: source,
            unpriced_dimensions: price
                .unpriced
                .iter()
                .map(|dimension| dimension.as_str().to_string())
                .collect(),
        });
        (cost, detail)
    } else {
        (
            output_only_cost(&route.fact, is_subscription),
            output_only_detail(route, is_subscription, multiplier, coverage),
        )
    };
    notes.push(format!("cost +{cost:.2} ({detail})"));
    if let Some(plane) = &route.fact.billing_plane {
        notes.push(format!(
            "billing plane: {}/{} ({}, serving method: {})",
            plane.provider_id, plane.auth_method, plane.kind, plane.selection_reason
        ));
    }

    let (mut objective, mut notes) =
        objective_tail(route, runtime, is_subscription, cost, basket_pricing, notes);
    objective.price_unknown = match basis {
        CostBasis::Basket { vector, .. } => route.fact.basket_price(vector).cost.is_none(),
        CostBasis::OutputOnly => output_price_unknown(&route.fact),
        CostBasis::TaskProfile { .. } => unreachable!("task profiles were priced above"),
    };
    if is_subscription {
        if let Some(pool) = &runtime.subscription_pool {
            // Compute list dollars before normalization or subsidy. A clamped
            // basket score cannot be inverted into its original message price.
            let (list_usd, reference) = match basis {
                CostBasis::Basket { vector, .. } => {
                    let mut dollars = 0.0;
                    let mut priced = true;
                    for dimension in crate::model_facts::BasketDimension::ALL {
                        let tokens = dimension.tokens(vector);
                        if tokens > 0.0 {
                            if let Some(rate) =
                                route.fact.basket_rate(dimension).filter(|r| r.is_finite())
                            {
                                dollars += tokens * rate / 1_000_000.0;
                            } else {
                                priced = false;
                            }
                        }
                    }
                    (
                        priced.then_some(dollars),
                        crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE,
                    )
                }
                // Without a usable workload vector the common comparison unit
                // remains one million output tokens, not an invented message.
                CostBasis::OutputOnly => (
                    route
                        .fact
                        .basket_rate(crate::model_facts::BasketDimension::OutputExcludingReasoning),
                    crate::routing_config::COST_REFERENCE_PER_MTOK,
                ),
                CostBasis::TaskProfile { .. } => unreachable!("task profiles were priced above"),
            };
            if let Some(list_usd) = list_usd {
                if let Some(mut pricing) = pool.price(
                    list_usd,
                    route.fact.subscription_multiplier,
                    pool.keep_free_percent,
                    head,
                ) {
                    if matches!(basis, CostBasis::OutputOnly) {
                        pricing.price_unit = "million output tokens";
                    }
                    notes.retain(|note| {
                        !note.starts_with("cost +")
                            && !note.starts_with("quota +")
                            && !note.starts_with("quota signal:")
                            && !note.starts_with("quota abundance")
                    });
                    notes.push(pricing.explanation());
                    objective.cost = pricing.dollars() / reference;
                    objective.quota = 0.0;
                    objective.selection_cost = objective.cost;
                    objective.subscription_pricing = Some(pricing);
                    objective.price_unknown = false;
                }
            }
        }
    }
    if objective.price_unknown {
        notes.push(price_unknown_note(route));
    }
    (objective, notes)
}

fn profile_list_price(
    fact: &ModelFact,
    tokens: TokenVector,
    omit_cache_write: bool,
) -> (Option<f64>, Vec<String>, Vec<String>) {
    let mut dollars = 0.0;
    let mut omitted = Vec::new();
    let mut unpriced = Vec::new();
    for dimension in BasketDimension::ALL {
        // A transferred profile may contain cache writes, but cannot establish
        // consumption when the provider's usage window never reports that counter.
        if dimension == BasketDimension::CacheWrite && omit_cache_write {
            omitted.push(dimension.as_str().to_string());
            continue;
        }
        let quantity = tokens.dimension(dimension);
        if !quantity.is_finite() || quantity < 0.0 {
            unpriced.push(dimension.as_str().to_string());
        } else if quantity > 0.0 {
            if let Some(rate) = fact
                .basket_rate(dimension)
                .filter(|r| r.is_finite() && *r >= 0.0)
            {
                dollars += quantity * rate / 1_000_000.0;
            } else {
                unpriced.push(dimension.as_str().to_string());
            }
        }
    }
    (
        (unpriced.is_empty() && dollars.is_finite()).then_some(dollars),
        omitted,
        unpriced,
    )
}

fn compute_profile_objective(
    route: &ModelRoute,
    runtime: &RouteRuntimeState,
    is_subscription: bool,
    profile: &TaskProfile,
    omit_cache_write: bool,
    head: bool,
) -> (ObjectiveBreakdown, Vec<String>) {
    let (list_cost_usd, omitted_dimensions, unpriced_dimensions) =
        profile_list_price(&route.fact, profile.tokens, omit_cache_write);
    // The empty pool uses the same multiplier resolver and pressure=1 path as an account
    // snapshot with no economics, rather than restoring the legacy free bucket.
    let empty_pool = crate::subscription::SubscriptionPool::default();
    let pool = runtime.subscription_pool.as_deref().unwrap_or(&empty_pool);
    let pricing = if is_subscription {
        list_cost_usd.and_then(|list| {
            pool.price(
                list,
                route.fact.subscription_multiplier,
                pool.keep_free_percent,
                head,
            )
        })
    } else {
        None
    };
    let final_cost_usd = if is_subscription {
        pricing.as_ref().map(|p| p.dollars())
    } else {
        list_cost_usd
    };
    let cost = final_cost_usd
        .map(|dollars| dollars / crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE)
        .unwrap_or(1.0);
    let (mut objective, mut notes) =
        objective_tail(route, runtime, is_subscription, cost, None, Vec::new());
    // Plan pressure is already in the dollar price. No independent legacy quota
    // surcharge applies to a task estimate, including one with no account pool.
    objective.quota = 0.0;
    objective.selection_cost = cost;
    objective.price_unknown = final_cost_usd.is_none();
    notes.retain(|note| !note.starts_with("quota +") && !note.starts_with("quota signal:"));
    let (value_multiplier, value_source, pressure) = pricing
        .as_ref()
        .map(|p| (p.value_multiplier, p.value_source.clone(), p.plan_pressure))
        .unwrap_or_else(|| {
            if is_subscription {
                let (value, source) = pool
                    .price(
                        0.0,
                        route.fact.subscription_multiplier,
                        pool.keep_free_percent,
                        head,
                    )
                    .map(|p| (p.value_multiplier, p.value_source))
                    .unwrap_or_else(|| {
                        crate::subscription::resolve_value(
                            route.fact.subscription_multiplier,
                            None,
                            pool.now_ms,
                        )
                    });
                (value, source, 1.0)
            } else {
                (1.0, "pay-per-token".to_string(), 1.0)
            }
        });
    objective.subscription_pricing = pricing.map(|mut p| {
        p.price_unit = "task";
        p
    });
    objective.task_pricing = Some(TaskRoutePricing {
        profile: Some(profile.clone()),
        tokens: Some(profile.tokens),
        price_unit: "task",
        list_cost_usd,
        value_multiplier,
        value_source,
        pressure,
        final_cost_usd,
        normalized_cost: cost,
        omitted_dimensions,
        unpriced_dimensions,
    });
    if let Some(plane) = &route.fact.billing_plane {
        notes.push(format!(
            "billing plane: {}/{} ({}, serving method: {})",
            plane.provider_id, plane.auth_method, plane.kind, plane.selection_reason
        ));
    }
    if objective.price_unknown {
        notes.push(price_unknown_note(route));
    }
    (objective, notes)
}

fn record_cold_start_pricing(
    objective: &mut ObjectiveBreakdown,
    fact: &ModelFact,
    basis: CostBasis<'_>,
    is_subscription: bool,
) {
    let tokens = match basis {
        CostBasis::Basket { vector, .. } => Some(TokenVector {
            fresh_input: vector.fresh_input,
            cached_read: vector.cached_read,
            cache_write: vector.cache_write,
            output_excluding_reasoning: vector.output_excluding_reasoning,
            reasoning: vector.reasoning,
        }),
        CostBasis::OutputOnly => None,
        CostBasis::TaskProfile { .. } => unreachable!("only cold-start bases are recorded here"),
    };
    let list_cost_usd = if let Some(tokens) = tokens {
        tokens.list_cost(fact)
    } else {
        fact.basket_rate(BasketDimension::OutputExcludingReasoning)
    };
    let reference = if tokens.is_some() {
        crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE
    } else {
        COST_REFERENCE_PER_MTOK
    };
    let (value_multiplier, value_source, pressure) =
        if let Some(p) = &objective.subscription_pricing {
            (p.value_multiplier, p.value_source.clone(), p.plan_pressure)
        } else {
            (
                subscription_multiplier(fact),
                "cold-start effective rate".to_string(),
                1.0,
            )
        };
    objective.task_pricing = Some(TaskRoutePricing {
        profile: None,
        tokens,
        price_unit: if tokens.is_some() {
            "message"
        } else {
            "million output tokens"
        },
        list_cost_usd,
        value_multiplier,
        value_source: if is_subscription {
            value_source
        } else {
            "cold-start effective rate".to_string()
        },
        pressure,
        final_cost_usd: (!objective.price_unknown).then_some(objective.cost * reference),
        normalized_cost: objective.cost,
        omitted_dimensions: Vec::new(),
        unpriced_dimensions: objective
            .basket_pricing
            .as_ref()
            .map(|p| p.unpriced_dimensions.clone())
            .unwrap_or_default(),
    });
}

/// Why a route has no known price, in the words a decision reader needs.
///
/// On a plan whose serving credential Fusiform publishes no rates for (the
/// `no_coverage` fallback plane on a non-apikey method), the price is unknown
/// only because nothing states what the plan bills: the route is then priced at
/// list rates when its provider has an operator-configured subscription
/// multiplier, and unpriced otherwise.
fn price_unknown_note(route: &ModelRoute) -> String {
    let uncovered_plan =
        route.fact.billing_plane.as_ref().is_some_and(|plane| {
            plane.kind == "no_coverage fallback" && plane.auth_method != "apikey"
        });
    if uncovered_plan {
        format!(
            "price_unknown: {} has no plan pricing and no subscription multiplier; ranked after known prices",
            route.fact.provider_id
        )
    } else {
        "price_unknown: no effective price; ranked after known prices".into()
    }
}

/// The cost line's detail on the output-only ladder.
fn output_only_detail(
    route: &ModelRoute,
    _is_subscription: bool,
    multiplier: f64,
    coverage: &str,
) -> String {
    if let Some(list_price) = route
        .fact
        .basket_rate(crate::model_facts::BasketDimension::OutputExcludingReasoning)
    {
        let effective = list_price / multiplier;
        let mut detail = format!(
            "output $/Mtok: effective ${}/Mtok, list ${}/Mtok",
            fmt_num(effective),
            fmt_num(list_price)
        );
        if (multiplier - 1.0).abs() > f64::EPSILON {
            detail.push_str(&format!(", x{} subsidy", fmt_num(multiplier)));
        }
        detail.push_str(&format!(", {coverage}"));
        detail
    } else if let Some(tier) = route
        .fact
        .cost_tier
        .filter(|_| route.fact.billing_plane.is_none())
    {
        format!("tier {}, {coverage}", fmt_num(tier))
    } else {
        // An unpriced metered route is ranked at the MAXIMUM cost (see
        // `selection_cost`), never as free. Say so on the line: a route losing
        // every cost comparison because nobody knows its price looks identical
        // to one losing because it is genuinely expensive.
        format!("price unknown (ranked as max), {coverage}")
    }
}

pub(crate) fn subscription_budget_available(runtime: &RouteRuntimeState, head: bool) -> bool {
    runtime
        .subscription_pool
        .as_ref()
        .is_none_or(|pool| pool.available(pool.keep_free_percent, head))
}

/// A quota-proxy billing plane identifies the subscription credential that
/// actually serves the route. A launching host's missing OAuth metadata must
/// not turn that shared quota into metered capacity and bypass the keep-free margin.
/// The policy predicate still covers subscriptions without a resolved plane.
pub(crate) fn subscription_route_covered(
    route: &ModelRoute,
    predicate: Option<&dyn Fn(&ModelRoute) -> bool>,
) -> bool {
    route
        .fact
        .billing_plane
        .as_ref()
        .is_some_and(|plane| plane.kind == "quota_proxy")
        || predicate.is_some_and(|pred| pred(route))
}

/// Everything in the objective after the cost line: quota terms, abundance,
/// and the speed tie-break.
fn objective_tail(
    route: &ModelRoute,
    runtime: &RouteRuntimeState,
    is_subscription: bool,
    cost: f64,
    basket_pricing: Option<BasketRoutePricing>,
    mut notes: Vec<String>,
) -> (ObjectiveBreakdown, Vec<String>) {
    // A subscription route with NO pressure reading must say so, or a decision
    // reader cannot tell "known to be idle" from "nothing was measured" - and
    // both land on the same 0.0 in the term below.
    //
    // This used to key on a `governing_window` that nothing in production ever
    // wrote, so it fired on every subscription route including the ones whose
    // pressure was known: 411 of 411 decisions in 24h printed it, several while
    // carrying a real pressure value from the same signal. It also named a cause
    // ("source stale/down") that absence cannot establish - never instrumented,
    // a gapped feed, and a refused projection are three different reasons and
    // this side sees none of them. Say only what the reading shows.
    if is_subscription && runtime.quota_pressure.is_none() {
        notes.push("quota signal: none (no reading)".to_string());
    }

    let quota = runtime.quota_pressure.unwrap_or(0.0) * OBJECTIVE_WEIGHT_QUOTA;
    if quota > 0.0 {
        notes.push(format!("quota +{:.2}", quota));
    }

    let quota_abundance = runtime.quota_abundance;
    let quota_abundance_window = runtime.quota_abundance_window.clone();
    if let (Some(abundance), Some(window)) = (quota_abundance, quota_abundance_window.as_deref()) {
        notes.push(format!("quota abundance {abundance:.4} (window {window})"));
    }

    let speed_score = normalized_speed(route.fact.scores.speed);

    let selection_cost = cost + quota;
    let speed_note = if let Some(mult) = route.fact.speed_multiplier {
        if (mult - 1.0).abs() > f64::EPSILON {
            format!(
                "speed {:.2} (x{} configured) (tiebreak)",
                speed_score,
                fmt_num(mult)
            )
        } else {
            format!("speed {:.2} (tiebreak)", speed_score)
        }
    } else {
        format!("speed {:.2} (tiebreak)", speed_score)
    };
    notes.push(speed_note);
    (
        ObjectiveBreakdown {
            price_unknown: false,
            subscription_pricing: None,
            cost,
            quota,
            speed_score,
            selection_cost,
            overqualification: 0.0,
            subscription_covered: is_subscription,
            quota_abundance,
            quota_abundance_window,
            basket_pricing,
            task_pricing: None,
        },
        notes,
    )
}

fn now_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn is_zero(value: &f64) -> bool {
    *value == 0.0
}

/// Adds the over-qualification term to one priced route. The route's iq is its
/// elo expressed on the request's own 0..100 demand scale: the inverse of the
/// bar mapping against the same frontier (`top_routable`), so a route at the
/// frontier is 100. A request with no iq demand has no iq bar to be above, so
/// it carries no term.
fn apply_overqualification(
    objective: &mut ObjectiveBreakdown,
    notes: &mut Vec<String>,
    route: &ModelRoute,
    requirements: &ModelRequirements,
    top_routable: TopRoutableScores,
    config: &RoutingConfig,
) {
    if requirements.iq <= 0.0 {
        return;
    }
    let model_iq = demand_for_score(
        top_routable.elo,
        route.fact.scores.elo,
        config.demand_exponent,
    );
    let surplus = model_iq - requirements.iq;
    let term = config.overqualification.term(surplus);
    if term <= 0.0 {
        return;
    }
    objective.overqualification = term;
    objective.selection_cost += term;
    // Right after the cost line, the term it adds to.
    let at = notes.len().min(1);
    notes.insert(
        at,
        format!(
            "overqualified +{term:.2} (surplus {surplus:.0} over {:.0})",
            requirements.iq
        ),
    );
}

/// Select the best model route for a task. Pure and deterministic.
/// Returns `Err(NoRouteError)` with a typed reason when nothing is
/// admissible/adequate.
pub fn select_model_route(
    input: SelectModelRouteInput<'_>,
) -> Result<SelectedModelRoute, NoRouteError> {
    select_model_route_with_config(input, &DEFAULT_ROUTING_CONFIG)
}

struct Scored {
    idx: usize,
    objective: ObjectiveBreakdown,
    objective_notes: Vec<String>,
    /// Cached-read term on the vector the route was priced on (or, when the
    /// selection priced output-only, on the fact's attached vector).
    cached_read_term_usd: Option<f64>,
}

struct PricedAdmission {
    admissible: Vec<AdmittedRoute>,
    rejected: Vec<RejectedRoute>,
    scored: Vec<Scored>,
    use_basket: bool,
    degraded_by: Option<String>,
    pricing_fallback: Option<String>,
    best_effort: bool,
    cleared_count: usize,
}

/// The fleet-wide ephemeral vector: the overlay the routing store lays over
/// every fact (one whole-population statistic, so every overlaid fact carries
/// the same one). When facts disagree, the one with the most samples is taken;
/// the first such fact wins a tie so the choice does not depend on anything
/// but input order.
fn fleet_ephemeral_vector(facts: &[ModelFact]) -> Option<&ClassVector> {
    facts
        .iter()
        .filter_map(ModelFact::fleet_ephemeral_vector)
        .fold(None, |best: Option<&ClassVector>, candidate| match best {
            Some(best) if best.sample_count >= candidate.sample_count => Some(best),
            _ => Some(candidate),
        })
}

/// The vector one route is basket-priced on: its own when that clears the
/// sample floor, else the fleet vector when that does.
fn route_basket_vector<'v>(
    fact: &'v ModelFact,
    fleet: Option<&'v ClassVector>,
) -> Option<(&'v ClassVector, BasketVectorSource)> {
    if let Some(own) = fact.own_ephemeral_vector().filter(|v| v.is_usable()) {
        return Some((own, BasketVectorSource::Own));
    }
    fleet
        .filter(|v| v.is_usable())
        .map(|v| (v, BasketVectorSource::Fleet))
}

/// Why no usable vector exists for a route that has none of its own.
fn fleet_vector_unusable_reason(fleet: Option<&ClassVector>) -> String {
    match fleet {
        None => "no fleet ephemeral vector".to_string(),
        Some(vector) => format!(
            "fleet ephemeral vector has {} samples, below the floor of {}",
            vector.sample_count, CLASS_VECTOR_SAMPLE_FLOOR
        ),
    }
}

/// Evidence from the same admission and pricing pass that selects the winner.
/// Kept separate from decision scores so diagnostic reads do not change ledger bytes.
#[derive(Debug, Default, serde::Serialize)]
pub struct RouteEvaluation {
    pub bar: BTreeMap<String, f64>,
    pub top_routable: BTreeMap<String, f64>,
    pub adequate: Vec<EvaluatedRoute>,
    pub excluded: Vec<RejectedRoute>,
}

#[derive(Debug, serde::Serialize)]
pub struct EvaluatedRoute {
    pub route: String,
    pub fit: BTreeMap<ModelMetric, MetricFit>,
    pub adequacy: f64,
    pub cost_usd: Option<f64>,
    pub normalized_cost: f64,
    /// The scorer's own task economics for this route (token profile, list
    /// cost, the subscription value multiplier, and plan pressure: the
    /// scarcity factor from the plan's remaining quota and time to reset),
    /// so an operator can check the arithmetic behind `cost_usd`.
    pub task_pricing: Option<TaskRoutePricing>,
    /// The per-million-token rate the scorer applied for each token kind
    /// (fresh input, cached read, cache write, output, reasoning); `None`
    /// marks a kind with no rate.
    pub rates_per_mtok: BTreeMap<&'static str, Option<f64>>,
}

fn admit_and_price_shortlist(
    input: &SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    counters_producer: bool,
    class_producer: bool,
    operator_group: &[String],
    availability_rejections: &HashMap<String, String>,
    profiles: &RouteTaskProfiles,
    mut evaluation: Option<&mut RouteEvaluation>,
) -> Result<PricedAdmission, NoRouteError> {
    let requirements = input.requirements;

    if !requirements_are_valid(&requirements)
        || !config.demand_exponent.is_finite()
        || config.demand_exponent <= 0.0
    {
        return Err(NoRouteError {
            candidates: candidate_quality_scores(&input.facts, &[]),
            reason: RouteFailureReason::InvalidRequirements,
            message: "model_requirements must be numbers in 0..100".to_string(),
            rejected: Vec::new(),
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset: None,
            earliest_reset_route: None,
            adequate_blockers: Default::default(),
        });
    }
    let capability_only = all_zero(&requirements);
    let caps = input.required_capabilities;
    let has_required_cap = caps
        .map(|c| c.vision == Some(true) || c.min_context.is_some())
        .unwrap_or(false);
    if capability_only && !has_required_cap {
        return Err(NoRouteError {
            candidates: candidate_quality_scores(&input.facts, &[]),
            reason: RouteFailureReason::InvalidRequirements,
            message: "all-zero model_requirements require a required capability (e.g. vision)"
                .to_string(),
            rejected: Vec::new(),
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset: None,
            earliest_reset_route: None,
            adequate_blockers: Default::default(),
        });
    }

    // `top_routable` is the frontier the demand bar was drawn against; the
    // over-qualification term maps each route back onto that same scale.
    let AdmissionResult {
        admitted: mut admissible,
        mut rejected,
        top_routable,
    } = admit_routes_with_availability(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy {
            operator: &input.operator_exclude_keys,
            caller: &input.exclude_route_keys,
        },
        operator_group,
        caps,
        &input.available_models,
        &requirements,
        config,
        availability_rejections,
    );
    if let Some(view) = evaluation.as_deref_mut() {
        view.top_routable = BTreeMap::from([
            ("elo".into(), top_routable.elo),
            ("eq".into(), top_routable.eq),
        ]);
        view.bar = BTreeMap::from([
            (
                "elo".into(),
                demand_bar(top_routable.elo, requirements.iq, config.demand_exponent),
            ),
            (
                "eq".into(),
                demand_bar(top_routable.eq, requirements.eq, config.demand_exponent),
            ),
        ]);
    }

    admissible.retain(|a| {
        let covered = subscription_route_covered(&a.route, input.is_subscription_route.as_deref());
        let permitted = !covered || subscription_budget_available(&a.runtime, input.target_is_head);
        if !permitted {
            rejected.push(RejectedRoute {
                route_key: a.route.route_key.clone(),
                reason: "subscription_reserve".into(),
                detail: a.runtime.subscription_pool.as_ref().map(|pool| {
                    pool.reserve_diagnostic(pool.keep_free_percent, input.target_is_head)
                }),
                credential_ids: Vec::new(),
            });
        }
        permitted
    });
    if let Some(view) = evaluation.as_deref_mut() {
        view.excluded = rejected.clone();
        view.excluded.extend(
            admissible
                .iter()
                .filter(|a| !a.adequacy.clears_adequacy_floor())
                .map(|a| RejectedRoute {
                    route_key: a.route.route_key.clone(),
                    reason: "below_floor".into(),
                    detail: None,
                    credential_ids: Vec::new(),
                }),
        );
    }

    if admissible.is_empty() {
        let (earliest_reset, earliest_reset_route) = earliest_admissible_recovery(
            input,
            config,
            operator_group,
            availability_rejections,
            &HashMap::new(),
        );
        return Err(NoRouteError {
            candidates: candidate_quality_scores(&input.facts, &admissible),
            reason: classify_empty_admission(&rejected),
            message: "no admissible model route for requirements".to_string(),
            adequate_blockers: adequate_blockers(input, config, top_routable, &rejected),
            rejected,
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset,
            earliest_reset_route,
        });
    }

    // Quality GATE: models that genuinely clear the bar. Speed/cost optimize ONLY
    // within these, so speed can never pull a sub-bar model in.
    let max_adequacy = admissible
        .iter()
        .map(|a| a.adequacy.adequacy)
        .fold(f64::NEG_INFINITY, f64::max);
    let cleared_count = admissible
        .iter()
        .filter(|a| a.adequacy.clears_adequacy_floor())
        .count();
    let best_effort = cleared_count == 0;

    // Sanity band for best-effort: if no route clears the floor, the highest-
    // adequacy survivor must still reach `min_best_effort_adequacy` (default
    // 0.6). Below that band, refuse selection with AllRoutesInadequate so the
    // caller never silently routes to a dangerously weak model.
    let min_band = input
        .min_best_effort_adequacy
        .unwrap_or(MIN_BEST_EFFORT_ADEQUACY);
    let refuse_below_bar = best_effort && !config.allow_best_effort;
    if best_effort && (refuse_below_bar || max_adequacy < min_band) {
        // Find the highest-adequacy survivor rejected by the quality policy.
        // A route whose adequacy is not a number has no rank, so it is never
        // reported as the closest survivor; `max_adequacy` above skips it too.
        let best_below_band = admissible
            .iter()
            .filter(|a| !a.adequacy.adequacy.is_nan())
            .max_by(|a, b| total_f64(a.adequacy.adequacy, b.adequacy.adequacy));
        let best_rejected = best_below_band.map(|a| RejectedCandidate {
            route_key: a.route.route_key.clone(),
            adequacy: a.adequacy.adequacy,
        });
        let (earliest_reset, earliest_reset_route) = earliest_admissible_recovery(
            input,
            config,
            operator_group,
            availability_rejections,
            &HashMap::new(),
        );

        let message = if refuse_below_bar {
            let unavailable = rejected
                .iter()
                .map(|route| match &route.detail {
                    Some(detail) => format!("{}: {} ({detail})", route.route_key, route.reason),
                    None => format!("{}: {}", route.route_key, route.reason),
                })
                .collect::<Vec<_>>();
            format!(
                "no route clears the adequacy floor ({ADEQUACY_FLOOR:.2}); best candidate {} has adequacy {max_adequacy:.2}; best effort disabled by consumer policy. Unavailable routes: {}",
                best_rejected.as_ref().map_or("unknown", |route| route.route_key.as_str()),
                if unavailable.is_empty() { "none".into() } else { unavailable.join("; ") },
            )
        } else {
            format!(
                "no route clears the adequacy floor ({:.2}); best effort adequacy {:.2} is below the sanity band ({:.2})",
                ADEQUACY_FLOOR, max_adequacy, min_band
            )
        };
        return Err(NoRouteError {
            candidates: candidate_quality_scores(&input.facts, &admissible),
            reason: if refuse_below_bar {
                RouteFailureReason::NoAdequateRoute
            } else {
                RouteFailureReason::AllRoutesInadequate
            },
            message,
            adequate_blockers: adequate_blockers(input, config, top_routable, &rejected),
            rejected,
            requirements: Some(requirements),
            best_rejected,
            earliest_reset,
            earliest_reset_route,
        });
    }

    // Shortlist indices into `admissible`.
    let shortlist: Vec<usize> = admissible
        .iter()
        .enumerate()
        .filter(|(_, a)| {
            if best_effort {
                a.adequacy.adequacy >= max_adequacy - RELATIVE_SHORTLIST_DELTA
            } else {
                a.adequacy.clears_adequacy_floor()
            }
        })
        .map(|(i, _)| i)
        .collect();

    // Fleet fallback makes profile support selection-wide: either all candidates
    // have a resolved task estimate, or all retain the cold-start comparison unit.
    // Never mix dollars per task and dollars per legacy message in one select.
    let use_profiles = !shortlist.is_empty()
        && shortlist
            .iter()
            .all(|&idx| profiles.contains_key(&admissible[idx].route.route_key));

    // Cold-start basket gate. Basket pricing is decided per route and per dimension: a
    // missing rate only affects the route that lacks it (see
    // `basket_route_cost`), so no catalog gap anywhere in the shortlist can
    // switch the whole selection's unit. The one whole-selection fallback left
    // is a shortlisted route with no usable token profile at all: neither its
    // own ephemeral vector nor the fleet's clears the sample floor, so nothing
    // can be basket-priced on equal terms. The producer flags are the other
    // precondition; account_producer is not in this predicate.
    let fleet_vector = fleet_ephemeral_vector(&input.facts);
    let degraded_by = shortlist.iter().filter(|_| !use_profiles).find_map(|&idx| {
        let route = &admissible[idx].route;
        route_basket_vector(&route.fact, fleet_vector)
            .is_none()
            .then(|| route.route_key.clone())
    });
    let producers_permit = use_basket_permitted(counters_producer, class_producer);
    let use_basket =
        use_profiles || (producers_permit && !shortlist.is_empty() && degraded_by.is_none());
    let pricing_fallback = if use_basket {
        None
    } else if !producers_permit {
        Some(format!(
            "basket producers absent (counters_producer={counters_producer}, class_producer={class_producer})"
        ))
    } else if let Some(route_key) = &degraded_by {
        Some(format!(
            "{route_key} has no usable ephemeral vector of its own and {}",
            fleet_vector_unusable_reason(fleet_vector)
        ))
    } else {
        Some("empty shortlist".to_string())
    };

    let scored: Vec<Scored> = shortlist
        .into_iter()
        .map(|idx| {
            let a = &admissible[idx];
            let is_sub =
                subscription_route_covered(&a.route, input.is_subscription_route.as_deref());
            let basis = if use_profiles {
                CostBasis::TaskProfile {
                    profile: &profiles[&a.route.route_key],
                    omit_cache_write: input
                        .providers_without_cache_write_reports
                        .contains(&a.route.provider_id),
                }
            } else if use_basket {
                let (vector, source) = route_basket_vector(&a.route.fact, fleet_vector)
                    .expect("the basket gate admitted only routes with a usable vector");
                CostBasis::Basket { vector, source }
            } else {
                CostBasis::OutputOnly
            };
            let cached_read_term = cached_read_term_usd(
                &a.route.fact,
                match basis {
                    CostBasis::Basket { vector, .. } => Some(vector),
                    CostBasis::OutputOnly => a.route.fact.ephemeral_vector(),
                    CostBasis::TaskProfile { .. } => None,
                },
            );
            let cached_read_term = if let CostBasis::TaskProfile { profile, .. } = basis {
                a.route
                    .fact
                    .basket_rate(BasketDimension::CachedRead)
                    .map(|rate| profile.tokens.cached_read * rate / 1_000_000.0)
            } else {
                cached_read_term
            };
            let (mut breakdown, mut notes) =
                compute_objective(&a.route, &a.runtime, is_sub, basis, input.target_is_head);
            if !use_profiles {
                record_cold_start_pricing(&mut breakdown, &a.route.fact, basis, is_sub);
            }
            apply_overqualification(
                &mut breakdown,
                &mut notes,
                &a.route,
                &requirements,
                top_routable,
                config,
            );
            Scored {
                idx,
                objective: breakdown,
                objective_notes: notes,
                cached_read_term_usd: cached_read_term,
            }
        })
        .collect();

    if let Some(view) = evaluation {
        view.adequate = scored
            .iter()
            .filter_map(|s| {
                let a = &admissible[s.idx];
                a.adequacy.clears_adequacy_floor().then(|| EvaluatedRoute {
                    route: a.route.route_key.clone(),
                    fit: a.adequacy.metrics.clone(),
                    adequacy: a.adequacy.adequacy,
                    cost_usd: s
                        .objective
                        .task_pricing
                        .as_ref()
                        .filter(|p| p.price_unit == "task")
                        .and_then(|p| p.final_cost_usd),
                    normalized_cost: s.objective.cost,
                    task_pricing: s.objective.task_pricing.clone(),
                    rates_per_mtok: BasketDimension::ALL
                        .into_iter()
                        .map(|dimension| (dimension.as_str(), a.route.fact.basket_rate(dimension)))
                        .collect(),
                })
            })
            .collect();
    }
    Ok(PricedAdmission {
        admissible,
        rejected,
        scored,
        use_basket,
        degraded_by,
        pricing_fallback,
        best_effort,
        cleared_count,
    })
}

/// Prices every shortlisted route the way [`select_model_route`] would, including
/// the selection's pricing mode. The bucket-membership replay uses this so a
/// route is judged free in the selection it was actually priced in.
///
/// `counters_producer` and `class_producer` are the two flags that may engage
/// the basket; there is no `account_producer` argument because its absence must
/// never force the basket off.
///
/// Reference calibration is measured off the host stores, not invented. When
/// the whole selection had to be all-or-nothing on rate cards, every
/// reconstructed mason shortlist held a route with no rate card
/// (gpt-5.6-sol-pro) and the basket never engaged. Pricing is per dimension
/// now: that route prices as unpriced and the rest of the shortlist stays on
/// the basket. See the catalog-and-readings replay test.
pub fn price_selection_shortlist(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    counters_producer: bool,
    class_producer: bool,
) -> Result<PricedShortlist, NoRouteError> {
    price_selection_shortlist_with_profiles(
        input,
        config,
        counters_producer,
        class_producer,
        &RouteTaskProfiles::new(),
    )
}

/// Shortlist replay with the same pre-resolved profile snapshot as selection.
pub fn price_selection_shortlist_with_profiles(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    counters_producer: bool,
    class_producer: bool,
    profiles: &RouteTaskProfiles,
) -> Result<PricedShortlist, NoRouteError> {
    let priced = admit_and_price_shortlist(
        &input,
        config,
        counters_producer,
        class_producer,
        &[],
        &HashMap::new(),
        profiles,
        None,
    )?;
    Ok(PricedShortlist {
        use_basket: priced.use_basket,
        degraded_by: priced.degraded_by,
        pricing_fallback: priced.pricing_fallback,
        routes: priced
            .scored
            .iter()
            .map(|s| {
                let admitted = &priced.admissible[s.idx];
                PricedShortlistEntry {
                    route_key: admitted.route.route_key.clone(),
                    cost: s.objective.cost,
                    selection_cost: s.objective.selection_cost,
                    overqualification: s.objective.overqualification,
                    cached_read_term_usd: s.cached_read_term_usd,
                    basket_pricing: s.objective.basket_pricing.clone(),
                    task_pricing: s.objective.task_pricing.clone(),
                }
            })
            .collect(),
    })
}

pub fn select_model_route_with_config(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
) -> Result<SelectedModelRoute, NoRouteError> {
    select_model_route_with_slots(input, config, None)
}

/// [`select_model_route_with_config`] with the caller's concurrency slot counts.
/// The shortlist is ordered exactly as without them; then the first candidate
/// whose slot has room wins (see [`crate::slot_saturation`]). `None` is the
/// unchanged selection.
pub fn select_model_route_with_slots(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
) -> Result<SelectedModelRoute, NoRouteError> {
    select_model_route_with_operator_group(input, config, slots, &[])
}

/// Select one route with global and consumer exclusions resolved separately.
/// A negation in the consumer's list cannot cancel a global exclusion.
pub fn select_model_route_with_operator_group(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    operator_group: &[String],
) -> Result<SelectedModelRoute, NoRouteError> {
    select_model_route_with_availability(input, config, slots, operator_group, &HashMap::new())
}

/// Select with per-route availability refusals while preserving the ordinary
/// exclusion, capability, scoring and quota gates. The refusal map follows
/// [`admit_routes_with_availability`]'s exact-key and detail contract.
pub fn select_model_route_with_availability(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    operator_group: &[String],
    availability_rejections: &HashMap<String, String>,
) -> Result<SelectedModelRoute, NoRouteError> {
    select_model_route_with_profiles(
        input,
        config,
        slots,
        operator_group,
        availability_rejections,
        &RouteTaskProfiles::new(),
    )
}

/// Select using task profiles already resolved for the request's workload and iq
/// bucket. An empty map is cold start. A partial map also falls back selection-wide
/// so task prices are never compared with legacy per-message or output-only prices.
/// The caller obtains these profiles from its refresh snapshot, not from the store
/// on the select path.
pub fn select_model_route_with_profiles(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    operator_group: &[String],
    availability_rejections: &HashMap<String, String>,
    profiles: &RouteTaskProfiles,
) -> Result<SelectedModelRoute, NoRouteError> {
    evaluate_model_route_with_profiles(
        input,
        config,
        slots,
        operator_group,
        availability_rejections,
        profiles,
        None,
    )
}

/// Run selection with optional diagnostic evidence, without changing the winner or its score.
pub fn evaluate_model_route_with_profiles(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    operator_group: &[String],
    availability_rejections: &HashMap<String, String>,
    profiles: &RouteTaskProfiles,
    evaluation: Option<&mut RouteEvaluation>,
) -> Result<SelectedModelRoute, NoRouteError> {
    let PricedAdmission {
        admissible,
        mut rejected,
        scored,
        use_basket,
        pricing_fallback,
        best_effort,
        cleared_count,
        ..
    } = admit_and_price_shortlist(
        &input,
        config,
        COUNTERS_PRODUCER,
        CLASS_PRODUCER,
        operator_group,
        availability_rejections,
        profiles,
        evaluation,
    )?;

    // Deterministic ordering. COST-PRIMARY lexicographic among adequate models.
    // Every key is a property of one route, computed once here, so the order is
    // total by construction (see `route_order_keys` for why that matters).
    // `with_term` prices each route with its over-qualification term; without
    // it, the order is the one the term-free router produced (cost + quota).
    let order = |with_term: bool| -> Vec<usize> {
        let facts: Vec<RouteOrderFacts<'_>> = scored
            .iter()
            .map(|s| {
                let a = &admissible[s.idx];
                RouteOrderFacts {
                    provider_id: &a.route.provider_id,
                    route_key: &a.route.route_key,
                    subscription_covered: s.objective.subscription_covered,
                    quota_priced: s.objective.subscription_pricing.is_some(),
                    profile_priced: s
                        .objective
                        .task_pricing
                        .as_ref()
                        .is_some_and(|p| p.profile.is_some()),
                    price_unknown: s.objective.price_unknown,
                    selection_cost: if with_term {
                        s.objective.selection_cost
                    } else {
                        s.objective.cost + s.objective.quota
                    },
                    uncached: input.prefer_prompt_cache
                        && input
                            .providers_without_prompt_cache
                            .contains(&a.route.fact.provider_id),
                    adequacy: a.adequacy.adequacy,
                    raw_adequacy: a.adequacy.raw_adequacy,
                    quota_abundance: a.runtime.quota_abundance,
                    speed_score: s.objective.speed_score,
                    metered_cost: normalized_metered_cost(&a.route.fact),
                }
            })
            .collect();
        let keys = route_order_keys(&facts);
        let mut positions: Vec<usize> = (0..keys.len()).collect();
        positions.sort_by(|&a, &b| compare_route_order_keys(&keys[a], &keys[b], best_effort));
        positions
    };
    let mut chosen = order(true);
    let mut guard_note = None;
    // Fit guard. Cost is compared before adequacy, so once the term pushes a
    // route out of the effectively-free bucket a cheaper route that only just
    // clears the adequacy floor beats one that fits exactly: an iq 55 / eq 40
    // mason went from opus (fit 1.00) to a luna whose eq fit is below one. The
    // term may move a decision only to a route that fits at least as well as
    // the term-free winner; otherwise the term-free winner stands.
    if scored.iter().any(|s| s.objective.overqualification > 0.0) {
        let without_term = order(false);
        let with_winner = &admissible[scored[chosen[0]].idx];
        let without_winner = &admissible[scored[without_term[0]].idx];
        if chosen[0] != without_term[0]
            && with_winner.adequacy.adequacy < without_winner.adequacy.adequacy
        {
            guard_note = Some(format!(
                "overqualification guard: kept {} (adequacy {:.2}); the term would have picked {}, which fits the demand less well (adequacy {:.2})",
                without_winner.route.route_key,
                without_winner.adequacy.adequacy,
                with_winner.route.route_key,
                with_winner.adequacy.adequacy,
            ));
            chosen = without_term;
        }
    }
    let mut taken: Vec<Option<Scored>> = scored.into_iter().map(Some).collect();
    let scored: Vec<Scored> = chosen
        .into_iter()
        .map(|position| taken[position].take().expect("each position is used once"))
        .collect();

    // Saturation only chooses among the ordered shortlist, so it can pass over a
    // full route but never admit one the quality gate left out.
    let ordered: Vec<(&str, &str)> = scored
        .iter()
        .map(|s| {
            let route = &admissible[s.idx].route;
            (route.route_key.as_str(), route.capacity_key.as_str())
        })
        .collect();
    let (winner_position, slot_saturation) = pick_unsaturated(&ordered, slots);
    let winner = &scored[winner_position];
    let winner_admissible = &admissible[winner.idx];
    let fact = &winner_admissible.route.fact;
    let mut explanation: Vec<String> = Vec::new();
    explanation.push(format!(
        "route {} for agent {}",
        winner_admissible.route.route_key, input.target_agent
    ));
    if fact.score_provenance == ScoreProvenance::AaInferred {
        explanation.push("provenance=aa_inferred".to_string());
    }
    explanation.extend(winner_admissible.adequacy.explanation.iter().cloned());
    explanation.push(format!(
        "{}: adequacy {:.2}, selection cost {:.2}",
        if cleared_count > 0 {
            "clears bar"
        } else {
            "best-effort (none cleared)"
        },
        winner_admissible.adequacy.adequacy,
        winner.objective.selection_cost,
    ));
    explanation.extend(winner.objective_notes.iter().cloned());
    for candidate in &scored {
        if let Some(pricing) = &candidate.objective.task_pricing {
            let level = pricing
                .profile
                .as_ref()
                .map(|profile| format!("{:?}", profile.level))
                .unwrap_or_else(|| "cold_start".to_string());
            let tasks = pricing
                .profile
                .as_ref()
                .map_or(0, |profile| profile.task_count);
            explanation.push(format!(
                "route {}: task pricing (level={level}, tasks={tasks}): {}",
                admissible[candidate.idx].route.route_key,
                serde_json::to_string(pricing).expect("task pricing contains finite numbers"),
            ));
        }
        if candidate.idx != winner.idx && candidate.objective.price_unknown {
            let route_key = &admissible[candidate.idx].route.route_key;
            explanation.push(format!(
                "route {route_key}: price_unknown (ranked after known prices)"
            ));
        }
    }
    // The unit the whole shortlist was priced in, and why when it is not the
    // basket: per-route lines above say how each route was priced, this one
    // says which comparison they were all part of.
    explanation.push(match &pricing_fallback {
        None if winner
            .objective
            .task_pricing
            .as_ref()
            .is_some_and(|p| p.profile.is_some()) =>
        {
            "pricing: task profiles for the whole shortlist".to_string()
        }
        None if use_basket => "pricing: basket for the whole shortlist".to_string(),
        Some(reason) => format!("pricing: output-only for the whole shortlist ({reason})"),
        None => "pricing: output-only for the whole shortlist".to_string(),
    });
    explanation.extend(guard_note);
    explanation.extend(
        rejected
            .iter()
            .filter(|route| route.reason == "backoff_active")
            .map(|route| {
                format!(
                    "route {} rejected: backoff_active{}",
                    route.route_key,
                    route
                        .detail
                        .as_deref()
                        .map(|detail| format!(" ({detail})"))
                        .unwrap_or_default()
                )
            }),
    );
    if let Some(saturation) = &slot_saturation {
        explanation.extend(saturation.explanation_lines());
        rejected.extend(saturation.skipped.iter().map(|skipped| RejectedRoute {
            route_key: skipped.route_key.clone(),
            reason: REASON_PROVIDER_SATURATED.to_string(),
            detail: Some(skipped.slot.describe()),
            credential_ids: Vec::new(),
        }));
    }

    let prompt_variant = fact
        .prompt_defaults
        .as_ref()
        .and_then(|d| d.variant.clone())
        .or_else(|| fact.variant.clone());
    let prompt_temperature = fact.prompt_defaults.as_ref().and_then(|d| d.temperature);
    let prompt_effort = fact
        .prompt_defaults
        .as_ref()
        .and_then(|d| d.reasoning_effort)
        .map(|e| e.as_str().to_string());

    Ok(SelectedModelRoute {
        model: SelectedModel {
            provider_id: fact.provider_id.clone(),
            model_id: fact.model_id.clone(),
            variant: fact.variant.clone(),
        },
        route_key: winner_admissible.route.route_key.clone(),
        capacity_key: winner_admissible.route.capacity_key.clone(),
        score: SelectedRouteScore {
            candidates: candidate_quality_scores(&input.facts, &admissible),
            adequacy: winner_admissible.adequacy.adequacy,
            raw_adequacy: winner_admissible.adequacy.raw_adequacy,
            selection_cost: winner.objective.selection_cost,
            metrics: winner_admissible.adequacy.metrics.clone(),
            objective: winner.objective.clone(),
            also_adequate: scored
                .iter()
                .filter(|candidate| candidate.idx != winner.idx)
                .map(|candidate| &admissible[candidate.idx])
                .filter(|candidate| candidate.adequacy.clears_adequacy_floor())
                .take(4)
                .map(|candidate| {
                    let fact = &candidate.route.fact;
                    let model = format!("{}/{}", fact.provider_id, fact.model_id);
                    match fact
                        .prompt_defaults
                        .as_ref()
                        .and_then(|defaults| defaults.variant.as_deref())
                        .or(fact.variant.as_deref())
                        .filter(|variant| !variant.is_empty())
                    {
                        Some(variant) => format!("{model}:{variant}"),
                        None => model,
                    }
                })
                .collect(),
            pricing_mode: if use_basket {
                PricingMode::Basket
            } else {
                PricingMode::OutputOnly
            },
            pricing_fallback: pricing_fallback.clone(),
        },
        prompt_params: PromptParams {
            provider_id: fact.provider_id.clone(),
            model_id: fact.model_id.clone(),
            variant: prompt_variant,
            temperature: prompt_temperature,
            reasoning_effort: prompt_effort,
        },
        rejected,
        explanation,
        priced_with_basket: use_basket,
        slot_saturation,
        below_floor: (best_effort && winner_admissible.adequacy.adequacy < ADEQUACY_FLOOR)
            .then_some(BelowFloorWarning {
                adequacy: winner_admissible.adequacy.adequacy,
                floor: ADEQUACY_FLOOR,
            }),
    })
}

fn adequate_blockers(
    input: &SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    top_routable: TopRoutableScores,
    rejected: &[RejectedRoute],
) -> std::collections::BTreeMap<String, Option<f64>> {
    let reasons: HashMap<_, _> = rejected
        .iter()
        .map(|route| (route.route_key.as_str(), route.reason.as_str()))
        .collect();
    input
        .facts
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            let reason = reasons.get(route.route_key.as_str())?;
            if fact.is_deprecated_upstream()
                || fact.score_provenance == ScoreProvenance::Unscored
                || capability_rejection_reason(fact, input.required_capabilities).is_some()
                || !score_adequacy(fact, &input.requirements, top_routable, config)
                    .clears_adequacy_floor()
            {
                return None;
            }
            let reset = matches!(
                *reason,
                "quota_exhausted" | "cooldown_active" | "backoff_active"
            )
            .then(|| {
                input.runtime_snapshot.as_ref().and_then(|snapshot| {
                    route_wall_until(snapshot, &route.route_key, &route.capacity_key)
                })
            })
            .flatten();
            Some((route.route_key, reset))
        })
        .collect()
}

/// Diagnose retry timing without changing admission, rejection evidence or picks.
/// Account resets must come only from accounts permitted to serve this request,
/// including its subscription-versus-metered policy. They lift account availability,
/// never structural or operator fences.
pub fn earliest_admissible_recovery(
    input: &SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    operator_group: &[String],
    availability_rejections: &HashMap<String, String>,
    account_resets: &HashMap<String, f64>,
) -> (Option<f64>, Option<String>) {
    let Some(snapshot) = input.runtime_snapshot.as_ref() else {
        return (None, None);
    };
    let mut recovered = snapshot.clone();
    for state in recovered
        .by_route
        .values_mut()
        .chain(recovered.by_capacity.values_mut())
    {
        state.cooldown_until = None;
        state.quota_exhausted_until = None;
        state.provider_reset_until = None;
        state.failure_backoff_until = None;
    }
    let mut deadlines = HashMap::new();
    for fact in &input.facts {
        let route = fact_to_route(fact);
        let account_reset = account_resets
            .get(&route.route_key)
            .copied()
            .filter(|reset| reset.is_finite() && *reset > snapshot.now);
        let wall = route_wall_until(snapshot, &route.route_key, &route.capacity_key);
        if let Some(reset) = latest_deadline(wall, account_reset) {
            deadlines.insert(route.route_key.clone(), reset);
        }
        if account_reset.is_some() {
            for state in [
                recovered.by_route.get_mut(&route.route_key),
                recovered.by_capacity.get_mut(&route.capacity_key),
            ]
            .into_iter()
            .flatten()
            {
                if state
                    .credential_refusal
                    .as_ref()
                    .is_some_and(|mark| mark.reason == "no_eligible_account")
                {
                    state.credential_refusal = None;
                }
                // The dated account recovery also releases the quota reserve that
                // excluded those accounts; no unrelated reserve is relaxed.
                state.subscription_pool = None;
            }
        }
    }
    let admission = admit_routes_with_availability(
        &input.facts,
        Some(&recovered),
        ExclusionPolicy {
            operator: &input.operator_exclude_keys,
            caller: &input.exclude_route_keys,
        },
        operator_group,
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
        availability_rejections,
    );
    let earliest = admission
        .admitted
        .iter()
        .filter(|candidate| candidate.adequacy.clears_adequacy_floor())
        .filter(|candidate| {
            !subscription_route_covered(&candidate.route, input.is_subscription_route.as_deref())
                || subscription_budget_available(&candidate.runtime, input.target_is_head)
        })
        .filter_map(|candidate| {
            deadlines
                .get(&candidate.route.route_key)
                .map(|reset| (*reset, candidate.route.route_key.clone()))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    match earliest {
        Some((reset, route)) => (Some(reset), Some(route)),
        None => (None, None),
    }
}

fn classify_empty_admission(rejected: &[RejectedRoute]) -> RouteFailureReason {
    let reasons: HashSet<&str> = rejected.iter().map(|r| r.reason.as_str()).collect();
    // Prioritize the most RECOVERABLE/actionable signal, even when reasons are
    // mixed. A quota/cooldown-blocked route passed the capability gates, so its
    // presence means a capable route merely needs to reset: break-glass can
    // relax it. This must win over "not_available" peers.
    if reasons.contains("quota_exhausted")
        || reasons.contains("cooldown_active")
        || reasons.contains("backoff_active")
    {
        return RouteFailureReason::QuotaOrCooldownExhausted;
    }
    if reasons.contains("missing_vision") {
        return RouteFailureReason::MissingRequiredCapability;
    }
    if reasons.contains("context_too_small") {
        return RouteFailureReason::ContextTooSmall;
    }
    RouteFailureReason::NoAvailableModel
}

/// What the route ordering needs to know about one priced, admissible route.
#[derive(Debug, Clone)]
pub(crate) struct RouteOrderFacts<'r> {
    pub price_unknown: bool,
    pub provider_id: &'r str,
    pub route_key: &'r str,
    pub subscription_covered: bool,
    pub quota_priced: bool,
    pub profile_priced: bool,
    pub selection_cost: f64,
    /// True only when the caller prefers prompt caching and this route's
    /// provider does not cache prompts.
    pub uncached: bool,
    pub adequacy: f64,
    pub raw_adequacy: f64,
    pub quota_abundance: Option<f64>,
    pub speed_score: f64,
    pub metered_cost: f64,
}

/// Sort keys for one route. Every field is a property of that route alone, so
/// comparing keys lexicographically is a total order.
#[derive(Debug, Clone)]
pub(crate) struct RouteOrderKey<'r> {
    pub price_unknown: bool,
    pub route_key: &'r str,
    pub uncached: bool,
    /// The cost this route is compared on: 0 inside the effectively-free
    /// bucket; above it, the pool's cost for a subscription-covered route
    /// and the route's own selection cost otherwise.
    pub cost_key: f64,
    /// Whether adequacy and abundance decide among routes of equal
    /// `cost_key`: true inside the effectively-free bucket and for a member of
    /// a subscription pool with at least two routes. A route outside both
    /// orders on speed at equal cost, as metered routes always have.
    pub in_bucket: bool,
    pub adequacy: f64,
    pub raw_adequacy: f64,
    pub abundance_quantum: i64,
    pub speed_score: f64,
    pub metered_cost: f64,
}

/// Compute a stable sort key for each candidate before comparing routes.
///
/// Quota-priced routes keep their own economic cost because different token
/// baskets consume different amounts of the same subscription pool. Legacy
/// cold-start snapshots without account economics retain a free bucket and price each
/// provider's covered routes at that provider's cheapest covered cost. Quality
/// then decides within a shared pool or free bucket.
///
/// Pool membership and cost keys must be properties of individual routes, not
/// pairwise exceptions: otherwise three routes can form inconsistent rankings
/// and violate the ordering contract required by Rust's sort.
pub(crate) fn route_order_keys<'r>(routes: &[RouteOrderFacts<'r>]) -> Vec<RouteOrderKey<'r>> {
    // Per provider: the cheapest covered route's cost and how many covered
    // routes share the pool.
    let mut pools: HashMap<&str, (f64, usize)> = HashMap::new();
    for route in routes.iter().filter(|r| {
        r.subscription_covered && !r.quota_priced && !r.profile_priced && !r.price_unknown
    }) {
        pools
            .entry(route.provider_id)
            .and_modify(|(min, members)| {
                *min = min.min(route.selection_cost);
                *members += 1;
            })
            .or_insert((route.selection_cost, 1));
    }
    routes
        .iter()
        .map(|route| {
            let (pool_cost, shared_pool) = if route.subscription_covered
                && !route.quota_priced
                && !route.profile_priced
                && !route.price_unknown
            {
                let (min, members) = pools[route.provider_id];
                (min, members >= 2)
            } else {
                (route.selection_cost, false)
            };
            let free = !route.quota_priced
                && !route.profile_priced
                && pool_cost <= EFFECTIVELY_FREE_SELECTION_COST;
            // An absent or non-finite abundance sample is neutral rather than
            // favourable or fatal; quantizing keeps tiny float noise from
            // outranking the speed tie-break.
            let abundance = route
                .quota_abundance
                .filter(|value| value.is_finite())
                .unwrap_or(1.0);
            RouteOrderKey {
                price_unknown: route.price_unknown,
                route_key: route.route_key,
                uncached: route.uncached,
                // Prices in one fixed 10% band share a cost key, letting quality
                // break the tie. Fixed bands keep rankings consistent across
                // three or more routes, unlike pairwise tolerance exceptions.
                cost_key: if route.quota_priced && pool_cost > 0.0 {
                    1.1_f64.powf((pool_cost.ln() / 1.1_f64.ln()).round())
                } else if free {
                    0.0
                } else {
                    pool_cost
                },
                in_bucket: route.quota_priced || free || shared_pool,
                adequacy: route.adequacy,
                raw_adequacy: route.raw_adequacy,
                abundance_quantum: (abundance * 1000.0).round() as i64,
                speed_score: route.speed_score,
                metered_cost: route.metered_cost,
            }
        })
        .collect()
}

/// Lexicographic comparison of two routes' sort keys; the first sorts first.
pub(crate) fn compare_route_order_keys(
    a: &RouteOrderKey<'_>,
    b: &RouteOrderKey<'_>,
    best_effort: bool,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Adequacy and abundance only decide for bucket routes; every route
    // outside the bucket carries the same neutral value, so speed decides.
    let bucket_adequacy = |k: &RouteOrderKey<'_>| if k.in_bucket { k.adequacy } else { 0.0 };
    let bucket_abundance = |k: &RouteOrderKey<'_>| {
        if k.in_bucket {
            k.abundance_quantum
        } else {
            0
        }
    };
    // 1. (best-effort only) most-capable first.
    let best_effort_first = if best_effort {
        total_f64(b.adequacy, a.adequacy)
    } else {
        Ordering::Equal
    };
    best_effort_first
        // Unknown prices cannot borrow a cheap pool cost or win a clamped tie.
        .then(a.price_unknown.cmp(&b.price_unknown))
        // 2. Prompt-cache preference partitions otherwise eligible candidates.
        // It never lets a cache-capable route bypass the normal adequacy gate.
        .then(a.uncached.cmp(&b.uncached))
        // 3. Lowest cost, with the free bucket and each subscription pool
        // collapsed to one cost (see `route_order_keys`).
        .then(total_f64(a.cost_key, b.cost_key))
        // At one cost above the free threshold, pool members sort before
        // routes outside any pool. Equal costs there are exact float ties,
        // rare in practice; the partition only keeps the order total.
        .then(b.in_bucket.cmp(&a.in_bucket))
        // 4. (bucket) highest adequacy first: the saturating blend, not raw,
        // so the DEMAND still shapes the pick. At iq 95 sol (1.00) outranks
        // luna (0.96) and edges terra (0.999); at iq 30 all three saturate to
        // 1.00 and speed decides, keeping cheap-band work on the fast small
        // model instead of promoting everything to the flagship.
        .then(total_f64(bucket_adequacy(b), bucket_adequacy(a)))
        // 5. (bucket) spend headroom that expires sooner first.
        .then(bucket_abundance(b).cmp(&bucket_abundance(a)))
        // 6. Fastest among equal cost.
        .then(total_f64(b.speed_score, a.speed_score))
        // 7. Cheaper-if-metered proxy, then capability, then routeKey.
        .then(total_f64(a.metered_cost, b.metered_cost))
        .then(total_f64(b.adequacy, a.adequacy))
        .then(total_f64(b.raw_adequacy, a.raw_adequacy))
        .then(a.route_key.cmp(b.route_key))
}

/// Ascending total order on f64 for sort keys. `-0.0` and `0.0` compare equal
/// as they do under `partial_cmp`; NaN ranks above every number instead of
/// comparing equal to everything, which would break transitivity.
fn total_f64(a: f64, b: f64) -> std::cmp::Ordering {
    let canon = |x: f64| if x.is_nan() { f64::NAN } else { x + 0.0 };
    canon(a).total_cmp(&canon(b))
}

/// Format a number like JS `${n}`: whole numbers without a decimal point.
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

#[cfg(test)]
mod task_profile_pricing_tests {
    use super::*;
    use crate::task_profile::{compute_profile, ClosedTask, ProfileLevel};

    fn model(provider: &str, id: &str) -> ModelFact {
        serde_json::from_value(serde_json::json!({
            "providerID": provider, "modelID": id, "family": "test",
            "scores": { "elo": 1800.0, "eq": 1800.0, "speed": 50.0 },
            "confidence": "curated", "costPerMTokOut": 1.0,
            "inputRate": 1.0, "cacheReadRate": 1.0, "cacheWriteRate": 1.0,
            "reasoningRate": 1.0
        }))
        .unwrap()
    }

    fn profile(tokens: f64) -> TaskProfile {
        TaskProfile {
            tokens: TokenVector {
                output_excluding_reasoning: tokens,
                ..Default::default()
            },
            level: ProfileLevel::OwnCell,
            task_count: 20,
            own_task_count: 20,
            own_weight: 1.0,
            keeps_tokenizer: true,
        }
    }

    fn profiles(facts: &[ModelFact], profiles: Vec<TaskProfile>) -> RouteTaskProfiles {
        facts
            .iter()
            .zip(profiles)
            .map(|(fact, profile)| (fact_to_route(fact).route_key, profile))
            .collect()
    }

    fn input(facts: Vec<ModelFact>, subscription: bool) -> SelectModelRouteInput<'static> {
        SelectModelRouteInput {
            target_agent: "mason".into(),
            target_is_head: false,
            requirements: ModelRequirements { iq: 75.0, eq: 20.0 },
            available_models: facts.iter().map(|f| fact_to_route(f).route_key).collect(),
            facts,
            runtime_snapshot: None,
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: None,
            is_subscription_route: Some(Box::new(move |_| subscription)),
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: HashSet::new(),
            providers_without_cache_write_reports: HashSet::new(),
        }
    }

    fn select(
        input: SelectModelRouteInput<'_>,
        profiles: &RouteTaskProfiles,
    ) -> SelectedModelRoute {
        select_model_route_with_profiles(
            input,
            &crate::routing_config::ROUTING_CONFIG_WITHOUT_OVERQUALIFICATION,
            None,
            &[],
            &HashMap::new(),
            profiles,
        )
        .unwrap()
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-10, "{actual} != {expected}");
    }

    #[test]
    fn sonnet_three_times_opus_tokens_has_three_times_the_list_cost() {
        let facts = vec![model("claude", "opus"), model("claude", "sonnet")];
        let tasks: Vec<_> = facts
            .iter()
            .enumerate()
            .flat_map(|(i, model)| {
                (0..20).map(move |_| ClosedTask {
                    workload: "mason",
                    model,
                    iq_bucket: 7,
                    tokens: TokenVector {
                        fresh_input: 1000.0 * (1 + i * 2) as f64,
                        cached_read: 2000.0 * (1 + i * 2) as f64,
                        cache_write: 3000.0 * (1 + i * 2) as f64,
                        output_excluding_reasoning: 4000.0 * (1 + i * 2) as f64,
                        reasoning: 5000.0 * (1 + i * 2) as f64,
                    },
                })
            })
            .collect();
        let resolved = facts
            .iter()
            .map(|fact| compute_profile(&tasks, fact, "mason", 7).unwrap())
            .collect();
        let profiles = profiles(&facts, resolved);
        let priced = price_selection_shortlist_with_profiles(
            input(facts, false),
            &crate::routing_config::ROUTING_CONFIG_WITHOUT_OVERQUALIFICATION,
            false,
            false,
            &profiles,
        )
        .unwrap();
        assert!(
            priced.use_basket,
            "profiles do not depend on legacy producer flags"
        );
        let opus = priced
            .routes
            .iter()
            .find(|r| r.route_key.contains("opus"))
            .unwrap()
            .task_pricing
            .as_ref()
            .unwrap();
        let sonnet = priced
            .routes
            .iter()
            .find(|r| r.route_key.contains("sonnet"))
            .unwrap()
            .task_pricing
            .as_ref()
            .unwrap();
        close(opus.list_cost_usd.unwrap(), 0.015);
        close(sonnet.list_cost_usd.unwrap(), 0.045);
        close(
            sonnet.list_cost_usd.unwrap() / opus.list_cost_usd.unwrap(),
            3.0,
        );
        assert_eq!(
            sonnet.profile.as_ref().unwrap().level,
            ProfileLevel::OwnCell
        );
    }

    #[test]
    fn no_pool_subscription_routes_rank_on_their_own_profile_costs() {
        let mut cheap = model("codex", "sol");
        cheap.subscription_multiplier = Some(18.7);
        let mut expensive = model("codex", "luna");
        expensive.subscription_multiplier = Some(18.7);
        expensive.scores.speed = 1000.0;
        let facts = vec![expensive, cheap];
        let profiles = profiles(&facts, vec![profile(3000.0), profile(1000.0)]);
        let selected = select(input(facts, true), &profiles);
        assert_eq!(selected.model.model_id, "sol");
        let pricing = selected.score.objective.task_pricing.as_ref().unwrap();
        close(pricing.list_cost_usd.unwrap(), 0.001);
        close(pricing.value_multiplier, 18.7);
        close(pricing.pressure, 1.0);
        close(pricing.final_cost_usd.unwrap(), 0.001 / 18.7);
        assert!(selected.score.objective.subscription_pricing.is_some());
    }

    #[test]
    fn muse_sol_decision_explains_both_profiles_and_full_economics() {
        let mut muse = model("meta", "muse-spark-1.3-contributor");
        muse.subscription_multiplier = Some(6.0);
        let mut sol = model("codex", "sol");
        sol.subscription_multiplier = Some(18.7);
        let facts = vec![muse, sol];
        let mut muse_profile = profile(63000.0);
        muse_profile.level = ProfileLevel::FleetBucket;
        muse_profile.keeps_tokenizer = false;
        muse_profile.own_task_count = 0;
        muse_profile.own_weight = 0.0;
        muse_profile.tokens.cache_write = 1000000.0;
        let profiles = profiles(&facts, vec![muse_profile, profile(840000.0)]);
        let mut request = input(facts, true);
        request
            .providers_without_cache_write_reports
            .insert(request.facts[0].provider_id.clone());
        let selected = select(request, &profiles);
        let records: Vec<serde_json::Value> = selected
            .explanation
            .iter()
            .filter_map(|line| {
                line.split_once("): ")
                    .filter(|_| line.contains(": task pricing ("))
                    .map(|(_, json)| serde_json::from_str(json).unwrap())
            })
            .collect();
        assert_eq!(records.len(), 2);
        let muse = records
            .iter()
            .find(|r| r["valueMultiplier"] == 6.0)
            .unwrap();
        let sol = records
            .iter()
            .find(|r| r["valueMultiplier"] == 18.7)
            .unwrap();
        assert_eq!(muse["profile"]["level"], "fleet_bucket");
        assert_eq!(muse["profile"]["taskCount"], 20);
        assert_eq!(muse["profile"]["keepsTokenizer"], false);
        assert_eq!(muse["tokens"]["outputExcludingReasoning"], 63000.0);
        assert_eq!(
            muse["omittedDimensions"],
            serde_json::json!(["cache_write"])
        );
        for (record, list, multiplier) in [(muse, 0.063, 6.0), (sol, 0.84, 18.7)] {
            close(record["listCostUsd"].as_f64().unwrap(), list);
            assert_eq!(record["valueSource"], "operator override");
            close(record["pressure"].as_f64().unwrap(), 1.0);
            close(record["finalCostUsd"].as_f64().unwrap(), list / multiplier);
            close(
                record["normalizedCost"].as_f64().unwrap(),
                list / multiplier / crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE,
            );
        }
        assert_eq!(sol["profile"]["level"], "own_cell");
        assert_eq!(sol["tokens"]["outputExcludingReasoning"], 840000.0);
    }

    #[test]
    fn profile_metered_cost_ignores_subsidy_and_legacy_quota_surcharge() {
        let mut fact = model("api", "sol");
        fact.subscription_multiplier = Some(100.0);
        let route = fact_to_route(&fact);
        let runtime = RouteRuntimeState {
            quota_pressure: Some(0.8),
            ..Default::default()
        };
        let (objective, _) = compute_objective(
            &route,
            &runtime,
            false,
            CostBasis::TaskProfile {
                profile: &profile(1000000.0),
                omit_cache_write: false,
            },
            false,
        );
        let pricing = objective.task_pricing.unwrap();
        close(pricing.list_cost_usd.unwrap(), 1.0);
        close(pricing.final_cost_usd.unwrap(), 1.0);
        close(pricing.value_multiplier, 1.0);
        close(
            objective.cost,
            1.0 / crate::routing_config::BASKET_REFERENCE_COST_PER_MESSAGE,
        );
        close(objective.quota, 0.0);
        close(objective.selection_cost, objective.cost);
    }

    #[test]
    fn profile_prices_fallback_rates_but_not_unknown_positive_dimensions() {
        let mut fact = model("api", "sol");
        fact.reasoning_rate = None;
        fact.cache_write_rate = None;
        fact.input_rate = Some(2.0);
        let mut profile = profile(1000.0);
        profile.tokens.reasoning = 2000.0;
        profile.tokens.cache_write = 3000.0;
        let (list, omitted, unpriced) = profile_list_price(&fact, profile.tokens, false);
        close(list.unwrap(), 0.009);
        assert!(omitted.is_empty() && unpriced.is_empty());
        fact.cache_read_rate = None;
        profile.tokens.cached_read = 4000.0;
        let (list, _, unpriced) = profile_list_price(&fact, profile.tokens, false);
        assert!(list.is_none());
        assert_eq!(unpriced, vec!["cached_read"]);
    }

    #[test]
    fn incomplete_profile_snapshot_keeps_the_whole_select_in_cold_start() {
        let facts = vec![model("api", "sol"), model("api", "spark")];
        let profiles = profiles(&facts[..1], vec![profile(1000000.0)]);
        let selected = select(input(facts, false), &profiles);
        assert!(!selected.priced_with_basket);
        assert!(selected
            .score
            .objective
            .task_pricing
            .as_ref()
            .unwrap()
            .profile
            .is_none());
        assert_eq!(
            selected
                .explanation
                .iter()
                .filter(|line| line.contains("level=cold_start, tasks=0"))
                .count(),
            2
        );
    }

    #[test]
    fn profile_uses_pool_measurement_and_pressure_for_workers_and_heads() {
        use crate::subscription::{
            MeasuredValue, QuotaAccount, SubscriptionPlan, SubscriptionPool,
        };
        let mut fact = model("codex", "sol");
        fact.subscription_multiplier = Some(2.0);
        let route = fact_to_route(&fact);
        let runtime = RouteRuntimeState {
            quota_pressure: Some(0.9),
            subscription_pool: Some(std::sync::Arc::new(SubscriptionPool {
                provider: "codex".into(),
                now_ms: 1000,
                plans: vec![SubscriptionPlan {
                    tier: Some("pro".into()),
                    plan: Some("openai/pro_200".into()),
                    measured: Some(MeasuredValue {
                        multiplier: 18.7,
                        interval_end_ms: 999,
                        row_date: "fresh".into(),
                    }),
                    fee_usd: 200.0,
                    accounts: vec![QuotaAccount {
                        weekly_used: 50.0,
                        weekly_remaining_fraction: 0.5,
                        hours_to_reset: 84.0,
                        short_walled: false,
                        short_windows: Vec::new(),
                        banked_resets: 0,
                        banked_expires_at_ms: None,
                        prepaid_credits: false,
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        for head in [false, true] {
            let (objective, _) = compute_objective(
                &route,
                &runtime,
                true,
                CostBasis::TaskProfile {
                    profile: &profile(840000.0),
                    omit_cache_write: false,
                },
                head,
            );
            let pricing = objective.task_pricing.as_ref().unwrap();
            close(pricing.list_cost_usd.unwrap(), 0.84);
            close(pricing.value_multiplier, 18.7);
            assert_eq!(pricing.value_source, "astrocyte fresh");
            close(pricing.pressure, 2.0);
            close(pricing.final_cost_usd.unwrap(), 0.84 / 18.7 * 2.0);
            close(objective.quota, 0.0);
        }
    }

    #[test]
    fn task_profiles_supersede_class_vectors_and_thin_fleets_stay_cold() {
        let mut fact = model("api", "sol");
        fact.class_vector = Some(ClassVector {
            workload_class: "ephemeral".into(),
            fresh_input: 9000000.0,
            output_excluding_reasoning: 9000000.0,
            reasoning: 0.0,
            cached_read: 0.0,
            cache_write: 0.0,
            sample_count: 50,
            trailing_window_days: 7.0,
            statistic: "mean".into(),
            is_overlay: false,
        });
        let facts = vec![fact];
        let profiles = profiles(&facts, vec![profile(1000.0)]);
        let selected = select(input(facts.clone(), false), &profiles);
        close(
            selected
                .score
                .objective
                .task_pricing
                .unwrap()
                .list_cost_usd
                .unwrap(),
            0.001,
        );
        let tasks: Vec<_> = (0..19)
            .map(|_| ClosedTask {
                workload: "mason",
                model: &facts[0],
                iq_bucket: 7,
                tokens: TokenVector {
                    output_excluding_reasoning: 1000.0,
                    ..Default::default()
                },
            })
            .collect();
        assert!(compute_profile(&tasks, &facts[0], "mason", 7).is_none());
        let selected = select(input(facts, false), &RouteTaskProfiles::new());
        let pricing = selected.score.objective.task_pricing.unwrap();
        assert!(pricing.profile.is_none());
        close(pricing.list_cost_usd.unwrap(), 18.0);
        assert!(selected.priced_with_basket);
    }

    #[test]
    fn profile_ordering_has_neither_legacy_pool_minimum_nor_free_bucket() {
        let routes: Vec<_> = [("plan/expensive", 0.002, 1.0), ("plan/cheap", 0.001, 0.99)]
            .into_iter()
            .map(|(route_key, selection_cost, adequacy)| RouteOrderFacts {
                price_unknown: false,
                provider_id: "plan",
                route_key,
                subscription_covered: true,
                quota_priced: false,
                profile_priced: true,
                selection_cost,
                uncached: false,
                adequacy,
                raw_adequacy: adequacy,
                quota_abundance: None,
                speed_score: 1.0,
                metered_cost: 1.0,
            })
            .collect();
        let keys = route_order_keys(&routes);
        close(keys[0].cost_key, 0.002);
        close(keys[1].cost_key, 0.001);
        assert!(!keys[0].in_bucket && !keys[1].in_bucket);
        assert!(compare_route_order_keys(&keys[1], &keys[0], false).is_lt());
    }

    #[test]
    fn cache_write_omission_is_driven_by_the_reporting_set() {
        for unreported in [false, true] {
            let fact = model("provider", "model");
            let mut profile = profile(0.0);
            profile.tokens = TokenVector {
                cache_write: 1000000.0,
                fresh_input: 1000.0,
                ..Default::default()
            };
            let profiles = profiles(std::slice::from_ref(&fact), vec![profile]);
            let mut request = input(vec![fact], false);
            if unreported {
                request
                    .providers_without_cache_write_reports
                    .insert("provider".into());
            }
            let selected = select(request, &profiles);
            let pricing = selected.score.objective.task_pricing.unwrap();
            close(
                pricing.list_cost_usd.unwrap(),
                if unreported { 0.001 } else { 1.001 },
            );
            assert_eq!(
                pricing.omitted_dimensions,
                if unreported {
                    vec!["cache_write"]
                } else {
                    vec![]
                }
            );
            assert!(pricing.unpriced_dimensions.is_empty());
        }
    }
}

#[cfg(test)]
mod abundance_tests {
    use super::*;
    use crate::model_facts::{make_capacity_key, Confidence, ModelScores};

    fn fact(provider: &str, model: &str, speed: f64) -> ModelFact {
        fact_with_cost_and_elo(provider, model, speed, Some(0.0), 1_800.0)
    }

    fn fact_with_cost_and_elo(
        provider: &str,
        model: &str,
        speed: f64,
        cost_per_m_tok_out: Option<f64>,
        elo: f64,
    ) -> ModelFact {
        ModelFact {
            billing_plane: None,
            auth_methods: None,
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            speed_is_fleet_median: false,
            provider_id: provider.into(),
            model_id: model.into(),
            variant: None,
            family: None,
            scores: ModelScores {
                elo,
                eq: 1_800.0,
                speed,
            },
            context_window: None,
            max_output_tokens: None,
            cost_tier: None,
            cost_per_m_tok_out,
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
            upstream_status: None, model_type: None, model_kind: None,
        }
    }

    #[derive(Default)]
    struct SelectOptions<'a> {
        requirements: Option<ModelRequirements>,
        exclude_route_keys: Vec<String>,
        is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
        prefer_prompt_cache: bool,
        providers_without_prompt_cache: HashSet<String>,
        slots: Option<ConcurrencySlots>,
    }

    fn select_with(
        facts: Vec<ModelFact>,
        states: &[(&str, Option<f64>, Option<&str>)],
        options: SelectOptions<'_>,
    ) -> SelectedModelRoute {
        let available_models = facts
            .iter()
            .map(|fact| make_capacity_key(&fact.provider_id, &fact.model_id))
            .collect();
        let by_capacity = states
            .iter()
            .map(|(capacity, abundance, window)| {
                (
                    (*capacity).to_string(),
                    RouteRuntimeState {
                        quota_abundance: *abundance,
                        quota_abundance_window: window.map(|value| value.to_string()),
                        ..Default::default()
                    },
                )
            })
            .collect();
        // Every fixture route sits at the frontier and the default demand is
        // iq 20, so the over-qualification term would be equal for all of
        // them; it is switched off so the bucket ordering under test is reached.
        select_model_route_with_availability(
            SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
                requirements: options
                    .requirements
                    .unwrap_or(ModelRequirements { iq: 20.0, eq: 20.0 }),
                available_models,
                facts,
                runtime_snapshot: Some(ModelRouteStateSnapshot {
                    now: 1_000.0,
                    by_route: HashMap::new(),
                    by_capacity,
                }),
                exclude_route_keys: options.exclude_route_keys,
                operator_exclude_keys: Vec::new(),

                required_capabilities: None,
                is_subscription_route: options.is_subscription_route,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: options.prefer_prompt_cache,
                providers_without_prompt_cache: options.providers_without_prompt_cache,
                providers_without_cache_write_reports: HashSet::new(),
            },
            &crate::routing_config::ROUTING_CONFIG_WITHOUT_OVERQUALIFICATION,
            options.slots.as_ref(),
            &[],
            &HashMap::new(),
        )
        .expect("fixture has an admissible route")
    }

    fn select(
        facts: Vec<ModelFact>,
        states: &[(&str, Option<f64>, Option<&str>)],
    ) -> SelectedModelRoute {
        select_with(facts, states, SelectOptions::default())
    }

    #[test]
    fn yielded_capacity_preference_preserves_quality_exclusions_and_saturation() {
        let facts = vec![
            fact_with_cost_and_elo("cheap", "model", 50.0, Some(1.0), 1_800.0),
            fact_with_cost_and_elo("preferred", "model", 50.0, Some(2.0), 1_800.0),
            fact_with_cost_and_elo("inadequate", "model", 50.0, Some(0.0), 200.0),
        ];
        let choose = |preference: Option<&str>, full: bool, exclude: bool| {
            select_with(
                facts.clone(),
                &[],
                SelectOptions {
                    requirements: Some(ModelRequirements { iq: 90.0, eq: 20.0 }),
                    exclude_route_keys: if exclude {
                        vec!["preferred/model".into()]
                    } else {
                        vec![]
                    },
                    slots: Some(
                        ConcurrencySlots::new(if full {
                            vec![crate::slot_saturation::ConcurrencySlotState {
                                key: "preferred/model".into(),
                                running: 1,
                                limit: 1,
                                queued: 1,
                            }]
                        } else {
                            vec![]
                        })
                        .with_preferred_capacity(preference.map(str::to_string)),
                    ),
                    ..Default::default()
                },
            )
            .capacity_key
        };
        assert_eq!(choose(None, false, false), "cheap/model");
        assert_eq!(
            choose(Some("preferred/model"), false, false),
            "preferred/model"
        );
        assert_eq!(
            choose(Some("inadequate/model"), false, false),
            "cheap/model"
        );
        assert_eq!(choose(Some("preferred/model"), false, true), "cheap/model");
        assert_eq!(choose(Some("preferred/model"), true, false), "cheap/model");
    }

    #[test]
    fn objective_without_abundance_is_byte_identical_to_the_previous_shape() {
        let objective = ObjectiveBreakdown {
            price_unknown: false,
            subscription_pricing: None,
            cost: 0.25,
            quota: 0.125,
            speed_score: 0.75,
            selection_cost: 0.375,
            overqualification: 0.0,
            subscription_covered: false,
            quota_abundance: None,
            quota_abundance_window: None,
            basket_pricing: None,
            task_pricing: None,
        };

        assert_eq!(
            serde_json::to_string(&objective).expect("serialize objective"),
            r#"{"cost":0.25,"quota":0.125,"speedScore":0.75,"selectionCost":0.375}"#
        );
    }

    #[test]
    fn decision_record_carries_abundance_value_and_window_identity() {
        let decision = select(
            vec![fact("openai", "gpt", 50.0)],
            &[("openai/gpt", Some(4.0), Some("codexbar:codex:primary"))],
        );
        let objective = &decision.score.objective;
        assert_eq!(objective.quota_abundance, Some(4.0));
        assert_eq!(
            objective.quota_abundance_window.as_deref(),
            Some("codexbar:codex:primary")
        );
        let json = serde_json::to_value(&decision).expect("serialize decision");
        assert_eq!(json["score"]["objective"]["quotaAbundance"], 4.0);
        assert_eq!(
            json["score"]["objective"]["quotaAbundanceWindow"],
            "codexbar:codex:primary"
        );
        assert!(decision
            .explanation
            .iter()
            .any(|line| line == "quota abundance 4.0000 (window codexbar:codex:primary)"));
    }

    #[test]
    fn abundances_with_descending_speeds_sort_identically_under_every_permutation_with_no_panic_or_cycle(
    ) {
        let r1 = fact("p", "r1", 150.0);
        let r2 = fact("p", "r2", 100.0);
        let r3 = fact("p", "r3", 50.0);

        let states = [
            ("p/r1", Some(1.0000), Some("w1")),
            ("p/r2", Some(1.0020), Some("w2")),
            ("p/r3", Some(1.0040), Some("w3")),
        ];

        let permutations = [
            vec![r1.clone(), r2.clone(), r3.clone()],
            vec![r1.clone(), r3.clone(), r2.clone()],
            vec![r2.clone(), r1.clone(), r3.clone()],
            vec![r2.clone(), r3.clone(), r1.clone()],
            vec![r3.clone(), r1.clone(), r2.clone()],
            vec![r3.clone(), r2.clone(), r1],
        ];

        for perm in permutations {
            let first = select(perm.clone(), &states);
            assert_eq!(first.route_key, "p/r3");

            let second = select_with(
                perm.clone(),
                &states,
                SelectOptions {
                    exclude_route_keys: vec!["p/r3".into()],
                    ..Default::default()
                },
            );
            assert_eq!(second.route_key, "p/r2");

            let third = select_with(
                perm,
                &states,
                SelectOptions {
                    exclude_route_keys: vec!["p/r3".into(), "p/r2".into()],
                    ..Default::default()
                },
            );
            assert_eq!(third.route_key, "p/r1");
        }
    }

    #[test]
    fn abundance_fall_through_to_speed_tie_break() {
        // 1.0000 and 1.0004 round to the same integer quantum (1000) and fall through to speed.
        let decision = select(
            vec![
                fact("scarce", "fast", 100.0),
                fact("abundant", "slow", 10.0),
            ],
            &[
                ("scarce/fast", Some(1.0000), Some("w1")),
                ("abundant/slow", Some(1.0004), Some("w2")),
            ],
        );
        assert_eq!(
            decision.route_key, "scarce/fast",
            "equal abundance within quantum must fall through to the speed tie-break"
        );
    }

    #[test]
    fn two_routes_with_unequal_blended_adequacy_order_by_adequacy_regardless_of_abundance() {
        let higher_adequacy = fact_with_cost_and_elo("p", "adequate", 10.0, Some(0.0), 2_000.0);
        let lower_adequacy =
            fact_with_cost_and_elo("p", "less_adequate", 100.0, Some(0.0), 1_800.0);

        let decision = select_with(
            vec![higher_adequacy, lower_adequacy],
            &[
                ("p/adequate", Some(1.0000), Some("w1")),
                ("p/less_adequate", Some(4.0000), Some("w2")),
            ],
            SelectOptions {
                requirements: Some(ModelRequirements { iq: 100.0, eq: 0.0 }),
                ..Default::default()
            },
        );
        assert_eq!(
            decision.route_key, "p/adequate",
            "unequal blended adequacy must order by adequacy regardless of abundance"
        );
    }

    #[test]
    fn mixed_pair_on_opposite_sides_of_effectively_free_orders_by_cost_and_never_reaches_abundance()
    {
        let free_route = fact_with_cost_and_elo("p", "free_scarce", 10.0, Some(0.0), 1_800.0);
        let costly_route =
            fact_with_cost_and_elo("p", "costly_abundant", 100.0, Some(3.0), 1_800.0);

        let decision = select(
            vec![free_route, costly_route],
            &[
                ("p/free_scarce", Some(0.5000), Some("w-free")),
                ("p/costly_abundant", Some(4.0000), Some("w-costly")),
            ],
        );
        assert_eq!(
            decision.route_key, "p/free_scarce",
            "routes on opposite sides of effectively-free must order by cost and never reach abundance"
        );
    }

    #[test]
    fn non_free_routes_with_identical_clamped_cost_order_by_speed_and_ignore_abundance() {
        let fast_route = fact_with_cost_and_elo("p", "fast", 100.0, Some(100.0), 1_800.0);
        let slow_route = fact_with_cost_and_elo("p", "slow", 10.0, Some(100.0), 1_800.0);

        let decision = select(
            vec![fast_route, slow_route],
            &[
                ("p/fast", Some(1.0000), Some("w-fast")),
                ("p/slow", Some(2.0000), Some("w-slow")),
            ],
        );
        assert_eq!(
            decision.route_key, "p/fast",
            "non-free routes must order by speed tie-break and ignore abundance; removing a_free && b_free guard reddens this"
        );
    }

    #[test]
    fn abundant_metered_route_outranks_scarce_subscription_route_in_free_bucket() {
        let metered_route = fact_with_cost_and_elo("metered", "model", 10.0, Some(0.0), 1_800.0);
        let sub_route = fact_with_cost_and_elo("sub", "model", 100.0, None, 1_800.0);

        let decision = select_with(
            vec![metered_route, sub_route],
            &[
                ("metered/model", Some(4.0000), Some("metered-window")),
                ("sub/model", Some(1.0000), Some("sub-window")),
            ],
            SelectOptions {
                is_subscription_route: Some(Box::new(|r: &ModelRoute| r.fact.provider_id == "sub")),
                ..Default::default()
            },
        );
        assert_eq!(
            decision.route_key, "metered/model",
            "abundant metered route must outrank scarce subscription route in free bucket; restricting step to subscription routes reddens this"
        );
    }

    #[test]
    fn route_with_no_usable_window_scores_neutral_abundance() {
        let abundant_over_neutral = select(
            vec![
                fact("abundant", "slow", 10.0),
                fact("neutral", "fast", 100.0),
            ],
            &[("abundant/slow", Some(1.0020), Some("w1"))],
        );
        assert_eq!(
            abundant_over_neutral.route_key, "abundant/slow",
            "route with at least one quantum more abundance must be preferred over neutral"
        );

        let neutral_over_scarce = select(
            vec![fact("neutral", "slow", 10.0), fact("scarce", "fast", 100.0)],
            &[("scarce/fast", Some(0.9980), Some("w2"))],
        );
        assert_eq!(
            neutral_over_scarce.route_key, "neutral/slow",
            "neutral route must never be demoted below a route at least one quantum scarcer"
        );

        let all_neutral = select(vec![fact("p", "fast", 100.0), fact("p", "slow", 10.0)], &[]);
        assert_eq!(
            all_neutral.route_key, "p/fast",
            "bucket where every route is neutral orders by speed tie-break"
        );
    }

    #[test]
    fn prefer_prompt_cache_partition_survives_and_shows_separate_explanation_lines() {
        let cached = fact("cached_provider", "model", 50.0);
        let uncached = fact("uncached_provider", "model", 100.0);

        let decision = select_with(
            vec![cached, uncached],
            &[
                ("cached_provider/model", Some(1.0000), Some("cache-window")),
                (
                    "uncached_provider/model",
                    Some(4.0000),
                    Some("uncached-window"),
                ),
            ],
            SelectOptions {
                prefer_prompt_cache: true,
                providers_without_prompt_cache: HashSet::from(["uncached_provider".into()]),
                ..Default::default()
            },
        );

        assert_eq!(
            decision.route_key, "cached_provider/model",
            "prompt-cache preference must partition out uncached provider even if uncached has higher abundance"
        );
        assert_eq!(
            decision.score.objective.selection_cost,
            decision.score.objective.cost + decision.score.objective.quota
        );
        assert_eq!(decision.score.objective.selection_cost, 0.0);

        assert!(
            decision
                .explanation
                .iter()
                .any(|line| line == "quota abundance 1.0000 (window cache-window)"),
            "explanation must contain abundance line: {:?}",
            decision.explanation
        );
        assert!(
            decision
                .explanation
                .iter()
                .any(|line| line.contains("(tiebreak)")),
            "explanation must contain speed line: {:?}",
            decision.explanation
        );
    }
}

#[cfg(test)]
mod route_order_tests {
    use super::*;

    fn route<'r>(
        provider_id: &'r str,
        route_key: &'r str,
        subscription_covered: bool,
        selection_cost: f64,
        adequacy: f64,
    ) -> RouteOrderFacts<'r> {
        RouteOrderFacts {
            price_unknown: false,
            quota_priced: false,
            profile_priced: false,
            provider_id,
            route_key,
            subscription_covered,
            selection_cost,
            uncached: false,
            adequacy,
            raw_adequacy: adequacy,
            quota_abundance: None,
            speed_score: 0.5,
            metered_cost: selection_cost,
        }
    }

    fn sorted_keys<'r>(routes: &[RouteOrderFacts<'r>], best_effort: bool) -> Vec<&'r str> {
        let mut keys = route_order_keys(routes);
        keys.sort_by(|a, b| compare_route_order_keys(a, b, best_effort));
        keys.into_iter().map(|k| k.route_key).collect()
    }

    // A and B are subscription routes on one provider; C is metered elsewhere.
    // Compared pairwise the old rule made A < B (same pool, so adequacy
    // decides), B < C (not a pool pair and neither free, so cost decides) and
    // C < A (cost again): a cycle, which Rust's sort may answer with a panic.
    // With per-route keys A and B both carry their pool's cheapest cost, 0.10,
    // and C carries its own 0.20. All three sit above the 0.05 free threshold,
    // so the cost keys are A=0.10, B=0.10, C=0.20: the pool sorts before C,
    // and inside the pool A's higher adequacy puts it first. Order: A, B, C,
    // whatever order the routes arrive in.
    #[test]
    fn same_pool_cycle_sorts_to_one_order() {
        let a = route("plan", "plan/a", true, 0.40, 1.00);
        let b = route("plan", "plan/b", true, 0.10, 0.90);
        let c = route("metered", "metered/c", false, 0.20, 0.95);
        let arrivals = [
            vec![a.clone(), b.clone(), c.clone()],
            vec![a.clone(), c.clone(), b.clone()],
            vec![b.clone(), a.clone(), c.clone()],
            vec![b.clone(), c.clone(), a.clone()],
            vec![c.clone(), a.clone(), b.clone()],
            vec![c.clone(), b.clone(), a.clone()],
        ];
        for routes in arrivals {
            assert_eq!(
                sorted_keys(&routes, false),
                vec!["plan/a", "plan/b", "metered/c"]
            );
        }
    }

    // Same pool: the pool's cheapest route sets the cost every member is
    // compared on, so the cheaper sibling's price pulls the dearer, stronger
    // route ahead of a metered route that is cheaper than the dearer route
    // alone. Pricing each covered route at its own cost puts the metered route
    // first; this pins the pool minimum.
    #[test]
    fn pool_members_carry_the_pool_minimum_cost() {
        let routes = vec![
            route("plan", "plan/strong", true, 0.40, 1.00),
            route("plan", "plan/cheap", true, 0.10, 0.90),
            route("metered", "metered/mid", false, 0.20, 1.00),
        ];
        assert_eq!(sorted_keys(&routes, false)[0], "plan/strong");
    }

    /// xorshift64: a tiny deterministic generator so failures replay exactly.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        /// One of a few coarse values, so ties (the interesting case) are common.
        fn pick(&mut self, values: &[f64]) -> f64 {
            values[self.below(values.len() as u64) as usize]
        }
    }

    // Generated route sets over a few providers with random costs, adequacies
    // and subscription flags: sorting never panics, and the sorted order is
    // consistent under a pairwise check (no later route compares strictly
    // before an earlier one, and the comparison is antisymmetric).
    #[test]
    fn generated_route_sets_sort_to_a_pairwise_consistent_order() {
        let providers = ["p0", "p1", "p2"];
        let costs = [0.0, 0.03, 0.05, 0.06, 0.10, 0.20, 0.40, 1.0];
        let adequacies = [0.80, 0.90, 0.95, 1.00];
        let abundances = [None, Some(0.2), Some(0.9), Some(f64::NAN)];
        let names: Vec<String> = (0..40).map(|i| format!("r{i}")).collect();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for trial in 0..2_000 {
            let len = 2 + rng.below(if trial % 10 == 0 { 38 } else { 6 }) as usize;
            let best_effort = rng.below(4) == 0;
            let routes: Vec<RouteOrderFacts<'_>> = (0..len)
                .map(|i| {
                    let mut r = route(
                        providers[rng.below(3) as usize],
                        &names[i],
                        rng.below(2) == 0,
                        rng.pick(&costs),
                        rng.pick(&adequacies),
                    );
                    r.uncached = rng.below(5) == 0;
                    r.quota_abundance = abundances[rng.below(4) as usize];
                    r.speed_score = rng.pick(&[0.3, 0.5, 0.8]);
                    r
                })
                .collect();
            let mut keys = route_order_keys(&routes);
            keys.sort_by(|a, b| compare_route_order_keys(a, b, best_effort));
            for i in 0..keys.len() {
                for j in (i + 1)..keys.len() {
                    let forward = compare_route_order_keys(&keys[i], &keys[j], best_effort);
                    let backward = compare_route_order_keys(&keys[j], &keys[i], best_effort);
                    assert_ne!(
                        forward,
                        std::cmp::Ordering::Greater,
                        "trial {trial}: {} sorted before {} but compares after it",
                        keys[i].route_key,
                        keys[j].route_key
                    );
                    assert_eq!(
                        forward,
                        backward.reverse(),
                        "trial {trial}: not antisymmetric"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod availability_credential_tests {
    use super::*;

    #[test]
    fn unavailable_routes_preserve_credential_refusal_without_hiding_other_unavailability() {
        let facts = ["accountless", "unavailable"]
            .into_iter()
            .map(|model| {
                serde_json::from_value(serde_json::json!({
                    "providerID":"vendor", "modelID":model, "confidence":"curated",
                    "scores":{"elo":1800.0,"eq":1900.0,"speed":100.0}
                }))
                .unwrap()
            })
            .collect();
        let result = select_model_route(SelectModelRouteInput {
            target_agent: "fixture-consumer".into(),
            target_is_head: false,
            requirements: ModelRequirements { iq: 20.0, eq: 20.0 },
            available_models: HashSet::new(),
            facts,
            runtime_snapshot: Some(ModelRouteStateSnapshot {
                now: 1_000.0,
                by_route: HashMap::new(),
                by_capacity: HashMap::from([(
                    "vendor/accountless".into(),
                    RouteRuntimeState {
                        credential_refusal: Some(CredentialRefusalMark {
                            reason: "no_eligible_account".into(),
                            credential_ids: vec!["credential-a".into()],
                        }),
                        ..Default::default()
                    },
                )]),
            }),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: None,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: HashSet::new(),
            providers_without_cache_write_reports: HashSet::new(),
        })
        .unwrap_err();
        let accountless = result
            .rejected
            .iter()
            .find(|route| route.route_key == "vendor/accountless")
            .unwrap();
        assert_eq!(accountless.reason, "no_eligible_account");
        assert_eq!(accountless.credential_ids, ["credential-a"]);
        let unavailable = result
            .rejected
            .iter()
            .find(|route| route.route_key == "vendor/unavailable")
            .unwrap();
        assert_eq!(unavailable.reason, "not_available");
        assert!(unavailable.credential_ids.is_empty());
    }
}
