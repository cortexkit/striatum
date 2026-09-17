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
    fact_to_route, make_capacity_key, ModelFact, ModelMetric, ModelRequirements, ModelRoute,
    ScoreProvenance, FAST_SUFFIX,
};
use crate::model_resolution::{use_basket_permitted, CLASS_PRODUCER, COUNTERS_PRODUCER};
use crate::provider_model_spec::{
    parse_spec_list, resolve_spec, ExclusionPolicy, ExclusionSource, ExclusionSpecs,
};
use crate::route_scoring::{score_adequacy, AdequacyScore, MetricFit};
use crate::routing_config::{
    RoutingConfig, TopRoutableScores, ADEQUACY_FLOOR, COST_REFERENCE_PER_MTOK,
    DEFAULT_ROUTING_CONFIG, EFFECTIVELY_FREE_SELECTION_COST, MIN_BEST_EFFORT_ADEQUACY,
    OBJECTIVE_WEIGHT_QUOTA, RELATIVE_SHORTLIST_DELTA, SPEED_CEILING_TPS, SPEED_FLOOR_TPS,
};

/// A route the operator's standing policy fences off. Distinct from [`REASON_EXCLUDED`]
/// so a reader of a rejection can tell "the machine owner said never" from "this
/// attempt skipped it". Both the producer and every consumer that maps rejection
/// reasons use these constants, so renaming one cannot silently split the mapping.
pub const REASON_EXCLUDED_BY_OPERATOR: &str = "excluded_by_operator";
/// A route the CALLER excluded for this attempt: the route that just failed on a
/// reroute, or a consult's `exclude_route_keys`.
pub const REASON_EXCLUDED: &str = "excluded";

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RouteRuntimeState {
    /// Confirmed cooldown/retry-after; route inadmissible until this time (ms epoch).
    pub cooldown_until: Option<f64>,
    /// Confirmed quota exhaustion until this time (ms epoch).
    pub quota_exhausted_until: Option<f64>,
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

/// Input to [`select_model_route`]. `is_subscription_route` mirrors the TS
/// optional predicate: a route the caller logs into as flat-rate subscription.
/// It is reported in the decision explanation (subscription coverage) but does NOT
/// zero the cost term — every route is priced from its model fact. `None` → treat
/// everything as metered.
pub struct SelectModelRouteInput<'a> {
    pub target_agent: String,
    pub requirements: ModelRequirements,
    /// Set of available routeKeys AND/OR capacityKeys the caller is logged into.
    pub available_models: HashSet<String>,
    pub facts: Vec<ModelFact>,
    pub runtime_snapshot: Option<ModelRouteStateSnapshot>,
    /// Per-attempt exclusions supplied by the caller (a route that just failed).
    pub exclude_route_keys: Vec<String>,
    /// The operator's standing exclude list, applied as the baseline under the
    /// caller's. Union only: see [`ExclusionPolicy`].
    pub operator_exclude_keys: Vec<String>,
    /// Base-model specs that deliberately admit matching priority-tier siblings.
    pub prefer_fast_specs: Vec<String>,
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectiveBreakdown {
    /// Continuous cost component (output $/Mtok ÷ reference, clamped 0..1).
    pub cost: f64,
    /// Soft quota pressure component (0..1 · weight).
    pub quota: f64,
    /// Normalized speed (0..1, higher = faster). Tie-break only, NOT in selectionCost.
    pub speed_score: f64,
    /// cost + quota (purely economic); lower is a better pick. Speed breaks ties.
    pub selection_cost: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_abundance: Option<f64>,
    /// Owned audit identity carried into the decision record. This allocates once
    /// per scored candidate; shortlists are normally tens of routes. If they grow
    /// by an order of magnitude, sharing provider window keys with `Arc<str>` is
    /// preferable to repeating these short strings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_abundance_window: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedRoute {
    pub route_key: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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
    pub adequacy: f64,
    pub raw_adequacy: f64,
    pub selection_cost: f64,
    pub metrics: BTreeMap<ModelMetric, MetricFit>,
    pub objective: ObjectiveBreakdown,
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
}

/// One shortlisted route priced in the selection it actually sat in.
#[derive(Debug, Clone, PartialEq)]
pub struct PricedShortlistEntry {
    pub route_key: String,
    /// Cost component of the objective (clamped 0..1), not including quota.
    pub cost: f64,
    /// Economic selection cost (cost + quota) used by the free-bucket gate.
    pub selection_cost: f64,
    /// Cached-read term in list dollars: `cached_read * cache_read_rate / 1e6`.
    pub cached_read_term_usd: Option<f64>,
}

/// The shortlist `select_model_route` would price, with the whole-selection
/// basket verdict. A route is effectively free in a replay when its
/// `selection_cost` here is at or below [`EFFECTIVELY_FREE_SELECTION_COST`].
#[derive(Debug, Clone, PartialEq)]
pub struct PricedShortlist {
    pub use_basket: bool,
    /// First shortlisted route missing a required basket rate or a usable
    /// ephemeral vector. Independent of the producer flags: when those flags
    /// forbid the basket, this still names the rate/vector degrader if any.
    pub degraded_by: Option<String>,
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
    pub reason: RouteFailureReason,
    pub message: String,
    pub rejected: Vec<RejectedRoute>,
    pub requirements: Option<ModelRequirements>,
    /// For `AllRoutesInadequate`: the best rejected candidate (highest adequacy
    /// below the band), so the caller can surface which route was closest.
    pub best_rejected: Option<RejectedCandidate>,
    /// For `AllRoutesInadequate`: the earliest quota/cooldown reset timestamp
    /// across blocked candidates (millisecond epoch), reused from the break-glass
    /// pass. `None` when no blocked candidate has a future reset.
    pub earliest_reset: Option<f64>,
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
    fact.cost_per_m_tok_out
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
    if let Some(cost_tier) = fact.cost_tier {
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

fn merged_runtime_state(
    route: &ModelRoute,
    snapshot: Option<&ModelRouteStateSnapshot>,
) -> RouteRuntimeState {
    let by_route = snapshot.and_then(|s| s.by_route.get(&route.route_key));
    let by_capacity = snapshot.and_then(|s| s.by_capacity.get(&route.capacity_key));
    let mut merged = by_capacity.cloned().unwrap_or_default();
    if let Some(route_state) = by_route {
        // Hard blocks compose across both scopes: a stale route deadline cannot
        // shorten a live capacity-wide block.
        merged.cooldown_until = latest_deadline(merged.cooldown_until, route_state.cooldown_until);
        merged.quota_exhausted_until = latest_deadline(
            merged.quota_exhausted_until,
            route_state.quota_exhausted_until,
        );
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
/// The request frontier is computed after static exclusions, availability, and
/// capability requirements, but before quota or cooldown state.
pub fn admit_routes(
    facts: &[ModelFact],
    runtime_snapshot: Option<&ModelRouteStateSnapshot>,
    exclusions: ExclusionPolicy<'_>,
    prefer_fast_specs: &[String],
    exact_model_pins: &[String],
    required_capabilities: Option<RequiredCapabilities>,
    available_models: &HashSet<String>,
    requirements: &ModelRequirements,
    config: &RoutingConfig,
) -> AdmissionResult {
    let now = runtime_snapshot.map(|s| s.now).unwrap_or_else(now_millis);
    let exclusion_specs = ExclusionSpecs::parse(exclusions);
    let prefer_fast_specs = parse_spec_list(prefer_fast_specs);
    let mut static_candidates = Vec::new();
    let mut top_routable = TopRoutableScores::default();
    let mut rejected = Vec::new();

    for fact in facts {
        let route = fact_to_route(fact);
        if fact.score_provenance == ScoreProvenance::Unscored {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "unscored_awaiting_placement".to_string(),
                detail: None,
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
            });
            continue;
        }
        if !is_route_available(&route, available_models) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "not_available".to_string(),
                detail: None,
            });
            continue;
        }
        if let Some(reason) = capability_rejection_reason(fact, required_capabilities) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: reason.to_string(),
                detail: None,
            });
            continue;
        }
        if let Some(base_model_id) = fact.model_id.strip_suffix(FAST_SUFFIX) {
            let base_exists = facts.iter().any(|candidate| {
                candidate.provider_id == fact.provider_id && candidate.model_id == base_model_id
            });
            let base_key = make_capacity_key(&fact.provider_id, base_model_id);
            let is_exact_pin = exact_model_pins.iter().any(|pin| {
                let pin = pin.trim();
                pin.eq_ignore_ascii_case(&route.capacity_key)
                    || pin.eq_ignore_ascii_case(&route.route_key)
            });
            let opted_in = resolve_spec(&prefer_fast_specs, &base_key, false)
                && resolve_spec(&prefer_fast_specs, &route.capacity_key, true);
            if base_exists && !is_exact_pin && !opted_in {
                rejected.push(RejectedRoute {
                    route_key: route.route_key,
                    reason: "fast_variant_not_opted_in".to_string(),
                    detail: None,
                });
                continue;
            }
        }
        top_routable.include(fact.scores);
        static_candidates.push((fact, route));
    }

    let mut admitted = Vec::new();
    for (fact, route) in static_candidates {
        let runtime = merged_runtime_state(&route, runtime_snapshot);
        // A wall rejection names the deadline it was compared against. Without it a
        // ledger row reading `quota_exhausted` cannot be told apart from a stale stored
        // wall, a live overlay wall, or a clock in the wrong unit, and the operator's
        // own quota view showing free capacity has nothing to argue with.
        if let Some(until) = runtime.cooldown_until.filter(|until| *until > now) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "cooldown_active".to_string(),
                detail: Some(wall_detail(until, now)),
            });
            continue;
        }
        if let Some(until) = runtime.quota_exhausted_until.filter(|until| *until > now) {
            rejected.push(RejectedRoute {
                route_key: route.route_key,
                reason: "quota_exhausted".to_string(),
                detail: Some(wall_detail(until, now)),
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

/// Cached-read term in list dollars. None when the ephemeral vector or the
/// cache-read rate is missing; an explicit zero rate is a present term of $0.
fn cached_read_term_usd(fact: &ModelFact) -> Option<f64> {
    let vector = fact.ephemeral_vector()?;
    let rate = fact.cache_read_rate?;
    Some((vector.cached_read * rate) / 1_000_000.0)
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
/// The selection cost of a route. An unpublished price under a subscription plan is
/// a zero marginal token cost: the plan is the operator's own business fact, and
/// the catalog (models.dev via Fusiform) publishes no per-token rate for
/// subscription planes such as ollama-cloud at all. Ranking those routes at the
/// maximum priced every glm, deepseek, and kimi route on the plan above sol. An
/// unpublished price under a metered provider still ranks at the maximum.
pub fn selection_cost(fact: &ModelFact, is_subscription: bool, use_basket: bool) -> f64 {
    if use_basket {
        if let Some(basket_cost) = fact.basket_cost() {
            return basket_cost;
        }
    }
    if is_subscription && fact.cost_per_m_tok_out.is_none() && fact.cost_tier.is_none() {
        return 0.0;
    }
    normalized_metered_cost(fact)
}

pub(crate) fn compute_objective(
    route: &ModelRoute,
    runtime: &RouteRuntimeState,
    is_subscription: bool,
    use_basket: bool,
) -> (ObjectiveBreakdown, Vec<String>) {
    let mut notes: Vec<String> = Vec::new();

    let cost = selection_cost(&route.fact, is_subscription, use_basket);
    let multiplier = subscription_multiplier(&route.fact);
    let coverage = if is_subscription {
        "subscription-covered"
    } else {
        "metered"
    };
    let detail = if use_basket {
        let quantity = if is_subscription {
            "basket-per-message quota proxy"
        } else {
            "list dollars"
        };
        let cached_read_term = cached_read_term_usd(&route.fact);
        let mut detail = if let Some(cr) = cached_read_term {
            format!("basket pricing ({quantity}), cached-read term ${:.4}", cr)
        } else {
            format!("basket pricing ({quantity})")
        };
        if (multiplier - 1.0).abs() > f64::EPSILON {
            detail.push_str(&format!(", x{} subsidy", fmt_num(multiplier)));
        }
        detail.push_str(&format!(", {coverage}"));
        detail
    } else if let Some(list_price) = route.fact.cost_per_m_tok_out {
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
    } else if let Some(tier) = route.fact.cost_tier {
        format!("tier {}, {coverage}", fmt_num(tier))
    } else if is_subscription {
        "price unpublished, subscription plan: marginal token cost zero".to_string()
    } else {
        // An unpriced metered route is ranked at the MAXIMUM cost (see
        // `selection_cost`), never as free. Say so on the line: a route losing
        // every cost comparison because nobody knows its price looks identical
        // to one losing because it is genuinely expensive.
        format!("price unknown (ranked as max), {coverage}")
    };
    notes.push(format!("cost +{cost:.2} ({detail})"));

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
            cost,
            quota,
            speed_score,
            selection_cost,
            quota_abundance,
            quota_abundance_window,
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
}

struct PricedAdmission {
    admissible: Vec<AdmittedRoute>,
    rejected: Vec<RejectedRoute>,
    scored: Vec<Scored>,
    use_basket: bool,
    degraded_by: Option<String>,
    best_effort: bool,
    cleared_count: usize,
}

fn admit_and_price_shortlist(
    input: &SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    counters_producer: bool,
    class_producer: bool,
) -> Result<PricedAdmission, NoRouteError> {
    let requirements = input.requirements;

    if !requirements_are_valid(&requirements)
        || !config.demand_exponent.is_finite()
        || config.demand_exponent <= 0.0
    {
        return Err(NoRouteError {
            reason: RouteFailureReason::InvalidRequirements,
            message: "model_requirements must be numbers in 0..100".to_string(),
            rejected: Vec::new(),
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset: None,
        });
    }
    let capability_only = all_zero(&requirements);
    let caps = input.required_capabilities;
    let has_required_cap = caps
        .map(|c| c.vision == Some(true) || c.min_context.is_some())
        .unwrap_or(false);
    if capability_only && !has_required_cap {
        return Err(NoRouteError {
            reason: RouteFailureReason::InvalidRequirements,
            message: "all-zero model_requirements require a required capability (e.g. vision)"
                .to_string(),
            rejected: Vec::new(),
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset: None,
        });
    }

    let AdmissionResult {
        admitted: admissible,
        rejected,
        ..
    } = admit_routes(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy {
            operator: &input.operator_exclude_keys,
            caller: &input.exclude_route_keys,
        },
        &input.prefer_fast_specs,
        &[],
        caps,
        &input.available_models,
        &requirements,
        config,
    );

    if admissible.is_empty() {
        let earliest_reset =
            earliest_reset_for_rejections(&input.facts, input.runtime_snapshot.as_ref(), &rejected);
        return Err(NoRouteError {
            reason: classify_empty_admission(&rejected),
            message: "no admissible model route for requirements".to_string(),
            rejected,
            requirements: Some(requirements),
            best_rejected: None,
            earliest_reset,
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
    if best_effort && max_adequacy < min_band {
        // Find the best rejected candidate (highest adequacy below the band).
        let best_below_band = admissible.iter().max_by(|a, b| {
            a.adequacy
                .adequacy
                .partial_cmp(&b.adequacy.adequacy)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let best_rejected = best_below_band.map(|a| RejectedCandidate {
            route_key: a.route.route_key.clone(),
            adequacy: a.adequacy.adequacy,
        });
        let earliest_reset =
            earliest_reset_for_rejections(&input.facts, input.runtime_snapshot.as_ref(), &rejected);

        return Err(NoRouteError {
            reason: RouteFailureReason::AllRoutesInadequate,
            message: format!(
                "no route clears the adequacy floor ({:.2}); best effort adequacy {:.2} is below the sanity band ({:.2})",
                ADEQUACY_FLOOR, max_adequacy, min_band
            ),
            rejected,
            requirements: Some(requirements),
            best_rejected,
            earliest_reset,
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

    // Whole-selection basket gate: producers must both exist, the overlay's
    // ephemeral vector must be usable, and every shortlisted candidate must
    // serve the required rates. account_producer is not in this predicate.
    let degraded_by = shortlist.iter().find_map(|&idx| {
        let fact = &admissible[idx].route.fact;
        if fact.has_required_basket_rates()
            && fact.ephemeral_vector().is_some_and(|v| v.is_usable())
        {
            None
        } else {
            Some(admissible[idx].route.route_key.clone())
        }
    });
    let use_basket = use_basket_permitted(counters_producer, class_producer)
        && !shortlist.is_empty()
        && degraded_by.is_none();

    let scored: Vec<Scored> = shortlist
        .into_iter()
        .map(|idx| {
            let a = &admissible[idx];
            let is_sub = input
                .is_subscription_route
                .as_ref()
                .map(|pred| pred(&a.route))
                .unwrap_or(false);
            let (breakdown, notes) = compute_objective(&a.route, &a.runtime, is_sub, use_basket);
            Scored {
                idx,
                objective: breakdown,
                objective_notes: notes,
            }
        })
        .collect();

    Ok(PricedAdmission {
        admissible,
        rejected,
        scored,
        use_basket,
        degraded_by,
        best_effort,
        cleared_count,
    })
}

/// Prices every shortlisted route the way [`select_model_route`] would, including
/// the whole-selection basket verdict. The bucket-membership replay uses this so
/// a route is judged free in the selection it was actually priced in.
///
/// `counters_producer` and `class_producer` are the two flags that may engage
/// the basket; there is no `account_producer` argument because its absence must
/// never force the basket off.
///
/// Reference calibration is measured off the host stores, not invented.
/// Live routing `model_facts` is schema v35 and does not persist Fusiform rates.
/// Joining the scored routable catalog to latest Fusiform eras, every
/// reconstructed mason shortlist includes a route with no rate card, so the
/// basket does not engage. See the catalog-and-readings replay test.
pub fn price_selection_shortlist(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    counters_producer: bool,
    class_producer: bool,
) -> Result<PricedShortlist, NoRouteError> {
    let priced = admit_and_price_shortlist(&input, config, counters_producer, class_producer)?;
    Ok(PricedShortlist {
        use_basket: priced.use_basket,
        degraded_by: priced.degraded_by,
        routes: priced
            .scored
            .iter()
            .map(|s| {
                let admitted = &priced.admissible[s.idx];
                PricedShortlistEntry {
                    route_key: admitted.route.route_key.clone(),
                    cost: s.objective.cost,
                    selection_cost: s.objective.selection_cost,
                    cached_read_term_usd: cached_read_term_usd(&admitted.route.fact),
                }
            })
            .collect(),
    })
}

pub fn select_model_route_with_config(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
) -> Result<SelectedModelRoute, NoRouteError> {
    let PricedAdmission {
        admissible,
        rejected,
        mut scored,
        use_basket,
        best_effort,
        cleared_count,
        ..
    } = admit_and_price_shortlist(&input, config, COUNTERS_PRODUCER, CLASS_PRODUCER)?;

    // Deterministic ordering. COST-PRIMARY lexicographic among adequate models.
    scored.sort_by(|sa, sb| {
        use std::cmp::Ordering;
        let a = &admissible[sa.idx];
        let b = &admissible[sb.idx];
        // 1. (best-effort only) most-capable first.
        if best_effort && b.adequacy.adequacy != a.adequacy.adequacy {
            return cmp_f64(b.adequacy.adequacy, a.adequacy.adequacy);
        }
        // 2. Prompt-cache preference partitions otherwise eligible candidates.
        // It never lets a cache-capable route bypass the normal adequacy gate.
        if input.prefer_prompt_cache {
            let a_uncached = input
                .providers_without_prompt_cache
                .contains(&a.route.fact.provider_id);
            let b_uncached = input
                .providers_without_prompt_cache
                .contains(&b.route.fact.provider_id);
            if a_uncached != b_uncached {
                return a_uncached.cmp(&b_uncached);
            }
        }
        // 3. lowest economic selectionCost - EXCEPT inside the effectively-free
        // bucket, where two subsidized routes compare as equal cost and the
        // demand's own quality metric decides. Without the bucket, cost-primary
        // ordering lets a deeper subsidy outrank a stronger model on work that
        // explicitly asked for strength: the caller's iq demand raised the bar,
        // both routes cleared it, and the router then optimized a cent-scale
        // dollar difference the subsidy made meaningless.
        let a_free = sa.objective.selection_cost <= EFFECTIVELY_FREE_SELECTION_COST;
        let b_free = sb.objective.selection_cost <= EFFECTIVELY_FREE_SELECTION_COST;
        if !(a_free && b_free) && sa.objective.selection_cost != sb.objective.selection_cost {
            return cmp_f64(sa.objective.selection_cost, sb.objective.selection_cost);
        }
        // 4. (free bucket) highest adequacy first: the saturating blend, not raw,
        // so the DEMAND still shapes the pick. At iq 95 sol (1.00) outranks luna
        // (0.96) and edges terra (0.999); at iq 30 all three saturate to 1.00 and
        // speed decides, keeping cheap-band work on the fast small model instead
        // of promoting everything to the flagship. Raw adequacy here would crown
        // the strongest model at every band regardless of what was asked.
        if a_free && b_free && b.adequacy.adequacy != a.adequacy.adequacy {
            return cmp_f64(b.adequacy.adequacy, a.adequacy.adequacy);
        }
        // 5. Inside the free bucket, spend headroom that expires sooner is used
        // first. Quantized integer keys preserve a total order; an absent sample is
        // neutral rather than favourable or fatal.
        if a_free && b_free {
            let a_abundance = a
                .runtime
                .quota_abundance
                .filter(|value| value.is_finite())
                .unwrap_or(1.0);
            let b_abundance = b
                .runtime
                .quota_abundance
                .filter(|value| value.is_finite())
                .unwrap_or(1.0);
            let a_quantum = (a_abundance * 1000.0).round() as i64;
            let b_quantum = (b_abundance * 1000.0).round() as i64;
            if a_quantum != b_quantum {
                return b_quantum.cmp(&a_quantum);
            }
        }
        // 6. fastest among equal-cost (speed tie-break).
        if sb.objective.speed_score != sa.objective.speed_score {
            return cmp_f64(sb.objective.speed_score, sa.objective.speed_score);
        }
        // 7. cheaper-if-metered proxy, then capability, then routeKey.
        let a_cost = normalized_metered_cost(&a.route.fact);
        let b_cost = normalized_metered_cost(&b.route.fact);
        if a_cost != b_cost {
            return cmp_f64(a_cost, b_cost);
        }
        if b.adequacy.adequacy != a.adequacy.adequacy {
            return cmp_f64(b.adequacy.adequacy, a.adequacy.adequacy);
        }
        if b.adequacy.raw_adequacy != a.adequacy.raw_adequacy {
            return cmp_f64(b.adequacy.raw_adequacy, a.adequacy.raw_adequacy);
        }
        a.route
            .route_key
            .cmp(&b.route.route_key)
            .then(Ordering::Equal)
    });

    let winner = &scored[0];
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
            adequacy: winner_admissible.adequacy.adequacy,
            raw_adequacy: winner_admissible.adequacy.raw_adequacy,
            selection_cost: winner.objective.selection_cost,
            metrics: winner_admissible.adequacy.metrics.clone(),
            objective: winner.objective.clone(),
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
        below_floor: (best_effort && winner_admissible.adequacy.adequacy < ADEQUACY_FLOOR)
            .then_some(BelowFloorWarning {
                adequacy: winner_admissible.adequacy.adequacy,
                floor: ADEQUACY_FLOOR,
            }),
    })
}

fn earliest_reset_for_rejections(
    facts: &[ModelFact],
    snapshot: Option<&ModelRouteStateSnapshot>,
    rejected: &[RejectedRoute],
) -> Option<f64> {
    let blocked: HashSet<&str> = rejected
        .iter()
        .filter(|rejection| {
            matches!(
                rejection.reason.as_str(),
                "quota_exhausted" | "cooldown_active" | "backoff_active"
            )
        })
        .map(|rejection| rejection.route_key.as_str())
        .collect();
    let candidates = facts
        .iter()
        .map(fact_to_route)
        .filter(|route| blocked.contains(route.route_key.as_str()))
        .map(|route| crate::break_glass::BreakGlassCandidate {
            route_key: route.route_key,
            capacity_key: route.capacity_key,
        })
        .collect::<Vec<_>>();
    snapshot.and_then(|snapshot| {
        crate::break_glass::earliest_reset_among_candidates(snapshot, &candidates)
    })
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

/// Compare two finite f64 in ascending order, matching the sign of JS `a - b`.
fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
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
mod abundance_tests {
    use super::*;
    use crate::model_facts::{Confidence, ModelScores};

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
        }
    }

    #[derive(Default)]
    struct SelectOptions<'a> {
        requirements: Option<ModelRequirements>,
        exclude_route_keys: Vec<String>,
        is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
        prefer_prompt_cache: bool,
        providers_without_prompt_cache: HashSet<String>,
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
        select_model_route(SelectModelRouteInput {
            target_agent: "mason".into(),
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
            prefer_fast_specs: Vec::new(),
            required_capabilities: None,
            is_subscription_route: options.is_subscription_route,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: options.prefer_prompt_cache,
            providers_without_prompt_cache: options.providers_without_prompt_cache,
        })
        .expect("fixture has an admissible route")
    }

    fn select(
        facts: Vec<ModelFact>,
        states: &[(&str, Option<f64>, Option<&str>)],
    ) -> SelectedModelRoute {
        select_with(facts, states, SelectOptions::default())
    }

    #[test]
    fn objective_without_abundance_is_byte_identical_to_the_previous_shape() {
        let objective = ObjectiveBreakdown {
            cost: 0.25,
            quota: 0.125,
            speed_score: 0.75,
            selection_cost: 0.375,
            quota_abundance: None,
            quota_abundance_window: None,
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
    fn runtime_abundance_orders_the_free_bucket_before_speed_and_none_is_neutral() {
        let abundant_over_neutral = select(
            vec![
                fact("abundant", "slow", 10.0),
                fact("neutral", "fast", 100.0),
            ],
            &[("abundant/slow", Some(4.0), Some("abundant-window"))],
        );
        assert_eq!(
            abundant_over_neutral.route_key, "abundant/slow",
            "runtime abundance must be read before the faster neutral route wins"
        );

        let neutral_over_scarce = select(
            vec![fact("neutral", "slow", 10.0), fact("scarce", "fast", 100.0)],
            &[("scarce/fast", Some(0.25), Some("scarce-window"))],
        );
        assert_eq!(
            neutral_over_scarce.route_key, "neutral/slow",
            "an absent abundance sample is neutral 1.0, not zero"
        );
    }

    #[test]
    fn two_adequate_routes_whose_abundances_differ_by_at_least_one_quantum_rank_by_abundance() {
        let decision = select(
            vec![fact("alpha", "slow", 10.0), fact("beta", "fast", 100.0)],
            &[
                ("alpha/slow", Some(1.0020), Some("alpha-window")),
                ("beta/fast", Some(1.0000), Some("beta-window")),
            ],
        );
        assert_eq!(
            decision.route_key, "alpha/slow",
            "more abundant route must outrank the faster route when abundances differ by at least one quantum"
        );
        assert_eq!(decision.score.objective.quota_abundance, Some(1.0020));
        assert_eq!(
            decision.score.objective.quota_abundance_window.as_deref(),
            Some("alpha-window")
        );
        assert!(
            decision
                .explanation
                .iter()
                .any(|line| line == "quota abundance 1.0020 (window alpha-window)"),
            "explanation must state abundance value and window key: {:?}",
            decision.explanation
        );
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
