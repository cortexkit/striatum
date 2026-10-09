//! Persona seat routing: pin, recommendation, and family-diversity selection.
//!
//! Persona substrate v1 routing contract: an invocation `model_pin`, when
//! present, names one exact canonical `provider/model` route and takes
//! precedence over persona recommendations; it must be available and adequate
//! or seat routing fails with a typed error. Without a pin, routing filters
//! available routes through the existing `ModelRequirements {iq, eq}` adequacy
//! gate, applies ordered persona recommendations in exact-model, provider, and
//! grand-family tiers (the first available adequate recommendation wins its
//! tier), prefers a route whose known grand family is absent from every known
//! incumbent grand family, and resolves the remainder with the existing
//! deterministic cost and speed tie-breaks.
//!
//! Grand families are curated model facts (`ModelFact::grand_family_id`),
//! never inferred from provider or model names. A route with unknown family is
//! never inadequate for that reason alone: it simply cannot satisfy the
//! diversity preference.
//!
//! Seat routing has NO best-effort fallback band: a seat that cannot be given
//! an adequate model must fail loudly (the invitation is refused) instead of
//! seating a weak model. Every failure is a typed `Err` from this pure
//! function, so callers decide against creating any invitation or session
//! effect before one exists.

use std::collections::{BTreeSet, HashSet};

use crate::model_facts::{ModelFact, ModelRequirements, ModelRoute};
use crate::model_router::{
    admit_routes, compute_objective, normalized_metered_cost, AdmittedRoute, CostBasis,
    ModelRouteStateSnapshot, ObjectiveBreakdown, RequiredCapabilities, SelectedModel,
};
use crate::provider_model_spec::ExclusionPolicy;
use crate::routing_config::{RoutingConfig, ADEQUACY_FLOOR, DEFAULT_ROUTING_CONFIG};

/// Ordered persona model recommendations, already normalized to the canonical
/// lowercase ids exported by model facts. Entry order is preference order.
/// This mirrors the persona manifest's `model_recommendations` shape; the
/// manifest type itself lives with the persona loader, which converts into
/// this routing-side struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeatRecommendations {
    /// Exact `provider/model` capacity keys, most preferred first.
    pub models: Vec<String>,
    /// Canonical provider ids, most preferred first.
    pub providers: Vec<String>,
    /// Canonical grand-family ids, most preferred first.
    pub families: Vec<String>,
}

impl SeatRecommendations {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty() && self.providers.is_empty() && self.families.is_empty()
    }
}

/// Which recommendation tier selected the pool, and the winning entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeatRecommendationTier {
    Model,
    Provider,
    Family,
}

impl SeatRecommendationTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            SeatRecommendationTier::Model => "model",
            SeatRecommendationTier::Provider => "provider",
            SeatRecommendationTier::Family => "family",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeatRecommendationMatch {
    pub tier: SeatRecommendationTier,
    /// The recommendation entry that won, in its canonical form.
    pub entry: String,
}

/// Input to [`select_seat_route`]. Pure data; no persistence or host calls.
pub struct SeatRouteInput<'a> {
    /// Persona routing demands (`routing: {iq, eq}`), 0..100 each.
    pub requirements: ModelRequirements,
    /// Invocation-level exact pin: one canonical `provider/model`. Takes
    /// precedence over all recommendations and the diversity preference.
    pub model_pin: Option<String>,
    /// Ordered persona recommendations. Preferences only: an unavailable or
    /// inadequate recommendation falls through, never fails the invitation.
    pub recommendations: SeatRecommendations,
    /// Known grand families of the room's incumbent members. Incumbents whose
    /// family is unknown contribute nothing here.
    pub incumbent_families: BTreeSet<String>,
    pub facts: Vec<ModelFact>,
    pub runtime_snapshot: Option<ModelRouteStateSnapshot>,
    pub exclude_route_keys: Vec<String>,
    pub required_capabilities: Option<RequiredCapabilities>,
    /// Set of available routeKeys AND/OR capacityKeys the caller is logged into.
    pub available_models: HashSet<String>,
    pub is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
}

/// A successful seat route selection with its provenance.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeatRouteSelection {
    pub model: SelectedModel,
    pub route_key: String,
    pub capacity_key: String,
    /// Canonical grand family of the selected route, when the fact carries one.
    /// Prompt family variant selection consumes this same value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grand_family_id: Option<String>,
    /// True when an invocation pin decided the route.
    pub pinned: bool,
    /// The recommendation tier and entry that restricted the pool, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<SeatRecommendationMatch>,
    /// True when the selected route's known grand family is absent from every
    /// known incumbent grand family.
    pub diversity_satisfied: bool,
    pub adequacy: f64,
    pub selection_cost: f64,
    pub explanation: Vec<String>,
}

/// Typed seat routing failure. Codes are the stable wire vocabulary; every
/// variant is a refusal BEFORE any invitation or session effect exists.
#[derive(Debug, Clone, PartialEq)]
pub enum SeatRouteError {
    /// Requirements outside 0..100 (defensive; the manifest validates earlier).
    InvalidRequirements { detail: String },
    /// The pinned `provider/model` is unknown, unavailable, excluded, cooling
    /// down, quota-exhausted, or missing a required capability.
    ModelPinUnavailable { pin: String, detail: String },
    /// The pinned route is available but does not clear the adequacy gate.
    ModelPinInadequate {
        pin: String,
        adequacy: f64,
        floor: f64,
    },
    /// No available route clears the adequacy gate.
    NoAdequateRoute { detail: String },
}

impl SeatRouteError {
    /// Stable machine code (the wire error vocabulary).
    pub fn code(&self) -> &'static str {
        match self {
            SeatRouteError::InvalidRequirements { .. } => "invalid_requirements",
            SeatRouteError::ModelPinUnavailable { .. } => "model_pin_unavailable",
            SeatRouteError::ModelPinInadequate { .. } => "model_pin_inadequate",
            SeatRouteError::NoAdequateRoute { .. } => "no_adequate_route",
        }
    }
}

impl std::fmt::Display for SeatRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeatRouteError::InvalidRequirements { detail } => {
                write!(f, "invalid_requirements: {detail}")
            }
            SeatRouteError::ModelPinUnavailable { pin, detail } => {
                write!(f, "model_pin_unavailable: `{pin}`: {detail}")
            }
            SeatRouteError::ModelPinInadequate {
                pin,
                adequacy,
                floor,
            } => write!(
                f,
                "model_pin_inadequate: `{pin}` adequacy {adequacy:.2} is below the gate {floor:.2}"
            ),
            SeatRouteError::NoAdequateRoute { detail } => {
                write!(f, "no_adequate_route: {detail}")
            }
        }
    }
}

impl std::error::Error for SeatRouteError {}

/// Resolve the known grand families of incumbent members from curated facts.
/// `incumbent_keys` may be route keys or capacity keys. Incumbents without a
/// matching fact, or whose fact carries no valid canonical family, are simply
/// absent from the result: unknown incumbents never block diversity.
pub fn incumbent_grand_families(
    facts: &[ModelFact],
    incumbent_keys: &[String],
) -> BTreeSet<String> {
    let mut families = BTreeSet::new();
    for fact in facts {
        let route = crate::model_facts::fact_to_route(fact);
        let matched = incumbent_keys
            .iter()
            .any(|key| *key == route.route_key || *key == route.capacity_key);
        if matched {
            if let Some(family) = fact.grand_family_id() {
                families.insert(family.to_string());
            }
        }
    }
    families
}

/// Select a seat route with the default demand curve.
pub fn select_seat_route(input: SeatRouteInput<'_>) -> Result<SeatRouteSelection, SeatRouteError> {
    select_seat_route_with_config(input, &DEFAULT_ROUTING_CONFIG)
}

/// Candidate wrapper carrying the precomputed objective for deterministic
/// ordering.
struct SeatCandidate {
    admitted: AdmittedRoute,
    objective: ObjectiveBreakdown,
    metered_cost: f64,
}

pub fn select_seat_route_with_config(
    input: SeatRouteInput<'_>,
    config: &RoutingConfig,
) -> Result<SeatRouteSelection, SeatRouteError> {
    for value in [input.requirements.iq, input.requirements.eq] {
        if value.is_nan() || !(0.0..=100.0).contains(&value) {
            return Err(SeatRouteError::InvalidRequirements {
                detail: "routing requirements must be numbers in 0..100".to_string(),
            });
        }
    }

    // Seat routing takes the operator fence through its available-model set, which
    // the caller resolves against the operator policy before it gets here, so only
    // the per-request exclusions are applied at admission.
    let mut admission = admit_routes(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy::caller_only(&input.exclude_route_keys),
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
    );
    admission.admitted.retain(|route| {
        let permitted = !input
            .is_subscription_route
            .as_ref()
            .is_some_and(|pred| pred(&route.route))
            || crate::model_router::subscription_budget_available(&route.runtime, false);
        if !permitted {
            admission.rejected.push(crate::model_router::RejectedRoute {
                route_key: route.route.route_key.clone(),
                reason: "subscription_reserve".into(),
                detail: Some("weekly headroom is reserved for heads".into()),
            });
        }
        permitted
    });

    // Adequacy gate: seats have no best-effort band. A route is adequate only
    // when it clears the existing floor against the persona's demands.
    let (adequate, inadequate): (Vec<AdmittedRoute>, Vec<AdmittedRoute>) = admission
        .admitted
        .into_iter()
        .partition(|route| route.adequacy.clears_adequacy_floor());

    if let Some(pin) = &input.model_pin {
        return select_pinned(
            pin,
            &input,
            adequate,
            inadequate,
            &admission.rejected,
            config,
        );
    }

    if adequate.is_empty() {
        let detail = if inadequate.is_empty() {
            format!(
                "no available route; {} route(s) rejected at admission",
                admission.rejected.len()
            )
        } else {
            format!(
                "{} available route(s), none clears the adequacy floor",
                inadequate.len()
            )
        };
        return Err(SeatRouteError::NoAdequateRoute { detail });
    }

    let mut explanation = Vec::new();
    let (pool, recommendation) = apply_recommendations(adequate, &input.recommendations);
    if let Some(matched) = &recommendation {
        explanation.push(format!(
            "recommendation tier {} matched `{}`",
            matched.tier.as_str(),
            matched.entry
        ));
    }

    let ordered = order_candidates(pool, &input);

    // Diversity preference: first candidate (in deterministic tie-break order)
    // whose KNOWN grand family is absent from every known incumbent family.
    // Unknown family never satisfies the preference; when nothing satisfies it,
    // fall back to the deterministic head of the pool.
    let preferred_index = ordered.iter().position(|candidate| {
        candidate
            .admitted
            .route
            .fact
            .grand_family_id()
            .is_some_and(|family| !input.incumbent_families.contains(family))
    });
    let diversity_applied = preferred_index.is_some();
    let index = preferred_index.unwrap_or(0);
    if diversity_applied && index > 0 {
        explanation.push("family-diversity preference re-ranked the pool".to_string());
    } else if !diversity_applied && !input.incumbent_families.is_empty() {
        explanation.push(
            "no adequate route offers a grand family different from the incumbents; \
             falling back to any adequate route"
                .to_string(),
        );
    }

    let winner = &ordered[index];
    Ok(build_selection(
        winner,
        false,
        recommendation,
        &input.incumbent_families,
        explanation,
    ))
}

fn select_pinned(
    pin: &str,
    input: &SeatRouteInput<'_>,
    adequate: Vec<AdmittedRoute>,
    inadequate: Vec<AdmittedRoute>,
    rejected: &[crate::model_router::RejectedRoute],
    _config: &RoutingConfig,
) -> Result<SeatRouteSelection, SeatRouteError> {
    let pin_key = pin.trim().to_ascii_lowercase();
    if pin_key.split('/').filter(|part| !part.is_empty()).count() != 2 {
        return Err(SeatRouteError::ModelPinUnavailable {
            pin: pin.to_string(),
            detail: "a model pin must name one exact canonical provider/model".to_string(),
        });
    }

    let pinned_adequate: Vec<AdmittedRoute> = adequate
        .into_iter()
        .filter(|route| route.route.capacity_key == pin_key)
        .collect();
    if pinned_adequate.is_empty() {
        // Distinguish "the pin exists but is not good enough" from "the pin is
        // not usable at all" so the caller's refusal names the true cause.
        if let Some(best) = inadequate
            .into_iter()
            .filter(|route| route.route.capacity_key == pin_key)
            .max_by(|a, b| a.adequacy.adequacy.total_cmp(&b.adequacy.adequacy))
        {
            return Err(SeatRouteError::ModelPinInadequate {
                pin: pin.to_string(),
                adequacy: best.adequacy.adequacy,
                floor: ADEQUACY_FLOOR,
            });
        }
        let detail = rejected
            .iter()
            .find(|route| {
                route.route_key == pin_key || route.route_key.starts_with(&format!("{pin_key}/"))
            })
            .map(|route| format!("rejected at admission: {}", route.reason))
            .unwrap_or_else(|| "no available route matches the pin".to_string());
        return Err(SeatRouteError::ModelPinUnavailable {
            pin: pin.to_string(),
            detail,
        });
    }

    let ordered = order_candidates(pinned_adequate, input);
    let winner = &ordered[0];
    Ok(build_selection(
        winner,
        true,
        None,
        &input.incumbent_families,
        vec![format!("invocation pin `{pin_key}` selected the route")],
    ))
}

/// Apply ordered recommendation tiers over the adequate pool. Tiers are
/// considered exact-model first, then provider, then grand family; within a
/// tier, entries are considered in declared order and the FIRST entry with any
/// available adequate route wins the whole recommendation step. A winning
/// entry restricts the pool to its matching routes; when nothing matches, the
/// pool passes through untouched.
fn apply_recommendations(
    adequate: Vec<AdmittedRoute>,
    recommendations: &SeatRecommendations,
) -> (Vec<AdmittedRoute>, Option<SeatRecommendationMatch>) {
    type TierMatcher = fn(&AdmittedRoute, &str) -> bool;
    let tiers: [(SeatRecommendationTier, &[String], TierMatcher); 3] = [
        (
            SeatRecommendationTier::Model,
            &recommendations.models,
            |route, entry| route.route.capacity_key == entry,
        ),
        (
            SeatRecommendationTier::Provider,
            &recommendations.providers,
            |route, entry| route.route.fact.provider_id == entry,
        ),
        (
            SeatRecommendationTier::Family,
            &recommendations.families,
            |route, entry| route.route.fact.grand_family_id() == Some(entry),
        ),
    ];

    for (tier, entries, matcher) in tiers {
        for entry in entries {
            let matching: Vec<AdmittedRoute> = adequate
                .iter()
                .filter(|route| matcher(route, entry))
                .cloned()
                .collect();
            if !matching.is_empty() {
                return (
                    matching,
                    Some(SeatRecommendationMatch {
                        tier,
                        entry: entry.clone(),
                    }),
                );
            }
        }
    }
    (adequate, None)
}

/// Deterministic cost-primary ordering, identical to the existing router
/// tie-break chain: lowest economic selection cost, then fastest, then
/// cheapest metered proxy, then highest adequacy, then route key.
fn order_candidates(pool: Vec<AdmittedRoute>, input: &SeatRouteInput<'_>) -> Vec<SeatCandidate> {
    let mut candidates: Vec<SeatCandidate> = pool
        .into_iter()
        .map(|admitted| {
            let is_subscription = input
                .is_subscription_route
                .as_ref()
                .map(|predicate| predicate(&admitted.route))
                .unwrap_or(false);
            let (objective, _) = compute_objective(
                &admitted.route,
                &admitted.runtime,
                is_subscription,
                CostBasis::OutputOnly,
                false,
            );
            SeatCandidate {
                metered_cost: normalized_metered_cost(&admitted.route.fact),
                admitted,
                objective,
            }
        })
        .collect();
    candidates.sort_by(|a, b| {
        a.objective
            .selection_cost
            .total_cmp(&b.objective.selection_cost)
            .then_with(|| b.objective.speed_score.total_cmp(&a.objective.speed_score))
            .then_with(|| a.metered_cost.total_cmp(&b.metered_cost))
            .then_with(|| {
                b.admitted
                    .adequacy
                    .adequacy
                    .total_cmp(&a.admitted.adequacy.adequacy)
            })
            .then_with(|| {
                b.admitted
                    .adequacy
                    .raw_adequacy
                    .total_cmp(&a.admitted.adequacy.raw_adequacy)
            })
            .then_with(|| a.admitted.route.route_key.cmp(&b.admitted.route.route_key))
    });
    candidates
}

fn build_selection(
    winner: &SeatCandidate,
    pinned: bool,
    recommendation: Option<SeatRecommendationMatch>,
    incumbent_families: &BTreeSet<String>,
    mut explanation: Vec<String>,
) -> SeatRouteSelection {
    let fact = &winner.admitted.route.fact;
    let grand_family_id = fact.grand_family_id().map(str::to_string);
    let diversity_satisfied = grand_family_id
        .as_deref()
        .is_some_and(|family| !incumbent_families.contains(family));
    explanation.push(format!(
        "selected {}: adequacy {:.2}, selection cost {:.2}",
        winner.admitted.route.route_key,
        winner.admitted.adequacy.adequacy,
        winner.objective.selection_cost,
    ));
    SeatRouteSelection {
        model: SelectedModel {
            provider_id: fact.provider_id.clone(),
            model_id: fact.model_id.clone(),
            variant: fact.variant.clone(),
        },
        route_key: winner.admitted.route.route_key.clone(),
        capacity_key: winner.admitted.route.capacity_key.clone(),
        grand_family_id,
        pinned,
        recommendation,
        diversity_satisfied,
        adequacy: winner.admitted.adequacy.adequacy,
        selection_cost: winner.objective.selection_cost,
        explanation,
    }
}
