//! Panel selection over the shared route admission boundary.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::model_facts::{
    make_capacity_key, ModelFact, ModelRequirements, ModelRoute, FAST_SUFFIX,
};
use crate::model_router::{
    admit_routes, compute_objective, normalized_metered_cost, CostBasis, ModelRouteStateSnapshot,
    RequiredCapabilities, REASON_EXCLUDED, REASON_EXCLUDED_BY_OPERATOR, REASON_NOT_AVAILABLE,
};
use crate::package_ceiling::seat_package_ceiling;
use crate::provider_model_spec::ExclusionPolicy;
use crate::routing_config::{demand_bar, RoutingConfig, DEFAULT_ROUTING_CONFIG};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelSelection {
    #[default]
    Roster,
    NPick,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PanelDiversity {
    None,
    Family,
}

/// Whose availability list a panel's seats were judged against.
///
/// A seat is only real if the thing that will SEND to it can resolve the route.
/// Panel legs do not all send through the same place: an Athena or spec consult
/// sends through Broca, while a host-run worker sends through the caller's own
/// provider list. Judging a Broca-bound seat against the caller's host list
/// rejected models Broca resolves perfectly well (the whole Kimi roster sat
/// `unavailable` for a day after its provider was renamed host-side), so the
/// substrate that sends picks the list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilitySubstrate {
    /// The caller's host provider list, which is what a host-run leg can send to.
    #[default]
    Host,
    /// Broca's send surface, with Broca's reach known for every served route.
    Broca,
    /// Broca's send surface, judged against every route the model catalog still
    /// serves because routing does not know Broca's reach for at least one of
    /// them. Unknown reach is admitted rather than refused: the served catalog
    /// excludes only routes nobody serves, and a seat Broca then fails to send
    /// is replaced when it dies, like any other dead seat.
    BrocaServedCatalog,
}

impl AvailabilitySubstrate {
    /// The text recorded on an availability rejection and on the panel decision,
    /// so the next reader of a decision row does not have to reconstruct which
    /// list was consulted, or that Broca's own reach was never known.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Broca => "broca",
            Self::BrocaServedCatalog => "broca reachability unknown: judged against served catalog",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PanelClass {
    Audit,
    DesignReview,
    Evaluate,
    Diagnose,
    Explain,
    Spec,
    #[serde(alias = "lab")]
    Lab,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelOrder {
    #[default]
    Iq,
    Eq,
    Cost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelExtraOrder {
    Random,
    Iq,
    Eq,
    Cost,
}

/// A complete class policy. Callers replace this value rather than merging fields
/// into a built-in default; an absent floor means there is no quality bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassPanelPolicy {
    pub top_k: usize,
    pub by: PanelOrder,
    pub extra: usize,
    pub extra_by: Option<PanelExtraOrder>,
    pub floor: Option<u8>,
}

impl ClassPanelPolicy {
    pub fn default_for_class(class: PanelClass) -> Self {
        use PanelClass::*;
        use PanelExtraOrder as Extra;
        use PanelOrder as By;
        let (top_k, by, extra, extra_by, floor) = match class {
            Audit => (2, By::Iq, 1, Some(Extra::Random), Some(80)),
            Spec => (3, By::Iq, 1, Some(Extra::Random), Some(80)),
            Diagnose => (3, By::Iq, 0, None, Some(80)),
            DesignReview => (3, By::Eq, 0, None, None),
            Evaluate => (2, By::Iq, 1, Some(Extra::Eq), None),
            Explain => (1, By::Cost, 0, None, Some(50)),
            Lab => (4, By::Iq, 0, None, None),
        };
        Self {
            top_k,
            by,
            extra,
            extra_by,
            floor,
        }
    }

    pub fn width(self) -> usize {
        self.top_k + self.extra
    }

    fn validate(self) -> Result<(), InvalidPanelRequest> {
        if self.top_k == 0
            || self
                .top_k
                .checked_add(self.extra)
                .is_none_or(|width| width > 8)
            || (self.extra > 0 && self.extra_by.is_none())
            || self.floor.is_some_and(|floor| floor > 100)
        {
            return Err(InvalidPanelRequest("invalid class panel policy".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelSpec {
    pub size: usize,
    pub iq_floor: f64,
    pub diversity: PanelDiversity,
    pub class: PanelClass,
    #[serde(default)]
    pub temperature_spread: Option<Vec<f64>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelRosterEntry {
    pub model: String,
    #[serde(default)]
    pub prompt_params: Map<String, Value>,
}

pub struct SelectPanelInput<'a> {
    /// Whole evidence package size, estimated by the caller. Never a request to
    /// truncate evidence or enlarge a model's budget.
    pub package_tokens: Option<usize>,
    pub panel_spec: PanelSpec,
    pub selection: PanelSelection,
    pub roster: Vec<PanelRosterEntry>,
    pub facts: Vec<ModelFact>,
    pub runtime_snapshot: Option<ModelRouteStateSnapshot>,
    pub exclude_route_keys: Vec<String>,
    /// The operator's standing exclude list. Applied to automatic (n_pick)
    /// admission only; an explicit roster is a pin and pins bypass exclusion.
    pub operator_exclude_keys: Vec<String>,
    pub required_capabilities: Option<RequiredCapabilities>,
    /// The routes the sending substrate can reach. Which substrate that is comes
    /// from `availability_substrate`, which also labels every rejection here.
    pub available_models: HashSet<String>,
    /// The substrate whose availability `available_models` describes.
    pub availability_substrate: AvailabilitySubstrate,
    pub is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelMember {
    pub route_key: String,
    pub prompt_params: Map<String, Value>,
    pub family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    /// The route's context window from the catalog fact. The consult sizes its
    /// evidence package from the smallest window among the seated members, so
    /// the window travels with the seat instead of being looked up again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelRunnerUp {
    pub route_key: String,
    pub prompt_params: Map<String, Value>,
    pub family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub phase: PanelPhase,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelPhase {
    #[default]
    #[serde(rename = "topK")]
    TopK,
    #[serde(rename = "extra")]
    Extra,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelSeat {
    pub slot: usize,
    pub route_key: String,
    pub reason: PanelPhase,
    pub rank: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PanelSynthesizerPin {
    pub model: String,
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PanelPins {
    pub synthesizer: Option<PanelSynthesizerPin>,
}

/// Request metadata for reproducible extra draws and the stored synthesizer pin.
/// The request handler must supply the task ID when no explicit seed was sent,
/// so retries of the same task draw the same routes from unchanged candidates.
#[derive(Debug, Clone, Default)]
pub struct PanelSelectionContext {
    pub seed: String,
    pub pins: PanelPins,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelCauseReason {
    PackageTooLarge,
    PackageExceedsCeiling,
    /// Rejected by a caller-supplied exclusion for this panel request.
    Excluded,
    /// Rejected by the operator's standing exclude list. Separate from `Excluded`
    /// because the fix is different: one is loosening a config file, the other is
    /// changing what this call asked for.
    ExcludedByOperator,
    Unavailable,
    QuotaExhausted,
    BackoffActive,
    Capability,
    BelowIqFloor,
    MissingFact,
    FamilyUnlabeled,
    FamilyDuplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PanelCause {
    pub model: String,
    pub reason: PanelCauseReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Known quota/cooldown reset in epoch milliseconds, independent of display text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelShortfallCode {
    PackageUnmeetable,
    IqFloorUnmeetable,
    DiversityUnmeetable,
    SizeReduced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PanelShortfall {
    pub code: PanelShortfallCode,
    pub detail: String,
    pub causes: Vec<PanelCause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SeatedSiblingVariant {
    Base,
    Pro,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeatedVariant {
    pub slot: usize,
    pub route_key: String,
    pub variant: SeatedSiblingVariant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiblingCollapse {
    pub model: String,
    pub retained_route_key: String,
    pub collapsed_route_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelPolicy {
    pub selection: String,
    pub class: String,
    pub top_k: usize,
    pub by: PanelOrder,
    pub extra: usize,
    pub extra_by: Option<PanelExtraOrder>,
    pub floor: Option<u8>,
    pub width: usize,
    pub seed: Option<String>,
    pub seats: Vec<PanelSeat>,
    pub pins: PanelPins,
    pub seated_variants: Vec<SeatedVariant>,
    pub sibling_collapses: Vec<SiblingCollapse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_tokens: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub largest_package_ceiling: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub replacements: Vec<PackageSeatReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageSeatReplacement {
    pub slot: usize,
    pub replaced_route_key: String,
    pub replacement_route_key: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectedPanel {
    pub members: Vec<PanelMember>,
    pub runner_up: Vec<PanelRunnerUp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shortfall: Option<PanelShortfall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<PanelPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPanelRequest(pub String);

struct Candidate {
    price_unknown: bool,
    route: ModelRoute,
    prompt_params: Map<String, Value>,
    selection_cost: f64,
    speed_score: f64,
    metered_cost: f64,
}

pub fn validate_panel_spec(spec: &PanelSpec) -> Result<(), InvalidPanelRequest> {
    if !(1..=8).contains(&spec.size) {
        return Err(InvalidPanelRequest(
            "panelSpec.size must be in 1..=8".into(),
        ));
    }
    if !spec.iq_floor.is_finite() || !(0.0..=100.0).contains(&spec.iq_floor) {
        return Err(InvalidPanelRequest(
            "panelSpec.iqFloor must be a finite number in 0..=100".into(),
        ));
    }
    if spec.temperature_spread.as_ref().is_some_and(|spread| {
        spread
            .iter()
            .any(|value| !value.is_finite() || !(0.0..=2.0).contains(value))
    }) {
        return Err(InvalidPanelRequest(
            "panelSpec.temperatureSpread values must be finite numbers in 0..=2".into(),
        ));
    }
    Ok(())
}

pub fn select_panel(input: SelectPanelInput<'_>) -> Result<SelectedPanel, InvalidPanelRequest> {
    select_panel_with_config(input, &DEFAULT_ROUTING_CONFIG)
}

pub fn select_panel_with_config(
    input: SelectPanelInput<'_>,
    config: &RoutingConfig,
) -> Result<SelectedPanel, InvalidPanelRequest> {
    let policy = ClassPanelPolicy::default_for_class(input.panel_spec.class);
    select_panel_with_policy(input, config, &policy)
}

pub fn select_panel_with_policy(
    input: SelectPanelInput<'_>,
    config: &RoutingConfig,
    class_policy: &ClassPanelPolicy,
) -> Result<SelectedPanel, InvalidPanelRequest> {
    select_panel_with_policy_context(
        input,
        config,
        class_policy,
        &PanelSelectionContext::default(),
    )
}

pub fn select_panel_with_policy_context(
    input: SelectPanelInput<'_>,
    config: &RoutingConfig,
    class_policy: &ClassPanelPolicy,
    context: &PanelSelectionContext,
) -> Result<SelectedPanel, InvalidPanelRequest> {
    class_policy.validate()?;
    if input.selection != PanelSelection::Roster || input.package_tokens.is_none() {
        return select_panel_admitted(&input, config, class_policy, context);
    }
    let tokens = input.package_tokens.unwrap();
    let mut panel = select_panel_admitted(&input, config, class_policy, context)?;
    let oversized = panel
        .members
        .iter()
        .enumerate()
        .filter_map(|(slot, member)| {
            let ceiling = seat_package_ceiling(
                &member.route_key,
                member.context_window,
                member.max_output_tokens,
            );
            (tokens > ceiling.tokens).then_some((slot, ceiling))
        })
        .collect::<Vec<_>>();
    if oversized.is_empty() {
        return Ok(panel);
    }
    let package_causes = oversized
        .iter()
        .map(|(slot, ceiling)| PanelCause {
            model: panel.members[*slot].route_key.clone(),
            reason: PanelCauseReason::PackageExceedsCeiling,
            reset_at_ms: None,
            detail: Some(format!(
                "package {tokens} tokens exceeds {} ceiling {} tokens",
                ceiling.route_key, ceiling.tokens
            )),
        })
        .collect::<Vec<_>>();
    // Keep the fitting pins, but replacements are automatic choices: the same
    // admission, adequacy, sibling ordering and lineage rules as n_pick apply.
    let automatic = SelectPanelInput {
        selection: PanelSelection::NPick,
        roster: Vec::new(),
        is_subscription_route: input.is_subscription_route,
        ..input
    };
    let spares = select_panel_admitted(&automatic, config, class_policy, context)?;
    let mut policy = spares.policy.expect("automatic selection records policy");
    let mut candidates = spares
        .members
        .into_iter()
        .chain(spares.runner_up.into_iter().map(|spare| PanelMember {
            route_key: spare.route_key,
            prompt_params: spare.prompt_params,
            family: spare.family,
            temperature: None,
            max_output_tokens: spare.max_output_tokens,
            context_window: spare.context_window,
        }))
        .collect::<Vec<_>>();
    let mut used = panel
        .members
        .iter()
        .map(|member| member.route_key.clone())
        .collect::<HashSet<_>>();
    let mut families = panel
        .members
        .iter()
        .enumerate()
        .filter(|(slot, _)| !oversized.iter().any(|(small, _)| small == slot))
        .filter_map(|(_, member)| member.family.clone())
        .collect::<HashSet<_>>();
    let mut unfilled = false;
    for (slot, ceiling) in oversized {
        let spare = candidates.iter().position(|candidate| {
            !used.contains(&candidate.route_key)
                && (automatic.panel_spec.diversity != PanelDiversity::Family
                    || candidate
                        .family
                        .as_ref()
                        .is_some_and(|family| !families.contains(family)))
        });
        let Some(index) = spare else {
            unfilled = true;
            continue;
        };
        let mut replacement = candidates.remove(index);
        replacement.temperature = panel.members[slot].temperature;
        policy.replacements.push(PackageSeatReplacement {
            slot,
            replaced_route_key: panel.members[slot].route_key.clone(),
            replacement_route_key: replacement.route_key.clone(),
            reason: format!(
                "package {tokens} tokens exceeds {} ceiling {} tokens",
                ceiling.route_key, ceiling.tokens
            ),
        });
        used.insert(replacement.route_key.clone());
        families.extend(replacement.family.clone());
        panel.members[slot] = replacement;
    }
    panel.runner_up = candidates
        .into_iter()
        .filter(|candidate| !used.contains(&candidate.route_key))
        .map(|candidate| PanelRunnerUp {
            route_key: candidate.route_key,
            prompt_params: candidate.prompt_params,
            family: candidate.family,
            max_output_tokens: candidate.max_output_tokens,
            context_window: candidate.context_window,
            phase: PanelPhase::TopK,
        })
        .collect();
    if unfilled {
        let mut causes = panel
            .shortfall
            .take()
            .map(|shortfall| shortfall.causes)
            .unwrap_or_default();
        let too_small = package_causes
            .iter()
            .filter_map(|cause| cause.detail.clone())
            .collect::<Vec<_>>();
        causes.extend(package_causes);
        let largest = policy.largest_package_ceiling.unwrap_or(0).max(
            panel
                .members
                .iter()
                .map(|member| {
                    seat_package_ceiling(
                        &member.route_key,
                        member.context_window,
                        member.max_output_tokens,
                    )
                    .tokens
                })
                .max()
                .unwrap_or(0),
        );
        panel.shortfall = Some(PanelShortfall {
            code: PanelShortfallCode::PackageUnmeetable,
            detail: package_refusal(tokens, largest, &too_small),
            causes,
        });
        // No oversized pin is a sendable seat, even on a refused decision.
        panel.members.retain(|member| {
            seat_package_ceiling(
                &member.route_key,
                member.context_window,
                member.max_output_tokens,
            )
            .tokens
                >= tokens
        });
    }
    policy.seated_variants = panel
        .members
        .iter()
        .enumerate()
        .map(|(slot, member)| SeatedVariant {
            slot,
            route_key: member.route_key.clone(),
            variant: sibling_variant(&member.route_key),
        })
        .collect();
    panel.policy = Some(policy);
    Ok(panel)
}

/// Names each pinned seat whose ceiling the package exceeds. A seat with an
/// unknown context window has ceiling 0, and without its name the refusal reads
/// as if the package were too big for every model.
fn package_refusal(tokens: usize, largest: usize, too_small: &[String]) -> String {
    let seats = if too_small.is_empty() {
        String::new()
    } else {
        format!(
            " ({}; no unused route of another family fits in its place)",
            too_small.join("; ")
        )
    };
    format!("package {tokens} tokens cannot fill the panel{seats}; largest available adequate ceiling {largest} tokens. Narrow evidence or change the project athena.members roster setting; evidence was not truncated")
}

fn select_panel_admitted(
    input: &SelectPanelInput<'_>,
    config: &RoutingConfig,
    class_policy: &ClassPanelPolicy,
    context: &PanelSelectionContext,
) -> Result<SelectedPanel, InvalidPanelRequest> {
    // Class policy owns automatic panel width and quality, so panel_spec.size
    // and panel_spec.iq_floor cannot invalidate it. The family rule below also
    // comes from class policy, not panel_spec.diversity. Legacy rosters still
    // validate and use their requested settings.
    let mut validated_spec = input.panel_spec.clone();
    let automatic = input.selection == PanelSelection::NPick;
    if automatic {
        validated_spec.size = class_policy.width();
        validated_spec.iq_floor = 0.0;
    }
    validate_panel_spec(&validated_spec)?;
    let width = validated_spec.size;

    let mut prompt_params: HashMap<String, VecDeque<Map<String, Value>>> = HashMap::new();
    let mut causes = Vec::new();
    let considered_facts = match input.selection {
        PanelSelection::Roster => roster_facts(input, &mut prompt_params, &mut causes),
        PanelSelection::NPick => input.facts.clone(),
    };

    // Explicit roster pins BYPASS BOTH exclusion lists by design: the operator's
    // model_routing.exclude reserves scarce models (kimi, anthropic/fable) from
    // automatic routing precisely SO they remain usable via deliberate roster
    // pins. Applying the list here silently shrank pinned audit panels to the
    // unexcluded seats. Availability, quota/cooldown, and capability gates
    // still apply to pins: only the exclusion predicate is roster-exempt.
    let effective_exclusions = match input.selection {
        PanelSelection::Roster => ExclusionPolicy::none(),
        PanelSelection::NPick => ExclusionPolicy {
            operator: &input.operator_exclude_keys,
            caller: &input.exclude_route_keys,
        },
    };
    // Panel selection has no adequacy fallback. The zero requirements are only an
    // unused scoring input to the shared hard-admission implementation.
    let admission = admit_routes(
        &considered_facts,
        input.runtime_snapshot.as_ref(),
        effective_exclusions,
        input.required_capabilities,
        &input.available_models,
        &ModelRequirements { iq: 0.0, eq: 0.0 },
        config,
    );
    let top_routable_elo = admission.top_routable.elo;
    let availability_substrate = input.availability_substrate;
    causes.extend(admission.rejected.into_iter().map(|rejected| {
        // Use the same merged route/capacity deadlines as admission. Display text
        // may change independently, and must not become a retry protocol.
        let reset_at_ms = considered_facts
            .iter()
            .map(crate::model_facts::fact_to_route)
            .find(|route| route.route_key == rejected.route_key)
            .map(|route| {
                crate::model_router::merged_runtime_state(&route, input.runtime_snapshot.as_ref())
            })
            .and_then(|runtime| match rejected.reason.as_str() {
                "quota_exhausted" => runtime.quota_exhausted_until,
                "cooldown_active" => runtime.cooldown_until,
                _ => None,
            })
            .filter(|until| until.is_finite() && *until > 0.0)
            .map(|until| until.ceil() as i64);
        PanelCause {
            reason: match rejected.reason.as_str() {
                REASON_EXCLUDED => PanelCauseReason::Excluded,
                REASON_EXCLUDED_BY_OPERATOR => PanelCauseReason::ExcludedByOperator,
                "quota_exhausted" | "cooldown_active" => PanelCauseReason::QuotaExhausted,
                "backoff_active" => PanelCauseReason::BackoffActive,
                "missing_vision" | "context_too_small" => PanelCauseReason::Capability,
                _ => PanelCauseReason::Unavailable,
            },
            // An availability rejection names the substrate whose list was consulted.
            // Without it, a reader of a recorded rejection cannot tell a route the
            // sender genuinely cannot reach from one that is merely missing from the
            // caller's host list, and the two have different fixes.
            detail: if rejected.reason == REASON_NOT_AVAILABLE {
                Some(availability_substrate.as_str().to_string())
            } else {
                rejected.detail
            },
            model: rejected.route_key,
            reset_at_ms,
        }
    }));

    // Reserved subscription headroom is an admission wall, not an IQ failure.
    // Apply it before counting survivors so an entirely reserved panel reports
    // each route's quota cause even when a pinned roster has no IQ bar.
    let admitted = admission
        .admitted
        .into_iter()
        .filter_map(|admitted| {
            let is_subscription = crate::model_router::subscription_route_covered(
                &admitted.route,
                input.is_subscription_route.as_deref(),
            );
            if is_subscription
                && !crate::model_router::subscription_budget_available(&admitted.runtime, false)
            {
                causes.push(PanelCause {
                    model: admitted.route.capacity_key.clone(),
                    reason: PanelCauseReason::QuotaExhausted,
                    detail: Some(
                        "no live account has weekly quota above its keep-free margin".into(),
                    ),
                    reset_at_ms: reserved_headroom_reset_at_ms(&admitted.runtime),
                });
                None
            } else {
                Some((admitted, is_subscription))
            }
        })
        .collect::<Vec<_>>();
    let admitted_count = admitted.len();

    let iq_bar = class_policy
        .floor
        .filter(|_| automatic)
        .map(|floor| demand_bar(top_routable_elo, f64::from(floor), config.demand_exponent));
    let mut candidates = Vec::new();
    let mut largest_package_ceiling = None;
    for (admitted, is_subscription) in admitted {
        let model = admitted.route.capacity_key.clone();
        if input.selection == PanelSelection::NPick && input.package_tokens.is_some() {
            let ceiling = seat_package_ceiling(
                &admitted.route.route_key,
                context_window_tokens(&admitted.route.fact),
                admitted.route.fact.max_output_tokens,
            );
            largest_package_ceiling = Some(
                largest_package_ceiling
                    .unwrap_or(0usize)
                    .max(ceiling.tokens),
            );
            if input.package_tokens.unwrap() > ceiling.tokens {
                causes.push(PanelCause {
                    model,
                    reason: PanelCauseReason::PackageTooLarge,
                    reset_at_ms: None,
                    detail: Some(format!(
                        "package {} tokens exceeds ceiling {} tokens",
                        input.package_tokens.unwrap(),
                        ceiling.tokens
                    )),
                });
                continue;
            }
        }
        let params = match input.selection {
            PanelSelection::Roster => prompt_params
                .get_mut(&model)
                .and_then(VecDeque::pop_front)
                .unwrap_or_default(),
            PanelSelection::NPick => fact_prompt_params(&admitted.route.fact),
        };
        let (objective, _) = compute_objective(
            &admitted.route,
            &admitted.runtime,
            is_subscription,
            CostBasis::OutputOnly,
            false,
        );
        candidates.push(Candidate {
            price_unknown: objective.price_unknown,
            metered_cost: normalized_metered_cost(&admitted.route.fact),
            route: admitted.route,
            prompt_params: params,
            selection_cost: objective.selection_cost,
            speed_score: objective.speed_score,
        });
    }
    let (mut candidates, sibling_collapses) = if input.selection == PanelSelection::NPick {
        collapse_siblings(
            candidates,
            &considered_facts,
            input.panel_spec.class,
            f64::from(class_policy.floor.unwrap_or(0)),
        )
    } else {
        (candidates, Vec::new())
    };

    let enforce_family_diversity = automatic;
    if enforce_family_diversity {
        candidates.retain(|candidate| match candidate.route.fact.family.as_ref() {
            Some(_) => true,
            None => {
                causes.push(PanelCause {
                    model: candidate.route.capacity_key.clone(),
                    reason: PanelCauseReason::FamilyUnlabeled,
                    reset_at_ms: None,
                    detail: None,
                });
                false
            }
        });
    }

    if automatic {
        order_candidates(&mut candidates, class_policy.by);
    } else {
        candidates.sort_by(iq_cost_order);
    }
    let clears_bar =
        |candidate: &Candidate| iq_bar.is_none_or(|bar| candidate.route.fact.scores.elo >= bar);
    let iq_eligible_count = candidates
        .iter()
        .filter(|candidate| clears_bar(candidate))
        .count();
    let mut selected = Vec::new();
    let mut seats = Vec::new();
    let mut selected_families = HashSet::new();
    let top_k_candidates = candidates
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            if automatic && class_policy.by != PanelOrder::Iq && !clears_bar(candidate) {
                causes.push(below_bar_cause(candidate));
                None
            } else {
                Some(index)
            }
        })
        .collect::<Vec<_>>();
    for (rank, &index) in top_k_candidates.iter().enumerate() {
        if selected.len() == if automatic { class_policy.top_k } else { width } {
            break;
        }
        let candidate = &candidates[index];
        if !enforce_family_diversity
            || selected_families.insert(candidate.route.fact.family.clone().unwrap())
        {
            seats.push(PanelSeat {
                slot: selected.len(),
                route_key: candidate.route.route_key.clone(),
                reason: PanelPhase::TopK,
                rank: Some(rank + 1),
            });
            selected.push(index);
        }
    }

    let mut extra_candidates = Vec::new();
    let mut random_drawn = false;
    if automatic && class_policy.extra > 0 {
        let mut extras = (0..candidates.len())
            .filter(|index| {
                !selected_families.contains(candidates[*index].route.fact.family.as_ref().unwrap())
            })
            .collect::<Vec<_>>();
        extras.retain(|index| {
            if clears_bar(&candidates[*index]) {
                true
            } else {
                causes.push(below_bar_cause(&candidates[*index]));
                false
            }
        });
        let extra_order = class_policy.extra_by.unwrap();
        order_extras(&mut extras, &candidates, extra_order);
        let random = extra_order == PanelExtraOrder::Random;
        if random {
            extras = draw_extras(extras, &candidates, &context.seed);
        }
        let mut seated_extras = 0;
        for (rank, &index) in extras.iter().enumerate() {
            if seated_extras == class_policy.extra {
                break;
            }
            if selected_families.insert(candidates[index].route.fact.family.clone().unwrap()) {
                seats.push(PanelSeat {
                    slot: selected.len(),
                    route_key: candidates[index].route.route_key.clone(),
                    reason: PanelPhase::Extra,
                    rank: (!random).then_some(rank + 1),
                });
                selected.push(index);
                seated_extras += 1;
                random_drawn |= random;
            }
        }
        extra_candidates = extras;
    }

    if enforce_family_diversity && selected.len() < width {
        for (index, candidate) in candidates.iter().enumerate() {
            if !selected.contains(&index)
                && selected_families.contains(candidate.route.fact.family.as_ref().unwrap())
            {
                causes.push(PanelCause {
                    model: candidate.route.capacity_key.clone(),
                    reason: PanelCauseReason::FamilyDuplicate,
                    reset_at_ms: None,
                    detail: None,
                });
            }
        }
    }

    // Preserve phase order even when an extra ranks ahead of a topK seat under
    // a different ordering. Temperatures are assigned after slot order is fixed.
    let mut members = selected
        .iter()
        .map(|&index| {
            let candidate = &candidates[index];
            PanelMember {
                route_key: candidate.route.route_key.clone(),
                prompt_params: candidate.prompt_params.clone(),
                family: candidate.route.fact.family.clone(),
                temperature: None,
                max_output_tokens: candidate.route.fact.max_output_tokens,
                context_window: context_window_tokens(&candidate.route.fact),
            }
        })
        .collect::<Vec<_>>();
    let runner_up = top_k_candidates
        .into_iter()
        .map(|index| (index, PanelPhase::TopK))
        .chain(
            extra_candidates
                .into_iter()
                .map(|index| (index, PanelPhase::Extra)),
        )
        .filter(|(index, _)| !selected.contains(index))
        .map(|(index, phase)| {
            let candidate = &candidates[index];
            PanelRunnerUp {
                route_key: candidate.route.route_key.clone(),
                prompt_params: candidate.prompt_params.clone(),
                family: candidate.route.fact.family.clone(),
                max_output_tokens: candidate.route.fact.max_output_tokens,
                context_window: context_window_tokens(&candidate.route.fact),
                phase,
            }
        })
        .collect();
    if let Some(spread) = &input.panel_spec.temperature_spread {
        for (member, temperature) in members.iter_mut().zip(spread.iter().copied()) {
            member.temperature = Some(temperature);
        }
    }

    let size_reduced = || {
        (
            PanelShortfallCode::SizeReduced,
            format!(
                "only {} of {} requested panel slots can be served",
                members.len(),
                width
            ),
        )
    };
    let shortfall_kind = if input.package_tokens.is_some()
        && members.len() < width
        && causes
            .iter()
            .any(|cause| cause.reason == PanelCauseReason::PackageTooLarge)
    {
        Some((
            PanelShortfallCode::PackageUnmeetable,
            package_refusal(
                input.package_tokens.unwrap(),
                largest_package_ceiling.unwrap_or(0),
                &[],
            ),
        ))
    } else if admitted_count == 0 {
        Some(size_reduced())
    } else if let Some(bar) = iq_bar.filter(|_| iq_eligible_count == 0 && members.len() < width) {
        Some((
            PanelShortfallCode::IqFloorUnmeetable,
            format!("no admitted model clears demand bar {bar:.2}"),
        ))
    } else if enforce_family_diversity && members.len() < width {
        Some((
            PanelShortfallCode::DiversityUnmeetable,
            format!(
                "only {} distinct labeled families can fill {} requested slots",
                members.len(),
                width
            ),
        ))
    } else if members.len() < width {
        Some(size_reduced())
    } else {
        None
    };

    let shortfall = shortfall_kind.map(|(code, detail)| {
        let missing_causes = if automatic {
            width
                .saturating_sub(members.len())
                .saturating_sub(causes.len())
        } else {
            usize::from(causes.is_empty())
        };
        for _ in 0..missing_causes {
            causes.push(PanelCause {
                model: "<unfilled-slot>".into(),
                reset_at_ms: None,
                reason: PanelCauseReason::Unavailable,
                detail: None,
            });
        }
        PanelShortfall {
            code,
            detail,
            causes,
        }
    });

    let policy = (input.selection == PanelSelection::NPick).then(|| PanelPolicy {
        package_tokens: input.package_tokens,
        largest_package_ceiling,
        replacements: Vec::new(),
        selection: "class_policy".into(),
        class: match input.panel_spec.class {
            PanelClass::Audit => "audit",
            PanelClass::DesignReview => "design_review",
            PanelClass::Evaluate => "evaluate",
            PanelClass::Diagnose => "diagnose",
            PanelClass::Explain => "explain",
            PanelClass::Spec => "spec",
            PanelClass::Lab => "lab",
        }
        .into(),
        top_k: class_policy.top_k,
        by: class_policy.by,
        extra: class_policy.extra,
        extra_by: class_policy.extra_by.filter(|_| class_policy.extra > 0),
        floor: class_policy.floor,
        width,
        seed: random_drawn.then(|| context.seed.clone()),
        seats,
        pins: context.pins.clone(),
        seated_variants: members
            .iter()
            .enumerate()
            .map(|(slot, member)| SeatedVariant {
                slot,
                route_key: member.route_key.clone(),
                variant: sibling_variant(&member.route_key),
            })
            .collect(),
        sibling_collapses,
    });

    Ok(SelectedPanel {
        members,
        runner_up,
        shortfall,
        policy,
    })
}

fn reserved_headroom_reset_at_ms(runtime: &crate::model_router::RouteRuntimeState) -> Option<i64> {
    // Reserved quota is unavailable to panel seats even though it is not a hard
    // wall. Preserve the known weekly reset separately from the display wording.
    runtime.subscription_pool.as_ref().and_then(|pool| {
        pool.plans
            .iter()
            .flat_map(|plan| &plan.accounts)
            .map(|account| account.hours_to_reset)
            .filter(|hours| hours.is_finite() && *hours > 0.0)
            .reduce(f64::min)
            .map(|hours| {
                pool.now_ms
                    .saturating_add((hours * 3_600_000.0).ceil() as i64)
            })
    })
}

#[cfg(test)]
mod reset_tests {
    use super::*;

    #[test]
    fn reserved_headroom_shortfall_retains_earliest_known_weekly_reset() {
        use crate::subscription::{QuotaAccount, SubscriptionPlan, SubscriptionPool};
        let account = |hours_to_reset| QuotaAccount {
            weekly_used: 99.0,
            weekly_remaining_fraction: 0.01,
            hours_to_reset,
            short_walled: false,
            short_windows: vec![],
            banked_resets: 0,
            banked_expires_at_ms: None,
        };
        let runtime = crate::model_router::RouteRuntimeState {
            subscription_pool: Some(std::sync::Arc::new(SubscriptionPool {
                now_ms: 1791682778812,
                plans: vec![SubscriptionPlan {
                    tier: None,
                    fee_usd: 0.0,
                    fusiform_multiplier: None,
                    accounts: vec![account(2.0), account(1.0), account(f64::NAN)],
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let cause = PanelCause {
            model: "anthropic/claude-opus-5-5".into(),
            reason: PanelCauseReason::QuotaExhausted,
            detail: Some("no live account has weekly quota above its keep-free margin".into()),
            reset_at_ms: reserved_headroom_reset_at_ms(&runtime),
        };
        assert_eq!(cause.reset_at_ms, Some(1791686378812));
        let wire = serde_json::to_value(&cause).unwrap();
        assert_eq!(wire["reset_at_ms"], 1791686378812i64);
        assert_eq!(
            wire["detail"],
            "no live account has weekly quota above its keep-free margin"
        );
        let unknown = PanelCause {
            reset_at_ms: reserved_headroom_reset_at_ms(&Default::default()),
            ..cause
        };
        assert!(serde_json::to_value(&unknown)
            .unwrap()
            .get("reset_at_ms")
            .is_none());
    }
}

/// A catalog context window as whole tokens; absent, non-finite or non-positive
/// windows read as unknown so the caller applies its own conservative default.
fn context_window_tokens(fact: &ModelFact) -> Option<u64> {
    fact.context_window
        .filter(|window| window.is_finite() && *window >= 1.0)
        .map(|window| window as u64)
}

fn roster_facts(
    input: &SelectPanelInput<'_>,
    prompt_params: &mut HashMap<String, VecDeque<Map<String, Value>>>,
    causes: &mut Vec<PanelCause>,
) -> Vec<ModelFact> {
    let facts_by_model: HashMap<String, &ModelFact> = input
        .facts
        .iter()
        .map(|fact| (make_capacity_key(&fact.provider_id, &fact.model_id), fact))
        .collect();
    let mut matched_facts = Vec::new();
    for entry in &input.roster {
        match facts_by_model.get(&entry.model) {
            Some(fact) => {
                matched_facts.push((*fact).clone());
                prompt_params
                    .entry(entry.model.clone())
                    .or_default()
                    .push_back(entry.prompt_params.clone());
            }
            None => causes.push(PanelCause {
                model: entry.model.clone(),
                reason: PanelCauseReason::MissingFact,
                reset_at_ms: None,
                detail: None,
            }),
        }
    }
    matched_facts
}

fn fact_prompt_params(fact: &ModelFact) -> Map<String, Value> {
    fact.prompt_defaults
        .as_ref()
        .and_then(|defaults| serde_json::to_value(defaults).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

fn below_bar_cause(candidate: &Candidate) -> PanelCause {
    PanelCause {
        model: candidate.route.capacity_key.clone(),
        reason: PanelCauseReason::BelowIqFloor,
        reset_at_ms: None,
        detail: None,
    }
}

fn order_candidates(candidates: &mut [Candidate], order: PanelOrder) {
    candidates.sort_by(|a, b| candidate_order(a, b, order));
}

fn candidate_order(a: &Candidate, b: &Candidate, order: PanelOrder) -> Ordering {
    match order {
        PanelOrder::Iq => iq_lineage_order(a, b),
        PanelOrder::Eq => b
            .route
            .fact
            .scores
            .eq
            .total_cmp(&a.route.fact.scores.eq)
            .then_with(|| iq_lineage_order(a, b)),
        PanelOrder::Cost => cost_primary_order(a, b),
    }
}

fn order_extras(indices: &mut [usize], candidates: &[Candidate], order: PanelExtraOrder) {
    indices.sort_by(|a, b| {
        let (a, b) = (&candidates[*a], &candidates[*b]);
        match order {
            PanelExtraOrder::Random => a.route.route_key.cmp(&b.route.route_key),
            PanelExtraOrder::Iq => candidate_order(a, b, PanelOrder::Iq),
            PanelExtraOrder::Eq => candidate_order(a, b, PanelOrder::Eq),
            PanelExtraOrder::Cost => candidate_order(a, b, PanelOrder::Cost),
        }
    });
}

/// Draw both the initial extra seats and all their replacement spares now, so a
/// resumed consult needs no random cursor. The pool arrives sorted by route key;
/// removing every route in the winning family leaves the remaining order intact.
fn draw_extras(mut pool: Vec<usize>, candidates: &[Candidate], seed: &str) -> Vec<usize> {
    let mut draws = Vec::new();
    while !pool.is_empty() {
        let digest = Sha256::digest(format!("{seed}\n{}", draws.len()).as_bytes());
        let value = u64::from_be_bytes(digest[..8].try_into().unwrap());
        let winner = pool[(value % pool.len() as u64) as usize];
        draws.push(winner);
        let family = &candidates[winner].route.fact.family;
        pool.retain(|index| &candidates[*index].route.fact.family != family);
    }
    draws
}

fn iq_cost_order(a: &Candidate, b: &Candidate) -> Ordering {
    b.route
        .fact
        .scores
        .elo
        .total_cmp(&a.route.fact.scores.elo)
        .then_with(|| a.price_unknown.cmp(&b.price_unknown))
        .then_with(|| a.selection_cost.total_cmp(&b.selection_cost))
        .then_with(|| b.speed_score.total_cmp(&a.speed_score))
        .then_with(|| a.metered_cost.total_cmp(&b.metered_cost))
        .then_with(|| a.route.route_key.cmp(&b.route.route_key))
}

fn iq_lineage_order(a: &Candidate, b: &Candidate) -> Ordering {
    b.route
        .fact
        .scores
        .elo
        .total_cmp(&a.route.fact.scores.elo)
        .then_with(|| a.price_unknown.cmp(&b.price_unknown))
        .then_with(|| a.selection_cost.total_cmp(&b.selection_cost))
        .then_with(|| b.speed_score.total_cmp(&a.speed_score))
        .then_with(|| a.metered_cost.total_cmp(&b.metered_cost))
        .then_with(|| a.route.fact.family.cmp(&b.route.fact.family))
        .then_with(|| a.route.route_key.cmp(&b.route.route_key))
}

fn cost_primary_order(a: &Candidate, b: &Candidate) -> Ordering {
    a.price_unknown
        .cmp(&b.price_unknown)
        .then_with(|| a.selection_cost.total_cmp(&b.selection_cost))
        .then_with(|| b.speed_score.total_cmp(&a.speed_score))
        .then_with(|| a.metered_cost.total_cmp(&b.metered_cost))
        .then_with(|| a.route.route_key.cmp(&b.route.route_key))
}

fn sibling_group_key(route: &ModelRoute, capacity_keys: &HashSet<String>) -> String {
    for suffix in [FAST_SUFFIX, "-pro"] {
        if let Some(base_model) = route.model_id.strip_suffix(suffix) {
            let base_key = make_capacity_key(&route.provider_id, base_model);
            if capacity_keys.contains(&base_key) {
                return base_key;
            }
        }
    }
    route.capacity_key.clone()
}

fn collapse_siblings(
    candidates: Vec<Candidate>,
    known_facts: &[ModelFact],
    class: PanelClass,
    floor: f64,
) -> (Vec<Candidate>, Vec<SiblingCollapse>) {
    let prefer_pro = matches!(
        class,
        PanelClass::Diagnose | PanelClass::Audit | PanelClass::Spec
    ) && floor >= 80.0;
    let capacity_keys: HashSet<String> = known_facts
        .iter()
        .map(|fact| make_capacity_key(&fact.provider_id, &fact.model_id))
        .collect();
    let mut groups: Vec<(String, Vec<Candidate>)> = Vec::new();

    for candidate in candidates {
        let group_key = sibling_group_key(&candidate.route, &capacity_keys);
        if let Some((_, group)) = groups.iter_mut().find(|(key, _)| *key == group_key) {
            group.push(candidate);
        } else {
            groups.push((group_key, vec![candidate]));
        }
    }

    let mut collapsed_candidates = Vec::new();
    let mut collapses = Vec::new();

    for (base_key, mut group) in groups {
        let pro_key = format!("{base_key}-pro");
        let has_pro = group
            .iter()
            .any(|candidate| candidate.route.capacity_key == pro_key);
        let preferred_key = if prefer_pro && has_pro {
            pro_key.as_str()
        } else {
            base_key.as_str()
        };
        let selected_index = group
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.route.capacity_key == preferred_key)
            .min_by(|(_, a), (_, b)| {
                let a_is_variant = a.route.route_key != a.route.capacity_key;
                let b_is_variant = b.route.route_key != b.route.capacity_key;
                a_is_variant
                    .cmp(&b_is_variant)
                    .then_with(|| a.route.route_key.cmp(&b.route.route_key))
            })
            .map(|(index, _)| index)
            .or_else(|| {
                if prefer_pro {
                    group
                        .iter()
                        .position(|candidate| candidate.route.capacity_key == base_key)
                } else {
                    None
                }
            })
            .or_else(|| (!group.is_empty()).then_some(0));
        let Some(selected_index) = selected_index else {
            continue;
        };
        let retained = group.remove(selected_index);
        let retained_route_key = retained.route.route_key.clone();
        let collapsed_route_keys: Vec<String> = group
            .iter()
            .map(|candidate| candidate.route.route_key.clone())
            .collect();
        if !collapsed_route_keys.is_empty() {
            collapses.push(SiblingCollapse {
                model: base_key,
                retained_route_key,
                collapsed_route_keys,
            });
        }
        collapsed_candidates.push(retained);
    }

    (collapsed_candidates, collapses)
}

fn sibling_variant(route_key: &str) -> SeatedSiblingVariant {
    if route_key
        .split('/')
        .nth(1)
        .is_some_and(|model_id| model_id.ends_with("-pro"))
    {
        SeatedSiblingVariant::Pro
    } else {
        SeatedSiblingVariant::Base
    }
}
