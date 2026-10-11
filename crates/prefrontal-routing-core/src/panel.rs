//! Panel selection over the shared route admission boundary.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
    /// Adversarial spec-campaign rounds. Seat policy mirrors Audit: family
    /// diversity is the point of a multi-seat spec panel, and pinned rosters
    /// bypass exclusion exactly as they do for audits.
    Spec,
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelRunnerUp {
    pub route_key: String,
    pub prompt_params: Map<String, Value>,
    pub family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
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
#[serde(rename_all = "snake_case")]
pub enum PanelSelectionPolicy {
    DiversityFirst,
    TopIq,
    Mixed,
    CheapestAdequate,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelPolicy {
    pub selection: PanelSelectionPolicy,
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
    if input.selection != PanelSelection::Roster || input.package_tokens.is_none() {
        return select_panel_admitted(&input, config);
    }
    let tokens = input.package_tokens.unwrap();
    let mut panel = select_panel_admitted(&input, config)?;
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
    let spares = select_panel_admitted(&automatic, config)?;
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
        })
        .collect();
    if unfilled {
        let mut causes = panel
            .shortfall
            .take()
            .map(|shortfall| shortfall.causes)
            .unwrap_or_default();
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
            detail: package_refusal(tokens, largest),
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

fn package_refusal(tokens: usize, largest: usize) -> String {
    format!("package {tokens} tokens cannot fill the panel; largest available adequate ceiling {largest} tokens. Narrow evidence or change the project athena.members roster setting; evidence was not truncated")
}

fn select_panel_admitted(
    input: &SelectPanelInput<'_>,
    config: &RoutingConfig,
) -> Result<SelectedPanel, InvalidPanelRequest> {
    validate_panel_spec(&input.panel_spec)?;

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
            let is_subscription = input
                .is_subscription_route
                .as_ref()
                .is_some_and(|predicate| predicate(&admitted.route));
            if is_subscription
                && !crate::model_router::subscription_budget_available(&admitted.runtime, false)
            {
                causes.push(PanelCause {
                    model: admitted.route.capacity_key.clone(),
                    reason: PanelCauseReason::QuotaExhausted,
                    detail: Some("weekly headroom is reserved for heads".into()),
                    reset_at_ms: reserved_headroom_reset_at_ms(&admitted.runtime),
                });
                None
            } else {
                Some((admitted, is_subscription))
            }
        })
        .collect::<Vec<_>>();
    let admitted_count = admitted.len();

    // Zero means no IQ requirement, matching singular route selection. The floor
    // governs AUTOMATIC (n_pick) admission only: an explicit roster is the
    // operator's statement of who sits, and every generic filter that has been
    // allowed to override a pin has produced a silent-downgrade incident
    // (exclusion dropped pinned Kimi 2026-07-21, family diversity dropped Opus
    // beside Fable 2026-07-16, and the floor halved a pinned 4-seat audit to
    // sol+opus 2026-08-06). Pins bypass the floor exactly as they bypass both.
    let iq_bar = (input.selection == PanelSelection::NPick && input.panel_spec.iq_floor > 0.0)
        .then(|| {
            demand_bar(
                top_routable_elo,
                input.panel_spec.iq_floor,
                config.demand_exponent,
            )
        });
    let mut candidates = Vec::new();
    let mut largest_package_ceiling = None;
    for (admitted, is_subscription) in admitted {
        let model = admitted.route.capacity_key.clone();
        if iq_bar.is_some_and(|bar| admitted.route.fact.scores.elo < bar) {
            causes.push(PanelCause {
                model,
                reason: PanelCauseReason::BelowIqFloor,
                reset_at_ms: None,
                detail: None,
            });
            continue;
        }
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
            metered_cost: normalized_metered_cost(&admitted.route.fact),
            route: admitted.route,
            prompt_params: params,
            selection_cost: objective.selection_cost,
            speed_score: objective.speed_score,
        });
    }
    let iq_eligible_count = candidates.len();

    let (mut candidates, sibling_collapses) = if input.selection == PanelSelection::NPick {
        collapse_siblings(
            candidates,
            &considered_facts,
            input.panel_spec.class,
            input.panel_spec.iq_floor,
        )
    } else {
        (candidates, Vec::new())
    };

    let selection_policy = selection_policy(&input.panel_spec);
    order_candidates(
        &mut candidates,
        input.selection,
        selection_policy,
        input.panel_spec.diversity,
    );

    // Family rules police the AUTOMATIC picker only. An explicit roster is the
    // caller's own diversity decision: pinning two same-family models means two
    // seats, and an unlabeled family must not eject a pin.
    let enforce_family_diversity = input.panel_spec.diversity == PanelDiversity::Family
        && input.selection == PanelSelection::NPick;
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

    let mut selected = HashSet::new();
    let mut selected_families = HashSet::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if selected.len() == input.panel_spec.size {
            break;
        }
        if !enforce_family_diversity
            || selected_families.insert(candidate.route.fact.family.clone().unwrap())
        {
            selected.insert(index);
        }
    }

    if enforce_family_diversity && selected.len() < input.panel_spec.size {
        for (index, candidate) in candidates.iter().enumerate() {
            if !selected.contains(&index) {
                causes.push(PanelCause {
                    model: candidate.route.capacity_key.clone(),
                    reason: PanelCauseReason::FamilyDuplicate,
                    reset_at_ms: None,
                    detail: None,
                });
            }
        }
    }

    let mut members = Vec::new();
    let mut runner_up = Vec::new();
    for (index, candidate) in candidates.into_iter().enumerate() {
        let context_window = context_window_tokens(&candidate.route.fact);
        if selected.contains(&index) {
            members.push(PanelMember {
                route_key: candidate.route.route_key,
                prompt_params: candidate.prompt_params,
                family: candidate.route.fact.family,
                temperature: None,
                max_output_tokens: candidate.route.fact.max_output_tokens,
                context_window,
            });
        } else {
            runner_up.push(PanelRunnerUp {
                context_window,
                route_key: candidate.route.route_key,
                prompt_params: candidate.prompt_params,
                family: candidate.route.fact.family,
                max_output_tokens: candidate.route.fact.max_output_tokens,
            });
        }
    }
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
                input.panel_spec.size
            ),
        )
    };
    let shortfall_kind = if input.package_tokens.is_some()
        && members.len() < input.panel_spec.size
        && causes
            .iter()
            .any(|cause| cause.reason == PanelCauseReason::PackageTooLarge)
    {
        Some((
            PanelShortfallCode::PackageUnmeetable,
            package_refusal(
                input.package_tokens.unwrap(),
                largest_package_ceiling.unwrap_or(0),
            ),
        ))
    } else if admitted_count == 0 {
        Some(size_reduced())
    } else if let Some(bar) = iq_bar.filter(|_| iq_eligible_count == 0) {
        Some((
            PanelShortfallCode::IqFloorUnmeetable,
            format!("no admitted roster model clears raw iq floor {bar:.2}"),
        ))
    } else if enforce_family_diversity && members.len() < input.panel_spec.size {
        Some((
            PanelShortfallCode::DiversityUnmeetable,
            format!(
                "only {} distinct labeled families can fill {} requested slots",
                members.len(),
                input.panel_spec.size
            ),
        ))
    } else if members.len() < input.panel_spec.size {
        Some(size_reduced())
    } else {
        None
    };

    let shortfall = shortfall_kind.map(|(code, detail)| {
        if causes.is_empty() {
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
        selection: selection_policy,
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
                    head_points_per_hour: None,
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let cause = PanelCause {
            model: "anthropic/claude-opus-5-5".into(),
            reason: PanelCauseReason::QuotaExhausted,
            detail: Some("weekly headroom is reserved for heads".into()),
            reset_at_ms: reserved_headroom_reset_at_ms(&runtime),
        };
        assert_eq!(cause.reset_at_ms, Some(1791686378812));
        let wire = serde_json::to_value(&cause).unwrap();
        assert_eq!(wire["reset_at_ms"], 1791686378812i64);
        assert_eq!(wire["detail"], "weekly headroom is reserved for heads");
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

fn selection_policy(spec: &PanelSpec) -> PanelSelectionPolicy {
    match spec.class {
        PanelClass::Audit | PanelClass::Spec if spec.diversity == PanelDiversity::Family => {
            PanelSelectionPolicy::DiversityFirst
        }
        PanelClass::DesignReview | PanelClass::Evaluate
            if spec.diversity == PanelDiversity::Family =>
        {
            PanelSelectionPolicy::Mixed
        }
        PanelClass::Explain => PanelSelectionPolicy::CheapestAdequate,
        PanelClass::Audit
        | PanelClass::Spec
        | PanelClass::DesignReview
        | PanelClass::Evaluate
        | PanelClass::Diagnose => PanelSelectionPolicy::TopIq,
    }
}

fn order_candidates(
    candidates: &mut [Candidate],
    mode: PanelSelection,
    policy: PanelSelectionPolicy,
    diversity: PanelDiversity,
) {
    if mode == PanelSelection::Roster {
        candidates.sort_by(iq_cost_order);
        return;
    }

    match policy {
        PanelSelectionPolicy::CheapestAdequate => candidates.sort_by(cost_primary_order),
        PanelSelectionPolicy::TopIq => candidates.sort_by(iq_lineage_order),
        PanelSelectionPolicy::DiversityFirst => diversity_first_order(candidates, false),
        PanelSelectionPolicy::Mixed => diversity_first_order(candidates, true),
    }

    if diversity == PanelDiversity::None && policy == PanelSelectionPolicy::DiversityFirst {
        candidates.sort_by(iq_lineage_order);
    }
}

fn diversity_first_order(candidates: &mut [Candidate], preserve_top_slot: bool) {
    candidates.sort_by(iq_lineage_order);
    if candidates.len() < 2 {
        return;
    }

    let mut ordered_indices = Vec::with_capacity(candidates.len());
    let mut seen_families = HashSet::new();
    if preserve_top_slot {
        ordered_indices.push(0);
        seen_families.insert(candidates[0].route.fact.family.clone());
    }
    for (index, candidate) in candidates.iter().enumerate() {
        if preserve_top_slot && index == 0 {
            continue;
        }
        if seen_families.insert(candidate.route.fact.family.clone()) {
            ordered_indices.push(index);
        }
    }
    for index in 0..candidates.len() {
        if !ordered_indices.contains(&index) {
            ordered_indices.push(index);
        }
    }

    let mut old_positions: Vec<usize> = (0..candidates.len()).collect();
    for (new_position, &wanted_old_position) in ordered_indices.iter().enumerate() {
        let current_position = old_positions
            .iter()
            .position(|position| *position == wanted_old_position)
            .expect("candidate permutation is complete");
        candidates.swap(new_position, current_position);
        old_positions.swap(new_position, current_position);
    }
}

fn iq_cost_order(a: &Candidate, b: &Candidate) -> Ordering {
    b.route
        .fact
        .scores
        .elo
        .total_cmp(&a.route.fact.scores.elo)
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
        .then_with(|| a.selection_cost.total_cmp(&b.selection_cost))
        .then_with(|| b.speed_score.total_cmp(&a.speed_score))
        .then_with(|| a.metered_cost.total_cmp(&b.metered_cost))
        .then_with(|| a.route.fact.family.cmp(&b.route.fact.family))
        .then_with(|| a.route.route_key.cmp(&b.route.route_key))
}

fn cost_primary_order(a: &Candidate, b: &Candidate) -> Ordering {
    a.selection_cost
        .total_cmp(&b.selection_cost)
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
