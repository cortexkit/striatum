//! Panel selection over the shared route admission boundary.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::model_facts::{
    make_capacity_key, ModelFact, ModelRequirements, ModelRoute, FAST_SUFFIX,
};
use crate::model_router::{
    admit_routes, compute_objective, normalized_metered_cost, ModelRouteStateSnapshot,
    RequiredCapabilities,
};
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
    pub panel_spec: PanelSpec,
    pub selection: PanelSelection,
    pub roster: Vec<PanelRosterEntry>,
    pub package_tokens_estimate: Option<u32>,
    pub pro_member_max_package_tokens: Option<u32>,
    pub facts: Vec<ModelFact>,
    pub runtime_snapshot: Option<ModelRouteStateSnapshot>,
    pub exclude_route_keys: Vec<String>,
    pub required_capabilities: Option<RequiredCapabilities>,
    pub available_models: HashSet<String>,
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
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PanelRunnerUp {
    pub route_key: String,
    pub prompt_params: Map<String, Value>,
    pub family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelCauseReason {
    Excluded,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelShortfallCode {
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
    pub pro_downgraded_for_size: bool,
    pub sibling_collapses: Vec<SiblingCollapse>,
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

fn validate_selection(input: &SelectPanelInput<'_>) -> Result<(), InvalidPanelRequest> {
    if input.selection == PanelSelection::NPick {
        if input.package_tokens_estimate.is_none() {
            return Err(InvalidPanelRequest(
                "packageTokensEstimate is required when selection is n_pick".into(),
            ));
        }
        if input.pro_member_max_package_tokens.is_none() {
            return Err(InvalidPanelRequest(
                "proMemberMaxPackageTokens is required when selection is n_pick".into(),
            ));
        }
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
    validate_panel_spec(&input.panel_spec)?;
    validate_selection(&input)?;

    let mut prompt_params: HashMap<String, VecDeque<Map<String, Value>>> = HashMap::new();
    let mut causes = Vec::new();
    let considered_facts = match input.selection {
        PanelSelection::Roster => roster_facts(&input, &mut prompt_params, &mut causes),
        PanelSelection::NPick => input.facts.clone(),
    };

    // Explicit roster pins BYPASS the exclusion list by design: the global
    // model_routing.exclude reserves scarce models (kimi, anthropic/fable) from
    // automatic routing precisely SO they remain usable via deliberate roster
    // pins. Applying the list here silently shrank pinned audit panels to the
    // unexcluded seats. Availability, quota/cooldown, and capability gates
    // still apply to pins: only the exclusion predicate is roster-exempt.
    let no_exclusions: Vec<String> = Vec::new();
    let effective_exclusions = match input.selection {
        PanelSelection::Roster => &no_exclusions,
        PanelSelection::NPick => &input.exclude_route_keys,
    };
    let exact_model_pins: Vec<String> = match input.selection {
        PanelSelection::Roster => input
            .roster
            .iter()
            .map(|entry| entry.model.clone())
            .collect(),
        PanelSelection::NPick => Vec::new(),
    };
    // Panel selection has no adequacy fallback. The zero requirements are only an
    // unused scoring input to the shared hard-admission implementation.
    let admission = admit_routes(
        &considered_facts,
        input.runtime_snapshot.as_ref(),
        effective_exclusions,
        &[],
        &exact_model_pins,
        input.required_capabilities,
        &input.available_models,
        &ModelRequirements { iq: 0.0, eq: 0.0 },
        config,
    );
    let admitted_count = admission.admitted.len();
    let top_routable_elo = admission.top_routable.elo;
    causes.extend(admission.rejected.into_iter().map(|rejected| PanelCause {
        model: rejected.route_key,
        reason: match rejected.reason.as_str() {
            "excluded" => PanelCauseReason::Excluded,
            "quota_exhausted" | "cooldown_active" => PanelCauseReason::QuotaExhausted,
            "backoff_active" => PanelCauseReason::BackoffActive,
            "missing_vision" | "context_too_small" => PanelCauseReason::Capability,
            _ => PanelCauseReason::Unavailable,
        },
        detail: rejected.detail,
    }));

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
    for admitted in admission.admitted {
        let model = admitted.route.capacity_key.clone();
        if iq_bar.is_some_and(|bar| admitted.route.fact.scores.elo < bar) {
            causes.push(PanelCause {
                model,
                reason: PanelCauseReason::BelowIqFloor,
                detail: None,
            });
            continue;
        }
        let params = match input.selection {
            PanelSelection::Roster => prompt_params
                .get_mut(&model)
                .and_then(VecDeque::pop_front)
                .unwrap_or_default(),
            PanelSelection::NPick => fact_prompt_params(&admitted.route.fact),
        };
        let is_subscription = input
            .is_subscription_route
            .as_ref()
            .map(|predicate| predicate(&admitted.route))
            .unwrap_or(false);
        let (objective, _) = compute_objective(&admitted.route, &admitted.runtime, is_subscription);
        candidates.push(Candidate {
            metered_cost: normalized_metered_cost(&admitted.route.fact),
            route: admitted.route,
            prompt_params: params,
            selection_cost: objective.selection_cost,
            speed_score: objective.speed_score,
        });
    }
    let iq_eligible_count = candidates.len();

    let (mut candidates, sibling_collapses, size_downgraded_routes) =
        if input.selection == PanelSelection::NPick {
            collapse_siblings(
                candidates,
                &considered_facts,
                input.panel_spec.class,
                input.panel_spec.iq_floor,
                input
                    .package_tokens_estimate
                    .expect("n_pick input validated"),
                input
                    .pro_member_max_package_tokens
                    .expect("n_pick input validated"),
            )
        } else {
            (candidates, Vec::new(), HashSet::new())
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
                    detail: None,
                });
            }
        }
    }

    let mut members = Vec::new();
    let mut runner_up = Vec::new();
    for (index, candidate) in candidates.into_iter().enumerate() {
        if selected.contains(&index) {
            members.push(PanelMember {
                route_key: candidate.route.route_key,
                prompt_params: candidate.prompt_params,
                family: candidate.route.fact.family,
                temperature: None,
                max_output_tokens: candidate.route.fact.max_output_tokens,
            });
        } else {
            runner_up.push(PanelRunnerUp {
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

    let shortfall_code = if admitted_count == 0 {
        Some(PanelShortfallCode::SizeReduced)
    } else if iq_eligible_count == 0 {
        Some(PanelShortfallCode::IqFloorUnmeetable)
    } else if enforce_family_diversity && members.len() < input.panel_spec.size {
        Some(PanelShortfallCode::DiversityUnmeetable)
    } else if members.len() < input.panel_spec.size {
        Some(PanelShortfallCode::SizeReduced)
    } else {
        None
    };

    let shortfall = shortfall_code.map(|code| {
        if causes.is_empty() {
            causes.push(PanelCause {
                model: "<unfilled-slot>".into(),
                reason: PanelCauseReason::Unavailable,
                detail: None,
            });
        }
        let detail = match code {
            PanelShortfallCode::IqFloorUnmeetable => {
                format!(
                    "no admitted roster model clears raw iq floor {:.2}",
                    iq_bar.expect("an IQ-floor shortfall requires a positive floor")
                )
            }
            PanelShortfallCode::DiversityUnmeetable => format!(
                "only {} distinct labeled families can fill {} requested slots",
                members.len(),
                input.panel_spec.size
            ),
            PanelShortfallCode::SizeReduced => format!(
                "only {} of {} requested panel slots can be served",
                members.len(),
                input.panel_spec.size
            ),
        };
        PanelShortfall {
            code,
            detail,
            causes,
        }
    });

    let policy = (input.selection == PanelSelection::NPick).then(|| PanelPolicy {
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
        pro_downgraded_for_size: members
            .iter()
            .any(|member| size_downgraded_routes.contains(&member.route_key)),
        sibling_collapses,
    });

    Ok(SelectedPanel {
        members,
        runner_up,
        shortfall,
        policy,
    })
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
    for new_position in 0..candidates.len() {
        let wanted_old_position = ordered_indices[new_position];
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
    package_tokens_estimate: u32,
    pro_member_max_package_tokens: u32,
) -> (Vec<Candidate>, Vec<SiblingCollapse>, HashSet<String>) {
    let pro_policy_eligible = matches!(
        class,
        PanelClass::Diagnose | PanelClass::Audit | PanelClass::Spec
    ) && floor >= 80.0;
    let size_blocked =
        pro_policy_eligible && package_tokens_estimate > pro_member_max_package_tokens;
    let prefer_pro = pro_policy_eligible && !size_blocked;
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
    let mut size_downgraded_routes = HashSet::new();

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
        if size_blocked && has_pro && retained.route.capacity_key == base_key {
            size_downgraded_routes.insert(retained_route_key.clone());
        }
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

    (collapsed_candidates, collapses, size_downgraded_routes)
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
