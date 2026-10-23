//! Apply consumer exclusions and select pinned models without quality ranking.

use crate::config_policy::{ExcludeGroups, Pins, SynthesizerPin};
use prefrontal_routing_core::model_facts::ModelRequirements;
use prefrontal_routing_core::model_router::{SelectModelRouteInput, SelectedModelRoute};
use prefrontal_routing_core::provider_model_spec::ExclusionPolicy;
use prefrontal_routing_core::{ConcurrencySlots, RoutingConfig};
use serde::Deserialize;
use std::collections::HashMap;

use prefrontal_routing_core::model_facts::{fact_to_route, ModelFact};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Consumer {
    Mason,
    Sidekick,
    Athena,
    Lab,
    Core,
    Codemode,
    Flow,
}

pub(crate) fn consumer_exclusions(groups: &ExcludeGroups, consumer: Option<Consumer>) -> &[String] {
    match consumer {
        Some(Consumer::Mason) => &groups.mason,
        Some(Consumer::Sidekick) => &groups.sidekick,
        Some(Consumer::Athena) => &groups.athena,
        Some(Consumer::Lab) => &groups.lab,
        Some(Consumer::Core) => &groups.core,
        Some(Consumer::Codemode) => &groups.codemode,
        Some(Consumer::Flow) => &groups.flow,
        None => &[],
    }
}

pub(crate) fn requires_metered(consumer: Option<Consumer>) -> bool {
    matches!(consumer, Some(Consumer::Codemode | Consumer::Flow))
}

/// Admission uses these route-local refusals after exclusions and before scoring,
/// including for pins. Neither an explicit model nor a quota retry lifts them.
pub(crate) fn candidate_rejections(
    facts: &[ModelFact],
    consumer: Option<Consumer>,
    allowed_providers: Option<&[String]>,
    subscription_plan_providers: Option<&std::collections::HashSet<String>>,
) -> HashMap<String, String> {
    facts
        .iter()
        .filter_map(|fact| {
            let detail = if allowed_providers
                .is_some_and(|allowed| !allowed.contains(&fact.provider_id))
            {
                Some(format!("allowed_providers filter denies provider {}", fact.provider_id))
            } else if requires_metered(consumer)
                && !(fact.auth_methods.as_ref().is_some_and(|methods| methods.iter().any(|method| method == "apikey"))
                    && fact.billing_plane.as_ref().is_some_and(|plane| plane.is_api_metered())
                    && subscription_plan_providers.is_some_and(|providers| !providers.contains(&fact.provider_id)))
            {
                Some(format!(
                    "consumer {} requires known apikey auth methods, a metered billing plane with a known output price, and known absence of a subscription plan",
                    crate::variant_policy::consumer_group(consumer).unwrap()
                ))
            } else {
                None
            };
            detail.map(|detail| (fact_to_route(fact).route_key, detail))
        })
        .collect()
}

pub(crate) fn describe_filter_refusal(
    mut error: prefrontal_routing_core::model_router::NoRouteError,
) -> prefrontal_routing_core::model_router::NoRouteError {
    use prefrontal_routing_core::model_router::RouteFailureReason;
    if error.reason == RouteFailureReason::NoAvailableModel {
        let details: std::collections::BTreeSet<_> = error
            .rejected
            .iter()
            .filter_map(|route| route.detail.as_deref())
            .filter(|detail| {
                detail.starts_with("allowed_providers filter") || detail.starts_with("consumer ")
            })
            .collect();
        if !details.is_empty() {
            error.message = format!(
                "{}; no candidates remain after selection filters: {}",
                error.message,
                details.into_iter().collect::<Vec<_>>().join("; ")
            );
        }
    }
    error
}

pub(crate) fn deserialize_allowed_providers<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let ids = Vec::<String>::deserialize(deserializer)
        .map_err(|error| D::Error::custom(format!("allowed_providers: {error}")))?;
    let distinct: std::collections::HashSet<_> = ids.iter().collect();
    if ids.is_empty()
        || distinct.len() != ids.len()
        || ids.iter().any(|id| {
            id.trim().is_empty() || id.contains('/') || id.chars().any(char::is_whitespace)
        })
    {
        return Err(D::Error::custom(
            "allowed_providers must be a non-empty array of distinct non-empty provider ids",
        ));
    }
    Ok(Some(ids))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Pin {
    Slot(PinSlotRequest),
    Model(SynthesizerPin),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinSlotRequest {
    slot: PinSlot,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PinSlot {
    LabPlanner,
    SpecDesigner,
}

impl Pin {
    /// The pin a request names, or `None` when it names a slot the operator
    /// left unset; the request is then selected like an unpinned one.
    pub(crate) fn resolve(&self, pins: &Pins) -> Option<SynthesizerPin> {
        match self {
            Self::Slot(PinSlotRequest {
                slot: PinSlot::LabPlanner,
            }) => pins.lab_planner.as_ref().map(|pin| SynthesizerPin {
                model: pin.model.clone(),
                variant: None,
            }),
            // Only the designer's own pin: a configured synthesizer pin is
            // Athena's choice and never routes the designer.
            Self::Slot(PinSlotRequest {
                slot: PinSlot::SpecDesigner,
            }) => pins.spec_designer.clone(),
            Self::Model(pin) => Some(pin.clone()),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PinFallback {
    #[default]
    Route,
    None,
}

pub(crate) fn select_pin_with_availability(
    pin: &SynthesizerPin,
    mut input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    availability_rejections: &std::collections::HashMap<String, String>,
) -> Result<SelectedModelRoute, String> {
    let (provider, model) = pin.model.split_once('/').ok_or("invalid_model_pin")?;
    if provider.is_empty()
        || model.is_empty()
        || pin.model.chars().any(char::is_whitespace)
        || pin
            .variant
            .as_ref()
            .is_some_and(|variant| variant.trim().is_empty())
    {
        return Err("invalid_model_pin".into());
    }
    let mut fact = input
        .facts
        .iter()
        .filter(|fact| fact.provider_id == provider && fact.model_id == model)
        .min_by_key(|fact| {
            if fact.variant == pin.variant {
                0
            } else if fact.variant.is_none() {
                1
            } else {
                2
            }
        })
        .cloned()
        .ok_or("unknown_model")?;
    if let Some(variant) = &pin.variant {
        fact.variant = Some(variant.clone());
        let defaults = fact.prompt_defaults.get_or_insert_with(Default::default);
        defaults.variant = Some(variant.clone());
        defaults.reasoning_effort = None;
    }
    input.facts = vec![fact];
    input.operator_exclude_keys.clear();
    let admission = prefrontal_routing_core::model_router::admit_routes_with_availability(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy::caller_only(&input.exclude_route_keys),
        &[],
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
        availability_rejections,
    );
    let adequacy = admission
        .admitted
        .first()
        .ok_or_else(|| pin_rejection(&admission.rejected[0].reason))?
        .adequacy
        .clone();
    // A pin chooses a model rather than ranking it by quality. The normal router
    // still checks availability and quota reserved for head sessions, but a low
    // quality score must not reject or substitute the caller's explicit choice.
    input.requirements = ModelRequirements { iq: 0.0, eq: 0.0 };
    input
        .required_capabilities
        .get_or_insert_with(Default::default)
        .min_context
        .get_or_insert(0.0);
    let mut selected = prefrontal_routing_core::model_router::select_model_route_with_availability(
        input,
        config,
        slots,
        &[],
        availability_rejections,
    )
    .map_err(|error| {
        error.rejected.first().map_or_else(
            || error.reason.as_str().into(),
            |route| pin_rejection(&route.reason),
        )
    })?;
    selected.score.adequacy = adequacy.adequacy;
    selected.score.raw_adequacy = adequacy.raw_adequacy;
    selected.score.metrics = adequacy.metrics;
    selected.below_floor = None;
    selected.explanation = vec![format!("admission-only pin: {}", pin.model)];
    Ok(selected)
}

fn pin_rejection(reason: &str) -> String {
    match reason {
        "excluded" => "caller_excluded".into(),
        reason => reason.into(),
    }
}
