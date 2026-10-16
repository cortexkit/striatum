//! Apply consumer exclusions and select pinned models without quality ranking.

use crate::config_policy::{ExcludeGroups, Pins, SynthesizerPin};
use prefrontal_routing_core::model_facts::ModelRequirements;
use prefrontal_routing_core::model_router::{
    admit_routes, select_model_route_with_slots, SelectModelRouteInput, SelectedModelRoute,
};
use prefrontal_routing_core::provider_model_spec::ExclusionPolicy;
use prefrontal_routing_core::{ConcurrencySlots, RoutingConfig};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Consumer {
    Mason,
    Sidekick,
    Athena,
    Lab,
    Core,
}

pub(crate) fn consumer_exclusions(groups: &ExcludeGroups, consumer: Option<Consumer>) -> &[String] {
    match consumer {
        Some(Consumer::Mason) => &groups.mason,
        Some(Consumer::Sidekick) => &groups.sidekick,
        Some(Consumer::Athena) => &groups.athena,
        Some(Consumer::Lab) => &groups.lab,
        Some(Consumer::Core) => &groups.core,
        None => &[],
    }
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
}

impl Pin {
    pub(crate) fn resolve(&self, pins: &Pins) -> Option<SynthesizerPin> {
        match self {
            Self::Slot(PinSlotRequest {
                slot: PinSlot::LabPlanner,
            }) => pins.lab_planner.as_ref().map(|pin| SynthesizerPin {
                model: pin.model.clone(),
                variant: None,
            }),
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

pub(crate) fn select_pin(
    pin: &SynthesizerPin,
    mut input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
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
    input.explicit_only_providers.clear();
    let admission = admit_routes(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy::caller_only(&input.exclude_route_keys),
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
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
    let mut selected = select_model_route_with_slots(input, config, slots).map_err(|error| {
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
