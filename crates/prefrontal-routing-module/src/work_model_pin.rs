//! Exact worker pins share hard admission, but never the quality/cost competition.

use prefrontal_routing_core::model_facts::{fact_to_route, ModelFact, ModelRequirements};
use prefrontal_routing_core::model_resolution::{
    resolve_model_capabilities, ModelCapabilitiesInput,
};
use prefrontal_routing_core::model_router::{
    admit_routes, select_model_route_with_slots, SelectModelRouteInput, SelectedModelRoute,
};
use prefrontal_routing_core::provider_model_spec::ExclusionPolicy;
use prefrontal_routing_core::{ConcurrencySlots, RoutingConfig};

pub(crate) struct Refusal {
    pub code: String,
    pub message: String,
}

fn refuse(code: &str, message: String) -> Refusal {
    Refusal {
        code: code.into(),
        message,
    }
}

pub(crate) fn select(
    pin: &str,
    mut input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
) -> Result<SelectedModelRoute, Refusal> {
    let (provider, requested_model) = pin.split_once('/').ok_or_else(|| {
        refuse(
            "invalid_model_pin",
            format!("{pin:?}: expected provider/model or provider/model:variant"),
        )
    })?;
    if provider.is_empty()
        || provider.contains(':')
        || requested_model.is_empty()
        || pin.chars().any(char::is_whitespace)
    {
        return Err(refuse(
            "invalid_model_pin",
            format!("{pin:?}: expected provider/model or provider/model:variant"),
        ));
    }
    if !input.facts.iter().any(|fact| fact.provider_id == provider) {
        return Err(refuse(
            "unknown_model_provider",
            format!(
                "unknown provider {provider:?} in {pin:?}; valid variants: none (unknown provider)"
            ),
        ));
    }
    // Some catalog IDs contain a colon (for example a model size). Prefer a
    // complete catalog identity; only an additional suffix is a reasoning tier.
    let (model, variant) = if input
        .facts
        .iter()
        .any(|fact| fact.provider_id == provider && fact.model_id == requested_model)
    {
        (requested_model, None)
    } else {
        requested_model
            .rsplit_once(':')
            .map_or((requested_model, None), |(id, tier)| (id, Some(tier)))
    };
    let identity = format!("{provider}/{model}");
    let facts: Vec<ModelFact> = input
        .facts
        .iter()
        .filter(|fact| fact.provider_id == provider && fact.model_id == model)
        .cloned()
        .collect();
    if facts.is_empty() {
        return Err(refuse("unknown_model", format!("unknown model {model:?} for provider {provider:?}; valid variants: none (unknown model)")));
    }
    let mut tiers = resolve_model_capabilities(&ModelCapabilitiesInput {
        provider_id: provider.into(),
        model_id: model.into(),
        ..Default::default()
    })
    .variants
    .unwrap_or_default();
    for fact in &facts {
        if let Some(tier) = fact.variant.as_ref().or_else(|| {
            fact.prompt_defaults
                .as_ref()
                .and_then(|defaults| defaults.variant.as_ref())
        }) {
            if matches!(tier.as_str(), "low" | "medium" | "high" | "xhigh" | "max")
                && !tiers.contains(tier)
            {
                tiers.push(tier.clone());
            }
        }
    }
    tiers.retain(|tier| matches!(tier.as_str(), "low" | "medium" | "high" | "xhigh" | "max"));
    if variant.is_some_and(|tier| !tiers.iter().any(|valid| valid == tier)) {
        return Err(refuse(
            "invalid_model_variant",
            format!(
                "unknown variant {:?} for {identity}; valid variants: {}",
                variant.unwrap(),
                if tiers.is_empty() {
                    "none".into()
                } else {
                    tiers.join(", ")
                }
            ),
        ));
    }
    let mut fact = facts
        .iter()
        .find(|fact| fact.variant.as_deref() == variant)
        .or_else(|| facts.iter().find(|fact| fact.variant.is_none()))
        .unwrap_or(&facts[0])
        .clone();
    if fact.supports_tool_use != Some(true) {
        return Err(refuse(
            "missing_tool_support",
            format!("{identity}: tool support is not admitted by the catalog"),
        ));
    }
    if let Some(variant) = variant {
        fact.variant = Some(variant.into());
        let defaults = fact.prompt_defaults.get_or_insert_with(Default::default);
        defaults.variant = Some(variant.into());
        // A default effort must not override the explicit variant at a host boundary.
        defaults.reasoning_effort = None;
    }
    input.facts = vec![fact];
    // The caller named this exact route (a work run's `model=`), which is the one
    // way an explicit-only provider is admitted. A caller that forwarded the operator's explicit-only list
    // must not turn the pin into a refusal.
    input.explicit_only_providers.clear();
    let admission = admit_routes(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy {
            operator: &input.operator_exclude_keys,
            caller: &input.exclude_route_keys,
            explicit_only: &[],
        },
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
    );
    let Some(admitted) = admission.admitted.first() else {
        let rejection = &admission.rejected[0];
        let route = fact_to_route(&input.facts[0]);
        let until = input
            .runtime_snapshot
            .as_ref()
            .filter(|_| {
                matches!(
                    rejection.reason.as_str(),
                    "quota_exhausted" | "cooldown_active"
                )
            })
            .and_then(|snapshot| {
                [
                    snapshot.by_route.get(&route.route_key),
                    snapshot.by_capacity.get(&route.capacity_key),
                ]
                .into_iter()
                .flatten()
                .flat_map(|state| [state.cooldown_until, state.quota_exhausted_until])
                .flatten()
                .filter(|until| *until > snapshot.now)
                .max_by(f64::total_cmp)
            });
        let clearing = until
            .map(|ms| format!(" until {}", crate::account_selection::rfc3339(ms as i64)))
            .unwrap_or_default();
        return Err(refuse(
            &rejection.reason,
            format!(
                "{pin}: {}{clearing}{}",
                rejection.reason,
                rejection
                    .detail
                    .as_ref()
                    .map(|detail| format!(" ({detail})"))
                    .unwrap_or_default()
            ),
        ));
    };
    let adequacy = admitted.adequacy.clone();
    // The shared renderer prices one already-admitted route. Demand remains in
    // its audit score, but cannot reject or substitute the head's explicit choice.
    input.requirements = ModelRequirements { iq: 0.0, eq: 0.0 };
    // The singular router requires a capability on a zero-demand request. Hard
    // capabilities were already checked above; a zero context bound adds none.
    input
        .required_capabilities
        .get_or_insert_with(Default::default)
        .min_context
        .get_or_insert(0.0);
    let mut selected = select_model_route_with_slots(input, config, slots)
        .map_err(|error| refuse(error.reason.as_str(), error.message))?;
    selected.score.adequacy = adequacy.adequacy;
    selected.score.raw_adequacy = adequacy.raw_adequacy;
    selected.score.metrics = adequacy.metrics;
    selected.below_floor = None;
    selected.explanation = vec![format!("model pinned by head: {pin}; hard admission only")];
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_routing_core::model_router::RequiredCapabilities;
    use std::collections::HashSet;

    #[test]
    fn pinned_model_checks_context_tools_and_operator_fences_without_quality_selection() {
        for (tool_support, context, operator, available, expected) in [
            (true, 8192.0, false, true, None),
            (true, 300_000.0, false, true, Some("context_too_small")),
            (false, 8192.0, false, true, Some("missing_tool_support")),
            (true, 8192.0, true, true, Some("excluded_by_operator")),
            (true, 8192.0, false, false, Some("not_available")),
        ] {
            let mut fact = crate::tests::fact("openai", "gpt-5.5");
            // Even a model with weak judgment is the head's explicit choice.
            fact.scores.eq = 100.0;
            fact.supports_tool_use = Some(tool_support);
            let input = SelectModelRouteInput {
                target_agent: "mason".into(),
                requirements: ModelRequirements {
                    iq: 100.0,
                    eq: 100.0,
                },
                available_models: if available {
                    HashSet::from(["openai/gpt-5.5".into()])
                } else {
                    HashSet::new()
                },
                facts: vec![fact],
                runtime_snapshot: None,
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: if operator {
                    vec!["openai".into()]
                } else {
                    Vec::new()
                },
                explicit_only_providers: Vec::new(),
                required_capabilities: Some(RequiredCapabilities {
                    vision: None,
                    min_context: Some(context),
                }),
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
            };
            match (
                select(
                    "openai/gpt-5.5:high",
                    input,
                    &prefrontal_routing_core::DEFAULT_ROUTING_CONFIG,
                    None,
                ),
                expected,
            ) {
                (Ok(selected), None) => assert_eq!(selected.model.model_id, "gpt-5.5"),
                (Err(refusal), Some(code)) => assert_eq!(refusal.code, code, "{}", refusal.message),
                (Ok(_), Some(code)) => panic!("expected {code}, but the pin was admitted"),
                (Err(refusal), None) => panic!("{}: {}", refusal.code, refusal.message),
            }
        }
    }
}
