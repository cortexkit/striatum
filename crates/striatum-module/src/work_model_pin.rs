//! Exact worker pins share hard admission, but never the quality/cost competition.

use std::collections::{BTreeMap, HashMap};

use striatum_core::model_facts::{fact_to_route, ModelFact, ModelRequirements};
use striatum_core::model_router::{
    admit_routes_with_availability, select_model_route_with_availability, SelectModelRouteInput,
    SelectedModelRoute,
};
use striatum_core::provider_model_spec::ExclusionPolicy;
use striatum_core::{ConcurrencySlots, RoutingConfig};
use striatum_store::{PublishedEfforts, ReasoningCatalog};

use crate::variant_policy;

pub(crate) struct Refusal {
    pub code: String,
    pub message: String,
    /// The specific admission or scoring rejection behind a summary code.
    pub cause: Option<String>,
    /// When a probe this request lost to another request opens again; such a
    /// refusal is answered as a timed wait.
    pub retry_at: Option<i64>,
}

fn refuse(code: &str, message: String) -> Refusal {
    Refusal {
        code: code.into(),
        message,
        cause: None,
        retry_at: None,
    }
}

/// Split the model part of a pin into the catalog model id and the reasoning
/// tier the caller named, if any. A catalog id that itself contains a colon
/// (for example a model size) is kept whole; only an additional suffix is a tier.
fn split_model_and_variant<'a>(
    provider: &str,
    requested_model: &'a str,
    facts: &[ModelFact],
) -> (&'a str, Option<&'a str>) {
    if facts
        .iter()
        .any(|fact| fact.provider_id == provider && fact.model_id == requested_model)
    {
        (requested_model, None)
    } else {
        requested_model
            .rsplit_once(':')
            .map_or((requested_model, None), |(id, tier)| (id, Some(tier)))
    }
}

/// The reasoning tier a `provider/model:variant` pin names explicitly.
pub(crate) fn named_variant(pin: &str, facts: &[ModelFact]) -> Option<String> {
    let (provider, requested_model) = pin.split_once('/')?;
    split_model_and_variant(provider, requested_model, facts)
        .1
        .map(str::to_string)
}

pub(crate) fn route_keys(
    pin: &str,
    explicit: Option<&str>,
    facts: &[ModelFact],
) -> (String, String) {
    let Some((provider, requested_model)) = pin.split_once('/') else {
        return (pin.into(), pin.into());
    };
    let (model, variant) = split_model_and_variant(provider, requested_model, facts);
    let variant = explicit.or(variant);
    let variant = variant.or_else(|| {
        facts
            .iter()
            .filter(|fact| fact.provider_id == provider && fact.model_id == model)
            .min_by_key(|fact| usize::from(fact.variant.is_some()))
            .and_then(|fact| fact.variant.as_deref())
    });
    (
        striatum_core::make_route_key(provider, model, variant),
        format!("{provider}/{model}"),
    )
}

/// Select the exact route a caller named. The operator's `exclude` lists (the
/// `all` list and every consumer group) never apply here: they keep a route out
/// of automatic choice only.
///
/// A named variant is checked against the reasoning efforts the catalog
/// publishes for the model (Fusiform's `reasoning_options`, which Broca also
/// admits by), following a provider with no catalog rows to the catalog
/// provider whose rows it borrows (`catalog_targets`, resolved through the
/// vault's `serves`). A variant
/// outside a published list is refused. When the catalog has not been read for
/// the model yet, or no catalog is attached, the pin is admitted with a
/// warning and Broca decides.
#[cfg(test)]
pub(crate) fn select(
    pin: &str,
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    catalog: Option<&ReasoningCatalog>,
    catalog_targets: &BTreeMap<String, String>,
) -> Result<SelectedModelRoute, Refusal> {
    select_with_availability(
        pin,
        input,
        config,
        slots,
        catalog,
        catalog_targets,
        &HashMap::new(),
    )
}

pub(crate) fn select_with_availability(
    pin: &str,
    mut input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    catalog: Option<&ReasoningCatalog>,
    catalog_targets: &BTreeMap<String, String>,
    availability_rejections: &HashMap<String, String>,
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
    let (model, variant) = split_model_and_variant(provider, requested_model, &input.facts);
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
    let variant_warning =
        validate_variant(pin, provider, model, variant, catalog, catalog_targets)?;
    let mut fact = facts
        .iter()
        .find(|fact| fact.variant.as_deref() == variant)
        .or_else(|| facts.iter().find(|fact| fact.variant.is_none()))
        .unwrap_or(&facts[0])
        .clone();
    let route = fact_to_route(&fact);
    if fact.decommissioned {
        return Err(refuse(
            "decommissioned",
            format!("{pin}: model is decommissioned"),
        ));
    }
    if let Some(reason) = availability_rejections
        .get(&route.route_key)
        .or_else(|| availability_rejections.get(&route.capacity_key))
        .filter(|reason| {
            reason.starts_with("not_chat_model:") || reason.starts_with("not_chat_kind:")
        })
    {
        return Err(refuse(reason, format!("{identity}: {reason}")));
    }
    // A withdrawn model fails at the provider whatever else is true of it, so
    // say so first rather than reporting a capability gap.
    if fact.is_deprecated_upstream() {
        return Err(refuse(
            striatum_core::REASON_DEPRECATED_UPSTREAM,
            format!(
                "{pin}: {} is marked deprecated upstream; the provider no longer serves it",
                fact_to_route(&fact).route_key
            ),
        ));
    }
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
    // The caller named this exact route (a work run's `model=`). A caller that
    // forwarded the operator's exclude list must not turn the pin into a
    // refusal. Per-attempt caller exclusions, availability, credentials,
    // capability and quota walls still apply.
    input.operator_exclude_keys.clear();
    let admission = admit_routes_with_availability(
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
        let mut refusal = refuse(
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
        );
        refusal.cause = Some(rejection.reason.clone());
        return Err(refusal);
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
    let mut selected =
        select_model_route_with_availability(input, config, slots, &[], availability_rejections)
            .map_err(|error| {
                // The scorer can reject an admitted pin for subscription reserve.
                // Its summary code alone cannot tell the caller what must recover.
                let message = error.rejected.first().map_or_else(
                    || error.message.clone(),
                    |rejected| {
                        format!(
                            "{pin}: {}{}",
                            rejected.reason,
                            rejected
                                .detail
                                .as_ref()
                                .map(|detail| format!(" ({detail})"))
                                .unwrap_or_default()
                        )
                    },
                );
                let mut refusal = refuse(error.reason.as_str(), message);
                refusal.cause = error
                    .rejected
                    .first()
                    .map(|rejected| rejected.reason.clone());
                refusal
            })?;
    selected.score.adequacy = adequacy.adequacy;
    selected.score.raw_adequacy = adequacy.raw_adequacy;
    selected.score.metrics = adequacy.metrics;
    selected.below_floor = None;
    selected.explanation = vec![format!("model pinned by head: {pin}; hard admission only")];
    selected.explanation.extend(variant_warning);
    Ok(selected)
}

/// Validate an explicit effort without silently substituting a policy default.
pub(crate) fn validate_variant(
    pin: &str,
    provider: &str,
    model: &str,
    variant: Option<&str>,
    catalog: Option<&ReasoningCatalog>,
    catalog_targets: &BTreeMap<String, String>,
) -> Result<Option<String>, Refusal> {
    let Some(variant) = variant else {
        return Ok(None);
    };
    let identity = format!("{provider}/{model}");
    match catalog
        .and_then(|catalog| variant_policy::published(catalog, catalog_targets, provider, model))
    {
        Some(PublishedEfforts::Published(values)) => {
            if !values.iter().any(|value| value == variant) {
                return Err(refuse(
                    "invalid_model_variant",
                    format!(
                        "unknown variant {variant:?} for {identity}; valid variants: {}",
                        variant_policy::render(values)
                    ),
                ));
            }
            Ok(None)
        }
        unknown => {
            let why = match (catalog, unknown) {
                (None, _) => "no reasoning catalog is attached yet",
                (Some(_), Some(PublishedEfforts::Unread)) => {
                    "the catalog has not been read for this model yet"
                }
                (Some(_), _) => "the catalog has no row for this model",
            };
            let warning = format!("variant {variant} for {identity} not checked: {why}; the runner decides whether it is admitted");
            eprintln!("[striatum] WARN: {pin}: {warning}");
            Ok(Some(warning))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use striatum_core::model_router::RequiredCapabilities;

    /// The operator's exclude list is an automatic-selection fence only, so an
    /// excluded route named by the head is admitted; hard admission (context,
    /// tool support, availability) still refuses.
    #[test]
    fn pinned_model_checks_context_and_tools_but_admits_an_operator_excluded_route() {
        for (tool_support, context, operator, available, expected) in [
            (true, 8192.0, false, true, None),
            (true, 300_000.0, false, true, Some("context_too_small")),
            (false, 8192.0, false, true, Some("missing_tool_support")),
            (true, 8192.0, true, true, None),
            (true, 8192.0, false, false, Some("not_available")),
        ] {
            let mut fact = crate::tests::fact("openai", "gpt-5.5");
            // Even a model with weak judgment is the head's explicit choice.
            fact.scores.eq = 100.0;
            fact.supports_tool_use = Some(tool_support);
            let input = SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
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
                required_capabilities: Some(RequiredCapabilities {
                    vision: None,
                    min_context: Some(context),
                }),
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
                providers_without_cache_write_reports: HashSet::new(),
            };
            match (
                select(
                    "openai/gpt-5.5:high",
                    input,
                    &striatum_core::DEFAULT_ROUTING_CONFIG,
                    None,
                    None,
                    &BTreeMap::new(),
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

    fn pin_input(provider: &str, model: &str) -> SelectModelRouteInput<'static> {
        SelectModelRouteInput {
            target_agent: "mason".into(),
            target_is_head: false,
            requirements: ModelRequirements { iq: 0.0, eq: 0.0 },
            available_models: HashSet::from([format!("{provider}/{model}")]),
            facts: vec![crate::tests::fact(provider, model)],
            runtime_snapshot: None,
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: None,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: HashSet::new(),
            providers_without_cache_write_reports: HashSet::new(),
        }
    }

    #[test]
    fn unavailable_pin_preserves_its_admission_cause() {
        let mut input = pin_input("openai", "gpt-5.5");
        input.available_models.clear();
        let refusal = select(
            "openai/gpt-5.5",
            input,
            &striatum_core::DEFAULT_ROUTING_CONFIG,
            None,
            None,
            &BTreeMap::new(),
        )
        .err()
        .expect("an unavailable pin must be refused");
        assert_eq!(refusal.code, "not_available");
        assert_eq!(refusal.cause.as_deref(), Some("not_available"));
        assert_eq!(refusal.message, "openai/gpt-5.5: not_available");
    }

    fn catalog(entries: &[(&str, Option<&[&str]>)]) -> ReasoningCatalog {
        ReasoningCatalog {
            models: entries
                .iter()
                .map(|(key, values)| {
                    (
                        (*key).to_string(),
                        match values {
                            None => PublishedEfforts::Unread,
                            Some(values) => PublishedEfforts::Published(
                                values.iter().map(|value| (*value).to_string()).collect(),
                            ),
                        },
                    )
                })
                .collect(),
        }
    }

    /// A pinned variant is judged by the reasoning efforts the catalog
    /// publishes, the same list Broca admits by: `xhigh` on an Opus route is
    /// admitted when published, a variant outside the list is refused and the
    /// refusal names the list, a provider resolved to another provider's
    /// catalog rows reads that provider's efforts, and a model the catalog has
    /// not been read for is admitted with a warning.
    #[test]
    fn pinned_variants_are_checked_against_the_published_reasoning_efforts() {
        let published = catalog(&[
            (
                "anthropic/claude-opus-5-5",
                Some(&["low", "medium", "high", "xhigh", "max"]),
            ),
            ("openai/gpt-6.1-sol", Some(&["low", "medium", "high"])),
            ("google/gemini-4-argon", None),
        ]);
        let aliases = BTreeMap::from([("anthropic-platform".to_string(), "anthropic".to_string())]);
        let run = |pin: &str, catalog: Option<&ReasoningCatalog>| {
            let (provider, rest) = pin.split_once('/').unwrap();
            let model = rest.split_once(':').map_or(rest, |(model, _)| model);
            select(
                pin,
                pin_input(provider, model),
                &striatum_core::DEFAULT_ROUTING_CONFIG,
                None,
                catalog,
                &aliases,
            )
        };
        for pin in [
            "anthropic/claude-opus-5-5:xhigh",
            "anthropic-platform/claude-opus-5-5:xhigh",
            "openai/gpt-6.1-sol:low",
        ] {
            let selected = run(pin, Some(&published))
                .unwrap_or_else(|refusal| panic!("{pin}: {}: {}", refusal.code, refusal.message));
            assert_eq!(
                selected.explanation.len(),
                1,
                "{pin}: {:?}",
                selected.explanation
            );
        }
        for (pin, message) in [
            (
                "openai/gpt-6.1-sol:xhigh",
                "unknown variant \"xhigh\" for openai/gpt-6.1-sol; valid variants: low, medium, high",
            ),
            (
                "anthropic-platform/claude-opus-5-5:minimal",
                "valid variants: low, medium, high, xhigh, max",
            ),
        ] {
            let refusal = run(pin, Some(&published)).err().unwrap_or_else(|| panic!("{pin} was admitted"));
            assert_eq!(refusal.code, "invalid_model_variant");
            assert!(refusal.message.contains(message), "{pin}: {}", refusal.message);
        }
        for (pin, catalog, why) in [
            (
                "google/gemini-4-argon:ultra",
                Some(&published),
                "has not been read for this model yet",
            ),
            (
                "openai/gpt-6.1-sol:xhigh",
                None,
                "no reasoning catalog is attached yet",
            ),
        ] {
            let selected = run(pin, catalog)
                .unwrap_or_else(|refusal| panic!("{pin}: {}: {}", refusal.code, refusal.message));
            assert!(
                selected
                    .explanation
                    .iter()
                    .any(|line| line.contains("not checked") && line.contains(why)),
                "{pin}: {:?}",
                selected.explanation
            );
        }
    }
}
