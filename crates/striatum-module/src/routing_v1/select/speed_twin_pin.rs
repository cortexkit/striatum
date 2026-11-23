use std::collections::BTreeMap;

use cortexkit_role_routing::{errors::Refusal, select::SelectRequest, service_tier::ServiceTier};
use striatum_core::model_facts::{ModelFact, SpeedTwinMode};

pub(super) fn resolve(
    request: &mut SelectRequest,
    slot_pin: &mut Option<String>,
    facts: &[ModelFact],
    catalog_targets: &BTreeMap<String, String>,
) -> Result<Option<ServiceTier>, Refusal> {
    let Some(pin) = request.pin.as_mut().or(slot_pin.as_mut()) else {
        return Ok(None);
    };
    let Some((provider, requested_model)) = pin.split_once('/') else {
        return Ok(None);
    };
    let catalog_provider =
        striatum_core::score_alias::alias_target(catalog_targets, provider).unwrap_or(provider);
    let (model, variant) =
        crate::work_model_pin::split_model_and_variant(catalog_provider, requested_model, facts);
    let Some(twin) = facts.iter().find_map(|fact| {
        (fact.provider_id == catalog_provider && fact.model_id == model)
            .then_some(fact.mode_of.as_ref())
            .flatten()
    }) else {
        return Ok(None);
    };
    let tier = match twin.mode {
        SpeedTwinMode::Fast => ServiceTier::Fast,
        SpeedTwinMode::Ultrafast => ServiceTier::Ultrafast,
    };
    if let Some(explicit) = request.service_tier.filter(|explicit| *explicit != tier) {
        return Err(Refusal::invalid_request(
            "service_tier",
            format!(
                "pin {pin:?} implies service_tier {:?}, but the request specifies {:?}",
                tier.as_str(),
                explicit.as_str()
            ),
        ));
    }
    let model_id = &twin.model_id;
    *pin = match variant {
        Some(variant) => format!("{provider}/{model_id}:{variant}"),
        None => format!("{provider}/{model_id}"),
    };
    request.service_tier = Some(tier);
    Ok(Some(tier))
}
