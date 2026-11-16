//! Attribute usage and speed observations to variants in the stored catalog.

use striatum_core::usage_facts::build_usage_observation;
use striatum_core::{NativeUsageFacts, VariantFact};

use crate::SharedStore;

pub(super) fn usage_observation_from_store(
    params: NativeUsageFacts,
    store: &SharedStore,
) -> Result<Option<striatum_core::UsageObservation>, String> {
    Ok(build_usage_observation(
        &params,
        &usage_variants_from_store(store)?,
    ))
}

pub(super) fn speed_sample_observation_from_store(
    params: &NativeUsageFacts,
    store: &SharedStore,
) -> Result<Option<striatum_core::usage_facts::SpeedSampleObservation>, String> {
    Ok(striatum_core::usage_facts::build_speed_sample_observation(
        params,
        &usage_variants_from_store(store)?,
    ))
}

fn usage_variants_from_store(store: &SharedStore) -> Result<Vec<VariantFact>, String> {
    let catalog = store
        .get_catalog_facts()
        .map_err(|error| format!("record_usage catalog: {error}"))?;
    let variants = catalog
        .into_iter()
        .map(|entry| VariantFact {
            provider_id: entry.fact.provider_id,
            model_id: entry.fact.model_id,
            variant: entry
                .fact
                .prompt_defaults
                .and_then(|defaults| defaults.variant)
                .or(entry.fact.variant),
        })
        .collect::<Vec<_>>();
    Ok(variants)
}
