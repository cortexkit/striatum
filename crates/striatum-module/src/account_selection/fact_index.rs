//! Look up a catalog fact by provider and model without scanning the catalog.
//!
//! Account admission and retry diagnostics look up the route's fact many times
//! per request, once per catalog route on a refusal. A linear scan over a
//! catalog of hundreds of facts made each lookup cost as much as the work it
//! served, so the context indexes its facts once, on first use.

use super::*;

/// Fact positions by provider, then model. Only the first fact for a
/// provider and model is kept, matching the scan this index replaces.
pub(super) type FactIndex =
    std::collections::HashMap<String, std::collections::HashMap<String, usize>>;

impl ClassContext {
    /// The first fact for this provider and model, as
    /// `facts.iter().find(..)` would return it.
    pub(super) fn fact_for(
        &self,
        provider: &str,
        model: &str,
    ) -> Option<&striatum_core::ModelFact> {
        let index = self.indexes.facts.get_or_init(|| {
            let mut index = FactIndex::default();
            for (position, fact) in self.facts.iter().enumerate() {
                index
                    .entry(fact.provider_id.clone())
                    .or_default()
                    .entry(fact.model_id.clone())
                    .or_insert(position);
            }
            index
        });
        let position = *index.get(provider)?.get(model)?;
        self.facts.get(position)
    }
}
