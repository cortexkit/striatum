//! Route aliases and presentation for catalog-published pin options.
use std::collections::BTreeMap;

use striatum_store::{PublishedEfforts, ReasoningCatalog};

/// The catalog entry for a model, following a score alias to the provider
/// whose rows it borrows (an alias has no catalog rows of its own).
pub(crate) fn published<'a>(
    catalog: &'a ReasoningCatalog,
    aliases: &BTreeMap<String, String>,
    provider_id: &str,
    model_id: &str,
) -> Option<&'a PublishedEfforts> {
    catalog.get(provider_id, model_id).or_else(|| {
        aliases
            .get(provider_id)
            .and_then(|target| catalog.get(target, model_id))
    })
}

pub(crate) fn render(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}
