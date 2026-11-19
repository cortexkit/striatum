use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use striatum_core::RouteStateResult;

use crate::SqliteRouteStateStore;

/// The factual fields Fusiform may author for a provider/model pair.
#[derive(Debug, Clone, PartialEq)]
pub struct FusiformFactUpdate {
    pub provider_id: String,
    pub model_id: String,
    pub auth_methods: Option<Vec<String>>,
    pub login_methods_documented: bool,
    pub present: bool,
    pub context_window: Option<f64>,
    pub max_output_tokens: Option<u64>,
    pub cost_tier: Option<f64>,
    pub cost_per_mtok_out: Option<f64>,
    pub input_rate: Option<f64>,
    pub cache_read_rate: Option<f64>,
    pub cache_write_rate: Option<f64>,
    pub reasoning_rate: Option<f64>,
    pub supports_vision: Option<bool>,
    pub supports_tool_use: Option<bool>,
    /// Effort values from the model's `capability.reasoning_options`: an empty
    /// list when the catalog serves the key with no effort entry (or `null`).
    /// `None` only when the catalog did not serve the key at all, which leaves
    /// the row unread for reasoning purposes.
    pub reasoning_efforts: Option<Vec<String>>,
    pub catalog_efforts: Option<CatalogEfforts>,
    /// Fusiform's `model.canonical_id`, verbatim. `None` when the catalog served
    /// `null` or no value: the upstream states no identity for this route.
    pub canonical_id: Option<String>,
    /// Fusiform's `model.status` verbatim (`deprecated`, `beta`, `alpha`), or
    /// `None` when the catalog states no status.
    pub upstream_status: Option<String>,
    /// Fusiform's `model.lineage`, without rewriting or inferring a value.
    pub lineage: Option<String>,
    pub model_type: Option<String>,
    pub model_kind: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CatalogEfforts {
    /// Missing or null options do not establish an effort list. An explicit
    /// options array can establish that the model offers no named effort.
    pub offered: Option<Vec<String>>,
    pub graded: Option<String>,
    pub vendor_default: Option<String>,
    pub elo: BTreeMap<String, f64>,
    pub eq: BTreeMap<String, f64>,
    pub workloads: BTreeMap<String, CatalogWorkload>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CatalogWorkload {
    pub default: Option<String>,
    pub minimum: Option<String>,
    pub excluded: bool,
}

impl SqliteRouteStateStore {
    pub fn catalog_efforts(&self) -> RouteStateResult<BTreeMap<String, CatalogEfforts>> {
        Ok((*self.get_selection_catalog()?.efforts).clone())
    }
}

pub(super) fn decode(index: usize, value: &str) -> rusqlite::Result<CatalogEfforts> {
    serde_json::from_str(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}
