//! Curated model facts: the live resolved catalog, ported from the Alfonso TS
//! plugin (`src/features/model-routing/model-facts-seed.ts`).
//!
//! The seed is DATA, not logic: the generated TS catalog is exported to
//! `model_facts_seed.json` and parsed here. `ModelFact`'s serde field names match
//! that generated shape (`providerID`, `modelID`, `family`, `costPerMTokOut`, ...),
//! so the JSON round-trips without remapping.
//!
//! Provenance (all real sources, no fabrication):
//!   - `elo`  : operator-resolved open capability Elo placement.
//!   - `eq`   : EQ-Bench 3 elo_norm (absolute) + curated values on the same scale.
//!   - `speed`: AA median output tokens/sec (max-effort variant).
//!   - context/cost/vision: models.dev catalog.

use std::sync::OnceLock;

use crate::model_facts::ModelFact;

const SEED_JSON: &str = include_str!("model_facts_seed.json");

/// The curated model-facts seed, parsed once from the embedded JSON.
pub fn model_facts_seed() -> &'static [ModelFact] {
    static SEED: OnceLock<Vec<ModelFact>> = OnceLock::new();
    SEED.get_or_init(|| {
        serde_json::from_str(SEED_JSON).expect("model_facts_seed.json must parse into ModelFact[]")
    })
}
