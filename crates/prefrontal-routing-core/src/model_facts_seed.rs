//! Curated model facts kept unchanged as seed data for bootstrapping and testing migrations.
//!
//! The seed is DATA, not logic: `model_facts_seed.json` preserves the historical
//! catalog used by migration fixtures and is parsed here. Live catalog reads belong
//! to the routing store. `ModelFact`'s serde field names match the stored wire
//! shape (`providerID`, `modelID`, `family`, `costPerMTokOut`, ...), so the JSON
//! round-trips without remapping.
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
