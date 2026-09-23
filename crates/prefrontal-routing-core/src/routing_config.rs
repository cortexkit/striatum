//! Routing score configuration for Elo capability facts and 0..100 caller demand.
//!
//! Capability facts use an open Elo scale. A request maps its 0..100 demand
//! against the strongest route it could receive before transient quota state is
//! considered, so exclusions and availability define the request's frontier but
//! quota pressure cannot silently rescale it.

use crate::model_facts::ModelScores;

/// Bump when persisted decision scoring semantics change.
pub const ANCHOR_VERSION: u32 = 5;

pub const DEFAULT_DEMAND_EXPONENT: f64 = 0.35;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoutingConfig {
    pub demand_exponent: f64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            demand_exponent: DEFAULT_DEMAND_EXPONENT,
        }
    }
}

pub const DEFAULT_ROUTING_CONFIG: RoutingConfig = RoutingConfig {
    demand_exponent: DEFAULT_DEMAND_EXPONENT,
};

/// The strongest quality scores among routes that pass request-specific static
/// admission. Transient quota and cooldown state is deliberately excluded.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TopRoutableScores {
    pub elo: f64,
    pub eq: f64,
}

impl TopRoutableScores {
    pub fn include(&mut self, scores: ModelScores) {
        self.elo = self.elo.max(scores.elo);
        self.eq = self.eq.max(scores.eq);
    }
}

/// Convert caller demand into an absolute bar on an open score scale.
pub fn demand_bar(top_routable_score: f64, demand: f64, demand_exponent: f64) -> f64 {
    if demand <= 0.0 || top_routable_score <= 0.0 {
        return 0.0;
    }
    top_routable_score * (demand.clamp(0.0, 100.0) / 100.0).powf(demand_exponent)
}

/// A score-to-bar ratio capped at one. A zero bar means the axis is ignored.
pub fn fit_ratio(score: f64, bar: f64) -> f64 {
    if bar <= 0.0 {
        1.0
    } else {
        (score / bar).clamp(0.0, 1.0)
    }
}

/// Conservative defaults for models with no curated fact. Eligible but penalized.
pub const UNKNOWN_MODEL_SCORES: ModelScores = ModelScores {
    elo: 15.0,
    eq: 1_000.0,
    speed: 90.0,
};

pub const UNKNOWN_FACT_CONFIDENCE_PENALTY: f64 = 0.1;

/// A model is in the near-bar adequacy band at this fit ratio. Exact bar
/// clearance is tracked separately; this ratio remains the warning boundary.
/// Operator ruling 2026-09-23: 0.95, raised from 0.9. At 0.9 a model 153 elo
/// short of the iq-80 bar (deepseek-v4.1-flash, fit 0.92) counted as adequate
/// and took iq-80 masons whenever stronger subscription capacity was walled;
/// one such worker finished its edits and then looped for minutes on a long
/// verification step. At 0.95 a shortage falls through to best effort, which
/// ranks the most capable admissible model first.
pub const ADEQUACY_FLOOR: f64 = 0.95;
/// Best effort below this fit ratio is refused instead of silently dispatching a
/// dangerously weak route.
pub const MIN_BEST_EFFORT_ADEQUACY: f64 = 0.6;
pub const RELATIVE_SHORTLIST_DELTA: f64 = 0.12;

/// Normalized selection-cost at or below which routes compare as equally free.
pub const EFFECTIVELY_FREE_SELECTION_COST: f64 = 0.05;
/// Objective weight on soft quota pressure.
pub const OBJECTIVE_WEIGHT_QUOTA: f64 = 0.5;
/// $/Mtok output treated as fully expensive by normalized metered cost.
pub const COST_REFERENCE_PER_MTOK: f64 = 30.0;
/// Reference cost per message in USD used to normalize the token basket cost.
pub const BASKET_REFERENCE_COST_PER_MESSAGE: f64 = 0.05;

/// Fixed speed range used only for deterministic speed tie-breaking.
pub const SPEED_FLOOR_TPS: f64 = 50.0;
pub const SPEED_CEILING_TPS: f64 = 200.0;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elo_demand_bar_calibration_exponent_0_35() {
        assert_eq!(DEFAULT_DEMAND_EXPONENT, 0.35);
        let top = 1_838.0;
        let entry_demand = |elo: f64| 100.0 * (elo / top).powf(1.0 / DEFAULT_DEMAND_EXPONENT);

        assert!((demand_bar(top, 100.0, DEFAULT_DEMAND_EXPONENT) - top).abs() < 1e-9);
        assert!(
            (entry_demand(1_621.0) - 69.84).abs() < 0.05,
            "luna calibration moved"
        );
        assert!(
            (entry_demand(1_721.0) - 82.87).abs() < 0.05,
            "qwen calibration moved"
        );
        assert!(
            (entry_demand(1_685.0) - 78.01).abs() < 0.05,
            "terra calibration moved"
        );
        assert!(
            (entry_demand(1_410.0) - 46.89).abs() < 0.05,
            "gemini-flash calibration moved"
        );
    }

    #[test]
    fn zero_demand_has_zero_bar() {
        assert_eq!(demand_bar(1_838.0, 0.0, DEFAULT_DEMAND_EXPONENT), 0.0);
        assert_eq!(fit_ratio(15.0, 0.0), 1.0);
    }
}
