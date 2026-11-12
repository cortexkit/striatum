//! Routing score configuration for Elo capability facts and 0..100 caller demand.
//!
//! Capability facts use an open Elo scale. A request maps its 0..100 demand
//! against the strongest route it could receive before transient quota state is
//! considered, so exclusions and availability define the request's frontier but
//! quota pressure cannot silently rescale it.

use crate::model_facts::ModelScores;

/// Bump when persisted decision scoring semantics change.
pub const ANCHOR_VERSION: u32 = 6;

pub const DEFAULT_DEMAND_EXPONENT: f64 = 0.35;

/// Surplus (model iq minus demanded iq, both on the 0..100 demand scale) that
/// carries no over-qualification term. 40 keeps every request at demand 60 or
/// above untouched, because no route can sit more than 40 above it.
pub const DEFAULT_OVERQUALIFICATION_THRESHOLD: f64 = 40.0;
/// Surplus points beyond the threshold over which the term (plus `base`) doubles.
pub const DEFAULT_OVERQUALIFICATION_DOUBLING: f64 = 10.0;
/// Fractional cost surcharge: at threshold + doubling the surcharge is `base`.
pub const DEFAULT_OVERQUALIFICATION_BASE: f64 = 0.05;
/// Keep overqualification a soft preference: a four-times cheaper route can
/// still win at any surplus, before economic tie bands and the fit guard.
pub const MAX_OVERQUALIFICATION_FACTOR: f64 = 4.0;

/// Fractional selection-cost surcharge for routes far above the demand. Scaling
/// the economic cost keeps the preference meaningful as quota pressure rises;
/// the cap leaves sufficiently cheaper high-capability routes competitive.
///
/// `term = 0` when `surplus <= threshold`, else
/// `min(3, base * (2^((surplus - threshold) / doubling) - 1))`.
/// Selection multiplies cost plus quota by `1 + term`. The threshold and curve
/// keep their existing values; the surcharge is a fraction of selection cost.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OverqualificationConfig {
    pub threshold: f64,
    pub doubling: f64,
    pub base: f64,
}

pub const DEFAULT_OVERQUALIFICATION: OverqualificationConfig = OverqualificationConfig {
    threshold: DEFAULT_OVERQUALIFICATION_THRESHOLD,
    doubling: DEFAULT_OVERQUALIFICATION_DOUBLING,
    base: DEFAULT_OVERQUALIFICATION_BASE,
};

impl Default for OverqualificationConfig {
    fn default() -> Self {
        DEFAULT_OVERQUALIFICATION
    }
}

impl OverqualificationConfig {
    /// Whether every parameter is usable. `base == 0` is valid and switches the
    /// term off.
    pub fn is_valid(&self) -> bool {
        self.threshold.is_finite()
            && self.threshold >= 0.0
            && self.doubling.is_finite()
            && self.doubling > 0.0
            && self.base.is_finite()
            && self.base >= 0.0
    }

    /// The term for a route `surplus` demand points above the request. Zero for
    /// an invalid configuration, so a bad value can never inflate every price.
    pub fn term(&self, surplus: f64) -> f64 {
        if !self.is_valid() || self.base == 0.0 || !surplus.is_finite() || surplus <= self.threshold
        {
            return 0.0;
        }
        (self.base * ((2f64).powf((surplus - self.threshold) / self.doubling) - 1.0))
            .min(MAX_OVERQUALIFICATION_FACTOR - 1.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoutingConfig {
    pub demand_exponent: f64,
    pub overqualification: OverqualificationConfig,
    /// Whether automatic selection may use a route below the adequacy floor.
    pub allow_best_effort: bool,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        DEFAULT_ROUTING_CONFIG
    }
}

pub const DEFAULT_ROUTING_CONFIG: RoutingConfig = RoutingConfig {
    demand_exponent: DEFAULT_DEMAND_EXPONENT,
    overqualification: DEFAULT_OVERQUALIFICATION,
    allow_best_effort: true,
};

/// The default configuration with the over-qualification term switched off, for
/// tests of orderings the term is not about. Their fixtures put every route at
/// the request frontier and ask for a low demand, so every route carries the
/// same surcharge, which can lift them out of the effectively-free bucket and
/// bypass the bucket-only keys (abundance, adequacy) those tests exercise.
#[cfg(test)]
pub(crate) const ROUTING_CONFIG_WITHOUT_OVERQUALIFICATION: RoutingConfig = RoutingConfig {
    demand_exponent: DEFAULT_DEMAND_EXPONENT,
    allow_best_effort: true,
    overqualification: OverqualificationConfig {
        threshold: DEFAULT_OVERQUALIFICATION_THRESHOLD,
        doubling: DEFAULT_OVERQUALIFICATION_DOUBLING,
        base: 0.0,
    },
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

/// The inverse of [`demand_bar`]: the demand (0..100) whose bar equals `score`
/// against the same request frontier. A route at the frontier maps to 100.
pub fn demand_for_score(top_routable_score: f64, score: f64, demand_exponent: f64) -> f64 {
    if top_routable_score <= 0.0 || score <= 0.0 || demand_exponent <= 0.0 {
        return 0.0;
    }
    100.0
        * (score / top_routable_score)
            .clamp(0.0, 1.0)
            .powf(1.0 / demand_exponent)
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
    fn demand_for_score_inverts_demand_bar() {
        let top = 1_960.0;
        for demand in [10.0, 30.0, 50.0, 70.0, 100.0] {
            let bar = demand_bar(top, demand, DEFAULT_DEMAND_EXPONENT);
            assert!((demand_for_score(top, bar, DEFAULT_DEMAND_EXPONENT) - demand).abs() < 1e-9);
        }
        assert_eq!(demand_for_score(top, top, DEFAULT_DEMAND_EXPONENT), 100.0);
        assert!((demand_for_score(top, 1_884.0, DEFAULT_DEMAND_EXPONENT) - 89.3).abs() < 0.05);
        assert!((demand_for_score(top, 1_710.0, DEFAULT_DEMAND_EXPONENT) - 67.7).abs() < 0.05);
    }

    #[test]
    fn overqualification_term_is_zero_to_the_threshold_then_doubles_every_step() {
        let config = DEFAULT_OVERQUALIFICATION;
        assert_eq!(config.term(40.0), 0.0);
        assert!((config.term(50.0) - 0.05).abs() < 1e-12);
        assert!((config.term(60.0) - 0.15).abs() < 1e-12);
        assert!((config.term(70.0) - 0.35).abs() < 1e-12);
        let off = OverqualificationConfig {
            base: 0.0,
            ..config
        };
        assert_eq!(off.term(70.0), 0.0);
        let broken = OverqualificationConfig {
            doubling: 0.0,
            ..config
        };
        assert_eq!(broken.term(70.0), 0.0);
        let mut previous = 0.0;
        for surplus in 0..=200 {
            let term = config.term(surplus as f64);
            assert!(term >= previous && term <= 3.0);
            previous = term;
        }
        assert_eq!(config.term(f64::MAX), 3.0);
        assert_eq!(off.term(f64::MAX), 0.0);
    }

    #[test]
    fn zero_demand_has_zero_bar() {
        assert_eq!(demand_bar(1_838.0, 0.0, DEFAULT_DEMAND_EXPONENT), 0.0);
        assert_eq!(fit_ratio(15.0, 0.0), 1.0);
    }
}
