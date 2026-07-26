//! Anchored normalization for absolute model scores.
//!
//! Ported from the Alfonso TS plugin (`src/features/model-routing/route-anchors.ts`).
//!
//! Anchors are FIXED and curated centrally, never derived from the live min/max
//! of the fact table. This keeps routing stable when a new model is added: a
//! future iq:75 model does not shift every other model's fit score.
//!
//! Anchor calibration is grounded in real measured distributions:
//!   - `iq`   : Artificial Analysis intelligence index v4.1, frontier ~56..60.
//!   - `eq`   : EQ-Bench 3 elo_norm, observed ~1430..2115 across the live pool.
//!   - `speed`: AA median output tokens/sec, observed ~35..222 for the live pool.
//!
//! `weak`  ≈ a requirement of 0..20 targets here.
//! `strong`≈ a requirement of ~60 targets here.
//! `elite` ≈ a requirement of 100 targets here.

use crate::model_facts::{ModelMetric, ModelScores};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricAnchor {
    pub weak: f64,
    pub strong: f64,
    pub elite: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Anchors {
    pub iq: MetricAnchor,
    pub eq: MetricAnchor,
    pub speed: MetricAnchor,
}

impl Anchors {
    pub fn get(&self, metric: ModelMetric) -> MetricAnchor {
        match metric {
            ModelMetric::Iq => self.iq,
            ModelMetric::Eq => self.eq,
            ModelMetric::Speed => self.speed,
        }
    }
}

/// Bump when anchors/spread change so persisted decisions stay interpretable.
pub const ANCHOR_VERSION: u32 = 4;

pub const DEFAULT_ANCHORS: Anchors = Anchors {
    // AA intelligence index (v4.1 scale: frontier compresses to ~56-60).
    iq: MetricAnchor {
        weak: 30.0,
        strong: 44.0,
        elite: 57.0,
    },
    // EQ-Bench 3 elo_norm.
    eq: MetricAnchor {
        weak: 1000.0,
        strong: 1500.0,
        elite: 2000.0,
    },
    // AA median tok/s (live pool ceiling).
    speed: MetricAnchor {
        weak: 50.0,
        strong: 120.0,
        elite: 200.0,
    },
};

/// Conservative defaults for models with no curated fact. Eligible but penalized.
pub const UNKNOWN_MODEL_SCORES: ModelScores = ModelScores {
    iq: 30.0,
    eq: 1000.0,
    speed: 90.0,
};

pub const UNKNOWN_FACT_CONFIDENCE_PENALTY: f64 = 0.1;

// ── Selection model: quality THRESHOLD, then COST-PRIMARY ordering ───────────
// iq/eq are adequacy gates ("meet the bar"); exceeding the target yields no
// bonus (saturating fit). Among models that clear the bar the router picks
// strictly COST-FIRST: lowest economic selectionCost (effective metered cost +
// quota pressure) wins, and speed only breaks cost ties (fastest among equal-cost).
// A faster model can NEVER outrank a cheaper one, so a trivial task falls to
// the cheapest adequate model, and speed differentiates equal-cost models (e.g.
// siblings that share a list price).

/// Metrics treated as quality gates (thresholds). Speed is a tie-break, not a gate.
pub const QUALITY_METRICS: [ModelMetric; 2] = [ModelMetric::Iq, ModelMetric::Eq];

/// A model "clears the bar" when its weighted saturating adequacy ≥ this floor.
/// Cost optimizes ONLY within cleared models: it can never pull a sub-bar model
/// in. Graceful fallback to the relative top when nothing clears.
pub const ADEQUACY_FLOOR: f64 = 0.9;
/// Minimum adequacy for best-effort selection. When no route clears the adequacy
/// floor, selection is only permitted within [band, floor); below the band,
/// `route.select` fails with `AllRoutesInadequate` instead of returning a
/// dangerously weak route. Default 0.6, configurable via
/// `model_routing.min_best_effort_adequacy`.
pub const MIN_BEST_EFFORT_ADEQUACY: f64 = 0.6;
pub const RELATIVE_SHORTLIST_DELTA: f64 = 0.12;

/// Normalized selection-cost at or below which a route is "effectively free":
/// subsidized routes in this bucket compare as EQUAL cost and capability decides
/// between them (then speed). 0.05 of the $30/Mtok cost reference is $1.50/Mtok
/// effective: at that price the dollar difference between two routes is noise
/// against what the tokens buy, and letting it order selections produced the
/// inversion this constant exists to prevent: a 40x-subsidized luna ($0.15
/// effective, iq 53) beating sol (iq 58, $0.75 effective) on iq-95 work, where
/// the caller asked for the strongest model and the router saved 60 imaginary
/// cents. Metered routes above the bucket keep exact cost-primary ordering:
/// real dollar differences still decide.
pub const EFFECTIVELY_FREE_SELECTION_COST: f64 = 0.05;

/// Objective weight on soft quota pressure (selectionCost = costNorm + quota·Wq).
/// Speed is NOT here: it is a strict tie-break after selectionCost.
pub const OBJECTIVE_WEIGHT_QUOTA: f64 = 0.5;

/// $/Mtok output treated as fully "expensive" (costNorm → 1.0); linear below,
/// clamped at 1.0 above. Continuous cost is required so the cost ladder resolves.
pub const COST_REFERENCE_PER_MTOK: f64 = 30.0;

/// Linear interpolation.
pub fn lerp(a: f64, b: f64, t: f64) -> f64 {
    a + (b - a) * t
}

/// Logistic sigmoid.
pub fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Fraction of the strong→elite climb reached at demand TOP_KNEE_DEMAND. The
/// knee front-loads the top segment so the mid tier is priced out by the high
/// 80s instead of only at the extreme end: with the AA v4.1 iq anchors, demand
/// 85 lands one point above the mid-band facts while 100 still sits exactly on
/// elite (a bar past elite would exceed every live fact and inadequate all).
pub const TOP_KNEE_DEMAND: f64 = 85.0;
pub const TOP_KNEE_FRACTION: f64 = 0.85;

/// Convert a 0..100 requirement into a target absolute score on the metric's
/// anchor scale. 0..60 interpolates weak→strong; 60..100 interpolates
/// strong→elite with a knee at (TOP_KNEE_DEMAND, TOP_KNEE_FRACTION), so most
/// of the top climb happens by the mid-80s.
pub fn requirement_target(anchor: MetricAnchor, requirement: f64) -> f64 {
    let r = requirement.clamp(0.0, 100.0);
    if r <= 60.0 {
        return lerp(anchor.weak, anchor.strong, r / 60.0);
    }
    let fraction = if r <= TOP_KNEE_DEMAND {
        TOP_KNEE_FRACTION * (r - 60.0) / (TOP_KNEE_DEMAND - 60.0)
    } else {
        TOP_KNEE_FRACTION
            + (1.0 - TOP_KNEE_FRACTION) * (r - TOP_KNEE_DEMAND) / (100.0 - TOP_KNEE_DEMAND)
    };
    lerp(anchor.strong, anchor.elite, fraction)
}

/// Fixed per-metric spread for the sigmoid. Not a live distribution spread.
pub fn metric_spread(anchor: MetricAnchor) -> f64 {
    (anchor.elite - anchor.weak) / 4.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aa_v41_iq_anchors_and_version() {
        assert_eq!(ANCHOR_VERSION, 4);
        assert_eq!(DEFAULT_ANCHORS.iq.weak, 30.0);
        assert_eq!(DEFAULT_ANCHORS.iq.strong, 44.0);
        assert_eq!(DEFAULT_ANCHORS.iq.elite, 57.0);
        // iq need 30/100 → bar = weak + (strong-weak)*(30/60) = 37
        assert!((requirement_target(DEFAULT_ANCHORS.iq, 30.0) - 37.0).abs() < 1e-9);
        // iq need 60/100 sits on strong; need 100/100 sits on elite.
        assert!((requirement_target(DEFAULT_ANCHORS.iq, 60.0) - 44.0).abs() < 1e-9);
        assert!((requirement_target(DEFAULT_ANCHORS.iq, 100.0) - 57.0).abs() < 1e-9);
        assert_eq!(UNKNOWN_MODEL_SCORES.iq, DEFAULT_ANCHORS.iq.weak);
    }

    #[test]
    fn top_knee_prices_out_mid_band_by_85() {
        // Knee lands demand 85 at strong + 0.85*(elite-strong) = 55.05 for iq:
        // above the 53-class mid band, below the 57 elite anchor.
        let bar_85 = requirement_target(DEFAULT_ANCHORS.iq, 85.0);
        assert!((bar_85 - 55.05).abs() < 1e-9);
        // The knee is continuous: just below/above 85 stays monotonic.
        let bar_84 = requirement_target(DEFAULT_ANCHORS.iq, 84.0);
        let bar_86 = requirement_target(DEFAULT_ANCHORS.iq, 86.0);
        assert!(bar_84 < bar_85 && bar_85 < bar_86);
        // 70 (standard slice tier) stays in the mid band.
        let bar_70 = requirement_target(DEFAULT_ANCHORS.iq, 70.0);
        assert!(bar_70 < 49.0, "bar_70 = {bar_70}");
    }
}
