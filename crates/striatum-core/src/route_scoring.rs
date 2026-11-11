//! Quality adequacy scoring for open Elo facts.
//!
//! Caller demand remains 0..100. Each quality axis is mapped to an absolute bar
//! against the strongest statically routable fact for the request, then scored as
//! a capped score-to-bar ratio. Exceeding a bar gives no bonus.

use std::collections::BTreeMap;

use crate::model_facts::{Confidence, ModelFact, ModelMetric, ModelRequirements};
use crate::routing_config::{
    demand_bar, fit_ratio, RoutingConfig, TopRoutableScores, ADEQUACY_FLOOR,
    UNKNOWN_FACT_CONFIDENCE_PENALTY,
};

const QUALITY_METRICS: [ModelMetric; 2] = [ModelMetric::Elo, ModelMetric::Eq];

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct MetricFit {
    pub requirement: f64,
    pub fact: f64,
    pub target: f64,
    pub fit: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdequacyScore {
    /// Weighted capped fit over quality metrics, after unknown-fact penalty.
    pub adequacy: f64,
    /// Raw adequacy before the unknown-fact penalty.
    pub raw_adequacy: f64,
    /// Per-quality-metric audit; excludes metrics with requirement 0.
    pub metrics: BTreeMap<ModelMetric, MetricFit>,
    /// True when no quality metric had a non-zero requirement.
    pub capability_only: bool,
    pub explanation: Vec<String>,
}

impl AdequacyScore {
    /// Whether the route clears the normal adequacy gate. Fit remains a ratio,
    /// so a route may clear while slightly below one component's exact bar, but
    /// every demanded metric must clear the floor on its own: the blended
    /// adequacy is a requirement-weighted mean, and a mean lets an easy eq
    /// demand lift a route whose elo fit is below the floor (on 2026-09-10 a
    /// 1710-elo model cleared an iq-100 bar of 1902 at blended 0.92 because its
    /// eq fit was 1.0, and took a hunter the demand had priced for the top model).
    pub fn clears_adequacy_floor(&self) -> bool {
        self.capability_only
            || (self.adequacy >= ADEQUACY_FLOOR
                && self.metrics.values().all(|fit| fit.fit >= ADEQUACY_FLOOR))
    }
}

/// Score how well a fact clears this request's quality bars.
pub fn score_adequacy(
    fact: &ModelFact,
    requirements: &ModelRequirements,
    top_routable: TopRoutableScores,
    config: &RoutingConfig,
) -> AdequacyScore {
    let mut metrics: BTreeMap<ModelMetric, MetricFit> = BTreeMap::new();
    let mut explanation: Vec<String> = Vec::new();
    let mut numerator = 0.0;
    let mut denominator = 0.0;

    for metric in QUALITY_METRICS {
        let requirement = requirements.get(metric);
        if requirement == 0.0 {
            continue;
        }
        let fact_score = fact.scores.get(metric);
        let top_score = match metric {
            ModelMetric::Elo => top_routable.elo,
            ModelMetric::Eq => top_routable.eq,
            ModelMetric::Speed => unreachable!("speed is not a quality gate"),
        };
        let target = demand_bar(top_score, requirement, config.demand_exponent);
        let fit = fit_ratio(fact_score, target);
        numerator += fit * requirement;
        denominator += requirement;
        metrics.insert(
            metric,
            MetricFit {
                requirement,
                fact: fact_score,
                target,
                fit,
            },
        );
        let tail = if fit >= 1.0 {
            "clears".to_string()
        } else {
            format!("fit {:.2}", fit)
        };
        explanation.push(format!(
            "{}: need {}/100 (bar {:.0}), have {:.0} → {}",
            metric.as_str(),
            fmt_num(requirement),
            target,
            fact_score,
            tail,
        ));
    }

    if denominator == 0.0 {
        explanation.push(
            "capability-only: no quality bar (differentiated by cost/speed objective)".to_string(),
        );
        return AdequacyScore {
            adequacy: 1.0,
            raw_adequacy: 1.0,
            metrics,
            capability_only: true,
            explanation,
        };
    }

    let raw_adequacy = numerator / denominator;
    let mut adequacy = raw_adequacy;
    if fact.confidence == Confidence::Unknown {
        adequacy = (adequacy - UNKNOWN_FACT_CONFIDENCE_PENALTY).max(0.0);
        explanation.push(format!(
            "unknown-fact penalty −{:.2}",
            UNKNOWN_FACT_CONFIDENCE_PENALTY
        ));
    }

    AdequacyScore {
        adequacy,
        raw_adequacy,
        metrics,
        capability_only: false,
        explanation,
    }
}

fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

#[cfg(test)]
mod floor_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn score(fits: &[(ModelMetric, f64, f64)]) -> AdequacyScore {
        let mut metrics = BTreeMap::new();
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        for (metric, requirement, fit) in fits {
            numerator += fit * requirement;
            denominator += requirement;
            metrics.insert(
                *metric,
                MetricFit {
                    requirement: *requirement,
                    fact: 0.0,
                    target: 0.0,
                    fit: *fit,
                },
            );
        }
        AdequacyScore {
            adequacy: numerator / denominator,
            raw_adequacy: numerator / denominator,
            metrics,
            capability_only: false,
            explanation: Vec::new(),
        }
    }

    /// One metric just under the floor, the other comfortably above it, blend
    /// to an adequacy above the floor; the shortfall on the one metric alone
    /// must refuse the route. Fits are written relative to the floor so the
    /// specimen stays on the boundary it tests if the floor moves again.
    #[test]
    fn a_metric_below_the_floor_is_not_papered_over_by_the_blend() {
        let specimen = score(&[
            (ModelMetric::Elo, 100.0, ADEQUACY_FLOOR - 0.01),
            (ModelMetric::Eq, 30.0, 1.0),
        ]);
        assert!(
            specimen.adequacy > ADEQUACY_FLOOR,
            "the blend clears on its own"
        );
        assert!(!specimen.clears_adequacy_floor());

        let both_clear = score(&[
            (ModelMetric::Elo, 100.0, ADEQUACY_FLOOR),
            (ModelMetric::Eq, 30.0, 1.0),
        ]);
        assert!(both_clear.clears_adequacy_floor());
    }

    /// The floor's reason as behaviour: a model whose fit against the bar is
    /// 0.92 (deepseek-v4.1-flash at elo 1660 against the iq-80 bar of 1813)
    /// is not adequate, however well its other metrics fit, and a model at
    /// 0.96 is. Lowering the floor back to 0.9 turns the first assertion red.
    #[test]
    fn a_model_eight_percent_short_of_the_bar_is_not_adequate() {
        let short = score(&[
            (ModelMetric::Elo, 100.0, 0.92),
            (ModelMetric::Eq, 30.0, 1.0),
        ]);
        assert!(!short.clears_adequacy_floor());
        let near = score(&[
            (ModelMetric::Elo, 100.0, 0.96),
            (ModelMetric::Eq, 30.0, 1.0),
        ]);
        assert!(near.clears_adequacy_floor());
    }
}
