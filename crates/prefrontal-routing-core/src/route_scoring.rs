//! Quality adequacy scoring (threshold model).
//!
//! Ported from the Alfonso TS plugin (`src/features/model-routing/route-scoring.ts`).
//!
//! Converts absolute iq/eq facts + 0..100 requirements into a SATURATING "clears
//! the bar" adequacy score: meeting the requirement target scores ~1.0, and
//! exceeding it yields NO additional bonus. This is the core of the routing
//! model: it stops "more is always better", so the router can pick the cheapest
//! adequate model instead of always the highest-capability (most expensive) one.
//!
//! Speed is NOT scored here. It is an OBJECTIVE handled by the router among the
//! models that already clear the quality bar, never a gate that could pull a
//! sub-bar model in.

use std::collections::BTreeMap;

use crate::model_facts::{Confidence, ModelFact, ModelMetric, ModelRequirements};
use crate::route_anchors::{
    metric_spread, requirement_target, sigmoid, Anchors, QUALITY_METRICS,
    UNKNOWN_FACT_CONFIDENCE_PENALTY,
};

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct MetricFit {
    pub requirement: f64,
    pub fact: f64,
    pub target: f64,
    pub fit: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdequacyScore {
    /// Weighted saturating adequacy over quality metrics, after unknown penalty.
    pub adequacy: f64,
    /// Raw adequacy before the unknown-fact penalty (tie-break / audit).
    pub raw_adequacy: f64,
    /// Per-quality-metric audit; excludes metrics with requirement 0.
    pub metrics: BTreeMap<ModelMetric, MetricFit>,
    /// True when no quality metric had a non-zero requirement (capability-only).
    pub capability_only: bool,
    pub explanation: Vec<String>,
}

/// Saturating fit: 1.0 at-or-above the target (exceeding gives NO bonus), ramps
/// down below the target. `2·sigmoid(0) = 1`, so fit == 1 exactly at the target.
pub fn saturating_fit(fact_score: f64, target: f64, spread: f64) -> f64 {
    (2.0 * sigmoid((fact_score - target) / spread)).min(1.0)
}

/// Score how well a fact clears the task's quality bar (iq/eq thresholds).
///
/// Requirement 0 means "ignore this metric". All-zero quality requirements yield
/// `capability_only = true` with adequacy 1 (the caller must have admitted via a
/// required capability such as vision; the router differentiates such routes by
/// the cost/speed objective alone).
pub fn score_adequacy(
    fact: &ModelFact,
    requirements: &ModelRequirements,
    anchors: &Anchors,
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
        let anchor = anchors.get(metric);
        let fact_score = fact.scores.get(metric);
        let target = requirement_target(anchor, requirement);
        let spread = metric_spread(anchor);
        let fit = saturating_fit(fact_score, target, spread);
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

/// Format a requirement like JS `${n}`: integers without a decimal point, others
/// with their natural representation. Keeps explanation strings byte-identical to
/// the TS port source for whole-number demands (the common case).
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}
