//! Media grades are comparable only within a kind; demand is linear on that scale.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use super::{wire, EngineCandidate, EvalScore, FactNumber, NoRouteReason, Refusal};
use crate::config_policy::{BelowBarPolicy, ConsumerPolicy};

type Candidate = EngineCandidate<EvalScore, FactNumber>;

pub(super) struct Ranked<'a> {
    pub top: Option<f64>,
    pub bar: Option<f64>,
    pub adequate: Vec<&'a Candidate>,
    fallback: Vec<&'a Candidate>,
}

impl<'a> Ranked<'a> {
    pub fn pick(&self, policy: &ConsumerPolicy) -> Result<&'a Candidate, Refusal> {
        self.adequate
            .first()
            .copied()
            .or_else(|| {
                (policy.below_bar != Some(BelowBarPolicy::Refuse))
                    .then(|| self.fallback.first().copied())
                    .flatten()
            })
            .ok_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None))
    }
}

pub(super) fn rank<'a>(
    request: &wire::SelectEngineRequest,
    candidates: &'a [Candidate],
    grades: &BTreeMap<String, f64>,
    top_routes: &BTreeSet<String>,
    minimums: &BTreeMap<String, f64>,
    policy: &ConsumerPolicy,
) -> Ranked<'a> {
    let key =
        |candidate: &Candidate| format!("{}/{}", candidate.engine.provider, candidate.engine.model);
    let grade = |candidate: &Candidate| grades.get(&key(candidate)).copied().map(FactNumber);
    let usable = |candidate: &Candidate| {
        wire::select_engine(request, std::slice::from_ref(candidate)).is_ok()
    };
    // Policy, privacy and requirements bound the quality scale. Temporary route
    // and credential fences and account backoff must not lower demand in an outage.
    let top = candidates
        .iter()
        .filter(|candidate| top_routes.contains(&key(candidate)))
        .filter(|candidate| {
            let mut static_candidate = (*candidate).clone();
            static_candidate.fenced = false;
            usable(&static_candidate)
        })
        .filter_map(grade)
        .max();
    let bar = policy
        .iq
        .zip(top)
        .map(|(level, top)| FactNumber(top.0 * level / 100.0));
    let identity = |a: &Candidate, b: &Candidate| {
        a.engine
            .provider
            .cmp(&b.engine.provider)
            .then_with(|| a.engine.model.cmp(&b.engine.model))
            .then_with(|| a.selected.module.cmp(&b.selected.module))
            .then_with(|| a.selected.index.cmp(&b.selected.index))
    };
    let unknown_minimum_costs: Vec<_> = candidates
        .iter()
        .filter(|candidate| usable(candidate) && !minimums.contains_key(&key(candidate)))
        .filter_map(|candidate| candidate.cost)
        .collect();
    let cost_order = |a: &Candidate, b: &Candidate| {
        cost_with_minimum(
            a.cost,
            b.cost,
            minimums.get(&key(a)).copied(),
            minimums.get(&key(b)).copied(),
            &unknown_minimum_costs,
        )
    };
    let all_ungraded = !candidates
        .iter()
        .filter(|candidate| usable(candidate))
        .any(|candidate| grade(candidate).is_some());
    let no_level = |a: &&Candidate, b: &&Candidate| {
        let (a_grade, b_grade) = (grade(a), grade(b));
        b_grade
            .cmp(&a_grade)
            .then_with(|| {
                if all_ungraded {
                    // The published contract gives an entirely ungraded population the
                    // compatibility order: cost before measurement.
                    cost_order(a, b).then_with(|| b.accuracy.cmp(&a.accuracy))
                } else {
                    b.accuracy.cmp(&a.accuracy).then_with(|| cost_order(a, b))
                }
            })
            .then_with(|| identity(a, b))
    };
    let mut fallback: Vec<_> = candidates
        .iter()
        .filter(|candidate| usable(candidate))
        .collect();
    fallback.sort_by(no_level);
    let mut adequate = fallback.clone();
    if let Some(bar) = bar {
        adequate.retain(|candidate| grade(candidate).is_some_and(|grade| grade >= bar));
        adequate.sort_by(|a, b| {
            cost_order(a, b)
                .then_with(|| grade(b).cmp(&grade(a)))
                .then_with(|| b.accuracy.cmp(&a.accuracy))
                .then_with(|| identity(a, b))
        });
    }
    Ranked {
        top: top.map(|n| n.0),
        bar: bar.map(|n| n.0),
        adequate,
        fallback,
    }
}

fn cost(a: Option<FactNumber>, b: Option<FactNumber>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn cost_with_minimum(
    a_cost: Option<FactNumber>,
    b_cost: Option<FactNumber>,
    a_minimum: Option<f64>,
    b_minimum: Option<f64>,
    unknown_minimum_costs: &[FactNumber],
) -> Ordering {
    cost(a_cost, b_cost).then_with(|| {
        // A mixed known/unknown group must use one tie order throughout.
        // Pairwise minimum comparisons can otherwise form an ordering cycle.
        if a_cost.is_some_and(|cost| unknown_minimum_costs.contains(&cost)) {
            return Ordering::Equal;
        }
        a_minimum
            .zip(b_minimum)
            .map_or(Ordering::Equal, |(a, b)| a.total_cmp(&b))
    })
}

#[cfg(test)]
mod minimum_tests {
    use super::*;

    #[test]
    fn minimum_comparison_is_transitive_for_every_permutation() {
        let rates = [Some(FactNumber(2.0)); 3];
        let minimums = [Some(60.0), None, Some(20.0)];
        let accuracy = [0.9_f64, 0.8, 0.7];
        let unknowns: Vec<_> = rates
            .iter()
            .zip(minimums)
            .filter(|(_, minimum)| minimum.is_none())
            .filter_map(|(cost, _)| *cost)
            .collect();
        let compare = |a: usize, b: usize| {
            cost_with_minimum(rates[a], rates[b], minimums[a], minimums[b], &unknowns)
                .then_with(|| accuracy[b].total_cmp(&accuracy[a]))
        };
        for permutation in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let [a, b, c] = permutation;
            let (ab, bc, ac) = (compare(a, b), compare(b, c), compare(a, c));
            if ab.is_le() && bc.is_le() {
                assert!(ac.is_le(), "non-transitive {permutation:?}");
            }
            if ab.is_ge() && bc.is_ge() {
                assert!(ac.is_ge(), "non-transitive {permutation:?}");
            }
            let mut ordered = permutation;
            ordered.sort_by(|a, b| compare(*a, *b));
            assert_eq!(ordered, [0, 1, 2]);
        }
    }
}
