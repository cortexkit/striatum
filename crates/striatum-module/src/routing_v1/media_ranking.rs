//! Media grades are comparable only within a kind; demand is linear on that scale.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use super::{wire, EngineCandidate, EvalScore, FactNumber, NoRouteReason, Refusal};
use crate::config_policy::{BelowBarPolicy, ConsumerPolicy};

type Candidate = EngineCandidate<EvalScore, FactNumber>;

pub(super) fn pick<'a>(
    request: &wire::SelectEngineRequest,
    candidates: &'a [Candidate],
    grades: &BTreeMap<String, f64>,
    top_routes: &BTreeSet<String>,
    policy: &ConsumerPolicy,
) -> Result<&'a Candidate, Refusal> {
    let key =
        |candidate: &Candidate| format!("{}/{}", candidate.engine.provider, candidate.engine.model);
    let grade = |candidate: &Candidate| {
        // An offer belongs to this caller, not to a module's catalog identity.
        (!candidate.selected.caller_local)
            .then(|| grades.get(&key(candidate)).copied().map(FactNumber))
            .flatten()
    };
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
                    cost(a.cost, b.cost).then_with(|| b.accuracy.cmp(&a.accuracy))
                } else {
                    b.accuracy
                        .cmp(&a.accuracy)
                        .then_with(|| cost(a.cost, b.cost))
                }
            })
            .then_with(|| identity(a, b))
    };
    if let Some(bar) = bar {
        if let Some(winner) = candidates
            .iter()
            .filter(|candidate| usable(candidate))
            .filter(|candidate| grade(candidate).is_some_and(|grade| grade >= bar))
            .min_by(|a, b| {
                cost(a.cost, b.cost)
                    .then_with(|| grade(b).cmp(&grade(a)))
                    .then_with(|| b.accuracy.cmp(&a.accuracy))
                    .then_with(|| identity(a, b))
            })
        {
            return Ok(winner);
        }
        if policy.below_bar == Some(BelowBarPolicy::Refuse) {
            return Err(Refusal::no_route(NoRouteReason::NoAvailableModel, None));
        }
    }
    candidates
        .iter()
        .filter(|candidate| usable(candidate))
        .min_by(no_level)
        .ok_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None))
}

fn cost(a: Option<FactNumber>, b: Option<FactNumber>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}
