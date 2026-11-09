//! Break-glass handling for an all-exhausted candidate set.
//!
//! A subscription provider can report every usable route as exhausted for a long
//! window. The caller retries once after clearing the candidate with the earliest
//! future reset; a real provider rejection will immediately restore the block.

use crate::model_router::{ModelRouteStateSnapshot, RouteRuntimeState};
use crate::route_state_store::RouteWallClass;

/// One route rejected by hard quota or cooldown admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakGlassCandidate {
    pub route_key: String,
    pub capacity_key: String,
}

/// The cleared snapshot and the candidate chosen for the one-time retry.
#[derive(Debug, Clone, PartialEq)]
pub struct RelaxedSnapshot {
    pub snapshot: ModelRouteStateSnapshot,
    pub route_key: String,
    /// `None` when neither state scope reports a future reset.
    pub resets_at: Option<f64>,
}

fn earliest_future_reset(state: &RouteRuntimeState, now: f64) -> Option<f64> {
    [
        state.quota_exhausted_until,
        state.cooldown_until,
        state.failure_backoff_until,
        state.provider_reset_until,
    ]
    .into_iter()
    .flatten()
    .filter(|reset| *reset > now)
    .min_by(f64::total_cmp)
}

/// The candidate's own earliest future reset across both state scopes.
fn candidate_earliest_reset(
    snapshot: &ModelRouteStateSnapshot,
    candidate: &BreakGlassCandidate,
) -> Option<f64> {
    [
        snapshot.by_route.get(&candidate.route_key),
        snapshot.by_capacity.get(&candidate.capacity_key),
    ]
    .into_iter()
    .flatten()
    .filter_map(|state| earliest_future_reset(state, snapshot.now))
    .min_by(f64::total_cmp)
}

/// The earliest reset the relaxation can actually lift: quota and cooldown only.
/// `failure_backoff_until` is deliberately not in this set. The relaxation never
/// clears a failure backoff (that is the operator's `route.clear_failure_backoff`),
/// so ranking on it chose a route that stayed blocked on retry and spent the
/// one-shot relaxation for nothing while a sibling with only a quota wall was
/// relaxable (Loop R153).
fn candidate_clearable_reset(
    snapshot: &ModelRouteStateSnapshot,
    candidate: &BreakGlassCandidate,
) -> Option<f64> {
    [
        snapshot.by_route.get(&candidate.route_key),
        snapshot.by_capacity.get(&candidate.capacity_key),
    ]
    .into_iter()
    .flatten()
    .filter_map(|state| {
        [state.quota_exhausted_until, state.cooldown_until]
            .into_iter()
            .flatten()
            .filter(|reset| *reset > snapshot.now)
            .min_by(f64::total_cmp)
    })
    .min_by(f64::total_cmp)
}

/// A candidate under a live failure backoff in either scope stays blocked after
/// the relaxation no matter how it ranks, so it is not a candidate at all.
fn under_live_backoff(snapshot: &ModelRouteStateSnapshot, candidate: &BreakGlassCandidate) -> bool {
    [
        snapshot.by_route.get(&candidate.route_key),
        snapshot.by_capacity.get(&candidate.capacity_key),
    ]
    .into_iter()
    .flatten()
    .any(|state| {
        state
            .failure_backoff_until
            .is_some_and(|until| until > snapshot.now)
    })
}

/// The 2026-09-20 xai/grok-4.6 specimen showed that clearing an empty-account
/// wall only repeats a payment refusal. Unlike quota windows, a live billing wall
/// cannot become successful because break-glass retried it early.
fn under_live_billing_wall(
    snapshot: &ModelRouteStateSnapshot,
    candidate: &BreakGlassCandidate,
) -> bool {
    live_billing_wall_error(snapshot, candidate).is_some()
}

/// Return the provider error attached to a live billing wall. Legacy rows have no
/// class and remain relaxable; a classified row without error text still returns a
/// stable diagnostic phrase.
pub fn live_billing_wall_error<'a>(
    snapshot: &'a ModelRouteStateSnapshot,
    candidate: &BreakGlassCandidate,
) -> Option<&'a str> {
    [
        snapshot.by_route.get(&candidate.route_key),
        snapshot.by_capacity.get(&candidate.capacity_key),
    ]
    .into_iter()
    .flatten()
    .find(|state| {
        state.wall_class == Some(RouteWallClass::Billing)
            && (state
                .cooldown_until
                .is_some_and(|until| until > snapshot.now)
                || state
                    .quota_exhausted_until
                    .is_some_and(|until| until > snapshot.now))
    })
    .map(|state| state.last_error.as_deref().unwrap_or("billing wall"))
}

/// Earliest future reset across the GIVEN candidates only. This is the single
/// producer for "when could a blocked route recover": diagnostics must scope
/// the answer to the request's own rejected candidates, because a reset read
/// from an unrelated route (excluded, capability-rejected, or belonging to a
/// different request's pool) promises a recovery this caller can never use.
pub fn earliest_reset_among_candidates(
    snapshot: &ModelRouteStateSnapshot,
    candidates: &[BreakGlassCandidate],
) -> Option<f64> {
    candidates
        .iter()
        .filter_map(|candidate| candidate_earliest_reset(snapshot, candidate))
        .min_by(f64::total_cmp)
}

/// Clear both state scopes for the rejected candidate whose next reset is earliest.
/// Candidates with no stated future reset are eligible last: they provide the least
/// evidence of recovery, so they are least preferred for the one-time probe. They are
/// still eligible when no candidate has a known reset. Entries outside `candidates`
/// cannot consume the one-time relaxation.
pub fn relax_earliest_reset(
    snapshot: &ModelRouteStateSnapshot,
    candidates: &[BreakGlassCandidate],
) -> Option<RelaxedSnapshot> {
    let mut earliest: Option<(&BreakGlassCandidate, Option<f64>)> = None;

    for candidate in candidates {
        if under_live_backoff(snapshot, candidate) {
            continue;
        }
        if [
            snapshot.by_route.get(&candidate.route_key),
            snapshot.by_capacity.get(&candidate.capacity_key),
        ]
        .into_iter()
        .flatten()
        .any(|state| {
            state
                .provider_reset_until
                .is_some_and(|at| at > snapshot.now)
        }) {
            continue;
        }
        if under_live_billing_wall(snapshot, candidate) {
            continue;
        }
        let resets_at = candidate_clearable_reset(snapshot, candidate);
        let should_replace = earliest.as_ref().is_none_or(|(current, current_reset)| {
            match (resets_at, *current_reset) {
                // A stated reset is stronger recovery evidence than an unknown one.
                (Some(new_reset), Some(current_reset)) => {
                    new_reset < current_reset
                        || (new_reset == current_reset && candidate.route_key < current.route_key)
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => candidate.route_key < current.route_key,
            }
        });
        if should_replace {
            earliest = Some((candidate, resets_at));
        }
    }

    let (candidate, resets_at) = earliest?;
    let mut relaxed = snapshot.clone();
    for state in [
        relaxed.by_route.get_mut(&candidate.route_key),
        relaxed.by_capacity.get_mut(&candidate.capacity_key),
    ]
    .into_iter()
    .flatten()
    {
        state.quota_exhausted_until = None;
        state.cooldown_until = None;
    }

    Some(RelaxedSnapshot {
        snapshot: relaxed,
        route_key: candidate.route_key.clone(),
        resets_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn candidate(route_key: &str, capacity_key: &str) -> BreakGlassCandidate {
        BreakGlassCandidate {
            route_key: route_key.into(),
            capacity_key: capacity_key.into(),
        }
    }

    fn exhausted(quota_until: Option<f64>, cooldown_until: Option<f64>) -> RouteRuntimeState {
        RouteRuntimeState {
            quota_exhausted_until: quota_until,
            cooldown_until,
            wall_class: Some(RouteWallClass::Quota),
            ..Default::default()
        }
    }

    #[test]
    fn only_unknown_reset_candidate_is_still_relaxed() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([("p/a".into(), exhausted(Some(500.0), None))]),
            by_capacity: HashMap::new(),
        };
        let relaxed = relax_earliest_reset(&snap, &[candidate("p/a", "p/a")])
            .expect("an unknown-reset candidate remains eligible");
        assert_eq!(relaxed.route_key, "p/a");
        assert_eq!(relaxed.resets_at, None);
        assert_eq!(relaxed.snapshot.by_route["p/a"].quota_exhausted_until, None);
    }

    #[test]
    fn billing_wall_candidate_is_not_relaxed() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([(
                "xai/grok-4.6".into(),
                RouteRuntimeState {
                    quota_exhausted_until: Some(31_000.0),
                    wall_class: Some(RouteWallClass::Billing),
                    last_error: Some("spending-limit: out of credits".into()),
                    ..Default::default()
                },
            )]),
            by_capacity: HashMap::new(),
        };
        let candidate = candidate("xai/grok-4.6", "xai/grok-4.6");
        assert_eq!(
            live_billing_wall_error(&snap, &candidate),
            Some("spending-limit: out of credits")
        );
        assert_eq!(relax_earliest_reset(&snap, &[candidate]), None);
    }

    #[test]
    fn relaxes_only_the_earliest_rejected_candidate() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([
                ("p/a".into(), exhausted(Some(5_000.0), None)),
                ("p/b".into(), exhausted(Some(3_000.0), None)),
                ("p/c".into(), exhausted(Some(9_000.0), None)),
                ("p/not-a-candidate".into(), exhausted(Some(2_000.0), None)),
            ]),
            by_capacity: HashMap::new(),
        };
        let candidates = [
            candidate("p/a", "p/a"),
            candidate("p/b", "p/b"),
            candidate("p/c", "p/c"),
        ];

        let relaxed = relax_earliest_reset(&snap, &candidates).expect("candidate is exhausted");

        assert_eq!(relaxed.route_key, "p/b");
        assert_eq!(relaxed.resets_at, Some(3_000.0));
        assert_eq!(relaxed.snapshot.by_route["p/b"].quota_exhausted_until, None);
        assert_eq!(
            relaxed.snapshot.by_route["p/not-a-candidate"].quota_exhausted_until,
            Some(2_000.0)
        );
        assert_eq!(
            relaxed.snapshot.by_route["p/a"].quota_exhausted_until,
            Some(5_000.0)
        );
    }

    #[test]
    fn known_reset_beats_unknown_reset_candidate() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([
                ("p/known".into(), exhausted(Some(4_600.0), None)),
                ("p/unknown".into(), exhausted(Some(500.0), None)),
            ]),
            by_capacity: HashMap::new(),
        };
        let candidates = [
            candidate("p/unknown", "p/unknown"),
            candidate("p/known", "p/known"),
        ];

        let relaxed = relax_earliest_reset(&snap, &candidates).expect("candidate is exhausted");

        assert_eq!(relaxed.route_key, "p/known");
        assert_eq!(relaxed.resets_at, Some(4_600.0));
        assert_eq!(
            relaxed.snapshot.by_route["p/known"].quota_exhausted_until,
            None
        );
        assert_eq!(
            relaxed.snapshot.by_route["p/unknown"].quota_exhausted_until,
            Some(500.0)
        );
    }

    #[test]
    fn clears_route_and_capacity_mirrors_together() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([("p/a/high".into(), exhausted(Some(4_000.0), None))]),
            by_capacity: HashMap::from([("p/a".into(), exhausted(Some(4_000.0), Some(4_000.0)))]),
        };

        let relaxed = relax_earliest_reset(&snap, &[candidate("p/a/high", "p/a")])
            .expect("candidate is exhausted");

        assert_eq!(
            relaxed.snapshot.by_route["p/a/high"].quota_exhausted_until,
            None
        );
        assert_eq!(
            relaxed.snapshot.by_capacity["p/a"].quota_exhausted_until,
            None
        );
        assert_eq!(relaxed.snapshot.by_capacity["p/a"].cooldown_until, None);
    }

    #[test]
    fn ignores_past_route_reset_and_uses_live_capacity_reset() {
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([("p/a/high".into(), exhausted(Some(500.0), None))]),
            by_capacity: HashMap::from([("p/a".into(), exhausted(None, Some(4_000.0)))]),
        };

        let relaxed = relax_earliest_reset(&snap, &[candidate("p/a/high", "p/a")])
            .expect("capacity cooldown is future");

        assert_eq!(relaxed.route_key, "p/a/high");
        assert_eq!(relaxed.resets_at, Some(4_000.0));
        assert_eq!(relaxed.snapshot.by_capacity["p/a"].cooldown_until, None);
    }

    /// Loop R153: opus is quota-walled to T+4h AND under a route-local failure
    /// backoff to T+8min; sonnet is only quota-walled to T+4h. Ranking on the
    /// all-walls horizon picked opus (its backoff reset is earliest), cleared its
    /// quota, and the retry rejected it as backoff_active: the one-shot relaxation
    /// was spent and route.select returned no_route while sonnet was relaxable.
    #[test]
    fn a_candidate_under_live_backoff_is_skipped_and_the_clearable_sibling_is_relaxed() {
        let now = 1_000.0;
        let opus = RouteRuntimeState {
            quota_exhausted_until: Some(now + 4.0 * 3_600_000.0),
            failure_backoff_until: Some(now + 8.0 * 60_000.0),
            ..Default::default()
        };
        let sonnet = exhausted(Some(now + 4.0 * 3_600_000.0), None);
        let snap = ModelRouteStateSnapshot {
            now,
            by_route: HashMap::from([("a/opus".into(), opus), ("a/sonnet".into(), sonnet)]),
            by_capacity: HashMap::new(),
        };
        let relaxed = relax_earliest_reset(
            &snap,
            &[
                candidate("a/opus", "a/opus"),
                candidate("a/sonnet", "a/sonnet"),
            ],
        )
        .expect("the quota-only sibling is relaxable");
        assert_eq!(relaxed.route_key, "a/sonnet");
        assert_eq!(
            relaxed.snapshot.by_route["a/sonnet"].quota_exhausted_until,
            None
        );
        // The backoff-carrying route keeps every wall: the relaxation never lifts a backoff.
        assert_eq!(
            relaxed.snapshot.by_route["a/opus"].failure_backoff_until,
            Some(now + 8.0 * 60_000.0)
        );
        assert!(relaxed.snapshot.by_route["a/opus"]
            .quota_exhausted_until
            .is_some());

        // Every candidate under a live backoff: nothing to relax, and saying so is
        // honest; the caller's no_route stands.
        let only_backoff = ModelRouteStateSnapshot {
            now,
            by_route: HashMap::from([(
                "a/opus".into(),
                RouteRuntimeState {
                    quota_exhausted_until: Some(now + 60_000.0),
                    failure_backoff_until: Some(now + 60_000.0),
                    ..Default::default()
                },
            )]),
            by_capacity: HashMap::new(),
        };
        assert!(relax_earliest_reset(&only_backoff, &[candidate("a/opus", "a/opus")]).is_none());
    }
}
