//! Route checks that do not use vault account state.

use std::collections::HashSet;

use striatum_core::{fact_to_route, ModelFact, ModelRouteStateSnapshot};

use crate::config_policy::RunnerPolicy;
use crate::routing_v1::runners::Registry;

/// Identify models that need no account test. A test holds the next attempt
/// to see whether a blocked credential can send again. Keep account tests
/// enabled if any runnable placement of this model or variant needs a login.
pub(in crate::routing_v1) fn models_without_logins<'a>(
    facts: &'a [ModelFact],
    registry: &Registry,
    policies: &[RunnerPolicy],
) -> HashSet<(&'a str, &'a str)> {
    let mut keyless = HashSet::new();
    let mut credentialed = HashSet::new();
    if registry.has_keyless_routes() {
        for fact in facts {
            let route = fact_to_route(fact);
            for policy in policies
                .iter()
                .filter(|policy| registry.runs(*policy, &route.route_key, &route.capacity_key))
            {
                let model = (fact.provider_id.as_str(), fact.model_id.as_str());
                if registry
                    .get(policy)
                    .is_some_and(|runner| runner.keyless(&route.route_key, &route.capacity_key))
                {
                    keyless.insert(model);
                } else {
                    credentialed.insert(model);
                }
            }
        }
        keyless.retain(|model| !credentialed.contains(model));
    }
    keyless
}

pub(in crate::routing_v1) fn routes(
    facts: &[ModelFact],
    registry: &Registry,
    policies: &[RunnerPolicy],
    preferred: Option<&str>,
    runs: impl Fn(&RunnerPolicy, &str, &str) -> bool,
) -> HashSet<String> {
    if !registry.has_keyless_routes() {
        return HashSet::new();
    }
    facts
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            crate::select_policy::choose_runner(
                policies,
                preferred,
                &route.route_key,
                &route.capacity_key,
                &runs,
            )
            .and_then(|policy| registry.get(policy))
            .filter(|runner| runner.keyless(&route.route_key, &route.capacity_key))
            .map(|_| route.route_key)
        })
        .collect()
}

pub(in crate::routing_v1) fn clear_account_state(
    routes: &HashSet<String>,
    facts: &mut [ModelFact],
    snapshot: &mut ModelRouteStateSnapshot,
) {
    if routes.is_empty() {
        return;
    }
    let shared: HashSet<_> = facts
        .iter()
        .map(fact_to_route)
        .filter(|route| !routes.contains(&route.route_key))
        .map(|route| route.capacity_key)
        .collect();
    for fact in facts {
        let route = fact_to_route(fact);
        if !routes.contains(&route.route_key) {
            continue;
        }
        fact.billing_plane = None;
        fact.subscription_multiplier = None;
        if let Some(state) = snapshot.by_route.get_mut(&route.route_key) {
            state.clear_account_state();
        }
        if shared.contains(&route.capacity_key) {
            snapshot.keyless_mixed_routes.insert(route.route_key);
        } else if let Some(state) = snapshot.by_capacity.get_mut(&route.capacity_key) {
            state.clear_account_state();
        }
    }
}
