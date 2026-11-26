//! Route checks that do not use vault account state.

use std::collections::HashSet;

use striatum_core::{fact_to_route, ModelFact, ModelRouteStateSnapshot};

use crate::config_policy::RunnerPolicy;
use crate::routing_v1::runners::Registry;

/// Do not reserve a credential's next recovery attempt when no runnable
/// placement uses a login. Another variant can still need that credential.
pub(super) fn models_without_logins<'a>(
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

pub(super) fn routes(
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

pub(super) fn clear_account_state(
    routes: &HashSet<String>,
    facts: &mut [ModelFact],
    snapshot: &mut ModelRouteStateSnapshot,
) {
    if routes.is_empty() {
        return;
    }
    for fact in facts {
        let route = fact_to_route(fact);
        if !routes.contains(&route.route_key) {
            continue;
        }
        fact.billing_plane = None;
        fact.subscription_multiplier = None;
        for state in [
            snapshot.by_route.get_mut(&route.route_key),
            snapshot.by_capacity.get_mut(&route.capacity_key),
        ]
        .into_iter()
        .flatten()
        {
            state.credential_refusal = None;
            state.quota_exhausted_until = None;
            state.quota_pressure = None;
            state.subscription_pool = None;
        }
    }
}
