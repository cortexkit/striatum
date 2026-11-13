//! Session task admission is separate from route admission and runner ranking.

use cortexkit_role_routing::placement::HarnessCapability;
use cortexkit_role_routing::select::{Requirements, ToolsRequirement};

use super::{EffectiveRunner, Registry, RunnerPolicies};

#[derive(Clone, Debug)]
pub(super) struct Candidate {
    harness: String,
    runner: EffectiveRunner,
}

fn task_needs(requirements: &Requirements) -> Vec<HarnessCapability> {
    let mut needs = Vec::new();
    if requirements.tools == Some(ToolsRequirement::None) {
        needs.push(HarnessCapability::OneOff);
    }
    if requirements.max_output_tokens.is_some() {
        needs.push(HarnessCapability::OutputCap);
    }
    needs
}

impl Registry {
    pub(crate) fn excluded_by_harness(&self, module: &str, route: &str, capacity: &str) -> bool {
        self.get(module).is_some_and(|runner| {
            !self.can_run(module, route, capacity)
                && runner.harness_choices.as_ref().is_some_and(|choices| {
                    choices.iter().any(|candidate| {
                        candidate.runner.matches_route(route, capacity, |runner| {
                            &runner.statically_reachable_models
                        })
                    })
                })
        })
    }

    pub(crate) fn task_policies(&self) -> Vec<crate::config_policy::RunnerPolicy> {
        self.entries
            .iter()
            // Let task admission judge declaring runners, even if all their
            // harnesses are stopped. That produces harness_unavailable rather
            // than treating the runner as absent from operator settings.
            .filter(|runner| runner.starts_sessions || runner.harness_choices.is_some())
            .map(|runner| runner.policy.clone())
            .collect()
    }

    pub(crate) fn build_for_task<'a>(
        policies: impl Into<RunnerPolicies<'a>>,
        credentials: &crate::broca_credentials::BrocaCredentials,
        facts: &[striatum_core::ModelFact],
        now_ms: u64,
        requested: Option<&str>,
        requirements: &Requirements,
    ) -> Self {
        let source = policies.into();
        let config = source.config;
        let original = source.policies.clone();
        let mut registry = Self::build_for_selection(source, credentials, facts, now_ms, requested);
        let needs = task_needs(requirements);
        for (entry, policy) in registry.entries.iter_mut().zip(original) {
            let Some(declaration) = entry
                .wire_declaration
                .as_ref()
                .filter(|declaration| declaration.declares_harness_facts())
            else {
                // Without declared facts, route reachability uses the default
                // harness alone. Trying another harness after the default
                // refuses a route would change existing picks.
                continue;
            };
            let mut names = Vec::new();
            if let Some(requested) = requested {
                if entry.harnesses.iter().any(|name| name == requested) {
                    names.push(requested.to_owned());
                }
            } else {
                names.extend(entry.default_harness.iter().cloned());
                for name in &entry.harnesses {
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
            }
            let candidates = names
                .into_iter()
                .filter(|name| {
                    declaration
                        .limits_for_harness(Some(name))
                        .is_ok_and(|limits| limits.starts_sessions == Some(true))
                        && needs
                            .iter()
                            .all(|need| declaration.harness_supports(name, need))
                })
                .map(|harness| Candidate {
                    runner: Self::build_for_harness(
                        RunnerPolicies {
                            policies: vec![policy.clone()],
                            config,
                        },
                        credentials,
                        facts,
                        now_ms,
                        Some(&harness),
                    )
                    .entries
                    .remove(0),
                    harness,
                })
                .collect::<Vec<_>>();
            // The parent retains runner-wide credential metadata for account
            // selection. Route admission uses each candidate's own permitted
            // transport/auth pairs and providers, never a union of their limits.
            let mut parent = Self::build_for_harness(
                RunnerPolicies {
                    policies: vec![policy],
                    config,
                },
                credentials,
                facts,
                now_ms,
                None,
            )
            .entries
            .remove(0);
            parent.harness_choices = Some(candidates);
            parent.policy.routes = Some(
                facts
                    .iter()
                    .map(|fact| {
                        (
                            striatum_core::make_route_key(
                                &fact.provider_id,
                                &fact.model_id,
                                fact.variant.as_deref(),
                            ),
                            format!("{}/{}", fact.provider_id, fact.model_id),
                        )
                    })
                    .filter(|(route, capacity)| {
                        parent.matches_route(route, capacity, |runner| &runner.reachable_models)
                    })
                    .map(|(route, _)| route)
                    .collect(),
            );
            *entry = parent;
        }
        registry
    }
}

impl EffectiveRunner {
    pub(crate) fn hosts_task(&self) -> bool {
        self.harness_choices
            .as_ref()
            .is_none_or(|choices| !choices.is_empty())
    }

    pub(super) fn matches_route(
        &self,
        route: &str,
        capacity: &str,
        models: impl Fn(&Self) -> &std::collections::BTreeSet<String> + Copy,
    ) -> bool {
        if let Some(choices) = &self.harness_choices {
            return choices
                .iter()
                .any(|candidate| candidate.admits(route, capacity, models));
        }
        models(self).contains(capacity)
            && self
                .route_policy
                .as_ref()
                .is_none_or(|specs| specs.resolve(route, false))
    }

    pub(crate) fn harness_for_route(
        &self,
        requested: Option<&str>,
        route: &str,
        capacity: &str,
    ) -> Option<String> {
        match &self.harness_choices {
            Some(choices) => choices
                .iter()
                .find(|candidate| {
                    candidate.admits(route, capacity, |runner| &runner.reachable_models)
                })
                .map(|candidate| candidate.harness.clone()),
            None => self.selection_harness(requested),
        }
    }
}

impl Candidate {
    fn admits(
        &self,
        route: &str,
        capacity: &str,
        models: impl Fn(&EffectiveRunner) -> &std::collections::BTreeSet<String> + Copy,
    ) -> bool {
        !self
            .runner
            .wire_declaration
            .as_ref()
            .is_some_and(|declaration| declaration.harness_excludes(&self.harness, route))
            && self.runner.matches_route(route, capacity, models)
    }
}
