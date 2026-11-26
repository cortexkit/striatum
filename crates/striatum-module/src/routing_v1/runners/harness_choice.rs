//! Session task admission is separate from route admission and runner ranking.

use cortexkit_role_routing::errors::{NoRouteReason, Refusal};
use cortexkit_role_routing::placement::HarnessCapability;
use cortexkit_role_routing::select::{Requirements, SelectRequest, ToolsRequirement};

use super::{EffectiveRunner, Registry, RunnerKey, RunnerPolicies};

#[path = "task_refusal.rs"]
mod task_refusal;
pub(crate) use task_refusal::empty_reachable;
pub(crate) use task_refusal::TaskRunnerDiagnostics;

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
    for need in explicit_needs(requirements) {
        if !needs.contains(need) {
            needs.push(need.clone());
        }
    }
    needs
}

fn explicit_needs(requirements: &Requirements) -> &[HarnessCapability] {
    requirements
        .harness_capabilities
        .as_deref()
        .unwrap_or_default()
}

impl Registry {
    pub(crate) fn task_harness_unavailable(
        &self,
        request: &SelectRequest,
        runners: &[crate::config_policy::RunnerPolicy],
    ) -> Refusal {
        let mut refusal = Refusal::no_route(NoRouteReason::HarnessUnavailable, None);
        if let Some(explicit) = &request.requirements.harness_capabilities {
            let needs = task_needs(&request.requirements);
            let mut missing = std::collections::BTreeMap::new();
            for policy in runners {
                let Some(runner) = self.get(policy) else {
                    continue;
                };
                let requested = request.harness.as_deref().or(policy.harness.as_deref());
                for harness in runner.task_harnesses(requested) {
                    let lacking: Vec<_> = needs
                        .iter()
                        .filter(|need| !runner.supports_need(harness, need, explicit))
                        .collect();
                    missing.insert(format!("{}/{harness}", policy.module), lacking);
                }
            }
            refusal
                .detail
                .insert("missing".into(), serde_json::json!(missing));
        }
        refusal
    }

    pub(crate) fn serves_on_other_harness(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            !self.can_run(key, route, capacity)
                && (runner.placement_baseline.as_ref().is_some_and(|baseline| {
                    baseline.matches_route(route, capacity, |runner| {
                        &runner.statically_reachable_models
                    })
                }))
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
        let catalog = facts.iter().collect();
        let config = source.config;
        let original = source.enrolled(credentials, now_ms);
        let source_policies = original.clone();
        // A settings entry's harness narrows only where new sessions start.
        // The head-choice picker still lists every model the runner can open
        // on any harness, even when the settings entry names one harness.
        let mut registry = Self {
            entries: original
                .iter()
                .flat_map(|policy| {
                    Self::build_resolved(
                        RunnerPolicies {
                            policies: vec![policy.clone()],
                            config,
                            consumer: None,
                            auto_enroll: false,
                        },
                        credentials,
                        facts,
                        now_ms,
                        requested.or(policy.harness.as_deref()),
                        true,
                        &catalog,
                    )
                    .entries
                })
                .collect(),
        };
        let needs = task_needs(requirements);
        let explicit = explicit_needs(requirements);
        for (entry, policy) in registry.entries.iter_mut().zip(original) {
            let requested = requested.or(policy.harness.as_deref());
            let declaring = entry
                .wire_declaration
                .as_ref()
                .is_some_and(|declaration| declaration.declares_harness_facts());
            if !declaring && !explicit.is_empty() {
                // Legacy inference is permissive, but an explicit need requires
                // affirmative evidence from the named harness.
                entry.harness_choices = Some(Vec::new());
                continue;
            }
            let unbound_legacy = !declaring
                && requested.is_none()
                && !entry.harnesses.is_empty()
                && entry.selection_harness(None).is_none()
                && super::super::describe::capabilities()
                    .contains(&cortexkit_role_routing::capabilities::DECISION_HARNESS);
            if !declaring && !unbound_legacy {
                // A legacy default keeps its existing reach: another harness
                // must not rescue a route that the default refuses. A runner
                // declaring no harnesses also keeps its unnamed wire shape.
                continue;
            }
            let candidates = entry
                .task_harnesses(requested)
                .into_iter()
                .filter(|name| {
                    !declaring
                        || entry.wire_declaration.as_ref().is_some_and(|declaration| {
                            declaration
                                .limits_for_harness(Some(name))
                                .is_ok_and(|limits| limits.starts_sessions == Some(true))
                                && needs
                                    .iter()
                                    .all(|need| declaration.harness_supports(name, need))
                        })
                })
                .map(|harness| Candidate {
                    runner: Self::build_resolved(
                        RunnerPolicies {
                            policies: vec![policy.clone()],
                            config,
                            consumer: None,
                            auto_enroll: false,
                        },
                        credentials,
                        facts,
                        now_ms,
                        Some(harness),
                        false,
                        &catalog,
                    )
                    .entries
                    .remove(0),
                    harness: harness.to_owned(),
                })
                .collect::<Vec<_>>();
            // The parent retains runner-wide credential metadata for account
            // selection. Route admission uses each candidate's own permitted
            // served models, never a union for a single selected harness.
            let mut parent = Self::build_resolved(
                RunnerPolicies {
                    policies: vec![policy.clone()],
                    config,
                    consumer: None,
                    auto_enroll: false,
                },
                credentials,
                facts,
                now_ms,
                None,
                false,
                &catalog,
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
        for (entry, policy) in registry.entries.iter_mut().zip(&source_policies) {
            if requested.is_some() || policy.harness.is_some() || !explicit.is_empty() {
                entry.placement_baseline = Self::build_resolved(
                    RunnerPolicies {
                        policies: vec![policy.clone()],
                        config,
                        consumer: None,
                        auto_enroll: false,
                    },
                    credentials,
                    facts,
                    now_ms,
                    None,
                    false,
                    &catalog,
                )
                .entries
                .pop()
                .map(Box::new);
            }
        }
        registry.keep_distinct_placement_keys(&source_policies);
        registry
    }
}

impl EffectiveRunner {
    fn task_harnesses(&self, requested: Option<&str>) -> Vec<&str> {
        if let Some(requested) = requested {
            return self
                .harnesses
                .iter()
                .map(String::as_str)
                .filter(|name| *name == requested)
                .collect();
        }
        let mut names: Vec<_> = self.default_harness.as_deref().into_iter().collect();
        for name in &self.harnesses {
            if !names.contains(&name.as_str()) {
                names.push(name);
            }
        }
        names
    }

    fn supports_need(
        &self,
        harness: &str,
        need: &HarnessCapability,
        explicit: &[HarnessCapability],
    ) -> bool {
        let declaring = self
            .wire_declaration
            .as_ref()
            .filter(|declaration| declaration.declares_harness_facts());
        declaring.map_or_else(
            || !explicit.contains(need),
            |declaration| declaration.harness_supports(harness, need),
        )
    }

    pub(crate) fn hosts_task(&self, requirements: &Requirements) -> bool {
        self.harness_choices.as_ref().map_or_else(
            || explicit_needs(requirements).is_empty(),
            |choices| !choices.is_empty(),
        )
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
            None => self.selection_harness(requested.or(self.policy.harness.as_deref())),
        }
    }

    pub(crate) fn keyless(&self, route: &str, capacity: &str) -> bool {
        match &self.harness_choices {
            Some(choices) => choices
                .iter()
                .find(|candidate| {
                    candidate.admits(route, capacity, |runner| &runner.reachable_models)
                })
                .is_some_and(|candidate| candidate.runner.keyless_models.contains(capacity)),
            None => self.matches_route(route, capacity, |runner| &runner.keyless_models),
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
        self.runner.matches_route(route, capacity, models)
    }
}
