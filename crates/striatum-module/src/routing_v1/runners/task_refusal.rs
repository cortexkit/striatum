use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use serde_json::{json, Value};

use super::{explicit_needs, task_needs, EffectiveRunner, Refusal, Registry, SelectRequest};
use crate::config_policy::{ConsumerPolicy, RunnerPolicy};

struct Candidate {
    policy: RunnerPolicy,
    excluded_by: Option<&'static str>,
}

/// Keep policy order even when enrollment or admission drops all candidates.
#[derive(Default)]
pub(crate) struct TaskRunnerDiagnostics {
    candidates: Vec<Candidate>,
    declared_module: Option<String>,
}

impl TaskRunnerDiagnostics {
    pub(crate) fn prepare(
        consumer: &str,
        project: Option<&str>,
        declared_module: Option<&str>,
        policy: &mut ConsumerPolicy,
        project_candidates: Option<Vec<(RunnerPolicy, bool)>>,
        enrolled: &[RunnerPolicy],
    ) -> Self {
        let mut candidates = match project_candidates {
            Some(entries) => {
                policy.runners = Some(
                    entries
                        .iter()
                        .filter(|(_, allowed)| *allowed)
                        .map(|(entry, _)| entry.clone())
                        .collect(),
                );
                policy.runner = None;
                entries
                    .into_iter()
                    .map(|(entry, allowed)| {
                        let harness_not_allowed =
                            !allowed && enrolled.iter().any(|p| p.module == entry.module);
                        Candidate {
                            policy: entry,
                            excluded_by: harness_not_allowed.then_some("global_enrollment"),
                        }
                    })
                    .collect()
            }
            None => configured(policy, enrolled),
        };
        if let Some(module) = declared_module {
            let allowed: Vec<_> = enrolled
                .iter()
                .filter(|entry| entry.module == module)
                .cloned()
                .collect();
            let all_outside = !candidates.is_empty()
                && candidates.iter().all(|entry| entry.policy.module != module);
            for entry in &mut candidates {
                if entry.policy.module != module {
                    entry.excluded_by = Some(if allowed.is_empty() {
                        "global_enrollment"
                    } else {
                        "declared_placement"
                    });
                }
            }
            if all_outside {
                warn_once(consumer, project, module, &candidates, !allowed.is_empty());
                if !allowed.is_empty() {
                    // Fall back to the operator's placements for the declared runner,
                    // retaining their harness and route limits.
                    policy.runner = None;
                    policy.runners = Some(allowed.clone());
                    candidates.extend(allowed.into_iter().map(|policy| Candidate {
                        policy,
                        excluded_by: None,
                    }));
                }
            }
        }
        Self {
            candidates,
            declared_module: declared_module.map(str::to_owned),
        }
    }

    pub(crate) fn attach(
        &self,
        mut refusal: Refusal,
        request: &SelectRequest,
        registry: &Registry,
        remaining_filter: &'static str,
    ) -> Refusal {
        let needs = task_needs(&request.requirements);
        let explicit = explicit_needs(&request.requirements);
        let rows: Vec<_> = self
            .candidates
            .iter()
            .map(|entry| {
                let policy = &entry.policy;
                let requested = request.harness.as_deref().or(policy.harness.as_deref());
                let filtered = |filter| json!({"reason":"excluded_by_policy", "filter":filter});
                let mut row = match entry.excluded_by {
                    Some(filter) => {
                        let mut row = filtered(filter);
                        if let Some(module) = self.declared_module.as_deref() {
                            row["declared_module"] = json!(module);
                        }
                        row
                    }
                    None if remaining_filter == "declared_placement"
                        && self.declared_module.is_none() =>
                    {
                        filtered(remaining_filter)
                    }
                    None if request
                        .runners
                        .as_ref()
                        .is_some_and(|list| !list.contains(&policy.module)) =>
                    {
                        filtered("request_runners")
                    }
                    None if request.harness.as_ref().is_some_and(|harness| {
                        policy
                            .harness
                            .as_ref()
                            .is_some_and(|pinned| pinned != harness)
                    }) =>
                    {
                        let mut row = filtered("request_harness");
                        row["policy_harness"] = json!(policy.harness);
                        row
                    }
                    None => registry
                        .get(policy)
                        .and_then(|runner| runner.wire_declaration().map(|_| runner))
                        .map_or_else(
                            || json!({"reason":"not_registered"}),
                            |runner| {
                                describe_harnesses(
                                    runner,
                                    requested,
                                    &needs,
                                    explicit,
                                    remaining_filter,
                                )
                            },
                        ),
                };
                row["module"] = json!(policy.module);
                if let Some(harness) = requested {
                    row["harness"] = json!(harness);
                }
                row
            })
            .collect();
        refusal.detail.insert("runners".into(), json!(rows));
        refusal
    }
}

fn configured(policy: &ConsumerPolicy, enrolled: &[RunnerPolicy]) -> Vec<Candidate> {
    let mut entries = policy
        .runners
        .as_deref()
        .unwrap_or(enrolled)
        .iter()
        .filter(|entry| {
            policy
                .runner
                .as_ref()
                .is_none_or(|module| module == &entry.module)
        })
        .cloned()
        .map(|policy| Candidate {
            policy,
            excluded_by: None,
        })
        .collect::<Vec<_>>();
    if let Some(module) = policy.runner.as_ref().filter(|_| entries.is_empty()) {
        entries.push(Candidate {
            policy: RunnerPolicy {
                module: module.clone(),
                harness: None,
                routes: None,
                tool_capabilities: Vec::new(),
                via_vault: false,
            },
            excluded_by: None,
        });
    }
    entries
}

fn describe_harnesses(
    runner: &EffectiveRunner,
    requested: Option<&str>,
    needs: &[cortexkit_role_routing::placement::HarnessCapability],
    explicit: &[cortexkit_role_routing::placement::HarnessCapability],
    remaining_filter: &str,
) -> Value {
    let harnesses = runner.task_harnesses(requested);
    if harnesses.is_empty() && (requested.is_some() || !explicit.is_empty()) {
        return json!({"reason":"harness_not_declared", "declared":runner.harnesses});
    }
    if !runner.starts_sessions {
        return json!({"reason":"harness_stopped"});
    }
    let mut blocked = BTreeMap::new();
    let mut running = 0;
    let mut invalid_limits = false;
    for harness in &harnesses {
        if let Some(declaration) = runner.wire_declaration() {
            match declaration.limits_for_harness(Some(harness)) {
                Ok(limits) if limits.starts_sessions == Some(false) => continue,
                Err(_) => invalid_limits = true,
                _ => {}
            }
        }
        running += 1;
        let lacking: Vec<_> = needs
            .iter()
            .filter(|need| !runner.supports_need(harness, need, explicit))
            .collect();
        if !lacking.is_empty() {
            blocked.insert(format!("{}/{harness}", runner.policy.module), lacking);
        }
    }
    if invalid_limits {
        json!({"reason":"excluded_by_policy", "filter":"invalid_harness_limits"})
    } else if running == 0 && !harnesses.is_empty() {
        json!({"reason":"harness_stopped"})
    } else if !blocked.is_empty() && blocked.len() == running {
        json!({"reason":"capabilities_missing", "missing":blocked})
    } else {
        json!({"reason":"excluded_by_policy", "filter":remaining_filter})
    }
}

fn warn_once(
    consumer: &str,
    project: Option<&str>,
    module: &str,
    candidates: &[Candidate],
    fallback: bool,
) {
    static REPORTED: Mutex<BTreeSet<(String, Option<String>)>> = Mutex::new(BTreeSet::new());
    if REPORTED
        .lock()
        .is_ok_and(|mut reported| reported.insert((consumer.into(), project.map(str::to_owned))))
    {
        let dropped: Vec<_> = candidates
            .iter()
            .map(|entry| (&entry.policy.module, &entry.policy.harness))
            .collect();
        let outcome = if fallback {
            "using globally enrolled declared runner"
        } else {
            "declared runner is not globally enrolled"
        };
        crate::warn_operator(format!("runner settings conflict with declared placement: consumer={consumer:?} project={project:?} declared_module={module:?} dropped={dropped:?}; {outcome}"));
    }
}
