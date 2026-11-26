use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use serde_json::{json, Value};

use super::{explicit_needs, task_needs, EffectiveRunner, Refusal, Registry, SelectRequest};
use crate::config_policy::{ConsumerPolicy, RunnerPolicy};

// A connected runner normally answers quickly, but a harness may still be
// loading its catalog. Don't make callers poll it every second while it starts.
const RUNNER_READY_RETRY_MS: u64 = 10_000;

pub(crate) fn empty_reachable(
    request: &SelectRequest,
    pin: Option<&str>,
    registry: &Registry,
    policies: &[RunnerPolicy],
    facts: &[&striatum_core::ModelFact],
    credentials: &crate::broca_credentials::BrocaCredentials,
    now: u64,
) -> Refusal {
    use super::super::ListStatus;
    use cortexkit_role_routing::errors::{NoRouteReason, PinUnavailableReason};
    use cortexkit_role_routing::select::PinFallback;

    let pin = pin.filter(|_| request.pin_fallback != Some(PinFallback::Route));
    let parsed_pin = pin.and_then(cortexkit_role_routing::select::RouteRef::parse_pin);
    let relevant: Vec<_> = facts
        .iter()
        .filter(|fact| {
            striatum_core::model_facts::llm_classification_rejection(fact, false).is_none()
                && parsed_pin.as_ref().is_none_or(|pin| {
                    pin.provider == fact.provider_id
                        && pin.model == fact.model_id
                        && pin.variant.as_ref().is_none_or(|variant| {
                            fact.variant
                                .as_ref()
                                .is_none_or(|fact_variant| variant == fact_variant)
                        })
                })
        })
        .map(|fact| (fact, striatum_core::fact_to_route(fact)))
        .collect();

    let mut rows = Vec::new();
    let mut any_served = false;
    let mut all_pending = true;
    for policy in policies {
        let Some(runner) = registry
            .get(policy)
            .filter(|runner| runner.wire_declaration().is_some())
        else {
            all_pending = false;
            continue;
        };
        let requested = request.harness.as_deref().or(policy.harness.as_deref());
        let selected = runner.selection_harness(requested);
        let harnesses = if let Some(choices) = &runner.harness_choices {
            choices
                .iter()
                .map(|candidate| Some(candidate.harness.as_str()))
                .collect()
        } else if let Some(harness) = selected.as_deref() {
            vec![Some(harness)]
        } else if runner.harnesses.is_empty() {
            vec![None]
        } else {
            runner
                .harnesses
                .iter()
                .map(|name| Some(name.as_str()))
                .collect()
        };
        let lists = credentials.runner_serves.get(&policy.module);
        let mut unavailable = Vec::new();
        for harness in harnesses {
            if runner.wire_declaration().is_some_and(|declaration| {
                declaration
                    .limits_for_harness(harness)
                    .is_ok_and(|limits| limits.starts_sessions == Some(false))
            }) {
                continue;
            }
            let mut allowed = relevant
                .iter()
                .filter(|(_, route)| {
                    runner
                        .route_policy
                        .as_ref()
                        .is_none_or(|specs| specs.resolve(&route.route_key, false))
                })
                .peekable();
            if allowed.peek().is_none() {
                continue;
            }
            let status = lists.map_or(ListStatus::Missing, |lists| {
                lists.status(harness, now as i64)
            });
            if matches!(status, ListStatus::Ready)
                && allowed.any(|(fact, _)| {
                    lists.is_some_and(|lists| {
                        lists.includes(harness, &fact.provider_id, &fact.model_id, now as i64)
                    })
                })
            {
                any_served = true;
                all_pending = false;
                continue;
            }
            all_pending &= matches!(status, ListStatus::Missing)
                && credentials
                    .runner_describe_connected
                    .contains(&policy.module);
            unavailable.push(harness_list_row(harness, status));
        }
        if !unavailable.is_empty() {
            let mut row = json!({"module":policy.module, "reason":"excluded_by_policy", "filter":"harness_routes", "unavailable_harnesses":unavailable});
            if let Some(harness) = requested {
                row["harness"] = json!(harness);
            }
            rows.push(row);
        }
    }

    let reset =
        (all_pending && !rows.is_empty()).then(|| now.saturating_add(RUNNER_READY_RETRY_MS));
    // The routing kit also treats two settled account states as unservable
    // pins: runner_serves_login_none before the list waives login on an empty
    // vault, and runner_gateway_credential when its vault alias has no unique
    // target. A still-loading or stale vault is not either case.
    let confirmed_no_login = any_served && credentials.listed_credentials().is_empty();
    let unresolved_gateway = parsed_pin.as_ref().is_some_and(|pin| {
        relevant.is_empty()
            && credentials.listed_credentials().iter().any(|row| {
                row.state == "active"
                    && row.auth_method.as_deref() == Some("gateway")
                    && row.provider_ids.contains(&pin.provider)
                    && row.serves.len() != 1
            })
    });
    let account_cannot_serve =
        credentials.listing_is_fresh(now) && (confirmed_no_login || unresolved_gateway);
    let mut refusal = if let Some(pin) = pin.filter(|_| {
        parsed_pin.is_some() && ((!any_served && !rows.is_empty()) || account_cannot_serve)
    }) {
        Refusal::pin_unavailable(pin, PinUnavailableReason::NotAvailable, reset)
    } else {
        Refusal::no_route(NoRouteReason::MissingCapability, reset)
    };
    if !rows.is_empty() {
        refusal.detail.insert("runners".into(), json!(rows));
    }
    refusal
}

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
        let message = if rows
            .iter()
            .all(|row| row["reason"] == "capabilities_missing")
        {
            capability_message(&rows)
        } else if !rows.is_empty()
            && rows.iter().all(|row| {
                row["reason"] == "excluded_by_policy" && row["filter"] == "harness_routes"
            })
        {
            self.model_route_message(request, registry)
        } else {
            None
        };
        if let Some(message) = message {
            refusal.message = message;
        }
        refusal.detail.insert("runners".into(), json!(rows));
        refusal
    }

    fn model_route_message(&self, request: &SelectRequest, registry: &Registry) -> Option<String> {
        let mut pairs = BTreeSet::new();
        for entry in &self.candidates {
            let policy = &entry.policy;
            let runner = registry.get(policy)?;
            if request
                .requirements
                .tool_capabilities
                .as_ref()
                .is_some_and(|tags| {
                    tags.iter()
                        .any(|tag| !runner.policy.tool_capabilities.contains(tag))
                })
            {
                return None;
            }
            let requested = request.harness.as_deref().or(policy.harness.as_deref());
            if let Some(choices) = &runner.harness_choices {
                // A runner's row can report no matching models even when some
                // of its harnesses were stopped or lacked required capabilities.
                if choices.len() != runner.task_harnesses(requested).len() {
                    return None;
                }
                pairs.extend(
                    choices
                        .iter()
                        .map(|choice| (&policy.module, Some(choice.harness.clone()))),
                );
            } else {
                pairs.insert((&policy.module, runner.selection_harness(requested)));
            }
        }
        let subject = "No model this request may use runs on";
        if let (1, Some((module, Some(harness)))) = (pairs.len(), pairs.first()) {
            Some(format!("{subject} {module}/{harness}"))
        } else {
            Some(format!("{subject} any eligible runner/harness pair"))
        }
    }
}

fn capability_message(rows: &[Value]) -> Option<String> {
    let gaps: BTreeSet<_> = rows
        .iter()
        .filter_map(|row| row["missing"].as_object())
        .flatten()
        .filter_map(|(pair, missing)| {
            let names: Vec<_> = missing
                .as_array()?
                .iter()
                .filter_map(Value::as_str)
                .collect();
            (!names.is_empty()).then(|| format!("{pair} lacks {}", names.join(", ")))
        })
        .collect();
    (!gaps.is_empty()).then(|| gaps.into_iter().collect::<Vec<_>>().join("; "))
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
    } else if remaining_filter == "harness_routes" {
        let declarations = if harnesses.is_empty() {
            vec![None]
        } else {
            harnesses.iter().copied().map(Some).collect()
        };
        let unavailable: Vec<_> = declarations
            .into_iter()
            .filter(|harness| {
                harness.is_none_or(|name| {
                    !blocked.contains_key(&format!("{}/{name}", runner.policy.module))
                        && runner.wire_declaration().is_none_or(|d| {
                            d.limits_for_harness(Some(name))
                                .is_ok_and(|limits| limits.starts_sessions == Some(true))
                        })
                })
            })
            .map(|harness| {
                harness_list_row(
                    harness,
                    runner
                        .serves_status
                        .get(&harness.map(str::to_owned))
                        .copied()
                        .unwrap_or(super::super::ListStatus::Missing),
                )
            })
            .collect();
        json!({"reason":"excluded_by_policy", "filter":remaining_filter, "unavailable_harnesses":unavailable})
    } else {
        json!({"reason":"excluded_by_policy", "filter":remaining_filter})
    }
}

fn harness_list_row(harness: Option<&str>, status: super::super::ListStatus) -> Value {
    use super::super::ListStatus;
    let (reason, message) = match status {
        ListStatus::Ready => (
            "model_not_served",
            "The model isn't in this harness's serves list.",
        ),
        ListStatus::Expired => (
            "serves_list_expired",
            "This harness's last serves list is over five minutes old.",
        ),
        ListStatus::Missing => (
            "serves_list_missing",
            "The runner hasn't sent a serves list for this harness yet.",
        ),
    };
    let mut row = json!({"reason":reason, "message":message});
    if let Some(harness) = harness {
        row["harness"] = json!(harness);
    }
    row
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
