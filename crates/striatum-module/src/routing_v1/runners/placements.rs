//! Validate harness-bound entries when settings or runner declarations change.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};

use super::RunnerDeclaration;
use crate::config_policy::RunnerPolicy;

pub(crate) fn compile(routes: &[String]) -> striatum_core::provider_model_spec::CompiledSpecs {
    #[cfg(test)]
    PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    striatum_core::provider_model_spec::CompiledSpecs::compile(routes)
}

#[cfg(test)]
pub(super) static PARSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq)]
enum Status {
    Ready,
    Pending,
    Refused,
}

fn status(policy: &RunnerPolicy, declaration: Option<&RunnerDeclaration>) -> Status {
    let Some(harness) = policy.harness.as_deref() else {
        return Status::Ready;
    };
    let Some(declaration) =
        declaration.filter(|d| d.harnesses.as_ref().is_some_and(|h| !h.is_empty()))
    else {
        return Status::Pending;
    };
    if !declaration
        .harnesses
        .as_ref()
        .unwrap()
        .iter()
        .any(|h| h == harness)
        || declaration
            .limits_for_harness(Some(harness))
            .is_ok_and(|limits| limits.starts_sessions == Some(false))
    {
        return Status::Refused;
    }
    Status::Ready
}

pub(crate) fn usable(policy: &RunnerPolicy, declaration: Option<&RunnerDeclaration>) -> bool {
    status(policy, declaration) == Status::Ready
}

#[derive(Default)]
pub(super) struct Monitor {
    policies: Vec<RunnerPolicy>,
    routes: Vec<Option<striatum_core::provider_model_spec::CompiledSpecs>>,
    declarations: BTreeMap<String, RunnerDeclaration>,
    warned: HashSet<String>,
}

impl Monitor {
    pub(super) fn settings(&mut self, policies: Vec<RunnerPolicy>, report: impl FnMut(String)) {
        self.routes = policies
            .iter()
            .map(|policy| policy.routes.as_deref().map(compile))
            .collect();
        self.policies = policies;
        self.check(report);
    }

    pub(super) fn declarations(
        &mut self,
        declarations: &BTreeMap<String, RunnerDeclaration>,
        report: impl FnMut(String),
    ) {
        if self.declarations != *declarations {
            self.declarations = declarations.clone();
            self.check(report);
        }
    }
    fn check(&mut self, mut report: impl FnMut(String)) {
        for (policy, routes) in self.policies.iter().zip(&self.routes) {
            let Some(harness) = &policy.harness else {
                continue;
            };
            let declaration = self.declarations.get(&policy.module);
            let issue = match status(policy, declaration) {
                Status::Pending => Some("kept but skipped until the runner declares harnesses"),
                Status::Refused => {
                    Some("refused: harness is not hosted or does not start sessions")
                }
                Status::Ready => declaration
                    .filter(|declaration| {
                        policy.routes.as_ref().is_some_and(|routes| {
                            !routes.is_empty()
                                && routes
                                    .iter()
                                    .all(|route| declaration.harness_excludes(harness, route))
                        }) && declaration
                            .limits_for_harness(Some(harness))
                            .ok()
                            .and_then(|limits| limits.exclude)
                            // Only an exception inside this placement can reopen
                            // one of its routes; unrelated exceptions do not.
                            .is_some_and(|specs| {
                                !specs
                                    .iter()
                                    .filter_map(|spec| spec.trim().strip_prefix('-'))
                                    .any(|route| {
                                        routes
                                            .as_ref()
                                            .is_some_and(|routes| routes.resolve(route, false))
                                            && !declaration.harness_excludes(harness, route)
                                    })
                            })
                    })
                    .map(|_| {
                        "all configured routes are excluded; kept because the catalog can change"
                    }),
            };
            if let Some(issue) = issue {
                let warning = format!(
                    "[striatum] settings runner {:?} harness {harness:?}: {issue}",
                    policy.module
                );
                if self.warned.insert(warning.clone()) {
                    report(warning);
                }
            }
        }
    }
}

fn monitor() -> &'static Mutex<Monitor> {
    static MONITOR: OnceLock<Mutex<Monitor>> = OnceLock::new();
    MONITOR.get_or_init(Default::default)
}

pub(crate) fn settings_loaded(policies: Vec<RunnerPolicy>) {
    let mut monitor = monitor().lock().unwrap();
    monitor.settings(policies, crate::warn_operator);
}

pub(super) fn declarations_changed(declarations: &BTreeMap<String, RunnerDeclaration>) {
    let mut monitor = monitor().lock().unwrap();
    monitor.declarations(declarations, crate::warn_operator);
}
