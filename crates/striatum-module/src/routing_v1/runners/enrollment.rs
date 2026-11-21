//! Catalog enrollment is discovery; each outbound open must attest the same spawn.

use std::collections::BTreeMap;

use serde_json::Value;
use subc_client_rs::{RouteOpenTarget, RouteOpenTargetKind};

use crate::{broca_credentials::BrocaCredentials, config_policy::RunnerPolicy};

#[derive(Debug)]
pub(crate) enum DescribeError {
    Call(subc_client_rs::CallError),
    TargetChangedNeverSent,
}

impl std::fmt::Display for DescribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Call(error) => error.fmt(f),
            Self::TargetChangedNeverSent => {
                f.write_str("runner target changed: never_sent, retryable after catalog refresh")
            }
        }
    }
}

pub(crate) async fn describe(
    consumer: &subc_client_rs::SubcConsumer,
    module: &str,
    identity: subc_protocol::BindIdentity,
    body: Vec<u8>,
    options: subc_client_rs::CallOptions,
    settings: &[RunnerPolicy],
    credentials: &crate::SharedBrocaCredentials,
) -> Result<Vec<u8>, DescribeError> {
    let expected = credentials
        .lock()
        .expect("credentials lock")
        .first_party_runners
        .get(module)
        .cloned()
        .filter(|_| !settings.iter().any(|policy| policy.module == module));
    let target = subc_protocol::RouteTarget::ManagementSurface {
        module_id: module.to_owned(),
    };
    let Some(expected) = expected else {
        // Explicit settings keep the managed-call path, including its retry policy.
        return consumer
            .call(target, identity, body, options)
            .await
            .map_err(DescribeError::Call);
    };
    let handle = consumer
        .open_route(target, identity, options.clone())
        .await
        .map_err(DescribeError::Call)?;
    if !expected.accepts(consumer.route_target(&handle).as_ref()) {
        if let Ok(credentials) = credentials.lock() {
            credentials.runner_catalog_wake.notify_one();
        }
        crate::warn_operator(format!("runner call refused: module={module:?} never_sent=true retryable=true target_attestation_mismatch"));
        let _ = consumer.close_handle(&handle, Default::default()).await;
        return Err(DescribeError::TargetChangedNeverSent);
    }
    // A handle cannot reopen on another connection: no automatic retry may bypass
    // the target check when the daemon replaces the route or the module restarts.
    consumer
        .request(&handle, body, options)
        .await
        .map_err(DescribeError::Call)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    incarnation: Option<String>,
    spawn: Option<u64>,
}

impl Identity {
    pub(super) fn accepts(&self, target: Option<&RouteOpenTarget>) -> bool {
        target.is_some_and(|target| {
            target.first_party
                && target.kind == RouteOpenTargetKind::Spawn
                && self.incarnation.as_ref() == Some(&target.incarnation)
                && self.spawn.is_some()
                && self.spawn == target.spawn
        })
    }
}

pub(crate) fn policies(
    settings: &[RunnerPolicy],
    credentials: &BrocaCredentials,
) -> Vec<RunnerPolicy> {
    policies_at(settings, credentials, crate::now_ms() as i64)
}

pub(super) fn policies_at(
    settings: &[RunnerPolicy],
    credentials: &BrocaCredentials,
    now: i64,
) -> Vec<RunnerPolicy> {
    let mut policies = enrolled(settings, credentials);
    policies.retain(|policy| declaration_available_at(&policy.module, credentials, now));
    policies
}

/// An unavailable declaration affects selection, not whether a project may
/// name a module that the user already enrolled.
pub(crate) fn enrolled(
    settings: &[RunnerPolicy],
    credentials: &BrocaCredentials,
) -> Vec<RunnerPolicy> {
    let mut policies = settings.to_vec();
    for module in credentials.first_party_runners.keys() {
        if !settings.iter().any(|policy| policy.module == *module) {
            policies.push(RunnerPolicy {
                module: module.clone(),
                harness: None,
                routes: None,
                tool_capabilities: Vec::new(),
                via_vault: false,
            });
        }
    }
    policies
}

pub(super) fn declaration_available_at(
    module: &str,
    credentials: &BrocaCredentials,
    now: i64,
) -> bool {
    !credentials.runner_declarations_seen.contains(module)
        || (credentials.runner_declarations.contains_key(module)
            && (!credentials.runner_retained_declarations.contains(module)
                || super::describe_cache::fresh_observation(credentials, module, now)))
}

pub(super) fn replace_catalog(modules: &Value, credentials: &mut BrocaCredentials) {
    credentials.first_party_runners = modules
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let module = entry["module_id"].as_str()?;
            (entry["first_party"] == true && credentials.runner_declarations.contains_key(module))
                .then(|| {
                    (
                        module.to_owned(),
                        Identity {
                            incarnation: entry["incarnation"].as_str().map(str::to_owned),
                            spawn: entry["spawn"].as_u64(),
                        },
                    )
                })
        })
        .collect::<BTreeMap<_, _>>();
}
