//! `role.describe`: the role, its ops, the configured consumers and the
//! requirement members this provider reads.

use cortexkit_role_routing::capabilities::{SELECT_HARNESS_CAPABILITIES, SELECT_WORKLOAD};
use cortexkit_role_routing::describe::{ConsumerEntry, RoleDescribe};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use super::{operator_config, Deps};

const ACCOUNT_CONTINUE_RESUME: &str = "account_continue_resume";

// Keep callers from sending explicit needs until every harness currently
// running workers declares submit_tool, final_text and plans.
pub(super) const ADVERTISE_SELECT_HARNESS_CAPABILITIES: bool = false;

/// The optional routing/v1 groups this provider serves, listed in
/// `role.describe` as `capabilities` and passed to the select and account request
/// parsers, so declarations and accepted members agree. Harnesses come from
/// runner declarations.
pub(super) const CAPABILITIES: &[&str] = if ADVERTISE_SELECT_HARNESS_CAPABILITIES {
    ALL_CAPABILITIES
} else {
    ALL_CAPABILITIES.split_first().unwrap().1
};

const ALL_CAPABILITIES: &[&str] = &[
    // Keep the rollout-gated group first so CAPABILITIES can omit it.
    SELECT_HARNESS_CAPABILITIES,
    cortexkit_role_routing::capabilities::CONSUMER_DECLARATIONS,
    cortexkit_role_routing::capabilities::CONSUMER_PRIORITY,
    SELECT_WORKLOAD,
    cortexkit_role_routing::capabilities::SELECT_KIND,
    cortexkit_role_routing::capabilities::SELECT_MEDIA,
    cortexkit_role_routing::capabilities::SELECT_SEARCH,
    "select_engine",
    "engine_reports",
    "engine_evals",
    "select_panel",
    cortexkit_role_routing::capabilities::SELECT_PANEL_ESTIMATE,
    "consumer_models",
    "explain",
    "budget_quote",
    "account_status",
    "head_choices",
    "report_outcomes",
    cortexkit_role_routing::capabilities::REPORT_ATTEMPT_OUTCOMES,
    "report_usage_run_id",
    "select_pin_slot",
    cortexkit_role_routing::capabilities::SELECT_PIN_ORIGIN,
    "select_available_routes",
    cortexkit_role_routing::capabilities::SELECT_ELIGIBLE_ROUTES,
    "select_prefer_prompt_cache",
    "select_settings",
    cortexkit_role_routing::capabilities::SELECT_PRICE,
    "observe_backoff",
    "observe_credential_refused",
    cortexkit_role_routing::capabilities::ACCOUNT_OBSERVE_SUCCESS,
    cortexkit_role_routing::capabilities::SELECT_HARNESS,
    cortexkit_role_routing::capabilities::RUNNER_HARNESS_FACTS,
    cortexkit_role_routing::capabilities::DECISION_HARNESS,
    "select_hold_control",
    cortexkit_role_routing::capabilities::SELECT_PREFER,
    cortexkit_role_routing::capabilities::SELECT_EXCLUDE_PROVIDERS,
    cortexkit_role_routing::capabilities::SELECT_TASK_REF,
    "service_tier",
    "service_tier_settings",
    cortexkit_role_routing::capabilities::ACCOUNT_CONTINUE,
    cortexkit_role_routing::capabilities::ACCOUNT_CONTINUE_SIGNED_OUT,
    cortexkit_role_routing::capabilities::ACCOUNT_WARM_CREDENTIAL,
    cortexkit_role_routing::capabilities::ACCOUNT_HEAD_TURN,
    cortexkit_role_routing::capabilities::ACCOUNT_FORK_OF,
    ACCOUNT_CONTINUE_RESUME,
];

/// Advertisement and serving validation share the same list. Production always
/// borrows the advertised groups; candidate readers can only be enabled by tests.
pub(crate) fn capabilities() -> std::borrow::Cow<'static, [&'static str]> {
    #[cfg(test)]
    if let Ok(candidate) = CANDIDATE_CAPABILITIES.try_with(Clone::clone) {
        return std::borrow::Cow::Owned(candidate);
    }
    std::borrow::Cow::Borrowed(CAPABILITIES)
}

#[cfg(test)]
tokio::task_local! {
    static CANDIDATE_CAPABILITIES: Vec<&'static str>;
}

/// A task-local scope cannot change another request's groups, even when the
/// requests run on the same thread. Dropping the future also drops the override.
#[cfg(test)]
pub(super) async fn with_candidate_capabilities<T>(
    groups: &[&'static str],
    future: impl std::future::Future<Output = T>,
) -> T {
    let mut candidate = capabilities().into_owned();
    for group in groups {
        if !candidate.contains(group) {
            candidate.push(group);
        }
    }
    CANDIDATE_CAPABILITIES.scope(candidate, future).await
}

#[cfg(test)]
pub(super) async fn without_price<T>(future: impl std::future::Future<Output = T>) -> T {
    without_capability(cortexkit_role_routing::capabilities::SELECT_PRICE, future).await
}

/// Serve every advertised group except `group`, to pin what a router that
/// doesn't serve it answers.
#[cfg(test)]
pub(super) async fn without_capability<T>(
    group: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    let groups = capabilities()
        .iter()
        .copied()
        .filter(|served| *served != group)
        .collect();
    CANDIDATE_CAPABILITIES.scope(groups, future).await
}

/// Capture the test request's scope before dispatch moves it to another task
/// or the blocking pool. Unrelated requests keep their own capability list.
#[cfg(test)]
pub(crate) fn inherit_capabilities<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let candidate = CANDIDATE_CAPABILITIES.try_with(Clone::clone).ok();
    let future =
        crate::fusiform_catalog::inherit_calls(super::engines::grade_observer::inherit(future));
    async move {
        match candidate {
            Some(candidate) => CANDIDATE_CAPABILITIES.scope(candidate, future).await,
            None => future.await,
        }
    }
}

/// Params are ignored: the role defines `{}` and no refusal for this op.
pub(super) fn describe(deps: &Deps<'_>) -> Result<Value, Refusal> {
    let config = operator_config(deps);
    let registry = super::consumers::effective(&config);
    let consumers = registry
        .names()
        .into_iter()
        .map(|id| ConsumerEntry {
            // The contract flag describes a restriction, not whether a route
            // happens to have prices. Only explicit consumer policy sets it.
            priced_only: config
                .consumer_policy(&id, &Default::default(), &registry.policies)
                .pay_per_token
                .unwrap_or(false),
            id,
        })
        .collect();
    // A `detail: true` select is answered with the selection's explanation
    // lines, rejections and score.
    let mut capabilities = capabilities();
    if deps
        .quota_cache
        .lock()
        .ok()
        .is_some_and(|cache| cache.consumer.route_facts.ready(cache.consumer.attached))
    {
        capabilities
            .to_mut()
            .push(cortexkit_role_routing::capabilities::ROUTE_FACT_STREAM);
    }
    let mut describe = capabilities.iter().fold(
        RoleDescribe::new(consumers, true),
        |describe, capability| describe.with_capability(*capability),
    );
    if capabilities.contains(&cortexkit_role_routing::capabilities::SELECT_MEDIA) {
        // Advertise the handler's fixed wire kinds, even before any engine is
        // declared. Availability affects selection, not the supported protocol.
        describe.kinds = super::engines::MEDIA_KINDS
            .iter()
            .map(|kind| cortexkit_role_routing::placement::Kind::parse(kind))
            .collect();
        if capabilities.contains(&"select_search") {
            describe
                .kinds
                .push(cortexkit_role_routing::placement::Kind::Search);
        }
    }
    for &(op, group) in cortexkit_role_routing::capabilities::OP_GROUPS {
        if capabilities.contains(&group) && !describe.ops.iter().any(|name| name == op) {
            describe.ops.push(op.into());
        }
    }
    let mut value = serde_json::to_value(describe)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    if let Some(fields) = value["requirement_fields"].as_array_mut() {
        fields.extend(
            cortexkit_role_routing::select::OPTIONAL_REQUIREMENT_FIELDS
                .iter()
                .copied()
                .filter(|field| match *field {
                    "workload" => capabilities.contains(&SELECT_WORKLOAD),
                    "harness_capabilities" => capabilities.contains(&SELECT_HARNESS_CAPABILITIES),
                    _ => true,
                })
                .map(|field| Value::String(field.into())),
        );
        if capabilities.contains(&"select_media") {
            fields.extend(
                cortexkit_role_routing::select_media::REQUIREMENT_FIELDS
                    .iter()
                    .map(|field| Value::String((*field).into())),
            );
        }
    }
    // Describe lists runner modules, not placements. Retain the first entry's
    // policy when several placements name the same module.
    let mut seen = std::collections::HashSet::new();
    let runners: Vec<_> = super::runners::effective(deps)
        .entries
        .into_iter()
        .filter(|runner| seen.insert(runner.policy.module.clone()))
        .map(|mut runner| {
            runner.policy.harness = None;
            runner
        })
        .collect();
    value["runners"] = serde_json::to_value(runners)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    value["account_policies"] = config.effective_account_policies();
    Ok(value)
}
