//! `role.describe`: the role, its ops, the configured consumers and the
//! requirement members this provider reads.

use cortexkit_role_routing::describe::{ConsumerEntry, RoleDescribe};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use super::{operator_config, Deps};
use crate::select_policy;

/// The optional routing/v1 groups this provider serves, listed in
/// `role.describe` as `capabilities` and passed to the select and account request
/// parsers, so declarations and accepted members agree. Harnesses come from
/// runner declarations.
pub(super) const CAPABILITIES: &[&str] = &[
    "select_panel",
    "consumer_models",
    "explain",
    "budget_quote",
    "account_status",
    "head_choices",
    "report_outcomes",
    "report_usage_run_id",
    "select_pin_slot",
    "select_available_routes",
    "select_prefer_prompt_cache",
    "select_settings",
    "observe_backoff",
    "observe_credential_refused",
    cortexkit_role_routing::capabilities::SELECT_HARNESS,
    "select_hold_control",
    cortexkit_role_routing::capabilities::SELECT_PREFER,
    cortexkit_role_routing::capabilities::SELECT_EXCLUDE_PROVIDERS,
    cortexkit_role_routing::capabilities::SELECT_TASK_REF,
    "service_tier",
    "service_tier_settings",
    cortexkit_role_routing::capabilities::ACCOUNT_CONTINUE,
];

/// Params are ignored: the role defines `{}` and no refusal for this op.
pub(super) fn describe(deps: &Deps<'_>) -> Result<Value, Refusal> {
    let config = operator_config(deps);
    let registry = super::consumers::effective(&config);
    let consumers = registry
        .names()
        .into_iter()
        .map(|id| ConsumerEntry {
            // The same rule selection applies to the request class the id
            // names: codemode and flow run only priced per token.
            priced_only: config
                .consumer_policy(&id, &Default::default(), &registry.policies)
                .pay_per_token
                .unwrap_or_else(|| {
                    select_policy::requires_metered(select_policy::Consumer::from_id(&id))
                }),
            id,
        })
        .collect();
    // A `detail: true` select is answered with the selection's explanation
    // lines, rejections and score.
    let mut describe = CAPABILITIES.iter().fold(
        RoleDescribe::new(consumers, true),
        |describe, capability| describe.with_capability(*capability),
    );
    for &(op, group) in cortexkit_role_routing::capabilities::OP_GROUPS {
        if CAPABILITIES.contains(&group) && !describe.ops.iter().any(|name| name == op) {
            describe.ops.push(op.into());
        }
    }
    let mut value = serde_json::to_value(describe)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    if let Some(fields) = value["requirement_fields"].as_array_mut() {
        fields.extend(
            cortexkit_role_routing::select::OPTIONAL_REQUIREMENT_FIELDS
                .iter()
                .map(|field| Value::String((*field).into())),
        );
    }
    value["runners"] = serde_json::to_value(super::runners::effective(deps).entries)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    value["account_policies"] = config.effective_account_policies();
    Ok(value)
}
