//! `role.describe`: the role, its ops, the configured consumers and the
//! requirement members this provider reads.

use cortexkit_role_routing::describe::{ConsumerEntry, RoleDescribe};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use super::{operator_config, Deps};
use crate::select_policy;

/// The optional routing/v1 groups this provider serves, listed in
/// `role.describe` as `capabilities` and passed to `parse_select_request_serving`,
/// so the two always agree. Harnesses come from runner declarations.
/// Hold control remains unsupported: `hold_ok` and
/// `hold_wait_max_s` are still refused by name.
pub(super) const CAPABILITIES: &[&str] = &[cortexkit_role_routing::capabilities::SELECT_HARNESS];

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
    let describe = CAPABILITIES.iter().fold(
        RoleDescribe::new(consumers, true),
        |describe, capability| describe.with_capability(*capability),
    );
    let mut value = serde_json::to_value(describe)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    value["runners"] = serde_json::to_value(super::runners::effective(deps).entries)
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))?;
    Ok(value)
}
