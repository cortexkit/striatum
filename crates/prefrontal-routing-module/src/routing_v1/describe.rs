//! `role.describe`: the role, its ops, the configured consumers and the
//! requirement members this provider reads.

use cortexkit_role_routing::describe::{ConsumerEntry, RoleDescribe};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use super::{operator_config, Deps};

/// Params are ignored: the role defines `{}` and no refusal for this op.
pub(super) fn describe(deps: &Deps<'_>) -> Result<Value, Refusal> {
    let consumers = operator_config(deps)
        .v1_consumers()
        .into_iter()
        .map(|(id, policy)| ConsumerEntry {
            id,
            priced_only: policy.priced_only,
        })
        .collect();
    // A `detail: true` select is answered with the selection's explanation
    // lines, rejections and score.
    serde_json::to_value(RoleDescribe::new(consumers, true))
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))
}
