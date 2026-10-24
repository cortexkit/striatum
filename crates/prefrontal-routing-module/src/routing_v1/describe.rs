//! `role.describe`: the role, its ops, the configured consumers and the
//! requirement members this provider reads.

use cortexkit_role_routing::describe::{ConsumerEntry, RoleDescribe};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use super::{operator_config, Deps};
use crate::select_policy;

/// Params are ignored: the role defines `{}` and no refusal for this op.
pub(super) fn describe(deps: &Deps<'_>) -> Result<Value, Refusal> {
    let consumers = operator_config(deps)
        .v1_consumers()
        .into_iter()
        .map(|id| ConsumerEntry {
            // The same rule selection applies to the request class the id
            // names: codemode and flow run only priced per token.
            priced_only: select_policy::requires_metered(select_policy::Consumer::from_id(&id)),
            id,
        })
        .collect();
    // A `detail: true` select is answered with the selection's explanation
    // lines, rejections and score.
    serde_json::to_value(RoleDescribe::new(consumers, true))
        .map_err(|error| super::provider_refusal("internal_error", error.to_string()))
}
