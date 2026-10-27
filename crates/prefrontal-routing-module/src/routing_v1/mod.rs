//! The `routing/v1` role, served beside the routing module's older ops.
//!
//! The contract is `CONTRACT.md` in the `cortexkit-role-routing` crate, whose
//! types and validators this module uses for every request and answer. It
//! serves `role.describe`, `route.select`, `route.report`,
//! `route.account_for` and `route.account_observe`. Three of those names
//! were already served with older shapes that core and Broca still send, so
//! [`claims`] decides which surface answers a request on those names; see it
//! for the rule.
//!
//! Every v1 op reads its caller from the route: the principal the daemon
//! attested when it bound the route, and the project the route's scope stamp
//! names. No request member is ever read as the caller or the scope.

use std::path::Path;

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::ops;
use serde_json::{Map, Value};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::{
    account_selection, send, ModuleError, SharedBrocaCredentials, SharedQuotaCache, SharedStore,
};

mod accounts;
pub(crate) mod consumers;
mod describe;
mod report;
pub(crate) mod runners;
mod select;

#[cfg(test)]
mod account_classes_tests;
#[cfg(test)]
mod account_ops_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod consumers_tests;
#[cfg(test)]
mod diagnostics_tests;
#[cfg(test)]
mod dispatch_tests;
#[cfg(test)]
mod key_pool_tests;
#[cfg(test)]
mod metered_tests;
#[cfg(test)]
mod order_tests;
#[cfg(test)]
mod report_fence_tests;
#[cfg(test)]
mod runners_tests;
#[cfg(test)]
mod scope_attribution_tests;
#[cfg(test)]
mod select_options_tests;

/// The `routing/v1` capability a routing provider lists in its manifest.
pub(crate) const PROVIDES: &str = cortexkit_role_routing::PROVIDES;

/// The ops only `routing/v1` serves. The other three v1 ops share their
/// names with older ops and are listed with those.
pub(crate) const V1_ONLY_OPS: &[&str] = &[ops::ROLE_DESCRIBE, ops::ROUTE_REPORT];

/// Whether the `routing/v1` surface answers this request body.
///
/// `role.describe` and `route.report` exist only in v1. `route.select`,
/// `route.account_for` and `route.account_observe` are also the names of
/// older ops that core and Broca still call, and each older shape carries a
/// member v1 never defines: an older `route.select` always names its
/// `targetAgent`, and the older account ops always name their `caller`. A
/// body carrying that member is answered by the older op exactly as before;
/// any other body on those names is a v1 request, and the v1 surface refuses
/// a member it does not define by name, so an older body that left out its
/// marker fails loudly instead of being misread.
///
/// The older branch exists only until core and Broca send v1; it is deleted
/// then, and these names belong to v1 alone.
pub(crate) fn claims(body: &[u8]) -> bool {
    let Ok(Value::Object(request)) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return false;
    };
    let carries = |member: &str| {
        request
            .get("params")
            .and_then(Value::as_object)
            .is_some_and(|params| params.contains_key(member))
    };
    match method {
        ops::ROLE_DESCRIBE | ops::ROUTE_REPORT => true,
        ops::ROUTE_SELECT => !carries("targetAgent"),
        ops::ROUTE_ACCOUNT_FOR | ops::ROUTE_ACCOUNT_OBSERVE => !carries("caller"),
        _ => false,
    }
}

/// Everything a v1 op reads besides its request and its caller.
pub(crate) struct Deps<'a> {
    pub quota_cache: &'a SharedQuotaCache,
    pub store: &'a SharedStore,
    pub routing_config_path: &'a Path,
    pub operator_config_path: &'a Path,
    pub broca_credentials: &'a SharedBrocaCredentials,
}

/// The caller of one request, read from its route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Caller {
    /// The route binding is rechecked at admission, after any queued work.
    pub binding: Option<(u16, u32)>,
    /// `reserved:<module>`, `direct`, `unverified` or `unattested`.
    pub principal: String,
    /// The scope the route is bound under; `None` for an unscoped route.
    pub scope: Option<crate::DecisionScope>,
    /// Authority from the daemon's scope stamp, independent of ledger metadata.
    pub scope_authority: Option<ScopeAuthority>,
}

/// Retain daemon-stamped authorization for consumer admission even when routing
/// receives no project or workdir metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeAuthority {
    pub owner: String,
    pub reference: String,
    pub kind: subc_protocol::scope::ScopeKind,
    pub scope_epoch: u64,
    pub project_id: Option<String>,
    pub owner_authorized: bool,
    /// The flow the scope belongs to, when the daemon stamped one: the route
    /// carries a flow step, never its owner agent.
    pub flow_id: Option<String>,
    /// The agent run the scope belongs to, when the daemon stamped one.
    pub run_id: Option<String>,
}

impl Caller {
    pub(crate) fn for_route(channel: u16, epoch: u32) -> Self {
        Self {
            binding: Some((channel, epoch)),
            principal: crate::route_principal(channel, epoch),
            scope: crate::route_scope(channel, epoch),
            scope_authority: crate::route_scope_authority(channel, epoch),
        }
    }

    pub(crate) fn binding_is_current(&self) -> bool {
        self.binding.is_some_and(|(channel, epoch)| {
            // A missing route and a real unattested caller share a principal
            // label, so require a stored binding with the matching epoch.
            crate::route_principals()
                .lock()
                .ok()
                .is_some_and(|principals| {
                    principals.get(&channel).is_some_and(|(bound, principal)| {
                        *bound == epoch && principal == &self.principal
                    })
                })
                && crate::route_scope_authority(channel, epoch) == self.scope_authority
        })
    }

    /// The module the daemon verified this caller to be, if it did.
    fn attested_module(&self) -> Option<&str> {
        self.principal
            .strip_prefix("reserved:")
            .filter(|module| !module.is_empty())
    }
}

/// Answer one v1 request on its route and send the reply frame.
pub(crate) async fn serve(
    frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    deps: &Deps<'_>,
) -> Result<(), ModuleError> {
    let (ver, channel, corr) = (frame.header.ver, frame.header.channel, frame.header.corr);
    let caller = Caller::for_route(channel, route_epoch);
    let reply = match serde_json::from_slice::<Value>(&frame.body) {
        Ok(request) => {
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let params = request.get("params").cloned().unwrap_or(Value::Null);
            answer(&method, &params, &caller, deps).await
        }
        Err(error) => Err(Refusal::invalid_request(
            "params",
            format!("the request body is not JSON: {error}"),
        )),
    };
    let (ty, body) = match reply {
        Ok(answer) => (
            FrameType::Response,
            cortexkit_role_routing::wire::reply_body(&answer).map_err(ModuleError::Json)?,
        ),
        Err(refusal) => (
            FrameType::Error,
            serde_json::to_vec(&ErrorBody {
                code: refusal.code.as_str().to_owned(),
                message: refusal.message,
                detail: Some(Value::Object(refusal.detail)),
            })
            .map_err(ModuleError::Json)?,
        ),
    };
    let reply = Frame::build_with_version(
        ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        channel,
        route_epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, reply).await
}

async fn answer(
    method: &str,
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    match method {
        ops::ROLE_DESCRIBE => describe::describe(deps),
        ops::ROUTE_SELECT => select::select(params, caller, deps).await,
        ops::ROUTE_REPORT => report::report(params, caller, deps),
        ops::ROUTE_ACCOUNT_FOR => accounts::account_for(params, caller, deps),
        ops::ROUTE_ACCOUNT_OBSERVE => accounts::account_observe(params, caller, deps),
        other => Err(Refusal::new(
            RefusalCode::parse("unknown_method"),
            format!("unknown method '{other}'"),
        )),
    }
}

/// Refuse a request member the op does not define, naming it. The published
/// validators read only the members they know; v1 refuses the rest so a body
/// shaped for an older op is never half-read.
fn refuse_unknown_members(params: &Value, known: &[&str]) -> Result<(), Refusal> {
    let Value::Object(members) = params else {
        return Err(Refusal::invalid_request(
            "params",
            "the request params must be an object",
        ));
    };
    match members
        .keys()
        .filter(|member| !known.contains(&member.as_str()))
        .min()
    {
        Some(unknown) => Err(Refusal::invalid_request(
            unknown.clone(),
            format!("{unknown} is not a member of this request"),
        )),
        None => Ok(()),
    }
}

/// A refusal under a code the role leaves open: routing's own conditions
/// such as a store failure. A reader treats it as untimed.
fn provider_refusal(code: &str, message: impl Into<String>) -> Refusal {
    Refusal {
        code: RefusalCode::parse(code),
        message: message.into(),
        detail: Map::new(),
    }
}

fn store_refusal(error: impl std::fmt::Display) -> Refusal {
    provider_refusal("store_error", format!("route-state store: {error}"))
}

/// The account-selection context the runner ops and pin admission read.
fn account_ctx<'a>(
    deps: &'a Deps<'a>,
    flags: crate::config_overrides::AccountSelectionFlags,
    policy: &'a prefrontal_routing_core::accounts::AllocatorPolicy,
) -> Result<account_selection::Ctx<'a>, Refusal> {
    Ok(account_selection::Ctx {
        store: deps.store.as_ref(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags,
        policy,
        now_ms: now_ms() as i64,
        classes: Some(
            account_selection::ClassContext::load(
                deps.store,
                deps.broca_credentials,
                operator_config(deps),
                crate::ACCOUNT_QUOTA.get(),
                now_ms() as i64,
            )
            .map_err(store_refusal)?,
        ),
    })
}

/// The routing configuration (`model_routing`) the v1 ops read.
fn operator_config(deps: &Deps<'_>) -> std::sync::Arc<crate::config_overrides::LiveConfig> {
    crate::config_overrides::live_config(deps.operator_config_path)
}

fn now_ms() -> u64 {
    crate::now_ms()
}
