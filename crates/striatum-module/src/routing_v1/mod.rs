//! The `routing/v1` role, served beside the routing module's older ops.
//!
//! The contract is `CONTRACT.md` in the `cortexkit-role-routing` crate, whose
//! types and validators this module uses for every request and answer. It
//! serves selection, reporting, account operations and public reads. Several
//! names also have older shapes that core and Broca still send, so [`claims`]
//! uses the per-operation table in `shapes` to choose their surface.
//!
//! Every v1 op reads its caller from the route: the principal the daemon
//! attested when it bound the route, and the project the route's scope stamp
//! names. No request member is ever read as the caller or the scope.
//!
//! Credential-refusal observations block the credential for a documented
//! recheck horizon of at most one hour. A later refusal renews that horizon without
//! shortening an existing block. An operator can clear it early with
//! `route.clear_cooldown`.

use std::path::Path;

use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::ops;
use serde_json::{Map, Value};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::{
    account_selection, send, ModuleError, SharedBrocaCredentials, SharedQuotaCache, SharedStore,
};

pub(crate) mod accounts;
pub(crate) mod classifier;
#[cfg(test)]
mod classifier_tests;
pub(crate) mod consumers;
pub(crate) mod describe;
pub(crate) mod effort_preview;
#[cfg(test)]
pub(crate) use describe::inherit_capabilities;
mod engines;
#[cfg(test)]
mod engines_tests;
#[cfg(test)]
#[path = "native_fixture_sources/tests.rs"]
mod native_fixture_sources;
mod panels;
#[cfg(test)]
mod panels_estimate_tests;
#[cfg(test)]
mod panels_tests;
mod reads;
pub(crate) mod report;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(crate) mod request_cpu_bench;
pub(crate) mod runners;
#[cfg(test)]
mod search_tests;
mod select;
#[cfg(test)]
mod service_tier_tests;
mod shapes;
#[cfg(test)]
mod subscription_incident_tests;
mod what_if;
#[cfg(test)]
mod what_if_tests;

/// Quota headroom can return before a provider reset. A known reset schedules
/// an extra check, but must not postpone the regular quota recheck.
fn quota_retry_hint(reset: Option<u64>, now: u64) -> Option<u64> {
    reset.map(|reset| {
        reset.min(now.saturating_add(striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS as u64))
    })
}

#[cfg(test)]
mod account_classes_tests;
#[cfg(test)]
mod account_ops_tests;
#[cfg(test)]
mod account_refusal_codes_tests;
#[cfg(test)]
mod add_shapes_tests;
#[cfg(test)]
mod broca_output_cap_harness_1124_tests;
#[cfg(test)]
mod concurrency_samples_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod consumers_tests;
#[cfg(test)]
mod core_wire_fixture_tests;
#[cfg(test)]
mod credential_startup_tests;
#[cfg(test)]
mod diagnostics_tests;
#[cfg(test)]
mod dispatch_tests;
#[cfg(test)]
mod gateway_tests;
#[cfg(test)]
#[path = "../head_choices_tests.rs"]
mod head_choices_tests;
#[cfg(test)]
mod key_pool_tests;
#[cfg(test)]
mod metered_tests;
#[cfg(test)]
mod order_tests;
#[cfg(test)]
mod prefrontal_rename_tests;
#[cfg(test)]
mod profile_tests;
#[cfg(test)]
pub(crate) mod replay_tests;
#[cfg(test)]
mod report_delivery_tests;

/// Reuse real-handler contract tests in a child process with production logging.
#[cfg(test)]
pub(crate) fn exercise_normal_traffic_for_logging() {
    account_ops_tests::session_ids_and_affinity_are_scoped_by_owner_and_ref_and_parent_use_is_shared_only_within_owner();
    report_delivery_tests::report_run_repeat_returns_first_answer_and_usage_but_new_runs_count();
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod refusal_recovery_tests;
#[cfg(test)]
mod report_fence_tests;
#[cfg(test)]
mod route_fence_pool_tests;
#[cfg(test)]
mod runners_tests;
#[cfg(test)]
mod scope_attribution_tests;
#[cfg(test)]
mod select_availability_tests;
#[cfg(test)]
mod select_engine_vectors_tests;
#[cfg(test)]
mod select_options_tests;
#[cfg(test)]
mod task_ref_tests;
#[cfg(test)]
mod wire_pair_tests;

#[cfg(test)]
mod route_fact_tests;

/// The `routing/v1` capability a routing provider lists in its manifest.
pub(crate) const PROVIDES: &str = cortexkit_role_routing::PROVIDES;

/// Operations exclusive to `routing/v1`; shared names are registered with
/// the older operations in the module manifest.
pub(crate) const V1_ONLY_OPS: &[&str] = &[
    ops::ROLE_DESCRIBE,
    ops::ROUTE_SELECT_ENGINE,
    ops::ROUTE_ENGINE_REPORT,
    ops::ROUTE_ENGINE_EVAL,
    ops::ROUTE_REPORT,
    ops::ROUTE_BUDGET_QUOTE,
    ops::ROUTE_CONSUMER_MODELS,
];

/// Whether the `routing/v1` surface answers this request body.
pub(crate) fn parse_route_body(body: &[u8]) -> serde_json::Result<Value> {
    #[cfg(test)]
    crate::test_thread_state::lifecycle_probe(|probe| {
        probe
            .parses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    serde_json::from_slice(body)
}

#[cfg(test)]
pub(crate) fn claims(body: &[u8]) -> bool {
    parse_route_body(body).as_ref().is_ok_and(claims_request)
}

/// When v1 claims a method and parameter shape, its parser can name invalid
/// members instead of letting the legacy decoder silently ignore them.
pub(crate) fn claims_request(request: &Value) -> bool {
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return false;
    };
    method == crate::ROUTE_WHAT_IF_OP
        || shapes::claims(method, request.get("params").unwrap_or(&Value::Null))
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
    /// A module authenticated by the bus's publisher subject permissions.
    event_module: Option<String>,
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
    pub(crate) fn stamped_project(&self) -> Option<&str> {
        self.scope_authority
            .as_ref()
            .and_then(|scope| scope.project_id.as_deref())
            .or_else(|| {
                self.scope
                    .as_ref()
                    .and_then(|scope| scope.project_id.as_deref())
            })
    }

    pub(crate) fn for_route(channel: u16, epoch: u32) -> Self {
        #[cfg(test)]
        crate::test_thread_state::lifecycle_probe(|probe| {
            probe
                .callers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        Self {
            binding: Some((channel, epoch)),
            principal: crate::route_principal(channel, epoch),
            scope: crate::route_scope(channel, epoch),
            scope_authority: crate::route_scope_authority(channel, epoch),
            event_module: None,
        }
    }

    pub(crate) fn for_event(module: &str) -> Self {
        Self {
            binding: None,
            principal: format!("reserved:{module}"),
            scope: None,
            scope_authority: None,
            event_module: Some(module.to_owned()),
        }
    }

    fn module_identity_is_current(&self) -> bool {
        self.binding_is_current()
            || self.event_module.as_ref().is_some_and(|module| {
                striatum_core::module_identity::equal(
                    &self.principal,
                    &format!("reserved:{module}"),
                )
            })
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
                        *bound == epoch
                            && striatum_core::module_identity::equal(principal, &self.principal)
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

async fn scoped_settings(
    settings: Option<&cortexkit_role_routing::select::SelectSettings>,
    caller: &Caller,
    deps: &Deps<'_>,
    config: &crate::config_overrides::LiveConfig,
    request_views: Option<&crate::managed_settings::RequestViews>,
) -> Result<
    (
        crate::tier_settings::Layers,
        Option<cortexkit_role_routing::select::ProjectSettings>,
    ),
    Refusal,
> {
    use cortexkit_role_routing::select::ProjectSettings;
    let stamped_project = caller.stamped_project();
    if let Some(settings) = settings {
        settings.check_project_stamp(stamped_project)?;
    }
    let requested_project = settings.and_then(|settings| settings.project_id.as_deref());
    // The stamp supplies the default tier view; only an explicit lookup
    // gets a project_settings status in the reply.
    let Some(project) = stamped_project.or(requested_project) else {
        return Ok((crate::tier_settings::Layers::default(), None));
    };
    let view = match request_views {
        Some(views) => views.get(deps.routing_config_path, project).await,
        None => crate::managed_settings::project_view(deps.routing_config_path, project).await,
    };
    let Some(view) = view else {
        return Ok((
            crate::tier_settings::Layers::default(),
            requested_project.map(|_| ProjectSettings::Unresolved),
        ));
    };
    // The wire contract lets an unscoped caller opt into tier settings, but
    // runner placement is operator policy keyed only by the daemon's stamp.
    let enrolled = if stamped_project.is_some()
        && view.consumers.values().any(|entry| entry.runners.is_some())
    {
        Some(runners::enrollment::enrolled(
            &config.v1_runners(),
            &*deps.broca_credentials.lock().map_err(store_refusal)?,
        ))
    } else {
        None
    };
    let layers = crate::tier_settings::Layers::new(view.consumers.clone(), enrolled);
    let status = if view.applied {
        ProjectSettings::Applied
    } else {
        ProjectSettings::None
    };
    Ok((layers, requested_project.map(|_| status)))
}

/// Answer one v1 request on its route and send the reply frame.
#[cfg(test)]
pub(crate) async fn serve(
    frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    deps: &Deps<'_>,
) -> Result<(), ModuleError> {
    let caller = Caller::for_route(frame.header.channel, route_epoch);
    let request = parse_route_body(&frame.body);
    serve_request(frame, route_epoch, request, caller, writer, deps).await
}

pub(crate) async fn serve_request(
    frame: Frame,
    route_epoch: u32,
    request: serde_json::Result<Value>,
    caller: Caller,
    writer: &mpsc::Sender<Frame>,
    deps: &Deps<'_>,
) -> Result<(), ModuleError> {
    let timing = crate::stage_timing::AccountCallTiming::new();
    let (ver, channel, corr) = (frame.header.ver, frame.header.channel, frame.header.corr);
    let mut operation = String::new();
    let mut call_params = Value::Null;
    let mut call_stages = striatum_store::StoreStageTiming::default();
    let reply = match request {
        Ok(mut request) => {
            let method = match request.get_mut("method").map(Value::take) {
                Some(Value::String(method)) => method,
                _ => String::new(),
            };
            let params = request
                .get_mut("params")
                .map(Value::take)
                .unwrap_or(Value::Null);
            #[cfg(test)]
            if method == ops::ROUTE_SELECT {
                crate::test_thread_state::lifecycle_probe(|probe| {
                    if probe.delay_select {
                        std::thread::sleep(std::time::Duration::from_millis(1_050));
                    }
                });
            }
            #[cfg(test)]
            if matches!(
                method.as_str(),
                ops::ROUTE_ACCOUNT_FOR
                    | ops::ROUTE_ACCOUNT_OBSERVE
                    | ops::ROUTE_SELECT_PANEL
                    | ops::ROUTE_HEAD_CHOICES
            ) {
                crate::stage_timing::ACCOUNT_DELAY
                    .with(|delay| std::thread::sleep(delay.replace(std::time::Duration::ZERO)));
            }
            let call = async {
                #[cfg(test)]
                let probe =
                    crate::stage_timing::ACCOUNT_PROBE.with(|probe| probe.borrow_mut().take());
                #[cfg(test)]
                let probe = match probe {
                    Some(probe) => Some(probe.pause().await),
                    None => None,
                };
                #[cfg(test)]
                let before = probe.as_ref().map(|_| deps.store.stage_timing());
                let reply = answer(&method, &params, &caller, deps).await;
                #[cfg(test)]
                if let (Some((injected, expected)), Some(before)) = (probe, before) {
                    // Account handlers are synchronous. These store-wide deltas
                    // cover only the uninterrupted handler, not the injected await.
                    let after = deps.store.stage_timing();
                    expected
                        .send(striatum_store::StoreStageTiming {
                            writer_queue_ns: injected.writer_queue_ns + after.writer_queue_ns
                                - before.writer_queue_ns,
                            sql_ns: injected.sql_ns + after.sql_ns - before.sql_ns,
                            commit_ns: injected.commit_ns + after.commit_ns - before.commit_ns,
                        })
                        .unwrap();
                }
                reply
            };
            // The scope restores thread-local timing after each poll, so an
            // awaiting panel cannot pick up another request's store work.
            let (reply, stages) = if matches!(
                method.as_str(),
                ops::ROUTE_SELECT
                    | ops::ROUTE_ACCOUNT_FOR
                    | ops::ROUTE_ACCOUNT_OBSERVE
                    | ops::ROUTE_SELECT_PANEL
                    | ops::ROUTE_HEAD_CHOICES
            ) {
                striatum_store::scope_stage_timing(call).await
            } else {
                (call.await, striatum_store::StoreStageTiming::default())
            };
            call_stages = stages;
            operation = method;
            call_params = params;
            reply
        }
        Err(error) => Err(Refusal::invalid_request(
            "params",
            format!("the request body is not JSON: {error}"),
        )),
    };
    let panel_counts = if operation == ops::ROUTE_SELECT_PANEL {
        let answer = reply.as_ref().ok();
        (
            answer
                .and_then(|value| value["seats"].as_array())
                .map(|seats| seats.len() as u64),
            answer.and_then(|value| value["policy"]["size"].as_u64()),
        )
    } else {
        (None, None)
    };
    let serialization = std::time::Instant::now();
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
    let serialization = crate::stage_timing::record_serialization(serialization);
    let total = timing.elapsed();
    if matches!(
        operation.as_str(),
        ops::ROUTE_ACCOUNT_FOR | ops::ROUTE_ACCOUNT_OBSERVE
    ) && total > crate::stage_timing::SLOW_ACCOUNT_CALL
    {
        let provider = accounts::slow_call_provider(&operation, &call_params, &caller, deps);
        timing.emit(
            total,
            &operation,
            call_params["mode"].as_str().unwrap_or("admit"),
            &provider,
            &caller.principal,
            serialization,
            call_stages,
        );
    } else if operation == ops::ROUTE_SELECT_PANEL {
        timing.emit_panel(
            total,
            &call_params,
            panel_counts,
            serialization,
            call_stages,
        );
    } else if operation == ops::ROUTE_HEAD_CHOICES {
        timing.emit_head_choices(total, serialization, call_stages);
    } else if operation == ops::ROUTE_SELECT {
        crate::stage_timing::emit_select(total, serialization, call_stages);
    }
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

pub(crate) fn record_call(
    request: &Value,
    caller: &Caller,
    store: &striatum_store::SqliteRouteStateStore,
) -> Result<(), ModuleError> {
    if let Some(method) = request.get("method").and_then(Value::as_str) {
        if method == crate::ROUTE_WHAT_IF_OP {
            return Ok(());
        }
        let params = request.get("params").unwrap_or(&Value::Null);
        store
            .record_routing_call(
                method,
                shapes::census_shape(method, params),
                &caller.principal,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
    }
    Ok(())
}

pub(crate) async fn answer(
    method: &str,
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    match method {
        crate::ROUTE_WHAT_IF_OP => what_if::what_if(params, caller, deps).await,
        ops::ROLE_DESCRIBE => describe::describe(deps),
        ops::ROUTE_SELECT => select::select(params, caller, deps).await,
        ops::ROUTE_SELECT_ENGINE => engines::select(params, caller, deps),
        ops::ROUTE_ENGINE_REPORT => engines::report(params, caller, deps),
        ops::ROUTE_ENGINE_EVAL => engines::evaluate(params, caller, deps),
        ops::ROUTE_REPORT => report::report(params, caller, deps).map(|(answer, _)| answer),
        ops::ROUTE_ACCOUNT_FOR => accounts::account_for(params, caller, deps),
        ops::ROUTE_ACCOUNT_OBSERVE => accounts::account_observe(params, caller, deps),
        ops::ROUTE_SELECT_PANEL => panels::select_panel(params, caller, deps).await,
        ops::ROUTE_EXPLAIN => reads::explain(params, caller, deps),
        ops::ROUTE_BUDGET_QUOTE => reads::budget_quote(params, caller, deps),
        ops::ROUTE_ACCOUNT_STATUS => reads::account_status(params, caller, deps),
        ops::ROUTE_HEAD_CHOICES => reads::head_choices(params, deps),
        ops::ROUTE_CONSUMER_MODELS => reads::consumer_models(params, caller, deps),
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
    policy: &'a striatum_core::accounts::AllocatorPolicy,
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
