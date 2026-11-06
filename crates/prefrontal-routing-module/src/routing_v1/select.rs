//! `route.select` for `routing/v1`.
//!
//! The selection itself is routing's existing core: the same facts, quota
//! overlay, configuration overlays, billing-plane prices, credential
//! entitlement fences and operator exclusions every route
//! selection reads ([`crate::prepare_selection_inputs`],
//! [`crate::price_selection_inputs`]), the scorer
//! ([`select_policy::select_candidates`]) and the pin admission
//! ([`crate::work_model_pin::select_with_availability`]). What this module
//! adds is the v1 request: which runners can serve, the requirements the
//! scorer has no input for, the consumer's policy, the project's pin and the
//! affinity preference, and the decision binding.
//!
//! What it answers is exactly what runs: the decision it records names the
//! route and runner of the answer, and nothing selects a second time.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use cortexkit_role_routing::errors::{NoRouteReason, PinUnavailableReason, Refusal};
use cortexkit_role_routing::select::{
    parse_select_request_serving, Modality, PinFallback, PinOutcome, RouteRef, RunnerRef,
    SelectAnswer, SelectRequest, ToolsRequirement, EXPLANATION_MAX_CHARS,
};
use prefrontal_routing_core::model_router::{
    ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities, RouteFailureReason,
    SelectedModelRoute,
};
use prefrontal_routing_core::slot_saturation::{ConcurrencySlotState, ConcurrencySlots};
use prefrontal_routing_core::{fact_to_route, ModelFact, ModelRequirements, RoutingConfig};
use prefrontal_routing_store::V1Decision;
use serde_json::{json, Value};

use super::{now_ms, operator_config, provider_refusal, store_refusal, Caller, Deps};
use crate::config_policy::RunnerPolicy;
use crate::{
    account_selection, config_overrides, key_pool, select_policy, work_model_pin,
    DecisionProvenance, SelectionPolicySources,
};

/// The members a v1 select may carry.
const MEMBERS: &[&str] = &[
    "settings",
    "pin_slot",
    "available_routes",
    "prefer_prompt_cache",
    "consumer",
    "requirements",
    "pin",
    "exclude",
    "exclude_providers",
    "runners",
    "affinity",
    "correlation_id",
    "task_ref",
    "concurrency_slots",
    "detail",
    "harness",
    "hold_ok",
    "hold_wait_max_s",
    "pin_fallback",
    "prefer",
    "service_tier",
    "project_root",
    "workspace_root",
    "tier_consumer",
];

/// How long an answer stays current routing. A report after this is still
/// accepted; running on the decision then is the caller's risk.
const DECISION_TTL_MS: u64 = 10 * 60_000;

/// How long a select waits for the first billing-plane load before refusing.
///
/// Callers time a select at 3 s (core's `CALL_TIMEOUT`), so the wait ends
/// 1500 ms early and the typed `routing_not_ready` refusal reaches the caller
/// instead of a transport timeout that drops the caller's shared connection.
const BILLING_WAIT: Duration = Duration::from_millis(1500);

/// Why the answered route was chosen, for the explanation and the detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Basis {
    RequestPin,
    ProjectPin,
    Affinity,
    Preferred,
    Automatic,
}

impl Basis {
    fn as_str(self) -> &'static str {
        match self {
            Basis::RequestPin => "the request's pin",
            Basis::ProjectPin => "the project's operator pin",
            Basis::Affinity => "affinity with the previous decision",
            Basis::Preferred => "the request's preference among adequate routes",
            Basis::Automatic => "the lowest-cost route that meets the requirements",
        }
    }
}

pub(super) async fn select(
    params: &serde_json::Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    select_kind(params, caller, deps, false).await
}

pub(super) async fn panel_seat(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    select_kind(params, caller, deps, true).await
}

async fn select_kind(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    let lookup = |workload: &str, iq: f64, facts: &[ModelFact]| {
        let complete = crate::select::task_profiles_for(workload, iq, facts);
        if !complete.profiles.is_empty() || facts.len() <= 1 {
            return complete;
        }
        // Resolve each route from one refresh snapshot. The core scorer, which
        // knows the final adequate shortlist, enforces all-or-nothing pricing;
        // an excluded or inadequate route must not prevent the remaining routes
        // from using profile pricing.
        let snapshot = crate::task_profile_refresh::cache().snapshot();
        crate::select::TaskProfileLookup {
            profiles: facts
                .iter()
                .flat_map(|fact| snapshot.lookup(workload, iq, std::slice::from_ref(fact)))
                .collect(),
            providers_without_cache_write_reports: snapshot
                .providers_without_cache_write_reports
                .clone(),
        }
    };
    select_with_profile_lookup_kind(params, caller, deps, &lookup, panel_seat).await
}

pub(super) type ProfileLookup =
    dyn Fn(&str, f64, &[ModelFact]) -> crate::select::TaskProfileLookup + Sync;

pub(super) async fn select_with_profile_lookup(
    params: &serde_json::Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
) -> Result<Value, Refusal> {
    select_with_profile_lookup_kind(params, caller, deps, profile_lookup, false).await
}

async fn select_with_profile_lookup_kind(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    super::refuse_unknown_members(params, MEMBERS)?;
    if super::shapes::NEW_SELECT
        .iter()
        .any(|field| params.get(*field).is_some())
    {
        if let Some(field) = super::shapes::INJECTED
            .iter()
            .find(|field| params.get(**field).is_some())
        {
            return Err(Refusal::invalid_request(
                *field,
                "injected context cannot accompany a contract selection",
            ));
        }
    }
    super::shapes::settings(params)?;
    super::shapes::closed_array(
        params,
        "concurrency_slots",
        &["key", "running", "limit", "queued"],
    )?;
    let request = parse_select_request_serving(
        &crate::tier_settings::without_context(params),
        super::describe::CAPABILITIES,
    )?;
    let (settings_layers, project_settings) = if let Some(settings) = &request.settings {
        settings.check_project_stamp(
            caller
                .scope_authority
                .as_ref()
                .and_then(|scope| scope.project_id.as_deref())
                .or_else(|| {
                    caller
                        .scope
                        .as_ref()
                        .and_then(|scope| scope.project_id.as_deref())
                }),
        )?;
        match settings.project_id.as_deref() {
            Some(project) => {
                let (layers, status) =
                    crate::project_settings::layers(project, deps.routing_config_path).await;
                (layers, Some(status))
            }
            None => (crate::tier_settings::Layers::new(None, None), None),
        }
    } else {
        (
            crate::tier_settings::Layers::new(
                params.get("project_root").and_then(Value::as_str),
                params.get("workspace_root").and_then(Value::as_str),
            ),
            None,
        )
    };
    let mut result = select_with_tier_fallback(
        params,
        caller,
        deps,
        profile_lookup,
        &request,
        &settings_layers,
        project_settings,
        panel_seat,
    )
    .await;
    if let (Err(refusal), Some(slot)) = (&mut result, &request.pin_slot) {
        if refusal.code.as_str() == "pin_unavailable" {
            refusal.detail.insert("pin".into(), json!(slot));
        }
    }
    if let Err(refusal) = &result {
        if (request.pin.is_some() || request.pin_slot.is_some())
            && request.pin_fallback != Some(PinFallback::Route)
            && matches!(
                refusal.code.as_str(),
                "no_route"
                    | "pin_unavailable"
                    | "accounts_blocked"
                    | "account_hold"
                    | "auth_required"
                    | "provider_requires_runner_pin"
            )
        {
            // Record the final refusal, not unsuccessful probes of higher tiers.
            // Hard pins bypass scoring, which normally writes the refusal decision.
            record_pinned_refusal(&request, refusal, caller, deps);
        }
    }
    result
}

async fn select_with_tier_fallback(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
    request: &SelectRequest,
    settings_layers: &crate::tier_settings::Layers,
    project_settings: Option<cortexkit_role_routing::select::ProjectSettings>,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    use cortexkit_role_routing::service_tier::ServiceTier;
    let mut trace = TierTrace {
        project_settings,
        ..Default::default()
    };
    let Some(requested) = request.service_tier else {
        return select_at_tier(
            params,
            caller,
            deps,
            profile_lookup,
            None,
            None,
            &mut trace,
            settings_layers,
            panel_seat,
        )
        .await;
    };
    if let Some(previous) = request
        .affinity
        .as_deref()
        .map(|key| {
            deps.store
                .v1_affinity_decision(&caller.principal, &request.consumer, key)
        })
        .transpose()
        .map_err(store_refusal)?
        .flatten()
    {
        if let Some((tier, _)) = deps
            .store
            .decision_service_tier(&previous.decision_id)
            .map_err(store_refusal)?
        {
            if let Some(tier) = ServiceTier::parse(&tier).filter(|tier| *tier <= requested) {
                let credential = deps
                    .store
                    .decision_provider_identity(&previous.decision_id)
                    .map_err(store_refusal)?
                    .and_then(|identity| identity.credential_id);
                trace.affinity = true;
                match select_at_tier(
                    params,
                    caller,
                    deps,
                    profile_lookup,
                    Some(tier),
                    credential.as_deref(),
                    &mut trace,
                    settings_layers,
                    panel_seat,
                )
                .await
                {
                    Ok(answer) => return Ok(answer),
                    Err(error) => trace.record_failure(tier, &error),
                }
                trace.affinity = false;
            }
        }
    }
    let mut last = None;
    for tier in ServiceTier::ALL
        .iter()
        .rev()
        .copied()
        .filter(|tier| *tier <= requested)
    {
        match select_at_tier(
            params,
            caller,
            deps,
            profile_lookup,
            Some(tier),
            None,
            &mut trace,
            settings_layers,
            panel_seat,
        )
        .await
        {
            Ok(answer) => return Ok(answer),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "no_route"
                        | "pin_unavailable"
                        | "accounts_blocked"
                        | "account_hold"
                        | "auth_required"
                ) =>
            {
                trace.record_failure(tier, &error);
                last = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.unwrap_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None)))
}

#[derive(Default)]
struct TierTrace {
    project_settings: Option<cortexkit_role_routing::select::ProjectSettings>,
    attempts: Vec<Value>,
    rejections: Vec<Value>,
    affinity: bool,
}

impl TierTrace {
    fn record_failure(
        &mut self,
        tier: cortexkit_role_routing::service_tier::ServiceTier,
        refusal: &Refusal,
    ) {
        if matches!(
            refusal.code.as_str(),
            "accounts_blocked" | "account_hold" | "auth_required"
        ) {
            self.rejections
                .push(json!({"check": "account", "refusal_code": refusal.code}));
        }
        self.attempts.push(json!({
            "tier": tier.as_str(),
            "affinity": self.affinity,
            "refusal_code": refusal.code,
            "reason": refusal.detail.get("reason"),
            "rejections": std::mem::take(&mut self.rejections),
        }));
    }
}

fn record_pinned_refusal(
    request: &SelectRequest,
    refusal: &Refusal,
    caller: &Caller,
    deps: &Deps<'_>,
) {
    let decision = prefrontal_routing_core::route_state_store::DecisionInput {
        decision_kind: "single".into(),
        consumer: Some(request.consumer.clone()),
        target_agent: request.consumer.clone(),
        task_id: request.task_ref.clone(),
        harness: request.harness.clone(),
        project_id: caller.scope.as_ref().and_then(|scope| scope.project_id.clone()),
        workdir: caller.scope.as_ref().and_then(|scope| scope.workdir.clone()),
        requirements_json: serde_json::to_string(&request.requirements).unwrap(),
        score_json: "{}".into(),
        rejected_json: refusal.detail.get("rejected").cloned().unwrap_or_else(|| json!([])).to_string(),
        explanation_json: Some(json!({
            "failureReason": refusal.detail.get("reason").and_then(Value::as_str).unwrap_or(refusal.code.as_str()),
            "refusal": refusal,
            "pin": request.pin,
            "correlation_id": request.correlation_id,
        }).to_string()),
        anchor_version: prefrontal_routing_core::ANCHOR_VERSION,
        ..Default::default()
    };
    if let Err(error) =
        deps.store
            .record_decision_with_task_ref(&decision, None, None, request.task_ref.as_deref())
    {
        crate::warn_operator(format!("routing refused a pinned route.select for {} (task {}) and couldn't record the decision: {error}", request.consumer, request.task_ref.as_deref().unwrap_or("-")));
    }
}

async fn select_at_tier(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
    service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    sticky_credential: Option<&str>,
    tier_trace: &mut TierTrace,
    tier_settings: &crate::tier_settings::Layers,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    tier_trace.rejections.clear();
    super::refuse_unknown_members(params, MEMBERS)?;
    // An omitted task_ref is valid, but a present value must be a string.
    // Reject null rather than letting the optional-string parser treat it as omitted.
    if params.get("task_ref").is_some_and(|value| {
        value.as_str().is_none_or(|key| {
            key.is_empty()
                || key.len() > 128
                || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        })
    }) {
        return Err(Refusal::invalid_request(
            "task_ref",
            "task_ref must be 1-128 bytes of printable ASCII (0x21-0x7E)",
        ));
    }
    let request = parse_select_request_serving(
        &crate::tier_settings::without_context(params),
        super::describe::CAPABILITIES,
    )?;
    let live_config = config_overrides::live_config(deps.routing_config_path);
    let operator_config = operator_config(deps);
    let consumer = request.consumer.clone();
    let consumers = super::consumers::effective(&operator_config);
    consumers.admit(&consumer, caller)?;
    let effective_params = consumers.with_defaults(&consumer, params, &operator_config);
    let request = parse_select_request_serving(
        &crate::tier_settings::without_context(&effective_params),
        super::describe::CAPABILITIES,
    )?;
    if !crate::broca_credentials::wait_for_billing_catalog(deps.broca_credentials, BILLING_WAIT)
        .await
    {
        return Err(provider_refusal(
            "routing_not_ready",
            "routing pricing is awaiting its first billing-plane load; retry shortly",
        ));
    }

    let operator_policy = operator_config.operator_policy();
    // Payment class comes only from the resolved consumer policy, not from the
    // consumer's name.
    let legacy_consumer = select_policy::Consumer::from_id(&consumer);
    let consumer_policy = operator_config.consumer_policy(
        &consumer,
        &crate::config_policy::ConsumerPolicy {
            iq: Some(f64::from(request.requirements.iq)),
            eq: request.requirements.eq.map(f64::from),
            ..Default::default()
        },
        &consumers.policies,
    );
    let priced_per_token = select_policy::priced_per_token(&consumer_policy, legacy_consumer);
    let crate::PreparedSelectionInputs {
        now,
        mut facts,
        mut snapshot,
        quota_signal_json,
        quota_note: _,
    } = crate::prepare_selection_inputs(
        deps.quota_cache,
        deps.store,
        live_config.pricing(),
        &live_config.quota_model_pools(),
    )
    .map_err(store_refusal)?;
    // Providers with no catalog rows of their own, each resolved to the
    // catalog provider whose rows it borrows, read once for this selection.
    let catalog_targets = deps
        .broca_credentials
        .lock()
        .map(|credentials| credentials.catalog_targets(&facts))
        .unwrap_or_default();
    let sources = SelectionPolicySources {
        live_config: &live_config,
        operator_config: &operator_config,
        operator_config_path: deps.operator_config_path,
        catalog_targets: &catalog_targets,
    };
    let mut priced = crate::price_selection_inputs(
        &mut facts,
        quota_signal_json,
        &sources,
        deps.store,
        deps.broca_credentials,
        true,
        priced_per_token,
    )
    .map_err(|error| store_refusal(error.to_string()))?;

    // Runners the caller can send to that reach every tool capability tag the
    // work lists. Tags are matched; a tool provider's module name is not.
    let tags = request
        .requirements
        .tool_capabilities
        .clone()
        .unwrap_or_default();
    let registry = deps
        .broca_credentials
        .lock()
        .map(|credentials| {
            super::runners::Registry::build(
                operator_config.v1_runners(),
                &credentials,
                &facts,
                now as u64,
            )
        })
        .unwrap_or_default();
    let runners: Vec<RunnerPolicy> = registry
        .policies()
        .into_iter()
        .filter(|runner| {
            consumer_policy
                .runner
                .as_ref()
                .is_none_or(|module| module == &runner.module)
        })
        .filter(|runner| {
            request
                .runners
                .as_ref()
                .is_none_or(|listed| listed.contains(&runner.module))
        })
        .collect();
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            request.harness.as_deref().is_none_or(|harness| {
                registry
                    .get(&runner.module)
                    .is_some_and(|runner| runner.harnesses.iter().any(|hosted| hosted == harness))
            })
        })
        .collect();
    if runners.is_empty() {
        return Err(if let Some(harness) = &request.harness {
            let mut refusal = Refusal::no_route(NoRouteReason::HarnessUnavailable, None);
            refusal.message = format!("no session-starting runner declares harness {harness}");
            refusal
        } else {
            Refusal::no_route(NoRouteReason::NoAvailableModel, None)
        });
    }
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            tags.iter()
                .all(|tag| runner.tool_capabilities.contains(tag))
        })
        .collect();
    if runners.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
    }
    let tier_consumer = request
        .settings
        .as_ref()
        .and_then(|settings| settings.tier_consumer.as_deref())
        .or_else(|| params.get("tier_consumer").and_then(Value::as_str))
        .or(request.pin_slot.as_deref())
        .unwrap_or(&consumer);
    let configured_tiers = if service_tier.is_none() {
        priced
            .classes
            .as_mut()
            .map(|classes| {
                classes.apply_configured_service_tiers(
                    &mut facts,
                    |fact| {
                        tier_settings.resolve(
                            &operator_config,
                            tier_consumer,
                            &fact.provider_id,
                            &fact.model_id,
                            None,
                        )
                    },
                    &runners,
                    &registry,
                    priced_per_token,
                )
            })
            .unwrap_or_default()
    } else {
        Default::default()
    };
    if let Some(tier) = service_tier {
        let Some(classes) = &mut priced.classes else {
            tier_trace.rejections.push(json!({"check": "account"}));
            return Err(Refusal::no_route(NoRouteReason::NoAvailableModel, None));
        };
        tier_trace.rejections = classes.apply_service_tier(
            &mut facts,
            tier.as_str(),
            &runners,
            &registry,
            priced_per_token,
            sticky_credential,
        );
    }
    let runs = |runner: &RunnerPolicy, route_key: &str, capacity_key: &str| -> bool {
        registry.runs(&runner.module, route_key, capacity_key)
            && service_tier
                .or_else(|| configured_tiers.get(capacity_key).copied())
                .filter(|tier| !tier.is_standard())
                .is_none_or(|tier| {
                    priced.classes.as_ref().is_some_and(|classes| {
                        facts
                            .iter()
                            // Pins may add a reasoning variant absent from the catalog's
                            // fact rows. Service tiers belong to the underlying model,
                            // while registry.runs above still checks the exact route.
                            .find(|fact| fact_to_route(fact).capacity_key == capacity_key)
                            .is_some_and(|fact| {
                                registry.get(&runner.module).is_some_and(|runner| {
                                    classes.tier_runner_serves(runner, fact, tier.as_str())
                                })
                            })
                    })
                })
    };

    // The routes some eligible runner runs, then the requirements the scorer
    // has no input for. Routing records no fact for PDF or audio input or for
    // structured output, so it cannot confirm a route serves them and answers
    // none.
    let reachable: Vec<&ModelFact> = facts
        .iter()
        .filter(|fact| {
            request.available_routes.as_ref().is_none_or(|routes| {
                routes.contains(&format!("{}/{}", fact.provider_id, fact.model_id))
            })
        })
        .filter(|fact| {
            let route = fact_to_route(fact);
            runners.iter().any(|runner| {
                registry.can_run(&runner.module, &route.route_key, &route.capacity_key)
            })
        })
        .collect();
    let pair_rejections: HashMap<String, String> = facts
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            let unverified = !runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.wire_family_unverified(
                        &runner.module,
                        &route.route_key,
                        &route.capacity_key,
                    )
                });
            unverified.then(|| {
                (
                    route.route_key,
                    prefrontal_routing_core::model_router::REASON_RUNNER_WIRE_FAMILY_UNVERIFIED
                        .into(),
                )
            })
        })
        .collect();
    let requirements = &request.requirements;
    let modalities = requirements.modalities.clone().unwrap_or_default();
    let unconfirmable = modalities
        .iter()
        .any(|modality| matches!(modality, Modality::Pdf | Modality::Audio))
        || requirements.structured_output == Some(true);
    let capable: Vec<&ModelFact> = reachable
        .iter()
        .copied()
        .filter(|_| !unconfirmable)
        .filter(|fact| {
            requirements.tools != Some(ToolsRequirement::Required)
                || fact.supports_tool_use == Some(true)
        })
        .filter(|fact| {
            requirements
                .max_output_tokens
                .is_none_or(|floor| fact.max_output_tokens.is_some_and(|limit| limit >= floor))
        })
        .collect();
    if capable.is_empty() && !reachable.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
    }
    // A key-pool provider's keys are leased per send (`route.pool_acquire`),
    // and a v1 answer cannot yet name the leased key the way `runner.auth`
    // will. Answering such a route would send the work with the wrong
    // credential or none, so it is not a v1 candidate until the answer can
    // carry its key.
    // Every route that could serve this request is a key-pool one: if any other
    // route were adequate, the filter below would keep it.
    let currently_capable: Vec<_> = capable
        .iter()
        .filter(|fact| {
            let route = fact_to_route(fact);
            runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
        })
        .collect();
    let key_pool_only = !currently_capable.is_empty()
        && currently_capable
            .iter()
            .all(|fact| key_pool::is_pool_provider(&catalog_targets, &fact.provider_id));
    let capable: Vec<&ModelFact> = capable
        .into_iter()
        .filter(|fact| !key_pool::is_pool_provider(&catalog_targets, &fact.provider_id))
        .collect();
    if capable.is_empty() && pair_rejections.is_empty() {
        return Err(if key_pool_only {
            key_pool_route_refusal()
        } else {
            Refusal::no_route(NoRouteReason::MissingCapability, None)
        });
    }

    let statically_available: HashSet<String> = capable
        .iter()
        .map(|fact| fact_to_route(fact).route_key)
        .collect();
    let available: HashSet<String> = capable
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                .then_some(route.route_key)
        })
        .collect();
    if available.is_empty() && pair_rejections.is_empty() {
        return Err(if key_pool_only {
            key_pool_route_refusal()
        } else {
            Refusal::no_route(NoRouteReason::MissingCapability, None)
        });
    }
    if let Some(classes) = &priced.classes {
        classes.apply_eligibility(&mut snapshot, &facts, priced_per_token, false);
    }
    let candidates = select_policy::prepare_candidates(
        &facts,
        &mut snapshot,
        available,
        pair_rejections,
        select_policy::CandidateSource::Vault {
            credentials: deps.broca_credentials,
            fences: &priced.entitlement_fences,
            stale: &[],
            defer_pool_pin: false,
        },
        legacy_consumer,
        priced_per_token,
        None,
        priced.subscription_plan_providers.as_ref(),
        &operator_policy,
        &catalog_targets,
        priced.classes.as_ref(),
    );
    let available = candidates.available;
    let mut usable = available.clone();
    let mut runtime_unavailable: HashSet<String> = statically_available
        .difference(&available)
        .filter(|key| !candidates.rejected.contains_key(*key))
        .cloned()
        .collect();
    let mut account_blocks = HashMap::new();
    if let Some(classes) = &priced.classes {
        for fact in &facts {
            if !matches!(
                classes
                    .probe(
                        &fact.provider_id,
                        &fact.model_id,
                        priced_per_token,
                        false,
                        None,
                        "demand-preflight"
                    )
                    .outcome,
                prefrontal_routing_core::accounts::Outcome::Admit(_)
            ) {
                let key = fact_to_route(fact).route_key;
                if usable.remove(&key) && !candidates.rejected.contains_key(&key) {
                    runtime_unavailable.insert(key);
                }
                if let Some(reset) =
                    classes.blocked_reset(&fact.provider_id, &fact.model_id, priced_per_token)
                {
                    account_blocks.insert(
                        prefrontal_routing_core::fact_to_route(fact).route_key,
                        reset,
                    );
                }
            }
        }
    }
    let subscription_keys = candidates.subscription_keys;
    let rejections = candidates.rejected;

    // Operator score targets allow fractions; keep them in the internal demand
    // rather than sending them through the integer-only request validator.
    let operator_demand =
        operator_config.consumer_policy(&consumer, &Default::default(), &Default::default());
    let demand = ModelRequirements {
        iq: operator_demand
            .iq
            .unwrap_or_else(|| f64::from(requirements.iq)),
        eq: operator_demand
            .eq
            .unwrap_or_else(|| f64::from(requirements.eq.unwrap_or(0))),
    };
    let vision = modalities.contains(&Modality::Image);
    let required_capabilities = match (vision, requirements.context_window) {
        (false, None) if demand.iq > 0.0 || demand.eq > 0.0 => None,
        // The scorer refuses an all-zero demand that names no capability; a
        // zero context floor states "no demand" without adding a constraint.
        (vision, context) => Some(RequiredCapabilities {
            vision: vision.then_some(true),
            min_context: Some(context.map_or(0.0, |floor| floor as f64)),
        }),
    };
    // The selection's exclusion matcher reads a bare provider id as every
    // route of that provider, so `exclude_providers` joins the request's
    // route-key exclusions in the one list every selection path reads.
    let mut exclude: Vec<String> = request.exclude.clone().unwrap_or_default();
    exclude.extend(request.exclude_providers.clone().unwrap_or_default());
    let consumer_exclude = operator_config.exclude_groups().named_group(&consumer);
    let slots = request.concurrency_slots.as_ref().map(|slots| {
        ConcurrencySlots::new(slots.iter().map(|slot| ConcurrencySlotState {
            key: slot.key.clone(),
            running: u32::try_from(slot.running).unwrap_or(u32::MAX),
            limit: u32::try_from(slot.limit).unwrap_or(u32::MAX),
            queued: u32::try_from(slot.queued.unwrap_or(0)).unwrap_or(u32::MAX),
        }))
    });
    let selection = Selection {
        prefer_prompt_cache: request.prefer_prompt_cache.unwrap_or(false),
        consumer: &consumer,
        priced_per_token,
        demand,
        profile_lookup,
        task_ref: request.task_ref.as_deref(),
        required_capabilities,
        facts: &facts,
        snapshot: &snapshot,
        exclude: &exclude,
        operator_exclude: &operator_policy.exclude,
        consumer_exclude: &consumer_exclude,
        subscription_keys: &subscription_keys,
        rejections: &rejections,
        runtime_unavailable: &runtime_unavailable,
        routing_config: &priced.routing_config,
        slots: slots.as_ref(),
        min_band: live_config.min_band(),
        providers_without_prompt_cache: live_config.providers_without_prompt_cache(),
        deps,
        sources: &sources,
        account_flags: live_config.account_flags(),
        classes: priced.classes.as_ref(),
        scope: caller.scope.clone(),
        now,
        hold_ok: params
            .get("hold_ok")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        hold_wait_max_s: params.get("hold_wait_max_s").and_then(Value::as_u64),
        conversation_key: request.affinity.as_deref().unwrap_or("select"),
    };

    let project_pin = request
        .settings
        .as_ref()
        .is_none_or(|settings| settings.project_id.is_some())
        .then(|| {
            caller
                .scope
                .as_ref()
                .and_then(|scope| scope.project_id.as_deref())
                .and_then(|project| operator_config.project_pin(project, &consumer))
                .map(str::to_owned)
        })
        .flatten();
    let previous = match &request.affinity {
        Some(key) => deps
            .store
            .v1_affinity_decision(&caller.principal, &consumer, key)
            .map_err(store_refusal)?,
        None => None,
    };
    let continuing = previous
        .as_ref()
        .map(|previous| format!("{}/{}", previous.provider, previous.model));
    // Request pin, then project pin, then the continuing model, then the mode.
    // A pin does not fall through unless the request asks for it with
    // `pin_fallback: "route"`: then a pin that cannot be served is dropped
    // before the order runs, and the order runs as for an unpinned request.
    // The continuing model falls through once it is no longer allowed. Legacy
    // targetAgent bodies never reach this surface.
    let mut request_pin = request.pin.clone().or_else(|| {
        request
            .pin_slot
            .as_deref()
            .and_then(|slot| operator_config.select_slot_pin(slot))
    });
    let mut pin_reason = None;
    if request.pin_slot.is_some() && request_pin.is_none() {
        if request.pin_fallback == Some(PinFallback::Route) {
            pin_reason = Some("unknown_slot".into());
        } else {
            return Err(Refusal::pin_unavailable(
                request.pin_slot.as_deref().unwrap(),
                PinUnavailableReason::parse("unknown_slot"),
                None,
            ));
        }
    }
    if request.pin_fallback == Some(PinFallback::Route) {
        if let Some(Err(refusal)) = request_pin
            .as_deref()
            .map(|pin| selection.pin(pin, &available))
        {
            pin_reason = Some(refused_pin_reason(&refusal));
            request_pin = None;
        }
    }
    let ladder_choice = if request_pin.is_none()
        && project_pin.is_none()
        && continuing.is_none()
        && consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
    {
        let mut result = None;
        let mut fallback_hold = None;
        let mut skipped_key_pool = false;
        for entry in consumer_policy.ladder.iter().flatten() {
            let Some(route) = RouteRef::parse_pin(entry) else {
                continue;
            };
            // A ladder entry on a key-pool provider cannot be answered here, but
            // a later entry can be, so skip it and remember why: only a ladder
            // that ends with nothing answered for this reason says so.
            if key_pool::is_pool_provider(&catalog_targets, &route.provider) {
                skipped_key_pool = true;
                continue;
            }
            let subset = available
                .iter()
                .filter(|key| {
                    *key == &route.route_key()
                        || key.starts_with(&format!("{}/", route.route_key()))
                })
                .cloned()
                .collect();
            if let Ok(selected) = selection.score(&subset) {
                match selection.account_check(&selected) {
                    Ok(()) => {
                        result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                        break;
                    }
                    Err(refusal) if refusal.code.as_str() == "account_hold" => {
                        if selection.hold_ok {
                            result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                            break;
                        }
                        if fallback_hold.as_ref().is_none_or(
                            |(_, previous): &(SelectedModelRoute, Refusal)| {
                                previous.detail.get("reset_at_ms").and_then(Value::as_i64)
                                    > refusal.detail.get("reset_at_ms").and_then(Value::as_i64)
                            },
                        ) {
                            fallback_hold = Some((selected, refusal));
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        result
            .or_else(|| {
                fallback_hold.map(|(selected, _)| Ok((selected, select_policy::OrderBasis::Mode)))
            })
            // A ladder whose only admissible entries were key-pool routes says so,
            // rather than falling through to the automatic order.
            .or_else(|| {
                skipped_key_pool
                    .then(|| Err(select_policy::OrderFailure::Pin(key_pool_route_refusal())))
            })
    } else {
        None
    };
    let mut ordered = ladder_choice.unwrap_or_else(|| {
        select_policy::select_ordered(
            &usable,
            request_pin.as_deref(),
            project_pin.as_deref(),
            continuing.as_deref(),
            |pin| selection.pin(pin, &available),
            |only| selection.score(only),
            || {
                select_policy::select_in_order(&usable, &consumer_policy, |only| {
                    selection.score(only)
                })
            },
        )
    });
    // `pin_outcome` is answered exactly when the request pinned: `fell_back`
    // when its pin was dropped above, `pinned` when the pin was served.
    let pin_outcome = request.pin.as_ref().or(request.pin_slot.as_ref()).map(|_| {
        if pin_reason.is_some() {
            PinOutcome::FellBack
        } else {
            PinOutcome::Pinned
        }
    });
    // `prefer` ranks within the automatic order only: a pin, the continuing
    // model and an operator ladder each decide before it. A listed route is
    // answered only when it alone would be an adequate answer to this same
    // request, so the preference never admits a route the request would not
    // otherwise get.
    let mut preferred = false;
    let laddered = consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder);
    if let (Ok((normal, select_policy::OrderBasis::Mode)), Some(prefer), false) =
        (&ordered, &request.prefer, laddered)
    {
        if let Some(listed) = selection.preferred(&usable, prefer, normal) {
            ordered = Ok((listed, select_policy::OrderBasis::Mode));
            preferred = true;
        }
    }
    let (mut selected, basis, explicit_variant) = match ordered {
        Ok((selected, order)) => {
            let explicit = match order {
                select_policy::OrderBasis::RequestPin => request_pin
                    .as_deref()
                    .and_then(RouteRef::parse_pin)
                    .and_then(|pin| pin.variant),
                select_policy::OrderBasis::ProjectPin => project_pin
                    .as_deref()
                    .and_then(RouteRef::parse_pin)
                    .and_then(|pin| pin.variant),
                select_policy::OrderBasis::Continuing | select_policy::OrderBasis::Mode => None,
            };
            let basis = match order {
                select_policy::OrderBasis::RequestPin => Basis::RequestPin,
                select_policy::OrderBasis::ProjectPin => Basis::ProjectPin,
                select_policy::OrderBasis::Continuing => Basis::Affinity,
                select_policy::OrderBasis::Mode if preferred => Basis::Preferred,
                select_policy::OrderBasis::Mode => Basis::Automatic,
            };
            (selected, basis, explicit)
        }
        Err(select_policy::OrderFailure::Pin(refusal)) => return Err(refusal),
        Err(select_policy::OrderFailure::Score(no_route)) => {
            selection.record_refusal(&no_route, &priced.provenance);
            // Account preflight is separate from model scoring. Only name an
            // account block when an otherwise adequate, policy-allowed model
            // would have been selected without that block.
            let mut resets = Vec::new();
            let blocked: HashSet<_> = account_blocks
                .iter()
                .filter_map(|(key, reset)| {
                    if consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
                        && !consumer_policy.ladder.iter().flatten().any(|entry| {
                            RouteRef::parse_pin(entry)
                                .is_some_and(|route| route.route_key() == *key)
                        })
                    {
                        return None;
                    }
                    if !available.contains(key)
                        || selection.score(&HashSet::from([key.clone()])).is_err()
                    {
                        return None;
                    }
                    resets.extend(*reset);
                    Some(key.clone())
                })
                .collect();
            if !blocked.is_empty() {
                return Err(Refusal::no_route(
                    NoRouteReason::parse("accounts_blocked"),
                    resets.into_iter().min().map(|reset| reset.max(0) as u64),
                ));
            }
            return Err(no_route_refusal(&no_route, now));
        }
    };
    let variant_choice = crate::apply_variant_policy(
        &mut selected,
        &operator_config,
        deps.operator_config_path,
        &catalog_targets,
        Some(&consumer),
        explicit_variant.as_deref(),
    );

    let runner = select_policy::choose_runner(
        &runners,
        consumer_policy.runner.as_deref(),
        &selected.route_key,
        &selected.capacity_key,
        &runs,
    )
    .ok_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None))?;

    // Record the decision in the ledger every selection writes, then bind it.
    let mut decision = crate::decision_input_for(
        &consumer,
        &demand,
        Some(&selected),
        None,
        &crate::SelectParams {
            scope: caller.scope.clone(),
            ..crate::SelectParams::for_record()
        },
        Some(&priced.provenance),
        Some(&crate::policy_sources_line(&operator_policy)),
    );
    decision.consumer = Some(consumer.clone());
    // Selection chooses the route; account_for enforces its hold. Preserve the
    // effective wait cap so later policy edits cannot widen this decision.
    let max_hold_s = operator_config
        .allocator_policy(&selected.model.provider_id)
        .max_hold_s;
    let hold_cap = request
        .hold_wait_max_s
        .unwrap_or(account_selection::DEFAULT_HOLD_WAIT_S)
        .min(max_hold_s);
    let mut input: Value =
        serde_json::from_str(&decision.requirements_json).map_err(store_refusal)?;
    input["hold_wait_max_s"] = json!(hold_cap);
    let requested_tier = tier_settings.resolve(
        &operator_config,
        tier_consumer,
        &selected.model.provider_id,
        &selected.model.model_id,
        request.service_tier,
    );
    if let Some(requested) = requested_tier {
        input["service_tier_requested"] = json!(requested);
    }
    if let Some((requested, chosen)) = requested_tier
        .zip(service_tier)
        .filter(|(requested, chosen)| requested > chosen)
    {
        // Keep failed tier probes with the successful decision's input snapshot,
        // so a downgrade does not require reconstructing earlier account state.
        input["service_tier_fallback"] = json!({
            "requested": requested.as_str(),
            "chosen": chosen.as_str(),
            "affinity": tier_trace.affinity,
            "attempts": tier_trace.attempts,
        });
    }
    decision.requirements_json = input.to_string();
    decision.allowed_account_class =
        prefrontal_routing_core::accounts::AllowedAccountClass::from_pay_per_token(
            priced_per_token,
        );
    let decision_id = deps
        .store
        .record_decision_with_task_ref(
            &decision,
            Some(&runner.module),
            None,
            request.task_ref.as_deref(),
        )
        .map_err(store_refusal)?;
    let service_tier = service_tier.or_else(|| {
        configured_tiers
            .get(&format!(
                "{}/{}",
                selected.model.provider_id, selected.model.model_id
            ))
            .copied()
    });
    if let Some(tier) = service_tier {
        let accounts = priced
            .classes
            .as_ref()
            .map(|classes| {
                classes.service_tier_accounts(&selected.model.provider_id, &selected.model.model_id)
            })
            .unwrap_or_default();
        deps.store
            .record_service_tier(&decision_id, tier.as_str(), &accounts)
            .map_err(store_refusal)?;
    }
    if registry
        .get(&runner.module)
        .is_some_and(|runner| runner.follows_account_choice)
    {
        // The decision stands without the serving identity: an outcome for
        // it is then recorded as for a route with several credentials.
        if let Err(error) = crate::remember_provider_identity(
            deps.store,
            &decision_id,
            &selected,
            deps.broca_credentials,
        ) {
            crate::warn_operator(format!(
                "[prefrontal-routing] WARN: v1 decision {decision_id} for {} has no remembered serving identity: {error}",
                selected.route_key
            ));
        }
    }
    let now_ms = now_ms();
    let expires_at_ms = now_ms + DECISION_TTL_MS;
    let route = RouteRef {
        provider: selected.model.provider_id.clone(),
        model: selected.model.model_id.clone(),
        variant: selected.prompt_params.variant.clone(),
    };
    let binding = V1Decision {
        decision_id: decision_id.clone(),
        principal: caller.principal.clone(),
        consumer: consumer.clone(),
        allowed_account_class: decision.allowed_account_class,
        project_id: caller
            .scope
            .as_ref()
            .and_then(|scope| scope.project_id.clone()),
        provider: route.provider.clone(),
        model: route.model.clone(),
        variant: route.variant.clone(),
        runner_module: runner.module.clone(),
        affinity: request.affinity.clone(),
        created_at_ms: now_ms as i64,
        expires_at_ms: expires_at_ms as i64,
    };

    let fact = facts
        .iter()
        .filter(|fact| fact.provider_id == route.provider && fact.model_id == route.model)
        .min_by_key(|fact| u8::from(fact.variant != route.variant));
    let explanation = clamp_sentence(format!(
        "{} on {} for consumer {consumer}, chosen by {}.",
        route.pin(),
        runner.module,
        basis.as_str()
    ));
    let mut runner_ref = RunnerRef::new(runner.module.clone(), route.clone());
    if let Some(harness) = registry
        .get(&runner.module)
        .and_then(|runner| runner.selection_harness(request.harness.as_deref()))
    {
        runner_ref = runner_ref.with_harness(harness);
    }
    let mut answer = SelectAnswer::new(
        decision_id,
        route.clone(),
        runner_ref,
        fact.and_then(|fact| fact.context_window)
            .map_or(0, |window| window as u64),
        fact.and_then(|fact| fact.max_output_tokens).unwrap_or(0),
        expires_at_ms,
        explanation,
    );
    if let Some(tier) = service_tier {
        answer = answer.with_service_tier_chosen(tier);
    }
    let mut detail = (request.detail == Some(true)).then(|| {
        json!({
            "basis": basis.as_str(),
            "explanation": selected.explanation,
            "rejected": selected.rejected,
            "score": selected.score,
            "variant": variant_choice,
        })
    });
    // A fell-back answer always says why the pin was skipped, asked or not.
    if let Some(reason) = pin_reason {
        detail.get_or_insert_with(|| json!({}))["pin_reason"] = json!(reason);
    }
    if let Some(detail) = detail {
        answer = answer.with_detail(detail);
    }
    // A panel's policy chose this route. The internal pin fixes the scorer's
    // candidate, but is not an operator/caller pin on the public panel request.
    if !panel_seat {
        if let Some(outcome) = pin_outcome {
            answer = answer.with_pin_outcome(outcome);
        }
    }
    if let Some(status) = &tier_trace.project_settings {
        answer = answer.with_project_settings(status.clone());
    }
    let value = serde_json::to_value(answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    deps.store
        .v1_record_selection(&binding, Some(&value.to_string()))
        .map_err(store_refusal)?;
    Ok(value)
}

/// Everything one select's automatic and pinned attempts share.
struct Selection<'a> {
    prefer_prompt_cache: bool,
    hold_ok: bool,
    hold_wait_max_s: Option<u64>,
    conversation_key: &'a str,
    consumer: &'a str,
    priced_per_token: bool,
    demand: ModelRequirements,
    profile_lookup: &'a ProfileLookup,
    task_ref: Option<&'a str>,
    required_capabilities: Option<RequiredCapabilities>,
    facts: &'a [ModelFact],
    snapshot: &'a ModelRouteStateSnapshot,
    exclude: &'a [String],
    operator_exclude: &'a [String],
    consumer_exclude: &'a [String],
    subscription_keys: &'a HashSet<String>,
    /// Route-local refusals, by route key ([`select_policy::candidate_rejections`]).
    rejections: &'a HashMap<String, String>,
    runtime_unavailable: &'a HashSet<String>,
    routing_config: &'a RoutingConfig,
    slots: Option<&'a ConcurrencySlots>,
    min_band: Option<f64>,
    providers_without_prompt_cache: Vec<String>,
    deps: &'a Deps<'a>,
    sources: &'a SelectionPolicySources<'a>,
    account_flags: config_overrides::AccountSelectionFlags,
    classes: Option<&'a account_selection::ClassContext>,
    /// The scope the selecting route is bound under, recorded with a refusal
    /// the same way a decision records it.
    scope: Option<crate::DecisionScope>,
    now: f64,
}

impl<'a> Selection<'a> {
    fn score(&self, available: &HashSet<String>) -> Result<SelectedModelRoute, NoRouteError> {
        let allowed = available
            .iter()
            .filter(|route| {
                self.sources
                    .operator_config
                    .route_allowed(self.consumer, route, false)
            })
            .cloned()
            .collect();
        let mut snapshot = self.snapshot.clone();
        for key in self.runtime_unavailable {
            // Account availability cannot lower the maximum model scores used to
            // interpret IQ. Operator exclusions still can, including not_for rules.
            if self
                .sources
                .operator_config
                .route_allowed(self.consumer, key, false)
            {
                snapshot
                    .by_route
                    .entry(key.clone())
                    .or_default()
                    .statically_routable
                    .get_or_insert(true);
            }
        }
        let input = self.pass().input(&allowed, snapshot);
        self.score_input(input, self.consumer_exclude, None)
            .map_err(select_policy::describe_filter_refusal)
    }

    fn score_input(
        &self,
        mut input: prefrontal_routing_core::SelectModelRouteInput<'_>,
        consumer_exclude: &[String],
        explicit_variant: Option<&str>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        let catalog = config_overrides::reasoning_catalog_for(self.sources.operator_config_path);
        let groups = self.sources.operator_config.variant_groups();
        let mut identities = Vec::new();
        let mut shortlist = Vec::new();
        for fact in &input.facts {
            let route = fact_to_route(fact);
            if !input.available_models.contains(&route.route_key)
                && !input.available_models.contains(&route.capacity_key)
            {
                continue;
            }
            let choice = crate::variant_policy::resolve(
                groups,
                self.sources.catalog_targets,
                catalog.as_ref(),
                &crate::variant_policy::VariantRequest {
                    route_key: &route.route_key,
                    provider_id: &fact.provider_id,
                    model_id: &fact.model_id,
                    explicit: explicit_variant,
                    curated: fact
                        .prompt_defaults
                        .as_ref()
                        .and_then(|p| p.variant.as_deref())
                        .or(fact.variant.as_deref()),
                    consumer: Some(self.consumer),
                },
            );
            let mut served = fact.clone();
            served.variant = choice.variant;
            // Profiles follow the variant that will run, while routing keeps
            // the original keys for exclusions, availability and reservations.
            identities.push((route.route_key, fact_to_route(&served).route_key));
            shortlist.push(served);
        }
        // The lookup reads a refresh snapshot, never task history on the select path.
        let lookup = (self.profile_lookup)(self.consumer, self.demand.iq, &shortlist);
        let profiles = identities
            .into_iter()
            .filter_map(|(route, served)| {
                lookup
                    .profiles
                    .get(&served)
                    .map(|profile| (route, profile.clone()))
            })
            .collect();
        input.providers_without_cache_write_reports = lookup.providers_without_cache_write_reports;
        prefrontal_routing_core::model_router::select_model_route_with_profiles(
            input,
            self.routing_config,
            self.slots,
            consumer_exclude,
            self.rejections,
            &profiles,
        )
    }

    fn pass(&self) -> select_policy::SelectionPass<'_> {
        select_policy::SelectionPass {
            consumer: self.consumer,
            target_agent: self.consumer,
            requirements: self.demand,
            facts: self.facts,
            snapshot: self.snapshot,
            exclude: self.exclude,
            operator_exclude: self.operator_exclude,
            required_capabilities: self.required_capabilities,
            subscription_keys: self.subscription_keys,
            priced_per_token: self.priced_per_token,
            min_band: self.min_band,
            prefer_prompt_cache: self.prefer_prompt_cache,
            providers_without_prompt_cache: &self.providers_without_prompt_cache,
            consumer_exclude: self.consumer_exclude,
            rejections: self.rejections,
            config: self.routing_config,
            slots: self.slots,
            operator_config: self.sources.operator_config,
        }
    }

    /// A named model, through the same pin admission a work run's `model=`
    /// takes: hard admission only, never a quality ranking.
    fn pin(&self, pin: &str, available: &HashSet<String>) -> Result<SelectedModelRoute, Refusal> {
        let parsed = RouteRef::parse_pin(pin)
            .ok_or_else(|| Refusal::pin_unavailable(pin, PinUnavailableReason::Invalid, None))?;
        if self.exclude.contains(&parsed.route_key())
            || self.exclude.contains(&parsed.provider)
            || !self
                .sources
                .operator_config
                .route_allowed(self.consumer, &parsed.route_key(), true)
        {
            return Err(Refusal::pin_unavailable(
                pin,
                PinUnavailableReason::Excluded,
                None,
            ));
        }
        let variant = work_model_pin::named_variant(pin, self.facts);
        let (pin_key, pin_capacity) =
            work_model_pin::route_keys(pin, variant.as_deref(), self.facts);
        let snapshot = self.snapshot.clone();
        let refused = |reason: &str| {
            let (reason, reset) = pin_reason(reason, &snapshot, &pin_key, &pin_capacity, self.now);
            Refusal::pin_unavailable(pin, reason, reset)
        };
        let catalog = config_overrides::reasoning_catalog_for(self.sources.operator_config_path);
        let mut selected = work_model_pin::select_with_availability(
            pin,
            self.pass().input(available, snapshot.clone()),
            self.routing_config,
            self.slots,
            catalog.as_ref(),
            self.sources.catalog_targets,
            self.rejections,
        )
        .map_err(|refusal| refused(&refusal.code))?;
        // Hard pin admission remains shared with legacy selection. Reprice only
        // the admitted route, retaining its real demand's profile bucket.
        let mut input = self.pass().input(available, snapshot.clone());
        let mut fact = input
            .facts
            .iter()
            .find(|fact| {
                fact.provider_id == selected.model.provider_id
                    && fact.model_id == selected.model.model_id
                    && fact.variant == selected.prompt_params.variant
            })
            .or_else(|| {
                input.facts.iter().find(|fact| {
                    fact.provider_id == selected.model.provider_id
                        && fact.model_id == selected.model.model_id
                        && fact.variant.is_none()
                })
            })
            .or_else(|| {
                input.facts.iter().find(|fact| {
                    fact.provider_id == selected.model.provider_id
                        && fact.model_id == selected.model.model_id
                })
            })
            .expect("pin admission selected a catalog fact")
            .clone();
        fact.variant = selected.prompt_params.variant.clone();
        input.facts = vec![fact];
        input.operator_exclude_keys.clear();
        input.requirements = ModelRequirements { iq: 0.0, eq: 0.0 };
        input
            .required_capabilities
            .get_or_insert_with(Default::default)
            .min_context = Some(0.0);
        let mut priced = self
            .score_input(input, &[], variant.as_deref())
            .map_err(|error| refused(error.reason.as_str()))?;
        priced.score.adequacy = selected.score.adequacy;
        priced.score.raw_adequacy = selected.score.raw_adequacy;
        priced.score.metrics = selected.score.metrics;
        selected.score = priced.score;
        selected.priced_with_basket = priced.priced_with_basket;
        selected.explanation.extend(priced.explanation);
        if let Err(refusal) = self.account_check(&selected) {
            if refusal.code.as_str() != "account_hold" {
                return Err(refusal);
            }
        }
        Ok(selected)
    }

    /// The first `prefer` entry, in list order, that would itself be an
    /// adequate answer to this request among the `usable` routes: it clears the
    /// adequacy floor, and it does not queue on a full concurrency slot unless
    /// the `normal` answer queues too. `None` keeps the normal answer.
    fn preferred(
        &self,
        usable: &HashSet<String>,
        prefer: &[String],
        normal: &SelectedModelRoute,
    ) -> Option<SelectedModelRoute> {
        let normal_queues = normal
            .slot_saturation
            .as_ref()
            .is_some_and(|saturation| saturation.queued_on.is_some());
        prefer.iter().find_map(|entry| {
            let only = select_policy::routes_matching(usable, entry);
            if only.is_empty() {
                return None;
            }
            let listed = self.score(&only).ok()?;
            let queues = listed
                .slot_saturation
                .as_ref()
                .is_some_and(|saturation| saturation.queued_on.is_some());
            (listed.below_floor.is_none() && (normal_queues || !queues)).then_some(listed)
        })
    }

    fn account_check(&self, selected: &SelectedModelRoute) -> Result<(), Refusal> {
        // See the key-pool filter before the candidates: a pin can name such a
        // route without the automatic order ever reaching it.
        if key_pool::is_pool_provider(self.sources.catalog_targets, &selected.model.provider_id) {
            return Err(key_pool_route_refusal());
        }
        let policy = self
            .sources
            .operator_config
            .allocator_policy(&selected.model.provider_id);
        if let Some(classes) = self.classes {
            let wait_limit = self
                .hold_wait_max_s
                .unwrap_or(account_selection::DEFAULT_HOLD_WAIT_S);
            let wait_limit = self
                .sources
                .operator_config
                .select_hold_threshold_s(&selected.model.provider_id)
                .map_or(wait_limit, |threshold| wait_limit.min(threshold));
            match classes
                .probe(
                    &selected.model.provider_id,
                    &selected.model.model_id,
                    self.priced_per_token,
                    true,
                    Some(wait_limit),
                    self.conversation_key,
                )
                .outcome
            {
                prefrontal_routing_core::accounts::Outcome::Admit(_) => return Ok(()),
                prefrontal_routing_core::accounts::Outcome::Hold(hold) => {
                    let mut refusal = Refusal::account_hold(hold.reset_at_ms.max(0) as u64);
                    refusal
                        .detail
                        .insert("retry_after_s".into(), json!(hold.retry_after_s));
                    refusal
                        .detail
                        .insert("reset_at_ms".into(), json!(hold.reset_at_ms));
                    return Err(refusal);
                }
                _ => {}
            }
        }
        let ctx = account_selection::Ctx {
            store: self.deps.store,
            accounts: crate::ACCOUNT_QUOTA.get(),
            flags: self.account_flags,
            policy: &policy,
            now_ms: self
                .classes
                .map_or((self.now * 1000.0) as i64, |classes| classes.now_ms),
            classes: self.classes.cloned(),
        };
        account_selection::preflight_model_for_class(
            &ctx,
            &selected.model.provider_id,
            &selected.model.model_id,
            self.priced_per_token,
        )
        .map_err(|(code, message)| provider_refusal(&code, message))
    }

    /// Record a refused selection in the ledger, as every selection does.
    fn record_refusal(&self, no_route: &NoRouteError, provenance: &DecisionProvenance) {
        let mut decision = crate::decision_input_for(
            self.consumer,
            &self.demand,
            None,
            Some(no_route),
            &crate::SelectParams {
                scope: self.scope.clone(),
                task_id: self.task_ref.map(str::to_owned),
                ..crate::SelectParams::for_record()
            },
            Some(provenance),
            None,
        );
        decision.consumer = Some(self.consumer.to_owned());
        crate::record_failed_selection_decision(self.deps.store.as_ref(), &decision);
    }
}

/// The pin's refusal reason, and when a timed cause ends. A cooldown, a
/// failure backoff and exhausted quota are timed when the route's state
/// names a future deadline for them.
fn pin_reason(
    code: &str,
    snapshot: &ModelRouteStateSnapshot,
    route_key: &str,
    capacity_key: &str,
    now: f64,
) -> (PinUnavailableReason, Option<u64>) {
    let states = [
        snapshot.by_route.get(route_key),
        snapshot.by_capacity.get(capacity_key),
    ];
    let deadline = |pick: fn(&prefrontal_routing_core::RouteRuntimeState) -> Option<f64>| {
        states
            .iter()
            .flatten()
            .filter_map(|state| pick(state))
            .filter(|until| *until > now)
            .max_by(f64::total_cmp)
            .map(|until| until as u64)
    };
    match code {
        "invalid_model_pin" => (PinUnavailableReason::Invalid, None),
        "unknown_model_provider" => (PinUnavailableReason::UnknownProvider, None),
        "unknown_model" => (PinUnavailableReason::UnknownModel, None),
        "invalid_model_variant" => (PinUnavailableReason::InvalidVariant, None),
        "excluded" | "caller_excluded" | "excluded_by_caller" => {
            (PinUnavailableReason::Excluded, None)
        }
        "credential_unentitled" => (PinUnavailableReason::CredentialUnentitled, None),
        "credential_billing_exhausted" => (PinUnavailableReason::CredentialBillingExhausted, None),
        "cooldown_active" => (
            PinUnavailableReason::Cooldown,
            deadline(|state| state.cooldown_until),
        ),
        "backoff_active" => (
            PinUnavailableReason::Backoff,
            deadline(|state| state.failure_backoff_until),
        ),
        "quota_exhausted" | "account_pool_exhausted" => (
            PinUnavailableReason::QuotaExhausted,
            deadline(|state| state.quota_exhausted_until),
        ),
        _ => (PinUnavailableReason::NotAvailable, None),
    }
}

/// Why a pin could not be served, for `detail.pin_reason` on a fell-back
/// answer: a `pin_unavailable` refusal's own reason, otherwise the refusal
/// code (an account or key-pool refusal of the pinned route).
fn refused_pin_reason(refusal: &Refusal) -> String {
    refusal
        .detail
        .get("reason")
        .and_then(Value::as_str)
        .filter(|_| refusal.code == cortexkit_role_routing::errors::RefusalCode::PinUnavailable)
        .map_or_else(|| refusal.code.as_str().to_owned(), str::to_owned)
}

/// The scorer's failure as a v1 `no_route`. Only the routes blocked by
/// exhausted quota, a cooldown or a failure backoff make it timed, by the
/// earliest deadline among them.
fn no_route_refusal(no_route: &NoRouteError, now: f64) -> Refusal {
    let reason = match no_route.reason {
        RouteFailureReason::NoAvailableModel => NoRouteReason::NoAvailableModel,
        RouteFailureReason::MissingRequiredCapability => NoRouteReason::MissingCapability,
        RouteFailureReason::ContextTooSmall => NoRouteReason::ContextTooSmall,
        RouteFailureReason::QuotaOrCooldownExhausted => NoRouteReason::QuotaOrCooldown,
        RouteFailureReason::NoAdequateRoute
        | RouteFailureReason::AllRoutesInadequate
        | RouteFailureReason::InvalidRequirements => NoRouteReason::AllInadequate,
    };
    let reset = (reason == NoRouteReason::QuotaOrCooldown)
        .then_some(no_route.earliest_reset)
        .flatten()
        .filter(|reset| *reset > now)
        .map(|reset| reset as u64);
    let mut refusal = Refusal::no_route(reason, reset);
    refusal.message = format!("{}: {}", refusal.message, no_route.refusal_message());
    refusal
        .detail
        .insert("rejected".into(), json!(no_route.rejected));
    refusal
        .detail
        .insert("blockingCauses".into(), json!(no_route.blocking_causes()));
    refusal
}

/// The refusal for work only a key-pool route could serve. A key-pool provider
/// is a provider Broca reaches only through an API key leased for each send
/// (`key_pool`), and a v1 answer does not name that key yet, so nothing this
/// answer can carry would run the work.
fn key_pool_route_refusal() -> Refusal {
    let mut refusal = Refusal::no_route(NoRouteReason::NoAvailableModel, None);
    refusal.message = format!(
        "{}: every route that meets the request is served through a routing key-pool lease, which a routing/v1 answer cannot name yet",
        refusal.message
    );
    refusal
}

/// One sentence of at most [`EXPLANATION_MAX_CHARS`] characters.
fn clamp_sentence(sentence: String) -> String {
    if sentence.chars().count() <= EXPLANATION_MAX_CHARS {
        return sentence;
    }
    let mut clamped: String = sentence.chars().take(EXPLANATION_MAX_CHARS - 1).collect();
    clamped.push('.');
    clamped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_explanation_is_clamped_to_one_bounded_sentence() {
        let long = "x".repeat(EXPLANATION_MAX_CHARS + 50);
        let clamped = clamp_sentence(long);
        assert_eq!(clamped.chars().count(), EXPLANATION_MAX_CHARS);
        assert!(clamped.ends_with('.'));
    }

    #[test]
    fn only_quota_and_cooldown_refusals_carry_a_reset() {
        let refusal = |reason, reset| NoRouteError {
            adequate_blockers: Default::default(),
            candidates: Vec::new(),
            reason,
            message: "m".into(),
            rejected: Vec::new(),
            requirements: None,
            best_rejected: None,
            earliest_reset: reset,
            earliest_reset_route: None,
        };
        let timed = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(timed.wait_until_ms(), Some(2_000));
        let past = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(500.0)),
            1_000.0,
        );
        assert_eq!(past.wait_until_ms(), None);
        let untimed = no_route_refusal(
            &refusal(RouteFailureReason::AllRoutesInadequate, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(untimed.wait_until_ms(), None);
    }

    #[test]
    fn no_route_reply_keeps_full_rejections_and_routings_cause_groups() {
        let mut error = NoRouteError {
            candidates: Vec::new(),
            reason: RouteFailureReason::QuotaOrCooldownExhausted,
            message: "no admissible model route for requirements".into(),
            rejected: Vec::new(),
            requirements: Some(ModelRequirements {
                iq: 100.0,
                eq: 100.0,
            }),
            best_rejected: None,
            earliest_reset: None,
            earliest_reset_route: None,
            adequate_blockers: Default::default(),
        };
        error
            .rejected
            .push(prefrontal_routing_core::model_router::RejectedRoute {
                route_key: "openai/gpt-6.1-sol".into(),
                reason: "backoff_active".into(),
                detail: Some("three failed sends".into()),
                credential_ids: Vec::new(),
            });
        error
            .adequate_blockers
            .insert("openai/gpt-6.1-sol".into(), Some(2_000.0));
        let reply = no_route_refusal(&error, 1_000.0);
        assert_eq!(reply.detail["rejected"], json!(error.rejected));
        assert_eq!(
            reply.detail["blockingCauses"],
            json!(error.blocking_causes())
        );
        let decision = crate::decision_input_for(
            "mason",
            &ModelRequirements {
                iq: 100.0,
                eq: 100.0,
            },
            None,
            Some(&error),
            &crate::SelectParams::for_record(),
            None,
            None,
        );
        assert_eq!(
            serde_json::from_str::<Value>(&decision.rejected_json).unwrap(),
            reply.detail["rejected"]
        );
        let explanation: Value =
            serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            explanation["blockingCauses"],
            reply.detail["blockingCauses"]
        );
    }
}
