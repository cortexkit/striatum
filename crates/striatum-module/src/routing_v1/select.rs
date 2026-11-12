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
    parse_select_request_serving, Modality, PinFallback, PinOutcome, RouteRef, RunnerAuth,
    RunnerRef, SelectAnswer, SelectRequest, ToolsRequirement, EXPLANATION_MAX_CHARS,
};
use striatum_core::model_router::{
    ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities, RouteFailureReason,
    SelectedModelRoute,
};
use striatum_core::slot_saturation::{ConcurrencySlotState, ConcurrencySlots};
use striatum_core::{fact_to_route, ModelFact, ModelRequirements, RoutingConfig};
use striatum_store::V1Decision;
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
    select_with_profile_lookup_kind(params, caller, deps, &live_profile_lookup, panel_seat).await
}

pub(super) fn live_profile_lookup(
    workload: &str,
    iq: f64,
    facts: &[ModelFact],
) -> crate::select::TaskProfileLookup {
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
}

pub(super) type ProfileLookup =
    dyn Fn(&str, f64, &[ModelFact]) -> crate::select::TaskProfileLookup + Sync;

pub(super) struct Evaluation<'a> {
    pub live_config: std::sync::Arc<config_overrides::LiveConfig>,
    pub operator_config: std::sync::Arc<config_overrides::LiveConfig>,
    pub account_store: &'a crate::SharedStore,
    pub iq: f64,
    pub eq: Option<f64>,
    pub evidence: std::sync::Mutex<striatum_core::model_router::RouteEvaluation>,
}

pub(super) async fn evaluate(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    view: &Evaluation<'_>,
    profile_lookup: &ProfileLookup,
) -> Result<Value, Refusal> {
    select_at_tier(
        params,
        caller,
        deps,
        profile_lookup,
        None,
        None,
        &mut TierTrace::default(),
        &crate::tier_settings::Layers::new(None, None),
        false,
        Some(view),
    )
    .await
}

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
            None,
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
                    None,
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
            None,
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
    let decision = striatum_core::route_state_store::DecisionInput {
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
        anchor_version: crate::config_overrides::live_config(deps.routing_config_path)
            .scoring_anchor_version(),
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
    evaluation: Option<&Evaluation<'_>>,
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
    let live_config = evaluation.map_or_else(
        || config_overrides::live_config(deps.routing_config_path),
        |view| view.live_config.clone(),
    );
    let operator_config = evaluation.map_or_else(
        || operator_config(deps),
        |view| view.operator_config.clone(),
    );
    let consumer = request.consumer.clone();
    let consumers = super::consumers::effective(&operator_config);
    if evaluation.is_none() {
        consumers.admit(&consumer, caller)?;
    }
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

    let operator_policy = operator_config.operator_policy_ref();
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
        catalog_snapshot,
        now,
        mut facts,
        mut snapshot,
        quota_signal_json,
        quota_note: _,
    } = crate::prepare_selection_inputs(
        deps.quota_cache,
        deps.store,
        live_config.pricing(),
        live_config.quota_model_pools_ref(),
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
    let mut priced = crate::select::price_selection_inputs_with_catalog(
        &mut facts,
        quota_signal_json,
        &sources,
        Some(&catalog_snapshot),
        evaluation.map_or(deps.store, |view| view.account_store),
        deps.broca_credentials,
        true,
        priced_per_token,
        evaluation.is_none(),
    )
    .map_err(|error| store_refusal(error.to_string()))?;

    // Runners the caller can send to that reach every tool capability tag the
    // work lists. Tags are matched; a tool provider's module name is not.
    let tags = request
        .requirements
        .tool_capabilities
        .clone()
        .unwrap_or_default();
    let classifier = consumer == "classifier";
    let registry = deps
        .broca_credentials
        .lock()
        .map(|credentials| {
            if classifier {
                return caller
                    .attested_module()
                    .filter(|_| caller.binding_is_current())
                    .map(|module| {
                        super::runners::Registry::classifier(
                            module,
                            &credentials,
                            &facts,
                            now as u64,
                        )
                    })
                    .unwrap_or_default();
            }
            super::runners::Registry::build_for_selection(
                operator_config.as_ref(),
                &credentials,
                &facts,
                now as u64,
                request.harness.as_deref(),
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
            if !runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.requires_wire_api(
                        &runner.module,
                        &route.route_key,
                        &route.capacity_key,
                    )
                })
            {
                return Some((
                    route.route_key,
                    striatum_core::model_router::REASON_RUNNER_REQUIRES_WIRE_API.into(),
                ));
            }
            if !runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.no_runner_account(
                        &runner.module,
                        &route.route_key,
                        &route.capacity_key,
                    )
                })
            {
                return Some((route.route_key, "no_runner_account".into()));
            }
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
                    striatum_core::model_router::REASON_RUNNER_WIRE_FAMILY_UNVERIFIED
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
        .filter(|_| classifier || requirements.questions.is_none())
        .filter(|fact| {
            classifier
                || requirements.max_cost_usd.as_ref().is_none_or(|cap| {
                    deps.broca_credentials.lock().is_ok_and(|credentials| {
                        super::classifier::input_price(&credentials, fact)
                            .zip(cap.as_f64())
                            .is_some_and(|(price, cap)| {
                                price * requirements.input_tokens.unwrap_or(0) as f64 / 1_000_000.0
                                    <= cap
                            })
                    })
                })
        })
        .filter(|fact| {
            !classifier
                || deps.broca_credentials.lock().is_ok_and(|credentials| {
                    caller.attested_module().is_some_and(|module| {
                        super::classifier::eligible(
                            &credentials,
                            module,
                            fact,
                            requirements,
                            now as i64,
                        )
                    })
                })
        })
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
    if capable.is_empty() && pair_rejections.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
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
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
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
        classifier,
        legacy_consumer,
        priced_per_token,
        None,
        priced.subscription_plan_providers.as_ref(),
        operator_config.as_ref(),
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
                striatum_core::accounts::Outcome::Admit(_)
            ) {
                let key = fact_to_route(fact).route_key;
                if usable.remove(&key) && !candidates.rejected.contains_key(&key) {
                    runtime_unavailable.insert(key);
                }
                if let Some(reset) =
                    classes.blocked_reset(&fact.provider_id, &fact.model_id, priced_per_token)
                {
                    account_blocks.insert(striatum_core::fact_to_route(fact).route_key, reset);
                }
            }
        }
    }
    let subscription_keys = candidates.subscription_keys;
    let rejections = candidates.rejected;

    // V1 keeps account walls out of scoring so pinned and ladder requests can
    // still inspect holds. Its ledger must nevertheless record the same timed
    // account exclusions that removed routes from automatic selection.
    let mut account_snapshot = snapshot.clone();
    for fact in &facts {
        let route = fact_to_route(fact);
        if let Some(Some(reset)) = account_blocks.get(&route.route_key) {
            let state = account_snapshot
                .by_capacity
                .entry(route.capacity_key)
                .or_default();
            state.quota_exhausted_until = Some(*reset as f64);
            state.quota_pressure = Some(1.0);
        }
    }
    priced.provenance = priced.provenance.with_account_snapshot(&account_snapshot);

    // Operator score targets allow fractions; keep them in the internal demand
    // rather than sending them through the integer-only request validator.
    let operator_demand =
        operator_config.consumer_policy(&consumer, &Default::default(), &Default::default());
    let demand = if classifier {
        ModelRequirements { iq: 0.0, eq: 0.0 }
    } else {
        ModelRequirements {
            iq: operator_demand
                .iq
                .unwrap_or_else(|| evaluation.map_or(f64::from(requirements.iq), |view| view.iq)),
            eq: operator_demand.eq.unwrap_or_else(|| {
                evaluation
                    .and_then(|view| view.eq)
                    .unwrap_or_else(|| f64::from(requirements.eq.unwrap_or(0)))
            }),
        }
    };
    let vision = modalities.contains(&Modality::Image);
    let required_capabilities = if classifier {
        Some(RequiredCapabilities {
            vision: None,
            min_context: Some(0.0),
        })
    } else {
        match (vision, requirements.context_window) {
            (false, None) if demand.iq > 0.0 || demand.eq > 0.0 => None,
            // The scorer refuses an all-zero demand that names no capability; a
            // zero context floor states "no demand" without adding a constraint.
            (vision, context) => Some(RequiredCapabilities {
                vision: vision.then_some(true),
                min_context: Some(context.map_or(0.0, |floor| floor as f64)),
            }),
        }
    };
    // The selection's exclusion matcher reads a bare provider id as every
    // route of that provider, so `exclude_providers` joins the request's
    // route-key exclusions in the one list every selection path reads.
    let mut exclude: Vec<String> = request.exclude.clone().unwrap_or_default();
    exclude.extend(request.exclude_providers.clone().unwrap_or_default());
    let consumer_exclude = operator_config.consumer_exclusions(&consumer);
    let slots = request.concurrency_slots.as_ref().map(|slots| {
        ConcurrencySlots::new(slots.iter().map(|slot| ConcurrencySlotState {
            key: slot.key.clone(),
            running: u32::try_from(slot.running).unwrap_or(u32::MAX),
            limit: u32::try_from(slot.limit).unwrap_or(u32::MAX),
            queued: u32::try_from(slot.queued.unwrap_or(0)).unwrap_or(u32::MAX),
        }))
    });
    let classifier_order = classifier.then(|| {
        let mut order: Vec<String> = consumer_policy.ladder.iter().flatten().cloned().collect();
        let eligible: Vec<_> = facts
            .iter()
            .filter(|fact| available.contains(&fact_to_route(fact).route_key))
            .cloned()
            .collect();
        let prices = deps
            .broca_credentials
            .lock()
            .map(|credentials| super::classifier::ordered_routes(&credentials, &eligible))
            .unwrap_or_default();
        for route in prices {
            if !order.contains(&route) {
                order.push(route);
            }
        }
        order
    });
    let recovery_available = available
        .iter()
        .filter(|key| {
            consumer_policy.mode != Some(crate::config_policy::ConsumerMode::Ladder)
                || consumer_policy.ladder.iter().flatten().any(|entry| {
                    RouteRef::parse_pin(entry).is_some_and(|route| route.route_key() == **key)
                })
        })
        .cloned()
        .collect();
    let selection = Selection {
        classifier_order,
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
        recovery_available: &recovery_available,
        account_blocks: &account_blocks,
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

    if let Some(view) = evaluation {
        let mut evidence = view.evidence.lock().unwrap();
        let _ = selection.score_candidates_evaluated(&usable, Some(&mut evidence));
    }

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
        && !classifier
        && project_pin.is_none()
        && continuing.is_none()
        && consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
    {
        let mut result = None;
        let mut fallback_hold = None;
        for entry in consumer_policy.ladder.iter().flatten() {
            let Some(route) = RouteRef::parse_pin(entry) else {
                continue;
            };
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
        result.or_else(|| {
            fallback_hold.map(|(selected, _)| Ok((selected, select_policy::OrderBasis::Mode)))
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
                if classifier {
                    return selection.score(&usable);
                }
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
    let laddered =
        classifier || consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder);
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
            if evaluation.is_none() {
                selection.record_refusal(&no_route, &priced.provenance);
            }
            // Account preflight is separate from model scoring. Only name an
            // account block when an otherwise adequate, policy-allowed model
            // would have been selected without that block.
            let blocked: HashSet<_> = account_blocks
                .iter()
                .filter_map(|(key, _)| {
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
                    Some(key.clone())
                })
                .collect();
            if !blocked.is_empty() {
                let mut refusal = Refusal::no_route(
                    NoRouteReason::parse("accounts_blocked"),
                    no_route.earliest_reset.map(|reset| reset as u64),
                );
                if no_route.earliest_reset.is_some() {
                    refusal.message =
                        format!("{}: {}", refusal.message, no_route.refusal_message());
                }
                return Err(refusal);
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

    if evaluation.is_some() {
        return Ok(json!({
            "route": selected.route_key,
            "variant": selected.prompt_params.variant,
            "cost_usd": selected.score.objective.task_pricing.as_ref().filter(|price| price.price_unit == "task").and_then(|price| price.final_cost_usd),
        }));
    }

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
        .allocator_policy_ref(&selected.model.provider_id)
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
        striatum_core::accounts::AllowedAccountClass::from_pay_per_token(priced_per_token);
    let decision_id = deps
        .store
        .record_decision_with_task_ref(
            &decision,
            Some(&runner.module),
            None,
            request.task_ref.as_deref(),
        )
        .map_err(store_refusal)?;
    if let Some(classes) = &priced.classes {
        classes
            .bind_selected_probe(
                deps.store,
                &selected.model.provider_id,
                &selected.model.model_id,
                priced_per_token,
                &decision_id,
            )
            .map_err(store_refusal)?;
    }
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
                "[striatum] WARN: v1 decision {decision_id} for {} has no remembered serving identity: {error}",
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
    // Gateways borrow catalog rows too, but the runner uses the gateway
    // credential's endpoint directly; they are not API-key pools.
    if key_pool::is_pool_provider(&catalog_targets, &route.provider)
        && !fact.is_some_and(|fact| {
            fact.billing_plane
                .as_ref()
                .is_some_and(|plane| plane.auth_method == "gateway")
        })
    {
        let runner_admits_keys = registry.get(&runner.module).is_some_and(|runner| {
            runner
                .routed_accounts
                .iter()
                .any(|method| method == "apikey")
        });
        let mut auth = RunnerAuth {
            pool: Some(route.provider.clone()),
            credential_id: None,
        };
        if runner_admits_keys {
            // The account admission owns the lease. Select supplies only the
            // pool hint, so selecting work never counts as a second active run.
            runner_ref.model.provider = catalog_targets[&route.provider].clone();
        } else {
            let session = format!(
                "v1:{}:{}:{}",
                caller.principal.len(),
                caller.principal,
                request
                    .affinity
                    .as_deref()
                    .or(request.task_ref.as_deref())
                    .unwrap_or(&decision_id),
            );
            let grant = key_pool::acquire(
                &key_pool::PoolCtx {
                    config: Some(&super::operator_config(deps)),
                    store: deps.store,
                    credentials: deps.broca_credentials,
                    catalog_targets: &catalog_targets,
                    now_ms: now_ms as i64,
                },
                &route.provider,
                &route.model,
                route.variant.as_deref(),
                &session,
            )
            .map_err(|refusal| {
                let pin = match basis {
                    Basis::RequestPin => request.pin_slot.as_deref().or(request_pin.as_deref()),
                    Basis::ProjectPin => project_pin.as_deref(),
                    _ => None,
                };
                pool_select_refusal(refusal, pin)
            })?;
            runner_ref.model.provider = grant.runner.provider;
            runner_ref.model.model = grant.runner.model;
            runner_ref.model.variant = grant.runner.variant;
            auth.credential_id = Some(grant.credential_id);
        }
        runner_ref = runner_ref.with_auth(auth);
    }
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
    classifier_order: Option<Vec<String>>,
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
    recovery_available: &'a HashSet<String>,
    account_blocks: &'a HashMap<String, Option<i64>>,
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
        if let Some(order) = &self.classifier_order {
            // Classifier order uses input price, not chat workload profiles or
            // output price. The ordinary scorer still admits each candidate,
            // preserving exclusions, quotas and account fences.
            for route in order {
                let only = available
                    .iter()
                    .filter(|key| *key == route || key.starts_with(&format!("{route}/")))
                    .cloned()
                    .collect();
                if let Ok(selected) = self.score_candidates(&only) {
                    if self.account_check(&selected).is_ok() {
                        return Ok(selected);
                    }
                }
            }
            return self.score_candidates(&HashSet::new());
        }
        self.score_candidates(available)
    }

    fn score_candidates(
        &self,
        available: &HashSet<String>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        self.score_candidates_evaluated(available, None)
    }

    fn scoring_snapshot(&self) -> ModelRouteStateSnapshot {
        let mut snapshot = clone_scoring_snapshot(self.snapshot);
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
        snapshot
    }

    fn score_candidates_evaluated(
        &self,
        available: &HashSet<String>,
        evaluation: Option<&mut striatum_core::model_router::RouteEvaluation>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        let allowed = available
            .iter()
            .filter(|route| {
                self.sources
                    .operator_config
                    .route_allowed(self.consumer, route, false)
            })
            .cloned()
            .collect();
        let snapshot = self.scoring_snapshot();
        let input = self.pass().input(&allowed, snapshot);
        let result = self
            .score_input_evaluated(input, self.consumer_exclude, None, evaluation)
            .map_err(select_policy::describe_filter_refusal)
            .map_err(|mut error| {
                if error.reason == RouteFailureReason::InvalidRequirements {
                    return error;
                }
                let account_resets: HashMap<_, _> = self
                    .facts
                    .iter()
                    .filter_map(|fact| {
                        let key = fact_to_route(fact).route_key;
                        if !self.recovery_available.contains(&key)
                            || !self.sources.operator_config.route_allowed(
                                self.consumer,
                                &key,
                                false,
                            )
                        {
                            return None;
                        }
                        let reset = self
                            .classes
                            .and_then(|classes| {
                                classes.recovery_reset(
                                    &fact.provider_id,
                                    &fact.model_id,
                                    self.priced_per_token,
                                )
                            })
                            .or_else(|| self.account_blocks.get(&key).copied().flatten())?;
                        Some((key, reset as f64))
                    })
                    .collect();
                let mut recovery_available: HashSet<_> = self
                    .recovery_available
                    .iter()
                    .filter(|key| {
                        self.sources.operator_config.route_allowed(self.consumer, key, false)
                            && (!self.account_blocks.contains_key(*key)
                                || account_resets.contains_key(*key))
                    })
                    .cloned()
                    .collect();
                recovery_available.extend(account_resets.keys().cloned());
                let recovery_input = self.pass().input(&recovery_available, self.scoring_snapshot());
                (error.earliest_reset, error.earliest_reset_route) =
                    striatum_core::model_router::earliest_admissible_recovery(
                        &recovery_input,
                        &self.pass().routing_config(),
                        self.consumer_exclude,
                        self.rejections,
                        &account_resets,
                    );
                error
            });
        select_policy::explain_dotted_exclusions(
            result,
            self.sources.operator_config,
            self.consumer,
        )
    }

    fn score_input(
        &self,
        input: striatum_core::SelectModelRouteInput<'_>,
        consumer_exclude: &[String],
        explicit_variant: Option<&str>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        self.score_input_evaluated(input, consumer_exclude, explicit_variant, None)
    }

    fn score_input_evaluated(
        &self,
        mut input: striatum_core::SelectModelRouteInput<'_>,
        consumer_exclude: &[String],
        explicit_variant: Option<&str>,
        evaluation: Option<&mut striatum_core::model_router::RouteEvaluation>,
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
        striatum_core::model_router::evaluate_model_route_with_profiles(
            input,
            &self.pass().routing_config(),
            self.slots,
            consumer_exclude,
            self.rejections,
            &profiles,
            evaluation,
        )
    }

    fn pass(&self) -> select_policy::SelectionPass<'_> {
        select_policy::SelectionPass {
            classifier: self.classifier_order.is_some(),
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
                let mut pin_refusal = Refusal::pin_unavailable(
                    pin,
                    if refusal.detail.get("reason").and_then(Value::as_str)
                        == Some("accounts_blocked")
                    {
                        PinUnavailableReason::QuotaExhausted
                    } else {
                        PinUnavailableReason::NotAvailable
                    },
                    refusal.wait_until_ms(),
                );
                pin_refusal.message = "No account can serve the pinned route right now. \
                    The detail gives the reason and, when known, the time to retry."
                    .into();
                return Err(pin_refusal);
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
        let policy = self
            .sources
            .operator_config
            .allocator_policy_ref(&selected.model.provider_id);
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
                striatum_core::accounts::Outcome::Admit(_) => return Ok(()),
                striatum_core::accounts::Outcome::Hold(hold) => {
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
        account_selection::preflight_model_for_class_with_reset(
            &ctx,
            &selected.model.provider_id,
            &selected.model.model_id,
            self.priced_per_token,
        )
        .map_err(|refusal| {
            if matches!(
                refusal.code.as_str(),
                "no_eligible_account"
                    | "quota_exhausted"
                    | "account_pool_exhausted"
                    | "account_pool_reserve_limited"
            ) {
                account_limit_refusal(None, refusal.reset_at_ms)
            } else {
                provider_refusal(&refusal.code, refusal.message)
            }
        })
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

fn clone_scoring_snapshot(snapshot: &ModelRouteStateSnapshot) -> ModelRouteStateSnapshot {
    #[cfg(test)]
    tests::SCORING_SNAPSHOT_COPIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    snapshot.clone()
}

/// Select exposes account limits as route refusals; account admission's private
/// codes and diagnostic messages are not a retry protocol for select callers.
fn account_limit_refusal(pin: Option<&str>, reset: Option<i64>) -> Refusal {
    let reset = reset.and_then(|reset| u64::try_from(reset).ok());
    let mut refusal = match pin {
        Some(pin) => Refusal::pin_unavailable(pin, PinUnavailableReason::QuotaExhausted, reset),
        None => Refusal::no_route(NoRouteReason::parse("accounts_blocked"), reset),
    };
    refusal.message = "Every account that can serve this route has reached its limit. \
        The detail gives the time an account frees up, when routing knows it."
        .into();
    refusal
}

fn pool_select_refusal(refusal: key_pool::PoolRefusal, pin: Option<&str>) -> Refusal {
    if refusal.code == striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED {
        account_limit_refusal(
            pin,
            refusal
                .detail
                .as_ref()
                .and_then(|detail| detail["earliestResetAtMs"].as_i64()),
        )
    } else {
        provider_refusal(&refusal.code, refusal.message)
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
    let deadline = |pick: fn(&striatum_core::RouteRuntimeState) -> Option<f64>| {
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
        reason if reason.starts_with("not_chat_model:") || reason.starts_with("not_chat_kind:") =>
            (PinUnavailableReason::parse(reason), None),
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

/// The scorer's failure as a v1 `no_route`. Retry timing is independent of the
/// reason: even a quality refusal can have an adequate route behind a timed block.
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
    let reset = no_route
        .earliest_reset
        .filter(|reset| *reset > now)
        .map(|reset| reset as u64);
    let mut refusal = Refusal::no_route(reason, reset);
    let mut diagnostic = no_route.clone();
    diagnostic.earliest_reset = reset.map(|reset| reset as f64);
    refusal.message = format!("{}: {}", refusal.message, diagnostic.refusal_message());
    refusal
        .detail
        .insert("rejected".into(), json!(no_route.rejected));
    refusal
        .detail
        .insert("blockingCauses".into(), json!(no_route.blocking_causes()));
    if no_route.reason == RouteFailureReason::NoAdequateRoute {
        refusal.detail.insert(
            "adequacyFloor".into(),
            json!(striatum_core::routing_config::ADEQUACY_FLOOR),
        );
        refusal
            .detail
            .insert("bestRejected".into(), json!(no_route.best_rejected));
    }
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
    use super::super::{
        conformance_tests::{install_catalog, Harness},
        dispatch_tests::configured,
    };
    use super::*;
    use cortexkit_role_routing_conformance::fixture;
    use striatum_core::RouteStateStore;

    pub(super) static SCORING_SNAPSHOT_COPIES: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    #[tokio::test]
    async fn successful_select_does_not_copy_a_recovery_snapshot() {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (_, route) = configured().await;
        SCORING_SNAPSHOT_COPIES.store(0, std::sync::atomic::Ordering::Relaxed);
        let reply = route
            .call(
                "route.select",
                &json!({"consumer":fixture::SUMMARY, "requirements":{"iq":50}}),
            )
            .await;
        assert_eq!(reply.0, subc_protocol::FrameType::Response, "{reply:?}");
        let copies = SCORING_SNAPSHOT_COPIES.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            copies, 1,
            "only the existing scoring copy is needed; recovery must stay lazy"
        );
    }

    const RECOVERY_NOW: u64 = 1_800_000_000_000;
    const ACCOUNT_RESET: u64 = RECOVERY_NOW + 600_000;
    const DISTANT_WALL: u64 = RECOVERY_NOW + 864_000_000;
    const ACCOUNT_ROUTE: &str = "example-subscription/model-c";
    const MANUAL_ROUTE: &str = "example-subscription/model-d";
    const WALL_ROUTE: &str = "example-provider/model-a";

    fn subscription_recovery_catalog(harness: &Harness) {
        let mut catalog = fixture::catalog();
        for model in &mut catalog.models {
            model.priced_per_token = false;
        }
        let mut cheap_account = catalog.accounts[0].clone();
        cheap_account.provider = fixture::PRICED_PROVIDER.into();
        cheap_account.credential_id = "cheap-credential".into();
        cheap_account.account_id = "cheap-account".into();
        catalog.accounts.push(cheap_account);
        install_catalog(harness, &catalog);
    }

    fn fence_recovery_accounts(harness: &Harness) {
        for (index, account) in fixture::catalog().accounts.into_iter().enumerate() {
            if index == 1 {
                let fingerprint = match harness
                    .credentials()
                    .lock()
                    .unwrap()
                    .listing_fingerprint(&account.credential_id)
                {
                    crate::broca_credentials::ListingLookup::Listed(fingerprint) => fingerprint,
                    _ => panic!("fixture credential must be listed"),
                };
                harness
                    .store()
                    .record_account_eligibility(&striatum_store::AccountEligibilityFact {
                        provider: fixture::SUBSCRIPTION_PROVIDER.into(),
                        credential_id: account.credential_id,
                        model: String::new(),
                        listing_fingerprint: Some(fingerprint),
                        code: "ineligible".into(),
                        observed_at: RECOVERY_NOW as i64,
                    })
                    .unwrap();
                continue;
            }
            crate::credential_fences::record(
                harness.store(),
                &harness.credentials(),
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: fixture::SUBSCRIPTION_PROVIDER.into(),
                    credential_id: account.credential_id,
                    code: "billing_exhausted".into(),
                },
                RECOVERY_NOW as i64,
            )
            .unwrap();
        }
    }

    fn recovery_wall(harness: &Harness, key: &str, until: u64) {
        harness
            .store()
            .update_route_state(
                key,
                key,
                &striatum_core::RouteStateUpdate {
                    cooldown_until: Some(until as f64),
                    observed_at_ms: Some(RECOVERY_NOW as i64 + 1),
                    wall_class: Some(striatum_core::RouteWallClass::Quota),
                    last_error: Some("quota exhausted".into()),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    fn exhaust_recovery_accounts(reset: Option<u64>) {
        let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
        for reading in quota.readings.readings.values_mut() {
            if reading.entry["apiProvider"] != fixture::SUBSCRIPTION_PROVIDER {
                continue;
            }
            reading.received_at_ms = RECOVERY_NOW as i64;
            reading.entry["fetchedAt"] =
                json!(crate::account_selection::rfc3339(RECOVERY_NOW as i64));
            for name in ["primary", "secondary"] {
                if let Some(window) = reading.entry["usage"][name].as_object_mut() {
                    window.insert("usedPercent".into(), json!(100.0));
                    match reset {
                        Some(reset) => {
                            window.insert(
                                "resetsAt".into(),
                                json!(crate::account_selection::rfc3339(reset as i64)),
                            );
                        }
                        None => {
                            window.remove("resetsAt");
                        }
                    }
                }
            }
        }
    }

    async fn recovery_reply(policy: Value, exhaust: bool) -> Refusal {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (harness, route) = configured().await;
        subscription_recovery_catalog(&harness);
        harness.edit_config(|config| {
            config["models"][MANUAL_ROUTE] = policy;
        });
        recovery_wall(&harness, MANUAL_ROUTE, DISTANT_WALL);
        if exhaust {
            exhaust_recovery_accounts(Some(ACCOUNT_RESET));
            fence_recovery_accounts(&harness);
            recovery_wall(&harness, WALL_ROUTE, ACCOUNT_RESET + 900_000);
        } else {
            recovery_wall(&harness, ACCOUNT_ROUTE, ACCOUNT_RESET);
        }
        let available = if exhaust {
            vec![ACCOUNT_ROUTE, MANUAL_ROUTE, WALL_ROUTE]
        } else {
            vec![ACCOUNT_ROUTE, MANUAL_ROUTE]
        };
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "requirements":{"iq":50},
                    "available_routes":available, "hold_wait_max_s":0
                }),
            )
            .await;
        assert_eq!(reply.1["code"], "no_route", "{reply:?}");
        serde_json::from_value(reply.1).unwrap()
    }

    #[tokio::test]
    async fn no_route_recovery_ignores_pin_only_wall_for_account_reset() {
        let reply = recovery_reply(json!({"pin_only":true}), true).await;
        assert_eq!(reply.detail["reason"], "quota_or_cooldown", "{reply:?}");
        assert!(
            reply.detail["rejected"]
                .as_array()
                .unwrap()
                .iter()
                .any(|route| route["routeKey"] == ACCOUNT_ROUTE
                    && route["reason"] == "no_eligible_account"),
            "{reply:?}"
        );
        assert_eq!(reply.wait_until_ms(), Some(ACCOUNT_RESET), "{reply:?}");
    }

    #[tokio::test]
    async fn no_route_recovery_uses_admissible_wall_with_other_routes_excluded() {
        let reply = recovery_reply(json!({"not_for":[fixture::SUMMARY]}), false).await;
        assert_eq!(reply.wait_until_ms(), Some(ACCOUNT_RESET), "{reply:?}");
    }

    #[tokio::test]
    async fn no_route_recovery_message_names_the_account_route_and_reset() {
        let reply = recovery_reply(json!({"pin_only":true}), true).await;
        assert!(
            reply.message.contains(&format!(
                "Earliest route wall expiry: {ACCOUNT_ROUTE} (until_ms={ACCOUNT_RESET})"
            )),
            "{reply:?}"
        );
        assert!(
            !reply
                .message
                .contains(&format!("Earliest route wall expiry: {MANUAL_ROUTE}")),
            "{reply:?}"
        );
    }

    #[tokio::test]
    async fn no_route_recovery_only_operator_excluded_walls_is_untimed() {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (harness, route) = configured().await;
        harness.edit_config(|config| {
            config["providers"][fixture::SUBSCRIPTION_PROVIDER]["pin_only"] = json!(true);
        });
        recovery_wall(&harness, ACCOUNT_ROUTE, ACCOUNT_RESET);
        recovery_wall(&harness, MANUAL_ROUTE, DISTANT_WALL);
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "requirements":{"iq":50},
                    "available_routes":[ACCOUNT_ROUTE, MANUAL_ROUTE]
                }),
            )
            .await;
        assert_eq!(reply.1["code"], "no_route", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), None, "{refusal:?}");
        assert!(
            !refusal.message.contains("Earliest route wall expiry"),
            "{refusal:?}"
        );
    }

    #[tokio::test]
    async fn no_route_recovery_all_inadequate_waits_for_adequate_accounts() {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (harness, route) = configured().await;
        subscription_recovery_catalog(&harness);
        harness.edit_config(|config| {
            config["consumers"][fixture::SUMMARY]["below_bar"] = json!("refuse");
        });
        for mut fact in harness.store().get_facts().unwrap() {
            fact.scores.elo = if fact.provider_id == fixture::SUBSCRIPTION_PROVIDER {
                2000.0
            } else {
                400.0
            };
            harness
                .store()
                .upsert_fact(
                    &fact,
                    striatum_core::route_state_store::FactSource::Override,
                )
                .unwrap();
        }
        exhaust_recovery_accounts(Some(ACCOUNT_RESET));
        fence_recovery_accounts(&harness);
        let reply = route
            .call(
                "route.select",
                &json!({
                    "consumer":fixture::SUMMARY, "requirements":{"iq":100}, "hold_wait_max_s":0
                }),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "all_inadequate", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), Some(ACCOUNT_RESET), "{refusal:?}");
    }

    #[tokio::test]
    async fn no_route_recovery_accounts_blocked_keeps_a_known_reset_despite_an_unknown_account() {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (_, route) = configured().await;
        exhaust_recovery_accounts(Some(ACCOUNT_RESET));
        {
            let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
            let reading = quota.readings.readings.values_mut().next().unwrap();
            for name in ["primary", "secondary"] {
                reading.entry["usage"][name]
                    .as_object_mut()
                    .unwrap()
                    .remove("resetsAt");
            }
        }
        let reply = route
            .call(
                "route.select",
                &json!({"consumer":fixture::SUMMARY,
            "requirements":{"iq":50}, "available_routes":[ACCOUNT_ROUTE], "hold_wait_max_s":0}),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "accounts_blocked", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), Some(ACCOUNT_RESET), "{refusal:?}");
    }

    #[tokio::test]
    async fn no_route_recovery_accounts_blocked_without_known_resets_is_untimed() {
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let (_, route) = configured().await;
        exhaust_recovery_accounts(None);
        let reply = route
            .call(
                "route.select",
                &json!({"consumer":fixture::SUMMARY,
            "requirements":{"iq":50}, "available_routes":[ACCOUNT_ROUTE], "hold_wait_max_s":0}),
            )
            .await;
        assert_eq!(reply.1["detail"]["reason"], "accounts_blocked", "{reply:?}");
        let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.wait_until_ms(), None, "{refusal:?}");
    }

    #[test]
    fn a_long_explanation_is_clamped_to_one_bounded_sentence() {
        let long = "x".repeat(EXPLANATION_MAX_CHARS + 50);
        let clamped = clamp_sentence(long);
        assert_eq!(clamped.chars().count(), EXPLANATION_MAX_CHARS);
        assert!(clamped.ends_with('.'));
    }

    #[test]
    fn known_recoveries_time_quota_and_quality_refusals() {
        let refusal = |reason, reset| NoRouteError {
            adequate_blockers: Default::default(),
            candidates: Vec::new(),
            reason,
            message: "m".into(),
            rejected: Vec::new(),
            requirements: None,
            best_rejected: None,
            earliest_reset: reset,
            earliest_reset_route: Some("provider/model".into()),
        };
        let timed = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(timed.wait_until_ms(), Some(2_000));
        assert!(timed.message.contains("provider/model (until_ms=2000)"));
        let past = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(500.0)),
            1_000.0,
        );
        assert_eq!(past.wait_until_ms(), None);
        assert!(!past.message.contains("Earliest route wall expiry"));
        let quality = no_route_refusal(
            &refusal(RouteFailureReason::AllRoutesInadequate, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(quality.wait_until_ms(), Some(2_000));
        let untimed = no_route_refusal(
            &refusal(RouteFailureReason::AllRoutesInadequate, None),
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
            .push(striatum_core::model_router::RejectedRoute {
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
    #[test]
    fn key_pool_acquire_refusals_preserve_only_known_resets() {
        for pin in [Some("pool/model"), None] {
            for reset in [Some(1_800_000_600_000_i64), None] {
                let refusal = pool_select_refusal(
                    key_pool::PoolRefusal {
                        code: striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED
                            .into(),
                        message: "private message is not a retry protocol".into(),
                        detail: Some(json!({"earliestResetAtMs":reset})),
                    },
                    pin,
                );
                assert_eq!(refusal.wait_until_ms(), reset.map(|reset| reset as u64));
                let expected = match pin {
                    Some(pin) => Refusal::pin_unavailable(
                        pin,
                        PinUnavailableReason::QuotaExhausted,
                        reset.map(|reset| reset as u64),
                    ),
                    None => Refusal::no_route(
                        NoRouteReason::parse("accounts_blocked"),
                        reset.map(|reset| reset as u64),
                    ),
                };
                assert_eq!(refusal.code, expected.code);
                assert_eq!(refusal.detail, expected.detail);
            }
        }
    }
}
