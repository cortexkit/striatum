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

use cortexkit_role_routing::errors::{NoRouteReason, PinUnavailableReason, Refusal, RefusalCode};
use cortexkit_role_routing::select::{
    parse_select_request_serving, parse_select_request_with_defaults, Modality, PinFallback,
    PinOrigin, PinOutcome, RouteRef, RunnerAuth, RunnerRef, SelectAnswer, SelectRequest,
    ToolsRequirement, EXPLANATION_MAX_CHARS,
};
use serde_json::{json, Value};
use striatum_core::model_router::{
    ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities, RouteFailureReason,
    SelectedModelRoute,
};
use striatum_core::slot_saturation::{ConcurrencySlotState, ConcurrencySlots};
use striatum_core::{fact_to_route, ModelFact, ModelRequirements, RoutingConfig};
use striatum_store::V1Decision;

use super::{now_ms, operator_config, provider_refusal, store_refusal, Caller, Deps};
use crate::config_policy::RunnerPolicy;
use crate::{
    account_selection, config_overrides, key_pool, select_policy, work_model_pin,
    DecisionProvenance, SelectionPolicySources,
};

#[cfg(test)]
mod consumer_pin_tests;
#[cfg(test)]
mod declared_samples_tests;
mod eligible_routes;
#[cfg(test)]
mod eligible_routes_tests;
mod price_answer;
#[cfg(test)]
mod price_answer_tests;

/// The members a v1 select may carry.
const MEMBERS: &[&str] = &[
    "settings",
    "pin_slot",
    "available_routes",
    "eligible_routes",
    "prefer_prompt_cache",
    "consumer",
    "requirements",
    "pin",
    "pin_origin",
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
    ConsumerPin,
    ProjectPin,
    Affinity,
    Preferred,
    Automatic,
}

impl Basis {
    fn as_str(self) -> &'static str {
        match self {
            Basis::RequestPin => "the request's pin",
            Basis::ConsumerPin => "the consumer's operator pin",
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
    super::consumers::record_select("routing/v1", params, &super::operator_config(deps));
    let capabilities = super::describe::capabilities();
    if capabilities.contains(&"select_kind") && capabilities.contains(&"select_media") {
        let config = super::operator_config(deps);
        let consumers = super::consumers::effective(&config);
        let declared = capabilities
            .contains(&"consumer_declarations")
            .then(|| {
                params
                    .get("consumer")
                    .and_then(Value::as_str)
                    .map(|consumer| consumers.defaults_for(consumer))
            })
            .flatten();
        let sent = params
            .get("kind")
            .filter(|kind| !kind.is_null())
            .map(|kind| {
                kind.as_str()
                    .ok_or_else(|| Refusal::invalid_request("kind", "expected a string"))
            })
            .transpose()?;
        // Declaration aliases are catalog spellings, not additional wire kinds.
        if sent.is_some_and(|kind| {
            kind != "llm" && kind != "search" && !super::engines::MEDIA_KINDS.contains(&kind)
        }) {
            return Err(Refusal::invalid_request(
                "kind",
                "kind is not served by select_media",
            ));
        }
        let kind = super::consumers::kind::resolve(
            declared
                .as_ref()
                .and_then(|defaults| defaults.kind.as_ref())
                .map(|kind| kind.as_str()),
            sent,
        )?;
        if kind != "llm" {
            let mut resolved = params.clone();
            resolved
                .as_object_mut()
                .ok_or_else(|| Refusal::invalid_request("params", "expected an object"))?
                .insert("kind".into(), kind.into());
            // Media never enters the LLM billing wait or merges LLM-only defaults.
            if kind == "search" {
                return super::engines::select_search(&resolved, caller, deps);
            }
            return super::engines::select_media(&resolved, caller, deps);
        }
        if params.get("kind").is_some() {
            let mut resolved = params.clone();
            resolved.as_object_mut().unwrap().remove("kind");
            return select_kind(&resolved, caller, deps, false).await;
        }
    }
    select_kind(params, caller, deps, false).await
}

pub(super) async fn panel_seat(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    shared: &RequestInputs,
) -> Result<Value, Refusal> {
    select_with_profile_lookup_kind(
        params,
        caller,
        deps,
        &live_profile_lookup,
        true,
        Some(shared),
    )
    .await
}

async fn select_kind(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    select_with_profile_lookup_kind(params, caller, deps, &live_profile_lookup, panel_seat, None)
        .await
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
    pub accounts: &'a crate::account_selection::Sweep,
    pub inputs: &'a RequestInputs,
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
    let shared = RequestInputs::read(deps, Some(view))?;
    evaluate_with_inputs(params, caller, deps, view, profile_lookup, &shared, false).await
}

pub(super) async fn evaluate_with_inputs(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    view: &Evaluation<'_>,
    profile_lookup: &ProfileLookup,
    shared: &RequestInputs,
    panel_seat: bool,
) -> Result<Value, Refusal> {
    let mut inputs = prepare_selection(params, caller, deps, shared, Some(view)).await?;
    if panel_seat {
        let (layers, status) = selection_settings(params, &inputs.request, caller, deps).await?;
        let request = inputs.request.clone();
        return select_with_tier_fallback(
            params,
            caller,
            deps,
            profile_lookup,
            &request,
            &layers,
            status,
            true,
            shared,
            &mut inputs,
            Some(view),
        )
        .await;
    }
    select_at_tier(
        params,
        caller,
        deps,
        profile_lookup,
        None,
        None,
        &mut TierTrace::default(),
        &crate::tier_settings::Layers::new(None, None),
        panel_seat,
        Some(view),
        shared,
        &mut inputs,
    )
    .await
}

#[cfg(test)]
pub(super) async fn select_with_profile_lookup(
    params: &serde_json::Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
) -> Result<Value, Refusal> {
    select_with_profile_lookup_kind(params, caller, deps, profile_lookup, false, None).await
}

async fn select_with_profile_lookup_kind(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &ProfileLookup,
    panel_seat: bool,
    shared: Option<&RequestInputs>,
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
    let config = shared.map_or_else(
        || operator_config(deps),
        |inputs| inputs.operator_config.clone(),
    );
    let consumers = super::consumers::effective(&config);
    let request = parse_admitted_request(params, caller, &consumers, false)?;
    let (settings_layers, project_settings) =
        selection_settings(params, &request, caller, deps).await?;
    let mut effective_request = request.clone();
    let mut result = async {
        let owned;
        let shared = match shared {
            Some(shared) => shared,
            None => {
                owned = RequestInputs::read(deps, None)?;
                &owned
            }
        };
        let mut inputs = prepare_selection(params, caller, deps, shared, None).await?;
        effective_request = inputs.request.clone();
        select_with_tier_fallback(
            params,
            caller,
            deps,
            profile_lookup,
            &request,
            &settings_layers,
            project_settings,
            panel_seat,
            shared,
            &mut inputs,
            None,
        )
        .await
    }
    .await;
    if let (Err(refusal), Some(slot)) = (&mut result, &request.pin_slot) {
        if refusal.code.as_str() == "pin_unavailable" {
            refusal.detail.insert("pin".into(), json!(slot));
        }
    }
    if let Err(refusal) = &result {
        if (effective_request.pin.is_some() || effective_request.pin_slot.is_some())
            && effective_request.pin_fallback != Some(PinFallback::Route)
            && (matches!(
                refusal.code,
                RefusalCode::NoRoute
                    | RefusalCode::PinUnavailable
                    | RefusalCode::AccountHold
                    | RefusalCode::AuthRequired
            ) || refusal.code.as_str() == "provider_requires_runner_pin")
        {
            // Record the final refusal, not unsuccessful probes of higher tiers.
            // Hard pins bypass scoring, which normally writes the refusal decision.
            record_pinned_refusal(&effective_request, refusal, caller, deps);
        }
    }
    result
}

async fn selection_settings(
    params: &Value,
    request: &SelectRequest,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<
    (
        crate::tier_settings::Layers,
        Option<cortexkit_role_routing::select::ProjectSettings>,
    ),
    Refusal,
> {
    Ok(if let Some(settings) = &request.settings {
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
    })
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
    shared: &RequestInputs,
    inputs: &mut SelectionInputs,
    evaluation: Option<&Evaluation<'_>>,
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
            evaluation,
            shared,
            inputs,
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
                    evaluation,
                    shared,
                    inputs,
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
            evaluation,
            shared,
            inputs,
        )
        .await
        {
            Ok(answer) => return Ok(answer),
            Err(error)
                if matches!(
                    error.code,
                    RefusalCode::NoRoute
                        | RefusalCode::PinUnavailable
                        | RefusalCode::AccountHold
                        | RefusalCode::AuthRequired
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
            refusal.code,
            RefusalCode::AccountHold | RefusalCode::AuthRequired
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

/// Catalog/configuration data shared by a tier fallback or all seats of a panel.
/// Account evidence and route state are deliberately not retained here: an
/// earlier seat can consume a probe or add an in-flight request.
#[derive(Clone)]
pub(super) struct RequestInputs {
    pub(super) live_config: std::sync::Arc<config_overrides::LiveConfig>,
    pub(super) operator_config: std::sync::Arc<config_overrides::LiveConfig>,
    pub(super) catalog_snapshot: std::sync::Arc<striatum_store::CatalogFactsSnapshot>,
    facts: Vec<ModelFact>,
    pub(super) catalog_targets: std::collections::BTreeMap<String, String>,
    policy_memo: std::sync::Arc<config_overrides::SelectionPolicyMemo>,
}

impl RequestInputs {
    pub(super) fn read(
        deps: &Deps<'_>,
        evaluation: Option<&Evaluation<'_>>,
    ) -> Result<Self, Refusal> {
        if let Some(view) = evaluation {
            return Ok(view.inputs.clone());
        }
        let live_config = evaluation.map_or_else(
            || config_overrides::live_config(deps.routing_config_path),
            |view| view.live_config.clone(),
        );
        let operator_config = evaluation.map_or_else(
            || operator_config(deps),
            |view| view.operator_config.clone(),
        );
        Self::read_with_config(deps, live_config, operator_config)
    }

    pub(super) fn read_with_config(
        deps: &Deps<'_>,
        live_config: std::sync::Arc<config_overrides::LiveConfig>,
        operator_config: std::sync::Arc<config_overrides::LiveConfig>,
    ) -> Result<Self, Refusal> {
        let (catalog_snapshot, facts) = deps
            .store
            .get_facts_with_observed_speed_and_generation(now_ms() as f64, live_config.pricing())
            .map_err(store_refusal)?;
        let catalog_targets = deps
            .broca_credentials
            .lock()
            .map(|credentials| credentials.catalog_targets(&facts))
            .unwrap_or_default();
        #[cfg(test)]
        count_input_build(&operator_config, 0);
        Ok(Self {
            live_config,
            operator_config,
            catalog_snapshot,
            facts,
            catalog_targets,
            policy_memo: Default::default(),
        })
    }

    pub(super) fn prepare_for_sweep(
        &self,
        deps: &Deps<'_>,
        sweep: Option<&crate::account_selection::Sweep>,
    ) -> Result<crate::PreparedSelectionInputs, Refusal> {
        let now = now_ms() as f64;
        // Read after the previous seat's admission, before this seat reserves
        // its own probes. Only the immutable catalog is shared between seats.
        let mut snapshot = match sweep {
            Some(sweep) => sweep.route_snapshot(now),
            None => crate::account_state::published(deps.store, now as i64)
                .and_then(|blocks| blocks.route_state_snapshot(now)),
        }
        .map_err(store_refusal)?;
        let (
            windows,
            available,
            refreshed_at_ms,
            retained_degraded_count,
            last_error,
            restored_from_ms,
        ) = match deps.quota_cache.lock() {
            Ok(cache) => (
                cache.windows.clone(),
                cache.available,
                cache.refreshed_at_ms,
                cache.retained_degraded_since_ms.len(),
                cache.last_error.clone(),
                cache.restored_from_ms,
            ),
            Err(_) => (Vec::new(), false, 0, 0, Some("cache poisoned".into()), None),
        };
        let stale_age_ms = (!windows.is_empty())
            .then(|| crate::stale_quota_age_ms(refreshed_at_ms, now as u64))
            .flatten();
        if stale_age_ms.is_none() {
            striatum_core::merge_quota_overlay(
                &mut snapshot,
                &windows,
                &self.facts,
                self.live_config.quota_model_pools_ref(),
            );
        }
        crate::subscription_economics::overlay(
            &mut snapshot,
            &self.facts,
            now as i64,
            self.live_config.pricing(),
        );
        let quota_signal_json = crate::select::quota_signal_json(
            &snapshot,
            &crate::select::QuotaSignalView {
                windows: &windows,
                available,
                refreshed_at_ms,
                retained_degraded_count,
                last_error: last_error.as_deref(),
                restored_from_ms,
                stale_age_ms,
            },
        );
        Ok(crate::PreparedSelectionInputs {
            catalog_snapshot: self.catalog_snapshot.clone(),
            now,
            facts: self.facts.clone(),
            snapshot,
            quota_signal_json,
            quota_note: stale_age_ms.map(|age| crate::stale_quota_note(age, last_error.as_deref())),
        })
    }
}

#[cfg(test)]
static INPUT_BUILDS: std::sync::Mutex<std::collections::BTreeMap<usize, [usize; 3]>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
static EVIDENCE_PASSES: std::sync::Mutex<std::collections::BTreeMap<std::path::PathBuf, usize>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
pub(super) fn evidence_passes(path: &std::path::Path) -> usize {
    *EVIDENCE_PASSES.lock().unwrap().get(path).unwrap_or(&0)
}

#[cfg(test)]
fn count_input_build(config: &std::sync::Arc<config_overrides::LiveConfig>, stage: usize) {
    INPUT_BUILDS
        .lock()
        .unwrap()
        .entry(std::sync::Arc::as_ptr(config) as usize)
        .or_default()[stage] += 1;
}

/// A single selection owns the probe reservation in this pristine baseline.
/// Every tier attempt clones it before applying tier or sticky-account filters.
struct SelectionInputs {
    request: SelectRequest,
    consumer_pinned: bool,
    now: f64,
    facts: Vec<ModelFact>,
    snapshot: ModelRouteStateSnapshot,
    priced: crate::select::PricedSelection,
    registry: super::runners::Registry,
    runners: Vec<RunnerPolicy>,
    consumer_policy: crate::config_policy::ConsumerPolicy,
    priced_per_token: bool,
}

fn parse_admitted_request(
    params: &Value,
    caller: &Caller,
    consumers: &super::consumers::Registry,
    evaluation: bool,
) -> Result<SelectRequest, Refusal> {
    let capabilities = super::describe::capabilities();
    let params = crate::tier_settings::without_context(params);
    if capabilities
        .iter()
        .any(|cap| *cap == "consumer_declarations")
    {
        if let Some(consumer) = params.get("consumer").and_then(Value::as_str) {
            let defaults = consumers.defaults_for(consumer);
            if defaults.iq.is_some() {
                // A declaration relaxes required IQ only for its admitted callers.
                // Requests without a declared IQ default keep using
                // parse_select_request_serving, so their refusals are unchanged.
                if !evaluation {
                    consumers.admit(consumer, caller)?;
                }
                return parse_select_request_with_defaults(&params, &capabilities, &defaults);
            }
        }
    }
    parse_select_request_serving(&params, &capabilities)
}

async fn prepare_selection(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    shared: &RequestInputs,
    evaluation: Option<&Evaluation<'_>>,
) -> Result<SelectionInputs, Refusal> {
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
    let live_config = &shared.live_config;
    let operator_config = &shared.operator_config;
    let consumers = super::consumers::effective(&operator_config);
    let request = parse_admitted_request(params, caller, &consumers, evaluation.is_some())?;
    let consumer = request.consumer.clone();
    if evaluation.is_none() {
        consumers.admit(&consumer, caller)?;
    }
    let declared = super::describe::capabilities()
        .contains(&"consumer_declarations")
        .then(|| consumers.defaults_for(&consumer))
        .unwrap_or_default();
    let kind =
        super::consumers::kind::resolve(declared.kind.as_ref().map(|kind| kind.as_str()), None)?;
    if kind != "llm" {
        let mut refusal = Refusal::no_route(NoRouteReason::NoAvailableModel, None);
        refusal.message = format!("consumer kind {kind} must use route.select_engine");
        return Err(refusal);
    }
    let effective_params = consumers.with_defaults(&consumer, params, &operator_config);
    let mut request =
        parse_admitted_request(&effective_params, caller, &consumers, evaluation.is_some())?;
    // Validate the marker for every caller, but only enrolled principals may
    // use it to spend the pinned model's subscription reserve.
    if !operator_config.honours_pin_origin(&caller.principal) {
        request.pin_origin = None;
    }
    if request.eligible_routes.as_ref().is_some_and(Vec::is_empty) {
        return Err(Refusal::no_route(NoRouteReason::NoAvailableModel, None));
    }
    if !crate::broca_credentials::wait_for_billing_catalog(deps.broca_credentials, BILLING_WAIT)
        .await
    {
        return Err(provider_refusal(
            "routing_not_ready",
            "routing pricing is awaiting its first billing-plane load; retry shortly",
        ));
    }

    // Payment class comes only from the resolved consumer policy, not from the
    // consumer's name.
    let legacy_consumer = select_policy::Consumer::from_id(&consumer);
    // Keep the declared ladder separate from settings policy looked up by
    // consumer name: a settings child belonging to another owner must not
    // inherit it.
    let ladder = super::describe::capabilities()
        .contains(&"consumer_declarations")
        .then(|| consumers.defaults_for(&consumer).ladder)
        .flatten();
    let consumer_policy = operator_config.consumer_policy(
        &consumer,
        &crate::config_policy::ConsumerPolicy {
            iq: Some(f64::from(request.requirements.iq)),
            eq: request.requirements.eq.map(f64::from),
            mode: ladder
                .as_ref()
                .map(|_| crate::config_policy::ConsumerMode::Ladder),
            ladder,
            ladder_source: Some(crate::config_policy::ConsumerLadderSource::Declaration),
            ..Default::default()
        },
        &consumers.policies,
    );
    // An operator's consumer pin is a hard selection, not a one-route ladder:
    // it bypasses automatic pin-only fences but still uses all pin admission
    // and account checks. Never let a caller's fallback preference replace it.
    let consumer_pinned =
        request.pin.is_none() && request.pin_slot.is_none() && consumer_policy.pin.is_some();
    if consumer_pinned {
        let pin = consumer_policy.pin.as_ref().unwrap();
        request.pin = Some(pin.variant.as_ref().map_or_else(
            || pin.model.clone(),
            |variant| format!("{}:{variant}", pin.model),
        ));
        request.pin_fallback = None;
    }
    let priced_per_token = select_policy::priced_per_token(&consumer_policy, legacy_consumer);
    let crate::PreparedSelectionInputs {
        catalog_snapshot,
        now,
        mut facts,
        snapshot,
        quota_signal_json,
        quota_note: _,
    } = shared.prepare_for_sweep(deps, evaluation.map(|view| view.accounts))?;
    let catalog_targets = &shared.catalog_targets;
    let sources = SelectionPolicySources {
        live_config: &live_config,
        operator_config: &operator_config,
        operator_config_path: deps.operator_config_path,
        catalog_targets: &catalog_targets,
    };
    let priced = crate::select::price_selection_inputs_for_sweep(
        &mut facts,
        quota_signal_json,
        &sources,
        Some(&catalog_snapshot),
        deps.store,
        deps.broca_credentials,
        true,
        priced_per_token,
        evaluation.is_none(),
        evaluation.map(|view| view.accounts),
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
    let placement = if declared.placement.is_some() {
        let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
        super::consumers::declared::placement_module(
            declared.placement.as_ref(),
            caller,
            request.runners.as_deref(),
            |module| credentials.runner_declarations.contains_key(module),
        )?
    } else {
        None
    };
    let registry = deps
        .broca_credentials
        .lock()
        .map(|credentials| {
            if classifier {
                return caller
                    .attested_module()
                    .filter(|_| caller.binding_is_current())
                    .map(|module| {
                        super::runners::Registry::classifier_for_consumer(
                            module,
                            operator_config,
                            &consumer_policy,
                            &credentials,
                            &facts,
                            now as u64,
                        )
                    })
                    .unwrap_or_default();
            }
            super::runners::Registry::build_for_task(
                super::runners::RunnerPolicies::for_consumer(operator_config, &consumer_policy),
                &credentials,
                &facts,
                now as u64,
                request.harness.as_deref(),
                &request.requirements,
            )
        })
        .unwrap_or_default();
    let runners: Vec<RunnerPolicy> = registry
        .task_policies()
        .into_iter()
        .filter(|runner| {
            consumer_policy
                .runner
                .as_ref()
                .is_none_or(|module| module == &runner.module)
        })
        .collect();
    if runners.is_empty() {
        return Err(if let Some(harness) = &request.harness {
            let mut refusal = Refusal::no_route(NoRouteReason::HarnessUnavailable, None);
            refusal.message = format!("no session-starting runner declares harness {harness}");
            refusal
        } else {
            let reason = if consumer_policy.runners.is_some() && consumer_policy.runner.is_none() {
                NoRouteReason::HarnessUnavailable
            } else {
                NoRouteReason::NoAvailableModel
            };
            Refusal::no_route(reason, None)
        });
    }
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|policy| {
            registry.get(policy).is_some_and(|runner| {
                super::runners::placements::usable(policy, runner.wire_declaration())
            })
        })
        .collect();
    if runners.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::HarnessUnavailable, None));
    }
    // Declarations narrow the configured placements before any caller filters.
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            placement
                .as_ref()
                .is_none_or(|module| module == &runner.module)
        })
        .collect();
    if runners.is_empty() && placement.is_some() {
        return Err(Refusal::no_route(NoRouteReason::HarnessUnavailable, None));
    }
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            request
                .runners
                .as_ref()
                .is_none_or(|listed| listed.contains(&runner.module))
        })
        .filter(|runner| {
            request.harness.as_deref().is_none_or(|harness| {
                runner
                    .harness
                    .as_deref()
                    .is_none_or(|entry| entry == harness)
                    && registry.get(runner).is_some_and(|runner| {
                        runner.harnesses.iter().any(|hosted| hosted == harness)
                    })
            })
        })
        .collect();
    if runners.is_empty() {
        let mut refusal = Refusal::no_route(NoRouteReason::HarnessUnavailable, None);
        if let Some(harness) = &request.harness {
            refusal.message = format!("no session-starting runner declares harness {harness}");
        }
        return Err(refusal);
    }
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            registry
                .get(runner)
                .is_some_and(|runner| runner.hosts_task())
        })
        .collect();
    if runners.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::HarnessUnavailable, None));
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
    #[cfg(test)]
    count_input_build(&shared.operator_config, 1);
    Ok(SelectionInputs {
        request,
        consumer_pinned,
        now,
        facts,
        snapshot,
        priced,
        registry,
        runners,
        consumer_policy,
        priced_per_token,
    })
}

const RUNNER_PROVIDER_LIMIT: &str = "runner_provider_limit";

enum SelectionAttempt {
    Selected(Value),
    ProbeLost(i64),
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
    shared: &RequestInputs,
    inputs: &mut SelectionInputs,
) -> Result<Value, Refusal> {
    // A lost binding means another decision already owns this credential's
    // recovery probe. The retry records that owner's probe as held until its
    // deadline, and a held probe makes the credential ineligible for the rest
    // of this request, so each retry has one fewer candidate and the loop ends
    // on its own. The cap is a backstop: if a hold ever failed to exclude its
    // credential, the request answers the timed wait instead of spinning.
    const MAX_LOST_PROBE_RETRIES: usize = 16;
    let mut lost_until: Option<i64> = None;
    let mut lost = 0;
    loop {
        match select_at_tier_attempt(
            params,
            caller,
            deps,
            profile_lookup,
            service_tier,
            sticky_credential,
            tier_trace,
            tier_settings,
            panel_seat,
            evaluation,
            shared,
            inputs,
        )
        .await
        {
            Ok(SelectionAttempt::Selected(answer)) => return Ok(answer),
            Ok(SelectionAttempt::ProbeLost(until)) => {
                lost_until = Some(lost_until.map_or(until, |prior| prior.min(until)));
                lost += 1;
                if lost >= MAX_LOST_PROBE_RETRIES {
                    return Err(account_limit_refusal(
                        inputs.request.pin.as_deref(),
                        lost_until,
                    ));
                }
            }
            Err(refusal) => {
                if let Some(until) = lost_until.filter(|_| {
                    matches!(
                        refusal.code,
                        RefusalCode::NoRoute | RefusalCode::PinUnavailable
                    )
                }) {
                    return Err(account_limit_refusal(
                        inputs.request.pin.as_deref(),
                        Some(until),
                    ));
                }
                return Err(refusal);
            }
        }
    }
}

async fn select_at_tier_attempt(
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
    shared: &RequestInputs,
    inputs: &mut SelectionInputs,
) -> Result<SelectionAttempt, Refusal> {
    #[cfg(test)]
    count_input_build(&shared.operator_config, 2);
    tier_trace.rejections.clear();
    let live_config = &shared.live_config;
    let operator_config = &shared.operator_config;
    let catalog_targets = &shared.catalog_targets;
    let sources = SelectionPolicySources {
        live_config,
        operator_config,
        operator_config_path: deps.operator_config_path,
        catalog_targets,
    };
    let request = &inputs.request;
    let consumer = request.consumer.clone();
    let consumer_policy = &inputs.consumer_policy;
    let operator_policy = operator_config.operator_policy_ref();
    let legacy_consumer = select_policy::Consumer::from_id(&consumer);
    let priced_per_token = inputs.priced_per_token;
    let now = inputs.now;
    let mut facts = inputs.facts.clone();
    let mut snapshot = inputs.snapshot.clone();
    let registry = &inputs.registry;
    let runners = &inputs.runners;
    // Start from pre-tier facts and account evidence, never the affinity
    // attempt's tier-filtered account lists or sticky credential.
    let mut priced = crate::select::PricedSelection {
        routing_config: inputs.priced.routing_config.clone(),
        provenance: inputs.priced.provenance.clone(),
        entitlement_fences: inputs.priced.entitlement_fences.clone(),
        subscription_plan_providers: inputs.priced.subscription_plan_providers.clone(),
        // A single-attempt selection can use the original context, including
        // its already-built indexes. Only tier retries need a pristine clone.
        classes: if service_tier.is_none() {
            inputs.priced.classes.take()
        } else {
            inputs.priced.classes.clone()
        },
    };
    let classifier = consumer == "classifier";
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
        registry.runs(runner, route_key, capacity_key)
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
                                registry.get(runner).is_some_and(|runner| {
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
    let eligible_catalog_empty = request.eligible_routes.is_some()
        && !facts
            .iter()
            .any(|fact| eligible_routes::includes_model(request.eligible_routes.as_deref(), fact));
    let reachable: Vec<&ModelFact> = facts
        .iter()
        .filter(|fact| eligible_routes::includes_model(request.eligible_routes.as_deref(), fact))
        .filter(|fact| {
            request.available_routes.as_ref().is_none_or(|routes| {
                routes.contains(&format!("{}/{}", fact.provider_id, fact.model_id))
            })
        })
        .filter(|fact| {
            let route = fact_to_route(fact);
            runners.iter().any(|runner| {
                registry.can_run(runner, &route.route_key, &route.capacity_key)
                    || registry.excluded_by_harness(runner, &route.route_key, &route.capacity_key)
            })
        })
        .collect();
    let pair_rejections: HashMap<String, String> = facts
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            // Provider limits can empty the runnable catalog before a hard
            // pin reaches admission. Preserve that cause so admission can
            // distinguish a limited row from an absent model, without changing
            // empty-catalog answers caused by other capability gaps.
            if reachable.is_empty()
                && (request.pin.is_some() || request.pin_slot.is_some())
                && request.pin_fallback != Some(PinFallback::Route)
                && request
                    .available_routes
                    .as_ref()
                    .is_none_or(|routes| routes.contains(&route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.provider_limit(runner, &route.route_key, &route.capacity_key)
                })
            {
                return Some((route.route_key, RUNNER_PROVIDER_LIMIT.into()));
            }
            if !runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.requires_wire_api(runner, &route.route_key, &route.capacity_key)
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
                    registry.no_runner_account(runner, &route.route_key, &route.capacity_key)
                })
            {
                return Some((route.route_key, "no_runner_account".into()));
            }
            let unverified = !runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
                && runners.iter().any(|runner| {
                    registry.wire_family_unverified(runner, &route.route_key, &route.capacity_key)
                });
            unverified.then(|| {
                (
                    route.route_key,
                    striatum_core::model_router::REASON_RUNNER_WIRE_FAMILY_UNVERIFIED.into(),
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
    if capable.is_empty() && pair_rejections.is_empty() && !eligible_catalog_empty {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
    }

    // Keep exclusion-only routes long enough to check task requirements and
    // policy, but never make them available to a pin or automatic selection.
    let harness_excluded: HashSet<String> = capable
        .iter()
        .filter_map(|fact| {
            let route = fact_to_route(fact);
            (!runners
                .iter()
                .any(|runner| registry.can_run(runner, &route.route_key, &route.capacity_key)))
            .then_some(route.route_key)
        })
        .collect();
    let statically_available: HashSet<String> = capable
        .iter()
        .map(|fact| fact_to_route(fact).route_key)
        .filter(|key| !harness_excluded.contains(key))
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
    if available.is_empty()
        && pair_rejections.is_empty()
        && !eligible_catalog_empty
        && harness_excluded.is_empty()
    {
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
                if let Some(block) =
                    classes.blocked_account(&fact.provider_id, &fact.model_id, priced_per_token)
                {
                    account_blocks.insert(striatum_core::fact_to_route(fact).route_key, block);
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
        if let Some(block) = account_blocks.get(&route.route_key) {
            let state = account_snapshot
                .by_capacity
                .entry(route.capacity_key)
                .or_default();
            if block.lifetime == striatum_core::blocks::LifetimeClass::Quota {
                state.quota_exhausted_until = block.reset_at_ms.map(|reset| reset as f64);
                state.quota_pressure = Some(1.0);
            } else {
                state.failure_backoff_until = block.reset_at_ms.map(|reset| reset as f64);
            }
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
    let ledger_pin = request
        .pin
        .clone()
        .or_else(|| {
            request
                .pin_slot
                .as_deref()
                .and_then(|slot| operator_config.select_slot_pin(slot))
        })
        .filter(|_| request.pin_fallback != Some(PinFallback::Route))
        .map(|pin| crate::select_concurrency::pin_capacity(&pin, &facts));
    let concurrency = crate::select_concurrency::capture_with_snapshot(
        deps.store,
        priced.classes.as_ref(),
        &facts,
        priced_per_token,
        now as i64,
        evaluation.map(|view| view.accounts.initial.as_ref()),
        |fact| {
            crate::select_concurrency::considered(fact, &available, ledger_pin.as_deref())
                && request.eligible_routes.as_ref().is_none_or(|routes| {
                    routes.iter().any(|route| {
                        route.provider == fact.provider_id && route.model == fact.model_id
                    })
                })
        },
    )
    .map_err(store_refusal)?;
    let use_concurrency = std::cell::Cell::new(!concurrency.full_routes.is_empty());
    let selection = Selection {
        policy_memo: &shared.policy_memo,
        concurrency: &concurrency,
        use_concurrency: &use_concurrency,
        eligible_routes: request.eligible_routes.as_deref(),
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

    // route.select_panel pins each member to an already ranked route; it never
    // reads the quality bar, adequate routes, or exclusions from this scorer.
    // Ordinary route.what_if model previews still return all three diagnostics.
    if let Some(view) = evaluation.filter(|_| !panel_seat) {
        let mut evidence = view.evidence.lock().unwrap();
        let _ = selection.score_candidates_evaluated(
            &usable,
            consumer_policy.operator_ladder(),
            None,
            None,
            Some(&mut evidence),
        );
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
        .filter(|previous| {
            request.eligible_routes.as_ref().is_none_or(|routes| {
                routes.contains(&RouteRef {
                    provider: previous.provider.clone(),
                    model: previous.model.clone(),
                    variant: previous.variant.clone(),
                })
            })
        })
        .map(|previous| format!("{}/{}", previous.provider, previous.model));
    // Request (or consumer settings) pin, then project pin, then the continuing
    // model, then the mode.
    // A pin does not fall through unless the request asks for it with
    // `pin_fallback: "route"`: then a pin that cannot be served is dropped
    // before the order runs, and the order runs as for an unpinned request.
    // The continuing model falls through once it is no longer allowed. Legacy
    // targetAgent bodies never reach this surface.
    let choose = || {
        let request_pin_exempt = inputs.consumer_pinned
            || (request.pin.is_none() && request.pin_slot.is_some())
            || request.pin_origin == Some(PinOrigin::User)
            || request_pin_is_keep_free_exempt(request.pin.as_deref(), &consumer_policy);
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
                .map(|pin| selection.pin(pin, &available, request_pin_exempt))
            {
                // Explicit fallback authorizes another route, including when
                // the pin's eligible accounts have no concurrency capacity.
                pin_reason = Some(refused_pin_reason(&refusal));
                request_pin = None;
            }
        }
        let mut ladder_variant = None;
        let ladder_choice = if request_pin.is_none()
            && !classifier
            && project_pin.is_none()
            && continuing.is_none()
            && consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
        {
            let mut result = None;
            let mut fallback_hold = None;
            let mut ledger_rejected = Vec::new();
            for entry in consumer_policy.ladder.iter().flatten() {
                let Some(route) = RouteRef::parse_pin(entry) else {
                    continue;
                };
                let route_key = if consumer_policy.operator_ladder() {
                    format!("{}/{}", route.provider, route.model)
                } else {
                    route.route_key()
                };
                let subset = available
                    .iter()
                    .filter(|key| *key == &route_key || key.starts_with(&format!("{route_key}/")))
                    .cloned()
                    .collect();
                let variant = consumer_policy
                    .operator_ladder()
                    .then_some(route.variant)
                    .flatten();
                let scored = selection.score_ladder(
                    &subset,
                    &consumer_policy,
                    variant.as_deref(),
                    Some(&route_key),
                );
                if let Err(error) = &scored {
                    ledger_rejected.extend(
                        error
                            .rejected
                            .iter()
                            .filter(|r| r.reason == "concurrency_full")
                            .cloned(),
                    );
                }
                if let Ok(mut selected) = scored {
                    selected.rejected.extend(ledger_rejected.clone());
                    selection.check_ladder_variant(&selected, variant.as_deref())?;
                    match selection.account_check(&selected) {
                        Ok(()) => {
                            ladder_variant = variant;
                            result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                            break;
                        }
                        Err(refusal) if refusal.code.as_str() == "account_hold" => {
                            if selection.hold_ok {
                                ladder_variant = variant;
                                result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                                break;
                            }
                            if fallback_hold.as_ref().is_none_or(
                                |(_, previous, _): &(
                                    SelectedModelRoute,
                                    Refusal,
                                    Option<String>,
                                )| {
                                    previous.detail.get("reset_at_ms").and_then(Value::as_i64)
                                        > refusal.detail.get("reset_at_ms").and_then(Value::as_i64)
                                },
                            ) {
                                fallback_hold = Some((selected, refusal, variant));
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
            result.or_else(|| {
                fallback_hold.map(|(selected, _, variant)| {
                    ladder_variant = variant;
                    Ok((selected, select_policy::OrderBasis::Mode))
                })
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
                |pin| selection.pin(pin, &available, request_pin.is_none() || request_pin_exempt),
                |only| selection.score(only),
                || {
                    if classifier {
                        return selection.score(&usable);
                    }
                    select_policy::select_in_order(
                        &usable,
                        &consumer_policy,
                        |only, variant, key| {
                            let result =
                                selection.score_ladder(only, &consumer_policy, variant, key);
                            if result.is_ok() {
                                ladder_variant = variant.map(str::to_owned);
                            }
                            result
                        },
                    )
                },
            )
        });
        // The contract reserves pin_outcome for a pin carried by the request,
        // not a settings pin promoted internally for admission.
        let pin_outcome = (!inputs.consumer_pinned)
            .then(|| request.pin.as_ref().or(request.pin_slot.as_ref()))
            .flatten()
            .map(|_| {
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
        Ok((
            ordered,
            request_pin,
            pin_reason,
            pin_outcome,
            ladder_variant,
            preferred,
        ))
    };
    let mut choice = choose()?;
    let concurrency_failure = match &choice.0 {
        Err(select_policy::OrderFailure::Score(error)) => {
            error.reason == RouteFailureReason::ConcurrencyFull
        }
        Err(select_policy::OrderFailure::Pin(error)) => {
            error.detail.get("reason").and_then(Value::as_str) == Some("concurrency_full")
        }
        _ => false,
    };
    if (concurrency_failure && crate::select_concurrency::head(&operator_config, &consumer))
        || (!live_config.concurrency_caps().enforcement_enabled && use_concurrency.get())
    {
        if concurrency_failure
            && evaluation.is_none()
            && !crate::select_concurrency::head(&operator_config, &consumer)
        {
            if let Err(select_policy::OrderFailure::Score(error)) = &choice.0 {
                crate::select_concurrency::record("routing/v1", &consumer, &operator_config, error);
            } else {
                let routes = match &choice.0 {
                    Err(select_policy::OrderFailure::Pin(refusal)) => {
                        concurrency_rejected_routes(refusal)
                    }
                    _ => Vec::new(),
                };
                crate::select_concurrency::record(
                    "routing/v1",
                    &consumer,
                    &operator_config,
                    &concurrency.refusal(&routes),
                );
            }
        }
        use_concurrency.set(false);
        choice = choose()?;
    }
    let (ordered, request_pin, pin_reason, pin_outcome, ladder_variant, preferred) = choice;
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
                select_policy::OrderBasis::Mode => ladder_variant,
                select_policy::OrderBasis::Continuing => None,
            };
            let basis = match order {
                select_policy::OrderBasis::RequestPin if inputs.consumer_pinned => {
                    Basis::ConsumerPin
                }
                select_policy::OrderBasis::RequestPin => Basis::RequestPin,
                select_policy::OrderBasis::ProjectPin => Basis::ProjectPin,
                select_policy::OrderBasis::Continuing => Basis::Affinity,
                select_policy::OrderBasis::Mode if preferred => Basis::Preferred,
                select_policy::OrderBasis::Mode => Basis::Automatic,
            };
            (selected, basis, explicit)
        }
        Err(select_policy::OrderFailure::Pin(refusal)) => {
            if refusal.detail.get("reason").and_then(Value::as_str) == Some("concurrency_full")
                && evaluation.is_none()
            {
                let routes = concurrency_rejected_routes(&refusal);
                let error = concurrency.refusal(&routes);
                if (request.pin.is_none() && request.pin_slot.is_none())
                    || request.pin_fallback == Some(PinFallback::Route)
                {
                    selection.record_refusal(&error, &priced.provenance);
                }
                crate::select_concurrency::record(
                    "routing/v1",
                    &consumer,
                    &operator_config,
                    &error,
                );
            }
            return Err(refusal);
        }
        Err(select_policy::OrderFailure::Score(no_route)) => {
            if selection.harness_exclusions_emptied_routes(&harness_excluded, &statically_available)
            {
                return Err(Refusal::no_route(NoRouteReason::HarnessUnavailable, None));
            }
            if evaluation.is_none() {
                selection.record_refusal(&no_route, &priced.provenance);
                crate::select_concurrency::record(
                    "routing/v1",
                    &consumer,
                    &operator_config,
                    &no_route,
                );
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
                    let variant = consumer_policy
                        .operator_ladder()
                        .then(|| {
                            consumer_policy.ladder.iter().flatten().find_map(|entry| {
                                let route = RouteRef::parse_pin(entry)?;
                                (format!("{}/{}", route.provider, route.model) == *key)
                                    .then_some(route.variant)
                                    .flatten()
                            })
                        })
                        .flatten();
                    let named_non_quota = no_route.earliest_reset_route.as_ref() == Some(key)
                        && !matches!(
                            no_route.reason,
                            RouteFailureReason::NoAdequateRoute
                                | RouteFailureReason::AllRoutesInadequate
                        )
                        && account_blocks.get(key).is_some_and(|block| {
                            block.lifetime == striatum_core::blocks::LifetimeClass::NonQuota
                        });
                    if !named_non_quota
                        && (!available.contains(key)
                            || selection
                                .score_ladder(
                                    &HashSet::from([key.clone()]),
                                    &consumer_policy,
                                    variant.as_deref(),
                                    Some(key),
                                )
                                .is_err())
                    {
                        return None;
                    }
                    Some(key.clone())
                })
                .collect();
            if !blocked.is_empty() {
                let reset = no_route.earliest_reset.map(|reset| reset as u64);
                let quota = no_route
                    .earliest_reset_route
                    .as_ref()
                    .and_then(|key| account_blocks.get(key))
                    .is_some_and(|block| {
                        block.lifetime == striatum_core::blocks::LifetimeClass::Quota
                    });
                let mut refusal = Refusal::no_route(
                    NoRouteReason::parse("accounts_blocked"),
                    if quota {
                        super::quota_retry_hint(reset, now as u64)
                    } else {
                        reset
                    },
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
    if consumer_policy.operator_ladder() {
        selection.check_ladder_variant(&selected, explicit_variant.as_deref())?;
    }
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
        let mut preview = json!({
            "route": selected.route_key,
            "variant": selected.prompt_params.variant,
            "cost_usd": selected.score.objective.task_pricing.as_ref().filter(|price| price.price_unit == "task").and_then(|price| price.final_cost_usd),
        });
        if selected
            .explanation
            .iter()
            .any(|line| line == PINNED_KEEP_FREE_EXPLANATION)
        {
            preview["explanation"] = json!(PINNED_KEEP_FREE_EXPLANATION);
        }
        if panel_seat {
            let route = RouteRef {
                provider: selected.model.provider_id.clone(),
                model: selected.model.model_id.clone(),
                variant: selected.prompt_params.variant.clone(),
            };
            let fact = facts
                .iter()
                .filter(|fact| fact.provider_id == route.provider && fact.model_id == route.model)
                .min_by_key(|fact| u8::from(fact.variant != route.variant));
            let mut runner_ref = RunnerRef::new(runner.module.clone(), route.clone());
            // An estimate can describe the pool but cannot acquire a key or
            // promise a credential. The actual selection owns that reservation.
            if key_pool::is_pool_provider(&catalog_targets, &route.provider)
                && !fact.is_some_and(|fact| {
                    fact.billing_plane
                        .as_ref()
                        .is_some_and(|plane| plane.auth_method == "gateway")
                })
            {
                runner_ref.model.provider = catalog_targets[&route.provider].clone();
                runner_ref = runner_ref.with_auth(RunnerAuth {
                    pool: Some(route.provider.clone()),
                    credential_id: None,
                });
            }
            if let Some(harness) = registry.get(runner).and_then(|runner| {
                runner.harness_for_route(
                    request.harness.as_deref(),
                    &selected.route_key,
                    &selected.capacity_key,
                )
            }) {
                runner_ref = runner_ref.with_harness(harness);
            }
            let explanation = clamp_sentence(format!(
                "{} on {} for consumer {consumer}, chosen by {}.",
                route.pin(),
                runner.module,
                basis.as_str()
            ));
            let mut selection = cortexkit_role_routing::panel::PanelEstimateSelection::new(
                route,
                runner_ref,
                fact.and_then(|fact| fact.context_window)
                    .map_or(0, |window| window as u64),
                fact.and_then(|fact| fact.max_output_tokens).unwrap_or(0),
                now_ms() + DECISION_TTL_MS,
                explanation,
            );
            selection.service_tier_chosen = service_tier.map(|tier| tier.as_str().to_owned());
            selection.price = price_answer::for_selection(
                &selected,
                &inputs.facts,
                priced.classes.as_ref(),
                live_config,
                priced_per_token,
                request,
                demand.iq,
                service_tier,
                profile_lookup,
            );
            selection.detail = (request.detail == Some(true)).then(|| json!({
                "basis": basis.as_str(), "explanation": selected.explanation,
                "rejected": selected.rejected, "score": selected.score, "variant": variant_choice,
            }));
            selection.project_settings = tier_trace.project_settings.clone();
            preview["selection"] = serde_json::to_value(selection).map_err(store_refusal)?;
        }
        return Ok(SelectionAttempt::Selected(preview));
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
        let binding = classes
            .bind_selected_probe(
                deps.store,
                &selected.model.provider_id,
                &selected.model.model_id,
                priced_per_token,
                &decision_id,
            )
            .map_err(store_refusal)?;
        if let account_selection::ProbeBinding::Lost { retry_at } = binding {
            // Retry the same tier with the lost credential held. Each conflict
            // removes a credential from this request's remaining candidates.
            if let Some(original) = &inputs.priced.classes {
                original.remember_probe_holds(classes);
            } else {
                inputs.priced.classes = priced.classes.take();
            }
            return Ok(SelectionAttempt::ProbeLost(retry_at));
        }
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
        .get(runner)
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
        let runner_admits_keys = registry.get(runner).is_some_and(|runner| {
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
                    Basis::RequestPin | Basis::ConsumerPin => {
                        request.pin_slot.as_deref().or(request_pin.as_deref())
                    }
                    Basis::ProjectPin => project_pin.as_deref(),
                    _ => None,
                };
                pool_select_refusal(refusal, pin, now as u64)
            })?;
            runner_ref.model.provider = grant.runner.provider;
            runner_ref.model.model = grant.runner.model;
            runner_ref.model.variant = grant.runner.variant;
            auth.credential_id = Some(grant.credential_id);
        }
        runner_ref = runner_ref.with_auth(auth);
    }
    if let Some(harness) = registry.get(runner).and_then(|runner| {
        runner.harness_for_route(
            request.harness.as_deref(),
            &selected.route_key,
            &selected.capacity_key,
        )
    }) {
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
    if let Some(price) = price_answer::for_selection(
        &selected,
        &inputs.facts,
        priced.classes.as_ref(),
        live_config,
        priced_per_token,
        request,
        demand.iq,
        service_tier,
        profile_lookup,
    ) {
        answer = answer.with_price(price);
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
    crate::account_writer::v1_record_selection(deps.store, &binding, Some(&value.to_string()))
        .map_err(store_refusal)?;
    Ok(SelectionAttempt::Selected(value))
}

fn concurrency_rejected_routes(refusal: &Refusal) -> Vec<String> {
    refusal
        .detail
        .get("rejected")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|route| route["reason"] == "concurrency_full")
        .filter_map(|route| route["routeKey"].as_str().map(str::to_owned))
        .collect()
}

/// Everything one select's automatic and pinned attempts share.
struct Selection<'a> {
    policy_memo: &'a config_overrides::SelectionPolicyMemo,
    concurrency: &'a striatum_core::concurrency_availability::ConcurrencyAvailability,
    use_concurrency: &'a std::cell::Cell<bool>,
    eligible_routes: Option<&'a [RouteRef]>,
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
    account_blocks: &'a HashMap<String, account_selection::AccountBlock>,
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
    fn harness_exclusions_emptied_routes(
        &self,
        excluded: &HashSet<String>,
        hostable: &HashSet<String>,
    ) -> bool {
        if excluded.is_empty() {
            return false;
        }
        let allowed = excluded
            .union(hostable)
            .filter(|key| {
                self.sources
                    .operator_config
                    .route_allowed(self.consumer, key, false)
            })
            .cloned()
            .collect();
        let input = self.pass().input(&allowed, Default::default());
        let rejections = self.eligible_rejections(&input.facts, None);
        // Reuse static admission so exclusions cannot hide a policy or capability
        // refusal. Ignore quota walls, blocks and cooldowns here: a hostable route
        // behind a cooldown still belongs to ranking and keeps the scorer's timed
        // refusal.
        let admission = striatum_core::model_router::admit_routes_with_availability(
            &input.facts,
            None,
            striatum_core::provider_model_spec::ExclusionPolicy {
                operator: &input.operator_exclude_keys,
                caller: &input.exclude_route_keys,
            },
            self.consumer_exclude,
            input.required_capabilities,
            &input.available_models,
            &input.requirements,
            &self.pass().routing_config(),
            &rejections,
        );
        !admission
            .admitted
            .iter()
            .any(|entry| hostable.contains(&entry.route.route_key))
            && admission
                .admitted
                .iter()
                .any(|entry| excluded.contains(&entry.route.route_key))
    }

    fn score(&self, available: &HashSet<String>) -> Result<SelectedModelRoute, NoRouteError> {
        if let Some(order) = &self.classifier_order {
            let mut full_error: Option<NoRouteError> = None;
            // Classifier order uses input price, not chat workload profiles or
            // output price. The ordinary scorer still admits each candidate,
            // preserving exclusions, quotas and account fences.
            for route in order {
                let only = available
                    .iter()
                    .filter(|key| *key == route || key.starts_with(&format!("{route}/")))
                    .cloned()
                    .collect();
                match self.score_candidates(&only) {
                    Ok(mut selected) if self.account_check(&selected).is_ok() => {
                        if let Some(error) = full_error {
                            selected.rejected.extend(error.rejected);
                        }
                        return Ok(selected);
                    }
                    Err(mut error) if error.reason == RouteFailureReason::ConcurrencyFull => {
                        if let Some(previous) = full_error.take() {
                            error.earliest_reset =
                                match (error.earliest_reset, previous.earliest_reset) {
                                    (Some(a), Some(b)) => Some(a.min(b)),
                                    (a, b) => a.or(b),
                                };
                            error.rejected.extend(previous.rejected);
                        }
                        full_error = Some(error);
                    }
                    _ => {}
                }
            }
            return full_error.map_or_else(|| self.score_candidates(&HashSet::new()), Err);
        }
        self.score_candidates(available)
    }

    fn score_candidates(
        &self,
        available: &HashSet<String>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        self.score_candidates_evaluated(available, false, None, None, None)
    }

    fn score_ladder(
        &self,
        available: &HashSet<String>,
        policy: &crate::config_policy::ConsumerPolicy,
        variant: Option<&str>,
        key: Option<&str>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        if policy.operator_ladder() {
            self.score_candidates_evaluated(available, true, variant, key, None)
        } else {
            self.score(available)
        }
    }

    fn check_ladder_variant(
        &self,
        selected: &SelectedModelRoute,
        variant: Option<&str>,
    ) -> Result<(), Refusal> {
        let Some(variant) = variant else {
            return Ok(());
        };
        let pin = format!(
            "{}/{}:{variant}",
            selected.model.provider_id, selected.model.model_id
        );
        work_model_pin::validate_variant(
            &pin,
            &selected.model.provider_id,
            &selected.model.model_id,
            Some(variant),
            config_overrides::reasoning_catalog_for(self.sources.operator_config_path).as_ref(),
            self.sources.catalog_targets,
        )
        .map(|_| ())
        .map_err(|_| Refusal::pin_unavailable(&pin, PinUnavailableReason::InvalidVariant, None))
    }

    fn scoring_snapshot(&self, pinned: bool) -> ModelRouteStateSnapshot {
        let mut snapshot = clone_scoring_snapshot(self.snapshot);
        for key in self.runtime_unavailable {
            // Account availability cannot lower the maximum model scores used to
            // interpret IQ. Operator exclusions still can, including not_for rules.
            if self
                .sources
                .operator_config
                .route_allowed(self.consumer, key, pinned)
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
        pinned: bool,
        explicit_variant: Option<&str>,
        recovery_key: Option<&str>,
        evaluation: Option<&mut striatum_core::model_router::RouteEvaluation>,
    ) -> Result<SelectedModelRoute, NoRouteError> {
        #[cfg(test)]
        if evaluation.is_some() {
            *EVIDENCE_PASSES
                .lock()
                .unwrap()
                .entry(self.sources.operator_config_path.to_path_buf())
                .or_default() += 1;
        }
        let allowed = available
            .iter()
            .filter(|route| {
                self.sources
                    .operator_config
                    .route_allowed(self.consumer, route, pinned)
            })
            .cloned()
            .collect();
        let mut snapshot = self.scoring_snapshot(pinned);
        let keep_free_exempt =
            pinned && recovery_key.is_some_and(|key| exempt_pinned_keep_free(&mut snapshot, key));
        let mut input = self.pass().input_with_policy(&allowed, snapshot, pinned);
        select_policy::apply_explicit_variant(&mut input, explicit_variant);
        let result = self
            .score_input_evaluated(input, self.consumer_exclude, explicit_variant, evaluation)
            .map_err(select_policy::describe_filter_refusal)
            .map_err(|mut error| {
                if matches!(
                    error.reason,
                    RouteFailureReason::InvalidRequirements | RouteFailureReason::ConcurrencyFull
                ) {
                    return error;
                }
                let account_recoveries: HashMap<_, _> = self
                    .facts
                    .iter()
                    .filter_map(|fact| {
                        let key = fact_to_route(fact).route_key;
                        if !self.recovery_available.contains(&key)
                            || recovery_key.is_some_and(|only| {
                                key != only && !key.starts_with(&format!("{only}/"))
                            })
                            || !self.sources.operator_config.route_allowed(
                                self.consumer,
                                &key,
                                pinned,
                            )
                        {
                            return None;
                        }
                        let block = self
                            .classes
                            .and_then(|classes| {
                                classes.account_recovery_with_keep_free(
                                    &fact.provider_id,
                                    &fact.model_id,
                                    self.priced_per_token,
                                    0.0,
                                )
                            })
                            .or_else(|| self.account_blocks.get(&key).cloned())?;
                        block.reset_at_ms?;
                        Some((key, block))
                    })
                    .collect();
                let account_resets: HashMap<_, _> = account_recoveries
                    .iter()
                    .filter_map(|(key, block)| {
                        block.reset_at_ms.map(|reset| (key.clone(), reset as f64))
                    })
                    .collect();
                for rejected in &mut error.rejected {
                    if let Some(block) = account_recoveries.get(&rejected.route_key) {
                        if block.lifetime == striatum_core::blocks::LifetimeClass::NonQuota
                            && matches!(
                                rejected.reason.as_str(),
                                "no_eligible_account"
                                    | "not_available"
                                    | "credential_billing_exhausted"
                                    | "credential_unentitled"
                            )
                        {
                            rejected.reason = match account_block_pin_reason(block) {
                                PinUnavailableReason::Backoff => "backoff_active".into(),
                                reason => reason.as_str().into(),
                            };
                        }
                    }
                }
                let mut recovery_available: HashSet<_> = self
                    .recovery_available
                    .iter()
                    .filter(|key| {
                        if recovery_key.is_some_and(|only| {
                            *key != only && !key.starts_with(&format!("{only}/"))
                        }) {
                            return false;
                        }
                        self.sources
                            .operator_config
                            .route_allowed(self.consumer, key, pinned)
                            && (!self.account_blocks.contains_key(*key)
                                || account_resets.contains_key(*key))
                    })
                    .cloned()
                    .collect();
                recovery_available.extend(account_resets.keys().cloned());
                let mut recovery_snapshot = self.scoring_snapshot(pinned);
                for fact in self.facts {
                    let route = fact_to_route(fact);
                    if account_recoveries
                        .get(&route.route_key)
                        .is_some_and(|block| {
                            block
                                .credential_code
                                .as_deref()
                                .is_some_and(crate::credential_fences::is_billing_code)
                        })
                    {
                        for state in [
                            recovery_snapshot.by_route.get_mut(&route.route_key),
                            recovery_snapshot.by_capacity.get_mut(&route.capacity_key),
                        ]
                        .into_iter()
                        .flatten()
                        {
                            // Only the recovery diagnostic may lift the credential
                            // wall whose typed account block supplies this deadline.
                            state.credential_refusal = None;
                        }
                    }
                }
                if pinned {
                    if let Some(key) = recovery_key {
                        exempt_pinned_keep_free(&mut recovery_snapshot, key);
                    }
                }
                let mut recovery_input =
                    self.pass()
                        .input_with_policy(&recovery_available, recovery_snapshot, pinned);
                select_policy::apply_explicit_variant(&mut recovery_input, explicit_variant);
                let mut recovery_rejections =
                    self.eligible_rejections(&recovery_input.facts, explicit_variant);
                recovery_rejections.to_mut().retain(|key, reason| {
                    !account_recoveries.get(key).is_some_and(|block| {
                        block
                            .credential_code
                            .as_deref()
                            .is_some_and(crate::credential_fences::is_billing_code)
                            && matches!(
                                reason.as_str(),
                                "credential_billing_exhausted" | "credential_unentitled"
                            )
                    })
                });
                (error.earliest_reset, error.earliest_reset_route) =
                    striatum_core::model_router::earliest_admissible_recovery(
                        &recovery_input,
                        &self.pass().routing_config(),
                        self.consumer_exclude,
                        &recovery_rejections,
                        &account_resets,
                    );
                error
            });
        let mut result = select_policy::explain_dotted_exclusions(
            result,
            self.sources.operator_config,
            self.consumer,
        );
        if keep_free_exempt {
            if let Ok(selected) = &mut result {
                selected
                    .explanation
                    .push(PINNED_KEEP_FREE_EXPLANATION.into());
            }
        }
        result
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
        let rejections = self.eligible_rejections(&input.facts, explicit_variant);
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
        striatum_core::model_router::evaluate_model_route_with_concurrency(
            input,
            &self.pass().routing_config(),
            self.slots,
            consumer_exclude,
            &rejections,
            &profiles,
            evaluation,
            self.use_concurrency.get().then_some(self.concurrency),
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
            concurrency: self.use_concurrency.get().then_some(self.concurrency),
            operator_config: self.sources.operator_config,
            policy_memo: self.policy_memo,
        }
    }

    /// A named model, through the same pin admission a work run's `model=`
    /// takes: hard admission only, never a quality ranking.
    fn pin(
        &self,
        pin: &str,
        available: &HashSet<String>,
        keep_free_exempt: bool,
    ) -> Result<SelectedModelRoute, Refusal> {
        let parsed = RouteRef::parse_pin(pin)
            .ok_or_else(|| Refusal::pin_unavailable(pin, PinUnavailableReason::Invalid, None))?;
        if self.facts.iter().any(|fact| {
            fact.decommissioned
                && fact.provider_id == parsed.provider
                && fact.model_id == parsed.model
        }) {
            return Err(Refusal::pin_unavailable(
                pin,
                PinUnavailableReason::parse("decommissioned"),
                None,
            ));
        }
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
        let mut snapshot = self.snapshot.clone();
        let keep_free_exempt = keep_free_exempt
            && exempt_pinned_keep_free(
                &mut snapshot,
                &format!("{}/{}", parsed.provider, parsed.model),
            );
        let refused = |code: &str, cause: Option<String>, message: String| {
            let reserve_limited =
                code == "subscription_reserve" || cause.as_deref() == Some("subscription_reserve");
            let provider_limited = (code == "not_available" || code == RUNNER_PROVIDER_LIMIT)
                && self
                    .rejections
                    .get(&pin_key)
                    .or_else(|| self.rejections.get(&pin_capacity))
                    .is_some_and(|cause| cause == RUNNER_PROVIDER_LIMIT);
            let (mut reason, mut reset) = if provider_limited {
                (PinUnavailableReason::NotAvailable, None)
            } else {
                pin_reason(code, &snapshot, &pin_key, &pin_capacity, self.now)
            };
            let unknown = reason == PinUnavailableReason::NotAvailable;
            if !provider_limited
                && ((unknown
                    && matches!(
                        code,
                        "not_available" | "no_eligible_account" | "no_available_model"
                    ))
                    || matches!(
                        code,
                        "credential_billing_exhausted" | "credential_unentitled"
                    )
                    || reserve_limited)
            {
                let recovery = self.pin_account_recovery(
                    &pin_key,
                    &pin_capacity,
                    variant.as_deref(),
                    available,
                    &snapshot,
                );
                if let Some(block) = recovery {
                    reason = account_block_pin_reason(&block);
                    reset = block
                        .reset_at_ms
                        .and_then(|reset| u64::try_from(reset).ok());
                } else if reserve_limited {
                    reason = PinUnavailableReason::QuotaExhausted;
                }
            }
            if reason == PinUnavailableReason::QuotaExhausted {
                reset = super::quota_retry_hint(reset, self.now as u64);
            }
            let mut refusal = Refusal::pin_unavailable(pin, reason, reset);
            if provider_limited {
                // A declared provider limit has no recovery time. Keep the
                // missing_capability cause alongside the pin's wire reason.
                refusal
                    .detail
                    .insert("cause".into(), json!("missing_capability"));
            }
            if unknown {
                // Availability's specific cause lives in the private refusal's
                // message, not its generic code. Keep it even without a retry time.
                refusal.message = message;
            }
            refusal
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
        .map_err(|refusal| refused(&refusal.code, refusal.cause, refusal.message))?;
        self.check_eligible_pin(pin, &selected, parsed.variant.as_deref())?;
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
            .map_err(|error| {
                if error.reason == RouteFailureReason::ConcurrencyFull {
                    no_route_refusal(&error, self.now)
                } else {
                    refused(
                        error.reason.as_str(),
                        error
                            .rejected
                            .first()
                            .map(|rejected| rejected.reason.clone()),
                        error.refusal_message(),
                    )
                }
            })?;
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
                    self.classes
                        .and_then(|classes| {
                            classes.blocked_account(
                                &selected.model.provider_id,
                                &selected.model.model_id,
                                self.priced_per_token,
                            )
                        })
                        .as_ref()
                        .map(account_block_pin_reason)
                        .unwrap_or(PinUnavailableReason::NotAvailable),
                    refusal.wait_until_ms(),
                );
                pin_refusal.message = "No account can serve the pinned route right now. \
                    The detail gives the reason and, when known, the time to retry."
                    .into();
                return Err(pin_refusal);
            }
        }
        if keep_free_exempt {
            selected
                .explanation
                .push(PINNED_KEEP_FREE_EXPLANATION.into());
        }
        Ok(selected)
    }

    fn pin_account_recovery(
        &self,
        pin_key: &str,
        pin_capacity: &str,
        variant: Option<&str>,
        available: &HashSet<String>,
        snapshot: &ModelRouteStateSnapshot,
    ) -> Option<account_selection::AccountBlock> {
        if ![pin_key, pin_capacity]
            .iter()
            .any(|key| available.contains(*key) || self.runtime_unavailable.contains(*key))
        {
            return None;
        }
        let mut fact = self
            .facts
            .iter()
            .find(|fact| fact_to_route(fact).route_key == pin_key)
            .or_else(|| {
                self.facts
                    .iter()
                    .find(|fact| fact_to_route(fact).capacity_key == pin_capacity)
            })?
            .clone();
        if let Some(variant) = variant {
            fact.variant = Some(variant.into());
        }
        let mut input = self.pass().input(available, snapshot.clone());
        let keep_free = [
            snapshot.by_route.get(pin_key),
            snapshot.by_capacity.get(pin_capacity),
        ]
        .into_iter()
        .flatten()
        .find_map(|state| state.subscription_pool.as_ref())
        .filter(|pool| !pool.available(pool.keep_free_percent, input.target_is_head))
        .map_or(0.0, |pool| {
            if input.target_is_head {
                0.0
            } else {
                pool.keep_free_percent
            }
        });
        let account_recovery = self.classes.and_then(|classes| {
            classes.account_recovery_with_keep_free(
                &fact.provider_id,
                &fact.model_id,
                self.priced_per_token,
                keep_free,
            )
        });
        let mut block = account_recovery
            .clone()
            .or_else(|| self.account_blocks.get(pin_key).cloned())?;
        let reset = block.reset_at_ms?;
        if account_recovery.is_some() {
            let snapshot = input.runtime_snapshot.as_mut()?;
            for state in [
                snapshot.by_route.get_mut(pin_key),
                snapshot.by_capacity.get_mut(pin_capacity),
            ]
            .into_iter()
            .flatten()
            {
                // A dated account recovery can lift a billing refusal for the
                // recovery probe. Its stored class still controls the public reason.
                if state.credential_refusal.as_ref().is_some_and(|mark| {
                    mark.reason == "credential_billing_exhausted"
                        || (block.credential_code.is_some()
                            && block.lifetime == striatum_core::blocks::LifetimeClass::NonQuota
                            && mark.reason == "credential_unentitled")
                }) {
                    state.credential_refusal = None;
                }
            }
        }
        input.facts = vec![fact];
        input.available_models.insert(pin_key.into());
        input.operator_exclude_keys.clear();
        // A pin bypasses quality ranking, but its recovery must still pass hard
        // admission. Narrowing to the pinned fact prevents an unrelated route's
        // earlier reset from becoming a promise that this pin can be served.
        input.requirements = ModelRequirements { iq: 0.0, eq: 0.0 };
        input
            .required_capabilities
            .get_or_insert_with(Default::default)
            .min_context
            .get_or_insert(0.0);
        let account_resets = HashMap::from([(pin_key.into(), reset as f64)]);
        let (reset, _) = striatum_core::model_router::earliest_admissible_recovery(
            &input,
            &self.pass().routing_config(),
            &[],
            self.rejections,
            &account_resets,
        );
        block.reset_at_ms = reset.map(|reset| reset as i64);
        block.reset_at_ms.map(|_| block)
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
                let block = self.classes.and_then(|classes| {
                    classes.blocked_account(
                        &selected.model.provider_id,
                        &selected.model.model_id,
                        self.priced_per_token,
                    )
                });
                if block.as_ref().is_some_and(|block| {
                    block.lifetime == striatum_core::blocks::LifetimeClass::NonQuota
                }) {
                    account_limit_refusal(None, refusal.reset_at_ms)
                } else {
                    quota_account_limit_refusal(None, refusal.reset_at_ms, self.now as u64)
                }
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

const PINNED_KEEP_FREE_EXPLANATION: &str = "pinned: keep-free margin not applied";

fn request_pin_is_keep_free_exempt(
    pin: Option<&str>,
    policy: &crate::config_policy::ConsumerPolicy,
) -> bool {
    let Some(requested) = pin.and_then(RouteRef::parse_pin) else {
        return false;
    };
    // Repeating the consumer's inherited settings choice is still an operator
    // pin. Match the whole route so a different model or effort keeps its reserve.
    let matches = |configured: &str| {
        RouteRef::parse_pin(configured).is_some_and(|configured| {
            requested.provider == configured.provider
                && requested.model == configured.model
                && requested.variant == configured.variant
        })
    };
    // Module-declared ladders do not express an operator's model choice.
    policy.pin.as_ref().is_some_and(|pin| {
        matches(&pin.variant.as_ref().map_or_else(
            || pin.model.clone(),
            |variant| format!("{}:{variant}", pin.model),
        ))
    }) || (policy.operator_ladder() && policy.ladder.iter().flatten().any(|step| matches(step)))
}

fn exempt_pinned_keep_free(snapshot: &mut ModelRouteStateSnapshot, capacity: &str) -> bool {
    let Some(pool) = snapshot
        .by_capacity
        .get_mut(capacity)
        .and_then(|state| state.subscription_pool.as_mut())
    else {
        return false;
    };
    if pool.keep_free_percent <= 0.0 {
        return false;
    }
    // Copy only the chosen model's pool. Automatic fallbacks and other models
    // retain their reserve. Quota usage, account blocks and the allocator's
    // minimum spare quota are unchanged.
    std::sync::Arc::make_mut(pool).keep_free_percent = 0.0;
    true
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

fn quota_account_limit_refusal(pin: Option<&str>, reset: Option<i64>, now: u64) -> Refusal {
    let real_reset = reset.and_then(|reset| u64::try_from(reset).ok());
    let retry = super::quota_retry_hint(real_reset, now);
    let mut refusal = account_limit_refusal(pin, retry.map(|retry| retry as i64));
    if retry != real_reset {
        refusal.message = "Every account that can serve this route has reached its limit. \
            The detail gives the next time to check for account headroom, when routing knows it."
            .into();
    }
    refusal
}

fn account_block_pin_reason(block: &account_selection::AccountBlock) -> PinUnavailableReason {
    if block.lifetime == striatum_core::blocks::LifetimeClass::Quota {
        return PinUnavailableReason::QuotaExhausted;
    }
    // A known recovery time does not make a billing refusal quota-caused.
    // Preserve the credential reason instead of overwriting it with quota_exhausted.
    match block.credential_code.as_deref() {
        Some(code) if crate::credential_fences::is_billing_code(code) => {
            PinUnavailableReason::CredentialBillingExhausted
        }
        Some(_) => PinUnavailableReason::CredentialUnentitled,
        None => PinUnavailableReason::Backoff,
    }
}

fn pool_select_refusal(refusal: key_pool::PoolRefusal, pin: Option<&str>, now: u64) -> Refusal {
    if refusal.code == striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED {
        quota_account_limit_refusal(
            pin,
            refusal
                .detail
                .as_ref()
                .and_then(|detail| detail["earliestResetAtMs"].as_i64()),
            now,
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
        "decommissioned" => (PinUnavailableReason::parse("decommissioned"), None),
        "unknown_model_provider" => (PinUnavailableReason::UnknownProvider, None),
        "unknown_model" => (PinUnavailableReason::UnknownModel, None),
        "invalid_model_variant" => (PinUnavailableReason::InvalidVariant, None),
        "excluded" | "caller_excluded" | "excluded_by_caller" => {
            (PinUnavailableReason::Excluded, None)
        }
        "credential_unentitled" => (PinUnavailableReason::CredentialUnentitled, None),
        "credential_billing_exhausted" => (PinUnavailableReason::CredentialBillingExhausted, None),
        reason if reason.starts_with("not_chat_model:") || reason.starts_with("not_chat_kind:") => {
            (PinUnavailableReason::parse(reason), None)
        }
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
        RouteFailureReason::ConcurrencyFull => NoRouteReason::ConcurrencyFull,
        RouteFailureReason::NoAvailableModel => NoRouteReason::NoAvailableModel,
        RouteFailureReason::MissingRequiredCapability => NoRouteReason::MissingCapability,
        RouteFailureReason::ContextTooSmall => NoRouteReason::ContextTooSmall,
        RouteFailureReason::QuotaOrCooldownExhausted => NoRouteReason::QuotaOrCooldown,
        RouteFailureReason::NoAdequateRoute
        | RouteFailureReason::AllRoutesInadequate
        | RouteFailureReason::InvalidRequirements => NoRouteReason::AllInadequate,
    };
    let mut reset = no_route
        .earliest_reset
        .filter(|reset| *reset > now)
        .map(|reset| reset as u64);
    if no_route.reason != RouteFailureReason::ConcurrencyFull
        && no_route.rejected.iter().any(|route| {
            Some(&route.route_key) == no_route.earliest_reset_route.as_ref()
                && matches!(
                    route.reason.as_str(),
                    "quota_exhausted" | "subscription_reserve" | "no_eligible_account"
                )
        })
    {
        reset = super::quota_retry_hint(reset, now as u64);
    }
    let mut refusal = Refusal::no_route(reason, reset);
    let mut diagnostic = no_route.clone();
    diagnostic.earliest_reset = no_route.earliest_reset.filter(|reset| *reset > now);
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
    mod quota_retry_tests;
    use super::super::{
        conformance_tests::{install_catalog, reserved, Harness},
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

    fn take_input_builds(harness: &Harness) -> [usize; 3] {
        INPUT_BUILDS
            .lock()
            .unwrap()
            .remove(&(std::sync::Arc::as_ptr(&harness.operator_config()) as usize))
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn tier_fallback_builds_inputs_once_and_preserves_both_fixture_picks() {
        use super::super::request_cpu_bench::{
            production_fixture, representative_fixture, FixtureCounts,
        };
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        for fixture in [
            production_fixture(),
            representative_fixture(FixtureCounts {
                subscription_rules: 10,
                exclusion_rules: 103,
                runner_route_rules: 0,
                catalog_facts: 836,
                listed_credentials: 39,
            }),
        ] {
            let route = fixture
                .harness
                .bind(reserved(fixture::CONSUMER), None)
                .await;
            let request = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"pin":"cpu-provider-0/model-0"});
            let before = route.call("route.select", &request).await;
            assert_eq!(before.0, subc_protocol::FrameType::Response, "{before:?}");
            let mut fallback = request;
            fallback["service_tier"] = json!("ultrafast");
            take_input_builds(&fixture.harness);
            let after = route.call("route.select", &fallback).await;
            assert_eq!(after.0, subc_protocol::FrameType::Response, "{after:?}");
            assert!(!after.1["result"]["route"].is_null());
            assert!(!after.1["result"]["runner"].is_null());
            assert_eq!(after.1["result"]["route"], before.1["result"]["route"]);
            assert_eq!(after.1["result"]["runner"], before.1["result"]["runner"]);
            assert_eq!(after.1["result"]["service_tier_chosen"], "standard");
            assert_eq!(take_input_builds(&fixture.harness), [1, 1, 3]);
        }
    }

    #[tokio::test]
    async fn affinity_sticky_failure_retries_same_tier_with_another_credential() {
        use crate::broca_credentials::BrocaCredentials;
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let harness = Harness::new("shared-tier-affinity");
        let model = "gpt-6.1-sol";
        let mut fact = crate::tests::fact("openai", model);
        fact.auth_methods = Some(vec!["oauth".into()]);
        harness.install_facts(&[fact.clone()]);
        let served: Value = serde_json::from_str(include_str!(
            "../../fixtures/service-tier-v1/fusiform-served-payloads.json"
        ))
        .unwrap();
        let wires = [(format!("openai/{model}"), "openai.responses".into())]
            .into_iter()
            .collect();
        let mut credentials = BrocaCredentials::default();
        let mut rows: Vec<_> = (0..2)
            .map(|i| ListedCredential {
                id: format!("oauth:{i}"),
                account_id: Some(format!("account-{i}")),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["openai".into()],
                serves: vec!["openai".into()],
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                plan_tier: Some("pro_200".into()),
                ..Default::default()
            })
            .collect();
        // Start with only one account so the persisted affinity has an
        // unambiguous credential, then make only the other account fast-eligible.
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "first".into(),
                credentials: rows[..1].to_vec(),
                ..Default::default()
            },
            RECOVERY_NOW,
        );
        credentials.publish_billing_catalog(
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact])
                .with_test_catalog(
                    &serde_json::to_vec(
                        &served["catalog.get — provider default endpoint"]["response"],
                    )
                    .unwrap(),
                )
                .with_wire_apis(&wires),
        );
        credentials.wire_apis = wires;
        credentials.publish_plan_prices(&served["plan.prices — every curated row"]["response"]);
        credentials.runner_declarations.insert("tier-runner".into(), super::super::runners::parse_declaration(&json!({"runner":{
            "pairs":[{"wire_api":"openai.responses","auth_method":"oauth"}],"follows_account_choice":true
        }})).unwrap().unwrap());
        credentials.runner_service_tiers.insert("tier-runner".into(), super::super::runners::service_tiers_from_describe(&json!({
            "capabilities":["service_tier"],"service_tiers":{"openai.responses":{"levels":["fast"],"served":"reported"}}
        })).unwrap());
        harness.set_credentials(credentials.clone());
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":"tier-runner","via_vault":true}]);
            config["providers"]["openai"]["subscription"] = json!(true);
            config["consumers"] = json!({"mason":{"owner":"tier-owner","mode":"demand"}});
        });
        let route = harness.bind(reserved("tier-owner"), None).await;
        let request = json!({"consumer":"mason","requirements":{"iq":30},"service_tier":"fast","affinity":"conversation"});
        let first = route.call("route.select", &request).await;
        assert_eq!(first.0, subc_protocol::FrameType::Response, "{first:?}");
        let first_id = first.1["result"]["decision_id"].as_str().unwrap();
        assert_eq!(
            harness
                .store()
                .decision_provider_identity(first_id)
                .unwrap()
                .unwrap()
                .credential_id
                .as_deref(),
            Some("oauth:0")
        );
        rows[0].plan_tier = Some("unknown".into());
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "second".into(),
                credentials: rows,
                ..Default::default()
            },
            RECOVERY_NOW,
        );
        harness.set_credentials(credentials);
        take_input_builds(&harness);
        let second = route.call("route.select", &request).await;
        assert_eq!(second.0, subc_protocol::FrameType::Response, "{second:?}");
        assert_eq!(second.1["result"]["service_tier_chosen"], "fast");
        assert_eq!(second.1["result"]["route"], first.1["result"]["route"]);
        let id = second.1["result"]["decision_id"].as_str().unwrap();
        let (_, accounts) = harness.store().decision_service_tier(id).unwrap().unwrap();
        assert_eq!(accounts, vec!["oauth:1".to_owned()]);
        assert_eq!(take_input_builds(&harness), [1, 1, 2]);
    }

    #[tokio::test]
    async fn panel_builds_catalog_once_but_each_seat_owns_its_probe() {
        for quota in [false, true] {
            panel_shared_probe_case(quota).await;
        }
    }

    async fn panel_shared_probe_case(quota: bool) {
        use super::super::conformance_tests::install_panel_families;
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let harness = Harness::new("shared-panel-probes");
        let mut catalog = fixture::catalog();
        // Two different families use the same subscription account. A due
        // account fence permits exactly one decision to own its recovery probe.
        catalog
            .models
            .retain(|model| model.provider == fixture::SUBSCRIPTION_PROVIDER);
        assert_eq!(catalog.models.len(), 2);
        for (i, model) in catalog.models.iter_mut().enumerate() {
            model.family = Some(format!("family-{i}"));
        }
        catalog.accounts.truncate(1);
        install_catalog(&harness, &catalog);
        install_panel_families(&harness, &catalog);
        harness.edit_config(|config| {
            config["panels"]["audit"] = json!({"topK":2,"by":"iq","extra":0,"floor":0});
            config["consumers"][fixture::SUMMARY] = json!({"owner":"prefrontal-core"});
        });
        let route = harness.bind(reserved("prefrontal-core"), None).await;
        let routes: Vec<_> = catalog
            .models
            .iter()
            .map(|model| model.route_key())
            .collect();
        let request = json!({"class":"audit","consumer":fixture::SUMMARY,"seed":"fixed","available_routes":routes});
        take_input_builds(&harness);
        let baseline = route.call("route.select_panel", &request).await;
        assert_eq!(
            baseline.0,
            subc_protocol::FrameType::Response,
            "{baseline:?}"
        );
        assert_eq!(baseline.1["result"]["seats"].as_array().unwrap().len(), 2);
        assert_eq!(take_input_builds(&harness), [1, 2, 2]);
        if quota {
            *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = Default::default();
            let at = RECOVERY_NOW as i64 - 900_001;
            let scope = striatum_core::accounts::FenceScope::General(
                striatum_core::accounts::GeneralWindow::FiveHour,
            );
            let fence = striatum_core::accounts::Fence {
                account_id: catalog.accounts[0].account_id.clone(),
                pool: Some(scope.clone().into()),
                recovery_set: Vec::new(),
                observed_at_ms: at,
                reset: striatum_core::accounts::ResetTime::At(RECOVERY_NOW as i64 + 5 * 86_400_000),
                retry_hint_ms: None,
                source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
            };
            harness
                .store()
                .account_transaction(|tx| {
                    tx.insert_fence(&striatum_store::AccountFenceRow {
                        fence_id: "shared-panel-quota".into(),
                        provider: fixture::SUBSCRIPTION_PROVIDER.into(),
                        account_id: fence.account_id.clone(),
                        scope_json: serde_json::to_string(&scope).unwrap(),
                        fence_json: serde_json::to_string(&fence).unwrap(),
                        source: "broca_limit_exhausted".into(),
                        observed_at: at,
                        principal: "test".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: at,
                        cleared_at: None,
                        cleared_reason: None,
                    })
                })
                .unwrap();
        } else {
            crate::credential_fences::record(
                harness.store(),
                &harness.credentials(),
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: fixture::SUBSCRIPTION_PROVIDER.into(),
                    credential_id: catalog.accounts[0].credential_id.clone(),
                    code: "billing_exhausted".into(),
                },
                RECOVERY_NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1,
            )
            .unwrap();
        }
        take_input_builds(&harness);
        let probe = route.call("route.select_panel", &request).await;
        assert_eq!(probe.0, subc_protocol::FrameType::Response, "{probe:?}");
        let seats = probe.1["result"]["seats"].as_array().unwrap();
        assert_eq!(
            seats.len(),
            1,
            "only one seat can own the due probe: {probe:?}"
        );
        assert!(!seats[0]["answer"]["route"].is_null());
        assert_eq!(
            seats[0]["answer"]["route"],
            baseline.1["result"]["seats"][0]["answer"]["route"]
        );
        assert_eq!(take_input_builds(&harness), [1, 2, 2]);
        if !quota {
            return;
        }
        let decision = seats[0]["answer"]["decision_id"].clone();
        let _later_panel = crate::InjectedClock::set(RECOVERY_NOW + 120_000);
        let later = route.call("route.select_panel", &request).await;
        assert_eq!(later.0, subc_protocol::FrameType::Response, "{later:?}");
        assert_eq!(
            later.1["result"]["seats"].as_array().unwrap().len(),
            0,
            "a later panel cannot reuse the same quota probe: {later:?}"
        );
        let _failure = crate::InjectedClock::set(RECOVERY_NOW + 200_000);
        let reported = route
            .call(
                "route.report",
                &json!({"decision_id":decision,"outcome":"failed"}),
            )
            .await;
        assert_eq!(
            reported.0,
            subc_protocol::FrameType::Response,
            "{reported:?}"
        );
        let _old_interval = crate::InjectedClock::set(RECOVERY_NOW + 900_000);
        let waiting = route.call("route.select_panel", &request).await;
        assert_eq!(
            waiting.1["result"]["seats"].as_array().unwrap().len(),
            0,
            "{waiting:?}"
        );
        let _next_interval = crate::InjectedClock::set(RECOVERY_NOW + 1_100_000);
        let credentials = harness.credentials();
        let mut credentials = credentials.lock().unwrap();
        let rows = credentials.listed_credentials().to_vec();
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "next-panel-interval".into(),
                credentials: rows,
                ..Default::default()
            },
            RECOVERY_NOW + 1_100_000,
        );
        drop(credentials);
        let next = route.call("route.select_panel", &request).await;
        assert_eq!(
            next.1["result"]["seats"].as_array().unwrap().len(),
            1,
            "{next:?}"
        );
    }

    /// A quota route fence whose recheck has been overdue for days, with no
    /// reading to contradict it, is claimed by the panel seat that serves its
    /// route: the seat's decision owns the recovery probe, so a runner that
    /// sends without account admission still sends it exactly once.
    #[tokio::test]
    async fn panel_seat_claims_an_overdue_never_claimed_route_fence_recheck() {
        use super::super::conformance_tests::install_panel_families;
        let _clock = crate::InjectedClock::set(RECOVERY_NOW);
        let harness = Harness::new("panel-route-fence-probe");
        let mut catalog = fixture::catalog();
        catalog
            .models
            .retain(|model| model.provider == fixture::SUBSCRIPTION_PROVIDER);
        for (i, model) in catalog.models.iter_mut().enumerate() {
            model.family = Some(format!("family-{i}"));
        }
        catalog.accounts.truncate(1);
        install_catalog(&harness, &catalog);
        install_panel_families(&harness, &catalog);
        harness.edit_config(|config| {
            config["panels"]["audit"] = json!({"topK":2,"by":"iq","extra":0,"floor":0});
            config["consumers"][fixture::SUMMARY] = json!({"owner":"prefrontal-core"});
        });
        *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = Default::default();
        let fenced = &catalog.models[0];
        let reported = RECOVERY_NOW as i64 - 3 * 86_400_000;
        harness
            .store()
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: fenced.provider.clone(),
                credential_id: catalog.accounts[0].credential_id.clone(),
                model: fenced.model.clone(),
                until_ms: RECOVERY_NOW as i64 + 2 * 86_400_000,
                decision_id: "quota-report".into(),
                recorded_at: reported,
            })
            .unwrap();
        let route = harness.bind(reserved("prefrontal-core"), None).await;
        let routes: Vec<_> = catalog
            .models
            .iter()
            .map(|model| model.route_key())
            .collect();
        let panel = route
            .call(
                "route.select_panel",
                &json!({"class":"audit","consumer":fixture::SUMMARY,"seed":"fixed","available_routes":routes}),
            )
            .await;
        assert_eq!(panel.0, subc_protocol::FrameType::Response, "{panel:?}");
        let seat = panel.1["result"]["seats"]
            .as_array()
            .unwrap()
            .iter()
            .find(|seat| {
                let route = &seat["answer"]["route"];
                *route == fenced.route_key().as_str() || route["model"] == fenced.model.as_str()
            })
            .unwrap_or_else(|| panic!("the fenced route is seated as its probe: {panel:?}"));
        let decision = seat["answer"]["decision_id"].as_str().unwrap();
        let recheck = harness
            .store()
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|origin| origin.kind == "account_route")
            .expect("the route fence keeps its recheck clock");
        assert_eq!(
            recheck.claim_owner.as_deref(),
            Some(decision),
            "{recheck:?}"
        );
        assert_eq!(
            recheck.last_claimed_at,
            Some(RECOVERY_NOW as i64),
            "{recheck:?}"
        );
    }

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
                        code: striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED.into(),
                        message: "private message is not a retry protocol".into(),
                        detail: Some(json!({"earliestResetAtMs":reset})),
                    },
                    pin,
                    1_800_000_000_000,
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
