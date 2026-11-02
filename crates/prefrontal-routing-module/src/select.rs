//! Route and panel selection, including policy inputs and decision provenance.

use super::*;

/// One refresh snapshot supplies both profiles and provider reporting metadata.
pub(crate) struct TaskProfileLookup {
    pub(crate) profiles: prefrontal_routing_core::model_router::RouteTaskProfiles,
    pub(crate) providers_without_cache_write_reports: HashSet<String>,
}

/// Resolve only from memory; routing/v1 can call this after forming its shortlist.
pub(crate) fn task_profiles_for(
    workload: &str,
    iq: f64,
    shortlist: &[ModelFact],
) -> TaskProfileLookup {
    let snapshot = task_profile_refresh::cache().snapshot();
    TaskProfileLookup {
        profiles: snapshot.lookup(workload, iq, shortlist),
        providers_without_cache_write_reports: snapshot
            .providers_without_cache_write_reports
            .clone(),
    }
}

/// The full `route.select` body, decoded only after the method is confirmed.
#[derive(Debug, Deserialize)]
pub(super) struct SelectBody {
    pub(super) params: SelectParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SelectParams {
    #[serde(default)]
    pub(super) consumer: Option<select_policy::Consumer>,
    #[serde(
        default,
        rename = "allowed_providers",
        alias = "allowedProviders",
        deserialize_with = "select_policy::deserialize_allowed_providers"
    )]
    pub(super) allowed_providers: Option<Vec<String>>,
    #[serde(default)]
    pub(super) pin: Option<select_policy::Pin>,
    #[serde(default, rename = "pin_fallback", alias = "pinFallback")]
    pub(super) pin_fallback: select_policy::PinFallback,
    #[serde(default)]
    pub(super) model_pin: Option<String>,
    /// Revalidate a retained worker credential without changing its auth method.
    #[serde(default)]
    pub(super) continuation_credential_id: Option<String>,
    #[serde(default = "default_target_agent")]
    pub(super) target_agent: String,
    #[serde(default)]
    pub(super) workload: Option<selection_harness::Workload>,
    #[serde(default)]
    pub(super) executor: Option<selection_harness::Executor>,
    #[serde(default)]
    pub(super) required_harness: Option<substrate_policy::Harness>,
    /// Existing module callers may provide an already-resolved demand vector.
    #[serde(default)]
    pub(super) requirements: Option<ModelRequirements>,
    /// Launch callers provide raw optional requirements so the module applies
    /// clamping, specialist defaults, and Mason's required non-zero demand.
    #[serde(default)]
    pub(super) requested_requirements: Option<RequestedModelRequirements>,
    /// Raw routeKeys/capacityKeys reported by the harness.
    #[serde(default)]
    pub(super) available_models: Vec<String>,
    /// Per-attempt exclusions such as a route that just failed.
    #[serde(default)]
    pub(super) exclude_route_keys: Vec<String>,
    #[serde(default)]
    pub(super) required_capabilities: Option<RequiredCapabilities>,
    /// Retained so older callers can still decode; routing ignores these
    /// caller-policy fields in favor of its routing-owned config.
    #[serde(default)]
    pub(super) subscription_keys: Vec<String>,
    #[serde(default)]
    pub(super) routing_policy: Option<ModelRoutingPolicyInput>,
    #[serde(default)]
    pub(super) provider_auth_types: Option<HashMap<String, String>>,
    /// Gather seats can request cache-capable providers without changing the
    /// normal quality and cost ordering for other route selections.
    #[serde(default)]
    pub(super) prefer_prompt_cache: bool,
    /// Audit context for the recorded decision (route.select is now a mutate that
    /// records the decision server-side and returns its id).
    #[serde(default)]
    pub(super) task_id: Option<String>,
    #[serde(default, alias = "parentSessionID")]
    pub(super) parent_session_id: Option<String>,
    /// The project and folder the daemon stamped on the request's scope. This
    /// is never a request member: it is filled in from the route's bind just
    /// before the decision is built, so a body cannot name its own project.
    #[serde(skip)]
    pub(super) scope: Option<DecisionScope>,
    /// For `executor: broca`, `substrate: broca` uses Broca's served routes;
    /// absent or other values use `availableModels`. Launch selections ignore
    /// this legacy field and use each route's mapped harness.
    #[serde(default)]
    pub(super) substrate: Option<String>,
    /// The caller's harness, used to match the decision with recorded spend facts when provided.
    #[serde(default)]
    pub(super) harness: Option<String>,
    /// The caller's project directory, used to match the decision with recorded spend facts.
    #[serde(default)]
    pub(super) project_root: Option<String>,
    /// The caller may provide a launch attempt id before selection; otherwise it
    /// remains null because the router never invents a send correlation.
    #[serde(default, rename = "sendID")]
    pub(super) send_id: Option<String>,
    /// The caller's concurrency slot counts at request time, keyed by
    /// `provider/model`. A candidate whose slot is full is passed over for this
    /// decision only; absent (older callers, or a caller whose launch is not
    /// limited by these slots) leaves the selection unchanged.
    #[serde(default)]
    pub(super) concurrency_slots: Option<Vec<ConcurrencySlotState>>,
    #[serde(default, rename = "preferCapacityKey")]
    pub(super) preferred_capacity_key: Option<String>,
}

impl SelectParams {
    /// The params of a selection that carries none of the older request's
    /// audit members (task, session, harness, project root, send id): what a
    /// `routing/v1` decision records them as.
    pub(super) fn for_record() -> Self {
        serde_json::from_value(json!({})).expect("every select param has a default")
    }

    /// A host chooses from its own model list. A request explicitly sent to the
    /// vault substrate instead needs credential and account admission.
    pub(super) fn is_host_run(&self) -> bool {
        self.executor.is_none()
            && self.workload.is_none()
            && self.required_harness.is_none()
            && !is_broca_substrate(self.substrate.as_deref())
    }

    pub(super) fn resolved_workload(&self) -> Option<selection_harness::Workload> {
        self.workload.or_else(|| {
            if self.executor.is_none() {
                selection_harness::Workload::for_agent(&self.target_agent)
            } else {
                None
            }
        })
    }

    pub(super) fn executor_workload_line(&self) -> Option<String> {
        self.executor.as_ref().map(|_| {
            format!(
                "workload: {}",
                self.resolved_workload()
                    .map(selection_harness::Workload::as_str)
                    .unwrap_or("none")
            )
        })
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct SelectPanelBody {
    pub(super) params: SelectPanelParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SelectPanelParams {
    #[serde(default)]
    pub(super) package_tokens: Option<usize>,
    pub(super) panel_spec: panel_policy::PanelRequestSpec,
    /// The substrate this panel's legs will dispatch on, resolved by the caller
    /// at launch. It decides which availability list the seats are judged
    /// against; absent or unknown keeps the caller's host provider list.
    #[serde(default)]
    pub(super) substrate: Option<String>,
    #[serde(default)]
    pub(super) selection: Option<PanelSelection>,
    #[serde(default)]
    pub(super) roster: Vec<serde_json::Value>,
    #[serde(default)]
    pub(super) available_models: Vec<String>,
    #[serde(default)]
    pub(super) exclude_route_keys: Vec<String>,
    #[serde(default)]
    pub(super) required_capabilities: Option<RequiredCapabilities>,
    #[serde(default)]
    pub(super) subscription_keys: Vec<String>,
    #[serde(default)]
    pub(super) task_id: Option<String>,
    #[serde(default)]
    pub(super) seed: Option<String>,
    #[serde(default)]
    pub(super) parent_session_id: Option<String>,
    #[serde(default)]
    pub(super) harness: Option<String>,
    #[serde(default)]
    pub(super) project_root: Option<String>,
    #[serde(default, rename = "sendID")]
    pub(super) send_id: Option<String>,
}

fn default_target_agent() -> String {
    "unknown".to_string()
}

pub(super) struct ResolvedSelectRequest {
    pub(super) requirements: ModelRequirements,
    pub(super) required_capabilities: Option<RequiredCapabilities>,
    pub(super) policy: ResolvedRoutingPolicy,
}

/// Host auth facts come only from the request. Non-host selection classifies
/// each serving credential directly from the vault in the shared candidate pass.
pub(super) fn provider_auth_types(
    raw: Option<&HashMap<String, String>>,
) -> HashMap<String, String> {
    raw.cloned().unwrap_or_default()
}

/// Record the sole policy source with each decision so explanations remain tied
/// to the exact routing-owned document used for selection.
pub(super) fn policy_sources_line(operator: &ModelRoutingPolicyInput) -> String {
    format!(
        "policy sources: operator {} ({} exclude entries)",
        config_overrides::operator_policy_revision(operator),
        operator.exclude.len(),
    )
}

/// How long a head's content-policy refusal keeps that provider off its launches.
/// Long enough to cover a rephrase-and-relaunch cycle, short enough that a head
/// working on unrelated material afterwards gets the provider back on its own.
const HEAD_CONTENT_POLICY_EXCLUSION_MS: i64 = 60 * 60 * 1000;

/// Routes withheld from launches sharing a parent session after one of that
/// session's workers was refused by the provider's content policy, plus the
/// explanation lines that say so.
#[derive(Debug, Default, PartialEq)]
struct HeadContentPolicyExclusions {
    route_keys: HashSet<String>,
    notes: Vec<String>,
}

/// A content-policy refusal is about the words the head sent, not the route, so it
/// charges no strike and the route stays at full adequacy for everyone. That is
/// right for the fleet and wrong for the head that just sent those words: its
/// relaunch carries the same brief, routes to the same provider, and dies the same
/// way, and a launch decision is minted before the task exists so nothing keys the
/// retry to the refusal. Two seats lost two workers each in one day this way, every
/// one on OpenAI, every relaunch back onto OpenAI (2026-09-20). The head is the
/// stable key: for a bounded window after such a refusal, every route of the
/// refusing provider is withheld from that head's launches only.
fn head_content_policy_exclusions(
    store: &SqliteRouteStateStore,
    parent_session_id: Option<&str>,
    facts: &[ModelFact],
    now: i64,
) -> HeadContentPolicyExclusions {
    let Some(parent_session_id) = parent_session_id.map(str::trim).filter(|id| !id.is_empty())
    else {
        return HeadContentPolicyExclusions::default();
    };
    let failed = match store.recent_failed_decisions_for_parent(
        parent_session_id,
        now - HEAD_CONTENT_POLICY_EXCLUSION_MS,
    ) {
        Ok(failed) => failed,
        Err(error) => {
            // An unreadable ledger must not silently widen routing: the head keeps
            // every route and the decision says the read failed.
            return HeadContentPolicyExclusions {
                route_keys: HashSet::new(),
                notes: vec![format!(
                    "head content-policy history unreadable, no routes withheld: {error}"
                )],
            };
        }
    };
    let mut refused_providers: Vec<(String, String, i64)> = Vec::new();
    for decision in failed {
        // Typed core refusals carry durable item-scoped exclusions. Withholding
        // this provider from the whole head would also penalize unrelated items.
        if decision.outcome_error.contains("provider_policy_refusal") {
            continue;
        }
        let attribution = prefrontal_routing_core::classify_failure_attribution(
            RouteOutcome::SessionError,
            Some(decision.outcome_error.as_str()),
        );
        if attribution != prefrontal_routing_core::FailureAttribution::ContentPolicy {
            continue;
        }
        if refused_providers
            .iter()
            .any(|(provider, _, _)| provider == &decision.provider_id)
        {
            continue;
        }
        refused_providers.push((
            decision.provider_id,
            decision.route_key,
            decision.outcome_at,
        ));
    }
    if refused_providers.is_empty() {
        return HeadContentPolicyExclusions::default();
    }
    let mut exclusions = HeadContentPolicyExclusions::default();
    for (provider_id, route_key, outcome_at) in refused_providers {
        for fact in facts.iter().filter(|fact| fact.provider_id == provider_id) {
            exclusions.route_keys.insert(make_route_key(
                &fact.provider_id,
                &fact.model_id,
                fact.variant.as_deref(),
            ));
        }
        let minutes_ago = (now - outcome_at).max(0) / 60_000;
        exclusions.notes.push(format!(
            "withheld provider {provider_id} from this head for {} minutes: its worker on {route_key} was refused by the provider's content policy {minutes_ago}m ago; the refusal is about the request, not the route, so no strike was charged",
            HEAD_CONTENT_POLICY_EXCLUSION_MS / 60_000
        ));
    }
    exclusions
}

pub(super) fn claim_ignored_caller_policy_warning(sent: bool, warned: &Once) -> bool {
    if !sent {
        return false;
    }
    let mut claimed = false;
    warned.call_once(|| claimed = true);
    claimed
}

pub(super) fn warn_ignored_caller_policy_once(sent: bool) {
    static WARNED: Once = Once::new();
    if claim_ignored_caller_policy_warning(sent, &WARNED) {
        eprintln!(
            "[prefrontal-routing] ignoring caller-supplied model routing policy; configure model_routing in ~/.config/cortexkit/prefrontal-routing.jsonc"
        );
    }
}

fn warn_missing_consumer_once(target_agent: &str) -> bool {
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let Ok(mut warned) = WARNED.get_or_init(|| Mutex::new(HashSet::new())).lock() else {
        return false;
    };
    if !warned.insert(target_agent.to_owned()) {
        return false;
    }
    tracing::warn!(
        target_agent,
        "route.select omitted consumer; applying exclude.all only"
    );
    true
}

fn no_route_error(
    reason: RouteFailureReason,
    message: String,
    requirements: ModelRequirements,
) -> NoRouteError {
    NoRouteError {
        candidates: Vec::new(),
        reason,
        message,
        rejected: Vec::new(),
        requirements: Some(requirements),
        best_rejected: None,
        earliest_reset: None,
        earliest_reset_route: None,
        adequate_blockers: Default::default(),
    }
}

/// [`resolve_select_request_with_catalog_targets`] with no provider resolved
/// through the vault, for tests that resolve a request without a listing.
#[cfg(test)]
pub(super) fn resolve_select_request(
    params: &SelectParams,
    operator_policy: &ModelRoutingPolicyInput,
) -> Result<ResolvedSelectRequest, NoRouteError> {
    resolve_select_request_with_catalog_targets(
        params,
        operator_policy,
        &std::collections::BTreeMap::new(),
    )
}

/// Resolve a legacy select's requirements and available-model policy.
/// `catalog_targets` names the providers with no catalog rows that resolved
/// through the vault; they are never subscription-covered.
fn resolve_select_request_with_catalog_targets(
    params: &SelectParams,
    operator_policy: &ModelRoutingPolicyInput,
    catalog_targets: &std::collections::BTreeMap<String, String>,
) -> Result<ResolvedSelectRequest, NoRouteError> {
    let (requirements, required_capabilities) = if let Some(requirements) = params.requirements {
        (requirements, params.required_capabilities)
    } else {
        match resolve_launch_requirements(
            &params.target_agent,
            params.requested_requirements.as_ref(),
        ) {
            Ok(resolved) => {
                let capabilities =
                    match (resolved.required_capabilities, params.required_capabilities) {
                        (Some(mut defaults), Some(explicit)) => {
                            if explicit.vision == Some(true) {
                                defaults.vision = Some(true);
                            }
                            if let Some(min) = explicit.min_context {
                                defaults.min_context =
                                    Some(defaults.min_context.map_or(min, |value| value.max(min)));
                            }
                            Some(defaults)
                        }
                        (defaults, explicit) => explicit.or(defaults),
                    };
                (resolved.requirements, capabilities)
            }
            Err(error) => {
                return Err(no_route_error(
                    RouteFailureReason::InvalidRequirements,
                    error.message,
                    ModelRequirements { iq: 0.0, eq: 0.0 },
                ));
            }
        }
    };

    // A request that names its model (an admission pin or a work run's
    // `model=`) keeps operator-excluded routes in the available set: `exclude`
    // keeps a route out of automatic choice only. An admission pin's automatic
    // fallback still applies the operator's exclusions during admission.
    let mut availability_policy = operator_policy.clone();
    if params.pin.is_some() || params.model_pin.is_some() {
        availability_policy.exclude.clear();
    }
    let policy = resolve_routing_policy_with_catalog_targets(
        params.available_models.iter().cloned(),
        &availability_policy,
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(params.provider_auth_types.as_ref()),
        catalog_targets,
    );
    let uses_host_availability = (params.executor.is_some() || params.is_host_run())
        && !is_broca_substrate(params.substrate.as_deref());
    if uses_host_availability
        && params.model_pin.is_none()
        && params.pin.is_none()
        && policy.connected_model_count == 0
    {
        return Err(no_route_error(
            RouteFailureReason::NoAvailableModel,
            format!(
                "No models available to route {} (no connected providers detected).",
                params.target_agent
            ),
            requirements,
        ));
    }
    if uses_host_availability
        && params.model_pin.is_none()
        && params.pin.is_none()
        && policy.available_models.is_empty()
    {
        return Err(no_route_error(
            RouteFailureReason::NoAvailableModel,
            format!(
                "No models available to route {}: all {} connected models are excluded by operator policy or per-attempt route exclusions. Loosen model_routing.exclude.",
                params.target_agent, policy.connected_model_count
            ),
            requirements,
        ));
    }

    Ok(ResolvedSelectRequest {
        requirements,
        required_capabilities,
        policy,
    })
}

pub(super) struct PreparedSelectionInputs {
    pub(super) now: f64,
    pub(super) facts: Vec<ModelFact>,
    pub(super) snapshot: ModelRouteStateSnapshot,
    pub(super) quota_signal_json: String,
    /// Set when the quota snapshot was too old to use: the explanation line
    /// saying so, which a selected route carries.
    pub(super) quota_note: Option<String>,
}

#[derive(Clone)]
pub(super) struct DecisionProvenance {
    pub(super) quota_signal_json: String,
    pub(super) policy_revision: String,
}

/// The cache facts a decision's quota signal summary is built from.
pub(super) struct QuotaSignalView<'a> {
    pub(super) windows: &'a [ProviderUsage],
    pub(super) available: bool,
    pub(super) refreshed_at_ms: u64,
    pub(super) retained_degraded_count: usize,
    pub(super) last_error: Option<&'a str>,
    pub(super) restored_from_ms: Option<u64>,
    /// The snapshot's age when it is past the staleness bound; its windows
    /// were then NOT merged into the snapshot the scorer read.
    pub(super) stale_age_ms: Option<u64>,
}

/// Summarize the cached overlay that the scorer consumed without serializing the
/// full quota response. A no-signal marker is explicit so it cannot be confused
/// with old rows written before the provenance columns existed.
pub(super) fn quota_signal_json(
    snapshot: &ModelRouteStateSnapshot,
    view: &QuotaSignalView<'_>,
) -> String {
    let QuotaSignalView {
        windows,
        available,
        refreshed_at_ms,
        retained_degraded_count,
        last_error,
        restored_from_ms,
        stale_age_ms,
    } = *view;
    let mut signals: Vec<serde_json::Value> = snapshot
        .by_capacity
        .iter()
        .filter(|(_, state)| {
            state.quota_pressure.is_some() || state.quota_exhausted_until.is_some()
        })
        .map(|(capacity_key, state)| {
            json!({
                "capacityKey": capacity_key,
                "pressure": state.quota_pressure,
                "windowModel": state.quota_window_model,
                "exhaustedUntil": state.quota_exhausted_until,
            })
        })
        .collect();
    signals.sort_by(|left, right| {
        left["capacityKey"]
            .as_str()
            .cmp(&right["capacityKey"].as_str())
    });

    let healthy_provider_count = windows.iter().filter(|window| window.is_healthy()).count();
    let mut summary = if let Some(age_ms) = stale_age_ms.filter(|_| healthy_provider_count > 0) {
        // The cached readings were too old to steer this decision: say so, with
        // their age, rather than recording them as the signal that was used.
        json!({
            "state": "stale",
            "reason": stale_quota_note(age_ms, None),
            "available": available,
            "refreshedAtMs": refreshed_at_ms,
            "ageMs": age_ms,
            "staleAfterMs": QUOTA_STALE_AFTER_MS,
            "lastError": last_error,
            "healthyProviderCount": healthy_provider_count,
            "signals": signals,
        })
    } else if healthy_provider_count == 0 {
        let reason = if windows.is_empty() {
            "quota signal: none (no cached readings)"
        } else if last_error.is_some() {
            "quota signal: none (refresh unavailable)"
        } else {
            "quota signal: none (no usable readings)"
        };
        json!({
            "state": "none",
            "reason": reason,
            "available": available,
            "refreshedAtMs": refreshed_at_ms,
            "lastError": last_error,
        })
    } else {
        json!({
            "state": "present",
            "available": available,
            "refreshedAtMs": refreshed_at_ms,
            "healthyProviderCount": healthy_provider_count,
            "retainedDegradedCount": retained_degraded_count,
            "signals": signals,
        })
    };
    // A decision made on windows restored from before a restart says so, with the
    // time that snapshot was accepted; rows made on this process's own polls keep
    // the shape they always had.
    if let (Some(restored_from_ms), Some(object)) = (restored_from_ms, summary.as_object_mut()) {
        object.insert("restoredFromMs".into(), json!(restored_from_ms));
    }
    serde_json::to_string(&summary).unwrap_or_else(|_| {
        r#"{"state":"none","reason":"quota signal: serialization failed"}"#.to_string()
    })
}

/// Read one consistent store/cache view for every selection operation. Quota
/// windows are cloned before merging so no handler holds the cache lock while
/// recording or writing its response. `pricing` is the selection's own routing
/// configuration's multipliers and keep-free reserves: facts and subscription
/// pools are both read against it, so a configuration published mid-selection
/// cannot price or reserve some candidates differently from others.
pub(super) fn prepare_selection_inputs(
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    pricing: &RoutePricing,
    model_pools: &[prefrontal_routing_core::quota_extract::ModelPool],
) -> Result<PreparedSelectionInputs, String> {
    let now = now_ms() as f64;
    let facts = store
        .get_facts_with_observed_speed(now, pricing)
        .map_err(|e| format!("reading facts from route-state store: {e}"))?;
    let mut snapshot = store
        .build_snapshot(now)
        .map_err(|e| format!("building route-state snapshot: {e}"))?;
    // Retain cached windows through a transient quota outage: dropping
    // last-known pressure the moment one refresh fails would make every blip
    // look optimistically free. Past the staleness bound, though, the windows
    // are no longer evidence of current quota (resets get spent, windows roll
    // over), so they are not merged at all and every provider is quota-unknown:
    // neutral pressure, no quota wall. A provider that really is exhausted
    // still answers its next request with a 429, and that writes a hard
    // cooldown into route state, which this does not touch.
    let (
        quota_windows,
        quota_available,
        quota_refreshed_at_ms,
        retained_degraded_count,
        quota_last_error,
        quota_restored_from_ms,
    ) = match quota_cache.lock() {
        Ok(guard) => (
            (!guard.windows.is_empty()).then(|| guard.windows.clone()),
            guard.available,
            guard.refreshed_at_ms,
            guard.retained_degraded_since_ms.len(),
            guard.last_error.clone(),
            guard.restored_from_ms,
        ),
        Err(_) => (None, false, 0, 0, Some("cache poisoned".into()), None),
    };
    let stale_age_ms = quota_windows
        .as_ref()
        .and_then(|_| stale_quota_age_ms(quota_refreshed_at_ms, now as u64));
    if let Some(windows) = quota_windows.as_ref().filter(|_| stale_age_ms.is_none()) {
        merge_quota_overlay(&mut snapshot, windows, &facts, model_pools);
    }
    subscription_economics::overlay(&mut snapshot, &facts, now as i64, pricing);
    let quota_signal_json = quota_signal_json(
        &snapshot,
        &QuotaSignalView {
            windows: quota_windows.as_deref().unwrap_or_default(),
            available: quota_available,
            refreshed_at_ms: quota_refreshed_at_ms,
            retained_degraded_count,
            last_error: quota_last_error.as_deref(),
            restored_from_ms: quota_restored_from_ms,
            stale_age_ms,
        },
    );
    let quota_note =
        stale_age_ms.map(|age_ms| stale_quota_note(age_ms, quota_last_error.as_deref()));
    Ok(PreparedSelectionInputs {
        now,
        facts,
        snapshot,
        quota_signal_json,
        quota_note,
    })
}

/// The configuration documents one selection reads, resolved once for it.
pub(super) struct SelectionPolicySources<'a> {
    pub(super) live_config: &'a config_overrides::LiveConfig,
    pub(super) operator_config: &'a config_overrides::LiveConfig,
    pub(super) operator_config_path: &'a Path,
    /// Each provider with no catalog rows that resolved to a catalog provider
    /// through its credentials' `serves` vendor
    /// ([`BrocaCredentials::catalog_targets`]), read once for the selection.
    pub(super) catalog_targets: &'a std::collections::BTreeMap<String, String>,
}

/// What pricing a selection's inputs yields besides the priced facts and the
/// fenced snapshot.
pub(super) struct PricedSelection {
    pub(super) routing_config: prefrontal_routing_core::RoutingConfig,
    pub(super) provenance: DecisionProvenance,
    /// Empty input for legacy candidate selection: account choice supplies
    /// all credential eligibility decisions.
    pub(super) entitlement_fences:
        Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>,
    /// Providers known to publish a subscription plan. Host-only payment
    /// admission uses this list when it has no vault account class.
    /// `None` means the plan listing is not known yet.
    pub(super) subscription_plan_providers: Option<HashSet<String>>,
    pub(super) classes: Option<account_selection::ClassContext>,
}

/// Apply one selection's configuration to the facts and snapshot that
/// [`prepare_selection_inputs`] read: the live overlays, the rows borrowed by
/// providers with no catalog rows, Broca's billing-plane prices, the
/// credential entitlement fences. Every route selection runs this same sequence, so a
/// route fenced for one caller's selection is fenced for every other.
#[allow(clippy::too_many_arguments)]
pub(super) fn price_selection_inputs(
    facts: &mut Vec<ModelFact>,
    quota_signal_json: String,
    sources: &SelectionPolicySources<'_>,
    store: &SharedStore,
    broca_credentials: &SharedBrocaCredentials,
    credentialed: bool,
    priced_per_token: bool,
) -> Result<PricedSelection, ModuleError> {
    let classes = if credentialed {
        Some(
            account_selection::ClassContext::read(
                store,
                broca_credentials,
                Arc::new(sources.operator_config.clone()),
                ACCOUNT_QUOTA.get(),
                facts.clone(),
                now_ms() as i64,
            )
            .map_err(ModuleError::Message)?,
        )
    } else {
        None
    };
    let outcome = sources.live_config.apply(facts);
    let provenance = DecisionProvenance {
        quota_signal_json,
        policy_revision: outcome.policy_revision,
    };
    // Borrowed rows are added after the live overlays so they carry the target's
    // current quality data, and before pricing so they get their own plane.
    prefrontal_routing_core::score_alias::append_alias_rows(facts, sources.catalog_targets);
    let subscription_plan_providers = if let Ok(credentials) = broca_credentials.lock() {
        credentials.price_facts(facts, sources.live_config.pricing());
        credentials.subscription_plan_providers()
    } else {
        None
    };
    if let Some(classes) = &classes {
        for fact in facts.iter_mut() {
            let class = classes.class(fact, priced_per_token);
            if let Some(method) = class
                .rows
                .first()
                .and_then(|row| row.auth_method.as_deref())
            {
                classes
                    .credentials
                    .price_for_method(fact, method, sources.live_config.pricing());
            }
        }
    }
    sources.live_config.apply(facts);
    // A consumer priced per token is never priced as if a subscription
    // subsidised it. Only the consumer flag authorizes that payment class.
    if priced_per_token {
        for fact in facts.iter_mut() {
            fact.subscription_multiplier = None;
        }
    }
    // Eligibility is resolved by account choice; selection never reads the
    // credential-refusal table as an independent route-level restriction.
    let entitlement_fences = Vec::new();
    Ok(PricedSelection {
        routing_config: outcome.routing_config,
        provenance,
        entitlement_fences,
        subscription_plan_providers,
        classes,
    })
}

/// Where one route.select spent its time, for the slow-request log line. Each
/// stage is a separate bucket so `unclassified_ms` (total minus every bucket)
/// stays near zero; a large remainder means a stage is missing a timer.
pub(super) struct RouteSelectTiming {
    pub(super) started: Instant,
    pub(super) is_select: std::cell::Cell<bool>,
    pub(super) live_config_check_ms: std::cell::Cell<u64>,
    /// Waiting for the first billing-plane load before pricing anything.
    pub(super) billing_wait_ms: std::cell::Cell<u64>,
    /// Decoding the parameters and resolving requirements and the caller's
    /// available models (reads the auth file).
    pub(super) resolve_ms: std::cell::Cell<u64>,
    pub(super) snapshot_closure_ms: std::cell::Cell<u64>,
    /// Live overlays, score aliases and billing-plane pricing on the facts.
    pub(super) pricing_ms: std::cell::Cell<u64>,
    /// Harness availability, subscription policy, exclusions and pin lookup.
    pub(super) policy_ms: std::cell::Cell<u64>,
    /// Reading this head's recent failed launches from the route store.
    pub(super) head_history_ms: std::cell::Cell<u64>,
    pub(super) selection_ms: std::cell::Cell<u64>,
    /// Recording the selection on the winner ladder.
    pub(super) ladder_ms: std::cell::Cell<u64>,
    /// Writing the decision row whose id route.select returns to the caller.
    pub(super) decision_write_ms: std::cell::Cell<u64>,
    /// Resolving the Broca launch target (and any key-pool lease).
    pub(super) runner_ms: std::cell::Cell<u64>,
    /// Variant policy, explanation notes and building the response body.
    pub(super) finish_ms: std::cell::Cell<u64>,
    pub(super) reply_ms: std::cell::Cell<u64>,
}

/// Add `elapsed` to one stage bucket of a [`RouteSelectTiming`].
pub(super) fn add_stage_ms(bucket: &std::cell::Cell<u64>, elapsed: Duration) {
    bucket.set(
        bucket
            .get()
            .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
    );
}

impl RouteSelectTiming {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            is_select: std::cell::Cell::new(false),
            live_config_check_ms: std::cell::Cell::new(0),
            billing_wait_ms: std::cell::Cell::new(0),
            resolve_ms: std::cell::Cell::new(0),
            snapshot_closure_ms: std::cell::Cell::new(0),
            pricing_ms: std::cell::Cell::new(0),
            policy_ms: std::cell::Cell::new(0),
            head_history_ms: std::cell::Cell::new(0),
            selection_ms: std::cell::Cell::new(0),
            ladder_ms: std::cell::Cell::new(0),
            decision_write_ms: std::cell::Cell::new(0),
            runner_ms: std::cell::Cell::new(0),
            finish_ms: std::cell::Cell::new(0),
            reply_ms: std::cell::Cell::new(0),
        }
    }

    pub(super) fn add_live_config_check(&self, elapsed: Duration) {
        self.live_config_check_ms.set(
            self.live_config_check_ms
                .get()
                .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
        );
    }

    pub(super) fn add_snapshot_closure(&self, elapsed: Duration) {
        self.snapshot_closure_ms.set(
            self.snapshot_closure_ms
                .get()
                .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
        );
    }

    pub(super) fn record_selection(&self, elapsed: Duration) {
        // Adds rather than sets: a rejected pin with a fallback runs a second
        // selection, and both count.
        add_stage_ms(&self.selection_ms, elapsed);
    }

    pub(super) fn record_reply(&self, elapsed: Duration) {
        self.reply_ms
            .set(elapsed.as_millis().min(u64::MAX as u128) as u64);
    }

    pub(super) fn line_at(&self, now: Instant) -> Option<String> {
        let total_ms = now
            .saturating_duration_since(self.started)
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if !self.is_select.get() || total_ms <= 1_000 {
            return None;
        }
        let live_config_check_ms = self.live_config_check_ms.get();
        let billing_wait_ms = self.billing_wait_ms.get();
        let resolve_ms = self.resolve_ms.get();
        let snapshot_closure_ms = self.snapshot_closure_ms.get();
        let pricing_ms = self.pricing_ms.get();
        let policy_ms = self.policy_ms.get();
        let head_history_ms = self.head_history_ms.get();
        let selection_ms = self.selection_ms.get();
        let ladder_ms = self.ladder_ms.get();
        let decision_write_ms = self.decision_write_ms.get();
        let runner_ms = self.runner_ms.get();
        let finish_ms = self.finish_ms.get();
        let reply_ms = self.reply_ms.get();
        let classified_ms = [
            live_config_check_ms,
            billing_wait_ms,
            resolve_ms,
            snapshot_closure_ms,
            pricing_ms,
            policy_ms,
            head_history_ms,
            selection_ms,
            ladder_ms,
            decision_write_ms,
            runner_ms,
            finish_ms,
            reply_ms,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add);
        let unclassified_ms = total_ms.saturating_sub(classified_ms);
        Some(format!(
            "[routing] slow route.select total_ms={total_ms} live_config_check_ms={live_config_check_ms} snapshot_closure_ms={snapshot_closure_ms} selection_ms={selection_ms} reply_ms={reply_ms} billing_wait_ms={billing_wait_ms} resolve_ms={resolve_ms} pricing_ms={pricing_ms} policy_ms={policy_ms} head_history_ms={head_history_ms} ladder_ms={ladder_ms} decision_write_ms={decision_write_ms} runner_ms={runner_ms} finish_ms={finish_ms} unclassified_ms={unclassified_ms}"
        ))
    }
}

#[derive(Default)]
pub(super) struct RouteSelectLogLimiter {
    last_report: Option<Instant>,
    suppressed: u64,
}

pub(super) fn rate_limit_route_select_line(
    line: String,
    now: Instant,
    limiter: &mut RouteSelectLogLimiter,
) -> Option<String> {
    const WINDOW: Duration = Duration::from_secs(10);
    if limiter
        .last_report
        .is_some_and(|last| now.saturating_duration_since(last) < WINDOW)
    {
        limiter.suppressed = limiter.suppressed.saturating_add(1);
        return None;
    }
    limiter.last_report = Some(now);
    let suppressed = std::mem::take(&mut limiter.suppressed);
    Some(if suppressed == 0 {
        line
    } else {
        format!("{line} (suppressed {suppressed} similar lines)")
    })
}

fn emit_route_select_line(line: String) {
    static LIMITER: OnceLock<Mutex<RouteSelectLogLimiter>> = OnceLock::new();
    let now = Instant::now();
    let Some(mut limiter) = LIMITER.get_or_init(Default::default).lock().ok() else {
        return;
    };
    if let Some(line) = rate_limit_route_select_line(line, now, &mut limiter) {
        eprintln!("{line}");
    }
}

struct RouteSelectTimingGuard(RouteSelectTiming);

impl RouteSelectTimingGuard {
    pub(super) fn new() -> Self {
        Self(RouteSelectTiming::new())
    }
}

impl std::ops::Deref for RouteSelectTimingGuard {
    type Target = RouteSelectTiming;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for RouteSelectTimingGuard {
    fn drop(&mut self) {
        if let Some(line) = self.0.line_at(Instant::now()) {
            emit_route_select_line(line);
        }
    }
}

pub(super) async fn handle_select_request(
    mut frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    _min_band: &SharedMinBand,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let timing = RouteSelectTimingGuard::new();
    let live_config_started = Instant::now();
    let channel = frame.header.channel;
    frame.header.epoch = route_epoch;
    let epoch = route_epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
    let live_config = config_overrides::live_config(routing_config_path);
    let live_config_elapsed = live_config_started.elapsed();

    // Decode the method first: an unknown method must report `unknown_method` even
    // when params are absent/malformed (the params are method-specific).
    let envelope: MethodEnvelope = match serde_json::from_slice(&frame.body) {
        Ok(e) => e,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("route request body has no decodable method: {e}"),
            )
            .await;
        }
    };

    timing.is_select.set(envelope.method == ROUTE_SELECT_OP);
    timing.add_live_config_check(live_config_elapsed);

    if !handles(&envelope.method) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "unknown_method",
            &format!("unknown method '{}'", envelope.method),
        )
        .await;
    }

    if envelope.method == ROUTE_CATALOG_TARGETS_OP {
        return crate::catalog_targets::serve(frame, writer, broca_credentials).await;
    }

    if envelope.method == QUOTA_STATUS_OP {
        let consumer = quota_cache.lock().ok().map(|cache| cache.consumer.clone());
        let (available, refreshed_at_ms, providers, healthy, last_error, restored_from_ms) =
            match quota_cache.lock() {
                Ok(g) => (
                    g.available,
                    g.refreshed_at_ms,
                    g.windows.len(),
                    g.windows.iter().filter(|w| w.is_healthy()).count(),
                    g.last_error.clone(),
                    g.restored_from_ms,
                ),
                Err(_) => (false, 0, 0, 0, Some("cache poisoned".to_string()), None),
            };
        let body = serde_json::to_vec(&json!({ "result": {
            "available": available,
            "refreshedAtMs": refreshed_at_ms,
            "providerCount": providers,
            "healthyCount": healthy,
            "lastError": last_error,
            "restoredFromMs": restored_from_ms,
            "staleAfterMs": QUOTA_STALE_AFTER_MS,
            "staleAgeMs": stale_quota_age_ms(refreshed_at_ms, now_ms()),
            "consumer": consumer,
        }}))
        .map_err(ModuleError::Json)?;
        let response = Frame::build_with_version(
            ver,
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            channel,
            epoch,
            corr,
            body,
        )
        .map_err(|e| ModuleError::Message(e.to_string()))?;
        return send(writer, response).await;
    }

    if envelope.method == ROUTE_ACCOUNT_QUOTA_STATUS_OP {
        let status = match ACCOUNT_QUOTA.get().map(|state| state.lock()) {
            Some(Ok(guard)) => account_quota::status_json(&guard, now_ms() as i64),
            Some(Err(_)) => json!({ "error": "account quota state poisoned" }),
            None => json!({ "error": "account quota state not initialized yet" }),
        };
        let body = serde_json::to_vec(&json!({ "result": status })).map_err(ModuleError::Json)?;
        let response = Frame::build_with_version(
            ver,
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            channel,
            epoch,
            corr,
            body,
        )
        .map_err(|e| ModuleError::Message(e.to_string()))?;
        return send(writer, response).await;
    }

    if account_selection::handles(&envelope.method) {
        let principal = route_principal(channel, epoch);
        let policy = prefrontal_routing_core::accounts::AllocatorPolicy {
            model_pools: live_config.quota_model_pools(),
            ..Default::default()
        };
        // The contract has no `store_error` or `internal_error` for
        // `route.account_for`; a context that cannot load is transient
        // unavailability, not a transport failure.
        let classes = match account_selection::ClassContext::load(
            store,
            broca_credentials,
            config_overrides::live_config(operator_config_path),
            ACCOUNT_QUOTA.get(),
            account_selection::now_ms(),
        ) {
            Ok(classes) => classes,
            Err(error) => {
                let reply = account_selection::OpReply::Error {
                    code: "account_selection_unavailable".to_string(),
                    message: format!("routing could not load account context: {error}"),
                    detail: Some(json!({
                        "code": "account_selection_unavailable",
                        "retry_after_s": null,
                        "decision_id": null,
                        "exclusions": [],
                    })),
                };
                return send_account_reply(writer, ver, channel, epoch, corr, reply).await;
            }
        };
        let ctx = account_selection::Ctx {
            store: store.as_ref(),
            accounts: ACCOUNT_QUOTA.get(),
            flags: live_config.account_flags(),
            policy: &policy,
            now_ms: account_selection::now_ms(),
            classes: Some(classes),
        };
        let reply = account_selection::handle(&envelope.method, &frame.body, &principal, &ctx);
        if envelope.method == account_selection::ACCOUNT_OBSERVE_OP {
            if let (Some(shared), Some(observation)) = (
                USAGE_OBSERVE.get(),
                account_selection::usage_observation(&frame.body, &principal, &ctx),
            ) {
                shared.record(observation);
            }
        }
        return send_account_reply(writer, ver, channel, epoch, corr, reply).await;
    }

    if envelope.method == ROUTE_SET_DECISION_OUTCOME_OP {
        return handle_set_decision_outcome(
            frame,
            writer,
            store,
            routing_config_path,
            broca_credentials,
        )
        .await;
    }
    if envelope.method == ROUTE_RECORD_USAGE_OP {
        return handle_record_usage(frame, writer, store).await;
    }
    if envelope.method == ROUTE_RECORD_COOLDOWN_OP {
        return handle_record_cooldown(frame, writer, store).await;
    }
    if envelope.method == ROUTE_CLEAR_FAILURE_BACKOFF_OP {
        return handle_clear_failure_backoff(frame, writer, store).await;
    }
    if envelope.method == ROUTE_CLEAR_COOLDOWN_OP {
        return handle_clear_cooldown(frame, writer, store).await;
    }
    if envelope.method == ROUTE_CLEAR_CREDENTIAL_FENCES_OP {
        return handle_clear_credential_fences(frame, writer, store).await;
    }
    if envelope.method == ROUTE_COOLDOWN_STATUS_OP {
        return handle_cooldown_status(frame, writer, store).await;
    }
    if envelope.method == ROUTE_EXPLAIN_OP {
        return handle_route_explain(frame, writer, store).await;
    }
    if envelope.method == ROUTE_DECISIONS_LIST_OP {
        return handle_route_decisions_list(frame, writer, store).await;
    }
    if envelope.method == diagnostics_findings::OP {
        return diagnostics_findings::handle(frame, writer, store).await;
    }
    if envelope.method == ROUTE_RESOLVE_POLICY_OP {
        return handle_resolve_policy(frame, writer, operator_config_path).await;
    }
    if envelope.method == ROUTE_MODEL_FACT_OP {
        return handle_model_fact(frame, writer, store).await;
    }
    if envelope.method == ROUTE_MODEL_FACTS_OP {
        return handle_model_facts(frame, writer, store, operator_config_path).await;
    }
    if envelope.method == model_resolution::RESOLVE_MODEL_OP {
        return model_resolution::handle_resolve_model(frame, writer, &live_config.model_naming())
            .await;
    }
    if envelope.method == model_resolution::MODEL_CAPABILITIES_OP {
        return model_resolution::handle_model_capabilities(
            frame,
            writer,
            store,
            &live_config.model_naming(),
        )
        .await;
    }
    if envelope.method == model_resolution::AGENT_MODEL_REQUIREMENTS_OP {
        return model_resolution::handle_agent_model_requirements(frame, writer).await;
    }
    if envelope.method == available_models_text::OP {
        return available_models_text::handle(frame, writer, store, operator_config_path).await;
    }
    if envelope.method == ROUTE_MODEL_UPSERT_OP {
        let ladder_sources =
            winner_ladder::LadderSources::live(routing_config_path, operator_config_path);
        return handle_model_upsert(frame, writer, store, &ladder_sources, broca_credentials).await;
    }
    if envelope.method == ROUTE_LIST_INFERRED_OP {
        return handle_list_inferred(frame, writer, store).await;
    }
    if envelope.method == ROUTE_LIST_UNSCORED_OP {
        return handle_list_unscored(frame, writer, store).await;
    }
    if envelope.method == ROUTE_LIST_CANONICAL_GAPS_OP {
        return handle_list_canonical_gaps(frame, writer, store).await;
    }
    if key_pool::handles(&envelope.method) {
        // The pool's providers resolve from the stored catalog rows and the
        // vault listing.
        let facts = match store.get_facts() {
            Ok(facts) => facts,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("key pool: {error}"),
                )
                .await;
            }
        };
        let catalog_targets = broca_credentials
            .lock()
            .map(|credentials| credentials.catalog_targets(&facts))
            .unwrap_or_default();
        let reply = key_pool::handle(
            &envelope.method,
            &frame.body,
            &key_pool::PoolCtx {
                store: store.as_ref(),
                credentials: broca_credentials,
                catalog_targets: &catalog_targets,
                now_ms: now_ms() as i64,
            },
        );
        return send_pool_reply(writer, ver, channel, epoch, corr, reply).await;
    }
    if envelope.method == ROUTE_SELECT_OP {
        if let Ok(request) = serde_json::from_slice::<serde_json::Value>(&frame.body) {
            let params = &request["params"];
            let consumer = routing_v1::consumers::legacy_consumer(params);
            if !routing_v1::consumers::legacy_core_consumer(consumer) {
                let operator = config_overrides::live_config(operator_config_path);
                let consumers = routing_v1::consumers::effective(&operator);
                // Preserve the legacy decoder's invalid_request for malformed
                // enum values. A recognized non-core consumer must pass caller
                // authorization before its legacy enum value is decoded.
                let invalid_legacy_consumer = params.get("consumer").is_some_and(|value| {
                    serde_json::from_value::<Option<select_policy::Consumer>>(value.clone())
                        .is_err()
                }) && !consumers.knows(consumer);
                if let Err(refusal) = if invalid_legacy_consumer {
                    Ok(())
                } else {
                    consumers.admit(consumer, &routing_v1::Caller::for_route(channel, epoch))
                } {
                    let body = ErrorBody {
                        code: refusal.code.as_str().to_owned(),
                        message: refusal.message,
                        detail: Some(serde_json::Value::Object(refusal.detail)),
                    };
                    return send_route_error_body(writer, ver, channel, epoch, corr, body).await;
                }
            }
        }
    }
    let host_request = envelope.method == ROUTE_SELECT_OP
        && serde_json::from_slice::<SelectBody>(&frame.body)
            .is_ok_and(|body| body.params.is_host_run());
    let billing_ready = if !host_request
        && matches!(
            envelope.method.as_str(),
            ROUTE_SELECT_OP | ROUTE_SELECT_PANEL_OP
        ) {
        let billing_started = Instant::now();
        let ready =
            broca_credentials::wait_for_billing_catalog(broca_credentials, Duration::from_secs(3))
                .await;
        add_stage_ms(&timing.billing_wait_ms, billing_started.elapsed());
        ready
    } else {
        true
    };
    if !billing_ready {
        let body = serde_json::to_vec(&ErrorBody {
            code: "routing_not_ready".into(),
            message: "routing pricing is awaiting its first billing-plane load; retry shortly"
                .into(),
            detail: Some(json!({"retryable": true, "retry_after_ms": 1000})),
        })
        .map_err(ModuleError::Json)?;
        return send(
            writer,
            Frame::build_with_version(
                ver,
                FrameType::Error,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?,
        )
        .await;
    }
    if envelope.method == ROUTE_SELECT_PANEL_OP {
        return handle_select_panel(
            frame,
            writer,
            quota_cache,
            store,
            routing_config_path,
            operator_config_path,
            broca_credentials,
        )
        .await;
    }

    if envelope.method != ROUTE_SELECT_OP {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "unknown_method",
            &format!("unknown method '{}'", envelope.method),
        )
        .await;
    }

    let resolve_started = Instant::now();
    let params: SelectParams = match serde_json::from_slice::<SelectBody>(&frame.body) {
        Ok(b) => b.params,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("route.select params not decodable: {e}"),
            )
            .await;
        }
    };
    // The decision ledger records the project and folder the daemon stamped on
    // this route's scope, never anything the request body says.
    let params = SelectParams {
        scope: route_scope(channel, epoch),
        ..params
    };
    let host_run = params.is_host_run();
    let selected_workload = params
        .resolved_workload()
        .filter(|_| params.executor.is_none() && !host_run)
        .map(selection_harness::Workload::as_str);
    let executor_workload = params.executor_workload_line();
    warn_ignored_caller_policy_once(
        params.routing_policy.is_some() || !params.subscription_keys.is_empty(),
    );
    if params.consumer.is_none() {
        warn_missing_consumer_once(&params.target_agent);
    }
    if params.pin.is_some() && params.model_pin.is_some() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "pin and modelPin are mutually exclusive",
        )
        .await;
    }
    add_stage_ms(&timing.resolve_ms, resolve_started.elapsed());
    // The operator fence is read HERE, on the module side, for every selection.
    let live_config_started = Instant::now();
    let operator_config = if operator_config_path == routing_config_path {
        Arc::clone(&live_config)
    } else {
        config_overrides::live_config(operator_config_path)
    };
    let operator_policy = operator_config.operator_policy();
    timing.add_live_config_check(live_config_started.elapsed());
    let snapshot_started = Instant::now();
    let stored_facts = match store.get_facts() {
        Ok(facts) => facts,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("reading facts from route-state store: {e}"),
            )
            .await;
        }
    };
    // Providers with no catalog rows of their own, each resolved to the catalog
    // provider whose rows it borrows, read once for this selection.
    let catalog_targets = broca_credentials
        .lock()
        .map(|credentials| credentials.catalog_targets(&stored_facts))
        .unwrap_or_default();
    timing.add_snapshot_closure(snapshot_started.elapsed());
    // A host does not choose the substrate of the mason it launches. A host's
    // mason pin on a provider that needs a per-send credential pin is admitted
    // provisionally against Broca's routes, without leasing a key; core selects
    // the pin again with substrate broca before it records the launch.
    let deferred_runner_pin = host_run
        && params.substrate.is_none()
        && params.target_agent == "mason"
        && params.model_pin.as_deref().is_some_and(|pin| {
            pin.split_once('/')
                .is_some_and(|(provider, _)| needs_runner_pin(&catalog_targets, provider))
        });
    // The harness a host-run selection is judged on: Broca's served routes when
    // the caller names substrate broca (or for a deferred runner pin), otherwise
    // the caller's own list, which has no harness of routing's choosing.
    let host_run_harness = (host_run
        && (deferred_runner_pin || is_broca_substrate(params.substrate.as_deref())))
    .then_some(substrate_policy::Harness::Broca);
    let snapshot_started = Instant::now();
    let mut availability = if host_run && !deferred_runner_pin {
        SelectAvailability::host(params.available_models.clone())
    } else {
        let mut facts = stored_facts;
        live_config.apply(&mut facts);
        // Resolved providers are reachable through their own credentials, so
        // their borrowed rows join the served-route join.
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &catalog_targets);
        select_availability(
            Some(legacy_wire::BROCA),
            Vec::new(),
            &facts,
            broca_credentials,
        )
    };
    timing.add_snapshot_closure(snapshot_started.elapsed());
    // A host-run selection judged on the caller's own list never consulted
    // Broca's routes, so its explanation must not describe them.
    if host_run && host_run_harness.is_none() {
        availability.note = None;
    }
    let resolve_started = Instant::now();
    let mut resolved_request = match resolve_select_request_with_catalog_targets(
        &params,
        &operator_policy,
        &catalog_targets,
    ) {
        Ok(resolved) => {
            add_stage_ms(&timing.resolve_ms, resolve_started.elapsed());
            resolved
        }
        Err(mut no_route) => {
            add_stage_ms(&timing.resolve_ms, resolve_started.elapsed());
            let write_started = Instant::now();
            if let Some(note) = &availability.note {
                no_route.message = format!("{} ({note})", no_route.message);
            }
            let requirements = no_route
                .requirements
                .unwrap_or(ModelRequirements { iq: 0.0, eq: 0.0 });
            let decision = decision_input_for(
                &params.target_agent,
                &requirements,
                None,
                Some(&no_route),
                &params,
                None,
                Some(&policy_sources_line(&operator_policy)),
            );
            record_failed_selection_decision(store.as_ref(), &decision);
            add_stage_ms(&timing.decision_write_ms, write_started.elapsed());
            return send_no_route(
                writer,
                ver,
                channel,
                epoch,
                corr,
                &no_route,
                params.pin.as_ref().map(|_| "invalid_requirements"),
            )
            .await;
        }
    };
    let providers_without_prompt_cache = live_config.providers_without_prompt_cache();
    let snapshot_started = Instant::now();
    let prepared = prepare_selection_inputs(
        quota_cache,
        store,
        live_config.pricing(),
        &live_config.quota_model_pools(),
    );
    timing.add_snapshot_closure(snapshot_started.elapsed());
    let PreparedSelectionInputs {
        now,
        mut facts,
        mut snapshot,
        quota_signal_json,
        quota_note,
    } = match prepared {
        Ok(prepared) => prepared,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &e).await;
        }
    };
    let pricing_started = Instant::now();
    let consumer_id = variant_policy::consumer_group(params.consumer).unwrap_or("");
    let consumer_policy = operator_config.consumer_policy(
        consumer_id,
        &config_policy::ConsumerPolicy::default(),
        &std::collections::BTreeMap::new(),
    );
    // Apply the configured payment-class policy even when a legacy caller names
    // its consumer through targetAgent rather than the consumer member.
    let payment_consumer = params
        .consumer
        .or_else(|| select_policy::Consumer::from_id(&params.target_agent));
    let payment_policy = operator_config.consumer_policy(
        variant_policy::consumer_group(payment_consumer).unwrap_or(""),
        &config_policy::ConsumerPolicy::default(),
        &std::collections::BTreeMap::new(),
    );
    let priced_per_token = select_policy::priced_per_token(&payment_policy, payment_consumer);
    let PricedSelection {
        routing_config,
        provenance,
        entitlement_fences,
        subscription_plan_providers,
        mut classes,
    } = price_selection_inputs(
        &mut facts,
        quota_signal_json,
        &SelectionPolicySources {
            live_config: &live_config,
            operator_config: &operator_config,
            operator_config_path,
            catalog_targets: &catalog_targets,
        },
        store,
        broca_credentials,
        !host_run,
        priced_per_token,
    )?;
    if let Some(credential) = &params.continuation_credential_id {
        if let Some(classes) = &mut classes {
            classes.revalidate_credential(credential);
        }
    }
    add_stage_ms(&timing.pricing_ms, pricing_started.elapsed());
    let policy_started = Instant::now();
    let host_models: HashSet<String> = resolved_request
        .policy
        .available_models
        .iter()
        .cloned()
        .collect();
    let broca_models: HashSet<String> = availability.models.iter().cloned().collect();
    let frontier_broca_models = broca_credentials
        .lock()
        .map(|credentials| credentials.statically_served_routes(&facts))
        .unwrap_or_default();
    let executor_models = if is_broca_substrate(params.substrate.as_deref()) {
        &broca_models
    } else {
        &host_models
    };
    let harness_context = selection_harness::SelectionHarness {
        groups: operator_config.substrate_groups(),
        consumer: params.consumer,
        workload: params.resolved_workload(),
        executor: params.executor.is_some(),
        required: params.required_harness,
        caller: params.harness.as_deref(),
        host_models: &host_models,
        broca_models: &broca_models,
        executor_models,
    };
    // A host-run selection judged on Broca that carries the head's model list
    // re-selects a launch the head already routed on that list (core's Broca
    // step for a routed mason, or a route switch of such a worker). It stays
    // inside the list: Broca's reach decides which of those models can run,
    // never which other models may. A pin names its own model and is not
    // confined.
    let confine_broca = (host_run_harness.is_some()
        || (is_broca_substrate(params.substrate.as_deref())
            && params.executor.is_none()
            && params.workload.is_none()
            && params.required_harness.is_none()))
        && params.pin.is_none()
        && params.model_pin.is_none()
        && !params.available_models.is_empty();
    let confined_broca_models = confine_broca.then(|| {
        let confined = confine_to_caller_list(&broca_models, &params.available_models, &facts);
        if let Some(note) = availability.note.as_mut() {
            note.push_str(&format!(
                "; confined to the caller's {} listed models: {} reachable",
                params.available_models.len(),
                confined.len()
            ));
        }
        confined
    });
    let mut mapped_availability = if host_run || confined_broca_models.is_some() {
        selection_harness::MappedAvailability {
            models: if host_run_harness.is_some() || !host_run {
                confined_broca_models.unwrap_or_else(|| broca_models.clone())
            } else {
                host_models.clone()
            },
            rejected: HashMap::new(),
        }
    } else {
        harness_context.availability(&facts)
    };
    let frontier_models = if host_run || confine_broca {
        if host_run_harness.is_some() || !host_run {
            if confine_broca {
                confine_to_caller_list(&frontier_broca_models, &params.available_models, &facts)
            } else {
                frontier_broca_models.clone()
            }
        } else {
            host_models.clone()
        }
    } else {
        selection_harness::SelectionHarness {
            broca_models: &frontier_broca_models,
            executor_models: if is_broca_substrate(params.substrate.as_deref()) {
                &frontier_broca_models
            } else {
                &host_models
            },
            ..harness_context
        }
        .availability(&facts)
        .models
    };
    let host_auth = params.provider_auth_types.clone().unwrap_or_default();
    if let Some(classes) = &classes {
        classes.apply_eligibility(
            &mut snapshot,
            &facts,
            priced_per_token,
            params.pin.is_some() || params.model_pin.is_some(),
        );
        classes.apply_legacy_route_fences(
            &mut snapshot,
            &facts,
            priced_per_token,
            params.pin.is_some() || params.model_pin.is_some(),
        );
    }
    let candidates = select_policy::prepare_candidates(
        &facts,
        &mut snapshot,
        mapped_availability.models,
        mapped_availability.rejected,
        if host_run {
            select_policy::CandidateSource::Host {
                auth_types: &host_auth,
            }
        } else {
            select_policy::CandidateSource::Vault {
                credentials: broca_credentials,
                fences: &entitlement_fences,
                stale: &[],
                defer_pool_pin: params.pin.is_some() || params.model_pin.is_some(),
            }
        },
        params.consumer,
        priced_per_token,
        params.allowed_providers.as_deref(),
        subscription_plan_providers.as_ref(),
        &operator_policy,
        &catalog_targets,
        classes.as_ref(),
    );
    let subscription_keys = candidates.subscription_keys;
    let candidate_filters = candidates.filters;
    resolved_request.policy.subscription_keys = subscription_keys.iter().cloned().collect();
    resolved_request.policy.subscription_keys.sort();
    mapped_availability = selection_harness::MappedAvailability {
        models: candidates.available,
        rejected: candidates.rejected,
    };
    let runner_registry = broca_credentials
        .lock()
        .map(|credentials| {
            routing_v1::runners::Registry::build(
                operator_config.v1_runners(),
                &credentials,
                &facts,
                snapshot.now as u64,
            )
        })
        .unwrap_or_default();
    let configured_runners = runner_registry.policies();
    let runner_runs = |runner: &config_policy::RunnerPolicy, key: &str, capacity: &str| {
        runner_registry.runs(&runner.module, key, capacity)
    };
    if !host_run {
        mapped_availability.models.retain(|key| {
            let capacity = facts
                .iter()
                .map(fact_to_route)
                .find(|route| route.route_key == *key)
                .map(|route| route.capacity_key)
                .unwrap_or_else(|| key.clone());
            select_policy::choose_runner(
                &configured_runners,
                consumer_policy.runner.as_deref(),
                key,
                &capacity,
                &runner_runs,
            )
            .is_some()
        });
    }
    for fact in &facts {
        let route = fact_to_route(fact);
        if frontier_models.contains(&route.route_key)
            && (!mapped_availability.models.contains(&route.route_key)
                || mapped_availability.rejected.contains_key(&route.route_key))
            && !candidate_filters.contains_key(&route.route_key)
            && operator_config.route_allowed(consumer_id, &route.route_key, false)
            && select_policy::choose_runner(
                &configured_runners,
                consumer_policy.runner.as_deref(),
                &route.route_key,
                &route.capacity_key,
                |runner, key, capacity| runner_registry.can_run(&runner.module, key, capacity),
            )
            .is_some()
        {
            snapshot
                .by_route
                .entry(route.route_key)
                .or_default()
                .statically_routable
                .get_or_insert(true);
        }
    }
    let available = mapped_availability.models.clone();
    let runner_module = |selected: &SelectedModelRoute| -> Option<String> {
        if host_run {
            Some(
                if deferred_runner_pin {
                    legacy_wire::BROCA
                } else {
                    params
                        .harness
                        .as_deref()
                        .or(params.substrate.as_deref())
                        .unwrap_or("host")
                }
                .to_owned(),
            )
        } else {
            select_policy::choose_runner(
                &configured_runners,
                consumer_policy.runner.as_deref(),
                &selected.route_key,
                &selected.capacity_key,
                &runner_runs,
            )
            .map(|runner| runner.module.clone())
        }
    };
    // The harness a route selected here runs on, as far as routing knows: the
    // mapped harness, or for a host-run selection Broca when it was judged on
    // Broca's routes and unknown otherwise.
    let run_harness = |route_key: &str| {
        if host_run {
            host_run_harness
        } else {
            Some(harness_context.mapped(route_key))
        }
    };
    // Record on the winner's harness ladder, keeping other harnesses' routes
    // out of its availability history. Target agents retain separate histories.
    //
    // A request that names its model keeps operator-excluded routes in
    // `available` (see resolve_select_request). The ladder replays automatic
    // selection, so it is handed the fenced list an unpinned request would see;
    // otherwise pinned and unpinned launches of one class would record two lists
    // and read as an availability change on every switch.
    let ladder_fence = ExclusionSpecs::parse(ExclusionPolicy {
        operator: &operator_policy.exclude,
        caller: &[],
    });
    let record_ladder = |harness: Option<substrate_policy::Harness>| {
        winner_ladder::observed_selections().record(
            winner_ladder::LadderKey::new(
                if harness == Some(substrate_policy::Harness::Broca) {
                    winner_ladder::SUBSTRATE_BROCA
                } else {
                    winner_ladder::SUBSTRATE_HOST
                },
                &params.target_agent,
            ),
            available
                .iter()
                .filter(|key| host_run || harness == Some(harness_context.mapped(key)))
                .filter(|key| !ladder_fence.excludes(key))
                .cloned(),
            subscription_keys
                .iter()
                .filter(|key| !ladder_fence.excludes(key))
                .cloned(),
            snapshot.clone(),
        )
    };
    let mut exclude = params.exclude_route_keys.clone();
    add_stage_ms(&timing.policy_ms, policy_started.elapsed());
    let head_history_started = Instant::now();
    let head_refusals = head_content_policy_exclusions(
        store,
        params.parent_session_id.as_deref(),
        &facts,
        now as i64,
    );
    add_stage_ms(&timing.head_history_ms, head_history_started.elapsed());
    let mut policy_started = Instant::now();
    exclude.extend(head_refusals.route_keys.iter().cloned());
    let operator_exclude = operator_policy.exclude.clone();
    let consumer_exclude =
        select_policy::consumer_exclusions(operator_config.exclude_groups(), params.consumer);
    let requirements = resolved_request.requirements;
    let required_capabilities = resolved_request.required_capabilities;
    let target_agent = params.target_agent.clone();
    let prefer_prompt_cache = params.prefer_prompt_cache;

    let min_best_effort_adequacy = live_config.min_band();
    let concurrency_slots =
        if params.concurrency_slots.is_some() || params.preferred_capacity_key.is_some() {
            Some(
                ConcurrencySlots::new(params.concurrency_slots.clone().unwrap_or_default())
                    .with_preferred_capacity(params.preferred_capacity_key.clone()),
            )
        } else {
            None
        };

    let pass = select_policy::SelectionPass {
        consumer: consumer_id,
        target_agent: &target_agent,
        requirements,
        required_capabilities,
        facts: &facts,
        snapshot: &snapshot,
        exclude: &exclude,
        operator_exclude: &operator_exclude,
        consumer_exclude,
        subscription_keys: &subscription_keys,
        priced_per_token,
        rejections: &mapped_availability.rejected,
        config: &routing_config,
        slots: concurrency_slots.as_ref(),
        min_band: min_best_effort_adequacy,
        prefer_prompt_cache,
        providers_without_prompt_cache: &providers_without_prompt_cache,
        operator_config: &operator_config,
    };

    let admission_pin = params
        .pin
        .as_ref()
        .and_then(|pin| pin.resolve(|slot| operator_config.slot_pin(slot).cloned()));
    let mut pin_rejected = None;
    if let Some(pin) = params
        .model_pin
        .as_deref()
        .or_else(|| admission_pin.as_ref().map(|pin| pin.model.as_str()))
    {
        let variant = admission_pin
            .as_ref()
            .and_then(|pin| pin.variant.clone())
            .or_else(|| work_model_pin::named_variant(pin, &facts));
        let (pin_key, pin_capacity) = work_model_pin::route_keys(pin, variant.as_deref(), &facts);
        let pin_snapshot = snapshot.clone();
        let mut pin_availability = mapped_availability.rejected.clone();
        // Capture the timed route wall's expiry so a caller forbidden to fall
        // back can wait until the pinned route becomes admissible again.
        let pin_wall_until = prefrontal_routing_core::model_router::route_wall_until(
            &pin_snapshot,
            &pin_key,
            &pin_capacity,
        );
        let mut pin_available = available.clone();
        let allowed = operator_config.route_allowed(consumer_id, &pin_key, true)
            && operator_config.route_allowed(consumer_id, &pin_capacity, true);
        if !allowed {
            pin_availability.insert(pin_key.clone(), format!("not_for excludes {consumer_id}"));
        }
        // A host-run pin is judged on the same list as the host's automatic
        // selections; it is not added to that list.
        if !host_run && allowed {
            if let Some(detail) = harness_context.rejection(&pin_key, &pin_capacity) {
                pin_availability.insert(pin_key.clone(), detail);
            } else {
                pin_available.insert(pin_key.clone());
            }
        }
        if let Some(detail) = candidate_filters
            .get(&pin_key)
            .or_else(|| candidate_filters.get(&pin_capacity))
        {
            pin_availability.insert(pin_key.clone(), detail.clone());
        }
        let pin_harness = run_harness(&pin_key);
        let runner_pin_refusal = if host_run {
            host_runner_pin_refusal(
                &catalog_targets,
                if deferred_runner_pin {
                    Some(legacy_wire::BROCA)
                } else {
                    params.substrate.as_deref()
                },
                "this request names substrate",
                [pin],
            )
        } else {
            let mapped = harness_context.mapped(&pin_key);
            host_runner_pin_refusal(
                &catalog_targets,
                Some(mapped.as_str()),
                "this route maps to harness",
                [pin],
            )
            .filter(|_| {
                params.executor.is_none()
                    && params
                        .required_harness
                        .is_none_or(|required| required == mapped)
            })
        };
        if let Some(refusal) = runner_pin_refusal {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                PROVIDER_REQUIRES_RUNNER_PIN,
                &refusal,
            )
            .await;
        }
        add_stage_ms(&timing.policy_ms, policy_started.elapsed());
        let selection_started = Instant::now();
        let pin_credential_refusal = pin_snapshot
            .by_capacity
            .get(&pin_capacity)
            .and_then(|state| state.credential_refusal.clone());
        let selection = {
            let input = select_policy::SelectionPass {
                prefer_prompt_cache: false,
                ..pass
            }
            .input(&pin_available, pin_snapshot);
            let mut input = Some(input);
            pass.select(&pin_available, Some(pin), None, None, |_| {
                let input = input.take().expect("a terminal pin is admitted once");
                if let Some(pin) = &admission_pin {
                    select_policy::select_pin_with_availability(
                        pin,
                        input,
                        &routing_config,
                        concurrency_slots.as_ref(),
                        &pin_availability,
                    )
                    .map_err(|code| work_model_pin::Refusal {
                        message: code.clone(),
                        code,
                    })
                } else {
                    work_model_pin::select_with_availability(
                        pin,
                        input,
                        &routing_config,
                        concurrency_slots.as_ref(),
                        config_overrides::reasoning_catalog_for(operator_config_path).as_ref(),
                        &catalog_targets,
                        &pin_availability,
                    )
                }
            })
            .map(|(selected, _)| selected)
            .map_err(|error| match error {
                select_policy::OrderFailure::Pin(refusal) => refusal,
                select_policy::OrderFailure::Score(_) => unreachable!("a request pin is terminal"),
            })
        };
        let selection = selection.map(|mut selected| {
            let explicit = admission_pin.as_ref().map_or_else(
                || work_model_pin::named_variant(pin, &facts),
                |pin| pin.variant.clone(),
            );
            let choice = apply_variant_policy(
                &mut selected,
                &operator_config,
                operator_config_path,
                &catalog_targets,
                params.consumer,
                explicit.as_deref(),
            );
            (selected, choice)
        });
        let selection = selection.and_then(|(selected, variant_choice)| {
            let policy = prefrontal_routing_core::accounts::AllocatorPolicy {
                model_pools: live_config.quota_model_pools(),
                ..Default::default()
            };
            let ctx = account_selection::Ctx {
                store: store.as_ref(),
                accounts: ACCOUNT_QUOTA.get(),
                flags: live_config.account_flags(),
                policy: &policy,
                now_ms: now as i64,
                classes: classes.clone(),
            };
            // Explicit catalog-less providers lease API keys through key_pool;
            // its own admission must preserve the pool's typed refusal.
            if !host_run && !catalog_targets.contains_key(&selected.model.provider_id) {
                account_selection::preflight_model_for_class(
                    &ctx,
                    &selected.model.provider_id,
                    &selected.model.model_id,
                    priced_per_token,
                )
                .map_err(|(code, message)| work_model_pin::Refusal { code, message })?;
            }
            // Resolve the Broca launch target and its credential mapping
            // before recording a decision; an unusable mapping rejects the pin.
            // A deferred runner pin leases nothing until core completes it.
            let runner =
                if pin_harness == Some(substrate_policy::Harness::Broca) && !deferred_runner_pin {
                    let session = pool_session(
                        params.task_id.as_deref(),
                        params.send_id.as_deref(),
                        &format!("route.select:{}", now as i64),
                    );
                    Some(
                        runner_for_route(
                            &catalog_targets,
                            broca_credentials,
                            store.as_ref(),
                            &session,
                            &selected.model.provider_id,
                            &selected.model.model_id,
                            selected.prompt_params.variant.as_deref(),
                            classes.as_ref(),
                            priced_per_token,
                        )
                        .map_err(|(code, message)| work_model_pin::Refusal { code, message })?,
                    )
                } else {
                    None
                };
            Ok((selected, runner, variant_choice))
        });
        timing.record_selection(selection_started.elapsed());
        policy_started = Instant::now();
        match selection {
            Ok((mut selected, runner, variant_choice)) => {
                let Some(module) = runner_module(&selected) else {
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "no_route",
                        "no configured runner can run the pinned route",
                    )
                    .await;
                };
                let ladder_started = Instant::now();
                record_ladder(pin_harness);
                add_stage_ms(&timing.ladder_ms, ladder_started.elapsed());
                if let Some(line) = &executor_workload {
                    selected.explanation.push(line.clone());
                }
                let write_started = Instant::now();
                let mut decision = decision_input_for(
                    &target_agent,
                    &requirements,
                    Some(&selected),
                    None,
                    &params,
                    Some(&provenance),
                    Some(&policy_sources_line(&operator_policy)),
                );
                decision.allowed_account_class =
                    prefrontal_routing_core::accounts::AllowedAccountClass::from_pay_per_token(
                        priced_per_token,
                    );
                let decision_id = store
                    .record_decision_with_selection(
                        &decision,
                        if host_run {
                            pin_harness.map(substrate_policy::Harness::as_str)
                        } else {
                            Some(&module)
                        },
                        selected_workload,
                    )
                    .map_err(|error| ModuleError::Message(error.to_string()))?;
                if pin_harness == Some(substrate_policy::Harness::Broca) {
                    remember_provider_identity(store, &decision_id, &selected, broca_credentials)?;
                }
                add_stage_ms(&timing.decision_write_ms, write_started.elapsed());
                let finish_started = Instant::now();
                let mut result = json!({
                    "selected": selected, "decisionID": decision_id,
                    "resolvedInput": {"requirements": requirements, "availableModels": resolved_request.policy.available_models, "subscriptionKeys": resolved_request.policy.subscription_keys, "requiredCapabilities": required_capabilities},
                    "variantSelection": variant_choice,
                });
                if let Some(harness) = pin_harness {
                    result["selected"]["harness"] = json!(harness);
                }
                if params.pin.is_some() {
                    result["pin_rejected"] = serde_json::Value::Null;
                }
                if deferred_runner_pin {
                    result["requiredSubstrate"] = json!(legacy_wire::BROCA);
                }
                if let Some((runner, grant)) = runner {
                    result["runner"] = json!(runner);
                    if let Some(grant) = grant {
                        result["poolLease"] = grant.lease_json();
                    }
                }
                if host_run && !deferred_runner_pin {
                    result["runner"] = host_runner(&params, &selected);
                }
                if !deferred_runner_pin {
                    complete_legacy_selection(&mut result, &selected, &facts, &module);
                }
                let body =
                    serde_json::to_vec(&json!({"result": result})).map_err(ModuleError::Json)?;
                let response = Frame::build_with_version(
                    ver,
                    FrameType::Response,
                    Flags::new(false, Priority::Interactive, false),
                    channel,
                    epoch,
                    corr,
                    body,
                )
                .map_err(|error| ModuleError::Message(error.to_string()))?;
                add_stage_ms(&timing.finish_ms, finish_started.elapsed());
                let reply_started = Instant::now();
                let reply = send(writer, response).await;
                timing.record_reply(reply_started.elapsed());
                return reply;
            }
            Err(mut refusal) => {
                if let Some(detail) = pin_availability.get(&pin_key) {
                    if !refusal.message.contains(detail) {
                        refusal.message = format!("{} ({detail})", refusal.message);
                    }
                }
                if params.pin.is_some() {
                    let timed = matches!(
                        refusal.code.as_str(),
                        "cooldown_active" | "quota_exhausted" | "backoff_active"
                    );
                    pin_rejected = Some(refusal.code);
                    if params.pin_fallback == select_policy::PinFallback::None {
                        let mut result = json!({
                            "selected": null, "decisionID": null, "pin_rejected": pin_rejected,
                        });
                        if let Some(until) = pin_wall_until.filter(|_| timed) {
                            result["pin_rejected_until_ms"] = json!(until as i64);
                        }
                        let body = serde_json::to_vec(&json!({ "result": result }))
                            .map_err(ModuleError::Json)?;
                        let response = Frame::build_with_version(
                            ver,
                            FrameType::Response,
                            Flags::new(false, Priority::Interactive, false),
                            channel,
                            epoch,
                            corr,
                            body,
                        )
                        .map_err(|error| ModuleError::Message(error.to_string()))?;
                        return send(writer, response).await;
                    }
                } else {
                    let detail = pin_credential_refusal.as_ref()
                        .filter(|mark| mark.reason == refusal.code)
                        .map(|mark| json!({"reason": mark.reason, "credentialIds": mark.credential_ids}));
                    if !host_run
                        && matches!(
                            refusal.code.as_str(),
                            "not_available" | "no_eligible_account"
                        )
                    {
                        refusal.code = "pinned_route_refused".into();
                    }
                    return send_route_error_body(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        ErrorBody {
                            code: refusal.code,
                            message: refusal.message,
                            detail,
                        },
                    )
                    .await;
                }
            }
        }
    }

    // A project pin is a one-entry ladder and beats the consumer mode. Legacy
    // bodies carry no affinity, so each one is fresh. A request pin was already
    // admitted above.
    let project_pin = route_scope(channel, epoch).and_then(|scope| {
        scope
            .project_id
            .as_deref()
            .and_then(|project| operator_config.project_pin(project, consumer_id))
            .map(str::to_owned)
    });
    let attempt = |snap: ModelRouteStateSnapshot| -> Result<SelectedModelRoute, NoRouteError> {
        let pass = select_policy::SelectionPass {
            snapshot: &snap,
            ..pass
        };
        pass.select(&available, None, project_pin.as_deref(), None, |pin| {
            let allowed = available
                .iter()
                .filter(|key| operator_config.route_allowed(consumer_id, key, true))
                .cloned()
                .collect();
            work_model_pin::select_with_availability(
                pin,
                pass.input(&allowed, snap.clone()),
                &routing_config,
                concurrency_slots.as_ref(),
                config_overrides::reasoning_catalog_for(operator_config_path).as_ref(),
                &catalog_targets,
                &mapped_availability.rejected,
            )
            .map_err(|refusal| {
                let mut error = pass.score(&HashSet::new()).unwrap_err();
                error.message = refusal.message;
                error
            })
        })
        .map(|(selected, _)| selected)
        .map_err(|error| match error {
            select_policy::OrderFailure::Pin(error) | select_policy::OrderFailure::Score(error) => {
                error
            }
        })
    };

    // First attempt against the live snapshot.
    add_stage_ms(&timing.policy_ms, policy_started.elapsed());
    let selection_started = Instant::now();
    let selection = match attempt(snapshot.clone()) {
        Ok(selected) => Ok(selected),
        Err(no_route) if no_route.reason == RouteFailureReason::QuotaOrCooldownExhausted => {
            // Break-glass: in a subscription environment "all routes exhausted" is a
            // common steady state and stalls the orchestrator. Retry ONCE against a
            // snapshot that clears the single earliest-reset route.
            let hard_rejected_keys: HashSet<&str> = no_route
                .rejected
                .iter()
                .filter(|rejected| {
                    matches!(
                        rejected.reason.as_str(),
                        "quota_exhausted" | "cooldown_active"
                    )
                })
                .map(|rejected| rejected.route_key.as_str())
                .collect();
            let rejected_candidates: Vec<BreakGlassCandidate> = facts
                .iter()
                .map(fact_to_route)
                .filter(|route| hard_rejected_keys.contains(route.route_key.as_str()))
                // Do not select a rejected pin by temporarily clearing its quota
                // wall during the fallback retry.
                .filter(|route| {
                    !admission_pin.as_ref().is_some_and(|pin| {
                        pin_rejected.is_some() && route.capacity_key == pin.model
                    })
                })
                .map(|route| BreakGlassCandidate {
                    route_key: route.route_key,
                    capacity_key: route.capacity_key,
                })
                .collect();
            match relax_earliest_reset(&snapshot, &rejected_candidates) {
                Some(relaxed) => match attempt(relaxed.snapshot) {
                    Ok(mut selected) => {
                        let reset_label = relaxed.resets_at.map_or_else(
                            || "unknown".to_string(),
                            |resets_at| (resets_at as i64).to_string(),
                        );
                        selected.explanation.insert(
                            0,
                            format!(
                                "break-glass: all routes quota-exhausted; relaxed earliest-reset route {} (resets {})",
                                relaxed.route_key, reset_label
                            ),
                        );
                        Ok(selected)
                    }
                    Err(_) => Err(no_route),
                },
                None => Err(no_route),
            }
        }
        Err(no_route) => Err(no_route),
    };
    timing.record_selection(selection_started.elapsed());
    let finish_started = Instant::now();

    // Record the decision, then respond. The decisionID is returned ONLY after
    // record_decision commits (it is a single store txn that either commits or
    // errors), so a returned id always maps to a committed row; never an id whose
    // write then failed (the contract requirement). A failed selection records a
    // no-selection row and surfaces the typed routing failure.
    let policy_sources = policy_sources_line(&operator_policy);
    match selection {
        Ok(mut selected) => {
            let Some(module) = runner_module(&selected) else {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "no_route",
                    "no configured runner can run the selected route",
                )
                .await;
            };
            let selected_harness = run_harness(&selected.route_key);
            add_stage_ms(&timing.finish_ms, finish_started.elapsed());
            let ladder_started = Instant::now();
            record_ladder(selected_harness);
            add_stage_ms(&timing.ladder_ms, ladder_started.elapsed());
            let finish_started = Instant::now();
            let variant_choice = apply_variant_policy(
                &mut selected,
                &operator_config,
                operator_config_path,
                &catalog_targets,
                params.consumer,
                project_pin
                    .as_deref()
                    .and_then(|pin| cortexkit_role_routing::select::RouteRef::parse_pin(pin))
                    .and_then(|pin| pin.variant)
                    .as_deref(),
            );
            selected.explanation.push(policy_sources.clone());
            if let Some(line) = &executor_workload {
                selected.explanation.push(line.clone());
            }
            if let Some(note) = &quota_note {
                selected.explanation.push(note.clone());
            }
            if let Some(note) = availability.note.as_ref().filter(|_| {
                selected_harness == Some(substrate_policy::Harness::Broca)
                    && (params.executor.is_none()
                        || is_broca_substrate(params.substrate.as_deref()))
            }) {
                selected.explanation.push(note.clone());
            }
            selected
                .explanation
                .extend(head_refusals.notes.iter().cloned());
            add_stage_ms(&timing.finish_ms, finish_started.elapsed());
            let write_started = Instant::now();
            let mut decision = decision_input_for(
                &target_agent,
                &requirements,
                Some(&selected),
                None,
                &params,
                Some(&provenance),
                Some(&policy_sources),
            );
            decision.allowed_account_class =
                prefrontal_routing_core::accounts::AllowedAccountClass::from_pay_per_token(
                    priced_per_token,
                );
            let recorded = store.record_decision_with_selection(
                &decision,
                if host_run {
                    selected_harness.map(substrate_policy::Harness::as_str)
                } else {
                    Some(&module)
                },
                selected_workload,
            );
            add_stage_ms(&timing.decision_write_ms, write_started.elapsed());
            match recorded {
                Ok(decision_id) => {
                    if selected_harness == Some(substrate_policy::Harness::Broca) {
                        // The decision row has committed, so the selection stands
                        // without the identity. Failing the call here would make
                        // the caller retry and record a second decision for one
                        // launch. Without the identity, the outcome for this
                        // decision is recorded the same way as for a route with
                        // several serving credentials, where none is remembered.
                        if let Err(error) = remember_provider_identity(
                            store,
                            &decision_id,
                            &selected,
                            broca_credentials,
                        ) {
                            warn_operator(format!(
                                "[prefrontal-routing] WARN: decision {decision_id} for {} has no remembered serving identity: {error}",
                                selected.route_key
                            ));
                        }
                    }
                    let finish_started = Instant::now();
                    let mut resolved_input = json!({
                        "requirements": requirements,
                        "availableModels": &resolved_request.policy.available_models,
                        "subscriptionKeys": &resolved_request.policy.subscription_keys,
                    });
                    if let Some(required_capabilities) = required_capabilities {
                        resolved_input["requiredCapabilities"] = json!(required_capabilities);
                    }
                    let mut result = json!({
                        "selected": selected,
                        "decisionID": decision_id,
                        "resolvedInput": resolved_input,
                        "variantSelection": variant_choice,
                        "billingPlanes": facts.iter().filter(|fact| {
                            let route = fact_to_route(fact);
                            available.contains(&route.route_key) || available.contains(&route.capacity_key)
                        }).filter_map(|fact| fact.billing_plane.as_ref().map(|plane| json!({
                            "providerID": fact.provider_id,
                            "modelID": fact.model_id,
                            "authMethod": plane.auth_method,
                            "kind": plane.kind,
                            "servingMethod": plane.selection_reason,
                            "dimensionRates": prefrontal_routing_core::BasketDimension::ALL.into_iter().map(|dimension| {
                                (dimension.fact_key().to_string(), fact.basket_rate(dimension))
                            }).collect::<std::collections::BTreeMap<_, _>>(),
                        }))).collect::<Vec<_>>(),
                    });
                    if let Some(harness) = selected_harness {
                        result["selected"]["harness"] = json!(harness);
                    }
                    if params.pin.is_some() {
                        result["pin_rejected"] = json!(pin_rejected);
                    }
                    if host_run {
                        result["runner"] = host_runner(&params, &selected);
                    }
                    add_stage_ms(&timing.finish_ms, finish_started.elapsed());
                    let runner_started = Instant::now();
                    if selected_harness == Some(substrate_policy::Harness::Broca) {
                        // An automatically chosen key-pool provider leases its key
                        // here; any other provider resolves to the ordinary
                        // translation. Going through runner_for_route keeps one
                        // rule for every launch target.
                        let session = pool_session(
                            params.task_id.as_deref(),
                            params.send_id.as_deref(),
                            &decision_id,
                        );
                        match runner_for_route(
                            &catalog_targets,
                            broca_credentials,
                            store.as_ref(),
                            &session,
                            &selected.model.provider_id,
                            &selected.model.model_id,
                            selected.prompt_params.variant.as_deref(),
                            classes.as_ref(),
                            priced_per_token,
                        ) {
                            Ok((runner, _)) => result["runner"] = json!(runner),
                            Err((code, message)) => {
                                return send_route_error(
                                    writer, ver, channel, epoch, corr, &code, &message,
                                )
                                .await;
                            }
                        }
                    }
                    add_stage_ms(&timing.runner_ms, runner_started.elapsed());
                    complete_legacy_selection(&mut result, &selected, &facts, &module);
                    let finish_started = Instant::now();
                    let body = serde_json::to_vec(&json!({ "result": result }))
                        .map_err(ModuleError::Json)?;
                    let response = Frame::build_with_version(
                        ver,
                        FrameType::Response,
                        Flags::new(false, Priority::Interactive, false),
                        channel,
                        epoch,
                        corr,
                        body,
                    )
                    .map_err(|e| ModuleError::Message(e.to_string()))?;
                    add_stage_ms(&timing.finish_ms, finish_started.elapsed());
                    let reply_started = Instant::now();
                    let reply = send(writer, response).await;
                    timing.record_reply(reply_started.elapsed());
                    reply
                }
                Err(e) => {
                    send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "store_error",
                        &format!("recording decision: {e}"),
                    )
                    .await
                }
            }
        }
        Err(mut no_route) => {
            // Head-specific exclusions and non-relaxable billing walls explain the
            // same no-route result. Keep them on one message path so callers receive
            // the full reason rather than a second, competing error surface.
            let rejected_keys: HashSet<&str> = no_route
                .rejected
                .iter()
                .filter(|rejected| {
                    matches!(
                        rejected.reason.as_str(),
                        "quota_exhausted" | "cooldown_active"
                    )
                })
                .map(|rejected| rejected.route_key.as_str())
                .collect();
            let mut no_route_notes = head_refusals.notes.clone();
            if no_route
                .rejected
                .iter()
                .any(|route| route.reason == "excluded_by_operator")
            {
                no_route_notes.push("routes excluded by operator policy".into());
            }
            no_route_notes.extend(availability.note.iter().cloned());
            no_route_notes.extend(facts.iter().filter_map(|fact| {
                let route = fact_to_route(fact);
                if !rejected_keys.contains(route.route_key.as_str()) {
                    return None;
                }
                let candidate = BreakGlassCandidate {
                    route_key: route.route_key.clone(),
                    capacity_key: route.capacity_key,
                };
                live_billing_wall_error(&snapshot, &candidate).map(|error| {
                    format!(
                        "billing wall on route {} (provider {}): {}",
                        route.route_key, fact.provider_id, error
                    )
                })
            }));
            if !no_route_notes.is_empty() {
                no_route.message = format!("{} ({})", no_route.message, no_route_notes.join("; "));
            }
            add_stage_ms(&timing.finish_ms, finish_started.elapsed());
            let write_started = Instant::now();
            // Best-effort failed-decision audit row: a store error is logged, and
            // the routing failure is still the reply.
            let decision = decision_input_for(
                &target_agent,
                &requirements,
                None,
                Some(&no_route),
                &params,
                Some(&provenance),
                Some(&policy_sources),
            );
            record_failed_selection_decision(store.as_ref(), &decision);
            add_stage_ms(&timing.decision_write_ms, write_started.elapsed());
            let reply_started = Instant::now();
            let reply = send_no_route(
                writer,
                ver,
                channel,
                epoch,
                corr,
                &no_route,
                pin_rejected.as_deref(),
            )
            .await;
            timing.record_reply(reply_started.elapsed());
            reply
        }
    }
}

fn host_runner(params: &SelectParams, selected: &SelectedModelRoute) -> serde_json::Value {
    let module = params
        .harness
        .as_deref()
        .or(params.substrate.as_deref())
        .unwrap_or("host");
    json!(cortexkit_role_routing::select::RunnerRef::new(
        module,
        cortexkit_role_routing::select::RouteRef {
            provider: selected.model.provider_id.clone(),
            model: selected.model.model_id.clone(),
            variant: selected.prompt_params.variant.clone(),
        },
    ))
}

/// Add the common decision envelope without removing legacy launch fields
/// (notably credential pins and pool leases) that existing callers consume.
fn complete_legacy_selection(
    result: &mut serde_json::Value,
    selected: &SelectedModelRoute,
    facts: &[ModelFact],
    module: &str,
) {
    let route = cortexkit_role_routing::select::RouteRef {
        provider: selected.model.provider_id.clone(),
        model: selected.model.model_id.clone(),
        variant: selected.prompt_params.variant.clone(),
    };
    let fact = facts
        .iter()
        .filter(|fact| fact.provider_id == route.provider && fact.model_id == route.model)
        .min_by_key(|fact| u8::from(fact.variant != route.variant));
    if !result["runner"].is_object() {
        result["runner"] = json!({});
    }
    result["runner"]["module"] = json!(module);
    result["runner"]["route"] = json!(route);
    result["route"] = json!(route);
    result["decision_id"] = result["decisionID"].clone();
    result["context_window"] = json!(fact
        .and_then(|fact| fact.context_window)
        .map_or(0, |window| window as u64));
    result["max_output_tokens"] = json!(fact.and_then(|fact| fact.max_output_tokens).unwrap_or(0));
    result["expires_at_ms"] = json!(now_ms() + 10 * 60_000);
}

/// Panel legs use the substrate that sends. Claustrum's complete mappings
/// restrict Broca to credentialed providers; incomplete/stale listings fall
/// back to the served catalog, just like single-route Broca selection.
pub(super) fn panel_availability(
    requested_substrate: Option<&str>,
    host_available_models: HashSet<String>,
    facts: &[ModelFact],
    broca_credentials: &SharedBrocaCredentials,
) -> (AvailabilitySubstrate, HashSet<String>) {
    if !is_broca_substrate(requested_substrate) {
        return (AvailabilitySubstrate::Host, host_available_models);
    }
    let (available, fallback) = broca_served_routes(facts, broca_credentials);
    let substrate = if fallback.is_some() {
        AvailabilitySubstrate::BrocaServedCatalog
    } else {
        AvailabilitySubstrate::Broca
    };
    (substrate, available.into_iter().collect())
}

/// Refusal code for a pinned route whose provider Broca reaches only through a
/// per-send credential pin, requested for a substrate that cannot carry one.
const PROVIDER_REQUIRES_RUNNER_PIN: &str = "provider_requires_runner_pin";

pub(crate) fn needs_runner_pin(
    catalog_targets: &std::collections::BTreeMap<String, String>,
    provider: &str,
) -> bool {
    key_pool::is_pool_provider(catalog_targets, provider)
}

/// The Broca launch target for a selected route: the plain translation for an
/// ordinary provider. For a key-pool provider (one with no catalog rows that
/// resolved through its credentials' `serves` vendor) it leases a key for
/// `session` from the pool and returns the target that pins it, with the grant;
/// a pool that cannot be resolved or has no admissible key refuses by name.
fn runner_for_route(
    catalog_targets: &std::collections::BTreeMap<String, String>,
    credentials: &SharedBrocaCredentials,
    store: &SqliteRouteStateStore,
    session: &str,
    provider: &str,
    model: &str,
    variant: Option<&str>,
    classes: Option<&account_selection::ClassContext>,
    priced_per_token: bool,
) -> Result<
    (
        prefrontal_routing_core::runner::RunnerModel,
        Option<key_pool::PoolGrant>,
    ),
    (String, String),
> {
    if !needs_runner_pin(catalog_targets, provider) {
        if let Some(classes) =
            classes.filter(|classes| classes.needs_credential_pin(provider, model))
        {
            let id = classes.launch_credential(provider, model, priced_per_token)?;
            if let Some(runner) =
                prefrontal_routing_core::runner::RunnerModel::pinned(&id, model, variant)
            {
                return Ok((runner, None));
            }
        }
        return Ok((
            prefrontal_routing_core::runner::RunnerModel::broca(provider, model, variant),
            None,
        ));
    }
    let grant = key_pool::acquire(
        &key_pool::PoolCtx {
            store,
            credentials,
            catalog_targets,
            now_ms: now_ms() as i64,
        },
        provider,
        model,
        variant,
        session,
    )
    .map_err(|refusal| (refusal.code, refusal.message))?;
    Ok((grant.runner.clone(), Some(grant)))
}

/// The session a selection's pool lease belongs to: the caller's task, else its
/// send, else this decision alone (its lease then simply expires).
fn pool_session(task_id: Option<&str>, send_id: Option<&str>, fallback: &str) -> String {
    task_id
        .or(send_id)
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

/// A pinned route on a provider that needs a per-send credential pin can only
/// run on Broca: the host plugins send a model with no auth selection, so the
/// send would silently land on the provider's default logins. Refuse such pins
/// by name for every other mapped harness.
fn host_runner_pin_refusal<'a>(
    catalog_targets: &std::collections::BTreeMap<String, String>,
    requested_substrate: Option<&str>,
    substrate_source: &str,
    pinned_routes: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    if is_broca_substrate(requested_substrate) {
        return None;
    }
    pinned_routes.into_iter().find_map(|route| {
        let provider = route.split_once('/').map_or(route, |(provider, _)| provider);
        needs_runner_pin(catalog_targets, provider).then(|| {
            format!(
                "{route}: provider {provider} needs a per-send credential pin that only the {} runner carries, but {substrate_source} {}",
                runner_title(legacy_wire::BROCA),
                requested_substrate.unwrap_or("none")
            )
        })
    })
}

/// A runner's name as prose capitalizes it: `broca` reads as `Broca`.
fn runner_title(runner: &str) -> String {
    let mut chars = runner.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

fn is_broca_substrate(requested_substrate: Option<&str>) -> bool {
    requested_substrate.map(str::trim) == Some(legacy_wire::BROCA)
}

/// The Broca routes a caller's model list admits: a route stays when the list
/// names its route key or its `provider/model` capacity key, the same match
/// selection uses for a host's list.
fn confine_to_caller_list(
    broca_models: &HashSet<String>,
    caller: &[String],
    facts: &[ModelFact],
) -> HashSet<String> {
    let caller: HashSet<&str> = caller.iter().map(String::as_str).collect();
    facts
        .iter()
        .filter_map(|fact| {
            let route_key =
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
            let capacity_key = make_capacity_key(&fact.provider_id, &fact.model_id);
            (broca_models.contains(&route_key)
                && (caller.contains(route_key.as_str()) || caller.contains(capacity_key.as_str())))
            .then_some(route_key)
        })
        .collect()
}

/// Read the cached metadata only; selection never waits on the vault.
fn broca_served_routes(
    facts: &[ModelFact],
    broca_credentials: &SharedBrocaCredentials,
) -> (Vec<String>, Option<String>) {
    broca_credentials
        .lock()
        .map(|credentials| credentials.served_routes(facts, now_ms()))
        .unwrap_or_else(|_| BrocaCredentials::default().served_routes(facts, now_ms()))
}

/// A substrate's served routes and the provenance of that availability list.
pub(super) struct SelectAvailability {
    pub(super) models: Vec<String>,
    pub(super) note: Option<String>,
}

impl SelectAvailability {
    fn host(models: Vec<String>) -> Self {
        Self { models, note: None }
    }
}

/// Availability for a `route.select` whose caller names the substrate that
/// will send.
///
/// A Broca send is judged against what Broca can reach now, not against the
/// list a caller carries: callers without a host (core's own classifier,
/// wake author, and comment reviewer) have no list of their own, and a host's
/// list describes a different substrate. (A host-run re-selection that carries
/// the head's list is then confined to it; see `confine_to_caller_list`.) If
/// the vault listing is unknown or
/// no credential has a provider mapping, selection proceeds over every route
/// the catalog still serves. Otherwise it uses only explicitly mapped providers.
/// It stays closed to routes known to be unserved, because withdrawn catalog
/// rows never reach `facts`, and operator exclusions, cooldowns, and failure
/// backoff still apply downstream.
pub(super) fn select_availability(
    requested_substrate: Option<&str>,
    host_available_models: Vec<String>,
    facts: &[ModelFact],
    broca_credentials: &SharedBrocaCredentials,
) -> SelectAvailability {
    if !is_broca_substrate(requested_substrate) {
        return SelectAvailability::host(host_available_models);
    }
    let (models, fallback) = broca_served_routes(facts, broca_credentials);
    let note = match fallback {
        Some(reason) => format!("availability unknown: {reason}; routed over all {} served catalog routes", models.len()),
        None => format!(
            "availability: {runner} ({} served catalog routes on providers {} holds a credential for)",
            models.len(),
            runner_title(legacy_wire::BROCA),
            runner = legacy_wire::BROCA,
        ),
    };
    SelectAvailability {
        models,
        note: Some(note),
    }
}

async fn handle_select_panel(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
    let live_config = config_overrides::live_config(routing_config_path);
    let operator_config = if operator_config_path == routing_config_path {
        Arc::clone(&live_config)
    } else {
        config_overrides::live_config(operator_config_path)
    };
    let operator_policy = operator_config.operator_policy();
    let params = match serde_json::from_slice::<SelectPanelBody>(&frame.body) {
        Ok(body) => body.params,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("route.select_panel params not decodable: {e}"),
            )
            .await;
        }
    };
    if params.selection == Some(PanelSelection::Roster) || !params.roster.is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "roster_removed",
            "route.select_panel rosters are removed; configure model_routing.panels instead",
        )
        .await;
    }
    let mut prepared = match prepare_selection_inputs(
        quota_cache,
        store,
        live_config.pricing(),
        &live_config.quota_model_pools(),
    ) {
        Ok(prepared) => prepared,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &e).await;
        }
    };
    let outcome = live_config.apply(&mut prepared.facts);
    let (routing_config, policy_revision) = (outcome.routing_config, outcome.policy_revision);
    let provenance = DecisionProvenance {
        quota_signal_json: prepared.quota_signal_json.clone(),
        policy_revision,
    };
    // Providers with no catalog rows borrow their catalog provider's rows;
    // the operator's subscription coverage, never the caller's
    // subscriptionKeys, prices and admits panel candidates.
    let catalog_targets = broca_credentials
        .lock()
        .map(|credentials| credentials.catalog_targets(&prepared.facts))
        .unwrap_or_default();
    prefrontal_routing_core::score_alias::append_alias_rows(&mut prepared.facts, &catalog_targets);
    if let Ok(credentials) = broca_credentials.lock() {
        credentials.price_facts(&mut prepared.facts, live_config.pricing());
    }
    live_config.apply(&mut prepared.facts);
    let panel_consumer =
        if params.panel_spec.class == prefrontal_routing_core::panel::PanelClass::Lab {
            select_policy::Consumer::Lab
        } else {
            select_policy::Consumer::Athena
        };
    let classes = account_selection::ClassContext::read(
        store,
        broca_credentials,
        live_config.clone(),
        ACCOUNT_QUOTA.get(),
        prepared.facts.clone(),
        now_ms() as i64,
    )
    .map_err(ModuleError::Message)?;
    classes.apply_eligibility(&mut prepared.snapshot, &prepared.facts, false, false);
    classes.apply_legacy_route_fences(&mut prepared.snapshot, &prepared.facts, false, false);
    warn_ignored_caller_policy_once(!params.subscription_keys.is_empty());
    let resolved_policy = resolve_routing_policy_with_catalog_targets(
        params.available_models.iter().cloned(),
        &ModelRoutingPolicyInput {
            subscription: operator_policy.subscription.clone(),
            ..ModelRoutingPolicyInput::default()
        },
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(None),
        &catalog_targets,
    );
    let subscription_keys: HashSet<String> =
        resolved_policy.subscription_keys.into_iter().collect();
    let (availability_substrate, available_models) = panel_availability(
        params.substrate.as_deref(),
        resolved_policy.available_models.into_iter().collect(),
        &prepared.facts,
        broca_credentials,
    );
    let mapped_availability = selection_harness::SelectionHarness {
        groups: operator_config.substrate_groups(),
        consumer: None,
        workload: Some(selection_harness::Workload::Athena),
        executor: true,
        required: None,
        caller: params.harness.as_deref(),
        host_models: &available_models,
        broca_models: &available_models,
        executor_models: &available_models,
    }
    .availability(&prepared.facts);
    if availability_substrate != AvailabilitySubstrate::Host {
        if let Ok(credentials) = broca_credentials.lock() {
            let served = credentials.statically_served_routes(&prepared.facts);
            let registry = routing_v1::runners::Registry::build(
                operator_config.v1_runners(),
                &credentials,
                &prepared.facts,
                prepared.now as u64,
            );
            let frontier = selection_harness::SelectionHarness {
                groups: operator_config.substrate_groups(),
                consumer: None,
                workload: Some(selection_harness::Workload::Athena),
                executor: true,
                required: None,
                caller: params.harness.as_deref(),
                host_models: &served,
                broca_models: &served,
                executor_models: &served,
            }
            .availability(&prepared.facts);
            let runners = registry.policies();
            for fact in &prepared.facts {
                let route = fact_to_route(fact);
                let can_run = runners.iter().any(|runner| {
                    registry.can_run(&runner.module, &route.route_key, &route.capacity_key)
                });
                let state = prepared
                    .snapshot
                    .by_route
                    .entry(route.route_key.clone())
                    .or_default();
                if !served.contains(&route.route_key) || !can_run {
                    state.statically_routable = Some(false);
                } else if frontier.models.contains(&route.route_key)
                    && (!mapped_availability.models.contains(&route.route_key)
                        || mapped_availability.rejected.contains_key(&route.route_key))
                {
                    state.statically_routable.get_or_insert(true);
                }
            }
        }
    }
    // Broca legs have no route.select of their own, so their availability is
    // recorded here for the winner ladder. Host-list panels are not recorded:
    // the host's ladder follows route.select, whose list is already fenced by
    // the operator's exclusions, and alternating two differently fenced lists
    // would read as an availability change on every other request.
    if availability_substrate != AvailabilitySubstrate::Host {
        winner_ladder::observed_selections().record(
            winner_ladder::LadderKey::new(
                winner_ladder::SUBSTRATE_BROCA,
                winner_ladder::CALLER_CLASS_PANEL,
            ),
            mapped_availability.models.iter().cloned(),
            subscription_keys.iter().cloned(),
            prepared.snapshot.clone(),
        );
    }
    let class = params.panel_spec.class;
    let groups = live_config.exclude_groups();
    let group = if class == prefrontal_routing_core::panel::PanelClass::Lab {
        &groups.lab
    } else {
        &groups.athena
    };
    let operator_exclude_keys =
        panel_policy::operator_exclusions(&prepared.facts, &operator_policy.exclude, group);
    let policy = panel_policy::class_policy(live_config.panels(), class);
    let canonical_ids = match panel_canonical_ids(store, &prepared.facts, &catalog_targets) {
        Ok(canonical_ids) => canonical_ids,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("reading canonical model ids: {e}"),
            )
            .await;
        }
    };
    let context = prefrontal_routing_core::panel::PanelSelectionContext {
        seed: params
            .seed
            .as_ref()
            .or(params.task_id.as_ref())
            .cloned()
            .unwrap_or_default(),
        pins: prefrontal_routing_core::panel::PanelPins {
            synthesizer: live_config.slot_pin("synthesizer").map(|pin| {
                prefrontal_routing_core::panel::PanelSynthesizerPin {
                    model: pin.model.clone(),
                    variant: pin.variant.clone(),
                }
            }),
        },
        canonical_ids,
    };
    // Seats are matched back to their catalog model to apply variant overrides;
    // the facts move into the selection below.
    let seat_models: HashMap<String, (String, String)> = if live_config.variant_groups().is_empty()
    {
        HashMap::new()
    } else {
        prepared
            .facts
            .iter()
            .map(|fact| {
                (
                    fact_to_route(fact).route_key,
                    (fact.provider_id.clone(), fact.model_id.clone()),
                )
            })
            .collect()
    };
    let mut availability_rejections = mapped_availability.rejected;
    let session_root = pool_session(
        params.task_id.as_deref(),
        params.send_id.as_deref(),
        &format!("route.select_panel:{}", now_ms()),
    );
    let panel = loop {
        let mut panel = match prefrontal_routing_core::panel::select_panel_with_availability(
            SelectPanelInput {
                package_tokens: params.package_tokens,
                panel_spec: params.panel_spec.selection_spec(),
                selection: PanelSelection::NPick,
                roster: Vec::new(),
                facts: prepared.facts.clone(),
                runtime_snapshot: Some(prepared.snapshot.clone()),
                exclude_route_keys: params.exclude_route_keys.clone(),
                operator_exclude_keys: operator_exclude_keys.clone(),
                required_capabilities: params.required_capabilities,
                available_models: mapped_availability.models.clone(),
                availability_substrate,
                availability_label: legacy_wire::availability_label(availability_substrate)
                    .to_string(),
                is_subscription_route: Some(Box::new(|route: &ModelRoute| {
                    subscription_keys.contains(&route.route_key)
                        || subscription_keys.contains(&route.capacity_key)
                })),
            },
            &routing_config,
            &policy,
            &context,
            &availability_rejections,
        ) {
            Ok(panel) => panel,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &e.0,
                )
                .await;
            }
        };
        // Every seat, the extra draws included, is an automatic choice for the
        // class's consumer: lab panels read `variants.lab`, every other class
        // `variants.athena`.
        apply_panel_variant_policy(
            &mut panel,
            &live_config,
            routing_config_path,
            &catalog_targets,
            panel_consumer,
            &seat_models,
        );
        // Vault-mapped providers need the selected credential and launch target in
        // promptParams.runner so core can persist and dispatch each member correctly.
        let mut leases = Vec::new();
        let mut refusal = None;
        if is_broca_substrate(params.substrate.as_deref()) {
            for (slot, member) in panel.members.iter_mut().enumerate() {
                let Some((provider, model)) = member.route_key.split_once('/') else {
                    continue;
                };
                if !needs_runner_pin(&catalog_targets, provider) {
                    continue;
                }
                let variant = member
                    .prompt_params
                    .get("variant")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string);
                // Each seat is its own session for the key pool: seats spread over
                // the keys and each keeps its own prompt-cache workspace.
                let session = format!("{session_root}#seat{slot}");
                match key_pool::acquire(
                    &key_pool::PoolCtx {
                        store: store.as_ref(),
                        credentials: broca_credentials,
                        catalog_targets: &catalog_targets,
                        now_ms: now_ms() as i64,
                    },
                    provider,
                    model,
                    variant.as_deref(),
                    &session,
                ) {
                    Ok(grant) => {
                        member
                            .prompt_params
                            .insert("runner".into(), json!(grant.runner));
                        leases.push((provider.to_string(), session, grant.lease_id.clone()));
                        member
                            .prompt_params
                            .insert("poolLease".into(), grant.lease_json());
                    }
                    Err(error) => {
                        refusal = Some((member.route_key.clone(), error));
                        break;
                    }
                }
            }
        }
        let Some((route_key, error)) = refusal else {
            break panel;
        };
        // A discarded panel must not keep leases for seats that may move or vanish
        // on reselection. Keep affinity so a surviving seat can reuse its workspace.
        for (provider, session, lease_id) in leases {
            if let Err(error) =
                store.pool_release(&provider, &session, Some(&lease_id), false, now_ms() as i64)
            {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &error.to_string(),
                )
                .await;
            }
        }
        if error.code != prefrontal_routing_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                &error.code,
                &error.message,
            )
            .await;
        }
        // Pool exhaustion is route-local unavailability for a panel, not a refusal
        // of the entire request. Rerun all class rules so sibling/family candidates
        // skipped for the failed seat can compete, with the blocked keys recorded.
        // Every model and variant on this provider draws from the same key pool.
        let detail = json!({
            "code": error.code, "message": error.message, "pool": error.detail,
        })
        .to_string();
        let provider = route_key
            .split_once('/')
            .expect("pool route has a provider")
            .0;
        for fact in prepared
            .facts
            .iter()
            .filter(|fact| fact.provider_id == provider)
        {
            let route = fact_to_route(fact);
            if (mapped_availability.models.contains(&route.route_key)
                || mapped_availability.models.contains(&route.capacity_key))
                && !availability_rejections.contains_key(&route.route_key)
            {
                let state = prepared
                    .snapshot
                    .by_route
                    .entry(route.route_key.clone())
                    .or_default();
                state.statically_routable.get_or_insert(true);
            }
            availability_rejections.insert(route.route_key, detail.clone());
        }
    };
    let decision = panel_decision_input(
        &params,
        &panel,
        availability_substrate,
        &provenance,
        &policy_sources_line(&operator_policy),
    );
    let decision_id =
        match store.record_decision_with_selection(&decision, Some(legacy_wire::BROCA), None) {
            Ok(id) => id,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("recording panel decision: {e}"),
                )
                .await;
            }
        };
    let mut result = json!({
        "members": &panel.members,
        "runnerUp": &panel.runner_up,
        "decisionID": decision_id,
    });
    for member in result["members"]
        .as_array_mut()
        .expect("panel members are an array")
    {
        member["harness"] = json!(legacy_wire::BROCA);
    }
    if let Some(shortfall) = &panel.shortfall {
        result
            .as_object_mut()
            .expect("panel result is an object")
            .insert("shortfall".into(), json!(shortfall));
    }
    if let Some(policy) = &panel.policy {
        result
            .as_object_mut()
            .expect("panel result is an object")
            .insert("policy".into(), json!(policy));
    }
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, response).await
}

/// Run a selected route at the variant the operator's overrides choose, and
/// record which rule chose it (and any override the catalog no longer
/// publishes) in the selection's explanation, which the decision log keeps.
pub(super) fn apply_variant_policy(
    selected: &mut SelectedModelRoute,
    config: &config_overrides::LiveConfig,
    config_path: &Path,
    catalog_targets: &std::collections::BTreeMap<String, String>,
    consumer: Option<select_policy::Consumer>,
    explicit: Option<&str>,
) -> variant_policy::VariantChoice {
    let groups = config.variant_groups();
    let catalog = if groups.is_empty() {
        None
    } else {
        config_overrides::reasoning_catalog_for(config_path)
    };
    let curated = selected.prompt_params.variant.clone();
    let choice = variant_policy::resolve(
        groups,
        catalog_targets,
        catalog.as_ref(),
        &variant_policy::VariantRequest {
            route_key: &selected.route_key,
            provider_id: &selected.model.provider_id,
            model_id: &selected.model.model_id,
            explicit,
            curated: curated.as_deref(),
            consumer,
        },
    );
    if choice.variant != curated {
        selected.prompt_params.variant = choice.variant.clone();
        // Core launches a run from promptParams; a curated default effort left
        // there would be sent beside the chosen variant and could contradict it,
        // so it is dropped, as it is for an explicit pin.
        selected.prompt_params.reasoning_effort = None;
    }
    // With no `model_routing.variants` entries the selection's explanation is
    // left unchanged; otherwise the variant lines are appended. The reply's
    // `variantSelection` names the rule either way.
    if !groups.is_empty() {
        selected
            .explanation
            .extend(choice.explanation_lines(&selected.route_key));
    }
    choice
}

pub(super) fn remember_provider_identity(
    store: &SharedStore,
    decision_id: &str,
    selected: &SelectedModelRoute,
    credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let facts = store
        .get_facts()
        .map_err(|e| ModuleError::Message(e.to_string()))?;
    if let Some(fact) = facts.iter().find(|f| {
        f.provider_id == selected.model.provider_id && f.model_id == selected.model.model_id
    }) {
        let identities = credentials
            .lock()
            .ok()
            .map(|c| c.serving_identities(fact))
            .unwrap_or_default();
        if let [(credential, account)] = identities.as_slice() {
            store
                .set_decision_provider_identity(
                    decision_id,
                    &prefrontal_routing_core::route_state_store::ProviderOutcome {
                        account_ref: account.clone().unwrap_or_else(|| credential.clone()),
                        credential_id: Some(credential.clone()),
                        resets_at_ms: None,
                        credential_refusal: None,
                    },
                )
                .map_err(|e| ModuleError::Message(e.to_string()))?;
        }
    }
    Ok(())
}

/// Apply the variant overrides to every seated member and runner-up of an
/// automatic panel, recording each seat's variant and rule beside its entry in
/// the policy's `seatedVariants`.
fn apply_panel_variant_policy(
    panel: &mut SelectedPanel,
    config: &config_overrides::LiveConfig,
    config_path: &Path,
    catalog_targets: &std::collections::BTreeMap<String, String>,
    consumer: select_policy::Consumer,
    seat_models: &HashMap<String, (String, String)>,
) {
    let groups = config.variant_groups();
    if groups.is_empty() {
        return;
    }
    let catalog = config_overrides::reasoning_catalog_for(config_path);
    let choose = |route_key: &str,
                  prompt_params: &mut serde_json::Map<String, serde_json::Value>| {
        let (provider_id, model_id) = seat_models.get(route_key).cloned().unwrap_or_else(|| {
            let (provider, model) = route_key.split_once('/').unwrap_or((route_key, ""));
            (provider.to_string(), model.to_string())
        });
        let curated = prompt_params
            .get("variant")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let choice = variant_policy::resolve(
            groups,
            catalog_targets,
            catalog.as_ref(),
            &variant_policy::VariantRequest {
                route_key,
                provider_id: &provider_id,
                model_id: &model_id,
                explicit: None,
                curated: curated.as_deref(),
                consumer: Some(consumer),
            },
        );
        if choice.variant != curated {
            match &choice.variant {
                Some(variant) => {
                    prompt_params.insert("variant".into(), json!(variant));
                }
                None => {
                    prompt_params.remove("variant");
                }
            }
            prompt_params.remove("reasoningEffort");
        }
        choice
    };
    let mut choices = Vec::with_capacity(panel.members.len());
    for member in &mut panel.members {
        choices.push(choose(&member.route_key, &mut member.prompt_params));
    }
    for runner_up in &mut panel.runner_up {
        choose(&runner_up.route_key, &mut runner_up.prompt_params);
    }
    if let Some(policy) = &mut panel.policy {
        for seated in &mut policy.seated_variants {
            let Some(choice) = choices.get(seated.slot) else {
                continue;
            };
            seated.reasoning_variant = choice.variant.clone();
            seated.reasoning_variant_source = Some(
                serde_json::to_value(choice.source)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_string))
                    .unwrap_or_default(),
            );
            seated.reasoning_variant_fallbacks = choice.fallbacks.clone();
        }
    }
}

pub(super) fn panel_decision_input(
    params: &SelectPanelParams,
    panel: &SelectedPanel,
    availability: AvailabilitySubstrate,
    provenance: &DecisionProvenance,
    policy_sources: &str,
) -> DecisionInput {
    let mut causes = panel
        .shortfall
        .as_ref()
        .map(|shortfall| shortfall.causes.clone())
        .unwrap_or_default();
    for cause in &panel.availability_rejections {
        if !causes.iter().any(|existing| existing.model == cause.model) {
            causes.push(cause.clone());
        }
    }
    let member_route_keys: Vec<&str> = panel
        .members
        .iter()
        .map(|member| member.route_key.as_str())
        .collect();
    let explanation = json!({
        "members": member_route_keys,
        "causes": causes,
        "selection": "class_policy",
        "policy": panel.policy,
        "policySources": policy_sources,
        "availability": legacy_wire::availability_label(availability),
        "workload": "workload: athena",
    });
    DecisionInput {
        consumer: Some(PANEL_DECISION_CONSUMER.into()),
        allowed_account_class: Default::default(),
        allowed_providers_json: None,
        decision_id: None,
        decision_kind: "panel".into(),
        task_id: params.task_id.clone(),
        parent_session_id: params.parent_session_id.clone(),
        harness: params.harness.as_deref().map(str::to_owned),
        project_root: params
            .project_root
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned),
        // A panel decision keeps no scope attribution: it names no single
        // caller route to read one from.
        project_id: None,
        workdir: None,
        send_id: params.send_id.clone(),
        target_agent: "athena-panel".into(),
        requirements_json: serde_json::to_string(&params.panel_spec)
            .unwrap_or_else(|_| "null".into()),
        selected_route_key: None,
        selected_provider_id: None,
        selected_model_id: None,
        selected_variant: None,
        score_json: serde_json::to_string(&json!({"candidates": panel.candidates})).unwrap(),
        rejected_json: serde_json::to_string(&causes).unwrap_or_else(|_| "[]".into()),
        explanation_json: Some(serde_json::to_string(&explanation).unwrap_or_else(|_| "{}".into())),
        quota_signal_json: Some(provenance.quota_signal_json.clone()),
        policy_revision: Some(provenance.policy_revision.clone()),
        anchor_version: ANCHOR_VERSION,
    }
}

/// Build the decision audit row from a selection or the refusal that prevented one.
pub(super) fn decision_input_for(
    target_agent: &str,
    requirements: &ModelRequirements,
    selected: Option<&SelectedModelRoute>,
    no_route: Option<&NoRouteError>,
    params: &SelectParams,
    provenance: Option<&DecisionProvenance>,
    policy_sources: Option<&str>,
) -> DecisionInput {
    let requirements_json = serde_json::to_string(requirements).unwrap_or_else(|_| "null".into());
    let quota_signal_json = provenance.map(|value| value.quota_signal_json.clone());
    let policy_revision = provenance.map(|value| value.policy_revision.clone());
    let consumer = variant_policy::consumer_group(
        params
            .consumer
            .or_else(|| select_policy::Consumer::from_id(target_agent)),
    )
    .map(str::to_string);
    match selected {
        Some(sel) => DecisionInput {
            allowed_account_class: Default::default(),
            allowed_providers_json: params
                .allowed_providers
                .as_ref()
                .map(|ids| serde_json::to_string(ids).unwrap()),
            consumer,
            decision_id: None,
            decision_kind: "single".into(),
            task_id: params.task_id.clone(),
            parent_session_id: params.parent_session_id.clone(),
            harness: params.harness.as_deref().map(str::to_owned),
            project_root: params
                .project_root
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned),
            project_id: params
                .scope
                .as_ref()
                .and_then(|scope| scope.project_id.clone()),
            workdir: params
                .scope
                .as_ref()
                .and_then(|scope| scope.workdir.clone()),
            send_id: params.send_id.clone(),
            target_agent: target_agent.to_string(),
            requirements_json,
            selected_route_key: Some(sel.route_key.clone()),
            selected_provider_id: Some(sel.model.provider_id.clone()),
            selected_model_id: Some(sel.model.model_id.clone()),
            selected_variant: sel.prompt_params.variant.clone(),
            score_json: serde_json::to_string(&sel.score).unwrap_or_else(|_| "null".into()),
            rejected_json: serde_json::to_string(&sel.rejected).unwrap_or_else(|_| "[]".into()),
            explanation_json: Some(
                serde_json::to_string(&sel.explanation).unwrap_or_else(|_| "[]".into()),
            ),
            quota_signal_json,
            policy_revision,
            anchor_version: ANCHOR_VERSION,
        },
        None => {
            let failure = no_route.expect("a refusal decision must preserve its routing failure");
            DecisionInput {
                allowed_account_class: Default::default(),
                allowed_providers_json: params
                    .allowed_providers
                    .as_ref()
                    .map(|ids| serde_json::to_string(ids).unwrap()),
                consumer,
                decision_id: None,
                decision_kind: "single".into(),
                task_id: params.task_id.clone(),
                parent_session_id: params.parent_session_id.clone(),
                harness: params.harness.as_deref().map(str::to_owned),
                project_root: params
                    .project_root
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned),
                project_id: params
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.project_id.clone()),
                workdir: params
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.workdir.clone()),
                send_id: params.send_id.clone(),
                target_agent: target_agent.to_string(),
                requirements_json,
                selected_route_key: None,
                selected_provider_id: None,
                selected_model_id: None,
                selected_variant: None,
                score_json: serde_json::to_string(&json!({"candidates": failure.candidates}))
                    .unwrap(),
                rejected_json: serde_json::to_string(&failure.rejected)
                    .unwrap_or_else(|_| "[]".into()),
                explanation_json: Some(
                    serde_json::to_string(&json!({
                        "failureReason": failure.reason.as_str(),
                        "message": failure.refusal_message(),
                        "bestRejected": failure.best_rejected.as_ref(),
                        "earliestReset": failure.earliest_reset,
                        "earliestResetRoute": failure.earliest_reset_route,
                        "blockingCauses": failure.blocking_causes(),
                        "policySources": policy_sources,
                        "workload": params.executor_workload_line(),
                    }))
                    .unwrap_or_else(|_| "{}".into()),
                ),
                quota_signal_json,
                policy_revision,
                anchor_version: ANCHOR_VERSION,
            }
        }
    }
}
