//! Route and panel selection, including policy inputs and decision provenance.

use super::*;

/// One refresh snapshot supplies both profiles and provider reporting metadata.
pub(crate) struct TaskProfileLookup {
    pub(crate) profiles: striatum_core::model_router::RouteTaskProfiles,
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

/// Metadata used to build a decision audit row, not a route.select wire body.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct SelectParams {
    pub(super) consumer: Option<select_policy::Consumer>,
    #[serde(
        rename = "allowed_providers",
        alias = "allowedProviders",
        deserialize_with = "select_policy::deserialize_allowed_providers"
    )]
    pub(super) allowed_providers: Option<Vec<String>>,
    pub(super) task_id: Option<String>,
    #[serde(alias = "parentSessionID")]
    pub(super) parent_session_id: Option<String>,
    #[serde(skip)]
    pub(super) scope: Option<DecisionScope>,
    pub(super) harness: Option<String>,
    #[serde(alias = "project_root")]
    pub(super) project_root: Option<String>,
    #[serde(rename = "sendID")]
    pub(super) send_id: Option<String>,
}

impl SelectParams {
    /// The params of a selection that carries none of the older request's
    /// audit members (task, session, harness, project root, send id): what a
    /// `routing/v1` decision records them as.
    pub(super) fn for_record() -> Self {
        Self::default()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct SelectPanelBody {
    pub(super) params: SelectPanelParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SelectPanelParams {
    #[serde(default, rename = "service_tier")]
    pub(super) service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    #[serde(default, rename = "workspace_root", alias = "workspaceRoot")]
    pub(super) workspace_root: Option<String>,
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
    #[serde(default, alias = "project_root")]
    pub(super) project_root: Option<String>,
    #[serde(default, rename = "sendID")]
    pub(super) send_id: Option<String>,
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
            "[striatum] ignoring caller-supplied model routing policy; configure model_routing in ~/.config/cortexkit/striatum.jsonc"
        );
    }
}

pub(super) struct PreparedSelectionInputs {
    pub(super) catalog_snapshot: Arc<striatum_store::CatalogFactsSnapshot>,
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
    pub(super) anchor_version: u32,
}

impl DecisionProvenance {
    /// Account eligibility is resolved after the shared quota cache. Persist the
    /// request-local walls too, rather than logging the pre-eligibility view.
    pub(super) fn with_account_snapshot(&self, snapshot: &ModelRouteStateSnapshot) -> Self {
        let mut result = self.clone();
        if let Ok(mut signal) = serde_json::from_str::<serde_json::Value>(&result.quota_signal_json)
        {
            signal["signals"] = json!(quota_signals(snapshot));
            result.quota_signal_json = signal.to_string();
        }
        result
    }
}

fn quota_signals(snapshot: &ModelRouteStateSnapshot) -> Vec<serde_json::Value> {
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
    signals
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
    let signals = quota_signals(snapshot);

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
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> Result<PreparedSelectionInputs, String> {
    prepare_selection_inputs_for_sweep(quota_cache, store, pricing, model_pools, None)
}

pub(super) fn prepare_selection_inputs_for_sweep(
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    pricing: &RoutePricing,
    model_pools: &[striatum_core::quota_extract::ModelPool],
    sweep: Option<&account_selection::Sweep>,
) -> Result<PreparedSelectionInputs, String> {
    let now = now_ms() as f64;
    let (catalog_snapshot, facts) = store
        .get_facts_with_observed_speed_and_generation(now, pricing)
        .map_err(|e| format!("reading facts from route-state store: {e}"))?;
    let mut snapshot = match sweep {
        Some(sweep) => sweep.route_snapshot(now),
        None => crate::account_state::published(store, now as i64)
            .and_then(|blocks| blocks.route_state_snapshot(now)),
    }
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
        catalog_snapshot,
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
    pub(super) operator_config: &'a Arc<config_overrides::LiveConfig>,
    pub(super) operator_config_path: &'a Path,
    /// Each provider with no catalog rows that resolved to a catalog provider
    /// through its credentials' `serves` vendor
    /// ([`BrocaCredentials::catalog_targets`]), read once for the selection.
    pub(super) catalog_targets: &'a std::collections::BTreeMap<String, String>,
}

/// What pricing a selection's inputs yields besides the priced facts and the
/// fenced snapshot.
pub(super) struct PricedSelection {
    pub(super) routing_config: striatum_core::RoutingConfig,
    pub(super) provenance: DecisionProvenance,
    /// Empty input for legacy candidate selection: account choice supplies
    /// all credential eligibility decisions.
    pub(super) entitlement_fences:
        Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    /// Providers known to publish a subscription plan. Host-only payment
    /// admission uses this list when it has no vault account class.
    /// `None` means the plan listing is not known yet.
    pub(super) subscription_plan_providers: Option<HashSet<String>>,
    pub(super) classes: Option<account_selection::ClassContext>,
}

/// Test shorthand for [`price_selection_inputs_with_catalog`]: reads a fresh
/// catalog snapshot and reserves probes, as a live selection does.
#[cfg(test)]
pub(super) fn price_selection_inputs(
    facts: &mut Vec<ModelFact>,
    quota_signal_json: String,
    sources: &SelectionPolicySources<'_>,
    store: &SharedStore,
    broca_credentials: &SharedBrocaCredentials,
    credentialed: bool,
    priced_per_token: bool,
) -> Result<PricedSelection, ModuleError> {
    price_selection_inputs_with_catalog(
        facts,
        quota_signal_json,
        sources,
        None,
        store,
        broca_credentials,
        credentialed,
        priced_per_token,
        true,
    )
}

/// Apply one selection's configuration to the facts and snapshot that
/// [`prepare_selection_inputs`] read: the live overlays, the rows borrowed by
/// providers with no catalog rows, the rates each credential kind is billed at
/// (Fusiform's `BillingPlane` rows), the
/// credential entitlement fences. Every route selection runs this same sequence, so a
/// route fenced for one caller's selection is fenced for every other.
///
/// `catalog_snapshot` is the catalog view the facts were read from, so the
/// account indexes are keyed to that same view; `None` reads a fresh one.
/// Evaluations pass private sweep accounts and `reserve_probes: false`:
/// they never claim a credential's next retry after a provider refusal.
#[allow(clippy::too_many_arguments)]
pub(super) fn price_selection_inputs_with_catalog(
    facts: &mut Vec<ModelFact>,
    quota_signal_json: String,
    sources: &SelectionPolicySources<'_>,
    catalog_snapshot: Option<&striatum_store::CatalogFactsSnapshot>,
    store: &SharedStore,
    broca_credentials: &SharedBrocaCredentials,
    credentialed: bool,
    priced_per_token: bool,
    reserve_probes: bool,
) -> Result<PricedSelection, ModuleError> {
    price_selection_inputs_for_sweep(
        facts,
        quota_signal_json,
        sources,
        catalog_snapshot,
        store,
        broca_credentials,
        credentialed,
        priced_per_token,
        reserve_probes,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn price_selection_inputs_for_sweep(
    facts: &mut Vec<ModelFact>,
    quota_signal_json: String,
    sources: &SelectionPolicySources<'_>,
    catalog_snapshot: Option<&striatum_store::CatalogFactsSnapshot>,
    store: &SharedStore,
    broca_credentials: &SharedBrocaCredentials,
    credentialed: bool,
    priced_per_token: bool,
    reserve_probes: bool,
    sweep: Option<&account_selection::Sweep>,
) -> Result<PricedSelection, ModuleError> {
    let mut classes = if credentialed {
        let snapshot;
        let catalog_snapshot = if let Some(snapshot) = catalog_snapshot {
            snapshot
        } else {
            snapshot = store
                .get_facts_with_generation()
                .map_err(|e| ModuleError::Message(e.to_string()))?;
            &snapshot
        };
        Some(
            account_selection::ClassContext::read_for_sweep(
                store,
                broca_credentials,
                Arc::clone(sources.operator_config),
                ACCOUNT_QUOTA.get(),
                facts.clone(),
                catalog_snapshot,
                now_ms() as i64,
                sweep,
            )
            .map_err(ModuleError::Message)?,
        )
    } else {
        None
    };
    if let Some(classes) = classes.as_mut().filter(|_| reserve_probes) {
        classes
            .reserve_selection_probes(store, broca_credentials)
            .map_err(ModuleError::Message)?;
    }
    let outcome = sources.live_config.apply(facts);
    let provenance = DecisionProvenance {
        quota_signal_json,
        policy_revision: outcome.policy_revision,
        anchor_version: outcome
            .routing_config
            .overqualification
            .mode
            .anchor_version(),
    };
    // Borrowed rows are added after the live overlays so they carry the target's
    // current quality data, and before pricing so they get their own plane.
    striatum_core::score_alias::append_alias_rows(facts, sources.catalog_targets);
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

/// Serve retained legacy panel/account and operator operations. The public
/// frame dispatcher sends every route.select request to routing_v1 instead.
pub(super) async fn handle_select_request(
    frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    min_band: &SharedMinBand,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    handle_select_request_attempt(
        frame,
        route_epoch,
        writer,
        quota_cache,
        store,
        min_band,
        routing_config_path,
        operator_config_path,
        broca_credentials,
    )
    .await
}

async fn handle_select_request_attempt(
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
    if envelope.method == ROUTE_SELECT_PANEL_OP {
        crate::info_operator(format!(
            "[routing] legacy panel call principal={}",
            route_principal(channel, epoch)
        ));
    }

    if envelope.method == ROUTE_CATALOG_TARGETS_OP {
        return crate::catalog_targets::serve(frame, writer, broca_credentials).await;
    }
    if envelope.method == ROUTE_HEAD_CHOICES_OP {
        return crate::head_choices::serve(
            frame,
            writer,
            store,
            operator_config_path,
            broca_credentials,
        )
        .await;
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
        let request: serde_json::Value = serde_json::from_slice(&frame.body).unwrap_or_default();
        let call = (envelope.method == account_selection::ACCOUNT_FOR_OP).then(|| {
            let credentials = broca_credentials.lock().ok();
            account_selection::call_log::AccountCall::legacy(
                &request["params"],
                &principal,
                Some(&live_config),
                credentials.as_deref(),
                store,
            )
        });
        let reply = answer_legacy_account(
            &envelope.method,
            &frame.body,
            &request["params"],
            &principal,
            store,
            broca_credentials,
            operator_config_path,
            &live_config,
            call.clone(),
        );
        if let Some(call) = call {
            call.finish();
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
        return handle_record_cooldown(
            frame,
            writer,
            store,
            broca_credentials,
            operator_config_path,
        )
        .await;
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
        return diagnostics_findings::handle(
            frame,
            writer,
            store,
            operator_config_path,
            broca_credentials,
        )
        .await;
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
                config: Some(&live_config),
                store: store.as_ref(),
                credentials: broca_credentials,
                catalog_targets: &catalog_targets,
                now_ms: now_ms() as i64,
            },
        );
        return send_pool_reply(writer, ver, channel, epoch, corr, reply).await;
    }
    let billing_ready = if envelope.method == ROUTE_SELECT_PANEL_OP {
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

    send_route_error(
        writer,
        ver,
        channel,
        epoch,
        corr,
        "unknown_method",
        &format!("unknown method '{}'", envelope.method),
    )
    .await
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
#[cfg(test)]
pub(crate) fn runner_for_route(
    catalog_targets: &std::collections::BTreeMap<String, String>,
    credentials: &SharedBrocaCredentials,
    store: &SqliteRouteStateStore,
    session: &str,
    provider: &str,
    model: &str,
    variant: Option<&str>,
    classes: Option<&account_selection::ClassContext>,
    priced_per_token: bool,
    owner: Option<&str>,
) -> Result<
    (
        striatum_core::runner::RunnerModel,
        Option<key_pool::PoolGrant>,
    ),
    account_selection::LaunchRefusal,
> {
    if !needs_runner_pin(catalog_targets, provider) {
        if let Some(classes) = classes {
            // The claim answers whether the launch must pin its credential,
            // decided on the state it claimed on, not on the selection's
            // older read. A credential id that does not parse as a pin
            // (`method:provider[:account]`) keeps the plain target, as before.
            let pin = classes.claim_launch_credential(
                store,
                provider,
                model,
                priced_per_token,
                true,
                owner,
            )?;
            if let Some(runner) =
                pin.and_then(|id| striatum_core::runner::RunnerModel::pinned(&id, model, variant))
            {
                return Ok((runner, None));
            }
        }
        return Ok((
            striatum_core::runner::RunnerModel::broca(provider, model, variant),
            None,
        ));
    }
    let grant = key_pool::acquire(
        &key_pool::PoolCtx {
            config: classes.map(|classes| classes.fence_config()),
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
    .map_err(|refusal| account_selection::LaunchRefusal::Refused {
        code: refusal.code,
        message: refusal.message,
    })?;
    if let Some(owner) = owner {
        crate::account_writer::bind_fence_probe_owner(store, &grant.lease_id, owner)
            .map_err(account_selection::LaunchRefusal::store)?;
    }
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

/// A runner's name as prose capitalizes it: `broca` reads as `Broca`.
#[cfg(test)]
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
#[cfg(test)]
pub(super) struct SelectAvailability {
    pub(super) models: Vec<String>,
    pub(super) note: Option<String>,
}

#[cfg(test)]
impl SelectAvailability {
    fn host(models: Vec<String>) -> Self {
        Self { models, note: None }
    }
}

/// Test probe for caller-list versus Broca-served availability. Production
/// panel availability reads the same credential mapping through panel_availability.
#[cfg(test)]
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
    let operator_policy = operator_config.operator_policy_ref();
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
        live_config.quota_model_pools_ref(),
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
        anchor_version: routing_config.overqualification.mode.anchor_version(),
    };
    // Providers with no catalog rows borrow their catalog provider's rows;
    // the operator's subscription coverage, never the caller's
    // subscriptionKeys, prices and admits panel candidates.
    let catalog_targets = broca_credentials
        .lock()
        .map(|credentials| credentials.catalog_targets(&prepared.facts))
        .unwrap_or_default();
    striatum_core::score_alias::append_alias_rows(&mut prepared.facts, &catalog_targets);
    if let Ok(credentials) = broca_credentials.lock() {
        credentials.price_facts(&mut prepared.facts, live_config.pricing());
    }
    live_config.apply(&mut prepared.facts);
    let panel_consumer = if params.panel_spec.class == striatum_core::panel::PanelClass::Lab {
        select_policy::Consumer::Lab
    } else {
        select_policy::Consumer::Athena
    };
    let mut classes = account_selection::ClassContext::read_with_catalog(
        store,
        broca_credentials,
        live_config.clone(),
        ACCOUNT_QUOTA.get(),
        prepared.facts.clone(),
        &prepared.catalog_snapshot,
        now_ms() as i64,
    )
    .map_err(ModuleError::Message)?;
    let registry = broca_credentials
        .lock()
        .map(|credentials| {
            routing_v1::runners::Registry::build(
                operator_config.as_ref(),
                &credentials,
                &prepared.facts,
                now_ms(),
            )
        })
        .unwrap_or_default();
    let tier_settings = crate::tier_settings::Layers::new(
        params.project_root.as_deref(),
        params.workspace_root.as_deref(),
    );
    let tiers = classes.apply_configured_service_tiers(
        &mut prepared.facts,
        |fact| {
            tier_settings.resolve(
                &operator_config,
                panel_consumer.id(),
                &fact.provider_id,
                &fact.model_id,
                params.service_tier,
            )
        },
        &registry.policies(),
        &registry,
        false,
    );
    // Broca seats acquire their pool lease below. Defer those account gates so
    // a refused lease records the exact blocked keys before panel reselection.
    let defer_pool_pin = is_broca_substrate(params.substrate.as_deref());
    classes.apply_eligibility(
        &mut prepared.snapshot,
        &prepared.facts,
        false,
        defer_pool_pin,
    );
    classes.apply_legacy_route_fences(
        &mut prepared.snapshot,
        &prepared.facts,
        false,
        defer_pool_pin,
    );
    warn_ignored_caller_policy_once(!params.subscription_keys.is_empty());
    let resolved_policy = striatum_core::launch_policy::resolve_compiled_routing_policy(
        params.available_models.iter().cloned(),
        operator_config.compiled_routing_policy(),
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(None),
        &catalog_targets,
        false,
    );
    let subscription_keys: HashSet<String> =
        resolved_policy.subscription_keys.into_iter().collect();
    let (availability_substrate, available_models) = panel_availability(
        params.substrate.as_deref(),
        resolved_policy.available_models.into_iter().collect(),
        &prepared.facts,
        broca_credentials,
    );
    #[derive(Default)]
    struct PanelAvailability {
        models: HashSet<String>,
        rejected: HashMap<String, String>,
    }
    let mut mapped_availability = PanelAvailability::default();
    for fact in &prepared.facts {
        let route = fact_to_route(fact);
        if available_models.contains(&route.route_key)
            || available_models.contains(&route.capacity_key)
        {
            mapped_availability.models.insert(route.route_key);
        } else {
            mapped_availability.rejected.insert(
                route.route_key,
                format!(
                    "harness {} does not serve this route for this request",
                    legacy_wire::BROCA
                ),
            );
        }
    }
    if availability_substrate != AvailabilitySubstrate::Host {
        if let Ok(credentials) = broca_credentials.lock() {
            let served = credentials.statically_served_routes(&prepared.facts);
            let registry = routing_v1::runners::Registry::build(
                operator_config.as_ref(),
                &credentials,
                &prepared.facts,
                prepared.now as u64,
            );
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
                } else if served.contains(&route.route_key)
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
    let group = if class == striatum_core::panel::PanelClass::Lab {
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
    let context = striatum_core::panel::PanelSelectionContext {
        seed: params
            .seed
            .as_ref()
            .or(params.task_id.as_ref())
            .cloned()
            .unwrap_or_default(),
        pins: striatum_core::panel::PanelPins {
            synthesizer: live_config
                .slot_pin(crate::config_policy::ATHENA_SYNTHESIZER_CONSUMER)
                .map(|pin| striatum_core::panel::PanelSynthesizerPin {
                    model: pin.model.clone(),
                    variant: pin.variant.clone(),
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
    let mut panel = loop {
        let mut panel = match striatum_core::panel::select_panel_with_availability(
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
                        config: Some(&live_config),
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
            if let Err(error) = crate::account_writer::pool_release(
                store,
                &provider,
                &session,
                Some(&lease_id),
                false,
                now_ms() as i64,
            ) {
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
        if error.code != striatum_core::key_pool::EXPLICIT_PROVIDER_POOL_EXHAUSTED {
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
    for member in &mut panel.members {
        let fact = prepared
            .facts
            .iter()
            .find(|fact| fact_to_route(fact).route_key == member.route_key);
        let key = fact
            .map(|fact| fact_to_route(fact).capacity_key)
            .unwrap_or_else(|| member.route_key.clone());
        if let Some(tier) = tiers.get(&key) {
            member
                .prompt_params
                .insert("service_tier_chosen".into(), json!(tier));
        }
        if let Some(requested) = fact.and_then(|fact| {
            tier_settings.resolve(
                &operator_config,
                panel_consumer.id(),
                &fact.provider_id,
                &fact.model_id,
                params.service_tier,
            )
        }) {
            member
                .prompt_params
                .insert("service_tier_requested".into(), json!(requested));
        }
    }
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
    consumer: Option<&str>,
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
                    &striatum_core::route_state_store::ProviderOutcome {
                        account_ref: account.clone().unwrap_or_else(|| credential.clone()),
                        credential_id: Some(credential.clone()),
                        resets_at_ms: None,
                        credential_refusal: None,
                        ..Default::default()
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
pub(super) fn apply_panel_variant_policy(
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
                consumer: Some(consumer.id()),
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
        anchor_version: provenance.anchor_version,
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
    let anchor_version = provenance.map_or(ANCHOR_VERSION, |value| value.anchor_version);
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
            anchor_version,
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
                        "workload": serde_json::Value::Null,
                    }))
                    .unwrap_or_else(|_| "{}".into()),
                ),
                quota_signal_json,
                policy_revision,
                anchor_version,
            }
        }
    }
}

// Both the route and EVENT reader enter here, so legacy caller authorization
// and account-context construction cannot drift between delivery paths.
#[allow(clippy::too_many_arguments)]
pub(crate) fn answer_legacy_account(
    method: &str,
    body: &[u8],
    params: &serde_json::Value,
    principal: &str,
    store: &SharedStore,
    broca_credentials: &SharedBrocaCredentials,
    operator_config_path: &std::path::Path,
    live_config: &config_overrides::LiveConfig,
    call: Option<account_selection::call_log::AccountCall>,
) -> account_selection::OpReply {
    if method == account_selection::ACCOUNT_OBSERVE_OP
        && params["caller"]["runner"].as_str().is_some_and(|runner| {
            principal != format!("reserved:{}", runner.trim().to_ascii_lowercase())
        })
    {
        return account_selection::OpReply::Error {
            code: "account_not_authorized".into(),
            message: "observation caller.runner must match the authenticated module".into(),
            detail: None,
        };
    }
    crate::account_refusal_log::record(method, principal, params, || {
        if method == account_selection::ACCOUNT_FOR_OP
            && live_config.account_flags().enabled
            && !crate::broca_credentials::wait_for_first_listing(broca_credentials)
        {
            return crate::broca_credentials::listing_not_ready(params);
        }
        let policy = striatum_core::accounts::AllocatorPolicy {
            model_pools: live_config.quota_model_pools_ref().to_vec(),
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
                return reply;
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
        account_selection::handle_with_call(method, body, principal, &ctx, call)
    })
}
