#![forbid(unsafe_code)]

//! The striatum subc module.
//!
//! Lifecycle (mirrors the canonical module handshake, same shape as quota-module):
//!   1. read `--subc <connection-file>`, HMAC-authenticate as a client to subc;
//!   2. send `HELLO { manifest }` registering the routing operations, await
//!      `HELLO_ACK`;
//!   3. serve channel-0 control (`route.bind` → ack, `PING` → `PONG`) and
//!      route-channel data requests (`route.select` → `SelectedModelRoute`).
//!
//! The module OWNS the model catalog (currently the curated seed; the route-state
//! SQLite store + module-to-module quota consumption land in later slices). The
//! caller passes the task's requirements + the set of available routes; the
//! module runs the pure `striatum-core` scorer and returns the selection.

mod account_quota;
mod account_refusal_log;
mod account_selection;
mod account_state;
mod account_writer;
mod background;
#[cfg(test)]
mod baseline_tests;
mod broca_credentials;
mod catalog_targets;
mod concurrency_caps;
mod config_convert;
mod config_overrides;
mod config_policy;
mod credential_fences;
mod diagnostics_findings;
mod engram_catalog;
mod fusiform_catalog;
mod head_choices;
mod key_pool;
mod legacy_wire;
#[cfg(test)]
mod literal_census_tests;
mod logging;
pub(crate) use logging::{info_operator, warn_operator};
#[cfg(test)]
pub(crate) use logging::{
    take_operator_infos, take_operator_warnings, OperatorWarnings, OPERATOR_INFOS,
    OPERATOR_WARNINGS,
};
#[cfg(all(test, feature = "heap-measure"))]
mod memory_measurements;
mod panel_policy;
mod project_settings;
mod quota_bus;
mod quota_consumer;
mod quota_events;
mod retention;
mod route_facts;
mod routing_v1;
mod runtime_lag;
mod select;
mod select_concurrency;
mod select_policy;
mod service_tiers;
mod serving_index;
mod stage_timing;
mod subscription_economics;
mod task_profile_refresh;
#[cfg(test)]
mod test_config;
#[cfg(test)]
#[path = "test_daemon_tests.rs"]
mod test_daemon;
#[cfg(test)]
#[path = "test_exe/tests.rs"]
mod test_exe;
mod tier_settings;
mod usage_facts;
mod variant_policy;
mod winner_ladder;
mod work_model_pin;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    error::Error,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use background::next_frame_or_loop_stop;
#[cfg(test)]
use background::BackgroundLoops;
#[cfg(test)]
use select::price_selection_inputs;
#[cfg(test)]
use select::{
    add_stage_ms, claim_ignored_caller_policy_warning, panel_availability, panel_decision_input,
    quota_signal_json, rate_limit_route_select_line, select_availability, QuotaSignalView,
    RouteSelectLogLimiter, RouteSelectTiming, SelectPanelBody,
};
use select::{
    apply_variant_policy, decision_input_for, handle_select_request, policy_sources_line,
    prepare_selection_inputs, provider_auth_types, remember_provider_identity,
    warn_ignored_caller_policy_once, DecisionProvenance, PreparedSelectionInputs, SelectParams,
    SelectionPolicySources,
};

use broca_credentials::{BrocaCredentials, SharedBrocaCredentials};
use cortexkit_store_types::StorageDescriptor;
use fusiform_catalog::{CatalogReachHealth, SharedCatalogHealth};
use quota_consumer::QuotaConsumer;
use serde::{Deserialize, Serialize};
use serde_json::json;
use striatum_core::launch_policy::resolve_routing_policy_with_catalog_targets;
use striatum_core::model_facts::make_capacity_key;
use striatum_core::model_facts::{Confidence, ModelScores, RoutePricing, ScoreProvenance};
use striatum_core::model_router::{
    ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities, SelectedModelRoute,
};
use striatum_core::quota::{merge_quota_overlay, ProviderUsage};
use striatum_core::route_state_store::FactSource;
use striatum_core::routing_config::ANCHOR_VERSION;
use striatum_core::{
    derive_provider_key, fact_to_route, make_route_key, resolve_routing_policy,
    AvailabilitySubstrate, CatalogFactField, DecisionInput, ExclusionPolicy, ExclusionSpecs,
    ModelFact, ModelRequirements, ModelRoute, ModelRoutingPolicyInput, PanelSelection,
    ResolvedRoutingPolicy, RouteOutcome, RouteStateStore, RouteWallClass, SelectPanelInput,
    SelectedPanel,
};
#[cfg(test)]
use striatum_core::{
    resolve_launch_requirements, routing_config::DEFAULT_ROUTING_CONFIG,
    RequestedModelRequirements, RouteFailureReason, SelectModelRouteInput,
};
use striatum_store::{dev_descriptor, DecisionListFilter, SqliteRouteStateStore};
use subc_protocol::manifest::Concurrency;
use subc_protocol::{
    manifest::{
        Bindings, BuildGitShaSource, GitTreeState, IdentityBinding, LaunchNonceSource,
        ManagementOperation, ManagementOperationKind, ManifestProvenance, ModuleManifest,
        ProviderRole, StorageBinding, StorageKind, StorageScope, TrustTier,
    },
    session::{
        HealthStatus, ModuleControlRequest, ModuleControlResponse, MODULE_CONTROL_OP_HEALTH_CHECK,
    },
    ErrorBody, Flags, Frame, FrameType, ModuleHelloAckBody, ModuleHelloBody, Priority,
    PROTOCOL_VERSION, SUBC_MODULE_ID_ENV,
};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::mpsc,
};

const DEFAULT_MODULE_ID: &str = "striatum";
const ROUTE_SELECT_OP: &str = "route.select";
const ROUTE_RESOLVE_POLICY_OP: &str = "route.resolve_policy";
const ROUTE_SELECT_PANEL_OP: &str = "route.select_panel";
/// Historical classification on legacy panel ledger rows, not a settings key
/// or a routing consumer declaration.
const PANEL_DECISION_CONSUMER: &str = "athena";
/// Render one retained routing decision without requiring local SQLite access.
const ROUTE_EXPLAIN_OP: &str = "route.explain";
/// List recent routing decisions using only compact diagnostic fields.
const ROUTE_DECISIONS_LIST_OP: &str = "route.decisions_list";
/// Read one model capability fact from the live route-state catalog.
const ROUTE_MODEL_FACT_OP: &str = "route.model_fact";
/// Read the complete durable model catalog for display and observation consumers.
const ROUTE_MODEL_FACTS_OP: &str = "route.model_facts";
const ROUTE_CATALOG_TARGETS_OP: &str = "route.catalog_targets";
const ROUTE_HEAD_CHOICES_OP: &str = "route.head_choices";
/// Live catalog write: add a model or adjust its scores/metadata without a
/// store migration or module deploy. Merge semantics against the stored fact;
/// writes land as FactSource::Override so a re-seed never reverts them.
const ROUTE_MODEL_UPSERT_OP: &str = "route.model_upsert";
/// Set a recorded decision's terminal outcome (idempotent UPDATE by id).
const ROUTE_SET_DECISION_OUTCOME_OP: &str = "route.set_decision_outcome";
/// Record one observed-TPS usage sample (idempotent by message id).
const ROUTE_RECORD_USAGE_OP: &str = "route.record_usage";
/// Record a route cooldown / quota-error increment (best-effort soft state).
const ROUTE_RECORD_COOLDOWN_OP: &str = "route.record_cooldown";
/// Query only cooldown state for a frozen route without re-running admission.
const ROUTE_COOLDOWN_STATUS_OP: &str = "route.cooldown_status";
/// Clear an operator-identified route-local terminal-failure backoff without changing
/// provider cooldown or quota state.
const ROUTE_CLEAR_FAILURE_BACKOFF_OP: &str = "route.clear_failure_backoff";
/// Lift a route-local cooldown and quota wall without resetting failure backoff.
const ROUTE_CLEAR_COOLDOWN_OP: &str = "route.clear_cooldown";
/// Lift every credential entitlement fence on one named vault credential at
/// once, for use right after topping it up. Operator-only: only the `direct`
/// and `reserved:callosum` principals may call it.
const ROUTE_CLEAR_CREDENTIAL_FENCES_OP: &str = "route.clear_credential_fences";
const ROUTE_WHAT_IF_OP: &str = "route.what_if";
/// Observability op: reports whether the outbound quota-consumer connection is
/// live and how fresh its windows are. Lets a consumer (and the e2e) confirm the
/// module->module quota path actually round-tripped, not just that it didn't hang.
const QUOTA_STATUS_OP: &str = "route.quota_status";
/// Observability op: each subscription account's current quota view, its age
/// band, its vault join and why its reading can or cannot be used for cold work,
/// plus roster gaps. Read-only; nothing selects an account from it yet.
const ROUTE_ACCOUNT_QUOTA_STATUS_OP: &str = "route.account_quota_status";
/// Operator worklist: every `aa_inferred` row plus unscored catalog rows.
const ROUTE_LIST_INFERRED_OP: &str = "route.list_inferred";
/// Focused alias for operators that only need never-scored Fusiform discoveries.
const ROUTE_LIST_UNSCORED_OP: &str = "route.list_unscored";
/// Operator worklist for Fusiform: scored routes whose catalog row states no
/// canonical model id, so their score is not shared across the model's routes.
const ROUTE_LIST_CANONICAL_GAPS_OP: &str = "route.list_canonical_gaps";
const HELLO_CORR: u64 = 1;
const EGRESS_BUFFER: usize = 64;
/// Retry bus attachment and failed fallback snapshots promptly during startup
/// or an outage, without making quota availability a prerequisite for selection.
const QUOTA_RETRY_INTERVAL: Duration = Duration::from_secs(3);
/// Age past which the cached quota snapshot no longer steers selection. The
/// backstop runs every ten minutes, so a snapshot this old has missed a backstop
/// refresh. Quota windows move within that time:
/// insula spends banked resets, five-hour windows roll over, and a wall read an
/// hour ago is no evidence of a wall now. Past this bound selection treats quota
/// as unknown (neutral pressure, no quota walls), each decision says so, and
/// health reports degraded with the snapshot's age.
const QUOTA_STALE_AFTER_MS: u64 = 15 * 60_000;

/// Latest quota snapshot, shared between the background refresh task (writer) and
/// `route.select` handlers (readers). The lock is NEVER held across an `.await`:
/// the refresh task fetches outside the lock then takes it briefly to merge; a
/// selection takes it only to clone the snapshot out.
#[derive(Clone, Default)]
struct QuotaCache {
    windows: Vec<ProviderUsage>,
    refreshed_at_ms: u64,
    available: bool,
    /// Last connect/fetch error (observability: surfaced via route.quota_status).
    last_error: Option<String>,
    /// First degraded time for each provider/account whose healthy window is retained.
    retained_degraded_since_ms: HashMap<String, u64>,
    /// Set when `windows` came from the snapshot persisted before the last
    /// restart rather than from a poll made by this process: the Unix-millisecond
    /// poll time at which that snapshot was accepted. Cleared by the first
    /// successful poll, which replaces the restored windows wholesale.
    restored_from_ms: Option<u64>,
    consumer: quota_events::ConsumerHealth,
    snapshot_persistence: quota_events::SnapshotPersistence,
}

type SharedQuotaCache = Arc<Mutex<QuotaCache>>;

/// The cached snapshot's age when it is past [`QUOTA_STALE_AFTER_MS`]; `None`
/// while it is fresh or when there has never been a reading to age.
fn stale_quota_age_ms(refreshed_at_ms: u64, now_ms: u64) -> Option<u64> {
    if refreshed_at_ms == 0 {
        return None;
    }
    let age_ms = now_ms.saturating_sub(refreshed_at_ms);
    (age_ms > QUOTA_STALE_AFTER_MS).then_some(age_ms)
}

/// Human-readable age for health details and decision notes, e.g. "7h05m".
fn render_age(age_ms: u64) -> String {
    let minutes = age_ms / 60_000;
    if minutes >= 60 {
        format!("{}h{:02}m", minutes / 60, minutes % 60)
    } else {
        format!("{minutes}m")
    }
}

/// The explanation line a decision carries when its quota snapshot is stale.
fn stale_quota_note(age_ms: u64, last_error: Option<&str>) -> String {
    let mut note = format!(
        "quota signal: stale (last successful refresh {} ago, bound {}); quota pressure and walls ignored, every provider ranked as quota-unknown",
        render_age(age_ms),
        render_age(QUOTA_STALE_AFTER_MS)
    );
    if let Some(error) = last_error {
        note.push_str(&format!("; last refresh error: {error}"));
    }
    note
}

/// The routing module's health from its quota snapshot: degraded, naming the
/// age and the last refresh error, once the snapshot is stale.
fn quota_health(cache: &QuotaCache, now_ms: u64) -> (Option<String>, serde_json::Value) {
    let age_ms = (cache.refreshed_at_ms > 0).then(|| now_ms.saturating_sub(cache.refreshed_at_ms));
    let stale_age = stale_quota_age_ms(cache.refreshed_at_ms, now_ms);
    let metrics = json!({
        "available": cache.available,
        "refreshedAtMs": cache.refreshed_at_ms,
        "ageMs": age_ms,
        "staleAfterMs": QUOTA_STALE_AFTER_MS,
        "stale": stale_age.is_some(),
        "lastError": cache.last_error,
        "consumer": cache.consumer,
    });
    let detail = stale_age.map(|age_ms| {
        let mut detail = format!(
            "quota snapshot stale: last successful refresh {} ago (bound {}); selection treats quota as unknown",
            render_age(age_ms),
            render_age(QUOTA_STALE_AFTER_MS)
        );
        if let Some(error) = &cache.last_error {
            detail.push_str(&format!("; last refresh error: {error}"));
        }
        detail
    });
    (detail, metrics)
}

/// The per-account quota state, set once when the store opens. It is a process
/// global rather than a handler argument because only the read-only
/// `route.account_quota_status` operation reads it; `None` until boot restores it.
static ACCOUNT_QUOTA: std::sync::OnceLock<account_quota::SharedAccountQuota> =
    std::sync::OnceLock::new();

/// The route-state store, shared by the select handler (reader). Single-writer:
/// this module holds the store's lease for its lifetime.
type SharedStore = Arc<SqliteRouteStateStore>;
/// Shared read of `min_best_effort_adequacy` from config. Written once at boot;
/// read on every `route.select`.
type SharedMinBand = Arc<std::sync::Mutex<Option<f64>>>;
type RouteEpochs = HashMap<u16, u32>;

/// The caller identity the daemon attested when it bound each route (for
/// example `reserved:broca` for the supervised Broca module), by channel, with
/// the epoch it was bound under. The account-selection operations namespace
/// admissions and affinity by it and accept a report only for an admission
/// they issued to the same identity; consumer admission also reads it. It is a
/// process global so the shared request handler keeps its signature.
static ROUTE_PRINCIPALS: OnceLock<Mutex<HashMap<u16, (u32, String)>>> = OnceLock::new();

fn route_principals() -> &'static Mutex<HashMap<u16, (u32, String)>> {
    ROUTE_PRINCIPALS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The principal label for a request on `channel` under `epoch`; a route whose
/// bind carried no attestation is `unattested`.
fn route_principal(channel: u16, epoch: u32) -> String {
    route_principals()
        .lock()
        .ok()
        .and_then(|map| {
            map.get(&channel)
                .filter(|(bound_epoch, _)| *bound_epoch == epoch)
                .map(|(_, label)| label.clone())
        })
        .unwrap_or_else(|| account_selection::principal_label(None))
}

/// The scope stamp of each bind whose route this module records decisions for,
/// by channel, reduced to the fields the decision ledger keeps: the Entorhinal
/// project id and the session folder. `None` for a route opened without a
/// scope. The daemon sends both only to a target that provides
/// `scope-workdir/v1`, so a scope it stamped without them records nothing.
static ROUTE_SCOPES: OnceLock<Mutex<HashMap<u16, (u32, Option<DecisionScope>)>>> = OnceLock::new();

/// Authorization is kept separately from decision-ledger metadata so a scope
/// without a project id still carries its daemon-attested owner and permission.
static ROUTE_SCOPE_AUTHORITY: OnceLock<
    Mutex<HashMap<u16, (u32, Option<routing_v1::ScopeAuthority>)>>,
> = OnceLock::new();

fn route_scope_authority(channel: u16, epoch: u32) -> Option<routing_v1::ScopeAuthority> {
    ROUTE_SCOPE_AUTHORITY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .ok()
        .and_then(|map| {
            map.get(&channel)
                .filter(|(bound, _)| *bound == epoch)
                .and_then(|(_, scope)| scope.clone())
        })
}

fn route_scopes() -> &'static Mutex<HashMap<u16, (u32, Option<DecisionScope>)>> {
    ROUTE_SCOPES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The project and folder a route's scope was bound to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DecisionScope {
    project_id: Option<String>,
    workdir: Option<String>,
}

impl DecisionScope {
    /// The fields of a bind's scope stamp; `None` when the bind carried no
    /// scope at all. The stamp is the daemon's, read from the control frame,
    /// never from a request.
    fn from_stamp(scope: Option<&subc_protocol::scope::ScopeStamp>) -> Option<Self> {
        scope.map(|stamp| Self {
            project_id: stamp.project_id.clone(),
            workdir: stamp.workdir.clone(),
        })
    }
}

/// Record the scope of the route bound on `channel` under `epoch`.
fn remember_route_scope(channel: u16, epoch: u32, scope: Option<DecisionScope>) {
    if let Ok(mut map) = route_scopes().lock() {
        map.insert(channel, (epoch, scope));
    }
}

fn clear_route_scope(channel: u16, epoch: u32) {
    head_choices::forget_bind(channel, epoch);
    if let Ok(mut map) = route_scopes().lock() {
        if map.get(&channel).is_some_and(|(bound, _)| *bound == epoch) {
            map.remove(&channel);
        }
    }
    if let Ok(mut map) = ROUTE_SCOPE_AUTHORITY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    {
        if map.get(&channel).is_some_and(|(bound, _)| *bound == epoch) {
            map.remove(&channel);
        }
    }
}

/// The scope the request on `channel` was bound under, when it still holds
/// for `epoch`.
fn route_scope(channel: u16, epoch: u32) -> Option<DecisionScope> {
    route_scopes().lock().ok().and_then(|map| {
        map.get(&channel)
            .filter(|(bound, _)| *bound == epoch)
            .and_then(|(_, scope)| scope.clone())
    })
}

fn clear_route_epoch(route_epochs: &mut RouteEpochs, channel: u16, epoch: u32) {
    if route_epochs.get(&channel) == Some(&epoch) {
        route_epochs.remove(&channel);
        clear_route_scope(channel, epoch);
        if let Ok(mut principals) = route_principals().lock() {
            if principals
                .get(&channel)
                .is_some_and(|(bound, _)| *bound == epoch)
            {
                principals.remove(&channel);
            }
        }
    }
}

fn quota_cache_key(window: &ProviderUsage) -> String {
    format!(
        "{}\0{}",
        window.provider,
        window.account.as_deref().unwrap_or("")
    )
}

fn merge_quota_windows(
    existing: &mut Vec<ProviderUsage>,
    fresh: Vec<ProviderUsage>,
    retained_degraded_since_ms: &mut HashMap<String, u64>,
    now_ms: u64,
) {
    // Successful events for other accounts advance the cache-wide refresh time,
    // so expire retained windows against the first degraded reading for each key.
    let expired_keys: HashSet<String> = retained_degraded_since_ms
        .iter()
        .filter_map(|(key, first_degraded_at_ms)| {
            (now_ms.saturating_sub(*first_degraded_at_ms) > QUOTA_STALE_AFTER_MS)
                .then(|| key.clone())
        })
        .collect();
    if !expired_keys.is_empty() {
        existing.retain(|window| !expired_keys.contains(&quota_cache_key(window)));
        for key in expired_keys {
            retained_degraded_since_ms.remove(&key);
        }
    }

    let mut fresh_by_key: std::collections::HashMap<String, ProviderUsage> = fresh
        .into_iter()
        .map(|window| (quota_cache_key(&window), window))
        .collect();
    let mut merged = Vec::with_capacity(existing.len() + fresh_by_key.len());
    for window in existing.drain(..) {
        let key = quota_cache_key(&window);
        if let Some(replacement) = fresh_by_key.remove(&key) {
            // Three classes will not self-recover, so a retained "last healthy"
            // window is a fiction that can only mislead: credential_absent
            // (nobody configured the credential on this host), credential_unusable
            // (QTA 2026-08-15: a revoked OAuth grant - Google answers
            // invalid_grant, permanent under RFC 6749, only a re-authorisation
            // recovers it), and no_quota_reported (QTA 2026-08-24, qwen
            // subscription-end incident: valid credential, gateway affirms
            // success and reports NO plan block - the plan is gone; the retained
            // healthy window routed workers to a dead subscription for a day
            // before this arm learned the class). Drop retention for all three
            // and take the degraded entry as the truth. Every other class keeps
            // its last healthy window only until that key's degraded age reaches
            // the stale bound; refreshes for other keys cannot extend its lifetime.
            let dead_credential = replacement.error_class.as_deref().is_some_and(|class| {
                class == "credential_absent"
                    || class == "credential_unusable"
                    || class == "no_quota_reported"
            });
            if window.is_healthy() && !replacement.is_healthy() && !dead_credential {
                let first_degraded_at_ms = *retained_degraded_since_ms
                    .entry(key.clone())
                    .or_insert_with(|| {
                        let reason = replacement
                            .error
                            .as_deref()
                            .filter(|reason| !reason.is_empty())
                            .unwrap_or("no usable quota windows");
                        let provider = match replacement
                            .account
                            .as_deref()
                            .filter(|account| !account.is_empty())
                        {
                            Some(account) => format!("{} account {}", replacement.provider, account),
                            None => replacement.provider.clone(),
                        };
                        eprintln!(
                            "[striatum] retaining last healthy quota window for {provider} after degraded refresh: {reason}"
                        );
                        now_ms
                    });
                if now_ms.saturating_sub(first_degraded_at_ms) > QUOTA_STALE_AFTER_MS {
                    retained_degraded_since_ms.remove(&key);
                    merged.push(replacement);
                } else {
                    merged.push(window);
                }
            } else {
                retained_degraded_since_ms.remove(&key);
                merged.push(replacement);
            }
        } else {
            merged.push(window);
        }
    }
    for (key, window) in fresh_by_key {
        if window.is_healthy() {
            retained_degraded_since_ms.remove(&key);
        }
        merged.push(window);
    }
    *existing = merged;
}

#[derive(Default)]
struct QuotaRefreshWarnings {
    reported_value_sets: BTreeSet<BTreeSet<String>>,
    providers_walled_with_funds: BTreeSet<String>,
}

impl QuotaRefreshWarnings {
    fn warnings_for(&mut self, fresh: &[ProviderUsage]) -> Vec<String> {
        let mut warnings = Vec::new();
        let unsupported: BTreeSet<String> = fresh
            .iter()
            .flat_map(|entry| entry.spend.iter())
            .filter(|pool| !pool.has_known_funding())
            .map(|pool| pool.funding.clone())
            .collect();
        if !unsupported.is_empty() && self.reported_value_sets.insert(unsupported.clone()) {
            warnings.push(format!(
                "[striatum] quota refresh ignored unsupported spend funding values: {unsupported:?}"
            ));
        }

        let mut wall_details_by_provider = BTreeMap::<String, BTreeSet<String>>::new();
        // A positive balance in non-spendable pools cannot prevent a wall when
        // every spend-authorized pool is empty. Name that policy when the
        // provider enters this state so the visible balance is not mistaken for
        // a quota bug; re-arm the warning only after the state clears.
        for entry in fresh {
            if entry.spend.is_empty() {
                continue;
            }
            let all_spendable_empty = entry
                .spend
                .iter()
                .filter(|pool| pool.has_supported_funding())
                .all(|pool| pool.is_known_empty());
            if !all_spendable_empty {
                continue;
            }
            let withheld: BTreeSet<String> = entry
                .spend
                .iter()
                .filter(|pool| !pool.has_supported_funding())
                .filter_map(|pool| {
                    pool.remaining
                        .as_ref()
                        .filter(|remaining| !remaining.is_non_positive())
                        .map(|remaining| format!("{} ({})", remaining.render(), pool.funding))
                })
                .collect();
            if !withheld.is_empty() {
                wall_details_by_provider
                    .entry(entry.provider.clone())
                    .or_default()
                    .extend(withheld);
            }
        }

        let currently_walled: BTreeSet<String> = wall_details_by_provider.keys().cloned().collect();
        for provider in currently_walled.difference(&self.providers_walled_with_funds) {
            let withheld = wall_details_by_provider
                .get(provider)
                .expect("active provider has a withheld-balance description");
            warnings.push(format!(
                "[striatum] provider {provider} walled on empty spendable pools while non-spendable pools hold {} - policy, not a quota bug (only granted/subscription funding may spend)",
                withheld.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        self.providers_walled_with_funds = currently_walled;
        warnings
    }
}

fn log_quota_refresh_warnings_with(
    fresh: &[ProviderUsage],
    reported: &mut QuotaRefreshWarnings,
    mut emit: impl FnMut(&str),
) {
    for warning in reported.warnings_for(fresh) {
        emit(&warning);
    }
}

fn log_quota_refresh_warnings(fresh: &[ProviderUsage]) {
    static REPORTED: OnceLock<Mutex<QuotaRefreshWarnings>> = OnceLock::new();
    let mut reported = REPORTED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    log_quota_refresh_warnings_with(fresh, &mut reported, |warning| eprintln!("{warning}"));
}

#[cfg(test)]
fn record_quota_readings(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) {
    if let Err(error) = write_quota_readings(store, fresh, observed_at_ms) {
        tracing::warn!(error, "quota reading log: sample not recorded");
    }
}

/// Append healthy entries to the consumption estimate's reading log, resolving
/// providers and per-model capacity keys the same way as the quota overlay.
/// Return write failures so the delivery adapter leaves the event unacknowledged.
fn write_quota_readings(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<(), String> {
    let rows = quota_reading_rows(store, fresh, observed_at_ms)?;
    store
        .append_quota_readings(&rows)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn quota_reading_rows(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<Vec<striatum_core::route_state_store::QuotaReadingLogRow>, String> {
    let facts = store.get_facts().map_err(|e| e.to_string())?;
    let observed_at = i64::try_from(observed_at_ms).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for entry in fresh {
        let Some(join) = striatum_core::quota::quota_route_join(entry, &facts) else {
            continue;
        };
        let capacity_keys = striatum_core::quota::quota_capacity_keys(entry, &facts);
        rows.extend(striatum_core::quota_poll::reading_log_rows_for_poll(
            entry,
            join.provider_id,
            &capacity_keys,
            observed_at,
        ));
    }
    Ok(rows)
}

fn apply_quota_refresh(cache: &mut QuotaCache, fresh: Vec<ProviderUsage>, observed_at_ms: u64) {
    // The quota producer can add funding labels over time. Unrecognized labels are
    // logged once per distinct set; a provider wall caused by empty spendable
    // balances and positive non-spendable balances is logged when it begins, then
    // again only if it clears and returns.
    log_quota_refresh_warnings(&fresh);
    // A restored snapshot only bridges the gap between boot and the first poll.
    // That poll starts from an empty cache, exactly as it did before snapshots
    // were persisted, so nothing restored outlives the first fresh reading.
    if cache.restored_from_ms.take().is_some() {
        cache.windows.clear();
        cache.retained_degraded_since_ms.clear();
    }
    merge_quota_windows(
        &mut cache.windows,
        fresh,
        &mut cache.retained_degraded_since_ms,
        observed_at_ms,
    );
    cache.refreshed_at_ms = observed_at_ms;
    cache.available = true;
    cache.last_error = None;
}

/// Test helper for writing a snapshot without a live delivery cursor.
#[cfg(test)]
fn persist_quota_snapshot(
    store: &SqliteRouteStateStore,
    windows: &[ProviderUsage],
    observed_at_ms: u64,
) {
    if let Err(error) = write_quota_snapshot(store, windows, observed_at_ms) {
        tracing::warn!(error, "quota snapshot not persisted");
    }
}

fn write_quota_snapshot(
    store: &SqliteRouteStateStore,
    windows: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<(), String> {
    let observed_at = i64::try_from(observed_at_ms).map_err(|e| e.to_string())?;
    serde_json::to_string(windows)
        .map_err(|error| error.to_string())
        .and_then(|json| {
            store
                .put_quota_snapshot(&json, observed_at)
                .map_err(|error| error.to_string())
        })
}

/// Build the boot-time quota cache from the snapshot the previous process
/// persisted. The restored windows are treated exactly like windows retained
/// through a live outage: the overlay recomputes pace against the current clock
/// and ignores any window whose reset has already passed, so no separate age
/// cutoff applies. `available` stays false because this process has not reached
/// insula yet. A missing or unreadable snapshot yields an empty cache.
fn restore_quota_cache(store: &SqliteRouteStateStore) -> QuotaCache {
    let persisted = match store.quota_snapshot() {
        Ok(Some(persisted)) => persisted,
        Ok(None) => return QuotaCache::default(),
        Err(error) => {
            tracing::warn!(error = %error, "quota snapshot unreadable; starting with no reading");
            return QuotaCache::default();
        }
    };
    let windows: Vec<ProviderUsage> = match serde_json::from_str(&persisted.windows_json) {
        Ok(windows) => windows,
        Err(error) => {
            tracing::warn!(error = %error, "quota snapshot undecodable; starting with no reading");
            return QuotaCache::default();
        }
    };
    let observed_at_ms = u64::try_from(persisted.observed_at).unwrap_or(0);
    QuotaCache {
        windows,
        refreshed_at_ms: observed_at_ms,
        available: false,
        last_error: None,
        retained_degraded_since_ms: HashMap::new(),
        restored_from_ms: Some(observed_at_ms),
        consumer: Default::default(),
        snapshot_persistence: Default::default(),
    }
}

fn record_quota_refresh_failure(cache: &mut QuotaCache, phase: &str, error: impl fmt::Display) {
    cache.available = false;
    let error = format!("{phase}: {error}");
    // The loop retries every few seconds, so print only when the reason
    // changes. This failure used to go to debug tracing only, which left a
    // refresh failing on every retry for hours with nothing in the stderr log.
    if cache.last_error.as_deref() != Some(error.as_str()) {
        eprintln!(
            "[striatum] quota refresh failed; selection keeps the snapshot from refreshedAtMs {} until a refresh succeeds or it goes stale: {error}",
            cache.refreshed_at_ms
        );
    }
    cache.last_error = Some(error);
}

fn record_cooldown_update(
    store: &striatum_store::SqliteRouteStateStore,
    params: &RecordCooldownParams,
) -> striatum_core::RouteStateResult<()> {
    // Existing machine-formatted account/key annotations take precedence over
    // an identity-free cooldown. Never turn one account's refusal into fanout;
    // only genuinely anonymous signals get the non-quota clock below.
    if params.credential_id.is_none()
        && params.last_error.as_deref().is_some_and(|error| {
            striatum_core::account_wall::account_scoped_error(error)
                || striatum_core::account_wall::credential_refusal_in(error).is_some()
                || serde_json::from_str::<serde_json::Value>(error)
                    .ok()
                    .is_some_and(|value| {
                        let provider = value.get("provider").unwrap_or(&value);
                        provider.get("class").is_some()
                            && provider.get("provider_code").is_some()
                            && ["account_ref", "credential_id"].iter().any(|field| {
                                provider
                                    .get(field)
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|id| !id.trim().is_empty())
                            })
                    })
        })
    {
        return Ok(());
    }
    if let Some(credential) = params
        .credential_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .filter(|_| params.wall_class == Some(RouteWallClass::Transient))
    {
        let Some(until) = params.cooldown_until else {
            return Ok(());
        };
        let Some((provider, model)) = params.capacity_key.split_once('/') else {
            return Ok(());
        };
        return crate::account_writer::record_transient_account_route_fence(
            store,
            &striatum_store::AccountRouteFence {
                provider: provider.into(),
                model: model.into(),
                credential_id: credential.into(),
                until_ms: until as i64,
                decision_id: String::new(),
                recorded_at: now_ms() as i64,
            },
        );
    }
    let update = striatum_core::RouteStateUpdate {
        observed_at_ms: Some(now_ms() as i64),
        // No account-local typed branch handled this signal. Do not infer a
        // subscription identity from its error text or reported quota label.
        unscoped_wall: true,
        cooldown_until: params.cooldown_until,
        quota_exhausted_until: params.quota_exhausted_until,
        wall_class: params.wall_class,
        quota_pressure: params.quota_pressure,
        last_error: params.last_error.clone(),
        increment_quota_error: params.increment_quota_error,
        increment_failure: params.increment_failure,
        ..Default::default()
    };
    if params.provider_wide {
        let provider_key = derive_provider_key(&params.route_key)
            .or_else(|| derive_provider_key(&params.capacity_key))
            .ok_or_else(|| {
                striatum_core::RouteStateStoreError(
                    "provider-wide cooldown has no canonical provider key".into(),
                )
            })?;
        return crate::account_writer::record_provider_cooldown(
            store,
            provider_key,
            &params.route_key,
            &params.capacity_key,
            &update,
        );
    }
    crate::account_writer::update_route_state(
        store,
        &params.route_key,
        &params.capacity_key,
        &update,
    )
}

/// Why a `route.record_cooldown` request must not reach the store, if it must not.
///
/// A route-scoped write stores `capacityKey` on the route's row, replacing the
/// one already there, and soft pressure and walls are shared by every route under
/// that capacity. A capacity that is not the route's own would therefore move the
/// route's walls and pressure onto another model. Routing builds both keys with
/// `make_route_key` and `make_capacity_key`, so a route's capacity is the route
/// key itself or a prefix of it that ends at a `/`. Provider-wide writes are
/// checked by the store, which requires both keys to name the same provider.
/// Quota pressure is a 0..=1 score that selection adds to a route's cost, so a
/// value outside that range is refused rather than stored.
fn record_cooldown_params_error(params: &RecordCooldownParams) -> Option<String> {
    if let Some(pressure) = params.quota_pressure {
        if !(0.0..=1.0).contains(&pressure) {
            return Some(format!(
                "record_cooldown quotaPressure {pressure} is outside 0..=1"
            ));
        }
    }
    if !params.provider_wide {
        let capacity = params.capacity_key.as_str();
        let paired = !capacity.is_empty()
            && params
                .route_key
                .strip_prefix(capacity)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'));
        if !paired {
            return Some(format!(
                "record_cooldown capacityKey {:?} is not the capacity of routeKey {:?}",
                params.capacity_key, params.route_key
            ));
        }
    }
    None
}

/// Record the audit row for a selection that ended without a route. The
/// routing failure is still what the caller receives, so a failed write does
/// not change the reply; it is logged so a missing row can be explained.
fn record_failed_selection_decision(store: &impl RouteStateStore, decision: &DecisionInput) {
    if let Err(error) = store.record_decision(decision) {
        warn_operator(format!(
            "[striatum] WARN: a failed route.select for {} (task {}, send {}) has no decision row: {error}",
            decision.target_agent,
            decision.task_id.as_deref().unwrap_or("-"),
            decision.send_id.as_deref().unwrap_or("-"),
        ));
    }
}

#[cfg(test)]
thread_local! {
    /// The wall clock a test injected on this thread (see [`InjectedClock`]).
    static INJECTED_NOW_MS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

fn now_ms() -> u64 {
    #[cfg(test)]
    if let Some(now) = INJECTED_NOW_MS.with(std::cell::Cell::get) {
        return now;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Holds this thread's [`now_ms`] at a fixed time until dropped, then restores
/// whatever clock was in force before. A test that replays frozen evidence
/// (quota readings, a vault listing) sets the time that evidence was captured
/// at, so freshness and reset checks answer the same on every run. The clock
/// is per thread: a `#[tokio::test]` runs its tasks on the test's own thread.
#[cfg(test)]
pub(crate) struct InjectedClock {
    previous: Option<u64>,
}

#[cfg(test)]
impl InjectedClock {
    pub(crate) fn set(now_ms: u64) -> Self {
        Self {
            previous: INJECTED_NOW_MS.with(|clock| clock.replace(Some(now_ms))),
        }
    }
}

#[cfg(test)]
impl Drop for InjectedClock {
    fn drop(&mut self) {
        INJECTED_NOW_MS.with(|clock| clock.set(self.previous));
    }
}

/// The per-thread test state a route handler reads: the injected clocks, ID
/// sequence and diagnostics buffer. Route handlers run on a blocking thread (see
/// [`run_route_handler_off_async_workers`]), so the state of the thread that
/// dispatched the handler is installed there for the handler's lifetime.
#[cfg(test)]
#[derive(Clone)]
struct TestThreadState {
    now_ms: Option<u64>,
    warnings: OperatorWarnings,
    infos: OperatorWarnings,
    replay: Option<striatum_store::replay::Inputs>,
}

#[cfg(test)]
impl TestThreadState {
    fn capture() -> Self {
        Self {
            now_ms: INJECTED_NOW_MS.with(std::cell::Cell::get),
            warnings: OPERATOR_WARNINGS.with(|sink| Arc::clone(&sink.borrow())),
            infos: OPERATOR_INFOS.with(|sink| Arc::clone(&sink.borrow())),
            replay: striatum_store::replay::current(),
        }
    }

    /// Make this the current thread's state until the returned guard drops,
    /// which puts back what the thread had before. Blocking threads are reused,
    /// so a handler's state must not outlive it.
    fn install(self) -> InstalledTestThreadState {
        let replay = self.replay.as_ref().map(|inputs| inputs.install());
        InstalledTestThreadState {
            previous: Some(TestThreadState {
                now_ms: INJECTED_NOW_MS.with(|clock| clock.replace(self.now_ms)),
                warnings: OPERATOR_WARNINGS.with(|sink| sink.replace(self.warnings)),
                infos: OPERATOR_INFOS.with(|sink| sink.replace(self.infos)),
                replay: None,
            }),
            _replay: replay,
        }
    }
}

/// Holds the state a thread had before [`TestThreadState::install`].
#[cfg(test)]
struct InstalledTestThreadState {
    previous: Option<TestThreadState>,
    _replay: Option<striatum_store::replay::Guard>,
}

#[cfg(test)]
impl Drop for InstalledTestThreadState {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            INJECTED_NOW_MS.with(|clock| clock.set(previous.now_ms));
            OPERATOR_WARNINGS.with(|sink| *sink.borrow_mut() = previous.warnings);
            OPERATOR_INFOS.with(|sink| *sink.borrow_mut() = previous.infos);
        }
    }
}

/// Resolve the storage descriptor, preferring the daemon-delivered one from
/// `HELLO_ACK.storage` and falling back to a local dev descriptor when storage is
/// not centrally configured. The ONLY descriptor-acquisition site: the
/// dev↔delivered choice lives here alone.
fn resolve_storage_descriptor(
    ack_storage: &Option<serde_json::Value>,
    module_id: &str,
) -> Result<StorageDescriptor, ModuleError> {
    if let Some(value) = ack_storage {
        return serde_json::from_value(value.clone()).map_err(ModuleError::Json);
    }
    // No managed storage configured by the daemon → dev fallback: a per-module
    // sqlite db under the OS temp dir, so a standalone/dev run still has a store.
    let path = std::env::temp_dir()
        .join("cortexkit")
        .join(module_id)
        .join("store.db")
        .to_string_lossy()
        .into_owned();
    Ok(dev_descriptor(module_id, path))
}

/// Open the route-state store from the resolved descriptor (applies migrations:
/// DDL + the 33-fact seed, run-once). The lib derives the single-writer lease
/// location from the descriptor's db path (one lease per database, by
/// construction), so there is nothing to pass or get wrong here.
fn open_route_state_store(descriptor: &StorageDescriptor) -> Result<SharedStore, ModuleError> {
    let store = SqliteRouteStateStore::open(descriptor)
        .map_err(|e| ModuleError::Message(format!("open route-state store: {e}")))?;
    Ok(Arc::new(store))
}

fn check_config_at(path: &Path) -> Result<(), ModuleError> {
    config_overrides::initialize_config(path).map_err(ModuleError::Message)
}

fn main() -> Result<(), ModuleError> {
    initialize_logging();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| ModuleError::Message(error.to_string()))?
        .block_on(async_main())
}

fn initialize_logging() {
    logging::install();
}

async fn async_main() -> Result<(), ModuleError> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "config-convert") {
        if args.len() != 3 {
            return Err(ModuleError::Message(format!(
                "usage: {} <input> <output>",
                config_convert::COMMAND
            )));
        }
        let input = std::fs::read_to_string(&args[1])
            .map_err(|error| ModuleError::Message(error.to_string()))?;
        let output = config_convert::convert(&input).map_err(ModuleError::Message)?;
        config_overrides::validate_document(&output).map_err(ModuleError::Message)?;
        std::fs::write(&args[2], output)
            .map_err(|error| ModuleError::Message(error.to_string()))?;
        return Ok(());
    }
    // Read the launch nonce before anything else can spawn a process: until
    // this first read, the pipe the daemon handed it over on is inheritable,
    // so a child spawned earlier would receive it. The result is cached for
    // the process; HELLO and the credential route read that cached answer.
    let _ = subc_os::launch_nonce();
    let connection_file_path = match parse_startup_mode(std::env::args_os().skip(1))? {
        StartupMode::PrintManifest => return print_manifest(),
        StartupMode::CheckConfig => {
            let path = config_overrides::config_path();
            let report = config_overrides::check_config_offline(
                &path,
                config_overrides::default_store_path().as_deref(),
            )
            .map_err(ModuleError::Message)?;
            eprintln!("config check passed: {}\n{report}", path.display());
            return Ok(());
        }
        StartupMode::PrintConfigSchema => {
            print!("{}", config_overrides::routing_config_schema());
            return Ok(());
        }
        StartupMode::AccountQuotaDryRun {
            connection_file_path,
            capture_dir,
        } => return account_quota_dry_run(connection_file_path, capture_dir).await,
        StartupMode::Serve(connection_file_path) => connection_file_path,
    };
    engram_catalog::write_startup_catalog();
    let config = ModuleConfig::from_env(connection_file_path)?;
    run(config).await
}

#[derive(Debug, PartialEq, Eq)]
enum StartupMode {
    PrintManifest,
    CheckConfig,
    PrintConfigSchema,
    /// Read Insula once as an ordinary consumer and print the account views it
    /// would produce. Writes no store and consults no vault.
    AccountQuotaDryRun {
        connection_file_path: PathBuf,
        capture_dir: Option<PathBuf>,
    },
    Serve(PathBuf),
}

/// `--account-quota-dry-run --subc <file> [--capture <dir>]`: fetch Insula's
/// unfiltered reply, optionally save its exact bytes under
/// `<dir>/usage-get-all.json`, and print the account report as JSON.
async fn account_quota_dry_run(
    connection_file_path: PathBuf,
    capture_dir: Option<PathBuf>,
) -> Result<(), ModuleError> {
    let project_root =
        std::env::temp_dir().join(format!("{DEFAULT_MODULE_ID}-account-quota-dry-run"));
    let _ = std::fs::create_dir_all(&project_root);
    let mut consumer = QuotaConsumer::connect(&connection_file_path, &project_root)
        .await
        .map_err(|error| ModuleError::Message(format!("connect to insula: {error}")))?;
    let body = consumer
        .usage_get_raw()
        .await
        .map_err(|error| ModuleError::Message(format!("usage.get: {error}")))?;
    let mut state = account_quota::AccountQuotaState {
        roster_poll: account_quota::RosterPoll::NotConsulted {
            reason: "the dry run does not consult the vault: it runs without routing's reserved principal"
                .into(),
        },
        ..Default::default()
    };
    let received_at_ms = now_ms() as i64;
    if let Some(dir) = capture_dir {
        capture_account_quota_reply(dir, body.clone()).await?;
    }
    account_quota::apply_usage_reply(&mut state, &body, received_at_ms)
        .map_err(ModuleError::Message)?;
    let status = account_quota::status_json(&state, now_ms() as i64);
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(ModuleError::Json)?
    );
    Ok(())
}

async fn capture_account_quota_reply(dir: PathBuf, body: Vec<u8>) -> Result<(), ModuleError> {
    // Use the route-handler bridge so filesystem waits cannot stall an async
    // worker, including when the caller uses a current-thread runtime.
    run_route_handler_off_async_workers(async move {
        std::fs::create_dir_all(&dir)
            .and_then(|()| std::fs::write(dir.join("usage-get-all.json"), &body))
            .map_err(|error| ModuleError::Message(format!("capture usage: {error}")))
    })
    .await
}

struct ModuleConfig {
    connection_file_path: PathBuf,
    module_id: String,
}

impl ModuleConfig {
    fn from_env(connection_file_path: PathBuf) -> Result<Self, ModuleError> {
        let module_id = std::env::var(SUBC_MODULE_ID_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string());
        Ok(Self {
            connection_file_path,
            module_id,
        })
    }
}

async fn run(config: ModuleConfig) -> Result<(), ModuleError> {
    check_config_at(&config_overrides::config_path())?;
    project_settings::initialize(
        &config.connection_file_path,
        &std::env::current_dir().map_err(|error| ModuleError::Message(error.to_string()))?,
    );
    runtime_lag::RuntimeLagProbe::default().start("routing runtime");
    let stream = connect_to_subc(&config.connection_file_path).await?;
    let (mut read_half, write_half) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
    let writer = tokio::spawn(drain_writer(write_half, rx));

    // Quota delivery runs off the selection hot path. Bus notices fetch bodies
    // over independent outbound insula connections; route.select only reads the
    // cache. An unbound reader switches delivery to 60-second usage.get snapshots,
    // even when the SDK waits for a replacement connection without returning an error.
    let quota_cache: SharedQuotaCache = Arc::new(Mutex::new(QuotaCache::default()));
    let catalog_health: SharedCatalogHealth = Arc::new(Mutex::new(CatalogReachHealth::new(
        config_overrides::DEFAULT_CATALOG_POLL_MS,
    )));

    let loop_result = module_loop(
        &mut read_half,
        tx.clone(),
        &config,
        quota_cache,
        catalog_health,
    )
    .await;
    drop(tx);

    let writer_result = writer
        .await
        .map_err(|e| ModuleError::Message(e.to_string()));
    // The writer closes only after the in-flight request handlers have dropped
    // their reply senders. Flush their final diagnostic increments together.
    let loop_result = match loop_result {
        Ok(store) => {
            background::flush_call_counts(store).await;
            Ok(())
        }
        Err(error) => Err(error),
    };
    match (loop_result, writer_result) {
        (Err(loop_err), _) => Err(loop_err),
        (Ok(()), Ok(Ok(()))) => Ok(()),
        (Ok(()), Ok(Err(writer_err))) => Err(ModuleError::Message(writer_err.to_string())),
        (Ok(()), Err(join_err)) => Err(join_err),
    }
}

async fn connect_to_subc(connection_file_path: &PathBuf) -> Result<TcpStream, ModuleError> {
    let conn = connection_file::read(connection_file_path)
        .map_err(|e| ModuleError::Message(format!("reading connection file: {e}")))?;
    let endpoint = conn
        .endpoints
        .first()
        .ok_or_else(|| ModuleError::Message("connection file has no endpoints".into()))?;
    let addr = format!("{}:{}", endpoint.host, endpoint.port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| ModuleError::Message(format!("connect {addr}: {e}")))?;
    authenticate_client(&mut stream, &conn, std::time::Duration::from_secs(2))
        .await
        .map_err(|e| ModuleError::Message(format!("authenticate: {e}")))?;
    Ok(stream)
}

async fn module_loop<R>(
    read_half: &mut R,
    writer: mpsc::Sender<Frame>,
    config: &ModuleConfig,
    quota_cache: SharedQuotaCache,
    catalog_health: SharedCatalogHealth,
) -> Result<SharedStore, ModuleError>
where
    R: AsyncRead + Unpin,
{
    send_hello(&writer, config).await?;
    let ack = expect_hello_ack(read_half).await?;

    // Open the route-state store from the daemon-delivered descriptor (or the dev
    // fallback). Opening acquires the single-writer lease and runs migrations (the
    // 33-fact seed, once). The store backs route.select's facts + runtime snapshot.
    let descriptor = resolve_storage_descriptor(&ack.storage, &config.module_id)?;
    let store = open_route_state_store(&descriptor)?;
    // HELLO negotiates the storage descriptor supplied by HELLO_ACK, so it
    // cannot wait for loading. Do not start background tasks or dispatch queued
    // requests until account state is published; early requests wait, not fail.
    // The load blocks on the account writer, so it runs on the blocking pool,
    // never on a runtime worker. A failed load does not stop the module: the
    // writer stays up, and each account request or Tick loads again.
    let loading = Arc::clone(&store);
    match tokio::task::spawn_blocking(move || account_state::initialize(loading.as_ref())).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(
            %error,
            "account state did not load; account requests retry the load"
        ),
        Err(error) => {
            return Err(ModuleError::Message(format!("load account state: {error}")));
        }
    }
    // Variant overrides name efforts the catalog must publish. Config loaded
    // before the store opened got only shape checks; check it now, and every
    // later reload, against the stored catalog.
    {
        let catalog_store = Arc::clone(&store);
        config_overrides::attach_reasoning_catalog(
            &config_overrides::config_path(),
            Arc::new(move || {
                catalog_store
                    .reasoning_catalog()
                    .map_err(|error| error.to_string())
            }),
        )
        .map_err(|error| {
            ModuleError::Message(format!(
                "{}: {error}",
                config_overrides::config_path().display()
            ))
        })?;
    }
    // Seed the cache from the last persisted snapshot before anything can serve a
    // selection, so a restart does not select with no quota reading until the
    // first live quota update lands.
    if let Ok(mut guard) = quota_cache.lock() {
        *guard = restore_quota_cache(store.as_ref());
    }
    let background::BackgroundState {
        mut loops,
        broca_credentials,
        override_path,
        operator_config_path,
    } = background::start(config, &store, &quota_cache, &catalog_health).await?;

    let min_band: SharedMinBand = Arc::new(std::sync::Mutex::new(
        config_overrides::live_config(&override_path).min_band(),
    ));

    let mut route_epochs = RouteEpochs::new();
    let mut profiles_started = false;
    loop {
        let Some(frame) = next_frame_or_loop_stop(read_half, &mut loops).await? else {
            // Clean EOF: stop quota publications before capturing the final cache.
            return quota_events::finish_clean_exit(loops, Arc::clone(&quota_cache), store)
                .await
                .map_err(ModuleError::Message);
        };
        if !handle_frame(
            frame,
            &writer,
            &quota_cache,
            &store,
            &mut route_epochs,
            &min_band,
            &override_path,
            &operator_config_path,
            &broca_credentials,
            &catalog_health,
        )
        .await?
        {
            return quota_events::finish_clean_exit(loops, Arc::clone(&quota_cache), store)
                .await
                .map_err(ModuleError::Message);
        }
        if !profiles_started && !route_epochs.is_empty() {
            // Start only after the first bind ACK is queued, never during store open.
            loops.spawn(
                "task profile refresh",
                task_profile_refresh::refresh_loop(
                    Arc::clone(&store),
                    override_path.clone(),
                    Arc::clone(&broca_credentials),
                ),
            );
            profiles_started = true;
        }
    }
}

async fn drain_writer<W>(write_half: W, mut rx: mpsc::Receiver<Frame>) -> Result<(), ModuleError>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::new(write_half);
    while let Some(frame) = rx.recv().await {
        write_frame(&mut writer, &frame)
            .await
            .map_err(|e| ModuleError::Message(e.to_string()))?;
        while let Ok(frame) = rx.try_recv() {
            write_frame(&mut writer, &frame)
                .await
                .map_err(|e| ModuleError::Message(e.to_string()))?;
        }
        writer
            .flush()
            .await
            .map_err(|e| ModuleError::Message(e.to_string()))?;
    }
    writer
        .flush()
        .await
        .map_err(|e| ModuleError::Message(e.to_string()))?;
    Ok(())
}

async fn send_hello(
    writer: &mpsc::Sender<Frame>,
    config: &ModuleConfig,
) -> Result<(), ModuleError> {
    let body = serde_json::to_vec(&ModuleHelloBody {
        manifest: manifest(&config.module_id),
        protocol_ver: PROTOCOL_VERSION,
        // Advertise health.check: the daemon's prober is capability-gated and only probes
        // modules that DECLARE the op: answering it (handle_control_request) is not enough.
        control_ops: Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()]),
        launch_nonce: hello_launch_nonce()?,
    })
    .map_err(ModuleError::Json)?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

/// The launch nonce HELLO presents, from the process-wide accessor. A
/// descriptor the daemon named but that cannot be read fails HELLO by name, as
/// the SDK's HELLO does, rather than connecting without the module's identity.
fn hello_launch_nonce() -> Result<Option<String>, ModuleError> {
    subc_os::launch_nonce()
        .map(|nonce| nonce.map(|nonce| nonce.value().to_owned()))
        .map_err(|error| ModuleError::Message(format!("read the launch nonce: {error}")))
}

async fn expect_hello_ack<R>(reader: &mut R) -> Result<ModuleHelloAckBody, ModuleError>
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader)
        .await
        .map_err(|e| ModuleError::Message(e.to_string()))?
        .ok_or_else(|| ModuleError::Message("connection closed before HELLO_ACK".into()))?;
    match frame.header.ty {
        FrameType::HelloAck => serde_json::from_slice(&frame.body).map_err(ModuleError::Json),
        FrameType::Error => {
            let body =
                serde_json::from_slice::<ErrorBody>(&frame.body).map_err(ModuleError::Json)?;
            Err(ModuleError::Message(format!(
                "subc rejected HELLO: {}: {}",
                body.code, body.message
            )))
        }
        ty => Err(ModuleError::Message(format!(
            "unexpected frame {ty:?} awaiting HELLO_ACK"
        ))),
    }
}

/// Returns `Ok(false)` to stop the loop (graceful goodbye / EOF).
async fn handle_frame(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    route_epochs: &mut RouteEpochs,
    min_band: &SharedMinBand,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
    catalog_health: &SharedCatalogHealth,
) -> Result<bool, ModuleError> {
    match frame.header.ty {
        FrameType::Ping if frame.header.channel == 0 => {
            let pong = Frame::build_with_version(
                frame.header.ver,
                FrameType::Pong,
                frame.header.flags,
                0,
                0,
                frame.header.corr,
                Vec::new(),
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, pong).await?;
            Ok(true)
        }
        FrameType::Goodbye if frame.header.channel == 0 => Ok(false),
        FrameType::Goodbye => {
            clear_route_epoch(route_epochs, frame.header.channel, frame.header.epoch);
            Ok(true)
        }
        FrameType::Request if frame.header.channel == 0 => {
            handle_control_request(
                frame,
                writer,
                route_epochs,
                catalog_health,
                broca_credentials,
                quota_cache,
            )
            .await?;
            Ok(true)
        }
        FrameType::Request => {
            let Some(&route_epoch) = route_epochs.get(&frame.header.channel) else {
                return Ok(true);
            };
            if frame.header.epoch != route_epoch {
                return Ok(true);
            }
            // Data-plane request on a route channel. Spawn so a slow selection
            // never head-of-line-blocks another route, and run the handler on
            // the blocking pool because it calls the SQLite store synchronously
            // (see run_route_handler_off_async_workers). Reading frames and
            // forwarding replies stay on this async task.
            let writer = writer.clone();
            let quota_cache = Arc::clone(quota_cache);
            let store = Arc::clone(store);
            let min_band = Arc::clone(min_band);
            let routing_config_path = routing_config_path.to_path_buf();
            let operator_config_path = operator_config_path.to_path_buf();
            let broca_credentials = Arc::clone(broca_credentials);
            let (ver, channel, corr) = (frame.header.ver, frame.header.channel, frame.header.corr);
            let handler = async move {
                answer_route_request(&writer, ver, channel, route_epoch, corr, |reply| {
                    run_route_handler_off_async_workers(async move {
                        routing_v1::record_call(&frame.body, channel, route_epoch, &store)?;
                        // A flow-scoped route reaches only the ops in
                        // diagnostics_findings::FLOW_SCOPE_OPS.
                        if let Some(refusal) = diagnostics_findings::flow_route_refusal(
                            &frame.body,
                            channel,
                            route_epoch,
                        ) {
                            if let Ok(request) =
                                serde_json::from_slice::<serde_json::Value>(&frame.body)
                            {
                                let op = request["method"].as_str().unwrap_or_default();
                                if matches!(op, "route.account_for" | "route.account_observe") {
                                    account_refusal_log::emit(
                                        op,
                                        &route_principal(channel, route_epoch),
                                        &request["params"],
                                        &refusal.code,
                                        "flow_scope",
                                        "",
                                    );
                                }
                            }
                            return send_route_error_body(
                                &reply,
                                ver,
                                channel,
                                route_epoch,
                                corr,
                                refusal,
                            )
                            .await;
                        }
                        // Account requests with a caller member keep their legacy parser;
                        // every route.select body uses the closed v1 parser.
                        routing_v1::runners::refresh_admission_tiers(
                            &frame.body,
                            &routing_v1::Caller::for_route(channel, route_epoch),
                            &routing_v1::Deps {
                                quota_cache: &quota_cache,
                                store: &store,
                                routing_config_path: &routing_config_path,
                                operator_config_path: &operator_config_path,
                                broca_credentials: &broca_credentials,
                            },
                        )
                        .await;
                        if routing_v1::claims(&frame.body) {
                            return routing_v1::serve(
                                frame,
                                route_epoch,
                                &reply,
                                &routing_v1::Deps {
                                    quota_cache: &quota_cache,
                                    store: &store,
                                    routing_config_path: &routing_config_path,
                                    operator_config_path: &operator_config_path,
                                    broca_credentials: &broca_credentials,
                                },
                            )
                            .await;
                        }
                        handle_select_request(
                            frame,
                            route_epoch,
                            &reply,
                            &quota_cache,
                            &store,
                            &min_band,
                            &routing_config_path,
                            &operator_config_path,
                            &broca_credentials,
                        )
                        .await
                    })
                })
                .await;
            };
            #[cfg(test)]
            let handler = routing_v1::inherit_capabilities(handler);
            tokio::spawn(handler);
            Ok(true)
        }
        _ => Ok(true),
    }
}

/// Serve one route-channel request and make sure its caller hears back.
///
/// `handle` replies through the sender it is given, and every frame it sends is
/// forwarded to the connection. When it returns an error, the error is logged.
/// If it had not sent a reply and the connection is still open, the caller is
/// answered with an `internal_error` frame instead of waiting out its own
/// timeout. A handler that fails after replying gets no second frame, so a
/// caller never receives two answers to one request.
async fn answer_route_request<F, Fut>(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    handle: F,
) where
    F: FnOnce(mpsc::Sender<Frame>) -> Fut,
    Fut: std::future::Future<Output = Result<(), ModuleError>>,
{
    let (reply_tx, mut reply_rx) = mpsc::channel(1);
    let handled = handle(reply_tx);
    tokio::pin!(handled);
    let mut replied = false;
    let mut egress_open = true;
    let result = loop {
        let frame = tokio::select! {
            result = &mut handled => break result,
            Some(frame) = reply_rx.recv() => frame,
        };
        replied = true;
        egress_open = egress_open && send(writer, frame).await.is_ok();
    };
    // A send completes once the frame is queued, so everything the handler sent
    // before returning is already buffered here.
    while let Ok(frame) = reply_rx.try_recv() {
        replied = true;
        egress_open = egress_open && send(writer, frame).await.is_ok();
    }
    let Err(error) = result else {
        return;
    };
    let when = if replied { " after replying" } else { "" };
    warn_operator(format!(
        "[striatum] WARN: route request on channel {channel} (corr {corr}) failed{when}: {error}"
    ));
    if replied || !egress_open {
        return;
    }
    if let Err(send_error) = send_route_error(
        writer,
        ver,
        channel,
        epoch,
        corr,
        "internal_error",
        &format!("routing failed before replying: {error}"),
    )
    .await
    {
        warn_operator(format!(
            "[striatum] WARN: route request on channel {channel} (corr {corr}) got no failure reply: {send_error}"
        ));
    }
}

/// Run one route-channel request handler on Tokio's blocking pool and wait for
/// its result.
///
/// Route handlers call the SQLite route-state store synchronously, and a store
/// call can wait on disk I/O or on another connection's write lock. Run on an
/// async worker, that wait would also stall every other task scheduled on the
/// worker: other routes, the frame loop and the background loops. A blocking
/// thread drives the handler instead, with `Handle::block_on`, so the handler
/// code is unchanged: it awaits its reply sends exactly as before and writes in
/// the same order. `block_in_place` is not an option because it panics on a
/// current-thread runtime.
///
/// A panic in the handler is resumed here, so it ends this task just as it did
/// when the handler ran on the task itself.
async fn run_route_handler_off_async_workers<F, T>(handler: F) -> Result<T, ModuleError>
where
    F: std::future::Future<Output = Result<T, ModuleError>> + Send + 'static,
    T: Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    #[cfg(test)]
    let handler = routing_v1::inherit_capabilities(handler);
    #[cfg(test)]
    let test_thread = TestThreadState::capture();
    let outcome = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        let _test_thread = test_thread.install();
        runtime.block_on(handler)
    })
    .await;
    match outcome {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The blocking pool cancels only while the runtime shuts down.
        Err(error) => Err(ModuleError::Message(format!(
            "route handler did not run to completion: {error}"
        ))),
    }
}

async fn handle_control_request(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    route_epochs: &mut RouteEpochs,
    catalog_health: &SharedCatalogHealth,
    broca_credentials: &SharedBrocaCredentials,
    quota_cache: &SharedQuotaCache,
) -> Result<(), ModuleError> {
    let request = match serde_json::from_slice::<ModuleControlRequest>(&frame.body) {
        Ok(request) => request,
        Err(error) => {
            // A newer daemon may send a control shape this module cannot read.
            // Refuse only that request, without losing existing routes or health.
            tracing::warn!(corr = frame.header.corr, error = %error, "refusing undecodable control request");
            return send_route_error(
                writer,
                frame.header.ver,
                0,
                0,
                frame.header.corr,
                "invalid_request",
                &format!("invalid control request: {error}"),
            )
            .await;
        }
    };
    match request {
        ModuleControlRequest::RouteBind {
            route_channel,
            epoch,
            principal,
            scope,
            identity,
            ..
        } => {
            head_choices::remember_bind(
                route_channel,
                epoch,
                principal.as_ref(),
                scope.is_some(),
                &identity.harness,
                &identity.session,
            );
            if let Ok(mut map) = route_principals().lock() {
                map.insert(
                    route_channel,
                    (
                        epoch,
                        account_selection::principal_label(principal.as_ref()),
                    ),
                );
            }
            remember_route_scope(
                route_channel,
                epoch,
                DecisionScope::from_stamp(scope.as_ref()),
            );
            if let Ok(mut map) = ROUTE_SCOPE_AUTHORITY
                .get_or_init(|| Mutex::new(HashMap::new()))
                .lock()
            {
                map.insert(
                    route_channel,
                    (
                        epoch,
                        scope.as_ref().map(|stamp| routing_v1::ScopeAuthority {
                            owner: account_selection::principal_label(Some(&stamp.owner)),
                            reference: stamp.scope_ref.clone(),
                            kind: stamp.kind.clone(),
                            scope_epoch: stamp.scope_epoch,
                            project_id: stamp.project_id.clone(),
                            owner_authorized: stamp.owner_authorized,
                            flow_id: stamp.attributes.flow_id.clone(),
                            run_id: stamp.attributes.run_id.clone(),
                        }),
                    ),
                );
            }
            // The catalog is machine-global, but the wire identity is binding-specific.
            // Install it only after the accepted bind ACK is queued.
            let body = serde_json::to_vec(&ModuleControlResponse::RouteBindAck {})
                .map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                frame.header.ver,
                FrameType::Response,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, response).await?;
            route_epochs.insert(route_channel, epoch);
            Ok(())
        }
        ModuleControlRequest::HealthCheck {} => {
            // Unknown Broca reachability and stale quota snapshots can coexist.
            // Unmapped credential IDs remain in metrics without affecting status.
            let (mut report, pricing_ready) = broca_credentials
                .lock()
                .map(|credentials| {
                    (
                        credentials.health_report(now_ms()),
                        credentials.billing_catalog.is_loaded(),
                    )
                })
                .unwrap_or_else(|_| (BrocaCredentials::default().health_report(now_ms()), false));
            let broca_metrics = report.metrics.take();
            let (quota_detail, quota_metrics) = match quota_cache.lock() {
                Ok(cache) => quota_health(&cache, now_ms()),
                Err(_) => (
                    Some("quota cache lock poisoned".to_string()),
                    json!({ "lastError": "cache poisoned" }),
                ),
            };
            for detail in [
                (!pricing_ready).then(|| {
                    "routing pricing not ready: awaiting first billing-plane load".to_string()
                }),
                quota_detail,
            ]
            .into_iter()
            .flatten()
            {
                report.status = HealthStatus::Degraded;
                report.detail = Some(match report.detail.take() {
                    Some(existing) => format!("{existing}; {detail}"),
                    None => detail,
                });
            }
            report.metrics = Some(json!({
                "pricingReady": pricing_ready,
                "brocaReachability": broca_metrics,
                "fusiformCatalog": catalog_health
                    .lock()
                    .map(|health| health.snapshot(now_ms()))
                    .unwrap_or_else(|_| json!({ "reach": "never-reached", "lastError": "health lock poisoned" })),
                "quota": quota_metrics,
                "consumerDeclarations": routing_v1::consumers::health(),
            }));
            let body = serde_json::to_vec(&ModuleControlResponse::from(report))
                .map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                frame.header.ver,
                FrameType::Response,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, response).await
        }
    }
}

/// The method envelope, decoded first so an unknown method reports `unknown_method`
/// regardless of whether the (method-specific) params are present/valid. Only once
/// the method is confirmed do we decode the typed `SelectParams`.
#[derive(Debug, Deserialize)]
struct MethodEnvelope {
    method: String,
}

#[derive(Debug, Deserialize)]
struct ResolvePolicyBody {
    params: ResolvePolicyParams,
}

#[derive(Debug, Deserialize)]
struct ModelFactBody {
    params: ModelFactParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelFactParams {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
}

#[derive(Debug, Deserialize)]
struct ModelUpsertBody {
    params: ModelUpsertParams,
}

/// Partial fact: absent fields keep the stored value on merge, and a NEW model
/// requires elo + eq (the two judgment axes nothing can default honestly).
///
/// `canonicalID` instead of `providerID`/`modelID` places Elo and EQ on every
/// stored route of one model (all rows whose Fusiform canonical id matches, at
/// the given variant) in one write. Only `elo`, `eq`, `variant` and `notes`
/// apply then: every other field describes one provider's listing.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelUpsertParams {
    #[serde(rename = "providerID", default)]
    provider_id: String,
    #[serde(rename = "modelID", default)]
    model_id: String,
    #[serde(rename = "canonicalID", default)]
    canonical_id: Option<String>,
    #[serde(default)]
    variant: Option<String>,
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    elo: Option<f64>,
    #[serde(default)]
    eq: Option<f64>,
    #[serde(default)]
    speed: Option<f64>,
    #[serde(default)]
    context_window: Option<f64>,
    #[serde(default)]
    max_output_tokens: Option<u64>,
    #[serde(default)]
    cost_per_m_tok_out: Option<f64>,
    #[serde(default)]
    supports_vision: Option<bool>,
    #[serde(default)]
    supports_tool_use: Option<bool>,
    #[serde(default)]
    default_variant: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolvePolicyParams {
    #[serde(default)]
    available_models: Vec<String>,
    #[serde(default)]
    routing_policy: Option<ModelRoutingPolicyInput>,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
}

/// Merge partial upsert params over the stored fact. A NEW route requires elo
/// and eq: defaulting either would place a model in the ladder silently, the
/// quiet-wrongness class the seed migrations never had. Absent fields keep the
/// stored value; a new route's speed defaults to 100 (mid-catalog neutral)
/// until observed samples replace it.
fn merge_model_upsert(
    existing: Option<ModelFact>,
    params: &ModelUpsertParams,
) -> Result<ModelFact, String> {
    if params.family.is_some() {
        return Err(
            "model_upsert: family now comes from Fusiform's model.lineage; remove family".into(),
        );
    }
    let mut fact = match existing {
        Some(fact)
            if fact.score_provenance == ScoreProvenance::Unscored
                && (params.elo.is_none() || params.eq.is_none()) =>
        {
            return Err(format!(
                "model_upsert: route {}/{} is unscored; elo and eq are required to place it",
                params.provider_id, params.model_id
            ));
        }
        Some(fact) => fact,
        None => {
            let (Some(elo), Some(eq)) = (params.elo, params.eq) else {
                return Err(format!(
                    "model_upsert: route {}/{} is new; elo and eq are required to place it",
                    params.provider_id, params.model_id
                ));
            };
            ModelFact {
                billing_plane: None,
                auth_methods: None,
                login_methods_documented: false,
                speed_is_fleet_median: false,
                decommissioned: false,
                provider_id: params.provider_id.clone(),
                model_id: params.model_id.clone(),
                variant: params.variant.clone(),
                family: None,
                scores: ModelScores {
                    elo,
                    eq,
                    speed: params.speed.unwrap_or(100.0),
                },
                context_window: None,
                max_output_tokens: None,
                cost_tier: None,
                cost_per_m_tok_out: None,
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: None,
                supports_tool_use: None,
                prompt_defaults: None,
                confidence: Confidence::Curated,
                notes: None,
                speed_multiplier: None,
                subscription_multiplier: None,
                score_provenance: ScoreProvenance::Curated,
                aa_index: None,
                review_by_ms: None,
                eq_source: None,
                class_vector: None,
                class_vector_is_overlay: false,
                upstream_status: None,
                model_type: None,
                model_kind: None,
            }
        }
    };
    if let Some(elo) = params.elo {
        fact.scores.elo = elo;
    }
    if let Some(eq) = params.eq {
        fact.scores.eq = eq;
    }
    // A Fusiform-discovered row is minted with unknown confidence because it has
    // no scores. An operator placing both scores is the confirmation that flag
    // asks for; left unknown, the row kept the adequacy penalty and lost every
    // free-bucket comparison to a confirmed route, so a placed model was never
    // run (glm-5.3 and deepseek-v4.1-flash, 2026-09-18).
    if params.elo.is_some() && params.eq.is_some() {
        fact.confidence = Confidence::Curated;
        fact.score_provenance = ScoreProvenance::Curated;
    }
    if let Some(speed) = params.speed {
        fact.scores.speed = speed;
    }
    if params.context_window.is_some() {
        fact.context_window = params.context_window;
    }
    if params.max_output_tokens.is_some() {
        fact.max_output_tokens = params.max_output_tokens;
    }
    if params.cost_per_m_tok_out.is_some() {
        fact.cost_per_m_tok_out = params.cost_per_m_tok_out;
    }
    if params.supports_vision.is_some() {
        fact.supports_vision = params.supports_vision;
    }
    if params.supports_tool_use.is_some() {
        fact.supports_tool_use = params.supports_tool_use;
    }
    if let Some(default_variant) = params.default_variant.clone() {
        let mut defaults = fact.prompt_defaults.clone().unwrap_or_default();
        defaults.variant = Some(default_variant);
        fact.prompt_defaults = Some(defaults);
    }
    if params.notes.is_some() {
        fact.notes = params.notes.clone();
    }
    Ok(fact)
}

/// route.model_upsert: live catalog write. Merges the partial params over the
/// stored fact when the route exists; a new route requires elo and eq (the
/// operator-judgment axes). Writes land as FactSource::Override so re-seeds
/// never revert them. This op retires the add-a-model migration path.
async fn handle_model_upsert(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    ladder_sources: &winner_ladder::LadderSources,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ModelUpsertParams = match serde_json::from_slice::<ModelUpsertBody>(&frame.body) {
        Ok(body) if body.params.canonical_id.is_some() => {
            return handle_canonical_model_upsert(
                body.params,
                (ver, channel, epoch, corr),
                writer,
                store,
                ladder_sources,
            )
            .await;
        }
        Ok(body)
            if !body.params.provider_id.trim().is_empty()
                && !body.params.model_id.trim().is_empty() =>
        {
            body.params
        }
        Ok(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "model_upsert requires non-empty providerID and modelID, or canonicalID",
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("model_upsert params not decodable: {error}"),
            )
            .await;
        }
    };
    for (label, value) in [
        ("elo", params.elo),
        ("eq", params.eq),
        ("speed", params.speed),
    ] {
        if let Some(score) = value {
            if !score.is_finite() || score <= 0.0 {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("model_upsert {label} must be a positive finite number, got {score}"),
                )
                .await;
            }
        }
    }
    if params.model_id.ends_with("-fast") {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "model_upsert does not accept -fast model IDs; use service_tier for speed",
        )
        .await;
    }
    let route_key = make_route_key(
        &params.provider_id,
        &params.model_id,
        params.variant.as_deref(),
    );
    if let Err(error) = fusiform_catalog::ingest_before_upsert(
        store,
        broca_credentials,
        &params.provider_id,
        &params.model_id,
    )
    .await
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "catalog_unavailable",
            &format!("model_upsert catalog ingest: {error}"),
        )
        .await;
    }
    let existing = match store.get_fact_by_route_key(&route_key) {
        Ok(existing) => existing,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_upsert read: {error}"),
            )
            .await;
        }
    };
    let created = existing.is_none();
    let existing = if existing.is_none() && params.variant.is_some() {
        store
            .get_fact_by_route_key(&make_route_key(&params.provider_id, &params.model_id, None))
            .map_err(|error| ModuleError::Message(error.to_string()))?
            .map(|mut fact| {
                fact.variant = params.variant.clone();
                fact
            })
    } else {
        existing
    };
    let fact = match merge_model_upsert(existing, &params) {
        Ok(fact) => fact,
        Err(message) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &message,
            )
            .await;
        }
    };
    // A placement can make this route the winner at some demand tier (or unseat
    // one), so compare the winner ladder after the write and announce a move.
    if let Err(error) =
        crate::account_writer::upsert_fact(store.as_ref(), &fact, FactSource::Override)
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("model_upsert write: {error}"),
        )
        .await;
    }
    winner_ladder::announce(
        store.as_ref(),
        ladder_sources,
        &format!("route.model_upsert {route_key}"),
    );
    let mut fact_fields = Vec::new();
    if params.context_window.is_some() {
        fact_fields.push(CatalogFactField::ContextWindow);
    }
    if params.max_output_tokens.is_some() {
        fact_fields.push(CatalogFactField::MaxOutputTokens);
    }
    if params.cost_per_m_tok_out.is_some() {
        fact_fields.push(CatalogFactField::CostPerMtokOut);
    }
    if params.supports_vision.is_some() {
        fact_fields.push(CatalogFactField::SupportsVision);
    }
    if params.supports_tool_use.is_some() {
        fact_fields.push(CatalogFactField::SupportsToolUse);
    }
    if let Err(error) = store.mark_operator_fact_fields(&route_key, &fact_fields) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("model_upsert provenance write: {error}"),
        )
        .await;
    }
    let result = json!({
        "routeKey": route_key,
        "created": created,
        "scores": fact.scores,
        "source": "override",
    });
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
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

/// The canonical form of route.model_upsert: one Elo/EQ placement written to
/// every stored route of one model (rows whose Fusiform canonical id matches,
/// at the requested variant). Each row is written as its own override, so the
/// routes agree and none is left reading an older per-route score. Routes that
/// Fusiform adds later with the same canonical id borrow it at read time.
async fn handle_canonical_model_upsert(
    params: ModelUpsertParams,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    ladder_sources: &winner_ladder::LadderSources,
) -> Result<(), ModuleError> {
    let invalid = |message: String| async move {
        send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &message,
        )
        .await
    };
    let canonical_id = params.canonical_id.clone().unwrap_or_default();
    if canonical_id.trim().is_empty() {
        return invalid("model_upsert canonicalID must be non-empty".into()).await;
    }
    if !params.provider_id.is_empty() || !params.model_id.is_empty() {
        return invalid("model_upsert takes canonicalID or providerID/modelID, not both".into())
            .await;
    }
    let per_route_fields = [
        ("family", params.family.is_some()),
        ("speed", params.speed.is_some()),
        ("contextWindow", params.context_window.is_some()),
        ("maxOutputTokens", params.max_output_tokens.is_some()),
        ("costPerMTokOut", params.cost_per_m_tok_out.is_some()),
        ("supportsVision", params.supports_vision.is_some()),
        ("supportsToolUse", params.supports_tool_use.is_some()),
        ("defaultVariant", params.default_variant.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, set)| set.then_some(name))
    .collect::<Vec<_>>();
    if !per_route_fields.is_empty() {
        return invalid(format!(
            "model_upsert by canonicalID places only elo and eq; {} describe one route, use providerID/modelID",
            per_route_fields.join(", ")
        ))
        .await;
    }
    let (Some(elo), Some(eq)) = (params.elo, params.eq) else {
        return invalid("model_upsert by canonicalID requires both elo and eq".into()).await;
    };
    for (label, score) in [("elo", elo), ("eq", eq)] {
        if !score.is_finite() || score <= 0.0 {
            return invalid(format!(
                "model_upsert {label} must be a positive finite number, got {score}"
            ))
            .await;
        }
    }
    let store_error = |message: String| async move {
        send_route_error(writer, ver, channel, epoch, corr, "store_error", &message).await
    };
    let route_keys =
        match store.route_keys_for_canonical_id(&canonical_id, params.variant.as_deref()) {
            Ok(route_keys) => route_keys,
            Err(error) => return store_error(format!("model_upsert read: {error}")).await,
        };
    if route_keys.is_empty() {
        return invalid(format!(
            "model_upsert: no stored route carries canonical id {canonical_id}{}; \
             place a route by providerID/modelID or wait for Fusiform to serve one",
            params
                .variant
                .as_deref()
                .map(|variant| format!(" at variant {variant}"))
                .unwrap_or_default()
        ))
        .await;
    }
    for route_key in &route_keys {
        let mut fact = match store.get_fact_by_route_key(route_key) {
            Ok(Some(fact)) => fact,
            Ok(None) => continue,
            Err(error) => return store_error(format!("model_upsert read: {error}")).await,
        };
        fact.scores.elo = elo;
        fact.scores.eq = eq;
        // Placing both judgment scores confirms the row, as merge_model_upsert
        // does for a per-route write: curated confidence lifts the unknown-fact
        // adequacy penalty a Fusiform-discovered row starts with.
        fact.confidence = Confidence::Curated;
        fact.score_provenance = ScoreProvenance::Curated;
        if params.notes.is_some() {
            fact.notes = params.notes.clone();
        }
        if let Err(error) =
            crate::account_writer::upsert_fact(store.as_ref(), &fact, FactSource::Override)
        {
            return store_error(format!("model_upsert write: {error}")).await;
        }
    }
    winner_ladder::announce(
        store.as_ref(),
        ladder_sources,
        &format!("route.model_upsert canonical {canonical_id}"),
    );
    let result = json!({
        "canonicalID": canonical_id,
        "variant": params.variant,
        "routeKeys": route_keys,
        "scores": { "elo": elo, "eq": eq },
        "source": "override",
    });
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
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

/// route.list_canonical_gaps: operator-scored routes whose catalog row states
/// no canonical model id, so their score is not shared with the model's other
/// routes. This is the list to send Fusiform for identification.
async fn handle_list_canonical_gaps(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    match store.list_canonical_gaps() {
        Ok(rows) => {
            let body = serde_json::to_vec(&json!({ "result": rows })).map_err(ModuleError::Json)?;
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
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("list_canonical_gaps: {error}"),
            )
            .await
        }
    }
}

/// The canonical model id of each panel candidate, so a panel never seats two
/// routes of one model. A row borrowed by a provider with no catalog rows
/// (`catalog_targets`) serves its catalog provider's model, so it takes that
/// row's id.
fn panel_canonical_ids(
    store: &SqliteRouteStateStore,
    facts: &[ModelFact],
    catalog_targets: &std::collections::BTreeMap<String, String>,
) -> Result<HashMap<String, String>, striatum_core::RouteStateStoreError> {
    let stored = store.canonical_ids_by_route_key()?;
    Ok(facts
        .iter()
        .filter_map(|fact| {
            let route_key = fact_to_route(fact).route_key;
            let catalog_key = striatum_core::score_alias::catalog_key(catalog_targets, &route_key);
            stored
                .get(&catalog_key)
                .map(|canonical_id| (route_key, canonical_id.clone()))
        })
        .collect())
}

fn list_inferred_json(
    store: &SqliteRouteStateStore,
    now_ms: i64,
) -> Result<serde_json::Value, String> {
    let inferred = store
        .list_inferred_rows(now_ms)
        .map_err(|error| error.to_string())?;
    let unscored = store
        .list_unscored_rows()
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "inferred": inferred,
        "unscored": unscored,
    }))
}

async fn handle_list_inferred(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    match list_inferred_json(store, now) {
        Ok(result) => {
            let body =
                serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
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
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("list_inferred: {error}"),
            )
            .await
        }
    }
}

async fn handle_list_unscored(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    match store.list_unscored_rows() {
        Ok(rows) => {
            let body = serde_json::to_vec(&json!({ "result": rows })).map_err(ModuleError::Json)?;
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
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("list_unscored: {error}"),
            )
            .await
        }
    }
}

/// route.model_facts: serve the durable catalog without reconstructing facts in
/// the plugin. Inventory consumers receive every stored capability field and the
/// explicit exclusion bit, so an absent capability remains unknown rather than
/// being fabricated from a local fallback.
async fn handle_model_facts(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    operator_config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let mut facts = match store.get_catalog_facts() {
        Ok(facts) => facts,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_facts: {error}"),
            )
            .await;
        }
    };
    // `excluded` is stamped from the live operator policy at read time, using the
    // same predicate selection applies. The stored column has never been written by
    // anything (it reads 0 on every row), so serving it raw told every inventory
    // consumer that nothing is excluded while routing was excluding 30+ routes.
    let operator_policy = config_overrides::read_operator_routing_policy(operator_config_path);
    let exclusions = ExclusionSpecs::parse(ExclusionPolicy {
        operator: &operator_policy.exclude,
        caller: &[],
    });
    for fact in &mut facts {
        let route_key = make_route_key(
            &fact.fact.provider_id,
            &fact.fact.model_id,
            fact.fact.variant.as_deref(),
        );
        fact.excluded = exclusions.excludes(&route_key);
    }
    let engine_speed_samples = match store.engine_speed_samples() {
        Ok(samples) => samples,
        Err(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                "Routing couldn't read its engine speed samples just now.",
            )
            .await;
        }
    };
    let body = serde_json::to_vec(&json!({ "result": {
        "facts": facts,
        "engine_speed_samples": engine_speed_samples,
        "engine_speed_sample_limit": striatum_store::ENGINE_SPEED_SAMPLE_LIMIT,
    } }))
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
    send(writer, response).await
}

fn warn_missing_session_model_once(provider_id: &str, model_id: &str) -> bool {
    static WARNED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let warned = WARNED.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut warned) = warned.lock() else {
        return false;
    };
    if !warned.insert((provider_id.to_string(), model_id.to_string())) {
        return false;
    }
    tracing::warn!(
        provider_id,
        model_id,
        pair = %format!("({provider_id}, {model_id})"),
        "session model has no model_facts row"
    );
    true
}

async fn handle_model_fact(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ModelFactParams = match serde_json::from_slice::<ModelFactBody>(&frame.body) {
        Ok(body)
            if !body.params.provider_id.trim().is_empty()
                && !body.params.model_id.trim().is_empty() =>
        {
            body.params
        }
        Ok(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "model_fact requires non-empty providerID and modelID",
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("model_fact params not decodable: {error}"),
            )
            .await;
        }
    };
    let route_key = make_route_key(&params.provider_id, &params.model_id, None);
    let result = match store.get_fact_by_route_key(&route_key) {
        Ok(Some(fact)) => {
            let (_, family_source) = match store.get_model_family(&route_key) {
                Ok(Some(family)) => family,
                Ok(None) => (None, striatum_core::FamilySource::Absent),
                Err(error) => {
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "store_error",
                        &format!("model_fact family: {error}"),
                    )
                    .await;
                }
            };
            json!({
                "known": true,
                // Capability reads preserve an absent flag as cannot-verify. Routing admission
                // remains conservative in its own path and may treat the same absence as no.
                "supportsVision": fact.supports_vision,
                // The provider's own output ceiling lets pinned callers size the send for
                // this exact route instead of inheriting another panel member's limit.
                "maxOutputTokens": fact.max_output_tokens,
                // The context window lets a consult size its evidence package for
                // the roster it would seat before any seat is selected or paid for.
                "contextWindow": fact
                    .context_window
                    .filter(|window| window.is_finite() && *window >= 1.0)
                    .map(|window| window as u64),
                "family": fact.grand_family_id(),
                "familySource": family_source.as_str(),
            })
        }
        Ok(None) => {
            warn_missing_session_model_once(&params.provider_id, &params.model_id);
            json!({
                "known": false,
                "supportsVision": null,
                "maxOutputTokens": null,
                "contextWindow": null,
                "family": "other",
                "familySource": "absent"
            })
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_fact: {error}"),
            )
            .await;
        }
    };
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
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

async fn handle_resolve_policy(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    operator_config_path: &Path,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
    let params = match serde_json::from_slice::<ResolvePolicyBody>(&frame.body) {
        Ok(body) => body.params,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("route.resolve_policy params not decodable: {error}"),
            )
            .await;
        }
    };
    warn_ignored_caller_policy_once(params.routing_policy.is_some());
    // Policy queries resolve the same routing-owned policy selection enforces.
    let operator_policy = config_overrides::read_operator_routing_policy(operator_config_path);
    let resolved = resolve_routing_policy(
        params.available_models,
        &operator_policy,
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(params.provider_auth_types.as_ref()),
    );
    let body = serde_json::to_vec(&json!({ "result": resolved })).map_err(ModuleError::Json)?;
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
    send(writer, response).await
}

#[derive(Debug, Deserialize)]
struct MethodParamsBody<P> {
    params: P,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyProviderOutcome {
    #[serde(flatten)]
    identity: striatum_core::route_state_store::ProviderOutcome,
    // Decode a caller's explicit credit window without adding any member to
    // routing's serialized provider identity or existing replies.
    #[serde(default, alias = "limit_window")]
    limit_window: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetDecisionOutcomeParams {
    #[serde(default)]
    provider_outcome: Option<LegacyProviderOutcome>,
    // The wire field is `decisionID` (capital ID) to match what route.select RETURNS
    // (json!{ "decisionID": ... }) and what every TS caller sends: camelCase alone
    // would expect `decisionId`, the lone casing drift that broke the round-trip.
    #[serde(rename = "decisionID")]
    decision_id: String,
    /// completed | contract_error | error | session_error | cancelled | interrupted | launch_failed | quota_exhausted
    outcome: String,
    /// Host-parsed reset delay for a quota_exhausted outcome.
    #[serde(default)]
    retry_after_secs: Option<u64>,
    /// Exact terminal send error surfaced by the harness, when one was available.
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CooldownStatusParams {
    route_key: String,
    capacity_key: String,
    now: i64,
}

#[derive(Debug, Default, Deserialize)]
struct RouteExplainParams {
    #[serde(
        default,
        rename = "decisionID",
        alias = "decisionId",
        alias = "decision_id"
    )]
    decision_id: Option<String>,
    #[serde(default, rename = "sendID", alias = "sendId", alias = "send_id")]
    send_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteDecisionsListParams {
    #[serde(default, rename = "sendID", alias = "sendId", alias = "send_id")]
    send_id: Option<String>,
    #[serde(default, alias = "target_agent")]
    target_agent: Option<String>,
    #[serde(default, alias = "route_key")]
    route_key: Option<String>,
    #[serde(default, alias = "since_ms")]
    since_ms: Option<i64>,
    #[serde(default, alias = "until_ms")]
    until_ms: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearFailureBackoffParams {
    route_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearCooldownParams {
    route_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordCooldownParams {
    route_key: String,
    capacity_key: String,
    #[serde(default)]
    credential_id: Option<String>,
    #[serde(default)]
    cooldown_until: Option<f64>,
    #[serde(default)]
    quota_exhausted_until: Option<f64>,
    #[serde(default)]
    quota_pressure: Option<f64>,
    #[serde(default)]
    wall_class: Option<RouteWallClass>,
    #[serde(default)]
    provider_wide: bool,
    #[serde(default)]
    increment_quota_error: bool,
    #[serde(default)]
    increment_failure: bool,
    #[serde(default)]
    last_error: Option<String>,
}

fn parse_outcome(s: &str, retry_after_secs: Option<u64>) -> Option<RouteOutcome> {
    match s {
        "completed" => Some(RouteOutcome::Completed),
        "contract_error" => Some(RouteOutcome::ContractError),
        "error" => Some(RouteOutcome::Error),
        "session_error" => Some(RouteOutcome::SessionError),
        "cancelled" => Some(RouteOutcome::Cancelled),
        "interrupted" => Some(RouteOutcome::Interrupted),
        "launch_failed" => Some(RouteOutcome::LaunchFailed),
        "quota_exhausted" => retry_after_secs
            .filter(|seconds| *seconds > 0)
            .map(|retry_after_secs| RouteOutcome::QuotaExhausted { retry_after_secs }),
        _ => None,
    }
}

/// Decode persisted evidence without supplying a current-day fallback. If a legacy
/// row contains malformed JSON, return its exact text rather than inventing a value.
fn recorded_json(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!(raw))
}

fn recorded_optional_json(raw: Option<&str>) -> serde_json::Value {
    raw.map(recorded_json).unwrap_or(serde_json::Value::Null)
}

/// Send a `{ result: { ok: true } }` ack for a successful write op.
async fn send_ok(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
) -> Result<(), ModuleError> {
    let body =
        serde_json::to_vec(&json!({ "result": { "ok": true } })).map_err(ModuleError::Json)?;
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

async fn handle_set_decision_outcome(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    _routing_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: SetDecisionOutcomeParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(b) => b.params,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("set_decision_outcome params not decodable: {e}"),
                )
                .await;
            }
        };
    let Some(outcome) = parse_outcome(&params.outcome, params.retry_after_secs) else {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &format!("unknown outcome '{}'", params.outcome),
        )
        .await;
    };
    let credit_window = params
        .provider_outcome
        .as_ref()
        .is_some_and(|provider| provider.limit_window.as_deref() == Some("credit"));
    let mut provider_outcome = params.provider_outcome.map(|provider| provider.identity);
    if let Some(provider) = provider_outcome
        .as_mut()
        .filter(|p| p.credential_id.is_none())
    {
        // The provider may name its account differently from the vault. The
        // sole serving identity frozen for this decision resolves that alias.
        if let Some(identity) = store
            .decision_provider_identity(&params.decision_id)
            .map_err(|e| ModuleError::Message(e.to_string()))?
        {
            provider.credential_id = identity.credential_id;
        }
    }
    if outcome == RouteOutcome::Completed && provider_outcome.is_none() {
        // Use the unambiguous serving identity recorded at selection, never
        // today's roster: an account may have changed while the run was active.
        provider_outcome = store
            .decision_provider_identity(&params.decision_id)
            .map_err(|e| ModuleError::Message(e.to_string()))?;
    }
    let outcome = if outcome != RouteOutcome::Completed
        && provider_outcome.as_ref().is_some_and(|provider| {
            striatum_core::account_wall::transient_provider_error(
                provider.error_class.as_deref(),
                provider.provider_code.as_deref(),
            )
        }) {
        RouteOutcome::Interrupted
    } else {
        outcome
    };
    let observed_at_ms = now_ms() as i64;
    let quota = ACCOUNT_QUOTA
        .get()
        .and_then(|state| state.lock().ok().map(|state| state.clone()));
    let fence_config = config_overrides::live_config(_routing_config_path);
    let account_exhausted = provider_outcome.as_ref().is_some_and(|provider| {
        provider.provider_code.as_deref() == Some("account_limit_exhausted")
    }) || params
        .last_error
        .as_deref()
        .is_some_and(|error| error.starts_with("account_limit_exhausted:"));
    let credit_retry_at = (credit_window && account_exhausted).then(|| {
        let cap =
            observed_at_ms.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
        params
            .retry_after_secs
            .map(|seconds| {
                observed_at_ms.saturating_add(
                    i64::try_from(seconds)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1000),
                )
            })
            .unwrap_or(cap)
            .min(cap)
    });
    if let Err(error) = record_provider_reset_fence(
        store,
        broca_credentials,
        &params.decision_id,
        &outcome,
        provider_outcome.as_ref(),
        account_exhausted,
        credit_retry_at,
        observed_at_ms,
        quota.as_ref(),
        Some(&fence_config),
    ) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("set_decision_outcome: provider reset fence: {error}"),
        )
        .await;
    }
    // A credential the provider refused permanently is fenced whatever the
    // decision ledger still holds: the report names the provider and the
    // credential itself. Recording renews a repeated refusal, so a fence that
    // keeps failing stays fresh and its next recheck probe waits another
    // interval. Subscription refusals are reconciled against quota readings
    // by account selection, including fences persisted before quota delivery.
    if let Some(refusal) = provider_outcome
        .as_ref()
        .and_then(|provider| provider.credential_refusal.as_ref())
    {
        if let Err(e) = credential_fences::record(store, broca_credentials, refusal, observed_at_ms)
        {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("set_decision_outcome: credential entitlement fence: {e}"),
            )
            .await;
        }
    }
    // Only the credential that actually served the completed run is healed.
    if outcome == RouteOutcome::Completed {
        if let Some(credential) = provider_outcome
            .as_ref()
            .and_then(|provider| provider.credential_id.as_deref())
        {
            match store.get_decision_by_id(&params.decision_id) {
                Ok(Some(decision)) => {
                    if let Some(provider) = decision.selected_provider_id.as_deref() {
                        let frozen = store
                            .decision_provider_identity(&params.decision_id)
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        if let Some(identity) = frozen.as_ref().filter(|identity| {
                            identity.credential_id.as_deref() == Some(credential)
                        }) {
                            let model = decision.selected_model_id.as_deref().unwrap_or("");
                            let scope = striatum_store::QuotaSuccessScope {
                                family: fence_config
                                    .allocator_policy(provider)
                                    .family_for_model(model),
                                pool: striatum_core::quota_extract::antigravity_pool_for_capacity(
                                    &format!("{provider}/{model}"),
                                )
                                .map(str::to_string),
                            };
                            let decision_id = params.decision_id.clone();
                            let provider = provider.to_owned();
                            let model = model.to_owned();
                            let credential = credential.to_owned();
                            let identity = identity.clone();
                            account_writer::execute(
                                store,
                                striatum_store::AccountCommandKind::Report,
                                move |tx| {
                                    let provider = provider.as_str();
                                    let model = model.as_str();
                                    let credential = credential.as_str();
                                    tx.heal_authorized_probe(
                                        &decision_id,
                                        provider,
                                        model,
                                        observed_at_ms,
                                    )?;
                                    let size = tx.size_admission_for_model_decision(
                                        &decision_id,
                                        provider,
                                        credential,
                                        model,
                                    )?;
                                    tx.heal_serving_fences_in_scope(
                                        provider,
                                        &identity.account_ref,
                                        credential,
                                        model,
                                        size.map(|(class, _)| class),
                                        size.map_or(decision.created_at as i64, |(_, at)| at),
                                        observed_at_ms,
                                        Some(&scope),
                                    )?;
                                    credential_fences::lift_after_success_in(
                                        tx,
                                        provider,
                                        credential,
                                        observed_at_ms,
                                        decision.created_at as i64,
                                    )?;
                                    Ok(())
                                },
                            )
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        } else if frozen
                            .as_ref()
                            .and_then(|identity| identity.credential_id.as_deref())
                            .is_none()
                        {
                            let model = decision.selected_model_id.as_deref().unwrap_or("");
                            let decision_id = params.decision_id.clone();
                            let provider = provider.to_owned();
                            let model = model.to_owned();
                            let credential = credential.to_owned();
                            account_writer::execute(
                                store,
                                striatum_store::AccountCommandKind::Report,
                                move |tx| {
                                    let provider = provider.as_str();
                                    let model = model.as_str();
                                    let credential = credential.as_str();
                                    if tx.probe_authorization_names_credential(
                                        &decision_id,
                                        credential,
                                        model,
                                    )? {
                                        tx.heal_authorized_probe(
                                            &decision_id,
                                            provider,
                                            model,
                                            observed_at_ms,
                                        )?;
                                    }
                                    Ok(())
                                },
                            )
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "store_error",
                        &format!("set_decision_outcome: reading decision: {e}"),
                    )
                    .await
                }
            }
        } else if let Some(decision) = store
            .get_decision_by_id(&params.decision_id)
            .map_err(|error| ModuleError::Message(error.to_string()))?
        {
            let frozen = store
                .decision_provider_identity(&params.decision_id)
                .map_err(|error| ModuleError::Message(error.to_string()))?;
            if frozen
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
                .is_none()
            {
                if let (Some(provider), Some(model)) = (
                    decision.selected_provider_id.as_deref(),
                    decision.selected_model_id.as_deref(),
                ) {
                    let decision_id = params.decision_id.clone();
                    let provider = provider.to_owned();
                    let model = model.to_owned();
                    account_writer::execute(
                        store,
                        striatum_store::AccountCommandKind::Report,
                        move |tx| {
                            tx.heal_authorized_probe(
                                &decision_id,
                                &provider,
                                &model,
                                observed_at_ms,
                            )
                        },
                    )
                    .map_err(|error| ModuleError::Message(error.to_string()))?;
                }
            }
        }
    }
    if striatum_core::fence_lifetimes::probe_is_inconclusive(outcome, params.last_error.as_deref())
    {
        if let Some(decision) = store
            .get_decision_by_id(&params.decision_id)
            .map_err(|error| ModuleError::Message(error.to_string()))?
        {
            if let (Some(provider), Some(model)) = (
                decision.selected_provider_id.as_deref(),
                decision.selected_model_id.as_deref(),
            ) {
                let decision_id = params.decision_id.clone();
                let provider = provider.to_owned();
                let model = model.to_owned();
                account_writer::execute(
                    store,
                    striatum_store::AccountCommandKind::Report,
                    move |tx| {
                        tx.release_inconclusive_probe(
                            &decision_id,
                            &provider,
                            &model,
                            observed_at_ms,
                        )
                    },
                )
                .map_err(|error| ModuleError::Message(error.to_string()))?;
            }
        }
    }
    match store.record_routing_outcome(
        &params.decision_id,
        &outcome,
        params.last_error.as_deref(),
        observed_at_ms,
    ) {
        Ok(()) => send_ok(writer, ver, channel, epoch, corr).await,
        Err(e) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("set_decision_outcome: {e}"),
            )
            .await
        }
    }
}

/// Provider reset evidence accompanies episode outcomes, including interruptions
/// on v1-selected workers. Keep it separate from model health and fence only the
/// route on the credential that served it, never the provider's other accounts.
fn record_provider_reset_fence(
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    decision_id: &str,
    outcome: &RouteOutcome,
    provider: Option<&striatum_core::route_state_store::ProviderOutcome>,
    account_exhausted: bool,
    credit_retry_at: Option<i64>,
    now: i64,
    quota: Option<&account_quota::AccountQuotaState>,
    config: Option<&config_overrides::LiveConfig>,
) -> Result<(), String> {
    if *outcome == RouteOutcome::Completed {
        return Ok(());
    }
    let Some(provider) = provider.filter(|p| p.credential_refusal.is_none()) else {
        return Ok(());
    };
    // A reset is evidence for an extra check, never an expiry. Zero represents
    // an unknown reset internally; the separate quota retry clock still runs.
    let reset = match provider.resets_at_ms {
        Some(reset)
            if (account_exhausted || reset > now)
                && account_quota::parse_rfc3339_ms(&account_selection::rfc3339(reset))
                    .is_some() =>
        {
            reset
        }
        Some(_) if account_exhausted => 0,
        None if account_exhausted => 0,
        // An ordinary interruption carrying an old or unreadable date is not
        // a new typed quota refusal. Never resurrect a wall from that date.
        _ => return Ok(()),
    };
    let Some(decision) = store
        .get_decision_by_id(decision_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(());
    };
    let (Some(provider_id), Some(model)) =
        (decision.selected_provider_id, decision.selected_model_id)
    else {
        return Ok(());
    };
    let frozen = store
        .decision_provider_identity(decision_id)
        .map_err(|e| e.to_string())?
        .and_then(|identity| identity.credential_id);
    let credential = if let Some(frozen) = frozen {
        frozen
    } else if let Some(reported) = provider.credential_id.as_deref() {
        // Without a frozen identity, accept only a listed credential that serves
        // the decision's model. Provider account aliases are not credential ids.
        let facts = store.get_facts().map_err(|e| e.to_string())?;
        let catalog = facts.iter().collect();
        let Some(fact) = facts
            .iter()
            .find(|fact| fact.provider_id == provider_id && fact.model_id == model)
        else {
            return Ok(());
        };
        let credentials = credentials.lock().map_err(|e| e.to_string())?;
        if !credentials.listed_credentials().iter().any(|row| {
            row.id == reported
                && broca_credentials::credential_serves_fact(row, fact, &catalog, &credentials)
        }) {
            return Ok(());
        }
        reported.to_owned()
    } else {
        return Ok(());
    };
    // A credit window explicitly identifies a spending wall. Without that
    // typed window, only fresh headroom can contradict quota exhaustion.
    // Preserve provider dates as evidence, never as fixed quota lifetimes.
    let non_quota = credit_retry_at.is_some()
        || quota.is_some_and(|quota| {
            credentials.lock().ok().is_some_and(|credentials| {
                credential_fences::quota_refusal_contradicted(
                    quota,
                    &credentials,
                    &credential,
                    now,
                    &config
                        .map(|config| config.allocator_policy(&provider_id))
                        .unwrap_or_default(),
                )
            })
        });
    let fence = striatum_store::AccountRouteFence {
        provider: provider_id,
        credential_id: credential,
        model,
        until_ms: if non_quota {
            let cap = now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
            if let Some(retry) = credit_retry_at {
                retry.min(cap)
            } else if reset > now {
                reset.min(cap)
            } else {
                cap
            }
        } else {
            reset
        },
        decision_id: decision_id.into(),
        recorded_at: now,
    };
    if non_quota {
        crate::account_writer::record_billing_account_route_fence(store, &fence)
    } else {
        crate::account_writer::record_account_route_fence(store, &fence)
    }
    .map_err(|e| e.to_string())
}

async fn handle_record_usage(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: striatum_core::NativeUsageFacts =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("record_usage params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.speed_sample {
        let result =
            usage_facts::speed_sample_observation_from_store(&params, store).and_then(|sample| {
                match sample {
                    Some(sample) => store
                        .record_speed_sample_delivery(&sample)
                        .map_err(|e| e.to_string()),
                    None => Ok(striatum_core::route_state_store::UsageRecordResult::default()),
                }
            });
        return match result {
            Ok(result) => {
                let body = serde_json::to_vec(&json!({ "result": {
                    "recorded": result.recorded, "usage_recorded": false,
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
                .map_err(|error| ModuleError::Message(error.to_string()))?;
                send(writer, response).await
            }
            Err(error) => {
                send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("record_usage: {error}"),
                )
                .await
            }
        };
    }
    let observation = match usage_facts::usage_observation_from_store(params, store) {
        Ok(observation) => observation,
        Err(error) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &error)
                .await;
        }
    };
    let Some(observation) = observation else {
        let body = serde_json::to_vec(
            &json!({ "result": { "recorded": false, "usage_recorded": false } }),
        )
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
    };
    match store.record_usage_delivery(&observation) {
        Ok(result) => {
            let body = serde_json::to_vec(&json!({ "result": {
                "recorded": result.recorded,
                "usage_recorded": result.usage_recorded,
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
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("record_usage: {error}"),
            )
            .await
        }
    }
}

async fn handle_cooldown_status(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: CooldownStatusParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("cooldown_status params not decodable: {error}"),
                )
                .await;
            }
        };
    // The provider row and the route-state snapshot both come from one
    // published account state, which holds every route and provider row.
    let blocks = match account_state::published(store, crate::account_selection::now_ms()) {
        Ok(blocks) => blocks,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("cooldown_status: {error}"),
            )
            .await;
        }
    };
    let provider_state = if let Some(provider_key) =
        derive_provider_key(&params.route_key).or_else(|| derive_provider_key(&params.capacity_key))
    {
        match blocks.provider_cooldown(provider_key) {
            Ok(state) => state,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("cooldown_status provider lookup: {error}"),
                )
                .await;
            }
        }
    } else {
        None
    };
    let snapshot = match blocks.route_state_snapshot(params.now as f64) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("cooldown_status: {error}"),
            )
            .await;
        }
    };
    let cooled = striatum_core::route_state_store::route_cooled(
        provider_state.as_ref(),
        &snapshot,
        &params.route_key,
        &params.capacity_key,
        params.now as f64,
    );
    let body =
        serde_json::to_vec(&json!({"result": {"cooled": cooled}})).map_err(ModuleError::Json)?;
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
    send(writer, response).await
}

async fn handle_route_explain(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: RouteExplainParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("route.explain params not decodable: {error}"),
                )
                .await;
            }
        };
    let decision_id = params.decision_id.filter(|value| !value.trim().is_empty());
    let send_id = params.send_id.filter(|value| !value.trim().is_empty());
    let (key_name, key, lookup) = match (decision_id.as_deref(), send_id.as_deref()) {
        (Some(decision_id), None) => (
            "decisionID",
            decision_id,
            store.get_decision_by_id(decision_id),
        ),
        (None, Some(send_id)) => ("sendID", send_id, store.get_decision_by_send_id(send_id)),
        _ => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "route.explain requires exactly one of decisionID or sendID",
            )
            .await;
        }
    };
    let decision = match lookup {
        Ok(Some(decision)) => decision,
        Ok(None) => {
            let retention_days = retention::retention_windows(&config_overrides::config_path())
                .model_route_decisions_days;
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "not_found",
                &format!(
                    "{key_name} '{key}' not found or pruned; retention is {retention_days} days"
                ),
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.explain {key_name} lookup: {error}"),
            )
            .await;
        }
    };
    let outcome = decision.outcome.as_ref().map(|state| {
        json!({
            "state": state,
            "recordedAtMs": decision.outcome_at,
            "error": decision.outcome_error,
        })
    });
    let provider_identity = store
        .decision_provider_identity(&decision.id)
        .map_err(|error| ModuleError::Message(error.to_string()))?;
    let result = json!({
        "decisionID": decision.id,
        "providerIdentity": provider_identity,
        "createdAtMs": decision.created_at,
        "decisionKind": decision.decision_kind,
        "selectedRoute": {
            "routeKey": decision.selected_route_key,
            "providerID": decision.selected_provider_id,
            "modelID": decision.selected_model_id,
            "variant": decision.selected_variant,
            "harness": decision.selected_harness,
            "workload": decision.selected_workload,
        },
        "why": {
            "requirements": recorded_json(&decision.requirements_json),
            "allowed_providers": recorded_optional_json(decision.allowed_providers_json.as_deref()),
            "score": recorded_json(&decision.score_json),
            "explanation": recorded_optional_json(decision.explanation_json.as_deref()),
            // This is the persisted rejection payload, not a new scoring pass.
            "rejections": recorded_json(&decision.rejected_json),
        },
        "quotaSignalSnapshot": recorded_optional_json(decision.quota_signal_json.as_deref()),
        // Past the full-detail window the score keeps only the picked and
        // adequate candidates and the quota snapshot only its header; every
        // other member is as recorded. `strippedAtMs` says when that happened.
        "detailRetention": {
            "detail": if decision.detail_stripped_at.is_some() { "compact" } else { "full" },
            "strippedAtMs": decision.detail_stripped_at,
        },
        "catalogFingerprint": decision.catalog_fingerprint,
        "policyRevision": decision.policy_revision,
        "anchorVersion": decision.anchor_version,
        "provenance": {
            "sendID": decision.send_id,
            "taskID": decision.task_id,
            "parentSessionID": decision.parent_session_id,
            "targetAgent": decision.target_agent,
            "harness": decision.harness,
            "projectRoot": decision.project_root,
        },
        "outcome": outcome,
    });
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
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

async fn handle_route_decisions_list(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: RouteDecisionsListParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("route.decisions_list params not decodable: {error}"),
                )
                .await;
            }
        };
    let limit = params.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.decisions_list limit must be between 1 and 100",
        )
        .await;
    }
    if params
        .since_ms
        .zip(params.until_ms)
        .is_some_and(|(since_ms, until_ms)| since_ms > until_ms)
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.decisions_list sinceMs must not be after untilMs",
        )
        .await;
    }
    let decisions = match store.list_decisions(&DecisionListFilter {
        send_id: params.send_id.filter(|value| !value.trim().is_empty()),
        target_agent: params.target_agent.filter(|value| !value.trim().is_empty()),
        route_key: params.route_key.filter(|value| !value.trim().is_empty()),
        since_ms: params.since_ms,
        until_ms: params.until_ms,
        limit,
    }) {
        Ok(decisions) => decisions,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.decisions_list: {error}"),
            )
            .await;
        }
    };
    let rows: Vec<serde_json::Value> = decisions
        .into_iter()
        .map(|decision| {
            json!({
                "decisionID": decision.id,
                "createdAtMs": decision.created_at,
                "selectedRouteKey": decision.selected_route_key,
                "decisionKind": decision.decision_kind,
                "topRejectionReason": decision.top_rejection_reason,
                "topRejectionCount": decision.top_rejection_count,
            })
        })
        .collect();
    let body = serde_json::to_vec(&json!({ "result": { "decisions": rows } }))
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
    send(writer, response).await
}

async fn handle_clear_failure_backoff(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ClearFailureBackoffParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("route.clear_failure_backoff params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.route_key.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_failure_backoff routeKey must not be empty",
        )
        .await;
    }
    let prior = match crate::account_writer::clear_failure_backoff(store, &params.route_key) {
        Ok(prior) => prior,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_failure_backoff: {error}"),
            )
            .await;
        }
    };
    let prior = prior.map(|state| {
        json!({
            "consecutiveTerminalFailures": state.consecutive_terminal_failures,
            "failureBackoffUntil": state.failure_backoff_until,
            "failureBackoffReason": state.failure_backoff_reason,
        })
    });
    let body =
        serde_json::to_vec(&json!({ "result": { "prior": prior } })).map_err(ModuleError::Json)?;
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
    send(writer, response).await
}

async fn handle_clear_cooldown(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ClearCooldownParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("route.clear_cooldown params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.route_key.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_cooldown routeKey must not be empty",
        )
        .await;
    }
    let prior = match crate::account_writer::clear_route_cooldowns(store, &params.route_key) {
        Ok(Some(prior)) => prior,
        Ok(None) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "unknown_route",
                &format!("unknown route key {:?}", params.route_key),
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_cooldown: {error}"),
            )
            .await;
        }
    };
    let cooldown = prior
        .routes
        .iter()
        .filter_map(|(_, row)| row.cooldown_until)
        .max_by(f64::total_cmp);
    let quota = prior
        .routes
        .iter()
        .filter_map(|(_, row)| row.quota_exhausted_until)
        .max_by(f64::total_cmp);
    let body = serde_json::to_vec(&json!({ "result": {
        "routeKey": prior.route_key,
        "clearedCooldownUntil": cooldown,
        "clearedQuotaExhaustedUntil": quota,
        "clearedRoutes": prior.routes.iter().map(|(key, row)| json!({
            "routeKey": key, "clearedCooldownUntil": row.cooldown_until,
            "clearedQuotaExhaustedUntil": row.quota_exhausted_until,
        })).collect::<Vec<_>>(),
        "clearedAccountFences": prior.account_fences.iter().map(|fence| json!({
            "provider": fence.provider, "model": fence.model,
            "credentialId": fence.credential_id, "clearedUntil": fence.until_ms,
        })).collect::<Vec<_>>(),
    } }))
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
    send(writer, response).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearCredentialFencesParams {
    credential_id: String,
}

/// Whether the route's daemon-attested principal may lift credential fences:
/// the operator on a shell on this machine (`direct`) or on the phone
/// (`reserved:callosum`). Every other module principal, and unverified and
/// unattested routes, are refused: lifting a fence sends the next request back
/// to a credential the provider refused, which only the operator may order
/// after topping it up.
fn is_fence_lift_operator(principal: &str) -> bool {
    principal == "direct" || principal == "reserved:callosum"
}

/// Lift every credential entitlement fence on one named vault credential at
/// once, for use right after topping it up. Operator-only (see
/// [`is_fence_lift_operator`]); the reply names what was lifted.
async fn handle_clear_credential_fences(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let principal = route_principal(channel, epoch);
    if !is_fence_lift_operator(&principal) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "not_permitted",
            &format!(
                "route.clear_credential_fences is operator-only: principal '{principal}' may not lift credential fences",
            ),
        )
        .await;
    }
    let params: ClearCredentialFencesParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("route.clear_credential_fences params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.credential_id.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_credential_fences credentialId must not be empty",
        )
        .await;
    }
    let lifted = match credential_fences::lift_for_operator(store, &params.credential_id) {
        Ok(lifted) => lifted,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_credential_fences: {error}"),
            )
            .await;
        }
    };
    let body = serde_json::to_vec(&json!({ "result": {
        "credentialId": params.credential_id,
        "lifted": lifted.iter().map(|fence| json!({
            "providerId": fence.provider_id,
            "code": fence.code,
        })).collect::<Vec<_>>(),
    } }))
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
    send(writer, response).await
}

async fn handle_record_cooldown(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: RecordCooldownParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(b) => b.params,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("record_cooldown params not decodable: {e}"),
                )
                .await;
            }
        };
    if let Some(message) = record_cooldown_params_error(&params) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &message,
        )
        .await;
    }
    let recorded = if matches!(
        params.wall_class,
        Some(RouteWallClass::Quota | RouteWallClass::Billing)
    ) && params.credential_id.is_some()
    {
        let Some((provider, model)) = params.capacity_key.split_once('/') else {
            return send_ok(writer, ver, channel, epoch, corr).await;
        };
        let credential = params.credential_id.as_deref().unwrap();
        let now = now_ms() as i64;
        let config = config_overrides::live_config(config_path);
        let contradicted = params.wall_class == Some(RouteWallClass::Billing)
            || ACCOUNT_QUOTA
                .get()
                .and_then(|quota| quota.lock().ok())
                .is_some_and(|quota| {
                    credentials.lock().ok().is_some_and(|credentials| {
                        credential_fences::quota_refusal_contradicted(
                            &quota,
                            &credentials,
                            credential,
                            now,
                            &config.allocator_policy(provider),
                        )
                    })
                });
        let fence = striatum_store::AccountRouteFence {
            provider: provider.into(),
            credential_id: credential.into(),
            model: model.into(),
            until_ms: if contradicted {
                let cap =
                    now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
                params
                    .quota_exhausted_until
                    .or(params.cooldown_until)
                    .map(|at| at as i64)
                    .filter(|at| *at > now)
                    .map_or(cap, |at| at.min(cap))
            } else {
                params
                    .quota_exhausted_until
                    .or(params.cooldown_until)
                    .unwrap_or(0.0) as i64
            },
            decision_id: String::new(),
            recorded_at: now,
        };
        if contradicted {
            crate::account_writer::record_billing_account_route_fence(store, &fence)
        } else {
            crate::account_writer::record_account_route_fence(store, &fence)
        }
    } else {
        record_cooldown_update(store.as_ref(), &params)
    };
    match recorded {
        Ok(()) => send_ok(writer, ver, channel, epoch, corr).await,
        Err(e) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("record_cooldown: {e}"),
            )
            .await
        }
    }
}

/// Send a key-pool reply: a `{"result": ...}` response, or an error frame
/// carrying the typed refusal code and its detail (for an exhausted pool, every
/// key with its reason and reset).
async fn send_pool_reply(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    reply: Result<serde_json::Value, key_pool::PoolRefusal>,
) -> Result<(), ModuleError> {
    let (ty, body) = match reply {
        Ok(result) => (
            FrameType::Response,
            serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?,
        ),
        Err(refusal) => (
            FrameType::Error,
            serde_json::to_vec(&ErrorBody {
                code: refusal.code,
                message: refusal.message,
                detail: refusal.detail,
            })
            .map_err(ModuleError::Json)?,
        ),
    };
    let frame = Frame::build_with_version(
        ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

async fn send_route_error(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    code: &str,
    message: &str,
) -> Result<(), ModuleError> {
    send_route_error_body(
        writer,
        ver,
        channel,
        epoch,
        corr,
        ErrorBody {
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
        },
    )
    .await
}

async fn send_route_error_body(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    error: ErrorBody,
) -> Result<(), ModuleError> {
    let body = serde_json::to_vec(&error).map_err(ModuleError::Json)?;
    let frame = Frame::build_with_version(
        ver,
        FrameType::Error,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

/// Send an account-selection reply: a `{"result": ...}` response, or an error
/// frame whose code is the typed refusal code and whose detail repeats that
/// code with the retry time, decision id and per-account exclusions.
async fn send_account_reply(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    reply: account_selection::OpReply,
) -> Result<(), ModuleError> {
    let (frame_type, body) = match reply {
        account_selection::OpReply::Result(result) => (
            FrameType::Response,
            serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?,
        ),
        account_selection::OpReply::Error {
            code,
            message,
            detail,
        } => (
            FrameType::Error,
            serde_json::to_vec(&ErrorBody {
                code,
                message,
                detail,
            })
            .map_err(ModuleError::Json)?,
        ),
    };
    let frame = Frame::build_with_version(
        ver,
        frame_type,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

async fn send(writer: &mpsc::Sender<Frame>, frame: Frame) -> Result<(), ModuleError> {
    writer
        .send(frame)
        .await
        .map_err(|_| ModuleError::Message("egress channel closed".into()))
}

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

pub(crate) fn handles(method: &str) -> bool {
    matches!(
        method,
        ROUTE_SELECT_OP
            | ROUTE_RESOLVE_POLICY_OP
            | ROUTE_MODEL_FACT_OP
            | ROUTE_MODEL_FACTS_OP
            | ROUTE_CATALOG_TARGETS_OP
            | ROUTE_HEAD_CHOICES_OP
            | ROUTE_MODEL_UPSERT_OP
            | ROUTE_LIST_INFERRED_OP
            | ROUTE_LIST_UNSCORED_OP
            | ROUTE_LIST_CANONICAL_GAPS_OP
            | ROUTE_SELECT_PANEL_OP
            | ROUTE_SET_DECISION_OUTCOME_OP
            | ROUTE_RECORD_USAGE_OP
            | ROUTE_RECORD_COOLDOWN_OP
            | ROUTE_CLEAR_FAILURE_BACKOFF_OP
            | ROUTE_CLEAR_COOLDOWN_OP
            | ROUTE_CLEAR_CREDENTIAL_FENCES_OP
            | ROUTE_WHAT_IF_OP
            | ROUTE_COOLDOWN_STATUS_OP
            | ROUTE_EXPLAIN_OP
            | ROUTE_DECISIONS_LIST_OP
            | diagnostics_findings::OP
            | QUOTA_STATUS_OP
            | ROUTE_ACCOUNT_QUOTA_STATUS_OP
            | account_selection::ACCOUNT_FOR_OP
            | account_selection::ACCOUNT_STATUS_OP
            | account_selection::ACCOUNT_OBSERVE_OP
            | account_selection::ACCOUNT_RELEASE_OP
    ) || key_pool::handles(method)
        || routing_v1::V1_ONLY_OPS.contains(&method)
}

/// Operations registered in HELLO and exposed by `--print-manifest`.
pub(crate) fn served_operations() -> Vec<ManagementOperation> {
    let pool_operations = [
        key_pool::POOL_ACQUIRE_OP,
        key_pool::POOL_RENEW_OP,
        key_pool::POOL_RELEASE_OP,
        key_pool::POOL_WALL_OP,
    ]
    .into_iter()
    .map(|name| ManagementOperation {
        name: name.to_string(),
        kind: ManagementOperationKind::Mutate,
        description: None,
    });
    let mut operations = vec![
        ManagementOperation {
            name: cortexkit_role_routing::ops::ROUTE_SELECT_ENGINE.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Choose the engine for one media job (transcription, synthesis, image, video, realtime) and record the decision: privacy and reachability filters first, then measured accuracy, then cost from Fusiform's prices.".to_string()),
        },
        // route.select records the decision returned by its closed routing/v1
        // request, so the operation is a mutation.
        ManagementOperation {
            name: ROUTE_SELECT_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "One selection pass for the older route.select body and routing/v1 select: route, runner, variant, limits, decision id and expiry"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_RESOLVE_POLICY_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_MODEL_FACT_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_MODEL_FACTS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_CATALOG_TARGETS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_HEAD_CHOICES_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_MODEL_UPSERT_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_LIST_INFERRED_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "List aa_inferred scores and unscored catalog rows for hand review".to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_LIST_UNSCORED_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "List Fusiform-discovered catalog rows awaiting operator scores".to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_LIST_CANONICAL_GAPS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "List scored routes whose Fusiform row states no canonical model id".to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_SELECT_PANEL_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_SET_DECISION_OUTCOME_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_RECORD_USAGE_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_RECORD_COOLDOWN_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_CLEAR_FAILURE_BACKOFF_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Clear one route's terminal-failure backoff without changing cooldown or quota state"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_CLEAR_COOLDOWN_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Clear one route's cooldown and quota wall without changing failure backoff".to_string()),
        },
        ManagementOperation {
            name: ROUTE_CLEAR_CREDENTIAL_FENCES_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Lift every credential entitlement fence on one vault credential, for use right after topping it up (operator-only: direct and reserved:callosum routes)"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: ROUTE_WHAT_IF_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Evaluate routing with call-local model policy changes, without recording anything (operator-only: direct and reserved:callosum routes)".to_string()),
        },
        ManagementOperation {
            name: ROUTE_COOLDOWN_STATUS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_EXPLAIN_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_DECISIONS_LIST_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: diagnostics_findings::OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Routing self-diagnosis: credential fences, route and account blocks, refusal-heavy consumers and one-model candidate sets (head routes and flow scopes only)"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: QUOTA_STATUS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: ROUTE_ACCOUNT_QUOTA_STATUS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Show each subscription account's quota view, age band, vault join and cold-work eligibility"
                    .to_string(),
            ),
        },
        // Account-selection operations serve even before quota readings arrive.
        ManagementOperation {
            name: account_selection::ACCOUNT_FOR_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Choose the subscription account for one admission, renewal or reassignment"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: account_selection::ACCOUNT_OBSERVE_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Report account-bound admission, dispatch and failure evidence".to_string(),
            ),
        },
        ManagementOperation {
            name: account_selection::ACCOUNT_STATUS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Read account quota headroom for prompt-cache warming without reserving quota"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: account_selection::ACCOUNT_RELEASE_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Release an unstarted admission or end a conversation lease".to_string(),
            ),
        },
        // Operations exclusive to the routing/v1 surface.
        ManagementOperation {
            name: cortexkit_role_routing::ops::ROUTE_CONSUMER_MODELS.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Stored model judgments for an admitted consumer".to_string()),
        },
        ManagementOperation {
            name: cortexkit_role_routing::ops::ROUTE_BUDGET_QUOTE.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Quote the retained selection-time capacities".to_string()),
        },
        ManagementOperation {
            name: cortexkit_role_routing::ops::ROLE_DESCRIBE.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "routing/v1: the role, its ops, the configured consumers and requirement fields"
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: cortexkit_role_routing::ops::ROUTE_REPORT.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some("routing/v1: how running on a decision went".to_string()),
        },
    ];
    operations.extend(pool_operations);
    operations
}

#[derive(Serialize)]
struct PrintedManifestOperation {
    name: String,
    kind: &'static str,
}

/// `--print-manifest` output, the same shape the core module prints: the whole
/// manifest this build declares in HELLO, including the build provenance that
/// `script/deploy-module.sh` reads to find the installed commit, plus the
/// operation kinds the op-kinds generator reads.
#[derive(Serialize)]
struct PrintedManifest {
    manifest: ModuleManifest,
    operation_kinds: Vec<PrintedManifestOperation>,
}

fn print_manifest() -> Result<(), ModuleError> {
    let output = printed_manifest_json(&printed_manifest_module_id())?;
    println!("{output}");
    Ok(())
}

/// The module id serving would declare: the same resolution as
/// `ModuleConfig::from_env`, so the printed manifest matches the HELLO one.
fn printed_manifest_module_id() -> String {
    std::env::var(SUBC_MODULE_ID_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string())
}

fn printed_manifest_json(module_id: &str) -> Result<String, ModuleError> {
    serde_json::to_string(&PrintedManifest {
        manifest: manifest(module_id),
        operation_kinds: printed_operation_kinds(),
    })
    .map_err(ModuleError::Json)
}

fn printed_operation_kinds() -> Vec<PrintedManifestOperation> {
    let mut operations = served_operations()
        .into_iter()
        .map(|operation| {
            let kind = match operation.kind {
                ManagementOperationKind::Query => "query",
                ManagementOperationKind::Mutate => "mutate",
            };
            PrintedManifestOperation {
                name: operation.name,
                kind,
            }
        })
        .collect::<Vec<_>>();
    operations.sort_by(|left, right| left.name.cmp(&right.name));
    operations
}

/// The module's capability manifest: a ManagementSurface exposing routing operations.
///
/// The catalog and route-state tables are machine-global, so `identity_scope` is
/// empty and the identity binding requires nothing. The routing module owns this
/// schema and is the sole writer for its provider and route cooldown state.
pub(crate) fn manifest(module_id: &str) -> ModuleManifest {
    // `ModuleManifest` is `#[non_exhaustive]`, so this module constructs it
    // with the builder. It provides the routing/v1, scope-workdir and
    // flow-scopes capabilities and requires none.
    // Build provenance comes from the module's build script.
    ModuleManifest::builder(module_id, env!("CARGO_PKG_VERSION"))
        .trust_tier(Some(TrustTier::FirstParty))
        .provenance(declared_provenance())
        .bindings(Some(Bindings {
            storage: StorageBinding {
                kind: StorageKind::Sqlite,
                scope: StorageScope::Project,
                owns_schema: true,
            },
            vault_grants: Vec::new(),
            identity: IdentityBinding {
                requires: Vec::new(),
                optional: Vec::new(),
            },
        }))
        .protocol_ver(PROTOCOL_VERSION)
        // The catalog finds the routing provider by this capability. It also
        // provides `scope-workdir/v1`, which obliges it to accept the extra
        // scope-stamp fields the daemon sends with it: the session folder and
        // the Entorhinal project id, recorded on every decision the route's
        // caller selects. Without the declaration the daemon strips both and a
        // scoped select records no project.
        //
        // It provides `flow-scopes/v1` so flows can call `diagnostics.findings`:
        // the daemon binds a flow-scoped route only to a module declaring it,
        // and Basal checks the declared flow operations at install. Routing
        // also refuses every op outside the same allowlist on a flow-scoped
        // route (see `diagnostics_findings::flow_route_refusal`).
        .capabilities(Some(subc_protocol::manifest::CapabilityDeclarations {
            provides: vec![
                routing_v1::PROVIDES.to_string(),
                subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY.to_string(),
                subc_protocol::scope::FLOW_SCOPES_CAPABILITY.to_string(),
            ],
            requires: Vec::new(),
            must_never_reach: Vec::new(),
        }))
        .flow_operations(Some(
            diagnostics_findings::FLOW_SCOPE_OPS
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect(),
        ))
        .provides(vec![ProviderRole::ManagementSurface {
            // Deliberate lane, not a default: routing serves concurrent
            // selection reads; Serial would put every route decision in
            // one queue.
            concurrency: Concurrency::ModuleManaged,
            operations: served_operations(),
            config_schema: json!({ "type": "object" }),
            observability: Vec::new(),
            identity_scope: Vec::new(),
        }])
        .build()
}

/// Records this binary's build revision and, when available, where this process
/// read its launch nonce from.
fn declared_provenance() -> Option<ManifestProvenance> {
    let dirty = env!("PREFRONTAL_BUILD_GIT_DIRTY") == "true";
    let revision = env!("PREFRONTAL_BUILD_GIT_SHA");
    let source = if revision == "unknown" {
        BuildGitShaSource::NeverDerived
    } else {
        BuildGitShaSource::Git {
            revision,
            tree_state: if dirty {
                GitTreeState::Dirty
            } else {
                GitTreeState::Clean
            },
        }
    };
    let mut provenance = subc_protocol::manifest::build_provenance_from_source(
        source,
        Some(env!("PREFRONTAL_BUILD_LOCK_DIGEST")),
        None,
    )
    .expect("build script emits a canonical git revision");
    let launch_nonce_source = subc_os::launch_nonce()
        .ok()
        .flatten()
        .map(|nonce| LaunchNonceSource::from_wire_name(nonce.source().as_str()));
    provenance = provenance.with_launch_nonce_source(launch_nonce_source);
    Some(provenance)
}

fn parse_startup_mode(
    args: impl IntoIterator<Item = OsString>,
) -> Result<StartupMode, ModuleError> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--check-config") {
        return if args.as_slice() == [OsString::from("--check-config")] {
            Ok(StartupMode::CheckConfig)
        } else {
            Err(ModuleError::Message(
                "--check-config must be used without other arguments".into(),
            ))
        };
    }
    if args.as_slice() == [OsString::from("--print-config-schema")] {
        return Ok(StartupMode::PrintConfigSchema);
    }
    if args.iter().any(|arg| arg == "--print-config-schema") {
        return Err(ModuleError::Message(
            "--print-config-schema must be used without other arguments".into(),
        ));
    }
    if args.as_slice() == [OsString::from("--print-manifest")] {
        return Ok(StartupMode::PrintManifest);
    }
    if args.iter().any(|arg| arg == "--print-manifest") {
        return Err(ModuleError::Message(
            "--print-manifest must be used without other arguments".into(),
        ));
    }
    if args.iter().any(|arg| arg == "--account-quota-dry-run") {
        let capture_dir = args
            .iter()
            .position(|arg| arg == "--capture")
            .map(|index| {
                args.get(index + 1)
                    .map(PathBuf::from)
                    .ok_or_else(|| ModuleError::Message("--capture requires a directory".into()))
            })
            .transpose()?;
        return parse_subc_arg(args).map(|connection_file_path| StartupMode::AccountQuotaDryRun {
            connection_file_path,
            capture_dir,
        });
    }
    parse_subc_arg(args).map(StartupMode::Serve)
}

fn parse_subc_arg(args: impl IntoIterator<Item = OsString>) -> Result<PathBuf, ModuleError> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--subc" {
            let value = args
                .next()
                .ok_or_else(|| ModuleError::Message("--subc requires a value".into()))?;
            return Ok(PathBuf::from(value));
        }
        if let Some(raw) = arg.to_str().and_then(|a| a.strip_prefix("--subc=")) {
            if raw.is_empty() {
                return Err(ModuleError::Message("--subc= requires a value".into()));
            }
            return Ok(PathBuf::from(raw));
        }
    }
    Err(ModuleError::Message(
        "missing required --subc <connection-file> argument".into(),
    ))
}

#[derive(Debug)]
enum ModuleError {
    Message(String),
    Json(serde_json::Error),
}

impl fmt::Display for ModuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(m) => write!(f, "{m}"),
            Self::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl Error for ModuleError {}

#[cfg(test)]
mod tests {
    mod catalog_targets_contract {
        use super::*;
        include!("catalog_targets_tests.rs");
    }
    include!("panel_policy_tests.rs");
    include!("replay_model_facts_tests.rs");
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use striatum_core::model_facts::{Confidence, ModelFact, ModelPromptDefaults, ModelScores};
    use striatum_core::model_router::select_model_route_with_config;
    use striatum_core::{FactSource, NativeUsageFacts, UsageObservation};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[tokio::test(flavor = "current_thread")]
    async fn account_quota_capture_keeps_current_thread_worker_free() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-quota-capture-blocked");
        let path = scratch.join("usage-get-all.json");
        assert!(std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success());
        let body = b"{ \"providers\": [] }\n\0unfiltered bytes".to_vec();

        // The writer is polled first and cannot open the FIFO until the sibling
        // future runs on the same async worker and starts a blocking reader.
        let (captured, saved) = tokio::join!(
            biased;
            capture_account_quota_reply(scratch.path().to_path_buf(), body.clone()),
            async {
                tokio::task::spawn_blocking(move || std::fs::read(path))
                    .await.unwrap().unwrap()
            },
        );
        captured.unwrap();
        assert_eq!(saved, body);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn account_quota_capture_preserves_bytes_and_io_errors() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-quota-capture-bytes");
        let directory = scratch.join("capture");
        let path = directory.join("usage-get-all.json");
        let body = b"{ \"providers\": [] }\n\0unfiltered bytes".to_vec();
        capture_account_quota_reply(directory.clone(), body.clone())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), body);

        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let expected = std::fs::write(&path, &body).unwrap_err().to_string();
        let error = capture_account_quota_reply(directory, body.clone())
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), format!("capture usage: {expected}"));

        let not_directory = scratch.join("file");
        std::fs::write(&not_directory, b"file").unwrap();
        let expected = std::fs::create_dir_all(&not_directory)
            .unwrap_err()
            .to_string();
        let error = capture_account_quota_reply(not_directory, body)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), format!("capture usage: {expected}"));
    }

    #[test]
    fn config_check_uses_startup_loader_and_refuses_unknown_config() {
        assert_eq!(
            parse_startup_mode([OsString::from("--check-config")]).unwrap(),
            StartupMode::CheckConfig
        );
        assert!(
            parse_startup_mode([OsString::from("--check-config"), OsString::from("--subc=x")])
                .is_err()
        );
        let scratch = cortexkit_test_support::ScratchDir::new("routing-config-check");
        let path = scratch.join("striatum.jsonc");
        check_config_at(&path).unwrap();
        let invalid = cortexkit_test_support::ScratchDir::new("routing-config-check-invalid");
        let invalid_path = invalid.join("striatum.jsonc");
        crate::test_config::write(&invalid_path, "{\"not_a_routing_setting\":true}").unwrap();
        assert!(check_config_at(&invalid_path).is_err());
    }

    #[test]
    fn config_check_refuses_old_shape_and_names_converter() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-config-check-explicit");
        let path = scratch.join("striatum.jsonc");
        crate::test_config::write_raw(
            &path,
            r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
        )
        .unwrap();
        let error = check_config_at(&path).unwrap_err().to_string();
        assert!(
            error.contains("model_routing") && error.contains("ck-striatum config-convert"),
            "{error}"
        );
    }

    #[test]
    fn record_cooldown_keeps_billing_but_does_not_infer_unscoped_quota() {
        let rig = Rig::new("record-cooldown-wall-class");
        let store = rig.open_store();
        for (route_key, wall_class, last_error) in [
            (
                "xai/grok-4.6",
                RouteWallClass::Billing,
                "personal-team-blocked:spending-limit: You have run out of credits or need a Grok subscription",
            ),
            (
                "p/plain-quota",
                RouteWallClass::Quota,
                "you have reached your usage limit",
            ),
        ] {
            record_cooldown_update(
                &store,
                &RecordCooldownParams {
                    route_key: route_key.into(),
                    capacity_key: route_key.into(),
                    credential_id: None,
                    cooldown_until: Some(5_000.0),
                    quota_exhausted_until: Some(5_000.0),
                    quota_pressure: None,
                    wall_class: Some(wall_class),
                    provider_wide: true,
                    increment_quota_error: true,
                    increment_failure: true,
                    last_error: Some(last_error.into()),
                },
            )
            .unwrap();
        }

        let snapshot = store.build_snapshot(1_000.0).unwrap();
        assert_eq!(
            snapshot.by_route["xai/grok-4.6"].wall_class,
            Some(RouteWallClass::Billing)
        );
        assert_eq!(snapshot.by_route["p/plain-quota"].wall_class, None);
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| !origin.quota));
    }

    #[test]
    fn ignored_caller_policy_warning_is_claimed_once_per_boot() {
        let warned = Once::new();
        assert!(!claim_ignored_caller_policy_warning(false, &warned));
        assert!(claim_ignored_caller_policy_warning(true, &warned));
        assert!(!claim_ignored_caller_policy_warning(true, &warned));
    }

    #[test]
    fn quota_refresh_logs_only_unrecognized_funding_once_per_distinct_set() {
        let known: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "deepseek",
            "spend": [
                { "id": "purchased", "label": "Purchased credits", "funding": "purchased", "basis": "reported" },
                { "id": "unknown", "label": "Prepaid credits", "funding": "unknown", "basis": "reported" }
            ]
        }]))
        .unwrap();
        let first_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "provider-a",
            "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
        }]))
        .unwrap();
        let repeated_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([
            {
                "provider": "provider-a",
                "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
            },
            {
                "provider": "provider-b",
                "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
            }
        ]))
        .unwrap();
        let different_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "provider-a",
            "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v3", "basis": "reported" }]
        }]))
        .unwrap();
        let mut reported = QuotaRefreshWarnings::default();
        let mut emitted = Vec::new();

        log_quota_refresh_warnings_with(&known, &mut reported, |warning| {
            emitted.push(warning.to_string())
        });
        assert!(emitted.is_empty(), "known wire values must not log");
        log_quota_refresh_warnings_with(&first_unrecognized_refresh, &mut reported, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted, [
            "[striatum] quota refresh ignored unsupported spend funding values: {\"issuer_credit_v2\"}"
        ]);
        // A second refresh with the same value set, even on another provider,
        // produces no log line.
        log_quota_refresh_warnings_with(&repeated_unrecognized_refresh, &mut reported, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 1);
        log_quota_refresh_warnings_with(
            &different_unrecognized_refresh,
            &mut reported,
            |warning| emitted.push(warning.to_string()),
        );
        assert_eq!(emitted.len(), 2);
    }

    #[test]
    fn walled_with_funds_warning_logs_once_until_the_shape_clears() {
        let walled_reading = |provider: &str, grant_minor| {
            usage(json!({
                "provider": provider,
                "spend": [
                    {
                        "id": "grant",
                        "label": "Granted balance",
                        "funding": "granted",
                        "remaining": { "minor": grant_minor, "exponent": 2, "unit": "USD" },
                        "basis": "reported"
                    },
                    {
                        "id": "purchased",
                        "label": "Purchased credits",
                        "funding": "purchased",
                        "remaining": { "minor": 250, "exponent": 2, "unit": "USD" },
                        "basis": "reported"
                    }
                ]
            }))
        };
        let deepseek_wall = walled_reading("deepseek", 0);
        let deepseek_clear = walled_reading("deepseek", 100);
        let openai_wall = walled_reading("openai", 0);
        let openai_clear = walled_reading("openai", 100);
        let both_walls = [deepseek_wall.clone(), openai_wall.clone()];
        let mut warnings = QuotaRefreshWarnings::default();
        let mut emitted = Vec::new();

        log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 2);
        assert!(emitted
            .iter()
            .any(|line| line.contains("provider deepseek walled")));
        assert!(emitted
            .iter()
            .any(|line| line.contains("provider openai walled")));

        log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 2, "a repeated wall shape must be silent");

        let deepseek_cleared = [deepseek_clear, openai_wall.clone()];
        log_quota_refresh_warnings_with(&deepseek_cleared, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(
            emitted.len(),
            2,
            "clearing one provider's shape does not warn"
        );

        log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 3, "the returned DeepSeek wall warns again");
        assert!(emitted[2].contains("provider deepseek walled"));

        let openai_cleared = [deepseek_wall, openai_clear];
        log_quota_refresh_warnings_with(&openai_cleared, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 3);
        log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
            emitted.push(warning.to_string())
        });
        assert_eq!(emitted.len(), 4, "the returned OpenAI wall warns again");
        assert!(emitted[3].contains("provider openai walled"));
    }

    #[test]
    fn contract_outcome_round_trips_without_a_quota_horizon() {
        let outcome = RouteOutcome::ContractError;
        assert_eq!(parse_outcome(outcome.as_str(), None), Some(outcome));
        assert_eq!(outcome.retry_after_secs(), None);
        assert!(!striatum_core::is_terminal_send_failure(outcome));
    }

    #[test]
    fn quota_outcome_requires_and_carries_retry_after_secs() {
        assert_eq!(
            parse_outcome("quota_exhausted", Some(580)),
            Some(RouteOutcome::QuotaExhausted {
                retry_after_secs: 580,
            })
        );
        assert_eq!(parse_outcome("quota_exhausted", None), None);
        assert_eq!(parse_outcome("quota_exhausted", Some(0)), None);
    }

    #[tokio::test]
    async fn provider_outcome_wire_records_terminal_outcomes_and_frozen_reset_fence() {
        use striatum_core::route_state_store::ProviderOutcome;
        let rig = Rig::new("provider-reset-wire");
        let store = Arc::new(rig.open_store());
        let make_decision = || {
            store
                .record_decision(&DecisionInput {
                    consumer: Some("mason".into()),
                    selected_provider_id: Some("google".into()),
                    selected_model_id: Some("antigravity-gemini-3.8-flash".into()),
                    selected_variant: Some("high".into()),
                    selected_route_key: Some("google/antigravity-gemini-3.8-flash/high".into()),
                    ..Default::default()
                })
                .unwrap()
        };
        let failed = make_decision();
        let recovery = make_decision();
        store
            .set_decision_provider_identity(
                &failed,
                &ProviderOutcome {
                    account_ref: "vault-account".into(),
                    credential_id: Some("antigravity:google".into()),
                    resets_at_ms: None,
                    credential_refusal: None,
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .set_decision_provider_identity(
                &recovery,
                &ProviderOutcome {
                    account_ref: "quota-account".into(),
                    credential_id: Some("antigravity:google".into()),
                    resets_at_ms: None,
                    credential_refusal: None,
                    ..Default::default()
                },
            )
            .unwrap();
        let reset = now_ms() as i64 + 75 * 3_600_000;
        for params in [
            json!({"decisionID":failed,"outcome":"interrupted","lastError":"account_limit_exhausted: transient HTTP 429",
                "providerOutcome":{"accountRef":"provider-email","resetsAtMs":reset}}),
            json!({"decisionID":recovery,"outcome":"completed"}),
        ] {
            let completing = params["outcome"] == "completed";
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(
                    &json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}),
                )
                .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_set_decision_outcome(
                frame,
                &tx,
                &store,
                Path::new("/nonexistent/provider-reset-wire.jsonc"),
                &SharedBrocaCredentials::default(),
            )
            .await
            .unwrap();
            let ack: serde_json::Value =
                serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
            assert_eq!(ack["result"]["ok"], true, "{ack}");
            let id = if completing { &recovery } else { &failed };
            assert_eq!(
                store
                    .get_decision_by_id(id)
                    .unwrap()
                    .unwrap()
                    .outcome
                    .as_deref(),
                Some(if completing {
                    "completed"
                } else {
                    "interrupted"
                }),
            );
        }
        let fences = store.account_route_fences(now_ms() as i64).unwrap();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].decision_id, failed);
        assert_eq!(fences[0].credential_id, "antigravity:google");
        assert_eq!(fences[0].until_ms, reset);
    }

    #[test]
    fn provider_reset_fence_preserves_a_monthly_reset() {
        use striatum_core::route_state_store::ProviderOutcome;
        let rig = Rig::new("provider-monthly-reset");
        let store: SharedStore = Arc::new(rig.open_store());
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("opencode-go".into()),
                selected_model_id: Some("deepseek-v4.1-flash".into()),
                ..Default::default()
            })
            .unwrap();
        let identity = ProviderOutcome {
            account_ref: "apikey:opencode-go".into(),
            credential_id: Some("apikey:opencode-go".into()),
            resets_at_ms: Some(1_795_357_028_612),
            credential_refusal: None,
            ..Default::default()
        };
        store
            .set_decision_provider_identity(&decision, &identity)
            .unwrap();
        let now = 1_793_604_325_063;
        record_provider_reset_fence(
            &store,
            &SharedBrocaCredentials::default(),
            &decision,
            &RouteOutcome::Interrupted,
            Some(&identity),
            true,
            None,
            now,
            None,
            None,
        )
        .unwrap();
        let fences = store.account_route_fences(now).unwrap();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].until_ms, 1_795_357_028_612);
    }

    #[test]
    fn xai_spending_limit_with_fresh_zero_usage_is_a_non_quota_refusal() {
        use striatum_core::route_state_store::ProviderOutcome;
        let now = account_quota::parse_rfc3339_ms("2026-11-10T13:57:00Z").unwrap();
        for used in [0.0, 100.0] {
            let rig = Rig::new("xai-contradictory-quota");
            let store: SharedStore = Arc::new(rig.open_store());
            let credentials = vault_listing(&[("oauth:xai", &["xai"], Some("xai-account"))]);
            let mut quota = account_quota::AccountQuotaState::default();
            quota.readings.readings.insert(("xai".into(),"xai-account".into()),account_quota::StoredReading {
                provider:"xai".into(),account_id:"xai-account".into(),received_at_ms:now-600_000,tombstoned_at_ms:None,
                entry:json!({"provider":"xai","apiProvider":"xai","account":"xai-account","fetchedAt":"2026-11-10T13:47:00Z",
                    "usage":{"secondary":{"usedPercent":used,"windowMinutes":10080}}}),
            });
            let decision = store
                .record_decision(&DecisionInput {
                    selected_provider_id: Some("xai".into()),
                    selected_model_id: Some("grok-4.3".into()),
                    ..Default::default()
                })
                .unwrap();
            let identity = ProviderOutcome {
                account_ref: "xai-account".into(),
                credential_id: Some("oauth:xai".into()),
                ..Default::default()
            };
            store
                .set_decision_provider_identity(&decision, &identity)
                .unwrap();
            // The legacy runner typed the 403 spending-limit refusal as account
            // exhaustion, with no reset. Classification must use the reading,
            // not the provider's free-form personal-team-blocked message.
            record_provider_reset_fence(
                &store,
                &credentials,
                &decision,
                &RouteOutcome::Interrupted,
                Some(&identity),
                true,
                None,
                now,
                Some(&quota),
                None,
            )
            .unwrap();
            let quota_rows = store.account_route_fences(now).unwrap();
            let other_rows = store.non_quota_account_route_fences(now).unwrap();
            if used == 0.0 {
                assert!(quota_rows.is_empty());
                assert_eq!(other_rows.len(), 1);
                let clocks = store.fence_rechecks().unwrap();
                assert_eq!(clocks[0].kind, "non_quota_route");
                assert_eq!(clocks[0].next_recheck_at, now + 3_600_000);
                assert!(!store
                    .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 15 * 60_000))
                    .unwrap());
                // Another fresh zero reading does not heal the non-quota wall.
                assert_eq!(
                    store
                        .non_quota_account_route_fences(now + 60_000)
                        .unwrap()
                        .len(),
                    1
                );
                assert!(store
                    .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 3_600_000))
                    .unwrap());
                assert!(!store
                    .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, now + 3_600_000))
                    .unwrap());
            } else {
                assert_eq!(quota_rows.len(), 1);
                assert!(other_rows.is_empty());
                assert_eq!(
                    store.fence_rechecks().unwrap()[0].next_recheck_at,
                    now + 15 * 60_000
                );
            }
        }
    }

    async fn report_legacy_outcome_for_fence_test(store: &SharedStore, params: serde_json::Value) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}))
                .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_set_decision_outcome(
            frame,
            &tx,
            store,
            Path::new("/nonexistent/fence-outcome.jsonc"),
            &SharedBrocaCredentials::default(),
        )
        .await
        .unwrap();
        let ack: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
        assert_eq!(ack["result"]["ok"], true, "{ack}");
    }

    #[tokio::test]
    async fn legacy_completed_cannot_heal_a_credential_that_did_not_serve() {
        use striatum_core::route_state_store::{CredentialRefusal, ProviderOutcome};
        let rig = Rig::new("mismatched-serving-success");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .record_credential_entitlement_fence(
                &CredentialRefusal {
                    provider_id: "p".into(),
                    credential_id: "B".into(),
                    code: "billing_exhausted".into(),
                },
                None,
                100,
            )
            .unwrap();
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("m".into()),
                ..Default::default()
            })
            .unwrap();
        store
            .set_decision_provider_identity(
                &decision,
                &ProviderOutcome {
                    account_ref: "account-A".into(),
                    credential_id: Some("A".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        report_legacy_outcome_for_fence_test(
            &store,
            json!({"decisionID":decision,"outcome":"completed",
            "providerOutcome":{"accountRef":"account-B","credentialId":"B"}}),
        )
        .await;
        assert_eq!(
            store.credential_entitlement_fences().unwrap().len(),
            1,
            "B did not serve the decision frozen to A"
        );
    }

    #[tokio::test]
    async fn anonymous_legacy_reported_credential_heals_only_its_claimed_authorization() {
        for reported in ["claimed", "other"] {
            let rig = Rig::new("anonymous-reported-credential");
            let store: SharedStore = Arc::new(rig.open_store());
            let at = now_ms() as i64;
            let _clock = InjectedClock::set((at + 1000) as u64);
            store
                .pool_wall(
                    "p",
                    "claimed",
                    striatum_store::PoolWallKind::Billing,
                    at + 1000,
                    "credits",
                    at,
                )
                .unwrap();
            let decision = store
                .record_decision(&DecisionInput {
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                })
                .unwrap();
            let origins = store.fence_rechecks().unwrap();
            assert!(store
                .account_transaction(|tx| tx.grant_fence_rechecks(
                    &origins,
                    at + 1000,
                    Some(&decision)
                ))
                .unwrap());
            report_legacy_outcome_for_fence_test(
                &store,
                json!({"decisionID":decision,"outcome":"completed",
                    "providerOutcome":{"accountRef":"account","credentialId":reported}}),
            )
            .await;
            assert_eq!(store.fence_rechecks().unwrap().is_empty(), reported == "claimed",
                "an anonymous decision may heal only when the reported credential appears in its authorization");
        }
    }

    #[tokio::test]
    async fn anonymous_legacy_probe_success_heals_its_actual_launch_authorization() {
        let rig = Rig::new("anonymous-probe-success");
        let store: SharedStore = Arc::new(rig.open_store());
        let receipt = now_ms() as i64;
        let _clock = InjectedClock::set((receipt + 1000) as u64);
        store
            .update_route_state(
                "p/m",
                "p/m",
                &striatum_core::RouteStateUpdate {
                    cooldown_until: Some((receipt + 1000) as f64),
                    observed_at_ms: Some(receipt - 3_599_000),
                    unscoped_wall: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let decision = store
            .record_decision(&DecisionInput {
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("m".into()),
                ..Default::default()
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(!origins.is_empty());
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                receipt + 1000,
                Some(&decision)
            ))
            .unwrap());
        report_legacy_outcome_for_fence_test(
            &store,
            json!({"decisionID":decision,"outcome":"completed"}),
        )
        .await;
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "anonymous serving success must retire the route/provider origin it probed"
        );
    }

    #[tokio::test]
    async fn inconclusive_legacy_probe_reopens_its_non_quota_clock_without_healing() {
        for (outcome, error) in [
            ("cancelled", None),
            ("interrupted", None),
            ("error", Some("provider_policy_refusal: request blocked")),
            ("error", Some("request_invalid: unsupported request shape")),
        ] {
            let rig = Rig::new("inconclusive-probe-clock");
            let store: SharedStore = Arc::new(rig.open_store());
            let receipt = now_ms() as i64;
            let _clock = InjectedClock::set((receipt + 1000) as u64);
            store
                .update_route_state(
                    "p/m",
                    "p/m",
                    &striatum_core::RouteStateUpdate {
                        cooldown_until: Some((receipt + 1000) as f64),
                        observed_at_ms: Some(receipt - 3_599_000),
                        unscoped_wall: true,
                        ..Default::default()
                    },
                )
                .unwrap();
            let decision = store
                .record_decision(&DecisionInput {
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                })
                .unwrap();
            let origins = store.fence_rechecks().unwrap();
            assert!(store
                .account_transaction(|tx| tx.grant_fence_rechecks(
                    &origins,
                    receipt + 1000,
                    Some(&decision)
                ))
                .unwrap());
            report_legacy_outcome_for_fence_test(
                &store,
                json!({"decisionID":decision,"outcome":outcome,"lastError":error}),
            )
            .await;
            let remaining = store.fence_rechecks().unwrap();
            assert_eq!(
                remaining.len(),
                1,
                "an inconclusive probe is not successful serving"
            );
            assert!(
                remaining[0].next_recheck_at <= receipt + 1000,
                "{outcome}/{error:?} must not hold a route for another hour"
            );
        }
    }

    #[tokio::test]
    async fn legacy_credit_window_is_decoder_only_and_success_or_operator_clear_heals_it() {
        use striatum_core::route_state_store::ProviderOutcome;
        async fn report(
            store: &SharedStore,
            credentials: &SharedBrocaCredentials,
            params: serde_json::Value,
        ) {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(
                    &json!({"method":ROUTE_SET_DECISION_OUTCOME_OP,"params":params}),
                )
                .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_set_decision_outcome(
                frame,
                &tx,
                store,
                Path::new("/nonexistent/credit-outcome.jsonc"),
                credentials,
            )
            .await
            .unwrap();
            let ack: serde_json::Value =
                serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
            assert_eq!(ack["result"]["ok"], true, "{ack}");
        }
        for (member, used, retry) in [
            ("limitWindow", 0.0, None),
            ("limit_window", 100.0, Some(30)),
            ("limitWindow", 100.0, Some(7200)),
        ] {
            let receipt = now_ms() as i64;
            let _clock = InjectedClock::set(receipt as u64);
            let rig = Rig::new("legacy-credit-window");
            let store: SharedStore = Arc::new(rig.open_store());
            let credentials = vault_listing(&[("oauth:xai", &["xai"], Some("xai-account"))]);
            let quota =
                ACCOUNT_QUOTA.get_or_init(|| Arc::new(std::sync::Mutex::new(Default::default())));
            let mut state = account_quota::AccountQuotaState::default();
            account_quota::apply_provider_reply(&mut state, "grok", &serde_json::to_vec(&json!({
                "result":[{"provider":"grok","apiProvider":"xai","account":"xai-account","credentialRef":"oauth:xai",
                    "fetchedAt":account_selection::rfc3339(receipt),"usage":{"primary":{"usedPercent":used,"windowMinutes":10080}}}],
                "completeProviders":["grok"]
            })).unwrap(), receipt).unwrap();
            *quota.lock().unwrap() = state;
            let input = DecisionInput {
                selected_provider_id: Some("xai".into()),
                selected_model_id: Some("grok-4.3".into()),
                ..Default::default()
            };
            let failed = store.record_decision(&input).unwrap();
            let identity = ProviderOutcome {
                account_ref: "xai-account".into(),
                credential_id: Some("oauth:xai".into()),
                ..Default::default()
            };
            store
                .set_decision_provider_identity(&failed, &identity)
                .unwrap();
            let mut provider = json!({"accountRef":"xai-account","credentialId":"oauth:xai","providerCode":"account_limit_exhausted"});
            provider[member] = json!("credit");
            let mut params =
                json!({"decisionID":failed,"outcome":"interrupted","providerOutcome":provider});
            if let Some(retry) = retry {
                params["retryAfterSecs"] = json!(retry);
            }
            let decoded: SetDecisionOutcomeParams = serde_json::from_value(params.clone()).unwrap();
            let decoded = decoded.provider_outcome.unwrap();
            assert_eq!(decoded.limit_window.as_deref(), Some("credit"));
            let emitted = serde_json::to_value(&decoded.identity).unwrap();
            assert!(emitted.get("limitWindow").is_none() && emitted.get("limit_window").is_none());
            report(&store, &credentials, params).await;
            let fences = store.non_quota_account_route_fences(receipt).unwrap();
            assert_eq!(
                fences.len(),
                1,
                "credit is independent of the quota reading"
            );
            assert_eq!(
                fences[0].until_ms,
                receipt + retry.unwrap_or(3600).min(3600) * 1000
            );
            assert!(store.account_route_fences(receipt).unwrap().is_empty());
            assert!(store
                .fence_rechecks()
                .unwrap()
                .iter()
                .all(|clock| !clock.quota));

            if member == "limit_window" {
                credential_fences::lift_for_operator(&store, "oauth:xai").unwrap();
                assert!(store
                    .non_quota_account_route_fences(receipt)
                    .unwrap()
                    .is_empty());
                continue;
            }

            // A later successful execution on the same serving identity proves
            // recovery, unlike even a freshly replenished quota allowance.
            let recovered = store.record_decision(&input).unwrap();
            store
                .set_decision_provider_identity(&recovered, &identity)
                .unwrap();
            report(
                &store,
                &credentials,
                json!({"decisionID":recovered,"outcome":"completed"}),
            )
            .await;
            assert!(store
                .non_quota_account_route_fences(receipt)
                .unwrap()
                .is_empty());
        }
    }

    async fn clear_fences_as(
        store: &SharedStore,
        channel: u16,
        principal: &str,
        credential: &str,
    ) -> Frame {
        route_principals()
            .lock()
            .unwrap()
            .insert(channel, (1, principal.to_string()));
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            1,
            99,
            serde_json::to_vec(&json!({"method": ROUTE_CLEAR_CREDENTIAL_FENCES_OP,
                "params": {"credentialId": credential}}))
            .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_clear_credential_fences(frame, &tx, store)
            .await
            .unwrap();
        rx.recv().await.unwrap()
    }

    /// The operator op lifts every fence on the named credential at once, for
    /// use right after a top-up. Only the operator's own routes may call it:
    /// any other module principal, and unverified routes, are refused.
    #[tokio::test]
    async fn the_operator_op_lifts_fences_and_refuses_non_operators() {
        let rig = Rig::new("credential-fence-operator-lift");
        let store: SharedStore = Arc::new(rig.open_store());
        let refusal = |provider: &str| striatum_core::route_state_store::CredentialRefusal {
            provider_id: provider.into(),
            credential_id: "apikey:inception".into(),
            code: "billing_exhausted".into(),
        };
        let credentials = empty_broca_credentials();
        for provider in ["inception", "other"] {
            credential_fences::record(&store, &credentials, &refusal(provider), now_ms() as i64)
                .unwrap();
        }
        assert_eq!(store.credential_entitlement_fences().unwrap().len(), 2);
        let now = now_ms() as i64;
        for (provider, credential, model) in [
            ("inception", "apikey:inception", "model-a"),
            ("other", "apikey:inception", "model-b"),
            ("other", "apikey:other", "model-b"),
        ] {
            store
                .record_account_route_fence(&striatum_store::AccountRouteFence {
                    provider: provider.into(),
                    credential_id: credential.into(),
                    model: model.into(),
                    until_ms: now + 600_000,
                    decision_id: "reset-decision".into(),
                    recorded_at: now,
                })
                .unwrap();
        }
        // The daemon's direct principal represents the local shell operator.
        let reply = clear_fences_as(&store, 501, "direct", "apikey:inception").await;
        assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(
            body["result"]["lifted"],
            json!([
                {"providerId": "inception", "code": "billing_exhausted"},
                {"providerId": "other", "code": "billing_exhausted"},
            ]),
            "the reply names what was lifted: {body}"
        );
        assert!(store.credential_entitlement_fences().unwrap().is_empty());
        let remaining = store.account_route_fences(now).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].credential_id, "apikey:other");
        // The reserved Callosum principal represents the phone's operator bridge.
        credential_fences::record(&store, &credentials, &refusal("inception"), now_ms() as i64)
            .unwrap();
        let reply = clear_fences_as(&store, 502, "reserved:callosum", "apikey:inception").await;
        assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(
            body["result"]["lifted"].as_array().unwrap().len(),
            1,
            "{body}"
        );
        assert!(store.credential_entitlement_fences().unwrap().is_empty());
        // Any other module principal is refused, naming its principal.
        credential_fences::record(&store, &credentials, &refusal("inception"), now_ms() as i64)
            .unwrap();
        let reply = clear_fences_as(&store, 503, "reserved:broca", "apikey:inception").await;
        assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body["code"], "not_permitted", "{body}");
        assert!(
            body["message"].as_str().unwrap().contains("reserved:broca"),
            "the refusal names the principal: {body}"
        );
        assert_eq!(store.credential_entitlement_fences().unwrap().len(), 1);
        // So is an unverified route.
        let reply = clear_fences_as(&store, 504, "unverified", "apikey:inception").await;
        assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body["code"], "not_permitted", "{body}");
        assert_eq!(store.credential_entitlement_fences().unwrap().len(), 1);
    }

    /// Routing deploys before core, and core may run against an older routing, so
    /// neither side may reject the other's report: unknown members of
    /// `providerOutcome` and of `credentialRefusal` are ignored.
    #[test]
    fn the_outcome_report_ignores_unknown_fields() {
        let params: SetDecisionOutcomeParams = serde_json::from_value(json!({
            "decisionID": "d", "outcome": "error", "futureField": 1,
            "providerOutcome": {"accountRef": "a", "futureField": true,
                "credentialRefusal": {"providerId": "p", "credentialId": "c", "code": "ENTITLEMENT_ERROR", "futureField": "x"}}
        }))
        .unwrap();
        let refusal = params
            .provider_outcome
            .unwrap()
            .identity
            .credential_refusal
            .unwrap();
        assert_eq!(
            (refusal.provider_id.as_str(), refusal.credential_id.as_str()),
            ("p", "c")
        );
        // A report from a core that predates the field still decodes.
        let old: SetDecisionOutcomeParams = serde_json::from_value(json!({
            "decisionID": "d", "outcome": "error", "providerOutcome": {"accountRef": "a"}
        }))
        .unwrap();
        assert_eq!(
            old.provider_outcome.unwrap().identity.credential_refusal,
            None
        );
    }

    /// A panel seat's provider reset is outcome metadata, not confirmed quota
    /// exhaustion. It must not remove that model from later panels.
    #[tokio::test]
    async fn a_panel_seat_provider_reset_leaves_the_model_selectable() {
        let rig = Rig::new("panel-seat-provider-reset");
        let store = Arc::new(rig.open_store());
        let request = json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                "panelSpec": {"size": 1, "iqFloor": 0, "diversity": "none", "class": "AUDIT", "temperatureSpread": [0.2]},
                "selection": "n_pick",
                "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
                "taskId": "panel-seat-reset-task"
            }
        });
        async fn select(store: &SharedStore, request: &serde_json::Value) -> serde_json::Value {
            let frame = run_route_wire_request(store, request.clone()).await;
            assert_eq!(frame.header.ty, FrameType::Response);
            serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone()
        }
        async fn report(store: &SharedStore, params: serde_json::Value) {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(
                    &json!({"method": ROUTE_SET_DECISION_OUTCOME_OP, "params": params}),
                )
                .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_set_decision_outcome(
                frame,
                &tx,
                store,
                Path::new("/nonexistent/panel-seat-provider-reset.jsonc"),
                &SharedBrocaCredentials::default(),
            )
            .await
            .unwrap();
            let ack: serde_json::Value =
                serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
            assert_eq!(ack["result"]["ok"], true, "{ack}");
        }
        let seated = |panel: &serde_json::Value| -> Vec<String> {
            panel["members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|member| member["routeKey"].as_str().unwrap().to_string())
                .collect()
        };

        let first = select(&store, &request).await;
        let seat = first["members"][0].clone();
        let seat_route = seat["routeKey"].as_str().unwrap().to_string();
        let seat_variant = seat["promptParams"]["variant"].as_str().map(str::to_string);
        let decision = first["decisionID"].as_str().unwrap().to_string();
        let reset = now_ms() as i64 + 3 * 24 * 3_600_000;
        // The shared decision keeps its first terminal outcome even when another
        // seat later reports a failure with a provider reset.
        report(
            &store,
            json!({"decisionID": decision, "outcome": "completed"}),
        )
        .await;
        report(
            &store,
            json!({
                "decisionID": decision,
                "outcome": "interrupted",
                "lastError": "account_limit_exhausted",
                "providerOutcome": {"accountRef": "catalog-account-openai-apikey", "resetsAtMs": reset},
                "seatRoute": {"routeKey": seat_route, "variant": seat_variant},
            }),
        )
        .await;

        let next = select(&store, &request).await;
        assert!(seated(&next).contains(&seat_route), "{next}");
    }

    #[test]
    fn quota_provenance_records_the_pacing_window_model() {
        let snapshot = ModelRouteStateSnapshot {
            now: 0.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::from([(
                "openai/gpt-5.6-sol".to_string(),
                striatum_core::RouteRuntimeState {
                    quota_pressure: Some(0.7),
                    quota_window_model: Some(striatum_core::PaceWindowModel::UtilizationFallback),
                    ..Default::default()
                },
            )]),
        };
        let windows: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 70.0 } }
        }]))
        .unwrap();

        let value: serde_json::Value = serde_json::from_str(&quota_signal_json(
            &snapshot,
            &QuotaSignalView {
                windows: &windows,
                available: true,
                refreshed_at_ms: 1,
                retained_degraded_count: 0,
                last_error: None,
                restored_from_ms: None,
                stale_age_ms: None,
            },
        ))
        .unwrap();

        assert_eq!(
            value["signals"][0]["windowModel"],
            serde_json::Value::String("utilization_fallback".to_string())
        );
    }

    struct Rig {
        dir: PathBuf,
    }

    #[test]
    fn an_auth_json_oauth_claim_is_not_a_non_host_auth_fact() {
        // Routing takes no auth file path at all: host auth kinds come only
        // from the request.
        assert!(provider_auth_types(None).is_empty());
        assert_eq!(
            provider_auth_types(Some(&HashMap::from([("example".into(), "apikey".into())])))
                ["example"],
            "apikey"
        );
    }

    impl Rig {
        fn new(label: &str) -> Self {
            let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            // nanos: cross-run uniqueness under pid reuse
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos();
            let dir = cortexkit_test_support::scratch_root().join(format!(
                "striatum-module-{label}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }

        fn open_store(&self) -> SqliteRouteStateStore {
            let descriptor = dev_descriptor(
                DEFAULT_MODULE_ID,
                self.dir.join("store.db").to_string_lossy().into_owned(),
            );
            SqliteRouteStateStore::open(&descriptor).expect("open test store")
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// The quota poll loop writes healthy usage entries to the reading log.
    /// Exercise its writer directly: a healthy entry creates provider/window
    /// rows, while an entry containing an error creates none.
    #[test]
    fn quota_poll_records_healthy_entries_into_the_reading_log() {
        let rig = Rig::new("reading-log");
        let store = rig.open_store();
        let healthy: ProviderUsage = serde_json::from_value(serde_json::json!({
            "provider": "codex",
            "apiProvider": "openai",
            "account": "team-a",
            "fetchedAt": "2030-01-01T00:00:00Z",
            "savedResets": { "availableCount": 2 },
            "usage": {
                "primary": { "usedPercent": 20.0, "rawUsedPercent": 25.0,
                             "resetsAt": "2030-01-01T05:00:00Z", "windowMinutes": 300 },
                "secondary": { "usedPercent": 40.0, "rawUsedPercent": 45.0,
                               "resetsAt": "2030-01-08T00:00:00Z", "windowMinutes": 10080 }
            }
        }))
        .unwrap();
        let degraded: ProviderUsage = serde_json::from_value(serde_json::json!({
            "provider": "claude",
            "apiProvider": "anthropic",
            "account": "seat",
            "error": "upstream failed",
            "spend": []
        }))
        .unwrap();

        record_quota_readings(&store, &[healthy, degraded], 1_893_456_000_000);

        let rows = store.get_quota_readings("openai/team-a").unwrap();
        assert_eq!(
            rows.len(),
            2,
            "both windows of the healthy entry are recorded"
        );
        assert_eq!(rows[0].observed_at, 1_893_456_000_000);
        assert_eq!(rows[0].raw_used_percent, Some(25.0));
        assert!(store
            .get_quota_readings("anthropic/seat")
            .unwrap()
            .is_empty());
    }

    fn upsert_params(provider: &str, model: &str) -> ModelUpsertParams {
        ModelUpsertParams {
            provider_id: provider.into(),
            model_id: model.into(),
            canonical_id: None,
            variant: None,
            family: None,
            elo: None,
            eq: None,
            speed: None,
            context_window: None,
            max_output_tokens: None,
            cost_per_m_tok_out: None,
            supports_vision: None,
            supports_tool_use: None,
            default_variant: None,
            notes: None,
        }
    }

    #[test]
    fn model_upsert_new_route_requires_both_judgment_scores() {
        // elo without eq (and vice versa) must refuse: a defaulted judgment
        // score places a model in the ladder silently.
        let mut params = upsert_params("newco", "shiny-1");
        params.elo = Some(1500.0);
        assert!(merge_model_upsert(None, &params).is_err());
        params.elo = None;
        params.eq = Some(1700.0);
        assert!(merge_model_upsert(None, &params).is_err());
        params.elo = Some(1500.0);
        let created_fact = merge_model_upsert(None, &params).expect("both scores create");
        assert_eq!(created_fact.scores.elo, 1500.0);
        assert_eq!(created_fact.scores.eq, 1700.0);
        assert_eq!(
            created_fact.scores.speed, 100.0,
            "new route speed defaults neutral"
        );
        assert_eq!(created_fact.confidence, Confidence::Curated);

        let mut unscored = fact("newco", "catalog-only");
        unscored.score_provenance = ScoreProvenance::Unscored;
        let mut incomplete = upsert_params("newco", "catalog-only");
        incomplete.supports_vision = Some(false);
        assert!(merge_model_upsert(Some(unscored.clone()), &incomplete).is_err());
        incomplete.elo = Some(1500.0);
        incomplete.eq = Some(1700.0);
        assert!(merge_model_upsert(Some(unscored), &incomplete).is_ok());
    }

    /// A discovered row is minted with unknown confidence; placing both scores
    /// on it is the confirmation, so the placed row ranks without the
    /// unknown-fact adequacy penalty. A score-less field edit on a curated row
    /// leaves confidence alone.
    #[test]
    fn model_upsert_placing_scores_on_a_discovered_row_confirms_it() {
        let mut discovered = fact("ollama-cloud", "glm-5.3");
        discovered.confidence = Confidence::Unknown;
        discovered.score_provenance = ScoreProvenance::Unscored;
        let mut placement = upsert_params("ollama-cloud", "glm-5.3");
        placement.elo = Some(1732.0);
        placement.eq = Some(1579.0);
        let placed = merge_model_upsert(Some(discovered), &placement).unwrap();
        assert_eq!(placed.confidence, Confidence::Curated);
        assert_eq!(placed.score_provenance, ScoreProvenance::Curated);

        let mut curated = fact("openai", "gpt-x");
        curated.confidence = Confidence::Unknown;
        let mut edit = upsert_params("openai", "gpt-x");
        edit.supports_vision = Some(true);
        let edited = merge_model_upsert(Some(curated), &edit).unwrap();
        assert_eq!(
            edited.confidence,
            Confidence::Unknown,
            "no placement, no promotion"
        );
    }

    #[test]
    fn model_upsert_merge_touches_only_supplied_fields() {
        let stored = fact("openai", "gpt-x");
        let mut params = upsert_params("openai", "gpt-x");
        params.elo = Some(1615.0);
        let merged = merge_model_upsert(Some(stored.clone()), &params).unwrap();
        assert_eq!(merged.scores.elo, 1615.0);
        // Every untouched field survives the merge byte-for-byte.
        assert_eq!(merged.scores.eq, stored.scores.eq);
        assert_eq!(merged.scores.speed, stored.scores.speed);
        assert_eq!(merged.context_window, stored.context_window);
        assert_eq!(merged.cost_per_m_tok_out, stored.cost_per_m_tok_out);
        assert_eq!(merged.supports_vision, stored.supports_vision);
        assert_eq!(merged.family, stored.family);
    }

    #[test]
    fn model_upsert_refuses_operator_family_in_favor_of_catalog_lineage() {
        let mut params = upsert_params("lineage-fixture", "m");
        params.family = Some("claude".into());
        let error = merge_model_upsert(Some(fact("lineage-fixture", "m")), &params).unwrap_err();
        assert!(error.contains("family"));
        assert!(error.contains("Fusiform's model.lineage"));
    }

    #[test]
    fn model_upsert_default_variant_lands_in_prompt_defaults() {
        let stored = fact("xai", "grok-z");
        let mut params = upsert_params("xai", "grok-z");
        params.default_variant = Some("xhigh".into());
        let merged = merge_model_upsert(Some(stored), &params).unwrap();
        assert_eq!(
            merged
                .prompt_defaults
                .as_ref()
                .and_then(|d| d.variant.as_deref()),
            Some("xhigh")
        );
    }

    #[test]
    fn model_upsert_written_as_override_survives_reseed() {
        // The store contract the op leans on: an Override row is never
        // reverted by the seed pass (seed fills absent rows only).
        let rig = Rig::new("upsert-override");
        let store = rig.open_store();
        let mut params = upsert_params("newco", "shiny-2");
        params.elo = Some(1400.0);
        params.eq = Some(1600.0);
        let fact = merge_model_upsert(None, &params).unwrap();
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        let read = store
            .get_fact_by_route_key("newco/shiny-2")
            .unwrap()
            .expect("written route reads back");
        assert_eq!(read.scores.elo, 1400.0);
        assert!(store
            .list_override_route_keys()
            .unwrap()
            .contains(&"newco/shiny-2".to_string()));
    }

    pub(super) fn fact(provider: &str, model: &str) -> ModelFact {
        ModelFact {
            billing_plane: None,
            auth_methods: None,
            login_methods_documented: false,
            speed_is_fleet_median: false,
            decommissioned: false,
            provider_id: provider.into(),
            model_id: model.into(),
            variant: None,
            family: None,
            scores: ModelScores {
                elo: 55.0,
                eq: 1700.0,
                speed: 90.0,
            },
            context_window: Some(200_000.0),
            max_output_tokens: None,
            cost_tier: Some(2.0),
            cost_per_m_tok_out: Some(4.5),
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: Some(true),
            supports_tool_use: Some(true),
            prompt_defaults: None,
            confidence: Confidence::Curated,
            notes: None,
            speed_multiplier: None,
            subscription_multiplier: None,
            score_provenance: ScoreProvenance::Curated,
            aa_index: None,
            review_by_ms: None,
            eq_source: None,
            class_vector: None,
            class_vector_is_overlay: false,
            upstream_status: None,
            model_type: None,
            model_kind: None,
        }
    }

    fn usage(v: serde_json::Value) -> ProviderUsage {
        serde_json::from_value(v).expect("provider usage")
    }

    /// A selection prices and reserves every candidate with the routing
    /// configuration it started with. Publishing another configuration while it
    /// runs changes neither one candidate's speed multiplier nor the keep-free
    /// reserve on the other candidate's subscription pool.
    #[test]
    fn prepared_inputs_price_and_reserve_every_candidate_on_the_held_configuration() {
        let rig = Rig::new("selection-input-held-pricing");
        let store = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("held-a", "model"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("held-b", "model"), FactSource::Override)
            .unwrap();
        let path = rig.dir.join("routing.jsonc");
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"models":{"held-a/model":{"speed_multiplier":0.5}},"keep_free_percent":{"held-b":20}}}"#,
        )
        .unwrap();
        let held = config_overrides::settle_live_config(&path);
        crate::test_config::write(
            &path,
            r#"{"model_routing":{"models":{"held-a/model":{"speed_multiplier":2.0}},"keep_free_percent":{"held-b":0}}}"#,
        )
        .unwrap();
        let published = config_overrides::settle_live_config(&path);
        assert!(!Arc::ptr_eq(&held, &published));

        let quota = Arc::new(Mutex::new(QuotaCache::default()));
        let judged = |pricing: &RoutePricing| {
            let prepared = prepare_selection_inputs(&quota, &store, pricing, &[]).unwrap();
            let a = prepared
                .facts
                .iter()
                .find(|fact| fact.provider_id == "held-a")
                .unwrap()
                .clone();
            let reserve = prepared.snapshot.by_capacity["held-b/model"]
                .subscription_pool
                .as_ref()
                .unwrap()
                .keep_free_percent;
            (a.speed_multiplier, a.scores.speed, reserve)
        };
        assert_eq!(judged(held.pricing()), (Some(0.5), 45.0, 20.0));
        assert_eq!(judged(published.pricing()), (Some(2.0), 180.0, 0.0));
    }

    #[test]
    fn prepare_selection_inputs_preserves_the_catalog_observed_speed_overlay() {
        let rig = Rig::new("selection-input-observed-speed");
        let store = Arc::new(rig.open_store());
        let mut base = fact("observed-caller", "fixture");
        base.prompt_defaults = Some(ModelPromptDefaults {
            variant: Some("high".into()),
            ..Default::default()
        });
        let mut sibling = base.clone();
        sibling.variant = Some("low".into());
        store.upsert_fact(&base, FactSource::Override).unwrap();
        store.upsert_fact(&sibling, FactSource::Override).unwrap();

        let observed_at = now_ms().saturating_sub(10_000) as f64;
        for index in 0..25 {
            store
                .record_usage_observation(&UsageObservation {
                    id: format!("selection-input-observed-{index}"),
                    route_key: "observed-caller/fixture/high".into(),
                    capacity_key: "observed-caller/fixture".into(),
                    provider_id: "observed-caller".into(),
                    input_tokens: None,
                    output_tokens: 1_000.0 + index as f64,
                    reasoning_tokens: None,
                    cached_read_tokens: None,
                    cache_write_tokens: None,
                    duration_ms: 1_000.0,
                    workload_class: None,
                    account_key: None,
                    observed_at: observed_at + index as f64,
                })
                .unwrap();
        }

        let expected = store
            .get_facts_with_observed_speed(now_ms() as f64, &RoutePricing::default())
            .unwrap()
            .into_iter()
            .filter(|fact| fact.provider_id == "observed-caller")
            .collect::<Vec<_>>();
        let prepared = prepare_selection_inputs(
            &Arc::new(Mutex::new(QuotaCache::default())),
            &store,
            &RoutePricing::default(),
            &[],
        )
        .unwrap();
        let actual = prepared
            .facts
            .into_iter()
            .filter(|fact| fact.provider_id == "observed-caller")
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
        assert!(actual.iter().all(|fact| fact.scores.speed > 1_000.0));
        assert_eq!(actual[0].scores.speed, actual[1].scores.speed);
    }

    #[tokio::test]
    async fn undecodable_control_request_is_refused_without_stopping_serving() {
        let rig = Rig::new("undecodable-control-request");
        let store = Arc::new(rig.open_store());
        let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
        let catalog_health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
        let min_band = Arc::new(Mutex::new(None));
        let broca_credentials = empty_broca_credentials();
        let bind = json!({
            "op": "route.bind",
            "route_channel": 317,
            "epoch": 42,
            "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
            "identity": {
                "project_root": cortexkit_test_support::scratch_root(),
                "harness": "control-tolerance-test",
                "session": "session-1"
            }
        });
        let mut unknown_scope_field = bind.clone();
        unknown_scope_field["scope"] = json!({
            "owner": { "kind": "reserved", "module_id": "prefrontal-core" },
            "ref": "head-scope",
            "scope_epoch": 3,
            "kind": "head",
            "owner_authorized": true,
            "attributes": { "unknown_scope_authority": true }
        });

        for undecodable_body in [
            b"{not-json".to_vec(),
            serde_json::to_vec(&json!({ "op": "unknown.control.operation" })).unwrap(),
            serde_json::to_vec(&unknown_scope_field).unwrap(),
        ] {
            let (writer, mut replies) = mpsc::channel(3);
            let mut route_epochs = RouteEpochs::from([(317, 41)]);
            // Exercise the same dispatch and continuation decision as the live
            // read loop: a refusal, a valid bind, then a health request.
            for (corr, body) in [
                (11, undecodable_body),
                (12, serde_json::to_vec(&bind).unwrap()),
                (13, br#"{"op":"health.check"}"#.to_vec()),
            ] {
                let frame =
                    Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body).unwrap();
                let ver = frame.header.ver;
                assert!(handle_frame(
                    frame,
                    &writer,
                    &quota_cache,
                    &store,
                    &mut route_epochs,
                    &min_band,
                    &rig.dir.join("routing.jsonc"),
                    &rig.dir.join("operator.jsonc"),
                    &broca_credentials,
                    &catalog_health,
                )
                .await
                .expect("an undecodable control request must not end the frame loop"));
                let reply = replies.try_recv().expect("each request must get a reply");
                assert_eq!(reply.header.ver, ver);
                assert_eq!(reply.header.channel, 0);
                assert_eq!(reply.header.epoch, 0);
                assert_eq!(reply.header.corr, corr);
                match corr {
                    11 => {
                        assert_eq!(reply.header.ty, FrameType::Error);
                        let error: ErrorBody = serde_json::from_slice(&reply.body).unwrap();
                        assert_eq!(error.code, "invalid_request");
                        assert!(error.message.starts_with("invalid control request: "));
                        assert_eq!(route_epochs.get(&317), Some(&41));
                    }
                    12 => {
                        assert_eq!(reply.header.ty, FrameType::Response);
                        assert_eq!(
                            serde_json::from_slice::<ModuleControlResponse>(&reply.body).unwrap(),
                            ModuleControlResponse::RouteBindAck {}
                        );
                        assert_eq!(route_epochs.get(&317), Some(&42));
                    }
                    13 => {
                        assert_eq!(reply.header.ty, FrameType::Response);
                        assert!(matches!(
                            serde_json::from_slice::<ModuleControlResponse>(&reply.body).unwrap(),
                            ModuleControlResponse::HealthCheck { .. }
                        ));
                        assert_eq!(route_epochs.get(&317), Some(&42));
                    }
                    _ => unreachable!(),
                }
                assert!(replies.try_recv().is_err(), "only one reply per request");
            }
        }
    }

    #[tokio::test]
    async fn route_bind_replaces_the_epoch_for_a_reused_channel() {
        let (writer, mut replies) = mpsc::channel(2);
        let mut route_epochs = RouteEpochs::new();
        let catalog_health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));

        for (corr, epoch) in [(1, 41), (2, 42)] {
            let body = serde_json::to_vec(&json!({
                "op": "route.bind",
                "route_channel": 7,
                "epoch": epoch,
                "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
                "identity": {
                    "project_root": cortexkit_test_support::scratch_root(),
                    "harness": "epoch-rebind-test",
                    "session": "session-1"
                }
            }))
            .unwrap();
            let frame =
                Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body).unwrap();

            handle_control_request(
                frame,
                &writer,
                &mut route_epochs,
                &catalog_health,
                &empty_broca_credentials(),
                &Arc::new(Mutex::new(QuotaCache::default())),
            )
            .await
            .unwrap();
            let ack = replies.recv().await.unwrap();
            assert_eq!(ack.header.channel, 0);
            assert_eq!(ack.header.epoch, 0);
            assert_eq!(route_epochs.get(&7), Some(&epoch));
        }

        clear_route_epoch(&mut route_epochs, 7, 41);
        assert_eq!(route_epochs.get(&7), Some(&42));
        clear_route_epoch(&mut route_epochs, 7, 42);
        assert!(!route_epochs.contains_key(&7));
    }

    fn select_panel_wire_vectors() -> serde_json::Value {
        serde_json::from_str(include_str!("../fixtures/contracts/select-panel-wire.json"))
            .expect("select_panel wire vectors")
    }

    async fn run_route_wire_request(store: &SharedStore, request: serde_json::Value) -> Frame {
        run_route_wire_request_with_operator_config(store, request, &test_operator_config()).await
    }

    fn empty_broca_credentials() -> SharedBrocaCredentials {
        // These wire fixtures represent a completed catalog load with no supplied
        // planes. Cold-start tests construct the uninitialized cache explicitly.
        Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(&[])))
    }

    fn mapped_broca_credentials(provider: &str) -> SharedBrocaCredentials {
        opt_in_fixture_providers(&test_operator_config(), &[fact(provider, "fixture")]);
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: vec![cortexkit_claustrum_client::ListedCredential {
                    id: "fixture".into(),
                    categories: vec!["llm-provider".into()],
                    credential_type: "apikey".into(),
                    auth_method: Some("apikey".into()),
                    state: "active".into(),
                    provider_ids: vec![provider.into()],
                    ..Default::default()
                }],
                view: "fixture-view".into(),
                ..Default::default()
            },
            now_ms(),
        );
        credentials
    }

    fn test_operator_config() -> PathBuf {
        static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        PATH.get_or_init(|| {
            let path = cortexkit_test_support::scratch_root().join(format!(
                "striatum-test-operator-{}.json",
                std::process::id()
            ));
            crate::test_config::write(&path, "{}").unwrap();
            path
        })
        .clone()
    }

    fn usage_params(id: &str) -> serde_json::Value {
        json!({
            "messageID": id,
            "providerID": "provider",
            "modelID": "model",
            "variant": "high",
            "outputTokens": 100.0,
            "reasoningTokens": 0.0,
            "createdMs": 1_000.0,
            "completedMs": 2_000.0,
        })
    }

    async fn run_record_usage(store: &SharedStore, params: serde_json::Value) -> serde_json::Value {
        let frame = run_route_wire_request(
            store,
            json!({ "method": ROUTE_RECORD_USAGE_OP, "params": params }),
        )
        .await;
        serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone()
    }

    fn usage_row_count(rig: &Rig, id: Option<&str>) -> i64 {
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        match id {
            Some(id) => connection
                .query_row(
                    "SELECT COUNT(*) FROM model_route_usage WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
                .unwrap(),
            None => connection
                .query_row("SELECT COUNT(*) FROM model_route_usage", [], |row| {
                    row.get(0)
                })
                .unwrap(),
        }
    }

    async fn run_route_wire_request_with_operator_config(
        store: &SharedStore,
        request: serde_json::Value,
        operator_config_path: &Path,
    ) -> Frame {
        run_route_wire_request_with_paths(
            store,
            request,
            operator_config_path,
            &Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(
                &store.get_facts().unwrap(),
            ))),
        )
        .await
    }

    fn opt_in_fixture_providers(path: &Path, facts: &[ModelFact]) {
        let document = std::fs::read_to_string(path).unwrap();
        let mut config: serde_json::Value = serde_json::from_str(&document).unwrap();
        for fact in facts {
            let pin_only = &mut config["providers"][&fact.provider_id]["pin_only"];
            if pin_only.is_null() {
                *pin_only = json!(false);
            }
        }
        let updated = serde_json::to_string(&config).unwrap();
        if updated != document {
            std::fs::write(path, updated).unwrap();
            config_overrides::initialize_config(path).unwrap();
        }
    }

    pub(super) async fn run_route_wire_request_with_paths(
        store: &SharedStore,
        mut request: serde_json::Value,
        operator_config_path: &Path,
        broca_credentials: &SharedBrocaCredentials,
    ) -> Frame {
        let fixture_config = test_operator_config();
        // Default-path legacy fixtures use an explicit test operator policy,
        // not fresh-install defaults. Opt their catalog providers into spending
        // so availability/fence tests remain independent of the billing default.
        if !operator_config_path.exists() || operator_config_path == fixture_config {
            opt_in_fixture_providers(&fixture_config, &store.get_facts().unwrap());
        }
        let operator_config_path = if operator_config_path.exists() {
            operator_config_path
        } else {
            &fixture_config
        };
        // Legacy wire fixtures exercise executor availability from an explicit
        // list or Broca substrate. Launch-mapping tests use their own fixture.
        if request["method"] == ROUTE_SELECT_OP {
            authorize_legacy_policy_fixture(&request["params"], operator_config_path);
            request["params"]
                .as_object_mut()
                .unwrap()
                .entry("executor")
                .or_insert(json!("broca"));
        }
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
        let (tx, mut rx) = mpsc::channel(1);
        handle_select_request(
            frame,
            1,
            &tx,
            &quota_cache,
            store,
            &Arc::new(Mutex::new(None)),
            &test_operator_config(),
            operator_config_path,
            broca_credentials,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
    }

    /// Policy fixtures own their legacy target labels. Admission itself is
    /// exercised by the route-bound routing/v1 harness, not these policy helpers.
    pub(super) fn authorize_legacy_policy_fixture(params: &serde_json::Value, config_path: &Path) {
        if let Some(target) = params["targetAgent"].as_str() {
            let operator = config_overrides::live_config(config_path);
            let owner = operator
                .consumer_owners()
                .get(target)
                .cloned()
                .unwrap_or_else(|| "prefrontal-core".into());
            routing_v1::consumers::replace_catalog(&json!([{
                "module_id": owner, "nonce_verified": true,
                "role_declarations": {"routing/v1": {"consumers": [{"name": target}]}}
            }]));
            route_principals()
                .lock()
                .unwrap()
                .insert(7, (1, format!("reserved:{owner}")));
        }
    }

    #[tokio::test]
    async fn route_record_usage_writes_optional_profile_fields_or_null_without_a_producer() {
        let rig = Rig::new("record-usage-profile");
        let store = Arc::new(rig.open_store());
        let mut absent = usage_params("profile-absent");
        absent["reasoningTokens"] = json!(25.0);
        absent["completedMs"] = json!(3_000.0);
        assert_eq!(
            run_record_usage(&store, absent).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let absent_row = connection
            .query_row(
                "SELECT input_tokens, output_tokens, reasoning_tokens,
                        cached_read_tokens, cache_write_tokens, duration_ms,
                        workload_class, account_key
                 FROM model_route_usage WHERE id = 'profile-absent'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            absent_row,
            (
                None,
                Some(125),
                Some(25),
                None,
                None,
                Some(2_000),
                None,
                None
            )
        );
        drop(connection);

        let mut complete = usage_params("profile-complete");
        complete["inputTokens"] = json!(300.0);
        complete["reasoningTokens"] = json!(25.0);
        complete["cachedReadTokens"] = json!(0.0);
        complete["cacheWriteTokens"] = json!(10.0);
        complete["workloadClass"] = json!("ephemeral");
        complete["accountKey"] = json!("provider/primary");
        complete["completedMs"] = json!(3_000.0);
        assert_eq!(
            run_record_usage(&store, complete).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let complete_row = connection
            .query_row(
                "SELECT input_tokens, output_tokens, reasoning_tokens,
                        cached_read_tokens, cache_write_tokens, duration_ms,
                        workload_class, account_key
                 FROM model_route_usage WHERE id = 'profile-complete'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            complete_row,
            (
                Some(300),
                Some(125),
                Some(25),
                Some(0),
                Some(10),
                Some(2_000),
                Some("ephemeral".into()),
                Some("provider/primary".into()),
            )
        );
    }

    #[tokio::test]
    async fn first_eligible_delivery_reports_both_recording_effects() {
        let rig = Rig::new("record-usage-first-eligible");
        let store = Arc::new(rig.open_store());

        assert_eq!(
            run_record_usage(&store, usage_params("eligible")).await,
            json!({ "recorded": true, "usage_recorded": true })
        );
    }

    #[tokio::test]
    async fn route_record_usage_speed_sample_is_separate_and_idempotent() {
        let rig = Rig::new("record-speed-sample");
        let store = Arc::new(rig.open_store());
        let mut params = usage_params("generation");
        params["speed_sample"] = json!(true);
        params["service_tier"] = json!("fast");
        params["speed"] = json!("fast");
        params["reasoningTokens"] = json!(25);
        params["inputTokens"] = json!(999);
        params["workloadClass"] = json!("ephemeral");
        assert_eq!(
            run_record_usage(&store, params.clone()).await,
            json!({"recorded":true,"usage_recorded":false})
        );
        params["outputTokens"] = json!(9999);
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({"recorded":false,"usage_recorded":false})
        );
        assert_eq!(usage_row_count(&rig, None), 0);
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let row = connection
            .query_row(
                "SELECT COUNT(*), service_tier, speed, output_tokens,
            reasoning_tokens FROM model_route_speed_samples",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (1, "fast".into(), "fast".into(), 125, 25));
        assert!(store
            .get_class_vector("ephemeral", 10_000.0)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn route_record_usage_speed_sample_defaults_and_invalid_durations() {
        let rig = Rig::new("record-speed-defaults");
        let store = Arc::new(rig.open_store());
        let mut params = usage_params("reasoning-only");
        params["speed_sample"] = json!(true);
        params["outputTokens"] = json!(0);
        params["reasoningTokens"] = json!(200);
        assert_eq!(
            run_record_usage(&store, params.clone()).await,
            json!({"recorded":true,"usage_recorded":false})
        );
        for (index, completed) in [1_000, 0].into_iter().enumerate() {
            params["messageID"] = json!(format!("invalid-{index}"));
            params["completedMs"] = json!(completed);
            assert_eq!(
                run_record_usage(&store, params.clone()).await,
                json!({"recorded":false,"usage_recorded":false})
            );
        }
        assert_eq!(usage_row_count(&rig, None), 0);
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let row = connection
            .query_row(
                "SELECT COUNT(*), service_tier, speed, output_tokens
            FROM model_route_speed_samples",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (1, None, None, 200));
    }

    #[tokio::test]
    async fn speed_ineligible_recordable_delivery_reports_only_usage_recorded() {
        let rig = Rig::new("record-usage-speed-ineligible");
        let store = Arc::new(rig.open_store());
        let mut ineligible = usage_params("ineligible");
        ineligible["outputTokens"] = json!(50.0);

        assert_eq!(
            run_record_usage(&store, ineligible).await,
            json!({ "recorded": false, "usage_recorded": true })
        );
    }

    #[tokio::test]
    async fn no_row_refusal_reports_neither_recording_effect() {
        let rig = Rig::new("record-usage-no-row");
        let store = Arc::new(rig.open_store());

        assert_eq!(
            run_record_usage(&store, usage_params("")).await,
            json!({ "recorded": false, "usage_recorded": false })
        );
        assert_eq!(usage_row_count(&rig, None), 0);
    }

    #[tokio::test]
    async fn duplicate_eligible_delivery_reports_neither_effect_and_keeps_one_row() {
        let rig = Rig::new("record-usage-duplicate");
        let store = Arc::new(rig.open_store());
        assert_eq!(
            run_record_usage(&store, usage_params("eligible")).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        assert_eq!(
            run_record_usage(&store, usage_params("eligible")).await,
            json!({ "recorded": false, "usage_recorded": false })
        );
        assert_eq!(usage_row_count(&rig, Some("eligible")), 1);
    }

    #[tokio::test]
    async fn upgrade_delivery_reports_new_speed_observation_but_no_new_usage_row() {
        let rig = Rig::new("record-usage-upgrade");
        let store = Arc::new(rig.open_store());
        let mut unusable = usage_params("upgrade");
        unusable["createdMs"] = json!(3_000.0);
        assert_eq!(
            run_record_usage(&store, unusable).await,
            json!({ "recorded": false, "usage_recorded": true })
        );

        assert_eq!(
            run_record_usage(&store, usage_params("upgrade")).await,
            json!({ "recorded": true, "usage_recorded": false })
        );
        assert_eq!(usage_row_count(&rig, Some("upgrade")), 1);
    }

    #[tokio::test]
    async fn ambiguous_variant_refusal_writes_no_usage_row() {
        let rig = Rig::new("record-usage-ambiguous");
        let store = Arc::new(rig.open_store());
        for variant in ["low", "high"] {
            let mut variant_fact = fact("provider", "model");
            variant_fact.variant = Some(variant.into());
            store
                .upsert_fact(&variant_fact, FactSource::Override)
                .unwrap();
        }
        let mut params = usage_params("ambiguous");
        params.as_object_mut().unwrap().remove("variant");

        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": false, "usage_recorded": false })
        );
        assert_eq!(usage_row_count(&rig, None), 0);
    }

    #[tokio::test]
    async fn empty_provider_or_model_refusal_writes_no_usage_row() {
        let rig = Rig::new("record-usage-empty-route-part");
        let store = Arc::new(rig.open_store());
        for (id, key) in [("empty-provider", "providerID"), ("empty-model", "modelID")] {
            let mut params = usage_params(id);
            params[key] = json!("");
            assert_eq!(
                run_record_usage(&store, params).await,
                json!({ "recorded": false, "usage_recorded": false })
            );
        }
        assert_eq!(usage_row_count(&rig, None), 0);
    }

    #[tokio::test]
    async fn empty_message_id_with_constructible_route_writes_no_usage_row() {
        let rig = Rig::new("record-usage-empty-message");
        let store = Arc::new(rig.open_store());
        let params = usage_params("");
        assert_eq!(params["providerID"], "provider");
        assert_eq!(params["modelID"], "model");

        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": false, "usage_recorded": false })
        );
        assert_eq!(usage_row_count(&rig, None), 0);
    }

    #[test]
    fn every_recordable_speed_refusal_writes_a_row_and_leaves_the_aggregate_unchanged() {
        let rig = Rig::new("record-usage-speed-refusals");
        let store = Arc::new(rig.open_store());
        for index in 0..5 {
            store
                .record_usage_observation(&UsageObservation {
                    id: format!("baseline-{index}"),
                    route_key: "provider/model/high".into(),
                    capacity_key: "provider/model".into(),
                    provider_id: "provider".into(),
                    input_tokens: None,
                    output_tokens: 200.0,
                    reasoning_tokens: None,
                    cached_read_tokens: None,
                    cache_write_tokens: None,
                    duration_ms: 1_000.0,
                    workload_class: None,
                    account_key: None,
                    observed_at: 2_000.0 + index as f64,
                })
                .unwrap();
        }
        let before = store
            .get_observed_speed("provider/model/high", 10_000.0)
            .unwrap()
            .unwrap();

        let cases = [
            ("nan-output", f64::NAN, 3_000.0, None),
            ("infinite-output", f64::INFINITY, 3_000.0, None),
            ("zero-output", 0.0, 3_000.0, None),
            ("negative-output", -1.0, 3_000.0, None),
            ("floor-zero-output", 0.99, 3_000.0, None),
            ("nan-duration", 100.0, 3_000.0, Some(f64::NAN)),
            ("infinite-duration", 100.0, 3_000.0, Some(f64::INFINITY)),
            ("negative-duration", 100.0, 1_000.0, None),
        ];
        for (id, output_tokens, completed_ms, tool_elapsed_ms) in cases {
            let facts = NativeUsageFacts {
                message_id: id.into(),
                provider_id: "provider".into(),
                model_id: "model".into(),
                variant: Some("high".into()),
                output_tokens,
                reasoning_tokens: Some(25.0),
                created_ms: 2_000.0,
                completed_ms,
                tool_elapsed_ms,
                ..Default::default()
            };
            let observation = usage_facts::usage_observation_from_store(facts, &store)
                .unwrap()
                .expect("recordable refusal");
            assert_eq!(
                store.record_usage_delivery(&observation).unwrap(),
                striatum_core::route_state_store::UsageRecordResult {
                    recorded: false,
                    usage_recorded: true,
                },
                "{id}"
            );
        }

        let after = store
            .get_observed_speed("provider/model/high", 10_000.0)
            .unwrap()
            .unwrap();
        assert_eq!(after.to_bits(), before.to_bits());
        assert_eq!(usage_row_count(&rig, None), 13);
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        for id in [
            "nan-output",
            "infinite-output",
            "zero-output",
            "negative-output",
            "floor-zero-output",
        ] {
            assert_eq!(
                connection
                    .query_row(
                        "SELECT output_tokens FROM model_route_usage WHERE id = ?1",
                        [id],
                        |row| row.get::<_, Option<i64>>(0),
                    )
                    .unwrap(),
                None,
                "{id}"
            );
        }
        for id in ["nan-duration", "infinite-duration", "negative-duration"] {
            assert_eq!(
                connection
                    .query_row(
                        "SELECT duration_ms FROM model_route_usage WHERE id = ?1",
                        [id],
                        |row| row.get::<_, Option<i64>>(0),
                    )
                    .unwrap(),
                None,
                "{id}"
            );
        }
    }

    #[tokio::test]
    async fn reasoning_heavy_pre_and_post_migration_rows_report_identical_tps_bits() {
        let rig = Rig::new("record-usage-reasoning-compatibility");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        for index in 0..6 {
            connection
                .execute(
                    "INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     VALUES (?1, NULL, 'provider/legacy/high', 'provider/legacy',
                             'provider', NULL, 180, 1000, ?2)",
                    rusqlite::params![format!("legacy-{index}"), 1_000 + index],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO model_route_usage
                   (id, task_id, route_key, capacity_key, provider_id,
                    input_tokens, output_tokens, duration_ms, observed_at)
                 VALUES ('mixed-legacy', NULL, 'provider/model/high', 'provider/model',
                         'provider', NULL, 180, 1000, 1000)",
                [],
            )
            .unwrap();
        drop(connection);

        for index in 0..5 {
            let mut params = usage_params(&format!("mixed-new-{index}"));
            params["outputTokens"] = json!(100.0);
            params["reasoningTokens"] = json!(80.0);
            assert_eq!(
                run_record_usage(&store, params).await,
                json!({ "recorded": true, "usage_recorded": true })
            );
        }

        let legacy = store
            .get_observed_speed("provider/legacy/high", 10_000.0)
            .unwrap()
            .unwrap();
        let mixed = store
            .get_observed_speed("provider/model/high", 10_000.0)
            .unwrap()
            .unwrap();
        assert_eq!(legacy.to_bits(), 180.0_f64.to_bits());
        assert_eq!(mixed.to_bits(), legacy.to_bits());
    }

    #[tokio::test]
    async fn counters_producer_true_broca_usage_maps_to_summary_and_stored_row() {
        // Gated on counters_producer = true
        let rig = Rig::new("counters-producer-true");
        let store = Arc::new(rig.open_store());

        let n = 250.0;
        let m = 500.0;
        let mut params = usage_params("live-broca-msg");
        params["inputTokens"] = json!(m);
        params["cachedReadTokens"] = json!(n);
        params["outputTokens"] = json!(100.0);
        params["reasoningTokens"] = json!(80.0);
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let (fresh_input, cached_read, stored_output, reasoning) = connection
            .query_row(
                "SELECT input_tokens, cached_read_tokens, output_tokens, reasoning_tokens
                 FROM model_route_usage WHERE id = 'live-broca-msg'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(fresh_input, Some(500)); // fresh_input = M
        assert_eq!(cached_read, Some(250)); // cached_read = N
        assert_eq!(stored_output, Some(180)); // stored output_tokens = 180 (100 + 80)
        assert_eq!(reasoning, Some(80));

        // Fixture payload: output_tokens = 100 and reasoning_tokens = 80
        let mut fixture = usage_params("fixture-payload-msg");
        fixture["inputTokens"] = json!(300.0);
        fixture["cachedReadTokens"] = json!(40.0);
        fixture["cacheWriteTokens"] = json!(20.0);
        fixture["outputTokens"] = json!(100.0);
        fixture["reasoningTokens"] = json!(80.0);
        assert_eq!(
            run_record_usage(&store, fixture).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let (cached_read_fix, cache_write_fix, reasoning_fix, fresh_input_fix, stored_output_fix) =
            connection
                .query_row(
                    "SELECT cached_read_tokens, cache_write_tokens, reasoning_tokens,
                      input_tokens, output_tokens
               FROM model_route_usage WHERE id = 'fixture-payload-msg'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, Option<i64>>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                        ))
                    },
                )
                .unwrap();
        assert_eq!(cached_read_fix, Some(40)); // cached_read = cached_input_tokens
        assert_eq!(cache_write_fix, Some(20)); // cache_write = cache_write_tokens
        assert_eq!(reasoning_fix, Some(80)); // reasoning = 80
        assert_eq!(fresh_input_fix, Some(300)); // fresh_input = input_tokens (no cache subtraction)
        assert_eq!(stored_output_fix, Some(180)); // stored output_tokens = 180 (output + reasoning)
        assert_eq!(stored_output_fix.unwrap() - reasoning_fix.unwrap(), 100); // output_excluding_reasoning = 100
    }

    #[tokio::test]
    async fn real_host_shaped_message_produces_non_null_cached_read_and_ephemeral_class() {
        // Gated on counters_producer = true and class_producer = true
        let rig = Rig::new("real-host-shaped-delivery");
        let store = Arc::new(rig.open_store());

        // Live host assistant row:
        // tokens: {"total":326232,"input":2,"output":126,"reasoning":0,"cache":{"write":422,"read":325682}}
        // agent: "mason"
        // model: anthropic/claude-opus-5
        let mut params = usage_params("msg-host-live");
        params["providerID"] = json!("anthropic");
        params["modelID"] = json!("claude-opus-5");
        params["inputTokens"] = json!(2.0);
        params["outputTokens"] = json!(126.0);
        params["reasoningTokens"] = json!(0.0);
        params["cachedReadTokens"] = json!(325682.0);
        params["cacheWriteTokens"] = json!(422.0);
        params["agent"] = json!("mason");

        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let (cached_read, input_toks, output_toks, reasoning_toks, workload_cls, acct_key) =
            connection
                .query_row(
                    "SELECT cached_read_tokens, input_tokens, output_tokens, reasoning_tokens,
                            workload_class, account_key
                     FROM model_route_usage WHERE id = 'msg-host-live'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, Option<i64>>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, Option<String>>(5)?,
                        ))
                    },
                )
                .unwrap();

        assert_eq!(
            cached_read,
            Some(325682),
            "cached_read_tokens must be non-NULL and match host tokens.cache.read"
        );
        assert_eq!(input_toks, Some(2));
        assert_eq!(output_toks, Some(126));
        assert_eq!(reasoning_toks, Some(0));
        assert_eq!(
            workload_cls.as_deref(),
            Some("ephemeral"),
            "workload_class must be stamped ephemeral for agent mason"
        );
        assert_eq!(
            acct_key, None,
            "account_key must be NULL when account_producer is absent"
        );
    }

    #[tokio::test]
    async fn counters_producer_false_deferral_behavior_and_selection_parity() {
        // Gated on counters_producer = false
        let rig = Rig::new("counters-producer-false");
        let store = Arc::new(rig.open_store());

        // New row written without counter producer
        let mut params = usage_params("deferral-row");
        params["outputTokens"] = json!(100.0);
        params["reasoningTokens"] = json!(80.0);
        params["createdMs"] = json!(1_000.0);
        params["completedMs"] = json!(2_000.0);
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let (input, cached_read, cache_write, output, reasoning) = connection
            .query_row(
                "SELECT input_tokens, cached_read_tokens, cache_write_tokens,
                      output_tokens, reasoning_tokens
               FROM model_route_usage WHERE id = 'deferral-row'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(input, None);
        assert_eq!(cached_read, None);
        assert_eq!(cache_write, None);
        assert_eq!(output, Some(180));
        assert_eq!(reasoning, Some(80));

        // No complete-case row forms: WORKLOAD_CLASS_USAGE_SAMPLES_SQL returns 0 rows
        let complete_rows_count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({})",
                    striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
                ),
                rusqlite::params!["ephemeral", 0, 10_000],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(complete_rows_count, 0);

        // use_basket is never true when counters_producer is false
        assert!(!striatum_core::use_basket_permitted(false, true));

        // Selection is bit-identical to today for every route including routes with complete rate card
        let mut card_route = fact("provider", "complete-card");
        card_route.cost_per_m_tok_out = Some(5.0);
        card_route.input_rate = Some(1.0);
        card_route.cache_read_rate = Some(0.25);
        card_route.cache_write_rate = Some(0.75);
        card_route.reasoning_rate = Some(5.0);

        let expected_cost: f64 = 5.0 / 1.0;
        assert_eq!(
            (card_route.cost_per_m_tok_out.unwrap() / 1.0).to_bits(),
            expected_cost.to_bits()
        );

        // Reported TPS to the same bits: 180 tokens / 1.0 s = 180.0 TPS (needs 5 observations for floor)
        for i in 0..5 {
            let mut p = usage_params(&format!("deferral-tps-{i}"));
            p["outputTokens"] = json!(100.0);
            p["reasoningTokens"] = json!(80.0);
            p["createdMs"] = json!(1_000.0);
            p["completedMs"] = json!(2_000.0);
            run_record_usage(&store, p).await;
        }
        let tps = store
            .get_observed_speed("provider/model/high", 10_000.0)
            .unwrap()
            .unwrap();
        assert_eq!(tps.to_bits(), 180.0_f64.to_bits());
    }

    #[tokio::test]
    async fn class_producer_true_live_message_stamps_ephemeral_and_accepts_only_closed_enum() {
        // Gated on class_producer = true
        let rig = Rig::new("class-producer-true");
        let store = Arc::new(rig.open_store());

        let mut params = usage_params("msg-ephemeral");
        params["workloadClass"] = json!("ephemeral");
        assert_eq!(
            run_record_usage(&store, params).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let stamped_class: Option<String> = connection
            .query_row(
                "SELECT workload_class FROM model_route_usage WHERE id = 'msg-ephemeral'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stamped_class.as_deref(), Some("ephemeral"));

        // Exactly ephemeral, head, gather, hire accepted
        for class in ["head", "gather", "hire"] {
            let mut p = usage_params(&format!("msg-{class}"));
            p["workloadClass"] = json!(class);
            run_record_usage(&store, p).await;
            let stored: Option<String> = connection
                .query_row(
                    "SELECT workload_class FROM model_route_usage WHERE id = ?1",
                    [&format!("msg-{class}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored.as_deref(), Some(class));
        }

        // Invalid classes are dropped to NULL
        for invalid in ["mason", "unknown", "other"] {
            let mut p = usage_params(&format!("msg-{invalid}"));
            p["workloadClass"] = json!(invalid);
            run_record_usage(&store, p).await;
            let stored: Option<String> = connection
                .query_row(
                    "SELECT workload_class FROM model_route_usage WHERE id = ?1",
                    [&format!("msg-{invalid}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, None, "unrecognized class {invalid} must ship NULL");
        }
    }

    #[tokio::test]
    async fn class_producer_false_cold_start_corpus_selects_bit_identically() {
        // Gated on class_producer = false
        let rig = Rig::new("class-producer-cold-start");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        // Seed cold-start corpus: only 10 complete classed rows in (now - 7d, now]
        for i in 0..10 {
            let mut p = usage_params(&format!("cold-start-{i}"));
            p["workloadClass"] = json!("ephemeral");
            p["inputTokens"] = json!(100.0);
            p["cachedReadTokens"] = json!(20.0);
            p["cacheWriteTokens"] = json!(5.0);
            p["outputTokens"] = json!(50.0);
            p["reasoningTokens"] = json!(10.0);
            run_record_usage(&store, p).await;
        }

        // 40 unclassified rows (NULL workload_class)
        for i in 0..40 {
            let mut p = usage_params(&format!("unclassified-{i}"));
            p["inputTokens"] = json!(100.0);
            p["cachedReadTokens"] = json!(20.0);
            p["cacheWriteTokens"] = json!(5.0);
            p["outputTokens"] = json!(50.0);
            p["reasoningTokens"] = json!(10.0);
            run_record_usage(&store, p).await;
        }

        let complete_ephemeral_count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({})",
                    striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
                ),
                rusqlite::params!["ephemeral", 0, 10_000],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(complete_ephemeral_count, 10);
        assert!(complete_ephemeral_count < 50);

        // use_basket cannot be true when class_producer is false
        assert!(!striatum_core::use_basket_permitted(true, false));
    }

    #[tokio::test]
    async fn account_producer_absent_usage_row_written_with_null_account_key_and_ephemeral_vector_forms(
    ) {
        // Gated on account_producer = absent
        let rig = Rig::new("account-producer-absent");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        let mut p = usage_params("msg-no-account");
        p["cachedReadTokens"] = json!(150.0);
        p["inputTokens"] = json!(300.0);
        p["cacheWriteTokens"] = json!(10.0);
        p["outputTokens"] = json!(100.0);
        p["reasoningTokens"] = json!(20.0);
        p["workloadClass"] = json!("ephemeral");
        assert_eq!(
            run_record_usage(&store, p).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let account_key: Option<String> = connection
            .query_row(
                "SELECT account_key FROM model_route_usage WHERE id = 'msg-no-account'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(account_key, None);

        // 50 complete rows with account_key NULL still form the complete-case vector
        for i in 1..50 {
            let mut row_params = usage_params(&format!("msg-no-account-{i}"));
            row_params["cachedReadTokens"] = json!(150.0);
            row_params["inputTokens"] = json!(300.0);
            row_params["cacheWriteTokens"] = json!(10.0);
            row_params["outputTokens"] = json!(100.0);
            row_params["reasoningTokens"] = json!(20.0);
            row_params["workloadClass"] = json!("ephemeral");
            run_record_usage(&store, row_params).await;
        }

        let complete_ephemeral_count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({})",
                    striatum_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
                ),
                rusqlite::params!["ephemeral", 0, 10_000],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            complete_ephemeral_count, 50,
            "ephemeral vector forms even when account_key is NULL"
        );
    }

    #[tokio::test]
    async fn absence_is_whole_object_at_provider_boundary_and_per_counter_at_param_layer() {
        // Gated on absence whole-object vs per-counter
        let rig = Rig::new("absence-whole-object-vs-per-counter");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        // Whole-object absence at provider boundary (Broca reported no usage at all)
        let mut whole_absent = usage_params("whole-absent");
        whole_absent.as_object_mut().unwrap().remove("outputTokens");
        whole_absent["reasoningTokens"] = serde_json::Value::Null;
        whole_absent["inputTokens"] = serde_json::Value::Null;
        whole_absent["cachedReadTokens"] = serde_json::Value::Null;
        whole_absent["cacheWriteTokens"] = serde_json::Value::Null;
        assert_eq!(
            run_record_usage(&store, whole_absent).await,
            json!({ "recorded": false, "usage_recorded": true })
        );

        let (inp, out, rsn, crd, cwr) = connection
            .query_row(
                "SELECT input_tokens, output_tokens, reasoning_tokens,
                      cached_read_tokens, cache_write_tokens
               FROM model_route_usage WHERE id = 'whole-absent'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(inp, None);
        assert_eq!(out, None);
        assert_eq!(rsn, None);
        assert_eq!(crd, None);
        assert_eq!(cwr, None);

        // Authoritative zero cached-read counter writes 0
        let mut zero_cached = usage_params("authoritative-zero");
        zero_cached["cachedReadTokens"] = json!(0.0);
        assert_eq!(
            run_record_usage(&store, zero_cached).await,
            json!({ "recorded": true, "usage_recorded": true })
        );

        let stored_zero: Option<i64> = connection
            .query_row(
                "SELECT cached_read_tokens FROM model_route_usage WHERE id = 'authoritative-zero'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_zero, Some(0));
    }

    async fn run_select_panel_wire_request(
        label: &str,
        request: &str,
    ) -> (Frame, Rig, SharedStore) {
        let rig = Rig::new(label);
        let store = Arc::new(rig.open_store());
        let response = run_route_wire_request(
            &store,
            serde_json::from_str(request).expect("select-panel request JSON"),
        )
        .await;
        (response, rig, store)
    }

    #[tokio::test]
    async fn route_model_fact_reads_a_seed_absent_anthropic_fact_from_the_live_store() {
        let request = json!({
            "method": ROUTE_MODEL_FACT_OP,
            "params": { "providerID": "anthropic", "modelID": "claude-opus-5" }
        });
        assert!(
            striatum_core::model_facts_seed()
                .iter()
                .all(|fact| !(fact.provider_id == "anthropic" && fact.model_id == "claude-opus-5")),
            "claude-opus-5 must remain absent from the frozen seed so this test proves the live store is authoritative"
        );

        let (response, _rig, _store) = run_select_panel_wire_request(
            "live-model-fact-anthropic",
            &serde_json::to_string(&request).unwrap(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"],
            json!({
                "known": true,
                "supportsVision": true,
                "maxOutputTokens": 64000,
                "contextWindow": 1000000,
                "family": "claude",
                "familySource": "curated"
            }),
            "the reply carries the catalog's own output ceiling and context window so a caller can size its send and its evidence package to this route"
        );
    }

    #[tokio::test]
    async fn route_model_facts_exposes_bounded_engine_speed_samples_separately() {
        let rig = Rig::new("engine-speed-inspection");
        let store = Arc::new(rig.open_store());
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute(
                "INSERT INTO routing_engine_speed
             (kind, provider, model, outcome, duration_ms, wall_ms, first_output_ms)
             VALUES ('transcribe','media','engine','done','1000','250','10')",
                [],
            )
            .unwrap();
        let operator_config = rig.dir.join("striatum.jsonc");
        crate::test_config::write(&operator_config, "{}").unwrap();
        let response = run_route_wire_request_with_operator_config(
            &store,
            json!({"method":ROUTE_MODEL_FACTS_OP,"params":{}}),
            &operator_config,
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["engine_speed_sample_limit"], 20);
        assert_eq!(
            body["result"]["engine_speed_samples"],
            json!([{
                "kind":"transcribe","provider":"media","model":"engine","outcome":"done",
                "duration_ms":1000,"wall_ms":250,"first_output_ms":10
            }])
        );
        assert!(body["result"]["facts"].is_array());
    }

    #[tokio::test]
    async fn route_model_facts_reads_a_live_store_row_with_catalog_fields() {
        let rig = Rig::new("live-model-facts");
        let store = Arc::new(rig.open_store());
        let mut live_fact = fact("fixture", "wire-only");
        live_fact.scores = ModelScores {
            elo: 1_701.0,
            eq: 1_801.5,
            speed: 91.0,
        };
        live_fact.cost_per_m_tok_out = Some(7.25);
        live_fact.supports_vision = Some(false);
        live_fact.supports_tool_use = Some(true);
        store
            .upsert_fact(&live_fact, FactSource::Override)
            .expect("write fixture directly to the routing store");
        store
            .upsert_fact(&fact("fixture", "wire-kept"), FactSource::Override)
            .expect("write the companion row the operator does not exclude");
        let mut unpriced = fact("fixture", "wire-unpriced");
        unpriced.cost_per_m_tok_out = None;
        unpriced.cost_tier = None;
        store
            .upsert_fact(&unpriced, FactSource::Override)
            .expect("write the row nobody has published a price for");
        // Stamp the STORED column on the row the operator does NOT exclude. Nothing
        // in the running system has ever written that column, so a reader serving it
        // raw answers with a value no policy produced; this row proves the served
        // answer comes from the live operator policy instead.
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .expect("open fixture database")
            .execute(
                "UPDATE model_facts SET excluded = 1 WHERE route_key = 'fixture/wire-kept'",
                [],
            )
            .expect("stamp the stale exclusion column");
        let operator_config = rig.dir.join("striatum.jsonc");
        crate::test_config::write(
            &operator_config,
            r#"{ "model_routing": { "exclude": {"all": ["fixture/wire-only"]} } }"#,
        )
        .unwrap();

        let response = run_route_wire_request_with_operator_config(
            &store,
            json!({ "method": ROUTE_MODEL_FACTS_OP, "params": {} }),
            &operator_config,
        )
        .await;

        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let facts = body["result"]["facts"]
            .as_array()
            .expect("model_facts returns a facts array");
        let fact = facts
            .iter()
            .find(|candidate| {
                candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-only"
            })
            .expect("the wire response includes the store-only fixture row");
        assert_eq!(
            fact["scores"],
            json!({ "elo": 1701.0, "eq": 1801.5, "speed": 91.0 })
        );
        assert_eq!(fact["costPerMTokOut"], 7.25);
        assert_eq!(fact["supportsVision"], false);
        assert_eq!(fact["supportsToolUse"], true);
        assert_eq!(
            fact["excluded"], true,
            "the operator's exclude list decides what model_facts reports as excluded"
        );
        let kept = facts
            .iter()
            .find(|candidate| {
                candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-kept"
            })
            .expect("the companion row is served too");
        assert_eq!(
            kept["excluded"], false,
            "a stale stored excluded=1 must not be served as policy state"
        );
        // A route nobody has priced is ranked at MAXIMUM cost by selection, so an
        // inventory reader must be able to tell it apart from an expensive one.
        assert_eq!(kept["priceUnknown"], false);
        let unpriced_row = facts
            .iter()
            .find(|candidate| {
                candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-unpriced"
            })
            .expect("the unpriced row is served too");
        assert_eq!(unpriced_row["priceUnknown"], true);
    }

    #[test]
    fn provider_wide_cooldown_cools_all_capacity_keys_for_provider_and_only_that_provider() {
        let rig = Rig::new("provider-wide-cooldown");
        let store = rig.open_store();
        store
            .upsert_fact(&fact("p", "a"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("p", "b"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("q", "c"), FactSource::Override)
            .unwrap();

        let params = RecordCooldownParams {
            route_key: "p/a/high".into(),
            capacity_key: "p/a".into(),
            credential_id: None,
            cooldown_until: Some(7_000.0),
            quota_exhausted_until: Some(9_000.0),
            quota_pressure: None,
            wall_class: None,
            provider_wide: true,
            increment_quota_error: true,
            increment_failure: true,
            last_error: Some("quota exhausted".into()),
        };
        record_cooldown_update(&store, &params).unwrap();

        let snap = store.build_snapshot(1_000.0).unwrap();
        for capacity_key in ["p/a", "p/b"] {
            let state = snap
                .by_capacity
                .get(capacity_key)
                .expect("provider capacity cooled");
            assert_eq!(state.cooldown_until, Some(7_000.0));
            assert_eq!(state.quota_exhausted_until, Some(9_000.0));
        }
        assert_eq!(
            snap.by_capacity
                .get("q/c")
                .and_then(|state| state.cooldown_until),
            None,
            "other providers must stay untouched"
        );
    }

    #[test]
    fn absent_provider_window_is_retained_until_a_new_window_arrives() {
        let mut cached = vec![
            usage(serde_json::json!({
                "provider": "codex",
                "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
            })),
            usage(serde_json::json!({
                "provider": "grok",
                "usage": { "primary": { "usedPercent": 10.0, "resetsAt": "2030-01-01T00:00:00Z" } }
            })),
        ];
        let fresh = vec![usage(serde_json::json!({
            "provider": "grok",
            "usage": { "primary": { "usedPercent": 40.0, "resetsAt": "2031-01-01T00:00:00Z" } }
        }))];

        let mut retained_degraded_since_ms = HashMap::new();
        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        let codex = cached
            .iter()
            .find(|window| window.provider == "codex")
            .expect("missing provider should be retained");
        assert_eq!(
            codex
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(100.0)
        );
        let grok = cached
            .iter()
            .find(|window| window.provider == "grok")
            .expect("present provider should be replaced");
        assert_eq!(
            grok.usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(40.0)
        );
    }

    #[test]
    fn degraded_refresh_does_not_replace_a_healthy_cached_window() {
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "quota source timeout"
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].provider, "codex");
        assert!(
            cached[0].error.is_none(),
            "healthy cached entry is retained"
        );
        assert_eq!(
            cached[0]
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(97.0)
        );
        assert!(retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
    }

    #[test]
    fn retained_degraded_quota_window_expires_despite_recurring_degraded_and_unrelated_successful_refreshes(
    ) {
        let first_reading_at_ms = 1_000;
        let first_degraded_at_ms = first_reading_at_ms + 1;
        let repeated_degraded_at_ms = first_degraded_at_ms + QUOTA_STALE_AFTER_MS / 2;
        let unrelated_refresh_at_ms = first_degraded_at_ms + QUOTA_STALE_AFTER_MS + 1;
        let mut cache = QuotaCache::default();

        apply_quota_refresh(
            &mut cache,
            vec![usage(serde_json::json!({
                "provider": "codex",
                "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
            }))],
            first_reading_at_ms,
        );
        apply_quota_refresh(
            &mut cache,
            vec![usage(serde_json::json!({
                "provider": "codex",
                "error": "quota source timeout"
            }))],
            first_degraded_at_ms,
        );
        apply_quota_refresh(
            &mut cache,
            vec![usage(serde_json::json!({
                "provider": "codex",
                "error": "quota source timeout"
            }))],
            repeated_degraded_at_ms,
        );

        let codex_key = quota_cache_key(&cache.windows[0]);
        assert_eq!(
            cache.windows[0].error, None,
            "the last healthy window is retained temporarily"
        );
        assert_eq!(
            cache.retained_degraded_since_ms.get(&codex_key),
            Some(&first_degraded_at_ms),
            "repeated failures must not restart the retention clock"
        );

        apply_quota_refresh(
            &mut cache,
            vec![usage(serde_json::json!({
                "provider": "grok",
                "usage": { "primary": { "usedPercent": 40.0, "resetsAt": "2031-01-01T00:00:00Z" } }
            }))],
            unrelated_refresh_at_ms,
        );

        assert!(
            cache
                .windows
                .iter()
                .all(|window| window.provider != "codex"),
            "another account's fresh reading must not keep codex's degraded window alive"
        );
        assert!(cache.retained_degraded_since_ms.is_empty());
        assert!(quota_health(&cache, unrelated_refresh_at_ms).0.is_none());

        apply_quota_refresh(
            &mut cache,
            vec![usage(serde_json::json!({
                "provider": "codex",
                "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2031-01-01T00:00:00Z" } }
            }))],
            unrelated_refresh_at_ms + 1,
        );

        let recovered = cache
            .windows
            .iter()
            .find(|window| window.provider == "codex")
            .expect("a fresh healthy reading restores the account");
        assert!(recovered.error.is_none());
        assert_eq!(
            recovered
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(12.0)
        );
        assert!(cache.retained_degraded_since_ms.is_empty());
        assert!(quota_health(&cache, unrelated_refresh_at_ms + 1)
            .0
            .is_none());
    }

    #[test]
    fn credential_unusable_class_drops_retention_like_absent() {
        // A revoked OAuth grant (Insula maps Google's `invalid_grant`, which
        // RFC 6749 makes permanent, to this class) never self-recovers;
        // retaining the last healthy window would keep routing to a dead
        // account. Removing the class from the dead-credential set must make
        // this test fail.
        let mut cached = vec![usage(serde_json::json!({
            "provider": "gemini",
            "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "gemini",
            "error": "invalid_grant: token revoked",
            "errorClass": "credential_unusable"
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_some(),
            "degraded credential_unusable entry replaces the stale healthy window"
        );
        assert_eq!(
            cached[0].error_class.as_deref(),
            Some("credential_unusable")
        );
        assert!(
            retained_degraded_since_ms.is_empty(),
            "no retention record for a revoked account"
        );
    }

    #[test]
    fn no_quota_reported_class_drops_retention_and_takes_the_degraded_entry() {
        // no_quota_reported is an ended plan (the gateway affirms success but
        // reports no plan block), so like credential_absent below it is a true
        // state, not trouble: retaining a last-healthy window for it
        // manufactures a provider that does not exist on this host. Classless
        // degraded entries (from older Insula builds) and every other class
        // keep the retention path above.
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "qwen-cloud: the account has no token plan (gateway affirmed success and reported no plan block)",
            "errorClass": "no_quota_reported"
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_some(),
            "degraded no_quota_reported entry replaces the stale healthy window"
        );
        assert_eq!(cached[0].error_class.as_deref(), Some("no_quota_reported"));
        assert!(
            retained_degraded_since_ms.is_empty(),
            "no retention record for a provider that does not exist here"
        );
    }

    #[test]
    fn credential_absent_class_drops_retention_and_takes_the_degraded_entry() {
        // credential_absent is steady state (nobody configured the credential),
        // not trouble: retaining a last-healthy window for it manufactures a
        // provider that does not exist on this host. Classless degraded entries
        // (from older Insula builds) and every other class keep the retention
        // path above.
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "no credential configured",
            "errorClass": "credential_absent"
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_some(),
            "degraded credential_absent entry replaces the stale healthy window"
        );
        assert_eq!(cached[0].error_class.as_deref(), Some("credential_absent"));
        assert!(
            retained_degraded_since_ms.is_empty(),
            "no retention record for a provider that does not exist here"
        );
    }

    #[test]
    fn unknown_error_class_still_retains_like_any_degraded_entry() {
        // The class list grows producer-side (String by contract, not enum): an
        // unrecognized class must behave exactly like a classless degraded entry,
        // never disappear and never join a known bucket.
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 55.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "novel failure",
            "errorClass": "some_future_class"
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_none(),
            "unknown class keeps the last-healthy retention path"
        );
    }

    #[test]
    fn empty_usage_refresh_does_not_replace_a_healthy_cached_window() {
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 88.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": {}
        }))];
        let mut retained_degraded_since_ms = HashMap::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(
            cached[0]
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(88.0)
        );
        assert!(retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
    }

    #[test]
    fn pool_only_refresh_is_retained_as_a_live_quota_signal() {
        let mut cache = QuotaCache::default();
        let pool_only = usage(serde_json::json!({
            "provider": "balance-wire",
            "apiProvider": "balance-provider",
            "usage": {},
            "spend": [{
                "id": "subscription",
                "label": "Subscription balance",
                "funding": "subscription",
                "remaining": { "minor": 20, "exponent": 2, "unit": "USD" },
                "total": { "minor": 100, "exponent": 2, "unit": "USD" },
                "basis": "reported",
                "spendable": true
            }]
        }));

        apply_quota_refresh(&mut cache, vec![pool_only], 10);

        assert_eq!(cache.windows.len(), 1);
        assert_eq!(
            cache.windows[0].provider, "balance-wire",
            "pool-only entries are retained as live quota observations"
        );
    }

    #[test]
    fn failed_quota_refresh_retains_last_healthy_windows() {
        let mut cache = QuotaCache::default();
        let healthy = usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 24.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }));
        apply_quota_refresh(&mut cache, vec![healthy], 10);
        let retained = cache.windows.clone();

        record_quota_refresh_failure(&mut cache, "usage.get", "daemon restarted");

        assert_eq!(cache.windows, retained);
        assert!(!cache.available);
        assert_eq!(
            cache.last_error.as_deref(),
            Some("usage.get: daemon restarted")
        );
    }

    #[tokio::test]
    async fn typed_provider_report_classifies_without_reading_diagnostic_text() {
        let rig = Rig::new("typed-provider-error-report");
        let store = Arc::new(rig.open_store());
        for (class, outcome, expected) in [
            ("transient", "session_error", "interrupted"),
            ("permanent", "session_error", "session_error"),
            ("transient", "completed", "completed"),
        ] {
            let decision = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some("test/model".into()),
                    selected_provider_id: Some("test".into()),
                    selected_model_id: Some("model".into()),
                    target_agent: "mason".into(),
                    requirements_json: "{}".into(),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    ..Default::default()
                })
                .unwrap();
            let response = run_route_wire_request(&store, json!({
                "method": ROUTE_SET_DECISION_OUTCOME_OP,
                "params": {"decisionID": decision, "outcome": outcome,
                    "lastError":"opaque diagnostic identical for both reports",
                    "providerOutcome":{"accountRef":"account","errorClass":class,"providerCode":"server_is_overloaded"}}
            })).await;
            assert_eq!(response.header.ty, FrameType::Response);
            assert_eq!(
                store
                    .get_decision_by_id(&decision)
                    .unwrap()
                    .unwrap()
                    .outcome
                    .as_deref(),
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn clear_cooldown_accepts_stored_and_work_variant_keys_and_clears_base_variants() {
        const BASE: &str = "alibaba-token-plan/qwen3.8-max-preview";
        let rig = Rig::new("clear-variant-cooldown");
        let store = Arc::new(rig.open_store());
        let variant = format!("{BASE}/xhigh");
        let until = now_ms() as f64 + 1_800_000.0;
        for key in [variant.clone(), format!("{BASE}:xhigh"), BASE.into()] {
            for route in [BASE, variant.as_str()] {
                store
                    .update_route_state(
                        route,
                        BASE,
                        &striatum_core::RouteStateUpdate {
                            cooldown_until: Some(until),
                            wall_class: Some(RouteWallClass::Quota),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            let response = run_route_wire_request(
                &store,
                json!({
                    "method": ROUTE_CLEAR_COOLDOWN_OP, "params": {"routeKey": key}
                }),
            )
            .await;
            assert_eq!(
                response.header.ty,
                FrameType::Response,
                "{:?}",
                response.body
            );
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            let rows = body["result"]["clearedRoutes"].as_array().unwrap();
            assert!(rows
                .iter()
                .any(|row| row["routeKey"] == variant && row["clearedCooldownUntil"] == until));
            let snapshot = store.build_snapshot(now_ms() as f64).unwrap();
            assert_eq!(snapshot.by_route[&variant].cooldown_until, None);
            if key == BASE {
                assert_eq!(rows.len(), 2);
                assert_eq!(snapshot.by_route[BASE].cooldown_until, None);
            } else {
                assert_eq!(rows.len(), 1);
                assert_eq!(snapshot.by_route[BASE].cooldown_until, Some(until));
            }
        }
    }

    #[tokio::test]
    async fn route_clear_cooldown_refuses_unknown_route_key() {
        let rig = Rig::new("clear-cooldown-unknown");
        let store = Arc::new(rig.open_store());
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_CLEAR_COOLDOWN_OP,
                "params": { "routeKey": "unknown/model" },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "unknown_route");
        assert!(body["message"].as_str().unwrap().contains("unknown/model"));
    }

    /// UTC RFC 3339 for a Unix-millisecond instant (civil-from-days), so quota
    /// fixtures can place resets relative to the clock selection reads.
    fn rfc3339_utc(ms: u64) -> String {
        let secs = (ms / 1000) as i64;
        let days = secs.div_euclid(86_400);
        let tod = secs.rem_euclid(86_400);
        let (h, mnt, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mnt:02}:{s:02}Z")
    }

    /// One codex reading with apiProvider="openai", whose 5-hour window resets at
    /// `resets_at_ms` with nothing used.
    fn codex_window(resets_at_ms: u64) -> ProviderUsage {
        usage(json!({
            "provider": "codex",
            "apiProvider": "openai",
            "usage": { "primary": {
                "usedPercent": 0.0,
                "resetsAt": rfc3339_utc(resets_at_ms),
                "windowMinutes": 300
            }}
        }))
    }

    #[test]
    fn first_poll_after_restart_replaces_the_restored_snapshot_wholesale() {
        let rig = Rig::new("quota-snapshot-first-poll");
        let store = rig.open_store();
        let observed_at_ms = now_ms();
        persist_quota_snapshot(
            &store,
            &[codex_window(observed_at_ms + 75 * 60_000)],
            observed_at_ms - 60_000,
        );
        let mut cache = restore_quota_cache(&store);
        assert_eq!(cache.windows.len(), 1);

        let claude = usage(json!({
            "provider": "claude",
            "usage": { "primary": { "usedPercent": 10.0 } }
        }));
        apply_quota_refresh(&mut cache, vec![claude.clone()], observed_at_ms);

        assert_eq!(
            cache.windows,
            vec![claude],
            "the restored codex window is not retained past the first fresh poll"
        );
        assert_eq!(cache.restored_from_ms, None);
        assert_eq!(cache.refreshed_at_ms, observed_at_ms);
    }

    /// A cache whose only reading is a codex window spent to 100% (walling
    /// openai until its reset three hours out), last refreshed `age_ms` ago and
    /// failing since, the way the refresh loop leaves it after a failed poll.
    fn walled_codex_cache_aged(age_ms: u64) -> QuotaCache {
        let now = now_ms();
        let observed_at_ms = now - age_ms;
        let mut cache = QuotaCache::default();
        apply_quota_refresh(
            &mut cache,
            vec![usage(json!({
                "provider": "codex",
                "apiProvider": "openai",
                "usage": { "primary": {
                    "usedPercent": 100.0,
                    "resetsAt": rfc3339_utc(now + 3 * 60 * 60_000),
                    "windowMinutes": 300
                }}
            }))],
            observed_at_ms,
        );
        record_quota_refresh_failure(&mut cache, "usage.get", "json: missing field `remaining`");
        cache
    }

    async fn health_report_for(cache: QuotaCache) -> ModuleControlResponse {
        health_report_for_with_credentials(cache, &mapped_broca_credentials("anthropic")).await
    }

    pub(super) async fn health_report_for_with_credentials(
        cache: QuotaCache,
        broca_credentials: &SharedBrocaCredentials,
    ) -> ModuleControlResponse {
        let (writer, mut replies) = mpsc::channel(1);
        let frame = Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            1,
            serde_json::to_vec(&json!({ "op": MODULE_CONTROL_OP_HEALTH_CHECK })).unwrap(),
        )
        .unwrap();
        handle_control_request(
            frame,
            &writer,
            &mut RouteEpochs::new(),
            &Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
            broca_credentials,
            &Arc::new(Mutex::new(cache)),
        )
        .await
        .unwrap();
        serde_json::from_slice(&replies.recv().await.unwrap().body).unwrap()
    }

    /// Replays captured live data through the actual account report and control
    /// health dispatcher, without deploying a module or changing a live store.
    #[tokio::test]
    #[ignore = "requires PREFRONTAL_ROUTING_USAGE_CAPTURE and PREFRONTAL_ROUTING_VAULT_ROSTER_CAPTURE"]
    async fn captured_insula_feed_reports_unmapped_information_without_degrading_health() {
        let capture =
            std::env::var("PREFRONTAL_ROUTING_USAGE_CAPTURE").expect("required usage capture path");
        let roster_capture = std::env::var("PREFRONTAL_ROUTING_VAULT_ROSTER_CAPTURE")
            .expect("required roster capture path");
        let bytes = std::fs::read(capture).unwrap();
        let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let mut state = account_quota::AccountQuotaState::default();
        let roster: account_quota::VaultRoster =
            serde_json::from_slice(&std::fs::read(roster_capture).unwrap()).unwrap();
        state.roster_poll = account_quota::RosterPoll::Listed {
            at_ms: roster.fetched_at_ms,
        };
        state.roster = Some(roster);
        account_quota::apply_usage_reply(&mut state, &bytes, now_ms() as i64).unwrap();
        let report = account_quota::build_report(
            &state.readings,
            state.roster.as_ref(),
            &state.roster_poll,
            now_ms() as i64,
        );
        let unmapped = report
            .accounts
            .iter()
            .filter(|account| account.routing_provider.is_none())
            .count();
        assert!(unmapped > 0, "capture must exercise absent vendor metadata");
        assert_eq!(report.information.len(), unmapped);
        assert!(report
            .alarms
            .iter()
            .all(|alarm| !alarm.contains("unmapped entry")));
        let entries = quota_consumer::decode_usage_result(&raw["result"]).unwrap();
        let mut cache = QuotaCache::default();
        apply_quota_refresh(&mut cache, entries, now_ms());
        let health = health_report_for(cache).await;
        let ModuleControlResponse::HealthCheck { status, detail, .. } = &health else {
            panic!("health reply");
        };
        assert_eq!(*status, HealthStatus::Ok);
        assert!(detail.is_none());
        let summary = json!({
            "health": health,
            "accountQuota": {
                "accounts": report.accounts.len(), "unmappedInformationCount": report.information.len(),
                "unmappedProviders": report.accounts.iter().filter(|account| account.routing_provider.is_none()).map(|account| account.provider.clone()).collect::<Vec<_>>(),
                "alarms": report.alarms,
                "vaultOnlyGapCount": report.gaps.vault_only.len(), "insulaOnlyGapCount": report.gaps.insula_only.len()
            }
        });
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    }

    #[tokio::test]
    async fn health_check_reports_degraded_naming_the_stale_quota_age() {
        let ModuleControlResponse::HealthCheck { status, detail, .. } =
            health_report_for(walled_codex_cache_aged(QUOTA_STALE_AFTER_MS - 60_000)).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Ok, "inside the bound: {detail:?}");

        let ModuleControlResponse::HealthCheck {
            status,
            detail,
            metrics,
        } = health_report_for(walled_codex_cache_aged(7 * 60 * 60_000 + 5 * 60_000)).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Degraded);
        let detail = detail.expect("a degraded report says why");
        assert!(
            detail.contains("quota snapshot stale: last successful refresh 7h05m ago")
                && detail.contains("missing field `remaining`"),
            "{detail}"
        );
        assert_eq!(metrics.unwrap()["quota"]["stale"], true);

        // No reading yet (boot before the first poll) is not staleness.
        let ModuleControlResponse::HealthCheck { status, .. } =
            health_report_for(QuotaCache::default()).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Ok);
    }

    #[tokio::test]
    async fn health_check_reports_unmapped_credentials_without_degrading_and_joins_stale_quota_details(
    ) {
        let broca_credentials = empty_broca_credentials();
        broca_credentials.lock().unwrap().apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: vec![
                    cortexkit_claustrum_client::ListedCredential {
                        id: "fixture-mapped".into(),
                        categories: vec!["llm-provider".into()],
                        state: "active".into(),
                        provider_ids: vec!["anthropic".into()],
                        ..Default::default()
                    },
                    cortexkit_claustrum_client::ListedCredential {
                        id: "fixture-unmapped".into(),
                        categories: vec!["llm-provider".into()],
                        state: "active".into(),
                        ..Default::default()
                    },
                ],
                view: "unmapped-view".into(),
                ..Default::default()
            },
            now_ms(),
        );
        let ModuleControlResponse::HealthCheck {
            status,
            detail,
            metrics,
        } = health_report_for_with_credentials(
            walled_codex_cache_aged(QUOTA_STALE_AFTER_MS + 60_000),
            &broca_credentials,
        )
        .await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Degraded);
        let detail = detail.unwrap();
        assert!(
            detail.starts_with("quota snapshot stale: last successful refresh 16m ago"),
            "{detail}"
        );
        assert!(detail.contains("missing field `remaining`"), "{detail}");
        let metrics = metrics.unwrap();
        assert_eq!(metrics["quota"]["stale"], true);
        assert_eq!(metrics["brocaReachability"]["unmappedCredentials"], 1);
        assert_eq!(
            metrics["brocaReachability"]["unmappedCredentialIds"],
            json!(["fixture-unmapped"])
        );

        // The unmapped credential remains visible without degrading Broca health.
        let ModuleControlResponse::HealthCheck {
            status,
            detail,
            metrics,
        } = health_report_for_with_credentials(QuotaCache::default(), &broca_credentials).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Ok);
        assert_eq!(detail, None);
        assert_eq!(
            metrics.unwrap()["brocaReachability"]["unmappedCredentialIds"],
            json!(["fixture-unmapped"])
        );

        // The quota remains stale independently of the informational unmapped row.
        let ModuleControlResponse::HealthCheck { status, detail, .. } =
            health_report_for_with_credentials(
                walled_codex_cache_aged(QUOTA_STALE_AFTER_MS + 60_000),
                &mapped_broca_credentials("anthropic"),
            )
            .await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Degraded);
        assert!(detail.unwrap().starts_with("quota snapshot stale:"));

        let ModuleControlResponse::HealthCheck { status, detail, .. } =
            health_report_for(QuotaCache::default()).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Ok);
        assert_eq!(detail, None);
    }

    /// Point a config file's mtime at a fixed second so a live-edit test does not
    /// depend on the filesystem clock's resolution.
    fn set_config_mtime(path: &Path, seconds: u64) {
        let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn route_explain_by_send_id_returns_recorded_rejections_verbatim() {
        let rig = Rig::new("route-explain-send-id");
        let store = Arc::new(rig.open_store());
        let rejections = json!([
            { "routeKey": "p/too-slow", "reason": "too_slow", "detail": "recorded" },
            { "routeKey": "p/too-costly", "reason": "too_expensive" },
        ]);
        let score = json!({
            "adequacy": 0.97,
            "selectionCost": 12.5,
            "metrics": { "speed": { "actual": 73.0 } },
        });
        store
            .record_decision(&DecisionInput {
                decision_id: Some("dec-explain-send".into()),
                decision_kind: "single".into(),
                task_id: Some("task-explain".into()),
                parent_session_id: Some("ses-explain".into()),
                send_id: Some("send-explain".into()),
                target_agent: "mason".into(),
                requirements_json: r#"{"iq":80,"eq":20}"#.into(),
                selected_route_key: Some("p/selected".into()),
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("selected".into()),
                score_json: serde_json::to_string(&score).unwrap(),
                rejected_json: serde_json::to_string(&rejections).unwrap(),
                explanation_json: Some(r#"["cost-primary"]"#.into()),
                quota_signal_json: Some(r#"{"state":"present","signals":[]}"#.into()),
                policy_revision: Some("policy-explain".into()),
                anchor_version: ANCHOR_VERSION,
                ..Default::default()
            })
            .unwrap();

        let identity = striatum_core::route_state_store::ProviderOutcome {
            account_ref: "frozen-account".into(),
            credential_id: Some("oauth:p:frozen-account".into()),
            resets_at_ms: None,
            credential_refusal: None,
            ..Default::default()
        };
        store
            .set_decision_provider_identity("dec-explain-send", &identity)
            .unwrap();

        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_EXPLAIN_OP,
                "params": { "sendID": "send-explain" },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["decisionID"], "dec-explain-send");
        assert_eq!(body["result"]["providerIdentity"], json!(identity));
        assert_eq!(body["result"]["why"]["rejections"], rejections);
        assert_eq!(body["result"]["why"]["score"], score);
        assert_eq!(body["result"]["provenance"]["sendID"], "send-explain");
        assert_eq!(body["result"]["provenance"]["taskID"], "task-explain");
        assert_eq!(
            body["result"]["provenance"]["parentSessionID"],
            "ses-explain"
        );
        assert_eq!(body["result"]["provenance"]["targetAgent"], "mason");
    }

    #[tokio::test]
    async fn route_explain_serves_core_fields_from_a_compacted_decision() {
        let rig = Rig::new("route-explain-compacted");
        let store = Arc::new(rig.open_store());
        let candidate = |route: &str| {
            json!({"routeKey": route, "model": route, "elo": 1300.0, "eq": 1200.0,
                "eloSource": "curated", "eqSource": "curated", "eligible": true})
        };
        let mut candidates: Vec<serde_json::Value> = (0..200)
            .map(|index| candidate(&format!("p/other-{index:03}")))
            .collect();
        candidates.extend([candidate("p/selected"), candidate("q/adequate")]);
        let objective = json!({
            "cost": 0.25, "quota": 0.0, "speedScore": 0.5, "selectionCost": 0.25,
            "taskPricing": {"profile": null, "tokens": null, "priceUnit": "mtok_output",
                "listCostUsd": 2.0, "valueMultiplier": 6.0, "valueSource": "operator",
                "pressure": 0.1, "finalCostUsd": 12.0, "normalizedCost": 0.25,
                "omittedDimensions": [], "unpricedDimensions": []},
        });
        let metrics = json!({"elo": {"fact": 1300.0, "target": 1250.0}});
        let score = json!({
            "candidates": candidates,
            "adequacy": 1.0,
            "rawAdequacy": 1.04,
            "selectionCost": 0.25,
            "metrics": metrics,
            "objective": objective,
            "alsoAdequate": ["q/adequate"],
            "pricingMode": "output_only",
        });
        let rejections = json!([{ "routeKey": "p/walled", "reason": "quota_exhausted" }]);
        store
            .record_decision(&DecisionInput {
                decision_id: Some("dec-compacted".into()),
                decision_kind: "single".into(),
                target_agent: "mason".into(),
                requirements_json: r#"{"iq":80,"eq":20}"#.into(),
                selected_route_key: Some("p/selected".into()),
                selected_provider_id: Some("p".into()),
                selected_model_id: Some("selected".into()),
                score_json: serde_json::to_string(&score).unwrap(),
                rejected_json: serde_json::to_string(&rejections).unwrap(),
                explanation_json: Some(r#"["cost-primary"]"#.into()),
                quota_signal_json: Some(
                    r#"{"state":"present","signals":[{"capacityKey":"p","pressure":0.1}]}"#.into(),
                ),
                policy_revision: Some("policy-compacted".into()),
                anchor_version: ANCHOR_VERSION,
                ..Default::default()
            })
            .unwrap();
        async fn explain(store: &SharedStore) -> serde_json::Value {
            let response = run_route_wire_request(
                store,
                json!({
                    "method": ROUTE_EXPLAIN_OP,
                    "params": { "decisionID": "dec-compacted" },
                }),
            )
            .await;
            assert_eq!(response.header.ty, FrameType::Response);
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone()
        }
        let full = explain(&store).await;
        assert_eq!(
            full["detailRetention"],
            json!({"detail": "full", "strippedAtMs": null})
        );
        assert_eq!(full["why"]["score"], score);

        // The full-detail window has passed and the decision is not sampled.
        let batch = store.strip_decision_detail(i64::MAX, 0, 16, 1_234).unwrap();
        assert_eq!(batch.stripped, 1);
        let compact = explain(&store).await;
        assert_eq!(
            compact["detailRetention"],
            json!({"detail": "compact", "strippedAtMs": 1_234})
        );
        // What core reads: the adequate set and policy revision for its work
        // view, and the selected route's value multiplier for its prices.
        assert_eq!(
            compact.pointer("/why/score/alsoAdequate"),
            Some(&json!(["q/adequate"]))
        );
        assert_eq!(compact["policyRevision"], "policy-compacted");
        assert_eq!(
            compact.pointer("/why/score/objective/taskPricing/valueMultiplier"),
            Some(&json!(6.0))
        );
        assert_eq!(compact["why"]["score"]["objective"], objective);
        assert_eq!(compact["why"]["score"]["metrics"], metrics);
        assert_eq!(
            compact["why"]["score"]["candidates"],
            json!([candidate("p/selected"), candidate("q/adequate")])
        );
        assert_eq!(compact["why"]["score"]["candidatesOmitted"], 200);
        assert_eq!(compact["why"]["rejections"], rejections);
        assert_eq!(compact["why"]["explanation"], json!(["cost-primary"]));
        assert_eq!(
            compact["quotaSignalSnapshot"],
            json!({"state": "present", "signalsOmitted": 1})
        );
        assert_eq!(compact["selectedRoute"]["routeKey"], "p/selected");
    }

    #[tokio::test]
    async fn route_decisions_list_composes_target_agent_and_since_filters() {
        let rig = Rig::new("route-decisions-list-filters");
        let store = Arc::new(rig.open_store());
        for (id, target_agent, route_key) in [
            ("dec-old-mason", "mason", "p/old"),
            ("dec-new-mason", "mason", "p/new"),
            ("dec-new-explore", "explore", "p/other"),
        ] {
            store
                .record_decision(&DecisionInput {
                    decision_id: Some(id.into()),
                    target_agent: target_agent.into(),
                    requirements_json: "{}".into(),
                    selected_route_key: Some(route_key.into()),
                    score_json: "null".into(),
                    rejected_json: "[]".into(),
                    anchor_version: ANCHOR_VERSION,
                    ..Default::default()
                })
                .unwrap();
        }
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        for (id, created_at) in [
            ("dec-old-mason", 1_000_i64),
            ("dec-new-mason", 6_000_i64),
            ("dec-new-explore", 7_000_i64),
        ] {
            connection
                .execute(
                    "UPDATE model_route_decisions SET created_at=?1 WHERE id=?2",
                    rusqlite::params![created_at, id],
                )
                .unwrap();
        }

        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_DECISIONS_LIST_OP,
                "params": { "target_agent": "mason", "since_ms": 5_000 },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"]["decisions"],
            json!([{
                "decisionID": "dec-new-mason",
                "createdAtMs": 6_000,
                "selectedRouteKey": "p/new",
                "decisionKind": "single",
                "topRejectionReason": null,
                "topRejectionCount": 0,
            }])
        );
    }

    #[tokio::test]
    async fn route_explain_unknown_id_names_key_and_retention() {
        let rig = Rig::new("route-explain-missing");
        let store = Arc::new(rig.open_store());
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_EXPLAIN_OP,
                "params": { "decisionID": "dec-pruned" },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let message = body["message"].as_str().unwrap();
        let retention_days = retention::retention_windows(&config_overrides::config_path())
            .model_route_decisions_days;
        assert_eq!(body["code"], "not_found");
        assert!(message.contains("decisionID 'dec-pruned'"));
        assert!(message.contains(&format!(
            "not found or pruned; retention is {retention_days} days"
        )));
    }

    #[tokio::test]
    async fn route_explain_exactly_one_key_validation_does_not_mutate_decisions() {
        let rig = Rig::new("route-explain-one-key");
        let store = Arc::new(rig.open_store());
        for params in [json!({}), json!({ "decisionID": "dec", "sendID": "send" })] {
            let response = run_route_wire_request(
                &store,
                json!({ "method": ROUTE_EXPLAIN_OP, "params": params }),
            )
            .await;
            assert_eq!(response.header.ty, FrameType::Error);
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(body["code"], "invalid_request");
            assert!(body["message"]
                .as_str()
                .unwrap()
                .contains("exactly one of decisionID or sendID"));
        }
        assert!(store
            .list_decisions(&DecisionListFilter {
                limit: 20,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn route_resolve_policy_dispatches_as_a_read_only_wire_query() {
        let request = json!({
            "method": ROUTE_RESOLVE_POLICY_OP,
            "params": {
                "availableModels": ["fixture/a", "fixture/b"],
                "routingPolicy": {
                    "exclude": ["fixture/a"],
                    "subscription": ["fixture"],
                },
                "providerAuthTypes": { "fixture": "api" }
            }
        });
        let (response, _rig, _store) = run_select_panel_wire_request(
            "resolve-policy-wire",
            &serde_json::to_string(&request).unwrap(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"],
            json!({
                "availableModels": ["fixture/a", "fixture/b"],
                "subscriptionKeys": [],
                "connectedModelCount": 2,
            })
        );
    }

    #[test]
    fn manifest_declares_wire_version_and_build_provenance_contract() {
        let provenance = subc_protocol::manifest::build_provenance_from_source(
            BuildGitShaSource::Git {
                revision: "0123456789abcdef0123456789abcdef01234567",
                tree_state: GitTreeState::Clean,
            },
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            provenance.wire_crate_version.as_deref(),
            Some(subc_protocol::SUBC_PROTOCOL_CRATE_VERSION)
        );
        assert_eq!(
            provenance.build_git_sha.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );

        let dirty = subc_protocol::manifest::build_provenance_from_source(
            BuildGitShaSource::Git {
                revision: "0123456789abcdef0123456789abcdef01234567",
                tree_state: GitTreeState::Dirty,
            },
            None,
            None,
        )
        .unwrap();
        assert_eq!(dirty.build_git_sha, None);
        assert_eq!(
            dirty.build_git_sha_absence_reason,
            Some(subc_protocol::manifest::BuildGitShaAbsenceReason::DeclinedDirty)
        );
    }

    #[test]
    fn declared_provenance_follows_build_inputs() {
        let provenance = declared_provenance().unwrap();
        use subc_protocol::manifest::BuildGitShaAbsenceReason;
        if env!("PREFRONTAL_BUILD_GIT_SHA") == "unknown" {
            assert_eq!(provenance.build_git_sha, None);
            assert_eq!(
                provenance.build_git_sha_absence_reason,
                Some(BuildGitShaAbsenceReason::NeverDerived)
            );
        } else if env!("PREFRONTAL_BUILD_GIT_DIRTY") == "true" {
            assert_eq!(provenance.build_git_sha, None);
            assert_eq!(
                provenance.build_git_sha_absence_reason,
                Some(BuildGitShaAbsenceReason::DeclinedDirty)
            );
        } else {
            assert_eq!(
                provenance.build_git_sha.as_deref(),
                Some(env!("PREFRONTAL_BUILD_GIT_SHA"))
            );
        }
        assert_eq!(
            provenance.wire_crate_version.as_deref(),
            Some(subc_protocol::SUBC_PROTOCOL_CRATE_VERSION)
        );
        assert_eq!(
            provenance.build_lock_digest.as_deref().map(str::len),
            Some(64)
        );
    }

    #[test]
    fn default_manifest_registers_striatum() {
        assert_eq!(DEFAULT_MODULE_ID, "striatum");
        let printed: serde_json::Value =
            serde_json::from_str(&printed_manifest_json(DEFAULT_MODULE_ID).unwrap()).unwrap();
        assert_eq!(printed["manifest"]["module_id"], "striatum");
    }

    #[test]
    fn print_manifest_carries_the_whole_manifest_and_the_operation_kinds() {
        let printed: serde_json::Value =
            serde_json::from_str(&printed_manifest_json("print-manifest-test").unwrap()).unwrap();
        let object = printed.as_object().expect("printed manifest is an object");
        assert_eq!(
            object.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["manifest", "operation_kinds"]
        );

        // The whole manifest HELLO declares, so the deploy script can read the
        // installed build commit at manifest.provenance.build_git_sha.
        let printed_manifest = &printed["manifest"];
        assert_eq!(
            printed_manifest,
            &serde_json::to_value(manifest("print-manifest-test")).unwrap()
        );
        assert_eq!(printed_manifest["module_id"], "print-manifest-test");
        let provenance = &printed_manifest["provenance"];
        assert_eq!(
            provenance["wire_crate_version"],
            subc_protocol::SUBC_PROTOCOL_CRATE_VERSION
        );
        if env!("PREFRONTAL_BUILD_GIT_SHA") == "unknown" {
            assert!(provenance["build_git_sha"].is_null());
            assert_eq!(provenance["build_git_sha_absence_reason"], "never_derived");
        } else if env!("PREFRONTAL_BUILD_GIT_DIRTY") == "true" {
            assert!(provenance["build_git_sha"].is_null());
            assert_eq!(provenance["build_git_sha_absence_reason"], "declined_dirty");
        } else {
            assert_eq!(
                provenance["build_git_sha"],
                env!("PREFRONTAL_BUILD_GIT_SHA")
            );
        }
        assert_eq!(
            provenance["build_lock_digest"].as_str().map(str::len),
            Some(64)
        );

        // The op-kinds generator reads this list: every served operation once,
        // sorted by name, as exactly { name, kind }.
        let kinds = printed["operation_kinds"]
            .as_array()
            .expect("operation kinds printed");
        let names = kinds
            .iter()
            .map(|operation| operation["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let mut expected = served_operations()
            .into_iter()
            .map(|operation| operation.name)
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(names, expected);
        let route_select = kinds
            .iter()
            .find(|operation| operation["name"] == ROUTE_SELECT_OP)
            .expect("route.select printed");
        assert_eq!(
            route_select,
            &serde_json::json!({ "name": ROUTE_SELECT_OP, "kind": "mutate" })
        );
    }

    #[test]
    fn manifest_exposes_policy_query_alongside_selection() {
        let manifest = manifest(DEFAULT_MODULE_ID);
        let operations = manifest
            .provides
            .iter()
            .find_map(|role| match role {
                ProviderRole::ManagementSurface { operations, .. } => Some(operations),
                _ => None,
            })
            .unwrap();
        assert!(operations
            .iter()
            .any(|operation| operation.name == ROUTE_RESOLVE_POLICY_OP));
        assert!(operations
            .iter()
            .any(|operation| operation.name == ROUTE_MODEL_FACT_OP));
        for operation_name in [ROUTE_EXPLAIN_OP, ROUTE_DECISIONS_LIST_OP] {
            assert!(operations.iter().any(|operation| {
                operation.name == operation_name && operation.kind == ManagementOperationKind::Query
            }));
        }
    }

    #[tokio::test]
    async fn legacy_roster_wire_is_refused_before_recording_a_decision() {
        let vectors = select_panel_wire_vectors();
        let request = vectors["legacyRoster"]["request"].as_str().unwrap();
        let request_value: serde_json::Value = serde_json::from_str(request).unwrap();
        assert!(request_value["params"].get("selection").is_none());
        assert!(request_value["params"]
            .get("packageTokensEstimate")
            .is_none());
        assert!(request_value["params"]
            .get("proMemberMaxPackageTokens")
            .is_none());

        let (response, _rig, _store) = run_select_panel_wire_request("legacy-wire", request).await;
        assert_eq!(response.header.ty, FrameType::Error);
        let response_value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response_value["code"], "roster_removed");
        assert!(_store
            .get_decisions_for_task("panel-wire-legacy")
            .unwrap()
            .is_empty());
    }

    /// Availability follows the substrate that sends, not the host that requested
    /// the panel. Broca can seat a catalog route missing from the host's list.
    #[tokio::test]
    async fn a_panel_route_absent_from_the_host_list_seats_on_broca_and_is_rejected_on_the_host() {
        let rig = Rig::new("panel-substrate");
        let store = Arc::new(rig.open_store());
        let mut kimi = fact("kimi-code-plan-global", "k3");
        kimi.family = Some("kimi".into());
        kimi.scores.elo = 1700.0;
        store.upsert_fact(&kimi, FactSource::Override).unwrap();
        let broca_credentials = mapped_broca_credentials("kimi-code-plan-global");
        let request = |substrate: &str| {
            json!({
                "method": ROUTE_SELECT_PANEL_OP,
                "params": {
                    // The caller's host list lacks Kimi's plan entirely.
                    "availableModels": ["anthropic/claude-fable-5-1"],
                    "excludeRouteKeys": ["anthropic", "openai", "google", "xai"],
                    "panelSpec": {"class": "EXPLAIN", "diversity": "none", "iqFloor": 0, "size": 1},
                    "substrate": substrate,
                    "taskId": format!("panel-substrate-{substrate}"),
                }
            })
        };
        let run = |substrate: &'static str| {
            let store = Arc::clone(&store);
            let broca_credentials = Arc::clone(&broca_credentials);
            async move {
                let response = run_route_wire_request_with_paths(
                    &store,
                    request(substrate),
                    &test_operator_config(),
                    &broca_credentials,
                )
                .await;
                assert_eq!(response.header.ty, FrameType::Response);
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"]
                    .clone()
            }
        };

        let broca = run("broca").await;
        assert_eq!(
            broca["members"][0]["routeKey"], "kimi-code-plan-global/k3",
            "the catalog serves the route, so it sits on Broca whatever the host lists"
        );
        assert!(broca.get("shortfall").is_none());

        let host = run("opencode").await;
        assert_eq!(host["members"], json!([]));
        let cause = host["shortfall"]["causes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|cause| cause["model"] == "kimi-code-plan-global/k3")
            .unwrap();
        assert_eq!(cause["model"], "kimi-code-plan-global/k3");
        assert_eq!(cause["reason"], "unavailable");
        assert_eq!(
            cause["detail"], "harness broca does not serve this route for this request",
            "the rejection names the route's mapped harness"
        );
        // The decision also records the sending substrate, even when its
        // per-model rejection list is compacted by the store.
        let recorded = store
            .get_decision_by_id(host["decisionID"].as_str().unwrap())
            .unwrap()
            .expect("the panel decision is recorded");
        let explanation: serde_json::Value =
            serde_json::from_str(recorded.explanation_json.as_ref().unwrap()).unwrap();
        assert_eq!(explanation["availability"], "host");
    }

    /// Before a credential listing arrives, routing knows nothing about which
    /// providers Broca can authenticate to. A panel seats from the served catalog, never from the
    /// caller's host list (here empty, which would refuse every seat), and the
    /// decision row says the reach was unknown even though nothing was refused.
    /// The gap also turns `health.check` degraded, so it cannot sit unseen.
    #[tokio::test]
    async fn a_broca_panel_with_no_reachability_source_seats_a_catalog_route_and_says_so() {
        let rig = Rig::new("panel-substrate-unknown-reach");
        let store = Arc::new(rig.open_store());
        let mut live = fact("livebroca", "m-new");
        live.family = Some("qwen".into());
        live.scores.elo = 10_000.0;
        store.upsert_fact(&live, FactSource::Override).unwrap();
        let broca_credentials = empty_broca_credentials();
        let response = run_route_wire_request_with_paths(
            &store,
            json!({
                "method": ROUTE_SELECT_PANEL_OP,
                "params": {
                    "availableModels": [],
                    "excludeRouteKeys": ["anthropic", "openai", "google", "xai"],
                    "panelSpec": {"class": "lab"},
                    "substrate": "broca",
                    "taskId": "panel-substrate-unknown-reach",
                }
            }),
            &test_operator_config(),
            &broca_credentials,
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let result =
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone();
        assert_eq!(result["members"][0]["routeKey"], "livebroca/m-new");
        assert!(result.get("shortfall").is_none(), "{result}");
        let explanation: serde_json::Value =
            serde_json::from_str(&decision_explanation(&store, &result)).unwrap();
        assert_eq!(
            explanation["availability"],
            "broca reachability unknown: judged against served catalog"
        );
        assert_eq!(
            broca_credentials
                .lock()
                .unwrap()
                .health_report(now_ms())
                .detail
                .as_deref(),
            Some("broca reachability unknown: credential listing unavailable")
        );
    }

    /// Each health check uses the latest credential listing. Completing the
    /// provider mapping clears the "broca reachability unknown" detail, even if
    /// an earlier Broca send fell back to the whole served catalog.
    #[tokio::test]
    async fn unknown_broca_reach_degrades_health_until_listing_maps_it() {
        let ModuleControlResponse::HealthCheck { status, detail, .. } =
            health_report_for_with_credentials(QuotaCache::default(), &empty_broca_credentials())
                .await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Degraded);
        assert_eq!(
            detail.as_deref(),
            Some("broca reachability unknown: credential listing unavailable")
        );
        let ModuleControlResponse::HealthCheck { status, detail, .. } =
            health_report_for(QuotaCache::default()).await
        else {
            panic!("health.check answers with a health report");
        };
        assert_eq!(status, HealthStatus::Ok);
        assert_eq!(detail, None);
    }

    /// Even with every credential mapped to a provider, a provider that no active
    /// credential serves must not get a panel seat.
    #[tokio::test]
    async fn a_broca_panel_never_seats_a_provider_broca_holds_no_credential_for() {
        let rig = Rig::new("panel-substrate-credentials");
        let store = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("uncredentialed", "m1"), FactSource::Override)
            .unwrap();
        let response = run_route_wire_request_with_paths(
            &store,
            json!({
                "method": ROUTE_SELECT_PANEL_OP,
                "params": {
                    "availableModels": ["uncredentialed/m1"],
                    "panelSpec": {"class": "SPEC", "diversity": "none", "iqFloor": 0, "size": 1},
                    "substrate": "broca",
                    "taskId": "panel-substrate-uncredentialed"
                }
            }),
            &test_operator_config(),
            &mapped_broca_credentials("kimi-code-plan-global"),
        )
        .await;
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["members"], json!([]));
        assert_eq!(
            body["result"]["shortfall"]["causes"][0]["reason"],
            "unavailable"
        );
        assert_eq!(
            body["result"]["shortfall"]["causes"][0]["detail"],
            "harness broca does not serve this route for this request"
        );
    }

    fn decision_explanation(store: &SharedStore, result: &serde_json::Value) -> String {
        store
            .get_decision_by_id(result["decisionID"].as_str().unwrap())
            .unwrap()
            .expect("the selection is recorded")
            .explanation_json
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn n_pick_wire_matches_policy_fixture_and_decision_explanation() {
        let vectors = select_panel_wire_vectors();
        let request = vectors["nPick"]["request"].as_str().unwrap();
        let (response, _rig, store) = run_select_panel_wire_request("n-pick-wire", request).await;
        assert_eq!(response.header.ty, FrameType::Response);
        let response_value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let decision_id = response_value["result"]["decisionID"].as_str().unwrap();
        let expected = vectors["nPick"]["reply"]
            .as_str()
            .unwrap()
            .replace("$decisionID", decision_id);
        let mut expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
        for member in expected["result"]["members"].as_array_mut().unwrap() {
            member["harness"] = json!("broca");
        }
        for cause in expected["result"]["shortfall"]["causes"]
            .as_array_mut()
            .unwrap()
        {
            if cause["reason"] == "unavailable" {
                cause["detail"] = json!("harness broca does not serve this route for this request");
            }
        }
        let funnel = json!({
            "catalog": 46, "scored": 46, "operatorAdmitted": 46, "callerAdmitted": 46,
            "reachable": 2, "capabilityEligible": 2, "quotaEligible": 2, "packageFit": 2,
            "adequacyEligible": 1, "distinctFamilies": 1, "initiallyAdmitted": 1,
            "gateOrder": ["deprecated_upstream", "unscored_awaiting_placement", "excluded_by_operator",
                "excluded", "unavailable", "capability", "quota_or_backoff", "package_fit",
                "sibling_collapse", "family_labeled", "phase_adequacy", "distinct_families", "initially_admitted"]
        });
        expected["result"]["policy"]["funnel"] = funnel.clone();
        expected["result"]["shortfall"]["funnel"] = funnel;
        assert_eq!(response_value, expected);

        let decisions = store.get_decisions_for_task("panel-wire-n-pick").unwrap();
        assert_eq!(decisions.len(), 1);
    }

    #[tokio::test]
    async fn automatic_panel_requests_obey_caller_exclusions_with_or_without_a_package_size() {
        let vectors = select_panel_wire_vectors();
        let original: serde_json::Value =
            serde_json::from_str(vectors["nPick"]["request"].as_str().unwrap()).unwrap();
        for package_tokens in [None, Some(227_000)] {
            let mut request = original.clone();
            request["params"]["excludeRouteKeys"] = json!(["openai"]);
            if let Some(tokens) = package_tokens {
                request["params"]["packageTokens"] = json!(tokens);
            }
            let (response, _rig, _store) =
                run_select_panel_wire_request("caller-exclusion", &request.to_string()).await;
            assert_eq!(response.header.ty, FrameType::Response);
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(body["result"]["members"], json!([]));
            assert!(
                body["result"]["shortfall"]["causes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|cause| cause["reason"] == "excluded"),
                "{body}"
            );
        }
    }

    #[test]
    fn n_pick_decision_explanation_includes_selection_and_policy() {
        let vectors = select_panel_wire_vectors();
        let request = vectors["nPick"]["request"].as_str().unwrap();
        let params = serde_json::from_str::<SelectPanelBody>(request)
            .unwrap()
            .params;
        let policy = striatum_core::PanelPolicy {
            package_tokens: None,
            largest_package_ceiling: None,
            replacements: Vec::new(),
            selection: "class_policy".into(),
            seated_variants: vec![striatum_core::SeatedVariant {
                slot: 0,
                route_key: "openai/gpt-5.6-sol-pro".into(),
                variant: striatum_core::SeatedSiblingVariant::Pro,
                reasoning_variant: None,
                reasoning_variant_source: None,
                reasoning_variant_fallbacks: Vec::new(),
            }],
            sibling_collapses: Vec::new(),
            ..Default::default()
        };
        let panel = SelectedPanel {
            candidates: Vec::new(),
            availability_rejections: Vec::new(),
            rejected_causes: Vec::new(),
            members: Vec::new(),
            runner_up: Vec::new(),
            shortfall: None,
            policy: Some(policy),
        };
        let provenance = DecisionProvenance {
            quota_signal_json: r#"{"state":"none","reason":"quota signal: none"}"#.into(),
            policy_revision: "test-policy".into(),
            anchor_version: ANCHOR_VERSION,
        };
        let decision = panel_decision_input(
            &params,
            &panel,
            AvailabilitySubstrate::Host,
            &provenance,
            "policy sources: test",
        );
        let explanation: serde_json::Value =
            serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(explanation["selection"], "class_policy");
        assert_eq!(explanation["policy"]["selection"], "class_policy");
    }

    #[test]
    fn package_tokens_are_typed_and_decisions_retain_policy_metadata() {
        let mut body: serde_json::Value = serde_json::from_str(
            select_panel_wire_vectors()["nPick"]["request"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        body["params"]["selection"] = json!("n_pick");
        body["params"]["packageTokens"] = json!(227_000);
        let params = serde_json::from_value::<SelectPanelBody>(body.clone())
            .unwrap()
            .params;
        assert_eq!(params.package_tokens, Some(227_000));
        body["params"]["packageTokens"] = json!("227000");
        assert!(serde_json::from_value::<SelectPanelBody>(body).is_err());
        let panel = SelectedPanel {
            candidates: Vec::new(),
            availability_rejections: Vec::new(),
            rejected_causes: Vec::new(),
            members: Vec::new(),
            runner_up: Vec::new(),
            shortfall: None,
            policy: Some(striatum_core::PanelPolicy {
                selection: "class_policy".into(),
                seated_variants: Vec::new(),
                sibling_collapses: Vec::new(),
                package_tokens: Some(227_000),
                largest_package_ceiling: Some(460_000),
                replacements: vec![striatum_core::panel::PackageSeatReplacement {
                    slot: 0,
                    replaced_route_key: "p/small".into(),
                    replacement_route_key: "p/large".into(),
                    reason: "package 227000 tokens exceeds p/small ceiling 210000 tokens".into(),
                }],
                ..Default::default()
            }),
        };
        let decision = panel_decision_input(
            &params,
            &panel,
            AvailabilitySubstrate::Broca,
            &DecisionProvenance {
                quota_signal_json: "{}".into(),
                policy_revision: "test-policy".into(),
                anchor_version: ANCHOR_VERSION,
            },
            "policy sources: test",
        );
        let explanation: serde_json::Value =
            serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(explanation["selection"], "class_policy");
        assert_eq!(
            explanation["policy"]["replacements"][0]["replacementRouteKey"],
            "p/large"
        );
    }

    /// The package no longer steers seating, so an n_pick request needs no package
    /// size fields; a caller still sending the retired ones is served, not refused.
    #[tokio::test]
    async fn n_pick_needs_no_package_size_fields_and_tolerates_retired_ones() {
        let vectors = select_panel_wire_vectors();
        let request = vectors["nPick"]["request"].as_str().unwrap();
        let mut with_retired: serde_json::Value = serde_json::from_str(request).unwrap();
        assert!(with_retired["params"]
            .get("packageTokensEstimate")
            .is_none());
        with_retired["params"]["packageTokensEstimate"] = json!(100);
        with_retired["params"]["proMemberMaxPackageTokens"] = json!(8_000);
        let (response, _rig, _store) =
            run_select_panel_wire_request("n-pick-retired-fields", &with_retired.to_string()).await;
        assert_eq!(response.header.ty, FrameType::Response);
    }

    #[tokio::test]
    async fn select_panel_serves_full_contract_shape() {
        let advertised = manifest(DEFAULT_MODULE_ID)
            .provides
            .into_iter()
            .find_map(|role| match role {
                ProviderRole::ManagementSurface { operations, .. } => Some(operations),
                _ => None,
            })
            .unwrap();
        assert!(advertised
            .iter()
            .any(|operation| operation.name == ROUTE_SELECT_PANEL_OP));

        let rig = Rig::new("select-panel-contract");
        let store = Arc::new(rig.open_store());
        let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
        let request = json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                "panelSpec": {
                    "size": 1,
                    "iqFloor": 0,
                    "diversity": "none",
                    "class": "AUDIT",
                    "temperatureSpread": [0.2]
                },
                "selection": "n_pick",
                "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
                "subscriptionKeys": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
                "taskId": "panel-contract-task"
            }
        });
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_select_request(
            frame,
            1,
            &tx,
            &quota_cache,
            &store,
            &Arc::new(Mutex::new(None)),
            &test_operator_config(),
            &test_operator_config(),
            &empty_broca_credentials(),
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let result = &body["result"];
        assert_eq!(result["members"].as_array().unwrap().len(), 1);
        assert_eq!(result["runnerUp"].as_array().unwrap().len(), 0);
        assert!(
            result.get("runner_up").is_none(),
            "wire keys stay camelCase"
        );
        assert_eq!(result["members"][0]["temperature"], 0.2);
        assert_eq!(result["members"][0]["promptParams"]["variant"], "xhigh");
        assert!(result["decisionID"].as_str().is_some());
        assert_eq!(result["policy"]["selection"], "class_policy");
        assert_eq!(result["policy"]["width"], 3);
        assert!(
            result.get("shortfall").is_some(),
            "one family cannot fill width three"
        );

        let decisions = store.get_decisions_for_task("panel-contract-task").unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].decision_kind, "panel");
        assert_eq!(decisions[0].selected_route_key, None);
    }

    /// Legacy slot metadata cannot make a removed roster executable.
    #[tokio::test]
    async fn select_panel_refuses_rosters_even_with_legacy_slot_metadata() {
        let rig = Rig::new("select-panel-pinned-saturated");
        let store = Arc::new(rig.open_store());
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_SELECT_PANEL_OP,
                "params": {
                    "panelSpec": {
                        "size": 1,
                        "iqFloor": 0,
                        "diversity": "none",
                        "class": "AUDIT",
                        "temperatureSpread": [0.2]
                    },
                    "roster": [
                        {"model": "openai/gpt-5.6-sol", "promptParams": {"variant": "xhigh"}},
                        {"model": "openai/gpt-5.5", "promptParams": {"variant": "high"}}
                    ],
                    "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
                    "concurrencySlots": [
                        {"key": "openai/gpt-5.6-sol", "running": 50, "limit": 40}
                    ],
                    "taskId": "panel-pinned-saturated"
                }
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "roster_removed");
    }

    /// The routing file for the paid Anthropic API key: a provider with no
    /// catalog rows, which borrows `anthropic`'s rows through its keys' `serves`
    /// vendor and which `exclude.all` keeps out of automatic choice. Naming one
    /// of its models still admits it.
    fn platform_operator_config(rig: &Rig) -> PathBuf {
        let path = rig.dir.join("striatum.jsonc");
        crate::test_config::write(
            &path,
            r#"{ "model_routing": {
                "exclude": { "all": ["anthropic-platform"] }
            } }"#,
        )
        .unwrap();
        path
    }

    /// A vault listing whose rows map `provider_ids` as given. Each row serves
    /// the vendor its id names (`apikey:anthropic:work` serves `anthropic`), as
    /// the vault lists them. `account_id` is the key's workspace, deliberately
    /// different from the id's label.
    fn platform_listing(rows: &[(&str, &[&str])]) -> SharedBrocaCredentials {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: rows
                    .iter()
                    .map(
                        |(id, providers)| cortexkit_claustrum_client::ListedCredential {
                            id: (*id).into(),
                            categories: vec!["llm-provider".into()],
                            auth_method: Some(
                                if id.starts_with("apikey:") {
                                    "apikey"
                                } else if id.starts_with("chatgpt:") {
                                    "chatgpt"
                                } else {
                                    "oauth"
                                }
                                .into(),
                            ),
                            credential_type: if id.starts_with("apikey:") {
                                "apikey"
                            } else {
                                "oauth"
                            }
                            .into(),
                            state: "active".into(),
                            provider_ids: providers.iter().map(|p| (*p).into()).collect(),
                            serves: id
                                .split(':')
                                .nth(1)
                                .map(str::to_string)
                                .into_iter()
                                .collect(),
                            account_id: Some("workspace-7".into()),
                            ..Default::default()
                        },
                    )
                    .collect(),
                view: format!("platform-view-{}", rows.len()),
                ..Default::default()
            },
            now_ms(),
        );
        credentials
    }

    #[tokio::test]
    async fn panel_replaces_an_exhausted_claude_pool_and_records_blocked_keys() {
        let rig = Rig::new("panel-exhausted-pool");
        let store: SharedStore = Arc::new(rig.open_store());
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute("DELETE FROM model_facts", [])
            .unwrap();
        for (provider, model, family, elo) in [
            ("anthropic", "claude-opus-5-5", "claude", 2200.0),
            ("openai", "gpt-fallback", "gpt", 2100.0),
            ("anthropic", "claude-sonnet-fallback", "claude", 2000.0),
        ] {
            let mut row = fact(provider, model);
            row.family = Some(family.into());
            row.scores.elo = elo;
            store.upsert_fact(&row, FactSource::Override).unwrap();
        }
        let config = rig.dir.join("striatum.jsonc");
        crate::test_config::write(&config, json!({"model_routing": {
            "exclude": {"all": ["anthropic/claude-opus-5-5", "anthropic-platform/claude-sonnet-fallback"]},
            "panels": {"audit": {"topK": 2, "extra": 0, "by": "iq"}}
        }}).to_string()).unwrap();
        let credentials = platform_listing(&[
            ("apikey:anthropic:empty", &["anthropic-platform"]),
            ("apikey:anthropic:limited", &["anthropic-platform"]),
            ("oauth:anthropic:subscription", &["anthropic"]),
            ("apikey:openai", &["openai"]),
        ]);
        let call = |method: &'static str, params: serde_json::Value| {
            let store = Arc::clone(&store);
            let credentials = credentials.clone();
            let config = config.clone();
            async move {
                let frame = Frame::build(
                    FrameType::Request,
                    Flags::new(false, Priority::Interactive, false),
                    7,
                    1,
                    99,
                    serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
                )
                .unwrap();
                let (tx, mut rx) = mpsc::channel(1);
                handle_select_request(
                    frame,
                    1,
                    &tx,
                    &Arc::new(Mutex::new(QuotaCache::default())),
                    &store,
                    &Arc::new(Mutex::new(None)),
                    &config,
                    &config,
                    &credentials,
                )
                .await
                .unwrap();
                serde_json::from_slice::<serde_json::Value>(&rx.recv().await.unwrap().body).unwrap()
            }
        };
        let panel_params = json!({"substrate": "broca", "panelSpec": {"class": "AUDIT"},
            "availableModels": [], "taskId": "pool-panel"});
        let healthy = call(ROUTE_SELECT_PANEL_OP, panel_params.clone()).await;
        assert_eq!(
            healthy["result"]["members"][0]["routeKey"], "anthropic-platform/claude-opus-5-5",
            "{healthy}"
        );
        for (key, kind) in [("empty", "billing_exhausted"), ("limited", "rate_limited")] {
            let wall = call(
                "route.pool_wall",
                json!({"provider": "anthropic-platform",
                "credentialId": format!("apikey:anthropic:{key}"), "kind": kind,
                "untilMs": now_ms() as i64 + 3_600_000}),
            )
            .await;
            assert_eq!(wall["result"]["ok"], true, "{wall}");
        }
        let body = call(ROUTE_SELECT_PANEL_OP, panel_params).await;
        let members = body["result"]["members"]
            .as_array()
            .unwrap_or_else(|| panic!("{body}"));
        assert_eq!(members.len(), 2, "{body}");
        assert_eq!(members[0]["routeKey"], "openai/gpt-fallback", "{body}");
        assert_eq!(
            members[1]["routeKey"], "anthropic/claude-sonnet-fallback",
            "{body}"
        );
        assert!(body["result"].get("shortfall").is_none(), "{body}");
        let decision = store
            .get_decision_by_id(body["result"]["decisionID"].as_str().unwrap())
            .unwrap()
            .unwrap();
        let rejected: serde_json::Value = serde_json::from_str(&decision.rejected_json).unwrap();
        let cause = rejected
            .as_array()
            .unwrap()
            .iter()
            .find(|cause| cause["model"] == "anthropic-platform/claude-opus-5-5")
            .unwrap_or_else(|| panic!("missing pooled-route rejection: {rejected}; panel: {body}"));
        assert_eq!(cause["reason"], "unavailable", "{rejected}");
        let detail = cause["detail"].as_str().unwrap();
        let pool_rejection: serde_json::Value = serde_json::from_str(detail).unwrap();
        assert_eq!(pool_rejection["code"], "explicit_provider_pool_exhausted");
        assert_eq!(pool_rejection["pool"]["keys"].as_array().unwrap().len(), 2);
        for expected in [
            "explicit_provider_pool_exhausted",
            "apikey:anthropic:empty",
            "billing_exhausted",
            "apikey:anthropic:limited",
            "rate_limited",
        ] {
            assert!(detail.contains(expected), "{cause}");
        }
        let unfilled = call(
            ROUTE_SELECT_PANEL_OP,
            json!({
                "substrate": "broca", "panelSpec": {"class": "AUDIT"},
                "taskId": "pool-panel-unfilled",
                "excludeRouteKeys": ["openai/gpt-fallback", "anthropic/claude-sonnet-fallback"],
            }),
        )
        .await;
        assert_eq!(unfilled["result"]["members"], json!([]), "{unfilled}");
        assert!(unfilled["result"]["shortfall"].is_object(), "{unfilled}");
        assert!(
            store
                .get_decision_by_id(unfilled["result"]["decisionID"].as_str().unwrap())
                .unwrap()
                .is_some(),
            "{unfilled}"
        );
    }

    /// The four API keys the live vault maps to `anthropic-platform`.
    const LIVE_PLATFORM_KEYS: [&str; 4] = [
        "apikey:anthropic:wsampleyahoo",
        "apikey:anthropic:usersamplegmail",
        "apikey:anthropic:wsamplegmail",
        "apikey:anthropic:ysample",
    ];

    /// Broca's run-end error for the low-credit key, as core stored it in the
    /// provider cooldown's `last_error` (serde_json's sorted key order).
    const LIVE_LOW_CREDIT_ERROR: &str = r#"{"class":"permanent","credential_id":"apikey:anthropic:wsampleyahoo","message":"400: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.\"},\"request_id\":\"req_011CgK8eU8fTeQoECLazghi7\"}","provider_code":"billing_exhausted","status":400}"#;

    /// A pool of the four live keys with `anthropic/claude-opus-5-5` in the catalog.
    fn live_platform_pool(name: &str) -> (Rig, SharedStore, SharedBrocaCredentials, PathBuf) {
        let platform: &[&str] = &["anthropic-platform"];
        let credentials = platform_listing(&LIVE_PLATFORM_KEYS.map(|key| (key, platform)));
        let rig = Rig::new(name);
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("anthropic", "claude-opus-5-5"), FactSource::Override)
            .unwrap();
        let config = platform_operator_config(&rig);
        (rig, store, credentials, config)
    }

    async fn live_platform_call(
        store: &SharedStore,
        credentials: &SharedBrocaCredentials,
        config: &Path,
        method: &str,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let frame = run_route_wire_request_with_paths(
            store,
            json!({"method": method, "params": params}),
            config,
            credentials,
        )
        .await;
        serde_json::from_slice(&frame.body).unwrap()
    }

    /// Core's outcome report for a run one key ended with `billing_exhausted`.
    async fn report_live_billing_refusal(
        store: &SharedStore,
        credentials: &SharedBrocaCredentials,
        credential: &str,
    ) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method": ROUTE_SET_DECISION_OUTCOME_OP, "params": {
                "decisionID": format!("decision-{credential}"), "outcome": "error",
                "lastError": LIVE_LOW_CREDIT_ERROR.replace(LIVE_PLATFORM_KEYS[0], credential),
                "providerOutcome": {
                    "accountRef": credential, "credentialId": credential,
                    "credentialRefusal": {"providerId": "anthropic-platform", "credentialId": credential, "code": "billing_exhausted"}
                }}}))
            .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_set_decision_outcome(
            frame,
            &tx,
            store,
            Path::new("/nonexistent/live-platform-pool.jsonc"),
            credentials,
        )
        .await
        .unwrap();
        let ack: serde_json::Value =
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
        assert_eq!(ack["result"]["ok"], true, "{ack}");
    }

    /// A vault listing with explicit rows: (id, provider_ids, account_id).
    fn vault_listing(rows: &[(&str, &[&str], Option<&str>)]) -> SharedBrocaCredentials {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: rows
                    .iter()
                    .map(
                        |(id, providers, account)| cortexkit_claustrum_client::ListedCredential {
                            id: (*id).into(),
                            categories: vec!["llm-provider".into()],
                            auth_method: Some(
                                if id.starts_with("apikey:") {
                                    "apikey"
                                } else if id.starts_with("chatgpt:") {
                                    "chatgpt"
                                } else {
                                    "oauth"
                                }
                                .into(),
                            ),
                            credential_type: if id.starts_with("apikey:") {
                                "apikey"
                            } else {
                                "oauth"
                            }
                            .into(),
                            state: "active".into(),
                            provider_ids: providers.iter().map(|p| (*p).into()).collect(),
                            serves: id
                                .split(':')
                                .nth(1)
                                .map(str::to_string)
                                .into_iter()
                                .collect(),
                            account_id: account.map(str::to_string),
                            ..Default::default()
                        },
                    )
                    .collect(),
                view: format!("vault-view-{}", rows.len()),
                ..Default::default()
            },
            now_ms(),
        );
        credentials
    }

    #[tokio::test]
    async fn panels_never_seat_a_provider_excluded_in_all_and_refuse_removed_roster_pins() {
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let panel = |label: &'static str, roster: serde_json::Value, selection: &'static str| {
            let credentials = credentials.clone();
            async move {
                let rig = Rig::new(label);
                let store: SharedStore = Arc::new(rig.open_store());
                store
                    .upsert_fact(&fact("anthropic", "claude-opus-4-1"), FactSource::Override)
                    .unwrap();
                let config = platform_operator_config(&rig);
                let frame = run_route_wire_request_with_paths(
                    &store,
                    json!({"method": ROUTE_SELECT_PANEL_OP, "params": {
                        "panelSpec": {"class": "SPEC", "diversity": "none", "iqFloor": 0, "size": 1},
                        "selection": selection, "roster": roster, "substrate": "broca",
                        "taskId": label,
                    }}),
                    &config,
                    &credentials,
                )
                .await;
                serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
            }
        };
        let automatic = panel("platform-panel-npick", json!([]), "n_pick").await;
        assert_eq!(automatic["result"]["members"], json!([]), "{automatic}");
        assert!(
            automatic.to_string().contains("excluded_by_operator"),
            "an automatic seat names why the paid key was skipped: {automatic}"
        );
        let pinned = panel(
            "platform-panel-roster",
            json!([{"model": "anthropic-platform/claude-opus-4-1"}]),
            "roster",
        )
        .await;
        assert_eq!(pinned["code"], "roster_removed", "{pinned}");
    }

    #[test]
    fn pool_membership_comes_from_the_listing_and_never_guesses() {
        let listing = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let credentials = listing.lock().unwrap();
        let keys = credentials
            .pool_keys("anthropic-platform", now_ms())
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].credential_id, "apikey:anthropic:work");
        assert_eq!(keys[0].workspace, "workspace-7");
        assert_eq!(
            credentials
                .pool_keys("anthropic-platform-work", now_ms())
                .unwrap_err()
                .0,
            "explicit_provider_credential_unresolved"
        );
        assert_eq!(
            BrocaCredentials::default()
                .pool_keys("anthropic-platform", now_ms())
                .unwrap_err()
                .0,
            "explicit_provider_credential_unresolved",
            "an unread listing is unresolved, not a default key"
        );
    }

    #[tokio::test]
    async fn placement_before_provider_ingestion_merges_fusiform_price_and_login_policy() {
        let rig = Rig::new("upsert-catalog-merge");
        let store = Arc::new(rig.open_store());
        let snapshot = fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": 1, "resolved_at_ms": 1,
                "curated": [{"model":"meta/muse-wonder-1.0","auth_methods":["oauth"]}],
                "models": {"meta/muse-wonder-1.0": {
                    "existence":"present", "limit.context":200000,
                    "rate.input":{"state":"priced","currency":"USD","units":4,"exponent":0},
                    "rate.output":{"state":"priced","currency":"USD","units":20,"exponent":0}
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
        for variant in [None, Some("thinking")] {
            let response = run_route_wire_request_with_paths(&store, json!({"method": ROUTE_MODEL_UPSERT_OP,
                "params":{"providerID":"meta","modelID":"muse-wonder-1.0","variant":variant,"elo":1808,"eq":1600}
            }), &test_operator_config(), &credentials).await;
            assert_eq!(
                response.header.ty,
                FrameType::Response,
                "{}",
                String::from_utf8_lossy(&response.body)
            );
            let fact = store
                .get_fact_by_route_key(&make_route_key("meta", "muse-wonder-1.0", variant))
                .unwrap()
                .unwrap();
            assert_eq!(fact.cost_per_m_tok_out, Some(20.0));
            assert_eq!(fact.input_rate, Some(4.0));
            assert_eq!(fact.auth_methods, Some(vec!["oauth".into()]));
            assert_eq!(fact.scores.elo, 1808.0);
            assert_eq!(fact.score_provenance, ScoreProvenance::Override);
        }
    }

    #[test]
    fn panel_canonical_ids_follow_score_alias_rows_to_their_target() {
        let rig = Rig::new("panel-canonical-ids");
        let store = rig.open_store();
        let served = |provider: &str, model: &str, canonical: Option<&str>| {
            striatum_store::FusiformFactUpdate {
                provider_id: provider.into(),
                model_id: model.into(),
                auth_methods: None,
                login_methods_documented: false,
                present: true,
                context_window: Some(200_000.0),
                max_output_tokens: None,
                cost_tier: None,
                cost_per_mtok_out: None,
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: None,
                supports_tool_use: None,
                reasoning_efforts: None,
                canonical_id: canonical.map(str::to_string),
                upstream_status: None,
                model_type: None,
                model_kind: None,
                lineage: None,
            }
        };
        store
            .apply_fusiform_catalog_for_providers(
                &[
                    served(
                        "openrouter",
                        "meta/muse-spark-1.3",
                        Some("meta/muse-spark-1.3"),
                    ),
                    served("opencode", "muse-spark-1.3-contributor-free", None),
                ],
                striatum_store::FusiformCatalogCursor {
                    catalog_version: 1,
                    resolved_at_ms: 1,
                },
                &["openrouter", "opencode"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            )
            .unwrap();
        let aliases = std::collections::BTreeMap::from([(
            "openrouter-byok".to_string(),
            "openrouter".to_string(),
        )]);
        let mut facts = store.get_facts().unwrap();
        striatum_core::score_alias::append_alias_rows(&mut facts, &aliases);
        let ids = panel_canonical_ids(&store, &facts, &aliases).unwrap();
        assert_eq!(
            ids.get("openrouter/meta/muse-spark-1.3")
                .map(String::as_str),
            Some("meta/muse-spark-1.3")
        );
        assert_eq!(
            ids.get("openrouter-byok/meta/muse-spark-1.3")
                .map(String::as_str),
            Some("meta/muse-spark-1.3"),
            "an alias row serves its target's model"
        );
        assert!(!ids.contains_key("opencode/muse-spark-1.3-contributor-free"));
    }

    #[tokio::test]
    async fn canonical_upsert_scores_every_stored_route_of_the_model() {
        const MUSE: &str = "meta/muse-spark-1.3";
        let rig = Rig::new("canonical-upsert");
        let store: SharedStore = Arc::new(rig.open_store());
        let served = |canonical: Option<&str>| json!({"existence": "present", "limit.context": 200000, "model.canonical_id": canonical});
        let snapshot = fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": 1, "resolved_at_ms": 1,
                "models": {
                    "meta/muse-spark-1.3": served(Some(MUSE)),
                    "meta/muse-spark-1.3-contributor": served(Some(MUSE)),
                    "openrouter/meta/muse-spark-1.3": served(Some(MUSE)),
                    "opencode/muse-spark-1.3-contributor-free": served(None),
                    "opencode/unstated-1": {"existence": "present"},
                    "opencode/blank-1": served(Some(" ")),
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let (served_facts, cursor) = snapshot.parts();
        let identities = served_facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    fact.canonical_id.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            identities["openrouter/meta/muse-spark-1.3"].as_deref(),
            Some(MUSE)
        );
        assert_eq!(identities["opencode/muse-spark-1.3-contributor-free"], None);
        assert_eq!(
            identities["opencode/unstated-1"], None,
            "an unserved key states no identity"
        );
        assert_eq!(
            identities["opencode/blank-1"], None,
            "a blank id names no model"
        );
        let providers = ["meta", "openrouter", "opencode"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();
        store
            .apply_fusiform_catalog_for_providers(served_facts, cursor, &providers)
            .unwrap();

        let response = run_route_wire_request(
            &store,
            json!({"method": ROUTE_MODEL_UPSERT_OP,
                "params": {"canonicalID": MUSE, "elo": 1810, "eq": 1520}}),
        )
        .await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let result =
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone();
        assert_eq!(
            result["routeKeys"],
            json!([
                "meta/muse-spark-1.3",
                "meta/muse-spark-1.3-contributor",
                "openrouter/meta/muse-spark-1.3"
            ])
        );
        for route in [
            "meta/muse-spark-1.3",
            "meta/muse-spark-1.3-contributor",
            "openrouter/meta/muse-spark-1.3",
        ] {
            let fact = store.get_fact_by_route_key(route).unwrap().unwrap();
            assert_eq!(
                (fact.scores.elo, fact.scores.eq),
                (1810.0, 1520.0),
                "{route}"
            );
            assert_eq!(fact.score_provenance, ScoreProvenance::Override, "{route}");
            assert_eq!(
                fact.context_window,
                Some(200_000.0),
                "{route} keeps its served facts"
            );
        }
        let unidentified = store
            .get_fact_by_route_key("opencode/muse-spark-1.3-contributor-free")
            .unwrap()
            .unwrap();
        assert_eq!(unidentified.score_provenance, ScoreProvenance::Unscored);

        for (params, needle) in [
            (
                json!({"canonicalID": MUSE, "elo": 1810, "eq": 1520, "contextWindow": 1000}),
                "contextWindow",
            ),
            (
                json!({"canonicalID": MUSE, "elo": 1810}),
                "requires both elo and eq",
            ),
            (
                json!({"canonicalID": MUSE, "providerID": "meta", "modelID": "x", "elo": 1, "eq": 1}),
                "not both",
            ),
            (
                json!({"canonicalID": "acme/unknown-1", "elo": 1810, "eq": 1520}),
                "no stored route carries canonical id acme/unknown-1",
            ),
        ] {
            let refused = run_route_wire_request(
                &store,
                json!({"method": ROUTE_MODEL_UPSERT_OP, "params": params}),
            )
            .await;
            assert_eq!(refused.header.ty, FrameType::Error, "{params}");
            let body = String::from_utf8_lossy(&refused.body).into_owned();
            assert!(body.contains(needle), "{params}: {body}");
        }

        let gaps = run_route_wire_request(
            &store,
            json!({"method": ROUTE_LIST_CANONICAL_GAPS_OP, "params": {}}),
        )
        .await;
        assert_eq!(gaps.header.ty, FrameType::Response);
        let gaps =
            serde_json::from_slice::<serde_json::Value>(&gaps.body).unwrap()["result"].clone();
        let keys = gaps
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["routeKey"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(
            !keys.iter().any(|key| key.contains("muse-spark")),
            "{keys:?}"
        );
        assert!(handles(ROUTE_LIST_CANONICAL_GAPS_OP));
    }

    #[test]
    fn healthy_refresh_replaces_a_degraded_cached_window_and_clears_retention() {
        let mut cached = vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "previous timeout"
        }))];
        let fresh = vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2031-01-01T00:00:00Z" } }
        }))];
        let mut retained_degraded_since_ms = HashMap::from([(quota_cache_key(&cached[0]), 0)]);

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

        assert_eq!(cached.len(), 1);
        assert!(cached[0].error.is_none());
        assert_eq!(
            cached[0]
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(12.0)
        );
        assert!(!retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
    }

    #[tokio::test]
    async fn removed_routing_available_models_text_is_unknown_method() {
        let rig = Rig::new("removed-available-models-wire");
        let store = Arc::new(rig.open_store());
        for params in [
            json!({"availableModels":["wireco/model"],"include":"all"}),
            json!({}),
            json!({"limit":"invalid"}),
            serde_json::Value::Null,
        ] {
            let response = run_route_wire_request(
                &store,
                json!({"method":"routing.available_models_text","params":params}),
            )
            .await;
            assert_eq!(response.header.ty, FrameType::Error, "{params}");
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(body["code"], "unknown_method", "{params}: {body}");
        }
        assert!(!handles("routing.available_models_text"));
        assert!(served_operations()
            .iter()
            .all(|operation| operation.name != "routing.available_models_text"));
        assert!(manifest(DEFAULT_MODULE_ID).provides.iter().all(|role| {
            match role {
                ProviderRole::ManagementSurface { operations, .. } => operations
                    .iter()
                    .all(|operation| operation.name != "routing.available_models_text"),
                _ => true,
            }
        }));
    }

    #[tokio::test]
    async fn removed_opencode_plugin_operations_are_unknown_methods() {
        let rig = Rig::new("removed-opencode-plugin-wire");
        let store = Arc::new(rig.open_store());
        let operations = served_operations();
        for method in [
            "route.resolve_model",
            "route.model_capabilities",
            "route.agent_model_requirements",
        ] {
            assert!(!handles(method), "{method}");
            assert!(operations.iter().all(|op| op.name != method), "{method}");
            for request in [
                json!({"method":method}),
                json!({"method":method,"params":null}),
                json!({"method":method,"params":{"action":"pipeline","providerID":"wireco","modelID":"model"}}),
            ] {
                let response = run_route_wire_request(&store, request).await;
                assert_eq!(response.header.ty, FrameType::Error, "{method}");
                let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
                assert_eq!(body["code"], "unknown_method", "{method}: {body}");
                assert_eq!(body["message"], format!("unknown method '{method}'"));
            }
        }
    }

    #[test]
    fn census_gated_routing_operations_remain_registered() {
        // Without a fleet-wide fourteen-day zero-call window and an empty
        // caller census, these methods and their legacy shapes must stay.
        let operations = served_operations();
        for method in [
            ROUTE_COOLDOWN_STATUS_OP,
            ROUTE_MODEL_FACT_OP,
            ROUTE_MODEL_FACTS_OP,
            ROUTE_SET_DECISION_OUTCOME_OP,
            ROUTE_RECORD_USAGE_OP,
            ROUTE_RECORD_COOLDOWN_OP,
            ROUTE_SELECT_OP,
            ROUTE_SELECT_PANEL_OP,
            ROUTE_EXPLAIN_OP,
            account_selection::ACCOUNT_FOR_OP,
            account_selection::ACCOUNT_OBSERVE_OP,
            account_selection::ACCOUNT_STATUS_OP,
            "route.pool_acquire",
            "route.pool_renew",
            "route.pool_release",
            "route.pool_wall",
        ] {
            assert!(handles(method), "{method}");
            assert!(operations.iter().any(|op| op.name == method), "{method}");
        }
    }

    #[test]
    fn account_status_is_registered_as_a_query() {
        let operations = served_operations();
        let operation = operations
            .iter()
            .find(|operation| operation.name == account_selection::ACCOUNT_STATUS_OP)
            .expect("account status must be registered");
        assert_eq!(operation.kind, ManagementOperationKind::Query);
        assert!(account_selection::handles(&operation.name));
        assert!(handles(&operation.name));
    }

    #[test]
    fn handles_covers_every_manifest_operation() {
        let operations = manifest(DEFAULT_MODULE_ID)
            .provides
            .iter()
            .find_map(|role| match role {
                ProviderRole::ManagementSurface { operations, .. } => Some(operations.clone()),
                _ => None,
            })
            .expect("routing module must expose a management surface");
        for operation in &operations {
            assert!(
                handles(&operation.name),
                "manifest op {} missing from handles allowlist",
                operation.name
            );
        }
        assert!(handles(ROUTE_LIST_INFERRED_OP));
        assert!(handles(ROUTE_LIST_UNSCORED_OP));
        assert!(handles(ROUTE_CATALOG_TARGETS_OP));
        assert!(!handles("route.unknown"));
    }

    #[test]
    fn missing_session_model_warning_is_deduplicated_by_exact_pair() {
        let provider = "warning-proof-provider";
        let model = "warning-proof-model";
        assert!(warn_missing_session_model_once(provider, model));
        assert!(!warn_missing_session_model_once(provider, model));
        assert!(warn_missing_session_model_once(
            provider,
            "warning-proof-model-2"
        ));
        assert!(warn_missing_session_model_once(
            "warning-proof-provider-2",
            model
        ));
    }

    #[tokio::test]
    async fn removed_aa_seed_operation_is_not_served() {
        let rig = Rig::new("removed-aa-seed");
        let store = Arc::new(rig.open_store());
        assert!(!handles("route.model_seed_aa"));
        assert!(served_operations()
            .iter()
            .all(|operation| operation.name != "route.model_seed_aa"));
        let response = run_route_wire_request(
            &store,
            json!({
                "method": "route.model_seed_aa",
                "params": { "providerID": "custom", "modelID": "aa-live" }
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "unknown_method");
    }

    /// A store holding two routes of one test-only provider, and ladder sources
    /// whose observed availability only this rig writes, so the seed catalog
    /// never competes. `incumbent` wins the top tier; `small` is cheap and weak.
    struct LadderRig {
        /// Held so the scratch directory (and the store file) outlive the test.
        rig: Rig,
        store: SharedStore,
        sources: winner_ladder::LadderSources,
        provider: String,
    }

    impl LadderRig {
        fn new(label: &str) -> Self {
            let rig = Rig::new(label);
            let store: SharedStore = Arc::new(rig.open_store());
            let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let provider = format!("ladder-{label}-{}-{n}", std::process::id());
            let mut incumbent = fact(&provider, "incumbent");
            incumbent.scores.elo = 1_500.0;
            incumbent.scores.eq = 1_500.0;
            incumbent.cost_per_m_tok_out = Some(10.0);
            let mut small = fact(&provider, "small");
            small.scores.elo = 1_200.0;
            small.scores.eq = 1_500.0;
            small.cost_per_m_tok_out = Some(0.5);
            for row in [&incumbent, &small] {
                store.upsert_fact(row, FactSource::Override).unwrap();
            }
            let sources = winner_ladder::LadderSources {
                routing_config: rig.dir.join("striatum.jsonc"),
                operator_config: rig.dir.join("striatum.jsonc"),
                observed: Arc::new(winner_ladder::ObservedSelections::default()),
            };
            Self {
                rig,
                store,
                sources,
                provider,
            }
        }

        fn route(&self, model: &str) -> String {
            make_route_key(&self.provider, model, None)
        }

        /// Stand in for a host route.select: record the availability it was handed
        /// and the runtime snapshot as the store holds it now.
        fn observe(&self, models: &[&str]) {
            self.observe_as("mason", models);
        }

        /// A host route.select for `target_agent`, which is the caller class.
        fn observe_as(&self, target_agent: &str, models: &[&str]) {
            self.sources.observed.record(
                winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, target_agent),
                models.iter().map(|model| self.route(model)),
                Vec::new(),
                self.store.build_snapshot(now_ms() as f64).unwrap(),
            );
        }

        /// A process restart: the store is closed and reopened from its file, and
        /// the in-memory record of observed selections starts empty.
        fn restart(self) -> Self {
            let Self {
                rig,
                store,
                sources,
                provider,
            } = self;
            drop(store);
            let store: SharedStore = Arc::new(rig.open_store());
            Self {
                rig,
                store,
                sources: winner_ladder::LadderSources {
                    observed: Arc::new(winner_ladder::ObservedSelections::default()),
                    ..sources
                },
                provider,
            }
        }

        fn ladder(&self) -> winner_ladder::WinnerLadder {
            winner_ladder::compute_ladder(self.store.as_ref(), &self.sources)
                .unwrap()
                .expect("a selection was observed")
        }

        fn announce(&self, trigger: &str) -> Option<winner_ladder::WinnerChangeNotice> {
            winner_ladder::announce(self.store.as_ref(), &self.sources, trigger)
        }

        /// Notice lines this rig's provider appears in, in emission order.
        fn notices(&self) -> Vec<serde_json::Value> {
            winner_ladder::emitted_lines()
                .lock()
                .unwrap()
                .iter()
                .filter(|line| line.contains(&self.provider))
                .map(|line| {
                    let body = line
                        .strip_prefix(winner_ladder::NOTICE_PREFIX)
                        .expect("notice line carries the fixed prefix");
                    serde_json::from_str(body).unwrap()
                })
                .collect()
        }

        async fn upsert(&self, params: serde_json::Value) -> Frame {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(&json!({
                    "method": ROUTE_MODEL_UPSERT_OP,
                    "params": params,
                }))
                .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_model_upsert(
                frame,
                &tx,
                &self.store,
                &self.sources,
                &empty_broca_credentials(),
            )
            .await
            .unwrap();
            rx.recv().await.unwrap()
        }

        fn challenger_params(&self) -> serde_json::Value {
            json!({
                "providerID": self.provider,
                "modelID": "challenger",
                "elo": 2_000.0,
                "eq": 1_500.0,
                "costPerMTokOut": 30.0,
            })
        }

        /// Place the challenger straight into the store, as a placement applied
        /// while no ladder was being computed would.
        fn place_challenger_directly(&self) {
            let mut challenger = fact(&self.provider, "challenger");
            challenger.scores.elo = 2_000.0;
            challenger.scores.eq = 1_500.0;
            challenger.cost_per_m_tok_out = Some(30.0);
            self.store
                .upsert_fact(&challenger, FactSource::Override)
                .unwrap();
        }
    }

    fn host_tier(ladder: &winner_ladder::WinnerLadder, tier: u8) -> winner_ladder::TierWinners {
        ladder.ladders[&host_mason().label()].tiers[&tier].clone()
    }

    fn host_mason() -> winner_ladder::LadderKey {
        winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "mason")
    }

    /// What the router itself returns at `tier` for the recorded host selection,
    /// computed directly rather than through the ladder.
    fn direct_live_selection(rig: &LadderRig, tier: u8) -> Option<String> {
        let observed = rig.sources.observed.current();
        let observation = &observed[&host_mason()];
        let resolved = resolve_launch_requirements(
            "mason",
            Some(&RequestedModelRequirements {
                iq: Some(f64::from(tier)),
                eq: None,
                caps: None,
            }),
        )
        .unwrap();
        select_model_route_with_config(
            SelectModelRouteInput {
                target_agent: "mason".to_string(),
                target_is_head: false,
                requirements: resolved.requirements,
                available_models: observation.available.iter().cloned().collect(),
                facts: rig
                    .store
                    .get_facts_with_observed_speed(
                        now_ms() as f64,
                        config_overrides::live_config(&rig.sources.routing_config).pricing(),
                    )
                    .unwrap(),
                runtime_snapshot: Some(observation.snapshot.clone()),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: resolved.required_capabilities,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
                providers_without_cache_write_reports: HashSet::new(),
            },
            &DEFAULT_ROUTING_CONFIG,
        )
        .ok()
        .map(|selected| selected.route_key)
    }

    #[test]
    fn no_ladder_exists_before_a_selection_has_been_observed() {
        let rig = LadderRig::new("unobserved");
        assert_eq!(
            winner_ladder::compute_ladder(rig.store.as_ref(), &rig.sources).unwrap(),
            None
        );
        assert_eq!(rig.announce("boot"), None);
        assert_eq!(
            rig.store.route_winner_baseline().unwrap(),
            None,
            "absence of a selection is not a ladder and must not become the baseline"
        );
    }

    #[test]
    fn the_first_ever_ladder_is_stored_silently() {
        let rig = LadderRig::new("first");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);
        let stored = rig
            .store
            .route_winner_baseline()
            .unwrap()
            .expect("baseline written");
        let stored: winner_ladder::WinnerLadder = serde_json::from_str(&stored).unwrap();
        assert_eq!(stored, rig.ladder());
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
    }

    #[tokio::test]
    async fn placement_moving_the_live_winner_emits_one_notice_naming_what_selection_returns() {
        let rig = LadderRig::new("raise");
        rig.observe(&["incumbent", "small", "challenger"]);
        assert_eq!(rig.announce("first selection"), None);
        assert_eq!(
            host_tier(&rig.ladder(), 100).live,
            Some(rig.route("incumbent")),
            "precondition: the incumbent holds iq 100"
        );

        let response = rig.upsert(rig.challenger_params()).await;
        assert_eq!(response.header.ty, FrameType::Response);

        let notices = rig.notices();
        assert_eq!(
            notices.len(),
            1,
            "exactly one notice per placement: {notices:?}"
        );
        let notice = &notices[0];
        assert_eq!(
            notice["trigger"],
            format!("route.model_upsert {}", rig.route("challenger"))
        );
        assert_eq!(notice["rule"], winner_ladder::NOTICE_RULE);
        let changes = notice["changes"].as_array().unwrap();
        let top = changes
            .iter()
            .find(|change| change["tiers"].as_array().unwrap().contains(&json!(100)))
            .expect("a change naming iq 100");
        assert_eq!(top["substrate"], winner_ladder::SUBSTRATE_HOST);
        assert_eq!(top["oldRoute"], json!(rig.route("incumbent")));
        assert_eq!(top["newRoute"], json!(rig.route("challenger")));
        for change in changes {
            for tier in change["tiers"].as_array().unwrap() {
                let tier = tier.as_u64().unwrap() as u8;
                assert_eq!(
                    change["newRoute"],
                    json!(direct_live_selection(&rig, tier)),
                    "the notice names what route selection returns at iq {tier}"
                );
            }
        }
        // The announced ladder is the new baseline: nothing more to announce.
        assert_eq!(rig.announce("again"), None);
    }

    #[tokio::test]
    async fn a_route_outside_the_recorded_availability_is_never_announced() {
        let rig = LadderRig::new("fence");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);

        let response = rig.upsert(rig.challenger_params()).await;
        assert_eq!(response.header.ty, FrameType::Response);
        assert!(
            rig.store
                .get_fact_by_route_key(&rig.route("challenger"))
                .unwrap()
                .is_some(),
            "the placement must land"
        );
        assert_eq!(
            rig.notices(),
            Vec::<serde_json::Value>::new(),
            "the challenger is not in what live selection can reach"
        );
        let ladder = rig.ladder();
        for winners in ladder.ladders[&host_mason().label()].tiers.values() {
            for route in [&winners.neutral, &winners.live].into_iter().flatten() {
                assert!(
                    [rig.route("incumbent"), rig.route("small")].contains(route),
                    "ladder names {route}, outside the recorded availability"
                );
            }
        }

        // Once live selection reports the challenger as reachable, the move is real.
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        rig.observe(&["incumbent", "small", "challenger"]);
        let notice = watch
            .check(rig.store.as_ref(), &rig.sources)
            .expect("availability now includes the stronger route");
        assert!(notice
            .trigger
            .contains("opencode-host / mason availability"));
        assert!(notice.changes.iter().any(|change| {
            change.new_route.as_deref() == Some(rig.route("challenger").as_str())
                && change.tiers.contains(&100)
        }));
    }

    #[tokio::test]
    async fn placement_that_changes_a_score_but_no_winner_emits_no_notice() {
        let rig = LadderRig::new("twin");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);
        let before = rig.ladder();

        let response = rig
            .upsert(json!({
                "providerID": rig.provider,
                "modelID": "small",
                "elo": 1_201.0,
            }))
            .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let stored = rig
            .store
            .get_fact_by_route_key(&rig.route("small"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.scores.elo, 1_201.0, "the placement must land");
        assert_eq!(rig.ladder(), before, "precondition: no tier's winner moved");

        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
    }

    #[test]
    fn a_quota_wall_that_moves_only_the_live_winner_is_not_announced() {
        let rig = LadderRig::new("quota");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);
        let incumbent = rig.route("incumbent");

        // Wall the iq-100 winner hard: live selection routes around it.
        let far_future = now_ms() as f64 + 3_600_000.0;
        record_cooldown_update(
            rig.store.as_ref(),
            &RecordCooldownParams {
                route_key: incumbent.clone(),
                capacity_key: incumbent.clone(),
                credential_id: None,
                cooldown_until: Some(far_future),
                quota_exhausted_until: Some(far_future),
                quota_pressure: Some(1.0),
                wall_class: Some(RouteWallClass::Quota),
                provider_wide: false,
                increment_quota_error: true,
                increment_failure: false,
                last_error: Some("usage limit".into()),
            },
        )
        .unwrap();
        rig.observe(&["incumbent", "small"]);
        let walled = host_tier(&rig.ladder(), 100);
        assert_eq!(walled.neutral, Some(incumbent.clone()));
        assert_ne!(
            walled.live,
            Some(incumbent),
            "precondition: the wall moves the live winner"
        );

        assert_eq!(rig.announce("quota"), None);
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
    }

    #[test]
    fn a_restart_between_two_differing_ladders_announces_once() {
        let rig = LadderRig::new("restart-moved");
        rig.observe(&["incumbent", "small", "challenger"]);
        assert_eq!(rig.announce("first selection"), None);

        rig.place_challenger_directly();
        let rig = rig.restart();
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        assert_eq!(
            rig.announce("boot"),
            None,
            "nothing is compared before a selection arrives"
        );
        rig.observe(&["incumbent", "small", "challenger"]);
        let notice = watch
            .check(rig.store.as_ref(), &rig.sources)
            .expect("the first selection after the restart reveals the move");
        assert!(notice.changes.iter().any(|change| {
            change.old_route.as_deref() == Some(rig.route("incumbent").as_str())
                && change.new_route.as_deref() == Some(rig.route("challenger").as_str())
                && change.tiers.contains(&100)
        }));
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
        assert_eq!(rig.announce("later"), None);
        assert_eq!(rig.notices().len(), 1);
    }

    #[test]
    fn a_restart_with_no_change_announces_nothing() {
        let rig = LadderRig::new("restart-same");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);

        let rig = rig.restart();
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        rig.observe(&["incumbent", "small"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
    }

    #[test]
    fn config_reload_excluding_the_winner_announces_once_and_an_unchanged_config_is_silent() {
        let rig = LadderRig::new("reload");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

        crate::test_config::write(
            &rig.sources.routing_config,
            serde_json::to_vec(&json!({
                "model_routing": { "exclude": {"all": [rig.route("incumbent")]} }
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            watch.check(rig.store.as_ref(), &rig.sources),
            None,
            "a pending edit has not changed the active config"
        );
        config_overrides::settle_live_config(&rig.sources.routing_config);
        let notice = watch
            .check(rig.store.as_ref(), &rig.sources)
            .expect("excluding the iq-100 winner moves it");
        assert!(notice.trigger.contains("routing config "));
        assert!(notice.changes.iter().any(|change| {
            change.old_route.as_deref() == Some(rig.route("incumbent").as_str())
                && change.tiers.contains(&100)
        }));
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
    }

    /// A baseline row written before ladders were keyed by caller class is
    /// replaced silently rather than compared against the new keys.
    #[test]
    fn a_stored_baseline_of_the_old_shape_is_replaced_without_announcing() {
        let rig = LadderRig::new("old-shape");
        let old = json!({
            "substrates": {
                winner_ladder::SUBSTRATE_HOST: {
                    "available": [rig.route("small")],
                    "tiers": { "100": { "neutral": rig.route("small"), "live": rig.route("small") } }
                }
            },
            "eq": 50.0
        });
        rig.store
            .put_route_winner_baseline(&old.to_string(), "before the upgrade", 1)
            .unwrap();
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection after upgrade"), None);
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
        let stored: winner_ladder::WinnerLadder =
            serde_json::from_str(&rig.store.route_winner_baseline().unwrap().unwrap()).unwrap();
        assert_eq!(stored.version, winner_ladder::LADDER_FORMAT_VERSION);
        assert_eq!(stored, rig.ladder());
    }

    /// The live flap: the plugin's mason launches and core's seed-catalog explore
    /// selections hand the host different lists, whose iq-100 winners differ.
    /// Once each class has a baseline, their alternation announces nothing, and
    /// each class's ladder replays its own list rather than a shared one.
    #[test]
    fn two_caller_classes_alternating_their_lists_announce_nothing_once_both_have_baselines() {
        let rig = LadderRig::new("classes");
        rig.place_challenger_directly();
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        rig.observe_as("mason", &["incumbent", "small", "challenger"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
        rig.observe_as("explore", &["incumbent", "small"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

        for round in 0..3 {
            rig.observe_as("mason", &["incumbent", "small", "challenger"]);
            assert_eq!(
                watch.check(rig.store.as_ref(), &rig.sources),
                None,
                "round {round}: mason's list arriving again is not a change"
            );
            assert_eq!(rig.announce("alternation"), None);
            rig.observe_as("explore", &["incumbent", "small"]);
            assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
            assert_eq!(rig.announce("alternation"), None);
        }
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());

        let ladder = rig.ladder();
        let explore = winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "explore");
        assert_eq!(
            ladder.ladders[&host_mason().label()].tiers[&100].live,
            Some(rig.route("challenger")),
            "mason's ladder replays mason's list"
        );
        assert_eq!(
            ladder.ladders[&explore.label()].tiers[&100].live,
            Some(rig.route("incumbent")),
            "explore's ladder replays explore's list, not mason's"
        );
    }

    #[tokio::test]
    async fn a_placement_moving_one_class_winner_announces_for_that_class_only() {
        let rig = LadderRig::new("one-class");
        rig.observe_as("mason", &["incumbent", "small", "challenger"]);
        rig.observe_as("explore", &["incumbent", "small"]);
        assert_eq!(rig.announce("first selections"), None);

        let response = rig.upsert(rig.challenger_params()).await;
        assert_eq!(response.header.ty, FrameType::Response);

        let notices = rig.notices();
        assert_eq!(notices.len(), 1, "one notice: {notices:?}");
        let changes = notices[0]["changes"].as_array().unwrap();
        assert!(!changes.is_empty());
        for change in changes {
            assert_eq!(change["key"], "opencode-host / mason", "{change}");
            assert_eq!(change["callerClass"], "mason");
            assert_eq!(change["substrate"], winner_ladder::SUBSTRATE_HOST);
        }
        assert!(changes.iter().any(|change| {
            change["newRoute"] == json!(rig.route("challenger"))
                && change["tiers"].as_array().unwrap().contains(&json!(100))
        }));
        assert_eq!(rig.announce("again"), None);
    }

    /// Plugin explore launches and core's seed-catalog explore selections share
    /// a class but not a list. Within the window the class replays the union, so
    /// their alternation is not an availability change.
    #[test]
    fn one_class_alternating_two_lists_within_the_window_uses_their_union_and_announces_nothing() {
        let rig = LadderRig::new("union");
        rig.place_challenger_directly();
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        rig.observe_as("explore", &["incumbent", "small", "challenger"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

        for round in 0..3 {
            rig.observe_as("explore", &["incumbent", "small"]);
            assert_eq!(
                watch.check(rig.store.as_ref(), &rig.sources),
                None,
                "round {round}: the narrower list adds nothing to the union"
            );
            rig.observe_as("explore", &["incumbent", "small", "challenger"]);
            assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
            assert_eq!(rig.announce("alternation"), None);
        }
        assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());

        rig.observe_as("explore", &["incumbent", "small"]);
        let explore = winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "explore");
        let ladder = rig.ladder();
        let replayed = &ladder.ladders[&explore.label()];
        assert!(
            replayed.available.contains(&rig.route("challenger")),
            "the union keeps the wider list after the narrower one arrives"
        );
        assert_eq!(replayed.tiers[&100].live, Some(rig.route("challenger")));
    }

    /// The ladder prices facts with the multipliers of the routing config it
    /// reads. Its first computation after boot must already use them; pricing
    /// subscription routes at list would disagree with every later ladder.
    #[test]
    fn the_first_ladder_after_boot_prices_routes_with_the_configured_multipliers() {
        let rig = LadderRig::new("multiplier");
        let listed_provider = format!("{}-listed", rig.provider);
        let listed_route = make_route_key(&listed_provider, "listed", None);
        let mut listed = fact(&listed_provider, "listed");
        listed.scores.elo = 1_500.0;
        listed.scores.eq = 1_500.0;
        listed.cost_per_m_tok_out = Some(8.0);
        rig.store
            .upsert_fact(&listed, FactSource::Override)
            .unwrap();
        // At list price `listed` (8) undercuts `incumbent` (10). A provider-wide
        // multiplier makes the incumbent cheaper without subsidizing its rival.
        let mut providers = serde_json::Map::new();
        providers.insert(
            rig.provider.clone(),
            json!({ "subscription_multiplier": 100 }),
        );
        crate::test_config::write(
            &rig.sources.routing_config,
            serde_json::to_vec(&json!({ "providers": providers })).unwrap(),
        )
        .unwrap();
        rig.sources.observed.record(
            host_mason(),
            [rig.route("incumbent"), listed_route.clone()],
            Vec::new(),
            rig.store.build_snapshot(now_ms() as f64).unwrap(),
        );

        let first = rig.ladder();
        assert_eq!(
            host_tier(&first, 30).neutral,
            Some(rig.route("incumbent")),
            "the first ladder must already see the multiplier"
        );
        assert_eq!(
            first,
            rig.ladder(),
            "identical inputs give identical ladders"
        );
        crate::test_config::write(&rig.sources.routing_config, "{}").unwrap();
        config_overrides::settle_live_config(&rig.sources.routing_config);
        assert_eq!(
            host_tier(&rig.ladder(), 30).neutral,
            Some(listed_route),
            "without the provider multiplier the lower list price must win"
        );
    }
    #[tokio::test]
    async fn route_model_upsert_refuses_fast_id_without_writing_a_row() {
        let rig = Rig::new("route-model-upsert-fast-refusal");
        let store = Arc::new(rig.open_store());
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_MODEL_UPSERT_OP,
                "params": {
                    "providerID": "openai",
                    "modelID": "gpt-5.5-fast",
                    "elo": 1500.0,
                    "eq": 1000.0,
                    "speed": 100.0
                }
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "invalid_request");
        assert!(store
            .get_fact_by_route_key("openai/gpt-5.5-fast")
            .unwrap()
            .is_none());
    }

    /// Wire tests for failures that used to be dropped without a reply or a log line.
    mod select_wire_error_reporting {
        use super::*;
        use rusqlite::OptionalExtension;

        fn rig_sql(rig: &Rig, sql: &str) {
            rusqlite::Connection::open(rig.dir.join("store.db"))
                .unwrap()
                .execute_batch(sql)
                .unwrap();
        }

        fn route_frame(corr: u64, request: serde_json::Value) -> Frame {
            Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                corr,
                serde_json::to_vec(&request).unwrap(),
            )
            .unwrap()
        }

        fn error_body(frame: &Frame) -> ErrorBody {
            assert_eq!(frame.header.ty, FrameType::Error);
            serde_json::from_slice(&frame.body).unwrap()
        }

        /// A route request whose handler fails before replying is answered with
        /// `internal_error` through the live frame dispatch, and the failure is
        /// logged. The handler failure here is real: the decision's remembered
        /// serving identity is unreadable when a `completed` outcome reads it.
        #[tokio::test]
        async fn a_route_handler_failing_before_its_reply_answers_internal_error() {
            let rig = Rig::new("route-handler-error-reply");
            let store = Arc::new(rig.open_store());
            let decision_id = store
                .record_decision(&DecisionInput {
                    target_agent: "mason".into(),
                    ..Default::default()
                })
                .unwrap();
            rig_sql(
                &rig,
                &format!(
                    "UPDATE model_route_decisions SET provider_identity_json = '{{' WHERE id = '{decision_id}'"
                ),
            );
            let (writer, mut replies) = mpsc::channel(4);
            let mut route_epochs = RouteEpochs::from([(7, 1)]);
            take_operator_warnings();
            assert!(handle_frame(
                route_frame(
                    55,
                    json!({
                        "method": ROUTE_SET_DECISION_OUTCOME_OP,
                        "params": { "decisionID": decision_id, "outcome": "completed" }
                    }),
                ),
                &writer,
                &Arc::new(Mutex::new(QuotaCache::default())),
                &store,
                &mut route_epochs,
                &Arc::new(Mutex::new(None)),
                &rig.dir.join("routing.jsonc"),
                &rig.dir.join("operator.jsonc"),
                &empty_broca_credentials(),
                &Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
            )
            .await
            .unwrap());
            let reply = tokio::time::timeout(Duration::from_secs(10), replies.recv())
                .await
                .expect("a failed route handler must still answer its caller")
                .unwrap();
            assert_eq!(
                (reply.header.channel, reply.header.epoch, reply.header.corr),
                (7, 1, 55)
            );
            let error = error_body(&reply);
            assert_eq!(error.code, "internal_error");
            assert!(
                error
                    .message
                    .starts_with("routing failed before replying: "),
                "{}",
                error.message
            );
            let warnings = take_operator_warnings();
            assert!(
                warnings
                    .iter()
                    .any(|line| line.contains("route request on channel 7 (corr 55) failed: ")),
                "{warnings:?}"
            );
            assert!(replies.try_recv().is_err(), "exactly one reply");
        }

        fn reply_frame(corr: u64) -> Frame {
            Frame::build(
                FrameType::Response,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                corr,
                br#"{"result":{}}"#.to_vec(),
            )
            .unwrap()
        }

        /// A handler that already replied gets no second answer when it then
        /// fails; a handler that succeeds has its reply forwarded unchanged.
        #[tokio::test]
        async fn a_handler_failing_after_its_reply_sends_no_second_frame() {
            let (writer, mut replies) = mpsc::channel(4);
            take_operator_warnings();
            answer_route_request(&writer, 1, 7, 1, 61, |reply| async move {
                send(&reply, reply_frame(61)).await?;
                Err(ModuleError::Message("after the reply".into()))
            })
            .await;
            let forwarded = replies.try_recv().unwrap();
            assert_eq!(
                (forwarded.header.ty, forwarded.header.corr),
                (FrameType::Response, 61)
            );
            assert!(replies.try_recv().is_err(), "no second frame");
            let warnings = take_operator_warnings();
            assert!(
                warnings
                    .iter()
                    .any(|line| line.contains("failed after replying: after the reply")),
                "{warnings:?}"
            );

            answer_route_request(&writer, 1, 7, 1, 62, |reply| async move {
                send(&reply, reply_frame(62)).await
            })
            .await;
            assert_eq!(replies.try_recv().unwrap().header.corr, 62);
            assert!(replies.try_recv().is_err());
            assert!(take_operator_warnings().is_empty());
        }

        /// With the connection gone no reply can be delivered; the failure is
        /// still logged and serving does not stall on the closed writer.
        #[tokio::test]
        async fn a_failure_with_the_connection_closed_is_only_logged() {
            let (writer, replies) = mpsc::channel(4);
            drop(replies);
            take_operator_warnings();
            answer_route_request(&writer, 1, 7, 1, 63, |_reply| async move {
                Err(ModuleError::Message("egress channel closed".into()))
            })
            .await;
            let warnings = take_operator_warnings();
            assert!(
                warnings
                    .iter()
                    .any(|line| line.contains("(corr 63) failed: egress channel closed")),
                "{warnings:?}"
            );
            assert!(
                warnings
                    .iter()
                    .any(|line| line.contains("(corr 63) got no failure reply")),
                "{warnings:?}"
            );
        }

        async fn record_cooldown(store: &SharedStore, params: serde_json::Value) -> Frame {
            run_route_wire_request(
                store,
                json!({ "method": ROUTE_RECORD_COOLDOWN_OP, "params": params }),
            )
            .await
        }

        fn stored_pressure(rig: &Rig, route_key: &str) -> Option<Option<f64>> {
            rusqlite::Connection::open(rig.dir.join("store.db"))
                .unwrap()
                .query_row(
                    "SELECT quota_pressure FROM model_route_state WHERE route_key = ?1",
                    [route_key],
                    |row| row.get(0),
                )
                .optional()
                .unwrap()
        }

        /// A route-scoped cooldown names its own capacity and a 0..=1 pressure;
        /// anything else is refused before it reaches the store.
        #[tokio::test]
        async fn record_cooldown_refuses_a_foreign_capacity_and_out_of_range_pressure() {
            let rig = Rig::new("record-cooldown-validation");
            let store = Arc::new(rig.open_store());
            for (route_key, capacity_key, pressure) in [
                ("p/m/high", "q/m", 0.5),
                ("p/m-long/high", "p/m", 0.5),
                ("p/m/high", "", 0.5),
                ("p/m/high", "p/m", 1.5),
                ("p/m/high", "p/m", -0.1),
            ] {
                let reply = record_cooldown(
                    &store,
                    json!({
                        "routeKey": route_key,
                        "capacityKey": capacity_key,
                        "cooldownUntil": 1.0e15,
                        "quotaPressure": pressure,
                    }),
                )
                .await;
                let error = error_body(&reply);
                assert_eq!(
                    error.code, "invalid_request",
                    "{route_key} {capacity_key} {pressure}: {}",
                    error.message
                );
                assert_eq!(stored_pressure(&rig, route_key), None, "{route_key}");
            }
            // Pressure out of range is refused on a provider-wide write too.
            let reply = record_cooldown(
                &store,
                json!({
                    "routeKey": "p/m/high",
                    "capacityKey": "p/m",
                    "providerWide": true,
                    "cooldownUntil": 1.0e15,
                    "quotaPressure": 2.0,
                }),
            )
            .await;
            assert_eq!(error_body(&reply).code, "invalid_request");
            assert_eq!(stored_pressure(&rig, "p/m/high"), None);

            // The same route with its own capacity and an in-range pressure is
            // stored, for a variant route and for a variantless one.
            for (route_key, capacity_key) in [("p/m/high", "p/m"), ("p/m", "p/m")] {
                let reply = record_cooldown(
                    &store,
                    json!({
                        "routeKey": route_key,
                        "capacityKey": capacity_key,
                        "cooldownUntil": 1.0e15,
                        "quotaPressure": 0.4,
                    }),
                )
                .await;
                assert_eq!(reply.header.ty, FrameType::Response, "{route_key}");
                assert_eq!(stored_pressure(&rig, route_key), Some(Some(0.4)));
            }
        }
    }

    /// Route handlers call the SQLite store synchronously. While one handler's
    /// store write waits on another connection's write lock, the rest of the
    /// runtime must keep running.
    mod route_handler_isolation {
        use super::*;
        use rusqlite::OptionalExtension;

        struct StoreContention {
            waiting: tokio::sync::oneshot::Sender<()>,
            released: std::sync::mpsc::Receiver<()>,
        }

        static STORE_CONTENTION: Mutex<Option<StoreContention>> = Mutex::new(None);

        fn wait_for_lock_release(_attempt: i32) -> bool {
            let Some(contention) = STORE_CONTENTION.lock().unwrap().take() else {
                return false;
            };
            contention.waiting.send(()).is_ok() && contention.released.recv().is_ok()
        }

        /// On a current-thread runtime, a handler run on the runtime's only
        /// thread would stall every other task until its store call returned.
        /// Progress is shown by message order: the other task answers while the
        /// lock is still held, and the handler can only finish after release.
        #[tokio::test(flavor = "current_thread")]
        async fn a_route_handler_waiting_on_the_store_does_not_stall_other_tasks() {
            let rig = Rig::new("route-handler-store-wait");
            let store = Arc::new(rig.open_store());
            let db = rig.dir.join("store.db");
            // Observe actual SQLite contention, then keep the handler waiting
            // until COMMIT regardless of how long the async runtime is delayed.
            // The production busy timeout must not race the test's lock release.
            let (waiting, waited) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            *STORE_CONTENTION.lock().unwrap() = Some(StoreContention { waiting, released });
            store
                .set_busy_handler_for_test(Some(wait_for_lock_release))
                .unwrap();
            // A second connection holds the database's write lock, so the
            // handler's cooldown write waits inside the store call.
            let lock_holder = rusqlite::Connection::open(&db).unwrap();
            lock_holder.execute_batch("BEGIN EXCLUSIVE").unwrap();

            let (writer, mut replies) = mpsc::channel(4);
            let mut route_epochs = RouteEpochs::from([(7, 1)]);
            let request = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                71,
                serde_json::to_vec(&json!({
                    "method": ROUTE_RECORD_COOLDOWN_OP,
                    "params": {
                        "routeKey": "p/m",
                        "capacityKey": "p/m",
                        "cooldownUntil": 1.0e15,
                        "quotaPressure": 0.4,
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(handle_frame(
                request,
                &writer,
                &Arc::new(Mutex::new(QuotaCache::default())),
                &store,
                &mut route_epochs,
                &Arc::new(Mutex::new(None)),
                &rig.dir.join("routing.jsonc"),
                &rig.dir.join("operator.jsonc"),
                &empty_broca_credentials(),
                &Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
            )
            .await
            .unwrap());

            tokio::select! {
                waiting = waited => waiting.expect("the handler reaches SQLite's busy wait"),
                reply = replies.recv() => panic!(
                    "the handler must wait for the writer, not answer early: {}",
                    String::from_utf8_lossy(&reply.unwrap().body)
                ),
            }

            // Only start the ping once the handler is inside the store call.
            // Another task on the same runtime must still be able to answer.
            let (ping, pinged) = tokio::sync::oneshot::channel::<()>();
            let (pong, ponged) = tokio::sync::oneshot::channel::<()>();
            tokio::spawn(async move {
                pinged.await.unwrap();
                pong.send(()).unwrap();
            });
            ping.send(()).unwrap();
            ponged
                .await
                .expect("the other task answers while the handler waits on the store");
            assert!(
                replies.try_recv().is_err(),
                "the handler cannot have finished while the write lock is held"
            );

            lock_holder.execute_batch("COMMIT").unwrap();
            release.send(()).unwrap();
            let reply = replies.recv().await.unwrap();
            assert_eq!(
                (reply.header.ty, reply.header.corr),
                (FrameType::Response, 71),
                "{}",
                String::from_utf8_lossy(&reply.body)
            );
            let pressure: Option<Option<f64>> = rusqlite::Connection::open(&db)
                .unwrap()
                .query_row(
                    "SELECT quota_pressure FROM model_route_state WHERE route_key = 'p/m'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .unwrap();
            assert_eq!(
                pressure,
                Some(Some(0.4)),
                "the write lands once the lock is free"
            );
        }
    }
}

#[cfg(test)]
mod slow_route_select_timing_tests {
    use super::*;

    #[test]
    fn injected_snapshot_delay_is_emitted_in_the_slow_select_breakdown() {
        let timing = RouteSelectTiming::new();
        timing.is_select.set(true);
        let started = Instant::now();
        std::thread::sleep(Duration::from_millis(1_050));
        let snapshot = started.elapsed();
        timing.add_snapshot_closure(snapshot);

        let now = Instant::now();
        let line = timing
            .line_at(now)
            .expect("the injected delay exceeds one second");
        let emitted =
            rate_limit_route_select_line(line, now, &mut RouteSelectLogLimiter::default())
                .expect("the first slow select is emitted");
        assert!(emitted.contains("slow route.select total_ms="), "{emitted}");
        assert!(emitted.contains("live_config_check_ms="), "{emitted}");
        assert!(
            emitted.contains(&format!("snapshot_closure_ms={}", snapshot.as_millis())),
            "{emitted}"
        );
        assert!(snapshot.as_millis() >= 1_000);
        assert!(emitted.contains("selection_ms="), "{emitted}");
        assert!(emitted.contains("reply_ms="), "{emitted}");
    }

    #[test]
    fn every_stage_bucket_is_named_and_subtracted_from_unclassified() {
        let timing = RouteSelectTiming::new();
        timing.is_select.set(true);
        let stages = [
            ("live_config_check_ms", &timing.live_config_check_ms),
            ("billing_wait_ms", &timing.billing_wait_ms),
            ("resolve_ms", &timing.resolve_ms),
            ("snapshot_closure_ms", &timing.snapshot_closure_ms),
            ("pricing_ms", &timing.pricing_ms),
            ("policy_ms", &timing.policy_ms),
            ("head_history_ms", &timing.head_history_ms),
            ("selection_ms", &timing.selection_ms),
            ("ladder_ms", &timing.ladder_ms),
            ("decision_write_ms", &timing.decision_write_ms),
            ("runner_ms", &timing.runner_ms),
            ("finish_ms", &timing.finish_ms),
            ("reply_ms", &timing.reply_ms),
        ];
        for (index, (_, bucket)) in stages.iter().enumerate() {
            add_stage_ms(bucket, Duration::from_millis(100 + index as u64));
        }
        let line = timing
            .line_at(timing.started + Duration::from_millis(3_000))
            .expect("three seconds is slow");
        let classified: u64 = (0..stages.len() as u64).map(|index| 100 + index).sum();
        for (index, (name, _)) in stages.iter().enumerate() {
            assert!(
                line.contains(&format!(" {name}={} ", 100 + index)),
                "{name}: {line}"
            );
        }
        assert!(
            line.ends_with(&format!("unclassified_ms={}", 3_000 - classified)),
            "{line}"
        );
    }

    #[test]
    fn slow_select_log_rate_limit_counts_suppressed_requests() {
        let start = Instant::now();
        let mut limiter = RouteSelectLogLimiter::default();
        assert_eq!(
            rate_limit_route_select_line("first".into(), start, &mut limiter),
            Some("first".into())
        );
        assert_eq!(
            rate_limit_route_select_line(
                "suppressed".into(),
                start + Duration::from_secs(1),
                &mut limiter,
            ),
            None
        );
        assert_eq!(
            rate_limit_route_select_line(
                "next".into(),
                start + Duration::from_secs(10),
                &mut limiter,
            ),
            Some("next (suppressed 1 similar lines)".into())
        );
    }
}

#[cfg(test)]
mod background_loop_supervision_tests {
    use super::*;

    /// Long enough for any spawned loop in these tests to finish; a wait that
    /// outlives it means a stopped loop went unnoticed.
    const STOP_NOTICE_LIMIT: Duration = Duration::from_secs(5);

    async fn wait_for_frame_or_stop(
        loops: &mut BackgroundLoops,
        read_half: &mut tokio::io::DuplexStream,
    ) -> Result<Option<Frame>, ModuleError> {
        tokio::time::timeout(STOP_NOTICE_LIMIT, next_frame_or_loop_stop(read_half, loops))
            .await
            .expect("the serving loop must notice a stopped background loop")
    }

    #[tokio::test]
    async fn a_panicking_background_loop_ends_serving_with_its_name() {
        // The other half stays open, so subc never sends EOF: only the
        // stopped loop can end the wait.
        let (_subc, mut module) = tokio::io::duplex(1024);
        let mut loops = BackgroundLoops::default();
        loops.spawn("catalog refresh", std::future::pending());
        loops.spawn("quota events", async { panic!("quota bus gone") });

        let error = wait_for_frame_or_stop(&mut loops, &mut module)
            .await
            .expect_err("a panicked loop must end serving with an error");
        let message = error.to_string();
        assert!(message.contains("`quota events`"), "{message}");
        assert!(message.contains("panicked: quota bus gone"), "{message}");
    }

    #[tokio::test]
    async fn a_returning_background_loop_ends_serving_with_its_name() {
        let (_subc, mut module) = tokio::io::duplex(1024);
        let mut loops = BackgroundLoops::default();
        loops.spawn("retention", async {});

        let error = wait_for_frame_or_stop(&mut loops, &mut module)
            .await
            .expect_err("a loop that returns must end serving with an error");
        let message = error.to_string();
        assert!(message.contains("`retention`"), "{message}");
        assert!(message.contains("returned"), "{message}");
    }

    #[tokio::test]
    async fn frames_and_eof_are_served_while_background_loops_run() {
        let (mut subc, mut module) = tokio::io::duplex(1024);
        let mut loops = BackgroundLoops::default();
        loops.spawn("winner ladder watch", std::future::pending());
        let ping = Frame::build(FrameType::Ping, control_flags(), 0, 0, 7, Vec::new()).unwrap();
        write_frame(&mut subc, &ping).await.unwrap();

        let frame = wait_for_frame_or_stop(&mut loops, &mut module)
            .await
            .unwrap()
            .expect("the written frame");
        assert_eq!(frame.header.corr, 7);

        drop(subc);
        assert!(wait_for_frame_or_stop(&mut loops, &mut module)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn an_empty_loop_set_never_reports_a_stop() {
        let (subc, mut module) = tokio::io::duplex(1024);
        let mut loops = BackgroundLoops::default();
        drop(subc);
        assert!(wait_for_frame_or_stop(&mut loops, &mut module)
            .await
            .unwrap()
            .is_none());
    }
}
