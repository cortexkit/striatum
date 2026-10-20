#![forbid(unsafe_code)]

//! The prefrontal-routing subc module.
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
//! module runs the pure `prefrontal-routing-core` scorer and returns the selection.

mod account_quota;
mod account_selection;
mod available_models_text;
mod broca_credentials;
mod config_overrides;
mod config_policy;
mod engram_catalog;
mod fusiform_catalog;
mod key_pool;
mod model_resolution;
mod panel_policy;
mod quota_bus;
mod quota_consumer;
mod quota_events;
mod retention;
mod runtime_lag;
mod select_policy;
mod selection_harness;
mod subscription_economics;
mod substrate_policy;
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

use broca_credentials::{BrocaCredentials, SharedBrocaCredentials};
use cortexkit_store_types::StorageDescriptor;
use fusiform_catalog::{CatalogReachHealth, SharedCatalogHealth};
use prefrontal_routing_core::model_facts::make_capacity_key;
use prefrontal_routing_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use prefrontal_routing_core::model_router::{
    select_model_route_with_availability, ModelRouteStateSnapshot, NoRouteError,
    RequiredCapabilities, RouteFailureReason, SelectModelRouteInput, SelectedModelRoute,
};
use prefrontal_routing_core::quota::quota_poll::record_reading_log_poll;
use prefrontal_routing_core::quota::{merge_quota_overlay, ProviderUsage};
use prefrontal_routing_core::route_state_store::FactSource;
use prefrontal_routing_core::routing_config::ANCHOR_VERSION;
use prefrontal_routing_core::slot_saturation::{ConcurrencySlotState, ConcurrencySlots};
use prefrontal_routing_core::{
    derive_provider_key, fact_to_route, live_billing_wall_error, make_route_key,
    relax_earliest_reset, resolve_launch_requirements, resolve_routing_policy,
    AvailabilitySubstrate, BreakGlassCandidate, CatalogFactField, DecisionInput,
    DecisionOutcomeUpdate, ExclusionPolicy, ExclusionSpecs, ModelFact, ModelRequirements,
    ModelRoute, ModelRoutingPolicyInput, PanelSelection, RequestedModelRequirements,
    ResolvedRoutingPolicy, RouteOutcome, RouteStateStore, RouteWallClass, SelectPanelInput,
    SelectedPanel,
};
#[cfg(test)]
use prefrontal_routing_core::{routing_config::DEFAULT_ROUTING_CONFIG, FailureBackoffConfig};
use prefrontal_routing_store::{dev_descriptor, DecisionListFilter, SqliteRouteStateStore};
use quota_consumer::QuotaConsumer;
use serde::{Deserialize, Serialize};
use serde_json::json;
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

const DEFAULT_MODULE_ID: &str = "prefrontal-routing";
const ROUTE_SELECT_OP: &str = "route.select";
const ROUTE_RESOLVE_POLICY_OP: &str = "route.resolve_policy";
const ROUTE_SELECT_PANEL_OP: &str = "route.select_panel";
/// Render one retained routing decision without requiring local SQLite access.
const ROUTE_EXPLAIN_OP: &str = "route.explain";
/// List recent routing decisions using only compact diagnostic fields.
const ROUTE_DECISIONS_LIST_OP: &str = "route.decisions_list";
/// Read one model capability fact from the live route-state catalog.
const ROUTE_MODEL_FACT_OP: &str = "route.model_fact";
/// Read the complete durable model catalog for display and observation consumers.
const ROUTE_MODEL_FACTS_OP: &str = "route.model_facts";
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
/// they issued to the same identity; no other operation reads it. It is a
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

fn clear_route_epoch(route_epochs: &mut RouteEpochs, channel: u16, epoch: u32) {
    if route_epochs.get(&channel) == Some(&epoch) {
        route_epochs.remove(&channel);
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
                            "[prefrontal-routing] retaining last healthy quota window for {provider} after degraded refresh: {reason}"
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
                "[prefrontal-routing] quota refresh ignored unsupported spend funding values: {unsupported:?}"
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
                "[prefrontal-routing] provider {provider} walled on empty spendable pools while non-spendable pools hold {} - policy, not a quota bug (only granted/subscription funding may spend)",
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
    let facts = store.get_facts().map_err(|e| e.to_string())?;
    let mut caps_by_provider: HashMap<String, Vec<String>> = HashMap::new();
    for fact in &facts {
        caps_by_provider
            .entry(fact.provider_id.clone())
            .or_default()
            .push(make_capacity_key(&fact.provider_id, &fact.model_id));
    }
    let name_map = quota_name_map();
    let observed_at = i64::try_from(observed_at_ms).map_err(|e| e.to_string())?;
    let empty: Vec<String> = Vec::new();
    for entry in fresh {
        let provider_id = match entry.api_provider.as_deref() {
            Some(slug) if !slug.is_empty() => slug.to_string(),
            _ => match name_map.get(&entry.provider) {
                Some(mapped) => mapped.clone(),
                None => continue,
            },
        };
        let capacity_keys = caps_by_provider.get(&provider_id).unwrap_or(&empty);
        record_reading_log_poll(store, entry, &provider_id, capacity_keys, observed_at)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
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
            "[prefrontal-routing] quota refresh failed; selection keeps the snapshot from refreshedAtMs {} until a refresh succeeds or it goes stale: {error}",
            cache.refreshed_at_ms
        );
    }
    cache.last_error = Some(error);
}

fn record_cooldown_update(
    store: &impl RouteStateStore,
    params: &RecordCooldownParams,
) -> prefrontal_routing_core::RouteStateResult<()> {
    let update = prefrontal_routing_core::RouteStateUpdate {
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
                prefrontal_routing_core::RouteStateStoreError(
                    "provider-wide cooldown has no canonical provider key".into(),
                )
            })?;
        return store.record_provider_cooldown(
            provider_key,
            &params.route_key,
            &params.capacity_key,
            &update,
        );
    }
    store.update_route_state(&params.route_key, &params.capacity_key, &update)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

#[tokio::main]
async fn main() -> Result<(), ModuleError> {
    // Read the launch nonce before anything else can spawn a process: until
    // this first read, the pipe the daemon handed it over on is inheritable,
    // so a child spawned earlier would receive it. The result is cached for
    // the process; HELLO and the credential route read that cached answer.
    let _ = subc_os::launch_nonce();
    let connection_file_path = match parse_startup_mode(std::env::args_os().skip(1))? {
        StartupMode::PrintManifest => return print_manifest_operations(),
        StartupMode::CheckConfig => {
            let report = config_overrides::check_config_offline(
                &config_overrides::config_path(),
                config_overrides::default_store_path().as_deref(),
            )
            .map_err(ModuleError::Message)?;
            eprintln!("{report}");
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
        prefrontal_core_store::scratch_root().join("prefrontal-routing-account-quota-dry-run");
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
    if let Some(dir) = &capture_dir {
        std::fs::create_dir_all(dir)
            .and_then(|()| std::fs::write(dir.join("usage-get-all.json"), &body))
            .map_err(|error| ModuleError::Message(format!("capture usage: {error}")))?;
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
    runtime_lag::RuntimeLagProbe::default().start("routing runtime");
    let stream = connect_to_subc(&config.connection_file_path).await?;
    let (mut read_half, write_half) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
    let writer = tokio::spawn(drain_writer(write_half, rx));

    // Quota delivery runs off the selection hot path. Bus notices fetch bodies
    // over independent outbound insula connections; route.select only reads the
    // cache. A bus outage switches delivery to usage.get snapshots.
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

/// The long-lived background loops one serving connection starts: retention,
/// quota events, the account roster, config refresh, catalog refresh and the
/// winner-ladder watch. Each is written to run until the process exits, so one
/// that finishes, by panicking or by returning, has silently stopped a duty
/// route.select depends on (quota readings, catalog facts, config reloads).
/// The serving loop watches this set and turns the first stop into a module
/// error: the process exits non-zero, and the subc daemon's crash-restart
/// policy starts a fresh module that restores its caches from the store and
/// runs every loop again. Dropping the set aborts the remaining loops.
#[derive(Default)]
struct BackgroundLoops {
    tasks: tokio::task::JoinSet<()>,
    names: HashMap<tokio::task::Id, &'static str>,
}

impl BackgroundLoops {
    fn spawn<F>(&mut self, name: &'static str, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = self.tasks.spawn(task);
        self.names.insert(handle.id(), name);
    }

    /// Wait until one loop stops and describe which one and how. Stays pending
    /// while every loop runs, including when no loop was started.
    async fn next_stop(&mut self) -> ModuleError {
        let Some(joined) = self.tasks.join_next_with_id().await else {
            return std::future::pending().await;
        };
        let (id, how) = match joined {
            Ok((id, ())) => (id, "returned".to_string()),
            Err(error) if error.is_panic() => {
                let id = error.id();
                (
                    id,
                    format!("panicked: {}", panic_message(error.into_panic().as_ref())),
                )
            }
            Err(error) => (error.id(), "was cancelled".to_string()),
        };
        let name = self.names.remove(&id).unwrap_or("unnamed");
        ModuleError::Message(format!(
            "routing background loop `{name}` stopped unexpectedly ({how}); \
             exiting so the daemon restarts the module"
        ))
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Read the next frame from subc unless a background loop stops first. A stop
/// is printed to stderr, so the module's output names the loop, and returned
/// as the error that ends the process.
async fn next_frame_or_loop_stop<R>(
    read_half: &mut R,
    loops: &mut BackgroundLoops,
) -> Result<Option<Frame>, ModuleError>
where
    R: AsyncRead + Unpin,
{
    tokio::select! {
        frame = read_frame(read_half) => frame.map_err(|e| ModuleError::Message(e.to_string())),
        stopped = loops.next_stop() => {
            eprintln!("[prefrontal-routing] background-loop-stopped: {stopped}");
            Err(stopped)
        }
    }
}

async fn module_loop<R>(
    read_half: &mut R,
    writer: mpsc::Sender<Frame>,
    config: &ModuleConfig,
    quota_cache: SharedQuotaCache,
    catalog_health: SharedCatalogHealth,
) -> Result<(), ModuleError>
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
    // Old audit, usage and account-selection rows are deleted in small batches
    // on their own timer. Each retention pass re-reads how long every table keeps its
    // rows from the routing file's `retention` block.
    let mut loops = BackgroundLoops::default();
    loops.spawn(
        "retention",
        retention::retention_loop(Arc::clone(&store), config_overrides::config_path()),
    );
    // Restore per-account views before the shared quota delivery task starts.
    // The roster has a separate refresh loop; readings share the event consumer.
    let account_state: account_quota::SharedAccountQuota = Arc::new(Mutex::new(
        account_quota::restore_account_state(store.as_ref()),
    ));
    loops.spawn(
        "quota events",
        quota_events::run(
            config.connection_file_path.clone(),
            Arc::clone(&quota_cache),
            Arc::clone(&account_state),
            Arc::clone(&store),
        ),
    );
    let broca_credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
    broca_credentials.lock().unwrap().connection_file_path =
        Some(config.connection_file_path.clone());
    // Restored login identities identify their serving plane before the first
    // catalog poll; API-only providers wait for the non-secret listing poll.
    if let (Ok(accounts), Ok(facts), Ok(mut credentials)) = (
        account_state.lock(),
        store.get_facts(),
        broca_credentials.lock(),
    ) {
        credentials.set_score_alias(
            config_overrides::read_operator_routing_policy(&config_overrides::config_path())
                .score_alias,
        );
        credentials.refresh_serving_methods(&accounts, &facts, now_ms() as i64);
    }
    if ACCOUNT_QUOTA.set(Arc::clone(&account_state)).is_ok() {
        let roster_resolver: Arc<dyn prefrontal_ckcred_client::CredentialResolver> =
            Arc::new(prefrontal_ckcred_client::CkcredCredentialResolver::new(
                config.connection_file_path.clone(),
                std::env::current_dir().map_err(|error| {
                    ModuleError::Message(format!("resolve routing roster project root: {error}"))
                })?,
                DEFAULT_MODULE_ID,
            ));
        loops.spawn(
            "account roster",
            account_quota::account_quota_loop(
                config.connection_file_path.clone(),
                account_state,
                Arc::clone(&store),
                roster_resolver,
                Arc::clone(&broca_credentials),
            ),
        );
    }
    // Every placement and config reload compares the winner ladder over the same
    // config a selection reads (the operator policy lives in the routing file)
    // and the availability live selections report.
    let ladder_sources = winner_ladder::LadderSources::live(
        &config_overrides::config_path(),
        &config_overrides::config_path(),
    );

    // Load a validated config snapshot before serving. The watcher publishes
    // replacements; model overrides remain request overlays so withdrawing one
    // exposes the stored fact without rewriting operator score placements.
    let override_path = config_overrides::config_path();
    // Selection policy and model-fact overrides share the routing-owned file.
    let operator_config_path = override_path.clone();
    // OpenCode owns this file; routing reads only each provider's auth kind.
    let opencode_auth_path = config_overrides::auth_json_path();
    config_overrides::live_config(&override_path);
    loops.spawn(
        "config refresh",
        config_overrides::config_refresh_loop(
            override_path.clone(),
            Arc::clone(&broca_credentials.lock().unwrap().catalog_wake),
        ),
    );

    // Seed catalog health with the active polling cadence; reloads wake the
    // catalog loop, which updates both its cadence and the health threshold.
    let catalog_poll_ms = config_overrides::live_config(&override_path).catalog_poll_ms();
    if let Ok(mut health) = catalog_health.lock() {
        health.set_poll_ms(catalog_poll_ms);
    }
    loops.spawn(
        "catalog refresh",
        fusiform_catalog::catalog_refresh_loop(
            config.connection_file_path.clone(),
            Arc::clone(&store),
            Arc::clone(&catalog_health),
            catalog_poll_ms,
            ladder_sources.clone(),
            Arc::clone(&broca_credentials),
        ),
    );
    // Compares a ladder when a config file the ladder reads changed or live
    // selection reported a different availability set (including the first one
    // after boot, which is compared with the ladder stored before the restart).
    loops.spawn(
        "winner ladder watch",
        winner_ladder::config_reload_watch_loop(Arc::clone(&store), ladder_sources),
    );

    let min_band: SharedMinBand = Arc::new(std::sync::Mutex::new(
        config_overrides::live_config(&override_path).min_band(),
    ));

    let mut route_epochs = RouteEpochs::new();
    loop {
        let Some(frame) = next_frame_or_loop_stop(read_half, &mut loops).await? else {
            return Ok(()); // clean EOF: subc closed the connection.
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
            &opencode_auth_path,
            &catalog_health,
        )
        .await?
        {
            return Ok(());
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
    opencode_auth_path: &Path,
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
            // never head-of-line-blocks another route.
            let writer = writer.clone();
            let quota_cache = Arc::clone(quota_cache);
            let store = Arc::clone(store);
            let min_band = Arc::clone(min_band);
            let routing_config_path = routing_config_path.to_path_buf();
            let operator_config_path = operator_config_path.to_path_buf();
            let broca_credentials = Arc::clone(broca_credentials);
            let opencode_auth_path = opencode_auth_path.to_path_buf();
            tokio::spawn(async move {
                let _ = handle_select_request(
                    frame,
                    route_epoch,
                    &writer,
                    &quota_cache,
                    &store,
                    &min_band,
                    &routing_config_path,
                    &operator_config_path,
                    &broca_credentials,
                    &opencode_auth_path,
                )
                .await;
            });
            Ok(true)
        }
        _ => Ok(true),
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
            ..
        } => {
            if let Ok(mut map) = route_principals().lock() {
                map.insert(
                    route_channel,
                    (
                        epoch,
                        account_selection::principal_label(principal.as_ref()),
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

/// The full `route.select` body, decoded only after the method is confirmed.
#[derive(Debug, Deserialize)]
struct SelectBody {
    params: SelectParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SelectParams {
    #[serde(default)]
    consumer: Option<select_policy::Consumer>,
    #[serde(default)]
    pin: Option<select_policy::Pin>,
    #[serde(default, rename = "pin_fallback", alias = "pinFallback")]
    pin_fallback: select_policy::PinFallback,
    #[serde(default)]
    model_pin: Option<String>,
    #[serde(default = "default_target_agent")]
    target_agent: String,
    #[serde(default)]
    workload: Option<selection_harness::Workload>,
    #[serde(default)]
    executor: Option<selection_harness::Executor>,
    #[serde(default)]
    required_harness: Option<substrate_policy::Harness>,
    /// Existing module callers may provide an already-resolved demand vector.
    #[serde(default)]
    requirements: Option<ModelRequirements>,
    /// Launch callers provide raw optional requirements so the module applies
    /// clamping, specialist defaults, and Mason's required non-zero demand.
    #[serde(default)]
    requested_requirements: Option<RequestedModelRequirements>,
    /// Raw routeKeys/capacityKeys reported by the harness.
    #[serde(default)]
    available_models: Vec<String>,
    /// Per-attempt exclusions such as a route that just failed.
    #[serde(default)]
    exclude_route_keys: Vec<String>,
    #[serde(default)]
    required_capabilities: Option<RequiredCapabilities>,
    /// Retained for one release so older callers decode; routing ignores these
    /// caller-policy fields in favor of its routing-owned config.
    #[serde(default)]
    subscription_keys: Vec<String>,
    #[serde(default)]
    routing_policy: Option<ModelRoutingPolicyInput>,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
    /// Gather seats can request cache-capable providers without changing the
    /// normal quality and cost ordering for other route selections.
    #[serde(default)]
    prefer_prompt_cache: bool,
    /// Audit context for the recorded decision (route.select is now a mutate that
    /// records the decision server-side and returns its id).
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default, alias = "parentSessionID")]
    parent_session_id: Option<String>,
    /// For `executor: broca`, `substrate: broca` uses Broca's served routes;
    /// absent or other values use `availableModels`. Launch selections ignore
    /// this legacy field and use each route's mapped harness.
    #[serde(default)]
    substrate: Option<String>,
    /// The caller's harness, used to match the decision with recorded spend facts when provided.
    #[serde(default)]
    harness: Option<String>,
    /// The caller's project directory, used to match the decision with recorded spend facts.
    #[serde(default)]
    project_root: Option<String>,
    /// The caller may provide a launch attempt id before selection; otherwise it
    /// remains null because the router never invents a send correlation.
    #[serde(default, rename = "sendID")]
    send_id: Option<String>,
    /// The caller's concurrency slot counts at request time, keyed by
    /// `provider/model`. A candidate whose slot is full is passed over for this
    /// decision only; absent (older callers, or a caller whose launch is not
    /// limited by these slots) leaves the selection unchanged.
    #[serde(default)]
    concurrency_slots: Option<Vec<ConcurrencySlotState>>,
}

impl SelectParams {
    /// A selection that names no executor, no workload and no required harness
    /// comes from a host's own route resolver (an OpenCode or Pi head choosing
    /// the model for a mason or explore subagent it launches) or from core
    /// re-running such a selection after a failed attempt. Core still decides
    /// where those workers run, from its own substrate settings, so the host's
    /// model list (or Broca's, when the caller names substrate `broca`) stays
    /// authoritative and the route-to-harness mapping is not consulted. Only
    /// callers that name a workload, an executor or a required harness are
    /// judged by the mapped harness.
    fn is_host_run(&self) -> bool {
        self.executor.is_none() && self.workload.is_none() && self.required_harness.is_none()
    }

    fn resolved_workload(&self) -> Option<selection_harness::Workload> {
        self.workload.or_else(|| {
            if self.executor.is_none() {
                selection_harness::Workload::for_agent(&self.target_agent)
            } else {
                None
            }
        })
    }

    fn executor_workload_line(&self) -> Option<String> {
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

#[derive(Debug, Deserialize)]
struct SelectPanelBody {
    params: SelectPanelParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SelectPanelParams {
    #[serde(default)]
    package_tokens: Option<usize>,
    panel_spec: panel_policy::PanelRequestSpec,
    /// The substrate this panel's legs will dispatch on, resolved by the caller
    /// at launch. It decides which availability list the seats are judged
    /// against; absent or unknown keeps the caller's host provider list.
    #[serde(default)]
    substrate: Option<String>,
    #[serde(default)]
    selection: Option<PanelSelection>,
    #[serde(default)]
    roster: Vec<serde_json::Value>,
    #[serde(default)]
    available_models: Vec<String>,
    #[serde(default)]
    exclude_route_keys: Vec<String>,
    #[serde(default)]
    required_capabilities: Option<RequiredCapabilities>,
    #[serde(default)]
    subscription_keys: Vec<String>,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    seed: Option<String>,
    #[serde(default)]
    parent_session_id: Option<String>,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    project_root: Option<String>,
    #[serde(default, rename = "sendID")]
    send_id: Option<String>,
}

fn default_target_agent() -> String {
    "unknown".to_string()
}

struct ResolvedSelectRequest {
    requirements: ModelRequirements,
    required_capabilities: Option<RequiredCapabilities>,
    policy: ResolvedRoutingPolicy,
}

/// Provider auth kinds from the request, else read from OpenCode's `auth.json`
/// at `opencode_auth_path`. The path is passed in rather than resolved here so
/// unit tests hand in a scratch file; resolving it from HOME would read the
/// operator's live credentials file.
fn provider_auth_types(
    raw: Option<&HashMap<String, String>>,
    opencode_auth_path: &Path,
) -> HashMap<String, String> {
    if let Some(raw) = raw {
        return raw.clone();
    }
    // A unit test must never read the operator's real auth.json (it holds API
    // keys and OAuth tokens, and the test would then depend on which providers
    // the operator has configured).
    #[cfg(test)]
    assert!(
        opencode_auth_path.starts_with(prefrontal_test_support::scratch_root()),
        "routing unit test read OpenCode auth from {} instead of a scratch file",
        opencode_auth_path.display()
    );
    match config_overrides::read_provider_auth_types(opencode_auth_path) {
        Ok(types) => types,
        Err(error) => {
            eprintln!(
                "[prefrontal-routing] provider auth read failed (treating non-curated providers as metered): {error}"
            );
            HashMap::new()
        }
    }
}

/// Record the sole policy source with each decision so explanations remain tied
/// to the exact routing-owned document used for selection.
fn policy_sources_line(operator: &ModelRoutingPolicyInput) -> String {
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

/// Routes withheld from one head's launch because that head's recent worker was
/// refused by the provider's content policy, plus the explanation lines that say so.
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

fn claim_ignored_caller_policy_warning(sent: bool, warned: &Once) -> bool {
    if !sent {
        return false;
    }
    let mut claimed = false;
    warned.call_once(|| claimed = true);
    claimed
}

fn warn_ignored_caller_policy_once(sent: bool) {
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
        reason,
        message,
        rejected: Vec::new(),
        requirements: Some(requirements),
        best_rejected: None,
        earliest_reset: None,
        earliest_reset_route: None,
    }
}

fn resolve_select_request(
    params: &SelectParams,
    operator_policy: &ModelRoutingPolicyInput,
    opencode_auth_path: &Path,
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
    let policy = resolve_routing_policy(
        params.available_models.iter().cloned(),
        &availability_policy,
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(params.provider_auth_types.as_ref(), opencode_auth_path),
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

/// Merge partial upsert params over the stored fact. A NEW route requires elo
/// and eq: defaulting either would place a model in the ladder silently, the
/// quiet-wrongness class the seed migrations never had. Absent fields keep the
/// stored value; a new route's speed defaults to 100 (mid-catalog neutral)
/// until observed samples replace it.
fn merge_model_upsert(
    existing: Option<ModelFact>,
    params: &ModelUpsertParams,
) -> Result<ModelFact, String> {
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
                speed_is_fleet_median: false,
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
    if params.family.is_some() {
        fact.family = params.family.clone();
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
    if let Err(error) = store.upsert_fact(&fact, FactSource::Override) {
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
        if let Err(error) = store.upsert_fact(&fact, FactSource::Override) {
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
/// routes of one model. A score-alias row serves its target provider's model,
/// so it takes the target row's id.
fn panel_canonical_ids(
    store: &SqliteRouteStateStore,
    facts: &[ModelFact],
    aliases: &std::collections::BTreeMap<String, String>,
) -> Result<HashMap<String, String>, prefrontal_routing_core::RouteStateStoreError> {
    let stored = store.canonical_ids_by_route_key()?;
    Ok(facts
        .iter()
        .filter_map(|fact| {
            let route_key = fact_to_route(fact).route_key;
            let catalog_key =
                prefrontal_routing_core::score_alias::catalog_key(aliases, &route_key);
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
    let body =
        serde_json::to_vec(&json!({ "result": { "facts": facts } })).map_err(ModuleError::Json)?;
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
            let (family, family_source) = match store.get_model_family(&route_key) {
                Ok(Some(family)) => family,
                Ok(None) => (None, prefrontal_routing_core::FamilySource::Absent),
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
                "family": family.map(prefrontal_routing_core::GrandFamily::as_str),
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
                "family": null,
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
    opencode_auth_path: &Path,
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
        &provider_auth_types(params.provider_auth_types.as_ref(), opencode_auth_path),
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

/// subc-wire-name → routing providerID reconciliation. The quota module's wire
/// provider names ("codex", "claude", "antigravity", ...) differ from our
/// providerIDs ("openai", "anthropic", "google", ...). This mirrors the plugin's
/// live CODEXBAR_PROVIDER_MAP exactly (the 6 providers the plugin currently routes
/// quota for). An unmapped wire name contributes no pressure (degrades safely); the
/// `antigravity → google` entry pairs with the per-model extractor so the Geminis'
/// own windows pressure their capacities (not the misleading aggregate).
fn quota_name_map() -> &'static std::collections::HashMap<String, String> {
    use std::sync::OnceLock;
    static MAP: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        [
            ("codex", "openai"),
            ("grok", "xai"),
            ("antigravity", "google"),
            ("opencodego", "opencode-go"),
            ("ollama", "ollama-cloud"),
            ("claude", "anthropic"),
        ]
        .into_iter()
        .map(|(wire, pid)| (wire.to_string(), pid.to_string()))
        .collect()
    })
}

struct PreparedSelectionInputs {
    now: f64,
    facts: Vec<ModelFact>,
    snapshot: ModelRouteStateSnapshot,
    quota_signal_json: String,
    /// Set when the quota snapshot was too old to use: the explanation line
    /// saying so, which a selected route carries.
    quota_note: Option<String>,
}

#[derive(Clone)]
struct DecisionProvenance {
    quota_signal_json: String,
    policy_revision: String,
}

/// The cache facts a decision's quota signal summary is built from.
struct QuotaSignalView<'a> {
    windows: &'a [ProviderUsage],
    available: bool,
    refreshed_at_ms: u64,
    retained_degraded_count: usize,
    last_error: Option<&'a str>,
    restored_from_ms: Option<u64>,
    /// The snapshot's age when it is past the staleness bound; its windows
    /// were then NOT merged into the snapshot the scorer read.
    stale_age_ms: Option<u64>,
}

/// Summarize the cached overlay that the scorer consumed without serializing the
/// full quota response. A no-signal marker is explicit so it cannot be confused
/// with old rows written before the provenance columns existed.
fn quota_signal_json(snapshot: &ModelRouteStateSnapshot, view: &QuotaSignalView<'_>) -> String {
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
/// recording or writing its response.
fn prepare_selection_inputs(
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
) -> Result<PreparedSelectionInputs, String> {
    let now = now_ms() as f64;
    let facts = store
        .get_facts_with_observed_speed(now)
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
        merge_quota_overlay(&mut snapshot, windows, &facts, quota_name_map());
    }
    subscription_economics::overlay(&mut snapshot, &facts, now as i64);
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

/// Where one route.select spent its time, for the slow-request log line. Each
/// stage is a separate bucket so `unclassified_ms` (total minus every bucket)
/// stays near zero; a large remainder means a stage is missing a timer.
struct RouteSelectTiming {
    started: Instant,
    is_select: std::cell::Cell<bool>,
    live_config_check_ms: std::cell::Cell<u64>,
    /// Waiting for the first billing-plane load before pricing anything.
    billing_wait_ms: std::cell::Cell<u64>,
    /// Decoding the parameters and resolving requirements and the caller's
    /// available models (reads the auth file).
    resolve_ms: std::cell::Cell<u64>,
    prepare_wait_ms: std::cell::Cell<u64>,
    snapshot_closure_ms: std::cell::Cell<u64>,
    /// Live overlays, score aliases and billing-plane pricing on the facts.
    pricing_ms: std::cell::Cell<u64>,
    /// Harness availability, subscription policy, exclusions and pin lookup.
    policy_ms: std::cell::Cell<u64>,
    /// Reading this head's recent failed launches from the route store.
    head_history_ms: std::cell::Cell<u64>,
    selection_ms: std::cell::Cell<u64>,
    /// Recording the selection on the winner ladder.
    ladder_ms: std::cell::Cell<u64>,
    /// Writing the decision row whose id route.select returns to the caller.
    decision_write_ms: std::cell::Cell<u64>,
    /// Resolving the Broca launch target (and any key-pool lease).
    runner_ms: std::cell::Cell<u64>,
    /// Variant policy, explanation notes and building the response body.
    finish_ms: std::cell::Cell<u64>,
    reply_ms: std::cell::Cell<u64>,
}

/// Add `elapsed` to one stage bucket of a [`RouteSelectTiming`].
fn add_stage_ms(bucket: &std::cell::Cell<u64>, elapsed: Duration) {
    bucket.set(
        bucket
            .get()
            .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
    );
}

impl RouteSelectTiming {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            is_select: std::cell::Cell::new(false),
            live_config_check_ms: std::cell::Cell::new(0),
            billing_wait_ms: std::cell::Cell::new(0),
            resolve_ms: std::cell::Cell::new(0),
            prepare_wait_ms: std::cell::Cell::new(0),
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

    fn add_live_config_check(&self, elapsed: Duration) {
        self.live_config_check_ms.set(
            self.live_config_check_ms
                .get()
                .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
        );
    }

    fn record_prepare(&self, wait: Duration, snapshot: Duration) {
        self.prepare_wait_ms
            .set(wait.as_millis().min(u64::MAX as u128) as u64);
        self.add_snapshot_closure(snapshot);
    }

    fn add_snapshot_closure(&self, elapsed: Duration) {
        self.snapshot_closure_ms.set(
            self.snapshot_closure_ms
                .get()
                .saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64),
        );
    }

    fn record_selection(&self, elapsed: Duration) {
        // Adds rather than sets: a rejected pin with a fallback runs a second
        // selection, and both count.
        add_stage_ms(&self.selection_ms, elapsed);
    }

    fn record_reply(&self, elapsed: Duration) {
        self.reply_ms
            .set(elapsed.as_millis().min(u64::MAX as u128) as u64);
    }

    fn line_at(&self, now: Instant) -> Option<String> {
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
        let prepare_wait_ms = self.prepare_wait_ms.get();
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
            prepare_wait_ms,
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
            "[routing] slow route.select total_ms={total_ms} wait_prepare_ms={prepare_wait_ms} live_config_check_ms={live_config_check_ms} snapshot_closure_ms={snapshot_closure_ms} selection_ms={selection_ms} reply_ms={reply_ms} billing_wait_ms={billing_wait_ms} resolve_ms={resolve_ms} pricing_ms={pricing_ms} policy_ms={policy_ms} head_history_ms={head_history_ms} ladder_ms={ladder_ms} decision_write_ms={decision_write_ms} runner_ms={runner_ms} finish_ms={finish_ms} unclassified_ms={unclassified_ms}"
        ))
    }
}

#[derive(Default)]
struct RouteSelectLogLimiter {
    last_report: Option<Instant>,
    suppressed: u64,
}

fn rate_limit_route_select_line(
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
    fn new() -> Self {
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

async fn handle_select_request(
    mut frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    _min_band: &SharedMinBand,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
    opencode_auth_path: &Path,
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
        let policy = prefrontal_routing_core::accounts::AllocatorPolicy::default();
        let ctx = account_selection::Ctx {
            store: store.as_ref(),
            accounts: ACCOUNT_QUOTA.get(),
            flags: live_config.account_flags(),
            policy: &policy,
            now_ms: account_selection::now_ms(),
        };
        let reply = account_selection::handle(&envelope.method, &frame.body, &principal, &ctx);
        return send_account_reply(writer, ver, channel, epoch, corr, reply).await;
    }

    if envelope.method == ROUTE_SET_DECISION_OUTCOME_OP {
        return handle_set_decision_outcome(frame, writer, store, routing_config_path).await;
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
    if envelope.method == ROUTE_COOLDOWN_STATUS_OP {
        return handle_cooldown_status(frame, writer, store).await;
    }
    if envelope.method == ROUTE_EXPLAIN_OP {
        return handle_route_explain(frame, writer, store).await;
    }
    if envelope.method == ROUTE_DECISIONS_LIST_OP {
        return handle_route_decisions_list(frame, writer, store).await;
    }
    if envelope.method == ROUTE_RESOLVE_POLICY_OP {
        return handle_resolve_policy(frame, writer, operator_config_path, opencode_auth_path)
            .await;
    }
    if envelope.method == ROUTE_MODEL_FACT_OP {
        return handle_model_fact(frame, writer, store).await;
    }
    if envelope.method == ROUTE_MODEL_FACTS_OP {
        return handle_model_facts(frame, writer, store, operator_config_path).await;
    }
    if envelope.method == model_resolution::RESOLVE_MODEL_OP {
        return model_resolution::handle_resolve_model(frame, writer).await;
    }
    if envelope.method == model_resolution::MODEL_CAPABILITIES_OP {
        return model_resolution::handle_model_capabilities(frame, writer, store).await;
    }
    if envelope.method == model_resolution::AGENT_MODEL_REQUIREMENTS_OP {
        return model_resolution::handle_agent_model_requirements(frame, writer).await;
    }
    if envelope.method == available_models_text::OP {
        return available_models_text::handle(
            frame,
            writer,
            store,
            operator_config_path,
            opencode_auth_path,
        )
        .await;
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
        let policy = config_overrides::read_operator_routing_policy(operator_config_path);
        let reply = key_pool::handle(
            &envelope.method,
            &frame.body,
            &key_pool::PoolCtx {
                store: store.as_ref(),
                credentials: broca_credentials,
                policy: &policy,
                now_ms: now_ms() as i64,
            },
        );
        return send_pool_reply(writer, ver, channel, epoch, corr, reply).await;
    }
    let billing_ready = if matches!(
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
            opencode_auth_path,
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
    // A host does not choose the substrate of the mason it launches. A host's
    // mason pin on a provider that needs a per-send credential pin is admitted
    // provisionally against Broca's routes, without leasing a key; core selects
    // the pin again with substrate broca before it records the launch.
    let deferred_runner_pin = host_run
        && params.substrate.is_none()
        && params.target_agent == "mason"
        && params.model_pin.as_deref().is_some_and(|pin| {
            pin.split_once('/')
                .is_some_and(|(provider, _)| needs_runner_pin(&operator_policy, provider))
        });
    // The harness a host-run selection is judged on: Broca's served routes when
    // the caller names substrate broca (or for a deferred runner pin), otherwise
    // the caller's own list, which has no harness of routing's choosing.
    let host_run_harness = (host_run
        && (deferred_runner_pin || is_broca_substrate(params.substrate.as_deref())))
    .then_some(substrate_policy::Harness::Broca);
    let snapshot_started = Instant::now();
    let mut availability = match store.get_facts() {
        Ok(mut facts) => {
            live_config.apply(&mut facts);
            // Aliased providers are reachable through their own credentials,
            // so their borrowed rows join the served-route join.
            prefrontal_routing_core::score_alias::append_alias_rows(
                &mut facts,
                &operator_policy.score_alias,
            );
            select_availability(Some("broca"), Vec::new(), &facts, broca_credentials)
        }
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
    timing.add_snapshot_closure(snapshot_started.elapsed());
    // A host-run selection judged on the caller's own list never consulted
    // Broca's routes, so its explanation must not describe them.
    if host_run && host_run_harness.is_none() {
        availability.note = None;
    }
    let resolve_started = Instant::now();
    let resolved_request =
        match resolve_select_request(&params, &operator_policy, opencode_auth_path) {
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
                let _ = store.record_decision(&decision);
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
    let (prepared, prepare_wait, snapshot_closure) =
        live_config.prepare_with_timing(|| prepare_selection_inputs(quota_cache, store));
    timing.record_prepare(prepare_wait, snapshot_closure);
    let PreparedSelectionInputs {
        now,
        mut facts,
        snapshot,
        quota_signal_json,
        quota_note,
    } = match prepared {
        Ok(prepared) => prepared,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &e).await;
        }
    };
    let pricing_started = Instant::now();
    let outcome = live_config.apply(&mut facts);
    let (routing_config, policy_revision) = (outcome.routing_config, outcome.policy_revision);
    let provenance = DecisionProvenance {
        quota_signal_json,
        policy_revision,
    };

    // Borrowed rows are added after the live overlays so they carry the target's
    // current quality data, and before pricing so they get their own plane.
    prefrontal_routing_core::score_alias::append_alias_rows(
        &mut facts,
        &operator_policy.score_alias,
    );
    if let Ok(credentials) = broca_credentials.lock() {
        credentials.price_facts(&mut facts);
    }
    live_config.apply(&mut facts);
    add_stage_ms(&timing.pricing_ms, pricing_started.elapsed());

    let policy_started = Instant::now();
    let host_models: HashSet<String> = resolved_request
        .policy
        .available_models
        .iter()
        .cloned()
        .collect();
    let broca_models: HashSet<String> = availability.models.iter().cloned().collect();
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
    let mapped_availability = if host_run {
        selection_harness::MappedAvailability {
            models: if host_run_harness.is_some() {
                broca_models.clone()
            } else {
                host_models.clone()
            },
            rejected: HashMap::new(),
        }
    } else {
        harness_context.availability(&facts)
    };
    let available = mapped_availability.models.clone();
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
    let mut execution_operator_policy = operator_policy.clone();
    if params.pin.is_some() || params.model_pin.is_some() {
        execution_operator_policy.exclude.clear();
    }
    let execution_policy = resolve_routing_policy(
        available.iter().cloned(),
        &execution_operator_policy,
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(params.provider_auth_types.as_ref(), opencode_auth_path),
    );
    let subscription_keys: HashSet<String> =
        execution_policy.subscription_keys.into_iter().collect();
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
    let concurrency_slots = params.concurrency_slots.clone().map(ConcurrencySlots::new);

    let admission_pin = params
        .pin
        .as_ref()
        .and_then(|pin| pin.resolve(operator_config.pins()));
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
        let mut pin_availability = mapped_availability.rejected.clone();
        let mut pin_available = available.clone();
        // A host-run pin is judged on the same list as the host's automatic
        // selections; it is not added to that list.
        if !host_run {
            if let Some(detail) = harness_context.rejection(&pin_key, &pin_capacity) {
                pin_availability.insert(pin_key.clone(), detail);
            } else {
                pin_available.insert(pin_key.clone());
            }
        }
        let pin_harness = run_harness(&pin_key);
        let runner_pin_refusal = if host_run {
            host_runner_pin_refusal(
                &operator_policy,
                if deferred_runner_pin {
                    Some("broca")
                } else {
                    params.substrate.as_deref()
                },
                "this request names substrate",
                [pin],
            )
        } else {
            let mapped = harness_context.mapped(&pin_key);
            host_runner_pin_refusal(
                &operator_policy,
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
        let sub = subscription_keys.clone();
        add_stage_ms(&timing.policy_ms, policy_started.elapsed());
        let selection_started = Instant::now();
        let selection = {
            let input = SelectModelRouteInput {
                target_agent: target_agent.clone(),
                requirements,
                available_models: pin_available,
                facts: facts.clone(),
                runtime_snapshot: Some(snapshot.clone()),
                exclude_route_keys: exclude.clone(),
                operator_exclude_keys: operator_exclude.clone(),
                required_capabilities,
                is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                    sub.contains(&route.route_key) || sub.contains(&route.capacity_key)
                })),
                min_best_effort_adequacy,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
            };
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
                    operator_config.score_aliases(),
                    &pin_availability,
                )
            }
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
                params.consumer,
                explicit.as_deref(),
            );
            (selected, choice)
        });
        let selection = selection.and_then(|(selected, variant_choice)| {
            let policy = prefrontal_routing_core::accounts::AllocatorPolicy::default();
            let ctx = account_selection::Ctx {
                store: store.as_ref(),
                accounts: ACCOUNT_QUOTA.get(),
                flags: live_config.account_flags(),
                policy: &policy,
                now_ms: now as i64,
            };
            account_selection::preflight_model(
                &ctx,
                &selected.model.provider_id,
                &selected.model.model_id,
            )
            .map_err(|(code, message)| work_model_pin::Refusal { code, message })?;
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
                            &operator_policy,
                            broca_credentials,
                            store.as_ref(),
                            &session,
                            &selected.model.provider_id,
                            &selected.model.model_id,
                            selected.prompt_params.variant.as_deref(),
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
                let ladder_started = Instant::now();
                record_ladder(pin_harness);
                add_stage_ms(&timing.ladder_ms, ladder_started.elapsed());
                if let Some(line) = &executor_workload {
                    selected.explanation.push(line.clone());
                }
                let write_started = Instant::now();
                let decision = decision_input_for(
                    &target_agent,
                    &requirements,
                    Some(&selected),
                    None,
                    &params,
                    Some(&provenance),
                    Some(&policy_sources_line(&operator_policy)),
                );
                let decision_id = store
                    .record_decision_with_selection(
                        &decision,
                        pin_harness.map(substrate_policy::Harness::as_str),
                        selected_workload,
                    )
                    .map_err(|error| ModuleError::Message(error.to_string()))?;
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
                    result["requiredSubstrate"] = json!("broca");
                }
                if let Some((runner, grant)) = runner {
                    result["runner"] = json!(runner);
                    if let Some(grant) = grant {
                        result["poolLease"] = grant.lease_json();
                    }
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
                    pin_rejected = Some(refusal.code);
                    if params.pin_fallback == select_policy::PinFallback::None {
                        let body = serde_json::to_vec(&json!({ "result": {
                            "selected": null, "decisionID": null, "pin_rejected": pin_rejected,
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
                        return send(writer, response).await;
                    }
                } else {
                    if refusal.code == "not_available" && !host_run {
                        refusal.code = "pinned_route_refused".into();
                    }
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        &refusal.code,
                        &refusal.message,
                    )
                    .await;
                }
            }
        }
    }

    let attempt = |snap: ModelRouteStateSnapshot| -> Result<SelectedModelRoute, NoRouteError> {
        let sub = subscription_keys.clone();
        select_model_route_with_availability(
            SelectModelRouteInput {
                target_agent: target_agent.clone(),
                requirements,
                available_models: available.clone(),
                facts: facts.clone(),
                runtime_snapshot: Some(snap),
                exclude_route_keys: exclude.clone(),
                operator_exclude_keys: operator_exclude.clone(),
                required_capabilities,
                is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                    sub.contains(&route.route_key) || sub.contains(&route.capacity_key)
                })),
                min_best_effort_adequacy,
                prefer_prompt_cache,
                providers_without_prompt_cache: providers_without_prompt_cache
                    .iter()
                    .cloned()
                    .collect(),
            },
            &routing_config,
            concurrency_slots.as_ref(),
            consumer_exclude,
            &mapped_availability.rejected,
        )
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
                params.consumer,
                None,
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
            let decision = decision_input_for(
                &target_agent,
                &requirements,
                Some(&selected),
                None,
                &params,
                Some(&provenance),
                Some(&policy_sources),
            );
            let recorded = store.record_decision_with_selection(
                &decision,
                selected_harness.map(substrate_policy::Harness::as_str),
                selected_workload,
            );
            add_stage_ms(&timing.decision_write_ms, write_started.elapsed());
            match recorded {
                Ok(decision_id) => {
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
                            &operator_policy,
                            broca_credentials,
                            store.as_ref(),
                            &session,
                            &selected.model.provider_id,
                            &selected.model.model_id,
                            selected.prompt_params.variant.as_deref(),
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
            // Best-effort failed-decision audit row (ignore a store error here: the
            // routing failure is the real result to surface).
            let decision = decision_input_for(
                &target_agent,
                &requirements,
                None,
                Some(&no_route),
                &params,
                Some(&provenance),
                Some(&policy_sources),
            );
            let _ = store.record_decision(&decision);
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

/// Panel legs use the substrate that sends. Claustrum's complete mappings
/// restrict Broca to credentialed providers; incomplete/stale listings fall
/// back to the served catalog, just like single-route Broca selection.
fn panel_availability(
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

fn needs_runner_pin(policy: &ModelRoutingPolicyInput, provider: &str) -> bool {
    key_pool::is_pool_provider(policy, provider)
}

/// The Broca launch target for a selected route: the plain translation for an
/// ordinary provider. For a score-aliased key-pool provider it leases a key for
/// `session` from the pool and returns the target that pins it, with the grant;
/// a pool that cannot be resolved or has no admissible key refuses by name.
fn runner_for_route(
    policy: &ModelRoutingPolicyInput,
    credentials: &SharedBrocaCredentials,
    store: &SqliteRouteStateStore,
    session: &str,
    provider: &str,
    model: &str,
    variant: Option<&str>,
) -> Result<
    (
        prefrontal_routing_core::runner::RunnerModel,
        Option<key_pool::PoolGrant>,
    ),
    (String, String),
> {
    if !needs_runner_pin(policy, provider) {
        return Ok((
            prefrontal_routing_core::runner::RunnerModel::broca(provider, model, variant),
            None,
        ));
    }
    let grant = key_pool::acquire(
        &key_pool::PoolCtx {
            store,
            credentials,
            policy,
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
    policy: &ModelRoutingPolicyInput,
    requested_substrate: Option<&str>,
    substrate_source: &str,
    pinned_routes: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    if is_broca_substrate(requested_substrate) {
        return None;
    }
    pinned_routes.into_iter().find_map(|route| {
        let provider = route.split_once('/').map_or(route, |(provider, _)| provider);
        needs_runner_pin(policy, provider).then(|| {
            format!(
                "{route}: provider {provider} needs a per-send credential pin that only the Broca runner carries, but {substrate_source} {}",
                requested_substrate.unwrap_or("none")
            )
        })
    })
}

fn is_broca_substrate(requested_substrate: Option<&str>) -> bool {
    requested_substrate.map(str::trim) == Some("broca")
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
struct SelectAvailability {
    models: Vec<String>,
    note: Option<String>,
}

impl SelectAvailability {
    fn host(models: Vec<String>) -> Self {
        Self { models, note: None }
    }
}

/// Availability for a `route.select` whose caller names the substrate that
/// will send.
///
/// A Broca send is judged against what Broca can reach now, never against a
/// list the caller carries: callers without a host (core's own classifier,
/// wake author, and comment reviewer) have no list of their own, and a host's
/// list describes a different substrate. If the vault listing is unknown or
/// no credential has a provider mapping, selection proceeds over every route
/// the catalog still serves. Otherwise it uses only explicitly mapped providers.
/// It stays closed to routes known to be unserved, because withdrawn catalog
/// rows never reach `facts`, and operator exclusions, cooldowns, and failure
/// backoff still apply downstream.
fn select_availability(
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
        None => format!("availability: broca ({} served catalog routes on providers Broca holds a credential for)", models.len()),
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
    opencode_auth_path: &Path,
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
    let mut prepared = match live_config.prepare(|| prepare_selection_inputs(quota_cache, store)) {
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
    // Use the operator's score aliases and subscription coverage, never the
    // caller's subscriptionKeys, to price and admit panel candidates.
    prefrontal_routing_core::score_alias::append_alias_rows(
        &mut prepared.facts,
        &operator_policy.score_alias,
    );
    if let Ok(credentials) = broca_credentials.lock() {
        credentials.price_facts(&mut prepared.facts);
    }
    live_config.apply(&mut prepared.facts);
    warn_ignored_caller_policy_once(!params.subscription_keys.is_empty());
    let resolved_policy = resolve_routing_policy(
        params.available_models.iter().cloned(),
        &ModelRoutingPolicyInput {
            subscription: operator_policy.subscription.clone(),
            score_alias: operator_policy.score_alias.clone(),
            ..ModelRoutingPolicyInput::default()
        },
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(None, opencode_auth_path),
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
    let canonical_ids =
        match panel_canonical_ids(store, &prepared.facts, &operator_policy.score_alias) {
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
            synthesizer: live_config.pins().synthesizer.as_ref().map(|pin| {
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
    let mut panel = match prefrontal_routing_core::panel::select_panel_with_availability(
        SelectPanelInput {
            package_tokens: params.package_tokens,
            panel_spec: params.panel_spec.selection_spec(),
            selection: PanelSelection::NPick,
            roster: Vec::new(),
            facts: prepared.facts,
            runtime_snapshot: Some(prepared.snapshot),
            exclude_route_keys: params.exclude_route_keys.clone(),
            operator_exclude_keys,
            required_capabilities: params.required_capabilities,
            available_models: mapped_availability.models,
            availability_substrate,
            is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                subscription_keys.contains(&route.route_key)
                    || subscription_keys.contains(&route.capacity_key)
            })),
        },
        &routing_config,
        &policy,
        &context,
        &mapped_availability.rejected,
    ) {
        Ok(panel) => panel,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "invalid_request", &e.0)
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
        if class == prefrontal_routing_core::panel::PanelClass::Lab {
            select_policy::Consumer::Lab
        } else {
            select_policy::Consumer::Athena
        },
        &seat_models,
    );
    // Vault-mapped providers need the selected credential and launch target in
    // promptParams.runner so core can persist and dispatch each member correctly.
    if is_broca_substrate(params.substrate.as_deref()) {
        for (slot, member) in panel.members.iter_mut().enumerate() {
            let Some((provider, model)) = member.route_key.split_once('/') else {
                continue;
            };
            if !needs_runner_pin(&operator_policy, provider) {
                continue;
            }
            let variant = member
                .prompt_params
                .get("variant")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            // Each seat is its own session for the key pool: seats spread over
            // the keys and each keeps its own prompt-cache workspace.
            let session = format!(
                "{}#seat{slot}",
                pool_session(
                    params.task_id.as_deref(),
                    params.send_id.as_deref(),
                    &format!("route.select_panel:{}", now_ms())
                )
            );
            match runner_for_route(
                &operator_policy,
                broca_credentials,
                store.as_ref(),
                &session,
                provider,
                model,
                variant.as_deref(),
            ) {
                Ok((runner, grant)) => {
                    member.prompt_params.insert("runner".into(), json!(runner));
                    if let Some(grant) = grant {
                        member
                            .prompt_params
                            .insert("poolLease".into(), grant.lease_json());
                    }
                }
                Err((code, message)) => {
                    return send_route_error(writer, ver, channel, epoch, corr, &code, &message)
                        .await;
                }
            }
        }
    }
    let decision = panel_decision_input(
        &params,
        &panel,
        availability_substrate,
        &provenance,
        &policy_sources_line(&operator_policy),
    );
    let decision_id = match store.record_decision_with_selection(&decision, Some("broca"), None) {
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
        member["harness"] = json!("broca");
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
fn apply_variant_policy(
    selected: &mut SelectedModelRoute,
    config: &config_overrides::LiveConfig,
    config_path: &Path,
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
        config.score_aliases(),
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

/// Apply the variant overrides to every seated member and runner-up of an
/// automatic panel, recording each seat's variant and rule beside its entry in
/// the policy's `seatedVariants`.
fn apply_panel_variant_policy(
    panel: &mut SelectedPanel,
    config: &config_overrides::LiveConfig,
    config_path: &Path,
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
            config.score_aliases(),
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

fn panel_decision_input(
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
        "availability": availability.as_str(),
        "workload": "workload: athena",
    });
    DecisionInput {
        decision_id: None,
        decision_kind: "panel".into(),
        task_id: params.task_id.clone(),
        parent_session_id: params.parent_session_id.clone(),
        harness: params
            .harness
            .as_deref()
            .filter(|value| matches!(*value, "opencode" | "broca" | "pi"))
            .map(str::to_owned),
        project_root: params
            .project_root
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned),
        send_id: params.send_id.clone(),
        target_agent: "athena-panel".into(),
        requirements_json: serde_json::to_string(&params.panel_spec)
            .unwrap_or_else(|_| "null".into()),
        selected_route_key: None,
        selected_provider_id: None,
        selected_model_id: None,
        selected_variant: None,
        score_json: "null".into(),
        rejected_json: serde_json::to_string(&causes).unwrap_or_else(|_| "[]".into()),
        explanation_json: Some(serde_json::to_string(&explanation).unwrap_or_else(|_| "{}".into())),
        quota_signal_json: Some(provenance.quota_signal_json.clone()),
        policy_revision: Some(provenance.policy_revision.clone()),
        anchor_version: ANCHOR_VERSION,
    }
}

/// Build the decision audit row from a selection or the refusal that prevented one.
fn decision_input_for(
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
    match selected {
        Some(sel) => DecisionInput {
            decision_id: None,
            decision_kind: "single".into(),
            task_id: params.task_id.clone(),
            parent_session_id: params.parent_session_id.clone(),
            harness: params
                .harness
                .as_deref()
                .filter(|value| matches!(*value, "opencode" | "broca" | "pi"))
                .map(str::to_owned),
            project_root: params
                .project_root
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned),
            send_id: params.send_id.clone(),
            target_agent: target_agent.to_string(),
            requirements_json,
            selected_route_key: Some(sel.route_key.clone()),
            selected_provider_id: Some(sel.model.provider_id.clone()),
            selected_model_id: Some(sel.model.model_id.clone()),
            selected_variant: sel.model.variant.clone(),
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
                decision_id: None,
                decision_kind: "single".into(),
                task_id: params.task_id.clone(),
                parent_session_id: params.parent_session_id.clone(),
                harness: params
                    .harness
                    .as_deref()
                    .filter(|value| matches!(*value, "opencode" | "broca" | "pi"))
                    .map(str::to_owned),
                project_root: params
                    .project_root
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned),
                send_id: params.send_id.clone(),
                target_agent: target_agent.to_string(),
                requirements_json,
                selected_route_key: None,
                selected_provider_id: None,
                selected_model_id: None,
                selected_variant: None,
                score_json: "null".into(),
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

#[derive(Debug, Deserialize)]
struct MethodParamsBody<P> {
    params: P,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetDecisionOutcomeParams {
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
    routing_config_path: &Path,
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
    let failure_backoff = config_overrides::live_config(routing_config_path).failure_backoff();
    let update = DecisionOutcomeUpdate {
        outcome,
        last_error: params.last_error,
        observed_at_ms: now_ms() as i64,
        failure_backoff,
    };
    match store.set_decision_outcome(&params.decision_id, &update) {
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
    let params: prefrontal_routing_core::NativeUsageFacts =
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
    let observation = match model_resolution::usage_observation_from_store(params, store) {
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
    let provider_state = if let Some(provider_key) =
        derive_provider_key(&params.route_key).or_else(|| derive_provider_key(&params.capacity_key))
    {
        match store.get_provider_cooldown(provider_key) {
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
    let snapshot = match store.build_snapshot(params.now as f64) {
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
    let provider_cooled = provider_state.as_ref().is_some_and(|state| {
        state
            .cooldown_until
            .is_some_and(|until| until > params.now as f64)
            || state
                .quota_exhausted_until
                .is_some_and(|until| until > params.now as f64)
    });
    let cooled = provider_cooled
        || snapshot
            .by_route
            .get(&params.route_key)
            .into_iter()
            .chain(snapshot.by_capacity.get(&params.capacity_key))
            .any(|state| {
                state
                    .cooldown_until
                    .is_some_and(|until| until > params.now as f64)
                    || state
                        .quota_exhausted_until
                        .is_some_and(|until| until > params.now as f64)
                    || state
                        .failure_backoff_until
                        .is_some_and(|until| until > params.now as f64)
            });
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
    let result = json!({
        "decisionID": decision.id,
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
            "score": recorded_json(&decision.score_json),
            "explanation": recorded_optional_json(decision.explanation_json.as_deref()),
            // This is the persisted rejection payload, not a new scoring pass.
            "rejections": recorded_json(&decision.rejected_json),
        },
        "quotaSignalSnapshot": recorded_optional_json(decision.quota_signal_json.as_deref()),
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
    let prior = match store.clear_failure_backoff(&params.route_key) {
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
    let prior = match store.clear_cooldown(&params.route_key) {
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
    eprintln!("[prefrontal-routing] route.clear_cooldown {}: previous cooldown_until={:?}, quota_exhausted_until={:?}",
        params.route_key, prior.cooldown_until, prior.quota_exhausted_until);
    let body = serde_json::to_vec(&json!({ "result": {
        "routeKey": params.route_key,
        "clearedCooldownUntil": prior.cooldown_until,
        "clearedQuotaExhaustedUntil": prior.quota_exhausted_until,
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
    match record_cooldown_update(store.as_ref(), &params) {
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

/// Preserve the `NoRouteError` reason code on the wire so consumers can distinguish
/// quota/cooldown exhaustion from invalid requirements or inadequate models.
/// Include grouped blocking reasons and the route whose wall expires earliest;
/// inadequate-route errors also name the best rejected candidate.
async fn send_no_route(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    err: &NoRouteError,
    pin_rejected: Option<&str>,
) -> Result<(), ModuleError> {
    let code = err.reason.as_str();
    let mut message = if err.reason == RouteFailureReason::AllRoutesInadequate {
        let best_part = match &err.best_rejected {
            Some(b) => format!(
                " Best candidate: {} (adequacy {:.2}).",
                b.route_key, b.adequacy
            ),
            None => String::new(),
        };
        let reset_part = match err.earliest_reset {
            Some(ts) => format!(" Earliest quota reset: {:.0}.", ts),
            None => String::new(),
        };
        format!("{}{}{}", err.message, best_part, reset_part)
    } else {
        err.refusal_message()
    };
    if !message.starts_with("No models available")
        && !message.contains("requires explicit model_requirements")
    {
        if let Some(requirements) = err.requirements {
            message.push_str(&format!(
                " Requirements iq={} eq={}.",
                requirements.iq, requirements.eq
            ));
        }
    }
    // blockingCauses names why routes are blocked, and earliestResetRoute names
    // whose expiry earliestReset reports; clients need not parse the message.
    let mut body_value = if matches!(
        err.reason,
        RouteFailureReason::AllRoutesInadequate | RouteFailureReason::QuotaOrCooldownExhausted
    ) {
        let mut obj = serde_json::Map::new();
        obj.insert("code".into(), json!(code));
        obj.insert("message".into(), json!(message));
        if let Some(ref best) = err.best_rejected {
            obj.insert(
                "bestRejected".into(),
                json!({ "routeKey": best.route_key, "adequacy": best.adequacy }),
            );
        }
        if let Some(earliest) = err.earliest_reset {
            obj.insert("earliestReset".into(), json!(earliest));
        }
        if let Some(route) = &err.earliest_reset_route {
            obj.insert("earliestResetRoute".into(), json!(route));
        }
        obj.insert("blockingCauses".into(), json!(err.blocking_causes()));
        serde_json::Value::Object(obj)
    } else {
        json!({ "code": code, "message": message })
    };
    if let Some(reason) = pin_rejected {
        body_value["pin_rejected"] = json!(reason);
    }
    let body = serde_json::to_vec(&body_value).map_err(ModuleError::Json)?;
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
    let body = serde_json::to_vec(&ErrorBody {
        code: code.to_string(),
        message: message.to_string(),
        detail: None,
    })
    .map_err(ModuleError::Json)?;
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

fn handles(method: &str) -> bool {
    matches!(
        method,
        ROUTE_SELECT_OP
            | ROUTE_RESOLVE_POLICY_OP
            | ROUTE_MODEL_FACT_OP
            | ROUTE_MODEL_FACTS_OP
            | model_resolution::RESOLVE_MODEL_OP
            | model_resolution::MODEL_CAPABILITIES_OP
            | model_resolution::AGENT_MODEL_REQUIREMENTS_OP
            | available_models_text::OP
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
            | ROUTE_COOLDOWN_STATUS_OP
            | ROUTE_EXPLAIN_OP
            | ROUTE_DECISIONS_LIST_OP
            | QUOTA_STATUS_OP
            | ROUTE_ACCOUNT_QUOTA_STATUS_OP
            | account_selection::ACCOUNT_FOR_OP
            | account_selection::ACCOUNT_STATUS_OP
            | account_selection::ACCOUNT_OBSERVE_OP
            | account_selection::ACCOUNT_RELEASE_OP
    ) || key_pool::handles(method)
}

/// Operations registered in HELLO and exposed by `--print-manifest`.
fn served_operations() -> Vec<ManagementOperation> {
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
        // route.select is a mutation because it records the decision it returns.
        ManagementOperation {
            name: ROUTE_SELECT_OP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: None,
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
            name: model_resolution::RESOLVE_MODEL_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: model_resolution::MODEL_CAPABILITIES_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: model_resolution::AGENT_MODEL_REQUIREMENTS_OP.to_string(),
            kind: ManagementOperationKind::Query,
            description: None,
        },
        ManagementOperation {
            name: available_models_text::OP.to_string(),
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
        // The account-selection operations. Each refuses with
        // `account_selection_disabled` until routing policy enables serving.
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
    ];
    operations.extend(pool_operations);
    operations
}

#[derive(Serialize)]
struct PrintedManifestOperation {
    name: String,
    kind: &'static str,
}

fn print_manifest_operations() -> Result<(), ModuleError> {
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
    let output = serde_json::to_string(&operations).map_err(ModuleError::Json)?;
    println!("{output}");
    Ok(())
}

/// The module's capability manifest: a ManagementSurface exposing routing operations.
///
/// The catalog and route-state tables are machine-global, so `identity_scope` is
/// empty and the identity binding requires nothing. The routing module owns this
/// schema and is the sole writer for its provider and route cooldown state.
fn manifest(module_id: &str) -> ModuleManifest {
    // `ModuleManifest` is `#[non_exhaustive]`, so this module constructs it
    // with the builder. This module declares no capabilities or self-signals.
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
    mod harness_selection {
        use super::*;
        include!("selection_harness/contract_tests.rs");
    }
    include!("select_policy_tests.rs");
    include!("panel_policy_tests.rs");
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use prefrontal_routing_core::model_facts::{
        Confidence, ModelFact, ModelPromptDefaults, ModelScores,
    };
    use prefrontal_routing_core::model_router::select_model_route_with_config;
    use prefrontal_routing_core::{FactSource, NativeUsageFacts, UsageObservation};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn config_check_uses_startup_loader_and_refuses_renamed_or_unknown_config() {
        assert_eq!(
            parse_startup_mode([OsString::from("--check-config")]).unwrap(),
            StartupMode::CheckConfig
        );
        assert!(
            parse_startup_mode([OsString::from("--check-config"), OsString::from("--subc=x")])
                .is_err()
        );
        let scratch = prefrontal_test_support::ScratchDir::new("routing-config-check");
        let path = scratch.join("prefrontal-routing.jsonc");
        check_config_at(&path).unwrap();
        let renamed = prefrontal_test_support::ScratchDir::new("routing-config-check-renamed");
        std::fs::write(renamed.join("alfonso-routing.jsonc"), "{}").unwrap();
        assert!(check_config_at(&renamed.join("prefrontal-routing.jsonc"))
            .unwrap_err()
            .to_string()
            .contains("config_renamed"));
        let invalid = prefrontal_test_support::ScratchDir::new("routing-config-check-invalid");
        let invalid_path = invalid.join("prefrontal-routing.jsonc");
        std::fs::write(&invalid_path, "{\"not_a_routing_setting\":true}").unwrap();
        assert!(check_config_at(&invalid_path).is_err());
    }

    #[test]
    fn config_check_refuses_explicit_only_providers_and_names_the_exclude_remedy() {
        let scratch = prefrontal_test_support::ScratchDir::new("routing-config-check-explicit");
        let path = scratch.join("prefrontal-routing.jsonc");
        std::fs::write(
            &path,
            r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
        )
        .unwrap();
        let error = check_config_at(&path).unwrap_err().to_string();
        assert!(
            error.contains("config_key_removed: model_routing.explicit_only_providers")
                && error
                    .contains("list the provider in model_routing.exclude.all; pins still work"),
            "{error}"
        );
    }

    #[test]
    fn record_cooldown_persists_typed_billing_and_quota_wall_classes() {
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
        assert_eq!(
            snapshot.by_route["p/plain-quota"].wall_class,
            Some(RouteWallClass::Quota)
        );
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
            "[prefrontal-routing] quota refresh ignored unsupported spend funding values: {\"issuer_credit_v2\"}"
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
        assert!(!prefrontal_routing_core::is_terminal_send_failure(outcome));
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

    #[test]
    fn quota_provenance_records_the_pacing_window_model() {
        let snapshot = ModelRouteStateSnapshot {
            now: 0.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::from([(
                "openai/gpt-5.6-sol".to_string(),
                prefrontal_routing_core::RouteRuntimeState {
                    quota_pressure: Some(0.7),
                    quota_window_model: Some(
                        prefrontal_routing_core::PaceWindowModel::UtilizationFallback,
                    ),
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

    impl Rig {
        fn new(label: &str) -> Self {
            let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            // nanos: cross-run uniqueness under pid reuse
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos();
            let dir = prefrontal_test_support::scratch_root().join(format!(
                "prefrontal-routing-module-{label}-{}-{nanos}-{n}",
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

    /// The poll loop is the only producer of the quota reading log, and the slice
    /// that built the log fenced the loop out, so this is the test that the log
    /// is written at all: one healthy Insula entry through the same call the loop
    /// makes lands as rows under the overlay's provider id and window keys, and a
    /// degraded entry (error present) lands nothing.
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
            speed_is_fleet_median: false,
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
        }
    }

    fn usage(v: serde_json::Value) -> ProviderUsage {
        serde_json::from_value(v).expect("provider usage")
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
            .get_facts_with_observed_speed(now_ms() as f64)
            .unwrap()
            .into_iter()
            .filter(|fact| fact.provider_id == "observed-caller")
            .collect::<Vec<_>>();
        let prepared =
            prepare_selection_inputs(&Arc::new(Mutex::new(QuotaCache::default())), &store).unwrap();
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
                "project_root": prefrontal_test_support::scratch_root(),
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
                    &rig.dir.join("auth.json"),
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
                    "project_root": prefrontal_test_support::scratch_root(),
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
        serde_json::from_str(include_str!("../../../test-vectors/select-panel-wire.json"))
            .expect("select_panel wire vectors")
    }

    async fn run_route_wire_request(store: &SharedStore, request: serde_json::Value) -> Frame {
        run_route_wire_request_with_operator_config(
            store,
            request,
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
        )
        .await
    }

    fn empty_broca_credentials() -> SharedBrocaCredentials {
        // These wire fixtures represent a completed catalog load with no supplied
        // planes. Cold-start tests construct the uninitialized cache explicitly.
        Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(&[])))
    }

    fn mapped_broca_credentials(provider: &str) -> SharedBrocaCredentials {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: vec![prefrontal_ckcred_client::ListedCredential {
                    id: "fixture".into(),
                    categories: vec!["llm-provider".into()],
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

    /// An OpenCode `auth.json` with no providers, under the scratch root, for
    /// requests that carry no provider auth types (see `provider_auth_types`).
    fn empty_opencode_auth() -> PathBuf {
        static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        PATH.get_or_init(|| {
            let path = prefrontal_test_support::scratch_root().join(format!(
                "prefrontal-routing-empty-opencode-auth-{}.json",
                std::process::id()
            ));
            std::fs::write(&path, "{}").unwrap();
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
            &empty_broca_credentials(),
        )
        .await
    }

    pub(super) async fn run_route_wire_request_with_paths(
        store: &SharedStore,
        mut request: serde_json::Value,
        operator_config_path: &Path,
        broca_credentials: &SharedBrocaCredentials,
    ) -> Frame {
        // Legacy wire fixtures exercise executor availability from an explicit
        // list or Broca substrate. Launch-mapping tests use their own fixture.
        if request["method"] == ROUTE_SELECT_OP {
            request["params"]
                .as_object_mut()
                .unwrap()
                .entry("executor")
                .or_insert(json!("broca"));
        }
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            operator_config_path,
            broca_credentials,
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
    }

    /// Explicitly ignored: reads live Fusiform metadata, but all route.select
    /// decisions and catalog updates go only to a VACUUM INTO copy under target.
    /// The real persisted account roster identifies login methods. The listing
    /// is not persisted, so non-roster serving methods remain explicitly unknown.
    #[tokio::test]
    #[ignore = "requires a copied routing store and live Fusiform"]
    async fn billing_plane_route_select_dry_run_on_store_copy() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let copy = root.join("target/billing-dry-run/store.db");
        let connection = PathBuf::from(
            std::env::var("ROUTING_BILLING_SUBC_CONNECTION").expect("live subc connection path"),
        );
        assert!(copy.is_file(), "create target/billing-dry-run/store.db with sqlite3 -readonly live.db VACUUM INTO first");
        let store: SharedStore = Arc::new(
            SqliteRouteStateStore::open(&prefrontal_routing_store::dev_descriptor(
                "billing-dry-run-copy",
                copy.to_string_lossy().into_owned(),
            ))
            .unwrap(),
        );
        let mut before = BrocaCredentials::default();
        before.billing_catalog = fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[]);
        before.legacy_pricing_baseline = true;
        let before_credentials = Arc::new(Mutex::new(before));
        let state = account_quota::restore_account_state(&store);
        assert!(
            state.roster.is_some(),
            "the copy must carry the real persisted account roster"
        );
        let credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
        let routing_config = config_overrides::config_path();
        // The roster is non-secret metadata; never read the operator's secret
        // OpenCode auth file in a replay. Subscription policy remains live.
        let auth_path = empty_opencode_auth();
        let mut report = Vec::new();
        for (phase, cache) in [
            ("before_ordinary_catalog", &before_credentials),
            ("after_credential_planes", &credentials),
        ] {
            if phase == "after_credential_planes" {
                {
                    let mut guard = credentials.lock().unwrap();
                    guard.refresh_serving_methods(
                        &state,
                        &store.get_facts().unwrap(),
                        now_ms() as i64,
                    );
                    guard.mark_copy_listing_gap();
                }
                let mut catalog =
                    fusiform_catalog::FusiformCatalogConsumer::connect(&connection, &root)
                        .await
                        .expect("live Fusiform catalog");
                catalog
                    .dry_run_refresh(&store, Arc::clone(&credentials))
                    .await
                    .unwrap();
                let mut guard = credentials.lock().unwrap();
                guard.refresh_serving_methods(&state, &store.get_facts().unwrap(), now_ms() as i64);
                guard.mark_copy_listing_gap();
            }
            for iq in [70, 85] {
                let request = json!({"method": ROUTE_SELECT_OP, "params": { "executor": "broca",
                    "targetAgent":"mason", "requirements":{"iq":iq,"eq":40},
                    "substrate":"broca", "availableModels":[], "taskId":format!("billing-dry-run-{phase}-{iq}")
                }});
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
                    &Arc::new(Mutex::new(restore_quota_cache(&store))),
                    &store,
                    &Arc::new(Mutex::new(None)),
                    &routing_config,
                    &routing_config,
                    cache,
                    &auth_path,
                )
                .await
                .unwrap();
                let response = rx.recv().await.unwrap();
                assert_eq!(
                    response.header.ty,
                    FrameType::Response,
                    "{}",
                    String::from_utf8_lossy(&response.body)
                );
                let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
                let result = &value["result"];
                let ordinary_rates: Vec<_> = if phase == "before_ordinary_catalog" {
                    store.get_facts().unwrap().iter().map(|fact| json!({
                        "providerID":fact.provider_id, "modelID":fact.model_id,
                        "dimensionRates":prefrontal_routing_core::BasketDimension::ALL.into_iter().map(|dimension| (dimension.fact_key().to_string(), fact.basket_rate(dimension))).collect::<std::collections::BTreeMap<_,_>>(),
                    })).collect()
                } else {
                    Vec::new()
                };
                report.push(json!({"phase":phase, "iq":iq, "eq":40, "selected":result["selected"], "billingPlanes":result["billingPlanes"], "ordinaryCatalogRates":ordinary_rates}));
            }
        }
        std::fs::write(
            root.join("target/billing-dry-run/route-select.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        for entry in &report {
            println!(
                "{} IQ {}: {} cost={} pricing={}",
                entry["phase"],
                entry["iq"],
                entry["selected"]["routeKey"],
                entry["selected"]["score"]["objective"]["cost"],
                entry["selected"]["score"]["pricingMode"]
            );
        }
        println!("4 route.select dry runs; per-plane prices captured in target/billing-dry-run/route-select.json; non-roster providers have no live listing proof");
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
            let observation = model_resolution::usage_observation_from_store(facts, &store)
                .unwrap()
                .expect("recordable refusal");
            assert_eq!(
                store.record_usage_delivery(&observation).unwrap(),
                prefrontal_routing_core::route_state_store::UsageRecordResult {
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
                    prefrontal_routing_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
                ),
                rusqlite::params!["ephemeral", 0, 10_000],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(complete_rows_count, 0);

        // use_basket is never true when counters_producer is false
        assert!(!prefrontal_routing_core::use_basket_permitted(false, true));

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
    async fn mutation_nulling_output_or_reasoning_in_counter_deferral_branch_reddens() {
        // Gated on counters_producer = false
        let rig = Rig::new("counter-deferral-mutation");
        let store = Arc::new(rig.open_store());

        let mut params = usage_params("deferral-check");
        params["outputTokens"] = json!(100.0);
        params["reasoningTokens"] = json!(80.0);
        run_record_usage(&store, params).await;

        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        let (output, reasoning) = connection
            .query_row(
                "SELECT output_tokens, reasoning_tokens FROM model_route_usage WHERE id = 'deferral-check'",
                [],
                |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .unwrap();
        assert!(output.is_some());
        assert!(reasoning.is_some());
        assert_eq!(output, Some(180));
        assert_eq!(reasoning, Some(80));
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
                    prefrontal_routing_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
                ),
                rusqlite::params!["ephemeral", 0, 10_000],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(complete_ephemeral_count, 10);
        assert!(complete_ephemeral_count < 50);

        // use_basket cannot be true when class_producer is false
        assert!(!prefrontal_routing_core::use_basket_permitted(true, false));
    }

    #[tokio::test]
    async fn mutation_pooling_unclassified_rows_into_ephemeral_reddens() {
        // Gated on class_producer = false
        let rig = Rig::new("class-pooling-mutation");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        let mut p = usage_params("unclassified-msg");
        p["inputTokens"] = json!(100.0);
        p["cachedReadTokens"] = json!(20.0);
        p["cacheWriteTokens"] = json!(5.0);
        p["outputTokens"] = json!(50.0);
        p["reasoningTokens"] = json!(10.0);
        run_record_usage(&store, p).await;

        let unclassified_row_class: Option<String> = connection
            .query_row(
                "SELECT workload_class FROM model_route_usage WHERE id = 'unclassified-msg'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unclassified_row_class, None);
        assert_ne!(unclassified_row_class.as_deref(), Some("ephemeral"));
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
                    prefrontal_routing_store::WORKLOAD_CLASS_USAGE_SAMPLES_SQL
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
    async fn mutation_refusing_or_dropping_rows_lacking_account_key_reddens() {
        // Gated on account_producer = absent
        let rig = Rig::new("account-refusing-mutation");
        let store = Arc::new(rig.open_store());

        let mut p = usage_params("msg-without-account");
        p["outputTokens"] = json!(100.0);
        let outcome = run_record_usage(&store, p).await;
        assert_eq!(outcome["usage_recorded"], true);
        assert_eq!(usage_row_count(&rig, Some("msg-without-account")), 1);
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

    #[tokio::test]
    async fn mutation_writing_zero_for_whole_object_absence_reddens() {
        // Gated on whole-object absence
        let rig = Rig::new("mutation-whole-absence");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        let mut whole_absent = usage_params("mut-whole-absent");
        whole_absent.as_object_mut().unwrap().remove("outputTokens");
        whole_absent["reasoningTokens"] = serde_json::Value::Null;
        whole_absent["inputTokens"] = serde_json::Value::Null;
        whole_absent["cachedReadTokens"] = serde_json::Value::Null;
        whole_absent["cacheWriteTokens"] = serde_json::Value::Null;
        run_record_usage(&store, whole_absent).await;

        let (crd, inp) = connection
            .query_row(
                "SELECT cached_read_tokens, input_tokens FROM model_route_usage WHERE id = 'mut-whole-absent'",
                [],
                |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .unwrap();
        assert_ne!(crd, Some(0));
        assert_ne!(inp, Some(0));
        assert_eq!(crd, None);
        assert_eq!(inp, None);
    }

    #[tokio::test]
    async fn mutation_writing_null_for_authoritative_zero_cached_read_reddens() {
        // Gated on authoritative zero cached-read
        let rig = Rig::new("mutation-auth-zero");
        let store = Arc::new(rig.open_store());
        let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();

        let mut zero_cached = usage_params("mut-auth-zero");
        zero_cached["cachedReadTokens"] = json!(0.0);
        run_record_usage(&store, zero_cached).await;

        let stored_zero: Option<i64> = connection
            .query_row(
                "SELECT cached_read_tokens FROM model_route_usage WHERE id = 'mut-auth-zero'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_zero, Some(0));
        assert_ne!(stored_zero, None);
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
            prefrontal_routing_core::model_facts_seed()
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
        let operator_config = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(
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
        // A revoked OAuth grant (QTA maps Google's invalid_grant here, RFC 6749
        // permanent) never self-recovers; retaining the last healthy window
        // would keep routing to a dead account. Mutation: remove the class from
        // the dead-credential set and this reddens by name.
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
        // no_quota_reported is an ended plan (gateway affirms success, no plan
        // block) - QTA 4f6d048; the same truth-not-degradation family,
        // not trouble: retaining a last-healthy window for it manufactures a
        // provider that does not exist on this host. Classless degraded entries
        // (older QTA) and every other class keep the retention path above.
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
        // (older QTA) and every other class keep the retention path above.
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

    #[test]
    fn select_params_accepts_parent_session_id_acronym_alias() {
        let body: SelectBody = serde_json::from_value(json!({
            "params": {
                "requirements": { "iq": 90, "eq": 0 },
                "parentSessionID": "parent-session"
            }
        }))
        .unwrap();

        assert_eq!(
            body.params.parent_session_id.as_deref(),
            Some("parent-session")
        );
        assert_eq!(body.params.requirements.unwrap().iq, 90.0);
    }

    async fn run_select_wire_request(
        label: &str,
        params: serde_json::Value,
        facts: &[ModelFact],
    ) -> (Frame, Rig) {
        let rig = Rig::new(label);
        let response = run_select_against_rig(
            &rig,
            params,
            facts,
            &PathBuf::from("/nonexistent/prefrontal-routing.jsonc"),
        )
        .await;
        (response, rig)
    }

    /// Drive one `route.select` against an existing rig, so a test can control the
    /// operator config file the module reads (and edit it between selections).
    async fn run_select_against_rig(
        rig: &Rig,
        mut params: serde_json::Value,
        facts: &[ModelFact],
        operator_config_path: &Path,
    ) -> Frame {
        params
            .as_object_mut()
            .unwrap()
            .entry("executor")
            .or_insert(json!("broca"));
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let store = Arc::new(rig.open_store());
        for fact in facts {
            store.upsert_fact(fact, FactSource::Override).unwrap();
        }
        let request = json!({ "method": ROUTE_SELECT_OP, "params": params });
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
            &store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            operator_config_path,
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
    }

    #[tokio::test]
    async fn refusal_decision_row_carries_per_candidate_rejections() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = Rig::new("refusal-decision-evidence");
        let store = Arc::new(rig.open_store());
        let mut rejected = fact("p", "missing-vision");
        rejected.supports_vision = Some(false);
        store.upsert_fact(&rejected, FactSource::Override).unwrap();
        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "mason",
                "requirements": { "iq": 30, "eq": 0 },
                "requiredCapabilities": { "vision": true },
                "availableModels": ["p/missing-vision"],
                "taskId": "refusal-evidence"
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
            &Arc::new(Mutex::new(QuotaCache::default())),
            &store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().await.unwrap().header.ty, FrameType::Error);

        let decisions = store.get_decisions_for_task("refusal-evidence").unwrap();
        assert_eq!(decisions.len(), 1);
        let rejections: serde_json::Value =
            serde_json::from_str(&decisions[0].rejected_json).unwrap();
        let candidates = rejections
            .as_array()
            .expect("refusal rejections serialize as an array");
        assert!(
            !candidates.is_empty(),
            "refusal preserves every candidate rejection"
        );
        let detailed = candidates
            .iter()
            .filter(|entry| entry["routeKey"].as_str().is_some())
            .collect::<Vec<_>>();
        assert!(!detailed.is_empty());
        assert!(detailed
            .iter()
            .all(|entry| entry["reason"].as_str().is_some()));
        let explanation: serde_json::Value =
            serde_json::from_str(decisions[0].explanation_json.as_deref().unwrap()).unwrap();
        assert!(explanation["failureReason"].as_str().is_some());
    }

    #[tokio::test]
    async fn third_session_error_writes_backoff_active_into_refusal_decision_ledger() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
        const LAST_ERROR: &str =
            "Access to model denied. Please make sure you are eligible for using the model.";
        let rig = Rig::new("backoff-decision-evidence");
        let store = Arc::new(rig.open_store());
        for failure in 1..=3 {
            let decision_id = store
                .record_decision(&DecisionInput {
                    decision_id: Some(format!("dec-session-error-{failure}")),
                    task_id: Some(format!("failed-task-{failure}")),
                    target_agent: "mason".into(),
                    requirements_json: "{\"iq\":80,\"eq\":20}".into(),
                    selected_route_key: Some(ROUTE.into()),
                    selected_provider_id: Some("alibaba-token-plan".into()),
                    selected_model_id: Some("qwen3.8-max-preview".into()),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    anchor_version: ANCHOR_VERSION,
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &decision_id,
                    &DecisionOutcomeUpdate {
                        outcome: RouteOutcome::SessionError,
                        last_error: Some(LAST_ERROR.into()),
                        observed_at_ms: now_ms() as i64,
                        failure_backoff: FailureBackoffConfig::default(),
                    },
                )
                .unwrap();
        }

        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "mason",
                "requirements": { "iq": 80, "eq": 20 },
                "availableModels": [ROUTE],
                "taskId": "backoff-refusal-ledger"
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
            &Arc::new(Mutex::new(QuotaCache::default())),
            &store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().await.unwrap().header.ty, FrameType::Error);

        let decisions = store
            .get_decisions_for_task("backoff-refusal-ledger")
            .unwrap();
        let rejections: serde_json::Value =
            serde_json::from_str(&decisions[0].rejected_json).unwrap();
        let qwen = rejections
            .as_array()
            .unwrap()
            .iter()
            .find(|rejection| rejection["routeKey"] == ROUTE)
            .expect("qwen rejection is visible inline");
        assert_eq!(qwen["reason"], "backoff_active");
        assert!(qwen["detail"].as_str().unwrap().contains(LAST_ERROR));
        let explanation: serde_json::Value =
            serde_json::from_str(decisions[0].explanation_json.as_deref().unwrap()).unwrap();
        assert!(explanation["earliestReset"].as_f64().is_some());
    }

    #[tokio::test]
    async fn route_clear_failure_backoff_returns_prior_state_and_readmits_route() {
        const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
        const LAST_ERROR: &str = "provider returned HTTP 503 after dispatch";
        let rig = Rig::new("clear-failure-backoff");
        let store = Arc::new(rig.open_store());
        for failure in 1..=3 {
            let decision_id = store
                .record_decision(&DecisionInput {
                    decision_id: Some(format!("dec-provider-error-{failure}")),
                    task_id: Some(format!("failed-task-{failure}")),
                    target_agent: "mason".into(),
                    requirements_json: "{\"iq\":80,\"eq\":20}".into(),
                    selected_route_key: Some(ROUTE.into()),
                    selected_provider_id: Some("alibaba-token-plan".into()),
                    selected_model_id: Some("qwen3.8-max-preview".into()),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    anchor_version: ANCHOR_VERSION,
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &decision_id,
                    &DecisionOutcomeUpdate {
                        outcome: RouteOutcome::Error,
                        last_error: Some(LAST_ERROR.into()),
                        observed_at_ms: now_ms() as i64,
                        failure_backoff: FailureBackoffConfig::default(),
                    },
                )
                .unwrap();
        }

        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_CLEAR_FAILURE_BACKOFF_OP,
                "params": { "routeKey": ROUTE },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let prior = &body["result"]["prior"];
        assert_eq!(prior["consecutiveTerminalFailures"], 3);
        assert!(prior["failureBackoffUntil"].as_f64().is_some());
        assert!(prior["failureBackoffReason"]
            .as_str()
            .is_some_and(|reason| reason.contains(LAST_ERROR)));

        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "executor": "broca",
                    "targetAgent": "mason",
                    "requirements": { "iq": 80, "eq": 20 },
                    "availableModels": [ROUTE],
                    "taskId": "backoff-cleared-route-selection",
                },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["selected"]["routeKey"], ROUTE);
    }

    #[tokio::test]
    async fn route_clear_cooldown_readmits_walled_route_without_resetting_backoff() {
        const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
        let rig = Rig::new("clear-cooldown");
        let store = Arc::new(rig.open_store());
        let until = now_ms() as f64 + 4.0 * 24.0 * 60.0 * 60.0 * 1000.0;
        store
            .update_route_state(
                ROUTE,
                ROUTE,
                &prefrontal_routing_core::RouteStateUpdate {
                    cooldown_until: Some(until),
                    quota_exhausted_until: Some(until),
                    wall_class: Some(RouteWallClass::Quota),
                    ..Default::default()
                },
            )
            .unwrap();
        let decision_id = store
            .record_decision(&DecisionInput {
                selected_route_key: Some(ROUTE.into()),
                selected_provider_id: Some("alibaba-token-plan".into()),
                selected_model_id: Some("qwen3.8-max-preview".into()),
                target_agent: "mason".into(),
                requirements_json: "{}".into(),
                score_json: "{}".into(),
                rejected_json: "[]".into(),
                anchor_version: ANCHOR_VERSION,
                ..Default::default()
            })
            .unwrap();
        store
            .set_decision_outcome(
                &decision_id,
                &DecisionOutcomeUpdate {
                    outcome: RouteOutcome::Error,
                    last_error: Some("provider HTTP 503".into()),
                    observed_at_ms: now_ms() as i64,
                    failure_backoff: FailureBackoffConfig::default(),
                },
            )
            .unwrap();
        let before = store.build_snapshot(now_ms() as f64).unwrap().by_route[ROUTE].clone();
        let select = |task: &str| {
            json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "executor": "broca", "targetAgent": "mason", "requirements": { "iq": 80, "eq": 20 },
                    "availableModels": [ROUTE], "taskId": task },
            })
        };
        let before_select = run_route_wire_request(&store, select("before-clear")).await;
        assert_eq!(before_select.header.ty, FrameType::Response);
        let before_body: serde_json::Value = serde_json::from_slice(&before_select.body).unwrap();
        assert!(before_body["result"]["selected"]["explanation"]
            .to_string()
            .contains("break-glass"));
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_CLEAR_COOLDOWN_OP, "params": { "routeKey": ROUTE },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["routeKey"], ROUTE);
        assert_eq!(body["result"]["clearedCooldownUntil"], until);
        assert_eq!(body["result"]["clearedQuotaExhaustedUntil"], until);
        let after = store.build_snapshot(now_ms() as f64).unwrap().by_route[ROUTE].clone();
        assert_eq!(after.cooldown_until, None);
        assert_eq!(after.quota_exhausted_until, None);
        assert_eq!(after.wall_class, None);
        assert_eq!(after.failure_backoff_until, before.failure_backoff_until);
        assert_eq!(after.failure_backoff_reason, before.failure_backoff_reason);
        let response = run_route_wire_request(&store, select("after-clear")).await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["selected"]["routeKey"], ROUTE);
        assert!(!body["result"]["selected"]["explanation"]
            .to_string()
            .contains("break-glass"));
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

    #[tokio::test]
    async fn send_id_round_trips_launch_to_decision_row_or_stays_null_without_one() {
        let route = fact("p", "send-id-route");
        let (with_send, with_send_rig) = run_select_wire_request(
            "send-id-present",
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["p/send-id-route"],
                "taskId": "send-id-present",
                "sendID": "send-attempt-42"
            }),
            std::slice::from_ref(&route),
        )
        .await;
        assert_eq!(with_send.header.ty, FrameType::Response);
        assert_eq!(
            with_send_rig
                .open_store()
                .get_decisions_for_task("send-id-present")
                .unwrap()[0]
                .send_id
                .as_deref(),
            Some("send-attempt-42")
        );

        let (without_send, without_send_rig) = run_select_wire_request(
            "send-id-absent",
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["p/send-id-route"],
                "taskId": "send-id-absent"
            }),
            &[route],
        )
        .await;
        assert_eq!(without_send.header.ty, FrameType::Response);
        assert_eq!(
            without_send_rig
                .open_store()
                .get_decisions_for_task("send-id-absent")
                .unwrap()[0]
                .send_id,
            None
        );
    }

    #[tokio::test]
    async fn project_root_population_round_trips_through_route_select_write_path() {
        let route = fact("p", "join-identity-route");
        let (with_identity, with_identity_rig) = run_select_wire_request(
            "join-identity-present",
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["p/join-identity-route"],
                "taskId": "run-with-identity",
                "parentSessionID": "ses-reused",
                "harness": "pi",
                "projectRoot": "/repos/one"
            }),
            std::slice::from_ref(&route),
        )
        .await;
        assert_eq!(with_identity.header.ty, FrameType::Response);
        let connection =
            rusqlite::Connection::open(with_identity_rig.dir.join("store.db")).unwrap();
        let stored_identity: (Option<String>, Option<String>) = connection
            .query_row(
                "SELECT harness, project_root FROM model_route_decisions WHERE task_id='run-with-identity'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            stored_identity,
            (Some("pi".into()), Some("/repos/one".into()))
        );

        let (without_identity, without_identity_rig) = run_select_wire_request(
            "join-identity-absent",
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["p/join-identity-route"],
                "taskId": "run-without-identity"
            }),
            &[route],
        )
        .await;
        assert_eq!(without_identity.header.ty, FrameType::Response);
        let connection =
            rusqlite::Connection::open(without_identity_rig.dir.join("store.db")).unwrap();
        let absent_identity: (Option<String>, Option<String>) = connection
            .query_row(
                "SELECT harness, project_root FROM model_route_decisions WHERE task_id='run-without-identity'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(absent_identity, (None, None));
    }

    #[tokio::test]
    async fn only_billing_walled_route_returns_no_route_and_quota_control_relaxes() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = Rig::new("billing-wall-break-glass");
        let store = Arc::new(rig.open_store());
        let grok = fact("xai", "grok-4.6");
        store.upsert_fact(&grok, FactSource::Override).unwrap();
        let billing_error = "personal-team-blocked:spending-limit: You have run out of credits or need a Grok subscription";
        let wall_until = now_ms() as f64 + 30.0 * 60_000.0;
        store
            .update_route_state(
                "xai/grok-4.6",
                "xai/grok-4.6",
                &prefrontal_routing_core::RouteStateUpdate {
                    cooldown_until: Some(wall_until),
                    quota_exhausted_until: Some(wall_until),
                    wall_class: Some(RouteWallClass::Billing),
                    last_error: Some(billing_error.into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let request = |task_id: &str| {
            json!({
                "targetAgent": "mason",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["xai/grok-4.6"],
                "taskId": task_id
            })
        };

        let blocked = select_with_store(&store, request("billing-wall-blocked")).await;
        assert_eq!(blocked["earliestReset"], json!(wall_until));
        assert_eq!(blocked["earliestResetRoute"], "xai/grok-4.6");
        assert_eq!(blocked["blockingCauses"][0]["count"], 1);
        let decision = store
            .get_decisions_for_task("billing-wall-blocked")
            .unwrap()
            .remove(0);
        let explanation: serde_json::Value =
            serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            explanation["earliestResetRoute"],
            blocked["earliestResetRoute"]
        );
        assert_eq!(explanation["blockingCauses"], blocked["blockingCauses"]);
        let message = blocked["message"]
            .as_str()
            .unwrap_or_else(|| panic!("billing wall must return no_route: {blocked}"));
        assert!(message.contains("xai/grok-4.6"), "{message}");
        assert!(message.contains("provider xai"), "{message}");
        assert!(message.contains("billing wall"), "{message}");
        assert!(message.contains(billing_error), "{message}");

        store
            .update_route_state(
                "xai/grok-4.6",
                "xai/grok-4.6",
                &prefrontal_routing_core::RouteStateUpdate {
                    wall_class: Some(RouteWallClass::Quota),
                    last_error: Some("you have reached your usage limit".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let relaxed = select_with_store(&store, request("plain-quota-relaxed")).await;
        assert_eq!(
            relaxed["result"]["selected"]["routeKey"], "xai/grok-4.6",
            "{relaxed}"
        );
        assert!(relaxed["result"]["selected"]["explanation"][0]
            .as_str()
            .is_some_and(|line| line.contains("break-glass")));
    }

    /// Replay the 2026-09-20 17:09:27 xai/grok-4.6 handout: OpenAI is
    /// withheld from the same head and the only remaining route has the stored
    /// billing refusal. The corrected selector must explain both facts.
    #[tokio::test]
    async fn specimen_billing_grok_plus_head_withheld_openai_returns_no_route() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = Rig::new("billing-wall-specimen");
        let store = Arc::new(rig.open_store());
        for route in [fact("openai", "gpt-5.6-sol"), fact("xai", "grok-4.6")] {
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        let parent = "ses_313660571ffeZTsf4koSJwk50Q";
        let first = select_with_store(
            &store,
            json!({
                "targetAgent": "mason",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["openai/gpt-5.6-sol", "xai/grok-4.6"],
                "excludeRouteKeys": ["xai/grok-4.6"],
                "parentSessionID": parent,
                "taskId": "specimen-openai-refusal"
            }),
        )
        .await;
        assert_eq!(
            first["result"]["selected"]["routeKey"], "openai/gpt-5.6-sol",
            "{first}"
        );
        let decision_id = store
            .get_decisions_for_task("specimen-openai-refusal")
            .unwrap()
            .remove(0)
            .id;
        store
            .set_decision_outcome(
                &decision_id,
                &DecisionOutcomeUpdate {
                    outcome: RouteOutcome::SessionError,
                    last_error: Some("This content was flagged for possible cybersecurity risk. If you believe this is an error, contact us via the Trusted Access for Cyber program.".into()),
                    observed_at_ms: now_ms() as i64,
                    failure_backoff: FailureBackoffConfig::default(),
                },
            )
            .unwrap();

        let billing_error = "personal-team-blocked:spending-limit: You have run out of credits or need a Grok subscription. Add credits at https://grok.com/?_s=usage or upgrade at https://grok.com/supergrok.";
        let now = now_ms() as f64;
        store
            .update_route_state(
                "xai/grok-4.6",
                "xai/grok-4.6",
                &prefrontal_routing_core::RouteStateUpdate {
                    cooldown_until: Some(now + 30.0 * 60_000.0),
                    quota_exhausted_until: Some(now + 30.0 * 60_000.0),
                    wall_class: Some(RouteWallClass::Billing),
                    last_error: Some(billing_error.into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let replay = select_with_store(
            &store,
            json!({
                "targetAgent": "mason",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["openai/gpt-5.6-sol", "xai/grok-4.6"],
                "parentSessionID": parent,
                "taskId": "specimen-replay"
            }),
        )
        .await;
        let message = replay["message"]
            .as_str()
            .unwrap_or_else(|| panic!("specimen must return no_route: {replay}"));
        assert!(
            message.contains("withheld provider openai from this head"),
            "{message}"
        );
        assert!(message.contains("xai/grok-4.6"), "{message}");
        assert!(message.contains("provider xai"), "{message}");
        assert!(message.contains(billing_error), "{message}");
    }

    #[tokio::test]
    async fn typed_policy_refusal_excludes_only_the_retry_item_not_its_heads_other_items() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = Rig::new("item-policy-refusal");
        let store = Arc::new(rig.open_store());
        for model in ["sol", "astra"] {
            let mut route = fact("openai", model);
            route.scores.elo = 95.0;
            route.scores.speed = 200.0;
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        store
            .upsert_fact(&fact("anthropic", "opus"), FactSource::Override)
            .unwrap();
        let select = |label: &str, exclude: &[&str]| {
            json!({
                "targetAgent":"mason", "requirements":{"iq":30,"eq":0},
                "availableModels":["openai/sol","openai/astra","anthropic/opus"],
                "excludeRouteKeys":exclude, "parentSessionID":"same-head", "taskId":label
            })
        };
        let first = select_with_store(&store, select("refused-item-first", &["anthropic"])).await;
        assert_eq!(
            first["result"]["selected"]["model"]["providerID"], "openai",
            "{first}"
        );
        let decision = store
            .get_decisions_for_task("refused-item-first")
            .unwrap()
            .remove(0);
        store
            .set_decision_outcome(
                &decision.id,
                &DecisionOutcomeUpdate {
                    outcome: RouteOutcome::SessionError,
                    last_error: Some(
                        "provider_policy_refusal: provider openai: content_filter".into(),
                    ),
                    observed_at_ms: now_ms() as i64,
                    failure_backoff: FailureBackoffConfig::default(),
                },
            )
            .unwrap();
        let retry = select_with_store(&store, select("refused-item-retry", &["openai"])).await;
        assert_eq!(
            retry["result"]["selected"]["model"]["providerID"], "anthropic",
            "{retry}"
        );
        let other = select_with_store(&store, select("unrelated-item", &[])).await;
        assert_eq!(
            other["result"]["selected"]["model"]["providerID"], "openai",
            "{other}"
        );
    }

    // Legacy untyped refusals still use the bounded head-level exclusion window.
    #[tokio::test]
    async fn head_refused_on_content_policy_loses_that_provider_for_its_next_launch() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = Rig::new("head-content-policy-exclusion");
        let store = Arc::new(rig.open_store());
        // Both OpenAI routes outrank opus on every axis, so opus wins only when
        // BOTH are withheld: a route-only exclusion would leave sol to win.
        let mut sol = fact("openai", "sol");
        sol.scores.elo = 90.0;
        sol.scores.speed = 200.0;
        let mut astra = fact("openai", "astra");
        astra.scores.elo = 95.0;
        astra.scores.speed = 200.0;
        for route in [astra, sol, fact("anthropic", "opus")] {
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        let refused_head = "ses_refused_head";
        let control_head = "ses_control_head";
        let select = |parent: &str, label: &str, exclude: &[&str]| {
            json!({
                "targetAgent": "mason",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["openai/astra", "openai/sol", "anthropic/opus"],
                "excludeRouteKeys": exclude,
                "parentSessionID": parent,
                "taskId": label
            })
        };
        let record_failure = |label: &str, error: &str| {
            let decision_id = store.get_decisions_for_task(label).unwrap().remove(0).id;
            store
                .set_decision_outcome(
                    &decision_id,
                    &DecisionOutcomeUpdate {
                        outcome: RouteOutcome::SessionError,
                        last_error: Some(error.to_string()),
                        observed_at_ms: now_ms() as i64,
                        failure_backoff: FailureBackoffConfig::default(),
                    },
                )
                .unwrap();
        };

        // Both heads launch on OpenAI first, and both launches fail: one refused
        // by the provider's content policy, the other on a transport drop.
        let refused_first = select_with_store(
            &store,
            select(refused_head, "refused-first", &["anthropic/opus"]),
        )
        .await;
        assert_eq!(
            refused_first["result"]["selected"]["model"]["providerID"], "openai",
            "{refused_first}"
        );
        record_failure(
            "refused-first",
            "This content was flagged for possible cybersecurity risk. If you believe this is an error, contact us via the Trusted Access for Cyber program.",
        );
        let control_first = select_with_store(
            &store,
            select(control_head, "control-first", &["anthropic/opus"]),
        )
        .await;
        assert_eq!(
            control_first["result"]["selected"]["model"]["providerID"], "openai",
            "{control_first}"
        );
        record_failure("control-first", "stream closed with code 1006");

        // The refused head's next launch may not land on any OpenAI route, and the
        // decision says why. The control head keeps OpenAI: its failure was the
        // wire, not the words.
        let refused_next =
            select_with_store(&store, select(refused_head, "refused-next", &[])).await;
        assert_eq!(
            refused_next["result"]["selected"]["model"]["providerID"], "anthropic",
            "{refused_next}"
        );
        let explanation = refused_next["result"]["selected"]["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|line| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explanation.contains("withheld provider openai from this head")
                && explanation.contains("refused by the provider's content policy"),
            "{explanation}"
        );
        let control_next = select_with_store(
            &store,
            select(control_head, "control-next", &["anthropic/opus"]),
        )
        .await;
        assert_eq!(
            control_next["result"]["selected"]["model"]["providerID"], "openai",
            "{control_next}"
        );

        // When the withholding leaves the head no route at all (its other
        // providers operator-excluded, as on this desk today), the refusal must
        // say so, or it reads as a policy misconfiguration.
        let refused_nothing_left = select_with_store(
            &store,
            select(refused_head, "refused-nothing-left", &["anthropic/opus"]),
        )
        .await;
        let message = refused_nothing_left["message"]
            .as_str()
            .unwrap_or_else(|| panic!("expected a no-route error: {refused_nothing_left}"));
        assert!(
            message.contains("withheld provider openai from this head"),
            "{message}"
        );
    }

    /// Drive one `route.select` through the wire handler against an already-open
    /// store, for tests that must read and write decisions between selections.
    async fn select_with_store(
        store: &Arc<SqliteRouteStateStore>,
        mut params: serde_json::Value,
    ) -> serde_json::Value {
        params["executor"] = json!("broca");
        let request = json!({ "method": ROUTE_SELECT_OP, "params": params });
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
            &Arc::new(Mutex::new(QuotaCache::default())),
            store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        serde_json::from_slice(&response.body).unwrap()
    }

    #[tokio::test]
    async fn decision_rows_record_quota_signal_state_and_policy_revision() {
        let route = fact("p", "quota-none");
        let (none_response, none_rig) = run_select_wire_request(
            "quota-none-provenance",
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["p/quota-none"],
                "taskId": "quota-none-provenance"
            }),
            std::slice::from_ref(&route),
        )
        .await;
        assert_eq!(none_response.header.ty, FrameType::Response);
        let none_row = none_rig
            .open_store()
            .get_decisions_for_task("quota-none-provenance")
            .unwrap()
            .remove(0);
        let none_signal: serde_json::Value =
            serde_json::from_str(none_row.quota_signal_json.as_deref().unwrap()).unwrap();
        assert_eq!(none_signal["state"], "none");
        assert!(none_signal["reason"]
            .as_str()
            .is_some_and(|reason| reason.starts_with("quota signal: none")));
        assert!(none_row
            .policy_revision
            .as_deref()
            .is_some_and(|revision| revision.starts_with("sha256:")));

        let rig = Rig::new("quota-present-provenance");
        let store = Arc::new(rig.open_store());
        let quota_route = fact("openai", "quota-present");
        store
            .upsert_fact(&quota_route, FactSource::Override)
            .unwrap();
        let mut quota_cache = QuotaCache::default();
        apply_quota_refresh(
            &mut quota_cache,
            vec![usage(json!({
                "provider": "codex",
                "usage": { "primary": { "usedPercent": 75.0, "resetsAt": "2099-01-01T00:00:00Z", "windowMinutes": 60 } }
            }))],
            now_ms(),
        );
        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["openai/quota-present"],
                "taskId": "quota-present-provenance"
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
            &Arc::new(Mutex::new(quota_cache)),
            &store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().await.unwrap().header.ty, FrameType::Response);
        let present_row = store
            .get_decisions_for_task("quota-present-provenance")
            .unwrap()
            .remove(0);
        let present_signal: serde_json::Value =
            serde_json::from_str(present_row.quota_signal_json.as_deref().unwrap()).unwrap();
        assert_eq!(present_signal["state"], "present");
        assert_eq!(present_signal["healthyProviderCount"], 1);
        assert!(present_row.policy_revision.is_some());
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

    /// Two free routes that differ only in speed: with no quota reading the
    /// faster anthropic route wins the speed tiebreak, and only a quota reading
    /// that makes the openai capacity abundant moves the win to openai. A restart
    /// that loses the quota reading therefore flips the winner to anthropic.
    fn quota_decides_facts() -> Vec<ModelFact> {
        let mut luna = fact("openai", "restart-luna");
        luna.scores.speed = 10.0;
        let mut opus = fact("anthropic", "restart-opus");
        opus.scores.speed = 100.0;
        for route in [&mut luna, &mut opus] {
            route.cost_tier = None;
            route.cost_per_m_tok_out = Some(0.0);
        }
        vec![luna, opus]
    }

    /// One codex reading, mapped to openai, whose 5-hour window resets at
    /// `resets_at_ms` with nothing used.
    fn codex_window(resets_at_ms: u64) -> ProviderUsage {
        usage(json!({
            "provider": "codex",
            "usage": { "primary": {
                "usedPercent": 0.0,
                "resetsAt": rfc3339_utc(resets_at_ms),
                "windowMinutes": 300
            }}
        }))
    }

    /// Run one `route.select` between the two quota-decides routes and return the
    /// recorded winner with the decision's quota signal.
    async fn select_quota_decides(
        store: &SharedStore,
        quota_cache: QuotaCache,
        task_id: &str,
    ) -> (String, serde_json::Value) {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        // iq 70, not lower: both fixture routes sit at the request frontier, so
        // below demand 60 both would carry the same over-qualification term,
        // which lifts them out of the effectively-free bucket where the quota
        // abundance this test is about decides.
        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "explore",
                "requirements": { "iq": 70, "eq": 0 },
                "availableModels": ["openai/restart-luna", "anthropic/restart-opus"],
                "taskId": task_id
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
            &Arc::new(Mutex::new(quota_cache)),
            store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(
                &quota_decides_facts(),
            ))),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().await.unwrap().header.ty, FrameType::Response);
        let row = store.get_decisions_for_task(task_id).unwrap().remove(0);
        let signal = serde_json::from_str(row.quota_signal_json.as_deref().unwrap()).unwrap();
        (row.selected_route_key.unwrap(), signal)
    }

    /// One Antigravity account as Insula publishes it (null apiProvider, no
    /// primary window, every pool an `extraRateWindows` entry).
    fn antigravity_reading(account: &str, pools: &[(&str, f64, u64)]) -> ProviderUsage {
        let windows: Vec<serde_json::Value> = pools
            .iter()
            .map(|(id, used, resets_at_ms)| {
                json!({
                    "title": format!("{id} title"),
                    "id": id,
                    "window": {
                        "usedPercent": used,
                        "resetsAt": rfc3339_utc(*resets_at_ms),
                        "windowMinutes": if id.ends_with("-5h") { 300 } else { 10_080 }
                    }
                })
            })
            .collect();
        usage(json!({
            "provider": "antigravity",
            "apiProvider": null,
            "account": account,
            "usage": { "extraRateWindows": windows }
        }))
    }

    /// Drive one `route.select` shaped exactly like the spec dispatcher's slice
    /// worker request (core-module `spec/runtime.rs`: target agent `mason`,
    /// iq 70 / eq 0, the consult's available and subscription keys).
    async fn select_spec_slice(
        store: &SharedStore,
        quota_cache: QuotaCache,
        available: &[&str],
        task_id: &str,
    ) -> String {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "mason",
                "requirements": { "iq": 70, "eq": 0 },
                "availableModels": available,
                "excludeRouteKeys": [],
                "subscriptionKeys": available,
                "preferPromptCache": false,
                "taskId": task_id,
                "parentSessionId": "spec-parent"
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
            &Arc::new(Mutex::new(quota_cache)),
            store,
            &Arc::new(Mutex::new(None)),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        store
            .get_decisions_for_task(task_id)
            .unwrap()
            .remove(0)
            .selected_route_key
            .unwrap()
    }

    /// Live 2026-10-02: every Antigravity account had its Gemini pool spent (one
    /// on `gemini-5h`, one on `gemini-weekly`), Insula had published both, and
    /// spec-slice masons still went to `google/antigravity-gemini-3.8-flash` and
    /// failed at start. The Insula reading alone must wall the route on the
    /// spec dispatcher's selection path.
    #[tokio::test]
    async fn spec_slice_selection_skips_a_walled_antigravity_route() {
        const ANTIGRAVITY: &str = "google/antigravity-gemini-3.8-flash";
        const ALTERNATIVE: &str = "openai/spec-alternative";
        let mut antigravity = fact("google", "antigravity-gemini-3.8-flash");
        antigravity.cost_per_m_tok_out = Some(0.0);
        antigravity.cost_tier = None;
        let mut alternative = fact("openai", "spec-alternative");
        alternative.cost_per_m_tok_out = Some(10.0);
        for route in [&mut antigravity, &mut alternative] {
            route.scores.elo = 1_900.0;
        }
        let rig = Rig::new("spec-slice-antigravity-wall");
        let store: SharedStore = Arc::new(rig.open_store());
        for route in [&antigravity, &alternative] {
            store.upsert_fact(route, FactSource::Override).unwrap();
        }
        let available = [ANTIGRAVITY, ALTERNATIVE];

        assert_eq!(
            select_spec_slice(&store, QuotaCache::default(), &available, "spec-baseline").await,
            ANTIGRAVITY,
            "fixture: without a quota reading the cheaper Antigravity route wins"
        );

        let now = now_ms();
        let five_hour_reset = now + 74 * 60_000;
        let mut walled = QuotaCache::default();
        apply_quota_refresh(
            &mut walled,
            vec![
                antigravity_reading(
                    "heidi",
                    &[
                        ("gemini-weekly", 29.4, now + 4 * 86_400_000),
                        ("gemini-5h", 100.0, five_hour_reset),
                        ("3p-5h", 0.0, now + 3_600_000),
                    ],
                ),
                antigravity_reading("grace", &[("gemini-weekly", 100.0, now + 6 * 86_400_000)]),
            ],
            now,
        );
        assert_eq!(
            select_spec_slice(&store, walled, &available, "spec-walled").await,
            ALTERNATIVE,
            "every usable Antigravity account is spent on the gemini pool"
        );
    }

    #[tokio::test]
    async fn restarted_module_selects_the_same_quota_winner_from_the_persisted_snapshot() {
        let rig = Rig::new("quota-snapshot-restart");
        let store: SharedStore = Arc::new(rig.open_store());
        for route in quota_decides_facts() {
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        let fresh_boot = restore_quota_cache(&store);
        assert!(fresh_boot.windows.is_empty() && fresh_boot.restored_from_ms.is_none());

        // Control: with no reading the speed tiebreak picks anthropic, so the
        // winners below are decided by the quota reading and nothing else.
        let (no_reading, _) =
            select_quota_decides(&store, QuotaCache::default(), "restart-no-reading").await;
        assert_eq!(no_reading, "anthropic/restart-opus");

        // Before the restart: one accepted poll, persisted as the refresh loop does.
        let observed_at_ms = now_ms();
        let mut live = QuotaCache::default();
        apply_quota_refresh(
            &mut live,
            vec![codex_window(observed_at_ms + 75 * 60_000)],
            observed_at_ms,
        );
        persist_quota_snapshot(&store, &live.windows, observed_at_ms);
        let (before, before_signal) = select_quota_decides(&store, live, "restart-before").await;
        assert_eq!(before, "openai/restart-luna");
        assert!(before_signal.get("restoredFromMs").is_none());

        // Restart: a fresh module over the same database, before any poll.
        drop(store);
        let store: SharedStore = Arc::new(rig.open_store());
        let restored = restore_quota_cache(&store);
        assert_eq!(restored.restored_from_ms, Some(observed_at_ms));
        assert!(!restored.available, "no poll has reached insula yet");
        let (after, after_signal) = select_quota_decides(&store, restored, "restart-after").await;
        assert_eq!(
            after, before,
            "a restart must not flip a quota-decided winner"
        );
        assert_eq!(after_signal["state"], "present");
        assert_eq!(after_signal["restoredFromMs"], json!(observed_at_ms));
    }

    #[tokio::test]
    async fn restored_snapshot_whose_windows_have_all_reset_selects_as_with_no_reading() {
        let rig = Rig::new("quota-snapshot-reset");
        let store: SharedStore = Arc::new(rig.open_store());
        for route in quota_decides_facts() {
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        // Accepted six hours ago; its only window reset an hour ago.
        let now = now_ms();
        let observed_at_ms = now - 6 * 60 * 60_000;
        let windows = vec![codex_window(now - 60 * 60_000)];
        persist_quota_snapshot(&store, &windows, observed_at_ms);

        let (no_reading, _) =
            select_quota_decides(&store, QuotaCache::default(), "reset-no-reading").await;
        // The comparison a restored reading must match: a process that kept the
        // same windows in memory, as the cache does through an insula outage,
        // evaluates their resets against the current clock.
        let mut kept_in_memory = QuotaCache::default();
        apply_quota_refresh(&mut kept_in_memory, windows, observed_at_ms);
        let (in_memory, _) = select_quota_decides(&store, kept_in_memory, "reset-in-memory").await;

        drop(store);
        let store: SharedStore = Arc::new(rig.open_store());
        let (after, after_signal) =
            select_quota_decides(&store, restore_quota_cache(&store), "reset-after").await;
        assert_eq!(no_reading, "anthropic/restart-opus");
        assert_eq!(
            after, no_reading,
            "a window past its reset contributes nothing"
        );
        assert_eq!(after, in_memory);
        assert_eq!(after_signal["signals"], json!([]));
        assert_eq!(after_signal["restoredFromMs"], json!(observed_at_ms));
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

    /// Selection must not keep choosing on a snapshot that stopped advancing.
    /// Within the bound a failed refresh keeps the last reading (a transient
    /// blip must not read as free quota); past it the reading is ignored, every
    /// provider is quota-unknown, and the decision says so with the age.
    #[tokio::test]
    async fn a_stale_quota_snapshot_neither_walls_nor_steers_and_the_decision_says_so() {
        let rig = Rig::new("quota-snapshot-stale");
        let store: SharedStore = Arc::new(rig.open_store());
        // The quota-decides pair with the speeds swapped: openai wins the speed
        // tiebreak when quota says nothing, so only a quota wall moves it.
        for mut route in quota_decides_facts() {
            route.scores.speed = if route.provider_id == "openai" {
                100.0
            } else {
                10.0
            };
            store.upsert_fact(&route, FactSource::Override).unwrap();
        }
        let (no_reading, _) =
            select_quota_decides(&store, QuotaCache::default(), "stale-no-reading").await;
        assert_eq!(no_reading, "openai/restart-luna");

        // Control: a failing refresh 14 minutes old is inside the bound, so the
        // wall it last read still holds.
        let (fresh, fresh_signal) = select_quota_decides(
            &store,
            walled_codex_cache_aged(QUOTA_STALE_AFTER_MS - 60_000),
            "stale-within-bound",
        )
        .await;
        assert_eq!(fresh, "anthropic/restart-opus", "{fresh_signal}");
        assert_eq!(fresh_signal["state"], "present");

        let (stale, stale_signal) = select_quota_decides(
            &store,
            walled_codex_cache_aged(QUOTA_STALE_AFTER_MS + 60_000),
            "stale-past-bound",
        )
        .await;
        assert_eq!(
            stale, no_reading,
            "a wall read from a stale snapshot must not move selection: {stale_signal}"
        );
        assert_eq!(stale_signal["state"], "stale");
        assert!(stale_signal["ageMs"].as_u64().unwrap() > QUOTA_STALE_AFTER_MS);
        assert_eq!(stale_signal["staleAfterMs"], json!(QUOTA_STALE_AFTER_MS));
        assert_eq!(
            stale_signal["lastError"],
            "usage.get: json: missing field `remaining`"
        );
        let explanation = store
            .get_decisions_for_task("stale-past-bound")
            .unwrap()
            .remove(0)
            .explanation_json
            .unwrap();
        assert!(
            explanation.contains("quota signal: stale (last successful refresh 16m ago")
                && explanation.contains("missing field `remaining`"),
            "the decision explanation names the stale snapshot and why: {explanation}"
        );
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
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: vec![
                    prefrontal_ckcred_client::ListedCredential {
                        id: "fixture-mapped".into(),
                        categories: vec!["llm-provider".into()],
                        state: "active".into(),
                        provider_ids: vec!["anthropic".into()],
                        ..Default::default()
                    },
                    prefrontal_ckcred_client::ListedCredential {
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

    #[tokio::test]
    async fn route_select_reports_empty_and_ignores_legacy_policy_exclusions() {
        let (empty, _empty_rig) = run_select_wire_request(
            "empty-launch-availability",
            json!({
                "targetAgent": "explore",
                "availableModels": [],
                "providerAuthTypes": {}
            }),
            &[],
        )
        .await;
        assert_eq!(empty.header.ty, FrameType::Error);
        let empty_body: serde_json::Value = serde_json::from_slice(&empty.body).unwrap();
        assert_eq!(empty_body["code"], "no_available_model");
        assert!(empty_body["message"]
            .as_str()
            .unwrap()
            .contains("no connected providers detected"));

        let route = fact("openai", "gpt-5.5");
        let (excluded, _excluded_rig) = run_select_wire_request(
            "policy-excluded-launch-availability",
            json!({
                "targetAgent": "explore",
                "availableModels": ["openai/gpt-5.5"],
                "routingPolicy": { "exclude": ["openai"] },
                "providerAuthTypes": {}
            }),
            &[route],
        )
        .await;
        assert_eq!(excluded.header.ty, FrameType::Response);
        let excluded_body: serde_json::Value = serde_json::from_slice(&excluded.body).unwrap();
        assert_eq!(
            excluded_body["result"]["selected"]["routeKey"],
            "openai/gpt-5.5"
        );
    }

    #[tokio::test]
    async fn retired_argon_alias_cannot_route_from_stale_host_availability_after_cleanup() {
        let rig = Rig::new("retired-argon-alias");
        let store = Arc::new(rig.open_store());
        let route = fact("google", "antigravity-gemini-4-argon-high");
        store.upsert_fact(&route, FactSource::Override).unwrap();
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute(
                "UPDATE model_facts SET fusiform_present = 0, facts_source = 'fusiform'
                 WHERE route_key = 'google/antigravity-gemini-4-argon-high'",
                [],
            )
            .unwrap();
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute_batch(include_str!(
                "../../prefrontal-routing-store/migrations/057_prune_absent_fusiform_facts.sql"
            ))
            .unwrap();
        let operator_config = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(
            &operator_config,
            r#"{ "model_routing": { "exclude": { "all": ["google/antigravity-gemini-4-argon"] } } }"#,
        )
        .unwrap();
        let frame = run_route_wire_request_with_operator_config(
            &store,
            json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "targetAgent": "explore",
                    "requirements": {"iq": 30, "eq": 0},
                    "availableModels": ["google/antigravity-gemini-4-argon-high"],
                    "providerAuthTypes": {}
                }
            }),
            &operator_config,
        )
        .await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert!(
            body.pointer("/result/selected").is_none(),
            "a stale alias cannot bypass the canonical model exclusion: {body}"
        );
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

    fn rejection_reason(body: &serde_json::Value, route_key: &str) -> Option<String> {
        body["result"]["selected"]["rejected"]
            .as_array()?
            .iter()
            .find(|rejected| rejected["routeKey"] == route_key)?["reason"]
            .as_str()
            .map(str::to_owned)
    }

    #[tokio::test]
    async fn routing_owned_exclude_wins_when_plugin_and_routing_files_differ() {
        // Every selection minted inside the daemon sends no routingPolicy at all.
        // Before the module read the operator's config itself, those selections saw
        // an empty exclude list and picked excluded routes.
        let rig = Rig::new("operator-baseline-fence");
        let facts = [
            fact("anthropic", "claude-opus-4-8"),
            fact("openai", "gpt-5.5"),
        ];
        let operator_config = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(
            &operator_config,
            r#"{ "model_routing": { "exclude": {"all": ["anthropic"]} } }"#,
        )
        .unwrap();
        std::fs::write(
            rig.dir.join("prefrontal.jsonc"),
            r#"{ "model_routing": { "exclude": {"all": ["openai"]} } }"#,
        )
        .unwrap();
        set_config_mtime(&operator_config, 1_700_000_000);
        let select_params = |routing_policy: Option<serde_json::Value>| {
            let mut params = json!({
                "targetAgent": "explore",
                "requirements": { "iq": 30, "eq": 0 },
                "availableModels": ["anthropic/claude-opus-4-8", "openai/gpt-5.5"],
                "providerAuthTypes": {},
                "taskId": "operator-baseline-fence"
            });
            if let Some(routing_policy) = routing_policy {
                params["routingPolicy"] = routing_policy;
            }
            params
        };

        let baseline =
            run_select_against_rig(&rig, select_params(None), &facts, &operator_config).await;
        assert_eq!(baseline.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&baseline.body).unwrap();
        assert_eq!(body["result"]["selected"]["routeKey"], "openai/gpt-5.5");
        assert_eq!(
            rejection_reason(&body, "anthropic/claude-opus-4-8").as_deref(),
            Some("excluded_by_operator"),
            "an operator-fenced route reports the fence, not a generic unavailability"
        );

        // A legacy caller cannot add an exclusion to routing-owned policy.
        let negated = run_select_against_rig(
            &rig,
            select_params(Some(json!({ "exclude": ["openai"] }))),
            &facts,
            &operator_config,
        )
        .await;
        let negated_body: serde_json::Value = serde_json::from_slice(&negated.body).unwrap();
        assert_eq!(
            negated_body["result"]["selected"]["routeKey"],
            "openai/gpt-5.5"
        );
        assert_eq!(
            rejection_reason(&negated_body, "anthropic/claude-opus-4-8").as_deref(),
            Some("excluded_by_operator")
        );

        // A caller-supplied route key keeps reporting as the caller's own exclusion,
        // so a reroute after failure stays distinguishable from the standing fence.
        let caller_excluded = run_select_against_rig(
            &rig,
            {
                let mut params = select_params(None);
                params["excludeRouteKeys"] = json!(["openai/gpt-5.5"]);
                params["availableModels"] = json!(["openai/gpt-5.5", "openai/gpt-5.4"]);
                params
            },
            &[fact("openai", "gpt-5.4")],
            &operator_config,
        )
        .await;
        let caller_body: serde_json::Value = serde_json::from_slice(&caller_excluded.body).unwrap();
        assert_eq!(
            rejection_reason(&caller_body, "openai/gpt-5.5").as_deref(),
            Some("excluded")
        );

        // The decision records only the routing-owned policy revision.
        let explanation = body["result"]["selected"]["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|line| line.as_str())
            .find(|line| line.starts_with("policy sources:"))
            .expect("the selection explanation names its policy sources")
            .to_string();
        assert!(
            explanation.contains("operator sha256:")
                && explanation.contains("1 exclude entries")
                && !explanation.contains("caller"),
            "unexpected policy-source line: {explanation}"
        );

        // Live edit: a new exclude list takes effect on the next selection, no restart.
        std::fs::write(
            &operator_config,
            r#"{ "model_routing": { "exclude": {"all": ["openai"]} } }"#,
        )
        .unwrap();
        set_config_mtime(&operator_config, 1_700_000_001);
        config_overrides::settle_live_config(&operator_config);
        let reloaded =
            run_select_against_rig(&rig, select_params(None), &facts, &operator_config).await;
        let reloaded_body: serde_json::Value = serde_json::from_slice(&reloaded.body).unwrap();
        assert_eq!(
            reloaded_body["result"]["selected"]["routeKey"], "anthropic/claude-opus-4-8",
            "the edited exclude list is in force without a module restart"
        );
        assert_eq!(
            rejection_reason(&reloaded_body, "openai/gpt-5.5").as_deref(),
            Some("excluded_by_operator")
        );
    }

    #[tokio::test]
    async fn route_select_accepts_but_ignores_legacy_caller_policy() {
        let routes = [
            fact("openai", "gpt-5.5"),
            fact("opencode-go", "deepseek-v4-flash"),
            fact("opencode-go", "minimax-m2.5"),
        ];
        let (response, _rig) = run_select_wire_request(
            "raw-launch-policy",
            json!({
                "targetAgent": "mason",
                "requestedRequirements": {
                    "iq": 80,
                    "eq": 30,
                    "caps": { "vision": true, "minContext": 200_000 }
                },
                "availableModels": [
                    "openai/gpt-5.5",
                    "opencode-go/minimax-m2.5",
                    "opencode-go/deepseek-v4-flash"
                ],
                "excludeRouteKeys": [],
                "routingPolicy": {
                    "exclude": ["openai/gpt-5.5"],
                    "subscription": ["opencode-go", "-opencode-go/deepseek-v4-flash"]
                },
                "providerAuthTypes": { "openai": "oauth", "opencode-go": "api" }
            }),
            &routes,
        )
        .await;

        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"]["resolvedInput"],
            json!({
                "requirements": { "iq": 80.0, "eq": 30.0 },
                "requiredCapabilities": { "vision": true, "minContext": 200_000.0 },
                "availableModels": [
                    "openai/gpt-5.5",
                    "opencode-go/deepseek-v4-flash",
                    "opencode-go/minimax-m2.5"
                ],
                "subscriptionKeys": [
                    "openai/gpt-5.5",
                    "opencode-go/deepseek-v4-flash",
                    "opencode-go/minimax-m2.5"
                ]
            })
        );
    }

    /// Live-shaped head-to-head between two routes on one subscription. Both are
    /// measured below the 50 tok/s speed floor, so both speed scores are 0 and the
    /// cheaper luna must win on metered cost. The sol window also holds one
    /// runaway generation (128,000 tokens over 2,305,748 ms, run to the output
    /// limit); if that one sample set sol's observed speed above the floor, sol
    /// would win the speed tie-break instead.
    #[tokio::test]
    async fn explore_head_to_head_is_not_flipped_by_one_runaway_sample() {
        let mut luna = fact("openai", "gpt-6-luna");
        luna.scores.elo = 1641.0;
        luna.cost_per_m_tok_out = Some(0.5);
        let mut sol = fact("openai", "gpt-6-sol");
        sol.scores.elo = 1851.0;
        sol.cost_per_m_tok_out = Some(10.0);

        let rig = Rig::new("explore-runaway-sample");
        {
            let store = rig.open_store();
            let observed_at = now_ms().saturating_sub(600_000) as f64;
            let record = |id: String, route_key: &str, output: f64, duration: f64, at: f64| {
                store
                    .record_usage_observation(&UsageObservation {
                        id,
                        route_key: route_key.into(),
                        capacity_key: route_key.into(),
                        provider_id: "openai".into(),
                        input_tokens: None,
                        output_tokens: output,
                        reasoning_tokens: None,
                        cached_read_tokens: None,
                        cache_write_tokens: None,
                        duration_ms: duration,
                        workload_class: None,
                        account_key: None,
                        observed_at: at,
                    })
                    .unwrap();
            };
            // Luna: about 40.9 tok/s on every sample.
            for index in 0..20 {
                record(
                    format!("luna-{index}"),
                    "openai/gpt-6-luna",
                    365.0,
                    8_930.0,
                    observed_at + index as f64,
                );
            }
            // Sol: ordinary samples spread over 36..45 tok/s, plus the runaway.
            for index in 0..19 {
                let speed = 36.0 + index as f64 * 0.5;
                record(
                    format!("sol-{index}"),
                    "openai/gpt-6-sol",
                    570.0,
                    (570.0 / speed * 1000.0).round(),
                    observed_at + index as f64,
                );
            }
            record(
                "sol-runaway".into(),
                "openai/gpt-6-sol",
                128_000.0,
                2_305_748.0,
                observed_at + 100.0,
            );
        }

        let response = run_select_against_rig(
            &rig,
            json!({
                "targetAgent": "explore",
                "requirements": { "iq": 50, "eq": 0 },
                "availableModels": ["openai/gpt-6-luna", "openai/gpt-6-sol"],
                "taskId": "explore-runaway-sample",
                "providerAuthTypes": { "openai": "oauth" }
            }),
            &[luna, sol],
            &PathBuf::from("/nonexistent/prefrontal-routing.jsonc"),
        )
        .await;

        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["routeKey"], "openai/gpt-6-luna",
            "{body:#}"
        );
    }

    #[tokio::test]
    async fn route_select_applies_specialist_default_and_rejects_implicit_mason_demand() {
        let route = fact("p", "default-vector");
        let (explore, _explore_rig) = run_select_wire_request(
            "explore-default-vector",
            json!({
                "targetAgent": "explore",
                "availableModels": ["p/default-vector"],
                "providerAuthTypes": {}
            }),
            std::slice::from_ref(&route),
        )
        .await;
        assert_eq!(explore.header.ty, FrameType::Response);
        let explore_body: serde_json::Value = serde_json::from_slice(&explore.body).unwrap();
        assert_eq!(
            explore_body["result"]["resolvedInput"]["requirements"],
            json!({ "iq": 30.0, "eq": 0.0 })
        );

        let (mason, _mason_rig) = run_select_wire_request(
            "mason-explicit-required",
            json!({
                "targetAgent": "mason",
                "availableModels": ["p/default-vector"],
                "providerAuthTypes": {}
            }),
            &[route],
        )
        .await;
        assert_eq!(mason.header.ty, FrameType::Error);
        let mason_body: serde_json::Value = serde_json::from_slice(&mason.body).unwrap();
        assert_eq!(mason_body["code"], "invalid_requirements");
        assert!(mason_body["message"]
            .as_str()
            .unwrap()
            .contains("requires explicit model_requirements"));
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
    async fn wire_demand_uses_the_caller_0_to_100_scale_without_rescaling() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        async fn select(demand: f64) -> (Frame, Rig) {
            let rig = Rig::new(&format!("wire-demand-{demand}"));
            let store = Arc::new(rig.open_store());
            let mut route = fact("p", "fact-53");
            route.scores.elo = 53.0;
            store.upsert_fact(&route, FactSource::Override).unwrap();
            let request = json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "executor": "broca",
                    "targetAgent": "mason",
                    "requirements": { "iq": demand, "eq": 0 },
                    "availableModels": ["p/fact-53"]
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
            let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
            let min_band = Arc::new(Mutex::new(Some(0.9)));
            let (tx, mut rx) = mpsc::channel(1);
            handle_select_request(
                frame,
                1,
                &tx,
                &quota_cache,
                &store,
                &min_band,
                Path::new("/nonexistent/prefrontal-routing.jsonc"),
                Path::new("/nonexistent/prefrontal-routing.jsonc"),
                &empty_broca_credentials(),
                &empty_opencode_auth(),
            )
            .await
            .unwrap();
            (rx.recv().await.unwrap(), rig)
        }

        let (low, _low_rig) = select(8.0).await;
        let (high, _high_rig) = select(80.0).await;
        assert_eq!(low.header.ty, FrameType::Response);
        assert_eq!(high.header.ty, FrameType::Response);
        let low_body: serde_json::Value = serde_json::from_slice(&low.body).unwrap();
        let high_body: serde_json::Value = serde_json::from_slice(&high.body).unwrap();
        assert!(low_body["result"]["selected"]["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .any(|line| line.as_str().unwrap().contains("need 8/100")));
        assert!(high_body["result"]["selected"]["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .any(|line| line.as_str().unwrap().contains("need 80/100")));
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
                    Path::new("/nonexistent/prefrontal-routing.jsonc"),
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
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
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
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
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

    fn broca_select_request(
        available_models: &[&str],
        substrate: Option<&str>,
    ) -> serde_json::Value {
        let mut params = json!({
            "targetAgent": "explore",
            "requirements": {"iq": 30, "eq": 0},
            "availableModels": available_models,
        });
        if let Some(substrate) = substrate {
            params["substrate"] = json!(substrate);
        }
        json!({ "method": ROUTE_SELECT_OP, "params": params })
    }

    #[tokio::test]
    async fn broca_runner_maps_gateway_and_omitted_availability_matches_explicit() {
        let rig = Rig::new("broca-runner");
        let store = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("opencode-go", "m-native-fast"), FactSource::Override)
            .unwrap();
        let broca_credentials = mapped_broca_credentials("opencode-go");
        let mut results = Vec::new();
        for omitted in [false, true] {
            let mut request = broca_select_request(&["opencode-go/m-native-fast"], Some("broca"));
            if omitted {
                request["params"]
                    .as_object_mut()
                    .unwrap()
                    .remove("availableModels");
            }
            let response = run_route_wire_request_with_paths(
                &store,
                request,
                Path::new("/nonexistent/prefrontal-routing.jsonc"),
                &broca_credentials,
            )
            .await;
            assert_eq!(response.header.ty, FrameType::Response);
            results.push(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"]
                    .clone(),
            );
        }
        assert_eq!(results[0]["selected"], results[1]["selected"]);
        assert_eq!(
            results[0]["resolvedInput"]["availableModels"],
            json!(["opencode-go/m-native-fast"])
        );
        assert_eq!(results[1]["resolvedInput"]["availableModels"], json!([]));
        assert_eq!(
            results[1]["runner"],
            json!({"provider":"opencode", "model":"m-native-fast"})
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

    /// Core's own classifier, wake author, and comment reviewer send through
    /// Broca and carry no host list. Naming substrate `broca` judges the
    /// selection against Broca's availability, not the caller's list: a served
    /// catalog route wins although the caller's list does not name it. The same
    /// request without the substrate keeps the caller's list, so the substrate
    /// field is the only difference between the halves.
    #[tokio::test]
    async fn a_broca_route_select_reads_brocas_availability_not_the_callers_list() {
        let rig = Rig::new("select-substrate-broca");
        let store = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("livebroca", "m-new"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("hostonly", "m-old"), FactSource::Override)
            .unwrap();
        let broca_credentials = mapped_broca_credentials("livebroca");
        let run = |substrate: Option<&'static str>| {
            let store = Arc::clone(&store);
            let broca_credentials = Arc::clone(&broca_credentials);
            async move {
                let response = run_route_wire_request_with_paths(
                    &store,
                    broca_select_request(&["hostonly/m-old"], substrate),
                    Path::new("/nonexistent/prefrontal-routing.jsonc"),
                    &broca_credentials,
                )
                .await;
                assert_eq!(response.header.ty, FrameType::Response);
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"]
                    .clone()
            }
        };

        let broca = run(Some("broca")).await;
        let available: Vec<&str> = broca["resolvedInput"]["availableModels"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert_eq!(
            available,
            ["hostonly/m-old"],
            "the reply retains only the caller's list"
        );
        assert_eq!(broca["selected"]["routeKey"], "livebroca/m-new");
        assert!(!decision_explanation(&store, &broca).contains("availability unknown"));

        let host = run(None).await;
        assert_eq!(host["selected"]["routeKey"], "hostonly/m-old");
        assert_eq!(
            host["resolvedInput"]["availableModels"],
            json!(["hostonly/m-old"])
        );
    }

    /// With Broca's reach unknown the selection still routes, over every route the catalog serves, and the
    /// recorded decision says the availability was unknown; a route the catalog
    /// has withdrawn stays out, because nothing serves it.
    #[tokio::test]
    async fn a_broca_route_select_with_unknown_reach_routes_over_served_rows_and_says_so() {
        let rig = Rig::new("select-substrate-unknown");
        let store = Arc::new(rig.open_store());
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute("DELETE FROM model_facts", [])
            .unwrap();
        store
            .upsert_fact(&fact("livebroca", "m-new"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("retired", "m-gone"), FactSource::Override)
            .unwrap();
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute(
                "UPDATE model_facts SET fusiform_present = 0 WHERE route_key = 'retired/m-gone'",
                [],
            )
            .unwrap();

        let response = run_route_wire_request_with_paths(
            &store,
            broca_select_request(&[], Some("broca")),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let result =
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone();
        let available: Vec<&str> = result["resolvedInput"]["availableModels"]
            .as_array()
            .expect("an unknown reach still routes")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert!(
            available.is_empty(),
            "Broca-served routes never enter the caller's resolved list: {available:?}"
        );
        assert_eq!(result["selected"]["routeKey"], "livebroca/m-new");
        assert!(decision_explanation(&store, &result)
            .contains("broca reachability unknown: credential listing unavailable"));
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
        let policy = prefrontal_routing_core::PanelPolicy {
            package_tokens: None,
            largest_package_ceiling: None,
            replacements: Vec::new(),
            selection: "class_policy".into(),
            seated_variants: vec![prefrontal_routing_core::SeatedVariant {
                slot: 0,
                route_key: "openai/gpt-5.6-sol-pro".into(),
                variant: prefrontal_routing_core::SeatedSiblingVariant::Pro,
                reasoning_variant: None,
                reasoning_variant_source: None,
                reasoning_variant_fallbacks: Vec::new(),
            }],
            sibling_collapses: Vec::new(),
            ..Default::default()
        };
        let panel = SelectedPanel {
            availability_rejections: Vec::new(),
            members: Vec::new(),
            runner_up: Vec::new(),
            shortfall: None,
            policy: Some(policy),
        };
        let provenance = DecisionProvenance {
            quota_signal_json: r#"{"state":"none","reason":"quota signal: none"}"#.into(),
            policy_revision: "test-policy".into(),
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
            availability_rejections: Vec::new(),
            members: Vec::new(),
            runner_up: Vec::new(),
            shortfall: None,
            policy: Some(prefrontal_routing_core::PanelPolicy {
                selection: "class_policy".into(),
                seated_variants: Vec::new(),
                sibling_collapses: Vec::new(),
                package_tokens: Some(227_000),
                largest_package_ceiling: Some(460_000),
                replacements: vec![prefrontal_routing_core::panel::PackageSeatReplacement {
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            Path::new("/nonexistent/prefrontal-routing.jsonc"),
            &empty_broca_credentials(),
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let result = &body["result"];
        assert_eq!(result["members"].as_array().unwrap().len(), 1);
        assert_eq!(result["runnerUp"].as_array().unwrap().len(), 1);
        assert!(
            result.get("runner_up").is_none(),
            "wire keys stay camelCase"
        );
        assert_eq!(result["members"][0]["temperature"], 0.2);
        assert!(result["runnerUp"][0].get("temperature").is_none());
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

    fn saturation_fact(provider: &str, model: &str, cost: f64) -> ModelFact {
        let mut fact = fact(provider, model);
        fact.cost_per_m_tok_out = Some(cost);
        fact.cost_tier = None;
        fact
    }

    fn saturation_select_params(slots: Option<serde_json::Value>) -> serde_json::Value {
        let mut params = json!({
            "targetAgent": "mason",
            "requirements": { "iq": 30, "eq": 0 },
            "availableModels": ["anthropic/fixture-cheap", "openai/fixture-dear"],
            "taskId": "slot-saturation"
        });
        if let Some(slots) = slots {
            params["concurrencySlots"] = slots;
        }
        params
    }

    fn saturation_facts() -> [ModelFact; 2] {
        [
            saturation_fact("anthropic", "fixture-cheap", 2.0),
            saturation_fact("openai", "fixture-dear", 9.0),
        ]
    }

    #[tokio::test]
    async fn route_select_without_slot_state_is_unchanged() {
        let (response, _rig) = run_select_wire_request(
            "slot-saturation-absent",
            saturation_select_params(None),
            &saturation_facts(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["routeKey"],
            "anthropic/fixture-cheap"
        );
        assert!(body["result"]["selected"].get("slotSaturation").is_none());
    }

    async fn pinned_select(pin: &str, wall: bool) -> (Frame, Rig) {
        let rig = Rig::new("head-pin");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("openai", "gpt-5.5"), FactSource::Override)
            .unwrap();
        store
            .upsert_fact(&fact("anthropic", "claude-opus-5-5"), FactSource::Override)
            .unwrap();
        if wall {
            store
                .update_route_state(
                    "openai/gpt-5.5",
                    "openai/gpt-5.5",
                    &prefrontal_routing_core::RouteStateUpdate {
                        quota_exhausted_until: Some(2_000_000_000_000.0),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        let frame = run_route_wire_request(
            &store,
            json!({"method":"route.select","params":{
                "targetAgent":"mason", "requestedRequirements":{"iq":100,"eq":90},
                "availableModels":["openai/gpt-5.5","anthropic/claude-opus-5-5"], "modelPin":pin
            }}),
        )
        .await;
        (frame, rig)
    }

    #[tokio::test]
    async fn work_model_pin_selects_only_the_named_model_and_variant() {
        let (frame, _rig) = pinned_select("openai/gpt-5.5:high", false).await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["promptParams"]["providerID"], "openai",
            "{body}"
        );
        assert_eq!(
            body["result"]["selected"]["promptParams"]["modelID"],
            "gpt-5.5"
        );
        assert_eq!(
            body["result"]["selected"]["promptParams"]["variant"],
            "high"
        );
        assert_eq!(
            body["result"]["resolvedInput"]["requirements"],
            json!({"iq":100.0,"eq":90.0})
        );
    }

    #[tokio::test]
    async fn work_model_pin_quota_wall_refuses_with_reset_without_fallback() {
        let (frame, _rig) = pinned_select("openai/gpt-5.5:high", true).await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["code"], "quota_exhausted", "{body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("until 2033-05-18T03:33:20Z"),
            "{body}"
        );
        assert!(
            body.get("result").is_none(),
            "a healthy sibling must not replace the pin: {body}"
        );
    }

    #[tokio::test]
    async fn work_model_pin_bad_variant_names_valid_catalog_tiers() {
        let (frame, _rig) = pinned_select_with_published_efforts(
            "openai/gpt-5.5:absurd",
            &[("openai", "gpt-5.5", &["low", "medium", "high", "xhigh"])],
        )
        .await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["code"], "invalid_model_variant", "{body}");
        let message = body["message"].as_str().unwrap();
        assert!(
            message.contains("absurd")
                && message.contains("valid variants: low, medium, high, xhigh"),
            "{message}"
        );
    }

    /// Run a pinned selection with the given reasoning efforts published for
    /// each `(provider, model)` and the store's catalog attached to the
    /// operator config path, as the module does once its store opens.
    async fn pinned_select_with_published_efforts(
        pin: &str,
        published: &[(&str, &str, &[&str])],
    ) -> (Frame, Rig) {
        let rig = Rig::new("head-pin-published");
        let store: SharedStore = Arc::new(rig.open_store());
        for (provider, model, _) in published {
            store
                .upsert_fact(&fact(provider, model), FactSource::Override)
                .unwrap();
        }
        let updates = published
            .iter()
            .map(
                |(provider, model, efforts)| prefrontal_routing_store::FusiformFactUpdate {
                    provider_id: (*provider).into(),
                    model_id: (*model).into(),
                    auth_methods: None,
                    present: true,
                    context_window: Some(200_000.0),
                    max_output_tokens: None,
                    cost_tier: Some(2.0),
                    cost_per_mtok_out: Some(4.5),
                    input_rate: None,
                    cache_read_rate: None,
                    cache_write_rate: None,
                    reasoning_rate: None,
                    supports_vision: Some(true),
                    supports_tool_use: Some(true),
                    reasoning_efforts: Some(efforts.iter().map(|e| (*e).to_string()).collect()),
                    canonical_id: None,
                    upstream_status: None,
                },
            )
            .collect::<Vec<_>>();
        store
            .apply_fusiform_catalog(
                &updates,
                prefrontal_routing_store::FusiformCatalogCursor {
                    catalog_version: 1,
                    resolved_at_ms: 1,
                },
            )
            .unwrap();
        let operator_config = rig.dir.join("prefrontal-routing.jsonc");
        let catalog_store = Arc::clone(&store);
        config_overrides::attach_reasoning_catalog(
            &operator_config,
            Arc::new(move || {
                catalog_store
                    .reasoning_catalog()
                    .map_err(|error| error.to_string())
            }),
        )
        .unwrap();
        let available = published
            .iter()
            .map(|(provider, model, _)| format!("{provider}/{model}"))
            .collect::<Vec<_>>();
        let frame = run_route_wire_request_with_operator_config(
            &store,
            json!({"method":"route.select","params":{
                "targetAgent":"mason", "requestedRequirements":{"iq":100,"eq":90},
                "availableModels": available, "modelPin":pin
            }}),
            &operator_config,
        )
        .await;
        (frame, rig)
    }

    /// Broca admits `xhigh` on an Opus route when Fusiform publishes it, so a
    /// pin naming it is admitted at that variant; a variant outside the
    /// published list is refused.
    #[tokio::test]
    async fn work_model_pin_admits_a_variant_the_catalog_publishes() {
        let opus: &[(&str, &str, &[&str])] = &[(
            "anthropic",
            "claude-opus-5-5",
            &["low", "medium", "high", "xhigh", "max"],
        )];
        let (frame, _rig) =
            pinned_select_with_published_efforts("anthropic/claude-opus-5-5:xhigh", opus).await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["promptParams"]["variant"], "xhigh",
            "{body}"
        );
        let (frame, _rig) =
            pinned_select_with_published_efforts("anthropic/claude-opus-5-5:minimal", opus).await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["code"], "invalid_model_variant", "{body}");
    }

    #[tokio::test]
    async fn work_model_pin_unknown_provider_and_model_name_the_bad_part() {
        for (pin, code, bad) in [
            ("nowhere/gpt-5.5", "unknown_model_provider", "nowhere"),
            ("openai/not-a-model", "unknown_model", "not-a-model"),
        ] {
            let (frame, _rig) = pinned_select(pin, false).await;
            let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(body["code"], code, "{body}");
            assert!(body["message"].as_str().unwrap().contains(bad), "{body}");
        }
    }

    #[tokio::test]
    async fn work_model_pin_preserves_a_colon_inside_the_catalog_model_id() {
        let rig = Rig::new("pin-colon-id");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("ollama-cloud", "qwen3.5:397b"), FactSource::Override)
            .unwrap();
        let frame = run_route_wire_request(&store, json!({"method":"route.select","params":{
            "targetAgent":"mason", "requestedRequirements":{"iq":72,"eq":0},
            "availableModels":["ollama-cloud/qwen3.5:397b"], "modelPin":"ollama-cloud/qwen3.5:397b"
        }})).await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["model"]["modelID"], "qwen3.5:397b",
            "{body}"
        );
        assert!(
            body["result"]["selected"]["promptParams"]
                .get("variant")
                .is_none(),
            "a size tag is not a reasoning tier: {body}"
        );
    }

    #[tokio::test]
    async fn work_model_pin_without_credentials_refuses_the_named_route() {
        let rig = Rig::new("pin-no-credentials");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("openai", "gpt-5.5"), FactSource::Override)
            .unwrap();
        let frame = run_route_wire_request(
            &store,
            json!({"method":"route.select","params":{
                "targetAgent":"mason", "requestedRequirements":{"iq":72,"eq":0},
                "availableModels":[], "modelPin":"openai/gpt-5.5:high"
            }}),
        )
        .await;
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["code"], "pinned_route_refused", "{body}");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("openai/gpt-5.5:high"));
        assert!(body.get("result").is_none());
    }

    /// The routing file for the paid Anthropic API key: a provider whose rows are
    /// borrowed from `anthropic` and which `exclude.all` keeps out of automatic
    /// choice. Naming one of its models still admits it.
    fn platform_operator_config(rig: &Rig) -> PathBuf {
        let path = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(
            &path,
            r#"{ "model_routing": {
                "exclude": { "all": ["anthropic-platform"] },
                "score_alias": { "anthropic-platform": "anthropic" }
            } }"#,
        )
        .unwrap();
        path
    }

    /// A vault listing whose rows map `provider_ids` as given. `account_id` is
    /// the key's workspace, deliberately different from the id's label.
    fn platform_listing(rows: &[(&str, &[&str])]) -> SharedBrocaCredentials {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: rows
                    .iter()
                    .map(
                        |(id, providers)| prefrontal_ckcred_client::ListedCredential {
                            id: (*id).into(),
                            categories: vec!["llm-provider".into()],
                            credential_type: if id.starts_with("apikey:") {
                                "apikey"
                            } else {
                                "oauth"
                            }
                            .into(),
                            state: "active".into(),
                            provider_ids: providers.iter().map(|p| (*p).into()).collect(),
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

    async fn platform_select(
        label: &str,
        params: serde_json::Value,
        credentials: &SharedBrocaCredentials,
        wall_subscription: bool,
    ) -> (serde_json::Value, Rig, SharedStore) {
        platform_select_model(
            label,
            "claude-opus-4-1",
            params,
            credentials,
            wall_subscription,
        )
        .await
    }

    async fn platform_select_model(
        label: &str,
        model: &str,
        params: serde_json::Value,
        credentials: &SharedBrocaCredentials,
        wall_subscription: bool,
    ) -> (serde_json::Value, Rig, SharedStore) {
        let rig = Rig::new(label);
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("anthropic", model), FactSource::Override)
            .unwrap();
        if wall_subscription {
            // Every subscription route is quota-walled, so failover (and its
            // break-glass retry) has nothing on the logins left to move to.
            for stored in store.get_facts().unwrap() {
                if stored.provider_id != "anthropic" {
                    continue;
                }
                let key = make_route_key(
                    &stored.provider_id,
                    &stored.model_id,
                    stored.variant.as_deref(),
                );
                store
                    .update_route_state(
                        &key,
                        &make_capacity_key(&stored.provider_id, &stored.model_id),
                        &prefrontal_routing_core::RouteStateUpdate {
                            quota_exhausted_until: Some(2_000_000_000_000.0),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
        }
        let config = platform_operator_config(&rig);
        let frame = run_route_wire_request_with_paths(
            &store,
            json!({"method": "route.select", "params": params}),
            &config,
            credentials,
        )
        .await;
        (serde_json::from_slice(&frame.body).unwrap(), rig, store)
    }

    fn recorded_rejections(store: &SharedStore, send_id: &str) -> serde_json::Value {
        let row = store
            .get_decision_by_send_id(send_id)
            .unwrap()
            .expect("the selection recorded a decision");
        serde_json::from_str(&row.rejected_json).unwrap()
    }

    #[tokio::test]
    async fn automatic_selection_never_returns_a_provider_excluded_in_all() {
        // Broca can reach only the paid key: the subscription login is absent.
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let (body, _rig, store) = platform_select(
            "platform-auto-only",
            json!({"targetAgent": "explore", "requirements": {"iq": 30, "eq": 0},
                   "substrate": "broca", "sendID": "auto-only"}),
            &credentials,
            false,
        )
        .await;
        assert!(
            body.get("result").is_none(),
            "auto selection took the paid key: {body}"
        );
        // The paid key is the only reachable route and the operator excluded
        // it, so the refusal names the operator's exclusion before any scoring.
        assert!(
            body.to_string().contains("excluded by operator policy"),
            "{body}"
        );
        assert!(
            store
                .get_decision_by_send_id("auto-only")
                .unwrap()
                .is_some(),
            "the refusal is recorded as a decision"
        );
    }

    #[tokio::test]
    async fn failover_and_reroute_never_fall_to_a_provider_excluded_in_all() {
        let credentials = platform_listing(&[
            ("oauth:anthropic:ysample", &["anthropic"]),
            ("apikey:anthropic", &["anthropic-platform"]),
        ]);
        // Reroute: every subscription route is excluded by the caller, so the paid
        // key is the only reachable route left.
        let (rerouted, _rig, _store) = platform_select(
            "platform-reroute",
            json!({"targetAgent": "explore", "requirements": {"iq": 30, "eq": 0},
                   "substrate": "broca", "excludeRouteKeys": ["anthropic"]}),
            &credentials,
            false,
        )
        .await;
        assert!(
            rerouted.get("result").is_none(),
            "reroute fell to the paid key: {rerouted}"
        );
        // Failover past a quota wall, including the break-glass retry.
        let (walled, _rig, store) = platform_select(
            "platform-failover",
            json!({"targetAgent": "explore", "requirements": {"iq": 30, "eq": 0},
                   "substrate": "broca", "sendID": "failover"}),
            &credentials,
            true,
        )
        .await;
        let chosen = walled["result"]["selected"]["routeKey"]
            .as_str()
            .unwrap_or("");
        assert!(!chosen.starts_with("anthropic-platform/"), "{walled}");
        if walled.get("result").is_none() {
            let rejected = recorded_rejections(&store, "failover");
            assert!(
                rejected.to_string().contains("excluded_by_operator"),
                "{rejected}"
            );
        }
    }

    #[tokio::test]
    async fn a_pinned_selection_admits_a_provider_excluded_in_all_with_its_key_pin() {
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let (body, _rig, _store) = platform_select(
            "platform-pin",
            json!({"targetAgent": "mason", "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "broca", "modelPin": "anthropic-platform/claude-opus-4-1"}),
            &credentials,
            false,
        )
        .await;
        assert_eq!(
            body["result"]["selected"]["routeKey"], "anthropic-platform/claude-opus-4-1",
            "{body}"
        );
        // The pin's account is the credential's label, never its workspace.
        let session = body["result"]["runner"]["pool_session"].clone();
        assert!(session.is_string(), "{body}");
        assert_eq!(
            body["result"]["runner"],
            json!({"provider": "anthropic", "model": "claude-opus-4-1",
                   "auth": {"method": "apikey", "account": "work"},
                   "pool": "anthropic-platform", "pool_session": session}),
            "{body}"
        );
        assert_eq!(
            body["result"]["poolLease"]["credentialId"],
            "apikey:anthropic:work"
        );
        assert_eq!(body["result"]["poolLease"]["workspace"], "workspace-7");
        // Pricing is the target's API plane under the key's own method, never
        // a subscription view.
        let plane = &body["result"]["billingPlanes"];
        assert!(
            plane.is_null() || !plane.to_string().contains("oauth"),
            "{plane}"
        );
    }

    /// The operator's own shape: `anthropic-platform` listed in `exclude.all` is
    /// never chosen automatically for any consumer, yet a mason launch that names
    /// `anthropic-platform/claude-opus-5-5` is admitted on its pool key.
    #[tokio::test]
    async fn a_provider_in_exclude_all_is_never_automatic_but_its_named_model_is_admitted() {
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        for consumer in ["mason", "sidekick", "athena", "lab", "core"] {
            let send_id = format!("opus-5-5-auto-{consumer}");
            let (automatic, _rig, store) = platform_select_model(
                &format!("platform-opus-5-5-auto-{consumer}"),
                "claude-opus-5-5",
                json!({"targetAgent": "explore", "consumer": consumer,
                       "requirements": {"iq": 30, "eq": 0},
                       "substrate": "broca", "sendID": send_id}),
                &credentials,
                false,
            )
            .await;
            assert!(
                automatic.get("result").is_none(),
                "consumer={consumer}: automatic selection took the excluded provider: {automatic}"
            );
            assert!(
                automatic
                    .to_string()
                    .contains("excluded by operator policy"),
                "consumer={consumer}: {automatic}"
            );
            assert!(store.get_decision_by_send_id(&send_id).unwrap().is_some());
        }
        let (pinned, _rig, _store) = platform_select_model(
            "platform-opus-5-5-pin",
            "claude-opus-5-5",
            json!({"targetAgent": "mason", "consumer": "mason",
                   "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "broca", "modelPin": "anthropic-platform/claude-opus-5-5"}),
            &credentials,
            false,
        )
        .await;
        assert_eq!(
            pinned["result"]["selected"]["routeKey"], "anthropic-platform/claude-opus-5-5",
            "{pinned}"
        );
        assert_eq!(
            pinned["result"]["poolLease"]["credentialId"], "apikey:anthropic:work",
            "{pinned}"
        );
    }

    /// A host's own mason pin (no workload named) on a key-pool provider is
    /// admitted against Broca's routes without a key; core selects the pin
    /// again with substrate broca before launch, and that selection leases.
    #[tokio::test]
    async fn a_host_mason_pin_is_provisional_until_core_completes_it_on_broca() {
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let (body, _rig, _store) = platform_select(
            "platform-host-pin",
            json!({"targetAgent":"mason", "requestedRequirements":{"iq":90,"eq":0},
                   "harness": "opencode", "executor": null,
                   "availableModels":["anthropic/claude-opus-4-1"],
                   "modelPin":"anthropic-platform/claude-opus-4-1"}),
            &credentials,
            false,
        )
        .await;
        assert_eq!(
            body["result"]["selected"]["routeKey"], "anthropic-platform/claude-opus-4-1",
            "{body}"
        );
        assert_eq!(body["result"]["requiredSubstrate"], "broca", "{body}");
        assert!(
            body["result"].get("runner").is_none(),
            "no key before core admission: {body}"
        );
        assert!(body["result"].get("poolLease").is_none(), "{body}");
    }

    #[tokio::test]
    async fn a_mason_launch_pin_mapped_to_broca_is_admitted_with_its_runner() {
        let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
        let (body, _rig, _store) = platform_select(
            "platform-host-pin",
            json!({"targetAgent":"mason", "requestedRequirements":{"iq":90,"eq":0},
                   "workload": "mason",
                   "harness": "opencode", "substrate": "opencode", "executor": null,
                   "availableModels":["anthropic/claude-opus-4-1"],
                   "modelPin":"anthropic-platform/claude-opus-4-1"}),
            &credentials,
            false,
        )
        .await;
        assert_eq!(
            body["result"]["selected"]["routeKey"], "anthropic-platform/claude-opus-4-1",
            "{body}"
        );
        assert_eq!(body["result"]["selected"]["harness"], "broca", "{body}");
        assert!(body["result"].get("requiredSubstrate").is_none(), "{body}");
        assert!(body["result"].get("runner").is_some(), "{body}");
        assert!(body["result"].get("poolLease").is_some(), "{body}");
    }

    #[tokio::test]
    async fn the_key_pool_spreads_walls_keys_and_refuses_without_falling_back() {
        // Two keys of one workspace; the second key's label differs from its
        // workspace (`account_id`), and only the label goes in the pin.
        let credentials = platform_listing(&[
            ("apikey:anthropic", &["anthropic-platform"]),
            ("apikey:anthropic:work", &["anthropic-platform"]),
        ]);
        let rig = Rig::new("platform-pool");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("anthropic", "claude-opus-4-1"), FactSource::Override)
            .unwrap();
        let config = platform_operator_config(&rig);
        let call = |method: &'static str, params: serde_json::Value| {
            let store = Arc::clone(&store);
            let credentials = credentials.clone();
            let config = config.clone();
            async move {
                let frame = run_route_wire_request_with_paths(
                    &store,
                    json!({"method": method, "params": params}),
                    &config,
                    &credentials,
                )
                .await;
                serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
            }
        };
        let acquire = |session: &'static str| {
            call(
                "route.pool_acquire",
                json!({"provider": "anthropic-platform", "model": "claude-opus-4-1", "session": session}),
            )
        };
        // Spread: two concurrent sessions take the two keys.
        let first = acquire("ses-1").await;
        let second = acquire("ses-2").await;
        let keys = [
            first["result"]["credentialId"]
                .as_str()
                .unwrap()
                .to_string(),
            second["result"]["credentialId"]
                .as_str()
                .unwrap()
                .to_string(),
        ];
        assert_ne!(keys[0], keys[1], "{first} {second}");
        let work = if keys[0] == "apikey:anthropic:work" {
            &first
        } else {
            &second
        };
        assert_eq!(work["result"]["workspace"], "workspace-7");
        assert_eq!(
            work["result"]["runner"]["auth"],
            json!({"method": "apikey", "account": "work"}),
            "the pin carries the key's label, never its workspace: {work}"
        );
        assert_eq!(work["result"]["runner"]["provider"], "anthropic");
        assert_eq!(work["result"]["runner"]["pool"], "anthropic-platform");

        // ses-1's key is walled, so its revival is handed the remaining key and its pin.
        let walled_key = keys[0].clone();
        let wall = call(
            "route.pool_wall",
            json!({"provider": "anthropic-platform", "credentialId": walled_key,
                   "kind": "billing_exhausted", "untilMs": now_ms() as i64 + 3_600_000}),
        )
        .await;
        assert_eq!(wall["result"]["ok"], true, "{wall}");
        let revival = acquire("ses-1").await;
        assert_eq!(
            revival["result"]["credentialId"],
            keys[1].as_str(),
            "{revival}"
        );

        // Every key walled: a named refusal listing each key, never a subscription route.
        call(
            "route.pool_wall",
            json!({"provider": "anthropic-platform", "credentialId": keys[1],
                   "kind": "rate_limited", "untilMs": now_ms() as i64 + 30_000}),
        )
        .await;
        let refused = acquire("ses-3").await;
        assert_eq!(
            refused["code"], "explicit_provider_pool_exhausted",
            "{refused}"
        );
        let reasons: Vec<&str> = refused["detail"]["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| key["reason"].as_str().unwrap())
            .collect();
        assert_eq!(reasons.len(), 2, "{refused}");
        assert!(reasons.contains(&"billing_exhausted") && reasons.contains(&"rate_limited"));
        let pinned = call(
            "route.select",
            json!({"targetAgent": "mason", "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "broca", "modelPin": "anthropic-platform/claude-opus-4-1",
                   "taskId": "ses-4"}),
        )
        .await;
        assert_eq!(
            pinned["code"], "explicit_provider_pool_exhausted",
            "{pinned}"
        );
        assert!(
            pinned.get("result").is_none(),
            "no fallback route: {pinned}"
        );

        // A legacy substrate value cannot move a Broca-mapped pin off its pool
        // or let it bypass the same exhausted keys.
        let host = call(
            "route.select",
            json!({"targetAgent": "mason", "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "opencode", "availableModels": ["anthropic-platform/claude-opus-4-1"],
                   "modelPin": "anthropic-platform/claude-opus-4-1"}),
        )
        .await;
        assert_eq!(host["code"], "explicit_provider_pool_exhausted", "{host}");
        assert!(host.get("result").is_none(), "no fallback route: {host}");
    }

    /// The first real key, as CKCRED lists it: a labelled id, active,
    /// llm-provider, mapped to `anthropic-platform`, and no `account_id`. Every
    /// real key carries a label, so the pin always names it; with no
    /// `account_id` the key is its own prompt-cache workspace.
    #[tokio::test]
    async fn the_first_real_platform_key_pins_its_label_and_is_its_own_workspace() {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: vec![prefrontal_ckcred_client::ListedCredential {
                    id: "apikey:anthropic:wsampleyahoo".into(),
                    categories: vec!["llm-provider".into()],
                    credential_type: "apikey".into(),
                    state: "active".into(),
                    provider_ids: vec!["anthropic-platform".into()],
                    account_id: None,
                    ..Default::default()
                }],
                view: "ckcred-first-real-key".into(),
                ..Default::default()
            },
            now_ms(),
        );
        let (body, _rig, _store) = platform_select(
            "platform-first-real-key",
            json!({"targetAgent": "mason", "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "broca", "modelPin": "anthropic-platform/claude-opus-4-1",
                   "taskId": "bg_first_key"}),
            &credentials,
            false,
        )
        .await;
        assert_eq!(
            body["result"]["runner"]["auth"],
            json!({"method": "apikey", "account": "wsampleyahoo"}),
            "a labelled key never pins without its account: {body}"
        );
        assert_eq!(body["result"]["runner"]["provider"], "anthropic");
        assert_eq!(
            body["result"]["poolLease"]["credentialId"],
            "apikey:anthropic:wsampleyahoo"
        );
        assert_eq!(
            body["result"]["poolLease"]["workspace"], "key:apikey:anthropic:wsampleyahoo",
            "no account_id: the key is its own workspace"
        );
    }

    /// A vault listing with explicit rows: (id, provider_ids, account_id).
    fn vault_listing(rows: &[(&str, &[&str], Option<&str>)]) -> SharedBrocaCredentials {
        let credentials = empty_broca_credentials();
        credentials.lock().unwrap().apply_listing(
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: rows
                    .iter()
                    .map(
                        |(id, providers, account)| prefrontal_ckcred_client::ListedCredential {
                            id: (*id).into(),
                            categories: vec!["llm-provider".into()],
                            credential_type: if id.starts_with("apikey:") {
                                "apikey"
                            } else {
                                "oauth"
                            }
                            .into(),
                            state: "active".into(),
                            provider_ids: providers.iter().map(|p| (*p).into()).collect(),
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

    /// The live vault: four labelled Anthropic API keys mapped to the pool, a
    /// key whose id looks like a pool key but is not mapped to it, and a
    /// subscription login sharing the label `ysample`. Membership comes from the
    /// mapping alone, keyed on the full credential id.
    #[tokio::test]
    async fn the_pool_is_the_mapped_api_keys_and_never_a_login_with_the_same_label() {
        let platform: &[&str] = &["anthropic-platform"];
        let credentials = vault_listing(&[
            ("apikey:anthropic:wsampleyahoo", platform, None),
            ("apikey:anthropic:wsamplegmail", platform, None),
            ("apikey:anthropic:usersamplegmail", platform, None),
            ("apikey:anthropic:ysample", platform, None),
            ("apikey:anthropic:unmapped", &["anthropic"], None),
            ("oauth:anthropic:ysample", &["anthropic"], Some("acct-ysample")),
        ]);
        let mut members: Vec<String> = credentials
            .lock()
            .unwrap()
            .pool_keys("anthropic-platform", now_ms())
            .unwrap()
            .into_iter()
            .map(|key| key.credential_id)
            .collect();
        members.sort();
        assert_eq!(
            members,
            vec![
                "apikey:anthropic:usersamplegmail",
                "apikey:anthropic:wsamplegmail",
                "apikey:anthropic:wsampleyahoo",
                "apikey:anthropic:ysample",
            ],
            "an apikey:anthropic: id not mapped to the provider is not a member"
        );
        // Even a login wrongly mapped to the pool is never a member.
        let misconfigured = vault_listing(&[
            ("apikey:anthropic:ysample", platform, None),
            ("oauth:anthropic:ysample", platform, Some("acct-ysample")),
        ]);
        let keys = misconfigured
            .lock()
            .unwrap()
            .pool_keys("anthropic-platform", now_ms())
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].credential_id, "apikey:anthropic:ysample");

        // Four sessions spread over the four keys; a wall recorded on the
        // subscription login's id never touches the API key sharing its label.
        let rig = Rig::new("platform-live-vault");
        let store: SharedStore = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("anthropic", "claude-opus-4-1"), FactSource::Override)
            .unwrap();
        let config = platform_operator_config(&rig);
        let call = |method: &'static str, params: serde_json::Value| {
            let store = Arc::clone(&store);
            let credentials = credentials.clone();
            let config = config.clone();
            async move {
                let frame = run_route_wire_request_with_paths(
                    &store,
                    json!({"method": method, "params": params}),
                    &config,
                    &credentials,
                )
                .await;
                serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
            }
        };
        call(
            "route.pool_wall",
            json!({"provider": "anthropic-platform", "credentialId": "oauth:anthropic:ysample",
                   "kind": "billing_exhausted", "untilMs": now_ms() as i64 + 3_600_000}),
        )
        .await;
        let mut leased = Vec::new();
        for session in ["s1", "s2", "s3", "s4"] {
            let reply = call(
                "route.pool_acquire",
                json!({"provider": "anthropic-platform", "model": "claude-opus-4-1", "session": session}),
            )
            .await;
            let id = reply["result"]["credentialId"]
                .as_str()
                .unwrap_or_else(|| panic!("{reply}"))
                .to_string();
            let label = id.rsplit(':').next().unwrap().to_string();
            assert_eq!(
                reply["result"]["runner"]["auth"],
                json!({"method": "apikey", "account": label}),
                "the pin names the key's own label: {reply}"
            );
            leased.push(id);
        }
        leased.sort();
        assert_eq!(
            leased, members,
            "four concurrent runs take the four keys, apikey:anthropic:ysample included"
        );
    }

    #[tokio::test]
    async fn a_pinned_launch_and_its_sends_share_one_pool_lease() {
        // Selection leases under the caller's task; the launch target carries
        // that session, and the first send acquires under it again. One launch
        // must be one run in flight, never two for the lease's lifetime.
        let credentials = platform_listing(&[
            ("apikey:anthropic", &["anthropic-platform"]),
            ("apikey:anthropic:work", &["anthropic-platform"]),
        ]);
        let (body, rig, store) = platform_select(
            "platform-one-lease",
            json!({"targetAgent": "mason", "requestedRequirements": {"iq": 90, "eq": 0},
                   "substrate": "broca", "modelPin": "anthropic-platform/claude-opus-4-1",
                   "taskId": "bg_launch"}),
            &credentials,
            false,
        )
        .await;
        let runner = &body["result"]["runner"];
        assert_eq!(runner["pool_session"], "bg_launch", "{body}");
        let selected_lease = body["result"]["poolLease"]["leaseId"].clone();
        let config = platform_operator_config(&rig);
        let frame = run_route_wire_request_with_paths(
            &store,
            json!({"method": "route.pool_acquire", "params": {
                "provider": "anthropic-platform", "model": "claude-opus-4-1",
                "session": runner["pool_session"]}}),
            &config,
            &credentials,
        )
        .await;
        let send: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            send["result"]["leaseId"], selected_lease,
            "the send reuses the selection's lease: {send}"
        );
        let in_flight: u32 = store
            .pool_key_states("anthropic-platform", now_ms() as i64)
            .unwrap()
            .values()
            .map(|state| state.in_flight)
            .sum();
        assert_eq!(in_flight, 1, "one launch is one run in flight");
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
    async fn route_select_skips_a_saturated_provider_for_the_next_adequate_route() {
        let (response, rig) = run_select_wire_request(
            "slot-saturation-skip",
            saturation_select_params(Some(json!([
                {"key": "anthropic/fixture-cheap", "running": 50, "limit": 40, "queued": 7},
                {"key": "openai/fixture-dear", "running": 3, "limit": 25}
            ]))),
            &saturation_facts(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let selected = &body["result"]["selected"];
        assert_eq!(selected["routeKey"], "openai/fixture-dear");
        assert_eq!(
            rejection_reason(&body, "anthropic/fixture-cheap").as_deref(),
            Some("provider_saturated")
        );
        assert_eq!(
            selected["slotSaturation"]["skipped"][0]["slot"],
            json!({"key": "anthropic/fixture-cheap", "running": 50, "limit": 40, "queued": 7})
        );
        assert!(selected["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .any(|line| line
                == "route anthropic/fixture-cheap skipped: provider_saturated: anthropic/fixture-cheap 50/40, 7 queued"));
        // The skip is audit, not a wall: the recorded decision carries the
        // rejection, and no cooldown is stored for the skipped route.
        let store = rig.open_store();
        let decisions = store.get_decisions_for_task("slot-saturation").unwrap();
        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].rejected_json.contains("provider_saturated"));
        let snapshot = store.build_snapshot(now_ms() as f64).unwrap();
        let unwalled = |state: Option<&prefrontal_routing_core::RouteRuntimeState>| {
            state.is_none_or(|state| {
                state.cooldown_until.is_none()
                    && state.quota_exhausted_until.is_none()
                    && state.failure_backoff_until.is_none()
            })
        };
        assert!(unwalled(snapshot.by_route.get("anthropic/fixture-cheap")));
        assert!(unwalled(
            snapshot.by_capacity.get("anthropic/fixture-cheap")
        ));
        drop(store);
        // The next decision re-reads the counts: with room again, the skipped
        // route wins as if it had never been passed over.
        let next = run_select_against_rig(
            &rig,
            saturation_select_params(None),
            &saturation_facts(),
            &PathBuf::from("/nonexistent/prefrontal-routing.jsonc"),
        )
        .await;
        let next: serde_json::Value = serde_json::from_slice(&next.body).unwrap();
        assert_eq!(
            next["result"]["selected"]["routeKey"],
            "anthropic/fixture-cheap"
        );
    }

    #[tokio::test]
    async fn route_select_with_every_adequate_provider_saturated_keeps_the_winner_and_says_it_queues(
    ) {
        let (response, _rig) = run_select_wire_request(
            "slot-saturation-all",
            saturation_select_params(Some(json!([
                {"key": "anthropic/fixture-cheap", "running": 50, "limit": 40},
                {"key": "openai/fixture-dear", "running": 25, "limit": 25}
            ]))),
            &saturation_facts(),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let selected = &body["result"]["selected"];
        assert_eq!(selected["routeKey"], "anthropic/fixture-cheap");
        assert_eq!(
            selected["slotSaturation"]["queuedOn"]["key"],
            "anthropic/fixture-cheap"
        );
        assert!(selected["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .any(|line| line
                == "all adequate providers saturated; queued on anthropic/fixture-cheap (50/40)"));
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
            }), Path::new("/nonexistent/prefrontal-routing.jsonc"), &credentials).await;
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
            prefrontal_routing_store::FusiformFactUpdate {
                provider_id: provider.into(),
                model_id: model.into(),
                auth_methods: None,
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
                prefrontal_routing_store::FusiformCatalogCursor {
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
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &aliases);
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

    /// Live specimen: OpenCode withdrew `muse-spark-1.3-contributor-free` and
    /// models.dev marks it `deprecated`, yet routing kept seating it and the
    /// seats failed at the provider. A deprecated route is refused for automatic
    /// selection and for an explicit pin, naming the route; its `beta` sibling
    /// is still offered.
    #[tokio::test]
    async fn deprecated_upstream_route_is_refused_and_its_beta_sibling_is_offered() {
        const SPECIMEN: &str = "opencode/muse-spark-1.3-contributor-free";
        const SIBLING: &str = "meta/muse-spark-1.3-contributor";
        let rig = Rig::new("deprecated-upstream");
        let store: SharedStore = Arc::new(rig.open_store());
        for route in [SPECIMEN, SIBLING] {
            let (provider, model) = route.split_once('/').unwrap();
            store
                .upsert_fact(&fact(provider, model), FactSource::Override)
                .unwrap();
        }
        let served = |status: &str| {
            json!({"existence": "present", "limit.context": 200000,
                "capability.tool_call": true, "model.status": status})
        };
        let snapshot = fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": 1, "resolved_at_ms": 1,
                "models": { SPECIMEN: served("deprecated"), SIBLING: served("beta") }
            }))
            .unwrap(),
        )
        .unwrap();
        let (served_facts, cursor) = snapshot.parts();
        store.apply_fusiform_catalog(served_facts, cursor).unwrap();
        let stored = store.get_fact_by_route_key(SPECIMEN).unwrap().unwrap();
        assert_eq!(stored.upstream_status.as_deref(), Some("deprecated"));

        let select = |pin: Option<&str>| {
            let mut params = json!({
                "targetAgent": "mason", "requestedRequirements": {"iq": 10, "eq": 10},
                "availableModels": [SPECIMEN, SIBLING],
            });
            if let Some(pin) = pin {
                params["modelPin"] = json!(pin);
            }
            let store = store.clone();
            async move {
                let frame = run_route_wire_request(
                    &store,
                    json!({"method": "route.select", "params": params}),
                )
                .await;
                serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
            }
        };

        let pinned = select(Some(SPECIMEN)).await;
        assert_eq!(
            pinned["code"],
            prefrontal_routing_core::REASON_DEPRECATED_UPSTREAM,
            "{pinned}"
        );
        assert!(
            pinned["message"].as_str().unwrap().contains(SPECIMEN),
            "the refusal names the route: {pinned}"
        );

        let sibling = select(Some(SIBLING)).await;
        assert_eq!(
            sibling["result"]["selected"]["routeKey"], SIBLING,
            "a beta sibling is unaffected: {sibling}"
        );

        let automatic = select(None).await;
        assert_eq!(
            automatic["result"]["selected"]["routeKey"], SIBLING,
            "{automatic}"
        );
        assert!(
            automatic["result"]["selected"]["rejected"]
                .as_array()
                .unwrap()
                .iter()
                .any(|rejected| rejected["routeKey"] == SPECIMEN
                    && rejected["reason"] == prefrontal_routing_core::REASON_DEPRECATED_UPSTREAM),
            "{automatic}"
        );
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

    #[tokio::test]
    async fn live_config_reloads_cost_and_exclusion_atomically_and_keeps_last_good() {
        let _guard = config_overrides::multiplier_map_guard();
        let rig = Rig::new("live-atomic-config");
        let store = Arc::new(rig.open_store());
        let mut a = fact("a", "reader");
        a.cost_per_m_tok_out = Some(3.0);
        let mut b = fact("b", "reader");
        b.cost_per_m_tok_out = Some(6.0);
        for fact in [&a, &b] {
            store.upsert_fact(fact, FactSource::Seed).unwrap();
        }
        let credentials = Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(&[a, b])));
        let path = rig.dir.join("routing.jsonc");
        std::fs::write(&path, "{}").unwrap();
        async fn select(
            store: &SharedStore,
            credentials: &SharedBrocaCredentials,
            path: &Path,
        ) -> serde_json::Value {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(
                    &json!({"method": ROUTE_SELECT_OP, "params": { "executor": "broca",
                        "targetAgent": "explore", "requirements": {"iq": 30, "eq": 0},
                        "availableModels": ["a/reader", "b/reader"]
                    }}),
                )
                .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_select_request(
                frame,
                1,
                &tx,
                &Arc::new(Mutex::new(QuotaCache::default())),
                store,
                &Arc::new(Mutex::new(None)),
                path,
                path,
                credentials,
                &empty_opencode_auth(),
            )
            .await
            .unwrap();
            let response = rx.recv().await.unwrap();
            assert_eq!(response.header.ty, FrameType::Response);
            serde_json::from_slice(&response.body).unwrap()
        }
        let first = select(&store, &credentials, &path).await;
        assert_eq!(first["result"]["selected"]["routeKey"], "a/reader");
        std::fs::write(
            &path,
            r#"{"model_routing":{"models":{"b/reader":{"cost_per_mtok_out":0.1}}}}"#,
        )
        .unwrap();
        config_overrides::settle_live_config(&path);
        let cost_edit = select(&store, &credentials, &path).await;
        assert_eq!(cost_edit["result"]["selected"]["routeKey"], "b/reader");
        std::fs::write(&path, r#"{"model_routing":{"exclude":{"all":["b"]},"models":{"b/reader":{"cost_per_mtok_out":0.1}}}}"#).unwrap();
        config_overrides::settle_live_config(&path);
        let fenced = select(&store, &credentials, &path).await;
        assert_eq!(fenced["result"]["selected"]["routeKey"], "a/reader");
        assert_eq!(
            rejection_reason(&fenced, "b/reader").as_deref(),
            Some("excluded_by_operator")
        );
        let good = config_overrides::live_config(&path);
        for invalid in [
            "{broken",
            r#"{"model_routing":{"exclude":{"all":["a"]},"models":{"b/reader":{"cost_per_mtok_out":-1}}}}"#,
            r#"{"aa":{"api_key_ref":" "},"model_routing":{"exclude":{"all":["a"]}}}"#,
            // The removed key is refused whole, so its exclusion is never applied.
            r#"{"model_routing":{"explicit_only_providers":["a"]}}"#,
        ] {
            std::fs::write(&path, invalid).unwrap();
            let retained = config_overrides::settle_live_config(&path);
            assert!(
                Arc::ptr_eq(&good, &retained),
                "invalid documents must never partially publish"
            );
            let selected = select(&store, &credentials, &path).await;
            assert_eq!(selected["result"]["selected"], fenced["result"]["selected"]);
        }
        std::fs::write(&path, "{}").unwrap();
        config_overrides::settle_live_config(&path);
        let removed = select(&store, &credentials, &path).await;
        assert_eq!(removed["result"]["selected"]["routeKey"], "a/reader");
        assert_eq!(
            store
                .get_fact_by_route_key("b/reader")
                .unwrap()
                .unwrap()
                .cost_per_m_tok_out,
            Some(6.0),
            "overlays must not replace the catalog base"
        );
    }

    #[tokio::test]
    async fn next_selection_observes_a_live_elo_override_edit() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        async fn select(
            store: &SharedStore,
            quota_cache: &SharedQuotaCache,
            config_path: &Path,
        ) -> String {
            config_overrides::settle_live_config(config_path);
            let request = json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "executor": "broca",
                    "targetAgent": "mason",
                    "requirements": { "iq": 100, "eq": 0 },
                    "availableModels": ["p/weak", "p/strong"]
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
                quota_cache,
                store,
                &Arc::new(Mutex::new(None)),
                config_path,
                Path::new("/nonexistent/prefrontal-routing.jsonc"),
                &empty_broca_credentials(),
                &empty_opencode_auth(),
            )
            .await
            .unwrap();
            let response = rx.recv().await.unwrap();
            assert_eq!(response.header.ty, FrameType::Response);
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            body["result"]["selected"]["routeKey"]
                .as_str()
                .unwrap()
                .to_string()
        }

        let rig = Rig::new("live-elo-override");
        let store = Arc::new(rig.open_store());
        let mut weak = fact("p", "weak");
        weak.scores.elo = 1_000.0;
        weak.cost_per_m_tok_out = Some(0.0);
        let mut strong = fact("p", "strong");
        strong.scores.elo = 1_800.0;
        strong.cost_per_m_tok_out = Some(30.0);
        store.upsert_fact(&weak, FactSource::Override).unwrap();
        store.upsert_fact(&strong, FactSource::Override).unwrap();
        let config_path = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(&config_path, r#"{"model_routing":{}}"#).unwrap();
        let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));

        assert_eq!(select(&store, &quota_cache, &config_path).await, "p/strong");
        std::fs::write(
            &config_path,
            r#"{"model_routing":{"elo_overrides":{"p/weak":1900}}}"#,
        )
        .unwrap();
        assert_eq!(select(&store, &quota_cache, &config_path).await, "p/weak");
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
    async fn routing_available_models_text_is_a_query_served_by_its_dispatch_arm() {
        let rig = Rig::new("available-models-wire");
        let store = Arc::new(rig.open_store());
        store
            .upsert_fact(&fact("wireco", "model"), FactSource::Seed)
            .unwrap();
        let response = run_route_wire_request(
            &store,
            json!({
                "method": available_models_text::OP,
                "params": {
                    "availableModels": ["wireco/model"],
                    "subscriptionEntries": [],
                    "providerAuthTypes": {}
                }
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert!(body["result"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("wireco/model") && text.contains("← recommended")));

        let routing_manifest = manifest(DEFAULT_MODULE_ID);
        let operation = routing_manifest
            .provides
            .iter()
            .find_map(|role| match role {
                ProviderRole::ManagementSurface { operations, .. } => operations
                    .iter()
                    .find(|operation| operation.name == available_models_text::OP),
                _ => None,
            })
            .expect("available models text operation is advertised");
        assert_eq!(operation.kind, ManagementOperationKind::Query);
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

    #[tokio::test]
    async fn aa_inferred_is_admitted_and_unscored_is_not_replayable() {
        let rig = Rig::new("aa-inferred-admit");
        let store = rig.open_store();
        let mut inferred = fact("custom", "aa-live");
        inferred.scores.elo = 1_800.0;
        inferred.score_provenance = ScoreProvenance::AaInferred;
        inferred.aa_index = Some(55.0);
        inferred.review_by_ms = Some(1);
        inferred.eq_source = Some("custom/curated-neighbor".into());
        store
            .upsert_fact(&inferred, FactSource::AaInferred)
            .unwrap();
        let mut unscored = fact("custom", "no-score");
        unscored.scores.elo = 2_000.0;
        unscored.score_provenance = ScoreProvenance::Unscored;
        store.upsert_fact(&unscored, FactSource::Unscored).unwrap();
        drop(store);

        // Stored AA-inferred scores remain eligible after AA ingestion is removed,
        // even when their operator-review deadline has passed.
        let store = Arc::new(rig.open_store());
        let facts = store.get_facts().unwrap();
        let loaded = facts
            .iter()
            .find(|fact| fact.provider_id == "custom" && fact.model_id == "aa-live")
            .expect("stored AA-inferred fact must survive reopening");
        assert_eq!(loaded.score_provenance, ScoreProvenance::AaInferred);
        assert_eq!(loaded.scores.elo, 1_800.0);
        assert_eq!(loaded.aa_index, Some(55.0));
        assert_eq!(loaded.review_by_ms, Some(1));
        assert_eq!(loaded.eq_source.as_deref(), Some("custom/curated-neighbor"));
        let listed = run_route_wire_request(
            &store,
            json!({ "method": ROUTE_LIST_INFERRED_OP, "params": {} }),
        )
        .await;
        assert_eq!(listed.header.ty, FrameType::Response);
        let listed_body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
        let inferred_rows = listed_body["result"]["inferred"].as_array().unwrap();
        assert_eq!(inferred_rows.len(), 1);
        assert_eq!(inferred_rows[0]["routeKey"], "custom/aa-live");
        let unscored_rows = listed_body["result"]["unscored"].as_array().unwrap();
        assert_eq!(unscored_rows.len(), 1);
        assert_eq!(unscored_rows[0]["routeKey"], "custom/no-score");

        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "executor": "broca",
                "targetAgent": "mason",
                "requirements": { "iq": 20, "eq": 0 },
                "availableModels": ["custom/aa-live", "custom/no-score"],
                "taskId": "aa-inferred-admit"
            }
        });
        let response = run_route_wire_request(&store, request).await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["selected"]["model"]["modelID"], "aa-live");
        let explanation = body["result"]["selected"]["explanation"]
            .as_array()
            .unwrap();
        assert!(
            explanation
                .iter()
                .any(|line| line == "provenance=aa_inferred"),
            "decision must attribute an AA-inferred pick: {explanation:?}"
        );
        let decision_id = body["result"]["decisionID"].as_str().unwrap();
        let explain = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_EXPLAIN_OP,
                "params": { "decisionID": decision_id }
            }),
        )
        .await;
        assert_eq!(explain.header.ty, FrameType::Response);
        let explain_body: serde_json::Value = serde_json::from_slice(&explain.body).unwrap();
        let replayed = explain_body["result"]["why"]["explanation"].to_string();
        assert!(
            replayed.contains("provenance=aa_inferred"),
            "replayed decision must keep AA provenance: {replayed}"
        );

        let unscored_only = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_SELECT_OP,
                "params": {
                    "executor": "broca",
                    "targetAgent": "mason",
                    "requirements": { "iq": 20, "eq": 0 },
                    "availableModels": ["custom/no-score"],
                    "taskId": "unscored-excluded"
                }
            }),
        )
        .await;
        assert_eq!(unscored_only.header.ty, FrameType::Error);
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
                routing_config: rig.dir.join("prefrontal-routing.jsonc"),
                operator_config: rig.dir.join("prefrontal-routing.jsonc"),
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
                requirements: resolved.requirements,
                available_models: observation.available.iter().cloned().collect(),
                facts: rig
                    .store
                    .get_facts_with_observed_speed(now_ms() as f64)
                    .unwrap(),
                runtime_snapshot: Some(observation.snapshot.clone()),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: resolved.required_capabilities,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
            },
            &DEFAULT_ROUTING_CONFIG,
        )
        .ok()
        .map(|selected| selected.route_key)
    }

    #[test]
    fn no_ladder_exists_before_a_selection_has_been_observed() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = LadderRig::new("reload");
        rig.observe(&["incumbent", "small"]);
        assert_eq!(rig.announce("first selection"), None);
        let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

        std::fs::write(
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
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

    /// Multipliers are process-wide and priced into facts when they are read. The
    /// ladder must load them first, or its first computation after boot prices
    /// subscription routes at list and disagrees with the next one.
    #[test]
    fn the_first_ladder_after_boot_prices_routes_with_the_configured_multipliers() {
        let _multiplier_guard = crate::config_overrides::multiplier_map_guard();
        let rig = LadderRig::new("multiplier");
        let mut listed = fact(&rig.provider, "listed");
        listed.scores.elo = 1_500.0;
        listed.scores.eq = 1_500.0;
        listed.cost_per_m_tok_out = Some(8.0);
        rig.store
            .upsert_fact(&listed, FactSource::Override)
            .unwrap();
        // At list price `listed` (8) undercuts `incumbent` (10); with the
        // incumbent's configured multiplier it costs a tenth as much.
        let mut models = serde_json::Map::new();
        models.insert(
            rig.route("incumbent"),
            json!({ "subscription_multiplier": 100 }),
        );
        std::fs::write(
            &rig.sources.routing_config,
            serde_json::to_vec(&json!({ "model_routing": { "models": models } })).unwrap(),
        )
        .unwrap();
        // A fresh process: nothing has loaded the multipliers yet.
        prefrontal_routing_core::model_facts::set_subscription_multipliers(HashMap::new());
        prefrontal_routing_core::model_facts::set_speed_multipliers(HashMap::new());
        rig.observe(&["incumbent", "listed"]);

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
}

#[cfg(test)]
mod slow_route_select_timing_tests {
    use super::*;

    #[test]
    fn injected_snapshot_delay_is_emitted_in_the_slow_select_breakdown() {
        let timing = RouteSelectTiming::new();
        timing.is_select.set(true);
        let config = config_overrides::LiveConfig::default();
        let (_, wait, snapshot) = config.prepare_with_timing(|| {
            std::thread::sleep(Duration::from_millis(1_050));
        });
        timing.record_prepare(wait, snapshot);

        let now = Instant::now();
        let line = timing
            .line_at(now)
            .expect("the injected delay exceeds one second");
        let emitted =
            rate_limit_route_select_line(line, now, &mut RouteSelectLogLimiter::default())
                .expect("the first slow select is emitted");
        assert!(emitted.contains("slow route.select total_ms="), "{emitted}");
        assert!(emitted.contains("wait_prepare_ms="), "{emitted}");
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
            ("wait_prepare_ms", &timing.prepare_wait_ms),
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
