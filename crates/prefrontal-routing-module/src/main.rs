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

mod aa_ingest;
mod config_overrides;
mod fusiform_catalog;
mod quota_consumer;

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cortexkit_store_types::StorageDescriptor;
use fusiform_catalog::{CatalogReachHealth, SharedCatalogHealth};
use prefrontal_routing_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use prefrontal_routing_core::model_router::{
    select_model_route_with_config, ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities,
    RouteFailureReason, SelectModelRouteInput, SelectedModelRoute,
};
use prefrontal_routing_core::quota::{merge_quota_overlay, ProviderUsage};
use prefrontal_routing_core::route_state_store::FactSource;
use prefrontal_routing_core::routing_config::{ANCHOR_VERSION, DEFAULT_ROUTING_CONFIG};
use prefrontal_routing_core::{
    derive_provider_key, fact_to_route, make_route_key, maybe_upgrade_to_fast_variant,
    relax_earliest_reset, resolve_launch_requirements, resolve_routing_policy,
    select_panel_with_config, validate_panel_spec, BreakGlassCandidate, CatalogFactField,
    DecisionInput, DecisionOutcomeUpdate, FailureBackoffConfig, FastVariantUpgradeInput, ModelFact,
    ModelRequirements, ModelRoute, ModelRoutingPolicyInput, PanelRosterEntry, PanelSelection,
    PanelSpec, RequestedModelRequirements, ResolvedRoutingPolicy, RouteOutcome, RouteStateStore,
    SelectPanelInput, SelectedPanel,
};
use prefrontal_routing_store::{dev_descriptor, DecisionListFilter, SqliteRouteStateStore};
use quota_consumer::QuotaConsumer;
use serde::Deserialize;
use serde_json::json;
use subc_protocol::manifest::Concurrency;
use subc_protocol::{
    manifest::{
        Bindings, IdentityBinding, ManagementOperation, ManagementOperationKind, ModuleManifest,
        ProviderRole, StorageBinding, StorageKind, StorageScope, TrustTier,
    },
    session::{
        HealthReport, ModuleControlRequest, ModuleControlResponse, MODULE_CONTROL_OP_HEALTH_CHECK,
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
/// Observability op: reports whether the outbound quota-consumer connection is
/// live and how fresh its windows are. Lets a consumer (and the e2e) confirm the
/// module->module quota path actually round-tripped, not just that it didn't hang.
const QUOTA_STATUS_OP: &str = "route.quota_status";
/// Seed a provisional AA-inferred score for one router key (management, primary-only).
const ROUTE_MODEL_SEED_AA_OP: &str = "route.model_seed_aa";
/// Operator worklist: every `aa_inferred` row plus unscored catalog rows.
const ROUTE_LIST_INFERRED_OP: &str = "route.list_inferred";
/// Focused alias for operators that only need never-scored Fusiform discoveries.
const ROUTE_LIST_UNSCORED_OP: &str = "route.list_unscored";
const HELLO_CORR: u64 = 1;
const EGRESS_BUFFER: usize = 64;
/// How often the background task refreshes quota windows from insula.
/// Windows change slowly, so a cached snapshot refreshed off the selection hot
/// path keeps `route.select` from ever blocking on a live cross-module call.
const QUOTA_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Backoff between quota-connect attempts when insula is absent/down.
/// The module degrades gracefully (seed-only selection) until quota appears. Kept
/// short so a sibling module that registers slightly later is picked up promptly.
const QUOTA_RETRY_INTERVAL: Duration = Duration::from_secs(3);
/// Decision audit rows are retained for one month before the existing refresh
/// cycle deletes them.
const DECISION_RETENTION_MS: u64 = 30 * 24 * 60 * 60_000;
/// How recently a provider must have supplied a live quota entry for its
/// low-pressure reading to permit an optimistic fast-variant upgrade. Retained
/// windows still constrain normal selection but never authorize an upgrade.
const QUOTA_FRESH_FOR_UPGRADE_MS: u64 = 3 * 60_000;

/// Latest quota snapshot, shared between the background refresh task (writer) and
/// `route.select` handlers (readers). The lock is NEVER held across an `.await`:
/// the refresh task fetches outside the lock then takes it briefly to merge; a
/// selection takes it only to clone the snapshot out.
#[derive(Default)]
struct QuotaCache {
    windows: Vec<ProviderUsage>,
    refreshed_at_ms: u64,
    /// Observation time for providers that supplied a live entry in the latest
    /// successful poll. Replacing this map makes absent and degraded providers stale.
    provider_observed_at_ms: HashMap<String, u64>,
    available: bool,
    /// Last connect/fetch error (observability: surfaced via route.quota_status).
    last_error: Option<String>,
    /// Provider/account keys whose most recent refresh degraded while a healthy window is retained.
    retained_degraded_keys: HashSet<String>,
}

type SharedQuotaCache = Arc<Mutex<QuotaCache>>;

/// The route-state store, shared by the select handler (reader). Single-writer:
/// this module holds the store's lease for its lifetime.
type SharedStore = Arc<SqliteRouteStateStore>;
/// Shared read of `min_best_effort_adequacy` from config. Written once at boot;
/// read on every `route.select`.
type SharedMinBand = Arc<std::sync::Mutex<Option<f64>>>;
type RouteEpochs = HashMap<u16, u32>;

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
    retained_degraded_keys: &mut HashSet<String>,
) {
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
            // retention: decode_failed self-recovers, upstream/rejected age out
            // via the freshness map.
            let dead_credential = replacement.error_class.as_deref().is_some_and(|class| {
                class == "credential_absent"
                    || class == "credential_unusable"
                    || class == "no_quota_reported"
            });
            if window.is_healthy() && !replacement.is_healthy() && !dead_credential {
                if retained_degraded_keys.insert(key.clone()) {
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
                }
                merged.push(window);
            } else {
                if replacement.is_healthy() {
                    retained_degraded_keys.remove(&key);
                }
                merged.push(replacement);
            }
        } else {
            merged.push(window);
        }
    }
    for (key, window) in fresh_by_key {
        if window.is_healthy() {
            retained_degraded_keys.remove(&key);
        }
        merged.push(window);
    }
    *existing = merged;
}

fn log_unknown_spend_funding(fresh: &[ProviderUsage]) {
    let unknown: BTreeSet<&str> = fresh
        .iter()
        .flat_map(|entry| entry.spend.iter())
        .filter(|pool| !pool.has_supported_funding())
        .map(|pool| pool.funding.as_str())
        .collect();
    if !unknown.is_empty() {
        eprintln!(
            "[prefrontal-routing] quota refresh ignored unsupported spend funding values: {unknown:?}"
        );
    }
    // A provider whose spendable pools are all empty walls even when other
    // pools hold funds (producer fact: deepseek carries 0 granted beside a
    // positive purchased balance). Rendered undistinguished, that wall reads
    // as a quota bug to anyone who can see the account balance, so name the
    // withheld funds explicitly whenever the wall-with-funds shape occurs.
    for entry in fresh {
        if entry.spend.is_empty() {
            continue;
        }
        let all_spendable_empty = entry
            .spend
            .iter()
            .filter(|pool| pool.has_supported_funding())
            .all(|pool| pool.remaining.is_non_positive());
        let withheld: Vec<String> = entry
            .spend
            .iter()
            .filter(|pool| !pool.has_supported_funding() && !pool.remaining.is_non_positive())
            .map(|pool| format!("{} ({})", pool.remaining.render(), pool.funding))
            .collect();
        if all_spendable_empty && !withheld.is_empty() {
            eprintln!(
                "[prefrontal-routing] provider {} walled on empty spendable pools while non-spendable pools hold {} - policy, not a quota bug (only granted/subscription funding may spend)",
                entry.provider,
                withheld.join(", ")
            );
        }
    }
}

fn apply_quota_refresh(cache: &mut QuotaCache, fresh: Vec<ProviderUsage>, observed_at_ms: u64) {
    // Insula extends funding additively. Report unsupported values once per poll,
    // while the core policy treats each corresponding pool as not granted.
    log_unknown_spend_funding(&fresh);
    // Freshness keys on the same resolved id the overlay keys on: the published
    // canonical slug first, wire name as fallback. Keying this map on the wire
    // name alone while the overlay resolves via apiProvider would mark a
    // provider's windows live under one name and stale under the other.
    cache.provider_observed_at_ms = fresh
        .iter()
        .filter(|entry| entry.is_healthy())
        // A preserved reading (stale disclosure present) carries usage and no
        // error, so it passes is_healthy - but it is a reading QTA has been
        // UNABLE to refresh, not a fresh success. The fast-variant upgrade
        // demands positive fresh evidence, and before the disclosure existed a
        // preserved entry was wire-identical to a fresh one and silently
        // qualified. Overlay pressure still uses the preserved windows
        // (correct: last-known beats nothing); only upgrade freshness excludes
        // them.
        .filter(|entry| entry.stale.is_none())
        .map(|entry| {
            let key = entry
                .api_provider
                .clone()
                .filter(|slug| !slug.is_empty())
                .unwrap_or_else(|| entry.provider.clone());
            (key, observed_at_ms)
        })
        .collect();
    merge_quota_windows(&mut cache.windows, fresh, &mut cache.retained_degraded_keys);
    cache.refreshed_at_ms = observed_at_ms;
    cache.available = true;
    cache.last_error = None;
}

fn record_quota_refresh_failure(cache: &mut QuotaCache, phase: &str, error: impl fmt::Display) {
    cache.available = false;
    cache.provider_observed_at_ms.clear();
    cache.last_error = Some(format!("{phase}: {error}"));
}

fn record_cooldown_update(
    store: &impl RouteStateStore,
    params: &RecordCooldownParams,
) -> prefrontal_routing_core::RouteStateResult<()> {
    let update = prefrontal_routing_core::RouteStateUpdate {
        cooldown_until: params.cooldown_until,
        quota_exhausted_until: params.quota_exhausted_until,
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

fn prune_expired_decisions(store: &impl RouteStateStore, now: u64) {
    let cutoff = now.saturating_sub(DECISION_RETENTION_MS) as i64;
    if let Err(error) = store.delete_decisions_older_than(cutoff) {
        tracing::debug!(error = %error, "decision retention cycle failed");
    }
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

#[tokio::main]
async fn main() -> Result<(), ModuleError> {
    let config = ModuleConfig::from_env()?;
    run(config).await
}

struct ModuleConfig {
    connection_file_path: PathBuf,
    module_id: String,
}

impl ModuleConfig {
    fn from_env() -> Result<Self, ModuleError> {
        let connection_file_path = parse_subc_arg(std::env::args_os().skip(1))?;
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
    let stream = connect_to_subc(&config.connection_file_path).await?;
    let (mut read_half, write_half) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
    let writer = tokio::spawn(drain_writer(write_half, rx));

    // The quota cache is fed by SEPARATE short-lived outbound consumer connections
    // to insula (the second, independent connection while a cycle runs).
    // It refreshes off the selection hot path; route.select reads the cached snapshot,
    // never blocking on a live cross-module call. Quota being absent is non-fatal
    // (seed-only routing).
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

/// Background task refreshing quota windows on an interval. Each cycle owns its
/// outbound connection; the connection is never held across the refresh sleep. The
/// fetch `.await` happens with NO lock held; the cache lock is taken only to merge
/// the fresh poll, so a slow/blocked quota call can never stall `route.select`.
/// The same cadence also prunes expired decision audit rows, avoiding another timer.
async fn quota_refresh_loop(
    connection_file_path: PathBuf,
    cache: SharedQuotaCache,
    store: SharedStore,
) {
    // The quota module is machine-global (ignores project_root), but route.open
    // needs a valid identity. A stable per-process temp dir suffices.
    let project_root = std::env::temp_dir().join("prefrontal-routing-quota-consumer");
    let _ = std::fs::create_dir_all(&project_root);

    loop {
        prune_expired_decisions(store.as_ref(), now_ms());
        let mut consumer = match QuotaConsumer::connect(&connection_file_path, &project_root).await
        {
            Ok(c) => c,
            Err(e) => {
                // Quota module not up yet (or route.open failed). Degrade to
                // seed-only routing and retry; do not crash the module. Record the
                // error so route.quota_status can surface why quota is unavailable.
                tracing::debug!(error = %e, "quota refresh cycle failed");
                if let Ok(mut guard) = cache.lock() {
                    record_quota_refresh_failure(&mut guard, "connect", &e);
                }
                tokio::time::sleep(QUOTA_RETRY_INTERVAL).await;
                continue;
            }
        };

        match consumer.usage_get().await {
            Ok(windows) => {
                // Replace healthy providers we just heard from, but retain providers absent
                // from this poll and healthy windows whose fresh replacement degraded.
                if let Ok(mut guard) = cache.lock() {
                    apply_quota_refresh(&mut guard, windows, now_ms());
                }
                // `usage_get` sends GOODBYE and shuts down the stream on success;
                // drop it before beginning the long idle interval as well.
                drop(consumer);
                tokio::time::sleep(QUOTA_REFRESH_INTERVAL).await;
            }
            Err(e) => {
                // A failed cycle is expected during transient daemon restarts. Keep
                // the last healthy windows, mark the live observation unavailable,
                // and reconnect from scratch on the retry interval.
                tracing::debug!(error = %e, "quota refresh cycle failed");
                if let Ok(mut guard) = cache.lock() {
                    record_quota_refresh_failure(&mut guard, "usage.get", &e);
                }
                drop(consumer);
                tokio::time::sleep(QUOTA_RETRY_INTERVAL).await;
            }
        }
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
    tokio::spawn(quota_refresh_loop(
        config.connection_file_path.clone(),
        Arc::clone(&quota_cache),
        Arc::clone(&store),
    ));
    let aa_credential_resolver: Arc<dyn prefrontal_ckcred_client::CredentialResolver> =
        Arc::new(prefrontal_ckcred_client::CkcredCredentialResolver::new(
            config.connection_file_path.clone(),
            std::env::current_dir().map_err(|error| {
                ModuleError::Message(format!("resolve routing credential project root: {error}"))
            })?,
            DEFAULT_MODULE_ID,
        ));
    tokio::spawn(aa_ingest::aa_ingest_loop(
        Arc::clone(&store),
        config_overrides::config_path(),
        aa_credential_resolver,
    ));

    // Merge the user's model-fact overrides (model_routing.models) from the module's
    // OWN config-home file onto the migration-applied seed. The module owns its
    // catalog incl overrides (NOT subc-mediated config, which is unwired). A missing
    // file is a no-op; a parse error is logged but never aborts boot (routing
    // degrades to the seed catalog).
    let override_path = config_overrides::config_path();
    match config_overrides::apply_overrides_from_config(store.as_ref(), &override_path) {
        Ok(outcome) => {
            if outcome.applied > 0 || outcome.reverted > 0 || !outcome.skipped.is_empty() {
                eprintln!(
                    "[prefrontal-routing] model-fact overrides: applied {}, reverted {}, skipped {:?}",
                    outcome.applied, outcome.reverted, outcome.skipped
                );
            }
        }
        Err(e) => {
            eprintln!("[prefrontal-routing] model-fact override read failed (using seed): {e}")
        }
    }

    // Read `min_best_effort_adequacy` from the same config file. Clamp to [0,1];
    // reject NaN loudly. Absent means use the module default (0.6).
    let catalog_poll_ms = match config_overrides::read_catalog_poll_ms(&override_path) {
        Ok(poll_ms) => poll_ms,
        Err(error) => {
            tracing::warn!(error = %error, "invalid catalog.poll_ms; using ten-minute default");
            config_overrides::DEFAULT_CATALOG_POLL_MS
        }
    };
    if let Ok(mut health) = catalog_health.lock() {
        health.set_poll_ms(catalog_poll_ms);
    }
    tokio::spawn(fusiform_catalog::catalog_refresh_loop(
        config.connection_file_path.clone(),
        Arc::clone(&store),
        Arc::clone(&catalog_health),
        catalog_poll_ms,
    ));

    let min_band: SharedMinBand = Arc::new(std::sync::Mutex::new(
        match config_overrides::read_min_best_effort_adequacy(&override_path) {
            Ok(Some(val)) => {
                eprintln!("[prefrontal-routing] min_best_effort_adequacy: {val}");
                Some(val)
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!(
                    "[prefrontal-routing] min_best_effort_adequacy config error (using default): {e}"
                );
                None
            }
        },
    ));

    let mut route_epochs = RouteEpochs::new();
    loop {
        let Some(frame) = read_frame(read_half)
            .await
            .map_err(|e| ModuleError::Message(e.to_string()))?
        else {
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
        launch_nonce: std::env::var("SUBC_LAUNCH_NONCE").ok(),
    })
    .map_err(ModuleError::Json)?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
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
            handle_control_request(frame, writer, route_epochs, catalog_health).await?;
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
            tokio::spawn(async move {
                let _ = handle_select_request(
                    frame,
                    route_epoch,
                    &writer,
                    &quota_cache,
                    &store,
                    &min_band,
                    &routing_config_path,
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
) -> Result<(), ModuleError> {
    let request =
        serde_json::from_slice::<ModuleControlRequest>(&frame.body).map_err(ModuleError::Json)?;
    match request {
        ModuleControlRequest::RouteBind {
            route_channel,
            epoch,
            ..
        } => {
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
            // Routing is stateless: the catalog is machine-global and selection depends
            // only on the request body, so there is no domain state that can wedge: a
            // reply on this real dispatch path is itself the liveness proof the prober wants.
            let mut report = HealthReport::ok();
            report.metrics = Some(json!({
                "fusiformCatalog": catalog_health
                    .lock()
                    .map(|health| health.snapshot(now_ms()))
                    .unwrap_or_else(|_| json!({ "reach": "never-reached", "lastError": "health lock poisoned" })),
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
    #[serde(default = "default_target_agent")]
    target_agent: String,
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
    /// Legacy callers carry raw subscription specs in this field. The module
    /// resolves them together with OAuth and curated provider facts.
    #[serde(default)]
    subscription_keys: Vec<String>,
    #[serde(default)]
    prefer_fast_specs: Vec<String>,
    #[serde(default)]
    routing_policy: ModelRoutingPolicyInput,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
    #[serde(default)]
    suppress_fast_variant: bool,
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

#[derive(Debug, Deserialize)]
struct SeedAaBody {
    params: aa_ingest::SeedAaParams,
}

/// Partial fact: absent fields keep the stored value on merge, and a NEW model
/// requires elo + eq (the two judgment axes nothing can default honestly).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelUpsertParams {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
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
    routing_policy: ModelRoutingPolicyInput,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
    #[serde(default)]
    suppress_fast_variant: bool,
}

#[derive(Debug, Deserialize)]
struct SelectPanelBody {
    params: SelectPanelParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SelectPanelParams {
    panel_spec: PanelSpec,
    #[serde(default)]
    selection: PanelSelection,
    #[serde(default)]
    roster: Vec<PanelRosterEntry>,
    #[serde(default)]
    package_tokens_estimate: Option<u32>,
    #[serde(default)]
    pro_member_max_package_tokens: Option<u32>,
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

fn provider_auth_types(raw: Option<&HashMap<String, String>>) -> HashMap<String, String> {
    if let Some(raw) = raw {
        return raw.clone();
    }
    let path = config_overrides::auth_json_path();
    match config_overrides::read_provider_auth_types(&path) {
        Ok(types) => types,
        Err(error) => {
            eprintln!(
                "[prefrontal-routing] provider auth read failed (treating non-curated providers as metered): {error}"
            );
            HashMap::new()
        }
    }
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
    }
}

fn resolve_select_request(params: &SelectParams) -> Result<ResolvedSelectRequest, NoRouteError> {
    let (requirements, required_capabilities) = if let Some(requirements) = params.requirements {
        (requirements, params.required_capabilities)
    } else {
        match resolve_launch_requirements(
            &params.target_agent,
            params.requested_requirements.as_ref(),
        ) {
            Ok(resolved) => (resolved.requirements, resolved.required_capabilities),
            Err(error) => {
                return Err(no_route_error(
                    RouteFailureReason::InvalidRequirements,
                    error.message,
                    ModelRequirements { iq: 0.0, eq: 0.0 },
                ));
            }
        }
    };

    let mut routing_policy = params.routing_policy.clone();
    routing_policy
        .subscription
        .extend(params.subscription_keys.iter().cloned());
    routing_policy
        .prefer_fast_when_relaxed
        .extend(params.prefer_fast_specs.iter().cloned());
    let policy = resolve_routing_policy(
        params.available_models.iter().cloned(),
        &routing_policy,
        &provider_auth_types(params.provider_auth_types.as_ref()),
        params.suppress_fast_variant,
    );
    if policy.connected_model_count == 0 {
        return Err(no_route_error(
            RouteFailureReason::NoAvailableModel,
            format!(
                "No models available to route {} (no connected providers detected).",
                params.target_agent
            ),
            requirements,
        ));
    }
    if policy.available_models.is_empty() {
        return Err(no_route_error(
            RouteFailureReason::NoAvailableModel,
            format!(
                "No models available to route {}: all {} connected models are excluded by model_routing.exclude. Loosen the exclude list.",
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
            }
        }
    };
    if let Some(elo) = params.elo {
        fact.scores.elo = elo;
    }
    if let Some(eq) = params.eq {
        fact.scores.eq = eq;
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
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ModelUpsertParams = match serde_json::from_slice::<ModelUpsertBody>(&frame.body) {
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
                "model_upsert requires non-empty providerID and modelID",
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
    let route_key = make_route_key(
        &params.provider_id,
        &params.model_id,
        params.variant.as_deref(),
    );
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

async fn handle_model_seed_aa(
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
    let params = match serde_json::from_slice::<SeedAaBody>(&frame.body) {
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
                "model_seed_aa requires non-empty providerID and modelID",
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
                &format!("model_seed_aa params not decodable: {error}"),
            )
            .await;
        }
    };
    let aa_config = match config_overrides::read_aa_config(routing_config_path) {
        Ok(config) => config,
        Err(error) => {
            return send_route_error(writer, ver, channel, epoch, corr, "invalid_request", &error)
                .await;
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    match aa_ingest::seed_aa(store, &params, &aa_config, now) {
        Ok(ok) => {
            let body =
                serde_json::to_vec(&json!({ "result": aa_ingest::seed_aa_result_json(&ok) }))
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
        Err(refusal) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                refusal.as_str(),
                refusal.message(),
            )
            .await
        }
    }
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
    match aa_ingest::list_inferred_json(store, now) {
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
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let facts = match store.get_catalog_facts() {
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
        Ok(Some(fact)) => json!({
            "known": true,
            // Capability reads preserve an absent flag as cannot-verify. Routing admission
            // remains conservative in its own path and may treat the same absence as no.
            "supportsVision": fact.supports_vision,
        }),
        Ok(None) => {
            warn_missing_session_model_once(&params.provider_id, &params.model_id);
            json!({ "known": false, "supportsVision": null })
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
    let resolved = resolve_routing_policy(
        params.available_models,
        &params.routing_policy,
        &provider_auth_types(params.provider_auth_types.as_ref()),
        params.suppress_fast_variant,
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
    fresh_quota_provider_ids: HashSet<String>,
    quota_signal_json: String,
}

#[derive(Clone)]
struct DecisionProvenance {
    quota_signal_json: String,
    policy_revision: String,
}

/// Summarize the cached overlay that the scorer consumed without serializing the
/// full quota response. A no-signal marker is explicit so it cannot be confused
/// with old rows written before the provenance columns existed.
fn quota_signal_json(
    snapshot: &ModelRouteStateSnapshot,
    windows: &[ProviderUsage],
    available: bool,
    refreshed_at_ms: u64,
    retained_degraded_count: usize,
    last_error: Option<&str>,
) -> String {
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
    let summary = if healthy_provider_count == 0 {
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
    // Retain cached windows even while the quota source is unavailable. Dropping
    // last-known pressure would make a transient outage look optimistically free.
    let (
        quota_windows,
        provider_observed_at_ms,
        quota_available,
        quota_refreshed_at_ms,
        retained_degraded_count,
        quota_last_error,
    ) = match quota_cache.lock() {
        Ok(guard) => (
            (!guard.windows.is_empty()).then(|| guard.windows.clone()),
            guard.provider_observed_at_ms.clone(),
            guard.available,
            guard.refreshed_at_ms,
            guard.retained_degraded_keys.len(),
            guard.last_error.clone(),
        ),
        Err(_) => (
            None,
            HashMap::new(),
            false,
            0,
            0,
            Some("cache poisoned".into()),
        ),
    };
    if let Some(windows) = &quota_windows {
        merge_quota_overlay(&mut snapshot, windows, &facts, quota_name_map());
    }
    let current_ms = now_ms();
    let fresh_quota_provider_ids = provider_observed_at_ms
        .into_iter()
        .filter(|(_, observed_at_ms)| {
            *observed_at_ms != 0
                && current_ms.saturating_sub(*observed_at_ms) <= QUOTA_FRESH_FOR_UPGRADE_MS
        })
        .filter_map(|(key, _)| {
            // The freshness map's key is the published canonical slug when QTA
            // serves one (already a routing providerID), else the wire name that
            // still needs the local map. Try the map first so legacy wire names
            // resolve; a miss means the key is already canonical.
            quota_name_map().get(&key).cloned().or(Some(key))
        })
        .collect();
    let quota_signal_json = quota_signal_json(
        &snapshot,
        quota_windows.as_deref().unwrap_or_default(),
        quota_available,
        quota_refreshed_at_ms,
        retained_degraded_count,
        quota_last_error.as_deref(),
    );
    Ok(PreparedSelectionInputs {
        now,
        facts,
        snapshot,
        fresh_quota_provider_ids,
        quota_signal_json,
    })
}

async fn handle_select_request(
    mut frame: Frame,
    route_epoch: u32,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    min_band: &SharedMinBand,
    routing_config_path: &Path,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    frame.header.epoch = route_epoch;
    let epoch = route_epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;

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
        let (available, refreshed_at_ms, providers, healthy, last_error) = match quota_cache.lock()
        {
            Ok(g) => (
                g.available,
                g.refreshed_at_ms,
                g.windows.len(),
                g.windows.iter().filter(|w| w.is_healthy()).count(),
                g.last_error.clone(),
            ),
            Err(_) => (false, 0, 0, 0, Some("cache poisoned".to_string())),
        };
        let body = serde_json::to_vec(&json!({ "result": {
            "available": available,
            "refreshedAtMs": refreshed_at_ms,
            "providerCount": providers,
            "healthyCount": healthy,
            "lastError": last_error,
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
        return handle_resolve_policy(frame, writer).await;
    }
    if envelope.method == ROUTE_MODEL_FACT_OP {
        return handle_model_fact(frame, writer, store).await;
    }
    if envelope.method == ROUTE_MODEL_FACTS_OP {
        return handle_model_facts(frame, writer, store).await;
    }
    if envelope.method == ROUTE_MODEL_UPSERT_OP {
        return handle_model_upsert(frame, writer, store).await;
    }
    if envelope.method == ROUTE_MODEL_SEED_AA_OP {
        return handle_model_seed_aa(frame, writer, store, routing_config_path).await;
    }
    if envelope.method == ROUTE_LIST_INFERRED_OP {
        return handle_list_inferred(frame, writer, store).await;
    }
    if envelope.method == ROUTE_LIST_UNSCORED_OP {
        return handle_list_unscored(frame, writer, store).await;
    }
    if envelope.method == ROUTE_SELECT_PANEL_OP {
        return handle_select_panel(frame, writer, quota_cache, store, routing_config_path).await;
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
    let resolved_request = match resolve_select_request(&params) {
        Ok(resolved) => resolved,
        Err(no_route) => {
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
            );
            let _ = store.record_decision(&decision);
            return send_no_route(writer, ver, channel, epoch, corr, &no_route).await;
        }
    };
    let providers_without_prompt_cache = match config_overrides::read_providers_without_prompt_cache(
        routing_config_path,
    ) {
        Ok(providers) => providers,
        Err(error) => {
            eprintln!(
                    "[prefrontal-routing] prompt-cache provider config reload failed (using default): {error}"
                );
            config_overrides::default_providers_without_prompt_cache()
        }
    };
    if let Err(error) = config_overrides::reload_live_multipliers(routing_config_path) {
        eprintln!(
            "[prefrontal-routing] live route multiplier reload failed (using no multipliers): {error}"
        );
    }
    let PreparedSelectionInputs {
        now,
        mut facts,
        snapshot,
        fresh_quota_provider_ids,
        quota_signal_json,
    } = match prepare_selection_inputs(quota_cache, store) {
        Ok(prepared) => prepared,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &e).await;
        }
    };
    let (routing_config, policy_revision) = match config_overrides::apply_live_selection_config(
        routing_config_path,
        &mut facts,
    ) {
        Ok(outcome) => (outcome.routing_config, outcome.policy_revision),
        Err(error) => {
            eprintln!(
                "[prefrontal-routing] live Elo routing config reload failed (using defaults): {error}"
            );
            (
                DEFAULT_ROUTING_CONFIG,
                config_overrides::default_policy_revision(),
            )
        }
    };
    let provenance = DecisionProvenance {
        quota_signal_json,
        policy_revision,
    };

    let subscription_keys: HashSet<String> = resolved_request
        .policy
        .subscription_keys
        .iter()
        .cloned()
        .collect();
    let available: HashSet<String> = resolved_request
        .policy
        .available_models
        .iter()
        .cloned()
        .collect();
    let exclude = params.exclude_route_keys.clone();
    let prefer_fast = resolved_request.policy.prefer_fast_specs.clone();
    let requirements = resolved_request.requirements;
    let required_capabilities = resolved_request.required_capabilities;
    let target_agent = params.target_agent.clone();
    let prefer_prompt_cache = params.prefer_prompt_cache;

    let min_best_effort_adequacy = min_band.lock().ok().and_then(|g| *g);

    let attempt = |snap: ModelRouteStateSnapshot| -> Result<SelectedModelRoute, NoRouteError> {
        let sub = subscription_keys.clone();
        select_model_route_with_config(
            SelectModelRouteInput {
                target_agent: target_agent.clone(),
                requirements,
                available_models: available.clone(),
                facts: facts.clone(),
                runtime_snapshot: Some(snap),
                exclude_route_keys: exclude.clone(),
                prefer_fast_specs: prefer_fast.clone(),
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
        )
    };

    // First attempt against the live snapshot.
    let selection = match attempt(snapshot.clone()) {
        Ok(mut selected) => {
            // Post-selection fast-variant upgrade (no-op if not opted in / not relaxed).
            if !prefer_fast.is_empty() {
                let is_subscription_route = |route: &ModelRoute| {
                    subscription_keys.contains(&route.route_key)
                        || subscription_keys.contains(&route.capacity_key)
                };
                if let Some(upgraded) = maybe_upgrade_to_fast_variant(
                    &FastVariantUpgradeInput {
                        selected: &selected,
                        facts: &facts,
                        snapshot: &snapshot,
                        requirements: &requirements,
                        required_capabilities,
                        available_models: &available,
                        prefer_fast_specs: &prefer_fast,
                        exclude_route_keys: &exclude,
                        is_subscription_route: Some(&is_subscription_route),
                        now,
                        quota_signal_fresh: fresh_quota_provider_ids
                            .contains(&selected.model.provider_id),
                    },
                    &routing_config,
                ) {
                    selected = upgraded;
                }
            }
            Ok(selected)
        }
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

    // Record the decision, then respond. The decisionID is returned ONLY after
    // record_decision commits (it is a single store txn that either commits or
    // errors), so a returned id always maps to a committed row; never an id whose
    // write then failed (the contract requirement). A failed selection records a
    // no-selection row and surfaces the typed routing failure.
    match selection {
        Ok(selected) => {
            let decision = decision_input_for(
                &target_agent,
                &requirements,
                Some(&selected),
                None,
                &params,
                Some(&provenance),
            );
            match store.record_decision(&decision) {
                Ok(decision_id) => {
                    let mut resolved_input = json!({
                        "requirements": requirements,
                        "availableModels": &resolved_request.policy.available_models,
                        "subscriptionKeys": &resolved_request.policy.subscription_keys,
                        "preferFastSpecs": &resolved_request.policy.prefer_fast_specs,
                    });
                    if let Some(required_capabilities) = required_capabilities {
                        resolved_input["requiredCapabilities"] = json!(required_capabilities);
                    }
                    let body = serde_json::to_vec(&json!({ "result": {
                        "selected": selected,
                        "decisionID": decision_id,
                        "resolvedInput": resolved_input,
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
                    send(writer, response).await
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
        Err(no_route) => {
            // Best-effort failed-decision audit row (ignore a store error here: the
            // routing failure is the real result to surface).
            let decision = decision_input_for(
                &target_agent,
                &requirements,
                None,
                Some(&no_route),
                &params,
                Some(&provenance),
            );
            let _ = store.record_decision(&decision);
            send_no_route(writer, ver, channel, epoch, corr, &no_route).await
        }
    }
}

async fn handle_select_panel(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    routing_config_path: &Path,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
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
    if let Err(e) = validate_panel_spec(&params.panel_spec) {
        return send_route_error(writer, ver, channel, epoch, corr, "invalid_request", &e.0).await;
    }
    if params.selection == PanelSelection::NPick {
        if params.package_tokens_estimate.is_none() {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "packageTokensEstimate is required when selection is n_pick",
            )
            .await;
        }
        if params.pro_member_max_package_tokens.is_none() {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "proMemberMaxPackageTokens is required when selection is n_pick",
            )
            .await;
        }
    }
    if let Err(error) = config_overrides::reload_live_multipliers(routing_config_path) {
        eprintln!(
            "[prefrontal-routing] live route multiplier reload failed (using no multipliers): {error}"
        );
    }
    let mut prepared = match prepare_selection_inputs(quota_cache, store) {
        Ok(prepared) => prepared,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &e).await;
        }
    };
    let (routing_config, policy_revision) = match config_overrides::apply_live_selection_config(
        routing_config_path,
        &mut prepared.facts,
    ) {
        Ok(outcome) => (outcome.routing_config, outcome.policy_revision),
        Err(error) => {
            eprintln!(
                "[prefrontal-routing] live Elo routing config reload failed (using defaults): {error}"
            );
            (
                DEFAULT_ROUTING_CONFIG,
                config_overrides::default_policy_revision(),
            )
        }
    };
    let provenance = DecisionProvenance {
        quota_signal_json: prepared.quota_signal_json.clone(),
        policy_revision,
    };
    let resolved_policy = resolve_routing_policy(
        params.available_models.iter().cloned(),
        &ModelRoutingPolicyInput {
            subscription: params.subscription_keys.clone(),
            ..ModelRoutingPolicyInput::default()
        },
        &provider_auth_types(None),
        false,
    );
    let subscription_keys: HashSet<String> =
        resolved_policy.subscription_keys.into_iter().collect();
    let available_models = resolved_policy.available_models.into_iter().collect();
    let panel = match select_panel_with_config(
        SelectPanelInput {
            panel_spec: params.panel_spec.clone(),
            selection: params.selection,
            roster: params.roster.clone(),
            package_tokens_estimate: params.package_tokens_estimate,
            pro_member_max_package_tokens: params.pro_member_max_package_tokens,
            facts: prepared.facts,
            runtime_snapshot: Some(prepared.snapshot),
            exclude_route_keys: params.exclude_route_keys.clone(),
            required_capabilities: params.required_capabilities,
            available_models,
            is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                subscription_keys.contains(&route.route_key)
                    || subscription_keys.contains(&route.capacity_key)
            })),
        },
        &routing_config,
    ) {
        Ok(panel) => panel,
        Err(e) => {
            return send_route_error(writer, ver, channel, epoch, corr, "invalid_request", &e.0)
                .await;
        }
    };
    let decision = panel_decision_input(&params, &panel, &provenance);
    let decision_id = match store.record_decision(&decision) {
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

fn panel_decision_input(
    params: &SelectPanelParams,
    panel: &SelectedPanel,
    provenance: &DecisionProvenance,
) -> DecisionInput {
    let causes = panel
        .shortfall
        .as_ref()
        .map(|shortfall| shortfall.causes.as_slice())
        .unwrap_or(&[]);
    let member_route_keys: Vec<&str> = panel
        .members
        .iter()
        .map(|member| member.route_key.as_str())
        .collect();
    let explanation = if params.selection == PanelSelection::NPick {
        json!({
            "members": member_route_keys,
            "causes": causes,
            "selection": "n_pick",
            "policy": panel.policy,
        })
    } else {
        json!({
            "members": member_route_keys,
            "causes": causes,
        })
    };
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
        rejected_json: serde_json::to_string(causes).unwrap_or_else(|_| "[]".into()),
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
                        "message": &failure.message,
                        "bestRejected": failure.best_rejected.as_ref(),
                        "earliestReset": failure.earliest_reset,
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
    /// completed | error | session_error | cancelled | interrupted | launch_failed
    outcome: String,
    /// Exact terminal send error surfaced by the harness, when one was available.
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordUsageParams {
    /// The message id: the idempotency key.
    id: String,
    route_key: String,
    capacity_key: String,
    #[serde(rename = "providerID")]
    provider_id: String,
    output_tokens: f64,
    duration_ms: f64,
    observed_at: f64,
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
    provider_wide: bool,
    #[serde(default)]
    increment_quota_error: bool,
    #[serde(default)]
    increment_failure: bool,
    #[serde(default)]
    last_error: Option<String>,
}

fn parse_outcome(s: &str) -> Option<RouteOutcome> {
    match s {
        "completed" => Some(RouteOutcome::Completed),
        "error" => Some(RouteOutcome::Error),
        "session_error" => Some(RouteOutcome::SessionError),
        "cancelled" => Some(RouteOutcome::Cancelled),
        "interrupted" => Some(RouteOutcome::Interrupted),
        "launch_failed" => Some(RouteOutcome::LaunchFailed),
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
    let Some(outcome) = parse_outcome(&params.outcome) else {
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
    let failure_backoff = match config_overrides::read_failure_backoff_config(routing_config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("[prefrontal-routing] failure backoff config error (using default): {error}");
            FailureBackoffConfig::default()
        }
    };
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
    let params: RecordUsageParams = match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body)
    {
        Ok(b) => b.params,
        Err(e) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("record_usage params not decodable: {e}"),
            )
            .await;
        }
    };
    let obs = prefrontal_routing_core::UsageObservation {
        id: params.id,
        route_key: params.route_key,
        capacity_key: params.capacity_key,
        provider_id: params.provider_id,
        output_tokens: params.output_tokens,
        duration_ms: params.duration_ms,
        observed_at: params.observed_at,
    };
    match store.record_usage_observation(&obs) {
        // `recorded` reports whether a NEW row was inserted (false = gated out or a
        // duplicate); either way the op succeeded, so the consumer never retries it.
        Ok(recorded) => {
            let body = serde_json::to_vec(&json!({ "result": { "recorded": recorded } }))
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
            send(writer, response).await
        }
        Err(e) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("record_usage: {e}"),
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
            let retention_days = DECISION_RETENTION_MS / (24 * 60 * 60_000);
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

/// Map a typed `NoRouteError` to an Error frame, preserving the reason code so a
/// consumer can branch (e.g. break-glass on quota/cooldown) the same way the TS
/// caller does on the typed error. For `AllRoutesInadequate`, the error body also
/// carries the best rejected candidate and earliest reset timestamp.
async fn send_no_route(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    err: &NoRouteError,
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
        err.message.clone()
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
    // Build the error body. For AllRoutesInadequate, include structured fields
    // alongside the standard code/message so future clients can parse them.
    let body_value = if err.reason == RouteFailureReason::AllRoutesInadequate {
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
        serde_json::Value::Object(obj)
    } else {
        json!({ "code": code, "message": message })
    };
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
            | ROUTE_MODEL_UPSERT_OP
            | ROUTE_MODEL_SEED_AA_OP
            | ROUTE_LIST_INFERRED_OP
            | ROUTE_LIST_UNSCORED_OP
            | ROUTE_SELECT_PANEL_OP
            | ROUTE_SET_DECISION_OUTCOME_OP
            | ROUTE_RECORD_USAGE_OP
            | ROUTE_RECORD_COOLDOWN_OP
            | ROUTE_CLEAR_FAILURE_BACKOFF_OP
            | ROUTE_COOLDOWN_STATUS_OP
            | ROUTE_EXPLAIN_OP
            | ROUTE_DECISIONS_LIST_OP
            | QUOTA_STATUS_OP
    )
}

/// The module's capability manifest: a ManagementSurface exposing routing operations.
///
/// The catalog and route-state tables are machine-global, so `identity_scope` is
/// empty and the identity binding requires nothing. The routing module owns this
/// schema and is the sole writer for its provider and route cooldown state.
fn manifest(module_id: &str) -> ModuleManifest {
    // ModuleManifest went #[non_exhaustive] with a mandatory builder in
    // subc-protocol (sibling commit 9f3e6faa); optional declarations stay at
    // their absent defaults: no capabilities, no self-signals, and no
    // provenance until the deploy script injects CK_BUILD_* facts.
    ModuleManifest::builder(
        module_id,
        env!("CARGO_PKG_VERSION"),
        TrustTier::FirstParty,
        Bindings {
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
        },
    )
    .protocol_ver(PROTOCOL_VERSION)
    .provides(vec![ProviderRole::ManagementSurface {
        // Deliberate lane, not a default: routing serves concurrent
        // selection reads; Serial would put every route decision in
        // one queue.
        concurrency: Concurrency::ModuleManaged,
        operations: vec![
            // route.select is now a MUTATE: it records the decision it returns.
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
                name: ROUTE_MODEL_UPSERT_OP.to_string(),
                kind: ManagementOperationKind::Mutate,
                description: None,
            },
            ManagementOperation {
                name: ROUTE_MODEL_SEED_AA_OP.to_string(),
                kind: ManagementOperationKind::Mutate,
                description: Some(
                    "Seed a provisional AA-inferred score for one router key (primary-only)"
                        .to_string(),
                ),
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
        ],
        config_schema: json!({ "type": "object" }),
        observability: Vec::new(),
        identity_scope: Vec::new(),
    }])
    .build()
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
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use prefrontal_routing_core::model_facts::{Confidence, ModelFact, ModelScores};
    use prefrontal_routing_core::FactSource;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

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

        let value: serde_json::Value =
            serde_json::from_str(&quota_signal_json(&snapshot, &windows, true, 1, 0, None))
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
            let dir = std::env::temp_dir().join(format!(
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

    fn upsert_params(provider: &str, model: &str) -> ModelUpsertParams {
        ModelUpsertParams {
            provider_id: provider.into(),
            model_id: model.into(),
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

    fn fact(provider: &str, model: &str) -> ModelFact {
        ModelFact {
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
        }
    }

    fn usage(v: serde_json::Value) -> ProviderUsage {
        serde_json::from_value(v).expect("provider usage")
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
                    "project_root": std::env::temp_dir(),
                    "harness": "epoch-rebind-test",
                    "session": "session-1"
                }
            }))
            .unwrap();
            let frame =
                Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body).unwrap();

            handle_control_request(frame, &writer, &mut route_epochs, &catalog_health)
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
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
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
            json!({ "known": true, "supportsVision": true })
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
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .expect("open fixture database")
            .execute(
                "UPDATE model_facts SET excluded = 1 WHERE route_key = 'fixture/wire-only'",
                [],
            )
            .expect("mark the fixture route excluded");

        let response = run_route_wire_request(
            &store,
            json!({ "method": ROUTE_MODEL_FACTS_OP, "params": {} }),
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
        assert_eq!(fact["excluded"], true);
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

        let mut retained_degraded_keys = HashSet::new();
        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

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
        assert!(retained_degraded_keys.contains(&quota_cache_key(&cached[0])));
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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

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
            retained_degraded_keys.is_empty(),
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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_some(),
            "degraded no_quota_reported entry replaces the stale healthy window"
        );
        assert_eq!(cached[0].error_class.as_deref(), Some("no_quota_reported"));
        assert!(
            retained_degraded_keys.is_empty(),
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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

        assert_eq!(cached.len(), 1);
        assert!(
            cached[0].error.is_some(),
            "degraded credential_absent entry replaces the stale healthy window"
        );
        assert_eq!(cached[0].error_class.as_deref(), Some("credential_absent"));
        assert!(
            retained_degraded_keys.is_empty(),
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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

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
        let mut retained_degraded_keys = HashSet::new();

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

        assert_eq!(
            cached[0]
                .usage
                .as_ref()
                .and_then(|usage| usage.primary.as_ref())
                .map(|window| window.used_percent),
            Some(88.0)
        );
        assert!(retained_degraded_keys.contains(&quota_cache_key(&cached[0])));
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
        assert_eq!(cache.windows[0].provider, "balance-wire");
        assert_eq!(
            cache.provider_observed_at_ms.get("balance-provider"),
            Some(&10),
            "pool-only entries are successful quota observations, not degraded usage={{}}"
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
        cache.provider_observed_at_ms.insert("codex".into(), 10);

        record_quota_refresh_failure(&mut cache, "usage.get", "daemon restarted");

        assert_eq!(cache.windows, retained);
        assert!(!cache.available);
        assert!(cache.provider_observed_at_ms.is_empty());
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
        )
        .await
        .unwrap();
        (rx.recv().await.unwrap(), rig)
    }

    #[tokio::test]
    async fn refusal_decision_row_carries_per_candidate_rejections() {
        let rig = Rig::new("refusal-decision-evidence");
        let store = Arc::new(rig.open_store());
        let mut rejected = fact("p", "missing-vision");
        rejected.supports_vision = Some(false);
        store.upsert_fact(&rejected, FactSource::Override).unwrap();
        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
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

    #[tokio::test]
    async fn route_select_reports_empty_and_policy_excluded_availability() {
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
        assert_eq!(excluded.header.ty, FrameType::Error);
        let excluded_body: serde_json::Value = serde_json::from_slice(&excluded.body).unwrap();
        assert_eq!(excluded_body["code"], "no_available_model");
        assert!(excluded_body["message"]
            .as_str()
            .unwrap()
            .contains("all 1 connected models are excluded"));
    }

    #[tokio::test]
    async fn route_select_resolves_raw_launch_policy_before_shared_selection() {
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
                    "subscription": ["opencode-go", "-opencode-go/deepseek-v4-flash"],
                    "preferFastWhenRelaxed": ["openai", "opencode-go/minimax-m2.5"]
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
                    "opencode-go/deepseek-v4-flash",
                    "opencode-go/minimax-m2.5"
                ],
                "subscriptionKeys": ["opencode-go/minimax-m2.5"],
                "preferFastSpecs": ["openai", "opencode-go/minimax-m2.5"]
            })
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
        let retention_days = DECISION_RETENTION_MS / (24 * 60 * 60_000);
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
                "availableModels": ["openai/gpt-5.5", "opencode-go/minimax-m2.5"],
                "routingPolicy": {
                    "exclude": ["openai"],
                    "subscription": ["opencode-go"],
                    "preferFastWhenRelaxed": []
                },
                "providerAuthTypes": { "openai": "oauth", "opencode-go": "api" }
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
                "availableModels": ["opencode-go/minimax-m2.5"],
                "subscriptionKeys": ["opencode-go/minimax-m2.5"],
                "preferFastSpecs": [],
                "connectedModelCount": 2,
            })
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
        async fn select(demand: f64) -> (Frame, Rig) {
            let rig = Rig::new(&format!("wire-demand-{demand}"));
            let store = Arc::new(rig.open_store());
            let mut route = fact("p", "fact-53");
            route.scores.elo = 53.0;
            store.upsert_fact(&route, FactSource::Override).unwrap();
            let request = json!({
                "method": ROUTE_SELECT_OP,
                "params": {
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
    async fn empty_refresh_retains_pressure_but_revokes_fast_upgrade_freshness() {
        let rig = Rig::new("empty-refresh-fast-freshness");
        let store = Arc::new(rig.open_store());
        let base = fact("openai", "freshness-base");
        let mut fast = fact("openai", "freshness-base-fast");
        fast.cost_per_m_tok_out = Some(30.0);
        store.upsert_fact(&base, FactSource::Override).unwrap();
        store.upsert_fact(&fast, FactSource::Override).unwrap();

        let observed_at_ms = now_ms();
        let mut cache = QuotaCache::default();
        apply_quota_refresh(
            &mut cache,
            vec![usage(json!({
                "provider": "codex",
                "usage": {
                    "primary": {
                        "usedPercent": 1.0,
                        "resetsAt": "2099-01-01T00:00:00Z"
                    }
                }
            }))],
            observed_at_ms,
        );
        apply_quota_refresh(&mut cache, Vec::new(), observed_at_ms.saturating_add(1));
        assert_eq!(
            cache.windows.len(),
            1,
            "the low-pressure window is retained"
        );
        assert!(cache.provider_observed_at_ms.is_empty());
        let quota_cache = Arc::new(Mutex::new(cache));
        let prepared = prepare_selection_inputs(&quota_cache, &store).unwrap();
        assert!(
            prepared.snapshot.by_capacity["openai/freshness-base-fast"]
                .quota_pressure
                .is_some_and(
                    |pressure| pressure <= prefrontal_routing_core::FAST_UPGRADE_MAX_PRESSURE
                )
        );
        assert!(!prepared.fresh_quota_provider_ids.contains("openai"));

        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
                "targetAgent": "mason",
                "requirements": { "iq": 50, "eq": 0 },
                "availableModels": [
                    "openai/freshness-base",
                    "openai/freshness-base-fast"
                ],
                "subscriptionKeys": ["openai/freshness-base"],
                "preferFastSpecs": ["openai/freshness-base"],
                "taskId": "empty-refresh-fast-freshness"
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
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body["result"]["selected"]["model"]["modelID"],
            "freshness-base"
        );
    }

    /// A preserved reading (QTA's `stale` disclosure) carries usage and no
    /// error, so it is wire-identical to a fresh success everywhere EXCEPT the
    /// disclosure - and the fast-upgrade freshness map must treat it as
    /// not-fresh: the upgrade demands positive evidence of a relaxed window,
    /// and a reading the producer has been unable to refresh is not that.
    /// Overlay pressure keeps using the preserved windows.
    #[test]
    fn preserved_stale_reading_keeps_windows_but_is_not_upgrade_fresh() {
        let observed_at_ms = now_ms();
        let mut cache = QuotaCache::default();
        apply_quota_refresh(
            &mut cache,
            vec![
                usage(json!({
                    "provider": "codex",
                    "usage": { "primary": { "usedPercent": 1.0 } },
                    "fetchedAt": "2026-08-13T16:03:50Z",
                    "stale": { "since": "2026-08-13T16:04:50Z", "class": "upstream_failed" }
                })),
                usage(json!({
                    "provider": "gemini",
                    "usage": { "primary": { "usedPercent": 2.0 } }
                })),
            ],
            observed_at_ms,
        );
        assert_eq!(cache.windows.len(), 2, "preserved windows still serve");
        assert!(
            !cache.provider_observed_at_ms.contains_key("codex"),
            "a preserved reading must not qualify its provider as upgrade-fresh"
        );
        assert!(
            cache.provider_observed_at_ms.contains_key("gemini"),
            "a fresh reading (no disclosure) stays upgrade-fresh"
        );
    }

    #[tokio::test]
    async fn legacy_roster_wire_is_byte_identical_to_fixture() {
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
        assert_eq!(response.header.ty, FrameType::Response);
        let response_value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert!(response_value["result"].get("policy").is_none());
        let decision_id = response_value["result"]["decisionID"].as_str().unwrap();
        let expected = vectors["legacyRoster"]["reply"]
            .as_str()
            .unwrap()
            .replace("$decisionID", decision_id);
        assert_eq!(response.body, expected.as_bytes());
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
        assert_eq!(response.body, expected.as_bytes());

        let decisions = store.get_decisions_for_task("panel-wire-n-pick").unwrap();
        assert_eq!(decisions.len(), 1);
    }

    #[test]
    fn n_pick_decision_explanation_includes_selection_and_policy() {
        let vectors = select_panel_wire_vectors();
        let request = vectors["nPick"]["request"].as_str().unwrap();
        let params = serde_json::from_str::<SelectPanelBody>(request)
            .unwrap()
            .params;
        let policy = prefrontal_routing_core::PanelPolicy {
            selection: prefrontal_routing_core::PanelSelectionPolicy::TopIq,
            seated_variants: vec![prefrontal_routing_core::SeatedVariant {
                slot: 0,
                route_key: "openai/gpt-5.6-sol-pro".into(),
                variant: prefrontal_routing_core::SeatedSiblingVariant::Pro,
            }],
            pro_downgraded_for_size: false,
            sibling_collapses: Vec::new(),
        };
        let panel = SelectedPanel {
            members: Vec::new(),
            runner_up: Vec::new(),
            shortfall: None,
            policy: Some(policy),
        };
        let provenance = DecisionProvenance {
            quota_signal_json: r#"{"state":"none","reason":"quota signal: none"}"#.into(),
            policy_revision: "test-policy".into(),
        };
        let decision = panel_decision_input(&params, &panel, &provenance);
        let explanation: serde_json::Value =
            serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
        assert_eq!(explanation["selection"], "n_pick");
        assert_eq!(explanation["policy"]["selection"], "top_iq");
    }

    #[tokio::test]
    async fn n_pick_missing_gate_fields_are_typed_fixture_errors() {
        let vectors = select_panel_wire_vectors();
        for invalid in vectors["invalidNPick"].as_array().unwrap() {
            let missing_field = invalid["missingField"].as_str().unwrap();
            let request = invalid["request"].as_str().unwrap();
            let (response, _rig, _store) =
                run_select_panel_wire_request(&format!("invalid-n-pick-{missing_field}"), request)
                    .await;
            assert_eq!(response.header.ty, FrameType::Error);
            assert_eq!(response.body, invalid["reply"].as_str().unwrap().as_bytes());
            let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(error.code, "invalid_request");
            assert!(error.message.contains(missing_field));
        }
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
                "roster": [
                    {"model": "openai/gpt-5.6-sol", "promptParams": {"variant": "xhigh"}},
                    {"model": "openai/gpt-5.5", "promptParams": {"variant": "high"}}
                ],
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
        assert!(result.get("shortfall").is_none());

        let decisions = store.get_decisions_for_task("panel-contract-task").unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].decision_kind, "panel");
        assert_eq!(decisions[0].selected_route_key, None);
    }

    #[tokio::test]
    async fn next_selection_observes_a_live_elo_override_edit() {
        async fn select(
            store: &SharedStore,
            quota_cache: &SharedQuotaCache,
            config_path: &Path,
        ) -> String {
            let request = json!({
                "method": ROUTE_SELECT_OP,
                "params": {
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
        let config_path = rig.dir.join("alfonso-routing.jsonc");
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
        let mut retained_degraded_keys = HashSet::from([quota_cache_key(&cached[0])]);

        merge_quota_windows(&mut cached, fresh, &mut retained_degraded_keys);

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
        assert!(!retained_degraded_keys.contains(&quota_cache_key(&cached[0])));
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
        assert!(handles(ROUTE_MODEL_SEED_AA_OP));
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
    async fn route_model_seed_aa_and_list_inferred_wire() {
        let rig = Rig::new("seed-aa-wire");
        let store = Arc::new(rig.open_store());
        store
            .replace_aa_index_table(&[("new-slug".into(), 60.0)], 1)
            .unwrap();
        let config_path = rig.dir.join("alfonso-routing.jsonc");
        std::fs::write(
            &config_path,
            r#"{ "aa": { "model_map": { "custom/wire-inferred": "new-slug" } } }"#,
        )
        .unwrap();

        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({
                "method": ROUTE_MODEL_SEED_AA_OP,
                "params": { "providerID": "custom", "modelID": "wire-inferred" }
            }))
            .unwrap(),
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
            &config_path,
        )
        .await
        .unwrap();
        let response = rx.recv().await.unwrap();
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["provenance"], "aa_inferred");

        let listed = run_route_wire_request(
            &store,
            json!({ "method": ROUTE_LIST_INFERRED_OP, "params": {} }),
        )
        .await;
        assert_eq!(listed.header.ty, FrameType::Response);
        let listed_body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
        assert_eq!(
            listed_body["result"]["inferred"][0]["routeKey"],
            "custom/wire-inferred"
        );
        assert!(listed_body["result"]["unscored"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn aa_inferred_is_admitted_and_unscored_is_not_replayable() {
        let rig = Rig::new("aa-inferred-admit");
        let store = Arc::new(rig.open_store());
        let mut inferred = fact("custom", "aa-live");
        inferred.scores.elo = 1_800.0;
        inferred.score_provenance = ScoreProvenance::AaInferred;
        inferred.aa_index = Some(55.0);
        store
            .upsert_fact(&inferred, FactSource::AaInferred)
            .unwrap();
        let mut unscored = fact("custom", "no-score");
        unscored.scores.elo = 2_000.0;
        unscored.score_provenance = ScoreProvenance::Unscored;
        store.upsert_fact(&unscored, FactSource::Unscored).unwrap();

        let request = json!({
            "method": ROUTE_SELECT_OP,
            "params": {
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
}
