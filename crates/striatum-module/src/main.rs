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
mod background_loop_supervision_tests;
#[cfg(test)]
mod baseline_tests;
mod broca_credentials;
mod catalog_handlers;
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
mod operator_commands;
mod panel_policy;
mod project_settings;
mod quota_bus;
mod quota_cache;
mod quota_consumer;
mod quota_events;
mod retention;
mod route_facts;
mod route_operator;
mod route_outcomes;
mod route_transport;
mod routing_clock;
mod routing_v1;
mod runtime_lag;
mod select;
mod select_concurrency;
mod select_policy;
mod service_tiers;
mod serving_index;
mod shutdown;
#[cfg(test)]
mod slow_route_select_timing_tests;
mod stage_timing;
mod startup_settings;
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
#[cfg(test)]
mod tests;
mod tier_settings;
mod usage_facts;
mod variant_policy;
mod winner_ladder;
mod work_model_pin;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    error::Error,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Once, OnceLock},
    time::{Duration, Instant},
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
    decision_input_for, handle_select_request, policy_sources_line, prepare_selection_inputs,
    provider_auth_types, remember_provider_identity, warn_ignored_caller_policy_once,
    DecisionProvenance, PreparedSelectionInputs, SelectParams, SelectionPolicySources,
};

use broca_credentials::{BrocaCredentials, SharedBrocaCredentials};
use catalog_handlers::*;
use cortexkit_store_types::StorageDescriptor;
use fusiform_catalog::{CatalogReachHealth, SharedCatalogHealth};
use quota_cache::*;
use route_operator::*;
use route_outcomes::*;
use route_transport::*;
use routing_clock::now_ms;
#[cfg(test)]
pub(crate) use routing_clock::InjectedClock;
#[cfg(test)]
use routing_clock::INJECTED_NOW_MS;
use serde::{Deserialize, Serialize};
use serde_json::json;
use startup_settings::{check_config_at, ModuleConfig};
use striatum_core::launch_policy::resolve_routing_policy_with_catalog_targets;
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
    RouteOutcome, RouteStateStore, RouteWallClass, SelectPanelInput, SelectedPanel,
};
#[cfg(test)]
use striatum_core::{
    resolve_launch_requirements, routing_config::DEFAULT_ROUTING_CONFIG,
    RequestedModelRequirements, SelectModelRouteInput,
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

/// The route-state store, shared by the select handler (reader). Single-writer:
/// this module holds the store's lease for its lifetime.
type SharedStore = Arc<SqliteRouteStateStore>;
/// Shared read of `min_best_effort_adequacy` from config. Written once at boot;
/// read on every `route.select`.
type SharedMinBand = Arc<std::sync::Mutex<Option<f64>>>;

#[cfg(test)]
#[path = "test_thread_state/tests.rs"]
mod test_thread_state;
#[cfg(test)]
use test_thread_state::TestThreadState;

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
fn main() -> Result<(), ModuleError> {
    initialize_logging();
    shutdown::run_main()
}

fn initialize_logging() {
    logging::install();
}

async fn run(
    config: ModuleConfig,
    shutdown: &shutdown::Shutdown,
    store: &mut Option<SharedStore>,
) -> Result<(), ModuleError> {
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
    // cache. Without a subscription to quota events, refresh from `usage.get`
    // every 60 seconds, even if the SDK waits for a replacement connection
    // without returning an error.
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
        shutdown,
        store,
    )
    .await;
    shutdown.begin();
    drop(read_half);
    drop(tx);

    let writer_result = writer
        .await
        .map_err(|e| ModuleError::Message(e.to_string()));
    // The writer closes only after the in-flight request handlers have dropped
    // their reply senders. Flush their final diagnostic increments together.
    if let Some(store) = store {
        background::flush_call_counts(Arc::clone(store)).await;
    }
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
    shutdown: &shutdown::Shutdown,
    store: &mut Option<SharedStore>,
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
    let store = &*store.insert(engram_catalog::open_route_state_store(&descriptor)?);
    // HELLO negotiates the storage descriptor supplied by HELLO_ACK, so it
    // cannot wait for loading. Do not start background tasks or dispatch queued
    // requests until account state is published; early requests wait, not fail.
    // The load blocks on the account writer, so it runs on the blocking pool,
    // never on a runtime worker. A failed load does not stop the module: the
    // writer stays up, and each account request or Tick loads again.
    let loading = Arc::clone(store);
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
    let config_path = config_overrides::config_path();
    config_overrides::attach_store_catalog(&config_path, store)
        .map_err(|error| ModuleError::Message(format!("{}: {error}", config_path.display())))?;
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
    } = background::start(config, store, &quota_cache, &catalog_health).await?;

    let min_band: SharedMinBand = Arc::new(std::sync::Mutex::new(
        config_overrides::live_config(&override_path).min_band(),
    ));

    let mut route_epochs = RouteEpochs::new();
    let mut profiles_started = false;
    let result = loop {
        let frame = match next_frame_or_loop_stop(read_half, &mut loops).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        };
        match handle_frame(
            frame,
            &writer,
            &quota_cache,
            store,
            &mut route_epochs,
            &min_band,
            &override_path,
            &operator_config_path,
            &broca_credentials,
            &catalog_health,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => break Ok(()),
            Err(error) => break Err(error),
        }
        if !profiles_started && !route_epochs.is_empty() {
            // Start only after the first bind ACK is queued, never during store open.
            loops.spawn(
                "task profile refresh",
                task_profile_refresh::refresh_loop(
                    Arc::clone(store),
                    override_path.clone(),
                    Arc::clone(&broca_credentials),
                ),
            );
            profiles_started = true;
        }
    };
    shutdown.begin();
    let flushed = quota_events::finish_clean_exit(loops, quota_cache, Arc::clone(store))
        .await
        .map_err(ModuleError::Message);
    result.and(flushed)
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
        // `route.select` records the routing decision it returns.
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
