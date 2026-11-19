//! Common harness for the legacy routing wire tests.

mod catalog_targets_contract {
    use super::*;
    include!("catalog_targets_tests.rs");
}
include!("panel_policy_tests.rs");
include!("replay_model_facts_tests.rs");
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

use striatum_core::model_facts::{Confidence, ModelFact, ModelScores};
use striatum_core::model_router::select_model_route_with_config;
use striatum_core::{FactSource, NativeUsageFacts, UsageObservation};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

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

pub(super) fn usage(v: serde_json::Value) -> ProviderUsage {
    serde_json::from_value(v).expect("provider usage")
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

async fn run_select_panel_wire_request(label: &str, request: &str) -> (Frame, Rig, SharedStore) {
    let rig = Rig::new(label);
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        serde_json::from_str(request).expect("select-panel request JSON"),
    )
    .await;
    (response, rig, store)
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

fn set_config_mtime(path: &Path, seconds: u64) {
    let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    file.set_times(
        std::fs::FileTimes::new()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)),
    )
    .unwrap();
}

mod catalog_tests;
mod operator_tests;
mod outcome_tests;
mod quota_cache_tests;
mod selection_tests;
mod startup_tests;
mod transport_tests;
mod winner_ladder_tests;
