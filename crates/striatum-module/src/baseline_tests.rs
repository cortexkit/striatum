//! The conversion-equivalence baseline: today's route selection, run on the
//! owner's scrubbed live routing config and a frozen copy of everything else a
//! selection reads.
//!
//! `fixtures/routing-baseline-2026-10-24.json` holds the frozen inputs (the
//! Fusiform catalog rows, plan-price providers, the vault listing, the Insula
//! quota reading, OpenCode's auth.json, the host's available models and the
//! clock), the requests, and each request's checked-in answer: route key,
//! variant, runner and refusal code. The routing policy rebuild converts the
//! config to its new shape and compares its answers against these.
//!
//! Every request runs from its own empty store with the module's production
//! entry points: the store is opened with its migrations, the catalog is applied
//! as the Fusiform poll applies it, the vault listing and quota reading go
//! through the same decoders and merges as the live loops, and the request is
//! answered by the same handler a route frame reaches. The clock is injected
//! ([`crate::InjectedClock`]), so the frozen readings are as fresh as they were
//! at capture whenever the test runs.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cortexkit_claustrum_client::{
    ClaustrumCredentialResolver, CredentialGetTarget, CredentialResolver, CredentialResolverError,
    CredentialResolverFuture, ScopedCredentialListing,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use striatum_core::quota::ProviderUsage;
use striatum_core::route_state_store::FactSource;
use striatum_core::{ModelFact, RouteStateStore};
use striatum_store::{
    dev_descriptor, FusiformCatalogCursor, FusiformFactUpdate, SqliteRouteStateStore,
};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::account_quota::{self, AccountQuotaState};
use crate::broca_credentials::{BrocaCredentials, SharedBrocaCredentials};
use crate::fusiform_catalog::PlaneCatalog;
use crate::{QuotaCache, SharedQuotaCache, SharedStore, DEFAULT_MODULE_ID};

const CONFIG: &str = include_str!("../fixtures/routing-config-live-2026-10-24.jsonc");
const BASELINE: &str = include_str!("../fixtures/routing-baseline-2026-10-24.json");

static NEXT_WORLD: AtomicU64 = AtomicU64::new(0);

fn baseline() -> Value {
    serde_json::from_str(BASELINE).expect("the baseline fixture is JSON")
}

fn clock_ms(baseline: &Value) -> u64 {
    baseline["clock_ms"].as_u64().expect("clock_ms")
}

/// One fresh routing module state built from the frozen inputs: what the
/// module holds after startup, a catalog poll, a vault listing poll and a
/// quota poll, all at the frozen clock.
struct World {
    dir: PathBuf,
    store: SharedStore,
    config_path: PathBuf,
    credentials: SharedBrocaCredentials,
    quota_cache: SharedQuotaCache,
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl World {
    async fn build(baseline: &Value, label: &str) -> Self {
        Self::build_with(baseline, label, CONFIG).await
    }

    async fn build_with(baseline: &Value, label: &str, config: &str) -> Self {
        let now = clock_ms(baseline);
        let n = NEXT_WORLD.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-baseline-{label}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store: SharedStore = Arc::new(
            SqliteRouteStateStore::open(&dev_descriptor(
                DEFAULT_MODULE_ID,
                dir.join("store.db").to_string_lossy().into_owned(),
            ))
            .expect("open the routing store"),
        );
        let config_path = dir.join("striatum.jsonc");
        let mut archived: Value =
            serde_json::from_str(&subc_jsonc::jsonc_to_json(config).unwrap()).unwrap();
        // Keep the captured settings bytes for provenance; the replay cannot
        // load the historical effort override table on current routing.
        if let Some(policy) = archived
            .get_mut("model_routing")
            .and_then(Value::as_object_mut)
        {
            policy.remove("variants");
        }
        let mut fixture_config = crate::config_convert::convert_value(archived)
            .expect("the fixture config converts after removing the retired effort table");
        // Authorize the fixture's attested callers without changing its frozen
        // requests, policy choices, or expected execution identities.
        fixture_config["consumers"]["mc_dream_curate"]["owner"] = json!("magic-context");
        fixture_config["consumers"]["mason"]["owner"] = json!("prefrontal-core");
        fixture_config["consumers"]["codemode"]["owner"] = json!("prefrontal-core");
        crate::test_config::write(&config_path, serde_json::to_vec(&fixture_config).unwrap())
            .unwrap();

        let listing = decode_listing(baseline["credential_listing"].clone()).await;
        let mapped: HashSet<String> = listing
            .credentials
            .iter()
            .filter(|row| row.state == "active")
            .flat_map(|row| row.provider_ids.iter().cloned())
            .collect();
        install_catalog(&store, &baseline["catalog"], &mapped, now);

        // The config is checked against the stored catalog's published
        // efforts, as the module does once its store opens.
        let catalog_store = Arc::clone(&store);
        crate::config_overrides::attach_reasoning_catalog(
            &config_path,
            Arc::new(move || {
                catalog_store
                    .reasoning_catalog()
                    .map_err(|error| error.to_string())
            }),
        )
        .expect("the live config loads against the frozen catalog");

        let quota_body = serde_json::to_vec(&baseline["quota"]).unwrap();
        let mut accounts = AccountQuotaState::default();
        account_quota::apply_roster_listing(&mut accounts, &listing, now as i64);
        account_quota::apply_usage_reply(&mut accounts, &quota_body, now as i64)
            .expect("the frozen Insula reply decodes");

        let facts = store.get_facts().unwrap();
        let mut credentials = BrocaCredentials::default();
        credentials.apply_listing(&listing, now);
        credentials.refresh_serving_methods(
            &accounts,
            &facts,
            now as i64,
            &crate::config_overrides::live_config(&config_path).quota_model_pools(),
        );
        credentials.publish_billing_catalog(PlaneCatalog::ordinary_test_catalog(&facts));
        credentials.publish_plan_prices(&baseline["plan_prices"]);

        // The allocator state is process-wide, as in the module; every world
        // replaces it with the frozen one.
        *crate::ACCOUNT_QUOTA
            .get_or_init(|| Arc::new(Mutex::new(AccountQuotaState::default())))
            .lock()
            .unwrap() = accounts;

        let windows: Vec<ProviderUsage> =
            serde_json::from_value(baseline["quota"]["result"].clone())
                .expect("the frozen Insula entries decode as quota windows");
        let mut cache = QuotaCache::default();
        crate::apply_quota_refresh(&mut cache, windows, now);

        Self {
            dir,
            store,
            config_path,
            credentials: Arc::new(Mutex::new(credentials)),
            quota_cache: Arc::new(Mutex::new(cache)),
        }
    }

    /// Answer one older-shape request (`route.select`, `route.select_panel`)
    /// through the module's request handler.
    async fn legacy(&self, method: &str, params: &Value) -> (FrameType, Value) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        crate::handle_select_request(
            frame,
            1,
            &tx,
            &self.quota_cache,
            &self.store,
            &Arc::new(Mutex::new(None)),
            &self.config_path,
            &self.config_path,
            &self.credentials,
        )
        .await
        .expect("the handler answers");
        let reply = rx.recv().await.expect("one reply");
        (
            reply.header.ty,
            serde_json::from_slice(&reply.body).unwrap(),
        )
    }

    /// Answer one `routing/v1` request on a route the daemon attested as
    /// `principal`.
    async fn v1(&self, principal: &str, params: &Value) -> (FrameType, Value) {
        let (channel, epoch) = (9_001, 1);
        crate::route_principals()
            .lock()
            .unwrap()
            .insert(channel, (epoch, principal.to_owned()));
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            epoch,
            99,
            serde_json::to_vec(&json!({"method": "route.select", "params": params})).unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        crate::routing_v1::serve(
            frame,
            epoch,
            &tx,
            &crate::routing_v1::Deps {
                quota_cache: &self.quota_cache,
                store: &self.store,
                routing_config_path: &self.config_path,
                operator_config_path: &self.config_path,
                broca_credentials: &self.credentials,
            },
        )
        .await
        .expect("the v1 surface answers");
        let reply = rx.recv().await.expect("one reply");
        (
            reply.header.ty,
            serde_json::from_slice(&reply.body).unwrap(),
        )
    }
}

/// Decode the frozen `credential.list_scoped` reply through the live
/// Claustrum client, so the rows are read exactly as the vault poll reads them.
async fn decode_listing(reply: Value) -> ScopedCredentialListing {
    struct FixtureTarget(Value);

    impl CredentialGetTarget for FixtureTarget {
        fn credential_get<'a>(
            &'a self,
            _: &'a str,
            _: Option<u64>,
            _: bool,
        ) -> CredentialResolverFuture<'a, Result<Value, CredentialResolverError>> {
            panic!("routing reads only the non-secret listing")
        }

        fn credential_list_scoped(
            &self,
        ) -> CredentialResolverFuture<'_, Result<Value, CredentialResolverError>> {
            Box::pin(async { Ok(self.0.clone()) })
        }
    }

    let listing = ClaustrumCredentialResolver::with_target(Arc::new(FixtureTarget(reply)), 0)
        .list_scoped()
        .await
        .expect("the frozen vault listing decodes");
    assert!(
        listing.undecodable_credentials.is_empty(),
        "every frozen vault row decodes"
    );
    listing
}

/// Write the frozen catalog rows to the store the way the module's catalog
/// poll leaves them: each row carries its scores, and one complete Fusiform
/// snapshot then marks exactly these rows present, so the routes the store's
/// migrations seed are not candidates unless the frozen catalog lists them.
fn install_catalog(
    store: &SqliteRouteStateStore,
    catalog: &Value,
    mapped: &HashSet<String>,
    now: u64,
) {
    let rows = catalog["rows"].as_array().expect("catalog rows");
    let mut updates = Vec::new();
    for row in rows {
        let text = |key: &str| row[key].as_str().map(str::to_owned);
        let number = |key: &str| row[key].as_f64();
        let flag = |key: &str| row[key].as_bool();
        let list =
            |key: &str| -> Option<Vec<String>> { serde_json::from_value(row[key].clone()).ok() };
        let provider = text("provider").expect("provider");
        let model = text("model").expect("model");
        let fact = ModelFact {
            billing_plane: None,
            auth_methods: list("auth_methods"),
            login_methods_documented: false,
            speed_is_fleet_median: false,
            decommissioned: false,
            provider_id: provider.clone(),
            model_id: model.clone(),
            variant: None,
            family: text("family"),
            scores: ModelScores {
                elo: number("elo").expect("elo"),
                eq: number("eq").expect("eq"),
                speed: number("speed").expect("speed"),
            },
            context_window: number("context_window"),
            max_output_tokens: row["max_output_tokens"].as_u64(),
            cost_tier: number("cost_tier"),
            cost_per_m_tok_out: number("output_rate"),
            input_rate: number("input_rate"),
            cache_read_rate: number("cache_read_rate"),
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: flag("supports_vision"),
            supports_tool_use: flag("supports_tool_use"),
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
        };
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        updates.push(FusiformFactUpdate {
            provider_id: provider,
            model_id: model,
            auth_methods: fact.auth_methods.clone(),
            login_methods_documented: fact.login_methods_documented,
            present: true,
            context_window: fact.context_window,
            max_output_tokens: fact.max_output_tokens,
            cost_tier: fact.cost_tier,
            cost_per_mtok_out: fact.cost_per_m_tok_out,
            input_rate: fact.input_rate,
            cache_read_rate: fact.cache_read_rate,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: fact.supports_vision,
            supports_tool_use: fact.supports_tool_use,
            reasoning_efforts: list("reasoning_efforts"),
            catalog_efforts: None,
            canonical_id: text("canonical_id"),
            upstream_status: None,
            model_type: None,
            model_kind: None,
            lineage: fact.family.clone(),
        });
    }
    store
        .apply_fusiform_catalog_snapshot_for_providers(
            &updates,
            FusiformCatalogCursor {
                catalog_version: catalog["catalog_version"]
                    .as_i64()
                    .expect("catalog_version"),
                resolved_at_ms: now as i64,
            },
            mapped,
            &HashSet::new(),
        )
        .unwrap();
}

/// A request's params with `"$available_models"` replaced by the frozen list.
fn params_of(baseline: &Value, params: &Value) -> Value {
    let mut params = params.clone();
    if params["availableModels"] == json!("$available_models") {
        params["availableModels"] = baseline["available_models"].clone();
    }
    params
}

/// The four checked-in facts of an older-shape `route.select` answer.
fn legacy_answer(reply: &(FrameType, Value)) -> Value {
    let (ty, body) = reply;
    if *ty == FrameType::Error {
        return answer(None, None, None, body["code"].as_str());
    }
    let result = &body["result"];
    let selected = &result["selected"];
    answer(
        selected["routeKey"].as_str(),
        selected["promptParams"]["variant"].as_str(),
        selected["harness"].as_str(),
        result["pin_rejected"].as_str(),
    )
}

/// The four checked-in facts of a `routing/v1` select answer.
fn v1_answer(reply: &(FrameType, Value)) -> Value {
    let (ty, body) = reply;
    if *ty == FrameType::Error {
        return answer(None, None, None, body["code"].as_str());
    }
    let route = &body["result"]["route"];
    let key = route["provider"]
        .as_str()
        .zip(route["model"].as_str())
        .map(|(provider, model)| format!("{provider}/{model}"));
    answer(
        key.as_deref(),
        route["variant"].as_str(),
        body["result"]["runner"]["module"].as_str(),
        None,
    )
}

fn answer(
    route_key: Option<&str>,
    variant: Option<&str>,
    runner: Option<&str>,
    refusal_code: Option<&str>,
) -> Value {
    json!({
        "route_key": route_key,
        "variant": variant,
        "runner": runner,
        "refusal_code": refusal_code,
    })
}

/// For legacy route.select requests, the frozen baseline's `runner` records
/// the reply's `selected.harness`. For host requests, that field is absent and
/// `runner.module` matches the requested `harness`. Check both reply fields
/// independently. Every non-host winner in the frozen fixture runs on Broca.
fn verify_execution_identity(baseline: &Value, request: &Value, reply: &(FrameType, Value)) {
    let name = request["name"].as_str().unwrap();
    let wanted = listed_difference(name).unwrap_or_else(|| baseline["expected"][name].clone());
    let v1 = request["op"] == "routing/v1 route.select";
    let winner = wanted["route_key"].is_string();
    let expected_harness = (!v1).then(|| wanted["runner"].as_str()).flatten();
    let expected_runner = if !winner {
        None
    } else {
        wanted["runner"].as_str().or_else(|| {
            request["params"]["harness"]
                .as_str()
                .or_else(|| request["params"]["substrate"].as_str())
                .or(Some("host"))
        })
    };
    let result = &reply.1["result"];
    assert_eq!(
        result["runner"]["module"].as_str(),
        expected_runner,
        "{name}: runner module differs from frozen execution identity: {}",
        reply.1
    );
    let actual_harness = if v1 {
        &result["runner"]["harness"]
    } else {
        &result["selected"]["harness"]
    };
    assert_eq!(
        actual_harness.as_str(),
        expected_harness,
        "{name}: reply harness differs from frozen execution identity: {}",
        reply.1
    );
}

/// Run one request on a fresh world and return its checked-in facts.
async fn run_request(baseline: &Value, request: &Value) -> Value {
    let name = request["name"].as_str().expect("request name");
    let world = World::build(baseline, name).await;
    let params = params_of(baseline, &request["params"]);
    match request["op"].as_str().expect("request op") {
        "route.select" => {
            let reply = world.legacy("route.select", &params).await;
            verify_execution_identity(baseline, request, &reply);
            legacy_answer(&reply)
        }
        "synthesizer" => {
            // Core reads the synthesizer pin from the panel answer's policy,
            // then admits exactly that pin with a pinned route.select.
            let panel_params = params_of(baseline, &request["panel_params"]);
            let panel = world.legacy("route.select_panel", &panel_params).await;
            if panel.0 == FrameType::Error {
                return answer(None, None, None, panel.1["code"].as_str());
            }
            let pin = &panel.1["result"]["policy"]["pins"]["synthesizer"];
            let Some(model) = pin["model"].as_str() else {
                return answer(None, None, None, Some("no_synthesizer_pin"));
            };
            let mut params = params;
            params["pin"] = json!({"model": model});
            if let Some(variant) = pin["variant"].as_str() {
                params["pin"]["variant"] = json!(variant);
            }
            let reply = world.legacy("route.select", &params).await;
            verify_execution_identity(baseline, request, &reply);
            legacy_answer(&reply)
        }
        "routing/v1 route.select" => {
            let reply = world
                .v1(request["principal"].as_str().expect("principal"), &params)
                .await;
            verify_execution_identity(baseline, request, &reply);
            v1_answer(&reply)
        }
        other => panic!("unknown baseline op {other}"),
    }
}

async fn run_all(baseline: &Value) -> BTreeMap<String, Value> {
    let _clock = crate::InjectedClock::set(clock_ms(baseline));
    let mut answers = BTreeMap::new();
    for request in baseline["requests"].as_array().expect("requests") {
        // Keep the frozen history, but skip sidekick.lead because core no longer launches a lead session.
        if request["params"]["workload"] == "sidekick.lead" {
            continue;
        }
        let name = request["name"].as_str().expect("request name").to_owned();
        let answer = run_request(baseline, request).await;
        assert!(
            answers.insert(name.clone(), answer).is_none(),
            "request names are unique: {name}"
        );
    }
    answers
}

/// The checked-in config is byte-for-byte the copy the owner supplied
/// (238 lines, sha256 9447e7e8b5803b1b...), and the baseline names it.
#[test]
fn baseline_config_is_the_owner_supplied_live_copy() {
    let baseline = baseline();
    let digest = Sha256::digest(CONFIG.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, baseline["config"]["sha256"].as_str().unwrap());
    assert!(hex.starts_with("9447e7e8b5803b1b"));
    assert_eq!(CONFIG.lines().count(), 238);
    assert_eq!(
        baseline["config"]["file"],
        "routing-config-live-2026-10-24.jsonc"
    );
}

/// The baseline covers one request per configured slot pin, and every
/// request has a checked-in answer (and no answer lacks a request).
#[test]
fn baseline_lists_each_slot_pin_and_an_answer_per_request() {
    let baseline = baseline();
    let requests = baseline["requests"].as_array().unwrap();
    let names: Vec<&str> = requests
        .iter()
        .map(|request| request["name"].as_str().unwrap())
        .collect();
    let slot = |slot: &str| {
        requests.iter().any(|request| {
            request["params"]["pin"]["slot"] == slot
                || (slot == "synthesizer" && request["op"] == "synthesizer")
        })
    };
    for pinned in ["synthesizer", "lab_planner", "spec_designer"] {
        assert!(slot(pinned), "no request for the {pinned} slot pin");
    }
    let expected = baseline["expected"].as_object().unwrap();
    let mut answered: Vec<&str> = expected.keys().map(String::as_str).collect();
    let mut asked = names.clone();
    answered.sort_unstable();
    asked.sort_unstable();
    assert_eq!(answered, asked);
    for row in baseline["credential_listing"]["result"]["credentials"]
        .as_array()
        .unwrap()
    {
        assert!(
            row["auth_method"].is_string(),
            "vault row {} carries auth_method",
            row["id"]
        );
    }
}

/// Requests for which the new pass differs from the frozen baseline. Each
/// listed request names the answer the new pass must give and one allowed cause.
const EQUIVALENCE_DIFFERENCES: &[(&str, &str, &str)] = &[
    (
        "sidekick-muse-spark-pin",
        r#"{"refusal_code":null,"route_key":"meta/muse-spark-1.3-contributor","runner":"broca","variant":null}"#,
        "unqualified pin; frozen model catalog has neither a graded nor vendor effort default",
    ),
    (
        "athena-astra-pin",
        r#"{"refusal_code":null,"route_key":"openai/gpt-6-astra","runner":"broca","variant":null}"#,
        "unqualified pin; frozen model catalog has neither a graded nor vendor effort default",
    ),
    // A subscription whose credential-specific billing rates were never requested
    // now uses the configured-multiplier list-rate fallback instead of maximum unknown cost.
    (
        "core-broca",
        r#"{"refusal_code":null,"route_key":"meta/muse-spark-1.3-contributor","runner":"broca","variant":null}"#,
        "missing subscription planes retain catalog list rates with a configured multiplier",
    ),
    (
        "mason-excluded-model-pin",
        r#"{"refusal_code":"pinned_route_refused","route_key":null,"runner":null,"variant":null}"#,
        "a not_for-refused named model",
    ),
    (
        "flow-subscription-pin",
        r#"{"refusal_code":null,"route_key":"anthropic/claude-opus-5-5","runner":"broca","variant":null}"#,
        "flow no longer has an implicit metered-only payment restriction",
    ),
    (
        "codemode-broca",
        r#"{"refusal_code":null,"route_key":"opencode-go/deepseek-v4.1-flash","runner":"broca","variant":null}"#,
        "code-mode may use subscriptions while unpinned metered routes are excluded by default",
    ),
    (
        "flow-broca",
        r#"{"refusal_code":null,"route_key":"opencode-go/deepseek-v4.1-flash","runner":"broca","variant":null}"#,
        "flow may use subscriptions while unpinned metered routes are excluded by default",
    ),
    (
        "mason-no-consumer",
        r#"{"refusal_code":null,"route_key":"opencode-go/deepseek-v4.1-flash","runner":"broca","variant":null}"#,
        "legacy automatic selection no longer spends on an unpinned metered route by default",
    ),
    (
        "v1-codemode",
        r#"{"refusal_code":null,"route_key":"opencode-go/deepseek-v4.1-flash","runner":"broca","variant":null}"#,
        "code-mode may use subscriptions while unpinned metered routes are excluded by default",
    ),
    (
        "v1-mc-dream-curate",
        r#"{"refusal_code":null,"route_key":"opencode-go/deepseek-v4.1-flash","runner":"broca","variant":null}"#,
        "background automatic selection no longer spends on an unpinned metered route by default",
    ),
];

fn listed_difference(name: &str) -> Option<Value> {
    EQUIVALENCE_DIFFERENCES
        .iter()
        .find_map(|(request, answer, cause)| {
            (*request == name).then(|| {
                let mut answer: Value = serde_json::from_str(answer).expect("listed answer");
                answer["cause"] = json!(cause);
                answer
            })
        })
}

fn drop_at(document: &mut Value, path: &[&str]) {
    let (last, parents) = path.split_last().expect("path");
    let mut cursor = document;
    for segment in parents {
        cursor = cursor
            .get_mut(*segment)
            .unwrap_or_else(|| panic!("missing {segment} in {}", path.join("/")));
    }
    assert!(
        cursor
            .as_object_mut()
            .unwrap_or_else(|| panic!("not an object at {}", path.join("/")))
            .remove(*last)
            .is_some(),
        "nothing to drop at {}",
        path.join("/")
    );
}

async fn one_answer(baseline: &Value, config: &str, name: &str) -> Value {
    let request = baseline["requests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|request| request["name"] == name)
        .unwrap_or_else(|| panic!("no request {name}"));
    let world = World::build_with(baseline, name, config).await;
    let params = params_of(baseline, &request["params"]);
    match request["op"].as_str().unwrap() {
        "route.select" => legacy_answer(&world.legacy("route.select", &params).await),
        "synthesizer" => {
            let panel_params = params_of(baseline, &request["panel_params"]);
            let panel = world.legacy("route.select_panel", &panel_params).await;
            assert_eq!(panel.0, FrameType::Response, "{}", panel.1);
            let pin = &panel.1["result"]["policy"]["pins"]["synthesizer"];
            let mut params = params;
            params["pin"] = json!({"model": pin["model"]});
            if let Some(variant) = pin["variant"].as_str() {
                params["pin"]["variant"] = json!(variant);
            }
            legacy_answer(&world.legacy("route.select", &params).await)
        }
        "routing/v1 route.select" => v1_answer(
            &world
                .v1(request["principal"].as_str().unwrap(), &params)
                .await,
        ),
        other => panic!("control does not run {other}"),
    }
}
