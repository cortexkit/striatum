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
use prefrontal_routing_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use prefrontal_routing_core::quota::ProviderUsage;
use prefrontal_routing_core::route_state_store::FactSource;
use prefrontal_routing_core::{ModelFact, RouteStateStore};
use prefrontal_routing_store::{
    dev_descriptor, FusiformCatalogCursor, FusiformFactUpdate, SqliteRouteStateStore,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
    auth_path: PathBuf,
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-baseline-{label}-{}-{n}",
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
        let config_path = dir.join("prefrontal-routing.jsonc");
        crate::test_config::write(&config_path, config).unwrap();
        let auth_path = dir.join("auth.json");
        std::fs::write(&auth_path, baseline["auth_json"].to_string()).unwrap();

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
        credentials.refresh_serving_methods(&accounts, &facts, now as i64);
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
            auth_path,
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
            &self.auth_path,
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
                opencode_auth_path: &self.auth_path,
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
            speed_is_fleet_median: false,
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
        };
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        updates.push(FusiformFactUpdate {
            provider_id: provider,
            model_id: model,
            auth_methods: fact.auth_methods.clone(),
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
            canonical_id: text("canonical_id"),
            upstream_status: None,
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

/// Run one request on a fresh world and return its checked-in facts.
async fn run_request(baseline: &Value, request: &Value) -> Value {
    let name = request["name"].as_str().expect("request name");
    let world = World::build(baseline, name).await;
    let params = params_of(baseline, &request["params"]);
    match request["op"].as_str().expect("request op") {
        "route.select" => legacy_answer(&world.legacy("route.select", &params).await),
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
            legacy_answer(&world.legacy("route.select", &params).await)
        }
        "routing/v1 route.select" => v1_answer(
            &world
                .v1(request["principal"].as_str().expect("principal"), &params)
                .await,
        ),
        other => panic!("unknown baseline op {other}"),
    }
}

async fn run_all(baseline: &Value) -> BTreeMap<String, Value> {
    let _clock = crate::InjectedClock::set(clock_ms(baseline));
    let mut answers = BTreeMap::new();
    for request in baseline["requests"].as_array().expect("requests") {
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

/// A second run on fresh worlds answers identically: the baseline does not
/// depend on store ids, map order or anything else that varies between runs.
#[tokio::test]
async fn the_baseline_harness_is_deterministic() {
    let baseline = baseline();
    let first = run_all(&baseline).await;
    let second = run_all(&baseline).await;
    assert_eq!(first, second);
}

/// The selection path reads the injected clock, not the system time, and
/// dropping the guard restores the system clock.
#[test]
fn the_injected_clock_governs_now_ms_until_dropped() {
    let frozen = clock_ms(&baseline());
    {
        let _clock = crate::InjectedClock::set(frozen);
        assert_eq!(crate::now_ms(), frozen);
        {
            let _inner = crate::InjectedClock::set(frozen + 5);
            assert_eq!(crate::now_ms(), frozen + 5);
        }
        assert_eq!(crate::now_ms(), frozen);
    }
    assert_ne!(crate::now_ms(), frozen);
}

/// Requests for which the new pass differs from the frozen baseline. Each
/// listed request names the answer the new pass must give and one allowed cause.
const EQUIVALENCE_DIFFERENCES: &[(&str, &str, &str)] = &[(
    "mason-excluded-model-pin",
    r#"{"refusal_code":"pinned_route_refused","route_key":null,"runner":null,"variant":null}"#,
    "a not_for-refused named model",
)];

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

/// The new pass on the converted live file, each request from its own empty
/// store, matches the baseline. A difference must be listed with its exact
/// answer and one allowed cause.
#[tokio::test]
async fn the_new_pass_on_the_converted_fixture_matches_the_baseline() {
    let baseline = baseline();
    let answers = run_all(&baseline).await;
    let expected: BTreeMap<String, Value> =
        serde_json::from_value(baseline["expected"].clone()).unwrap();
    let mut unexpected = Vec::new();
    for (name, answer) in &answers {
        let wanted = listed_difference(name).unwrap_or_else(|| expected[name].clone());
        let comparable = if wanted.get("cause").is_some() {
            let mut got = answer.clone();
            got["cause"] = wanted["cause"].clone();
            got
        } else {
            answer.clone()
        };
        if comparable != wanted {
            unexpected.push((name.clone(), answer.clone(), wanted));
        }
    }
    assert!(
        unexpected.is_empty(),
        "new pass differs from the baseline without a listed cause.\n{}",
        unexpected
            .iter()
            .map(|(name, got, wanted)| format!("{name}: got {got} wanted {wanted}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    for (name, _, _) in EQUIVALENCE_DIFFERENCES {
        assert!(
            answers.contains_key(*name),
            "listed difference {name} is not a baseline request"
        );
    }
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

/// Dropping one not_for, variant, project-pin, slot-pin or subscription entry
/// from the converted file changes an answer, so a check that recorded the
/// undropped answer fails.
#[tokio::test]
async fn dropping_one_policy_entry_changes_the_converted_answer() {
    let baseline = baseline();
    let _clock = crate::InjectedClock::set(clock_ms(&baseline));
    let converted = crate::config_convert::convert(CONFIG).expect("the live file converts");
    let mut document: Value = serde_json::from_str(&converted).unwrap();
    // The live file has no project pin. Plant one so this control has an entry
    // to drop; a scoped athena select must answer that pin.
    document["consumers"]["athena"]["projects"] = json!({
        "baseline-project": { "pin": "anthropic/claude-opus-5-5" }
    });
    let planted = serde_json::to_string(&document).unwrap();

    let cases: &[(&[&str], &str)] = &[
        (
            &["models", "openai/gpt-6-luna", "not_for"],
            "mason-excluded-model-pin",
        ),
        (&["consumers", "athena", "variants"], "athena-astra-pin"),
        (&["consumers", "lab_planner", "pin"], "slot-lab-planner"),
    ];
    for (path, request) in cases {
        let mut mutated: Value = serde_json::from_str(&planted).unwrap();
        drop_at(&mut mutated, path);
        let before = one_answer(&baseline, &planted, request).await;
        let after = one_answer(
            &baseline,
            &serde_json::to_string(&mutated).unwrap(),
            request,
        )
        .await;
        assert_ne!(
            before,
            after,
            "dropping {} did not change {request}: {before}",
            path.join("/")
        );
    }

    // The route remains cheapest even without its subscription, so comparing
    // winners alone would miss this deletion. Check the resolved coverage too.
    let subscription_keys = |reply: &Value| {
        reply["result"]["resolvedInput"]["subscriptionKeys"]
            .as_array()
            .unwrap()
            .clone()
    };
    let world = World::build_with(&baseline, "subscription", &planted).await;
    let params = params_of(
        &baseline,
        &baseline["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|request| request["name"] == "mason-broca")
            .unwrap()["params"],
    );
    let covered = subscription_keys(&world.legacy("route.select", &params).await.1);
    assert!(covered.contains(&json!("opencode-go/deepseek-v4.1-flash")));
    let mut dropped: Value = serde_json::from_str(&planted).unwrap();
    drop_at(&mut dropped, &["providers", "opencode-go", "subscription"]);
    let world = World::build_with(
        &baseline,
        "subscription-dropped",
        &serde_json::to_string(&dropped).unwrap(),
    )
    .await;
    let uncovered = subscription_keys(&world.legacy("route.select", &params).await.1);
    assert!(!uncovered.contains(&json!("opencode-go/deepseek-v4.1-flash")));

    let pinned_config = planted.clone();
    let world = World::build_with(&baseline, "project-pin", &pinned_config).await;
    super::remember_route_scope(
        9_001,
        1,
        Some(super::DecisionScope {
            project_id: Some("baseline-project".into()),
            workdir: None,
        }),
    );
    let pinned = v1_answer(
        &world
            .v1(
                "reserved:prefrontal-core",
                &json!({"consumer": "athena", "requirements": {"iq": 90}}),
            )
            .await,
    );
    let mut dropped: Value = serde_json::from_str(&planted).unwrap();
    drop_at(
        &mut dropped,
        &["consumers", "athena", "projects", "baseline-project", "pin"],
    );
    let world = World::build_with(
        &baseline,
        "project-pin-dropped",
        &serde_json::to_string(&dropped).unwrap(),
    )
    .await;
    super::remember_route_scope(
        9_001,
        1,
        Some(super::DecisionScope {
            project_id: Some("baseline-project".into()),
            workdir: None,
        }),
    );
    let unpinned = v1_answer(
        &world
            .v1(
                "reserved:prefrontal-core",
                &json!({"consumer": "athena", "requirements": {"iq": 90}}),
            )
            .await,
    );
    assert_ne!(
        pinned, unpinned,
        "dropping the project pin did not change the scoped select: {pinned}"
    );
    assert_eq!(pinned["route_key"], "anthropic/claude-opus-5-5", "{pinned}");
}
