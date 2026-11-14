//! The `routing/v1` conformance kit, run against this module.
//!
//! [`Harness`] drives the module's own frame handler, [`crate::handle_frame`],
//! the function the module's read loop calls for every frame the daemon
//! sends. Each route is bound with a `route.bind` control frame that carries
//! the principal and scope stamp the kit names, exactly as the daemon binds
//! one, and every request then travels as a frame on that route. Nothing
//! calls the v1 handlers directly, so the provider reads its caller and
//! scope from the route as it does in production. The subc daemon itself is
//! not started here; `tests/it/real_daemon_e2e.rs` runs a select and a report
//! through a real daemon.
//!
//! [`KitSubject`] maps the kit's configuration hooks onto routing's own
//! state: consumers, runners, project pins and the subscription list are
//! written to the routing configuration file, models are written to the
//! route-state store's catalog, cooldowns to its route state, and
//! subscription accounts to the account allocator's vault roster and quota
//! readings.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use cortexkit_role_routing_conformance::{
    fixture, run_suite, Catalog, CatalogModule, ConsumerConfig, ConsumerMode, HoldLimits, Reply,
    RouteFailure, RouteStamp, RoutingRoute, RoutingSubject, CHECKS,
};
use striatum_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use striatum_core::route_state_store::FactSource;
use striatum_core::{ModelFact, RouteStateStore, RouteStateUpdate};
use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::account_quota::{self, AccountQuotaState};
use crate::broca_credentials::BrocaCredentials;
use crate::fusiform_catalog::CatalogReachHealth;
use crate::{
    account_selection, QuotaCache, RouteEpochs, SharedBrocaCredentials, SharedQuotaCache,
    SharedStore, DEFAULT_MODULE_ID,
};

static NEXT_HARNESS: AtomicU64 = AtomicU64::new(0);

mod continuation_tests;
mod zero_config_tests;
mod warm_credential_tests;
#[path = "media_conformance.rs"]
mod media_conformance;

/// The provider id the account allocator's quota readings are filed under
/// for the kit's subscription accounts; any id other than the routing
/// provider's own name works.
const QUOTA_SOURCE: &str = "example-quota-source";

/// One fresh routing module state: a store, a routing configuration file and
/// the module's in-process caches, served through its frame handler.
pub(super) struct Harness {
    dir: PathBuf,
    store: SharedStore,
    quota_cache: SharedQuotaCache,
    broca_credentials: Mutex<SharedBrocaCredentials>,
    catalog_health: crate::SharedCatalogHealth,
    /// The routing configuration file requests read. Every change is written
    /// to a new file so no cached reading of an older one is ever served.
    config_path: Mutex<PathBuf>,
    config: Mutex<Value>,
    config_generation: AtomicU64,
    route_epochs: tokio::sync::Mutex<RouteEpochs>,
    next_channel: AtomicU16,
    selection_profiles: Arc<Mutex<striatum_core::model_router::RouteTaskProfiles>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Harness {
    pub(super) fn new(label: &str) -> Arc<Self> {
        Self::new_at_version(label, None)
    }

    pub(super) fn new_at_version(label: &str, version: Option<u32>) -> Arc<Self> {
        super::consumers::replace_catalog(&json!([]));
        let n = NEXT_HARNESS.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("striatum-v1-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let descriptor = striatum_store::dev_descriptor(
            DEFAULT_MODULE_ID,
            dir.join("store.db").to_string_lossy().into_owned(),
        );
        let store = Arc::new(
            match version {
                Some(version) => {
                    striatum_store::SqliteRouteStateStore::open_at_version_for_test(
                        &descriptor,
                        version,
                    )
                }
                None => striatum_store::SqliteRouteStateStore::open(&descriptor),
            }
            .unwrap(),
        );
        if let Some(inputs) = striatum_store::replay::current() {
            // Catalog migrations stamp their initial route-state rows with
            // SQLite's wall clock. This is a fresh scratch store, before any
            // request: give those seed rows the replay's creation time too.
            // Request-time updates still go through the real effect writers.
            rusqlite::Connection::open(dir.join("store.db"))
                .unwrap()
                .execute(
                    "UPDATE model_route_state SET updated_at=?1 WHERE updated_at<>0",
                    [inputs.now_ms],
                )
                .unwrap();
        }
        let harness = Arc::new(Self {
            store,
            quota_cache: Arc::new(Mutex::new(QuotaCache::default())),
            broca_credentials: Mutex::new(Arc::new(Mutex::new(
                BrocaCredentials::api_test_credentials(&[]),
            ))),
            catalog_health: Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
            config_path: Mutex::new(dir.join("routing-0.jsonc")),
            config: Mutex::new(
                crate::config_convert::convert_value(json!({
                    "model_routing": {}, "account_selection": {"enabled": true},
                }))
                .unwrap(),
            ),
            config_generation: AtomicU64::new(0),
            route_epochs: tokio::sync::Mutex::new(RouteEpochs::new()),
            next_channel: AtomicU16::new(100),
            selection_profiles: Default::default(),
            dir,
        });
        harness.edit_config(|config| {
            for policy in config["consumers"].as_object_mut().unwrap().values_mut() {
                policy["owner"] = json!(fixture::CONSUMER.trim_start_matches("reserved:"));
            }
        });
        // The allocator reads the process-wide account state the module's
        // vault and quota loops keep; a fresh harness starts it empty.
        *crate::ACCOUNT_QUOTA
            .get_or_init(|| Arc::new(Mutex::new(AccountQuotaState::default())))
            .lock()
            .unwrap() = AccountQuotaState::default();
        harness
    }

    pub(super) fn store(&self) -> &SharedStore {
        &self.store
    }

    /// The store's database file, for tests that read a table directly.
    pub(super) fn store_path(&self) -> PathBuf {
        self.dir.join("store.db")
    }

    pub(super) fn operator_config(&self) -> Arc<crate::config_overrides::LiveConfig> {
        crate::config_overrides::live_config(&self.config_path.lock().unwrap())
    }

    pub(crate) fn settings_source(&self, source: Arc<dyn crate::project_settings::Source>) {
        crate::project_settings::set_test_source(&self.config_path.lock().unwrap(), source);
    }

    /// Change the routing configuration and publish it as a new file.
    pub(super) fn edit_config(&self, edit: impl FnOnce(&mut Value)) {
        let mut config = self.config.lock().unwrap();
        edit(&mut config);
        let generation = self.config_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let path = self.dir.join(format!("routing-{generation}.jsonc"));
        crate::test_config::write(&path, serde_json::to_vec_pretty(&*config).unwrap()).unwrap();
        // Reading it once here refuses an invalid document before any
        // request does.
        crate::config_overrides::initialize_config(&path).unwrap();
        *self.config_path.lock().unwrap() = path;
    }

    /// Write `facts` to the catalog and make Broca's billing view serve them.
    pub(super) fn install_facts(&self, facts: &[ModelFact]) {
        // These fixtures exercise selection mechanics with metered candidates.
        // Opt their providers in explicitly; no-file safety tests remove this
        // settings file before requesting the built-in policy.
        self.edit_config(|config| {
            for fact in facts {
                let policy = &mut config["providers"][&fact.provider_id]["pin_only"];
                if policy.is_null() {
                    *policy = json!(false);
                }
            }
        });
        for fact in facts {
            self.store.upsert_fact(fact, FactSource::Override).unwrap();
        }
        *self.broca_credentials.lock().unwrap() =
            Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(facts)));
    }

    /// Serve requests with `credentials` as Broca's credential and billing
    /// view.
    pub(super) fn set_credentials(&self, credentials: BrocaCredentials) {
        *self.broca_credentials.lock().unwrap() = Arc::new(Mutex::new(credentials));
    }

    pub(super) fn credentials(&self) -> crate::SharedBrocaCredentials {
        self.broca_credentials.lock().unwrap().clone()
    }

    pub(super) fn observe_runner_describe(&self, module: &str, describe: &Value) {
        let credentials = self.broca_credentials.lock().unwrap();
        super::runners::DescribeCache::default().reply(
            module,
            Some(describe),
            &mut credentials.lock().unwrap(),
            crate::now_ms() as i64,
        );
    }

    /// Bind a new route as `principal`, scoped to `project_id` when given,
    /// with the control frame the daemon sends.
    pub(super) async fn bind(
        self: &Arc<Self>,
        principal: Value,
        project_id: Option<&str>,
    ) -> Route {
        let channel = self.next_channel.fetch_add(1, Ordering::Relaxed);
        let epoch = 1;
        let mut bind = json!({
            "op": "route.bind",
            "route_channel": channel,
            "epoch": epoch,
            "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
            "identity": {
                "project_root": cortexkit_test_support::scratch_root(),
                "harness": "routing-v1-conformance",
                "session": format!("session-{channel}"),
            },
            "principal": principal,
        });
        if let Some(project_id) = project_id {
            bind["scope"] = json!({
                "owner": { "kind": "reserved", "module_id": "prefrontal-core" },
                "ref": format!("scope-{project_id}"),
                "scope_epoch": 1,
                "kind": "head",
                "project_id": project_id,
                "owner_authorized": true,
            });
        }
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            u64::from(channel),
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap();
        let reply = self.handle(frame).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "route.bind was refused: {}",
            String::from_utf8_lossy(&reply.body)
        );
        Route {
            harness: Arc::clone(self),
            channel,
            epoch,
            next_corr: AtomicU64::new(1),
        }
    }

    /// The next unused route channel for a bind on this harness.
    pub(super) fn reserve_channel(&self) -> u16 {
        self.next_channel.fetch_add(1, Ordering::Relaxed)
    }

    /// Send `bind` as the control frame for `channel` under `epoch` and return
    /// the route it bound. A test that needs a stamp shape `bind` builds its
    /// own frame; this one answers the daemon's `route.bind` acknowledgement.
    pub(super) async fn bind_frame(
        self: &Arc<Self>,
        bind: Value,
        channel: u16,
        epoch: u32,
    ) -> Route {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            u64::from(channel),
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap();
        let reply = self.handle(frame).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "route.bind was refused: {}",
            String::from_utf8_lossy(&reply.body)
        );
        Route {
            harness: Arc::clone(self),
            channel,
            epoch,
            next_corr: AtomicU64::new(1),
        }
    }

    /// Hand one frame to the module's frame handler and wait for its reply.
    pub(super) async fn handle(&self, frame: Frame) -> Frame {
        let (writer, mut replies) = mpsc::channel(4);
        let config_path = self.config_path.lock().unwrap().clone();
        let broca_credentials = Arc::clone(&self.broca_credentials.lock().unwrap());
        let mut route_epochs = self.route_epochs.lock().await;
        let serving = crate::handle_frame(
            frame,
            &writer,
            &self.quota_cache,
            &self.store,
            &mut route_epochs,
            &Arc::new(Mutex::new(None)),
            &config_path,
            &config_path,
            &broca_credentials,
            &self.catalog_health,
        )
        .await
        .unwrap();
        assert!(serving, "the frame loop must keep serving");
        drop(route_epochs);
        drop(writer);
        let reply = replies.recv().await.expect("every request gets a reply");
        assert_no_pending_refusal(&reply, || self.enforced());
        reply
    }

    fn enforced(&self) -> bool {
        self.operator_config()
            .concurrency_caps()
            .enforcement_enabled
    }

    /// Run a pre-rebind request frame through the production handler after a
    /// rebind, bypassing only the transport's stale-frame drop.
    pub(super) async fn finish_dispatched(&self, frame: Frame) -> (FrameType, Value) {
        let (writer, mut replies) = mpsc::channel(4);
        let config_path = self.config_path.lock().unwrap().clone();
        let credentials = Arc::clone(&self.broca_credentials.lock().unwrap());
        let epoch = frame.header.epoch;
        super::serve(
            frame,
            epoch,
            &writer,
            &super::Deps {
                quota_cache: &self.quota_cache,
                store: &self.store,
                routing_config_path: &config_path,
                operator_config_path: &config_path,
                broca_credentials: &credentials,
            },
        )
        .await
        .unwrap();
        let reply = replies.recv().await.unwrap();
        assert_no_pending_refusal(&reply, || self.enforced());
        (
            reply.header.ty,
            serde_json::from_slice(&reply.body).unwrap(),
        )
    }
}

/// One bound route to the module.
pub(super) struct Route {
    harness: Arc<Harness>,
    channel: u16,
    epoch: u32,
    next_corr: AtomicU64,
}

impl Route {
    pub(super) async fn what_if_with_profiles(
        &self,
        params: &Value,
        lookup: &super::select::ProfileLookup,
    ) -> Result<Value, cortexkit_role_routing::errors::Refusal> {
        let config_path = self.harness.config_path.lock().unwrap().clone();
        let credentials = Arc::clone(&self.harness.broca_credentials.lock().unwrap());
        super::what_if::what_if_with_profile_lookup(
            params,
            &super::Caller::for_route(self.channel, self.epoch),
            &super::Deps {
                quota_cache: &self.harness.quota_cache,
                store: &self.harness.store,
                routing_config_path: &config_path,
                operator_config_path: &config_path,
                broca_credentials: &credentials,
            },
            lookup,
        )
        .await
    }

    /// Exercise the select handler with a fixed refresh snapshot instead of the
    /// process-wide cache, so profile tests cannot change another test's prices.
    pub(super) async fn select_with_profiles(
        &self,
        params: &Value,
        lookup: &super::select::ProfileLookup,
    ) -> Result<Value, cortexkit_role_routing::errors::Refusal> {
        let config_path = self.harness.config_path.lock().unwrap().clone();
        let credentials = Arc::clone(&self.harness.broca_credentials.lock().unwrap());
        super::select::select_with_profile_lookup(
            params,
            &super::Caller::for_route(self.channel, self.epoch),
            &super::Deps {
                quota_cache: &self.harness.quota_cache,
                store: &self.harness.store,
                routing_config_path: &config_path,
                operator_config_path: &config_path,
                broca_credentials: &credentials,
            },
            lookup,
        )
        .await
    }

    /// Run `route.account_for` through the v1 handler on this thread, without
    /// the frame loop, so a test hook inside it can run a competing request.
    pub(super) fn account_for_direct(
        &self,
        params: &Value,
    ) -> Result<Value, cortexkit_role_routing::errors::Refusal> {
        let config_path = self.harness.config_path.lock().unwrap().clone();
        let credentials = Arc::clone(&self.harness.broca_credentials.lock().unwrap());
        super::accounts::account_for(
            params,
            &super::Caller::for_route(self.channel, self.epoch),
            &super::Deps {
                quota_cache: &self.harness.quota_cache,
                store: &self.harness.store,
                routing_config_path: &config_path,
                operator_config_path: &config_path,
                broca_credentials: &credentials,
            },
        )
    }

    pub(super) fn request_frame(&self, method: &str, params: &Value) -> Frame {
        Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            self.channel,
            self.epoch,
            self.next_corr.fetch_add(1, Ordering::Relaxed),
            serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap(),
        )
        .unwrap()
    }

    /// Send `{method, params}` on this route; the reply frame's type and the
    /// body bytes exactly as the module rendered them.
    pub(super) async fn call_raw(&self, method: &str, params: &Value) -> (FrameType, Vec<u8>) {
        let reply = self
            .harness
            .handle(self.request_frame(method, params))
            .await;
        (reply.header.ty, reply.body)
    }

    /// Send `{method, params}` on this route; the reply frame's type and body.
    pub(super) async fn call(&self, method: &str, params: &Value) -> (FrameType, Value) {
        let frame = self.request_frame(method, params);
        let reply = self.harness.handle(frame).await;
        (
            reply.header.ty,
            serde_json::from_slice(&reply.body).unwrap(),
        )
    }
}

#[async_trait]
impl RoutingRoute for Route {
    async fn request(&self, method: &str, params: &Value) -> Result<Reply, RouteFailure> {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            self.channel,
            self.epoch,
            self.next_corr.fetch_add(1, Ordering::Relaxed),
            serde_json::to_vec(&json!({ "method": method, "params": params }))
                .map_err(|error| RouteFailure::new(error.to_string()))?,
        )
        .map_err(|error| RouteFailure::new(error.to_string()))?;
        let reply = self.harness.handle(frame).await;
        match reply.header.ty {
            FrameType::Response => String::from_utf8(reply.body)
                .map(Reply::Response)
                .map_err(|error| RouteFailure::new(error.to_string())),
            FrameType::Error => serde_json::from_slice(&reply.body)
                .map(Reply::Error)
                .map_err(|error| RouteFailure::new(format!("undecodable ERROR body: {error}"))),
            other => Err(RouteFailure::new(format!(
                "unexpected reply frame {other:?}"
            ))),
        }
    }
}

/// A route principal the daemon verified as `module`.
pub(super) fn reserved(module: &str) -> Value {
    json!({ "kind": "reserved", "module_id": module.trim_start_matches("reserved:") })
}

/// A catalog model as a routing fact. `rank` orders the models by price, so
/// the cheapest is the one routing prefers when nothing else decides.
pub(super) fn catalog_fact(
    model: &cortexkit_role_routing_conformance::CatalogModel,
    rank: usize,
) -> ModelFact {
    ModelFact {
        billing_plane: None,
        auth_methods: None, login_methods_documented: false,
        speed_is_fleet_median: false,
        decommissioned: false,
        provider_id: model.provider.clone(),
        model_id: model.model.clone(),
        variant: None,
        family: None,
        // Adequacy is judged against the strongest routable fact, so only the
        // models' relative ability matters; the kit's 0-100 ability is spread
        // onto routing's score scale.
        scores: ModelScores {
            elo: 1000.0 + 10.0 * f64::from(model.iq),
            eq: 1000.0 + 10.0 * f64::from(model.iq),
            speed: 90.0,
        },
        context_window: Some(model.context_window as f64),
        max_output_tokens: Some(model.max_output_tokens),
        cost_tier: Some(1.0 + rank as f64),
        cost_per_m_tok_out: Some(1.0 + rank as f64),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(model.modalities.iter().any(|modality| modality == "image")),
        supports_tool_use: Some(model.tool_calls),
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::Override,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: None,
        class_vector_is_overlay: false,
        upstream_status: None, model_type: None, model_kind: None,
    }
}

/// The account allocator's state for `accounts`: a vault listing of each
/// account's credential and a fresh quota reading with headroom.
fn account_state(
    accounts: &[cortexkit_role_routing_conformance::CatalogAccount],
) -> AccountQuotaState {
    let now = account_selection::now_ms();
    let mut state = AccountQuotaState::default();
    let listing = ScopedCredentialListing {
        credentials: accounts
            .iter()
            .map(|account| ListedCredential {
                id: account.credential_id.clone(),
                categories: vec![if account.method == "gateway" {
                    "llm-gateway".into()
                } else {
                    "llm-provider".into()
                }],
                credential_type: account.method.clone(),
                auth_method: Some(account.method.clone()),
                provider_ids: vec![account.provider.clone()],
                serves: account.serves.clone(),
                refresh_adapter: Some(account.provider.clone()),
                state: "active".into(),
                record_version: 1,
                operations: vec!["list".into()],
                account_id: Some(account.account_id.clone()),
                email: Some(format!("{}@example.test", account.account_id)),
                ..Default::default()
            })
            .collect(),
        grant_tuples: vec![ListedGrant {
            selector_kind: "category".into(),
            selector: "llm-provider".into(),
            operation: "list".into(),
        }],
        view: "conformance-vault-view".into(),
        ..Default::default()
    };
    account_quota::apply_roster_listing(&mut state, &listing, now);
    let readings: Vec<Value> = accounts
        .iter()
        .map(|account| {
            json!({
                "provider": QUOTA_SOURCE,
                "apiProvider": account.provider,
                "account": account.account_id,
                "source": "vault",
                "fetchedAt": account_selection::rfc3339(now - 30_000),
                "usage": {
                    "primary": {
                        "usedPercent": 10.0, "windowMinutes": 300,
                        "resetsAt": account_selection::rfc3339(now + 2 * 3_600_000),
                    },
                    "secondary": {
                        "usedPercent": 20.0, "windowMinutes": 10080,
                        "resetsAt": account_selection::rfc3339(now + 3 * 86_400_000),
                    },
                },
            })
        })
        .collect();
    account_quota::apply_provider_reply(
        &mut state,
        QUOTA_SOURCE,
        &serde_json::to_vec(&json!({ "result": readings, "completeProviders": [QUOTA_SOURCE] }))
            .unwrap(),
        now,
    )
    .unwrap();
    state
}

/// Install the kit's catalog: models in the store, runners and the
/// subscription providers in the routing configuration, accounts in the
/// allocator.
pub(super) fn install_catalog(harness: &Harness, catalog: &Catalog) {
    let facts: Vec<ModelFact> = catalog
        .models
        .iter()
        .enumerate()
        .map(|(rank, model)| catalog_fact(model, rank))
        .collect();
    harness.install_facts(&facts);
    let subscription: Vec<String> = catalog
        .models
        .iter()
        .filter(|model| !model.priced_per_token)
        .map(|model| model.route_key())
        .collect();
    let runners: Vec<Value> = catalog
        .runners
        .iter()
        .map(|runner| {
            json!({
                "module": runner.module,
                "routes": runner.route_keys,
                "tool_capabilities": runner
                    .tool_providers
                    .iter()
                    .flat_map(|provider| provider.capabilities.clone())
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    // CatalogRunner supplies runner-owned harness declarations separately from
    // operator route policy. Use declarations_from_catalog to parse them as
    // verified module manifests, rather than adding harnesses to config.
    let declarations = catalog
        .runners
        .iter()
        .zip(&runners)
        .map(|(runner, policy)| {
            let pairs = json!([
                {"wire_api":"anthropic.messages", "auth_method":"apikey"},
                {"wire_api":"anthropic.messages", "auth_method":"oauth"}
            ]);
            // Pass the fixture's tags through unchanged, so an invalid fixture tag
            // fails here rather than being filtered out of the declaration.
            let capabilities = policy["tool_capabilities"].clone();
            json!({
                "module_id": runner.module, "nonce_verified": true,
                "role_declarations": {"routing/v1": {"runner": {
                    "pairs": pairs,
                    "tool_capabilities": capabilities,
                    "harnesses": runner.harnesses,
                    "default_harness": runner.default_harness,
                    "starts_sessions": runner.starts_sessions
                }}}
            })
        })
        .collect::<Vec<_>>();
    {
        let shared = harness.broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        // Providers with explicit fixture accounts must have exactly those
        // credentials, not the generated API-key stand-ins for catalog routes.
        let mut rows: Vec<_> = credentials
            .listed_credentials()
            .iter()
            .filter(|row| {
                !catalog
                    .accounts
                    .iter()
                    .any(|account| row.provider_ids.contains(&account.provider))
            })
            .cloned()
            .collect();
        rows.extend(catalog.accounts.iter().map(|account| ListedCredential {
            id: account.credential_id.clone(),
            account_id: Some(account.account_id.clone()),
            credential_type: account.method.clone(),
            auth_method: Some(account.method.clone()),
            provider_ids: vec![account.provider.clone()],
            serves: account.serves.clone(),
            categories: vec![if account.method == "gateway" {
                "llm-gateway".into()
            } else {
                "llm-provider".into()
            }],
            state: "active".into(),
            ..Default::default()
        }));
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "conformance-credentials".into(),
                credentials: rows,
                ..Default::default()
            },
            crate::now_ms(),
        );
        credentials.refresh_serving_methods(
            &Default::default(),
            &facts,
            crate::now_ms() as i64,
            &[],
        );
        // Catalog order supplies the kit's synthetic price order on every
        // serving method, including the OAuth accounts used by hold checks.
        let billing_facts: Vec<_> = facts
            .iter()
            .flat_map(|fact| {
                ["apikey", "oauth"].into_iter().map(move |method| {
                    let mut fact = fact.clone();
                    fact.billing_plane = Some(striatum_core::billing::BillingPlane {
                        provider_id: fact.provider_id.clone(),
                        auth_method: method.into(),
                        kind: "upstream".into(),
                        selection_reason: "conformance fixture price".into(),
                        rates: [(
                            "rate.output".into(),
                            striatum_core::billing::BillingRate::Priced {
                                rate: fact.cost_per_m_tok_out.unwrap(),
                            },
                        )]
                        .into_iter()
                        .collect(),
                    });
                    fact
                })
            })
            .collect();
        credentials.runner_declarations =
            super::runners::declarations_from_catalog(&json!(declarations));
        assert_eq!(credentials.runner_declarations.len(), catalog.runners.len());
        credentials.wire_apis = facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    "anthropic.messages".into(),
                )
            })
            .collect();
        let billing_catalog =
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&billing_facts)
                .with_wire_apis(&credentials.wire_apis);
        credentials.publish_billing_catalog(billing_catalog);
    }
    harness.edit_config(|config| {
        config["runners"] = json!(runners);
        for key in &subscription {
            config["models"][key] = json!({"subscription": true});
        }
        // The fixture offers its gateway routes for ordinary selection;
        // production otherwise keeps company gateways pin-only by default.
        for account in catalog.accounts.iter().filter(|account| account.method == "gateway") {
            config["providers"][&account.provider]["pin_only"] = json!(false);
        }
    });
    *crate::ACCOUNT_QUOTA
        .get()
        .expect("a harness initializes the account state")
        .lock()
        .unwrap() = account_state(&catalog.accounts);
}

pub(super) fn install_panel_families(harness: &Harness, catalog: &Catalog) {
    // Older replay fixtures installed no lineage on synthetic catalog rows.
    // Panel fixtures exercise the public family member and install it explicitly.
    for (rank, model) in catalog.models.iter().enumerate() {
        let mut fact = catalog_fact(model, rank);
        fact.family = model.family.clone();
        harness
            .store
            .upsert_fact(&fact, FactSource::Override)
            .unwrap();
    }
}

/// Public choices require a ready catalog snapshot, unlike selection's fixture
/// billing view. Install it only for checks that need that read surface.
pub(super) fn install_head_snapshot(harness: &Harness) {
    let models: serde_json::Map<_, _> = harness
        .store()
        .get_facts()
        .unwrap()
        .iter()
        .map(|fact| {
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"existence":"present", "capability.tool_call":true}),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":1,"resolved_at_ms":123,"models":models}))
            .unwrap(),
    )
    .unwrap();
    harness
        .broca_credentials
        .lock()
        .unwrap()
        .lock()
        .unwrap()
        .catalog_snapshot = Some(Arc::new(snapshot));
}

/// The kit's subject: a fresh [`Harness`] per check.
struct KitSubject {
    harness: Mutex<Arc<Harness>>,
    observe_blocks: Mutex<bool>,
    media: Mutex<media_conformance::Inputs>,
}

struct KitRoute {
    route: Route,
    profiles: Arc<Mutex<striatum_core::model_router::RouteTaskProfiles>>,
}

#[async_trait]
impl RoutingRoute for KitRoute {
    async fn request(&self, method: &str, params: &Value) -> Result<Reply, RouteFailure> {
        let profiles = self.profiles.lock().unwrap().clone();
        if method == "route.select" && !profiles.is_empty() {
            let lookup =
                move |_: &str, _: f64, facts: &[ModelFact]| crate::select::TaskProfileLookup {
                    profiles: facts
                        .iter()
                        .filter_map(|fact| {
                            let key = striatum_core::fact_to_route(fact).route_key;
                            profiles.get(&key).cloned().map(|profile| (key, profile))
                        })
                        .collect(),
                    providers_without_cache_write_reports: Default::default(),
                };
            return match self.route.select_with_profiles(params, &lookup).await {
                Ok(answer) => Ok(Reply::Response(json!({"result":answer}).to_string())),
                Err(refusal) => Ok(Reply::Error(
                    serde_json::from_value(serde_json::to_value(refusal).unwrap()).unwrap(),
                )),
            };
        }
        self.route.request(method, params).await
    }
}

/// The kit supplies isolated route estimates, not a shared history of completed
/// tasks from which profiles can be borrowed across models. Convert estimates to
/// tokens once; the serving code must compute prices and reserves.
pub(super) fn selection_facts(
    harness: &Harness,
    input: &cortexkit_role_routing_conformance::SelectionFacts,
) -> striatum_core::model_router::RouteTaskProfiles {
    use striatum_core::task_profile::{ProfileLevel, TaskProfile, TokenVector};
    let mut facts = harness.store().get_facts().unwrap();
    for fact in &mut facts {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        if let Some(kind) = input.kinds.get(&key) {
            fact.model_kind = Some(kind.clone());
        }
        if let Some(rates) = input.list_rates.get(&key) {
            fact.input_rate = rates.input.as_f64();
            fact.cost_per_m_tok_out = rates.output.as_f64();
            fact.cache_read_rate = rates
                .cache_read
                .as_ref()
                .and_then(serde_json::Number::as_f64);
            fact.cache_write_rate = rates
                .cache_write
                .as_ref()
                .and_then(serde_json::Number::as_f64);
            let mut plane = striatum_core::BillingPlane {
                provider_id: fact.provider_id.clone(),
                auth_method: "apikey".into(),
                kind: "upstream".into(),
                selection_reason: "selection facts".into(),
                rates: Default::default(),
            };
            for (key, rate) in [
                ("rate.input", Some(&rates.input)),
                ("rate.output", Some(&rates.output)),
                ("rate.cache_read", rates.cache_read.as_ref()),
                ("rate.cache_write", rates.cache_write.as_ref()),
            ] {
                if let Some(rate) = rate {
                    plane.rates.insert(
                        key.into(),
                        striatum_core::billing::BillingRate::Priced {
                            rate: rate.as_f64().unwrap(),
                        },
                    );
                }
            }
            for (ttl, rate) in rates.cache_write_ttl.iter().flatten() {
                plane.rates.insert(
                    format!("rate.cache_write.ttl.{ttl}"),
                    striatum_core::billing::BillingRate::Priced {
                        rate: rate.as_f64().unwrap(),
                    },
                );
            }
            fact.billing_plane = Some(plane);
        }
    }
    for fact in &facts {
        harness
            .store()
            .upsert_fact(fact, FactSource::Override)
            .unwrap();
    }
    harness.edit_config(|config| {
        for (key, multiplier) in &input.multipliers {
            let provider = key.split_once('/').unwrap().0;
            config["providers"][provider]["subscription_multiplier"] = json!(multiplier);
        }
    });
    let billing_facts: Vec<_> = facts
        .iter()
        .filter(|fact| fact.billing_plane.is_some())
        .flat_map(|fact| {
            ["apikey", "oauth"].into_iter().map(move |method| {
                let mut fact = fact.clone();
                fact.billing_plane.as_mut().unwrap().auth_method = method.into();
                fact
            })
        })
        .collect();
    let credentials = harness.credentials();
    let mut credentials = credentials.lock().unwrap();
    let catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&billing_facts)
        .with_wire_apis(&credentials.wire_apis);
    credentials.publish_billing_catalog(catalog);
    input
        .estimates
        .iter()
        .map(|(key, estimate)| {
            let output = input.list_rates[key].output.as_f64().unwrap()
                / input
                    .multipliers
                    .get(key)
                    .and_then(serde_json::Number::as_f64)
                    .unwrap_or(1.0);
            (
                key.clone(),
                TaskProfile {
                    tokens: TokenVector {
                        output_excluding_reasoning: estimate.as_f64().unwrap() * 1_000_000.0
                            / output,
                        ..Default::default()
                    },
                    level: ProfileLevel::OwnCell,
                    task_count: 20,
                    own_task_count: 20,
                    own_weight: 1.0,
                    keeps_tokenizer: true,
                },
            )
        })
        .collect()
}

impl KitSubject {
    fn harness(&self) -> Arc<Harness> {
        Arc::clone(&self.harness.lock().unwrap())
    }
}

#[tokio::test]
async fn observe_success_fixture_arms_one_shared_quota_slot() {
    let subject = KitSubject {
        harness: Mutex::new(Harness::new("observe-fixture")),
        observe_blocks: Mutex::new(false),
        media: Mutex::new(Default::default()),
    };
    let mut catalog = fixture::catalog();
    let account = catalog
        .accounts
        .iter()
        .find(|a| a.provider == fixture::SUBSCRIPTION_PROVIDER)
        .unwrap()
        .clone();
    let mut sibling = account.clone();
    sibling.credential_id = "success-sibling".into();
    let mut other = account.clone();
    other.account_id = "success-other-account".into();
    other.credential_id = "success-other-credential".into();
    catalog.accounts = vec![account.clone(), sibling.clone(), other];
    subject.configure_catalog(&catalog).await.unwrap();
    let h = subject.harness();
    let route = h.bind(reserved(fixture::RUNNER_ONE), None).await;
    for (index, account) in [account, sibling].iter().enumerate() {
        if index == 0 {
            subject
                .configure_observe_retest_block(
                    &account.account_id,
                    &account.credential_id,
                    "quota",
                    crate::now_ms() - 1000,
                )
                .await
                .unwrap();
        }
        let before = h.store().fence_rechecks().unwrap();
        let reply = route
            .call(
                "route.account_for",
                &json!({
                "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                "conversation_key":format!("retest-{index}"),"admission_id":format!("retest-{index}"),
                "candidates":{"view_digest":"fixture","accounts":[{
                    "account_id":account.account_id,"credential_id":account.credential_id}]}
                }),
            )
            .await;
        let after = h.store().fence_rechecks().unwrap();
        if index == 1 {
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "account_pool_exhausted");
            assert_eq!(after[0].last_claimed_at, before[0].last_claimed_at);
            assert_eq!(after[0].next_recheck_at, before[0].next_recheck_at);
            continue;
        }
        assert_eq!(
            reply.0,
            FrameType::Response,
            "reply={} before={before:?} after={after:?}",
            reply.1
        );
        assert!(after.iter().any(|o| o.last_claimed_at.is_some()));
    }
}

#[async_trait]
impl RoutingSubject for KitSubject {
    type Route = KitRoute;

    async fn reset(&self) -> Result<(), String> {
        *self.harness.lock().unwrap() = Harness::new("kit");
        *self.media.lock().unwrap() = Default::default();
        Ok(())
    }

    async fn configure_selection_facts(
        &self,
        facts: &cortexkit_role_routing_conformance::SelectionFacts,
    ) -> Result<(), String> {
        let harness = self.harness();
        *harness.selection_profiles.lock().unwrap() = selection_facts(&harness, facts);
        let mut media = self.media.lock().unwrap();
        media.facts = facts.clone();
        media.install(&harness)?;
        Ok(())
    }

    async fn configure_consumers(&self, consumers: &[ConsumerConfig]) -> Result<(), String> {
        self.harness().edit_config(|config| {
            config["consumers"] = consumers
                .iter()
                .map(|consumer| {
                    (
                        consumer.id.clone(),
                        json!({
                            "owner": (consumer.id == fixture::SUMMARY).then_some(fixture::CONSUMER),
                            "pay_per_token": consumer.priced_only,
                            "iq": consumer.requirements.iq,
                            "eq": consumer.requirements.defaults.eq,
                            "context_tokens": consumer.requirements.defaults.context_window,
                            "max_output_tokens": consumer.requirements.defaults.max_output_tokens,
                            "tool_capabilities": consumer.requirements.defaults.tool_capabilities,
                        }),
                    )
                })
                .collect::<serde_json::Map<String, Value>>()
                .into();
        });
        Ok(())
    }

    async fn configure_catalog(&self, catalog: &Catalog) -> Result<(), String> {
        let harness = self.harness();
        let reset_blocks = std::mem::take(&mut *self.observe_blocks.lock().unwrap());
        if reset_blocks {
            // Replacing the fixture catalog resets its blocks, not its admissions:
            // the success checks deliberately replay admissions across replacements.
            let h = harness.clone();
            tokio::task::spawn_blocking(move || {
                h.store().with_writer_for_test(|conn| {
                    conn.execute_batch(
                        "DELETE FROM account_fence;
                    DELETE FROM credential_entitlement_fences;
                    DELETE FROM account_non_quota_route_fence; DELETE FROM account_route_fence;
                    DELETE FROM fence_recheck; DELETE FROM fence_probe_authorization;",
                    )
                })
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        }
        install_catalog(&harness, catalog);
        install_panel_families(&harness, catalog);
        install_head_snapshot(&harness);
        Ok(())
    }

    async fn configure_runner_route_facts(
        &self,
        routes: &[cortexkit_role_routing_conformance::RunnerRouteFacts],
    ) -> Result<bool, String> {
        let harness = self.harness();
        let shared = harness.broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        let mut facts = harness
            .store()
            .get_facts()
            .map_err(|error| error.to_string())?;
        let catalog_targets = credentials.catalog_targets(&facts);
        let mut serving = std::collections::BTreeSet::new();
        for route in routes {
            let (provider, model) = route.route_key.split_once('/').ok_or_else(|| {
                format!("runner route fact {} has no provider", route.route_key)
            })?;
            // Gateway route keys keep the gateway's identity, while wire and
            // billing facts belong to the provider named by its credential's `serves`.
            let catalog_key = if route.auth_method == "gateway" {
                catalog_targets.get(provider)
                    .map(|target| format!("{target}/{model}"))
                    .unwrap_or_else(|| route.route_key.clone())
            } else {
                route.route_key.clone()
            };
            let fact = facts
                .iter_mut()
                .find(|fact| {
                    striatum_core::fact_to_route(fact).route_key == catalog_key
                });
            let Some(fact) = fact else {
                // A gateway can declare a route with no matching catalog model
                // under its credential's `serves`. Leave that route absent so
                // refusal checks exercise routing.
                if route.auth_method == "gateway"
                    && credentials.listed_credentials().iter().any(|row| {
                        row.provider_ids.iter().any(|id| id == provider)
                            && row.auth_method.as_deref() == Some("gateway")
                    })
                {
                    continue;
                }
                return Err(format!("runner route fact {} has no catalog model", route.route_key));
            };
            credentials
                .wire_apis
                .insert(catalog_key, route.wire_api.clone());
            serving.insert((provider.to_owned(), route.auth_method.clone()));
            let mut plane = striatum_core::billing::BillingPlane {
                provider_id: fact.provider_id.clone(),
                auth_method: route.auth_method.clone(),
                kind: "upstream".into(),
                selection_reason: "conformance runner route facts".into(),
                rates: Default::default(),
            };
            plane.rates.insert(
                "rate.output".into(),
                striatum_core::billing::BillingRate::Priced {
                    rate: fact.cost_per_m_tok_out.unwrap(),
                },
            );
            fact.billing_plane = Some(plane);
        }
        let mut rows: Vec<_> = credentials
            .listed_credentials()
            .iter()
            .filter(|row| {
                !serving
                    .iter()
                    .any(|(provider, _)| row.provider_ids.contains(provider))
            })
            .cloned()
            .collect();
        for (provider, method) in serving {
            let existing: Vec<_> = credentials
                .listed_credentials()
                .iter()
                .filter(|row| {
                    row.provider_ids.contains(&provider)
                        && row.auth_method.as_deref() == Some(method.as_str())
                })
                .cloned()
                .collect();
            if existing.is_empty() {
                rows.push(ListedCredential {
                    id: format!("runner-fact-{provider}-{method}"),
                    provider_ids: vec![provider.clone()],
                    serves: vec![provider],
                    auth_method: Some(method.clone()),
                    credential_type: method,
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    ..Default::default()
                });
            } else {
                // Route facts describe the existing login, not a replacement
                // identity; account_for must admit the seeded credential.
                rows.extend(existing);
            }
        }
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "runner-route-facts".into(),
                credentials: rows,
                ..Default::default()
            },
            crate::now_ms(),
        );
        credentials.refresh_serving_methods(
            &Default::default(),
            &facts,
            crate::now_ms() as i64,
            &[],
        );
        let catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
            .with_wire_apis(&credentials.wire_apis);
        credentials.publish_billing_catalog(catalog);
        Ok(true)
    }

    async fn configure_route_wire_api(
        &self,
        route_key: &str,
        endpoint: Option<&str>,
    ) -> Result<bool, String> {
        let harness = self.harness();
        let shared = harness.broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        if endpoint.is_some() {
            credentials.wire_api_models.insert(route_key.into());
        } else {
            credentials.wire_api_models.remove(route_key);
        }
        let catalog = credentials
            .billing_catalog
            .clone()
            .with_wire_api_models(&credentials.wire_api_models);
        credentials.publish_billing_catalog(catalog);
        Ok(true)
    }

    async fn configure_continue_credentials(
        &self,
        credentials: &[cortexkit_role_routing_conformance::ContinueCredential],
    ) -> Result<(), String> {
        let harness = self.harness();
        let mut catalog = fixture::continue_catalog();
        // The supplied roster can remove spares to test sign-in exclusion or
        // add accounts for chained continuations. Install it exactly rather
        // than limiting it to accounts from the static catalog fixture.
        catalog.accounts = credentials.iter().map(|credential| credential.account.clone()).collect();
        install_catalog(&harness, &catalog);
        for credential in credentials {
            for model in catalog.models.iter().filter(|model| {
                model.provider == credential.account.provider
                    && !credential.models.contains(&model.model)
            }) {
                harness
                    .store()
                    .record_account_eligibility(&striatum_store::AccountEligibilityFact {
                        provider: model.provider.clone(),
                        credential_id: credential.account.credential_id.clone(),
                        model: model.model.clone(),
                        listing_fingerprint: None,
                        code: "model_not_on_account".into(),
                        observed_at: account_selection::now_ms(),
                    })
                    .map_err(|error| error.to_string())?;
            }
        }
        Ok(())
    }

    async fn configure_service_tiers(
        &self,
        plans: &std::collections::BTreeMap<
            String,
            Vec<cortexkit_role_routing::service_tier::ServiceTier>,
        >,
    ) -> Result<(), String> {
        let harness = self.harness();
        let shared = harness.broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        let mut rows = credentials.listed_credentials().to_vec();
        for row in &mut rows {
            row.plan_tier = row.account_id.clone();
        }
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: format!(
                    "tier-fixture-{}",
                    NEXT_HARNESS.fetch_add(1, Ordering::Relaxed)
                ),
                credentials: rows,
                ..Default::default()
            },
            crate::now_ms(),
        );
        let prices: Vec<Value> = plans
            .iter()
            .map(|(account, tiers)| {
                json!({
                    "provider_id": fixture::SUBSCRIPTION_PROVIDER, "tier": account,
                    "service_tiers": {
                        "state": "established",
                        "tiers": tiers.iter().map(|tier| json!({
                            "tier": tier.as_str(), "included_usage_rate": {"units":1,"exponent":0},
                            "credit_rate": {"units":1,"exponent":0}
                        })).collect::<Vec<_>>()
                    }
                })
            })
            .collect();
        credentials.publish_plan_prices(&json!({"prices": prices}));
        let served: Value = serde_json::from_str(include_str!(
            "../../fixtures/service-tier-v1/fusiform-served-payloads.json"
        ))
        .unwrap();
        let template = &served["catalog.get — provider default endpoint"]["response"]["models"]
            ["openai/gpt-6.1-sol"];
        let models: serde_json::Map<String, Value> = harness
            .store()
            .get_facts()
            .map_err(|e| e.to_string())?
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    template.clone(),
                )
            })
            .collect();
        let mut tier_catalog =
            served["catalog.get — provider default endpoint"]["response"].clone();
        tier_catalog["models"] = json!(models);
        let catalog = credentials
            .billing_catalog
            .clone()
            .with_test_catalog(&serde_json::to_vec(&tier_catalog).unwrap());
        credentials.publish_billing_catalog(catalog);
        for declaration in credentials.runner_declarations.values_mut() {
            declaration.follows_account_choice = true;
        }
        for module in credentials
            .runner_declarations
            .keys()
            .cloned()
            .collect::<Vec<_>>()
        {
            let tiers = super::runners::service_tiers_from_describe(&json!({
                "capabilities": ["service_tier"],
                "service_tiers": {"anthropic.messages": {"levels":["fast","ultrafast"],"served":"reported"}}
            })).unwrap();
            credentials.runner_service_tiers.insert(module, tiers);
        }
        Ok(())
    }

    async fn configure_catalog_declarations(
        &self,
        modules: &[CatalogModule],
    ) -> Result<(), String> {
        let catalog = Value::Array(
            modules
                .iter()
                .map(|module| {
                    json!({
                        "module_id": module.principal.trim_start_matches("reserved:"),
                        "nonce_verified": module.nonce_verified,
                        "role_declarations": module.role_declarations,
                    })
                })
                .collect(),
        );
        super::consumers::replace_catalog(&catalog);
        // Engine fixtures need a metadata-only media key as well as the kit's
        // LLM accounts. Synthetic ids deliberately carry no auth-method prefix.
        let shared = self.harness().broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        credentials
            .runner_declarations
            .extend(super::runners::declarations_from_catalog(&catalog));
        let mut rows = credentials.listed_credentials().to_vec();
        rows.retain(|row| !row.id.starts_with("synthetic-media-"));
        let providers: std::collections::BTreeSet<_> = super::consumers::engine_declarations()
            .values()
            .flat_map(|slot| slot.engines.iter().map(|engine| engine.provider.clone()))
            .collect();
        rows.extend(providers.into_iter().map(|provider| ListedCredential {
            id: format!("synthetic-media-{provider}"),
            categories: vec!["media-provider".into()],
            credential_type: "apikey".into(),
            auth_method: Some("apikey".into()),
            provider_ids: vec![provider],
            state: "active".into(),
            ..Default::default()
        }));
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: format!(
                    "media-fixture-{}",
                    serde_json::to_string(&rows.iter().map(|row| &row.id).collect::<Vec<_>>())
                        .unwrap()
                ),
                credentials: rows,
                grant_tuples: vec![cortexkit_claustrum_client::ListedGrant {
                    selector_kind: "category".into(),
                    selector: "media-provider".into(),
                    operation: "list".into(),
                }],
                ..Default::default()
            },
            crate::now_ms(),
        );
        drop(credentials);
        self.media.lock().unwrap().install(&self.harness())?;
        Ok(())
    }

    async fn configure_engine_facts(
        &self,
        facts: &[cortexkit_role_routing_conformance::EngineFacts],
    ) -> Result<(), String> {
        self.harness()
            .broca_credentials
            .lock()
            .unwrap()
            .lock()
            .unwrap()
            .engine_test_facts = facts
            .iter()
            .map(|fact| {
                (
                    (
                        format!("{}/{}", fact.provider, fact.model),
                        fact.kind.as_str().to_owned(),
                    ),
                    (
                        fact.accuracy.map(|value| value as f64),
                        fact.cost.map(|value| value as f64),
                    ),
                )
            })
            .collect();
        let mut media = self.media.lock().unwrap();
        media.costs = facts.to_vec();
        media.install(&self.harness())?;
        Ok(())
    }

    async fn advance_clock(&self, ms: u64) -> Result<(), String> {
        self.harness()
            .broca_credentials
            .lock()
            .unwrap()
            .lock()
            .unwrap()
            .engine_clock_offset_ms += ms;
        Ok(())
    }

    async fn pin_for_project(
        &self,
        project_id: &str,
        consumer: &str,
        pin: &str,
    ) -> Result<(), String> {
        self.harness().edit_config(|config| {
            config["consumers"][consumer]["projects"][project_id] = json!({"pin": pin});
        });
        Ok(())
    }

    async fn configure_pin_slot(&self, slot: &str, route_key: &str) -> Result<(), String> {
        self.harness().edit_config(|config| {
            config["consumers"][slot]["pin"] = json!({"model":route_key});
        });
        Ok(())
    }

    async fn configure_panel_class(
        &self,
        class: &str,
        size: u8,
        top: u8,
        extra: u8,
    ) -> Result<(), String> {
        if !(1..=8).contains(&size) || top + extra != size {
            return Err("invalid panel counts".into());
        }
        self.harness().edit_config(|config| {
            let mut policy = json!({"topK":top,"extra":extra,"by":"iq","floor":0});
            if extra > 0 {
                policy["extraBy"] = json!("random");
            }
            config["panels"][class] = policy;
        });
        Ok(())
    }

    async fn cool_down(&self, route_key: &str, until_ms: u64) -> Result<(), String> {
        self.harness()
            .store()
            .update_route_state(
                route_key,
                route_key,
                &RouteStateUpdate {
                    cooldown_until: Some(until_ms as f64),
                    ..Default::default()
                },
            )
            .map_err(|error| error.to_string())
    }

    async fn configure_consumer_mode(
        &self,
        consumer: &str,
        mode: &ConsumerMode,
    ) -> Result<(), String> {
        let (mode, entries) = match mode {
            ConsumerMode::Demand => ("demand", None),
            ConsumerMode::Ladder { entries } => ("ladder", Some(entries)),
            _ => return Err("unsupported consumer mode".into()),
        };
        self.harness().edit_config(|config| {
            config["consumers"][consumer]["mode"] = json!(mode);
            if let Some(entries) = entries {
                config["consumers"][consumer]["ladder"] = json!(entries);
            } else {
                config["consumers"][consumer]
                    .as_object_mut()
                    .unwrap()
                    .remove("ladder");
            }
        });
        Ok(())
    }

    async fn configure_hold_limits(
        &self,
        provider: &str,
        limits: HoldLimits,
    ) -> Result<(), String> {
        self.harness().edit_config(|config| {
            config["providers"][provider]["accounts"]["max_hold"] = json!(limits.max_hold_s);
            config["providers"][provider]["accounts"]["hold_when_reset_within"] =
                json!(limits.hold_when_reset_within_s);
        });
        Ok(())
    }

    async fn block_account(
        &self,
        account_id: &str,
        reset_at_ms: Option<u64>,
    ) -> Result<(), String> {
        let reset = reset_at_ms
            .map(|ms| {
                i64::try_from(ms)
                    .map(|ms| {
                        format!(
                            "{}.{:03}Z",
                            account_selection::rfc3339(ms).trim_end_matches('Z'),
                            ms.rem_euclid(1000)
                        )
                    })
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        let mut quota = crate::ACCOUNT_QUOTA
            .get()
            .ok_or("account state missing")?
            .lock()
            .map_err(|error| error.to_string())?;
        let reading = quota
            .readings
            .readings
            .get_mut(&(QUOTA_SOURCE.to_owned(), account_id.to_owned()))
            .ok_or("account reading missing")?;
        let now = account_selection::now_ms();
        reading.received_at_ms = now;
        reading.entry["fetchedAt"] = json!(account_selection::rfc3339(now));
        for name in ["primary", "secondary"] {
            let window = reading.entry["usage"][name]
                .as_object_mut()
                .ok_or("quota window missing")?;
            window.insert("usedPercent".into(), json!(100.0));
            if let Some(reset) = &reset {
                window.insert("resetsAt".into(), json!(reset));
            } else {
                window.remove("resetsAt");
            }
        }
        Ok(())
    }

    async fn configure_observe_retest_block(
        &self,
        account_id: &str,
        credential_id: &str,
        kind: &str,
        recorded_at_ms: u64,
    ) -> Result<(), String> {
        let h = self.harness();
        let account = account_id.to_owned();
        let credential = credential_id.to_owned();
        let kind = kind.to_owned();
        let at = i64::try_from(recorded_at_ms).map_err(|e| e.to_string())?;
        if kind == "quota" {
            // Keep healthy quota available after recovery, but do not let a
            // reading newer than the seeded refusal settle the fixture early.
            if let Some(reading) = crate::ACCOUNT_QUOTA
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .readings
                .readings
                .get_mut(&(QUOTA_SOURCE.to_owned(), account.clone()))
            {
                reading.entry["fetchedAt"] = json!(account_selection::rfc3339(at - 60_000));
            }
        }
        let (provider, fingerprint) = {
            let credentials = h.credentials();
            let listing = credentials.lock().unwrap();
            let row = listing
                .listed_credentials()
                .iter()
                .find(|r| r.id == credential)
                .ok_or("fixture credential missing")?;
            let provider = row
                .provider_ids
                .first()
                .ok_or("fixture provider missing")?
                .clone();
            let fingerprint = match listing.listing_fingerprint(&credential) {
                crate::broca_credentials::ListingLookup::Listed(value) => Some(value),
                _ => None,
            };
            (provider, fingerprint)
        };
        *self.observe_blocks.lock().unwrap() = true;
        tokio::task::spawn_blocking(move || {
            h.store().account_transaction(|tx| {
                if matches!(kind.as_str(), "quota" | "transient_backoff") {
                    let fence = striatum_store::AccountRouteFence {
                        provider: provider.clone(),
                        credential_id: credential.clone(),
                        model: "model-c".into(),
                        until_ms: at + 3_600_000,
                        decision_id: "kit-success".into(),
                        recorded_at: at,
                    };
                    if kind == "quota" {
                        tx.record_account_route_fence(&fence)?;
                    } else {
                        tx.record_transient_account_route_fence(&fence)?;
                    }
                } else {
                    tx.record_credential_refusal(
                        &striatum_core::route_state_store::CredentialRefusal {
                            provider_id: provider.clone(),
                            credential_id: credential.clone(),
                            code: match kind.as_str() {
                                "credential_refused" => "credential_refused",
                                "credit" => "billing_exhausted",
                                _ => {
                                    return Err(
                                        striatum_core::route_state_store::RouteStateStoreError(
                                            "unknown fixture block".into(),
                                        ),
                                    )
                                }
                            }
                            .into(),
                        },
                        fingerprint.as_deref(),
                        at,
                    )?;
                }
                tx.arm_observe_retest_for_test(&provider, &account, &credential)
            })
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }

    async fn route(&self, stamp: &RouteStamp) -> Result<KitRoute, String> {
        if let Some(scope) = &stamp.scope {
            let harness = self.harness();
            let channel = harness.reserve_channel();
            let bind = json!({
                "op": "route.bind", "route_channel": channel, "epoch": 1,
                "target": { "kind": "management_surface", "module_id": DEFAULT_MODULE_ID },
                "identity": { "project_root": cortexkit_test_support::scratch_root(), "harness": "routing-kit", "session": format!("session-{channel}") },
                "principal": reserved(&stamp.principal),
                "scope": { "owner": reserved(&scope.owner), "ref": scope.scope_ref, "scope_epoch": scope.scope_epoch, "kind": "ephemeral", "project_id": stamp.project_id, "owner_authorized": true },
            });
            return Ok(KitRoute {
                route: harness.bind_frame(bind, channel, 1).await,
                profiles: harness.selection_profiles.clone(),
            });
        }
        Ok(KitRoute {
            route: self
                .harness()
                .bind(reserved(&stamp.principal), stamp.project_id.as_deref())
                .await,
            profiles: self.harness().selection_profiles.clone(),
        })
    }
}

/// Checks gated off by group declarations or optional fixture hooks. The
/// model-input hook is absent here; classifier handler tests cover question
/// limits and input-spend caps against the catalog and provider description.
const NOT_APPLICABLE: &[&str] = &[
    "account_observe_success_undeclared",
    "undeclared_pin_slot_refused",
    "undeclared_available_routes_refused",
    "undeclared_prefer_prompt_cache_refused",
    "undeclared_settings_refused",
    "undeclared_observe_decision_id_refused",
    "undeclared_observe_retry_at_ms_refused",
    "undeclared_observe_provider_code_refused",
    "account_for_tier_undeclared",
    "warm_credential_undeclared_refused",
    "continue_undeclared_group_refuses_mode",
    "continue_signed_out_undeclared_refuses_window",
    "undeclared_harness_group_refuses_member",
    "undeclared_prefer_group_refuses_member",
    "undeclared_exclude_providers_group_refuses_member",
    "questions_requirement_honoured",
    "max_cost_usd_requirement_honoured",
    "declared_consumer_conflict_refused",
    // Serving decision_harness means routing names a harness rather than
    // omitting an ambiguous one, so the kit skips the omission case.
    "unrequested_harness_uses_declaration",
];

// Consumer priority is read for classification. concurrency_full is a refusal
// reason, not a capability: `route.account_for` serves it only with
// concurrency enforcement on, and `select` does not serve it yet.
// Media selection stays isolated until the kit accepts valid floating-point
// unit rates and the caller-local cost policy is approved for production.
const PENDING_READERS: &[(&str, &[&str])] = &[
    ("select_kind", &["placement_select_kind"]),
    (
        "select_media",
        &[
            "select_media_input_characters",
            "select_media_privacy",
            "select_media_requirements",
            "select_media_video",
            "select_media_ranking",
            "select_media_ladder",
            "select_media_llm_refuses_media",
            "select_media_refuses_llm",
            "select_media_metric_limit",
            "select_media_unlisted_kind",
            "select_media_answer",
            "select_media_pin",
            "select_media_price",
            "select_media_demand",
            "select_media_ungraded_level",
            "select_media_policy",
            "select_media_report_admission",
        ],
    ),
];
const PENDING_REFUSAL_REASONS: &[&str] = &[];

fn assert_no_pending_refusal(reply: &Frame, enforced: impl FnOnce() -> bool) {
    if reply.header.ty == FrameType::Error {
        let body: Value = serde_json::from_slice(&reply.body).unwrap();
        if body["code"] == "no_route" {
            let reason = body["detail"]["reason"].as_str().unwrap_or_default();
            assert!(
                !PENDING_REFUSAL_REASONS.contains(&reason),
                "served refusal {reason} still marked pending: {body}"
            );
            // Enforcement defaults off, and off it must answer as before:
            // no request, kit case or burst over a cap is refused for it.
            assert!(
                reason != "concurrency_full" || enforced(),
                "concurrency_full served with enforcement off: {body}"
            );
        }
    }
}

// Capabilities routing advertises that the contract crate's list of known
// groups doesn't define: tier settings in operator configuration, and resuming
// an account session that already moved.
const ROUTING_EXTENSIONS: &[&str] = &["service_tier_settings", "account_continue_resume"];

/// Every kit check that applies to routing passes; the checks that don't apply
/// are pinned explicitly in `NOT_APPLICABLE`.
/// A failing check names the contract rule it holds the provider to.
#[tokio::test]
async fn routing_v1_conformance_kit_matches_served_checks() {
    let advertised: std::collections::BTreeSet<_> =
        super::describe::CAPABILITIES.iter().copied().collect();
    let known: std::collections::BTreeSet<_> = cortexkit_role_routing::capabilities::KNOWN
        .iter()
        .copied()
        .collect();
    let expected: std::collections::BTreeSet<_> = known
        .iter()
        .copied()
        .filter(|group| !PENDING_READERS.iter().any(|(pending, _)| pending == group))
        .chain(ROUTING_EXTENSIONS.iter().copied())
        .collect();
    assert_eq!(
        advertised, expected,
        "only the named pending readers may be unadvertised"
    );
    for group in ROUTING_EXTENSIONS {
        assert!(
            !known.contains(group),
            "extension {group} is now a contract group"
        );
    }
    for (group, _) in PENDING_READERS {
        assert!(known.contains(group), "unknown pending group {group}");
        assert!(
            !advertised.contains(group),
            "served group {group} still marked pending"
        );
    }
    let subject = KitSubject {
        observe_blocks: Mutex::new(false),
        media: Mutex::new(Default::default()),
        harness: Mutex::new(Harness::new("kit")),
    };
    for reason in PENDING_REFUSAL_REASONS {
        assert!(cortexkit_role_routing::errors::NoRouteReason::parse(reason).is_known());
        assert!(
            !known.contains(reason),
            "refusal {reason} became a capability"
        );
        assert!(
            !advertised.contains(reason),
            "pending refusal advertised as a group"
        );
    }
    let report = run_suite(&subject).await;
    eprintln!("{}", report.render());
    assert_eq!(report.checks.len(), CHECKS.len());
    for check in [
        "observe_credential_refused_requires_backoff",
        "credential_refused_blocks_credential",
        "credential_refused_retry_at_ms_refused",
        "account_observe_credit_without_reset_accepted",
        "harness_limits_pairs_narrow",
        "harness_limits_providers_narrow",
        "harness_limits_invalid_falls_back",
        "runner_requires_wire_api",
        "runner_gateway_credential",
        "runner_gateway_explicit_providers",
        "warm_credential_fresh_honoured",
        "warm_credential_hard_filter_ignored",
        "warm_credential_unknown_ignored",
        "warm_credential_existing_affinity",
        "warm_credential_admission_conflict",
        "warm_credential_non_admit_refused",
        "placement_eligible_routes",
        "placement_consumer_declarations",
        "continue_account_decision_preserves_route",
        "continue_account_decision_wrong_admission_refused",
        "continue_account_decision_chain",
    ] {
        assert_eq!(
            report.outcome(check),
            Some(&cortexkit_role_routing_conformance::CheckOutcome::Passed),
            "{}",
            report.render()
        );
    }
    assert!(report.failing().is_empty(), "{}", report.render());
    let expected_not_applicable: Vec<_> = NOT_APPLICABLE
        .iter()
        .copied()
        .chain(PENDING_READERS.iter().flat_map(|(_, cases)| cases.iter().copied()))
        .collect();
    let actual: std::collections::BTreeSet<_> = report.not_applicable().into_iter().collect();
    let expected: std::collections::BTreeSet<_> = expected_not_applicable.into_iter().collect();
    assert_eq!(actual, expected, "{}", report.render());
}

#[tokio::test]
async fn candidate_capabilities_reach_readers_without_leaking_to_production_calls() {
    use cortexkit_role_routing::capabilities::{
        CONSUMER_DECLARATIONS, REPORT_ATTEMPT_OUTCOMES, SELECT_KIND,
    };
    let groups = [SELECT_KIND, REPORT_ATTEMPT_OUTCOMES];
    let harness = Harness::new("candidate-capabilities");
    let slot = json!({
        "consumers": [{"name":"example-consumer.task", "iq":40}],
        "runner": {"pairs":[{"wire_api":"openai.chat", "auth_method":"apikey"}]}
    });
    let catalog = json!([{
        "module_id":"example-consumer", "nonce_verified":true,
        "role_declarations":{"routing/v1":slot}
    }]);
    let select =
        json!({"consumer":"example-consumer.task", "requirements":{"iq":40}, "kind":"llm"});
    let report = json!({"decision_id":"missing", "outcome":"cancelled", "attempt_outcome":{"status":"rejected"}});
    let production = || {
        assert_eq!(
            super::describe::capabilities().as_ref(),
            super::describe::CAPABILITIES
        );
        assert!(super::describe::capabilities().contains(&CONSUMER_DECLARATIONS));
        assert!(super::consumers::Catalog::from_catalog(&catalog)
            .effective(&harness.operator_config())
            .knows("example-consumer.task"));
        assert!(super::runners::parse_declaration(&slot).unwrap().is_some());
        assert!(
            cortexkit_role_routing::select::parse_select_request_serving(
                &select,
                &super::describe::capabilities()
            )
            .is_err()
        );
        assert!(
            cortexkit_role_routing::report::parse_report_request_serving(
                &report,
                &super::describe::capabilities()
            )
            .is_ok()
        );
    };
    production();
    super::describe::with_candidate_capabilities(&groups, async {
        assert!(super::consumers::Catalog::from_catalog(&catalog)
            .effective(&harness.operator_config())
            .knows("example-consumer.task"));
        // Live runner catalogs and route.what_if's runner_declarations overlay
        // use this same slot decoder to read a proposed runner declaration.
        assert!(super::runners::parse_declaration(&slot).unwrap().is_some());
        assert!(
            cortexkit_role_routing::select::parse_select_request_serving(
                &select,
                &super::describe::capabilities()
            )
            .is_ok()
        );
        assert!(
            cortexkit_role_routing::report::parse_report_request_serving(
                &report,
                &super::describe::capabilities()
            )
            .is_ok()
        );
        let candidate_route = harness.bind(reserved("example-consumer"), None).await;
        let candidate = candidate_route.call("role.describe", &json!({})).await;
        assert_eq!(candidate.0, FrameType::Response, "{candidate:?}");
        for group in groups {
            assert!(candidate.1["result"]["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!(group)));
        }
        let harness = harness.clone();
        let served = tokio::spawn(async move {
            assert_eq!(
                super::describe::capabilities().as_ref(),
                super::describe::CAPABILITIES
            );
            let route = harness.bind(reserved("example-consumer"), None).await;
            route.call("role.describe", &json!({})).await
        })
        .await
        .unwrap();
        assert_eq!(served.0, FrameType::Response, "{served:?}");
        let advertised = served.1["result"]["capabilities"].as_array().unwrap();
        for group in groups {
            assert_eq!(
                advertised.contains(&json!(group)),
                super::describe::CAPABILITIES.contains(&group),
                "{served:?}"
            );
        }
    })
    .await;
    production();
}

#[tokio::test]
async fn migration_065_restarts_replays_and_binds_preupgrade_admissions() {
    use striatum_store::{dev_descriptor, SqliteRouteStateStore};
    let now = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(now as u64);
    let harness = Harness::new_at_version("upgrade-062-admission", Some(62));
    let request = json!({
        "provider": fixture::SUBSCRIPTION_PROVIDER,
        "model": "model-c",
        "conversation_key": "upgrade-conversation",
        "admission_id": "preupgrade-v1",
    });
    let first = (FrameType::Response, seed_062_admission(&harness, &request, true, now));
    assert!(first.1["result"]["account"].is_object(), "{}", first.1);
    let mut legacy = request.clone();
    legacy["admission_id"] = json!("preupgrade-legacy");
    legacy["conversation_key"] = json!("legacy-upgrade-conversation");
    legacy["caller"] = json!({"runner": fixture::RUNNER_ONE});
    let legacy_first = (FrameType::Response, seed_062_admission(&harness, &legacy, false, now));

    // Release the old store's writer lease before reopening the same file.
    // The placeholder is a separate scratch database, never an upgrade input.
    let mut state = Arc::try_unwrap(harness).ok().unwrap();
    let descriptor = dev_descriptor(
        DEFAULT_MODULE_ID,
        state.dir.join("store.db").to_string_lossy(),
    );
    let placeholder = Arc::new(
        SqliteRouteStateStore::open(&dev_descriptor(
            "routing-upgrade-placeholder",
            state.dir.join("placeholder/store.db").to_string_lossy(),
        ))
        .unwrap(),
    );
    drop(std::mem::replace(&mut state.store, placeholder));
    state.store = Arc::new(SqliteRouteStateStore::open(&descriptor).unwrap());
    state.route_epochs = tokio::sync::Mutex::new(RouteEpochs::new());
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = AccountQuotaState::default();
    let harness = Arc::new(state);
    install_catalog(&harness, &fixture::catalog());
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    assert_eq!(runner.call("route.account_for", &request).await.1, first.1);
    assert_eq!(
        runner.call("route.account_for", &legacy).await.1,
        legacy_first.1
    );
    for mut conflicting in [request.clone(), legacy.clone()] {
        conflicting["model"] = json!("different-model");
        let conflict = runner.call("route.account_for", &conflicting).await;
        assert_eq!(conflict.0, FrameType::Error, "{}", conflict.1);
        assert_eq!(conflict.1["code"], "admission_conflict", "{}", conflict.1);
    }
    let observed = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id": "postupgrade-start", "kind": "admission_started",
                "admission_id": "preupgrade-v1", "run_id": "upgraded-run",
            }),
        )
        .await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
    assert_eq!(
        observed.1["result"]["observation"]["bound"], true,
        "{}",
        observed.1
    );
    harness
        .store()
        .account_transaction(|tx| {
            let principal = format!(
                "reserved:{}",
                fixture::RUNNER_ONE.trim_start_matches("reserved:")
            );
            let row = tx.admission(&principal, "preupgrade-v1")?.unwrap();
            assert_eq!(row.state, "started");
            assert_eq!(row.run_id.as_deref(), Some("upgraded-run"));
            Ok(())
        })
        .unwrap();
    let principal = format!(
        "reserved:{}",
        fixture::RUNNER_ONE.trim_start_matches("reserved:")
    );
    let reservation = harness
        .store()
        .account_transaction(|tx| tx.admission(&principal, "preupgrade-legacy"))
        .unwrap()
        .unwrap();
    let policy = striatum_core::accounts::AllocatorPolicy::default();
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: harness.operator_config().account_flags(),
        policy: &policy,
        now_ms: reservation.admission_valid_until + 1,
        classes: None,
    };
    let request = json!({"method":"route.account_for", "params": legacy});
    let answer = account_selection::handle(
        "route.account_for",
        &serde_json::to_vec(&request).unwrap(),
        &principal,
        &ctx,
    );
    assert!(
        matches!(answer, account_selection::OpReply::Result(_)),
        "{answer:?}"
    );
    harness
        .store()
        .account_transaction(|tx| {
            assert_eq!(
                tx.admission(&principal, "preupgrade-legacy")?
                    .unwrap()
                    .state,
                "expired"
            );
            assert_eq!(
                tx.admission(&principal, "preupgrade-v1")?.unwrap().state,
                "started"
            );
            Ok(())
        })
        .unwrap();
}

// Historical admissions were written by the schema-62 binary. Seed its rows
// directly: its model_facts table lacks columns today's fact readers require.
fn seed_062_admission(harness: &Harness, request: &Value, v1: bool, now: i64) -> Value {
    use cortexkit_role_routing::account::{
        Account, AccountForAnswer, AccountPin, AccountReason, AccountReasonCode, Admission,
        parse_account_for_request_serving,
    };
    use sha2::{Digest, Sha256};
    let provider = request["provider"].as_str().unwrap();
    let model = request["model"].as_str().unwrap();
    let conversation = request["conversation_key"].as_str().unwrap();
    let id = request["admission_id"].as_str().unwrap();
    let principal = format!("reserved:{}", fixture::RUNNER_ONE.trim_start_matches("reserved:"));
    let pin = format!("pin-{id}");
    let decision = format!("decision-{id}");
    let conversation_hash = format!("{:x}", Sha256::digest(conversation.as_bytes()));
    assert_ne!(fixture::RUNNER_ONE, crate::legacy_wire::BROCA);
    // Fixed-order identity encoding used by the historical plugin admission.
    let identity = format!(r#"{{"provider":{},"model":{},"affinity_model":{},"conversation_key":{},"caller":"plugin","mode":"admit","expected_pin":null,"requested_account":null,"preferred_existing_account":null}}"#,
        request["provider"], request["model"], request["model"], request["conversation_key"]);
    let fingerprint = format!("{:x}", Sha256::digest(identity.as_bytes()));
    let valid_until = now + 60_000;
    let lease_until = now + 120_000;
    let affinity_until = now + 3_600_000;
    let legacy_reply = json!({
        "admission":{"id":id,"state":"reserved"},
        "account":{"provider":provider,"account_id":"historical-account",
            "credential_id":"historical-credential","method":"oauth"},
        "pin":{"id":pin,"generation":1,"mode":"conversation_lease",
            "affinity_expires_at":account_selection::rfc3339(affinity_until),
            "admission_valid_until":account_selection::rfc3339(valid_until),
            "lease_valid_until":account_selection::rfc3339(lease_until)},
        "reason":{"code":"cold_scored","decision_id":decision,"policy_version":2,
            "quota_fetched_at":account_selection::rfc3339(now),"quota_age_band":"fresh"}
    });
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    connection.execute(
        "INSERT INTO account_pin (pin_id,principal,conversation_hash,provider,pin_generation,
          affinity_model,account_id,credential_id,pin_mode,decision_id,created_at)
         VALUES (?1,?2,?3,?4,1,?5,'historical-account','historical-credential','conversation_lease',?6,?7)",
        rusqlite::params![pin,principal,conversation_hash,provider,model,decision,now]).unwrap();
    connection.execute(
        "INSERT INTO account_affinity (principal,conversation_hash,provider,affinity_model,
          pin_id,pin_generation,account_id,credential_id,credential_binding_revision,policy_version,
          initial_demand_bytes,created_at,last_used_at,expires_at,lease_valid_until)
         VALUES (?1,?2,?3,?4,?5,1,'historical-account','historical-credential',1,2,0,?6,?6,?7,?8)",
        rusqlite::params![principal,conversation_hash,provider,model,pin,now,affinity_until,lease_until]).unwrap();
    connection.execute(
        "INSERT INTO account_admission (principal,admission_id,request_fingerprint,request_mode,
          decision_id,conversation_hash,provider,model,account_id,credential_id,pin_id,pin_generation,
          state,charged,request_bytes,request_bytes_estimated,committed_at,admission_valid_until,
          lease_valid_until,reply_json)
         VALUES (?1,?2,?3,'admit',?4,?5,?6,?7,'historical-account','historical-credential',?8,1,
          'reserved',1,0,0,?9,?10,?11,?12)",
        rusqlite::params![principal,id,fingerprint,decision,conversation_hash,provider,model,pin,
            now,valid_until,lease_until,legacy_reply.to_string()]).unwrap();
    let answer = if v1 {
        let parsed = parse_account_for_request_serving(request, super::describe::CAPABILITIES).unwrap();
        let answer = AccountForAnswer::new(
            Admission { id:id.into(), state:"reserved".into() },
            Account { provider:provider.into(), account_id:"historical-account".into(),
                credential_id:"historical-credential".into(), method:"oauth".into() },
            AccountPin::new(&pin,1,affinity_until as u64,valid_until as u64,lease_until as u64,parsed.mode()),
            AccountReason { code:AccountReasonCode::parse("cold_scored"),decision_id:decision,
                policy_version:"2".into(),quota_fetched_at_ms:Some(now as u64),quota_age_band:"fresh".into() },
        );
        let answer = serde_json::to_value(answer).unwrap();
        connection.execute("INSERT INTO routing_v1_admission VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![principal,id,serde_json::to_string(&parsed.normalized()).unwrap(),answer.to_string(),now]).unwrap();
        answer
    } else {
        legacy_reply
    };
    json!({"result":answer})
}

/// Reopen a released writer so migration tests exercise the real upgrade path.
pub(super) fn upgrade_harness(harness: Arc<Harness>) -> Arc<Harness> {
    let mut state = Arc::try_unwrap(harness)
        .ok()
        .expect("all test routes were dropped");
    let descriptor = striatum_store::dev_descriptor(
        DEFAULT_MODULE_ID,
        state.dir.join("store.db").to_string_lossy(),
    );
    let placeholder = Arc::new(
        striatum_store::SqliteRouteStateStore::open(
            &striatum_store::dev_descriptor(
                "routing-class-upgrade-placeholder",
                state.dir.join("placeholder/store.db").to_string_lossy(),
            ),
        )
        .unwrap(),
    );
    drop(std::mem::replace(&mut state.store, placeholder));
    state.store =
        Arc::new(striatum_store::SqliteRouteStateStore::open(&descriptor).unwrap());
    state.route_epochs = tokio::sync::Mutex::new(RouteEpochs::new());
    Arc::new(state)
}
