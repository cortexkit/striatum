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
use prefrontal_routing_core::model_facts::{Confidence, ModelScores, ScoreProvenance};
use prefrontal_routing_core::route_state_store::FactSource;
use prefrontal_routing_core::{ModelFact, RouteStateStore, RouteStateUpdate};
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
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "prefrontal-routing-v1-{label}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let descriptor = prefrontal_routing_store::dev_descriptor(
            DEFAULT_MODULE_ID,
            dir.join("store.db").to_string_lossy().into_owned(),
        );
        let store = Arc::new(
            match version {
                Some(version) => {
                    prefrontal_routing_store::SqliteRouteStateStore::open_at_version_for_test(
                        &descriptor,
                        version,
                    )
                }
                None => prefrontal_routing_store::SqliteRouteStateStore::open(&descriptor),
            }
            .unwrap(),
        );
        if let Some(inputs) = prefrontal_routing_store::replay::current() {
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
        replies.recv().await.expect("every request gets a reply")
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
        auth_methods: None,
        speed_is_fleet_median: false,
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
        upstream_status: None,
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
                categories: vec!["llm-provider".into()],
                credential_type: account.method.clone(),
                auth_method: Some(account.method.clone()),
                provider_ids: vec![account.provider.clone()],
                serves: vec![account.provider.clone()],
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
            serves: vec![account.provider.clone()],
            categories: vec!["llm-provider".into()],
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
                    fact.billing_plane = Some(prefrontal_routing_core::billing::BillingPlane {
                        provider_id: fact.provider_id.clone(),
                        auth_method: method.into(),
                        kind: "upstream".into(),
                        selection_reason: "conformance fixture price".into(),
                        rates: [(
                            "rate.output".into(),
                            prefrontal_routing_core::billing::BillingRate::Priced {
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
}

impl KitSubject {
    fn harness(&self) -> Arc<Harness> {
        Arc::clone(&self.harness.lock().unwrap())
    }
}

#[async_trait]
impl RoutingSubject for KitSubject {
    type Route = Route;

    async fn reset(&self) -> Result<(), String> {
        *self.harness.lock().unwrap() = Harness::new("kit");
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
        install_catalog(&harness, catalog);
        install_panel_families(&harness, catalog);
        install_head_snapshot(&harness);
        Ok(())
    }

    async fn configure_continue_credentials(
        &self,
        credentials: &[cortexkit_role_routing_conformance::ContinueCredential],
    ) -> Result<(), String> {
        let harness = self.harness();
        let catalog = fixture::continue_catalog();
        install_catalog(&harness, &catalog);
        for credential in credentials {
            for model in catalog.models.iter().filter(|model| {
                model.provider == credential.account.provider
                    && !credential.models.contains(&model.model)
            }) {
                harness
                    .store()
                    .record_account_eligibility(&prefrontal_routing_store::AccountEligibilityFact {
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
        super::consumers::replace_catalog(&Value::Array(
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
        ));
        // Engine fixtures need a metadata-only media key as well as the kit's
        // LLM accounts. Synthetic ids deliberately carry no auth-method prefix.
        let shared = self.harness().broca_credentials.lock().unwrap().clone();
        let mut credentials = shared.lock().unwrap();
        let mut rows = credentials.listed_credentials().to_vec();
        rows.retain(|row| !row.id.starts_with("synthetic-media-"));
        let providers: std::collections::BTreeSet<_> = super::consumers::engine_declarations()
            .values()
            .flat_map(|slot| slot.engines.iter().map(|engine| engine.provider.clone()))
            .collect();
        rows.extend(providers.into_iter().map(|provider| ListedCredential {
            id: format!("synthetic-media-{provider}"),
            categories: vec!["media-provider".into()],
            credential_type: "api_key".into(),
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

    async fn route(&self, stamp: &RouteStamp) -> Result<Route, String> {
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
            return Ok(harness.bind_frame(bind, channel, 1).await);
        }
        Ok(self
            .harness()
            .bind(reserved(&stamp.principal), stamp.project_id.as_deref())
            .await)
    }
}

/// Checks gated off by group declarations or optional fixture hooks. The
/// model-input hook is absent here; classifier handler tests cover question
/// limits and input-spend caps against the catalog and provider description.
const NOT_APPLICABLE: &[&str] = &[
    "undeclared_pin_slot_refused",
    "undeclared_available_routes_refused",
    "undeclared_prefer_prompt_cache_refused",
    "undeclared_settings_refused",
    "undeclared_observe_decision_id_refused",
    "undeclared_observe_retry_at_ms_refused",
    "undeclared_observe_provider_code_refused",
    "account_for_tier_undeclared",
    "continue_undeclared_group_refuses_mode",
    "undeclared_harness_group_refuses_member",
    "undeclared_prefer_group_refuses_member",
    "undeclared_exclude_providers_group_refuses_member",
    "questions_requirement_honoured",
    "max_cost_usd_requirement_honoured",
];

/// Every served check passes, and exactly [`NOT_APPLICABLE`] are inapplicable.
/// A failing check names the contract rule it holds the provider to.
#[tokio::test]
async fn routing_v1_conformance_kit_matches_served_checks() {
    for group in cortexkit_role_routing::capabilities::KNOWN {
        assert!(
            super::describe::CAPABILITIES.contains(group),
            "undeclared group {group}"
        );
    }
    let subject = KitSubject {
        harness: Mutex::new(Harness::new("kit")),
    };
    let report = run_suite(&subject).await;
    eprintln!("{}", report.render());
    assert_eq!(report.checks.len(), CHECKS.len());
    for check in [
        "observe_credential_refused_requires_backoff",
        "credential_refused_blocks_credential",
        "credential_refused_retry_at_ms_refused",
    ] {
        assert_eq!(
            report.outcome(check),
            Some(&cortexkit_role_routing_conformance::CheckOutcome::Passed),
            "{}",
            report.render()
        );
    }
    assert!(report.failing().is_empty(), "{}", report.render());
    assert_eq!(
        report.not_applicable(),
        NOT_APPLICABLE,
        "{}",
        report.render()
    );
}

#[tokio::test]
async fn migration_065_restarts_replays_and_binds_preupgrade_admissions() {
    use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
    let harness = Harness::new_at_version("upgrade-062-admission", Some(62));
    install_catalog(&harness, &fixture::catalog());
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let request = json!({
        "provider": fixture::SUBSCRIPTION_PROVIDER,
        "model": "model-c",
        "conversation_key": "upgrade-conversation",
        "admission_id": "preupgrade-v1",
    });
    let first = runner.call("route.account_for", &request).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert!(first.1["result"]["account"].is_object(), "{}", first.1);
    let mut legacy = request.clone();
    legacy["admission_id"] = json!("preupgrade-legacy");
    legacy["conversation_key"] = json!("legacy-upgrade-conversation");
    legacy["caller"] = json!({"runner": fixture::RUNNER_ONE});
    let legacy_first = runner.call("route.account_for", &legacy).await;
    assert_eq!(legacy_first.0, FrameType::Response, "{}", legacy_first.1);
    drop(runner);

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
    let policy = prefrontal_routing_core::accounts::AllocatorPolicy::default();
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

/// Reopen a released writer so migration tests exercise the real upgrade path.
pub(super) fn upgrade_harness(harness: Arc<Harness>) -> Arc<Harness> {
    let mut state = Arc::try_unwrap(harness)
        .ok()
        .expect("all test routes were dropped");
    let descriptor = prefrontal_routing_store::dev_descriptor(
        DEFAULT_MODULE_ID,
        state.dir.join("store.db").to_string_lossy(),
    );
    let placeholder = Arc::new(
        prefrontal_routing_store::SqliteRouteStateStore::open(
            &prefrontal_routing_store::dev_descriptor(
                "routing-class-upgrade-placeholder",
                state.dir.join("placeholder/store.db").to_string_lossy(),
            ),
        )
        .unwrap(),
    );
    drop(std::mem::replace(&mut state.store, placeholder));
    state.store =
        Arc::new(prefrontal_routing_store::SqliteRouteStateStore::open(&descriptor).unwrap());
    state.route_epochs = tokio::sync::Mutex::new(RouteEpochs::new());
    Arc::new(state)
}
