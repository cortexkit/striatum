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
    run_suite, Catalog, ConsumerConfig, Reply, RouteFailure, RouteStamp, RoutingRoute,
    RoutingSubject, CHECKS,
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
    auth_path: PathBuf,
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
        let n = NEXT_HARNESS.fetch_add(1, Ordering::Relaxed);
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-v1-{label}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(
            prefrontal_routing_store::SqliteRouteStateStore::open(
                &prefrontal_routing_store::dev_descriptor(
                    DEFAULT_MODULE_ID,
                    dir.join("store.db").to_string_lossy().into_owned(),
                ),
            )
            .unwrap(),
        );
        let auth_path = dir.join("auth.json");
        std::fs::write(&auth_path, "{}").unwrap();
        let harness = Arc::new(Self {
            store,
            quota_cache: Arc::new(Mutex::new(QuotaCache::default())),
            broca_credentials: Mutex::new(Arc::new(Mutex::new(
                BrocaCredentials::api_test_credentials(&[]),
            ))),
            catalog_health: Arc::new(Mutex::new(CatalogReachHealth::new(600_000))),
            auth_path,
            config_path: Mutex::new(dir.join("routing-0.jsonc")),
            config: Mutex::new(json!({
                "model_routing": {},
                "account_selection": {"enabled": true},
            })),
            config_generation: AtomicU64::new(0),
            route_epochs: tokio::sync::Mutex::new(RouteEpochs::new()),
            next_channel: AtomicU16::new(100),
            dir,
        });
        harness.edit_config(|_| {});
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

    /// Change the routing configuration and publish it as a new file.
    pub(super) fn edit_config(&self, edit: impl FnOnce(&mut Value)) {
        let mut config = self.config.lock().unwrap();
        edit(&mut config);
        let generation = self.config_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let path = self.dir.join(format!("routing-{generation}.jsonc"));
        std::fs::write(&path, serde_json::to_vec_pretty(&*config).unwrap()).unwrap();
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
                "project_root": prefrontal_test_support::scratch_root(),
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

    /// Hand one frame to the module's frame handler and wait for its reply.
    async fn handle(&self, frame: Frame) -> Frame {
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
            &self.auth_path,
            &self.catalog_health,
        )
        .await
        .unwrap();
        assert!(serving, "the frame loop must keep serving");
        drop(route_epochs);
        drop(writer);
        replies.recv().await.expect("every request gets a reply")
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
    /// Send `{method, params}` on this route; the reply frame's type and body.
    pub(super) async fn call(&self, method: &str, params: &Value) -> (FrameType, Value) {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            self.channel,
            self.epoch,
            self.next_corr.fetch_add(1, Ordering::Relaxed),
            serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap(),
        )
        .unwrap();
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
    json!({ "kind": "reserved", "module_id": module })
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
    harness.edit_config(|config| {
        config["model_routing"]["runners"] = json!(runners);
        config["model_routing"]["subscription"] = json!(subscription);
    });
    *crate::ACCOUNT_QUOTA
        .get()
        .expect("a harness initializes the account state")
        .lock()
        .unwrap() = account_state(&catalog.accounts);
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
        // Routing configures consumer ids only: whether a consumer runs
        // priced per token is the rule of the request class its id names.
        // A kit consumer whose priced_only that rule does not give cannot be
        // configured.
        for consumer in consumers {
            let routing = crate::select_policy::requires_metered(
                crate::select_policy::Consumer::from_id(&consumer.id),
            );
            if routing != consumer.priced_only {
                return Err(format!(
                    "consumer {} is priced_only {routing} in routing, the kit asks {}",
                    consumer.id, consumer.priced_only
                ));
            }
        }
        let ids: Vec<&str> = consumers
            .iter()
            .map(|consumer| consumer.id.as_str())
            .collect();
        self.harness().edit_config(|config| {
            config["model_routing"]["consumers"] = json!(ids);
        });
        Ok(())
    }

    async fn configure_catalog(&self, catalog: &Catalog) -> Result<(), String> {
        install_catalog(&self.harness(), catalog);
        Ok(())
    }

    async fn pin_for_project(
        &self,
        project_id: &str,
        consumer: &str,
        pin: &str,
    ) -> Result<(), String> {
        self.harness().edit_config(|config| {
            config["model_routing"]["project_pins"][project_id][consumer] = json!(pin);
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

    async fn route(&self, stamp: &RouteStamp) -> Result<Route, String> {
        Ok(self
            .harness()
            .bind(reserved(&stamp.principal), stamp.project_id.as_deref())
            .await)
    }
}

/// Kit checks this module does not serve yet, in the kit's run order. The
/// first seven need the account-op members routing/v1 0.2.0 added
/// (`parent_conversation_key`, `pool`, `eligibility`, `request_input_tokens`),
/// which the v1 account ops and report still refuse as unknown members. The
/// next six need consumer declarations read from the daemon catalog, which
/// this subject's `configure_catalog_declarations` hook does not support yet.
/// The last needs `runner.harness` in the select answer, filled from a
/// runner's own declaration, which routing does not read yet.
/// Each is expected to fail until routing serves it; the work that makes one
/// pass removes it here, and the full-kit verdict holds once the list is empty.
const NOT_YET_SERVED: &[&str] = &[
    "parent_account_preference",
    "parent_account_fallback",
    "eligibility_never_holds_or_exhausts",
    "unknown_pool_accepted",
    "pool_body_admission_conflict",
    "parent_body_admission_conflict",
    "new_report_members_conflict",
    "declared_consumer_owner_uses_default",
    "declared_consumer_scope_owner_admitted",
    "declared_consumer_non_owner_refused",
    "operator_config_overrides_declared_default",
    "declared_consumer_unverified_entry_ignored",
    "declared_consumer_conflict_refused",
    "harness_only_when_declared",
];

/// The kit's hold checks, in run order. The kit reports them not applicable
/// because `role.describe` does not declare `select_hold_control`
/// ([`super::describe::CAPABILITIES`] is empty). When routing declares the
/// group, these checks run, and this list empties alongside the subject's
/// hold hooks (`configure_consumer_mode`, `configure_hold_limits`,
/// `block_account`), which this subject does not implement yet.
const NOT_APPLICABLE_WITHOUT_HOLD_CONTROL: &[&str] = &[
    "hold_answers_blocked_entry_within_limits",
    "hold_moves_when_reset_beyond_window",
    "hold_moves_when_reset_unknown",
    "hold_moves_beyond_caller_cap",
    "hold_ok_false_moves_to_usable_entry",
    "hold_ok_false_falls_back_to_soonest_entry",
    "hold_nothing_within_limit_accounts_blocked",
    "hold_demand_mode_skips_blocked_model",
    "hold_account_for_applies_recorded_cap",
];

/// Every kit check this module serves passes, exactly the checks in
/// [`NOT_YET_SERVED`] fail, and exactly the hold checks are not applicable:
/// a newly failing check, or a listed check that now passes or runs, breaks
/// the test. A failing check names the contract rule it holds the provider
/// to.
#[tokio::test]
async fn routing_v1_conformance_kit_passes_every_served_check() {
    let subject = KitSubject {
        harness: Mutex::new(Harness::new("kit")),
    };
    let report = run_suite(&subject).await;
    assert_eq!(report.checks.len(), CHECKS.len());
    assert_eq!(report.failing(), NOT_YET_SERVED, "{}", report.render());
    assert_eq!(
        report.not_applicable(),
        NOT_APPLICABLE_WITHOUT_HOLD_CONTROL,
        "{}",
        report.render()
    );
}
